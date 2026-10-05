"""DataPulse demo API.

FastAPI backend servi dans le conteneur `demo` : expose les resultats de
qualite enregistres par les DAGs Airflow, relance les contrats Soda a la
demande, et injecte / repare des donnees invalides pour montrer le gate en live.

Deux jeux de donnees coexistent :
- transactions : silver.transactions (60 lignes, scenario pedagogique inject/fix)
- taxi         : silver.nyc_taxi (~20 M lignes TLC reelles, 2024-01..06)
"""

from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path

import psycopg2
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse

try:
    from demo import startup
except ImportError:  # excecution directe hors package
    import startup

app = FastAPI(title="DataPulse Demo API", version="2.0")

DS_CONFIG = os.environ.get("DS_CONFIG", "/opt/airflow/config/ds_config.yml")

# Initialisation idempotente (no-op en local, seed + load au 1er boot heberge).
startup.start()
SODA_TIMEOUT_SECONDS = 600
OUTCOMES = {0: "pass", 1: "fail", 2: "warn"}
STATIC_DIR = Path(__file__).parent / "static"

DATASETS = {
    "transactions": {
        "dqn": "lakehouse/lakehouse/silver/transactions",
        "table": "silver.transactions",
        "contract": "/opt/airflow/contracts/silver_transactions.yml",
        "bad_where": "amount < 0 OR status NOT IN ('completed','pending','refunded')",
    },
    "taxi": {
        "dqn": "lakehouse/lakehouse/silver/nyc_taxi",
        "table": "silver.nyc_taxi",
        "contract": os.environ.get("TAXI_CONTRACT", "/opt/airflow/contracts/nyc_taxi.yml"),
        "bad_where": (
            "trip_distance < 0 OR trip_distance > 400000 "
            "OR total_amount < -2000 OR total_amount > 400000 "
            "OR payment_type NOT IN (0,1,2,3,4,5)"
        ),
    },
}


def _conn():
    return psycopg2.connect(
        host=os.environ.get("LAKEHOUSE_HOST", "postgres-lakehouse"),
        port=os.environ.get("LAKEHOUSE_PORT", "5432"),
        dbname=os.environ.get("LAKEHOUSE_DB", "lakehouse"),
        user=os.environ.get("LAKEHOUSE_USER", "soda"),
        password=os.environ.get("LAKEHOUSE_PASSWORD", "soda"),
    )


def _ds(name: str) -> dict:
    if name not in DATASETS:
        raise HTTPException(status_code=404, detail=f"dataset inconnu: {name}")
    return DATASETS[name]


def _record(ds: dict, outcome: str, exit_code: int, details: str, passed: bool) -> None:
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dq_results (dataset, contract, outcome, exit_code, details)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (ds["dqn"], ds["contract"].rsplit("/", 1)[-1], outcome, exit_code, details),
        )
        cur.execute(
            """
            INSERT INTO dq_metrics (dataset, metric, value)
            VALUES (%s, 'contract_passed', %s)
            """,
            (ds["dqn"], 1.0 if passed else 0.0),
        )


@app.get("/")
def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/summary")
def summary(dataset: str = Query("transactions")) -> dict:
    ds = _ds(dataset)
    empty = {
        "dataset": dataset,
        "last_check": None,
        "contract_passed": None,
        "rows_total": None,
        "rows_bad": None,
        "gold_publications": None,
        "checks_total": None,
        "init": dict(startup.status),
    }
    try:
        with _conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT dataset, contract, outcome, exit_code, executed_at
                FROM dq_results WHERE dataset = %s ORDER BY id DESC LIMIT 1
                """,
                (ds["dqn"],),
            )
            last = cur.fetchone()
            cur.execute(
                """
                SELECT value FROM dq_metrics
                WHERE dataset = %s AND metric = 'contract_passed'
                ORDER BY id DESC LIMIT 1
                """,
                (ds["dqn"],),
            )
            m = cur.fetchone()
            try:
                cur.execute(f"SELECT count(*) FROM {ds['table']}")
                total = cur.fetchone()[0]
                cur.execute(f"SELECT count(*) FROM {ds['table']} WHERE {ds['bad_where']}")
                bad = cur.fetchone()[0]
            except Exception:  # noqa: BLE001 - table absente (chargement en cours)
                conn.rollback()
                total, bad = None, None
            cur.execute(
                "SELECT count(*) FROM gold_publications WHERE dataset = %s", (ds["dqn"],)
            )
            pubs = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM dq_results WHERE dataset = %s", (ds["dqn"],))
            runs = cur.fetchone()[0]
    except Exception:  # noqa: BLE001 - BDD pas encore prete au premier boot
        return empty
    return {
        "dataset": dataset,
        "last_check": (
            {
                "dataset": last[0],
                "contract": last[1],
                "outcome": last[2],
                "exit_code": last[3],
                "executed_at": last[4].isoformat() if last else None,
            }
            if last
            else None
        ),
        "contract_passed": int(m[0]) if m else None,
        "rows_total": total,
        "rows_bad": bad,
        "gold_publications": pubs,
        "checks_total": runs,
        "init": dict(startup.status),
    }


@app.get("/api/history")
def history(dataset: str = Query("transactions"), limit: int = 20) -> dict:
    ds = _ds(dataset)
    limit = max(1, min(limit, 100))
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT outcome, exit_code, executed_at
            FROM dq_results WHERE dataset = %s ORDER BY id DESC LIMIT %s
            """,
            (ds["dqn"], limit),
        )
        rows = cur.fetchall()
    return {
        "dataset": dataset,
        "history": [
            {"outcome": r[0], "exit_code": r[1], "executed_at": r[2].isoformat()}
            for r in rows
        ],
    }


@app.post("/api/inject")
def inject() -> dict:
    """Insere une ligne invalide (montant negatif) : le contrat doit echouer."""
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO silver.transactions (transaction_id, customer_id, amount, status, event_ts)
            SELECT coalesce(max(transaction_id), 0) + 1, -1, -99.99, 'completed', now()
            FROM silver.transactions
            RETURNING transaction_id
            """
        )
        tx_id = cur.fetchone()[0]
    return {"injected": True, "transaction_id": tx_id, "amount": -99.99}


@app.post("/api/fix")
def fix() -> dict:
    """Supprime les lignes invalides : le contrat doit repasser."""
    with _conn() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM silver.transactions WHERE amount < 0")
        deleted = cur.rowcount
    return {"fixed": True, "deleted": deleted}


@app.post("/api/check")
def check(dataset: str = Query("transactions")) -> dict:
    """Execute soda contract verify sur le dataset choisi, enregistre le resultat."""
    ds = _ds(dataset)
    t0 = time.monotonic()
    proc = subprocess.run(
        ["soda", "contract", "verify", "-ds", DS_CONFIG, "-c", ds["contract"]],
        capture_output=True,
        text=True,
        timeout=SODA_TIMEOUT_SECONDS,
    )
    duration = round(time.monotonic() - t0, 2)
    output = (proc.stdout or "") + (proc.stderr or "")
    outcome = OUTCOMES.get(proc.returncode, "error")
    passed = proc.returncode in (0, 2)
    try:
        _record(ds, outcome, proc.returncode, output[-4000:], passed)
    except Exception as exc:  # noqa: BLE001 - demo API : on renvoie l'erreur
        raise HTTPException(status_code=500, detail=f"enregistrement impossible: {exc}")
    return {
        "dataset": dataset,
        "outcome": outcome,
        "exit_code": proc.returncode,
        "passed": passed,
        "duration_s": duration,
        "output_tail": output[-2000:],
    }
