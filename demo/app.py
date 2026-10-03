"""DataPulse demo API.

FastAPI backend served in the `demo` container : exposes the quality results
stored by the Airflow DAG, runs Soda contract checks on demand, and injects /
repairs bad data to demonstrate the Silver -> Gold quality gate live.

Runs from the same image as Airflow (soda + psycopg2 available).
"""

from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path

import psycopg2
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse

app = FastAPI(title="DataPulse Demo API", version="1.0")

DS_CONFIG = "/opt/airflow/config/ds_config.yml"
CONTRACT = "/opt/airflow/contracts/silver_transactions.yml"
SODA_TIMEOUT_SECONDS = 600
OUTCOMES = {0: "pass", 1: "fail", 2: "warn"}
STATIC_DIR = Path(__file__).parent / "static"


def _conn():
    return psycopg2.connect(
        host=os.environ.get("LAKEHOUSE_HOST", "postgres-lakehouse"),
        port=os.environ.get("LAKEHOUSE_PORT", "5432"),
        dbname=os.environ.get("LAKEHOUSE_DB", "lakehouse"),
        user=os.environ.get("LAKEHOUSE_USER", "soda"),
        password=os.environ.get("LAKEHOUSE_PASSWORD", "soda"),
    )


def _record(outcome: str, exit_code: int, details: str, passed: bool) -> None:
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dq_results (dataset, contract, outcome, exit_code, details)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                "lakehouse/lakehouse/silver/transactions",
                "silver_transactions.yml",
                outcome,
                exit_code,
                details,
            ),
        )
        cur.execute(
            """
            INSERT INTO dq_metrics (dataset, metric, value)
            VALUES (%s, 'contract_passed', %s)
            """,
            ("lakehouse/lakehouse/silver/transactions", 1.0 if passed else 0.0),
        )


@app.get("/")
def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/summary")
def summary() -> dict:
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT dataset, contract, outcome, exit_code, executed_at
            FROM dq_results ORDER BY id DESC LIMIT 1
            """
        )
        last = cur.fetchone()
        cur.execute(
            """
            SELECT value FROM dq_metrics
            WHERE metric = 'contract_passed' ORDER BY id DESC LIMIT 1
            """
        )
        m = cur.fetchone()
        cur.execute("SELECT count(*) FROM silver.transactions")
        total = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM silver.transactions WHERE amount < 0 OR status NOT IN ('completed','pending','refunded')")
        bad = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM gold_publications")
        pubs = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM dq_results")
        runs = cur.fetchone()[0]
    return {
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
    }


@app.get("/api/history")
def history(limit: int = 20) -> dict:
    limit = max(1, min(limit, 100))
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT outcome, exit_code, executed_at
            FROM dq_results ORDER BY id DESC LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()
    return {
        "history": [
            {"outcome": r[0], "exit_code": r[1], "executed_at": r[2].isoformat()}
            for r in rows
        ]
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
def check() -> dict:
    """Execute soda contract verify, enregistre le resultat, renvoie le detail."""
    t0 = time.monotonic()
    proc = subprocess.run(
        ["soda", "contract", "verify", "-ds", DS_CONFIG, "-c", CONTRACT],
        capture_output=True,
        text=True,
        timeout=SODA_TIMEOUT_SECONDS,
    )
    duration = round(time.monotonic() - t0, 2)
    output = (proc.stdout or "") + (proc.stderr or "")
    outcome = OUTCOMES.get(proc.returncode, "error")
    passed = proc.returncode in (0, 2)
    try:
        _record(outcome, proc.returncode, output[-4000:], passed)
    except Exception as exc:  # noqa: BLE001 - demo API : on renvoie l'erreur
        raise HTTPException(status_code=500, detail=f"enregistrement impossible: {exc}")
    return {
        "outcome": outcome,
        "exit_code": proc.returncode,
        "passed": passed,
        "duration_s": duration,
        "output_tail": output[-2000:],
    }
