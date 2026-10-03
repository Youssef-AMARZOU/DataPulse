"""Quality gate Silver -> Gold.

1. `verify_contract`  : soda contract verify -> dq_results/dq_metrics (Postgres) -> gauge OTLP (Grafana Cloud).
2. `publish_gold`     : ne s'exécute que si la vérification passe (Airflow gate).

Codes de sortie Soda Core v4 (docs.soda.io) : 0 = pass, 1 = fail, 2 = warn, >= 3 = error.
Un warning ne bloque pas le pipeline ; fail et error bloquent.
"""

from __future__ import annotations

import logging
import os
import subprocess
from datetime import datetime

import psycopg2
from airflow.sdk import DAG, task

log = logging.getLogger(__name__)

DATASET_DQN = "lakehouse/lakehouse/silver/transactions"
DATASET_LABEL = "lakehouse.silver.transactions"
DS_CONFIG = "/opt/airflow/config/ds_config.yml"
CONTRACT = "/opt/airflow/contracts/silver_transactions.yml"
SODA_TIMEOUT_SECONDS = 600

OUTCOMES = {0: "pass", 1: "fail", 2: "warn"}


def _lakehouse_conn():
    return psycopg2.connect(
        host=os.environ.get("LAKEHOUSE_HOST", "postgres-lakehouse"),
        port=os.environ.get("LAKEHOUSE_PORT", "5432"),
        dbname=os.environ.get("LAKEHOUSE_DB", "lakehouse"),
        user=os.environ.get("LAKEHOUSE_USER", "soda"),
        password=os.environ.get("LAKEHOUSE_PASSWORD", "soda"),
    )


def _run_soda() -> tuple[int, str]:
    proc = subprocess.run(
        ["soda", "contract", "verify", "-ds", DS_CONFIG, "-c", CONTRACT],
        capture_output=True,
        text=True,
        timeout=SODA_TIMEOUT_SECONDS,
    )
    output = (proc.stdout or "") + (proc.stderr or "")
    return proc.returncode, output[-4000:]


def _record_result(outcome: str, exit_code: int, details: str) -> None:
    with _lakehouse_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dq_results (dataset, contract, outcome, exit_code, details)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (DATASET_DQN, CONTRACT.rsplit("/", 1)[-1], outcome, exit_code, details),
        )
        cur.execute(
            """
            INSERT INTO dq_metrics (dataset, metric, value)
            VALUES (%s, 'contract_passed', %s)
            """,
            (DATASET_DQN, 1.0 if outcome in ("pass", "warn") else 0.0),
        )


def _push_otel(passed: bool) -> None:
    """Exporte une jauge vers le gateway OTLP de Grafana Cloud (no-op si non configuré)."""
    if not os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT"):
        log.info("OTEL_EXPORTER_OTLP_ENDPOINT absent : export des métriques ignoré.")
        return

    from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
    from opentelemetry.sdk.resources import Resource

    reader = PeriodicExportingMetricReader(
        OTLPMetricExporter(), export_interval_millis=10_000
    )
    provider = MeterProvider(
        resource=Resource.create(
            {"service.name": "dq-checks", "deployment.environment": "poc"}
        ),
        metric_readers=[reader],
    )
    gauge = provider.get_meter("dq").create_gauge(
        name="dq_contract_passed",
        unit="1",
        description="Dernier résultat de vérification du contrat (1 = pass, 0 = fail/error)",
    )
    gauge.set(
        1.0 if passed else 0.0,
        {"dataset": DATASET_LABEL, "layer": "silver"},
    )
    # La tâche est courte : flush obligatoire avant shutdown.
    provider.force_flush(timeout_millis=15_000)
    provider.shutdown()


def _fail_reason(exit_code: int) -> str:
    if exit_code == 1:
        return "au moins un check du contrat a échoué"
    if exit_code >= 3:
        return f"erreur d'exécution de Soda (exit code {exit_code})"
    return f"inconnu (exit code {exit_code})"


with DAG(
    "dq_silver_transactions",
    start_date=datetime(2026, 1, 1),
    schedule="*/15 * * * *",
    catchup=False,
    tags=["dq", "poc"],
) as dag:

    @task
    def verify_contract() -> dict:
        exit_code, output = _run_soda()
        outcome = OUTCOMES.get(exit_code, "error")
        passed = exit_code in (0, 2)

        log.info("soda contract verify -> exit_code=%s outcome=%s", exit_code, outcome)

        try:
            _record_result(outcome, exit_code, output)
        except Exception:
            log.exception("Échec de l'écriture de dq_results/dq_metrics")
            raise

        try:
            _push_otel(passed)
        except Exception:
            # L'observabilité ne doit pas faire échouer le contrôle de qualité.
            log.exception("Échec de l'export OTLP (la vérification reste valable)")

        if not passed:
            raise RuntimeError(
                f"Contrat {CONTRACT} en échec : {_fail_reason(exit_code)}\n"
                f"--- sortie Soda (fin) ---\n{output}"
            )
        return {"outcome": outcome, "exit_code": exit_code}

    @task
    def publish_gold(result: dict) -> None:
        with _lakehouse_conn() as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO gold_publications (dataset) VALUES (%s)",
                (DATASET_DQN,),
            )
        log.info("Gold publié pour %s (%s)", DATASET_LABEL, result["outcome"])

    publish_gold(verify_contract())
