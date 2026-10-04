"""Helpers partages par les DAGs de qualite (quality gates Silver -> Gold)."""

from __future__ import annotations

import logging
import os
import subprocess

import psycopg2

log = logging.getLogger(__name__)

DS_CONFIG = "/opt/airflow/config/ds_config.yml"
SODA_TIMEOUT_SECONDS = 600
OUTCOMES = {0: "pass", 1: "fail", 2: "warn"}


def lakehouse_conn():
    return psycopg2.connect(
        host=os.environ.get("LAKEHOUSE_HOST", "postgres-lakehouse"),
        port=os.environ.get("LAKEHOUSE_PORT", "5432"),
        dbname=os.environ.get("LAKEHOUSE_DB", "lakehouse"),
        user=os.environ.get("LAKEHOUSE_USER", "soda"),
        password=os.environ.get("LAKEHOUSE_PASSWORD", "soda"),
    )


def run_soda(contract: str) -> tuple[int, str]:
    proc = subprocess.run(
        ["soda", "contract", "verify", "-ds", DS_CONFIG, "-c", contract],
        capture_output=True,
        text=True,
        timeout=SODA_TIMEOUT_SECONDS,
    )
    output = (proc.stdout or "") + (proc.stderr or "")
    return proc.returncode, output[-4000:]


def record_result(dataset_dqn: str, contract: str, outcome: str, exit_code: int, details: str) -> None:
    with lakehouse_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dq_results (dataset, contract, outcome, exit_code, details)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (dataset_dqn, contract.rsplit("/", 1)[-1], outcome, exit_code, details),
        )
        cur.execute(
            """
            INSERT INTO dq_metrics (dataset, metric, value)
            VALUES (%s, 'contract_passed', %s)
            """,
            (dataset_dqn, 1.0 if outcome in ("pass", "warn") else 0.0),
        )


def push_otel(dataset_label: str, passed: bool) -> None:
    """Exporte une jauge vers le gateway OTLP de Grafana Cloud (no-op si non configure)."""
    if not os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT"):
        log.info("OTEL_EXPORTER_OTLP_ENDPOINT absent : export des metriques ignore.")
        return

    from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
    from opentelemetry.sdk.resources import Resource

    reader = PeriodicExportingMetricReader(OTLPMetricExporter(), export_interval_millis=10_000)
    provider = MeterProvider(
        resource=Resource.create({"service.name": "dq-checks", "deployment.environment": "poc"}),
        metric_readers=[reader],
    )
    gauge = provider.get_meter("dq").create_gauge(
        name="dq_contract_passed",
        unit="1",
        description="Dernier resultat de verification du contrat (1 = pass, 0 = fail/error)",
    )
    gauge.set(1.0 if passed else 0.0, {"dataset": dataset_label, "layer": "silver"})
    # La tache est courte : flush obligatoire avant shutdown.
    provider.force_flush(timeout_millis=15_000)
    provider.shutdown()


def fail_reason(exit_code: int) -> str:
    if exit_code == 1:
        return "au moins un check du contrat a echoue"
    if exit_code >= 3:
        return f"erreur d'execution de Soda (exit code {exit_code})"
    return f"inconnu (exit code {exit_code})"
