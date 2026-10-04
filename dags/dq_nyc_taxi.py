"""Quality gate Silver -> Gold pour silver.nyc_taxi (6 mois de donnees TLC reelles).

Meme mecanisme que dq_silver_transactions (voir dq_lib) : verification Soda,
persistance, jauge OTLP, puis publication Gold sous condition de reussite.
"""

from __future__ import annotations

import logging
from datetime import datetime

from airflow.sdk import DAG, task

import dq_lib

log = logging.getLogger(__name__)

DATASET_DQN = "lakehouse/lakehouse/silver/nyc_taxi"
DATASET_LABEL = "lakehouse.silver.nyc_taxi"
CONTRACT = "/opt/airflow/contracts/nyc_taxi.yml"


def _verify() -> dict:
    exit_code, output = dq_lib.run_soda(CONTRACT)
    outcome = dq_lib.OUTCOMES.get(exit_code, "error")
    passed = exit_code in (0, 2)

    log.info("soda contract verify -> exit_code=%s outcome=%s", exit_code, outcome)

    try:
        dq_lib.record_result(DATASET_DQN, CONTRACT, outcome, exit_code, output)
    except Exception:
        log.exception("Echec de l'ecriture de dq_results/dq_metrics")
        raise

    try:
        dq_lib.push_otel(DATASET_LABEL, passed)
    except Exception:
        log.exception("Echec de l'export OTLP (la verification reste valable)")

    if not passed:
        raise RuntimeError(
            f"Contrat {CONTRACT} en echec : {dq_lib.fail_reason(exit_code)}\n"
            f"--- sortie Soda (fin) ---\n{output}"
        )
    return {"outcome": outcome, "exit_code": exit_code}


with DAG(
    "dq_nyc_taxi",
    start_date=datetime(2026, 1, 1),
    schedule="30 */6 * * *",  # toutes les 6 h : le check porte sur ~20 M de lignes
    catchup=False,
    tags=["dq", "poc", "bigdata"],
) as dag:

    @task
    def verify_contract() -> dict:
        return _verify()

    @task
    def publish_gold(result: dict) -> None:
        with dq_lib.lakehouse_conn() as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO gold_publications (dataset) VALUES (%s)",
                (DATASET_DQN,),
            )
        log.info("Gold publie pour %s (%s)", DATASET_LABEL, result["outcome"])

    publish_gold(verify_contract())
