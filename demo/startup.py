"""Initialisation idempotente du lakehouse au demarrage du service demo.

Utile pour la demo heergee free tier (Render + Neon) : la base est neuve au
premier boot. En local (postgres-lakehouse avec seed) tout existe deja -> no-op.

- cree silver.transactions + tables dq_results / dq_metrics / gold_publications
- insere les 60 lignes de seed si la table est vide
- telecharge et charge silver.nyc_taxi (TAXI_MONTHS, defaut 2024-01 = ~3 M
  lignes pour rester dans les limits des plans gratuits) si la table est absente

Le travail se fait dans un thread daemon : l'API repond des le demarrage et
l'etat est expose via /api/summary ("init": pending|running|ready|error).
"""

from __future__ import annotations

import os
import sys
import threading
import traceback

import psycopg2

status: dict = {"state": "pending", "detail": "attente du demarrage"}

INIT_SQL = """
CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.transactions (
    transaction_id BIGINT       PRIMARY KEY,
    customer_id    BIGINT       NOT NULL,
    amount         NUMERIC(12,2) NOT NULL,
    status         TEXT         NOT NULL CHECK (status IN ('completed', 'pending', 'refunded')),
    event_ts       TIMESTAMPTZ  NOT NULL
);

CREATE TABLE IF NOT EXISTS dq_results (
    id          BIGSERIAL PRIMARY KEY,
    dataset     TEXT        NOT NULL,
    contract    TEXT        NOT NULL,
    outcome     TEXT        NOT NULL,
    exit_code   INT         NOT NULL,
    details     TEXT,
    executed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dq_metrics (
    id      BIGSERIAL PRIMARY KEY,
    dataset TEXT        NOT NULL,
    metric  TEXT        NOT NULL,
    value   DOUBLE PRECISION NOT NULL,
    ts      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS gold_publications (
    id           BIGSERIAL PRIMARY KEY,
    dataset      TEXT       NOT NULL,
    published_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

SEED_SQL = """
INSERT INTO silver.transactions (transaction_id, customer_id, amount, status, event_ts)
SELECT
    g,
    1000 + (g % 50),
    round((random() * 500 + 1)::numeric, 2),
    (ARRAY['completed', 'pending', 'refunded'])[1 + (g % 3)],
    now() - (g || ' minutes')::interval
FROM generate_series(1, 60) AS g
ON CONFLICT (transaction_id) DO NOTHING
"""

# Flux simule : 4 nouvelles lignes toutes les 15 min pour que le check de
# fraicheur (< 24 h) reste vert sans ingestion reelle (meme logique que le
# workflow GitHub Actions quality-gate).
STREAM_SQL = """
INSERT INTO silver.transactions (transaction_id, customer_id, amount, status, event_ts)
SELECT base + g,
       1000 + (g % 50),
       round((random() * 500 + 1)::numeric, 2),
       (ARRAY['completed', 'pending', 'refunded'])[1 + (g % 3)],
       now() - (g || ' minutes')::interval
FROM generate_series(1, 4) AS g, (
    SELECT coalesce(max(transaction_id), 0) AS base FROM silver.transactions
) s
ON CONFLICT (transaction_id) DO NOTHING
"""

STREAM_INTERVAL_SECONDS = 900


def _conn():
    return psycopg2.connect(
        host=os.environ.get("LAKEHOUSE_HOST", "postgres-lakehouse"),
        port=os.environ.get("LAKEHOUSE_PORT", "5432"),
        dbname=os.environ.get("LAKEHOUSE_DB", "lakehouse"),
        user=os.environ.get("LAKEHOUSE_USER", "soda"),
        password=os.environ.get("LAKEHOUSE_PASSWORD", "soda"),
        sslmode=os.environ.get("LAKEHOUSE_SSLMODE", "prefer"),
    )


def ensure_ready() -> None:
    status.update(state="running", detail="creation du schema et des tables")
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(INIT_SQL)

        cur.execute("SELECT count(*) FROM silver.transactions")
        if cur.fetchone()[0] == 0:
            status.update(detail="seed silver.transactions (60 lignes)")
            cur.execute(SEED_SQL)

        cur.execute("SELECT to_regclass('silver.nyc_taxi')")
        if cur.fetchone()[0] is None:
            months = os.environ.get("TAXI_MONTHS", "2024-01")
            status.update(detail=f"chargement NYC Taxi ({months}) — ~1 min au 1er boot")
            if "/opt/airflow" not in sys.path:
                sys.path.insert(0, "/opt/airflow")
            import loader.load_taxi as taxi  # noqa: PLC0415

            paths = taxi.download()
            taxi.load(paths)

    status.update(state="ready", detail="lakehouse pret")


def _stream_once() -> None:
    with _conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT to_regclass('silver.transactions')")
        if cur.fetchone()[0] is None:
            return
        cur.execute(STREAM_SQL)


def start() -> None:
    def _run() -> None:
        try:
            ensure_ready()
            _stream_once()
        except Exception as exc:  # noqa: BLE001 - l'API doit vivre meme si le setup echoue
            status.update(state="error", detail=f"{exc}")
            traceback.print_exc()
            return

        def _loop() -> None:
            while True:
                threading.Event().wait(STREAM_INTERVAL_SECONDS)
                try:
                    _stream_once()
                except Exception:  # noqa: BLE001 - jamais fatal
                    traceback.print_exc()

        threading.Thread(target=_loop, name="lakehouse-stream", daemon=True).start()

    threading.Thread(target=_run, name="lakehouse-init", daemon=True).start()
