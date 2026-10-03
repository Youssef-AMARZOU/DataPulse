-- Jeu de données de démonstration + tables de résultats qualité.
-- Exécuté automatiquement au premier démarrage de postgres-lakehouse.

CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.transactions (
    transaction_id BIGINT       PRIMARY KEY,
    customer_id    BIGINT       NOT NULL,
    amount         NUMERIC(12,2) NOT NULL,
    status         TEXT         NOT NULL CHECK (status IN ('completed', 'pending', 'refunded')),
    event_ts       TIMESTAMPTZ  NOT NULL
);

-- 60 lignes récentes (fraîcheur < 24h) pour que le contrat passe au premier run.
INSERT INTO silver.transactions (transaction_id, customer_id, amount, status, event_ts)
SELECT
    g,
    1000 + (g % 50),
    round((random() * 500 + 1)::numeric, 2),
    (ARRAY['completed', 'pending', 'refunded'])[1 + (g % 3)],
    now() - (g || ' minutes')::interval
FROM generate_series(1, 60) AS g;

-- Historique local des vérifications (source de vérité au-delà des 14 jours de Grafana Cloud).
CREATE TABLE IF NOT EXISTS dq_results (
    id          BIGSERIAL PRIMARY KEY,
    dataset     TEXT        NOT NULL,
    contract    TEXT        NOT NULL,
    outcome     TEXT        NOT NULL, -- pass | warn | fail | error
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

-- Journal des publications Gold : une ligne n'est insérée que si le contrat passe.
CREATE TABLE IF NOT EXISTS gold_publications (
    id          BIGSERIAL PRIMARY KEY,
    dataset     TEXT        NOT NULL,
    published_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
