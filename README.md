# Data Quality Observability POC

POC de contrôle de qualité de données : contrats **Soda Core v4** exécutés par un **DAG Airflow 3.3.2**, résultats stockés en Postgres et métriques poussées en **OTLP vers Grafana Cloud** (palier Free), avec un dashboard importable.

## Stack

| Composant | Rôle |
|-----------|------|
| Airflow 3.3.2 (Docker, image custom `dq-poc-airflow:3.3.2`) | Orchestration, quality gate Silver → Gold |
| Soda Core 4.25 (`soda-postgres`) | Vérification des contrats de données (format v4 Data Contract) |
| Postgres 16 `postgres-lakehouse` (port 5433) | Jeu de démo Silver + tables `dq_results`, `dq_metrics`, `gold_publications` |
| OpenTelemetry SDK + exporter OTLP/HTTP | Jauge `dq_contract_passed` envoyée à Grafana Cloud |
| Grafana Cloud Free (externe) | Dashboard + alertes (rétention 14 j) |

## Arborescence

```
Data Quality Observability POC/
├── docker-compose.yml          # compose officiel Airflow 3.3.2 adapté (+ postgres-lakehouse)
├── Dockerfile                  # apache/airflow:3.3.2-python3.12 + soda + otel
├── requirements.txt
├── .env.example                # modèle (copier vers .env) — jamais de secrets réels
├── config/
│   ├── ds_config.yml           # data source Soda (vars ${env.*})
│   └── postgres_init/001_init.sql   # seed silver.transactions + tables de résultats
├── contracts/silver_transactions.yml
├── dags/dq_silver_transactions.py
└── grafana/dq_dashboard.json   # dashboard à importer (variable DS_PROMETHEUS)
```

## Démarrage rapide

Prérequis : Docker Desktop (testé : Docker 29.6.2 / Compose v5.3.1).

```powershell
cp .env.example .env        # puis renseigner OTEL_* si envoy vers Grafana Cloud (optionnel)
docker compose up -d
```

- UI Airflow : http://localhost:8080 (airflow/airflow par défaut)
- Lakehouse : `localhost:5433` (soda/soda, base `lakehouse`)
- Le DAG `dq_silver_transactions` est créé **paused** (cron `*/15 * * * *`) : le dépauser dans l'UI ou :

```powershell
docker compose run --rm airflow-cli dags unpause dq_silver_transactions
docker compose run --rm airflow-cli dags trigger dq_silver_transactions
```

Contrôle manuel du contrat (hors Airflow) :

```powershell
docker compose run --rm --no-deps --entrypoint soda airflow-worker `
  contract verify -ds /opt/airflow/config/ds_config.yml -c /opt/airflow/contracts/silver_transactions.yml
```

Arrêt : `docker compose down` (ajouter `-v` pour supprimer les volumes).

## Grafana Cloud (OTLP)

1. Grafana Cloud → **Connections → Add new connection → OpenTelemetry (OTLP)** et noter l'endpoint `https://otlp-gateway-<region>.grafana.net/otlp`.
2. Créer un token d'instance (Explorer du projet), puis générer l'en-tête **sans retour à la ligne** :
   - Linux/macOS : `printf '%s' "<instance-id>:<glc_token>" | base64 | tr -d '\n'`
   - PowerShell : `[Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes("<instance-id>:<glc_token>"))`
3. Renseigner dans `.env` :
   ```
   OTEL_EXPORTER_OTLP_ENDPOINT=https://otlp-gateway-<region>.grafana.net/otlp
   OTEL_EXPORTER_OTLP_HEADERS=Authorization=Basic%20<base64>
   ```
4. `docker compose up -d` (redémarrage des services Airflow pour recharger l'env).
5. Importer `grafana/dq_dashboard.json` (choisir le datasource Prometheus de l'instance) et créer deux alertes :
   - Contrat en échec : `dq_contract_passed == 0`
   - Pipeline de contrôle silencieusement cassé : `absent_over_time(dq_contract_passed{dataset="lakehouse.silver.transactions"}[1h])`

Si `OTEL_EXPORTER_OTLP_ENDPOINT` est vide, l'export des métriques est simplement ignoré (log dans la tâche) ; le contrôle de qualité reste fonctionnel.

## Fonctionnement

DAG `dq_silver_transactions` (deux tâches TaskFlow) :

1. **`verify_contract`** — exécute `soda contract verify`, écrit le résultat dans `dq_results`/`dq_metrics` (Postgres), pousse la jauge OTLP (import + création + flush validés ; un échec d'export est loggé et n'échoue **jamais** la tâche), puis **échoue la tâche** si le contrat échoue.
2. **`publish_gold`** — n'exécute que si la vérification passe (gate Airflow `upstream_failed` sinon) et journalise dans `gold_publications`.

Codes de sortie Soda v4 : `0` = pass, `1` = fail, `2` = warn (n'arrête pas le pipeline), `>= 3` = error (bloquant).

Contrat (`contracts/silver_transactions.yml`) sur `lakehouse/lakehouse/silver/transactions` : `row_count`, `freshness < 24 h`, `schema`, `missing`, `duplicate`, `invalid` (montant ≥ 0, statut dans la liste).

## Vérifié le [2026-10-03]

- `docker compose build` et `docker compose up -d` : 8/8 conteneurs healthy.
- `soda contract verify` : 8/8 checks PASSED (exit 0).
- Exécution du DAG (manuel + schedule) : `verify_contract` = success, `publish_gold` = success ; lignes présentes dans `dq_results`, `dq_metrics`, `gold_publications`.
- Chemin échec : montant négatif inséré → `verify_contract` = failed (exit 1, `contract_passed = 0`), `publish_gold` = `upstream_failed` (non exécuté). Ligne supprimée après le test.
- Code OTel (import, `create_gauge`, `set`, `force_flush`, `shutdown`) exécuté dans le conteneur : OK, l'échec d'export est loggé sans exception.

**Non testé** : réception réelle des métriques par Grafana Cloud (aucun endpoint/token dans l'environnement), import du dashboard et création des alertes dans Grafana, exécution prolongée (cron sur plusieurs cycles).

## Notes

- **Licences** : Soda Core est sous licence ELv2 (usage interne / service propre autorisé ; redistri bloqué — ne pas empaqueter Soda dans un produit distribué). Airflow Apache 2.0, Postgres PostgreSQL.
- **Conflits de dépendances** : soda-core exige `ruamel.yaml<0.18` et `requests<2.34`, alors que les constraints officielles Airflow 3.3.2 pinlent `0.19.1` / `2.34.2` — le `Dockerfile` installe donc **sans constraints** (l'image de base a déjà Airflow épinglé). Seul `google-cloud-aiplatform` (extras inactifs) consomme ruamel.
- L'API Soda v3 (`scan.get_scan_results()["checks"]`) est obsolète : ce POC passe par la CLI v4.
- `.env` est dans `.gitignore` ; ne jamais committer de token.
