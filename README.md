# Data Quality Observability POC

[![quality-gate](https://github.com/Youssef-AMARZOU/DataPulse/actions/workflows/quality-gate.yml/badge.svg)](https://github.com/Youssef-AMARZOU/DataPulse/actions/workflows/quality-gate.yml)

POC de contrôle de qualité de données : contrats **Soda Core v4** exécutés par un **DAG Airflow 3.3.2**, résultats stockés en Postgres et métriques poussées en **OTLP vers Grafana Cloud** (palier Free), avec un dashboard importable et une **démo web live** (FastAPI + UI) — utilisable en local (Docker) ou **hébergée gratuitement** (voir section démo hébergée).

## Stack

| Composant | Rôle |
|-----------|------|
| Airflow 3.3.2 (Docker, image custom `dq-poc-airflow:3.3.2`) | Orchestration, quality gate Silver → Gold |
| Soda Core 4.25 (`soda-postgres`) | Vérification des contrats de données (format v4 Data Contract) |
| Postgres 16 `postgres-lakehouse` (port 5433) | Jeu de démo Silver + tables `dq_results`, `dq_metrics`, `gold_publications` |
| **FastAPI + Uvicorn (`demo`, port 8000)** | **API de démo + UI : statut live, historique, injection/réparation de données, relance de check** |
| OpenTelemetry SDK + exporter OTLP/HTTP | Jauge `dq_contract_passed` envoyée à Grafana Cloud |
| Grafana Cloud Free (externe) | Dashboard + alertes (rétention 14 j) |

## Arborescence

```
Data Quality Observability POC/
├── docker-compose.yml          # compose officiel Airflow 3.3.2 adapté (+ postgres-lakehouse, + demo)
├── Dockerfile                  # apache/airflow:3.3.2-python3.12 + soda + otel + fastapi
├── requirements.txt
├── .env.example                # modèle (copier vers .env) — jamais de secrets réels
├── config/
│   ├── ds_config.yml           # data source Soda (vars ${env.*})
│   └── postgres_init/001_init.sql   # seed silver.transactions + tables de résultats
├── contracts/silver_transactions.yml
├── contracts/nyc_taxi.yml       # contrat sur ~20 M lignes TLC réelles (2024-01..06)
├── dags/dq_lib.py               # helpers partagés (connexions, run_soda, OTel)
├── dags/dq_silver_transactions.py
├── dags/dq_nyc_taxi.py          # quality gate sur silver.nyc_taxi (cron 30 */6)
├── loader/load_taxi.py          # télécharge les Parquet TLC et charge silver.nyc_taxi (DuckDB → Postgres)
├── demo/                       # backend FastAPI + UI de démo (port 8000)
│   ├── app.py
│   └── static/index.html
└── grafana/dq_dashboard.json   # dashboard à importer (variable DS_PROMETHEUS)
```

## Démarrage rapide

Prérequis : Docker Desktop (testé : Docker 29.6.2 / Compose v5.3.1).

```powershell
cp .env.example .env        # puis renseigner OTEL_* si envoy vers Grafana Cloud (optionnel)
docker compose up -d
```

- UI Airflow : http://localhost:8080 (airflow/airflow par défaut)
- **Démo web : http://localhost:8000** (voir section suivante)
- Lakehouse : `localhost:5433` (soda/soda, base `lakehouse`)
- Le DAG `dq_silver_transactions` (cron `*/15 * * * *`) et `dq_nyc_taxi` (cron `30 */6 * * *`) sont créés **paused** : les dépauser dans l'UI ou :

```powershell
docker compose run --rm --no-deps airflow-cli dags unpause dq_silver_transactions
docker compose run --rm --no-deps airflow-cli dags unpause dq_nyc_taxi
docker compose run --rm --no-deps airflow-cli dags trigger dq_silver_transactions
```

Contrôle manuel du contrat (hors Airflow) :

```powershell
docker compose run --rm --no-deps --entrypoint soda airflow-worker `
  contract verify -ds /opt/airflow/config/ds_config.yml -c /opt/airflow/contracts/silver_transactions.yml
```

### Jeu de données NYC Taxi (~20 M lignes)

Le service `loader` (profile Compose `loader`) télécharge les 6 fichiers Parquet Yellow Taxi
2024-01 à 2024-06 depuis `d37ci6vzurychx.cloudfront.net` dans `/tmp/taxi`, puis charge
`silver.nyc_taxi` via **DuckDB** (extension `postgres`) en un seul `CREATE TABLE ... AS SELECT`
sans index (économie d'espace) :

```powershell
docker compose run -d --name dq-loader loader    # ~350 Mo téléchargés + chargement complet
docker logs -f dq-loader                          # progression (Python non bufferisé ? utiliser docker exec)
```

La progression se lit aussi dans Postgres : `SELECT tuples_processed FROM pg_stat_progress_copy;`.

Arrêt : `docker compose down` (ajouter `-v` pour supprimer les volumes).

## Démo hébergée (free tier, zéro localhost)

Deux fichiers rendent la démo accessible par un lien public, sans Docker chez le visiteur :

- **`Dockerfile.demo`** — image légère (Python + Soda + FastAPI, **sans Airflow**, ~400 Mo) ; l'API initialise la base au premier boot (seed des 60 transactions + chargement TLC selon `TAXI_MONTHS`, défaut `2024-01` ≈ 3 M lignes pour tenir dans les plans gratuits).
- **`render.yaml`** — blueprint Render (service web free) ; l'état d'initialisation est exposé dans `/api/summary` → champ `init`.

### Mise en place (une fois, ~15 min)

1. **BDD gratuite — Neon** (Postgres managé, tier free) : créer un projet, noter host/port/db/user/password (SSL requis).
2. **GitHub — secrets du quality gate** : `Settings → Secrets and variables → Actions` puis créer
   `LAKEHOUSE_HOST`, `LAKEHOUSE_PORT` (5432), `LAKEHOUSE_DB`, `LAKEHOUSE_USER`, `LAKEHOUSE_PASSWORD` (les valeurs Neon).
   Le workflow **quality-gate** tourne alors toutes les 15 min : il simule un flux (+4 lignes/run, la fraicheur reste < 24 h),
   vérifie les deux contrats (warn = vert, fail = croix) et alimente le badge en haut de ce README.
3. **Render — déploiement** : `New + → Blueprint`, pointer le dépôt GitHub ; le `render.yaml` crée le service ;
   dans ses env vars renseigner `LAKEHOUSE_HOST/DB/USER/PASSWORD` (valeurs Neon, `LAKEHOUSE_SSLMODE` est déjà `require`).
4. **URLs** : démo `https://datapulse-demo.onrender.com` (1ᵉʳ boot ~1-2 min : seed + 3 M lignes TLC), badge Actions dans le README.

Notes : le plan free met le service en veille après inactivité (1ᵉʳ appel ~50 s, comme le cold start local) ;
le contrat taxi hébergé est `nyc_taxi_demo.yml` (`row_count ≥ 2 500 000`, via l'env `TAXI_CONTRACT`) — le contrat
local `nyc_taxi.yml` garde son seuil 15 M pour les 6 mois. En local tout continue de fonctionner sans rien changer
(`startup` est un no-op quand les tables existent déjà).

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

## Démo web live (port 8000)

Service `demo` (FastAPI, même image que Airflow → Soda disponible) avec une UI sombre auto-rafraîchie (5 s) et un **sélecteur de jeu de données** :

- **transactions** (`silver.transactions`, 60 lignes) — scénario pédagogique complet
- **nyc_taxi** (`silver.nyc_taxi`, ~20 M lignes TLC 2024-01..06) — check réel (~1 min), inject/fix désactivés

Par jeu de données :

- **Statut** du dernier check (PASS/FAIL, exit code, horodatage) et état du lakehouse (lignes totales / hors contrat, publications Gold, checks exécutés)
- **Historique** des checks (table)
- **Boutons de scénario de démo** (transactions uniquement) :
  1. *Lancer le check* → exécute `soda contract verify` en direct (transactions ~5-7 s ; taxi ~1 min), journal en sortie
  2. *Injecter des données invalides* → insère une ligne à montant négatif
  3. *Relancer le check* → **FAIL** (le gate passe au rouge)
  4. *Réparer les données* → supprime les lignes invalides, le check repasse **PASS**

Endpoints API (OpenAPI sur `/docs`) — tous acceptent `?dataset=transactions|taxi` :

| Méthode | Route | Rôle |
|---------|-------|------|
| GET | `/api/summary?dataset=` | Dernier check + compteurs lakehouse (table absente → `null`) |
| GET | `/api/history?dataset=&limit=20` | Historique `dq_results` |
| POST | `/api/check?dataset=` | Exécute le contrat Soda et enregistre le résultat |
| POST | `/api/inject` | Insère une ligne invalide (montant négatif) — transactions |
| POST | `/api/fix` | Supprime les lignes invalides — transactions |

Notes : le service écrit dans les mêmes tables que le DAG (duplicata volontaire pour que la démo soit autonome) ; aucune authentification (POC local, ne pas exposer le port 8000).

## Fonctionnement

Les deux DAGs (`dq_silver_transactions`, `dq_nyc_taxi`) partagent `dags/dq_lib.py` (deux tâches TaskFlow) :

1. **`verify_contract`** — exécute `soda contract verify`, écrit le résultat dans `dq_results`/`dq_metrics` (Postgres), pousse la jauge OTLP (import + création + flush validés ; un échec d'export est loggé et n'échoue **jamais** la tâche), puis **échoue la tâche** si le contrat échoue.
2. **`publish_gold`** — n'exécute que si la vérification passe (gate Airflow `upstream_failed` sinon) et journalise dans `gold_publications`.

Codes de sortie Soda v4 : `0` = pass, `1` = fail, `2` = warn (n'arrête pas le pipeline), `>= 3` = error (bloquant).

Contrat (`contracts/silver_transactions.yml`) sur `lakehouse/lakehouse/silver/transactions` : `row_count`, `freshness < 24 h`, `schema`, `missing`, `duplicate`, `invalid` (montant ≥ 0, statut dans la liste).

Contrat (`contracts/nyc_taxi.yml`) sur `lakehouse/lakehouse/silver/nyc_taxi` — **21 checks** dérivés du
[Data Dictionary officiel NYC TLC](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf) :

- **Hard (bloquants, 19)** : `schema` (20 colonnes dans l'ordre), `row_count ≥ 15 M`, `freshness loaded_at < 14 j`,
  domaines officiels vérifiés à 0 violation sur les 20,3 M lignes (`vendor_id ∈ [1,2,6,7]`, `rate_code_id ∈ [1..6,99]`,
  `store_and_fwd_flag ∈ [Y,N]`, `payment_type ∈ [0..6]`, zones `pulocationid/dolocationid ∈ [1,265]`,
  `passenger_count ∈ [0,9]`, `trip_distance ≥ 0`) + plages largees sur les montants (outliers réels jusqu'à 334 076).
- **Warn (non bloquants, 2)** — règles du dictionnaire que les données réelles violent, en `failed_rows` + `level: warn`
  (exit 2, le gate reste vert) : cohérence temporelle (`dropoff < pickup` → 300 lignes) et non-négativité des montants
  (remboursements/ajustements → 316 784 lignes). Chaque check partage un `qualifier` unique (sinon Soda erreur
  `Duplicate identity`).

Un contrat en WARN sort en exit 2 : la tâche `verify_contract` reste en success, `publish_gold` s'exécute, et
`dq_results` enregistre `outcome = warn` (affiché en ambre dans la démo).

## Vérifié le [2026-10-03]

- `docker compose build` et `docker compose up -d` : 8/8 conteneurs healthy.
- `soda contract verify` : 8/8 checks PASSED (exit 0).
- Exécution du DAG (manuel + schedule) : `verify_contract` = success, `publish_gold` = success ; lignes présentes dans `dq_results`, `dq_metrics`, `gold_publications`.
- Chemin échec : montant négatif inséré → `verify_contract` = failed (exit 1, `contract_passed = 0`), `publish_gold` = `upstream_failed` (non exécuté). Ligne supprimée après le test.
- Code OTel (import, `create_gauge`, `set`, `force_flush`, `shutdown`) exécuté dans le conteneur : OK, l'échec d'export est loggé sans exception.
- **Démo web** (service `demo`, port 8000) : healthy, UI 200, cycle complet `check pass → inject → check fail (exit 1) → fix → check pass` via l'API ; durées de check ~6 s après réchauffement (50 s au premier lancer = démarrage à froid).

**Non testé** : réception réelle des métriques par Grafana Cloud (aucun endpoint/token dans l'environnement), import du dashboard et création des alertes dans Grafana, exécution prolongée (cron sur plusieurs cycles).

## Vérifié le [2026-10-04]

- **Reconstruction complète** de l'environnement (reset Docker Desktop en cours de session : conteneurs/volumes images perdus, le code étant safe sur git) : `docker compose build` + `up -d` → 9/9 conteneurs healthy, seed régénéré.
- **Chargement NYC Taxi** : service `loader` — 6 Parquet TLC 2024-01..06 téléchargés (~350 Mo), `silver.nyc_taxi` créée avec **20 332 093 lignes en 66 s** (DuckDB → extension `postgres`, un seul CTAS sans index).
- **Calibrage du contrat sur les stats réelles** : outliers extrêmes présents (fare_amount -1 285 → 334 076 ; trip_distance jusqu'à 312 722 ; payment_type inclut 0 = ~1,97 M lignes ; vendor_id ∈ {1,2,6}) — seuils élargis pour refléter la réalité TLC (`fare/total ∈ [-2 000 ; 400 000]`, `trip_distance ∈ [0 ; 400 000]`, `payment_type ∈ 0..5`).
- **Bug loader corrigé** : `pulocationid/dolocationid/airport_fee` étaient créés en casse mixte (DuckDB conservait la casse source sans alias `AS`) → colonnes renommées dans Postgres + alias explicites ajoutés dans `loader/load_taxi.py`.
- `soda contract verify` taxi : **14/14 checks PASSED (exit 0)** en ~6 s malgré les 20 M de lignes.
- **Contrat enrichi depuis le Data Dictionary TLC** (après vérification empirique sur les 20,3 M) : 21 checks —
  domaines officiels ajoutés en hard (`vendor_id`, `rate_code_id`, `store_and_fwd_flag`, zones `[1,265]`,
  `payment_type 0..6` — tous à 0 violation) ; non-négativité des montants et `dropoff ≥ pickup` ajoutés en
  **warn non-bloquant** (`failed_rows` + `level: warn`, `qualifier` unique obligatoire) car violés par les
  données réelles (316 784 / 300 lignes) → résultat **19 pass / 2 warn / 0 fail, exit 2**.
- **Le warn ne casse pas le pipeline** (testé de bout en bout) : run manuel `dq_nyc_taxi` = success,
  `dq_results.outcome = warn`, `gold_publications` alimenté, démo API `passed=true` (affichage ambre).
- **DAG `dq_nyc_taxi`** : dépause + run manuel **success** (`verify_contract` + `publish_gold`, `gold_publications` et `dq_metrics` peuplés pour le dataset taxi). Le run schedule du 03:05 a échoué (table absente avant chargement — artefact attendu, désormais le cron `30 */6` repasse en success).
- **Démo multi-datasets** : `?dataset=taxi` sur `/api/summary` (20 332 093 lignes, 0 hors contrat), `/api/history`, `POST /api/check` → pass en **5,6 s** ; UI 200 ; cycle transactions `inject → check fail (exit 1) → fix → check pass` revalidé après refactor ; table absente renvoyée `null` au lieu d'erreur 500.
- Les deux DAGs parsent sans erreur d'import (`dags list` : `dq_silver_transactions`, `dq_nyc_taxi`).

## Vérifié le [2026-10-05] — socle démo hébergée

- **`Dockerfile.demo`** construit (429 Mo) et testé de bout en bout contre le lakehouse local :
  `/api/summary` (init ready, seed + flux simulé), `POST /api/check` transactions → **pass en 1,4 s**,
  taxi avec `nyc_taxi_demo.yml` → **warn exit 2 en 25 s**, UI HTTP 200.
- **sslmode** : `ds_config.yml` enrichi de `sslmode: ${env.LAKEHOUSE_SSLMODE}` — vérifié : contrat
  transactions **8/8 pass** avec le nouveau champ (défaut `prefer` en local, `require` sur Neon).
- **Flux simulé** : la fraîcheur du seed expirait après 24 h (cron en fail permanent) →
  `demo/startup.py` insère 4 lignes / 15 min (+ au boot), idem dans le workflow Actions ;
  fraîcheur repassée en vert après insertion.
- **`TAXI_MONTHS`** : parsing validé (`2024-01` → `['2024-01']`) ; défaut 6 mois conservé en local.
- **Pièges documentés** : `${env.*}` n'est **pas résolu dans les seuils des contrats Soda** → contrat
  dédié `nyc_taxi_demo.yml` ; le paquet PyPI v4 s'appelle **`soda-postgres`** (`soda-core-postgres`
  plafonne à 3.5.6) ; le port 8001 est déjà pris par un autre projet local (`ppe`).
- **Non testé ici** (nécessite des comptes) : déploiement réel Render + base Neon, secrets Actions,
  premier boot complet sur base vierge (le code de démarrage est écrit et le no-op est testé en local).

## Notes

- **Licences** : Soda Core est sous licence ELv2 (usage interne / service propre autorisé ; redistri bloqué — ne pas empaqueter Soda dans un produit distribué). Airflow Apache 2.0, Postgres PostgreSQL.
- **Conflits de dépendances** : soda-core exige `ruamel.yaml<0.18` et `requests<2.34`, alors que les constraints officielles Airflow 3.3.2 pinlent `0.19.1` / `2.34.2` — le `Dockerfile` installe donc **sans constraints** (l'image de base a déjà Airflow épinglé). Seul `google-cloud-aiplatform` (extras inactifs) consomme ruamel.
- L'API Soda v3 (`scan.get_scan_results()["checks"]`) est obsolète : ce POC passe par la CLI v4.
- `.env` est dans `.gitignore` ; ne jamais committer de token.
