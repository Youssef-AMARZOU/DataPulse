"""Charge 6 mois de NYC TLC yellow taxi (2024-01 .. 2024-06) dans silver.nyc_taxi.

Usage :  docker compose run --rm loader

- Telecharge les Parquet depuis le CDN TLC (~450 Mo total, une fois).
- Copie vers Postgres via l'extension duckdb `postgres` (une passe, rapide).
- Colonne loaded_at : base des checks de fraicheur du contrat.
"""

import os
import sys
import time
import urllib.request
from pathlib import Path

import duckdb

# Mois a charger : 6 mois par defaut (~20,3 M lignes) ; TAXI_MONTHS="2024-01"
# pour un jeu reduit compatible free tiers (~3 M lignes, ~350 Mo).
MONTHS = [
    m.strip()
    for m in os.environ.get("TAXI_MONTHS", "2024-01,2024-02,2024-03,2024-04,2024-05,2024-06").split(",")
    if m.strip()
]
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{}.parquet"
CACHE_DIR = Path(os.environ.get("TAXI_CACHE_DIR", "/tmp/taxi"))

# Noms TLC -> noms en minuscules (stables pour le contrat Soda).
COLUMNS = """
    "VendorID"                        AS vendor_id,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    "RatecodeID"                      AS rate_code_id,
    store_and_fwd_flag,
    "PULocationID"                    AS pulocationid,
    "DOLocationID"                    AS dolocationid,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    "Airport_fee"                     AS airport_fee,
    current_timestamp                 AS loaded_at
"""


def download() -> list[str]:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    paths = []
    for month in MONTHS:
        dest = CACHE_DIR / f"yellow_tripdata_{month}.parquet"
        if dest.exists() and dest.stat().st_size > 1_000_000:
            print(f"[skip] {dest.name} deja present ({dest.stat().st_size // 1_048_576} Mo)")
            paths.append(str(dest))
            continue
        url = BASE_URL.format(month)
        print(f"[dl]   {url}")
        t0 = time.monotonic()
        with urllib.request.urlopen(url, timeout=120) as resp, open(dest, "wb") as fh:
            total = int(resp.headers.get("Content-Length") or 0)
            done = 0
            while True:
                chunk = resp.read(1 << 20)
                if not chunk:
                    break
                fh.write(chunk)
                done += len(chunk)
                if total:
                    print(f"\r       {done // 1_048_576}/{total // 1_048_576} Mo", end="")
            print()
        print(f"[ok]   {dest.name} en {time.monotonic() - t0:.1f}s")
        paths.append(str(dest))
    return paths


def load(paths: list[str]) -> None:
    host = os.environ.get("LAKEHOUSE_HOST", "postgres-lakehouse")
    port = os.environ.get("LAKEHOUSE_PORT", "5432")
    db = os.environ.get("LAKEHOUSE_DB", "lakehouse")
    user = os.environ.get("LAKEHOUSE_USER", "soda")
    password = os.environ.get("LAKEHOUSE_PASSWORD", "soda")

    con = duckdb.connect()
    print("[duckdb] installation de l'extension postgres...")
    con.execute("INSTALL postgres")
    con.execute("LOAD postgres")
    con.execute(
        f"ATTACH 'host={host} port={port} dbname={db} user={user} password={password}' "
        "AS lh (TYPE postgres)"
    )

    glob = str(CACHE_DIR / "yellow_tripdata_*.parquet")
    print(f"[load] {glob} -> lh.silver.nyc_taxi")
    t0 = time.monotonic()
    con.execute("DROP TABLE IF EXISTS lh.silver.nyc_taxi")
    con.execute(
        f"""
        CREATE TABLE lh.silver.nyc_taxi AS
        SELECT {COLUMNS}
        FROM read_parquet('{glob}')
        """
    )
    n = con.execute("SELECT count(*) FROM lh.silver.nyc_taxi").fetchone()[0]
    print(f"[load] {n:,} lignes en {time.monotonic() - t0:.1f}s")
    con.execute("DETACH lh")
    con.close()
    print("[done] chargement termine")


if __name__ == "__main__":
    try:
        files = download()
        load(files)
    except Exception as exc:  # noqa: BLE001
        print(f"[error] {exc}", file=sys.stderr)
        sys.exit(1)
