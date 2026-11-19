"""
Pipeline completo para gerar pacotes city_<ibge>.json.gz de todos os municipios do Brasil
a partir do Foursquare Open Source Places (Apache 2.0) via DuckDB, pronto para upload no Cloudflare R2.

Requisitos:
    pip install duckdb

Uso:
    python generate_all_brazil_city_packs.py --cities ibge_cities_br.csv --out-dir dist --version 2026-11
"""

import argparse
import csv
import gzip
import json
import math
from collections import defaultdict
from pathlib import Path
import duckdb


def map_fsq_category(cat_labels: str) -> tuple[str, int]:
    t = (cat_labels or "").lower()
    if any(k in t for k in ("gym", "fitness", "yoga", "pilates", "crossfit")):
        return "GYM", 60
    if any(k in t for k in ("supermarket", "grocery", "market", "convenience")):
        return "SUPERMARKET", 60
    if any(k in t for k in ("park", "plaza", "square", "garden", "playground")):
        return "PARK", 200
    if any(k in t for k in ("university", "college")):
        return "UNIVERSITY", 200
    if any(k in t for k in ("school", "language", "academy")):
        return "SCHOOL", 80
    if "bakery" in t:
        return "BAKERY", 50
    if any(k in t for k in ("pharmacy", "drugstore")):
        return "PHARMACY", 50
    if any(k in t for k in ("cafe", "coffee")):
        return "CAFE", 50
    if any(k in t for k in ("restaurant", "diner", "pizzeria", "burger", "food")):
        return "RESTAURANT", 50
    if any(k in t for k in ("hospital", "clinic", "medical", "dentist", "doctor")):
        return "HOSPITAL", 90
    if any(k in t for k in ("mall", "department store", "shopping", "bookstore")):
        return "SHOPPING", 180
    if any(k in t for k in ("bank", "atm")):
        return "BANK", 50
    if "pet" in t:
        return "PET_SHOP", 50
    if any(k in t for k in ("post office", "government", "service")):
        return "SERVICES", 60
    return "OTHER", 60


def load_ibge_cities(csv_path: Path):
    cities = []
    with csv_path.open("r", encoding="utf-8") as fh:
        for row in csv.reader(fh):
            if len(row) < 5:
                continue
            cities.append((row[0].strip(), float(row[3]), float(row[4])))
    return cities


def nearest_ibge_code(lat: float, lon: float, cities_by_grid: dict) -> str | None:
    gx, gy = int(math.floor(lat)), int(math.floor(lon))
    best_code = None
    best_dist = 1e18
    for dx in (-1, 0, 1):
        for dy in (-1, 0, 1):
            for code, clat, clon in cities_by_grid.get((gx + dx, gy + dy), ()):
                d = (lat - clat) ** 2 + (lon - clon) ** 2
                if d < best_dist:
                    best_dist = d
                    best_code = code
    return best_code


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cities", required=True, type=Path)
    parser.add_argument("--out-dir", required=True, type=Path)
    parser.add_argument("--version", default="2026-11")
    args = parser.parse_args()

    cities = load_ibge_cities(args.cities)
    cities_by_grid = defaultdict(list)
    for code, clat, clon in cities:
        cities_by_grid[(int(math.floor(clat)), int(math.floor(clon)))].append((code, clat, clon))

    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs; SET s3_region='us-east-1';")

    query = """
        SELECT
            fsq_place_id,
            name,
            latitude,
            longitude,
            array_to_string(fsq_category_labels, ' ') AS cat_labels
        FROM read_parquet('s3://fsq-os-places-us-east-1/release/dt=*/places/parquet/*.parquet')
        WHERE country = 'BR'
          AND date_closed IS NULL
          AND name IS NOT NULL
          AND latitude IS NOT NULL
          AND longitude IS NOT NULL
    """

    grouped = defaultdict(list)
    cursor = con.execute(query)
    while True:
        batch = cursor.fetchmany(50_000)
        if not batch:
            break
        for fsq_id, name, lat, lon, cat_labels in batch:
            code = nearest_ibge_code(float(lat), float(lon), cities_by_grid)
            if not code:
                continue
            cat_type, radius = map_fsq_category(cat_labels)
            grouped[code].append({
                "id": str(fsq_id),
                "n": str(name).strip(),
                "la": round(float(lat), 5),
                "lo": round(float(lon), 5),
                "t": cat_type,
                "r": radius,
            })

    args.out_dir.mkdir(parents=True, exist_ok=True)
    for code, pois in grouped.items():
        payload = json.dumps({"version": args.version, "ibge": code, "pois": pois}, ensure_ascii=False).encode("utf-8")
        out_file = args.out_dir / f"city_{code}.json.gz"
        with gzip.open(out_file, "wb") as gz:
            gz.write(payload)

    print(f"Concluido: {len(grouped)} municipios gerados em {args.out_dir}.")


if __name__ == "__main__":
    main()
