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
import urllib.request
from collections import defaultdict
from pathlib import Path

import duckdb


def fetch_latest_overture_release() -> str:
    url = "https://stac.overturemaps.org/catalog.json"
    try:
        with urllib.request.urlopen(url, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            latest = data.get("latest")
            if latest:
                return str(latest)
    except Exception as exc:
        print(f"Aviso: falha ao consultar {url} ({exc}); usando fallback 2026-11-18.0")
    return "2026-11-18.0"


def map_overture_category(raw: str | None) -> tuple[str, int]:
    text = (raw or "").lower()
    if any(k in text for k in ("office", "coworking", "business", "corporate", "factory", "industrial")):
        return "WORK", 80
    if any(k in text for k in ("gym", "fitness", "sports", "crossfit", "pilates", "yoga", "stadium")):
        return "GYM", 75
    if any(k in text for k in ("restaurant", "cafe", "coffee", "bakery", "bar", "pub", "pizza", "burger", "food", "bistro")):
        return "RESTAURANT", 60
    if any(k in text for k in ("supermarket", "grocery", "market", "convenience", "butcher")):
        return "MARKET", 75
    if any(k in text for k in ("hospital", "clinic", "doctor", "dentist", "pharmacy", "medical", "health")):
        return "HEALTH", 80
    if any(k in text for k in ("school", "university", "college", "library", "education", "training")):
        return "EDUCATION", 90
    if any(k in text for k in ("mall", "shopping", "store", "shop", "retail", "clothing", "electronics")):
        return "SHOPPING", 85
    if any(k in text for k in ("station", "airport", "bus", "metro", "subway", "train", "terminal", "parking", "gas_station")):
        return "TRANSPORT", 85
    if any(k in text for k in ("park", "cinema", "theater", "museum", "hotel", "beach", "club", "entertainment")):
        return "LEISURE", 90
    return "OTHER", 70


def load_ibge_cities(csv_path: Path):
    cities = []
    with csv_path.open("r", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row or len(row) < 5 or row[0] == "codigo_ibge":
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

    release = fetch_latest_overture_release()
    print(f"Usando release Overture Maps: {release}")

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; INSTALL httpfs; LOAD httpfs; SET s3_region='us-west-2';")

    parquet_glob = f"s3://overturemaps-us-west-2/release/{release}/theme=places/type=place/*"
    query = f"""
        SELECT
            id,
            names.primary AS name,
            ST_Y(geometry) AS latitude,
            ST_X(geometry) AS longitude,
            coalesce(categories.primary, '') AS cat_label
        FROM read_parquet('{parquet_glob}', hive_partitioning=1)
        WHERE bbox.xmin BETWEEN -74.5 AND -34.0
          AND bbox.ymin BETWEEN -34.0 AND 5.5
          AND addresses[1].country = 'BR'
          AND names.primary IS NOT NULL
          AND geometry IS NOT NULL
    """

    grouped = defaultdict(list)
    cursor = con.execute(query)
    while True:
        batch = cursor.fetchmany(50_000)
        if not batch:
            break
        for place_id, name, lat, lon, cat_label in batch:
            code = nearest_ibge_code(float(lat), float(lon), cities_by_grid)
            if not code:
                continue
            cat_type, radius = map_overture_category(cat_label)
            grouped[code].append({
                "id": str(place_id),
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
