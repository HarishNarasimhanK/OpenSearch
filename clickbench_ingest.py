#!/usr/bin/env python3
"""
Ingest ClickBench hits.parquet into local OpenSearch, creating exactly N parquet files.

Each cycle: ingest a batch of rows → refresh → creates 1 parquet file.
Repeat N times = N parquet files.

Usage:
  # Download hits.parquet first (~15GB):
  # wget https://datasets.clickhouse.com/hits_compatible/hits.parquet

  # Ingest 1M rows across 100 parquet files (10K rows per file):
  python3 clickbench_ingest.py --file hits.parquet --rows 1000000 --files 100

  # Ingest 500K rows across 50 parquet files:
  python3 clickbench_ingest.py --file hits.parquet --rows 500000 --files 50
"""

import argparse
import json
import subprocess
import sys
import time

try:
    import pyarrow.parquet as pq
except ImportError:
    print("pip install pyarrow")
    sys.exit(1)

BASE_URL = "http://localhost:9200"
INDEX_NAME = "hits"
BULK_BATCH_SIZE = 5000

# Subset of ClickBench columns — numeric only for clean mapping
COLUMNS = [
    "WatchID", "JavaEnable", "GoodEvent", "CounterID",
    "ClientIP", "RegionID", "UserID", "CounterClass",
    "OS", "UserAgent", "IsRefresh", "RefererCategoryID",
    "RefererRegionID", "URLCategoryID", "URLRegionID",
    "ResolutionWidth", "ResolutionHeight", "ResolutionDepth",
    "FlashMajor", "FlashMinor", "NetMajor", "NetMinor",
    "Age", "Sex", "Income", "HID",
    "IsOldCounter", "IsDownload", "IsLink", "IsNotBounce", "Interests",
]

MAPPING = {
    "settings": {
        "index.number_of_shards": 1,
        "index.number_of_replicas": 0,
        "index.pluggable.dataformat.enabled": True,
        "index.pluggable.dataformat": "composite",
        "index.composite.primary_data_format": "parquet",
        "index.composite.secondary_data_formats": [],
        "index.refresh_interval": "-1"
    },
    "mappings": {
        "properties": {
            "WatchID": {"type": "long"},
            "JavaEnable": {"type": "short"},
            "GoodEvent": {"type": "short"},
            "CounterID": {"type": "integer"},
            "ClientIP": {"type": "integer"},
            "RegionID": {"type": "integer"},
            "UserID": {"type": "long"},
            "CounterClass": {"type": "short"},
            "OS": {"type": "short"},
            "UserAgent": {"type": "short"},
            "IsRefresh": {"type": "short"},
            "RefererCategoryID": {"type": "short"},
            "RefererRegionID": {"type": "integer"},
            "URLCategoryID": {"type": "short"},
            "URLRegionID": {"type": "integer"},
            "ResolutionWidth": {"type": "integer"},
            "ResolutionHeight": {"type": "integer"},
            "ResolutionDepth": {"type": "short"},
            "FlashMajor": {"type": "short"},
            "FlashMinor": {"type": "short"},
            "NetMajor": {"type": "short"},
            "NetMinor": {"type": "short"},
            "Age": {"type": "short"},
            "Sex": {"type": "short"},
            "Income": {"type": "short"},
            "HID": {"type": "integer"},
            "IsOldCounter": {"type": "short"},
            "IsDownload": {"type": "short"},
            "IsLink": {"type": "short"},
            "IsNotBounce": {"type": "short"},
            "Interests": {"type": "integer"},
        }
    }
}


def curl_json(method, path, body=None):
    cmd = ["curl", "-s", f"-X{method}", f"{BASE_URL}{path}", "-H", "Content-Type: application/json"]
    if body:
        cmd += ["-d", json.dumps(body)]
    r = subprocess.run(cmd, capture_output=True, text=True)
    try:
        return json.loads(r.stdout)
    except:
        return {"raw": r.stdout}


def curl_ndjson(path, filepath):
    cmd = ["curl", "-s", "-XPOST", f"{BASE_URL}{path}", "-H", "Content-Type: application/x-ndjson", "--data-binary", f"@{filepath}"]
    r = subprocess.run(cmd, capture_output=True, text=True)
    try:
        return json.loads(r.stdout)
    except:
        return {"raw": r.stdout}


def ingest_rows(rows_iter, count):
    """Ingest exactly `count` rows from the iterator via bulk API. Returns actual count ingested."""
    lines = []
    ingested = 0
    for row in rows_iter:
        lines.append(json.dumps({"index": {"_index": INDEX_NAME}}))
        doc = {}
        for col in COLUMNS:
            val = row.get(col)
            if val is not None:
                doc[col] = int(val) if isinstance(val, (int, float)) else val
        lines.append(json.dumps(doc))
        ingested += 1

        # Flush in sub-batches to avoid huge payloads
        if ingested % BULK_BATCH_SIZE == 0:
            tmpfile = "/tmp/clickbench_batch.ndjson"
            with open(tmpfile, "w") as f:
                f.write("\n".join(lines) + "\n")
            curl_ndjson("/_bulk", tmpfile)
            lines = []

        if ingested >= count:
            break

    # Flush remaining
    if lines:
        tmpfile = "/tmp/clickbench_batch.ndjson"
        with open(tmpfile, "w") as f:
            f.write("\n".join(lines) + "\n")
        curl_ndjson("/_bulk", tmpfile)

    return ingested


def row_iterator(parquet_file, columns):
    """Yield one dict per row from parquet file, memory efficient."""
    for batch in parquet_file.iter_batches(batch_size=1000, columns=columns):
        table = batch.to_pydict()
        num_rows = len(table[columns[0]])
        for i in range(num_rows):
            yield {col: table[col][i] for col in columns}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True, help="Path to hits.parquet")
    parser.add_argument("--rows", type=int, default=1000000, help="Total rows to ingest")
    parser.add_argument("--files", type=int, default=100, help="Number of parquet files to create")
    args = parser.parse_args()

    rows_per_file = args.rows // args.files

    print(f"Plan: {args.rows} rows across {args.files} parquet files ({rows_per_file} rows/file)")
    print(f"Columns: {len(COLUMNS)}")
    print()

    # --- Warmup: create a toy index, ingest, refresh, query ---
    WARMUP_INDEX = "warmup"
    print("--- Warming up ---")
    time.sleep(1)
    curl_json("PUT", f"/{WARMUP_INDEX}", {
        "settings": {
            "index.number_of_shards": 1,
            "index.number_of_replicas": 0,
            "index.pluggable.dataformat.enabled": True,
            "index.pluggable.dataformat": "composite",
            "index.composite.primary_data_format": "parquet",
            "index.composite.secondary_data_formats": []
        },
        "mappings": {"properties": {"msg": {"type": "keyword"}, "count": {"type": "integer"}}}
    })
    curl_json("POST", f"/{WARMUP_INDEX}/_doc", {"msg": "hello", "count": 1})
    curl_json("POST", f"/{WARMUP_INDEX}/_doc", {"msg": "world", "count": 2})
    curl_json("POST", f"/{WARMUP_INDEX}/_refresh")
    time.sleep(1)
    cmd = ["curl", "-s", "-XPOST", f"{BASE_URL}/_analytics/search",
           "-H", "Content-Type: application/json",
           "-d", json.dumps({"index": WARMUP_INDEX, "sql": f"SELECT msg, count FROM {WARMUP_INDEX}"})]
    subprocess.run(cmd, capture_output=True, text=True)
    subprocess.run(cmd, capture_output=True, text=True)
    print("  Done")
    print()

    # Delete and create index
    print(f"Creating index '{INDEX_NAME}'...")
    curl_json("DELETE", f"/{INDEX_NAME}")
    time.sleep(1)
    result = curl_json("PUT", f"/{INDEX_NAME}", MAPPING)
    print(f"  Created: {result.get('acknowledged', False)}")

    # Open parquet file
    pf = pq.ParquetFile(args.file)
    rows = row_iterator(pf, COLUMNS)

    total_ingested = 0
    t0 = time.time()

    for file_num in range(1, args.files + 1):
        # Ingest rows_per_file rows
        count = ingest_rows(rows, rows_per_file)
        total_ingested += count

        # Refresh to create 1 parquet file
        curl_json("POST", f"/{INDEX_NAME}/_refresh")

        elapsed = time.time() - t0
        rate = total_ingested / elapsed if elapsed > 0 else 0
        print(f"  File {file_num}/{args.files}: +{count} rows, total={total_ingested}, "
              f"{rate:.0f} docs/sec, elapsed={elapsed:.1f}s")

    elapsed = time.time() - t0
    print(f"\nDone: {total_ingested} rows, {args.files} parquet files, {elapsed:.1f}s")
    print(f"Rate: {total_ingested/elapsed:.0f} docs/sec")
    print()
    print("Test queries:")
    print(f'  curl -s -XPOST "{BASE_URL}/_analytics/search" -H "Content-Type: application/json" '
          f'-d \'{{"index": "hits", "sql": "SELECT COUNT(*) FROM hits WHERE RegionID > 100"}}\'')
    print(f'  curl -s -XPOST "{BASE_URL}/_analytics/search" -H "Content-Type: application/json" '
          f'-d \'{{"index": "hits", "sql": "SELECT SUM(ResolutionWidth), AVG(Age) FROM hits WHERE OS > 5"}}\'')


if __name__ == "__main__":
    main()
