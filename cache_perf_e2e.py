#!/usr/bin/env python3
"""
E2E Cache Performance Test Script

Prerequisites:
  - OpenSearch node running with:
    ./gradlew run -Dsandbox.enabled=true \
      -Dtests.jvm.argline='-Dopensearch.experimental.feature.pluggable.dataformat.enabled=true' \
      -PinstalledPlugins='["analytics-engine","analytics-backend-datafusion","analytics-backend-lucene","parquet-data-format","composite-engine"]'

Usage:
  python3 cache_perf_e2e.py
"""

import json
import random
import string
import subprocess
import sys
import time
import os

BASE_URL = "http://localhost:9200"
INDEX_NAME = "cache_perf_test1"
RANDOM_SEED = 42
NUM_COLUMNS_INT = 20
NUM_COLUMNS_KW = 10
NUM_DOCS = 500000
BATCH_SIZE = 5000
NUM_QUERIES = 20
NUM_WARMUP_QUERIES = 0
CLEAR_CACHE_MODE = "--no-cache" in sys.argv  # Run with: python3 cache_perf_e2e.py --no-cache
LOG_FILE_PATTERN = "build/testclusters/runTask-0/logs/opensearch.stdout.log"


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


def timed_query(index, sql):
    start = time.time()
    cmd = ["curl", "-s", "-XPOST", f"{BASE_URL}/_analytics/search",
           "-H", "Content-Type: application/json",
           "-d", json.dumps({"index": index, "sql": sql})]
    r = subprocess.run(cmd, capture_output=True, text=True)
    elapsed_ms = (time.time() - start) * 1000
    return elapsed_ms, r.stdout


def check_node():
    try:
        r = subprocess.run(["curl", "-s", BASE_URL], capture_output=True, text=True, timeout=5)
        return "cluster_name" in r.stdout
    except:
        return False


def main():
    print("=" * 70)
    print("  E2E Cache Performance Test")
    print("=" * 70)
    print()
    random.seed(RANDOM_SEED)

    # Check node is running
    if not check_node():
        print("ERROR: OpenSearch node not running at", BASE_URL)
        print("Start it with:")
        print("  ./gradlew run -Dsandbox.enabled=true \\")
        print("    -Dtests.jvm.argline='-Dopensearch.experimental.feature.pluggable.dataformat.enabled=true' \\")
        print("    -PinstalledPlugins='[\"analytics-engine\",\"analytics-backend-datafusion\",\"analytics-backend-lucene\",\"parquet-data-format\",\"composite-engine\"]'")
        sys.exit(1)
    print("[OK] Node is running")

    # Delete index if exists
    curl_json("DELETE", f"/{INDEX_NAME}")
    time.sleep(1)

    # Create index
    print(f"\n--- Creating index '{INDEX_NAME}' ({NUM_COLUMNS_INT} int + {NUM_COLUMNS_KW} keyword columns) ---")
    props = {}
    for i in range(NUM_COLUMNS_INT):
        props[f"int_col_{i}"] = {"type": "integer"}
    for i in range(NUM_COLUMNS_KW):
        props[f"kw_col_{i}"] = {"type": "keyword"}

    result = curl_json("PUT", f"/{INDEX_NAME}", {
        "settings": {
            "index.number_of_shards": 1,
            "index.number_of_replicas": 0,
            "index.pluggable.dataformat.enabled": True,
            "index.pluggable.dataformat": "composite",
            "index.composite.primary_data_format": "parquet",
            "index.composite.secondary_data_formats": []
        },
        "mappings": {"properties": props}
    })
    print(f"  Index created: {result.get('acknowledged', False)}")

    # Generate and bulk insert
    print(f"\n--- Bulk inserting {NUM_DOCS} documents ---")
    num_batches = NUM_DOCS // BATCH_SIZE
    for batch in range(num_batches):
        lines = []
        for _ in range(BATCH_SIZE):
            lines.append(json.dumps({"index": {"_index": INDEX_NAME}}))
            doc = {}
            for j in range(NUM_COLUMNS_INT):
                doc[f"int_col_{j}"] = random.randint(0, 1000000)
            for j in range(NUM_COLUMNS_KW):
                doc[f"kw_col_{j}"] = "".join(random.choices(string.ascii_lowercase, k=10))
            lines.append(json.dumps(doc))

        tmpfile = f"/tmp/cache_perf_batch_{batch}.ndjson"
        with open(tmpfile, "w") as f:
            f.write("\n".join(lines) + "\n")

        result = curl_ndjson("/_bulk", tmpfile)
        errors = result.get("errors", "unknown")
        items = len(result.get("items", []))
        print(f"  Batch {batch + 1}/{num_batches}: {items} docs, errors={errors}")

    # Refresh
    print("\n--- Refreshing ---")
    curl_json("POST", f"/{INDEX_NAME}/_refresh")
    time.sleep(2)
    print("  Refresh done")

    # Check parquet files
    print("\n--- Parquet files on disk ---")
    parquet_files = []
    for root, dirs, files in os.walk("build/testclusters/runTask-0/data"):
        for f in files:
            if f.endswith(".parquet") and INDEX_NAME.replace("_", "") not in root:
                pass
            if f.endswith(".parquet"):
                full = os.path.join(root, f)
                size = os.path.getsize(full)
                parquet_files.append((f, size))
                print(f"  {f}: {size / 1024:.0f} KB")
    print(f"  Total: {len(parquet_files)} files")

    # Check cache wiring logs
    print("\n--- Cache logs (chronological, Java + Rust) ---")
    log_path = LOG_FILE_PATTERN
    if os.path.exists(log_path):
        with open(log_path) as f:
            for line in f:
                if ("CACHE WIRING" in line or "CACHE INIT" in line or "CACHE STATS" in line) and "didRefresh=false" not in line:
                    print(f"  [JAVA] {line.strip()[:1000]}")
                elif "RustLoggerBridge" in line and ("CACHE" in line or "QUERY" in line):
                    print(f"  [RUST] {line.strip()[:1000]}")
    else:
        print(f"  Log file not found: {log_path}")

    # Remove the separate Rust section since it's now merged above
    
    # Measured queries
    print(f"\n--- Queries ({NUM_QUERIES} runs) ---")
    sql = f"SELECT int_col_0, int_col_1, kw_col_0 FROM {INDEX_NAME} WHERE int_col_0 > 500000"
    timings = []
    for i in range(NUM_QUERIES):
        ms, _ = timed_query(INDEX_NAME, sql)
        timings.append(ms)
        print(f"  Query {i + 1}: {ms:.0f} ms")

    # Report
    avg = sum(timings) / len(timings)
    print()
    print("=" * 70)
    print("  RESULTS")
    print("=" * 70)
    print(f"  Index:          {INDEX_NAME}")
    print(f"  Schema:         {NUM_COLUMNS_INT + NUM_COLUMNS_KW} columns ({NUM_COLUMNS_INT} int + {NUM_COLUMNS_KW} keyword)")
    print(f"  Documents:      {NUM_DOCS}")
    print(f"  Parquet files:  {len(parquet_files)}")
    print(f"  Query:          {sql}")
    print()
    print(f"  Avg latency:    {avg:.0f} ms")
    print(f"  Min latency:    {min(timings):.0f} ms")
    print(f"  Max latency:    {max(timings):.0f} ms")
    print()

    # Check Rust cache logs after queries
    print("--- Rust cache logs after queries ---")
    if os.path.exists(log_path):
        meta_hits = 0
        meta_misses = 0
        stats_hits = 0
        stats_misses = 0
        prewarm_count = 0
        timing_lines = []
        with open(log_path) as f:
            for line in f:
                if "METADATA CACHE HIT" in line:
                    meta_hits += 1
                if "METADATA CACHE MISS" in line:
                    meta_misses += 1
                if "STATISTICS CACHE HIT" in line:
                    stats_hits += 1
                if "STATISTICS CACHE MISS" in line:
                    stats_misses += 1
                if "CACHE PRE-WARM" in line:
                    prewarm_count += 1
                if "QUERY TIMING" in line:
                    timing_lines.append(line.strip())
        print(f"  Pre-warm calls:          {prewarm_count}")
        print(f"  Metadata cache HITs:     {meta_hits}")
        print(f"  Metadata cache MISSes:   {meta_misses}")
        print(f"  Statistics cache HITs:    {stats_hits}")
        print(f"  Statistics cache MISSes:  {stats_misses}")
        total = meta_hits + meta_misses + stats_hits + stats_misses
        total_hits = meta_hits + stats_hits
        if total > 0:
            print(f"  Overall hit rate:        {total_hits / total * 100:.1f}%")
        print()
        if timing_lines:
            print("--- Query timing breakdown (first 2 queries) ---")
            shown = 0
            for line in timing_lines:
                if shown < 12:
                    # Extract just the timing part
                    idx = line.find("[QUERY TIMING]")
                    if idx >= 0:
                        print(f"  {line[idx:]}")
                        shown += 1

            # Compute averages per phase across ALL queries
            import re
            phase_times = {}
            for line in timing_lines:
                m = re.search(r'\[QUERY TIMING\] (.+?) took (\d+) µs', line)
                if m:
                    phase = m.group(1)
                    us = int(m.group(2))
                    phase_times.setdefault(phase, []).append(us)

            if phase_times:
                print()
                print("--- Average Rust-side timing per phase (all queries) ---")
                for phase, times in phase_times.items():
                    avg_us = sum(times) / len(times)
                    print(f"  {phase}: avg={avg_us:.0f} µs  (n={len(times)}, min={min(times)}, max={max(times)})")

            # Per-query breakdown table (6 phases per query)
            phases_per_query = 6
            num_queries_logged = len(timing_lines) // phases_per_query
            if num_queries_logged > 0:
                print()
                print(f"--- Per-query Rust-side timing (all {num_queries_logged} queries, µs) ---")
                print(f"  {'Q':>3}  {'s2s:infer':>10} {'s2s:reg':>8} {'s2s:plan':>9} {'s2s:enc':>8} {'eq:infer':>9} {'eq:plan':>8} {'total':>8}")
                print(f"  {'---':>3}  {'--------':>10} {'-------':>8} {'--------':>9} {'-------':>8} {'--------':>9} {'-------':>8} {'-------':>8}")
                all_totals = []
                for q in range(num_queries_logged):
                    vals = []
                    for i in range(phases_per_query):
                        line = timing_lines[q * phases_per_query + i]
                        m = re.search(r'took (\d+) µs', line)
                        vals.append(int(m.group(1)) if m else 0)
                    total = sum(vals)
                    all_totals.append(total)
                    print(f"  {q+1:>3}  {vals[0]:>10} {vals[1]:>8} {vals[2]:>9} {vals[3]:>8} {vals[4]:>9} {vals[5]:>8} {total:>8}")
                avg_total = sum(all_totals) / len(all_totals)
                avg_excl_q1 = sum(all_totals[1:]) / len(all_totals[1:]) if len(all_totals) > 1 else 0
                print(f"  {'avg':>3}  {'':>10} {'':>8} {'':>9} {'':>8} {'':>9} {'':>8} {avg_total:>8.0f}")
                print(f"  {'avg(Q2+)':>8} {'':>5} {'':>8} {'':>9} {'':>8} {'':>9} {'':>8} {avg_excl_q1:>8.0f}")
    print()


if __name__ == "__main__":
    main()
