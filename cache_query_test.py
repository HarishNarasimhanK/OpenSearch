#!/usr/bin/env python3
"""
Cache query test — runs a query N times, measures latency, reports cache stats from NEW log lines only.
"""

import json
import os
import subprocess
import time

BASE_URL = "http://localhost:9200"
INDEX = "hits"
SQL = 'SELECT COUNT(*) FROM hits WHERE "RegionID" > 100'
NUM_QUERIES = 20
LOG_FILE = "build/testclusters/runTask-0/logs/opensearch.stdout.log"


def query(sql):
    start = time.time()
    cmd = ["curl", "-s", "-XPOST", f"{BASE_URL}/_analytics/search",
           "-H", "Content-Type: application/json",
           "-d", json.dumps({"index": INDEX, "sql": sql})]
    r = subprocess.run(cmd, capture_output=True, text=True)
    ms = (time.time() - start) * 1000
    return ms, r.stdout


def count_lines():
    if not os.path.exists(LOG_FILE):
        return 0
    with open(LOG_FILE) as f:
        return sum(1 for _ in f)


def count_cache_in_lines(skip_lines):
    counts = {"meta_hits": 0, "meta_misses": 0, "stats_hits": 0, "stats_misses": 0}
    with open(LOG_FILE) as f:
        for i, line in enumerate(f):
            if i < skip_lines:
                continue
            if "METADATA CACHE HIT" in line: counts["meta_hits"] += 1
            if "METADATA CACHE MISS" in line: counts["meta_misses"] += 1
            if "STATISTICS CACHE HIT" in line: counts["stats_hits"] += 1
            if "STATISTICS CACHE MISS" in line: counts["stats_misses"] += 1
    return counts


def main():
    print(f"Query: {SQL}")
    print(f"Index: {INDEX}")
    print(f"Runs:  {NUM_QUERIES}")
    print()

    # Count lines before queries
    lines_before = count_lines()
    print(f"Log lines before queries: {lines_before}")

    # Run queries
    print(f"\n--- Queries ({NUM_QUERIES} runs) ---")
    timings = []
    for i in range(NUM_QUERIES):
        ms, _ = query(SQL)
        timings.append(ms)
        print(f"  Q{i+1:>2}: {ms:.0f} ms")

    time.sleep(2)

    lines_after = count_lines()
    new_lines = lines_after - lines_before
    print(f"\nNew log lines after queries: {new_lines}")

    # Count cache patterns only in new lines
    counts = count_cache_in_lines(lines_before)

    print()
    print(f"  Avg:      {sum(timings)/len(timings):.0f} ms")
    print(f"  Min:      {min(timings):.0f} ms")
    print(f"  Max:      {max(timings):.0f} ms")
    if len(timings) > 1:
        print(f"  Avg(Q2+): {sum(timings[1:])/len(timings[1:]):.0f} ms")
    print()

    print("--- Cache stats (this run only) ---")
    for key in ["meta_hits", "meta_misses", "stats_hits", "stats_misses"]:
        print(f"  {key}: {counts[key]}")
    total_hits = counts["meta_hits"] + counts["stats_hits"]
    total_all = total_hits + counts["meta_misses"] + counts["stats_misses"]
    if total_all > 0:
        print(f"  hit_rate: {total_hits/total_all*100:.1f}%")
    else:
        print("  (no cache activity detected in new log lines)")


if __name__ == "__main__":
    main()
