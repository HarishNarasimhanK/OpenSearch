# Cache Testing Guide

## Prerequisites

- JDK 25+
- On the `cache-e2e-perf-test` branch
- Bharathwaj's single-shard-exec commit cherry-picked

## 1. Build the Rust Native Library

```bash
cd OpenSearch/sandbox/libs/dataformat-native/rust
cargo clean -p opensearch-datafusion
cargo clean -p opensearch-native-lib
cargo build --release
```

Verify logs are compiled in:
```bash
strings target/release/libopensearch_native.dylib | grep "CACHE PRE-WARM"
```

## 2. Start the Node

```bash
cd OpenSearch
./gradlew run -Dsandbox.enabled=true \
  -Dtests.jvm.argline='-Dopensearch.experimental.feature.pluggable.dataformat.enabled=true' \
  -PinstalledPlugins='["analytics-engine","analytics-backend-datafusion","analytics-backend-lucene","parquet-data-format","composite-engine"]'
```

## 3. Run the Performance Test

```bash
cd OpenSearch
python3 cache_perf_e2e.py
```

## 4. Check Logs Manually

```bash
# All cache-related logs
grep -E "CACHE PRE-WARM|METADATA CACHE|STATISTICS CACHE|QUERY PATH|CACHE CREATE|CACHE INIT|CACHE WIRING|CACHE STATS" \
  build/testclusters/runTask-0/logs/opensearch.stdout.log

# Just Rust-side logs
grep "RustLoggerBridge" build/testclusters/runTask-0/logs/opensearch.stdout.log | grep -E "CACHE|QUERY"

# Just pre-warm events
grep "CACHE PRE-WARM" build/testclusters/runTask-0/logs/opensearch.stdout.log

# Just query-time cache hits/misses
grep -E "METADATA CACHE HIT|METADATA CACHE MISS" build/testclusters/runTask-0/logs/opensearch.stdout.log
```

## 5. Test with Cache Disabled (via settings)

Stop the node, restart with cache disabled:

```bash
./gradlew run -Dsandbox.enabled=true \
  -Dtests.jvm.argline='-Dopensearch.experimental.feature.pluggable.dataformat.enabled=true' \
  -Dtests.opensearch.datafusion.metadata.cache.enabled=false \
  -Dtests.opensearch.datafusion.statistics.cache.enabled=false \
  -PinstalledPlugins='["analytics-engine","analytics-backend-datafusion","analytics-backend-lucene","parquet-data-format","composite-engine"]'
```

Then run `python3 cache_perf_e2e.py` and compare results.

## 6. Test with Cache Unwired (code change)

To completely remove cache from the query path, edit two Rust files:

### File 1: `sandbox/plugins/analytics-backend-datafusion/rust/src/api.rs`

In `sql_to_substrait()`, find:
```rust
.with_file_metadata_cache(Some(
    runtime.runtime_env.cache_manager.get_file_metadata_cache(),
))
.with_files_statistics_cache(
    runtime.runtime_env.cache_manager.get_file_statistic_cache(),
),
```

Replace with:
```rust
.with_file_metadata_cache(None)
.with_files_statistics_cache(None),
```

### File 2: `sandbox/plugins/analytics-backend-datafusion/rust/src/query_executor.rs`

In `execute_query()`, find the same block and replace with:
```rust
.with_file_metadata_cache(None)
.with_files_statistics_cache(None),
```

### Rebuild and test:
```bash
cd sandbox/libs/dataformat-native/rust
cargo clean -p opensearch-datafusion
cargo clean -p opensearch-native-lib
cargo build --release
cd ../../../..
# Restart node and run python3 cache_perf_e2e.py
```

Expected: 0 metadata cache hits, higher latency on all queries.

## 7. Quick Manual Test (small data)

```bash
# Create index
curl -s -XPUT 'http://localhost:9200/demo' -H 'Content-Type: application/json' -d '{
  "settings": {"index.number_of_shards":1,"index.number_of_replicas":0,"index.pluggable.dataformat.enabled":true,"index.pluggable.dataformat":"composite","index.composite.primary_data_format":"parquet","index.composite.secondary_data_formats":[]},
  "mappings": {"properties": {"msg":{"type":"keyword"},"val":{"type":"integer"}}}
}'

# Index docs
curl -s -XPOST 'http://localhost:9200/demo/_doc' -H 'Content-Type: application/json' -d '{"msg":"hello","val":1}'
curl -s -XPOST 'http://localhost:9200/demo/_doc' -H 'Content-Type: application/json' -d '{"msg":"world","val":2}'
curl -s -XPOST 'http://localhost:9200/demo/_doc' -H 'Content-Type: application/json' -d '{"msg":"hello","val":3}'

# Refresh
curl -s -XPOST 'http://localhost:9200/demo/_refresh'
sleep 3

# Query
curl -s -XPOST 'http://localhost:9200/_analytics/search' -H 'Content-Type: application/json' \
  -d '{"index":"demo","sql":"SELECT msg, val FROM demo"}'

# Check cache logs
grep -E "CACHE PRE-WARM|METADATA CACHE|STATISTICS CACHE|QUERY PATH" \
  build/testclusters/runTask-0/logs/opensearch.stdout.log
```

## Expected Log Flow

```
[CACHE INIT] Starting DataFusion service
[CACHE INIT] Cache manager created with ptr=...
  ↓ (index created, docs indexed, refresh happens)
[CACHE WIRING] afterRefresh called, didRefresh=true
[CACHE WIRING] onFilesAdded called with 1 files: [...]
[CACHE WIRING] DataFusionService.onFilesAdded called with 1 files: [...]
[CACHE PRE-WARM] add_files called with 1 files: [...]        ← Rust
[METADATA CACHE MISS] .../generation_1.parquet                ← Rust (first read from disk)
[STATISTICS CACHE] put_with_extra called for: ...             ← Rust (stats computed and stored)
[METADATA CACHE HIT] .../generation_1.parquet                 ← Rust (verification)
[CACHE WIRING] NativeBridge.cacheManagerAddFiles completed
[CACHE STATS] After addFiles: metadata=X bytes, statistics=Y bytes
  ↓ (query arrives)
[QUERY PATH] sql_to_substrait: caches wired into per-query RuntimeEnv
[METADATA CACHE HIT] .../generation_1.parquet                 ← Rust (query uses cached footer)
[QUERY PATH] sql_to_substrait: infer_schema completed
[QUERY PATH] sql_to_substrait: SQL planned
[METADATA CACHE HIT] .../generation_1.parquet                 ← Rust (execution uses cached footer)
[QUERY PATH] execute_query: infer_schema completed
[QUERY PATH] execute_query: physical plan created
```
