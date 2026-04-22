# Analytics Search — End-to-End Quickstart

Minimal guide to run the new `POST /_analytics/search` endpoint against a local
cluster with the DataFusion backend wired in.

## Prerequisites
- JDK 25+ installed. Sandbox modules require JDK 25.
- Set **`RUNTIME_JAVA_HOME`** (not just `JAVA_HOME`) to the JDK 25 install.
  `JAVA_HOME` can still point to JDK 21 — OpenSearch only uses `RUNTIME_JAVA_HOME`
  for compile/test. Without it you'll see
  `error: release version 25 not supported`.
- `-Dsandbox.enabled=true` on every Gradle invocation (sandbox is off by default).

```bash
export RUNTIME_JAVA_HOME=/Users/you/Library/Java/JavaVirtualMachines/corretto-25.0.2/Contents/Home
```

## Run a local node

```bash
RUNTIME_JAVA_HOME=<jdk25> ./gradlew run \
  -Dsandbox.enabled=true \
  -PinstalledPlugins='["analytics-engine","analytics-backend-datafusion","parquet-data-format","composite-engine"]'
```

Node listens on `http://localhost:9200`.

## Create a parquet-backed index

# 1. Create index
curl -s -XPUT 'http://localhost:9200/demo' -H 'Content-Type: application/json' -d '{
"settings": {
"index.number_of_shards": 1,
"index.number_of_replicas": 0,
"index.pluggable.dataformat.enabled": true,
"index.pluggable.dataformat": "composite",
"index.composite.primary_data_format": "parquet",
"index.composite.secondary_data_formats": []
},
"mappings": {
"properties": {
"msg":   { "type": "keyword" },
"count": { "type": "integer" }
}
}
}'

# 2. Index docs
curl -s -XPOST 'http://localhost:9200/demo/_doc' -H 'Content-Type: application/json' -d '{"msg":"hello","count":1}'
curl -s -XPOST 'http://localhost:9200/demo/_doc' -H 'Content-Type: application/json' -d '{"msg":"world","count":2}'
curl -s -XPOST 'http://localhost:9200/demo/_doc' -H 'Content-Type: application/json' -d '{"msg":"hello","count":3}'

# 3. Refresh
curl -s -XPOST 'http://localhost:9200/demo/_refresh'

# 4. Run SQL queries via the new endpoint
curl -s -XPOST 'http://localhost:9200/_analytics/search' -H 'Content-Type: application/json' -d '{"index":"demo","sql":"SELECT msg, count FROM demo"}'

curl -s -XPOST 'http://localhost:9200/_analytics/search' -H 'Content-Type: application/json' -d '{"index":"demo","sql":"SELECT SUM(count) AS total FROM demo"}'

curl -s -XPOST 'http://localhost:9200/_analytics/search' -H 'Content-Type: application/json' -d '{"index":"demo","sql":"SELECT msg FROM demo WHERE count > 1"}'

curl -s -XPOST 'http://localhost:9200/_analytics/search' -H 'Content-Type: application/json' -d '{"index":"demo","sql":"SELECT msg, COUNT(*) AS n FROM demo GROUP BY msg"}'

## Notes
- The SQL string is compiled to Substrait by the first registered backend
  (`AnalyticsSearchBackendPlugin.compileSql`). The DataFusion backend calls
  `NativeBridge.sqlToSubstrait` internally using the acquired shard reader.
- Current prototype executes on the first local shard only — no cross-node
  routing. Run on a single-node cluster for now.
- Execution is forked to the `SEARCH` threadpool
  (`SEARCH_THROTTLED` for throttled indices, `SYSTEM_READ` for system indices),
  matching the `SearchService` pattern.

## Run the IT

```bash
RUNTIME_JAVA_HOME=<jdk25> ./gradlew \
  :sandbox:plugins:analytics-engine:internalClusterTest \
  --tests "*.AnalyticsSearchActionIT" \
  -Dsandbox.enabled=true
```

The IT spins up a real cluster with `AnalyticsPlugin`, creates an index, and
drives the action via `client().execute(...)` — proves the REST → transport →
SEARCH threadpool → `AnalyticsSearchService` wiring is correct.
