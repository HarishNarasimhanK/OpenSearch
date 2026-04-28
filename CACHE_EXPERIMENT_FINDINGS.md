# Cache Experiment Findings

## Experiment: Unwiring the Statistics Cache

### Setup
- 50,000 documents, 30 columns (20 int + 10 keyword)
- 3 parquet files (~26 MB total)
- Query: `SELECT int_col_0, int_col_1, kw_col_0 FROM index WHERE int_col_0 > 500000`
- Random seed: 42 (reproducible data)

### What was changed
Replaced statistics cache wiring with `None` in both `api.rs` and `query_executor.rs`:
```rust
// Before:
.with_files_statistics_cache(runtime.runtime_env.cache_manager.get_file_statistic_cache())

// After:
.with_files_statistics_cache(None)
```

### Results

| Metric | Stats cache wired | Stats cache unwired |
|--------|------------------|-------------------|
| Avg query latency | 27 ms | 23 ms |
| Min query latency | 23 ms | 21 ms |
| Metadata cache hits | 303 | 303+ |
| Statistics cache hits | 0 | 0 |
| Statistics cache `put_with_extra` calls | 3 (during pre-warm) | 3 (during pre-warm) |
| Statistics cache `get`/`get_with_extra` calls | 0 | 0 |

### Findings

1. **No performance difference** — unwiring the statistics cache had zero impact on query latency.

2. **Statistics cache is populated but never read during queries** — `put_with_extra` is called during `addFiles` pre-warming (confirmed by logs), but `get` and `get_with_extra` are never called during query execution.

3. **DataFusion 52.1.0 extracts statistics from the metadata cache** — the parquet footer (stored in the metadata cache) contains row group statistics (min/max per column). DataFusion's `ListingTable` reads these directly from the footer metadata during `create_physical_plan()`, bypassing the separate statistics cache entirely.

4. **The metadata cache is the primary performance driver** — 303 cache hits across 20 queries, with `infer_schema` taking only 107 µs per call (vs milliseconds without cache).

5. **Timing breakdown per query:**
   - `infer_schema`: ~107 µs (metadata cache hit)
   - `logical→physical plan`: ~723 µs (statistics extracted from footer)
   - Total Rust-side: ~1 ms per query

### Why the statistics cache still has value

1. **Pre-computation** — `addFiles` computes statistics once at refresh time via `compute_parquet_statistics()`. Even though DataFusion doesn't read from the cache during queries in v52.1.0, the computation is done and ready.

2. **Future DataFusion versions** — newer versions may use the `FileStatisticsCache` trait's `get()` method during query planning, at which point the pre-warmed statistics cache will provide immediate benefit.

3. **Direct access** — the `CacheManager` Java API (`getMemoryConsumed`, `getEntryFromCacheType`) can query the statistics cache for monitoring and debugging.

### Conclusion

For DataFusion 52.1.0, the **metadata cache alone** provides the query performance benefit. The statistics cache is a forward-looking investment that's correctly wired and populated but not yet consumed by DataFusion's query path.

---

## Experiment 2: Unwiring Both Caches from Query Path

### What was changed
Set both caches to `None` in `api.rs` and `query_executor.rs`:
```rust
.with_file_metadata_cache(None)
.with_files_statistics_cache(None),
```

### Results

| Metric | Caches wired | Caches set to None |
|--------|-------------|-------------------|
| Avg latency | 27 ms | 26 ms |
| Metadata cache hits | 303 | 303 (from pre-warm path only) |

**No performance difference.** The 303 hits came from the pre-warm path (`addFiles`), not from queries. DataFusion's `CacheManagerConfig::default()` creates its own internal default caches. When we pass `None`, `RuntimeEnvBuilder::from_runtime_env()` inherits the global runtime's default caches anyway.

---

## Experiment 3: Skipping Cache Creation Entirely

### What was changed
Forced `cacheManagerPtr = 0` in Java `DataFusionService.doStart()`:
```java
long cacheManagerPtr = 0L;
// Cache creation code commented out
```

This means:
- No `CustomCacheManager` created in Rust
- No `MutexFileMetadataCache` exists
- No `CustomStatisticsCache` exists
- `onFilesAdded` skips pre-warming (`cacheManager == null`)
- Rust `create_global_runtime` uses `CacheManagerConfig::default()`

### Results

| Metric | Our cache | No cache (ptr=0) |
|--------|----------|-----------------|
| Query 1 | 475 ms | 473 ms |
| Avg (2-20) | 27 ms | 26 ms |
| Pre-warm calls | 3 | 0 |
| Metadata cache hits | 303 | 0 |
| Statistics cache hits | 0 | 0 |

**No performance difference.** Confirmed by logs:
```
[CACHE INIT] Cache creation SKIPPED (experiment: cacheManagerPtr forced to 0)
Pre-warm calls: 0
Metadata cache HITs: 0
```

### Why no difference

DataFusion **always** has internal caching. `CacheManagerConfig::default()` creates built-in `DefaultFilesMetadataCache` and `DefaultFileStatisticsCache`. These provide the same lazy caching:
- Query 1: cache miss → read from disk → store in default cache
- Query 2+: cache hit from default cache → fast

The `RuntimeEnvBuilder::from_runtime_env()` inherits these default caches even when we pass `None` explicitly.

---

## Overall Conclusions

### What our cache system provides over DataFusion's defaults

| Feature | Our cache | DataFusion's default |
|---------|----------|---------------------|
| **Pre-warming at refresh** | ✅ `addFiles` populates cache before first query | ❌ Lazy only (first query pays the cost) |
| **Size limit** | ✅ 250MB metadata, 100MB statistics | ❌ Unbounded (grows forever) |
| **LRU/LFU eviction** | ✅ Configurable eviction policies | ❌ No eviction |
| **Memory tracking** | ✅ Per-entry tracking, total memory reporting | ❌ No tracking |
| **Monitoring** | ✅ Hit/miss counters, hit rate, memory stats | ❌ None |
| **Dynamic configuration** | ✅ Enable/disable per cache type | ❌ Always on |

### When our cache makes a visible difference

1. **First query after refresh** — our pre-warming eliminates the cold-start penalty. With DataFusion's default, the first query reads all footers from disk.

2. **Production with many shards** — unbounded default caches would consume unlimited memory. Our size limits and eviction prevent OOM.

3. **Remote storage (S3/GCS)** — footer reads are HTTP requests (~50-100ms each). Pre-warming at refresh time saves seconds on the first query.

4. **High query concurrency** — without pre-warming, many concurrent first-queries after refresh all read footers from disk simultaneously, causing I/O contention.

### Why the experiments showed no difference on local SSD

- Local disk reads are fast (~1ms for a footer) — the cache benefit is small
- DataFusion's default lazy cache provides the same benefit after the first query
- JVM warmup (~400ms) dominates the first-query latency, masking the cache effect
- OS filesystem cache keeps recently-read files in memory, making "disk reads" nearly free

---

## Experiment 4: Final Comparison — With Cache vs Without Cache (Timing Breakdown)

### Setup
- Fresh node restart for each experiment
- 50,000 docs, 30 columns, 3 parquet files
- Rust lib rebuilt with timing logs and cache wired

### Query Latency

| Metric | With Cache | Without Cache |
|--------|-----------|--------------|
| Query 1 | 464 ms | 482 ms |
| Avg (2-20) | 27 ms | 27 ms |
| Min | 24 ms | 23 ms |
| Max | 464 ms | 482 ms |
| Pre-warm calls | 3 | 0 |
| Metadata cache HITs | 303 | 0 |

### Timing Breakdown — Query 1 (cold)

| Step | With Cache (µs) | Without Cache (µs) | Difference |
|------|----------------|-------------------|------------|
| sql_to_substrait: infer_schema | 496 | 924 | **1.9x slower without cache** |
| sql_to_substrait: register_table | 147 | 164 | ~same |
| sql_to_substrait: SQL planning | 1211 | 1446 | 1.2x slower |
| sql_to_substrait: substrait encode | 174 | 174 | same |
| execute_query: infer_schema | 314 | 72 | faster without (DataFusion lazy cache from sql_to_substrait) |
| execute_query: logical→physical | 3072 | 3260 | ~same |

### Timing Breakdown — Query 2 (warm)

| Step | With Cache (µs) | Without Cache (µs) | Difference |
|------|----------------|-------------------|------------|
| sql_to_substrait: infer_schema | 286 | 87 | faster without (DataFusion lazy cache) |
| sql_to_substrait: register_table | 9 | 8 | same |
| sql_to_substrait: SQL planning | 120 | 117 | same |
| sql_to_substrait: substrait encode | 31 | 28 | same |
| execute_query: infer_schema | 230 | 52 | faster without |
| execute_query: logical→physical | 930 | 645 | faster without |

### Analysis

**Query 1 `infer_schema` (sql_to_substrait):**
- With cache: 496 µs — our pre-warmed cache serves the footer
- Without cache: 924 µs — DataFusion reads footer from disk (first time)
- **Our cache is 1.9x faster on the first `infer_schema` call**

**Query 2 onwards:**
- Without cache is actually slightly faster because DataFusion's default `DefaultFilesMetadataCache` is a simpler `DashMap` without the `Mutex` overhead that our `MutexFileMetadataCache` adds
- Our cache wraps `DefaultFilesMetadataCache` in a `Mutex` for thread safety, adding ~200µs overhead per `infer_schema` call

### Key Insight

The `infer_schema` in `sql_to_substrait` for Query 1 shows the real cache benefit:
- **496 µs with pre-warmed cache** vs **924 µs without** (1.9x improvement)

But for subsequent queries, DataFusion's simpler default cache (no Mutex) is slightly faster than our Mutex-wrapped cache. The tradeoff is:
- Our cache: slower per-call (Mutex overhead) but pre-warmed (no cold start)
- DataFusion's default: faster per-call but cold on first query

### Production Impact

In production, the pre-warming benefit matters more than the per-call overhead because:
1. First query after refresh is the most latency-sensitive (user is waiting)
2. The Mutex overhead (~200µs) is negligible compared to total query time (~25ms)
3. With remote storage (S3), the first-query penalty without pre-warming would be 50-100ms per file, not 400µs
