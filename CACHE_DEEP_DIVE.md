# DataFusion Cache System — End-to-End Deep Dive

## 1. Overview

The DataFusion cache system stores two types of data in memory to avoid repeated disk I/O:

- **Metadata Cache**: Parquet file footers (schema, row group locations, column/offset indexes)
- **Statistics Cache**: Per-file statistics (row count, column min/max, null count, distinct count)

Without caching, every query reads these from disk. With caching, they're read once and reused across all subsequent queries on the same files.

## 2. Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     OpenSearch Node                              │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ REST Layer                                                │   │
│  │  POST /_analytics/search {index, sql}                     │   │
│  │  → AnalyticsSearchAction.RestAction                       │   │
│  └──────────────┬───────────────────────────────────────────┘   │
│                 │                                                │
│  ┌──────────────▼───────────────────────────────────────────┐   │
│  │ Transport Layer                                           │   │
│  │  AnalyticsSearchAction.TransportAction                    │   │
│  │  → AnalyticsSearchService.executeSql()                    │   │
│  └──────────────┬───────────────────────────────────────────┘   │
│                 │                                                │
│  ┌──────────────▼───────────────────────────────────────────┐   │
│  │ DataFusion Plugin (Java)                                  │   │
│  │  DataFusionPlugin.compileSql()                            │   │
│  │  DataFusionPlugin.getSearchExecEngineProvider()           │   │
│  │  DataFusionService (node-level singleton)                 │   │
│  │  DatafusionReaderManager (per-shard)                      │   │
│  │  CacheManager (Java wrapper for cache operations)         │   │
│  └──────────────┬───────────────────────────────────────────┘   │
│                 │ FFM (Foreign Function & Memory API)            │
│  ┌──────────────▼───────────────────────────────────────────┐   │
│  │ NativeBridge.java → ffm.rs (C ABI)                       │   │
│  └──────────────┬───────────────────────────────────────────┘   │
│                 │                                                │
│  ┌──────────────▼───────────────────────────────────────────┐   │
│  │ Rust Native Layer                                         │   │
│  │  api.rs          — query planning & execution             │   │
│  │  query_executor.rs — production query path                │   │
│  │  cache.rs        — MutexFileMetadataCache                 │   │
│  │  statistics_cache.rs — CustomStatisticsCache              │   │
│  │  custom_cache_manager.rs — orchestrates both caches       │   │
│  │  eviction_policy.rs — LRU/LFU eviction                   │   │
│  │                                                           │   │
│  │  ┌─────────────────────┐  ┌─────────────────────────┐   │   │
│  │  │ Metadata Cache      │  │ Statistics Cache         │   │   │
│  │  │ (parquet footers)   │  │ (column min/max/nulls)   │   │   │
│  │  │ 250MB default       │  │ 100MB default            │   │   │
│  │  └─────────────────────┘  └─────────────────────────┘   │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ Disk: Parquet Files                                       │   │
│  │  /data/indices/{uuid}/0/parquet/segment_1.parquet         │   │
│  │  /data/indices/{uuid}/0/parquet/segment_2.parquet         │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

## 3. Node Startup Flow — How Caches Are Created

When OpenSearch starts, the DataFusion plugin creates the cache system.

### Step 1: DataFusionPlugin.createComponents()
**File**: `DataFusionPlugin.java`

```java
public Collection<Object> createComponents(...) {
    dataFusionService = DataFusionService.builder()
        .memoryPoolLimit(memoryPoolLimit)
        .spillMemoryLimit(spillMemoryLimit)
        .spillDirectory(spillDir)
        .clusterSettings(clusterService.getClusterSettings())  // ← passes settings for cache config
        .build();
    dataFusionService.start();  // ← triggers doStart()
}
```

### Step 2: DataFusionService.doStart()
**File**: `DataFusionService.java`

```java
protected void doStart() {
    NativeBridge.initTokioRuntimeManager(cpuThreads);  // Start Rust async runtime

    long cacheManagerPtr = 0L;
    if (clusterSettings != null) {
        cacheManagerPtr = CacheUtils.createCacheConfig(clusterSettings);  // ← creates caches
    }

    long ptr = NativeBridge.createGlobalRuntime(memoryPoolLimit, cacheManagerPtr, spillDirectory, spillMemoryLimit);
    this.runtimeHandle = new NativeRuntimeHandle(ptr);

    if (clusterSettings != null) {
        this.cacheManager = new CacheManager(runtimeHandle);  // Java wrapper for cache ops
    }
}
```

### Step 3: CacheUtils.createCacheConfig()
**File**: `CacheUtils.java`

```java
public static long createCacheConfig(ClusterSettings clusterSettings) {
    long cacheManagerPtr = NativeBridge.createCustomCacheManager();  // Rust: CustomCacheManager::new()

    for (CacheType type : CacheType.values()) {  // METADATA, STATISTICS
        if (type.isEnabled(clusterSettings)) {
            NativeBridge.createCache(
                cacheManagerPtr,
                type.cacheTypeName,           // "METADATA" or "STATISTICS"
                type.getSizeLimit(...),        // 250MB or 100MB
                type.getEvictionType(...)      // "LRU"
            );
        }
    }
    return cacheManagerPtr;
}
```

### Step 4: Rust — ffm.rs → CustomCacheManager
**File**: `ffm.rs`

```rust
pub extern "C" fn df_create_custom_cache_manager() -> i64 {
    let manager = CustomCacheManager::new();
    Box::into_raw(Box::new(manager)) as i64  // heap allocate, return pointer
}

pub unsafe extern "C" fn df_create_cache(cache_manager_ptr, cache_type, size_limit, ...) {
    let manager = &mut *(cache_manager_ptr as *mut CustomCacheManager);
    match cache_type {
        "METADATA" => {
            let cache = DefaultFilesMetadataCache::new(size_limit);
            manager.set_file_metadata_cache(Arc::new(MutexFileMetadataCache::new(cache)));
        }
        "STATISTICS" => {
            let cache = CustomStatisticsCache::new(PolicyType::Lru, size_limit, 0.8);
            manager.set_statistics_cache(Arc::new(cache));
        }
    }
}
```

### Step 5: Rust — api.rs → DataFusionRuntime
**File**: `api.rs`

```rust
pub fn create_global_runtime(memory_pool_limit, cache_manager_ptr, spill_dir, spill_limit) {
    // Take ownership of the cache manager
    let (cache_manager_config, custom_cache_manager) = if cache_manager_ptr != 0 {
        let mgr = unsafe { *Box::from_raw(cache_manager_ptr as *mut CustomCacheManager) };
        (mgr.build_cache_manager_config(), Some(mgr))
    } else {
        (CacheManagerConfig::default(), None)
    };

    let runtime_env = RuntimeEnvBuilder::new()
        .with_memory_pool(memory_pool)
        .with_disk_manager_builder(disk_manager)
        .with_cache_manager(cache_manager_config)  // ← caches wired into DataFusion
        .build()?;

    let runtime = DataFusionRuntime { runtime_env, custom_cache_manager };
    Ok(Box::into_raw(Box::new(runtime)) as i64)
}
```

**Result**: The `DataFusionRuntime` now holds both caches. They're accessible via `runtime.runtime_env.cache_manager`.

---

## 4. Refresh Flow — How Caches Are Pre-Warmed

When OpenSearch refreshes (flushes indexed docs to parquet), the caches are pre-warmed.

### Step 1: Engine triggers refresh
**File**: `DataFormatAwareEngine.java` (server module)

```java
// After flushing docs to parquet:
for (EngineReaderManager<?> rm : readerManagers.values()) {
    rm.afterRefresh(refreshed, newSnapshot);
}
```

### Step 2: DatafusionReaderManager.afterRefresh()
**File**: `DatafusionReaderManager.java`

```java
public void afterRefresh(boolean didRefresh, CatalogSnapshot catalogSnapshot) {
    if (didRefresh == false) return;  // nothing new
    // Create a native reader for the new parquet files
    DatafusionReader reader = new DatafusionReader(directoryPath,
        catalogSnapshot.getSearchableFiles(dataFormat.name()));
    readers.put(catalogSnapshot, reader);
}
```

### Step 3: IndexFileDeleter notifies about new files
**File**: `IndexFileDeleter.java` (server module)

```java
private void notifyFilesAdded(Map<String, Collection<String>> newFilesByFormat) {
    for (Map.Entry<String, Collection<String>> entry : newFilesByFormat.entrySet()) {
        FilesListener listener = filesListeners.get(entry.getKey());
        listener.onFilesAdded(entry.getValue());  // ← calls DatafusionReaderManager
    }
}
```

### Step 4: DatafusionReaderManager.onFilesAdded()
**File**: `DatafusionReaderManager.java`

```java
public void onFilesAdded(Collection<String> files) {
    if (files == null || files.isEmpty()) return;
    dataFusionService.onFilesAdded(toAbsolutePaths(files));  // convert to absolute paths
}
```

### Step 5: DataFusionService.onFilesAdded()
**File**: `DataFusionService.java`

```java
public void onFilesAdded(Collection<String> filePaths) {
    NativeBridge.cacheManagerAddFiles(runtimeHandle.get(), filePaths.toArray(new String[0]));
}
```

### Step 6: Rust — ffm.rs → CustomCacheManager.add_files()
**File**: `ffm.rs`

```rust
pub unsafe extern "C" fn df_cache_manager_add_files(runtime_ptr, files_ptr, ...) {
    let runtime = &*(runtime_ptr as *const DataFusionRuntime);
    let manager = runtime.custom_cache_manager.as_ref().unwrap();
    manager.add_files(&file_paths)?;
}
```

### Step 7: Rust — CustomCacheManager.add_files()
**File**: `custom_cache_manager.rs`

For each file, two things happen:

**a) Metadata cache pre-warm:**
```rust
fn metadata_cache_put(&self, file_path: &str) -> Result<bool, String> {
    // Only parquet files
    if !file_path.to_lowercase().ends_with(".parquet") { return Ok(false); }

    let store = Arc::new(LocalFileSystem::new());
    let metadata_cache = cache_ref.clone() as Arc<dyn FileMetadataCache>;

    // DataFusion reads the footer and stores it in our cache automatically
    let df_metadata = DFParquetMetadata::new(store.as_ref(), object_meta)
        .with_file_metadata_cache(Some(metadata_cache));
    df_metadata.fetch_metadata().await  // reads footer → cache.put() internally
}
```

**b) Statistics cache pre-warm:**
```rust
pub fn statistics_cache_compute_and_put(&self, file_path: &str) -> Result<bool, String> {
    // Compute statistics from parquet metadata
    let stats = compute_parquet_statistics(file_path)?;
    // Store in cache
    cache.put_with_extra(&path, Arc::new(stats), &meta);
    Ok(true)
}
```

**Result**: After refresh, both caches contain the footer and statistics for every new parquet file.

---

## 5. Query Flow — How Caches Are Used

### Step 1: REST request arrives
```
POST /_analytics/search
{"index": "demo", "sql": "SELECT msg, count FROM demo WHERE count > 1"}
```

### Step 2: AnalyticsSearchAction.RestAction
**File**: `AnalyticsSearchAction.java`

```java
protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
    Map<String, Object> body = request.contentParser().map();
    String index = (String) body.get("index");
    String sql = (String) body.get("sql");
    Request r = new Request(index, sql);
    return channel -> client.execute(INSTANCE, r, new RestToXContentListener<>(channel));
}
```

### Step 3: AnalyticsSearchService.executeSql()
**File**: `AnalyticsSearchService.java`

```java
public Result executeSql(String indexName, String sql) {
    IndexShard shard = resolveShard(indexName);
    // Fork to SEARCH threadpool
    Future<Result> future = executor.submit(() -> executeSqlOnShard(shard, sql));
    return future.get();
}

public Result executeSqlOnShard(IndexShard shard, String sql) {
    AnalyticsSearchBackendPlugin backend = backends.values().iterator().next();  // DataFusionPlugin

    try (GatedCloseable<IndexReaderProvider.Reader> gated = indexReaderProvider.acquireReader()) {
        // Step A: Compile SQL to Substrait plan
        byte[] planBytes = backend.compileSql(sql, indexName, gated.get());

        // Step B: Create execution engine and execute
        ExecutionContext ctx = new ExecutionContext(indexName, null, gated.get());
        ctx.setPlanBytes(planBytes);
        SearchExecEngine engine = backend.getSearchExecEngineProvider().createSearchExecEngine(ctx);
        EngineResultStream stream = engine.execute(ctx);
        return collect(stream);
    }
}
```

### Step 4: DataFusionPlugin.compileSql()
**File**: `DataFusionPlugin.java`

```java
public byte[] compileSql(String sql, String indexName, IndexReaderProvider.Reader reader) {
    DatafusionReader dfReader = reader.getReader(getSupportedFormats().get(0), DatafusionReader.class);
    return NativeBridge.sqlToSubstrait(dfReader.getPointer(), indexName, sql, dataFusionService.getNativeRuntime().get());
}
```

### Step 5: Rust — api.rs → sql_to_substrait()
**File**: `api.rs`

This is where the caches are used:

```rust
pub unsafe fn sql_to_substrait(shard_view_ptr, table_name, sql, runtime_ptr, manager) {
    let runtime = &*(runtime_ptr as *const DataFusionRuntime);

    // Wire the caches into the per-query RuntimeEnv
    let runtime_env = RuntimeEnvBuilder::from_runtime_env(&runtime.runtime_env)
        .with_cache_manager(
            CacheManagerConfig::default()
                .with_list_files_cache(Some(list_file_cache))
                .with_file_metadata_cache(Some(
                    runtime.runtime_env.cache_manager.get_file_metadata_cache(),  // ← METADATA CACHE
                ))
                .with_files_statistics_cache(
                    runtime.runtime_env.cache_manager.get_file_statistic_cache(), // ← STATISTICS CACHE
                ),
        )
        .build()?;

    // Create session and register table
    let listing_options = ListingOptions::new(Arc::new(ParquetFormat::new()))
        .with_collect_stat(true);

    // THIS CALL CHECKS THE METADATA CACHE:
    let schema = listing_options.infer_schema(&ctx.state(), &table_path).await?;
    //   → DataFusion internally: cache.get(object_meta)
    //   → HIT: returns cached footer instantly
    //   → MISS: reads footer from disk, then cache.put()

    let config = ListingTableConfig::new(table_path)
        .with_listing_options(listing_options)
        .with_schema(schema);
    ctx.register_table(&table_name, Arc::new(ListingTable::try_new(config)?))?;

    // THIS CALL CHECKS THE STATISTICS CACHE:
    let plan = ctx.sql(sql).await?.logical_plan().clone();
    //   → During planning, DataFusion calls ListingTable::statistics()
    //   → statistics_cache.get(file_path)
    //   → HIT: returns cached stats
    //   → MISS: computes from footer metadata

    let substrait = to_substrait_plan(&plan, &ctx.state())?;
    Ok(substrait_bytes)
}
```

### Step 6: query_executor.rs — execute_query()
**File**: `query_executor.rs`

The same cache wiring happens again for the execution phase:

```rust
pub async fn execute_query(table_path, object_metas, table_name, plan_bytes, runtime, ...) {
    // Same cache wiring as sql_to_substrait
    let runtime_env = RuntimeEnvBuilder::from_runtime_env(&runtime.runtime_env)
        .with_cache_manager(
            CacheManagerConfig::default()
                .with_file_metadata_cache(Some(
                    runtime.runtime_env.cache_manager.get_file_metadata_cache(),
                ))
                .with_files_statistics_cache(
                    runtime.runtime_env.cache_manager.get_file_statistic_cache(),
                ),
        )
        .build()?;

    // Register table (metadata cache checked again)
    let schema = listing_options.infer_schema(&ctx.state(), &table_path).await?;

    // Decode substrait → logical plan → physical plan
    // (statistics cache used for predicate pushdown optimization)
    let physical_plan = dataframe.create_physical_plan().await?;

    // Execute and stream results
    let df_stream = execute_stream(physical_plan, ctx.task_ctx())?;
}
```

---

## 6. The Two Caches — Deep Dive

### 6a. Metadata Cache (MutexFileMetadataCache)
**File**: `cache.rs`

**What it stores**: Parquet file footers — schema, row group metadata, column indexes, offset indexes.

**Keyed by**: `ObjectMeta` (file path + size + last_modified timestamp)

**Implementation**: Wraps DataFusion's `DefaultFilesMetadataCache` in a `Mutex` for thread safety.

```rust
pub struct MutexFileMetadataCache {
    pub inner: Mutex<DefaultFilesMetadataCache>,
}
```

The `DefaultFilesMetadataCache` is a `HashMap<ObjectMeta, Arc<FileMetadata>>` with a configurable size limit. When the limit is exceeded, oldest entries are evicted.

**How DataFusion uses it**: During `infer_schema()` → `fetch_metadata()`:
1. Check `cache.get(object_meta)` → if HIT, return cached footer
2. If MISS: read footer from disk (seek to end of file, read bytes, parse Thrift)
3. Load column indexes and offset indexes (additional I/O per column per row group)
4. `cache.put(object_meta, metadata)` → store for next time

### 6b. Statistics Cache (CustomStatisticsCache)
**File**: `statistics_cache.rs`

**What it stores**: Per-file `Statistics` — num_rows, total_byte_size, and per-column ColumnStatistics (min, max, null_count, distinct_count).

**Keyed by**: `object_store::path::Path` (file path)

**Implementation**: Custom cache with DashMap (concurrent HashMap) + pluggable eviction policy + memory tracking.

```rust
pub struct CustomStatisticsCache {
    inner_cache: DashMap<Path, (ObjectMeta, Arc<Statistics>)>,
    policy: Arc<Mutex<Box<dyn CachePolicy>>>,
    size_limit: AtomicUsize,
    eviction_threshold: f64,          // 0.8 = evict when 80% full
    memory_tracker: Arc<Mutex<HashMap<String, usize>>>,
    total_memory: Arc<Mutex<usize>>,
    hit_count: Arc<Mutex<usize>>,
    miss_count: Arc<Mutex<usize>>,
}
```

**How DataFusion uses it**: During `create_physical_plan()` → `ListingTable::statistics()`:
1. Check `cache.get(file_path)` → if HIT, return cached statistics
2. If MISS: compute statistics from parquet metadata (iterate row groups, extract min/max per column)
3. `cache.put(file_path, statistics)` → store for next time

**How the optimizer uses statistics**:
- `WHERE int_col > 500000` + statistics say `max(int_col) = 300000` for row group 3 → **skip entire row group**
- Row count estimates → choose optimal join strategy, aggregation approach

---

## 7. Eviction Policies
**File**: `eviction_policy.rs`

Both policies track entries via `CacheEntryMetadata`:

```rust
pub struct CacheEntryMetadata {
    pub size: usize,
    pub last_accessed: Instant,
    pub access_count: usize,
}
```

### LRU (Least Recently Used)
Evicts entries that haven't been accessed recently.

```rust
fn select_for_eviction(&self, target_size: usize) -> Vec<String> {
    // Sort by last_accessed time (oldest first)
    entries.sort_by_key(|(_, last_accessed)| *last_accessed);
    // Pick entries from oldest until freed_size >= target_size
}
```

### LFU (Least Frequently Used)
Evicts entries that are accessed least often.

```rust
fn select_for_eviction(&self, target_size: usize) -> Vec<String> {
    // Sort by access_count (least frequent first), tie-break by time
    entries.sort_by(|(_, count_a, time_a), (_, count_b, time_b)| {
        count_a.cmp(count_b).then(time_a.cmp(time_b))
    });
}
```

**When eviction happens**: Inside `CustomStatisticsCache::put_with_extra()`, before inserting a new entry:

```rust
if current_size + memory_size > (size_limit * eviction_threshold) {
    let target = (current + new) - (size_limit * 0.6);  // evict down to 60%
    let candidates = policy.select_for_eviction(target);
    for candidate in candidates {
        self.remove_internal(&path);
    }
}
```

---

## 8. FFM Bridge — How Java Talks to Rust
**File**: `NativeBridge.java` (Java) ↔ `ffm.rs` (Rust)

### How it works

Java uses JDK's Foreign Function & Memory API (FFM, JEP 454) to call Rust functions directly:

```java
// At class load time: look up the C function symbol and create a MethodHandle
static {
    SymbolLookup lib = NativeLibraryLoader.symbolLookup();
    Linker linker = Linker.nativeLinker();

    CREATE_GLOBAL_RUNTIME = linker.downcallHandle(
        lib.find("df_create_global_runtime").orElseThrow(),
        FunctionDescriptor.of(JAVA_LONG, JAVA_LONG, JAVA_LONG, ADDRESS, JAVA_LONG, JAVA_LONG)
    );
}

// At call time: invoke the MethodHandle
public static long createGlobalRuntime(long memoryLimit, long cacheManagerPtr, String spillDir, long spillLimit) {
    try (var call = new NativeCall()) {
        var dir = call.str(spillDir);  // allocate string in confined Arena
        return call.invoke(CREATE_GLOBAL_RUNTIME, memoryLimit, cacheManagerPtr, dir.segment(), dir.len(), spillLimit);
    }
    // Arena closed → temp string memory freed
}
```

On the Rust side, `ffm.rs` exports `extern "C"` functions:

```rust
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_create_global_runtime(
    memory_pool_limit: i64,
    cache_manager_ptr: i64,
    spill_dir_ptr: *const u8,
    spill_dir_len: i64,
    spill_limit: i64,
) -> i64 {
    let spill_dir = str_from_raw(spill_dir_ptr, spill_dir_len)?;
    api::create_global_runtime(memory_pool_limit, cache_manager_ptr, spill_dir, spill_limit)
}
```

### Pointer lifecycle

- `Box::into_raw(Box::new(value))` → allocates on Rust heap, returns raw pointer as `i64` to Java
- Java stores the `i64` as a `long` in `NativeRuntimeHandle`
- `Box::from_raw(ptr as *mut Type)` → takes ownership back, drops when scope ends

No Java GC involvement. No Arena needed for long-lived pointers. Only short-lived strings/byte arrays use Arena.

---

## 9. Cache Settings
**File**: `CacheSettings.java`

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `datafusion.metadata.cache.enabled` | boolean | `true` | Enable/disable metadata cache |
| `datafusion.metadata.cache.size.limit` | ByteSize | `250mb` | Max memory for metadata cache |
| `datafusion.metadata.cache.eviction.type` | String | `LRU` | Eviction policy (LRU or LFU) |
| `datafusion.statistics.cache.enabled` | boolean | `true` | Enable/disable statistics cache |
| `datafusion.statistics.cache.size.limit` | ByteSize | `100mb` | Max memory for statistics cache |
| `datafusion.statistics.cache.eviction.type` | String | `LRU` | Eviction policy (LRU or LFU) |

All settings are `NodeScope` (per-node) and `Dynamic` (can be changed at runtime, but only take effect on next node restart since caches are created at startup).

---

## 10. Cache Lifecycle

```
Node Start
  │
  ├─ DataFusionPlugin.createComponents()
  │    └─ DataFusionService.doStart()
  │         ├─ CacheUtils.createCacheConfig() → creates empty caches in Rust
  │         └─ NativeBridge.createGlobalRuntime() → wires caches into DataFusion RuntimeEnv
  │
  ▼
Index Created + Documents Indexed
  │
  ├─ Auto-refresh every 1 second
  │    └─ Flushes buffered docs to parquet segment file
  │
  ▼
Refresh Happens (didRefresh=true)
  │
  ├─ DatafusionReaderManager.afterRefresh()
  │    └─ Creates native reader for new parquet files
  │
  ├─ IndexFileDeleter.notifyFilesAdded()
  │    └─ DatafusionReaderManager.onFilesAdded()
  │         └─ DataFusionService.onFilesAdded()
  │              └─ NativeBridge.cacheManagerAddFiles()
  │                   └─ Rust: CustomCacheManager.add_files()
  │                        ├─ metadata_cache_put() → reads footer, stores in cache
  │                        └─ statistics_cache_compute_and_put() → computes stats, stores in cache
  │
  ▼
Queries Arrive
  │
  ├─ sql_to_substrait() / execute_query()
  │    ├─ infer_schema() → metadata cache HIT (footer already cached)
  │    └─ create_physical_plan() → statistics cache HIT (stats already cached)
  │
  ▼
More Refreshes (new data)
  │
  ├─ New parquet files → onFilesAdded → cache pre-warmed
  ├─ Old parquet files merged/deleted → onFilesDeleted → cache entries evicted
  │
  ▼
Cache Full (exceeds size limit)
  │
  ├─ Eviction policy selects victims (LRU: oldest accessed, LFU: least frequent)
  ├─ Victims removed from cache, memory freed
  │
  ▼
Node Shutdown
  │
  ├─ DataFusionService.doStop()
  │    └─ NativeRuntimeHandle.close()
  │         └─ Rust: close_global_runtime()
  │              └─ Drops DataFusionRuntime
  │                   ├─ Drops runtime_env (memory pool, disk manager)
  │                   └─ Drops custom_cache_manager (both caches freed)
  │
  ▼
All cache memory freed. No persistence — caches start empty on next node start.
```
