/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.be.datafusion.cache.CacheManager;
import org.opensearch.be.datafusion.cache.CacheSettings;
import org.opensearch.be.datafusion.cache.CacheUtils;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.be.datafusion.nativelib.ReaderHandle;
import org.opensearch.be.datafusion.nativelib.StreamHandle;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.opensearch.common.settings.ClusterSettings.BUILT_IN_CLUSTER_SETTINGS;
import static org.apache.arrow.c.Data.importField;

/**
 * Performance test demonstrating cache effectiveness.
 *
 * Uses the same runtime, same reader, same query — only variable is whether
 * the cache is populated or cleared before each query.
 *
 * Scenario A (cache miss): clear cache before each query → every query reads footer from disk
 * Scenario B (cache hit):  cache stays warm → every query reads footer from memory
 */
public class DatafusionCachePerformanceTests extends OpenSearchTestCase {

    private static final int NUM_FILES = 5;
    private static final int WARMUP_ITERATIONS = 2;
    private static final int MEASURED_ITERATIONS = 10;
    private static final String SQL = "SELECT int_col_0, float_col_1, str_col_2 FROM test_table WHERE int_col_0 > 500000";

    public void testQueryPerformanceWithAndWithoutCache() throws Exception {
        NativeBridge.initTokioRuntimeManager(2);

        Path dataDir = createTempDir("datafusion-data");
        Path sourceParquet = Path.of(getClass().getClassLoader().getResource("large_test.parquet").toURI());

        String[] fileNames = new String[NUM_FILES];
        String[] filePaths = new String[NUM_FILES];
        for (int i = 0; i < NUM_FILES; i++) {
            fileNames[i] = "segment_" + i + ".parquet";
            Files.copy(sourceParquet, dataDir.resolve(fileNames[i]));
            filePaths[i] = dataDir.resolve(fileNames[i]).toAbsolutePath().toString();
        }

        ClusterSettings clusterSettings = createCacheClusterSettings();
        Path spillDir = createTempDir("spill");

        DataFusionService service = DataFusionService.builder()
            .memoryPoolLimit(128 * 1024 * 1024)
            .spillMemoryLimit(64 * 1024 * 1024)
            .spillDirectory(spillDir.toString())
            .cpuThreads(2)
            .clusterSettings(clusterSettings)
            .build();
        service.start();

        CacheManager cacheManager = service.getCacheManager();
        assertNotNull(cacheManager);
        NativeRuntimeHandle runtimeHandle = service.getNativeRuntime();
        ReaderHandle readerHandle = new ReaderHandle(dataDir.toString(), fileNames);

        try {
            // ---- SCENARIO A: Cache miss (clear before each query) ----
            // Warmup
            for (int i = 0; i < WARMUP_ITERATIONS; i++) {
                cacheManager.addFilesToCacheManager(List.of(filePaths));
                executeFullQuery(readerHandle, runtimeHandle);
                cacheManager.clearAllCache();
            }
            // Measure
            long[] missTimings = new long[MEASURED_ITERATIONS];
            for (int i = 0; i < MEASURED_ITERATIONS; i++) {
                cacheManager.clearAllCache();
                // Verify cache is actually empty
                assertEquals(0, cacheManager.getTotalMemoryConsumed());
                for (String fp : filePaths) {
                    assertFalse(
                        "Cache should be empty before miss query",
                        cacheManager.getEntryFromCacheType(CacheUtils.CacheType.METADATA, fp)
                    );
                }
                long start = System.nanoTime();
                executeFullQuery(readerHandle, runtimeHandle);
                missTimings[i] = System.nanoTime() - start;
            }

            // ---- SCENARIO B: Cache hit (pre-warm once, stays warm) ----
            cacheManager.clearAllCache();
            cacheManager.addFilesToCacheManager(List.of(filePaths));
            // Verify cache is actually populated
            assertTrue("Cache should have data after addFiles", cacheManager.getTotalMemoryConsumed() > 0);
            for (String fp : filePaths) {
                assertTrue(
                    "File should be in metadata cache: " + fp,
                    cacheManager.getEntryFromCacheType(CacheUtils.CacheType.METADATA, fp)
                );
                assertTrue(
                    "File should be in statistics cache: " + fp,
                    cacheManager.getEntryFromCacheType(CacheUtils.CacheType.STATISTICS, fp)
                );
            }
            long cacheMemoryAfterWarm = cacheManager.getTotalMemoryConsumed();
            logger.info("Cache populated: {} files, {} bytes consumed", filePaths.length, cacheMemoryAfterWarm);
            // Warmup
            for (int i = 0; i < WARMUP_ITERATIONS; i++) {
                executeFullQuery(readerHandle, runtimeHandle);
            }
            // Measure
            long[] hitTimings = new long[MEASURED_ITERATIONS];
            for (int i = 0; i < MEASURED_ITERATIONS; i++) {
                // Verify cache is still warm
                assertTrue("Cache should still be warm during hit queries", cacheManager.getTotalMemoryConsumed() > 0);
                long start = System.nanoTime();
                executeFullQuery(readerHandle, runtimeHandle);
                hitTimings[i] = System.nanoTime() - start;
            }

            // ---- REPORT ----
            long missAvg = average(missTimings);
            long hitAvg = average(hitTimings);

            logger.info("=== Cache Performance Results ({} parquet files) ===", NUM_FILES);
            logger.info("Cache MISS (cleared before each query): avg={} µs", missAvg / 1000);
            logger.info("Cache HIT  (pre-warmed via addFiles):   avg={} µs", hitAvg / 1000);
            logger.info("Speedup: {}x", String.format("%.2f", (double) missAvg / Math.max(hitAvg, 1)));
            logger.info("Memory consumed by cache: {} bytes", cacheManager.getTotalMemoryConsumed());
            logger.info("");
            for (int i = 0; i < MEASURED_ITERATIONS; i++) {
                logger.info("  Iteration {}: miss={} µs, hit={} µs", i + 1, missTimings[i] / 1000, hitTimings[i] / 1000);
            }
        } finally {
            readerHandle.close();
            service.stop();
        }
    }

    private void executeFullQuery(ReaderHandle readerHandle, NativeRuntimeHandle runtimeHandle) {
        byte[] substraitBytes = NativeBridge.sqlToSubstrait(readerHandle.getPointer(), "test_table", SQL, runtimeHandle.get());

        long streamPtr = asyncCall(
            listener -> NativeBridge.executeQueryAsync(
                readerHandle.getPointer(),
                "test_table",
                substraitBytes,
                runtimeHandle.get(),
                0L,
                listener
            )
        );

        try (
            StreamHandle streamHandle = new StreamHandle(streamPtr, runtimeHandle);
            RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
            CDataDictionaryProvider dictProvider = new CDataDictionaryProvider()
        ) {
            long schemaAddr = asyncCall(listener -> NativeBridge.streamGetSchema(streamHandle.getPointer(), listener));
            Schema schema = new Schema(importField(allocator, ArrowSchema.wrap(schemaAddr), dictProvider).getChildren(), null);
            VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);

            while (true) {
                long arrayAddr = asyncCall(listener -> NativeBridge.streamNext(runtimeHandle.get(), streamHandle.getPointer(), listener));
                if (arrayAddr == 0) break;
                Data.importIntoVectorSchemaRoot(allocator, ArrowArray.wrap(arrayAddr), root, dictProvider);
            }
            root.close();
        }
    }

    private long asyncCall(java.util.function.Consumer<ActionListener<Long>> call) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        call.accept(new ActionListener<>() {
            @Override
            public void onResponse(Long v) {
                future.complete(v);
            }

            @Override
            public void onFailure(Exception e) {
                future.completeExceptionally(e);
            }
        });
        return future.join();
    }

    private long average(long[] values) {
        long sum = 0;
        for (long v : values)
            sum += v;
        return sum / values.length;
    }

    private ClusterSettings createCacheClusterSettings() {
        Set<Setting<?>> all = new HashSet<>(BUILT_IN_CLUSTER_SETTINGS);
        all.add(CacheSettings.METADATA_CACHE_ENABLED);
        all.add(CacheSettings.METADATA_CACHE_SIZE_LIMIT);
        all.add(CacheSettings.METADATA_CACHE_EVICTION_TYPE);
        all.add(CacheSettings.STATISTICS_CACHE_ENABLED);
        all.add(CacheSettings.STATISTICS_CACHE_SIZE_LIMIT);
        all.add(CacheSettings.STATISTICS_CACHE_EVICTION_TYPE);
        all.add(DataFusionPlugin.DATAFUSION_MEMORY_POOL_LIMIT);
        all.add(DataFusionPlugin.DATAFUSION_SPILL_MEMORY_LIMIT);
        return new ClusterSettings(Settings.EMPTY, all);
    }
}
