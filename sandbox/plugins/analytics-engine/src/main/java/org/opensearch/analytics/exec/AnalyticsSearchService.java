/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.backend.EngineResultBatch;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.backend.ExecutionContext;
import org.opensearch.analytics.backend.SearchExecEngine;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.exec.IndexReaderProvider;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.threadpool.ThreadPool.Names;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;

/**
 * Minimal shard-local search service: accepts SQL, compiles to the selected backend's native plan
 * via {@link AnalyticsSearchBackendPlugin#compileSql}, and executes it against the first local shard
 * of the target index.
 *
 * <p>The heavy work (reader acquisition, SQL compilation, execution, result collection) is submitted
 * to the shard-appropriate search threadpool ({@code SEARCH}, {@code SEARCH_THROTTLED}, or
 * {@code SYSTEM_READ}). The caller blocks until the pool completes.
 *
 * @opensearch.internal
 */
public class AnalyticsSearchService {

    private static final Logger logger = LogManager.getLogger(AnalyticsSearchService.class);

    private final Map<String, AnalyticsSearchBackendPlugin> backends;
    private final IndicesService indicesService;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;

    public AnalyticsSearchService(
        List<AnalyticsSearchBackendPlugin> backends,
        IndicesService indicesService,
        ClusterService clusterService,
        ThreadPool threadPool
    ) {
        Map<String, AnalyticsSearchBackendPlugin> map = new LinkedHashMap<>();
        for (AnalyticsSearchBackendPlugin backend : backends) {
            map.put(backend.name(), backend);
        }
        this.backends = map;
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
    }

    /** Result of executing a SQL query: ordered field names and row values. */
    public static final class Result {
        private final List<String> fieldNames;
        private final List<Object[]> rows;

        public Result(List<String> fieldNames, List<Object[]> rows) {
            this.fieldNames = fieldNames;
            this.rows = rows;
        }

        public List<String> getFieldNames() {
            return fieldNames;
        }

        public List<Object[]> getRows() {
            return rows;
        }
    }

    /**
     * Executes the SQL against the first local shard of {@code indexName} on the shard-appropriate
     * search threadpool. Blocks the caller until execution completes.
     */
    public Result executeSql(String indexName, String sql) {
        IndexShard shard = resolveShard(indexName);
        Executor executor = getExecutor(shard);
        Future<Result> future = ((java.util.concurrent.ExecutorService) executor).submit(() -> executeSqlOnShard(shard, sql));
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted waiting for analytics search", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException re) throw re;
            throw new RuntimeException(cause);
        }
    }

    /** Synchronous execution on the calling thread. Exposed for tests and callers that already fork. */
    public Result executeSqlOnShard(IndexShard shard, String sql) {
        if (backends.isEmpty()) {
            throw new IllegalStateException("No analytics backends registered");
        }
        AnalyticsSearchBackendPlugin backend = backends.values().iterator().next();
        String indexName = shard.shardId().getIndexName();
        IndexReaderProvider indexReaderProvider = shard.getReaderProvider();
        if (indexReaderProvider == null) {
            throw new IllegalStateException("No CompositeEngine on shard [" + shard.shardId() + "]");
        }

        try (GatedCloseable<IndexReaderProvider.Reader> gated = indexReaderProvider.acquireReader()) {
            byte[] planBytes = backend.compileSql(sql, indexName, gated.get());
            ExecutionContext ctx = new ExecutionContext(indexName, null, gated.get());
            ctx.setPlanBytes(planBytes);
            try (
                SearchExecEngine<ExecutionContext, EngineResultStream> engine = backend.getSearchExecEngineProvider()
                    .createSearchExecEngine(ctx)
            ) {
                logger.info("[AnalyticsSearchService] Executing SQL via backend [{}] on shard [{}]", backend.name(), shard.shardId());
                try (EngineResultStream stream = engine.execute(ctx)) {
                    return collect(stream);
                }
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Failed to execute SQL on index [" + indexName + "]", e);
        }
    }

    /** Resolves the first local shard of the given index. */
    public IndexShard resolveShard(String indexName) {
        IndexService indexService = indicesService.indexService(clusterService.state().metadata().index(indexName).getIndex());
        if (indexService == null) {
            throw new IllegalStateException("Index [" + indexName + "] not on this node");
        }
        Set<Integer> shardIds = indexService.shardIds();
        if (shardIds.isEmpty()) {
            throw new IllegalStateException("No shards for [" + indexName + "]");
        }
        return indexService.getShardOrNull(shardIds.iterator().next());
    }

    private Executor getExecutor(IndexShard indexShard) {
        assert indexShard != null;
        final String name;
        if (indexShard.isSystem()) {
            name = Names.SYSTEM_READ;
        } else if (indexShard.indexSettings().isSearchThrottled()) {
            name = Names.SEARCH_THROTTLED;
        } else {
            name = Names.SEARCH;
        }
        return threadPool.executor(name);
    }

    private static Result collect(EngineResultStream stream) {
        List<Object[]> rows = new ArrayList<>();
        List<String> fieldNames = null;
        Iterator<EngineResultBatch> it = stream.iterator();
        while (it.hasNext()) {
            EngineResultBatch batch = it.next();
            if (fieldNames == null) {
                fieldNames = batch.getFieldNames();
            }
            for (int row = 0; row < batch.getRowCount(); row++) {
                Object[] vals = new Object[fieldNames.size()];
                for (int col = 0; col < fieldNames.size(); col++) {
                    vals[col] = batch.getFieldValue(fieldNames.get(col), row);
                }
                rows.add(vals);
            }
        }
        return new Result(fieldNames != null ? fieldNames : List.of(), rows);
    }
}
