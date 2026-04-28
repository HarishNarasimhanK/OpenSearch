/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.shard.ShardPath;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Manages {@link DatafusionReader} instances per shard.
 * <p>
 * On refresh, a new reader is created from the updated catalog snapshot.
 * File lifecycle events (add/delete) are delegated to the node-level
 * {@link DataFusionService} for cache management.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DatafusionReaderManager implements EngineReaderManager<DatafusionReader> {

    private static final Logger logger = LogManager.getLogger(DatafusionReaderManager.class);

    private final Map<CatalogSnapshot, DatafusionReader> readers = new HashMap<>();
    private DatafusionReader latestReader;
    private final DataFormat dataFormat;
    private final String directoryPath;
    private final DataFusionService dataFusionService;

    /**
     * Creates a reader manager.
     * @param dataFormat the data format for this reader
     * @param shardPath the shard path to read data from
     * @param dataFusionService node-level service for cache management
     */
    public DatafusionReaderManager(DataFormat dataFormat, ShardPath shardPath, DataFusionService dataFusionService) {
        this.dataFormat = dataFormat;
        this.directoryPath = shardPath.getDataPath().resolve(dataFormat.name()).toString();
        this.dataFusionService = dataFusionService;
    }

    @Override
    public DatafusionReader getReader(CatalogSnapshot catalogSnapshot) throws IOException {
        if (readers.containsKey(catalogSnapshot)) {
            return readers.get(catalogSnapshot);
        }
        throw new IOException("No DataFusion reader available");
    }

    @Override
    public void onDeleted(CatalogSnapshot catalogSnapshot) throws IOException {
        DatafusionReader removed = readers.remove(catalogSnapshot);
        // Only close if no other snapshot key references it and it's not the cached latestReader.
        if (removed != null && removed != latestReader && readers.containsValue(removed) == false) {
            removed.close();
        }
    }

    @Override
    public void onFilesDeleted(Collection<String> files) throws IOException {
        if (files == null || files.isEmpty()) return;
        dataFusionService.onFilesDeleted(toAbsolutePaths(files));
    }

    @Override
    public void onFilesAdded(Collection<String> files) throws IOException {
        if (files == null || files.isEmpty()) return;
        logger.info("[CACHE WIRING] onFilesAdded called with {} files: {}", files.size(), files);
        dataFusionService.onFilesAdded(toAbsolutePaths(files));
    }

    @Override
    public void beforeRefresh() throws IOException {}

    @Override
    public void afterRefresh(boolean didRefresh, CatalogSnapshot catalogSnapshot) throws IOException {
        if (didRefresh) {
            logger.info("[CACHE WIRING] afterRefresh called, didRefresh=true");
        }
        if (readers.containsKey(catalogSnapshot)) return;
        if (didRefresh == false) {
            // No new data; reuse the most recent reader (if any) for this snapshot so later
            // acquireReader() calls — which pass the latest snapshot from the manager — can find it.
            if (latestReader != null) {
                readers.put(catalogSnapshot, latestReader);
            }
            return;
        }
        DatafusionReader reader = new DatafusionReader(directoryPath, catalogSnapshot.getSearchableFiles(dataFormat.name()));
        readers.put(catalogSnapshot, reader);
        latestReader = reader;
    }

    private Collection<String> toAbsolutePaths(Collection<String> fileNames) {
        return fileNames.stream().map(f -> directoryPath + "/" + f).collect(Collectors.toList());
    }

    @Override
    public void close() throws IOException {
        for (DatafusionReader reader : readers.values()) {
            reader.close();
        }
        readers.clear();
    }
}
