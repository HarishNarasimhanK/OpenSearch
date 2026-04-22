/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.action;

import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * End-to-end IT that spins up a real cluster with {@link AnalyticsPlugin} installed,
 * creates an index, and drives the {@link AnalyticsSearchAction} through the
 * transport client. Proves the REST/transport/service wiring is correct.
 *
 * <p>No backend plugin is installed, so the service is expected to fail with
 * "No analytics backends registered" — this is the minimal check that the request
 * reaches {@link org.opensearch.analytics.exec.AnalyticsSearchService}.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class AnalyticsSearchActionIT extends OpenSearchIntegTestCase {

    private static final String INDEX = "analytics_it";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(AnalyticsPlugin.class);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        prepareCreate(INDEX).setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0).build())
            .setMapping("msg", "type=keyword")
            .get();
        ensureGreen(INDEX);
    }

    public void testReachesService() throws Exception {
        AnalyticsSearchAction.Request request = new AnalyticsSearchAction.Request(INDEX, "SELECT msg FROM " + INDEX);
        ExecutionException ex = expectThrows(
            ExecutionException.class,
            () -> client().execute(AnalyticsSearchAction.INSTANCE, request).get()
        );
        // Without a backend plugin, the service throws either:
        // - "No analytics backends registered" (if coord == data node), or
        // - "Index [...] not on this node" (if coord is separate from the data node holding the shard).
        // Either confirms the request traversed REST/transport/threadpool → AnalyticsSearchService.
        String chain = collectMessages(ex);
        assertTrue(
            "Expected AnalyticsSearchService failure in chain, got: " + chain,
            chain.contains("No analytics backends registered") || chain.contains("not on this node")
        );
    }

    private static String collectMessages(Throwable t) {
        StringBuilder sb = new StringBuilder();
        while (t != null) {
            if (t.getMessage() != null) sb.append(t.getMessage()).append(" | ");
            if (t.getCause() == t) break;
            t = t.getCause();
        }
        return sb.toString();
    }
}
