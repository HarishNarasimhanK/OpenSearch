/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.action;

import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;

import java.util.List;

/**
 * Simple wrapper around the list of registered {@link AnalyticsSearchBackendPlugin}s.
 * Exists so the list can be bound as a single component and injected into transport actions via Guice.
 */
public final class AnalyticsBackends {

    private final List<AnalyticsSearchBackendPlugin> backends;

    public AnalyticsBackends(List<AnalyticsSearchBackendPlugin> backends) {
        this.backends = List.copyOf(backends);
    }

    public List<AnalyticsSearchBackendPlugin> get() {
        return backends;
    }
}
