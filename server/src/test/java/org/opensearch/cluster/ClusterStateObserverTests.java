/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.cluster;

import org.opensearch.OpenSearchException;
import org.opensearch.Version;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterApplierService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ClusterStateObserverTests extends OpenSearchTestCase {

    public void testClusterStateListenerToStringIncludesListenerToString() {
        final ClusterApplierService clusterApplierService = mock(ClusterApplierService.class);
        final AtomicBoolean listenerAdded = new AtomicBoolean();

        doAnswer(invocation -> {
            assertThat(Arrays.toString(invocation.getArguments()), containsString("test-listener"));
            listenerAdded.set(true);
            return null;
        }).when(clusterApplierService).addTimeoutListener(any(), any());

        final ClusterState clusterState = ClusterState.builder(new ClusterName("test")).nodes(DiscoveryNodes.builder()).build();
        when(clusterApplierService.state()).thenReturn(clusterState);

        final ClusterStateObserver clusterStateObserver = new ClusterStateObserver(
            clusterState,
            clusterApplierService,
            null,
            logger,
            new ThreadContext(Settings.EMPTY)
        );
        clusterStateObserver.waitForNextChange(new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {}

            @Override
            public void onClusterServiceClose() {}

            @Override
            public void onTimeout(TimeValue timeout) {}

            @Override
            public String toString() {
                return "test-listener";
            }
        });

        assertTrue(listenerAdded.get());
    }

    /**
     * Tests that the ClusterStateObserver constructed with pre-extracted (String, long) values
     * correctly detects a newer cluster state via waitForNextChange, matching the behavior of
     * the ClusterState-based constructor.
     */
    public void testPrimitiveConstructorDetectsNewerState() {
        final ClusterApplierService clusterApplierService = mock(ClusterApplierService.class);
        final ThreadPool threadPool = mock(ThreadPool.class);
        when(clusterApplierService.threadPool()).thenReturn(threadPool);
        when(threadPool.relativeTimeInMillis()).thenReturn(0L);

        final DiscoveryNode masterNode = new DiscoveryNode("master", buildNewFakeTransportAddress(), Version.CURRENT);
        final ClusterState newerState = ClusterState.builder(new ClusterName("test"))
            .nodes(DiscoveryNodes.builder().add(masterNode).clusterManagerNodeId(masterNode.getId()))
            .version(5)
            .build();
        when(clusterApplierService.state()).thenReturn(newerState);

        final AtomicBoolean listenerAdded = new AtomicBoolean();
        doAnswer(invocation -> {
            listenerAdded.set(true);
            return null;
        }).when(clusterApplierService).addTimeoutListener(any(), any());

        // Construct with persistent node ID and version 1 — newerState has version 5, same master
        final ClusterStateObserver observer = new ClusterStateObserver(
            masterNode.getId(),
            1L,
            clusterApplierService,
            TimeValue.timeValueSeconds(30),
            logger,
            new ThreadContext(Settings.EMPTY)
        );

        final AtomicReference<ClusterState> receivedState = new AtomicReference<>();
        observer.waitForNextChange(new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {
                receivedState.set(state);
            }

            @Override
            public void onClusterServiceClose() {}

            @Override
            public void onTimeout(TimeValue timeout) {}
        });

        // The sampled state (version 5) is newer than stored (version 1) with same master,
        // so the predicate should accept it immediately without adding a listener
        assertFalse(listenerAdded.get());
        assertNotNull(receivedState.get());
        assertEquals(5L, receivedState.get().version());
    }

    /**
     * Tests that the ClusterStateObserver constructed with (String, long) correctly waits
     * when the current state has the same version and master as the stored state.
     */
    public void testPrimitiveConstructorWaitsWhenStateUnchanged() {
        final ClusterApplierService clusterApplierService = mock(ClusterApplierService.class);
        final ThreadPool threadPool = mock(ThreadPool.class);
        when(clusterApplierService.threadPool()).thenReturn(threadPool);
        when(threadPool.relativeTimeInMillis()).thenReturn(0L);

        final DiscoveryNode masterNode = new DiscoveryNode("master", buildNewFakeTransportAddress(), Version.CURRENT);
        final ClusterState sameState = ClusterState.builder(new ClusterName("test"))
            .nodes(DiscoveryNodes.builder().add(masterNode).clusterManagerNodeId(masterNode.getId()))
            .version(5)
            .build();
        when(clusterApplierService.state()).thenReturn(sameState);

        final AtomicBoolean listenerAdded = new AtomicBoolean();
        doAnswer(invocation -> {
            listenerAdded.set(true);
            return null;
        }).when(clusterApplierService).addTimeoutListener(any(), any());

        // Construct with same persistent node ID and same version — should NOT detect a change
        final ClusterStateObserver observer = new ClusterStateObserver(
            masterNode.getId(),
            5L,
            clusterApplierService,
            TimeValue.timeValueSeconds(30),
            logger,
            new ThreadContext(Settings.EMPTY)
        );

        final AtomicReference<ClusterState> receivedState = new AtomicReference<>();
        observer.waitForNextChange(new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {
                receivedState.set(state);
            }

            @Override
            public void onClusterServiceClose() {}

            @Override
            public void onTimeout(TimeValue timeout) {}
        });

        // State hasn't changed, so observer should add a listener and wait
        assertTrue(listenerAdded.get());
        assertNull(receivedState.get());
    }

    /**
     * Tests that the ClusterStateObserver constructed with (String, long) detects a different
     * cluster manager even when the version is the same.
     */
    public void testPrimitiveConstructorDetectsDifferentClusterManager() {
        final ClusterApplierService clusterApplierService = mock(ClusterApplierService.class);
        final ThreadPool threadPool = mock(ThreadPool.class);
        when(clusterApplierService.threadPool()).thenReturn(threadPool);
        when(threadPool.relativeTimeInMillis()).thenReturn(0L);

        final DiscoveryNode oldMaster = new DiscoveryNode("old_master", buildNewFakeTransportAddress(), Version.CURRENT);
        final DiscoveryNode newMaster = new DiscoveryNode("new_master", buildNewFakeTransportAddress(), Version.CURRENT);
        final ClusterState newMasterState = ClusterState.builder(new ClusterName("test"))
            .nodes(DiscoveryNodes.builder().add(oldMaster).add(newMaster).clusterManagerNodeId(newMaster.getId()))
            .version(5)
            .build();
        when(clusterApplierService.state()).thenReturn(newMasterState);

        final AtomicBoolean listenerAdded = new AtomicBoolean();
        doAnswer(invocation -> {
            listenerAdded.set(true);
            return null;
        }).when(clusterApplierService).addTimeoutListener(any(), any());

        // Construct with old master's persistent ID — new state has different master
        final ClusterStateObserver observer = new ClusterStateObserver(
            oldMaster.getId(),
            5L,
            clusterApplierService,
            TimeValue.timeValueSeconds(30),
            logger,
            new ThreadContext(Settings.EMPTY)
        );

        final AtomicReference<ClusterState> receivedState = new AtomicReference<>();
        observer.waitForNextChange(new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {
                receivedState.set(state);
            }

            @Override
            public void onClusterServiceClose() {}

            @Override
            public void onTimeout(TimeValue timeout) {}
        });

        // Different master detected — should accept immediately
        assertFalse(listenerAdded.get());
        assertNotNull(receivedState.get());
    }

    /**
     * Tests that the ClusterService-based primitive constructor delegates correctly
     * to the ClusterApplierService-based constructor.
     */
    public void testPrimitiveConstructorViaClusterService() {
        final ClusterApplierService clusterApplierService = mock(ClusterApplierService.class);
        final ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterApplierService()).thenReturn(clusterApplierService);
        final ThreadPool threadPool = mock(ThreadPool.class);
        when(clusterApplierService.threadPool()).thenReturn(threadPool);
        when(threadPool.relativeTimeInMillis()).thenReturn(0L);

        final DiscoveryNode masterNode = new DiscoveryNode("master", buildNewFakeTransportAddress(), Version.CURRENT);
        final ClusterState newerState = ClusterState.builder(new ClusterName("test"))
            .nodes(DiscoveryNodes.builder().add(masterNode).clusterManagerNodeId(masterNode.getId()))
            .version(10)
            .build();
        when(clusterApplierService.state()).thenReturn(newerState);

        // Use the ClusterService-based constructor
        final ClusterStateObserver observer = new ClusterStateObserver(
            masterNode.getId(),
            1L,
            clusterService,
            TimeValue.timeValueSeconds(30),
            logger,
            new ThreadContext(Settings.EMPTY)
        );

        final AtomicReference<ClusterState> receivedState = new AtomicReference<>();
        observer.waitForNextChange(new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {
                receivedState.set(state);
            }

            @Override
            public void onClusterServiceClose() {}

            @Override
            public void onTimeout(TimeValue timeout) {}
        });

        // Newer version detected — should accept immediately
        assertNotNull(receivedState.get());
        assertEquals(10L, receivedState.get().version());
    }

    /**
     * Tests that the primitive constructor with null clusterManagerNodeId (no master)
     * detects when a master appears.
     */
    public void testPrimitiveConstructorNullMasterDetectsNewMaster() {
        final ClusterApplierService clusterApplierService = mock(ClusterApplierService.class);
        final ThreadPool threadPool = mock(ThreadPool.class);
        when(clusterApplierService.threadPool()).thenReturn(threadPool);
        when(threadPool.relativeTimeInMillis()).thenReturn(0L);

        final DiscoveryNode newMaster = new DiscoveryNode("new_master", buildNewFakeTransportAddress(), Version.CURRENT);
        final ClusterState stateWithMaster = ClusterState.builder(new ClusterName("test"))
            .nodes(DiscoveryNodes.builder().add(newMaster).clusterManagerNodeId(newMaster.getId()))
            .version(5)
            .build();
        when(clusterApplierService.state()).thenReturn(stateWithMaster);

        final AtomicBoolean listenerAdded = new AtomicBoolean();
        doAnswer(invocation -> {
            listenerAdded.set(true);
            return null;
        }).when(clusterApplierService).addTimeoutListener(any(), any());

        // Construct with null master ID — simulates "no master" initial state
        final ClusterStateObserver observer = new ClusterStateObserver(
            null,
            5L,
            clusterApplierService,
            TimeValue.timeValueSeconds(30),
            logger,
            new ThreadContext(Settings.EMPTY)
        );

        final AtomicReference<ClusterState> receivedState = new AtomicReference<>();
        observer.waitForNextChange(new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {
                receivedState.set(state);
            }

            @Override
            public void onClusterServiceClose() {}

            @Override
            public void onTimeout(TimeValue timeout) {}
        });

        // Different master (null -> newMaster) — should accept immediately
        assertFalse("should not need to add listener", listenerAdded.get());
        assertNotNull(receivedState.get());
    }

    /**
     * Tests that setAndGetObservedState returns the current cluster state and updates lastObservedState.
     */
    public void testSetAndGetObservedState() {
        final ClusterApplierService clusterApplierService = mock(ClusterApplierService.class);
        final DiscoveryNode masterNode = new DiscoveryNode("master", buildNewFakeTransportAddress(), Version.CURRENT);
        final ClusterState initialState = ClusterState.builder(new ClusterName("test"))
            .nodes(DiscoveryNodes.builder().add(masterNode).clusterManagerNodeId(masterNode.getId()))
            .version(1)
            .build();
        final ClusterState updatedState = ClusterState.builder(new ClusterName("test"))
            .nodes(DiscoveryNodes.builder().add(masterNode).clusterManagerNodeId(masterNode.getId()))
            .version(5)
            .build();
        when(clusterApplierService.state()).thenReturn(updatedState);

        final ClusterStateObserver observer = new ClusterStateObserver(
            initialState,
            clusterApplierService,
            null,
            logger,
            new ThreadContext(Settings.EMPTY)
        );

        ClusterState result = observer.setAndGetObservedState();
        assertEquals(5L, result.version());
    }

    /**
     * Tests that setAndGetObservedState throws when called while waiting for a change.
     */
    public void testSetAndGetObservedStateThrowsWhileWaiting() {
        final ClusterApplierService clusterApplierService = mock(ClusterApplierService.class);
        final ClusterState clusterState = ClusterState.builder(new ClusterName("test")).nodes(DiscoveryNodes.builder()).build();
        when(clusterApplierService.state()).thenReturn(clusterState);

        final ClusterStateObserver observer = new ClusterStateObserver(
            clusterState,
            clusterApplierService,
            null,
            logger,
            new ThreadContext(Settings.EMPTY)
        );

        observer.observingContext.set(new ClusterStateObserver.ObservingContext(new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {}

            @Override
            public void onClusterServiceClose() {}

            @Override
            public void onTimeout(TimeValue timeout) {}
        }, state -> true));

        expectThrows(OpenSearchException.class, observer::setAndGetObservedState);
    }

    /**
     * Tests that isTimedOut returns false initially and true after timeout.
     */
    public void testIsTimedOut() {
        final ClusterApplierService clusterApplierService = mock(ClusterApplierService.class);
        final ThreadPool threadPool = mock(ThreadPool.class);
        when(clusterApplierService.threadPool()).thenReturn(threadPool);
        when(threadPool.relativeTimeInMillis()).thenReturn(0L);

        final ClusterState clusterState = ClusterState.builder(new ClusterName("test")).nodes(DiscoveryNodes.builder()).build();
        when(clusterApplierService.state()).thenReturn(clusterState);

        final ClusterStateObserver observer = new ClusterStateObserver(
            clusterState,
            clusterApplierService,
            TimeValue.timeValueSeconds(30),
            logger,
            new ThreadContext(Settings.EMPTY)
        );

        assertFalse(observer.isTimedOut());
        observer.timedOut = true;
        assertTrue(observer.isTimedOut());
    }

    /**
     * Tests that waitForNextChange triggers onTimeout when the global timeout has already expired.
     */
    public void testWaitForNextChangeGlobalTimeoutExpired() {
        final ClusterApplierService clusterApplierService = mock(ClusterApplierService.class);
        final ThreadPool threadPool = mock(ThreadPool.class);
        when(clusterApplierService.threadPool()).thenReturn(threadPool);
        when(threadPool.relativeTimeInMillis()).thenReturn(0L, 70000L);

        final ClusterState clusterState = ClusterState.builder(new ClusterName("test")).nodes(DiscoveryNodes.builder()).build();
        when(clusterApplierService.state()).thenReturn(clusterState);

        final ClusterStateObserver observer = new ClusterStateObserver(
            clusterState,
            clusterApplierService,
            TimeValue.timeValueSeconds(60),
            logger,
            new ThreadContext(Settings.EMPTY)
        );

        final AtomicBoolean timedOut = new AtomicBoolean();
        observer.waitForNextChange(new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {}

            @Override
            public void onClusterServiceClose() {}

            @Override
            public void onTimeout(TimeValue timeout) {
                timedOut.set(true);
            }
        });

        assertTrue(timedOut.get());
        assertTrue(observer.isTimedOut());
    }

    /**
     * Tests that waitForNextChange with an explicit timeOutValue resets the startTime and timedOut flag.
     */
    public void testWaitForNextChangeWithExplicitTimeout() {
        final ClusterApplierService clusterApplierService = mock(ClusterApplierService.class);
        final ThreadPool threadPool = mock(ThreadPool.class);
        when(clusterApplierService.threadPool()).thenReturn(threadPool);
        when(threadPool.relativeTimeInMillis()).thenReturn(0L);

        final ClusterState clusterState = ClusterState.builder(new ClusterName("test")).nodes(DiscoveryNodes.builder()).build();
        when(clusterApplierService.state()).thenReturn(clusterState);

        final ClusterStateObserver observer = new ClusterStateObserver(
            clusterState,
            clusterApplierService,
            null,
            logger,
            new ThreadContext(Settings.EMPTY)
        );

        final AtomicBoolean listenerAdded = new AtomicBoolean();
        doAnswer(invocation -> {
            listenerAdded.set(true);
            return null;
        }).when(clusterApplierService).addTimeoutListener(any(), any());

        observer.waitForNextChange(new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {}

            @Override
            public void onClusterServiceClose() {}

            @Override
            public void onTimeout(TimeValue timeout) {}
        }, state -> false, TimeValue.timeValueSeconds(10));

        assertTrue(listenerAdded.get());
        assertFalse(observer.isTimedOut());
    }

    /**
     * Tests that waitForNextChange throws when already waiting.
     */
    public void testWaitForNextChangeThrowsWhenAlreadyWaiting() {
        final ClusterApplierService clusterApplierService = mock(ClusterApplierService.class);
        final ClusterState clusterState = ClusterState.builder(new ClusterName("test")).nodes(DiscoveryNodes.builder()).build();
        when(clusterApplierService.state()).thenReturn(clusterState);

        final ClusterStateObserver observer = new ClusterStateObserver(
            clusterState,
            clusterApplierService,
            null,
            logger,
            new ThreadContext(Settings.EMPTY)
        );

        observer.observingContext.set(new ClusterStateObserver.ObservingContext(new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {}

            @Override
            public void onClusterServiceClose() {}

            @Override
            public void onTimeout(TimeValue timeout) {}
        }, state -> true));

        expectThrows(OpenSearchException.class, () -> observer.waitForNextChange(new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {}

            @Override
            public void onClusterServiceClose() {}

            @Override
            public void onTimeout(TimeValue timeout) {}
        }));
    }

    /**
     * Tests the ObserverClusterStateListener.clusterChanged when predicate accepts the state.
     */
    public void testClusterChangedPredicateAccepts() {
        final ClusterApplierService clusterApplierService = mock(ClusterApplierService.class);
        final ThreadPool threadPool = mock(ThreadPool.class);
        when(clusterApplierService.threadPool()).thenReturn(threadPool);
        when(threadPool.relativeTimeInMillis()).thenReturn(0L);

        final DiscoveryNode masterNode = new DiscoveryNode("master", buildNewFakeTransportAddress(), Version.CURRENT);
        final ClusterState clusterState = ClusterState.builder(new ClusterName("test"))
            .nodes(DiscoveryNodes.builder().add(masterNode).clusterManagerNodeId(masterNode.getId()))
            .version(1)
            .build();
        when(clusterApplierService.state()).thenReturn(clusterState);

        final ClusterStateObserver observer = new ClusterStateObserver(
            clusterState,
            clusterApplierService,
            TimeValue.timeValueSeconds(30),
            logger,
            new ThreadContext(Settings.EMPTY)
        );

        final AtomicReference<ClusterState> receivedState = new AtomicReference<>();
        observer.observingContext.set(new ClusterStateObserver.ObservingContext(new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {
                receivedState.set(state);
            }

            @Override
            public void onClusterServiceClose() {}

            @Override
            public void onTimeout(TimeValue timeout) {}
        }, state -> true));

        final ClusterState newState = ClusterState.builder(new ClusterName("test"))
            .nodes(DiscoveryNodes.builder().add(masterNode).clusterManagerNodeId(masterNode.getId()))
            .version(2)
            .build();
        observer.clusterStateListener.clusterChanged(new ClusterChangedEvent("test", newState, clusterState));

        assertNotNull(receivedState.get());
        assertEquals(2L, receivedState.get().version());
        assertNull(observer.observingContext.get());
        verify(clusterApplierService).removeTimeoutListener(observer.clusterStateListener);
    }

    /**
     * Tests the ObserverClusterStateListener.clusterChanged when predicate rejects the state.
     */
    public void testClusterChangedPredicateRejects() {
        final ClusterApplierService clusterApplierService = mock(ClusterApplierService.class);
        final ThreadPool threadPool = mock(ThreadPool.class);
        when(clusterApplierService.threadPool()).thenReturn(threadPool);
        when(threadPool.relativeTimeInMillis()).thenReturn(0L);

        final DiscoveryNode masterNode = new DiscoveryNode("master", buildNewFakeTransportAddress(), Version.CURRENT);
        final ClusterState clusterState = ClusterState.builder(new ClusterName("test"))
            .nodes(DiscoveryNodes.builder().add(masterNode).clusterManagerNodeId(masterNode.getId()))
            .version(1)
            .build();
        when(clusterApplierService.state()).thenReturn(clusterState);

        final ClusterStateObserver observer = new ClusterStateObserver(
            clusterState,
            clusterApplierService,
            TimeValue.timeValueSeconds(30),
            logger,
            new ThreadContext(Settings.EMPTY)
        );

        final AtomicReference<ClusterState> receivedState = new AtomicReference<>();
        observer.observingContext.set(new ClusterStateObserver.ObservingContext(new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {
                receivedState.set(state);
            }

            @Override
            public void onClusterServiceClose() {}

            @Override
            public void onTimeout(TimeValue timeout) {}
        }, state -> false));

        final ClusterState newState = ClusterState.builder(new ClusterName("test"))
            .nodes(DiscoveryNodes.builder().add(masterNode).clusterManagerNodeId(masterNode.getId()))
            .version(2)
            .build();
        observer.clusterStateListener.clusterChanged(new ClusterChangedEvent("test", newState, clusterState));

        assertNull(receivedState.get());
        assertNotNull(observer.observingContext.get());
    }

    /**
     * Tests the ObserverClusterStateListener.clusterChanged when observingContext is null (no-op).
     */
    public void testClusterChangedWithNullContext() {
        final ClusterApplierService clusterApplierService = mock(ClusterApplierService.class);
        final ClusterState clusterState = ClusterState.builder(new ClusterName("test")).nodes(DiscoveryNodes.builder()).build();
        when(clusterApplierService.state()).thenReturn(clusterState);

        final ClusterStateObserver observer = new ClusterStateObserver(
            clusterState,
            clusterApplierService,
            null,
            logger,
            new ThreadContext(Settings.EMPTY)
        );

        observer.clusterStateListener.clusterChanged(new ClusterChangedEvent("test", clusterState, clusterState));
        assertNull(observer.observingContext.get());
    }

    /**
     * Tests the ObserverClusterStateListener.postAdded when state is accepted.
     */
    public void testPostAddedAcceptsNewerState() {
        final ClusterApplierService clusterApplierService = mock(ClusterApplierService.class);
        final ThreadPool threadPool = mock(ThreadPool.class);
        when(clusterApplierService.threadPool()).thenReturn(threadPool);
        when(threadPool.relativeTimeInMillis()).thenReturn(0L);

        final DiscoveryNode masterNode = new DiscoveryNode("master", buildNewFakeTransportAddress(), Version.CURRENT);
        final ClusterState oldState = ClusterState.builder(new ClusterName("test"))
            .nodes(DiscoveryNodes.builder().add(masterNode).clusterManagerNodeId(masterNode.getId()))
            .version(1)
            .build();
        final ClusterState newState = ClusterState.builder(new ClusterName("test"))
            .nodes(DiscoveryNodes.builder().add(masterNode).clusterManagerNodeId(masterNode.getId()))
            .version(5)
            .build();
        when(clusterApplierService.state()).thenReturn(newState);

        final ClusterStateObserver observer = new ClusterStateObserver(
            oldState,
            clusterApplierService,
            TimeValue.timeValueSeconds(30),
            logger,
            new ThreadContext(Settings.EMPTY)
        );

        final AtomicReference<ClusterState> receivedState = new AtomicReference<>();
        observer.observingContext.set(new ClusterStateObserver.ObservingContext(new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {
                receivedState.set(state);
            }

            @Override
            public void onClusterServiceClose() {}

            @Override
            public void onTimeout(TimeValue timeout) {}
        }, state -> true));

        observer.clusterStateListener.postAdded();

        assertNotNull(receivedState.get());
        assertEquals(5L, receivedState.get().version());
        assertNull(observer.observingContext.get());
        verify(clusterApplierService).removeTimeoutListener(observer.clusterStateListener);
    }

    /**
     * Tests the ObserverClusterStateListener.postAdded when context is null (no-op).
     */
    public void testPostAddedWithNullContext() {
        final ClusterApplierService clusterApplierService = mock(ClusterApplierService.class);
        final ClusterState clusterState = ClusterState.builder(new ClusterName("test")).nodes(DiscoveryNodes.builder()).build();
        when(clusterApplierService.state()).thenReturn(clusterState);

        final ClusterStateObserver observer = new ClusterStateObserver(
            clusterState,
            clusterApplierService,
            null,
            logger,
            new ThreadContext(Settings.EMPTY)
        );

        observer.clusterStateListener.postAdded();
        assertNull(observer.observingContext.get());
    }

    /**
     * Tests the ObserverClusterStateListener.onClose notifies the listener.
     */
    public void testOnCloseNotifiesListener() {
        final ClusterApplierService clusterApplierService = mock(ClusterApplierService.class);
        final ClusterState clusterState = ClusterState.builder(new ClusterName("test")).nodes(DiscoveryNodes.builder()).build();
        when(clusterApplierService.state()).thenReturn(clusterState);

        final ClusterStateObserver observer = new ClusterStateObserver(
            clusterState,
            clusterApplierService,
            null,
            logger,
            new ThreadContext(Settings.EMPTY)
        );

        final AtomicBoolean closeCalled = new AtomicBoolean();
        observer.observingContext.set(new ClusterStateObserver.ObservingContext(new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {}

            @Override
            public void onClusterServiceClose() {
                closeCalled.set(true);
            }

            @Override
            public void onTimeout(TimeValue timeout) {}
        }, state -> true));

        observer.clusterStateListener.onClose();

        assertTrue(closeCalled.get());
        assertNull(observer.observingContext.get());
        verify(clusterApplierService).removeTimeoutListener(observer.clusterStateListener);
    }

    /**
     * Tests the ObserverClusterStateListener.onClose when context is null (no-op).
     */
    public void testOnCloseWithNullContext() {
        final ClusterApplierService clusterApplierService = mock(ClusterApplierService.class);
        final ClusterState clusterState = ClusterState.builder(new ClusterName("test")).nodes(DiscoveryNodes.builder()).build();
        when(clusterApplierService.state()).thenReturn(clusterState);

        final ClusterStateObserver observer = new ClusterStateObserver(
            clusterState,
            clusterApplierService,
            null,
            logger,
            new ThreadContext(Settings.EMPTY)
        );

        observer.clusterStateListener.onClose();
        assertNull(observer.observingContext.get());
    }

    /**
     * Tests the ObserverClusterStateListener.onTimeout notifies the listener and sets timedOut.
     */
    public void testOnTimeoutNotifiesListener() {
        final ClusterApplierService clusterApplierService = mock(ClusterApplierService.class);
        final ThreadPool threadPool = mock(ThreadPool.class);
        when(clusterApplierService.threadPool()).thenReturn(threadPool);
        when(threadPool.relativeTimeInMillis()).thenReturn(0L);

        final ClusterState clusterState = ClusterState.builder(new ClusterName("test")).nodes(DiscoveryNodes.builder()).build();
        when(clusterApplierService.state()).thenReturn(clusterState);

        final ClusterStateObserver observer = new ClusterStateObserver(
            clusterState,
            clusterApplierService,
            TimeValue.timeValueSeconds(30),
            logger,
            new ThreadContext(Settings.EMPTY)
        );

        final AtomicBoolean timeoutCalled = new AtomicBoolean();
        observer.observingContext.set(new ClusterStateObserver.ObservingContext(new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {}

            @Override
            public void onClusterServiceClose() {}

            @Override
            public void onTimeout(TimeValue timeout) {
                timeoutCalled.set(true);
            }
        }, state -> true));

        observer.clusterStateListener.onTimeout(TimeValue.timeValueSeconds(30));

        assertTrue(timeoutCalled.get());
        assertTrue(observer.isTimedOut());
        assertNull(observer.observingContext.get());
        verify(clusterApplierService).removeTimeoutListener(observer.clusterStateListener);
    }

    /**
     * Tests the ObserverClusterStateListener.onTimeout when context is null (no-op).
     */
    public void testOnTimeoutWithNullContext() {
        final ClusterApplierService clusterApplierService = mock(ClusterApplierService.class);
        final ThreadPool threadPool = mock(ThreadPool.class);
        when(clusterApplierService.threadPool()).thenReturn(threadPool);
        when(threadPool.relativeTimeInMillis()).thenReturn(0L);

        final ClusterState clusterState = ClusterState.builder(new ClusterName("test")).nodes(DiscoveryNodes.builder()).build();
        when(clusterApplierService.state()).thenReturn(clusterState);

        final ClusterStateObserver observer = new ClusterStateObserver(
            clusterState,
            clusterApplierService,
            TimeValue.timeValueSeconds(30),
            logger,
            new ThreadContext(Settings.EMPTY)
        );

        observer.clusterStateListener.onTimeout(TimeValue.timeValueSeconds(30));
        assertFalse(observer.isTimedOut());
    }

    /**
     * Tests the ObserverClusterStateListener.toString includes the observing context.
     */
    public void testObserverClusterStateListenerToString() {
        final ClusterApplierService clusterApplierService = mock(ClusterApplierService.class);
        final ClusterState clusterState = ClusterState.builder(new ClusterName("test")).nodes(DiscoveryNodes.builder()).build();
        when(clusterApplierService.state()).thenReturn(clusterState);

        final ClusterStateObserver observer = new ClusterStateObserver(
            clusterState,
            clusterApplierService,
            null,
            logger,
            new ThreadContext(Settings.EMPTY)
        );

        assertThat(observer.clusterStateListener.toString(), containsString("ClusterStateObserver["));
    }

    /**
     * Tests the waitForNextChange with null global timeout (indefinite wait).
     */
    public void testWaitForNextChangeWithNullGlobalTimeout() {
        final ClusterApplierService clusterApplierService = mock(ClusterApplierService.class);
        final ClusterState clusterState = ClusterState.builder(new ClusterName("test")).nodes(DiscoveryNodes.builder()).build();
        when(clusterApplierService.state()).thenReturn(clusterState);

        final AtomicBoolean listenerAdded = new AtomicBoolean();
        doAnswer(invocation -> {
            assertNull(invocation.getArgument(0));
            listenerAdded.set(true);
            return null;
        }).when(clusterApplierService).addTimeoutListener(any(), any());

        final ClusterStateObserver observer = new ClusterStateObserver(
            clusterState,
            clusterApplierService,
            null,
            logger,
            new ThreadContext(Settings.EMPTY)
        );

        observer.waitForNextChange(new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {}

            @Override
            public void onClusterServiceClose() {}

            @Override
            public void onTimeout(TimeValue timeout) {}
        });

        assertTrue(listenerAdded.get());
    }

    /**
     * Tests the ClusterService-based constructor (default 60s timeout) delegates correctly.
     */
    public void testClusterServiceConstructorDefaultTimeout() {
        final ClusterApplierService clusterApplierService = mock(ClusterApplierService.class);
        final ThreadPool threadPool = mock(ThreadPool.class);
        when(clusterApplierService.threadPool()).thenReturn(threadPool);
        when(threadPool.relativeTimeInMillis()).thenReturn(0L);

        final ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterApplierService()).thenReturn(clusterApplierService);

        final ClusterState clusterState = ClusterState.builder(new ClusterName("test")).nodes(DiscoveryNodes.builder()).build();
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterApplierService.state()).thenReturn(clusterState);

        final ClusterStateObserver observer = new ClusterStateObserver(clusterService, logger, new ThreadContext(Settings.EMPTY));

        assertFalse(observer.isTimedOut());
    }

    /**
     * Tests the primitive constructor with null timeout — exercises the false branch
     * of the {@code if (timeOutValue != null)} check, ensuring startTimeMS is not set.
     */
    public void testPrimitiveConstructorWithNullTimeout() {
        final ClusterApplierService clusterApplierService = mock(ClusterApplierService.class);
        final ThreadPool threadPool = mock(ThreadPool.class);
        when(clusterApplierService.threadPool()).thenReturn(threadPool);

        final DiscoveryNode masterNode = new DiscoveryNode("master", buildNewFakeTransportAddress(), Version.CURRENT);
        final ClusterState newerState = ClusterState.builder(new ClusterName("test"))
            .nodes(DiscoveryNodes.builder().add(masterNode).clusterManagerNodeId(masterNode.getId()))
            .version(5)
            .build();
        when(clusterApplierService.state()).thenReturn(newerState);

        // Pass null timeout — should skip the startTimeMS assignment
        final ClusterStateObserver observer = new ClusterStateObserver(
            masterNode.getId(),
            1L,
            clusterApplierService,
            null,
            logger,
            new ThreadContext(Settings.EMPTY)
        );

        assertNull(observer.startTimeMS);
        assertFalse(observer.isTimedOut());

        final AtomicReference<ClusterState> receivedState = new AtomicReference<>();
        observer.waitForNextChange(new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {
                receivedState.set(state);
            }

            @Override
            public void onClusterServiceClose() {}

            @Override
            public void onTimeout(TimeValue timeout) {}
        });

        assertNotNull(receivedState.get());
        assertEquals(5L, receivedState.get().version());
    }

}
