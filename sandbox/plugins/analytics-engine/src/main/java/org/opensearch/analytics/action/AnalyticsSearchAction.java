/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ActionType;
import org.opensearch.action.ValidateActions;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.analytics.exec.AnalyticsSearchService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.indices.IndicesService;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * Minimal analytics search action: POST /_analytics/search with body {index, sql}.
 * Executes on the SEARCH threadpool against the first local shard of the target index.
 */
public class AnalyticsSearchAction extends ActionType<AnalyticsSearchAction.Response> {

    public static final AnalyticsSearchAction INSTANCE = new AnalyticsSearchAction();
    public static final String NAME = "indices:data/read/analytics/search";

    private AnalyticsSearchAction() {
        super(NAME, Response::new);
    }

    /** Request carrying the target index and SQL query. */
    public static class Request extends ActionRequest {
        private final String index;
        private final String sql;

        public Request(String index, String sql) {
            this.index = index;
            this.sql = sql;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.index = in.readString();
            this.sql = in.readString();
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException e = null;
            if (index == null || index.isEmpty()) {
                e = ValidateActions.addValidationError("index is required", e);
            }
            if (sql == null || sql.isEmpty()) {
                e = ValidateActions.addValidationError("sql is required", e);
            }
            return e;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(index);
            out.writeString(sql);
        }

        public String getIndex() {
            return index;
        }

        public String getSql() {
            return sql;
        }
    }

    /** Response containing field names and rows. */
    public static class Response extends ActionResponse implements ToXContentObject {
        private final List<String> fieldNames;
        private final List<Object[]> rows;

        public Response(List<String> fieldNames, List<Object[]> rows) {
            this.fieldNames = fieldNames;
            this.rows = rows;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            this.fieldNames = in.readStringList();
            int n = in.readVInt();
            this.rows = new java.util.ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                this.rows.add(in.readArray(StreamInput::readGenericValue, Object[]::new));
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeStringCollection(fieldNames);
            out.writeVInt(rows.size());
            for (Object[] row : rows) {
                out.writeArray(StreamOutput::writeGenericValue, row);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("fields", fieldNames);
            builder.startArray("rows");
            for (Object[] row : rows) {
                builder.startArray();
                for (Object v : row) {
                    // XContent supports primitives, String, Number, Boolean. Arrow returns Text/ByteBuffer for
                    // variable-width types — convert anything unrecognized to its string form.
                    if (v == null || v instanceof String || v instanceof Number || v instanceof Boolean) {
                        builder.value(v);
                    } else {
                        builder.value(v.toString());
                    }
                }
                builder.endArray();
            }
            builder.endArray();
            builder.endObject();
            return builder;
        }
    }

    /** Transport action: delegates to {@link AnalyticsSearchService} which runs on the search threadpool. */
    public static class TransportAction extends HandledTransportAction<Request, Response> {

        private final AnalyticsSearchService service;

        @Inject
        public TransportAction(
            TransportService transportService,
            ActionFilters actionFilters,
            ThreadPool threadPool,
            IndicesService indicesService,
            ClusterService clusterService,
            AnalyticsBackends backends
        ) {
            super(NAME, transportService, actionFilters, Request::new);
            this.service = new AnalyticsSearchService(backends.get(), indicesService, clusterService, threadPool);
        }

        @Override
        protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
            try {
                AnalyticsSearchService.Result result = service.executeSql(request.getIndex(), request.getSql());
                listener.onResponse(new Response(result.getFieldNames(), result.getRows()));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }
    }

    /** REST handler: POST /_analytics/search with JSON body {"index":"...","sql":"..."}. */
    public static class RestAction extends BaseRestHandler {

        @Override
        public String getName() {
            return "analytics_search_action";
        }

        @Override
        public List<Route> routes() {
            return singletonList(new Route(POST, "/_analytics/search"));
        }

        @Override
        protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
            Map<String, Object> body = request.contentParser().map();
            String index = (String) body.get("index");
            String sql = (String) body.get("sql");
            Request r = new Request(index, sql);
            return channel -> client.execute(INSTANCE, r, new RestToXContentListener<>(channel));
        }
    }
}
