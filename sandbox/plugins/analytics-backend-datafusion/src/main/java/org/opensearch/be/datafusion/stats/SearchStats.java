/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.stats;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class SearchStats implements Writeable, ToXContentFragment {

    public final long elapsedComputeMs;
    public final long delegationCalls;
    public final long rgProcessed;
    public final long rgSkipped;
    public final long prefetchWaitTimeMs;
    public final long prefetchWaitCount;
    public final long onBatchMaskTimeMs;
    public final long parquetPollTimeMs;
    public final long buildMaskTimeMs;
    public final long filterRecordBatchTimeMs;
    public final long parquetTimeMs;
    public final long listingTableScan;
    public final long singleCollectorScan;
    public final long bitmapTreeScan;

    public SearchStats(
        long elapsedComputeMs,
        long delegationCalls,
        long rgProcessed,
        long rgSkipped,
        long prefetchWaitTimeMs,
        long prefetchWaitCount,
        long onBatchMaskTimeMs,
        long parquetPollTimeMs,
        long buildMaskTimeMs,
        long filterRecordBatchTimeMs,
        long parquetTimeMs,
        long listingTableScan,
        long singleCollectorScan,
        long bitmapTreeScan
    ) {
        this.elapsedComputeMs = elapsedComputeMs;
        this.delegationCalls = delegationCalls;
        this.rgProcessed = rgProcessed;
        this.rgSkipped = rgSkipped;
        this.prefetchWaitTimeMs = prefetchWaitTimeMs;
        this.prefetchWaitCount = prefetchWaitCount;
        this.onBatchMaskTimeMs = onBatchMaskTimeMs;
        this.parquetPollTimeMs = parquetPollTimeMs;
        this.buildMaskTimeMs = buildMaskTimeMs;
        this.filterRecordBatchTimeMs = filterRecordBatchTimeMs;
        this.parquetTimeMs = parquetTimeMs;
        this.listingTableScan = listingTableScan;
        this.singleCollectorScan = singleCollectorScan;
        this.bitmapTreeScan = bitmapTreeScan;
    }

    public SearchStats(StreamInput in) throws IOException {
        this.elapsedComputeMs = in.readVLong();
        this.delegationCalls = in.readVLong();
        this.rgProcessed = in.readVLong();
        this.rgSkipped = in.readVLong();
        this.prefetchWaitTimeMs = in.readVLong();
        this.prefetchWaitCount = in.readVLong();
        this.onBatchMaskTimeMs = in.readVLong();
        this.parquetPollTimeMs = in.readVLong();
        this.buildMaskTimeMs = in.readVLong();
        this.filterRecordBatchTimeMs = in.readVLong();
        this.parquetTimeMs = in.readVLong();
        this.listingTableScan = in.readVLong();
        this.singleCollectorScan = in.readVLong();
        this.bitmapTreeScan = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(elapsedComputeMs);
        out.writeVLong(delegationCalls);
        out.writeVLong(rgProcessed);
        out.writeVLong(rgSkipped);
        out.writeVLong(prefetchWaitTimeMs);
        out.writeVLong(prefetchWaitCount);
        out.writeVLong(onBatchMaskTimeMs);
        out.writeVLong(parquetPollTimeMs);
        out.writeVLong(buildMaskTimeMs);
        out.writeVLong(filterRecordBatchTimeMs);
        out.writeVLong(parquetTimeMs);
        out.writeVLong(listingTableScan);
        out.writeVLong(singleCollectorScan);
        out.writeVLong(bitmapTreeScan);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("search_stats");
        builder.field("elapsed_compute_ms", elapsedComputeMs);
        builder.field("delegation_calls", delegationCalls);
        builder.field("rg_processed", rgProcessed);
        builder.field("rg_skipped", rgSkipped);
        builder.field("prefetch_wait_time_ms", prefetchWaitTimeMs);
        builder.field("prefetch_wait_count", prefetchWaitCount);
        builder.field("on_batch_mask_time_ms", onBatchMaskTimeMs);
        builder.field("parquet_poll_time_ms", parquetPollTimeMs);
        builder.field("build_mask_time_ms", buildMaskTimeMs);
        builder.field("filter_record_batch_time_ms", filterRecordBatchTimeMs);
        builder.field("parquet_time_ms", parquetTimeMs);
        builder.field("listing_table_scan", listingTableScan);
        builder.field("single_collector_scan", singleCollectorScan);
        builder.field("bitmap_tree_scan", bitmapTreeScan);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SearchStats that = (SearchStats) o;
        return elapsedComputeMs == that.elapsedComputeMs
            && delegationCalls == that.delegationCalls
            && rgProcessed == that.rgProcessed
            && rgSkipped == that.rgSkipped
            && prefetchWaitTimeMs == that.prefetchWaitTimeMs
            && prefetchWaitCount == that.prefetchWaitCount
            && onBatchMaskTimeMs == that.onBatchMaskTimeMs
            && parquetPollTimeMs == that.parquetPollTimeMs
            && buildMaskTimeMs == that.buildMaskTimeMs
            && filterRecordBatchTimeMs == that.filterRecordBatchTimeMs
            && parquetTimeMs == that.parquetTimeMs
            && listingTableScan == that.listingTableScan
            && singleCollectorScan == that.singleCollectorScan
            && bitmapTreeScan == that.bitmapTreeScan;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            elapsedComputeMs,
            delegationCalls,
            rgProcessed,
            rgSkipped,
            prefetchWaitTimeMs,
            prefetchWaitCount,
            onBatchMaskTimeMs,
            parquetPollTimeMs,
            buildMaskTimeMs,
            filterRecordBatchTimeMs,
            parquetTimeMs,
            listingTableScan,
            singleCollectorScan,
            bitmapTreeScan
        );
    }
}
