/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Cumulative search execution counters exposed via `df_stats()`.

use std::sync::atomic::{AtomicI64, Ordering};

use crate::indexed_table::metrics::StreamMetrics;
use crate::stats::SearchStatsRepr;

static ELAPSED_COMPUTE_MS: AtomicI64 = AtomicI64::new(0);
static DELEGATION_CALLS: AtomicI64 = AtomicI64::new(0);
static RG_PROCESSED: AtomicI64 = AtomicI64::new(0);
static RG_SKIPPED: AtomicI64 = AtomicI64::new(0);
static PREFETCH_WAIT_TIME_MS: AtomicI64 = AtomicI64::new(0);
static PREFETCH_WAIT_COUNT: AtomicI64 = AtomicI64::new(0);
static ON_BATCH_MASK_TIME_MS: AtomicI64 = AtomicI64::new(0);
static PARQUET_POLL_TIME_MS: AtomicI64 = AtomicI64::new(0);
static BUILD_MASK_TIME_MS: AtomicI64 = AtomicI64::new(0);
static FILTER_RECORD_BATCH_TIME_MS: AtomicI64 = AtomicI64::new(0);
static PARQUET_TIME_MS: AtomicI64 = AtomicI64::new(0);
static LISTING_TABLE_SCAN: AtomicI64 = AtomicI64::new(0);
static SINGLE_COLLECTOR_SCAN: AtomicI64 = AtomicI64::new(0);
static BITMAP_TREE_SCAN: AtomicI64 = AtomicI64::new(0);

pub fn inc_listing_table_scan() {
    LISTING_TABLE_SCAN.fetch_add(1, Ordering::Relaxed);
}

pub fn inc_single_collector_scan() {
    SINGLE_COLLECTOR_SCAN.fetch_add(1, Ordering::Relaxed);
}

pub fn inc_bitmap_tree_scan() {
    BITMAP_TREE_SCAN.fetch_add(1, Ordering::Relaxed);
}

pub fn accumulate(m: &StreamMetrics) {
    if let Some(ref t) = m.elapsed_compute {
        ELAPSED_COMPUTE_MS.fetch_add(t.value() as i64, Ordering::Relaxed);
    }
    if let Some(ref c) = m.ffm_collector_calls {
        DELEGATION_CALLS.fetch_add(c.value() as i64, Ordering::Relaxed);
    }
    if let Some(ref c) = m.rg_processed {
        RG_PROCESSED.fetch_add(c.value() as i64, Ordering::Relaxed);
    }
    if let Some(ref c) = m.rg_skipped {
        RG_SKIPPED.fetch_add(c.value() as i64, Ordering::Relaxed);
    }
    if let Some(ref t) = m.prefetch_wait_time {
        PREFETCH_WAIT_TIME_MS.fetch_add(t.value() as i64, Ordering::Relaxed);
    }
    if let Some(ref t) = m.mask_slice_time {
        // Hack: reuse prefetch_wait_count slot for mask_slice_time on test branch
        PREFETCH_WAIT_COUNT.fetch_add(t.value() as i64, Ordering::Relaxed);
    }
    if let Some(ref t) = m.on_batch_mask_time {
        ON_BATCH_MASK_TIME_MS.fetch_add(t.value() as i64, Ordering::Relaxed);
    }
    if let Some(ref t) = m.projection_fixup_time {
        // Hack: reuse listing_table_scan slot for projection_fixup_time on test branch
        LISTING_TABLE_SCAN.fetch_add(t.value() as i64, Ordering::Relaxed);
    }
    if let Some(ref t) = m.parquet_poll_time {
        PARQUET_POLL_TIME_MS.fetch_add(t.value() as i64, Ordering::Relaxed);
    }
    if let Some(ref t) = m.build_mask_time {
        BUILD_MASK_TIME_MS.fetch_add(t.value() as i64, Ordering::Relaxed);
    }
    if let Some(ref t) = m.filter_record_batch_time {
        FILTER_RECORD_BATCH_TIME_MS.fetch_add(t.value() as i64, Ordering::Relaxed);
    }
    if let Some(ref t) = m.parquet_time {
        PARQUET_TIME_MS.fetch_add(t.value() as i64, Ordering::Relaxed);
    }
}

pub fn snapshot() -> SearchStatsRepr {
    SearchStatsRepr {
        elapsed_compute_ms: ELAPSED_COMPUTE_MS.load(Ordering::Relaxed),
        delegation_calls: DELEGATION_CALLS.load(Ordering::Relaxed),
        rg_processed: RG_PROCESSED.load(Ordering::Relaxed),
        rg_skipped: RG_SKIPPED.load(Ordering::Relaxed),
        prefetch_wait_time_ms: PREFETCH_WAIT_TIME_MS.load(Ordering::Relaxed),
        prefetch_wait_count: PREFETCH_WAIT_COUNT.load(Ordering::Relaxed),
        on_batch_mask_time_ms: ON_BATCH_MASK_TIME_MS.load(Ordering::Relaxed),
        parquet_poll_time_ms: PARQUET_POLL_TIME_MS.load(Ordering::Relaxed),
        build_mask_time_ms: BUILD_MASK_TIME_MS.load(Ordering::Relaxed),
        filter_record_batch_time_ms: FILTER_RECORD_BATCH_TIME_MS.load(Ordering::Relaxed),
        parquet_time_ms: PARQUET_TIME_MS.load(Ordering::Relaxed),
        listing_table_scan: LISTING_TABLE_SCAN.load(Ordering::Relaxed),
        single_collector_scan: SINGLE_COLLECTOR_SCAN.load(Ordering::Relaxed),
        bitmap_tree_scan: BITMAP_TREE_SCAN.load(Ordering::Relaxed),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexed_table::metrics::PartitionMetrics;
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;

    #[test]
    fn path_counters_increment() {
        let before = snapshot();
        inc_listing_table_scan();
        inc_single_collector_scan();
        inc_single_collector_scan();
        inc_bitmap_tree_scan();
        let after = snapshot();
        assert_eq!(after.listing_table_scan - before.listing_table_scan, 1);
        assert_eq!(after.single_collector_scan - before.single_collector_scan, 2);
        assert_eq!(after.bitmap_tree_scan - before.bitmap_tree_scan, 1);
    }

    #[test]
    fn accumulate_folds_partition_metrics() {
        let before = snapshot();
        let metrics_set = ExecutionPlanMetricsSet::new();
        let pm = PartitionMetrics::new(&metrics_set, 0);

        pm.elapsed_compute.add_duration(std::time::Duration::from_millis(50));
        pm.ffm_collector_calls.add(3);
        pm.row_groups_processed.add(2);
        pm.row_groups_skipped.add(1);
        pm.prefetch_wait_time.add_duration(std::time::Duration::from_millis(10));
        pm.prefetch_wait_count.add(2);

        accumulate(&pm.into_stream_metrics(None));
        let after = snapshot();

        assert_eq!(after.delegation_calls - before.delegation_calls, 3);
        assert_eq!(after.rg_processed - before.rg_processed, 2);
        assert_eq!(after.rg_skipped - before.rg_skipped, 1);
        assert!(after.prefetch_wait_time_ms - before.prefetch_wait_time_ms >= 10);
        assert_eq!(after.prefetch_wait_count - before.prefetch_wait_count, 2);
    }

    #[test]
    fn empty_stream_metrics_is_safe() {
        let before = snapshot();
        accumulate(&StreamMetrics::empty());
        let after = snapshot();
        assert_eq!(after.delegation_calls, before.delegation_calls);
    }
}
