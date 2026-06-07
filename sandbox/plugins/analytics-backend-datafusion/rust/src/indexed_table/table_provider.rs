/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Aache-2.0 license or a
 * compatible open source license.
 */

//! Unified DataFusion `TableProvider` for all indexed-query paths.
//!
//! This is the ONE provider. Paths B and C differ only in the evaluator
//! factory closure supplied in `IndexedTableConfig`. The provider itself,
//! the `QueryShardExec` it wraps, and the `IndexedExec`s it spawns are
//! identical across paths.
//!
//! ```text
//!     IndexedTableProvider (scan)
//!             │
//!             ▼
//!     QueryShardExec (1 per query, partitioned across chunks)
//!             │
//!             ├── IndexedExec(chunk_0) ── IndexedStream ── RowGroupBitsetSource
//!             ├── IndexedExec(chunk_1) ── IndexedStream ── RowGroupBitsetSource
//!             └── IndexedExec(chunk_N) ── IndexedStream ── RowGroupBitsetSource
//! ```

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{Result, Statistics};
use datafusion::datasource::TableType;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_common::DataFusionError;

use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::StreamExt;

use super::eval::RowGroupBitsetSource;
use super::metrics::PartitionMetrics;
use super::partitioning::{compute_assignments, PartitionAssignment, SegmentChunk, SegmentLayout};
use super::stream::{FilterStrategy, IndexedExec, RowGroupInfo};
use crate::datafusion_query_config::DatafusionQueryConfig;
use crate::indexed_table::metrics::StreamMetrics;
use std::collections::HashSet;
use std::pin::Pin;
use std::task::{Context, Poll};

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::RecordBatchStream;
use futures::Stream;

/// Info about a segment and its corresponding parquet file.
#[derive(Debug, Clone)]
pub struct SegmentFileInfo {
    /// Writer generation for this segment — the stable per-segment identifier
    /// that crosses the FFM boundary to identify a segment on the Java side.
    /// Read from the parquet footer key-value metadata
    /// (`opensearch.writer_generation`) at `build_segments` time.
    pub writer_generation: i64,
    pub max_doc: i64,
    /// Object-store-relative path to the parquet file (same as the
    /// `ObjectMeta.location` DataFusion uses for the vanilla `ListingTable`).
    pub object_path: object_store::path::Path,
    pub parquet_size: u64,
    pub row_groups: Vec<RowGroupInfo>,
    pub metadata: Arc<ParquetMetaData>,
    /// Cumulative row count from all preceding segments. Used to compute
    /// shard-global row IDs: `global_base + rg.first_row + position_in_rg`.
    pub global_base: u64,
}

/// Factory: build a `RowGroupBitsetSource` for one `SegmentChunk`.
///
/// Invoked once per chunk per query. For the single-collector path this
/// produces a `SingleCollectorEvaluator`. For the multi-filter tree path it
/// produces a `BitmapTreeEvaluator`-backed `TreeBitsetSource`.
///
/// The closure is cloneable (stored in an `Arc`) so the provider can spawn
/// many `IndexedExec`s from a single config.
///
/// # Pluggability
///
/// `RowGroupBitsetSource` is the single seam that determines *where* tree
/// evaluation happens. Today the built-in impls all walk the tree in Rust,
/// but a future `JavaTreeBitsetSource` could route per-RG evaluation to
/// analytics-core via an FFM upcall without touching `IndexedStream`,
/// `IndexedExec`, or this factory's signature. Evaluators that carry
/// cross-chunk or cross-query state (e.g. a Java-resident tree) should
/// keep that state external and reference it by handle from the evaluator.
pub type EvaluatorFactory = Arc<
    dyn Fn(
            &SegmentFileInfo,
            &SegmentChunk,
            &StreamMetrics,
        ) -> Result<Arc<dyn RowGroupBitsetSource>, String>
        + Send
        + Sync,
>;

/// Configuration used to build an `IndexedTableProvider`.
pub struct IndexedTableConfig {
    pub schema: SchemaRef,
    pub segments: Vec<SegmentFileInfo>,
    /// Object store for reading parquet bytes. All I/O on the indexed path
    /// goes through this same store resolution as vanilla — no hardcoded
    /// LocalFileSystem. Resolved once per query from the runtime env.
    pub store: Arc<dyn object_store::ObjectStore>,
    /// URL of the store for DataFusion's `FileScanConfig`.
    pub store_url: datafusion::execution::object_store::ObjectStoreUrl,
    pub evaluator_factory: EvaluatorFactory,
    /// Parquet-native residual predicate to push into decode time via
    /// `ParquetSource::with_predicate`. Derived from the BoolNode tree
    /// by `execute_indexed_query`:
    /// - `FilterClass::SingleCollector`: residual (non-Collector
    ///   children of top AND) as a single `PhysicalExpr`.
    /// - `FilterClass::Tree`: `None` (BitmapTreeEvaluator does all
    ///   refinement in `on_batch_mask`; pushdown would risk invoking
    ///   the `index_filter` UDF).
    ///
    /// `scan()` uses this rather than the `filters` argument it
    /// receives from DataFusion, because DataFusion's filters include
    /// the `index_filter(...)` UDF marker whose body panics.
    pub pushdown_predicate: Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
    /// Query-scoped tunables (batch_size, target_partitions, costs, …).
    /// Shared by reference across fanned-out `QueryShardExec` instances.
    pub query_config: Arc<DatafusionQueryConfig>,
    /// Full-schema column indices referenced by BoolNode Predicate leaves.
    pub predicate_columns: Vec<usize>,
    /// When true, the `___row_id` column in the output projection is computed
    /// from position (global_base + rg.first_row + position_in_rg) instead of
    /// being read from parquet. Other projected columns are read normally.
    pub emit_row_ids: bool,
}

/// Table provider. Returns a `QueryShardExec` that fans out across chunks.
pub struct IndexedTableProvider {
    config: Arc<IndexedTableConfig>,
}

impl fmt::Debug for IndexedTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IndexedTableProvider")
            .field("segments", &self.config.segments.len())
            .field("partitions", &self.config.query_config.target_partitions)
            .finish()
    }
}

impl IndexedTableProvider {
    pub fn new(config: IndexedTableConfig) -> Self {
        Self {
            config: Arc::new(config),
        }
    }
}

#[async_trait]
impl TableProvider for IndexedTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.config.schema.clone()
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        // `Exact` — the BoolNode tree held by the evaluator factory
        // fully handles every WHERE filter (Collectors via FFM bitsets,
        // Predicates via arrow kernels in refinement). DataFusion
        // removes the outer FilterExec, which is important because
        // otherwise FilterExec would try to evaluate the
        // `index_filter(...)` UDF whose body panics by design.
        Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let full_schema = self.config.schema.clone();

        // Detect __row_id__ in the output projection when emit_row_ids=true.
        // If present, we strip it from the parquet read and compute it from position.
        let row_id_col_in_full_schema = full_schema.index_of(crate::ROW_ID_COLUMN_NAME).ok();
        let row_id_output_index: Option<usize> = if self.config.emit_row_ids {
            match projection {
                Some(proj) => proj.iter().position(|&idx| Some(idx) == row_id_col_in_full_schema),
                None => row_id_col_in_full_schema,
            }
        } else {
            None
        };

        // Output schema = what DataFusion expects (includes ___row_id if projected).
        // When computing row IDs, replace the ___row_id field type with UInt64.
        let output_schema: SchemaRef = {
            let base: SchemaRef = match projection {
                Some(proj) => Arc::new(full_schema.project(proj)?),
                None => full_schema.clone(),
            };
            if let Some(idx) = row_id_output_index {
                let mut fields: Vec<Field> = base.fields().iter().map(|f| f.as_ref().clone()).collect();
                fields[idx] = Field::new(crate::ROW_ID_COLUMN_NAME, DataType::Int64, false);
                Arc::new(Schema::new(fields))
            } else {
                base
            }
        };

        // Read projection = output columns (minus ___row_id) + predicate columns for evaluator.
        let read_projection: Option<Vec<usize>> = if self.config.emit_row_ids {
            let output_cols: Vec<usize> = match projection {
                Some(proj) => proj.iter()
                    .filter(|&&idx| Some(idx) != row_id_col_in_full_schema)
                    .copied()
                    .collect(),
                None => (0..full_schema.fields().len())
                    .filter(|&idx| Some(idx) != row_id_col_in_full_schema)
                    .collect(),
            };
            let mut cols = output_cols;
            for &idx in &self.config.predicate_columns {
                if !cols.contains(&idx) {
                    cols.push(idx);
                }
            }
            cols.sort();
            Some(cols)
        } else if self.config.predicate_columns.is_empty() {
            projection.cloned()
        } else {
            projection.map(|proj| {
                let mut cols = proj.clone();
                for &idx in &self.config.predicate_columns {
                    if !cols.contains(&idx) {
                        cols.push(idx);
                    }
                }
                cols.sort();
                cols
            })
        };

        let projected_schema = output_schema;

        // Ignore DataFusion's `filters` argument. The `index_filter(...)`
        // UDF call would be in there (its body panics), and the
        // BoolNode tree held by the evaluator factory already contains
        // the full WHERE semantics.
        //
        // The pushdown predicate — the parquet-native residual to hand
        // to `ParquetSource::with_predicate` — is derived from the
        // BoolNode in `execute_indexed_query` and stashed on the
        // config by that caller.
        let predicate = self.config.pushdown_predicate.clone();

        // Row-group-aligned partition assignments
        let layouts: Vec<SegmentLayout> = self
            .config
            .segments
            .iter()
            .map(|seg| SegmentLayout {
                row_groups: seg.row_groups.clone(),
            })
            .collect();
        let assignments =
            compute_assignments(&layouts, self.config.query_config.target_partitions.max(1));

        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(projected_schema.clone()),
            Partitioning::UnknownPartitioning(assignments.len().max(1)),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));

        Ok(Arc::new(QueryShardExec {
            config: Arc::clone(&self.config),
            full_schema,
            projected_schema,
            projection: read_projection,
            assignments,
            properties,
            predicate,
            metrics: ExecutionPlanMetricsSet::new(),
            inner_parquet_metrics: Arc::new(std::sync::Mutex::new(Vec::new())),
            row_id_output_index,
        }))
    }

    fn statistics(&self) -> Option<Statistics> {
        None
    }
}

// ── QueryShardExec ───────────────────────────────────────────────────

/// One execution plan per query. Partitions into `assignments.len()` streams,
/// each backed by one or more `IndexedExec`s (chained per-chunk).
pub struct QueryShardExec {
    config: Arc<IndexedTableConfig>,
    full_schema: SchemaRef,
    projected_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    assignments: Vec<PartitionAssignment>,
    properties: Arc<PlanProperties>,
    /// Residual physical predicate pushed down from the planner. Threaded
    /// into each `IndexedExec` so `ParquetSource.with_predicate(...)` can
    /// apply it during decode.
    predicate: Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
    metrics: ExecutionPlanMetricsSet,
    inner_parquet_metrics: Arc<std::sync::Mutex<Vec<MetricsSet>>>,
    /// Column index in the OUTPUT schema where computed `___row_id` should be
    /// injected. `None` means no row ID computation (normal data path).
    row_id_output_index: Option<usize>,
}

impl fmt::Debug for QueryShardExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueryShardExec")
            .field("partitions", &self.assignments.len())
            .field("segments", &self.config.segments.len())
            .finish()
    }
}

impl DisplayAs for QueryShardExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "QueryShardExec: partitions={}, segments={}",
            self.assignments.len(),
            self.config.segments.len(),
        )
    }
}

impl ExecutionPlan for QueryShardExec {
    fn name(&self) -> &str {
        "QueryShardExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }
    fn metrics(&self) -> Option<MetricsSet> {
        let mut combined = self.metrics.clone_inner();
        if let Ok(inner) = self.inner_parquet_metrics.lock() {
            for set in inner.iter() {
                for m in set.iter() {
                    let name = m.value().name();
                    if name == "output_rows" || name == "output_batches" || name == "output_bytes" {
                        continue;
                    }
                    combined.push(m.clone());
                }
            }
        }
        Some(combined)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let t_execute = std::time::Instant::now();

        let assignment = self.assignments.get(partition).ok_or_else(|| {
            DataFusionError::Internal(format!("partition {} out of range", partition))
        })?;

        native_bridge_common::log_info!(
            "[QueryShardExec::execute] starting execution: partition={} num_chunks={}",
            partition, assignment.chunks.len()
        );

        let pmetrics = PartitionMetrics::new(&self.metrics, partition);
        let stream_metrics =
            pmetrics.into_stream_metrics(Some(Arc::clone(&self.inner_parquet_metrics)));
        let stream_metrics_for_drop = stream_metrics.clone();

        // Build one IndexedExec per SegmentChunk and execute it immediately,
        // collecting per-chunk streams. We then chain them sequentially into
        // a single stream for this partition. This avoids the
        // UnionExec + CoalescePartitionsExec wrapping (which would re-shape
        // partitioning and add an extra coalesce hop) — chunks here are
        // already serialized within one partition assignment.
        let mut streams: Vec<SendableRecordBatchStream> =
            Vec::with_capacity(assignment.chunks.len());
        for (chunk_idx, chunk) in assignment.chunks.iter().enumerate() {
            let t_chunk = std::time::Instant::now();
            let segment = self.config.segments.get(chunk.segment_idx).ok_or_else(|| {
                DataFusionError::Internal(format!("segment_idx {} out of range", chunk.segment_idx))
            })?;

            // Subset the segment's row groups to just this chunk's.
            let rg_set: HashSet<usize> = chunk.row_group_indices.iter().copied().collect();
            let row_groups: Vec<RowGroupInfo> = segment
                .row_groups
                .iter()
                .filter(|rg| rg_set.contains(&rg.index))
                .cloned()
                .collect();

            if row_groups.is_empty() {
                native_bridge_common::log_info!(
                    "[QueryShardExec::execute] chunk has no row_groups, skipping: partition={} chunk_idx={}",
                    partition, chunk_idx
                );
                continue;
            }

            let t_eval = std::time::Instant::now();
            // Build evaluator for this chunk.
            let evaluator = (self.config.evaluator_factory)(segment, chunk, &stream_metrics)
                .map_err(|e| DataFusionError::External(e.into()))?;
            native_bridge_common::log_info!(
                "[QueryShardExec::execute] time for evaluator_factory(): partition={} chunk_idx={} num_row_groups={} doc_range=[{},{}] elapsed={:.3}ms",
                partition, chunk_idx, row_groups.len(), chunk.doc_min, chunk.doc_max,
                t_eval.elapsed().as_nanos() as f64 / 1_000_000.0
            );

            let props = Arc::new(PlanProperties::new(
                EquivalenceProperties::new(self.projected_schema.clone()),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ));

            let exec = IndexedExec {
                schema: self.projected_schema.clone(),
                full_schema: self.full_schema.clone(),
                object_path: segment.object_path.clone(),
                file_size: segment.parquet_size,
                store: Arc::clone(&self.config.store),
                store_url: self.config.store_url.clone(),
                row_groups,
                projection: self.projection.clone(),
                properties: props,
                metadata: Arc::clone(&segment.metadata),
                predicate: self.predicate.clone(),
                evaluator: std::sync::Mutex::new(Some(evaluator)),
                doc_range: Some((chunk.doc_min, chunk.doc_max)),
                metrics: ExecutionPlanMetricsSet::new(),
                stream_metrics: stream_metrics.clone(),
                query_config: Arc::clone(&self.config.query_config),
                global_base: segment.global_base,
                emit_row_ids: self.config.emit_row_ids,
                row_id_output_index: self.row_id_output_index,
            };

            let t_exec = std::time::Instant::now();
            streams.push(exec.execute(0, Arc::clone(&context))?);
            native_bridge_common::log_info!(
                "[QueryShardExec::execute] time for IndexedExec::execute (creates IndexedStream): partition={} chunk_idx={} elapsed={:.3}ms chunk_total={:.3}ms",
                partition, chunk_idx,
                t_exec.elapsed().as_nanos() as f64 / 1_000_000.0,
                t_chunk.elapsed().as_nanos() as f64 / 1_000_000.0
            );
        }

        // Chain the per-chunk streams into one stream for this partition
        // (Arpit's fix-table change: avoids the UnionExec +
        // CoalescePartitionsExec wrapping that re-shaped partitioning).
        let inner: SendableRecordBatchStream = match streams.len() {
            0 => {
                native_bridge_common::log_info!(
                    "[QueryShardExec::execute] all chunks empty, using EmptyExec: partition={}",
                    partition
                );
                let empty = datafusion::physical_plan::empty::EmptyExec::new(
                    self.projected_schema.clone(),
                );
                empty.execute(0, context)?
            }
            1 => streams.into_iter().next().unwrap(),
            _ => {
                let schema = self.projected_schema.clone();
                let chained = futures::stream::iter(streams).flatten();
                Box::pin(RecordBatchStreamAdapter::new(schema, chained))
            }
        };

        native_bridge_common::log_info!(
            "[QueryShardExec::execute] time for entire execute(): partition={} num_streams={} elapsed={:.3}ms",
            partition, assignment.chunks.len(),
            t_execute.elapsed().as_nanos() as f64 / 1_000_000.0
        );

        // Wrap so the Drop impl flushes search_stats for this query.
        Ok(Box::pin(AccumulatingStream {
            inner,
            stream_metrics: stream_metrics_for_drop,
            created_at: std::time::Instant::now(),
        }))
    }
}

struct AccumulatingStream {
    inner: SendableRecordBatchStream,
    stream_metrics: StreamMetrics,
    /// Wall-clock start of the stream's lifetime. Used at Drop to report
    /// the total wall time (active poll work + parked/idle), which is the
    /// correct denominator for "is the hot path fully accfsounted".
    created_at: std::time::Instant,
}

impl Stream for AccumulatingStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}

impl RecordBatchStream for AccumulatingStream {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

impl Drop for AccumulatingStream {
    fn drop(&mut self) {
        // Debug: print the timing breakdown and how much of the stream's
        // wall-clock lifetime is accounted for. No new partition metrics —
        // just reads what we already record, plus the stream wall-clock.
        let m = &self.stream_metrics;
        use datafusion::physical_plan::metrics::Time;
        let to_ms = |t: &Option<Time>| -> f64 {
            t.as_ref().map_or(0usize, |v| v.value()) as f64 / 1_000_000.0
        };
        let wall = self.created_at.elapsed().as_nanos() as f64 / 1_000_000.0;
        let elapsed = to_ms(&m.elapsed_compute);
        let parquet_poll = to_ms(&m.parquet_poll_time);
        let parquet_time = to_ms(&m.parquet_time);
        let on_batch_mask = to_ms(&m.on_batch_mask_time);
        let build_mask = to_ms(&m.build_mask_time);
        let filter_rb = to_ms(&m.filter_record_batch_time);
        let prefetch_wait = to_ms(&m.prefetch_wait_time);
        // index_time = total Lucene/index collection time: sum of each RG's
        // evaluator eval_nanos (the FFM upcall into Lucene + bitset build).
        // This is the "overall Lucene collection time" metric; it runs on the
        // background prefetch thread, overlapped with the hot path.
        let index_time = to_ms(&m.index_time);
        let init_prefetch = to_ms(&m.init_prefetch_time);
        let inter_poll_gap = to_ms(&m.inter_poll_gap);
        let poll_count = m.poll_count.as_ref().map_or(0, |c| c.value());
        // fg_pct: foreground in-poll work vs elapsed_compute (active poll time).
        // init_prefetch runs inside the first poll (so inside elapsed_compute),
        // so it's part of the fg sum.
        let fg = parquet_poll + parquet_time + on_batch_mask + build_mask + filter_rb + init_prefetch;
        let fg_pct = if elapsed > 0.0 { fg / elapsed * 100.0 } else { 0.0 };
        native_bridge_common::log_info!(
            "[stream-drop] wall={:.3}ms elapsed={:.3}ms prefetch_wait={:.3}ms inter_poll_gap={:.3}ms poll_count={} index_time={:.3}ms | parquet_poll={:.3}ms parquet_time={:.3}ms on_batch_mask={:.3}ms build_mask={:.3}ms filter_rb={:.3}ms init_prefetch={:.3}ms | fg={:.3}ms fg_pct={:.1}% (of elapsed)",
            wall,
            elapsed,
            prefetch_wait,
            inter_poll_gap,
            poll_count,
            index_time,
            parquet_poll,
            parquet_time,
            on_batch_mask,
            build_mask,
            filter_rb,
            init_prefetch,
            fg,
            fg_pct,
        );
        crate::search_stats::accumulate(&self.stream_metrics);
    }
}

#[cfg(test)]
impl QueryShardExec {
    /// Test-only accessor for the conjoined physical predicate produced
    /// by `scan()`. `None` when no filters were pushed down.
    pub(crate) fn test_predicate(
        &self,
    ) -> Option<&Arc<dyn datafusion::physical_expr::PhysicalExpr>> {
        self.predicate.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::logical_expr::{col, lit};
    use datafusion::prelude::SessionContext;

    fn empty_config() -> IndexedTableConfig {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ]));
        IndexedTableConfig {
            schema,
            segments: Vec::new(),
            store: Arc::new(object_store::local::LocalFileSystem::new()),
            store_url: datafusion::execution::object_store::ObjectStoreUrl::local_filesystem(),
            // Evaluator factory would never be invoked for this test (no segments).
            evaluator_factory: Arc::new(|_, _, _| unreachable!()),
            pushdown_predicate: None,
            query_config: std::sync::Arc::new(
                crate::datafusion_query_config::DatafusionQueryConfig::test_default(),
            ),
            predicate_columns: vec![],
            emit_row_ids: false,
        }
    }

    // QueryShardExec holds an ExecutionPlanMetricsSet (not Clone). We only
    // need to inspect `.predicate`, so read through a reference.
    async fn scan_predicate(
        provider: &IndexedTableProvider,
        filters: &[Expr],
    ) -> Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>> {
        let ctx = SessionContext::new();
        let plan = provider
            .scan(&ctx.state(), None, filters, None)
            .await
            .expect("scan");
        let shard = plan
            .as_any()
            .downcast_ref::<QueryShardExec>()
            .expect("scan returns QueryShardExec");
        shard.test_predicate().cloned()
    }

    #[tokio::test]
    async fn scan_with_no_filters_produces_none_predicate() {
        let provider = IndexedTableProvider::new(empty_config());
        let pred = scan_predicate(&provider, &[]).await;
        assert!(pred.is_none(), "no filters → no predicate");
    }
}
