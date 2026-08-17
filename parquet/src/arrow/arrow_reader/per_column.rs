// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Internal per-column row-selection planning and execution.
//!
//! Planning resolves one [`RowSelectionStrategy`] per row group and projected
//! top-level Arrow field, from the selection's run statistics and a
//! metadata-only cost model. If every decision agrees, planning declines and
//! the caller keeps the existing global execution path.
//!
//! Execution gives each top-level field its own reader and replays the same
//! compiled chunks through all of them, one lane per field. Lanes issue
//! different reads and skips but must advance in lockstep; see the
//! [`window`](super::selection) module for that invariant.

use super::metrics::ArrowReaderMetrics;
use super::selection::{
    BatchWindow, ColumnInstruction, DEFAULT_ROW_SELECTION_THRESHOLD, LoadedRowRanges,
    RowSelectionExecutionPlan, RowSelectionStrategy, mask_run_count,
};
use super::{ReadPlanBuilder, RowSelection, RowSelectionPolicy};
use crate::arrow::ProjectionMask;
use crate::arrow::array_reader::{ArrayReader, ArrayReaderBuilder, RowGroups};
use crate::arrow::schema::{ParquetField, ParquetFieldType};
use crate::basic::Encoding;
use crate::errors::{ParquetError, Result};
use crate::file::metadata::RowGroupMetaData;
use crate::file::page_index::offset_index::OffsetIndexMetaData;
use arrow_array::{Array, ArrayRef, BooleanArray, StructArray, new_empty_array};
use arrow_buffer::BooleanBufferBuilder;
use arrow_schema::{DataType, Fields};
use arrow_select::filter::filter;
use std::any::Any;
use std::sync::Arc;

pub(crate) struct ColumnSelectionContext<'a> {
    /// Projected top-level field, including its nested Parquet leaves.
    pub(crate) field: &'a ParquetField,
    pub(crate) row_group: &'a RowGroupMetaData,
    /// Selection statistics for this row group, computed once and shared by
    /// every projected column.
    pub(crate) selection: RowSelectionStatistics,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct RowSelectionStatistics {
    physical_rows: usize,
    run_count: usize,
}

impl RowSelectionStatistics {
    #[inline]
    fn auto_selection_strategy(self, threshold: usize) -> RowSelectionStrategy {
        debug_assert!(threshold >= WIDE_BYTE_RUN_THRESHOLD);
        if self.run_count == 0 || self.physical_rows < self.run_count.saturating_mul(threshold) {
            RowSelectionStrategy::Mask
        } else {
            RowSelectionStrategy::Selectors
        }
    }
}

/// Internal seam for the metadata cost model. Returning `None` applies the
/// column-local compatibility fallback.
pub(crate) trait ColumnSelectionPlanner {
    fn strategy(&self, context: ColumnSelectionContext<'_>) -> Option<RowSelectionStrategy>;
}

/// Exclusive run-length thresholds calibrated by the refinement sampler.
///
/// `RowSelection::auto_selection_strategy` selects Mask when the average run
/// is below the threshold. The values below deliberately stop at the last
/// repeatable Mask win outside the sampler's 3% practical-equivalence band.
///
/// `WIDE_BYTE_RUN_THRESHOLD` is 9 rather than 5 because a wide byte column at
/// an average run of 8 still decodes faster under Mask. At 5 the heterogeneous
/// `boundary_50_run8` policy-validation case split its projection and lost
/// roughly 17% against the global `Auto` policy; at 9 it reaches a practical
/// tie. Both fixed-binary widths sampled to the same threshold, so the
/// `NARROW_FIXED_BINARY_BYTES` split currently maps to one value.
const INT32_RUN_THRESHOLD: usize = 13;
const DICTIONARY_UTF8_RUN_THRESHOLD: usize = 17;
const NARROW_BYTE_RUN_THRESHOLD: usize = 13;
const MEDIUM_BYTE_RUN_THRESHOLD: usize = 9;
const WIDE_BYTE_RUN_THRESHOLD: usize = 9;
const NARROW_UTF8_VIEW_BYTES: usize = 32;
const NARROW_FIXED_BINARY_BYTES: i32 = 8;

struct MetadataColumnSelectionPlanner;

impl ColumnSelectionPlanner for MetadataColumnSelectionPlanner {
    fn strategy(&self, context: ColumnSelectionContext<'_>) -> Option<RowSelectionStrategy> {
        let threshold = metadata_run_threshold(context.field, context.row_group)?;
        Some(context.selection.auto_selection_strategy(threshold))
    }
}

/// Returns `None` for columns outside the sampled model. The caller preserves
/// compatibility by applying the legacy global threshold to those columns.
fn metadata_run_threshold(field: &ParquetField, row_group: &RowGroupMetaData) -> Option<usize> {
    let ParquetFieldType::Primitive { col_idx, .. } = &field.field_type else {
        return None;
    };
    match &field.arrow_type {
        DataType::Int32 => Some(INT32_RUN_THRESHOLD),
        DataType::Dictionary(key, value)
            if key.as_ref() == &DataType::Int32 && value.as_ref() == &DataType::Utf8 =>
        {
            Some(DICTIONARY_UTF8_RUN_THRESHOLD)
        }
        DataType::FixedSizeBinary(width) if *width > 0 => {
            Some(if *width <= NARROW_FIXED_BINARY_BYTES {
                MEDIUM_BYTE_RUN_THRESHOLD
            } else {
                WIDE_BYTE_RUN_THRESHOLD
            })
        }
        DataType::Utf8View => {
            let column = row_group.columns().get(*col_idx)?;
            if column.encodings().any(|encoding| {
                matches!(
                    encoding,
                    Encoding::PLAIN_DICTIONARY | Encoding::RLE_DICTIONARY
                )
            }) {
                return None;
            }
            let values = usize::try_from(column.num_values()).ok()?;
            let bytes = usize::try_from(column.uncompressed_size()).ok()?;
            let narrow_bytes = values.checked_mul(NARROW_UTF8_VIEW_BYTES)?;
            if values == 0 {
                None
            } else if bytes <= narrow_bytes {
                Some(NARROW_BYTE_RUN_THRESHOLD)
            } else {
                Some(WIDE_BYTE_RUN_THRESHOLD)
            }
        }
        _ => None,
    }
}

pub(crate) enum PerColumnDecision {
    Fallback(RowSelectionStrategy),
    Engaged(PerColumnReader),
}

/// Computes the sparse page ranges available to each projected top-level
/// Arrow field. Nested leaves are intersected so a Mask fragment never asks a
/// shared reader subtree to decode through a missing page.
pub(crate) fn loaded_row_ranges_for_top_level_fields(
    fields: Option<&ParquetField>,
    projection: &ProjectionMask,
    selection: Option<&RowSelection>,
    offset_index: Option<&[OffsetIndexMetaData]>,
    total_rows: usize,
) -> Vec<Option<LoadedRowRanges>> {
    projected_top_level_fields(fields, projection)
        .into_iter()
        .map(|field| {
            let (Some(selection), Some(offset_index)) = (selection, offset_index) else {
                return None;
            };
            let mut leaves = Vec::new();
            collect_projected_leaves(field, projection, &mut leaves);
            leaves
                .into_iter()
                .filter_map(|leaf_idx| {
                    let pages = &offset_index.get(leaf_idx)?.page_locations;
                    (!pages.is_empty()).then(|| {
                        RowSelection::from_consecutive_ranges(
                            selection
                                .row_ranges_for_selected_pages(pages, total_rows)
                                .into_iter(),
                            total_rows,
                        )
                    })
                })
                .reduce(|loaded, leaf| loaded.intersection(&leaf))
                .filter(|loaded| loaded.skipped_row_count() != 0)
                .map(LoadedRowRanges::from_selection)
        })
        .collect()
}

fn projected_top_level_fields<'a>(
    fields: Option<&'a ParquetField>,
    projection: &ProjectionMask,
) -> Vec<&'a ParquetField> {
    fields
        .and_then(ParquetField::children)
        .into_iter()
        .flatten()
        .filter(|field| field_is_projected(field, projection))
        .collect()
}

fn field_is_projected(field: &ParquetField, projection: &ProjectionMask) -> bool {
    match &field.field_type {
        ParquetFieldType::Primitive { col_idx, .. } => projection.leaf_included(*col_idx),
        ParquetFieldType::Group { children } => children
            .iter()
            .any(|child| field_is_projected(child, projection)),
        ParquetFieldType::Virtual(_) => true,
    }
}

fn collect_projected_leaves(
    field: &ParquetField,
    projection: &ProjectionMask,
    leaves: &mut Vec<usize>,
) {
    match &field.field_type {
        ParquetFieldType::Primitive { col_idx, .. } => {
            if projection.leaf_included(*col_idx) {
                leaves.push(*col_idx);
            }
        }
        ParquetFieldType::Group { children } => {
            for child in children {
                collect_projected_leaves(child, projection, leaves);
            }
        }
        ParquetFieldType::Virtual(_) => {}
    }
}

struct TopLevelColumnReader {
    reader: Box<dyn ArrayReader>,
}

pub(crate) struct PerColumnReader {
    columns: Vec<TopLevelColumnReader>,
    fields: Fields,
    data_type: DataType,
    plan: Arc<RowSelectionExecutionPlan>,
    next_batch: usize,
    buffered: Option<ArrayRef>,
}

impl PerColumnReader {
    pub(super) fn try_new(
        row_groups: &dyn RowGroups,
        array_reader_builder: &ArrayReaderBuilder<'_>,
        fields: Option<&ParquetField>,
        projection: &ProjectionMask,
        plan_builder: &ReadPlanBuilder,
        batch_size: usize,
        metrics: &ArrowReaderMetrics,
    ) -> Result<PerColumnDecision> {
        Self::try_new_with_planner(
            row_groups,
            array_reader_builder,
            fields,
            projection,
            plan_builder,
            batch_size,
            metrics,
            None,
            &MetadataColumnSelectionPlanner,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_new_with_loaded_ranges(
        row_groups: &dyn RowGroups,
        array_reader_builder: &ArrayReaderBuilder<'_>,
        fields: Option<&ParquetField>,
        projection: &ProjectionMask,
        plan_builder: &ReadPlanBuilder,
        batch_size: usize,
        metrics: &ArrowReaderMetrics,
        loaded_row_ranges: Vec<Option<LoadedRowRanges>>,
    ) -> Result<PerColumnDecision> {
        Self::try_new_with_planner(
            row_groups,
            array_reader_builder,
            fields,
            projection,
            plan_builder,
            batch_size,
            metrics,
            Some(loaded_row_ranges),
            &MetadataColumnSelectionPlanner,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn try_new_with_planner(
        row_groups: &dyn RowGroups,
        array_reader_builder: &ArrayReaderBuilder<'_>,
        fields: Option<&ParquetField>,
        projection: &ProjectionMask,
        plan_builder: &ReadPlanBuilder,
        batch_size: usize,
        metrics: &ArrowReaderMetrics,
        loaded_row_ranges: Option<Vec<Option<LoadedRowRanges>>>,
        planner: &dyn ColumnSelectionPlanner,
    ) -> Result<PerColumnDecision> {
        if !matches!(
            plan_builder.row_selection_policy(),
            RowSelectionPolicy::AutoPerColumn
        ) {
            return Ok(PerColumnDecision::Fallback(
                plan_builder.resolve_selection_strategy(),
            ));
        }
        let Some(selection) = plan_builder.selection() else {
            return Ok(PerColumnDecision::Fallback(RowSelectionStrategy::Selectors));
        };
        if batch_size == 0 || !selection.selects_any() {
            return Ok(PerColumnDecision::Fallback(
                plan_builder.resolve_selection_strategy(),
            ));
        }

        let projected_fields = projected_top_level_fields(fields, projection);
        let column_count = projected_fields.len();
        if column_count == 0 {
            return Ok(PerColumnDecision::Fallback(
                plan_builder.resolve_selection_strategy(),
            ));
        }

        let (row_group_rows, strategies, uniform) =
            plan_strategies(row_groups, selection, &projected_fields, metrics, planner)?;
        if let Some(strategy) = uniform {
            return Ok(PerColumnDecision::Fallback(strategy));
        }

        let readers = array_reader_builder.build_top_level_array_readers(fields, projection)?;
        if readers.len() != column_count {
            return Err(general_err!(
                "Internal Error: planned {column_count} top-level columns but built {}",
                readers.len()
            ));
        }
        let (output_fields, columns): (Vec<_>, Vec<_>) = readers
            .into_iter()
            .map(|(field, reader)| (field, TopLevelColumnReader { reader }))
            .unzip();
        let fields = Fields::from(output_fields);
        let data_type = DataType::Struct(fields.clone());
        let plan = RowSelectionExecutionPlan::try_new(
            selection.clone(),
            &row_group_rows,
            column_count,
            strategies,
            loaded_row_ranges,
            batch_size,
        )?;
        Ok(PerColumnDecision::Engaged(Self {
            columns,
            fields,
            data_type,
            plan: Arc::new(plan),
            next_batch: 0,
            buffered: None,
        }))
    }

    fn next_array(&mut self) -> Result<Option<ArrayRef>> {
        let Self {
            columns,
            fields,
            plan,
            next_batch,
            ..
        } = self;
        let Some(batch) = plan.batch(*next_batch) else {
            return Ok(None);
        };
        *next_batch += 1;

        let arrays = columns
            .iter_mut()
            .enumerate()
            .map(|(column_idx, column)| {
                read_column_batch(column.reader.as_mut(), plan, batch, column_idx)
            })
            .collect::<Result<Vec<_>>>()?;
        let array = StructArray::try_new(fields.clone(), arrays, None)?;
        if array.len() != batch.selected_rows {
            return Err(general_err!(
                "Internal Error: per-column batch produced {} rows, expected {}",
                array.len(),
                batch.selected_rows
            ));
        }
        Ok(Some(Arc::new(array)))
    }

    fn empty_array(&self) -> ArrayRef {
        let arrays = self
            .fields
            .iter()
            .map(|field| new_empty_array(field.data_type()))
            .collect();
        Arc::new(StructArray::new(self.fields.clone(), arrays, None))
    }
}

impl ArrayReader for PerColumnReader {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_data_type(&self) -> &DataType {
        &self.data_type
    }

    fn read_records(&mut self, batch_size: usize) -> Result<usize> {
        if self.buffered.is_some() {
            return Err(general_err!(
                "Internal Error: per-column batch must be consumed before reading again"
            ));
        }
        if batch_size == 0 {
            self.buffered = Some(self.empty_array());
            return Ok(0);
        }
        let Some(array) = self.next_array()? else {
            self.buffered = Some(self.empty_array());
            return Ok(0);
        };
        if array.len() > batch_size {
            return Err(general_err!(
                "Internal Error: per-column reader produced {} rows for batch size {batch_size}",
                array.len()
            ));
        }
        let rows = array.len();
        self.buffered = Some(array);
        Ok(rows)
    }

    fn consume_batch(&mut self) -> Result<ArrayRef> {
        Ok(self.buffered.take().unwrap_or_else(|| self.empty_array()))
    }

    fn skip_records(&mut self, _num_records: usize) -> Result<usize> {
        Err(general_err!(
            "Internal Error: per-column root reader does not support outer row skipping"
        ))
    }

    fn get_def_levels(&self) -> Option<&[i16]> {
        None
    }

    fn get_rep_levels(&self) -> Option<&[i16]> {
        None
    }
}

fn plan_strategies(
    row_groups: &dyn RowGroups,
    selection: &RowSelection,
    fields: &[&ParquetField],
    metrics: &ArrowReaderMetrics,
    planner: &dyn ColumnSelectionPlanner,
) -> Result<(
    Vec<usize>,
    Vec<RowSelectionStrategy>,
    Option<RowSelectionStrategy>,
)> {
    let column_count = fields.len();
    let row_groups = row_groups.row_groups().collect::<Vec<_>>();
    let row_group_rows = row_groups
        .iter()
        .map(|row_group| row_group.num_rows() as usize)
        .collect::<Vec<_>>();
    let selection_statistics = selection_statistics(selection, &row_group_rows);

    let mut strategies = vec![RowSelectionStrategy::Selectors; row_groups.len() * column_count];
    let mut first_active = None;
    let mut uniform = true;

    for (row_group_index, (row_group, &statistics)) in
        row_groups.iter().zip(&selection_statistics).enumerate()
    {
        if statistics.physical_rows == 0 {
            continue;
        }

        let fallback = statistics.auto_selection_strategy(DEFAULT_ROW_SELECTION_THRESHOLD);
        for (column_index, field) in fields.iter().enumerate() {
            let context = ColumnSelectionContext {
                field,
                row_group,
                selection: statistics,
            };
            let planned = planner.strategy(context);
            let strategy = planned.unwrap_or(fallback);
            metrics.record_row_selection_decision(strategy, planned.is_none());
            strategies[row_group_index * column_count + column_index] = strategy;
            match first_active {
                Some(first) if first != strategy => uniform = false,
                None => first_active = Some(strategy),
                _ => {}
            }
        }
    }

    let first_active = first_active.unwrap_or(RowSelectionStrategy::Selectors);
    for (row_group_index, statistics) in selection_statistics.iter().enumerate() {
        if statistics.physical_rows == 0 {
            strategies[row_group_index * column_count..(row_group_index + 1) * column_count]
                .fill(first_active);
        }
    }

    Ok((row_group_rows, strategies, uniform.then_some(first_active)))
}

/// Computes row-group-local run statistics in one pass over either selection
/// backing. Selector run counts saturate once every supported threshold must
/// choose Mask, avoiding unnecessary arithmetic on highly fragmented input.
fn selection_statistics(
    selection: &RowSelection,
    row_group_rows: &[usize],
) -> Vec<RowSelectionStatistics> {
    match selection.as_mask() {
        Some(mask) => {
            let mut offset = 0usize;
            row_group_rows
                .iter()
                .map(|&row_group_rows| {
                    let remaining = mask.len().saturating_sub(offset);
                    let physical_rows = row_group_rows.min(remaining);
                    let run_count = mask_run_count(&mask.slice(offset, physical_rows));
                    offset = offset.saturating_add(physical_rows);
                    RowSelectionStatistics {
                        physical_rows,
                        run_count,
                    }
                })
                .collect()
        }
        None => {
            let mut statistics = vec![RowSelectionStatistics::default(); row_group_rows.len()];
            let mut row_group_index = 0;
            let mut rows_left_in_group = row_group_rows.first().copied().unwrap_or_default();

            for selector in selection.iter() {
                let mut rows_left_in_run = selector.row_count;
                while rows_left_in_run != 0 {
                    while rows_left_in_group == 0 {
                        row_group_index += 1;
                        let Some(&rows) = row_group_rows.get(row_group_index) else {
                            return statistics;
                        };
                        rows_left_in_group = rows;
                    }

                    let rows = rows_left_in_run.min(rows_left_in_group);
                    let current = &mut statistics[row_group_index];
                    current.physical_rows += rows;

                    // Once this boundary is reached, even the smallest model
                    // threshold chooses Mask. Keep a lower bound instead of
                    // counting the remainder of this fragmented row group.
                    let saturation = row_group_rows[row_group_index]
                        .checked_div(WIDE_BYTE_RUN_THRESHOLD)
                        .and_then(|runs| runs.checked_add(1))
                        .unwrap_or(usize::MAX);
                    if current.run_count < saturation {
                        current.run_count += 1;
                    }

                    rows_left_in_run -= rows;
                    rows_left_in_group -= rows;
                }
            }

            statistics
        }
    }
}

fn read_column_batch(
    reader: &mut dyn ArrayReader,
    plan: &RowSelectionExecutionPlan,
    batch: &BatchWindow,
    column_idx: usize,
) -> Result<ArrayRef> {
    let chunks = plan.chunks(batch);
    let needs_filter = chunks
        .iter()
        .any(|chunk| plan.strategy(chunk, column_idx) == RowSelectionStrategy::Mask);
    let mut filter_mask = needs_filter.then(|| BooleanBufferBuilder::new(batch.selected_rows));

    for chunk in chunks {
        let mut consumed_rows = 0usize;
        for instruction in plan.lower_chunk(chunk, column_idx)? {
            match instruction {
                ColumnInstruction::Skip(rows) => {
                    exact_skip(reader, rows)?;
                    consumed_rows += rows;
                }
                ColumnInstruction::Read { rows, mask } => {
                    exact_read(reader, rows)?;
                    consumed_rows += rows;
                    if let Some(filter_mask) = filter_mask.as_mut() {
                        match mask {
                            Some(mask) => filter_mask.append_buffer(&mask),
                            None => filter_mask.append_n(rows, true),
                        }
                    }
                }
            }
        }
        // Lanes that advance by different amounts misalign the output columns
        // without necessarily changing any column's length, so check here
        // rather than relying on the batch-level length checks alone.
        if consumed_rows != chunk.physical_rows.len() {
            return Err(general_err!(
                "Internal Error: column {column_idx} consumed {consumed_rows} rows of a {}-row chunk",
                chunk.physical_rows.len()
            ));
        }
    }

    let array = reader.consume_batch()?;
    let array = match filter_mask {
        Some(mut filter_mask) => {
            let filter_mask = BooleanArray::from(filter_mask.finish());
            if filter_mask.len() != array.len() {
                return Err(general_err!(
                    "Internal Error: per-column filter has {} rows for an array of {} rows",
                    filter_mask.len(),
                    array.len()
                ));
            }
            filter(array.as_ref(), &filter_mask)?
        }
        None => array,
    };
    if array.len() != batch.selected_rows {
        return Err(general_err!(
            "Internal Error: per-column reader produced {} rows, expected {}",
            array.len(),
            batch.selected_rows
        ));
    }
    Ok(array)
}

fn exact_skip(reader: &mut dyn ArrayReader, rows: usize) -> Result<()> {
    if rows == 0 {
        return Ok(());
    }
    let skipped = reader.skip_records(rows)?;
    if skipped != rows {
        return Err(general_err!(
            "failed to skip rows, expected {rows}, got {skipped}"
        ));
    }
    Ok(())
}

fn exact_read(reader: &mut dyn ArrayReader, rows: usize) -> Result<()> {
    if rows == 0 {
        return Ok(());
    }
    let read = reader.read_records(rows)?;
    if read != rows {
        return Err(general_err!(
            "failed to read rows, expected {rows}, got {read}"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::arrow_reader::RowSelector;
    use crate::arrow::schema::VirtualColumnType;
    use crate::basic::Type as PhysicalType;
    use crate::column::page::PageIterator;
    use crate::file::metadata::ParquetMetaData;
    use crate::schema::types::{SchemaDescriptor, Type};
    use arrow_array::{Array, Int32Array, RecordBatch};
    use arrow_buffer::BooleanBuffer;
    use arrow_schema::{DataType, Field};
    use std::any::Any;

    struct TestArrayReader {
        values: Vec<i32>,
        position: usize,
        buffered: Vec<i32>,
        data_type: DataType,
    }

    impl TestArrayReader {
        fn new(values: impl IntoIterator<Item = i32>) -> Self {
            Self {
                values: values.into_iter().collect(),
                position: 0,
                buffered: Vec::new(),
                data_type: DataType::Int32,
            }
        }
    }

    impl ArrayReader for TestArrayReader {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn get_data_type(&self) -> &DataType {
            &self.data_type
        }

        fn read_records(&mut self, batch_size: usize) -> Result<usize> {
            let end = (self.position + batch_size).min(self.values.len());
            self.buffered
                .extend_from_slice(&self.values[self.position..end]);
            let read = end - self.position;
            self.position = end;
            Ok(read)
        }

        fn consume_batch(&mut self) -> Result<ArrayRef> {
            Ok(Arc::new(Int32Array::from(std::mem::take(
                &mut self.buffered,
            ))))
        }

        fn skip_records(&mut self, num_records: usize) -> Result<usize> {
            let end = (self.position + num_records).min(self.values.len());
            let skipped = end - self.position;
            self.position = end;
            Ok(skipped)
        }

        fn get_def_levels(&self) -> Option<&[i16]> {
            None
        }

        fn get_rep_levels(&self) -> Option<&[i16]> {
            None
        }
    }

    fn empty_row_group() -> RowGroupMetaData {
        let schema = Arc::new(SchemaDescriptor::new(Arc::new(
            Type::group_type_builder("schema")
                .with_fields(Vec::new())
                .build()
                .unwrap(),
        )));
        RowGroupMetaData::builder(schema)
            .set_num_rows(0)
            .set_total_byte_size(0)
            .set_column_metadata(Vec::new())
            .build()
            .unwrap()
    }

    fn primitive_field(arrow_type: DataType) -> ParquetField {
        let primitive_type = Arc::new(
            Type::primitive_type_builder("payload", PhysicalType::INT32)
                .build()
                .unwrap(),
        );
        ParquetField {
            rep_level: 0,
            def_level: 0,
            nullable: false,
            arrow_type,
            field_type: ParquetFieldType::Primitive {
                col_idx: 0,
                primitive_type,
            },
        }
    }

    #[test]
    fn metadata_model_thresholds_and_fallback_are_explicit() {
        let row_group = empty_row_group();
        assert_eq!(
            metadata_run_threshold(&primitive_field(DataType::Int32), &row_group),
            Some(INT32_RUN_THRESHOLD)
        );
        assert_eq!(
            metadata_run_threshold(
                &primitive_field(DataType::Dictionary(
                    Box::new(DataType::Int32),
                    Box::new(DataType::Utf8),
                )),
                &row_group,
            ),
            Some(DICTIONARY_UTF8_RUN_THRESHOLD)
        );
        assert_eq!(
            metadata_run_threshold(&primitive_field(DataType::FixedSizeBinary(8)), &row_group,),
            Some(MEDIUM_BYTE_RUN_THRESHOLD)
        );
        assert_eq!(
            metadata_run_threshold(&primitive_field(DataType::FixedSizeBinary(32)), &row_group,),
            Some(WIDE_BYTE_RUN_THRESHOLD)
        );

        let unmodeled = ParquetField {
            rep_level: 0,
            def_level: 0,
            nullable: false,
            arrow_type: DataType::Int64,
            field_type: ParquetFieldType::Virtual(VirtualColumnType::RowNumber),
        };
        assert_eq!(metadata_run_threshold(&unmodeled, &row_group), None);
    }

    #[test]
    fn row_group_statistics_match_split_selection_decisions() {
        let selectors = vec![
            RowSelector::skip(2),
            RowSelector::select(5),
            RowSelector::skip(1),
            RowSelector::select(7),
        ];
        let row_group_rows = [3, 0, 5, 7];
        let selector_selection = RowSelection::from(selectors.clone());
        let mask = selectors
            .iter()
            .flat_map(|selector| std::iter::repeat_n(!selector.skip, selector.row_count))
            .collect::<BooleanBuffer>();
        let mask_selection = RowSelection::from_boolean_buffer(mask);

        for selection in [&selector_selection, &mask_selection] {
            let statistics = selection_statistics(selection, &row_group_rows);
            let mut remaining = selection.clone();
            for (&rows, statistics) in row_group_rows.iter().zip(statistics) {
                let row_group_selection = remaining.split_off(rows);
                for threshold in [
                    WIDE_BYTE_RUN_THRESHOLD,
                    INT32_RUN_THRESHOLD,
                    DICTIONARY_UTF8_RUN_THRESHOLD,
                    DEFAULT_ROW_SELECTION_THRESHOLD,
                ] {
                    assert_eq!(
                        statistics.auto_selection_strategy(threshold),
                        row_group_selection.auto_selection_strategy(threshold)
                    );
                }
            }
        }
    }

    #[test]
    fn mixed_columns_stay_aligned_when_a_batch_crosses_row_groups() {
        let selection = RowSelection::from(vec![
            RowSelector::skip(1),
            RowSelector::select(1),
            RowSelector::skip(1),
            RowSelector::select(2),
            RowSelector::skip(1),
            RowSelector::select(1),
            RowSelector::skip(1),
            RowSelector::select(2),
            RowSelector::skip(1),
            RowSelector::select(1),
        ]);
        let plan = RowSelectionExecutionPlan::try_new(
            selection,
            &[6, 6],
            2,
            vec![
                RowSelectionStrategy::Selectors,
                RowSelectionStrategy::Mask,
                RowSelectionStrategy::Mask,
                RowSelectionStrategy::Selectors,
            ],
            None,
            4,
        )
        .unwrap();
        let fields = vec![
            Arc::new(Field::new("left", DataType::Int32, false)),
            Arc::new(Field::new("right", DataType::Int32, false)),
        ];
        let fields = Fields::from(fields);
        let mut reader = PerColumnReader {
            columns: vec![
                TopLevelColumnReader {
                    reader: Box::new(TestArrayReader::new(0..12)),
                },
                TopLevelColumnReader {
                    reader: Box::new(TestArrayReader::new(100..112)),
                },
            ],
            data_type: DataType::Struct(fields.clone()),
            fields,
            plan: Arc::new(plan),
            next_batch: 0,
            buffered: None,
        };

        let mut next_batch = |batch_size| {
            let rows = reader.read_records(batch_size).unwrap();
            let array = reader.consume_batch().unwrap();
            let array = array.as_any().downcast_ref::<StructArray>().unwrap();
            let batch = RecordBatch::from(array);
            assert_eq!(batch.num_rows(), rows);
            batch
        };
        let first = next_batch(4);
        let second = next_batch(4);
        assert_eq!(next_batch(4).num_rows(), 0);

        let values = |batch: &RecordBatch, column| {
            batch
                .column(column)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        };
        assert_eq!(values(&first, 0), vec![1, 3, 4, 6]);
        assert_eq!(values(&first, 1), vec![101, 103, 104, 106]);
        assert_eq!(values(&second, 0), vec![8, 9, 11]);
        assert_eq!(values(&second, 1), vec![108, 109, 111]);
    }

    struct StrategyRowGroups {
        row_groups: Vec<RowGroupMetaData>,
    }

    impl RowGroups for StrategyRowGroups {
        fn num_rows(&self) -> usize {
            self.row_groups
                .iter()
                .map(|row_group| row_group.num_rows() as usize)
                .sum()
        }

        fn column_chunks(&self, _i: usize) -> Result<Box<dyn PageIterator>> {
            unreachable!("strategy planning does not open column chunks")
        }

        fn row_groups(&self) -> Box<dyn Iterator<Item = &RowGroupMetaData> + '_> {
            Box::new(self.row_groups.iter())
        }

        fn metadata(&self) -> &ParquetMetaData {
            unreachable!("strategy planning does not access file metadata")
        }
    }

    struct ForcedMixedPlanner;

    impl ColumnSelectionPlanner for ForcedMixedPlanner {
        fn strategy(&self, context: ColumnSelectionContext<'_>) -> Option<RowSelectionStrategy> {
            assert_eq!(context.field.arrow_type, DataType::Int32);
            Some(match &context.field.field_type {
                ParquetFieldType::Virtual(VirtualColumnType::RowNumber) => {
                    RowSelectionStrategy::Selectors
                }
                ParquetFieldType::Virtual(VirtualColumnType::RowGroupIndex) => {
                    RowSelectionStrategy::Mask
                }
                _ => unreachable!(),
            })
        }
    }

    #[test]
    fn planner_receives_top_level_field_and_records_forced_decisions() {
        let schema = Arc::new(SchemaDescriptor::new(Arc::new(
            Type::group_type_builder("schema")
                .with_fields(Vec::new())
                .build()
                .unwrap(),
        )));
        let row_group = || {
            RowGroupMetaData::builder(Arc::clone(&schema))
                .set_num_rows(6)
                .set_total_byte_size(0)
                .set_column_metadata(Vec::new())
                .build()
                .unwrap()
        };
        let row_groups = StrategyRowGroups {
            row_groups: vec![row_group(), row_group()],
        };
        let fields = [
            ParquetField {
                rep_level: 0,
                def_level: 0,
                nullable: false,
                arrow_type: DataType::Int32,
                field_type: ParquetFieldType::Virtual(VirtualColumnType::RowNumber),
            },
            ParquetField {
                rep_level: 0,
                def_level: 0,
                nullable: false,
                arrow_type: DataType::Int32,
                field_type: ParquetFieldType::Virtual(VirtualColumnType::RowGroupIndex),
            },
        ];
        let field_refs = fields.iter().collect::<Vec<_>>();
        let selection = RowSelection::from(vec![
            RowSelector::select(2),
            RowSelector::skip(2),
            RowSelector::select(4),
            RowSelector::skip(1),
            RowSelector::select(3),
        ]);
        let metrics = ArrowReaderMetrics::enabled();

        let (rows, strategies, uniform) = plan_strategies(
            &row_groups,
            &selection,
            &field_refs,
            &metrics,
            &ForcedMixedPlanner,
        )
        .unwrap();
        assert_eq!(rows, vec![6, 6]);
        assert_eq!(
            strategies,
            vec![
                RowSelectionStrategy::Selectors,
                RowSelectionStrategy::Mask,
                RowSelectionStrategy::Selectors,
                RowSelectionStrategy::Mask,
            ]
        );
        assert_eq!(uniform, None);
        assert_eq!(metrics.row_selection_selector_decisions(), Some(2));
        assert_eq!(metrics.row_selection_mask_decisions(), Some(2));
        assert_eq!(metrics.row_selection_fallback_decisions(), Some(0));
    }
}
