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

//! Post-decode filtering support for parquet row-filter fallback.

use crate::arrow::ProjectionMask;
use crate::arrow::arrow_reader::{RowFilter, RowSelection};
use crate::errors::{ParquetError, Result};
use crate::schema::types::SchemaDescriptor;
use arrow_array::{BooleanArray, RecordBatch};
use arrow_buffer::BooleanBuffer;
use arrow_schema::{ArrowError, Schema, SchemaRef};
use arrow_select::filter::filter_record_batch;
use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub(super) struct PostFilterState {
    filter: Arc<Mutex<RowFilter>>,
    predicate_projection_indices: Vec<Vec<usize>>,
    predicate_projection_schemas: Vec<SchemaRef>,
    output_projection_indices: Vec<usize>,
    pub(super) output_schema: SchemaRef,
}

impl PostFilterState {
    pub(super) fn try_new(
        filter: Arc<Mutex<RowFilter>>,
        parquet_schema: &SchemaDescriptor,
        read_schema: &Schema,
        read_projection: &ProjectionMask,
        output_projection: &ProjectionMask,
    ) -> Result<Self> {
        let filter_guard = filter.lock().map_err(|_| {
            ParquetError::General("post-filter predicate state was poisoned".to_string())
        })?;

        let predicate_projection_indices = filter_guard
            .predicates
            .iter()
            .map(|predicate| {
                projection_indices(parquet_schema, read_projection, predicate.projection())
            })
            .collect::<Result<Vec<_>>>()?;
        drop(filter_guard);

        let predicate_projection_schemas = predicate_projection_indices
            .iter()
            .map(|indices| read_schema.project(indices).map(SchemaRef::new))
            .collect::<Result<Vec<_>, _>>()?;

        let output_projection_indices =
            projection_indices(parquet_schema, read_projection, output_projection)?;
        let output_schema = SchemaRef::new(read_schema.project(&output_projection_indices)?);

        Ok(Self {
            filter,
            predicate_projection_indices,
            predicate_projection_schemas,
            output_projection_indices,
            output_schema,
        })
    }

    pub(super) fn apply(&mut self, mut batch: RecordBatch) -> Result<RecordBatch> {
        let mut filter = self.filter.lock().map_err(|_| {
            ParquetError::General("post-filter predicate state was poisoned".to_string())
        })?;

        for (predicate_idx, (predicate, projection_indices)) in filter
            .predicates
            .iter_mut()
            .zip(self.predicate_projection_indices.iter())
            .enumerate()
        {
            let input_rows = batch.num_rows();
            let predicate_batch = project_record_batch(
                &batch,
                projection_indices,
                Arc::clone(&self.predicate_projection_schemas[predicate_idx]),
            )?;
            let predicate_filter = predicate.evaluate(predicate_batch)?;

            if predicate_filter.len() != input_rows {
                return Err(general_err!(
                    "ArrowPredicate predicate returned {} rows, expected {input_rows}",
                    predicate_filter.len()
                ));
            }

            batch = filter_record_batch(&batch, &predicate_filter)?;
            if batch.num_rows() == 0 {
                break;
            }
        }

        Ok(project_record_batch(
            &batch,
            &self.output_projection_indices,
            Arc::clone(&self.output_schema),
        )?)
    }
}

#[derive(Debug)]
pub(super) struct PostSelectionFilterState {
    mask: BooleanBuffer,
    position: usize,
}

impl PostSelectionFilterState {
    pub(super) fn new(selection: RowSelection) -> Self {
        Self {
            mask: selection.boolean_mask(),
            position: 0,
        }
    }

    pub(super) fn apply(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
        let input_rows = batch.num_rows();
        let end = self.position.saturating_add(input_rows);
        if end > self.mask.len() {
            return Err(general_err!(
                "post-selection filter exceeded selection length: end {end}, selection length {}",
                self.mask.len()
            ));
        }

        let filter = BooleanArray::from(self.mask.slice(self.position, input_rows));
        self.position = end;
        Ok(filter_record_batch(&batch, &filter)?)
    }
}

#[inline(always)]
fn project_record_batch(
    batch: &RecordBatch,
    indices: &[usize],
    schema: SchemaRef,
) -> std::result::Result<RecordBatch, ArrowError> {
    if indices.len() == batch.num_columns() && indices.iter().copied().eq(0..batch.num_columns()) {
        debug_assert_eq!(batch.schema_ref().as_ref(), schema.as_ref());
        return Ok(batch.clone());
    }

    let columns = indices
        .iter()
        .map(|idx| {
            batch.columns().get(*idx).cloned().ok_or_else(|| {
                ArrowError::SchemaError(format!(
                    "project index {} out of bounds, max field {}",
                    idx,
                    batch.num_columns()
                ))
            })
        })
        .collect::<std::result::Result<Vec<_>, ArrowError>>()?;

    unsafe {
        // The indices and schema are produced from the same valid read schema
        // at construction time, and filtering preserves column lengths.
        Ok(RecordBatch::new_unchecked(
            schema,
            columns,
            batch.num_rows(),
        ))
    }
}

fn projection_indices(
    parquet_schema: &SchemaDescriptor,
    read_projection: &ProjectionMask,
    target_projection: &ProjectionMask,
) -> Result<Vec<usize>> {
    let mut indices = Vec::new();
    let mut read_idx = 0;

    for leaf_idx in 0..parquet_schema.num_columns() {
        if read_projection.leaf_included(leaf_idx) {
            let root = parquet_schema.get_column_root(leaf_idx);
            if !root.is_primitive() {
                return Err(general_err!(
                    "post-filter fallback does not support nested read column {}",
                    root.name()
                ));
            }
            if target_projection.leaf_included(leaf_idx) {
                indices.push(read_idx);
            }
            read_idx += 1;
        } else if target_projection.leaf_included(leaf_idx) {
            return Err(general_err!(
                "post-filter target projection includes leaf column {leaf_idx} not present in read projection"
            ));
        }
    }

    Ok(indices)
}
