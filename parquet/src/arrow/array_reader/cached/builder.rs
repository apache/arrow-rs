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

use crate::arrow::array_reader::incremental_batch_builder::IncrementalRecordBatchBuilder;
use crate::arrow::array_reader::CachedPredicateResult;
use crate::arrow::arrow_reader::RowSelection;
use crate::arrow::ProjectionMask;
use arrow_array::{Array, BooleanArray, RecordBatch};
use arrow_schema::{ArrowError, Schema, SchemaRef};
use arrow_select::filter::prep_null_mask_filter;
use std::sync::Arc;

/// Incrementally builds the result of evaluating an ArrowPredicate on
/// a RowGroup.
#[derive(Debug)]
pub(crate) struct CachedPredicateResultBuilder {
    /// What is being cached
    strategy: CacheStrategy,
    /// Total number of columns in the original parquet schema
    num_original_columns: usize,
    /// Any filters that have been applied. Note this the complete set of filters
    /// that have been applied to the cached batches.
    filters: Vec<BooleanArray>,
}

#[derive(Debug)]
enum CacheStrategy {
    /// Don't cache any results
    None,
    /// Cache the result of filtering all columns in the filter schema
    All {
        /// The builder for the cached batches
        cached_batches_builder: IncrementalRecordBatchBuilder,
        /// The indexes of the columns in the original parquet schema that are in the projection
        original_projection: Vec<usize>,
    },
    /// Cache the result of filtering a subset of the columns in the filter schema
    Subset {
        /// The builder for the cached batches
        cached_batches_builder: IncrementalRecordBatchBuilder,
        /// The indexes of the columns in the filter schema that are in the projection
        filter_projection: Vec<usize>,
        /// The indexes of the columns in the original parquet schema that are in the projection
        original_projection: Vec<usize>,
    },
}

impl CachedPredicateResultBuilder {
    /// Create a new CachedPredicateResultBuilder
    ///
    /// # Arguments:
    /// * `num_original_columns`: The number of columns in the original parquet schema
    /// * `schema`: The schema of the filtered record batch (not the original parquet schema)
    /// * `filter_mask`: which columns of the original parquet schema did the filter columns come from?
    /// * `projection_mask`: which columns of the original parquet schema are in the final projection?
    ///
    /// This structure does not cache filter results for the columns that are not
    /// in the projection mask. This is because the filter results are not needed
    pub(crate) fn try_new(
        num_original_columns: usize,
        filter_schema: &SchemaRef,
        filter_mask: &ProjectionMask,
        projection_mask: &ProjectionMask,
        batch_size: usize,
    ) -> Result<Self, ArrowError> {
        let (filter_mask_inner, projection_mask_inner) =
            match (filter_mask.mask(), projection_mask.mask()) {
                (Some(filter_mask), Some(projection_mask)) => (filter_mask, projection_mask),
                // None means "select all columns" so in this case cache all filtered columns
                (Some(filter_mask), None) => (filter_mask, filter_mask),
                // None means "select all columns" so in this case cache all columns used in projection
                (None, Some(projection_mask)) => (projection_mask, projection_mask),
                (None, None) => {
                    // this means all columns are in the projection *and* filter so cache them all when possible
                    let cached_batches_builder = IncrementalRecordBatchBuilder::try_new(
                        Arc::clone(filter_schema),
                        batch_size,
                    )?;
                    let strategy = CacheStrategy::All {
                        cached_batches_builder,
                        original_projection: (0..num_original_columns).collect(),
                    };
                    return {
                        Ok(Self {
                            strategy,
                            num_original_columns,
                            filters: vec![],
                        })
                    };
                }
            };

        // Otherwise, need to select a subset of the fields from each batch to cache

        // This is an iterator over the fields of the schema of batches passed
        // to the filter.
        let mut filter_field_iter = filter_schema.fields.iter().enumerate();

        let mut filter_projection = vec![];
        let mut original_projection = vec![];
        let mut fields = vec![];

        // Iterate over the masks from the original schema
        assert_eq!(filter_mask_inner.len(), projection_mask_inner.len());
        for (original_index, (&in_filter, &in_projection)) in filter_mask_inner
            .iter()
            .zip(projection_mask_inner.iter())
            .enumerate()
        {
            if !in_filter {
                continue;
            }
            // take next field from the filter schema
            let (filter_index, field) =
                filter_field_iter.next().expect("mismatch in field lengths");
            if !in_projection {
                // this field is not in the projection, so don't cache it
                continue;
            }
            // this field is both in filter and the projection, so cache the results
            filter_projection.push(filter_index);
            original_projection.push(original_index);
            fields.push(Arc::clone(field));
        }
        let strategy = if fields.is_empty() {
            CacheStrategy::None
        } else {
            let cached_batches_builder =
                IncrementalRecordBatchBuilder::try_new(Arc::new(Schema::new(fields)), batch_size)?;
            CacheStrategy::Subset {
                cached_batches_builder,
                filter_projection,
                original_projection,
            }
        };

        Ok(Self {
            strategy,
            num_original_columns,
            filters: vec![],
        })
    }

    /// Add a new batch and filter to the builder
    pub(crate) fn add(
        &mut self,
        batch: RecordBatch,
        mut filter: BooleanArray,
    ) -> crate::errors::Result<()> {
        if filter.null_count() > 0 {
            filter = prep_null_mask_filter(&filter);
        }

        match &mut self.strategy {
            CacheStrategy::None => {}
            CacheStrategy::All {
                cached_batches_builder,
                ..
            } => {
                cached_batches_builder.append_filtered(batch, &filter)?;
            }
            CacheStrategy::Subset {
                cached_batches_builder,
                ref filter_projection,
                ..
            } => {
                // If we have a filter projection, we need to project the batch
                // to only the columns that are in the filter projection
                let projected_batch = batch.project(filter_projection)?;
                cached_batches_builder.append_filtered(projected_batch, &filter)?;
            }
        }

        self.filters.push(filter);

        Ok(())
    }

    /// Return (selection, maybe_cached_predicate_result) that represents the rows
    /// that were selected and batches that were evaluated.
    pub(crate) fn build(
        self,
    ) -> crate::errors::Result<(RowSelection, Option<CachedPredicateResult>)> {
        let Self {
            strategy,
            num_original_columns,
            filters,
        } = self;

        let new_selection = RowSelection::from_filters(&filters);

        match strategy {
            CacheStrategy::None => Ok((new_selection, None)),
            CacheStrategy::All {
                cached_batches_builder,
                original_projection,
            }
            | CacheStrategy::Subset {
                cached_batches_builder,
                original_projection,
                ..
            } => {
                // explode out the cached batches into the proper place in the original schema
                let completed_batches = cached_batches_builder.build()?;
                let mut cached_result = CachedPredicateResult::new(num_original_columns, filters);
                for (batch_index, original_idx) in original_projection.iter().enumerate() {
                    let mut column_arrays = Vec::with_capacity(completed_batches.len());
                    for batch in &completed_batches {
                        column_arrays.push(Arc::clone(batch.column(batch_index)));
                    }
                    cached_result.add_result(*original_idx, column_arrays);
                }

                Ok((new_selection, Some(cached_result)))
            }
        }
    }
}
