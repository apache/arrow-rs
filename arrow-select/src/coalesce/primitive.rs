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

use crate::coalesce::InProgressArray;
use crate::filter::{FilterPredicate, IndexIterator, IterationStrategy, SlicesIterator};
use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, ArrowPrimitiveType, PrimitiveArray};
use arrow_buffer::{NullBufferBuilder, ScalarBuffer, bit_util};
use arrow_schema::{ArrowError, DataType};
use std::fmt::Debug;
use std::sync::Arc;

/// InProgressArray for [`PrimitiveArray`]
#[derive(Debug)]
pub(crate) struct InProgressPrimitiveArray<T: ArrowPrimitiveType> {
    /// Data type of the array
    data_type: DataType,
    /// The current source, if any
    source: Option<ArrayRef>,
    /// the target batch size (and thus size for views allocation)
    batch_size: usize,
    /// In progress nulls
    nulls: NullBufferBuilder,
    /// The currently in progress array
    current: Vec<T::Native>,
}

impl<T: ArrowPrimitiveType> InProgressPrimitiveArray<T> {
    /// Create a new `InProgressPrimitiveArray`
    pub(crate) fn new(batch_size: usize, data_type: DataType) -> Self {
        Self {
            data_type,
            batch_size,
            source: None,
            nulls: NullBufferBuilder::new(batch_size),
            current: vec![],
        }
    }

    /// Allocate space for output values if necessary.
    ///
    /// This is done on write (when we know it is necessary) rather than
    /// eagerly to avoid allocations that are not used.
    fn ensure_capacity(&mut self) {
        if self.current.capacity() == 0 {
            self.current.reserve(self.batch_size);
        }
        debug_assert_eq!(self.current.capacity(), self.batch_size);
    }
}

impl<T: ArrowPrimitiveType + Debug> InProgressArray for InProgressPrimitiveArray<T> {
    fn set_source(&mut self, source: Option<ArrayRef>, _selectivity: Option<f64>) {
        self.source = source;
    }

    fn copy_rows(&mut self, offset: usize, len: usize) -> Result<(), ArrowError> {
        self.ensure_capacity();

        let s = self
            .source
            .as_ref()
            .ok_or_else(|| {
                ArrowError::InvalidArgumentError(
                    "Internal Error: InProgressPrimitiveArray: source not set".to_string(),
                )
            })?
            .as_primitive::<T>();

        // add nulls if necessary
        if let Some(nulls) = s.nulls().as_ref() {
            let nulls = nulls.slice(offset, len);
            self.nulls.append_buffer(&nulls);
        } else {
            self.nulls.append_n_non_nulls(len);
        };

        // Copy the values
        self.current
            .extend_from_slice(&s.values()[offset..offset + len]);

        Ok(())
    }

    /// Copy rows using a predicate
    fn copy_rows_by_filter(
        &mut self,
        filter: &FilterPredicate,
        offset: usize,
        len: usize,
    ) -> Result<(), ArrowError> {
        self.ensure_capacity();

        let s = self
            .source
            .as_ref()
            .ok_or_else(|| {
                ArrowError::InvalidArgumentError(
                    "Internal Error: InProgressPrimitiveArray: source not set".to_string(),
                )
            })?
            .slice(offset, len);
        let s = s.as_primitive::<T>();

        let values = s.values();
        let count = filter.count();

        // Use the predicate's strategy for optimal iteration
        match filter.strategy() {
            IterationStrategy::SlicesIterator => {
                // Copy values, nulls using slices
                if let Some(nulls) = s.nulls().filter(|n| n.null_count() > 0) {
                    for (start, end) in SlicesIterator::new(filter.filter_array()) {
                        // SAFETY: slices are derived from filter predicate
                        self.current
                            .extend_from_slice(unsafe { values.get_unchecked(start..end) });
                        let slice = nulls.slice(start, end - start);
                        self.nulls.append_buffer(&slice);
                    }
                } else {
                    for (start, end) in SlicesIterator::new(filter.filter_array()) {
                        // SAFETY: SlicesIterator produces valid ranges derived from filter
                        self.current
                            .extend_from_slice(unsafe { values.get_unchecked(start..end) });
                    }
                    self.nulls.append_n_non_nulls(count);
                }
            }
            IterationStrategy::Slices(slices) => {
                // Copy values and nulls using precomputed slices - single iteration
                if let Some(nulls) = s.nulls().filter(|n| n.null_count() > 0) {
                    for &(start, end) in slices {
                        // SAFETY: slices are derived from filter predicate
                        self.current
                            .extend_from_slice(unsafe { values.get_unchecked(start..end) });
                        let slice = nulls.slice(start, end - start);
                        self.nulls.append_buffer(&slice);
                    }
                } else {
                    for &(start, end) in slices {
                        // SAFETY: slices are derived from filter predicate
                        self.current
                            .extend_from_slice(unsafe { values.get_unchecked(start..end) });
                    }
                    self.nulls.append_n_non_nulls(count);
                }
            }
            IterationStrategy::IndexIterator => {
                // Copy values and nulls for each index
                if let Some(nulls) = s.nulls().filter(|n| n.null_count() > 0) {
                    let null_buffer = nulls.inner();
                    let null_ptr = null_buffer.values().as_ptr();
                    let null_offset = null_buffer.offset();

                    // Collect indices for reuse (values + nulls)
                    let indices = IndexIterator::new(filter.filter_array(), count);

                    // Efficiently extend null buffer
                    // SAFETY: indices iterator reports correct length
                    unsafe {
                        self.nulls.extend_trusted_len(
                            indices.map(|idx| bit_util::get_bit_raw(null_ptr, idx + null_offset)),
                        );
                    }
                    let indices = IndexIterator::new(filter.filter_array(), count);

                    // Copy values
                    // SAFETY: indices are derived from filter predicate
                    self.current
                        .extend(indices.map(|idx| unsafe { *values.get_unchecked(idx) }));
                } else {
                    self.nulls.append_n_non_nulls(count);
                    let indices = IndexIterator::new(filter.filter_array(), count);
                    // SAFETY: indices are derived from filter predicate
                    self.current
                        .extend(indices.map(|idx: usize| unsafe { *values.get_unchecked(idx) }));
                }
            }
            IterationStrategy::Indices(indices) => {
                // Copy values and nulls using precomputed indices
                if let Some(nulls) = s.nulls().filter(|n| n.null_count() > 0) {
                    let null_buffer = nulls.inner();
                    let null_ptr = null_buffer.values().as_ptr();
                    let null_offset = null_buffer.offset();

                    // Efficiently extend null buffer
                    // SAFETY: indices iterator reports correct length
                    unsafe {
                        self.nulls.extend_trusted_len(
                            indices
                                .iter()
                                .map(|&idx| bit_util::get_bit_raw(null_ptr, idx + null_offset)),
                        );
                    }

                    // Copy values
                    // SAFETY: indices are derived from filter predicate
                    self.current.extend(
                        indices
                            .iter()
                            .map(|&idx| unsafe { *values.get_unchecked(idx) }),
                    );
                } else {
                    self.nulls.append_n_non_nulls(count);
                    // SAFETY: indices are derived from filter predicate
                    self.current.extend(
                        indices
                            .iter()
                            .map(|&idx| unsafe { *values.get_unchecked(idx) }),
                    )
                };
            }
            IterationStrategy::All => {
                // Copy all values
                self.current.extend_from_slice(&values);
                if let Some(nulls) = s.nulls() {
                    self.nulls.append_buffer(nulls);
                } else {
                    self.nulls.append_n_non_nulls(values.len());
                }
            }
            IterationStrategy::None => {
                // Nothing to copy
            }
        }

        Ok(())
    }

    fn finish(&mut self) -> Result<ArrayRef, ArrowError> {
        // take and reset the current values and nulls
        let values = std::mem::take(&mut self.current);
        let nulls = self.nulls.finish();
        self.nulls = NullBufferBuilder::new(self.batch_size);

        let array = PrimitiveArray::<T>::try_new(ScalarBuffer::from(values), nulls)?
            // preserve timezone / precision+scale if applicable
            .with_data_type(self.data_type.clone());
        Ok(Arc::new(array))
    }
}
