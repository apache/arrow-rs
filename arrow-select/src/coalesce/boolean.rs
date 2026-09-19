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

use super::InProgressArray;
use crate::filter::{FilterPredicate, FilterSelection};
use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, BooleanArray};
use arrow_buffer::{BooleanBufferBuilder, Buffer, NullBuffer};
use arrow_schema::ArrowError;
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct InProgressBooleanArray {
    source: Option<ArrayRef>,
    batch_size: usize,
    values: BooleanBufferBuilder,
    null_bits: Option<BooleanBufferBuilder>,
    len: usize,
}

impl InProgressBooleanArray {
    pub(crate) fn new(batch_size: usize) -> Self {
        Self {
            source: None,
            batch_size,
            values: BooleanBufferBuilder::new(0),
            null_bits: None,
            len: 0,
        }
    }

    fn ensure_capacity(&mut self) {
        if self.values.capacity() == 0 {
            self.values = BooleanBufferBuilder::new(self.batch_size);
        }
    }
}

impl InProgressArray for InProgressBooleanArray {
    fn set_source(&mut self, source: Option<ArrayRef>) {
        self.source = source;
    }

    fn copy_rows(&mut self, offset: usize, len: usize) -> Result<(), ArrowError> {
        self.ensure_capacity();

        let source = self.source.as_ref().ok_or_else(|| {
            ArrowError::InvalidArgumentError(
                "Internal Error: InProgressBooleanArray: source not set".to_string(),
            )
        })?;
        let bool_arr = source.as_boolean();

        if let Some(nulls) = bool_arr.nulls() {
            let null_buf = nulls.inner();
            let bit_offset = null_buf.offset() + offset;
            let null_builder = self.null_bits.get_or_insert_with(|| {
                let mut builder = BooleanBufferBuilder::new(self.batch_size);
                builder.append_n(self.len, true);
                builder
            });
            null_builder
                .append_packed_range(bit_offset..bit_offset + len, null_buf.inner().as_slice());
        } else if let Some(null_builder) = self.null_bits.as_mut() {
            null_builder.append_n(len, true);
        }

        let values = bool_arr.values();
        let bit_offset = values.offset() + offset;
        self.values
            .append_packed_range(bit_offset..bit_offset + len, values.inner().as_slice());
        self.len += len;
        Ok(())
    }

    fn copy_rows_by_filter(&mut self, filter: &FilterPredicate) -> Result<(), ArrowError> {
        if self.source.is_none() {
            return Err(ArrowError::InvalidArgumentError(
                "Internal Error: InProgressBooleanArray: source not set".to_string(),
            ));
        }
        match filter.selection() {
            FilterSelection::None => {}

            FilterSelection::All { len } => {
                self.ensure_capacity();
                let source = self.source.as_ref().unwrap();
                let bool_arr = source.as_boolean();
                if let Some(nulls) = bool_arr.nulls() {
                    let null_buf = nulls.inner();
                    let bit_offset = null_buf.offset();
                    let null_builder = self.null_bits.get_or_insert_with(|| {
                        let mut builder = BooleanBufferBuilder::new(self.batch_size);
                        builder.append_n(self.len, true);
                        builder
                    });
                    null_builder.append_packed_range(
                        bit_offset..bit_offset + len,
                        null_buf.inner().as_slice(),
                    );
                } else if let Some(null_builder) = self.null_bits.as_mut() {
                    null_builder.append_n(len, true);
                }
                let values = bool_arr.values();
                let base = values.offset();
                self.values
                    .append_packed_range(base..base + len, values.inner().as_slice());
                self.len += len;
            }

            FilterSelection::Slices(slices) => {
                self.ensure_capacity();
                let count = filter.count();
                let (null_data, val_base, val_raw): (Option<NullBuffer>, usize, Buffer) = {
                    let source = self.source.as_ref().unwrap();
                    let bool_arr = source.as_boolean();
                    let null_data = filter.filter_nulls(bool_arr.nulls());
                    let values = bool_arr.values();
                    (null_data, values.offset(), values.inner().clone())
                };

                if let Some(filtered_nulls) = null_data {
                    let null_buf = filtered_nulls.inner();
                    let null_builder = self.null_bits.get_or_insert_with(|| {
                        let mut builder = BooleanBufferBuilder::new(self.batch_size);
                        builder.append_n(self.len, true);
                        builder
                    });
                    null_builder.append_buffer(null_buf);
                } else if let Some(null_builder) = self.null_bits.as_mut() {
                    null_builder.append_n(count, true);
                }

                slices.for_each(|(start, end)| {
                    self.values
                        .append_packed_range(val_base + start..val_base + end, val_raw.as_slice());
                });
                self.len += count;
            }

            FilterSelection::Indices(indices) => {
                self.ensure_capacity();
                let count = filter.count();
                let source = self.source.as_ref().unwrap();
                let bool_arr = source.as_boolean();

                // Handle nulls via filter_nulls (does not consume `indices`)
                let null_data = filter.filter_nulls(bool_arr.nulls());
                if let Some(filtered_nulls) = null_data {
                    let null_buf = filtered_nulls.inner();
                    let null_builder = self.null_bits.get_or_insert_with(|| {
                        let mut builder = BooleanBufferBuilder::new(self.batch_size);
                        builder.append_n(self.len, true);
                        builder
                    });
                    null_builder.append_buffer(null_buf);
                } else if let Some(null_builder) = self.null_bits.as_mut() {
                    null_builder.append_n(count, true);
                }

                // Gather value bits directly from packed storage, avoiding an
                // intermediate BooleanArray allocation.
                let values = bool_arr.values();
                let bit_offset = values.offset();
                let raw = values.inner().as_slice();
                indices.for_each(|idx| {
                    let bit_idx = bit_offset + idx;
                    self.values
                        .append((raw[bit_idx >> 3] >> (bit_idx & 7)) & 1 != 0);
                });
                self.len += count;
            }
        }

        Ok(())
    }

    fn finish(&mut self) -> Result<ArrayRef, ArrowError> {
        let values = self.values.finish();
        let nulls = self
            .null_bits
            .take()
            .map(|mut null_builder| NullBuffer::new(null_builder.finish()));
        self.values = BooleanBufferBuilder::new(0);
        // Do NOT clear self.source here — push_batch sets it once and may call
        // finish_buffered_batch mid-loop, expecting source to remain valid.
        self.len = 0;
        Ok(Arc::new(BooleanArray::new(values, nulls)))
    }

    fn size(&self) -> usize {
        self.source
            .as_ref()
            .map_or(0, |source| source.get_array_memory_size())
            + self.values.capacity().div_ceil(8)
            + self
                .null_bits
                .as_ref()
                .map_or(0, |nb| nb.capacity().div_ceil(8))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filter::FilterBuilder;
    use arrow_array::BooleanArray;

    fn make_bool(vals: &[Option<bool>]) -> ArrayRef {
        Arc::new(BooleanArray::from(vals.to_vec()))
    }

    #[test]
    fn test_copy_rows_no_nulls() {
        let source = make_bool(&[Some(true), Some(false), Some(true), Some(false)]);
        let mut ip = InProgressBooleanArray::new(8);
        ip.set_source(Some(Arc::clone(&source)));
        ip.copy_rows(0, 4).unwrap();
        let out = ip.finish().unwrap();
        let out = out.as_boolean();
        assert_eq!(out.len(), 4);
        assert_eq!(out.value(0), true);
        assert_eq!(out.value(1), false);
        assert_eq!(out.value(2), true);
        assert_eq!(out.value(3), false);
        assert!(out.nulls().is_none());
    }

    #[test]
    fn test_copy_rows_with_nulls() {
        let source = make_bool(&[Some(true), None, Some(false)]);
        let mut ip = InProgressBooleanArray::new(8);
        ip.set_source(Some(Arc::clone(&source)));
        ip.copy_rows(0, 3).unwrap();
        let out = ip.finish().unwrap();
        let out = out.as_boolean();
        assert_eq!(out.len(), 3);
        assert_eq!(out.value(0), true);
        assert!(out.is_null(1));
        assert_eq!(out.value(2), false);
    }

    #[test]
    fn test_copy_rows_offset() {
        let source = make_bool(&[Some(true), Some(false), Some(true), Some(false), Some(true)]);
        let mut ip = InProgressBooleanArray::new(8);
        ip.set_source(Some(Arc::clone(&source)));
        ip.copy_rows(2, 3).unwrap();
        let out = ip.finish().unwrap();
        let out = out.as_boolean();
        assert_eq!(out.len(), 3);
        assert_eq!(out.value(0), true);
        assert_eq!(out.value(1), false);
        assert_eq!(out.value(2), true);
    }

    #[test]
    fn test_copy_rows_multiple_calls() {
        let source = make_bool(&[Some(true), Some(false), Some(true), Some(false)]);
        let mut ip = InProgressBooleanArray::new(8);
        ip.set_source(Some(Arc::clone(&source)));
        ip.copy_rows(0, 2).unwrap();
        ip.copy_rows(2, 2).unwrap();
        let out = ip.finish().unwrap();
        let out = out.as_boolean();
        assert_eq!(out.len(), 4);
        assert_eq!(out.value(0), true);
        assert_eq!(out.value(1), false);
        assert_eq!(out.value(2), true);
        assert_eq!(out.value(3), false);
    }

    #[test]
    fn test_filter_slices_path() {
        let source = make_bool(&[
            Some(true),
            Some(false),
            Some(true),
            Some(false),
            Some(true),
            Some(false),
            Some(true),
            Some(false),
            Some(true),
            Some(false),
        ]);
        let filter = BooleanArray::from(vec![
            true, true, true, true, true, true, true, true, true, false,
        ]);
        let predicate = FilterBuilder::new(&filter).build();
        assert!(matches!(predicate.selection(), FilterSelection::Slices(_)));
        let mut ip = InProgressBooleanArray::new(16);
        ip.copy_rows_by_filter_from(Arc::clone(&source), &predicate)
            .unwrap();
        let out = ip.finish().unwrap();
        let out = out.as_boolean();
        assert_eq!(out.len(), 9);
        assert_eq!(out.value(0), true);
        assert_eq!(out.value(1), false);
        assert_eq!(out.value(4), true);
        assert_eq!(out.value(8), true);
    }

    #[test]
    fn test_filter_indices_path() {
        let source = make_bool(&[Some(true), Some(false), Some(true), Some(false), Some(true)]);
        let filter = BooleanArray::from(vec![true, false, true, false, true]);
        let predicate = FilterBuilder::new(&filter).build();
        assert!(matches!(predicate.selection(), FilterSelection::Indices(_)));
        let mut ip = InProgressBooleanArray::new(8);
        ip.copy_rows_by_filter_from(source, &predicate).unwrap();
        let out = ip.finish().unwrap();
        let out = out.as_boolean();
        assert_eq!(out.len(), 3);
        assert_eq!(out.value(0), true);
        assert_eq!(out.value(1), true);
        assert_eq!(out.value(2), true);
    }

    #[test]
    fn test_filter_with_nulls_slices() {
        let vals = vec![
            Some(true),
            None,
            Some(false),
            Some(true),
            Some(false),
            None,
            Some(true),
            Some(false),
            None,
            Some(true),
        ];
        let filter = BooleanArray::from(vec![
            true, true, true, true, true, true, true, true, true, false,
        ]);
        let predicate = FilterBuilder::new(&filter).build();
        assert!(matches!(predicate.selection(), FilterSelection::Slices(_)));
        let mut ip = InProgressBooleanArray::new(16);
        ip.copy_rows_by_filter_from(Arc::new(BooleanArray::from(vals)), &predicate)
            .unwrap();
        let out = ip.finish().unwrap();
        let out = out.as_boolean();
        assert_eq!(out.len(), 9);
        assert_eq!(out.value(0), true);
        assert!(out.is_null(1));
        assert_eq!(out.value(2), false);
        assert_eq!(out.value(3), true);
        assert!(out.is_null(5));
        assert!(out.is_null(8));
    }

    #[test]
    fn test_filter_with_nulls_indices() {
        let source = make_bool(&[Some(true), None, Some(false), None, Some(true)]);
        let filter = BooleanArray::from(vec![true, true, false, true, false]);
        let predicate = FilterBuilder::new(&filter).build();
        assert!(matches!(predicate.selection(), FilterSelection::Indices(_)));
        let mut ip = InProgressBooleanArray::new(8);
        ip.copy_rows_by_filter_from(source, &predicate).unwrap();
        let out = ip.finish().unwrap();
        let out = out.as_boolean();
        assert_eq!(out.len(), 3);
        assert_eq!(out.value(0), true);
        assert!(out.is_null(1));
        assert!(out.is_null(2));
    }

    #[test]
    fn test_filter_all() {
        let source = make_bool(&[Some(true), Some(false), Some(true)]);
        let filter = BooleanArray::from(vec![true, true, true]);
        let predicate = FilterBuilder::new(&filter).build();
        assert!(matches!(predicate.selection(), FilterSelection::All { .. }));
        let mut ip = InProgressBooleanArray::new(8);
        ip.copy_rows_by_filter_from(source, &predicate).unwrap();
        let out = ip.finish().unwrap();
        let out = out.as_boolean();
        assert_eq!(out.len(), 3);
        assert_eq!(out.value(0), true);
        assert_eq!(out.value(1), false);
        assert_eq!(out.value(2), true);
    }

    #[test]
    fn test_filter_none() {
        let source = make_bool(&[Some(true), Some(false), Some(true)]);
        let filter = BooleanArray::from(vec![false, false, false]);
        let predicate = FilterBuilder::new(&filter).build();
        assert!(matches!(predicate.selection(), FilterSelection::None));
        let mut ip = InProgressBooleanArray::new(8);
        ip.copy_rows_by_filter_from(source, &predicate).unwrap();
        let out = ip.finish().unwrap();
        assert_eq!(out.len(), 0);
    }

    #[test]
    fn test_finish_empty() {
        let mut ip = InProgressBooleanArray::new(8);
        let out = ip.finish().unwrap();
        assert_eq!(out.len(), 0);
        assert_eq!(out.data_type(), &arrow_schema::DataType::Boolean);
    }

    #[test]
    fn test_size_empty() {
        let ip = InProgressBooleanArray::new(64);
        assert_eq!(ip.size(), 0);
    }

    #[test]
    fn test_null_backfill_after_non_null_rows() {
        // Rows 0-1 from a non-null source, then rows 0-1 from a null source.
        // The null_bits builder must be backfilled with 2 valid bits before the nulls.
        let non_null = make_bool(&[Some(true), Some(false)]);
        let with_null = make_bool(&[None, Some(true)]);
        let mut ip = InProgressBooleanArray::new(8);
        ip.set_source(Some(Arc::clone(&non_null)));
        ip.copy_rows(0, 2).unwrap();
        ip.set_source(Some(Arc::clone(&with_null)));
        ip.copy_rows(0, 2).unwrap();
        let out = ip.finish().unwrap();
        let out = out.as_boolean();
        assert_eq!(out.len(), 4);
        assert!(!out.is_null(0));
        assert!(!out.is_null(1));
        assert!(out.is_null(2));
        assert!(!out.is_null(3));
        assert_eq!(out.value(0), true);
        assert_eq!(out.value(1), false);
        assert_eq!(out.value(3), true);
        // null_bits must cover all 4 rows
        assert_eq!(out.nulls().unwrap().len(), 4);
    }

    #[test]
    fn test_copy_rows_after_mid_loop_finish() {
        // Simulates the push_batch pattern: source is set once, copy_rows is
        // called, finish() is called (flushing one output batch), then
        // copy_rows is called again on the same still-set source.
        let source = make_bool(&[Some(true), Some(false), Some(true), Some(false), Some(true)]);
        let mut ip = InProgressBooleanArray::new(8);
        ip.set_source(Some(Arc::clone(&source)));
        ip.copy_rows(0, 3).unwrap();
        let first = ip.finish().unwrap(); // source must survive finish
        assert_eq!(first.len(), 3);
        // source is still set — copy the remaining rows
        ip.copy_rows(3, 2).unwrap();
        let second = ip.finish().unwrap();
        let second = second.as_boolean();
        assert_eq!(second.len(), 2);
        assert_eq!(second.value(0), false);
        assert_eq!(second.value(1), true);
        assert!(second.nulls().is_none());
    }

    #[test]
    fn test_reuse_across_finish_cycles() {
        let source = make_bool(&[Some(true), Some(false), Some(true)]);
        let mut ip = InProgressBooleanArray::new(8);
        ip.set_source(Some(Arc::clone(&source)));
        ip.copy_rows(0, 3).unwrap();
        let first = ip.finish().unwrap();
        assert_eq!(first.len(), 3);

        // Second cycle on the same instance
        ip.set_source(Some(Arc::clone(&source)));
        ip.copy_rows(0, 2).unwrap();
        let second = ip.finish().unwrap();
        let second = second.as_boolean();
        assert_eq!(second.len(), 2);
        assert_eq!(second.value(0), true);
        assert_eq!(second.value(1), false);
        assert!(second.nulls().is_none());
    }
}
