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
use arrow_buffer::{BooleanBuffer, BooleanBufferBuilder, NullBufferBuilder};
use arrow_schema::ArrowError;
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct InProgressBooleanArray {
    source: Option<ArrayRef>,
    batch_size: usize,
    values: Option<BooleanBufferBuilder>,
    nulls: NullBufferBuilder,
}

impl InProgressBooleanArray {
    pub(crate) fn new(batch_size: usize) -> Self {
        Self {
            source: None,
            batch_size,
            values: None,
            nulls: NullBufferBuilder::new(batch_size),
        }
    }

    fn values(&mut self) -> &mut BooleanBufferBuilder {
        self.values
            .get_or_insert_with(|| BooleanBufferBuilder::new(self.batch_size))
    }

    fn append_array(&mut self, array: &BooleanArray) {
        self.values().append_buffer(array.values());
        if let Some(nulls) = array.nulls() {
            self.nulls.append_buffer(nulls);
        } else {
            self.nulls.append_n_non_nulls(array.len());
        }
    }
}

impl InProgressArray for InProgressBooleanArray {
    fn set_source(&mut self, source: Option<ArrayRef>) {
        self.source = source;
    }

    fn copy_rows(&mut self, offset: usize, len: usize) -> Result<(), ArrowError> {
        let source = self.source.as_ref().ok_or_else(|| {
            ArrowError::InvalidArgumentError(
                "Internal Error: InProgressBooleanArray: source not set".to_string(),
            )
        })?;
        let array = source.as_boolean().slice(offset, len);
        self.append_array(&array);
        Ok(())
    }

    fn copy_rows_by_filter_from(
        &mut self,
        source: ArrayRef,
        filter: &FilterPredicate,
    ) -> Result<(), ArrowError> {
        // Benchmarks show per-bit copying only wins at selectivities at or below 1/128.
        if filter.count() > filter.filter_len() / 128 {
            let filtered = filter.filter(source.as_ref())?;
            self.append_array(filtered.as_boolean());
            return Ok(());
        }

        let source = source.as_boolean();
        let source_values = source.values();
        if self.values.is_none() {
            self.values = Some(BooleanBufferBuilder::new(self.batch_size));
        }
        let values = self.values.as_mut().unwrap();
        let output_nulls = &mut self.nulls;
        let source_nulls = source.nulls().filter(|nulls| nulls.null_count() != 0);

        match filter.selection() {
            FilterSelection::None => {}
            FilterSelection::All { len } => {
                values.append_packed_range(
                    source_values.offset()..source_values.offset() + len,
                    source_values.values(),
                );
                if let Some(nulls) = source_nulls {
                    output_nulls.append_buffer(&nulls.slice(0, len));
                } else {
                    output_nulls.append_n_non_nulls(len);
                }
            }
            FilterSelection::Slices(slices) => {
                if let Some(nulls) = source_nulls {
                    slices.for_each(|(start, end)| {
                        values.append_packed_range(
                            start + source_values.offset()..end + source_values.offset(),
                            source_values.values(),
                        );
                        output_nulls.append_buffer(&nulls.slice(start, end - start));
                    });
                } else {
                    slices.for_each(|(start, end)| {
                        values.append_packed_range(
                            start + source_values.offset()..end + source_values.offset(),
                            source_values.values(),
                        );
                    });
                    output_nulls.append_n_non_nulls(filter.count());
                }
            }
            FilterSelection::Indices(indices) => {
                if let Some(nulls) = source_nulls {
                    indices.for_each(|index| {
                        values.append(source_values.value(index));
                        output_nulls.append(nulls.is_valid(index));
                    });
                } else {
                    indices.for_each(|index| values.append(source_values.value(index)));
                    output_nulls.append_n_non_nulls(filter.count());
                }
            }
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<ArrayRef, ArrowError> {
        let values = self
            .values
            .take()
            .map(|mut values| values.finish())
            .unwrap_or_else(|| BooleanBuffer::new_unset(0));
        let nulls = self.nulls.finish();
        self.nulls = NullBufferBuilder::new(self.batch_size);
        Ok(Arc::new(BooleanArray::new(values, nulls)))
    }

    fn size(&self) -> usize {
        self.source
            .as_ref()
            .map_or(0, |source| source.get_array_memory_size())
            + self
                .values
                .as_ref()
                .map_or(0, |values| values.capacity() / 8)
            + self.nulls.allocated_size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filter::FilterBuilder;

    #[test]
    fn copies_slices_and_sparse_filters() {
        let source = Arc::new(BooleanArray::from(vec![
            Some(true),
            None,
            Some(false),
            Some(true),
            Some(false),
        ])) as ArrayRef;
        let mut in_progress = InProgressBooleanArray::new(8);
        in_progress.set_source(Some(Arc::clone(&source)));
        in_progress.copy_rows(1, 2).unwrap();
        in_progress.set_source(None);

        let filter = BooleanArray::from(vec![true, false, false, true, false]);
        let predicate = FilterBuilder::new(&filter).build();
        in_progress
            .copy_rows_by_filter_from(source, &predicate)
            .unwrap();

        let output = in_progress.finish().unwrap();
        let expected = BooleanArray::from(vec![None, Some(false), Some(true), Some(true)]);
        assert_eq!(output.as_boolean(), &expected);
    }

    #[test]
    fn finish_resets_buffers() {
        let mut in_progress = InProgressBooleanArray::new(8);
        assert_eq!(in_progress.finish().unwrap().len(), 0);

        let source = Arc::new(BooleanArray::from(vec![
            Some(true),
            None,
            Some(false),
            None,
        ]));
        in_progress.set_source(Some(Arc::clone(&source) as ArrayRef));
        in_progress.copy_rows(0, 2).unwrap();
        let first = in_progress.finish().unwrap();
        assert_eq!(first.as_boolean(), &source.slice(0, 2));

        in_progress.copy_rows(2, 2).unwrap();
        let second = in_progress.finish().unwrap();
        assert_eq!(second.as_boolean(), &source.slice(2, 2));
        assert_eq!(in_progress.finish().unwrap().len(), 0);
    }
}
