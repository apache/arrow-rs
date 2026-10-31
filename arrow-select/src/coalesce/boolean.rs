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
use crate::filter::{FilterPredicate, filter_bits_compress, filter_bits_compress_pair};
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
        if filter.filter_len() > source.len() {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Filter predicate of length {} is larger than target array of length {}",
                filter.filter_len(),
                source.len()
            )));
        }

        let source = source.as_boolean();
        let output_values = self
            .values
            .get_or_insert_with(|| BooleanBufferBuilder::new(self.batch_size));
        // Treat all-valid sources as non-null to keep output validity lazy.
        if let Some(source_nulls) = source.nulls().filter(|nulls| nulls.null_count() != 0) {
            filter_bits_compress_pair(
                source.values(),
                source_nulls.inner(),
                filter,
                |values, validity, count| {
                    output_values.append_word(values, count);
                    self.nulls.append_word(validity, count);
                },
            );
        } else {
            filter_bits_compress(source.values(), filter, |values, count| {
                output_values.append_word(values, count)
            });
            self.nulls.append_n_non_nulls(filter.count());
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
    use arrow_buffer::NullBuffer;

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

    const FILTER_LEN: usize = 269;
    const SOURCE_OFFSET: usize = 3;

    #[test]
    fn filtered_words_without_nulls_match_filter_kernel() {
        let len = SOURCE_OFFSET + FILTER_LEN;
        let values = (0..len).map(source_value).collect();
        let all_valid = BooleanArray::new(values, Some(NullBuffer::new_valid(len)))
            .slice(SOURCE_OFFSET, FILTER_LEN);
        let without_validity = non_null_source();
        assert!(without_validity.nulls().is_none());
        assert!(all_valid.nulls().is_some());

        for (case, source) in [
            ("without validity", without_validity),
            ("all-valid validity", all_valid),
        ] {
            assert_eq!(source.null_count(), 0);
            assert_filtered_words_match_filter_kernel(case, source);
        }
    }

    #[test]
    fn filtered_words_with_nulls_match_filter_kernel() {
        let source = BooleanArray::from_iter(
            (0..SOURCE_OFFSET + FILTER_LEN)
                .map(|index| (index % 11 != 0).then_some(index % 3 == 0 || index % 7 == 0)),
        )
        .slice(SOURCE_OFFSET, FILTER_LEN);
        assert!(source.null_count() > 0);

        assert_filtered_words_match_filter_kernel("with nulls", source);
    }

    fn assert_filtered_words_match_filter_kernel(source_case: &str, source: BooleanArray) {
        for (filter_case, filter) in filter_cases() {
            assert_filter_copy_matches_kernel(source_case, filter_case, &source, &filter);
        }
    }

    fn filter_cases() -> Vec<(&'static str, BooleanArray)> {
        let selections = [
            ("none", vec![]),
            ("all", (0..FILTER_LEN).collect()),
            (
                "word boundaries",
                vec![0, 1, 7, 63, 64, 65, 127, 128, 191, 192, 255, 256, 268],
            ),
            (
                "carry across source words",
                // The first two chunks leave 24 carried bits; the third completes a word.
                (0..40).chain(64..112).chain(128..168).collect(),
            ),
            ("alternating", (0..FILTER_LEN).step_by(2).collect()),
        ];
        let mut filters: Vec<(&str, BooleanArray)> = selections
            .into_iter()
            .map(|(case, selected_indices)| {
                let filter = (0..FILTER_LEN)
                    .map(|index| selected_indices.contains(&index))
                    .collect();
                (case, filter)
            })
            .collect();

        const FILTER_OFFSET: usize = 5;
        let sliced_filter: BooleanArray = std::iter::repeat_n(false, FILTER_OFFSET)
            .chain((0..FILTER_LEN).map(|index| index.is_multiple_of(20)))
            .collect();
        let sliced_filter = sliced_filter.slice(FILTER_OFFSET, FILTER_LEN);
        assert_eq!(sliced_filter.values().offset(), FILTER_OFFSET);
        filters.push(("sliced filter", sliced_filter));

        let shorter_filter = (0_usize..197)
            .map(|index| index.is_multiple_of(17))
            .collect();
        filters.push(("shorter than source", shorter_filter));

        let aligned_filter = (0_usize..256)
            .map(|index| index.is_multiple_of(19))
            .collect();
        filters.push(("empty remainder", aligned_filter));

        let mut filter_values = vec![false; FILTER_LEN];
        for index in [1, 65, 129, 268] {
            filter_values[index] = true;
        }
        let mut filter_validity = vec![true; FILTER_LEN];
        filter_validity[65] = false;
        let filter_with_null = BooleanArray::new(
            BooleanBuffer::from(filter_values),
            Some(NullBuffer::new(BooleanBuffer::from(filter_validity))),
        );
        assert!(filter_with_null.value(65));
        assert!(filter_with_null.is_null(65));
        filters.push(("null filter entry", filter_with_null));
        filters
    }

    fn assert_filter_copy_matches_kernel(
        source_case: &str,
        filter_case: &str,
        source: &BooleanArray,
        filter: &BooleanArray,
    ) {
        const PREFIX_LEN: usize = 5;
        let expect_lazy_validity = source.null_count() == 0;

        for optimize in [false, true] {
            let builder = FilterBuilder::new(filter);
            let predicate = if optimize {
                builder.optimize().build()
            } else {
                builder.build()
            };
            let filtered = predicate.filter(source).unwrap();
            let prefix = source.slice(0, PREFIX_LEN);
            let expected = crate::concat::concat(&[&prefix, filtered.as_ref()]).unwrap();

            let source = Arc::new(source.clone()) as ArrayRef;
            let mut in_progress = InProgressBooleanArray::new(FILTER_LEN);
            in_progress.set_source(Some(Arc::clone(&source)));
            in_progress.copy_rows(0, PREFIX_LEN).unwrap();
            in_progress.set_source(None);
            in_progress
                .copy_rows_by_filter_from(source, &predicate)
                .unwrap();

            let output = in_progress.finish().unwrap();
            assert_eq!(
                output.as_boolean(),
                expected.as_boolean(),
                "{source_case}, {filter_case}, optimize={optimize}"
            );
            if expect_lazy_validity {
                assert!(
                    output.nulls().is_none(),
                    "{source_case}, {filter_case}, optimize={optimize} should preserve lazy validity"
                );
            }
        }
    }

    fn non_null_source() -> BooleanArray {
        let source: BooleanArray = (0..SOURCE_OFFSET + FILTER_LEN).map(source_value).collect();
        source.slice(SOURCE_OFFSET, FILTER_LEN)
    }

    fn source_value(index: usize) -> bool {
        index.is_multiple_of(3) || index.is_multiple_of(7)
    }

    #[test]
    fn filter_longer_than_source_returns_error() {
        let source = Arc::new(BooleanArray::from(vec![true, false])) as ArrayRef;
        let filter_mask = BooleanArray::from(vec![true, false, true]);
        let predicate = FilterBuilder::new(&filter_mask).build();
        let mut in_progress = InProgressBooleanArray::new(8);

        let error = in_progress
            .copy_rows_by_filter_from(source, &predicate)
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "Invalid argument error: Filter predicate of length 3 is larger than target array of length 2"
        );
    }
}
