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
use crate::filter::FilterPredicate;
use arrow_array::builder::{ArrayBuilder, FixedSizeBinaryBuilder};
use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, FixedSizeBinaryArray};
use arrow_schema::ArrowError;
use std::sync::Arc;

/// Specialized [`InProgressArray`] for [`FixedSizeBinaryArray`].
///
/// Uses [`FixedSizeBinaryBuilder::append_array`] to copy rows directly
/// via `extend_from_slice` on the value buffer, avoiding the
/// `concat_fallback` / `MutableArrayData` boxed-closure dispatch overhead
/// that the generic path incurs.
#[derive(Debug)]
pub(crate) struct InProgressFixedSizeBinaryArray {
    /// The current source array, if any
    source: Option<ArrayRef>,
    /// Fixed byte width of each element
    value_length: i32,
    /// Target batch size (used for pre-allocating builder capacity)
    batch_size: usize,
    /// Lazily-initialized builder; `None` until the first row is copied
    builder: Option<FixedSizeBinaryBuilder>,
}

impl InProgressFixedSizeBinaryArray {
    pub(crate) fn new(value_length: i32, batch_size: usize) -> Self {
        Self {
            source: None,
            value_length,
            batch_size,
            builder: None,
        }
    }

    /// Return the builder, creating it with pre-allocated capacity on first use.
    fn ensure_builder(&mut self) -> &mut FixedSizeBinaryBuilder {
        if self.builder.is_none() {
            self.builder = Some(FixedSizeBinaryBuilder::with_capacity(
                self.batch_size,
                self.value_length,
            ));
        }
        self.builder.as_mut().unwrap()
    }
}

impl InProgressArray for InProgressFixedSizeBinaryArray {
    fn set_source(&mut self, source: Option<ArrayRef>) {
        self.source = source;
    }

    fn copy_rows(&mut self, offset: usize, len: usize) -> Result<(), ArrowError> {
        let source = self.source.as_ref().ok_or_else(|| {
            ArrowError::InvalidArgumentError(
                "Internal Error: InProgressFixedSizeBinaryArray: source not set".to_string(),
            )
        })?;
        // FixedSizeBinaryArray::slice normalizes the offset into value_data,
        // so append_array sees a contiguous slice with no leading padding.
        let sliced = source.as_fixed_size_binary().slice(offset, len);
        self.ensure_builder().append_array(&sliced)?;
        Ok(())
    }

    fn copy_rows_by_filter_from(
        &mut self,
        source: ArrayRef,
        filter: &FilterPredicate,
    ) -> Result<(), ArrowError> {
        let filtered = filter.filter(source.as_ref())?;
        if filtered.is_empty() {
            return Ok(());
        }
        let fsb = filtered
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .ok_or_else(|| {
                ArrowError::InvalidArgumentError(
                    "Expected FixedSizeBinaryArray after filter".to_string(),
                )
            })?;
        self.ensure_builder().append_array(fsb)?;
        Ok(())
    }

    fn finish(&mut self) -> Result<ArrayRef, ArrowError> {
        match self.builder.take() {
            Some(mut b) => Ok(Arc::new(b.finish())),
            None => {
                // No data was ever pushed; return a zero-length array.
                let mut b = FixedSizeBinaryBuilder::new(self.value_length);
                Ok(Arc::new(b.finish()))
            }
        }
    }

    fn size(&self) -> usize {
        // Count the builder's allocated bytes (value buffer + null bitmap).
        let builder_size = self.builder.as_ref().map_or(0, |b| {
            b.len() * self.value_length as usize + b.len().div_ceil(8)
        });
        builder_size
            + self
                .source
                .as_ref()
                .map_or(0, |a| a.get_array_memory_size())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filter::FilterBuilder;
    use arrow_array::BooleanArray;

    fn make_fsb(value_length: i32, data: &[Option<&[u8]>]) -> FixedSizeBinaryArray {
        let mut b = FixedSizeBinaryBuilder::with_capacity(data.len(), value_length);
        for v in data {
            match v {
                Some(bytes) => b.append_value(bytes).unwrap(),
                None => b.append_null(),
            }
        }
        b.finish()
    }

    #[test]
    fn test_roundtrip_non_null() {
        let src = Arc::new(make_fsb(4, &[Some(b"abcd"), Some(b"efgh"), Some(b"ijkl")])) as ArrayRef;

        let mut ip = InProgressFixedSizeBinaryArray::new(4, 8);
        ip.set_source(Some(Arc::clone(&src)));
        ip.copy_rows(0, 3).unwrap();

        let result = ip.finish().unwrap();
        let result = result.as_fixed_size_binary();
        assert_eq!(result.len(), 3);
        assert_eq!(result.value(0), b"abcd");
        assert_eq!(result.value(1), b"efgh");
        assert_eq!(result.value(2), b"ijkl");
    }

    #[test]
    fn test_roundtrip_with_nulls() {
        let src = Arc::new(make_fsb(4, &[Some(b"abcd"), None, Some(b"ijkl")])) as ArrayRef;

        let mut ip = InProgressFixedSizeBinaryArray::new(4, 8);
        ip.set_source(Some(Arc::clone(&src)));
        ip.copy_rows(0, 3).unwrap();

        let result = ip.finish().unwrap();
        let result = result.as_fixed_size_binary();
        assert_eq!(result.len(), 3);
        assert_eq!(result.value(0), b"abcd");
        assert!(result.is_null(1));
        assert_eq!(result.value(2), b"ijkl");
    }

    #[test]
    fn test_multiple_copy_rows_accumulate() {
        let src = Arc::new(make_fsb(
            4,
            &[Some(b"aaaa"), Some(b"bbbb"), Some(b"cccc"), Some(b"dddd")],
        )) as ArrayRef;

        let mut ip = InProgressFixedSizeBinaryArray::new(4, 8);
        ip.set_source(Some(Arc::clone(&src)));
        ip.copy_rows(0, 2).unwrap();
        ip.copy_rows(2, 2).unwrap();

        let result = ip.finish().unwrap();
        let result = result.as_fixed_size_binary();
        assert_eq!(result.len(), 4);
        assert_eq!(result.value(0), b"aaaa");
        assert_eq!(result.value(1), b"bbbb");
        assert_eq!(result.value(2), b"cccc");
        assert_eq!(result.value(3), b"dddd");
    }

    #[test]
    fn test_finish_on_empty_produces_zero_length_array() {
        let mut ip = InProgressFixedSizeBinaryArray::new(16, 8);
        let result = ip.finish().unwrap();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_value_length_16() {
        let value: Vec<u8> = (0u8..16).collect();
        let src = Arc::new(make_fsb(16, &[Some(&value), None])) as ArrayRef;

        let mut ip = InProgressFixedSizeBinaryArray::new(16, 8);
        ip.set_source(Some(Arc::clone(&src)));
        ip.copy_rows(0, 2).unwrap();

        let result = ip.finish().unwrap();
        let result = result.as_fixed_size_binary();
        assert_eq!(result.value_length(), 16);
        assert_eq!(result.value(0), &value[..]);
        assert!(result.is_null(1));
    }

    #[test]
    fn test_value_length_32() {
        let value: Vec<u8> = (0u8..32).collect();
        let src = Arc::new(make_fsb(32, &[Some(&value)])) as ArrayRef;

        let mut ip = InProgressFixedSizeBinaryArray::new(32, 8);
        ip.set_source(Some(Arc::clone(&src)));
        ip.copy_rows(0, 1).unwrap();

        let result = ip.finish().unwrap();
        let result = result.as_fixed_size_binary();
        assert_eq!(result.value_length(), 32);
        assert_eq!(result.value(0), &value[..]);
    }

    #[test]
    fn test_copy_rows_by_filter_from() {
        let src = Arc::new(make_fsb(
            4,
            &[Some(b"aaaa"), Some(b"bbbb"), Some(b"cccc"), Some(b"dddd")],
        )) as ArrayRef;

        let filter_arr = BooleanArray::from(vec![true, false, true, false]);
        let predicate = FilterBuilder::new(&filter_arr).build();

        let mut ip = InProgressFixedSizeBinaryArray::new(4, 8);
        ip.copy_rows_by_filter_from(src, &predicate).unwrap();

        let result = ip.finish().unwrap();
        let result = result.as_fixed_size_binary();
        assert_eq!(result.len(), 2);
        assert_eq!(result.value(0), b"aaaa");
        assert_eq!(result.value(1), b"cccc");
    }

    #[test]
    fn test_offset_copy_rows() {
        // Ensure copy_rows with a non-zero offset works correctly
        let src = Arc::new(make_fsb(4, &[Some(b"aaaa"), Some(b"bbbb"), Some(b"cccc")])) as ArrayRef;

        let mut ip = InProgressFixedSizeBinaryArray::new(4, 8);
        ip.set_source(Some(Arc::clone(&src)));
        ip.copy_rows(1, 2).unwrap(); // skip "aaaa", copy "bbbb" and "cccc"

        let result = ip.finish().unwrap();
        let result = result.as_fixed_size_binary();
        assert_eq!(result.len(), 2);
        assert_eq!(result.value(0), b"bbbb");
        assert_eq!(result.value(1), b"cccc");
    }
}
