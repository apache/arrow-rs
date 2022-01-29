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

//! Defines miscellaneous array kernels.

use crate::array::*;
use crate::buffer::buffer_bin_and;
use crate::datatypes::DataType;
use crate::error::Result;
use crate::record_batch::RecordBatch;
use crate::util::bit_chunk_iterator::{UnalignedBitChunk, UnalignedBitChunkIterator};

/// Function that can filter arbitrary arrays
pub type Filter<'a> = Box<dyn Fn(&ArrayData) -> ArrayData + 'a>;

/// An iterator of `(usize, usize)` each representing an interval `[start,end[` whose
/// slots of a [BooleanArray] are true. Each interval corresponds to a contiguous region of memory to be
/// "taken" from an array to be filtered.
#[derive(Debug)]
pub struct SlicesIterator<'a> {
    iter: UnalignedBitChunkIterator<'a>,
    len: usize,
    chunk_end_offset: usize,
    current_chunk: u64,
}

impl<'a> SlicesIterator<'a> {
    pub fn new(filter: &'a BooleanArray) -> Self {
        let values = &filter.data_ref().buffers()[0];
        let len = filter.len();
        let chunk = UnalignedBitChunk::new(values.as_slice(), filter.offset(), len);
        let mut iter = chunk.iter();

        let chunk_end_offset = 64 - chunk.lead_padding();
        let current_chunk = iter.next().unwrap_or(0);

        Self {
            iter,
            len,
            chunk_end_offset,
            current_chunk,
        }
    }

    fn advance_to_set_bit(&mut self) -> Option<(usize, u32)> {
        loop {
            if self.current_chunk != 0 {
                // Find the index of the first 1
                let bit_pos = self.current_chunk.trailing_zeros();
                return Some((self.chunk_end_offset, bit_pos));
            }

            self.current_chunk = self.iter.next()?;
            self.chunk_end_offset += 64;
        }
    }
}

impl<'a> Iterator for SlicesIterator<'a> {
    type Item = (usize, usize);

    fn next(&mut self) -> Option<Self::Item> {
        // Used as termination condition
        if self.len == 0 {
            return None;
        }

        let (start_chunk, start_bit) = self.advance_to_set_bit()?;

        // Set bits up to start
        self.current_chunk |= (1 << start_bit) - 1;

        loop {
            if self.current_chunk != u64::MAX {
                // Find the index of the first 0
                let end_bit = self.current_chunk.trailing_ones();

                // Zero out up to end_bit
                self.current_chunk &= !((1 << end_bit) - 1);

                return Some((
                    start_chunk + start_bit as usize - 64,
                    self.chunk_end_offset + end_bit as usize - 64,
                ));
            }

            match self.iter.next() {
                Some(next) => {
                    self.current_chunk = next;
                    self.chunk_end_offset += 64;
                }
                None => {
                    return Some((
                        start_chunk + start_bit as usize - 64,
                        std::mem::replace(&mut self.len, 0),
                    ));
                }
            }
        }
    }
}

fn filter_count(filter: &BooleanArray) -> usize {
    filter
        .values()
        .count_set_bits_offset(filter.offset(), filter.len())
}

/// Returns a prepared function optimized to filter multiple arrays.
/// Creating this function requires time, but using it is faster than [filter] when the
/// same filter needs to be applied to multiple arrays (e.g. a multi-column `RecordBatch`).
/// WARNING: the nulls of `filter` are ignored and the value on its slot is considered.
/// Therefore, it is considered undefined behavior to pass `filter` with null values.
pub fn build_filter(filter: &BooleanArray) -> Result<Filter> {
    let iter = SlicesIterator::new(filter);
    let filter_count = filter_count(filter);
    let chunks = iter.collect::<Vec<_>>();

    Ok(Box::new(move |array: &ArrayData| {
        match filter_count {
            // return all
            len if len == array.len() => array.clone(),
            0 => ArrayData::new_empty(array.data_type()),
            _ => {
                let mut mutable = MutableArrayData::new(vec![array], false, filter_count);
                chunks
                    .iter()
                    .for_each(|(start, end)| mutable.extend(0, *start, *end));
                mutable.freeze()
            }
        }
    }))
}

/// Remove null values by do a bitmask AND operation with null bits and the boolean bits.
pub fn prep_null_mask_filter(filter: &BooleanArray) -> BooleanArray {
    let array_data = filter.data_ref();
    let null_bitmap = array_data.null_buffer().unwrap();
    let mask = filter.values();
    let offset = filter.offset();

    let new_mask = buffer_bin_and(mask, offset, null_bitmap, offset, filter.len());

    let array_data = ArrayData::builder(DataType::Boolean)
        .len(filter.len())
        .add_buffer(new_mask);

    let array_data = unsafe { array_data.build_unchecked() };

    BooleanArray::from(array_data)
}

/// Filters an [Array], returning elements matching the filter (i.e. where the values are true).
///
/// # Example
/// ```rust
/// # use arrow::array::{Int32Array, BooleanArray};
/// # use arrow::error::Result;
/// # use arrow::compute::kernels::filter::filter;
/// # fn main() -> Result<()> {
/// let array = Int32Array::from(vec![5, 6, 7, 8, 9]);
/// let filter_array = BooleanArray::from(vec![true, false, false, true, false]);
/// let c = filter(&array, &filter_array)?;
/// let c = c.as_any().downcast_ref::<Int32Array>().unwrap();
/// assert_eq!(c, &Int32Array::from(vec![5, 8]));
/// # Ok(())
/// # }
/// ```
pub fn filter(array: &dyn Array, predicate: &BooleanArray) -> Result<ArrayRef> {
    if predicate.null_count() > 0 {
        // this greatly simplifies subsequent filtering code
        // now we only have a boolean mask to deal with
        let predicate = prep_null_mask_filter(predicate);
        return filter(array, &predicate);
    }

    let filter_count = filter_count(predicate);

    match filter_count {
        0 => {
            // return empty
            Ok(new_empty_array(array.data_type()))
        }
        len if len == array.len() => {
            // return all
            let data = array.data().clone();
            Ok(make_array(data))
        }
        _ => {
            // actually filter
            let mut mutable =
                MutableArrayData::new(vec![array.data_ref()], false, filter_count);

            let iter = SlicesIterator::new(predicate);
            iter.for_each(|(start, end)| mutable.extend(0, start, end));

            let data = mutable.freeze();
            Ok(make_array(data))
        }
    }
}

/// Returns a new [RecordBatch] with arrays containing only values matching the filter.
pub fn filter_record_batch(
    record_batch: &RecordBatch,
    predicate: &BooleanArray,
) -> Result<RecordBatch> {
    if predicate.null_count() > 0 {
        // this greatly simplifies subsequent filtering code
        // now we only have a boolean mask to deal with
        let predicate = prep_null_mask_filter(predicate);
        return filter_record_batch(record_batch, &predicate);
    }

    let num_columns = record_batch.columns().len();

    let filtered_arrays = match num_columns {
        1 => {
            vec![filter(record_batch.columns()[0].as_ref(), predicate)?]
        }
        _ => {
            let filter = build_filter(predicate)?;
            record_batch
                .columns()
                .iter()
                .map(|a| make_array(filter(a.data())))
                .collect()
        }
    };
    RecordBatch::try_new(record_batch.schema(), filtered_arrays)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datatypes::Int64Type;
    use crate::{
        buffer::Buffer,
        datatypes::{DataType, Field},
    };
    use rand::prelude::*;

    macro_rules! def_temporal_test {
        ($test:ident, $array_type: ident, $data: expr) => {
            #[test]
            fn $test() {
                let a = $data;
                let b = BooleanArray::from(vec![true, false, true, false]);
                let c = filter(&a, &b).unwrap();
                let d = c.as_ref().as_any().downcast_ref::<$array_type>().unwrap();
                assert_eq!(2, d.len());
                assert_eq!(1, d.value(0));
                assert_eq!(3, d.value(1));
            }
        };
    }

    def_temporal_test!(
        test_filter_date32,
        Date32Array,
        Date32Array::from(vec![1, 2, 3, 4])
    );
    def_temporal_test!(
        test_filter_date64,
        Date64Array,
        Date64Array::from(vec![1, 2, 3, 4])
    );
    def_temporal_test!(
        test_filter_time32_second,
        Time32SecondArray,
        Time32SecondArray::from(vec![1, 2, 3, 4])
    );
    def_temporal_test!(
        test_filter_time32_millisecond,
        Time32MillisecondArray,
        Time32MillisecondArray::from(vec![1, 2, 3, 4])
    );
    def_temporal_test!(
        test_filter_time64_microsecond,
        Time64MicrosecondArray,
        Time64MicrosecondArray::from(vec![1, 2, 3, 4])
    );
    def_temporal_test!(
        test_filter_time64_nanosecond,
        Time64NanosecondArray,
        Time64NanosecondArray::from(vec![1, 2, 3, 4])
    );
    def_temporal_test!(
        test_filter_duration_second,
        DurationSecondArray,
        DurationSecondArray::from(vec![1, 2, 3, 4])
    );
    def_temporal_test!(
        test_filter_duration_millisecond,
        DurationMillisecondArray,
        DurationMillisecondArray::from(vec![1, 2, 3, 4])
    );
    def_temporal_test!(
        test_filter_duration_microsecond,
        DurationMicrosecondArray,
        DurationMicrosecondArray::from(vec![1, 2, 3, 4])
    );
    def_temporal_test!(
        test_filter_duration_nanosecond,
        DurationNanosecondArray,
        DurationNanosecondArray::from(vec![1, 2, 3, 4])
    );
    def_temporal_test!(
        test_filter_timestamp_second,
        TimestampSecondArray,
        TimestampSecondArray::from_vec(vec![1, 2, 3, 4], None)
    );
    def_temporal_test!(
        test_filter_timestamp_millisecond,
        TimestampMillisecondArray,
        TimestampMillisecondArray::from_vec(vec![1, 2, 3, 4], None)
    );
    def_temporal_test!(
        test_filter_timestamp_microsecond,
        TimestampMicrosecondArray,
        TimestampMicrosecondArray::from_vec(vec![1, 2, 3, 4], None)
    );
    def_temporal_test!(
        test_filter_timestamp_nanosecond,
        TimestampNanosecondArray,
        TimestampNanosecondArray::from_vec(vec![1, 2, 3, 4], None)
    );

    #[test]
    fn test_filter_array_slice() {
        let a_slice = Int32Array::from(vec![5, 6, 7, 8, 9]).slice(1, 4);
        let a = a_slice.as_ref();
        let b = BooleanArray::from(vec![true, false, false, true]);
        // filtering with sliced filter array is not currently supported
        // let b_slice = BooleanArray::from(vec![true, false, false, true, false]).slice(1, 4);
        // let b = b_slice.as_any().downcast_ref().unwrap();
        let c = filter(a, &b).unwrap();
        let d = c.as_ref().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(2, d.len());
        assert_eq!(6, d.value(0));
        assert_eq!(9, d.value(1));
    }

    #[test]
    fn test_filter_array_low_density() {
        // this test exercises the all 0's branch of the filter algorithm
        let mut data_values = (1..=65).collect::<Vec<i32>>();
        let mut filter_values =
            (1..=65).map(|i| matches!(i % 65, 0)).collect::<Vec<bool>>();
        // set up two more values after the batch
        data_values.extend_from_slice(&[66, 67]);
        filter_values.extend_from_slice(&[false, true]);
        let a = Int32Array::from(data_values);
        let b = BooleanArray::from(filter_values);
        let c = filter(&a, &b).unwrap();
        let d = c.as_ref().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(2, d.len());
        assert_eq!(65, d.value(0));
        assert_eq!(67, d.value(1));
    }

    #[test]
    fn test_filter_array_high_density() {
        // this test exercises the all 1's branch of the filter algorithm
        let mut data_values = (1..=65).map(Some).collect::<Vec<_>>();
        let mut filter_values = (1..=65)
            .map(|i| !matches!(i % 65, 0))
            .collect::<Vec<bool>>();
        // set second data value to null
        data_values[1] = None;
        // set up two more values after the batch
        data_values.extend_from_slice(&[Some(66), None, Some(67), None]);
        filter_values.extend_from_slice(&[false, true, true, true]);
        let a = Int32Array::from(data_values);
        let b = BooleanArray::from(filter_values);
        let c = filter(&a, &b).unwrap();
        let d = c.as_ref().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(67, d.len());
        assert_eq!(3, d.null_count());
        assert_eq!(1, d.value(0));
        assert!(d.is_null(1));
        assert_eq!(64, d.value(63));
        assert!(d.is_null(64));
        assert_eq!(67, d.value(65));
    }

    #[test]
    fn test_filter_string_array_simple() {
        let a = StringArray::from(vec!["hello", " ", "world", "!"]);
        let b = BooleanArray::from(vec![true, false, true, false]);
        let c = filter(&a, &b).unwrap();
        let d = c.as_ref().as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(2, d.len());
        assert_eq!("hello", d.value(0));
        assert_eq!("world", d.value(1));
    }

    #[test]
    fn test_filter_primitive_array_with_null() {
        let a = Int32Array::from(vec![Some(5), None]);
        let b = BooleanArray::from(vec![false, true]);
        let c = filter(&a, &b).unwrap();
        let d = c.as_ref().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(1, d.len());
        assert!(d.is_null(0));
    }

    #[test]
    fn test_filter_string_array_with_null() {
        let a = StringArray::from(vec![Some("hello"), None, Some("world"), None]);
        let b = BooleanArray::from(vec![true, false, false, true]);
        let c = filter(&a, &b).unwrap();
        let d = c.as_ref().as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(2, d.len());
        assert_eq!("hello", d.value(0));
        assert!(!d.is_null(0));
        assert!(d.is_null(1));
    }

    #[test]
    fn test_filter_binary_array_with_null() {
        let data: Vec<Option<&[u8]>> = vec![Some(b"hello"), None, Some(b"world"), None];
        let a = BinaryArray::from(data);
        let b = BooleanArray::from(vec![true, false, false, true]);
        let c = filter(&a, &b).unwrap();
        let d = c.as_ref().as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(2, d.len());
        assert_eq!(b"hello", d.value(0));
        assert!(!d.is_null(0));
        assert!(d.is_null(1));
    }

    #[test]
    fn test_filter_array_slice_with_null() {
        let a_slice =
            Int32Array::from(vec![Some(5), None, Some(7), Some(8), Some(9)]).slice(1, 4);
        let a = a_slice.as_ref();
        let b = BooleanArray::from(vec![true, false, false, true]);
        // filtering with sliced filter array is not currently supported
        // let b_slice = BooleanArray::from(vec![true, false, false, true, false]).slice(1, 4);
        // let b = b_slice.as_any().downcast_ref().unwrap();
        let c = filter(a, &b).unwrap();
        let d = c.as_ref().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(2, d.len());
        assert!(d.is_null(0));
        assert!(!d.is_null(1));
        assert_eq!(9, d.value(1));
    }

    #[test]
    fn test_filter_dictionary_array() {
        let values = vec![Some("hello"), None, Some("world"), Some("!")];
        let a: Int8DictionaryArray = values.iter().copied().collect();
        let b = BooleanArray::from(vec![false, true, true, false]);
        let c = filter(&a, &b).unwrap();
        let d = c
            .as_ref()
            .as_any()
            .downcast_ref::<Int8DictionaryArray>()
            .unwrap();
        let value_array = d.values();
        let values = value_array.as_any().downcast_ref::<StringArray>().unwrap();
        // values are cloned in the filtered dictionary array
        assert_eq!(3, values.len());
        // but keys are filtered
        assert_eq!(2, d.len());
        assert!(d.is_null(0));
        assert_eq!("world", values.value(d.keys().value(1) as usize));
    }

    #[test]
    fn test_filter_string_array_with_negated_boolean_array() {
        let a = StringArray::from(vec!["hello", " ", "world", "!"]);
        let mut bb = BooleanBuilder::new(2);
        bb.append_value(false).unwrap();
        bb.append_value(true).unwrap();
        bb.append_value(false).unwrap();
        bb.append_value(true).unwrap();
        let b = bb.finish();
        let b = crate::compute::not(&b).unwrap();

        let c = filter(&a, &b).unwrap();
        let d = c.as_ref().as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(2, d.len());
        assert_eq!("hello", d.value(0));
        assert_eq!("world", d.value(1));
    }

    #[test]
    fn test_filter_list_array() {
        let value_data = ArrayData::builder(DataType::Int32)
            .len(8)
            .add_buffer(Buffer::from_slice_ref(&[0, 1, 2, 3, 4, 5, 6, 7]))
            .build()
            .unwrap();

        let value_offsets = Buffer::from_slice_ref(&[0i64, 3, 6, 8, 8]);

        let list_data_type =
            DataType::LargeList(Box::new(Field::new("item", DataType::Int32, false)));
        let list_data = ArrayData::builder(list_data_type)
            .len(4)
            .add_buffer(value_offsets)
            .add_child_data(value_data)
            .null_bit_buffer(Buffer::from([0b00000111]))
            .build()
            .unwrap();

        //  a = [[0, 1, 2], [3, 4, 5], [6, 7], null]
        let a = LargeListArray::from(list_data);
        let b = BooleanArray::from(vec![false, true, false, true]);
        let result = filter(&a, &b).unwrap();

        // expected: [[3, 4, 5], null]
        let value_data = ArrayData::builder(DataType::Int32)
            .len(3)
            .add_buffer(Buffer::from_slice_ref(&[3, 4, 5]))
            .build()
            .unwrap();

        let value_offsets = Buffer::from_slice_ref(&[0i64, 3, 3]);

        let list_data_type =
            DataType::LargeList(Box::new(Field::new("item", DataType::Int32, false)));
        let expected = ArrayData::builder(list_data_type)
            .len(2)
            .add_buffer(value_offsets)
            .add_child_data(value_data)
            .null_bit_buffer(Buffer::from([0b00000001]))
            .build()
            .unwrap();

        assert_eq!(&make_array(expected), &result);
    }

    #[test]
    fn test_slice_iterator_bits() {
        let filter_values = (0..64).map(|i| i == 1).collect::<Vec<bool>>();
        let filter = BooleanArray::from(filter_values);
        let filter_count = filter_count(&filter);

        let iter = SlicesIterator::new(&filter);
        let chunks = iter.collect::<Vec<_>>();

        assert_eq!(chunks, vec![(1, 2)]);
        assert_eq!(filter_count, 1);
    }

    #[test]
    fn test_slice_iterator_bits1() {
        let filter_values = (0..64).map(|i| i != 1).collect::<Vec<bool>>();
        let filter = BooleanArray::from(filter_values);
        let filter_count = filter_count(&filter);

        let iter = SlicesIterator::new(&filter);
        let chunks = iter.collect::<Vec<_>>();

        assert_eq!(chunks, vec![(0, 1), (2, 64)]);
        assert_eq!(filter_count, 64 - 1);
    }

    #[test]
    fn test_slice_iterator_chunk_and_bits() {
        let filter_values = (0..130).map(|i| i % 62 != 0).collect::<Vec<bool>>();
        let filter = BooleanArray::from(filter_values);
        let filter_count = filter_count(&filter);

        let iter = SlicesIterator::new(&filter);
        let chunks = iter.collect::<Vec<_>>();

        assert_eq!(chunks, vec![(1, 62), (63, 124), (125, 130)]);
        assert_eq!(filter_count, 61 + 61 + 5);
    }

    #[test]
    fn test_null_mask() -> Result<()> {
        use crate::compute::kernels::comparison;
        let a: PrimitiveArray<Int64Type> =
            PrimitiveArray::from(vec![Some(1), Some(2), None]);
        let mask0 = comparison::eq(&a, &a)?;
        let out0 = filter(&a, &mask0)?;
        let out_arr0 = out0
            .as_any()
            .downcast_ref::<PrimitiveArray<Int64Type>>()
            .unwrap();

        let mask1 = BooleanArray::from(vec![Some(true), Some(true), None]);
        let out1 = filter(&a, &mask1)?;
        let out_arr1 = out1
            .as_any()
            .downcast_ref::<PrimitiveArray<Int64Type>>()
            .unwrap();
        assert_eq!(mask0, mask1);
        assert_eq!(out_arr0, out_arr1);
        Ok(())
    }

    #[test]
    fn test_fast_path() -> Result<()> {
        let a: PrimitiveArray<Int64Type> =
            PrimitiveArray::from(vec![Some(1), Some(2), None]);

        // all true
        let mask = BooleanArray::from(vec![true, true, true]);
        let out = filter(&a, &mask)?;
        let b = out
            .as_any()
            .downcast_ref::<PrimitiveArray<Int64Type>>()
            .unwrap();
        assert_eq!(&a, b);

        // all false
        let mask = BooleanArray::from(vec![false, false, false]);
        let out = filter(&a, &mask)?;
        assert_eq!(out.len(), 0);
        assert_eq!(out.data_type(), &DataType::Int64);
        Ok(())
    }

    fn test_slices_fuzz(mask_len: usize, offset: usize, truncate: usize) {
        let mut rng = thread_rng();

        let bools: Vec<bool> = std::iter::from_fn(|| Some(rng.gen()))
            .take(mask_len)
            .collect();

        let buffer = Buffer::from_iter(bools.iter().cloned());

        let truncated_length = mask_len - offset - truncate;

        let data = ArrayDataBuilder::new(DataType::Boolean)
            .len(truncated_length)
            .offset(offset)
            .add_buffer(buffer)
            .build()
            .unwrap();

        let bool_array = BooleanArray::from(data);

        let bits: Vec<_> = SlicesIterator::new(&bool_array)
            .flat_map(|(start, end)| start..end)
            .collect();

        let expected_bits: Vec<_> = bools
            .iter()
            .skip(offset)
            .take(truncated_length)
            .enumerate()
            .flat_map(|(idx, v)| v.then(|| idx))
            .collect();

        assert_eq!(bits, expected_bits);
    }

    #[test]
    fn fuzz_test_slices_iterator() {
        let mut rng = thread_rng();

        for _ in 0..100 {
            let mask_len = rng.gen_range(0..1024);
            let max_offset = 64.min(mask_len);
            let offset = rng.gen::<usize>().checked_rem(max_offset).unwrap_or(0);

            let max_truncate = 128.min(mask_len - offset);
            let truncate = rng.gen::<usize>().checked_rem(max_truncate).unwrap_or(0);

            test_slices_fuzz(mask_len, offset, truncate);
        }

        test_slices_fuzz(64, 0, 0);
        test_slices_fuzz(64, 8, 0);
        test_slices_fuzz(64, 8, 8);
        test_slices_fuzz(32, 8, 8);
        test_slices_fuzz(32, 5, 9);
    }
}
