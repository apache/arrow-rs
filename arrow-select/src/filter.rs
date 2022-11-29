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

//! Defines filter kernels

use std::sync::Arc;

use arrow_array::builder::BooleanBufferBuilder;
use arrow_array::cast::{as_generic_binary_array, as_largestring_array, as_string_array};
use arrow_array::types::ByteArrayType;
use arrow_array::*;
use arrow_buffer::bit_util;
use arrow_buffer::{buffer::buffer_bin_and, Buffer, MutableBuffer};
use arrow_data::bit_iterator::{BitIndexIterator, BitSliceIterator};
use arrow_data::transform::MutableArrayData;
use arrow_data::{ArrayData, ArrayDataBuilder};
use arrow_schema::*;

/// If the filter selects more than this fraction of rows, use
/// [`SlicesIterator`] to copy ranges of values. Otherwise iterate
/// over individual rows using [`IndexIterator`]
///
/// Threshold of 0.8 chosen based on <https://dl.acm.org/doi/abs/10.1145/3465998.3466009>
///
const FILTER_SLICES_SELECTIVITY_THRESHOLD: f64 = 0.8;

/// An iterator of `(usize, usize)` each representing an interval
/// `[start, end)` whose slots of a bitmap [Buffer] are true. Each
/// interval corresponds to a contiguous region of memory to be
/// "taken" from an array to be filtered.
///
/// ## Notes:
///
/// 1. Ignores the validity bitmap (ignores nulls)
///
/// 2. Only performant for filters that copy across long contiguous runs
#[derive(Debug)]
pub struct SlicesIterator<'a>(BitSliceIterator<'a>);

impl<'a> SlicesIterator<'a> {
    pub fn new(filter: &'a BooleanArray) -> Self {
        let values = &filter.data_ref().buffers()[0];
        let len = filter.len();
        let offset = filter.offset();

        Self(BitSliceIterator::new(values, offset, len))
    }
}

impl<'a> Iterator for SlicesIterator<'a> {
    type Item = (usize, usize);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

/// An iterator of `usize` whose index in [`BooleanArray`] is true
///
/// This provides the best performance on most predicates, apart from those which keep
/// large runs and therefore favour [`SlicesIterator`]
struct IndexIterator<'a> {
    remaining: usize,
    iter: BitIndexIterator<'a>,
}

impl<'a> IndexIterator<'a> {
    fn new(filter: &'a BooleanArray, remaining: usize) -> Self {
        assert_eq!(filter.null_count(), 0);
        let data = filter.data();
        let iter = BitIndexIterator::new(&data.buffers()[0], data.offset(), data.len());
        Self { remaining, iter }
    }
}

impl<'a> Iterator for IndexIterator<'a> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining != 0 {
            // Fascinatingly swapping these two lines around results in a 50%
            // performance regression for some benchmarks
            let next = self.iter.next().expect("IndexIterator exhausted early");
            self.remaining -= 1;
            // Must panic if exhausted early as trusted length iterator
            return Some(next);
        }
        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

/// Counts the number of set bits in `filter`
fn filter_count(filter: &BooleanArray) -> usize {
    filter
        .values()
        .count_set_bits_offset(filter.offset(), filter.len())
}

/// Function that can filter arbitrary arrays
///
/// Deprecated: Use [`FilterPredicate`] instead
#[deprecated]
pub type Filter<'a> = Box<dyn Fn(&ArrayData) -> ArrayData + 'a>;

/// Returns a prepared function optimized to filter multiple arrays.
/// Creating this function requires time, but using it is faster than [filter] when the
/// same filter needs to be applied to multiple arrays (e.g. a multi-column `RecordBatch`).
/// WARNING: the nulls of `filter` are ignored and the value on its slot is considered.
/// Therefore, it is considered undefined behavior to pass `filter` with null values.
///
/// Deprecated: Use [`FilterBuilder`] instead
#[deprecated]
#[allow(deprecated)]
pub fn build_filter(filter: &BooleanArray) -> Result<Filter, ArrowError> {
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
/// # use arrow_array::{Int32Array, BooleanArray};
/// # use arrow_select::filter::filter;
/// let array = Int32Array::from(vec![5, 6, 7, 8, 9]);
/// let filter_array = BooleanArray::from(vec![true, false, false, true, false]);
/// let c = filter(&array, &filter_array).unwrap();
/// let c = c.as_any().downcast_ref::<Int32Array>().unwrap();
/// assert_eq!(c, &Int32Array::from(vec![5, 8]));
/// ```
pub fn filter(
    values: &dyn Array,
    predicate: &BooleanArray,
) -> Result<ArrayRef, ArrowError> {
    let predicate = FilterBuilder::new(predicate).build();
    filter_array(values, &predicate)
}

/// Returns a new [RecordBatch] with arrays containing only values matching the filter.
pub fn filter_record_batch(
    record_batch: &RecordBatch,
    predicate: &BooleanArray,
) -> Result<RecordBatch, ArrowError> {
    let mut filter_builder = FilterBuilder::new(predicate);
    if record_batch.num_columns() > 1 {
        // Only optimize if filtering more than one column
        filter_builder = filter_builder.optimize();
    }
    let filter = filter_builder.build();

    let filtered_arrays = record_batch
        .columns()
        .iter()
        .map(|a| filter_array(a, &filter))
        .collect::<Result<Vec<_>, _>>()?;

    RecordBatch::try_new(record_batch.schema(), filtered_arrays)
}

/// A builder to construct [`FilterPredicate`]
#[derive(Debug)]
pub struct FilterBuilder {
    filter: BooleanArray,
    count: usize,
    strategy: IterationStrategy,
}

impl FilterBuilder {
    /// Create a new [`FilterBuilder`] that can be used to construct a [`FilterPredicate`]
    pub fn new(filter: &BooleanArray) -> Self {
        let filter = match filter.null_count() {
            0 => BooleanArray::from(filter.data().clone()),
            _ => prep_null_mask_filter(filter),
        };

        let count = filter_count(&filter);
        let strategy = IterationStrategy::default_strategy(filter.len(), count);

        Self {
            filter,
            count,
            strategy,
        }
    }

    /// Compute an optimised representation of the provided `filter` mask that can be
    /// applied to an array more quickly.
    ///
    /// Note: There is limited benefit to calling this to then filter a single array
    /// Note: This will likely have a larger memory footprint than the original mask
    pub fn optimize(mut self) -> Self {
        match self.strategy {
            IterationStrategy::SlicesIterator => {
                let slices = SlicesIterator::new(&self.filter).collect();
                self.strategy = IterationStrategy::Slices(slices)
            }
            IterationStrategy::IndexIterator => {
                let indices = IndexIterator::new(&self.filter, self.count).collect();
                self.strategy = IterationStrategy::Indices(indices)
            }
            _ => {}
        }
        self
    }

    /// Construct the final `FilterPredicate`
    pub fn build(self) -> FilterPredicate {
        FilterPredicate {
            filter: self.filter,
            count: self.count,
            strategy: self.strategy,
        }
    }
}

/// The iteration strategy used to evaluate [`FilterPredicate`]
#[derive(Debug)]
enum IterationStrategy {
    /// A lazily evaluated iterator of ranges
    SlicesIterator,
    /// A lazily evaluated iterator of indices
    IndexIterator,
    /// A precomputed list of indices
    Indices(Vec<usize>),
    /// A precomputed array of ranges
    Slices(Vec<(usize, usize)>),
    /// Select all rows
    All,
    /// Select no rows
    None,
}

impl IterationStrategy {
    /// The default [`IterationStrategy`] for a filter of length `filter_length`
    /// and selecting `filter_count` rows
    fn default_strategy(filter_length: usize, filter_count: usize) -> Self {
        if filter_length == 0 || filter_count == 0 {
            return IterationStrategy::None;
        }

        if filter_count == filter_length {
            return IterationStrategy::All;
        }

        // Compute the selectivity of the predicate by dividing the number of true
        // bits in the predicate by the predicate's total length
        //
        // This can then be used as a heuristic for the optimal iteration strategy
        let selectivity_frac = filter_count as f64 / filter_length as f64;
        if selectivity_frac > FILTER_SLICES_SELECTIVITY_THRESHOLD {
            return IterationStrategy::SlicesIterator;
        }
        IterationStrategy::IndexIterator
    }
}

/// A filtering predicate that can be applied to an [`Array`]
#[derive(Debug)]
pub struct FilterPredicate {
    filter: BooleanArray,
    count: usize,
    strategy: IterationStrategy,
}

impl FilterPredicate {
    /// Selects rows from `values` based on this [`FilterPredicate`]
    pub fn filter(&self, values: &dyn Array) -> Result<ArrayRef, ArrowError> {
        filter_array(values, self)
    }
}

fn filter_array(
    values: &dyn Array,
    predicate: &FilterPredicate,
) -> Result<ArrayRef, ArrowError> {
    if predicate.filter.len() > values.len() {
        return Err(ArrowError::InvalidArgumentError(format!(
            "Filter predicate of length {} is larger than target array of length {}",
            predicate.filter.len(),
            values.len()
        )));
    }

    match predicate.strategy {
        IterationStrategy::None => Ok(new_empty_array(values.data_type())),
        IterationStrategy::All => Ok(make_array(values.data().slice(0, predicate.count))),
        // actually filter
        _ => downcast_primitive_array! {
            values => Ok(Arc::new(filter_primitive(values, predicate))),
            DataType::Decimal128(p, s) => {
                let values = values.as_any().downcast_ref::<Decimal128Array>().unwrap();
                let filtered = filter_primitive(values, predicate);
                Ok(Arc::new(filtered.with_precision_and_scale(*p, *s).unwrap()))
            }
            DataType::Decimal256(p, s) => {
                let values = values.as_any().downcast_ref::<Decimal256Array>().unwrap();
                let filtered = filter_primitive(values, predicate);
                Ok(Arc::new(filtered.with_precision_and_scale(*p, *s).unwrap()))
            }
            DataType::Boolean => {
                let values = values.as_any().downcast_ref::<BooleanArray>().unwrap();
                Ok(Arc::new(filter_boolean(values, predicate)))
            }
            DataType::Utf8 => {
                Ok(Arc::new(filter_bytes(as_string_array(values), predicate)))
            }
            DataType::LargeUtf8 => {
                Ok(Arc::new(filter_bytes(as_largestring_array(values), predicate)))
            }
            DataType::Binary => {
                Ok(Arc::new(filter_bytes(as_generic_binary_array::<i32>(values), predicate)))
            }
            DataType::LargeBinary => {
                Ok(Arc::new(filter_bytes(as_generic_binary_array::<i64>(values), predicate)))
            }
            DataType::Dictionary(_, _) => downcast_dictionary_array! {
                values => Ok(Arc::new(filter_dict(values, predicate))),
                t => unimplemented!("Filter not supported for dictionary type {:?}", t)
            }
            _ => {
                // fallback to using MutableArrayData
                let mut mutable = MutableArrayData::new(
                    vec![values.data_ref()],
                    false,
                    predicate.count,
                );

                match &predicate.strategy {
                    IterationStrategy::Slices(slices) => {
                        slices
                            .iter()
                            .for_each(|(start, end)| mutable.extend(0, *start, *end));
                    }
                    _ => {
                        let iter = SlicesIterator::new(&predicate.filter);
                        iter.for_each(|(start, end)| mutable.extend(0, start, end));
                    }
                }

                let data = mutable.freeze();
                Ok(make_array(data))
            }
        },
    }
}

/// Computes a new null mask for `data` based on `predicate`
///
/// If the predicate selected no null-rows, returns `None`, otherwise returns
/// `Some((null_count, null_buffer))` where `null_count` is the number of nulls
/// in the filtered output, and `null_buffer` is the filtered null buffer
///
fn filter_null_mask(
    data: &ArrayData,
    predicate: &FilterPredicate,
) -> Option<(usize, Buffer)> {
    if data.null_count() == 0 {
        return None;
    }

    let nulls = filter_bits(data.null_buffer()?, data.offset(), predicate);
    // The filtered `nulls` has a length of `predicate.count` bits and
    // therefore the null count is this minus the number of valid bits
    let null_count = predicate.count - nulls.count_set_bits_offset(0, predicate.count);

    if null_count == 0 {
        return None;
    }

    Some((null_count, nulls))
}

/// Filter the packed bitmask `buffer`, with `predicate` starting at bit offset `offset`
fn filter_bits(buffer: &Buffer, offset: usize, predicate: &FilterPredicate) -> Buffer {
    let src = buffer.as_slice();

    match &predicate.strategy {
        IterationStrategy::IndexIterator => {
            let bits = IndexIterator::new(&predicate.filter, predicate.count)
                .map(|src_idx| bit_util::get_bit(src, src_idx + offset));

            // SAFETY: `IndexIterator` reports its size correctly
            unsafe { MutableBuffer::from_trusted_len_iter_bool(bits).into() }
        }
        IterationStrategy::Indices(indices) => {
            let bits = indices
                .iter()
                .map(|src_idx| bit_util::get_bit(src, *src_idx + offset));

            // SAFETY: `Vec::iter()` reports its size correctly
            unsafe { MutableBuffer::from_trusted_len_iter_bool(bits).into() }
        }
        IterationStrategy::SlicesIterator => {
            let mut builder =
                BooleanBufferBuilder::new(bit_util::ceil(predicate.count, 8));
            for (start, end) in SlicesIterator::new(&predicate.filter) {
                builder.append_packed_range(start + offset..end + offset, src)
            }
            builder.finish()
        }
        IterationStrategy::Slices(slices) => {
            let mut builder =
                BooleanBufferBuilder::new(bit_util::ceil(predicate.count, 8));
            for (start, end) in slices {
                builder.append_packed_range(*start + offset..*end + offset, src)
            }
            builder.finish()
        }
        IterationStrategy::All | IterationStrategy::None => unreachable!(),
    }
}

/// `filter` implementation for boolean buffers
fn filter_boolean(values: &BooleanArray, predicate: &FilterPredicate) -> BooleanArray {
    let data = values.data();
    assert_eq!(data.buffers().len(), 1);
    assert_eq!(data.child_data().len(), 0);

    let values = filter_bits(&data.buffers()[0], data.offset(), predicate);

    let mut builder = ArrayDataBuilder::new(DataType::Boolean)
        .len(predicate.count)
        .add_buffer(values);

    if let Some((null_count, nulls)) = filter_null_mask(data, predicate) {
        builder = builder.null_count(null_count).null_bit_buffer(Some(nulls));
    }

    let data = unsafe { builder.build_unchecked() };
    BooleanArray::from(data)
}

/// `filter` implementation for primitive arrays
fn filter_primitive<T>(
    values: &PrimitiveArray<T>,
    predicate: &FilterPredicate,
) -> PrimitiveArray<T>
where
    T: ArrowPrimitiveType,
{
    let data = values.data();
    assert_eq!(data.buffers().len(), 1);
    assert_eq!(data.child_data().len(), 0);

    let values = data.buffer::<T::Native>(0);
    assert!(values.len() >= predicate.filter.len());

    let buffer = match &predicate.strategy {
        IterationStrategy::SlicesIterator => {
            let mut buffer =
                MutableBuffer::with_capacity(predicate.count * T::get_byte_width());
            for (start, end) in SlicesIterator::new(&predicate.filter) {
                buffer.extend_from_slice(&values[start..end]);
            }
            buffer
        }
        IterationStrategy::Slices(slices) => {
            let mut buffer =
                MutableBuffer::with_capacity(predicate.count * T::get_byte_width());
            for (start, end) in slices {
                buffer.extend_from_slice(&values[*start..*end]);
            }
            buffer
        }
        IterationStrategy::IndexIterator => {
            let iter =
                IndexIterator::new(&predicate.filter, predicate.count).map(|x| values[x]);

            // SAFETY: IndexIterator is trusted length
            unsafe { MutableBuffer::from_trusted_len_iter(iter) }
        }
        IterationStrategy::Indices(indices) => {
            let iter = indices.iter().map(|x| values[*x]);

            // SAFETY: `Vec::iter` is trusted length
            unsafe { MutableBuffer::from_trusted_len_iter(iter) }
        }
        IterationStrategy::All | IterationStrategy::None => unreachable!(),
    };

    let mut builder = ArrayDataBuilder::new(data.data_type().clone())
        .len(predicate.count)
        .add_buffer(buffer.into());

    if let Some((null_count, nulls)) = filter_null_mask(data, predicate) {
        builder = builder.null_count(null_count).null_bit_buffer(Some(nulls));
    }

    let data = unsafe { builder.build_unchecked() };
    PrimitiveArray::from(data)
}

/// [`FilterBytes`] is created from a source [`GenericByteArray`] and can be
/// used to build a new [`GenericByteArray`] by copying values from the source
///
/// TODO(raphael): Could this be used for the take kernel as well?
struct FilterBytes<'a, OffsetSize> {
    src_offsets: &'a [OffsetSize],
    src_values: &'a [u8],
    dst_offsets: MutableBuffer,
    dst_values: MutableBuffer,
    cur_offset: OffsetSize,
}

impl<'a, OffsetSize> FilterBytes<'a, OffsetSize>
where
    OffsetSize: OffsetSizeTrait,
{
    fn new<T>(capacity: usize, array: &'a GenericByteArray<T>) -> Self
    where
        T: ByteArrayType<Offset = OffsetSize>,
    {
        let num_offsets_bytes = (capacity + 1) * std::mem::size_of::<OffsetSize>();
        let mut dst_offsets = MutableBuffer::new(num_offsets_bytes);
        let dst_values = MutableBuffer::new(0);
        let cur_offset = OffsetSize::from_usize(0).unwrap();
        dst_offsets.push(cur_offset);

        Self {
            src_offsets: array.value_offsets(),
            src_values: &array.data().buffers()[1],
            dst_offsets,
            dst_values,
            cur_offset,
        }
    }

    /// Returns the byte offset at `idx`
    #[inline]
    fn get_value_offset(&self, idx: usize) -> usize {
        self.src_offsets[idx].as_usize()
    }

    /// Returns the start and end of the value at index `idx` along with its length
    #[inline]
    fn get_value_range(&self, idx: usize) -> (usize, usize, OffsetSize) {
        // These can only fail if `array` contains invalid data
        let start = self.get_value_offset(idx);
        let end = self.get_value_offset(idx + 1);
        let len = OffsetSize::from_usize(end - start).expect("illegal offset range");
        (start, end, len)
    }

    /// Extends the in-progress array by the indexes in the provided iterator
    fn extend_idx(&mut self, iter: impl Iterator<Item = usize>) {
        for idx in iter {
            let (start, end, len) = self.get_value_range(idx);
            self.cur_offset += len;
            self.dst_offsets.push(self.cur_offset);
            self.dst_values
                .extend_from_slice(&self.src_values[start..end]);
        }
    }

    /// Extends the in-progress array by the ranges in the provided iterator
    fn extend_slices(&mut self, iter: impl Iterator<Item = (usize, usize)>) {
        for (start, end) in iter {
            // These can only fail if `array` contains invalid data
            for idx in start..end {
                let (_, _, len) = self.get_value_range(idx);
                self.cur_offset += len;
                self.dst_offsets.push(self.cur_offset); // push_unchecked?
            }

            let value_start = self.get_value_offset(start);
            let value_end = self.get_value_offset(end);
            self.dst_values
                .extend_from_slice(&self.src_values[value_start..value_end]);
        }
    }
}

/// `filter` implementation for byte arrays
///
/// Note: NULLs with a non-zero slot length in `array` will have the corresponding
/// data copied across. This allows handling the null mask separately from the data
fn filter_bytes<T>(
    array: &GenericByteArray<T>,
    predicate: &FilterPredicate,
) -> GenericByteArray<T>
where
    T: ByteArrayType,
{
    let data = array.data();
    assert_eq!(data.buffers().len(), 2);
    assert_eq!(data.child_data().len(), 0);
    let mut filter = FilterBytes::new(predicate.count, array);

    match &predicate.strategy {
        IterationStrategy::SlicesIterator => {
            filter.extend_slices(SlicesIterator::new(&predicate.filter))
        }
        IterationStrategy::Slices(slices) => filter.extend_slices(slices.iter().cloned()),
        IterationStrategy::IndexIterator => {
            filter.extend_idx(IndexIterator::new(&predicate.filter, predicate.count))
        }
        IterationStrategy::Indices(indices) => filter.extend_idx(indices.iter().cloned()),
        IterationStrategy::All | IterationStrategy::None => unreachable!(),
    }

    let mut builder = ArrayDataBuilder::new(T::DATA_TYPE)
        .len(predicate.count)
        .add_buffer(filter.dst_offsets.into())
        .add_buffer(filter.dst_values.into());

    if let Some((null_count, nulls)) = filter_null_mask(data, predicate) {
        builder = builder.null_count(null_count).null_bit_buffer(Some(nulls));
    }

    let data = unsafe { builder.build_unchecked() };
    GenericByteArray::from(data)
}

/// `filter` implementation for dictionaries
fn filter_dict<T>(
    array: &DictionaryArray<T>,
    predicate: &FilterPredicate,
) -> DictionaryArray<T>
where
    T: ArrowPrimitiveType,
    T::Native: num::Num,
{
    let builder = filter_primitive::<T>(array.keys(), predicate)
        .into_data()
        .into_builder()
        .data_type(array.data_type().clone())
        .child_data(array.data().child_data().to_vec());

    // SAFETY:
    // Keys were valid before, filtered subset is therefore still valid
    DictionaryArray::from(unsafe { builder.build_unchecked() })
}

#[cfg(test)]
mod tests {
    use arrow_array::builder::*;
    use arrow_array::types::*;
    use rand::distributions::{Alphanumeric, Standard};
    use rand::prelude::*;

    use super::*;

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
        TimestampSecondArray::from(vec![1, 2, 3, 4])
    );
    def_temporal_test!(
        test_filter_timestamp_millisecond,
        TimestampMillisecondArray,
        TimestampMillisecondArray::from(vec![1, 2, 3, 4])
    );
    def_temporal_test!(
        test_filter_timestamp_microsecond,
        TimestampMicrosecondArray,
        TimestampMicrosecondArray::from(vec![1, 2, 3, 4])
    );
    def_temporal_test!(
        test_filter_timestamp_nanosecond,
        TimestampNanosecondArray,
        TimestampNanosecondArray::from(vec![1, 2, 3, 4])
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
    fn test_filter_list_array() {
        let value_data = ArrayData::builder(DataType::Int32)
            .len(8)
            .add_buffer(Buffer::from_slice_ref([0, 1, 2, 3, 4, 5, 6, 7]))
            .build()
            .unwrap();

        let value_offsets = Buffer::from_slice_ref([0i64, 3, 6, 8, 8]);

        let list_data_type =
            DataType::LargeList(Box::new(Field::new("item", DataType::Int32, false)));
        let list_data = ArrayData::builder(list_data_type)
            .len(4)
            .add_buffer(value_offsets)
            .add_child_data(value_data)
            .null_bit_buffer(Some(Buffer::from([0b00000111])))
            .build()
            .unwrap();

        //  a = [[0, 1, 2], [3, 4, 5], [6, 7], null]
        let a = LargeListArray::from(list_data);
        let b = BooleanArray::from(vec![false, true, false, true]);
        let result = filter(&a, &b).unwrap();

        // expected: [[3, 4, 5], null]
        let value_data = ArrayData::builder(DataType::Int32)
            .len(3)
            .add_buffer(Buffer::from_slice_ref([3, 4, 5]))
            .build()
            .unwrap();

        let value_offsets = Buffer::from_slice_ref([0i64, 3, 3]);

        let list_data_type =
            DataType::LargeList(Box::new(Field::new("item", DataType::Int32, false)));
        let expected = ArrayData::builder(list_data_type)
            .len(2)
            .add_buffer(value_offsets)
            .add_child_data(value_data)
            .null_bit_buffer(Some(Buffer::from([0b00000001])))
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
    fn test_null_mask() {
        let a = Int64Array::from(vec![Some(1), Some(2), None]);

        let mask1 = BooleanArray::from(vec![Some(true), Some(true), None]);
        let out = filter(&a, &mask1).unwrap();
        assert_eq!(&out, &a.slice(0, 2));
    }

    #[test]
    fn test_fast_path() {
        let a: PrimitiveArray<Int64Type> =
            PrimitiveArray::from(vec![Some(1), Some(2), None]);

        // all true
        let mask = BooleanArray::from(vec![true, true, true]);
        let out = filter(&a, &mask).unwrap();
        let b = out
            .as_any()
            .downcast_ref::<PrimitiveArray<Int64Type>>()
            .unwrap();
        assert_eq!(&a, b);

        // all false
        let mask = BooleanArray::from(vec![false, false, false]);
        let out = filter(&a, &mask).unwrap();
        assert_eq!(out.len(), 0);
        assert_eq!(out.data_type(), &DataType::Int64);
    }

    #[test]
    fn test_slices() {
        // takes up 2 u64s
        let bools = std::iter::repeat(true)
            .take(10)
            .chain(std::iter::repeat(false).take(30))
            .chain(std::iter::repeat(true).take(20))
            .chain(std::iter::repeat(false).take(17))
            .chain(std::iter::repeat(true).take(4));

        let bool_array: BooleanArray = bools.map(Some).collect();

        let slices: Vec<_> = SlicesIterator::new(&bool_array).collect();
        let expected = vec![(0, 10), (40, 60), (77, 81)];
        assert_eq!(slices, expected);

        // slice with offset and truncated len
        let len = bool_array.len();
        let sliced_array = bool_array.slice(7, len - 10);
        let sliced_array = sliced_array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let slices: Vec<_> = SlicesIterator::new(sliced_array).collect();
        let expected = vec![(0, 3), (33, 53), (70, 71)];
        assert_eq!(slices, expected);
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

        let filter = BooleanArray::from(data);

        let slice_bits: Vec<_> = SlicesIterator::new(&filter)
            .flat_map(|(start, end)| start..end)
            .collect();

        let count = filter_count(&filter);
        let index_bits: Vec<_> = IndexIterator::new(&filter, count).collect();

        let expected_bits: Vec<_> = bools
            .iter()
            .skip(offset)
            .take(truncated_length)
            .enumerate()
            .flat_map(|(idx, v)| v.then(|| idx))
            .collect();

        assert_eq!(slice_bits, expected_bits);
        assert_eq!(index_bits, expected_bits);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
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

    /// Filters `values` by `predicate` using standard rust iterators
    fn filter_rust<T>(values: impl IntoIterator<Item = T>, predicate: &[bool]) -> Vec<T> {
        values
            .into_iter()
            .zip(predicate)
            .filter(|(_, x)| **x)
            .map(|(a, _)| a)
            .collect()
    }

    /// Generates an array of length `len` with `valid_percent` non-null values
    fn gen_primitive<T>(len: usize, valid_percent: f64) -> Vec<Option<T>>
    where
        Standard: Distribution<T>,
    {
        let mut rng = thread_rng();
        (0..len)
            .map(|_| rng.gen_bool(valid_percent).then(|| rng.gen()))
            .collect()
    }

    /// Generates an array of length `len` with `valid_percent` non-null values
    fn gen_strings(
        len: usize,
        valid_percent: f64,
        str_len_range: std::ops::Range<usize>,
    ) -> Vec<Option<String>> {
        let mut rng = thread_rng();
        (0..len)
            .map(|_| {
                rng.gen_bool(valid_percent).then(|| {
                    let len = rng.gen_range(str_len_range.clone());
                    (0..len)
                        .map(|_| char::from(rng.sample(Alphanumeric)))
                        .collect()
                })
            })
            .collect()
    }

    /// Returns an iterator that calls `Option::as_deref` on each item
    fn as_deref<T: std::ops::Deref>(
        src: &[Option<T>],
    ) -> impl Iterator<Item = Option<&T::Target>> {
        src.iter().map(|x| x.as_deref())
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn fuzz_filter() {
        let mut rng = thread_rng();

        for i in 0..100 {
            let filter_percent = match i {
                0..=4 => 1.,
                5..=10 => 0.,
                _ => rng.gen_range(0.0..1.0),
            };

            let valid_percent = rng.gen_range(0.0..1.0);

            let array_len = rng.gen_range(32..256);
            let array_offset = rng.gen_range(0..10);

            // Construct a predicate
            let filter_offset = rng.gen_range(0..10);
            let filter_truncate = rng.gen_range(0..10);
            let bools: Vec<_> = std::iter::from_fn(|| Some(rng.gen_bool(filter_percent)))
                .take(array_len + filter_offset - filter_truncate)
                .collect();

            let predicate = BooleanArray::from_iter(bools.iter().cloned().map(Some));

            // Offset predicate
            let predicate = predicate.slice(filter_offset, array_len - filter_truncate);
            let predicate = predicate.as_any().downcast_ref::<BooleanArray>().unwrap();
            let bools = &bools[filter_offset..];

            // Test i32
            let values = gen_primitive(array_len + array_offset, valid_percent);
            let src = Int32Array::from_iter(values.iter().cloned());

            let src = src.slice(array_offset, array_len);
            let src = src.as_any().downcast_ref::<Int32Array>().unwrap();
            let values = &values[array_offset..];

            let filtered = filter(src, predicate).unwrap();
            let array = filtered.as_any().downcast_ref::<Int32Array>().unwrap();
            let actual: Vec<_> = array.iter().collect();

            assert_eq!(actual, filter_rust(values.iter().cloned(), bools));

            // Test string
            let strings = gen_strings(array_len + array_offset, valid_percent, 0..20);
            let src = StringArray::from_iter(as_deref(&strings));

            let src = src.slice(array_offset, array_len);
            let src = src.as_any().downcast_ref::<StringArray>().unwrap();

            let filtered = filter(src, predicate).unwrap();
            let array = filtered.as_any().downcast_ref::<StringArray>().unwrap();
            let actual: Vec<_> = array.iter().collect();

            let expected_strings = filter_rust(as_deref(&strings[array_offset..]), bools);
            assert_eq!(actual, expected_strings);

            // Test string dictionary
            let src = DictionaryArray::<Int32Type>::from_iter(as_deref(&strings));

            let src = src.slice(array_offset, array_len);
            let src = src
                .as_any()
                .downcast_ref::<DictionaryArray<Int32Type>>()
                .unwrap();

            let filtered = filter(src, predicate).unwrap();

            let array = filtered
                .as_any()
                .downcast_ref::<DictionaryArray<Int32Type>>()
                .unwrap();

            let values = array
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();

            let actual: Vec<_> = array
                .keys()
                .iter()
                .map(|key| key.map(|key| values.value(key as usize)))
                .collect();

            assert_eq!(actual, expected_strings);
        }
    }

    #[test]
    fn test_filter_map() {
        let mut builder =
            MapBuilder::new(None, StringBuilder::new(), Int64Builder::with_capacity(4));
        // [{"key1": 1}, {"key2": 2, "key3": 3}, null, {"key1": 1}
        builder.keys().append_value("key1");
        builder.values().append_value(1);
        builder.append(true).unwrap();
        builder.keys().append_value("key2");
        builder.keys().append_value("key3");
        builder.values().append_value(2);
        builder.values().append_value(3);
        builder.append(true).unwrap();
        builder.append(false).unwrap();
        builder.keys().append_value("key1");
        builder.values().append_value(1);
        builder.append(true).unwrap();
        let maparray = Arc::new(builder.finish()) as ArrayRef;

        let indices = vec![Some(true), Some(false), Some(false), Some(true)]
            .into_iter()
            .collect::<BooleanArray>();
        let got = filter(&maparray, &indices).unwrap();

        let mut builder =
            MapBuilder::new(None, StringBuilder::new(), Int64Builder::with_capacity(2));
        builder.keys().append_value("key1");
        builder.values().append_value(1);
        builder.append(true).unwrap();
        builder.keys().append_value("key1");
        builder.values().append_value(1);
        builder.append(true).unwrap();
        let expected = Arc::new(builder.finish()) as ArrayRef;

        assert_eq!(&expected, &got);
    }

    #[test]
    fn test_filter_fixed_size_list_arrays() {
        let value_data = ArrayData::builder(DataType::Int32)
            .len(9)
            .add_buffer(Buffer::from_slice_ref([0, 1, 2, 3, 4, 5, 6, 7, 8]))
            .build()
            .unwrap();
        let list_data_type = DataType::FixedSizeList(
            Box::new(Field::new("item", DataType::Int32, false)),
            3,
        );
        let list_data = ArrayData::builder(list_data_type)
            .len(3)
            .add_child_data(value_data)
            .build()
            .unwrap();
        let array = FixedSizeListArray::from(list_data);

        let filter_array = BooleanArray::from(vec![true, false, false]);

        let c = filter(&array, &filter_array).unwrap();
        let filtered = c.as_any().downcast_ref::<FixedSizeListArray>().unwrap();

        assert_eq!(filtered.len(), 1);

        let list = filtered.value(0);
        assert_eq!(
            &[0, 1, 2],
            list.as_any().downcast_ref::<Int32Array>().unwrap().values()
        );

        let filter_array = BooleanArray::from(vec![true, false, true]);

        let c = filter(&array, &filter_array).unwrap();
        let filtered = c.as_any().downcast_ref::<FixedSizeListArray>().unwrap();

        assert_eq!(filtered.len(), 2);

        let list = filtered.value(0);
        assert_eq!(
            &[0, 1, 2],
            list.as_any().downcast_ref::<Int32Array>().unwrap().values()
        );
        let list = filtered.value(1);
        assert_eq!(
            &[6, 7, 8],
            list.as_any().downcast_ref::<Int32Array>().unwrap().values()
        );
    }

    #[test]
    fn test_filter_fixed_size_list_arrays_with_null() {
        let value_data = ArrayData::builder(DataType::Int32)
            .len(10)
            .add_buffer(Buffer::from_slice_ref([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]))
            .build()
            .unwrap();

        // Set null buts for the nested array:
        //  [[0, 1], null, null, [6, 7], [8, 9]]
        // 01011001 00000001
        let mut null_bits: [u8; 1] = [0; 1];
        bit_util::set_bit(&mut null_bits, 0);
        bit_util::set_bit(&mut null_bits, 3);
        bit_util::set_bit(&mut null_bits, 4);

        let list_data_type = DataType::FixedSizeList(
            Box::new(Field::new("item", DataType::Int32, false)),
            2,
        );
        let list_data = ArrayData::builder(list_data_type)
            .len(5)
            .add_child_data(value_data)
            .null_bit_buffer(Some(Buffer::from(null_bits)))
            .build()
            .unwrap();
        let array = FixedSizeListArray::from(list_data);

        let filter_array = BooleanArray::from(vec![true, true, false, true, false]);

        let c = filter(&array, &filter_array).unwrap();
        let filtered = c.as_any().downcast_ref::<FixedSizeListArray>().unwrap();

        assert_eq!(filtered.len(), 3);

        let list = filtered.value(0);
        assert_eq!(
            &[0, 1],
            list.as_any().downcast_ref::<Int32Array>().unwrap().values()
        );
        assert!(filtered.is_null(1));
        let list = filtered.value(2);
        assert_eq!(
            &[6, 7],
            list.as_any().downcast_ref::<Int32Array>().unwrap().values()
        );
    }

    fn test_filter_union_array(array: UnionArray) {
        let filter_array = BooleanArray::from(vec![true, false, false]);
        let c = filter(&array, &filter_array).unwrap();
        let filtered = c.as_any().downcast_ref::<UnionArray>().unwrap();

        let mut builder = UnionBuilder::new_dense();
        builder.append::<Int32Type>("A", 1).unwrap();
        let expected_array = builder.build().unwrap();

        compare_union_arrays(filtered, &expected_array);

        let filter_array = BooleanArray::from(vec![true, false, true]);
        let c = filter(&array, &filter_array).unwrap();
        let filtered = c.as_any().downcast_ref::<UnionArray>().unwrap();

        let mut builder = UnionBuilder::new_dense();
        builder.append::<Int32Type>("A", 1).unwrap();
        builder.append::<Int32Type>("A", 34).unwrap();
        let expected_array = builder.build().unwrap();

        compare_union_arrays(filtered, &expected_array);

        let filter_array = BooleanArray::from(vec![true, true, false]);
        let c = filter(&array, &filter_array).unwrap();
        let filtered = c.as_any().downcast_ref::<UnionArray>().unwrap();

        let mut builder = UnionBuilder::new_dense();
        builder.append::<Int32Type>("A", 1).unwrap();
        builder.append::<Float64Type>("B", 3.2).unwrap();
        let expected_array = builder.build().unwrap();

        compare_union_arrays(filtered, &expected_array);
    }

    #[test]
    fn test_filter_union_array_dense() {
        let mut builder = UnionBuilder::new_dense();
        builder.append::<Int32Type>("A", 1).unwrap();
        builder.append::<Float64Type>("B", 3.2).unwrap();
        builder.append::<Int32Type>("A", 34).unwrap();
        let array = builder.build().unwrap();

        test_filter_union_array(array);
    }

    #[test]
    fn test_filter_run_union_array_dense() {
        let mut builder = UnionBuilder::new_dense();
        builder.append::<Int32Type>("A", 1).unwrap();
        builder.append::<Int32Type>("A", 3).unwrap();
        builder.append::<Int32Type>("A", 34).unwrap();
        let array = builder.build().unwrap();

        let filter_array = BooleanArray::from(vec![true, true, false]);
        let c = filter(&array, &filter_array).unwrap();
        let filtered = c.as_any().downcast_ref::<UnionArray>().unwrap();

        let mut builder = UnionBuilder::new_dense();
        builder.append::<Int32Type>("A", 1).unwrap();
        builder.append::<Int32Type>("A", 3).unwrap();
        let expected = builder.build().unwrap();

        assert_eq!(filtered.data(), expected.data());
    }

    #[test]
    fn test_filter_union_array_dense_with_nulls() {
        let mut builder = UnionBuilder::new_dense();
        builder.append::<Int32Type>("A", 1).unwrap();
        builder.append::<Float64Type>("B", 3.2).unwrap();
        builder.append_null::<Float64Type>("B").unwrap();
        builder.append::<Int32Type>("A", 34).unwrap();
        let array = builder.build().unwrap();

        let filter_array = BooleanArray::from(vec![true, true, false, false]);
        let c = filter(&array, &filter_array).unwrap();
        let filtered = c.as_any().downcast_ref::<UnionArray>().unwrap();

        let mut builder = UnionBuilder::new_dense();
        builder.append::<Int32Type>("A", 1).unwrap();
        builder.append::<Float64Type>("B", 3.2).unwrap();
        let expected_array = builder.build().unwrap();

        compare_union_arrays(filtered, &expected_array);

        let filter_array = BooleanArray::from(vec![true, false, true, false]);
        let c = filter(&array, &filter_array).unwrap();
        let filtered = c.as_any().downcast_ref::<UnionArray>().unwrap();

        let mut builder = UnionBuilder::new_dense();
        builder.append::<Int32Type>("A", 1).unwrap();
        builder.append_null::<Float64Type>("B").unwrap();
        let expected_array = builder.build().unwrap();

        compare_union_arrays(filtered, &expected_array);
    }

    #[test]
    fn test_filter_union_array_sparse() {
        let mut builder = UnionBuilder::new_sparse();
        builder.append::<Int32Type>("A", 1).unwrap();
        builder.append::<Float64Type>("B", 3.2).unwrap();
        builder.append::<Int32Type>("A", 34).unwrap();
        let array = builder.build().unwrap();

        test_filter_union_array(array);
    }

    #[test]
    fn test_filter_union_array_sparse_with_nulls() {
        let mut builder = UnionBuilder::new_sparse();
        builder.append::<Int32Type>("A", 1).unwrap();
        builder.append::<Float64Type>("B", 3.2).unwrap();
        builder.append_null::<Float64Type>("B").unwrap();
        builder.append::<Int32Type>("A", 34).unwrap();
        let array = builder.build().unwrap();

        let filter_array = BooleanArray::from(vec![true, false, true, false]);
        let c = filter(&array, &filter_array).unwrap();
        let filtered = c.as_any().downcast_ref::<UnionArray>().unwrap();

        let mut builder = UnionBuilder::new_sparse();
        builder.append::<Int32Type>("A", 1).unwrap();
        builder.append_null::<Float64Type>("B").unwrap();
        let expected_array = builder.build().unwrap();

        compare_union_arrays(filtered, &expected_array);
    }

    fn compare_union_arrays(union1: &UnionArray, union2: &UnionArray) {
        assert_eq!(union1.len(), union2.len());

        for i in 0..union1.len() {
            let type_id = union1.type_id(i);

            let slot1 = union1.value(i);
            let slot2 = union2.value(i);

            assert_eq!(slot1.is_null(0), slot2.is_null(0));

            if !slot1.is_null(0) && !slot2.is_null(0) {
                match type_id {
                    0 => {
                        let slot1 = slot1.as_any().downcast_ref::<Int32Array>().unwrap();
                        assert_eq!(slot1.len(), 1);
                        let value1 = slot1.value(0);

                        let slot2 = slot2.as_any().downcast_ref::<Int32Array>().unwrap();
                        assert_eq!(slot2.len(), 1);
                        let value2 = slot2.value(0);
                        assert_eq!(value1, value2);
                    }
                    1 => {
                        let slot1 =
                            slot1.as_any().downcast_ref::<Float64Array>().unwrap();
                        assert_eq!(slot1.len(), 1);
                        let value1 = slot1.value(0);

                        let slot2 =
                            slot2.as_any().downcast_ref::<Float64Array>().unwrap();
                        assert_eq!(slot2.len(), 1);
                        let value2 = slot2.value(0);
                        assert_eq!(value1, value2);
                    }
                    _ => unreachable!(),
                }
            }
        }
    }
}
