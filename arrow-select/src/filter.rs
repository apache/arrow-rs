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

use std::ops::AddAssign;
use std::sync::Arc;

use arrow_array::builder::BooleanBufferBuilder;
use arrow_array::cast::AsArray;
use arrow_array::types::{
    ArrowDictionaryKeyType, ArrowPrimitiveType, ByteArrayType, ByteViewType, RunEndIndexType,
};
use arrow_array::*;
use arrow_buffer::{bit_util, ArrowNativeType, BooleanBuffer, NullBuffer, RunEndBuffer};
use arrow_buffer::{Buffer, MutableBuffer};
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
/// `[start, end)` whose slots of a bitmap [Buffer] are true.
///
/// Each interval corresponds to a contiguous region of memory to be
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
    /// Creates a new iterator from a [BooleanArray]
    pub fn new(filter: &'a BooleanArray) -> Self {
        Self(filter.values().set_slices())
    }
}

impl Iterator for SlicesIterator<'_> {
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
        let iter = filter.values().set_indices();
        Self { remaining, iter }
    }
}

impl Iterator for IndexIterator<'_> {
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
    filter.values().count_set_bits()
}

/// Function that can filter arbitrary arrays
///
/// Deprecated: Use [`FilterPredicate`] instead
#[deprecated]
pub type Filter<'a> = Box<dyn Fn(&ArrayData) -> ArrayData + 'a>;

/// Returns a prepared function optimized to filter multiple arrays.
///
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
    let nulls = filter.nulls().unwrap();
    let mask = filter.values() & nulls.inner();
    BooleanArray::new(mask, None)
}

/// Returns a filtered `values` [Array] where the corresponding elements of
/// `predicate` are `true`.
///
/// See also [`FilterBuilder`] for more control over the filtering process.
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
pub fn filter(values: &dyn Array, predicate: &BooleanArray) -> Result<ArrayRef, ArrowError> {
    let mut filter_builder = FilterBuilder::new(predicate);

    if multiple_arrays(values.data_type()) {
        // Only optimize if filtering more than one array
        // Otherwise, the overhead of optimization can be more than the benefit
        filter_builder = filter_builder.optimize();
    }

    let predicate = filter_builder.build();

    filter_array(values, &predicate)
}

fn multiple_arrays(data_type: &DataType) -> bool {
    match data_type {
        DataType::Struct(fields) => {
            fields.len() > 1 || fields.len() == 1 && multiple_arrays(fields[0].data_type())
        }
        DataType::Union(fields, UnionMode::Sparse) => !fields.is_empty(),
        _ => false,
    }
}

/// Returns a filtered [RecordBatch] where the corresponding elements of
/// `predicate` are true.
///
/// This is the equivalent of calling [filter] on each column of the [RecordBatch].
pub fn filter_record_batch(
    record_batch: &RecordBatch,
    predicate: &BooleanArray,
) -> Result<RecordBatch, ArrowError> {
    let mut filter_builder = FilterBuilder::new(predicate);
    if record_batch.num_columns() > 1 {
        // Only optimize if filtering more than one column
        // Otherwise, the overhead of optimization can be more than the benefit
        filter_builder = filter_builder.optimize();
    }
    let filter = filter_builder.build();

    let filtered_arrays = record_batch
        .columns()
        .iter()
        .map(|a| filter_array(a, &filter))
        .collect::<Result<Vec<_>, _>>()?;
    let options = RecordBatchOptions::default().with_row_count(Some(filter.count()));
    RecordBatch::try_new_with_options(record_batch.schema(), filtered_arrays, &options)
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
            0 => filter.clone(),
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

    /// Number of rows being selected based on this [`FilterPredicate`]
    pub fn count(&self) -> usize {
        self.count
    }
}

fn filter_array(values: &dyn Array, predicate: &FilterPredicate) -> Result<ArrayRef, ArrowError> {
    if predicate.filter.len() > values.len() {
        return Err(ArrowError::InvalidArgumentError(format!(
            "Filter predicate of length {} is larger than target array of length {}",
            predicate.filter.len(),
            values.len()
        )));
    }

    match predicate.strategy {
        IterationStrategy::None => Ok(new_empty_array(values.data_type())),
        IterationStrategy::All => Ok(values.slice(0, predicate.count)),
        // actually filter
        _ => downcast_primitive_array! {
            values => Ok(Arc::new(filter_primitive(values, predicate))),
            DataType::Boolean => {
                let values = values.as_any().downcast_ref::<BooleanArray>().unwrap();
                Ok(Arc::new(filter_boolean(values, predicate)))
            }
            DataType::Utf8 => {
                Ok(Arc::new(filter_bytes(values.as_string::<i32>(), predicate)))
            }
            DataType::LargeUtf8 => {
                Ok(Arc::new(filter_bytes(values.as_string::<i64>(), predicate)))
            }
            DataType::Utf8View => {
                Ok(Arc::new(filter_byte_view(values.as_string_view(), predicate)))
            }
            DataType::Binary => {
                Ok(Arc::new(filter_bytes(values.as_binary::<i32>(), predicate)))
            }
            DataType::LargeBinary => {
                Ok(Arc::new(filter_bytes(values.as_binary::<i64>(), predicate)))
            }
            DataType::BinaryView => {
                Ok(Arc::new(filter_byte_view(values.as_binary_view(), predicate)))
            }
            DataType::FixedSizeBinary(_) => {
                Ok(Arc::new(filter_fixed_size_binary(values.as_fixed_size_binary(), predicate)))
            }
            DataType::RunEndEncoded(_, _) => {
                downcast_run_array!{
                    values => Ok(Arc::new(filter_run_end_array(values, predicate)?)),
                    t => unimplemented!("Filter not supported for RunEndEncoded type {:?}", t)
                }
            }
            DataType::Dictionary(_, _) => downcast_dictionary_array! {
                values => Ok(Arc::new(filter_dict(values, predicate))),
                t => unimplemented!("Filter not supported for dictionary type {:?}", t)
            }
            DataType::Struct(_) => {
                Ok(Arc::new(filter_struct(values.as_struct(), predicate)?))
            }
            DataType::Union(_, UnionMode::Sparse) => {
                Ok(Arc::new(filter_sparse_union(values.as_union(), predicate)?))
            }
            _ => {
                let data = values.to_data();
                // fallback to using MutableArrayData
                let mut mutable = MutableArrayData::new(
                    vec![&data],
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

/// Filter any supported [`RunArray`] based on a [`FilterPredicate`]
fn filter_run_end_array<R: RunEndIndexType>(
    re_arr: &RunArray<R>,
    pred: &FilterPredicate,
) -> Result<RunArray<R>, ArrowError>
where
    R::Native: Into<i64> + From<bool>,
    R::Native: AddAssign,
{
    let run_ends: &RunEndBuffer<R::Native> = re_arr.run_ends();
    let mut values_filter = BooleanBufferBuilder::new(run_ends.len());
    let mut new_run_ends = vec![R::default_value(); run_ends.len()];

    let mut start = 0i64;
    let mut i = 0;
    let mut count = R::default_value();
    let filter_values = pred.filter.values();

    for end in run_ends.inner().into_iter().map(|i| (*i).into()) {
        let mut keep = false;

        for pred in filter_values
            .iter()
            .skip(start as usize)
            .take((end - start) as usize)
        {
            count += R::Native::from(pred);
            keep |= pred
        }

        // this is to avoid branching
        new_run_ends[i] = count;
        i += keep as usize;

        values_filter.append(keep);
        start = end;
    }

    new_run_ends.truncate(i);

    if values_filter.is_empty() {
        new_run_ends.clear();
    }

    let values = re_arr.values();
    let pred = BooleanArray::new(values_filter.finish(), None);
    let values = filter(&values, &pred)?;

    let run_ends = PrimitiveArray::<R>::new(new_run_ends.into(), None);
    RunArray::try_new(&run_ends, &values)
}

/// Computes a new null mask for `data` based on `predicate`
///
/// If the predicate selected no null-rows, returns `None`, otherwise returns
/// `Some((null_count, null_buffer))` where `null_count` is the number of nulls
/// in the filtered output, and `null_buffer` is the filtered null buffer
///
fn filter_null_mask(
    nulls: Option<&NullBuffer>,
    predicate: &FilterPredicate,
) -> Option<(usize, Buffer)> {
    let nulls = nulls?;
    if nulls.null_count() == 0 {
        return None;
    }

    let nulls = filter_bits(nulls.inner(), predicate);
    // The filtered `nulls` has a length of `predicate.count` bits and
    // therefore the null count is this minus the number of valid bits
    let null_count = predicate.count - nulls.count_set_bits_offset(0, predicate.count);

    if null_count == 0 {
        return None;
    }

    Some((null_count, nulls))
}

/// Filter the packed bitmask `buffer`, with `predicate` starting at bit offset `offset`
fn filter_bits(buffer: &BooleanBuffer, predicate: &FilterPredicate) -> Buffer {
    let src = buffer.values();
    let offset = buffer.offset();

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
            let mut builder = BooleanBufferBuilder::new(bit_util::ceil(predicate.count, 8));
            for (start, end) in SlicesIterator::new(&predicate.filter) {
                builder.append_packed_range(start + offset..end + offset, src)
            }
            builder.into()
        }
        IterationStrategy::Slices(slices) => {
            let mut builder = BooleanBufferBuilder::new(bit_util::ceil(predicate.count, 8));
            for (start, end) in slices {
                builder.append_packed_range(*start + offset..*end + offset, src)
            }
            builder.into()
        }
        IterationStrategy::All | IterationStrategy::None => unreachable!(),
    }
}

/// `filter` implementation for boolean buffers
fn filter_boolean(array: &BooleanArray, predicate: &FilterPredicate) -> BooleanArray {
    let values = filter_bits(array.values(), predicate);

    let mut builder = ArrayDataBuilder::new(DataType::Boolean)
        .len(predicate.count)
        .add_buffer(values);

    if let Some((null_count, nulls)) = filter_null_mask(array.nulls(), predicate) {
        builder = builder.null_count(null_count).null_bit_buffer(Some(nulls));
    }

    let data = unsafe { builder.build_unchecked() };
    BooleanArray::from(data)
}

#[inline(never)]
fn filter_native<T: ArrowNativeType>(values: &[T], predicate: &FilterPredicate) -> Buffer {
    assert!(values.len() >= predicate.filter.len());

    let buffer = match &predicate.strategy {
        IterationStrategy::SlicesIterator => {
            let mut buffer = MutableBuffer::with_capacity(predicate.count * T::get_byte_width());
            for (start, end) in SlicesIterator::new(&predicate.filter) {
                buffer.extend_from_slice(&values[start..end]);
            }
            buffer
        }
        IterationStrategy::Slices(slices) => {
            let mut buffer = MutableBuffer::with_capacity(predicate.count * T::get_byte_width());
            for (start, end) in slices {
                buffer.extend_from_slice(&values[*start..*end]);
            }
            buffer
        }
        IterationStrategy::IndexIterator => {
            let iter = IndexIterator::new(&predicate.filter, predicate.count).map(|x| values[x]);

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

    buffer.into()
}

/// `filter` implementation for primitive arrays
fn filter_primitive<T>(array: &PrimitiveArray<T>, predicate: &FilterPredicate) -> PrimitiveArray<T>
where
    T: ArrowPrimitiveType,
{
    let values = array.values();
    let buffer = filter_native(values, predicate);
    let mut builder = ArrayDataBuilder::new(array.data_type().clone())
        .len(predicate.count)
        .add_buffer(buffer);

    if let Some((null_count, nulls)) = filter_null_mask(array.nulls(), predicate) {
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
            src_values: array.value_data(),
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
fn filter_bytes<T>(array: &GenericByteArray<T>, predicate: &FilterPredicate) -> GenericByteArray<T>
where
    T: ByteArrayType,
{
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

    if let Some((null_count, nulls)) = filter_null_mask(array.nulls(), predicate) {
        builder = builder.null_count(null_count).null_bit_buffer(Some(nulls));
    }

    let data = unsafe { builder.build_unchecked() };
    GenericByteArray::from(data)
}

/// `filter` implementation for byte view arrays.
fn filter_byte_view<T: ByteViewType>(
    array: &GenericByteViewArray<T>,
    predicate: &FilterPredicate,
) -> GenericByteViewArray<T> {
    let new_view_buffer = filter_native(array.views(), predicate);

    let mut builder = ArrayDataBuilder::new(T::DATA_TYPE)
        .len(predicate.count)
        .add_buffer(new_view_buffer)
        .add_buffers(array.data_buffers().to_vec());

    if let Some((null_count, nulls)) = filter_null_mask(array.nulls(), predicate) {
        builder = builder.null_count(null_count).null_bit_buffer(Some(nulls));
    }

    GenericByteViewArray::from(unsafe { builder.build_unchecked() })
}

fn filter_fixed_size_binary(
    array: &FixedSizeBinaryArray,
    predicate: &FilterPredicate,
) -> FixedSizeBinaryArray {
    let values: &[u8] = array.values();
    let value_length = array.value_length() as usize;
    let calculate_offset_from_index = |index: usize| index * value_length;
    let buffer = match &predicate.strategy {
        IterationStrategy::SlicesIterator => {
            let mut buffer = MutableBuffer::with_capacity(predicate.count * value_length);
            for (start, end) in SlicesIterator::new(&predicate.filter) {
                buffer.extend_from_slice(
                    &values[calculate_offset_from_index(start)..calculate_offset_from_index(end)],
                );
            }
            buffer
        }
        IterationStrategy::Slices(slices) => {
            let mut buffer = MutableBuffer::with_capacity(predicate.count * value_length);
            for (start, end) in slices {
                buffer.extend_from_slice(
                    &values[calculate_offset_from_index(*start)..calculate_offset_from_index(*end)],
                );
            }
            buffer
        }
        IterationStrategy::IndexIterator => {
            let iter = IndexIterator::new(&predicate.filter, predicate.count).map(|x| {
                &values[calculate_offset_from_index(x)..calculate_offset_from_index(x + 1)]
            });

            let mut buffer = MutableBuffer::new(predicate.count * value_length);
            iter.for_each(|item| buffer.extend_from_slice(item));
            buffer
        }
        IterationStrategy::Indices(indices) => {
            let iter = indices.iter().map(|x| {
                &values[calculate_offset_from_index(*x)..calculate_offset_from_index(*x + 1)]
            });

            let mut buffer = MutableBuffer::new(predicate.count * value_length);
            iter.for_each(|item| buffer.extend_from_slice(item));
            buffer
        }
        IterationStrategy::All | IterationStrategy::None => unreachable!(),
    };
    let mut builder = ArrayDataBuilder::new(array.data_type().clone())
        .len(predicate.count)
        .add_buffer(buffer.into());

    if let Some((null_count, nulls)) = filter_null_mask(array.nulls(), predicate) {
        builder = builder.null_count(null_count).null_bit_buffer(Some(nulls));
    }

    let data = unsafe { builder.build_unchecked() };
    FixedSizeBinaryArray::from(data)
}

/// `filter` implementation for dictionaries
fn filter_dict<T>(array: &DictionaryArray<T>, predicate: &FilterPredicate) -> DictionaryArray<T>
where
    T: ArrowDictionaryKeyType,
    T::Native: num::Num,
{
    let builder = filter_primitive::<T>(array.keys(), predicate)
        .into_data()
        .into_builder()
        .data_type(array.data_type().clone())
        .child_data(vec![array.values().to_data()]);

    // SAFETY:
    // Keys were valid before, filtered subset is therefore still valid
    DictionaryArray::from(unsafe { builder.build_unchecked() })
}

/// `filter` implementation for structs
fn filter_struct(
    array: &StructArray,
    predicate: &FilterPredicate,
) -> Result<StructArray, ArrowError> {
    let columns = array
        .columns()
        .iter()
        .map(|column| filter_array(column, predicate))
        .collect::<Result<_, _>>()?;

    let nulls = if let Some((null_count, nulls)) = filter_null_mask(array.nulls(), predicate) {
        let buffer = BooleanBuffer::new(nulls, 0, predicate.count);

        Some(unsafe { NullBuffer::new_unchecked(buffer, null_count) })
    } else {
        None
    };

    Ok(unsafe { StructArray::new_unchecked(array.fields().clone(), columns, nulls) })
}

/// `filter` implementation for sparse unions
fn filter_sparse_union(
    array: &UnionArray,
    predicate: &FilterPredicate,
) -> Result<UnionArray, ArrowError> {
    let DataType::Union(fields, UnionMode::Sparse) = array.data_type() else {
        unreachable!()
    };

    let type_ids = filter_primitive(&Int8Array::new(array.type_ids().clone(), None), predicate);

    let children = fields
        .iter()
        .map(|(child_type_id, _)| filter_array(array.child(child_type_id), predicate))
        .collect::<Result<_, _>>()?;

    Ok(unsafe {
        UnionArray::new_unchecked(fields.clone(), type_ids.into_parts().1, None, children)
    })
}

#[cfg(test)]
mod tests {
    use arrow_array::builder::*;
    use arrow_array::cast::as_run_array;
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
        let a = Int32Array::from(vec![5, 6, 7, 8, 9]).slice(1, 4);
        let b = BooleanArray::from(vec![true, false, false, true]);
        // filtering with sliced filter array is not currently supported
        // let b_slice = BooleanArray::from(vec![true, false, false, true, false]).slice(1, 4);
        // let b = b_slice.as_any().downcast_ref().unwrap();
        let c = filter(&a, &b).unwrap();
        let d = c.as_ref().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(2, d.len());
        assert_eq!(6, d.value(0));
        assert_eq!(9, d.value(1));
    }

    #[test]
    fn test_filter_array_low_density() {
        // this test exercises the all 0's branch of the filter algorithm
        let mut data_values = (1..=65).collect::<Vec<i32>>();
        let mut filter_values = (1..=65).map(|i| matches!(i % 65, 0)).collect::<Vec<bool>>();
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

    fn _test_filter_byte_view<T>()
    where
        T: ByteViewType,
        str: AsRef<T::Native>,
        T::Native: PartialEq,
    {
        let array = {
            // ["hello", "world", null, "large payload over 12 bytes", "lulu"]
            let mut builder = GenericByteViewBuilder::<T>::new();
            builder.append_value("hello");
            builder.append_value("world");
            builder.append_null();
            builder.append_value("large payload over 12 bytes");
            builder.append_value("lulu");
            builder.finish()
        };

        {
            let predicate = BooleanArray::from(vec![true, false, true, true, false]);
            let actual = filter(&array, &predicate).unwrap();

            assert_eq!(actual.len(), 3);

            let expected = {
                // ["hello", null, "large payload over 12 bytes"]
                let mut builder = GenericByteViewBuilder::<T>::new();
                builder.append_value("hello");
                builder.append_null();
                builder.append_value("large payload over 12 bytes");
                builder.finish()
            };

            assert_eq!(actual.as_ref(), &expected);
        }

        {
            let predicate = BooleanArray::from(vec![true, false, false, false, true]);
            let actual = filter(&array, &predicate).unwrap();

            assert_eq!(actual.len(), 2);

            let expected = {
                // ["hello", "lulu"]
                let mut builder = GenericByteViewBuilder::<T>::new();
                builder.append_value("hello");
                builder.append_value("lulu");
                builder.finish()
            };

            assert_eq!(actual.as_ref(), &expected);
        }
    }

    #[test]
    fn test_filter_string_view() {
        _test_filter_byte_view::<StringViewType>()
    }

    #[test]
    fn test_filter_binary_view() {
        _test_filter_byte_view::<BinaryViewType>()
    }

    #[test]
    fn test_filter_fixed_binary() {
        let v1 = [1_u8, 2];
        let v2 = [3_u8, 4];
        let v3 = [5_u8, 6];
        let v = vec![&v1, &v2, &v3];
        let a = FixedSizeBinaryArray::from(v);
        let b = BooleanArray::from(vec![true, false, true]);
        let c = filter(&a, &b).unwrap();
        let d = c
            .as_ref()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(d.len(), 2);
        assert_eq!(d.value(0), &v1);
        assert_eq!(d.value(1), &v3);
        let c2 = FilterBuilder::new(&b)
            .optimize()
            .build()
            .filter(&a)
            .unwrap();
        let d2 = c2
            .as_ref()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(d, d2);

        let b = BooleanArray::from(vec![false, false, false]);
        let c = filter(&a, &b).unwrap();
        let d = c
            .as_ref()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(d.len(), 0);

        let b = BooleanArray::from(vec![true, true, true]);
        let c = filter(&a, &b).unwrap();
        let d = c
            .as_ref()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(d.len(), 3);
        assert_eq!(d.value(0), &v1);
        assert_eq!(d.value(1), &v2);
        assert_eq!(d.value(2), &v3);

        let b = BooleanArray::from(vec![false, false, true]);
        let c = filter(&a, &b).unwrap();
        let d = c
            .as_ref()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(d.len(), 1);
        assert_eq!(d.value(0), &v3);
        let c2 = FilterBuilder::new(&b)
            .optimize()
            .build()
            .filter(&a)
            .unwrap();
        let d2 = c2
            .as_ref()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(d, d2);
    }

    #[test]
    fn test_filter_array_slice_with_null() {
        let a = Int32Array::from(vec![Some(5), None, Some(7), Some(8), Some(9)]).slice(1, 4);
        let b = BooleanArray::from(vec![true, false, false, true]);
        // filtering with sliced filter array is not currently supported
        // let b_slice = BooleanArray::from(vec![true, false, false, true, false]).slice(1, 4);
        // let b = b_slice.as_any().downcast_ref().unwrap();
        let c = filter(&a, &b).unwrap();
        let d = c.as_ref().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(2, d.len());
        assert!(d.is_null(0));
        assert!(!d.is_null(1));
        assert_eq!(9, d.value(1));
    }

    #[test]
    fn test_filter_run_end_encoding_array() {
        let run_ends = Int64Array::from(vec![2, 3, 8]);
        let values = Int64Array::from(vec![7, -2, 9]);
        let a = RunArray::try_new(&run_ends, &values).expect("Failed to create RunArray");
        let b = BooleanArray::from(vec![true, false, true, false, true, false, true, false]);
        let c = filter(&a, &b).unwrap();
        let actual: &RunArray<Int64Type> = as_run_array(&c);
        assert_eq!(4, actual.len());

        let expected = RunArray::try_new(
            &Int64Array::from(vec![1, 2, 4]),
            &Int64Array::from(vec![7, -2, 9]),
        )
        .expect("Failed to make expected RunArray test is broken");

        assert_eq!(&actual.run_ends().values(), &expected.run_ends().values());
        assert_eq!(actual.values(), expected.values())
    }

    #[test]
    fn test_filter_run_end_encoding_array_remove_value() {
        let run_ends = Int32Array::from(vec![2, 3, 8, 10]);
        let values = Int32Array::from(vec![7, -2, 9, -8]);
        let a = RunArray::try_new(&run_ends, &values).expect("Failed to create RunArray");
        let b = BooleanArray::from(vec![
            false, true, false, false, true, false, true, false, false, false,
        ]);
        let c = filter(&a, &b).unwrap();
        let actual: &RunArray<Int32Type> = as_run_array(&c);
        assert_eq!(3, actual.len());

        let expected =
            RunArray::try_new(&Int32Array::from(vec![1, 3]), &Int32Array::from(vec![7, 9]))
                .expect("Failed to make expected RunArray test is broken");

        assert_eq!(&actual.run_ends().values(), &expected.run_ends().values());
        assert_eq!(actual.values(), expected.values())
    }

    #[test]
    fn test_filter_run_end_encoding_array_remove_all_but_one() {
        let run_ends = Int16Array::from(vec![2, 3, 8, 10]);
        let values = Int16Array::from(vec![7, -2, 9, -8]);
        let a = RunArray::try_new(&run_ends, &values).expect("Failed to create RunArray");
        let b = BooleanArray::from(vec![
            false, false, false, false, false, false, true, false, false, false,
        ]);
        let c = filter(&a, &b).unwrap();
        let actual: &RunArray<Int16Type> = as_run_array(&c);
        assert_eq!(1, actual.len());

        let expected = RunArray::try_new(&Int16Array::from(vec![1]), &Int16Array::from(vec![9]))
            .expect("Failed to make expected RunArray test is broken");

        assert_eq!(&actual.run_ends().values(), &expected.run_ends().values());
        assert_eq!(actual.values(), expected.values())
    }

    #[test]
    fn test_filter_run_end_encoding_array_empty() {
        let run_ends = Int64Array::from(vec![2, 3, 8, 10]);
        let values = Int64Array::from(vec![7, -2, 9, -8]);
        let a = RunArray::try_new(&run_ends, &values).expect("Failed to create RunArray");
        let b = BooleanArray::from(vec![
            false, false, false, false, false, false, false, false, false, false,
        ]);
        let c = filter(&a, &b).unwrap();
        let actual: &RunArray<Int64Type> = as_run_array(&c);
        assert_eq!(0, actual.len());
    }

    #[test]
    fn test_filter_run_end_encoding_array_max_value_gt_predicate_len() {
        let run_ends = Int64Array::from(vec![2, 3, 8, 10]);
        let values = Int64Array::from(vec![7, -2, 9, -8]);
        let a = RunArray::try_new(&run_ends, &values).expect("Failed to create RunArray");
        let b = BooleanArray::from(vec![false, true, true]);
        let c = filter(&a, &b).unwrap();
        let actual: &RunArray<Int64Type> = as_run_array(&c);
        assert_eq!(2, actual.len());

        let expected = RunArray::try_new(
            &Int64Array::from(vec![1, 2]),
            &Int64Array::from(vec![7, -2]),
        )
        .expect("Failed to make expected RunArray test is broken");

        assert_eq!(&actual.run_ends().values(), &expected.run_ends().values());
        assert_eq!(actual.values(), expected.values())
    }

    #[test]
    fn test_filter_dictionary_array() {
        let values = [Some("hello"), None, Some("world"), Some("!")];
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
            DataType::LargeList(Arc::new(Field::new("item", DataType::Int32, false)));
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
            DataType::LargeList(Arc::new(Field::new("item", DataType::Int32, false)));
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
        assert_eq!(out.as_ref(), &a.slice(0, 2));
    }

    #[test]
    fn test_filter_record_batch_no_columns() {
        let pred = BooleanArray::from(vec![Some(true), Some(true), None]);
        let options = RecordBatchOptions::default().with_row_count(Some(100));
        let record_batch =
            RecordBatch::try_new_with_options(Arc::new(Schema::empty()), vec![], &options).unwrap();
        let out = filter_record_batch(&record_batch, &pred).unwrap();

        assert_eq!(out.num_rows(), 2);
    }

    #[test]
    fn test_fast_path() {
        let a: PrimitiveArray<Int64Type> = PrimitiveArray::from(vec![Some(1), Some(2), None]);

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
    fn as_deref<T: std::ops::Deref>(src: &[Option<T>]) -> impl Iterator<Item = Option<&T::Target>> {
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
        let list_data_type = DataType::new_fixed_size_list(DataType::Int32, 3, false);
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

        let list_data_type = DataType::new_fixed_size_list(DataType::Int32, 2, false);
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

        assert_eq!(filtered.to_data(), expected.to_data());
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
                        let slot1 = slot1.as_any().downcast_ref::<Float64Array>().unwrap();
                        assert_eq!(slot1.len(), 1);
                        let value1 = slot1.value(0);

                        let slot2 = slot2.as_any().downcast_ref::<Float64Array>().unwrap();
                        assert_eq!(slot2.len(), 1);
                        let value2 = slot2.value(0);
                        assert_eq!(value1, value2);
                    }
                    _ => unreachable!(),
                }
            }
        }
    }

    #[test]
    fn test_filter_struct() {
        let predicate = BooleanArray::from(vec![true, false, true, false]);

        let a = Arc::new(StringArray::from(vec!["hello", " ", "world", "!"]));
        let a_filtered = Arc::new(StringArray::from(vec!["hello", "world"]));

        let b = Arc::new(Int32Array::from(vec![5, 6, 7, 8]));
        let b_filtered = Arc::new(Int32Array::from(vec![5, 7]));

        let null_mask = NullBuffer::from(vec![true, false, false, true]);
        let null_mask_filtered = NullBuffer::from(vec![true, false]);

        let a_field = Field::new("a", DataType::Utf8, false);
        let b_field = Field::new("b", DataType::Int32, false);

        let array = StructArray::new(vec![a_field.clone()].into(), vec![a.clone()], None);
        let expected =
            StructArray::new(vec![a_field.clone()].into(), vec![a_filtered.clone()], None);

        let result = filter(&array, &predicate).unwrap();

        assert_eq!(result.to_data(), expected.to_data());

        let array = StructArray::new(
            vec![a_field.clone()].into(),
            vec![a.clone()],
            Some(null_mask.clone()),
        );
        let expected = StructArray::new(
            vec![a_field.clone()].into(),
            vec![a_filtered.clone()],
            Some(null_mask_filtered.clone()),
        );

        let result = filter(&array, &predicate).unwrap();

        assert_eq!(result.to_data(), expected.to_data());

        let array = StructArray::new(
            vec![a_field.clone(), b_field.clone()].into(),
            vec![a.clone(), b.clone()],
            None,
        );
        let expected = StructArray::new(
            vec![a_field.clone(), b_field.clone()].into(),
            vec![a_filtered.clone(), b_filtered.clone()],
            None,
        );

        let result = filter(&array, &predicate).unwrap();

        assert_eq!(result.to_data(), expected.to_data());

        let array = StructArray::new(
            vec![a_field.clone(), b_field.clone()].into(),
            vec![a.clone(), b.clone()],
            Some(null_mask.clone()),
        );

        let expected = StructArray::new(
            vec![a_field.clone(), b_field.clone()].into(),
            vec![a_filtered.clone(), b_filtered.clone()],
            Some(null_mask_filtered.clone()),
        );

        let result = filter(&array, &predicate).unwrap();

        assert_eq!(result.to_data(), expected.to_data());
    }
}
