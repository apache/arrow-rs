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

use bytes::Bytes;

use crate::basic::{ConvertedType, Encoding, LogicalType, Type};
use crate::bloom_filter::Sbbf;
use crate::column::writer::{compare_greater, is_nan};
use crate::column::writer::{fallback_encoding, has_dictionary_support, update_max, update_min};
use crate::data_type::private::ParquetValueType;
use crate::data_type::{ByteArray, DataType, FixedLenByteArray, Int96};
use crate::encodings::encoding::{
    BoolEncodingFamily, ByteArrayEncodingFamily, DictionaryValue, Encoder, EncodingFamily,
    FixedLenByteArrayEncodingFamily, NumericEncodingFamily,
};
use crate::errors::{ParquetError, Result};
use crate::file::properties::{EnabledStatistics, ResolvedColumnProperties, WriterProperties};
use crate::geospatial::accumulator::{GeoStatsAccumulator, try_new_geo_stats_accumulator};
use crate::geospatial::statistics::GeospatialStatistics;
use crate::schema::types::{BasicTypeInfo, ColumnDescPtr};

/// A collection of [`ParquetValueType`] encoded by a [`ColumnChunkEncoder`]
pub trait ColumnValues {
    /// The number of values in this collection
    fn len(&self) -> usize;
}

#[cfg(feature = "arrow")]
impl ColumnValues for dyn arrow_array::Array {
    fn len(&self) -> usize {
        arrow_array::Array::len(self)
    }
}

impl<T: ParquetValueType> ColumnValues for [T] {
    fn len(&self) -> usize {
        self.len()
    }
}

/// The encoded data for a dictionary page
pub struct DictionaryPage {
    pub buf: Bytes,
    pub num_values: usize,
    pub is_sorted: bool,
}

/// The encoded values for a data page, with optional statistics
pub struct DataPageValues<T> {
    pub buf: Bytes,
    pub num_values: usize,
    pub encoding: Encoding,
    pub min_value: Option<T>,
    pub max_value: Option<T>,
    pub nan_count: Option<u64>,
    pub variable_length_bytes: Option<i64>,
}

/// Column-chunk encoding state used by [`super::GenericColumnWriter`].
pub trait ColumnChunkEncoder {
    /// The underlying value type encoded by this encoder.
    type Value: ParquetValueType;

    /// The values encoded by this encoder
    type Values: ColumnValues + ?Sized;

    /// Create a new [`ColumnChunkEncoder`] using already-resolved per-column settings.
    #[expect(
        private_interfaces,
        reason = "this trait is not nameable outside the crate"
    )]
    fn try_new(
        descr: &ColumnDescPtr,
        props: &WriterProperties,
        column_props: &ResolvedColumnProperties,
    ) -> Result<Self>
    where
        Self: Sized;

    /// Write the corresponding values to this [`ColumnChunkEncoder`]
    fn write(&mut self, values: &Self::Values, offset: usize, len: usize) -> Result<()>;

    /// Write the values at the indexes in `indices` to this [`ColumnChunkEncoder`]
    fn write_gather(&mut self, values: &Self::Values, indices: &[usize]) -> Result<()>;

    /// Returns the largest `k` such that the first `k` values in
    /// `values[offset..offset + len]` encode to at most `byte_budget`
    /// bytes — i.e. how many values fit in a single page byte budget.
    ///
    /// Returns `len` if every value fits. Returns at least 1 if a single
    /// value alone exceeds the budget, matching parquet's "at least one
    /// value per data page" rule.
    ///
    /// `None` means "no cheap estimate available"; the caller stays on
    /// the batched fast path and lets the post-write
    /// `should_add_data_page` check handle bounding.
    fn count_values_within_byte_budget(
        _values: &Self::Values,
        _offset: usize,
        _len: usize,
        _byte_budget: usize,
    ) -> Option<usize> {
        None
    }

    /// As [`Self::count_values_within_byte_budget`] but using gather
    /// `indices` rather than a contiguous range. Returns the number of
    /// `indices` that fit, not the maximum index value.
    fn count_values_within_byte_budget_gather(
        _values: &Self::Values,
        _indices: &[usize],
        _byte_budget: usize,
    ) -> Option<usize> {
        None
    }

    /// Returns the number of buffered values
    fn num_values(&self) -> usize;

    /// Returns true if this encoder has a dictionary page
    fn has_dictionary(&self) -> bool;

    /// Returns true if the encoder compresses each value against the value
    /// immediately before it, within the current page.
    ///
    /// For such encodings a page boundary is not free: flushing discards the
    /// previous value, so the first value of the next page is stored in full.
    /// [`GenericColumnWriter::should_add_data_page`] uses this to exempt a
    /// page's mandatory first value from the data page byte limit.
    ///
    /// Per encoding:
    /// * `DELTA_BYTE_ARRAY`: true. Each value is stored as the length of the
    ///   prefix it shares with its predecessor plus the remaining suffix.
    /// * Everything else: false, the default. `PLAIN` and
    ///   `DELTA_LENGTH_BYTE_ARRAY` store a value at the same cost wherever it
    ///   lands, and a dictionary outlives the pages that index into it, so no
    ///   page boundary makes a value more expensive.
    ///
    /// [`GenericColumnWriter::should_add_data_page`]: crate::column::writer::GenericColumnWriter::should_add_data_page
    fn compresses_against_previous_value(&self) -> bool {
        false
    }

    /// Returns the estimated total memory usage of the encoder
    ///
    fn estimated_memory_size(&self) -> usize;

    /// Returns an estimate of the encoded size of dictionary page size in bytes, or `None` if no dictionary
    fn estimated_dict_page_size(&self) -> Option<usize>;

    /// Returns an estimate of the encoded data page size in bytes
    ///
    /// This should include:
    /// <already_written_encoded_byte_size> + <estimated_encoded_size_of_unflushed_bytes>
    fn estimated_data_page_size(&self) -> usize;

    /// Flush the dictionary page for this column chunk, if any. Subsequent
    /// writes use non-dictionary encoding.
    ///
    /// Note: [`Self::flush_data_page`] must be called first, as this will error if there
    /// are any pending page values
    fn flush_dict_page(&mut self) -> Result<Option<DictionaryPage>>;

    /// Flush the next data page for this column chunk
    fn flush_data_page(&mut self) -> Result<DataPageValues<Self::Value>>;

    /// Flushes bloom filter if enabled and returns it, otherwise returns `None`. Subsequent writes
    /// will *not* be tracked by the bloom filter as it is empty since. This should be called once
    /// near the end of encoding.
    fn flush_bloom_filter(&mut self) -> Option<Sbbf>;

    /// Computes [`GeospatialStatistics`], if any, and resets internal state such that any internal
    /// accumulator is prepared to accumulate statistics for the next column chunk.
    fn flush_geospatial_statistics(&mut self) -> Option<Box<GeospatialStatistics>>;
}

/// Writer-specific encoding and dictionary policy for a physical value type.
pub trait ColumnWriterValue: DictionaryValue + Sized {
    type Family<D>: EncodingFamily<D> + Encoder<D> + 'static
    where
        D: DataType<T = Self>;

    fn encode_slice<D>(enc: &mut TypedColumnChunkEncoder<D>, values: &[Self]) -> Result<()>
    where
        D: DataType<T = Self>;
}

/// Resolves the encoding family selected by [`DataType::T`].
pub(crate) type EncodingFamilyFor<D> = <<D as DataType>::T as ColumnWriterValue>::Family<D>;

macro_rules! impl_numeric_encoder_dispatch {
    ($value:ty) => {
        impl ColumnWriterValue for $value {
            type Family<D>
                = NumericEncodingFamily<D>
            where
                D: DataType<T = Self>;

            fn encode_slice<D>(enc: &mut TypedColumnChunkEncoder<D>, values: &[Self]) -> Result<()>
            where
                D: DataType<T = Self>,
            {
                enc.write_generic_slice(values)
            }
        }
    };
}

impl ColumnWriterValue for bool {
    type Family<D>
        = BoolEncodingFamily
    where
        D: DataType<T = Self>;

    fn encode_slice<D>(enc: &mut TypedColumnChunkEncoder<D>, values: &[Self]) -> Result<()>
    where
        D: DataType<T = Self>,
    {
        enc.write_generic_slice(values)
    }
}

impl_numeric_encoder_dispatch!(i32);
impl_numeric_encoder_dispatch!(i64);
impl_numeric_encoder_dispatch!(Int96);
impl_numeric_encoder_dispatch!(f32);
impl_numeric_encoder_dispatch!(f64);

impl ColumnWriterValue for FixedLenByteArray {
    type Family<D>
        = FixedLenByteArrayEncodingFamily
    where
        D: DataType<T = Self>;

    fn encode_slice<D>(enc: &mut TypedColumnChunkEncoder<D>, values: &[Self]) -> Result<()>
    where
        D: DataType<T = Self>,
    {
        enc.write_generic_slice(values)
    }
}

impl ColumnWriterValue for ByteArray {
    type Family<D>
        = ByteArrayEncodingFamily
    where
        D: DataType<T = Self>;

    fn encode_slice<D>(enc: &mut TypedColumnChunkEncoder<D>, values: &[Self]) -> Result<()>
    where
        D: DataType<T = Self>,
    {
        enc.write_generic_slice(values)
    }
}

/// Concrete [`ColumnChunkEncoder`] implementation for one Parquet physical type.
///
/// This owns the physical encoder together with the state accumulated while a
/// column chunk is being written: dictionary adoption/fallback, value count,
/// statistics, bloom filter state, variable-length byte totals and geospatial
/// statistics. Slice-based writers call the trait methods directly; with the
/// `arrow` feature, numeric writers can feed values directly from Arrow buffers.
pub struct TypedColumnChunkEncoder<T: DataType> {
    /// The active encoding family. Dictionary fallback is a variant transition
    /// inside [`EncodingFamily::take_dict_page`].
    encoding_family: EncodingFamilyFor<T>,
    descr: ColumnDescPtr,
    num_values: usize,
    statistics_enabled: EnabledStatistics,
    min_value: Option<T::T>,
    max_value: Option<T::T>,
    nan_count: Option<u64>,
    bloom_filter: Option<Sbbf>,
    bloom_filter_target_fpp: f64,
    variable_length_bytes: Option<i64>,
    geo_stats_accumulator: Option<Box<dyn GeoStatsAccumulator>>,
    /// The non-dictionary page encoding selected from the writer properties.
    /// [`EncodingFamily::take_dict_page`] uses it to build the fallback encoder
    /// when dictionary encoding is abandoned.
    fallback_encoding: Encoding,
}

impl<T: DataType> TypedColumnChunkEncoder<T> {
    fn is_floating_point_column(&self) -> bool {
        matches!(self.descr.physical_type(), Type::FLOAT | Type::DOUBLE)
            || self.descr.logical_type_ref() == Some(&LogicalType::Float16)
    }

    /// Fold statistics, bloom filter and encoded bytes for a dense slice of
    /// values. Every physical type shares this path until its own batch sink
    /// takes over.
    fn write_generic_slice(&mut self, slice: &[T::T]) -> Result<()> {
        if self.statistics_enabled != EnabledStatistics::None
            // INTERVAL, Geometry, and Geography have undefined sort order, so don't write min/max stats for them
            && self.descr.converted_type() != ConvertedType::INTERVAL
        {
            if let Some(accumulator) = self.geo_stats_accumulator.as_deref_mut() {
                update_geo_stats_accumulator(accumulator, slice.iter());
            } else if let Some((min, max, nan_count)) =
                get_min_max(self.descr.get_basic_info(), slice.iter())
            {
                update_min(&self.descr, &min, &mut self.min_value);
                update_max(&self.descr, &max, &mut self.max_value);
                if self.is_floating_point_column() {
                    *self.nan_count.get_or_insert(0) += nan_count;
                }
            }

            if let Some(var_bytes) = T::T::variable_length_bytes(slice) {
                *self.variable_length_bytes.get_or_insert(0) += var_bytes;
            }
        }

        // Dictionary values are hashed once at dictionary flush, not once per row.
        if !self.encoding_family.is_dictionary()
            && let Some(bloom_filter) = &mut self.bloom_filter
        {
            for value in slice {
                bloom_filter.insert(value);
            }
        }

        self.encoding_family.put(slice)
    }
}

impl<T: DataType> ColumnChunkEncoder for TypedColumnChunkEncoder<T> {
    type Value = T::T;

    type Values = [T::T];

    fn write(&mut self, values: &[T::T], offset: usize, len: usize) -> Result<()> {
        self.num_values += len;

        let slice = values.get(offset..offset + len).ok_or_else(|| {
            general_err!(
                "Expected to write {} values, but have only {}",
                len,
                values.len() - offset
            )
        })?;

        <T::T as ColumnWriterValue>::encode_slice(self, slice)
    }

    fn write_gather(&mut self, values: &Self::Values, indices: &[usize]) -> Result<()> {
        self.num_values += indices.len();
        let slice: Vec<_> = indices.iter().map(|idx| values[*idx].clone()).collect();
        <T::T as ColumnWriterValue>::encode_slice(self, &slice)
    }

    fn count_values_within_byte_budget(
        values: &[T::T],
        offset: usize,
        len: usize,
        byte_budget: usize,
    ) -> Option<usize> {
        // Clamp so that a caller-supplied `len` that overruns the input
        // (e.g. a level/value mismatch the encoder will reject later)
        // returns an estimate instead of panicking here.
        let end = (offset + len).min(values.len());
        let start = offset.min(end);
        count_within_budget::<T>(
            end - start,
            byte_budget,
            values[start..end].iter().map(Some),
        )
    }

    fn count_values_within_byte_budget_gather(
        values: &[T::T],
        indices: &[usize],
        byte_budget: usize,
    ) -> Option<usize> {
        // `values.get` yields `None` for an out-of-range index (defensive
        // against a level/value mismatch the encoder rejects later); such a
        // position is counted but contributes no bytes.
        count_within_budget::<T>(
            indices.len(),
            byte_budget,
            indices.iter().map(|&i| values.get(i)),
        )
    }

    fn flush_bloom_filter(&mut self) -> Option<Sbbf> {
        let mut sbbf = self.bloom_filter.take()?;
        sbbf.fold_to_target_fpp(self.bloom_filter_target_fpp);
        Some(sbbf)
    }

    #[expect(
        private_interfaces,
        reason = "this trait is not nameable outside the crate"
    )]
    fn try_new(
        descr: &ColumnDescPtr,
        props: &WriterProperties,
        column_props: &ResolvedColumnProperties,
    ) -> Result<Self> {
        let dict_supported = column_props.dictionary_enabled
            && has_dictionary_support(T::get_physical_type(), props);

        // The non-dictionary page encoding comes from writer properties so
        // `flush_dict_page` can build the fallback lazily.
        let fallback_encoding = column_props
            .encoding
            .unwrap_or_else(|| fallback_encoding(T::get_physical_type(), props));

        // Encoding families validate the fallback even when starting with a
        // dictionary because dictionary fallback constructs it lazily.
        let encoding_family = <EncodingFamilyFor<T> as EncodingFamily<T>>::try_new(
            dict_supported,
            fallback_encoding,
            descr,
        )?;

        let statistics_enabled = column_props.statistics_enabled;

        let (bloom_filter, bloom_filter_target_fpp) = create_bloom_filter(column_props)?;

        let geo_stats_accumulator = try_new_geo_stats_accumulator(descr);

        Ok(Self {
            encoding_family,
            descr: descr.clone(),
            num_values: 0,
            statistics_enabled,
            bloom_filter,
            bloom_filter_target_fpp,
            min_value: None,
            max_value: None,
            nan_count: None,
            variable_length_bytes: None,
            geo_stats_accumulator,
            fallback_encoding,
        })
    }

    fn num_values(&self) -> usize {
        self.num_values
    }

    fn has_dictionary(&self) -> bool {
        self.encoding_family.is_dictionary()
    }

    fn compresses_against_previous_value(&self) -> bool {
        !self.encoding_family.is_dictionary()
            && self.encoding_family.encoding() == Encoding::DELTA_BYTE_ARRAY
    }

    fn estimated_memory_size(&self) -> usize {
        let encoder_size = self.encoding_family.memory_size();

        let bloom_filter_size = self
            .bloom_filter
            .as_ref()
            .map(|bf| bf.estimated_memory_size())
            .unwrap_or_default();

        // The running column min/max pin their values' heap bytes through page
        // flush — for byte arrays that can be two full values
        // (zero for the fixed-width scalars, whose `variable_length_bytes` is
        // `None`).
        let stats_size = [&self.min_value, &self.max_value]
            .into_iter()
            .flatten()
            .map(|v| {
                <T::T as ParquetValueType>::variable_length_bytes(std::slice::from_ref(v))
                    .unwrap_or(0) as usize
            })
            .sum::<usize>();

        encoder_size + bloom_filter_size + stats_size
    }

    fn estimated_dict_page_size(&self) -> Option<usize> {
        self.encoding_family.dict_page_size()
    }

    fn estimated_data_page_size(&self) -> usize {
        self.encoding_family.data_page_size()
    }

    fn flush_dict_page(&mut self) -> Result<Option<DictionaryPage>> {
        if self.encoding_family.is_dictionary() && self.num_values != 0 {
            return Err(general_err!(
                "Must flush data pages before flushing dictionary"
            ));
        }
        if let Some(bloom_filter) = &mut self.bloom_filter {
            self.encoding_family
                .visit_dictionary_values(|value| bloom_filter.insert(value));
        }
        // `take_dict_page` serializes the dictionary page and transitions the encoder
        // to the fallback encoding (dictionary fallback); `None` when not dictionary.
        Ok(self
            .encoding_family
            .take_dict_page(self.fallback_encoding, &self.descr)?
            .map(|(buf, num_values, is_sorted)| DictionaryPage {
                buf,
                num_values,
                is_sorted,
            }))
    }

    fn flush_data_page(&mut self) -> Result<DataPageValues<T::T>> {
        let (buf, encoding) = self.encoding_family.flush_data_page()?;

        Ok(DataPageValues {
            buf,
            encoding,
            num_values: std::mem::take(&mut self.num_values),
            min_value: self.min_value.take(),
            max_value: self.max_value.take(),
            nan_count: self.nan_count.take(),
            variable_length_bytes: self.variable_length_bytes.take(),
        })
    }

    fn flush_geospatial_statistics(&mut self) -> Option<Box<GeospatialStatistics>> {
        self.geo_stats_accumulator.as_mut().map(|a| a.finish())?
    }
}

// Get min and max values for all values in `iter`.
//
// For floating point we need to compare NaN values until we encounter a non-NaN
// value which then becomes the new min/max. After this, only non-NaN values are
// evaluated. If all values are NaN, then the min/max NaNs as determined by
// IEEE 754 total order are returned.
fn get_min_max<'a, T, I>(basic_type_info: &BasicTypeInfo, mut iter: I) -> Option<(T, T, u64)>
where
    T: ParquetValueType + 'a,
    I: Iterator<Item = &'a T>,
{
    let first = iter.next()?;
    let mut min_max_nan = is_nan(basic_type_info, first);
    let mut nan_count = min_max_nan as u64;

    let mut min = first;
    let mut max = first;
    for val in iter {
        match (min_max_nan, is_nan(basic_type_info, val)) {
            // skip NaNs if we've encounter non-NaN
            (false, true) => {
                nan_count += 1;
            }
            // if min/max are NaN, check for non-NaN and reset
            (true, false) => {
                min = val;
                max = val;
                min_max_nan = false;
            }
            // both are NaN or non-NaN, so do the comparison
            (_, val_is_nan) => {
                nan_count += val_is_nan as u64;
                // we've already initialized min and max, so a single value can't be both
                // extremes
                if compare_greater(basic_type_info, min, val) {
                    min = val;
                } else if compare_greater(basic_type_info, val, max) {
                    max = val;
                }
            }
        }
    }

    Some((min.clone(), max.clone(), nan_count))
}

fn update_geo_stats_accumulator<'a, T, I>(bounder: &mut dyn GeoStatsAccumulator, iter: I)
where
    T: ParquetValueType + 'a,
    I: Iterator<Item = &'a T>,
{
    if bounder.is_valid() {
        for val in iter {
            bounder.update_wkb(val.as_bytes());
        }
    }
}

/// Creates a bloom filter sized for the column's configured NDV, returning the filter
/// and the target FPP for folding.
pub(crate) fn create_bloom_filter(
    column_props: &ResolvedColumnProperties,
) -> Result<(Option<Sbbf>, f64)> {
    match column_props.bloom_filter_properties.as_ref() {
        Some(bf_props) => Ok((
            Some(Sbbf::new_with_ndv_fpp(bf_props.ndv(), bf_props.fpp())?),
            bf_props.fpp(),
        )),
        None => Ok((None, 0.0)),
    }
}

/// Plain-encoded byte cost of a single value of type `T::T`.
///
/// Derived from [`ParquetValueType::dict_encoding_size`] (which returns
/// `(per-value overhead, value-bytes)`) so we don't add a parallel
/// per-value-size hook to the trait. Mirrors the dispatch in
/// `KeyStorage::push` (`encodings/encoding/dict_encoder.rs`).
///
#[inline]
fn plain_encoded_byte_size<T: DataType>(value: &T::T) -> usize {
    let (overhead, bytes) = value.dict_encoding_size();
    match <T::T as ParquetValueType>::PHYSICAL_TYPE {
        // Plain BYTE_ARRAY = 4-byte length prefix + payload.
        Type::BYTE_ARRAY => overhead + bytes,
        // Plain fixed-length byte arrays are raw bytes only; `dict_encoding_size`'s length prefix
        // is irrelevant here, so the encoder passes `type_length` directly.
        Type::FIXED_LEN_BYTE_ARRAY => bytes,
        // Numeric/bool are short-circuited by the caller via
        // `mem::size_of`, so this is unreachable in practice; fall back to
        // `overhead` defensively.
        _ => overhead,
    }
}

fn count_within_budget<'a, T: DataType>(
    n: usize,
    byte_budget: usize,
    vals: impl Iterator<Item = Option<&'a T::T>>,
) -> Option<usize>
where
    T::T: 'a,
{
    // Fixed-size physical types have a constant per-value byte cost, so the
    // answer is one division — no walk needed.
    let phys = <T::T as ParquetValueType>::PHYSICAL_TYPE;
    if phys != Type::BYTE_ARRAY && phys != Type::FIXED_LEN_BYTE_ARRAY {
        let per = std::mem::size_of::<T::T>().max(1);
        return Some((byte_budget / per).max(1).min(n));
    }
    // Variable-width: accumulate, exit at the first value past the budget.
    let mut cum: usize = 0;
    for (i, v) in vals.enumerate() {
        if let Some(v) = v {
            cum = cum.saturating_add(plain_encoded_byte_size::<T>(v));
        }
        if cum > byte_budget {
            return Some(i + 1);
        }
    }
    Some(n)
}
