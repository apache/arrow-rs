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

#[cfg(feature = "arrow")]
use std::mem::MaybeUninit;

use bytes::Bytes;

use self::byte_array::encode_byte_slice;
use crate::basic::{ConvertedType, Encoding, LogicalType, Type};
use crate::bloom_filter::Sbbf;
use crate::column::value_batch::{BatchSink, RunBatch, ValueProducer};
#[cfg(feature = "arrow")]
use crate::column::value_batch::{
    assume_init_prefix, gather_run_groups_tiled, gather_tiled, map_values,
};
#[cfg(feature = "arrow")]
use crate::column::value_selection::PhysicalValueSelection;
use crate::column::writer::{compare_greater_byte_array, is_f16_nan};
use crate::column::writer::{fallback_encoding, has_dictionary_support, update_max, update_min};
use crate::data_type::FixedLenByteArrayType;
use crate::data_type::private::ParquetValueType;
use crate::data_type::{BoolType, ByteArray, DataType, FixedLenByteArray, Int96};
#[cfg(feature = "arrow")]
use crate::encodings::encoding::DictionaryStorage;
use crate::encodings::encoding::{BoolBatch, BoolEncoder};
use crate::encodings::encoding::{
    BoolEncodingFamily, ByteArrayEncodingFamily, DictEncoder, DictionaryValue, Encoder,
    EncodingFamily, FixedLenByteArrayEncodingFamily, NumericEncodingFamily, PlainEncoderType,
};
use crate::encodings::encoding::{FixedLenByteArrayEncoder, PackedFixedLenByteArrayBatch};
use crate::errors::{ParquetError, Result};
use crate::file::properties::{EnabledStatistics, ResolvedColumnProperties, WriterProperties};
use crate::geospatial::accumulator::{GeoStatsAccumulator, try_new_geo_stats_accumulator};
use crate::geospatial::statistics::GeospatialStatistics;
use crate::schema::types::{ColumnDescPtr, ColumnDescriptor};

mod boolean;
pub(super) mod byte_array;
mod fixed_len_byte_array;
mod numeric;

#[cfg(feature = "arrow")]
pub(crate) use byte_array::{ByteArrayBatch, ByteArraySink, ByteArraySource};
use fixed_len_byte_array::encode_fixed_len_byte_array_slice;
#[cfg(feature = "arrow")]
pub(crate) use fixed_len_byte_array::{
    FIXED_LEN_BYTE_ARRAY_BATCH_VALUES, FIXED_LEN_BYTE_ARRAY_MAX_WIDTH, FixedLenByteArrayBatchPacker,
};
#[cfg(any(feature = "arrow", test))]
pub(crate) use fixed_len_byte_array::{
    FixedLenByteArrayBatch, FixedLenByteArraySink, FixedLenByteArraySource,
};
use numeric::NumericBatch;
#[cfg(feature = "arrow")]
pub(crate) use numeric::PhysicalNumericSource;

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
                if values.is_empty() {
                    Ok(())
                } else {
                    enc.push_batch(NumericBatch::Flat(values))
                }
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
        enc.push_batch(BoolBatch::from_bool_slice(values))
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
        encode_fixed_len_byte_array_slice(enc, values)
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
        encode_byte_slice(enc, values)
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
    /// Reusable extrema buffers for fixed-length byte-array writes. Empty for
    /// every other physical type.
    fixed_len_byte_array_scratch: FixedLenByteArrayScratch,
}

/// Reusable storage for extrema copied from transient fixed-length byte-array batches.
#[derive(Default)]
struct FixedLenByteArrayScratch {
    min: Vec<u8>,
    max: Vec<u8>,
}

impl<T: DataType> TypedColumnChunkEncoder<T> {
    #[cfg(feature = "arrow")]
    pub(crate) fn start_arrow_source(&mut self) {
        self.encoding_family.start_arrow_source()
    }
}

impl<T: DataType> ColumnChunkEncoder for TypedColumnChunkEncoder<T> {
    type Value = T::T;

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
            fixed_len_byte_array_scratch: FixedLenByteArrayScratch::default(),
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
        // `None`). The fixed-length byte-array scratch accumulator is folded in
        // the same way; it
        // stays empty for every other family.
        let stats_size = [&self.min_value, &self.max_value]
            .into_iter()
            .flatten()
            .map(|v| {
                <T::T as ParquetValueType>::variable_length_bytes(std::slice::from_ref(v))
                    .unwrap_or(0) as usize
            })
            .sum::<usize>()
            + self.fixed_len_byte_array_scratch.min.capacity()
            + self.fixed_len_byte_array_scratch.max.capacity();

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
            let mut batch = bloom_filter.batch();
            self.encoding_family
                .visit_dictionary_values(|value| batch.insert(value));
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

#[inline]
fn int_is_unsigned(descr: &ColumnDescriptor) -> bool {
    if let Some(LogicalType::Integer(int)) = descr.logical_type_ref()
        && !int.is_signed
    {
        return true;
    }
    matches!(
        descr.converted_type(),
        ConvertedType::UINT_8
            | ConvertedType::UINT_16
            | ConvertedType::UINT_32
            | ConvertedType::UINT_64
    )
}

#[inline(always)]
fn int32_greater(unsigned: bool, a: i32, b: i32) -> bool {
    if unsigned {
        (a as u32) > (b as u32)
    } else {
        a > b
    }
}

#[inline(always)]
fn int64_greater(unsigned: bool, a: i64, b: i64) -> bool {
    if unsigned {
        (a as u64) > (b as u64)
    } else {
        a > b
    }
}

/// Min/max folding for numeric scalars and variable-width byte arrays. Numeric
/// strategies retain scalar values; the byte strategy retains sized `&[u8]`
/// handles to variable-width payloads without per-value allocation. `Owned` is
/// the materialized column statistic. Fixed-length byte arrays use a separate owned accumulator
/// because its computed values can borrow transient tiles.
///
/// Integer columns need descriptor-derived context because Parquet min/max for
/// unsigned logical types must compare the stored bits as unsigned values.
/// Float columns do not need descriptor context, but must count NaNs and retain
/// an IEEE-total-ordered NaN extremum when a page contains only NaNs. [`Self::Ctx`]
/// stores descriptor decisions once per column so comparison and classification
/// can stay small inside per-value loops.
pub(crate) trait MinMaxStrategy<'v> {
    /// The per-value handle folded into statistics (owned scalar or borrowed bytes).
    type Elem: Copy;
    /// The materialized column statistic type.
    type Owned;
    /// Per-column comparison context, derived from the descriptor and reused
    /// for every value (e.g. integer signedness).
    type Ctx: Copy;

    /// Build the comparison context for this column.
    fn ctx(descr: &ColumnDescriptor) -> Self::Ctx;
    /// `a > b` under the column's logical order.
    fn greater(ctx: Self::Ctx, a: Self::Elem, b: Self::Elem) -> bool;
    /// Whether this strategy represents a floating-point type with NaNs.
    const TRACKS_NAN: bool = false;
    /// True when `value` is NaN; false by default.
    #[inline(always)]
    fn is_nan(_ctx: Self::Ctx, _value: Self::Elem) -> bool {
        false
    }
    /// Materialize an accumulated element into the owned column statistic.
    fn to_owned(value: Self::Elem) -> Self::Owned;

    /// Fold one value into the running `(min, max)`.
    #[inline(always)]
    fn observe(
        ctx: Self::Ctx,
        value: Self::Elem,
        min: &mut Option<Self::Elem>,
        max: &mut Option<Self::Elem>,
    ) {
        let value_is_nan = Self::is_nan(ctx, value);
        match min {
            None => *min = Some(value),
            Some(current) => match (Self::is_nan(ctx, *current), value_is_nan) {
                // Once a non-NaN is observed, later NaNs do not participate in extrema.
                (false, true) => {}
                // The first non-NaN replaces an all-NaN running extremum.
                (true, false) => *current = value,
                // Both values are NaN or both are non-NaN.
                _ if Self::greater(ctx, *current, value) => *current = value,
                _ => {}
            },
        }
        match max {
            None => *max = Some(value),
            Some(current) => match (Self::is_nan(ctx, *current), value_is_nan) {
                (false, true) => {}
                (true, false) => *current = value,
                _ if Self::greater(ctx, value, *current) => *current = value,
                _ => {}
            },
        }
    }
}

impl MinMaxStrategy<'_> for i32 {
    type Elem = i32;
    type Owned = i32;
    type Ctx = bool;
    #[inline(always)]
    fn ctx(descr: &ColumnDescriptor) -> bool {
        int_is_unsigned(descr)
    }
    #[inline(always)]
    fn greater(unsigned: bool, a: i32, b: i32) -> bool {
        int32_greater(unsigned, a, b)
    }
    #[inline(always)]
    fn to_owned(v: i32) -> i32 {
        v
    }
}

impl MinMaxStrategy<'_> for i64 {
    type Elem = i64;
    type Owned = i64;
    type Ctx = bool;
    #[inline(always)]
    fn ctx(descr: &ColumnDescriptor) -> bool {
        int_is_unsigned(descr)
    }
    #[inline(always)]
    fn greater(unsigned: bool, a: i64, b: i64) -> bool {
        int64_greater(unsigned, a, b)
    }
    #[inline(always)]
    fn to_owned(v: i64) -> i64 {
        v
    }
}

impl MinMaxStrategy<'_> for f32 {
    type Elem = f32;
    type Owned = f32;
    type Ctx = ();
    #[inline(always)]
    fn ctx(_: &ColumnDescriptor) {}
    #[inline(always)]
    fn greater((): (), a: f32, b: f32) -> bool {
        a.total_cmp(&b).is_gt()
    }
    const TRACKS_NAN: bool = true;
    #[inline(always)]
    fn is_nan((): (), value: f32) -> bool {
        value.is_nan()
    }
    #[inline(always)]
    fn to_owned(v: f32) -> f32 {
        v
    }
}

impl MinMaxStrategy<'_> for f64 {
    type Elem = f64;
    type Owned = f64;
    type Ctx = ();
    #[inline(always)]
    fn ctx(_: &ColumnDescriptor) {}
    #[inline(always)]
    fn greater((): (), a: f64, b: f64) -> bool {
        a.total_cmp(&b).is_gt()
    }
    const TRACKS_NAN: bool = true;
    #[inline(always)]
    fn is_nan((): (), value: f64) -> bool {
        value.is_nan()
    }
    #[inline(always)]
    fn to_owned(v: f64) -> f64 {
        v
    }
}

impl MinMaxStrategy<'_> for Int96 {
    type Elem = Int96;
    type Owned = Int96;
    type Ctx = ();
    #[inline(always)]
    fn ctx(_: &ColumnDescriptor) {}
    #[inline(always)]
    fn greater((): (), a: Int96, b: Int96) -> bool {
        // INT96 min/max use the timestamp `(days, nanos)` order (`Int96: Ord`),
        // matching the descriptor-driven `compare_greater` merge in `merge_batch_stats`.
        a > b
    }
    #[inline(always)]
    fn to_owned(v: Int96) -> Int96 {
        v
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

/// How many leading present values fit in `byte_budget` bytes. The value that
/// crosses the budget is included so the post-write page check flushes on this
/// mini-batch rather than leaving a sliver for the next page.
#[inline]
pub(crate) fn count_within_budget<'a, T: DataType>(
    byte_budget: usize,
    vals: impl Iterator<Item = &'a T::T>,
) -> usize
where
    T::T: 'a,
{
    let mut cum: usize = 0;
    let mut count = 0;
    for value in vals {
        count += 1;
        cum = cum.saturating_add(plain_encoded_byte_size::<T>(value));
        if cum > byte_budget {
            return count;
        }
    }
    count
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::data_type::FixedLenByteArrayType;
    use crate::schema::types::{ColumnPath, Type as SchemaType};

    #[test]
    fn packed_fixed_len_byte_array_batches_merge_statistics() {
        let primitive = SchemaType::primitive_type_builder("col", Type::FIXED_LEN_BYTE_ARRAY)
            .with_length(2)
            .with_logical_type(Some(LogicalType::Float16))
            .build()
            .unwrap();
        let descriptor = Arc::new(ColumnDescriptor::new(
            Arc::new(primitive),
            0,
            0,
            ColumnPath::from("col"),
        ));
        let properties = WriterProperties::builder()
            .set_dictionary_enabled(false)
            .build();
        let column_props = properties.resolve_column_properties(descriptor.path());
        let mut encoder = TypedColumnChunkEncoder::<FixedLenByteArrayType>::try_new(
            &descriptor,
            &properties,
            &column_props,
        )
        .unwrap();

        struct PackedSource<'a>(&'a [&'a [u8]]);
        impl FixedLenByteArraySource for PackedSource<'_> {
            fn len(&self) -> usize {
                self.0.iter().map(|batch| batch.len() / 2).sum()
            }

            fn validate_width(&self, expected_width: usize) -> Result<()> {
                if expected_width != 2 {
                    return Err(general_err!(
                        "Mismatched FixedLenByteArray sizes: 2 != {}",
                        expected_width
                    ));
                }
                Ok(())
            }

            fn write_to(self, sink: &mut FixedLenByteArraySink<'_>) -> Result<()> {
                for batch in self.0 {
                    sink.push_batch(FixedLenByteArrayBatch::Packed(
                        PackedFixedLenByteArrayBatch::new(batch, 2, batch.len() / 2),
                    ))?;
                }
                Ok(())
            }
        }

        let all_nan: &[&[u8]] = &[&[0x01, 0x7e], &[0x01, 0xfe]];
        let mixed: &[&[u8]] = &[
            &[0x01, 0x7e, 0x01, 0xfe],
            &[0x00, 0x42, 0x00, 0xc0], // 3.0, -2.0
            &[0x00, 0x7e],
        ];
        for (batches, min, max, nan_count) in [
            (all_nan, [0x01, 0xfe], [0x01, 0x7e], 2),
            (mixed, [0x00, 0xc0], [0x00, 0x42], 3),
        ] {
            encoder
                .write_fixed_len_byte_array_source(PackedSource(batches))
                .unwrap();
            let page = encoder.flush_data_page().unwrap();
            assert_eq!(page.min_value, Some(min.to_vec().into()));
            assert_eq!(page.max_value, Some(max.to_vec().into()));
            assert_eq!(page.nan_count, Some(nan_count));
        }

        let properties = WriterProperties::builder()
            .set_dictionary_enabled(true)
            .set_writer_version(crate::file::properties::WriterVersion::PARQUET_2_0)
            .build();
        let column_props = properties.resolve_column_properties(descriptor.path());
        let mut encoder = TypedColumnChunkEncoder::<FixedLenByteArrayType>::try_new(
            &descriptor,
            &properties,
            &column_props,
        )
        .unwrap();
        let values = [FixedLenByteArray::from(vec![0_u8; 2])];
        encoder
            .write_fixed_len_byte_array_source(&values[..])
            .unwrap();
        assert!(encoder.flush_dict_page().is_err());
        encoder.flush_data_page().unwrap();
        assert!(encoder.flush_dict_page().unwrap().is_some());
    }
}
