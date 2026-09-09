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

//! Byte-array sources, batches, and write-scoped encoding state.
//!
//! Sources can provide contiguous Arrow offset ranges directly or gather
//! non-contiguous values into bounded `&[u8]` batches.

use std::mem::MaybeUninit;

use super::{ColumnChunkEncoder, MinMaxStrategy, TypedColumnChunkEncoder};
use crate::basic::{ConvertedType, LogicalType};
use crate::bloom_filter::Sbbf;
use crate::column::value_batch::{
    BatchSink, RunBatch, ValueProducer, gather_run_groups_tiled, gather_tiled,
};
use crate::column::writer::{compare_greater_byte_array_decimals, compare_greater_f16};
use crate::data_type::private::byte_array_length;
use crate::data_type::{AsBytes, ByteArray, ByteArrayType, DataType};
#[cfg(test)]
use crate::encodings::encoding::Encoder;
use crate::encodings::encoding::{
    ByteArrayDeltaEncoder, ByteArrayDeltaLengthEncoder, ByteArrayEncodingFamily,
    ByteArrayPlainEncoder, DictEncoder, EncodingFamily,
};
#[cfg(feature = "arrow")]
use crate::errors::ParquetError;
use crate::errors::Result;
use crate::file::properties::EnabledStatistics;
use crate::geospatial::accumulator::GeoStatsAccumulator;
use crate::schema::types::ColumnDescriptor;
#[cfg(feature = "arrow")]
use crate::util::interner::Interner;
#[cfg(feature = "arrow")]
use arrow_buffer::ArrowNativeType;

/// Observation performed alongside byte-array encoding.
trait ByteArrayValueObserver<'a> {
    #[cfg(feature = "arrow")]
    fn observe(&mut self, value: &'a [u8]);

    fn observe_batch<'batch>(&mut self, values: ByteArrayBatch<'batch, 'a>) -> Result<()>
    where
        'a: 'batch;
}

/// Consumer for logical byte values in a batch.
trait ByteArrayValueConsumer<'source> {
    fn consume(&mut self, value: &'source [u8]) -> Result<()>;
}

struct PlainValueByteCounter {
    unencoded_value_bytes: usize,
}

impl ByteArrayValueConsumer<'_> for PlainValueByteCounter {
    #[inline(always)]
    fn consume(&mut self, value: &[u8]) -> Result<()> {
        byte_array_length(value.len())?;
        self.unencoded_value_bytes = self.unencoded_value_bytes.saturating_add(value.len());
        Ok(())
    }
}

struct PlainEncode<'state> {
    encoder: &'state mut ByteArrayPlainEncoder,
}

impl ByteArrayValueConsumer<'_> for PlainEncode<'_> {
    #[inline(always)]
    fn consume(&mut self, value: &[u8]) -> Result<()> {
        self.encoder.put_value(value);
        Ok(())
    }
}

struct DeltaLengthEncode<'state> {
    encoder: &'state mut ByteArrayDeltaLengthEncoder,
    unencoded_value_bytes: i64,
}

impl ByteArrayValueConsumer<'_> for DeltaLengthEncode<'_> {
    #[inline(always)]
    fn consume(&mut self, value: &[u8]) -> Result<()> {
        self.encoder.put_value(value)?;
        self.unencoded_value_bytes += value.len() as i64;
        Ok(())
    }
}

#[cfg(feature = "arrow")]
struct ObserveAndConsume<'state, O, C> {
    observer: &'state mut O,
    consumer: &'state mut C,
}

#[cfg(feature = "arrow")]
impl<'source, O, C> ByteArrayValueConsumer<'source> for ObserveAndConsume<'_, O, C>
where
    O: ByteArrayValueObserver<'source>,
    C: ByteArrayValueConsumer<'source>,
{
    #[inline(always)]
    fn consume(&mut self, value: &'source [u8]) -> Result<()> {
        self.observer.observe(value);
        self.consumer.consume(value)
    }
}

struct DictionaryEncode<'state> {
    encoder: &'state mut DictEncoder<ByteArrayType>,
    unencoded_value_bytes: i64,
}

impl<'source> ByteArrayValueConsumer<'source> for DictionaryEncode<'_> {
    #[inline(always)]
    fn consume(&mut self, value: &'source [u8]) -> Result<()> {
        self.encoder
            .put_value_bytes(value, || value.to_vec().into())?;
        self.unencoded_value_bytes += value.len() as i64;
        Ok(())
    }
}

impl DictEncoder<ByteArrayType> {
    #[inline(never)]
    fn encode_values_observed<'batch, 'source: 'batch>(
        &mut self,
        values: ByteArrayBatch<'batch, 'source>,
        observer: &mut ByteArrayObserver<'source, '_>,
    ) -> Result<i64> {
        observer.observe_batch(values)?;
        let mut encode = DictionaryEncode {
            encoder: self,
            unencoded_value_bytes: 0,
        };
        values.try_for_each(&mut encode)?;
        Ok(encode.unencoded_value_bytes)
    }
}

/// How byte-array min/max statistics are ordered. Determined from the column
/// descriptor. `Unsigned` (plain lexicographic) is the case for every Arrow byte
/// column (Utf8/Binary/views); the other variants handle the numeric ordering
/// of byte-array and fixed-length byte-array `DECIMAL`/`FLOAT16` columns.
#[derive(Clone, Copy, PartialEq)]
pub(crate) enum ByteMinMaxOrder {
    Unsigned,
    Decimal,
    Float16,
}

impl ByteMinMaxOrder {
    pub(crate) fn from_descr(descr: &ColumnDescriptor) -> Self {
        if matches!(descr.logical_type_ref(), Some(LogicalType::Decimal(_)))
            || descr.converted_type() == ConvertedType::DECIMAL
        {
            ByteMinMaxOrder::Decimal
        } else if matches!(descr.logical_type_ref(), Some(LogicalType::Float16)) {
            ByteMinMaxOrder::Float16
        } else {
            ByteMinMaxOrder::Unsigned
        }
    }

    /// `true` when `a > b` under this column's byte order.
    #[inline]
    pub(crate) fn greater(self, a: &[u8], b: &[u8]) -> bool {
        match self {
            ByteMinMaxOrder::Unsigned => a > b,
            ByteMinMaxOrder::Decimal => compare_greater_byte_array_decimals(a, b),
            ByteMinMaxOrder::Float16 => compare_greater_f16(a, b),
        }
    }
}

/// [`MinMaxStrategy`] marker for variable-width byte arrays. The sized `&[u8]`
/// handles retain borrowed payloads as min/max without per-value allocation; the
/// materialized column statistic is a [`ByteArray`]. Ordering comes from the
/// column's [`ByteMinMaxOrder`].
pub(crate) struct ByteMinMax;

impl<'v> MinMaxStrategy<'v> for ByteMinMax {
    type Elem = &'v [u8];
    type Owned = ByteArray;
    type Ctx = ByteMinMaxOrder;

    #[inline(always)]
    fn ctx(descr: &ColumnDescriptor) -> ByteMinMaxOrder {
        ByteMinMaxOrder::from_descr(descr)
    }
    #[inline(always)]
    fn greater(order: ByteMinMaxOrder, a: &[u8], b: &[u8]) -> bool {
        order.greater(a, b)
    }
    #[inline(always)]
    fn to_owned(v: &[u8]) -> ByteArray {
        ByteArray::from(v.to_vec())
    }
}

impl ByteArrayEncodingFamily {
    /// Encode and observe one batch.
    #[inline(always)]
    fn encode_values_observed<'batch, 'source: 'batch, O>(
        &mut self,
        values: ByteArrayBatch<'batch, 'source>,
        observer: &mut O,
    ) -> Result<i64>
    where
        O: ByteArrayValueObserver<'source>,
    {
        match self {
            Self::Dictionary(_) => {
                unreachable!("dictionary variant is not routed through the fallback encoder")
            }
            Self::Plain(encoder) => encode_plain_values(encoder, values, observer),
            Self::DeltaLength(encoder) => encode_delta_length_values(encoder, values, observer),
            Self::Delta(encoder) => encode_delta_values(encoder, values, observer),
        }
    }
}

/// Validate a PLAIN batch and reserve its encoded size.
#[inline(always)]
fn prepare_plain_values<'batch, 'source: 'batch>(
    encoder: &mut ByteArrayPlainEncoder,
    values: ByteArrayBatch<'batch, 'source>,
) -> Result<i64> {
    // Dense small-offset sources provide an exact O(1) reservation and
    // intrinsically valid u32 lengths. Other sources retain the full
    // validation pass before the buffer is mutated.
    let unencoded_value_bytes = if let Some((payload, encoded)) = values.exact_plain_size() {
        encoder.reserve(encoded);
        payload
    } else {
        let mut value_bytes = PlainValueByteCounter {
            unencoded_value_bytes: 0,
        };
        values.try_for_each(&mut value_bytes)?;
        encoder.reserve(
            value_bytes
                .unencoded_value_bytes
                .saturating_add(values.len().saturating_mul(4)),
        );
        value_bytes.unencoded_value_bytes
    };
    Ok(unencoded_value_bytes as i64)
}

#[inline(never)]
fn encode_plain_values<'batch, 'source: 'batch, O>(
    encoder: &mut ByteArrayPlainEncoder,
    values: ByteArrayBatch<'batch, 'source>,
    observer: &mut O,
) -> Result<i64>
where
    O: ByteArrayValueObserver<'source>,
{
    let unencoded_value_bytes = prepare_plain_values(encoder, values)?;
    let mut encode = PlainEncode { encoder };
    values.try_for_each_observed(observer, &mut encode)?;
    Ok(unencoded_value_bytes)
}

#[inline(never)]
fn encode_delta_length_values<'batch, 'source: 'batch, O>(
    encoder: &mut ByteArrayDeltaLengthEncoder,
    values: ByteArrayBatch<'batch, 'source>,
    observer: &mut O,
) -> Result<i64>
where
    O: ByteArrayValueObserver<'source>,
{
    let mut encode = DeltaLengthEncode {
        encoder,
        unencoded_value_bytes: 0,
    };
    values.try_for_each_observed(observer, &mut encode)?;
    Ok(encode.unencoded_value_bytes)
}

#[inline(never)]
fn encode_delta_values<'batch, 'source: 'batch, O>(
    encoder: &mut ByteArrayDeltaEncoder,
    values: ByteArrayBatch<'batch, 'source>,
    observer: &mut O,
) -> Result<i64>
where
    O: ByteArrayValueObserver<'source>,
{
    observer.observe_batch(values)?;
    let unencoded_value_bytes = match values {
        #[cfg(feature = "arrow")]
        ByteArrayBatch::Offset32 { offsets, data } => encoder.put_values(
            offsets
                .windows(2)
                .map(|w| &data[w[0].as_usize()..w[1].as_usize()]),
        )?,
        #[cfg(feature = "arrow")]
        ByteArrayBatch::Offset64 { offsets, data } => encoder.put_values(
            offsets
                .windows(2)
                .map(|w| &data[w[0].as_usize()..w[1].as_usize()]),
        )?,
        ByteArrayBatch::Gathered(values) => encoder.put_values(values.iter().copied())?,
    };
    Ok(unencoded_value_bytes)
}

/// A batch of byte values supplied to the column encoder.
///
/// Offset arrays retain their borrowed representation. Other sources use a
/// bounded slice of borrowed values.
#[derive(Clone, Copy)]
pub(crate) enum ByteArrayBatch<'batch, 'source> {
    #[cfg(feature = "arrow")]
    Offset32 {
        offsets: &'source [i32],
        data: &'source [u8],
    },
    #[cfg(feature = "arrow")]
    Offset64 {
        offsets: &'source [i64],
        data: &'source [u8],
    },
    Gathered(&'batch [&'source [u8]]),
}

impl<'batch, 'source: 'batch> ByteArrayBatch<'batch, 'source> {
    #[inline(always)]
    pub(crate) fn len(self) -> usize {
        match self {
            #[cfg(feature = "arrow")]
            Self::Offset32 { offsets, .. } => offsets.len().saturating_sub(1),
            #[cfg(feature = "arrow")]
            Self::Offset64 { offsets, .. } => offsets.len().saturating_sub(1),
            Self::Gathered(values) => values.len(),
        }
    }

    /// Returns `(payload_bytes, encoded_bytes)` when it is both O(1) to compute
    /// and the representation guarantees every value length fits a Parquet
    /// `u32` prefix.
    #[inline(always)]
    pub(crate) fn exact_plain_size(self) -> Option<(usize, usize)> {
        match self {
            #[cfg(feature = "arrow")]
            Self::Offset32 { offsets, .. } => {
                let payload = (*offsets.last()? - *offsets.first()?) as usize;
                Some((
                    payload,
                    payload.saturating_add(self.len().saturating_mul(std::mem::size_of::<u32>())),
                ))
            }
            _ => None,
        }
    }

    /// Visit logical values using the batch's physical representation.
    #[inline(always)]
    fn try_for_each<C>(self, consumer: &mut C) -> Result<()>
    where
        C: ByteArrayValueConsumer<'source>,
    {
        match self {
            #[cfg(feature = "arrow")]
            Self::Offset32 { offsets, data } => walk_offsets(offsets, data, consumer),
            #[cfg(feature = "arrow")]
            Self::Offset64 { offsets, data } => walk_offsets(offsets, data, consumer),
            Self::Gathered(values) => walk_gathered(values, consumer),
        }
    }

    /// Observe and consume one batch. Gathered values are observed before they
    /// are consumed; offset spans are observed and consumed together.
    #[inline(always)]
    fn try_for_each_observed<C, O>(self, observer: &mut O, consumer: &mut C) -> Result<()>
    where
        C: ByteArrayValueConsumer<'source>,
        O: ByteArrayValueObserver<'source>,
    {
        match self {
            Self::Gathered(values) => {
                observer.observe_batch(Self::Gathered(values))?;
                walk_gathered(values, consumer)
            }
            #[cfg(feature = "arrow")]
            values => values.try_for_each(&mut ObserveAndConsume { observer, consumer }),
        }
    }
}

#[cfg(feature = "arrow")]
#[inline(always)]
fn walk_offsets<'source, O, C>(
    offsets: &'source [O],
    data: &'source [u8],
    consumer: &mut C,
) -> Result<()>
where
    O: ArrowNativeType,
    C: ByteArrayValueConsumer<'source>,
{
    for window in offsets.windows(2) {
        consumer.consume(&data[window[0].as_usize()..window[1].as_usize()])?;
    }
    Ok(())
}

#[inline(always)]
fn walk_gathered<'batch, 'source: 'batch, C>(
    values: &'batch [&'source [u8]],
    consumer: &mut C,
) -> Result<()>
where
    C: ByteArrayValueConsumer<'source>,
{
    for &value in values {
        consumer.consume(value)?;
    }
    Ok(())
}

/// Maximum number of non-contiguous values in one gathered batch.
pub(crate) const BYTE_ARRAY_BATCH_VALUES: usize = 64;

/// Produces byte values as bounded gathered batches or contiguous offset ranges.
pub(crate) trait ByteArraySource<'source>: ValueProducer<&'source [u8]> {
    /// Whether this source carries native run groups.
    #[inline]
    fn is_grouped(self) -> bool {
        false
    }

    #[inline(always)]
    fn write_flat_to(self, sink: &mut ByteArraySink<'source, '_>) -> Result<()> {
        gather_tiled::<BYTE_ARRAY_BATCH_VALUES, _, _, _>(self, |values| {
            sink.push_batch(ByteArrayBatch::Gathered(values))
        })
    }

    #[inline(always)]
    fn write_run_groups_to(self, sink: &mut ByteArraySink<'source, '_>) -> Result<()> {
        self.write_run_groups_fallback_to(sink)
    }

    fn write_run_groups_fallback_to(self, sink: &mut ByteArraySink<'source, '_>) -> Result<()> {
        gather_run_groups_tiled::<BYTE_ARRAY_BATCH_VALUES, _, _>(self, |values, counts| {
            sink.push_batch(RunBatch { values, counts })
        })
    }
}

/// Destination selected once for a complete byte-value write.
pub(crate) enum ByteArraySinkTarget<'encoder> {
    Dictionary(&'encoder mut DictEncoder<ByteArrayType>),
    Fallback(&'encoder mut ByteArrayEncodingFamily),
}

/// Accumulates encoding observations and unencoded value bytes for one write.
///
/// Borrowed min/max values are copied into the column statistics only after the
/// complete source succeeds.
pub(crate) struct ByteArraySink<'source, 'encoder> {
    pub(crate) collect_stats: bool,
    pub(crate) order: ByteMinMaxOrder,
    pub(crate) min: Option<&'source [u8]>,
    pub(crate) max: Option<&'source [u8]>,
    pub(crate) unencoded_value_bytes: i64,
    pub(crate) accumulator: Option<&'encoder mut Box<dyn GeoStatsAccumulator>>,
    pub(crate) bloom: Option<&'encoder mut Sbbf>,
    pub(crate) target: ByteArraySinkTarget<'encoder>,
}

struct ByteArrayObserver<'source, 'state> {
    collect_stats: bool,
    order: ByteMinMaxOrder,
    min: &'state mut Option<&'source [u8]>,
    max: &'state mut Option<&'source [u8]>,
    accumulator: Option<&'state mut (dyn GeoStatsAccumulator + 'static)>,
    bloom: Option<&'state mut Sbbf>,
}

struct ObserveMinMax<'source, 'state> {
    order: ByteMinMaxOrder,
    min: &'state mut Option<&'source [u8]>,
    max: &'state mut Option<&'source [u8]>,
}

impl<'source> ByteArrayValueConsumer<'source> for ObserveMinMax<'source, '_> {
    #[inline(always)]
    fn consume(&mut self, value: &'source [u8]) -> Result<()> {
        <ByteMinMax as MinMaxStrategy<'_>>::observe(self.order, value, self.min, self.max);
        Ok(())
    }
}

struct ObserveGeo<'state> {
    accumulator: &'state mut (dyn GeoStatsAccumulator + 'static),
}

impl<'source> ByteArrayValueConsumer<'source> for ObserveGeo<'_> {
    #[inline(always)]
    fn consume(&mut self, value: &'source [u8]) -> Result<()> {
        if self.accumulator.is_valid() {
            self.accumulator.update_wkb(value);
        }
        Ok(())
    }
}

struct ObserveBloom<'state> {
    bloom: &'state mut Sbbf,
}

impl<'source> ByteArrayValueConsumer<'source> for ObserveBloom<'_> {
    #[inline(always)]
    fn consume(&mut self, value: &'source [u8]) -> Result<()> {
        self.bloom.insert(value);
        Ok(())
    }
}

impl<'source> ByteArrayObserver<'source, '_> {
    #[inline(always)]
    fn observe_batch<'batch>(&mut self, values: ByteArrayBatch<'batch, 'source>) -> Result<()>
    where
        'source: 'batch,
    {
        if self.collect_stats {
            if let Some(accumulator) = self.accumulator.as_deref_mut() {
                values.try_for_each(&mut ObserveGeo { accumulator })?;
            } else {
                values.try_for_each(&mut ObserveMinMax {
                    order: self.order,
                    min: self.min,
                    max: self.max,
                })?;
            }
        }
        if let Some(bloom) = self.bloom.as_deref_mut() {
            values.try_for_each(&mut ObserveBloom { bloom })?;
        }
        Ok(())
    }
}

impl<'source> ByteArrayValueObserver<'source> for ByteArrayObserver<'source, '_> {
    #[cfg(feature = "arrow")]
    #[inline(always)]
    fn observe(&mut self, value: &'source [u8]) {
        if self.collect_stats {
            if let Some(accumulator) = self.accumulator.as_mut() {
                if accumulator.is_valid() {
                    accumulator.update_wkb(value);
                }
            } else {
                <ByteMinMax as MinMaxStrategy<'_>>::observe(self.order, value, self.min, self.max);
            }
        }
        if let Some(bloom) = self.bloom.as_mut() {
            bloom.insert(value);
        }
    }

    #[inline(always)]
    fn observe_batch<'batch>(&mut self, values: ByteArrayBatch<'batch, 'source>) -> Result<()>
    where
        'source: 'batch,
    {
        ByteArrayObserver::observe_batch(self, values)
    }
}

impl<'source, 'encoder> ByteArraySink<'source, 'encoder> {
    #[inline(always)]
    #[cfg(feature = "arrow")]
    fn parts(
        &mut self,
    ) -> (
        ByteArrayObserver<'source, '_>,
        &mut ByteArraySinkTarget<'encoder>,
        &mut i64,
    ) {
        (
            ByteArrayObserver {
                collect_stats: self.collect_stats,
                order: self.order,
                min: &mut self.min,
                max: &mut self.max,
                accumulator: self
                    .accumulator
                    .as_deref_mut()
                    .map(|accumulator| accumulator.as_mut()),
                bloom: self.bloom.as_deref_mut(),
            },
            &mut self.target,
            &mut self.unencoded_value_bytes,
        )
    }
}

impl<'batch, 'source: 'batch> BatchSink<ByteArrayBatch<'batch, 'source>>
    for ByteArraySink<'source, '_>
{
    #[inline(always)]
    fn push_batch(&mut self, values: ByteArrayBatch<'batch, 'source>) -> Result<()> {
        let Self {
            collect_stats,
            order,
            min,
            max,
            unencoded_value_bytes,
            accumulator,
            bloom,
            target,
        } = self;
        let mut observer = ByteArrayObserver {
            collect_stats: *collect_stats,
            order: *order,
            min,
            max,
            accumulator: accumulator
                .as_deref_mut()
                .map(|accumulator| accumulator.as_mut()),
            bloom: bloom.as_deref_mut(),
        };
        let batch_unencoded_value_bytes = match target {
            ByteArraySinkTarget::Dictionary(encoder) => {
                encoder.encode_values_observed(values, &mut observer)
            }
            ByteArraySinkTarget::Fallback(encoder) => {
                encoder.encode_values_observed(values, &mut observer)
            }
        }?;
        *unencoded_value_bytes += batch_unencoded_value_bytes;
        Ok(())
    }
}

impl<'source> ByteArraySink<'source, '_> {
    /// Whether dictionary encoding is active for this write window.
    #[cfg(feature = "arrow")]
    #[inline]
    pub(crate) fn is_dictionary(&self) -> bool {
        matches!(self.target, ByteArraySinkTarget::Dictionary(_))
    }

    /// Encode a mapped Arrow dictionary source, caching the Parquet dictionary
    /// index by Arrow physical value index for the lifetime of this binding.
    #[cfg(feature = "arrow")]
    #[inline]
    pub(crate) fn push_dictionary_source(
        &mut self,
        indices: impl ValueProducer<usize>,
        grouped: bool,
        value: impl Fn(usize) -> &'source [u8] + Copy,
    ) -> Result<()> {
        let (mut observer, target, unencoded_value_bytes) = self.parts();
        let ByteArraySinkTarget::Dictionary(encoder) = target else {
            unreachable!("Arrow dictionary cache selected without a dictionary encoder")
        };
        let mut source_bytes = 0;
        let mut push = |index, count| {
            let bytes = value(index);
            observer.observe(bytes);
            byte_array_length(bytes.len())?;
            encoder.put_arrow_dictionary(index, grouped.then_some(count), |dictionary| {
                Ok(Interner::intern(dictionary, bytes))
            })?;
            source_bytes += (bytes.len() as i64).saturating_mul(count as i64);
            Ok::<(), ParquetError>(())
        };
        if grouped {
            indices.for_each_run_group(&mut push)?;
        } else {
            indices.try_for_each(|index| push(index, 1))?;
        }
        *unencoded_value_bytes += source_bytes;
        Ok(())
    }

    /// Encode a selection over inline Arrow byte-view descriptors without
    /// first gathering temporary `&[u8]` handles.
    ///
    /// The raw descriptors provide both an order-preserving comparison key and
    /// the complete Parquet PLAIN bytes. The generic path remains responsible
    /// for indirect views and non-standard byte ordering.
    #[cfg(feature = "arrow")]
    #[inline]
    pub(crate) fn try_push_inline_view_source(
        &mut self,
        indices: impl ValueProducer<usize>,
        views: &'source [u128],
        value: impl Fn(usize) -> &'source [u8] + Copy,
    ) -> Result<bool> {
        // Arrow strings and binary values use unsigned byte ordering. Keep
        // DECIMAL/FLOAT16, geospatial accumulation, disabled statistics, and
        // delta encodings on the generic byte-value path.
        if !self.collect_stats
            || self.order != ByteMinMaxOrder::Unsigned
            || self.accumulator.is_some()
            || !matches!(
                self.target,
                ByteArraySinkTarget::Dictionary(_)
                    | ByteArraySinkTarget::Fallback(ByteArrayEncodingFamily::Plain(_))
            )
        {
            return Ok(false);
        }

        let Self {
            order,
            min,
            max,
            unencoded_value_bytes,
            bloom,
            target,
            ..
        } = self;

        let mut payload_bytes = 0usize;
        let mut native_min: Option<(u128, usize)> = None;
        let mut native_max: Option<(u128, usize)> = None;

        match target {
            ByteArraySinkTarget::Dictionary(encoder) => {
                indices.try_for_each(|index| {
                    let raw = views[index];
                    payload_bytes = payload_bytes.saturating_add(raw as u32 as usize);
                    let key = inline_view_key(raw);
                    if native_min.is_none_or(|(current, _)| key < current) {
                        native_min = Some((key, index));
                    }
                    if native_max.is_none_or(|(current, _)| key > current) {
                        native_max = Some((key, index));
                    }
                    let bytes = value(index);
                    if let Some(bloom) = bloom.as_deref_mut() {
                        bloom.insert(bytes);
                    }
                    encoder.put_value_bytes(bytes, || bytes.to_vec().into())
                })?;
            }
            ByteArraySinkTarget::Fallback(ByteArrayEncodingFamily::Plain(encoder)) => {
                indices.try_for_each(|index| {
                    let raw = views[index];
                    payload_bytes = payload_bytes.saturating_add(raw as u32 as usize);
                    let key = inline_view_key(raw);
                    if native_min.is_none_or(|(current, _)| key < current) {
                        native_min = Some((key, index));
                    }
                    if native_max.is_none_or(|(current, _)| key > current) {
                        native_max = Some((key, index));
                    }
                    if let Some(bloom) = bloom.as_deref_mut() {
                        bloom.insert(value(index));
                    }
                    Ok::<(), ParquetError>(())
                })?;

                encoder.reserve(
                    payload_bytes.saturating_add(indices.len().saturating_mul(size_of::<u32>())),
                );
                indices.try_for_each(|index| {
                    encoder.put_inline_view(views[index]);
                    Ok::<(), ParquetError>(())
                })?;
            }
            ByteArraySinkTarget::Fallback(_) => {
                unreachable!("unsupported view sink target was filtered above")
            }
        }

        // Only the two winning native extrema need slice resolution.
        if let Some((_, index)) = native_min {
            <ByteMinMax as MinMaxStrategy<'_>>::observe(*order, value(index), min, max);
        }
        if let Some((_, index)) = native_max {
            <ByteMinMax as MinMaxStrategy<'_>>::observe(*order, value(index), min, max);
        }
        *unencoded_value_bytes += payload_bytes as i64;
        Ok(true)
    }

    /// Stream a non-contiguous source directly while dictionary encoding is
    /// active. Fallback encoders retain their bounded gathered path.
    #[cfg(feature = "arrow")]
    #[inline]
    pub(crate) fn push_source(&mut self, values: impl ValueProducer<&'source [u8]>) -> Result<()> {
        if matches!(self.target, ByteArraySinkTarget::Fallback(_)) {
            return gather_tiled::<BYTE_ARRAY_BATCH_VALUES, _, _, _>(values, |values| {
                self.push_batch(ByteArrayBatch::Gathered(values))
            });
        }

        let (mut observer, target, unencoded_value_bytes) = self.parts();
        let ByteArraySinkTarget::Dictionary(encoder) = target else {
            unreachable!()
        };
        let mut source_bytes = 0;
        values.try_for_each(|value| {
            observer.observe(value);
            encoder.put_value_bytes(value, || value.to_vec().into())?;
            source_bytes += value.len() as i64;
            Ok::<(), ParquetError>(())
        })?;
        *unencoded_value_bytes += source_bytes;
        Ok(())
    }

    #[inline(never)]
    fn push_grouped_batch(&mut self, values: &[&'source [u8]], counts: &[usize]) -> Result<()> {
        debug_assert!(self.accumulator.is_none());
        if self.collect_stats {
            let order = self.order;
            for &value in values {
                <ByteMinMax as MinMaxStrategy<'_>>::observe(
                    order,
                    value,
                    &mut self.min,
                    &mut self.max,
                );
            }
        }
        if let Some(bloom) = self.bloom.as_deref_mut() {
            for &value in values {
                bloom.insert(value);
            }
        }

        let ByteArraySinkTarget::Dictionary(encoder) = &mut self.target else {
            unreachable!("grouped byte batch emitted without a dictionary encoder")
        };
        for (&value, &count) in values.iter().zip(counts) {
            encoder.put_value_bytes_run(value, count, || value.to_vec().into())?;
            self.unencoded_value_bytes = self
                .unencoded_value_bytes
                .saturating_add((value.len() as i64).saturating_mul(count as i64));
        }
        Ok(())
    }
}

/// The order-preserving key used by Arrow for inline byte views.
#[cfg(feature = "arrow")]
#[inline(always)]
fn inline_view_key(raw: u128) -> u128 {
    (raw.swap_bytes() << 32) | raw as u32 as u128
}

impl<'batch, 'source: 'batch> BatchSink<RunBatch<'batch, &'source [u8]>>
    for ByteArraySink<'source, '_>
{
    #[inline(always)]
    fn push_batch(&mut self, values: RunBatch<'batch, &'source [u8]>) -> Result<()> {
        self.push_grouped_batch(values.values, values.counts)
    }
}

/// Independently allocated byte values supplied by the low-level slice API are
/// already a dense source descriptor.
impl<'a, T: AsBytes> ValueProducer<&'a [u8]> for &'a [T] {
    #[inline]
    fn len(self) -> usize {
        <[T]>::len(self)
    }

    #[inline]
    fn fill(&mut self, out: &mut [MaybeUninit<&'a [u8]>]) -> usize {
        let filled = <[T]>::len(self).min(out.len());
        let (head, tail) = self.split_at(filled);
        *self = tail;
        for (slot, value) in out.iter_mut().zip(head) {
            slot.write(value.as_bytes());
        }
        filled
    }

    #[inline]
    fn try_for_each<E>(self, mut f: impl FnMut(&'a [u8]) -> Result<(), E>) -> Result<(), E> {
        for value in self {
            f(value.as_bytes())?;
        }
        Ok(())
    }
}

impl<'a, T: AsBytes> ByteArraySource<'a> for &'a [T] {}

/// The byte-array encoder drives one write-scoped [`ByteArraySink`]. Contiguous
/// offset-array spans cross it as native borrowed batches; non-contiguous
/// sources use bounded gathered batches. The sink retains borrowed min/max and
/// the unencoded value-byte total until the complete source has been encoded.
impl<D: DataType<T = ByteArray>> TypedColumnChunkEncoder<D> {
    #[cfg(feature = "arrow")]
    pub(crate) fn has_arrow_dictionary(&self, index: usize) -> bool {
        match &self.encoding_family {
            ByteArrayEncodingFamily::Dictionary(dict) => dict.has_arrow_dictionary(index),
            _ => false,
        }
    }

    /// Encode a selected byte-value source, preserving native run groups for
    /// dictionary encoding when the source and active observations allow it.
    pub(crate) fn write_byte_array_source<'a, S>(&mut self, values: S) -> Result<()>
    where
        S: ByteArraySource<'a>,
    {
        let len = values.len();
        let collect_stats = self.statistics_enabled != EnabledStatistics::None;
        let use_grouped_dictionary_encoding = values.is_grouped()
            && <ByteArrayEncodingFamily as EncodingFamily<D>>::is_dictionary(&self.encoding_family)
            && self.geo_stats_accumulator.is_none();

        let (min, max, unencoded_value_bytes) = {
            // Hash dictionary entries only when the dictionary is flushed.
            let bloom = if self.has_dictionary() {
                None
            } else {
                self.bloom_filter.as_mut()
            };
            let target = match &mut self.encoding_family {
                ByteArrayEncodingFamily::Dictionary(dict_encoder) => {
                    // A flat write materializes any pending run-buffered
                    // indices before the first new value and reserves once for
                    // the complete logical source.
                    if !use_grouped_dictionary_encoding {
                        dict_encoder.reserve(len);
                    }
                    ByteArraySinkTarget::Dictionary(dict_encoder)
                }
                other => ByteArraySinkTarget::Fallback(other),
            };
            let mut sink = ByteArraySink {
                collect_stats,
                order: ByteMinMaxOrder::from_descr(&self.descr),
                min: None,
                max: None,
                unencoded_value_bytes: 0,
                accumulator: self.geo_stats_accumulator.as_mut(),
                bloom,
                target,
            };
            if use_grouped_dictionary_encoding {
                values.write_run_groups_to(&mut sink)?;
            } else {
                values.write_flat_to(&mut sink)?;
            }
            (sink.min, sink.max, sink.unencoded_value_bytes)
        };

        self.num_values += len;
        self.merge_page_minmax(min, max);
        // This feeds the offset index even when ordinary statistics are
        // disabled.
        *self.variable_length_bytes.get_or_insert(0) += unencoded_value_bytes;
        Ok(())
    }

    /// Merge page extrema, sharing storage when they refer to the same value.
    fn merge_page_minmax(&mut self, min: Option<&[u8]>, max: Option<&[u8]>) {
        let order = ByteMinMaxOrder::from_descr(&self.descr);
        let min = min.filter(|min| {
            self.min_value
                .as_ref()
                .is_none_or(|m| <ByteMinMax as MinMaxStrategy<'_>>::greater(order, m.data(), min))
        });
        let max = max.filter(|max| {
            self.max_value
                .as_ref()
                .is_none_or(|m| <ByteMinMax as MinMaxStrategy<'_>>::greater(order, max, m.data()))
        });
        let owned_min = min.map(<ByteMinMax as MinMaxStrategy<'_>>::to_owned);
        if let Some(max) = max {
            self.max_value = Some(if min.is_some_and(|min| std::ptr::eq(min, max)) {
                owned_min.as_ref().unwrap().clone()
            } else {
                <ByteMinMax as MinMaxStrategy<'_>>::to_owned(max)
            });
        }
        if let Some(min) = owned_min {
            self.min_value = Some(min);
        }
    }
}

/// Slice `write_batch(&[ByteArray])`: drive the dense values through the same
/// `write_byte_array_source` engine as Arrow input. The values are separate
/// `ByteArray`s (not a contiguous packed run), so this takes the bounded
/// gathered path.
pub(super) fn encode_byte_slice<D: DataType<T = ByteArray>>(
    enc: &mut TypedColumnChunkEncoder<D>,
    values: &[ByteArray],
) -> Result<()> {
    enc.write_byte_array_source(values)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "arrow")]
    use arrow_array::{Array, BinaryViewArray};

    #[test]
    fn fallback_memory_size_accounts_for_retained_allocations() {
        let values = [
            ByteArray::from("common-prefix-a"),
            ByteArray::from("common-prefix-b"),
        ];
        let encoders = [
            ByteArrayEncodingFamily::Plain(Default::default()),
            ByteArrayEncodingFamily::DeltaLength(Default::default()),
            ByteArrayEncodingFamily::Delta(Default::default()),
        ];

        for mut encoder in encoders {
            <ByteArrayEncodingFamily as Encoder<ByteArrayType>>::put(&mut encoder, &values)
                .unwrap();
            let encoded = Encoder::<ByteArrayType>::estimated_data_encoded_size(&encoder);
            assert!(Encoder::<ByteArrayType>::estimated_memory_size(&encoder) >= encoded);
            Encoder::<ByteArrayType>::flush_buffer(&mut encoder).unwrap();
            assert_eq!(
                Encoder::<ByteArrayType>::estimated_data_encoded_size(&encoder),
                0
            );
        }
    }

    #[cfg(feature = "arrow")]
    #[test]
    fn inline_view_key_matches_byte_lexical_order() {
        let input: &[&[u8]] = &[
            b"",
            b"a",
            b"a\0",
            b"aa",
            b"abcd",
            b"abcde",
            b"abcdefghijk",
            b"abcdefghijkl",
            b"b",
            &[0x7f],
            &[0x80],
            &[0xff],
        ];
        let values = BinaryViewArray::from_iter_values(input.iter().copied());
        assert!(values.lengths().all(|len| len <= 12));

        for left in 0..values.len() {
            for right in 0..values.len() {
                assert_eq!(
                    inline_view_key(values.views()[left])
                        .cmp(&inline_view_key(values.views()[right])),
                    values.value(left).cmp(values.value(right)),
                );
            }
        }
    }

    #[cfg(feature = "arrow")]
    #[test]
    fn inline_view_plain_encoding_matches_value_encoding() {
        let input: &[&[u8]] = &[b"", b"a", b"four", b"abcdefghijkl", b"tail"];
        let values = BinaryViewArray::from_iter_values(input.iter().copied());
        let mut views = ByteArrayPlainEncoder::default();
        let mut ordinary = ByteArrayPlainEncoder::default();

        for index in 0..values.len() {
            views.put_inline_view(values.views()[index]);
            ordinary.put_value(values.value(index));
        }

        assert_eq!(views.flush_buffer(), ordinary.flush_buffer());
    }
}
