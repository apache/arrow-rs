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
//! Sources gather borrowed values into bounded `&[u8]` batches.

use std::mem::MaybeUninit;

use super::{ColumnChunkEncoder, MinMaxStrategy, TypedColumnChunkEncoder};
use crate::basic::{ConvertedType, LogicalType};
use crate::bloom_filter::Sbbf;
use crate::column::value_batch::{BatchSink, ValueProducer, gather_tiled};
use crate::column::writer::{compare_greater_byte_array_decimals, compare_greater_f16};
use crate::data_type::private::byte_array_length;
use crate::data_type::{AsBytes, ByteArray, ByteArrayType, DataType};
#[cfg(test)]
use crate::encodings::encoding::Encoder;
use crate::encodings::encoding::{
    ByteArrayDeltaEncoder, ByteArrayDeltaLengthEncoder, ByteArrayEncodingFamily,
    ByteArrayPlainEncoder, DictEncoder,
};
use crate::errors::Result;
use crate::file::properties::EnabledStatistics;
use crate::geospatial::accumulator::GeoStatsAccumulator;
use crate::schema::types::ColumnDescriptor;
/// Observation performed alongside byte-array encoding.
trait ByteArrayValueObserver<'a> {
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
    // Validate value lengths before mutating the encoding buffer.
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
        ByteArrayBatch::Gathered(values) => encoder.put_values(values.iter().copied())?,
    };
    Ok(unencoded_value_bytes)
}

/// A batch of byte values supplied to the column encoder.
///
/// Values are supplied as a bounded slice of borrowed payloads.
#[derive(Clone, Copy)]
pub(crate) enum ByteArrayBatch<'batch, 'source> {
    Gathered(&'batch [&'source [u8]]),
}

impl<'batch, 'source: 'batch> ByteArrayBatch<'batch, 'source> {
    #[inline(always)]
    pub(crate) fn len(self) -> usize {
        match self {
            Self::Gathered(values) => values.len(),
        }
    }

    /// Returns `(payload_bytes, encoded_bytes)` when it is both O(1) to compute
    /// and the representation guarantees every value length fits a Parquet
    /// `u32` prefix.
    #[inline(always)]
    pub(crate) fn exact_plain_size(self) -> Option<(usize, usize)> {
        None
    }

    /// Visit logical values using the batch's physical representation.
    #[inline(always)]
    fn try_for_each<C>(self, consumer: &mut C) -> Result<()>
    where
        C: ByteArrayValueConsumer<'source>,
    {
        match self {
            Self::Gathered(values) => walk_gathered(values, consumer),
        }
    }

    /// Observe one gathered batch before consuming its values.
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
        }
    }
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

/// Produces byte values as bounded gathered batches.
pub(crate) trait ByteArraySource<'source>: ValueProducer<&'source [u8]> {
    #[inline(always)]
    fn write_flat_to(self, sink: &mut ByteArraySink<'source, '_>) -> Result<()> {
        gather_tiled::<BYTE_ARRAY_BATCH_VALUES, _, _, _>(self, |values| {
            sink.push_batch(ByteArrayBatch::Gathered(values))
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
    #[inline(always)]
    fn observe_batch<'batch>(&mut self, values: ByteArrayBatch<'batch, 'source>) -> Result<()>
    where
        'source: 'batch,
    {
        ByteArrayObserver::observe_batch(self, values)
    }
}

impl ByteArraySink<'_, '_> {}

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

/// The byte-array encoder drives one write-scoped [`ByteArraySink`] with
/// bounded gathered batches. The sink retains borrowed min/max and the
/// unencoded value-byte total until the complete source has been encoded.
impl<D: DataType<T = ByteArray>> TypedColumnChunkEncoder<D> {
    /// Encode a selected byte-value source.
    pub(crate) fn write_byte_array_source<'a, S>(&mut self, values: S) -> Result<()>
    where
        S: ByteArraySource<'a>,
    {
        let len = values.len();
        let collect_stats = self.statistics_enabled != EnabledStatistics::None;

        let (min, max, unencoded_value_bytes) = {
            // Hash dictionary entries only when the dictionary is flushed.
            let bloom = if self.has_dictionary() {
                None
            } else {
                self.bloom_filter.as_mut()
            };
            let target = match &mut self.encoding_family {
                ByteArrayEncodingFamily::Dictionary(dict_encoder) => {
                    // Reserve once for the complete logical source.
                    dict_encoder.reserve(len);
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
            values.write_flat_to(&mut sink)?;
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

/// Slice `write_batch(&[ByteArray])`: drive borrowed values through
/// `write_byte_array_source` in bounded gathered batches.
pub(super) fn encode_byte_slice<D: DataType<T = ByteArray>>(
    enc: &mut TypedColumnChunkEncoder<D>,
    values: &[ByteArray],
) -> Result<()> {
    enc.write_byte_array_source(values)
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
