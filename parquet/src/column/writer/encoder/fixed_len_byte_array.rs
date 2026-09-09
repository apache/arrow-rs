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

//! Fixed-length byte-array sources, batches, and write-scoped encoding state.

use super::*;
#[cfg(feature = "arrow")]
use crate::encodings::encoding::DictionaryStorage;

/// Retains a source's physical fixed-length byte-array layout until the active encoder is known.
pub(crate) trait FixedLenByteArraySource {
    fn len(&self) -> usize;
    fn is_grouped(&self) -> bool {
        false
    }
    /// Validate every emitted value before encoding or updating observers.
    /// Uniform-width sources can check their physical representation once.
    fn validate_width(&self, expected_width: usize) -> Result<()>;
    fn write_to(self, sink: &mut FixedLenByteArraySink<'_>) -> Result<()>;
}

enum FixedLenByteArraySinkTarget<'a> {
    Dictionary(&'a mut DictEncoder<FixedLenByteArrayType>),
    Fallback(&'a mut FixedLenByteArrayEncodingFamily),
}

/// Write-scoped fixed-length byte-array state shared by every bounded batch emitted for one input.
pub(crate) struct FixedLenByteArraySink<'a> {
    target: FixedLenByteArraySinkTarget<'a>,
    observer: FixedLenByteArrayObserver<'a>,
}

struct FixedLenByteArrayObserver<'a> {
    descr: &'a ColumnDescriptor,
    bloom: Option<&'a mut Sbbf>,
    scratch: &'a mut FixedLenByteArrayScratch,
    nan_count: &'a mut Option<u64>,
    collect_stats: bool,
    /// Whether this column is Float16, resolved once so the NaN test costs a
    /// branch on a bool rather than a `LogicalType` comparison per value.
    /// Grouped with the other flags so the struct keeps its layout.
    is_float16: bool,
    has_min: bool,
    has_max: bool,
}

/// Widest Arrow logical value computed into fixed-length bytes (decimal256).
#[cfg(feature = "arrow")]
pub(crate) const FIXED_LEN_BYTE_ARRAY_MAX_WIDTH: usize = 32;
/// Number of fixed-length values or run groups per stack batch.
#[cfg(feature = "arrow")]
pub(crate) const FIXED_LEN_BYTE_ARRAY_BATCH_VALUES: usize = 64;

#[cfg(feature = "arrow")]
pub(crate) struct FixedLenByteArrayBatchPacker<'sink, 'encoder> {
    sink: &'sink mut FixedLenByteArraySink<'encoder>,
    tile: [u8; FIXED_LEN_BYTE_ARRAY_BATCH_VALUES * FIXED_LEN_BYTE_ARRAY_MAX_WIDTH],
    width: usize,
    filled: usize,
}

#[cfg(feature = "arrow")]
impl<'sink, 'encoder> FixedLenByteArrayBatchPacker<'sink, 'encoder> {
    #[inline]
    pub(crate) fn new(sink: &'sink mut FixedLenByteArraySink<'encoder>, width: usize) -> Self {
        Self {
            sink,
            tile: [0; FIXED_LEN_BYTE_ARRAY_BATCH_VALUES * FIXED_LEN_BYTE_ARRAY_MAX_WIDTH],
            width,
            filled: 0,
        }
    }

    #[inline]
    pub(crate) fn push(&mut self, fill: impl FnOnce(&mut [u8])) -> Result<()> {
        let offset = self.filled * self.width;
        let end = offset + self.width;
        fill(&mut self.tile[offset..end]);
        self.filled += 1;
        if self.filled == FIXED_LEN_BYTE_ARRAY_BATCH_VALUES {
            self.flush()?;
        }
        Ok(())
    }

    #[inline]
    pub(crate) fn finish(mut self) -> Result<()> {
        self.flush()
    }

    fn flush(&mut self) -> Result<()> {
        let len = self.filled * self.width;
        self.sink.push_batch(FixedLenByteArrayBatch::Packed(
            PackedFixedLenByteArrayBatch::new(&self.tile[..len], self.width, self.filled),
        ))?;
        self.filled = 0;
        Ok(())
    }
}

impl<D: DataType<T = FixedLenByteArray>> TypedColumnChunkEncoder<D> {
    #[inline]
    pub(crate) fn write_fixed_len_byte_array_source(
        &mut self,
        values: impl FixedLenByteArraySource,
    ) -> Result<()> {
        values.validate_width(self.descr.type_length() as usize)?;
        let len = values.len();
        let grouped = values.is_grouped();
        self.num_values += len;
        let bloom = if self.has_dictionary() {
            None
        } else {
            self.bloom_filter.as_mut()
        };
        let target = match &mut self.encoding_family {
            FixedLenByteArrayEncodingFamily::Dictionary(dict) => {
                if !grouped {
                    dict.reserve(len);
                }
                FixedLenByteArraySinkTarget::Dictionary(dict)
            }
            encoder => {
                encoder.reserve_fixed_len(
                    (self.descr.type_length().max(0) as usize).saturating_mul(len),
                );
                FixedLenByteArraySinkTarget::Fallback(encoder)
            }
        };

        self.fixed_len_byte_array_scratch.min.clear();
        self.fixed_len_byte_array_scratch.max.clear();
        let collect_stats = self.statistics_enabled != EnabledStatistics::None
            && self.descr.converted_type() != ConvertedType::INTERVAL;
        let (has_min, has_max) = {
            let mut sink = FixedLenByteArraySink {
                target,
                observer: FixedLenByteArrayObserver {
                    descr: self.descr.as_ref(),
                    bloom,
                    scratch: &mut self.fixed_len_byte_array_scratch,
                    nan_count: &mut self.nan_count,
                    collect_stats,
                    is_float16: matches!(self.descr.logical_type_ref(), Some(LogicalType::Float16)),
                    has_min: false,
                    has_max: false,
                },
            };
            values.write_to(&mut sink)?;
            (sink.observer.has_min, sink.observer.has_max)
        };

        if has_min && has_max {
            let (min, max) = raw_fixed_len_min_max_values(
                &self.descr,
                &self.fixed_len_byte_array_scratch.min,
                &self.fixed_len_byte_array_scratch.max,
            );
            update_min(&self.descr, &min, &mut self.min_value);
            update_max(&self.descr, &max, &mut self.max_value);
        }
        Ok(())
    }
}

impl FixedLenByteArrayObserver<'_> {
    #[inline(always)]
    fn is_nan(&self, value: &[u8]) -> bool {
        self.is_float16 && is_f16_nan(value)
    }

    #[inline(always)]
    fn merge_extrema(&mut self, min: &[u8], max: &[u8], are_nan: bool) {
        let update_min = !self.has_min
            || match (self.is_nan(&self.scratch.min), are_nan) {
                (false, true) => false,
                (true, false) => true,
                _ => compare_greater_byte_array(self.descr, &self.scratch.min, min),
            };
        if update_min {
            self.scratch.min.clear();
            self.scratch.min.extend_from_slice(min);
            self.has_min = true;
        }
        let update_max = !self.has_max
            || match (self.is_nan(&self.scratch.max), are_nan) {
                (false, true) => false,
                (true, false) => true,
                _ => compare_greater_byte_array(self.descr, max, &self.scratch.max),
            };
        if update_max {
            self.scratch.max.clear();
            self.scratch.max.extend_from_slice(max);
            self.has_max = true;
        }
    }

    #[inline(always)]
    fn observe(&mut self, value: &[u8], multiplicity: usize) {
        if self.collect_stats {
            let value_is_nan = self.is_nan(value);
            if self.is_float16 {
                let count = self.nan_count.get_or_insert(0);
                if value_is_nan {
                    *count += multiplicity as u64;
                }
            }
            self.merge_extrema(value, value, value_is_nan);
        }
        if let Some(bloom) = self.bloom.as_deref_mut() {
            bloom.insert(value);
        }
    }
}

impl FixedLenByteArraySink<'_> {
    #[inline(never)]
    fn encode_packed(&mut self, values: PackedFixedLenByteArrayBatch<'_>) -> Result<()> {
        if matches!(self.target, FixedLenByteArraySinkTarget::Dictionary(_)) {
            self.encode_dictionary_packed(values)
        } else {
            self.encode_fallback_packed(values)
        }
    }

    fn encode_dictionary_packed(&mut self, values: PackedFixedLenByteArrayBatch<'_>) -> Result<()> {
        for value in values.iter() {
            self.observer.observe(value, 1);
            let FixedLenByteArraySinkTarget::Dictionary(dict) = &mut self.target else {
                unreachable!()
            };
            dict.put_value_bytes(value, || value.to_vec().into())?;
        }
        Ok(())
    }

    #[inline(never)]
    fn encode_fallback_packed(&mut self, values: PackedFixedLenByteArrayBatch<'_>) -> Result<()> {
        debug_assert!(matches!(
            self.target,
            FixedLenByteArraySinkTarget::Fallback(_)
        ));
        if self.observer.collect_stats || self.observer.bloom.is_some() {
            let observer = &mut self.observer;
            let descr = observer.descr;
            let is_float16 = observer.is_float16;
            let collect_stats = observer.collect_stats;
            let mut extrema: Option<(&[u8], &[u8], bool)> = None;
            let mut tile_nan_count = 0_u64;
            {
                let mut bloom = observer.bloom.as_deref_mut();
                for value in values.iter() {
                    if collect_stats {
                        let value_is_nan = is_float16 && is_f16_nan(value);
                        tile_nan_count += value_is_nan as u64;
                        match extrema.as_mut() {
                            None => extrema = Some((value, value, value_is_nan)),
                            Some((min, max, are_nan)) => match (*are_nan, value_is_nan) {
                                (false, true) => {}
                                (true, false) => {
                                    *min = value;
                                    *max = value;
                                    *are_nan = false;
                                }
                                _ if compare_greater_byte_array(descr, min, value) => *min = value,
                                _ if compare_greater_byte_array(descr, value, max) => *max = value,
                                _ => {}
                            },
                        }
                    }
                    if let Some(bloom) = bloom.as_deref_mut() {
                        bloom.insert(value);
                    }
                }
            }
            if collect_stats {
                if is_float16 {
                    *observer.nan_count.get_or_insert(0) += tile_nan_count;
                }
                if let Some((min, max, are_nan)) = extrema {
                    observer.merge_extrema(min, max, are_nan);
                }
            }
        }
        match &mut self.target {
            FixedLenByteArraySinkTarget::Fallback(encoder) => {
                encoder.put_fixed_len_byte_array_batch(values)
            }
            FixedLenByteArraySinkTarget::Dictionary(_) => unreachable!(),
        }
    }

    #[inline]
    pub(crate) fn push_selected<'a>(&mut self, values: impl ValueProducer<&'a [u8]>) -> Result<()> {
        let observer = &mut self.observer;
        // Take the filter out of the observer — so `observe` handles statistics
        // only — and feed it through a batch instead. Keeping the block updates
        // together can help overlap their cache misses; see [`Sbbf::batch`].
        let mut bloom = observer.bloom.take();
        let mut batch = bloom.as_deref_mut().map(|bloom| bloom.batch());
        let result = match &mut self.target {
            FixedLenByteArraySinkTarget::Dictionary(dict) => values.try_for_each(|value| {
                observer.observe(value, 1);
                if let Some(batch) = batch.as_mut() {
                    batch.insert(value);
                }
                dict.put_value_bytes(value, || value.to_vec().into())
            }),
            FixedLenByteArraySinkTarget::Fallback(encoder) => values.try_for_each(|value| {
                observer.observe(value, 1);
                if let Some(batch) = batch.as_mut() {
                    batch.insert(value);
                }
                encoder.append_fixed_len_value(value)
            }),
        };
        drop(batch);
        observer.bloom = bloom;
        result
    }
}

#[cfg_attr(not(feature = "arrow"), allow(dead_code))]
pub(crate) enum FixedLenByteArrayBatch<'a> {
    Packed(PackedFixedLenByteArrayBatch<'a>),
}

impl<'batch> BatchSink<FixedLenByteArrayBatch<'batch>> for FixedLenByteArraySink<'_> {
    #[inline(never)]
    fn push_batch(&mut self, values: FixedLenByteArrayBatch<'batch>) -> Result<()> {
        match values {
            FixedLenByteArrayBatch::Packed(values) => self.encode_packed(values),
        }
    }
}

impl FixedLenByteArraySource for &[FixedLenByteArray] {
    fn len(&self) -> usize {
        <[FixedLenByteArray]>::len(self)
    }

    fn validate_width(&self, expected_width: usize) -> Result<()> {
        if let Some(value) = self
            .iter()
            .find(|value| value.data().len() != expected_width)
        {
            return Err(general_err!(
                "Mismatched FixedLenByteArray sizes: {} != {}",
                value.data().len(),
                expected_width
            ));
        }
        Ok(())
    }

    fn write_to(self, sink: &mut FixedLenByteArraySink<'_>) -> Result<()> {
        sink.push_selected(self)
    }
}

pub(super) fn encode_fixed_len_byte_array_slice<D: DataType<T = FixedLenByteArray>>(
    enc: &mut TypedColumnChunkEncoder<D>,
    values: &[FixedLenByteArray],
) -> Result<()> {
    enc.write_fixed_len_byte_array_source(values)
}

fn raw_fixed_len_min_max_values(
    _descr: &ColumnDescriptor,
    min: &[u8],
    max: &[u8],
) -> (FixedLenByteArray, FixedLenByteArray) {
    (min.to_vec().into(), max.to_vec().into())
}

#[cfg(feature = "arrow")]
impl FixedLenByteArraySink<'_> {
    /// Consume a reused physical source, or pre-observe it for fallback.
    #[cfg(feature = "arrow")]
    pub(crate) fn try_consume_physical_source(
        &mut self,
        physical_len: usize,
        indices: impl ValueProducer<usize>,
        width: usize,
        write_at: impl Fn(usize, &mut [u8]),
    ) -> Result<bool> {
        debug_assert!(width <= FIXED_LEN_BYTE_ARRAY_MAX_WIDTH);
        // NaN counts follow logical multiplicity, so the unique-physical-value
        // shortcut cannot be used for Float16 statistics.
        if self.observer.collect_stats && self.observer.is_float16 {
            return Ok(false);
        }
        let dictionary = matches!(self.target, FixedLenByteArraySinkTarget::Dictionary(_));
        let observe = self.observer.collect_stats || self.observer.bloom.is_some();
        if !dictionary && (!observe || physical_len > u64::BITS as usize) {
            return Ok(false);
        }
        let mut observed = (physical_len <= u64::BITS as usize).then_some(0_u64);
        let mut value = [0_u8; FIXED_LEN_BYTE_ARRAY_MAX_WIDTH];
        let mut first = |index| {
            observed.as_mut().is_none_or(|observed| {
                let bit = 1_u64 << index;
                let first = *observed & bit == 0;
                *observed |= bit;
                first
            })
        };
        if !dictionary {
            indices.try_for_each(|index| {
                if first(index) {
                    write_at(index, &mut value[..width]);
                    self.observer.observe(&value[..width], 1);
                }
                Ok::<_, ParquetError>(())
            })?;
            self.observer.collect_stats = false;
            self.observer.bloom = None;
            return Ok(false);
        }
        let mut push = |index: usize, count: usize| {
            let mut rendered = false;
            if observe && first(index) {
                write_at(index, &mut value[..width]);
                rendered = true;
                self.observer.observe(&value[..width], count);
            }
            let FixedLenByteArraySinkTarget::Dictionary(dict) = &mut self.target else {
                unreachable!("physical dictionary source requires dictionary encoding")
            };
            let intern = |dictionary: &mut <FixedLenByteArray as DictionaryValue>::Storage| {
                if !rendered {
                    write_at(index, &mut value[..width]);
                }
                let bytes = &value[..width];
                DictionaryStorage::intern_bytes(dictionary, bytes, || bytes.to_vec().into())
            };
            dict.put_arrow_dictionary(index, intern)
        };
        indices.try_for_each(|index| push(index, 1))?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::properties::WriterVersion;
    use crate::schema::types::{ColumnPath, Type as SchemaType};
    use std::sync::Arc;

    #[test]
    fn fixed_width_validation_precedes_encoder_mutation() {
        let descriptor = Arc::new(ColumnDescriptor::new(
            Arc::new(
                SchemaType::primitive_type_builder("fixed", Type::FIXED_LEN_BYTE_ARRAY)
                    .with_length(2)
                    .build()
                    .unwrap(),
            ),
            0,
            0,
            ColumnPath::from("fixed"),
        ));
        for encoding in [
            Encoding::PLAIN,
            Encoding::DELTA_BYTE_ARRAY,
            Encoding::BYTE_STREAM_SPLIT,
        ] {
            for dictionary in [false, true] {
                for observers in [false, true] {
                    let props = WriterProperties::builder()
                        .set_writer_version(WriterVersion::PARQUET_2_0)
                        .set_dictionary_enabled(dictionary)
                        .set_encoding(encoding)
                        .set_statistics_enabled(if observers {
                            EnabledStatistics::Page
                        } else {
                            EnabledStatistics::None
                        })
                        .set_bloom_filter_enabled(observers)
                        .build();
                    let column_props = props.resolve_column_properties(descriptor.path());
                    let new_encoder = || {
                        TypedColumnChunkEncoder::<FixedLenByteArrayType>::try_new(
                            &descriptor,
                            &props,
                            &column_props,
                        )
                        .unwrap()
                    };
                    let mut actual = new_encoder();
                    let mut expected = new_encoder();
                    let first = [FixedLenByteArray::from(vec![10, 11])];
                    actual
                        .write_fixed_len_byte_array_source(first.as_slice())
                        .unwrap();
                    expected
                        .write_fixed_len_byte_array_source(first.as_slice())
                        .unwrap();
                    for width in [1, 3] {
                        // Even a valid prefix must not be emitted before a bad suffix.
                        let invalid = [
                            FixedLenByteArray::from(vec![99, 99]),
                            FixedLenByteArray::from(vec![99; width]),
                        ];
                        let error = actual
                            .write_fixed_len_byte_array_source(invalid.as_slice())
                            .unwrap_err();
                        assert!(
                            error.to_string().contains(&format!(
                                "Mismatched FixedLenByteArray sizes: {width} != 2"
                            )),
                            "{error}"
                        );
                        assert_eq!(actual.num_values(), 1);
                    }
                    actual
                        .write_fixed_len_byte_array_source(&[] as &[FixedLenByteArray])
                        .unwrap();
                    let last = [FixedLenByteArray::from(vec![12, 13])];
                    actual
                        .write_fixed_len_byte_array_source(last.as_slice())
                        .unwrap();
                    expected
                        .write_fixed_len_byte_array_source(last.as_slice())
                        .unwrap();
                    let actual_page = actual.flush_data_page().unwrap();
                    let expected_page = expected.flush_data_page().unwrap();
                    assert_eq!(actual_page.num_values, 2);
                    assert_eq!(actual_page.buf, expected_page.buf);
                    assert_eq!(actual_page.encoding, expected_page.encoding);
                    assert_eq!(actual_page.min_value, expected_page.min_value);
                    assert_eq!(actual_page.max_value, expected_page.max_value);
                    let dictionary_page =
                        |encoder: &mut TypedColumnChunkEncoder<FixedLenByteArrayType>| {
                            encoder
                                .flush_dict_page()
                                .unwrap()
                                .map(|page| (page.num_values, page.buf))
                        };
                    assert_eq!(dictionary_page(&mut actual), dictionary_page(&mut expected));
                    let bloom_bytes =
                        |encoder: &mut TypedColumnChunkEncoder<FixedLenByteArrayType>| {
                            encoder.flush_bloom_filter().map(|bloom| {
                                let mut bytes = Vec::new();
                                bloom.write(&mut bytes).unwrap();
                                bytes
                            })
                        };
                    assert_eq!(bloom_bytes(&mut actual), bloom_bytes(&mut expected));
                }
            }
        }
    }
}
