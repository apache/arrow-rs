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

use crate::basic::Encoding;
use crate::bloom_filter::Sbbf;
use crate::column::writer::encoder::{
    ColumnValueEncoder, DataPageValues, DictionaryPage, create_bloom_filter,
};
use crate::data_type::{AsBytes, ByteArray, Int32Type};
use crate::encodings::encoding::{DeltaBitPackEncoder, Encoder};
use crate::encodings::rle::RleEncoder;
use crate::errors::{ParquetError, Result};
use crate::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use crate::geospatial::accumulator::{GeoStatsAccumulator, try_new_geo_stats_accumulator};
use crate::geospatial::statistics::GeospatialStatistics;
use crate::schema::types::ColumnDescPtr;
use crate::util::bit_util::num_required_bits;
use crate::util::interner::{Interner, Storage};
use arrow_array::types::ByteArrayType;
use arrow_array::{
    Array, ArrayAccessor, BinaryArray, BinaryViewArray, DictionaryArray, FixedSizeBinaryArray,
    GenericByteArray, LargeBinaryArray, LargeStringArray, StringArray, StringViewArray,
};
use arrow_buffer::ArrowNativeType;
use arrow_schema::DataType;

macro_rules! downcast_dict_impl {
    ($array:ident, $key:ident, $val:ident, $op:expr $(, $arg:expr)*) => {{
        $op($array
            .as_any()
            .downcast_ref::<DictionaryArray<arrow_array::types::$key>>()
            .unwrap()
            .downcast_dict::<$val>()
            .unwrap()$(, $arg)*)
    }};
}

macro_rules! downcast_dict_op {
    ($key_type:expr, $val:ident, $array:ident, $op:expr $(, $arg:expr)*) => {
        match $key_type.as_ref() {
            DataType::UInt8 => downcast_dict_impl!($array, UInt8Type, $val, $op$(, $arg)*),
            DataType::UInt16 => downcast_dict_impl!($array, UInt16Type, $val, $op$(, $arg)*),
            DataType::UInt32 => downcast_dict_impl!($array, UInt32Type, $val, $op$(, $arg)*),
            DataType::UInt64 => downcast_dict_impl!($array, UInt64Type, $val, $op$(, $arg)*),
            DataType::Int8 => downcast_dict_impl!($array, Int8Type, $val, $op$(, $arg)*),
            DataType::Int16 => downcast_dict_impl!($array, Int16Type, $val, $op$(, $arg)*),
            DataType::Int32 => downcast_dict_impl!($array, Int32Type, $val, $op$(, $arg)*),
            DataType::Int64 => downcast_dict_impl!($array, Int64Type, $val, $op$(, $arg)*),
            _ => unreachable!(),
        }
    };
}

macro_rules! downcast_op {
    ($data_type:expr, $array:ident, $op:expr $(, $arg:expr)*) => {
        match $data_type {
            DataType::Utf8 => $op($array.as_any().downcast_ref::<StringArray>().unwrap()$(, $arg)*),
            DataType::LargeUtf8 => {
                $op($array.as_any().downcast_ref::<LargeStringArray>().unwrap()$(, $arg)*)
            }
            DataType::Utf8View => $op($array.as_any().downcast_ref::<StringViewArray>().unwrap()$(, $arg)*),
            DataType::Binary => {
                $op($array.as_any().downcast_ref::<BinaryArray>().unwrap()$(, $arg)*)
            }
            DataType::LargeBinary => {
                $op($array.as_any().downcast_ref::<LargeBinaryArray>().unwrap()$(, $arg)*)
            }
            DataType::BinaryView => {
                $op($array.as_any().downcast_ref::<BinaryViewArray>().unwrap()$(, $arg)*)
            }
            DataType::Dictionary(key, value) => match value.as_ref() {
                DataType::Utf8 => downcast_dict_op!(key, StringArray, $array, $op$(, $arg)*),
                DataType::LargeUtf8 => {
                    downcast_dict_op!(key, LargeStringArray, $array, $op$(, $arg)*)
                }
                DataType::Binary => downcast_dict_op!(key, BinaryArray, $array, $op$(, $arg)*),
                DataType::LargeBinary => {
                    downcast_dict_op!(key, LargeBinaryArray, $array, $op$(, $arg)*)
                }
                DataType::FixedSizeBinary(_) => {
                    downcast_dict_op!(key, FixedSizeBinaryArray, $array, $op$(, $arg)*)
                }
                d => unreachable!("cannot downcast {} dictionary value to byte array", d),
            },
            d => unreachable!("cannot downcast {} to byte array", d),
        }
    };
}

/// A fallback encoder, i.e. non-dictionary, for [`ByteArray`]
struct FallbackEncoder {
    encoder: FallbackEncoderImpl,
    num_values: usize,
    variable_length_bytes: i64,
}

/// The fallback encoder in use
///
/// Note: DeltaBitPackEncoder is boxed as it is rather large
enum FallbackEncoderImpl {
    Plain {
        buffer: Vec<u8>,
    },
    DeltaLength {
        buffer: Vec<u8>,
        lengths: Box<DeltaBitPackEncoder<Int32Type>>,
    },
    Delta {
        buffer: Vec<u8>,
        last_value: Vec<u8>,
        prefix_lengths: Box<DeltaBitPackEncoder<Int32Type>>,
        suffix_lengths: Box<DeltaBitPackEncoder<Int32Type>>,
    },
}

impl FallbackEncoder {
    /// Create the fallback encoder for the given [`ColumnDescPtr`] and [`WriterProperties`]
    fn new(descr: &ColumnDescPtr, props: &WriterProperties) -> Result<Self> {
        // Set either main encoder or fallback encoder.
        let encoding =
            props
                .encoding(descr.path())
                .unwrap_or_else(|| match props.writer_version() {
                    WriterVersion::PARQUET_1_0 => Encoding::PLAIN,
                    WriterVersion::PARQUET_2_0 => Encoding::DELTA_BYTE_ARRAY,
                });

        let encoder = match encoding {
            Encoding::PLAIN => FallbackEncoderImpl::Plain { buffer: vec![] },
            Encoding::DELTA_LENGTH_BYTE_ARRAY => FallbackEncoderImpl::DeltaLength {
                buffer: vec![],
                lengths: Box::new(DeltaBitPackEncoder::new()),
            },
            Encoding::DELTA_BYTE_ARRAY => FallbackEncoderImpl::Delta {
                buffer: vec![],
                last_value: vec![],
                prefix_lengths: Box::new(DeltaBitPackEncoder::new()),
                suffix_lengths: Box::new(DeltaBitPackEncoder::new()),
            },
            _ => {
                return Err(general_err!(
                    "unsupported encoding {} for byte array",
                    encoding
                ));
            }
        };

        Ok(Self {
            encoder,
            num_values: 0,
            variable_length_bytes: 0,
        })
    }

    /// Encode `values` to the in-progress page
    fn encode<T>(&mut self, values: T, indices: impl ExactSizeIterator<Item = usize>)
    where
        T: ArrayAccessor + Copy,
        T::Item: AsRef<[u8]>,
    {
        self.num_values += indices.len();
        match &mut self.encoder {
            FallbackEncoderImpl::Plain { buffer } => {
                for idx in indices {
                    let value = values.value(idx);
                    let value = value.as_ref();
                    buffer.extend_from_slice((value.len() as u32).as_bytes());
                    buffer.extend_from_slice(value);
                    self.variable_length_bytes += value.len() as i64;
                }
            }
            FallbackEncoderImpl::DeltaLength { buffer, lengths } => {
                for idx in indices {
                    let value = values.value(idx);
                    let value = value.as_ref();
                    lengths.put(&[value.len() as i32]).unwrap();
                    buffer.extend_from_slice(value);
                    self.variable_length_bytes += value.len() as i64;
                }
            }
            FallbackEncoderImpl::Delta {
                buffer,
                last_value,
                prefix_lengths,
                suffix_lengths,
            } => {
                for idx in indices {
                    let value = values.value(idx);
                    let value = value.as_ref();
                    let mut prefix_length = 0;

                    while prefix_length < last_value.len()
                        && prefix_length < value.len()
                        && last_value[prefix_length] == value[prefix_length]
                    {
                        prefix_length += 1;
                    }

                    let suffix_length = value.len() - prefix_length;

                    last_value.clear();
                    last_value.extend_from_slice(value);

                    buffer.extend_from_slice(&value[prefix_length..]);
                    prefix_lengths.put(&[prefix_length as i32]).unwrap();
                    suffix_lengths.put(&[suffix_length as i32]).unwrap();
                    self.variable_length_bytes += value.len() as i64;
                }
            }
        }
    }

    /// Encode a contiguous range from an offset-based byte array
    fn encode_dense<T>(&mut self, values: &GenericByteArray<T>, offset: usize, len: usize)
    where
        T: ByteArrayType,
    {
        self.num_values += len;
        let offsets = values.value_offsets();
        let data = values.value_data();
        let end = offset + len;

        let first_byte = offsets[offset].as_usize();
        let last_byte = offsets[end].as_usize();
        let total_bytes = last_byte - first_byte;

        match &mut self.encoder {
            FallbackEncoderImpl::Plain { buffer } => {
                buffer.reserve(total_bytes.saturating_add(len.saturating_mul(4)));
                for idx in offset..end {
                    let value = dense_byte_value::<T>(offsets, data, idx);
                    buffer.extend_from_slice((value.len() as u32).as_bytes());
                    buffer.extend_from_slice(value);
                    self.variable_length_bytes += value.len() as i64;
                }
            }
            FallbackEncoderImpl::DeltaLength { buffer, lengths } => {
                buffer.reserve(total_bytes);
                for idx in offset..end {
                    let value = dense_byte_value::<T>(offsets, data, idx);
                    lengths.put(&[value.len() as i32]).unwrap();
                    buffer.extend_from_slice(value);
                    self.variable_length_bytes += value.len() as i64;
                }
            }
            FallbackEncoderImpl::Delta {
                buffer,
                last_value,
                prefix_lengths,
                suffix_lengths,
            } => {
                buffer.reserve(total_bytes);
                for idx in offset..end {
                    let value = dense_byte_value::<T>(offsets, data, idx);
                    let mut prefix_length = 0;

                    while prefix_length < last_value.len()
                        && prefix_length < value.len()
                        && last_value[prefix_length] == value[prefix_length]
                    {
                        prefix_length += 1;
                    }

                    let suffix_length = value.len() - prefix_length;

                    last_value.clear();
                    last_value.extend_from_slice(value);

                    buffer.extend_from_slice(&value[prefix_length..]);
                    prefix_lengths.put(&[prefix_length as i32]).unwrap();
                    suffix_lengths.put(&[suffix_length as i32]).unwrap();
                    self.variable_length_bytes += value.len() as i64;
                }
            }
        }
    }

    /// Returns an estimate of the data page size in bytes
    ///
    /// This includes:
    /// <already_written_encoded_byte_size> + <estimated_encoded_size_of_unflushed_bytes>
    fn estimated_data_page_size(&self) -> usize {
        match &self.encoder {
            FallbackEncoderImpl::Plain { buffer, .. } => buffer.len(),
            FallbackEncoderImpl::DeltaLength { buffer, lengths } => {
                buffer.len() + lengths.estimated_data_encoded_size()
            }
            FallbackEncoderImpl::Delta {
                buffer,
                prefix_lengths,
                suffix_lengths,
                ..
            } => {
                buffer.len()
                    + prefix_lengths.estimated_data_encoded_size()
                    + suffix_lengths.estimated_data_encoded_size()
            }
        }
    }

    fn flush_data_page(
        &mut self,
        min_value: Option<ByteArray>,
        max_value: Option<ByteArray>,
    ) -> Result<DataPageValues<ByteArray>> {
        let (buf, encoding) = match &mut self.encoder {
            FallbackEncoderImpl::Plain { buffer } => (std::mem::take(buffer), Encoding::PLAIN),
            FallbackEncoderImpl::DeltaLength { buffer, lengths } => {
                let lengths = lengths.flush_buffer()?;

                let mut out = Vec::with_capacity(lengths.len() + buffer.len());
                out.extend_from_slice(&lengths);
                out.extend_from_slice(buffer);
                buffer.clear();
                (out, Encoding::DELTA_LENGTH_BYTE_ARRAY)
            }
            FallbackEncoderImpl::Delta {
                buffer,
                prefix_lengths,
                suffix_lengths,
                last_value,
            } => {
                let prefix_lengths = prefix_lengths.flush_buffer()?;
                let suffix_lengths = suffix_lengths.flush_buffer()?;

                let mut out =
                    Vec::with_capacity(prefix_lengths.len() + suffix_lengths.len() + buffer.len());
                out.extend_from_slice(&prefix_lengths);
                out.extend_from_slice(&suffix_lengths);
                out.extend_from_slice(buffer);
                buffer.clear();
                last_value.clear();
                (out, Encoding::DELTA_BYTE_ARRAY)
            }
        };

        // Capture value of variable_length_bytes and reset for next page
        let variable_length_bytes = Some(self.variable_length_bytes);
        self.variable_length_bytes = 0;

        Ok(DataPageValues {
            buf: buf.into(),
            num_values: std::mem::take(&mut self.num_values),
            encoding,
            min_value,
            max_value,
            variable_length_bytes,
        })
    }
}

/// [`Storage`] for the [`Interner`] used by [`DictEncoder`]
#[derive(Debug, Default)]
struct ByteArrayStorage {
    /// Encoded dictionary data
    page: Vec<u8>,

    values: Vec<std::ops::Range<usize>>,
}

impl Storage for ByteArrayStorage {
    type Key = u64;
    type Value = [u8];

    fn get(&self, idx: Self::Key) -> &Self::Value {
        &self.page[self.values[idx as usize].clone()]
    }

    fn push(&mut self, value: &Self::Value) -> Self::Key {
        let key = self.values.len();

        self.page.reserve(4 + value.len());
        self.page.extend_from_slice((value.len() as u32).as_bytes());

        let start = self.page.len();
        self.page.extend_from_slice(value);
        self.values.push(start..self.page.len());

        key as u64
    }

    #[allow(dead_code)] // not used in parquet_derive, so is dead there
    fn estimated_memory_size(&self) -> usize {
        self.page.capacity() * std::mem::size_of::<u8>()
            + self.values.capacity() * std::mem::size_of::<std::ops::Range<usize>>()
    }
}

/// A dictionary encoder for byte array data
#[derive(Debug, Default)]
struct DictEncoder {
    interner: Interner<ByteArrayStorage>,
    indices: Vec<u64>,
    variable_length_bytes: i64,
}

impl DictEncoder {
    /// Encode `values` to the in-progress page
    fn encode<T>(&mut self, values: T, indices: impl ExactSizeIterator<Item = usize>)
    where
        T: ArrayAccessor + Copy,
        T::Item: AsRef<[u8]>,
    {
        self.indices.reserve(indices.len());

        for idx in indices {
            let value = values.value(idx);
            let interned = self.interner.intern(value.as_ref());
            self.indices.push(interned);
            self.variable_length_bytes += value.as_ref().len() as i64;
        }
    }

    /// Encode a contiguous range from an offset-based byte array
    fn encode_dense<T>(&mut self, values: &GenericByteArray<T>, offset: usize, len: usize)
    where
        T: ByteArrayType,
    {
        self.indices.reserve(len);

        let offsets = values.value_offsets();
        let data = values.value_data();
        for idx in offset..offset + len {
            let value = dense_byte_value::<T>(offsets, data, idx);
            let interned = self.interner.intern(value);
            self.indices.push(interned);
            self.variable_length_bytes += value.len() as i64;
        }
    }

    fn bit_width(&self) -> u8 {
        let length = self.interner.storage().values.len();
        num_required_bits(length.saturating_sub(1) as u64)
    }

    fn estimated_memory_size(&self) -> usize {
        self.interner.estimated_memory_size() + self.indices.capacity() * std::mem::size_of::<u64>()
    }

    fn estimated_data_page_size(&self) -> usize {
        let bit_width = self.bit_width();
        1 + RleEncoder::max_buffer_size(bit_width, self.indices.len())
    }

    fn estimated_dict_page_size(&self) -> usize {
        self.interner.storage().page.len()
    }

    fn flush_dict_page(self) -> DictionaryPage {
        let storage = self.interner.into_inner();

        DictionaryPage {
            buf: storage.page.into(),
            num_values: storage.values.len(),
            is_sorted: false,
        }
    }

    fn flush_data_page(
        &mut self,
        min_value: Option<ByteArray>,
        max_value: Option<ByteArray>,
    ) -> DataPageValues<ByteArray> {
        let num_values = self.indices.len();
        let buffer_len = self.estimated_data_page_size();
        let mut buffer = Vec::with_capacity(buffer_len);
        buffer.push(self.bit_width());

        let mut encoder = RleEncoder::new_from_buf(self.bit_width(), buffer);
        for index in &self.indices {
            encoder.put(*index)
        }

        self.indices.clear();

        // Capture value of variable_length_bytes and reset for next page
        let variable_length_bytes = Some(self.variable_length_bytes);
        self.variable_length_bytes = 0;

        DataPageValues {
            buf: encoder.consume().into(),
            num_values,
            encoding: Encoding::RLE_DICTIONARY,
            min_value,
            max_value,
            variable_length_bytes,
        }
    }
}

pub struct ByteArrayEncoder {
    fallback: FallbackEncoder,
    dict_encoder: Option<DictEncoder>,
    statistics_enabled: EnabledStatistics,
    min_value: Option<ByteArray>,
    max_value: Option<ByteArray>,
    bloom_filter: Option<Sbbf>,
    bloom_filter_target_fpp: f64,
    geo_stats_accumulator: Option<Box<dyn GeoStatsAccumulator>>,
}

impl ColumnValueEncoder for ByteArrayEncoder {
    type T = ByteArray;
    type Values = dyn Array;
    fn flush_bloom_filter(&mut self) -> Option<Sbbf> {
        let mut sbbf = self.bloom_filter.take()?;
        sbbf.fold_to_target_fpp(self.bloom_filter_target_fpp);
        Some(sbbf)
    }

    fn try_new(descr: &ColumnDescPtr, props: &WriterProperties) -> Result<Self>
    where
        Self: Sized,
    {
        let dictionary = props
            .dictionary_enabled(descr.path())
            .then(DictEncoder::default);

        let fallback = FallbackEncoder::new(descr, props)?;

        let (bloom_filter, bloom_filter_target_fpp) = create_bloom_filter(props, descr)?;

        let statistics_enabled = props.statistics_enabled(descr.path());

        let geo_stats_accumulator = try_new_geo_stats_accumulator(descr);

        Ok(Self {
            fallback,
            statistics_enabled,
            bloom_filter,
            bloom_filter_target_fpp,
            dict_encoder: dictionary,
            min_value: None,
            max_value: None,
            geo_stats_accumulator,
        })
    }

    fn write(&mut self, values: &Self::Values, offset: usize, len: usize) -> Result<()> {
        match values.data_type() {
            DataType::Utf8 => encode_dense(
                values.as_any().downcast_ref::<StringArray>().unwrap(),
                offset,
                len,
                self,
            ),
            DataType::LargeUtf8 => encode_dense(
                values.as_any().downcast_ref::<LargeStringArray>().unwrap(),
                offset,
                len,
                self,
            ),
            DataType::Binary => encode_dense(
                values.as_any().downcast_ref::<BinaryArray>().unwrap(),
                offset,
                len,
                self,
            ),
            DataType::LargeBinary => encode_dense(
                values.as_any().downcast_ref::<LargeBinaryArray>().unwrap(),
                offset,
                len,
                self,
            ),
            _ => {
                downcast_op!(
                    values.data_type(),
                    values,
                    encode,
                    offset..offset + len,
                    self
                );
            }
        }
        Ok(())
    }

    fn write_gather(&mut self, values: &Self::Values, indices: &[usize]) -> Result<()> {
        downcast_op!(
            values.data_type(),
            values,
            encode,
            indices.iter().copied(),
            self
        );
        Ok(())
    }

    fn num_values(&self) -> usize {
        match &self.dict_encoder {
            Some(encoder) => encoder.indices.len(),
            None => self.fallback.num_values,
        }
    }

    fn has_dictionary(&self) -> bool {
        self.dict_encoder.is_some()
    }

    fn estimated_memory_size(&self) -> usize {
        let encoder_size = match &self.dict_encoder {
            Some(encoder) => encoder.estimated_memory_size(),
            // For the FallbackEncoder, these unflushed bytes are already encoded.
            // Therefore, the size should be the same as estimated_data_page_size.
            None => self.fallback.estimated_data_page_size(),
        };

        let bloom_filter_size = self
            .bloom_filter
            .as_ref()
            .map(|bf| bf.estimated_memory_size())
            .unwrap_or_default();

        let stats_size = self.min_value.as_ref().map(|v| v.len()).unwrap_or_default()
            + self.max_value.as_ref().map(|v| v.len()).unwrap_or_default();

        encoder_size + bloom_filter_size + stats_size
    }

    fn estimated_dict_page_size(&self) -> Option<usize> {
        Some(self.dict_encoder.as_ref()?.estimated_dict_page_size())
    }

    /// Returns an estimate of the data page size in bytes
    ///
    /// This includes:
    /// <already_written_encoded_byte_size> + <estimated_encoded_size_of_unflushed_bytes>
    fn estimated_data_page_size(&self) -> usize {
        match &self.dict_encoder {
            Some(encoder) => encoder.estimated_data_page_size(),
            None => self.fallback.estimated_data_page_size(),
        }
    }

    fn flush_dict_page(&mut self) -> Result<Option<DictionaryPage>> {
        match self.dict_encoder.take() {
            Some(encoder) => {
                if !encoder.indices.is_empty() {
                    return Err(general_err!(
                        "Must flush data pages before flushing dictionary"
                    ));
                }

                Ok(Some(encoder.flush_dict_page()))
            }
            _ => Ok(None),
        }
    }

    fn flush_data_page(&mut self) -> Result<DataPageValues<ByteArray>> {
        let min_value = self.min_value.take();
        let max_value = self.max_value.take();

        match &mut self.dict_encoder {
            Some(encoder) => Ok(encoder.flush_data_page(min_value, max_value)),
            _ => self.fallback.flush_data_page(min_value, max_value),
        }
    }

    fn flush_geospatial_statistics(&mut self) -> Option<Box<GeospatialStatistics>> {
        self.geo_stats_accumulator.as_mut().map(|a| a.finish())?
    }
}

#[inline]
fn dense_byte_value<'a, T>(offsets: &[T::Offset], data: &'a [u8], idx: usize) -> &'a [u8]
where
    T: ByteArrayType,
{
    let start = offsets[idx].as_usize();
    let end = offsets[idx + 1].as_usize();
    &data[start..end]
}

/// Encodes the provided `values` and `indices` to `encoder`
///
/// This is a free function so it can be used with `downcast_op!`
fn encode<T, I>(values: T, indices: I, encoder: &mut ByteArrayEncoder)
where
    T: ArrayAccessor + Copy,
    T::Item: Copy + Ord + AsRef<[u8]>,
    I: ExactSizeIterator<Item = usize> + Clone,
{
    if encoder.statistics_enabled != EnabledStatistics::None {
        update_statistics(encoder, byte_values(values, indices.clone()));
    }

    // encode the values into bloom filter if enabled
    if let Some(bloom_filter) = &mut encoder.bloom_filter {
        update_bloom_filter(bloom_filter, byte_values(values, indices.clone()));
    }

    match &mut encoder.dict_encoder {
        Some(dict_encoder) => dict_encoder.encode(values, indices),
        None => encoder.fallback.encode(values, indices),
    }
}

/// Encodes a contiguous range from an offset-based byte array
fn encode_dense<T>(
    values: &GenericByteArray<T>,
    offset: usize,
    len: usize,
    encoder: &mut ByteArrayEncoder,
) where
    T: ByteArrayType,
{
    if len == 0 {
        return;
    }

    if encoder.statistics_enabled != EnabledStatistics::None {
        update_statistics(encoder, dense_byte_values(values, offset, len));
    }

    if let Some(bloom_filter) = &mut encoder.bloom_filter {
        update_bloom_filter(bloom_filter, dense_byte_values(values, offset, len));
    }

    match &mut encoder.dict_encoder {
        Some(dict_encoder) => dict_encoder.encode_dense(values, offset, len),
        None => encoder.fallback.encode_dense(values, offset, len),
    }
}

#[inline]
fn byte_values<T, I>(values: T, indices: I) -> impl Iterator<Item = T::Item>
where
    T: ArrayAccessor + Copy,
    I: Iterator<Item = usize>,
{
    indices.map(move |idx| values.value(idx))
}

#[inline]
fn dense_byte_values<T>(
    values: &GenericByteArray<T>,
    offset: usize,
    len: usize,
) -> impl ExactSizeIterator<Item = &[u8]>
where
    T: ByteArrayType,
{
    let offsets = values.value_offsets();
    let data = values.value_data();
    (offset..offset + len).map(move |idx| dense_byte_value::<T>(offsets, data, idx))
}

#[inline]
fn update_statistics<T>(encoder: &mut ByteArrayEncoder, values: impl Iterator<Item = T>)
where
    T: Copy + Ord + AsRef<[u8]>,
{
    if let Some(accumulator) = encoder.geo_stats_accumulator.as_mut() {
        update_geo_stats_accumulator(accumulator.as_mut(), values);
    } else if let Some((min, max)) = compute_min_max(values) {
        if encoder.min_value.as_ref().is_none_or(|m| m > &min) {
            encoder.min_value = Some(min);
        }

        if encoder.max_value.as_ref().is_none_or(|m| m < &max) {
            encoder.max_value = Some(max);
        }
    }
}

#[inline]
fn update_bloom_filter<T>(bloom_filter: &mut Sbbf, values: impl Iterator<Item = T>)
where
    T: AsRef<[u8]>,
{
    for value in values {
        bloom_filter.insert(value.as_ref());
    }
}

/// Computes the min and max for the provided values
#[inline]
fn compute_min_max<T>(mut values: impl Iterator<Item = T>) -> Option<(ByteArray, ByteArray)>
where
    T: Copy + Ord + AsRef<[u8]>,
{
    let first_val = values.next()?;
    let mut min = first_val;
    let mut max = first_val;
    for val in values {
        min = min.min(val);
        max = max.max(val);
    }
    Some((min.as_ref().to_vec().into(), max.as_ref().to_vec().into()))
}

/// Updates geospatial statistics for the provided values
#[inline]
fn update_geo_stats_accumulator<T>(
    bounder: &mut dyn GeoStatsAccumulator,
    values: impl Iterator<Item = T>,
) where
    T: AsRef<[u8]>,
{
    if bounder.is_valid() {
        for value in values {
            bounder.update_wkb(value.as_ref());
        }
    }
}
