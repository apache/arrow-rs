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
use crate::column::writer::encoder::{ColumnValueEncoder, DataPageValues, DictionaryPage};
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
use arrow_array::{
    Array, ArrayAccessor, BinaryArray, BinaryViewArray, DictionaryArray, FixedSizeBinaryArray,
    LargeBinaryArray, LargeStringArray, StringArray, StringViewArray,
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

/// Dispatches to `encode_with_remap` for Dictionary types, providing the
/// `row_to_key` closure that maps row indices to dictionary key indices.
macro_rules! downcast_dict_remap {
    ($key_type:expr, $val:ident, $array:ident, $op:expr, $indices:expr, $encoder:expr) => {{
        macro_rules! inner {
            ($kt:ident) => {{
                let dict_array = $array
                    .as_any()
                    .downcast_ref::<DictionaryArray<arrow_array::types::$kt>>()
                    .unwrap();
                let typed = dict_array.downcast_dict::<$val>().unwrap();
                let keys = dict_array.keys();
                let dict_len = dict_array.values().len();
                let row_to_key = |idx: usize| -> usize { keys.value(idx).as_usize() };
                $op(typed, $indices, $encoder, dict_len, &row_to_key)
            }};
        }
        match $key_type.as_ref() {
            DataType::UInt8 => inner!(UInt8Type),
            DataType::UInt16 => inner!(UInt16Type),
            DataType::UInt32 => inner!(UInt32Type),
            DataType::UInt64 => inner!(UInt64Type),
            DataType::Int8 => inner!(Int8Type),
            DataType::Int16 => inner!(Int16Type),
            DataType::Int32 => inner!(Int32Type),
            DataType::Int64 => inner!(Int64Type),
            _ => unreachable!(),
        }
    }};
}

/// Macro that dispatches to `encode_with_remap` for Dictionary data types.
/// For non-Dictionary types, this macro should never be called.
macro_rules! downcast_op_remap {
    ($data_type:expr, $array:ident, $op:expr, $indices:expr, $encoder:expr) => {
        match $data_type {
            DataType::Dictionary(key, value) => match value.as_ref() {
                DataType::Utf8 => {
                    downcast_dict_remap!(key, StringArray, $array, $op, $indices, $encoder)
                }
                DataType::LargeUtf8 => {
                    downcast_dict_remap!(key, LargeStringArray, $array, $op, $indices, $encoder)
                }
                DataType::Binary => {
                    downcast_dict_remap!(key, BinaryArray, $array, $op, $indices, $encoder)
                }
                DataType::LargeBinary => {
                    downcast_dict_remap!(key, LargeBinaryArray, $array, $op, $indices, $encoder)
                }
                DataType::FixedSizeBinary(_) => {
                    downcast_dict_remap!(key, FixedSizeBinaryArray, $array, $op, $indices, $encoder)
                }
                d => unreachable!("cannot downcast {} dictionary value to byte array", d),
            },
            d => unreachable!("downcast_op_remap called with non-dictionary type {}", d),
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
    fn encode<T>(&mut self, values: T, indices: &[usize])
    where
        T: ArrayAccessor + Copy,
        T::Item: AsRef<[u8]>,
    {
        self.num_values += indices.len();
        match &mut self.encoder {
            FallbackEncoderImpl::Plain { buffer } => {
                for idx in indices {
                    let value = values.value(*idx);
                    let value = value.as_ref();
                    buffer.extend_from_slice((value.len() as u32).as_bytes());
                    buffer.extend_from_slice(value);
                    self.variable_length_bytes += value.len() as i64;
                }
            }
            FallbackEncoderImpl::DeltaLength { buffer, lengths } => {
                for idx in indices {
                    let value = values.value(*idx);
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
                    let value = values.value(*idx);
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
    fn encode<T>(&mut self, values: T, indices: &[usize])
    where
        T: ArrayAccessor + Copy,
        T::Item: AsRef<[u8]>,
    {
        self.indices.reserve(indices.len());

        for idx in indices {
            let value = values.value(*idx);
            let interned = self.interner.intern(value.as_ref());
            self.indices.push(interned);
            self.variable_length_bytes += value.as_ref().len() as i64;
        }
    }

    /// Fast path for DictionaryArray input with a lazy remap table.
    ///
    /// Instead of interning each row's value individually (O(N) hash operations),
    /// this method builds a lazy remap table of size O(D) where D is the number
    /// of unique dictionary values actually referenced, then maps each row's key
    /// through the remap table using a simple array index lookup.
    ///
    /// The `row_to_key` closure extracts the dictionary key (as usize) for a given
    /// row index. This avoids allocating a separate `Vec<usize>` for the keys.
    ///
    /// The remap table uses `Vec<Option<u64>>` with lazy population: values are
    /// interned on first encounter and cached for subsequent rows. This ensures
    /// only referenced dictionary values are interned, producing byte-identical
    /// output to the per-row path.
    fn encode_with_remap<T, F>(
        &mut self,
        values: T,
        indices: &[usize],
        dict_len: usize,
        row_to_key: F,
    ) where
        T: ArrayAccessor + Copy,
        T::Item: AsRef<[u8]>,
        F: Fn(usize) -> usize,
    {
        let mut remap: Vec<Option<u64>> = vec![None; dict_len];

        self.indices.reserve(indices.len());
        for &idx in indices {
            let key = row_to_key(idx);
            let interned = match remap[key] {
                Some(cached) => cached,
                None => {
                    let value = values.value(idx);
                    let fresh = self.interner.intern(value.as_ref());
                    remap[key] = Some(fresh);
                    fresh
                }
            };
            self.indices.push(interned);
            let value = values.value(idx);
            self.variable_length_bytes += value.as_ref().len() as i64;
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
    geo_stats_accumulator: Option<Box<dyn GeoStatsAccumulator>>,
}

impl ColumnValueEncoder for ByteArrayEncoder {
    type T = ByteArray;
    type Values = dyn Array;
    fn flush_bloom_filter(&mut self) -> Option<Sbbf> {
        self.bloom_filter.take()
    }

    fn try_new(descr: &ColumnDescPtr, props: &WriterProperties) -> Result<Self>
    where
        Self: Sized,
    {
        let dictionary = props
            .dictionary_enabled(descr.path())
            .then(DictEncoder::default);

        let fallback = FallbackEncoder::new(descr, props)?;

        let bloom_filter = props
            .bloom_filter_properties(descr.path())
            .map(|props| Sbbf::new_with_ndv_fpp(props.ndv, props.fpp))
            .transpose()?;

        let statistics_enabled = props.statistics_enabled(descr.path());

        let geo_stats_accumulator = try_new_geo_stats_accumulator(descr);

        Ok(Self {
            fallback,
            statistics_enabled,
            bloom_filter,
            dict_encoder: dictionary,
            min_value: None,
            max_value: None,
            geo_stats_accumulator,
        })
    }

    fn write(&mut self, _values: &Self::Values, _offset: usize, _len: usize) -> Result<()> {
        unreachable!("should call write_gather instead")
    }

    fn write_gather(&mut self, values: &Self::Values, indices: &[usize]) -> Result<()> {
        // Fast path: when input is a DictionaryArray and dictionary encoding is
        // enabled, use a remap-based approach that replaces O(N) hash operations
        // with O(D) hash operations (D = unique dictionary values) plus O(N)
        // simple array index lookups. Only used when D < N/2 (low cardinality),
        // as the remap table overhead is not worthwhile for high-cardinality
        // dictionaries.
        if let DataType::Dictionary(key_type, _value_type) = values.data_type() {
            if self.dict_encoder.is_some() && self.geo_stats_accumulator.is_none() {
                let dict_len = get_dict_len(values, key_type);
                if dict_len <= indices.len() / 2 {
                    downcast_op_remap!(
                        values.data_type(),
                        values,
                        encode_with_remap,
                        indices,
                        self
                    );
                    return Ok(());
                }
            }
        }

        downcast_op!(values.data_type(), values, encode, indices, self);
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

/// Encodes the provided `values` and `indices` to `encoder`
///
/// This is a free function so it can be used with `downcast_op!`
fn encode<T>(values: T, indices: &[usize], encoder: &mut ByteArrayEncoder)
where
    T: ArrayAccessor + Copy,
    T::Item: Copy + Ord + AsRef<[u8]>,
{
    if encoder.statistics_enabled != EnabledStatistics::None {
        if let Some(accumulator) = encoder.geo_stats_accumulator.as_mut() {
            update_geo_stats_accumulator(accumulator.as_mut(), values, indices.iter().cloned());
        } else if let Some((min, max)) = compute_min_max(values, indices.iter().cloned()) {
            if encoder.min_value.as_ref().is_none_or(|m| m > &min) {
                encoder.min_value = Some(min);
            }

            if encoder.max_value.as_ref().is_none_or(|m| m < &max) {
                encoder.max_value = Some(max);
            }
        }
    }

    // encode the values into bloom filter if enabled
    if let Some(bloom_filter) = &mut encoder.bloom_filter {
        let valid = indices.iter().cloned();
        for idx in valid {
            bloom_filter.insert(values.value(idx).as_ref());
        }
    }

    match &mut encoder.dict_encoder {
        Some(dict_encoder) => dict_encoder.encode(values, indices),
        None => encoder.fallback.encode(values, indices),
    }
}

/// Get the dictionary length from a DictionaryArray, dispatching on key type.
fn get_dict_len(values: &dyn Array, key_type: &Box<DataType>) -> usize {
    macro_rules! get_len {
        ($kt:ident) => {
            values
                .as_any()
                .downcast_ref::<DictionaryArray<arrow_array::types::$kt>>()
                .unwrap()
                .values()
                .len()
        };
    }
    match key_type.as_ref() {
        DataType::Int8 => get_len!(Int8Type),
        DataType::Int16 => get_len!(Int16Type),
        DataType::Int32 => get_len!(Int32Type),
        DataType::Int64 => get_len!(Int64Type),
        DataType::UInt8 => get_len!(UInt8Type),
        DataType::UInt16 => get_len!(UInt16Type),
        DataType::UInt32 => get_len!(UInt32Type),
        DataType::UInt64 => get_len!(UInt64Type),
        _ => unreachable!(),
    }
}

/// Encodes dictionary array values using a remap-based fast path.
///
/// This is equivalent to [`encode`] but optimizes the dictionary encoding step:
/// instead of O(N) hash operations, it uses O(D) hash operations where D is
/// the number of unique dictionary values, plus O(N) simple array lookups.
///
/// Called via `downcast_op!` which dispatches to the appropriate
/// `TypedDictionaryArray` for Dictionary types. The `TypedDictionaryArray`
/// implements `ArrayAccessor`, which transparently resolves dictionary keys
/// to values.
///
/// The `row_to_key` closure extracts the dictionary key (as usize) for a given
/// row index. This is provided by the `downcast_dict_remap_op!` macro.
fn encode_with_remap<T>(
    values: T,
    indices: &[usize],
    encoder: &mut ByteArrayEncoder,
    dict_len: usize,
    row_to_key: &dyn Fn(usize) -> usize,
) where
    T: ArrayAccessor + Copy,
    T::Item: Copy + Ord + AsRef<[u8]>,
{
    // Statistics: use existing per-row computation for correctness
    if encoder.statistics_enabled != EnabledStatistics::None {
        // geo_stats_accumulator is guaranteed None (checked in write_gather)
        if let Some((min, max)) = compute_min_max(values, indices.iter().cloned()) {
            if encoder.min_value.as_ref().is_none_or(|m| m > &min) {
                encoder.min_value = Some(min);
            }
            if encoder.max_value.as_ref().is_none_or(|m| m < &max) {
                encoder.max_value = Some(max);
            }
        }
    }

    // Bloom filter: O(D) insertion using seen-tracking per dictionary key
    if let Some(bloom_filter) = &mut encoder.bloom_filter {
        let mut seen = vec![false; dict_len];
        for &idx in indices {
            let key = row_to_key(idx);
            if !seen[key] {
                seen[key] = true;
                bloom_filter.insert(values.value(idx).as_ref());
            }
        }
    }

    // Dictionary encoding: remap-based fast path
    let dict_encoder = encoder.dict_encoder.as_mut().unwrap();
    dict_encoder.encode_with_remap(values, indices, dict_len, row_to_key);
}

/// Computes the min and max for the provided array and indices
///
/// This is a free function so it can be used with `downcast_op!`
fn compute_min_max<T>(
    array: T,
    mut valid: impl Iterator<Item = usize>,
) -> Option<(ByteArray, ByteArray)>
where
    T: ArrayAccessor,
    T::Item: Copy + Ord + AsRef<[u8]>,
{
    let first_idx = valid.next()?;

    let first_val = array.value(first_idx);
    let mut min = first_val;
    let mut max = first_val;
    for idx in valid {
        let val = array.value(idx);
        min = min.min(val);
        max = max.max(val);
    }
    Some((min.as_ref().to_vec().into(), max.as_ref().to_vec().into()))
}

/// Updates geospatial statistics for the provided array and indices
fn update_geo_stats_accumulator<T>(
    bounder: &mut dyn GeoStatsAccumulator,
    array: T,
    valid: impl Iterator<Item = usize>,
) where
    T: ArrayAccessor,
    T::Item: Copy + Ord + AsRef<[u8]>,
{
    if bounder.is_valid() {
        for idx in valid {
            let val = array.value(idx);
            bounder.update_wkb(val.as_ref());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::builder::StringDictionaryBuilder;
    use arrow_array::cast::AsArray;
    use arrow_array::types::Int32Type;
    use arrow_array::{Array, ArrayAccessor, DictionaryArray, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use bytes::Bytes;

    use crate::arrow::ArrowWriter;
    use crate::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use crate::file::properties::WriterProperties;

    /// Write a single RecordBatch to Parquet bytes using the given properties.
    fn write_batch_to_bytes(batch: &RecordBatch, props: Option<WriterProperties>) -> Bytes {
        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), props).unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();
        buf.into()
    }

    /// Read all rows from Parquet bytes as RecordBatches.
    fn read_batches_from_bytes(data: &Bytes) -> Vec<RecordBatch> {
        let reader = ParquetRecordBatchReaderBuilder::try_new(data.clone())
            .unwrap()
            .build()
            .unwrap();
        reader.collect::<Result<Vec<_>, _>>().unwrap()
    }

    /// Extract string values from a column, handling both StringArray and
    /// DictionaryArray<Int32, Utf8> transparently.
    fn column_to_strings(col: &dyn Array) -> Vec<Option<String>> {
        match col.data_type() {
            DataType::Utf8 => {
                let sa = col.as_string::<i32>();
                (0..sa.len())
                    .map(|i| {
                        if sa.is_null(i) {
                            None
                        } else {
                            Some(sa.value(i).to_string())
                        }
                    })
                    .collect()
            }
            DataType::Dictionary(_, _) => {
                let da = col
                    .as_any()
                    .downcast_ref::<DictionaryArray<Int32Type>>()
                    .unwrap();
                let typed = da.downcast_dict::<StringArray>().unwrap();
                (0..col.len())
                    .map(|i| {
                        if col.is_null(i) {
                            None
                        } else {
                            Some(typed.value(i).to_string())
                        }
                    })
                    .collect()
            }
            other => panic!("Unexpected data type: {other}"),
        }
    }

    // T1: Data equivalence (DictionaryArray vs StringArray)
    //
    // The Parquet files differ in Arrow schema metadata (Utf8 vs Dictionary),
    // but the data pages, dictionary pages, and column statistics must match.
    #[test]
    fn test_dict_passthrough_data_equivalence() {
        use crate::file::reader::FileReader;
        use crate::file::serialized_reader::SerializedFileReader;

        let strings = vec!["alpha", "beta", "alpha", "gamma", "beta"];

        // Plain StringArray
        let plain = StringArray::from(strings.clone());
        let plain_schema = Arc::new(Schema::new(vec![Field::new("col", DataType::Utf8, false)]));
        let plain_batch = RecordBatch::try_new(plain_schema, vec![Arc::new(plain)]).unwrap();

        // DictionaryArray with the same data
        let dict: DictionaryArray<Int32Type> = strings.into_iter().collect();
        let dict_schema = Arc::new(Schema::new(vec![Field::new(
            "col",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        )]));
        let dict_batch = RecordBatch::try_new(dict_schema, vec![Arc::new(dict)]).unwrap();

        let plain_bytes = write_batch_to_bytes(&plain_batch, None);
        let dict_bytes = write_batch_to_bytes(&dict_batch, None);

        // Compare column chunk metadata
        let plain_reader = SerializedFileReader::new(plain_bytes.clone()).unwrap();
        let dict_reader = SerializedFileReader::new(dict_bytes.clone()).unwrap();

        let plain_meta = plain_reader.metadata().row_group(0).column(0);
        let dict_meta = dict_reader.metadata().row_group(0).column(0);

        assert_eq!(plain_meta.statistics(), dict_meta.statistics());
        assert_eq!(plain_meta.num_values(), dict_meta.num_values());
        assert_eq!(plain_meta.compressed_size(), dict_meta.compressed_size());
        assert_eq!(
            plain_meta.uncompressed_size(),
            dict_meta.uncompressed_size()
        );

        // Verify both read back the same logical values
        let pb = read_batches_from_bytes(&plain_bytes);
        let db = read_batches_from_bytes(&dict_bytes);
        let plain_vals = column_to_strings(pb[0].column(0).as_ref());
        let dict_vals = column_to_strings(db[0].column(0).as_ref());
        assert_eq!(plain_vals, dict_vals);
    }

    // T2: Roundtrip DictionaryArray -> read back -> verify values
    #[test]
    fn test_dict_passthrough_roundtrip() {
        let strings = vec!["hello", "world", "hello", "foo", "world", "bar"];
        let dict: DictionaryArray<Int32Type> = strings.iter().copied().collect();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(dict)]).unwrap();

        let bytes = write_batch_to_bytes(&batch, None);
        let batches = read_batches_from_bytes(&bytes);

        assert_eq!(batches.len(), 1);
        let vals = column_to_strings(batches[0].column(0).as_ref());
        let expected: Vec<Option<String>> = strings.iter().map(|s| Some(s.to_string())).collect();
        assert_eq!(vals, expected);
    }

    // T3: Roundtrip DictionaryArray -> verify values match
    #[test]
    fn test_dict_passthrough_roundtrip_to_plain() {
        let strings = vec!["cat", "dog", "cat", "bird"];
        let dict: DictionaryArray<Int32Type> = strings.iter().copied().collect();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(dict)]).unwrap();

        let bytes = write_batch_to_bytes(&batch, None);
        let batches = read_batches_from_bytes(&bytes);

        let vals = column_to_strings(batches[0].column(0).as_ref());
        assert_eq!(
            vals,
            vec![
                Some("cat".into()),
                Some("dog".into()),
                Some("cat".into()),
                Some("bird".into()),
            ]
        );
    }

    // T4: DictionaryArray with null keys
    #[test]
    fn test_dict_passthrough_null_keys() {
        let mut builder = StringDictionaryBuilder::<Int32Type>::new();
        builder.append_value("alpha");
        builder.append_null();
        builder.append_value("beta");
        builder.append_null();
        builder.append_value("alpha");
        let dict = builder.finish();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "col",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(dict)]).unwrap();

        let bytes = write_batch_to_bytes(&batch, None);
        let batches = read_batches_from_bytes(&bytes);

        let vals = column_to_strings(batches[0].column(0).as_ref());
        assert_eq!(
            vals,
            vec![
                Some("alpha".into()),
                None,
                Some("beta".into()),
                None,
                Some("alpha".into()),
            ]
        );
    }

    // T5: Mixed batches (DictionaryArray then StringArray for same column writer)
    #[test]
    fn test_dict_passthrough_mixed_batches() {
        // First batch: DictionaryArray
        let dict: DictionaryArray<Int32Type> = vec!["aaa", "bbb", "aaa"].into_iter().collect();
        let dict_schema = Arc::new(Schema::new(vec![Field::new(
            "col",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        )]));
        let dict_batch = RecordBatch::try_new(dict_schema, vec![Arc::new(dict)]).unwrap();

        // Second batch: plain StringArray (same logical column)
        let plain = StringArray::from(vec!["ccc", "bbb"]);
        let plain_schema = Arc::new(Schema::new(vec![Field::new("col", DataType::Utf8, false)]));
        let plain_batch = RecordBatch::try_new(plain_schema, vec![Arc::new(plain)]).unwrap();

        // Write both batches to same writer using the dict schema
        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, dict_batch.schema(), None).unwrap();
        writer.write(&dict_batch).unwrap();
        writer.write(&plain_batch).unwrap();
        writer.close().unwrap();

        let bytes: Bytes = buf.into();
        let batches = read_batches_from_bytes(&bytes);

        let mut all_values = Vec::new();
        for b in &batches {
            all_values.extend(column_to_strings(b.column(0).as_ref()));
        }
        assert_eq!(
            all_values,
            vec![
                Some("aaa".into()),
                Some("bbb".into()),
                Some("aaa".into()),
                Some("ccc".into()),
                Some("bbb".into()),
            ]
        );
    }

    // T6: Multiple row groups with DictionaryArray input
    #[test]
    fn test_dict_passthrough_multiple_row_groups() {
        let strings1: DictionaryArray<Int32Type> = vec!["x", "y", "z", "x"].into_iter().collect();
        let strings2: DictionaryArray<Int32Type> = vec!["a", "b", "a", "c"].into_iter().collect();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "col",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        )]));

        let batch1 = RecordBatch::try_new(schema.clone(), vec![Arc::new(strings1)]).unwrap();
        let batch2 = RecordBatch::try_new(schema.clone(), vec![Arc::new(strings2)]).unwrap();

        // Force each batch into its own row group
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(4))
            .build();

        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props)).unwrap();
        writer.write(&batch1).unwrap();
        writer.write(&batch2).unwrap();
        writer.close().unwrap();

        let bytes: Bytes = buf.into();
        let batches = read_batches_from_bytes(&bytes);

        let mut all_values: Vec<Option<String>> = Vec::new();
        for b in &batches {
            all_values.extend(column_to_strings(b.column(0).as_ref()));
        }
        let expected: Vec<Option<String>> = vec!["x", "y", "z", "x", "a", "b", "a", "c"]
            .into_iter()
            .map(|s| Some(s.to_string()))
            .collect();
        assert_eq!(all_values, expected);
    }

    // T7: Statistics correctness — same data as Dict and Plain should produce same stats
    #[test]
    fn test_dict_passthrough_statistics_correctness() {
        use crate::file::reader::FileReader;
        use crate::file::serialized_reader::SerializedFileReader;

        let strings = vec!["cherry", "apple", "banana", "apple", "cherry"];

        // Plain
        let plain = StringArray::from(strings.clone());
        let plain_schema = Arc::new(Schema::new(vec![Field::new("col", DataType::Utf8, false)]));
        let plain_batch = RecordBatch::try_new(plain_schema, vec![Arc::new(plain)]).unwrap();

        // Dict
        let dict: DictionaryArray<Int32Type> = strings.into_iter().collect();
        let dict_schema = Arc::new(Schema::new(vec![Field::new(
            "col",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        )]));
        let dict_batch = RecordBatch::try_new(dict_schema, vec![Arc::new(dict)]).unwrap();

        let plain_bytes = write_batch_to_bytes(&plain_batch, None);
        let dict_bytes = write_batch_to_bytes(&dict_batch, None);

        // Compare metadata statistics
        let plain_reader = SerializedFileReader::new(plain_bytes).unwrap();
        let dict_reader = SerializedFileReader::new(dict_bytes).unwrap();

        let plain_meta = plain_reader.metadata().row_group(0).column(0);
        let dict_meta = dict_reader.metadata().row_group(0).column(0);

        assert_eq!(
            plain_meta.statistics(),
            dict_meta.statistics(),
            "Statistics must match between plain and dictionary paths"
        );
    }

    // T8: High cardinality dictionary that may trigger fallback
    #[test]
    fn test_dict_passthrough_high_cardinality() {
        // Create a dictionary with many unique values
        let values: Vec<String> = (0..5000).map(|i| format!("value_{i:06}")).collect();
        let dict: DictionaryArray<Int32Type> = values.iter().map(|s| s.as_str()).collect();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "col",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(dict)]).unwrap();

        // Use small dictionary page size to potentially trigger fallback
        let props = WriterProperties::builder()
            .set_dictionary_page_size_limit(1024)
            .build();
        let bytes = write_batch_to_bytes(&batch, Some(props));

        // Verify roundtrip correctness regardless of fallback or row group splitting
        let batches = read_batches_from_bytes(&bytes);
        let mut all_values: Vec<Option<String>> = Vec::new();
        for b in &batches {
            all_values.extend(column_to_strings(b.column(0).as_ref()));
        }
        let expected: Vec<Option<String>> = values.iter().map(|s| Some(s.clone())).collect();
        assert_eq!(all_values, expected);
    }
}
