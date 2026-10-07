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
use crate::file::properties::{
    EnabledStatistics, ResolvedColumnProperties, WriterProperties, WriterVersion,
};
use crate::geospatial::accumulator::{GeoStatsAccumulator, try_new_geo_stats_accumulator};
use crate::geospatial::statistics::GeospatialStatistics;
use crate::schema::types::ColumnDescPtr;
use crate::util::bit_util::num_required_bits;
use crate::util::prefix::common_prefix_length;
use arrow_array::types::ByteArrayType;
use arrow_array::{
    Array, ArrayAccessor, BinaryArray, BinaryViewArray, DictionaryArray, FixedSizeBinaryArray,
    GenericByteArray, LargeBinaryArray, LargeStringArray, OffsetSizeTrait, StringArray,
    StringViewArray,
};
use arrow_buffer::{ArrowNativeType, Buffer};
use arrow_schema::DataType;
use hashbrown::HashTable;
use std::cmp::Ordering;
use std::ops::Range;

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
                DataType::Utf8View => {
                    downcast_dict_op!(key, StringViewArray, $array, $op$(, $arg)*)
                }
                DataType::Binary => downcast_dict_op!(key, BinaryArray, $array, $op$(, $arg)*),
                DataType::LargeBinary => {
                    downcast_dict_op!(key, LargeBinaryArray, $array, $op$(, $arg)*)
                }
                DataType::BinaryView => {
                    downcast_dict_op!(key, BinaryViewArray, $array, $op$(, $arg)*)
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
    /// Create the fallback encoder for the given [`WriterProperties`] and the
    /// column settings already resolved from them
    fn new(props: &WriterProperties, column_props: &ResolvedColumnProperties) -> Result<Self> {
        // Set either main encoder or fallback encoder.
        let encoding = column_props
            .encoding
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
    /// Encode a contiguous range of values, which are adjacent in memory, so
    /// their sizes are known up front and their bytes can be copied in bulk
    fn encode_contiguous<O: OffsetSizeTrait>(&mut self, values: &ContiguousValues<'_, O>) {
        let total_len = values.total_len();
        self.num_values += values.len();
        self.variable_length_bytes += total_len as i64;
        match &mut self.encoder {
            FallbackEncoderImpl::Plain { buffer } => {
                // Each value is its `u32` length followed by its bytes
                buffer.reserve(values.len() * std::mem::size_of::<u32>() + total_len);
                for value in values.iter() {
                    buffer.extend_from_slice((value.len() as u32).as_bytes());
                    buffer.extend_from_slice(value);
                }
            }
            FallbackEncoderImpl::DeltaLength { buffer, lengths } => {
                let value_lengths: Vec<i32> = values.lengths().map(|len| len as i32).collect();
                lengths.put(&value_lengths).unwrap();
                // The bytes of all the values back to back, as they already are
                buffer.extend_from_slice(values.bytes());
            }
            FallbackEncoderImpl::Delta {
                buffer,
                last_value,
                prefix_lengths,
                suffix_lengths,
            } => {
                let mut prefixes = Vec::with_capacity(values.len());
                let mut suffixes = Vec::with_capacity(values.len());
                for value in values.iter() {
                    let prefix_length = common_prefix_length(last_value, value);
                    last_value.clear();
                    last_value.extend_from_slice(value);
                    buffer.extend_from_slice(&value[prefix_length..]);
                    prefixes.push(prefix_length as i32);
                    suffixes.push((value.len() - prefix_length) as i32);
                }
                prefix_lengths.put(&prefixes).unwrap();
                suffix_lengths.put(&suffixes).unwrap();
            }
        }
    }

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

                    let prefix_length = common_prefix_length(last_value, value);
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
            nan_count: None,
            variable_length_bytes,
        })
    }
}

/// Values of at most this many bytes are stored inline in their [`Entry`]
const INLINE_LEN: usize = 7;

/// An entry of [`ByteArrayInterner`]'s hash table
#[derive(Debug, Clone, Copy)]
struct Entry {
    /// Hash of the value, so growing the table never hashes values again, and
    /// a probe only compares values whose hash matches
    hash: u64,
    /// The value, if short enough to store inline, or where it is in the
    /// dictionary page
    value: InlineOrOffset,
    /// Index of the value in the dictionary
    key: u64,
}

/// A value of at most [`INLINE_LEN`] bytes stored inline, compared without
/// reading the dictionary page, or the offset of a longer value in the
/// dictionary page
///
/// The format of the `u64` is:
///
/// ```text
///   +--------------------+------------+-----------------+-----------------------+
///   | inline flag (1bit) | 0 (4 bits) | length (3 bits) | value bytes (56 bits) |
///   +--------------------+------------+-----------------+-----------------------+
///   | inline flag (1bit) | offset in the dictionary page (63 bits)              |
///   +--------------------+------------------------------------------------------+
/// ```
///
/// `inline flag`: 1 for an inline value and 0 for an offset, so an inline value
/// never equals an offset. The length is needed as values of different lengths
/// can have the same bytes as an integer, such as `"\0a"` and `"a"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct InlineOrOffset(u64);

impl InlineOrOffset {
    const INLINE_FLAG: u64 = 1 << 63;

    /// A value of at most [`INLINE_LEN`] bytes, stored inline
    #[inline]
    fn new_inline(value: &[u8]) -> Self {
        debug_assert!(value.len() <= INLINE_LEN);
        let bytes = value.iter().fold(0, |acc, &x| (acc << 8) | x as u64);
        Self(Self::INLINE_FLAG | ((value.len() as u64) << 56) | bytes)
    }

    /// The offset of a value in the dictionary page
    #[inline]
    fn new_offset(offset: usize) -> Self {
        let offset = offset as u64;
        debug_assert!(offset < Self::INLINE_FLAG);
        Self(offset)
    }

    #[inline]
    fn is_inline(&self) -> bool {
        self.0 & Self::INLINE_FLAG != 0
    }

    /// The offset in the dictionary page of a value not stored inline
    #[inline]
    fn offset(&self) -> usize {
        debug_assert!(!self.is_inline());
        self.0 as usize
    }
}

/// Interns byte array values for [`DictEncoder`], writing each distinct value
/// to the PLAIN encoded dictionary page as it is first seen.
///
/// Modeled on DataFusion's `ArrowBytesMap`:
///
/// * Each entry keeps the hash of its value, so growing the table never hashes
///   values again, and a probe only compares values whose hash matches
/// * A value of up to [`INLINE_LEN`] bytes is stored inline in its entry and
///   compared there; a longer value is compared with the dictionary page
/// * The values of a batch are all hashed before any is looked up, so a
///   lookup starts from a hash already in memory
/// * The insert path is out of line, so the lookup loop needs no registers for
///   it
#[derive(Debug, Default)]
struct ByteArrayInterner {
    state: ahash::RandomState,

    map: HashTable<Entry>,

    /// Encoded dictionary page: each value prefixed by its length as a `u32`
    page: Vec<u8>,

    /// Number of distinct values
    num_values: usize,
}

impl ByteArrayInterner {
    /// Interns `values` at `indices`, appending their dictionary keys to `keys`
    ///
    /// Returns the total length of the values in bytes.
    fn intern_batch<T>(
        &mut self,
        values: T,
        indices: impl ExactSizeIterator<Item = usize> + Clone,
        keys: &mut Vec<u64>,
    ) -> i64
    where
        T: ArrayAccessor + Copy,
        T::Item: AsRef<[u8]>,
    {
        let values = indices.map(move |idx| values.value(idx));
        self.intern_hashed(values, keys) as i64
    }

    /// Interns each of `values`, appending their dictionary keys to `keys`
    fn intern_values<'a>(
        &mut self,
        values: impl ExactSizeIterator<Item = &'a [u8]> + Clone,
        keys: &mut Vec<u64>,
    ) {
        self.intern_hashed(values, keys);
    }

    /// Hashes all of `values` first, then looks each up with its hash,
    /// returning their total length in bytes
    ///
    /// The hashes are written where the keys go, and replaced by the keys: the
    /// fewer values live across the lookup loop, which calls the insert path,
    /// the fewer the loop keeps on the stack. Both passes are plain loops in
    /// this function, so the running total stays in a register rather than
    /// being reloaded around every store of a key.
    fn intern_hashed<V: AsRef<[u8]>>(
        &mut self,
        values: impl ExactSizeIterator<Item = V> + Clone,
        keys: &mut Vec<u64>,
    ) -> usize {
        let start = keys.len();
        keys.resize(start + values.len(), 0);
        let keys = &mut keys[start..];
        let mut total_len = 0;
        for (value, hash) in values.clone().zip(keys.iter_mut()) {
            let value = value.as_ref();
            total_len += value.len();
            *hash = self.state.hash_one(value);
        }
        for (value, key) in values.zip(keys.iter_mut()) {
            *key = self.intern(value.as_ref(), *key);
        }
        total_len
    }

    /// Returns the key of `value` with `hash`, inserting it if absent
    #[inline]
    fn intern(&mut self, value: &[u8], hash: u64) -> u64 {
        let found = if value.len() <= INLINE_LEN {
            let inline = InlineOrOffset::new_inline(value);
            self.map
                .find(hash, |entry| entry.hash == hash && entry.value == inline)
        } else {
            self.map.find(hash, |entry| {
                entry.hash == hash
                    && !entry.value.is_inline()
                    && self.is_at(entry.value.offset(), value)
            })
        };
        match found {
            Some(entry) => entry.key,
            None => self.insert(value, hash),
        }
    }

    /// Returns whether `value` is in the dictionary page at `offset`
    #[inline(always)]
    fn is_at(&self, offset: usize, value: &[u8]) -> bool {
        // SAFETY: every value in the page is preceded by its length
        let stored_len = unsafe { self.page.get_unchecked(offset - 4..offset) };
        // Rarely false, as the hashes matched, so the branch predicts well and
        // the comparison's reads issue before the length is known
        if u32::from_le_bytes(stored_len.try_into().unwrap()) as usize != value.len() {
            return false;
        }
        // SAFETY: the value at `offset` has the length just checked
        let existing = unsafe { self.page.get_unchecked(offset..offset + value.len()) };
        existing == value
    }

    /// Appends `value`, absent from the dictionary, to it
    ///
    /// Out of line: inlined, its code made every lookup pay for setting up
    /// registers that only the insert path needs.
    #[cold]
    #[inline(never)]
    fn insert(&mut self, value: &[u8], hash: u64) -> u64 {
        let key = self.num_values as u64;

        self.page.reserve(4 + value.len());
        self.page.extend_from_slice((value.len() as u32).as_bytes());
        let offset = self.page.len();
        self.page.extend_from_slice(value);
        self.num_values += 1;

        let entry = Entry {
            hash,
            value: match value.len() <= INLINE_LEN {
                true => InlineOrOffset::new_inline(value),
                false => InlineOrOffset::new_offset(offset),
            },
            key,
        };
        self.map.insert_unique(hash, entry, |entry| entry.hash);
        key
    }

    /// Returns the distinct values in dictionary order
    fn values(&self) -> impl Iterator<Item = &[u8]> {
        let mut page = self.page.as_slice();
        std::iter::from_fn(move || {
            let (len, rest) = page.split_first_chunk::<4>()?;
            let (value, rest) = rest.split_at(u32::from_le_bytes(*len) as usize);
            page = rest;
            Some(value)
        })
    }

    fn estimated_memory_size(&self) -> usize {
        self.page.capacity() + self.map.allocation_size()
    }
}

/// A dictionary encoder for byte array data
#[derive(Debug, Default)]
struct DictEncoder {
    interner: ByteArrayInterner,
    indices: Vec<u64>,
    variable_length_bytes: i64,
}

impl DictEncoder {
    /// Encode `values` to the in-progress page
    fn encode<T>(&mut self, values: T, indices: impl ExactSizeIterator<Item = usize> + Clone)
    where
        T: ArrayAccessor + Copy,
        T::Item: AsRef<[u8]>,
    {
        self.indices.reserve(indices.len());
        self.variable_length_bytes +=
            self.interner
                .intern_batch(values, indices, &mut self.indices);
    }

    /// Encode a contiguous range of values to the in-progress page
    fn encode_contiguous<O: OffsetSizeTrait>(&mut self, values: &ContiguousValues<'_, O>) {
        self.indices.reserve(values.len());
        self.interner
            .intern_values(values.iter(), &mut self.indices);
        self.variable_length_bytes += values.total_len() as i64;
    }

    fn bit_width(&self) -> u8 {
        let length = self.interner.num_values;
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
        self.interner.page.len()
    }

    fn flush_dict_page(self) -> DictionaryPage {
        DictionaryPage {
            buf: self.interner.page.into(),
            num_values: self.interner.num_values,
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
        encoder.put_batch(&self.indices);

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
            nan_count: None,
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

    fn try_new(
        descr: &ColumnDescPtr,
        props: &WriterProperties,
        column_props: &ResolvedColumnProperties,
    ) -> Result<Self>
    where
        Self: Sized,
    {
        let dictionary = column_props.dictionary_enabled.then(DictEncoder::default);

        let fallback = FallbackEncoder::new(props, column_props)?;

        let (bloom_filter, bloom_filter_target_fpp) = create_bloom_filter(column_props)?;

        let statistics_enabled = column_props.statistics_enabled;

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
        let range = offset..offset + len;
        let any = values.as_any();
        match values.data_type() {
            DataType::Utf8 => encode_range(any.downcast_ref::<StringArray>().unwrap(), range, self),
            DataType::LargeUtf8 => {
                encode_range(any.downcast_ref::<LargeStringArray>().unwrap(), range, self)
            }
            DataType::Binary => {
                encode_range(any.downcast_ref::<BinaryArray>().unwrap(), range, self)
            }
            DataType::LargeBinary => {
                encode_range(any.downcast_ref::<LargeBinaryArray>().unwrap(), range, self)
            }
            // View and dictionary arrays don't store their values contiguously
            _ => downcast_op!(values.data_type(), values, encode, range, self),
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

    fn count_values_within_byte_budget_gather(
        values: &Self::Values,
        indices: &[usize],
        byte_budget: usize,
    ) -> Option<usize> {
        // `ByteArrayEncoder` only ever writes via `write_gather`, so this
        // is the relevant method.
        //
        // Two-stage walk for the simple offset-buffer byte array types:
        //   1. If indices are contiguous, compute the total payload in
        //      O(1) via a single subtraction on the offsets buffer.
        //      When the total fits the budget — the overwhelmingly
        //      common "small values" case — return immediately.
        //   2. Otherwise, walk per-value byte sizes from the offsets
        //      buffer (still cheap, no slice/UTF-8 construction) and
        //      exit at the first value that pushes the cumulative sum
        //      past the budget. This bounds skewed distributions: an
        //      outlier value is caught wherever it lands in the chunk.
        let count = match values.data_type() {
            DataType::Utf8 => count_within_budget_offsets(
                values.as_any().downcast_ref::<StringArray>().unwrap(),
                indices,
                byte_budget,
            ),
            DataType::LargeUtf8 => count_within_budget_offsets(
                values.as_any().downcast_ref::<LargeStringArray>().unwrap(),
                indices,
                byte_budget,
            ),
            DataType::Binary => count_within_budget_offsets(
                values.as_any().downcast_ref::<BinaryArray>().unwrap(),
                indices,
                byte_budget,
            ),
            DataType::LargeBinary => count_within_budget_offsets(
                values.as_any().downcast_ref::<LargeBinaryArray>().unwrap(),
                indices,
                byte_budget,
            ),
            // View arrays carry each value's length in the low 32 bits of
            // its u128 view word, so lengths are scannable without touching
            // any data buffer — and the common small-value case skips even
            // that scan via an O(1) conservative bound.
            DataType::Utf8View => {
                let array = values.as_any().downcast_ref::<StringViewArray>().unwrap();
                count_within_budget_views(
                    array.views(),
                    indices,
                    byte_budget,
                    max_view_value_len(array.data_buffers()),
                )
            }
            DataType::BinaryView => {
                let array = values.as_any().downcast_ref::<BinaryViewArray>().unwrap();
                count_within_budget_views(
                    array.views(),
                    indices,
                    byte_budget,
                    max_view_value_len(array.data_buffers()),
                )
            }
            // The values in an arrow dictionary are already small and
            // deduplicated, so there is nothing to bound — treat every
            // chunk as fitting and stay on the batched path. (A per-value
            // walk through dict keys on every chunk also measured ~+30-80%
            // slower than `main`.)
            DataType::Dictionary(_, _) => indices.len(),
            // Every byte-array type `ByteArrayEncoder` is constructed for
            // has an explicit arm above. A `Dictionary(value = FixedSizeBinary)`
            // column hits the `Dictionary(_, _)` arm (its `values.data_type()`
            // is `Dictionary`), and a bare `FixedSizeBinary` column is routed
            // to the generic column writer, never this encoder — so no other
            // type can reach here.
            data_type => unreachable!("ByteArrayEncoder cannot be constructed for {data_type:?}"),
        };
        Some(count)
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

    fn compresses_against_previous_value(&self) -> bool {
        // While dictionary encoding is active the data page holds RLE
        // indices, which carry no cross-value state; only the DELTA_BYTE_ARRAY
        // fallback shares prefixes with the preceding value.
        self.dict_encoder.is_none()
            && matches!(self.fallback.encoder, FallbackEncoderImpl::Delta { .. })
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

                if let Some(bloom_filter) = &mut self.bloom_filter {
                    for value in encoder.interner.values() {
                        bloom_filter.insert(value);
                    }
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

/// The values of a contiguous range of an offset based byte array, which sit
/// back to back in the array's value buffer
struct ContiguousValues<'a, O: OffsetSizeTrait> {
    /// The `len() + 1` offsets delimiting the values in `data`
    offsets: &'a [O],
    data: &'a [u8],
}

impl<'a, O: OffsetSizeTrait> ContiguousValues<'a, O> {
    fn new<T: ByteArrayType<Offset = O>>(
        array: &'a GenericByteArray<T>,
        range: Range<usize>,
    ) -> Self {
        Self {
            offsets: &array.value_offsets()[range.start..=range.end],
            data: array.value_data(),
        }
    }

    fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    /// The bytes of all the values, back to back
    fn bytes(&self) -> &'a [u8] {
        let (first, last) = (self.offsets[0], self.offsets[self.offsets.len() - 1]);
        &self.data[first.as_usize()..last.as_usize()]
    }

    /// The total length of the values in bytes
    fn total_len(&self) -> usize {
        self.bytes().len()
    }

    /// The length of each value in bytes
    fn lengths(&self) -> impl Iterator<Item = usize> + 'a {
        self.offsets.windows(2).map(|w| (w[1] - w[0]).as_usize())
    }

    fn iter(&self) -> impl ExactSizeIterator<Item = &'a [u8]> + Clone + 'a {
        let data = self.data;
        self.offsets.windows(2).map(move |w| {
            // SAFETY: the offsets of a `GenericByteArray` are monotonically
            // increasing and within its value buffer
            unsafe { data.get_unchecked(w[0].as_usize()..w[1].as_usize()) }
        })
    }
}

/// Encodes the contiguous `range` of an offset based byte array to `encoder`
///
/// Like [`encode`], but the values are read straight from the array's offsets
/// rather than looked up one index at a time, their total length is a single
/// subtraction, and the fallback encoders size and copy them in bulk.
fn encode_range<T: ByteArrayType>(
    array: &GenericByteArray<T>,
    range: Range<usize>,
    encoder: &mut ByteArrayEncoder,
) {
    if encoder.geo_stats_accumulator.is_some() {
        // Geospatial statistics are rare, and need the generic path
        return encode(array, range, encoder);
    }
    let values = ContiguousValues::new(array, range);

    if encoder.statistics_enabled != EnabledStatistics::None
        && let Some((min, max)) = min_max(values.iter())
    {
        if encoder.min_value.as_ref().is_none_or(|m| m.data() > min) {
            encoder.min_value = Some(min.to_vec().into());
        }
        if encoder.max_value.as_ref().is_none_or(|m| m.data() < max) {
            encoder.max_value = Some(max.to_vec().into());
        }
    }

    // While a dictionary is in use the filter is populated from its distinct values in
    // `flush_dict_page`, so each value is hashed once rather than once per row.
    match &mut encoder.dict_encoder {
        Some(dict_encoder) => dict_encoder.encode_contiguous(&values),
        None => {
            if let Some(bloom_filter) = &mut encoder.bloom_filter {
                for value in values.iter() {
                    bloom_filter.insert(value);
                }
            }
            encoder.fallback.encode_contiguous(&values)
        }
    }
}

/// Encodes the provided `values` and `indices` to `encoder`
///
/// This is a free function so it can be used with `downcast_op!`
fn encode<T, I>(values: T, indices: I, encoder: &mut ByteArrayEncoder)
where
    T: ArrayAccessor + Copy,
    T::Item: Copy + AsRef<[u8]>,
    I: ExactSizeIterator<Item = usize> + Clone,
{
    if encoder.statistics_enabled != EnabledStatistics::None {
        if let Some(accumulator) = encoder.geo_stats_accumulator.as_mut() {
            update_geo_stats_accumulator(accumulator.as_mut(), values, indices.clone());
        } else if let Some((min, max)) = compute_min_max(values, indices.clone()) {
            // Compare before copying: `write_gather` runs once per
            // mini-batch, and a byte-budgeted mini-batch of large values can
            // hold a single value, so an unconditional copy here would
            // duplicate every value once for `min` and once for `max`.
            let min = min.as_ref();
            if encoder.min_value.as_ref().is_none_or(|m| m.data() > min) {
                encoder.min_value = Some(min.to_vec().into());
            }

            let max = max.as_ref();
            if encoder.max_value.as_ref().is_none_or(|m| m.data() < max) {
                encoder.max_value = Some(max.to_vec().into());
            }
        }
    }

    // While a dictionary is in use the filter is populated from its distinct values in
    // `flush_dict_page`, so each value is hashed once rather than once per row.
    match &mut encoder.dict_encoder {
        Some(dict_encoder) => dict_encoder.encode(values, indices),
        None => {
            if let Some(bloom_filter) = &mut encoder.bloom_filter {
                for idx in indices.clone() {
                    bloom_filter.insert(values.value(idx).as_ref());
                }
            }
            encoder.fallback.encode(values, indices)
        }
    }
}

/// Upper bound on any single value's byte length in a view array.
fn max_view_value_len(buffers: &[Buffer]) -> usize {
    /// Bytes that fit inline in a u128 view word (the rest is len + prefix).
    const MAX_INLINE_VIEW_LEN: usize = 12;
    // An out-of-line view's data is a contiguous slice of exactly one data
    // buffer, so it cannot exceed the largest buffer; inline views hold at
    // most `MAX_INLINE_VIEW_LEN`. Loose (a value is usually far smaller than
    // a whole buffer) but O(number of buffers) and always sound.
    buffers
        .iter()
        .map(|b| b.len())
        .max()
        .unwrap_or(0)
        .max(MAX_INLINE_VIEW_LEN)
}

/// Number of leading `indices` whose cumulative plain-encoded size fits
/// `byte_budget` (boundary value included), for view arrays (`Utf8View`,
/// `BinaryView`).
fn count_within_budget_views(
    views: &[u128],
    indices: &[usize],
    byte_budget: usize,
    max_value_len: usize,
) -> usize {
    // Each plain-encoded BYTE_ARRAY value carries a 4-byte length prefix, so
    // the budget is compared against `value_len + size_of::<u32>()` — the
    // bytes actually written to the page, not just the payload.
    //
    // Stage 1: O(1) conservative bound. View arrays have no prefix-sum
    // offsets buffer, so the exact span subtraction used by
    // `count_within_budget_offsets` is unavailable; instead bound every
    // value by `max_value_len`. Skips the walk for the common small-value
    // case (what view arrays are built for, and where there is nothing to
    // bound).
    let per_value = max_value_len + std::mem::size_of::<u32>();
    if indices.len().saturating_mul(per_value) <= byte_budget {
        return indices.len();
    }
    // Stage 2: exact per-value scan, reading each length from the low 32
    // bits of its u128 view word (no data-buffer dereference).
    let mut cum: usize = 0;
    for (i, idx) in indices.iter().enumerate() {
        let len = (views[*idx] as u32) as usize;
        cum = cum.saturating_add(len + std::mem::size_of::<u32>());
        if cum > byte_budget {
            return i + 1;
        }
    }
    indices.len()
}

/// Number of leading `indices` whose cumulative plain-encoded size fits
/// `byte_budget` (boundary value included), for offset-buffer byte arrays
/// (`Utf8`/`LargeUtf8`/`Binary`/`LargeBinary`).
///
/// `indices` are assumed sorted ascending — they always are here, since
/// they come from `non_null_indices`, which is built in array order.
fn count_within_budget_offsets<T: ByteArrayType>(
    values: &GenericByteArray<T>,
    indices: &[usize],
    byte_budget: usize,
) -> usize {
    if indices.is_empty() {
        return 0;
    }
    let n = indices.len();
    let first = indices[0];
    let last = indices[n - 1];
    let offsets = values.value_offsets();
    // Each plain-encoded value carries a 4-byte length prefix on the page.
    let prefix_overhead = std::mem::size_of::<u32>();

    // Stage 1: O(1) span upper bound. The span `offsets[last+1] -
    // offsets[first]` covers every array position in `[first, last]`, a
    // superset of `indices` — and the skipped positions in a nullable
    // column are nulls with zero offset delta, so the span still equals the
    // exact payload. If it fits the budget, every value fits. Covers the
    // common small-value case for both non-null and (sparse) nullable
    // columns.
    if last >= first {
        let payload = (offsets[last + 1] - offsets[first]).as_usize();
        if payload + n * prefix_overhead <= byte_budget {
            return n;
        }
    }

    // Stage 2: scan per-index lengths from the offsets buffer.
    let mut cum: usize = 0;
    for (i, idx) in indices.iter().enumerate() {
        let len = (offsets[idx + 1] - offsets[*idx]).as_usize() + prefix_overhead;
        cum = cum.saturating_add(len);
        if cum > byte_budget {
            return i + 1;
        }
    }
    n
}

/// Computes the min and max for the provided array and indices
///
/// This is a free function so it can be used with `downcast_op!`
fn compute_min_max<T>(array: T, valid: impl Iterator<Item = usize>) -> Option<(T::Item, T::Item)>
where
    T: ArrayAccessor,
    T::Item: Copy + AsRef<[u8]>,
{
    min_max(valid.map(|idx| array.value(idx)))
}

/// Returns the lexicographically smallest and largest of `values`
fn min_max<V: Copy + AsRef<[u8]>>(mut values: impl Iterator<Item = V>) -> Option<(V, V)> {
    // Comparing every value to both `min` and `max` with `Ord` is two out of
    // line `memcmp` calls per value. Instead keep the prefix keys of `min` and
    // `max` in registers: once they settle, nearly every value is ruled out by
    // two integer comparisons, and only values sharing a prefix with the
    // current `min` or `max` are compared in full.
    let mut min = PrefixKeyed::new(values.next()?);
    let mut max = min;
    for val in values {
        let val = PrefixKeyed::new(val);
        // `min <= max`, so a value can't be smaller than `min` and larger than `max`
        if val.cmp(&min) == Ordering::Less {
            min = val;
        } else if val.cmp(&max) == Ordering::Greater {
            max = val;
        }
    }
    Some((min.value, max.value))
}

/// A byte string with its first [`PREFIX_LEN`] bytes as a big endian integer,
/// so that comparing keys compares the prefixes lexicographically.
#[derive(Clone, Copy)]
struct PrefixKeyed<T> {
    key: u64,
    value: T,
}

const PREFIX_LEN: usize = std::mem::size_of::<u64>();

impl<T: AsRef<[u8]>> PrefixKeyed<T> {
    #[inline(always)]
    fn new(value: T) -> Self {
        let bytes = value.as_ref();
        let key = match bytes.first_chunk::<PREFIX_LEN>() {
            Some(prefix) => u64::from_be_bytes(*prefix),
            // Zero pad short values. Ties with a longer value that continues
            // with zeros are resolved by length in `cmp`.
            None => {
                let mut prefix = [0; PREFIX_LEN];
                for (dst, src) in prefix.iter_mut().zip(bytes) {
                    *dst = *src;
                }
                u64::from_be_bytes(prefix)
            }
        };
        Self { key, value }
    }

    /// Lexicographic comparison of the values, equivalent to `Ord` for
    /// `[u8]` and `str`
    #[inline(always)]
    fn cmp(&self, other: &Self) -> Ordering {
        match self.key.cmp(&other.key) {
            Ordering::Equal => {
                let (a, b) = (self.value.as_ref(), other.value.as_ref());
                if a.len() > PREFIX_LEN && b.len() > PREFIX_LEN {
                    a[PREFIX_LEN..].cmp(&b[PREFIX_LEN..])
                } else {
                    // The shorter value (zero padded) is a prefix of the other
                    a.len().cmp(&b.len())
                }
            }
            ordering => ordering,
        }
    }
}

/// Updates geospatial statistics for the provided array and indices
fn update_geo_stats_accumulator<T>(
    bounder: &mut dyn GeoStatsAccumulator,
    array: T,
    valid: impl Iterator<Item = usize>,
) where
    T: ArrayAccessor,
    T::Item: Copy + AsRef<[u8]>,
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
    use super::*;
    use crate::schema::types::{ColumnDescriptor, ColumnPath};
    use arrow_array::ArrayRef;
    use bytes::Bytes;
    use rand::prelude::*;
    use std::sync::Arc;

    /// Values that stress the prefix key: zero bytes (which look like the
    /// padding of short values), shared prefixes and lengths around the prefix
    fn random_value(rng: &mut StdRng) -> Vec<u8> {
        let len = rng.random_range(0..=PREFIX_LEN * 2 + 2);
        let shared = rng.random_range(0..=len);
        (0..len)
            .map(|i| match i < shared {
                true => b"prefix\0\0xyz\0\0\0\0\0\0\0\0"[i % 19],
                false => *[0, 1, b'a', 0xff].choose(rng).unwrap(),
            })
            .collect()
    }

    #[test]
    fn test_byte_array_interner() {
        let mut rng = StdRng::seed_from_u64(42);
        // Short values that pack to the same integer, and longer ones
        let mut pool: Vec<Vec<u8>> = vec![
            vec![],
            b"\0".to_vec(),
            b"\0\0".to_vec(),
            b"ab".to_vec(),
            b"\0ab".to_vec(),
            b"\0\0\0\0\0\0ab".to_vec(),
            b"abcdefg".to_vec(),
            b"abcdefgh".to_vec(),
            b"abcdefghi".to_vec(),
            b"\0\0\0\0\0\0\0".to_vec(),
            b"\0\0\0\0\0\0\0\0".to_vec(),
        ];
        // Values around each length the hash and the comparison special case
        for len in [15, 16, 17, 31, 32, 33, 48, 100] {
            pool.push(vec![b'x'; len]);
            pool.push((0..len as u8).collect());
        }
        pool.extend((0..300).map(|_| random_value(&mut rng)));

        let mut interner = ByteArrayInterner::default();
        let mut expected_keys = std::collections::HashMap::new();
        let mut expected_values = vec![];
        for _ in 0..20 {
            let values: Vec<&[u8]> = (0..rng.random_range(0..500))
                .map(|_| pool.choose(&mut rng).unwrap().as_slice())
                .collect();
            let array = BinaryArray::from_iter_values(&values);
            let indices: Vec<usize> = (0..values.len()).filter(|_| rng.random_bool(0.9)).collect();

            let mut keys = vec![];
            let total_len = interner.intern_batch(&array, indices.iter().copied(), &mut keys);

            let mut expected_len = 0;
            for (idx, key) in indices.iter().zip(&keys) {
                let value = values[*idx];
                expected_len += value.len() as i64;
                let expected = *expected_keys.entry(value).or_insert_with(|| {
                    expected_values.push(value);
                    expected_values.len() as u64 - 1
                });
                assert_eq!(*key, expected, "{value:?}");
            }
            assert_eq!(total_len, expected_len);
        }
        assert_eq!(interner.num_values, expected_values.len());
        assert_eq!(interner.values().collect::<Vec<_>>(), expected_values);
    }

    /// A data page: its bytes, number of values, encoding, variable length
    /// bytes, and min and max statistics
    type Page = (
        Bytes,
        usize,
        Encoding,
        Option<i64>,
        Option<ByteArray>,
        Option<ByteArray>,
    );

    /// Everything an encoder produced: data pages and dictionary page
    #[derive(Debug, PartialEq)]
    struct Encoded {
        pages: Vec<Page>,
        dictionary: Option<(Bytes, usize)>,
    }

    /// Encodes `array` in mini-batches with `write_one`, flushing a data page
    /// every few batches, and returns everything the encoder produced
    fn encode_with(
        array: &ArrayRef,
        props: &WriterProperties,
        write_one: impl Fn(&mut ByteArrayEncoder, &ArrayRef, Range<usize>),
    ) -> Encoded {
        let descr = Arc::new(ColumnDescriptor::new(
            Arc::new(
                crate::schema::types::Type::primitive_type_builder(
                    "col",
                    crate::basic::Type::BYTE_ARRAY,
                )
                .build()
                .unwrap(),
            ),
            0,
            0,
            ColumnPath::new(vec!["col".to_string()]),
        ));
        let column_props = props.resolve_column_properties(descr.path());
        let mut encoder = ByteArrayEncoder::try_new(&descr, props, &column_props).unwrap();
        let mut pages = vec![];
        let mut flush = |encoder: &mut ByteArrayEncoder| {
            let page = encoder.flush_data_page().unwrap();
            pages.push((
                page.buf,
                page.num_values,
                page.encoding,
                page.variable_length_bytes,
                page.min_value,
                page.max_value,
            ));
        };
        for (i, start) in (0..array.len()).step_by(37).enumerate() {
            write_one(&mut encoder, array, start..(start + 37).min(array.len()));
            if i % 3 == 2 {
                flush(&mut encoder);
            }
        }
        flush(&mut encoder);
        let dictionary = encoder
            .flush_dict_page()
            .unwrap()
            .map(|page| (page.buf, page.num_values));
        Encoded { pages, dictionary }
    }

    #[test]
    fn test_write_range_matches_write_gather() {
        let mut rng = StdRng::seed_from_u64(42);
        let strings: Vec<String> = (0..500)
            .map(|_| {
                let len = rng.random_range(0..30);
                (0..len)
                    .map(|_| *b"abc".choose(&mut rng).unwrap() as char)
                    .collect()
            })
            .collect();
        // Sliced, so the arrays' offsets don't start at zero
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from_iter_values(&strings).slice(7, 450)),
            Arc::new(LargeStringArray::from_iter_values(&strings).slice(7, 450)),
            Arc::new(BinaryArray::from_iter_values(&strings).slice(7, 450)),
            Arc::new(LargeBinaryArray::from_iter_values(&strings).slice(7, 450)),
        ];
        let props: Vec<WriterProperties> = [
            None,
            Some(Encoding::PLAIN),
            Some(Encoding::DELTA_LENGTH_BYTE_ARRAY),
            Some(Encoding::DELTA_BYTE_ARRAY),
        ]
        .into_iter()
        .flat_map(|encoding| {
            [true, false].map(|dictionary| {
                let props = WriterProperties::builder().set_dictionary_enabled(dictionary);
                match encoding {
                    Some(encoding) => props.set_encoding(encoding),
                    None => props,
                }
                .build()
            })
        })
        .collect();

        for array in &arrays {
            for props in &props {
                let range = encode_with(array, props, |encoder, values, range| {
                    encoder.write(values, range.start, range.len()).unwrap()
                });
                let gather = encode_with(array, props, |encoder, values, range| {
                    let indices: Vec<usize> = range.collect();
                    encoder.write_gather(values, &indices).unwrap()
                });
                assert_eq!(range, gather, "{} {:?}", array.data_type(), props);
                assert!(
                    range
                        .pages
                        .iter()
                        .all(|page| page.4.is_some() && page.5.is_some())
                );
            }
        }
    }

    #[test]
    fn test_prefix_keyed_cmp() {
        let mut rng = StdRng::seed_from_u64(42);
        for _ in 0..100_000 {
            let a = random_value(&mut rng);
            let b = random_value(&mut rng);
            let actual = PrefixKeyed::new(a.as_slice()).cmp(&PrefixKeyed::new(b.as_slice()));
            assert_eq!(actual, a.cmp(&b), "{a:?} vs {b:?}");
        }
    }

    #[test]
    fn test_compute_min_max() {
        let mut rng = StdRng::seed_from_u64(42);
        for len in 0..200 {
            let values: Vec<Vec<u8>> = (0..len).map(|_| random_value(&mut rng)).collect();
            let array = BinaryArray::from_iter_values(&values);
            let indices: Vec<usize> = (0..len).filter(|_| rng.random_bool(0.8)).collect();

            let expected = indices.iter().map(|i| values[*i].as_slice());
            let expected = expected.clone().min().zip(expected.max());
            assert_eq!(compute_min_max(&array, indices.into_iter()), expected);
        }
    }
}

/// Replays only the dictionary interning of the writer, for profiling it in
/// isolation: run with `INTERN_BENCH_IPC=<file written by the
/// profile_wide_strings example with DUMP_IPC>` and optionally `ITERS=n`.
#[cfg(test)]
mod intern_microbench {
    use super::*;
    use crate::file::properties::{DEFAULT_DICTIONARY_PAGE_SIZE_LIMIT, DEFAULT_WRITE_BATCH_SIZE};
    use arrow_array::cast::AsArray;
    use arrow_array::{ArrayRef, MapArray, StructArray};
    use std::ops::Range;
    use std::time::Instant;

    /// Rows per `ArrowWriter::write` call in the profiling harness
    const ROWS_PER_WRITE: usize = 8192;

    /// A byte array leaf column, with the range of its values for each write
    struct Leaf {
        name: String,
        values: BinaryArray,
        ranges: Vec<Range<usize>>,
    }

    fn binary(array: &ArrayRef) -> Option<BinaryArray> {
        match array.data_type() {
            DataType::Utf8 => Some(BinaryArray::from(array.as_string::<i32>().clone())),
            DataType::Binary => Some(array.as_binary::<i32>().clone()),
            _ => None,
        }
    }

    /// The byte array leaves of `column` with the range of leaf values that
    /// each slice of rows in `row_ranges` maps to
    fn leaves(name: &str, column: &ArrayRef, row_ranges: &[Range<usize>], out: &mut Vec<Leaf>) {
        if let Some(values) = binary(column) {
            let ranges = row_ranges.to_vec();
            out.push(Leaf {
                name: name.to_string(),
                values,
                ranges,
            });
            return;
        }
        match column.data_type() {
            DataType::Struct(_) => {
                let column: &StructArray = column.as_struct();
                for (field, child) in column.fields().iter().zip(column.columns()) {
                    leaves(&format!("{name}.{}", field.name()), child, row_ranges, out);
                }
            }
            DataType::List(_) => {
                let list = column.as_list::<i32>();
                let offsets = list.value_offsets();
                let ranges: Vec<_> = row_ranges
                    .iter()
                    .map(|r| offsets[r.start] as usize..offsets[r.end] as usize)
                    .collect();
                leaves(&format!("{name}.list"), list.values(), &ranges, out);
            }
            DataType::Map(_, _) => {
                let map: &MapArray = column.as_map();
                let offsets = map.value_offsets();
                let ranges: Vec<_> = row_ranges
                    .iter()
                    .map(|r| offsets[r.start] as usize..offsets[r.end] as usize)
                    .collect();
                leaves(&format!("{name}.key"), map.keys(), &ranges, out);
                leaves(&format!("{name}.value"), map.values(), &ranges, out);
            }
            _ => {}
        }
    }

    #[test]
    #[ignore]
    fn intern_microbench() {
        let path = std::env::var("INTERN_BENCH_IPC").expect("set INTERN_BENCH_IPC");
        let iterations: usize = std::env::var("ITERS").map_or(20, |s| s.parse().unwrap());

        let file = std::fs::File::open(path).unwrap();
        let reader = arrow::ipc::reader::FileReader::try_new(file, None).unwrap();
        let batches: Vec<_> = reader.map(|b| b.unwrap()).collect();
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];

        let rows = batch.num_rows();
        let row_ranges: Vec<_> = (0..rows)
            .step_by(ROWS_PER_WRITE)
            .map(|start| start..(start + ROWS_PER_WRITE).min(rows))
            .collect();
        let mut all = vec![];
        for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
            leaves(field.name(), column, &row_ranges, &mut all);
        }

        // The writer only interns non-null values, in mini-batches of
        // `DEFAULT_WRITE_BATCH_SIZE` levels
        let work: Vec<Vec<Vec<Vec<usize>>>> = all
            .iter()
            .map(|leaf| {
                leaf.ranges
                    .iter()
                    .map(|range| {
                        let valid: Vec<usize> =
                            range.clone().filter(|i| leaf.values.is_valid(*i)).collect();
                        valid
                            .chunks(DEFAULT_WRITE_BATCH_SIZE)
                            .map(<[usize]>::to_vec)
                            .collect()
                    })
                    .collect()
            })
            .collect();
        eprintln!(
            "built {} byte array leaves from {} rows: {}",
            all.len(),
            rows,
            all.iter()
                .map(|l| l.name.as_str())
                .collect::<Vec<_>>()
                .join(",")
        );

        if std::env::var("INTERN_BENCH_LENS").is_ok() {
            // Interned values by length class, and how many leaves mix classes
            let classes = [0, 3, 7, 16, 32, 64, 128, usize::MAX];
            let class = |len: usize| classes.iter().position(|c| len <= *c).unwrap();
            let mut counts = [0usize; 8];
            let mut mixed = 0;
            for (leaf, work) in all.iter().zip(&work) {
                let mut leaf_counts = [0usize; 8];
                for idx in work.iter().flatten().flatten() {
                    leaf_counts[class(leaf.values.value(*idx).len())] += 1;
                }
                let total: usize = leaf_counts.iter().sum();
                if leaf_counts.iter().all(|c| *c < total * 9 / 10) {
                    mixed += 1;
                }
                counts
                    .iter_mut()
                    .zip(leaf_counts)
                    .for_each(|(c, l)| *c += l);
            }
            let total: usize = counts.iter().sum();
            for (c, n) in classes.iter().zip(counts) {
                eprintln!("len <= {c:>20}: {:5.1}%", 100.0 * n as f64 / total as f64);
            }
            eprintln!(
                "{mixed} of {} leaves have no length class with 90% of values",
                all.len()
            );
        }

        let mut times = vec![];
        let mut interned = 0;
        let mut inserted = 0;
        for _ in 0..iterations {
            let mut interners: Vec<_> = all.iter().map(|_| ByteArrayInterner::default()).collect();
            let mut keys = Vec::with_capacity(DEFAULT_WRITE_BATCH_SIZE);
            interned = 0;
            let start = Instant::now();
            // Like the writer, every leaf of a write before the next write
            for write in 0..row_ranges.len() {
                for ((leaf, work), interner) in all.iter().zip(&work).zip(interners.iter_mut()) {
                    for mini_batch in &work[write] {
                        // The writer falls back to plain encoding once the
                        // dictionary page outgrows its limit
                        if interner.page.len() > DEFAULT_DICTIONARY_PAGE_SIZE_LIMIT {
                            break;
                        }
                        keys.clear();
                        interner.intern_batch(&leaf.values, mini_batch.iter().copied(), &mut keys);
                        interned += mini_batch.len();
                    }
                }
            }
            times.push(start.elapsed());
            inserted = interners.iter().map(|i| i.values().count()).sum();
            std::hint::black_box(&interners);
        }
        times.sort();
        let median = times[times.len() / 2];
        eprintln!(
            "interned {interned} values per iteration, median {median:?}, {:.2} ns/value",
            median.as_secs_f64() * 1e9 / interned as f64
        );
        eprintln!(
            "inserted {inserted} new values ({:.1}%), found {} existing ({:.1}%)",
            100.0 * inserted as f64 / interned as f64,
            interned - inserted,
            100.0 * (interned - inserted) as f64 / interned as f64
        );
    }
}
