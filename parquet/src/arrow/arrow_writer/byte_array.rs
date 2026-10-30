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
use crate::column::writer::{compare_greater_byte_array_decimals, is_decimal_descr};
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
use arrow_array::cast::AsArray;
use arrow_array::types::{ArrowDictionaryKeyType, ByteArrayType};
use arrow_array::{
    Array, ArrayAccessor, ArrayRef, BinaryArray, BinaryViewArray, DictionaryArray,
    FixedSizeBinaryArray, GenericByteArray, LargeBinaryArray, LargeStringArray, StringArray,
    StringViewArray, TypedDictionaryArray,
};
use arrow_buffer::{ArrowNativeType, Buffer};
use arrow_schema::DataType;
use std::sync::{Arc, Weak};

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

macro_rules! downcast_dict_values_op {
    ($key_type:expr, $value_type:expr, $array:ident, $op:expr $(, $arg:expr)*) => {
        match $value_type.as_ref() {
            DataType::Utf8 => downcast_dict_op!($key_type, StringArray, $array, $op$(, $arg)*),
            DataType::LargeUtf8 => {
                downcast_dict_op!($key_type, LargeStringArray, $array, $op$(, $arg)*)
            }
            DataType::Utf8View => {
                downcast_dict_op!($key_type, StringViewArray, $array, $op$(, $arg)*)
            }
            DataType::Binary => downcast_dict_op!($key_type, BinaryArray, $array, $op$(, $arg)*),
            DataType::LargeBinary => {
                downcast_dict_op!($key_type, LargeBinaryArray, $array, $op$(, $arg)*)
            }
            DataType::BinaryView => {
                downcast_dict_op!($key_type, BinaryViewArray, $array, $op$(, $arg)*)
            }
            DataType::FixedSizeBinary(_) => {
                downcast_dict_op!($key_type, FixedSizeBinaryArray, $array, $op$(, $arg)*)
            }
            d => unreachable!("cannot downcast {} dictionary value to byte array", d),
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
            DataType::Dictionary(key, value) => {
                downcast_dict_values_op!(key, value, $array, $op$(, $arg)*)
            }
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
    fn encode<T>(&mut self, values: T, indices: &[usize])
    where
        T: ArrayAccessor + Copy,
        T::Item: AsRef<[u8]>,
    {
        self.num_values += indices.len();
        match &mut self.encoder {
            FallbackEncoderImpl::Plain { buffer } => {
                for &idx in indices {
                    let value = values.value(idx);
                    let value = value.as_ref();
                    buffer.extend_from_slice((value.len() as u32).as_bytes());
                    buffer.extend_from_slice(value);
                    self.variable_length_bytes += value.len() as i64;
                }
            }
            FallbackEncoderImpl::DeltaLength { buffer, lengths } => {
                for &idx in indices {
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
                for &idx in indices {
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

/// Maximum size of a value that can be inlined in the hash table
const SHORT_VALUE_LEN: usize = size_of::<u64>() - 1;

/// Entry in the hash table
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
struct Entry {
    /// hash of the value (stored to avoid recomputing it in hash table check)
    hash: u64,
    /// if len =< [`SHORT_VALUE_LEN`]: the data inlined
    /// if len > [`SHORT_VALUE_LEN`], the offset of where the data starts
    offset_or_inline: OffsetOrInlineView,
    index_in_dict: u64,
}

/// Either offset to the dictionary page bytes or inline bytes when the bytes are short enough
/// to avoid doing the indirection
///
/// For inlined view:
///   +------------+---------------+------------------------------+
///   | 1 bit set  | len {7 bits}  | value {56 bits}              |
///   +------------+---------------+------------------------------+
///
/// For non-inlined view:
///   +--------------+--------------------------------------------+
///   | 1 bit unset  | offset in dict {63 bits}                   |
///   +--------------+--------------------------------------------+
///
/// the inline view contain its length to avoid having another field for length in the entry
/// since if the hashes are the same, most probably the length will be the same, so we can do the indirection check
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct OffsetOrInlineView(u64);

impl OffsetOrInlineView {
    const INLINE_FLAG: u64 = 1 << 63;
    const LEN_SHIFT: u32 = 56;

    #[inline]
    fn is_non_inlined(&self) -> bool {
        (self.0 & Self::INLINE_FLAG) == 0
    }

    #[inline]
    fn is_inlined(&self) -> bool {
        !self.is_non_inlined()
    }

    #[inline]
    fn new_inlined(value: &[u8]) -> Self {
        debug_assert!(value.len() <= 7);

        // Value bytes occupy the low 56 bits, byte 0 in the least significant position
        let mut buf = [0u8; 8];
        buf[..value.len()].copy_from_slice(value);
        let packed = u64::from_le_bytes(buf);

        Self(Self::INLINE_FLAG | ((value.len() as u64) << Self::LEN_SHIFT) | packed)
    }

    #[inline]
    fn new_non_inlined(offset: u64) -> Self {
        // `offset` is a position in the dictionary page `Vec`, whose length is at
        // most `isize::MAX`, so it always fits in 63 bits and can't collide with
        // `INLINE_FLAG`
        // also, having the inline flag set for non-inlined values would mean the dictionary page is at least 9 exabytes.
        debug_assert_eq!(
            offset & Self::INLINE_FLAG,
            0,
            "dictionary page offset must fit in 63 bits"
        );
        let non_inlined_value = offset;
        Self(non_inlined_value)
    }

    fn maybe_non_inlined_value(&self) -> Option<u64> {
        if self.is_non_inlined() {
            // Not inlined is when the first bit is unset, so the value
            Some(self.0)
        } else {
            None
        }
    }
}

#[derive(Debug, Default)]
struct ByteArrayInterner {
    state: ahash::RandomState,
    /// Underlying hash set for each distinct value
    map: hashbrown::hash_table::HashTable<Entry>,
    /// Encoded dictionary page: each value prefixed by its length as a `u32`
    page: Vec<u8>,
}

impl ByteArrayInterner {
    fn intern(&mut self, value: &[u8]) -> Result<u64> {
        let hash = self.state.hash_one(value);

        // value is "small" - can be inlined
        let (found_entry, inline) = if let Some(inline) = maybe_inline_value(value) {
            // is value is already present in the set?
            let entry = self.map.find(hash, |header| {
                // value is stored inline so no need to consult buffer
                // (this is the "small string optimization")
                //
                // no need to compare the hashes since the hash is a function of the value which is compared here
                let is_same = inline == header.offset_or_inline;

                if is_same {
                    debug_assert_eq!(
                        header.hash, hash,
                        "hashes must match if the values are the same"
                    );
                }

                is_same
            });

            (entry, Some(inline))
        } else {
            // value is not "small"
            // Check if the value is already present in the set
            let entry = self.map.find(hash, |header| {
                // compare hash
                if header.hash != hash {
                    return false;
                }

                // If the value is not inlined compare it
                let Some(offset) = header.offset_or_inline.maybe_non_inlined_value() else {
                    // that value is inline, so it must be different
                    return false;
                };

                self.is_at(offset, value)
            });

            (entry, None)
        };

        if let Some(entry) = found_entry {
            return Ok(entry.index_in_dict);
        }

        // if no existing entry, make a new one
        self.insert(value, hash, inline)
    }

    /// Returns whether `value` is in the dictionary page at `offset`
    #[inline(always)]
    fn is_at(&self, offset: u64, value: &[u8]) -> bool {
        let offset = offset as usize;
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

    fn num_unique_values(&self) -> usize {
        self.map.len()
    }

    /// Appends `value` to the dictionary, in the empty slot at `pos`
    fn insert(
        &mut self,
        value: &[u8],
        hash: u64,
        inline: Option<OffsetOrInlineView>,
    ) -> Result<u64> {
        // If inline provided it should inlined
        debug_assert!(inline.is_none_or(|i| i.is_inlined()));

        // The dictionary page is PLAIN encoded, where each value is prefixed by
        // its length as a 4-byte little endian integer so we need to reject that since we use unsafe access later
        //
        // From the parquet-format spec:
        // > Encoding PLAIN:
        // > [...] For the byte array type, it encodes the length as a 4-byte little
        // > endian integer, followed by the bytes.
        // >
        // > Dictionary Encoding:
        // > [...] Dictionary page format: the entries in the dictionary using the plain encoding.
        // >
        //
        // Reference:
        // - Plain encoding: https://github.com/apache/parquet-format/blob/53b8bd8576827080d4382fa457003f45b41cda6e/Encodings.md?plain=1#L72-L73
        // - Plain dictionary encoding: https://github.com/apache/parquet-format/blob/53b8bd8576827080d4382fa457003f45b41cda6e/Encodings.md?plain=1#L83
        if value.len() > u32::MAX as usize {
            return Err(ParquetError::General(format!(
                "Byte array value is too large to be written in dictionary: {} bytes",
                value.len()
            )));
        }

        let key = self.num_unique_values() as u64;
        let len = value.len();

        self.page.reserve(4 + value.len());
        self.page.extend_from_slice(&(len as u32).to_le_bytes());
        let offset = self.page.len() as u64;
        self.page.extend_from_slice(value);

        // Make the new entry
        let new_header = Entry {
            hash,
            offset_or_inline: inline.unwrap_or_else(|| OffsetOrInlineView::new_non_inlined(offset)),
            index_in_dict: key,
        };
        self.map
            .insert_unique(hash, new_header, |header| header.hash);

        Ok(key)
    }

    fn estimated_memory_size(&self) -> usize {
        self.page.capacity() * size_of::<u8>() + self.map.allocation_size()
    }

    fn iter(&self) -> impl Iterator<Item = &[u8]> {
        ByteArrayInternerIterator {
            values: &self.page,
            current_index: 0,
        }
    }
}

/// Iterates the values of a [`ByteArrayInterner`]'s dictionary page
///
/// `values` must be a page built by [`ByteArrayInterner::insert`], which is a
/// sequence of values each preceded by its exact length as a `u32`, and
/// `current_index` must start at 0, so it is always at the start of a value
struct ByteArrayInternerIterator<'a> {
    values: &'a [u8],
    current_index: usize,
}

impl<'a> Iterator for ByteArrayInternerIterator<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_index >= self.values.len() {
            return None;
        }

        // SAFETY: `current_index` is at the start of a value and is not past
        // the end of the page, so the value's 4-byte length prefix is in bounds
        let len_bytes = unsafe {
            self.values
                .get_unchecked(self.current_index..self.current_index + 4)
        };
        let len = u32::from_le_bytes(len_bytes.try_into().unwrap()) as usize;

        let start = self.current_index + 4;
        let end = start + len;

        // SAFETY: `insert` writes exactly `len` bytes after the prefix, and
        // rejects values whose length does not fit in a `u32`, so the prefix is
        // never truncated and `start..end` is within the page
        let value = unsafe { self.values.get_unchecked(start..end) };

        self.current_index = end;

        Some(value)
    }
}

/// A dictionary encoder for byte array data
#[derive(Debug, Default)]
struct DictEncoder {
    interner: ByteArrayInterner,
    indices: Vec<u64>,
    variable_length_bytes: i64,
    /// The interned index of each value of the last arrow dictionary encoded
    dictionary_cache: Option<DictionaryCache>,
}

/// The interned index of each value of an arrow dictionary, so each value is hashed once
/// rather than once per row that references it
#[derive(Debug)]
struct DictionaryCache {
    /// The dictionary's values, to identify the dictionary in later batches. A weak reference,
    /// so the values aren't kept alive, while their allocation can't be reused by another
    /// dictionary
    values: Weak<dyn Array>,
    /// The interned index of each value, or [`DictionaryCache::NOT_INTERNED`]
    interned: Vec<u64>,
}

impl DictionaryCache {
    const NOT_INTERNED: u64 = u64::MAX;

    fn is_for(&self, dictionary: &ArrayRef) -> bool {
        std::ptr::addr_eq(self.values.as_ptr(), Arc::as_ptr(dictionary))
    }
}

impl DictEncoder {
    /// Encode `values` to the in-progress page
    fn encode<T>(&mut self, values: T, indices: &[usize]) -> Result<()>
    where
        T: ArrayAccessor + Copy,
        T::Item: AsRef<[u8]>,
    {
        self.indices.reserve(indices.len());

        for &idx in indices {
            let value = values.value(idx);
            let interned = self.interner.intern(value.as_ref())?;
            self.indices.push(interned);
            self.variable_length_bytes += value.as_ref().len() as i64;
        }

        Ok(())
    }

    /// Whether to encode the rows of an arrow dictionary with `dictionary` as its values and
    /// `num_rows` rows with [`Self::encode_dictionary`]
    fn use_dictionary_cache(&self, dictionary: &ArrayRef, num_rows: usize) -> bool {
        // A new cache has a slot per dictionary value, which only pays off if the values are
        // referenced by enough rows
        dictionary.len() <= num_rows
            || self
                .dictionary_cache
                .as_ref()
                .is_some_and(|cache| cache.is_for(dictionary))
    }

    /// Encode the rows of an arrow dictionary at `indices` to the in-progress page
    fn encode_dictionary<'a, K, V>(
        &mut self,
        values: TypedDictionaryArray<'a, K, V>,
        indices: &[usize],
        dictionary: &ArrayRef,
    ) -> Result<()>
    where
        K: ArrowDictionaryKeyType,
        &'a V: ArrayAccessor,
        <&'a V as ArrayAccessor>::Item: AsRef<[u8]>,
    {
        let cache = match &mut self.dictionary_cache {
            Some(cache) if cache.is_for(dictionary) => cache,
            cache => cache.insert(DictionaryCache {
                values: Arc::downgrade(dictionary),
                interned: vec![DictionaryCache::NOT_INTERNED; dictionary.len()],
            }),
        };

        let keys = values.keys().values();
        let dictionary_values = values.values();
        self.indices.reserve(indices.len());
        for &idx in indices {
            let key = keys[idx].as_usize();

            // Shadowed for lifetime reasons
            let value = dictionary_values.value(key);
            let value = value.as_ref();

            let mut interned = cache.interned[key];
            if interned == DictionaryCache::NOT_INTERNED {
                interned = self.interner.intern(value)?;
                cache.interned[key] = interned;
            }
            self.indices.push(interned);
            self.variable_length_bytes += value.len() as i64;
        }

        Ok(())
    }

    fn bit_width(&self) -> u8 {
        let length = self.interner.num_unique_values();
        num_required_bits(length.saturating_sub(1) as u64)
    }

    fn estimated_memory_size(&self) -> usize {
        let dictionary_cache_size = self
            .dictionary_cache
            .as_ref()
            .map(|cache| cache.interned.capacity() * size_of::<u64>())
            .unwrap_or_default();
        self.interner.estimated_memory_size()
            + self.indices.capacity() * size_of::<u64>()
            + dictionary_cache_size
    }

    fn estimated_data_page_size(&self) -> usize {
        let bit_width = self.bit_width();
        1 + RleEncoder::max_buffer_size(bit_width, self.indices.len())
    }

    fn estimated_dict_page_size(&self) -> usize {
        self.interner.page.len()
    }

    fn flush_dict_page(self) -> DictionaryPage {
        let unique_values = self.interner.num_unique_values();
        let bytes = self.interner.page;

        DictionaryPage {
            buf: bytes.into(),
            num_values: unique_values,
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

fn maybe_inline_value(value: &[u8]) -> Option<OffsetOrInlineView> {
    if value.len() <= SHORT_VALUE_LEN {
        Some(OffsetOrInlineView::new_inlined(value))
    } else {
        None
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
    is_decimal: bool,
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

        let is_decimal = is_decimal_descr(descr.get_basic_info());

        Ok(Self {
            fallback,
            statistics_enabled,
            bloom_filter,
            bloom_filter_target_fpp,
            dict_encoder: dictionary,
            min_value: None,
            max_value: None,
            geo_stats_accumulator,
            is_decimal,
        })
    }

    fn write(&mut self, _values: &Self::Values, _offset: usize, _len: usize) -> Result<()> {
        unreachable!("should call write_gather instead")
    }

    fn write_gather(&mut self, values: &Self::Values, indices: &[usize]) -> Result<()> {
        if let DataType::Dictionary(key, value) = values.data_type() {
            let dictionary = values.as_any_dictionary().values();
            if self
                .dict_encoder
                .as_ref()
                .is_some_and(|encoder| encoder.use_dictionary_cache(dictionary, values.len()))
            {
                return downcast_dict_values_op!(
                    key,
                    value,
                    values,
                    encode_dictionary,
                    indices,
                    dictionary,
                    self
                );
            }
        }

        downcast_op!(values.data_type(), values, encode, indices, self)
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
                    for value in encoder.interner.iter() {
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

/// Encodes the provided `values` and `indices` to `encoder`
///
/// This is a free function so it can be used with `downcast_op!`
fn encode<T>(values: T, indices: &[usize], encoder: &mut ByteArrayEncoder) -> Result<()>
where
    T: ArrayAccessor + Copy,
    T::Item: Copy + Ord + AsRef<[u8]>,
{
    update_statistics(values, indices, encoder);

    // While a dictionary is in use the filter is populated from its distinct values in
    // `flush_dict_page`, so each value is hashed once rather than once per row.
    match &mut encoder.dict_encoder {
        Some(dict_encoder) => dict_encoder.encode(values, indices),
        None => {
            if let Some(bloom_filter) = &mut encoder.bloom_filter {
                for &idx in indices {
                    bloom_filter.insert(values.value(idx).as_ref());
                }
            }
            encoder.fallback.encode(values, indices);

            Ok(())
        }
    }
}

/// Encodes the rows of an arrow dictionary at `indices` to `encoder`, which must have a
/// dictionary encoder
///
/// This is a free function so it can be used with `downcast_dict_values_op!`
fn encode_dictionary<'a, K, V>(
    values: TypedDictionaryArray<'a, K, V>,
    indices: &[usize],
    dictionary: &ArrayRef,
    encoder: &mut ByteArrayEncoder,
) -> Result<()>
where
    K: ArrowDictionaryKeyType,
    V: Sync + Send,
    &'a V: ArrayAccessor,
    <&'a V as ArrayAccessor>::Item: Copy + Ord + Default + AsRef<[u8]>,
{
    update_statistics(values, indices, encoder);
    encoder
        .dict_encoder
        .as_mut()
        .expect("dictionary encoder")
        .encode_dictionary(values, indices, dictionary)
}

/// Updates the statistics of `encoder` with the values at `indices`
fn update_statistics<T>(values: T, indices: &[usize], encoder: &mut ByteArrayEncoder)
where
    T: ArrayAccessor + Copy,
    T::Item: Copy + Ord + AsRef<[u8]>,
{
    if encoder.statistics_enabled != EnabledStatistics::None {
        if let Some(accumulator) = encoder.geo_stats_accumulator.as_mut() {
            update_geo_stats_accumulator(accumulator.as_mut(), values, indices);
        } else if let Some((min, max)) = compute_min_max(values, indices, encoder.is_decimal) {
            // Compare before copying: `write_gather` runs once per
            // mini-batch, and a byte-budgeted mini-batch of large values can
            // hold a single value, so an unconditional copy here would
            // duplicate every value once for `min` and once for `max`.
            let min = min.as_ref();
            if encoder
                .min_value
                .as_ref()
                .is_none_or(|m| is_greater(encoder.is_decimal, m.data(), min))
            {
                encoder.min_value = Some(min.to_vec().into());
            }

            let max = max.as_ref();
            if encoder
                .max_value
                .as_ref()
                .is_none_or(|m| is_greater(encoder.is_decimal, max, m.data()))
            {
                encoder.max_value = Some(max.to_vec().into());
            }
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
    let per_value = max_value_len + size_of::<u32>();
    if indices.len().saturating_mul(per_value) <= byte_budget {
        return indices.len();
    }
    // Stage 2: exact per-value scan, reading each length from the low 32
    // bits of its u128 view word (no data-buffer dereference).
    let mut cum: usize = 0;
    for (i, idx) in indices.iter().enumerate() {
        let len = (views[*idx] as u32) as usize;
        cum = cum.saturating_add(len + size_of::<u32>());
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
    let prefix_overhead = size_of::<u32>();

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

/// Returns `true` if `a > b`.
#[inline]
fn is_greater(is_decimal: bool, a: &[u8], b: &[u8]) -> bool {
    if is_decimal {
        compare_greater_byte_array_decimals(a, b)
    } else {
        a > b
    }
}

/// Computes the min and max for the provided array and indices
///
/// This is a free function so it can be used with `downcast_op!`
fn compute_min_max<T>(array: T, valid: &[usize], is_decimal: bool) -> Option<(T::Item, T::Item)>
where
    T: ArrayAccessor,
    T::Item: Copy + Ord + AsRef<[u8]>,
{
    let (&first_idx, valid) = valid.split_first()?;

    let first_val = array.value(first_idx);
    let mut min = first_val;
    let mut max = first_val;
    for &idx in valid {
        let val = array.value(idx);
        if is_greater(is_decimal, min.as_ref(), val.as_ref()) {
            min = val;
        }
        if is_greater(is_decimal, val.as_ref(), max.as_ref()) {
            max = val;
        }
    }
    Some((min, max))
}

/// Updates geospatial statistics for the provided array and indices
fn update_geo_stats_accumulator<T>(bounder: &mut dyn GeoStatsAccumulator, array: T, valid: &[usize])
where
    T: ArrayAccessor,
    T::Item: Copy + Ord + AsRef<[u8]>,
{
    if !bounder.is_valid() {
        return;
    }

    for &idx in valid {
        let val = array.value(idx);
        bounder.update_wkb(val.as_ref());
    }
}
