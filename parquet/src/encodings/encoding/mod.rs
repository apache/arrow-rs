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

//! Contains all supported encoders for Parquet.

use std::{cmp, marker::PhantomData};

use crate::basic::*;
use crate::data_type::private::ParquetValueType;
use crate::data_type::*;
use crate::encodings::rle::RleEncoder;
use crate::errors::{ParquetError, Result};
use crate::util::bit_util::{num_required_bits, BitWriter};

use bytes::Bytes;
pub use dict_encoder::DictEncoder;

mod byte_stream_split_encoder;
mod dict_encoder;

// ----------------------------------------------------------------------
// Encoders

/// An Parquet encoder for the data type `T`.
///
/// Currently this allocates internal buffers for the encoded values. After done putting
/// values, caller should call `flush_buffer()` to get an immutable buffer pointer.
pub trait Encoder<T: DataType>: Send {
    /// Encodes data from `values`.
    fn put(&mut self, values: &[T::T]) -> Result<()>;

    /// Encodes data from `values`, which contains spaces for null values, that is
    /// identified by `valid_bits`.
    ///
    /// Returns the number of non-null values encoded.
    #[cfg(test)]
    fn put_spaced(&mut self, values: &[T::T], valid_bits: &[u8]) -> Result<usize> {
        let num_values = values.len();
        let mut buffer = Vec::with_capacity(num_values);
        // TODO: this is pretty inefficient. Revisit in future.
        for (i, item) in values.iter().enumerate().take(num_values) {
            if crate::util::bit_util::get_bit(valid_bits, i) {
                buffer.push(item.clone());
            }
        }
        self.put(&buffer[..])?;
        Ok(buffer.len())
    }

    /// Returns the encoding type of this encoder.
    fn encoding(&self) -> Encoding;

    /// Returns an estimate of the encoded data, in bytes.
    /// Method call must be O(1).
    fn estimated_data_encoded_size(&self) -> usize;

    /// Flushes the underlying byte buffer that's being processed by this encoder, and
    /// return the immutable copy of it. This will also reset the internal state.
    fn flush_buffer(&mut self) -> Result<Bytes>;
}

/// Gets a encoder for the particular data type `T` and encoding `encoding`. Memory usage
/// for the encoder instance is tracked by `mem_tracker`.
pub fn get_encoder<T: DataType>(encoding: Encoding) -> Result<Box<dyn Encoder<T>>> {
    let encoder: Box<dyn Encoder<T>> = match encoding {
        Encoding::PLAIN => Box::new(PlainEncoder::new()),
        Encoding::RLE_DICTIONARY | Encoding::PLAIN_DICTIONARY => {
            return Err(general_err!(
                "Cannot initialize this encoding through this function"
            ));
        }
        Encoding::RLE => Box::new(RleValueEncoder::new()),
        Encoding::DELTA_BINARY_PACKED => Box::new(DeltaBitPackEncoder::new()),
        Encoding::DELTA_LENGTH_BYTE_ARRAY => Box::new(DeltaLengthByteArrayEncoder::new()),
        Encoding::DELTA_BYTE_ARRAY => Box::new(DeltaByteArrayEncoder::new()),
        Encoding::BYTE_STREAM_SPLIT => {
            Box::new(byte_stream_split_encoder::ByteStreamSplitEncoder::new())
        }
        e => return Err(nyi_err!("Encoding {} is not supported", e)),
    };
    Ok(encoder)
}

// ----------------------------------------------------------------------
// Plain encoding

/// Plain encoding that supports all types.
/// Values are encoded back to back.
/// The plain encoding is used whenever a more efficient encoding can not be used.
/// It stores the data in the following format:
/// - BOOLEAN - 1 bit per value, 0 is false; 1 is true.
/// - INT32 - 4 bytes per value, stored as little-endian.
/// - INT64 - 8 bytes per value, stored as little-endian.
/// - FLOAT - 4 bytes per value, stored as IEEE little-endian.
/// - DOUBLE - 8 bytes per value, stored as IEEE little-endian.
/// - BYTE_ARRAY - 4 byte length stored as little endian, followed by bytes.
/// - FIXED_LEN_BYTE_ARRAY - just the bytes are stored.
pub struct PlainEncoder<T: DataType> {
    buffer: Vec<u8>,
    bit_writer: BitWriter,
    _phantom: PhantomData<T>,
}

impl<T: DataType> Default for PlainEncoder<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: DataType> PlainEncoder<T> {
    /// Creates new plain encoder.
    pub fn new() -> Self {
        Self {
            buffer: vec![],
            bit_writer: BitWriter::new(256),
            _phantom: PhantomData,
        }
    }
}

impl<T: DataType> Encoder<T> for PlainEncoder<T> {
    // Performance Note:
    // As far as can be seen these functions are rarely called and as such we can hint to the
    // compiler that they dont need to be folded into hot locations in the final output.
    #[cold]
    fn encoding(&self) -> Encoding {
        Encoding::PLAIN
    }

    fn estimated_data_encoded_size(&self) -> usize {
        self.buffer.len() + self.bit_writer.bytes_written()
    }

    #[inline]
    fn flush_buffer(&mut self) -> Result<Bytes> {
        self.buffer
            .extend_from_slice(self.bit_writer.flush_buffer());
        self.bit_writer.clear();
        Ok(std::mem::take(&mut self.buffer).into())
    }

    #[inline]
    fn put(&mut self, values: &[T::T]) -> Result<()> {
        T::T::encode(values, &mut self.buffer, &mut self.bit_writer)?;
        Ok(())
    }
}

// ----------------------------------------------------------------------
// RLE encoding

const DEFAULT_RLE_BUFFER_LEN: usize = 1024;

/// RLE/Bit-Packing hybrid encoding for values.
/// Currently is used only for data pages v2 and supports boolean types.
pub struct RleValueEncoder<T: DataType> {
    // Buffer with raw values that we collect,
    // when flushing buffer they are encoded using RLE encoder
    encoder: Option<RleEncoder>,
    _phantom: PhantomData<T>,
}

impl<T: DataType> Default for RleValueEncoder<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: DataType> RleValueEncoder<T> {
    /// Creates new rle value encoder.
    pub fn new() -> Self {
        Self {
            encoder: None,
            _phantom: PhantomData,
        }
    }
}

impl<T: DataType> Encoder<T> for RleValueEncoder<T> {
    #[inline]
    fn put(&mut self, values: &[T::T]) -> Result<()> {
        ensure_phys_ty!(Type::BOOLEAN, "RleValueEncoder only supports BoolType");

        let rle_encoder = self.encoder.get_or_insert_with(|| {
            let mut buffer = Vec::with_capacity(DEFAULT_RLE_BUFFER_LEN);
            // Reserve space for length
            buffer.extend_from_slice(&[0; 4]);
            RleEncoder::new_from_buf(1, buffer)
        });

        for value in values {
            let value = value.as_u64()?;
            rle_encoder.put(value)
        }
        Ok(())
    }

    // Performance Note:
    // As far as can be seen these functions are rarely called and as such we can hint to the
    // compiler that they dont need to be folded into hot locations in the final output.
    #[cold]
    fn encoding(&self) -> Encoding {
        Encoding::RLE
    }

    #[inline]
    fn estimated_data_encoded_size(&self) -> usize {
        match self.encoder {
            Some(ref enc) => enc.len(),
            None => 0,
        }
    }

    #[inline]
    fn flush_buffer(&mut self) -> Result<Bytes> {
        ensure_phys_ty!(Type::BOOLEAN, "RleValueEncoder only supports BoolType");
        let rle_encoder = self
            .encoder
            .take()
            .expect("RLE value encoder is not initialized");

        // Flush all encoder buffers and raw values
        let mut buf = rle_encoder.consume();
        assert!(buf.len() >= 4, "should have had padding inserted");

        // Note that buf does not have any offset, all data is encoded bytes
        let len = (buf.len() - 4) as i32;
        buf[..4].copy_from_slice(&len.to_le_bytes());

        Ok(buf.into())
    }
}

// ----------------------------------------------------------------------
// DELTA_BINARY_PACKED encoding

const MAX_PAGE_HEADER_WRITER_SIZE: usize = 32;
const DEFAULT_BIT_WRITER_SIZE: usize = 1024 * 1024;
const DEFAULT_NUM_MINI_BLOCKS: usize = 4;

/// Delta bit packed encoder.
/// Consists of a header followed by blocks of delta encoded values binary packed.
///
/// Delta-binary-packing:
/// ```shell
///   [page-header] [block 1], [block 2], ... [block N]
/// ```
///
/// Each page header consists of:
/// ```shell
///   [block size] [number of miniblocks in a block] [total value count] [first value]
/// ```
///
/// Each block consists of:
/// ```shell
///   [min delta] [list of bitwidths of miniblocks] [miniblocks]
/// ```
///
/// Current implementation writes values in `put` method, multiple calls to `put` to
/// existing block or start new block if block size is exceeded. Calling `flush_buffer`
/// writes out all data and resets internal state, including page header.
///
/// Supports only INT32 and INT64.
pub struct DeltaBitPackEncoder<T: DataType> {
    page_header_writer: BitWriter,
    bit_writer: BitWriter,
    total_values: usize,
    first_value: i64,
    current_value: i64,
    block_size: usize,
    mini_block_size: usize,
    num_mini_blocks: usize,
    values_in_block: usize,
    deltas: Vec<i64>,
    _phantom: PhantomData<T>,
}

impl<T: DataType> Default for DeltaBitPackEncoder<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: DataType> DeltaBitPackEncoder<T> {
    /// Creates new delta bit packed encoder.
    pub fn new() -> Self {
        Self::assert_supported_type();

        // Size miniblocks so that they can be efficiently decoded
        let mini_block_size = match T::T::PHYSICAL_TYPE {
            Type::INT32 => 32,
            Type::INT64 => 64,
            _ => unreachable!(),
        };

        let num_mini_blocks = DEFAULT_NUM_MINI_BLOCKS;
        let block_size = mini_block_size * num_mini_blocks;
        assert_eq!(block_size % 128, 0);

        DeltaBitPackEncoder {
            page_header_writer: BitWriter::new(MAX_PAGE_HEADER_WRITER_SIZE),
            bit_writer: BitWriter::new(DEFAULT_BIT_WRITER_SIZE),
            total_values: 0,
            first_value: 0,
            current_value: 0, // current value to keep adding deltas
            block_size,       // can write fewer values than block size for last block
            mini_block_size,
            num_mini_blocks,
            values_in_block: 0, // will be at most block_size
            deltas: vec![0; block_size],
            _phantom: PhantomData,
        }
    }

    /// Writes page header for blocks, this method is invoked when we are done encoding
    /// values. It is also okay to encode when no values have been provided
    fn write_page_header(&mut self) {
        // We ignore the result of each 'put' operation, because
        // MAX_PAGE_HEADER_WRITER_SIZE is chosen to fit all header values and
        // guarantees that writes will not fail.

        // Write the size of each block
        self.page_header_writer.put_vlq_int(self.block_size as u64);
        // Write the number of mini blocks
        self.page_header_writer
            .put_vlq_int(self.num_mini_blocks as u64);
        // Write the number of all values (including non-encoded first value)
        self.page_header_writer
            .put_vlq_int(self.total_values as u64);
        // Write first value
        self.page_header_writer.put_zigzag_vlq_int(self.first_value);
    }

    // Write current delta buffer (<= 'block size' values) into bit writer
    #[inline(never)]
    fn flush_block_values(&mut self) -> Result<()> {
        if self.values_in_block == 0 {
            return Ok(());
        }

        let mut min_delta = i64::MAX;
        for i in 0..self.values_in_block {
            min_delta = cmp::min(min_delta, self.deltas[i]);
        }

        // Write min delta
        self.bit_writer.put_zigzag_vlq_int(min_delta);

        // Slice to store bit width for each mini block
        let offset = self.bit_writer.skip(self.num_mini_blocks);

        for i in 0..self.num_mini_blocks {
            // Find how many values we need to encode - either block size or whatever
            // values left
            let n = cmp::min(self.mini_block_size, self.values_in_block);
            if n == 0 {
                // Decoders should be agnostic to the padding value, we therefore use 0xFF
                // when running tests. However, not all implementations may handle this correctly
                // so pad with 0 when not running tests
                let pad_value = cfg!(test).then(|| 0xFF).unwrap_or(0);
                for j in i..self.num_mini_blocks {
                    self.bit_writer.write_at(offset + j, pad_value);
                }
                break;
            }

            // Compute the max delta in current mini block
            let mut max_delta = i64::MIN;
            for j in 0..n {
                max_delta = cmp::max(max_delta, self.deltas[i * self.mini_block_size + j]);
            }

            // Compute bit width to store (max_delta - min_delta)
            let bit_width = num_required_bits(self.subtract_u64(max_delta, min_delta)) as usize;
            self.bit_writer.write_at(offset + i, bit_width as u8);

            // Encode values in current mini block using min_delta and bit_width
            for j in 0..n {
                let packed_value =
                    self.subtract_u64(self.deltas[i * self.mini_block_size + j], min_delta);
                self.bit_writer.put_value(packed_value, bit_width);
            }

            // Pad the last block (n < mini_block_size)
            for _ in n..self.mini_block_size {
                self.bit_writer.put_value(0, bit_width);
            }

            self.values_in_block -= n;
        }

        assert_eq!(
            self.values_in_block, 0,
            "Expected 0 values in block, found {}",
            self.values_in_block
        );
        Ok(())
    }
}

// Implementation is shared between Int32Type and Int64Type,
// see `DeltaBitPackEncoderConversion` below for specifics.
impl<T: DataType> Encoder<T> for DeltaBitPackEncoder<T> {
    fn put(&mut self, values: &[T::T]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        // Define values to encode, initialize state
        let mut idx = if self.total_values == 0 {
            self.first_value = self.as_i64(values, 0);
            self.current_value = self.first_value;
            1
        } else {
            0
        };
        // Add all values (including first value)
        self.total_values += values.len();

        // Write block
        while idx < values.len() {
            let value = self.as_i64(values, idx);
            self.deltas[self.values_in_block] = self.subtract(value, self.current_value);
            self.current_value = value;
            idx += 1;
            self.values_in_block += 1;
            if self.values_in_block == self.block_size {
                self.flush_block_values()?;
            }
        }
        Ok(())
    }

    // Performance Note:
    // As far as can be seen these functions are rarely called and as such we can hint to the
    // compiler that they dont need to be folded into hot locations in the final output.
    #[cold]
    fn encoding(&self) -> Encoding {
        Encoding::DELTA_BINARY_PACKED
    }

    fn estimated_data_encoded_size(&self) -> usize {
        self.bit_writer.bytes_written()
    }

    fn flush_buffer(&mut self) -> Result<Bytes> {
        // Write remaining values
        self.flush_block_values()?;
        // Write page header with total values
        self.write_page_header();

        let mut buffer = Vec::new();
        buffer.extend_from_slice(self.page_header_writer.flush_buffer());
        buffer.extend_from_slice(self.bit_writer.flush_buffer());

        // Reset state
        self.page_header_writer.clear();
        self.bit_writer.clear();
        self.total_values = 0;
        self.first_value = 0;
        self.current_value = 0;
        self.values_in_block = 0;

        Ok(buffer.into())
    }
}

/// Helper trait to define specific conversions and subtractions when computing deltas
trait DeltaBitPackEncoderConversion<T: DataType> {
    // Method should panic if type is not supported, otherwise no-op
    fn assert_supported_type();

    fn as_i64(&self, values: &[T::T], index: usize) -> i64;

    fn subtract(&self, left: i64, right: i64) -> i64;

    fn subtract_u64(&self, left: i64, right: i64) -> u64;
}

impl<T: DataType> DeltaBitPackEncoderConversion<T> for DeltaBitPackEncoder<T> {
    #[inline]
    fn assert_supported_type() {
        ensure_phys_ty!(
            Type::INT32 | Type::INT64,
            "DeltaBitPackDecoder only supports Int32Type and Int64Type"
        );
    }

    #[inline]
    fn as_i64(&self, values: &[T::T], index: usize) -> i64 {
        values[index]
            .as_i64()
            .expect("DeltaBitPackDecoder only supports Int32Type and Int64Type")
    }

    #[inline]
    fn subtract(&self, left: i64, right: i64) -> i64 {
        // It is okay for values to overflow, wrapping_sub wrapping around at the boundary
        match T::get_physical_type() {
            Type::INT32 => (left as i32).wrapping_sub(right as i32) as i64,
            Type::INT64 => left.wrapping_sub(right),
            _ => panic!("DeltaBitPackDecoder only supports Int32Type and Int64Type"),
        }
    }

    #[inline]
    fn subtract_u64(&self, left: i64, right: i64) -> u64 {
        match T::get_physical_type() {
            // Conversion of i32 -> u32 -> u64 is to avoid non-zero left most bytes in int repr
            Type::INT32 => (left as i32).wrapping_sub(right as i32) as u32 as u64,
            Type::INT64 => left.wrapping_sub(right) as u64,
            _ => panic!("DeltaBitPackDecoder only supports Int32Type and Int64Type"),
        }
    }
}

// ----------------------------------------------------------------------
// DELTA_LENGTH_BYTE_ARRAY encoding

/// Encoding for byte arrays to separate the length values and the data.
/// The lengths are encoded using DELTA_BINARY_PACKED encoding, data is
/// stored as raw bytes.
pub struct DeltaLengthByteArrayEncoder<T: DataType> {
    // length encoder
    len_encoder: DeltaBitPackEncoder<Int32Type>,
    // byte array data
    data: Vec<ByteArray>,
    // data size in bytes of encoded values
    encoded_size: usize,
    _phantom: PhantomData<T>,
}

impl<T: DataType> Default for DeltaLengthByteArrayEncoder<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: DataType> DeltaLengthByteArrayEncoder<T> {
    /// Creates new delta length byte array encoder.
    pub fn new() -> Self {
        Self {
            len_encoder: DeltaBitPackEncoder::new(),
            data: vec![],
            encoded_size: 0,
            _phantom: PhantomData,
        }
    }
}

impl<T: DataType> Encoder<T> for DeltaLengthByteArrayEncoder<T> {
    fn put(&mut self, values: &[T::T]) -> Result<()> {
        ensure_phys_ty!(
            Type::BYTE_ARRAY | Type::FIXED_LEN_BYTE_ARRAY,
            "DeltaLengthByteArrayEncoder only supports ByteArrayType"
        );

        let val_it = || {
            values
                .iter()
                .map(|x| x.as_any().downcast_ref::<ByteArray>().unwrap())
        };

        let lengths: Vec<i32> = val_it().map(|byte_array| byte_array.len() as i32).collect();
        self.len_encoder.put(&lengths)?;
        for byte_array in val_it() {
            self.encoded_size += byte_array.len();
            self.data.push(byte_array.clone());
        }

        Ok(())
    }

    // Performance Note:
    // As far as can be seen these functions are rarely called and as such we can hint to the
    // compiler that they dont need to be folded into hot locations in the final output.
    #[cold]
    fn encoding(&self) -> Encoding {
        Encoding::DELTA_LENGTH_BYTE_ARRAY
    }

    fn estimated_data_encoded_size(&self) -> usize {
        self.len_encoder.estimated_data_encoded_size() + self.encoded_size
    }

    fn flush_buffer(&mut self) -> Result<Bytes> {
        ensure_phys_ty!(
            Type::BYTE_ARRAY | Type::FIXED_LEN_BYTE_ARRAY,
            "DeltaLengthByteArrayEncoder only supports ByteArrayType"
        );

        let mut total_bytes = vec![];
        let lengths = self.len_encoder.flush_buffer()?;
        total_bytes.extend_from_slice(&lengths);
        self.data.iter().for_each(|byte_array| {
            total_bytes.extend_from_slice(byte_array.data());
        });
        self.data.clear();
        self.encoded_size = 0;

        Ok(total_bytes.into())
    }
}

// ----------------------------------------------------------------------
// DELTA_BYTE_ARRAY encoding

/// Encoding for byte arrays, prefix lengths are encoded using DELTA_BINARY_PACKED
/// encoding, followed by suffixes with DELTA_LENGTH_BYTE_ARRAY encoding.
pub struct DeltaByteArrayEncoder<T: DataType> {
    prefix_len_encoder: DeltaBitPackEncoder<Int32Type>,
    suffix_writer: DeltaLengthByteArrayEncoder<ByteArrayType>,
    previous: Vec<u8>,
    _phantom: PhantomData<T>,
}

impl<T: DataType> Default for DeltaByteArrayEncoder<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: DataType> DeltaByteArrayEncoder<T> {
    /// Creates new delta byte array encoder.
    pub fn new() -> Self {
        Self {
            prefix_len_encoder: DeltaBitPackEncoder::new(),
            suffix_writer: DeltaLengthByteArrayEncoder::new(),
            previous: vec![],
            _phantom: PhantomData,
        }
    }
}

impl<T: DataType> Encoder<T> for DeltaByteArrayEncoder<T> {
    fn put(&mut self, values: &[T::T]) -> Result<()> {
        let mut prefix_lengths: Vec<i32> = vec![];
        let mut suffixes: Vec<ByteArray> = vec![];

        let values = values
            .iter()
            .map(|x| x.as_any())
            .map(|x| match T::get_physical_type() {
                Type::BYTE_ARRAY => x.downcast_ref::<ByteArray>().unwrap(),
                Type::FIXED_LEN_BYTE_ARRAY => x.downcast_ref::<FixedLenByteArray>().unwrap(),
                _ => panic!(
                    "DeltaByteArrayEncoder only supports ByteArrayType and FixedLenByteArrayType"
                ),
            });

        for byte_array in values {
            let current = byte_array.data();
            // Maximum prefix length that is shared between previous value and current
            // value
            let prefix_len = cmp::min(self.previous.len(), current.len());
            let mut match_len = 0;
            while match_len < prefix_len && self.previous[match_len] == current[match_len] {
                match_len += 1;
            }
            prefix_lengths.push(match_len as i32);
            suffixes.push(byte_array.slice(match_len, byte_array.len() - match_len));
            // Update previous for the next prefix
            self.previous.clear();
            self.previous.extend_from_slice(current);
        }
        self.prefix_len_encoder.put(&prefix_lengths)?;
        self.suffix_writer.put(&suffixes)?;

        Ok(())
    }

    // Performance Note:
    // As far as can be seen these functions are rarely called and as such we can hint to the
    // compiler that they dont need to be folded into hot locations in the final output.
    #[cold]
    fn encoding(&self) -> Encoding {
        Encoding::DELTA_BYTE_ARRAY
    }

    fn estimated_data_encoded_size(&self) -> usize {
        self.prefix_len_encoder.estimated_data_encoded_size()
            + self.suffix_writer.estimated_data_encoded_size()
    }

    fn flush_buffer(&mut self) -> Result<Bytes> {
        match T::get_physical_type() {
            Type::BYTE_ARRAY | Type::FIXED_LEN_BYTE_ARRAY => {
                // TODO: investigate if we can merge lengths and suffixes
                // without copying data into new vector.
                let mut total_bytes = vec![];
                // Insert lengths ...
                let lengths = self.prefix_len_encoder.flush_buffer()?;
                total_bytes.extend_from_slice(&lengths);
                // ... followed by suffixes
                let suffixes = self.suffix_writer.flush_buffer()?;
                total_bytes.extend_from_slice(&suffixes);

                self.previous.clear();
                Ok(total_bytes.into())
            }
            _ => panic!(
                "DeltaByteArrayEncoder only supports ByteArrayType and FixedLenByteArrayType"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use crate::encodings::decoding::{get_decoder, Decoder, DictDecoder, PlainDecoder};
    use crate::schema::types::{ColumnDescPtr, ColumnDescriptor, ColumnPath, Type as SchemaType};
    use crate::util::test_common::rand_gen::{random_bytes, RandGen};
    use crate::util::bit_util;

    const TEST_SET_SIZE: usize = 1024;

    #[test]
    fn test_get_encoders() {
        // supported encodings
        create_and_check_encoder::<Int32Type>(Encoding::PLAIN, None);
        create_and_check_encoder::<Int32Type>(Encoding::DELTA_BINARY_PACKED, None);
        create_and_check_encoder::<Int32Type>(Encoding::DELTA_LENGTH_BYTE_ARRAY, None);
        create_and_check_encoder::<Int32Type>(Encoding::DELTA_BYTE_ARRAY, None);
        create_and_check_encoder::<BoolType>(Encoding::RLE, None);

        // error when initializing
        create_and_check_encoder::<Int32Type>(
            Encoding::RLE_DICTIONARY,
            Some(general_err!(
                "Cannot initialize this encoding through this function"
            )),
        );
        create_and_check_encoder::<Int32Type>(
            Encoding::PLAIN_DICTIONARY,
            Some(general_err!(
                "Cannot initialize this encoding through this function"
            )),
        );

        // unsupported
        #[allow(deprecated)]
        create_and_check_encoder::<Int32Type>(
            Encoding::BIT_PACKED,
            Some(nyi_err!("Encoding BIT_PACKED is not supported")),
        );
    }

    #[test]
    fn test_bool() {
        BoolType::test(Encoding::PLAIN, TEST_SET_SIZE, -1);
        BoolType::test(Encoding::PLAIN_DICTIONARY, TEST_SET_SIZE, -1);
        BoolType::test(Encoding::RLE, TEST_SET_SIZE, -1);
    }

    #[test]
    fn test_i32() {
        Int32Type::test(Encoding::PLAIN, TEST_SET_SIZE, -1);
        Int32Type::test(Encoding::PLAIN_DICTIONARY, TEST_SET_SIZE, -1);
        Int32Type::test(Encoding::DELTA_BINARY_PACKED, TEST_SET_SIZE, -1);
    }

    #[test]
    fn test_i64() {
        Int64Type::test(Encoding::PLAIN, TEST_SET_SIZE, -1);
        Int64Type::test(Encoding::PLAIN_DICTIONARY, TEST_SET_SIZE, -1);
        Int64Type::test(Encoding::DELTA_BINARY_PACKED, TEST_SET_SIZE, -1);
    }

    #[test]
    fn test_i96() {
        Int96Type::test(Encoding::PLAIN, TEST_SET_SIZE, -1);
        Int96Type::test(Encoding::PLAIN_DICTIONARY, TEST_SET_SIZE, -1);
    }

    #[test]
    fn test_float() {
        FloatType::test(Encoding::PLAIN, TEST_SET_SIZE, -1);
        FloatType::test(Encoding::PLAIN_DICTIONARY, TEST_SET_SIZE, -1);
        FloatType::test(Encoding::BYTE_STREAM_SPLIT, TEST_SET_SIZE, -1);
    }

    #[test]
    fn test_double() {
        DoubleType::test(Encoding::PLAIN, TEST_SET_SIZE, -1);
        DoubleType::test(Encoding::PLAIN_DICTIONARY, TEST_SET_SIZE, -1);
        DoubleType::test(Encoding::BYTE_STREAM_SPLIT, TEST_SET_SIZE, -1);
    }

    #[test]
    fn test_byte_array() {
        ByteArrayType::test(Encoding::PLAIN, TEST_SET_SIZE, -1);
        ByteArrayType::test(Encoding::PLAIN_DICTIONARY, TEST_SET_SIZE, -1);
        ByteArrayType::test(Encoding::DELTA_LENGTH_BYTE_ARRAY, TEST_SET_SIZE, -1);
        ByteArrayType::test(Encoding::DELTA_BYTE_ARRAY, TEST_SET_SIZE, -1);
    }

    #[test]
    fn test_fixed_lenbyte_array() {
        FixedLenByteArrayType::test(Encoding::PLAIN, TEST_SET_SIZE, 100);
        FixedLenByteArrayType::test(Encoding::PLAIN_DICTIONARY, TEST_SET_SIZE, 100);
        FixedLenByteArrayType::test(Encoding::DELTA_BYTE_ARRAY, TEST_SET_SIZE, 100);
    }

    #[test]
    fn test_dict_encoded_size() {
        fn run_test<T: DataType>(type_length: i32, values: &[T::T], expected_size: usize) {
            let mut encoder = create_test_dict_encoder::<T>(type_length);
            assert_eq!(encoder.dict_encoded_size(), 0);
            encoder.put(values).unwrap();
            assert_eq!(encoder.dict_encoded_size(), expected_size);
            // We do not reset encoded size of the dictionary keys after flush_buffer
            encoder.flush_buffer().unwrap();
            assert_eq!(encoder.dict_encoded_size(), expected_size);
        }

        // Only 2 variations of values 1 byte each
        run_test::<BoolType>(-1, &[true, false, true, false, true], 2);
        run_test::<Int32Type>(-1, &[1i32, 2i32, 3i32, 4i32, 5i32], 20);
        run_test::<Int64Type>(-1, &[1i64, 2i64, 3i64, 4i64, 5i64], 40);
        run_test::<FloatType>(-1, &[1f32, 2f32, 3f32, 4f32, 5f32], 20);
        run_test::<DoubleType>(-1, &[1f64, 2f64, 3f64, 4f64, 5f64], 40);
        // Int96: len + reference
        run_test::<Int96Type>(
            -1,
            &[Int96::from(vec![1, 2, 3]), Int96::from(vec![2, 3, 4])],
            24,
        );
        run_test::<ByteArrayType>(-1, &[ByteArray::from("abcd"), ByteArray::from("efj")], 15);
        run_test::<FixedLenByteArrayType>(
            2,
            &[ByteArray::from("ab").into(), ByteArray::from("bc").into()],
            4,
        );
    }

    #[test]
    fn test_estimated_data_encoded_size() {
        fn run_test<T: DataType>(
            encoding: Encoding,
            type_length: i32,
            values: &[T::T],
            initial_size: usize,
            max_size: usize,
            flush_size: usize,
        ) {
            let mut encoder = match encoding {
                Encoding::PLAIN_DICTIONARY | Encoding::RLE_DICTIONARY => {
                    Box::new(create_test_dict_encoder::<T>(type_length))
                }
                _ => create_test_encoder::<T>(encoding),
            };
            assert_eq!(encoder.estimated_data_encoded_size(), initial_size);

            encoder.put(values).unwrap();
            assert_eq!(encoder.estimated_data_encoded_size(), max_size);

            encoder.flush_buffer().unwrap();
            assert_eq!(encoder.estimated_data_encoded_size(), flush_size);
        }

        // PLAIN
        run_test::<Int32Type>(Encoding::PLAIN, -1, &[123; 1024], 0, 4096, 0);

        // DICTIONARY
        // NOTE: The final size is almost the same because the dictionary entries are
        // preserved after encoded values have been written.
        run_test::<Int32Type>(Encoding::RLE_DICTIONARY, -1, &[123, 1024], 0, 2, 0);

        // DELTA_BINARY_PACKED
        run_test::<Int32Type>(Encoding::DELTA_BINARY_PACKED, -1, &[123; 1024], 0, 35, 0);

        // RLE
        let mut values = vec![];
        values.extend_from_slice(&[true; 16]);
        values.extend_from_slice(&[false; 16]);
        run_test::<BoolType>(Encoding::RLE, -1, &values, 0, 6, 0);

        // DELTA_LENGTH_BYTE_ARRAY
        run_test::<ByteArrayType>(
            Encoding::DELTA_LENGTH_BYTE_ARRAY,
            -1,
            &[ByteArray::from("ab"), ByteArray::from("abc")],
            0,
            5, // only value bytes, length encoder is not flushed yet
            0,
        );

        // DELTA_BYTE_ARRAY
        run_test::<ByteArrayType>(
            Encoding::DELTA_BYTE_ARRAY,
            -1,
            &[ByteArray::from("ab"), ByteArray::from("abc")],
            0,
            3, // only suffix bytes, length encoder is not flushed yet
            0,
        );

        // BYTE_STREAM_SPLIT
        run_test::<FloatType>(Encoding::BYTE_STREAM_SPLIT, -1, &[0.1, 0.2], 0, 8, 0);
    }

    #[test]
    fn test_byte_stream_split_example_f32() {
        // Test data from https://github.com/apache/parquet-format/blob/2a481fe1aad64ff770e21734533bb7ef5a057dac/Encodings.md#byte-stream-split-byte_stream_split--9
        let mut encoder = create_test_encoder::<FloatType>(Encoding::BYTE_STREAM_SPLIT);
        let mut decoder = create_test_decoder::<FloatType>(0, Encoding::BYTE_STREAM_SPLIT);

        let input = vec![
            f32::from_le_bytes([0xAA, 0xBB, 0xCC, 0xDD]),
            f32::from_le_bytes([0x00, 0x11, 0x22, 0x33]),
            f32::from_le_bytes([0xA3, 0xB4, 0xC5, 0xD6]),
        ];

        encoder.put(&input).unwrap();
        let encoded = encoder.flush_buffer().unwrap();

        assert_eq!(
            encoded,
            Bytes::from(vec![
                0xAA_u8, 0x00, 0xA3, 0xBB, 0x11, 0xB4, 0xCC, 0x22, 0xC5, 0xDD, 0x33, 0xD6
            ])
        );

        let mut decoded = vec![0.0; input.len()];
        decoder.set_data(encoded, input.len()).unwrap();
        decoder.get(&mut decoded).unwrap();

        assert_eq!(decoded, input);
    }

    // See: https://github.com/sunchao/parquet-rs/issues/47
    #[test]
    fn test_issue_47() {
        let mut encoder = create_test_encoder::<ByteArrayType>(Encoding::DELTA_BYTE_ARRAY);
        let mut decoder = create_test_decoder::<ByteArrayType>(0, Encoding::DELTA_BYTE_ARRAY);

        let input = vec![
            ByteArray::from("aa"),
            ByteArray::from("aaa"),
            ByteArray::from("aa"),
            ByteArray::from("aaa"),
        ];

        let mut output = vec![ByteArray::default(); input.len()];

        let mut result = put_and_get(&mut encoder, &mut decoder, &input[..2], &mut output[..2]);
        assert!(
            result.is_ok(),
            "first put_and_get() failed with: {}",
            result.unwrap_err()
        );
        result = put_and_get(&mut encoder, &mut decoder, &input[2..], &mut output[2..]);
        assert!(
            result.is_ok(),
            "second put_and_get() failed with: {}",
            result.unwrap_err()
        );
        assert_eq!(output, input);
    }

    trait EncodingTester<T: DataType> {
        fn test(enc: Encoding, total: usize, type_length: i32) {
            let result = match enc {
                Encoding::PLAIN_DICTIONARY | Encoding::RLE_DICTIONARY => {
                    Self::test_dict_internal(total, type_length)
                }
                enc => Self::test_internal(enc, total, type_length),
            };

            assert!(
                result.is_ok(),
                "Expected result to be OK but got err:\n {}",
                result.unwrap_err()
            );
        }

        fn test_internal(enc: Encoding, total: usize, type_length: i32) -> Result<()>;

        fn test_dict_internal(total: usize, type_length: i32) -> Result<()>;
    }

    impl<T: DataType + RandGen<T>> EncodingTester<T> for T {
        fn test_internal(enc: Encoding, total: usize, type_length: i32) -> Result<()> {
            let mut encoder = create_test_encoder::<T>(enc);
            let mut decoder = create_test_decoder::<T>(type_length, enc);
            let mut values = <T as RandGen<T>>::gen_vec(type_length, total);
            let mut result_data = vec![T::T::default(); total];

            // Test put/get spaced.
            let num_bytes = bit_util::ceil(total as i64, 8);
            let valid_bits = random_bytes(num_bytes as usize);
            let values_written = encoder.put_spaced(&values[..], &valid_bits[..])?;
            let data = encoder.flush_buffer()?;
            decoder.set_data(data, values_written)?;
            let _ = decoder.get_spaced(
                &mut result_data[..],
                values.len() - values_written,
                &valid_bits[..],
            )?;

            // Check equality
            for i in 0..total {
                if bit_util::get_bit(&valid_bits[..], i) {
                    assert_eq!(result_data[i], values[i]);
                } else {
                    assert_eq!(result_data[i], T::T::default());
                }
            }

            let mut actual_total = put_and_get(
                &mut encoder,
                &mut decoder,
                &values[..],
                &mut result_data[..],
            )?;
            assert_eq!(actual_total, total);
            assert_eq!(result_data, values);

            // Encode more data after flush and test with decoder

            values = <T as RandGen<T>>::gen_vec(type_length, total);
            actual_total = put_and_get(
                &mut encoder,
                &mut decoder,
                &values[..],
                &mut result_data[..],
            )?;
            assert_eq!(actual_total, total);
            assert_eq!(result_data, values);

            Ok(())
        }

        fn test_dict_internal(total: usize, type_length: i32) -> Result<()> {
            let mut encoder = create_test_dict_encoder::<T>(type_length);
            let mut values = <T as RandGen<T>>::gen_vec(type_length, total);
            encoder.put(&values[..])?;

            let mut data = encoder.flush_buffer()?;
            let mut decoder = create_test_dict_decoder::<T>();
            let mut dict_decoder = PlainDecoder::<T>::new(type_length);
            dict_decoder.set_data(encoder.write_dict()?, encoder.num_entries())?;
            decoder.set_dict(Box::new(dict_decoder))?;
            let mut result_data = vec![T::T::default(); total];
            decoder.set_data(data, total)?;
            let mut actual_total = decoder.get(&mut result_data)?;

            assert_eq!(actual_total, total);
            assert_eq!(result_data, values);

            // Encode more data after flush and test with decoder

            values = <T as RandGen<T>>::gen_vec(type_length, total);
            encoder.put(&values[..])?;
            data = encoder.flush_buffer()?;

            let mut dict_decoder = PlainDecoder::<T>::new(type_length);
            dict_decoder.set_data(encoder.write_dict()?, encoder.num_entries())?;
            decoder.set_dict(Box::new(dict_decoder))?;
            decoder.set_data(data, total)?;
            actual_total = decoder.get(&mut result_data)?;

            assert_eq!(actual_total, total);
            assert_eq!(result_data, values);

            Ok(())
        }
    }

    fn put_and_get<T: DataType>(
        encoder: &mut Box<dyn Encoder<T>>,
        decoder: &mut Box<dyn Decoder<T>>,
        input: &[T::T],
        output: &mut [T::T],
    ) -> Result<usize> {
        encoder.put(input)?;
        let data = encoder.flush_buffer()?;
        decoder.set_data(data, input.len())?;
        decoder.get(output)
    }

    fn create_and_check_encoder<T: DataType>(encoding: Encoding, err: Option<ParquetError>) {
        let encoder = get_encoder::<T>(encoding);
        match err {
            Some(parquet_error) => {
                assert_eq!(
                    encoder.err().unwrap().to_string(),
                    parquet_error.to_string()
                )
            }
            None => assert_eq!(encoder.unwrap().encoding(), encoding),
        }
    }

    // Creates test column descriptor.
    fn create_test_col_desc_ptr(type_len: i32, t: Type) -> ColumnDescPtr {
        let ty = SchemaType::primitive_type_builder("t", t)
            .with_length(type_len)
            .build()
            .unwrap();
        Arc::new(ColumnDescriptor::new(
            Arc::new(ty),
            0,
            0,
            ColumnPath::new(vec![]),
        ))
    }

    fn create_test_encoder<T: DataType>(enc: Encoding) -> Box<dyn Encoder<T>> {
        get_encoder(enc).unwrap()
    }

    fn create_test_decoder<T: DataType>(type_len: i32, enc: Encoding) -> Box<dyn Decoder<T>> {
        let desc = create_test_col_desc_ptr(type_len, T::get_physical_type());
        get_decoder(desc, enc).unwrap()
    }

    fn create_test_dict_encoder<T: DataType>(type_len: i32) -> DictEncoder<T> {
        let desc = create_test_col_desc_ptr(type_len, T::get_physical_type());
        DictEncoder::<T>::new(desc)
    }

    fn create_test_dict_decoder<T: DataType>() -> DictDecoder<T> {
        DictDecoder::<T>::new()
    }
}
