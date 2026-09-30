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

//! Numeric encoding-family adapters and delta binary packing.

use super::PlainEncoderImpl as PlainEncoder;
use super::*;

impl<T: PlainEncoderType> NumericEncodingFamily<T> {
    pub(super) fn from_encoding(encoding: Encoding, _descr: &ColumnDescPtr) -> Result<Self> {
        let physical_type = T::get_physical_type();
        match encoding {
            Encoding::PLAIN => Ok(Self::Plain(PlainEncoder::new())),
            Encoding::ALP => Ok(Self::Alp(Box::new(
                <T::T as AlpValue>::new_alp_encoder::<T>()?,
            ))),
            Encoding::DELTA_BINARY_PACKED if matches!(physical_type, Type::INT32 | Type::INT64) => {
                Ok(Self::DeltaBinaryPacked(Box::default()))
            }
            Encoding::BYTE_STREAM_SPLIT
                if matches!(
                    physical_type,
                    Type::INT32 | Type::INT64 | Type::FLOAT | Type::DOUBLE
                ) =>
            {
                Ok(Self::ByteStreamSplit(ByteStreamSplitEncoder::new()))
            }
            e => Err(unsupported_column_encoding(e, physical_type)),
        }
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

            // Pack the miniblock in one vectorized call. Its deltas are not used again.
            let start = i * self.mini_block_size;
            for j in start..start + n {
                self.deltas[j] = self.subtract_u64(self.deltas[j], min_delta) as i64;
            }
            self.bit_writer
                .put_batch(&self.deltas[start..start + n], bit_width);

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

    #[inline]
    pub(crate) fn put_i64(&mut self, value: i64) -> Result<()> {
        if self.total_values == 0 {
            self.first_value = value;
            self.current_value = value;
            self.total_values = 1;
            return Ok(());
        }

        self.total_values += 1;
        self.deltas[self.values_in_block] = self.subtract(value, self.current_value);
        self.current_value = value;
        self.values_in_block += 1;
        if self.values_in_block == self.block_size {
            self.flush_block_values()?;
        }
        Ok(())
    }

    #[inline]
    fn put_i64_values(&mut self, len: usize, mut value_at: impl FnMut(usize) -> i64) -> Result<()> {
        if len == 0 {
            return Ok(());
        }

        let mut idx = if self.total_values == 0 {
            self.first_value = value_at(0);
            self.current_value = self.first_value;
            1
        } else {
            0
        };
        self.total_values += len;

        while idx < len {
            let value = value_at(idx);
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
}

// Implementation is shared between Int32Type and Int64Type,
// see `DeltaBitPackEncoderConversion` below for specifics.
impl<T: DataType> Encoder<T> for DeltaBitPackEncoder<T> {
    fn put(&mut self, values: &[T::T]) -> Result<()> {
        self.put_i64_values(values.len(), |idx| {
            values[idx].as_i64().expect(DELTA_BIT_PACK_TYPE_ERROR)
        })
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

    /// return the estimated memory size of this encoder.
    fn estimated_memory_size(&self) -> usize {
        self.page_header_writer.estimated_memory_size()
            + self.bit_writer.estimated_memory_size()
            + self.deltas.capacity() * std::mem::size_of::<i64>()
            + std::mem::size_of::<Self>()
    }
}

/// Helper trait to define specific conversions and subtractions when computing deltas
trait DeltaBitPackEncoderConversion<T: DataType> {
    // Method should panic if type is not supported, otherwise no-op
    fn assert_supported_type();

    fn subtract(&self, left: i64, right: i64) -> i64;

    fn subtract_u64(&self, left: i64, right: i64) -> u64;
}

const DELTA_BIT_PACK_TYPE_ERROR: &str =
    "DeltaBitPackDecoder only supports Int32Type, UInt32Type, Int64Type, and UInt64Type";

impl<T: DataType> DeltaBitPackEncoderConversion<T> for DeltaBitPackEncoder<T> {
    #[inline]
    fn assert_supported_type() {
        ensure_phys_ty!(Type::INT32 | Type::INT64, "{}", DELTA_BIT_PACK_TYPE_ERROR);
    }

    #[inline]
    fn subtract(&self, left: i64, right: i64) -> i64 {
        // It is okay for values to overflow, wrapping_sub wrapping around at the boundary
        match T::get_physical_type() {
            Type::INT32 => (left as i32).wrapping_sub(right as i32) as i64,
            Type::INT64 => left.wrapping_sub(right),
            _ => panic!("{}", DELTA_BIT_PACK_TYPE_ERROR),
        }
    }

    #[inline]
    fn subtract_u64(&self, left: i64, right: i64) -> u64 {
        match T::get_physical_type() {
            // Conversion of i32 -> u32 -> u64 is to avoid non-zero left most bytes in int repr
            Type::INT32 => (left as i32).wrapping_sub(right as i32) as u32 as u64,
            Type::INT64 => left.wrapping_sub(right) as u64,
            _ => panic!("{}", DELTA_BIT_PACK_TYPE_ERROR),
        }
    }
}
