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

use crate::CompressionType;
use arrow_buffer::Buffer;
use arrow_schema::ArrowError;

const LENGTH_NO_COMPRESSED_DATA: i64 = -1;
const LENGTH_OF_PREFIX_DATA: i64 = 8;

/// Represents compressing a ipc stream using a particular compression algorithm
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionCodec {
    Lz4Frame,
    Zstd,
}

impl TryFrom<CompressionType> for CompressionCodec {
    type Error = ArrowError;

    fn try_from(compression_type: CompressionType) -> Result<Self, ArrowError> {
        match compression_type {
            CompressionType::ZSTD => Ok(CompressionCodec::Zstd),
            CompressionType::LZ4_FRAME => Ok(CompressionCodec::Lz4Frame),
            other_type => Err(ArrowError::NotYetImplemented(format!(
                "compression type {other_type:?} not supported "
            ))),
        }
    }
}

impl CompressionCodec {
    /// Compresses the data in `input` to `output` and appends the
    /// data using the specified compression mechanism.
    ///
    /// returns the number of bytes written to the stream
    ///
    /// Writes this format to output:
    /// ```text
    /// [8 bytes]:         uncompressed length
    /// [remaining bytes]: compressed data stream
    /// ```
    pub(crate) fn compress_to_vec(
        &self,
        input: &[u8],
        output: &mut Vec<u8>,
    ) -> Result<usize, ArrowError> {
        let uncompressed_data_len = input.len();
        let original_output_len = output.len();

        if input.is_empty() {
            // empty input, nothing to do
        } else {
            // write compressed data directly into the output buffer
            output.extend_from_slice(&uncompressed_data_len.to_le_bytes());
            self.compress(input, output)?;

            let compression_len = output.len() - original_output_len;
            if compression_len > uncompressed_data_len {
                // length of compressed data was larger than
                // uncompressed data, use the uncompressed data with
                // length -1 to indicate that we don't compress the
                // data
                output.truncate(original_output_len);
                output.extend_from_slice(&LENGTH_NO_COMPRESSED_DATA.to_le_bytes());
                output.extend_from_slice(input);
            }
        }
        Ok(output.len() - original_output_len)
    }

    /// Decompresses the input into a [`Buffer`]
    ///
    /// The input should look like:
    /// ```text
    /// [8 bytes]:         uncompressed length
    /// [remaining bytes]: compressed data stream
    /// ```
    pub(crate) fn decompress_to_buffer(
        &self,
        input: &Buffer,
    ) -> Result<Buffer, ArrowError> {
        // read the first 8 bytes to determine if the data is
        // compressed
        let decompressed_length = read_uncompressed_size(input);
        let buffer = if decompressed_length == 0 {
            // empty
            Buffer::from([])
        } else if decompressed_length == LENGTH_NO_COMPRESSED_DATA {
            // no compression
            input.slice(LENGTH_OF_PREFIX_DATA as usize)
        } else {
            // decompress data using the codec
            let mut uncompressed_buffer =
                Vec::with_capacity(decompressed_length as usize);
            let input_data = &input[(LENGTH_OF_PREFIX_DATA as usize)..];
            self.decompress(input_data, &mut uncompressed_buffer)?;
            Buffer::from(uncompressed_buffer)
        };
        Ok(buffer)
    }

    /// Compress the data in input buffer and write to output buffer
    /// using the specified compression
    fn compress(&self, input: &[u8], output: &mut Vec<u8>) -> Result<(), ArrowError> {
        match self {
            CompressionCodec::Lz4Frame => compress_lz4(input, output),
            CompressionCodec::Zstd => compress_zstd(input, output),
        }
    }

    /// Decompress the data in input buffer and write to output buffer
    /// using the specified compression
    fn decompress(
        &self,
        input: &[u8],
        output: &mut Vec<u8>,
    ) -> Result<usize, ArrowError> {
        match self {
            CompressionCodec::Lz4Frame => decompress_lz4(input, output),
            CompressionCodec::Zstd => decompress_zstd(input, output),
        }
    }
}

#[cfg(feature = "lz4")]
fn compress_lz4(input: &[u8], output: &mut Vec<u8>) -> Result<(), ArrowError> {
    use std::io::Write;
    let mut encoder = lz4::EncoderBuilder::new().build(output)?;
    encoder.write_all(input)?;
    encoder.finish().1?;
    Ok(())
}

#[cfg(not(feature = "lz4"))]
#[allow(clippy::ptr_arg)]
fn compress_lz4(_input: &[u8], _output: &mut Vec<u8>) -> Result<(), ArrowError> {
    Err(ArrowError::InvalidArgumentError(
        "lz4 IPC compression requires the lz4 feature".to_string(),
    ))
}

#[cfg(feature = "lz4")]
fn decompress_lz4(input: &[u8], output: &mut Vec<u8>) -> Result<usize, ArrowError> {
    use std::io::Read;
    Ok(lz4::Decoder::new(input)?.read_to_end(output)?)
}

#[cfg(not(feature = "lz4"))]
#[allow(clippy::ptr_arg)]
fn decompress_lz4(_input: &[u8], _output: &mut Vec<u8>) -> Result<usize, ArrowError> {
    Err(ArrowError::InvalidArgumentError(
        "lz4 IPC decompression requires the lz4 feature".to_string(),
    ))
}

#[cfg(feature = "zstd")]
fn compress_zstd(input: &[u8], output: &mut Vec<u8>) -> Result<(), ArrowError> {
    use std::io::Write;
    let mut encoder = zstd::Encoder::new(output, 0)?;
    encoder.write_all(input)?;
    encoder.finish()?;
    Ok(())
}

#[cfg(not(feature = "zstd"))]
#[allow(clippy::ptr_arg)]
fn compress_zstd(_input: &[u8], _output: &mut Vec<u8>) -> Result<(), ArrowError> {
    Err(ArrowError::InvalidArgumentError(
        "zstd IPC compression requires the zstd feature".to_string(),
    ))
}

#[cfg(feature = "zstd")]
fn decompress_zstd(input: &[u8], output: &mut Vec<u8>) -> Result<usize, ArrowError> {
    use std::io::Read;
    Ok(zstd::Decoder::new(input)?.read_to_end(output)?)
}

#[cfg(not(feature = "zstd"))]
#[allow(clippy::ptr_arg)]
fn decompress_zstd(_input: &[u8], _output: &mut Vec<u8>) -> Result<usize, ArrowError> {
    Err(ArrowError::InvalidArgumentError(
        "zstd IPC decompression requires the zstd feature".to_string(),
    ))
}

/// Get the uncompressed length
/// Notes:
///   LENGTH_NO_COMPRESSED_DATA: indicate that the data that follows is not compressed
///    0: indicate that there is no data
///   positive number: indicate the uncompressed length for the following data
#[inline]
fn read_uncompressed_size(buffer: &[u8]) -> i64 {
    let len_buffer = &buffer[0..8];
    // 64-bit little-endian signed integer
    i64::from_le_bytes(len_buffer.try_into().unwrap())
}

#[cfg(test)]
mod tests {
    #[test]
    #[cfg(feature = "lz4")]
    fn test_lz4_compression() {
        let input_bytes = "hello lz4".as_bytes();
        let codec = super::CompressionCodec::Lz4Frame;
        let mut output_bytes: Vec<u8> = Vec::new();
        codec.compress(input_bytes, &mut output_bytes).unwrap();
        let mut result_output_bytes: Vec<u8> = Vec::new();
        codec
            .decompress(output_bytes.as_slice(), &mut result_output_bytes)
            .unwrap();
        assert_eq!(input_bytes, result_output_bytes.as_slice());
    }

    #[test]
    #[cfg(feature = "zstd")]
    fn test_zstd_compression() {
        let input_bytes = "hello zstd".as_bytes();
        let codec = super::CompressionCodec::Zstd;
        let mut output_bytes: Vec<u8> = Vec::new();
        codec.compress(input_bytes, &mut output_bytes).unwrap();
        let mut result_output_bytes: Vec<u8> = Vec::new();
        codec
            .decompress(output_bytes.as_slice(), &mut result_output_bytes)
            .unwrap();
        assert_eq!(input_bytes, result_output_bytes.as_slice());
    }
}
