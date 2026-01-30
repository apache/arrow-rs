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
#[cfg(feature = "lz4_direct")]
use std::hash::Hasher;
#[cfg(feature = "lz4_direct")]
use twox_hash::XxHash32;

const LENGTH_NO_COMPRESSED_DATA: i64 = -1;
const LENGTH_OF_PREFIX_DATA: i64 = 8;

/// Additional context that may be needed for compression.
///
/// In the case of zstd, this will contain the zstd context, which can be reused between subsequent
/// compression calls to avoid the performance overhead of initialising a new context for every
/// compression.
pub struct CompressionContext {
    #[cfg(feature = "zstd")]
    compressor: zstd::bulk::Compressor<'static>,
}

// the reason we allow derivable_impls here is because when zstd feature is not enabled, this
// becomes derivable. however with zstd feature want to be explicit about the compression level.
#[allow(clippy::derivable_impls)]
impl Default for CompressionContext {
    fn default() -> Self {
        CompressionContext {
            // safety: `new` here will only return error here if using an invalid compression level
            #[cfg(feature = "zstd")]
            compressor: zstd::bulk::Compressor::new(zstd::DEFAULT_COMPRESSION_LEVEL)
                .expect("can use default compression level"),
        }
    }
}

impl std::fmt::Debug for CompressionContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("CompressionContext");

        #[cfg(feature = "zstd")]
        ds.field("compressor", &"zstd::bulk::Compressor");

        ds.finish()
    }
}

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
        context: &mut CompressionContext,
    ) -> Result<usize, ArrowError> {
        let uncompressed_data_len = input.len();
        let original_output_len = output.len();

        if input.is_empty() {
            // empty input, nothing to do
        } else {
            // write compressed data directly into the output buffer
            output.extend_from_slice(&uncompressed_data_len.to_le_bytes());
            self.compress(input, output, context)?;

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
    pub(crate) fn decompress_to_buffer(&self, input: &Buffer) -> Result<Buffer, ArrowError> {
        // read the first 8 bytes to determine if the data is
        // compressed
        let decompressed_length = read_uncompressed_size(input);
        let buffer = if decompressed_length == 0 {
            // empty
            Buffer::from([])
        } else if decompressed_length == LENGTH_NO_COMPRESSED_DATA {
            // no compression
            input.slice(LENGTH_OF_PREFIX_DATA as usize)
        } else if let Ok(decompressed_length) = usize::try_from(decompressed_length) {
            // decompress data using the codec
            let input_data = &input[(LENGTH_OF_PREFIX_DATA as usize)..];
            let v = self.decompress(input_data, decompressed_length as _)?;
            Buffer::from_vec(v)
        } else {
            return Err(ArrowError::IpcError(format!(
                "Invalid uncompressed length: {decompressed_length}"
            )));
        };
        Ok(buffer)
    }

    /// Compress the data in input buffer and write to output buffer
    /// using the specified compression
    fn compress(
        &self,
        input: &[u8],
        output: &mut Vec<u8>,
        context: &mut CompressionContext,
    ) -> Result<(), ArrowError> {
        match self {
            CompressionCodec::Lz4Frame => compress_lz4(input, output),
            CompressionCodec::Zstd => compress_zstd(input, output, context),
        }
    }

    /// Decompress the data in input buffer and write to output buffer
    /// using the specified compression
    fn decompress(&self, input: &[u8], decompressed_size: usize) -> Result<Vec<u8>, ArrowError> {
        let ret = match self {
            CompressionCodec::Lz4Frame => decompress_lz4(input, decompressed_size)?,
            CompressionCodec::Zstd => decompress_zstd(input, decompressed_size)?,
        };
        if ret.len() != decompressed_size {
            return Err(ArrowError::IpcError(format!(
                "Expected compressed length of {decompressed_size} got {}",
                ret.len()
            )));
        }
        Ok(ret)
    }
}

#[cfg(feature = "lz4")]
fn compress_lz4(input: &[u8], output: &mut Vec<u8>) -> Result<(), ArrowError> {
    use std::io::Write;
    let mut encoder = lz4_flex::frame::FrameEncoder::new(output);
    encoder.write_all(input)?;
    encoder
        .finish()
        .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;
    Ok(())
}

#[cfg(not(feature = "lz4"))]
#[allow(clippy::ptr_arg)]
fn compress_lz4(_input: &[u8], _output: &mut Vec<u8>) -> Result<(), ArrowError> {
    Err(ArrowError::InvalidArgumentError(
        "lz4 IPC compression requires the lz4 feature".to_string(),
    ))
}

#[cfg(all(feature = "lz4", not(feature = "lz4_direct")))]
fn decompress_lz4(input: &[u8], decompressed_size: usize) -> Result<Vec<u8>, ArrowError> {
    use std::io::Read;
    let mut output = Vec::with_capacity(decompressed_size);
    lz4_flex::frame::FrameDecoder::new(input).read_to_end(&mut output)?;
    Ok(output)
}

#[cfg(feature = "lz4_direct")]
fn decompress_lz4(input: &[u8], decompressed_size: usize) -> Result<Vec<u8>, ArrowError> {
    const MAGIC: u32 = 0x184D2204;
    const INCOMPRESSIBLE_MASK: u32 = 0x8000_0000;
    const FLG_RESERVED_MASK: u8 = 0b0000_0010;
    const FLG_DICTIONARY_ID: u8 = 0b0000_0001;
    const BD_BLOCK_SIZE_MASK: u8 = 0b0111_0000;
    const BD_RESERVED_MASK: u8 = !BD_BLOCK_SIZE_MASK;

    let mut offset = 0usize;
    let magic = read_u32_le(input, &mut offset)?;
    if magic != MAGIC {
        return Err(ArrowError::IpcError(
            "Invalid LZ4 frame magic (lz4_direct does not support legacy or skippable frames)"
                .to_string(),
        ));
    }

    let header_start = offset;
    let flg = read_u8(input, &mut offset)?;
    let bd = read_u8(input, &mut offset)?;

    let reserved = flg & FLG_RESERVED_MASK;
    let content_checksum = (flg >> 2) & 0b1;
    let content_size = (flg >> 3) & 0b1;
    let block_checksum = (flg >> 4) & 0b1;
    let block_independence = (flg >> 5) & 0b1;
    let version = (flg >> 6) & 0b11;
    let dict_id_flag = (flg & FLG_DICTIONARY_ID) != 0;

    if reserved != 0 || (bd & BD_RESERVED_MASK) != 0 || block_independence != 1 || version != 1 {
        return Err(ArrowError::IpcError(
            "Unsupported LZ4 frame flags (lz4_direct requires version=1, independent blocks, and no reserved bits)".to_string(),
        ));
    }

    let block_size_value = (bd >> 4) & 0b111;
    let max_block_size = match block_size_value {
        4 => 64 * 1024,
        5 => 256 * 1024,
        6 => 1024 * 1024,
        7 => 4 * 1024 * 1024,
        _ => {
            return Err(ArrowError::IpcError("Invalid LZ4 block size".to_string()));
        }
    };

    let mut expected_content_size = None;
    if content_size == 1 {
        expected_content_size = Some(read_u64_le(input, &mut offset)?);
    }

    let dict_id = if dict_id_flag {
        Some(read_u32_le(input, &mut offset)?)
    } else {
        None
    };

    let header_checksum_offset = offset;
    let expected_header_checksum = read_u8(input, &mut offset)?;
    let mut header_hasher = XxHash32::with_seed(0);
    header_hasher.write(&input[header_start..header_checksum_offset]);
    let header_checksum = (header_hasher.finish() >> 8) as u8;
    if header_checksum != expected_header_checksum {
        return Err(ArrowError::IpcError("Invalid LZ4 header checksum".to_string()));
    }

    if dict_id.is_some() {
        return Err(ArrowError::IpcError(
            "LZ4 dictionary IDs are not supported (lz4_direct does not support dictionaries)"
                .to_string(),
        ));
    }

    let mut output = vec![0u8; decompressed_size];
    let mut write_offset = 0usize;
    let mut content_hasher = if content_checksum == 1 {
        Some(XxHash32::with_seed(0))
    } else {
        None
    };
    let mut content_len = 0u64;

    loop {
        let raw_block_size = read_u32_le(input, &mut offset)?;
        if raw_block_size == 0 {
            break;
        }

        let compressed = (raw_block_size & INCOMPRESSIBLE_MASK) == 0;
        let block_size = (raw_block_size & !INCOMPRESSIBLE_MASK) as usize;
        if block_size > max_block_size {
            return Err(ArrowError::IpcError("LZ4 block too large".to_string()));
        }
        let block = read_slice(input, &mut offset, block_size)?;

        if block_checksum == 1 {
            let expected_checksum = read_u32_le(input, &mut offset)?;
            let mut block_hasher = XxHash32::with_seed(0);
            block_hasher.write(block);
            let block_hash = block_hasher.finish() as u32;
            if block_hash != expected_checksum {
                return Err(ArrowError::IpcError(
                    "Invalid LZ4 block checksum".to_string(),
                ));
            }
        }

        if compressed {
            let dst = output
                .get_mut(write_offset..)
                .ok_or_else(|| ArrowError::IpcError("Output buffer overflow".to_string()))?;
            let wrote = lz4_flex::block::decompress_into(block, dst)
                .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;
            if write_offset + wrote > output.len() {
                return Err(ArrowError::IpcError("Output buffer overflow".to_string()));
            }
            let out_slice = &output[write_offset..write_offset + wrote];
            if let Some(ref mut hasher) = content_hasher {
                hasher.write(out_slice);
            }
            content_len = content_len.saturating_add(wrote as u64);
            write_offset = write_offset
                .checked_add(wrote)
                .ok_or_else(|| ArrowError::IpcError("Output size overflow".to_string()))?;
        } else {
            let end = write_offset
                .checked_add(block.len())
                .ok_or_else(|| ArrowError::IpcError("Output size overflow".to_string()))?;
            if end > output.len() {
                return Err(ArrowError::IpcError("Output buffer overflow".to_string()));
            }
            output[write_offset..end].copy_from_slice(block);
            let out_slice = &output[write_offset..end];
            if let Some(ref mut hasher) = content_hasher {
                hasher.write(out_slice);
            }
            content_len = content_len.saturating_add(block.len() as u64);
            write_offset = end;
        }
    }

    if content_checksum == 1 {
        let expected_content_checksum = read_u32_le(input, &mut offset)?;
        let content_hash = content_hasher
            .as_ref()
            .map(|hasher| hasher.finish() as u32)
            .unwrap_or(0);
        if content_hash != expected_content_checksum {
            return Err(ArrowError::IpcError(
                "Invalid LZ4 content checksum".to_string(),
            ));
        }
    }

    if let Some(expected) = expected_content_size {
        if content_len != expected {
            return Err(ArrowError::IpcError(
                "LZ4 content size mismatch".to_string(),
            ));
        }
    }

    if write_offset != output.len() {
        output.truncate(write_offset);
    }
    Ok(output)
}

#[cfg(feature = "lz4_direct")]
fn read_u8(input: &[u8], offset: &mut usize) -> Result<u8, ArrowError> {
    if *offset >= input.len() {
        return Err(ArrowError::IpcError("Unexpected end of LZ4 frame".to_string()));
    }
    let value = input[*offset];
    *offset += 1;
    Ok(value)
}

#[cfg(feature = "lz4_direct")]
fn read_u32_le(input: &[u8], offset: &mut usize) -> Result<u32, ArrowError> {
    if *offset + 4 > input.len() {
        return Err(ArrowError::IpcError("Unexpected end of LZ4 frame".to_string()));
    }
    let value = u32::from_le_bytes(
        input[*offset..*offset + 4]
            .try_into()
            .expect("slice length checked"),
    );
    *offset += 4;
    Ok(value)
}

#[cfg(feature = "lz4_direct")]
fn read_u64_le(input: &[u8], offset: &mut usize) -> Result<u64, ArrowError> {
    if *offset + 8 > input.len() {
        return Err(ArrowError::IpcError("Unexpected end of LZ4 frame".to_string()));
    }
    let value = u64::from_le_bytes(
        input[*offset..*offset + 8]
            .try_into()
            .expect("slice length checked"),
    );
    *offset += 8;
    Ok(value)
}

#[cfg(feature = "lz4_direct")]
fn read_slice<'a>(
    input: &'a [u8],
    offset: &mut usize,
    len: usize,
) -> Result<&'a [u8], ArrowError> {
    if *offset + len > input.len() {
        return Err(ArrowError::IpcError("Unexpected end of LZ4 frame".to_string()));
    }
    let slice = &input[*offset..*offset + len];
    *offset += len;
    Ok(slice)
}

#[cfg(not(feature = "lz4"))]
#[allow(clippy::ptr_arg)]
fn decompress_lz4(_input: &[u8], _decompressed_size: usize) -> Result<Vec<u8>, ArrowError> {
    Err(ArrowError::InvalidArgumentError(
        "lz4 IPC decompression requires the lz4 feature".to_string(),
    ))
}

#[cfg(feature = "zstd")]
fn compress_zstd(
    input: &[u8],
    output: &mut Vec<u8>,
    context: &mut CompressionContext,
) -> Result<(), ArrowError> {
    let result = context.compressor.compress(input)?;
    output.extend_from_slice(&result);
    Ok(())
}

#[cfg(not(feature = "zstd"))]
#[allow(clippy::ptr_arg)]
fn compress_zstd(
    _input: &[u8],
    _output: &mut Vec<u8>,
    _context: &mut CompressionContext,
) -> Result<(), ArrowError> {
    Err(ArrowError::InvalidArgumentError(
        "zstd IPC compression requires the zstd feature".to_string(),
    ))
}

#[cfg(feature = "zstd")]
fn decompress_zstd(input: &[u8], decompressed_size: usize) -> Result<Vec<u8>, ArrowError> {
    use std::io::Read;
    let mut output = Vec::with_capacity(decompressed_size);
    zstd::Decoder::with_buffer(input)?.read_to_end(&mut output)?;
    Ok(output)
}

#[cfg(not(feature = "zstd"))]
#[allow(clippy::ptr_arg)]
fn decompress_zstd(_input: &[u8], _decompressed_size: usize) -> Result<Vec<u8>, ArrowError> {
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
    #[cfg(feature = "lz4_direct")]
    use lz4_flex::frame::{BlockMode, BlockSize, FrameEncoder, FrameInfo};
    #[cfg(feature = "lz4_direct")]
    use std::io::Write;
    #[cfg(feature = "lz4_direct")]
    use twox_hash::XxHash32;

    #[test]
    #[cfg(feature = "lz4")]
    fn test_lz4_compression() {
        let input_bytes = b"hello lz4";
        let codec = super::CompressionCodec::Lz4Frame;
        let mut output_bytes: Vec<u8> = Vec::new();
        codec
            .compress(input_bytes, &mut output_bytes, &mut Default::default())
            .unwrap();
        let result = codec
            .decompress(output_bytes.as_slice(), input_bytes.len())
            .unwrap();
        assert_eq!(input_bytes, result.as_slice());
    }

    #[cfg(feature = "lz4_direct")]
    struct FrameLayout {
        header_start: usize,
        header_checksum_offset: usize,
        block_start: usize,
        block_checksums: bool,
        content_checksum: bool,
        content_size_offset: Option<usize>,
    }

    #[cfg(feature = "lz4_direct")]
    fn parse_frame_layout(frame: &[u8]) -> FrameLayout {
        let mut offset = 0usize;
        let magic = super::read_u32_le(frame, &mut offset).unwrap();
        assert_eq!(magic, 0x184D2204);

        let header_start = offset;
        let flg = super::read_u8(frame, &mut offset).unwrap();
        let _bd = super::read_u8(frame, &mut offset).unwrap();

        let block_checksums = ((flg >> 4) & 0b1) == 1;
        let content_checksum = ((flg >> 2) & 0b1) == 1;
        let content_size_flag = ((flg >> 3) & 0b1) == 1;
        let dict_id_flag = (flg & 0b1) == 1;

        let mut content_size_offset = None;
        if content_size_flag {
            content_size_offset = Some(offset);
            offset += 8;
        }
        if dict_id_flag {
            offset += 4;
        }

        let header_checksum_offset = offset;
        offset += 1;

        FrameLayout {
            header_start,
            header_checksum_offset,
            block_start: offset,
            block_checksums,
            content_checksum,
            content_size_offset,
        }
    }

    #[cfg(feature = "lz4_direct")]
    fn content_checksum_offset(frame: &[u8], layout: &FrameLayout) -> Option<usize> {
        if !layout.content_checksum {
            return None;
        }
        let mut offset = layout.block_start;
        loop {
            let raw_block_size = super::read_u32_le(frame, &mut offset).unwrap();
            if raw_block_size == 0 {
                break;
            }
            let block_size = (raw_block_size & !0x8000_0000) as usize;
            offset += block_size;
            if layout.block_checksums {
                offset += 4;
            }
        }
        Some(offset)
    }

    #[cfg(feature = "lz4_direct")]
    fn encode_frame(info: FrameInfo, input: &[u8]) -> Vec<u8> {
        let mut output = Vec::new();
        let mut encoder = FrameEncoder::with_frame_info(info, &mut output);
        encoder.write_all(input).unwrap();
        encoder.finish().unwrap();
        output
    }

    #[test]
    #[cfg(feature = "lz4_direct")]
    fn test_lz4_header_checksum_mismatch() {
        let input_bytes = b"hello lz4 checksum";
        let info = FrameInfo::new()
            .block_mode(BlockMode::Independent)
            .block_size(BlockSize::Max64KB);
        let mut frame = encode_frame(info, input_bytes);
        let layout = parse_frame_layout(&frame);
        frame[layout.header_checksum_offset] ^= 0xFF;

        let codec = super::CompressionCodec::Lz4Frame;
        let err = codec
            .decompress(frame.as_slice(), input_bytes.len())
            .unwrap_err()
            .to_string();
        assert!(err.contains("Invalid LZ4 header checksum"));
    }

    #[test]
    #[cfg(feature = "lz4_direct")]
    fn test_lz4_block_checksum_mismatch() {
        let input_bytes = b"block checksum should fail";
        let info = FrameInfo::new()
            .block_mode(BlockMode::Independent)
            .block_size(BlockSize::Max64KB)
            .block_checksums(true)
            .content_size(Some(input_bytes.len() as u64));
        let mut frame = encode_frame(info, input_bytes);
        let layout = parse_frame_layout(&frame);

        let block_data_start = layout.block_start + 4;
        frame[block_data_start] ^= 0xFF;

        let codec = super::CompressionCodec::Lz4Frame;
        let err = codec
            .decompress(frame.as_slice(), input_bytes.len())
            .unwrap_err()
            .to_string();
        assert!(err.contains("Invalid LZ4 block checksum"));
    }

    #[test]
    #[cfg(feature = "lz4_direct")]
    fn test_lz4_content_checksum_mismatch() {
        let input_bytes = b"content checksum should fail";
        let info = FrameInfo::new()
            .block_mode(BlockMode::Independent)
            .block_size(BlockSize::Max64KB)
            .content_checksum(true)
            .content_size(Some(input_bytes.len() as u64));
        let mut frame = encode_frame(info, input_bytes);
        let layout = parse_frame_layout(&frame);
        let checksum_offset = content_checksum_offset(&frame, &layout).unwrap();
        frame[checksum_offset] ^= 0xFF;

        let codec = super::CompressionCodec::Lz4Frame;
        let err = codec
            .decompress(frame.as_slice(), input_bytes.len())
            .unwrap_err()
            .to_string();
        assert!(err.contains("Invalid LZ4 content checksum"));
    }

    #[test]
    #[cfg(feature = "lz4_direct")]
    fn test_lz4_content_size_mismatch() {
        let input_bytes = b"content size mismatch";
        let info = FrameInfo::new()
            .block_mode(BlockMode::Independent)
            .block_size(BlockSize::Max64KB)
            .content_size(Some(input_bytes.len() as u64));
        let mut frame = encode_frame(info, input_bytes);
        let layout = parse_frame_layout(&frame);
        let size_offset = layout.content_size_offset.expect("content size present");
        let wrong_size = (input_bytes.len() as u64).saturating_add(1);
        frame[size_offset..size_offset + 8].copy_from_slice(&wrong_size.to_le_bytes());

        let mut hasher = XxHash32::with_seed(0);
        hasher.write(&frame[layout.header_start..layout.header_checksum_offset]);
        let header_checksum = (hasher.finish() >> 8) as u8;
        frame[layout.header_checksum_offset] = header_checksum;

        let codec = super::CompressionCodec::Lz4Frame;
        let err = codec
            .decompress(frame.as_slice(), input_bytes.len())
            .unwrap_err()
            .to_string();
        assert!(err.contains("LZ4 content size mismatch"));
    }

    #[test]
    #[cfg(feature = "zstd")]
    fn test_zstd_compression() {
        let input_bytes = b"hello zstd";
        let codec = super::CompressionCodec::Zstd;
        let mut output_bytes: Vec<u8> = Vec::new();
        codec
            .compress(input_bytes, &mut output_bytes, &mut Default::default())
            .unwrap();
        let result = codec
            .decompress(output_bytes.as_slice(), input_bytes.len())
            .unwrap();
        assert_eq!(input_bytes, result.as_slice());
    }
}
