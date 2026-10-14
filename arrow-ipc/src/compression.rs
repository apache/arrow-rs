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
use flatbuffers::FlatBufferBuilder;

const LENGTH_NO_COMPRESSED_DATA: i64 = -1;
const LENGTH_OF_PREFIX_DATA: i64 = 8;
const DEFAULT_ZSTD_COMPRESSION_LEVEL: i32 = 3;

/// Additional context that may be needed for compression.
///
/// In the case of zstd, this will contain the zstd context, which can be reused between subsequent
/// compression calls to avoid the performance overhead of initialising a new context for every
/// compression. Also holds a [`FlatBufferBuilder`] that is reused across IPC writes.
#[derive(Default)]
pub struct IpcWriteContext {
    scratch: Vec<u8>,
    reserve_scratch: bool,
    fbb: FlatBufferBuilder<'static>,
    #[cfg(feature = "zstd")]
    compressor: Option<zstd::bulk::Compressor<'static>>,
}

impl IpcWriteContext {
    /// Get a mutable reference to the [`FlatBufferBuilder`] that is reused across IPC writes.
    pub(crate) fn mut_fbb(&mut self) -> &mut FlatBufferBuilder<'static> {
        &mut self.fbb
    }

    /// Set whether the scratch buffer capacity should be reserved after each encode for reuse
    /// on the next call. Set to `false` for the final batch in a sequence to avoid a
    /// pointless allocation. by default, this is set to `false`.
    pub fn set_reserve_scratch(&mut self, reserve: bool) {
        self.reserve_scratch = reserve;
    }
    /// Reserve the scratch buffer capacity for reuse on the next call. This is a no-op if
    /// `reserve_scratch` is set to `false`.
    pub(crate) fn reserve_scratch_with_capacity(&mut self, additional: usize) {
        if self.reserve_scratch {
            self.scratch.reserve(additional);
        }
    }
    pub(crate) fn scratch(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.scratch)
    }

    #[cfg(feature = "zstd")]
    fn zstd_compressor(&mut self, level: i32) -> &mut zstd::bulk::Compressor<'static> {
        self.compressor.get_or_insert_with(|| {
            zstd::bulk::Compressor::new(level).expect("can use default compression level")
        })
    }
}

impl std::fmt::Debug for IpcWriteContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("IpcWriteContext");

        #[cfg(feature = "zstd")]
        ds.field(
            "compressor",
            &self.compressor.as_ref().map(|_| "zstd::bulk::Compressor"),
        );

        ds.finish()
    }
}

/// Deprecated alias for [`IpcWriteContext`].
#[deprecated(since = "59.1.0", note = "Use IpcWriteContext instead")]
pub type CompressionContext = IpcWriteContext;

/// Additional context that may be needed for decompression.
///
/// In the case of zstd, this will contain the zstd decompression context, which can be reused
/// between subsequent decompression calls to avoid the performance overhead of initialising a new
/// context for every decompression.
pub struct DecompressionContext {
    #[cfg(feature = "zstd")]
    decompressor: Option<zstd::bulk::Decompressor<'static>>,
}

impl DecompressionContext {
    pub(crate) fn new() -> Self {
        Default::default()
    }

    #[cfg(feature = "zstd")]
    fn zstd_decompressor(&mut self) -> &mut zstd::bulk::Decompressor<'static> {
        self.decompressor.get_or_insert_with(|| {
            zstd::bulk::Decompressor::new().expect("can create zstd decompressor")
        })
    }
}

#[expect(clippy::derivable_impls)]
impl Default for DecompressionContext {
    fn default() -> Self {
        DecompressionContext {
            #[cfg(feature = "zstd")]
            decompressor: None,
        }
    }
}

impl std::fmt::Debug for DecompressionContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("DecompressionContext");

        #[cfg(feature = "zstd")]
        ds.field(
            "decompressor",
            &self
                .decompressor
                .as_ref()
                .map(|_| "zstd::bulk::Decompressor"),
        );

        ds.finish()
    }
}

/// Represents compressing a ipc stream using a particular compression algorithm
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionCodec {
    Lz4Frame,
    Zstd(i32),
}

impl TryFrom<CompressionType> for CompressionCodec {
    type Error = ArrowError;

    fn try_from(compression_type: CompressionType) -> Result<Self, ArrowError> {
        match compression_type {
            CompressionType::ZSTD => Ok(CompressionCodec::Zstd(DEFAULT_ZSTD_COMPRESSION_LEVEL)),
            CompressionType::LZ4_FRAME => Ok(CompressionCodec::Lz4Frame),
            other_type => Err(ArrowError::NotYetImplemented(format!(
                "compression type {other_type:?} not supported "
            ))),
        }
    }
}

impl CompressionCodec {
    /// Creates a [`CompressionCodec`] with an explicit compression level.
    ///
    /// The level is used for [`CompressionType::ZSTD`].
    /// [`CompressionType::LZ4_FRAME`] does not yet support compression levels
    /// and ignores this value. Returns an error for unsupported compression
    /// types.
    pub(crate) fn try_new_with_compression_level(
        compression_type: CompressionType,
        compression_level: i32,
    ) -> Result<Self, ArrowError> {
        match compression_type {
            CompressionType::ZSTD => Ok(CompressionCodec::Zstd(compression_level)),
            CompressionType::LZ4_FRAME => Ok(CompressionCodec::Lz4Frame),
            other_type => Err(ArrowError::NotYetImplemented(format!(
                "compression type {other_type:?} not supported "
            ))),
        }
    }

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
        context: &mut IpcWriteContext,
    ) -> Result<usize, ArrowError> {
        let uncompressed_data_len = input.len();
        let original_output_len = output.len();

        if input.is_empty() {
            // empty input, nothing to do
        } else {
            // write compressed data directly into the output buffer
            output.extend_from_slice(&(uncompressed_data_len as i64).to_le_bytes());
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
    pub(crate) fn decompress_to_buffer(
        &self,
        input: &Buffer,
        context: &mut DecompressionContext,
    ) -> Result<Buffer, ArrowError> {
        // read the first 8 bytes to determine if the data is
        // compressed
        let decompressed_length = read_uncompressed_size(input)?;
        let buffer = if decompressed_length == 0 {
            // empty
            Buffer::from([])
        } else if decompressed_length == LENGTH_NO_COMPRESSED_DATA {
            // no compression
            input.slice(LENGTH_OF_PREFIX_DATA as usize)
        } else if let Ok(decompressed_length) = usize::try_from(decompressed_length) {
            // decompress data using the codec
            let input_data = &input[(LENGTH_OF_PREFIX_DATA as usize)..];
            let v = self.decompress(input_data, decompressed_length, context)?;
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
        context: &mut IpcWriteContext,
    ) -> Result<(), ArrowError> {
        match self {
            CompressionCodec::Lz4Frame => compress_lz4(input, output),
            CompressionCodec::Zstd(level) => compress_zstd(input, output, context, *level),
        }
    }

    /// Decompress the data in input buffer and write to output buffer
    /// using the specified compression
    fn decompress(
        &self,
        input: &[u8],
        decompressed_size: usize,
        context: &mut DecompressionContext,
    ) -> Result<Vec<u8>, ArrowError> {
        let ret = match self {
            CompressionCodec::Lz4Frame => decompress_lz4(input, decompressed_size)?,
            CompressionCodec::Zstd(_) => decompress_zstd(input, decompressed_size, context)?,
        };
        if ret.len() != decompressed_size {
            return Err(ArrowError::IpcError(format!(
                "Expected decompressed length of {decompressed_size} got {}",
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
fn compress_lz4(_input: &[u8], _output: &mut Vec<u8>) -> Result<(), ArrowError> {
    Err(ArrowError::InvalidArgumentError(
        "lz4 IPC compression requires the lz4 feature".to_string(),
    ))
}

#[cfg(feature = "lz4")]
fn decompress_lz4(input: &[u8], decompressed_size: usize) -> Result<Vec<u8>, ArrowError> {
    use std::io::Read;
    if let Some(output) = decompress_lz4_frame(input, decompressed_size)? {
        return Ok(output);
    }
    let mut output = Vec::with_capacity(decompressed_size);
    let mut decoder = lz4_flex::frame::FrameDecoder::new(input);
    decoder
        .by_ref()
        .take(decompressed_size as u64)
        .read_to_end(&mut output)?;

    // Probe without growing `output` to reject data exceeding the advertised size.
    if decoder.read(&mut [0])? != 0 {
        return Err(ArrowError::IpcError(format!(
            "LZ4 decompressed buffer exceeds advertised size of {decompressed_size}"
        )));
    }
    Ok(output)
}

/// Decodes a single LZ4 frame straight into an output of the advertised size, avoiding the
/// per-buffer scratch allocations and copies of [`lz4_flex::frame::FrameDecoder`].
///
/// Returns `None` for frames this does not handle (checksums, dictionary ids, legacy or
/// concatenated frames), which then go through the `FrameDecoder`.
#[cfg(feature = "lz4")]
fn decompress_lz4_frame(
    input: &[u8],
    decompressed_size: usize,
) -> Result<Option<Vec<u8>>, ArrowError> {
    const MAGIC: u32 = 0x184D2204;
    const FLG_INDEPENDENT_BLOCKS: u8 = 0x20;
    const FLG_BLOCK_CHECKSUMS: u8 = 0x10;
    const FLG_CONTENT_SIZE: u8 = 0x08;
    const FLG_CONTENT_CHECKSUM: u8 = 0x04;
    const FLG_DICT_ID: u8 = 0x01;
    const BLOCK_UNCOMPRESSED: u32 = 0x8000_0000;
    const WINDOW_SIZE: usize = 64 * 1024;

    let invalid = |msg: &str| ArrowError::IpcError(format!("Invalid LZ4 frame: {msg}"));

    let Some(&[m0, m1, m2, m3, flg, bd]) = input.first_chunk::<6>() else {
        return Ok(None);
    };
    if u32::from_le_bytes([m0, m1, m2, m3]) != MAGIC
        || flg >> 6 != 0b01
        || flg & (FLG_BLOCK_CHECKSUMS | FLG_CONTENT_CHECKSUM | FLG_DICT_ID) != 0
    {
        return Ok(None);
    }
    let max_block_size = match (bd >> 4) & 0x7 {
        4 => 64 << 10,
        5 => 256 << 10,
        6 => 1 << 20,
        7 => 4 << 20,
        _ => return Ok(None),
    };
    let descriptor_end = if flg & FLG_CONTENT_SIZE != 0 { 14 } else { 6 };
    let header_checksum = *input
        .get(descriptor_end)
        .ok_or_else(|| invalid("truncated header"))?;
    if header_checksum != xxh32(&input[4..descriptor_end]).to_le_bytes()[1] {
        return Err(invalid("header checksum mismatch"));
    }
    let linked = flg & FLG_INDEPENDENT_BLOCKS == 0;

    let mut output = vec![0u8; decompressed_size];
    let mut written = 0;
    let mut pos = descriptor_end + 1;
    loop {
        let block_info = input
            .get(pos..pos + 4)
            .ok_or_else(|| invalid("truncated block"))?;
        let block_info = u32::from_le_bytes(block_info.try_into().unwrap());
        pos += 4;
        if block_info == 0 {
            break;
        }
        let len = (block_info & !BLOCK_UNCOMPRESSED) as usize;
        if len > max_block_size {
            return Err(invalid("block larger than the maximum block size"));
        }
        let block = input
            .get(pos..pos + len)
            .ok_or_else(|| invalid("truncated block"))?;
        pos += len;

        let (decoded, remaining) = output.split_at_mut(written);
        written += if block_info & BLOCK_UNCOMPRESSED != 0 {
            remaining
                .get_mut(..len)
                .ok_or_else(|| exceeds_advertised_size(decompressed_size))?
                .copy_from_slice(block);
            len
        } else if linked && !decoded.is_empty() {
            let dict = &decoded[decoded.len().saturating_sub(WINDOW_SIZE)..];
            lz4_flex::block::decompress_into_with_dict(block, remaining, dict)
                .map_err(|e| lz4_block_error(e, decompressed_size))?
        } else {
            lz4_flex::block::decompress_into(block, remaining)
                .map_err(|e| lz4_block_error(e, decompressed_size))?
        };
    }
    if pos != input.len() {
        return Ok(None);
    }
    if written != decompressed_size {
        return Err(ArrowError::IpcError(format!(
            "Expected compressed length of {decompressed_size} got {written}"
        )));
    }
    Ok(Some(output))
}

#[cfg(feature = "lz4")]
fn lz4_block_error(e: lz4_flex::block::DecompressError, decompressed_size: usize) -> ArrowError {
    match e {
        lz4_flex::block::DecompressError::OutputTooSmall { .. } => {
            exceeds_advertised_size(decompressed_size)
        }
        e => ArrowError::ExternalError(Box::new(e)),
    }
}

#[cfg(feature = "lz4")]
fn exceeds_advertised_size(decompressed_size: usize) -> ArrowError {
    ArrowError::IpcError(format!(
        "LZ4 decompressed buffer exceeds advertised size of {decompressed_size}"
    ))
}

#[cfg(feature = "lz4")]
fn xxh32(data: &[u8]) -> u32 {
    use std::hash::Hasher;
    let mut hasher = twox_hash::XxHash32::with_seed(0);
    hasher.write(data);
    hasher.finish() as u32
}

#[cfg(not(feature = "lz4"))]
fn decompress_lz4(_input: &[u8], _decompressed_size: usize) -> Result<Vec<u8>, ArrowError> {
    Err(ArrowError::InvalidArgumentError(
        "lz4 IPC decompression requires the lz4 feature".to_string(),
    ))
}

#[cfg(feature = "zstd")]
fn compress_zstd(
    input: &[u8],
    output: &mut Vec<u8>,
    context: &mut IpcWriteContext,
    level: i32,
) -> Result<(), ArrowError> {
    let start = output.len();
    output.reserve(zstd::zstd_safe::compress_bound(input.len()));

    let mut cursor = std::io::Cursor::new(output);
    cursor.set_position(start as u64);
    context
        .zstd_compressor(level)
        .compress_to_buffer(input, &mut cursor)?;

    Ok(())
}

#[cfg(not(feature = "zstd"))]
fn compress_zstd(
    _input: &[u8],
    _output: &mut Vec<u8>,
    _context: &mut IpcWriteContext,
    _level: i32,
) -> Result<(), ArrowError> {
    Err(ArrowError::InvalidArgumentError(
        "zstd IPC compression requires the zstd feature".to_string(),
    ))
}

#[cfg(feature = "zstd")]
fn decompress_zstd(
    input: &[u8],
    decompressed_size: usize,
    context: &mut DecompressionContext,
) -> Result<Vec<u8>, ArrowError> {
    let output = context
        .zstd_decompressor()
        .decompress(input, decompressed_size)?;
    Ok(output)
}

#[cfg(not(feature = "zstd"))]
fn decompress_zstd(
    _input: &[u8],
    _decompressed_size: usize,
    _context: &mut DecompressionContext,
) -> Result<Vec<u8>, ArrowError> {
    Err(ArrowError::InvalidArgumentError(
        "zstd IPC decompression requires the zstd feature".to_string(),
    ))
}

/// Get the uncompressed length
/// Notes:
///   LENGTH_NO_COMPRESSED_DATA: indicate that the data that follows is not compressed
///    0: indicate that there is no data
///   positive number: indicate the uncompressed length for the following data
/// Returns an error if the input buffer is shorter than 8 bytes
#[inline]
fn read_uncompressed_size(buffer: &[u8]) -> Result<i64, ArrowError> {
    let len_buffer = buffer.get(..LENGTH_OF_PREFIX_DATA as usize).ok_or_else(|| {
        ArrowError::IpcError(format!(
            "Compressed IPC buffer is too short: expected at least {LENGTH_OF_PREFIX_DATA} bytes, got {}",
            buffer.len()
        ))
    })?;
    Ok(i64::from_le_bytes(len_buffer.try_into().unwrap()))
}

#[cfg(test)]
mod tests {
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
            .decompress(
                output_bytes.as_slice(),
                input_bytes.len(),
                &mut Default::default(),
            )
            .unwrap();
        assert_eq!(input_bytes, result.as_slice());
    }

    #[test]
    #[cfg(feature = "lz4")]
    fn test_lz4_decompression_rejects_output_exceeding_advertised_size() {
        let input_bytes = b"hello lz4";
        let codec = super::CompressionCodec::Lz4Frame;
        let mut compressed = Vec::new();
        codec
            .compress(input_bytes, &mut compressed, &mut Default::default())
            .unwrap();

        let err = codec
            .decompress(&compressed, input_bytes.len() - 1, &mut Default::default())
            .expect_err("output larger than the advertised size should fail");

        assert!(
            err.to_string().contains("exceeds advertised size"),
            "unexpected error: {err}"
        );
    }

    /// Encodes `input` as one LZ4 frame with the given layout.
    #[cfg(feature = "lz4")]
    fn lz4_frame(input: &[u8], info: lz4_flex::frame::FrameInfo) -> Vec<u8> {
        use std::io::Write;
        let mut encoder = lz4_flex::frame::FrameEncoder::with_frame_info(info, Vec::new());
        encoder.write_all(input).unwrap();
        encoder.finish().unwrap()
    }

    /// Compressible data with matches that cross 64 KiB block boundaries, then a stretch
    /// of noise that LZ4 stores in uncompressed blocks.
    #[cfg(feature = "lz4")]
    fn lz4_test_input(len: usize) -> Vec<u8> {
        let mut state = 0x2545_f491_4f6c_dd1du64;
        (0..len)
            .map(|i| {
                if i < len / 2 {
                    b"arrow ipc lz4 frame "[i % 20] ^ (i / 7000) as u8
                } else {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    state as u8
                }
            })
            .collect()
    }

    #[test]
    #[cfg(feature = "lz4")]
    fn test_lz4_frame_layouts_round_trip() {
        use lz4_flex::frame::{BlockMode, BlockSize, FrameInfo};

        let codec = super::CompressionCodec::Lz4Frame;
        for len in [0, 1, 1000, 64 * 1024, 300 * 1024] {
            let input = lz4_test_input(len);
            for block_mode in [BlockMode::Independent, BlockMode::Linked] {
                for (block_checksums, content_checksum) in
                    [(false, false), (true, false), (false, true)]
                {
                    let info = FrameInfo::new()
                        .block_size(BlockSize::Max64KB)
                        .block_mode(block_mode)
                        .block_checksums(block_checksums)
                        .content_checksum(content_checksum);
                    let frame = lz4_frame(&input, info);
                    let in_place = super::decompress_lz4_frame(&frame, input.len()).unwrap();
                    assert_eq!(in_place.is_some(), !block_checksums && !content_checksum);
                    let output = codec
                        .decompress(&frame, input.len(), &mut Default::default())
                        .unwrap();
                    assert_eq!(
                        output, input,
                        "len {len}, {block_mode:?}, checksums {block_checksums}/{content_checksum}"
                    );
                }
            }
        }
    }

    #[test]
    #[cfg(feature = "lz4")]
    fn test_lz4_frame_rejects_truncated_input() {
        let codec = super::CompressionCodec::Lz4Frame;
        let input = lz4_test_input(200 * 1024);
        let frame = lz4_frame(&input, lz4_flex::frame::FrameInfo::new());
        for cut in [5, 7, frame.len() / 2, frame.len() - 1] {
            codec
                .decompress(&frame[..cut], input.len(), &mut Default::default())
                .expect_err("truncated frame should fail");
        }
    }

    #[test]
    #[cfg(feature = "zstd")]
    fn test_zstd_compression() {
        let input_bytes = b"hello zstd";
        let codec = super::CompressionCodec::Zstd(super::DEFAULT_ZSTD_COMPRESSION_LEVEL);
        let mut output_bytes: Vec<u8> = Vec::new();
        codec
            .compress(input_bytes, &mut output_bytes, &mut Default::default())
            .unwrap();
        let result = codec
            .decompress(
                output_bytes.as_slice(),
                input_bytes.len(),
                &mut Default::default(),
            )
            .unwrap();
        assert_eq!(input_bytes, result.as_slice());
    }

    #[test]
    fn test_read_uncompressed_size_rejects_short_prefix() {
        let err = super::read_uncompressed_size(&[1, 2, 3, 4, 5, 6, 7])
            .expect_err("short compressed IPC prefix should return an error");

        assert!(
            err.to_string()
                .contains("Compressed IPC buffer is too short"),
            "unexpected error: {err}"
        );
    }

    #[test]
    #[cfg(feature = "lz4")]
    fn test_compress_to_vec_writes_8_byte_length_prefix() {
        // The length prefix must always be 8 bytes (i64),
        // even on platforms where `usize` is narrower (e.g. wasm32).
        let input_bytes = vec![42u8; 132];
        let codec = super::CompressionCodec::Lz4Frame;
        let mut output_bytes: Vec<u8> = Vec::new();
        codec
            .compress_to_vec(&input_bytes, &mut output_bytes, &mut Default::default())
            .unwrap();

        let prefix: [u8; 8] = output_bytes[..8].try_into().unwrap();
        assert_eq!(i64::from_le_bytes(prefix), input_bytes.len() as i64);
    }
}
