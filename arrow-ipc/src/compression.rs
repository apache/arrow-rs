// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use crate::CompressionType;
use arrow_buffer::Buffer;
use arrow_schema::ArrowError;
use flatbuffers::FlatBufferBuilder;

const LENGTH_NO_COMPRESSED_DATA: i64 = -1;
const LENGTH_OF_PREFIX_DATA: i64 = 8;
const DEFAULT_ZSTD_COMPRESSION_LEVEL: i32 = 3;

/// The lz4 frame block size used unless the caller configures another size with
/// [`crate::writer::IpcWriteOptions::try_with_lz4_block_size`].
pub(crate) const DEFAULT_LZ4_BLOCK_SIZE: usize = LZ4_BLOCK_256_KIB;

const LZ4_BLOCK_64_KIB: usize = 64 * 1024;
const LZ4_BLOCK_256_KIB: usize = 256 * 1024;
const LZ4_BLOCK_1_MIB: usize = 1024 * 1024;
const LZ4_BLOCK_4_MIB: usize = 4 * 1024 * 1024;

/// The lz4 frame block sizes callers may configure, in bytes.
///
/// These are the only sizes the LZ4 frame format can represent: the frame header stores the block
/// size as a 3 bit code with the values 4 to 7. `lz4_flex` also offers `BlockSize::Max8MB`, but it
/// encodes it by setting the header's reserved bit, which the decoder then rejects, so it cannot
/// be exposed here.
pub(crate) const SUPPORTED_LZ4_BLOCK_SIZES: [usize; 4] = [
    LZ4_BLOCK_64_KIB,
    LZ4_BLOCK_256_KIB,
    LZ4_BLOCK_1_MIB,
    LZ4_BLOCK_4_MIB,
];

/// Additional context that may be needed for compression.
///
/// In the case of zstd, this will contain the zstd context, which can be reused between subsequent
/// compression calls to avoid the performance overhead of initialising a new context for every
/// compression. In the case of lz4, this will contain the lz4 frame encoder, whose hash table and
/// block buffers are likewise reused; its block size is configurable through
/// [`crate::writer::IpcWriteOptions::try_with_lz4_block_size`]. Also holds a [`FlatBufferBuilder`]
/// that is reused across IPC writes.
#[derive(Default)]
pub struct IpcWriteContext {
    scratch: Vec<u8>,
    reserve_scratch: bool,
    fbb: FlatBufferBuilder<'static>,
    /// The lz4 frame block size to build a cached encoder with; `None` selects
    /// [`DEFAULT_LZ4_BLOCK_SIZE`].
    lz4_block_size: Option<usize>,
    #[cfg(feature = "zstd")]
    compressor: Option<zstd::bulk::Compressor<'static>>,
    #[cfg(feature = "lz4")]
    lz4_compressor: Option<Lz4Compressor>,
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

    /// Set the lz4 frame block size used by the cached lz4 encoder.
    ///
    /// A cached encoder is dropped when the size changes so the next compression call builds one
    /// with the new block size. `block_size` must be one of [`SUPPORTED_LZ4_BLOCK_SIZES`];
    /// [`crate::writer::IpcWriteOptions::try_with_lz4_block_size`] validates it before it reaches
    /// this point.
    pub(crate) fn set_lz4_block_size(&mut self, block_size: usize) {
        debug_assert!(
            SUPPORTED_LZ4_BLOCK_SIZES.contains(&block_size),
            "unsupported lz4 block size {block_size}"
        );
        if self.lz4_block_size != Some(block_size) {
            self.lz4_block_size = Some(block_size);
            #[cfg(feature = "lz4")]
            {
                // The cached encoder was configured for the previous block size.
                self.lz4_compressor = None;
            }
        }
    }

    #[cfg(feature = "zstd")]
    fn zstd_compressor(&mut self, level: i32) -> &mut zstd::bulk::Compressor<'static> {
        self.compressor.get_or_insert_with(|| {
            zstd::bulk::Compressor::new(level).expect("can use default compression level")
        })
    }

    #[cfg(feature = "lz4")]
    fn lz4_compressor(&mut self) -> &mut Lz4Compressor {
        let block_size = self.lz4_block_size.unwrap_or(DEFAULT_LZ4_BLOCK_SIZE);
        self.lz4_compressor
            .get_or_insert_with(|| Lz4Compressor::new(block_size))
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

        #[cfg(feature = "lz4")]
        ds.field(
            "lz4_compressor",
            &self.lz4_compressor.as_ref().map(|_| "Lz4Compressor"),
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
            CompressionCodec::Lz4Frame => compress_lz4(input, output, context),
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

/// Reusable lz4 frame compressor, the lz4 counterpart of `zstd::bulk::Compressor`.
///
/// `lz4_flex` has no bulk frame API, so this wraps a [`lz4_flex::frame::FrameEncoder`] whose hash
/// table and block buffers are retained across calls to [`Self::compress_to_buffer`].
#[cfg(feature = "lz4")]
struct Lz4Compressor {
    encoder: lz4_flex::frame::FrameEncoder<Vec<u8>>,
    block_size: usize,
}

#[cfg(feature = "lz4")]
impl Lz4Compressor {
    fn new(block_size: usize) -> Self {
        // Pin the block size: with `BlockSize::Auto` the first buffer written would fix it for
        // every subsequent frame of the reused encoder. 256KB keeps large buffers as fast as
        // with bigger blocks while the encoder retains only ~0.5MB of buffers; callers can pick
        // another supported size via `IpcWriteOptions::try_with_lz4_block_size`.
        let frame_info =
            lz4_flex::frame::FrameInfo::new().block_size(to_lz4_block_size(block_size));
        Self {
            encoder: lz4_flex::frame::FrameEncoder::with_frame_info(frame_info, Vec::new()),
            block_size,
        }
    }

    /// Compresses `input` as a single lz4 frame appended to `output`.
    fn compress_to_buffer(&mut self, input: &[u8], output: &mut Vec<u8>) -> Result<(), ArrowError> {
        use std::io::Write;
        // The encoder skips the frame header on empty input once it has written a frame.
        debug_assert!(!input.is_empty());
        // Lend `output` to the encoder so that the frame is written straight into it.
        std::mem::swap(self.encoder.get_mut(), output);
        let result = self
            .encoder
            .write_all(input)
            .map_err(ArrowError::from)
            .and_then(|()| {
                self.encoder
                    .try_finish()
                    .map_err(|e| ArrowError::ExternalError(Box::new(e)))
            });
        std::mem::swap(self.encoder.get_mut(), output);
        if result.is_err() {
            // The encoder may be left mid-frame, start from a fresh one next time.
            let block_size = self.block_size;
            *self = Self::new(block_size);
        }
        result
    }
}

/// Maps a validated block size to the `lz4_flex` frame header code.
#[cfg(feature = "lz4")]
fn to_lz4_block_size(block_size: usize) -> lz4_flex::frame::BlockSize {
    match block_size {
        LZ4_BLOCK_64_KIB => lz4_flex::frame::BlockSize::Max64KB,
        LZ4_BLOCK_256_KIB => lz4_flex::frame::BlockSize::Max256KB,
        LZ4_BLOCK_1_MIB => lz4_flex::frame::BlockSize::Max1MB,
        LZ4_BLOCK_4_MIB => lz4_flex::frame::BlockSize::Max4MB,
        _ => {
            debug_assert!(
                SUPPORTED_LZ4_BLOCK_SIZES.contains(&block_size),
                "unsupported lz4 block size {block_size}"
            );
            // Unreachable for sizes validated by `try_with_lz4_block_size`; keep the default
            // rather than panicking in release builds.
            lz4_flex::frame::BlockSize::Max256KB
        }
    }
}

#[cfg(feature = "lz4")]
fn compress_lz4(
    input: &[u8],
    output: &mut Vec<u8>,
    context: &mut IpcWriteContext,
) -> Result<(), ArrowError> {
    context.lz4_compressor().compress_to_buffer(input, output)
}

#[cfg(not(feature = "lz4"))]
fn compress_lz4(
    _input: &[u8],
    _output: &mut Vec<u8>,
    _context: &mut IpcWriteContext,
) -> Result<(), ArrowError> {
    Err(ArrowError::InvalidArgumentError(
        "lz4 IPC compression requires the lz4 feature".to_string(),
    ))
}

#[cfg(feature = "lz4")]
fn decompress_lz4(input: &[u8], decompressed_size: usize) -> Result<Vec<u8>, ArrowError> {
    use std::io::Read;
    let mut output = Vec::with_capacity(decompressed_size.min(crate::reader::MAX_PREALLOC_BYTES));
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
    if decompressed_size > crate::reader::MAX_PREALLOC_BYTES {
        // The bulk decompressor writes into a buffer reserved up front, so stream instead.
        use std::io::Read;
        let mut output =
            Vec::with_capacity(decompressed_size.min(crate::reader::MAX_PREALLOC_BYTES));
        let mut decoder = zstd::stream::read::Decoder::with_buffer(input)?;
        decoder
            .by_ref()
            .take(decompressed_size as u64)
            .read_to_end(&mut output)?;

        // Probe without growing `output` to reject data exceeding the advertised size.
        if decoder.read(&mut [0])? != 0 {
            return Err(ArrowError::IpcError(format!(
                "ZSTD decompressed buffer exceeds advertised size of {decompressed_size}"
            )));
        }
        return Ok(output);
    }
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
    use arrow_buffer::Buffer;

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

    #[test]
    #[cfg(feature = "lz4")]
    fn test_lz4_compression_multiple_frames() {
        let codec = super::CompressionCodec::Lz4Frame;
        let mut context = super::IpcWriteContext::default();
        // Spans several 256KB blocks, followed by a smaller frame.
        let large: Vec<u8> = (0..200_000u32)
            .flat_map(|i| (i % 1000).to_le_bytes())
            .collect();
        let small = b"hello lz4".to_vec();
        for input in [&large, &small, &large] {
            let mut output = b"prefix".to_vec();
            codec.compress(input, &mut output, &mut context).unwrap();
            assert_eq!(&output[..6], b"prefix");
            let result = codec
                .decompress(&output[6..], input.len(), &mut Default::default())
                .unwrap();
            assert_eq!(input, &result);
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

    /// A buffer larger than `MAX_PREALLOC_BYTES` round-trips, and a size its data does not back fails.
    #[test]
    #[cfg(any(feature = "lz4", feature = "zstd"))]
    fn test_decompression_beyond_prealloc() {
        let input = vec![7u8; crate::reader::MAX_PREALLOC_BYTES + 2];
        for codec in [
            #[cfg(feature = "lz4")]
            super::CompressionCodec::Lz4Frame,
            #[cfg(feature = "zstd")]
            super::CompressionCodec::Zstd(super::DEFAULT_ZSTD_COMPRESSION_LEVEL),
        ] {
            let mut compressed = Vec::new();
            codec
                .compress(&input, &mut compressed, &mut Default::default())
                .unwrap();
            let decompress = |len| codec.decompress(&compressed, len, &mut Default::default());
            assert_eq!(decompress(input.len()).unwrap(), input, "{codec:?}");
            assert!(decompress(input.len() - 1).is_err(), "{codec:?}");
            assert!(decompress(usize::MAX).is_err(), "{codec:?}");
        }
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

    /// Reads the block size descriptor code from the lz4 frame header of a `compress_to_vec`
    /// buffer: `[8 byte length prefix][4 byte magic][FLG][BD]`, where BD bits 4-6 hold the code.
    #[cfg(feature = "lz4")]
    fn lz4_frame_block_size_code(compressed: &[u8]) -> u8 {
        const BD_OFFSET: usize = super::LENGTH_OF_PREFIX_DATA as usize + 4 + 1;
        let prefix: [u8; 8] = compressed[..8].try_into().unwrap();
        assert_ne!(
            i64::from_le_bytes(prefix),
            super::LENGTH_NO_COMPRESSED_DATA,
            "input was stored uncompressed, frame header unavailable"
        );
        (compressed[BD_OFFSET] & 0b0111_0000) >> 4
    }

    #[test]
    #[cfg(feature = "lz4")]
    fn test_lz4_block_size_default_is_256kb() {
        let input = vec![7u8; 4096];
        let codec = super::CompressionCodec::Lz4Frame;

        let mut untouched = Vec::new();
        codec
            .compress_to_vec(&input, &mut untouched, &mut Default::default())
            .unwrap();

        let mut explicit = super::IpcWriteContext::default();
        explicit.set_lz4_block_size(super::DEFAULT_LZ4_BLOCK_SIZE);
        let mut with_default = Vec::new();
        codec
            .compress_to_vec(&input, &mut with_default, &mut explicit)
            .unwrap();

        // The frame header code for 256KiB blocks is 5 (see the LZ4 frame format).
        assert_eq!(lz4_frame_block_size_code(&untouched), 5);
        assert_eq!(
            untouched, with_default,
            "explicitly configured default must be byte identical to the untouched default"
        );
    }

    #[test]
    #[cfg(feature = "lz4")]
    fn test_lz4_block_size_is_configurable() {
        // Larger than the smallest block size so multi-block frames are exercised too.
        let input: Vec<u8> = (0..300_000u32).map(|i| (i % 251) as u8).collect();
        let codec = super::CompressionCodec::Lz4Frame;

        for (block_size, expected_code) in [
            (super::SUPPORTED_LZ4_BLOCK_SIZES[0], 4),
            (super::SUPPORTED_LZ4_BLOCK_SIZES[1], 5),
            (super::SUPPORTED_LZ4_BLOCK_SIZES[2], 6),
            (super::SUPPORTED_LZ4_BLOCK_SIZES[3], 7),
        ] {
            let mut context = super::IpcWriteContext::default();
            context.set_lz4_block_size(block_size);
            let mut compressed = Vec::new();
            codec
                .compress_to_vec(&input, &mut compressed, &mut context)
                .unwrap();

            assert_eq!(
                lz4_frame_block_size_code(&compressed),
                expected_code,
                "wrong frame header code for block size {block_size}"
            );
            let decoded = codec
                .decompress_to_buffer(&Buffer::from(compressed), &mut Default::default())
                .unwrap();
            assert_eq!(decoded.as_slice(), input, "roundtrip with {block_size}");
        }
    }

    #[test]
    #[cfg(feature = "lz4")]
    fn test_lz4_block_size_change_reuses_context() {
        // Changing the size mid-context must reset the cached encoder without corrupting
        // the frames it produces afterwards.
        let input = vec![42u8; 100_000];
        let codec = super::CompressionCodec::Lz4Frame;
        let mut context = super::IpcWriteContext::default();

        for block_size in [
            super::LZ4_BLOCK_64_KIB,
            super::LZ4_BLOCK_4_MIB,
            super::DEFAULT_LZ4_BLOCK_SIZE,
        ] {
            context.set_lz4_block_size(block_size);
            let mut compressed = Vec::new();
            codec
                .compress_to_vec(&input, &mut compressed, &mut context)
                .unwrap();
            let decoded = codec
                .decompress_to_buffer(&Buffer::from(compressed), &mut Default::default())
                .unwrap();
            assert_eq!(
                decoded.as_slice(),
                input,
                "roundtrip after switching to {block_size}"
            );
        }
    }
}
