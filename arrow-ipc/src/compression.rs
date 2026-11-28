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

#[cfg(feature = "lz4")]
const LZ4_UNCOMPRESSED_BLOCK: u32 = 0x8000_0000;

/// LZ4 frame block sizes representable by the frame header. See the [LZ4 frame format] for the
/// block size codes; `lz4_flex`'s 8 MiB option sets a reserved bit, which the decoder rejects.
///
/// [LZ4 frame format]: https://github.com/lz4/lz4/blob/dev/doc/lz4_Frame_format.md
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Lz4BlockSize {
    Max64KiB,
    Max256KiB,
    Max1MiB,
    Max4MiB,
}

impl Lz4BlockSize {
    pub(crate) const DEFAULT: Self = Self::Max256KiB;

    pub(crate) const fn from_bytes(bytes: usize) -> Option<Self> {
        match bytes {
            65_536 => Some(Self::Max64KiB),
            262_144 => Some(Self::Max256KiB),
            1_048_576 => Some(Self::Max1MiB),
            4_194_304 => Some(Self::Max4MiB),
            _ => None,
        }
    }
}

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
    /// The lz4 frame block size to build a cached encoder with; `None` selects the default.
    lz4_block_size: Option<Lz4BlockSize>,
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
    /// with the new block size.
    pub(crate) fn set_lz4_block_size(&mut self, block_size: Lz4BlockSize) {
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
        let block_size = self.lz4_block_size.unwrap_or(Lz4BlockSize::DEFAULT);
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
    block_size: Lz4BlockSize,
}

#[cfg(feature = "lz4")]
impl Lz4Compressor {
    fn new(block_size: Lz4BlockSize) -> Self {
        // `BlockSize::Auto` fixes a reused encoder's size from its first buffer. Pin the selected
        // size so later frames keep using the block size requested in the write options.
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
fn to_lz4_block_size(block_size: Lz4BlockSize) -> lz4_flex::frame::BlockSize {
    match block_size {
        Lz4BlockSize::Max64KiB => lz4_flex::frame::BlockSize::Max64KB,
        Lz4BlockSize::Max256KiB => lz4_flex::frame::BlockSize::Max256KB,
        Lz4BlockSize::Max1MiB => lz4_flex::frame::BlockSize::Max1MB,
        Lz4BlockSize::Max4MiB => lz4_flex::frame::BlockSize::Max4MB,
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
    match decompress_lz4_blocks(input, decompressed_size) {
        Some(result) => result,
        // A frame this does not take apart itself: linked blocks, a block or content checksum,
        // or a dictionary. Independent blocks with no checksums are what every Arrow writer
        // produces, so the rest take the streaming decoder rather than more parsing here.
        None => decompress_lz4_streaming(input, decompressed_size),
    }
}

/// Decodes a frame of independent blocks straight into its output buffer.
///
/// `lz4_flex`'s `FrameDecoder` is a streaming decoder: it holds source and destination buffers
/// sized from the frame's *declared* maximum block size, and zero-fills the destination up to
/// that maximum before each block, whatever the block actually holds. The format lets a writer
/// declare 4 MiB blocks for a buffer of any size, so a decoder can cost megabytes of `memset`
/// before decompressing a few KiB -- and an IPC message decodes one buffer after another, each
/// with a decoder of its own.
///
/// None of that is needed here: the blocks decompress one after another straight into the output,
/// with no scratch buffer and nothing zeroed beyond the output itself. Returns `None` for a frame
/// outside the shape it handles, which the caller sends to the streaming decoder.
///
/// Like the streaming path, this never sizes the output from `decompressed_size` alone: that
/// length comes from the IPC prefix, which is untrusted, so the output starts at no more than
/// `MAX_PREALLOC_BYTES` and grows only as the blocks fill it.
#[cfg(feature = "lz4")]
fn decompress_lz4_blocks(
    input: &[u8],
    decompressed_size: usize,
) -> Option<Result<Vec<u8>, ArrowError>> {
    const MAGIC: u32 = 0x184D2204;
    const VERSION: u8 = 0b01;
    const INDEPENDENT_BLOCKS: u8 = 0b0010_0000;
    const BLOCK_CHECKSUM: u8 = 0b0001_0000;
    const CONTENT_SIZE: u8 = 0b0000_1000;
    const CONTENT_CHECKSUM: u8 = 0b0000_0100;
    const DICTIONARY_ID: u8 = 0b0000_0001;

    if input.len() < 7 || u32::from_le_bytes(input[0..4].try_into().ok()?) != MAGIC {
        return None;
    }
    let flg = input[4];
    if flg >> 6 != VERSION
        || flg & INDEPENDENT_BLOCKS == 0
        || flg & (BLOCK_CHECKSUM | CONTENT_CHECKSUM | DICTIONARY_ID) != 0
    {
        return None;
    }
    // Magic, FLG, BD, the content size when the frame carries one, then the header checksum. The
    // checksum is left to the blocks and to the caller's length check: a header corrupt enough to
    // matter cannot also describe blocks that decode to exactly the expected size.
    let at = 6 + if flg & CONTENT_SIZE != 0 { 8 } else { 0 } + 1;
    if input.len() < at {
        return None;
    }
    // The maximum size a single block of this frame may decompress to. A block size the format
    // does not define goes to the streaming decoder along with the other shapes declined above.
    let block_max = match (input[5] >> 4) & 0b111 {
        4 => 64 * 1024,
        5 => 256 * 1024,
        6 => 1024 * 1024,
        7 => 4 * 1024 * 1024,
        _ => return None,
    };

    Some(decode_independent_blocks(
        input,
        at,
        decompressed_size,
        block_max,
    ))
}

#[cfg(feature = "lz4")]
fn decode_independent_blocks(
    input: &[u8],
    mut at: usize,
    decompressed_size: usize,
    block_max: usize,
) -> Result<Vec<u8>, ArrowError> {
    let corrupt = |detail: &str| ArrowError::IpcError(format!("Corrupt lz4 frame: {detail}"));
    let exceeds = || {
        ArrowError::IpcError(format!(
            "LZ4 decompressed buffer exceeds advertised size of {decompressed_size}"
        ))
    };
    let mut output = vec![0u8; decompressed_size.min(crate::reader::MAX_PREALLOC_BYTES)];
    let mut written = 0usize;

    loop {
        let header = input
            .get(at..at + 4)
            .ok_or_else(|| corrupt("it ends before its end mark"))?;
        let word = u32::from_le_bytes(header.try_into().unwrap());
        at += 4;
        if word == 0 {
            break;
        }
        let len = (word & !LZ4_UNCOMPRESSED_BLOCK) as usize;
        let block = input
            .get(at..at + len)
            .ok_or_else(|| corrupt("a block runs past its end"))?;
        at += len;

        // A block decompresses to at most the frame's declared maximum, and the frame as a whole
        // to at most the advertised size, so this is every byte the next block can legitimately
        // need. Growing to it leaves the output at the size the blocks actually reach, and keeps
        // a bogus advertised length from sizing the allocation.
        let remaining = decompressed_size - written;
        let room = block_max.min(remaining);
        if output.len() < written + room {
            output.resize(written + room, 0);
        }
        let dst = &mut output[written..written + room];

        if word & LZ4_UNCOMPRESSED_BLOCK != 0 {
            let dst = match dst.get_mut(..len) {
                Some(dst) => dst,
                // The block does not fit. Which promise it breaks depends on which limit `room`
                // was capped by: the advertised size, or the frame's own declared block maximum.
                None if len > remaining => return Err(exceeds()),
                None => return Err(corrupt("a block exceeds the declared block maximum")),
            };
            dst.copy_from_slice(block);
            written += len;
        } else {
            written += lz4_flex::block::decompress_into(block, dst).map_err(|e| match e {
                // The output ran out, and `room` was capped by the advertised size rather than by
                // the block maximum, so the frame holds more than the IPC prefix said it would.
                lz4_flex::block::DecompressError::OutputTooSmall { .. } if room == remaining => {
                    exceeds()
                }
                e => ArrowError::ExternalError(Box::new(e)),
            })?;
        }
    }

    output.truncate(written);
    Ok(output)
}

/// The streaming decoder, for the frames [`decompress_lz4_blocks`] declines.
///
/// A decoder of its own each time, as before: these frames are rare in IPC, and `FrameDecoder`
/// sizes its buffers for the frame it is handed and asserts as much, so one cannot be carried
/// from a linked frame to an independent one.
#[cfg(feature = "lz4")]
fn decompress_lz4_streaming(input: &[u8], decompressed_size: usize) -> Result<Vec<u8>, ArrowError> {
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
    #[test]
    #[cfg(feature = "lz4")]
    fn test_lz4_decompression_handles_every_frame_shape() {
        // The shapes the direct block decode takes, and the ones it leaves to the streaming
        // decoder: linked blocks, and a content checksum. All must decode to what went in.
        use lz4_flex::frame::{BlockMode, FrameEncoder, FrameInfo};
        use std::io::Write;

        let codec = super::CompressionCodec::Lz4Frame;
        let mut context = super::DecompressionContext::new();

        // Incompressible, so the encoder stores blocks as they are, and long enough to cross
        // more than one block at the smallest block size.
        let noisy: Vec<u8> = (0..300_000u32)
            .map(|i| (i.wrapping_mul(2654435761) >> 13) as u8)
            .collect();
        let runs: Vec<u8> = (0..300_000).map(|i| ((i / 1000) % 7) as u8).collect();

        for payload in [&noisy, &runs] {
            for (linked, checksum) in [(false, false), (true, false), (false, true), (true, true)] {
                let mut info = FrameInfo::new();
                info.block_mode = if linked {
                    BlockMode::Linked
                } else {
                    BlockMode::Independent
                };
                info.content_checksum = checksum;
                let mut encoder = FrameEncoder::with_frame_info(info, Vec::new());
                encoder.write_all(payload).unwrap();
                let frame = encoder.finish().unwrap();

                let result = codec
                    .decompress(frame.as_slice(), payload.len(), &mut context)
                    .unwrap();
                assert_eq!(
                    payload.as_slice(),
                    result.as_slice(),
                    "linked={linked} checksum={checksum}"
                );
            }
        }
    }

    #[test]
    #[cfg(feature = "lz4")]
    fn test_lz4_decompression_rejects_a_truncated_frame() {
        let codec = super::CompressionCodec::Lz4Frame;
        let mut context = super::DecompressionContext::new();
        let input = vec![3u8; 50_000];
        let mut compressed = Vec::new();
        codec
            .compress(&input, &mut compressed, &mut Default::default())
            .unwrap();

        for cut in [8, compressed.len() / 2, compressed.len() - 1] {
            assert!(
                codec
                    .decompress(&compressed[..cut], input.len(), &mut context)
                    .is_err(),
                "a frame cut at {cut} decoded"
            );
        }
        // And a whole frame still decodes afterwards.
        let result = codec
            .decompress(compressed.as_slice(), input.len(), &mut context)
            .unwrap();
        assert_eq!(input.as_slice(), result.as_slice());
    }

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
}
