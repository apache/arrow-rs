// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Push-based decoding of serialized Parquet Bloom filters.

use std::ops::Range;

use bytes::Bytes;

use super::{SBBF_HEADER_SIZE_ESTIMATE, Sbbf, bloom_filter_bitset_range, bloom_filter_read_range};
use crate::DecodeResult;
use crate::errors::{ParquetError, Result};
use crate::file::metadata::ColumnChunkMetaData;
use crate::file::reader::ChunkReader;
use crate::util::push_buffers::PushBuffers;

/// Decoding state machine
#[derive(Debug)]
enum DecodeState {
    /// Finding the first Bloom filter range
    FindingRange,
    // Reading the SBBF header
    ReadingSbbfHeader(Range<u64>),
    // Reading the SBBF bitset bytes
    ReadingSbbfBitset(Range<u64>),
    // Decoding is complete
    Finished,
    /// State left during the `try_decode` method so something valid is present.
    /// This state should never be observed.
    Intermediate,
}

/// Decodes a Bloom filter from caller-provided file ranges, without performing I/O.
///
/// The metadata and pushed bytes must refer to the same file. Ranges use absolute
/// file offsets. Call [`Self::try_decode`] to discover which ranges are needed,
/// then supply them using [`Self::push_range`] or [`Self::push_ranges`]. Bytes may
/// also be pushed in advance to avoid requests for already-buffered data.
///
/// Each requested range must be contained in a single pushed buffer; adjacent
/// buffers are not coalesced. After an error from [`Self::try_decode`], discard
/// the decoder.
///
/// # Example
///
/// ```rust
/// use bytes::Bytes;
/// use parquet::DecodeResult;
/// use parquet::bloom_filter::{Sbbf, SbbfPushDecoder};
/// use parquet::errors::Result;
/// use parquet::file::metadata::ColumnChunkMetaData;
///
/// fn decode_filter(metadata: &ColumnChunkMetaData, file_bytes: Bytes) -> Result<Option<Sbbf>> {
///     let Some(mut decoder) = SbbfPushDecoder::try_new(metadata)? else {
///         return Ok(None);
///     };
///     loop {
///         match decoder.try_decode()? {
///             DecodeResult::NeedsData(ranges) => {
///                 let buffers = ranges.iter().map(|range| {
///                     file_bytes.slice(range.start as usize..range.end as usize)
///                 }).collect();
///                 decoder.push_ranges(ranges, buffers)?;
///             }
///             DecodeResult::Data(filter) => return Ok(Some(filter)),
///             DecodeResult::Finished => unreachable!("filter was returned above"),
///         }
///     }
/// }
///
/// # fn main() -> Result<()> {
/// # use std::sync::Arc;
/// # use parquet::schema::parser::parse_message_type;
/// # use parquet::schema::types::SchemaDescriptor;
/// # let schema = Arc::new(parse_message_type("message schema { REQUIRED BYTE_ARRAY value; }")?);
/// # let descriptor = SchemaDescriptor::new(schema).column(0);
/// # let mut original = Sbbf::new_with_num_of_bytes(32);
/// # original.insert("present");
/// # let offset = 13;
/// # let mut serialized = vec![0; offset];
/// # original.write(&mut serialized)?;
/// # let file_bytes = Bytes::from(serialized);
/// # let metadata = ColumnChunkMetaData::builder(descriptor)
/// #     .set_bloom_filter_offset(Some(offset as i64))
/// #     .set_bloom_filter_length(Some((file_bytes.len() - offset) as i32))
/// #     .build()?;
/// let filter = decode_filter(&metadata, file_bytes)?.unwrap();
/// assert!(filter.check("present"));
/// # Ok(())
/// # }
/// ```
pub struct SbbfPushDecoder {
    /// Decoding state
    state: DecodeState,
    /// Offset of the Bloom filter in the file
    offset: u64,
    /// Length of the Bloom filter
    length: Option<usize>,
    /// Underlying buffers
    buffers: PushBuffers,
}

impl SbbfPushDecoder {
    /// Create a new Bloom filter decoder for the given column chunk.
    ///
    /// Returns `None` if the column chunk has no Bloom filter offset.
    /// Invalid offsets and nonpositive advertised lengths return an error.
    pub fn try_new(column_metadata: &ColumnChunkMetaData) -> Result<Option<Self>> {
        let Some(offset) = column_metadata.bloom_filter_offset() else {
            return Ok(None);
        };
        let offset =
            u64::try_from(offset).map_err(|_| general_err!("Bloom filter offset is invalid"))?;
        let length = column_metadata
            .bloom_filter_length()
            .map(|length| {
                usize::try_from(length)
                    .ok()
                    .filter(|length| *length > 0)
                    .ok_or_else(|| general_err!("Bloom filter length is invalid"))
            })
            .transpose()?;
        Ok(Some(Self {
            state: DecodeState::FindingRange,
            offset,
            length,
            buffers: PushBuffers::default(),
        }))
    }

    /// Pushes a file range into the decoder's buffer.
    ///
    /// Returns an error if the buffer length differs from the range length or
    /// decoding has finished.
    pub fn push_range(&mut self, range: Range<u64>, buffer: Bytes) -> Result<()> {
        if matches!(&self.state, DecodeState::Finished) {
            return Err(general_err!(
                "SbbfPushDecoder: cannot push data after decoding is finished"
            ));
        }
        self.buffers.push_range(range, buffer)?;
        Ok(())
    }

    /// Pushes multiple file ranges into the decoder's buffer.
    ///
    /// The number of ranges must match the number of buffers, and each buffer
    /// must have the same length as its range. Pushing after decoding has
    /// finished returns an error.
    pub fn push_ranges(&mut self, ranges: Vec<Range<u64>>, buffers: Vec<Bytes>) -> Result<()> {
        if matches!(&self.state, DecodeState::Finished) {
            return Err(general_err!(
                "SbbfPushDecoder: cannot push data after decoding is finished"
            ));
        }
        self.buffers.push_ranges(ranges, buffers)?;
        Ok(())
    }

    /// Returns the bytes for the given range from the internal buffer
    fn get_bytes(&self, range: &Range<u64>) -> Result<Bytes> {
        let start = range.start;
        let raw_len = range.end - range.start;
        let len: usize = raw_len.try_into().map_err(|_| {
            ParquetError::General(format!(
                "SbbfPushDecoder: Range length too large to fit in usize: {raw_len}",
            ))
        })?;
        self.buffers.get_bytes(start, len)
    }

    /// Attempts to decode the filter, returning the file ranges needed if data
    /// is missing. Repeated calls before supplying data return the same ranges.
    ///
    /// Without prefetched data, requests one range when `bloom_filter_length` is
    /// known, or two otherwise (a 20-byte header estimate, then the bitset).
    /// Headers larger than the estimate require an advertised length.
    /// After returning [`DecodeResult::Data`], subsequent calls return
    /// [`DecodeResult::Finished`].
    ///
    /// The state transitions can be visualized as:
    /// ```text
    ///                 ┌──────────────┐
    ///                 │ FindingRange │
    ///                 └──────────────┘
    ///                        │
    ///               choose initial range
    ///                        │
    ///                        ▼
    ///              ┌───────────────────┐ ───┐ bytes missing:
    ///              │ ReadingSbbfHeader │    │ return NeedsData(range),
    ///              └───────────────────┘ ◀──┘ wait for push_range(s)
    ///                 │               │
    ///            length known   length unknown
    ///                 │               │
    ///                 │               ▼
    ///                 │     ┌───────────────────┐ ───┐ bytes missing:
    ///                 │     │ ReadingSbbfBitset │    │ return NeedsData(range),
    ///                 │     └───────────────────┘ ◀──┘ wait for push_range(s)
    ///                 │               │
    ///                 └───────┬───────┘
    ///                         │
    ///                  return Data(Sbbf)
    ///                         │
    ///                         ▼
    ///                   ┌──────────┐ ───┐
    ///                   │ Finished │    │ return Finished
    ///                   └──────────┘ ◀──┘
    /// ```
    ///
    /// In the above:
    ///  - The initial range is the whole filter when its length is known, otherwise a 20-byte header prefix.
    ///  - Transitions proceed within the same `try_decode()` call while the required bytes are buffered;
    ///    push_range(s) supplies bytes without changing state.
    ///  - Header parsing/validation must succeed before advancing. Any decoding error leaves the decoder unusable.
    pub fn try_decode(&mut self) -> Result<DecodeResult<Sbbf>> {
        loop {
            match std::mem::replace(&mut self.state, DecodeState::Intermediate) {
                DecodeState::FindingRange => {
                    let range = bloom_filter_read_range(
                        self.offset,
                        0..self.length.unwrap_or(SBBF_HEADER_SIZE_ESTIMATE),
                    )?;
                    self.state = DecodeState::ReadingSbbfHeader(range);
                }
                DecodeState::ReadingSbbfHeader(range) => {
                    if !self.buffers.has_range(&range) {
                        self.state = DecodeState::ReadingSbbfHeader(range.clone());
                        return Ok(DecodeResult::NeedsData(vec![range]));
                    }
                    let buffer = self.get_bytes(&range)?;
                    let bitset_range = bloom_filter_bitset_range(&buffer, self.length)?;
                    if self.length.is_some() {
                        let bitset = buffer.slice(bitset_range);
                        self.state = DecodeState::Finished;
                        return Ok(DecodeResult::Data(Sbbf::new(&bitset)));
                    }
                    let range = bloom_filter_read_range(self.offset, bitset_range)?;
                    self.state = DecodeState::ReadingSbbfBitset(range);
                }
                DecodeState::ReadingSbbfBitset(range) => {
                    if !self.buffers.has_range(&range) {
                        self.state = DecodeState::ReadingSbbfBitset(range.clone());
                        return Ok(DecodeResult::NeedsData(vec![range]));
                    }
                    let bitset = self.get_bytes(&range)?;
                    self.state = DecodeState::Finished;
                    return Ok(DecodeResult::Data(Sbbf::new(&bitset)));
                }
                DecodeState::Finished => {
                    self.state = DecodeState::Finished;
                    return Ok(DecodeResult::Finished);
                }
                DecodeState::Intermediate => {
                    return Err(general_err!(
                        "SbbfPushDecoder: internal error, invalid state"
                    ));
                }
            }
        }
    }
}
