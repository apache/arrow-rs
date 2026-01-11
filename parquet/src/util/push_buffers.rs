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

use crate::errors::ParquetError;
use crate::file::reader::{ChunkReader, Length};
use bytes::Bytes;
use std::fmt::Display;
use std::ops::Range;

/// Holds multiple buffers of data
///
/// This is the in-memory buffer for the ParquetDecoder and ParquetMetadataDecoders
///
/// Features:
/// 1. Zero copy
/// 2. non contiguous ranges of bytes
///
/// # Non Coalescing
///
/// This buffer does not coalesce  (merging adjacent ranges of bytes into a
/// single range). Coalescing at this level would require copying the data but
/// the caller may already have the needed data in a single buffer which would
/// require no copying.
///
/// Thus, the implementation defers to the caller to coalesce subsequent requests
/// if desired.
#[derive(Debug, Clone)]
pub(crate) struct PushBuffers {
    /// the virtual "offset" of this buffers (added to any request)
    offset: u64,
    /// The total length of the file being decoded
    file_len: u64,
    /// The ranges of data that are available for decoding (not adjusted for offset)
    ranges: Vec<Range<u64>>,
    /// The buffers of data that can be used to decode the Parquet file
    buffers: Vec<Bytes>,
}

impl Display for PushBuffers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "Buffers (offset: {}, file_len: {})",
            self.offset, self.file_len
        )?;
        writeln!(f, "Available Ranges (w/ offset):")?;
        for range in &self.ranges {
            writeln!(
                f,
                "  {}..{} ({}..{}): {} bytes",
                range.start,
                range.end,
                range.start + self.offset,
                range.end + self.offset,
                range.end - range.start
            )?;
        }

        Ok(())
    }
}

impl PushBuffers {
    /// Create a new Buffers instance with the given file length
    pub fn new(file_len: u64) -> Self {
        Self {
            offset: 0,
            file_len,
            ranges: Vec::new(),
            buffers: Vec::new(),
        }
    }

    /// Push all the ranges and buffers
    pub fn push_ranges(&mut self, ranges: Vec<Range<u64>>, buffers: Vec<Bytes>) {
        assert_eq!(
            ranges.len(),
            buffers.len(),
            "Number of ranges must match number of buffers"
        );
        for (range, buffer) in ranges.into_iter().zip(buffers.into_iter()) {
            self.push_range(range, buffer);
        }
    }

    /// Push a new range and its associated buffer
    pub fn push_range(&mut self, range: Range<u64>, buffer: Bytes) {
        assert_eq!(
            (range.end - range.start) as usize,
            buffer.len(),
            "Range length must match buffer length"
        );
        self.ranges.push(range);
        self.buffers.push(buffer);
    }

    /// Returns true if the Buffers contains data for the given range
    pub fn has_range(&self, range: &Range<u64>) -> bool {
        self.ranges
            .iter()
            .any(|r| r.start <= range.start && r.end >= range.end)
    }

    fn iter(&self) -> impl Iterator<Item = (&Range<u64>, &Bytes)> {
        self.ranges.iter().zip(self.buffers.iter())
    }

    /// return the file length of the Parquet file being read
    pub fn file_len(&self) -> u64 {
        self.file_len
    }

    /// Specify a new offset
    pub fn with_offset(mut self, offset: u64) -> Self {
        self.offset = offset;
        self
    }

    /// Return the total of all buffered ranges
    #[cfg(feature = "arrow")]
    pub fn buffered_bytes(&self) -> u64 {
        self.ranges.iter().map(|r| r.end - r.start).sum()
    }

    /// Clear any range and corresponding buffer that is exactly in the ranges_to_clear
    #[cfg(feature = "arrow")]
    pub fn clear_ranges(&mut self, ranges_to_clear: &[Range<u64>]) {
        let mut new_ranges = Vec::new();
        let mut new_buffers = Vec::new();

        for (range, buffer) in self.iter() {
            if !ranges_to_clear
                .iter()
                .any(|r| r.start == range.start && r.end == range.end)
            {
                new_ranges.push(range.clone());
                new_buffers.push(buffer.clone());
            }
        }
        self.ranges = new_ranges;
        self.buffers = new_buffers;
    }
}

impl Length for PushBuffers {
    fn len(&self) -> u64 {
        self.file_len
    }
}

/// less efficient implementation of Read for Buffers
impl std::io::Read for PushBuffers {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        // Find the range that contains the start offset
        let mut found = false;
        for (range, data) in self.iter() {
            if range.start <= self.offset && range.end >= self.offset + buf.len() as u64 {
                // Found the range, figure out the starting offset in the buffer
                let start_offset = (self.offset - range.start) as usize;
                let end_offset = start_offset + buf.len();
                let slice = data.slice(start_offset..end_offset);
                buf.copy_from_slice(slice.as_ref());
                found = true;
                break;
            }
        }
        if found {
            // If we found the range, we can return the number of bytes read
            // advance our offset
            self.offset += buf.len() as u64;
            Ok(buf.len())
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "No data available in Buffers",
            ))
        }
    }
}

impl ChunkReader for PushBuffers {
    type T = Self;

    fn get_read(&self, start: u64) -> Result<Self::T, ParquetError> {
        Ok(self.clone().with_offset(self.offset + start))
    }

    fn get_bytes(&self, start: u64, length: usize) -> Result<Bytes, ParquetError> {
        // find the range that contains the start offset
        for (range, data) in self.iter() {
            if range.start <= start && range.end >= start + length as u64 {
                // Found the range, figure out the starting offset in the buffer
                let start_offset = (start - range.start) as usize;
                return Ok(data.slice(start_offset..start_offset + length));
            }
        }
        // Signal that we need more data
        let requested_end = start + length as u64;
        Err(ParquetError::NeedMoreDataRange(start..requested_end))
    }
}
