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

use crate::schema::Schema;
use arrow_schema::ArrowError;

enum HeaderDecoderState {
    /// Decoding the [`MAGIC`] prefix
    Magic,
    /// Decoding a block count
    BlockCount,
    /// Decoding a block byte length
    BlockLen,
    /// Decoding a key length
    KeyLen,
    /// Decoding a key string
    Key,
    /// Decoding a value length
    ValueLen,
    /// Decoding a value payload
    Value,
    /// Decoding sync marker
    Sync,
    /// Finished decoding
    Finished,
}

/// A decoded header for an [Object Container File](https://avro.apache.org/docs/1.11.1/specification/#object-container-files)
#[derive(Debug, Clone)]
pub struct Header {
    meta_offsets: Vec<usize>,
    meta_buf: Vec<u8>,
    sync: u128,
}

impl Header {
    /// Returns an iterator over the meta keys in this header
    pub fn metadata(&self) -> impl Iterator<Item = (&[u8], &[u8])> {
        let mut last = 0;
        self.meta_offsets.windows(2).map(move |w| {
            let start = last;
            last = w[1];
            (&self.meta_buf[start..w[0]], &self.meta_buf[w[0]..w[1]])
        })
    }

    /// Returns the value for a given metadata key if present
    pub fn get(&self, key: impl AsRef<[u8]>) -> Option<&[u8]> {
        self.metadata()
            .find_map(|(k, v)| (k == key.as_ref()).then_some(v))
    }

    /// Returns the sync token for this file
    pub fn sync(&self) -> u128 {
        self.sync
    }
}

/// A decoder for [`Header`]
///
/// Unfortunately the avro file format does not encode the length of the header, and so it
/// is necessary to provide a push-based decoder that can be used with streams
pub struct HeaderDecoder {
    state: HeaderDecoderState,

    meta_offsets: Vec<usize>,
    meta_buf: Vec<u8>,

    /// The decoded sync marker
    sync_marker: [u8; 16],

    /// Scratch space for decoding VLQ integers
    vlq_scratch: u64,
    vlq_shift: u32,

    /// The number of remaining tuples in the current block
    tuples_remaining: usize,
    /// The number of bytes remaining in the current string/bytes payload
    bytes_remaining: usize,
}

impl Default for HeaderDecoder {
    fn default() -> Self {
        Self {
            state: HeaderDecoderState::Magic,
            meta_offsets: vec![],
            meta_buf: vec![],
            sync_marker: [0; 16],
            vlq_scratch: 0,
            vlq_shift: 0,
            tuples_remaining: 0,
            bytes_remaining: MAGIC.len(),
        }
    }
}

const MAGIC: &[u8; 4] = b"Obj\x01";

impl HeaderDecoder {
    pub fn decode(&mut self, mut buf: &[u8]) -> Result<usize, ArrowError> {
        let max_read = buf.len();
        while !buf.is_empty() {
            match self.state {
                HeaderDecoderState::Magic => {
                    let remaining = &MAGIC[MAGIC.len() - self.bytes_remaining..];
                    let to_decode = buf.len().min(remaining.len());
                    if !buf.starts_with(&remaining[..to_decode]) {
                        return Err(ArrowError::ParseError(
                            "Incorrect avro magic".to_string(),
                        ));
                    }
                    self.bytes_remaining -= to_decode;
                    buf = &buf[to_decode..];
                    if self.bytes_remaining == 0 {
                        self.state = HeaderDecoderState::BlockCount;
                        self.vlq_scratch = 0;
                        self.vlq_shift = 0;
                    }
                }
                HeaderDecoderState::BlockCount => {
                    if let Some(block_count) = self.long(&mut buf) {
                        match block_count.try_into() {
                            Ok(0) => {
                                self.state = HeaderDecoderState::Sync;
                                self.bytes_remaining = 16;
                            }
                            Ok(remaining) => {
                                self.tuples_remaining = remaining;
                                self.state = HeaderDecoderState::KeyLen;
                            }
                            Err(_) => {
                                self.tuples_remaining = block_count.unsigned_abs() as _;
                                self.state = HeaderDecoderState::BlockLen;
                            }
                        }
                    }
                }
                HeaderDecoderState::BlockLen => {
                    if self.long(&mut buf).is_some() {
                        self.state = HeaderDecoderState::KeyLen
                    }
                }
                HeaderDecoderState::Key => {
                    let to_read = self.bytes_remaining.min(buf.len());
                    self.meta_buf.extend_from_slice(&buf[..to_read]);
                    self.bytes_remaining -= to_read;
                    buf = &buf[to_read..];
                    if self.bytes_remaining == 0 {
                        self.meta_offsets.push(self.meta_buf.len());
                        self.state = HeaderDecoderState::ValueLen;
                    }
                }
                HeaderDecoderState::Value => {
                    let to_read = self.bytes_remaining.min(buf.len());
                    self.meta_buf.extend_from_slice(&buf[..to_read]);
                    self.bytes_remaining -= to_read;
                    buf = &buf[to_read..];
                    if self.bytes_remaining == 0 {
                        self.meta_offsets.push(self.meta_buf.len());

                        self.tuples_remaining -= 1;
                        match self.tuples_remaining {
                            0 => self.state = HeaderDecoderState::BlockCount,
                            _ => self.state = HeaderDecoderState::KeyLen,
                        }
                    }
                }
                HeaderDecoderState::KeyLen => {
                    if let Some(len) = self.long(&mut buf) {
                        self.bytes_remaining = len as _;
                        self.state = HeaderDecoderState::Key;
                    }
                }
                HeaderDecoderState::ValueLen => {
                    if let Some(len) = self.long(&mut buf) {
                        self.bytes_remaining = len as _;
                        self.state = HeaderDecoderState::Value;
                    }
                }
                HeaderDecoderState::Sync => {
                    let to_decode = buf.len().min(self.bytes_remaining);
                    let write = &mut self.sync_marker[16 - to_decode..];
                    write[..to_decode].copy_from_slice(&buf[..to_decode]);
                    self.bytes_remaining -= to_decode;
                    if self.bytes_remaining == 0 {
                        self.state = HeaderDecoderState::Finished;
                    }
                }
                HeaderDecoderState::Finished => return Ok(max_read - buf.len()),
            }
        }
        Ok(max_read)
    }

    /// Flush this decoder returning the parsed [`Header`] if any
    pub fn flush(&mut self) -> Option<Header> {
        match self.state {
            HeaderDecoderState::Finished => {
                self.state = HeaderDecoderState::Magic;
                Some(Header {
                    meta_offsets: std::mem::take(&mut self.meta_offsets),
                    meta_buf: std::mem::take(&mut self.meta_buf),
                    sync: u128::from_ne_bytes(self.sync_marker),
                })
            }
            _ => None,
        }
    }

    /// Decode a signed long from `buf`
    fn long(&mut self, buf: &mut &[u8]) -> Option<i64> {
        while let Some(byte) = buf.first().copied() {
            *buf = &buf[1..];
            self.vlq_scratch |= ((byte & 0x7F) as u64) << self.vlq_shift;
            self.vlq_shift += 7;
            if byte & 0x80 == 0 {
                let val = self.vlq_scratch;
                self.vlq_scratch = 0;
                self.vlq_shift = 0;
                return Some((val >> 1) as i64 ^ -((val & 1) as i64));
            }
        }
        None
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::schema::SCHEMA_METADATA_KEY;
    use std::fs::File;
    use std::io::{BufRead, BufReader};

    #[test]
    fn test_header_decode() {
        let mut decoder = HeaderDecoder::default();
        for m in MAGIC {
            decoder.decode(std::slice::from_ref(m)).unwrap();
        }

        let mut decoder = HeaderDecoder::default();
        decoder.decode(MAGIC).unwrap();

        let mut decoder = HeaderDecoder::default();
        decoder.decode(b"Ob").unwrap();
        let err = decoder.decode(b"s").unwrap_err().to_string();
        assert_eq!(err, "Parser error: Incorrect avro magic");
    }

    fn read_header(file: &str) -> Header {
        let file = File::open(file).unwrap();

        let mut decoder = HeaderDecoder::default();
        let mut buf_reader = BufReader::with_capacity(100, file);
        loop {
            let buf = buf_reader.fill_buf().unwrap();
            let decoded = decoder.decode(buf).unwrap();
            buf_reader.consume(decoded);
            if decoded == 0 {
                break;
            }
        }
        decoder.flush().unwrap()
    }

    #[test]
    fn test_header() {
        let header = read_header("../testing/data/avro/alltypes_plain.avro");
        let schema_json = header.get(SCHEMA_METADATA_KEY).unwrap();
        let _schema: Schema<'_> = serde_json::from_slice(schema_json).unwrap();
        assert_eq!(header.sync(), 226966037233754408753420635932530907102);

        let header = read_header("../testing/data/avro/fixed_length_decimal.avro");
        let schema_json = header.get(SCHEMA_METADATA_KEY).unwrap();
        let _schema: Schema<'_> = serde_json::from_slice(schema_json).unwrap();
        assert_eq!(header.sync(), 325166208089902833952788552656412487328);
    }
}
