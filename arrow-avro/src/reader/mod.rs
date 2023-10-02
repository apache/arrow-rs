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

//! Read Avro data to Arrow

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
                        return Err(ArrowError::ParseError(format!(
                            "Incorrect avro magic"
                        )));
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
    use std::fs::File;
    use std::io::{BufRead, BufReader};
    use super::*;

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

    #[test]
    fn test_header() {
        let file = File::open("../testing/data/avro/alltypes_plain.avro").unwrap();

        let mut decoder = HeaderDecoder::default();
        let mut buf_reader = BufReader::with_capacity(100, file);
        loop {
            let buf = buf_reader.fill_buf().unwrap();
            let decoded = decoder.decode(buf).unwrap();
            buf_reader.consume(decoded);
            if decoded == 0 {
                break
            }
        }

        let mut offset = 0;
        for end in decoder.meta_offsets {
            let s = &decoder.meta_buf[offset..end];
            offset = end;
            println!("{}", String::from_utf8_lossy(s))
        }
    }
}
