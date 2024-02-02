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

//! Decoder for [`Header`]

use crate::compression::{CompressionCodec, CODEC_METADATA_KEY};
use crate::reader::vlq::VLQDecoder;
use crate::schema::Schema;
use arrow_schema::ArrowError;

#[derive(Debug)]
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
    sync: [u8; 16],
}

impl Header {
    /// Returns an iterator over the meta keys in this header
    pub fn metadata(&self) -> impl Iterator<Item = (&[u8], &[u8])> {
        let mut last = 0;
        self.meta_offsets.chunks_exact(2).map(move |w| {
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
    pub fn sync(&self) -> [u8; 16] {
        self.sync
    }

    /// Returns the [`CompressionCodec`] if any
    pub fn compression(&self) -> Result<Option<CompressionCodec>, ArrowError> {
        let v = self.get(CODEC_METADATA_KEY);

        match v {
            None | Some(b"null") => Ok(None),
            Some(b"deflate") => Ok(Some(CompressionCodec::Deflate)),
            Some(b"snappy") => Ok(Some(CompressionCodec::Snappy)),
            Some(b"zstandard") => Ok(Some(CompressionCodec::ZStandard)),
            Some(v) => Err(ArrowError::ParseError(format!(
                "Unrecognized compression codec \'{}\'",
                String::from_utf8_lossy(v)
            ))),
        }
    }
}

/// A decoder for [`Header`]
///
/// The avro file format does not encode the length of the header, and so it
/// is necessary to provide a push-based decoder that can be used with streams
#[derive(Debug)]
pub struct HeaderDecoder {
    state: HeaderDecoderState,
    vlq_decoder: VLQDecoder,

    /// The end offsets of strings in `meta_buf`
    meta_offsets: Vec<usize>,
    /// The raw binary data of the metadata map
    meta_buf: Vec<u8>,

    /// The decoded sync marker
    sync_marker: [u8; 16],

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
            vlq_decoder: Default::default(),
            tuples_remaining: 0,
            bytes_remaining: MAGIC.len(),
        }
    }
}

const MAGIC: &[u8; 4] = b"Obj\x01";

impl HeaderDecoder {
    /// Parse [`Header`] from `buf`, returning the number of bytes read
    ///
    /// This method can be called multiple times with consecutive chunks of data, allowing
    /// integration with chunked IO systems like [`BufRead::fill_buf`]
    ///
    /// All errors should be considered fatal, and decoding aborted
    ///
    /// Once the entire [`Header`] has been decoded this method will not read any further
    /// input bytes, and the header can be obtained with [`Self::flush`]
    ///
    /// [`BufRead::fill_buf`]: std::io::BufRead::fill_buf
    pub fn decode(&mut self, mut buf: &[u8]) -> Result<usize, ArrowError> {
        let max_read = buf.len();
        while !buf.is_empty() {
            match self.state {
                HeaderDecoderState::Magic => {
                    let remaining = &MAGIC[MAGIC.len() - self.bytes_remaining..];
                    let to_decode = buf.len().min(remaining.len());
                    if !buf.starts_with(&remaining[..to_decode]) {
                        return Err(ArrowError::ParseError("Incorrect avro magic".to_string()));
                    }
                    self.bytes_remaining -= to_decode;
                    buf = &buf[to_decode..];
                    if self.bytes_remaining == 0 {
                        self.state = HeaderDecoderState::BlockCount;
                    }
                }
                HeaderDecoderState::BlockCount => {
                    if let Some(block_count) = self.vlq_decoder.long(&mut buf) {
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
                    if self.vlq_decoder.long(&mut buf).is_some() {
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
                    if let Some(len) = self.vlq_decoder.long(&mut buf) {
                        self.bytes_remaining = len as _;
                        self.state = HeaderDecoderState::Key;
                    }
                }
                HeaderDecoderState::ValueLen => {
                    if let Some(len) = self.vlq_decoder.long(&mut buf) {
                        self.bytes_remaining = len as _;
                        self.state = HeaderDecoderState::Value;
                    }
                }
                HeaderDecoderState::Sync => {
                    let to_decode = buf.len().min(self.bytes_remaining);
                    let write = &mut self.sync_marker[16 - to_decode..];
                    write[..to_decode].copy_from_slice(&buf[..to_decode]);
                    self.bytes_remaining -= to_decode;
                    buf = &buf[to_decode..];
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
                    sync: self.sync_marker,
                })
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::codec::{AvroDataType, AvroField};
    use crate::reader::read_header;
    use crate::schema::SCHEMA_METADATA_KEY;
    use crate::test_util::arrow_test_data;
    use arrow_schema::{DataType, Field, Fields, TimeUnit};
    use std::fs::File;
    use std::io::{BufRead, BufReader};

    #[test]
    fn test_header_decode() {
        let mut decoder = HeaderDecoder::default();
        for m in MAGIC {
            decoder.decode(std::slice::from_ref(m)).unwrap();
        }

        let mut decoder = HeaderDecoder::default();
        assert_eq!(decoder.decode(MAGIC).unwrap(), 4);

        let mut decoder = HeaderDecoder::default();
        decoder.decode(b"Ob").unwrap();
        let err = decoder.decode(b"s").unwrap_err().to_string();
        assert_eq!(err, "Parser error: Incorrect avro magic");
    }

    fn decode_file(file: &str) -> Header {
        let file = File::open(file).unwrap();
        read_header(BufReader::with_capacity(100, file)).unwrap()
    }

    #[test]
    fn test_header() {
        let header = decode_file(&arrow_test_data("avro/alltypes_plain.avro"));
        let schema_json = header.get(SCHEMA_METADATA_KEY).unwrap();
        let expected = br#"{"type":"record","name":"topLevelRecord","fields":[{"name":"id","type":["int","null"]},{"name":"bool_col","type":["boolean","null"]},{"name":"tinyint_col","type":["int","null"]},{"name":"smallint_col","type":["int","null"]},{"name":"int_col","type":["int","null"]},{"name":"bigint_col","type":["long","null"]},{"name":"float_col","type":["float","null"]},{"name":"double_col","type":["double","null"]},{"name":"date_string_col","type":["bytes","null"]},{"name":"string_col","type":["bytes","null"]},{"name":"timestamp_col","type":[{"type":"long","logicalType":"timestamp-micros"},"null"]}]}"#;
        assert_eq!(schema_json, expected);
        let schema: Schema<'_> = serde_json::from_slice(schema_json).unwrap();
        let field = AvroField::try_from(&schema).unwrap();

        assert_eq!(
            field.field(),
            Field::new(
                "topLevelRecord",
                DataType::Struct(Fields::from(vec![
                    Field::new("id", DataType::Int32, true),
                    Field::new("bool_col", DataType::Boolean, true),
                    Field::new("tinyint_col", DataType::Int32, true),
                    Field::new("smallint_col", DataType::Int32, true),
                    Field::new("int_col", DataType::Int32, true),
                    Field::new("bigint_col", DataType::Int64, true),
                    Field::new("float_col", DataType::Float32, true),
                    Field::new("double_col", DataType::Float64, true),
                    Field::new("date_string_col", DataType::Binary, true),
                    Field::new("string_col", DataType::Binary, true),
                    Field::new(
                        "timestamp_col",
                        DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
                        true
                    ),
                ])),
                false
            )
        );

        assert_eq!(
            u128::from_le_bytes(header.sync()),
            226966037233754408753420635932530907102
        );

        let header = decode_file(&arrow_test_data("avro/fixed_length_decimal.avro"));

        let meta: Vec<_> = header
            .metadata()
            .map(|(k, _)| std::str::from_utf8(k).unwrap())
            .collect();

        assert_eq!(
            meta,
            &["avro.schema", "org.apache.spark.version", "avro.codec"]
        );

        let schema_json = header.get(SCHEMA_METADATA_KEY).unwrap();
        let expected = br#"{"type":"record","name":"topLevelRecord","fields":[{"name":"value","type":[{"type":"fixed","name":"fixed","namespace":"topLevelRecord.value","size":11,"logicalType":"decimal","precision":25,"scale":2},"null"]}]}"#;
        assert_eq!(schema_json, expected);
        let _schema: Schema<'_> = serde_json::from_slice(schema_json).unwrap();
        assert_eq!(
            u128::from_le_bytes(header.sync()),
            325166208089902833952788552656412487328
        );
    }
}
