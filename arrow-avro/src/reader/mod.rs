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

//! Avro reader
//!
//! This module provides facilities to read Apache Avro-encoded files or streams
//! into Arrow's [`RecordBatch`] format. In particular, it introduces:
//!
//! * [`ReaderBuilder`]: Configures Avro reading, e.g., batch size
//! * [`Reader`]: Yields [`RecordBatch`] values, implementing [`Iterator`]
//! * [`Decoder`]: A low-level push-based decoder for Avro records
//!
//! # Basic Usage
//!
//! [`Reader`] can be used directly with synchronous data sources, such as [`std::fs::File`].
//!
//! ## Reading a Single Batch
//!
//! ```
//! # use std::fs::File;
//! # use std::io::BufReader;
//! # use arrow_avro::reader::ReaderBuilder;
//!
//! let file = File::open("../testing/data/avro/alltypes_plain.avro").unwrap();
//! let mut avro = ReaderBuilder::new().build(BufReader::new(file)).unwrap();
//! let batch = avro.next().unwrap();
//! ```
//!
//! # Async Usage
//!
//! The lower-level [`Decoder`] can be integrated with various forms of async data streams,
//! and is designed to be agnostic to different async IO primitives within
//! the Rust ecosystem. It works by incrementally decoding Avro data from byte slices.
//!
//! For example, see below for how it could be used with an arbitrary `Stream` of `Bytes`:
//!
//! ```
//! # use std::task::{Poll, ready};
//! # use bytes::{Buf, Bytes};
//! # use arrow_schema::ArrowError;
//! # use futures::stream::{Stream, StreamExt};
//! # use arrow_array::RecordBatch;
//! # use arrow_avro::reader::Decoder;
//!
//! fn decode_stream<S: Stream<Item = Bytes> + Unpin>(
//!     mut decoder: Decoder,
//!     mut input: S,
//! ) -> impl Stream<Item = Result<RecordBatch, ArrowError>> {
//!     let mut buffered = Bytes::new();
//!     futures::stream::poll_fn(move |cx| {
//!         loop {
//!             if buffered.is_empty() {
//!                 buffered = match ready!(input.poll_next_unpin(cx)) {
//!                     Some(b) => b,
//!                     None => break,
//!                 };
//!             }
//!             let decoded = match decoder.decode(buffered.as_ref()) {
//!                 Ok(decoded) => decoded,
//!                 Err(e) => return Poll::Ready(Some(Err(e))),
//!             };
//!             let read = buffered.len();
//!             buffered.advance(decoded);
//!             if decoded != read {
//!                 break
//!             }
//!         }
//!         // Convert any fully-decoded rows to a RecordBatch, if available
//!         Poll::Ready(decoder.flush().transpose())
//!     })
//! }
//! ```
//!

use crate::codec::AvroField;
use crate::schema::Schema as AvroSchema;
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, SchemaRef};
use block::BlockDecoder;
use header::{Header, HeaderDecoder};
use record::RecordDecoder;
use std::io::BufRead;

mod block;
mod cursor;
mod header;
mod record;
mod vlq;

/// Read the Avro file header (magic, metadata, sync marker) from `reader`.
fn read_header<R: BufRead>(mut reader: R) -> Result<Header, ArrowError> {
    let mut decoder = HeaderDecoder::default();
    loop {
        let buf = reader.fill_buf()?;
        if buf.is_empty() {
            break;
        }
        let read = buf.len();
        let decoded = decoder.decode(buf)?;
        reader.consume(decoded);
        if decoded != read {
            break;
        }
    }
    decoder.flush().ok_or_else(|| {
        ArrowError::ParseError("Unexpected EOF while reading Avro header".to_string())
    })
}

/// A low-level interface for decoding Avro-encoded bytes into Arrow [`RecordBatch`].
#[derive(Debug)]
pub struct Decoder {
    record_decoder: RecordDecoder,
    batch_size: usize,
    decoded_rows: usize,
}

impl Decoder {
    /// Create a new [`Decoder`], wrapping an existing [`RecordDecoder`].
    pub fn new(record_decoder: RecordDecoder, batch_size: usize) -> Self {
        Self {
            record_decoder,
            batch_size,
            decoded_rows: 0,
        }
    }

    /// Return the Arrow schema for the rows decoded by this decoder
    pub fn schema(&self) -> SchemaRef {
        self.record_decoder.schema().clone()
    }

    /// Return the configured maximum number of rows per batch
    pub fn batch_size(&self) -> usize {
        self.batch_size
    }

    /// Feed `data` into the decoder row by row until we either:
    /// - consume all bytes in `data`, or
    /// - reach `batch_size` decoded rows.
    ///
    /// Returns the number of bytes consumed.
    pub fn decode(&mut self, data: &[u8]) -> Result<usize, ArrowError> {
        let mut total_consumed = 0usize;
        while total_consumed < data.len() && self.decoded_rows < self.batch_size {
            let consumed = self.record_decoder.decode(&data[total_consumed..], 1)?;
            if consumed == 0 {
                break;
            }
            total_consumed += consumed;
            self.decoded_rows += 1;
        }
        Ok(total_consumed)
    }

    /// Produce a [`RecordBatch`] if at least one row is fully decoded, returning
    /// `Ok(None)` if no new rows are available.
    pub fn flush(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        if self.decoded_rows == 0 {
            Ok(None)
        } else {
            let batch = self.record_decoder.flush()?;
            self.decoded_rows = 0;
            Ok(Some(batch))
        }
    }
}

/// A builder to create an [`Avro Reader`](Reader) that reads Avro data
/// into Arrow [`RecordBatch`].
#[derive(Debug)]
pub struct ReaderBuilder {
    batch_size: usize,
    strict_mode: bool,
    utf8_view: bool,
    schema: Option<AvroSchema<'static>>,
}

impl Default for ReaderBuilder {
    fn default() -> Self {
        Self {
            batch_size: 1024,
            strict_mode: false,
            utf8_view: false,
            schema: None,
        }
    }
}

impl ReaderBuilder {
    /// Creates a new [`ReaderBuilder`] with default settings:
    /// - `batch_size` = 1024
    /// - `strict_mode` = false
    /// - `utf8_view` = false
    /// - `schema` = None
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the row-based batch size
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Set whether to use StringViewArray for string data
    ///
    /// When enabled, string data from Avro files will be loaded into
    /// Arrow's StringViewArray instead of the standard StringArray.
    pub fn with_utf8_view(mut self, utf8_view: bool) -> Self {
        self.utf8_view = utf8_view;
        self
    }

    /// Get whether StringViewArray is enabled for string data
    pub fn use_utf8view(&self) -> bool {
        self.utf8_view
    }

    /// Controls whether certain Avro unions of the form `[T, "null"]` should produce an error.
    pub fn with_strict_mode(mut self, strict_mode: bool) -> Self {
        self.strict_mode = strict_mode;
        self
    }

    /// Sets the Avro schema.
    ///
    /// If a schema is not provided, the schema will be read from the Avro file header.
    pub fn with_schema(mut self, schema: AvroSchema<'static>) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Create a [`Reader`] from this builder and a `BufRead`
    pub fn build<R: BufRead>(self, mut reader: R) -> Result<Reader<R>, ArrowError> {
        let header = read_header(&mut reader)?;
        let compression = header.compression()?;
        let root_field = if let Some(schema) = &self.schema {
            AvroField::try_from(schema)?
        } else {
            let avro_schema: Option<AvroSchema<'_>> = header
                .schema()
                .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;
            let avro_schema = avro_schema.ok_or_else(|| {
                ArrowError::ParseError("No Avro schema present in file header".to_string())
            })?;
            AvroField::try_from(&avro_schema)?
        };
        let record_decoder = RecordDecoder::try_new_with_options(
            root_field.data_type(),
            self.utf8_view,
            self.strict_mode,
        )?;
        let decoder = Decoder::new(record_decoder, self.batch_size);
        Ok(Reader {
            reader,
            header,
            compression,
            decoder,
            block_decoder: BlockDecoder::default(),
            block_data: Vec::new(),
            finished: false,
        })
    }

    /// Create a [`Decoder`] from this builder and a `BufRead` by
    /// reading and parsing the Avro file's header. This will
    /// not create a full [`Reader`].
    pub fn build_decoder<R: BufRead>(self, mut reader: R) -> Result<Decoder, ArrowError> {
        let record_decoder = if let Some(schema) = self.schema {
            let root_field = AvroField::try_from(&schema)?;
            RecordDecoder::try_new_with_options(
                root_field.data_type(),
                self.utf8_view,
                self.strict_mode,
            )?
        } else {
            let header = read_header(&mut reader)?;
            let avro_schema = header
                .schema()
                .map_err(|e| ArrowError::ExternalError(Box::new(e)))?
                .ok_or_else(|| {
                    ArrowError::ParseError("No Avro schema present in file header".to_string())
                })?;
            let root_field = AvroField::try_from(&avro_schema)?;
            RecordDecoder::try_new_with_options(
                root_field.data_type(),
                self.utf8_view,
                self.strict_mode,
            )?
        };
        Ok(Decoder::new(record_decoder, self.batch_size))
    }
}

/// A high-level Avro `Reader` that reads container-file blocks
/// and feeds them into a row-level [`Decoder`].
#[derive(Debug)]
pub struct Reader<R> {
    reader: R,
    header: Header,
    compression: Option<crate::compression::CompressionCodec>,
    decoder: Decoder,
    block_decoder: BlockDecoder,
    block_data: Vec<u8>,
    finished: bool,
}

impl<R> Reader<R> {
    /// Return the Arrow schema discovered from the Avro file header
    pub fn schema(&self) -> SchemaRef {
        self.decoder.schema()
    }

    /// Return the Avro container-file header
    pub fn avro_header(&self) -> &Header {
        &self.header
    }
}

impl<R: BufRead> Reader<R> {
    /// Reads the next [`RecordBatch`] from the Avro file or `Ok(None)` on EOF
    fn read(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        if self.finished {
            return Ok(None);
        }
        loop {
            if !self.block_data.is_empty() {
                let consumed = self.decoder.decode(&self.block_data)?;
                if consumed > 0 {
                    self.block_data.drain(..consumed);
                }
                match self.decoder.flush()? {
                    None => {
                        if !self.block_data.is_empty() {
                            break;
                        }
                    }
                    Some(batch) => {
                        return Ok(Some(batch));
                    }
                }
            }
            let maybe_block = {
                let buf = self.reader.fill_buf()?;
                if buf.is_empty() {
                    None
                } else {
                    let read_len = buf.len();
                    let consumed_len = self.block_decoder.decode(buf)?;
                    self.reader.consume(consumed_len);
                    if consumed_len == 0 && read_len != 0 {
                        return Err(ArrowError::ParseError(
                            "Could not decode next Avro block from partial data".to_string(),
                        ));
                    }
                    self.block_decoder.flush()
                }
            };
            match maybe_block {
                Some(block) => {
                    let block_data = if let Some(ref codec) = self.compression {
                        codec.decompress(&block.data)?
                    } else {
                        block.data
                    };
                    self.block_data = block_data;
                }
                None => {
                    self.finished = true;
                    if !self.block_data.is_empty() {
                        let consumed = self.decoder.decode(&self.block_data)?;
                        self.block_data.drain(..consumed);
                    }
                    return self.decoder.flush();
                }
            }
        }
        self.decoder.flush()
    }
}

impl<R: BufRead> Iterator for Reader<R> {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.read() {
            Ok(Some(batch)) => Some(Ok(batch)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

impl<R: BufRead> RecordBatchReader for Reader<R> {
    fn schema(&self) -> SchemaRef {
        self.schema()
    }
}

#[cfg(test)]
mod test {
    use crate::codec::{AvroDataType, AvroField, Codec};
    use crate::compression::CompressionCodec;
    use crate::reader::record::RecordDecoder;
    use crate::reader::vlq::VLQDecoder;
    use crate::reader::{read_header, Decoder, ReaderBuilder};
    use crate::test_util::arrow_test_data;
    use arrow_array::*;
    use arrow_schema::{ArrowError, DataType, Field, Schema};
    use bytes::{Buf, BufMut, Bytes};
    use futures::executor::block_on;
    use futures::{stream, Stream, StreamExt, TryStreamExt};
    use std::collections::HashMap;
    use std::fs;
    use std::fs::File;
    use std::io::{BufReader, Cursor, Read};
    use std::sync::Arc;
    use std::task::{ready, Poll};

    fn read_file(path: &str, batch_size: usize, utf8_view: bool) -> RecordBatch {
        let file = File::open(path).unwrap();
        let reader = ReaderBuilder::new()
            .with_batch_size(batch_size)
            .with_utf8_view(utf8_view)
            .build(BufReader::new(file))
            .unwrap();
        let schema = reader.schema();
        let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
        arrow::compute::concat_batches(&schema, &batches).unwrap()
    }

    fn decode_stream<S: Stream<Item = Bytes> + Unpin>(
        mut decoder: Decoder,
        mut input: S,
    ) -> impl Stream<Item = Result<RecordBatch, ArrowError>> {
        let mut buffered = Bytes::new();
        futures::stream::poll_fn(move |cx| {
            loop {
                if buffered.is_empty() {
                    buffered = match ready!(input.poll_next_unpin(cx)) {
                        Some(b) => b,
                        None => break,
                    };
                }
                let decoded = match decoder.decode(buffered.as_ref()) {
                    Ok(decoded) => decoded,
                    Err(e) => return Poll::Ready(Some(Err(e))),
                };
                let read = buffered.len();
                buffered.advance(decoded);
                if decoded != read {
                    break;
                }
            }
            Poll::Ready(decoder.flush().transpose())
        })
    }

    #[test]
    fn test_utf8view_support() {
        let schema_json = r#"{
            "type": "record",
            "name": "test",
            "fields": [{
                "name": "str_field",
                "type": "string"
            }]
        }"#;

        let schema: crate::schema::Schema = serde_json::from_str(schema_json).unwrap();
        let avro_field = AvroField::try_from(&schema).unwrap();

        let data_type = avro_field.data_type();

        struct TestHelper;
        impl TestHelper {
            fn with_utf8view(field: &Field) -> Field {
                match field.data_type() {
                    DataType::Utf8 => {
                        Field::new(field.name(), DataType::Utf8View, field.is_nullable())
                            .with_metadata(field.metadata().clone())
                    }
                    _ => field.clone(),
                }
            }
        }

        let field = TestHelper::with_utf8view(&Field::new("str_field", DataType::Utf8, false));

        assert_eq!(field.data_type(), &DataType::Utf8View);

        let array = StringViewArray::from(vec!["test1", "test2"]);
        let batch =
            RecordBatch::try_from_iter(vec![("str_field", Arc::new(array) as ArrayRef)]).unwrap();

        assert!(batch.column(0).as_any().is::<StringViewArray>());
    }

    #[test]
    fn test_alltypes() {
        let files = [
            "avro/alltypes_plain.avro",
            "avro/alltypes_plain.snappy.avro",
            "avro/alltypes_plain.zstandard.avro",
        ];

        let expected = RecordBatch::try_from_iter_with_nullable([
            (
                "id",
                Arc::new(Int32Array::from(vec![4, 5, 6, 7, 2, 3, 0, 1])) as _,
                true,
            ),
            (
                "bool_col",
                Arc::new(BooleanArray::from_iter((0..8).map(|x| Some(x % 2 == 0)))) as _,
                true,
            ),
            (
                "tinyint_col",
                Arc::new(Int32Array::from_iter_values((0..8).map(|x| x % 2))) as _,
                true,
            ),
            (
                "smallint_col",
                Arc::new(Int32Array::from_iter_values((0..8).map(|x| x % 2))) as _,
                true,
            ),
            (
                "int_col",
                Arc::new(Int32Array::from_iter_values((0..8).map(|x| x % 2))) as _,
                true,
            ),
            (
                "bigint_col",
                Arc::new(Int64Array::from_iter_values((0..8).map(|x| (x % 2) * 10))) as _,
                true,
            ),
            (
                "float_col",
                Arc::new(Float32Array::from_iter_values(
                    (0..8).map(|x| (x % 2) as f32 * 1.1),
                )) as _,
                true,
            ),
            (
                "double_col",
                Arc::new(Float64Array::from_iter_values(
                    (0..8).map(|x| (x % 2) as f64 * 10.1),
                )) as _,
                true,
            ),
            (
                "date_string_col",
                Arc::new(BinaryArray::from_iter_values([
                    [48, 51, 47, 48, 49, 47, 48, 57],
                    [48, 51, 47, 48, 49, 47, 48, 57],
                    [48, 52, 47, 48, 49, 47, 48, 57],
                    [48, 52, 47, 48, 49, 47, 48, 57],
                    [48, 50, 47, 48, 49, 47, 48, 57],
                    [48, 50, 47, 48, 49, 47, 48, 57],
                    [48, 49, 47, 48, 49, 47, 48, 57],
                    [48, 49, 47, 48, 49, 47, 48, 57],
                ])) as _,
                true,
            ),
            (
                "string_col",
                Arc::new(BinaryArray::from_iter_values((0..8).map(|x| [48 + x % 2]))) as _,
                true,
            ),
            (
                "timestamp_col",
                Arc::new(
                    TimestampMicrosecondArray::from_iter_values([
                        1235865600000000, // 2009-03-01T00:00:00.000
                        1235865660000000, // 2009-03-01T00:01:00.000
                        1238544000000000, // 2009-04-01T00:00:00.000
                        1238544060000000, // 2009-04-01T00:01:00.000
                        1233446400000000, // 2009-02-01T00:00:00.000
                        1233446460000000, // 2009-02-01T00:01:00.000
                        1230768000000000, // 2009-01-01T00:00:00.000
                        1230768060000000, // 2009-01-01T00:01:00.000
                    ])
                    .with_timezone("+00:00"),
                ) as _,
                true,
            ),
        ])
        .unwrap();

        for file in files {
            let file = arrow_test_data(file);

            assert_eq!(read_file(&file, 8, false), expected);
            assert_eq!(read_file(&file, 3, false), expected);
        }
    }

    #[test]
    fn test_decode_stream_with_schema() {
        const PROVIDED_SCHEMA: &str =
            r#"{"type":"record","name":"test","fields":[{"name":"f2","type":"string"}]}"#;
        let schema_s2: crate::schema::Schema = serde_json::from_str(PROVIDED_SCHEMA).unwrap();
        let record_val = "some_string";
        let mut body = vec![];
        body.push((record_val.len() as u8) << 1);
        body.extend_from_slice(record_val.as_bytes());
        let mut reader_placeholder = Cursor::new(&[] as &[u8]);
        let decoder = ReaderBuilder::new()
            .with_batch_size(1)
            .with_schema(schema_s2)
            .build_decoder(&mut reader_placeholder)
            .unwrap();
        let stream = Box::pin(stream::once(async { Bytes::from(body) }));
        let decoded_stream = decode_stream(decoder, stream);
        let batches: Vec<RecordBatch> = block_on(decoded_stream.try_collect()).unwrap();
        let batch = arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap();
        let expected_field = Field::new("f2", DataType::Utf8, false);
        let expected_schema = Arc::new(Schema::new(vec![expected_field]));
        let expected_array = Arc::new(StringArray::from(vec![record_val]));
        let expected_batch = RecordBatch::try_new(expected_schema, vec![expected_array]).unwrap();
        assert_eq!(batch, expected_batch);
        assert_eq!(batch.schema().field(0).name(), "f2");
    }
}
