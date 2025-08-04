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
//! into Arrow's `RecordBatch` format. In particular, it introduces:
//!
//! * `ReaderBuilder`: Configures Avro reading, e.g., batch size
//! * `Reader`: Yields `RecordBatch` values, implementing `Iterator`
//! * `Decoder`: A low-level push-based decoder for Avro records
//!
//! # Basic Usage
//!
//! `Reader` can be used directly with synchronous data sources, such as [`std::fs::File`].
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
//! The lower-level `Decoder` can be integrated with various forms of async data streams,
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

use crate::codec::{AvroField, AvroFieldBuilder};
use crate::schema::{
    compare_schemas, generate_fingerprint, Fingerprint, FingerprintAlgorithm, Schema as AvroSchema,
    SchemaStore, SINGLE_OBJECT_MAGIC,
};
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, SchemaRef};
use block::BlockDecoder;
use header::{Header, HeaderDecoder};
use indexmap::IndexMap;
#[cfg(feature = "lru")]
use lru::LruCache;
use record::RecordDecoder;
use std::io::BufRead;
#[cfg(feature = "lru")]
use std::num::NonZeroUsize;

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

/// A low-level interface for decoding Avro-encoded bytes into Arrow `RecordBatch`.
#[derive(Debug)]
pub struct Decoder {
    active_decoder: RecordDecoder,
    active_fingerprint: Option<Fingerprint>,
    batch_size: usize,
    remaining_capacity: usize,
    #[cfg(feature = "lru")]
    cache: LruCache<Fingerprint, RecordDecoder>,
    #[cfg(not(feature = "lru"))]
    cache: IndexMap<Fingerprint, RecordDecoder>,
    max_cache_size: usize,
    reader_schema: Option<AvroSchema<'static>>,
    writer_schema_store: Option<SchemaStore<'static>>,
    utf8_view: bool,
    strict_mode: bool,
    pending_schema: Option<(Fingerprint, RecordDecoder)>,
}

impl Decoder {
    /// Return the Arrow schema for the rows decoded by this decoder
    pub fn schema(&self) -> SchemaRef {
        self.active_decoder.schema().clone()
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
        if self.writer_schema_store.is_some()
            && data.len() >= SINGLE_OBJECT_MAGIC.len()
            && !data.starts_with(&SINGLE_OBJECT_MAGIC)
        {
            return Err(ArrowError::ParseError(
                "Expected single‑object encoding fingerprint prefix for first message \
                 (writer_schema_store is set but active_fingerprint is None)"
                    .into(),
            ));
        }
        let mut total_consumed = 0usize;
        let hash_type = self.writer_schema_store.as_ref().map_or(
            FingerprintAlgorithm::Rabin,
            SchemaStore::fingerprint_algorithm,
        );
        // The loop stops when the batch is full, a schema change is staged,
        // or handle_prefix indicates we need more bytes (Some(0)).
        while total_consumed < data.len() && self.remaining_capacity > 0 {
            if let Some(n) = self.handle_prefix(&data[total_consumed..], hash_type)? {
                // We either consumed a prefix (n > 0) and need a schema switch, or we need
                // more bytes to make a decision. Either way, this decoding attempt is finished.
                total_consumed += n;
            }
            // No prefix: decode one row and keep going.
            let n = self.active_decoder.decode(&data[total_consumed..], 1)?;
            self.remaining_capacity -= 1;
            total_consumed += n;
        }
        Ok(total_consumed)
    }

    // Attempt to handle a single‑object‑encoding prefix at the current position.
    //
    // * Ok(None) – buffer does not start with the prefix.
    // * Ok(Some(0)) – prefix detected, but the buffer is too short; caller should await more bytes.
    // * Ok(Some(n)) – consumed `n > 0` bytes of a complete prefix (magic and fingerprint).
    fn handle_prefix(
        &mut self,
        buf: &[u8],
        hash_type: FingerprintAlgorithm,
    ) -> Result<Option<usize>, ArrowError> {
        // If there is no schema store, prefixes are unrecognized.
        if self.writer_schema_store.is_none() {
            return Ok(None); // Continue to decode the next record
        }
        // Need at least the magic bytes to decide (2 bytes).
        let Some(magic_bytes) = buf.get(..SINGLE_OBJECT_MAGIC.len()) else {
            return Ok(Some(0)); // Get more bytes
        };
        // Bail out early if the magic does not match.
        if magic_bytes != SINGLE_OBJECT_MAGIC {
            return Ok(None); // Continue to decode the next record
        }
        // Try to parse the fingerprint that follows the magic.
        let fingerprint_size = match hash_type {
            FingerprintAlgorithm::Rabin => self
                .handle_fingerprint(&buf[SINGLE_OBJECT_MAGIC.len()..], |bytes| {
                    Fingerprint::Rabin(u64::from_le_bytes(bytes))
                })?,
        };
        // Convert the inner result into a “bytes consumed” count.
        let consumed = match fingerprint_size {
            Some(n) => n + SINGLE_OBJECT_MAGIC.len(), // magic + fingerprint
            None => 0,                                // incomplete fingerprint
        };
        Ok(Some(consumed))
    }

    // Attempts to read and install a new fingerprint of `N` bytes.
    //
    // * Ok(None) – insufficient bytes (`buf.len() < `N`).
    // * Ok(Some(N)) – fingerprint consumed (always `N`).
    fn handle_fingerprint<const N: usize>(
        &mut self,
        buf: &[u8],
        fingerprint_from: impl FnOnce([u8; N]) -> Fingerprint,
    ) -> Result<Option<usize>, ArrowError> {
        // Need enough bytes to get fingerprint (next N bytes)
        let Some(fingerprint_bytes) = buf.get(..N) else {
            return Ok(None); // Get more bytes
        };
        // SAFETY: length checked above.
        let new_fingerprint = fingerprint_from(fingerprint_bytes.try_into().unwrap());
        // If the fingerprint indicates a schema change, prepare to switch decoders.
        if self.active_fingerprint != Some(new_fingerprint) {
            #[cfg(feature = "lru")]
            let new_decoder = self.cache.pop(&new_fingerprint);
            #[cfg(not(feature = "lru"))]
            let new_decoder = self.cache.shift_remove(&new_fingerprint);

            let new_decoder = match new_decoder {
                Some(decoder) => decoder,
                None => self.create_decoder_for(new_fingerprint)?,
            };
            self.pending_schema = Some((new_fingerprint, new_decoder));
            // If there are already decoded rows, we must flush them first.
            // Reducing `remaining_capacity` to 0 ensures `flush` is called next.
            if self.remaining_capacity < self.batch_size {
                self.remaining_capacity = 0;
            }
        }
        Ok(Some(N))
    }

    fn create_decoder_for(
        &mut self,
        new_fingerprint: Fingerprint,
    ) -> Result<RecordDecoder, ArrowError> {
        let Some(ref writer_schema_store) = self.writer_schema_store else {
            return Err(ArrowError::ParseError("Schema store unavailable".into())));
        };
        let Some(writer_schema) = writer_schema_store.lookup(&new_fingerprint) else {
            return Err(ArrowError::ParseError(format!(
                "Unknown fingerprint: {new_fingerprint:?}"
            )));
        };
        let Some(ref reader_schema) = self.reader_schema else {
            return Err(ArrowError::ParseError(
                "Reader schema unavailable for resolution".into(),
            ));
        };
        let resolved = AvroField::resolve_from_writer_and_reader(
            writer_schema,
            reader_schema,
            self.utf8_view,
            self.strict_mode,
        )?;
        RecordDecoder::try_new_with_options(resolved.data_type(), self.utf8_view)
    }

    /// Produce a `RecordBatch` if at least one row is fully decoded, returning
    /// `Ok(None)` if no new rows are available.
    pub fn flush(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        if self.remaining_capacity == self.batch_size {
            return Ok(None);
        }
        let batch = self.active_decoder.flush()?;
        self.remaining_capacity = self.batch_size;
        // Apply any staged schema switch.
        if let Some((new_fingerprint, new_decoder)) = self.pending_schema.take() {
            if let Some(old_fingerprint) = self.active_fingerprint.replace(new_fingerprint) {
                let old_decoder = std::mem::replace(&mut self.active_decoder, new_decoder);
                #[cfg(feature = "lru")]
                self.cache.put(old_fingerprint, old_decoder);
                #[cfg(not(feature = "lru"))]
                self.cache.shift_remove(&old_fingerprint);
                #[cfg(not(feature = "lru"))]
                self.cache.insert(old_fingerprint, old_decoder);
                #[cfg(not(feature = "lru"))]
                if self.cache.len() > self.max_cache_size {
                    self.cache.shift_remove_index(0);
                }
            } else {
                self.active_decoder = new_decoder;
            }
        }
        Ok(Some(batch))
    }

    /// Returns the number of rows that can be added to this decoder before it is full.
    pub fn capacity(&self) -> usize {
        self.remaining_capacity
    }

    /// Returns true if the decoder has reached its capacity for the current batch.
    pub fn batch_is_full(&self) -> bool {
        self.remaining_capacity == 0
    }
}

/// A builder to create an [`Avro Reader`](Reader) that reads Avro data
/// into Arrow `RecordBatch`.
#[derive(Debug)]
pub struct ReaderBuilder {
    batch_size: usize,
    strict_mode: bool,
    utf8_view: bool,
    reader_schema: Option<AvroSchema<'static>>,
    writer_schema_store: Option<SchemaStore<'static>>,
    active_fingerprint: Option<Fingerprint>,
    decoder_cache_size: usize,
}

impl Default for ReaderBuilder {
    fn default() -> Self {
        Self {
            batch_size: 1024,
            strict_mode: false,
            utf8_view: false,
            reader_schema: None,
            writer_schema_store: None,
            active_fingerprint: None,
            decoder_cache_size: 20,
        }
    }
}

impl ReaderBuilder {
    /// Creates a new [`ReaderBuilder`] with default settings:
    /// - `batch_size` = 1024
    /// - `strict_mode` = false
    /// - `utf8_view` = false
    /// - `reader_schema` = None
    /// - `writer_schema_store` = None
    /// - `active_fingerprint` = None
    pub fn new() -> Self {
        Self::default()
    }

    fn make_record_decoder<'a>(
        &self,
        writer_schema: &AvroSchema<'a>,
        reader_schema: Option<&AvroSchema<'a>>,
    ) -> Result<RecordDecoder, ArrowError> {
        let root = match reader_schema {
            Some(reader_schema) if !compare_schemas(writer_schema, reader_schema)? => {
                AvroFieldBuilder::new(writer_schema).with_reader_schema(reader_schema)
            }
            _ => AvroFieldBuilder::new(writer_schema),
        }
        .with_utf8view(self.utf8_view)
        .with_strict_mode(self.strict_mode)
        .build()?;
        RecordDecoder::try_new_with_options(root.data_type(), self.utf8_view)
    }

    fn make_decoder_with_parts(
        &self,
        active_decoder: RecordDecoder,
        active_fingerprint: Option<Fingerprint>,
        reader_schema: Option<AvroSchema<'static>>,
        writer_schema_store: Option<SchemaStore<'static>>,
    ) -> Decoder {
        #[cfg(feature = "lru")]
        let capacity = NonZeroUsize::new(self.decoder_cache_size).unwrap_or(NonZeroUsize::MIN);
        Decoder {
            batch_size: self.batch_size,
            remaining_capacity: self.batch_size,
            active_fingerprint,
            active_decoder,
            #[cfg(feature = "lru")]
            cache: LruCache::new(capacity),
            #[cfg(not(feature = "lru"))]
            cache: IndexMap::new(),
            max_cache_size: self.decoder_cache_size,
            reader_schema,
            utf8_view: self.utf8_view,
            writer_schema_store,
            strict_mode: self.strict_mode,
            pending_schema: None,
        }
    }

    fn make_decoder(&self, header: Option<&Header>) -> Result<Decoder, ArrowError> {
        if let Some(hdr) = header {
            let writer_schema = hdr
                .schema()
                .map_err(|e| ArrowError::ExternalError(Box::new(e)))?
                .ok_or_else(|| {
                    ArrowError::ParseError("No Avro schema present in file header".into())
                })?;
            let record_decoder =
                self.make_record_decoder(&writer_schema, self.reader_schema.as_ref())?;
            return Ok(self.make_decoder_with_parts(record_decoder, None, None, None));
        }
        let writer_schema_store = self.writer_schema_store.as_ref().ok_or_else(|| {
            ArrowError::ParseError("Writer schema store required for raw Avro".into())
        })?;
        let fingerprint = self
            .active_fingerprint
            .or_else(|| writer_schema_store.fingerprints().into_iter().next())
            .ok_or_else(|| {
                ArrowError::ParseError(
                    "Writer schema store must contain at least one schema".into(),
                )
            })?;
        let writer_schema = writer_schema_store.lookup(&fingerprint).ok_or_else(|| {
            ArrowError::ParseError("Active fingerprint not found in schema store".into())
        })?;
        let record_decoder =
            self.make_record_decoder(writer_schema, self.reader_schema.as_ref())?;
        Ok(self.make_decoder_with_parts(
            record_decoder,
            Some(fingerprint),
            self.reader_schema.clone(),
            self.writer_schema_store.clone(),
        ))
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

    /// Sets the Avro reader schema.
    ///
    /// If a schema is not provided, the schema will be read from the Avro file header.
    pub fn with_reader_schema(mut self, reader_schema: AvroSchema<'static>) -> Self {
        self.reader_schema = Some(reader_schema);
        self
    }

    /// Sets the `SchemaStore` used for resolving writer schemas.
    ///
    /// This is necessary when decoding single-object encoded data that identifies
    /// schemas by a fingerprint. The store allows the decoder to look up the
    /// full writer schema from a fingerprint embedded in the data.
    ///
    /// Defaults to `None`.
    pub fn with_writer_schema_store(mut self, store: SchemaStore<'static>) -> Self {
        self.writer_schema_store = Some(store);
        self
    }

    /// Sets the initial schema fingerprint for decoding single-object encoded data.
    ///
    /// This is useful when the data stream does not begin with a schema definition
    /// or fingerprint, allowing the decoder to start with a known schema from the
    /// `SchemaStore`.
    ///
    /// Defaults to `None`.
    pub fn with_active_fingerprint(mut self, fp: Fingerprint) -> Self {
        self.active_fingerprint = Some(fp);
        self
    }

    /// Set the maximum number of decoders to cache.
    ///
    /// When dealing with Avro files that contain multiple schemas, we may need to switch
    /// between different decoders. This cache avoids rebuilding them from scratch every time.
    ///
    /// Defaults to `20`.
    pub fn with_max_decoder_cache_size(mut self, n: usize) -> Self {
        self.decoder_cache_size = n;
        self
    }

    /// Create a [`Reader`] from this builder and a `BufRead`
    pub fn build<R: BufRead>(self, mut reader: R) -> Result<Reader<R>, ArrowError> {
        let header = read_header(&mut reader)?;
        let decoder = self.make_decoder(Some(&header))?;
        Ok(Reader {
            reader,
            header,
            decoder,
            block_decoder: BlockDecoder::default(),
            block_data: Vec::new(),
            block_cursor: 0,
            finished: false,
        })
    }

    /// Create a [`Decoder`] from this builder.
    pub fn build_decoder(self) -> Result<Decoder, ArrowError> {
        if self.writer_schema_store.is_none() {
            return Err(ArrowError::InvalidArgumentError(
                "Building a decoder requires a writer schema store".to_string(),
            ));
        }
        self.make_decoder(None)
    }
}

/// A high-level Avro `Reader` that reads container-file blocks
/// and feeds them into a row-level [`Decoder`].
#[derive(Debug)]
pub struct Reader<R: BufRead> {
    reader: R,
    header: Header,
    decoder: Decoder,
    block_decoder: BlockDecoder,
    block_data: Vec<u8>,
    block_cursor: usize,
    finished: bool,
}

impl<R: BufRead> Reader<R> {
    /// Return the Arrow schema discovered from the Avro file header
    pub fn schema(&self) -> SchemaRef {
        self.decoder.schema()
    }

    /// Return the Avro container-file header
    pub fn avro_header(&self) -> &Header {
        &self.header
    }

    /// Reads the next [`RecordBatch`] from the Avro file or `Ok(None)` on EOF
    fn read(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        'outer: while !self.finished && !self.decoder.batch_is_full() {
            while self.block_cursor == self.block_data.len() {
                let buf = self.reader.fill_buf()?;
                if buf.is_empty() {
                    self.finished = true;
                    break 'outer;
                }
                // Try to decode another block from the buffered reader.
                let consumed = self.block_decoder.decode(buf)?;
                self.reader.consume(consumed);
                if let Some(block) = self.block_decoder.flush() {
                    // Successfully decoded a block.
                    let block_data = if let Some(ref codec) = self.header.compression()? {
                        codec.decompress(&block.data)?
                    } else {
                        block.data
                    };
                    self.block_data = block_data;
                    self.block_cursor = 0;
                } else if consumed == 0 {
                    // The block decoder made no progress on a non-empty buffer.
                    return Err(ArrowError::ParseError(
                        "Could not decode next Avro block from partial data".to_string(),
                    ));
                }
            }
            // Try to decode more rows from the current block.
            let consumed = self.decoder.decode(&self.block_data[self.block_cursor..])?;
            self.block_cursor += consumed;
        }
        self.decoder.flush()
    }
}

impl<R: BufRead> Iterator for Reader<R> {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.read().transpose()
    }
}

impl<R: BufRead> RecordBatchReader for Reader<R> {
    fn schema(&self) -> SchemaRef {
        self.schema()
    }
}

#[cfg(test)]
mod test {
    use crate::codec::{AvroDataType, AvroField, AvroFieldBuilder, Codec};
    use crate::compression::CompressionCodec;
    use crate::reader::record::RecordDecoder;
    use crate::reader::vlq::VLQDecoder;
    use crate::reader::{read_header, Decoder, Reader, ReaderBuilder};
    use crate::schema::{
        generate_fingerprint_rabin, ComplexType, Field as AvroFieldDef, Fingerprint,
        FingerprintAlgorithm, PrimitiveType, Record, Schema as AvroSchema, SchemaStore, TypeName,
        SINGLE_OBJECT_MAGIC,
    };
    use crate::test_util::arrow_test_data;
    use arrow::array::ArrayDataBuilder;
    use arrow_array::builder::{
        ArrayBuilder, BooleanBuilder, Float32Builder, Float64Builder, Int32Builder, Int64Builder,
        ListBuilder, MapBuilder, StringBuilder, StructBuilder,
    };
    use arrow_array::types::{Int32Type, IntervalMonthDayNanoType};
    use arrow_array::*;
    use arrow_buffer::{Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
    use arrow_schema::{ArrowError, DataType, Field, Fields, IntervalUnit, Schema};
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

    fn read_file_strict(
        path: &str,
        batch_size: usize,
        utf8_view: bool,
    ) -> Result<Reader<BufReader<File>>, ArrowError> {
        let file = File::open(path)?;
        ReaderBuilder::new()
            .with_batch_size(batch_size)
            .with_utf8_view(utf8_view)
            .with_strict_mode(true)
            .build(BufReader::new(file))
    }

    fn decode_stream<S: Stream<Item = Bytes> + Unpin>(
        mut decoder: Decoder,
        mut input: S,
    ) -> impl Stream<Item = Result<RecordBatch, ArrowError>> {
        async_stream::try_stream! {
            if let Some(data) = input.next().await {
                let consumed = decoder.decode(&data)?;
                if consumed < data.len() {
                    Err(ArrowError::ParseError(
                        "did not consume all bytes".to_string(),
                    ))?;
                }
            }
            if let Some(batch) = decoder.flush()? {
                yield batch
            }
        }
    }

    fn schema_from_json(js: &str) -> AvroSchema<'static> {
        let static_js = Box::leak(js.to_string().into_boxed_str());
        serde_json::from_str(static_js).expect("valid Avro schema JSON")
    }

    fn make_record_schema(pt: PrimitiveType) -> AvroSchema<'static> {
        AvroSchema::Complex(ComplexType::Record(Record {
            name: "TestRecord",
            namespace: None,
            doc: None,
            aliases: vec![],
            fields: vec![AvroFieldDef {
                name: "a",
                doc: None,
                r#type: AvroSchema::TypeName(TypeName::Primitive(pt)),
                default: None,
            }],
            attributes: Default::default(),
        }))
    }

    fn make_two_schema_store() -> (
        SchemaStore<'static>,
        Fingerprint,
        Fingerprint,
        AvroSchema<'static>,
        AvroSchema<'static>,
    ) {
        let schema_int = make_record_schema(PrimitiveType::Int);
        let schema_long = make_record_schema(PrimitiveType::Long);
        let mut store = SchemaStore::new();
        let fp_int = store
            .register(schema_int.clone())
            .expect("register int schema");
        let fp_long = store
            .register(schema_long.clone())
            .expect("register long schema");
        (store, fp_int, fp_long, schema_int, schema_long)
    }

    fn make_prefix(fp: Fingerprint) -> Vec<u8> {
        match fp {
            Fingerprint::Rabin(v) => {
                let mut out = Vec::with_capacity(10);
                out.extend_from_slice(&SINGLE_OBJECT_MAGIC);
                out.extend_from_slice(&v.to_le_bytes());
                out
            }
            _ => panic!("Only Rabin fingerprints are used in unit‑tests"),
        }
    }

    fn make_decoder(
        store: &SchemaStore<'static>,
        fp: Fingerprint,
        schema: &AvroSchema<'static>,
    ) -> Decoder {
        ReaderBuilder::new()
            .with_batch_size(8)
            .with_reader_schema(schema.clone())
            .with_writer_schema_store(store.clone())
            .with_active_fingerprint(fp)
            .build_decoder()
            .expect("decoder")
    }

    #[test]
    fn test_schema_store_register_lookup() {
        let schema_int = make_record_schema(PrimitiveType::Int);
        let schema_long = make_record_schema(PrimitiveType::Long);
        let mut store = SchemaStore::new();
        let fp_int = store
            .register(schema_int.clone())
            .expect("register int schema");
        let fp_long = store
            .register(schema_long.clone())
            .expect("register long schema");
        assert_eq!(store.lookup(&fp_int).cloned(), Some(schema_int));
        assert_eq!(store.lookup(&fp_long).cloned(), Some(schema_long));
        assert_eq!(store.fingerprint_algorithm(), FingerprintAlgorithm::Rabin);
    }

    #[test]
    fn test_unknown_fingerprint_is_error() {
        let (mut store, fp_int, _fp_long, schema_int, _schema_long) = make_two_schema_store();
        {
            let mut new_store = SchemaStore::new();
            new_store
                .register(schema_int.clone())
                .expect("register int schema");
            store = new_store;
        }
        let unknown_fp = Fingerprint::Rabin(0xDEADBEEFDEADBEEF);
        let prefix = make_prefix(unknown_fp);
        let mut decoder = ReaderBuilder::new()
            .with_batch_size(8)
            .with_reader_schema(schema_int.clone())
            .with_writer_schema_store(store.clone())
            .with_active_fingerprint(fp_int)
            .build_decoder()
            .expect("build decoder");
        let err = decoder.decode(&prefix).expect_err("decode should error");
        let msg = format!("{err}");
        assert!(
            msg.contains("Unknown fingerprint"),
            "unexpected error message: {msg}"
        );
    }

    #[test]
    fn test_missing_initial_fingerprint_error() {
        let (store, _fp_int, _fp_long, schema_int, _schema_long) = make_two_schema_store();
        let mut decoder = ReaderBuilder::new()
            .with_batch_size(8)
            .with_reader_schema(schema_int.clone())
            .with_writer_schema_store(store)
            .build_decoder()
            .expect("build decoder");
        let buf = [0x02u8, 0x00u8];
        let err = decoder.decode(&buf).expect_err("decode should error");
        let msg = format!("{err}");
        assert!(
            msg.contains("Expected single‑object encoding fingerprint"),
            "unexpected error message: {msg}"
        );
    }

    #[test]
    fn test_handle_prefix_no_schema_store() {
        let (store, fp_int, _fp_long, schema_int, _schema_long) = make_two_schema_store();
        let mut decoder = make_decoder(&store, fp_int, &schema_int);
        decoder.writer_schema_store = None;
        let res = decoder
            .handle_prefix(&SINGLE_OBJECT_MAGIC[..], FingerprintAlgorithm::Rabin)
            .expect("handle_prefix");
        assert!(
            res.is_none(),
            "Expected None when writer_schema_store is None"
        );
    }

    #[test]
    fn test_handle_prefix_incomplete_magic() {
        let (store, fp_int, _fp_long, schema_int, _schema_long) = make_two_schema_store();
        let mut decoder = make_decoder(&store, fp_int, &schema_int);
        let buf = &SINGLE_OBJECT_MAGIC[..1];
        let res = decoder
            .handle_prefix(buf, FingerprintAlgorithm::Rabin)
            .expect("handle_prefix");
        assert_eq!(res, Some(0), "Expected Some(0) for incomplete magic");
        assert!(
            decoder.pending_schema.is_none(),
            "No schema switch should be staged"
        );
    }

    #[test]
    fn test_handle_prefix_magic_mismatch() {
        let (store, fp_int, _fp_long, schema_int, _schema_long) = make_two_schema_store();
        let mut decoder = make_decoder(&store, fp_int, &schema_int);
        let buf = [0xFFu8, 0xFFu8, 0xABu8];
        let res = decoder
            .handle_prefix(&buf, FingerprintAlgorithm::Rabin)
            .expect("handle_prefix");
        assert!(res.is_none(), "Expected None when magic bytes do not match");
    }

    #[test]
    fn test_handle_prefix_incomplete_fingerprint() {
        let (store, fp_int, fp_long, schema_int, _schema_long) = make_two_schema_store();
        let mut decoder = make_decoder(&store, fp_int, &schema_int);
        let long_bytes = match fp_long {
            Fingerprint::Rabin(v) => v.to_le_bytes(),
            _ => unreachable!("only Rabin fingerprints are used in tests"),
        };
        let mut buf = Vec::from(SINGLE_OBJECT_MAGIC);
        buf.extend_from_slice(&long_bytes[..4]);
        let res = decoder
            .handle_prefix(&buf, FingerprintAlgorithm::Rabin)
            .expect("handle_prefix");
        assert_eq!(
            res,
            Some(0),
            "Expected Some(0) when fingerprint bytes are incomplete"
        );
        assert!(
            decoder.pending_schema.is_none(),
            "No schema switch should be staged yet"
        );
    }

    #[test]
    fn test_handle_prefix_valid_prefix_switches_schema() {
        let (store, fp_int, fp_long, schema_int, schema_long) = make_two_schema_store();
        let mut decoder = make_decoder(&store, fp_int, &schema_int);
        let root_long = AvroFieldBuilder::new(&schema_long)
            .build()
            .expect("root_long");
        let long_decoder =
            RecordDecoder::try_new_with_options(root_long.data_type(), decoder.utf8_view)
                .expect("long_decoder");
        #[cfg(feature = "lru")]
        let _ = decoder.cache.put(fp_long, long_decoder);
        #[cfg(not(feature = "lru"))]
        let _ = decoder.cache.insert(fp_long, long_decoder);
        let mut buf = Vec::from(SINGLE_OBJECT_MAGIC);
        let Fingerprint::Rabin(v) = fp_long;
        buf.extend_from_slice(&v.to_le_bytes());
        let consumed = decoder
            .handle_prefix(&buf, FingerprintAlgorithm::Rabin)
            .expect("handle_prefix")
            .expect("Some");
        assert_eq!(consumed, buf.len(), "Should consume the full prefix");
        assert!(
            decoder.pending_schema.is_some(),
            "A schema switch should be staged"
        );
        let (pending_fp, _) = decoder.pending_schema.as_ref().unwrap();
        assert_eq!(
            *pending_fp, fp_long,
            "The staged fingerprint should match the new schema"
        );
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
    fn test_read_zero_byte_avro_file() {
        let batch = read_file("test/data/zero_byte.avro", 3, false);
        let schema = batch.schema();
        assert_eq!(schema.fields().len(), 1);
        let field = schema.field(0);
        assert_eq!(field.name(), "data");
        assert_eq!(field.data_type(), &DataType::Binary);
        assert!(field.is_nullable());
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 1);
        let binary_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert!(binary_array.is_null(0));
        assert!(binary_array.is_valid(1));
        assert_eq!(binary_array.value(1), b"");
        assert!(binary_array.is_valid(2));
        assert_eq!(binary_array.value(2), b"some bytes");
    }

    #[test]
    fn test_alltypes() {
        let files = [
            "avro/alltypes_plain.avro",
            "avro/alltypes_plain.snappy.avro",
            "avro/alltypes_plain.zstandard.avro",
            "avro/alltypes_plain.bzip2.avro",
            "avro/alltypes_plain.xz.avro",
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
    fn test_alltypes_dictionary() {
        let file = "avro/alltypes_dictionary.avro";
        let expected = RecordBatch::try_from_iter_with_nullable([
            ("id", Arc::new(Int32Array::from(vec![0, 1])) as _, true),
            (
                "bool_col",
                Arc::new(BooleanArray::from(vec![Some(true), Some(false)])) as _,
                true,
            ),
            (
                "tinyint_col",
                Arc::new(Int32Array::from(vec![0, 1])) as _,
                true,
            ),
            (
                "smallint_col",
                Arc::new(Int32Array::from(vec![0, 1])) as _,
                true,
            ),
            ("int_col", Arc::new(Int32Array::from(vec![0, 1])) as _, true),
            (
                "bigint_col",
                Arc::new(Int64Array::from(vec![0, 10])) as _,
                true,
            ),
            (
                "float_col",
                Arc::new(Float32Array::from(vec![0.0, 1.1])) as _,
                true,
            ),
            (
                "double_col",
                Arc::new(Float64Array::from(vec![0.0, 10.1])) as _,
                true,
            ),
            (
                "date_string_col",
                Arc::new(BinaryArray::from_iter_values([b"01/01/09", b"01/01/09"])) as _,
                true,
            ),
            (
                "string_col",
                Arc::new(BinaryArray::from_iter_values([b"0", b"1"])) as _,
                true,
            ),
            (
                "timestamp_col",
                Arc::new(
                    TimestampMicrosecondArray::from_iter_values([
                        1230768000000000, // 2009-01-01T00:00:00.000
                        1230768060000000, // 2009-01-01T00:01:00.000
                    ])
                    .with_timezone("+00:00"),
                ) as _,
                true,
            ),
        ])
        .unwrap();
        let file_path = arrow_test_data(file);
        let batch_large = read_file(&file_path, 8, false);
        assert_eq!(
            batch_large, expected,
            "Decoded RecordBatch does not match for file {file}"
        );
        let batch_small = read_file(&file_path, 3, false);
        assert_eq!(
            batch_small, expected,
            "Decoded RecordBatch (batch size 3) does not match for file {file}"
        );
    }

    #[test]
    fn test_alltypes_nulls_plain() {
        let file = "avro/alltypes_nulls_plain.avro";
        let expected = RecordBatch::try_from_iter_with_nullable([
            (
                "string_col",
                Arc::new(StringArray::from(vec![None::<&str>])) as _,
                true,
            ),
            ("int_col", Arc::new(Int32Array::from(vec![None])) as _, true),
            (
                "bool_col",
                Arc::new(BooleanArray::from(vec![None])) as _,
                true,
            ),
            (
                "bigint_col",
                Arc::new(Int64Array::from(vec![None])) as _,
                true,
            ),
            (
                "float_col",
                Arc::new(Float32Array::from(vec![None])) as _,
                true,
            ),
            (
                "double_col",
                Arc::new(Float64Array::from(vec![None])) as _,
                true,
            ),
            (
                "bytes_col",
                Arc::new(BinaryArray::from(vec![None::<&[u8]>])) as _,
                true,
            ),
        ])
        .unwrap();
        let file_path = arrow_test_data(file);
        let batch_large = read_file(&file_path, 8, false);
        assert_eq!(
            batch_large, expected,
            "Decoded RecordBatch does not match for file {file}"
        );
        let batch_small = read_file(&file_path, 3, false);
        assert_eq!(
            batch_small, expected,
            "Decoded RecordBatch (batch size 3) does not match for file {file}"
        );
    }

    #[test]
    fn test_binary() {
        let file = arrow_test_data("avro/binary.avro");
        let batch = read_file(&file, 8, false);
        let expected = RecordBatch::try_from_iter_with_nullable([(
            "foo",
            Arc::new(BinaryArray::from_iter_values(vec![
                b"\x00".as_ref(),
                b"\x01".as_ref(),
                b"\x02".as_ref(),
                b"\x03".as_ref(),
                b"\x04".as_ref(),
                b"\x05".as_ref(),
                b"\x06".as_ref(),
                b"\x07".as_ref(),
                b"\x08".as_ref(),
                b"\t".as_ref(),
                b"\n".as_ref(),
                b"\x0b".as_ref(),
            ])) as Arc<dyn Array>,
            true,
        )])
        .unwrap();
        assert_eq!(batch, expected);
    }

    #[test]
    fn test_decode_stream_with_schema() {
        struct TestCase<'a> {
            name: &'a str,
            schema: &'a str,
            expected_error: Option<&'a str>,
        }
        let tests = vec![
            TestCase {
                name: "success",
                schema: r#"{"type":"record","name":"test","fields":[{"name":"f2","type":"string"}]}"#,
                expected_error: None,
            },
            TestCase {
                name: "valid schema invalid data",
                schema: r#"{"type":"record","name":"test","fields":[{"name":"f2","type":"long"}]}"#,
                expected_error: Some("did not consume all bytes"),
            },
        ];
        for test in tests {
            let writer_schema: crate::schema::Schema =
                serde_json::from_str(test.schema).expect("valid Avro schema JSON");
            let mut store = SchemaStore::new();
            let fp = store
                .register(writer_schema.clone())
                .expect("register schema and get fingerprint");
            let prefix = make_prefix(fp); // magic + fp
            let schema_s2: crate::schema::Schema = serde_json::from_str(test.schema).unwrap();
            let record_val = "some_string";
            let mut body = prefix;
            body.push((record_val.len() as u8) << 1);
            body.extend_from_slice(record_val.as_bytes());
            let decoder_result = ReaderBuilder::new()
                .with_batch_size(1)
                .with_writer_schema_store(store)
                .with_active_fingerprint(fp)
                .build_decoder();
            let decoder = match decoder_result {
                Ok(decoder) => decoder,
                Err(e) => {
                    if let Some(expected) = test.expected_error {
                        assert!(
                            e.to_string().contains(expected),
                            "Test '{}' failed: unexpected error message at build.\nExpected to contain: '{expected}'\nActual: '{e}'",
                            test.name,
                        );
                        continue;
                    } else {
                        panic!("Test '{}' failed at decoder build: {e}", test.name);
                    }
                }
            };
            let stream = Box::pin(stream::once(async { Bytes::from(body) }));
            let decoded_stream = decode_stream(decoder, stream);
            let batches_result: Result<Vec<RecordBatch>, ArrowError> =
                block_on(decoded_stream.try_collect());
            match (batches_result, test.expected_error) {
                (Ok(batches), None) => {
                    let batch =
                        arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap();
                    let expected_field = Field::new("f2", DataType::Utf8, false);
                    let expected_schema = Arc::new(Schema::new(vec![expected_field]));
                    let expected_array = Arc::new(StringArray::from(vec![record_val]));
                    let expected_batch =
                        RecordBatch::try_new(expected_schema, vec![expected_array]).unwrap();
                    assert_eq!(batch, expected_batch, "Test '{}' failed", test.name);
                    assert_eq!(
                        batch.schema().field(0).name(),
                        "f2",
                        "Test '{}' failed",
                        test.name
                    );
                }
                (Err(e), Some(expected)) => {
                    assert!(
                        e.to_string().contains(expected),
                        "Test '{}' failed: unexpected error message at decode.\nExpected to contain: '{expected}'\nActual: '{e}'",
                        test.name,
                    );
                }
                (Ok(batches), Some(expected)) => {
                    panic!(
                        "Test '{}' was expected to fail with '{expected}', but it succeeded with: {:?}",
                        test.name, batches
                    );
                }
                (Err(e), None) => {
                    panic!(
                        "Test '{}' was not expected to fail, but it did with '{e}'",
                        test.name
                    );
                }
            }
        }
    }

    #[test]
    fn test_decimal() {
        let files = [
            ("avro/fixed_length_decimal.avro", 25, 2),
            ("avro/fixed_length_decimal_legacy.avro", 13, 2),
            ("avro/int32_decimal.avro", 4, 2),
            ("avro/int64_decimal.avro", 10, 2),
        ];
        let decimal_values: Vec<i128> = (1..=24).map(|n| n as i128 * 100).collect();
        for (file, precision, scale) in files {
            let file_path = arrow_test_data(file);
            let actual_batch = read_file(&file_path, 8, false);
            let expected_array = Decimal128Array::from_iter_values(decimal_values.clone())
                .with_precision_and_scale(precision, scale)
                .unwrap();
            let mut meta = HashMap::new();
            meta.insert("precision".to_string(), precision.to_string());
            meta.insert("scale".to_string(), scale.to_string());
            let field_with_meta = Field::new("value", DataType::Decimal128(precision, scale), true)
                .with_metadata(meta);
            let expected_schema = Arc::new(Schema::new(vec![field_with_meta]));
            let expected_batch =
                RecordBatch::try_new(expected_schema.clone(), vec![Arc::new(expected_array)])
                    .expect("Failed to build expected RecordBatch");
            assert_eq!(
                actual_batch, expected_batch,
                "Decoded RecordBatch does not match the expected Decimal128 data for file {file}"
            );
            let actual_batch_small = read_file(&file_path, 3, false);
            assert_eq!(
                actual_batch_small,
                expected_batch,
                "Decoded RecordBatch does not match the expected Decimal128 data for file {file} with batch size 3"
            );
        }
    }

    #[test]
    fn test_dict_pages_offset_zero() {
        let file = arrow_test_data("avro/dict-page-offset-zero.avro");
        let batch = read_file(&file, 32, false);
        let num_rows = batch.num_rows();
        let expected_field = Int32Array::from(vec![Some(1552); num_rows]);
        let expected = RecordBatch::try_from_iter_with_nullable([(
            "l_partkey",
            Arc::new(expected_field) as Arc<dyn Array>,
            true,
        )])
        .unwrap();
        assert_eq!(batch, expected);
    }

    #[test]
    fn test_list_columns() {
        let file = arrow_test_data("avro/list_columns.avro");
        let mut int64_list_builder = ListBuilder::new(Int64Builder::new());
        {
            {
                let values = int64_list_builder.values();
                values.append_value(1);
                values.append_value(2);
                values.append_value(3);
            }
            int64_list_builder.append(true);
        }
        {
            {
                let values = int64_list_builder.values();
                values.append_null();
                values.append_value(1);
            }
            int64_list_builder.append(true);
        }
        {
            {
                let values = int64_list_builder.values();
                values.append_value(4);
            }
            int64_list_builder.append(true);
        }
        let int64_list = int64_list_builder.finish();
        let mut utf8_list_builder = ListBuilder::new(StringBuilder::new());
        {
            {
                let values = utf8_list_builder.values();
                values.append_value("abc");
                values.append_value("efg");
                values.append_value("hij");
            }
            utf8_list_builder.append(true);
        }
        {
            utf8_list_builder.append(false);
        }
        {
            {
                let values = utf8_list_builder.values();
                values.append_value("efg");
                values.append_null();
                values.append_value("hij");
                values.append_value("xyz");
            }
            utf8_list_builder.append(true);
        }
        let utf8_list = utf8_list_builder.finish();
        let expected = RecordBatch::try_from_iter_with_nullable([
            ("int64_list", Arc::new(int64_list) as Arc<dyn Array>, true),
            ("utf8_list", Arc::new(utf8_list) as Arc<dyn Array>, true),
        ])
        .unwrap();
        let batch = read_file(&file, 8, false);
        assert_eq!(batch, expected);
    }

    #[test]
    fn test_nested_lists() {
        use arrow_data::ArrayDataBuilder;
        let file = arrow_test_data("avro/nested_lists.snappy.avro");
        let inner_values = StringArray::from(vec![
            Some("a"),
            Some("b"),
            Some("c"),
            Some("d"),
            Some("a"),
            Some("b"),
            Some("c"),
            Some("d"),
            Some("e"),
            Some("a"),
            Some("b"),
            Some("c"),
            Some("d"),
            Some("e"),
            Some("f"),
        ]);
        let inner_offsets = Buffer::from_slice_ref([0, 2, 3, 3, 4, 6, 8, 8, 9, 11, 13, 14, 14, 15]);
        let inner_validity = [
            true, true, false, true, true, true, false, true, true, true, true, false, true,
        ];
        let inner_null_buffer = Buffer::from_iter(inner_validity.iter().copied());
        let inner_field = Field::new("item", DataType::Utf8, true);
        let inner_list_data = ArrayDataBuilder::new(DataType::List(Arc::new(inner_field)))
            .len(13)
            .add_buffer(inner_offsets)
            .add_child_data(inner_values.to_data())
            .null_bit_buffer(Some(inner_null_buffer))
            .build()
            .unwrap();
        let inner_list_array = ListArray::from(inner_list_data);
        let middle_offsets = Buffer::from_slice_ref([0, 2, 4, 6, 8, 11, 13]);
        let middle_validity = [true; 6];
        let middle_null_buffer = Buffer::from_iter(middle_validity.iter().copied());
        let middle_field = Field::new("item", inner_list_array.data_type().clone(), true);
        let middle_list_data = ArrayDataBuilder::new(DataType::List(Arc::new(middle_field)))
            .len(6)
            .add_buffer(middle_offsets)
            .add_child_data(inner_list_array.to_data())
            .null_bit_buffer(Some(middle_null_buffer))
            .build()
            .unwrap();
        let middle_list_array = ListArray::from(middle_list_data);
        let outer_offsets = Buffer::from_slice_ref([0, 2, 4, 6]);
        let outer_null_buffer = Buffer::from_slice_ref([0b111]); // all 3 rows valid
        let outer_field = Field::new("item", middle_list_array.data_type().clone(), true);
        let outer_list_data = ArrayDataBuilder::new(DataType::List(Arc::new(outer_field)))
            .len(3)
            .add_buffer(outer_offsets)
            .add_child_data(middle_list_array.to_data())
            .null_bit_buffer(Some(outer_null_buffer))
            .build()
            .unwrap();
        let a_expected = ListArray::from(outer_list_data);
        let b_expected = Int32Array::from(vec![1, 1, 1]);
        let expected = RecordBatch::try_from_iter_with_nullable([
            ("a", Arc::new(a_expected) as Arc<dyn Array>, true),
            ("b", Arc::new(b_expected) as Arc<dyn Array>, true),
        ])
        .unwrap();
        let left = read_file(&file, 8, false);
        assert_eq!(left, expected, "Mismatch for batch size=8");
        let left_small = read_file(&file, 3, false);
        assert_eq!(left_small, expected, "Mismatch for batch size=3");
    }

    #[test]
    fn test_simple() {
        let tests = [
            ("avro/simple_enum.avro", 4, build_expected_enum(), 2),
            ("avro/simple_fixed.avro", 2, build_expected_fixed(), 1),
        ];

        fn build_expected_enum() -> RecordBatch {
            // Build the DictionaryArrays for f1, f2, f3
            let keys_f1 = Int32Array::from(vec![0, 1, 2, 3]);
            let vals_f1 = StringArray::from(vec!["a", "b", "c", "d"]);
            let f1_dict =
                DictionaryArray::<Int32Type>::try_new(keys_f1, Arc::new(vals_f1)).unwrap();
            let keys_f2 = Int32Array::from(vec![2, 3, 0, 1]);
            let vals_f2 = StringArray::from(vec!["e", "f", "g", "h"]);
            let f2_dict =
                DictionaryArray::<Int32Type>::try_new(keys_f2, Arc::new(vals_f2)).unwrap();
            let keys_f3 = Int32Array::from(vec![Some(1), Some(2), None, Some(0)]);
            let vals_f3 = StringArray::from(vec!["i", "j", "k"]);
            let f3_dict =
                DictionaryArray::<Int32Type>::try_new(keys_f3, Arc::new(vals_f3)).unwrap();
            let dict_type =
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
            let mut md_f1 = HashMap::new();
            md_f1.insert(
                "avro.enum.symbols".to_string(),
                r#"["a","b","c","d"]"#.to_string(),
            );
            let f1_field = Field::new("f1", dict_type.clone(), false).with_metadata(md_f1);
            let mut md_f2 = HashMap::new();
            md_f2.insert(
                "avro.enum.symbols".to_string(),
                r#"["e","f","g","h"]"#.to_string(),
            );
            let f2_field = Field::new("f2", dict_type.clone(), false).with_metadata(md_f2);
            let mut md_f3 = HashMap::new();
            md_f3.insert(
                "avro.enum.symbols".to_string(),
                r#"["i","j","k"]"#.to_string(),
            );
            let f3_field = Field::new("f3", dict_type.clone(), true).with_metadata(md_f3);
            let expected_schema = Arc::new(Schema::new(vec![f1_field, f2_field, f3_field]));
            RecordBatch::try_new(
                expected_schema,
                vec![
                    Arc::new(f1_dict) as Arc<dyn Array>,
                    Arc::new(f2_dict) as Arc<dyn Array>,
                    Arc::new(f3_dict) as Arc<dyn Array>,
                ],
            )
            .unwrap()
        }

        fn build_expected_fixed() -> RecordBatch {
            let f1 =
                FixedSizeBinaryArray::try_from_iter(vec![b"abcde", b"12345"].into_iter()).unwrap();
            let f2 =
                FixedSizeBinaryArray::try_from_iter(vec![b"fghijklmno", b"1234567890"].into_iter())
                    .unwrap();
            let f3 = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                vec![Some(b"ABCDEF" as &[u8]), None].into_iter(),
                6,
            )
            .unwrap();
            let expected_schema = Arc::new(Schema::new(vec![
                Field::new("f1", DataType::FixedSizeBinary(5), false),
                Field::new("f2", DataType::FixedSizeBinary(10), false),
                Field::new("f3", DataType::FixedSizeBinary(6), true),
            ]));
            RecordBatch::try_new(
                expected_schema,
                vec![
                    Arc::new(f1) as Arc<dyn Array>,
                    Arc::new(f2) as Arc<dyn Array>,
                    Arc::new(f3) as Arc<dyn Array>,
                ],
            )
            .unwrap()
        }
        for (file_name, batch_size, expected, alt_batch_size) in tests {
            let file = arrow_test_data(file_name);
            let actual = read_file(&file, batch_size, false);
            assert_eq!(actual, expected);
            let actual2 = read_file(&file, alt_batch_size, false);
            assert_eq!(actual2, expected);
        }
    }

    #[test]
    fn test_single_nan() {
        let file = arrow_test_data("avro/single_nan.avro");
        let actual = read_file(&file, 1, false);
        use arrow_array::Float64Array;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "mycol",
            DataType::Float64,
            true,
        )]));
        let col = Float64Array::from(vec![None]);
        let expected = RecordBatch::try_new(schema, vec![Arc::new(col)]).unwrap();
        assert_eq!(actual, expected);
        let actual2 = read_file(&file, 2, false);
        assert_eq!(actual2, expected);
    }

    #[test]
    fn test_duration_uuid() {
        let batch = read_file("test/data/duration_uuid.avro", 4, false);
        let schema = batch.schema();
        let fields = schema.fields();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].name(), "duration_field");
        assert_eq!(
            fields[0].data_type(),
            &DataType::Interval(IntervalUnit::MonthDayNano)
        );
        assert_eq!(fields[1].name(), "uuid_field");
        assert_eq!(fields[1].data_type(), &DataType::FixedSizeBinary(16));
        assert_eq!(batch.num_rows(), 4);
        assert_eq!(batch.num_columns(), 2);
        let duration_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<IntervalMonthDayNanoArray>()
            .unwrap();
        let expected_duration_array: IntervalMonthDayNanoArray = [
            Some(IntervalMonthDayNanoType::make_value(1, 15, 500_000_000)),
            Some(IntervalMonthDayNanoType::make_value(0, 5, 2_500_000_000)),
            Some(IntervalMonthDayNanoType::make_value(2, 0, 0)),
            Some(IntervalMonthDayNanoType::make_value(12, 31, 999_000_000)),
        ]
        .iter()
        .copied()
        .collect();
        assert_eq!(&expected_duration_array, duration_array);
        let uuid_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let expected_uuid_array = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            [
                Some([
                    0xfe, 0x7b, 0xc3, 0x0b, 0x4c, 0xe8, 0x4c, 0x5e, 0xb6, 0x7c, 0x22, 0x34, 0xa2,
                    0xd3, 0x8e, 0x66,
                ]),
                Some([
                    0xb3, 0x3f, 0x2a, 0xd7, 0x97, 0xb4, 0x4d, 0xe1, 0x8b, 0xfe, 0x94, 0x94, 0x1d,
                    0x60, 0x15, 0x6e,
                ]),
                Some([
                    0x5f, 0x74, 0x92, 0x64, 0x07, 0x4b, 0x40, 0x05, 0x84, 0xbf, 0x11, 0x5e, 0xa8,
                    0x4e, 0xd2, 0x0a,
                ]),
                Some([
                    0x08, 0x26, 0xcc, 0x06, 0xd2, 0xe3, 0x45, 0x99, 0xb4, 0xad, 0xaf, 0x5f, 0xa6,
                    0x90, 0x5c, 0xdb,
                ]),
            ]
            .into_iter(),
            16,
        )
        .unwrap();
        assert_eq!(&expected_uuid_array, uuid_array);
    }

    #[test]
    fn test_datapage_v2() {
        let file = arrow_test_data("avro/datapage_v2.snappy.avro");
        let batch = read_file(&file, 8, false);
        let a = StringArray::from(vec![
            Some("abc"),
            Some("abc"),
            Some("abc"),
            None,
            Some("abc"),
        ]);
        let b = Int32Array::from(vec![Some(1), Some(2), Some(3), Some(4), Some(5)]);
        let c = Float64Array::from(vec![Some(2.0), Some(3.0), Some(4.0), Some(5.0), Some(2.0)]);
        let d = BooleanArray::from(vec![
            Some(true),
            Some(true),
            Some(true),
            Some(false),
            Some(true),
        ]);
        let e_values = Int32Array::from(vec![
            Some(1),
            Some(2),
            Some(3),
            Some(1),
            Some(2),
            Some(3),
            Some(1),
            Some(2),
        ]);
        let e_offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 3, 3, 3, 6, 8]));
        let e_validity = Some(NullBuffer::from(vec![true, false, false, true, true]));
        let field_e = Arc::new(Field::new("item", DataType::Int32, true));
        let e = ListArray::new(field_e, e_offsets, Arc::new(e_values), e_validity);
        let expected = RecordBatch::try_from_iter_with_nullable([
            ("a", Arc::new(a) as Arc<dyn Array>, true),
            ("b", Arc::new(b) as Arc<dyn Array>, true),
            ("c", Arc::new(c) as Arc<dyn Array>, true),
            ("d", Arc::new(d) as Arc<dyn Array>, true),
            ("e", Arc::new(e) as Arc<dyn Array>, true),
        ])
        .unwrap();
        assert_eq!(batch, expected);
    }

    #[test]
    fn test_nested_records() {
        let f1_f1_1 = StringArray::from(vec!["aaa", "bbb"]);
        let f1_f1_2 = Int32Array::from(vec![10, 20]);
        let rounded_pi = (std::f64::consts::PI * 100.0).round() / 100.0;
        let f1_f1_3_1 = Float64Array::from(vec![rounded_pi, rounded_pi]);
        let f1_f1_3 = StructArray::from(vec![(
            Arc::new(Field::new("f1_3_1", DataType::Float64, false)),
            Arc::new(f1_f1_3_1) as Arc<dyn Array>,
        )]);
        let f1_expected = StructArray::from(vec![
            (
                Arc::new(Field::new("f1_1", DataType::Utf8, false)),
                Arc::new(f1_f1_1) as Arc<dyn Array>,
            ),
            (
                Arc::new(Field::new("f1_2", DataType::Int32, false)),
                Arc::new(f1_f1_2) as Arc<dyn Array>,
            ),
            (
                Arc::new(Field::new(
                    "f1_3",
                    DataType::Struct(Fields::from(vec![Field::new(
                        "f1_3_1",
                        DataType::Float64,
                        false,
                    )])),
                    false,
                )),
                Arc::new(f1_f1_3) as Arc<dyn Array>,
            ),
        ]);

        let f2_fields = vec![
            Field::new("f2_1", DataType::Boolean, false),
            Field::new("f2_2", DataType::Float32, false),
        ];
        let f2_struct_builder = StructBuilder::new(
            f2_fields
                .iter()
                .map(|f| Arc::new(f.clone()))
                .collect::<Vec<Arc<Field>>>(),
            vec![
                Box::new(BooleanBuilder::new()) as Box<dyn arrow_array::builder::ArrayBuilder>,
                Box::new(Float32Builder::new()) as Box<dyn arrow_array::builder::ArrayBuilder>,
            ],
        );
        let mut f2_list_builder = ListBuilder::new(f2_struct_builder);
        {
            let struct_builder = f2_list_builder.values();
            struct_builder.append(true);
            {
                let b = struct_builder.field_builder::<BooleanBuilder>(0).unwrap();
                b.append_value(true);
            }
            {
                let b = struct_builder.field_builder::<Float32Builder>(1).unwrap();
                b.append_value(1.2_f32);
            }
            struct_builder.append(true);
            {
                let b = struct_builder.field_builder::<BooleanBuilder>(0).unwrap();
                b.append_value(true);
            }
            {
                let b = struct_builder.field_builder::<Float32Builder>(1).unwrap();
                b.append_value(2.2_f32);
            }
            f2_list_builder.append(true);
        }
        {
            let struct_builder = f2_list_builder.values();
            struct_builder.append(true);
            {
                let b = struct_builder.field_builder::<BooleanBuilder>(0).unwrap();
                b.append_value(false);
            }
            {
                let b = struct_builder.field_builder::<Float32Builder>(1).unwrap();
                b.append_value(10.2_f32);
            }
            f2_list_builder.append(true);
        }

        let list_array_with_nullable_items = f2_list_builder.finish();

        let item_field = Arc::new(Field::new(
            "item",
            list_array_with_nullable_items.values().data_type().clone(),
            false,
        ));
        let list_data_type = DataType::List(item_field);

        let f2_array_data = list_array_with_nullable_items
            .to_data()
            .into_builder()
            .data_type(list_data_type)
            .build()
            .unwrap();
        let f2_expected = ListArray::from(f2_array_data);

        let mut f3_struct_builder = StructBuilder::new(
            vec![Arc::new(Field::new("f3_1", DataType::Utf8, false))],
            vec![Box::new(StringBuilder::new()) as Box<dyn ArrayBuilder>],
        );
        f3_struct_builder.append(true);
        {
            let b = f3_struct_builder.field_builder::<StringBuilder>(0).unwrap();
            b.append_value("xyz");
        }
        f3_struct_builder.append(false);
        {
            let b = f3_struct_builder.field_builder::<StringBuilder>(0).unwrap();
            b.append_null();
        }
        let f3_expected = f3_struct_builder.finish();
        let f4_fields = [Field::new("f4_1", DataType::Int64, false)];
        let f4_struct_builder = StructBuilder::new(
            f4_fields
                .iter()
                .map(|f| Arc::new(f.clone()))
                .collect::<Vec<Arc<Field>>>(),
            vec![Box::new(Int64Builder::new()) as Box<dyn arrow_array::builder::ArrayBuilder>],
        );
        let mut f4_list_builder = ListBuilder::new(f4_struct_builder);
        {
            let struct_builder = f4_list_builder.values();
            struct_builder.append(true);
            {
                let b = struct_builder.field_builder::<Int64Builder>(0).unwrap();
                b.append_value(200);
            }
            struct_builder.append(false);
            {
                let b = struct_builder.field_builder::<Int64Builder>(0).unwrap();
                b.append_null();
            }
            f4_list_builder.append(true);
        }
        {
            let struct_builder = f4_list_builder.values();
            struct_builder.append(false);
            {
                let b = struct_builder.field_builder::<Int64Builder>(0).unwrap();
                b.append_null();
            }
            struct_builder.append(true);
            {
                let b = struct_builder.field_builder::<Int64Builder>(0).unwrap();
                b.append_value(300);
            }
            f4_list_builder.append(true);
        }
        let f4_expected = f4_list_builder.finish();

        let expected = RecordBatch::try_from_iter_with_nullable([
            ("f1", Arc::new(f1_expected) as Arc<dyn Array>, false),
            ("f2", Arc::new(f2_expected) as Arc<dyn Array>, false),
            ("f3", Arc::new(f3_expected) as Arc<dyn Array>, true),
            ("f4", Arc::new(f4_expected) as Arc<dyn Array>, false),
        ])
        .unwrap();

        let file = arrow_test_data("avro/nested_records.avro");
        let batch_large = read_file(&file, 8, false);
        assert_eq!(
            batch_large, expected,
            "Decoded RecordBatch does not match expected data for nested records (batch size 8)"
        );
        let batch_small = read_file(&file, 3, false);
        assert_eq!(
            batch_small, expected,
            "Decoded RecordBatch does not match expected data for nested records (batch size 3)"
        );
    }

    #[test]
    fn test_repeated_no_annotation() {
        let file = arrow_test_data("avro/repeated_no_annotation.avro");
        let batch_large = read_file(&file, 8, false);
        use arrow_array::{Int32Array, Int64Array, ListArray, StringArray, StructArray};
        use arrow_buffer::Buffer;
        use arrow_schema::{DataType, Field, Fields};
        let id_array = Int32Array::from(vec![1, 2, 3, 4, 5, 6]);
        let number_array = Int64Array::from(vec![
            Some(5555555555),
            Some(1111111111),
            Some(1111111111),
            Some(2222222222),
            Some(3333333333),
        ]);
        let kind_array =
            StringArray::from(vec![None, Some("home"), Some("home"), None, Some("mobile")]);
        let phone_fields = Fields::from(vec![
            Field::new("number", DataType::Int64, true),
            Field::new("kind", DataType::Utf8, true),
        ]);
        let phone_struct_data = ArrayDataBuilder::new(DataType::Struct(phone_fields))
            .len(5)
            .child_data(vec![number_array.into_data(), kind_array.into_data()])
            .build()
            .unwrap();
        let phone_struct_array = StructArray::from(phone_struct_data);
        let phone_list_offsets = Buffer::from_slice_ref([0, 0, 0, 0, 1, 2, 5]);
        let phone_list_validity = Buffer::from_iter([false, false, true, true, true, true]);
        let phone_item_field = Field::new("item", phone_struct_array.data_type().clone(), true);
        let phone_list_data = ArrayDataBuilder::new(DataType::List(Arc::new(phone_item_field)))
            .len(6)
            .add_buffer(phone_list_offsets)
            .null_bit_buffer(Some(phone_list_validity))
            .child_data(vec![phone_struct_array.into_data()])
            .build()
            .unwrap();
        let phone_list_array = ListArray::from(phone_list_data);
        let phone_numbers_validity = Buffer::from_iter([false, false, true, true, true, true]);
        let phone_numbers_field = Field::new("phone", phone_list_array.data_type().clone(), true);
        let phone_numbers_struct_data =
            ArrayDataBuilder::new(DataType::Struct(Fields::from(vec![phone_numbers_field])))
                .len(6)
                .null_bit_buffer(Some(phone_numbers_validity))
                .child_data(vec![phone_list_array.into_data()])
                .build()
                .unwrap();
        let phone_numbers_struct_array = StructArray::from(phone_numbers_struct_data);
        let expected = arrow_array::RecordBatch::try_from_iter_with_nullable([
            ("id", Arc::new(id_array) as _, true),
            (
                "phoneNumbers",
                Arc::new(phone_numbers_struct_array) as _,
                true,
            ),
        ])
        .unwrap();
        assert_eq!(batch_large, expected, "Mismatch for batch_size=8");
        let batch_small = read_file(&file, 3, false);
        assert_eq!(batch_small, expected, "Mismatch for batch_size=3");
    }

    #[test]
    fn test_nonnullable_impala() {
        let file = arrow_test_data("avro/nonnullable.impala.avro");
        let id = Int64Array::from(vec![Some(8)]);
        let mut int_array_builder = ListBuilder::new(Int32Builder::new());
        {
            let vb = int_array_builder.values();
            vb.append_value(-1);
        }
        int_array_builder.append(true); // finalize one sub-list
        let int_array = int_array_builder.finish();
        let mut iaa_builder = ListBuilder::new(ListBuilder::new(Int32Builder::new()));
        {
            let inner_list_builder = iaa_builder.values();
            {
                let vb = inner_list_builder.values();
                vb.append_value(-1);
                vb.append_value(-2);
            }
            inner_list_builder.append(true);
            inner_list_builder.append(true);
        }
        iaa_builder.append(true);
        let int_array_array = iaa_builder.finish();
        use arrow_array::builder::MapFieldNames;
        let field_names = MapFieldNames {
            entry: "entries".to_string(),
            key: "key".to_string(),
            value: "value".to_string(),
        };
        let mut int_map_builder =
            MapBuilder::new(Some(field_names), StringBuilder::new(), Int32Builder::new());
        {
            let (keys, vals) = int_map_builder.entries();
            keys.append_value("k1");
            vals.append_value(-1);
        }
        int_map_builder.append(true).unwrap(); // finalize map for row 0
        let int_map = int_map_builder.finish();
        let field_names2 = MapFieldNames {
            entry: "entries".to_string(),
            key: "key".to_string(),
            value: "value".to_string(),
        };
        let mut ima_builder = ListBuilder::new(MapBuilder::new(
            Some(field_names2),
            StringBuilder::new(),
            Int32Builder::new(),
        ));
        {
            let map_builder = ima_builder.values();
            map_builder.append(true).unwrap();
            {
                let (keys, vals) = map_builder.entries();
                keys.append_value("k1");
                vals.append_value(1);
            }
            map_builder.append(true).unwrap();
            map_builder.append(true).unwrap();
            map_builder.append(true).unwrap();
        }
        ima_builder.append(true);
        let int_map_array_ = ima_builder.finish();
        let mut nested_sb = StructBuilder::new(
            vec![
                Arc::new(Field::new("a", DataType::Int32, true)),
                Arc::new(Field::new(
                    "B",
                    DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                    true,
                )),
                Arc::new(Field::new(
                    "c",
                    DataType::Struct(
                        vec![Field::new(
                            "D",
                            DataType::List(Arc::new(Field::new(
                                "item",
                                DataType::List(Arc::new(Field::new(
                                    "item",
                                    DataType::Struct(
                                        vec![
                                            Field::new("e", DataType::Int32, true),
                                            Field::new("f", DataType::Utf8, true),
                                        ]
                                        .into(),
                                    ),
                                    true,
                                ))),
                                true,
                            ))),
                            true,
                        )]
                        .into(),
                    ),
                    true,
                )),
                Arc::new(Field::new(
                    "G",
                    DataType::Map(
                        Arc::new(Field::new(
                            "entries",
                            DataType::Struct(
                                vec![
                                    Field::new("key", DataType::Utf8, false),
                                    Field::new(
                                        "value",
                                        DataType::Struct(
                                            vec![Field::new(
                                                "h",
                                                DataType::Struct(
                                                    vec![Field::new(
                                                        "i",
                                                        DataType::List(Arc::new(Field::new(
                                                            "item",
                                                            DataType::Float64,
                                                            true,
                                                        ))),
                                                        true,
                                                    )]
                                                    .into(),
                                                ),
                                                true,
                                            )]
                                            .into(),
                                        ),
                                        true,
                                    ),
                                ]
                                .into(),
                            ),
                            false,
                        )),
                        false,
                    ),
                    true,
                )),
            ],
            vec![
                Box::new(Int32Builder::new()),
                Box::new(ListBuilder::new(Int32Builder::new())),
                {
                    let d_field = Field::new(
                        "D",
                        DataType::List(Arc::new(Field::new(
                            "item",
                            DataType::List(Arc::new(Field::new(
                                "item",
                                DataType::Struct(
                                    vec![
                                        Field::new("e", DataType::Int32, true),
                                        Field::new("f", DataType::Utf8, true),
                                    ]
                                    .into(),
                                ),
                                true,
                            ))),
                            true,
                        ))),
                        true,
                    );
                    Box::new(StructBuilder::new(
                        vec![Arc::new(d_field)],
                        vec![Box::new({
                            let ef_struct_builder = StructBuilder::new(
                                vec![
                                    Arc::new(Field::new("e", DataType::Int32, true)),
                                    Arc::new(Field::new("f", DataType::Utf8, true)),
                                ],
                                vec![
                                    Box::new(Int32Builder::new()),
                                    Box::new(StringBuilder::new()),
                                ],
                            );
                            let list_of_ef = ListBuilder::new(ef_struct_builder);
                            ListBuilder::new(list_of_ef)
                        })],
                    ))
                },
                {
                    let map_field_names = MapFieldNames {
                        entry: "entries".to_string(),
                        key: "key".to_string(),
                        value: "value".to_string(),
                    };
                    let i_list_builder = ListBuilder::new(Float64Builder::new());
                    let h_struct = StructBuilder::new(
                        vec![Arc::new(Field::new(
                            "i",
                            DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
                            true,
                        ))],
                        vec![Box::new(i_list_builder)],
                    );
                    let g_value_builder = StructBuilder::new(
                        vec![Arc::new(Field::new(
                            "h",
                            DataType::Struct(
                                vec![Field::new(
                                    "i",
                                    DataType::List(Arc::new(Field::new(
                                        "item",
                                        DataType::Float64,
                                        true,
                                    ))),
                                    true,
                                )]
                                .into(),
                            ),
                            true,
                        ))],
                        vec![Box::new(h_struct)],
                    );
                    Box::new(MapBuilder::new(
                        Some(map_field_names),
                        StringBuilder::new(),
                        g_value_builder,
                    ))
                },
            ],
        );
        nested_sb.append(true);
        {
            let a_builder = nested_sb.field_builder::<Int32Builder>(0).unwrap();
            a_builder.append_value(-1);
        }
        {
            let b_builder = nested_sb
                .field_builder::<ListBuilder<Int32Builder>>(1)
                .unwrap();
            {
                let vb = b_builder.values();
                vb.append_value(-1);
            }
            b_builder.append(true);
        }
        {
            let c_struct_builder = nested_sb.field_builder::<StructBuilder>(2).unwrap();
            c_struct_builder.append(true);
            let d_list_builder = c_struct_builder
                .field_builder::<ListBuilder<ListBuilder<StructBuilder>>>(0)
                .unwrap();
            {
                let sub_list_builder = d_list_builder.values();
                {
                    let ef_struct = sub_list_builder.values();
                    ef_struct.append(true);
                    {
                        let e_b = ef_struct.field_builder::<Int32Builder>(0).unwrap();
                        e_b.append_value(-1);
                        let f_b = ef_struct.field_builder::<StringBuilder>(1).unwrap();
                        f_b.append_value("nonnullable");
                    }
                    sub_list_builder.append(true);
                }
                d_list_builder.append(true);
            }
        }
        {
            let g_map_builder = nested_sb
                .field_builder::<MapBuilder<StringBuilder, StructBuilder>>(3)
                .unwrap();
            g_map_builder.append(true).unwrap();
        }
        let nested_struct = nested_sb.finish();
        let expected = RecordBatch::try_from_iter_with_nullable([
            ("ID", Arc::new(id) as Arc<dyn Array>, true),
            ("Int_Array", Arc::new(int_array), true),
            ("int_array_array", Arc::new(int_array_array), true),
            ("Int_Map", Arc::new(int_map), true),
            ("int_map_array", Arc::new(int_map_array_), true),
            ("nested_Struct", Arc::new(nested_struct), true),
        ])
        .unwrap();
        let batch_large = read_file(&file, 8, false);
        assert_eq!(batch_large, expected, "Mismatch for batch_size=8");
        let batch_small = read_file(&file, 3, false);
        assert_eq!(batch_small, expected, "Mismatch for batch_size=3");
    }

    #[test]
    fn test_nonnullable_impala_strict() {
        let file = arrow_test_data("avro/nonnullable.impala.avro");
        let err = read_file_strict(&file, 8, false).unwrap_err();
        assert!(err.to_string().contains(
            "Found Avro union of the form ['T','null'], which is disallowed in strict_mode"
        ));
    }

    #[test]
    fn test_nullable_impala() {
        let file = arrow_test_data("avro/nullable.impala.avro");
        let batch1 = read_file(&file, 3, false);
        let batch2 = read_file(&file, 8, false);
        assert_eq!(batch1, batch2);
        let batch = batch1;
        assert_eq!(batch.num_rows(), 7);
        let id_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column should be an Int64Array");
        let expected_ids = [1, 2, 3, 4, 5, 6, 7];
        for (i, &expected_id) in expected_ids.iter().enumerate() {
            assert_eq!(id_array.value(i), expected_id, "Mismatch in id at row {i}",);
        }
        let int_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("int_array column should be a ListArray");
        {
            let offsets = int_array.value_offsets();
            let start = offsets[0] as usize;
            let end = offsets[1] as usize;
            let values = int_array
                .values()
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("Values of int_array should be an Int32Array");
            let row0: Vec<Option<i32>> = (start..end).map(|i| Some(values.value(i))).collect();
            assert_eq!(
                row0,
                vec![Some(1), Some(2), Some(3)],
                "Mismatch in int_array row 0"
            );
        }
        let nested_struct = batch
            .column(5)
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("nested_struct column should be a StructArray");
        let a_array = nested_struct
            .column_by_name("A")
            .expect("Field A should exist in nested_struct")
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Field A should be an Int32Array");
        assert_eq!(a_array.value(0), 1, "Mismatch in nested_struct.A at row 0");
        assert!(
            !a_array.is_valid(1),
            "Expected null in nested_struct.A at row 1"
        );
        assert!(
            !a_array.is_valid(3),
            "Expected null in nested_struct.A at row 3"
        );
        assert_eq!(a_array.value(6), 7, "Mismatch in nested_struct.A at row 6");
    }

    #[test]
    fn test_nullable_impala_strict() {
        let file = arrow_test_data("avro/nullable.impala.avro");
        let err = read_file_strict(&file, 8, false).unwrap_err();
        assert!(err.to_string().contains(
            "Found Avro union of the form ['T','null'], which is disallowed in strict_mode"
        ));
    }
}
