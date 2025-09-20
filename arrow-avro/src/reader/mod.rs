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
//! Facilities to read Apache Avro–encoded data into Arrow's `RecordBatch` format.
//!
//! This module exposes three layers of the API surface, from highest to lowest-level:
//!
//! * `ReaderBuilder`: configures how Avro is read (batch size, strict union handling,
//!   string representation, reader schema, etc.) and produces either:
//!   * a `Reader` for **Avro Object Container Files (OCF)** read from any `BufRead`, or
//!   * a low-level `Decoder` for **single‑object encoded** Avro bytes and Confluent
//!     **Schema Registry** framed messages.
//! * `Reader`: a convenient, synchronous iterator over `RecordBatch` decoded from an OCF
//!   input. Implements [`Iterator<Item = Result<RecordBatch, ArrowError>>`] and
//!   `RecordBatchReader`.
//! * `Decoder`: a push‑based row decoder that consumes raw Avro bytes and yields ready
//!   `RecordBatch` values when batches fill. This is suitable for integrating with async
//!   byte streams, network protocols, or other custom data sources.
//!
//! ## Encodings and when to use which type
//!
//! * **Object Container File (OCF)**: A self‑describing file format with a header containing
//!   the writer schema, optional compression codec, and a sync marker, followed by one or
//!   more data blocks. Use `Reader` for this format. See the Avro specification for the
//!   structure of OCF headers and blocks. <https://avro.apache.org/docs/1.11.1/specification/>
//! * **Single‑Object Encoding**: A stream‑friendly framing that prefixes each record body with
//!   the 2‑byte magic `0xC3 0x01` followed by a schema fingerprint. Use `Decoder` with a
//!   populated `SchemaStore` to resolve fingerprints to full
//!   schemas. <https://avro.apache.org/docs/1.11.1/specification/>
//! * **Confluent Schema Registry wire format**: A 1‑byte magic `0x00`, a 4‑byte big‑endian
//!   schema ID, then the Avro‑encoded body. Use `Decoder` with a
//!   `SchemaStore` configured for `FingerprintAlgorithm::None`
//!   and entries keyed by `Fingerprint::Id`. Confluent docs
//!   describe this framing.
//!
//! ## Basic file usage (OCF)
//!
//! Use `ReaderBuilder::build` to construct a `Reader` from any `BufRead`, such as a
//! `BufReader<File>`. The reader yields `RecordBatch` values you can iterate over or collect.
//!
//! ```no_run
//! use std::fs::File;
//! use std::io::BufReader;
//! use arrow_array::RecordBatch;
//! use arrow_avro::reader::ReaderBuilder;
//!
//! // Locate a test file (mirrors Arrow's test data layout)
//! let path = "avro/alltypes_plain.avro";
//! let path = std::env::var("ARROW_TEST_DATA")
//!     .map(|dir| format!("{dir}/{path}"))
//!     .unwrap_or_else(|_| format!("../testing/data/{path}"));
//!
//! let file = File::open(path).unwrap();
//! let mut reader = ReaderBuilder::new().build(BufReader::new(file)).unwrap();
//!
//! // Iterate batches
//! let mut num_rows = 0usize;
//! while let Some(batch) = reader.next() {
//!     let batch: RecordBatch = batch.unwrap();
//!     num_rows += batch.num_rows();
//! }
//! println!("decoded {num_rows} rows");
//! ```
//!
//! ## Streaming usage (single‑object / Confluent)
//!
//! The `Decoder` lets you integrate Avro decoding with **any** source of bytes by
//! periodically calling `Decoder::decode` with new data and calling `Decoder::flush`
//! to get a `RecordBatch` once at least one row is complete.
//!
//! The example below shows how to decode from an arbitrary stream of `bytes::Bytes` using
//! `futures` utilities. Note: this is illustrative and keeps a single in‑memory `Bytes`
//! buffer for simplicity—real applications typically maintain a rolling buffer.
//!
//! ```no_run
//! use bytes::{Buf, Bytes};
//! use futures::{Stream, StreamExt};
//! use std::task::{Poll, ready};
//! use arrow_array::RecordBatch;
//! use arrow_schema::ArrowError;
//! use arrow_avro::reader::Decoder;
//!
//! /// Decode a stream of Avro-framed bytes into RecordBatch values.
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
//!                     None => break, // EOF
//!                 };
//!             }
//!             // Feed as much as possible
//!             let decoded = match decoder.decode(buffered.as_ref()) {
//!                 Ok(n) => n,
//!                 Err(e) => return Poll::Ready(Some(Err(e))),
//!             };
//!             let read = buffered.len();
//!             buffered.advance(decoded);
//!             if decoded != read {
//!                 // decoder made partial progress; request more bytes
//!                 break
//!             }
//!         }
//!         // Return a batch if one or more rows are complete
//!         Poll::Ready(decoder.flush().transpose())
//!     })
//! }
//! ```
//!
//! ### Building a `Decoder` for **single‑object encoding** (Rabin fingerprints)
//!
//! ```no_run
//! use arrow_avro::schema::{AvroSchema, SchemaStore};
//! use arrow_avro::reader::ReaderBuilder;
//!
//! // Build a SchemaStore and register known writer schemas
//! let mut store = SchemaStore::new(); // Rabin by default
//! let user_schema = AvroSchema::new(r#"{"type":"record","name":"User","fields":[
//!     {"name":"id","type":"long"},{"name":"name","type":"string"}]}"#.to_string());
//! let _fp = store.register(user_schema).unwrap(); // computes Rabin CRC-64-AVRO
//!
//! // Build a Decoder that expects single-object encoding (0xC3 0x01 + fingerprint and body)
//! let decoder = ReaderBuilder::new()
//!     .with_writer_schema_store(store)
//!     .with_batch_size(1024)
//!     .build_decoder()
//!     .unwrap();
//! // Feed decoder with framed bytes (not shown; see `decode_stream` above).
//! ```
//!
//! ### Building a `Decoder` for **Confluent Schema Registry** framed messages
//!
//! ```no_run
//! use arrow_avro::schema::{AvroSchema, SchemaStore, Fingerprint, FingerprintAlgorithm};
//! use arrow_avro::reader::ReaderBuilder;
//!
//! // Confluent wire format uses a magic 0x00 byte + 4-byte schema id (big-endian).
//! // Create a store keyed by `Fingerprint::Id` and pre-populate with known schemas.
//! let mut store = SchemaStore::new_with_type(FingerprintAlgorithm::None);
//!
//! // Suppose registry ID 42 corresponds to this Avro schema:
//! let avro = AvroSchema::new(r#"{"type":"string"}"#.to_string());
//! store.set(Fingerprint::Id(42), avro).unwrap();
//!
//! // Build a Decoder that understands Confluent framing
//! let decoder = ReaderBuilder::new()
//!     .with_writer_schema_store(store)
//!     .build_decoder()
//!     .unwrap();
//! // Feed decoder with 0x00 + [id:4] + Avro body frames.
//! ```
//!
//! ## Schema evolution and batch boundaries
//!
//! `Decoder` supports mid‑stream schema changes when the input framing carries a schema
//! fingerprint (single‑object or Confluent). When a new fingerprint is observed:
//!
//! * If the current `RecordBatch` is **empty**, the decoder switches to the new schema
//!   immediately.
//! * If not, the decoder finishes the current batch first and only then switches.
//!
//! Consequently, the schema of batches produced by `Decoder::flush` may change over time,
//! and `Decoder` intentionally does **not** implement `RecordBatchReader`. In contrast,
//! `Reader` (OCF) has a single writer schema for the entire file and therefore implements
//! `RecordBatchReader`.
//!
//! ## Performance & memory
//!
//! * `batch_size` controls the maximum number of rows per `RecordBatch`. Larger batches
//!   amortize per‑batch overhead; smaller batches reduce peak memory usage and latency.
//! * When `utf8_view` is enabled, string columns use Arrow’s `StringViewArray`, which can
//!   reduce allocations for short strings.
//! * For OCF, blocks may be compressed `Reader` will decompress using the codec specified
//!   in the file header and feed uncompressed bytes to the row `Decoder`.
//!
//! ## Error handling
//!
//! * Incomplete inputs return parse errors with "Unexpected EOF"; callers typically provide
//!   more bytes and try again.
//! * If a fingerprint is unknown to the provided `SchemaStore`, decoding fails with a
//!   descriptive error. Populate the store up front to avoid this.
//!
//! ---
use crate::codec::{AvroField, AvroFieldBuilder};
use crate::schema::{
    compare_schemas, AvroSchema, Fingerprint, FingerprintAlgorithm, Schema, SchemaStore,
    CONFLUENT_MAGIC, SINGLE_OBJECT_MAGIC,
};
use arrow_array::{Array, RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, SchemaRef};
use block::BlockDecoder;
use header::{Header, HeaderDecoder};
use indexmap::IndexMap;
use record::RecordDecoder;
use std::collections::HashMap;
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

// NOTE: The Current ` is_incomplete_data ` below is temporary and will be improved prior to public release
fn is_incomplete_data(err: &ArrowError) -> bool {
    matches!(
        err,
        ArrowError::ParseError(msg)
            if msg.contains("Unexpected EOF")
    )
}

/// A low‑level, push‑based decoder from Avro bytes to Arrow `RecordBatch`.
///
/// `Decoder` is designed for **streaming** scenarios:
///
/// * You *feed* freshly received bytes using `Self::decode`, potentially multiple times,
///   until at least one row is complete.
/// * You then *drain* completed rows with `Self::flush`, which yields a `RecordBatch`
///   if any rows were finished since the last flush.
///
/// Unlike `Reader`, which is specialized for Avro **Object Container Files**, `Decoder`
/// understands **framed single‑object** inputs and **Confluent Schema Registry** messages,
/// switching schemas mid‑stream when the framing indicates a new fingerprint.
///
/// ### Supported prefixes
///
/// On each new row boundary, `Decoder` tries to match one of the following "prefixes":
///
/// * **Single‑Object encoding**: magic `0xC3 0x01` + schema fingerprint (length depends on
///   the configured `FingerprintAlgorithm`); see `SINGLE_OBJECT_MAGIC`.
/// * **Confluent wire format**: magic `0x00` + 4‑byte big‑endian schema id; see
///   `CONFLUENT_MAGIC`.
///
/// The active fingerprint determines which cached row decoder is used to decode the following
/// record body bytes.
///
/// ### Schema switching semantics
///
/// When a new fingerprint is observed:
///
/// * If the current batch is empty, the decoder switches immediately;
/// * Otherwise, the current batch is finalized on the next `flush` and only then
///   does the decoder switch to the new schema. This guarantees that a single `RecordBatch`
///   never mixes rows with different schemas.
///
/// ### Examples
///
/// Build a `Decoder` for single‑object encoding using a `SchemaStore` with Rabin fingerprints:
///
/// ```no_run
/// use arrow_avro::schema::{AvroSchema, SchemaStore};
/// use arrow_avro::reader::ReaderBuilder;
///
/// let mut store = SchemaStore::new(); // Rabin by default
/// let avro = AvroSchema::new(r#""string""#.to_string());
/// let _fp = store.register(avro).unwrap();
///
/// let mut decoder = ReaderBuilder::new()
///     .with_writer_schema_store(store)
///     .with_batch_size(512)
///     .build_decoder()
///     .unwrap();
///
/// // Feed bytes (framed as 0xC3 0x01 + fingerprint and body)
/// // decoder.decode(&bytes)?;
/// // if let Some(batch) = decoder.flush()? { /* process */ }
/// ```
///
/// Build a `Decoder` for Confluent Registry messages (magic 0x00 + 4‑byte id):
///
/// ```no_run
/// use arrow_avro::schema::{AvroSchema, SchemaStore, Fingerprint, FingerprintAlgorithm};
/// use arrow_avro::reader::ReaderBuilder;
///
/// let mut store = SchemaStore::new_with_type(FingerprintAlgorithm::None);
/// store.set(Fingerprint::Id(7), AvroSchema::new(r#""long""#.to_string())).unwrap();
///
/// let mut decoder = ReaderBuilder::new()
///     .with_writer_schema_store(store)
///     .build_decoder()
///     .unwrap();
/// ```
#[derive(Debug)]
pub struct Decoder {
    active_decoder: RecordDecoder,
    active_fingerprint: Option<Fingerprint>,
    batch_size: usize,
    remaining_capacity: usize,
    cache: IndexMap<Fingerprint, RecordDecoder>,
    fingerprint_algorithm: FingerprintAlgorithm,
    utf8_view: bool,
    strict_mode: bool,
    pending_schema: Option<(Fingerprint, RecordDecoder)>,
    awaiting_body: bool,
}

impl Decoder {
    /// Returns the Arrow schema for the rows decoded by this decoder.
    ///
    /// **Note:** With single‑object or Confluent framing, the schema may change
    /// at a row boundary when the input indicates a new fingerprint.
    pub fn schema(&self) -> SchemaRef {
        self.active_decoder.schema().clone()
    }

    /// Returns the configured maximum number of rows per batch.
    pub fn batch_size(&self) -> usize {
        self.batch_size
    }

    /// Feed a chunk of bytes into the decoder.
    ///
    /// This will:
    ///
    /// * Decode at most `Self::batch_size` rows;
    /// * Return the number of input bytes **consumed** from `data` (which may be 0 if more
    ///   bytes are required, or less than `data.len()` if a prefix/body straddles the
    ///   chunk boundary);
    /// * Defer producing a `RecordBatch` until you call `Self::flush`.
    ///
    /// # Returns
    /// The number of bytes consumed from `data`.
    ///
    /// # Errors
    /// Returns an error if:
    ///
    /// * The input indicates an unknown fingerprint (not present in the provided
    ///   `SchemaStore`;
    /// * The Avro body is malformed;
    /// * A strict‑mode union rule is violated (see `ReaderBuilder::with_strict_mode`).
    pub fn decode(&mut self, data: &[u8]) -> Result<usize, ArrowError> {
        let mut total_consumed = 0usize;
        while total_consumed < data.len() && self.remaining_capacity > 0 {
            if self.awaiting_body {
                match self.active_decoder.decode(&data[total_consumed..], 1) {
                    Ok(n) => {
                        self.remaining_capacity -= 1;
                        total_consumed += n;
                        self.awaiting_body = false;
                        continue;
                    }
                    Err(ref e) if is_incomplete_data(e) => break,
                    err => return err,
                };
            }
            match self.handle_prefix(&data[total_consumed..])? {
                Some(0) => break, // Insufficient bytes
                Some(n) => {
                    total_consumed += n;
                    self.apply_pending_schema_if_batch_empty();
                    self.awaiting_body = true;
                }
                None => {
                    return Err(ArrowError::ParseError(
                        "Missing magic bytes and fingerprint".to_string(),
                    ))
                }
            }
        }
        Ok(total_consumed)
    }

    // Attempt to handle a prefix at the current position.
    // * Ok(None) – buffer does not start with the prefix.
    // * Ok(Some(0)) – prefix detected, but the buffer is too short; caller should await more bytes.
    // * Ok(Some(n)) – consumed `n > 0` bytes of a complete prefix (magic and fingerprint).
    fn handle_prefix(&mut self, buf: &[u8]) -> Result<Option<usize>, ArrowError> {
        match self.fingerprint_algorithm {
            FingerprintAlgorithm::Rabin => {
                self.handle_prefix_common(buf, &SINGLE_OBJECT_MAGIC, |bytes| {
                    Fingerprint::Rabin(u64::from_le_bytes(bytes))
                })
            }
            FingerprintAlgorithm::None => {
                self.handle_prefix_common(buf, &CONFLUENT_MAGIC, |bytes| {
                    Fingerprint::Id(u32::from_be_bytes(bytes))
                })
            }
            #[cfg(feature = "md5")]
            FingerprintAlgorithm::MD5 => {
                self.handle_prefix_common(buf, &SINGLE_OBJECT_MAGIC, |bytes| {
                    Fingerprint::MD5(bytes)
                })
            }
            #[cfg(feature = "sha256")]
            FingerprintAlgorithm::SHA256 => {
                self.handle_prefix_common(buf, &SINGLE_OBJECT_MAGIC, |bytes| {
                    Fingerprint::SHA256(bytes)
                })
            }
        }
    }

    /// This method checks for the provided `magic` bytes at the start of `buf` and, if present,
    /// attempts to read the following fingerprint of `N` bytes, converting it to a
    /// `Fingerprint` using `fingerprint_from`.
    fn handle_prefix_common<const MAGIC_LEN: usize, const N: usize>(
        &mut self,
        buf: &[u8],
        magic: &[u8; MAGIC_LEN],
        fingerprint_from: impl FnOnce([u8; N]) -> Fingerprint,
    ) -> Result<Option<usize>, ArrowError> {
        // Need at least the magic bytes to decide
        // 2 bytes for Avro Spec and 1 byte for Confluent Wire Protocol.
        if buf.len() < MAGIC_LEN {
            return Ok(Some(0));
        }
        // Bail out early if the magic does not match.
        if &buf[..MAGIC_LEN] != magic {
            return Ok(None);
        }
        // Try to parse the fingerprint that follows the magic.
        let consumed_fp = self.handle_fingerprint(&buf[MAGIC_LEN..], fingerprint_from)?;
        // Convert the inner result into a “bytes consumed” count.
        // NOTE: Incomplete fingerprint consumes no bytes.
        Ok(Some(consumed_fp.map_or(0, |n| n + MAGIC_LEN)))
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
            return Ok(None); // insufficient bytes
        };
        // SAFETY: length checked above.
        let new_fingerprint = fingerprint_from(fingerprint_bytes.try_into().unwrap());
        // If the fingerprint indicates a schema change, prepare to switch decoders.
        if self.active_fingerprint != Some(new_fingerprint) {
            let Some(new_decoder) = self.cache.shift_remove(&new_fingerprint) else {
                return Err(ArrowError::ParseError(format!(
                    "Unknown fingerprint: {new_fingerprint:?}"
                )));
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

    fn apply_pending_schema(&mut self) {
        if let Some((new_fingerprint, new_decoder)) = self.pending_schema.take() {
            if let Some(old_fingerprint) = self.active_fingerprint.replace(new_fingerprint) {
                let old_decoder = std::mem::replace(&mut self.active_decoder, new_decoder);
                self.cache.shift_remove(&old_fingerprint);
                self.cache.insert(old_fingerprint, old_decoder);
            } else {
                self.active_decoder = new_decoder;
            }
        }
    }

    fn apply_pending_schema_if_batch_empty(&mut self) {
        if self.batch_is_empty() {
            self.apply_pending_schema();
        }
    }

    fn flush_and_reset(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        if self.batch_is_empty() {
            return Ok(None);
        }
        let batch = self.active_decoder.flush()?;
        self.remaining_capacity = self.batch_size;
        Ok(Some(batch))
    }

    /// Produce a `RecordBatch` if at least one row is fully decoded, returning
    /// `Ok(None)` if no new rows are available.
    ///
    /// If a schema change was detected while decoding rows for the current batch, the
    /// schema switch is applied **after** flushing this batch, so the **next** batch
    /// (if any) may have a different schema.
    pub fn flush(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        // We must flush the active decoder before switching to the pending one.
        let batch = self.flush_and_reset();
        self.apply_pending_schema();
        batch
    }

    /// Returns the number of rows that can be added to this decoder before it is full.
    pub fn capacity(&self) -> usize {
        self.remaining_capacity
    }

    /// Returns true if the decoder has reached its capacity for the current batch.
    pub fn batch_is_full(&self) -> bool {
        self.remaining_capacity == 0
    }

    /// Returns true if the decoder has not decoded any batches yet (i.e., the current batch is empty).
    pub fn batch_is_empty(&self) -> bool {
        self.remaining_capacity == self.batch_size
    }

    // Decode either the block count or remaining capacity from `data` (an OCF block payload).
    //
    // Returns the number of bytes consumed from `data` along with the number of records decoded.
    fn decode_block(&mut self, data: &[u8], count: usize) -> Result<(usize, usize), ArrowError> {
        // OCF decoding never interleaves records across blocks, so no chunking.
        let to_decode = std::cmp::min(count, self.remaining_capacity);
        if to_decode == 0 {
            return Ok((0, 0));
        }
        let consumed = self.active_decoder.decode(data, to_decode)?;
        self.remaining_capacity -= to_decode;
        Ok((consumed, to_decode))
    }

    // Produce a `RecordBatch` if at least one row is fully decoded, returning
    // `Ok(None)` if no new rows are available.
    fn flush_block(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        self.flush_and_reset()
    }
}

/// A builder that configures and constructs Avro readers and decoders.
///
/// `ReaderBuilder` is the primary entry point for this module. It supports:
///
/// * OCF reading via `Self::build`, returning a `Reader` over any `BufRead`;
/// * streaming decoding via `Self::build_decoder`, returning a `Decoder`.
///
/// ### Options
///
/// * **`batch_size`**: Max rows per `RecordBatch` (default: `1024`). See `Self::with_batch_size`.
/// * **`utf8_view`**: Use Arrow `StringViewArray` for string columns (default: `false`).
///   See `Self::with_utf8_view`.
/// * **`strict_mode`**: Opt‑in to stricter union handling (default: `false`).
///   See `Self::with_strict_mode`.
/// * **`reader_schema`**: Optional reader schema (projection / evolution) used when decoding
///   values (default: `None`). See `Self::with_reader_schema`.
/// * **`writer_schema_store`**: Required for building a `Decoder` for single‑object or
///   Confluent framing. Maps fingerprints to Avro schemas. See `Self::with_writer_schema_store`.
/// * **`active_fingerprint`**: Optional starting fingerprint for streaming decode when the
///   first frame omits one (rare). See `Self::with_active_fingerprint`.
///
/// ### Examples
///
/// Read an OCF file in batches of 4096 rows:
///
/// ```no_run
/// use std::fs::File;
/// use std::io::BufReader;
/// use arrow_avro::reader::ReaderBuilder;
///
/// let file = File::open("data.avro")?;
/// let mut reader = ReaderBuilder::new()
///     .with_batch_size(4096)
///     .build(BufReader::new(file))?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
/// Build a `Decoder` for Confluent messages:
///
/// ```no_run
/// use arrow_avro::schema::{AvroSchema, SchemaStore, Fingerprint, FingerprintAlgorithm};
/// use arrow_avro::reader::ReaderBuilder;
///
/// let mut store = SchemaStore::new_with_type(FingerprintAlgorithm::None);
/// store.set(Fingerprint::Id(1234), AvroSchema::new(r#"{"type":"record","name":"E","fields":[]}"#.to_string()))?;
///
/// let decoder = ReaderBuilder::new()
///     .with_writer_schema_store(store)
///     .build_decoder()?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug)]
pub struct ReaderBuilder {
    batch_size: usize,
    strict_mode: bool,
    utf8_view: bool,
    reader_schema: Option<AvroSchema>,
    writer_schema_store: Option<SchemaStore>,
    active_fingerprint: Option<Fingerprint>,
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
        }
    }
}

impl ReaderBuilder {
    /// Creates a new `ReaderBuilder` with defaults:
    ///
    /// * `batch_size = 1024`
    /// * `strict_mode = false`
    /// * `utf8_view = false`
    /// * `reader_schema = None`
    /// * `writer_schema_store = None`
    /// * `active_fingerprint = None`
    pub fn new() -> Self {
        Self::default()
    }

    fn make_record_decoder(
        &self,
        writer_schema: &Schema,
        reader_schema: Option<&Schema>,
    ) -> Result<RecordDecoder, ArrowError> {
        let mut builder = AvroFieldBuilder::new(writer_schema);
        if let Some(reader_schema) = reader_schema {
            builder = builder.with_reader_schema(reader_schema);
        }
        let root = builder
            .with_utf8view(self.utf8_view)
            .with_strict_mode(self.strict_mode)
            .build()?;
        RecordDecoder::try_new_with_options(root.data_type(), self.utf8_view)
    }

    fn make_record_decoder_from_schemas(
        &self,
        writer_schema: &Schema,
        reader_schema: Option<&AvroSchema>,
    ) -> Result<RecordDecoder, ArrowError> {
        let reader_schema_raw = reader_schema.map(|s| s.schema()).transpose()?;
        self.make_record_decoder(writer_schema, reader_schema_raw.as_ref())
    }

    fn make_decoder_with_parts(
        &self,
        active_decoder: RecordDecoder,
        active_fingerprint: Option<Fingerprint>,
        cache: IndexMap<Fingerprint, RecordDecoder>,
        fingerprint_algorithm: FingerprintAlgorithm,
    ) -> Decoder {
        Decoder {
            batch_size: self.batch_size,
            remaining_capacity: self.batch_size,
            active_fingerprint,
            active_decoder,
            cache,
            utf8_view: self.utf8_view,
            fingerprint_algorithm,
            strict_mode: self.strict_mode,
            pending_schema: None,
            awaiting_body: false,
        }
    }

    fn make_decoder(
        &self,
        header: Option<&Header>,
        reader_schema: Option<&AvroSchema>,
    ) -> Result<Decoder, ArrowError> {
        if let Some(hdr) = header {
            let writer_schema = hdr
                .schema()
                .map_err(|e| ArrowError::ExternalError(Box::new(e)))?
                .ok_or_else(|| {
                    ArrowError::ParseError("No Avro schema present in file header".into())
                })?;
            let record_decoder =
                self.make_record_decoder_from_schemas(&writer_schema, reader_schema)?;
            return Ok(self.make_decoder_with_parts(
                record_decoder,
                None,
                IndexMap::new(),
                FingerprintAlgorithm::Rabin,
            ));
        }
        let store = self.writer_schema_store.as_ref().ok_or_else(|| {
            ArrowError::ParseError("Writer schema store required for raw Avro".into())
        })?;
        let fingerprints = store.fingerprints();
        if fingerprints.is_empty() {
            return Err(ArrowError::ParseError(
                "Writer schema store must contain at least one schema".into(),
            ));
        }
        let start_fingerprint = self
            .active_fingerprint
            .or_else(|| fingerprints.first().copied())
            .ok_or_else(|| {
                ArrowError::ParseError("Could not determine initial schema fingerprint".into())
            })?;
        let mut cache = IndexMap::with_capacity(fingerprints.len().saturating_sub(1));
        let mut active_decoder: Option<RecordDecoder> = None;
        for fingerprint in store.fingerprints() {
            let avro_schema = match store.lookup(&fingerprint) {
                Some(schema) => schema,
                None => {
                    return Err(ArrowError::ComputeError(format!(
                        "Fingerprint {fingerprint:?} not found in schema store",
                    )));
                }
            };
            let writer_schema = avro_schema.schema()?;
            let record_decoder =
                self.make_record_decoder_from_schemas(&writer_schema, reader_schema)?;
            if fingerprint == start_fingerprint {
                active_decoder = Some(record_decoder);
            } else {
                cache.insert(fingerprint, record_decoder);
            }
        }
        let active_decoder = active_decoder.ok_or_else(|| {
            ArrowError::ComputeError(format!(
                "Initial fingerprint {start_fingerprint:?} not found in schema store"
            ))
        })?;
        Ok(self.make_decoder_with_parts(
            active_decoder,
            Some(start_fingerprint),
            cache,
            store.fingerprint_algorithm(),
        ))
    }

    /// Sets the **row‑based batch size**.
    ///
    /// Each call to `Decoder::flush` or each iteration of `Reader` yields a batch with
    /// *up to* this many rows. Larger batches can reduce overhead; smaller batches can
    /// reduce peak memory usage and latency.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Choose Arrow's `StringViewArray` for UTF‑8 string data.
    ///
    /// When enabled, textual Avro fields are loaded into Arrow’s **StringViewArray**
    /// instead of the standard `StringArray`. This can improve performance for workloads
    /// with many short strings by reducing allocations.
    pub fn with_utf8_view(mut self, utf8_view: bool) -> Self {
        self.utf8_view = utf8_view;
        self
    }

    /// Returns whether `StringViewArray` is enabled for string data.
    pub fn use_utf8view(&self) -> bool {
        self.utf8_view
    }

    /// Enable stricter behavior for certain Avro unions (e.g., `[T, "null"]`).
    ///
    /// When `true`, ambiguous or lossy unions that would otherwise be coerced may instead
    /// produce a descriptive error. Use this to catch schema issues early during ingestion.
    pub fn with_strict_mode(mut self, strict_mode: bool) -> Self {
        self.strict_mode = strict_mode;
        self
    }

    /// Sets the **reader schema** used during decoding.
    ///
    /// If not provided, the writer schema from the OCF header (for `Reader`) or the
    /// schema looked up from the fingerprint (for `Decoder`) is used directly.
    ///
    /// A reader schema can be used for **schema evolution** or **projection**.
    pub fn with_reader_schema(mut self, schema: AvroSchema) -> Self {
        self.reader_schema = Some(schema);
        self
    }

    /// Sets the `SchemaStore` used to resolve writer schemas by fingerprint.
    ///
    /// This is required when building a `Decoder` for **single‑object encoding** or the
    /// **Confluent** wire format. The store maps a fingerprint (Rabin / MD5 / SHA‑256 /
    /// ID) to a full Avro schema.
    ///
    /// Defaults to `None`.
    pub fn with_writer_schema_store(mut self, store: SchemaStore) -> Self {
        self.writer_schema_store = Some(store);
        self
    }

    /// Sets the initial schema fingerprint for stream decoding.
    ///
    /// This can be useful for streams that **do not include** a fingerprint before the first
    /// record body (uncommon). If not set, the first observed fingerprint is used.
    pub fn with_active_fingerprint(mut self, fp: Fingerprint) -> Self {
        self.active_fingerprint = Some(fp);
        self
    }

    /// Build a `Reader` (OCF) from this builder and a `BufRead`.
    ///
    /// This reads and validates the OCF header, initializes an internal row decoder from
    /// the discovered writer (and optional reader) schema, and prepares to iterate blocks,
    /// decompressing if necessary.
    pub fn build<R: BufRead>(self, mut reader: R) -> Result<Reader<R>, ArrowError> {
        let header = read_header(&mut reader)?;
        let decoder = self.make_decoder(Some(&header), self.reader_schema.as_ref())?;
        Ok(Reader {
            reader,
            header,
            decoder,
            block_decoder: BlockDecoder::default(),
            block_data: Vec::new(),
            block_count: 0,
            block_cursor: 0,
            finished: false,
        })
    }

    /// Build a streaming `Decoder` from this builder.
    ///
    /// # Requirements
    /// * `SchemaStore` **must** be provided via `Self::with_writer_schema_store`.
    /// * The store should contain **all** fingerprints that may appear on the stream.
    ///
    /// # Errors
    /// * Returns [`ArrowError::InvalidArgumentError`] if the schema store is missing
    pub fn build_decoder(self) -> Result<Decoder, ArrowError> {
        if self.writer_schema_store.is_none() {
            return Err(ArrowError::InvalidArgumentError(
                "Building a decoder requires a writer schema store".to_string(),
            ));
        }
        self.make_decoder(None, self.reader_schema.as_ref())
    }
}

/// A high‑level Avro **Object Container File** reader.
///
/// `Reader` pulls blocks from a `BufRead` source, handles optional block compression,
/// and decodes them row‑by‑row into Arrow `RecordBatch` values using an internal
/// `Decoder`. It implements both:
///
/// * [`Iterator<Item = Result<RecordBatch, ArrowError>>`], and
/// * `RecordBatchReader`, guaranteeing a consistent schema across all produced batches.
///
#[derive(Debug)]
pub struct Reader<R: BufRead> {
    reader: R,
    header: Header,
    decoder: Decoder,
    block_decoder: BlockDecoder,
    block_data: Vec<u8>,
    block_count: usize,
    block_cursor: usize,
    finished: bool,
}

impl<R: BufRead> Reader<R> {
    /// Returns the Arrow schema discovered from the Avro file header (or derived via
    /// the optional reader schema).
    pub fn schema(&self) -> SchemaRef {
        self.decoder.schema()
    }

    /// Returns a reference to the parsed Avro container‑file header (magic, metadata, codec, sync).
    pub fn avro_header(&self) -> &Header {
        &self.header
    }

    /// Reads the next `RecordBatch` from the Avro file, or `Ok(None)` on EOF.
    ///
    /// Batches are bounded by `batch_size`; a single OCF block may yield multiple batches,
    /// and a batch may also span multiple blocks.
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
                    self.block_data = if let Some(ref codec) = self.header.compression()? {
                        codec.decompress(&block.data)?
                    } else {
                        block.data
                    };
                    self.block_count = block.count;
                    self.block_cursor = 0;
                } else if consumed == 0 {
                    // The block decoder made no progress on a non-empty buffer.
                    return Err(ArrowError::ParseError(
                        "Could not decode next Avro block from partial data".to_string(),
                    ));
                }
            }
            // Decode as many rows as will fit in the current batch
            if self.block_cursor < self.block_data.len() {
                let (consumed, records_decoded) = self
                    .decoder
                    .decode_block(&self.block_data[self.block_cursor..], self.block_count)?;
                self.block_cursor += consumed;
                self.block_count -= records_decoded;
            }
        }
        self.decoder.flush_block()
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
        AvroSchema, Fingerprint, FingerprintAlgorithm, PrimitiveType, Schema as AvroRaw,
        SchemaStore, AVRO_ENUM_SYMBOLS_METADATA_KEY, CONFLUENT_MAGIC, SINGLE_OBJECT_MAGIC,
    };
    use crate::test_util::arrow_test_data;
    use arrow::array::ArrayDataBuilder;
    use arrow_array::builder::{
        ArrayBuilder, BooleanBuilder, Float32Builder, Float64Builder, Int32Builder, Int64Builder,
        ListBuilder, MapBuilder, StringBuilder, StructBuilder,
    };
    use arrow_array::types::{Int32Type, IntervalMonthDayNanoType};
    use arrow_array::*;
    use arrow_buffer::{i256, Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
    use arrow_schema::{
        ArrowError, DataType, Field, FieldRef, Fields, IntervalUnit, Schema, UnionFields, UnionMode,
    };
    use bytes::{Buf, BufMut, Bytes};
    use futures::executor::block_on;
    use futures::{stream, Stream, StreamExt, TryStreamExt};
    use serde_json::{json, Value};
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

    fn make_record_schema(pt: PrimitiveType) -> AvroSchema {
        let js = format!(
            r#"{{"type":"record","name":"TestRecord","fields":[{{"name":"a","type":"{}"}}]}}"#,
            pt.as_ref()
        );
        AvroSchema::new(js)
    }

    fn make_two_schema_store() -> (
        SchemaStore,
        Fingerprint,
        Fingerprint,
        AvroSchema,
        AvroSchema,
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
                let mut out = Vec::with_capacity(2 + 8);
                out.extend_from_slice(&SINGLE_OBJECT_MAGIC);
                out.extend_from_slice(&v.to_le_bytes());
                out
            }
            Fingerprint::Id(v) => {
                panic!("make_prefix expects a Rabin fingerprint, got ({v})");
            }
            #[cfg(feature = "md5")]
            Fingerprint::MD5(v) => {
                panic!("make_prefix expects a Rabin fingerprint, got ({v:?})");
            }
            #[cfg(feature = "sha256")]
            Fingerprint::SHA256(id) => {
                panic!("make_prefix expects a Rabin fingerprint, got ({id:?})");
            }
        }
    }

    fn make_decoder(store: &SchemaStore, fp: Fingerprint, reader_schema: &AvroSchema) -> Decoder {
        ReaderBuilder::new()
            .with_batch_size(8)
            .with_reader_schema(reader_schema.clone())
            .with_writer_schema_store(store.clone())
            .with_active_fingerprint(fp)
            .build_decoder()
            .expect("decoder")
    }

    fn make_id_prefix(id: u32, additional: usize) -> Vec<u8> {
        let capacity = CONFLUENT_MAGIC.len() + size_of::<u32>() + additional;
        let mut out = Vec::with_capacity(capacity);
        out.extend_from_slice(&CONFLUENT_MAGIC);
        out.extend_from_slice(&id.to_be_bytes());
        out
    }

    fn make_message_id(id: u32, value: i64) -> Vec<u8> {
        let encoded_value = encode_zigzag(value);
        let mut msg = make_id_prefix(id, encoded_value.len());
        msg.extend_from_slice(&encoded_value);
        msg
    }

    fn make_value_schema(pt: PrimitiveType) -> AvroSchema {
        let json_schema = format!(
            r#"{{"type":"record","name":"S","fields":[{{"name":"v","type":"{}"}}]}}"#,
            pt.as_ref()
        );
        AvroSchema::new(json_schema)
    }

    fn encode_zigzag(value: i64) -> Vec<u8> {
        let mut n = ((value << 1) ^ (value >> 63)) as u64;
        let mut out = Vec::new();
        loop {
            if (n & !0x7F) == 0 {
                out.push(n as u8);
                break;
            } else {
                out.push(((n & 0x7F) | 0x80) as u8);
                n >>= 7;
            }
        }
        out
    }

    fn make_message(fp: Fingerprint, value: i64) -> Vec<u8> {
        let mut msg = make_prefix(fp);
        msg.extend_from_slice(&encode_zigzag(value));
        msg
    }

    fn load_writer_schema_json(path: &str) -> Value {
        let file = File::open(path).unwrap();
        let header = super::read_header(BufReader::new(file)).unwrap();
        let schema = header.schema().unwrap().unwrap();
        serde_json::to_value(&schema).unwrap()
    }

    fn make_reader_schema_with_promotions(
        path: &str,
        promotions: &HashMap<&str, &str>,
    ) -> AvroSchema {
        let mut root = load_writer_schema_json(path);
        assert_eq!(root["type"], "record", "writer schema must be a record");
        let fields = root
            .get_mut("fields")
            .and_then(|f| f.as_array_mut())
            .expect("record has fields");
        for f in fields.iter_mut() {
            let Some(name) = f.get("name").and_then(|n| n.as_str()) else {
                continue;
            };
            if let Some(new_ty) = promotions.get(name) {
                let ty = f.get_mut("type").expect("field has a type");
                match ty {
                    Value::String(_) => {
                        *ty = Value::String((*new_ty).to_string());
                    }
                    // Union
                    Value::Array(arr) => {
                        for b in arr.iter_mut() {
                            match b {
                                Value::String(s) if s != "null" => {
                                    *b = Value::String((*new_ty).to_string());
                                    break;
                                }
                                Value::Object(_) => {
                                    *b = Value::String((*new_ty).to_string());
                                    break;
                                }
                                _ => {}
                            }
                        }
                    }
                    Value::Object(_) => {
                        *ty = Value::String((*new_ty).to_string());
                    }
                    _ => {}
                }
            }
        }
        AvroSchema::new(root.to_string())
    }

    fn make_reader_schema_with_enum_remap(
        path: &str,
        remap: &HashMap<&str, Vec<&str>>,
    ) -> AvroSchema {
        let mut root = load_writer_schema_json(path);
        assert_eq!(root["type"], "record", "writer schema must be a record");
        let fields = root
            .get_mut("fields")
            .and_then(|f| f.as_array_mut())
            .expect("record has fields");

        fn to_symbols_array(symbols: &[&str]) -> Value {
            Value::Array(symbols.iter().map(|s| Value::String((*s).into())).collect())
        }

        fn update_enum_symbols(ty: &mut Value, symbols: &Value) {
            match ty {
                Value::Object(map) => {
                    if matches!(map.get("type"), Some(Value::String(t)) if t == "enum") {
                        map.insert("symbols".to_string(), symbols.clone());
                    }
                }
                Value::Array(arr) => {
                    for b in arr.iter_mut() {
                        if let Value::Object(map) = b {
                            if matches!(map.get("type"), Some(Value::String(t)) if t == "enum") {
                                map.insert("symbols".to_string(), symbols.clone());
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        for f in fields.iter_mut() {
            let Some(name) = f.get("name").and_then(|n| n.as_str()) else {
                continue;
            };
            if let Some(new_symbols) = remap.get(name) {
                let symbols_val = to_symbols_array(new_symbols);
                let ty = f.get_mut("type").expect("field has a type");
                update_enum_symbols(ty, &symbols_val);
            }
        }
        AvroSchema::new(root.to_string())
    }

    fn read_alltypes_with_reader_schema(path: &str, reader_schema: AvroSchema) -> RecordBatch {
        let file = File::open(path).unwrap();
        let reader = ReaderBuilder::new()
            .with_batch_size(1024)
            .with_utf8_view(false)
            .with_reader_schema(reader_schema)
            .build(BufReader::new(file))
            .unwrap();
        let schema = reader.schema();
        let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
        arrow::compute::concat_batches(&schema, &batches).unwrap()
    }

    fn make_reader_schema_with_selected_fields_in_order(
        path: &str,
        selected: &[&str],
    ) -> AvroSchema {
        let mut root = load_writer_schema_json(path);
        assert_eq!(root["type"], "record", "writer schema must be a record");
        let writer_fields = root
            .get("fields")
            .and_then(|f| f.as_array())
            .expect("record has fields");
        let mut field_map: HashMap<String, Value> = HashMap::with_capacity(writer_fields.len());
        for f in writer_fields {
            if let Some(name) = f.get("name").and_then(|n| n.as_str()) {
                field_map.insert(name.to_string(), f.clone());
            }
        }
        let mut new_fields = Vec::with_capacity(selected.len());
        for name in selected {
            let f = field_map
                .get(*name)
                .unwrap_or_else(|| panic!("field '{name}' not found in writer schema"))
                .clone();
            new_fields.push(f);
        }
        root["fields"] = Value::Array(new_fields);
        AvroSchema::new(root.to_string())
    }

    #[test]
    fn test_alltypes_schema_promotion_mixed() {
        let files = [
            "avro/alltypes_plain.avro",
            "avro/alltypes_plain.snappy.avro",
            "avro/alltypes_plain.zstandard.avro",
            "avro/alltypes_plain.bzip2.avro",
            "avro/alltypes_plain.xz.avro",
        ];
        for file in files {
            let file = arrow_test_data(file);
            let mut promotions: HashMap<&str, &str> = HashMap::new();
            promotions.insert("id", "long");
            promotions.insert("tinyint_col", "float");
            promotions.insert("smallint_col", "double");
            promotions.insert("int_col", "double");
            promotions.insert("bigint_col", "double");
            promotions.insert("float_col", "double");
            promotions.insert("date_string_col", "string");
            promotions.insert("string_col", "string");
            let reader_schema = make_reader_schema_with_promotions(&file, &promotions);
            let batch = read_alltypes_with_reader_schema(&file, reader_schema);
            let expected = RecordBatch::try_from_iter_with_nullable([
                (
                    "id",
                    Arc::new(Int64Array::from(vec![4i64, 5, 6, 7, 2, 3, 0, 1])) as _,
                    true,
                ),
                (
                    "bool_col",
                    Arc::new(BooleanArray::from_iter((0..8).map(|x| Some(x % 2 == 0)))) as _,
                    true,
                ),
                (
                    "tinyint_col",
                    Arc::new(Float32Array::from_iter_values(
                        (0..8).map(|x| (x % 2) as f32),
                    )) as _,
                    true,
                ),
                (
                    "smallint_col",
                    Arc::new(Float64Array::from_iter_values(
                        (0..8).map(|x| (x % 2) as f64),
                    )) as _,
                    true,
                ),
                (
                    "int_col",
                    Arc::new(Float64Array::from_iter_values(
                        (0..8).map(|x| (x % 2) as f64),
                    )) as _,
                    true,
                ),
                (
                    "bigint_col",
                    Arc::new(Float64Array::from_iter_values(
                        (0..8).map(|x| ((x % 2) * 10) as f64),
                    )) as _,
                    true,
                ),
                (
                    "float_col",
                    Arc::new(Float64Array::from_iter_values(
                        (0..8).map(|x| ((x % 2) as f32 * 1.1f32) as f64),
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
                    Arc::new(StringArray::from(vec![
                        "03/01/09", "03/01/09", "04/01/09", "04/01/09", "02/01/09", "02/01/09",
                        "01/01/09", "01/01/09",
                    ])) as _,
                    true,
                ),
                (
                    "string_col",
                    Arc::new(StringArray::from(
                        (0..8)
                            .map(|x| if x % 2 == 0 { "0" } else { "1" })
                            .collect::<Vec<_>>(),
                    )) as _,
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
            assert_eq!(batch, expected, "mismatch for file {file}");
        }
    }

    #[test]
    fn test_alltypes_schema_promotion_long_to_float_only() {
        let files = [
            "avro/alltypes_plain.avro",
            "avro/alltypes_plain.snappy.avro",
            "avro/alltypes_plain.zstandard.avro",
            "avro/alltypes_plain.bzip2.avro",
            "avro/alltypes_plain.xz.avro",
        ];
        for file in files {
            let file = arrow_test_data(file);
            let mut promotions: HashMap<&str, &str> = HashMap::new();
            promotions.insert("bigint_col", "float");
            let reader_schema = make_reader_schema_with_promotions(&file, &promotions);
            let batch = read_alltypes_with_reader_schema(&file, reader_schema);
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
                    Arc::new(Float32Array::from_iter_values(
                        (0..8).map(|x| ((x % 2) * 10) as f32),
                    )) as _,
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
            assert_eq!(batch, expected, "mismatch for file {file}");
        }
    }

    #[test]
    fn test_alltypes_schema_promotion_bytes_to_string_only() {
        let files = [
            "avro/alltypes_plain.avro",
            "avro/alltypes_plain.snappy.avro",
            "avro/alltypes_plain.zstandard.avro",
            "avro/alltypes_plain.bzip2.avro",
            "avro/alltypes_plain.xz.avro",
        ];
        for file in files {
            let file = arrow_test_data(file);
            let mut promotions: HashMap<&str, &str> = HashMap::new();
            promotions.insert("date_string_col", "string");
            promotions.insert("string_col", "string");
            let reader_schema = make_reader_schema_with_promotions(&file, &promotions);
            let batch = read_alltypes_with_reader_schema(&file, reader_schema);
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
                    Arc::new(StringArray::from(vec![
                        "03/01/09", "03/01/09", "04/01/09", "04/01/09", "02/01/09", "02/01/09",
                        "01/01/09", "01/01/09",
                    ])) as _,
                    true,
                ),
                (
                    "string_col",
                    Arc::new(StringArray::from(
                        (0..8)
                            .map(|x| if x % 2 == 0 { "0" } else { "1" })
                            .collect::<Vec<_>>(),
                    )) as _,
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
            assert_eq!(batch, expected, "mismatch for file {file}");
        }
    }

    #[test]
    fn test_alltypes_illegal_promotion_bool_to_double_errors() {
        let file = arrow_test_data("avro/alltypes_plain.avro");
        let mut promotions: HashMap<&str, &str> = HashMap::new();
        promotions.insert("bool_col", "double"); // illegal
        let reader_schema = make_reader_schema_with_promotions(&file, &promotions);
        let file_handle = File::open(&file).unwrap();
        let result = ReaderBuilder::new()
            .with_reader_schema(reader_schema)
            .build(BufReader::new(file_handle));
        let err = result.expect_err("expected illegal promotion to error");
        let msg = err.to_string();
        assert!(
            msg.contains("Illegal promotion") || msg.contains("illegal promotion"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn test_simple_enum_with_reader_schema_mapping() {
        let file = arrow_test_data("avro/simple_enum.avro");
        let mut remap: HashMap<&str, Vec<&str>> = HashMap::new();
        remap.insert("f1", vec!["d", "c", "b", "a"]);
        remap.insert("f2", vec!["h", "g", "f", "e"]);
        remap.insert("f3", vec!["k", "i", "j"]);
        let reader_schema = make_reader_schema_with_enum_remap(&file, &remap);
        let actual = read_alltypes_with_reader_schema(&file, reader_schema);
        let dict_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let f1_keys = Int32Array::from(vec![3, 2, 1, 0]);
        let f1_vals = StringArray::from(vec!["d", "c", "b", "a"]);
        let f1 = DictionaryArray::<Int32Type>::try_new(f1_keys, Arc::new(f1_vals)).unwrap();
        let mut md_f1 = HashMap::new();
        md_f1.insert(
            AVRO_ENUM_SYMBOLS_METADATA_KEY.to_string(),
            r#"["d","c","b","a"]"#.to_string(),
        );
        let f1_field = Field::new("f1", dict_type.clone(), false).with_metadata(md_f1);
        let f2_keys = Int32Array::from(vec![1, 0, 3, 2]);
        let f2_vals = StringArray::from(vec!["h", "g", "f", "e"]);
        let f2 = DictionaryArray::<Int32Type>::try_new(f2_keys, Arc::new(f2_vals)).unwrap();
        let mut md_f2 = HashMap::new();
        md_f2.insert(
            AVRO_ENUM_SYMBOLS_METADATA_KEY.to_string(),
            r#"["h","g","f","e"]"#.to_string(),
        );
        let f2_field = Field::new("f2", dict_type.clone(), false).with_metadata(md_f2);
        let f3_keys = Int32Array::from(vec![Some(2), Some(0), None, Some(1)]);
        let f3_vals = StringArray::from(vec!["k", "i", "j"]);
        let f3 = DictionaryArray::<Int32Type>::try_new(f3_keys, Arc::new(f3_vals)).unwrap();
        let mut md_f3 = HashMap::new();
        md_f3.insert(
            AVRO_ENUM_SYMBOLS_METADATA_KEY.to_string(),
            r#"["k","i","j"]"#.to_string(),
        );
        let f3_field = Field::new("f3", dict_type.clone(), true).with_metadata(md_f3);
        let expected_schema = Arc::new(Schema::new(vec![f1_field, f2_field, f3_field]));
        let expected = RecordBatch::try_new(
            expected_schema,
            vec![Arc::new(f1) as ArrayRef, Arc::new(f2), Arc::new(f3)],
        )
        .unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_schema_store_register_lookup() {
        let schema_int = make_record_schema(PrimitiveType::Int);
        let schema_long = make_record_schema(PrimitiveType::Long);
        let mut store = SchemaStore::new();
        let fp_int = store.register(schema_int.clone()).unwrap();
        let fp_long = store.register(schema_long.clone()).unwrap();
        assert_eq!(store.lookup(&fp_int).cloned(), Some(schema_int));
        assert_eq!(store.lookup(&fp_long).cloned(), Some(schema_long));
        assert_eq!(store.fingerprint_algorithm(), FingerprintAlgorithm::Rabin);
    }

    #[test]
    fn test_unknown_fingerprint_is_error() {
        let (store, fp_int, _fp_long, _schema_int, schema_long) = make_two_schema_store();
        let unknown_fp = Fingerprint::Rabin(0xDEAD_BEEF_DEAD_BEEF);
        let prefix = make_prefix(unknown_fp);
        let mut decoder = make_decoder(&store, fp_int, &schema_long);
        let err = decoder.decode(&prefix).expect_err("decode should error");
        let msg = err.to_string();
        assert!(
            msg.contains("Unknown fingerprint"),
            "unexpected message: {msg}"
        );
    }

    #[test]
    fn test_handle_prefix_incomplete_magic() {
        let (store, fp_int, _fp_long, _schema_int, schema_long) = make_two_schema_store();
        let mut decoder = make_decoder(&store, fp_int, &schema_long);
        let buf = &SINGLE_OBJECT_MAGIC[..1];
        let res = decoder.handle_prefix(buf).unwrap();
        assert_eq!(res, Some(0));
        assert!(decoder.pending_schema.is_none());
    }

    #[test]
    fn test_handle_prefix_magic_mismatch() {
        let (store, fp_int, _fp_long, _schema_int, schema_long) = make_two_schema_store();
        let mut decoder = make_decoder(&store, fp_int, &schema_long);
        let buf = [0xFFu8, 0x00u8, 0x01u8];
        let res = decoder.handle_prefix(&buf).unwrap();
        assert!(res.is_none());
    }

    #[test]
    fn test_handle_prefix_incomplete_fingerprint() {
        let (store, fp_int, fp_long, _schema_int, schema_long) = make_two_schema_store();
        let mut decoder = make_decoder(&store, fp_int, &schema_long);
        let long_bytes = match fp_long {
            Fingerprint::Rabin(v) => v.to_le_bytes(),
            Fingerprint::Id(id) => panic!("expected Rabin fingerprint, got ({id})"),
            #[cfg(feature = "md5")]
            Fingerprint::MD5(v) => panic!("expected Rabin fingerprint, got ({v:?})"),
            #[cfg(feature = "sha256")]
            Fingerprint::SHA256(v) => panic!("expected Rabin fingerprint, got ({v:?})"),
        };
        let mut buf = Vec::from(SINGLE_OBJECT_MAGIC);
        buf.extend_from_slice(&long_bytes[..4]);
        let res = decoder.handle_prefix(&buf).unwrap();
        assert_eq!(res, Some(0));
        assert!(decoder.pending_schema.is_none());
    }

    #[test]
    fn test_handle_prefix_valid_prefix_switches_schema() {
        let (store, fp_int, fp_long, _schema_int, schema_long) = make_two_schema_store();
        let mut decoder = make_decoder(&store, fp_int, &schema_long);
        let writer_schema_long = schema_long.schema().unwrap();
        let root_long = AvroFieldBuilder::new(&writer_schema_long).build().unwrap();
        let long_decoder =
            RecordDecoder::try_new_with_options(root_long.data_type(), decoder.utf8_view).unwrap();
        let _ = decoder.cache.insert(fp_long, long_decoder);
        let mut buf = Vec::from(SINGLE_OBJECT_MAGIC);
        match fp_long {
            Fingerprint::Rabin(v) => buf.extend_from_slice(&v.to_le_bytes()),
            Fingerprint::Id(id) => panic!("expected Rabin fingerprint, got ({id})"),
            #[cfg(feature = "md5")]
            Fingerprint::MD5(v) => panic!("expected Rabin fingerprint, got ({v:?})"),
            #[cfg(feature = "sha256")]
            Fingerprint::SHA256(v) => panic!("expected Rabin fingerprint, got ({v:?})"),
        }
        let consumed = decoder.handle_prefix(&buf).unwrap().unwrap();
        assert_eq!(consumed, buf.len());
        assert!(decoder.pending_schema.is_some());
        assert_eq!(decoder.pending_schema.as_ref().unwrap().0, fp_long);
    }

    #[test]
    fn test_two_messages_same_schema() {
        let writer_schema = make_value_schema(PrimitiveType::Int);
        let reader_schema = writer_schema.clone();
        let mut store = SchemaStore::new();
        let fp = store.register(writer_schema).unwrap();
        let msg1 = make_message(fp, 42);
        let msg2 = make_message(fp, 11);
        let input = [msg1.clone(), msg2.clone()].concat();
        let mut decoder = ReaderBuilder::new()
            .with_batch_size(8)
            .with_reader_schema(reader_schema.clone())
            .with_writer_schema_store(store)
            .with_active_fingerprint(fp)
            .build_decoder()
            .unwrap();
        let _ = decoder.decode(&input).unwrap();
        let batch = decoder.flush().unwrap().expect("batch");
        assert_eq!(batch.num_rows(), 2);
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(col.value(0), 42);
        assert_eq!(col.value(1), 11);
    }

    #[test]
    fn test_two_messages_schema_switch() {
        let w_int = make_value_schema(PrimitiveType::Int);
        let w_long = make_value_schema(PrimitiveType::Long);
        let r_long = w_long.clone();
        let mut store = SchemaStore::new();
        let fp_int = store.register(w_int).unwrap();
        let fp_long = store.register(w_long).unwrap();
        let msg_int = make_message(fp_int, 1);
        let msg_long = make_message(fp_long, 123456789_i64);
        let mut decoder = ReaderBuilder::new()
            .with_batch_size(8)
            .with_writer_schema_store(store)
            .with_active_fingerprint(fp_int)
            .build_decoder()
            .unwrap();
        let _ = decoder.decode(&msg_int).unwrap();
        let batch1 = decoder.flush().unwrap().expect("batch1");
        assert_eq!(batch1.num_rows(), 1);
        assert_eq!(
            batch1
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            1
        );
        let _ = decoder.decode(&msg_long).unwrap();
        let batch2 = decoder.flush().unwrap().expect("batch2");
        assert_eq!(batch2.num_rows(), 1);
        assert_eq!(
            batch2
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            123456789_i64
        );
    }

    #[test]
    fn test_two_messages_same_schema_id() {
        let writer_schema = make_value_schema(PrimitiveType::Int);
        let reader_schema = writer_schema.clone();
        let id = 100u32;
        // Set up store with None fingerprint algorithm and register schema by id
        let mut store = SchemaStore::new_with_type(FingerprintAlgorithm::None);
        let _ = store
            .set(Fingerprint::Id(id), writer_schema.clone())
            .expect("set id schema");
        let msg1 = make_message_id(id, 21);
        let msg2 = make_message_id(id, 22);
        let input = [msg1.clone(), msg2.clone()].concat();
        let mut decoder = ReaderBuilder::new()
            .with_batch_size(8)
            .with_reader_schema(reader_schema)
            .with_writer_schema_store(store)
            .with_active_fingerprint(Fingerprint::Id(id))
            .build_decoder()
            .unwrap();
        let _ = decoder.decode(&input).unwrap();
        let batch = decoder.flush().unwrap().expect("batch");
        assert_eq!(batch.num_rows(), 2);
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(col.value(0), 21);
        assert_eq!(col.value(1), 22);
    }

    #[test]
    fn test_unknown_id_fingerprint_is_error() {
        let writer_schema = make_value_schema(PrimitiveType::Int);
        let id_known = 7u32;
        let id_unknown = 9u32;
        let mut store = SchemaStore::new_with_type(FingerprintAlgorithm::None);
        let _ = store
            .set(Fingerprint::Id(id_known), writer_schema.clone())
            .expect("set id schema");
        let mut decoder = ReaderBuilder::new()
            .with_batch_size(8)
            .with_reader_schema(writer_schema)
            .with_writer_schema_store(store)
            .with_active_fingerprint(Fingerprint::Id(id_known))
            .build_decoder()
            .unwrap();
        let prefix = make_id_prefix(id_unknown, 0);
        let err = decoder.decode(&prefix).expect_err("decode should error");
        let msg = err.to_string();
        assert!(
            msg.contains("Unknown fingerprint"),
            "unexpected message: {msg}"
        );
    }

    #[test]
    fn test_handle_prefix_id_incomplete_magic() {
        let writer_schema = make_value_schema(PrimitiveType::Int);
        let id = 5u32;
        let mut store = SchemaStore::new_with_type(FingerprintAlgorithm::None);
        let _ = store
            .set(Fingerprint::Id(id), writer_schema.clone())
            .expect("set id schema");
        let mut decoder = ReaderBuilder::new()
            .with_batch_size(8)
            .with_reader_schema(writer_schema)
            .with_writer_schema_store(store)
            .with_active_fingerprint(Fingerprint::Id(id))
            .build_decoder()
            .unwrap();
        let buf = &crate::schema::CONFLUENT_MAGIC[..0]; // empty incomplete magic
        let res = decoder.handle_prefix(buf).unwrap();
        assert_eq!(res, Some(0));
        assert!(decoder.pending_schema.is_none());
    }

    fn test_split_message_across_chunks() {
        let writer_schema = make_value_schema(PrimitiveType::Int);
        let reader_schema = writer_schema.clone();
        let mut store = SchemaStore::new();
        let fp = store.register(writer_schema).unwrap();
        let msg1 = make_message(fp, 7);
        let msg2 = make_message(fp, 8);
        let msg3 = make_message(fp, 9);
        let (pref2, body2) = msg2.split_at(10);
        let (pref3, body3) = msg3.split_at(10);
        let mut decoder = ReaderBuilder::new()
            .with_batch_size(8)
            .with_reader_schema(reader_schema)
            .with_writer_schema_store(store)
            .with_active_fingerprint(fp)
            .build_decoder()
            .unwrap();
        let _ = decoder.decode(&msg1).unwrap();
        let batch1 = decoder.flush().unwrap().expect("batch1");
        assert_eq!(batch1.num_rows(), 1);
        assert_eq!(
            batch1
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            7
        );
        let _ = decoder.decode(pref2).unwrap();
        assert!(decoder.flush().unwrap().is_none());
        let mut chunk3 = Vec::from(body2);
        chunk3.extend_from_slice(pref3);
        let _ = decoder.decode(&chunk3).unwrap();
        let batch2 = decoder.flush().unwrap().expect("batch2");
        assert_eq!(batch2.num_rows(), 1);
        assert_eq!(
            batch2
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            8
        );
        let _ = decoder.decode(body3).unwrap();
        let batch3 = decoder.flush().unwrap().expect("batch3");
        assert_eq!(batch3.num_rows(), 1);
        assert_eq!(
            batch3
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            9
        );
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
            let avro_schema = AvroSchema::new(test.schema.to_string());
            let mut store = SchemaStore::new();
            let fp = store.register(avro_schema.clone()).unwrap();
            let prefix = make_prefix(fp);
            let record_val = "some_string";
            let mut body = prefix;
            body.push((record_val.len() as u8) << 1);
            body.extend_from_slice(record_val.as_bytes());
            let decoder_res = ReaderBuilder::new()
                .with_batch_size(1)
                .with_writer_schema_store(store)
                .with_active_fingerprint(fp)
                .build_decoder();
            let decoder = match decoder_res {
                Ok(d) => d,
                Err(e) => {
                    if let Some(expected) = test.expected_error {
                        assert!(
                            e.to_string().contains(expected),
                            "Test '{}' failed at build – expected '{expected}', got '{e}'",
                            test.name
                        );
                        continue;
                    } else {
                        panic!("Test '{}' failed during build: {e}", test.name);
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
                    assert_eq!(batch, expected_batch, "Test '{}'", test.name);
                }
                (Err(e), Some(expected)) => {
                    assert!(
                        e.to_string().contains(expected),
                        "Test '{}' – expected error containing '{expected}', got '{e}'",
                        test.name
                    );
                }
                (Ok(_), Some(expected)) => {
                    panic!(
                        "Test '{}' expected failure ('{expected}') but succeeded",
                        test.name
                    );
                }
                (Err(e), None) => {
                    panic!("Test '{}' unexpectedly failed with '{e}'", test.name);
                }
            }
        }
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

    fn make_reader_schema_with_default_fields(
        path: &str,
        default_fields: Vec<Value>,
    ) -> AvroSchema {
        let mut root = load_writer_schema_json(path);
        assert_eq!(root["type"], "record", "writer schema must be a record");
        root.as_object_mut()
            .expect("schema is a JSON object")
            .insert("fields".to_string(), Value::Array(default_fields));
        AvroSchema::new(root.to_string())
    }

    #[test]
    fn test_schema_resolution_defaults_all_supported_types() {
        let path = "test/data/skippable_types.avro";
        let duration_default = "\u{0000}".repeat(12);
        let reader_schema = make_reader_schema_with_default_fields(
            path,
            vec![
                serde_json::json!({"name":"d_bool","type":"boolean","default":true}),
                serde_json::json!({"name":"d_int","type":"int","default":42}),
                serde_json::json!({"name":"d_long","type":"long","default":12345}),
                serde_json::json!({"name":"d_float","type":"float","default":1.5}),
                serde_json::json!({"name":"d_double","type":"double","default":2.25}),
                serde_json::json!({"name":"d_bytes","type":"bytes","default":"XYZ"}),
                serde_json::json!({"name":"d_string","type":"string","default":"hello"}),
                serde_json::json!({"name":"d_date","type":{"type":"int","logicalType":"date"},"default":0}),
                serde_json::json!({"name":"d_time_ms","type":{"type":"int","logicalType":"time-millis"},"default":1000}),
                serde_json::json!({"name":"d_time_us","type":{"type":"long","logicalType":"time-micros"},"default":2000}),
                serde_json::json!({"name":"d_ts_ms","type":{"type":"long","logicalType":"local-timestamp-millis"},"default":0}),
                serde_json::json!({"name":"d_ts_us","type":{"type":"long","logicalType":"local-timestamp-micros"},"default":0}),
                serde_json::json!({"name":"d_decimal","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":2},"default":""}),
                serde_json::json!({"name":"d_fixed","type":{"type":"fixed","name":"F4","size":4},"default":"ABCD"}),
                serde_json::json!({"name":"d_enum","type":{"type":"enum","name":"E","symbols":["A","B","C"]},"default":"A"}),
                serde_json::json!({"name":"d_duration","type":{"type":"fixed","name":"Dur","size":12,"logicalType":"duration"},"default":duration_default}),
                serde_json::json!({"name":"d_uuid","type":{"type":"string","logicalType":"uuid"},"default":"00000000-0000-0000-0000-000000000000"}),
                serde_json::json!({"name":"d_array","type":{"type":"array","items":"int"},"default":[1,2,3]}),
                serde_json::json!({"name":"d_map","type":{"type":"map","values":"long"},"default":{"a":1,"b":2}}),
                serde_json::json!({"name":"d_record","type":{
              "type":"record","name":"DefaultRec","fields":[
                  {"name":"x","type":"int"},
                  {"name":"y","type":["null","string"],"default":null}
              ]
        },"default":{"x":7}}),
                serde_json::json!({"name":"d_nullable_null","type":["null","int"],"default":null}),
                serde_json::json!({"name":"d_nullable_value","type":["int","null"],"default":123}),
            ],
        );
        let actual = read_alltypes_with_reader_schema(path, reader_schema);
        let num_rows = actual.num_rows();
        assert!(num_rows > 0, "skippable_types.avro should contain rows");
        assert_eq!(
            actual.num_columns(),
            22,
            "expected exactly our defaulted fields"
        );
        let mut arrays: Vec<Arc<dyn Array>> = Vec::with_capacity(22);
        arrays.push(Arc::new(BooleanArray::from_iter(std::iter::repeat_n(
            Some(true),
            num_rows,
        ))));
        arrays.push(Arc::new(Int32Array::from_iter_values(std::iter::repeat_n(
            42, num_rows,
        ))));
        arrays.push(Arc::new(Int64Array::from_iter_values(std::iter::repeat_n(
            12345, num_rows,
        ))));
        arrays.push(Arc::new(Float32Array::from_iter_values(
            std::iter::repeat_n(1.5f32, num_rows),
        )));
        arrays.push(Arc::new(Float64Array::from_iter_values(
            std::iter::repeat_n(2.25f64, num_rows),
        )));
        arrays.push(Arc::new(BinaryArray::from_iter_values(
            std::iter::repeat_n(b"XYZ".as_ref(), num_rows),
        )));
        arrays.push(Arc::new(StringArray::from_iter_values(
            std::iter::repeat_n("hello", num_rows),
        )));
        arrays.push(Arc::new(Date32Array::from_iter_values(
            std::iter::repeat_n(0, num_rows),
        )));
        arrays.push(Arc::new(Time32MillisecondArray::from_iter_values(
            std::iter::repeat_n(1_000, num_rows),
        )));
        arrays.push(Arc::new(Time64MicrosecondArray::from_iter_values(
            std::iter::repeat_n(2_000i64, num_rows),
        )));
        arrays.push(Arc::new(TimestampMillisecondArray::from_iter_values(
            std::iter::repeat_n(0i64, num_rows),
        )));
        arrays.push(Arc::new(TimestampMicrosecondArray::from_iter_values(
            std::iter::repeat_n(0i64, num_rows),
        )));
        #[cfg(feature = "small_decimals")]
        let decimal = Decimal64Array::from_iter_values(std::iter::repeat_n(0i64, num_rows))
            .with_precision_and_scale(10, 2)
            .unwrap();
        #[cfg(not(feature = "small_decimals"))]
        let decimal = Decimal128Array::from_iter_values(std::iter::repeat_n(0i128, num_rows))
            .with_precision_and_scale(10, 2)
            .unwrap();
        arrays.push(Arc::new(decimal));
        let fixed_iter = std::iter::repeat_n(Some(*b"ABCD"), num_rows);
        arrays.push(Arc::new(
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(fixed_iter, 4).unwrap(),
        ));
        let enum_keys = Int32Array::from_iter_values(std::iter::repeat_n(0, num_rows));
        let enum_values = StringArray::from_iter_values(["A", "B", "C"]);
        let enum_arr =
            DictionaryArray::<Int32Type>::try_new(enum_keys, Arc::new(enum_values)).unwrap();
        arrays.push(Arc::new(enum_arr));
        let duration_values = std::iter::repeat_n(
            Some(IntervalMonthDayNanoType::make_value(0, 0, 0)),
            num_rows,
        );
        let duration_arr: IntervalMonthDayNanoArray = duration_values.collect();
        arrays.push(Arc::new(duration_arr));
        let uuid_bytes = [0u8; 16];
        let uuid_iter = std::iter::repeat_n(Some(uuid_bytes), num_rows);
        arrays.push(Arc::new(
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(uuid_iter, 16).unwrap(),
        ));
        let item_field = Arc::new(Field::new(
            Field::LIST_FIELD_DEFAULT_NAME,
            DataType::Int32,
            false,
        ));
        let mut list_builder = ListBuilder::new(Int32Builder::new()).with_field(item_field);
        for _ in 0..num_rows {
            list_builder.values().append_value(1);
            list_builder.values().append_value(2);
            list_builder.values().append_value(3);
            list_builder.append(true);
        }
        arrays.push(Arc::new(list_builder.finish()));
        let values_field = Arc::new(Field::new("value", DataType::Int64, false));
        let mut map_builder = MapBuilder::new(
            Some(builder::MapFieldNames {
                entry: "entries".to_string(),
                key: "key".to_string(),
                value: "value".to_string(),
            }),
            StringBuilder::new(),
            Int64Builder::new(),
        )
        .with_values_field(values_field);
        for _ in 0..num_rows {
            let (keys, vals) = map_builder.entries();
            keys.append_value("a");
            vals.append_value(1);
            keys.append_value("b");
            vals.append_value(2);
            map_builder.append(true).unwrap();
        }
        arrays.push(Arc::new(map_builder.finish()));
        let rec_fields: Fields = Fields::from(vec![
            Field::new("x", DataType::Int32, false),
            Field::new("y", DataType::Utf8, true),
        ]);
        let mut sb = StructBuilder::new(
            rec_fields.clone(),
            vec![
                Box::new(Int32Builder::new()),
                Box::new(StringBuilder::new()),
            ],
        );
        for _ in 0..num_rows {
            sb.field_builder::<Int32Builder>(0).unwrap().append_value(7);
            sb.field_builder::<StringBuilder>(1).unwrap().append_null();
            sb.append(true);
        }
        arrays.push(Arc::new(sb.finish()));
        arrays.push(Arc::new(Int32Array::from_iter(std::iter::repeat_n(
            None::<i32>,
            num_rows,
        ))));
        arrays.push(Arc::new(Int32Array::from_iter_values(std::iter::repeat_n(
            123, num_rows,
        ))));
        let expected = RecordBatch::try_new(actual.schema(), arrays).unwrap();
        assert_eq!(
            actual, expected,
            "defaults should materialize correctly for all fields"
        );
    }

    #[test]
    fn test_schema_resolution_default_enum_invalid_symbol_errors() {
        let path = "test/data/skippable_types.avro";
        let bad_schema = make_reader_schema_with_default_fields(
            path,
            vec![serde_json::json!({
                "name":"bad_enum",
                "type":{"type":"enum","name":"E","symbols":["A","B","C"]},
                "default":"Z"
            })],
        );
        let file = File::open(path).unwrap();
        let res = ReaderBuilder::new()
            .with_reader_schema(bad_schema)
            .build(BufReader::new(file));
        let err = res.expect_err("expected enum default validation to fail");
        let msg = err.to_string();
        let lower_msg = msg.to_lowercase();
        assert!(
            lower_msg.contains("enum")
                && (lower_msg.contains("symbol") || lower_msg.contains("default")),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn test_schema_resolution_default_fixed_size_mismatch_errors() {
        let path = "test/data/skippable_types.avro";
        let bad_schema = make_reader_schema_with_default_fields(
            path,
            vec![serde_json::json!({
                "name":"bad_fixed",
                "type":{"type":"fixed","name":"F","size":4},
                "default":"ABC"
            })],
        );
        let file = File::open(path).unwrap();
        let res = ReaderBuilder::new()
            .with_reader_schema(bad_schema)
            .build(BufReader::new(file));
        let err = res.expect_err("expected fixed default validation to fail");
        let msg = err.to_string();
        let lower_msg = msg.to_lowercase();
        assert!(
            lower_msg.contains("fixed")
                && (lower_msg.contains("size")
                    || lower_msg.contains("length")
                    || lower_msg.contains("does not match")),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn test_alltypes_skip_writer_fields_keep_double_only() {
        let file = arrow_test_data("avro/alltypes_plain.avro");
        let reader_schema =
            make_reader_schema_with_selected_fields_in_order(&file, &["double_col"]);
        let batch = read_alltypes_with_reader_schema(&file, reader_schema);
        let expected = RecordBatch::try_from_iter_with_nullable([(
            "double_col",
            Arc::new(Float64Array::from_iter_values(
                (0..8).map(|x| (x % 2) as f64 * 10.1),
            )) as _,
            true,
        )])
        .unwrap();
        assert_eq!(batch, expected);
    }

    #[test]
    fn test_alltypes_skip_writer_fields_reorder_and_skip_many() {
        let file = arrow_test_data("avro/alltypes_plain.avro");
        let reader_schema =
            make_reader_schema_with_selected_fields_in_order(&file, &["timestamp_col", "id"]);
        let batch = read_alltypes_with_reader_schema(&file, reader_schema);
        let expected = RecordBatch::try_from_iter_with_nullable([
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
            (
                "id",
                Arc::new(Int32Array::from(vec![4, 5, 6, 7, 2, 3, 0, 1])) as _,
                true,
            ),
        ])
        .unwrap();
        assert_eq!(batch, expected);
    }

    #[test]
    fn test_skippable_types_project_each_field_individually() {
        let path = "test/data/skippable_types.avro";
        let full = read_file(path, 1024, false);
        let schema_full = full.schema();
        let num_rows = full.num_rows();
        let writer_json = load_writer_schema_json(path);
        assert_eq!(
            writer_json["type"], "record",
            "writer schema must be a record"
        );
        let fields_json = writer_json
            .get("fields")
            .and_then(|f| f.as_array())
            .expect("record has fields");
        assert_eq!(
            schema_full.fields().len(),
            fields_json.len(),
            "full read column count vs writer fields"
        );
        for (idx, f) in fields_json.iter().enumerate() {
            let name = f
                .get("name")
                .and_then(|n| n.as_str())
                .unwrap_or_else(|| panic!("field at index {idx} has no name"));
            let reader_schema = make_reader_schema_with_selected_fields_in_order(path, &[name]);
            let projected = read_alltypes_with_reader_schema(path, reader_schema);
            assert_eq!(
                projected.num_columns(),
                1,
                "projected batch should contain exactly the selected column '{name}'"
            );
            assert_eq!(
                projected.num_rows(),
                num_rows,
                "row count mismatch for projected column '{name}'"
            );
            let field = schema_full.field(idx).clone();
            let col = full.column(idx).clone();
            let expected =
                RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![col]).unwrap();
            // Equality means: (1) read the right column values; and (2) all other
            // writer fields were skipped correctly for this projection (no misalignment).
            assert_eq!(
                projected, expected,
                "projected column '{name}' mismatch vs full read column"
            );
        }
    }

    #[test]
    fn test_union_fields_avro_nullable_and_general_unions() {
        let path = "test/data/union_fields.avro";
        let batch = read_file(path, 1024, false);
        let schema = batch.schema();
        let idx = schema.index_of("nullable_int_nullfirst").unwrap();
        let a = batch
            .column(idx)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("nullable_int_nullfirst should be Int32");
        assert_eq!(a.len(), 4);
        assert!(a.is_null(0));
        assert_eq!(a.value(1), 42);
        assert!(a.is_null(2));
        assert_eq!(a.value(3), 0);
        let idx = schema.index_of("nullable_string_nullsecond").unwrap();
        let s = batch
            .column(idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("nullable_string_nullsecond should be Utf8");
        assert_eq!(s.len(), 4);
        assert_eq!(s.value(0), "s1");
        assert!(s.is_null(1));
        assert_eq!(s.value(2), "s3");
        assert!(s.is_valid(3)); // empty string, not null
        assert_eq!(s.value(3), "");
        let idx = schema.index_of("union_prim").unwrap();
        let u = batch
            .column(idx)
            .as_any()
            .downcast_ref::<UnionArray>()
            .expect("union_prim should be Union");
        let fields = match u.data_type() {
            DataType::Union(fields, mode) => {
                assert!(matches!(mode, UnionMode::Dense), "expect dense unions");
                fields
            }
            other => panic!("expected Union, got {other:?}"),
        };
        let tid_by_name = |name: &str| -> i8 {
            for (tid, f) in fields.iter() {
                if f.name() == name {
                    return tid;
                }
            }
            panic!("union child '{name}' not found");
        };
        let expected_type_ids = vec![
            tid_by_name("long"),
            tid_by_name("int"),
            tid_by_name("float"),
            tid_by_name("double"),
        ];
        let type_ids: Vec<i8> = u.type_ids().iter().copied().collect();
        assert_eq!(
            type_ids, expected_type_ids,
            "branch selection for union_prim rows"
        );
        let longs = u
            .child(tid_by_name("long"))
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(longs.len(), 1);
        let ints = u
            .child(tid_by_name("int"))
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ints.len(), 1);
        let floats = u
            .child(tid_by_name("float"))
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert_eq!(floats.len(), 1);
        let doubles = u
            .child(tid_by_name("double"))
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(doubles.len(), 1);
        let idx = schema.index_of("union_bytes_vs_string").unwrap();
        let u = batch
            .column(idx)
            .as_any()
            .downcast_ref::<UnionArray>()
            .expect("union_bytes_vs_string should be Union");
        let fields = match u.data_type() {
            DataType::Union(fields, _) => fields,
            other => panic!("expected Union, got {other:?}"),
        };
        let tid_by_name = |name: &str| -> i8 {
            for (tid, f) in fields.iter() {
                if f.name() == name {
                    return tid;
                }
            }
            panic!("union child '{name}' not found");
        };
        let tid_bytes = tid_by_name("bytes");
        let tid_string = tid_by_name("string");
        let type_ids: Vec<i8> = u.type_ids().iter().copied().collect();
        assert_eq!(
            type_ids,
            vec![tid_bytes, tid_string, tid_string, tid_bytes],
            "branch selection for bytes/string union"
        );
        let s_child = u
            .child(tid_string)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(s_child.len(), 2);
        assert_eq!(s_child.value(0), "hello");
        assert_eq!(s_child.value(1), "world");
        let b_child = u
            .child(tid_bytes)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(b_child.len(), 2);
        assert_eq!(b_child.value(0), &[0x00, 0xFF, 0x7F]);
        assert_eq!(b_child.value(1), b""); // previously: &[]
        let idx = schema.index_of("union_enum_records_array_map").unwrap();
        let u = batch
            .column(idx)
            .as_any()
            .downcast_ref::<UnionArray>()
            .expect("union_enum_records_array_map should be Union");
        let fields = match u.data_type() {
            DataType::Union(fields, _) => fields,
            other => panic!("expected Union, got {other:?}"),
        };
        let mut tid_enum: Option<i8> = None;
        let mut tid_rec_a: Option<i8> = None;
        let mut tid_rec_b: Option<i8> = None;
        let mut tid_array: Option<i8> = None;
        let mut tid_map: Option<i8> = None;
        for (tid, f) in fields.iter() {
            match f.data_type() {
                DataType::Dictionary(_, _) => tid_enum = Some(tid),
                DataType::Struct(childs) => {
                    if childs.len() == 2 && childs[0].name() == "a" && childs[1].name() == "b" {
                        tid_rec_a = Some(tid);
                    } else if childs.len() == 2
                        && childs[0].name() == "x"
                        && childs[1].name() == "y"
                    {
                        tid_rec_b = Some(tid);
                    }
                }
                DataType::List(_) => tid_array = Some(tid),
                DataType::Map(_, _) => tid_map = Some(tid),
                _ => {}
            }
        }
        let (tid_enum, tid_rec_a, tid_rec_b, tid_array) = (
            tid_enum.expect("enum child"),
            tid_rec_a.expect("RecA child"),
            tid_rec_b.expect("RecB child"),
            tid_array.expect("array<long> child"),
        );
        let type_ids: Vec<i8> = u.type_ids().iter().copied().collect();
        assert_eq!(
            type_ids,
            vec![tid_enum, tid_rec_a, tid_rec_b, tid_array],
            "branch selection for complex union"
        );
        let dict = u
            .child(tid_enum)
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();
        assert_eq!(dict.len(), 1);
        assert!(dict.is_valid(0));
        let rec_a = u
            .child(tid_rec_a)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(rec_a.len(), 1);
        let a_val = rec_a
            .column_by_name("a")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(a_val.value(0), 7);
        let b_val = rec_a
            .column_by_name("b")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(b_val.value(0), "x");
        // RecB row: {"x": 123456789, "y": b"\xFF\x00"}
        let rec_b = u
            .child(tid_rec_b)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let x_val = rec_b
            .column_by_name("x")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(x_val.value(0), 123_456_789_i64);
        let y_val = rec_b
            .column_by_name("y")
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(y_val.value(0), &[0xFF, 0x00]);
        let arr = u
            .child(tid_array)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(arr.len(), 1);
        let first_values = arr.value(0);
        let longs = first_values.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(longs.len(), 3);
        assert_eq!(longs.value(0), 1);
        assert_eq!(longs.value(1), 2);
        assert_eq!(longs.value(2), 3);
        let idx = schema.index_of("union_date_or_fixed4").unwrap();
        let u = batch
            .column(idx)
            .as_any()
            .downcast_ref::<UnionArray>()
            .expect("union_date_or_fixed4 should be Union");
        let fields = match u.data_type() {
            DataType::Union(fields, _) => fields,
            other => panic!("expected Union, got {other:?}"),
        };
        let mut tid_date: Option<i8> = None;
        let mut tid_fixed: Option<i8> = None;
        for (tid, f) in fields.iter() {
            match f.data_type() {
                DataType::Date32 => tid_date = Some(tid),
                DataType::FixedSizeBinary(4) => tid_fixed = Some(tid),
                _ => {}
            }
        }
        let (tid_date, tid_fixed) = (tid_date.expect("date"), tid_fixed.expect("fixed(4)"));
        let type_ids: Vec<i8> = u.type_ids().iter().copied().collect();
        assert_eq!(
            type_ids,
            vec![tid_date, tid_fixed, tid_date, tid_fixed],
            "branch selection for date/fixed4 union"
        );
        let dates = u
            .child(tid_date)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        assert_eq!(dates.len(), 2);
        assert_eq!(dates.value(0), 19_000); // ~2022‑01‑15
        assert_eq!(dates.value(1), 0); // epoch
        let fixed = u
            .child(tid_fixed)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(fixed.len(), 2);
        assert_eq!(fixed.value(0), b"ABCD");
        assert_eq!(fixed.value(1), &[0x00, 0x11, 0x22, 0x33]);
    }

    #[test]
    fn test_union_schema_resolution_all_type_combinations() {
        let path = "test/data/union_fields.avro";
        let baseline = read_file(path, 1024, false);
        let baseline_schema = baseline.schema();
        let mut root = load_writer_schema_json(path);
        assert_eq!(root["type"], "record", "writer schema must be a record");
        let fields = root
            .get_mut("fields")
            .and_then(|f| f.as_array_mut())
            .expect("record has fields");
        fn is_named_type(obj: &Value, ty: &str, nm: &str) -> bool {
            obj.get("type").and_then(|v| v.as_str()) == Some(ty)
                && obj.get("name").and_then(|v| v.as_str()) == Some(nm)
        }
        fn is_logical(obj: &Value, prim: &str, lt: &str) -> bool {
            obj.get("type").and_then(|v| v.as_str()) == Some(prim)
                && obj.get("logicalType").and_then(|v| v.as_str()) == Some(lt)
        }
        fn find_first(arr: &[Value], pred: impl Fn(&Value) -> bool) -> Option<Value> {
            arr.iter().find(|v| pred(v)).cloned()
        }
        fn prim(s: &str) -> Value {
            Value::String(s.to_string())
        }
        for f in fields.iter_mut() {
            let Some(name) = f.get("name").and_then(|n| n.as_str()) else {
                continue;
            };
            match name {
                // Flip null ordering – should not affect values
                "nullable_int_nullfirst" => {
                    f["type"] = json!(["int", "null"]);
                }
                "nullable_string_nullsecond" => {
                    f["type"] = json!(["null", "string"]);
                }
                "union_prim" => {
                    let orig = f["type"].as_array().unwrap().clone();
                    let long = prim("long");
                    let double = prim("double");
                    let string = prim("string");
                    let bytes = prim("bytes");
                    let boolean = prim("boolean");
                    assert!(orig.contains(&long));
                    assert!(orig.contains(&double));
                    assert!(orig.contains(&string));
                    assert!(orig.contains(&bytes));
                    assert!(orig.contains(&boolean));
                    f["type"] = json!([long, double, string, bytes, boolean]);
                }
                "union_bytes_vs_string" => {
                    f["type"] = json!(["string", "bytes"]);
                }
                "union_fixed_dur_decfix" => {
                    let orig = f["type"].as_array().unwrap().clone();
                    let fx8 = find_first(&orig, |o| is_named_type(o, "fixed", "Fx8")).unwrap();
                    let dur12 = find_first(&orig, |o| is_named_type(o, "fixed", "Dur12")).unwrap();
                    let decfix16 =
                        find_first(&orig, |o| is_named_type(o, "fixed", "DecFix16")).unwrap();
                    f["type"] = json!([decfix16, dur12, fx8]);
                }
                "union_enum_records_array_map" => {
                    let orig = f["type"].as_array().unwrap().clone();
                    let enum_color = find_first(&orig, |o| {
                        o.get("type").and_then(|v| v.as_str()) == Some("enum")
                    })
                    .unwrap();
                    let rec_a = find_first(&orig, |o| is_named_type(o, "record", "RecA")).unwrap();
                    let rec_b = find_first(&orig, |o| is_named_type(o, "record", "RecB")).unwrap();
                    let arr = find_first(&orig, |o| {
                        o.get("type").and_then(|v| v.as_str()) == Some("array")
                    })
                    .unwrap();
                    let map = find_first(&orig, |o| {
                        o.get("type").and_then(|v| v.as_str()) == Some("map")
                    })
                    .unwrap();
                    f["type"] = json!([arr, map, rec_b, rec_a, enum_color]);
                }
                "union_date_or_fixed4" => {
                    let orig = f["type"].as_array().unwrap().clone();
                    let date = find_first(&orig, |o| is_logical(o, "int", "date")).unwrap();
                    let fx4 = find_first(&orig, |o| is_named_type(o, "fixed", "Fx4")).unwrap();
                    f["type"] = json!([fx4, date]);
                }
                "union_time_millis_or_enum" => {
                    let orig = f["type"].as_array().unwrap().clone();
                    let time_ms =
                        find_first(&orig, |o| is_logical(o, "int", "time-millis")).unwrap();
                    let en = find_first(&orig, |o| {
                        o.get("type").and_then(|v| v.as_str()) == Some("enum")
                    })
                    .unwrap();
                    f["type"] = json!([en, time_ms]);
                }
                "union_time_micros_or_string" => {
                    let orig = f["type"].as_array().unwrap().clone();
                    let time_us =
                        find_first(&orig, |o| is_logical(o, "long", "time-micros")).unwrap();
                    f["type"] = json!(["string", time_us]);
                }
                "union_ts_millis_utc_or_array" => {
                    let orig = f["type"].as_array().unwrap().clone();
                    let ts_ms =
                        find_first(&orig, |o| is_logical(o, "long", "timestamp-millis")).unwrap();
                    let arr = find_first(&orig, |o| {
                        o.get("type").and_then(|v| v.as_str()) == Some("array")
                    })
                    .unwrap();
                    f["type"] = json!([arr, ts_ms]);
                }
                "union_ts_micros_local_or_bytes" => {
                    let orig = f["type"].as_array().unwrap().clone();
                    let lts_us =
                        find_first(&orig, |o| is_logical(o, "long", "local-timestamp-micros"))
                            .unwrap();
                    f["type"] = json!(["bytes", lts_us]);
                }
                "union_uuid_or_fixed10" => {
                    let orig = f["type"].as_array().unwrap().clone();
                    let uuid = find_first(&orig, |o| is_logical(o, "string", "uuid")).unwrap();
                    let fx10 = find_first(&orig, |o| is_named_type(o, "fixed", "Fx10")).unwrap();
                    f["type"] = json!([fx10, uuid]);
                }
                "union_dec_bytes_or_dec_fixed" => {
                    let orig = f["type"].as_array().unwrap().clone();
                    let dec_bytes = find_first(&orig, |o| {
                        o.get("type").and_then(|v| v.as_str()) == Some("bytes")
                            && o.get("logicalType").and_then(|v| v.as_str()) == Some("decimal")
                    })
                    .unwrap();
                    let dec_fix = find_first(&orig, |o| {
                        is_named_type(o, "fixed", "DecFix20")
                            && o.get("logicalType").and_then(|v| v.as_str()) == Some("decimal")
                    })
                    .unwrap();
                    f["type"] = json!([dec_fix, dec_bytes]);
                }
                "union_null_bytes_string" => {
                    f["type"] = json!(["bytes", "string", "null"]);
                }
                "array_of_union" => {
                    let obj = f
                        .get_mut("type")
                        .expect("array type")
                        .as_object_mut()
                        .unwrap();
                    obj.insert("items".to_string(), json!(["string", "long"]));
                }
                "map_of_union" => {
                    let obj = f
                        .get_mut("type")
                        .expect("map type")
                        .as_object_mut()
                        .unwrap();
                    obj.insert("values".to_string(), json!(["double", "null"]));
                }
                "record_with_union_field" => {
                    let rec = f
                        .get_mut("type")
                        .expect("record type")
                        .as_object_mut()
                        .unwrap();
                    let rec_fields = rec.get_mut("fields").unwrap().as_array_mut().unwrap();
                    let mut found = false;
                    for rf in rec_fields.iter_mut() {
                        if rf.get("name").and_then(|v| v.as_str()) == Some("u") {
                            rf["type"] = json!(["string", "long"]); // rely on int→long promotion
                            found = true;
                            break;
                        }
                    }
                    assert!(found, "field 'u' expected in HasUnion");
                }
                "union_ts_micros_utc_or_map" => {
                    let orig = f["type"].as_array().unwrap().clone();
                    let ts_us =
                        find_first(&orig, |o| is_logical(o, "long", "timestamp-micros")).unwrap();
                    let map = find_first(&orig, |o| {
                        o.get("type").and_then(|v| v.as_str()) == Some("map")
                    })
                    .unwrap();
                    f["type"] = json!([map, ts_us]);
                }
                "union_ts_millis_local_or_string" => {
                    let orig = f["type"].as_array().unwrap().clone();
                    let lts_ms =
                        find_first(&orig, |o| is_logical(o, "long", "local-timestamp-millis"))
                            .unwrap();
                    f["type"] = json!(["string", lts_ms]);
                }
                "union_bool_or_string" => {
                    f["type"] = json!(["string", "boolean"]);
                }
                _ => {}
            }
        }
        let reader_schema = AvroSchema::new(root.to_string());
        let resolved = read_alltypes_with_reader_schema(path, reader_schema);

        fn branch_token(dt: &DataType) -> String {
            match dt {
                DataType::Null => "null".into(),
                DataType::Boolean => "boolean".into(),
                DataType::Int32 => "int".into(),
                DataType::Int64 => "long".into(),
                DataType::Float32 => "float".into(),
                DataType::Float64 => "double".into(),
                DataType::Binary => "bytes".into(),
                DataType::Utf8 => "string".into(),
                DataType::Date32 => "date".into(),
                DataType::Time32(arrow_schema::TimeUnit::Millisecond) => "time-millis".into(),
                DataType::Time64(arrow_schema::TimeUnit::Microsecond) => "time-micros".into(),
                DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, tz) => if tz.is_some() {
                    "timestamp-millis"
                } else {
                    "local-timestamp-millis"
                }
                .into(),
                DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, tz) => if tz.is_some() {
                    "timestamp-micros"
                } else {
                    "local-timestamp-micros"
                }
                .into(),
                DataType::Interval(IntervalUnit::MonthDayNano) => "duration".into(),
                DataType::FixedSizeBinary(n) => format!("fixed{n}"),
                DataType::Dictionary(_, _) => "enum".into(),
                DataType::Decimal128(p, s) => format!("decimal({p},{s})"),
                DataType::Decimal256(p, s) => format!("decimal({p},{s})"),
                #[cfg(feature = "small_decimals")]
                DataType::Decimal64(p, s) => format!("decimal({p},{s})"),
                DataType::Struct(fields) => {
                    if fields.len() == 2 && fields[0].name() == "a" && fields[1].name() == "b" {
                        "record:RecA".into()
                    } else if fields.len() == 2
                        && fields[0].name() == "x"
                        && fields[1].name() == "y"
                    {
                        "record:RecB".into()
                    } else {
                        "record".into()
                    }
                }
                DataType::List(_) => "array".into(),
                DataType::Map(_, _) => "map".into(),
                other => format!("{other:?}"),
            }
        }

        fn union_tokens(u: &UnionArray) -> (Vec<i8>, HashMap<i8, String>) {
            let fields = match u.data_type() {
                DataType::Union(fields, _) => fields,
                other => panic!("expected Union, got {other:?}"),
            };
            let mut dict: HashMap<i8, String> = HashMap::with_capacity(fields.len());
            for (tid, f) in fields.iter() {
                dict.insert(tid, branch_token(f.data_type()));
            }
            let ids: Vec<i8> = u.type_ids().iter().copied().collect();
            (ids, dict)
        }

        fn expected_token(field_name: &str, writer_token: &str) -> String {
            match field_name {
                "union_prim" => match writer_token {
                    "int" => "long".into(),
                    "float" => "double".into(),
                    other => other.into(),
                },
                "record_with_union_field.u" => match writer_token {
                    "int" => "long".into(),
                    other => other.into(),
                },
                _ => writer_token.into(),
            }
        }

        fn get_union<'a>(
            rb: &'a RecordBatch,
            schema: arrow_schema::SchemaRef,
            fname: &str,
        ) -> &'a UnionArray {
            let idx = schema.index_of(fname).unwrap();
            rb.column(idx)
                .as_any()
                .downcast_ref::<UnionArray>()
                .unwrap_or_else(|| panic!("{fname} should be a Union"))
        }

        fn assert_union_equivalent(field_name: &str, u_writer: &UnionArray, u_reader: &UnionArray) {
            let (ids_w, dict_w) = union_tokens(u_writer);
            let (ids_r, dict_r) = union_tokens(u_reader);
            assert_eq!(
                ids_w.len(),
                ids_r.len(),
                "{field_name}: row count mismatch between baseline and resolved"
            );
            for (i, (id_w, id_r)) in ids_w.iter().zip(ids_r.iter()).enumerate() {
                let w_tok = dict_w.get(id_w).unwrap();
                let want = expected_token(field_name, w_tok);
                let got = dict_r.get(id_r).unwrap();
                assert_eq!(
                    got, &want,
                    "{field_name}: row {i} resolved to wrong union branch (writer={w_tok}, expected={want}, got={got})"
                );
            }
        }

        for (fname, dt) in [
            ("nullable_int_nullfirst", DataType::Int32),
            ("nullable_string_nullsecond", DataType::Utf8),
        ] {
            let idx_b = baseline_schema.index_of(fname).unwrap();
            let idx_r = resolved.schema().index_of(fname).unwrap();
            let col_b = baseline.column(idx_b);
            let col_r = resolved.column(idx_r);
            assert_eq!(
                col_b.data_type(),
                &dt,
                "baseline {fname} should decode as non-union with nullability"
            );
            assert_eq!(
                col_b.as_ref(),
                col_r.as_ref(),
                "{fname}: values must be identical regardless of null-branch order"
            );
        }
        let union_fields = [
            "union_prim",
            "union_bytes_vs_string",
            "union_fixed_dur_decfix",
            "union_enum_records_array_map",
            "union_date_or_fixed4",
            "union_time_millis_or_enum",
            "union_time_micros_or_string",
            "union_ts_millis_utc_or_array",
            "union_ts_micros_local_or_bytes",
            "union_uuid_or_fixed10",
            "union_dec_bytes_or_dec_fixed",
            "union_null_bytes_string",
            "union_ts_micros_utc_or_map",
            "union_ts_millis_local_or_string",
            "union_bool_or_string",
        ];
        for fname in union_fields {
            let u_b = get_union(&baseline, baseline_schema.clone(), fname);
            let u_r = get_union(&resolved, resolved.schema(), fname);
            assert_union_equivalent(fname, u_b, u_r);
        }
        {
            let fname = "array_of_union";
            let idx_b = baseline_schema.index_of(fname).unwrap();
            let idx_r = resolved.schema().index_of(fname).unwrap();
            let arr_b = baseline
                .column(idx_b)
                .as_any()
                .downcast_ref::<ListArray>()
                .expect("array_of_union should be a List");
            let arr_r = resolved
                .column(idx_r)
                .as_any()
                .downcast_ref::<ListArray>()
                .expect("array_of_union should be a List");
            assert_eq!(
                arr_b.value_offsets(),
                arr_r.value_offsets(),
                "{fname}: list offsets changed after resolution"
            );
            let u_b = arr_b
                .values()
                .as_any()
                .downcast_ref::<UnionArray>()
                .expect("array items should be Union");
            let u_r = arr_r
                .values()
                .as_any()
                .downcast_ref::<UnionArray>()
                .expect("array items should be Union");
            let (ids_b, dict_b) = union_tokens(u_b);
            let (ids_r, dict_r) = union_tokens(u_r);
            assert_eq!(ids_b.len(), ids_r.len(), "{fname}: values length mismatch");
            for (i, (id_b, id_r)) in ids_b.iter().zip(ids_r.iter()).enumerate() {
                let w_tok = dict_b.get(id_b).unwrap();
                let got = dict_r.get(id_r).unwrap();
                assert_eq!(
                    got, w_tok,
                    "{fname}: value {i} resolved to wrong branch (writer={w_tok}, got={got})"
                );
            }
        }
        {
            let fname = "map_of_union";
            let idx_b = baseline_schema.index_of(fname).unwrap();
            let idx_r = resolved.schema().index_of(fname).unwrap();
            let map_b = baseline
                .column(idx_b)
                .as_any()
                .downcast_ref::<MapArray>()
                .expect("map_of_union should be a Map");
            let map_r = resolved
                .column(idx_r)
                .as_any()
                .downcast_ref::<MapArray>()
                .expect("map_of_union should be a Map");
            assert_eq!(
                map_b.value_offsets(),
                map_r.value_offsets(),
                "{fname}: map value offsets changed after resolution"
            );
            let ent_b = map_b.entries();
            let ent_r = map_r.entries();
            let val_b_any = ent_b.column(1).as_ref();
            let val_r_any = ent_r.column(1).as_ref();
            let b_union = val_b_any.as_any().downcast_ref::<UnionArray>();
            let r_union = val_r_any.as_any().downcast_ref::<UnionArray>();
            if let (Some(u_b), Some(u_r)) = (b_union, r_union) {
                assert_union_equivalent(fname, u_b, u_r);
            } else {
                assert_eq!(
                    val_b_any.data_type(),
                    val_r_any.data_type(),
                    "{fname}: value data types differ after resolution"
                );
                assert_eq!(
                    val_b_any, val_r_any,
                    "{fname}: value arrays differ after resolution (nullable value column case)"
                );
                let value_nullable = |m: &MapArray| -> bool {
                    match m.data_type() {
                        DataType::Map(entries_field, _sorted) => match entries_field.data_type() {
                            DataType::Struct(fields) => {
                                assert_eq!(fields.len(), 2, "entries struct must have 2 fields");
                                assert_eq!(fields[0].name(), "key");
                                assert_eq!(fields[1].name(), "value");
                                fields[1].is_nullable()
                            }
                            other => panic!("Map entries field must be Struct, got {other:?}"),
                        },
                        other => panic!("expected Map data type, got {other:?}"),
                    }
                };
                assert!(
                    value_nullable(map_b),
                    "{fname}: baseline Map value field should be nullable per Arrow spec"
                );
                assert!(
                    value_nullable(map_r),
                    "{fname}: resolved Map value field should be nullable per Arrow spec"
                );
            }
        }
        {
            let fname = "record_with_union_field";
            let idx_b = baseline_schema.index_of(fname).unwrap();
            let idx_r = resolved.schema().index_of(fname).unwrap();
            let rec_b = baseline
                .column(idx_b)
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("record_with_union_field should be a Struct");
            let rec_r = resolved
                .column(idx_r)
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("record_with_union_field should be a Struct");
            let u_b = rec_b
                .column_by_name("u")
                .unwrap()
                .as_any()
                .downcast_ref::<UnionArray>()
                .expect("field 'u' should be Union (baseline)");
            let u_r = rec_r
                .column_by_name("u")
                .unwrap()
                .as_any()
                .downcast_ref::<UnionArray>()
                .expect("field 'u' should be Union (resolved)");
            assert_union_equivalent("record_with_union_field.u", u_b, u_r);
        }
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
                b"\x00" as &[u8],
                b"\x01" as &[u8],
                b"\x02" as &[u8],
                b"\x03" as &[u8],
                b"\x04" as &[u8],
                b"\x05" as &[u8],
                b"\x06" as &[u8],
                b"\x07" as &[u8],
                b"\x08" as &[u8],
                b"\t" as &[u8],
                b"\n" as &[u8],
                b"\x0b" as &[u8],
            ])) as Arc<dyn Array>,
            true,
        )])
        .unwrap();
        assert_eq!(batch, expected);
    }

    #[test]
    fn test_decimal() {
        // Choose expected Arrow types depending on the `small_decimals` feature flag.
        // With `small_decimals` enabled, Decimal32/Decimal64 are used where their
        // precision allows; otherwise, those cases resolve to Decimal128.
        #[cfg(feature = "small_decimals")]
        let files: [(&str, DataType); 8] = [
            (
                "avro/fixed_length_decimal.avro",
                DataType::Decimal128(25, 2),
            ),
            (
                "avro/fixed_length_decimal_legacy.avro",
                DataType::Decimal64(13, 2),
            ),
            ("avro/int32_decimal.avro", DataType::Decimal32(4, 2)),
            ("avro/int64_decimal.avro", DataType::Decimal64(10, 2)),
            (
                "test/data/int256_decimal.avro",
                DataType::Decimal256(76, 10),
            ),
            (
                "test/data/fixed256_decimal.avro",
                DataType::Decimal256(76, 10),
            ),
            (
                "test/data/fixed_length_decimal_legacy_32.avro",
                DataType::Decimal32(9, 2),
            ),
            ("test/data/int128_decimal.avro", DataType::Decimal128(38, 2)),
        ];
        #[cfg(not(feature = "small_decimals"))]
        let files: [(&str, DataType); 8] = [
            (
                "avro/fixed_length_decimal.avro",
                DataType::Decimal128(25, 2),
            ),
            (
                "avro/fixed_length_decimal_legacy.avro",
                DataType::Decimal128(13, 2),
            ),
            ("avro/int32_decimal.avro", DataType::Decimal128(4, 2)),
            ("avro/int64_decimal.avro", DataType::Decimal128(10, 2)),
            (
                "test/data/int256_decimal.avro",
                DataType::Decimal256(76, 10),
            ),
            (
                "test/data/fixed256_decimal.avro",
                DataType::Decimal256(76, 10),
            ),
            (
                "test/data/fixed_length_decimal_legacy_32.avro",
                DataType::Decimal128(9, 2),
            ),
            ("test/data/int128_decimal.avro", DataType::Decimal128(38, 2)),
        ];
        for (file, expected_dt) in files {
            let (precision, scale) = match expected_dt {
                DataType::Decimal32(p, s)
                | DataType::Decimal64(p, s)
                | DataType::Decimal128(p, s)
                | DataType::Decimal256(p, s) => (p, s),
                _ => unreachable!("Unexpected decimal type in test inputs"),
            };
            assert!(scale >= 0, "test data uses non-negative scales only");
            let scale_u32 = scale as u32;
            let file_path: String = if file.starts_with("avro/") {
                arrow_test_data(file)
            } else {
                std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join(file)
                    .to_string_lossy()
                    .into_owned()
            };
            let pow10: i128 = 10i128.pow(scale_u32);
            let values_i128: Vec<i128> = (1..=24).map(|n| (n as i128) * pow10).collect();
            let build_expected = |dt: &DataType, values: &[i128]| -> ArrayRef {
                match *dt {
                    #[cfg(feature = "small_decimals")]
                    DataType::Decimal32(p, s) => {
                        let it = values.iter().map(|&v| v as i32);
                        Arc::new(
                            Decimal32Array::from_iter_values(it)
                                .with_precision_and_scale(p, s)
                                .unwrap(),
                        )
                    }
                    #[cfg(feature = "small_decimals")]
                    DataType::Decimal64(p, s) => {
                        let it = values.iter().map(|&v| v as i64);
                        Arc::new(
                            Decimal64Array::from_iter_values(it)
                                .with_precision_and_scale(p, s)
                                .unwrap(),
                        )
                    }
                    DataType::Decimal128(p, s) => {
                        let it = values.iter().copied();
                        Arc::new(
                            Decimal128Array::from_iter_values(it)
                                .with_precision_and_scale(p, s)
                                .unwrap(),
                        )
                    }
                    DataType::Decimal256(p, s) => {
                        let it = values.iter().map(|&v| i256::from_i128(v));
                        Arc::new(
                            Decimal256Array::from_iter_values(it)
                                .with_precision_and_scale(p, s)
                                .unwrap(),
                        )
                    }
                    _ => unreachable!("Unexpected decimal type in test"),
                }
            };
            let actual_batch = read_file(&file_path, 8, false);
            let actual_nullable = actual_batch.schema().field(0).is_nullable();
            let expected_array = build_expected(&expected_dt, &values_i128);
            let mut meta = HashMap::new();
            meta.insert("precision".to_string(), precision.to_string());
            meta.insert("scale".to_string(), scale.to_string());
            let field =
                Field::new("value", expected_dt.clone(), actual_nullable).with_metadata(meta);
            let expected_schema = Arc::new(Schema::new(vec![field]));
            let expected_batch =
                RecordBatch::try_new(expected_schema.clone(), vec![expected_array]).unwrap();
            assert_eq!(
                actual_batch, expected_batch,
                "Decoded RecordBatch does not match for {file}"
            );
            let actual_batch_small = read_file(&file_path, 3, false);
            assert_eq!(
                actual_batch_small, expected_batch,
                "Decoded RecordBatch does not match for {file} with batch size 3"
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
                AVRO_ENUM_SYMBOLS_METADATA_KEY.to_string(),
                r#"["a","b","c","d"]"#.to_string(),
            );
            let f1_field = Field::new("f1", dict_type.clone(), false).with_metadata(md_f1);
            let mut md_f2 = HashMap::new();
            md_f2.insert(
                AVRO_ENUM_SYMBOLS_METADATA_KEY.to_string(),
                r#"["e","f","g","h"]"#.to_string(),
            );
            let f2_field = Field::new("f2", dict_type.clone(), false).with_metadata(md_f2);
            let mut md_f3 = HashMap::new();
            md_f3.insert(
                AVRO_ENUM_SYMBOLS_METADATA_KEY.to_string(),
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

    #[test]
    fn test_nested_record_type_reuse() {
        // The .avro file has the following schema:
        // {
        // "type" : "record",
        // "name" : "Record",
        // "fields" : [ {
        //     "name" : "nested",
        //     "type" : {
        //     "type" : "record",
        //     "name" : "Nested",
        //     "fields" : [ {
        //         "name" : "nested_int",
        //         "type" : "int"
        //     } ]
        //     }
        // }, {
        //     "name" : "nestedRecord",
        //     "type" : "Nested"
        // }, {
        //     "name" : "nestedArray",
        //     "type" : {
        //     "type" : "array",
        //     "items" : "Nested"
        //     }
        // } ]
        // }
        let batch = read_file("test/data/nested_record_reuse.avro", 8, false);
        let schema = batch.schema();

        // Verify schema structure
        assert_eq!(schema.fields().len(), 3);
        let fields = schema.fields();
        assert_eq!(fields[0].name(), "nested");
        assert_eq!(fields[1].name(), "nestedRecord");
        assert_eq!(fields[2].name(), "nestedArray");
        assert!(matches!(fields[0].data_type(), DataType::Struct(_)));
        assert!(matches!(fields[1].data_type(), DataType::Struct(_)));
        assert!(matches!(fields[2].data_type(), DataType::List(_)));

        // Validate that the nested record type
        if let DataType::Struct(nested_fields) = fields[0].data_type() {
            assert_eq!(nested_fields.len(), 1);
            assert_eq!(nested_fields[0].name(), "nested_int");
            assert_eq!(nested_fields[0].data_type(), &DataType::Int32);
        }

        // Validate that the nested record type is reused
        assert_eq!(fields[0].data_type(), fields[1].data_type());
        if let DataType::List(array_field) = fields[2].data_type() {
            assert_eq!(array_field.data_type(), fields[0].data_type());
        }

        // Validate data
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3);

        // Validate the first column (nested)
        let nested_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let nested_int_array = nested_col
            .column_by_name("nested_int")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(nested_int_array.value(0), 42);
        assert_eq!(nested_int_array.value(1), 99);

        // Validate the second column (nestedRecord)
        let nested_record_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let nested_record_int_array = nested_record_col
            .column_by_name("nested_int")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(nested_record_int_array.value(0), 100);
        assert_eq!(nested_record_int_array.value(1), 200);

        // Validate the third column (nestedArray)
        let nested_array_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(nested_array_col.len(), 2);
        let first_array_struct = nested_array_col.value(0);
        let first_array_struct_array = first_array_struct
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let first_array_int_values = first_array_struct_array
            .column_by_name("nested_int")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(first_array_int_values.len(), 3);
        assert_eq!(first_array_int_values.value(0), 1);
        assert_eq!(first_array_int_values.value(1), 2);
        assert_eq!(first_array_int_values.value(2), 3);
    }

    #[test]
    fn test_enum_type_reuse() {
        // The .avro file has the following schema:
        // {
        //     "type" : "record",
        //     "name" : "Record",
        //     "fields" : [ {
        //       "name" : "status",
        //       "type" : {
        //         "type" : "enum",
        //         "name" : "Status",
        //         "symbols" : [ "ACTIVE", "INACTIVE", "PENDING" ]
        //       }
        //     }, {
        //       "name" : "backupStatus",
        //       "type" : "Status"
        //     }, {
        //       "name" : "statusHistory",
        //       "type" : {
        //         "type" : "array",
        //         "items" : "Status"
        //       }
        //     } ]
        //   }
        let batch = read_file("test/data/enum_reuse.avro", 8, false);
        let schema = batch.schema();

        // Verify schema structure
        assert_eq!(schema.fields().len(), 3);
        let fields = schema.fields();
        assert_eq!(fields[0].name(), "status");
        assert_eq!(fields[1].name(), "backupStatus");
        assert_eq!(fields[2].name(), "statusHistory");
        assert!(matches!(fields[0].data_type(), DataType::Dictionary(_, _)));
        assert!(matches!(fields[1].data_type(), DataType::Dictionary(_, _)));
        assert!(matches!(fields[2].data_type(), DataType::List(_)));

        if let DataType::Dictionary(key_type, value_type) = fields[0].data_type() {
            assert_eq!(key_type.as_ref(), &DataType::Int32);
            assert_eq!(value_type.as_ref(), &DataType::Utf8);
        }

        // Validate that the enum types are reused
        assert_eq!(fields[0].data_type(), fields[1].data_type());
        if let DataType::List(array_field) = fields[2].data_type() {
            assert_eq!(array_field.data_type(), fields[0].data_type());
        }

        // Validate data - should have 2 rows
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3);

        // Get status enum values
        let status_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();
        let status_values = status_col
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        // First row should be "ACTIVE", second row should be "PENDING"
        assert_eq!(
            status_values.value(status_col.key(0).unwrap() as usize),
            "ACTIVE"
        );
        assert_eq!(
            status_values.value(status_col.key(1).unwrap() as usize),
            "PENDING"
        );

        // Get backupStatus enum values (same as status)
        let backup_status_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();
        let backup_status_values = backup_status_col
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        // First row should be "INACTIVE", second row should be "ACTIVE"
        assert_eq!(
            backup_status_values.value(backup_status_col.key(0).unwrap() as usize),
            "INACTIVE"
        );
        assert_eq!(
            backup_status_values.value(backup_status_col.key(1).unwrap() as usize),
            "ACTIVE"
        );

        // Get statusHistory array
        let status_history_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(status_history_col.len(), 2);

        // Validate first row's array data
        let first_array_dict = status_history_col.value(0);
        let first_array_dict_array = first_array_dict
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();
        let first_array_values = first_array_dict_array
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        // First row: ["PENDING", "ACTIVE", "INACTIVE"]
        assert_eq!(first_array_dict_array.len(), 3);
        assert_eq!(
            first_array_values.value(first_array_dict_array.key(0).unwrap() as usize),
            "PENDING"
        );
        assert_eq!(
            first_array_values.value(first_array_dict_array.key(1).unwrap() as usize),
            "ACTIVE"
        );
        assert_eq!(
            first_array_values.value(first_array_dict_array.key(2).unwrap() as usize),
            "INACTIVE"
        );
    }
}
