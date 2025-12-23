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
//! ### Limitations
//!
//!- **Avro unions with > 127 branches are not supported.**
//!  When decoding Avro unions to Arrow `UnionArray`, Arrow stores the union
//!  type identifiers in an **8‑bit signed** buffer (`i8`). This implies a
//!  practical limit of **127** distinct branch ids. Inputs that resolve to
//!  more than 127 branches will return an error. If you truly need more,
//!  model the schema as a **union of unions**, per the Arrow format spec.
//!
//!  See: Arrow Columnar Format — Dense Union (“types buffer: 8‑bit signed;
//!  a union with more than 127 possible types can be modeled as a union of
//!  unions”).
//!
//! This module exposes three layers of the API surface, from highest to lowest-level:
//!
//! * [`ReaderBuilder`](crate::reader::ReaderBuilder): configures how Avro is read (batch size, strict union handling,
//!   string representation, reader schema, etc.) and produces either:
//!   * a `Reader` for **Avro Object Container Files (OCF)** read from any `BufRead`, or
//!   * a low-level `Decoder` for **single‑object encoded** Avro bytes and Confluent
//!     **Schema Registry** framed messages.
//! * [`Reader`](crate::reader::Reader): a convenient, synchronous iterator over `RecordBatch` decoded from an OCF
//!   input. Implements [`Iterator<Item = Result<RecordBatch, ArrowError>>`] and
//!   `RecordBatchReader`.
//! * [`Decoder`](crate::reader::Decoder): a push‑based row decoder that consumes SOE framed Avro bytes and yields ready
//!   `RecordBatch` values when batches fill. This is suitable for integrating with async
//!   byte streams, network protocols, or other custom data sources.
//!
//! ## Encodings and when to use which type
//!
//! * **Object Container File (OCF)**: A self‑describing file format with a header containing
//!   the writer schema, optional compression codec, and a sync marker, followed by one or
//!   more data blocks. Use `Reader` for this format. See the Avro 1.11.1 specification
//!   (“Object Container Files”). <https://avro.apache.org/docs/1.11.1/specification/#object-container-files>
//! * **Single‑Object Encoding**: A stream‑friendly framing that prefixes each record body with
//!   the 2‑byte marker `0xC3 0x01` followed by the **8‑byte little‑endian CRC‑64‑AVRO Rabin
//!   fingerprint** of the writer schema, then the Avro binary body. Use `Decoder` with a
//!   populated `SchemaStore` to resolve fingerprints to full schemas.
//!   See “Single object encoding” in the Avro 1.11.1 spec.
//!   <https://avro.apache.org/docs/1.11.1/specification/#single-object-encoding>
//! * **Confluent Schema Registry wire format**: A 1‑byte magic `0x00`, a **4‑byte big‑endian**
//!   schema ID, then the Avro‑encoded body. Use `Decoder` with a `SchemaStore` configured
//!   for `FingerprintAlgorithm::Id` and entries keyed by `Fingerprint::Id`. See
//!   Confluent’s “Wire format” documentation.
//!   <https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format>
//! * **Apicurio Schema Registry wire format**: A 1‑byte magic `0x00`, a **8‑byte big‑endian**
//!   global schema ID, then the Avro‑encoded body. Use `Decoder` with a `SchemaStore` configured
//!   for `FingerprintAlgorithm::Id64` and entries keyed by `Fingerprint::Id64`. See
//!   Apicurio’s “Avro SerDe” documentation.
//!   <https://www.apicur.io/registry/docs/apicurio-registry/1.3.3.Final/getting-started/assembly-using-kafka-client-serdes.html#registry-serdes-types-avro-registry>
//!
//! ## Basic file usage (OCF)
//!
//! Use `ReaderBuilder::build` to construct a `Reader` from any `BufRead`. The doctest below
//! creates a tiny OCF in memory using `AvroWriter` and then reads it back.
//!
//! ```
//! use std::io::Cursor;
//! use std::sync::Arc;
//! use arrow_array::{ArrayRef, Int32Array, RecordBatch};
//! use arrow_schema::{DataType, Field, Schema};
//! use arrow_avro::writer::AvroWriter;
//! use arrow_avro::reader::ReaderBuilder;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // Build a minimal Arrow schema and batch
//! let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
//! let batch = RecordBatch::try_new(
//!     Arc::new(schema.clone()),
//!     vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef],
//! )?;
//!
//! // Write an Avro OCF to memory
//! let buffer: Vec<u8> = Vec::new();
//! let mut writer = AvroWriter::new(buffer, schema.clone())?;
//! writer.write(&batch)?;
//! writer.finish()?;
//! let bytes = writer.into_inner();
//!
//! // Read it back with ReaderBuilder
//! let mut reader = ReaderBuilder::new().build(Cursor::new(bytes))?;
//! let out = reader.next().unwrap()?;
//! assert_eq!(out.num_rows(), 3);
//! # Ok(()) }
//! ```
//!
//! ## Streaming usage (single‑object / Confluent / Apicurio)
//!
//! The `Decoder` lets you integrate Avro decoding with **any** source of bytes by
//! periodically calling `Decoder::decode` with new data and calling `Decoder::flush`
//! to get a `RecordBatch` once at least one row is complete.
//!
//! The example below shows how to decode from an arbitrary stream of `bytes::Bytes` using
//! `futures` utilities. Note: this is illustrative and keeps a single in‑memory `Bytes`
//! buffer for simplicity—real applications typically maintain a rolling buffer.
//!
//! ```
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
//! ### Building and using a `Decoder` for **single‑object encoding** (Rabin fingerprints)
//!
//! The doctest below **writes** a single‑object framed record using the Avro writer
//! (no manual varints) for the writer schema
//! (`{"type":"record","name":"User","fields":[{"name":"id","type":"long"}]}`)
//! and then decodes it into a `RecordBatch`.
//!
//! ```
//! use std::sync::Arc;
//! use std::collections::HashMap;
//! use arrow_array::{ArrayRef, Int64Array, RecordBatch};
//! use arrow_schema::{DataType, Field, Schema};
//! use arrow_avro::schema::{AvroSchema, SchemaStore, SCHEMA_METADATA_KEY, FingerprintStrategy};
//! use arrow_avro::writer::{WriterBuilder, format::AvroSoeFormat};
//! use arrow_avro::reader::ReaderBuilder;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // Register the writer schema (Rabin fingerprint by default).
//! let mut store = SchemaStore::new();
//! let avro_schema = AvroSchema::new(r#"{"type":"record","name":"User","fields":[
//!   {"name":"id","type":"long"}]}"#.to_string());
//! let _fp = store.register(avro_schema.clone())?;
//!
//! // Create a single-object framed record { id: 42 } with the Avro writer.
//! let mut md = HashMap::new();
//! md.insert(SCHEMA_METADATA_KEY.to_string(), avro_schema.json_string.clone());
//! let arrow = Schema::new_with_metadata(vec![Field::new("id", DataType::Int64, false)], md);
//! let batch = RecordBatch::try_new(
//!     Arc::new(arrow.clone()),
//!     vec![Arc::new(Int64Array::from(vec![42])) as ArrayRef],
//! )?;
//! let mut w = WriterBuilder::new(arrow)
//!     .with_fingerprint_strategy(FingerprintStrategy::Rabin) // SOE prefix
//!     .build::<_, AvroSoeFormat>(Vec::new())?;
//! w.write(&batch)?;
//! w.finish()?;
//! let frame = w.into_inner(); // C3 01 + fp + Avro body
//!
//! // Decode with a `Decoder`
//! let mut dec = ReaderBuilder::new()
//!   .with_writer_schema_store(store)
//!   .with_batch_size(1024)
//!   .build_decoder()?;
//!
//! dec.decode(&frame)?;
//! let out = dec.flush()?.expect("one batch");
//! assert_eq!(out.num_rows(), 1);
//! # Ok(()) }
//! ```
//!
//! See Avro 1.11.1 “Single object encoding” for details of the 2‑byte marker
//! and little‑endian CRC‑64‑AVRO fingerprint:
//! <https://avro.apache.org/docs/1.11.1/specification/#single-object-encoding>
//!
//! ### Building and using a `Decoder` for **Confluent Schema Registry** framing
//!
//! The Confluent wire format is: 1‑byte magic `0x00`, then a **4‑byte big‑endian** schema ID,
//! then the Avro body. The doctest below crafts two messages for the same schema ID and
//! decodes them into a single `RecordBatch` with two rows.
//!
//! ```
//! use std::sync::Arc;
//! use std::collections::HashMap;
//! use arrow_array::{ArrayRef, Int64Array, StringArray, RecordBatch};
//! use arrow_schema::{DataType, Field, Schema};
//! use arrow_avro::schema::{AvroSchema, SchemaStore, Fingerprint, FingerprintAlgorithm, SCHEMA_METADATA_KEY, FingerprintStrategy};
//! use arrow_avro::writer::{WriterBuilder, format::AvroSoeFormat};
//! use arrow_avro::reader::ReaderBuilder;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // Set up a store keyed by numeric IDs (Confluent).
//! let mut store = SchemaStore::new_with_type(FingerprintAlgorithm::Id);
//! let schema_id = 7u32;
//! let avro_schema = AvroSchema::new(r#"{"type":"record","name":"User","fields":[
//!   {"name":"id","type":"long"}, {"name":"name","type":"string"}]}"#.to_string());
//! store.set(Fingerprint::Id(schema_id), avro_schema.clone())?;
//!
//! // Write two Confluent-framed messages {id:1,name:"a"} and {id:2,name:"b"}.
//! fn msg(id: i64, name: &str, schema: &AvroSchema, schema_id: u32) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
//!     let mut md = HashMap::new();
//!     md.insert(SCHEMA_METADATA_KEY.to_string(), schema.json_string.clone());
//!     let arrow = Schema::new_with_metadata(
//!         vec![Field::new("id", DataType::Int64, false), Field::new("name", DataType::Utf8, false)],
//!         md,
//!     );
//!     let batch = RecordBatch::try_new(
//!         Arc::new(arrow.clone()),
//!         vec![
//!           Arc::new(Int64Array::from(vec![id])) as ArrayRef,
//!           Arc::new(StringArray::from(vec![name])) as ArrayRef,
//!         ],
//!     )?;
//!     let mut w = WriterBuilder::new(arrow)
//!         .with_fingerprint_strategy(FingerprintStrategy::Id(schema_id)) // 0x00 + ID + body
//!         .build::<_, AvroSoeFormat>(Vec::new())?;
//!     w.write(&batch)?; w.finish()?;
//!     Ok(w.into_inner())
//! }
//! let m1 = msg(1, "a", &avro_schema, schema_id)?;
//! let m2 = msg(2, "b", &avro_schema, schema_id)?;
//!
//! // Decode both into a single batch.
//! let mut dec = ReaderBuilder::new()
//!   .with_writer_schema_store(store)
//!   .with_batch_size(1024)
//!   .build_decoder()?;
//! dec.decode(&m1)?;
//! dec.decode(&m2)?;
//! let batch = dec.flush()?.expect("batch");
//! assert_eq!(batch.num_rows(), 2);
//! # Ok(()) }
//! ```
//!
//! See Confluent’s “Wire format” notes: magic byte `0x00`, 4‑byte **big‑endian** schema ID,
//! then the Avro‑encoded payload.
//! <https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format>
//!
//! ## Schema resolution (reader vs. writer schemas)
//!
//! Avro supports resolving data written with one schema (“writer”) into another (“reader”)
//! using rules like **field aliases**, **default values**, and **numeric promotions**.
//! In practice this lets you evolve schemas over time while remaining compatible with old data.
//!
//! *Spec background:* See Avro’s **Schema Resolution** (aliases, defaults) and the Confluent
//! **Wire format** (magic `0x00` + big‑endian schema id + Avro body).
//! <https://avro.apache.org/docs/1.11.1/specification/#schema-resolution>
//! <https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format>
//!
//! ### OCF example: rename a field and add a default via a reader schema
//!
//! Below we write an OCF with a *writer schema* having fields `id: long`, `name: string`.
//! We then read it with a *reader schema* that:
//! - **renames** `name` to `full_name` via `aliases`, and
//! - **adds** `is_active: boolean` with a **default** value `true`.
//!
//! ```
//! use std::io::Cursor;
//! use std::sync::Arc;
//! use arrow_array::{ArrayRef, Int64Array, StringArray, RecordBatch};
//! use arrow_schema::{DataType, Field, Schema};
//! use arrow_avro::writer::AvroWriter;
//! use arrow_avro::reader::ReaderBuilder;
//! use arrow_avro::schema::AvroSchema;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // Writer (past version): { id: long, name: string }
//! let writer_arrow = Schema::new(vec![
//!     Field::new("id", DataType::Int64, false),
//!     Field::new("name", DataType::Utf8, false),
//! ]);
//! let batch = RecordBatch::try_new(
//!     Arc::new(writer_arrow.clone()),
//!     vec![
//!         Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
//!         Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
//!     ],
//! )?;
//!
//! // Write an OCF entirely in memory
//! let mut w = AvroWriter::new(Vec::<u8>::new(), writer_arrow)?;
//! w.write(&batch)?;
//! w.finish()?;
//! let bytes = w.into_inner();
//!
//! // Reader (current version):
//! //  - record name "topLevelRecord" matches the crate's default for OCF
//! //  - rename `name` -> `full_name` using aliases (optional)
//! let reader_json = r#"
//! {
//!   "type": "record",
//!   "name": "topLevelRecord",
//!   "fields": [
//!     { "name": "id", "type": "long" },
//!     { "name": "full_name", "type": ["null","string"], "aliases": ["name"], "default": null },
//!     { "name": "is_active", "type": "boolean", "default": true }
//!   ]
//! }"#;
//!
//! let mut reader = ReaderBuilder::new()
//!   .with_reader_schema(AvroSchema::new(reader_json.to_string()))
//!   .build(Cursor::new(bytes))?;
//!
//! let out = reader.next().unwrap()?;
//! assert_eq!(out.num_rows(), 2);
//! # Ok(()) }
//! ```
//!
//! ### Confluent single‑object example: resolve *past* writer versions to the topic’s **current** reader schema
//!
//! In this scenario, the **reader schema** is the topic’s *current* schema, while the two
//! **writer schemas** registered under Confluent IDs **1** and **2** represent *past versions*.
//! The decoder uses the reader schema to resolve both versions.
//!
//! ```
//! use std::sync::Arc;
//! use std::collections::HashMap;
//! use arrow_avro::reader::ReaderBuilder;
//! use arrow_avro::schema::{
//!     AvroSchema, Fingerprint, FingerprintAlgorithm, SchemaStore,
//!     SCHEMA_METADATA_KEY, FingerprintStrategy,
//! };
//! use arrow_array::{ArrayRef, Int32Array, Int64Array, StringArray, RecordBatch};
//! use arrow_schema::{DataType, Field, Schema};
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Reader: current topic schema (no reader-added fields)
//!     //   {"type":"record","name":"User","fields":[
//!     //     {"name":"id","type":"long"},
//!     //     {"name":"name","type":"string"}]}
//!     let reader_schema = AvroSchema::new(
//!         r#"{"type":"record","name":"User",
//!             "fields":[{"name":"id","type":"long"},{"name":"name","type":"string"}]}"#
//!             .to_string(),
//!     );
//!
//!     // Register two *writer* schemas under Confluent IDs 0 and 1
//!     let writer_v0 = AvroSchema::new(
//!         r#"{"type":"record","name":"User",
//!             "fields":[{"name":"id","type":"int"},{"name":"name","type":"string"}]}"#
//!             .to_string(),
//!     );
//!     let writer_v1 = AvroSchema::new(
//!         r#"{"type":"record","name":"User",
//!             "fields":[{"name":"id","type":"long"},{"name":"name","type":"string"},
//!                       {"name":"email","type":["null","string"],"default":null}]}"#
//!             .to_string(),
//!     );
//!
//!     let id_v0: u32 = 0;
//!     let id_v1: u32 = 1;
//!
//!     let mut store = SchemaStore::new_with_type(FingerprintAlgorithm::Id); // integer IDs
//!     store.set(Fingerprint::Id(id_v0), writer_v0.clone())?;
//!     store.set(Fingerprint::Id(id_v1), writer_v1.clone())?;
//!
//!     // Write two Confluent-framed messages using each writer version
//!     // frame0: writer v0 body {id:1001_i32, name:"v0-alice"}
//!     let mut md0 = HashMap::new();
//!     md0.insert(SCHEMA_METADATA_KEY.to_string(), writer_v0.json_string.clone());
//!     let arrow0 = Schema::new_with_metadata(
//!         vec![Field::new("id", DataType::Int32, false),
//!              Field::new("name", DataType::Utf8, false)], md0);
//!     let batch0 = RecordBatch::try_new(
//!         Arc::new(arrow0.clone()),
//!         vec![Arc::new(Int32Array::from(vec![1001])) as ArrayRef,
//!              Arc::new(StringArray::from(vec!["v0-alice"])) as ArrayRef])?;
//!     let mut w0 = arrow_avro::writer::WriterBuilder::new(arrow0)
//!         .with_fingerprint_strategy(FingerprintStrategy::Id(id_v0))
//!         .build::<_, arrow_avro::writer::format::AvroSoeFormat>(Vec::new())?;
//!     w0.write(&batch0)?; w0.finish()?;
//!     let frame0 = w0.into_inner(); // 0x00 + id_v0 + body
//!
//!     // frame1: writer v1 body {id:2002_i64, name:"v1-bob", email: Some("bob@example.com")}
//!     let mut md1 = HashMap::new();
//!    md1.insert(SCHEMA_METADATA_KEY.to_string(), writer_v1.json_string.clone());
//!     let arrow1 = Schema::new_with_metadata(
//!         vec![Field::new("id", DataType::Int64, false),
//!              Field::new("name", DataType::Utf8, false),
//!              Field::new("email", DataType::Utf8, true)], md1);
//!     let batch1 = RecordBatch::try_new(
//!         Arc::new(arrow1.clone()),
//!         vec![Arc::new(Int64Array::from(vec![2002])) as ArrayRef,
//!              Arc::new(StringArray::from(vec!["v1-bob"])) as ArrayRef,
//!              Arc::new(StringArray::from(vec![Some("bob@example.com")])) as ArrayRef])?;
//!     let mut w1 = arrow_avro::writer::WriterBuilder::new(arrow1)
//!         .with_fingerprint_strategy(FingerprintStrategy::Id(id_v1))
//!         .build::<_, arrow_avro::writer::format::AvroSoeFormat>(Vec::new())?;
//!     w1.write(&batch1)?; w1.finish()?;
//!     let frame1 = w1.into_inner(); // 0x00 + id_v1 + body
//!
//!     // Build a streaming Decoder that understands Confluent framing
//!     let mut decoder = ReaderBuilder::new()
//!         .with_reader_schema(reader_schema)
//!         .with_writer_schema_store(store)
//!         .with_batch_size(8) // small demo batches
//!         .build_decoder()?;
//!
//!     // Decode each whole frame, then drain completed rows with flush()
//!     let mut total_rows = 0usize;
//!
//!     let consumed0 = decoder.decode(&frame0)?;
//!     assert_eq!(consumed0, frame0.len(), "decoder must consume the whole frame");
//!     while let Some(batch) = decoder.flush()? { total_rows += batch.num_rows(); }
//!
//!     let consumed1 = decoder.decode(&frame1)?;
//!     assert_eq!(consumed1, frame1.len(), "decoder must consume the whole frame");
//!     while let Some(batch) = decoder.flush()? { total_rows += batch.num_rows(); }
//!
//!     // We sent 2 records so we should get 2 rows (possibly one per flush)
//!     assert_eq!(total_rows, 2);
//!     Ok(())
//! }
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
//! * For OCF, blocks may be compressed; `Reader` will decompress using the codec specified
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
use crate::codec::AvroFieldBuilder;
use crate::reader::header::read_header;
use crate::schema::{
    AvroSchema, CONFLUENT_MAGIC, Fingerprint, FingerprintAlgorithm, SINGLE_OBJECT_MAGIC, Schema,
    SchemaStore,
};
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, SchemaRef};
use block::BlockDecoder;
use header::Header;
use indexmap::IndexMap;
use record::RecordDecoder;
use std::io::BufRead;

mod block;
mod cursor;
mod header;
mod record;
mod vlq;

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
/// Build and use a `Decoder` for single‑object encoding:
///
/// ```
/// use arrow_avro::schema::{AvroSchema, SchemaStore};
/// use arrow_avro::reader::ReaderBuilder;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// // Use a record schema at the top level so we can build an Arrow RecordBatch
/// let mut store = SchemaStore::new(); // Rabin fingerprinting by default
/// let avro = AvroSchema::new(
///     r#"{"type":"record","name":"E","fields":[{"name":"x","type":"long"}]}"#.to_string()
/// );
/// let fp = store.register(avro)?;
///
/// // --- Hidden: write a single-object framed row {x:7} ---
/// # use std::sync::Arc;
/// # use std::collections::HashMap;
/// # use arrow_array::{ArrayRef, Int64Array, RecordBatch};
/// # use arrow_schema::{DataType, Field, Schema};
/// # use arrow_avro::schema::{SCHEMA_METADATA_KEY, FingerprintStrategy};
/// # use arrow_avro::writer::{WriterBuilder, format::AvroSoeFormat};
/// # let mut md = HashMap::new();
/// # md.insert(SCHEMA_METADATA_KEY.to_string(),
/// #     r#"{"type":"record","name":"E","fields":[{"name":"x","type":"long"}]}"#.to_string());
/// # let arrow = Schema::new_with_metadata(vec![Field::new("x", DataType::Int64, false)], md);
/// # let batch = RecordBatch::try_new(Arc::new(arrow.clone()), vec![Arc::new(Int64Array::from(vec![7])) as ArrayRef])?;
/// # let mut w = WriterBuilder::new(arrow)
/// #     .with_fingerprint_strategy(fp.into())
/// #     .build::<_, AvroSoeFormat>(Vec::new())?;
/// # w.write(&batch)?; w.finish()?; let frame = w.into_inner();
///
/// let mut decoder = ReaderBuilder::new()
///     .with_writer_schema_store(store)
///     .with_batch_size(16)
///     .build_decoder()?;
///
/// # decoder.decode(&frame)?;
/// let batch = decoder.flush()?.expect("one row");
/// assert_eq!(batch.num_rows(), 1);
/// # Ok(()) }
/// ```
///
/// *Background:* Avro's single‑object encoding is defined as `0xC3 0x01` + 8‑byte
/// little‑endian CRC‑64‑AVRO fingerprint of the **writer schema** + Avro binary body.
/// See the Avro 1.11.1 spec for details. <https://avro.apache.org/docs/1.11.1/specification/#single-object-encoding>
///
/// Build and use a `Decoder` for Confluent Registry messages:
///
/// ```
/// use arrow_avro::schema::{AvroSchema, SchemaStore, Fingerprint, FingerprintAlgorithm};
/// use arrow_avro::reader::ReaderBuilder;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let mut store = SchemaStore::new_with_type(FingerprintAlgorithm::Id);
/// store.set(Fingerprint::Id(1234), AvroSchema::new(r#"{"type":"record","name":"E","fields":[{"name":"x","type":"long"}]}"#.to_string()))?;
///
/// // --- Hidden: encode two Confluent-framed messages {x:1} and {x:2} ---
/// # use std::sync::Arc;
/// # use std::collections::HashMap;
/// # use arrow_array::{ArrayRef, Int64Array, RecordBatch};
/// # use arrow_schema::{DataType, Field, Schema};
/// # use arrow_avro::schema::{SCHEMA_METADATA_KEY, FingerprintStrategy};
/// # use arrow_avro::writer::{WriterBuilder, format::AvroSoeFormat};
/// # fn msg(x: i64) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
/// #   let mut md = HashMap::new();
/// #   md.insert(SCHEMA_METADATA_KEY.to_string(),
/// #     r#"{"type":"record","name":"E","fields":[{"name":"x","type":"long"}]}"#.to_string());
/// #   let arrow = Schema::new_with_metadata(vec![Field::new("x", DataType::Int64, false)], md);
/// #   let batch = RecordBatch::try_new(Arc::new(arrow.clone()), vec![Arc::new(Int64Array::from(vec![x])) as ArrayRef])?;
/// #   let mut w = WriterBuilder::new(arrow)
/// #       .with_fingerprint_strategy(FingerprintStrategy::Id(1234))
/// #       .build::<_, AvroSoeFormat>(Vec::new())?;
/// #   w.write(&batch)?; w.finish()?; Ok(w.into_inner())
/// # }
/// # let m1 = msg(1)?;
/// # let m2 = msg(2)?;
///
/// let mut decoder = ReaderBuilder::new()
///     .with_writer_schema_store(store)
///     .build_decoder()?;
/// # decoder.decode(&m1)?;
/// # decoder.decode(&m2)?;
/// let batch = decoder.flush()?.expect("two rows");
/// assert_eq!(batch.num_rows(), 2);
/// # Ok(()) }
/// ```
#[derive(Debug)]
pub struct Decoder {
    active_decoder: RecordDecoder,
    active_fingerprint: Option<Fingerprint>,
    batch_size: usize,
    remaining_capacity: usize,
    cache: IndexMap<Fingerprint, RecordDecoder>,
    fingerprint_algorithm: FingerprintAlgorithm,
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
                    ));
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
            FingerprintAlgorithm::Id => self.handle_prefix_common(buf, &CONFLUENT_MAGIC, |bytes| {
                Fingerprint::Id(u32::from_be_bytes(bytes))
            }),
            FingerprintAlgorithm::Id64 => {
                self.handle_prefix_common(buf, &CONFLUENT_MAGIC, |bytes| {
                    Fingerprint::Id64(u64::from_be_bytes(bytes))
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
/// ```
/// use arrow_avro::schema::{AvroSchema, SchemaStore, Fingerprint, FingerprintAlgorithm};
/// use arrow_avro::reader::ReaderBuilder;
///
/// let mut store = SchemaStore::new_with_type(FingerprintAlgorithm::Id);
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
        RecordDecoder::try_new_with_options(root.data_type())
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
            fingerprint_algorithm,
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
    use crate::codec::AvroFieldBuilder;
    use crate::reader::record::RecordDecoder;
    use crate::reader::{Decoder, Reader, ReaderBuilder};
    use crate::schema::{
        AVRO_ENUM_SYMBOLS_METADATA_KEY, AVRO_NAME_METADATA_KEY, AVRO_NAMESPACE_METADATA_KEY,
        AvroSchema, CONFLUENT_MAGIC, Fingerprint, FingerprintAlgorithm, PrimitiveType,
        SINGLE_OBJECT_MAGIC, SchemaStore,
    };
    use crate::test_util::arrow_test_data;
    use crate::writer::AvroWriter;
    use arrow_array::builder::{
        ArrayBuilder, BooleanBuilder, Float32Builder, Int32Builder, Int64Builder, ListBuilder,
        MapBuilder, StringBuilder, StructBuilder,
    };
    #[cfg(feature = "snappy")]
    use arrow_array::builder::{Float64Builder, MapFieldNames};
    use arrow_array::cast::AsArray;
    #[cfg(not(feature = "avro_custom_types"))]
    use arrow_array::types::Int64Type;
    #[cfg(feature = "avro_custom_types")]
    use arrow_array::types::{
        DurationMicrosecondType, DurationMillisecondType, DurationNanosecondType,
        DurationSecondType,
    };
    use arrow_array::types::{Int32Type, IntervalMonthDayNanoType};
    use arrow_array::*;
    #[cfg(feature = "snappy")]
    use arrow_buffer::{Buffer, NullBuffer};
    use arrow_buffer::{IntervalMonthDayNano, OffsetBuffer, ScalarBuffer, i256};
    #[cfg(feature = "avro_custom_types")]
    use arrow_schema::{
        ArrowError, DataType, Field, FieldRef, Fields, IntervalUnit, Schema, TimeUnit, UnionFields,
        UnionMode,
    };
    #[cfg(not(feature = "avro_custom_types"))]
    use arrow_schema::{
        ArrowError, DataType, Field, FieldRef, Fields, IntervalUnit, Schema, UnionFields, UnionMode,
    };
    use bytes::Bytes;
    use futures::executor::block_on;
    use futures::{Stream, StreamExt, TryStreamExt, stream};
    use serde_json::{Value, json};
    use std::collections::HashMap;
    use std::fs::File;
    use std::io::{BufReader, Cursor};
    use std::sync::Arc;

    fn files() -> impl Iterator<Item = &'static str> {
        [
            // TODO: avoid requiring snappy for this file
            #[cfg(feature = "snappy")]
            "avro/alltypes_plain.avro",
            #[cfg(feature = "snappy")]
            "avro/alltypes_plain.snappy.avro",
            #[cfg(feature = "zstd")]
            "avro/alltypes_plain.zstandard.avro",
            #[cfg(feature = "bzip2")]
            "avro/alltypes_plain.bzip2.avro",
            #[cfg(feature = "xz")]
            "avro/alltypes_plain.xz.avro",
        ]
        .into_iter()
    }

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
            Fingerprint::Id64(v) => {
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

    fn make_id64_prefix(id: u64, additional: usize) -> Vec<u8> {
        let capacity = CONFLUENT_MAGIC.len() + size_of::<u64>() + additional;
        let mut out = Vec::with_capacity(capacity);
        out.extend_from_slice(&CONFLUENT_MAGIC);
        out.extend_from_slice(&id.to_be_bytes());
        out
    }

    fn make_message_id64(id: u64, value: i64) -> Vec<u8> {
        let encoded_value = encode_zigzag(value);
        let mut msg = make_id64_prefix(id, encoded_value.len());
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

    fn write_ocf(schema: &Schema, batches: &[RecordBatch]) -> Vec<u8> {
        let mut w = AvroWriter::new(Vec::<u8>::new(), schema.clone()).expect("writer");
        for b in batches {
            w.write(b).expect("write");
        }
        w.finish().expect("finish");
        w.into_inner()
    }

    #[test]
    fn writer_string_reader_nullable_with_alias() -> Result<(), Box<dyn std::error::Error>> {
        // Writer: { id: long, name: string }
        let writer_schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(writer_schema.clone()),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
                Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
            ],
        )?;
        let bytes = write_ocf(&writer_schema, &[batch]);
        let reader_json = r#"
    {
      "type": "record",
      "name": "topLevelRecord",
      "fields": [
        { "name": "id", "type": "long" },
        { "name": "full_name", "type": ["null","string"], "aliases": ["name"], "default": null },
        { "name": "is_active", "type": "boolean", "default": true }
      ]
    }"#;
        let mut reader = ReaderBuilder::new()
            .with_reader_schema(AvroSchema::new(reader_json.to_string()))
            .build(Cursor::new(bytes))?;
        let out = reader.next().unwrap()?;
        // Evolved aliased field should be non-null and match original writer values
        let full_name = out.column(1).as_string::<i32>();
        assert_eq!(full_name.value(0), "a");
        assert_eq!(full_name.value(1), "b");

        Ok(())
    }

    #[test]
    fn writer_string_reader_string_null_order_second() -> Result<(), Box<dyn std::error::Error>> {
        // Writer: { name: string }
        let writer_schema = Schema::new(vec![Field::new("name", DataType::Utf8, false)]);
        let batch = RecordBatch::try_new(
            Arc::new(writer_schema.clone()),
            vec![Arc::new(StringArray::from(vec!["x", "y"])) as ArrayRef],
        )?;
        let bytes = write_ocf(&writer_schema, &[batch]);

        // Reader: ["string","null"] (NullSecond)
        let reader_json = r#"
    {
      "type":"record", "name":"topLevelRecord",
      "fields":[ { "name":"name", "type":["string","null"], "default":"x" } ]
    }"#;

        let mut reader = ReaderBuilder::new()
            .with_reader_schema(AvroSchema::new(reader_json.to_string()))
            .build(Cursor::new(bytes))?;

        let out = reader.next().unwrap()?;
        assert_eq!(out.num_rows(), 2);

        // Should decode as non-null strings (writer non-union -> reader union)
        let name = out.column(0).as_string::<i32>();
        assert_eq!(name.value(0), "x");
        assert_eq!(name.value(1), "y");

        Ok(())
    }

    #[test]
    fn promotion_writer_int_reader_nullable_long() -> Result<(), Box<dyn std::error::Error>> {
        // Writer: { v: int }
        let writer_schema = Schema::new(vec![Field::new("v", DataType::Int32, false)]);
        let batch = RecordBatch::try_new(
            Arc::new(writer_schema.clone()),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef],
        )?;
        let bytes = write_ocf(&writer_schema, &[batch]);

        // Reader: { v: ["null","long"] }
        let reader_json = r#"
    {
      "type":"record", "name":"topLevelRecord",
      "fields":[ { "name":"v", "type":["null","long"], "default": null } ]
    }"#;

        let mut reader = ReaderBuilder::new()
            .with_reader_schema(AvroSchema::new(reader_json.to_string()))
            .build(Cursor::new(bytes))?;

        let out = reader.next().unwrap()?;
        assert_eq!(out.num_rows(), 3);

        // Should have promoted to Int64 and be non-null (no union tag in writer)
        let v = out
            .column(0)
            .as_primitive::<arrow_array::types::Int64Type>();
        assert_eq!(v.values(), &[1, 2, 3]);
        assert!(
            out.column(0).nulls().is_none(),
            "expected no validity bitmap for all-valid column"
        );

        Ok(())
    }

    #[test]
    fn test_alltypes_schema_promotion_mixed() {
        for file in files() {
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
        for file in files() {
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
        for file in files() {
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
    // TODO: avoid requiring snappy for this file
    #[cfg(feature = "snappy")]
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
        // f1
        let f1_keys = Int32Array::from(vec![3, 2, 1, 0]);
        let f1_vals = StringArray::from(vec!["d", "c", "b", "a"]);
        let f1 = DictionaryArray::<Int32Type>::try_new(f1_keys, Arc::new(f1_vals)).unwrap();
        let mut md_f1 = HashMap::new();
        md_f1.insert(
            AVRO_ENUM_SYMBOLS_METADATA_KEY.to_string(),
            r#"["d","c","b","a"]"#.to_string(),
        );
        // New named-type metadata
        md_f1.insert("avro.name".to_string(), "enum1".to_string());
        md_f1.insert("avro.namespace".to_string(), "ns1".to_string());
        let f1_field = Field::new("f1", dict_type.clone(), false).with_metadata(md_f1);
        // f2
        let f2_keys = Int32Array::from(vec![1, 0, 3, 2]);
        let f2_vals = StringArray::from(vec!["h", "g", "f", "e"]);
        let f2 = DictionaryArray::<Int32Type>::try_new(f2_keys, Arc::new(f2_vals)).unwrap();
        let mut md_f2 = HashMap::new();
        md_f2.insert(
            AVRO_ENUM_SYMBOLS_METADATA_KEY.to_string(),
            r#"["h","g","f","e"]"#.to_string(),
        );
        // New named-type metadata
        md_f2.insert("avro.name".to_string(), "enum2".to_string());
        md_f2.insert("avro.namespace".to_string(), "ns2".to_string());
        let f2_field = Field::new("f2", dict_type.clone(), false).with_metadata(md_f2);
        // f3
        let f3_keys = Int32Array::from(vec![Some(2), Some(0), None, Some(1)]);
        let f3_vals = StringArray::from(vec!["k", "i", "j"]);
        let f3 = DictionaryArray::<Int32Type>::try_new(f3_keys, Arc::new(f3_vals)).unwrap();
        let mut md_f3 = HashMap::new();
        md_f3.insert(
            AVRO_ENUM_SYMBOLS_METADATA_KEY.to_string(),
            r#"["k","i","j"]"#.to_string(),
        );
        // New named-type metadata
        md_f3.insert("avro.name".to_string(), "enum3".to_string());
        md_f3.insert("avro.namespace".to_string(), "ns1".to_string());
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
            Fingerprint::Id64(id) => panic!("expected Rabin fingerprint, got ({id})"),
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
        let long_decoder = RecordDecoder::try_new_with_options(root_long.data_type()).unwrap();
        let _ = decoder.cache.insert(fp_long, long_decoder);
        let mut buf = Vec::from(SINGLE_OBJECT_MAGIC);
        match fp_long {
            Fingerprint::Rabin(v) => buf.extend_from_slice(&v.to_le_bytes()),
            Fingerprint::Id(id) => panic!("expected Rabin fingerprint, got ({id})"),
            Fingerprint::Id64(id) => panic!("expected Rabin fingerprint, got ({id})"),
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
        let mut store = SchemaStore::new_with_type(FingerprintAlgorithm::Id);
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
        let mut store = SchemaStore::new_with_type(FingerprintAlgorithm::Id);
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
        let mut store = SchemaStore::new_with_type(FingerprintAlgorithm::Id);
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
        let buf = &CONFLUENT_MAGIC[..0]; // empty incomplete magic
        let res = decoder.handle_prefix(buf).unwrap();
        assert_eq!(res, Some(0));
        assert!(decoder.pending_schema.is_none());
    }

    #[test]
    fn test_two_messages_same_schema_id64() {
        let writer_schema = make_value_schema(PrimitiveType::Int);
        let reader_schema = writer_schema.clone();
        let id = 100u64;
        // Set up store with None fingerprint algorithm and register schema by id
        let mut store = SchemaStore::new_with_type(FingerprintAlgorithm::Id64);
        let _ = store
            .set(Fingerprint::Id64(id), writer_schema.clone())
            .expect("set id schema");
        let msg1 = make_message_id64(id, 21);
        let msg2 = make_message_id64(id, 22);
        let input = [msg1.clone(), msg2.clone()].concat();
        let mut decoder = ReaderBuilder::new()
            .with_batch_size(8)
            .with_reader_schema(reader_schema)
            .with_writer_schema_store(store)
            .with_active_fingerprint(Fingerprint::Id64(id))
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
    // TODO: avoid requiring snappy for this file
    #[cfg(feature = "snappy")]
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
    // TODO: avoid requiring snappy for this file
    #[cfg(feature = "snappy")]
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
        fn rebuild_list_array_with_element(
            col: &ArrayRef,
            new_elem: Arc<Field>,
            is_large: bool,
        ) -> ArrayRef {
            if is_large {
                let list = col
                    .as_any()
                    .downcast_ref::<LargeListArray>()
                    .expect("expected LargeListArray");
                let offsets = list.offsets().clone();
                let values = list.values().clone();
                let validity = list.nulls().cloned();
                Arc::new(LargeListArray::try_new(new_elem, offsets, values, validity).unwrap())
            } else {
                let list = col
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .expect("expected ListArray");
                let offsets = list.offsets().clone();
                let values = list.values().clone();
                let validity = list.nulls().cloned();
                Arc::new(ListArray::try_new(new_elem, offsets, values, validity).unwrap())
            }
        }
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
            let col_full = full.column(idx).clone();
            let full_field = schema_full.field(idx).as_ref().clone();
            let proj_field_ref = projected.schema().field(0).clone();
            let proj_field = proj_field_ref.as_ref();
            let top_meta = proj_field.metadata().clone();
            let (expected_field_ref, expected_col): (Arc<Field>, ArrayRef) =
                match (full_field.data_type(), proj_field.data_type()) {
                    (&DataType::List(_), DataType::List(proj_elem)) => {
                        let new_col =
                            rebuild_list_array_with_element(&col_full, proj_elem.clone(), false);
                        let nf = Field::new(
                            full_field.name().clone(),
                            proj_field.data_type().clone(),
                            full_field.is_nullable(),
                        )
                        .with_metadata(top_meta);
                        (Arc::new(nf), new_col)
                    }
                    (&DataType::LargeList(_), DataType::LargeList(proj_elem)) => {
                        let new_col =
                            rebuild_list_array_with_element(&col_full, proj_elem.clone(), true);
                        let nf = Field::new(
                            full_field.name().clone(),
                            proj_field.data_type().clone(),
                            full_field.is_nullable(),
                        )
                        .with_metadata(top_meta);
                        (Arc::new(nf), new_col)
                    }
                    _ => {
                        let nf = full_field.with_metadata(top_meta);
                        (Arc::new(nf), col_full)
                    }
                };

            let expected = RecordBatch::try_new(
                Arc::new(Schema::new(vec![expected_field_ref])),
                vec![expected_col],
            )
            .unwrap();
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
        let a = batch.column(idx).as_primitive::<Int32Type>();
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
    fn test_union_fields_end_to_end_expected_arrays() {
        fn tid_by_name(fields: &UnionFields, want: &str) -> i8 {
            for (tid, f) in fields.iter() {
                if f.name() == want {
                    return tid;
                }
            }
            panic!("union child '{want}' not found")
        }

        fn tid_by_dt(fields: &UnionFields, pred: impl Fn(&DataType) -> bool) -> i8 {
            for (tid, f) in fields.iter() {
                if pred(f.data_type()) {
                    return tid;
                }
            }
            panic!("no union child matches predicate");
        }

        fn uuid16_from_str(s: &str) -> [u8; 16] {
            fn hex(b: u8) -> u8 {
                match b {
                    b'0'..=b'9' => b - b'0',
                    b'a'..=b'f' => b - b'a' + 10,
                    b'A'..=b'F' => b - b'A' + 10,
                    _ => panic!("invalid hex"),
                }
            }
            let mut out = [0u8; 16];
            let bytes = s.as_bytes();
            let (mut i, mut j) = (0, 0);
            while i < bytes.len() {
                if bytes[i] == b'-' {
                    i += 1;
                    continue;
                }
                let hi = hex(bytes[i]);
                let lo = hex(bytes[i + 1]);
                out[j] = (hi << 4) | lo;
                j += 1;
                i += 2;
            }
            assert_eq!(j, 16, "uuid must decode to 16 bytes");
            out
        }

        fn empty_child_for(dt: &DataType) -> Arc<dyn Array> {
            match dt {
                DataType::Null => Arc::new(NullArray::new(0)),
                DataType::Boolean => Arc::new(BooleanArray::from(Vec::<bool>::new())),
                DataType::Int32 => Arc::new(Int32Array::from(Vec::<i32>::new())),
                DataType::Int64 => Arc::new(Int64Array::from(Vec::<i64>::new())),
                DataType::Float32 => Arc::new(arrow_array::Float32Array::from(Vec::<f32>::new())),
                DataType::Float64 => Arc::new(arrow_array::Float64Array::from(Vec::<f64>::new())),
                DataType::Binary => Arc::new(BinaryArray::from(Vec::<&[u8]>::new())),
                DataType::Utf8 => Arc::new(StringArray::from(Vec::<&str>::new())),
                DataType::Date32 => Arc::new(arrow_array::Date32Array::from(Vec::<i32>::new())),
                DataType::Time32(arrow_schema::TimeUnit::Millisecond) => {
                    Arc::new(Time32MillisecondArray::from(Vec::<i32>::new()))
                }
                DataType::Time64(arrow_schema::TimeUnit::Microsecond) => {
                    Arc::new(Time64MicrosecondArray::from(Vec::<i64>::new()))
                }
                DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, tz) => {
                    let a = TimestampMillisecondArray::from(Vec::<i64>::new());
                    Arc::new(if let Some(tz) = tz {
                        a.with_timezone(tz.clone())
                    } else {
                        a
                    })
                }
                DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, tz) => {
                    let a = TimestampMicrosecondArray::from(Vec::<i64>::new());
                    Arc::new(if let Some(tz) = tz {
                        a.with_timezone(tz.clone())
                    } else {
                        a
                    })
                }
                DataType::Interval(IntervalUnit::MonthDayNano) => {
                    Arc::new(arrow_array::IntervalMonthDayNanoArray::from(Vec::<
                        IntervalMonthDayNano,
                    >::new(
                    )))
                }
                DataType::FixedSizeBinary(n) => Arc::new(FixedSizeBinaryArray::new_null(*n, 0)),
                DataType::Dictionary(k, v) => {
                    assert_eq!(**k, DataType::Int32, "expect int32 keys for enums");
                    let keys = Int32Array::from(Vec::<i32>::new());
                    let values = match v.as_ref() {
                        DataType::Utf8 => {
                            Arc::new(StringArray::from(Vec::<&str>::new())) as ArrayRef
                        }
                        other => panic!("unexpected dictionary value type {other:?}"),
                    };
                    Arc::new(DictionaryArray::<Int32Type>::try_new(keys, values).unwrap())
                }
                DataType::List(field) => {
                    let values: ArrayRef = match field.data_type() {
                        DataType::Int32 => {
                            Arc::new(Int32Array::from(Vec::<i32>::new())) as ArrayRef
                        }
                        DataType::Int64 => {
                            Arc::new(Int64Array::from(Vec::<i64>::new())) as ArrayRef
                        }
                        DataType::Utf8 => {
                            Arc::new(StringArray::from(Vec::<&str>::new())) as ArrayRef
                        }
                        DataType::Union(_, _) => {
                            let (uf, _) = if let DataType::Union(f, m) = field.data_type() {
                                (f.clone(), m)
                            } else {
                                unreachable!()
                            };
                            let children: Vec<ArrayRef> = uf
                                .iter()
                                .map(|(_, f)| empty_child_for(f.data_type()))
                                .collect();
                            Arc::new(
                                UnionArray::try_new(
                                    uf.clone(),
                                    ScalarBuffer::<i8>::from(Vec::<i8>::new()),
                                    Some(ScalarBuffer::<i32>::from(Vec::<i32>::new())),
                                    children,
                                )
                                .unwrap(),
                            ) as ArrayRef
                        }
                        other => panic!("unsupported list item type: {other:?}"),
                    };
                    let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0]));
                    Arc::new(ListArray::try_new(field.clone(), offsets, values, None).unwrap())
                }
                DataType::Map(entry_field, ordered) => {
                    let DataType::Struct(childs) = entry_field.data_type() else {
                        panic!("map entries must be struct")
                    };
                    let key_field = &childs[0];
                    let val_field = &childs[1];
                    assert_eq!(key_field.data_type(), &DataType::Utf8);
                    let keys = StringArray::from(Vec::<&str>::new());
                    let vals: ArrayRef = match val_field.data_type() {
                        DataType::Float64 => {
                            Arc::new(arrow_array::Float64Array::from(Vec::<f64>::new())) as ArrayRef
                        }
                        DataType::Int64 => {
                            Arc::new(Int64Array::from(Vec::<i64>::new())) as ArrayRef
                        }
                        DataType::Utf8 => {
                            Arc::new(StringArray::from(Vec::<&str>::new())) as ArrayRef
                        }
                        DataType::Union(uf, _) => {
                            let ch: Vec<ArrayRef> = uf
                                .iter()
                                .map(|(_, f)| empty_child_for(f.data_type()))
                                .collect();
                            Arc::new(
                                UnionArray::try_new(
                                    uf.clone(),
                                    ScalarBuffer::<i8>::from(Vec::<i8>::new()),
                                    Some(ScalarBuffer::<i32>::from(Vec::<i32>::new())),
                                    ch,
                                )
                                .unwrap(),
                            ) as ArrayRef
                        }
                        other => panic!("unsupported map value type: {other:?}"),
                    };
                    let entries = StructArray::new(
                        Fields::from(vec![key_field.as_ref().clone(), val_field.as_ref().clone()]),
                        vec![Arc::new(keys) as ArrayRef, vals],
                        None,
                    );
                    let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0]));
                    Arc::new(MapArray::new(
                        entry_field.clone(),
                        offsets,
                        entries,
                        None,
                        *ordered,
                    ))
                }
                other => panic!("empty_child_for: unhandled type {other:?}"),
            }
        }

        fn mk_dense_union(
            fields: &UnionFields,
            type_ids: Vec<i8>,
            offsets: Vec<i32>,
            provide: impl Fn(&Field) -> Option<ArrayRef>,
        ) -> ArrayRef {
            let children: Vec<ArrayRef> = fields
                .iter()
                .map(|(_, f)| provide(f).unwrap_or_else(|| empty_child_for(f.data_type())))
                .collect();

            Arc::new(
                UnionArray::try_new(
                    fields.clone(),
                    ScalarBuffer::<i8>::from(type_ids),
                    Some(ScalarBuffer::<i32>::from(offsets)),
                    children,
                )
                .unwrap(),
            ) as ArrayRef
        }

        // Dates / times / timestamps from the Avro content block:
        let date_a: i32 = 19_000;
        let time_ms_a: i32 = 13 * 3_600_000 + 45 * 60_000 + 30_000 + 123;
        let time_us_b: i64 = 23 * 3_600_000_000 + 59 * 60_000_000 + 59 * 1_000_000 + 999_999;
        let ts_ms_2024_01_01: i64 = 1_704_067_200_000;
        let ts_us_2024_01_01: i64 = ts_ms_2024_01_01 * 1000;
        // Fixed / bytes-like values:
        let fx8_a: [u8; 8] = *b"ABCDEFGH";
        let fx4_abcd: [u8; 4] = *b"ABCD";
        let fx4_misc: [u8; 4] = [0x00, 0x11, 0x22, 0x33];
        let fx10_ascii: [u8; 10] = *b"0123456789";
        let fx10_aa: [u8; 10] = [0xAA; 10];
        // Duration logical values as MonthDayNano:
        let dur_a = IntervalMonthDayNanoType::make_value(1, 2, 3_000_000_000);
        let dur_b = IntervalMonthDayNanoType::make_value(12, 31, 999_000_000);
        // UUID logical values (stored as 16-byte FixedSizeBinary in Arrow):
        let uuid1 = uuid16_from_str("fe7bc30b-4ce8-4c5e-b67c-2234a2d38e66");
        let uuid2 = uuid16_from_str("0826cc06-d2e3-4599-b4ad-af5fa6905cdb");
        // Decimals from Avro content:
        let dec_b_scale2_pos: i128 = 123_456; // "1234.56" bytes-decimal -> (precision=10, scale=2)
        let dec_fix16_neg: i128 = -101; // "-1.01" fixed(16) decimal(10,2)
        let dec_fix20_s4: i128 = 1_234_567_891_234; // "123456789.1234" fixed(20) decimal(20,4)
        let dec_fix20_s4_neg: i128 = -123; // "-0.0123" fixed(20) decimal(20,4)
        let path = "test/data/union_fields.avro";
        let actual = read_file(path, 1024, false);
        let schema = actual.schema();
        // Helper to fetch union metadata for a column
        let get_union = |name: &str| -> (UnionFields, UnionMode) {
            let idx = schema.index_of(name).unwrap();
            match schema.field(idx).data_type() {
                DataType::Union(f, m) => (f.clone(), *m),
                other => panic!("{name} should be a Union, got {other:?}"),
            }
        };
        let mut expected_cols: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
        // 1) ["null","int"]: Int32 (nullable)
        expected_cols.push(Arc::new(Int32Array::from(vec![
            None,
            Some(42),
            None,
            Some(0),
        ])));
        // 2) ["string","null"]: Utf8 (nullable)
        expected_cols.push(Arc::new(StringArray::from(vec![
            Some("s1"),
            None,
            Some("s3"),
            Some(""),
        ])));
        // 3) union_prim: ["boolean","int","long","float","double","bytes","string"]
        {
            let (uf, mode) = get_union("union_prim");
            assert!(matches!(mode, UnionMode::Dense));
            let generated_names: Vec<&str> = uf.iter().map(|(_, f)| f.name().as_str()).collect();
            let expected_names = vec![
                "boolean", "int", "long", "float", "double", "bytes", "string",
            ];
            assert_eq!(
                generated_names, expected_names,
                "Field names for union_prim are incorrect"
            );
            let tids = vec![
                tid_by_name(&uf, "long"),
                tid_by_name(&uf, "int"),
                tid_by_name(&uf, "float"),
                tid_by_name(&uf, "double"),
            ];
            let offs = vec![0, 0, 0, 0];
            let arr = mk_dense_union(&uf, tids, offs, |f| match f.name().as_str() {
                "int" => Some(Arc::new(Int32Array::from(vec![-1])) as ArrayRef),
                "long" => Some(Arc::new(Int64Array::from(vec![1_234_567_890_123i64])) as ArrayRef),
                "float" => {
                    Some(Arc::new(arrow_array::Float32Array::from(vec![1.25f32])) as ArrayRef)
                }
                "double" => {
                    Some(Arc::new(arrow_array::Float64Array::from(vec![-2.5f64])) as ArrayRef)
                }
                _ => None,
            });
            expected_cols.push(arr);
        }
        // 4) union_bytes_vs_string: ["bytes","string"]
        {
            let (uf, _) = get_union("union_bytes_vs_string");
            let tids = vec![
                tid_by_name(&uf, "bytes"),
                tid_by_name(&uf, "string"),
                tid_by_name(&uf, "string"),
                tid_by_name(&uf, "bytes"),
            ];
            let offs = vec![0, 0, 1, 1];
            let arr = mk_dense_union(&uf, tids, offs, |f| match f.name().as_str() {
                "bytes" => Some(
                    Arc::new(BinaryArray::from(vec![&[0x00, 0xFF, 0x7F][..], &[][..]])) as ArrayRef,
                ),
                "string" => Some(Arc::new(StringArray::from(vec!["hello", "world"])) as ArrayRef),
                _ => None,
            });
            expected_cols.push(arr);
        }
        // 5) union_fixed_dur_decfix: [Fx8, Dur12, DecFix16(decimal(10,2))]
        {
            let (uf, _) = get_union("union_fixed_dur_decfix");
            let tid_fx8 = tid_by_dt(&uf, |dt| matches!(dt, DataType::FixedSizeBinary(8)));
            let tid_dur = tid_by_dt(&uf, |dt| {
                matches!(
                    dt,
                    DataType::Interval(arrow_schema::IntervalUnit::MonthDayNano)
                )
            });
            let tid_dec = tid_by_dt(&uf, |dt| match dt {
                #[cfg(feature = "small_decimals")]
                DataType::Decimal64(10, 2) => true,
                DataType::Decimal128(10, 2) | DataType::Decimal256(10, 2) => true,
                _ => false,
            });
            let tids = vec![tid_fx8, tid_dur, tid_dec, tid_dur];
            let offs = vec![0, 0, 0, 1];
            let arr = mk_dense_union(&uf, tids, offs, |f| match f.data_type() {
                DataType::FixedSizeBinary(8) => {
                    let it = [Some(fx8_a)].into_iter();
                    Some(Arc::new(
                        FixedSizeBinaryArray::try_from_sparse_iter_with_size(it, 8).unwrap(),
                    ) as ArrayRef)
                }
                DataType::Interval(IntervalUnit::MonthDayNano) => {
                    Some(Arc::new(arrow_array::IntervalMonthDayNanoArray::from(vec![
                        dur_a, dur_b,
                    ])) as ArrayRef)
                }
                #[cfg(feature = "small_decimals")]
                DataType::Decimal64(10, 2) => {
                    let a = arrow_array::Decimal64Array::from_iter_values([dec_fix16_neg as i64]);
                    Some(Arc::new(a.with_precision_and_scale(10, 2).unwrap()) as ArrayRef)
                }
                DataType::Decimal128(10, 2) => {
                    let a = arrow_array::Decimal128Array::from_iter_values([dec_fix16_neg]);
                    Some(Arc::new(a.with_precision_and_scale(10, 2).unwrap()) as ArrayRef)
                }
                DataType::Decimal256(10, 2) => {
                    let a = arrow_array::Decimal256Array::from_iter_values([i256::from_i128(
                        dec_fix16_neg,
                    )]);
                    Some(Arc::new(a.with_precision_and_scale(10, 2).unwrap()) as ArrayRef)
                }
                _ => None,
            });
            let generated_names: Vec<&str> = uf.iter().map(|(_, f)| f.name().as_str()).collect();
            let expected_names = vec!["Fx8", "Dur12", "DecFix16"];
            assert_eq!(
                generated_names, expected_names,
                "Data type names were not generated correctly for union_fixed_dur_decfix"
            );
            expected_cols.push(arr);
        }
        // 6) union_enum_records_array_map: [enum ColorU, record RecA, record RecB, array<long>, map<string>]
        {
            let (uf, _) = get_union("union_enum_records_array_map");
            let tid_enum = tid_by_dt(&uf, |dt| matches!(dt, DataType::Dictionary(_, _)));
            let tid_reca = tid_by_dt(&uf, |dt| {
                if let DataType::Struct(fs) = dt {
                    fs.len() == 2 && fs[0].name() == "a" && fs[1].name() == "b"
                } else {
                    false
                }
            });
            let tid_recb = tid_by_dt(&uf, |dt| {
                if let DataType::Struct(fs) = dt {
                    fs.len() == 2 && fs[0].name() == "x" && fs[1].name() == "y"
                } else {
                    false
                }
            });
            let tid_arr = tid_by_dt(&uf, |dt| matches!(dt, DataType::List(_)));
            let tids = vec![tid_enum, tid_reca, tid_recb, tid_arr];
            let offs = vec![0, 0, 0, 0];
            let arr = mk_dense_union(&uf, tids, offs, |f| match f.data_type() {
                DataType::Dictionary(_, _) => {
                    let keys = Int32Array::from(vec![0i32]); // "RED"
                    let values =
                        Arc::new(StringArray::from(vec!["RED", "GREEN", "BLUE"])) as ArrayRef;
                    Some(
                        Arc::new(DictionaryArray::<Int32Type>::try_new(keys, values).unwrap())
                            as ArrayRef,
                    )
                }
                DataType::Struct(fs)
                    if fs.len() == 2 && fs[0].name() == "a" && fs[1].name() == "b" =>
                {
                    let a = Int32Array::from(vec![7]);
                    let b = StringArray::from(vec!["x"]);
                    Some(Arc::new(StructArray::new(
                        fs.clone(),
                        vec![Arc::new(a), Arc::new(b)],
                        None,
                    )) as ArrayRef)
                }
                DataType::Struct(fs)
                    if fs.len() == 2 && fs[0].name() == "x" && fs[1].name() == "y" =>
                {
                    let x = Int64Array::from(vec![123_456_789i64]);
                    let y = BinaryArray::from(vec![&[0xFF, 0x00][..]]);
                    Some(Arc::new(StructArray::new(
                        fs.clone(),
                        vec![Arc::new(x), Arc::new(y)],
                        None,
                    )) as ArrayRef)
                }
                DataType::List(field) => {
                    let values = Int64Array::from(vec![1i64, 2, 3]);
                    let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 3]));
                    Some(Arc::new(
                        ListArray::try_new(field.clone(), offsets, Arc::new(values), None).unwrap(),
                    ) as ArrayRef)
                }
                DataType::Map(_, _) => None,
                other => panic!("unexpected child {other:?}"),
            });
            expected_cols.push(arr);
        }
        // 7) union_date_or_fixed4: [date32, fixed(4)]
        {
            let (uf, _) = get_union("union_date_or_fixed4");
            let tid_date = tid_by_dt(&uf, |dt| matches!(dt, DataType::Date32));
            let tid_fx4 = tid_by_dt(&uf, |dt| matches!(dt, DataType::FixedSizeBinary(4)));
            let tids = vec![tid_date, tid_fx4, tid_date, tid_fx4];
            let offs = vec![0, 0, 1, 1];
            let arr = mk_dense_union(&uf, tids, offs, |f| match f.data_type() {
                DataType::Date32 => {
                    Some(Arc::new(arrow_array::Date32Array::from(vec![date_a, 0])) as ArrayRef)
                }
                DataType::FixedSizeBinary(4) => {
                    let it = [Some(fx4_abcd), Some(fx4_misc)].into_iter();
                    Some(Arc::new(
                        FixedSizeBinaryArray::try_from_sparse_iter_with_size(it, 4).unwrap(),
                    ) as ArrayRef)
                }
                _ => None,
            });
            expected_cols.push(arr);
        }
        // 8) union_time_millis_or_enum: [time-millis, enum OnOff]
        {
            let (uf, _) = get_union("union_time_millis_or_enum");
            let tid_ms = tid_by_dt(&uf, |dt| {
                matches!(dt, DataType::Time32(arrow_schema::TimeUnit::Millisecond))
            });
            let tid_en = tid_by_dt(&uf, |dt| matches!(dt, DataType::Dictionary(_, _)));
            let tids = vec![tid_ms, tid_en, tid_en, tid_ms];
            let offs = vec![0, 0, 1, 1];
            let arr = mk_dense_union(&uf, tids, offs, |f| match f.data_type() {
                DataType::Time32(arrow_schema::TimeUnit::Millisecond) => {
                    Some(Arc::new(Time32MillisecondArray::from(vec![time_ms_a, 0])) as ArrayRef)
                }
                DataType::Dictionary(_, _) => {
                    let keys = Int32Array::from(vec![0i32, 1]); // "ON", "OFF"
                    let values = Arc::new(StringArray::from(vec!["ON", "OFF"])) as ArrayRef;
                    Some(
                        Arc::new(DictionaryArray::<Int32Type>::try_new(keys, values).unwrap())
                            as ArrayRef,
                    )
                }
                _ => None,
            });
            expected_cols.push(arr);
        }
        // 9) union_time_micros_or_string: [time-micros, string]
        {
            let (uf, _) = get_union("union_time_micros_or_string");
            let tid_us = tid_by_dt(&uf, |dt| {
                matches!(dt, DataType::Time64(arrow_schema::TimeUnit::Microsecond))
            });
            let tid_s = tid_by_name(&uf, "string");
            let tids = vec![tid_s, tid_us, tid_s, tid_s];
            let offs = vec![0, 0, 1, 2];
            let arr = mk_dense_union(&uf, tids, offs, |f| match f.data_type() {
                DataType::Time64(arrow_schema::TimeUnit::Microsecond) => {
                    Some(Arc::new(Time64MicrosecondArray::from(vec![time_us_b])) as ArrayRef)
                }
                DataType::Utf8 => {
                    Some(Arc::new(StringArray::from(vec!["evening", "night", ""])) as ArrayRef)
                }
                _ => None,
            });
            expected_cols.push(arr);
        }
        // 10) union_ts_millis_utc_or_array: [timestamp-millis(TZ), array<int>]
        {
            let (uf, _) = get_union("union_ts_millis_utc_or_array");
            let tid_ts = tid_by_dt(&uf, |dt| {
                matches!(
                    dt,
                    DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, _)
                )
            });
            let tid_arr = tid_by_dt(&uf, |dt| matches!(dt, DataType::List(_)));
            let tids = vec![tid_ts, tid_arr, tid_arr, tid_ts];
            let offs = vec![0, 0, 1, 1];
            let arr = mk_dense_union(&uf, tids, offs, |f| match f.data_type() {
                DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, tz) => {
                    let a = TimestampMillisecondArray::from(vec![
                        ts_ms_2024_01_01,
                        ts_ms_2024_01_01 + 86_400_000,
                    ]);
                    Some(Arc::new(if let Some(tz) = tz {
                        a.with_timezone(tz.clone())
                    } else {
                        a
                    }) as ArrayRef)
                }
                DataType::List(field) => {
                    let values = Int32Array::from(vec![0, 1, 2, -1, 0, 1]);
                    let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 3, 6]));
                    Some(Arc::new(
                        ListArray::try_new(field.clone(), offsets, Arc::new(values), None).unwrap(),
                    ) as ArrayRef)
                }
                _ => None,
            });
            expected_cols.push(arr);
        }
        // 11) union_ts_micros_local_or_bytes: [local-timestamp-micros, bytes]
        {
            let (uf, _) = get_union("union_ts_micros_local_or_bytes");
            let tid_lts = tid_by_dt(&uf, |dt| {
                matches!(
                    dt,
                    DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None)
                )
            });
            let tid_b = tid_by_name(&uf, "bytes");
            let tids = vec![tid_b, tid_lts, tid_b, tid_b];
            let offs = vec![0, 0, 1, 2];
            let arr = mk_dense_union(&uf, tids, offs, |f| match f.data_type() {
                DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None) => Some(Arc::new(
                    TimestampMicrosecondArray::from(vec![ts_us_2024_01_01]),
                )
                    as ArrayRef),
                DataType::Binary => Some(Arc::new(BinaryArray::from(vec![
                    &b"\x11\x22\x33"[..],
                    &b"\x00"[..],
                    &b"\x10\x20\x30\x40"[..],
                ])) as ArrayRef),
                _ => None,
            });
            expected_cols.push(arr);
        }
        // 12) union_uuid_or_fixed10: [uuid(string)->fixed(16), fixed(10)]
        {
            let (uf, _) = get_union("union_uuid_or_fixed10");
            let tid_fx16 = tid_by_dt(&uf, |dt| matches!(dt, DataType::FixedSizeBinary(16)));
            let tid_fx10 = tid_by_dt(&uf, |dt| matches!(dt, DataType::FixedSizeBinary(10)));
            let tids = vec![tid_fx16, tid_fx10, tid_fx16, tid_fx10];
            let offs = vec![0, 0, 1, 1];
            let arr = mk_dense_union(&uf, tids, offs, |f| match f.data_type() {
                DataType::FixedSizeBinary(16) => {
                    let it = [Some(uuid1), Some(uuid2)].into_iter();
                    Some(Arc::new(
                        FixedSizeBinaryArray::try_from_sparse_iter_with_size(it, 16).unwrap(),
                    ) as ArrayRef)
                }
                DataType::FixedSizeBinary(10) => {
                    let it = [Some(fx10_ascii), Some(fx10_aa)].into_iter();
                    Some(Arc::new(
                        FixedSizeBinaryArray::try_from_sparse_iter_with_size(it, 10).unwrap(),
                    ) as ArrayRef)
                }
                _ => None,
            });
            expected_cols.push(arr);
        }
        // 13) union_dec_bytes_or_dec_fixed: [bytes dec(10,2), fixed(20) dec(20,4)]
        {
            let (uf, _) = get_union("union_dec_bytes_or_dec_fixed");
            let tid_b10s2 = tid_by_dt(&uf, |dt| match dt {
                #[cfg(feature = "small_decimals")]
                DataType::Decimal64(10, 2) => true,
                DataType::Decimal128(10, 2) | DataType::Decimal256(10, 2) => true,
                _ => false,
            });
            let tid_f20s4 = tid_by_dt(&uf, |dt| {
                matches!(
                    dt,
                    DataType::Decimal128(20, 4) | DataType::Decimal256(20, 4)
                )
            });
            let tids = vec![tid_b10s2, tid_f20s4, tid_b10s2, tid_f20s4];
            let offs = vec![0, 0, 1, 1];
            let arr = mk_dense_union(&uf, tids, offs, |f| match f.data_type() {
                #[cfg(feature = "small_decimals")]
                DataType::Decimal64(10, 2) => {
                    let a = Decimal64Array::from_iter_values([dec_b_scale2_pos as i64, 0i64]);
                    Some(Arc::new(a.with_precision_and_scale(10, 2).unwrap()) as ArrayRef)
                }
                DataType::Decimal128(10, 2) => {
                    let a = Decimal128Array::from_iter_values([dec_b_scale2_pos, 0]);
                    Some(Arc::new(a.with_precision_and_scale(10, 2).unwrap()) as ArrayRef)
                }
                DataType::Decimal256(10, 2) => {
                    let a = Decimal256Array::from_iter_values([
                        i256::from_i128(dec_b_scale2_pos),
                        i256::from(0),
                    ]);
                    Some(Arc::new(a.with_precision_and_scale(10, 2).unwrap()) as ArrayRef)
                }
                DataType::Decimal128(20, 4) => {
                    let a = Decimal128Array::from_iter_values([dec_fix20_s4_neg, dec_fix20_s4]);
                    Some(Arc::new(a.with_precision_and_scale(20, 4).unwrap()) as ArrayRef)
                }
                DataType::Decimal256(20, 4) => {
                    let a = Decimal256Array::from_iter_values([
                        i256::from_i128(dec_fix20_s4_neg),
                        i256::from_i128(dec_fix20_s4),
                    ]);
                    Some(Arc::new(a.with_precision_and_scale(20, 4).unwrap()) as ArrayRef)
                }
                _ => None,
            });
            expected_cols.push(arr);
        }
        // 14) union_null_bytes_string: ["null","bytes","string"]
        {
            let (uf, _) = get_union("union_null_bytes_string");
            let tid_n = tid_by_name(&uf, "null");
            let tid_b = tid_by_name(&uf, "bytes");
            let tid_s = tid_by_name(&uf, "string");
            let tids = vec![tid_n, tid_b, tid_s, tid_s];
            let offs = vec![0, 0, 0, 1];
            let arr = mk_dense_union(&uf, tids, offs, |f| match f.name().as_str() {
                "null" => Some(Arc::new(arrow_array::NullArray::new(1)) as ArrayRef),
                "bytes" => Some(Arc::new(BinaryArray::from(vec![&b"\x01\x02"[..]])) as ArrayRef),
                "string" => Some(Arc::new(StringArray::from(vec!["text", "u"])) as ArrayRef),
                _ => None,
            });
            expected_cols.push(arr);
        }
        // 15) array_of_union: array<[long,string]>
        {
            let idx = schema.index_of("array_of_union").unwrap();
            let dt = schema.field(idx).data_type().clone();
            let (item_field, _) = match &dt {
                DataType::List(f) => (f.clone(), ()),
                other => panic!("array_of_union must be List, got {other:?}"),
            };
            let (uf, _) = match item_field.data_type() {
                DataType::Union(f, m) => (f.clone(), m),
                other => panic!("array_of_union items must be Union, got {other:?}"),
            };
            let tid_l = tid_by_name(&uf, "long");
            let tid_s = tid_by_name(&uf, "string");
            let type_ids = vec![tid_l, tid_s, tid_l, tid_s, tid_l, tid_l, tid_s, tid_l];
            let offsets = vec![0, 0, 1, 1, 2, 3, 2, 4];
            let values_union =
                mk_dense_union(&uf, type_ids, offsets, |f| match f.name().as_str() {
                    "long" => {
                        Some(Arc::new(Int64Array::from(vec![1i64, -5, 42, -1, 0])) as ArrayRef)
                    }
                    "string" => Some(Arc::new(StringArray::from(vec!["a", "", "z"])) as ArrayRef),
                    _ => None,
                });
            let list_offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 3, 5, 6, 8]));
            expected_cols.push(Arc::new(
                ListArray::try_new(item_field.clone(), list_offsets, values_union, None).unwrap(),
            ));
        }
        // 16) map_of_union: map<[null,double]>
        {
            let idx = schema.index_of("map_of_union").unwrap();
            let dt = schema.field(idx).data_type().clone();
            let (entry_field, ordered) = match &dt {
                DataType::Map(f, ordered) => (f.clone(), *ordered),
                other => panic!("map_of_union must be Map, got {other:?}"),
            };
            let DataType::Struct(entry_fields) = entry_field.data_type() else {
                panic!("map entries must be struct")
            };
            let key_field = entry_fields[0].clone();
            let val_field = entry_fields[1].clone();
            let keys = StringArray::from(vec!["a", "b", "x", "pi"]);
            let rounded_pi = (std::f64::consts::PI * 100_000.0).round() / 100_000.0;
            let values: ArrayRef = match val_field.data_type() {
                DataType::Union(uf, _) => {
                    let tid_n = tid_by_name(uf, "null");
                    let tid_d = tid_by_name(uf, "double");
                    let tids = vec![tid_n, tid_d, tid_d, tid_d];
                    let offs = vec![0, 0, 1, 2];
                    mk_dense_union(uf, tids, offs, |f| match f.name().as_str() {
                        "null" => Some(Arc::new(NullArray::new(1)) as ArrayRef),
                        "double" => Some(Arc::new(arrow_array::Float64Array::from(vec![
                            2.5f64, -0.5f64, rounded_pi,
                        ])) as ArrayRef),
                        _ => None,
                    })
                }
                DataType::Float64 => Arc::new(arrow_array::Float64Array::from(vec![
                    None,
                    Some(2.5),
                    Some(-0.5),
                    Some(rounded_pi),
                ])),
                other => panic!("unexpected map value type {other:?}"),
            };
            let entries = StructArray::new(
                Fields::from(vec![key_field.as_ref().clone(), val_field.as_ref().clone()]),
                vec![Arc::new(keys) as ArrayRef, values],
                None,
            );
            let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 2, 3, 3, 4]));
            expected_cols.push(Arc::new(MapArray::new(
                entry_field,
                offsets,
                entries,
                None,
                ordered,
            )));
        }
        // 17) record_with_union_field: struct { id:int, u:[int,string] }
        {
            let idx = schema.index_of("record_with_union_field").unwrap();
            let DataType::Struct(rec_fields) = schema.field(idx).data_type() else {
                panic!("record_with_union_field should be Struct")
            };
            let id = Int32Array::from(vec![1, 2, 3, 4]);
            let u_field = rec_fields.iter().find(|f| f.name() == "u").unwrap();
            let DataType::Union(uf, _) = u_field.data_type() else {
                panic!("u must be Union")
            };
            let tid_i = tid_by_name(uf, "int");
            let tid_s = tid_by_name(uf, "string");
            let tids = vec![tid_s, tid_i, tid_i, tid_s];
            let offs = vec![0, 0, 1, 1];
            let u = mk_dense_union(uf, tids, offs, |f| match f.name().as_str() {
                "int" => Some(Arc::new(Int32Array::from(vec![99, 0])) as ArrayRef),
                "string" => Some(Arc::new(StringArray::from(vec!["one", "four"])) as ArrayRef),
                _ => None,
            });
            let rec = StructArray::new(rec_fields.clone(), vec![Arc::new(id) as ArrayRef, u], None);
            expected_cols.push(Arc::new(rec));
        }
        // 18) union_ts_micros_utc_or_map: [timestamp-micros(TZ), map<long>]
        {
            let (uf, _) = get_union("union_ts_micros_utc_or_map");
            let tid_ts = tid_by_dt(&uf, |dt| {
                matches!(
                    dt,
                    DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some(_))
                )
            });
            let tid_map = tid_by_dt(&uf, |dt| matches!(dt, DataType::Map(_, _)));
            let tids = vec![tid_ts, tid_map, tid_ts, tid_map];
            let offs = vec![0, 0, 1, 1];
            let arr = mk_dense_union(&uf, tids, offs, |f| match f.data_type() {
                DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, tz) => {
                    let a = TimestampMicrosecondArray::from(vec![ts_us_2024_01_01, 0i64]);
                    Some(Arc::new(if let Some(tz) = tz {
                        a.with_timezone(tz.clone())
                    } else {
                        a
                    }) as ArrayRef)
                }
                DataType::Map(entry_field, ordered) => {
                    let DataType::Struct(fs) = entry_field.data_type() else {
                        panic!("map entries must be struct")
                    };
                    let key_field = fs[0].clone();
                    let val_field = fs[1].clone();
                    assert_eq!(key_field.data_type(), &DataType::Utf8);
                    assert_eq!(val_field.data_type(), &DataType::Int64);
                    let keys = StringArray::from(vec!["k1", "k2", "n"]);
                    let vals = Int64Array::from(vec![1i64, 2, 0]);
                    let entries = StructArray::new(
                        Fields::from(vec![key_field.as_ref().clone(), val_field.as_ref().clone()]),
                        vec![Arc::new(keys) as ArrayRef, Arc::new(vals) as ArrayRef],
                        None,
                    );
                    let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 2, 3]));
                    Some(Arc::new(MapArray::new(
                        entry_field.clone(),
                        offsets,
                        entries,
                        None,
                        *ordered,
                    )) as ArrayRef)
                }
                _ => None,
            });
            expected_cols.push(arr);
        }
        // 19) union_ts_millis_local_or_string: [local-timestamp-millis, string]
        {
            let (uf, _) = get_union("union_ts_millis_local_or_string");
            let tid_ts = tid_by_dt(&uf, |dt| {
                matches!(
                    dt,
                    DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None)
                )
            });
            let tid_s = tid_by_name(&uf, "string");
            let tids = vec![tid_s, tid_ts, tid_s, tid_s];
            let offs = vec![0, 0, 1, 2];
            let arr = mk_dense_union(&uf, tids, offs, |f| match f.data_type() {
                DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None) => Some(Arc::new(
                    TimestampMillisecondArray::from(vec![ts_ms_2024_01_01]),
                )
                    as ArrayRef),
                DataType::Utf8 => {
                    Some(
                        Arc::new(StringArray::from(vec!["local midnight", "done", ""])) as ArrayRef,
                    )
                }
                _ => None,
            });
            expected_cols.push(arr);
        }
        // 20) union_bool_or_string: ["boolean","string"]
        {
            let (uf, _) = get_union("union_bool_or_string");
            let tid_b = tid_by_name(&uf, "boolean");
            let tid_s = tid_by_name(&uf, "string");
            let tids = vec![tid_b, tid_s, tid_b, tid_s];
            let offs = vec![0, 0, 1, 1];
            let arr = mk_dense_union(&uf, tids, offs, |f| match f.name().as_str() {
                "boolean" => Some(Arc::new(BooleanArray::from(vec![true, false])) as ArrayRef),
                "string" => Some(Arc::new(StringArray::from(vec!["no", "yes"])) as ArrayRef),
                _ => None,
            });
            expected_cols.push(arr);
        }
        let expected = RecordBatch::try_new(schema.clone(), expected_cols).unwrap();
        assert_eq!(
            actual, expected,
            "full end-to-end equality for union_fields.avro"
        );
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

        for file in files() {
            let file = arrow_test_data(file);

            assert_eq!(read_file(&file, 8, false), expected);
            assert_eq!(read_file(&file, 3, false), expected);
        }
    }

    #[test]
    // TODO: avoid requiring snappy for this file
    #[cfg(feature = "snappy")]
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
    // TODO: avoid requiring snappy for this file
    #[cfg(feature = "snappy")]
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
    // TODO: avoid requiring snappy for these files
    #[cfg(feature = "snappy")]
    fn test_decimal() {
        // Choose expected Arrow types depending on the `small_decimals` feature flag.
        // With `small_decimals` enabled, Decimal32/Decimal64 are used where their
        // precision allows; otherwise, those cases resolve to Decimal128.
        #[cfg(feature = "small_decimals")]
        let files: [(&str, DataType, HashMap<String, String>); 8] = [
            (
                "avro/fixed_length_decimal.avro",
                DataType::Decimal128(25, 2),
                HashMap::from([
                    (
                        "avro.namespace".to_string(),
                        "topLevelRecord.value".to_string(),
                    ),
                    ("avro.name".to_string(), "fixed".to_string()),
                ]),
            ),
            (
                "avro/fixed_length_decimal_legacy.avro",
                DataType::Decimal64(13, 2),
                HashMap::from([
                    (
                        "avro.namespace".to_string(),
                        "topLevelRecord.value".to_string(),
                    ),
                    ("avro.name".to_string(), "fixed".to_string()),
                ]),
            ),
            (
                "avro/int32_decimal.avro",
                DataType::Decimal32(4, 2),
                HashMap::from([
                    (
                        "avro.namespace".to_string(),
                        "topLevelRecord.value".to_string(),
                    ),
                    ("avro.name".to_string(), "fixed".to_string()),
                ]),
            ),
            (
                "avro/int64_decimal.avro",
                DataType::Decimal64(10, 2),
                HashMap::from([
                    (
                        "avro.namespace".to_string(),
                        "topLevelRecord.value".to_string(),
                    ),
                    ("avro.name".to_string(), "fixed".to_string()),
                ]),
            ),
            (
                "test/data/int256_decimal.avro",
                DataType::Decimal256(76, 10),
                HashMap::new(),
            ),
            (
                "test/data/fixed256_decimal.avro",
                DataType::Decimal256(76, 10),
                HashMap::from([("avro.name".to_string(), "Decimal256Fixed".to_string())]),
            ),
            (
                "test/data/fixed_length_decimal_legacy_32.avro",
                DataType::Decimal32(9, 2),
                HashMap::from([("avro.name".to_string(), "Decimal32FixedLegacy".to_string())]),
            ),
            (
                "test/data/int128_decimal.avro",
                DataType::Decimal128(38, 2),
                HashMap::new(),
            ),
        ];
        #[cfg(not(feature = "small_decimals"))]
        let files: [(&str, DataType, HashMap<String, String>); 8] = [
            (
                "avro/fixed_length_decimal.avro",
                DataType::Decimal128(25, 2),
                HashMap::from([
                    (
                        "avro.namespace".to_string(),
                        "topLevelRecord.value".to_string(),
                    ),
                    ("avro.name".to_string(), "fixed".to_string()),
                ]),
            ),
            (
                "avro/fixed_length_decimal_legacy.avro",
                DataType::Decimal128(13, 2),
                HashMap::from([
                    (
                        "avro.namespace".to_string(),
                        "topLevelRecord.value".to_string(),
                    ),
                    ("avro.name".to_string(), "fixed".to_string()),
                ]),
            ),
            (
                "avro/int32_decimal.avro",
                DataType::Decimal128(4, 2),
                HashMap::from([
                    (
                        "avro.namespace".to_string(),
                        "topLevelRecord.value".to_string(),
                    ),
                    ("avro.name".to_string(), "fixed".to_string()),
                ]),
            ),
            (
                "avro/int64_decimal.avro",
                DataType::Decimal128(10, 2),
                HashMap::from([
                    (
                        "avro.namespace".to_string(),
                        "topLevelRecord.value".to_string(),
                    ),
                    ("avro.name".to_string(), "fixed".to_string()),
                ]),
            ),
            (
                "test/data/int256_decimal.avro",
                DataType::Decimal256(76, 10),
                HashMap::new(),
            ),
            (
                "test/data/fixed256_decimal.avro",
                DataType::Decimal256(76, 10),
                HashMap::from([("avro.name".to_string(), "Decimal256Fixed".to_string())]),
            ),
            (
                "test/data/fixed_length_decimal_legacy_32.avro",
                DataType::Decimal128(9, 2),
                HashMap::from([("avro.name".to_string(), "Decimal32FixedLegacy".to_string())]),
            ),
            (
                "test/data/int128_decimal.avro",
                DataType::Decimal128(38, 2),
                HashMap::new(),
            ),
        ];
        for (file, expected_dt, mut metadata) in files {
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
            metadata.insert("precision".to_string(), precision.to_string());
            metadata.insert("scale".to_string(), scale.to_string());
            let field =
                Field::new("value", expected_dt.clone(), actual_nullable).with_metadata(metadata);
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
    fn test_read_duration_logical_types_feature_toggle() -> Result<(), ArrowError> {
        let file_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("test/data/duration_logical_types.avro")
            .to_string_lossy()
            .into_owned();

        let actual_batch = read_file(&file_path, 4, false);

        let expected_batch = {
            #[cfg(feature = "avro_custom_types")]
            {
                let schema = Arc::new(Schema::new(vec![
                    Field::new(
                        "duration_time_nanos",
                        DataType::Duration(TimeUnit::Nanosecond),
                        false,
                    ),
                    Field::new(
                        "duration_time_micros",
                        DataType::Duration(TimeUnit::Microsecond),
                        false,
                    ),
                    Field::new(
                        "duration_time_millis",
                        DataType::Duration(TimeUnit::Millisecond),
                        false,
                    ),
                    Field::new(
                        "duration_time_seconds",
                        DataType::Duration(TimeUnit::Second),
                        false,
                    ),
                ]));

                let nanos = Arc::new(PrimitiveArray::<DurationNanosecondType>::from(vec![
                    10, 20, 30, 40,
                ])) as ArrayRef;
                let micros = Arc::new(PrimitiveArray::<DurationMicrosecondType>::from(vec![
                    100, 200, 300, 400,
                ])) as ArrayRef;
                let millis = Arc::new(PrimitiveArray::<DurationMillisecondType>::from(vec![
                    1000, 2000, 3000, 4000,
                ])) as ArrayRef;
                let seconds = Arc::new(PrimitiveArray::<DurationSecondType>::from(vec![1, 2, 3, 4]))
                    as ArrayRef;

                RecordBatch::try_new(schema, vec![nanos, micros, millis, seconds])?
            }
            #[cfg(not(feature = "avro_custom_types"))]
            {
                let schema = Arc::new(Schema::new(vec![
                    Field::new("duration_time_nanos", DataType::Int64, false).with_metadata(
                        [(
                            "logicalType".to_string(),
                            "arrow.duration-nanos".to_string(),
                        )]
                        .into(),
                    ),
                    Field::new("duration_time_micros", DataType::Int64, false).with_metadata(
                        [(
                            "logicalType".to_string(),
                            "arrow.duration-micros".to_string(),
                        )]
                        .into(),
                    ),
                    Field::new("duration_time_millis", DataType::Int64, false).with_metadata(
                        [(
                            "logicalType".to_string(),
                            "arrow.duration-millis".to_string(),
                        )]
                        .into(),
                    ),
                    Field::new("duration_time_seconds", DataType::Int64, false).with_metadata(
                        [(
                            "logicalType".to_string(),
                            "arrow.duration-seconds".to_string(),
                        )]
                        .into(),
                    ),
                ]));

                let nanos =
                    Arc::new(PrimitiveArray::<Int64Type>::from(vec![10, 20, 30, 40])) as ArrayRef;
                let micros = Arc::new(PrimitiveArray::<Int64Type>::from(vec![100, 200, 300, 400]))
                    as ArrayRef;
                let millis = Arc::new(PrimitiveArray::<Int64Type>::from(vec![
                    1000, 2000, 3000, 4000,
                ])) as ArrayRef;
                let seconds =
                    Arc::new(PrimitiveArray::<Int64Type>::from(vec![1, 2, 3, 4])) as ArrayRef;

                RecordBatch::try_new(schema, vec![nanos, micros, millis, seconds])?
            }
        };

        assert_eq!(actual_batch, expected_batch);

        Ok(())
    }

    #[test]
    // TODO: avoid requiring snappy for this file
    #[cfg(feature = "snappy")]
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
    // TODO: avoid requiring snappy for this file
    #[cfg(feature = "snappy")]
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
    #[cfg(feature = "snappy")]
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
            md_f1.insert(AVRO_NAME_METADATA_KEY.to_string(), "enum1".to_string());
            md_f1.insert(AVRO_NAMESPACE_METADATA_KEY.to_string(), "ns1".to_string());
            let f1_field = Field::new("f1", dict_type.clone(), false).with_metadata(md_f1);
            let mut md_f2 = HashMap::new();
            md_f2.insert(
                AVRO_ENUM_SYMBOLS_METADATA_KEY.to_string(),
                r#"["e","f","g","h"]"#.to_string(),
            );
            md_f2.insert(AVRO_NAME_METADATA_KEY.to_string(), "enum2".to_string());
            md_f2.insert(AVRO_NAMESPACE_METADATA_KEY.to_string(), "ns2".to_string());
            let f2_field = Field::new("f2", dict_type.clone(), false).with_metadata(md_f2);
            let mut md_f3 = HashMap::new();
            md_f3.insert(
                AVRO_ENUM_SYMBOLS_METADATA_KEY.to_string(),
                r#"["i","j","k"]"#.to_string(),
            );
            md_f3.insert(AVRO_NAME_METADATA_KEY.to_string(), "enum3".to_string());
            md_f3.insert(AVRO_NAMESPACE_METADATA_KEY.to_string(), "ns1".to_string());
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

            // Add Avro named-type metadata for fixed fields
            let mut md_f1 = HashMap::new();
            md_f1.insert(
                crate::schema::AVRO_NAME_METADATA_KEY.to_string(),
                "fixed1".to_string(),
            );
            md_f1.insert(
                crate::schema::AVRO_NAMESPACE_METADATA_KEY.to_string(),
                "ns1".to_string(),
            );

            let mut md_f2 = HashMap::new();
            md_f2.insert(
                crate::schema::AVRO_NAME_METADATA_KEY.to_string(),
                "fixed2".to_string(),
            );
            md_f2.insert(
                crate::schema::AVRO_NAMESPACE_METADATA_KEY.to_string(),
                "ns2".to_string(),
            );

            let mut md_f3 = HashMap::new();
            md_f3.insert(
                crate::schema::AVRO_NAME_METADATA_KEY.to_string(),
                "fixed3".to_string(),
            );
            md_f3.insert(
                crate::schema::AVRO_NAMESPACE_METADATA_KEY.to_string(),
                "ns1".to_string(),
            );

            let expected_schema = Arc::new(Schema::new(vec![
                Field::new("f1", DataType::FixedSizeBinary(5), false).with_metadata(md_f1),
                Field::new("f2", DataType::FixedSizeBinary(10), false).with_metadata(md_f2),
                Field::new("f3", DataType::FixedSizeBinary(6), true).with_metadata(md_f3),
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
    #[cfg(feature = "snappy")]
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
    #[cfg(feature = "snappy")]
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
        // Add Avro named-type metadata to nested field f1_3 (ns3.record3)
        let mut f1_3_md: HashMap<String, String> = HashMap::new();
        f1_3_md.insert(AVRO_NAMESPACE_METADATA_KEY.to_string(), "ns3".to_string());
        f1_3_md.insert(AVRO_NAME_METADATA_KEY.to_string(), "record3".to_string());
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
                Arc::new(
                    Field::new(
                        "f1_3",
                        DataType::Struct(Fields::from(vec![Field::new(
                            "f1_3_1",
                            DataType::Float64,
                            false,
                        )])),
                        false,
                    )
                    .with_metadata(f1_3_md),
                ),
                Arc::new(f1_f1_3) as Arc<dyn Array>,
            ),
        ]);
        let f2_fields = [
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
        // Add Avro named-type metadata to f2's list item (ns4.record4)
        let mut f2_item_md: HashMap<String, String> = HashMap::new();
        f2_item_md.insert(AVRO_NAME_METADATA_KEY.to_string(), "record4".to_string());
        f2_item_md.insert(AVRO_NAMESPACE_METADATA_KEY.to_string(), "ns4".to_string());
        let item_field = Arc::new(
            Field::new(
                "item",
                list_array_with_nullable_items.values().data_type().clone(),
                false, // items are non-nullable for f2
            )
            .with_metadata(f2_item_md),
        );
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
        // Add Avro named-type metadata to f4's list item (ns6.record6), item is nullable
        let mut f4_item_md: HashMap<String, String> = HashMap::new();
        f4_item_md.insert(AVRO_NAMESPACE_METADATA_KEY.to_string(), "ns6".to_string());
        f4_item_md.insert(AVRO_NAME_METADATA_KEY.to_string(), "record6".to_string());
        let f4_item_field = Arc::new(
            Field::new("item", f4_expected.values().data_type().clone(), true)
                .with_metadata(f4_item_md),
        );
        let f4_list_data_type = DataType::List(f4_item_field);
        let f4_array_data = f4_expected
            .to_data()
            .into_builder()
            .data_type(f4_list_data_type)
            .build()
            .unwrap();
        let f4_expected = ListArray::from(f4_array_data);
        // Build Schema with Avro named-type metadata on the top-level f1 and f3 fields
        let mut f1_md: HashMap<String, String> = HashMap::new();
        f1_md.insert(AVRO_NAME_METADATA_KEY.to_string(), "record2".to_string());
        f1_md.insert(AVRO_NAMESPACE_METADATA_KEY.to_string(), "ns2".to_string());
        let mut f3_md: HashMap<String, String> = HashMap::new();
        f3_md.insert(AVRO_NAMESPACE_METADATA_KEY.to_string(), "ns5".to_string());
        f3_md.insert(AVRO_NAME_METADATA_KEY.to_string(), "record5".to_string());
        let expected_schema = Schema::new(vec![
            Field::new("f1", f1_expected.data_type().clone(), false).with_metadata(f1_md),
            Field::new("f2", f2_expected.data_type().clone(), false),
            Field::new("f3", f3_expected.data_type().clone(), true).with_metadata(f3_md),
            Field::new("f4", f4_expected.data_type().clone(), false),
        ]);
        let expected = RecordBatch::try_new(
            Arc::new(expected_schema),
            vec![
                Arc::new(f1_expected) as Arc<dyn Array>,
                Arc::new(f2_expected) as Arc<dyn Array>,
                Arc::new(f3_expected) as Arc<dyn Array>,
                Arc::new(f4_expected) as Arc<dyn Array>,
            ],
        )
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
    // TODO: avoid requiring snappy for this file
    #[cfg(feature = "snappy")]
    fn test_repeated_no_annotation() {
        use arrow_data::ArrayDataBuilder;
        let file = arrow_test_data("avro/repeated_no_annotation.avro");
        let batch_large = read_file(&file, 8, false);
        // id column
        let id_array = Int32Array::from(vec![1, 2, 3, 4, 5, 6]);
        // Build the inner Struct<number:int64, kind:utf8>
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
        // Build List<item: Struct<...>> with Avro named-type metadata on the *element* field
        let phone_list_offsets = Buffer::from_slice_ref([0i32, 0, 0, 0, 1, 2, 5]);
        let phone_list_validity = Buffer::from_iter([false, false, true, true, true, true]);
        // The Avro schema names this inner record "phone" in namespace "topLevelRecord.phoneNumbers"
        let mut phone_item_md = HashMap::new();
        phone_item_md.insert(AVRO_NAME_METADATA_KEY.to_string(), "phone".to_string());
        phone_item_md.insert(
            AVRO_NAMESPACE_METADATA_KEY.to_string(),
            "topLevelRecord.phoneNumbers".to_string(),
        );
        let phone_item_field = Field::new("item", phone_struct_array.data_type().clone(), true)
            .with_metadata(phone_item_md);
        let phone_list_data = ArrayDataBuilder::new(DataType::List(Arc::new(phone_item_field)))
            .len(6)
            .add_buffer(phone_list_offsets)
            .null_bit_buffer(Some(phone_list_validity))
            .child_data(vec![phone_struct_array.into_data()])
            .build()
            .unwrap();
        let phone_list_array = ListArray::from(phone_list_data);
        // Wrap in Struct { phone: List<...> }
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
        // Build the expected Schema, annotating the top-level "phoneNumbers" field with Avro name/namespace
        let mut phone_numbers_md = HashMap::new();
        phone_numbers_md.insert(
            AVRO_NAME_METADATA_KEY.to_string(),
            "phoneNumbers".to_string(),
        );
        phone_numbers_md.insert(
            AVRO_NAMESPACE_METADATA_KEY.to_string(),
            "topLevelRecord".to_string(),
        );
        let id_field = Field::new("id", DataType::Int32, true);
        let phone_numbers_schema_field = Field::new(
            "phoneNumbers",
            phone_numbers_struct_array.data_type().clone(),
            true,
        )
        .with_metadata(phone_numbers_md);
        let expected_schema = Schema::new(vec![id_field, phone_numbers_schema_field]);
        // Final expected RecordBatch (arrays already carry matching list-element metadata)
        let expected = RecordBatch::try_new(
            Arc::new(expected_schema),
            vec![
                Arc::new(id_array) as _,
                Arc::new(phone_numbers_struct_array) as _,
            ],
        )
        .unwrap();
        assert_eq!(batch_large, expected, "Mismatch for batch_size=8");
        let batch_small = read_file(&file, 3, false);
        assert_eq!(batch_small, expected, "Mismatch for batch_size=3");
    }

    #[test]
    // TODO: avoid requiring snappy for this file
    #[cfg(feature = "snappy")]
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
        // Helper metadata maps
        let meta_nested_struct: HashMap<String, String> = [
            ("avro.name", "nested_Struct"),
            ("avro.namespace", "topLevelRecord"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let meta_c: HashMap<String, String> = [
            ("avro.name", "c"),
            ("avro.namespace", "topLevelRecord.nested_Struct"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let meta_d_item_struct: HashMap<String, String> = [
            ("avro.name", "D"),
            ("avro.namespace", "topLevelRecord.nested_Struct.c"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let meta_g_value: HashMap<String, String> = [
            ("avro.name", "G"),
            ("avro.namespace", "topLevelRecord.nested_Struct"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let meta_h: HashMap<String, String> = [
            ("avro.name", "h"),
            ("avro.namespace", "topLevelRecord.nested_Struct.G"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        // Types used multiple times below
        let ef_struct_field = Arc::new(
            Field::new(
                "item",
                DataType::Struct(
                    vec![
                        Field::new("e", DataType::Int32, true),
                        Field::new("f", DataType::Utf8, true),
                    ]
                    .into(),
                ),
                true,
            )
            .with_metadata(meta_d_item_struct.clone()),
        );
        let d_inner_list_field = Arc::new(Field::new(
            "item",
            DataType::List(ef_struct_field.clone()),
            true,
        ));
        let d_field = Field::new("D", DataType::List(d_inner_list_field.clone()), true);
        // G.value.h.i : List<Float64>
        let i_list_field = Arc::new(Field::new("item", DataType::Float64, true));
        let i_field = Field::new("i", DataType::List(i_list_field.clone()), true);
        // G.value.h : Struct<{ i: List<Float64> }> with metadata (h)
        let h_field = Field::new("h", DataType::Struct(vec![i_field.clone()].into()), true)
            .with_metadata(meta_h.clone());
        // G.value : Struct<{ h: ... }> with metadata (G)
        let g_value_struct_field = Field::new(
            "value",
            DataType::Struct(vec![h_field.clone()].into()),
            true,
        )
        .with_metadata(meta_g_value.clone());
        // entries struct for Map G
        let entries_struct_field = Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("key", DataType::Utf8, false),
                    g_value_struct_field.clone(),
                ]
                .into(),
            ),
            false,
        );
        // Top-level nested_Struct fields (include metadata on "c")
        let a_field = Arc::new(Field::new("a", DataType::Int32, true));
        let b_field = Arc::new(Field::new(
            "B",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        ));
        let c_field = Arc::new(
            Field::new("c", DataType::Struct(vec![d_field.clone()].into()), true)
                .with_metadata(meta_c.clone()),
        );
        let g_field = Arc::new(Field::new(
            "G",
            DataType::Map(Arc::new(entries_struct_field.clone()), false),
            true,
        ));
        // Now create builders that match these exact field types (so nested types carry metadata)
        let mut nested_sb = StructBuilder::new(
            vec![
                a_field.clone(),
                b_field.clone(),
                c_field.clone(),
                g_field.clone(),
            ],
            vec![
                Box::new(Int32Builder::new()),
                Box::new(ListBuilder::new(Int32Builder::new())),
                {
                    // builder for "c" with correctly typed "D" including metadata on inner list item
                    Box::new(StructBuilder::new(
                        vec![Arc::new(d_field.clone())],
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
                            // Inner list that holds Struct<e,f> with Avro named-type metadata ("D")
                            let list_of_ef = ListBuilder::new(ef_struct_builder)
                                .with_field(ef_struct_field.clone());
                            // Outer list for "D"
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
                    let h_struct_builder = StructBuilder::new(
                        vec![Arc::new(Field::new(
                            "i",
                            DataType::List(i_list_field.clone()),
                            true,
                        ))],
                        vec![Box::new(i_list_builder)],
                    );
                    let g_value_builder = StructBuilder::new(
                        vec![Arc::new(
                            Field::new("h", DataType::Struct(vec![i_field.clone()].into()), true)
                                .with_metadata(meta_h.clone()),
                        )],
                        vec![Box::new(h_struct_builder)],
                    );
                    // Use with_values_field to attach metadata to "value" field in the map's entries
                    let map_builder = MapBuilder::new(
                        Some(map_field_names),
                        StringBuilder::new(),
                        g_value_builder,
                    )
                    .with_values_field(Arc::new(
                        Field::new(
                            "value",
                            DataType::Struct(vec![h_field.clone()].into()),
                            true,
                        )
                        .with_metadata(meta_g_value.clone()),
                    ));

                    Box::new(map_builder)
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
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            Field::new("ID", id.data_type().clone(), true),
            Field::new("Int_Array", int_array.data_type().clone(), true),
            Field::new("int_array_array", int_array_array.data_type().clone(), true),
            Field::new("Int_Map", int_map.data_type().clone(), true),
            Field::new("int_map_array", int_map_array_.data_type().clone(), true),
            Field::new("nested_Struct", nested_struct.data_type().clone(), true)
                .with_metadata(meta_nested_struct.clone()),
        ]));
        let expected = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(id) as Arc<dyn Array>,
                Arc::new(int_array),
                Arc::new(int_array_array),
                Arc::new(int_map),
                Arc::new(int_map_array_),
                Arc::new(nested_struct),
            ],
        )
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
    // TODO: avoid requiring snappy for this file
    #[cfg(feature = "snappy")]
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

    #[test]
    fn comprehensive_e2e_test() {
        let path = "test/data/comprehensive_e2e.avro";
        let batch = read_file(path, 1024, false);
        let schema = batch.schema();

        #[inline]
        fn tid_by_name(fields: &UnionFields, want: &str) -> i8 {
            for (tid, f) in fields.iter() {
                if f.name() == want {
                    return tid;
                }
            }
            panic!("union child '{want}' not found");
        }

        #[inline]
        fn tid_by_dt(fields: &UnionFields, pred: impl Fn(&DataType) -> bool) -> i8 {
            for (tid, f) in fields.iter() {
                if pred(f.data_type()) {
                    return tid;
                }
            }
            panic!("no union child matches predicate");
        }

        fn mk_dense_union(
            fields: &UnionFields,
            type_ids: Vec<i8>,
            offsets: Vec<i32>,
            provide: impl Fn(&Field) -> Option<ArrayRef>,
        ) -> ArrayRef {
            fn empty_child_for(dt: &DataType) -> Arc<dyn Array> {
                match dt {
                    DataType::Null => Arc::new(NullArray::new(0)),
                    DataType::Boolean => Arc::new(BooleanArray::from(Vec::<bool>::new())),
                    DataType::Int32 => Arc::new(Int32Array::from(Vec::<i32>::new())),
                    DataType::Int64 => Arc::new(Int64Array::from(Vec::<i64>::new())),
                    DataType::Float32 => Arc::new(Float32Array::from(Vec::<f32>::new())),
                    DataType::Float64 => Arc::new(Float64Array::from(Vec::<f64>::new())),
                    DataType::Binary => Arc::new(BinaryArray::from(Vec::<&[u8]>::new())),
                    DataType::Utf8 => Arc::new(StringArray::from(Vec::<&str>::new())),
                    DataType::Date32 => Arc::new(Date32Array::from(Vec::<i32>::new())),
                    DataType::Time32(arrow_schema::TimeUnit::Millisecond) => {
                        Arc::new(Time32MillisecondArray::from(Vec::<i32>::new()))
                    }
                    DataType::Time64(arrow_schema::TimeUnit::Microsecond) => {
                        Arc::new(Time64MicrosecondArray::from(Vec::<i64>::new()))
                    }
                    DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, tz) => {
                        let a = TimestampMillisecondArray::from(Vec::<i64>::new());
                        Arc::new(if let Some(tz) = tz {
                            a.with_timezone(tz.clone())
                        } else {
                            a
                        })
                    }
                    DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, tz) => {
                        let a = TimestampMicrosecondArray::from(Vec::<i64>::new());
                        Arc::new(if let Some(tz) = tz {
                            a.with_timezone(tz.clone())
                        } else {
                            a
                        })
                    }
                    DataType::Interval(IntervalUnit::MonthDayNano) => Arc::new(
                        IntervalMonthDayNanoArray::from(Vec::<IntervalMonthDayNano>::new()),
                    ),
                    DataType::FixedSizeBinary(sz) => Arc::new(
                        FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                            std::iter::empty::<Option<Vec<u8>>>(),
                            *sz,
                        )
                        .unwrap(),
                    ),
                    DataType::Dictionary(_, _) => {
                        let keys = Int32Array::from(Vec::<i32>::new());
                        let values = Arc::new(StringArray::from(Vec::<&str>::new()));
                        Arc::new(DictionaryArray::<Int32Type>::try_new(keys, values).unwrap())
                    }
                    DataType::Struct(fields) => {
                        let children: Vec<ArrayRef> = fields
                            .iter()
                            .map(|f| empty_child_for(f.data_type()) as ArrayRef)
                            .collect();
                        Arc::new(StructArray::new(fields.clone(), children, None))
                    }
                    DataType::List(field) => {
                        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0]));
                        Arc::new(
                            ListArray::try_new(
                                field.clone(),
                                offsets,
                                empty_child_for(field.data_type()),
                                None,
                            )
                            .unwrap(),
                        )
                    }
                    DataType::Map(entry_field, is_sorted) => {
                        let (key_field, val_field) = match entry_field.data_type() {
                            DataType::Struct(fs) => (fs[0].clone(), fs[1].clone()),
                            other => panic!("unexpected map entries type: {other:?}"),
                        };
                        let keys = StringArray::from(Vec::<&str>::new());
                        let vals: ArrayRef = match val_field.data_type() {
                            DataType::Null => Arc::new(NullArray::new(0)) as ArrayRef,
                            DataType::Boolean => {
                                Arc::new(BooleanArray::from(Vec::<bool>::new())) as ArrayRef
                            }
                            DataType::Int32 => {
                                Arc::new(Int32Array::from(Vec::<i32>::new())) as ArrayRef
                            }
                            DataType::Int64 => {
                                Arc::new(Int64Array::from(Vec::<i64>::new())) as ArrayRef
                            }
                            DataType::Float32 => {
                                Arc::new(Float32Array::from(Vec::<f32>::new())) as ArrayRef
                            }
                            DataType::Float64 => {
                                Arc::new(Float64Array::from(Vec::<f64>::new())) as ArrayRef
                            }
                            DataType::Utf8 => {
                                Arc::new(StringArray::from(Vec::<&str>::new())) as ArrayRef
                            }
                            DataType::Binary => {
                                Arc::new(BinaryArray::from(Vec::<&[u8]>::new())) as ArrayRef
                            }
                            DataType::Union(uf, _) => {
                                let children: Vec<ArrayRef> = uf
                                    .iter()
                                    .map(|(_, f)| empty_child_for(f.data_type()))
                                    .collect();
                                Arc::new(
                                    UnionArray::try_new(
                                        uf.clone(),
                                        ScalarBuffer::<i8>::from(Vec::<i8>::new()),
                                        Some(ScalarBuffer::<i32>::from(Vec::<i32>::new())),
                                        children,
                                    )
                                    .unwrap(),
                                ) as ArrayRef
                            }
                            other => panic!("unsupported map value type: {other:?}"),
                        };
                        let entries = StructArray::new(
                            Fields::from(vec![
                                key_field.as_ref().clone(),
                                val_field.as_ref().clone(),
                            ]),
                            vec![Arc::new(keys) as ArrayRef, vals],
                            None,
                        );
                        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0]));
                        Arc::new(MapArray::new(
                            entry_field.clone(),
                            offsets,
                            entries,
                            None,
                            *is_sorted,
                        ))
                    }
                    other => panic!("empty_child_for: unhandled type {other:?}"),
                }
            }
            let children: Vec<ArrayRef> = fields
                .iter()
                .map(|(_, f)| provide(f).unwrap_or_else(|| empty_child_for(f.data_type())))
                .collect();
            Arc::new(
                UnionArray::try_new(
                    fields.clone(),
                    ScalarBuffer::<i8>::from(type_ids),
                    Some(ScalarBuffer::<i32>::from(offsets)),
                    children,
                )
                .unwrap(),
            ) as ArrayRef
        }

        #[inline]
        fn uuid16_from_str(s: &str) -> [u8; 16] {
            let mut out = [0u8; 16];
            let mut idx = 0usize;
            let mut hi: Option<u8> = None;
            for ch in s.chars() {
                if ch == '-' {
                    continue;
                }
                let v = ch.to_digit(16).expect("invalid hex digit in UUID") as u8;
                if let Some(h) = hi {
                    out[idx] = (h << 4) | v;
                    idx += 1;
                    hi = None;
                } else {
                    hi = Some(v);
                }
            }
            assert_eq!(idx, 16, "UUID must decode to 16 bytes");
            out
        }
        let date_a: i32 = 19_000; // 2022-01-08
        let time_ms_a: i32 = 12 * 3_600_000 + 34 * 60_000 + 56_000 + 789;
        let time_us_eod: i64 = 86_400_000_000 - 1;
        let ts_ms_2024_01_01: i64 = 1_704_067_200_000; // 2024-01-01T00:00:00Z
        let ts_us_2024_01_01: i64 = ts_ms_2024_01_01 * 1_000;
        let dur_small = IntervalMonthDayNanoType::make_value(1, 2, 3_000_000_000);
        let dur_zero = IntervalMonthDayNanoType::make_value(0, 0, 0);
        let dur_large =
            IntervalMonthDayNanoType::make_value(12, 31, ((86_400_000 - 1) as i64) * 1_000_000);
        let dur_2years = IntervalMonthDayNanoType::make_value(24, 0, 0);
        let uuid1 = uuid16_from_str("fe7bc30b-4ce8-4c5e-b67c-2234a2d38e66");
        let uuid2 = uuid16_from_str("0826cc06-d2e3-4599-b4ad-af5fa6905cdb");

        #[inline]
        fn push_like(
            reader_schema: &arrow_schema::Schema,
            name: &str,
            arr: ArrayRef,
            fields: &mut Vec<FieldRef>,
            cols: &mut Vec<ArrayRef>,
        ) {
            let src = reader_schema
                .field_with_name(name)
                .unwrap_or_else(|_| panic!("source schema missing field '{name}'"));
            let mut f = Field::new(name, arr.data_type().clone(), src.is_nullable());
            let md = src.metadata();
            if !md.is_empty() {
                f = f.with_metadata(md.clone());
            }
            fields.push(Arc::new(f));
            cols.push(arr);
        }

        let mut fields: Vec<FieldRef> = Vec::new();
        let mut columns: Vec<ArrayRef> = Vec::new();
        push_like(
            schema.as_ref(),
            "id",
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])) as ArrayRef,
            &mut fields,
            &mut columns,
        );
        push_like(
            schema.as_ref(),
            "flag",
            Arc::new(BooleanArray::from(vec![true, false, true, false])) as ArrayRef,
            &mut fields,
            &mut columns,
        );
        push_like(
            schema.as_ref(),
            "ratio_f32",
            Arc::new(Float32Array::from(vec![1.25f32, -0.0, 3.5, 9.75])) as ArrayRef,
            &mut fields,
            &mut columns,
        );
        push_like(
            schema.as_ref(),
            "ratio_f64",
            Arc::new(Float64Array::from(vec![2.5f64, -1.0, 7.0, -2.25])) as ArrayRef,
            &mut fields,
            &mut columns,
        );
        push_like(
            schema.as_ref(),
            "count_i32",
            Arc::new(Int32Array::from(vec![7, -1, 0, 123])) as ArrayRef,
            &mut fields,
            &mut columns,
        );
        push_like(
            schema.as_ref(),
            "count_i64",
            Arc::new(Int64Array::from(vec![
                7_000_000_000i64,
                -2,
                0,
                -9_876_543_210i64,
            ])) as ArrayRef,
            &mut fields,
            &mut columns,
        );
        push_like(
            schema.as_ref(),
            "opt_i32_nullfirst",
            Arc::new(Int32Array::from(vec![None, Some(42), None, Some(0)])) as ArrayRef,
            &mut fields,
            &mut columns,
        );
        push_like(
            schema.as_ref(),
            "opt_str_nullsecond",
            Arc::new(StringArray::from(vec![
                Some("alpha"),
                None,
                Some("s3"),
                Some(""),
            ])) as ArrayRef,
            &mut fields,
            &mut columns,
        );
        {
            let uf = match schema
                .field_with_name("tri_union_prim")
                .unwrap()
                .data_type()
            {
                DataType::Union(f, UnionMode::Dense) => f.clone(),
                other => panic!("tri_union_prim should be dense union, got {other:?}"),
            };
            let tid_i = tid_by_name(&uf, "int");
            let tid_s = tid_by_name(&uf, "string");
            let tid_b = tid_by_name(&uf, "boolean");
            let tids = vec![tid_i, tid_s, tid_b, tid_s];
            let offs = vec![0, 0, 0, 1];
            let arr = mk_dense_union(&uf, tids, offs, |f| match f.data_type() {
                DataType::Int32 => Some(Arc::new(Int32Array::from(vec![0])) as ArrayRef),
                DataType::Utf8 => Some(Arc::new(StringArray::from(vec!["hi", ""])) as ArrayRef),
                DataType::Boolean => Some(Arc::new(BooleanArray::from(vec![true])) as ArrayRef),
                _ => None,
            });
            push_like(
                schema.as_ref(),
                "tri_union_prim",
                arr,
                &mut fields,
                &mut columns,
            );
        }

        push_like(
            schema.as_ref(),
            "str_utf8",
            Arc::new(StringArray::from(vec!["hello", "", "world", "✓ unicode"])) as ArrayRef,
            &mut fields,
            &mut columns,
        );
        push_like(
            schema.as_ref(),
            "raw_bytes",
            Arc::new(BinaryArray::from(vec![
                b"\x00\x01".as_ref(),
                b"".as_ref(),
                b"\xFF\x00".as_ref(),
                b"\x10\x20\x30\x40".as_ref(),
            ])) as ArrayRef,
            &mut fields,
            &mut columns,
        );
        {
            let it = [
                Some(*b"0123456789ABCDEF"),
                Some([0u8; 16]),
                Some(*b"ABCDEFGHIJKLMNOP"),
                Some([0xAA; 16]),
            ]
            .into_iter();
            let arr =
                Arc::new(FixedSizeBinaryArray::try_from_sparse_iter_with_size(it, 16).unwrap())
                    as ArrayRef;
            push_like(
                schema.as_ref(),
                "fx16_plain",
                arr,
                &mut fields,
                &mut columns,
            );
        }
        {
            #[cfg(feature = "small_decimals")]
            let dec10_2 = Arc::new(
                Decimal64Array::from_iter_values([123456i64, -1, 0, 9_999_999_999i64])
                    .with_precision_and_scale(10, 2)
                    .unwrap(),
            ) as ArrayRef;
            #[cfg(not(feature = "small_decimals"))]
            let dec10_2 = Arc::new(
                Decimal128Array::from_iter_values([123456i128, -1, 0, 9_999_999_999i128])
                    .with_precision_and_scale(10, 2)
                    .unwrap(),
            ) as ArrayRef;
            push_like(
                schema.as_ref(),
                "dec_bytes_s10_2",
                dec10_2,
                &mut fields,
                &mut columns,
            );
        }
        {
            #[cfg(feature = "small_decimals")]
            let dec20_4 = Arc::new(
                Decimal128Array::from_iter_values([1_234_567_891_234i128, -420_000i128, 0, -1i128])
                    .with_precision_and_scale(20, 4)
                    .unwrap(),
            ) as ArrayRef;
            #[cfg(not(feature = "small_decimals"))]
            let dec20_4 = Arc::new(
                Decimal128Array::from_iter_values([1_234_567_891_234i128, -420_000i128, 0, -1i128])
                    .with_precision_and_scale(20, 4)
                    .unwrap(),
            ) as ArrayRef;
            push_like(
                schema.as_ref(),
                "dec_fix_s20_4",
                dec20_4,
                &mut fields,
                &mut columns,
            );
        }
        {
            let it = [Some(uuid1), Some(uuid2), Some(uuid1), Some(uuid2)].into_iter();
            let arr =
                Arc::new(FixedSizeBinaryArray::try_from_sparse_iter_with_size(it, 16).unwrap())
                    as ArrayRef;
            push_like(schema.as_ref(), "uuid_str", arr, &mut fields, &mut columns);
        }
        push_like(
            schema.as_ref(),
            "d_date",
            Arc::new(Date32Array::from(vec![date_a, 0, 1, 365])) as ArrayRef,
            &mut fields,
            &mut columns,
        );
        push_like(
            schema.as_ref(),
            "t_millis",
            Arc::new(Time32MillisecondArray::from(vec![
                time_ms_a,
                0,
                1,
                86_400_000 - 1,
            ])) as ArrayRef,
            &mut fields,
            &mut columns,
        );
        push_like(
            schema.as_ref(),
            "t_micros",
            Arc::new(Time64MicrosecondArray::from(vec![
                time_us_eod,
                0,
                1,
                1_000_000,
            ])) as ArrayRef,
            &mut fields,
            &mut columns,
        );
        {
            let a = TimestampMillisecondArray::from(vec![
                ts_ms_2024_01_01,
                -1,
                ts_ms_2024_01_01 + 123,
                0,
            ])
            .with_timezone("+00:00");
            push_like(
                schema.as_ref(),
                "ts_millis_utc",
                Arc::new(a) as ArrayRef,
                &mut fields,
                &mut columns,
            );
        }
        {
            let a = TimestampMicrosecondArray::from(vec![
                ts_us_2024_01_01,
                1,
                ts_us_2024_01_01 + 456,
                0,
            ])
            .with_timezone("+00:00");
            push_like(
                schema.as_ref(),
                "ts_micros_utc",
                Arc::new(a) as ArrayRef,
                &mut fields,
                &mut columns,
            );
        }
        push_like(
            schema.as_ref(),
            "ts_millis_local",
            Arc::new(TimestampMillisecondArray::from(vec![
                ts_ms_2024_01_01 + 86_400_000,
                0,
                ts_ms_2024_01_01 + 789,
                123_456_789,
            ])) as ArrayRef,
            &mut fields,
            &mut columns,
        );
        push_like(
            schema.as_ref(),
            "ts_micros_local",
            Arc::new(TimestampMicrosecondArray::from(vec![
                ts_us_2024_01_01 + 123_456,
                0,
                ts_us_2024_01_01 + 101_112,
                987_654_321,
            ])) as ArrayRef,
            &mut fields,
            &mut columns,
        );
        {
            let v = vec![dur_small, dur_zero, dur_large, dur_2years];
            push_like(
                schema.as_ref(),
                "interval_mdn",
                Arc::new(IntervalMonthDayNanoArray::from(v)) as ArrayRef,
                &mut fields,
                &mut columns,
            );
        }
        {
            let keys = Int32Array::from(vec![1, 2, 3, 0]); // NEW, PROCESSING, DONE, UNKNOWN
            let values = Arc::new(StringArray::from(vec![
                "UNKNOWN",
                "NEW",
                "PROCESSING",
                "DONE",
            ])) as ArrayRef;
            let dict = DictionaryArray::<Int32Type>::try_new(keys, values).unwrap();
            push_like(
                schema.as_ref(),
                "status",
                Arc::new(dict) as ArrayRef,
                &mut fields,
                &mut columns,
            );
        }
        {
            let list_field = match schema.field_with_name("arr_union").unwrap().data_type() {
                DataType::List(f) => f.clone(),
                other => panic!("arr_union should be List, got {other:?}"),
            };
            let uf = match list_field.data_type() {
                DataType::Union(f, UnionMode::Dense) => f.clone(),
                other => panic!("arr_union item should be union, got {other:?}"),
            };
            let tid_l = tid_by_name(&uf, "long");
            let tid_s = tid_by_name(&uf, "string");
            let tid_n = tid_by_name(&uf, "null");
            let type_ids = vec![
                tid_l, tid_s, tid_n, tid_l, tid_n, tid_s, tid_l, tid_l, tid_s, tid_n, tid_l,
            ];
            let offsets = vec![0, 0, 0, 1, 1, 1, 2, 3, 2, 2, 4];
            let values = mk_dense_union(&uf, type_ids, offsets, |f| match f.data_type() {
                DataType::Int64 => {
                    Some(Arc::new(Int64Array::from(vec![1i64, -3, 0, -1, 0])) as ArrayRef)
                }
                DataType::Utf8 => {
                    Some(Arc::new(StringArray::from(vec!["x", "z", "end"])) as ArrayRef)
                }
                DataType::Null => Some(Arc::new(NullArray::new(3)) as ArrayRef),
                _ => None,
            });
            let list_offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 4, 7, 8, 11]));
            let arr = Arc::new(ListArray::try_new(list_field, list_offsets, values, None).unwrap())
                as ArrayRef;
            push_like(schema.as_ref(), "arr_union", arr, &mut fields, &mut columns);
        }
        {
            let (entry_field, entries_fields, uf, is_sorted) =
                match schema.field_with_name("map_union").unwrap().data_type() {
                    DataType::Map(entry_field, is_sorted) => {
                        let fs = match entry_field.data_type() {
                            DataType::Struct(fs) => fs.clone(),
                            other => panic!("map entries must be struct, got {other:?}"),
                        };
                        let val_f = fs[1].clone();
                        let uf = match val_f.data_type() {
                            DataType::Union(f, UnionMode::Dense) => f.clone(),
                            other => panic!("map value must be union, got {other:?}"),
                        };
                        (entry_field.clone(), fs, uf, *is_sorted)
                    }
                    other => panic!("map_union should be Map, got {other:?}"),
                };
            let keys = StringArray::from(vec!["a", "b", "c", "neg", "pi", "ok"]);
            let moff = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 3, 4, 4, 6]));
            let tid_null = tid_by_name(&uf, "null");
            let tid_d = tid_by_name(&uf, "double");
            let tid_s = tid_by_name(&uf, "string");
            let type_ids = vec![tid_d, tid_null, tid_s, tid_d, tid_d, tid_s];
            let offsets = vec![0, 0, 0, 1, 2, 1];
            let pi_5dp = (std::f64::consts::PI * 100_000.0).trunc() / 100_000.0;
            let vals = mk_dense_union(&uf, type_ids, offsets, |f| match f.data_type() {
                DataType::Float64 => {
                    Some(Arc::new(Float64Array::from(vec![1.5f64, -0.5, pi_5dp])) as ArrayRef)
                }
                DataType::Utf8 => {
                    Some(Arc::new(StringArray::from(vec!["yes", "true"])) as ArrayRef)
                }
                DataType::Null => Some(Arc::new(NullArray::new(2)) as ArrayRef),
                _ => None,
            });
            let entries = StructArray::new(
                entries_fields.clone(),
                vec![Arc::new(keys) as ArrayRef, vals],
                None,
            );
            let map =
                Arc::new(MapArray::new(entry_field, moff, entries, None, is_sorted)) as ArrayRef;
            push_like(schema.as_ref(), "map_union", map, &mut fields, &mut columns);
        }
        {
            let fs = match schema.field_with_name("address").unwrap().data_type() {
                DataType::Struct(fs) => fs.clone(),
                other => panic!("address should be Struct, got {other:?}"),
            };
            let street = Arc::new(StringArray::from(vec![
                "100 Main",
                "",
                "42 Galaxy Way",
                "End Ave",
            ])) as ArrayRef;
            let zip = Arc::new(Int32Array::from(vec![12345, 0, 42424, 1])) as ArrayRef;
            let country = Arc::new(StringArray::from(vec!["US", "CA", "US", "GB"])) as ArrayRef;
            let arr = Arc::new(StructArray::new(fs, vec![street, zip, country], None)) as ArrayRef;
            push_like(schema.as_ref(), "address", arr, &mut fields, &mut columns);
        }
        {
            let fs = match schema.field_with_name("maybe_auth").unwrap().data_type() {
                DataType::Struct(fs) => fs.clone(),
                other => panic!("maybe_auth should be Struct, got {other:?}"),
            };
            let user =
                Arc::new(StringArray::from(vec!["alice", "bob", "carol", "dave"])) as ArrayRef;
            let token_values: Vec<Option<&[u8]>> = vec![
                None,                           // row 1: null
                Some(b"\x01\x02\x03".as_ref()), // row 2: bytes
                None,                           // row 3: null
                Some(b"".as_ref()),             // row 4: empty bytes
            ];
            let token = Arc::new(BinaryArray::from(token_values)) as ArrayRef;
            let arr = Arc::new(StructArray::new(fs, vec![user, token], None)) as ArrayRef;
            push_like(
                schema.as_ref(),
                "maybe_auth",
                arr,
                &mut fields,
                &mut columns,
            );
        }
        {
            let uf = match schema
                .field_with_name("union_enum_record_array_map")
                .unwrap()
                .data_type()
            {
                DataType::Union(f, UnionMode::Dense) => f.clone(),
                other => panic!("union_enum_record_array_map should be union, got {other:?}"),
            };
            let mut tid_enum: Option<i8> = None;
            let mut tid_rec_a: Option<i8> = None;
            let mut tid_array: Option<i8> = None;
            let mut tid_map: Option<i8> = None;
            let mut map_entry_field: Option<FieldRef> = None;
            let mut map_sorted: bool = false;
            for (tid, f) in uf.iter() {
                match f.data_type() {
                    DataType::Dictionary(_, _) => tid_enum = Some(tid),
                    DataType::Struct(childs)
                        if childs.len() == 2
                            && childs[0].name() == "a"
                            && childs[1].name() == "b" =>
                    {
                        tid_rec_a = Some(tid)
                    }
                    DataType::List(item) if matches!(item.data_type(), DataType::Int64) => {
                        tid_array = Some(tid)
                    }
                    DataType::Map(ef, is_sorted) => {
                        tid_map = Some(tid);
                        map_entry_field = Some(ef.clone());
                        map_sorted = *is_sorted;
                    }
                    _ => {}
                }
            }
            let (tid_enum, tid_rec_a, tid_array, tid_map) = (
                tid_enum.unwrap(),
                tid_rec_a.unwrap(),
                tid_array.unwrap(),
                tid_map.unwrap(),
            );
            let tids = vec![tid_enum, tid_rec_a, tid_array, tid_map];
            let offs = vec![0, 0, 0, 0];
            let arr = mk_dense_union(&uf, tids, offs, |f| match f.data_type() {
                DataType::Dictionary(_, _) => {
                    let keys = Int32Array::from(vec![0i32]);
                    let values =
                        Arc::new(StringArray::from(vec!["RED", "GREEN", "BLUE"])) as ArrayRef;
                    Some(
                        Arc::new(DictionaryArray::<Int32Type>::try_new(keys, values).unwrap())
                            as ArrayRef,
                    )
                }
                DataType::Struct(fs)
                    if fs.len() == 2 && fs[0].name() == "a" && fs[1].name() == "b" =>
                {
                    let a = Int32Array::from(vec![7]);
                    let b = StringArray::from(vec!["rec"]);
                    Some(Arc::new(StructArray::new(
                        fs.clone(),
                        vec![Arc::new(a), Arc::new(b)],
                        None,
                    )) as ArrayRef)
                }
                DataType::List(field) => {
                    let values = Int64Array::from(vec![1i64, 2, 3]);
                    let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 3]));
                    Some(Arc::new(
                        ListArray::try_new(field.clone(), offsets, Arc::new(values), None).unwrap(),
                    ) as ArrayRef)
                }
                DataType::Map(_, _) => {
                    let entry_field = map_entry_field.clone().unwrap();
                    let (key_field, val_field) = match entry_field.data_type() {
                        DataType::Struct(fs) => (fs[0].clone(), fs[1].clone()),
                        _ => unreachable!(),
                    };
                    let keys = StringArray::from(vec!["k"]);
                    let vals = StringArray::from(vec!["v"]);
                    let entries = StructArray::new(
                        Fields::from(vec![key_field.as_ref().clone(), val_field.as_ref().clone()]),
                        vec![Arc::new(keys) as ArrayRef, Arc::new(vals) as ArrayRef],
                        None,
                    );
                    let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 1]));
                    Some(Arc::new(MapArray::new(
                        entry_field.clone(),
                        offsets,
                        entries,
                        None,
                        map_sorted,
                    )) as ArrayRef)
                }
                _ => None,
            });
            push_like(
                schema.as_ref(),
                "union_enum_record_array_map",
                arr,
                &mut fields,
                &mut columns,
            );
        }
        {
            let uf = match schema
                .field_with_name("union_date_or_fixed4")
                .unwrap()
                .data_type()
            {
                DataType::Union(f, UnionMode::Dense) => f.clone(),
                other => panic!("union_date_or_fixed4 should be union, got {other:?}"),
            };
            let tid_date = tid_by_dt(&uf, |dt| matches!(dt, DataType::Date32));
            let tid_fx4 = tid_by_dt(&uf, |dt| matches!(dt, DataType::FixedSizeBinary(4)));
            let tids = vec![tid_date, tid_fx4, tid_date, tid_fx4];
            let offs = vec![0, 0, 1, 1];
            let arr = mk_dense_union(&uf, tids, offs, |f| match f.data_type() {
                DataType::Date32 => Some(Arc::new(Date32Array::from(vec![date_a, 0])) as ArrayRef),
                DataType::FixedSizeBinary(4) => {
                    let it = [Some(*b"\x00\x11\x22\x33"), Some(*b"ABCD")].into_iter();
                    Some(Arc::new(
                        FixedSizeBinaryArray::try_from_sparse_iter_with_size(it, 4).unwrap(),
                    ) as ArrayRef)
                }
                _ => None,
            });
            push_like(
                schema.as_ref(),
                "union_date_or_fixed4",
                arr,
                &mut fields,
                &mut columns,
            );
        }
        {
            let uf = match schema
                .field_with_name("union_interval_or_string")
                .unwrap()
                .data_type()
            {
                DataType::Union(f, UnionMode::Dense) => f.clone(),
                other => panic!("union_interval_or_string should be union, got {other:?}"),
            };
            let tid_dur = tid_by_dt(&uf, |dt| {
                matches!(dt, DataType::Interval(IntervalUnit::MonthDayNano))
            });
            let tid_str = tid_by_dt(&uf, |dt| matches!(dt, DataType::Utf8));
            let tids = vec![tid_dur, tid_str, tid_dur, tid_str];
            let offs = vec![0, 0, 1, 1];
            let arr = mk_dense_union(&uf, tids, offs, |f| match f.data_type() {
                DataType::Interval(IntervalUnit::MonthDayNano) => Some(Arc::new(
                    IntervalMonthDayNanoArray::from(vec![dur_small, dur_large]),
                )
                    as ArrayRef),
                DataType::Utf8 => Some(Arc::new(StringArray::from(vec![
                    "duration-as-text",
                    "iso-8601-period-P1Y",
                ])) as ArrayRef),
                _ => None,
            });
            push_like(
                schema.as_ref(),
                "union_interval_or_string",
                arr,
                &mut fields,
                &mut columns,
            );
        }
        {
            let uf = match schema
                .field_with_name("union_uuid_or_fixed10")
                .unwrap()
                .data_type()
            {
                DataType::Union(f, UnionMode::Dense) => f.clone(),
                other => panic!("union_uuid_or_fixed10 should be union, got {other:?}"),
            };
            let tid_uuid = tid_by_dt(&uf, |dt| matches!(dt, DataType::FixedSizeBinary(16)));
            let tid_fx10 = tid_by_dt(&uf, |dt| matches!(dt, DataType::FixedSizeBinary(10)));
            let tids = vec![tid_uuid, tid_fx10, tid_uuid, tid_fx10];
            let offs = vec![0, 0, 1, 1];
            let arr = mk_dense_union(&uf, tids, offs, |f| match f.data_type() {
                DataType::FixedSizeBinary(16) => {
                    let it = [Some(uuid1), Some(uuid2)].into_iter();
                    Some(Arc::new(
                        FixedSizeBinaryArray::try_from_sparse_iter_with_size(it, 16).unwrap(),
                    ) as ArrayRef)
                }
                DataType::FixedSizeBinary(10) => {
                    let fx10_a = [0xAAu8; 10];
                    let fx10_b = [0x00u8, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99];
                    let it = [Some(fx10_a), Some(fx10_b)].into_iter();
                    Some(Arc::new(
                        FixedSizeBinaryArray::try_from_sparse_iter_with_size(it, 10).unwrap(),
                    ) as ArrayRef)
                }
                _ => None,
            });
            push_like(
                schema.as_ref(),
                "union_uuid_or_fixed10",
                arr,
                &mut fields,
                &mut columns,
            );
        }
        {
            let list_field = match schema
                .field_with_name("array_records_with_union")
                .unwrap()
                .data_type()
            {
                DataType::List(f) => f.clone(),
                other => panic!("array_records_with_union should be List, got {other:?}"),
            };
            let kv_fields = match list_field.data_type() {
                DataType::Struct(fs) => fs.clone(),
                other => panic!("array_records_with_union items must be Struct, got {other:?}"),
            };
            let val_field = kv_fields
                .iter()
                .find(|f| f.name() == "val")
                .unwrap()
                .clone();
            let uf = match val_field.data_type() {
                DataType::Union(f, UnionMode::Dense) => f.clone(),
                other => panic!("KV.val should be union, got {other:?}"),
            };
            let keys = Arc::new(StringArray::from(vec!["k1", "k2", "k", "k3", "x"])) as ArrayRef;
            let tid_null = tid_by_name(&uf, "null");
            let tid_i = tid_by_name(&uf, "int");
            let tid_l = tid_by_name(&uf, "long");
            let type_ids = vec![tid_i, tid_null, tid_l, tid_null, tid_i];
            let offsets = vec![0, 0, 0, 1, 1];
            let vals = mk_dense_union(&uf, type_ids, offsets, |f| match f.data_type() {
                DataType::Int32 => Some(Arc::new(Int32Array::from(vec![5, -5])) as ArrayRef),
                DataType::Int64 => Some(Arc::new(Int64Array::from(vec![99i64])) as ArrayRef),
                DataType::Null => Some(Arc::new(NullArray::new(2)) as ArrayRef),
                _ => None,
            });
            let values_struct =
                Arc::new(StructArray::new(kv_fields.clone(), vec![keys, vals], None)) as ArrayRef;
            let list_offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 2, 3, 4, 5]));
            let arr = Arc::new(
                ListArray::try_new(list_field, list_offsets, values_struct, None).unwrap(),
            ) as ArrayRef;
            push_like(
                schema.as_ref(),
                "array_records_with_union",
                arr,
                &mut fields,
                &mut columns,
            );
        }
        {
            let uf = match schema
                .field_with_name("union_map_or_array_int")
                .unwrap()
                .data_type()
            {
                DataType::Union(f, UnionMode::Dense) => f.clone(),
                other => panic!("union_map_or_array_int should be union, got {other:?}"),
            };
            let tid_map = tid_by_dt(&uf, |dt| matches!(dt, DataType::Map(_, _)));
            let tid_list = tid_by_dt(&uf, |dt| matches!(dt, DataType::List(_)));
            let map_child: ArrayRef = {
                let (entry_field, is_sorted) = match uf
                    .iter()
                    .find(|(tid, _)| *tid == tid_map)
                    .unwrap()
                    .1
                    .data_type()
                {
                    DataType::Map(ef, is_sorted) => (ef.clone(), *is_sorted),
                    _ => unreachable!(),
                };
                let (key_field, val_field) = match entry_field.data_type() {
                    DataType::Struct(fs) => (fs[0].clone(), fs[1].clone()),
                    _ => unreachable!(),
                };
                let keys = StringArray::from(vec!["x", "y", "only"]);
                let vals = Int32Array::from(vec![1, 2, 10]);
                let entries = StructArray::new(
                    Fields::from(vec![key_field.as_ref().clone(), val_field.as_ref().clone()]),
                    vec![Arc::new(keys) as ArrayRef, Arc::new(vals) as ArrayRef],
                    None,
                );
                let moff = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 2, 3]));
                Arc::new(MapArray::new(entry_field, moff, entries, None, is_sorted)) as ArrayRef
            };
            let list_child: ArrayRef = {
                let list_field = match uf
                    .iter()
                    .find(|(tid, _)| *tid == tid_list)
                    .unwrap()
                    .1
                    .data_type()
                {
                    DataType::List(f) => f.clone(),
                    _ => unreachable!(),
                };
                let values = Int32Array::from(vec![1, 2, 3, 0]);
                let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 3, 4]));
                Arc::new(ListArray::try_new(list_field, offsets, Arc::new(values), None).unwrap())
                    as ArrayRef
            };
            let tids = vec![tid_map, tid_list, tid_map, tid_list];
            let offs = vec![0, 0, 1, 1];
            let arr = mk_dense_union(&uf, tids, offs, |f| match f.data_type() {
                DataType::Map(_, _) => Some(map_child.clone()),
                DataType::List(_) => Some(list_child.clone()),
                _ => None,
            });
            push_like(
                schema.as_ref(),
                "union_map_or_array_int",
                arr,
                &mut fields,
                &mut columns,
            );
        }
        push_like(
            schema.as_ref(),
            "renamed_with_default",
            Arc::new(Int32Array::from(vec![100, 42, 7, 42])) as ArrayRef,
            &mut fields,
            &mut columns,
        );
        {
            let fs = match schema.field_with_name("person").unwrap().data_type() {
                DataType::Struct(fs) => fs.clone(),
                other => panic!("person should be Struct, got {other:?}"),
            };
            let name =
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Carol", "Dave"])) as ArrayRef;
            let age = Arc::new(Int32Array::from(vec![30, 0, 25, 41])) as ArrayRef;
            let arr = Arc::new(StructArray::new(fs, vec![name, age], None)) as ArrayRef;
            push_like(schema.as_ref(), "person", arr, &mut fields, &mut columns);
        }
        let expected =
            RecordBatch::try_new(Arc::new(Schema::new(Fields::from(fields))), columns).unwrap();
        assert_eq!(
            expected, batch,
            "entire RecordBatch mismatch (schema, all columns, all rows)"
        );
    }
    #[test]
    fn comprehensive_e2e_resolution_test() {
        use serde_json::Value;
        use std::collections::HashMap;

        // Build a reader schema that stresses Avro schema‑resolution
        //
        // Changes relative to writer schema:
        // * Rename fields using writer aliases:    id -> identifier, renamed_with_default -> old_count
        // * Promote numeric types:                 count_i32 (int) -> long, ratio_f32 (float) -> double
        // * Reorder many union branches (reverse), incl. nested unions
        // * Reorder array/map union item/value branches
        // * Rename nested Address field:           street -> street_name (uses alias in writer)
        // * Change Person type name/namespace:     com.example.Person (matches writer alias)
        // * Reverse top‑level field order
        //
        // Reader‑side aliases are added wherever names change (per Avro spec).
        fn make_comprehensive_reader_schema(path: &str) -> AvroSchema {
            fn set_type_string(f: &mut Value, new_ty: &str) {
                if let Some(ty) = f.get_mut("type") {
                    match ty {
                        Value::String(_) | Value::Object(_) => {
                            *ty = Value::String(new_ty.to_string());
                        }
                        Value::Array(arr) => {
                            for b in arr.iter_mut() {
                                match b {
                                    Value::String(s) if s != "null" => {
                                        *b = Value::String(new_ty.to_string());
                                        break;
                                    }
                                    Value::Object(_) => {
                                        *b = Value::String(new_ty.to_string());
                                        break;
                                    }
                                    _ => {}
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
            fn reverse_union_array(f: &mut Value) {
                if let Some(arr) = f.get_mut("type").and_then(|t| t.as_array_mut()) {
                    arr.reverse();
                }
            }
            fn reverse_items_union(f: &mut Value) {
                if let Some(obj) = f.get_mut("type").and_then(|t| t.as_object_mut()) {
                    if let Some(items) = obj.get_mut("items").and_then(|v| v.as_array_mut()) {
                        items.reverse();
                    }
                }
            }
            fn reverse_map_values_union(f: &mut Value) {
                if let Some(obj) = f.get_mut("type").and_then(|t| t.as_object_mut()) {
                    if let Some(values) = obj.get_mut("values").and_then(|v| v.as_array_mut()) {
                        values.reverse();
                    }
                }
            }
            fn reverse_nested_union_in_record(f: &mut Value, field_name: &str) {
                if let Some(obj) = f.get_mut("type").and_then(|t| t.as_object_mut()) {
                    if let Some(fields) = obj.get_mut("fields").and_then(|v| v.as_array_mut()) {
                        for ff in fields.iter_mut() {
                            if ff.get("name").and_then(|n| n.as_str()) == Some(field_name) {
                                if let Some(ty) = ff.get_mut("type") {
                                    if let Some(arr) = ty.as_array_mut() {
                                        arr.reverse();
                                    }
                                }
                            }
                        }
                    }
                }
            }
            fn rename_nested_field_with_alias(f: &mut Value, old: &str, new: &str) {
                if let Some(obj) = f.get_mut("type").and_then(|t| t.as_object_mut()) {
                    if let Some(fields) = obj.get_mut("fields").and_then(|v| v.as_array_mut()) {
                        for ff in fields.iter_mut() {
                            if ff.get("name").and_then(|n| n.as_str()) == Some(old) {
                                ff["name"] = Value::String(new.to_string());
                                ff["aliases"] = Value::Array(vec![Value::String(old.to_string())]);
                            }
                        }
                    }
                }
            }
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
                match name {
                    // Field aliasing (reader‑side aliases added)
                    "id" => {
                        f["name"] = Value::String("identifier".into());
                        f["aliases"] = Value::Array(vec![Value::String("id".into())]);
                    }
                    "renamed_with_default" => {
                        f["name"] = Value::String("old_count".into());
                        f["aliases"] =
                            Value::Array(vec![Value::String("renamed_with_default".into())]);
                    }
                    // Promotions
                    "count_i32" => set_type_string(f, "long"),
                    "ratio_f32" => set_type_string(f, "double"),
                    // Union reorder (exercise resolution)
                    "opt_str_nullsecond" => reverse_union_array(f),
                    "union_enum_record_array_map" => reverse_union_array(f),
                    "union_date_or_fixed4" => reverse_union_array(f),
                    "union_interval_or_string" => reverse_union_array(f),
                    "union_uuid_or_fixed10" => reverse_union_array(f),
                    "union_map_or_array_int" => reverse_union_array(f),
                    "maybe_auth" => reverse_nested_union_in_record(f, "token"),
                    // Array/Map unions
                    "arr_union" => reverse_items_union(f),
                    "map_union" => reverse_map_values_union(f),
                    // Nested rename using reader‑side alias
                    "address" => rename_nested_field_with_alias(f, "street", "street_name"),
                    // Type‑name alias for nested record
                    "person" => {
                        if let Some(tobj) = f.get_mut("type").and_then(|t| t.as_object_mut()) {
                            tobj.insert("name".to_string(), Value::String("Person".into()));
                            tobj.insert(
                                "namespace".to_string(),
                                Value::String("com.example".into()),
                            );
                            tobj.insert(
                                "aliases".into(),
                                Value::Array(vec![
                                    Value::String("PersonV2".into()),
                                    Value::String("com.example.v2.PersonV2".into()),
                                ]),
                            );
                        }
                    }
                    _ => {}
                }
            }
            fields.reverse();
            AvroSchema::new(root.to_string())
        }

        let path = "test/data/comprehensive_e2e.avro";
        let reader_schema = make_comprehensive_reader_schema(path);
        let batch = read_alltypes_with_reader_schema(path, reader_schema.clone());

        const UUID_EXT_KEY: &str = "ARROW:extension:name";
        const UUID_LOGICAL_KEY: &str = "logicalType";

        let uuid_md_top: Option<HashMap<String, String>> = batch
            .schema()
            .field_with_name("uuid_str")
            .ok()
            .and_then(|f| {
                let md = f.metadata();
                let has_ext = md.get(UUID_EXT_KEY).is_some();
                let is_uuid_logical = md
                    .get(UUID_LOGICAL_KEY)
                    .map(|v| v.trim_matches('"') == "uuid")
                    .unwrap_or(false);
                if has_ext || is_uuid_logical {
                    Some(md.clone())
                } else {
                    None
                }
            });

        let uuid_md_union: Option<HashMap<String, String>> = batch
            .schema()
            .field_with_name("union_uuid_or_fixed10")
            .ok()
            .and_then(|f| match f.data_type() {
                DataType::Union(uf, _) => uf
                    .iter()
                    .find(|(_, child)| child.name() == "uuid")
                    .and_then(|(_, child)| {
                        let md = child.metadata();
                        let has_ext = md.get(UUID_EXT_KEY).is_some();
                        let is_uuid_logical = md
                            .get(UUID_LOGICAL_KEY)
                            .map(|v| v.trim_matches('"') == "uuid")
                            .unwrap_or(false);
                        if has_ext || is_uuid_logical {
                            Some(md.clone())
                        } else {
                            None
                        }
                    }),
                _ => None,
            });

        let add_uuid_ext_top = |f: Field| -> Field {
            if let Some(md) = &uuid_md_top {
                f.with_metadata(md.clone())
            } else {
                f
            }
        };
        let add_uuid_ext_union = |f: Field| -> Field {
            if let Some(md) = &uuid_md_union {
                f.with_metadata(md.clone())
            } else {
                f
            }
        };

        #[inline]
        fn uuid16_from_str(s: &str) -> [u8; 16] {
            let mut out = [0u8; 16];
            let mut idx = 0usize;
            let mut hi: Option<u8> = None;
            for ch in s.chars() {
                if ch == '-' {
                    continue;
                }
                let v = ch.to_digit(16).expect("invalid hex digit in UUID") as u8;
                if let Some(h) = hi {
                    out[idx] = (h << 4) | v;
                    idx += 1;
                    hi = None;
                } else {
                    hi = Some(v);
                }
            }
            assert_eq!(idx, 16, "UUID must decode to 16 bytes");
            out
        }

        fn mk_dense_union(
            fields: &UnionFields,
            type_ids: Vec<i8>,
            offsets: Vec<i32>,
            provide: impl Fn(&Field) -> Option<ArrayRef>,
        ) -> ArrayRef {
            fn empty_child_for(dt: &DataType) -> Arc<dyn Array> {
                match dt {
                    DataType::Null => Arc::new(NullArray::new(0)),
                    DataType::Boolean => Arc::new(BooleanArray::from(Vec::<bool>::new())),
                    DataType::Int32 => Arc::new(Int32Array::from(Vec::<i32>::new())),
                    DataType::Int64 => Arc::new(Int64Array::from(Vec::<i64>::new())),
                    DataType::Float32 => Arc::new(Float32Array::from(Vec::<f32>::new())),
                    DataType::Float64 => Arc::new(Float64Array::from(Vec::<f64>::new())),
                    DataType::Binary => Arc::new(BinaryArray::from(Vec::<&[u8]>::new())),
                    DataType::Utf8 => Arc::new(StringArray::from(Vec::<&str>::new())),
                    DataType::Date32 => Arc::new(Date32Array::from(Vec::<i32>::new())),
                    DataType::Time32(arrow_schema::TimeUnit::Millisecond) => {
                        Arc::new(Time32MillisecondArray::from(Vec::<i32>::new()))
                    }
                    DataType::Time64(arrow_schema::TimeUnit::Microsecond) => {
                        Arc::new(Time64MicrosecondArray::from(Vec::<i64>::new()))
                    }
                    DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, tz) => {
                        let a = TimestampMillisecondArray::from(Vec::<i64>::new());
                        Arc::new(if let Some(tz) = tz {
                            a.with_timezone(tz.clone())
                        } else {
                            a
                        })
                    }
                    DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, tz) => {
                        let a = TimestampMicrosecondArray::from(Vec::<i64>::new());
                        Arc::new(if let Some(tz) = tz {
                            a.with_timezone(tz.clone())
                        } else {
                            a
                        })
                    }
                    DataType::Interval(IntervalUnit::MonthDayNano) => Arc::new(
                        IntervalMonthDayNanoArray::from(Vec::<IntervalMonthDayNano>::new()),
                    ),
                    DataType::FixedSizeBinary(sz) => Arc::new(
                        FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                            std::iter::empty::<Option<Vec<u8>>>(),
                            *sz,
                        )
                        .unwrap(),
                    ),
                    DataType::Dictionary(_, _) => {
                        let keys = Int32Array::from(Vec::<i32>::new());
                        let values = Arc::new(StringArray::from(Vec::<&str>::new()));
                        Arc::new(DictionaryArray::<Int32Type>::try_new(keys, values).unwrap())
                    }
                    DataType::Struct(fields) => {
                        let children: Vec<ArrayRef> = fields
                            .iter()
                            .map(|f| empty_child_for(f.data_type()) as ArrayRef)
                            .collect();
                        Arc::new(StructArray::new(fields.clone(), children, None))
                    }
                    DataType::List(field) => {
                        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0]));
                        Arc::new(
                            ListArray::try_new(
                                field.clone(),
                                offsets,
                                empty_child_for(field.data_type()),
                                None,
                            )
                            .unwrap(),
                        )
                    }
                    DataType::Map(entry_field, is_sorted) => {
                        let (key_field, val_field) = match entry_field.data_type() {
                            DataType::Struct(fs) => (fs[0].clone(), fs[1].clone()),
                            other => panic!("unexpected map entries type: {other:?}"),
                        };
                        let keys = StringArray::from(Vec::<&str>::new());
                        let vals: ArrayRef = match val_field.data_type() {
                            DataType::Null => Arc::new(NullArray::new(0)) as ArrayRef,
                            DataType::Boolean => {
                                Arc::new(BooleanArray::from(Vec::<bool>::new())) as ArrayRef
                            }
                            DataType::Int32 => {
                                Arc::new(Int32Array::from(Vec::<i32>::new())) as ArrayRef
                            }
                            DataType::Int64 => {
                                Arc::new(Int64Array::from(Vec::<i64>::new())) as ArrayRef
                            }
                            DataType::Float32 => {
                                Arc::new(Float32Array::from(Vec::<f32>::new())) as ArrayRef
                            }
                            DataType::Float64 => {
                                Arc::new(Float64Array::from(Vec::<f64>::new())) as ArrayRef
                            }
                            DataType::Utf8 => {
                                Arc::new(StringArray::from(Vec::<&str>::new())) as ArrayRef
                            }
                            DataType::Binary => {
                                Arc::new(BinaryArray::from(Vec::<&[u8]>::new())) as ArrayRef
                            }
                            DataType::Union(uf, _) => {
                                let children: Vec<ArrayRef> = uf
                                    .iter()
                                    .map(|(_, f)| empty_child_for(f.data_type()))
                                    .collect();
                                Arc::new(
                                    UnionArray::try_new(
                                        uf.clone(),
                                        ScalarBuffer::<i8>::from(Vec::<i8>::new()),
                                        Some(ScalarBuffer::<i32>::from(Vec::<i32>::new())),
                                        children,
                                    )
                                    .unwrap(),
                                ) as ArrayRef
                            }
                            other => panic!("unsupported map value type: {other:?}"),
                        };
                        let entries = StructArray::new(
                            Fields::from(vec![
                                key_field.as_ref().clone(),
                                val_field.as_ref().clone(),
                            ]),
                            vec![Arc::new(keys) as ArrayRef, vals],
                            None,
                        );
                        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0]));
                        Arc::new(MapArray::new(
                            entry_field.clone(),
                            offsets,
                            entries,
                            None,
                            *is_sorted,
                        ))
                    }
                    other => panic!("empty_child_for: unhandled type {other:?}"),
                }
            }
            let children: Vec<ArrayRef> = fields
                .iter()
                .map(|(_, f)| provide(f).unwrap_or_else(|| empty_child_for(f.data_type())))
                .collect();
            Arc::new(
                UnionArray::try_new(
                    fields.clone(),
                    ScalarBuffer::<i8>::from(type_ids),
                    Some(ScalarBuffer::<i32>::from(offsets)),
                    children,
                )
                .unwrap(),
            ) as ArrayRef
        }
        let date_a: i32 = 19_000; // 2022-01-08
        let time_ms_a: i32 = 12 * 3_600_000 + 34 * 60_000 + 56_000 + 789;
        let time_us_eod: i64 = 86_400_000_000 - 1;
        let ts_ms_2024_01_01: i64 = 1_704_067_200_000; // 2024-01-01T00:00:00Z
        let ts_us_2024_01_01: i64 = ts_ms_2024_01_01 * 1_000;
        let dur_small = IntervalMonthDayNanoType::make_value(1, 2, 3_000_000_000);
        let dur_zero = IntervalMonthDayNanoType::make_value(0, 0, 0);
        let dur_large =
            IntervalMonthDayNanoType::make_value(12, 31, ((86_400_000 - 1) as i64) * 1_000_000);
        let dur_2years = IntervalMonthDayNanoType::make_value(24, 0, 0);
        let uuid1 = uuid16_from_str("fe7bc30b-4ce8-4c5e-b67c-2234a2d38e66");
        let uuid2 = uuid16_from_str("0826cc06-d2e3-4599-b4ad-af5fa6905cdb");
        let item_name = Field::LIST_FIELD_DEFAULT_NAME;
        let uf_tri = UnionFields::try_new(
            vec![0, 1, 2],
            vec![
                Field::new("int", DataType::Int32, false),
                Field::new("string", DataType::Utf8, false),
                Field::new("boolean", DataType::Boolean, false),
            ],
        )
        .unwrap();
        let uf_arr_items = UnionFields::try_new(
            vec![0, 1, 2],
            vec![
                Field::new("null", DataType::Null, false),
                Field::new("string", DataType::Utf8, false),
                Field::new("long", DataType::Int64, false),
            ],
        )
        .unwrap();
        let arr_items_field = Arc::new(Field::new(
            item_name,
            DataType::Union(uf_arr_items.clone(), UnionMode::Dense),
            true,
        ));
        let uf_map_vals = UnionFields::try_new(
            vec![0, 1, 2],
            vec![
                Field::new("string", DataType::Utf8, false),
                Field::new("double", DataType::Float64, false),
                Field::new("null", DataType::Null, false),
            ],
        )
        .unwrap();
        let map_entries_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new(
                    "value",
                    DataType::Union(uf_map_vals.clone(), UnionMode::Dense),
                    true,
                ),
            ])),
            false,
        ));
        // Enum metadata for Color (now includes name/namespace)
        let mut enum_md_color = {
            let mut m = HashMap::<String, String>::new();
            m.insert(
                crate::schema::AVRO_ENUM_SYMBOLS_METADATA_KEY.to_string(),
                serde_json::to_string(&vec!["RED", "GREEN", "BLUE"]).unwrap(),
            );
            m
        };
        enum_md_color.insert(AVRO_NAME_METADATA_KEY.to_string(), "Color".to_string());
        enum_md_color.insert(
            AVRO_NAMESPACE_METADATA_KEY.to_string(),
            "org.apache.arrow.avrotests.v1.types".to_string(),
        );
        let union_rec_a_fields = Fields::from(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ]);
        let union_rec_b_fields = Fields::from(vec![
            Field::new("x", DataType::Int64, false),
            Field::new("y", DataType::Binary, false),
        ]);
        let union_map_entries = Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Utf8, false),
            ])),
            false,
        ));
        let rec_a_md = {
            let mut m = HashMap::<String, String>::new();
            m.insert(AVRO_NAME_METADATA_KEY.to_string(), "RecA".to_string());
            m.insert(
                AVRO_NAMESPACE_METADATA_KEY.to_string(),
                "org.apache.arrow.avrotests.v1.types".to_string(),
            );
            m
        };
        let rec_b_md = {
            let mut m = HashMap::<String, String>::new();
            m.insert(AVRO_NAME_METADATA_KEY.to_string(), "RecB".to_string());
            m.insert(
                AVRO_NAMESPACE_METADATA_KEY.to_string(),
                "org.apache.arrow.avrotests.v1.types".to_string(),
            );
            m
        };
        let uf_union_big = UnionFields::try_new(
            vec![0, 1, 2, 3, 4],
            vec![
                Field::new(
                    "map",
                    DataType::Map(union_map_entries.clone(), false),
                    false,
                ),
                Field::new(
                    "array",
                    DataType::List(Arc::new(Field::new(item_name, DataType::Int64, false))),
                    false,
                ),
                Field::new(
                    "org.apache.arrow.avrotests.v1.types.RecB",
                    DataType::Struct(union_rec_b_fields.clone()),
                    false,
                )
                .with_metadata(rec_b_md.clone()),
                Field::new(
                    "org.apache.arrow.avrotests.v1.types.RecA",
                    DataType::Struct(union_rec_a_fields.clone()),
                    false,
                )
                .with_metadata(rec_a_md.clone()),
                Field::new(
                    "org.apache.arrow.avrotests.v1.types.Color",
                    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                    false,
                )
                .with_metadata(enum_md_color.clone()),
            ],
        )
        .unwrap();
        let fx4_md = {
            let mut m = HashMap::<String, String>::new();
            m.insert(AVRO_NAME_METADATA_KEY.to_string(), "Fx4".to_string());
            m.insert(
                AVRO_NAMESPACE_METADATA_KEY.to_string(),
                "org.apache.arrow.avrotests.v1".to_string(),
            );
            m
        };
        let uf_date_fixed4 = UnionFields::try_new(
            vec![0, 1],
            vec![
                Field::new(
                    "org.apache.arrow.avrotests.v1.Fx4",
                    DataType::FixedSizeBinary(4),
                    false,
                )
                .with_metadata(fx4_md.clone()),
                Field::new("date", DataType::Date32, false),
            ],
        )
        .unwrap();
        let dur12u_md = {
            let mut m = HashMap::<String, String>::new();
            m.insert(AVRO_NAME_METADATA_KEY.to_string(), "Dur12U".to_string());
            m.insert(
                AVRO_NAMESPACE_METADATA_KEY.to_string(),
                "org.apache.arrow.avrotests.v1".to_string(),
            );
            m
        };
        let uf_dur_or_str = UnionFields::try_new(
            vec![0, 1],
            vec![
                Field::new("string", DataType::Utf8, false),
                Field::new(
                    "org.apache.arrow.avrotests.v1.Dur12U",
                    DataType::Interval(arrow_schema::IntervalUnit::MonthDayNano),
                    false,
                )
                .with_metadata(dur12u_md.clone()),
            ],
        )
        .unwrap();
        let fx10_md = {
            let mut m = HashMap::<String, String>::new();
            m.insert(AVRO_NAME_METADATA_KEY.to_string(), "Fx10".to_string());
            m.insert(
                AVRO_NAMESPACE_METADATA_KEY.to_string(),
                "org.apache.arrow.avrotests.v1".to_string(),
            );
            m
        };
        let uf_uuid_or_fx10 = UnionFields::try_new(
            vec![0, 1],
            vec![
                Field::new(
                    "org.apache.arrow.avrotests.v1.Fx10",
                    DataType::FixedSizeBinary(10),
                    false,
                )
                .with_metadata(fx10_md.clone()),
                add_uuid_ext_union(Field::new("uuid", DataType::FixedSizeBinary(16), false)),
            ],
        )
        .unwrap();
        let uf_kv_val = UnionFields::try_new(
            vec![0, 1, 2],
            vec![
                Field::new("null", DataType::Null, false),
                Field::new("int", DataType::Int32, false),
                Field::new("long", DataType::Int64, false),
            ],
        )
        .unwrap();
        let kv_fields = Fields::from(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new(
                "val",
                DataType::Union(uf_kv_val.clone(), UnionMode::Dense),
                true,
            ),
        ]);
        let kv_item_field = Arc::new(Field::new(
            item_name,
            DataType::Struct(kv_fields.clone()),
            false,
        ));
        let map_int_entries = Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int32, false),
            ])),
            false,
        ));
        let uf_map_or_array = UnionFields::try_new(
            vec![0, 1],
            vec![
                Field::new(
                    "array",
                    DataType::List(Arc::new(Field::new(item_name, DataType::Int32, false))),
                    false,
                ),
                Field::new("map", DataType::Map(map_int_entries.clone(), false), false),
            ],
        )
        .unwrap();
        let mut enum_md_status = {
            let mut m = HashMap::<String, String>::new();
            m.insert(
                crate::schema::AVRO_ENUM_SYMBOLS_METADATA_KEY.to_string(),
                serde_json::to_string(&vec!["UNKNOWN", "NEW", "PROCESSING", "DONE"]).unwrap(),
            );
            m
        };
        enum_md_status.insert(AVRO_NAME_METADATA_KEY.to_string(), "Status".to_string());
        enum_md_status.insert(
            AVRO_NAMESPACE_METADATA_KEY.to_string(),
            "org.apache.arrow.avrotests.v1.types".to_string(),
        );
        let mut dec20_md = HashMap::<String, String>::new();
        dec20_md.insert("precision".to_string(), "20".to_string());
        dec20_md.insert("scale".to_string(), "4".to_string());
        dec20_md.insert(AVRO_NAME_METADATA_KEY.to_string(), "DecFix20".to_string());
        dec20_md.insert(
            AVRO_NAMESPACE_METADATA_KEY.to_string(),
            "org.apache.arrow.avrotests.v1.types".to_string(),
        );
        let mut dec10_md = HashMap::<String, String>::new();
        dec10_md.insert("precision".to_string(), "10".to_string());
        dec10_md.insert("scale".to_string(), "2".to_string());
        let fx16_top_md = {
            let mut m = HashMap::<String, String>::new();
            m.insert(AVRO_NAME_METADATA_KEY.to_string(), "Fx16".to_string());
            m.insert(
                AVRO_NAMESPACE_METADATA_KEY.to_string(),
                "org.apache.arrow.avrotests.v1.types".to_string(),
            );
            m
        };
        let dur12_top_md = {
            let mut m = HashMap::<String, String>::new();
            m.insert(AVRO_NAME_METADATA_KEY.to_string(), "Dur12".to_string());
            m.insert(
                AVRO_NAMESPACE_METADATA_KEY.to_string(),
                "org.apache.arrow.avrotests.v1.types".to_string(),
            );
            m
        };
        #[cfg(feature = "small_decimals")]
        let dec20_dt = DataType::Decimal128(20, 4);
        #[cfg(not(feature = "small_decimals"))]
        let dec20_dt = DataType::Decimal128(20, 4);
        #[cfg(feature = "small_decimals")]
        let dec10_dt = DataType::Decimal64(10, 2);
        #[cfg(not(feature = "small_decimals"))]
        let dec10_dt = DataType::Decimal128(10, 2);
        let fields: Vec<FieldRef> = vec![
            Arc::new(Field::new(
                "person",
                DataType::Struct(Fields::from(vec![
                    Field::new("name", DataType::Utf8, false),
                    Field::new("age", DataType::Int32, false),
                ])),
                false,
            )),
            Arc::new(Field::new("old_count", DataType::Int32, false)),
            Arc::new(Field::new(
                "union_map_or_array_int",
                DataType::Union(uf_map_or_array.clone(), UnionMode::Dense),
                false,
            )),
            Arc::new(Field::new(
                "array_records_with_union",
                DataType::List(kv_item_field.clone()),
                false,
            )),
            Arc::new(Field::new(
                "union_uuid_or_fixed10",
                DataType::Union(uf_uuid_or_fx10.clone(), UnionMode::Dense),
                false,
            )),
            Arc::new(Field::new(
                "union_interval_or_string",
                DataType::Union(uf_dur_or_str.clone(), UnionMode::Dense),
                false,
            )),
            Arc::new(Field::new(
                "union_date_or_fixed4",
                DataType::Union(uf_date_fixed4.clone(), UnionMode::Dense),
                false,
            )),
            Arc::new(Field::new(
                "union_enum_record_array_map",
                DataType::Union(uf_union_big.clone(), UnionMode::Dense),
                false,
            )),
            Arc::new(Field::new(
                "maybe_auth",
                DataType::Struct(Fields::from(vec![
                    Field::new("user", DataType::Utf8, false),
                    Field::new("token", DataType::Binary, true), // [bytes,null] -> nullable bytes
                ])),
                false,
            )),
            Arc::new(Field::new(
                "address",
                DataType::Struct(Fields::from(vec![
                    Field::new("street_name", DataType::Utf8, false),
                    Field::new("zip", DataType::Int32, false),
                    Field::new("country", DataType::Utf8, false),
                ])),
                false,
            )),
            Arc::new(Field::new(
                "map_union",
                DataType::Map(map_entries_field.clone(), false),
                false,
            )),
            Arc::new(Field::new(
                "arr_union",
                DataType::List(arr_items_field.clone()),
                false,
            )),
            Arc::new(
                Field::new(
                    "status",
                    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                    false,
                )
                .with_metadata(enum_md_status.clone()),
            ),
            Arc::new(
                Field::new(
                    "interval_mdn",
                    DataType::Interval(IntervalUnit::MonthDayNano),
                    false,
                )
                .with_metadata(dur12_top_md.clone()),
            ),
            Arc::new(Field::new(
                "ts_micros_local",
                DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
                false,
            )),
            Arc::new(Field::new(
                "ts_millis_local",
                DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
                false,
            )),
            Arc::new(Field::new(
                "ts_micros_utc",
                DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("+00:00".into())),
                false,
            )),
            Arc::new(Field::new(
                "ts_millis_utc",
                DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, Some("+00:00".into())),
                false,
            )),
            Arc::new(Field::new(
                "t_micros",
                DataType::Time64(arrow_schema::TimeUnit::Microsecond),
                false,
            )),
            Arc::new(Field::new(
                "t_millis",
                DataType::Time32(arrow_schema::TimeUnit::Millisecond),
                false,
            )),
            Arc::new(Field::new("d_date", DataType::Date32, false)),
            Arc::new(add_uuid_ext_top(Field::new(
                "uuid_str",
                DataType::FixedSizeBinary(16),
                false,
            ))),
            Arc::new(Field::new("dec_fix_s20_4", dec20_dt, false).with_metadata(dec20_md.clone())),
            Arc::new(
                Field::new("dec_bytes_s10_2", dec10_dt, false).with_metadata(dec10_md.clone()),
            ),
            Arc::new(
                Field::new("fx16_plain", DataType::FixedSizeBinary(16), false)
                    .with_metadata(fx16_top_md.clone()),
            ),
            Arc::new(Field::new("raw_bytes", DataType::Binary, false)),
            Arc::new(Field::new("str_utf8", DataType::Utf8, false)),
            Arc::new(Field::new(
                "tri_union_prim",
                DataType::Union(uf_tri.clone(), UnionMode::Dense),
                false,
            )),
            Arc::new(Field::new("opt_str_nullsecond", DataType::Utf8, true)),
            Arc::new(Field::new("opt_i32_nullfirst", DataType::Int32, true)),
            Arc::new(Field::new("count_i64", DataType::Int64, false)),
            Arc::new(Field::new("count_i32", DataType::Int64, false)),
            Arc::new(Field::new("ratio_f64", DataType::Float64, false)),
            Arc::new(Field::new("ratio_f32", DataType::Float64, false)),
            Arc::new(Field::new("flag", DataType::Boolean, false)),
            Arc::new(Field::new("identifier", DataType::Int64, false)),
        ];
        let expected_schema = Arc::new(arrow_schema::Schema::new(Fields::from(fields)));
        let mut cols: Vec<ArrayRef> = vec![
            Arc::new(StructArray::new(
                match expected_schema
                    .field_with_name("person")
                    .unwrap()
                    .data_type()
                {
                    DataType::Struct(fs) => fs.clone(),
                    _ => unreachable!(),
                },
                vec![
                    Arc::new(StringArray::from(vec!["Alice", "Bob", "Carol", "Dave"])) as ArrayRef,
                    Arc::new(Int32Array::from(vec![30, 0, 25, 41])) as ArrayRef,
                ],
                None,
            )) as ArrayRef,
            Arc::new(Int32Array::from(vec![100, 42, 7, 42])) as ArrayRef,
        ];
        {
            let map_child: ArrayRef = {
                let keys = StringArray::from(vec!["x", "y", "only"]);
                let vals = Int32Array::from(vec![1, 2, 10]);
                let entries = StructArray::new(
                    Fields::from(vec![
                        Field::new("key", DataType::Utf8, false),
                        Field::new("value", DataType::Int32, false),
                    ]),
                    vec![Arc::new(keys) as ArrayRef, Arc::new(vals) as ArrayRef],
                    None,
                );
                let moff = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 2, 3]));
                Arc::new(MapArray::new(
                    map_int_entries.clone(),
                    moff,
                    entries,
                    None,
                    false,
                )) as ArrayRef
            };
            let list_child: ArrayRef = {
                let values = Int32Array::from(vec![1, 2, 3, 0]);
                let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 3, 4]));
                Arc::new(
                    ListArray::try_new(
                        Arc::new(Field::new(item_name, DataType::Int32, false)),
                        offsets,
                        Arc::new(values),
                        None,
                    )
                    .unwrap(),
                ) as ArrayRef
            };
            let tids = vec![1, 0, 1, 0];
            let offs = vec![0, 0, 1, 1];
            let arr = mk_dense_union(&uf_map_or_array, tids, offs, |f| match f.name().as_str() {
                "array" => Some(list_child.clone()),
                "map" => Some(map_child.clone()),
                _ => None,
            });
            cols.push(arr);
        }
        {
            let keys = Arc::new(StringArray::from(vec!["k1", "k2", "k", "k3", "x"])) as ArrayRef;
            let type_ids = vec![1, 0, 2, 0, 1];
            let offsets = vec![0, 0, 0, 1, 1];
            let vals = mk_dense_union(&uf_kv_val, type_ids, offsets, |f| match f.data_type() {
                DataType::Int32 => Some(Arc::new(Int32Array::from(vec![5, -5])) as ArrayRef),
                DataType::Int64 => Some(Arc::new(Int64Array::from(vec![99i64])) as ArrayRef),
                DataType::Null => Some(Arc::new(NullArray::new(2)) as ArrayRef),
                _ => None,
            });
            let values_struct =
                Arc::new(StructArray::new(kv_fields.clone(), vec![keys, vals], None));
            let list_offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 2, 3, 4, 5]));
            let arr = Arc::new(
                ListArray::try_new(kv_item_field.clone(), list_offsets, values_struct, None)
                    .unwrap(),
            ) as ArrayRef;
            cols.push(arr);
        }
        {
            let type_ids = vec![1, 0, 1, 0]; // [uuid, fixed10, uuid, fixed10] but uf order = [fixed10, uuid]
            let offs = vec![0, 0, 1, 1];
            let arr = mk_dense_union(&uf_uuid_or_fx10, type_ids, offs, |f| match f.data_type() {
                DataType::FixedSizeBinary(16) => {
                    let it = [Some(uuid1), Some(uuid2)].into_iter();
                    Some(Arc::new(
                        FixedSizeBinaryArray::try_from_sparse_iter_with_size(it, 16).unwrap(),
                    ) as ArrayRef)
                }
                DataType::FixedSizeBinary(10) => {
                    let fx10_a = [0xAAu8; 10];
                    let fx10_b = [0x00u8, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99];
                    let it = [Some(fx10_a), Some(fx10_b)].into_iter();
                    Some(Arc::new(
                        FixedSizeBinaryArray::try_from_sparse_iter_with_size(it, 10).unwrap(),
                    ) as ArrayRef)
                }
                _ => None,
            });
            cols.push(arr);
        }
        {
            let type_ids = vec![1, 0, 1, 0]; // [duration, string, duration, string] but uf order = [string, duration]
            let offs = vec![0, 0, 1, 1];
            let arr = mk_dense_union(&uf_dur_or_str, type_ids, offs, |f| match f.data_type() {
                DataType::Interval(arrow_schema::IntervalUnit::MonthDayNano) => Some(Arc::new(
                    IntervalMonthDayNanoArray::from(vec![dur_small, dur_large]),
                )
                    as ArrayRef),
                DataType::Utf8 => Some(Arc::new(StringArray::from(vec![
                    "duration-as-text",
                    "iso-8601-period-P1Y",
                ])) as ArrayRef),
                _ => None,
            });
            cols.push(arr);
        }
        {
            let type_ids = vec![1, 0, 1, 0]; // [date, fixed, date, fixed] but uf order = [fixed, date]
            let offs = vec![0, 0, 1, 1];
            let arr = mk_dense_union(&uf_date_fixed4, type_ids, offs, |f| match f.data_type() {
                DataType::Date32 => Some(Arc::new(Date32Array::from(vec![date_a, 0])) as ArrayRef),
                DataType::FixedSizeBinary(4) => {
                    let it = [Some(*b"\x00\x11\x22\x33"), Some(*b"ABCD")].into_iter();
                    Some(Arc::new(
                        FixedSizeBinaryArray::try_from_sparse_iter_with_size(it, 4).unwrap(),
                    ) as ArrayRef)
                }
                _ => None,
            });
            cols.push(arr);
        }
        {
            let tids = vec![4, 3, 1, 0]; // uf order = [map(0), array(1), RecB(2), RecA(3), enum(4)]
            let offs = vec![0, 0, 0, 0];
            let arr = mk_dense_union(&uf_union_big, tids, offs, |f| match f.data_type() {
                DataType::Dictionary(_, _) => {
                    let keys = Int32Array::from(vec![0i32]);
                    let values =
                        Arc::new(StringArray::from(vec!["RED", "GREEN", "BLUE"])) as ArrayRef;
                    Some(
                        Arc::new(DictionaryArray::<Int32Type>::try_new(keys, values).unwrap())
                            as ArrayRef,
                    )
                }
                DataType::Struct(fs) if fs == &union_rec_a_fields => {
                    let a = Int32Array::from(vec![7]);
                    let b = StringArray::from(vec!["rec"]);
                    Some(Arc::new(StructArray::new(
                        fs.clone(),
                        vec![Arc::new(a) as ArrayRef, Arc::new(b) as ArrayRef],
                        None,
                    )) as ArrayRef)
                }
                DataType::List(_) => {
                    let values = Int64Array::from(vec![1i64, 2, 3]);
                    let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 3]));
                    Some(Arc::new(
                        ListArray::try_new(
                            Arc::new(Field::new(item_name, DataType::Int64, false)),
                            offsets,
                            Arc::new(values),
                            None,
                        )
                        .unwrap(),
                    ) as ArrayRef)
                }
                DataType::Map(_, _) => {
                    let keys = StringArray::from(vec!["k"]);
                    let vals = StringArray::from(vec!["v"]);
                    let entries = StructArray::new(
                        Fields::from(vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::Utf8, false),
                        ]),
                        vec![Arc::new(keys) as ArrayRef, Arc::new(vals) as ArrayRef],
                        None,
                    );
                    let moff = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 1]));
                    Some(Arc::new(MapArray::new(
                        union_map_entries.clone(),
                        moff,
                        entries,
                        None,
                        false,
                    )) as ArrayRef)
                }
                _ => None,
            });
            cols.push(arr);
        }
        {
            let fs = match expected_schema
                .field_with_name("maybe_auth")
                .unwrap()
                .data_type()
            {
                DataType::Struct(fs) => fs.clone(),
                _ => unreachable!(),
            };
            let user =
                Arc::new(StringArray::from(vec!["alice", "bob", "carol", "dave"])) as ArrayRef;
            let token_values: Vec<Option<&[u8]>> = vec![
                None,
                Some(b"\x01\x02\x03".as_ref()),
                None,
                Some(b"".as_ref()),
            ];
            let token = Arc::new(BinaryArray::from(token_values)) as ArrayRef;
            cols.push(Arc::new(StructArray::new(fs, vec![user, token], None)) as ArrayRef);
        }
        {
            let fs = match expected_schema
                .field_with_name("address")
                .unwrap()
                .data_type()
            {
                DataType::Struct(fs) => fs.clone(),
                _ => unreachable!(),
            };
            let street = Arc::new(StringArray::from(vec![
                "100 Main",
                "",
                "42 Galaxy Way",
                "End Ave",
            ])) as ArrayRef;
            let zip = Arc::new(Int32Array::from(vec![12345, 0, 42424, 1])) as ArrayRef;
            let country = Arc::new(StringArray::from(vec!["US", "CA", "US", "GB"])) as ArrayRef;
            cols.push(Arc::new(StructArray::new(fs, vec![street, zip, country], None)) as ArrayRef);
        }
        {
            let keys = StringArray::from(vec!["a", "b", "c", "neg", "pi", "ok"]);
            let moff = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 3, 4, 4, 6]));
            let tid_s = 0; // string
            let tid_d = 1; // double
            let tid_n = 2; // null
            let type_ids = vec![tid_d, tid_n, tid_s, tid_d, tid_d, tid_s];
            let offsets = vec![0, 0, 0, 1, 2, 1];
            let pi_5dp = (std::f64::consts::PI * 100_000.0).trunc() / 100_000.0;
            let vals = mk_dense_union(&uf_map_vals, type_ids, offsets, |f| match f.data_type() {
                DataType::Float64 => {
                    Some(Arc::new(Float64Array::from(vec![1.5f64, -0.5, pi_5dp])) as ArrayRef)
                }
                DataType::Utf8 => {
                    Some(Arc::new(StringArray::from(vec!["yes", "true"])) as ArrayRef)
                }
                DataType::Null => Some(Arc::new(NullArray::new(1)) as ArrayRef),
                _ => None,
            });
            let entries = StructArray::new(
                Fields::from(vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new(
                        "value",
                        DataType::Union(uf_map_vals.clone(), UnionMode::Dense),
                        true,
                    ),
                ]),
                vec![Arc::new(keys) as ArrayRef, vals],
                None,
            );
            let map = Arc::new(MapArray::new(
                map_entries_field.clone(),
                moff,
                entries,
                None,
                false,
            )) as ArrayRef;
            cols.push(map);
        }
        {
            let type_ids = vec![
                2, 1, 0, 2, 0, 1, 2, 2, 1, 0,
                2, // long,string,null,long,null,string,long,long,string,null,long
            ];
            let offsets = vec![0, 0, 0, 1, 1, 1, 2, 3, 2, 2, 4];
            let values =
                mk_dense_union(&uf_arr_items, type_ids, offsets, |f| match f.data_type() {
                    DataType::Int64 => {
                        Some(Arc::new(Int64Array::from(vec![1i64, -3, 0, -1, 0])) as ArrayRef)
                    }
                    DataType::Utf8 => {
                        Some(Arc::new(StringArray::from(vec!["x", "z", "end"])) as ArrayRef)
                    }
                    DataType::Null => Some(Arc::new(NullArray::new(3)) as ArrayRef),
                    _ => None,
                });
            let list_offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 4, 7, 8, 11]));
            let arr = Arc::new(
                ListArray::try_new(arr_items_field.clone(), list_offsets, values, None).unwrap(),
            ) as ArrayRef;
            cols.push(arr);
        }
        {
            let keys = Int32Array::from(vec![1, 2, 3, 0]); // NEW, PROCESSING, DONE, UNKNOWN
            let values = Arc::new(StringArray::from(vec![
                "UNKNOWN",
                "NEW",
                "PROCESSING",
                "DONE",
            ])) as ArrayRef;
            let dict = DictionaryArray::<Int32Type>::try_new(keys, values).unwrap();
            cols.push(Arc::new(dict) as ArrayRef);
        }
        cols.push(Arc::new(IntervalMonthDayNanoArray::from(vec![
            dur_small, dur_zero, dur_large, dur_2years,
        ])) as ArrayRef);
        cols.push(Arc::new(TimestampMicrosecondArray::from(vec![
            ts_us_2024_01_01 + 123_456,
            0,
            ts_us_2024_01_01 + 101_112,
            987_654_321,
        ])) as ArrayRef);
        cols.push(Arc::new(TimestampMillisecondArray::from(vec![
            ts_ms_2024_01_01 + 86_400_000,
            0,
            ts_ms_2024_01_01 + 789,
            123_456_789,
        ])) as ArrayRef);
        {
            let a = TimestampMicrosecondArray::from(vec![
                ts_us_2024_01_01,
                1,
                ts_us_2024_01_01 + 456,
                0,
            ])
            .with_timezone("+00:00");
            cols.push(Arc::new(a) as ArrayRef);
        }
        {
            let a = TimestampMillisecondArray::from(vec![
                ts_ms_2024_01_01,
                -1,
                ts_ms_2024_01_01 + 123,
                0,
            ])
            .with_timezone("+00:00");
            cols.push(Arc::new(a) as ArrayRef);
        }
        cols.push(Arc::new(Time64MicrosecondArray::from(vec![
            time_us_eod,
            0,
            1,
            1_000_000,
        ])) as ArrayRef);
        cols.push(Arc::new(Time32MillisecondArray::from(vec![
            time_ms_a,
            0,
            1,
            86_400_000 - 1,
        ])) as ArrayRef);
        cols.push(Arc::new(Date32Array::from(vec![date_a, 0, 1, 365])) as ArrayRef);
        {
            let it = [Some(uuid1), Some(uuid2), Some(uuid1), Some(uuid2)].into_iter();
            cols.push(Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(it, 16).unwrap(),
            ) as ArrayRef);
        }
        {
            #[cfg(feature = "small_decimals")]
            let arr = Arc::new(
                Decimal128Array::from_iter_values([1_234_567_891_234i128, -420_000i128, 0, -1i128])
                    .with_precision_and_scale(20, 4)
                    .unwrap(),
            ) as ArrayRef;
            #[cfg(not(feature = "small_decimals"))]
            let arr = Arc::new(
                Decimal128Array::from_iter_values([1_234_567_891_234i128, -420_000i128, 0, -1i128])
                    .with_precision_and_scale(20, 4)
                    .unwrap(),
            ) as ArrayRef;
            cols.push(arr);
        }
        {
            #[cfg(feature = "small_decimals")]
            let arr = Arc::new(
                Decimal64Array::from_iter_values([123456i64, -1, 0, 9_999_999_999i64])
                    .with_precision_and_scale(10, 2)
                    .unwrap(),
            ) as ArrayRef;
            #[cfg(not(feature = "small_decimals"))]
            let arr = Arc::new(
                Decimal128Array::from_iter_values([123456i128, -1, 0, 9_999_999_999i128])
                    .with_precision_and_scale(10, 2)
                    .unwrap(),
            ) as ArrayRef;
            cols.push(arr);
        }
        {
            let it = [
                Some(*b"0123456789ABCDEF"),
                Some([0u8; 16]),
                Some(*b"ABCDEFGHIJKLMNOP"),
                Some([0xAA; 16]),
            ]
            .into_iter();
            cols.push(Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(it, 16).unwrap(),
            ) as ArrayRef);
        }
        cols.push(Arc::new(BinaryArray::from(vec![
            b"\x00\x01".as_ref(),
            b"".as_ref(),
            b"\xFF\x00".as_ref(),
            b"\x10\x20\x30\x40".as_ref(),
        ])) as ArrayRef);
        cols.push(Arc::new(StringArray::from(vec!["hello", "", "world", "✓ unicode"])) as ArrayRef);
        {
            let tids = vec![0, 1, 2, 1];
            let offs = vec![0, 0, 0, 1];
            let arr = mk_dense_union(&uf_tri, tids, offs, |f| match f.data_type() {
                DataType::Int32 => Some(Arc::new(Int32Array::from(vec![0])) as ArrayRef),
                DataType::Utf8 => Some(Arc::new(StringArray::from(vec!["hi", ""])) as ArrayRef),
                DataType::Boolean => Some(Arc::new(BooleanArray::from(vec![true])) as ArrayRef),
                _ => None,
            });
            cols.push(arr);
        }
        cols.push(Arc::new(StringArray::from(vec![
            Some("alpha"),
            None,
            Some("s3"),
            Some(""),
        ])) as ArrayRef);
        cols.push(Arc::new(Int32Array::from(vec![None, Some(42), None, Some(0)])) as ArrayRef);
        cols.push(Arc::new(Int64Array::from(vec![
            7_000_000_000i64,
            -2,
            0,
            -9_876_543_210i64,
        ])) as ArrayRef);
        cols.push(Arc::new(Int64Array::from(vec![7i64, -1, 0, 123])) as ArrayRef);
        cols.push(Arc::new(Float64Array::from(vec![2.5f64, -1.0, 7.0, -2.25])) as ArrayRef);
        cols.push(Arc::new(Float64Array::from(vec![1.25f64, -0.0, 3.5, 9.75])) as ArrayRef);
        cols.push(Arc::new(BooleanArray::from(vec![true, false, true, false])) as ArrayRef);
        cols.push(Arc::new(Int64Array::from(vec![1, 2, 3, 4])) as ArrayRef);
        let expected = RecordBatch::try_new(expected_schema, cols).unwrap();
        assert_eq!(
            expected, batch,
            "entire RecordBatch mismatch (schema, all columns, all rows)"
        );
    }
}
