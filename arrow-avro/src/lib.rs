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

//! Convert data to / from the [Apache Arrow] memory format and [Apache Avro].
//!
//! This crate provides:
//! - a [`reader`] that decodes Avro (Object Container Files, Avro Single‑Object encoding,
//!   and Confluent Schema Registry wire format) into Arrow `RecordBatch`es,
//! - and a [`writer`] that encodes Arrow `RecordBatch`es into Avro (OCF or raw Avro binary).
//!
//! If you’re new to Arrow or Avro, see:
//! - Arrow project site: <https://arrow.apache.org/>
//! - Avro 1.11.1 specification: <https://avro.apache.org/docs/1.11.1/specification/>
//!
//! ## Example: OCF (Object Container File) round‑trip
//!
//! The example below creates an Arrow table, writes an **Avro OCF** fully in memory,
//! and then reads it back. OCF is a self‑describing file format that embeds the Avro
//! schema in a header with optional compression and block sync markers.
//! Spec: <https://avro.apache.org/docs/1.11.1/specification/#object-container-files>
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
//! // Build a tiny Arrow batch
//! let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
//! let batch = RecordBatch::try_new(
//!     Arc::new(schema.clone()),
//!     vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef],
//! )?;
//!
//! // Write an Avro **Object Container File** (OCF) to a Vec<u8>
//! let sink: Vec<u8> = Vec::new();
//! let mut w = AvroWriter::new(sink, schema.clone())?;
//! w.write(&batch)?;
//! w.finish()?;
//! let bytes = w.into_inner();
//! assert!(!bytes.is_empty());
//!
//! // Read it back
//! let mut r = ReaderBuilder::new().build(Cursor::new(bytes))?;
//! let out = r.next().unwrap()?;
//! assert_eq!(out.num_rows(), 3);
//! # Ok(()) }
//! ```
//!
//! ## Quickstart: Confluent wire‑format round‑trip *(runnable)*
//!
//! The **Confluent Schema Registry wire format** prefixes each Avro message with a
//! 1‑byte magic `0x00` and a **4‑byte big‑endian** schema ID, followed by the Avro body.
//! See: <https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format>
//!
//! In this round‑trip, we:
//! 1) Use `AvroStreamWriter` to create a **raw Avro body** for a single‑row batch,
//! 2) Wrap it with the Confluent prefix (magic and schema ID),
//! 3) Decode it back to Arrow using a `Decoder` configured with a `SchemaStore` that
//!    maps the schema ID to the Avro schema used by the writer.
//!
//! ```
//! use std::collections::HashMap;
//! use std::sync::Arc;
//! use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
//! use arrow_schema::{DataType, Field, Schema};
//! use arrow_avro::writer::{AvroStreamWriter, WriterBuilder};
//! use arrow_avro::reader::ReaderBuilder;
//! use arrow_avro::schema::{
//!     AvroSchema, SchemaStore, Fingerprint, FingerprintAlgorithm,
//!     FingerprintStrategy, SCHEMA_METADATA_KEY
//! };
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // Writer schema registered under Schema Registry ID 1
//! let avro_json = r#"{
//!   "type":"record","name":"User",
//!   "fields":[{"name":"id","type":"long"},{"name":"name","type":"string"}]
//! }"#;
//!
//! let mut store = SchemaStore::new_with_type(FingerprintAlgorithm::None);
//! let id: u32 = 1;
//! store.set(Fingerprint::Id(id), AvroSchema::new(avro_json.to_string()))?;
//!
//! // Build an Arrow schema that references the same Avro JSON
//! let mut md = HashMap::new();
//! md.insert(SCHEMA_METADATA_KEY.to_string(), avro_json.to_string());
//! let schema = Schema::new_with_metadata(
//!     vec![
//!         Field::new("id", DataType::Int64, false),
//!         Field::new("name", DataType::Utf8, false),
//!     ],
//!     md,
//! );
//!
//! // One‑row batch: { id: 42, name: "alice" }
//! let batch = RecordBatch::try_new(
//!     Arc::new(schema.clone()),
//!     vec![
//!         Arc::new(Int64Array::from(vec![42])) as ArrayRef,
//!         Arc::new(StringArray::from(vec!["alice"])) as ArrayRef,
//!     ],
//! )?;
//!
//! // Stream‑write a single record, letting the writer add the **Confluent** prefix.
//! let sink: Vec<u8> = Vec::new();
//! let mut w: AvroStreamWriter<Vec<u8>> = WriterBuilder::new(schema.clone())
//!     .with_fingerprint_strategy(FingerprintStrategy::Id(id))
//!     .build(sink)?;
//! w.write(&batch)?;
//! w.finish()?;
//! let frame = w.into_inner(); // already: 0x00 + 4B BE ID + Avro body
//! assert!(frame.len() > 5);
//!
//! // Decode
//! let mut dec = ReaderBuilder::new()
//!   .with_writer_schema_store(store)
//!   .build_decoder()?;
//! dec.decode(&frame)?;
//! let out = dec.flush()?.expect("one row");
//! assert_eq!(out.num_rows(), 1);
//! # Ok(()) }
//! ```
//!
//! ## Quickstart: Avro Single‑Object Encoding round‑trip *(runnable)*
//!
//! Avro **Single‑Object Encoding (SOE)** wraps an Avro body with a 2‑byte marker
//! `0xC3 0x01` and an **8‑byte little‑endian CRC‑64‑AVRO Rabin fingerprint** of the
//! writer schema, then the Avro body. Spec:
//! <https://avro.apache.org/docs/1.11.1/specification/#single-object-encoding>
//!
//! This example registers the writer schema (computing a Rabin fingerprint), writes a
//! single‑row Avro body (using `AvroStreamWriter`), constructs the SOE frame, and decodes it back to Arrow.
//!
//! ```
//! use std::collections::HashMap;
//! use std::sync::Arc;
//! use arrow_array::{ArrayRef, Int64Array, RecordBatch};
//! use arrow_schema::{DataType, Field, Schema};
//! use arrow_avro::writer::{AvroStreamWriter, WriterBuilder};
//! use arrow_avro::reader::ReaderBuilder;
//! use arrow_avro::schema::{AvroSchema, SchemaStore, FingerprintStrategy, SCHEMA_METADATA_KEY};
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // Writer schema: { "type":"record","name":"User","fields":[{"name":"x","type":"long"}] }
//! let writer_json = r#"{"type":"record","name":"User","fields":[{"name":"x","type":"long"}]}"#;
//! let mut store = SchemaStore::new(); // Rabin CRC‑64‑AVRO by default
//! let _fp = store.register(AvroSchema::new(writer_json.to_string()))?;
//!
//! // Build an Arrow schema that references the same Avro JSON
//! let mut md = HashMap::new();
//! md.insert(SCHEMA_METADATA_KEY.to_string(), writer_json.to_string());
//! let schema = Schema::new_with_metadata(
//!     vec![Field::new("x", DataType::Int64, false)],
//!     md,
//! );
//!
//! // One‑row batch: { x: 7 }
//! let batch = RecordBatch::try_new(
//!     Arc::new(schema.clone()),
//!     vec![Arc::new(Int64Array::from(vec![7])) as ArrayRef],
//! )?;
//!
//! // Stream‑write a single record; the writer adds **SOE** (C3 01 + Rabin) automatically.
//! let sink: Vec<u8> = Vec::new();
//! let mut w: AvroStreamWriter<Vec<u8>> = WriterBuilder::new(schema.clone())
//!     .with_fingerprint_strategy(FingerprintStrategy::Rabin)
//!     .build(sink)?;
//! w.write(&batch)?;
//! w.finish()?;
//! let frame = w.into_inner(); // already: C3 01 + 8B LE Rabin + Avro body
//! assert!(frame.len() > 10);
//!
//! // Decode
//! let mut dec = ReaderBuilder::new()
//!   .with_writer_schema_store(store)
//!   .build_decoder()?;
//! dec.decode(&frame)?;
//! let out = dec.flush()?.expect("one row");
//! assert_eq!(out.num_rows(), 1);
//! # Ok(()) }
//! ```
//!
//! ---
//!
//! ### Modules
//!
//! - [`reader`]: read Avro (OCF, SOE, Confluent) into Arrow `RecordBatch`es.
//! - [`writer`]: write Arrow `RecordBatch`es as Avro (OCF, SOE, Confluent).
//! - [`schema`]: Avro schema parsing / fingerprints / registries.
//! - [`compression`]: codecs used for OCF blocks (i.e., Deflate, Snappy, Zstandard).
//! - [`codec`]: internal Avro↔Arrow type conversion and row decode/encode plans.
//!
//! ### Features
//!
//! - `md5`: enables dependency `md5` for md5 fingerprint hashing
//! - `sha256`: enables dependency `sha2` for sha256 fingerprint hashing
//! - `small_decimals`: enables support for small decimal types
//! - `avro_custom_types`: Enables custom logic that interprets an annotated Avro long with logicalType values of `arrow.duration-nanos`, `arrow.duration-micros`, `arrow.duration-millis`, or `arrow.duration-seconds` as a more descriptive Arrow Duration(TimeUnit) type.
//!
//! [Apache Arrow]: https://arrow.apache.org/
//! [Apache Avro]: https://avro.apache.org/

#![doc(
    html_logo_url = "https://arrow.apache.org/img/arrow-logo_chevrons_black-txt_white-bg.svg",
    html_favicon_url = "https://arrow.apache.org/img/arrow-logo_chevrons_black-txt_transparent-bg.svg"
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_docs)]
#![allow(unused)] // Temporary

/// Core functionality for reading Avro data into Arrow arrays
///
/// Implements the primary reader interface and record decoding logic.
pub mod reader;

/// Core functionality for writing Arrow arrays as Avro data
///
/// Implements the primary writer interface and record encoding logic.
pub mod writer;

/// Avro schema parsing and representation
///
/// Provides types for parsing and representing Avro schema definitions.
pub mod schema;

/// Compression codec implementations for Avro
///
/// Provides support for various compression algorithms used in Avro files,
/// including Deflate, Snappy, and ZStandard.
pub mod compression;

/// Data type conversions between Avro and Arrow types
///
/// This module contains the necessary types and functions to convert between
/// Avro data types and Arrow data types.
pub mod codec;

/// Extension trait for AvroField to add Utf8View support
///
/// This trait adds methods for working with Utf8View support to the AvroField struct.
pub trait AvroFieldExt {
    /// Returns a new field with Utf8View support enabled for string data
    ///
    /// This will convert any string data to use StringViewArray instead of StringArray.
    fn with_utf8view(&self) -> Self;
}

impl AvroFieldExt for codec::AvroField {
    fn with_utf8view(&self) -> Self {
        codec::AvroField::with_utf8view(self)
    }
}

#[cfg(test)]
mod test_util {
    pub fn arrow_test_data(path: &str) -> String {
        match std::env::var("ARROW_TEST_DATA") {
            Ok(dir) => format!("{dir}/{path}"),
            Err(_) => format!("../testing/data/{path}"),
        }
    }
}
