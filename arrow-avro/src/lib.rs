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
//! - and a [`writer`] that encodes Arrow `RecordBatch`es into Avro (OCF or SOE).
//!
//! If you’re new to Arrow or Avro, see:
//! - Arrow project site: <https://arrow.apache.org/>
//! - Avro 1.11.1 specification: <https://avro.apache.org/docs/1.11.1/specification/>
//!
//! ## Example: OCF (Object Container File) round‑trip *(runnable)*
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
//! ## Quickstart: SOE (Single‑Object Encoding) round‑trip *(runnable)*
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
//! - [`writer`]: write Arrow `RecordBatch`es as Avro (OCF, SOE, Confluent, Apicurio).
//! - [`schema`]: Avro schema parsing / fingerprints / registries.
//! - [`compression`]: codecs used for **OCF block compression** (i.e., Deflate, Snappy, Zstandard, BZip2, and XZ).
//! - [`codec`]: internal Avro-Arrow type conversion and row decode/encode plans.
//!
//! ### Features
//!
//! **OCF compression (enabled by default)**
//! - `deflate` — enable DEFLATE block compression (via `flate2`).
//! - `snappy` — enable Snappy block compression with 4‑byte BE CRC32 (per Avro).
//! - `zstd` — enable Zstandard block compression.
//! - `bzip2` — enable BZip2 block compression.
//! - `xz` — enable XZ/LZMA block compression.
//!
//! **Schema fingerprints & helpers (opt‑in)**
//! - `md5` — enable MD5 writer‑schema fingerprints.
//! - `sha256` — enable SHA‑256 writer‑schema fingerprints.
//! - `small_decimals` — support for compact Arrow representations of small Avro decimals (`Decimal32` and `Decimal64`).
//! - `avro_custom_types` — interpret Avro fields annotated with Arrow‑specific logical
//!   types such as `arrow.duration-nanos`, `arrow.duration-micros`,
//!   `arrow.duration-millis`, or `arrow.duration-seconds` as Arrow `Duration(TimeUnit)`.
//! - `canonical_extension_types` — enable support for Arrow [canonical extension types]
//!   from `arrow-schema` so `arrow-avro` can respect them during Avro↔Arrow mapping.
//!
//! **Notes**
//! - OCF compression codecs apply only to **Object Container Files**; they do not affect Avro
//!   single object encodings.
//!
//! [canonical extension types]: https://arrow.apache.org/docs/format/CanonicalExtensions.html
//!
//! [Apache Arrow]: https://arrow.apache.org/
//! [Apache Avro]: https://avro.apache.org/

#![doc(
    html_logo_url = "https://arrow.apache.org/img/arrow-logo_chevrons_black-txt_white-bg.svg",
    html_favicon_url = "https://arrow.apache.org/img/arrow-logo_chevrons_black-txt_transparent-bg.svg"
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_docs)]

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
