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

//! Avro writer implementation for the `arrow-avro` crate.
//!
//! # Overview
//!
//! Use this module to serialize Arrow `RecordBatch` values into Avro. Two output
//! formats are supported:
//!
//! * **[`AvroWriter`](crate::writer::AvroWriter)** — writes an **Object Container File (OCF)**: a self‑describing
//!   file with header (schema JSON + metadata), optional compression, data blocks, and
//!   sync markers. See Avro 1.11.1 “Object Container Files.”
//!   <https://avro.apache.org/docs/1.11.1/specification/#object-container-files>
//! * **[`AvroStreamWriter`](crate::writer::AvroStreamWriter)** — writes a **raw Avro binary stream** (“datum” bytes) without
//!   any container framing. This is useful when the schema is known out‑of‑band (i.e.,
//!   via a registry) and you want minimal overhead.
//!
//! ## Which format should I use?
//!
//! * Use **OCF** when you need a portable, self‑contained file. The schema travels with
//!   the data, making it easy to read elsewhere.
//! * Use the **raw stream** when your surrounding protocol supplies schema information
//!   (i.e., a schema registry). If you need **single‑object encoding (SOE)** or Confluent
//!   **Schema Registry** framing, you must add the appropriate prefix *outside* this writer:
//!   - **SOE**: `0xC3 0x01` + 8‑byte little‑endian CRC‑64‑AVRO fingerprint + Avro body
//!     (see Avro 1.11.1 “Single object encoding”).
//!     <https://avro.apache.org/docs/1.11.1/specification/#single-object-encoding>
//!   - **Confluent wire format**: magic `0x00` + **big‑endian** 4‑byte schema ID and Avro body.
//!     <https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format>
//!
//! ## Quickstart: Write an OCF in memory (runnable)
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
//! // Writer schema: { id: long, name: string }
//! let writer_schema = Schema::new(vec![
//!     Field::new("id", DataType::Int64, false),
//!     Field::new("name", DataType::Utf8, false),
//! ]);
//! let batch = RecordBatch::try_new(
//!     Arc::new(writer_schema.clone()),
//!     vec![
//!         Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
//!         Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
//!     ],
//! )?;
//!
//! // Write an Avro **Object Container File** (OCF) to memory
//! let mut w = AvroWriter::new(Vec::<u8>::new(), writer_schema.clone())?;
//! w.write(&batch)?;
//! w.finish()?;
//! let bytes = w.into_inner();
//!
//! // Build a Reader with the explicit reader schema
//! let mut r = ReaderBuilder::new()
//!     .build(Cursor::new(bytes))?;
//!
//! // Decode one batch and assert row count
//! let out = r.next().unwrap()?;
//! assert_eq!(out.num_rows(), 2);
//! # Ok(()) }
//! ```
//!
//! ## Quickstart: Write a raw Avro binary stream (runnable)
//!
//! This writes only the **Avro body** bytes - no OCF header/sync, no SOE/confluent prefix.
//! If you plan to interoperate with a schema registry, add the appropriate prefix yourself
//! (see links above).
//!
//! ```
//! use std::sync::Arc;
//! use arrow_array::{ArrayRef, Int64Array, RecordBatch};
//! use arrow_schema::{DataType, Field, Schema};
//! use arrow_avro::writer::AvroStreamWriter;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // One‑column Arrow batch
//! let schema = Schema::new(vec![Field::new("x", DataType::Int64, false)]);
//! let batch = RecordBatch::try_new(
//!     Arc::new(schema.clone()),
//!     vec![Arc::new(Int64Array::from(vec![10, 20])) as ArrayRef],
//! )?;
//!
//! // Write a **raw** Avro stream: just Avro binary datum bytes
//! let sink: Vec<u8> = Vec::new();
//! let mut w = AvroStreamWriter::new(sink, schema.clone())?;
//! w.write(&batch)?;
//! w.finish()?;
//! let body = w.into_inner();
//! assert!(!body.is_empty());
//! # Ok(()) }
//! ```
//!
//! ## Choosing the Avro schema
//!
//! By default, the writer converts your Arrow schema to Avro (including a top‑level record
//! name) and stores the resulting JSON under the `avro::schema` metadata key. If you already
//! have an Avro schema JSON, you want to use verbatim, put it into the Arrow schema metadata
//! under the same key before constructing the writer. The builder will pick it up.
//!
//! ## Compression
//!
//! For OCF, you may enable a compression codec via `WriterBuilder::with_compression`. The
//! chosen codec is written into the file header and used for subsequent blocks. Raw stream
//! writing doesn’t apply container‑level compression.
//!
//! ---
use crate::codec::AvroFieldBuilder;
use crate::compression::CompressionCodec;
use crate::schema::{
    AvroSchema, Fingerprint, FingerprintAlgorithm, FingerprintStrategy, SCHEMA_METADATA_KEY,
};
use crate::writer::encoder::{write_long, RecordEncoder, RecordEncoderBuilder};
use crate::writer::format::{AvroBinaryFormat, AvroFormat, AvroOcfFormat};
use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, Schema};
use std::io::Write;
use std::sync::Arc;

/// Encodes `RecordBatch` into the Avro binary format.
pub mod encoder;
/// Logic for different Avro container file formats.
pub mod format;

/// Builder to configure and create a `Writer`.
#[derive(Debug, Clone)]
pub struct WriterBuilder {
    schema: Schema,
    codec: Option<CompressionCodec>,
    capacity: usize,
    fingerprint_strategy: Option<FingerprintStrategy>,
}

impl WriterBuilder {
    /// Create a new builder with default settings.
    ///
    /// The Avro schema used for writing is determined as follows:
    /// 1) If the Arrow schema metadata contains `avro::schema` (see `SCHEMA_METADATA_KEY`),
    ///    that JSON is used verbatim.
    /// 2) Otherwise, the Arrow schema is converted to an Avro record schema.
    pub fn new(schema: Schema) -> Self {
        Self {
            schema,
            codec: None,
            capacity: 1024,
            fingerprint_strategy: None,
        }
    }

    /// Set the fingerprinting strategy for the stream writer.
    /// This determines the per-record prefix format.
    pub fn with_fingerprint_strategy(mut self, strategy: FingerprintStrategy) -> Self {
        self.fingerprint_strategy = Some(strategy);
        self
    }

    /// Change the compression codec.
    pub fn with_compression(mut self, codec: Option<CompressionCodec>) -> Self {
        self.codec = codec;
        self
    }

    /// Sets the capacity for the given object and returns the modified instance.
    pub fn with_capacity(mut self, capacity: usize) -> Self {
        self.capacity = capacity;
        self
    }

    /// Create a new `Writer` with specified `AvroFormat` and builder options.
    /// Performs one‑time startup (header/stream init, encoder plan).
    pub fn build<W, F>(self, mut writer: W) -> Result<Writer<W, F>, ArrowError>
    where
        W: Write,
        F: AvroFormat,
    {
        let mut format = F::default();
        let avro_schema = match self.schema.metadata.get(SCHEMA_METADATA_KEY) {
            Some(json) => AvroSchema::new(json.clone()),
            None => AvroSchema::try_from(&self.schema)?,
        };

        let maybe_fingerprint = if F::NEEDS_PREFIX {
            match self.fingerprint_strategy {
                Some(FingerprintStrategy::Id(id)) => Some(Fingerprint::Id(id)),
                Some(strategy) => {
                    Some(avro_schema.fingerprint(FingerprintAlgorithm::from(strategy))?)
                }
                None => Some(
                    avro_schema
                        .fingerprint(FingerprintAlgorithm::from(FingerprintStrategy::Rabin))?,
                ),
            }
        } else {
            None
        };

        let mut md = self.schema.metadata().clone();
        md.insert(
            SCHEMA_METADATA_KEY.to_string(),
            avro_schema.clone().json_string,
        );
        let schema = Arc::new(Schema::new_with_metadata(self.schema.fields().clone(), md));
        format.start_stream(&mut writer, &schema, self.codec)?;
        let avro_root = AvroFieldBuilder::new(&avro_schema.schema()?).build()?;
        let encoder = RecordEncoderBuilder::new(&avro_root, schema.as_ref())
            .with_fingerprint(maybe_fingerprint)
            .build()?;
        Ok(Writer {
            writer,
            schema,
            format,
            compression: self.codec,
            capacity: self.capacity,
            encoder,
        })
    }
}

/// Generic Avro writer.
///
/// This type is generic over the output Write sink (`W`) and the Avro format (`F`).
/// You’ll usually use the concrete aliases:
///
/// * **[`AvroWriter`]** for **OCF** (self‑describing container file)
/// * **[`AvroStreamWriter`]** for **raw** Avro binary streams
#[derive(Debug)]
pub struct Writer<W: Write, F: AvroFormat> {
    writer: W,
    schema: Arc<Schema>,
    format: F,
    compression: Option<CompressionCodec>,
    capacity: usize,
    encoder: RecordEncoder,
}

/// Alias for an Avro **Object Container File** writer.
pub type AvroWriter<W> = Writer<W, AvroOcfFormat>;
/// Alias for a raw Avro **binary stream** writer.
pub type AvroStreamWriter<W> = Writer<W, AvroBinaryFormat>;

impl<W: Write> Writer<W, AvroOcfFormat> {
    /// Convenience constructor – same as [`WriterBuilder::build`] with `AvroOcfFormat`.
    ///
    /// ### Example
    ///
    /// ```
    /// use std::sync::Arc;
    /// use arrow_array::{ArrayRef, Int32Array, RecordBatch};
    /// use arrow_schema::{DataType, Field, Schema};
    /// use arrow_avro::writer::AvroWriter;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
    /// let batch = RecordBatch::try_new(
    ///     Arc::new(schema.clone()),
    ///     vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef],
    /// )?;
    ///
    /// let buf: Vec<u8> = Vec::new();
    /// let mut w = AvroWriter::new(buf, schema)?;
    /// w.write(&batch)?;
    /// w.finish()?;
    /// let bytes = w.into_inner();
    /// assert!(!bytes.is_empty());
    /// # Ok(()) }
    /// ```
    pub fn new(writer: W, schema: Schema) -> Result<Self, ArrowError> {
        WriterBuilder::new(schema).build::<W, AvroOcfFormat>(writer)
    }

    /// Return a reference to the 16‑byte sync marker generated for this file.
    pub fn sync_marker(&self) -> Option<&[u8; 16]> {
        self.format.sync_marker()
    }
}

impl<W: Write> Writer<W, AvroBinaryFormat> {
    /// Convenience constructor to create a new [`AvroStreamWriter`].
    ///
    /// The resulting stream contains just **Avro binary** bodies (no OCF header/sync and no
    /// single‑object or Confluent framing). If you need those frames, add them externally.
    ///
    /// ### Example
    ///
    /// ```
    /// use std::sync::Arc;
    /// use arrow_array::{ArrayRef, Int64Array, RecordBatch};
    /// use arrow_schema::{DataType, Field, Schema};
    /// use arrow_avro::writer::AvroStreamWriter;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let schema = Schema::new(vec![Field::new("x", DataType::Int64, false)]);
    /// let batch = RecordBatch::try_new(
    ///     Arc::new(schema.clone()),
    ///     vec![Arc::new(Int64Array::from(vec![10, 20])) as ArrayRef],
    /// )?;
    ///
    /// let sink: Vec<u8> = Vec::new();
    /// let mut w = AvroStreamWriter::new(sink, schema)?;
    /// w.write(&batch)?;
    /// w.finish()?;
    /// let bytes = w.into_inner();
    /// assert!(!bytes.is_empty());
    /// # Ok(()) }
    /// ```
    pub fn new(writer: W, schema: Schema) -> Result<Self, ArrowError> {
        WriterBuilder::new(schema).build::<W, AvroBinaryFormat>(writer)
    }
}

impl<W: Write, F: AvroFormat> Writer<W, F> {
    /// Serialize one [`RecordBatch`] to the output.
    pub fn write(&mut self, batch: &RecordBatch) -> Result<(), ArrowError> {
        if batch.schema().fields() != self.schema.fields() {
            return Err(ArrowError::SchemaError(
                "Schema of RecordBatch differs from Writer schema".to_string(),
            ));
        }
        match self.format.sync_marker() {
            Some(&sync) => self.write_ocf_block(batch, &sync),
            None => self.write_stream(batch),
        }
    }

    /// A convenience method to write a slice of [`RecordBatch`].
    ///
    /// This is equivalent to calling `write` for each batch in the slice.
    pub fn write_batches(&mut self, batches: &[&RecordBatch]) -> Result<(), ArrowError> {
        for b in batches {
            self.write(b)?;
        }
        Ok(())
    }

    /// Flush remaining buffered data and (for OCF) ensure the header is present.
    pub fn finish(&mut self) -> Result<(), ArrowError> {
        self.writer
            .flush()
            .map_err(|e| ArrowError::IoError(format!("Error flushing writer: {e}"), e))
    }

    /// Consume the writer, returning the underlying output object.
    pub fn into_inner(self) -> W {
        self.writer
    }

    fn write_ocf_block(&mut self, batch: &RecordBatch, sync: &[u8; 16]) -> Result<(), ArrowError> {
        let mut buf = Vec::<u8>::with_capacity(1024);
        self.encoder.encode(&mut buf, batch)?;
        let encoded = match self.compression {
            Some(codec) => codec.compress(&buf)?,
            None => buf,
        };
        write_long(&mut self.writer, batch.num_rows() as i64)?;
        write_long(&mut self.writer, encoded.len() as i64)?;
        self.writer
            .write_all(&encoded)
            .map_err(|e| ArrowError::IoError(format!("Error writing Avro block: {e}"), e))?;
        self.writer
            .write_all(sync)
            .map_err(|e| ArrowError::IoError(format!("Error writing Avro sync: {e}"), e))?;
        Ok(())
    }

    fn write_stream(&mut self, batch: &RecordBatch) -> Result<(), ArrowError> {
        self.encoder.encode(&mut self.writer, batch)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compression::CompressionCodec;
    use crate::reader::ReaderBuilder;
    use crate::schema::{AvroSchema, SchemaStore, CONFLUENT_MAGIC};
    use crate::test_util::arrow_test_data;
    use arrow_array::{ArrayRef, BinaryArray, Int32Array, Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, IntervalUnit, Schema};
    use std::fs::File;
    use std::io::{BufReader, Cursor};
    use std::path::PathBuf;
    use std::sync::Arc;
    use tempfile::NamedTempFile;

    fn make_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Binary, false),
        ])
    }

    fn make_batch() -> RecordBatch {
        let ids = Int32Array::from(vec![1, 2, 3]);
        let names = BinaryArray::from_vec(vec![b"a".as_ref(), b"b".as_ref(), b"c".as_ref()]);
        RecordBatch::try_new(
            Arc::new(make_schema()),
            vec![Arc::new(ids) as ArrayRef, Arc::new(names) as ArrayRef],
        )
        .expect("failed to build test RecordBatch")
    }

    #[test]
    fn test_stream_writer_writes_prefix_per_row_rt() -> Result<(), ArrowError> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(Int32Array::from(vec![10, 20])) as ArrayRef],
        )?;
        let buf: Vec<u8> = Vec::new();
        let mut writer = AvroStreamWriter::new(buf, schema.clone())?;
        writer.write(&batch)?;
        let encoded = writer.into_inner();
        let mut store = SchemaStore::new(); // Rabin by default
        let avro_schema = AvroSchema::try_from(&schema)?;
        let _fp = store.register(avro_schema)?;
        let mut decoder = ReaderBuilder::new()
            .with_writer_schema_store(store)
            .build_decoder()?;
        let _consumed = decoder.decode(&encoded)?;
        let decoded = decoder
            .flush()?
            .expect("expected at least one batch from decoder");
        assert_eq!(decoded.num_columns(), 1);
        assert_eq!(decoded.num_rows(), 2);
        let col = decoded
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int column");
        assert_eq!(col, &Int32Array::from(vec![10, 20]));
        Ok(())
    }

    #[test]
    fn test_stream_writer_with_id_fingerprint_rt() -> Result<(), ArrowError> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef],
        )?;
        let schema_id: u32 = 42;
        let mut writer = WriterBuilder::new(schema.clone())
            .with_fingerprint_strategy(FingerprintStrategy::Id(schema_id))
            .build::<_, AvroBinaryFormat>(Vec::new())?;
        writer.write(&batch)?;
        let encoded = writer.into_inner();
        let mut store = SchemaStore::new_with_type(FingerprintAlgorithm::None);
        let avro_schema = AvroSchema::try_from(&schema)?;
        let _ = store.set(Fingerprint::Id(schema_id), avro_schema)?;
        let mut decoder = ReaderBuilder::new()
            .with_writer_schema_store(store)
            .build_decoder()?;
        let _ = decoder.decode(&encoded)?;
        let decoded = decoder
            .flush()?
            .expect("expected at least one batch from decoder");
        assert_eq!(decoded.num_columns(), 1);
        assert_eq!(decoded.num_rows(), 3);
        let col = decoded
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int column");
        assert_eq!(col, &Int32Array::from(vec![1, 2, 3]));
        Ok(())
    }

    #[test]
    fn test_ocf_writer_generates_header_and_sync() -> Result<(), ArrowError> {
        let batch = make_batch();
        let buffer: Vec<u8> = Vec::new();
        let mut writer = AvroWriter::new(buffer, make_schema())?;
        writer.write(&batch)?;
        writer.finish()?;
        let out = writer.into_inner();
        assert_eq!(&out[..4], b"Obj\x01", "OCF magic bytes missing/incorrect");
        let trailer = &out[out.len() - 16..];
        assert_eq!(trailer.len(), 16, "expected 16‑byte sync marker");
        Ok(())
    }

    #[test]
    fn test_schema_mismatch_yields_error() {
        let batch = make_batch();
        let alt_schema = Schema::new(vec![Field::new("x", DataType::Int32, false)]);
        let buffer = Vec::<u8>::new();
        let mut writer = AvroWriter::new(buffer, alt_schema).unwrap();
        let err = writer.write(&batch).unwrap_err();
        assert!(matches!(err, ArrowError::SchemaError(_)));
    }

    #[test]
    fn test_write_batches_accumulates_multiple() -> Result<(), ArrowError> {
        let batch1 = make_batch();
        let batch2 = make_batch();
        let buffer = Vec::<u8>::new();
        let mut writer = AvroWriter::new(buffer, make_schema())?;
        writer.write_batches(&[&batch1, &batch2])?;
        writer.finish()?;
        let out = writer.into_inner();
        assert!(out.len() > 4, "combined batches produced tiny file");
        Ok(())
    }

    #[test]
    fn test_finish_without_write_adds_header() -> Result<(), ArrowError> {
        let buffer = Vec::<u8>::new();
        let mut writer = AvroWriter::new(buffer, make_schema())?;
        writer.finish()?;
        let out = writer.into_inner();
        assert_eq!(&out[..4], b"Obj\x01", "finish() should emit OCF header");
        Ok(())
    }

    #[test]
    fn test_write_long_encodes_zigzag_varint() -> Result<(), ArrowError> {
        let mut buf = Vec::new();
        write_long(&mut buf, 0)?;
        write_long(&mut buf, -1)?;
        write_long(&mut buf, 1)?;
        write_long(&mut buf, -2)?;
        write_long(&mut buf, 2147483647)?;
        assert!(
            buf.starts_with(&[0x00, 0x01, 0x02, 0x03]),
            "zig‑zag varint encodings incorrect: {buf:?}"
        );
        Ok(())
    }

    #[test]
    fn test_roundtrip_alltypes_roundtrip_writer() -> Result<(), ArrowError> {
        let files = [
            "avro/alltypes_plain.avro",
            "avro/alltypes_plain.snappy.avro",
            "avro/alltypes_plain.zstandard.avro",
            "avro/alltypes_plain.bzip2.avro",
            "avro/alltypes_plain.xz.avro",
        ];
        for rel in files {
            let path = arrow_test_data(rel);
            let rdr_file = File::open(&path).expect("open input avro");
            let mut reader = ReaderBuilder::new()
                .build(BufReader::new(rdr_file))
                .expect("build reader");
            let schema = reader.schema();
            let input_batches = reader.collect::<Result<Vec<_>, _>>()?;
            let original =
                arrow::compute::concat_batches(&schema, &input_batches).expect("concat input");
            let tmp = NamedTempFile::new().expect("create temp file");
            let out_path = tmp.into_temp_path();
            let out_file = File::create(&out_path).expect("create temp avro");
            let codec = if rel.contains(".snappy.") {
                Some(CompressionCodec::Snappy)
            } else if rel.contains(".zstandard.") {
                Some(CompressionCodec::ZStandard)
            } else if rel.contains(".bzip2.") {
                Some(CompressionCodec::Bzip2)
            } else if rel.contains(".xz.") {
                Some(CompressionCodec::Xz)
            } else {
                None
            };
            let mut writer = WriterBuilder::new(original.schema().as_ref().clone())
                .with_compression(codec)
                .build::<_, AvroOcfFormat>(out_file)?;
            writer.write(&original)?;
            writer.finish()?;
            drop(writer);
            let rt_file = File::open(&out_path).expect("open roundtrip avro");
            let mut rt_reader = ReaderBuilder::new()
                .build(BufReader::new(rt_file))
                .expect("build roundtrip reader");
            let rt_schema = rt_reader.schema();
            let rt_batches = rt_reader.collect::<Result<Vec<_>, _>>()?;
            let roundtrip =
                arrow::compute::concat_batches(&rt_schema, &rt_batches).expect("concat roundtrip");
            assert_eq!(
                roundtrip, original,
                "Round-trip batch mismatch for file: {}",
                rel
            );
        }
        Ok(())
    }

    #[test]
    fn test_roundtrip_nested_records_writer() -> Result<(), ArrowError> {
        let path = arrow_test_data("avro/nested_records.avro");
        let rdr_file = File::open(&path).expect("open nested_records.avro");
        let mut reader = ReaderBuilder::new()
            .build(BufReader::new(rdr_file))
            .expect("build reader for nested_records.avro");
        let schema = reader.schema();
        let batches = reader.collect::<Result<Vec<_>, _>>()?;
        let original = arrow::compute::concat_batches(&schema, &batches).expect("concat original");
        let tmp = NamedTempFile::new().expect("create temp file");
        let out_path = tmp.into_temp_path();
        {
            let out_file = File::create(&out_path).expect("create output avro");
            let mut writer = AvroWriter::new(out_file, original.schema().as_ref().clone())?;
            writer.write(&original)?;
            writer.finish()?;
        }
        let rt_file = File::open(&out_path).expect("open round_trip avro");
        let mut rt_reader = ReaderBuilder::new()
            .build(BufReader::new(rt_file))
            .expect("build round_trip reader");
        let rt_schema = rt_reader.schema();
        let rt_batches = rt_reader.collect::<Result<Vec<_>, _>>()?;
        let round_trip =
            arrow::compute::concat_batches(&rt_schema, &rt_batches).expect("concat round_trip");
        assert_eq!(
            round_trip, original,
            "Round-trip batch mismatch for nested_records.avro"
        );
        Ok(())
    }

    #[test]
    fn test_roundtrip_nested_lists_writer() -> Result<(), ArrowError> {
        let path = arrow_test_data("avro/nested_lists.snappy.avro");
        let rdr_file = File::open(&path).expect("open nested_lists.snappy.avro");
        let mut reader = ReaderBuilder::new()
            .build(BufReader::new(rdr_file))
            .expect("build reader for nested_lists.snappy.avro");
        let schema = reader.schema();
        let batches = reader.collect::<Result<Vec<_>, _>>()?;
        let original = arrow::compute::concat_batches(&schema, &batches).expect("concat original");
        let tmp = NamedTempFile::new().expect("create temp file");
        let out_path = tmp.into_temp_path();
        {
            let out_file = File::create(&out_path).expect("create output avro");
            let mut writer = WriterBuilder::new(original.schema().as_ref().clone())
                .with_compression(Some(CompressionCodec::Snappy))
                .build::<_, AvroOcfFormat>(out_file)?;
            writer.write(&original)?;
            writer.finish()?;
        }
        let rt_file = File::open(&out_path).expect("open round_trip avro");
        let mut rt_reader = ReaderBuilder::new()
            .build(BufReader::new(rt_file))
            .expect("build round_trip reader");
        let rt_schema = rt_reader.schema();
        let rt_batches = rt_reader.collect::<Result<Vec<_>, _>>()?;
        let round_trip =
            arrow::compute::concat_batches(&rt_schema, &rt_batches).expect("concat round_trip");
        assert_eq!(
            round_trip, original,
            "Round-trip batch mismatch for nested_lists.snappy.avro"
        );
        Ok(())
    }

    #[test]
    fn test_round_trip_simple_fixed_ocf() -> Result<(), ArrowError> {
        let path = arrow_test_data("avro/simple_fixed.avro");
        let rdr_file = File::open(&path).expect("open avro/simple_fixed.avro");
        let mut reader = ReaderBuilder::new()
            .build(BufReader::new(rdr_file))
            .expect("build avro reader");
        let schema = reader.schema();
        let input_batches = reader.collect::<Result<Vec<_>, _>>()?;
        let original =
            arrow::compute::concat_batches(&schema, &input_batches).expect("concat input");
        let tmp = NamedTempFile::new().expect("create temp file");
        let out_file = File::create(tmp.path()).expect("create temp avro");
        let mut writer = AvroWriter::new(out_file, original.schema().as_ref().clone())?;
        writer.write(&original)?;
        writer.finish()?;
        drop(writer);
        let rt_file = File::open(tmp.path()).expect("open round_trip avro");
        let mut rt_reader = ReaderBuilder::new()
            .build(BufReader::new(rt_file))
            .expect("build round_trip reader");
        let rt_schema = rt_reader.schema();
        let rt_batches = rt_reader.collect::<Result<Vec<_>, _>>()?;
        let round_trip =
            arrow::compute::concat_batches(&rt_schema, &rt_batches).expect("concat round_trip");
        assert_eq!(round_trip, original);
        Ok(())
    }

    #[cfg(not(feature = "canonical_extension_types"))]
    #[test]
    fn test_round_trip_duration_and_uuid_ocf() -> Result<(), ArrowError> {
        let in_file =
            File::open("test/data/duration_uuid.avro").expect("open test/data/duration_uuid.avro");
        let mut reader = ReaderBuilder::new()
            .build(BufReader::new(in_file))
            .expect("build reader for duration_uuid.avro");
        let in_schema = reader.schema();
        let has_mdn = in_schema.fields().iter().any(|f| {
            matches!(
                f.data_type(),
                DataType::Interval(IntervalUnit::MonthDayNano)
            )
        });
        assert!(
            has_mdn,
            "expected at least one Interval(MonthDayNano) field in duration_uuid.avro"
        );
        let has_uuid_fixed = in_schema
            .fields()
            .iter()
            .any(|f| matches!(f.data_type(), DataType::FixedSizeBinary(16)));
        assert!(
            has_uuid_fixed,
            "expected at least one FixedSizeBinary(16) (uuid) field in duration_uuid.avro"
        );
        let input_batches = reader.collect::<Result<Vec<_>, _>>()?;
        let input =
            arrow::compute::concat_batches(&in_schema, &input_batches).expect("concat input");
        let tmp = NamedTempFile::new().expect("create temp file");
        {
            let out_file = File::create(tmp.path()).expect("create temp avro");
            let mut writer = AvroWriter::new(out_file, in_schema.as_ref().clone())?;
            writer.write(&input)?;
            writer.finish()?;
        }
        let rt_file = File::open(tmp.path()).expect("open round_trip avro");
        let mut rt_reader = ReaderBuilder::new()
            .build(BufReader::new(rt_file))
            .expect("build round_trip reader");
        let rt_schema = rt_reader.schema();
        let rt_batches = rt_reader.collect::<Result<Vec<_>, _>>()?;
        let round_trip =
            arrow::compute::concat_batches(&rt_schema, &rt_batches).expect("concat round_trip");
        assert_eq!(round_trip, input);
        Ok(())
    }

    // This test reads the same 'nonnullable.impala.avro' used by the reader tests,
    // writes it back out with the writer (hitting Map encoding paths), then reads it
    // again and asserts exact Arrow equivalence.
    #[test]
    fn test_nonnullable_impala_roundtrip_writer() -> Result<(), ArrowError> {
        // Load source Avro with Map fields
        let path = arrow_test_data("avro/nonnullable.impala.avro");
        let rdr_file = File::open(&path).expect("open avro/nonnullable.impala.avro");
        let mut reader = ReaderBuilder::new()
            .build(BufReader::new(rdr_file))
            .expect("build reader for nonnullable.impala.avro");
        // Collect all input batches and concatenate to a single RecordBatch
        let in_schema = reader.schema();
        // Sanity: ensure the file actually contains at least one Map field
        let has_map = in_schema
            .fields()
            .iter()
            .any(|f| matches!(f.data_type(), DataType::Map(_, _)));
        assert!(
            has_map,
            "expected at least one Map field in avro/nonnullable.impala.avro"
        );

        let input_batches = reader.collect::<Result<Vec<_>, _>>()?;
        let original =
            arrow::compute::concat_batches(&in_schema, &input_batches).expect("concat input");
        // Write out using the OCF writer into an in-memory Vec<u8>
        let buffer = Vec::<u8>::new();
        let mut writer = AvroWriter::new(buffer, in_schema.as_ref().clone())?;
        writer.write(&original)?;
        writer.finish()?;
        let out_bytes = writer.into_inner();
        // Read the produced bytes back with the Reader
        let mut rt_reader = ReaderBuilder::new()
            .build(Cursor::new(out_bytes))
            .expect("build reader for round-tripped in-memory OCF");
        let rt_schema = rt_reader.schema();
        let rt_batches = rt_reader.collect::<Result<Vec<_>, _>>()?;
        let roundtrip =
            arrow::compute::concat_batches(&rt_schema, &rt_batches).expect("concat roundtrip");
        // Exact value fidelity (schema + data)
        assert_eq!(
            roundtrip, original,
            "Round-trip Avro map data mismatch for nonnullable.impala.avro"
        );
        Ok(())
    }

    #[test]
    fn test_roundtrip_decimals_via_writer() -> Result<(), ArrowError> {
        // (file, resolve via ARROW_TEST_DATA?)
        let files: [(&str, bool); 8] = [
            ("avro/fixed_length_decimal.avro", true), // fixed-backed -> Decimal128(25,2)
            ("avro/fixed_length_decimal_legacy.avro", true), // legacy fixed[8] -> Decimal64(13,2)
            ("avro/int32_decimal.avro", true),        // bytes-backed -> Decimal32(4,2)
            ("avro/int64_decimal.avro", true),        // bytes-backed -> Decimal64(10,2)
            ("test/data/int256_decimal.avro", false), // bytes-backed -> Decimal256(76,2)
            ("test/data/fixed256_decimal.avro", false), // fixed[32]-backed -> Decimal256(76,10)
            ("test/data/fixed_length_decimal_legacy_32.avro", false), // legacy fixed[4] -> Decimal32(9,2)
            ("test/data/int128_decimal.avro", false), // bytes-backed -> Decimal128(38,2)
        ];
        for (rel, in_test_data_dir) in files {
            // Resolve path the same way as reader::test_decimal
            let path: String = if in_test_data_dir {
                arrow_test_data(rel)
            } else {
                PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join(rel)
                    .to_string_lossy()
                    .into_owned()
            };
            // Read original file into a single RecordBatch for comparison
            let f_in = File::open(&path).expect("open input avro");
            let mut rdr = ReaderBuilder::new().build(BufReader::new(f_in))?;
            let in_schema = rdr.schema();
            let in_batches = rdr.collect::<Result<Vec<_>, _>>()?;
            let original =
                arrow::compute::concat_batches(&in_schema, &in_batches).expect("concat input");
            // Write it out with the OCF writer (no special compression)
            let tmp = NamedTempFile::new().expect("create temp file");
            let out_path = tmp.into_temp_path();
            let out_file = File::create(&out_path).expect("create temp avro");
            let mut writer = AvroWriter::new(out_file, original.schema().as_ref().clone())?;
            writer.write(&original)?;
            writer.finish()?;
            // Read back the file we just wrote and compare equality (schema + data)
            let f_rt = File::open(&out_path).expect("open roundtrip avro");
            let mut rt_rdr = ReaderBuilder::new().build(BufReader::new(f_rt))?;
            let rt_schema = rt_rdr.schema();
            let rt_batches = rt_rdr.collect::<Result<Vec<_>, _>>()?;
            let roundtrip =
                arrow::compute::concat_batches(&rt_schema, &rt_batches).expect("concat rt");
            assert_eq!(roundtrip, original, "decimal round-trip mismatch for {rel}");
        }
        Ok(())
    }

    #[test]
    fn test_enum_roundtrip_uses_reader_fixture() -> Result<(), ArrowError> {
        // Read the known-good enum file (same as reader::test_simple)
        let path = arrow_test_data("avro/simple_enum.avro");
        let rdr_file = File::open(&path).expect("open avro/simple_enum.avro");
        let mut reader = ReaderBuilder::new()
            .build(BufReader::new(rdr_file))
            .expect("build reader for simple_enum.avro");
        // Concatenate all batches to one RecordBatch for a clean equality check
        let in_schema = reader.schema();
        let input_batches = reader.collect::<Result<Vec<_>, _>>()?;
        let original =
            arrow::compute::concat_batches(&in_schema, &input_batches).expect("concat input");
        // Sanity: expect at least one Dictionary(Int32, Utf8) column (enum)
        let has_enum_dict = in_schema.fields().iter().any(|f| {
            matches!(
                f.data_type(),
                DataType::Dictionary(k, v) if **k == DataType::Int32 && **v == DataType::Utf8
            )
        });
        assert!(
            has_enum_dict,
            "Expected at least one enum-mapped Dictionary<Int32, Utf8> field"
        );
        // Write with OCF writer into memory using the reader-provided Arrow schema.
        // The writer will embed the Avro JSON from `avro.schema` metadata if present.
        let buffer: Vec<u8> = Vec::new();
        let mut writer = AvroWriter::new(buffer, in_schema.as_ref().clone())?;
        writer.write(&original)?;
        writer.finish()?;
        let bytes = writer.into_inner();
        // Read back and compare for exact equality (schema + data)
        let mut rt_reader = ReaderBuilder::new()
            .build(Cursor::new(bytes))
            .expect("reader for round-trip");
        let rt_schema = rt_reader.schema();
        let rt_batches = rt_reader.collect::<Result<Vec<_>, _>>()?;
        let roundtrip =
            arrow::compute::concat_batches(&rt_schema, &rt_batches).expect("concat roundtrip");
        assert_eq!(roundtrip, original, "Avro enum round-trip mismatch");
        Ok(())
    }
}
