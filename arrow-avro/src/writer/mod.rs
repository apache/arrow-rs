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
//! *   Use **`AvroWriter`** (Object Container File) when you want a
//!     self‑contained Avro file with header, schema JSON, optional compression,
//!     blocks, and sync markers.
//! *   Use **`AvroStreamWriter`** (raw binary stream) when you already know the
//!     schema out‑of‑band (i.e., via a schema registry) and need a stream
//!     of Avro‑encoded records with minimal framing.
//!

/// Encodes `RecordBatch` into the Avro binary format.
pub mod encoder;
/// Logic for different Avro container file formats.
pub mod format;

use crate::codec::AvroFieldBuilder;
use crate::compression::CompressionCodec;
use crate::schema::{AvroSchema, SCHEMA_METADATA_KEY};
use crate::writer::encoder::{write_long, RecordEncoder, RecordEncoderBuilder};
use crate::writer::format::{AvroBinaryFormat, AvroFormat, AvroOcfFormat};
use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, Schema};
use std::io::Write;
use std::sync::Arc;

/// Builder to configure and create a `Writer`.
#[derive(Debug, Clone)]
pub struct WriterBuilder {
    schema: Schema,
    codec: Option<CompressionCodec>,
    capacity: usize,
}

impl WriterBuilder {
    /// Create a new builder with default settings.
    pub fn new(schema: Schema) -> Self {
        Self {
            schema,
            codec: None,
            capacity: 1024,
        }
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
        let mut md = self.schema.metadata().clone();
        md.insert(
            SCHEMA_METADATA_KEY.to_string(),
            avro_schema.clone().json_string,
        );
        let schema = Arc::new(Schema::new_with_metadata(self.schema.fields().clone(), md));
        format.start_stream(&mut writer, &schema, self.codec)?;
        let avro_root = AvroFieldBuilder::new(&avro_schema.schema()?).build()?;
        let encoder = RecordEncoderBuilder::new(&avro_root, schema.as_ref()).build()?;
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
        self.encoder.encode(batch, &mut buf)?;
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
        self.encoder.encode(batch, &mut self.writer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compression::CompressionCodec;
    use crate::reader::ReaderBuilder;
    use crate::schema::{AvroSchema, SchemaStore};
    use crate::test_util::arrow_test_data;
    use arrow_array::{ArrayRef, BinaryArray, Int32Array, RecordBatch};
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
}
