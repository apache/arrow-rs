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
//! * **[`AvroStreamWriter`](crate::writer::AvroStreamWriter)** — writes a **Single Object Encoding (SOE) Stream** (“datum” bytes) without
//!   any container framing. This is useful when the schema is known out‑of‑band (i.e.,
//!   via a registry) and you want minimal overhead.
//!
//! ## Which format should you use?
//!
//! * Use **OCF** when you need a portable, self‑contained file. The schema travels with
//!   the data, making it easy to read elsewhere.
//! * Use the **SOE stream** when your surrounding protocol supplies schema information
//!   (i.e., a schema registry). The writer automatically adds the per‑record prefix:
//!   - **SOE**: Each record is prefixed with the 2-byte header (`0xC3 0x01`) followed by
//!     an 8‑byte little‑endian CRC‑64‑AVRO fingerprint, then the Avro body.
//!     See Avro 1.11.1 "Single object encoding".
//!     <https://avro.apache.org/docs/1.11.1/specification/#single-object-encoding>
//!   - **Confluent wire format**: Each record is prefixed with magic byte `0x00` followed by
//!     a **big‑endian** 4‑byte schema ID, then the Avro body. Use `FingerprintStrategy::Id(schema_id)`.
//!     <https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format>
//!   - **Apicurio wire format**: Each record is prefixed with magic byte `0x00` followed by
//!     a **big‑endian** 8‑byte schema ID, then the Avro body. Use `FingerprintStrategy::Id64(schema_id)`.
//!     <https://www.apicur.io/registry/docs/apicurio-registry/1.3.3.Final/getting-started/assembly-using-kafka-client-serdes.html#registry-serdes-types-avro-registry>
//!
//! ## Choosing the Avro schema
//!
//! By default, the writer converts your Arrow schema to Avro (including a top‑level record
//! name). If you already have an Avro schema JSON you want to use verbatim, put it into the
//! Arrow schema metadata under the `avro.schema` key before constructing the writer. The
//! builder will use that schema instead of generating a new one (unless `strip_metadata` is
//! set to true in the options).
//!
//! ## Compression
//!
//! For OCF, you may enable a compression codec via `WriterBuilder::with_compression`. The
//! chosen codec is written into the file header and used for subsequent blocks. SOE stream
//! writing doesn’t apply container‑level compression.
//!
//! ---
use crate::codec::AvroFieldBuilder;
use crate::compression::CompressionCodec;
use crate::schema::{
    AvroSchema, Fingerprint, FingerprintAlgorithm, FingerprintStrategy, SCHEMA_METADATA_KEY,
};
use crate::writer::encoder::{RecordEncoder, RecordEncoderBuilder, write_long};
use crate::writer::format::{AvroFormat, AvroOcfFormat, AvroSoeFormat};
use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, Schema};
use std::io::Write;
use std::sync::Arc;

/// Encodes `RecordBatch` into the Avro binary format.
mod encoder;
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
                Some(FingerprintStrategy::Id64(id)) => Some(Fingerprint::Id64(id)),
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
/// * **[`AvroStreamWriter`]** for **SOE** Avro streams
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
///
/// ### Quickstart (runnable)
///
/// ```
/// use std::io::Cursor;
/// use std::sync::Arc;
/// use arrow_array::{ArrayRef, Int64Array, StringArray, RecordBatch};
/// use arrow_schema::{DataType, Field, Schema};
/// use arrow_avro::writer::AvroWriter;
/// use arrow_avro::reader::ReaderBuilder;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// // Writer schema: { id: long, name: string }
/// let writer_schema = Schema::new(vec![
///     Field::new("id", DataType::Int64, false),
///     Field::new("name", DataType::Utf8, false),
/// ]);
///
/// // Build a RecordBatch with two rows
/// let batch = RecordBatch::try_new(
///     Arc::new(writer_schema.clone()),
///     vec![
///         Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
///         Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
///     ],
/// )?;
///
/// // Write an Avro **Object Container File** (OCF) to memory
/// let mut w = AvroWriter::new(Vec::<u8>::new(), writer_schema.clone())?;
/// w.write(&batch)?;
/// w.finish()?;
/// let bytes = w.into_inner();
///
/// // Build a Reader and decode the batch back
/// let mut r = ReaderBuilder::new().build(Cursor::new(bytes))?;
/// let out = r.next().unwrap()?;
/// assert_eq!(out.num_rows(), 2);
/// # Ok(()) }
/// ```
pub type AvroWriter<W> = Writer<W, AvroOcfFormat>;

/// Alias for an Avro **Single Object Encoding** stream writer.
///
/// ### Example
///
/// This writer automatically adds the appropriate per-record prefix (based on the
/// fingerprint strategy) before the Avro body of each record. The default is Single
/// Object Encoding (SOE) with a Rabin fingerprint.
///
/// ```
/// use std::sync::Arc;
/// use arrow_array::{ArrayRef, Int64Array, RecordBatch};
/// use arrow_schema::{DataType, Field, Schema};
/// use arrow_avro::writer::AvroStreamWriter;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// // One‑column Arrow batch
/// let schema = Schema::new(vec![Field::new("x", DataType::Int64, false)]);
/// let batch = RecordBatch::try_new(
///     Arc::new(schema.clone()),
///     vec![Arc::new(Int64Array::from(vec![10, 20])) as ArrayRef],
/// )?;
///
/// // Write an Avro Single Object Encoding stream to a Vec<u8>
/// let sink: Vec<u8> = Vec::new();
/// let mut w = AvroStreamWriter::new(sink, schema)?;
/// w.write(&batch)?;
/// w.finish()?;
/// let bytes = w.into_inner();
/// assert!(!bytes.is_empty());
/// # Ok(()) }
/// ```
pub type AvroStreamWriter<W> = Writer<W, AvroSoeFormat>;

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

impl<W: Write> Writer<W, AvroSoeFormat> {
    /// Convenience constructor to create a new [`AvroStreamWriter`].
    ///
    /// The resulting stream contains **Single Object Encodings** (no OCF header/sync).
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
        WriterBuilder::new(schema).build::<W, AvroSoeFormat>(writer)
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
        let mut buf = Vec::<u8>::with_capacity(self.capacity);
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
    use crate::schema::{AvroSchema, SchemaStore};
    use crate::test_util::arrow_test_data;
    use arrow::datatypes::TimeUnit;
    #[cfg(feature = "avro_custom_types")]
    use arrow_array::types::{Int16Type, Int32Type, Int64Type};
    use arrow_array::types::{
        Time32MillisecondType, Time64MicrosecondType, TimestampMicrosecondType,
        TimestampMillisecondType, TimestampNanosecondType,
    };
    use arrow_array::{
        Array, ArrayRef, BinaryArray, Date32Array, Int32Array, PrimitiveArray, RecordBatch,
        StringArray, StructArray, UnionArray,
    };
    #[cfg(feature = "avro_custom_types")]
    use arrow_array::{Int16Array, Int64Array, RunArray};
    use arrow_schema::UnionMode;
    #[cfg(not(feature = "avro_custom_types"))]
    use arrow_schema::{DataType, Field, Schema};
    #[cfg(feature = "avro_custom_types")]
    use arrow_schema::{DataType, Field, Schema};
    use std::collections::HashMap;
    use std::collections::HashSet;
    use std::fs::File;
    use std::io::{BufReader, Cursor};
    use std::path::PathBuf;
    use std::sync::Arc;
    use tempfile::NamedTempFile;

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
    fn test_nullable_struct_with_nonnullable_field_sliced_encoding() {
        use arrow_array::{ArrayRef, Int32Array, StringArray, StructArray};
        use arrow_buffer::NullBuffer;
        use arrow_schema::{DataType, Field, Fields, Schema};
        use std::sync::Arc;
        let inner_fields = Fields::from(vec![
            Field::new("id", DataType::Int32, false), // non-nullable
            Field::new("name", DataType::Utf8, true), // nullable
        ]);
        let inner_struct_type = DataType::Struct(inner_fields.clone());
        let schema = Schema::new(vec![
            Field::new("before", inner_struct_type.clone(), true), // nullable struct
            Field::new("after", inner_struct_type.clone(), true),  // nullable struct
            Field::new("op", DataType::Utf8, false),               // non-nullable
        ]);
        let before_ids = Int32Array::from(vec![None, None]);
        let before_names = StringArray::from(vec![None::<&str>, None]);
        let before_struct = StructArray::new(
            inner_fields.clone(),
            vec![
                Arc::new(before_ids) as ArrayRef,
                Arc::new(before_names) as ArrayRef,
            ],
            Some(NullBuffer::from(vec![false, false])),
        );
        let after_ids = Int32Array::from(vec![1, 2]); // non-nullable, no nulls
        let after_names = StringArray::from(vec![Some("Alice"), Some("Bob")]);
        let after_struct = StructArray::new(
            inner_fields.clone(),
            vec![
                Arc::new(after_ids) as ArrayRef,
                Arc::new(after_names) as ArrayRef,
            ],
            Some(NullBuffer::from(vec![true, true])),
        );
        let op_col = StringArray::from(vec!["r", "r"]);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(before_struct) as ArrayRef,
                Arc::new(after_struct) as ArrayRef,
                Arc::new(op_col) as ArrayRef,
            ],
        )
        .expect("failed to create test batch");
        let mut sink = Vec::new();
        let mut writer = WriterBuilder::new(schema)
            .with_fingerprint_strategy(FingerprintStrategy::Id(1))
            .build::<_, AvroSoeFormat>(&mut sink)
            .expect("failed to create writer");
        for row_idx in 0..batch.num_rows() {
            let single_row = batch.slice(row_idx, 1);
            let after_col = single_row.column(1);
            assert_eq!(
                after_col.null_count(),
                0,
                "after column should have no nulls in sliced row"
            );
            writer
                .write(&single_row)
                .unwrap_or_else(|e| panic!("Failed to encode row {row_idx}: {e}"));
        }
        writer.finish().expect("failed to finish writer");
        assert!(!sink.is_empty(), "encoded output should not be empty");
    }

    #[test]
    fn test_nullable_struct_with_decimal_and_timestamp_sliced() {
        use arrow_array::{
            ArrayRef, Decimal128Array, Int32Array, StringArray, StructArray,
            TimestampMicrosecondArray,
        };
        use arrow_buffer::NullBuffer;
        use arrow_schema::{DataType, Field, Fields, Schema};
        use std::sync::Arc;
        let row_fields = Fields::from(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("category", DataType::Utf8, true),
            Field::new("price", DataType::Decimal128(10, 2), true),
            Field::new("stock_quantity", DataType::Int32, true),
            Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
        ]);
        let row_struct_type = DataType::Struct(row_fields.clone());
        let schema = Schema::new(vec![
            Field::new("before", row_struct_type.clone(), true),
            Field::new("after", row_struct_type.clone(), true),
            Field::new("op", DataType::Utf8, false),
        ]);
        let before_struct = StructArray::new_null(row_fields.clone(), 2);
        let ids = Int32Array::from(vec![1, 2]);
        let names = StringArray::from(vec![Some("Widget"), Some("Gadget")]);
        let categories = StringArray::from(vec![Some("Electronics"), Some("Electronics")]);
        let prices = Decimal128Array::from(vec![Some(1999), Some(2999)])
            .with_precision_and_scale(10, 2)
            .unwrap();
        let quantities = Int32Array::from(vec![Some(100), Some(50)]);
        let timestamps = TimestampMicrosecondArray::from(vec![
            Some(1700000000000000i64),
            Some(1700000001000000i64),
        ]);
        let after_struct = StructArray::new(
            row_fields.clone(),
            vec![
                Arc::new(ids) as ArrayRef,
                Arc::new(names) as ArrayRef,
                Arc::new(categories) as ArrayRef,
                Arc::new(prices) as ArrayRef,
                Arc::new(quantities) as ArrayRef,
                Arc::new(timestamps) as ArrayRef,
            ],
            Some(NullBuffer::from(vec![true, true])),
        );
        let op_col = StringArray::from(vec!["r", "r"]);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(before_struct) as ArrayRef,
                Arc::new(after_struct) as ArrayRef,
                Arc::new(op_col) as ArrayRef,
            ],
        )
        .expect("failed to create products batch");
        let mut sink = Vec::new();
        let mut writer = WriterBuilder::new(schema)
            .with_fingerprint_strategy(FingerprintStrategy::Id(1))
            .build::<_, AvroSoeFormat>(&mut sink)
            .expect("failed to create writer");
        // Encode row by row
        for row_idx in 0..batch.num_rows() {
            let single_row = batch.slice(row_idx, 1);
            writer
                .write(&single_row)
                .unwrap_or_else(|e| panic!("Failed to encode product row {row_idx}: {e}"));
        }
        writer.finish().expect("failed to finish writer");
        assert!(!sink.is_empty());
    }

    #[test]
    fn non_nullable_child_in_nullable_struct_should_encode_per_row() {
        use arrow_array::{
            ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray, StructArray,
        };
        use arrow_schema::{DataType, Field, Fields, Schema};
        use std::sync::Arc;
        let row_fields = Fields::from(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]);
        let row_struct_dt = DataType::Struct(row_fields.clone());
        let before: ArrayRef = Arc::new(StructArray::new_null(row_fields.clone(), 1));
        let id_col: ArrayRef = Arc::new(Int32Array::from(vec![1]));
        let name_col: ArrayRef = Arc::new(StringArray::from(vec![None::<&str>]));
        let after: ArrayRef = Arc::new(StructArray::new(
            row_fields.clone(),
            vec![id_col, name_col],
            None,
        ));
        let schema = Arc::new(Schema::new(vec![
            Field::new("before", row_struct_dt.clone(), true),
            Field::new("after", row_struct_dt, true),
            Field::new("op", DataType::Utf8, false),
            Field::new("ts_ms", DataType::Int64, false),
        ]));
        let op = Arc::new(StringArray::from(vec!["r"])) as ArrayRef;
        let ts_ms = Arc::new(Int64Array::from(vec![1732900000000_i64])) as ArrayRef;
        let batch = RecordBatch::try_new(schema.clone(), vec![before, after, op, ts_ms]).unwrap();
        let mut buf = Vec::new();
        let mut writer = WriterBuilder::new(schema.as_ref().clone())
            .build::<_, AvroSoeFormat>(&mut buf)
            .unwrap();
        let single = batch.slice(0, 1);
        let res = writer.write(&single);
        assert!(
            res.is_ok(),
            "expected to encode successfully, got: {:?}",
            res.err()
        );
    }

    #[test]
    fn test_union_nonzero_type_ids() -> Result<(), ArrowError> {
        use arrow_array::UnionArray;
        use arrow_buffer::Buffer;
        use arrow_schema::UnionFields;
        let union_fields = UnionFields::try_new(
            vec![2, 5],
            vec![
                Field::new("v_str", DataType::Utf8, true),
                Field::new("v_int", DataType::Int32, true),
            ],
        )
        .unwrap();
        let strings = StringArray::from(vec!["hello", "world"]);
        let ints = Int32Array::from(vec![10, 20, 30]);
        let type_ids = Buffer::from_slice_ref([2_i8, 5, 5, 2, 5]);
        let offsets = Buffer::from_slice_ref([0_i32, 0, 1, 1, 2]);
        let union_array = UnionArray::try_new(
            union_fields.clone(),
            type_ids.into(),
            Some(offsets.into()),
            vec![Arc::new(strings) as ArrayRef, Arc::new(ints) as ArrayRef],
        )?;
        let schema = Schema::new(vec![Field::new(
            "union_col",
            DataType::Union(union_fields, UnionMode::Dense),
            false,
        )]);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(union_array) as ArrayRef],
        )?;
        let mut writer = AvroWriter::new(Vec::<u8>::new(), schema.clone())?;
        assert!(
            writer.write(&batch).is_ok(),
            "Expected no error from writing"
        );
        writer.finish()?;
        assert!(
            writer.finish().is_ok(),
            "Expected no error from finishing writer"
        );
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
            .build::<_, AvroSoeFormat>(Vec::new())?;
        writer.write(&batch)?;
        let encoded = writer.into_inner();
        let mut store = SchemaStore::new_with_type(FingerprintAlgorithm::Id);
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
    fn test_stream_writer_with_id64_fingerprint_rt() -> Result<(), ArrowError> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef],
        )?;
        let schema_id: u64 = 42;
        let mut writer = WriterBuilder::new(schema.clone())
            .with_fingerprint_strategy(FingerprintStrategy::Id64(schema_id))
            .build::<_, AvroSoeFormat>(Vec::new())?;
        writer.write(&batch)?;
        let encoded = writer.into_inner();
        let mut store = SchemaStore::new_with_type(FingerprintAlgorithm::Id64);
        let avro_schema = AvroSchema::try_from(&schema)?;
        let _ = store.set(Fingerprint::Id64(schema_id), avro_schema)?;
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
        for rel in files() {
            let path = arrow_test_data(rel);
            let rdr_file = File::open(&path).expect("open input avro");
            let reader = ReaderBuilder::new()
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
            let rt_reader = ReaderBuilder::new()
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
        let reader = ReaderBuilder::new()
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
        let rt_reader = ReaderBuilder::new()
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
    #[cfg(feature = "snappy")]
    fn test_roundtrip_nested_lists_writer() -> Result<(), ArrowError> {
        let path = arrow_test_data("avro/nested_lists.snappy.avro");
        let rdr_file = File::open(&path).expect("open nested_lists.snappy.avro");
        let reader = ReaderBuilder::new()
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
        let rt_reader = ReaderBuilder::new()
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
        let reader = ReaderBuilder::new()
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
        let rt_reader = ReaderBuilder::new()
            .build(BufReader::new(rt_file))
            .expect("build round_trip reader");
        let rt_schema = rt_reader.schema();
        let rt_batches = rt_reader.collect::<Result<Vec<_>, _>>()?;
        let round_trip =
            arrow::compute::concat_batches(&rt_schema, &rt_batches).expect("concat round_trip");
        assert_eq!(round_trip, original);
        Ok(())
    }

    // Strict equality (schema + values) only when canonical extension types are enabled
    #[test]
    #[cfg(feature = "canonical_extension_types")]
    fn test_round_trip_duration_and_uuid_ocf() -> Result<(), ArrowError> {
        use arrow_schema::{DataType, IntervalUnit};
        let in_file =
            File::open("test/data/duration_uuid.avro").expect("open test/data/duration_uuid.avro");
        let reader = ReaderBuilder::new()
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
        // Write to an in‑memory OCF and read back
        let mut writer = AvroWriter::new(Vec::<u8>::new(), in_schema.as_ref().clone())?;
        writer.write(&input)?;
        writer.finish()?;
        let bytes = writer.into_inner();
        let rt_reader = ReaderBuilder::new()
            .build(Cursor::new(bytes))
            .expect("build round_trip reader");
        let rt_schema = rt_reader.schema();
        let rt_batches = rt_reader.collect::<Result<Vec<_>, _>>()?;
        let round_trip =
            arrow::compute::concat_batches(&rt_schema, &rt_batches).expect("concat round_trip");
        assert_eq!(round_trip, input);
        Ok(())
    }

    // Feature OFF: only values are asserted equal; schema may legitimately differ (uuid as fixed(16))
    #[test]
    #[cfg(not(feature = "canonical_extension_types"))]
    fn test_duration_and_uuid_ocf_without_extensions_round_trips_values() -> Result<(), ArrowError>
    {
        use arrow::datatypes::{DataType, IntervalUnit};
        use std::io::BufReader;

        // Read input Avro (duration + uuid)
        let in_file =
            File::open("test/data/duration_uuid.avro").expect("open test/data/duration_uuid.avro");
        let reader = ReaderBuilder::new()
            .build(BufReader::new(in_file))
            .expect("build reader for duration_uuid.avro");
        let in_schema = reader.schema();

        // Sanity checks: has MonthDayNano and a FixedSizeBinary(16)
        assert!(
            in_schema.fields().iter().any(|f| {
                matches!(
                    f.data_type(),
                    DataType::Interval(IntervalUnit::MonthDayNano)
                )
            }),
            "expected at least one Interval(MonthDayNano) field"
        );
        assert!(
            in_schema
                .fields()
                .iter()
                .any(|f| matches!(f.data_type(), DataType::FixedSizeBinary(16))),
            "expected a FixedSizeBinary(16) field (uuid)"
        );

        let input_batches = reader.collect::<Result<Vec<_>, _>>()?;
        let input =
            arrow::compute::concat_batches(&in_schema, &input_batches).expect("concat input");

        // Write to a temp OCF and read back
        let mut writer = AvroWriter::new(Vec::<u8>::new(), in_schema.as_ref().clone())?;
        writer.write(&input)?;
        writer.finish()?;
        let bytes = writer.into_inner();
        let rt_reader = ReaderBuilder::new()
            .build(Cursor::new(bytes))
            .expect("build round_trip reader");
        let rt_schema = rt_reader.schema();
        let rt_batches = rt_reader.collect::<Result<Vec<_>, _>>()?;
        let round_trip =
            arrow::compute::concat_batches(&rt_schema, &rt_batches).expect("concat round_trip");

        // 1) Values must round-trip for both columns
        assert_eq!(
            round_trip.column(0),
            input.column(0),
            "duration column values differ"
        );
        assert_eq!(round_trip.column(1), input.column(1), "uuid bytes differ");

        // 2) Schema expectation without extensions:
        //    uuid is written as named fixed(16), so reader attaches avro.name
        let uuid_rt = rt_schema.field_with_name("uuid_field")?;
        assert_eq!(uuid_rt.data_type(), &DataType::FixedSizeBinary(16));
        assert_eq!(
            uuid_rt.metadata().get("logicalType").map(|s| s.as_str()),
            Some("uuid"),
            "expected `logicalType = \"uuid\"` on round-tripped field metadata"
        );

        // 3) Duration remains Interval(MonthDayNano)
        let dur_rt = rt_schema.field_with_name("duration_field")?;
        assert!(matches!(
            dur_rt.data_type(),
            DataType::Interval(IntervalUnit::MonthDayNano)
        ));

        Ok(())
    }

    // This test reads the same 'nonnullable.impala.avro' used by the reader tests,
    // writes it back out with the writer (hitting Map encoding paths), then reads it
    // again and asserts exact Arrow equivalence.
    #[test]
    // TODO: avoid requiring snappy for this file
    #[cfg(feature = "snappy")]
    fn test_nonnullable_impala_roundtrip_writer() -> Result<(), ArrowError> {
        // Load source Avro with Map fields
        let path = arrow_test_data("avro/nonnullable.impala.avro");
        let rdr_file = File::open(&path).expect("open avro/nonnullable.impala.avro");
        let reader = ReaderBuilder::new()
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
        let rt_reader = ReaderBuilder::new()
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
    // TODO: avoid requiring snappy for these files
    #[cfg(feature = "snappy")]
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
            let rdr = ReaderBuilder::new().build(BufReader::new(f_in))?;
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
            let rt_rdr = ReaderBuilder::new().build(BufReader::new(f_rt))?;
            let rt_schema = rt_rdr.schema();
            let rt_batches = rt_rdr.collect::<Result<Vec<_>, _>>()?;
            let roundtrip =
                arrow::compute::concat_batches(&rt_schema, &rt_batches).expect("concat rt");
            assert_eq!(roundtrip, original, "decimal round-trip mismatch for {rel}");
        }
        Ok(())
    }

    #[test]
    fn test_named_types_complex_roundtrip() -> Result<(), ArrowError> {
        // 1. Read the new, more complex named references file.
        let path =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test/data/named_types_complex.avro");
        let rdr_file = File::open(&path).expect("open avro/named_types_complex.avro");

        let reader = ReaderBuilder::new()
            .build(BufReader::new(rdr_file))
            .expect("build reader for named_types_complex.avro");

        // 2. Concatenate all batches to one RecordBatch.
        let in_schema = reader.schema();
        let input_batches = reader.collect::<Result<Vec<_>, _>>()?;
        let original =
            arrow::compute::concat_batches(&in_schema, &input_batches).expect("concat input");

        // 3. Sanity Checks: Validate that all named types were reused correctly.
        {
            let arrow_schema = original.schema();

            // --- A. Validate 'User' record reuse ---
            let author_field = arrow_schema.field_with_name("author")?;
            let author_type = author_field.data_type();
            let editors_field = arrow_schema.field_with_name("editors")?;
            let editors_item_type = match editors_field.data_type() {
                DataType::List(item_field) => item_field.data_type(),
                other => panic!("Editors field should be a List, but was {:?}", other),
            };
            assert_eq!(
                author_type, editors_item_type,
                "The DataType for the 'author' struct and the 'editors' list items must be identical"
            );

            // --- B. Validate 'PostStatus' enum reuse ---
            let status_field = arrow_schema.field_with_name("status")?;
            let status_type = status_field.data_type();
            assert!(
                matches!(status_type, DataType::Dictionary(_, _)),
                "Status field should be a Dictionary (Enum)"
            );

            let prev_status_field = arrow_schema.field_with_name("previous_status")?;
            let prev_status_type = prev_status_field.data_type();
            assert_eq!(
                status_type, prev_status_type,
                "The DataType for 'status' and 'previous_status' enums must be identical"
            );

            // --- C. Validate 'MD5' fixed reuse ---
            let content_hash_field = arrow_schema.field_with_name("content_hash")?;
            let content_hash_type = content_hash_field.data_type();
            assert!(
                matches!(content_hash_type, DataType::FixedSizeBinary(16)),
                "Content hash should be FixedSizeBinary(16)"
            );

            let thumb_hash_field = arrow_schema.field_with_name("thumbnail_hash")?;
            let thumb_hash_type = thumb_hash_field.data_type();
            assert_eq!(
                content_hash_type, thumb_hash_type,
                "The DataType for 'content_hash' and 'thumbnail_hash' fixed types must be identical"
            );
        }

        // 4. Write the data to an in-memory buffer.
        let buffer: Vec<u8> = Vec::new();
        let mut writer = AvroWriter::new(buffer, original.schema().as_ref().clone())?;
        writer.write(&original)?;
        writer.finish()?;
        let bytes = writer.into_inner();

        // 5. Read the data back and compare for exact equality.
        let rt_reader = ReaderBuilder::new()
            .build(Cursor::new(bytes))
            .expect("build reader for round-trip");
        let rt_schema = rt_reader.schema();
        let rt_batches = rt_reader.collect::<Result<Vec<_>, _>>()?;
        let roundtrip =
            arrow::compute::concat_batches(&rt_schema, &rt_batches).expect("concat roundtrip");

        assert_eq!(
            roundtrip, original,
            "Avro complex named types round-trip mismatch"
        );

        Ok(())
    }

    // Union Roundtrip Test Helpers

    // Asserts that the `actual` schema is a semantically equivalent superset of the `expected` one.
    // This allows the `actual` schema to contain additional metadata keys
    // (`arrowUnionMode`, `arrowUnionTypeIds`, `avro.name`) that are added during an Arrow-to-Avro-to-Arrow
    // roundtrip, while ensuring no other information was lost or changed.
    fn assert_schema_is_semantically_equivalent(expected: &Schema, actual: &Schema) {
        // Compare top-level schema metadata using the same superset logic.
        assert_metadata_is_superset(expected.metadata(), actual.metadata(), "Schema");

        // Compare fields.
        assert_eq!(
            expected.fields().len(),
            actual.fields().len(),
            "Schema must have the same number of fields"
        );

        for (expected_field, actual_field) in expected.fields().iter().zip(actual.fields().iter()) {
            assert_field_is_semantically_equivalent(expected_field, actual_field);
        }
    }

    fn assert_field_is_semantically_equivalent(expected: &Field, actual: &Field) {
        let context = format!("Field '{}'", expected.name());

        assert_eq!(
            expected.name(),
            actual.name(),
            "{context}: names must match"
        );
        assert_eq!(
            expected.is_nullable(),
            actual.is_nullable(),
            "{context}: nullability must match"
        );

        // Recursively check the data types.
        assert_datatype_is_semantically_equivalent(
            expected.data_type(),
            actual.data_type(),
            &context,
        );

        // Check that metadata is a valid superset.
        assert_metadata_is_superset(expected.metadata(), actual.metadata(), &context);
    }

    fn assert_datatype_is_semantically_equivalent(
        expected: &DataType,
        actual: &DataType,
        context: &str,
    ) {
        match (expected, actual) {
            (DataType::List(expected_field), DataType::List(actual_field))
            | (DataType::LargeList(expected_field), DataType::LargeList(actual_field))
            | (DataType::Map(expected_field, _), DataType::Map(actual_field, _)) => {
                assert_field_is_semantically_equivalent(expected_field, actual_field);
            }
            (DataType::Struct(expected_fields), DataType::Struct(actual_fields)) => {
                assert_eq!(
                    expected_fields.len(),
                    actual_fields.len(),
                    "{context}: struct must have same number of fields"
                );
                for (ef, af) in expected_fields.iter().zip(actual_fields.iter()) {
                    assert_field_is_semantically_equivalent(ef, af);
                }
            }
            (
                DataType::Union(expected_fields, expected_mode),
                DataType::Union(actual_fields, actual_mode),
            ) => {
                assert_eq!(
                    expected_mode, actual_mode,
                    "{context}: union mode must match"
                );
                assert_eq!(
                    expected_fields.len(),
                    actual_fields.len(),
                    "{context}: union must have same number of variants"
                );
                for ((exp_id, exp_field), (act_id, act_field)) in
                    expected_fields.iter().zip(actual_fields.iter())
                {
                    assert_eq!(exp_id, act_id, "{context}: union type ids must match");
                    assert_field_is_semantically_equivalent(exp_field, act_field);
                }
            }
            _ => {
                assert_eq!(expected, actual, "{context}: data types must be identical");
            }
        }
    }

    fn assert_batch_data_is_identical(expected: &RecordBatch, actual: &RecordBatch) {
        assert_eq!(
            expected.num_columns(),
            actual.num_columns(),
            "RecordBatches must have the same number of columns"
        );
        assert_eq!(
            expected.num_rows(),
            actual.num_rows(),
            "RecordBatches must have the same number of rows"
        );

        for i in 0..expected.num_columns() {
            let context = format!("Column {i}");
            let expected_col = expected.column(i);
            let actual_col = actual.column(i);
            assert_array_data_is_identical(expected_col, actual_col, &context);
        }
    }

    /// Recursively asserts that the data content of two Arrays is identical.
    fn assert_array_data_is_identical(expected: &dyn Array, actual: &dyn Array, context: &str) {
        assert_eq!(
            expected.nulls(),
            actual.nulls(),
            "{context}: null buffers must match"
        );
        assert_eq!(
            expected.len(),
            actual.len(),
            "{context}: array lengths must match"
        );

        match (expected.data_type(), actual.data_type()) {
            (DataType::Union(expected_fields, _), DataType::Union(..)) => {
                let expected_union = expected.as_any().downcast_ref::<UnionArray>().unwrap();
                let actual_union = actual.as_any().downcast_ref::<UnionArray>().unwrap();

                // Compare the type_ids buffer (always the first buffer).
                assert_eq!(
                    &expected.to_data().buffers()[0],
                    &actual.to_data().buffers()[0],
                    "{context}: union type_ids buffer mismatch"
                );

                // For dense unions, compare the value_offsets buffer (the second buffer).
                if expected.to_data().buffers().len() > 1 {
                    assert_eq!(
                        &expected.to_data().buffers()[1],
                        &actual.to_data().buffers()[1],
                        "{context}: union value_offsets buffer mismatch"
                    );
                }

                // Recursively compare children based on the fields in the DataType.
                for (type_id, _) in expected_fields.iter() {
                    let child_context = format!("{context} -> child variant {type_id}");
                    assert_array_data_is_identical(
                        expected_union.child(type_id),
                        actual_union.child(type_id),
                        &child_context,
                    );
                }
            }
            (DataType::Struct(_), DataType::Struct(_)) => {
                let expected_struct = expected.as_any().downcast_ref::<StructArray>().unwrap();
                let actual_struct = actual.as_any().downcast_ref::<StructArray>().unwrap();
                for i in 0..expected_struct.num_columns() {
                    let child_context = format!("{context} -> struct child {i}");
                    assert_array_data_is_identical(
                        expected_struct.column(i),
                        actual_struct.column(i),
                        &child_context,
                    );
                }
            }
            // Fallback for primitive types and other types where buffer comparison is sufficient.
            _ => {
                assert_eq!(
                    expected.to_data().buffers(),
                    actual.to_data().buffers(),
                    "{context}: data buffers must match"
                );
            }
        }
    }

    /// Checks that `actual_meta` contains all of `expected_meta`, and any additional
    /// keys in `actual_meta` are from a permitted set.
    fn assert_metadata_is_superset(
        expected_meta: &HashMap<String, String>,
        actual_meta: &HashMap<String, String>,
        context: &str,
    ) {
        let allowed_additions: HashSet<&str> =
            vec!["arrowUnionMode", "arrowUnionTypeIds", "avro.name"]
                .into_iter()
                .collect();
        for (key, expected_value) in expected_meta {
            match actual_meta.get(key) {
                Some(actual_value) => assert_eq!(
                    expected_value, actual_value,
                    "{context}: preserved metadata for key '{key}' must have the same value"
                ),
                None => panic!("{context}: metadata key '{key}' was lost during roundtrip"),
            }
        }
        for key in actual_meta.keys() {
            if !expected_meta.contains_key(key) && !allowed_additions.contains(key.as_str()) {
                panic!("{context}: unexpected metadata key '{key}' was added during roundtrip");
            }
        }
    }

    #[test]
    fn test_union_roundtrip() -> Result<(), ArrowError> {
        let file_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("test/data/union_fields.avro")
            .to_string_lossy()
            .into_owned();
        let rdr_file = File::open(&file_path).expect("open avro/union_fields.avro");
        let reader = ReaderBuilder::new()
            .build(BufReader::new(rdr_file))
            .expect("build reader for union_fields.avro");
        let schema = reader.schema();
        let input_batches = reader.collect::<Result<Vec<_>, _>>()?;
        let original =
            arrow::compute::concat_batches(&schema, &input_batches).expect("concat input");
        let mut writer = AvroWriter::new(Vec::<u8>::new(), original.schema().as_ref().clone())?;
        writer.write(&original)?;
        writer.finish()?;
        let bytes = writer.into_inner();
        let rt_reader = ReaderBuilder::new()
            .build(Cursor::new(bytes))
            .expect("build round_trip reader");
        let rt_schema = rt_reader.schema();
        let rt_batches = rt_reader.collect::<Result<Vec<_>, _>>()?;
        let round_trip =
            arrow::compute::concat_batches(&rt_schema, &rt_batches).expect("concat round_trip");

        // The nature of the crate is such that metadata gets appended during the roundtrip,
        // so we can't compare the schemas directly. Instead, we semantically compare the schemas and data.
        assert_schema_is_semantically_equivalent(&original.schema(), &round_trip.schema());

        assert_batch_data_is_identical(&original, &round_trip);
        Ok(())
    }

    #[test]
    fn test_enum_roundtrip_uses_reader_fixture() -> Result<(), ArrowError> {
        // Read the known-good enum file (same as reader::test_simple)
        let path = arrow_test_data("avro/simple_enum.avro");
        let rdr_file = File::open(&path).expect("open avro/simple_enum.avro");
        let reader = ReaderBuilder::new()
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
        let rt_reader = ReaderBuilder::new()
            .build(Cursor::new(bytes))
            .expect("reader for round-trip");
        let rt_schema = rt_reader.schema();
        let rt_batches = rt_reader.collect::<Result<Vec<_>, _>>()?;
        let roundtrip =
            arrow::compute::concat_batches(&rt_schema, &rt_batches).expect("concat roundtrip");
        assert_eq!(roundtrip, original, "Avro enum round-trip mismatch");
        Ok(())
    }

    #[test]
    fn test_builder_propagates_capacity_to_writer() -> Result<(), ArrowError> {
        let cap = 64 * 1024;
        let buffer = Vec::<u8>::new();
        let mut writer = WriterBuilder::new(make_schema())
            .with_capacity(cap)
            .build::<_, AvroOcfFormat>(buffer)?;
        assert_eq!(writer.capacity, cap, "builder capacity not propagated");
        let batch = make_batch();
        writer.write(&batch)?;
        writer.finish()?;
        let out = writer.into_inner();
        assert_eq!(&out[..4], b"Obj\x01", "OCF magic missing/incorrect");
        Ok(())
    }

    #[test]
    fn test_stream_writer_stores_capacity_direct_writes() -> Result<(), ArrowError> {
        use arrow_array::{ArrayRef, Int32Array};
        use arrow_schema::{DataType, Field, Schema};
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef],
        )?;
        let cap = 8192;
        let mut writer = WriterBuilder::new(schema)
            .with_capacity(cap)
            .build::<_, AvroSoeFormat>(Vec::new())?;
        assert_eq!(writer.capacity, cap);
        writer.write(&batch)?;
        let _bytes = writer.into_inner();
        Ok(())
    }

    #[cfg(feature = "avro_custom_types")]
    #[test]
    fn test_roundtrip_duration_logical_types_ocf() -> Result<(), ArrowError> {
        let file_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("test/data/duration_logical_types.avro")
            .to_string_lossy()
            .into_owned();

        let in_file = File::open(&file_path)
            .unwrap_or_else(|_| panic!("Failed to open test file: {}", file_path));

        let reader = ReaderBuilder::new()
            .build(BufReader::new(in_file))
            .expect("build reader for duration_logical_types.avro");
        let in_schema = reader.schema();

        let expected_units: HashSet<TimeUnit> = [
            TimeUnit::Nanosecond,
            TimeUnit::Microsecond,
            TimeUnit::Millisecond,
            TimeUnit::Second,
        ]
        .into_iter()
        .collect();

        let found_units: HashSet<TimeUnit> = in_schema
            .fields()
            .iter()
            .filter_map(|f| match f.data_type() {
                DataType::Duration(unit) => Some(*unit),
                _ => None,
            })
            .collect();

        assert_eq!(
            found_units, expected_units,
            "Expected to find all four Duration TimeUnits in the schema from the initial read"
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
        let rt_reader = ReaderBuilder::new()
            .build(BufReader::new(rt_file))
            .expect("build round_trip reader");
        let rt_schema = rt_reader.schema();
        let rt_batches = rt_reader.collect::<Result<Vec<_>, _>>()?;
        let round_trip =
            arrow::compute::concat_batches(&rt_schema, &rt_batches).expect("concat round_trip");

        assert_eq!(round_trip, input);
        Ok(())
    }

    #[cfg(feature = "avro_custom_types")]
    #[test]
    fn test_run_end_encoded_roundtrip_writer() -> Result<(), ArrowError> {
        let run_ends = Int32Array::from(vec![3, 5, 7, 8]);
        let run_values = Int32Array::from(vec![Some(1), Some(2), None, Some(3)]);
        let ree = RunArray::<Int32Type>::try_new(&run_ends, &run_values)?;
        let field = Field::new("x", ree.data_type().clone(), true);
        let schema = Schema::new(vec![field]);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(ree.clone()) as ArrayRef],
        )?;
        let mut writer = AvroWriter::new(Vec::<u8>::new(), schema.clone())?;
        writer.write(&batch)?;
        writer.finish()?;
        let bytes = writer.into_inner();
        let reader = ReaderBuilder::new().build(Cursor::new(bytes))?;
        let out_schema = reader.schema();
        let batches = reader.collect::<Result<Vec<_>, _>>()?;
        let out = arrow::compute::concat_batches(&out_schema, &batches).expect("concat output");
        assert_eq!(out.num_columns(), 1);
        assert_eq!(out.num_rows(), 8);
        match out.schema().field(0).data_type() {
            DataType::RunEndEncoded(run_ends_field, values_field) => {
                assert_eq!(run_ends_field.name(), "run_ends");
                assert_eq!(run_ends_field.data_type(), &DataType::Int32);
                assert_eq!(values_field.name(), "values");
                assert_eq!(values_field.data_type(), &DataType::Int32);
                assert!(values_field.is_nullable());
                let got_ree = out
                    .column(0)
                    .as_any()
                    .downcast_ref::<RunArray<Int32Type>>()
                    .expect("RunArray<Int32Type>");
                assert_eq!(got_ree, &ree);
            }
            other => panic!(
                "Unexpected DataType for round-tripped RunEndEncoded column: {:?}",
                other
            ),
        }
        Ok(())
    }

    #[cfg(feature = "avro_custom_types")]
    #[test]
    fn test_run_end_encoded_string_values_int16_run_ends_roundtrip_writer() -> Result<(), ArrowError>
    {
        let run_ends = Int16Array::from(vec![2, 5, 7]); // end indices
        let run_values = StringArray::from(vec![Some("a"), None, Some("c")]);
        let ree = RunArray::<Int16Type>::try_new(&run_ends, &run_values)?;
        let field = Field::new("s", ree.data_type().clone(), true);
        let schema = Schema::new(vec![field]);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(ree.clone()) as ArrayRef],
        )?;
        let mut writer = AvroWriter::new(Vec::<u8>::new(), schema.clone())?;
        writer.write(&batch)?;
        writer.finish()?;
        let bytes = writer.into_inner();
        let reader = ReaderBuilder::new().build(Cursor::new(bytes))?;
        let out_schema = reader.schema();
        let batches = reader.collect::<Result<Vec<_>, _>>()?;
        let out = arrow::compute::concat_batches(&out_schema, &batches).expect("concat output");
        assert_eq!(out.num_columns(), 1);
        assert_eq!(out.num_rows(), 7);
        match out.schema().field(0).data_type() {
            DataType::RunEndEncoded(run_ends_field, values_field) => {
                assert_eq!(run_ends_field.data_type(), &DataType::Int16);
                assert_eq!(values_field.data_type(), &DataType::Utf8);
                assert!(
                    values_field.is_nullable(),
                    "REE 'values' child should be nullable"
                );
                let got = out
                    .column(0)
                    .as_any()
                    .downcast_ref::<RunArray<Int16Type>>()
                    .expect("RunArray<Int16Type>");
                assert_eq!(got, &ree);
            }
            other => panic!("Unexpected DataType: {:?}", other),
        }
        Ok(())
    }

    #[cfg(feature = "avro_custom_types")]
    #[test]
    fn test_run_end_encoded_int64_run_ends_numeric_values_roundtrip_writer()
    -> Result<(), ArrowError> {
        let run_ends = Int64Array::from(vec![4_i64, 8_i64]);
        let run_values = Int32Array::from(vec![Some(999), Some(-5)]);
        let ree = RunArray::<Int64Type>::try_new(&run_ends, &run_values)?;
        let field = Field::new("y", ree.data_type().clone(), true);
        let schema = Schema::new(vec![field]);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(ree.clone()) as ArrayRef],
        )?;
        let mut writer = AvroWriter::new(Vec::<u8>::new(), schema.clone())?;
        writer.write(&batch)?;
        writer.finish()?;
        let bytes = writer.into_inner();
        let reader = ReaderBuilder::new().build(Cursor::new(bytes))?;
        let out_schema = reader.schema();
        let batches = reader.collect::<Result<Vec<_>, _>>()?;
        let out = arrow::compute::concat_batches(&out_schema, &batches).expect("concat output");
        assert_eq!(out.num_columns(), 1);
        assert_eq!(out.num_rows(), 8);
        match out.schema().field(0).data_type() {
            DataType::RunEndEncoded(run_ends_field, values_field) => {
                assert_eq!(run_ends_field.data_type(), &DataType::Int64);
                assert_eq!(values_field.data_type(), &DataType::Int32);
                assert!(values_field.is_nullable());
                let got = out
                    .column(0)
                    .as_any()
                    .downcast_ref::<RunArray<Int64Type>>()
                    .expect("RunArray<Int64Type>");
                assert_eq!(got, &ree);
            }
            other => panic!("Unexpected DataType for REE column: {:?}", other),
        }
        Ok(())
    }

    #[cfg(feature = "avro_custom_types")]
    #[test]
    fn test_run_end_encoded_sliced_roundtrip_writer() -> Result<(), ArrowError> {
        let run_ends = Int32Array::from(vec![3, 5, 7, 8]);
        let run_values = Int32Array::from(vec![Some(1), Some(2), None, Some(3)]);
        let base = RunArray::<Int32Type>::try_new(&run_ends, &run_values)?;
        let offset = 1usize;
        let length = 6usize;
        let base_values = base
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("REE values as Int32Array");
        let mut logical_window: Vec<Option<i32>> = Vec::with_capacity(length);
        for i in offset..offset + length {
            let phys = base.get_physical_index(i);
            let v = if base_values.is_null(phys) {
                None
            } else {
                Some(base_values.value(phys))
            };
            logical_window.push(v);
        }

        fn compress_run_ends_i32(vals: &[Option<i32>]) -> (Int32Array, Int32Array) {
            if vals.is_empty() {
                return (Int32Array::new_null(0), Int32Array::new_null(0));
            }
            let mut run_ends_out: Vec<i32> = Vec::new();
            let mut run_vals_out: Vec<Option<i32>> = Vec::new();
            let mut cur = vals[0];
            let mut len = 1i32;
            for v in &vals[1..] {
                if *v == cur {
                    len += 1;
                } else {
                    let last_end = run_ends_out.last().copied().unwrap_or(0);
                    run_ends_out.push(last_end + len);
                    run_vals_out.push(cur);
                    cur = *v;
                    len = 1;
                }
            }
            let last_end = run_ends_out.last().copied().unwrap_or(0);
            run_ends_out.push(last_end + len);
            run_vals_out.push(cur);
            (
                Int32Array::from(run_ends_out),
                Int32Array::from(run_vals_out),
            )
        }
        let (owned_run_ends, owned_run_values) = compress_run_ends_i32(&logical_window);
        let owned_slice = RunArray::<Int32Type>::try_new(&owned_run_ends, &owned_run_values)?;
        let field = Field::new("x", owned_slice.data_type().clone(), true);
        let schema = Schema::new(vec![field]);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(owned_slice.clone()) as ArrayRef],
        )?;
        let mut writer = AvroWriter::new(Vec::<u8>::new(), schema.clone())?;
        writer.write(&batch)?;
        writer.finish()?;
        let bytes = writer.into_inner();
        let reader = ReaderBuilder::new().build(Cursor::new(bytes))?;
        let out_schema = reader.schema();
        let batches = reader.collect::<Result<Vec<_>, _>>()?;
        let out = arrow::compute::concat_batches(&out_schema, &batches).expect("concat output");
        assert_eq!(out.num_columns(), 1);
        assert_eq!(out.num_rows(), length);
        match out.schema().field(0).data_type() {
            DataType::RunEndEncoded(run_ends_field, values_field) => {
                assert_eq!(run_ends_field.data_type(), &DataType::Int32);
                assert_eq!(values_field.data_type(), &DataType::Int32);
                assert!(values_field.is_nullable());
                let got = out
                    .column(0)
                    .as_any()
                    .downcast_ref::<RunArray<Int32Type>>()
                    .expect("RunArray<Int32Type>");
                fn expand_ree_to_int32(a: &RunArray<Int32Type>) -> Int32Array {
                    let vals = a
                        .values()
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .expect("REE values as Int32Array");
                    let mut out: Vec<Option<i32>> = Vec::with_capacity(a.len());
                    for i in 0..a.len() {
                        let phys = a.get_physical_index(i);
                        out.push(if vals.is_null(phys) {
                            None
                        } else {
                            Some(vals.value(phys))
                        });
                    }
                    Int32Array::from(out)
                }
                let got_logical = expand_ree_to_int32(got);
                let expected_logical = Int32Array::from(logical_window);
                assert_eq!(
                    got_logical, expected_logical,
                    "Logical values differ after REE slice round-trip"
                );
            }
            other => panic!("Unexpected DataType for REE column: {:?}", other),
        }
        Ok(())
    }

    #[cfg(not(feature = "avro_custom_types"))]
    #[test]
    fn test_run_end_encoded_roundtrip_writer_feature_off() -> Result<(), ArrowError> {
        use arrow_schema::{DataType, Field, Schema};
        let run_ends = arrow_array::Int32Array::from(vec![3, 5, 7, 8]);
        let run_values = arrow_array::Int32Array::from(vec![Some(1), Some(2), None, Some(3)]);
        let ree = arrow_array::RunArray::<arrow_array::types::Int32Type>::try_new(
            &run_ends,
            &run_values,
        )?;
        let field = Field::new("x", ree.data_type().clone(), true);
        let schema = Schema::new(vec![field]);
        let batch =
            RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(ree) as ArrayRef])?;
        let mut writer = AvroWriter::new(Vec::<u8>::new(), schema.clone())?;
        writer.write(&batch)?;
        writer.finish()?;
        let bytes = writer.into_inner();
        let reader = ReaderBuilder::new().build(Cursor::new(bytes))?;
        let out_schema = reader.schema();
        let batches = reader.collect::<Result<Vec<_>, _>>()?;
        let out = arrow::compute::concat_batches(&out_schema, &batches).expect("concat output");
        assert_eq!(out.num_columns(), 1);
        assert_eq!(out.num_rows(), 8);
        assert_eq!(out.schema().field(0).data_type(), &DataType::Int32);
        let got = out
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Int32Array");
        let expected = Int32Array::from(vec![
            Some(1),
            Some(1),
            Some(1),
            Some(2),
            Some(2),
            None,
            None,
            Some(3),
        ]);
        assert_eq!(got, &expected);
        Ok(())
    }

    #[cfg(not(feature = "avro_custom_types"))]
    #[test]
    fn test_run_end_encoded_string_values_int16_run_ends_roundtrip_writer_feature_off()
    -> Result<(), ArrowError> {
        use arrow_schema::{DataType, Field, Schema};
        let run_ends = arrow_array::Int16Array::from(vec![2, 5, 7]);
        let run_values = arrow_array::StringArray::from(vec![Some("a"), None, Some("c")]);
        let ree = arrow_array::RunArray::<arrow_array::types::Int16Type>::try_new(
            &run_ends,
            &run_values,
        )?;
        let field = Field::new("s", ree.data_type().clone(), true);
        let schema = Schema::new(vec![field]);
        let batch =
            RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(ree) as ArrayRef])?;
        let mut writer = AvroWriter::new(Vec::<u8>::new(), schema.clone())?;
        writer.write(&batch)?;
        writer.finish()?;
        let bytes = writer.into_inner();
        let reader = ReaderBuilder::new().build(Cursor::new(bytes))?;
        let out_schema = reader.schema();
        let batches = reader.collect::<Result<Vec<_>, _>>()?;
        let out = arrow::compute::concat_batches(&out_schema, &batches).expect("concat output");
        assert_eq!(out.num_columns(), 1);
        assert_eq!(out.num_rows(), 7);
        assert_eq!(out.schema().field(0).data_type(), &DataType::Utf8);
        let got = out
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .expect("StringArray");
        let expected = arrow_array::StringArray::from(vec![
            Some("a"),
            Some("a"),
            None,
            None,
            None,
            Some("c"),
            Some("c"),
        ]);
        assert_eq!(got, &expected);
        Ok(())
    }

    #[cfg(not(feature = "avro_custom_types"))]
    #[test]
    fn test_run_end_encoded_int64_run_ends_numeric_values_roundtrip_writer_feature_off()
    -> Result<(), ArrowError> {
        use arrow_schema::{DataType, Field, Schema};
        let run_ends = arrow_array::Int64Array::from(vec![4_i64, 8_i64]);
        let run_values = Int32Array::from(vec![Some(999), Some(-5)]);
        let ree = arrow_array::RunArray::<arrow_array::types::Int64Type>::try_new(
            &run_ends,
            &run_values,
        )?;
        let field = Field::new("y", ree.data_type().clone(), true);
        let schema = Schema::new(vec![field]);
        let batch =
            RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(ree) as ArrayRef])?;
        let mut writer = AvroWriter::new(Vec::<u8>::new(), schema.clone())?;
        writer.write(&batch)?;
        writer.finish()?;
        let bytes = writer.into_inner();
        let reader = ReaderBuilder::new().build(Cursor::new(bytes))?;
        let out_schema = reader.schema();
        let batches = reader.collect::<Result<Vec<_>, _>>()?;
        let out = arrow::compute::concat_batches(&out_schema, &batches).expect("concat output");
        assert_eq!(out.num_columns(), 1);
        assert_eq!(out.num_rows(), 8);
        assert_eq!(out.schema().field(0).data_type(), &DataType::Int32);
        let got = out
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Int32Array");
        let expected = Int32Array::from(vec![
            Some(999),
            Some(999),
            Some(999),
            Some(999),
            Some(-5),
            Some(-5),
            Some(-5),
            Some(-5),
        ]);
        assert_eq!(got, &expected);
        Ok(())
    }

    #[cfg(not(feature = "avro_custom_types"))]
    #[test]
    fn test_run_end_encoded_sliced_roundtrip_writer_feature_off() -> Result<(), ArrowError> {
        use arrow_schema::{DataType, Field, Schema};
        let run_ends = Int32Array::from(vec![2, 4, 6]);
        let run_values = Int32Array::from(vec![Some(1), Some(2), None]);
        let ree = arrow_array::RunArray::<arrow_array::types::Int32Type>::try_new(
            &run_ends,
            &run_values,
        )?;
        let field = Field::new("x", ree.data_type().clone(), true);
        let schema = Schema::new(vec![field]);
        let batch =
            RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(ree) as ArrayRef])?;
        let mut writer = AvroWriter::new(Vec::<u8>::new(), schema.clone())?;
        writer.write(&batch)?;
        writer.finish()?;
        let bytes = writer.into_inner();
        let reader = ReaderBuilder::new().build(Cursor::new(bytes))?;
        let out_schema = reader.schema();
        let batches = reader.collect::<Result<Vec<_>, _>>()?;
        let out = arrow::compute::concat_batches(&out_schema, &batches).expect("concat output");
        assert_eq!(out.num_columns(), 1);
        assert_eq!(out.num_rows(), 6);
        assert_eq!(out.schema().field(0).data_type(), &DataType::Int32);
        let got = out
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Int32Array");
        let expected = Int32Array::from(vec![Some(1), Some(1), Some(2), Some(2), None, None]);
        assert_eq!(got, &expected);
        Ok(())
    }

    #[test]
    // TODO: avoid requiring snappy for this file
    #[cfg(feature = "snappy")]
    fn test_nullable_impala_roundtrip() -> Result<(), ArrowError> {
        let path = arrow_test_data("avro/nullable.impala.avro");
        let rdr_file = File::open(&path).expect("open avro/nullable.impala.avro");
        let reader = ReaderBuilder::new()
            .build(BufReader::new(rdr_file))
            .expect("build reader for nullable.impala.avro");
        let in_schema = reader.schema();
        assert!(
            in_schema.fields().iter().any(|f| f.is_nullable()),
            "expected at least one nullable field in avro/nullable.impala.avro"
        );
        let input_batches = reader.collect::<Result<Vec<_>, _>>()?;
        let original =
            arrow::compute::concat_batches(&in_schema, &input_batches).expect("concat input");
        let buffer: Vec<u8> = Vec::new();
        let mut writer = AvroWriter::new(buffer, in_schema.as_ref().clone())?;
        writer.write(&original)?;
        writer.finish()?;
        let out_bytes = writer.into_inner();
        let rt_reader = ReaderBuilder::new()
            .build(Cursor::new(out_bytes))
            .expect("build reader for round-tripped in-memory OCF");
        let rt_schema = rt_reader.schema();
        let rt_batches = rt_reader.collect::<Result<Vec<_>, _>>()?;
        let roundtrip =
            arrow::compute::concat_batches(&rt_schema, &rt_batches).expect("concat roundtrip");
        assert_eq!(
            roundtrip, original,
            "Round-trip Avro data mismatch for nullable.impala.avro"
        );
        Ok(())
    }

    #[test]
    #[cfg(feature = "snappy")]
    fn test_datapage_v2_roundtrip() -> Result<(), ArrowError> {
        let path = arrow_test_data("avro/datapage_v2.snappy.avro");
        let rdr_file = File::open(&path).expect("open avro/datapage_v2.snappy.avro");
        let reader = ReaderBuilder::new()
            .build(BufReader::new(rdr_file))
            .expect("build reader for datapage_v2.snappy.avro");
        let in_schema = reader.schema();
        let input_batches = reader.collect::<Result<Vec<_>, _>>()?;
        let original =
            arrow::compute::concat_batches(&in_schema, &input_batches).expect("concat input");
        let mut writer = AvroWriter::new(Vec::<u8>::new(), in_schema.as_ref().clone())?;
        writer.write(&original)?;
        writer.finish()?;
        let bytes = writer.into_inner();
        let rt_reader = ReaderBuilder::new()
            .build(Cursor::new(bytes))
            .expect("build round-trip reader");
        let rt_schema = rt_reader.schema();
        let rt_batches = rt_reader.collect::<Result<Vec<_>, _>>()?;
        let round_trip =
            arrow::compute::concat_batches(&rt_schema, &rt_batches).expect("concat round_trip");
        assert_eq!(
            round_trip, original,
            "Round-trip batch mismatch for datapage_v2.snappy.avro"
        );
        Ok(())
    }

    #[test]
    #[cfg(feature = "snappy")]
    fn test_single_nan_roundtrip() -> Result<(), ArrowError> {
        let path = arrow_test_data("avro/single_nan.avro");
        let in_file = File::open(&path).expect("open avro/single_nan.avro");
        let reader = ReaderBuilder::new()
            .build(BufReader::new(in_file))
            .expect("build reader for single_nan.avro");
        let in_schema = reader.schema();
        let in_batches = reader.collect::<Result<Vec<_>, _>>()?;
        let original =
            arrow::compute::concat_batches(&in_schema, &in_batches).expect("concat input");
        let mut writer = AvroWriter::new(Vec::<u8>::new(), original.schema().as_ref().clone())?;
        writer.write(&original)?;
        writer.finish()?;
        let bytes = writer.into_inner();
        let rt_reader = ReaderBuilder::new()
            .build(Cursor::new(bytes))
            .expect("build round_trip reader");
        let rt_schema = rt_reader.schema();
        let rt_batches = rt_reader.collect::<Result<Vec<_>, _>>()?;
        let round_trip =
            arrow::compute::concat_batches(&rt_schema, &rt_batches).expect("concat round_trip");
        assert_eq!(
            round_trip, original,
            "Round-trip batch mismatch for avro/single_nan.avro"
        );
        Ok(())
    }
    #[test]
    // TODO: avoid requiring snappy for this file
    #[cfg(feature = "snappy")]
    fn test_dict_pages_offset_zero_roundtrip() -> Result<(), ArrowError> {
        let path = arrow_test_data("avro/dict-page-offset-zero.avro");
        let rdr_file = File::open(&path).expect("open avro/dict-page-offset-zero.avro");
        let reader = ReaderBuilder::new()
            .build(BufReader::new(rdr_file))
            .expect("build reader for dict-page-offset-zero.avro");
        let in_schema = reader.schema();
        let input_batches = reader.collect::<Result<Vec<_>, _>>()?;
        let original =
            arrow::compute::concat_batches(&in_schema, &input_batches).expect("concat input");
        let buffer: Vec<u8> = Vec::new();
        let mut writer = AvroWriter::new(buffer, original.schema().as_ref().clone())?;
        writer.write(&original)?;
        writer.finish()?;
        let bytes = writer.into_inner();
        let rt_reader = ReaderBuilder::new()
            .build(Cursor::new(bytes))
            .expect("build reader for round-trip");
        let rt_schema = rt_reader.schema();
        let rt_batches = rt_reader.collect::<Result<Vec<_>, _>>()?;
        let roundtrip =
            arrow::compute::concat_batches(&rt_schema, &rt_batches).expect("concat roundtrip");
        assert_eq!(
            roundtrip, original,
            "Round-trip batch mismatch for avro/dict-page-offset-zero.avro"
        );
        Ok(())
    }

    #[test]
    #[cfg(feature = "snappy")]
    fn test_repeated_no_annotation_roundtrip() -> Result<(), ArrowError> {
        let path = arrow_test_data("avro/repeated_no_annotation.avro");
        let in_file = File::open(&path).expect("open avro/repeated_no_annotation.avro");
        let reader = ReaderBuilder::new()
            .build(BufReader::new(in_file))
            .expect("build reader for repeated_no_annotation.avro");
        let in_schema = reader.schema();
        let in_batches = reader.collect::<Result<Vec<_>, _>>()?;
        let original =
            arrow::compute::concat_batches(&in_schema, &in_batches).expect("concat input");
        let mut writer = AvroWriter::new(Vec::<u8>::new(), original.schema().as_ref().clone())?;
        writer.write(&original)?;
        writer.finish()?;
        let bytes = writer.into_inner();
        let rt_reader = ReaderBuilder::new()
            .build(Cursor::new(bytes))
            .expect("build reader for round-trip buffer");
        let rt_schema = rt_reader.schema();
        let rt_batches = rt_reader.collect::<Result<Vec<_>, _>>()?;
        let round_trip =
            arrow::compute::concat_batches(&rt_schema, &rt_batches).expect("concat round-trip");
        assert_eq!(
            round_trip, original,
            "Round-trip batch mismatch for avro/repeated_no_annotation.avro"
        );
        Ok(())
    }

    #[test]
    fn test_nested_record_type_reuse_roundtrip() -> Result<(), ArrowError> {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("test/data/nested_record_reuse.avro")
            .to_string_lossy()
            .into_owned();
        let in_file = File::open(&path).expect("open avro/nested_record_reuse.avro");
        let reader = ReaderBuilder::new()
            .build(BufReader::new(in_file))
            .expect("build reader for nested_record_reuse.avro");
        let in_schema = reader.schema();
        let input_batches = reader.collect::<Result<Vec<_>, _>>()?;
        let input =
            arrow::compute::concat_batches(&in_schema, &input_batches).expect("concat input");
        let mut writer = AvroWriter::new(Vec::<u8>::new(), in_schema.as_ref().clone())?;
        writer.write(&input)?;
        writer.finish()?;
        let bytes = writer.into_inner();
        let rt_reader = ReaderBuilder::new()
            .build(Cursor::new(bytes))
            .expect("build round_trip reader");
        let rt_schema = rt_reader.schema();
        let rt_batches = rt_reader.collect::<Result<Vec<_>, _>>()?;
        let round_trip =
            arrow::compute::concat_batches(&rt_schema, &rt_batches).expect("concat round_trip");
        assert_eq!(
            round_trip, input,
            "Round-trip batch mismatch for nested_record_reuse.avro"
        );
        Ok(())
    }

    #[test]
    fn test_enum_type_reuse_roundtrip() -> Result<(), ArrowError> {
        let path =
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test/data/enum_reuse.avro");
        let rdr_file = std::fs::File::open(&path).expect("open test/data/enum_reuse.avro");
        let reader = ReaderBuilder::new()
            .build(std::io::BufReader::new(rdr_file))
            .expect("build reader for enum_reuse.avro");
        let in_schema = reader.schema();
        let input_batches = reader.collect::<Result<Vec<_>, _>>()?;
        let original =
            arrow::compute::concat_batches(&in_schema, &input_batches).expect("concat input");
        let mut writer = AvroWriter::new(Vec::<u8>::new(), original.schema().as_ref().clone())?;
        writer.write(&original)?;
        writer.finish()?;
        let bytes = writer.into_inner();
        let rt_reader = ReaderBuilder::new()
            .build(std::io::Cursor::new(bytes))
            .expect("build round_trip reader");
        let rt_schema = rt_reader.schema();
        let rt_batches = rt_reader.collect::<Result<Vec<_>, _>>()?;
        let round_trip =
            arrow::compute::concat_batches(&rt_schema, &rt_batches).expect("concat round_trip");
        assert_eq!(
            round_trip, original,
            "Avro enum type reuse round-trip mismatch"
        );
        Ok(())
    }

    #[test]
    fn comprehensive_e2e_test_roundtrip() -> Result<(), ArrowError> {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("test/data/comprehensive_e2e.avro");
        let rdr_file = File::open(&path).expect("open test/data/comprehensive_e2e.avro");
        let reader = ReaderBuilder::new()
            .build(BufReader::new(rdr_file))
            .expect("build reader for comprehensive_e2e.avro");
        let in_schema = reader.schema();
        let in_batches = reader.collect::<Result<Vec<_>, _>>()?;
        let original =
            arrow::compute::concat_batches(&in_schema, &in_batches).expect("concat input");
        let sink: Vec<u8> = Vec::new();
        let mut writer = AvroWriter::new(sink, original.schema().as_ref().clone())?;
        writer.write(&original)?;
        writer.finish()?;
        let bytes = writer.into_inner();
        let rt_reader = ReaderBuilder::new()
            .build(Cursor::new(bytes))
            .expect("build round-trip reader");
        let rt_schema = rt_reader.schema();
        let rt_batches = rt_reader.collect::<Result<Vec<_>, _>>()?;
        let roundtrip =
            arrow::compute::concat_batches(&rt_schema, &rt_batches).expect("concat roundtrip");
        assert_eq!(
            roundtrip, original,
            "Round-trip batch mismatch for comprehensive_e2e.avro"
        );
        Ok(())
    }

    #[test]
    fn test_roundtrip_new_time_encoders_writer() -> Result<(), ArrowError> {
        let schema = Schema::new(vec![
            Field::new("d32", DataType::Date32, false),
            Field::new("t32_ms", DataType::Time32(TimeUnit::Millisecond), false),
            Field::new("t64_us", DataType::Time64(TimeUnit::Microsecond), false),
            Field::new(
                "ts_ms",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new(
                "ts_us",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new(
                "ts_ns",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
        ]);
        let d32 = Date32Array::from(vec![0, 1, -1]);
        let t32_ms: PrimitiveArray<Time32MillisecondType> =
            vec![0_i32, 12_345_i32, 86_399_999_i32].into();
        let t64_us: PrimitiveArray<Time64MicrosecondType> =
            vec![0_i64, 1_234_567_i64, 86_399_999_999_i64].into();
        let ts_ms: PrimitiveArray<TimestampMillisecondType> =
            vec![0_i64, -1_i64, 1_700_000_000_000_i64].into();
        let ts_us: PrimitiveArray<TimestampMicrosecondType> = vec![0_i64, 1_i64, -1_i64].into();
        let ts_ns: PrimitiveArray<TimestampNanosecondType> = vec![0_i64, 1_i64, -1_i64].into();
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(d32) as ArrayRef,
                Arc::new(t32_ms) as ArrayRef,
                Arc::new(t64_us) as ArrayRef,
                Arc::new(ts_ms) as ArrayRef,
                Arc::new(ts_us) as ArrayRef,
                Arc::new(ts_ns) as ArrayRef,
            ],
        )?;
        let mut writer = AvroWriter::new(Vec::<u8>::new(), schema.clone())?;
        writer.write(&batch)?;
        writer.finish()?;
        let bytes = writer.into_inner();
        let rt_reader = ReaderBuilder::new()
            .build(std::io::Cursor::new(bytes))
            .expect("build reader for round-trip of new time encoders");
        let rt_schema = rt_reader.schema();
        let rt_batches = rt_reader.collect::<Result<Vec<_>, _>>()?;
        let roundtrip =
            arrow::compute::concat_batches(&rt_schema, &rt_batches).expect("concat roundtrip");
        assert_eq!(roundtrip, batch);
        Ok(())
    }
}
