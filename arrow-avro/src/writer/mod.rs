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
//! Use this module to serialize Arrow [`arrow_array::RecordBatch`] values into Avro. Three output
//! modes are supported:
//!
//! * **[`crate::writer::AvroWriter`]** — writes an **Object Container File (OCF)**: a self‑describing
//!   file with header (schema JSON and metadata), optional compression, data blocks, and
//!   sync markers. See Avro 1.11.1 "Object Container Files."
//!   <https://avro.apache.org/docs/1.11.1/specification/#object-container-files>
//!
//! * **[`crate::writer::AvroStreamWriter`]** — writes a **Single Object Encoding (SOE) Stream** without
//!   any container framing. This is useful when the schema is known out‑of‑band (i.e.,
//!   via a registry) and you want minimal overhead.
//!
//! * **[`crate::writer::Encoder`]** — a row-by-row encoder that buffers encoded records into a single
//!   contiguous byte buffer and returns per-row [`bytes::Bytes`] slices.
//!   Ideal for publishing individual messages to Kafka, Pulsar, or other message queues
//!   where each message must be a self-contained Avro payload.
//!
//! ## Which writer should you use?
//!
//! | Use Case | Recommended Type |
//! |----------|------------------|
//! | Write an OCF file to disk | [`crate::writer::AvroWriter`] |
//! | Stream records continuously to a file/socket | [`crate::writer::AvroStreamWriter`] |
//! | Publish individual records to Kafka/Pulsar | [`crate::writer::Encoder`] |
//! | Need per-row byte slices for custom framing | [`crate::writer::Encoder`] |
//!
//! ## Per-Record Prefix Formats
//!
//! For [`crate::writer::AvroStreamWriter`] and [`crate::writer::Encoder`], each record is automatically prefixed
//! based on the fingerprint strategy:
//!
//! | Strategy | Prefix | Use Case |
//! |----------|--------|----------|
//! | `FingerprintStrategy::Rabin` (default) | `0xC3 0x01` + 8-byte LE Rabin fingerprint | Standard Avro SOE |
//! | `FingerprintStrategy::Id(id)` | `0x00` + 4-byte BE schema ID | [Confluent Schema Registry] |
//! | `FingerprintStrategy::Id64(id)` | `0x00` + 8-byte BE schema ID | [Apicurio Registry] |
//!
//! [Confluent Schema Registry]: https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format
//! [Apicurio Registry]: https://www.apicur.io/registry/docs/apicurio-registry/1.3.3.Final/getting-started/assembly-using-kafka-client-serdes.html#registry-serdes-types-avro-registry
//!
//! ## Choosing the Avro Schema
//!
//! By default, the writer converts your Arrow schema to Avro (including a top‑level record
//! name). If you already have an Avro schema JSON you want to use verbatim, put it into the
//! Arrow schema metadata under the [`SCHEMA_METADATA_KEY`](crate::schema::SCHEMA_METADATA_KEY)
//! key before constructing the writer. The builder will use that schema instead of generating
//! a new one.
//!
//! ## Compression
//!
//! For OCF ([`crate::writer::AvroWriter`]), you may enable a compression codec via
//! [`crate::writer::WriterBuilder::with_compression`]. The chosen codec is written into the file header
//! and used for subsequent blocks. SOE stream writing ([`crate::writer::AvroStreamWriter`], [`crate::writer::Encoder`])
//! does not apply container‑level compression.
//!
//! # Examples
//!
//! ## Writing an OCF File
//!
//! ```
//! use std::sync::Arc;
//! use arrow_array::{ArrayRef, Int64Array, StringArray, RecordBatch};
//! use arrow_schema::{DataType, Field, Schema};
//! use arrow_avro::writer::AvroWriter;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let schema = Schema::new(vec![
//!     Field::new("id", DataType::Int64, false),
//!     Field::new("name", DataType::Utf8, false),
//! ]);
//!
//! let batch = RecordBatch::try_new(
//!     Arc::new(schema.clone()),
//!     vec![
//!         Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
//!         Arc::new(StringArray::from(vec!["alice", "bob"])) as ArrayRef,
//!     ],
//! )?;
//!
//! let mut writer = AvroWriter::new(Vec::<u8>::new(), schema)?;
//! writer.write(&batch)?;
//! writer.finish()?;
//! let bytes = writer.into_inner();
//! assert!(!bytes.is_empty());
//! # Ok(())
//! # }
//! ```
//!
//! ## Using the Row-by-Row Encoder for Message Queues
//!
//! ```
//! use std::sync::Arc;
//! use arrow_array::{ArrayRef, Int32Array, RecordBatch};
//! use arrow_schema::{DataType, Field, Schema};
//! use arrow_avro::writer::{WriterBuilder, format::AvroSoeFormat};
//! use arrow_avro::schema::FingerprintStrategy;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let schema = Schema::new(vec![Field::new("x", DataType::Int32, false)]);
//! let batch = RecordBatch::try_new(
//!     Arc::new(schema.clone()),
//!     vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef],
//! )?;
//!
//! // Build an Encoder with Confluent wire format (schema ID = 42)
//! let mut encoder = WriterBuilder::new(schema)
//!     .with_fingerprint_strategy(FingerprintStrategy::Id(42))
//!     .build_encoder::<AvroSoeFormat>()?;
//!
//! encoder.encode(&batch)?;
//!
//! // Get the buffered rows (zero-copy views into a single backing buffer)
//! let rows = encoder.flush();
//! assert_eq!(rows.len(), 3);
//!
//! // Each row has Confluent wire format: magic byte + 4-byte schema ID + body
//! for row in rows.iter() {
//!     assert_eq!(row[0], 0x00); // Confluent magic byte
//! }
//! # Ok(())
//! # }
//! ```
//!
//! ---
use crate::codec::AvroFieldBuilder;
use crate::compression::CompressionCodec;
use crate::errors::AvroError;
use crate::schema::{
    AvroSchema, Fingerprint, FingerprintAlgorithm, FingerprintStrategy, SCHEMA_METADATA_KEY,
};
use crate::writer::encoder::{RecordEncoder, RecordEncoderBuilder, write_long};
use crate::writer::format::{AvroFormat, AvroOcfFormat, AvroSoeFormat};
use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use bytes::{Bytes, BytesMut};
use std::io::Write;
use std::sync::Arc;

/// Encodes `RecordBatch` into the Avro binary format.
mod encoder;
/// Logic for different Avro container file formats.
pub mod format;

/// A contiguous set of Avro encoded rows.
///
/// `EncodedRows` stores:
/// - a single backing byte buffer (`bytes::Bytes`)
/// - a `Vec<usize>` of row boundary offsets (length = `rows + 1`)
///
/// This lets callers get per-row payloads as zero-copy `Bytes` slices.
///
/// For compatibility with APIs that require owned `Vec<u8>`, use:
/// `let vecs: Vec<Vec<u8>> = rows.iter().map(|b| b.to_vec()).collect();`
#[derive(Debug, Clone)]
pub struct EncodedRows {
    data: Bytes,
    offsets: Vec<usize>,
}

impl EncodedRows {
    /// Create a new `EncodedRows` from a backing buffer and row boundary offsets.
    ///
    /// `offsets` must have length `rows + 1`, and be monotonically non-decreasing.
    /// The last offset should equal `data.len()`.
    pub fn new(data: Bytes, offsets: Vec<usize>) -> Self {
        Self { data, offsets }
    }

    /// Returns the number of encoded rows stored in this container.
    #[inline]
    pub fn len(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }

    /// Returns `true` if this container holds no encoded rows.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns a reference to the single contiguous backing buffer.
    ///
    /// This buffer contains the payloads of all rows concatenated together.
    ///
    /// # Note
    ///
    /// To access individual row payloads, prefer using [`Self::row`] or [`Self::iter`]
    /// rather than slicing this buffer manually.
    #[inline]
    pub fn bytes(&self) -> &Bytes {
        &self.data
    }

    /// Returns the row boundary offsets.
    ///
    /// The returned slice always has the length `self.len() + 1`. The `n`th row payload
    /// corresponds to `bytes[offsets[n] ... offsets[n+1]]`.
    #[inline]
    pub fn offsets(&self) -> &[usize] {
        &self.offsets
    }

    /// Return the `n`th row as a zero-copy `Bytes` slice.
    ///
    /// # Errors
    ///
    /// Returns an error if `n` is out of bounds or if the internal offsets are invalid
    /// (e.g., offsets are not within the backing buffer).
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use arrow_array::{ArrayRef, Int32Array, RecordBatch};
    /// use arrow_schema::{DataType, Field, Schema};
    /// use arrow_avro::writer::WriterBuilder;
    /// use arrow_avro::writer::format::AvroSoeFormat;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let schema = Schema::new(vec![Field::new("x", DataType::Int32, false)]);
    /// let batch = RecordBatch::try_new(
    ///     Arc::new(schema.clone()),
    ///     vec![Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef],
    /// )?;
    ///
    /// let mut encoder = WriterBuilder::new(schema).build_encoder::<AvroSoeFormat>()?;
    /// encoder.encode(&batch)?;
    /// let rows = encoder.flush();
    ///
    /// assert_eq!(rows.iter().count(), 2);
    /// # Ok(())
    /// # }
    /// ```
    pub fn row(&self, n: usize) -> Result<Bytes, AvroError> {
        if n >= self.len() {
            return Err(AvroError::General(format!(
                "Row index {n} out of bounds for len {}",
                self.len()
            )));
        }
        // SAFETY:
        // self.len() is defined as self.offsets.len().saturating_sub(1).
        // The check `n >= self.len()` above ensures that `n < self.offsets.len() - 1`.
        // Therefore, both `n` and `n + 1` are strictly within the bounds of `self.offsets`.
        let (start, end) = unsafe {
            (
                *self.offsets.get_unchecked(n),
                *self.offsets.get_unchecked(n + 1),
            )
        };
        if start > end || end > self.data.len() {
            return Err(AvroError::General(format!(
                "Invalid row offsets for row {n}: start={start}, end={end}, data_len={}",
                self.data.len()
            )));
        }
        Ok(self.data.slice(start..end))
    }

    /// Iterate over rows as zero-copy `Bytes` slices.
    ///
    /// This iterator is infallible and is intended for the common case where
    /// `EncodedRows` is produced by [`Encoder::flush`], which guarantees valid offsets.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use arrow_array::{ArrayRef, Int32Array, RecordBatch};
    /// use arrow_schema::{DataType, Field, Schema};
    /// use arrow_avro::writer::WriterBuilder;
    /// use arrow_avro::writer::format::AvroSoeFormat;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let schema = Schema::new(vec![Field::new("x", DataType::Int32, false)]);
    /// let batch = RecordBatch::try_new(
    ///     Arc::new(schema.clone()),
    ///     vec![Arc::new(Int32Array::from(vec![10, 20])) as ArrayRef],
    /// )?;
    ///
    /// let mut encoder = WriterBuilder::new(schema).build_encoder::<AvroSoeFormat>()?;
    /// encoder.encode(&batch)?;
    /// let rows = encoder.flush();
    ///
    /// assert_eq!(rows.iter().count(), 2);
    /// # Ok(())
    /// # }
    /// ```
    #[inline]
    pub fn iter(&self) -> impl ExactSizeIterator<Item = Bytes> + '_ {
        self.offsets.windows(2).map(|w| self.data.slice(w[0]..w[1]))
    }
}

/// Builder to configure and create a `Writer`.
#[derive(Debug, Clone)]
pub struct WriterBuilder {
    schema: Schema,
    codec: Option<CompressionCodec>,
    row_capacity: Option<usize>,
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
            row_capacity: None,
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

    /// Sets the expected capacity (in bytes) for internal buffers.
    ///
    /// This is used as a hint to pre-allocate staging buffers for writing.
    pub fn with_capacity(mut self, capacity: usize) -> Self {
        self.capacity = capacity;
        self
    }

    /// Sets the expected byte size for each encoded row.
    ///
    /// This setting affects [`Encoder`] created via [`build_encoder`](Self::build_encoder).
    /// It is used as a hint to reduce reallocations when the typical encoded row size is known.
    pub fn with_row_capacity(mut self, capacity: usize) -> Self {
        self.row_capacity = Some(capacity);
        self
    }

    fn prepare_encoder<F: AvroFormat>(&self) -> Result<(Arc<Schema>, RecordEncoder), AvroError> {
        let avro_schema = match self.schema.metadata.get(SCHEMA_METADATA_KEY) {
            Some(json) => AvroSchema::new(json.clone()),
            None => AvroSchema::try_from(&self.schema)?,
        };
        let maybe_fingerprint = if F::NEEDS_PREFIX {
            match &self.fingerprint_strategy {
                Some(FingerprintStrategy::Id(id)) => Some(Fingerprint::Id(*id)),
                Some(FingerprintStrategy::Id64(id)) => Some(Fingerprint::Id64(*id)),
                Some(strategy) => {
                    Some(avro_schema.fingerprint(FingerprintAlgorithm::from(*strategy))?)
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
        let avro_root = AvroFieldBuilder::new(&avro_schema.schema()?).build()?;
        let encoder = RecordEncoderBuilder::new(&avro_root, schema.as_ref())
            .with_fingerprint(maybe_fingerprint)
            .build()?;
        Ok((schema, encoder))
    }

    /// Build a new [`Encoder`] for the given [`AvroFormat`].
    ///
    /// `Encoder` only supports stream formats (no OCF sync markers). Attempting to build an
    /// encoder with an OCF format (e.g. [`AvroOcfFormat`]) will return an error.
    pub fn build_encoder<F: AvroFormat>(self) -> Result<Encoder, AvroError> {
        if F::default().sync_marker().is_some() {
            return Err(AvroError::InvalidArgument(
                "Encoder only supports stream formats (no OCF header/sync marker)".to_string(),
            ));
        }
        let (schema, encoder) = self.prepare_encoder::<F>()?;
        Ok(Encoder {
            schema,
            encoder,
            row_capacity: self.row_capacity,
            buffer: BytesMut::with_capacity(self.capacity),
            offsets: vec![0],
        })
    }

    /// Build a new [`Writer`] with the specified [`AvroFormat`] and builder options.
    pub fn build<W, F>(self, mut writer: W) -> Result<Writer<W, F>, AvroError>
    where
        W: Write,
        F: AvroFormat,
    {
        let mut format = F::default();
        if format.sync_marker().is_none() && !F::NEEDS_PREFIX {
            return Err(AvroError::InvalidArgument(
                "AvroBinaryFormat is only supported with Encoder, use build_encoder instead"
                    .to_string(),
            ));
        }
        let (schema, encoder) = self.prepare_encoder::<F>()?;
        format.start_stream(&mut writer, &schema, self.codec)?;
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

/// A row-by-row encoder for Avro *stream/message* formats (SOE / registry wire formats / raw binary).
///
/// Unlike [`Writer`], which emits a single continuous byte stream to a [`std::io::Write`] sink,
/// `Encoder` tracks row boundaries during encoding and returns an [`EncodedRows`] containing:
/// - one backing buffer (`Bytes`)
/// - row boundary offsets
///
/// This enables zero-copy per-row payloads (for instance, one Kafka message per Arrow row) without
/// re-encoding or decoding the byte stream to recover record boundaries.
///
/// ### Example
///
/// ```
/// use std::sync::Arc;
/// use arrow_array::{ArrayRef, Int32Array, RecordBatch};
/// use arrow_schema::{DataType, Field, Schema};
/// use arrow_avro::writer::{WriterBuilder, format::AvroSoeFormat};
/// use arrow_avro::schema::FingerprintStrategy;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let schema = Schema::new(vec![Field::new("value", DataType::Int32, false)]);
/// let batch = RecordBatch::try_new(
///     Arc::new(schema.clone()),
///     vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef],
/// )?;
///
/// // Configure the encoder (here: Confluent Wire Format with schema ID 100)
/// let mut encoder = WriterBuilder::new(schema)
///     .with_fingerprint_strategy(FingerprintStrategy::Id(100))
///     .build_encoder::<AvroSoeFormat>()?;
///
/// // Encode the batch
/// encoder.encode(&batch)?;
///
/// // Get the encoded rows
/// let rows = encoder.flush();
///
/// // Convert to owned Vec<u8> payloads (e.g., for a Kafka producer)
/// let payloads: Vec<Vec<u8>> = rows.iter().map(|row| row.to_vec()).collect();
///
/// assert_eq!(payloads.len(), 3);
/// assert_eq!(payloads[0][0], 0x00); // Magic byte
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct Encoder {
    schema: SchemaRef,
    encoder: RecordEncoder,
    row_capacity: Option<usize>,
    buffer: BytesMut,
    offsets: Vec<usize>,
}

impl Encoder {
    /// Serialize one [`RecordBatch`] into the internal buffer.
    pub fn encode(&mut self, batch: &RecordBatch) -> Result<(), AvroError> {
        if batch.schema().fields() != self.schema.fields() {
            return Err(AvroError::SchemaError(
                "Schema of RecordBatch differs from Writer schema".to_string(),
            ));
        }
        self.encoder.encode_rows(
            batch,
            self.row_capacity.unwrap_or(0),
            &mut self.buffer,
            &mut self.offsets,
        )?;
        Ok(())
    }

    /// A convenience method to write a slice of [`RecordBatch`] values.
    pub fn encode_batches(&mut self, batches: &[RecordBatch]) -> Result<(), AvroError> {
        for b in batches {
            self.encode(b)?;
        }
        Ok(())
    }

    /// Drain and return all currently buffered encoded rows.
    ///
    /// The returned [`EncodedRows`] provides per-row payloads as `Bytes` slices.
    pub fn flush(&mut self) -> EncodedRows {
        let data = self.buffer.split().freeze();
        let mut offsets = Vec::with_capacity(self.offsets.len());
        offsets.append(&mut self.offsets);
        self.offsets.push(0);
        EncodedRows::new(data, offsets)
    }

    /// Returns the Arrow schema used by this encoder.
    ///
    /// The returned schema includes metadata with the Avro schema JSON under
    /// the `avro.schema` key.
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Returns the number of encoded rows currently buffered.
    pub fn buffered_len(&self) -> usize {
        self.offsets.len().saturating_sub(1)
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
    schema: SchemaRef,
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
    pub fn new(writer: W, schema: Schema) -> Result<Self, AvroError> {
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
    pub fn new(writer: W, schema: Schema) -> Result<Self, AvroError> {
        WriterBuilder::new(schema).build::<W, AvroSoeFormat>(writer)
    }
}

impl<W: Write, F: AvroFormat> Writer<W, F> {
    /// Serialize one [`RecordBatch`] to the output.
    pub fn write(&mut self, batch: &RecordBatch) -> Result<(), AvroError> {
        if batch.schema().fields() != self.schema.fields() {
            return Err(AvroError::SchemaError(
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
    pub fn write_batches(&mut self, batches: &[&RecordBatch]) -> Result<(), AvroError> {
        for b in batches {
            self.write(b)?;
        }
        Ok(())
    }

    /// Flush remaining buffered data and (for OCF) ensure the header is present.
    pub fn finish(&mut self) -> Result<(), AvroError> {
        self.writer
            .flush()
            .map_err(|e| AvroError::IoError(format!("Error flushing writer: {e}"), e))
    }

    /// Consume the writer, returning the underlying output object.
    pub fn into_inner(self) -> W {
        self.writer
    }

    fn write_ocf_block(&mut self, batch: &RecordBatch, sync: &[u8; 16]) -> Result<(), AvroError> {
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
            .map_err(|e| AvroError::IoError(format!("Error writing Avro block: {e}"), e))?;
        self.writer
            .write_all(sync)
            .map_err(|e| AvroError::IoError(format!("Error writing Avro sync: {e}"), e))?;
        Ok(())
    }

    fn write_stream(&mut self, batch: &RecordBatch) -> Result<(), AvroError> {
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
    use arrow::util::pretty::pretty_format_batches;
    use arrow_array::builder::{Int32Builder, ListBuilder};
    #[cfg(feature = "avro_custom_types")]
    use arrow_array::types::{Int16Type, Int32Type, Int64Type};
    use arrow_array::types::{
        Time32MillisecondType, Time64MicrosecondType, TimestampMicrosecondType,
        TimestampMillisecondType, TimestampNanosecondType,
    };
    use arrow_array::{
        Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Int32Array, Int64Array,
        PrimitiveArray, RecordBatch, StringArray, StructArray, UnionArray,
    };
    #[cfg(feature = "avro_custom_types")]
    use arrow_array::{Int16Array, RunArray};
    #[cfg(not(feature = "avro_custom_types"))]
    use arrow_buffer::IntervalMonthDayNano;
    #[cfg(not(feature = "avro_custom_types"))]
    use arrow_schema::{DataType, Field, Schema};
    #[cfg(feature = "avro_custom_types")]
    use arrow_schema::{DataType, Field, Schema};
    use arrow_schema::{IntervalUnit, UnionMode};
    use bytes::BytesMut;
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
    fn test_stream_writer_writes_prefix_per_row_rt() -> Result<(), AvroError> {
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
    fn test_union_nonzero_type_ids() -> Result<(), AvroError> {
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
        assert!(
            writer.finish().is_ok(),
            "Expected no error from finishing writer"
        );
        Ok(())
    }

    #[test]
    fn test_stream_writer_with_id_fingerprint_rt() -> Result<(), AvroError> {
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
    fn test_stream_writer_with_id64_fingerprint_rt() -> Result<(), AvroError> {
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
    fn test_ocf_writer_generates_header_and_sync() -> Result<(), AvroError> {
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
        assert!(matches!(err, AvroError::SchemaError(_)));
    }

    #[test]
    fn test_write_batches_accumulates_multiple() -> Result<(), AvroError> {
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
    fn test_finish_without_write_adds_header() -> Result<(), AvroError> {
        let buffer = Vec::<u8>::new();
        let mut writer = AvroWriter::new(buffer, make_schema())?;
        writer.finish()?;
        let out = writer.into_inner();
        assert_eq!(&out[..4], b"Obj\x01", "finish() should emit OCF header");
        Ok(())
    }

    #[test]
    fn test_write_long_encodes_zigzag_varint() -> Result<(), AvroError> {
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
    fn test_roundtrip_alltypes_roundtrip_writer() -> Result<(), AvroError> {
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
    fn test_roundtrip_nested_records_writer() -> Result<(), AvroError> {
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
    fn test_roundtrip_nested_lists_writer() -> Result<(), AvroError> {
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
    fn test_round_trip_simple_fixed_ocf() -> Result<(), AvroError> {
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
    fn test_round_trip_duration_and_uuid_ocf() -> Result<(), AvroError> {
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
    fn test_duration_and_uuid_ocf_without_extensions_round_trips_values() -> Result<(), AvroError> {
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
    fn test_nonnullable_impala_roundtrip_writer() -> Result<(), AvroError> {
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
    fn test_roundtrip_decimals_via_writer() -> Result<(), AvroError> {
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
    fn test_named_types_complex_roundtrip() -> Result<(), AvroError> {
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
    fn test_union_roundtrip() -> Result<(), AvroError> {
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
    fn test_enum_roundtrip_uses_reader_fixture() -> Result<(), AvroError> {
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
    fn test_builder_propagates_capacity_to_writer() -> Result<(), AvroError> {
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
    fn test_stream_writer_stores_capacity_direct_writes() -> Result<(), AvroError> {
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
    fn test_roundtrip_duration_logical_types_ocf() -> Result<(), AvroError> {
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
    fn test_run_end_encoded_roundtrip_writer() -> Result<(), AvroError> {
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
    fn test_run_end_encoded_string_values_int16_run_ends_roundtrip_writer() -> Result<(), AvroError>
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
    fn test_run_end_encoded_int64_run_ends_numeric_values_roundtrip_writer() -> Result<(), AvroError>
    {
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
    fn test_run_end_encoded_sliced_roundtrip_writer() -> Result<(), AvroError> {
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
    fn test_run_end_encoded_roundtrip_writer_feature_off() -> Result<(), AvroError> {
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
    -> Result<(), AvroError> {
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
    -> Result<(), AvroError> {
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
    fn test_run_end_encoded_sliced_roundtrip_writer_feature_off() -> Result<(), AvroError> {
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
    fn test_nullable_impala_roundtrip() -> Result<(), AvroError> {
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
    fn test_datapage_v2_roundtrip() -> Result<(), AvroError> {
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
    fn test_single_nan_roundtrip() -> Result<(), AvroError> {
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
    fn test_dict_pages_offset_zero_roundtrip() -> Result<(), AvroError> {
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
    fn test_repeated_no_annotation_roundtrip() -> Result<(), AvroError> {
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
    fn test_nested_record_type_reuse_roundtrip() -> Result<(), AvroError> {
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
    fn test_enum_type_reuse_roundtrip() -> Result<(), AvroError> {
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
    fn comprehensive_e2e_test_roundtrip() -> Result<(), AvroError> {
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
    fn test_roundtrip_new_time_encoders_writer() -> Result<(), AvroError> {
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

    fn make_encoder_schema() -> Schema {
        Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ])
    }

    fn make_encoder_batch(schema: &Schema) -> RecordBatch {
        let a = Int32Array::from(vec![1, 2, 3]);
        let b = Int32Array::from(vec![10, 20, 30]);
        RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(a) as ArrayRef, Arc::new(b) as ArrayRef],
        )
        .expect("failed to build test RecordBatch")
    }

    fn make_real_avro_schema_and_batch() -> Result<(Schema, RecordBatch, AvroSchema), AvroError> {
        let avro_json = r#"
        {
          "type": "record",
          "name": "User",
          "fields": [
            { "name": "id",     "type": "long" },
            { "name": "name",   "type": "string" },
            { "name": "active", "type": "boolean" },
            { "name": "tags",   "type": { "type": "array", "items": "int" } },
            { "name": "opt",    "type": ["null", "string"], "default": null }
          ]
        }"#;
        let avro_schema = AvroSchema::new(avro_json.to_string());
        let mut md = HashMap::new();
        md.insert(
            SCHEMA_METADATA_KEY.to_string(),
            avro_schema.json_string.clone(),
        );
        let item_field = Arc::new(Field::new(
            Field::LIST_FIELD_DEFAULT_NAME,
            DataType::Int32,
            false,
        ));
        let schema = Schema::new_with_metadata(
            vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("active", DataType::Boolean, false),
                Field::new("tags", DataType::List(item_field.clone()), false),
                Field::new("opt", DataType::Utf8, true),
            ],
            md,
        );
        let id = Int64Array::from(vec![1, 2, 3]);
        let name = StringArray::from(vec!["alice", "bob", "carol"]);
        let active = BooleanArray::from(vec![true, false, true]);
        let mut tags_builder = ListBuilder::new(Int32Builder::new()).with_field(item_field);
        tags_builder.values().append_value(1);
        tags_builder.values().append_value(2);
        tags_builder.append(true);
        tags_builder.append(true);
        tags_builder.values().append_value(3);
        tags_builder.append(true);
        let tags = tags_builder.finish();
        let opt = StringArray::from(vec![Some("x"), None, Some("z")]);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(id) as ArrayRef,
                Arc::new(name) as ArrayRef,
                Arc::new(active) as ArrayRef,
                Arc::new(tags) as ArrayRef,
                Arc::new(opt) as ArrayRef,
            ],
        )?;
        Ok((schema, batch, avro_schema))
    }

    #[test]
    fn test_row_writer_matches_stream_writer_soe() -> Result<(), AvroError> {
        let schema = make_encoder_schema();
        let batch = make_encoder_batch(&schema);
        let mut stream = AvroStreamWriter::new(Vec::<u8>::new(), schema.clone())?;
        stream.write(&batch)?;
        stream.finish()?;
        let stream_bytes = stream.into_inner();
        let mut row_writer = WriterBuilder::new(schema).build_encoder::<AvroSoeFormat>()?;
        row_writer.encode(&batch)?;
        let rows = row_writer.flush();
        let row_bytes: Vec<u8> = rows.bytes().to_vec();
        assert_eq!(stream_bytes, row_bytes);
        Ok(())
    }

    #[test]
    fn test_row_writer_flush_clears_buffer() -> Result<(), AvroError> {
        let schema = make_encoder_schema();
        let batch = make_encoder_batch(&schema);
        let mut row_writer = WriterBuilder::new(schema).build_encoder::<AvroSoeFormat>()?;
        row_writer.encode(&batch)?;
        assert_eq!(row_writer.buffered_len(), batch.num_rows());
        let out1 = row_writer.flush();
        assert_eq!(out1.len(), batch.num_rows());
        assert_eq!(row_writer.buffered_len(), 0);
        let out2 = row_writer.flush();
        assert_eq!(out2.len(), 0);
        Ok(())
    }

    #[test]
    fn test_row_writer_roundtrip_decoder_soe_real_avro_data() -> Result<(), AvroError> {
        let (schema, batch, avro_schema) = make_real_avro_schema_and_batch()?;
        let mut store = SchemaStore::new();
        store.register(avro_schema.clone())?;
        let mut row_writer = WriterBuilder::new(schema).build_encoder::<AvroSoeFormat>()?;
        row_writer.encode(&batch)?;
        let rows = row_writer.flush();
        let mut decoder = ReaderBuilder::new()
            .with_writer_schema_store(store)
            .with_batch_size(1024)
            .build_decoder()?;
        for row in rows.iter() {
            let consumed = decoder.decode(row.as_ref())?;
            assert_eq!(
                consumed,
                row.len(),
                "decoder should consume the full row frame"
            );
        }
        let out = decoder.flush()?.expect("decoded batch");
        let expected = pretty_format_batches(std::slice::from_ref(&batch))?.to_string();
        let actual = pretty_format_batches(&[out])?.to_string();
        assert_eq!(expected, actual);
        Ok(())
    }

    #[test]
    fn test_row_writer_roundtrip_decoder_soe_streaming_chunks() -> Result<(), AvroError> {
        let (schema, batch, avro_schema) = make_real_avro_schema_and_batch()?;
        let mut store = SchemaStore::new();
        store.register(avro_schema.clone())?;
        let mut row_writer = WriterBuilder::new(schema).build_encoder::<AvroSoeFormat>()?;
        row_writer.encode(&batch)?;
        let rows = row_writer.flush();
        // Build a contiguous stream and frame boundaries (prefix sums) from EncodedRows.
        let mut stream: Vec<u8> = Vec::new();
        let mut boundaries: Vec<usize> = Vec::with_capacity(rows.len() + 1);
        boundaries.push(0usize);
        for row in rows.iter() {
            stream.extend_from_slice(row.as_ref());
            boundaries.push(stream.len());
        }
        let mut decoder = ReaderBuilder::new()
            .with_writer_schema_store(store)
            .with_batch_size(1024)
            .build_decoder()?;
        let mut buffered = BytesMut::new();
        let chunk_rows = [1usize, 2, 3, 1, 4, 2];
        let mut row_idx = 0usize;
        let mut i = 0usize;
        let n_rows = rows.len();
        while row_idx < n_rows {
            let take = chunk_rows[i % chunk_rows.len()];
            i += 1;
            let end_row = (row_idx + take).min(n_rows);
            let byte_start = boundaries[row_idx];
            let byte_end = boundaries[end_row];
            buffered.extend_from_slice(&stream[byte_start..byte_end]);
            loop {
                let consumed = decoder.decode(&buffered)?;
                if consumed == 0 {
                    break;
                }
                let _ = buffered.split_to(consumed);
            }
            assert!(
                buffered.is_empty(),
                "expected decoder to consume the entire frame-aligned chunk"
            );
            row_idx = end_row;
        }
        let out = decoder.flush()?.expect("decoded batch");
        let expected = pretty_format_batches(std::slice::from_ref(&batch))?.to_string();
        let actual = pretty_format_batches(&[out])?.to_string();
        assert_eq!(expected, actual);
        Ok(())
    }

    #[test]
    fn test_row_writer_roundtrip_decoder_confluent_wire_format_id() -> Result<(), AvroError> {
        let (schema, batch, avro_schema) = make_real_avro_schema_and_batch()?;
        let schema_id: u32 = 42;
        let mut store = SchemaStore::new_with_type(FingerprintAlgorithm::Id);
        store.set(Fingerprint::Id(schema_id), avro_schema.clone())?;
        let mut row_writer = WriterBuilder::new(schema)
            .with_fingerprint_strategy(FingerprintStrategy::Id(schema_id))
            .build_encoder::<AvroSoeFormat>()?;
        row_writer.encode(&batch)?;
        let rows = row_writer.flush();
        let mut decoder = ReaderBuilder::new()
            .with_writer_schema_store(store)
            .with_batch_size(1024)
            .build_decoder()?;
        for row in rows.iter() {
            let consumed = decoder.decode(row.as_ref())?;
            assert_eq!(consumed, row.len());
        }
        let out = decoder.flush()?.expect("decoded batch");
        let expected = pretty_format_batches(std::slice::from_ref(&batch))?.to_string();
        let actual = pretty_format_batches(&[out])?.to_string();
        assert_eq!(expected, actual);
        Ok(())
    }
    #[test]
    fn test_encoder_encode_batches_flush_and_encoded_rows_methods_with_avro_binary_format()
    -> Result<(), AvroError> {
        use crate::writer::format::AvroBinaryFormat;
        use arrow_array::{ArrayRef, Int32Array, RecordBatch};
        use arrow_schema::{DataType, Field, Schema};
        use std::sync::Arc;
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]);
        let schema_ref = Arc::new(schema.clone());
        let batch1 = RecordBatch::try_new(
            schema_ref.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
                Arc::new(Int32Array::from(vec![10, 20, 30])) as ArrayRef,
            ],
        )?;
        let batch2 = RecordBatch::try_new(
            schema_ref,
            vec![
                Arc::new(Int32Array::from(vec![4, 5])) as ArrayRef,
                Arc::new(Int32Array::from(vec![40, 50])) as ArrayRef,
            ],
        )?;
        let mut encoder = WriterBuilder::new(schema).build_encoder::<AvroBinaryFormat>()?;
        let empty = Encoder::flush(&mut encoder);
        assert_eq!(EncodedRows::len(&empty), 0);
        assert!(EncodedRows::is_empty(&empty));
        assert_eq!(EncodedRows::bytes(&empty).as_ref(), &[] as &[u8]);
        assert_eq!(EncodedRows::offsets(&empty), &[0usize]);
        assert_eq!(EncodedRows::iter(&empty).count(), 0);
        let empty_vecs: Vec<Vec<u8>> = empty.iter().map(|b| b.to_vec()).collect();
        assert!(empty_vecs.is_empty());
        let batches = vec![batch1, batch2];
        Encoder::encode_batches(&mut encoder, &batches)?;
        assert_eq!(encoder.buffered_len(), 5);
        let rows = Encoder::flush(&mut encoder);
        assert_eq!(
            encoder.buffered_len(),
            0,
            "Encoder::flush should reset the internal offsets"
        );
        assert_eq!(EncodedRows::len(&rows), 5);
        assert!(!EncodedRows::is_empty(&rows));
        let expected_offsets: &[usize] = &[0, 2, 4, 6, 8, 10];
        assert_eq!(EncodedRows::offsets(&rows), expected_offsets);
        let expected_rows: Vec<Vec<u8>> = vec![
            vec![2, 20],
            vec![4, 40],
            vec![6, 60],
            vec![8, 80],
            vec![10, 100],
        ];
        let expected_stream: Vec<u8> = expected_rows.concat();
        assert_eq!(
            EncodedRows::bytes(&rows).as_ref(),
            expected_stream.as_slice()
        );
        for (i, expected) in expected_rows.iter().enumerate() {
            assert_eq!(EncodedRows::row(&rows, i)?.as_ref(), expected.as_slice());
        }
        let iter_rows: Vec<Vec<u8>> = EncodedRows::iter(&rows).map(|b| b.to_vec()).collect();
        assert_eq!(iter_rows, expected_rows);
        let recreated = EncodedRows::new(
            EncodedRows::bytes(&rows).clone(),
            EncodedRows::offsets(&rows).to_vec(),
        );
        assert_eq!(EncodedRows::len(&recreated), EncodedRows::len(&rows));
        assert_eq!(EncodedRows::bytes(&recreated), EncodedRows::bytes(&rows));
        assert_eq!(
            EncodedRows::offsets(&recreated),
            EncodedRows::offsets(&rows)
        );
        let rec_vecs: Vec<Vec<u8>> = recreated.iter().map(|b| b.to_vec()).collect();
        assert_eq!(rec_vecs, iter_rows);
        let empty_again = Encoder::flush(&mut encoder);
        assert!(EncodedRows::is_empty(&empty_again));
        Ok(())
    }

    #[test]
    fn test_writer_builder_build_rejects_avro_binary_format() {
        use crate::writer::format::AvroBinaryFormat;
        use arrow_schema::{DataType, Field, Schema};
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let err = WriterBuilder::new(schema)
            .build::<_, AvroBinaryFormat>(Vec::<u8>::new())
            .unwrap_err();
        match err {
            AvroError::InvalidArgument(msg) => assert_eq!(
                msg,
                "AvroBinaryFormat is only supported with Encoder, use build_encoder instead"
            ),
            other => panic!("expected InvalidArgumentError, got {:?}", other),
        }
    }
    #[test]
    fn test_row_encoder_avro_binary_format_roundtrip_decoder_with_soe_framing()
    -> Result<(), AvroError> {
        use crate::writer::format::AvroBinaryFormat;
        let (schema, batch, avro_schema) = make_real_avro_schema_and_batch()?;
        let batches: Vec<RecordBatch> = vec![batch.clone(), batch.slice(1, 2)];
        let expected = arrow::compute::concat_batches(&batch.schema(), &batches)?;
        let mut binary_encoder =
            WriterBuilder::new(schema.clone()).build_encoder::<AvroBinaryFormat>()?;
        binary_encoder.encode_batches(&batches)?;
        let binary_rows = binary_encoder.flush();
        assert_eq!(
            binary_rows.len(),
            expected.num_rows(),
            "binary encoder row count mismatch"
        );
        let mut soe_encoder = WriterBuilder::new(schema).build_encoder::<AvroSoeFormat>()?;
        soe_encoder.encode_batches(&batches)?;
        let soe_rows = soe_encoder.flush();
        assert_eq!(
            soe_rows.len(),
            binary_rows.len(),
            "SOE vs binary row count mismatch"
        );
        let mut store = SchemaStore::new(); // Rabin by default
        let fp = store.register(avro_schema)?;
        let fp_le_bytes = match fp {
            Fingerprint::Rabin(v) => v.to_le_bytes(),
            other => panic!("expected Rabin fingerprint from SchemaStore::new(), got {other:?}"),
        };
        const SOE_MAGIC: [u8; 2] = [0xC3, 0x01];
        const SOE_PREFIX_LEN: usize = 2 + 8;
        for i in 0..binary_rows.len() {
            let body = binary_rows.row(i)?;
            let soe = soe_rows.row(i)?;
            assert!(
                soe.len() >= SOE_PREFIX_LEN,
                "expected SOE row to include prefix"
            );
            assert_eq!(&soe.as_ref()[..2], &SOE_MAGIC);
            assert_eq!(&soe.as_ref()[2..SOE_PREFIX_LEN], &fp_le_bytes);
            assert_eq!(
                &soe.as_ref()[SOE_PREFIX_LEN..],
                body.as_ref(),
                "SOE body bytes differ from AvroBinaryFormat body bytes (row {i})"
            );
        }
        let mut decoder = ReaderBuilder::new()
            .with_writer_schema_store(store)
            .with_batch_size(1024)
            .build_decoder()?;
        for body in binary_rows.iter() {
            let mut framed = Vec::with_capacity(SOE_PREFIX_LEN + body.len());
            framed.extend_from_slice(&SOE_MAGIC);
            framed.extend_from_slice(&fp_le_bytes);
            framed.extend_from_slice(body.as_ref());
            let consumed = decoder.decode(&framed)?;
            assert_eq!(
                consumed,
                framed.len(),
                "decoder should consume the full SOE-framed message"
            );
        }
        let out = decoder.flush()?.expect("expected a decoded RecordBatch");
        let expected_str = pretty_format_batches(&[expected])?.to_string();
        let actual_str = pretty_format_batches(&[out])?.to_string();
        assert_eq!(expected_str, actual_str);
        Ok(())
    }

    #[test]
    fn test_row_encoder_avro_binary_format_roundtrip_decoder_streaming_chunks()
    -> Result<(), AvroError> {
        use crate::writer::format::AvroBinaryFormat;
        let (schema, batch, avro_schema) = make_real_avro_schema_and_batch()?;
        let mut encoder = WriterBuilder::new(schema).build_encoder::<AvroBinaryFormat>()?;
        encoder.encode(&batch)?;
        let rows = encoder.flush();
        let mut store = SchemaStore::new();
        let fp = store.register(avro_schema)?;
        let fp_le_bytes = match fp {
            Fingerprint::Rabin(v) => v.to_le_bytes(),
            other => panic!("expected Rabin fingerprint from SchemaStore::new(), got {other:?}"),
        };
        const SOE_MAGIC: [u8; 2] = [0xC3, 0x01];
        const SOE_PREFIX_LEN: usize = 2 + 8;
        let mut stream: Vec<u8> = Vec::new();
        for body in rows.iter() {
            let msg_len: u32 = (SOE_PREFIX_LEN + body.len())
                .try_into()
                .expect("message length must fit in u32");
            stream.extend_from_slice(&msg_len.to_le_bytes());
            stream.extend_from_slice(&SOE_MAGIC);
            stream.extend_from_slice(&fp_le_bytes);
            stream.extend_from_slice(body.as_ref());
        }
        let mut decoder = ReaderBuilder::new()
            .with_writer_schema_store(store)
            .with_batch_size(1024)
            .build_decoder()?;
        let chunk_sizes = [1usize, 2, 3, 5, 8, 13, 21, 34];
        let mut pos = 0usize;
        let mut i = 0usize;
        let mut buffered = BytesMut::new();
        let mut decoded_frames = 0usize;
        while pos < stream.len() {
            let take = chunk_sizes[i % chunk_sizes.len()];
            i += 1;
            let end = (pos + take).min(stream.len());
            buffered.extend_from_slice(&stream[pos..end]);
            pos = end;
            loop {
                if buffered.len() < 4 {
                    break;
                }
                let msg_len =
                    u32::from_le_bytes([buffered[0], buffered[1], buffered[2], buffered[3]])
                        as usize;
                if buffered.len() < 4 + msg_len {
                    break;
                }
                let frame = buffered.split_to(4 + msg_len);
                let payload = &frame[4..];
                let consumed = decoder.decode(payload)?;
                assert_eq!(
                    consumed,
                    payload.len(),
                    "decoder should consume the full SOE-framed message"
                );

                decoded_frames += 1;
            }
        }
        assert!(
            buffered.is_empty(),
            "expected transport framer to consume all bytes; leftover = {}",
            buffered.len()
        );
        assert_eq!(
            decoded_frames,
            rows.len(),
            "expected to decode exactly one frame per encoded row"
        );
        let out = decoder.flush()?.expect("expected decoded RecordBatch");
        let expected_str = pretty_format_batches(std::slice::from_ref(&batch))?.to_string();
        let actual_str = pretty_format_batches(&[out])?.to_string();
        assert_eq!(expected_str, actual_str);
        Ok(())
    }

    /// Helper to roundtrip a RecordBatch through OCF writer/reader
    fn roundtrip_ocf(batch: &RecordBatch) -> Result<RecordBatch, AvroError> {
        let schema = batch.schema();
        let mut buffer = Vec::<u8>::new();
        let mut writer = AvroWriter::new(&mut buffer, schema.as_ref().clone())?;
        writer.write(batch)?;
        writer.finish()?;
        drop(writer);
        let reader = ReaderBuilder::new()
            .build(Cursor::new(buffer))
            .expect("build reader for roundtrip OCF");
        let rt_batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>()?;
        let rt_schema = rt_batches.first().map(|b| b.schema()).unwrap_or(schema);
        Ok(arrow::compute::concat_batches(&rt_schema, &rt_batches).expect("concat roundtrip"))
    }

    #[cfg(feature = "avro_custom_types")]
    #[test]
    fn test_roundtrip_int8_custom_types() -> Result<(), AvroError> {
        use arrow_array::Int8Array;

        let schema = Schema::new(vec![Field::new("val", DataType::Int8, true)]);
        let values: Vec<Option<i8>> = vec![
            Some(i8::MIN),
            Some(-1),
            Some(0),
            None,
            Some(1),
            Some(i8::MAX),
        ];
        let array = Int8Array::from(values.clone());
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array) as ArrayRef])?;

        let roundtrip = roundtrip_ocf(&batch)?;

        // With avro_custom_types: expect exact Int8 type
        assert_eq!(
            roundtrip.schema().field(0).data_type(),
            &DataType::Int8,
            "Expected Int8 type with avro_custom_types enabled"
        );
        let got = roundtrip
            .column(0)
            .as_any()
            .downcast_ref::<Int8Array>()
            .expect("Int8Array");
        assert_eq!(got, &Int8Array::from(values));
        Ok(())
    }

    #[cfg(not(feature = "avro_custom_types"))]
    #[test]
    fn test_roundtrip_int8_no_custom_widens_to_int32() -> Result<(), AvroError> {
        use arrow_array::Int8Array;

        let schema = Schema::new(vec![Field::new("val", DataType::Int8, true)]);
        let values: Vec<Option<i8>> = vec![
            Some(i8::MIN),
            Some(-1),
            Some(0),
            None,
            Some(1),
            Some(i8::MAX),
        ];
        let array = Int8Array::from(values.clone());
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array) as ArrayRef])?;

        let roundtrip = roundtrip_ocf(&batch)?;

        // Without avro_custom_types: expect Int32 (widened)
        assert_eq!(
            roundtrip.schema().field(0).data_type(),
            &DataType::Int32,
            "Expected Int32 type without avro_custom_types (widened from Int8)"
        );
        let got = roundtrip
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Int32Array");
        let expected: Vec<Option<i32>> = values.iter().map(|v| v.map(|x| x as i32)).collect();
        assert_eq!(got, &Int32Array::from(expected));
        Ok(())
    }

    #[cfg(feature = "avro_custom_types")]
    #[test]
    fn test_roundtrip_int16_custom_types() -> Result<(), AvroError> {
        let schema = Schema::new(vec![Field::new("val", DataType::Int16, true)]);
        let values: Vec<Option<i16>> = vec![
            Some(i16::MIN),
            Some(-1),
            Some(0),
            None,
            Some(1),
            Some(i16::MAX),
        ];
        let array = Int16Array::from(values.clone());
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array) as ArrayRef])?;

        let roundtrip = roundtrip_ocf(&batch)?;

        assert_eq!(
            roundtrip.schema().field(0).data_type(),
            &DataType::Int16,
            "Expected Int16 type with avro_custom_types enabled"
        );
        let got = roundtrip
            .column(0)
            .as_any()
            .downcast_ref::<Int16Array>()
            .expect("Int16Array");
        assert_eq!(got, &Int16Array::from(values));
        Ok(())
    }

    #[cfg(not(feature = "avro_custom_types"))]
    #[test]
    fn test_roundtrip_int16_no_custom_widens_to_int32() -> Result<(), AvroError> {
        use arrow_array::Int16Array;

        let schema = Schema::new(vec![Field::new("val", DataType::Int16, true)]);
        let values: Vec<Option<i16>> = vec![
            Some(i16::MIN),
            Some(-1),
            Some(0),
            None,
            Some(1),
            Some(i16::MAX),
        ];
        let array = Int16Array::from(values.clone());
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array) as ArrayRef])?;

        let roundtrip = roundtrip_ocf(&batch)?;

        assert_eq!(
            roundtrip.schema().field(0).data_type(),
            &DataType::Int32,
            "Expected Int32 type without avro_custom_types (widened from Int16)"
        );
        let got = roundtrip
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Int32Array");
        let expected: Vec<Option<i32>> = values.iter().map(|v| v.map(|x| x as i32)).collect();
        assert_eq!(got, &Int32Array::from(expected));
        Ok(())
    }

    #[cfg(feature = "avro_custom_types")]
    #[test]
    fn test_roundtrip_uint8_custom_types() -> Result<(), AvroError> {
        use arrow_array::UInt8Array;

        let schema = Schema::new(vec![Field::new("val", DataType::UInt8, true)]);
        let values: Vec<Option<u8>> = vec![Some(0), Some(1), None, Some(127), Some(u8::MAX)];
        let array = UInt8Array::from(values.clone());
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array) as ArrayRef])?;

        let roundtrip = roundtrip_ocf(&batch)?;

        assert_eq!(
            roundtrip.schema().field(0).data_type(),
            &DataType::UInt8,
            "Expected UInt8 type with avro_custom_types enabled"
        );
        let got = roundtrip
            .column(0)
            .as_any()
            .downcast_ref::<UInt8Array>()
            .expect("UInt8Array");
        assert_eq!(got, &UInt8Array::from(values));
        Ok(())
    }

    #[cfg(not(feature = "avro_custom_types"))]
    #[test]
    fn test_roundtrip_uint8_no_custom_widens_to_int32() -> Result<(), AvroError> {
        use arrow_array::UInt8Array;

        let schema = Schema::new(vec![Field::new("val", DataType::UInt8, true)]);
        let values: Vec<Option<u8>> = vec![Some(0), Some(1), None, Some(127), Some(u8::MAX)];
        let array = UInt8Array::from(values.clone());
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array) as ArrayRef])?;

        let roundtrip = roundtrip_ocf(&batch)?;

        assert_eq!(
            roundtrip.schema().field(0).data_type(),
            &DataType::Int32,
            "Expected Int32 type without avro_custom_types (widened from UInt8)"
        );
        let got = roundtrip
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Int32Array");
        let expected: Vec<Option<i32>> = values.iter().map(|v| v.map(|x| x as i32)).collect();
        assert_eq!(got, &Int32Array::from(expected));
        Ok(())
    }

    #[cfg(feature = "avro_custom_types")]
    #[test]
    fn test_roundtrip_uint16_custom_types() -> Result<(), AvroError> {
        use arrow_array::UInt16Array;

        let schema = Schema::new(vec![Field::new("val", DataType::UInt16, true)]);
        let values: Vec<Option<u16>> = vec![Some(0), Some(1), None, Some(32767), Some(u16::MAX)];
        let array = UInt16Array::from(values.clone());
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array) as ArrayRef])?;

        let roundtrip = roundtrip_ocf(&batch)?;

        assert_eq!(
            roundtrip.schema().field(0).data_type(),
            &DataType::UInt16,
            "Expected UInt16 type with avro_custom_types enabled"
        );
        let got = roundtrip
            .column(0)
            .as_any()
            .downcast_ref::<UInt16Array>()
            .expect("UInt16Array");
        assert_eq!(got, &UInt16Array::from(values));
        Ok(())
    }

    #[cfg(not(feature = "avro_custom_types"))]
    #[test]
    fn test_roundtrip_uint16_no_custom_widens_to_int32() -> Result<(), AvroError> {
        use arrow_array::UInt16Array;

        let schema = Schema::new(vec![Field::new("val", DataType::UInt16, true)]);
        let values: Vec<Option<u16>> = vec![Some(0), Some(1), None, Some(32767), Some(u16::MAX)];
        let array = UInt16Array::from(values.clone());
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array) as ArrayRef])?;

        let roundtrip = roundtrip_ocf(&batch)?;

        assert_eq!(
            roundtrip.schema().field(0).data_type(),
            &DataType::Int32,
            "Expected Int32 type without avro_custom_types (widened from UInt16)"
        );
        let got = roundtrip
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Int32Array");
        let expected: Vec<Option<i32>> = values.iter().map(|v| v.map(|x| x as i32)).collect();
        assert_eq!(got, &Int32Array::from(expected));
        Ok(())
    }

    #[cfg(feature = "avro_custom_types")]
    #[test]
    fn test_roundtrip_uint32_custom_types() -> Result<(), AvroError> {
        use arrow_array::UInt32Array;

        let schema = Schema::new(vec![Field::new("val", DataType::UInt32, true)]);
        let values: Vec<Option<u32>> = vec![
            Some(0),
            Some(1),
            None,
            Some(i32::MAX as u32),
            Some(u32::MAX),
        ];
        let array = UInt32Array::from(values.clone());
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array) as ArrayRef])?;

        let roundtrip = roundtrip_ocf(&batch)?;

        assert_eq!(
            roundtrip.schema().field(0).data_type(),
            &DataType::UInt32,
            "Expected UInt32 type with avro_custom_types enabled"
        );
        let got = roundtrip
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .expect("UInt32Array");
        assert_eq!(got, &UInt32Array::from(values));
        Ok(())
    }

    #[cfg(not(feature = "avro_custom_types"))]
    #[test]
    fn test_roundtrip_uint32_no_custom_widens_to_int64() -> Result<(), AvroError> {
        use arrow_array::UInt32Array;

        let schema = Schema::new(vec![Field::new("val", DataType::UInt32, true)]);
        let values: Vec<Option<u32>> = vec![
            Some(0),
            Some(1),
            None,
            Some(i32::MAX as u32),
            Some(u32::MAX),
        ];
        let array = UInt32Array::from(values.clone());
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array) as ArrayRef])?;

        let roundtrip = roundtrip_ocf(&batch)?;

        assert_eq!(
            roundtrip.schema().field(0).data_type(),
            &DataType::Int64,
            "Expected Int64 type without avro_custom_types (widened from UInt32)"
        );
        let got = roundtrip
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64Array");
        let expected: Vec<Option<i64>> = values.iter().map(|v| v.map(|x| x as i64)).collect();
        assert_eq!(got, &Int64Array::from(expected));
        Ok(())
    }

    #[cfg(feature = "avro_custom_types")]
    #[test]
    fn test_roundtrip_uint64_custom_types() -> Result<(), AvroError> {
        use arrow_array::UInt64Array;

        let schema = Schema::new(vec![Field::new("val", DataType::UInt64, true)]);
        // Include values > i64::MAX to test full u64 range
        let values: Vec<Option<u64>> = vec![
            Some(0),
            Some(1),
            None,
            Some(i64::MAX as u64),
            Some(u64::MAX),
        ];
        let array = UInt64Array::from(values.clone());
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array) as ArrayRef])?;

        let roundtrip = roundtrip_ocf(&batch)?;

        assert_eq!(
            roundtrip.schema().field(0).data_type(),
            &DataType::UInt64,
            "Expected UInt64 type with avro_custom_types enabled"
        );
        let got = roundtrip
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("UInt64Array");
        assert_eq!(got, &UInt64Array::from(values));
        Ok(())
    }

    #[cfg(not(feature = "avro_custom_types"))]
    #[test]
    fn test_roundtrip_uint64_no_custom_widens_to_int64() -> Result<(), AvroError> {
        use arrow_array::UInt64Array;
        let schema = Schema::new(vec![Field::new("val", DataType::UInt64, true)]);
        let values: Vec<Option<u64>> = vec![Some(0), Some(1), None, Some(i64::MAX as u64)];
        let array = UInt64Array::from(values.clone());
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array) as ArrayRef])?;
        let roundtrip = roundtrip_ocf(&batch)?;
        assert_eq!(
            roundtrip.schema().field(0).data_type(),
            &DataType::Int64,
            "Expected Int64 type without avro_custom_types (widened from UInt64)"
        );
        let got = roundtrip
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64Array");
        let expected: Vec<Option<i64>> = values.iter().map(|v| v.map(|x| x as i64)).collect();
        assert_eq!(got, &Int64Array::from(expected));
        Ok(())
    }

    #[cfg(not(feature = "avro_custom_types"))]
    #[test]
    fn test_roundtrip_uint64_overflow_errors_without_custom() {
        use arrow_array::UInt64Array;
        let schema = Schema::new(vec![Field::new("val", DataType::UInt64, false)]);
        let values: Vec<u64> = vec![u64::MAX];
        let array = UInt64Array::from(values);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array) as ArrayRef])
            .expect("create batch");
        let result = roundtrip_ocf(&batch);
        assert!(
            result.is_err(),
            "Expected error when encoding UInt64 > i64::MAX without avro_custom_types"
        );
    }

    #[cfg(feature = "avro_custom_types")]
    #[test]
    fn test_roundtrip_float16_custom_types() -> Result<(), AvroError> {
        use arrow_array::Float16Array;
        use half::f16;
        let schema = Schema::new(vec![Field::new("val", DataType::Float16, true)]);
        let values: Vec<Option<f16>> = vec![
            Some(f16::ZERO),
            Some(f16::ONE),
            None,
            Some(f16::NEG_ONE),
            Some(f16::MAX),
            Some(f16::MIN),
        ];
        let array = Float16Array::from(values.clone());
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array) as ArrayRef])?;
        let roundtrip = roundtrip_ocf(&batch)?;
        assert_eq!(
            roundtrip.schema().field(0).data_type(),
            &DataType::Float16,
            "Expected Float16 type with avro_custom_types enabled"
        );
        let got = roundtrip
            .column(0)
            .as_any()
            .downcast_ref::<Float16Array>()
            .expect("Float16Array");
        assert_eq!(got, &Float16Array::from(values));
        Ok(())
    }

    #[cfg(not(feature = "avro_custom_types"))]
    #[test]
    fn test_roundtrip_float16_no_custom_widens_to_float32() -> Result<(), AvroError> {
        use arrow_array::{Float16Array, Float32Array};
        use half::f16;
        let schema = Schema::new(vec![Field::new("val", DataType::Float16, true)]);
        let values: Vec<Option<f16>> =
            vec![Some(f16::ZERO), Some(f16::ONE), None, Some(f16::NEG_ONE)];
        let array = Float16Array::from(values.clone());
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array) as ArrayRef])?;
        let roundtrip = roundtrip_ocf(&batch)?;
        assert_eq!(
            roundtrip.schema().field(0).data_type(),
            &DataType::Float32,
            "Expected Float32 type without avro_custom_types (widened from Float16)"
        );
        let got = roundtrip
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .expect("Float32Array");
        let expected: Vec<Option<f32>> = values.iter().map(|v| v.map(|x| x.to_f32())).collect();
        assert_eq!(got, &Float32Array::from(expected));
        Ok(())
    }

    #[cfg(feature = "avro_custom_types")]
    #[test]
    fn test_roundtrip_date64_custom_types() -> Result<(), AvroError> {
        use arrow_array::Date64Array;
        let schema = Schema::new(vec![Field::new("val", DataType::Date64, true)]);
        // Date64 is milliseconds since epoch
        let values: Vec<Option<i64>> = vec![
            Some(0),
            Some(86_400_000), // 1 day
            None,
            Some(1_609_459_200_000), // 2021-01-01
        ];
        let array = Date64Array::from(values.clone());
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array) as ArrayRef])?;
        let roundtrip = roundtrip_ocf(&batch)?;
        assert_eq!(
            roundtrip.schema().field(0).data_type(),
            &DataType::Date64,
            "Expected Date64 type with avro_custom_types enabled"
        );
        let got = roundtrip
            .column(0)
            .as_any()
            .downcast_ref::<Date64Array>()
            .expect("Date64Array");
        assert_eq!(got, &Date64Array::from(values));
        Ok(())
    }

    #[cfg(not(feature = "avro_custom_types"))]
    #[test]
    fn test_roundtrip_date64_no_custom_as_timestamp_millis() -> Result<(), AvroError> {
        use arrow_array::{Date64Array, TimestampMillisecondArray};
        let schema = Schema::new(vec![Field::new("val", DataType::Date64, true)]);
        let values: Vec<Option<i64>> =
            vec![Some(0), Some(86_400_000), None, Some(1_609_459_200_000)];
        let array = Date64Array::from(values.clone());
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array) as ArrayRef])?;
        let roundtrip = roundtrip_ocf(&batch)?;
        // Date64 without custom types maps to local-timestamp-millis which reads as Timestamp(Millisecond, None)
        assert_eq!(
            roundtrip.schema().field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Millisecond, None),
            "Expected Timestamp(Millisecond, None) without avro_custom_types"
        );
        let got = roundtrip
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("TimestampMillisecondArray");
        // Values should be identical (both are i64 millis)
        assert_eq!(got, &TimestampMillisecondArray::from(values));
        Ok(())
    }

    #[cfg(feature = "avro_custom_types")]
    #[test]
    fn test_roundtrip_time64_nanosecond_custom_types() -> Result<(), AvroError> {
        use arrow_array::Time64NanosecondArray;
        let schema = Schema::new(vec![Field::new(
            "val",
            DataType::Time64(TimeUnit::Nanosecond),
            true,
        )]);
        let values: Vec<Option<i64>> = vec![
            Some(0),
            Some(1_000_000_000), // 1 second in nanos
            None,
            Some(86_399_999_999_999), // 23:59:59.999999999
        ];
        let array = Time64NanosecondArray::from(values.clone());
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array) as ArrayRef])?;
        let roundtrip = roundtrip_ocf(&batch)?;
        assert_eq!(
            roundtrip.schema().field(0).data_type(),
            &DataType::Time64(TimeUnit::Nanosecond),
            "Expected Time64(Nanosecond) with avro_custom_types enabled"
        );
        let got = roundtrip
            .column(0)
            .as_any()
            .downcast_ref::<Time64NanosecondArray>()
            .expect("Time64NanosecondArray");
        assert_eq!(got, &Time64NanosecondArray::from(values));
        Ok(())
    }

    #[cfg(not(feature = "avro_custom_types"))]
    #[test]
    fn test_roundtrip_time64_nanos_no_custom_truncates_to_micros() -> Result<(), AvroError> {
        use arrow_array::{Time64MicrosecondArray, Time64NanosecondArray};

        let schema = Schema::new(vec![Field::new(
            "val",
            DataType::Time64(TimeUnit::Nanosecond),
            true,
        )]);
        // Use values evenly divisible by 1000 to avoid truncation issues
        let values: Vec<Option<i64>> = vec![
            Some(0),
            Some(1_000_000_000), // 1 second
            None,
            Some(86_399_999_000_000), // 23:59:59.999000
        ];
        let array = Time64NanosecondArray::from(values.clone());
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array) as ArrayRef])?;
        let roundtrip = roundtrip_ocf(&batch)?;
        assert_eq!(
            roundtrip.schema().field(0).data_type(),
            &DataType::Time64(TimeUnit::Microsecond),
            "Expected Time64(Microsecond) without avro_custom_types (truncated from nanoseconds)"
        );
        let got = roundtrip
            .column(0)
            .as_any()
            .downcast_ref::<Time64MicrosecondArray>()
            .expect("Time64MicrosecondArray");
        let expected: Vec<Option<i64>> = values.iter().map(|v| v.map(|x| x / 1000)).collect();
        assert_eq!(got, &Time64MicrosecondArray::from(expected));
        Ok(())
    }

    #[cfg(feature = "avro_custom_types")]
    #[test]
    fn test_roundtrip_time32_second_custom_types() -> Result<(), AvroError> {
        use arrow_array::Time32SecondArray;
        let schema = Schema::new(vec![Field::new(
            "val",
            DataType::Time32(TimeUnit::Second),
            true,
        )]);
        let values: Vec<Option<i32>> = vec![
            Some(0),
            Some(3600), // 1 hour
            None,
            Some(86399), // 23:59:59
        ];
        let array = Time32SecondArray::from(values.clone());
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array) as ArrayRef])?;
        let roundtrip = roundtrip_ocf(&batch)?;
        assert_eq!(
            roundtrip.schema().field(0).data_type(),
            &DataType::Time32(TimeUnit::Second),
            "Expected Time32(Second) with avro_custom_types enabled"
        );
        let got = roundtrip
            .column(0)
            .as_any()
            .downcast_ref::<Time32SecondArray>()
            .expect("Time32SecondArray");
        assert_eq!(got, &Time32SecondArray::from(values));
        Ok(())
    }

    #[cfg(not(feature = "avro_custom_types"))]
    #[test]
    fn test_roundtrip_time32_second_no_custom_scales_to_millis() -> Result<(), AvroError> {
        use arrow_array::{Time32MillisecondArray, Time32SecondArray};
        let schema = Schema::new(vec![Field::new(
            "val",
            DataType::Time32(TimeUnit::Second),
            true,
        )]);
        let values: Vec<Option<i32>> = vec![Some(0), Some(3600), None, Some(86399)];
        let array = Time32SecondArray::from(values.clone());
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array) as ArrayRef])?;
        let roundtrip = roundtrip_ocf(&batch)?;
        assert_eq!(
            roundtrip.schema().field(0).data_type(),
            &DataType::Time32(TimeUnit::Millisecond),
            "Expected Time32(Millisecond) without avro_custom_types (scaled from seconds)"
        );
        let got = roundtrip
            .column(0)
            .as_any()
            .downcast_ref::<Time32MillisecondArray>()
            .expect("Time32MillisecondArray");
        let expected: Vec<Option<i32>> = values.iter().map(|v| v.map(|x| x * 1000)).collect();
        assert_eq!(got, &Time32MillisecondArray::from(expected));
        Ok(())
    }

    #[cfg(feature = "avro_custom_types")]
    #[test]
    fn test_roundtrip_timestamp_second_custom_types() -> Result<(), AvroError> {
        use arrow_array::TimestampSecondArray;
        let schema = Schema::new(vec![Field::new(
            "val",
            DataType::Timestamp(TimeUnit::Second, Some("+00:00".into())),
            true,
        )]);
        let values: Vec<Option<i64>> = vec![
            Some(0),
            Some(1609459200), // 2021-01-01 00:00:00 UTC
            None,
            Some(1735689600), // 2025-01-01 00:00:00 UTC
        ];
        let array = TimestampSecondArray::from(values.clone()).with_timezone("+00:00");
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array) as ArrayRef])?;
        let roundtrip = roundtrip_ocf(&batch)?;
        assert_eq!(
            roundtrip.schema().field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Second, Some("+00:00".into())),
            "Expected Timestamp(Second, UTC) with avro_custom_types enabled"
        );
        let got = roundtrip
            .column(0)
            .as_any()
            .downcast_ref::<TimestampSecondArray>()
            .expect("TimestampSecondArray");
        let expected = TimestampSecondArray::from(values).with_timezone("+00:00");
        assert_eq!(got, &expected);
        Ok(())
    }

    #[cfg(not(feature = "avro_custom_types"))]
    #[test]
    fn test_roundtrip_timestamp_second_no_custom_scales_to_millis() -> Result<(), AvroError> {
        use arrow_array::{TimestampMillisecondArray, TimestampSecondArray};
        let schema = Schema::new(vec![Field::new(
            "val",
            DataType::Timestamp(TimeUnit::Second, Some("+00:00".into())),
            true,
        )]);
        let values: Vec<Option<i64>> = vec![Some(0), Some(1609459200), None, Some(1735689600)];
        let array = TimestampSecondArray::from(values.clone()).with_timezone("+00:00");
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array) as ArrayRef])?;
        let roundtrip = roundtrip_ocf(&batch)?;
        assert_eq!(
            roundtrip.schema().field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Millisecond, Some("+00:00".into())),
            "Expected Timestamp(Millisecond, UTC) without avro_custom_types (scaled from seconds)"
        );
        let got = roundtrip
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("TimestampMillisecondArray");
        let expected: Vec<Option<i64>> = values.iter().map(|v| v.map(|x| x * 1000)).collect();
        let expected_array = TimestampMillisecondArray::from(expected).with_timezone("+00:00");
        assert_eq!(got, &expected_array);
        Ok(())
    }

    #[cfg(feature = "avro_custom_types")]
    #[test]
    fn test_roundtrip_interval_year_month_custom_types() -> Result<(), AvroError> {
        use arrow_array::IntervalYearMonthArray;
        let schema = Schema::new(vec![Field::new(
            "val",
            DataType::Interval(IntervalUnit::YearMonth),
            true,
        )]);
        let values: Vec<Option<i32>> = vec![
            Some(0),
            Some(12), // 1 year
            None,
            Some(-6), // -6 months
            Some(25), // 2 years 1 month
        ];
        let array = IntervalYearMonthArray::from(values.clone());
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array) as ArrayRef])?;
        let roundtrip = roundtrip_ocf(&batch)?;
        assert_eq!(
            roundtrip.schema().field(0).data_type(),
            &DataType::Interval(IntervalUnit::YearMonth),
            "Expected Interval(YearMonth) with avro_custom_types enabled"
        );
        let got = roundtrip
            .column(0)
            .as_any()
            .downcast_ref::<IntervalYearMonthArray>()
            .expect("IntervalYearMonthArray");
        assert_eq!(got, &IntervalYearMonthArray::from(values));
        Ok(())
    }

    #[cfg(not(feature = "avro_custom_types"))]
    #[test]
    fn test_roundtrip_interval_year_month_no_custom() -> Result<(), AvroError> {
        use arrow_array::{IntervalMonthDayNanoArray, IntervalYearMonthArray};
        let schema = Schema::new(vec![Field::new(
            "val",
            DataType::Interval(IntervalUnit::YearMonth),
            true,
        )]);
        // Only non-negative values for standard Avro duration
        let values: Vec<Option<i32>> = vec![Some(0), Some(12), None, Some(25)];
        let array = IntervalYearMonthArray::from(values.clone());
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array) as ArrayRef])?;
        let roundtrip = roundtrip_ocf(&batch)?;
        // Without custom types, reads as Interval(MonthDayNano) via Avro duration
        assert_eq!(
            roundtrip.schema().field(0).data_type(),
            &DataType::Interval(IntervalUnit::MonthDayNano),
            "Expected Interval(MonthDayNano) without avro_custom_types"
        );
        let got = roundtrip
            .column(0)
            .as_any()
            .downcast_ref::<IntervalMonthDayNanoArray>()
            .expect("IntervalMonthDayNanoArray");
        // Convert expected values to MonthDayNano format
        let expected: Vec<Option<IntervalMonthDayNano>> = values
            .iter()
            .map(|v| v.map(|months| IntervalMonthDayNano::new(months, 0, 0)))
            .collect();
        assert_eq!(got, &IntervalMonthDayNanoArray::from(expected));
        Ok(())
    }

    #[cfg(feature = "avro_custom_types")]
    #[test]
    fn test_roundtrip_interval_day_time_custom_types() -> Result<(), AvroError> {
        use arrow_array::IntervalDayTimeArray;
        use arrow_buffer::IntervalDayTime;
        let schema = Schema::new(vec![Field::new(
            "val",
            DataType::Interval(IntervalUnit::DayTime),
            true,
        )]);
        let values: Vec<Option<IntervalDayTime>> = vec![
            Some(IntervalDayTime::new(0, 0)),
            Some(IntervalDayTime::new(1, 1000)), // 1 day, 1 second
            None,
            Some(IntervalDayTime::new(30, 3600000)), // 30 days, 1 hour
        ];
        let array = IntervalDayTimeArray::from(values.clone());
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array) as ArrayRef])?;
        let roundtrip = roundtrip_ocf(&batch)?;
        assert_eq!(
            roundtrip.schema().field(0).data_type(),
            &DataType::Interval(IntervalUnit::DayTime),
            "Expected Interval(DayTime) with avro_custom_types enabled"
        );
        let got = roundtrip
            .column(0)
            .as_any()
            .downcast_ref::<IntervalDayTimeArray>()
            .expect("IntervalDayTimeArray");
        assert_eq!(got, &IntervalDayTimeArray::from(values));
        Ok(())
    }

    #[cfg(not(feature = "avro_custom_types"))]
    #[test]
    fn test_roundtrip_interval_day_time_no_custom() -> Result<(), AvroError> {
        use arrow_array::{IntervalDayTimeArray, IntervalMonthDayNanoArray};
        use arrow_buffer::IntervalDayTime;
        let schema = Schema::new(vec![Field::new(
            "val",
            DataType::Interval(IntervalUnit::DayTime),
            true,
        )]);
        let values: Vec<Option<IntervalDayTime>> = vec![
            Some(IntervalDayTime::new(0, 0)),
            Some(IntervalDayTime::new(1, 1000)),
            None,
            Some(IntervalDayTime::new(30, 3600000)),
        ];
        let array = IntervalDayTimeArray::from(values.clone());
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array) as ArrayRef])?;
        let roundtrip = roundtrip_ocf(&batch)?;
        assert_eq!(
            roundtrip.schema().field(0).data_type(),
            &DataType::Interval(IntervalUnit::MonthDayNano),
            "Expected Interval(MonthDayNano) without avro_custom_types"
        );
        let got = roundtrip
            .column(0)
            .as_any()
            .downcast_ref::<IntervalMonthDayNanoArray>()
            .expect("IntervalMonthDayNanoArray");
        let expected: Vec<Option<IntervalMonthDayNano>> = values
            .iter()
            .map(|v| {
                v.map(|dt| {
                    let nanos = (dt.milliseconds as i64) * 1_000_000;
                    IntervalMonthDayNano::new(0, dt.days, nanos)
                })
            })
            .collect();
        assert_eq!(got, &IntervalMonthDayNanoArray::from(expected));
        Ok(())
    }
}
