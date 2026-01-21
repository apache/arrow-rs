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

//! `async` API for writing [`RecordBatch`]es to Avro files
//!
//! This module provides async versions of the synchronous Avro writer.
//! See [`crate::writer`] for API details on the synchronous version.
//!
//! # Example
//!
//! ```no_run
//! use std::sync::Arc;
//! use arrow_array::{ArrayRef, Int64Array, RecordBatch};
//! use arrow_schema::{DataType, Field, Schema};
//! use arrow_avro::writer::AsyncAvroWriter;
//! use bytes::Bytes;
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
//! let batch = RecordBatch::try_new(
//!     Arc::new(schema.clone()),
//!     vec![Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef],
//! )?;
//!
//! let mut buffer = Vec::new();
//! let mut writer = AsyncAvroWriter::new(&mut buffer, schema)?;
//! writer.write(&batch).await?;
//! writer.finish().await?;
//!
//! let bytes = buffer.clone();
//! assert!(!bytes.is_empty());
//! # Ok(()) }
//! ```

use crate::compression::CompressionCodec;
use crate::schema::{AvroSchema, FingerprintAlgorithm, FingerprintStrategy, SCHEMA_METADATA_KEY};
use crate::writer::encoder::{RecordEncoder, RecordEncoderBuilder, write_long};
use crate::writer::format::{AvroFormat, AvroOcfFormat, AvroSoeFormat};
use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, Schema};
use bytes::Bytes;
use futures::future::{BoxFuture, FutureExt};
use std::sync::Arc;
use tokio::io::{AsyncWrite, AsyncWriteExt};

/// The asynchronous interface used by [`AsyncWriter`] to write Avro files.
pub trait AsyncFileWriter: Send {
    /// Write the provided bytes to the underlying writer
    fn write(&mut self, bs: Bytes) -> BoxFuture<'_, Result<(), ArrowError>>;

    /// Flush any buffered data and finish the writing process.
    ///
    /// After `complete` returns `Ok(())`, the caller SHOULD not call write again.
    fn complete(&mut self) -> BoxFuture<'_, Result<(), ArrowError>>;
}

impl AsyncFileWriter for Box<dyn AsyncFileWriter + '_> {
    fn write(&mut self, bs: Bytes) -> BoxFuture<'_, Result<(), ArrowError>> {
        self.as_mut().write(bs)
    }

    fn complete(&mut self) -> BoxFuture<'_, Result<(), ArrowError>> {
        self.as_mut().complete()
    }
}

impl<T: AsyncWrite + Unpin + Send> AsyncFileWriter for T {
    fn write(&mut self, bs: Bytes) -> BoxFuture<'_, Result<(), ArrowError>> {
        async move {
            self.write_all(&bs)
                .await
                .map_err(|e| ArrowError::IoError(format!("Error writing bytes: {e}"), e))
        }
        .boxed()
    }

    fn complete(&mut self) -> BoxFuture<'_, Result<(), ArrowError>> {
        async move {
            self.flush()
                .await
                .map_err(|e| ArrowError::IoError(format!("Error flushing: {e}"), e))?;
            self.shutdown()
                .await
                .map_err(|e| ArrowError::IoError(format!("Error closing: {e}"), e))
        }
        .boxed()
    }
}

/// Builder to configure and create an async `AsyncWriter`.
#[derive(Debug, Clone)]
pub struct AsyncWriterBuilder {
    schema: Schema,
    codec: Option<CompressionCodec>,
    capacity: usize,
    fingerprint_strategy: Option<FingerprintStrategy>,
}

impl AsyncWriterBuilder {
    /// Create a new builder with default settings.
    pub fn new(schema: Schema) -> Self {
        Self {
            schema,
            codec: None,
            capacity: 1024,
            fingerprint_strategy: None,
        }
    }

    /// Set the fingerprinting strategy for the stream writer.
    pub fn with_fingerprint_strategy(mut self, strategy: FingerprintStrategy) -> Self {
        self.fingerprint_strategy = Some(strategy);
        self
    }

    /// Change the compression codec.
    pub fn with_compression(mut self, codec: Option<CompressionCodec>) -> Self {
        self.codec = codec;
        self
    }

    /// Sets the capacity for internal buffers.
    pub fn with_capacity(mut self, capacity: usize) -> Self {
        self.capacity = capacity;
        self
    }

    /// Create a new async `AsyncWriter` with specified `AvroFormat`.
    pub async fn build<W, F>(self, mut writer: W) -> Result<AsyncWriter<W, F>, ArrowError>
    where
        W: AsyncFileWriter,
        F: AvroFormat,
    {
        let mut format = F::default();
        let avro_schema = match self.schema.metadata.get(SCHEMA_METADATA_KEY) {
            Some(json) => AvroSchema::new(json.clone()),
            None => AvroSchema::try_from(&self.schema)?,
        };
        let maybe_fingerprint = if F::NEEDS_PREFIX {
            match self.fingerprint_strategy {
                Some(FingerprintStrategy::Id(id)) => Some(crate::schema::Fingerprint::Id(id)),
                Some(FingerprintStrategy::Id64(id)) => Some(crate::schema::Fingerprint::Id64(id)),
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

        // Start the stream (write header, etc.)
        let mut header_buf = Vec::<u8>::with_capacity(256);
        format.start_stream(&mut header_buf, &schema, self.codec)?;
        writer.write(Bytes::from(header_buf)).await?;

        let avro_root = crate::codec::AvroFieldBuilder::new(&avro_schema.schema()?).build()?;
        let encoder = RecordEncoderBuilder::new(&avro_root, schema.as_ref())
            .with_fingerprint(maybe_fingerprint)
            .build()?;

        Ok(AsyncWriter {
            writer,
            schema,
            format,
            compression: self.codec,
            capacity: self.capacity,
            encoder,
        })
    }
}

/// Generic async Avro writer.
///
/// This type is generic over the output async sink (`W`) and the Avro format (`F`).
/// You'll usually use the concrete aliases:
///
/// * **[`AsyncAvroWriter`]** for **OCF** (Object Container File)
/// * **[`AsyncAvroStreamWriter`]** for **SOE** Avro streams
pub struct AsyncWriter<W: AsyncFileWriter, F: AvroFormat> {
    writer: W,
    schema: Arc<Schema>,
    format: F,
    compression: Option<CompressionCodec>,
    capacity: usize,
    encoder: RecordEncoder,
}

/// Alias for async **Object Container File** writer.
pub type AsyncAvroWriter<W> = AsyncWriter<W, AvroOcfFormat>;

/// Alias for async **Single Object Encoding** stream writer.
pub type AsyncAvroStreamWriter<W> = AsyncWriter<W, AvroSoeFormat>;

impl<W: AsyncFileWriter> AsyncAvroWriter<W> {
    /// Create a new async Avro OCF writer.
    pub async fn new(writer: W, schema: Schema) -> Result<Self, ArrowError> {
        AsyncWriterBuilder::new(schema)
            .build::<W, AvroOcfFormat>(writer)
            .await
    }

    /// Return a reference to the 16-byte sync marker generated for this file.
    pub fn sync_marker(&self) -> Option<&[u8; 16]> {
        self.format.sync_marker()
    }
}

impl<W: AsyncFileWriter> AsyncAvroStreamWriter<W> {
    /// Create a new async Single Object Encoding stream writer.
    pub async fn new(writer: W, schema: Schema) -> Result<Self, ArrowError> {
        AsyncWriterBuilder::new(schema)
            .build::<W, AvroSoeFormat>(writer)
            .await
    }
}

impl<W: AsyncFileWriter, F: AvroFormat> AsyncWriter<W, F> {
    /// Write a single [`RecordBatch`].
    pub async fn write(&mut self, batch: &RecordBatch) -> Result<(), ArrowError> {
        if batch.schema().fields() != self.schema.fields() {
            return Err(ArrowError::SchemaError(
                "Schema of RecordBatch differs from Writer schema".to_string(),
            ));
        }

        match self.format.sync_marker() {
            Some(&sync) => self.write_ocf_block(batch, &sync).await,
            None => self.write_stream(batch).await,
        }
    }

    /// Write multiple batches.
    pub async fn write_batches(&mut self, batches: &[&RecordBatch]) -> Result<(), ArrowError> {
        for batch in batches {
            self.write(batch).await?;
        }
        Ok(())
    }

    /// Finish writing and flush all data.
    pub async fn finish(&mut self) -> Result<(), ArrowError> {
        self.writer.complete().await
    }

    /// Consume the writer and return the underlying async sink.
    pub fn into_inner(self) -> W {
        self.writer
    }

    async fn write_ocf_block(
        &mut self,
        batch: &RecordBatch,
        sync: &[u8; 16],
    ) -> Result<(), ArrowError> {
        let mut buf = Vec::<u8>::with_capacity(self.capacity);
        self.encoder.encode(&mut buf, batch)?;
        let encoded = match self.compression {
            Some(codec) => codec.compress(&buf)?,
            None => buf,
        };

        let mut block_buf = Vec::<u8>::new();
        write_long(&mut block_buf, batch.num_rows() as i64)?;
        write_long(&mut block_buf, encoded.len() as i64)?;
        block_buf.extend_from_slice(&encoded);
        block_buf.extend_from_slice(sync);

        self.writer.write(Bytes::from(block_buf)).await
    }

    async fn write_stream(&mut self, batch: &RecordBatch) -> Result<(), ArrowError> {
        let mut buf = Vec::<u8>::with_capacity(self.capacity);
        self.encoder.encode(&mut buf, batch)?;
        self.writer.write(Bytes::from(buf)).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reader::ReaderBuilder;
    use crate::writer::format::AvroOcfFormat;
    use arrow_array::{ArrayRef, Int32Array, Int64Array, StringArray};
    use arrow_schema::{DataType, Field};
    use std::io::Cursor;

    #[tokio::test]
    async fn test_async_avro_writer_ocf() -> Result<(), Box<dyn std::error::Error>> {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
                Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef,
            ],
        )?;

        let mut buffer = Vec::new();
        let mut writer = AsyncAvroWriter::new(&mut buffer, schema).await?;
        writer.write(&batch).await?;
        writer.finish().await?;

        // Read back using sync reader
        let mut reader = ReaderBuilder::new().build(Cursor::new(buffer))?;
        let out = reader.next().unwrap()?;
        assert_eq!(out.num_rows(), 3);

        Ok(())
    }

    #[tokio::test]
    async fn test_async_avro_stream_writer() -> Result<(), Box<dyn std::error::Error>> {
        let schema = Schema::new(vec![Field::new("x", DataType::Int32, false)]);

        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(Int32Array::from(vec![10, 20, 30])) as ArrayRef],
        )?;

        let mut buffer = Vec::new();
        let mut writer = AsyncAvroStreamWriter::new(&mut buffer, schema).await?;
        writer.write(&batch).await?;
        writer.finish().await?;

        assert!(!buffer.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_async_writer_multiple_batches() -> Result<(), Box<dyn std::error::Error>> {
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);

        let batch1 = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef],
        )?;

        let batch2 = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(Int64Array::from(vec![3, 4])) as ArrayRef],
        )?;

        let mut buffer = Vec::new();
        let mut writer = AsyncAvroWriter::new(&mut buffer, schema).await?;
        writer.write_batches(&[&batch1, &batch2]).await?;
        writer.finish().await?;

        let mut reader = ReaderBuilder::new().build(Cursor::new(buffer))?;
        let mut total_rows = 0;
        while let Some(batch) = reader.next() {
            let out = batch?;
            total_rows += out.num_rows();
        }
        assert_eq!(total_rows, 4);

        Ok(())
    }

    #[tokio::test]
    async fn test_async_writer_builder_configuration() -> Result<(), Box<dyn std::error::Error>> {
        let schema = Schema::new(vec![Field::new("x", DataType::Int64, false)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(Int64Array::from(vec![42])) as ArrayRef],
        )?;

        let mut buffer = Vec::new();
        let mut writer = AsyncWriterBuilder::new(schema.clone())
            .with_capacity(2048)
            .build::<_, AvroOcfFormat>(&mut buffer)
            .await?;

        writer.write(&batch).await?;
        writer.finish().await?;

        let mut reader = ReaderBuilder::new().build(Cursor::new(buffer))?;
        let out = reader.next().unwrap()?;
        assert_eq!(out.num_rows(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_async_writer_into_inner() -> Result<(), Box<dyn std::error::Error>> {
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(Int32Array::from(vec![99])) as ArrayRef],
        )?;

        let mut buffer = Vec::new();
        {
            let mut writer = AsyncAvroWriter::new(&mut buffer, schema).await?;
            writer.write(&batch).await?;
            writer.finish().await?;
        }

        assert!(!buffer.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_async_writer_schema_mismatch_error() -> Result<(), Box<dyn std::error::Error>> {
        let schema1 = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let schema2 = Schema::new(vec![Field::new("name", DataType::Utf8, false)]);

        let batch = RecordBatch::try_new(
            Arc::new(schema2.clone()),
            vec![Arc::new(StringArray::from(vec!["test"])) as ArrayRef],
        )?;

        let mut buffer = Vec::new();
        let mut writer = AsyncAvroWriter::new(&mut buffer, schema1).await?;

        let result = writer.write(&batch).await;
        assert!(result.is_err());

        Ok(())
    }

    #[tokio::test]
    #[cfg(feature = "deflate")]
    async fn test_async_writer_with_deflate_compression() -> Result<(), Box<dyn std::error::Error>> {
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef],
        )?;

        let mut buffer = Vec::new();
        let mut writer = AsyncWriterBuilder::new(schema.clone())
            .with_compression(Some(CompressionCodec::Deflate))
            .build::<_, AvroOcfFormat>(&mut buffer)
            .await?;

        writer.write(&batch).await?;
        writer.finish().await?;

        let mut reader = ReaderBuilder::new().build(Cursor::new(buffer))?;
        let out = reader.next().unwrap()?;
        assert_eq!(out.num_rows(), 3);

        Ok(())
    }
}
