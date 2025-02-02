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

//! Contains writer which writes arrow data into parquet data.

use bytes::Bytes;
use std::io::{Read, Write};
use std::iter::Peekable;
use std::slice::Iter;
use std::sync::{Arc, Mutex};
use std::vec::IntoIter;
use thrift::protocol::TCompactOutputProtocol;

use arrow_array::cast::AsArray;
use arrow_array::types::*;
use arrow_array::{ArrayRef, RecordBatch, RecordBatchWriter};
use arrow_schema::{ArrowError, DataType as ArrowDataType, Field, IntervalUnit, SchemaRef};

use super::schema::{add_encoded_arrow_schema_to_metadata, decimal_length_from_precision};

use crate::arrow::arrow_writer::byte_array::ByteArrayEncoder;
use crate::arrow::ArrowSchemaConverter;
use crate::column::page::{CompressedPage, PageWriteSpec, PageWriter};
use crate::column::writer::encoder::ColumnValueEncoder;
use crate::column::writer::{
    get_column_writer, ColumnCloseResult, ColumnWriter, GenericColumnWriter,
};
use crate::data_type::{ByteArray, FixedLenByteArray};
use crate::errors::{ParquetError, Result};
use crate::file::metadata::{KeyValue, RowGroupMetaData};
use crate::file::properties::{WriterProperties, WriterPropertiesPtr};
use crate::file::reader::{ChunkReader, Length};
use crate::file::writer::{SerializedFileWriter, SerializedRowGroupWriter};
use crate::schema::types::{ColumnDescPtr, SchemaDescriptor};
use crate::thrift::TSerializable;
use levels::{calculate_array_levels, ArrayLevels};

mod byte_array;
mod levels;

/// Encodes [`RecordBatch`] to parquet
///
/// Writes Arrow `RecordBatch`es to a Parquet writer. Multiple [`RecordBatch`] will be encoded
/// to the same row group, up to `max_row_group_size` rows. Any remaining rows will be
/// flushed on close, leading the final row group in the output file to potentially
/// contain fewer than `max_row_group_size` rows
///
/// ```
/// # use std::sync::Arc;
/// # use bytes::Bytes;
/// # use arrow_array::{ArrayRef, Int64Array};
/// # use arrow_array::RecordBatch;
/// # use parquet::arrow::arrow_writer::ArrowWriter;
/// # use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
/// let col = Arc::new(Int64Array::from_iter_values([1, 2, 3])) as ArrayRef;
/// let to_write = RecordBatch::try_from_iter([("col", col)]).unwrap();
///
/// let mut buffer = Vec::new();
/// let mut writer = ArrowWriter::try_new(&mut buffer, to_write.schema(), None).unwrap();
/// writer.write(&to_write).unwrap();
/// writer.close().unwrap();
///
/// let mut reader = ParquetRecordBatchReader::try_new(Bytes::from(buffer), 1024).unwrap();
/// let read = reader.next().unwrap().unwrap();
///
/// assert_eq!(to_write, read);
/// ```
///
/// ## Memory Limiting
///
/// The nature of parquet forces buffering of an entire row group before it can
/// be flushed to the underlying writer. Data is mostly buffered in its encoded
/// form, reducing memory usage. However, some data such as dictionary keys or
/// large strings or very nested data may still result in non-trivial memory
/// usage.
///
/// See Also:
/// * [`ArrowWriter::memory_size`]: the current memory usage of the writer.
/// * [`ArrowWriter::in_progress_size`]: Estimated size of the buffered row group,
///
/// Call [`Self::flush`] to trigger an early flush of a row group based on a
/// memory threshold and/or global memory pressure. However,  smaller row groups
/// result in higher metadata overheads, and thus may worsen compression ratios
/// and query performance.
///
/// ```no_run
/// # use std::io::Write;
/// # use arrow_array::RecordBatch;
/// # use parquet::arrow::ArrowWriter;
/// # let mut writer: ArrowWriter<Vec<u8>> = todo!();
/// # let batch: RecordBatch = todo!();
/// writer.write(&batch).unwrap();
/// // Trigger an early flush if anticipated size exceeds 1_000_000
/// if writer.in_progress_size() > 1_000_000 {
///     writer.flush().unwrap();
/// }
/// ```
///
/// ## Type Support
///
/// The writer supports writing all Arrow [`DataType`]s that have a direct mapping to
/// Parquet types including  [`StructArray`] and [`ListArray`].
///
/// The following are not supported:
///
/// * [`IntervalMonthDayNanoArray`]: Parquet does not [support nanosecond intervals].
///
/// [`DataType`]: https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html
/// [`StructArray`]: https://docs.rs/arrow/latest/arrow/array/struct.StructArray.html
/// [`ListArray`]: https://docs.rs/arrow/latest/arrow/array/type.ListArray.html
/// [`IntervalMonthDayNanoArray`]: https://docs.rs/arrow/latest/arrow/array/type.IntervalMonthDayNanoArray.html
/// [support nanosecond intervals]: https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#interval
pub struct ArrowWriter<W: Write> {
    /// Underlying Parquet writer
    writer: SerializedFileWriter<W>,

    /// The in-progress row group if any
    in_progress: Option<ArrowRowGroupWriter>,

    /// A copy of the Arrow schema.
    ///
    /// The schema is used to verify that each record batch written has the correct schema
    arrow_schema: SchemaRef,

    /// The length of arrays to write to each row group
    max_row_group_size: usize,
}

impl<W: Write + Send> std::fmt::Debug for ArrowWriter<W> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let buffered_memory = self.in_progress_size();
        f.debug_struct("ArrowWriter")
            .field("writer", &self.writer)
            .field("in_progress_size", &format_args!("{buffered_memory} bytes"))
            .field("in_progress_rows", &self.in_progress_rows())
            .field("arrow_schema", &self.arrow_schema)
            .field("max_row_group_size", &self.max_row_group_size)
            .finish()
    }
}

impl<W: Write + Send> ArrowWriter<W> {
    /// Try to create a new Arrow writer
    ///
    /// The writer will fail if:
    ///  * a `SerializedFileWriter` cannot be created from the ParquetWriter
    ///  * the Arrow schema contains unsupported datatypes such as Unions
    pub fn try_new(
        writer: W,
        arrow_schema: SchemaRef,
        props: Option<WriterProperties>,
    ) -> Result<Self> {
        let options = ArrowWriterOptions::new().with_properties(props.unwrap_or_default());
        Self::try_new_with_options(writer, arrow_schema, options)
    }

    /// Try to create a new Arrow writer with [`ArrowWriterOptions`].
    ///
    /// The writer will fail if:
    ///  * a `SerializedFileWriter` cannot be created from the ParquetWriter
    ///  * the Arrow schema contains unsupported datatypes such as Unions
    pub fn try_new_with_options(
        writer: W,
        arrow_schema: SchemaRef,
        options: ArrowWriterOptions,
    ) -> Result<Self> {
        let mut props = options.properties;
        let mut converter = ArrowSchemaConverter::new().with_coerce_types(props.coerce_types());
        if let Some(schema_root) = &options.schema_root {
            converter = converter.schema_root(schema_root);
        }
        let schema = converter.convert(&arrow_schema)?;
        if !options.skip_arrow_metadata {
            // add serialized arrow schema
            add_encoded_arrow_schema_to_metadata(&arrow_schema, &mut props);
        }

        let max_row_group_size = props.max_row_group_size();

        let file_writer =
            SerializedFileWriter::new(writer, schema.root_schema_ptr(), Arc::new(props))?;

        Ok(Self {
            writer: file_writer,
            in_progress: None,
            arrow_schema,
            max_row_group_size,
        })
    }

    /// Returns metadata for any flushed row groups
    pub fn flushed_row_groups(&self) -> &[RowGroupMetaData] {
        self.writer.flushed_row_groups()
    }

    /// Estimated memory usage, in bytes, of this `ArrowWriter`
    ///
    /// This estimate is formed bu summing the values of
    /// [`ArrowColumnWriter::memory_size`] all in progress columns.
    pub fn memory_size(&self) -> usize {
        match &self.in_progress {
            Some(in_progress) => in_progress.writers.iter().map(|x| x.memory_size()).sum(),
            None => 0,
        }
    }

    /// Anticipated encoded size of the in progress row group.
    ///
    /// This estimate the row group size after being completely encoded is,
    /// formed by summing the values of
    /// [`ArrowColumnWriter::get_estimated_total_bytes`] for all in progress
    /// columns.
    pub fn in_progress_size(&self) -> usize {
        match &self.in_progress {
            Some(in_progress) => in_progress
                .writers
                .iter()
                .map(|x| x.get_estimated_total_bytes())
                .sum(),
            None => 0,
        }
    }

    /// Returns the number of rows buffered in the in progress row group
    pub fn in_progress_rows(&self) -> usize {
        self.in_progress
            .as_ref()
            .map(|x| x.buffered_rows)
            .unwrap_or_default()
    }

    /// Returns the number of bytes written by this instance
    pub fn bytes_written(&self) -> usize {
        self.writer.bytes_written()
    }

    /// Encodes the provided [`RecordBatch`]
    ///
    /// If this would cause the current row group to exceed [`WriterProperties::max_row_group_size`]
    /// rows, the contents of `batch` will be written to one or more row groups such that all but
    /// the final row group in the file contain [`WriterProperties::max_row_group_size`] rows.
    ///
    /// This will fail if the `batch`'s schema does not match the writer's schema.
    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let in_progress = match &mut self.in_progress {
            Some(in_progress) => in_progress,
            x => x.insert(ArrowRowGroupWriter::new(
                self.writer.schema_descr(),
                self.writer.properties(),
                &self.arrow_schema,
            )?),
        };

        // If would exceed max_row_group_size, split batch
        if in_progress.buffered_rows + batch.num_rows() > self.max_row_group_size {
            let to_write = self.max_row_group_size - in_progress.buffered_rows;
            let a = batch.slice(0, to_write);
            let b = batch.slice(to_write, batch.num_rows() - to_write);
            self.write(&a)?;
            return self.write(&b);
        }

        in_progress.write(batch)?;

        if in_progress.buffered_rows >= self.max_row_group_size {
            self.flush()?
        }
        Ok(())
    }

    /// Flushes all buffered rows into a new row group
    pub fn flush(&mut self) -> Result<()> {
        let in_progress = match self.in_progress.take() {
            Some(in_progress) => in_progress,
            None => return Ok(()),
        };

        let mut row_group_writer = self.writer.next_row_group()?;
        for chunk in in_progress.close()? {
            chunk.append_to_row_group(&mut row_group_writer)?;
        }
        row_group_writer.close()?;
        Ok(())
    }

    /// Additional [`KeyValue`] metadata to be written in addition to those from [`WriterProperties`]
    ///
    /// This method provide a way to append kv_metadata after write RecordBatch
    pub fn append_key_value_metadata(&mut self, kv_metadata: KeyValue) {
        self.writer.append_key_value_metadata(kv_metadata)
    }

    /// Returns a reference to the underlying writer.
    pub fn inner(&self) -> &W {
        self.writer.inner()
    }

    /// Returns a mutable reference to the underlying writer.
    ///
    /// It is inadvisable to directly write to the underlying writer, doing so
    /// will likely result in a corrupt parquet file
    pub fn inner_mut(&mut self) -> &mut W {
        self.writer.inner_mut()
    }

    /// Flushes any outstanding data and returns the underlying writer.
    pub fn into_inner(mut self) -> Result<W> {
        self.flush()?;
        self.writer.into_inner()
    }

    /// Close and finalize the underlying Parquet writer
    ///
    /// Unlike [`Self::close`] this does not consume self
    ///
    /// Attempting to write after calling finish will result in an error
    pub fn finish(&mut self) -> Result<crate::format::FileMetaData> {
        self.flush()?;
        self.writer.finish()
    }

    /// Close and finalize the underlying Parquet writer
    pub fn close(mut self) -> Result<crate::format::FileMetaData> {
        self.finish()
    }
}

impl<W: Write + Send> RecordBatchWriter for ArrowWriter<W> {
    fn write(&mut self, batch: &RecordBatch) -> Result<(), ArrowError> {
        self.write(batch).map_err(|e| e.into())
    }

    fn close(self) -> std::result::Result<(), ArrowError> {
        self.close()?;
        Ok(())
    }
}

/// Arrow-specific configuration settings for writing parquet files.
///
/// See [`ArrowWriter`] for how to configure the writer.
#[derive(Debug, Clone, Default)]
pub struct ArrowWriterOptions {
    properties: WriterProperties,
    skip_arrow_metadata: bool,
    schema_root: Option<String>,
}

impl ArrowWriterOptions {
    /// Creates a new [`ArrowWriterOptions`] with the default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the [`WriterProperties`] for writing parquet files.
    pub fn with_properties(self, properties: WriterProperties) -> Self {
        Self { properties, ..self }
    }

    /// Skip encoding the embedded arrow metadata (defaults to `false`)
    ///
    /// Parquet files generated by the [`ArrowWriter`] contain embedded arrow schema
    /// by default.
    ///
    /// Set `skip_arrow_metadata` to true, to skip encoding the embedded metadata.
    pub fn with_skip_arrow_metadata(self, skip_arrow_metadata: bool) -> Self {
        Self {
            skip_arrow_metadata,
            ..self
        }
    }

    /// Set the name of the root parquet schema element (defaults to `"arrow_schema"`)
    pub fn with_schema_root(self, schema_root: String) -> Self {
        Self {
            schema_root: Some(schema_root),
            ..self
        }
    }
}

/// A single column chunk produced by [`ArrowColumnWriter`]
#[derive(Default)]
struct ArrowColumnChunkData {
    length: usize,
    data: Vec<Bytes>,
}

impl Length for ArrowColumnChunkData {
    fn len(&self) -> u64 {
        self.length as _
    }
}

impl ChunkReader for ArrowColumnChunkData {
    type T = ArrowColumnChunkReader;

    fn get_read(&self, start: u64) -> Result<Self::T> {
        assert_eq!(start, 0); // Assume append_column writes all data in one-shot
        Ok(ArrowColumnChunkReader(
            self.data.clone().into_iter().peekable(),
        ))
    }

    fn get_bytes(&self, _start: u64, _length: usize) -> Result<Bytes> {
        unimplemented!()
    }
}

/// A [`Read`] for [`ArrowColumnChunkData`]
struct ArrowColumnChunkReader(Peekable<IntoIter<Bytes>>);

impl Read for ArrowColumnChunkReader {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        let buffer = loop {
            match self.0.peek_mut() {
                Some(b) if b.is_empty() => {
                    self.0.next();
                    continue;
                }
                Some(b) => break b,
                None => return Ok(0),
            }
        };

        let len = buffer.len().min(out.len());
        let b = buffer.split_to(len);
        out[..len].copy_from_slice(&b);
        Ok(len)
    }
}

/// A shared [`ArrowColumnChunkData`]
///
/// This allows it to be owned by [`ArrowPageWriter`] whilst allowing access via
/// [`ArrowRowGroupWriter`] on flush, without requiring self-referential borrows
type SharedColumnChunk = Arc<Mutex<ArrowColumnChunkData>>;

#[derive(Default)]
struct ArrowPageWriter {
    buffer: SharedColumnChunk,
}

impl PageWriter for ArrowPageWriter {
    fn write_page(&mut self, page: CompressedPage) -> Result<PageWriteSpec> {
        let mut buf = self.buffer.try_lock().unwrap();
        let page_header = page.to_thrift_header();
        let header = {
            let mut header = Vec::with_capacity(1024);
            let mut protocol = TCompactOutputProtocol::new(&mut header);
            page_header.write_to_out_protocol(&mut protocol)?;
            Bytes::from(header)
        };

        let data = page.compressed_page().buffer().clone();
        let compressed_size = data.len() + header.len();

        let mut spec = PageWriteSpec::new();
        spec.page_type = page.page_type();
        spec.num_values = page.num_values();
        spec.uncompressed_size = page.uncompressed_size() + header.len();
        spec.offset = buf.length as u64;
        spec.compressed_size = compressed_size;
        spec.bytes_written = compressed_size as u64;

        buf.length += compressed_size;
        buf.data.push(header);
        buf.data.push(data);

        Ok(spec)
    }

    fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

/// A leaf column that can be encoded by [`ArrowColumnWriter`]
#[derive(Debug)]
pub struct ArrowLeafColumn(ArrayLevels);

/// Computes the [`ArrowLeafColumn`] for a potentially nested [`ArrayRef`]
pub fn compute_leaves(field: &Field, array: &ArrayRef) -> Result<Vec<ArrowLeafColumn>> {
    let levels = calculate_array_levels(array, field)?;
    Ok(levels.into_iter().map(ArrowLeafColumn).collect())
}

/// The data for a single column chunk, see [`ArrowColumnWriter`]
pub struct ArrowColumnChunk {
    data: ArrowColumnChunkData,
    close: ColumnCloseResult,
}

impl std::fmt::Debug for ArrowColumnChunk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArrowColumnChunk")
            .field("length", &self.data.length)
            .finish_non_exhaustive()
    }
}

impl ArrowColumnChunk {
    /// Calls [`SerializedRowGroupWriter::append_column`] with this column's data
    pub fn append_to_row_group<W: Write + Send>(
        self,
        writer: &mut SerializedRowGroupWriter<'_, W>,
    ) -> Result<()> {
        writer.append_column(&self.data, self.close)
    }
}

/// Encodes [`ArrowLeafColumn`] to [`ArrowColumnChunk`]
///
/// Note: This is a low-level interface for applications that require fine-grained control
/// of encoding, see [`ArrowWriter`] for a higher-level interface
///
/// ```
/// // The arrow schema
/// # use std::sync::Arc;
/// # use arrow_array::*;
/// # use arrow_schema::*;
/// # use parquet::arrow::ArrowSchemaConverter;
/// # use parquet::arrow::arrow_writer::{ArrowLeafColumn, compute_leaves, get_column_writers};
/// # use parquet::file::properties::WriterProperties;
/// # use parquet::file::writer::SerializedFileWriter;
/// #
/// let schema = Arc::new(Schema::new(vec![
///     Field::new("i32", DataType::Int32, false),
///     Field::new("f32", DataType::Float32, false),
/// ]));
///
/// // Compute the parquet schema
/// let props = Arc::new(WriterProperties::default());
/// let parquet_schema = ArrowSchemaConverter::new()
///   .with_coerce_types(props.coerce_types())
///   .convert(&schema)
///   .unwrap();
///
/// // Create writers for each of the leaf columns
/// let col_writers = get_column_writers(&parquet_schema, &props, &schema).unwrap();
///
/// // Spawn a worker thread for each column
/// // This is for demonstration purposes, a thread-pool e.g. rayon or tokio, would be better
/// let mut workers: Vec<_> = col_writers
///     .into_iter()
///     .map(|mut col_writer| {
///         let (send, recv) = std::sync::mpsc::channel::<ArrowLeafColumn>();
///         let handle = std::thread::spawn(move || {
///             for col in recv {
///                 col_writer.write(&col)?;
///             }
///             col_writer.close()
///         });
///         (handle, send)
///     })
///     .collect();
///
/// // Create parquet writer
/// let root_schema = parquet_schema.root_schema_ptr();
/// let mut out = Vec::with_capacity(1024); // This could be a File
/// let mut writer = SerializedFileWriter::new(&mut out, root_schema, props.clone()).unwrap();
///
/// // Start row group
/// let mut row_group = writer.next_row_group().unwrap();
///
/// // Columns to encode
/// let to_write = vec![
///     Arc::new(Int32Array::from_iter_values([1, 2, 3])) as _,
///     Arc::new(Float32Array::from_iter_values([1., 45., -1.])) as _,
/// ];
///
/// // Spawn work to encode columns
/// let mut worker_iter = workers.iter_mut();
/// for (arr, field) in to_write.iter().zip(&schema.fields) {
///     for leaves in compute_leaves(field, arr).unwrap() {
///         worker_iter.next().unwrap().1.send(leaves).unwrap();
///     }
/// }
///
/// // Finish up parallel column encoding
/// for (handle, send) in workers {
///     drop(send); // Drop send side to signal termination
///     let chunk = handle.join().unwrap().unwrap();
///     chunk.append_to_row_group(&mut row_group).unwrap();
/// }
/// row_group.close().unwrap();
///
/// let metadata = writer.close().unwrap();
/// assert_eq!(metadata.num_rows, 3);
/// ```
pub struct ArrowColumnWriter {
    writer: ArrowColumnWriterImpl,
    chunk: SharedColumnChunk,
}

impl std::fmt::Debug for ArrowColumnWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArrowColumnWriter").finish_non_exhaustive()
    }
}

enum ArrowColumnWriterImpl {
    ByteArray(GenericColumnWriter<'static, ByteArrayEncoder>),
    Column(ColumnWriter<'static>),
}

impl ArrowColumnWriter {
    /// Write an [`ArrowLeafColumn`]
    pub fn write(&mut self, col: &ArrowLeafColumn) -> Result<()> {
        match &mut self.writer {
            ArrowColumnWriterImpl::Column(c) => {
                write_leaf(c, &col.0)?;
            }
            ArrowColumnWriterImpl::ByteArray(c) => {
                write_primitive(c, col.0.array().as_ref(), &col.0)?;
            }
        }
        Ok(())
    }

    /// Close this column returning the written [`ArrowColumnChunk`]
    pub fn close(self) -> Result<ArrowColumnChunk> {
        let close = match self.writer {
            ArrowColumnWriterImpl::ByteArray(c) => c.close()?,
            ArrowColumnWriterImpl::Column(c) => c.close()?,
        };
        let chunk = Arc::try_unwrap(self.chunk).ok().unwrap();
        let data = chunk.into_inner().unwrap();
        Ok(ArrowColumnChunk { data, close })
    }

    /// Returns the estimated total memory usage by the writer.
    ///
    /// This  [`Self::get_estimated_total_bytes`] this is an estimate
    /// of the current memory usage and not it's anticipated encoded size.
    ///
    /// This includes:
    /// 1. Data buffered in encoded form
    /// 2. Data buffered in un-encoded form (e.g. `usize` dictionary keys)
    ///
    /// This value should be greater than or equal to [`Self::get_estimated_total_bytes`]
    pub fn memory_size(&self) -> usize {
        match &self.writer {
            ArrowColumnWriterImpl::ByteArray(c) => c.memory_size(),
            ArrowColumnWriterImpl::Column(c) => c.memory_size(),
        }
    }

    /// Returns the estimated total encoded bytes for this column writer.
    ///
    /// This includes:
    /// 1. Data buffered in encoded form
    /// 2. An estimate of how large the data buffered in un-encoded form would be once encoded
    ///
    /// This value should be less than or equal to [`Self::memory_size`]
    pub fn get_estimated_total_bytes(&self) -> usize {
        match &self.writer {
            ArrowColumnWriterImpl::ByteArray(c) => c.get_estimated_total_bytes() as _,
            ArrowColumnWriterImpl::Column(c) => c.get_estimated_total_bytes() as _,
        }
    }
}

/// Encodes [`RecordBatch`] to a parquet row group
struct ArrowRowGroupWriter {
    writers: Vec<ArrowColumnWriter>,
    schema: SchemaRef,
    buffered_rows: usize,
}

impl ArrowRowGroupWriter {
    fn new(
        parquet: &SchemaDescriptor,
        props: &WriterPropertiesPtr,
        arrow: &SchemaRef,
    ) -> Result<Self> {
        let writers = get_column_writers(parquet, props, arrow)?;
        Ok(Self {
            writers,
            schema: arrow.clone(),
            buffered_rows: 0,
        })
    }

    fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.buffered_rows += batch.num_rows();
        let mut writers = self.writers.iter_mut();
        for (field, column) in self.schema.fields().iter().zip(batch.columns()) {
            for leaf in compute_leaves(field.as_ref(), column)? {
                writers.next().unwrap().write(&leaf)?
            }
        }
        Ok(())
    }

    fn close(self) -> Result<Vec<ArrowColumnChunk>> {
        self.writers
            .into_iter()
            .map(|writer| writer.close())
            .collect()
    }
}

/// Returns the [`ArrowColumnWriter`] for a given schema
pub fn get_column_writers(
    parquet: &SchemaDescriptor,
    props: &WriterPropertiesPtr,
    arrow: &SchemaRef,
) -> Result<Vec<ArrowColumnWriter>> {
    let mut writers = Vec::with_capacity(arrow.fields.len());
    let mut leaves = parquet.columns().iter();
    for field in &arrow.fields {
        get_arrow_column_writer(field.data_type(), props, &mut leaves, &mut writers)?;
    }
    Ok(writers)
}

/// Gets the [`ArrowColumnWriter`] for the given `data_type`
fn get_arrow_column_writer(
    data_type: &ArrowDataType,
    props: &WriterPropertiesPtr,
    leaves: &mut Iter<'_, ColumnDescPtr>,
    out: &mut Vec<ArrowColumnWriter>,
) -> Result<()> {
    let col = |desc: &ColumnDescPtr| {
        let page_writer = Box::<ArrowPageWriter>::default();
        let chunk = page_writer.buffer.clone();
        let writer = get_column_writer(desc.clone(), props.clone(), page_writer);
        ArrowColumnWriter {
            chunk,
            writer: ArrowColumnWriterImpl::Column(writer),
        }
    };

    let bytes = |desc: &ColumnDescPtr| {
        let page_writer = Box::<ArrowPageWriter>::default();
        let chunk = page_writer.buffer.clone();
        let writer = GenericColumnWriter::new(desc.clone(), props.clone(), page_writer);
        ArrowColumnWriter {
            chunk,
            writer: ArrowColumnWriterImpl::ByteArray(writer),
        }
    };

    match data_type {
        _ if data_type.is_primitive() => out.push(col(leaves.next().unwrap())),
        ArrowDataType::FixedSizeBinary(_) | ArrowDataType::Boolean | ArrowDataType::Null => out.push(col(leaves.next().unwrap())),
        ArrowDataType::LargeBinary
        | ArrowDataType::Binary
        | ArrowDataType::Utf8
        | ArrowDataType::LargeUtf8
        | ArrowDataType::BinaryView
        | ArrowDataType::Utf8View => {
            out.push(bytes(leaves.next().unwrap()))
        }
        ArrowDataType::List(f)
        | ArrowDataType::LargeList(f)
        | ArrowDataType::FixedSizeList(f, _) => {
            get_arrow_column_writer(f.data_type(), props, leaves, out)?
        }
        ArrowDataType::Struct(fields) => {
            for field in fields {
                get_arrow_column_writer(field.data_type(), props, leaves, out)?
            }
        }
        ArrowDataType::Map(f, _) => match f.data_type() {
            ArrowDataType::Struct(f) => {
                get_arrow_column_writer(f[0].data_type(), props, leaves, out)?;
                get_arrow_column_writer(f[1].data_type(), props, leaves, out)?
            }
            _ => unreachable!("invalid map type"),
        }
        ArrowDataType::Dictionary(_, value_type) => match value_type.as_ref() {
            ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 | ArrowDataType::Binary | ArrowDataType::LargeBinary => {
                out.push(bytes(leaves.next().unwrap()))
            }
            ArrowDataType::Utf8View | ArrowDataType::BinaryView => {
                out.push(bytes(leaves.next().unwrap()))
            }
            _ => {
                out.push(col(leaves.next().unwrap()))
            }
        }
       _ => return Err(ParquetError::NYI(
           format!(
               "Attempting to write an Arrow type {data_type:?} to parquet that is not yet implemented"
           )
       ))
    }
    Ok(())
}

fn write_leaf(writer: &mut ColumnWriter<'_>, levels: &ArrayLevels) -> Result<usize> {
    let column = levels.array().as_ref();
    let indices = levels.non_null_indices();
    match writer {
        ColumnWriter::Int32ColumnWriter(ref mut typed) => {
            match column.data_type() {
                ArrowDataType::Date64 => {
                    // If the column is a Date64, we cast it to a Date32, and then interpret that as Int32
                    let array = arrow_cast::cast(column, &ArrowDataType::Date32)?;
                    let array = arrow_cast::cast(&array, &ArrowDataType::Int32)?;

                    let array = array.as_primitive::<Int32Type>();
                    write_primitive(typed, array.values(), levels)
                }
                ArrowDataType::UInt32 => {
                    let values = column.as_primitive::<UInt32Type>().values();
                    // follow C++ implementation and use overflow/reinterpret cast from  u32 to i32 which will map
                    // `(i32::MAX as u32)..u32::MAX` to `i32::MIN..0`
                    let array = values.inner().typed_data::<i32>();
                    write_primitive(typed, array, levels)
                }
                ArrowDataType::Decimal128(_, _) => {
                    // use the int32 to represent the decimal with low precision
                    let array = column
                        .as_primitive::<Decimal128Type>()
                        .unary::<_, Int32Type>(|v| v as i32);
                    write_primitive(typed, array.values(), levels)
                }
                ArrowDataType::Decimal256(_, _) => {
                    // use the int32 to represent the decimal with low precision
                    let array = column
                        .as_primitive::<Decimal256Type>()
                        .unary::<_, Int32Type>(|v| v.as_i128() as i32);
                    write_primitive(typed, array.values(), levels)
                }
                ArrowDataType::Dictionary(_, value_type) => match value_type.as_ref() {
                    ArrowDataType::Decimal128(_, _) => {
                        let array = arrow_cast::cast(column, value_type)?;
                        let array = array
                            .as_primitive::<Decimal128Type>()
                            .unary::<_, Int32Type>(|v| v as i32);
                        write_primitive(typed, array.values(), levels)
                    }
                    ArrowDataType::Decimal256(_, _) => {
                        let array = arrow_cast::cast(column, value_type)?;
                        let array = array
                            .as_primitive::<Decimal256Type>()
                            .unary::<_, Int32Type>(|v| v.as_i128() as i32);
                        write_primitive(typed, array.values(), levels)
                    }
                    _ => {
                        let array = arrow_cast::cast(column, &ArrowDataType::Int32)?;
                        let array = array.as_primitive::<Int32Type>();
                        write_primitive(typed, array.values(), levels)
                    }
                },
                _ => {
                    let array = arrow_cast::cast(column, &ArrowDataType::Int32)?;
                    let array = array.as_primitive::<Int32Type>();
                    write_primitive(typed, array.values(), levels)
                }
            }
        }
        ColumnWriter::BoolColumnWriter(ref mut typed) => {
            let array = column.as_boolean();
            typed.write_batch(
                get_bool_array_slice(array, indices).as_slice(),
                levels.def_levels(),
                levels.rep_levels(),
            )
        }
        ColumnWriter::Int64ColumnWriter(ref mut typed) => {
            match column.data_type() {
                ArrowDataType::Date64 => {
                    let array = arrow_cast::cast(column, &ArrowDataType::Int64)?;

                    let array = array.as_primitive::<Int64Type>();
                    write_primitive(typed, array.values(), levels)
                }
                ArrowDataType::Int64 => {
                    let array = column.as_primitive::<Int64Type>();
                    write_primitive(typed, array.values(), levels)
                }
                ArrowDataType::UInt64 => {
                    let values = column.as_primitive::<UInt64Type>().values();
                    // follow C++ implementation and use overflow/reinterpret cast from  u64 to i64 which will map
                    // `(i64::MAX as u64)..u64::MAX` to `i64::MIN..0`
                    let array = values.inner().typed_data::<i64>();
                    write_primitive(typed, array, levels)
                }
                ArrowDataType::Decimal128(_, _) => {
                    // use the int64 to represent the decimal with low precision
                    let array = column
                        .as_primitive::<Decimal128Type>()
                        .unary::<_, Int64Type>(|v| v as i64);
                    write_primitive(typed, array.values(), levels)
                }
                ArrowDataType::Decimal256(_, _) => {
                    // use the int64 to represent the decimal with low precision
                    let array = column
                        .as_primitive::<Decimal256Type>()
                        .unary::<_, Int64Type>(|v| v.as_i128() as i64);
                    write_primitive(typed, array.values(), levels)
                }
                ArrowDataType::Dictionary(_, value_type) => match value_type.as_ref() {
                    ArrowDataType::Decimal128(_, _) => {
                        let array = arrow_cast::cast(column, value_type)?;
                        let array = array
                            .as_primitive::<Decimal128Type>()
                            .unary::<_, Int64Type>(|v| v as i64);
                        write_primitive(typed, array.values(), levels)
                    }
                    ArrowDataType::Decimal256(_, _) => {
                        let array = arrow_cast::cast(column, value_type)?;
                        let array = array
                            .as_primitive::<Decimal256Type>()
                            .unary::<_, Int64Type>(|v| v.as_i128() as i64);
                        write_primitive(typed, array.values(), levels)
                    }
                    _ => {
                        let array = arrow_cast::cast(column, &ArrowDataType::Int64)?;
                        let array = array.as_primitive::<Int64Type>();
                        write_primitive(typed, array.values(), levels)
                    }
                },
                _ => {
                    let array = arrow_cast::cast(column, &ArrowDataType::Int64)?;
                    let array = array.as_primitive::<Int64Type>();
                    write_primitive(typed, array.values(), levels)
                }
            }
        }
        ColumnWriter::Int96ColumnWriter(ref mut _typed) => {
            unreachable!("Currently unreachable because data type not supported")
        }
        ColumnWriter::FloatColumnWriter(ref mut typed) => {
            let array = column.as_primitive::<Float32Type>();
            write_primitive(typed, array.values(), levels)
        }
        ColumnWriter::DoubleColumnWriter(ref mut typed) => {
            let array = column.as_primitive::<Float64Type>();
            write_primitive(typed, array.values(), levels)
        }
        ColumnWriter::ByteArrayColumnWriter(_) => {
            unreachable!("should use ByteArrayWriter")
        }
        ColumnWriter::FixedLenByteArrayColumnWriter(ref mut typed) => {
            let bytes = match column.data_type() {
                ArrowDataType::Interval(interval_unit) => match interval_unit {
                    IntervalUnit::YearMonth => {
                        let array = column
                            .as_any()
                            .downcast_ref::<arrow_array::IntervalYearMonthArray>()
                            .unwrap();
                        get_interval_ym_array_slice(array, indices)
                    }
                    IntervalUnit::DayTime => {
                        let array = column
                            .as_any()
                            .downcast_ref::<arrow_array::IntervalDayTimeArray>()
                            .unwrap();
                        get_interval_dt_array_slice(array, indices)
                    }
                    _ => {
                        return Err(ParquetError::NYI(
                            format!(
                                "Attempting to write an Arrow interval type {interval_unit:?} to parquet that is not yet implemented"
                            )
                        ));
                    }
                },
                ArrowDataType::FixedSizeBinary(_) => {
                    let array = column
                        .as_any()
                        .downcast_ref::<arrow_array::FixedSizeBinaryArray>()
                        .unwrap();
                    get_fsb_array_slice(array, indices)
                }
                ArrowDataType::Decimal128(_, _) => {
                    let array = column.as_primitive::<Decimal128Type>();
                    get_decimal_128_array_slice(array, indices)
                }
                ArrowDataType::Decimal256(_, _) => {
                    let array = column
                        .as_any()
                        .downcast_ref::<arrow_array::Decimal256Array>()
                        .unwrap();
                    get_decimal_256_array_slice(array, indices)
                }
                ArrowDataType::Float16 => {
                    let array = column.as_primitive::<Float16Type>();
                    get_float_16_array_slice(array, indices)
                }
                _ => {
                    return Err(ParquetError::NYI(
                        "Attempting to write an Arrow type that is not yet implemented".to_string(),
                    ));
                }
            };
            typed.write_batch(bytes.as_slice(), levels.def_levels(), levels.rep_levels())
        }
    }
}

fn write_primitive<E: ColumnValueEncoder>(
    writer: &mut GenericColumnWriter<E>,
    values: &E::Values,
    levels: &ArrayLevels,
) -> Result<usize> {
    writer.write_batch_internal(
        values,
        Some(levels.non_null_indices()),
        levels.def_levels(),
        levels.rep_levels(),
        None,
        None,
        None,
    )
}

fn get_bool_array_slice(array: &arrow_array::BooleanArray, indices: &[usize]) -> Vec<bool> {
    let mut values = Vec::with_capacity(indices.len());
    for i in indices {
        values.push(array.value(*i))
    }
    values
}

/// Returns 12-byte values representing 3 values of months, days and milliseconds (4-bytes each).
/// An Arrow YearMonth interval only stores months, thus only the first 4 bytes are populated.
fn get_interval_ym_array_slice(
    array: &arrow_array::IntervalYearMonthArray,
    indices: &[usize],
) -> Vec<FixedLenByteArray> {
    let mut values = Vec::with_capacity(indices.len());
    for i in indices {
        let mut value = array.value(*i).to_le_bytes().to_vec();
        let mut suffix = vec![0; 8];
        value.append(&mut suffix);
        values.push(FixedLenByteArray::from(ByteArray::from(value)))
    }
    values
}

/// Returns 12-byte values representing 3 values of months, days and milliseconds (4-bytes each).
/// An Arrow DayTime interval only stores days and millis, thus the first 4 bytes are not populated.
fn get_interval_dt_array_slice(
    array: &arrow_array::IntervalDayTimeArray,
    indices: &[usize],
) -> Vec<FixedLenByteArray> {
    let mut values = Vec::with_capacity(indices.len());
    for i in indices {
        let mut out = [0; 12];
        let value = array.value(*i);
        out[4..8].copy_from_slice(&value.days.to_le_bytes());
        out[8..12].copy_from_slice(&value.milliseconds.to_le_bytes());
        values.push(FixedLenByteArray::from(ByteArray::from(out.to_vec())));
    }
    values
}

fn get_decimal_128_array_slice(
    array: &arrow_array::Decimal128Array,
    indices: &[usize],
) -> Vec<FixedLenByteArray> {
    let mut values = Vec::with_capacity(indices.len());
    let size = decimal_length_from_precision(array.precision());
    for i in indices {
        let as_be_bytes = array.value(*i).to_be_bytes();
        let resized_value = as_be_bytes[(16 - size)..].to_vec();
        values.push(FixedLenByteArray::from(ByteArray::from(resized_value)));
    }
    values
}

fn get_decimal_256_array_slice(
    array: &arrow_array::Decimal256Array,
    indices: &[usize],
) -> Vec<FixedLenByteArray> {
    let mut values = Vec::with_capacity(indices.len());
    let size = decimal_length_from_precision(array.precision());
    for i in indices {
        let as_be_bytes = array.value(*i).to_be_bytes();
        let resized_value = as_be_bytes[(32 - size)..].to_vec();
        values.push(FixedLenByteArray::from(ByteArray::from(resized_value)));
    }
    values
}

fn get_float_16_array_slice(
    array: &arrow_array::Float16Array,
    indices: &[usize],
) -> Vec<FixedLenByteArray> {
    let mut values = Vec::with_capacity(indices.len());
    for i in indices {
        let value = array.value(*i).to_le_bytes().to_vec();
        values.push(FixedLenByteArray::from(ByteArray::from(value)));
    }
    values
}

fn get_fsb_array_slice(
    array: &arrow_array::FixedSizeBinaryArray,
    indices: &[usize],
) -> Vec<FixedLenByteArray> {
    let mut values = Vec::with_capacity(indices.len());
    for i in indices {
        let value = array.value(*i).to_vec();
        values.push(FixedLenByteArray::from(ByteArray::from(value)))
    }
    values
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::fs::File;

    use crate::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
    use crate::arrow::ARROW_SCHEMA_META_KEY;
    use arrow::datatypes::ToByteSlice;
    use arrow::datatypes::{DataType, Schema};
    use arrow::error::Result as ArrowResult;
    use arrow::util::data_gen::create_random_array;
    use arrow::util::pretty::pretty_format_batches;
    use arrow::{array::*, buffer::Buffer};
    use arrow_buffer::{i256, IntervalDayTime, IntervalMonthDayNano, NullBuffer};
    use arrow_schema::Fields;
    use half::f16;

    use crate::basic::Encoding;
    use crate::data_type::AsBytes;
    use crate::file::metadata::ParquetMetaData;
    use crate::file::page_index::index::Index;
    use crate::file::page_index::index_reader::read_offset_indexes;
    use crate::file::properties::{
        BloomFilterPosition, EnabledStatistics, ReaderProperties, WriterVersion,
    };
    use crate::file::serialized_reader::ReadOptionsBuilder;
    use crate::file::{
        reader::{FileReader, SerializedFileReader},
        statistics::Statistics,
    };

    #[test]
    fn arrow_writer() {
        // define schema
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, true),
        ]);

        // create some data
        let a = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let b = Int32Array::from(vec![Some(1), None, None, Some(4), Some(5)]);

        // build a record batch
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a), Arc::new(b)]).unwrap();

        roundtrip(batch, Some(SMALL_SIZE / 2));
    }

    fn get_bytes_after_close(schema: SchemaRef, expected_batch: &RecordBatch) -> Vec<u8> {
        let mut buffer = vec![];

        let mut writer = ArrowWriter::try_new(&mut buffer, schema, None).unwrap();
        writer.write(expected_batch).unwrap();
        writer.close().unwrap();

        buffer
    }

    fn get_bytes_by_into_inner(schema: SchemaRef, expected_batch: &RecordBatch) -> Vec<u8> {
        let mut writer = ArrowWriter::try_new(Vec::new(), schema, None).unwrap();
        writer.write(expected_batch).unwrap();
        writer.into_inner().unwrap()
    }

    #[test]
    fn roundtrip_bytes() {
        // define schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, true),
        ]));

        // create some data
        let a = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let b = Int32Array::from(vec![Some(1), None, None, Some(4), Some(5)]);

        // build a record batch
        let expected_batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(a), Arc::new(b)]).unwrap();

        for buffer in [
            get_bytes_after_close(schema.clone(), &expected_batch),
            get_bytes_by_into_inner(schema, &expected_batch),
        ] {
            let cursor = Bytes::from(buffer);
            let mut record_batch_reader = ParquetRecordBatchReader::try_new(cursor, 1024).unwrap();

            let actual_batch = record_batch_reader
                .next()
                .expect("No batch found")
                .expect("Unable to get batch");

            assert_eq!(expected_batch.schema(), actual_batch.schema());
            assert_eq!(expected_batch.num_columns(), actual_batch.num_columns());
            assert_eq!(expected_batch.num_rows(), actual_batch.num_rows());
            for i in 0..expected_batch.num_columns() {
                let expected_data = expected_batch.column(i).to_data();
                let actual_data = actual_batch.column(i).to_data();

                assert_eq!(expected_data, actual_data);
            }
        }
    }

    #[test]
    fn arrow_writer_non_null() {
        // define schema
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

        // create some data
        let a = Int32Array::from(vec![1, 2, 3, 4, 5]);

        // build a record batch
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)]).unwrap();

        roundtrip(batch, Some(SMALL_SIZE / 2));
    }

    #[test]
    fn arrow_writer_list() {
        // define schema
        let schema = Schema::new(vec![Field::new(
            "a",
            DataType::List(Arc::new(Field::new_list_field(DataType::Int32, false))),
            true,
        )]);

        // create some data
        let a_values = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

        // Construct a buffer for value offsets, for the nested array:
        //  [[1], [2, 3], null, [4, 5, 6], [7, 8, 9, 10]]
        let a_value_offsets = arrow::buffer::Buffer::from([0, 1, 3, 3, 6, 10].to_byte_slice());

        // Construct a list array from the above two
        let a_list_data = ArrayData::builder(DataType::List(Arc::new(Field::new_list_field(
            DataType::Int32,
            false,
        ))))
        .len(5)
        .add_buffer(a_value_offsets)
        .add_child_data(a_values.into_data())
        .null_bit_buffer(Some(Buffer::from([0b00011011])))
        .build()
        .unwrap();
        let a = ListArray::from(a_list_data);

        // build a record batch
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)]).unwrap();

        assert_eq!(batch.column(0).null_count(), 1);

        // This test fails if the max row group size is less than the batch's length
        // see https://github.com/apache/arrow-rs/issues/518
        roundtrip(batch, None);
    }

    #[test]
    fn arrow_writer_list_non_null() {
        // define schema
        let schema = Schema::new(vec![Field::new(
            "a",
            DataType::List(Arc::new(Field::new_list_field(DataType::Int32, false))),
            false,
        )]);

        // create some data
        let a_values = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

        // Construct a buffer for value offsets, for the nested array:
        //  [[1], [2, 3], [], [4, 5, 6], [7, 8, 9, 10]]
        let a_value_offsets = arrow::buffer::Buffer::from([0, 1, 3, 3, 6, 10].to_byte_slice());

        // Construct a list array from the above two
        let a_list_data = ArrayData::builder(DataType::List(Arc::new(Field::new_list_field(
            DataType::Int32,
            false,
        ))))
        .len(5)
        .add_buffer(a_value_offsets)
        .add_child_data(a_values.into_data())
        .build()
        .unwrap();
        let a = ListArray::from(a_list_data);

        // build a record batch
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)]).unwrap();

        // This test fails if the max row group size is less than the batch's length
        // see https://github.com/apache/arrow-rs/issues/518
        assert_eq!(batch.column(0).null_count(), 0);

        roundtrip(batch, None);
    }

    #[test]
    fn arrow_writer_binary() {
        let string_field = Field::new("a", DataType::Utf8, false);
        let binary_field = Field::new("b", DataType::Binary, false);
        let schema = Schema::new(vec![string_field, binary_field]);

        let raw_string_values = vec!["foo", "bar", "baz", "quux"];
        let raw_binary_values = [
            b"foo".to_vec(),
            b"bar".to_vec(),
            b"baz".to_vec(),
            b"quux".to_vec(),
        ];
        let raw_binary_value_refs = raw_binary_values
            .iter()
            .map(|x| x.as_slice())
            .collect::<Vec<_>>();

        let string_values = StringArray::from(raw_string_values.clone());
        let binary_values = BinaryArray::from(raw_binary_value_refs);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(string_values), Arc::new(binary_values)],
        )
        .unwrap();

        roundtrip(batch, Some(SMALL_SIZE / 2));
    }

    #[test]
    fn arrow_writer_binary_view() {
        let string_field = Field::new("a", DataType::Utf8View, false);
        let binary_field = Field::new("b", DataType::BinaryView, false);
        let nullable_string_field = Field::new("a", DataType::Utf8View, true);
        let schema = Schema::new(vec![string_field, binary_field, nullable_string_field]);

        let raw_string_values = vec!["foo", "bar", "large payload over 12 bytes", "lulu"];
        let raw_binary_values = vec![
            b"foo".to_vec(),
            b"bar".to_vec(),
            b"large payload over 12 bytes".to_vec(),
            b"lulu".to_vec(),
        ];
        let nullable_string_values =
            vec![Some("foo"), None, Some("large payload over 12 bytes"), None];

        let string_view_values = StringViewArray::from(raw_string_values);
        let binary_view_values = BinaryViewArray::from_iter_values(raw_binary_values);
        let nullable_string_view_values = StringViewArray::from(nullable_string_values);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(string_view_values),
                Arc::new(binary_view_values),
                Arc::new(nullable_string_view_values),
            ],
        )
        .unwrap();

        roundtrip(batch.clone(), Some(SMALL_SIZE / 2));
        roundtrip(batch, None);
    }

    fn get_decimal_batch(precision: u8, scale: i8) -> RecordBatch {
        let decimal_field = Field::new("a", DataType::Decimal128(precision, scale), false);
        let schema = Schema::new(vec![decimal_field]);

        let decimal_values = vec![10_000, 50_000, 0, -100]
            .into_iter()
            .map(Some)
            .collect::<Decimal128Array>()
            .with_precision_and_scale(precision, scale)
            .unwrap();

        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(decimal_values)]).unwrap()
    }

    #[test]
    fn arrow_writer_decimal() {
        // int32 to store the decimal value
        let batch_int32_decimal = get_decimal_batch(5, 2);
        roundtrip(batch_int32_decimal, Some(SMALL_SIZE / 2));
        // int64 to store the decimal value
        let batch_int64_decimal = get_decimal_batch(12, 2);
        roundtrip(batch_int64_decimal, Some(SMALL_SIZE / 2));
        // fixed_length_byte_array to store the decimal value
        let batch_fixed_len_byte_array_decimal = get_decimal_batch(30, 2);
        roundtrip(batch_fixed_len_byte_array_decimal, Some(SMALL_SIZE / 2));
    }

    #[test]
    fn arrow_writer_complex() {
        // define schema
        let struct_field_d = Arc::new(Field::new("d", DataType::Float64, true));
        let struct_field_f = Arc::new(Field::new("f", DataType::Float32, true));
        let struct_field_g = Arc::new(Field::new_list(
            "g",
            Field::new_list_field(DataType::Int16, true),
            false,
        ));
        let struct_field_h = Arc::new(Field::new_list(
            "h",
            Field::new_list_field(DataType::Int16, false),
            true,
        ));
        let struct_field_e = Arc::new(Field::new_struct(
            "e",
            vec![
                struct_field_f.clone(),
                struct_field_g.clone(),
                struct_field_h.clone(),
            ],
            false,
        ));
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, true),
            Field::new_struct(
                "c",
                vec![struct_field_d.clone(), struct_field_e.clone()],
                false,
            ),
        ]);

        // create some data
        let a = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let b = Int32Array::from(vec![Some(1), None, None, Some(4), Some(5)]);
        let d = Float64Array::from(vec![None, None, None, Some(1.0), None]);
        let f = Float32Array::from(vec![Some(0.0), None, Some(333.3), None, Some(5.25)]);

        let g_value = Int16Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

        // Construct a buffer for value offsets, for the nested array:
        //  [[1], [2, 3], [], [4, 5, 6], [7, 8, 9, 10]]
        let g_value_offsets = arrow::buffer::Buffer::from([0, 1, 3, 3, 6, 10].to_byte_slice());

        // Construct a list array from the above two
        let g_list_data = ArrayData::builder(struct_field_g.data_type().clone())
            .len(5)
            .add_buffer(g_value_offsets.clone())
            .add_child_data(g_value.to_data())
            .build()
            .unwrap();
        let g = ListArray::from(g_list_data);
        // The difference between g and h is that h has a null bitmap
        let h_list_data = ArrayData::builder(struct_field_h.data_type().clone())
            .len(5)
            .add_buffer(g_value_offsets)
            .add_child_data(g_value.to_data())
            .null_bit_buffer(Some(Buffer::from([0b00011011])))
            .build()
            .unwrap();
        let h = ListArray::from(h_list_data);

        let e = StructArray::from(vec![
            (struct_field_f, Arc::new(f) as ArrayRef),
            (struct_field_g, Arc::new(g) as ArrayRef),
            (struct_field_h, Arc::new(h) as ArrayRef),
        ]);

        let c = StructArray::from(vec![
            (struct_field_d, Arc::new(d) as ArrayRef),
            (struct_field_e, Arc::new(e) as ArrayRef),
        ]);

        // build a record batch
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(a), Arc::new(b), Arc::new(c)],
        )
        .unwrap();

        roundtrip(batch.clone(), Some(SMALL_SIZE / 2));
        roundtrip(batch, Some(SMALL_SIZE / 3));
    }

    #[test]
    fn arrow_writer_complex_mixed() {
        // This test was added while investigating https://github.com/apache/arrow-rs/issues/244.
        // It was subsequently fixed while investigating https://github.com/apache/arrow-rs/issues/245.

        // define schema
        let offset_field = Arc::new(Field::new("offset", DataType::Int32, false));
        let partition_field = Arc::new(Field::new("partition", DataType::Int64, true));
        let topic_field = Arc::new(Field::new("topic", DataType::Utf8, true));
        let schema = Schema::new(vec![Field::new(
            "some_nested_object",
            DataType::Struct(Fields::from(vec![
                offset_field.clone(),
                partition_field.clone(),
                topic_field.clone(),
            ])),
            false,
        )]);

        // create some data
        let offset = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let partition = Int64Array::from(vec![Some(1), None, None, Some(4), Some(5)]);
        let topic = StringArray::from(vec![Some("A"), None, Some("A"), Some(""), None]);

        let some_nested_object = StructArray::from(vec![
            (offset_field, Arc::new(offset) as ArrayRef),
            (partition_field, Arc::new(partition) as ArrayRef),
            (topic_field, Arc::new(topic) as ArrayRef),
        ]);

        // build a record batch
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(some_nested_object)]).unwrap();

        roundtrip(batch, Some(SMALL_SIZE / 2));
    }

    #[test]
    fn arrow_writer_map() {
        // Note: we are using the JSON Arrow reader for brevity
        let json_content = r#"
        {"stocks":{"long": "$AAA", "short": "$BBB"}}
        {"stocks":{"long": null, "long": "$CCC", "short": null}}
        {"stocks":{"hedged": "$YYY", "long": null, "short": "$D"}}
        "#;
        let entries_struct_type = DataType::Struct(Fields::from(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
        ]));
        let stocks_field = Field::new(
            "stocks",
            DataType::Map(
                Arc::new(Field::new("entries", entries_struct_type, false)),
                false,
            ),
            true,
        );
        let schema = Arc::new(Schema::new(vec![stocks_field]));
        let builder = arrow::json::ReaderBuilder::new(schema).with_batch_size(64);
        let mut reader = builder.build(std::io::Cursor::new(json_content)).unwrap();

        let batch = reader.next().unwrap().unwrap();
        roundtrip(batch, None);
    }

    #[test]
    fn arrow_writer_2_level_struct() {
        // tests writing <struct<struct<primitive>>
        let field_c = Field::new("c", DataType::Int32, true);
        let field_b = Field::new("b", DataType::Struct(vec![field_c].into()), true);
        let type_a = DataType::Struct(vec![field_b.clone()].into());
        let field_a = Field::new("a", type_a, true);
        let schema = Schema::new(vec![field_a.clone()]);

        // create data
        let c = Int32Array::from(vec![Some(1), None, Some(3), None, None, Some(6)]);
        let b_data = ArrayDataBuilder::new(field_b.data_type().clone())
            .len(6)
            .null_bit_buffer(Some(Buffer::from([0b00100111])))
            .add_child_data(c.into_data())
            .build()
            .unwrap();
        let b = StructArray::from(b_data);
        let a_data = ArrayDataBuilder::new(field_a.data_type().clone())
            .len(6)
            .null_bit_buffer(Some(Buffer::from([0b00101111])))
            .add_child_data(b.into_data())
            .build()
            .unwrap();
        let a = StructArray::from(a_data);

        assert_eq!(a.null_count(), 1);
        assert_eq!(a.column(0).null_count(), 2);

        // build a racord batch
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)]).unwrap();

        roundtrip(batch, Some(SMALL_SIZE / 2));
    }

    #[test]
    fn arrow_writer_2_level_struct_non_null() {
        // tests writing <struct<struct<primitive>>
        let field_c = Field::new("c", DataType::Int32, false);
        let type_b = DataType::Struct(vec![field_c].into());
        let field_b = Field::new("b", type_b.clone(), false);
        let type_a = DataType::Struct(vec![field_b].into());
        let field_a = Field::new("a", type_a.clone(), false);
        let schema = Schema::new(vec![field_a]);

        // create data
        let c = Int32Array::from(vec![1, 2, 3, 4, 5, 6]);
        let b_data = ArrayDataBuilder::new(type_b)
            .len(6)
            .add_child_data(c.into_data())
            .build()
            .unwrap();
        let b = StructArray::from(b_data);
        let a_data = ArrayDataBuilder::new(type_a)
            .len(6)
            .add_child_data(b.into_data())
            .build()
            .unwrap();
        let a = StructArray::from(a_data);

        assert_eq!(a.null_count(), 0);
        assert_eq!(a.column(0).null_count(), 0);

        // build a racord batch
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)]).unwrap();

        roundtrip(batch, Some(SMALL_SIZE / 2));
    }

    #[test]
    fn arrow_writer_2_level_struct_mixed_null() {
        // tests writing <struct<struct<primitive>>
        let field_c = Field::new("c", DataType::Int32, false);
        let type_b = DataType::Struct(vec![field_c].into());
        let field_b = Field::new("b", type_b.clone(), true);
        let type_a = DataType::Struct(vec![field_b].into());
        let field_a = Field::new("a", type_a.clone(), false);
        let schema = Schema::new(vec![field_a]);

        // create data
        let c = Int32Array::from(vec![1, 2, 3, 4, 5, 6]);
        let b_data = ArrayDataBuilder::new(type_b)
            .len(6)
            .null_bit_buffer(Some(Buffer::from([0b00100111])))
            .add_child_data(c.into_data())
            .build()
            .unwrap();
        let b = StructArray::from(b_data);
        // a intentionally has no null buffer, to test that this is handled correctly
        let a_data = ArrayDataBuilder::new(type_a)
            .len(6)
            .add_child_data(b.into_data())
            .build()
            .unwrap();
        let a = StructArray::from(a_data);

        assert_eq!(a.null_count(), 0);
        assert_eq!(a.column(0).null_count(), 2);

        // build a racord batch
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)]).unwrap();

        roundtrip(batch, Some(SMALL_SIZE / 2));
    }

    #[test]
    fn arrow_writer_2_level_struct_mixed_null_2() {
        // tests writing <struct<struct<primitive>>, where the primitive columns are non-null.
        let field_c = Field::new("c", DataType::Int32, false);
        let field_d = Field::new("d", DataType::FixedSizeBinary(4), false);
        let field_e = Field::new(
            "e",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        );

        let field_b = Field::new(
            "b",
            DataType::Struct(vec![field_c, field_d, field_e].into()),
            false,
        );
        let type_a = DataType::Struct(vec![field_b.clone()].into());
        let field_a = Field::new("a", type_a, true);
        let schema = Schema::new(vec![field_a.clone()]);

        // create data
        let c = Int32Array::from_iter_values(0..6);
        let d = FixedSizeBinaryArray::try_from_iter(
            ["aaaa", "bbbb", "cccc", "dddd", "eeee", "ffff"].into_iter(),
        )
        .expect("four byte values");
        let e = Int32DictionaryArray::from_iter(["one", "two", "three", "four", "five", "one"]);
        let b_data = ArrayDataBuilder::new(field_b.data_type().clone())
            .len(6)
            .add_child_data(c.into_data())
            .add_child_data(d.into_data())
            .add_child_data(e.into_data())
            .build()
            .unwrap();
        let b = StructArray::from(b_data);
        let a_data = ArrayDataBuilder::new(field_a.data_type().clone())
            .len(6)
            .null_bit_buffer(Some(Buffer::from([0b00100101])))
            .add_child_data(b.into_data())
            .build()
            .unwrap();
        let a = StructArray::from(a_data);

        assert_eq!(a.null_count(), 3);
        assert_eq!(a.column(0).null_count(), 0);

        // build a record batch
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)]).unwrap();

        roundtrip(batch, Some(SMALL_SIZE / 2));
    }

    #[test]
    fn test_empty_dict() {
        let struct_fields = Fields::from(vec![Field::new(
            "dict",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        )]);

        let schema = Schema::new(vec![Field::new_struct(
            "struct",
            struct_fields.clone(),
            true,
        )]);
        let dictionary = Arc::new(DictionaryArray::new(
            Int32Array::new_null(5),
            Arc::new(StringArray::new_null(0)),
        ));

        let s = StructArray::new(
            struct_fields,
            vec![dictionary],
            Some(NullBuffer::new_null(5)),
        );

        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(s)]).unwrap();
        roundtrip(batch, None);
    }
    #[test]
    fn arrow_writer_page_size() {
        let schema = Arc::new(Schema::new(vec![Field::new("col", DataType::Utf8, false)]));

        let mut builder = StringBuilder::with_capacity(100, 329 * 10_000);

        // Generate an array of 10 unique 10 character string
        for i in 0..10 {
            let value = i
                .to_string()
                .repeat(10)
                .chars()
                .take(10)
                .collect::<String>();

            builder.append_value(value);
        }

        let array = Arc::new(builder.finish());

        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();

        let file = tempfile::tempfile().unwrap();

        // Set everything very low so we fallback to PLAIN encoding after the first row
        let props = WriterProperties::builder()
            .set_data_page_size_limit(1)
            .set_dictionary_page_size_limit(1)
            .set_write_batch_size(1)
            .build();

        let mut writer =
            ArrowWriter::try_new(file.try_clone().unwrap(), batch.schema(), Some(props))
                .expect("Unable to write file");
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let reader = SerializedFileReader::new(file.try_clone().unwrap()).unwrap();

        let column = reader.metadata().row_group(0).columns();

        assert_eq!(column.len(), 1);

        // We should write one row before falling back to PLAIN encoding so there should still be a
        // dictionary page.
        assert!(
            column[0].dictionary_page_offset().is_some(),
            "Expected a dictionary page"
        );

        let offset_indexes = read_offset_indexes(&file, column).unwrap().unwrap();

        let page_locations = offset_indexes[0].page_locations.clone();

        // We should fallback to PLAIN encoding after the first row and our max page size is 1 bytes
        // so we expect one dictionary encoded page and then a page per row thereafter.
        assert_eq!(
            page_locations.len(),
            10,
            "Expected 9 pages but got {page_locations:#?}"
        );
    }

    #[test]
    fn arrow_writer_float_nans() {
        let f16_field = Field::new("a", DataType::Float16, false);
        let f32_field = Field::new("b", DataType::Float32, false);
        let f64_field = Field::new("c", DataType::Float64, false);
        let schema = Schema::new(vec![f16_field, f32_field, f64_field]);

        let f16_values = (0..MEDIUM_SIZE)
            .map(|i| {
                Some(if i % 2 == 0 {
                    f16::NAN
                } else {
                    f16::from_f32(i as f32)
                })
            })
            .collect::<Float16Array>();

        let f32_values = (0..MEDIUM_SIZE)
            .map(|i| Some(if i % 2 == 0 { f32::NAN } else { i as f32 }))
            .collect::<Float32Array>();

        let f64_values = (0..MEDIUM_SIZE)
            .map(|i| Some(if i % 2 == 0 { f64::NAN } else { i as f64 }))
            .collect::<Float64Array>();

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(f16_values),
                Arc::new(f32_values),
                Arc::new(f64_values),
            ],
        )
        .unwrap();

        roundtrip(batch, None);
    }

    const SMALL_SIZE: usize = 7;
    const MEDIUM_SIZE: usize = 63;

    fn roundtrip(expected_batch: RecordBatch, max_row_group_size: Option<usize>) -> Vec<File> {
        let mut files = vec![];
        for version in [WriterVersion::PARQUET_1_0, WriterVersion::PARQUET_2_0] {
            let mut props = WriterProperties::builder().set_writer_version(version);

            if let Some(size) = max_row_group_size {
                props = props.set_max_row_group_size(size)
            }

            let props = props.build();
            files.push(roundtrip_opts(&expected_batch, props))
        }
        files
    }

    fn roundtrip_opts_with_array_validation<F>(
        expected_batch: &RecordBatch,
        props: WriterProperties,
        validate: F,
    ) -> File
    where
        F: Fn(&ArrayData, &ArrayData),
    {
        let file = tempfile::tempfile().unwrap();

        let mut writer = ArrowWriter::try_new(
            file.try_clone().unwrap(),
            expected_batch.schema(),
            Some(props),
        )
        .expect("Unable to write file");
        writer.write(expected_batch).unwrap();
        writer.close().unwrap();

        let mut record_batch_reader =
            ParquetRecordBatchReader::try_new(file.try_clone().unwrap(), 1024).unwrap();

        let actual_batch = record_batch_reader
            .next()
            .expect("No batch found")
            .expect("Unable to get batch");

        assert_eq!(expected_batch.schema(), actual_batch.schema());
        assert_eq!(expected_batch.num_columns(), actual_batch.num_columns());
        assert_eq!(expected_batch.num_rows(), actual_batch.num_rows());
        for i in 0..expected_batch.num_columns() {
            let expected_data = expected_batch.column(i).to_data();
            let actual_data = actual_batch.column(i).to_data();
            validate(&expected_data, &actual_data);
        }

        file
    }

    fn roundtrip_opts(expected_batch: &RecordBatch, props: WriterProperties) -> File {
        roundtrip_opts_with_array_validation(expected_batch, props, |a, b| {
            a.validate_full().expect("valid expected data");
            b.validate_full().expect("valid actual data");
            assert_eq!(a, b)
        })
    }

    struct RoundTripOptions {
        values: ArrayRef,
        schema: SchemaRef,
        bloom_filter: bool,
        bloom_filter_position: BloomFilterPosition,
    }

    impl RoundTripOptions {
        fn new(values: ArrayRef, nullable: bool) -> Self {
            let data_type = values.data_type().clone();
            let schema = Schema::new(vec![Field::new("col", data_type, nullable)]);
            Self {
                values,
                schema: Arc::new(schema),
                bloom_filter: false,
                bloom_filter_position: BloomFilterPosition::AfterRowGroup,
            }
        }
    }

    fn one_column_roundtrip(values: ArrayRef, nullable: bool) -> Vec<File> {
        one_column_roundtrip_with_options(RoundTripOptions::new(values, nullable))
    }

    fn one_column_roundtrip_with_schema(values: ArrayRef, schema: SchemaRef) -> Vec<File> {
        let mut options = RoundTripOptions::new(values, false);
        options.schema = schema;
        one_column_roundtrip_with_options(options)
    }

    fn one_column_roundtrip_with_options(options: RoundTripOptions) -> Vec<File> {
        let RoundTripOptions {
            values,
            schema,
            bloom_filter,
            bloom_filter_position,
        } = options;

        let encodings = match values.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary => {
                vec![
                    Encoding::PLAIN,
                    Encoding::DELTA_BYTE_ARRAY,
                    Encoding::DELTA_LENGTH_BYTE_ARRAY,
                ]
            }
            DataType::Int64
            | DataType::Int32
            | DataType::Int16
            | DataType::Int8
            | DataType::UInt64
            | DataType::UInt32
            | DataType::UInt16
            | DataType::UInt8 => vec![
                Encoding::PLAIN,
                Encoding::DELTA_BINARY_PACKED,
                Encoding::BYTE_STREAM_SPLIT,
            ],
            DataType::Float32 | DataType::Float64 => {
                vec![Encoding::PLAIN, Encoding::BYTE_STREAM_SPLIT]
            }
            _ => vec![Encoding::PLAIN],
        };

        let expected_batch = RecordBatch::try_new(schema, vec![values]).unwrap();

        let row_group_sizes = [1024, SMALL_SIZE, SMALL_SIZE / 2, SMALL_SIZE / 2 + 1, 10];

        let mut files = vec![];
        for dictionary_size in [0, 1, 1024] {
            for encoding in &encodings {
                for version in [WriterVersion::PARQUET_1_0, WriterVersion::PARQUET_2_0] {
                    for row_group_size in row_group_sizes {
                        let props = WriterProperties::builder()
                            .set_writer_version(version)
                            .set_max_row_group_size(row_group_size)
                            .set_dictionary_enabled(dictionary_size != 0)
                            .set_dictionary_page_size_limit(dictionary_size.max(1))
                            .set_encoding(*encoding)
                            .set_bloom_filter_enabled(bloom_filter)
                            .set_bloom_filter_position(bloom_filter_position)
                            .build();

                        files.push(roundtrip_opts(&expected_batch, props))
                    }
                }
            }
        }
        files
    }

    fn values_required<A, I>(iter: I) -> Vec<File>
    where
        A: From<Vec<I::Item>> + Array + 'static,
        I: IntoIterator,
    {
        let raw_values: Vec<_> = iter.into_iter().collect();
        let values = Arc::new(A::from(raw_values));
        one_column_roundtrip(values, false)
    }

    fn values_optional<A, I>(iter: I) -> Vec<File>
    where
        A: From<Vec<Option<I::Item>>> + Array + 'static,
        I: IntoIterator,
    {
        let optional_raw_values: Vec<_> = iter
            .into_iter()
            .enumerate()
            .map(|(i, v)| if i % 2 == 0 { None } else { Some(v) })
            .collect();
        let optional_values = Arc::new(A::from(optional_raw_values));
        one_column_roundtrip(optional_values, true)
    }

    fn required_and_optional<A, I>(iter: I)
    where
        A: From<Vec<I::Item>> + From<Vec<Option<I::Item>>> + Array + 'static,
        I: IntoIterator + Clone,
    {
        values_required::<A, I>(iter.clone());
        values_optional::<A, I>(iter);
    }

    fn check_bloom_filter<T: AsBytes>(
        files: Vec<File>,
        file_column: String,
        positive_values: Vec<T>,
        negative_values: Vec<T>,
    ) {
        files.into_iter().take(1).for_each(|file| {
            let file_reader = SerializedFileReader::new_with_options(
                file,
                ReadOptionsBuilder::new()
                    .with_reader_properties(
                        ReaderProperties::builder()
                            .set_read_bloom_filter(true)
                            .build(),
                    )
                    .build(),
            )
            .expect("Unable to open file as Parquet");
            let metadata = file_reader.metadata();

            // Gets bloom filters from all row groups.
            let mut bloom_filters: Vec<_> = vec![];
            for (ri, row_group) in metadata.row_groups().iter().enumerate() {
                if let Some((column_index, _)) = row_group
                    .columns()
                    .iter()
                    .enumerate()
                    .find(|(_, column)| column.column_path().string() == file_column)
                {
                    let row_group_reader = file_reader
                        .get_row_group(ri)
                        .expect("Unable to read row group");
                    if let Some(sbbf) = row_group_reader.get_column_bloom_filter(column_index) {
                        bloom_filters.push(sbbf.clone());
                    } else {
                        panic!("No bloom filter for column named {file_column} found");
                    }
                } else {
                    panic!("No column named {file_column} found");
                }
            }

            positive_values.iter().for_each(|value| {
                let found = bloom_filters.iter().find(|sbbf| sbbf.check(value));
                assert!(
                    found.is_some(),
                    "{}",
                    format!("Value {:?} should be in bloom filter", value.as_bytes())
                );
            });

            negative_values.iter().for_each(|value| {
                let found = bloom_filters.iter().find(|sbbf| sbbf.check(value));
                assert!(
                    found.is_none(),
                    "{}",
                    format!("Value {:?} should not be in bloom filter", value.as_bytes())
                );
            });
        });
    }

    #[test]
    fn all_null_primitive_single_column() {
        let values = Arc::new(Int32Array::from(vec![None; SMALL_SIZE]));
        one_column_roundtrip(values, true);
    }
    #[test]
    fn null_single_column() {
        let values = Arc::new(NullArray::new(SMALL_SIZE));
        one_column_roundtrip(values, true);
        // null arrays are always nullable, a test with non-nullable nulls fails
    }

    #[test]
    fn bool_single_column() {
        required_and_optional::<BooleanArray, _>(
            [true, false].iter().cycle().copied().take(SMALL_SIZE),
        );
    }

    #[test]
    fn bool_large_single_column() {
        let values = Arc::new(
            [None, Some(true), Some(false)]
                .iter()
                .cycle()
                .copied()
                .take(200_000)
                .collect::<BooleanArray>(),
        );
        let schema = Schema::new(vec![Field::new("col", values.data_type().clone(), true)]);
        let expected_batch = RecordBatch::try_new(Arc::new(schema), vec![values]).unwrap();
        let file = tempfile::tempfile().unwrap();

        let mut writer =
            ArrowWriter::try_new(file.try_clone().unwrap(), expected_batch.schema(), None)
                .expect("Unable to write file");
        writer.write(&expected_batch).unwrap();
        writer.close().unwrap();
    }

    #[test]
    fn check_page_offset_index_with_nan() {
        let values = Arc::new(Float64Array::from(vec![f64::NAN; 10]));
        let schema = Schema::new(vec![Field::new("col", DataType::Float64, true)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![values]).unwrap();

        let mut out = Vec::with_capacity(1024);
        let mut writer =
            ArrowWriter::try_new(&mut out, batch.schema(), None).expect("Unable to write file");
        writer.write(&batch).unwrap();
        let file_meta_data = writer.close().unwrap();
        for row_group in file_meta_data.row_groups {
            for column in row_group.columns {
                assert!(column.offset_index_offset.is_some());
                assert!(column.offset_index_length.is_some());
                assert!(column.column_index_offset.is_none());
                assert!(column.column_index_length.is_none());
            }
        }
    }

    #[test]
    fn i8_single_column() {
        required_and_optional::<Int8Array, _>(0..SMALL_SIZE as i8);
    }

    #[test]
    fn i16_single_column() {
        required_and_optional::<Int16Array, _>(0..SMALL_SIZE as i16);
    }

    #[test]
    fn i32_single_column() {
        required_and_optional::<Int32Array, _>(0..SMALL_SIZE as i32);
    }

    #[test]
    fn i64_single_column() {
        required_and_optional::<Int64Array, _>(0..SMALL_SIZE as i64);
    }

    #[test]
    fn u8_single_column() {
        required_and_optional::<UInt8Array, _>(0..SMALL_SIZE as u8);
    }

    #[test]
    fn u16_single_column() {
        required_and_optional::<UInt16Array, _>(0..SMALL_SIZE as u16);
    }

    #[test]
    fn u32_single_column() {
        required_and_optional::<UInt32Array, _>(0..SMALL_SIZE as u32);
    }

    #[test]
    fn u64_single_column() {
        required_and_optional::<UInt64Array, _>(0..SMALL_SIZE as u64);
    }

    #[test]
    fn f32_single_column() {
        required_and_optional::<Float32Array, _>((0..SMALL_SIZE).map(|i| i as f32));
    }

    #[test]
    fn f64_single_column() {
        required_and_optional::<Float64Array, _>((0..SMALL_SIZE).map(|i| i as f64));
    }

    // The timestamp array types don't implement From<Vec<T>> because they need the timezone
    // argument, and they also doesn't support building from a Vec<Option<T>>, so call
    // one_column_roundtrip manually instead of calling required_and_optional for these tests.

    #[test]
    fn timestamp_second_single_column() {
        let raw_values: Vec<_> = (0..SMALL_SIZE as i64).collect();
        let values = Arc::new(TimestampSecondArray::from(raw_values));

        one_column_roundtrip(values, false);
    }

    #[test]
    fn timestamp_millisecond_single_column() {
        let raw_values: Vec<_> = (0..SMALL_SIZE as i64).collect();
        let values = Arc::new(TimestampMillisecondArray::from(raw_values));

        one_column_roundtrip(values, false);
    }

    #[test]
    fn timestamp_microsecond_single_column() {
        let raw_values: Vec<_> = (0..SMALL_SIZE as i64).collect();
        let values = Arc::new(TimestampMicrosecondArray::from(raw_values));

        one_column_roundtrip(values, false);
    }

    #[test]
    fn timestamp_nanosecond_single_column() {
        let raw_values: Vec<_> = (0..SMALL_SIZE as i64).collect();
        let values = Arc::new(TimestampNanosecondArray::from(raw_values));

        one_column_roundtrip(values, false);
    }

    #[test]
    fn date32_single_column() {
        required_and_optional::<Date32Array, _>(0..SMALL_SIZE as i32);
    }

    #[test]
    fn date64_single_column() {
        // Date64 must be a multiple of 86400000, see ARROW-10925
        required_and_optional::<Date64Array, _>(
            (0..(SMALL_SIZE as i64 * 86400000)).step_by(86400000),
        );
    }

    #[test]
    fn time32_second_single_column() {
        required_and_optional::<Time32SecondArray, _>(0..SMALL_SIZE as i32);
    }

    #[test]
    fn time32_millisecond_single_column() {
        required_and_optional::<Time32MillisecondArray, _>(0..SMALL_SIZE as i32);
    }

    #[test]
    fn time64_microsecond_single_column() {
        required_and_optional::<Time64MicrosecondArray, _>(0..SMALL_SIZE as i64);
    }

    #[test]
    fn time64_nanosecond_single_column() {
        required_and_optional::<Time64NanosecondArray, _>(0..SMALL_SIZE as i64);
    }

    #[test]
    #[should_panic(expected = "Converting Duration to parquet not supported")]
    fn duration_second_single_column() {
        required_and_optional::<DurationSecondArray, _>(0..SMALL_SIZE as i64);
    }

    #[test]
    #[should_panic(expected = "Converting Duration to parquet not supported")]
    fn duration_millisecond_single_column() {
        required_and_optional::<DurationMillisecondArray, _>(0..SMALL_SIZE as i64);
    }

    #[test]
    #[should_panic(expected = "Converting Duration to parquet not supported")]
    fn duration_microsecond_single_column() {
        required_and_optional::<DurationMicrosecondArray, _>(0..SMALL_SIZE as i64);
    }

    #[test]
    #[should_panic(expected = "Converting Duration to parquet not supported")]
    fn duration_nanosecond_single_column() {
        required_and_optional::<DurationNanosecondArray, _>(0..SMALL_SIZE as i64);
    }

    #[test]
    fn interval_year_month_single_column() {
        required_and_optional::<IntervalYearMonthArray, _>(0..SMALL_SIZE as i32);
    }

    #[test]
    fn interval_day_time_single_column() {
        required_and_optional::<IntervalDayTimeArray, _>(vec![
            IntervalDayTime::new(0, 1),
            IntervalDayTime::new(0, 3),
            IntervalDayTime::new(3, -2),
            IntervalDayTime::new(-200, 4),
        ]);
    }

    #[test]
    #[should_panic(
        expected = "Attempting to write an Arrow interval type MonthDayNano to parquet that is not yet implemented"
    )]
    fn interval_month_day_nano_single_column() {
        required_and_optional::<IntervalMonthDayNanoArray, _>(vec![
            IntervalMonthDayNano::new(0, 1, 5),
            IntervalMonthDayNano::new(0, 3, 2),
            IntervalMonthDayNano::new(3, -2, -5),
            IntervalMonthDayNano::new(-200, 4, -1),
        ]);
    }

    #[test]
    fn binary_single_column() {
        let one_vec: Vec<u8> = (0..SMALL_SIZE as u8).collect();
        let many_vecs: Vec<_> = std::iter::repeat(one_vec).take(SMALL_SIZE).collect();
        let many_vecs_iter = many_vecs.iter().map(|v| v.as_slice());

        // BinaryArrays can't be built from Vec<Option<&str>>, so only call `values_required`
        values_required::<BinaryArray, _>(many_vecs_iter);
    }

    #[test]
    fn binary_view_single_column() {
        let one_vec: Vec<u8> = (0..SMALL_SIZE as u8).collect();
        let many_vecs: Vec<_> = std::iter::repeat(one_vec).take(SMALL_SIZE).collect();
        let many_vecs_iter = many_vecs.iter().map(|v| v.as_slice());

        // BinaryArrays can't be built from Vec<Option<&str>>, so only call `values_required`
        values_required::<BinaryViewArray, _>(many_vecs_iter);
    }

    #[test]
    fn i32_column_bloom_filter_at_end() {
        let array = Arc::new(Int32Array::from_iter(0..SMALL_SIZE as i32));
        let mut options = RoundTripOptions::new(array, false);
        options.bloom_filter = true;
        options.bloom_filter_position = BloomFilterPosition::End;

        let files = one_column_roundtrip_with_options(options);
        check_bloom_filter(
            files,
            "col".to_string(),
            (0..SMALL_SIZE as i32).collect(),
            (SMALL_SIZE as i32 + 1..SMALL_SIZE as i32 + 10).collect(),
        );
    }

    #[test]
    fn i32_column_bloom_filter() {
        let array = Arc::new(Int32Array::from_iter(0..SMALL_SIZE as i32));
        let mut options = RoundTripOptions::new(array, false);
        options.bloom_filter = true;

        let files = one_column_roundtrip_with_options(options);
        check_bloom_filter(
            files,
            "col".to_string(),
            (0..SMALL_SIZE as i32).collect(),
            (SMALL_SIZE as i32 + 1..SMALL_SIZE as i32 + 10).collect(),
        );
    }

    #[test]
    fn binary_column_bloom_filter() {
        let one_vec: Vec<u8> = (0..SMALL_SIZE as u8).collect();
        let many_vecs: Vec<_> = std::iter::repeat(one_vec).take(SMALL_SIZE).collect();
        let many_vecs_iter = many_vecs.iter().map(|v| v.as_slice());

        let array = Arc::new(BinaryArray::from_iter_values(many_vecs_iter));
        let mut options = RoundTripOptions::new(array, false);
        options.bloom_filter = true;

        let files = one_column_roundtrip_with_options(options);
        check_bloom_filter(
            files,
            "col".to_string(),
            many_vecs,
            vec![vec![(SMALL_SIZE + 1) as u8]],
        );
    }

    #[test]
    fn empty_string_null_column_bloom_filter() {
        let raw_values: Vec<_> = (0..SMALL_SIZE).map(|i| i.to_string()).collect();
        let raw_strs = raw_values.iter().map(|s| s.as_str());

        let array = Arc::new(StringArray::from_iter_values(raw_strs));
        let mut options = RoundTripOptions::new(array, false);
        options.bloom_filter = true;

        let files = one_column_roundtrip_with_options(options);

        let optional_raw_values: Vec<_> = raw_values
            .iter()
            .enumerate()
            .filter_map(|(i, v)| if i % 2 == 0 { None } else { Some(v.as_str()) })
            .collect();
        // For null slots, empty string should not be in bloom filter.
        check_bloom_filter(files, "col".to_string(), optional_raw_values, vec![""]);
    }

    #[test]
    fn large_binary_single_column() {
        let one_vec: Vec<u8> = (0..SMALL_SIZE as u8).collect();
        let many_vecs: Vec<_> = std::iter::repeat(one_vec).take(SMALL_SIZE).collect();
        let many_vecs_iter = many_vecs.iter().map(|v| v.as_slice());

        // LargeBinaryArrays can't be built from Vec<Option<&str>>, so only call `values_required`
        values_required::<LargeBinaryArray, _>(many_vecs_iter);
    }

    #[test]
    fn fixed_size_binary_single_column() {
        let mut builder = FixedSizeBinaryBuilder::new(4);
        builder.append_value(b"0123").unwrap();
        builder.append_null();
        builder.append_value(b"8910").unwrap();
        builder.append_value(b"1112").unwrap();
        let array = Arc::new(builder.finish());

        one_column_roundtrip(array, true);
    }

    #[test]
    fn string_single_column() {
        let raw_values: Vec<_> = (0..SMALL_SIZE).map(|i| i.to_string()).collect();
        let raw_strs = raw_values.iter().map(|s| s.as_str());

        required_and_optional::<StringArray, _>(raw_strs);
    }

    #[test]
    fn large_string_single_column() {
        let raw_values: Vec<_> = (0..SMALL_SIZE).map(|i| i.to_string()).collect();
        let raw_strs = raw_values.iter().map(|s| s.as_str());

        required_and_optional::<LargeStringArray, _>(raw_strs);
    }

    #[test]
    fn string_view_single_column() {
        let raw_values: Vec<_> = (0..SMALL_SIZE).map(|i| i.to_string()).collect();
        let raw_strs = raw_values.iter().map(|s| s.as_str());

        required_and_optional::<StringViewArray, _>(raw_strs);
    }

    #[test]
    fn null_list_single_column() {
        let null_field = Field::new_list_field(DataType::Null, true);
        let list_field = Field::new("emptylist", DataType::List(Arc::new(null_field)), true);

        let schema = Schema::new(vec![list_field]);

        // Build [[], null, [null, null]]
        let a_values = NullArray::new(2);
        let a_value_offsets = arrow::buffer::Buffer::from([0, 0, 0, 2].to_byte_slice());
        let a_list_data = ArrayData::builder(DataType::List(Arc::new(Field::new_list_field(
            DataType::Null,
            true,
        ))))
        .len(3)
        .add_buffer(a_value_offsets)
        .null_bit_buffer(Some(Buffer::from([0b00000101])))
        .add_child_data(a_values.into_data())
        .build()
        .unwrap();

        let a = ListArray::from(a_list_data);

        assert!(a.is_valid(0));
        assert!(!a.is_valid(1));
        assert!(a.is_valid(2));

        assert_eq!(a.value(0).len(), 0);
        assert_eq!(a.value(2).len(), 2);
        assert_eq!(a.value(2).logical_nulls().unwrap().null_count(), 2);

        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)]).unwrap();
        roundtrip(batch, None);
    }

    #[test]
    fn list_single_column() {
        let a_values = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let a_value_offsets = arrow::buffer::Buffer::from([0, 1, 3, 3, 6, 10].to_byte_slice());
        let a_list_data = ArrayData::builder(DataType::List(Arc::new(Field::new_list_field(
            DataType::Int32,
            false,
        ))))
        .len(5)
        .add_buffer(a_value_offsets)
        .null_bit_buffer(Some(Buffer::from([0b00011011])))
        .add_child_data(a_values.into_data())
        .build()
        .unwrap();

        assert_eq!(a_list_data.null_count(), 1);

        let a = ListArray::from(a_list_data);
        let values = Arc::new(a);

        one_column_roundtrip(values, true);
    }

    #[test]
    fn large_list_single_column() {
        let a_values = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let a_value_offsets = arrow::buffer::Buffer::from([0i64, 1, 3, 3, 6, 10].to_byte_slice());
        let a_list_data = ArrayData::builder(DataType::LargeList(Arc::new(Field::new(
            "large_item",
            DataType::Int32,
            true,
        ))))
        .len(5)
        .add_buffer(a_value_offsets)
        .add_child_data(a_values.into_data())
        .null_bit_buffer(Some(Buffer::from([0b00011011])))
        .build()
        .unwrap();

        // I think this setup is incorrect because this should pass
        assert_eq!(a_list_data.null_count(), 1);

        let a = LargeListArray::from(a_list_data);
        let values = Arc::new(a);

        one_column_roundtrip(values, true);
    }

    #[test]
    fn list_nested_nulls() {
        use arrow::datatypes::Int32Type;
        let data = vec![
            Some(vec![Some(1)]),
            Some(vec![Some(2), Some(3)]),
            None,
            Some(vec![Some(4), Some(5), None]),
            Some(vec![None]),
            Some(vec![Some(6), Some(7)]),
        ];

        let list = ListArray::from_iter_primitive::<Int32Type, _, _>(data.clone());
        one_column_roundtrip(Arc::new(list), true);

        let list = LargeListArray::from_iter_primitive::<Int32Type, _, _>(data);
        one_column_roundtrip(Arc::new(list), true);
    }

    #[test]
    fn struct_single_column() {
        let a_values = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let struct_field_a = Arc::new(Field::new("f", DataType::Int32, false));
        let s = StructArray::from(vec![(struct_field_a, Arc::new(a_values) as ArrayRef)]);

        let values = Arc::new(s);
        one_column_roundtrip(values, false);
    }

    #[test]
    fn list_and_map_coerced_names() {
        // Create map and list with non-Parquet naming
        let list_field =
            Field::new_list("my_list", Field::new("item", DataType::Int32, false), false);
        let map_field = Field::new_map(
            "my_map",
            "entries",
            Field::new("keys", DataType::Int32, false),
            Field::new("values", DataType::Int32, true),
            false,
            true,
        );

        let list_array = create_random_array(&list_field, 100, 0.0, 0.0).unwrap();
        let map_array = create_random_array(&map_field, 100, 0.0, 0.0).unwrap();

        let arrow_schema = Arc::new(Schema::new(vec![list_field, map_field]));

        // Write data to Parquet but coerce names to match spec
        let props = Some(WriterProperties::builder().set_coerce_types(true).build());
        let file = tempfile::tempfile().unwrap();
        let mut writer =
            ArrowWriter::try_new(file.try_clone().unwrap(), arrow_schema.clone(), props).unwrap();

        let batch = RecordBatch::try_new(arrow_schema, vec![list_array, map_array]).unwrap();
        writer.write(&batch).unwrap();
        let file_metadata = writer.close().unwrap();

        // Coerced name of "item" should be "element"
        assert_eq!(file_metadata.schema[3].name, "element");
        // Coerced name of "entries" should be "key_value"
        assert_eq!(file_metadata.schema[5].name, "key_value");
        // Coerced name of "keys" should be "key"
        assert_eq!(file_metadata.schema[6].name, "key");
        // Coerced name of "values" should be "value"
        assert_eq!(file_metadata.schema[7].name, "value");

        // Double check schema after reading from the file
        let reader = SerializedFileReader::new(file).unwrap();
        let file_schema = reader.metadata().file_metadata().schema();
        let fields = file_schema.get_fields();
        let list_field = &fields[0].get_fields()[0];
        assert_eq!(list_field.get_fields()[0].name(), "element");
        let map_field = &fields[1].get_fields()[0];
        assert_eq!(map_field.name(), "key_value");
        assert_eq!(map_field.get_fields()[0].name(), "key");
        assert_eq!(map_field.get_fields()[1].name(), "value");
    }

    #[test]
    fn fallback_flush_data_page() {
        //tests if the Fallback::flush_data_page clears all buffers correctly
        let raw_values: Vec<_> = (0..MEDIUM_SIZE).map(|i| i.to_string()).collect();
        let values = Arc::new(StringArray::from(raw_values));
        let encodings = vec![
            Encoding::DELTA_BYTE_ARRAY,
            Encoding::DELTA_LENGTH_BYTE_ARRAY,
        ];
        let data_type = values.data_type().clone();
        let schema = Arc::new(Schema::new(vec![Field::new("col", data_type, false)]));
        let expected_batch = RecordBatch::try_new(schema, vec![values]).unwrap();

        let row_group_sizes = [1024, SMALL_SIZE, SMALL_SIZE / 2, SMALL_SIZE / 2 + 1, 10];
        let data_page_size_limit: usize = 32;
        let write_batch_size: usize = 16;

        for encoding in &encodings {
            for row_group_size in row_group_sizes {
                let props = WriterProperties::builder()
                    .set_writer_version(WriterVersion::PARQUET_2_0)
                    .set_max_row_group_size(row_group_size)
                    .set_dictionary_enabled(false)
                    .set_encoding(*encoding)
                    .set_data_page_size_limit(data_page_size_limit)
                    .set_write_batch_size(write_batch_size)
                    .build();

                roundtrip_opts_with_array_validation(&expected_batch, props, |a, b| {
                    let string_array_a = StringArray::from(a.clone());
                    let string_array_b = StringArray::from(b.clone());
                    let vec_a: Vec<&str> = string_array_a.iter().map(|v| v.unwrap()).collect();
                    let vec_b: Vec<&str> = string_array_b.iter().map(|v| v.unwrap()).collect();
                    assert_eq!(
                        vec_a, vec_b,
                        "failed for encoder: {encoding:?} and row_group_size: {row_group_size:?}"
                    );
                });
            }
        }
    }

    #[test]
    fn arrow_writer_string_dictionary() {
        // define schema
        #[allow(deprecated)]
        let schema = Arc::new(Schema::new(vec![Field::new_dict(
            "dictionary",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
            42,
            true,
        )]));

        // create some data
        let d: Int32DictionaryArray = [Some("alpha"), None, Some("beta"), Some("alpha")]
            .iter()
            .copied()
            .collect();

        // build a record batch
        one_column_roundtrip_with_schema(Arc::new(d), schema);
    }

    #[test]
    fn arrow_writer_primitive_dictionary() {
        // define schema
        #[allow(deprecated)]
        let schema = Arc::new(Schema::new(vec![Field::new_dict(
            "dictionary",
            DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::UInt32)),
            true,
            42,
            true,
        )]));

        // create some data
        let mut builder = PrimitiveDictionaryBuilder::<UInt8Type, UInt32Type>::new();
        builder.append(12345678).unwrap();
        builder.append_null();
        builder.append(22345678).unwrap();
        builder.append(12345678).unwrap();
        let d = builder.finish();

        one_column_roundtrip_with_schema(Arc::new(d), schema);
    }

    #[test]
    fn arrow_writer_decimal128_dictionary() {
        let integers = vec![12345, 56789, 34567];

        let keys = UInt8Array::from(vec![Some(0), None, Some(1), Some(2), Some(1)]);

        let values = Decimal128Array::from(integers.clone())
            .with_precision_and_scale(5, 2)
            .unwrap();

        let array = DictionaryArray::new(keys, Arc::new(values));
        one_column_roundtrip(Arc::new(array.clone()), true);

        let values = Decimal128Array::from(integers)
            .with_precision_and_scale(12, 2)
            .unwrap();

        let array = array.with_values(Arc::new(values));
        one_column_roundtrip(Arc::new(array), true);
    }

    #[test]
    fn arrow_writer_decimal256_dictionary() {
        let integers = vec![
            i256::from_i128(12345),
            i256::from_i128(56789),
            i256::from_i128(34567),
        ];

        let keys = UInt8Array::from(vec![Some(0), None, Some(1), Some(2), Some(1)]);

        let values = Decimal256Array::from(integers.clone())
            .with_precision_and_scale(5, 2)
            .unwrap();

        let array = DictionaryArray::new(keys, Arc::new(values));
        one_column_roundtrip(Arc::new(array.clone()), true);

        let values = Decimal256Array::from(integers)
            .with_precision_and_scale(12, 2)
            .unwrap();

        let array = array.with_values(Arc::new(values));
        one_column_roundtrip(Arc::new(array), true);
    }

    #[test]
    fn arrow_writer_string_dictionary_unsigned_index() {
        // define schema
        #[allow(deprecated)]
        let schema = Arc::new(Schema::new(vec![Field::new_dict(
            "dictionary",
            DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8)),
            true,
            42,
            true,
        )]));

        // create some data
        let d: UInt8DictionaryArray = [Some("alpha"), None, Some("beta"), Some("alpha")]
            .iter()
            .copied()
            .collect();

        one_column_roundtrip_with_schema(Arc::new(d), schema);
    }

    #[test]
    fn u32_min_max() {
        // check values roundtrip through parquet
        let src = [
            u32::MIN,
            u32::MIN + 1,
            (i32::MAX as u32) - 1,
            i32::MAX as u32,
            (i32::MAX as u32) + 1,
            u32::MAX - 1,
            u32::MAX,
        ];
        let values = Arc::new(UInt32Array::from_iter_values(src.iter().cloned()));
        let files = one_column_roundtrip(values, false);

        for file in files {
            // check statistics are valid
            let reader = SerializedFileReader::new(file).unwrap();
            let metadata = reader.metadata();

            let mut row_offset = 0;
            for row_group in metadata.row_groups() {
                assert_eq!(row_group.num_columns(), 1);
                let column = row_group.column(0);

                let num_values = column.num_values() as usize;
                let src_slice = &src[row_offset..row_offset + num_values];
                row_offset += column.num_values() as usize;

                let stats = column.statistics().unwrap();
                if let Statistics::Int32(stats) = stats {
                    assert_eq!(
                        *stats.min_opt().unwrap() as u32,
                        *src_slice.iter().min().unwrap()
                    );
                    assert_eq!(
                        *stats.max_opt().unwrap() as u32,
                        *src_slice.iter().max().unwrap()
                    );
                } else {
                    panic!("Statistics::Int32 missing")
                }
            }
        }
    }

    #[test]
    fn u64_min_max() {
        // check values roundtrip through parquet
        let src = [
            u64::MIN,
            u64::MIN + 1,
            (i64::MAX as u64) - 1,
            i64::MAX as u64,
            (i64::MAX as u64) + 1,
            u64::MAX - 1,
            u64::MAX,
        ];
        let values = Arc::new(UInt64Array::from_iter_values(src.iter().cloned()));
        let files = one_column_roundtrip(values, false);

        for file in files {
            // check statistics are valid
            let reader = SerializedFileReader::new(file).unwrap();
            let metadata = reader.metadata();

            let mut row_offset = 0;
            for row_group in metadata.row_groups() {
                assert_eq!(row_group.num_columns(), 1);
                let column = row_group.column(0);

                let num_values = column.num_values() as usize;
                let src_slice = &src[row_offset..row_offset + num_values];
                row_offset += column.num_values() as usize;

                let stats = column.statistics().unwrap();
                if let Statistics::Int64(stats) = stats {
                    assert_eq!(
                        *stats.min_opt().unwrap() as u64,
                        *src_slice.iter().min().unwrap()
                    );
                    assert_eq!(
                        *stats.max_opt().unwrap() as u64,
                        *src_slice.iter().max().unwrap()
                    );
                } else {
                    panic!("Statistics::Int64 missing")
                }
            }
        }
    }

    #[test]
    fn statistics_null_counts_only_nulls() {
        // check that null-count statistics for "only NULL"-columns are correct
        let values = Arc::new(UInt64Array::from(vec![None, None]));
        let files = one_column_roundtrip(values, true);

        for file in files {
            // check statistics are valid
            let reader = SerializedFileReader::new(file).unwrap();
            let metadata = reader.metadata();
            assert_eq!(metadata.num_row_groups(), 1);
            let row_group = metadata.row_group(0);
            assert_eq!(row_group.num_columns(), 1);
            let column = row_group.column(0);
            let stats = column.statistics().unwrap();
            assert_eq!(stats.null_count_opt(), Some(2));
        }
    }

    #[test]
    fn test_list_of_struct_roundtrip() {
        // define schema
        let int_field = Field::new("a", DataType::Int32, true);
        let int_field2 = Field::new("b", DataType::Int32, true);

        let int_builder = Int32Builder::with_capacity(10);
        let int_builder2 = Int32Builder::with_capacity(10);

        let struct_builder = StructBuilder::new(
            vec![int_field, int_field2],
            vec![Box::new(int_builder), Box::new(int_builder2)],
        );
        let mut list_builder = ListBuilder::new(struct_builder);

        // Construct the following array
        // [{a: 1, b: 2}], [], null, [null, null], [{a: null, b: 3}], [{a: 2, b: null}]

        // [{a: 1, b: 2}]
        let values = list_builder.values();
        values
            .field_builder::<Int32Builder>(0)
            .unwrap()
            .append_value(1);
        values
            .field_builder::<Int32Builder>(1)
            .unwrap()
            .append_value(2);
        values.append(true);
        list_builder.append(true);

        // []
        list_builder.append(true);

        // null
        list_builder.append(false);

        // [null, null]
        let values = list_builder.values();
        values
            .field_builder::<Int32Builder>(0)
            .unwrap()
            .append_null();
        values
            .field_builder::<Int32Builder>(1)
            .unwrap()
            .append_null();
        values.append(false);
        values
            .field_builder::<Int32Builder>(0)
            .unwrap()
            .append_null();
        values
            .field_builder::<Int32Builder>(1)
            .unwrap()
            .append_null();
        values.append(false);
        list_builder.append(true);

        // [{a: null, b: 3}]
        let values = list_builder.values();
        values
            .field_builder::<Int32Builder>(0)
            .unwrap()
            .append_null();
        values
            .field_builder::<Int32Builder>(1)
            .unwrap()
            .append_value(3);
        values.append(true);
        list_builder.append(true);

        // [{a: 2, b: null}]
        let values = list_builder.values();
        values
            .field_builder::<Int32Builder>(0)
            .unwrap()
            .append_value(2);
        values
            .field_builder::<Int32Builder>(1)
            .unwrap()
            .append_null();
        values.append(true);
        list_builder.append(true);

        let array = Arc::new(list_builder.finish());

        one_column_roundtrip(array, true);
    }

    fn row_group_sizes(metadata: &ParquetMetaData) -> Vec<i64> {
        metadata.row_groups().iter().map(|x| x.num_rows()).collect()
    }

    #[test]
    fn test_aggregates_records() {
        let arrays = [
            Int32Array::from((0..100).collect::<Vec<_>>()),
            Int32Array::from((0..50).collect::<Vec<_>>()),
            Int32Array::from((200..500).collect::<Vec<_>>()),
        ];

        let schema = Arc::new(Schema::new(vec![Field::new(
            "int",
            ArrowDataType::Int32,
            false,
        )]));

        let file = tempfile::tempfile().unwrap();

        let props = WriterProperties::builder()
            .set_max_row_group_size(200)
            .build();

        let mut writer =
            ArrowWriter::try_new(file.try_clone().unwrap(), schema.clone(), Some(props)).unwrap();

        for array in arrays {
            let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap();
            writer.write(&batch).unwrap();
        }

        writer.close().unwrap();

        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        assert_eq!(&row_group_sizes(builder.metadata()), &[200, 200, 50]);

        let batches = builder
            .with_batch_size(100)
            .build()
            .unwrap()
            .collect::<ArrowResult<Vec<_>>>()
            .unwrap();

        assert_eq!(batches.len(), 5);
        assert!(batches.iter().all(|x| x.num_columns() == 1));

        let batch_sizes: Vec<_> = batches.iter().map(|x| x.num_rows()).collect();

        assert_eq!(&batch_sizes, &[100, 100, 100, 100, 50]);

        let values: Vec<_> = batches
            .iter()
            .flat_map(|x| {
                x.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .cloned()
            })
            .collect();

        let expected_values: Vec<_> = [0..100, 0..50, 200..500].into_iter().flatten().collect();
        assert_eq!(&values, &expected_values)
    }

    #[test]
    fn complex_aggregate() {
        // Tests aggregating nested data
        let field_a = Arc::new(Field::new("leaf_a", DataType::Int32, false));
        let field_b = Arc::new(Field::new("leaf_b", DataType::Int32, true));
        let struct_a = Arc::new(Field::new(
            "struct_a",
            DataType::Struct(vec![field_a.clone(), field_b.clone()].into()),
            true,
        ));

        let list_a = Arc::new(Field::new("list", DataType::List(struct_a), true));
        let struct_b = Arc::new(Field::new(
            "struct_b",
            DataType::Struct(vec![list_a.clone()].into()),
            false,
        ));

        let schema = Arc::new(Schema::new(vec![struct_b]));

        // create nested data
        let field_a_array = Int32Array::from(vec![1, 2, 3, 4, 5, 6]);
        let field_b_array =
            Int32Array::from_iter(vec![Some(1), None, Some(2), None, None, Some(6)]);

        let struct_a_array = StructArray::from(vec![
            (field_a.clone(), Arc::new(field_a_array) as ArrayRef),
            (field_b.clone(), Arc::new(field_b_array) as ArrayRef),
        ]);

        let list_data = ArrayDataBuilder::new(list_a.data_type().clone())
            .len(5)
            .add_buffer(Buffer::from_iter(vec![
                0_i32, 1_i32, 1_i32, 3_i32, 3_i32, 5_i32,
            ]))
            .null_bit_buffer(Some(Buffer::from_iter(vec![
                true, false, true, false, true,
            ])))
            .child_data(vec![struct_a_array.into_data()])
            .build()
            .unwrap();

        let list_a_array = Arc::new(ListArray::from(list_data)) as ArrayRef;
        let struct_b_array = StructArray::from(vec![(list_a.clone(), list_a_array)]);

        let batch1 =
            RecordBatch::try_from_iter(vec![("struct_b", Arc::new(struct_b_array) as ArrayRef)])
                .unwrap();

        let field_a_array = Int32Array::from(vec![6, 7, 8, 9, 10]);
        let field_b_array = Int32Array::from_iter(vec![None, None, None, Some(1), None]);

        let struct_a_array = StructArray::from(vec![
            (field_a, Arc::new(field_a_array) as ArrayRef),
            (field_b, Arc::new(field_b_array) as ArrayRef),
        ]);

        let list_data = ArrayDataBuilder::new(list_a.data_type().clone())
            .len(2)
            .add_buffer(Buffer::from_iter(vec![0_i32, 4_i32, 5_i32]))
            .child_data(vec![struct_a_array.into_data()])
            .build()
            .unwrap();

        let list_a_array = Arc::new(ListArray::from(list_data)) as ArrayRef;
        let struct_b_array = StructArray::from(vec![(list_a, list_a_array)]);

        let batch2 =
            RecordBatch::try_from_iter(vec![("struct_b", Arc::new(struct_b_array) as ArrayRef)])
                .unwrap();

        let batches = &[batch1, batch2];

        // Verify data is as expected

        let expected = r#"
            +-------------------------------------------------------------------------------------------------------+
            | struct_b                                                                                              |
            +-------------------------------------------------------------------------------------------------------+
            | {list: [{leaf_a: 1, leaf_b: 1}]}                                                                      |
            | {list: }                                                                                              |
            | {list: [{leaf_a: 2, leaf_b: }, {leaf_a: 3, leaf_b: 2}]}                                               |
            | {list: }                                                                                              |
            | {list: [{leaf_a: 4, leaf_b: }, {leaf_a: 5, leaf_b: }]}                                                |
            | {list: [{leaf_a: 6, leaf_b: }, {leaf_a: 7, leaf_b: }, {leaf_a: 8, leaf_b: }, {leaf_a: 9, leaf_b: 1}]} |
            | {list: [{leaf_a: 10, leaf_b: }]}                                                                      |
            +-------------------------------------------------------------------------------------------------------+
        "#.trim().split('\n').map(|x| x.trim()).collect::<Vec<_>>().join("\n");

        let actual = pretty_format_batches(batches).unwrap().to_string();
        assert_eq!(actual, expected);

        // Write data
        let file = tempfile::tempfile().unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_size(6)
            .build();

        let mut writer =
            ArrowWriter::try_new(file.try_clone().unwrap(), schema, Some(props)).unwrap();

        for batch in batches {
            writer.write(batch).unwrap();
        }
        writer.close().unwrap();

        // Read Data
        // Should have written entire first batch and first row of second to the first row group
        // leaving a single row in the second row group

        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        assert_eq!(&row_group_sizes(builder.metadata()), &[6, 1]);

        let batches = builder
            .with_batch_size(2)
            .build()
            .unwrap()
            .collect::<ArrowResult<Vec<_>>>()
            .unwrap();

        assert_eq!(batches.len(), 4);
        let batch_counts: Vec<_> = batches.iter().map(|x| x.num_rows()).collect();
        assert_eq!(&batch_counts, &[2, 2, 2, 1]);

        let actual = pretty_format_batches(&batches).unwrap().to_string();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_arrow_writer_metadata() {
        let batch_schema = Schema::new(vec![Field::new("int32", DataType::Int32, false)]);
        let file_schema = batch_schema.clone().with_metadata(
            vec![("foo".to_string(), "bar".to_string())]
                .into_iter()
                .collect(),
        );

        let batch = RecordBatch::try_new(
            Arc::new(batch_schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as _],
        )
        .unwrap();

        let mut buf = Vec::with_capacity(1024);
        let mut writer = ArrowWriter::try_new(&mut buf, Arc::new(file_schema), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    #[test]
    fn test_arrow_writer_nullable() {
        let batch_schema = Schema::new(vec![Field::new("int32", DataType::Int32, false)]);
        let file_schema = Schema::new(vec![Field::new("int32", DataType::Int32, true)]);
        let file_schema = Arc::new(file_schema);

        let batch = RecordBatch::try_new(
            Arc::new(batch_schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as _],
        )
        .unwrap();

        let mut buf = Vec::with_capacity(1024);
        let mut writer = ArrowWriter::try_new(&mut buf, file_schema.clone(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let mut read = ParquetRecordBatchReader::try_new(Bytes::from(buf), 1024).unwrap();
        let back = read.next().unwrap().unwrap();
        assert_eq!(back.schema(), file_schema);
        assert_ne!(back.schema(), batch.schema());
        assert_eq!(back.column(0).as_ref(), batch.column(0).as_ref());
    }

    #[test]
    fn in_progress_accounting() {
        // define schema
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

        // create some data
        let a = Int32Array::from(vec![1, 2, 3, 4, 5]);

        // build a record batch
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)]).unwrap();

        let mut writer = ArrowWriter::try_new(vec![], batch.schema(), None).unwrap();

        // starts empty
        assert_eq!(writer.in_progress_size(), 0);
        assert_eq!(writer.in_progress_rows(), 0);
        assert_eq!(writer.memory_size(), 0);
        assert_eq!(writer.bytes_written(), 4); // Initial header
        writer.write(&batch).unwrap();

        // updated on write
        let initial_size = writer.in_progress_size();
        assert!(initial_size > 0);
        assert_eq!(writer.in_progress_rows(), 5);
        let initial_memory = writer.memory_size();
        assert!(initial_memory > 0);
        // memory estimate is larger than estimated encoded size
        assert!(
            initial_size <= initial_memory,
            "{initial_size} <= {initial_memory}"
        );

        // updated on second write
        writer.write(&batch).unwrap();
        assert!(writer.in_progress_size() > initial_size);
        assert_eq!(writer.in_progress_rows(), 10);
        assert!(writer.memory_size() > initial_memory);
        assert!(
            writer.in_progress_size() <= writer.memory_size(),
            "in_progress_size {} <= memory_size {}",
            writer.in_progress_size(),
            writer.memory_size()
        );

        // in progress tracking is cleared, but the overall data written is updated
        let pre_flush_bytes_written = writer.bytes_written();
        writer.flush().unwrap();
        assert_eq!(writer.in_progress_size(), 0);
        assert_eq!(writer.memory_size(), 0);
        assert!(writer.bytes_written() > pre_flush_bytes_written);

        writer.close().unwrap();
    }

    #[test]
    fn test_writer_all_null() {
        let a = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let b = Int32Array::new(vec![0; 5].into(), Some(NullBuffer::new_null(5)));
        let batch = RecordBatch::try_from_iter(vec![
            ("a", Arc::new(a) as ArrayRef),
            ("b", Arc::new(b) as ArrayRef),
        ])
        .unwrap();

        let mut buf = Vec::with_capacity(1024);
        let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let bytes = Bytes::from(buf);
        let options = ReadOptionsBuilder::new().with_page_index().build();
        let reader = SerializedFileReader::new_with_options(bytes, options).unwrap();
        let index = reader.metadata().offset_index().unwrap();

        assert_eq!(index.len(), 1);
        assert_eq!(index[0].len(), 2); // 2 columns
        assert_eq!(index[0][0].page_locations().len(), 1); // 1 page
        assert_eq!(index[0][1].page_locations().len(), 1); // 1 page
    }

    #[test]
    fn test_disabled_statistics_with_page() {
        let file_schema = Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Utf8, true),
        ]);
        let file_schema = Arc::new(file_schema);

        let batch = RecordBatch::try_new(
            file_schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "c", "d"])) as _,
                Arc::new(StringArray::from(vec!["w", "x", "y", "z"])) as _,
            ],
        )
        .unwrap();

        let props = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::None)
            .set_column_statistics_enabled("a".into(), EnabledStatistics::Page)
            .build();

        let mut buf = Vec::with_capacity(1024);
        let mut writer = ArrowWriter::try_new(&mut buf, file_schema.clone(), Some(props)).unwrap();
        writer.write(&batch).unwrap();

        let metadata = writer.close().unwrap();
        assert_eq!(metadata.row_groups.len(), 1);
        let row_group = &metadata.row_groups[0];
        assert_eq!(row_group.columns.len(), 2);
        // Column "a" has both offset and column index, as requested
        assert!(row_group.columns[0].offset_index_offset.is_some());
        assert!(row_group.columns[0].column_index_offset.is_some());
        // Column "b" should only have offset index
        assert!(row_group.columns[1].offset_index_offset.is_some());
        assert!(row_group.columns[1].column_index_offset.is_none());

        let options = ReadOptionsBuilder::new().with_page_index().build();
        let reader = SerializedFileReader::new_with_options(Bytes::from(buf), options).unwrap();

        let row_group = reader.get_row_group(0).unwrap();
        let a_col = row_group.metadata().column(0);
        let b_col = row_group.metadata().column(1);

        // Column chunk of column "a" should have chunk level statistics
        if let Statistics::ByteArray(byte_array_stats) = a_col.statistics().unwrap() {
            let min = byte_array_stats.min_opt().unwrap();
            let max = byte_array_stats.max_opt().unwrap();

            assert_eq!(min.as_bytes(), b"a");
            assert_eq!(max.as_bytes(), b"d");
        } else {
            panic!("expecting Statistics::ByteArray");
        }

        // The column chunk for column "b" shouldn't have statistics
        assert!(b_col.statistics().is_none());

        let offset_index = reader.metadata().offset_index().unwrap();
        assert_eq!(offset_index.len(), 1); // 1 row group
        assert_eq!(offset_index[0].len(), 2); // 2 columns

        let column_index = reader.metadata().column_index().unwrap();
        assert_eq!(column_index.len(), 1); // 1 row group
        assert_eq!(column_index[0].len(), 2); // 2 columns

        let a_idx = &column_index[0][0];
        assert!(matches!(a_idx, Index::BYTE_ARRAY(_)), "{a_idx:?}");
        let b_idx = &column_index[0][1];
        assert!(matches!(b_idx, Index::NONE), "{b_idx:?}");
    }

    #[test]
    fn test_disabled_statistics_with_chunk() {
        let file_schema = Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Utf8, true),
        ]);
        let file_schema = Arc::new(file_schema);

        let batch = RecordBatch::try_new(
            file_schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "c", "d"])) as _,
                Arc::new(StringArray::from(vec!["w", "x", "y", "z"])) as _,
            ],
        )
        .unwrap();

        let props = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::None)
            .set_column_statistics_enabled("a".into(), EnabledStatistics::Chunk)
            .build();

        let mut buf = Vec::with_capacity(1024);
        let mut writer = ArrowWriter::try_new(&mut buf, file_schema.clone(), Some(props)).unwrap();
        writer.write(&batch).unwrap();

        let metadata = writer.close().unwrap();
        assert_eq!(metadata.row_groups.len(), 1);
        let row_group = &metadata.row_groups[0];
        assert_eq!(row_group.columns.len(), 2);
        // Column "a" should only have offset index
        assert!(row_group.columns[0].offset_index_offset.is_some());
        assert!(row_group.columns[0].column_index_offset.is_none());
        // Column "b" should only have offset index
        assert!(row_group.columns[1].offset_index_offset.is_some());
        assert!(row_group.columns[1].column_index_offset.is_none());

        let options = ReadOptionsBuilder::new().with_page_index().build();
        let reader = SerializedFileReader::new_with_options(Bytes::from(buf), options).unwrap();

        let row_group = reader.get_row_group(0).unwrap();
        let a_col = row_group.metadata().column(0);
        let b_col = row_group.metadata().column(1);

        // Column chunk of column "a" should have chunk level statistics
        if let Statistics::ByteArray(byte_array_stats) = a_col.statistics().unwrap() {
            let min = byte_array_stats.min_opt().unwrap();
            let max = byte_array_stats.max_opt().unwrap();

            assert_eq!(min.as_bytes(), b"a");
            assert_eq!(max.as_bytes(), b"d");
        } else {
            panic!("expecting Statistics::ByteArray");
        }

        // The column chunk for column "b"  shouldn't have statistics
        assert!(b_col.statistics().is_none());

        let column_index = reader.metadata().column_index().unwrap();
        assert_eq!(column_index.len(), 1); // 1 row group
        assert_eq!(column_index[0].len(), 2); // 2 columns

        let a_idx = &column_index[0][0];
        assert!(matches!(a_idx, Index::NONE), "{a_idx:?}");
        let b_idx = &column_index[0][1];
        assert!(matches!(b_idx, Index::NONE), "{b_idx:?}");
    }

    #[test]
    fn test_arrow_writer_skip_metadata() {
        let batch_schema = Schema::new(vec![Field::new("int32", DataType::Int32, false)]);
        let file_schema = Arc::new(batch_schema.clone());

        let batch = RecordBatch::try_new(
            Arc::new(batch_schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as _],
        )
        .unwrap();
        let skip_options = ArrowWriterOptions::new().with_skip_arrow_metadata(true);

        let mut buf = Vec::with_capacity(1024);
        let mut writer =
            ArrowWriter::try_new_with_options(&mut buf, file_schema.clone(), skip_options).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let bytes = Bytes::from(buf);
        let reader_builder = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
        assert_eq!(file_schema, *reader_builder.schema());
        if let Some(key_value_metadata) = reader_builder
            .metadata()
            .file_metadata()
            .key_value_metadata()
        {
            assert!(!key_value_metadata
                .iter()
                .any(|kv| kv.key.as_str() == ARROW_SCHEMA_META_KEY));
        }
    }

    #[test]
    fn mismatched_schemas() {
        let batch_schema = Schema::new(vec![Field::new("count", DataType::Int32, false)]);
        let file_schema = Arc::new(Schema::new(vec![Field::new(
            "temperature",
            DataType::Float64,
            false,
        )]));

        let batch = RecordBatch::try_new(
            Arc::new(batch_schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as _],
        )
        .unwrap();

        let mut buf = Vec::with_capacity(1024);
        let mut writer = ArrowWriter::try_new(&mut buf, file_schema.clone(), None).unwrap();

        let err = writer.write(&batch).unwrap_err().to_string();
        assert_eq!(
            err,
            "Arrow: Incompatible type. Field 'temperature' has type Float64, array has type Int32"
        );
    }

    #[test]
    // https://github.com/apache/arrow-rs/issues/6988
    fn test_roundtrip_empty_schema() {
        // create empty record batch with empty schema
        let empty_batch = RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            vec![],
            &RecordBatchOptions::default().with_row_count(Some(0)),
        )
        .unwrap();

        // write to parquet
        let mut parquet_bytes: Vec<u8> = Vec::new();
        let mut writer =
            ArrowWriter::try_new(&mut parquet_bytes, empty_batch.schema(), None).unwrap();
        writer.write(&empty_batch).unwrap();
        writer.close().unwrap();

        // read from parquet
        let bytes = Bytes::from(parquet_bytes);
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
        assert_eq!(reader.schema(), &empty_batch.schema());
        let batches: Vec<_> = reader
            .build()
            .unwrap()
            .collect::<ArrowResult<Vec<_>>>()
            .unwrap();
        assert_eq!(batches.len(), 0);
    }
}
