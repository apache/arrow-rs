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

use crate::column::chunker::ContentDefinedChunker;

use bytes::Bytes;
use half::f16;
use std::io::Write;
use std::ops::Range;
use std::sync::{Arc, Mutex};
use std::vec::IntoIter;

use arrow_array::cast::AsArray;
use arrow_array::types::*;
use arrow_array::{ArrayRef, RecordBatch, RecordBatchWriter, new_empty_array};
use arrow_schema::{
    ArrowError, DataType as ArrowDataType, Field, FieldRef, IntervalUnit, SchemaRef, TimeUnit,
};

use super::schema::{
    add_encoded_arrow_schema_to_metadata, decimal_length_from_precision, validate_map_key_type,
};

use crate::arrow::ArrowSchemaConverter;
use crate::arrow::arrow_writer::byte_array::ByteArrayStorage;
use crate::basic::PageType;
use crate::column::page::{CompressedPage, PageWriteSpec, PageWriter};
use crate::column::page_encryption::PageEncryptor;
use crate::column::value_batch::{BatchSink, map_values};
use crate::column::value_selection::{DictionaryKeys, PhysicalValueSelection, ValueSelectionRef};
use crate::column::writer::encoder::ColumnChunkEncoder;
use crate::column::writer::encoder::{
    FixedLenByteArrayBatch, FixedLenByteArrayBatchPacker, FixedLenByteArraySink,
    FixedLenByteArraySource, PhysicalNumericSource, TypedColumnChunkEncoder,
};
use std::collections::HashSet;
type DistinctValuesSet = HashSet<u64>;
use crate::column::writer::{
    ByteBudgetTarget, ColumnCloseResult, ColumnWriteSource, ColumnWriter, GenericColumnWriter,
    get_column_writer,
};
use crate::data_type::{
    BoolType, DoubleType as ParquetDoubleType, FixedLenByteArrayType,
    FloatType as ParquetFloatType, Int32Type as ParquetInt32Type, Int64Type as ParquetInt64Type,
};
use crate::encodings::encoding::{BoolBatch, PackedFixedLenByteArrayBatch};
#[cfg(feature = "encryption")]
use crate::encryption::encrypt::FileEncryptor;
use crate::errors::{ParquetError, Result};
use crate::file::metadata::{KeyValue, ParquetMetaData, RowGroupMetaData};
use crate::file::properties::{WriterProperties, WriterPropertiesPtr};
use crate::file::writer::{SerializedFileWriter, SerializedRowGroupWriter};
use crate::parquet_thrift::{ThriftCompactOutputProtocol, WriteThrift};
use crate::schema::types::{ColumnDescPtr, SchemaDescriptor};
use levels::{ArrayLevels, LeafBatch, calculate_array_levels};

mod boolean;
mod byte_array;
mod fixed_len_byte_array;
mod levels;
mod numeric;

use boolean::BoolStorage;
use fixed_len_byte_array::FixedLenByteArrayStorage;
use numeric::{Float32Storage, Float64Storage, Int32Storage, Int64Storage};

#[doc(inline)]
pub use crate::column::page_store::{
    InMemoryPageStore, InMemoryPageStoreFactory, PageKey, PageStore, PageStoreArgs,
    PageStoreFactory,
};

/// Encodes [`RecordBatch`] to parquet
///
/// Writes Arrow `RecordBatch`es to a Parquet writer. Multiple [`RecordBatch`] will be encoded
/// to the same row group, up to `max_row_group_size` rows. Any remaining rows will be
/// flushed on close, leading the final row group in the output file to potentially
/// contain fewer than `max_row_group_size` rows
///
/// # Example: Writing `RecordBatch`es
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
/// # Memory Usage and Limiting
///
/// The nature of Parquet requires buffering of an entire row group before it can
/// be flushed to the underlying writer. Data is mostly buffered in its encoded
/// form, reducing memory usage. However, some data such as dictionary keys,
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
///
/// ## Type Compatibility
/// The writer can write Arrow [`RecordBatch`]s that are logically equivalent. This means that for
/// a  given column, the writer can accept multiple Arrow [`DataType`]s that contain the same
/// value type.
///
/// For example, the following [`DataType`]s are all logically equivalent and can be written
/// to the same column:
/// * String, LargeString, StringView
/// * Binary, LargeBinary, BinaryView
///
/// The writer can will also accept both native and dictionary encoded arrays if the dictionaries
/// contain compatible values.
/// ```
/// # use std::sync::Arc;
/// # use arrow_array::{DictionaryArray, LargeStringArray, RecordBatch, StringArray, UInt8Array};
/// # use arrow_schema::{DataType, Field, Schema};
/// # use parquet::arrow::arrow_writer::ArrowWriter;
/// let record_batch1 = RecordBatch::try_new(
///    Arc::new(Schema::new(vec![Field::new("col", DataType::LargeUtf8, false)])),
///    vec![Arc::new(LargeStringArray::from_iter_values(vec!["a", "b"]))]
///  )
/// .unwrap();
///
/// let mut buffer = Vec::new();
/// let mut writer = ArrowWriter::try_new(&mut buffer, record_batch1.schema(), None).unwrap();
/// writer.write(&record_batch1).unwrap();
///
/// let record_batch2 = RecordBatch::try_new(
///     Arc::new(Schema::new(vec![Field::new(
///         "col",
///         DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8)),
///          false,
///     )])),
///     vec![Arc::new(DictionaryArray::new(
///          UInt8Array::from_iter_values(vec![0, 1]),
///          Arc::new(StringArray::from_iter_values(vec!["b", "c"])),
///      ))],
///  )
///  .unwrap();
///  writer.write(&record_batch2).unwrap();
///  writer.close();
/// ```
pub struct ArrowWriter<W: Write> {
    /// Underlying Parquet writer
    writer: SerializedFileWriter<W>,

    /// The in-progress row group if any
    in_progress: Option<ArrowRowGroupWriter>,

    /// A copy of the Arrow schema.
    ///
    /// The schema is used to verify that each record batch written has the correct schema
    arrow_schema: SchemaRef,

    /// Creates new [`ArrowRowGroupWriter`] instances as required
    row_group_writer_factory: ArrowRowGroupWriterFactory,

    /// The maximum number of rows to write to each row group, or None for unlimited
    max_row_group_row_count: Option<usize>,

    /// The maximum size in bytes for a row group, or None for unlimited
    max_row_group_bytes: Option<usize>,

    /// CDC chunkers persisted across row groups (one per leaf column).
    cdc_chunkers: Option<Vec<ContentDefinedChunker>>,
}

impl<W: Write + Send> std::fmt::Debug for ArrowWriter<W> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let buffered_memory = self.in_progress_size();
        f.debug_struct("ArrowWriter")
            .field("writer", &self.writer)
            .field("in_progress_size", &format_args!("{buffered_memory} bytes"))
            .field("in_progress_rows", &self.in_progress_rows())
            .field("arrow_schema", &self.arrow_schema)
            .field("max_row_group_row_count", &self.max_row_group_row_count)
            .field("max_row_group_bytes", &self.max_row_group_bytes)
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
        for field in arrow_schema.fields() {
            validate_map_key_type(field.data_type())?;
        }
        let mut props = options.properties;

        let schema = if let Some(parquet_schema) = options.schema_descr {
            parquet_schema.clone()
        } else {
            let mut converter = ArrowSchemaConverter::new().with_coerce_types(props.coerce_types());
            if let Some(schema_root) = &options.schema_root {
                converter = converter.schema_root(schema_root);
            }

            converter.convert(&arrow_schema)?
        };

        if !options.skip_arrow_metadata {
            // add serialized arrow schema
            add_encoded_arrow_schema_to_metadata(&arrow_schema, &mut props);
        }

        let max_row_group_row_count = props.max_row_group_row_count();
        let max_row_group_bytes = props.max_row_group_bytes();

        let props_ptr = Arc::new(props);
        let file_writer =
            SerializedFileWriter::new(writer, schema.root_schema_ptr(), Arc::clone(&props_ptr))?;

        let mut row_group_writer_factory =
            ArrowRowGroupWriterFactory::new(&file_writer, arrow_schema.clone());
        if let Some(page_store_factory) = options.page_store_factory {
            row_group_writer_factory =
                row_group_writer_factory.with_page_store_factory(page_store_factory);
        }

        let cdc_chunkers = props_ptr
            .content_defined_chunking()
            .map(|opts| {
                file_writer
                    .schema_descr()
                    .columns()
                    .iter()
                    .map(|desc| ContentDefinedChunker::new(desc, opts))
                    .collect::<Result<Vec<_>>>()
            })
            .transpose()?;

        Ok(Self {
            writer: file_writer,
            in_progress: None,
            arrow_schema,
            row_group_writer_factory,
            max_row_group_row_count,
            max_row_group_bytes,
            cdc_chunkers,
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
    /// If this would cause the current row group to exceed [`WriterProperties::max_row_group_row_count`]
    /// rows or [`WriterProperties::max_row_group_bytes`] bytes, the contents of `batch` will be
    /// written to one or more row groups such that limits are respected.
    ///
    /// If both limits are `None`, all data is written to a single row group.
    /// If one limit is set, that limit is respected.
    /// If both limits are set, the lower bound (whichever triggers first) is respected.
    ///
    /// This will fail if the `batch`'s schema does not match the writer's schema.
    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        // Rows not yet handed to a row group writer. Splitting iterates here instead of
        // recursing, so a small row group limit over a large batch cannot exhaust the stack.
        let mut remaining = batch.clone();

        loop {
            let in_progress = match &mut self.in_progress {
                Some(in_progress) => in_progress,
                x => x.insert(
                    self.row_group_writer_factory
                        .create_row_group_writer(self.writer.flushed_row_groups().len())?,
                ),
            };
            let buffered_rows = in_progress.buffered_rows;

            // Leading rows of `remaining` that still fit in the current row group, when the
            // rest has to go to a later one.
            let mut split_at = match self.max_row_group_row_count {
                Some(max_rows) if buffered_rows + remaining.num_rows() > max_rows => {
                    Some(max_rows - buffered_rows)
                }
                _ => None,
            };

            // Check byte limit: if we have buffered data, use measured average row size
            // to split batch proactively before exceeding byte limit. Both limits apply to
            // the same rows, so measure against whatever the row limit already trimmed
            // `remaining` down to; otherwise the row limit would always win.
            let candidate_rows = split_at.unwrap_or_else(|| remaining.num_rows());

            if let Some(max_bytes) = self.max_row_group_bytes
                && buffered_rows > 0
            {
                let current_bytes = in_progress.get_estimated_total_bytes();

                if current_bytes >= max_bytes {
                    self.flush()?;
                    continue;
                }

                let avg_row_bytes = current_bytes / buffered_rows;
                if let Some(rows_that_fit) = (max_bytes - current_bytes).checked_div(avg_row_bytes)
                {
                    // At this point, `current_bytes < max_bytes` (checked above)
                    if candidate_rows > rows_that_fit {
                        if rows_that_fit > 0 {
                            split_at = Some(rows_that_fit);
                        } else {
                            self.flush()?;
                            continue;
                        }
                    }
                }
            }

            let rest = split_at.map(|to_write| {
                let rest = remaining.slice(to_write, remaining.num_rows() - to_write);
                remaining = remaining.slice(0, to_write);
                rest
            });

            let in_progress = self.in_progress.as_mut().unwrap();
            match self.cdc_chunkers.as_mut() {
                Some(chunkers) => in_progress.write_with_chunkers(&remaining, chunkers)?,
                None => in_progress.write(&remaining)?,
            }

            let should_flush = self
                .max_row_group_row_count
                .is_some_and(|max| in_progress.buffered_rows >= max)
                || self
                    .max_row_group_bytes
                    .is_some_and(|max| in_progress.get_estimated_total_bytes() >= max);

            if should_flush {
                self.flush()?
            }

            match rest {
                Some(rest) => remaining = rest,
                None => return Ok(()),
            }
        }
    }

    /// Writes the given buf bytes to the internal buffer.
    ///
    /// It's safe to use this method to write data to the underlying writer,
    /// because it will ensure that the buffering and byte‐counting layers are used.
    pub fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        self.writer.write_all(buf)
    }

    /// Flushes underlying writer
    pub fn sync(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }

    /// Flushes all buffered rows into a new row group
    ///
    /// Note the underlying writer is not flushed with this call.
    /// If this is a desired behavior, please call [`ArrowWriter::sync`].
    pub fn flush(&mut self) -> Result<()> {
        let Some(in_progress) = self.in_progress.take() else {
            return Ok(());
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
    /// **Warning**: if you write directly to this writer, you will skip
    /// the `TrackedWrite` buffering and byte‐counting layers. That’ll cause
    /// the file footer’s recorded offsets and sizes to diverge from reality,
    /// resulting in an unreadable or corrupted Parquet file.
    ///
    /// If you want to write safely to the underlying writer, use [`Self::write_all`].
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
    pub fn finish(&mut self) -> Result<ParquetMetaData> {
        self.flush()?;
        self.writer.finish()
    }

    /// Close and finalize the underlying Parquet writer
    pub fn close(mut self) -> Result<ParquetMetaData> {
        self.finish()
    }

    /// Converts this writer into a lower-level [`SerializedFileWriter`] and [`ArrowRowGroupWriterFactory`].
    ///
    /// Flushes any outstanding data before returning.
    ///
    /// This can be useful to provide more control over how files are written, for example
    /// to write columns in parallel. See the example on [`ArrowColumnWriter`].
    pub fn into_serialized_writer(
        mut self,
    ) -> Result<(SerializedFileWriter<W>, ArrowRowGroupWriterFactory)> {
        self.flush()?;
        Ok((self.writer, self.row_group_writer_factory))
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
    schema_descr: Option<SchemaDescriptor>,
    page_store_factory: Option<Arc<dyn PageStoreFactory>>,
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

    /// Sets the [`PageStoreFactory`] used to buffer completed pages while a row
    /// group is being written.
    ///
    /// The default implementation ([`InMemoryPageStore`]) buffers all completed
    /// pages on the heap until the row group is flushed, so peak write memory
    /// grows with the row group size. Using this API, pages can be spilled to a
    /// file or object storage instead, reducing peak write memory substantially
    /// at the expense of an extra write to and read from secondary storage.
    ///
    /// # Example: spilling pages to a temp file
    ///
    /// A simple spilling backend uses one temp file per column chunk; `put`
    /// appends the page and `take` reads it back.
    ///
    /// ```
    /// # use std::fs::File;
    /// # use std::io::{Read, Seek, SeekFrom, Write};
    /// # use std::sync::Arc;
    /// # use bytes::Bytes;
    /// # use arrow_array::{ArrayRef, Int64Array, RecordBatch};
    /// # use parquet::arrow::arrow_writer::{
    /// #     ArrowWriter, ArrowWriterOptions, PageKey, PageStore, PageStoreArgs, PageStoreFactory,
    /// # };
    /// # use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
    /// # use parquet::errors::Result;
    /// struct TempFilePageStore {
    ///     file: File,
    ///     /// Total size of the file
    ///     end: u64,
    ///     /// Location of pages: (offset, len)
    ///     locs: Vec<(u64, usize)>,
    /// }
    ///
    /// impl PageStore for TempFilePageStore {
    ///     fn put(&mut self, value: Bytes) -> Result<PageKey> {
    ///         // Append to the end of the file
    ///         self.file.seek(SeekFrom::Start(self.end))?;
    ///         self.file.write_all(&value)?;
    ///         let key = PageKey::new(self.locs.len() as u64);
    ///         self.locs.push((self.end, value.len()));
    ///         self.end += value.len() as u64;
    ///         Ok(key)
    ///     }
    ///
    ///     fn take(&mut self, key: PageKey) -> Result<Bytes> {
    ///         let (offset, len) = self.locs[key.get() as usize];
    ///         let mut buf = vec![0u8; len];
    ///         self.file.seek(SeekFrom::Start(offset))?;
    ///         self.file.read_exact(&mut buf)?;
    ///         Ok(Bytes::from(buf))
    ///     }
    /// }
    ///
    /// /// Factory for creating [`TempFilePageStore`]
    /// #[derive(Debug)]
    /// struct TempFilePageStoreFactory;
    ///
    /// impl PageStoreFactory for TempFilePageStoreFactory {
    ///     fn create(&self, args: &PageStoreArgs<'_>) -> Result<Box<dyn PageStore>> {
    ///         // `args` exposes the column index and descriptor (physical/logical
    ///         // type, path), so a real backend might choose to spill only large columns.
    ///         let _ = (args.column_index(), args.column_descriptor());
    ///         Ok(Box::new(TempFilePageStore {
    ///             file: tempfile::tempfile()?, // temp file is cleaned on drop
    ///             end: 0,
    ///             locs: Vec::new(),
    ///         }))
    ///     }
    /// }
    /// // write 1000 integers
    /// let col = Arc::new(Int64Array::from_iter_values(0..1000)) as ArrayRef;
    /// let to_write = RecordBatch::try_from_iter([("col", col)]).unwrap();
    ///
    /// let options =
    ///     ArrowWriterOptions::new().with_page_store_factory(Arc::new(TempFilePageStoreFactory));
    /// let mut buffer = Vec::new();
    /// let mut writer =
    ///     ArrowWriter::try_new_with_options(&mut buffer, to_write.schema(), options).unwrap();
    /// writer.write(&to_write).unwrap();
    /// writer.close().unwrap();
    ///
    /// // buffer now holds valid Parquet data, which can be read as normal:
    /// let mut reader = ParquetRecordBatchReader::try_new(Bytes::from(buffer), 1024).unwrap();
    /// assert_eq!(to_write, reader.next().unwrap().unwrap());
    /// ```
    pub fn with_page_store_factory(self, page_store_factory: Arc<dyn PageStoreFactory>) -> Self {
        Self {
            page_store_factory: Some(page_store_factory),
            ..self
        }
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

    /// Explicitly specify the Parquet schema to be used
    ///
    /// If omitted (the default), the [`ArrowSchemaConverter`] is used to compute the
    /// Parquet [`SchemaDescriptor`]. This may be used When the [`SchemaDescriptor`] is
    /// already known or must be calculated using custom logic.
    pub fn with_parquet_schema(self, schema_descr: SchemaDescriptor) -> Self {
        Self {
            schema_descr: Some(schema_descr),
            ..self
        }
    }
}

/// A single column chunk produced by [`ArrowColumnWriter`].
///
/// Holds the serialized page blobs (each page's header ‖ compressed data, in
/// write order) in a [`PageStore`], plus the handles needed to read them back,
/// in order, when the chunk is spliced into the output file.
struct ArrowColumnChunkData {
    length: usize,
    store: Box<dyn PageStore>,
    keys: Vec<PageKey>,
    /// Handles to the dictionary page's blobs (header then data) in the store.
    ///
    /// A dictionary page is produced at most once and bounded by
    /// `dict_page_size_limit`, but it must be written *first* in the chunk even
    /// though the data pages reach the writer before it (see
    /// [`PageWriter::defers_dictionary_ordering`]). Its header and data are `put`
    /// into the store like any other page — which keeps the store uniform, and
    /// lets an oversized dictionary page spill — and their handles are held apart
    /// so they can be emitted ahead of the data pages at splice.
    /// Empty for non-dictionary columns.
    dictionary_keys: Vec<PageKey>,
    /// Serialized length of the dictionary page (0 if there is none), recorded
    /// so the data pages can be shifted past it when offsets are rewritten to a
    /// dictionary-first layout at splice.
    dictionary_len: usize,
}

impl ArrowColumnChunkData {
    fn new(store: Box<dyn PageStore>) -> Self {
        Self {
            length: 0,
            store,
            keys: Vec::new(),
            dictionary_keys: Vec::new(),
            dictionary_len: 0,
        }
    }

    /// Append a data-page blob to the store, recording its handle in write
    /// order.
    fn push(&mut self, value: Bytes) -> Result<()> {
        let key = self.store.put(value)?;
        self.keys.push(key);
        Ok(())
    }

    /// Store a dictionary-page blob (header or data) in the page store,
    /// recording its handle (emitted first at splice) and accumulating its
    /// serialized length.
    fn push_dictionary(&mut self, value: Bytes) -> Result<()> {
        self.dictionary_len += value.len();
        let key = self.store.put(value)?;
        self.dictionary_keys.push(key);
        Ok(())
    }

    /// Bytes this chunk currently holds on the heap: whatever the store keeps
    /// resident (zero for a spilling backend).
    fn memory_size(&self) -> usize {
        self.store.memory_size()
    }
}

/// A streaming iterator over one column chunk's buffered page blobs, in final
/// file order: the dictionary page (if any) first, then the data pages.
///
/// Each blob is taken back out of the [`PageStore`] *as it is
/// consumed* and released immediately afterwards, so splicing a chunk into the
/// output file never materializes more than a single page in memory at a time.
/// This is what keeps the splice phase within the memory bound for a spilling
/// backend (an in-memory store already holds the bytes, so it is unaffected).
struct StreamingColumnChunkPages {
    store: Box<dyn PageStore>,
    /// Page handles in final file order: the dictionary page first (if any),
    /// then the data pages.
    keys: IntoIter<PageKey>,
}

impl StreamingColumnChunkPages {
    fn new(data: ArrowColumnChunkData) -> Self {
        // The dictionary page must be emitted first, ahead of the data pages,
        // even though it was the last page produced.
        let keys = if data.dictionary_keys.is_empty() {
            data.keys
        } else {
            let mut keys = Vec::with_capacity(data.dictionary_keys.len() + data.keys.len());
            keys.extend(data.dictionary_keys);
            keys.extend(data.keys);
            keys
        };
        Self {
            store: data.store,
            keys: keys.into_iter(),
        }
    }
}

impl Iterator for StreamingColumnChunkPages {
    type Item = Result<Bytes>;

    fn next(&mut self) -> Option<Self::Item> {
        let key = self.keys.next()?;
        Some(self.store.take(key))
    }
}

/// A shared [`ArrowColumnChunkData`]
///
/// This allows it to be owned by [`ArrowPageWriter`] whilst allowing access via
/// [`ArrowRowGroupWriter`] on flush, without requiring self-referential borrows
type SharedColumnChunk = Arc<Mutex<ArrowColumnChunkData>>;

struct ArrowPageWriter {
    buffer: SharedColumnChunk,
    #[cfg(feature = "encryption")]
    page_encryptor: Option<PageEncryptor>,
}

impl ArrowPageWriter {
    /// Create a page writer that buffers completed pages in `store`.
    fn new(store: Box<dyn PageStore>) -> Self {
        Self {
            buffer: Arc::new(Mutex::new(ArrowColumnChunkData::new(store))),
            #[cfg(feature = "encryption")]
            page_encryptor: None,
        }
    }

    #[cfg(feature = "encryption")]
    pub fn with_encryptor(mut self, page_encryptor: Option<PageEncryptor>) -> Self {
        self.page_encryptor = page_encryptor;
        self
    }

    #[cfg(feature = "encryption")]
    fn page_encryptor_mut(&mut self) -> Option<&mut PageEncryptor> {
        self.page_encryptor.as_mut()
    }

    // Mirrors the signature of the encryption-enabled version above, so that the
    // callers do not need a `cfg` of their own.
    #[cfg(not(feature = "encryption"))]
    #[expect(
        clippy::needless_pass_by_ref_mut,
        reason = "mirrors the encryption-enabled signature"
    )]
    fn page_encryptor_mut(&mut self) -> Option<&mut PageEncryptor> {
        None
    }
}

impl PageWriter for ArrowPageWriter {
    fn write_page(&mut self, page: CompressedPage) -> Result<PageWriteSpec> {
        let page = match self.page_encryptor_mut() {
            Some(page_encryptor) => page_encryptor.encrypt_compressed_page(page)?,
            None => page,
        };

        let page_header = page.to_thrift_header()?;
        let header = {
            let mut header = Vec::with_capacity(1024);

            match self.page_encryptor_mut() {
                Some(page_encryptor) => {
                    page_encryptor.encrypt_page_header(&page_header, &mut header)?;
                    if page.compressed_page().is_data_page() {
                        page_encryptor.increment_page();
                    }
                }
                None => {
                    let mut protocol = ThriftCompactOutputProtocol::new(&mut header);
                    page_header.write_thrift(&mut protocol)?;
                }
            }

            Bytes::from(header)
        };

        let mut buf = self.buffer.try_lock().unwrap();

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
        if spec.page_type == PageType::DICTIONARY_PAGE {
            // Recorded apart from the data pages so it is emitted first at
            // splice — see `ArrowColumnChunkData::dictionary_keys`.
            buf.push_dictionary(header)?;
            buf.push_dictionary(data)?;
        } else {
            buf.push(header)?;
            buf.push(data)?;
        }

        Ok(spec)
    }

    fn defers_dictionary_ordering(&self) -> bool {
        // The Arrow chunk is buffered in full and spliced at row-group flush, so
        // data pages may be accepted before the dictionary page and reordered
        // then. This lets `GenericColumnWriter` stream dictionary-column data
        // pages straight through instead of buffering them in memory.
        true
    }

    fn buffered_memory_size(&self) -> usize {
        // Only what is actually resident: a spilling store reports ~0 here even
        // though the chunk's bytes have all passed through it.
        self.buffer.try_lock().unwrap().memory_size()
    }

    fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

/// A leaf column that can be encoded by [`ArrowColumnWriter`]
#[derive(Debug)]
pub struct ArrowLeafColumn(ArrayLevels);

/// Computes the [`ArrowLeafColumn`] for a potentially nested [`ArrayRef`]
///
/// This function can be used to encode individual columns in parallel.
/// See example on [`ArrowColumnWriter`]
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
    /// Returns the [`ColumnCloseResult`] produced when the chunk was closed.
    ///
    /// Exposes encoding information, collected statistics, and the optional
    /// [`ColumnIndexMetaData`](crate::file::page_index::column_index::ColumnIndexMetaData)
    /// / [`OffsetIndexMetaData`](crate::file::page_index::offset_index::OffsetIndexMetaData)
    /// gathered for the column chunk.
    pub fn close(&self) -> &ColumnCloseResult {
        &self.close
    }

    /// Returns a mutable reference to the [`ColumnCloseResult`].
    ///
    /// This allows callers to mutate the close result before the chunk is
    /// appended to a row group — for example, clearing `column_index` or
    /// `bloom_filter` based on a dynamic rule that inspects the encodings and
    /// collected page statistics.
    pub fn close_mut(&mut self) -> &mut ColumnCloseResult {
        &mut self.close
    }

    /// Splices this column's buffered pages into the row group, streaming them
    /// back out of the [`PageStore`] one page at a time.
    pub fn append_to_row_group<W: Write + Send>(
        self,
        writer: &mut SerializedRowGroupWriter<'_, W>,
    ) -> Result<()> {
        let ArrowColumnChunk { data, close } = self;

        // The dictionary page is produced *after* the data pages on this path (so
        // they can stream straight through) but must be written *first*, so move
        // it ahead of the data pages in the recorded offsets before the splice.
        let close = close.update_dictionary_location(data.dictionary_len)?;

        let pages = StreamingColumnChunkPages::new(data);
        writer.append_column_from_pages(pages, close)
    }
}

/// Encodes [`ArrowLeafColumn`] to [`ArrowColumnChunk`]
///
/// `ArrowColumnWriter` instances can be created using an [`ArrowRowGroupWriterFactory`];
///
/// Note: This is a low-level interface for applications that require
/// fine-grained control of encoding (e.g. encoding using multiple threads),
/// see [`ArrowWriter`] for a higher-level interface
///
/// # Example: Encoding two Arrow Array's in Parallel
/// ```
/// // The arrow schema
/// # use std::sync::Arc;
/// # use arrow_array::*;
/// # use arrow_schema::*;
/// # use parquet::arrow::ArrowSchemaConverter;
/// # use parquet::arrow::arrow_writer::{compute_leaves, ArrowColumnChunk, ArrowLeafColumn, ArrowRowGroupWriterFactory};
/// # use parquet::file::properties::WriterProperties;
/// # use parquet::file::writer::{SerializedFileWriter, SerializedRowGroupWriter};
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
/// // Create parquet writer
/// let root_schema = parquet_schema.root_schema_ptr();
/// // write to memory in the example, but this could be a File
/// let mut out = Vec::with_capacity(1024);
/// let mut writer = SerializedFileWriter::new(&mut out, root_schema, props.clone())
///   .unwrap();
///
/// // Create a factory for building Arrow column writers
/// let row_group_factory = ArrowRowGroupWriterFactory::new(&writer, Arc::clone(&schema));
/// // Create column writers for the 0th row group
/// let col_writers = row_group_factory.create_column_writers(0).unwrap();
///
/// // Spawn a worker thread for each column
/// //
/// // Note: This is for demonstration purposes, a thread-pool e.g. rayon or tokio, would be better.
/// // The `map` produces an iterator of type `tuple of (thread handle, send channel)`.
/// let mut workers: Vec<_> = col_writers
///     .into_iter()
///     .map(|mut col_writer| {
///         let (send, recv) = std::sync::mpsc::channel::<ArrowLeafColumn>();
///         let handle = std::thread::spawn(move || {
///             // receive Arrays to encode via the channel
///             for col in recv {
///                 col_writer.write(&col)?;
///             }
///             // once the input is complete, close the writer
///             // to return the newly created ArrowColumnChunk
///             col_writer.close()
///         });
///         (handle, send)
///     })
///     .collect();
///
/// // Start row group
/// let mut row_group_writer: SerializedRowGroupWriter<'_, _> = writer
///   .next_row_group()
///   .unwrap();
///
/// // Create some example input columns to encode
/// let to_write = vec![
///     Arc::new(Int32Array::from_iter_values([1, 2, 3])) as _,
///     Arc::new(Float32Array::from_iter_values([1., 45., -1.])) as _,
/// ];
///
/// // Send the input columns to the workers
/// let mut worker_iter = workers.iter_mut();
/// for (arr, field) in to_write.iter().zip(&schema.fields) {
///     for leaves in compute_leaves(field, arr).unwrap() {
///         worker_iter.next().unwrap().1.send(leaves).unwrap();
///     }
/// }
///
/// // Wait for the workers to complete encoding, and append
/// // the resulting column chunks to the row group (and the file)
/// for (handle, send) in workers {
///     drop(send); // Drop send side to signal termination
///     // wait for the worker to send the completed chunk
///     let chunk: ArrowColumnChunk = handle.join().unwrap().unwrap();
///     chunk.append_to_row_group(&mut row_group_writer).unwrap();
/// }
/// // Close the row group which writes to the underlying file
/// row_group_writer.close().unwrap();
///
/// let metadata = writer.close().unwrap();
/// assert_eq!(metadata.file_metadata().num_rows(), 3);
/// ```
pub struct ArrowColumnWriter {
    writer: ColumnWriter<'static>,
    chunk: SharedColumnChunk,
    /// Non-null value hashes accumulated across all writes for this column's row group.
    /// `None` when tracking is disabled via [`WriterProperties::write_row_group_number_distinct_values`].
    distinct_values_seen: Option<DistinctValuesSet>,
}

impl std::fmt::Debug for ArrowColumnWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArrowColumnWriter").finish_non_exhaustive()
    }
}

impl ArrowColumnWriter {
    /// Write an [`ArrowLeafColumn`]
    pub fn write(&mut self, col: &ArrowLeafColumn) -> Result<()> {
        col.0.validate()?;
        self.writer.start_arrow_source();
        self.write_internal(&col.0)
    }

    /// Write with content-defined chunking, inserting page flushes at chunk boundaries.
    fn write_with_chunker(
        &mut self,
        col: &ArrowLeafColumn,
        chunker: &mut ContentDefinedChunker,
    ) -> Result<()> {
        let levels = &col.0;
        levels.validate()?;
        self.writer.start_arrow_source();
        let chunks = chunker.get_arrow_chunks(
            levels.def_level_data().as_ref(),
            levels.rep_level_data().as_ref(),
            levels.array(),
        )?;

        let num_chunks = chunks.len();
        for (i, chunk) in chunks.iter().enumerate() {
            let chunk_levels = levels.slice_for_chunk(chunk);
            self.write_internal(&chunk_levels)?;

            // Add a page break after each chunk except the last
            if i + 1 < num_chunks {
                self.writer.add_data_page()?;
            }
        }
        Ok(())
    }

    fn write_internal(&mut self, levels: &ArrayLevels) -> Result<()> {
        let batch = levels.leaf_batch();
        if let Some(seen) = &mut self.distinct_values_seen {
            let (array, selection) =
                dispatch_physical_input(batch.array(), batch.value_selection());
            update_distinct_values_seen(array, selection, seen);
        }
        write_leaf(&mut self.writer, batch)?;
        Ok(())
    }

    /// Close this column returning the written [`ArrowColumnChunk`]
    ///
    /// # Errors
    ///
    /// Returns an error if the column could not be finalised, or if another thread
    /// panicked while holding the column chunk. The caller cannot cause either.
    pub fn close(mut self) -> Result<ArrowColumnChunk> {
        if let Some(seen) = &self.distinct_values_seen
            && !seen.is_empty()
        {
            self.writer.set_distinct_count_override(seen.len() as u64);
        }
        let close = self.writer.close()?;
        let chunk = Arc::try_unwrap(self.chunk)
            .map_err(|_| general_err!("Internal Error: the column chunk is still shared"))?;
        let data = chunk
            .into_inner()
            .map_err(|_| general_err!("The column chunk lock is poisoned"))?;
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
        self.writer.memory_size()
    }

    /// Returns the estimated total encoded bytes for this column writer.
    ///
    /// This includes:
    /// 1. Data buffered in encoded form
    /// 2. An estimate of how large the data buffered in un-encoded form would be once encoded
    ///
    /// This value should be less than or equal to [`Self::memory_size`]
    pub fn get_estimated_total_bytes(&self) -> usize {
        self.writer.get_estimated_total_bytes() as _
    }
}

/// Associates a top-level Arrow field with its range of Parquet leaf writers.
#[derive(Debug)]
struct ArrowTopLevelWriterSpec {
    /// The writer schema's field supplies the target nullability and nesting,
    /// while each batch can use a compatible physical layout (dictionary,
    /// run-end, string/binary offset width, or view).
    field: FieldRef,
    leaf_range: Range<usize>,
}

#[derive(Debug)]
enum ArrowWriteSchemaPlanError {
    General(String),
    Nyi(String),
}

impl ArrowWriteSchemaPlanError {
    fn to_parquet_error(&self) -> ParquetError {
        match self {
            Self::General(message) => ParquetError::General(message.clone()),
            Self::Nyi(message) => ParquetError::NYI(message.clone()),
        }
    }

    fn from_parquet_error(error: ParquetError) -> Self {
        match error {
            ParquetError::NYI(message) => Self::Nyi(message),
            ParquetError::General(message) => Self::General(message),
            error => Self::General(error.to_string()),
        }
    }
}

/// Immutable schema glue shared by every row group.
///
/// This deliberately caches only schema fields and Parquet descriptors, never
/// arrays or per-batch level/value plans.
#[derive(Debug)]
struct ArrowWriteSchemaPlan {
    fields: Box<[ArrowTopLevelWriterSpec]>,
    leaves: Box<[ColumnDescPtr]>,
}

impl ArrowWriteSchemaPlan {
    fn try_new(
        parquet: &SchemaDescriptor,
        arrow: &SchemaRef,
    ) -> std::result::Result<Self, ArrowWriteSchemaPlanError> {
        let parquet_leaves = parquet.columns();
        let mut leaf_count = 0;
        let mut fields = Vec::with_capacity(arrow.fields.len());

        for field in &arrow.fields {
            validate_map_key_type(field.data_type())
                .map_err(ArrowWriteSchemaPlanError::from_parquet_error)?;
            let start = leaf_count;
            leaf_count += compute_leaves(field, &new_empty_array(field.data_type()))
                .map_err(ArrowWriteSchemaPlanError::from_parquet_error)?
                .len();
            fields.push(ArrowTopLevelWriterSpec {
                field: Arc::clone(field),
                leaf_range: start..leaf_count,
            });
        }

        if leaf_count != parquet_leaves.len() {
            return Err(ArrowWriteSchemaPlanError::General(format!(
                "Arrow schema maps to {} leaf columns but Parquet schema has {}",
                leaf_count,
                parquet_leaves.len()
            )));
        }

        Ok(Self {
            fields: fields.into_boxed_slice(),
            leaves: parquet_leaves.to_vec().into_boxed_slice(),
        })
    }
}

/// Encodes [`RecordBatch`] to a parquet row group
///
/// Note: this structure is created by [`ArrowRowGroupWriterFactory`] internally used to
/// create [`ArrowRowGroupWriter`]s, but it is not exposed publicly.
///
/// See the example on [`ArrowColumnWriter`] for how to encode columns in parallel
#[derive(Debug)]
struct ArrowRowGroupWriter {
    writers: Vec<ArrowColumnWriter>,
    schema_plan: Arc<ArrowWriteSchemaPlan>,
    buffered_rows: usize,
}

impl ArrowRowGroupWriter {
    fn new(writers: Vec<ArrowColumnWriter>, schema_plan: Arc<ArrowWriteSchemaPlan>) -> Self {
        debug_assert_eq!(writers.len(), schema_plan.leaves.len());
        Self {
            writers,
            schema_plan,
            buffered_rows: 0,
        }
    }

    fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.validate_batch_shape(batch, None)?;
        self.buffered_rows += batch.num_rows();

        for (column_idx, field) in self.schema_plan.fields.iter().enumerate() {
            let leaves = compute_leaves(field.field.as_ref(), batch.column(column_idx))?;
            self.validate_leaf_count(field, leaves.len())?;
            for (offset, leaf) in leaves.into_iter().enumerate() {
                self.writers[field.leaf_range.start + offset].write(&leaf)?;
            }
        }
        Ok(())
    }

    fn write_with_chunkers(
        &mut self,
        batch: &RecordBatch,
        chunkers: &mut [ContentDefinedChunker],
    ) -> Result<()> {
        self.validate_batch_shape(batch, Some(chunkers.len()))?;
        self.buffered_rows += batch.num_rows();

        for (column_idx, field) in self.schema_plan.fields.iter().enumerate() {
            let leaves = compute_leaves(field.field.as_ref(), batch.column(column_idx))?;
            self.validate_leaf_count(field, leaves.len())?;
            for (offset, leaf) in leaves.into_iter().enumerate() {
                let leaf_idx = field.leaf_range.start + offset;
                self.writers[leaf_idx].write_with_chunker(&leaf, &mut chunkers[leaf_idx])?;
            }
        }
        Ok(())
    }

    fn validate_batch_shape(
        &self,
        batch: &RecordBatch,
        chunker_count: Option<usize>,
    ) -> Result<()> {
        if batch.num_columns() != self.schema_plan.fields.len() {
            return Err(ParquetError::ArrowError(format!(
                "Incompatible schema: writer has {} top-level fields but batch has {} columns",
                self.schema_plan.fields.len(),
                batch.num_columns()
            )));
        }
        if self.writers.len() != self.schema_plan.leaves.len() {
            return Err(ParquetError::General(format!(
                "Arrow row-group writer has {} column writers for {} planned leaves",
                self.writers.len(),
                self.schema_plan.leaves.len()
            )));
        }
        if let Some(actual) = chunker_count
            && actual != self.schema_plan.leaves.len()
        {
            return Err(ParquetError::General(format!(
                "content-defined chunking has {actual} chunkers for {} planned leaves",
                self.schema_plan.leaves.len()
            )));
        }
        Ok(())
    }

    fn validate_leaf_count(&self, field: &ArrowTopLevelWriterSpec, actual: usize) -> Result<()> {
        let expected = field.leaf_range.end - field.leaf_range.start;
        if actual != expected {
            return Err(ParquetError::ArrowError(format!(
                "Incompatible schema: field '{}' produced {actual} leaf columns but writer expects {expected}",
                field.field.name()
            )));
        }
        Ok(())
    }

    /// Returns the estimated total encoded bytes for this row group
    fn get_estimated_total_bytes(&self) -> usize {
        self.writers
            .iter()
            .map(|x| x.get_estimated_total_bytes())
            .sum()
    }

    fn close(self) -> Result<Vec<ArrowColumnChunk>> {
        self.writers
            .into_iter()
            .map(|writer| writer.close())
            .collect()
    }
}

/// Factory that creates new column writers for each row group in the Parquet file.
///
/// You can create this structure via an [`ArrowWriter::into_serialized_writer`].
/// See the example on [`ArrowColumnWriter`] for how to encode columns in parallel
#[derive(Debug)]
pub struct ArrowRowGroupWriterFactory {
    schema_plan: std::result::Result<Arc<ArrowWriteSchemaPlan>, ArrowWriteSchemaPlanError>,
    props: WriterPropertiesPtr,
    page_store_factory: Arc<dyn PageStoreFactory>,
    #[cfg(feature = "encryption")]
    file_encryptor: Option<Arc<FileEncryptor>>,
}

impl ArrowRowGroupWriterFactory {
    /// Create a new [`ArrowRowGroupWriterFactory`] for the provided file writer and Arrow schema
    pub fn new<W: Write + Send>(
        file_writer: &SerializedFileWriter<W>,
        arrow_schema: SchemaRef,
    ) -> Self {
        let schema_plan =
            ArrowWriteSchemaPlan::try_new(file_writer.schema_descr_ptr().as_ref(), &arrow_schema)
                .map(Arc::new);
        let props = Arc::clone(file_writer.properties());
        Self {
            schema_plan,
            props,
            page_store_factory: Arc::new(InMemoryPageStoreFactory),
            #[cfg(feature = "encryption")]
            file_encryptor: file_writer.file_encryptor(),
        }
    }

    /// Set the [`PageStoreFactory`] used to allocate the buffer for each column
    /// chunk, e.g. to spill completed pages to a temp file or object storage
    /// instead of the heap. Defaults to [`InMemoryPageStoreFactory`].
    pub fn with_page_store_factory(
        mut self,
        page_store_factory: Arc<dyn PageStoreFactory>,
    ) -> Self {
        self.page_store_factory = page_store_factory;
        self
    }

    fn create_row_group_writer(&self, row_group_index: usize) -> Result<ArrowRowGroupWriter> {
        let schema_plan = Arc::clone(self.schema_plan()?);
        let writers = self.create_column_writers_from_plan(row_group_index, &schema_plan)?;
        Ok(ArrowRowGroupWriter::new(writers, schema_plan))
    }

    /// Create column writers for a new row group, with the given row group index
    pub fn create_column_writers(&self, row_group_index: usize) -> Result<Vec<ArrowColumnWriter>> {
        self.create_column_writers_from_plan(row_group_index, self.schema_plan()?)
    }

    fn schema_plan(&self) -> Result<&Arc<ArrowWriteSchemaPlan>> {
        match &self.schema_plan {
            Ok(plan) => Ok(plan),
            Err(error) => Err(error.to_parquet_error()),
        }
    }

    fn create_column_writers_from_plan(
        &self,
        row_group_index: usize,
        schema_plan: &ArrowWriteSchemaPlan,
    ) -> Result<Vec<ArrowColumnWriter>> {
        self.column_writer_factory(row_group_index)
            .create_column_writers(&schema_plan.leaves, &self.props)
    }

    #[cfg(feature = "encryption")]
    fn column_writer_factory(&self, row_group_idx: usize) -> ArrowColumnWriterFactory {
        ArrowColumnWriterFactory::new()
            .with_page_store_factory(self.page_store_factory.clone())
            .with_file_encryptor(row_group_idx, self.file_encryptor.clone())
    }

    #[cfg(not(feature = "encryption"))]
    fn column_writer_factory(&self, _row_group_idx: usize) -> ArrowColumnWriterFactory {
        ArrowColumnWriterFactory::new().with_page_store_factory(self.page_store_factory.clone())
    }
}

/// Creates [`ArrowColumnWriter`] instances
struct ArrowColumnWriterFactory {
    /// Allocates the per-column-chunk [`PageStore`] backing each page writer.
    page_store_factory: Arc<dyn PageStoreFactory>,
    #[cfg(feature = "encryption")]
    row_group_index: usize,
    #[cfg(feature = "encryption")]
    file_encryptor: Option<Arc<FileEncryptor>>,
}

impl ArrowColumnWriterFactory {
    pub fn new() -> Self {
        Self {
            page_store_factory: Arc::new(InMemoryPageStoreFactory),
            #[cfg(feature = "encryption")]
            row_group_index: 0,
            #[cfg(feature = "encryption")]
            file_encryptor: None,
        }
    }

    /// Use `page_store_factory` to allocate the buffer for each column chunk.
    pub fn with_page_store_factory(
        mut self,
        page_store_factory: Arc<dyn PageStoreFactory>,
    ) -> Self {
        self.page_store_factory = page_store_factory;
        self
    }

    #[cfg(feature = "encryption")]
    pub fn with_file_encryptor(
        mut self,
        row_group_index: usize,
        file_encryptor: Option<Arc<FileEncryptor>>,
    ) -> Self {
        self.row_group_index = row_group_index;
        self.file_encryptor = file_encryptor;
        self
    }

    #[cfg(feature = "encryption")]
    fn create_page_writer(
        &self,
        column_descriptor: &ColumnDescPtr,
        column_index: usize,
    ) -> Result<Box<ArrowPageWriter>> {
        let column_path = column_descriptor.path().string();
        let page_encryptor = PageEncryptor::create_if_column_encrypted(
            self.file_encryptor.as_ref(),
            self.row_group_index,
            column_index,
            &column_path,
        )?;
        let args = PageStoreArgs::new(column_index, column_descriptor);
        let store = self.page_store_factory.create(&args)?;
        Ok(Box::new(
            ArrowPageWriter::new(store).with_encryptor(page_encryptor),
        ))
    }

    #[cfg(not(feature = "encryption"))]
    fn create_page_writer(
        &self,
        column_descriptor: &ColumnDescPtr,
        column_index: usize,
    ) -> Result<Box<ArrowPageWriter>> {
        let args = PageStoreArgs::new(column_index, column_descriptor);
        let store = self.page_store_factory.create(&args)?;
        Ok(Box::new(ArrowPageWriter::new(store)))
    }

    fn create_column_writers(
        &self,
        descriptors: &[ColumnDescPtr],
        props: &WriterPropertiesPtr,
    ) -> Result<Vec<ArrowColumnWriter>> {
        let mut out = Vec::with_capacity(descriptors.len());
        for (column_index, descriptor) in descriptors.iter().enumerate() {
            out.push(self.create_column_writer(descriptor, props, column_index)?);
        }
        Ok(out)
    }

    fn create_column_writer(
        &self,
        descriptor: &ColumnDescPtr,
        props: &WriterPropertiesPtr,
        column_index: usize,
    ) -> Result<ArrowColumnWriter> {
        let page_writer = self.create_page_writer(descriptor, column_index)?;
        let chunk = page_writer.buffer.clone();
        let writer = get_column_writer(Arc::clone(descriptor), Arc::clone(props), page_writer);
        Ok(ArrowColumnWriter {
            writer,
            chunk,
            distinct_values_seen: props
                .write_row_group_number_distinct_values()
                .then(HashSet::new),
        })
    }
}

trait ArrowPhysicalBridge<'a>: Copy {
    type ColumnEncoder: ColumnChunkEncoder;

    /// Bind the final physical Arrow layout after run/dictionary wrappers have
    /// been lowered. The returned descriptor borrows the input synchronously;
    /// it never retains an `ArrayRef`.
    fn bind(column: &'a dyn arrow_array::Array) -> Result<Self>;

    fn write_values(
        self,
        encoder: &mut Self::ColumnEncoder,
        selection: PhysicalValueSelection<'a>,
    ) -> Result<()>;

    fn count_variable_width_within_byte_budget(
        self,
        _encoder: &Self::ColumnEncoder,
        _selection: PhysicalValueSelection<'a>,
        _budget: usize,
        _target: ByteBudgetTarget,
    ) -> Option<usize> {
        None
    }
}

/// One stack-bound physical descriptor shared by every page window of a leaf.
/// Keeping the comparatively large composed selection here makes the Copy
/// window passed through `GenericColumnWriter` only a pointer and two indices.
#[derive(Clone, Copy)]
struct ArrowPhysicalBinding<'a, B>
where
    B: ArrowPhysicalBridge<'a>,
{
    storage: B,
    selection: PhysicalValueSelection<'a>,
}

impl<'a, B> ArrowPhysicalBinding<'a, B>
where
    B: ArrowPhysicalBridge<'a>,
{
    fn bind(column: &'a dyn arrow_array::Array, selection: ValueSelectionRef<'a>) -> Result<Self> {
        let (values, selection) = dispatch_physical_input(column, selection);
        Ok(Self {
            storage: B::bind(values)?,
            selection,
        })
    }

    #[inline]
    fn source(&self) -> ArrowPhysicalSource<'_, 'a, B> {
        ArrowPhysicalSource {
            binding: self,
            offset: 0,
            len: self.selection.len(),
        }
    }
}

#[derive(Clone, Copy)]
struct ArrowPhysicalSource<'binding, 'a, B>
where
    B: ArrowPhysicalBridge<'a>,
{
    binding: &'binding ArrowPhysicalBinding<'a, B>,
    offset: usize,
    len: usize,
}

impl<'a, B> ArrowPhysicalSource<'_, 'a, B>
where
    B: ArrowPhysicalBridge<'a>,
{
    #[inline]
    fn len(self) -> usize {
        self.len
    }

    #[inline]
    fn slice(self, offset: usize, len: usize) -> Self {
        debug_assert!(offset <= self.len && len <= self.len - offset);
        if offset == 0 && len == self.len {
            return self;
        }
        Self {
            offset: self.offset + offset,
            len,
            binding: self.binding,
        }
    }

    #[inline]
    fn selection(self) -> PhysicalValueSelection<'a> {
        if self.offset == 0 && self.len == self.binding.selection.len() {
            self.binding.selection
        } else {
            self.binding.selection.slice(self.offset, self.len)
        }
    }
}

impl<'a, B> ColumnWriteSource<B::ColumnEncoder> for ArrowPhysicalSource<'_, 'a, B>
where
    B: ArrowPhysicalBridge<'a>,
{
    #[inline]
    fn len(self) -> usize {
        ArrowPhysicalSource::len(self)
    }

    #[inline]
    fn slice(self, offset: usize, len: usize) -> Self {
        ArrowPhysicalSource::slice(self, offset, len)
    }

    #[inline]
    fn write_to(self, encoder: &mut B::ColumnEncoder) -> Result<()> {
        self.binding.storage.write_values(encoder, self.selection())
    }

    #[inline]
    fn count_variable_width_within_byte_budget(
        self,
        encoder: &B::ColumnEncoder,
        budget: usize,
        target: ByteBudgetTarget,
    ) -> Option<usize> {
        self.binding
            .storage
            .count_variable_width_within_byte_budget(encoder, self.selection(), budget, target)
    }
}

fn write_arrow_physical<'a, B>(
    writer: &mut GenericColumnWriter<B::ColumnEncoder>,
    column: &'a dyn arrow_array::Array,
    levels: LeafBatch<'a>,
) -> Result<usize>
where
    B: ArrowPhysicalBridge<'a>,
{
    let binding = ArrowPhysicalBinding::<B>::bind(column, levels.value_selection())?;
    writer.write_batch_internal(
        binding.source(),
        levels.def_level_data(),
        levels.rep_level_data(),
        None,
        None,
        None,
    )
}

/// Dispatch over the eight `ColumnWriter` physical variants. Every supported
/// Arrow family uses the same physical source abstraction. `Int96` is
/// unreachable because `arrow_to_parquet_type` never emits it.
macro_rules! dispatch_leaf_writer {
    ($writer:expr, $phys:ident) => {
        match $writer {
            ColumnWriter::Int32ColumnWriter(typed) => $phys!(Int32Storage<'_>, typed),
            ColumnWriter::BoolColumnWriter(typed) => $phys!(BoolStorage<'_>, typed),
            ColumnWriter::Int64ColumnWriter(typed) => $phys!(Int64Storage<'_>, typed),
            ColumnWriter::Int96ColumnWriter(_typed) => {
                unreachable!("Arrow schema conversion does not produce INT96 columns")
            }
            ColumnWriter::FloatColumnWriter(typed) => $phys!(Float32Storage<'_>, typed),
            ColumnWriter::DoubleColumnWriter(typed) => $phys!(Float64Storage<'_>, typed),
            ColumnWriter::ByteArrayColumnWriter(typed) => $phys!(ByteArrayStorage<'_>, typed),
            ColumnWriter::FixedLenByteArrayColumnWriter(typed) => {
                $phys!(FixedLenByteArrayStorage<'_>, typed)
            }
        }
    };
}

fn dictionary_keys(keys: &dyn arrow_array::Array) -> DictionaryKeys<'_> {
    macro_rules! extract {
        ($ty:ty, $variant:ident) => {
            DictionaryKeys::$variant(keys.as_primitive::<$ty>().values().as_ref())
        };
    }
    match keys.data_type() {
        ArrowDataType::UInt8 => extract!(UInt8Type, U8),
        ArrowDataType::UInt16 => extract!(UInt16Type, U16),
        ArrowDataType::UInt32 => extract!(UInt32Type, U32),
        ArrowDataType::UInt64 => extract!(UInt64Type, U64),
        ArrowDataType::Int8 => extract!(Int8Type, I8),
        ArrowDataType::Int16 => extract!(Int16Type, I16),
        ArrowDataType::Int32 => extract!(Int32Type, I32),
        ArrowDataType::Int64 => extract!(Int64Type, I64),
        key_type => unreachable!("unsupported dictionary key type {key_type}"),
    }
}

fn dispatch_physical_input<'a>(
    column: &'a dyn arrow_array::Array,
    selection: ValueSelectionRef<'a>,
) -> (&'a dyn arrow_array::Array, PhysicalValueSelection<'a>) {
    if let Some(dictionary) = column.as_any_dictionary_opt() {
        let keys = dictionary_keys(dictionary.keys());
        (
            dictionary.values().as_ref(),
            PhysicalValueSelection::dictionary(selection, keys),
        )
    } else {
        (column, PhysicalValueSelection::identity(selection))
    }
}

fn primitive_values<T: ArrowPrimitiveType>(column: &dyn arrow_array::Array) -> &[T::Native] {
    column.as_primitive::<T>().values().as_ref()
}

// The arm-to-bridge mapping mirrors `arrow_to_parquet_type`.
fn write_leaf(writer: &mut ColumnWriter<'_>, levels: LeafBatch<'_>) -> Result<usize> {
    let column = levels.array();
    macro_rules! phys {
        ($bridge:ty, $typed:ident) => {
            write_arrow_physical::<$bridge>($typed, column, levels)
        };
    }
    dispatch_leaf_writer!(writer, phys)
}

/// Hash a byte slice to a u64 for NDV tracking.
#[inline]
fn hash_bytes(bytes: &[u8]) -> u64 {
    twox_hash::XxHash64::oneshot(0, bytes)
}

/// Returns the fixed byte width for primitive Arrow types, or `None` for variable-length types.
fn fixed_byte_width(dt: &ArrowDataType) -> Option<usize> {
    use ArrowDataType::*;
    match dt {
        Int8 | UInt8 => Some(1),
        Int16 | UInt16 | Float16 => Some(2),
        Int32 | UInt32 | Float32 | Date32 | Time32(_) | Decimal32(_, _) => Some(4),
        Int64
        | UInt64
        | Float64
        | Date64
        | Time64(_)
        | Timestamp(_, _)
        | Duration(_)
        | Decimal64(_, _) => Some(8),
        Interval(IntervalUnit::YearMonth) => Some(4),
        Interval(IntervalUnit::DayTime) => Some(8),
        Interval(IntervalUnit::MonthDayNano) => Some(16),
        Decimal128(_, _) => Some(16),
        Decimal256(_, _) => Some(32),
        _ => None,
    }
}

/// Hash selected non-null physical values without materializing dictionary keys
/// or a gathered index vector. Dictionary mappings are applied by the same
/// borrowed selection used by the native writer, so unused entries never count.
fn update_distinct_values_seen(
    array: &dyn arrow_array::Array,
    selection: PhysicalValueSelection<'_>,
    seen: &mut DistinctValuesSet,
) {
    fn collect(
        array: &dyn arrow_array::Array,
        selection: PhysicalValueSelection<'_>,
        seen: &mut DistinctValuesSet,
        mut hash_at: impl FnMut(usize) -> u64,
    ) {
        selection
            .try_for_each_index(|row| -> Result<(), std::convert::Infallible> {
                if array.is_valid(row) {
                    seen.insert(hash_at(row));
                }
                Ok(())
            })
            .unwrap();
    }

    let data = array.to_data();
    let offset = data.offset();
    match array.data_type() {
        ArrowDataType::Boolean => {
            let arr = array.as_boolean();
            collect(array, selection, seen, |row| arr.value(row) as u64);
        }
        ArrowDataType::Utf8 | ArrowDataType::Binary => {
            let offsets = data.buffers()[0].typed_data::<i32>();
            let values = data.buffers()[1].as_slice();
            collect(array, selection, seen, |row| {
                let start = offsets[offset + row] as usize;
                let end = offsets[offset + row + 1] as usize;
                hash_bytes(&values[start..end])
            });
        }
        ArrowDataType::LargeUtf8 | ArrowDataType::LargeBinary => {
            let offsets = data.buffers()[0].typed_data::<i64>();
            let values = data.buffers()[1].as_slice();
            collect(array, selection, seen, |row| {
                let start = offsets[offset + row] as usize;
                let end = offsets[offset + row + 1] as usize;
                hash_bytes(&values[start..end])
            });
        }
        ArrowDataType::FixedSizeBinary(width) => {
            let width = *width as usize;
            let buffer = data.buffers()[0].as_slice();
            collect(array, selection, seen, |row| {
                let start = (offset + row) * width;
                hash_bytes(&buffer[start..start + width])
            });
        }
        ArrowDataType::Utf8View => {
            let values = array.as_string_view();
            collect(array, selection, seen, |row| {
                hash_bytes(values.value(row).as_bytes())
            });
        }
        ArrowDataType::BinaryView => {
            let values = array.as_binary_view();
            collect(array, selection, seen, |row| hash_bytes(values.value(row)));
        }
        data_type => {
            if let Some(width) = fixed_byte_width(data_type) {
                let buffer = data.buffers()[0].as_slice();
                collect(array, selection, seen, |row| {
                    let start = (offset + row) * width;
                    hash_bytes(&buffer[start..start + width])
                });
            }
        }
    }
}

// Allow the helpers to use the same imports in unit and integration tests.
#[cfg(test)]
use crate as parquet_crate;

#[cfg(test)]
#[path = "../../../tests/arrow_writer/roundtrip_helpers.rs"]
mod roundtrip_helpers;

#[cfg(test)]
mod tests {
    use super::roundtrip_helpers::{
        RoundTripTest, SMALL_SIZE, required_and_optional, roundtrip, roundtrip_opts,
        roundtrip_opts_with_array_validation,
    };
    use super::*;
    use crate::file::properties::CdcOptions;
    use std::cmp::Ordering;
    use std::collections::HashMap;

    use std::fs::File;

    use crate::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
    use crate::arrow::{ARROW_SCHEMA_META_KEY, PARQUET_FIELD_ID_META_KEY};
    use crate::column::page::{Page, PageReader};
    use crate::file::metadata::thrift::PageHeader;
    use crate::file::page_index::column_index::ColumnIndexMetaData;
    use crate::file::reader::SerializedPageReader;
    use crate::parquet_thrift::{ReadThrift, ThriftSliceInputProtocol};
    use crate::schema::types::ColumnPath;
    use arrow::datatypes::{DataType, Schema};
    use arrow::error::Result as ArrowResult;
    use arrow::util::data_gen::create_random_array;
    use arrow::util::pretty::pretty_format_batches;
    use arrow::{array::*, buffer::Buffer};
    use arrow_buffer::{IntervalDayTime, IntervalMonthDayNano, NullBuffer, OffsetBuffer};
    use arrow_schema::Fields;
    use half::f16;
    use tempfile::tempfile;

    use crate::basic::{Encoding, EncodingMask};
    use crate::data_type::AsBytes;
    use crate::file::metadata::{ColumnChunkMetaData, ParquetMetaData, ParquetMetaDataReader};
    use crate::file::properties::{
        BloomFilterPosition, EnabledStatistics, ReaderProperties, WriterVersion,
    };
    use crate::file::serialized_reader::ReadOptionsBuilder;
    use crate::file::{
        reader::{FileReader, SerializedFileReader},
        statistics::Statistics,
    };

    /// A [`PageStore`] that allocates *sparse, non-contiguous* handles and keeps
    /// blobs in a `HashMap` — nothing like the default `Vec<Bytes>`. Used to
    /// prove the writer relies only on the opaque-handle contract and never on
    /// handles being dense `Vec` indices. Records how many blobs were stored.
    #[derive(Debug, Default)]
    struct RecordingPageStore {
        next: u64,
        blobs: HashMap<u64, Bytes>,
        puts: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl PageStore for RecordingPageStore {
        fn put(&mut self, value: Bytes) -> Result<PageKey> {
            // Deliberately non-sequential, never-zero handles.
            let id = 100 + self.next * 7;
            self.next += 1;
            self.puts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.blobs.insert(id, value);
            Ok(PageKey::new(id))
        }

        fn take(&mut self, key: PageKey) -> Result<Bytes> {
            self.blobs
                .remove(&key.get())
                .ok_or_else(|| ParquetError::General(format!("missing key {}", key.get())))
        }
    }

    #[derive(Debug)]
    struct RecordingPageStoreFactory {
        puts: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl PageStoreFactory for RecordingPageStoreFactory {
        fn create(&self, _args: &PageStoreArgs<'_>) -> Result<Box<dyn PageStore>> {
            Ok(Box::new(RecordingPageStore {
                puts: self.puts.clone(),
                ..Default::default()
            }))
        }
    }

    /// A custom [`PageStore`] must produce byte-identical files to the in-memory
    /// default, across dictionary and non-dictionary columns and multiple row
    /// groups (so multiple store instances are exercised).
    #[test]
    fn custom_page_store_is_byte_identical_to_default() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("i", DataType::Int32, true),
            // A low-cardinality string column to exercise the dictionary path.
            Field::new("s", DataType::Utf8, true),
        ]));
        let i = Int32Array::from(vec![Some(1), None, Some(3), Some(4), Some(5), Some(6)]);
        let s = StringArray::from(vec![
            Some("a"),
            Some("bb"),
            Some("a"),
            None,
            Some("bb"),
            Some("ccc"),
        ]);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(i), Arc::new(s)]).unwrap();

        // Small row groups so multiple column chunks (hence multiple store
        // instances) are produced.
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(3))
            .build();

        let write = |factory: Option<Arc<dyn PageStoreFactory>>| {
            let mut buffer = Vec::new();
            let mut opts = ArrowWriterOptions::new().with_properties(props.clone());
            if let Some(factory) = factory {
                opts = opts.with_page_store_factory(factory);
            }
            let mut writer =
                ArrowWriter::try_new_with_options(&mut buffer, schema.clone(), opts).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
            buffer
        };

        let default_bytes = write(None);

        let puts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let custom_bytes = write(Some(Arc::new(RecordingPageStoreFactory {
            puts: puts.clone(),
        })));

        assert!(
            puts.load(std::sync::atomic::Ordering::Relaxed) > 0,
            "custom PageStore was never written to"
        );
        assert_eq!(
            default_bytes, custom_bytes,
            "a custom PageStore must produce byte-identical output to the default"
        );
    }

    /// A dictionary-encoded column written through the deferred-ordering Arrow
    /// path must round-trip correctly even with the offset index disabled, when
    /// only the chunk-level dictionary/data page offsets are rewritten (there is
    /// no offset index to rebuild). Spans multiple data pages so the
    /// dictionary-first reordering is exercised.
    #[test]
    #[cfg_attr(miri, ignore)] // Takes too long
    fn dictionary_column_round_trips_with_offset_index_disabled() {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, true)]));

        // Low cardinality so the column stays dictionary-encoded; enough rows to
        // span several data pages within a single row group.
        let values: Vec<Option<i32>> = (0..50_000).map(|i| Some(i % 8)).collect();
        let array = Int32Array::from(values.clone());
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap();

        let props = WriterProperties::builder()
            .set_offset_index_disabled(true)
            .set_data_page_row_count_limit(4096)
            .build();
        let opts = ArrowWriterOptions::new().with_properties(props);

        let mut buffer = Vec::new();
        let mut writer =
            ArrowWriter::try_new_with_options(&mut buffer, schema.clone(), opts).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let reader = ParquetRecordBatchReader::try_new(Bytes::from(buffer), values.len()).unwrap();
        let read: Vec<RecordBatch> = reader.collect::<ArrowResult<_>>().unwrap();
        let read_values: Vec<Option<i32>> = read
            .iter()
            .flat_map(|b| b.column(0).as_primitive::<Int32Type>().iter())
            .collect();
        assert_eq!(read_values, values);
    }

    /// The dictionary page is routed through the [`PageStore`] like any other
    /// page rather than held resident in memory, so a dictionary column chunk's
    /// *entire* serialized size — dictionary page included — passes through the
    /// store.
    #[test]
    fn dictionary_page_is_routed_through_the_store() {
        /// A store that sums the bytes handed to `put`.
        #[derive(Debug, Default)]
        struct SizeRecordingPageStore {
            blobs: Vec<Bytes>,
            bytes_put: Arc<std::sync::atomic::AtomicUsize>,
        }
        impl PageStore for SizeRecordingPageStore {
            fn put(&mut self, value: Bytes) -> Result<PageKey> {
                self.bytes_put
                    .fetch_add(value.len(), std::sync::atomic::Ordering::Relaxed);
                let key = PageKey::new(self.blobs.len() as u64);
                self.blobs.push(value);
                Ok(key)
            }
            fn take(&mut self, key: PageKey) -> Result<Bytes> {
                Ok(std::mem::take(&mut self.blobs[key.get() as usize]))
            }
        }
        #[derive(Debug)]
        struct Factory {
            bytes_put: Arc<std::sync::atomic::AtomicUsize>,
        }
        impl PageStoreFactory for Factory {
            fn create(&self, _args: &PageStoreArgs<'_>) -> Result<Box<dyn PageStore>> {
                Ok(Box::new(SizeRecordingPageStore {
                    bytes_put: self.bytes_put.clone(),
                    ..Default::default()
                }))
            }
        }

        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)]));
        // Low cardinality keeps the column dictionary-encoded with a real,
        // non-empty dictionary page.
        let values: Vec<&str> = (0..2048)
            .map(|i| ["alpha", "beta", "gamma", "delta"][i % 4])
            .collect();
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(values))])
            .unwrap();

        let bytes_put = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let opts = ArrowWriterOptions::new().with_page_store_factory(Arc::new(Factory {
            bytes_put: bytes_put.clone(),
        }));

        // A single batch / single column means exactly one row group and one
        // store instance, so the bytes it saw map to one column chunk.
        let mut buffer = Vec::new();
        let mut writer =
            ArrowWriter::try_new_with_options(&mut buffer, schema.clone(), opts).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let reader = SerializedFileReader::new(Bytes::from(buffer)).unwrap();
        let column = reader.metadata().row_group(0).column(0);
        assert!(
            column.dictionary_page_offset().is_some(),
            "expected the column to be dictionary-encoded"
        );

        // The bytes the store was handed must account for the whole chunk,
        // dictionary page included. Holding the dictionary page apart from the
        // store would make this fall short by the dictionary page's size.
        assert_eq!(
            bytes_put.load(std::sync::atomic::Ordering::Relaxed) as i64,
            column.compressed_size(),
            "the dictionary page must pass through the store like any other page"
        );
    }

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
    #[cfg_attr(miri, ignore)] // Takes too long
    fn arrow_writer_non_null() {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let a = Int32Array::from(vec![1, 2, 3, 4, 5]);

        RoundTripTest::new(Arc::new(a))
            .with_schema(Arc::new(schema))
            .run();
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Takes too long
    fn arrow_writer_binary() {
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
        assert_eq!(string_values.null_count(), 0);
        assert_eq!(binary_values.null_count(), 0);

        RoundTripTest::new(Arc::new(string_values.clone())).run();
        RoundTripTest::new(Arc::new(binary_values.clone())).run();

        let string_field = Field::new("a", DataType::Utf8, false);
        let binary_field = Field::new("b", DataType::Binary, false);
        let schema = Schema::new(vec![string_field, binary_field]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(string_values), Arc::new(binary_values)],
        )
        .unwrap();

        roundtrip(batch, Some(SMALL_SIZE / 2));
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Takes too long
    fn arrow_writer_binary_view() {
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

        RoundTripTest::new(Arc::new(string_view_values.clone())).run();
        RoundTripTest::new(Arc::new(binary_view_values.clone())).run();
        RoundTripTest::new(Arc::new(nullable_string_view_values.clone())).run();

        let string_field = Field::new("a", DataType::Utf8View, false);
        let binary_field = Field::new("b", DataType::BinaryView, false);
        let nullable_string_field = Field::new("a", DataType::Utf8View, true);
        let schema = Schema::new(vec![string_field, binary_field, nullable_string_field]);

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

    #[test]
    #[cfg_attr(miri, ignore)] // Takes too long
    fn arrow_writer_binary_view_long_value() {
        let string_field = Field::new("a", DataType::Utf8View, false);
        let binary_field = Field::new("b", DataType::BinaryView, false);
        let schema = Schema::new(vec![string_field, binary_field]);

        // There is special case validation for long values (greater than 128)
        // 128 encodes as 0x80 0x00 0x00 0x00 in little endian, which should
        // trigger the long-string UTF-8 validation branch in the plain decoder.
        let long = "a".repeat(128);
        let raw_string_values = vec!["foo", long.as_str(), "bar"];
        let raw_binary_values = vec![b"foo".to_vec(), long.as_bytes().to_vec(), b"bar".to_vec()];

        let string_view_values: ArrayRef = Arc::new(StringViewArray::from(raw_string_values));
        let binary_view_values: ArrayRef =
            Arc::new(BinaryViewArray::from_iter_values(raw_binary_values));

        RoundTripTest::new(Arc::clone(&string_view_values))
            .with_nullable(false)
            .run();
        RoundTripTest::new(Arc::clone(&binary_view_values))
            .with_nullable(false)
            .run();

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![string_view_values, binary_view_values],
        )
        .unwrap();

        // Disable dictionary to exercise plain encoding paths in the reader.
        for version in [WriterVersion::PARQUET_1_0, WriterVersion::PARQUET_2_0] {
            let props = WriterProperties::builder()
                .set_writer_version(version)
                .set_dictionary_enabled(false)
                .build();
            roundtrip_opts(&batch, props);
        }
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

    fn read_column(file: Vec<u8>) -> ArrayRef {
        let reader = ParquetRecordBatchReader::try_new(Bytes::from(file), 4096).unwrap();
        let batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
        let arrays: Vec<&dyn Array> = batches.iter().map(|b| b.column(0).as_ref()).collect();
        arrow_select::concat::concat(&arrays).unwrap()
    }

    fn roundtrip_compatible_column(field: Field, col: ArrayRef) -> ArrayRef {
        let writer_schema = Arc::new(Schema::new(vec![field]));
        let batch_schema = Arc::new(Schema::new(vec![Field::new(
            "c",
            col.data_type().clone(),
            col.logical_null_count() != 0,
        )]));
        let batch = RecordBatch::try_new(batch_schema, vec![col]).unwrap();
        let options = ArrowWriterOptions::new().with_skip_arrow_metadata(true);
        let mut file = vec![];
        let mut writer =
            ArrowWriter::try_new_with_options(&mut file, writer_schema, options).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        read_column(file)
    }

    #[test]
    fn arrow_writer_dense_batches_under_nested_wrapper_schemas() {
        let run_ends = || Arc::new(Field::new("run_ends", DataType::Int32, false));
        let ree = |value: DataType, nullable| {
            DataType::RunEndEncoded(run_ends(), Arc::new(Field::new("values", value, nullable)))
        };
        let assert_unified = |field: Field, actual: ArrayRef, expected: &ArrayRef| {
            assert!(!compute_leaves(&field, &actual).unwrap().is_empty());
            assert_eq!(
                roundtrip_compatible_column(field, actual).as_ref(),
                expected.as_ref()
            );
        };

        let item = Arc::new(Field::new_list_field(DataType::Int32, true));
        let list: ArrayRef = Arc::new(ListArray::new(
            item.clone(),
            OffsetBuffer::new(vec![0_i32, 2, 3].into()),
            Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])),
            None,
        ));

        // A dense list under an REE<List> schema uses range traversal; the
        // schema wrapper is physical, not a second logical list node.
        let ree_list_field = Field::new("c", ree(list.data_type().clone(), false), false);
        assert_unified(ree_list_field, list.clone(), &list);

        // Dictionary<List> has the same dense logical shape and follows the
        // same list traversal after schema normalization.
        let dictionary_list = Field::new(
            "c",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(list.data_type().clone())),
            false,
        );
        assert_unified(dictionary_list, list.clone(), &list);

        let dense: ArrayRef = Arc::new(Int32Array::from(vec![Some(7), None, Some(9)]));
        let nested_ree = Field::new("c", ree(ree(DataType::Int32, true), false), false);
        assert_unified(nested_ree, dense.clone(), &dense);

        let dictionary_ree = Field::new(
            "c",
            DataType::Dictionary(
                Box::new(DataType::Int8),
                Box::new(ree(DataType::Int32, true)),
            ),
            false,
        );
        assert_unified(dictionary_ree, dense.clone(), &dense);

        // The inverse wrapper order must hoist REE value nullability before
        // peeling the dictionary; the dense batch contains an actual null.
        let ree_dictionary = Field::new(
            "c",
            ree(
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Int32)),
                true,
            ),
            false,
        );
        assert_unified(ree_dictionary, dense.clone(), &dense);

        // Wrapper normalization is per logical node: a wrapper nested under a
        // struct child must not make the enclosing struct incompatible.
        let struct_fields = Fields::from(vec![Field::new("value", DataType::Int32, true)]);
        let dense_struct: ArrayRef =
            Arc::new(StructArray::new(struct_fields, vec![dense.clone()], None));
        let wrapped_struct = Field::new(
            "c",
            DataType::Struct(Fields::from(vec![Field::new(
                "value",
                ree(DataType::Int32, true),
                false,
            )])),
            false,
        );
        assert_unified(wrapped_struct, dense_struct.clone(), &dense_struct);

        // The same recursive compatibility is required for list children.
        let wrapped_list = Field::new(
            "c",
            DataType::List(Arc::new(Field::new(
                "item",
                ree(DataType::Int32, true),
                false,
            ))),
            false,
        );
        assert_unified(wrapped_list, list.clone(), &list);

        // Nested scalar dictionaries may alternate with their dense logical
        // value in either direction between writer schema and batch.
        let dict: ArrayRef = Arc::new(DictionaryArray::new(
            Int8Array::from(vec![Some(0), None, Some(1)]),
            Arc::new(Int32Array::from(vec![1, 3])),
        ));
        let dict_list: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new_list_field(dict.data_type().clone(), true)),
            OffsetBuffer::new(vec![0_i32, 2, 3].into()),
            dict,
            None,
        ));
        let list_of_dictionary = Field::new(
            "c",
            DataType::List(Arc::new(Field::new_list_field(
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Int32)),
                true,
            ))),
            false,
        );
        assert_unified(list_of_dictionary, list.clone(), &list);
        assert_unified(
            Field::new("c", list.data_type().clone(), false),
            dict_list,
            &list,
        );

        // A wrapper below a Map value is validated at the value node, after
        // walking through the repeated entries struct.
        let key_field = Arc::new(Field::new("keys", DataType::Utf8, false));
        let entries = StructArray::new(
            Fields::from(vec![
                key_field.clone(),
                Arc::new(Field::new("values", DataType::Int32, true)),
            ]),
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef,
                dense.clone(),
            ],
            None,
        );
        let map: ArrayRef = Arc::new(MapArray::new(
            Arc::new(Field::new("entries", entries.data_type().clone(), false)),
            OffsetBuffer::new(vec![0_i32, 2, 3].into()),
            entries,
            None,
            false,
        ));
        let wrapped_map = Field::new(
            "c",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![
                        key_field,
                        Arc::new(Field::new("values", ree(DataType::Int32, true), false)),
                    ])),
                    false,
                )),
                false,
            ),
            false,
        );
        assert_unified(wrapped_map, map.clone(), &map);
    }

    #[test]
    fn arrow_writer_rejects_missing_or_extra_top_level_columns_cleanly() {
        let one_field = Arc::new(Schema::new(vec![Field::new(
            "a",
            ArrowDataType::Int32,
            false,
        )]));
        let two_fields = Arc::new(Schema::new(vec![
            Field::new("a", ArrowDataType::Int32, false),
            Field::new("b", ArrowDataType::Int32, false),
        ]));
        let one_column = RecordBatch::try_new(
            Arc::clone(&one_field),
            vec![Arc::new(Int32Array::from(vec![1]))],
        )
        .unwrap();
        let two_columns = RecordBatch::try_new(
            Arc::clone(&two_fields),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(Int32Array::from(vec![2])),
            ],
        )
        .unwrap();

        let mut writer = ArrowWriter::try_new(Vec::new(), Arc::clone(&two_fields), None).unwrap();
        let err = writer.write(&one_column).unwrap_err();
        assert!(
            err.to_string()
                .contains("writer has 2 top-level fields but batch has 1 columns"),
            "{err}"
        );
        assert_eq!(writer.in_progress_rows(), 0);

        let mut writer = ArrowWriter::try_new(Vec::new(), Arc::clone(&one_field), None).unwrap();
        let err = writer.write(&two_columns).unwrap_err();
        assert!(
            err.to_string()
                .contains("writer has 1 top-level fields but batch has 2 columns"),
            "{err}"
        );
        assert_eq!(writer.in_progress_rows(), 0);
    }

    #[test]
    fn arrow_row_group_factory_rejects_parquet_leaf_count_mismatch_cleanly() {
        let one_field = Arc::new(Schema::new(vec![Field::new(
            "a",
            ArrowDataType::Int32,
            false,
        )]));
        let two_fields = Arc::new(Schema::new(vec![
            Field::new("a", ArrowDataType::Int32, false),
            Field::new("b", ArrowDataType::Int32, false),
        ]));
        let parquet = ArrowSchemaConverter::new().convert(&one_field).unwrap();
        let props = Arc::new(WriterProperties::default());
        let file_writer =
            SerializedFileWriter::new(Vec::new(), parquet.root_schema_ptr(), Arc::clone(&props))
                .unwrap();

        let factory = ArrowRowGroupWriterFactory::new(&file_writer, Arc::clone(&two_fields));
        let Err(err) = factory.create_column_writers(0) else {
            panic!("mismatched schemas should not create column writers")
        };
        assert!(
            err.to_string()
                .contains("Arrow schema maps to 2 leaf columns but Parquet schema has 1"),
            "{err}"
        );

        let parquet = ArrowSchemaConverter::new().convert(&two_fields).unwrap();
        let file_writer =
            SerializedFileWriter::new(Vec::new(), parquet.root_schema_ptr(), props).unwrap();
        let factory = ArrowRowGroupWriterFactory::new(&file_writer, one_field);
        let Err(err) = factory.create_column_writers(0) else {
            panic!("mismatched schemas should not create column writers")
        };
        assert!(
            err.to_string()
                .contains("Arrow schema maps to 1 leaf columns but Parquet schema has 2"),
            "{err}"
        );
    }

    #[test]
    fn arrow_writer_required_map_keys_reject_only_reachable_nulls() {
        let keys: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::new(
            Int32Array::from(vec![1, 0, 1]),
            Arc::new(Int32Array::from(vec![None, Some(7)])),
        ));
        let values = Int32Array::from(vec![Some(10), Some(20), None]);
        let fields = Fields::from(vec![
            Field::new("key", keys.data_type().clone(), false),
            Field::new("value", DataType::Int32, true),
        ]);
        assert!(
            StructArray::try_new(
                fields.clone(),
                vec![keys.clone(), Arc::new(values.clone())],
                None
            )
            .is_err()
        );

        // ArrayData validates physical nulls, not null dictionary values. This
        // safe construction reaches the writer with a logically null map key.
        let data = ArrayData::builder(DataType::Struct(fields))
            .len(3)
            .add_child_data(keys.to_data())
            .add_child_data(values.to_data())
            .build()
            .unwrap();
        data.validate_full().unwrap();
        let entries = StructArray::from(data);
        let entries_field = Arc::new(Field::new("entries", entries.data_type().clone(), false));
        let offsets = OffsetBuffer::new(vec![0i32, 1, 2, 3].into());
        let map = MapArray::try_new(
            entries_field.clone(),
            offsets.clone(),
            entries.clone(),
            None,
            false,
        )
        .unwrap();
        assert_eq!(map.keys().logical_null_count(), 1);
        let masked = MapArray::try_new(
            entries_field,
            offsets,
            entries,
            Some(NullBuffer::from(vec![true, false, true])),
            false,
        )
        .unwrap();
        let writer_schema = Arc::new(Schema::new(vec![Field::new(
            "map",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Int32, false),
                            Field::new("value", DataType::Int32, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            true,
        )]));
        for cdc in [false, true] {
            let props = WriterProperties::builder()
                .set_content_defined_chunking(cdc.then(Default::default))
                .build();
            for (array, expected_keys, expected_null_maps) in [
                (map.clone(), None, 0),
                (map.slice(0, 1), Some(1), 0),
                (map.slice(2, 1), Some(1), 0),
                (masked.clone(), Some(2), 1),
            ] {
                let schema = Arc::new(Schema::new(vec![Field::new(
                    "map",
                    array.data_type().clone(),
                    true,
                )]));
                let batch = RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap();
                let mut bytes = Vec::new();
                let mut writer =
                    ArrowWriter::try_new(&mut bytes, writer_schema.clone(), Some(props.clone()))
                        .unwrap();
                let result = writer.write(&batch);
                let Some(expected_keys) = expected_keys else {
                    let err = result.unwrap_err();
                    assert!(
                        err.to_string().contains("Found null")
                            && err.to_string().contains("required field 'key'"),
                        "{err}"
                    );
                    continue;
                };
                result.unwrap();
                writer.close().unwrap();
                let actual = ParquetRecordBatchReader::try_new(Bytes::from(bytes), 1024)
                    .unwrap()
                    .next()
                    .unwrap()
                    .unwrap();
                let actual = actual.column(0).as_map();
                assert_eq!(actual.null_count(), expected_null_maps);
                assert_eq!(
                    actual.keys().as_primitive::<Int32Type>().values().as_ref(),
                    vec![7; expected_keys]
                );
            }
        }
    }

    #[test]
    fn arrow_writer_rejects_nullable_map_key_schemas_early() {
        let schema_with_key = |key| {
            Arc::new(Schema::new(vec![Field::new(
                "map",
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(
                            vec![key, Field::new("value", DataType::Int32, true)].into(),
                        ),
                        false,
                    )),
                    false,
                ),
                true,
            )]))
        };
        let required = schema_with_key(Field::new("key", DataType::Int32, false));
        let parquet = ArrowSchemaConverter::new().convert(&required).unwrap();
        let ree = DataType::RunEndEncoded(
            Arc::new(Field::new("run_ends", DataType::Int32, false)),
            Arc::new(Field::new("values", DataType::Int32, true)),
        );
        for key in [
            Field::new("key", DataType::Int32, true),
            Field::new("key", ree.clone(), false),
            Field::new(
                "key",
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(ree)),
                false,
            ),
        ] {
            let schema = schema_with_key(key);
            for explicit_schema in [false, true] {
                for skip_metadata in [false, true] {
                    let mut options =
                        ArrowWriterOptions::new().with_skip_arrow_metadata(skip_metadata);
                    if explicit_schema {
                        options = options.with_parquet_schema(parquet.clone());
                    }
                    let mut bytes = Vec::new();
                    let err =
                        ArrowWriter::try_new_with_options(&mut bytes, schema.clone(), options)
                            .unwrap_err();
                    assert!(
                        err.to_string()
                            .contains("Map key field 'key' must be non-nullable"),
                        "{err}"
                    );
                    assert!(
                        bytes.is_empty(),
                        "schema rejection must precede file initialization"
                    );
                }
            }
            let file = SerializedFileWriter::new(
                Vec::new(),
                parquet.root_schema_ptr(),
                Arc::new(WriterProperties::default()),
            )
            .unwrap();
            let factory = ArrowRowGroupWriterFactory::new(&file, schema);
            let Err(err) = factory.create_column_writers(0) else {
                panic!("nullable map-key schema should not create column writers")
            };
            assert!(
                err.to_string()
                    .contains("Map key field 'key' must be non-nullable"),
                "{err}"
            );
        }
    }

    #[test]
    fn ree_required_schema_writes_dense_int32_batch() {
        let run_ends = Int32Array::from(vec![1]);
        let values = Int32Array::from(vec![7]);
        let ree = Int32RunArray::try_new(&run_ends, &values).unwrap();
        let writer_schema = Arc::new(Schema::new(vec![Field::new(
            "c",
            ree.data_type().clone(),
            false,
        )]));

        let dense_schema = Arc::new(Schema::new(vec![Field::new("c", DataType::Int32, false)]));
        let dense_batch =
            RecordBatch::try_new(dense_schema, vec![Arc::new(Int32Array::from(vec![3, 4]))])
                .unwrap();

        let mut file = vec![];
        let mut writer = ArrowWriter::try_new(&mut file, writer_schema, None).unwrap();
        writer.write(&dense_batch).unwrap();
        writer.close().unwrap();

        let mut reader = ParquetRecordBatchReader::try_new(Bytes::from(file), 1024).unwrap();
        let actual = reader.next().unwrap().unwrap();
        assert_eq!(
            actual.column(0).as_primitive::<Int32Type>().values(),
            &[3, 4]
        );
    }

    #[test]
    fn ree_nulls_respect_dense_schema_nullability() {
        let run_ends = Int32Array::from(vec![2, 4, 5]);
        let values = Int32Array::from(vec![Some(1), None, Some(2)]);
        let ree: ArrayRef = Arc::new(Int32RunArray::try_new(&run_ends, &values).unwrap());
        let ree_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "c",
                ree.data_type().clone(),
                true,
            )])),
            vec![ree.clone()],
        )
        .unwrap();

        let nullable_schema = Arc::new(Schema::new(vec![Field::new("c", DataType::Int32, true)]));
        let mut file = vec![];
        let mut writer = ArrowWriter::try_new(&mut file, nullable_schema, None).unwrap();
        writer.write(&ree_batch).unwrap();
        writer.close().unwrap();

        let mut reader = ParquetRecordBatchReader::try_new(Bytes::from(file), 1024).unwrap();
        let actual = reader.next().unwrap().unwrap();
        assert_eq!(
            actual.column(0).as_primitive::<Int32Type>(),
            &Int32Array::from(vec![Some(1), Some(1), None, None, Some(2)])
        );

        let required_schema = Arc::new(Schema::new(vec![Field::new("c", DataType::Int32, false)]));
        let mut writer = ArrowWriter::try_new(Vec::new(), required_schema, None).unwrap();
        let err = writer.write(&ree_batch).unwrap_err();
        assert!(
            err.to_string().contains("required field") && err.to_string().contains("Found null"),
            "{err}"
        );
    }

    #[test]
    fn eager_required_null_validation_respects_reachability_and_write_timing() {
        let required = Field::new("c", DataType::Int32, false);
        let nullable: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None]));
        let leaves = compute_leaves(&required, &nullable).unwrap();
        let schema = Arc::new(Schema::new(vec![required.clone()]));
        let writer = ArrowWriter::try_new(Vec::new(), schema, None).unwrap();
        let (_file, factory) = writer.into_serialized_writer().unwrap();
        let mut columns = factory.create_column_writers(0).unwrap();
        assert!(
            columns[0]
                .write(&leaves[0])
                .unwrap_err()
                .to_string()
                .contains("required field 'c'")
        );

        // Only a referenced logical dictionary null is invalid, not an unused value.
        let values: ArrayRef = Arc::new(Int32Array::from(vec![Some(7), None]));
        let dict: ArrayRef = Arc::new(DictionaryArray::new(
            Int8Array::from(vec![0, 0]),
            values.clone(),
        ));
        assert_eq!(
            roundtrip_compatible_column(required.clone(), dict).as_primitive::<Int32Type>(),
            &Int32Array::from(vec![7, 7])
        );
        let dict: ArrayRef = Arc::new(DictionaryArray::new(Int8Array::from(vec![0, 1]), values));
        let leaves = compute_leaves(&required, &dict).unwrap();
        assert!(leaves[0].0.validate().is_err());

        // A nullable ancestor masks an otherwise required child null.
        let actual_fields = Fields::from(vec![Field::new("source_child", DataType::Int32, true)]);
        let array: ArrayRef = Arc::new(StructArray::new(
            actual_fields,
            vec![nullable.clone()],
            Some(NullBuffer::from(vec![true, false])),
        ));
        let target = Field::new(
            "c",
            DataType::Struct(Fields::from(vec![Field::new(
                "target_child",
                DataType::Int32,
                false,
            )])),
            true,
        );
        let leaves = compute_leaves(&target, &array).unwrap();
        assert!(leaves[0].0.validate().is_ok());
        let result = roundtrip_compatible_column(target, array);
        assert_eq!(result.as_struct().fields()[0].name(), "target_child");
        assert!(result.is_null(1));

        // Unused list child slots and out-of-slice scalar nulls are unreachable.
        let values: ArrayRef = Arc::new(Int32Array::from(vec![None, Some(9), None]));
        let list: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new_list_field(DataType::Int32, true)),
            OffsetBuffer::new(vec![1_i32, 2].into()),
            values,
            None,
        ));
        let target = Field::new(
            "c",
            DataType::List(Arc::new(Field::new_list_field(DataType::Int32, false))),
            false,
        );
        assert!(
            compute_leaves(&target, &list).unwrap()[0]
                .0
                .validate()
                .is_ok()
        );
        assert_eq!(
            roundtrip_compatible_column(required, nullable.slice(0, 1)).as_primitive::<Int32Type>(),
            &Int32Array::from(vec![1])
        );
    }

    // Temporary eager-path coverage; P20-P25 replace this with target cursor tests.
    fn check_eager_required_list_validation(list_type: fn(Arc<Field>) -> DataType) {
        let make_array = |values: Vec<Option<i32>>,
                          offsets: Vec<i32>,
                          sizes: Vec<i32>,
                          nulls: Option<NullBuffer>| {
            let child = Arc::new(Field::new("source_child", DataType::Int32, true));
            let values: ArrayRef = Arc::new(Int32Array::from(values));
            match list_type(child.clone()) {
                DataType::List(_) => Arc::new(ListArray::new(
                    child,
                    OffsetBuffer::new(offsets.into()),
                    values,
                    nulls,
                )) as ArrayRef,
                DataType::LargeList(_) => Arc::new(LargeListArray::new(
                    child,
                    OffsetBuffer::new(
                        offsets
                            .into_iter()
                            .map(i64::from)
                            .collect::<Vec<_>>()
                            .into(),
                    ),
                    values,
                    nulls,
                )) as ArrayRef,
                DataType::ListView(_) => Arc::new(ListViewArray::new(
                    child,
                    offsets.into(),
                    sizes.into(),
                    values,
                    nulls,
                )) as ArrayRef,
                DataType::LargeListView(_) => Arc::new(LargeListViewArray::new(
                    child,
                    offsets
                        .into_iter()
                        .map(i64::from)
                        .collect::<Vec<_>>()
                        .into(),
                    sizes.into_iter().map(i64::from).collect::<Vec<_>>().into(),
                    values,
                    nulls,
                )) as ArrayRef,
                _ => unreachable!(),
            }
        };
        let is_view = matches!(
            list_type(Arc::new(Field::new("item", DataType::Int32, true))),
            DataType::ListView(_) | DataType::LargeListView(_)
        );
        let offsets = |starts: Vec<i32>, end: i32| {
            let mut result = starts;
            if !is_view {
                result.push(end);
            }
            result
        };
        let target = Field::new(
            "c",
            list_type(Arc::new(Field::new("target_child", DataType::Int32, false))),
            true,
        );
        let invalid = make_array(vec![None, Some(7)], offsets(vec![0], 2), vec![2], None);
        let valid = make_array(vec![Some(7), Some(8)], offsets(vec![0], 2), vec![2], None);
        let masked = make_array(
            vec![None, Some(7)],
            offsets(vec![0, 1], 2),
            vec![1, 1],
            Some(NullBuffer::from(vec![false, true])),
        );
        let unused = make_array(
            vec![None, Some(7), None],
            offsets(vec![1], 2),
            vec![1],
            None,
        );
        let empty = make_array(vec![None], offsets(vec![0], 0), vec![0], None);
        let sliced = make_array(
            vec![None, Some(7)],
            offsets(vec![0, 1], 2),
            vec![1, 1],
            None,
        )
        .slice(1, 1);
        let schema = Arc::new(Schema::new(vec![target.clone()]));

        // Binding must succeed even for invalid input; only an individual write rejects it.
        for (name, array, invalid) in [
            ("reachable null", invalid, true),
            ("valid required child", valid.clone(), false),
            ("null ancestor", masked, false),
            ("unused child slots", unused, false),
            ("empty list", empty, false),
            ("out-of-slice null", sliced, false),
        ] {
            let leaves = compute_leaves(&target, &array).unwrap();
            assert_eq!(leaves.len(), 1);
            for cdc in [false, true] {
                let props = WriterProperties::builder()
                    .set_content_defined_chunking(cdc.then(Default::default))
                    .build();
                let mut writer =
                    ArrowWriter::try_new(Vec::new(), schema.clone(), Some(props)).unwrap();
                let mut chunkers = writer.cdc_chunkers.take();
                let (_file, factory) = writer.into_serialized_writer().unwrap();
                let mut columns = factory.create_column_writers(0).unwrap();
                let before = (
                    columns[0].memory_size(),
                    columns[0].get_estimated_total_bytes(),
                );
                let result = match &mut chunkers {
                    Some(chunkers) => columns[0].write_with_chunker(&leaves[0], &mut chunkers[0]),
                    None => columns[0].write(&leaves[0]),
                };
                if invalid {
                    let error = result.unwrap_err().to_string();
                    assert!(
                        error.contains("Found null at index 0 for required field 'target_child'"),
                        "{name}, cdc={cdc}: {error}"
                    );
                    assert_eq!(
                        before,
                        (
                            columns[0].memory_size(),
                            columns[0].get_estimated_total_bytes()
                        ),
                        "invalid leaf must be rejected before encoding"
                    );
                    // The rejected write must leave no encoded values behind.
                    let valid_leaves = compute_leaves(&target, &valid).unwrap();
                    match &mut chunkers {
                        Some(chunkers) => columns[0]
                            .write_with_chunker(&valid_leaves[0], &mut chunkers[0])
                            .unwrap(),
                        None => columns[0].write(&valid_leaves[0]).unwrap(),
                    }
                    assert_eq!(
                        columns
                            .pop()
                            .unwrap()
                            .close()
                            .unwrap()
                            .close
                            .metadata
                            .num_values(),
                        2
                    );
                } else {
                    result.unwrap();
                    columns.pop().unwrap().close().unwrap();
                }

                // Exercise the ordinary and CDC row-group driving entry points as well.
                let batch = RecordBatch::try_new(
                    Arc::new(Schema::new(vec![Field::new(
                        "c",
                        array.data_type().clone(),
                        true,
                    )])),
                    vec![array.clone()],
                )
                .unwrap();
                let props = WriterProperties::builder()
                    .set_content_defined_chunking(cdc.then(Default::default))
                    .build();
                let mut writer =
                    ArrowWriter::try_new(Vec::new(), schema.clone(), Some(props)).unwrap();
                let result = writer.write(&batch);
                if invalid {
                    assert!(
                        result
                            .unwrap_err()
                            .to_string()
                            .contains("required field 'target_child'")
                    );
                } else {
                    result.unwrap();
                    writer.close().unwrap();
                }
            }
        }
    }

    #[test]
    fn eager_required_list_validation_preserves_level_generation() {
        check_eager_required_list_validation(DataType::List);
    }

    #[test]
    fn eager_required_large_list_validation_preserves_level_generation() {
        check_eager_required_list_validation(DataType::LargeList);
    }

    #[test]
    fn eager_required_list_view_validation_preserves_level_generation() {
        check_eager_required_list_validation(DataType::ListView);
    }

    #[test]
    fn eager_required_large_list_view_validation_preserves_level_generation() {
        check_eager_required_list_validation(DataType::LargeListView);
    }

    #[test]
    fn eager_row_group_validation_rejects_writer_and_chunker_count_mismatches() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1]))])
            .unwrap();
        let writer = ArrowWriter::try_new(Vec::new(), schema, None).unwrap();
        let (_file, factory) = writer.into_serialized_writer().unwrap();
        let mut group = factory.create_row_group_writer(0).unwrap();
        assert!(
            group
                .write_with_chunkers(&batch, &mut [])
                .unwrap_err()
                .to_string()
                .contains("chunkers")
        );
        assert_eq!(group.buffered_rows, 0);
        group.writers.clear();
        assert!(
            group
                .write(&batch)
                .unwrap_err()
                .to_string()
                .contains("column writers")
        );
        assert_eq!(group.buffered_rows, 0);
    }

    #[test]
    fn arrow_write_schema_plan_caches_nested_leaf_ranges() {
        let nested = ArrowDataType::Struct(Fields::from(vec![
            Field::new("i", ArrowDataType::Int32, false),
            Field::new("s", ArrowDataType::Utf8, true),
        ]));
        let list = ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::Boolean, true)));
        let ree = ArrowDataType::RunEndEncoded(
            Arc::new(Field::new("run_ends", ArrowDataType::Int32, false)),
            Arc::new(Field::new("values", ArrowDataType::Int64, false)),
        );
        let dictionary_fsb = ArrowDataType::Dictionary(
            Box::new(ArrowDataType::Int8),
            Box::new(ArrowDataType::FixedSizeBinary(2)),
        );
        let schema = Arc::new(Schema::new(vec![
            Field::new("nested", nested, true),
            Field::new("list", list, true),
            Field::new("ree", ree, false),
            Field::new("dictionary_fsb", dictionary_fsb, true),
        ]));
        let parquet = ArrowSchemaConverter::new().convert(&schema).unwrap();

        let plan = ArrowWriteSchemaPlan::try_new(&parquet, &schema).unwrap();
        let ranges = plan
            .fields
            .iter()
            .map(|field| field.leaf_range.clone())
            .collect::<Vec<_>>();
        assert_eq!(ranges, vec![0..2, 2..3, 3..4, 4..5]);
        assert_eq!(plan.leaves.len(), parquet.num_columns());
        assert_eq!(
            plan.leaves[4].physical_type(),
            crate::basic::Type::FIXED_LEN_BYTE_ARRAY
        );
    }

    #[test]
    fn cached_schema_plan_preserves_compatible_physical_layout_alternation() {
        let writer_schema = Arc::new(Schema::new(vec![
            Field::new("number", ArrowDataType::Int32, false),
            Field::new("text", ArrowDataType::Utf8, false),
            Field::new("bytes", ArrowDataType::Binary, false),
        ]));

        let dense = RecordBatch::try_new(
            Arc::clone(&writer_schema),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["a"])),
                Arc::new(BinaryArray::from_iter_values([b"a".as_slice()])),
            ],
        )
        .unwrap();

        let number_dictionary = DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![0]),
            Arc::new(Int32Array::from(vec![2])),
        )
        .unwrap();
        let text_dictionary = DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![0]),
            Arc::new(StringArray::from(vec!["b"])),
        )
        .unwrap();
        let bytes_dictionary = DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![0]),
            Arc::new(BinaryArray::from_iter_values([b"b".as_slice()])),
        )
        .unwrap();
        let dictionary = RecordBatch::try_from_iter(vec![
            ("number", Arc::new(number_dictionary) as ArrayRef),
            ("text", Arc::new(text_dictionary) as ArrayRef),
            ("bytes", Arc::new(bytes_dictionary) as ArrayRef),
        ])
        .unwrap();

        let run_ends = Int32Array::from(vec![1]);
        let number_ree: ArrayRef =
            Arc::new(Int32RunArray::try_new(&run_ends, &Int32Array::from(vec![3])).unwrap());
        let text_ree: ArrayRef =
            Arc::new(Int32RunArray::try_new(&run_ends, &StringArray::from(vec!["c"])).unwrap());
        let bytes_ree: ArrayRef = Arc::new(
            Int32RunArray::try_new(&run_ends, &BinaryArray::from_iter_values([b"c".as_slice()]))
                .unwrap(),
        );
        let ree = RecordBatch::try_from_iter(vec![
            ("number", number_ree),
            ("text", text_ree),
            ("bytes", bytes_ree),
        ])
        .unwrap();

        let alternate = RecordBatch::try_from_iter(vec![
            ("number", Arc::new(Int32Array::from(vec![4])) as ArrayRef),
            (
                "text",
                Arc::new(LargeStringArray::from(vec!["d"])) as ArrayRef,
            ),
            (
                "bytes",
                Arc::new(BinaryViewArray::from_iter_values([b"d".as_slice()])) as ArrayRef,
            ),
        ])
        .unwrap();

        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(1))
            .build();
        let mut out = Vec::new();
        let mut writer =
            ArrowWriter::try_new(&mut out, Arc::clone(&writer_schema), Some(props)).unwrap();
        for batch in [&dense, &dictionary, &ree, &alternate] {
            writer.write(batch).unwrap();
        }
        writer.close().unwrap();

        let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(out)).unwrap();
        assert_eq!(builder.metadata().num_row_groups(), 4);
        let mut reader = builder.build().unwrap();
        let actual = reader.next().unwrap().unwrap();
        assert_eq!(
            actual.column(0).as_primitive::<Int32Type>(),
            &Int32Array::from(vec![1, 2, 3, 4])
        );
        assert_eq!(
            actual.column(1).as_string::<i32>(),
            &StringArray::from(vec!["a", "b", "c", "d"])
        );
        assert_eq!(
            actual.column(2).as_binary::<i32>(),
            &BinaryArray::from_iter_values([
                b"a".as_slice(),
                b"b".as_slice(),
                b"c".as_slice(),
                b"d".as_slice(),
            ])
        );
    }

    #[test]
    fn cached_schema_plan_does_not_retain_batch_arrays() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "a",
            ArrowDataType::Int32,
            false,
        )]));
        let array = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![array.clone()]).unwrap();
        let mut writer = ArrowWriter::try_new(Vec::new(), schema, None).unwrap();

        writer.write(&batch).unwrap();
        drop(batch);
        assert_eq!(
            Arc::strong_count(&array),
            1,
            "schema and row-group caches must not retain input ArrayRefs"
        );
        writer.close().unwrap();
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

        let options = ReadOptionsBuilder::new().with_page_index().build();
        let reader =
            SerializedFileReader::new_with_options(file.try_clone().unwrap(), options).unwrap();

        let column = reader.metadata().row_group(0).columns();

        assert_eq!(column.len(), 1);

        // We should write one row before falling back to PLAIN encoding so there should still be a
        // dictionary page.
        assert!(
            column[0].dictionary_page_offset().is_some(),
            "Expected a dictionary page"
        );

        let page_index = reader
            .metadata()
            .page_index()
            .expect("page index should be present");
        let page_locations = page_index
            .page_locations(0, 0)
            .expect("page locations should exist");

        // We should fallback to PLAIN encoding after the first row and our max page size is 1 bytes
        // so we expect one dictionary encoded page and then a page per row thereafter.
        assert_eq!(
            page_locations.len(),
            10,
            "Expected 10 pages but got {page_locations:#?}"
        );
    }

    #[test]
    #[cfg_attr(miri, ignore)] // inline assembly is not supported
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

    const MEDIUM_SIZE: usize = 63;

    fn check_bloom_filter<T: AsBytes>(
        files: Vec<Bytes>,
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
    #[cfg_attr(miri, ignore)] // Takes too long
    fn all_null_primitive_single_column() {
        let values = Arc::new(Int32Array::from(vec![None; SMALL_SIZE]));
        RoundTripTest::new(values).run();
    }
    #[test]
    #[cfg_attr(miri, ignore)] // Takes too long
    fn null_single_column() {
        let values = Arc::new(NullArray::new(SMALL_SIZE));
        RoundTripTest::new(values).run();
        // null arrays are always nullable, a test with non-nullable nulls fails
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Takes too long
    fn bool_single_column() {
        required_and_optional::<BooleanArray, _>(
            [true, false].iter().cycle().copied().take(SMALL_SIZE),
        );
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Takes too long
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
        for row_group in file_meta_data.row_groups() {
            for column in row_group.columns() {
                assert!(column.offset_index_offset().is_some());
                assert!(column.offset_index_length().is_some());
                assert!(column.column_index_offset().is_some());
                assert!(column.column_index_length().is_some());
            }
        }
        if let Some(page_index) = file_meta_data.page_index() {
            for rg in 0..file_meta_data.num_row_groups() {
                for col in 0..file_meta_data.row_group(rg).num_columns() {
                    let idx = page_index
                        .column_index(rg, col)
                        .expect("column index should exist");
                    assert!(idx.nan_counts().is_some());
                    let ColumnIndexMetaData::DOUBLE(float_idx) = idx else {
                        panic!("expected double statistics")
                    };
                    for i in 0..idx.num_pages() as usize {
                        assert_eq!(float_idx.nan_count(i), Some(10));
                        assert_eq!(
                            f64::NAN.total_cmp(float_idx.min_value(i).unwrap()),
                            Ordering::Equal
                        );
                        assert_eq!(
                            f64::NAN.total_cmp(float_idx.max_value(i).unwrap()),
                            Ordering::Equal
                        );
                    }
                }
            }
        } else {
            panic!("page index should be present");
        }
    }

    #[test]
    fn check_page_offset_index_with_mixed_nan() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col",
            DataType::Float64,
            true,
        )]));

        let mut out = Vec::with_capacity(1024);
        let props = WriterProperties::builder()
            .set_data_page_row_count_limit(10)
            .build();
        let mut writer = ArrowWriter::try_new(&mut out, schema.clone(), Some(props))
            .expect("Unable to write file");

        // write a page of all NaN (since batch min and max are NaN, global min/max are NaN)
        let values = Arc::new(Float64Array::from(vec![f64::NAN; 10]));
        let batch = RecordBatch::try_new(schema.clone(), vec![values]).unwrap();
        writer.write(&batch).unwrap();

        // write a page of all -NaN (batch min/max is -NaN, should update global min to -NaN)
        let values = Arc::new(Float64Array::from(vec![-f64::NAN; 10]));
        let batch = RecordBatch::try_new(schema.clone(), vec![values]).unwrap();
        writer.write(&batch).unwrap();

        // write a page of all 0 (non-NaN should override global min/max, now 0/0)
        let values = Arc::new(Float64Array::from(vec![0_f64; 10]));
        let batch = RecordBatch::try_new(schema.clone(), vec![values]).unwrap();
        writer.write(&batch).unwrap();

        // write a mixed page (should now have min -1, max 1)
        let values = Arc::new(Float64Array::from(vec![
            -1.0,
            0.0,
            f64::NAN,
            -f64::NAN,
            1.0,
        ]));
        let batch = RecordBatch::try_new(schema.clone(), vec![values]).unwrap();
        writer.write(&batch).unwrap();

        let file_meta_data = writer.close().unwrap();

        // check the column chunk stats are correct
        let col_stats = file_meta_data
            .row_group(0)
            .column(0)
            .statistics()
            .expect("missing column chunk statistics");

        assert_eq!(col_stats.nan_count_opt(), Some(22));
        assert_eq!(col_stats.min_bytes_opt(), Some((-1.0f64).as_bytes()));
        assert_eq!(col_stats.max_bytes_opt(), Some(1.0f64.as_bytes()));

        assert!(file_meta_data.page_index().is_some());
        let col_idx = &file_meta_data.page_index().unwrap().column_index(0, 0);
        assert_eq!(col_idx.as_ref().unwrap().num_pages(), 4);

        // test each page
        let Some(ColumnIndexMetaData::DOUBLE(float_idx)) = col_idx else {
            panic!("expected double statistics")
        };

        assert_eq!(float_idx.nan_counts, Some(vec![10, 10, 0, 2]));
        assert_eq!(
            f64::NAN.total_cmp(float_idx.min_value(0).unwrap()),
            Ordering::Equal
        );
        assert_eq!(
            f64::NAN.total_cmp(float_idx.max_value(0).unwrap()),
            Ordering::Equal
        );
        assert_eq!(
            (-f64::NAN).total_cmp(float_idx.min_value(1).unwrap()),
            Ordering::Equal
        );
        assert_eq!(
            (-f64::NAN).total_cmp(float_idx.max_value(1).unwrap()),
            Ordering::Equal
        );
        assert_eq!(float_idx.min_value(2), Some(&0.0));
        assert_eq!(float_idx.max_value(2), Some(&0.0));
        assert_eq!(float_idx.min_value(3), Some(&-1.0));
        assert_eq!(float_idx.max_value(3), Some(&1.0));
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Takes too long
    fn interval_year_month_single_column() {
        required_and_optional::<IntervalYearMonthArray, _>(0..SMALL_SIZE as i32);
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Takes too long
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
    #[cfg_attr(miri, ignore)] // Takes too long
    fn i32_column_bloom_filter_at_end() {
        let array = Arc::new(Int32Array::from_iter(0..SMALL_SIZE as i32));
        let files = RoundTripTest::new(array)
            .with_nullable(false)
            .with_bloom_filter(true)
            .with_bloom_filter_position(BloomFilterPosition::End)
            .run();

        check_bloom_filter(
            files,
            "col".to_string(),
            (0..SMALL_SIZE as i32).collect(),
            (SMALL_SIZE as i32 + 1..SMALL_SIZE as i32 + 10).collect(),
        );
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Takes too long
    fn i32_column_bloom_filter() {
        let array = Arc::new(Int32Array::from_iter(0..SMALL_SIZE as i32));
        let files = RoundTripTest::new(array)
            .with_nullable(false)
            .with_bloom_filter(true)
            .run();

        check_bloom_filter(
            files,
            "col".to_string(),
            (0..SMALL_SIZE as i32).collect(),
            (SMALL_SIZE as i32 + 1..SMALL_SIZE as i32 + 10).collect(),
        );
    }

    fn write_with_bloom_filter(array: ArrayRef, dictionary_page_size_limit: usize) -> Bytes {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col",
            array.data_type().clone(),
            false,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();
        let props = WriterProperties::builder()
            .set_dictionary_enabled(true)
            .set_dictionary_page_size_limit(dictionary_page_size_limit)
            .set_write_batch_size(256)
            .set_bloom_filter_enabled(true)
            .build();
        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        Bytes::from(buf)
    }

    fn data_page_encoding_mask(file: &Bytes) -> EncodingMask {
        let metadata = ParquetMetaDataReader::new().parse_and_finish(file).unwrap();
        *metadata
            .row_group(0)
            .column(0)
            .page_encoding_stats_mask()
            .unwrap()
    }

    /// While a column is dictionary encoded the bloom filter is populated from the dictionary
    /// when it is flushed, so a chunk that stays dictionary encoded must still contain every value.
    #[test]
    fn string_column_bloom_filter_populated_from_dictionary() {
        let values: Vec<String> = (0..2000).map(|i| format!("value-{}", i % 10)).collect();
        let array = Arc::new(StringArray::from_iter_values(&values));
        let file = write_with_bloom_filter(array, 1024 * 1024);
        assert!(data_page_encoding_mask(&file).is_only(Encoding::RLE_DICTIONARY));

        check_bloom_filter(
            vec![file],
            "col".to_string(),
            (0..10).map(|i| format!("value-{i}").into_bytes()).collect(),
            (10..20)
                .map(|i| format!("value-{i}").into_bytes())
                .collect(),
        );
    }

    /// After falling back from dictionary encoding the filter holds the dictionary's values
    /// and every value written plain afterwards.
    #[test]
    fn string_column_bloom_filter_across_dictionary_fallback() {
        let values: Vec<String> = (0..2000).map(|i| format!("value-{i}")).collect();
        let array = Arc::new(StringArray::from_iter_values(&values));
        let file = write_with_bloom_filter(array, 1024);
        let encodings = data_page_encoding_mask(&file);
        assert!(
            encodings.is_set(Encoding::RLE_DICTIONARY) && encodings.is_set(Encoding::PLAIN),
            "expected dictionary and plain data pages, got {encodings:?}"
        );

        check_bloom_filter(
            vec![file],
            "col".to_string(),
            values.into_iter().map(String::into_bytes).collect(),
            (2000..2010)
                .map(|i| format!("value-{i}").into_bytes())
                .collect(),
        );
    }

    #[test]
    fn i64_column_bloom_filter_populated_from_dictionary() {
        let array = Arc::new(Int64Array::from_iter_values((0..2000).map(|i| i % 10)));
        let file = write_with_bloom_filter(array, 1024 * 1024);
        assert!(data_page_encoding_mask(&file).is_only(Encoding::RLE_DICTIONARY));

        check_bloom_filter(
            vec![file],
            "col".to_string(),
            (0..10i64).collect(),
            (10..20i64).collect(),
        );
    }

    #[test]
    fn i64_column_bloom_filter_across_dictionary_fallback() {
        let array = Arc::new(Int64Array::from_iter_values(0..2000i64));
        let file = write_with_bloom_filter(array, 1024);
        let encodings = data_page_encoding_mask(&file);
        assert!(
            encodings.is_set(Encoding::RLE_DICTIONARY) && encodings.is_set(Encoding::PLAIN),
            "expected dictionary and plain data pages, got {encodings:?}"
        );

        check_bloom_filter(
            vec![file],
            "col".to_string(),
            (0..2000i64).collect(),
            (2000..2010i64).collect(),
        );
    }

    /// Test that bloom filter folding produces correct results even when
    /// the configured NDV differs significantly from actual NDV.
    /// A large NDV means a larger initial filter that gets folded down;
    /// a small NDV means a smaller initial filter.
    #[test]
    #[cfg_attr(miri, ignore)] // Takes too long
    fn i32_column_bloom_filter_fixed_ndv() {
        let array = Arc::new(Int32Array::from_iter(0..SMALL_SIZE as i32));

        // NDV much larger than actual distinct values — tests folding a large filter down
        let files = RoundTripTest::new(array.clone())
            .with_nullable(false)
            .with_bloom_filter(true)
            .with_bloom_filter_ndv(1_000_000)
            .run();

        check_bloom_filter(
            files,
            "col".to_string(),
            (0..SMALL_SIZE as i32).collect(),
            (SMALL_SIZE as i32 + 1..SMALL_SIZE as i32 + 10).collect(),
        );

        // NDV smaller than actual distinct values — tests the underestimate path
        let files = RoundTripTest::new(array)
            .with_nullable(false)
            .with_bloom_filter(true)
            .with_bloom_filter_ndv(3)
            .run();

        check_bloom_filter(
            files,
            "col".to_string(),
            (0..SMALL_SIZE as i32).collect(),
            (SMALL_SIZE as i32 + 1..SMALL_SIZE as i32 + 10).collect(),
        );
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Takes too long
    fn binary_column_bloom_filter() {
        let one_vec: Vec<u8> = (0..SMALL_SIZE as u8).collect();
        let many_vecs: Vec<_> = std::iter::repeat_n(one_vec, SMALL_SIZE).collect();
        let many_vecs_iter = many_vecs.iter().map(|v| v.as_slice());

        let array = Arc::new(BinaryArray::from_iter_values(many_vecs_iter));
        let files = RoundTripTest::new(array)
            .with_nullable(false)
            .with_bloom_filter(true)
            .run();

        check_bloom_filter(
            files,
            "col".to_string(),
            many_vecs,
            vec![vec![(SMALL_SIZE + 1) as u8]],
        );
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Takes too long
    fn empty_string_null_column_bloom_filter() {
        let raw_values: Vec<_> = (0..SMALL_SIZE).map(|i| i.to_string()).collect();
        let raw_strs = raw_values.iter().map(|s| s.as_str());

        let array = Arc::new(StringArray::from_iter_values(raw_strs));
        let files = RoundTripTest::new(array)
            .with_nullable(false)
            .with_bloom_filter(true)
            .run();

        let optional_raw_values: Vec<_> = raw_values
            .iter()
            .enumerate()
            .filter_map(|(i, v)| if i % 2 == 0 { None } else { Some(v.as_str()) })
            .collect();
        // For null slots, empty string should not be in bloom filter.
        check_bloom_filter(files, "col".to_string(), optional_raw_values, vec![""]);
    }

    #[test]
    fn list_and_map_coerced_names() {
        // Create map and list with non-Parquet naming
        let list_field =
            Field::new_list("my_list", Field::new("item", DataType::Int32, false), false);
        let map_field = Field::new_map(
            "my_map",
            "my_entries",
            Field::new("my_keys", DataType::Int32, false),
            Field::new("my_values", DataType::Int32, true),
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

        let schema = file_metadata.file_metadata().schema();
        // Coerced name of "item" should be "element"
        let list_field = &schema.get_fields()[0].get_fields()[0];
        assert_eq!(list_field.get_fields()[0].name(), "element");

        let map_field = &schema.get_fields()[1].get_fields()[0];
        // Coerced name of "entries" should be "key_value"
        assert_eq!(map_field.name(), "key_value");
        // Coerced name of "my_keys" should be "key"
        assert_eq!(map_field.get_fields()[0].name(), "key");
        // Coerced name of "my_values" should be "value"
        assert_eq!(map_field.get_fields()[1].name(), "value");

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
    #[cfg_attr(miri, ignore)] // Takes too long
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
                    .set_max_row_group_row_count(Some(row_group_size))
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
    fn arrow_writer_test_type_compatibility() {
        fn ensure_compatible_write<T1, T2>(array1: T1, array2: T2, expected_result: T1)
        where
            T1: Array + 'static,
            T2: Array + 'static,
        {
            let schema1 = Arc::new(Schema::new(vec![Field::new(
                "a",
                array1.data_type().clone(),
                false,
            )]));

            let file = tempfile().unwrap();
            let mut writer =
                ArrowWriter::try_new(file.try_clone().unwrap(), schema1.clone(), None).unwrap();

            let rb1 = RecordBatch::try_new(schema1.clone(), vec![Arc::new(array1)]).unwrap();
            writer.write(&rb1).unwrap();

            let schema2 = Arc::new(Schema::new(vec![Field::new(
                "a",
                array2.data_type().clone(),
                false,
            )]));
            let rb2 = RecordBatch::try_new(schema2, vec![Arc::new(array2)]).unwrap();
            writer.write(&rb2).unwrap();

            writer.close().unwrap();

            let mut record_batch_reader =
                ParquetRecordBatchReader::try_new(file.try_clone().unwrap(), 1024).unwrap();
            let actual_batch = record_batch_reader.next().unwrap().unwrap();

            let expected_batch =
                RecordBatch::try_new(schema1, vec![Arc::new(expected_result)]).unwrap();
            assert_eq!(actual_batch, expected_batch);
        }

        // check compatibility between native and dictionaries

        ensure_compatible_write(
            DictionaryArray::new(
                UInt8Array::from_iter_values(vec![0]),
                Arc::new(StringArray::from_iter_values(vec!["parquet"])),
            ),
            StringArray::from_iter_values(vec!["barquet"]),
            DictionaryArray::new(
                UInt8Array::from_iter_values(vec![0, 1]),
                Arc::new(StringArray::from_iter_values(vec!["parquet", "barquet"])),
            ),
        );

        ensure_compatible_write(
            StringArray::from_iter_values(vec!["parquet"]),
            DictionaryArray::new(
                UInt8Array::from_iter_values(vec![0]),
                Arc::new(StringArray::from_iter_values(vec!["barquet"])),
            ),
            StringArray::from_iter_values(vec!["parquet", "barquet"]),
        );

        // check compatibility between dictionaries with different key types

        ensure_compatible_write(
            DictionaryArray::new(
                UInt8Array::from_iter_values(vec![0]),
                Arc::new(StringArray::from_iter_values(vec!["parquet"])),
            ),
            DictionaryArray::new(
                UInt16Array::from_iter_values(vec![0]),
                Arc::new(StringArray::from_iter_values(vec!["barquet"])),
            ),
            DictionaryArray::new(
                UInt8Array::from_iter_values(vec![0, 1]),
                Arc::new(StringArray::from_iter_values(vec!["parquet", "barquet"])),
            ),
        );

        // check compatibility between dictionaries with different value types
        ensure_compatible_write(
            DictionaryArray::new(
                UInt8Array::from_iter_values(vec![0]),
                Arc::new(StringArray::from_iter_values(vec!["parquet"])),
            ),
            DictionaryArray::new(
                UInt8Array::from_iter_values(vec![0]),
                Arc::new(LargeStringArray::from_iter_values(vec!["barquet"])),
            ),
            DictionaryArray::new(
                UInt8Array::from_iter_values(vec![0, 1]),
                Arc::new(StringArray::from_iter_values(vec!["parquet", "barquet"])),
            ),
        );

        // check compatibility between a dictionary and a native array with a different type
        ensure_compatible_write(
            DictionaryArray::new(
                UInt8Array::from_iter_values(vec![0]),
                Arc::new(StringArray::from_iter_values(vec!["parquet"])),
            ),
            LargeStringArray::from_iter_values(vec!["barquet"]),
            DictionaryArray::new(
                UInt8Array::from_iter_values(vec![0, 1]),
                Arc::new(StringArray::from_iter_values(vec!["parquet", "barquet"])),
            ),
        );

        // check compatibility for string types

        ensure_compatible_write(
            StringArray::from_iter_values(vec!["parquet"]),
            LargeStringArray::from_iter_values(vec!["barquet"]),
            StringArray::from_iter_values(vec!["parquet", "barquet"]),
        );

        ensure_compatible_write(
            LargeStringArray::from_iter_values(vec!["parquet"]),
            StringArray::from_iter_values(vec!["barquet"]),
            LargeStringArray::from_iter_values(vec!["parquet", "barquet"]),
        );

        ensure_compatible_write(
            StringArray::from_iter_values(vec!["parquet"]),
            StringViewArray::from_iter_values(vec!["barquet"]),
            StringArray::from_iter_values(vec!["parquet", "barquet"]),
        );

        ensure_compatible_write(
            StringViewArray::from_iter_values(vec!["parquet"]),
            StringArray::from_iter_values(vec!["barquet"]),
            StringViewArray::from_iter_values(vec!["parquet", "barquet"]),
        );

        ensure_compatible_write(
            LargeStringArray::from_iter_values(vec!["parquet"]),
            StringViewArray::from_iter_values(vec!["barquet"]),
            LargeStringArray::from_iter_values(vec!["parquet", "barquet"]),
        );

        ensure_compatible_write(
            StringViewArray::from_iter_values(vec!["parquet"]),
            LargeStringArray::from_iter_values(vec!["barquet"]),
            StringViewArray::from_iter_values(vec!["parquet", "barquet"]),
        );

        // check compatibility for binary types

        ensure_compatible_write(
            BinaryArray::from_iter_values(vec![b"parquet"]),
            LargeBinaryArray::from_iter_values(vec![b"barquet"]),
            BinaryArray::from_iter_values(vec![b"parquet", b"barquet"]),
        );

        ensure_compatible_write(
            LargeBinaryArray::from_iter_values(vec![b"parquet"]),
            BinaryArray::from_iter_values(vec![b"barquet"]),
            LargeBinaryArray::from_iter_values(vec![b"parquet", b"barquet"]),
        );

        ensure_compatible_write(
            BinaryArray::from_iter_values(vec![b"parquet"]),
            BinaryViewArray::from_iter_values(vec![b"barquet"]),
            BinaryArray::from_iter_values(vec![b"parquet", b"barquet"]),
        );

        ensure_compatible_write(
            BinaryViewArray::from_iter_values(vec![b"parquet"]),
            BinaryArray::from_iter_values(vec![b"barquet"]),
            BinaryViewArray::from_iter_values(vec![b"parquet", b"barquet"]),
        );

        ensure_compatible_write(
            BinaryViewArray::from_iter_values(vec![b"parquet"]),
            LargeBinaryArray::from_iter_values(vec![b"barquet"]),
            BinaryViewArray::from_iter_values(vec![b"parquet", b"barquet"]),
        );

        ensure_compatible_write(
            LargeBinaryArray::from_iter_values(vec![b"parquet"]),
            BinaryViewArray::from_iter_values(vec![b"barquet"]),
            LargeBinaryArray::from_iter_values(vec![b"parquet", b"barquet"]),
        );

        // check compatibility for list types

        let list_field_metadata = HashMap::from_iter(vec![(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            "1".to_string(),
        )]);
        let list_field = Field::new_list_field(DataType::Int32, false);

        let values1 = Arc::new(Int32Array::from(vec![0, 1, 2, 3, 4]));
        let offsets1 = OffsetBuffer::new(vec![0, 2, 5].into());

        let values2 = Arc::new(Int32Array::from(vec![5, 6, 7, 8, 9]));
        let offsets2 = OffsetBuffer::new(vec![0, 3, 5].into());

        let values_expected = Arc::new(Int32Array::from(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]));
        let offsets_expected = OffsetBuffer::new(vec![0, 2, 5, 8, 10].into());

        ensure_compatible_write(
            // when the initial schema has the metadata ...
            ListArray::try_new(
                Arc::new(
                    list_field
                        .clone()
                        .with_metadata(list_field_metadata.clone()),
                ),
                offsets1,
                values1,
                None,
            )
            .unwrap(),
            // ... and some intermediate schema doesn't have the metadata
            ListArray::try_new(Arc::new(list_field.clone()), offsets2, values2, None).unwrap(),
            // ... the write will still go through, and the resulting schema will inherit the initial metadata
            ListArray::try_new(
                Arc::new(
                    list_field
                        .clone()
                        .with_metadata(list_field_metadata.clone()),
                ),
                offsets_expected,
                values_expected,
                None,
            )
            .unwrap(),
        );
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Takes too long
    fn u32_min_max() {
        // check values roundtrip through parquet
        let src = [
            u32::MIN,
            1,
            (i32::MAX as u32) - 1,
            i32::MAX as u32,
            (i32::MAX as u32) + 1,
            u32::MAX - 1,
            u32::MAX,
        ];
        let values = Arc::new(UInt32Array::from_iter_values(src.iter().copied()));
        let files = RoundTripTest::new(values).with_nullable(false).run();

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
    #[cfg_attr(miri, ignore)] // Takes too long
    fn u64_min_max() {
        // check values roundtrip through parquet
        let src = [
            u64::MIN,
            1,
            (i64::MAX as u64) - 1,
            i64::MAX as u64,
            (i64::MAX as u64) + 1,
            u64::MAX - 1,
            u64::MAX,
        ];
        let values = Arc::new(UInt64Array::from_iter_values(src.iter().copied()));
        let files = RoundTripTest::new(values).with_nullable(false).run();

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
    #[cfg_attr(miri, ignore)] // Takes too long
    fn statistics_null_counts_only_nulls() {
        // check that null-count statistics for "only NULL"-columns are correct
        let values = Arc::new(UInt64Array::from(vec![None, None]));
        let files = RoundTripTest::new(values).run();

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
    #[cfg_attr(miri, ignore)] // Takes too long
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

        RoundTripTest::new(array).run();
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
            .set_max_row_group_row_count(Some(200))
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
                    .copied()
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

        let expected = r"
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
        ".trim().split('\n').map(|x| x.trim()).collect::<Vec<_>>().join("\n");

        let actual = pretty_format_batches(batches).unwrap().to_string();
        assert_eq!(actual, expected);

        // Write data
        let file = tempfile::tempfile().unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(6))
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
        let file_schema = batch_schema.clone().with_metadata([("foo", "bar")]);

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

        fn check(values: ArrayRef, props: WriterProperties) {
            let batch = RecordBatch::try_from_iter([("a", values)]).unwrap();
            let mut writer = ArrowWriter::try_new(Vec::new(), batch.schema(), Some(props)).unwrap();
            writer.write(&batch).unwrap();
            assert!(writer.memory_size() >= writer.in_progress_size());
            writer.flush().unwrap();
            assert_eq!(writer.memory_size(), 0);
            writer.close().unwrap();
        }

        check(
            Arc::new(BooleanArray::from(vec![true, false, true])),
            WriterProperties::builder()
                .set_dictionary_enabled(false)
                .set_encoding(Encoding::RLE)
                .set_statistics_enabled(EnabledStatistics::None)
                .build(),
        );
        check(
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            WriterProperties::builder()
                .set_dictionary_enabled(false)
                .set_encoding(Encoding::DELTA_BINARY_PACKED)
                .build(),
        );
        check(
            Arc::new(StringArray::from(vec!["prefix-a", "prefix-b"])),
            WriterProperties::builder()
                .set_dictionary_enabled(false)
                .set_encoding(Encoding::DELTA_BYTE_ARRAY)
                .build(),
        );
        let fixed =
            FixedSizeBinaryArray::try_from_iter([[1, 2, 3, 4], [5, 6, 7, 8]].into_iter()).unwrap();
        check(
            Arc::new(fixed),
            WriterProperties::builder()
                .set_dictionary_enabled(false)
                .set_encoding(Encoding::BYTE_STREAM_SPLIT)
                .build(),
        );
        for limit in [usize::MAX, 1] {
            check(
                Arc::new(StringArray::from(vec!["dictionary-a", "dictionary-b"])),
                WriterProperties::builder()
                    .set_dictionary_page_size_limit(limit)
                    .build(),
            );
        }
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
        let index = reader.metadata().page_index().unwrap();

        assert_eq!(index.num_data_pages(0, 0), Some(1)); // 1 page
        assert_eq!(index.num_data_pages(0, 1), Some(1)); // 1 page
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
        assert_eq!(metadata.num_row_groups(), 1);
        let row_group = metadata.row_group(0);
        assert_eq!(row_group.num_columns(), 2);
        // Column "a" has both offset and column index, as requested
        assert!(row_group.column(0).offset_index_offset().is_some());
        assert!(row_group.column(0).column_index_offset().is_some());
        // Column "b" should only have offset index
        assert!(row_group.column(1).offset_index_offset().is_some());
        assert!(row_group.column(1).column_index_offset().is_none());

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

        let page_index = reader.metadata().page_index().unwrap();

        let a_idx = page_index.column_index(0, 0);
        assert!(
            matches!(a_idx, Some(ColumnIndexMetaData::BYTE_ARRAY(_))),
            "{a_idx:?}"
        );
        let b_idx = page_index.column_index(0, 1);
        assert!(b_idx.is_none(), "{b_idx:?}");
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
        assert_eq!(metadata.num_row_groups(), 1);
        let row_group = metadata.row_group(0);
        assert_eq!(row_group.num_columns(), 2);
        // Column "a" should only have offset index
        assert!(row_group.column(0).offset_index_offset().is_some());
        assert!(row_group.column(0).column_index_offset().is_none());
        // Column "b" should only have offset index
        assert!(row_group.column(1).offset_index_offset().is_some());
        assert!(row_group.column(1).column_index_offset().is_none());

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

        let page_index = reader.metadata().page_index().unwrap();

        let a_idx = page_index.column_index(0, 0);
        assert!(a_idx.is_none(), "{a_idx:?}");
        let b_idx = page_index.column_index(0, 1);
        assert!(b_idx.is_none(), "{b_idx:?}");
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
            assert!(
                !key_value_metadata
                    .iter()
                    .any(|kv| kv.key.as_str() == ARROW_SCHEMA_META_KEY)
            );
        }
    }

    #[test]
    fn test_arrow_writer_skip_path_in_schema() {
        let batch_schema = Schema::new(vec![Field::new("int32", DataType::Int32, false)]);
        let file_schema = Arc::new(batch_schema.clone());

        let batch = RecordBatch::try_new(
            Arc::new(batch_schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as _],
        )
        .unwrap();

        // default options should still write path_in_schema
        let skip_options = ArrowWriterOptions::new();

        let mut buf = Vec::with_capacity(1024);
        let mut writer =
            ArrowWriter::try_new_with_options(&mut buf, file_schema.clone(), skip_options).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        // override to not write path_in_schema
        let skip_options = ArrowWriterOptions::new().with_properties(
            WriterProperties::builder()
                .set_write_path_in_schema(false)
                .build(),
        );

        let mut buf2 = Vec::with_capacity(1024);
        let mut writer =
            ArrowWriter::try_new_with_options(&mut buf2, file_schema.clone(), skip_options)
                .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        // buf2 should be a bit smaller due to lack of path_in_schema
        assert!(buf.len() > buf2.len());
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

    #[test]
    fn test_page_stats_not_written_by_default() {
        let string_field = Field::new("a", DataType::Utf8, false);
        let schema = Schema::new(vec![string_field]);
        let raw_string_values = vec!["Blart Versenwald III"];
        let string_values = StringArray::from(raw_string_values.clone());
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(string_values)]).unwrap();

        let props = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::Page)
            .set_dictionary_enabled(false)
            .set_encoding(Encoding::PLAIN)
            .set_compression(crate::basic::Compression::UNCOMPRESSED)
            .build();

        let file = roundtrip_opts(&batch, props);

        // read file and decode page headers
        // Note: use the thrift API as there is no Rust API to access the statistics in the page headers

        // decode first page header
        let first_page = &file[4..];
        let mut prot = ThriftSliceInputProtocol::new(first_page);
        let hdr = PageHeader::read_thrift(&mut prot).unwrap();
        let stats = hdr.data_page_header.unwrap().statistics;

        assert!(stats.is_none());
    }

    #[test]
    fn test_page_stats_when_enabled() {
        let string_field = Field::new("a", DataType::Utf8, false);
        let schema = Schema::new(vec![string_field]);
        let raw_string_values = vec!["Blart Versenwald III", "Andrew Lamb"];
        let string_values = StringArray::from(raw_string_values.clone());
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(string_values)]).unwrap();

        let props = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::Page)
            .set_dictionary_enabled(false)
            .set_encoding(Encoding::PLAIN)
            .set_write_page_header_statistics(true)
            .set_compression(crate::basic::Compression::UNCOMPRESSED)
            .build();

        let file = roundtrip_opts(&batch, props);

        // read file and decode page headers
        // Note: use the thrift API as there is no Rust API to access the statistics in the page headers

        // decode first page header
        let first_page = &file[4..];
        let mut prot = ThriftSliceInputProtocol::new(first_page);
        let hdr = PageHeader::read_thrift(&mut prot).unwrap();
        let stats = hdr.data_page_header.unwrap().statistics;

        let stats = stats.unwrap();
        // check that min/max were actually written to the page
        assert!(stats.is_max_value_exact.unwrap());
        assert!(stats.is_min_value_exact.unwrap());
        assert_eq!(stats.max_value.unwrap(), b"Blart Versenwald III");
        assert_eq!(stats.min_value.unwrap(), b"Andrew Lamb");
    }

    #[test]
    fn test_page_stats_truncation() {
        let string_field = Field::new("a", DataType::Utf8, false);
        let binary_field = Field::new("b", DataType::Binary, false);
        let schema = Schema::new(vec![string_field, binary_field]);

        let raw_string_values = vec!["Blart Versenwald III"];
        let raw_binary_values = [b"Blart Versenwald III".to_vec()];
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

        let props = WriterProperties::builder()
            .set_statistics_truncate_length(Some(2))
            .set_dictionary_enabled(false)
            .set_encoding(Encoding::PLAIN)
            .set_write_page_header_statistics(true)
            .set_compression(crate::basic::Compression::UNCOMPRESSED)
            .build();

        let file = roundtrip_opts(&batch, props);

        // read file and decode page headers
        // Note: use the thrift API as there is no Rust API to access the statistics in the page headers

        // decode first page header
        let first_page = &file[4..];
        let mut prot = ThriftSliceInputProtocol::new(first_page);
        let hdr = PageHeader::read_thrift(&mut prot).unwrap();
        let stats = hdr.data_page_header.unwrap().statistics;
        assert!(stats.is_some());
        let stats = stats.unwrap();
        // check that min/max were properly truncated
        assert!(!stats.is_max_value_exact.unwrap());
        assert!(!stats.is_min_value_exact.unwrap());
        assert_eq!(stats.max_value.unwrap(), b"Bm");
        assert_eq!(stats.min_value.unwrap(), b"Bl");

        // check second page now
        let second_page = &prot.as_slice()[hdr.compressed_page_size as usize..];
        let mut prot = ThriftSliceInputProtocol::new(second_page);
        let hdr = PageHeader::read_thrift(&mut prot).unwrap();
        let stats = hdr.data_page_header.unwrap().statistics;
        assert!(stats.is_some());
        let stats = stats.unwrap();
        // check that min/max were properly truncated
        assert!(!stats.is_max_value_exact.unwrap());
        assert!(!stats.is_min_value_exact.unwrap());
        assert_eq!(stats.max_value.unwrap(), b"Bm");
        assert_eq!(stats.min_value.unwrap(), b"Bl");
    }

    #[test]
    fn test_page_encoding_statistics_roundtrip() {
        let batch_schema = Schema::new(vec![Field::new(
            "int32",
            arrow_schema::DataType::Int32,
            false,
        )]);

        let batch = RecordBatch::try_new(
            Arc::new(batch_schema.clone()),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as _],
        )
        .unwrap();

        let mut file: File = tempfile::tempfile().unwrap();
        let mut writer = ArrowWriter::try_new(&mut file, Arc::new(batch_schema), None).unwrap();
        writer.write(&batch).unwrap();
        let file_metadata = writer.close().unwrap();

        assert_eq!(file_metadata.num_row_groups(), 1);
        assert_eq!(file_metadata.row_group(0).num_columns(), 1);
        assert!(
            file_metadata
                .row_group(0)
                .column(0)
                .page_encoding_stats()
                .is_some()
        );
        let chunk_page_stats = file_metadata
            .row_group(0)
            .column(0)
            .page_encoding_stats()
            .unwrap();

        // check that the read metadata is also correct
        let options = ReadOptionsBuilder::new()
            .with_page_index()
            .with_encoding_stats_as_mask(false)
            .build();
        let reader = SerializedFileReader::new_with_options(file, options).unwrap();

        let rowgroup = reader.get_row_group(0).expect("row group missing");
        assert_eq!(rowgroup.num_columns(), 1);
        let column = rowgroup.metadata().column(0);
        assert!(column.page_encoding_stats().is_some());
        let file_page_stats = column.page_encoding_stats().unwrap();
        assert_eq!(chunk_page_stats, file_page_stats);
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Takes too long
    fn test_different_dict_page_size_limit() {
        let array = Arc::new(Int64Array::from_iter(0..1024 * 1024));
        let schema = Arc::new(Schema::new(vec![
            Field::new("col0", arrow_schema::DataType::Int64, false),
            Field::new("col1", arrow_schema::DataType::Int64, false),
        ]));
        let batch =
            arrow_array::RecordBatch::try_new(schema.clone(), vec![array.clone(), array]).unwrap();

        let props = WriterProperties::builder()
            .set_dictionary_page_size_limit(1024 * 1024)
            .set_column_dictionary_page_size_limit(ColumnPath::from("col1"), 1024 * 1024 * 4)
            .build();
        let mut writer = ArrowWriter::try_new(Vec::new(), schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        let data = Bytes::from(writer.into_inner().unwrap());

        let mut metadata = ParquetMetaDataReader::new();
        metadata.try_parse(&data).unwrap();
        let metadata = metadata.finish().unwrap();
        let col0_meta = metadata.row_group(0).column(0);
        let col1_meta = metadata.row_group(0).column(1);

        let get_dict_page_size = move |meta: &ColumnChunkMetaData| {
            let mut reader =
                SerializedPageReader::new(Arc::new(data.clone()), meta, 0, None).unwrap();
            let page = reader.get_next_page().unwrap().unwrap();
            match page {
                Page::DictionaryPage { buf, .. } => buf.len(),
                _ => panic!("expected DictionaryPage"),
            }
        };

        assert_eq!(get_dict_page_size(col0_meta), 1024 * 1024);
        assert_eq!(get_dict_page_size(col1_meta), 1024 * 1024 * 4);
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Takes too long
    fn test_arrow_writer_granular_mode_roundtrip() {
        // Granular mode subdivides chunks and writes more pages than the
        // default batched path. Make sure the data we write back is
        // bit-identical to what went in — page-count assertions elsewhere
        // only prove pages were cut, not that the encoded data is correct.
        //
        // Mix value sizes so that the cumulative-byte-budget cutoff
        // lands mid-chunk, exercising both batched and granular paths
        // within the same `write_batch_internal` call.
        let small = "tiny".to_string();
        let big = "x".repeat(64 * 1024);
        let strings: Vec<String> = (0..256)
            .map(|i| {
                if i % 16 == 0 {
                    big.clone()
                } else {
                    small.clone()
                }
            })
            .collect();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "col",
            ArrowDataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(strings.clone())) as _],
        )
        .unwrap();

        let props = WriterProperties::builder()
            .set_dictionary_enabled(false)
            .set_data_page_size_limit(16 * 1024)
            .build();
        let mut writer = ArrowWriter::try_new(Vec::new(), schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        let data = Bytes::from(writer.into_inner().unwrap());

        let mut reader = ParquetRecordBatchReader::try_new(data, 1024).unwrap();
        let read = reader.next().unwrap().unwrap();
        assert!(reader.next().is_none(), "expected one batch");
        let col = read
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(col.len(), strings.len());
        for (i, expected) in strings.iter().enumerate() {
            assert_eq!(
                col.value(i),
                expected.as_str(),
                "value mismatch at index {i}"
            );
        }
    }

    #[test]
    fn test_arrow_writer_all_null_string_column() {
        // The `LevelDataRef::value_count` Uniform branch with
        // `value != max_def` (entirely-null chunk) must return 0 so the
        // sub-batch sizer short-circuits to batch mode without trying
        // to estimate byte budgets for non-existent values.
        let num_rows = 1024;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col",
            ArrowDataType::Utf8,
            true,
        )]));
        let nulls: Vec<Option<&str>> = vec![None; num_rows];
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(nulls)) as _],
        )
        .unwrap();

        let props = WriterProperties::builder()
            .set_dictionary_enabled(false)
            .set_data_page_size_limit(16 * 1024)
            .build();
        let mut writer = ArrowWriter::try_new(Vec::new(), schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        let data = Bytes::from(writer.into_inner().unwrap());

        // Re-parse the file: row group has one column, every row is
        // null, all data pages report `num_rows / page_count` rows.
        let mut metadata = ParquetMetaDataReader::new();
        metadata.try_parse(&data).unwrap();
        let metadata = metadata.finish().unwrap();
        let row_group = metadata.row_group(0);
        let col_meta = row_group.column(0);
        assert_eq!(row_group.num_rows() as usize, num_rows);
        // Statistics record `null_count = num_rows` — proves every value
        // was written as null.
        if let Some(stats) = col_meta.statistics() {
            assert_eq!(
                stats.null_count_opt().unwrap_or(0) as usize,
                num_rows,
                "expected all-null column to report null_count = num_rows"
            );
        }

        let mut reader =
            SerializedPageReader::new(Arc::new(data.clone()), col_meta, num_rows, None).unwrap();
        let mut total_values = 0u32;
        while let Some(page) = reader.get_next_page().unwrap() {
            if matches!(page, Page::DataPage { .. } | Page::DataPageV2 { .. }) {
                total_values += page.num_values();
            }
        }
        assert_eq!(
            total_values as usize, num_rows,
            "expected every level position to be represented in some page"
        );
    }

    struct WriteBatchesShape {
        num_batches: usize,
        rows_per_batch: usize,
        row_size: usize,
    }

    /// Helper function to write batches with the provided `WriteBatchesShape` into an `ArrowWriter`
    fn write_batches(
        WriteBatchesShape {
            num_batches,
            rows_per_batch,
            row_size,
        }: WriteBatchesShape,
        props: WriterProperties,
    ) -> ParquetRecordBatchReaderBuilder<File> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "str",
            ArrowDataType::Utf8,
            false,
        )]));
        let file = tempfile::tempfile().unwrap();
        let mut writer =
            ArrowWriter::try_new(file.try_clone().unwrap(), schema.clone(), Some(props)).unwrap();

        for batch_idx in 0..num_batches {
            let strings: Vec<String> = (0..rows_per_batch)
                .map(|i| format!("{:0>width$}", batch_idx * 10 + i, width = row_size))
                .collect();
            let array = StringArray::from(strings);
            let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap();
            writer.write(&batch).unwrap();
        }
        writer.close().unwrap();
        ParquetRecordBatchReaderBuilder::try_new(file).unwrap()
    }

    #[test]
    // When both limits are None, all data should go into a single row group
    fn test_row_group_limit_none_writes_single_row_group() {
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(None)
            .set_max_row_group_bytes(None)
            .build();

        let builder = write_batches(
            WriteBatchesShape {
                num_batches: 1,
                rows_per_batch: 1000,
                row_size: 4,
            },
            props,
        );

        assert_eq!(
            &row_group_sizes(builder.metadata()),
            &[1000],
            "With no limits, all rows should be in a single row group"
        );
    }

    #[test]
    // When only max_row_group_size is set, respect the row limit
    fn test_row_group_limit_rows_only() {
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(300))
            .set_max_row_group_bytes(None)
            .build();

        let builder = write_batches(
            WriteBatchesShape {
                num_batches: 1,
                rows_per_batch: 1000,
                row_size: 4,
            },
            props,
        );

        assert_eq!(
            &row_group_sizes(builder.metadata()),
            &[300, 300, 300, 100],
            "Row groups should be split by row count"
        );
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Takes too long
    // A row limit far smaller than the batch splits it many times over; the split must not
    // consume stack proportional to the number of row groups.
    fn test_row_group_limit_rows_only_many_splits() {
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(1))
            .set_max_row_group_bytes(None)
            .build();

        let rows = 50_000;
        let builder = write_batches(
            WriteBatchesShape {
                num_batches: 1,
                rows_per_batch: rows,
                row_size: 4,
            },
            props,
        );

        let sizes = row_group_sizes(builder.metadata());
        assert_eq!(sizes.len(), rows, "Every row should get its own row group");
        assert_eq!(
            sizes.iter().sum::<i64>(),
            rows as i64,
            "Total rows should be preserved"
        );
    }

    #[test]
    // When only max_row_group_bytes is set, respect the byte limit
    fn test_row_group_limit_bytes_only() {
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(None)
            // Set byte limit to approximately fit ~30 rows worth of data (~100 bytes each)
            .set_max_row_group_bytes(Some(3500))
            .build();

        let builder = write_batches(
            WriteBatchesShape {
                num_batches: 10,
                rows_per_batch: 10,
                row_size: 100,
            },
            props,
        );

        let sizes = row_group_sizes(builder.metadata());

        assert!(
            sizes.len() > 1,
            "Should have multiple row groups due to byte limit, got {sizes:?}",
        );

        let total_rows: i64 = sizes.iter().sum();
        assert_eq!(total_rows, 100, "Total rows should be preserved");
    }

    #[test]
    // If an in-progress row group is already oversized, it should be flushed before writing more.
    fn test_row_group_limit_bytes_flushes_when_current_group_already_too_large() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "str",
            ArrowDataType::Utf8,
            false,
        )]));
        let file = tempfile::tempfile().unwrap();

        // Start with no byte limit so we can intentionally build an oversized in-progress row group.
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(None)
            .set_max_row_group_bytes(None)
            .build();
        let mut writer =
            ArrowWriter::try_new(file.try_clone().unwrap(), schema.clone(), Some(props)).unwrap();

        let first_array = StringArray::from(
            (0..10)
                .map(|i| format!("{i:0>100}"))
                .collect::<Vec<String>>(),
        );
        let first_batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(first_array)]).unwrap();
        writer.write(&first_batch).unwrap();
        assert_eq!(writer.in_progress_rows(), 10);

        // Tighten the limit below the current in-progress bytes to exercise:
        // `if current_bytes >= max_bytes { self.flush()?; ... }`
        writer.max_row_group_bytes = Some(1);

        let second_array = StringArray::from(vec!["x".to_string()]);
        let second_batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(second_array)]).unwrap();
        writer.write(&second_batch).unwrap();
        writer.close().unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();

        assert_eq!(
            &row_group_sizes(builder.metadata()),
            &[10, 1],
            "The second write should flush an oversized in-progress row group first",
        );
    }

    #[test]
    // When both limits are set, the row limit triggers first
    fn test_row_group_limit_both_row_wins_single_batch() {
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(200)) // Will trigger at 200 rows
            .set_max_row_group_bytes(Some(1024 * 1024)) // 1MB - won't trigger for small int data
            .build();

        let builder = write_batches(
            WriteBatchesShape {
                num_batches: 1,
                row_size: 4,
                rows_per_batch: 1000,
            },
            props,
        );

        assert_eq!(
            &row_group_sizes(builder.metadata()),
            &[200, 200, 200, 200, 200],
            "Row limit should trigger before byte limit"
        );
    }

    #[test]
    // When both limits are set, the row limit triggers first
    fn test_row_group_limit_both_row_wins_multiple_batches() {
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(5)) // Will trigger every 5 rows
            .set_max_row_group_bytes(Some(9999)) // Won't trigger
            .build();

        let builder = write_batches(
            WriteBatchesShape {
                num_batches: 10,
                rows_per_batch: 10,
                row_size: 100,
            },
            props,
        );

        assert_eq!(
            &row_group_sizes(builder.metadata()),
            &[5; 20],
            "Row limit should trigger before byte limit"
        );
    }

    #[test]
    // When both limits are set, the byte limit triggers first
    fn test_row_group_limit_both_bytes_wins() {
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(1000)) // Won't trigger for 100 rows
            .set_max_row_group_bytes(Some(3500)) // Will trigger at ~30-35 rows
            .build();

        let builder = write_batches(
            WriteBatchesShape {
                num_batches: 10,
                rows_per_batch: 10,
                row_size: 100,
            },
            props,
        );

        let sizes = row_group_sizes(builder.metadata());

        assert!(
            sizes.len() > 1,
            "Byte limit should trigger before row limit, got {sizes:?}",
        );

        assert!(
            sizes.iter().all(|&s| s < 1000),
            "No row group should hit the row limit"
        );

        let total_rows: i64 = sizes.iter().sum();
        assert_eq!(total_rows, 100, "Total rows should be preserved");
    }

    #[test]
    // Both limits can apply to the same batch: the row limit trims it to 5 rows, and the
    // byte limit then trims those 5 down to 4.
    fn test_row_group_limit_both_apply_to_same_batch() {
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(15))
            .set_max_row_group_bytes(Some(1500))
            .build();

        let builder = write_batches(
            WriteBatchesShape {
                num_batches: 2,
                rows_per_batch: 10,
                row_size: 100,
            },
            props,
        );

        assert_eq!(
            &row_group_sizes(builder.metadata()),
            &[14, 6],
            "Byte limit should still apply to a batch the row limit already split"
        );
    }

    #[test]
    fn arrow_column_chunk_close_mut_drops_column_index() {
        use crate::arrow::ArrowSchemaConverter;
        use crate::file::writer::SerializedFileWriter;

        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, false)]));
        let props = Arc::new(
            WriterProperties::builder()
                .set_statistics_enabled(EnabledStatistics::Page)
                .build(),
        );
        let parquet_schema = ArrowSchemaConverter::new()
            .with_coerce_types(props.coerce_types())
            .convert(&schema)
            .unwrap();

        let mut buf = Vec::with_capacity(1024);
        let mut writer =
            SerializedFileWriter::new(&mut buf, parquet_schema.root_schema_ptr(), props.clone())
                .unwrap();

        let factory = ArrowRowGroupWriterFactory::new(&writer, Arc::clone(&schema));
        let mut col_writers = factory.create_column_writers(0).unwrap();
        let arr: ArrayRef = Arc::new(Int32Array::from_iter_values(0..64));
        for leaves in compute_leaves(schema.field(0), &arr).unwrap() {
            col_writers[0].write(&leaves).unwrap();
        }
        let mut chunk = col_writers.pop().unwrap().close().unwrap();

        // Immutable accessor exposes the close result produced at close time.
        assert!(
            chunk.close().column_index.is_some(),
            "EnabledStatistics::Page should produce a column_index"
        );

        // Mutable accessor lets callers drop the page-level index before append.
        chunk.close_mut().column_index = None;
        assert!(chunk.close().column_index.is_none());

        let mut rg = writer.next_row_group().unwrap();
        chunk.append_to_row_group(&mut rg).unwrap();
        rg.close().unwrap();
        let file_meta = writer.close().unwrap();

        // After dropping column_index, the resulting file records no column
        // index offset/length for this chunk.
        let cc = file_meta.row_group(0).column(0);
        assert!(cc.column_index_range().is_none());
    }

    /// Writes a single-column RecordBatch to an in-memory Parquet buffer.
    fn write_column_to_bytes(array: ArrayRef) -> Bytes {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col",
            array.data_type().clone(),
            true,
        )]));
        let buf = get_bytes_after_close(
            schema.clone(),
            &RecordBatch::try_new(schema, vec![array]).unwrap(),
        );
        Bytes::from(buf)
    }

    /// Reads column 0 from a single-row-group Parquet buffer, projecting it with the given schema.
    /// Passing a flat schema when the buffer was written from a REE array lets callers decode
    /// the physical values without the run-end encoding wrapper.
    fn read_column_with_schema(bytes: Bytes, schema: SchemaRef) -> ArrayRef {
        let opts = crate::arrow::arrow_reader::ArrowReaderOptions::new().with_schema(schema);
        ParquetRecordBatchReaderBuilder::try_new_with_options(bytes, opts)
            .unwrap()
            .build()
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .column(0)
            .clone()
    }

    fn ree_write_read_roundtrip(ree: ArrayRef, flat: ArrayRef) {
        let flat_schema = Arc::new(Schema::new(vec![Field::new(
            "col",
            flat.data_type().clone(),
            true,
        )]));
        let ree_bytes = write_column_to_bytes(ree);
        let flat_bytes = write_column_to_bytes(flat.clone());
        assert_eq!(
            ree_bytes, flat_bytes,
            "REE and flat bytes should be identical"
        );

        let decoded_ree = read_column_with_schema(ree_bytes, flat_schema.clone());
        let decoded_flat = read_column_with_schema(flat_bytes, flat_schema);

        assert_eq!(decoded_ree.as_ref(), flat.as_ref());
        assert_eq!(decoded_ree.as_ref(), decoded_flat.as_ref());
    }

    #[test]
    fn ree_string() {
        let ree: ArrayRef = Arc::new(
            [Some("a"), Some("a"), None, Some("b"), Some("b")]
                .into_iter()
                .collect::<Int32RunArray>(),
        );
        let flat: ArrayRef = Arc::new(StringArray::from(vec![
            Some("a"),
            Some("a"),
            None,
            Some("b"),
            Some("b"),
        ]));
        ree_write_read_roundtrip(ree, flat);
    }

    #[test]
    fn ree_int32() {
        let mut b = PrimitiveRunBuilder::<Int32Type, Int32Type>::new();
        for v in [Some(1), Some(1), None, Some(2), Some(2)] {
            b.append_option(v);
        }
        let ree: ArrayRef = Arc::new(b.finish());
        let flat: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(1),
            None,
            Some(2),
            Some(2),
        ]));
        ree_write_read_roundtrip(ree, flat);
    }

    #[test]
    fn ree_bool() {
        // run_ends [3, 5, 7] → [T,T,T, null,null, F,F]
        let ree: ArrayRef = Arc::new(
            RunArray::try_new(
                &Int32Array::from(vec![3, 5, 7]),
                &BooleanArray::from(vec![Some(true), None, Some(false)]),
            )
            .unwrap(),
        );
        let flat: ArrayRef = Arc::new(BooleanArray::from(vec![
            Some(true),
            Some(true),
            Some(true),
            None,
            None,
            Some(false),
            Some(false),
        ]));
        ree_write_read_roundtrip(ree, flat);
    }

    #[test]
    fn ree_fixed_size_binary() {
        let mk = |vals: &[Option<&[u8]>]| -> FixedSizeBinaryArray {
            let mut b = FixedSizeBinaryBuilder::new(2);
            for v in vals {
                match v {
                    Some(x) => b.append_value(x).unwrap(),
                    None => b.append_null(),
                }
            }
            b.finish()
        };
        // run_ends [2, 4, 6] → [aa,aa, null,null, bb,bb]
        let ree: ArrayRef = Arc::new(
            RunArray::try_new(
                &Int32Array::from(vec![2, 4, 6]),
                &mk(&[Some(b"aa"), None, Some(b"bb")]),
            )
            .unwrap(),
        );
        let flat: ArrayRef = Arc::new(mk(&[
            Some(b"aa"),
            Some(b"aa"),
            None,
            None,
            Some(b"bb"),
            Some(b"bb"),
        ]));
        ree_write_read_roundtrip(ree, flat);
    }

    #[test]
    fn ree_single_run() {
        let ree: ArrayRef = Arc::new(["x", "x", "x"].into_iter().collect::<Int32RunArray>());
        let flat: ArrayRef = Arc::new(StringArray::from(vec!["x", "x", "x"]));
        ree_write_read_roundtrip(ree, flat);
    }

    #[test]
    fn ree_float32() {
        // run_ends [2, 4, 5] → [1.0, 1.0, null, null, 2.5]
        let ree: ArrayRef = Arc::new(
            RunArray::try_new(
                &Int32Array::from(vec![2, 4, 5]),
                &Float32Array::from(vec![Some(1.0_f32), None, Some(2.5_f32)]),
            )
            .unwrap(),
        );
        let flat: ArrayRef = Arc::new(Float32Array::from(vec![
            Some(1.0_f32),
            Some(1.0_f32),
            None,
            None,
            Some(2.5_f32),
        ]));
        ree_write_read_roundtrip(ree, flat);
    }

    #[test]
    fn ree_sliced() {
        // A sliced (non-zero offset) REE array: verify that get_physical_index
        // correctly accounts for the logical offset when expanding.
        // Full array: run_ends [3, 5, 7] → [a,a,a, b,b, c,c]
        // After slice(2, 5) the logical view is [a, b, b, c, c].
        let full: ArrayRef = Arc::new(
            RunArray::try_new(
                &Int32Array::from(vec![3, 5, 7]),
                &StringArray::from(vec!["a", "b", "c"]),
            )
            .unwrap(),
        );
        let sliced = full.slice(2, 5);
        let flat: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "b", "c", "c"]));
        ree_write_read_roundtrip(sliced, flat);
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Takes too long
    fn test_number_distinct_values_exact_count() {
        // 50 distinct Int32 values repeated across 100k rows, with every 7th row null.
        // Nulls must not be counted as a distinct value.
        let cardinality = 50u32;
        let array: ArrayRef = Arc::new(Int32Array::from_iter((0..100_000u32).map(|i| {
            if i % 7 == 0 {
                None
            } else {
                Some((i % cardinality) as i32)
            }
        })));
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, true)]));
        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();

        let props = WriterProperties::builder()
            .set_write_row_group_number_distinct_values(true)
            .build();
        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        let metadata = writer.close().unwrap();

        let count = metadata
            .row_group(0)
            .column(0)
            .statistics()
            .and_then(|s| s.distinct_count_opt())
            .expect("distinct_count should be set");
        // Must equal cardinality exactly; nulls must not inflate the count.
        assert_eq!(count, cardinality as u64);
    }

    #[test]
    fn test_number_distinct_values_view_types() {
        // 5 distinct values repeated across 30 rows, with every 4th row null.
        // Verifies Utf8View is counted correctly (BinaryView shares the same code path).
        let cardinality = 5u32;
        let distinct_strings = ["alpha", "beta", "gamma", "delta", "epsilon"];

        let string_view_col: ArrayRef = Arc::new(StringViewArray::from_iter((0..30u32).map(|i| {
            if i % 4 == 0 {
                None
            } else {
                Some(distinct_strings[(i % cardinality) as usize])
            }
        })));

        let schema = Arc::new(Schema::new(vec![Field::new(
            "string_view_col",
            DataType::Utf8View,
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![string_view_col]).unwrap();

        let props = WriterProperties::builder()
            .set_write_row_group_number_distinct_values(true)
            .build();
        let mut parquet_bytes = Vec::new();
        let mut writer =
            ArrowWriter::try_new(&mut parquet_bytes, batch.schema(), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        let metadata = writer.close().unwrap();

        let distinct_count = metadata
            .row_group(0)
            .column(0)
            .statistics()
            .and_then(|s| s.distinct_count_opt())
            .expect("distinct_count should be set for Utf8View column");
        assert_eq!(distinct_count, cardinality as u64);
    }

    #[test]
    fn test_number_distinct_values_not_written_by_default() {
        let array: ArrayRef = Arc::new(Int32Array::from_iter_values(0..100));
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();

        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        let metadata = writer.close().unwrap();

        let count = metadata
            .row_group(0)
            .column(0)
            .statistics()
            .and_then(|s| s.distinct_count_opt());
        assert!(count.is_none());
    }

    #[test]
    fn test_dictionary_ndv_single_batch() {
        // Dictionary array with 3 distinct string values repeated many times.
        // NDV must equal the number of distinct values in the dictionary (3),
        // not the number of rows.
        let keys = Int32Array::from(vec![0, 1, 2, 0, 1, 2, 0, 1, 2]);
        let values: ArrayRef = Arc::new(StringArray::from(vec!["cat", "dog", "bird"]));
        let dict: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::try_new(keys, values).unwrap());

        let schema = Arc::new(Schema::new(vec![Field::new(
            "x",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![dict]).unwrap();

        let props = WriterProperties::builder()
            .set_write_row_group_number_distinct_values(true)
            .build();
        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        let metadata = writer.close().unwrap();

        let count = metadata
            .row_group(0)
            .column(0)
            .statistics()
            .and_then(|s| s.distinct_count_opt())
            .expect("distinct_count should be set");
        assert_eq!(count, 3);
    }

    #[test]
    fn test_dictionary_ndv_excludes_unreferenced_values() {
        // Keys only reference indices 0 and 1; value at index 2 ("unreferenced") should not
        // count toward NDV even though it appears in the dictionary's values array.
        let keys = Int32Array::from(vec![0, 1, 0, 1]);
        let values: ArrayRef = Arc::new(StringArray::from(vec!["cat", "dog", "unreferenced"]));
        let dict: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::try_new(keys, values).unwrap());

        let schema = Arc::new(Schema::new(vec![Field::new(
            "x",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![dict]).unwrap();

        let props = WriterProperties::builder()
            .set_write_row_group_number_distinct_values(true)
            .build();
        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        let metadata = writer.close().unwrap();

        let count = metadata
            .row_group(0)
            .column(0)
            .statistics()
            .and_then(|s| s.distinct_count_opt())
            .expect("distinct_count should be set");
        assert_eq!(
            count, 2,
            "unreferenced dictionary values must not count toward NDV"
        );
    }

    #[test]
    fn test_dictionary_ndv_across_batches_regression() {
        // Regression test for https://github.com/apache/arrow-rs/issues/11172.
        let make_dict_batch = |a: &str, b: &str| -> RecordBatch {
            let keys = Int32Array::from(vec![0, 1, 0, 1]);
            let values: ArrayRef = Arc::new(StringArray::from(vec![a, b]));
            let dict: ArrayRef =
                Arc::new(DictionaryArray::<Int32Type>::try_new(keys, values).unwrap());
            let schema = Arc::new(Schema::new(vec![Field::new(
                "x",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                false,
            )]));
            RecordBatch::try_new(schema, vec![dict]).unwrap()
        };

        // batch1: dict = ["cat", "dog"], batch2: dict = ["fish", "cat"]
        // Distinct values across both batches: "cat", "dog", "fish" NDV = 3
        let batch1 = make_dict_batch("cat", "dog");
        let batch2 = make_dict_batch("fish", "cat");

        let props = WriterProperties::builder()
            .set_write_row_group_number_distinct_values(true)
            .build();
        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, batch1.schema(), Some(props)).unwrap();
        writer.write(&batch1).unwrap();
        writer.write(&batch2).unwrap();
        let metadata = writer.close().unwrap();

        let count = metadata
            .row_group(0)
            .column(0)
            .statistics()
            .and_then(|s| s.distinct_count_opt())
            .expect("distinct_count should be set");
        assert_eq!(
            count, 3,
            "NDV should count distinct values, not distinct key indices"
        );
    }

    #[test]
    fn ree_struct_with_ree_child() {
        // Struct with a REE string field and a REE int field — confirms
        // recursion visits every child and each collapses to the right leaf type.
        let run_ends = Int32Array::from(vec![2i32, 3, 5]);

        let col_a: ArrayRef = Arc::new(
            RunArray::try_new(
                &run_ends,
                &StringArray::from(vec![Some("foo"), None, Some("bar")]),
            )
            .unwrap(),
        );
        let col_b: ArrayRef = Arc::new(
            RunArray::try_new(&run_ends, &Int32Array::from(vec![Some(1), None, Some(2)])).unwrap(),
        );

        let struct_array: ArrayRef = Arc::new(StructArray::new(
            Fields::from(vec![
                Field::new("a", col_a.data_type().clone(), true),
                Field::new("b", col_b.data_type().clone(), true),
            ]),
            vec![col_a, col_b],
            None,
        ));

        let schema = Arc::new(Schema::new(vec![Field::new(
            "row",
            struct_array.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![struct_array]).unwrap();

        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, schema, None).unwrap();
        writer.write(&batch).unwrap();
        let metadata = writer.close().unwrap();

        let parquet_schema = metadata.file_metadata().schema_descr();
        assert_eq!(parquet_schema.num_columns(), 2);
        assert_eq!(
            parquet_schema.column(0).physical_type(),
            crate::basic::Type::BYTE_ARRAY
        );
        assert_eq!(parquet_schema.column(0).path().string(), "row.a");
        assert_eq!(
            parquet_schema.column(1).physical_type(),
            crate::basic::Type::INT32
        );
        assert_eq!(parquet_schema.column(1).path().string(), "row.b");
    }

    #[test]
    fn test_fixed_size_binary_in_dict_cache_width_boundary() {
        for width in [32, 33, 64] {
            let keys = UInt8Array::from(vec![0, 0]);
            let values =
                FixedSizeBinaryArray::try_from_iter([vec![42; width]].into_iter()).unwrap();
            let data = DictionaryArray::<UInt8Type>::new(keys, Arc::new(values));
            let batch = RecordBatch::try_from_iter([("a", Arc::new(data) as ArrayRef)]).unwrap();

            roundtrip(batch.clone(), None);
            if width == 32 {
                for (dictionary, statistics) in [
                    (true, EnabledStatistics::None),
                    (false, EnabledStatistics::Chunk),
                    (false, EnabledStatistics::None),
                ] {
                    let props = WriterProperties::builder()
                        .set_dictionary_enabled(dictionary)
                        .set_statistics_enabled(statistics)
                        .build();
                    roundtrip_opts(&batch, props);
                }
            }
        }

        let keys = UInt8Array::from_iter_values((0..130).map(|index| (index % 65) as u8));
        let values = FixedSizeBinaryArray::try_from_iter(
            (0..65_u32).map(|value| value.to_be_bytes().to_vec()),
        )
        .unwrap();
        let batch = RecordBatch::try_from_iter([(
            "a",
            Arc::new(DictionaryArray::<UInt8Type>::new(keys, Arc::new(values))) as ArrayRef,
        )])
        .unwrap();
        let props = WriterProperties::builder()
            .set_dictionary_enabled(false)
            .set_bloom_filter_enabled(true)
            .build();
        roundtrip_opts(&batch, props);
    }

    #[test]
    fn arrow_writer_dictionary_of_view_roundtrip() {
        fn check(flat_type: DataType, nullable: bool, dict: ArrayRef, expected: ArrayRef) {
            let writer_schema = Arc::new(Schema::new(vec![Field::new(
                "d",
                flat_type.clone(),
                nullable,
            )]));
            let batch_schema = Arc::new(Schema::new(vec![Field::new(
                "d",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(flat_type)),
                nullable,
            )]));
            let batch = RecordBatch::try_new(batch_schema, vec![dict]).unwrap();

            let mut file = vec![];
            let mut writer = ArrowWriter::try_new(&mut file, writer_schema.clone(), None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();

            let mut reader = ParquetRecordBatchReader::try_new(Bytes::from(file), 1024).unwrap();
            let actual = reader.next().unwrap().unwrap();
            let expected = RecordBatch::try_new(writer_schema, vec![expected]).unwrap();
            assert_eq!(actual, expected);
        }

        // A value longer than 12 bytes forces the view array's out-of-line buffer.
        let long = "a longer payload that exceeds twelve bytes";

        // Required Utf8View dictionary with repeated keys.
        check(
            DataType::Utf8View,
            false,
            Arc::new(DictionaryArray::<Int32Type>::new(
                Int32Array::from(vec![0, 1, 0, 2, 1]),
                Arc::new(StringViewArray::from(vec!["alpha", long, "beta"])),
            )),
            Arc::new(StringViewArray::from(vec![
                "alpha", long, "alpha", "beta", long,
            ])),
        );

        // Required BinaryView dictionary with an out-of-line value.
        check(
            DataType::BinaryView,
            false,
            Arc::new(DictionaryArray::<Int32Type>::new(
                Int32Array::from(vec![1, 0, 1]),
                Arc::new(BinaryViewArray::from_iter_values(vec![
                    b"x".as_slice(),
                    long.as_bytes(),
                ])),
            )),
            Arc::new(BinaryViewArray::from_iter_values(vec![
                long.as_bytes(),
                b"x".as_slice(),
                long.as_bytes(),
            ])),
        );

        // Nullable Utf8View dictionary with a null key.
        check(
            DataType::Utf8View,
            true,
            Arc::new(DictionaryArray::<Int32Type>::new(
                Int32Array::new(
                    vec![0, 1, 2, 0].into(),
                    Some(NullBuffer::from(vec![true, false, true, true])),
                ),
                Arc::new(StringViewArray::from(vec!["x", "unused", long])),
            )),
            Arc::new(StringViewArray::from(vec![
                Some("x"),
                None,
                Some(long),
                Some("x"),
            ])),
        );
    }

    fn check_required_dict_null_value(values: ArrayRef) {
        let value_type = values.data_type().clone();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "d",
            value_type.clone(),
            false,
        )]));
        let batch_schema = Arc::new(Schema::new(vec![Field::new(
            "d",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(value_type)),
            true,
        )]));
        let keys = Int32Array::from(vec![0, 1, 2]);
        let array = DictionaryArray::<Int32Type>::new(keys, values);
        let batch = RecordBatch::try_new(batch_schema, vec![Arc::new(array)]).unwrap();

        let mut writer = ArrowWriter::try_new(Vec::new(), schema, None).unwrap();
        let err = writer.write(&batch).unwrap_err();
        assert!(err.to_string().contains("Found null"), "{err}");
    }

    #[test]
    fn arrow_writer_rejects_null_dictionary_value_for_required_column() {
        check_required_dict_null_value(Arc::new(Int32Array::from(vec![Some(10), None, Some(20)])));

        let mut fixed = FixedSizeBinaryBuilder::new(2);
        fixed.append_value([1, 2]).unwrap();
        fixed.append_null();
        fixed.append_value([3, 4]).unwrap();
        check_required_dict_null_value(Arc::new(fixed.finish()));
    }

    #[test]
    fn arrow_writer_rejects_null_dictionary_key_for_required_column() {
        let values: ArrayRef = Arc::new(Int32Array::from(vec![10, 20]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "dictionary",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Int32)),
                true,
            )])),
            vec![Arc::new(DictionaryArray::<Int32Type>::new(
                Int32Array::new(
                    vec![0, 99, 1].into(),
                    Some(NullBuffer::from(vec![true, false, true])),
                ),
                values,
            ))],
        )
        .unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "dictionary",
            DataType::Int32,
            false,
        )]));
        let mut writer = ArrowWriter::try_new(Vec::new(), schema, None).unwrap();
        let err = writer.write(&batch).unwrap_err();
        assert!(err.to_string().contains("Found null"), "{err}");
    }

    #[test]
    fn arrow_writer_low_cardinality_binary_dictionary() {
        let dict_vals: Vec<&[u8]> = vec![b"alpha".as_ref(), b"beta", b"gamma", b"delta"];
        let keys = Int32Array::from_iter_values((0..64).map(|i| i % 4));
        let bin = DictionaryArray::<Int32Type>::new(
            keys.clone(),
            Arc::new(BinaryArray::from_iter_values(dict_vals.clone())),
        );
        RoundTripTest::new(Arc::new(bin)).run();
        let lbin = DictionaryArray::<Int32Type>::new(
            keys,
            Arc::new(LargeBinaryArray::from_iter_values(dict_vals)),
        );
        RoundTripTest::new(Arc::new(lbin)).run();
    }

    #[test]
    fn arrow_writer_low_cardinality_dictionary_with_bloom_filter() {
        let keys = Int32Array::from_iter_values((0..64).map(|i| i % 4));
        let values = StringArray::from(vec!["alpha", "beta", "gamma", "delta"]);
        let dict = DictionaryArray::<Int32Type>::new(keys, Arc::new(values));
        RoundTripTest::new(Arc::new(dict))
            .with_bloom_filter(true)
            .run();
    }

    #[test]
    fn arrow_writer_non_byte_dictionary_physical_types() {
        fn roundtrip_with_native_schema(
            values: ArrayRef,
            native_type: DataType,
            expected: ArrayRef,
        ) {
            let writer_schema = Arc::new(Schema::new(vec![Field::new("col", native_type, true)]));
            let batch_schema = Arc::new(Schema::new(vec![Field::new(
                "col",
                values.data_type().clone(),
                true,
            )]));
            let batch = RecordBatch::try_new(batch_schema, vec![values]).unwrap();

            let mut file = vec![];
            let mut writer = ArrowWriter::try_new(&mut file, writer_schema.clone(), None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();

            let mut reader = ParquetRecordBatchReader::try_new(Bytes::from(file), 1024).unwrap();
            let actual = reader.next().unwrap().unwrap();
            let expected = RecordBatch::try_new(writer_schema, vec![expected]).unwrap();
            assert_eq!(actual, expected);
        }

        let keys = UInt8Array::from(vec![Some(0), Some(1), None, Some(0), Some(1)]);

        let bool_values = BooleanArray::from(vec![Some(true), Some(false), None]);
        let array = DictionaryArray::new(
            UInt8Array::from(vec![Some(0), Some(1), None, Some(2), Some(0)]),
            Arc::new(bool_values),
        );
        roundtrip_with_native_schema(
            Arc::new(array),
            DataType::Boolean,
            Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(false),
                None,
                None,
                Some(true),
            ])),
        );

        let float_values = Float32Array::from(vec![1.25, -2.5]);
        let array = DictionaryArray::new(keys.clone(), Arc::new(float_values));
        roundtrip_with_native_schema(
            Arc::new(array),
            DataType::Float32,
            Arc::new(Float32Array::from(vec![
                Some(1.25),
                Some(-2.5),
                None,
                Some(1.25),
                Some(-2.5),
            ])),
        );

        let double_values = Float64Array::from(vec![1.25, -2.5]);
        let array = DictionaryArray::new(keys.clone(), Arc::new(double_values));
        roundtrip_with_native_schema(
            Arc::new(array),
            DataType::Float64,
            Arc::new(Float64Array::from(vec![
                Some(1.25),
                Some(-2.5),
                None,
                Some(1.25),
                Some(-2.5),
            ])),
        );

        let int64_values = Int64Array::from(vec![1234567890123, -987654321098]);
        let array = DictionaryArray::new(keys, Arc::new(int64_values));
        roundtrip_with_native_schema(
            Arc::new(array),
            DataType::Int64,
            Arc::new(Int64Array::from(vec![
                Some(1234567890123),
                Some(-987654321098),
                None,
                Some(1234567890123),
                Some(-987654321098),
            ])),
        );

        let keys = UInt8Array::from(vec![Some(0), None, Some(1), Some(2), Some(1)]);
        let decimal = Decimal128Array::from(vec![12345, 56789, 34567])
            .with_precision_and_scale(30, 2)
            .unwrap();
        roundtrip_with_native_schema(
            Arc::new(DictionaryArray::new(keys.clone(), Arc::new(decimal))),
            DataType::Decimal128(30, 2),
            Arc::new(
                Decimal128Array::from(vec![
                    Some(12345),
                    None,
                    Some(56789),
                    Some(34567),
                    Some(56789),
                ])
                .with_precision_and_scale(30, 2)
                .unwrap(),
            ),
        );

        let values = [1.25, -2.5, 4.0].map(f16::from_f32);
        roundtrip_with_native_schema(
            Arc::new(DictionaryArray::new(
                keys,
                Arc::new(Float16Array::from(values.to_vec())),
            )),
            DataType::Float16,
            Arc::new(Float16Array::from(vec![
                Some(values[0]),
                None,
                Some(values[1]),
                Some(values[2]),
                Some(values[1]),
            ])),
        );
    }

    #[test]
    fn arrow_writer_primitive_dictionary_with_cdc() {
        #[expect(deprecated)]
        let schema = Arc::new(Schema::new(vec![Field::new_dict(
            "dictionary",
            DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::UInt32)),
            true,
            42,
            true,
        )]));

        let keys = UInt8Array::from(
            (0..1024)
                .map(|i| {
                    if i % 11 == 0 {
                        None
                    } else {
                        Some((i % 4) as u8)
                    }
                })
                .collect::<Vec<_>>(),
        );
        let values = UInt32Array::from(vec![12345678, 22345678, 32345678, 42345678]);
        let array = Arc::new(DictionaryArray::new(keys, Arc::new(values)));
        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();

        let props = WriterProperties::builder()
            .set_write_batch_size(64)
            .set_content_defined_chunking(Some(CdcOptions {
                min_chunk_size: 64,
                max_chunk_size: 256,
                norm_level: 0,
            }))
            .build();

        roundtrip_opts(&batch, props);
    }

    #[test]
    fn arrow_writer_byte_dictionary_per_page_stats() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "a",
            DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8)),
            false,
        )]));

        let values = StringArray::from(vec!["a", "m", "z"]);
        let keys = UInt8Array::from(vec![0, 1, 0, 1, 0, 1, 0, 1, 1, 2, 1, 2, 1, 2, 1, 2]);
        let dict = DictionaryArray::new(keys, Arc::new(values));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(dict)]).unwrap();

        let props = WriterProperties::builder()
            .set_write_batch_size(8)
            .set_data_page_row_count_limit(8)
            .set_statistics_enabled(EnabledStatistics::Page)
            .build();

        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let options = ReadOptionsBuilder::new().with_page_index().build();
        let reader = SerializedFileReader::new_with_options(Bytes::from(buf), options).unwrap();
        let column_index = reader
            .metadata()
            .page_index()
            .unwrap()
            .column_index(0, 0)
            .unwrap();
        let ColumnIndexMetaData::BYTE_ARRAY(idx) = column_index else {
            panic!("expected BYTE_ARRAY column index, got {column_index:?}");
        };

        assert_eq!(idx.min_values_iter().count(), 2, "expected two data pages");
        assert_eq!(idx.min_value(0), Some(b"a".as_slice()));
        assert_eq!(idx.max_value(0), Some(b"m".as_slice()));
        assert_eq!(idx.min_value(1), Some(b"m".as_slice()));
        assert_eq!(idx.max_value(1), Some(b"z".as_slice()));
    }
    #[test]
    fn dense_dictionary_source_changes_preserve_cache_and_pages() {
        let sources: Vec<(ArrayRef, ArrayRef)> = vec![
            (
                Arc::new(StringArray::from(vec!["alpha", "beta"])),
                Arc::new(StringArray::from(vec!["changed", "different"])),
            ),
            (
                Arc::new(Int32Array::from(vec![11, 22])),
                Arc::new(Int32Array::from(vec![33, 44])),
            ),
            (
                Arc::new(Float32Array::from(vec![1.25, -2.5])),
                Arc::new(Float32Array::from(vec![3.5, -4.75])),
            ),
            (
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter([b"abcd", b"efgh"].into_iter()).unwrap(),
                ),
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter([b"ijkl", b"mnop"].into_iter()).unwrap(),
                ),
            ),
        ];
        for (first, second) in sources {
            for cdc in [false, true] {
                for budget in [32, 1024] {
                    let schema = Arc::new(Schema::new(vec![Field::new(
                        "a",
                        first.data_type().clone(),
                        true,
                    )]));
                    let keys =
                        Int32Array::from_iter((0..90).map(|i| (i % 11 != 0).then_some(i % 2)));
                    let dict1: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::new(
                        keys.clone(),
                        first.clone(),
                    ));
                    let dict2: ArrayRef =
                        Arc::new(DictionaryArray::<Int32Type>::new(keys, second.clone()));
                    let dict1 = dict1.slice(1, 65);
                    let dict2 = dict2.slice(2, 66);
                    let dense = arrow_cast::cast(dict1.as_ref(), first.data_type()).unwrap();
                    let mut props = WriterProperties::builder()
                        .set_write_batch_size(7)
                        .set_data_page_row_count_limit(16)
                        .set_dictionary_page_size_limit(budget)
                        .set_bloom_filter_enabled(true)
                        .set_statistics_enabled(EnabledStatistics::Page);
                    if cdc {
                        props = props.set_content_defined_chunking(Some(CdcOptions {
                            min_chunk_size: 32,
                            max_chunk_size: 128,
                            norm_level: 0,
                        }));
                    }
                    let mut writer =
                        ArrowWriter::try_new(Vec::new(), schema.clone(), Some(props.build()))
                            .unwrap();
                    let mut expected = Vec::new();
                    for array in [dict1, dense, dict2] {
                        expected.push(arrow_cast::cast(array.as_ref(), first.data_type()).unwrap());
                        let batch = RecordBatch::try_from_iter([("a", array)]).unwrap();
                        writer.write(&batch).unwrap();
                    }
                    let bytes = Bytes::from(writer.into_inner().unwrap());
                    let parquet = SerializedFileReader::new(bytes.clone()).unwrap();
                    assert_eq!(
                        parquet.metadata().row_groups()[0].column(0).num_values(),
                        196
                    );
                    let actual = ParquetRecordBatchReaderBuilder::try_new(bytes)
                        .unwrap()
                        .with_batch_size(1024)
                        .build()
                        .unwrap()
                        .next()
                        .unwrap()
                        .unwrap();
                    let expected = arrow_select::concat::concat(
                        &expected.iter().map(|a| a.as_ref()).collect::<Vec<_>>(),
                    )
                    .unwrap();
                    assert_eq!(actual.column(0).as_ref(), expected.as_ref());
                }
            }
        }
    }

    #[test]
    fn test_fixed_size_binary_zero_width_write() {
        let mut builder = FixedSizeBinaryBuilder::new(0);
        builder.append_value(b"").unwrap();
        builder.append_value(b"").unwrap();
        builder.append_value(b"").unwrap();

        let array: ArrayRef = Arc::new(builder.finish());
        let schema = Arc::new(Schema::new(vec![Field::new(
            "a",
            DataType::FixedSizeBinary(0),
            false,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();

        for props in [
            WriterProperties::builder()
                .set_writer_version(WriterVersion::PARQUET_1_0)
                .build(),
            WriterProperties::builder()
                .set_writer_version(WriterVersion::PARQUET_1_0)
                .set_dictionary_enabled(false)
                .set_encoding(Encoding::BYTE_STREAM_SPLIT)
                .build(),
        ] {
            let file = roundtrip_opts(&batch, props);
            let parquet = SerializedFileReader::new(file).unwrap();
            let metadata = parquet.metadata();

            assert_eq!(metadata.file_metadata().num_rows(), 3);
            assert_eq!(metadata.row_groups()[0].column(0).num_values(), 3);
        }
    }

    #[test]
    fn all_null_bool_rle_single_column() {
        let values = Arc::new(BooleanArray::from(vec![None; SMALL_SIZE]));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col",
            DataType::Boolean,
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![values]).unwrap();
        let props = WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .set_encoding(Encoding::RLE)
            .build();

        roundtrip_opts(&batch, props);
    }

    #[test]
    fn arrow_writer_decimal32_decimal64_plain_column() {
        let d32 = Decimal32Array::from(vec![Some(12345), Some(56789), Some(34567)])
            .with_precision_and_scale(9, 2)
            .unwrap();
        RoundTripTest::new(Arc::new(d32)).with_nullable(false).run();
        let d32n = Decimal32Array::from(vec![Some(12345), None, Some(34567)])
            .with_precision_and_scale(9, 2)
            .unwrap();
        RoundTripTest::new(Arc::new(d32n)).run();
        let d64 = Decimal64Array::from(vec![Some(12345i64), Some(56789), Some(34567)])
            .with_precision_and_scale(12, 2)
            .unwrap();
        RoundTripTest::new(Arc::new(d64)).with_nullable(false).run();
        let d64n = Decimal64Array::from(vec![Some(12345i64), None, Some(34567)])
            .with_precision_and_scale(12, 2)
            .unwrap();
        RoundTripTest::new(Arc::new(d64n)).run();
    }

    #[test]
    fn string_view_fallback_observes_across_gathered_tiles() {
        let mut raw_values = (0..130)
            .map(|i| format!("middle-{i:03}"))
            .collect::<Vec<_>>();
        raw_values[63] = "aaa-min".to_string();
        raw_values[64] = "zzz-max".to_string();

        let values: ArrayRef = Arc::new(StringViewArray::from_iter_values(
            raw_values.iter().map(String::as_str),
        ));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col",
            values.data_type().clone(),
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![values]).unwrap();

        for encoding in [
            Encoding::PLAIN,
            Encoding::DELTA_LENGTH_BYTE_ARRAY,
            Encoding::DELTA_BYTE_ARRAY,
        ] {
            let props = WriterProperties::builder()
                .set_writer_version(WriterVersion::PARQUET_2_0)
                .set_dictionary_enabled(false)
                .set_encoding(encoding)
                .set_statistics_enabled(EnabledStatistics::Chunk)
                .set_bloom_filter_enabled(true)
                .build();
            let file = roundtrip_opts(&batch, props);
            let reader = SerializedFileReader::new(file.clone()).unwrap();
            let column = reader.metadata().row_group(0).column(0);
            let Statistics::ByteArray(stats) = column.statistics().unwrap() else {
                panic!("expected byte-array statistics for {encoding:?}");
            };
            assert_eq!(stats.min_opt().unwrap().as_bytes(), b"aaa-min");
            assert_eq!(stats.max_opt().unwrap().as_bytes(), b"zzz-max");

            check_bloom_filter(
                vec![file],
                "col".to_string(),
                vec!["middle-000", "aaa-min", "zzz-max", "middle-129"],
                Vec::<&str>::new(),
            );
        }
    }

    #[test]
    fn bound_physical_sources_slice_the_selection_without_retaining_arrays() {
        let values: ArrayRef = Arc::new(Int32Array::from(vec![10, 20, 30]));
        let keys = Int32Array::from(vec![2, 0, 1, 2]);
        let dictionary: ArrayRef =
            Arc::new(DictionaryArray::<Int32Type>::try_new(keys, values.clone()).unwrap());
        let value_refs = Arc::strong_count(&values);
        let dictionary_refs = Arc::strong_count(&dictionary);

        {
            let binding = ArrowPhysicalBinding::<Int32Storage<'_>>::bind(
                dictionary.as_ref(),
                ValueSelectionRef::Dense { offset: 0, len: 4 },
            )
            .unwrap();
            let source = binding.source();
            assert_eq!(Arc::strong_count(&values), value_refs);
            assert_eq!(Arc::strong_count(&dictionary), dictionary_refs);
            assert_eq!(source.len(), 4);
            assert!(matches!(binding.storage, Int32Storage::Identity(_)));

            let sliced = source.slice(1, 2);
            let mut physical = Vec::new();
            sliced
                .selection()
                .try_for_each_index(|index| -> Result<()> {
                    physical.push(index);
                    Ok(())
                })
                .unwrap();
            assert_eq!(physical, [0, 1]);
        }
        assert_eq!(Arc::strong_count(&values), value_refs);
        assert_eq!(Arc::strong_count(&dictionary), dictionary_refs);

        let byte_values: ArrayRef = Arc::new(StringArray::from(vec!["a", "bb", "ccc"]));
        let byte_keys = Int32Array::from(vec![2, 0, 1, 2]);
        let byte_dictionary: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(byte_keys, byte_values.clone()).unwrap(),
        );
        let byte_value_refs = Arc::strong_count(&byte_values);
        let byte_dictionary_refs = Arc::strong_count(&byte_dictionary);

        {
            let _binding = ArrowPhysicalBinding::<ByteArrayStorage<'_>>::bind(
                byte_dictionary.as_ref(),
                ValueSelectionRef::Dense { offset: 0, len: 4 },
            )
            .unwrap();
            assert_eq!(Arc::strong_count(&byte_values), byte_value_refs);
            assert_eq!(Arc::strong_count(&byte_dictionary), byte_dictionary_refs);
        }
        assert_eq!(Arc::strong_count(&byte_values), byte_value_refs);
        assert_eq!(Arc::strong_count(&byte_dictionary), byte_dictionary_refs);
    }

    #[test]
    fn arrow_writer_caps_page_size_for_fixed_len_decimal_inputs() {
        const ROWS: usize = 4000;
        let precision = 30u8;
        assert_eq!(decimal_length_from_precision(precision), 13);

        let values = Arc::new(
            Decimal128Array::from((0..ROWS as i128).collect::<Vec<_>>())
                .with_precision_and_scale(precision, 2)
                .unwrap(),
        );
        let writer_schema = Arc::new(Schema::new(vec![Field::new(
            "d",
            values.data_type().clone(),
            false,
        )]));
        let dense = RecordBatch::try_new(writer_schema.clone(), vec![values.clone()]).unwrap();
        let batch_schema = Arc::new(Schema::new(vec![Field::new(
            "d",
            DataType::Dictionary(
                Box::new(DataType::Int32),
                Box::new(DataType::Decimal128(precision, 2)),
            ),
            false,
        )]));
        let dictionary = RecordBatch::try_new(
            batch_schema,
            vec![Arc::new(DictionaryArray::<Int32Type>::new(
                Int32Array::from_iter_values(0..ROWS as i32),
                values,
            ))],
        )
        .unwrap();

        let data_page_size_limit = 4096;
        for (kind, batch) in [("dense", dense), ("dictionary", dictionary)] {
            let props = WriterProperties::builder()
                .set_dictionary_enabled(false)
                .set_data_page_size_limit(data_page_size_limit)
                .set_data_page_row_count_limit(ROWS + 1)
                .set_write_batch_size(ROWS)
                .build();
            let pages = data_page_count(writer_schema.clone(), &batch, props);
            assert!(
                pages > 1,
                "expected the {data_page_size_limit}-byte page budget to split the \
                 13-byte FLBA {kind} decimal column, got {pages} page(s)",
            );
        }
    }

    fn data_page_count(schema: SchemaRef, batch: &RecordBatch, props: WriterProperties) -> usize {
        let file = tempfile::tempfile().unwrap();
        let mut writer =
            ArrowWriter::try_new(file.try_clone().unwrap(), schema, Some(props)).unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();

        let options = ReadOptionsBuilder::new().with_page_index().build();
        let reader = SerializedFileReader::new_with_options(file, options).unwrap();
        reader
            .metadata()
            .page_index()
            .unwrap()
            .offset_index(0, 0)
            .expect("offset index")
            .page_locations()
            .len()
    }
}
