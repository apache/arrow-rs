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

//! Contains reader which reads parquet data into arrow [`RecordBatch`]

use std::collections::VecDeque;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::Array;
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, DataType as ArrowType, Schema, SchemaRef};
use arrow_select::filter::prep_null_mask_filter;
pub use filter::{ArrowPredicate, ArrowPredicateFn, RowFilter};
pub use selection::{RowSelection, RowSelector};

pub use crate::arrow::array_reader::RowGroups;
use crate::arrow::array_reader::{build_array_reader, ArrayReader};
use crate::arrow::schema::{parquet_to_arrow_schema_and_fields, ParquetField};
use crate::arrow::{parquet_to_arrow_field_levels, FieldLevels, ProjectionMask};
use crate::column::page::{PageIterator, PageReader};
use crate::errors::{ParquetError, Result};
use crate::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use crate::file::reader::{ChunkReader, SerializedPageReader};
use crate::schema::types::SchemaDescriptor;

mod filter;
mod selection;
pub mod statistics;

/// Builder for constructing parquet readers into arrow.
///
/// Most users should use one of the following specializations:
///
/// * synchronous API: [`ParquetRecordBatchReaderBuilder::try_new`]
/// * `async` API: [`ParquetRecordBatchStreamBuilder::new`]
///
/// [`ParquetRecordBatchStreamBuilder::new`]: crate::arrow::async_reader::ParquetRecordBatchStreamBuilder::new
pub struct ArrowReaderBuilder<T> {
    pub(crate) input: T,

    pub(crate) metadata: Arc<ParquetMetaData>,

    pub(crate) schema: SchemaRef,

    pub(crate) fields: Option<Arc<ParquetField>>,

    pub(crate) batch_size: usize,

    pub(crate) row_groups: Option<Vec<usize>>,

    pub(crate) projection: ProjectionMask,

    pub(crate) filter: Option<RowFilter>,

    pub(crate) selection: Option<RowSelection>,

    pub(crate) limit: Option<usize>,

    pub(crate) offset: Option<usize>,
}

impl<T> ArrowReaderBuilder<T> {
    pub(crate) fn new_builder(input: T, metadata: ArrowReaderMetadata) -> Self {
        Self {
            input,
            metadata: metadata.metadata,
            schema: metadata.schema,
            fields: metadata.fields,
            batch_size: 1024,
            row_groups: None,
            projection: ProjectionMask::all(),
            filter: None,
            selection: None,
            limit: None,
            offset: None,
        }
    }

    /// Returns a reference to the [`ParquetMetaData`] for this parquet file
    pub fn metadata(&self) -> &Arc<ParquetMetaData> {
        &self.metadata
    }

    /// Returns the parquet [`SchemaDescriptor`] for this parquet file
    pub fn parquet_schema(&self) -> &SchemaDescriptor {
        self.metadata.file_metadata().schema_descr()
    }

    /// Returns the arrow [`SchemaRef`] for this parquet file
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Set the size of [`RecordBatch`] to produce. Defaults to 1024
    /// If the batch_size more than the file row count, use the file row count.
    pub fn with_batch_size(self, batch_size: usize) -> Self {
        // Try to avoid allocate large buffer
        let batch_size = batch_size.min(self.metadata.file_metadata().num_rows() as usize);
        Self { batch_size, ..self }
    }

    /// Only read data from the provided row group indexes
    ///
    /// This is also called row group filtering
    pub fn with_row_groups(self, row_groups: Vec<usize>) -> Self {
        Self {
            row_groups: Some(row_groups),
            ..self
        }
    }

    /// Only read data from the provided column indexes
    pub fn with_projection(self, mask: ProjectionMask) -> Self {
        Self {
            projection: mask,
            ..self
        }
    }

    /// Provide a [`RowSelection`] to filter out rows, and avoid fetching their
    /// data into memory.
    ///
    /// This feature is used to restrict which rows are decoded within row
    /// groups, skipping ranges of rows that are not needed. Such selections
    /// could be determined by evaluating predicates against the parquet page
    /// [`Index`] or some other external information available to a query
    /// engine.
    ///
    /// # Notes
    ///
    /// Row group filtering (see [`Self::with_row_groups`]) is applied prior to
    /// applying the row selection, and therefore rows from skipped row groups
    /// should not be included in the [`RowSelection`] (see example below)
    ///
    /// It is recommended to enable writing the page index if using this
    /// functionality, to allow more efficient skipping over data pages. See
    /// [`ArrowReaderOptions::with_page_index`].
    ///
    /// # Example
    ///
    /// Given a parquet file with 4 row groups, and a row group filter of `[0,
    /// 2, 3]`, in order to scan rows 50-100 in row group 2 and rows 200-300 in
    /// row group 3:
    ///
    /// ```text
    ///   Row Group 0, 1000 rows (selected)
    ///   Row Group 1, 1000 rows (skipped)
    ///   Row Group 2, 1000 rows (selected, but want to only scan rows 50-100)
    ///   Row Group 3, 1000 rows (selected, but want to only scan rows 200-300)
    /// ```
    ///
    /// You could pass the following [`RowSelection`]:
    ///
    /// ```text
    ///  Select 1000    (scan all rows in row group 0)
    ///  Skip 50        (skip the first 50 rows in row group 2)
    ///  Select 50      (scan rows 50-100 in row group 2)
    ///  Skip 900       (skip the remaining rows in row group 2)
    ///  Skip 200       (skip the first 200 rows in row group 3)
    ///  Select 100     (scan rows 200-300 in row group 3)
    ///  Skip 700       (skip the remaining rows in row group 3)
    /// ```
    /// Note there is no entry for the (entirely) skipped row group 1.
    ///
    /// Note you can represent the same selection with fewer entries. Instead of
    ///
    /// ```text
    ///  Skip 900       (skip the remaining rows in row group 2)
    ///  Skip 200       (skip the first 200 rows in row group 3)
    /// ```
    ///
    /// you could use
    ///
    /// ```text
    /// Skip 1100      (skip the remaining 900 rows in row group 2 and the first 200 rows in row group 3)
    /// ```
    ///
    /// [`Index`]: crate::file::page_index::index::Index
    pub fn with_row_selection(self, selection: RowSelection) -> Self {
        Self {
            selection: Some(selection),
            ..self
        }
    }

    /// Provide a [`RowFilter`] to skip decoding rows
    ///
    /// Row filters are applied after row group selection and row selection
    ///
    /// It is recommended to enable reading the page index if using this functionality, to allow
    /// more efficient skipping over data pages. See [`ArrowReaderOptions::with_page_index`].
    pub fn with_row_filter(self, filter: RowFilter) -> Self {
        Self {
            filter: Some(filter),
            ..self
        }
    }

    /// Provide a limit to the number of rows to be read
    ///
    /// The limit will be applied after any [`Self::with_row_selection`] and [`Self::with_row_filter`]
    /// allowing it to limit the final set of rows decoded after any pushed down predicates
    ///
    /// It is recommended to enable reading the page index if using this functionality, to allow
    /// more efficient skipping over data pages. See [`ArrowReaderOptions::with_page_index`]
    pub fn with_limit(self, limit: usize) -> Self {
        Self {
            limit: Some(limit),
            ..self
        }
    }

    /// Provide an offset to skip over the given number of rows
    ///
    /// The offset will be applied after any [`Self::with_row_selection`] and [`Self::with_row_filter`]
    /// allowing it to skip rows after any pushed down predicates
    ///
    /// It is recommended to enable reading the page index if using this functionality, to allow
    /// more efficient skipping over data pages. See [`ArrowReaderOptions::with_page_index`]
    pub fn with_offset(self, offset: usize) -> Self {
        Self {
            offset: Some(offset),
            ..self
        }
    }
}

/// Options that control how metadata is read for a parquet file
///
/// See [`ArrowReaderBuilder`] for how to configure how the column data
/// is then read from the file, including projection and filter pushdown
#[derive(Debug, Clone, Default)]
pub struct ArrowReaderOptions {
    /// Should the reader strip any user defined metadata from the Arrow schema
    skip_arrow_metadata: bool,
    /// If provided used as the schema for the file, otherwise the schema is read from the file
    supplied_schema: Option<SchemaRef>,
    /// If true, attempt to read `OffsetIndex` and `ColumnIndex`
    pub(crate) page_index: bool,
}

impl ArrowReaderOptions {
    /// Create a new [`ArrowReaderOptions`] with the default settings
    pub fn new() -> Self {
        Self::default()
    }

    /// Skip decoding the embedded arrow metadata (defaults to `false`)
    ///
    /// Parquet files generated by some writers may contain embedded arrow
    /// schema and metadata.
    /// This may not be correct or compatible with your system,
    /// for example: [ARROW-16184](https://issues.apache.org/jira/browse/ARROW-16184)
    pub fn with_skip_arrow_metadata(self, skip_arrow_metadata: bool) -> Self {
        Self {
            skip_arrow_metadata,
            ..self
        }
    }

    /// Provide a schema to use when reading the parquet file. If provided it
    /// takes precedence over the schema inferred from the file or the schema defined
    /// in the file's metadata. If the schema is not compatible with the file's
    /// schema an error will be returned when constructing the builder.
    ///
    /// This option is only required if you want to cast columns to a different type.
    /// For example, if you wanted to cast from an Int64 in the Parquet file to a Timestamp
    /// in the Arrow schema.
    ///
    /// The supplied schema must have the same number of columns as the parquet schema and
    /// the column names need to be the same.
    ///
    /// # Example
    /// ```
    /// use std::io::Bytes;
    /// use std::sync::Arc;
    /// use tempfile::tempfile;
    /// use arrow_array::{ArrayRef, Int32Array, RecordBatch};
    /// use arrow_schema::{DataType, Field, Schema, TimeUnit};
    /// use parquet::arrow::arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
    /// use parquet::arrow::ArrowWriter;
    ///
    /// // Write data - schema is inferred from the data to be Int32
    /// let file = tempfile().unwrap();
    /// let batch = RecordBatch::try_from_iter(vec![
    ///     ("col_1", Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef),
    /// ]).unwrap();
    /// let mut writer = ArrowWriter::try_new(file.try_clone().unwrap(), batch.schema(), None).unwrap();
    /// writer.write(&batch).unwrap();
    /// writer.close().unwrap();
    ///
    /// // Read the file back.
    /// // Supply a schema that interprets the Int32 column as a Timestamp.
    /// let supplied_schema = Arc::new(Schema::new(vec![
    ///     Field::new("col_1", DataType::Timestamp(TimeUnit::Nanosecond, None), false)
    /// ]));
    /// let options = ArrowReaderOptions::new().with_schema(supplied_schema.clone());
    /// let mut builder = ParquetRecordBatchReaderBuilder::try_new_with_options(
    ///     file.try_clone().unwrap(),
    ///     options
    /// ).expect("Error if the schema is not compatible with the parquet file schema.");
    ///
    /// // Create the reader and read the data using the supplied schema.
    /// let mut reader = builder.build().unwrap();
    /// let _batch = reader.next().unwrap().unwrap();
    /// ```
    pub fn with_schema(self, schema: SchemaRef) -> Self {
        Self {
            supplied_schema: Some(schema),
            skip_arrow_metadata: true,
            ..self
        }
    }

    /// Enable reading [`PageIndex`], if present (defaults to `false`)
    ///
    /// The `PageIndex` can be used to push down predicates to the parquet scan,
    /// potentially eliminating unnecessary IO, by some query engines.
    ///
    /// If this is enabled, [`ParquetMetaData::column_index`] and
    /// [`ParquetMetaData::offset_index`] will be populated if the corresponding
    /// information is present in the file.
    ///
    /// [`PageIndex`]: https://github.com/apache/parquet-format/blob/master/PageIndex.md
    /// [`ParquetMetaData::column_index`]: crate::file::metadata::ParquetMetaData::column_index
    /// [`ParquetMetaData::offset_index`]: crate::file::metadata::ParquetMetaData::offset_index
    pub fn with_page_index(self, page_index: bool) -> Self {
        Self { page_index, ..self }
    }
}

/// The metadata necessary to construct a [`ArrowReaderBuilder`]
///
/// Note this structure is cheaply clone-able as it consists of several arcs.
///
/// This structure allows
///
/// 1. Loading metadata for a file once and then using that same metadata to
///    construct multiple separate readers, for example, to distribute readers
///    across multiple threads
///
/// 2. Using a cached copy of the [`ParquetMetadata`] rather than reading it
///    from the file each time a reader is constructed.
///
/// [`ParquetMetadata`]: crate::file::metadata::ParquetMetaData
#[derive(Debug, Clone)]
pub struct ArrowReaderMetadata {
    /// The Parquet Metadata, if known aprior
    pub(crate) metadata: Arc<ParquetMetaData>,
    /// The Arrow Schema
    pub(crate) schema: SchemaRef,

    pub(crate) fields: Option<Arc<ParquetField>>,
}

impl ArrowReaderMetadata {
    /// Loads [`ArrowReaderMetadata`] from the provided [`ChunkReader`], if necessary
    ///
    /// See [`ParquetRecordBatchReaderBuilder::new_with_metadata`] for an
    /// example of how this can be used
    ///
    /// # Notes
    ///
    /// If `options` has [`ArrowReaderOptions::with_page_index`] true, but
    /// `Self::metadata` is missing the page index, this function will attempt
    /// to load the page index by making an object store request.
    pub fn load<T: ChunkReader>(reader: &T, options: ArrowReaderOptions) -> Result<Self> {
        let metadata = ParquetMetaDataReader::new()
            .with_page_indexes(options.page_index)
            .parse_and_finish(reader)?;
        Self::try_new(Arc::new(metadata), options)
    }

    /// Create a new [`ArrowReaderMetadata`]
    ///
    /// # Notes
    ///
    /// This function does not attempt to load the PageIndex if not present in the metadata.
    /// See [`Self::load`] for more details.
    pub fn try_new(metadata: Arc<ParquetMetaData>, options: ArrowReaderOptions) -> Result<Self> {
        match options.supplied_schema {
            Some(supplied_schema) => Self::with_supplied_schema(metadata, supplied_schema.clone()),
            None => {
                let kv_metadata = match options.skip_arrow_metadata {
                    true => None,
                    false => metadata.file_metadata().key_value_metadata(),
                };

                let (schema, fields) = parquet_to_arrow_schema_and_fields(
                    metadata.file_metadata().schema_descr(),
                    ProjectionMask::all(),
                    kv_metadata,
                )?;

                Ok(Self {
                    metadata,
                    schema: Arc::new(schema),
                    fields: fields.map(Arc::new),
                })
            }
        }
    }

    fn with_supplied_schema(
        metadata: Arc<ParquetMetaData>,
        supplied_schema: SchemaRef,
    ) -> Result<Self> {
        let parquet_schema = metadata.file_metadata().schema_descr();
        let field_levels = parquet_to_arrow_field_levels(
            parquet_schema,
            ProjectionMask::all(),
            Some(supplied_schema.fields()),
        )?;
        let fields = field_levels.fields;
        let inferred_len = fields.len();
        let supplied_len = supplied_schema.fields().len();
        // Ensure the supplied schema has the same number of columns as the parquet schema.
        // parquet_to_arrow_field_levels is expected to throw an error if the schemas have
        // different lengths, but we check here to be safe.
        if inferred_len != supplied_len {
            Err(arrow_err!(format!(
                "incompatible arrow schema, expected {} columns received {}",
                inferred_len, supplied_len
            )))
        } else {
            let diff_fields: Vec<_> = supplied_schema
                .fields()
                .iter()
                .zip(fields.iter())
                .filter_map(|(field1, field2)| {
                    if field1 != field2 {
                        Some(field1.name().clone())
                    } else {
                        None
                    }
                })
                .collect();

            if !diff_fields.is_empty() {
                Err(ParquetError::ArrowError(format!(
                    "incompatible arrow schema, the following fields could not be cast: [{}]",
                    diff_fields.join(", ")
                )))
            } else {
                Ok(Self {
                    metadata,
                    schema: supplied_schema,
                    fields: field_levels.levels.map(Arc::new),
                })
            }
        }
    }

    /// Returns a reference to the [`ParquetMetaData`] for this parquet file
    pub fn metadata(&self) -> &Arc<ParquetMetaData> {
        &self.metadata
    }

    /// Returns the parquet [`SchemaDescriptor`] for this parquet file
    pub fn parquet_schema(&self) -> &SchemaDescriptor {
        self.metadata.file_metadata().schema_descr()
    }

    /// Returns the arrow [`SchemaRef`] for this parquet file
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }
}

#[doc(hidden)]
/// A newtype used within [`ReaderOptionsBuilder`] to distinguish sync readers from async
pub struct SyncReader<T: ChunkReader>(T);

/// A synchronous builder used to construct [`ParquetRecordBatchReader`] for a file
///
/// For an async API see [`crate::arrow::async_reader::ParquetRecordBatchStreamBuilder`]
///
/// See [`ArrowReaderBuilder`] for additional member functions
pub type ParquetRecordBatchReaderBuilder<T> = ArrowReaderBuilder<SyncReader<T>>;

impl<T: ChunkReader + 'static> ParquetRecordBatchReaderBuilder<T> {
    /// Create a new [`ParquetRecordBatchReaderBuilder`]
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use bytes::Bytes;
    /// # use arrow_array::{Int32Array, RecordBatch};
    /// # use arrow_schema::{DataType, Field, Schema};
    /// # use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
    /// # use parquet::arrow::ArrowWriter;
    /// # let mut file: Vec<u8> = Vec::with_capacity(1024);
    /// # let schema = Arc::new(Schema::new(vec![Field::new("i32", DataType::Int32, false)]));
    /// # let mut writer = ArrowWriter::try_new(&mut file, schema.clone(), None).unwrap();
    /// # let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap();
    /// # writer.write(&batch).unwrap();
    /// # writer.close().unwrap();
    /// # let file = Bytes::from(file);
    /// #
    /// let mut builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    ///
    /// // Inspect metadata
    /// assert_eq!(builder.metadata().num_row_groups(), 1);
    ///
    /// // Construct reader
    /// let mut reader: ParquetRecordBatchReader = builder.with_row_groups(vec![0]).build().unwrap();
    ///
    /// // Read data
    /// let _batch = reader.next().unwrap().unwrap();
    /// ```
    pub fn try_new(reader: T) -> Result<Self> {
        Self::try_new_with_options(reader, Default::default())
    }

    /// Create a new [`ParquetRecordBatchReaderBuilder`] with [`ArrowReaderOptions`]
    pub fn try_new_with_options(reader: T, options: ArrowReaderOptions) -> Result<Self> {
        let metadata = ArrowReaderMetadata::load(&reader, options)?;
        Ok(Self::new_with_metadata(reader, metadata))
    }

    /// Create a [`ParquetRecordBatchReaderBuilder`] from the provided [`ArrowReaderMetadata`]
    ///
    /// This interface allows:
    ///
    /// 1. Loading metadata once and using it to create multiple builders with
    ///    potentially different settings or run on different threads
    ///
    /// 2. Using a cached copy of the metadata rather than re-reading it from the
    ///    file each time a reader is constructed.
    ///
    /// See the docs on [`ArrowReaderMetadata`] for more details
    ///
    /// # Example
    /// ```
    /// # use std::fs::metadata;
    /// # use std::sync::Arc;
    /// # use bytes::Bytes;
    /// # use arrow_array::{Int32Array, RecordBatch};
    /// # use arrow_schema::{DataType, Field, Schema};
    /// # use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
    /// # use parquet::arrow::ArrowWriter;
    /// # let mut file: Vec<u8> = Vec::with_capacity(1024);
    /// # let schema = Arc::new(Schema::new(vec![Field::new("i32", DataType::Int32, false)]));
    /// # let mut writer = ArrowWriter::try_new(&mut file, schema.clone(), None).unwrap();
    /// # let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap();
    /// # writer.write(&batch).unwrap();
    /// # writer.close().unwrap();
    /// # let file = Bytes::from(file);
    /// #
    /// let metadata = ArrowReaderMetadata::load(&file, Default::default()).unwrap();
    /// let mut a = ParquetRecordBatchReaderBuilder::new_with_metadata(file.clone(), metadata.clone()).build().unwrap();
    /// let mut b = ParquetRecordBatchReaderBuilder::new_with_metadata(file, metadata).build().unwrap();
    ///
    /// // Should be able to read from both in parallel
    /// assert_eq!(a.next().unwrap().unwrap(), b.next().unwrap().unwrap());
    /// ```
    pub fn new_with_metadata(input: T, metadata: ArrowReaderMetadata) -> Self {
        Self::new_builder(SyncReader(input), metadata)
    }

    /// Build a [`ParquetRecordBatchReader`]
    ///
    /// Note: this will eagerly evaluate any `RowFilter` before returning
    pub fn build(self) -> Result<ParquetRecordBatchReader> {
        // Try to avoid allocate large buffer
        let batch_size = self
            .batch_size
            .min(self.metadata.file_metadata().num_rows() as usize);

        let row_groups = self
            .row_groups
            .unwrap_or_else(|| (0..self.metadata.num_row_groups()).collect());

        let reader = ReaderRowGroups {
            reader: Arc::new(self.input.0),
            metadata: self.metadata,
            row_groups,
        };

        let mut filter = self.filter;
        let mut selection = self.selection;

        if let Some(filter) = filter.as_mut() {
            for predicate in filter.predicates.iter_mut() {
                if !selects_any(selection.as_ref()) {
                    break;
                }

                let array_reader =
                    build_array_reader(self.fields.as_deref(), predicate.projection(), &reader)?;

                selection = Some(evaluate_predicate(
                    batch_size,
                    array_reader,
                    selection,
                    predicate.as_mut(),
                )?);
            }
        }

        let array_reader = build_array_reader(self.fields.as_deref(), &self.projection, &reader)?;

        // If selection is empty, truncate
        if !selects_any(selection.as_ref()) {
            selection = Some(RowSelection::from(vec![]));
        }

        Ok(ParquetRecordBatchReader::new(
            batch_size,
            array_reader,
            apply_range(selection, reader.num_rows(), self.offset, self.limit),
        ))
    }
}

struct ReaderRowGroups<T: ChunkReader> {
    reader: Arc<T>,

    metadata: Arc<ParquetMetaData>,
    /// Optional list of row group indices to scan
    row_groups: Vec<usize>,
}

impl<T: ChunkReader + 'static> RowGroups for ReaderRowGroups<T> {
    fn num_rows(&self) -> usize {
        let meta = self.metadata.row_groups();
        self.row_groups
            .iter()
            .map(|x| meta[*x].num_rows() as usize)
            .sum()
    }

    fn column_chunks(&self, i: usize) -> Result<Box<dyn PageIterator>> {
        Ok(Box::new(ReaderPageIterator {
            column_idx: i,
            reader: self.reader.clone(),
            metadata: self.metadata.clone(),
            row_groups: self.row_groups.clone().into_iter(),
        }))
    }
}

struct ReaderPageIterator<T: ChunkReader> {
    reader: Arc<T>,
    column_idx: usize,
    row_groups: std::vec::IntoIter<usize>,
    metadata: Arc<ParquetMetaData>,
}

impl<T: ChunkReader + 'static> Iterator for ReaderPageIterator<T> {
    type Item = Result<Box<dyn PageReader>>;

    fn next(&mut self) -> Option<Self::Item> {
        let rg_idx = self.row_groups.next()?;
        let rg = self.metadata.row_group(rg_idx);
        let meta = rg.column(self.column_idx);
        let offset_index = self.metadata.offset_index();
        // `offset_index` may not exist and `i[rg_idx]` will be empty.
        // To avoid `i[rg_idx][self.oolumn_idx`] panic, we need to filter out empty `i[rg_idx]`.
        let page_locations = offset_index
            .filter(|i| !i[rg_idx].is_empty())
            .map(|i| i[rg_idx][self.column_idx].page_locations.clone());
        let total_rows = rg.num_rows() as usize;
        let reader = self.reader.clone();

        let ret = SerializedPageReader::new(reader, meta, total_rows, page_locations);
        Some(ret.map(|x| Box::new(x) as _))
    }
}

impl<T: ChunkReader + 'static> PageIterator for ReaderPageIterator<T> {}

/// An `Iterator<Item = ArrowResult<RecordBatch>>` that yields [`RecordBatch`]
/// read from a parquet data source
pub struct ParquetRecordBatchReader {
    batch_size: usize,
    array_reader: Box<dyn ArrayReader>,
    schema: SchemaRef,
    selection: Option<VecDeque<RowSelector>>,
}

impl Iterator for ParquetRecordBatchReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut read_records = 0;
        match self.selection.as_mut() {
            Some(selection) => {
                while read_records < self.batch_size && !selection.is_empty() {
                    let front = selection.pop_front().unwrap();
                    if front.skip {
                        let skipped = match self.array_reader.skip_records(front.row_count) {
                            Ok(skipped) => skipped,
                            Err(e) => return Some(Err(e.into())),
                        };

                        if skipped != front.row_count {
                            return Some(Err(general_err!(
                                "failed to skip rows, expected {}, got {}",
                                front.row_count,
                                skipped
                            )
                            .into()));
                        }
                        continue;
                    }

                    //Currently, when RowSelectors with row_count = 0 are included then its interpreted as end of reader.
                    //Fix is to skip such entries. See https://github.com/apache/arrow-rs/issues/2669
                    if front.row_count == 0 {
                        continue;
                    }

                    // try to read record
                    let need_read = self.batch_size - read_records;
                    let to_read = match front.row_count.checked_sub(need_read) {
                        Some(remaining) if remaining != 0 => {
                            // if page row count less than batch_size we must set batch size to page row count.
                            // add check avoid dead loop
                            selection.push_front(RowSelector::select(remaining));
                            need_read
                        }
                        _ => front.row_count,
                    };
                    match self.array_reader.read_records(to_read) {
                        Ok(0) => break,
                        Ok(rec) => read_records += rec,
                        Err(error) => return Some(Err(error.into())),
                    }
                }
            }
            None => {
                if let Err(error) = self.array_reader.read_records(self.batch_size) {
                    return Some(Err(error.into()));
                }
            }
        };

        match self.array_reader.consume_batch() {
            Err(error) => Some(Err(error.into())),
            Ok(array) => {
                let struct_array = array.as_struct_opt().ok_or_else(|| {
                    ArrowError::ParquetError(
                        "Struct array reader should return struct array".to_string(),
                    )
                });

                match struct_array {
                    Err(err) => Some(Err(err)),
                    Ok(e) => (e.len() > 0).then(|| Ok(RecordBatch::from(e))),
                }
            }
        }
    }
}

impl RecordBatchReader for ParquetRecordBatchReader {
    /// Returns the projected [`SchemaRef`] for reading the parquet file.
    ///
    /// Note that the schema metadata will be stripped here. See
    /// [`ParquetRecordBatchReaderBuilder::schema`] if the metadata is desired.
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl ParquetRecordBatchReader {
    /// Create a new [`ParquetRecordBatchReader`] from the provided chunk reader
    ///
    /// See [`ParquetRecordBatchReaderBuilder`] for more options
    pub fn try_new<T: ChunkReader + 'static>(reader: T, batch_size: usize) -> Result<Self> {
        ParquetRecordBatchReaderBuilder::try_new(reader)?
            .with_batch_size(batch_size)
            .build()
    }

    /// Create a new [`ParquetRecordBatchReader`] from the provided [`RowGroups`]
    ///
    /// Note: this is a low-level interface see [`ParquetRecordBatchReader::try_new`] for a
    /// higher-level interface for reading parquet data from a file
    pub fn try_new_with_row_groups(
        levels: &FieldLevels,
        row_groups: &dyn RowGroups,
        batch_size: usize,
        selection: Option<RowSelection>,
    ) -> Result<Self> {
        let array_reader =
            build_array_reader(levels.levels.as_ref(), &ProjectionMask::all(), row_groups)?;

        Ok(Self {
            batch_size,
            array_reader,
            schema: Arc::new(Schema::new(levels.fields.clone())),
            selection: selection.map(|s| s.trim().into()),
        })
    }

    /// Create a new [`ParquetRecordBatchReader`] that will read at most `batch_size` rows at
    /// a time from [`ArrayReader`] based on the configured `selection`. If `selection` is `None`
    /// all rows will be returned
    pub(crate) fn new(
        batch_size: usize,
        array_reader: Box<dyn ArrayReader>,
        selection: Option<RowSelection>,
    ) -> Self {
        let schema = match array_reader.get_data_type() {
            ArrowType::Struct(ref fields) => Schema::new(fields.clone()),
            _ => unreachable!("Struct array reader's data type is not struct!"),
        };

        Self {
            batch_size,
            array_reader,
            schema: Arc::new(schema),
            selection: selection.map(|s| s.trim().into()),
        }
    }
}

/// Returns `true` if `selection` is `None` or selects some rows
pub(crate) fn selects_any(selection: Option<&RowSelection>) -> bool {
    selection.map(|x| x.selects_any()).unwrap_or(true)
}

/// Applies an optional offset and limit to an optional [`RowSelection`]
pub(crate) fn apply_range(
    mut selection: Option<RowSelection>,
    row_count: usize,
    offset: Option<usize>,
    limit: Option<usize>,
) -> Option<RowSelection> {
    // If an offset is defined, apply it to the `selection`
    if let Some(offset) = offset {
        selection = Some(match row_count.checked_sub(offset) {
            None => RowSelection::from(vec![]),
            Some(remaining) => selection
                .map(|selection| selection.offset(offset))
                .unwrap_or_else(|| {
                    RowSelection::from(vec![
                        RowSelector::skip(offset),
                        RowSelector::select(remaining),
                    ])
                }),
        });
    }

    // If a limit is defined, apply it to the final `selection`
    if let Some(limit) = limit {
        selection = Some(
            selection
                .map(|selection| selection.limit(limit))
                .unwrap_or_else(|| {
                    RowSelection::from(vec![RowSelector::select(limit.min(row_count))])
                }),
        );
    }
    selection
}

/// Evaluates an [`ArrowPredicate`], returning a [`RowSelection`] indicating
/// which rows to return.
///
/// `input_selection`: Optional pre-existing selection. If `Some`, then the
/// final [`RowSelection`] will be the conjunction of it and the rows selected
/// by `predicate`.
///
/// Note: A pre-existing selection may come from evaluating a previous predicate
/// or if the [`ParquetRecordBatchReader`] specified an explicit
/// [`RowSelection`] in addition to one or more predicates.
pub(crate) fn evaluate_predicate(
    batch_size: usize,
    array_reader: Box<dyn ArrayReader>,
    input_selection: Option<RowSelection>,
    predicate: &mut dyn ArrowPredicate,
) -> Result<RowSelection> {
    let reader = ParquetRecordBatchReader::new(batch_size, array_reader, input_selection.clone());
    let mut filters = vec![];
    for maybe_batch in reader {
        let maybe_batch = maybe_batch?;
        let input_rows = maybe_batch.num_rows();
        let filter = predicate.evaluate(maybe_batch)?;
        // Since user supplied predicate, check error here to catch bugs quickly
        if filter.len() != input_rows {
            return Err(arrow_err!(
                "ArrowPredicate predicate returned {} rows, expected {input_rows}",
                filter.len()
            ));
        }
        match filter.null_count() {
            0 => filters.push(filter),
            _ => filters.push(prep_null_mask_filter(&filter)),
        };
    }

    let raw = RowSelection::from_filters(&filters);
    Ok(match input_selection {
        Some(selection) => selection.and_then(&raw),
        None => raw,
    })
}

#[cfg(test)]
mod tests {
    use std::cmp::min;
    use std::collections::{HashMap, VecDeque};
    use std::fmt::Formatter;
    use std::fs::File;
    use std::io::Seek;
    use std::path::PathBuf;
    use std::sync::Arc;

    use bytes::Bytes;
    use half::f16;
    use num::PrimInt;
    use rand::{rng, Rng, RngCore};
    use tempfile::tempfile;

    use arrow_array::builder::*;
    use arrow_array::cast::AsArray;
    use arrow_array::types::{
        Date32Type, Date64Type, Decimal128Type, Decimal256Type, DecimalType, Float16Type,
        Float32Type, Float64Type, Time32MillisecondType, Time64MicrosecondType,
    };
    use arrow_array::*;
    use arrow_buffer::{i256, ArrowNativeType, Buffer, IntervalDayTime};
    use arrow_data::{ArrayData, ArrayDataBuilder};
    use arrow_schema::{
        ArrowError, DataType as ArrowDataType, Field, Fields, Schema, SchemaRef, TimeUnit,
    };
    use arrow_select::concat::concat_batches;

    use crate::arrow::arrow_reader::{
        ArrowPredicateFn, ArrowReaderBuilder, ArrowReaderOptions, ParquetRecordBatchReader,
        ParquetRecordBatchReaderBuilder, RowFilter, RowSelection, RowSelector,
    };
    use crate::arrow::schema::add_encoded_arrow_schema_to_metadata;
    use crate::arrow::{ArrowWriter, ProjectionMask};
    use crate::basic::{ConvertedType, Encoding, Repetition, Type as PhysicalType};
    use crate::column::reader::decoder::REPETITION_LEVELS_BATCH_SIZE;
    use crate::data_type::{
        BoolType, ByteArray, ByteArrayType, DataType, FixedLenByteArray, FixedLenByteArrayType,
        FloatType, Int32Type, Int64Type, Int96Type,
    };
    use crate::errors::Result;
    use crate::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
    use crate::file::writer::SerializedFileWriter;
    use crate::schema::parser::parse_message_type;
    use crate::schema::types::{Type, TypePtr};
    use crate::util::test_common::rand_gen::RandGen;

    #[test]
    fn test_arrow_reader_all_columns() {
        let file = get_test_file("parquet/generated_simple_numerics/blogs.parquet");

        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let original_schema = Arc::clone(builder.schema());
        let reader = builder.build().unwrap();

        // Verify that the schema was correctly parsed
        assert_eq!(original_schema.fields(), reader.schema().fields());
    }

    #[test]
    fn test_arrow_reader_single_column() {
        let file = get_test_file("parquet/generated_simple_numerics/blogs.parquet");

        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let original_schema = Arc::clone(builder.schema());

        let mask = ProjectionMask::leaves(builder.parquet_schema(), [2]);
        let reader = builder.with_projection(mask).build().unwrap();

        // Verify that the schema was correctly parsed
        assert_eq!(1, reader.schema().fields().len());
        assert_eq!(original_schema.fields()[1], reader.schema().fields()[0]);
    }

    #[test]
    fn test_arrow_reader_single_column_by_name() {
        let file = get_test_file("parquet/generated_simple_numerics/blogs.parquet");

        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let original_schema = Arc::clone(builder.schema());

        let mask = ProjectionMask::columns(builder.parquet_schema(), ["blog_id"]);
        let reader = builder.with_projection(mask).build().unwrap();

        // Verify that the schema was correctly parsed
        assert_eq!(1, reader.schema().fields().len());
        assert_eq!(original_schema.fields()[1], reader.schema().fields()[0]);
    }

    #[test]
    fn test_null_column_reader_test() {
        let mut file = tempfile::tempfile().unwrap();

        let schema = "
            message message {
                OPTIONAL INT32 int32;
            }
        ";
        let schema = Arc::new(parse_message_type(schema).unwrap());

        let def_levels = vec![vec![0, 0, 0], vec![0, 0, 0, 0]];
        generate_single_column_file_with_data::<Int32Type>(
            &[vec![], vec![]],
            Some(&def_levels),
            file.try_clone().unwrap(), // Cannot use &mut File (#1163)
            schema,
            Some(Field::new("int32", ArrowDataType::Null, true)),
            &Default::default(),
        )
        .unwrap();

        file.rewind().unwrap();

        let record_reader = ParquetRecordBatchReader::try_new(file, 2).unwrap();
        let batches = record_reader.collect::<Result<Vec<_>, _>>().unwrap();

        assert_eq!(batches.len(), 4);
        for batch in &batches[0..3] {
            assert_eq!(batch.num_rows(), 2);
            assert_eq!(batch.num_columns(), 1);
            assert_eq!(batch.column(0).null_count(), 2);
        }

        assert_eq!(batches[3].num_rows(), 1);
        assert_eq!(batches[3].num_columns(), 1);
        assert_eq!(batches[3].column(0).null_count(), 1);
    }

    #[test]
    fn test_primitive_single_column_reader_test() {
        run_single_column_reader_tests::<BoolType, _, BoolType>(
            2,
            ConvertedType::NONE,
            None,
            |vals| Arc::new(BooleanArray::from_iter(vals.iter().cloned())),
            &[Encoding::PLAIN, Encoding::RLE, Encoding::RLE_DICTIONARY],
        );
        run_single_column_reader_tests::<Int32Type, _, Int32Type>(
            2,
            ConvertedType::NONE,
            None,
            |vals| Arc::new(Int32Array::from_iter(vals.iter().cloned())),
            &[
                Encoding::PLAIN,
                Encoding::RLE_DICTIONARY,
                Encoding::DELTA_BINARY_PACKED,
                Encoding::BYTE_STREAM_SPLIT,
            ],
        );
        run_single_column_reader_tests::<Int64Type, _, Int64Type>(
            2,
            ConvertedType::NONE,
            None,
            |vals| Arc::new(Int64Array::from_iter(vals.iter().cloned())),
            &[
                Encoding::PLAIN,
                Encoding::RLE_DICTIONARY,
                Encoding::DELTA_BINARY_PACKED,
                Encoding::BYTE_STREAM_SPLIT,
            ],
        );
        run_single_column_reader_tests::<FloatType, _, FloatType>(
            2,
            ConvertedType::NONE,
            None,
            |vals| Arc::new(Float32Array::from_iter(vals.iter().cloned())),
            &[Encoding::PLAIN, Encoding::BYTE_STREAM_SPLIT],
        );
    }

    #[test]
    fn test_unsigned_primitive_single_column_reader_test() {
        run_single_column_reader_tests::<Int32Type, _, Int32Type>(
            2,
            ConvertedType::UINT_32,
            Some(ArrowDataType::UInt32),
            |vals| {
                Arc::new(UInt32Array::from_iter(
                    vals.iter().map(|x| x.map(|x| x as u32)),
                ))
            },
            &[
                Encoding::PLAIN,
                Encoding::RLE_DICTIONARY,
                Encoding::DELTA_BINARY_PACKED,
            ],
        );
        run_single_column_reader_tests::<Int64Type, _, Int64Type>(
            2,
            ConvertedType::UINT_64,
            Some(ArrowDataType::UInt64),
            |vals| {
                Arc::new(UInt64Array::from_iter(
                    vals.iter().map(|x| x.map(|x| x as u64)),
                ))
            },
            &[
                Encoding::PLAIN,
                Encoding::RLE_DICTIONARY,
                Encoding::DELTA_BINARY_PACKED,
            ],
        );
    }

    #[test]
    fn test_unsigned_roundtrip() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("uint32", ArrowDataType::UInt32, true),
            Field::new("uint64", ArrowDataType::UInt64, true),
        ]));

        let mut buf = Vec::with_capacity(1024);
        let mut writer = ArrowWriter::try_new(&mut buf, schema.clone(), None).unwrap();

        let original = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt32Array::from_iter_values([
                    0,
                    i32::MAX as u32,
                    u32::MAX,
                ])),
                Arc::new(UInt64Array::from_iter_values([
                    0,
                    i64::MAX as u64,
                    u64::MAX,
                ])),
            ],
        )
        .unwrap();

        writer.write(&original).unwrap();
        writer.close().unwrap();

        let mut reader = ParquetRecordBatchReader::try_new(Bytes::from(buf), 1024).unwrap();
        let ret = reader.next().unwrap().unwrap();
        assert_eq!(ret, original);

        // Check they can be downcast to the correct type
        ret.column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();

        ret.column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
    }

    #[test]
    fn test_float16_roundtrip() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("float16", ArrowDataType::Float16, false),
            Field::new("float16-nullable", ArrowDataType::Float16, true),
        ]));

        let mut buf = Vec::with_capacity(1024);
        let mut writer = ArrowWriter::try_new(&mut buf, schema.clone(), None)?;

        let original = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Float16Array::from_iter_values([
                    f16::EPSILON,
                    f16::MIN,
                    f16::MAX,
                    f16::NAN,
                    f16::INFINITY,
                    f16::NEG_INFINITY,
                    f16::ONE,
                    f16::NEG_ONE,
                    f16::ZERO,
                    f16::NEG_ZERO,
                    f16::E,
                    f16::PI,
                    f16::FRAC_1_PI,
                ])),
                Arc::new(Float16Array::from(vec![
                    None,
                    None,
                    None,
                    Some(f16::NAN),
                    Some(f16::INFINITY),
                    Some(f16::NEG_INFINITY),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    Some(f16::FRAC_1_PI),
                ])),
            ],
        )?;

        writer.write(&original)?;
        writer.close()?;

        let mut reader = ParquetRecordBatchReader::try_new(Bytes::from(buf), 1024)?;
        let ret = reader.next().unwrap()?;
        assert_eq!(ret, original);

        // Ensure can be downcast to the correct type
        ret.column(0).as_primitive::<Float16Type>();
        ret.column(1).as_primitive::<Float16Type>();

        Ok(())
    }

    #[test]
    fn test_time_utc_roundtrip() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "time_millis",
                ArrowDataType::Time32(TimeUnit::Millisecond),
                true,
            )
            .with_metadata(HashMap::from_iter(vec![(
                "adjusted_to_utc".to_string(),
                "".to_string(),
            )])),
            Field::new(
                "time_micros",
                ArrowDataType::Time64(TimeUnit::Microsecond),
                true,
            )
            .with_metadata(HashMap::from_iter(vec![(
                "adjusted_to_utc".to_string(),
                "".to_string(),
            )])),
        ]));

        let mut buf = Vec::with_capacity(1024);
        let mut writer = ArrowWriter::try_new(&mut buf, schema.clone(), None)?;

        let original = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Time32MillisecondArray::from(vec![
                    Some(-1),
                    Some(0),
                    Some(86_399_000),
                    Some(86_400_000),
                    Some(86_401_000),
                    None,
                ])),
                Arc::new(Time64MicrosecondArray::from(vec![
                    Some(-1),
                    Some(0),
                    Some(86_399 * 1_000_000),
                    Some(86_400 * 1_000_000),
                    Some(86_401 * 1_000_000),
                    None,
                ])),
            ],
        )?;

        writer.write(&original)?;
        writer.close()?;

        let mut reader = ParquetRecordBatchReader::try_new(Bytes::from(buf), 1024)?;
        let ret = reader.next().unwrap()?;
        assert_eq!(ret, original);

        // Ensure can be downcast to the correct type
        ret.column(0).as_primitive::<Time32MillisecondType>();
        ret.column(1).as_primitive::<Time64MicrosecondType>();

        Ok(())
    }

    #[test]
    fn test_date32_roundtrip() -> Result<()> {
        use arrow_array::Date32Array;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "date32",
            ArrowDataType::Date32,
            false,
        )]));

        let mut buf = Vec::with_capacity(1024);

        let mut writer = ArrowWriter::try_new(&mut buf, schema.clone(), None)?;

        let original = RecordBatch::try_new(
            schema,
            vec![Arc::new(Date32Array::from(vec![
                -1_000_000, -100_000, -10_000, -1_000, 0, 1_000, 10_000, 100_000, 1_000_000,
            ]))],
        )?;

        writer.write(&original)?;
        writer.close()?;

        let mut reader = ParquetRecordBatchReader::try_new(Bytes::from(buf), 1024)?;
        let ret = reader.next().unwrap()?;
        assert_eq!(ret, original);

        // Ensure can be downcast to the correct type
        ret.column(0).as_primitive::<Date32Type>();

        Ok(())
    }

    #[test]
    fn test_date64_roundtrip() -> Result<()> {
        use arrow_array::Date64Array;

        let schema = Arc::new(Schema::new(vec![
            Field::new("small-date64", ArrowDataType::Date64, false),
            Field::new("big-date64", ArrowDataType::Date64, false),
            Field::new("invalid-date64", ArrowDataType::Date64, false),
        ]));

        let mut default_buf = Vec::with_capacity(1024);
        let mut coerce_buf = Vec::with_capacity(1024);

        let coerce_props = WriterProperties::builder().set_coerce_types(true).build();

        let mut default_writer = ArrowWriter::try_new(&mut default_buf, schema.clone(), None)?;
        let mut coerce_writer =
            ArrowWriter::try_new(&mut coerce_buf, schema.clone(), Some(coerce_props))?;

        static NUM_MILLISECONDS_IN_DAY: i64 = 1000 * 60 * 60 * 24;

        let original = RecordBatch::try_new(
            schema,
            vec![
                // small-date64
                Arc::new(Date64Array::from(vec![
                    -1_000_000 * NUM_MILLISECONDS_IN_DAY,
                    -1_000 * NUM_MILLISECONDS_IN_DAY,
                    0,
                    1_000 * NUM_MILLISECONDS_IN_DAY,
                    1_000_000 * NUM_MILLISECONDS_IN_DAY,
                ])),
                // big-date64
                Arc::new(Date64Array::from(vec![
                    -10_000_000_000 * NUM_MILLISECONDS_IN_DAY,
                    -1_000_000_000 * NUM_MILLISECONDS_IN_DAY,
                    0,
                    1_000_000_000 * NUM_MILLISECONDS_IN_DAY,
                    10_000_000_000 * NUM_MILLISECONDS_IN_DAY,
                ])),
                // invalid-date64
                Arc::new(Date64Array::from(vec![
                    -1_000_000 * NUM_MILLISECONDS_IN_DAY + 1,
                    -1_000 * NUM_MILLISECONDS_IN_DAY + 1,
                    1,
                    1_000 * NUM_MILLISECONDS_IN_DAY + 1,
                    1_000_000 * NUM_MILLISECONDS_IN_DAY + 1,
                ])),
            ],
        )?;

        default_writer.write(&original)?;
        coerce_writer.write(&original)?;

        default_writer.close()?;
        coerce_writer.close()?;

        let mut default_reader = ParquetRecordBatchReader::try_new(Bytes::from(default_buf), 1024)?;
        let mut coerce_reader = ParquetRecordBatchReader::try_new(Bytes::from(coerce_buf), 1024)?;

        let default_ret = default_reader.next().unwrap()?;
        let coerce_ret = coerce_reader.next().unwrap()?;

        // Roundtrip should be successful when default writer used
        assert_eq!(default_ret, original);

        // Only small-date64 should roundtrip successfully when coerce_types writer is used
        assert_eq!(coerce_ret.column(0), original.column(0));
        assert_ne!(coerce_ret.column(1), original.column(1));
        assert_ne!(coerce_ret.column(2), original.column(2));

        // Ensure both can be downcast to the correct type
        default_ret.column(0).as_primitive::<Date64Type>();
        coerce_ret.column(0).as_primitive::<Date64Type>();

        Ok(())
    }
    struct RandFixedLenGen {}

    impl RandGen<FixedLenByteArrayType> for RandFixedLenGen {
        fn gen(len: i32) -> FixedLenByteArray {
            let mut v = vec![0u8; len as usize];
            rng().fill_bytes(&mut v);
            ByteArray::from(v).into()
        }
    }

    #[test]
    fn test_fixed_length_binary_column_reader() {
        run_single_column_reader_tests::<FixedLenByteArrayType, _, RandFixedLenGen>(
            20,
            ConvertedType::NONE,
            None,
            |vals| {
                let mut builder = FixedSizeBinaryBuilder::with_capacity(vals.len(), 20);
                for val in vals {
                    match val {
                        Some(b) => builder.append_value(b).unwrap(),
                        None => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            },
            &[Encoding::PLAIN, Encoding::RLE_DICTIONARY],
        );
    }

    #[test]
    fn test_interval_day_time_column_reader() {
        run_single_column_reader_tests::<FixedLenByteArrayType, _, RandFixedLenGen>(
            12,
            ConvertedType::INTERVAL,
            None,
            |vals| {
                Arc::new(
                    vals.iter()
                        .map(|x| {
                            x.as_ref().map(|b| IntervalDayTime {
                                days: i32::from_le_bytes(b.as_ref()[4..8].try_into().unwrap()),
                                milliseconds: i32::from_le_bytes(
                                    b.as_ref()[8..12].try_into().unwrap(),
                                ),
                            })
                        })
                        .collect::<IntervalDayTimeArray>(),
                )
            },
            &[Encoding::PLAIN, Encoding::RLE_DICTIONARY],
        );
    }

    #[test]
    fn test_int96_single_column_reader_test() {
        let encodings = &[Encoding::PLAIN, Encoding::RLE_DICTIONARY];
        run_single_column_reader_tests::<Int96Type, _, Int96Type>(
            2,
            ConvertedType::NONE,
            None,
            |vals| {
                Arc::new(TimestampNanosecondArray::from_iter(
                    vals.iter().map(|x| x.map(|x| x.to_nanos())),
                )) as _
            },
            encodings,
        );
    }

    struct RandUtf8Gen {}

    impl RandGen<ByteArrayType> for RandUtf8Gen {
        fn gen(len: i32) -> ByteArray {
            Int32Type::gen(len).to_string().as_str().into()
        }
    }

    #[test]
    fn test_utf8_single_column_reader_test() {
        fn string_converter<O: OffsetSizeTrait>(vals: &[Option<ByteArray>]) -> ArrayRef {
            Arc::new(GenericStringArray::<O>::from_iter(vals.iter().map(|x| {
                x.as_ref().map(|b| std::str::from_utf8(b.data()).unwrap())
            })))
        }

        let encodings = &[
            Encoding::PLAIN,
            Encoding::RLE_DICTIONARY,
            Encoding::DELTA_LENGTH_BYTE_ARRAY,
            Encoding::DELTA_BYTE_ARRAY,
        ];

        run_single_column_reader_tests::<ByteArrayType, _, RandUtf8Gen>(
            2,
            ConvertedType::NONE,
            None,
            |vals| {
                Arc::new(BinaryArray::from_iter(
                    vals.iter().map(|x| x.as_ref().map(|x| x.data())),
                ))
            },
            encodings,
        );

        run_single_column_reader_tests::<ByteArrayType, _, RandUtf8Gen>(
            2,
            ConvertedType::UTF8,
            None,
            string_converter::<i32>,
            encodings,
        );

        run_single_column_reader_tests::<ByteArrayType, _, RandUtf8Gen>(
            2,
            ConvertedType::UTF8,
            Some(ArrowDataType::Utf8),
            string_converter::<i32>,
            encodings,
        );

        run_single_column_reader_tests::<ByteArrayType, _, RandUtf8Gen>(
            2,
            ConvertedType::UTF8,
            Some(ArrowDataType::LargeUtf8),
            string_converter::<i64>,
            encodings,
        );

        let small_key_types = [ArrowDataType::Int8, ArrowDataType::UInt8];
        for key in &small_key_types {
            for encoding in encodings {
                let mut opts = TestOptions::new(2, 20, 15).with_null_percent(50);
                opts.encoding = *encoding;

                let data_type =
                    ArrowDataType::Dictionary(Box::new(key.clone()), Box::new(ArrowDataType::Utf8));

                // Cannot run full test suite as keys overflow, run small test instead
                single_column_reader_test::<ByteArrayType, _, RandUtf8Gen>(
                    opts,
                    2,
                    ConvertedType::UTF8,
                    Some(data_type.clone()),
                    move |vals| {
                        let vals = string_converter::<i32>(vals);
                        arrow::compute::cast(&vals, &data_type).unwrap()
                    },
                );
            }
        }

        let key_types = [
            ArrowDataType::Int16,
            ArrowDataType::UInt16,
            ArrowDataType::Int32,
            ArrowDataType::UInt32,
            ArrowDataType::Int64,
            ArrowDataType::UInt64,
        ];

        for key in &key_types {
            let data_type =
                ArrowDataType::Dictionary(Box::new(key.clone()), Box::new(ArrowDataType::Utf8));

            run_single_column_reader_tests::<ByteArrayType, _, RandUtf8Gen>(
                2,
                ConvertedType::UTF8,
                Some(data_type.clone()),
                move |vals| {
                    let vals = string_converter::<i32>(vals);
                    arrow::compute::cast(&vals, &data_type).unwrap()
                },
                encodings,
            );

            // https://github.com/apache/arrow-rs/issues/1179
            // let data_type = ArrowDataType::Dictionary(
            //     Box::new(key.clone()),
            //     Box::new(ArrowDataType::LargeUtf8),
            // );
            //
            // run_single_column_reader_tests::<ByteArrayType, _, RandUtf8Gen>(
            //     2,
            //     ConvertedType::UTF8,
            //     Some(data_type.clone()),
            //     move |vals| {
            //         let vals = string_converter::<i64>(vals);
            //         arrow::compute::cast(&vals, &data_type).unwrap()
            //     },
            //     encodings,
            // );
        }
    }

    #[test]
    fn test_decimal_nullable_struct() {
        let decimals = Decimal256Array::from_iter_values(
            [1, 2, 3, 4, 5, 6, 7, 8].into_iter().map(i256::from_i128),
        );

        let data = ArrayDataBuilder::new(ArrowDataType::Struct(Fields::from(vec![Field::new(
            "decimals",
            decimals.data_type().clone(),
            false,
        )])))
        .len(8)
        .null_bit_buffer(Some(Buffer::from(&[0b11101111])))
        .child_data(vec![decimals.into_data()])
        .build()
        .unwrap();

        let written =
            RecordBatch::try_from_iter([("struct", Arc::new(StructArray::from(data)) as ArrayRef)])
                .unwrap();

        let mut buffer = Vec::with_capacity(1024);
        let mut writer = ArrowWriter::try_new(&mut buffer, written.schema(), None).unwrap();
        writer.write(&written).unwrap();
        writer.close().unwrap();

        let read = ParquetRecordBatchReader::try_new(Bytes::from(buffer), 3)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(&written.slice(0, 3), &read[0]);
        assert_eq!(&written.slice(3, 3), &read[1]);
        assert_eq!(&written.slice(6, 2), &read[2]);
    }

    #[test]
    fn test_int32_nullable_struct() {
        let int32 = Int32Array::from_iter_values([1, 2, 3, 4, 5, 6, 7, 8]);
        let data = ArrayDataBuilder::new(ArrowDataType::Struct(Fields::from(vec![Field::new(
            "int32",
            int32.data_type().clone(),
            false,
        )])))
        .len(8)
        .null_bit_buffer(Some(Buffer::from(&[0b11101111])))
        .child_data(vec![int32.into_data()])
        .build()
        .unwrap();

        let written =
            RecordBatch::try_from_iter([("struct", Arc::new(StructArray::from(data)) as ArrayRef)])
                .unwrap();

        let mut buffer = Vec::with_capacity(1024);
        let mut writer = ArrowWriter::try_new(&mut buffer, written.schema(), None).unwrap();
        writer.write(&written).unwrap();
        writer.close().unwrap();

        let read = ParquetRecordBatchReader::try_new(Bytes::from(buffer), 3)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(&written.slice(0, 3), &read[0]);
        assert_eq!(&written.slice(3, 3), &read[1]);
        assert_eq!(&written.slice(6, 2), &read[2]);
    }

    #[test]
    #[ignore] // https://github.com/apache/arrow-rs/issues/2253
    fn test_decimal_list() {
        let decimals = Decimal128Array::from_iter_values([1, 2, 3, 4, 5, 6, 7, 8]);

        // [[], [1], [2, 3], null, [4], null, [6, 7, 8]]
        let data = ArrayDataBuilder::new(ArrowDataType::List(Arc::new(Field::new_list_field(
            decimals.data_type().clone(),
            false,
        ))))
        .len(7)
        .add_buffer(Buffer::from_iter([0_i32, 0, 1, 3, 3, 4, 5, 8]))
        .null_bit_buffer(Some(Buffer::from(&[0b01010111])))
        .child_data(vec![decimals.into_data()])
        .build()
        .unwrap();

        let written =
            RecordBatch::try_from_iter([("list", Arc::new(ListArray::from(data)) as ArrayRef)])
                .unwrap();

        let mut buffer = Vec::with_capacity(1024);
        let mut writer = ArrowWriter::try_new(&mut buffer, written.schema(), None).unwrap();
        writer.write(&written).unwrap();
        writer.close().unwrap();

        let read = ParquetRecordBatchReader::try_new(Bytes::from(buffer), 3)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(&written.slice(0, 3), &read[0]);
        assert_eq!(&written.slice(3, 3), &read[1]);
        assert_eq!(&written.slice(6, 1), &read[2]);
    }

    #[test]
    fn test_read_decimal_file() {
        use arrow_array::Decimal128Array;
        let testdata = arrow::util::test_util::parquet_test_data();
        let file_variants = vec![
            ("byte_array", 4),
            ("fixed_length", 25),
            ("int32", 4),
            ("int64", 10),
        ];
        for (prefix, target_precision) in file_variants {
            let path = format!("{testdata}/{prefix}_decimal.parquet");
            let file = File::open(path).unwrap();
            let mut record_reader = ParquetRecordBatchReader::try_new(file, 32).unwrap();

            let batch = record_reader.next().unwrap().unwrap();
            assert_eq!(batch.num_rows(), 24);
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap();

            let expected = 1..25;

            assert_eq!(col.precision(), target_precision);
            assert_eq!(col.scale(), 2);

            for (i, v) in expected.enumerate() {
                assert_eq!(col.value(i), v * 100_i128);
            }
        }
    }

    #[test]
    fn test_read_float16_nonzeros_file() {
        use arrow_array::Float16Array;
        let testdata = arrow::util::test_util::parquet_test_data();
        // see https://github.com/apache/parquet-testing/pull/40
        let path = format!("{testdata}/float16_nonzeros_and_nans.parquet");
        let file = File::open(path).unwrap();
        let mut record_reader = ParquetRecordBatchReader::try_new(file, 32).unwrap();

        let batch = record_reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 8);
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float16Array>()
            .unwrap();

        let f16_two = f16::ONE + f16::ONE;

        assert_eq!(col.null_count(), 1);
        assert!(col.is_null(0));
        assert_eq!(col.value(1), f16::ONE);
        assert_eq!(col.value(2), -f16_two);
        assert!(col.value(3).is_nan());
        assert_eq!(col.value(4), f16::ZERO);
        assert!(col.value(4).is_sign_positive());
        assert_eq!(col.value(5), f16::NEG_ONE);
        assert_eq!(col.value(6), f16::NEG_ZERO);
        assert!(col.value(6).is_sign_negative());
        assert_eq!(col.value(7), f16_two);
    }

    #[test]
    fn test_read_float16_zeros_file() {
        use arrow_array::Float16Array;
        let testdata = arrow::util::test_util::parquet_test_data();
        // see https://github.com/apache/parquet-testing/pull/40
        let path = format!("{testdata}/float16_zeros_and_nans.parquet");
        let file = File::open(path).unwrap();
        let mut record_reader = ParquetRecordBatchReader::try_new(file, 32).unwrap();

        let batch = record_reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 3);
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float16Array>()
            .unwrap();

        assert_eq!(col.null_count(), 1);
        assert!(col.is_null(0));
        assert_eq!(col.value(1), f16::ZERO);
        assert!(col.value(1).is_sign_positive());
        assert!(col.value(2).is_nan());
    }

    #[test]
    fn test_read_float32_float64_byte_stream_split() {
        let path = format!(
            "{}/byte_stream_split.zstd.parquet",
            arrow::util::test_util::parquet_test_data(),
        );
        let file = File::open(path).unwrap();
        let record_reader = ParquetRecordBatchReader::try_new(file, 128).unwrap();

        let mut row_count = 0;
        for batch in record_reader {
            let batch = batch.unwrap();
            row_count += batch.num_rows();
            let f32_col = batch.column(0).as_primitive::<Float32Type>();
            let f64_col = batch.column(1).as_primitive::<Float64Type>();

            // This file contains floats from a standard normal distribution
            for &x in f32_col.values() {
                assert!(x > -10.0);
                assert!(x < 10.0);
            }
            for &x in f64_col.values() {
                assert!(x > -10.0);
                assert!(x < 10.0);
            }
        }
        assert_eq!(row_count, 300);
    }

    #[test]
    fn test_read_extended_byte_stream_split() {
        let path = format!(
            "{}/byte_stream_split_extended.gzip.parquet",
            arrow::util::test_util::parquet_test_data(),
        );
        let file = File::open(path).unwrap();
        let record_reader = ParquetRecordBatchReader::try_new(file, 128).unwrap();

        let mut row_count = 0;
        for batch in record_reader {
            let batch = batch.unwrap();
            row_count += batch.num_rows();

            // 0,1 are f16
            let f16_col = batch.column(0).as_primitive::<Float16Type>();
            let f16_bss = batch.column(1).as_primitive::<Float16Type>();
            assert_eq!(f16_col.len(), f16_bss.len());
            f16_col
                .iter()
                .zip(f16_bss.iter())
                .for_each(|(l, r)| assert_eq!(l.unwrap(), r.unwrap()));

            // 2,3 are f32
            let f32_col = batch.column(2).as_primitive::<Float32Type>();
            let f32_bss = batch.column(3).as_primitive::<Float32Type>();
            assert_eq!(f32_col.len(), f32_bss.len());
            f32_col
                .iter()
                .zip(f32_bss.iter())
                .for_each(|(l, r)| assert_eq!(l.unwrap(), r.unwrap()));

            // 4,5 are f64
            let f64_col = batch.column(4).as_primitive::<Float64Type>();
            let f64_bss = batch.column(5).as_primitive::<Float64Type>();
            assert_eq!(f64_col.len(), f64_bss.len());
            f64_col
                .iter()
                .zip(f64_bss.iter())
                .for_each(|(l, r)| assert_eq!(l.unwrap(), r.unwrap()));

            // 6,7 are i32
            let i32_col = batch.column(6).as_primitive::<types::Int32Type>();
            let i32_bss = batch.column(7).as_primitive::<types::Int32Type>();
            assert_eq!(i32_col.len(), i32_bss.len());
            i32_col
                .iter()
                .zip(i32_bss.iter())
                .for_each(|(l, r)| assert_eq!(l.unwrap(), r.unwrap()));

            // 8,9 are i64
            let i64_col = batch.column(8).as_primitive::<types::Int64Type>();
            let i64_bss = batch.column(9).as_primitive::<types::Int64Type>();
            assert_eq!(i64_col.len(), i64_bss.len());
            i64_col
                .iter()
                .zip(i64_bss.iter())
                .for_each(|(l, r)| assert_eq!(l.unwrap(), r.unwrap()));

            // 10,11 are FLBA(5)
            let flba_col = batch.column(10).as_fixed_size_binary();
            let flba_bss = batch.column(11).as_fixed_size_binary();
            assert_eq!(flba_col.len(), flba_bss.len());
            flba_col
                .iter()
                .zip(flba_bss.iter())
                .for_each(|(l, r)| assert_eq!(l.unwrap(), r.unwrap()));

            // 12,13 are FLBA(4) (decimal(7,3))
            let dec_col = batch.column(12).as_primitive::<Decimal128Type>();
            let dec_bss = batch.column(13).as_primitive::<Decimal128Type>();
            assert_eq!(dec_col.len(), dec_bss.len());
            dec_col
                .iter()
                .zip(dec_bss.iter())
                .for_each(|(l, r)| assert_eq!(l.unwrap(), r.unwrap()));
        }
        assert_eq!(row_count, 200);
    }

    #[test]
    fn test_read_incorrect_map_schema_file() {
        let testdata = arrow::util::test_util::parquet_test_data();
        // see https://github.com/apache/parquet-testing/pull/47
        let path = format!("{testdata}/incorrect_map_schema.parquet");
        let file = File::open(path).unwrap();
        let mut record_reader = ParquetRecordBatchReader::try_new(file, 32).unwrap();

        let batch = record_reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);

        let expected_schema = Schema::new(Fields::from(vec![Field::new(
            "my_map",
            ArrowDataType::Map(
                Arc::new(Field::new(
                    "key_value",
                    ArrowDataType::Struct(Fields::from(vec![
                        Field::new("key", ArrowDataType::Utf8, false),
                        Field::new("value", ArrowDataType::Utf8, true),
                    ])),
                    false,
                )),
                false,
            ),
            true,
        )]));
        assert_eq!(batch.schema().as_ref(), &expected_schema);

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.column(0).null_count(), 0);
        assert_eq!(
            batch.column(0).as_map().keys().as_ref(),
            &StringArray::from(vec!["parent", "name"])
        );
        assert_eq!(
            batch.column(0).as_map().values().as_ref(),
            &StringArray::from(vec!["another", "report"])
        );
    }

    /// Parameters for single_column_reader_test
    #[derive(Clone)]
    struct TestOptions {
        /// Number of row group to write to parquet (row group size =
        /// num_row_groups / num_rows)
        num_row_groups: usize,
        /// Total number of rows per row group
        num_rows: usize,
        /// Size of batches to read back
        record_batch_size: usize,
        /// Percentage of nulls in column or None if required
        null_percent: Option<usize>,
        /// Set write batch size
        ///
        /// This is the number of rows that are written at once to a page and
        /// therefore acts as a bound on the page granularity of a row group
        write_batch_size: usize,
        /// Maximum size of page in bytes
        max_data_page_size: usize,
        /// Maximum size of dictionary page in bytes
        max_dict_page_size: usize,
        /// Writer version
        writer_version: WriterVersion,
        /// Enabled statistics
        enabled_statistics: EnabledStatistics,
        /// Encoding
        encoding: Encoding,
        /// row selections and total selected row count
        row_selections: Option<(RowSelection, usize)>,
        /// row filter
        row_filter: Option<Vec<bool>>,
        /// limit
        limit: Option<usize>,
        /// offset
        offset: Option<usize>,
    }

    /// Manually implement this to avoid printing entire contents of row_selections and row_filter
    impl std::fmt::Debug for TestOptions {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("TestOptions")
                .field("num_row_groups", &self.num_row_groups)
                .field("num_rows", &self.num_rows)
                .field("record_batch_size", &self.record_batch_size)
                .field("null_percent", &self.null_percent)
                .field("write_batch_size", &self.write_batch_size)
                .field("max_data_page_size", &self.max_data_page_size)
                .field("max_dict_page_size", &self.max_dict_page_size)
                .field("writer_version", &self.writer_version)
                .field("enabled_statistics", &self.enabled_statistics)
                .field("encoding", &self.encoding)
                .field("row_selections", &self.row_selections.is_some())
                .field("row_filter", &self.row_filter.is_some())
                .field("limit", &self.limit)
                .field("offset", &self.offset)
                .finish()
        }
    }

    impl Default for TestOptions {
        fn default() -> Self {
            Self {
                num_row_groups: 2,
                num_rows: 100,
                record_batch_size: 15,
                null_percent: None,
                write_batch_size: 64,
                max_data_page_size: 1024 * 1024,
                max_dict_page_size: 1024 * 1024,
                writer_version: WriterVersion::PARQUET_1_0,
                enabled_statistics: EnabledStatistics::Page,
                encoding: Encoding::PLAIN,
                row_selections: None,
                row_filter: None,
                limit: None,
                offset: None,
            }
        }
    }

    impl TestOptions {
        fn new(num_row_groups: usize, num_rows: usize, record_batch_size: usize) -> Self {
            Self {
                num_row_groups,
                num_rows,
                record_batch_size,
                ..Default::default()
            }
        }

        fn with_null_percent(self, null_percent: usize) -> Self {
            Self {
                null_percent: Some(null_percent),
                ..self
            }
        }

        fn with_max_data_page_size(self, max_data_page_size: usize) -> Self {
            Self {
                max_data_page_size,
                ..self
            }
        }

        fn with_max_dict_page_size(self, max_dict_page_size: usize) -> Self {
            Self {
                max_dict_page_size,
                ..self
            }
        }

        fn with_enabled_statistics(self, enabled_statistics: EnabledStatistics) -> Self {
            Self {
                enabled_statistics,
                ..self
            }
        }

        fn with_row_selections(self) -> Self {
            assert!(self.row_filter.is_none(), "Must set row selection first");

            let mut rng = rng();
            let step = rng.random_range(self.record_batch_size..self.num_rows);
            let row_selections = create_test_selection(
                step,
                self.num_row_groups * self.num_rows,
                rng.random::<bool>(),
            );
            Self {
                row_selections: Some(row_selections),
                ..self
            }
        }

        fn with_row_filter(self) -> Self {
            let row_count = match &self.row_selections {
                Some((_, count)) => *count,
                None => self.num_row_groups * self.num_rows,
            };

            let mut rng = rng();
            Self {
                row_filter: Some((0..row_count).map(|_| rng.random_bool(0.9)).collect()),
                ..self
            }
        }

        fn with_limit(self, limit: usize) -> Self {
            Self {
                limit: Some(limit),
                ..self
            }
        }

        fn with_offset(self, offset: usize) -> Self {
            Self {
                offset: Some(offset),
                ..self
            }
        }

        fn writer_props(&self) -> WriterProperties {
            let builder = WriterProperties::builder()
                .set_data_page_size_limit(self.max_data_page_size)
                .set_write_batch_size(self.write_batch_size)
                .set_writer_version(self.writer_version)
                .set_statistics_enabled(self.enabled_statistics);

            let builder = match self.encoding {
                Encoding::RLE_DICTIONARY | Encoding::PLAIN_DICTIONARY => builder
                    .set_dictionary_enabled(true)
                    .set_dictionary_page_size_limit(self.max_dict_page_size),
                _ => builder
                    .set_dictionary_enabled(false)
                    .set_encoding(self.encoding),
            };

            builder.build()
        }
    }

    /// Create a parquet file and then read it using
    /// `ParquetFileArrowReader` using a standard set of parameters
    /// `opts`.
    ///
    /// `rand_max` represents the maximum size of value to pass to to
    /// value generator
    fn run_single_column_reader_tests<T, F, G>(
        rand_max: i32,
        converted_type: ConvertedType,
        arrow_type: Option<ArrowDataType>,
        converter: F,
        encodings: &[Encoding],
    ) where
        T: DataType,
        G: RandGen<T>,
        F: Fn(&[Option<T::T>]) -> ArrayRef,
    {
        let all_options = vec![
            // choose record_batch_batch (15) so batches cross row
            // group boundaries (50 rows in 2 row groups) cases.
            TestOptions::new(2, 100, 15),
            // choose record_batch_batch (5) so batches sometime fall
            // on row group boundaries and (25 rows in 3 row groups
            // --> row groups of 10, 10, and 5). Tests buffer
            // refilling edge cases.
            TestOptions::new(3, 25, 5),
            // Choose record_batch_size (25) so all batches fall
            // exactly on row group boundary (25). Tests buffer
            // refilling edge cases.
            TestOptions::new(4, 100, 25),
            // Set maximum page size so row groups have multiple pages
            TestOptions::new(3, 256, 73).with_max_data_page_size(128),
            // Set small dictionary page size to test dictionary fallback
            TestOptions::new(3, 256, 57).with_max_dict_page_size(128),
            // Test optional but with no nulls
            TestOptions::new(2, 256, 127).with_null_percent(0),
            // Test optional with nulls
            TestOptions::new(2, 256, 93).with_null_percent(25),
            // Test with limit of 0
            TestOptions::new(4, 100, 25).with_limit(0),
            // Test with limit of 50
            TestOptions::new(4, 100, 25).with_limit(50),
            // Test with limit equal to number of rows
            TestOptions::new(4, 100, 25).with_limit(10),
            // Test with limit larger than number of rows
            TestOptions::new(4, 100, 25).with_limit(101),
            // Test with limit + offset equal to number of rows
            TestOptions::new(4, 100, 25).with_offset(30).with_limit(20),
            // Test with limit + offset equal to number of rows
            TestOptions::new(4, 100, 25).with_offset(20).with_limit(80),
            // Test with limit + offset larger than number of rows
            TestOptions::new(4, 100, 25).with_offset(20).with_limit(81),
            // Test with no page-level statistics
            TestOptions::new(2, 256, 91)
                .with_null_percent(25)
                .with_enabled_statistics(EnabledStatistics::Chunk),
            // Test with no statistics
            TestOptions::new(2, 256, 91)
                .with_null_percent(25)
                .with_enabled_statistics(EnabledStatistics::None),
            // Test with all null
            TestOptions::new(2, 128, 91)
                .with_null_percent(100)
                .with_enabled_statistics(EnabledStatistics::None),
            // Test skip

            // choose record_batch_batch (15) so batches cross row
            // group boundaries (50 rows in 2 row groups) cases.
            TestOptions::new(2, 100, 15).with_row_selections(),
            // choose record_batch_batch (5) so batches sometime fall
            // on row group boundaries and (25 rows in 3 row groups
            // --> row groups of 10, 10, and 5). Tests buffer
            // refilling edge cases.
            TestOptions::new(3, 25, 5).with_row_selections(),
            // Choose record_batch_size (25) so all batches fall
            // exactly on row group boundary (25). Tests buffer
            // refilling edge cases.
            TestOptions::new(4, 100, 25).with_row_selections(),
            // Set maximum page size so row groups have multiple pages
            TestOptions::new(3, 256, 73)
                .with_max_data_page_size(128)
                .with_row_selections(),
            // Set small dictionary page size to test dictionary fallback
            TestOptions::new(3, 256, 57)
                .with_max_dict_page_size(128)
                .with_row_selections(),
            // Test optional but with no nulls
            TestOptions::new(2, 256, 127)
                .with_null_percent(0)
                .with_row_selections(),
            // Test optional with nulls
            TestOptions::new(2, 256, 93)
                .with_null_percent(25)
                .with_row_selections(),
            // Test optional with nulls
            TestOptions::new(2, 256, 93)
                .with_null_percent(25)
                .with_row_selections()
                .with_limit(10),
            // Test optional with nulls
            TestOptions::new(2, 256, 93)
                .with_null_percent(25)
                .with_row_selections()
                .with_offset(20)
                .with_limit(10),
            // Test filter

            // Test with row filter
            TestOptions::new(4, 100, 25).with_row_filter(),
            // Test with row selection and row filter
            TestOptions::new(4, 100, 25)
                .with_row_selections()
                .with_row_filter(),
            // Test with nulls and row filter
            TestOptions::new(2, 256, 93)
                .with_null_percent(25)
                .with_max_data_page_size(10)
                .with_row_filter(),
            // Test with nulls and row filter and small pages
            TestOptions::new(2, 256, 93)
                .with_null_percent(25)
                .with_max_data_page_size(10)
                .with_row_selections()
                .with_row_filter(),
            // Test with row selection and no offset index and small pages
            TestOptions::new(2, 256, 93)
                .with_enabled_statistics(EnabledStatistics::None)
                .with_max_data_page_size(10)
                .with_row_selections(),
        ];

        all_options.into_iter().for_each(|opts| {
            for writer_version in [WriterVersion::PARQUET_1_0, WriterVersion::PARQUET_2_0] {
                for encoding in encodings {
                    let opts = TestOptions {
                        writer_version,
                        encoding: *encoding,
                        ..opts.clone()
                    };

                    single_column_reader_test::<T, _, G>(
                        opts,
                        rand_max,
                        converted_type,
                        arrow_type.clone(),
                        &converter,
                    )
                }
            }
        });
    }

    /// Create a parquet file and then read it using
    /// `ParquetFileArrowReader` using the parameters described in
    /// `opts`.
    fn single_column_reader_test<T, F, G>(
        opts: TestOptions,
        rand_max: i32,
        converted_type: ConvertedType,
        arrow_type: Option<ArrowDataType>,
        converter: F,
    ) where
        T: DataType,
        G: RandGen<T>,
        F: Fn(&[Option<T::T>]) -> ArrayRef,
    {
        // Print out options to facilitate debugging failures on CI
        println!(
            "Running type {:?} single_column_reader_test ConvertedType::{}/ArrowType::{:?} with Options: {:?}",
            T::get_physical_type(), converted_type, arrow_type, opts
        );

        //according to null_percent generate def_levels
        let (repetition, def_levels) = match opts.null_percent.as_ref() {
            Some(null_percent) => {
                let mut rng = rng();

                let def_levels: Vec<Vec<i16>> = (0..opts.num_row_groups)
                    .map(|_| {
                        std::iter::from_fn(|| {
                            Some((rng.next_u32() as usize % 100 >= *null_percent) as i16)
                        })
                        .take(opts.num_rows)
                        .collect()
                    })
                    .collect();
                (Repetition::OPTIONAL, Some(def_levels))
            }
            None => (Repetition::REQUIRED, None),
        };

        //generate random table data
        let values: Vec<Vec<T::T>> = (0..opts.num_row_groups)
            .map(|idx| {
                let null_count = match def_levels.as_ref() {
                    Some(d) => d[idx].iter().filter(|x| **x == 0).count(),
                    None => 0,
                };
                G::gen_vec(rand_max, opts.num_rows - null_count)
            })
            .collect();

        let len = match T::get_physical_type() {
            crate::basic::Type::FIXED_LEN_BYTE_ARRAY => rand_max,
            crate::basic::Type::INT96 => 12,
            _ => -1,
        };

        let fields = vec![Arc::new(
            Type::primitive_type_builder("leaf", T::get_physical_type())
                .with_repetition(repetition)
                .with_converted_type(converted_type)
                .with_length(len)
                .build()
                .unwrap(),
        )];

        let schema = Arc::new(
            Type::group_type_builder("test_schema")
                .with_fields(fields)
                .build()
                .unwrap(),
        );

        let arrow_field = arrow_type.map(|t| Field::new("leaf", t, false));

        let mut file = tempfile::tempfile().unwrap();

        generate_single_column_file_with_data::<T>(
            &values,
            def_levels.as_ref(),
            file.try_clone().unwrap(), // Cannot use &mut File (#1163)
            schema,
            arrow_field,
            &opts,
        )
        .unwrap();

        file.rewind().unwrap();

        let options = ArrowReaderOptions::new()
            .with_page_index(opts.enabled_statistics == EnabledStatistics::Page);

        let mut builder =
            ParquetRecordBatchReaderBuilder::try_new_with_options(file, options).unwrap();

        let expected_data = match opts.row_selections {
            Some((selections, row_count)) => {
                let mut without_skip_data = gen_expected_data::<T>(def_levels.as_ref(), &values);

                let mut skip_data: Vec<Option<T::T>> = vec![];
                let dequeue: VecDeque<RowSelector> = selections.clone().into();
                for select in dequeue {
                    if select.skip {
                        without_skip_data.drain(0..select.row_count);
                    } else {
                        skip_data.extend(without_skip_data.drain(0..select.row_count));
                    }
                }
                builder = builder.with_row_selection(selections);

                assert_eq!(skip_data.len(), row_count);
                skip_data
            }
            None => {
                //get flatten table data
                let expected_data = gen_expected_data::<T>(def_levels.as_ref(), &values);
                assert_eq!(expected_data.len(), opts.num_rows * opts.num_row_groups);
                expected_data
            }
        };

        let mut expected_data = match opts.row_filter {
            Some(filter) => {
                let expected_data = expected_data
                    .into_iter()
                    .zip(filter.iter())
                    .filter_map(|(d, f)| f.then(|| d))
                    .collect();

                let mut filter_offset = 0;
                let filter = RowFilter::new(vec![Box::new(ArrowPredicateFn::new(
                    ProjectionMask::all(),
                    move |b| {
                        let array = BooleanArray::from_iter(
                            filter
                                .iter()
                                .skip(filter_offset)
                                .take(b.num_rows())
                                .map(|x| Some(*x)),
                        );
                        filter_offset += b.num_rows();
                        Ok(array)
                    },
                ))]);

                builder = builder.with_row_filter(filter);
                expected_data
            }
            None => expected_data,
        };

        if let Some(offset) = opts.offset {
            builder = builder.with_offset(offset);
            expected_data = expected_data.into_iter().skip(offset).collect();
        }

        if let Some(limit) = opts.limit {
            builder = builder.with_limit(limit);
            expected_data = expected_data.into_iter().take(limit).collect();
        }

        let mut record_reader = builder
            .with_batch_size(opts.record_batch_size)
            .build()
            .unwrap();

        let mut total_read = 0;
        loop {
            let maybe_batch = record_reader.next();
            if total_read < expected_data.len() {
                let end = min(total_read + opts.record_batch_size, expected_data.len());
                let batch = maybe_batch.unwrap().unwrap();
                assert_eq!(end - total_read, batch.num_rows());

                let a = converter(&expected_data[total_read..end]);
                let b = Arc::clone(batch.column(0));

                assert_eq!(a.data_type(), b.data_type());
                assert_eq!(a.to_data(), b.to_data());
                assert_eq!(
                    a.as_any().type_id(),
                    b.as_any().type_id(),
                    "incorrect type ids"
                );

                total_read = end;
            } else {
                assert!(maybe_batch.is_none());
                break;
            }
        }
    }

    fn gen_expected_data<T: DataType>(
        def_levels: Option<&Vec<Vec<i16>>>,
        values: &[Vec<T::T>],
    ) -> Vec<Option<T::T>> {
        let data: Vec<Option<T::T>> = match def_levels {
            Some(levels) => {
                let mut values_iter = values.iter().flatten();
                levels
                    .iter()
                    .flatten()
                    .map(|d| match d {
                        1 => Some(values_iter.next().cloned().unwrap()),
                        0 => None,
                        _ => unreachable!(),
                    })
                    .collect()
            }
            None => values.iter().flatten().map(|b| Some(b.clone())).collect(),
        };
        data
    }

    fn generate_single_column_file_with_data<T: DataType>(
        values: &[Vec<T::T>],
        def_levels: Option<&Vec<Vec<i16>>>,
        file: File,
        schema: TypePtr,
        field: Option<Field>,
        opts: &TestOptions,
    ) -> Result<crate::format::FileMetaData> {
        let mut writer_props = opts.writer_props();
        if let Some(field) = field {
            let arrow_schema = Schema::new(vec![field]);
            add_encoded_arrow_schema_to_metadata(&arrow_schema, &mut writer_props);
        }

        let mut writer = SerializedFileWriter::new(file, schema, Arc::new(writer_props))?;

        for (idx, v) in values.iter().enumerate() {
            let def_levels = def_levels.map(|d| d[idx].as_slice());
            let mut row_group_writer = writer.next_row_group()?;
            {
                let mut column_writer = row_group_writer
                    .next_column()?
                    .expect("Column writer is none!");

                column_writer
                    .typed::<T>()
                    .write_batch(v, def_levels, None)?;

                column_writer.close()?;
            }
            row_group_writer.close()?;
        }

        writer.close()
    }

    fn get_test_file(file_name: &str) -> File {
        let mut path = PathBuf::new();
        path.push(arrow::util::test_util::arrow_test_data());
        path.push(file_name);

        File::open(path.as_path()).expect("File not found!")
    }

    #[test]
    fn test_read_structs() {
        // This particular test file has columns of struct types where there is
        // a column that has the same name as one of the struct fields
        // (see: ARROW-11452)
        let testdata = arrow::util::test_util::parquet_test_data();
        let path = format!("{testdata}/nested_structs.rust.parquet");
        let file = File::open(&path).unwrap();
        let record_batch_reader = ParquetRecordBatchReader::try_new(file, 60).unwrap();

        for batch in record_batch_reader {
            batch.unwrap();
        }

        let file = File::open(&path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();

        let mask = ProjectionMask::leaves(builder.parquet_schema(), [3, 8, 10]);
        let projected_reader = builder
            .with_projection(mask)
            .with_batch_size(60)
            .build()
            .unwrap();

        let expected_schema = Schema::new(vec![
            Field::new(
                "roll_num",
                ArrowDataType::Struct(Fields::from(vec![Field::new(
                    "count",
                    ArrowDataType::UInt64,
                    false,
                )])),
                false,
            ),
            Field::new(
                "PC_CUR",
                ArrowDataType::Struct(Fields::from(vec![
                    Field::new("mean", ArrowDataType::Int64, false),
                    Field::new("sum", ArrowDataType::Int64, false),
                ])),
                false,
            ),
        ]);

        // Tests for #1652 and #1654
        assert_eq!(&expected_schema, projected_reader.schema().as_ref());

        for batch in projected_reader {
            let batch = batch.unwrap();
            assert_eq!(batch.schema().as_ref(), &expected_schema);
        }
    }

    #[test]
    // same as test_read_structs but constructs projection mask via column names
    fn test_read_structs_by_name() {
        let testdata = arrow::util::test_util::parquet_test_data();
        let path = format!("{testdata}/nested_structs.rust.parquet");
        let file = File::open(&path).unwrap();
        let record_batch_reader = ParquetRecordBatchReader::try_new(file, 60).unwrap();

        for batch in record_batch_reader {
            batch.unwrap();
        }

        let file = File::open(&path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();

        let mask = ProjectionMask::columns(
            builder.parquet_schema(),
            ["roll_num.count", "PC_CUR.mean", "PC_CUR.sum"],
        );
        let projected_reader = builder
            .with_projection(mask)
            .with_batch_size(60)
            .build()
            .unwrap();

        let expected_schema = Schema::new(vec![
            Field::new(
                "roll_num",
                ArrowDataType::Struct(Fields::from(vec![Field::new(
                    "count",
                    ArrowDataType::UInt64,
                    false,
                )])),
                false,
            ),
            Field::new(
                "PC_CUR",
                ArrowDataType::Struct(Fields::from(vec![
                    Field::new("mean", ArrowDataType::Int64, false),
                    Field::new("sum", ArrowDataType::Int64, false),
                ])),
                false,
            ),
        ]);

        assert_eq!(&expected_schema, projected_reader.schema().as_ref());

        for batch in projected_reader {
            let batch = batch.unwrap();
            assert_eq!(batch.schema().as_ref(), &expected_schema);
        }
    }

    #[test]
    fn test_read_maps() {
        let testdata = arrow::util::test_util::parquet_test_data();
        let path = format!("{testdata}/nested_maps.snappy.parquet");
        let file = File::open(path).unwrap();
        let record_batch_reader = ParquetRecordBatchReader::try_new(file, 60).unwrap();

        for batch in record_batch_reader {
            batch.unwrap();
        }
    }

    #[test]
    fn test_nested_nullability() {
        let message_type = "message nested {
          OPTIONAL Group group {
            REQUIRED INT32 leaf;
          }
        }";

        let file = tempfile::tempfile().unwrap();
        let schema = Arc::new(parse_message_type(message_type).unwrap());

        {
            // Write using low-level parquet API (#1167)
            let mut writer =
                SerializedFileWriter::new(file.try_clone().unwrap(), schema, Default::default())
                    .unwrap();

            {
                let mut row_group_writer = writer.next_row_group().unwrap();
                let mut column_writer = row_group_writer.next_column().unwrap().unwrap();

                column_writer
                    .typed::<Int32Type>()
                    .write_batch(&[34, 76], Some(&[0, 1, 0, 1]), None)
                    .unwrap();

                column_writer.close().unwrap();
                row_group_writer.close().unwrap();
            }

            writer.close().unwrap();
        }

        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let mask = ProjectionMask::leaves(builder.parquet_schema(), [0]);

        let reader = builder.with_projection(mask).build().unwrap();

        let expected_schema = Schema::new(Fields::from(vec![Field::new(
            "group",
            ArrowDataType::Struct(vec![Field::new("leaf", ArrowDataType::Int32, false)].into()),
            true,
        )]));

        let batch = reader.into_iter().next().unwrap().unwrap();
        assert_eq!(batch.schema().as_ref(), &expected_schema);
        assert_eq!(batch.num_rows(), 4);
        assert_eq!(batch.column(0).null_count(), 2);
    }

    #[test]
    fn test_invalid_utf8() {
        // a parquet file with 1 column with invalid utf8
        let data = vec![
            80, 65, 82, 49, 21, 6, 21, 22, 21, 22, 92, 21, 2, 21, 0, 21, 2, 21, 0, 21, 4, 21, 0,
            18, 28, 54, 0, 40, 5, 104, 101, 255, 108, 111, 24, 5, 104, 101, 255, 108, 111, 0, 0, 0,
            3, 1, 5, 0, 0, 0, 104, 101, 255, 108, 111, 38, 110, 28, 21, 12, 25, 37, 6, 0, 25, 24,
            2, 99, 49, 21, 0, 22, 2, 22, 102, 22, 102, 38, 8, 60, 54, 0, 40, 5, 104, 101, 255, 108,
            111, 24, 5, 104, 101, 255, 108, 111, 0, 0, 0, 21, 4, 25, 44, 72, 4, 114, 111, 111, 116,
            21, 2, 0, 21, 12, 37, 2, 24, 2, 99, 49, 37, 0, 76, 28, 0, 0, 0, 22, 2, 25, 28, 25, 28,
            38, 110, 28, 21, 12, 25, 37, 6, 0, 25, 24, 2, 99, 49, 21, 0, 22, 2, 22, 102, 22, 102,
            38, 8, 60, 54, 0, 40, 5, 104, 101, 255, 108, 111, 24, 5, 104, 101, 255, 108, 111, 0, 0,
            0, 22, 102, 22, 2, 0, 40, 44, 65, 114, 114, 111, 119, 50, 32, 45, 32, 78, 97, 116, 105,
            118, 101, 32, 82, 117, 115, 116, 32, 105, 109, 112, 108, 101, 109, 101, 110, 116, 97,
            116, 105, 111, 110, 32, 111, 102, 32, 65, 114, 114, 111, 119, 0, 130, 0, 0, 0, 80, 65,
            82, 49,
        ];

        let file = Bytes::from(data);
        let mut record_batch_reader = ParquetRecordBatchReader::try_new(file, 10).unwrap();

        let error = record_batch_reader.next().unwrap().unwrap_err();

        assert!(
            error.to_string().contains("invalid utf-8 sequence"),
            "{}",
            error
        );
    }

    #[test]
    fn test_invalid_utf8_string_array() {
        test_invalid_utf8_string_array_inner::<i32>();
    }

    #[test]
    fn test_invalid_utf8_large_string_array() {
        test_invalid_utf8_string_array_inner::<i64>();
    }

    fn test_invalid_utf8_string_array_inner<O: OffsetSizeTrait>() {
        let cases = [
            invalid_utf8_first_char::<O>(),
            invalid_utf8_first_char_long_strings::<O>(),
            invalid_utf8_later_char::<O>(),
            invalid_utf8_later_char_long_strings::<O>(),
            invalid_utf8_later_char_really_long_strings::<O>(),
            invalid_utf8_later_char_really_long_strings2::<O>(),
        ];
        for array in &cases {
            for encoding in STRING_ENCODINGS {
                // data is not valid utf8 we can not construct a correct StringArray
                // safely, so purposely create an invalid StringArray
                let array = unsafe {
                    GenericStringArray::<O>::new_unchecked(
                        array.offsets().clone(),
                        array.values().clone(),
                        array.nulls().cloned(),
                    )
                };
                let data_type = array.data_type().clone();
                let data = write_to_parquet_with_encoding(Arc::new(array), *encoding);
                let err = read_from_parquet(data).unwrap_err();
                let expected_err =
                    "Parquet argument error: Parquet error: encountered non UTF-8 data";
                assert!(
                    err.to_string().contains(expected_err),
                    "data type: {data_type:?}, expected: {expected_err}, got: {err}"
                );
            }
        }
    }

    #[test]
    fn test_invalid_utf8_string_view_array() {
        let cases = [
            invalid_utf8_first_char::<i32>(),
            invalid_utf8_first_char_long_strings::<i32>(),
            invalid_utf8_later_char::<i32>(),
            invalid_utf8_later_char_long_strings::<i32>(),
            invalid_utf8_later_char_really_long_strings::<i32>(),
            invalid_utf8_later_char_really_long_strings2::<i32>(),
        ];

        for encoding in STRING_ENCODINGS {
            for array in &cases {
                let array = arrow_cast::cast(&array, &ArrowDataType::BinaryView).unwrap();
                let array = array.as_binary_view();

                // data is not valid utf8 we can not construct a correct StringArray
                // safely, so purposely create an invalid StringViewArray
                let array = unsafe {
                    StringViewArray::new_unchecked(
                        array.views().clone(),
                        array.data_buffers().to_vec(),
                        array.nulls().cloned(),
                    )
                };

                let data_type = array.data_type().clone();
                let data = write_to_parquet_with_encoding(Arc::new(array), *encoding);
                let err = read_from_parquet(data).unwrap_err();
                let expected_err =
                    "Parquet argument error: Parquet error: encountered non UTF-8 data";
                assert!(
                    err.to_string().contains(expected_err),
                    "data type: {data_type:?}, expected: {expected_err}, got: {err}"
                );
            }
        }
    }

    /// Encodings suitable for string data
    const STRING_ENCODINGS: &[Option<Encoding>] = &[
        None,
        Some(Encoding::PLAIN),
        Some(Encoding::DELTA_LENGTH_BYTE_ARRAY),
        Some(Encoding::DELTA_BYTE_ARRAY),
    ];

    /// Invalid Utf-8 sequence in the first character
    /// <https://stackoverflow.com/questions/1301402/example-invalid-utf8-string>
    const INVALID_UTF8_FIRST_CHAR: &[u8] = &[0xa0, 0xa1, 0x20, 0x20];

    /// Invalid Utf=8 sequence in NOT the first character
    /// <https://stackoverflow.com/questions/1301402/example-invalid-utf8-string>
    const INVALID_UTF8_LATER_CHAR: &[u8] = &[0x20, 0x20, 0x20, 0xa0, 0xa1, 0x20, 0x20];

    /// returns a BinaryArray with invalid UTF8 data in the first character
    fn invalid_utf8_first_char<O: OffsetSizeTrait>() -> GenericBinaryArray<O> {
        let valid: &[u8] = b"   ";
        let invalid = INVALID_UTF8_FIRST_CHAR;
        GenericBinaryArray::<O>::from_iter(vec![None, Some(valid), None, Some(invalid)])
    }

    /// Returns a BinaryArray with invalid UTF8 data in the first character of a
    /// string larger than 12 bytes which is handled specially when reading
    /// `ByteViewArray`s
    fn invalid_utf8_first_char_long_strings<O: OffsetSizeTrait>() -> GenericBinaryArray<O> {
        let valid: &[u8] = b"   ";
        let mut invalid = vec![];
        invalid.extend_from_slice(b"ThisStringIsCertainlyLongerThan12Bytes");
        invalid.extend_from_slice(INVALID_UTF8_FIRST_CHAR);
        GenericBinaryArray::<O>::from_iter(vec![None, Some(valid), None, Some(&invalid)])
    }

    /// returns a BinaryArray with invalid UTF8 data in a character other than
    /// the first (this is checked in a special codepath)
    fn invalid_utf8_later_char<O: OffsetSizeTrait>() -> GenericBinaryArray<O> {
        let valid: &[u8] = b"   ";
        let invalid: &[u8] = INVALID_UTF8_LATER_CHAR;
        GenericBinaryArray::<O>::from_iter(vec![None, Some(valid), None, Some(invalid)])
    }

    /// returns a BinaryArray with invalid UTF8 data in a character other than
    /// the first in a string larger than 12 bytes which is handled specially
    /// when reading `ByteViewArray`s (this is checked in a special codepath)
    fn invalid_utf8_later_char_long_strings<O: OffsetSizeTrait>() -> GenericBinaryArray<O> {
        let valid: &[u8] = b"   ";
        let mut invalid = vec![];
        invalid.extend_from_slice(b"ThisStringIsCertainlyLongerThan12Bytes");
        invalid.extend_from_slice(INVALID_UTF8_LATER_CHAR);
        GenericBinaryArray::<O>::from_iter(vec![None, Some(valid), None, Some(&invalid)])
    }

    /// returns a BinaryArray with invalid UTF8 data in a character other than
    /// the first in a string larger than 128 bytes which is handled specially
    /// when reading `ByteViewArray`s (this is checked in a special codepath)
    fn invalid_utf8_later_char_really_long_strings<O: OffsetSizeTrait>() -> GenericBinaryArray<O> {
        let valid: &[u8] = b"   ";
        let mut invalid = vec![];
        for _ in 0..10 {
            // each instance is 38 bytes
            invalid.extend_from_slice(b"ThisStringIsCertainlyLongerThan12Bytes");
        }
        invalid.extend_from_slice(INVALID_UTF8_LATER_CHAR);
        GenericBinaryArray::<O>::from_iter(vec![None, Some(valid), None, Some(&invalid)])
    }

    /// returns a BinaryArray with small invalid UTF8 data followed by a large
    /// invalid UTF8 data in a character other than the first in a string larger
    fn invalid_utf8_later_char_really_long_strings2<O: OffsetSizeTrait>() -> GenericBinaryArray<O> {
        let valid: &[u8] = b"   ";
        let mut valid_long = vec![];
        for _ in 0..10 {
            // each instance is 38 bytes
            valid_long.extend_from_slice(b"ThisStringIsCertainlyLongerThan12Bytes");
        }
        let invalid = INVALID_UTF8_LATER_CHAR;
        GenericBinaryArray::<O>::from_iter(vec![
            None,
            Some(valid),
            Some(invalid),
            None,
            Some(&valid_long),
            Some(valid),
        ])
    }

    /// writes the array into a single column parquet file with the specified
    /// encoding.
    ///
    /// If no encoding is specified, use default (dictionary) encoding
    fn write_to_parquet_with_encoding(array: ArrayRef, encoding: Option<Encoding>) -> Vec<u8> {
        let batch = RecordBatch::try_from_iter(vec![("c", array)]).unwrap();
        let mut data = vec![];
        let schema = batch.schema();
        let props = encoding.map(|encoding| {
            WriterProperties::builder()
                // must disable dictionary encoding to actually use encoding
                .set_dictionary_enabled(false)
                .set_encoding(encoding)
                .build()
        });

        {
            let mut writer = ArrowWriter::try_new(&mut data, schema, props).unwrap();
            writer.write(&batch).unwrap();
            writer.flush().unwrap();
            writer.close().unwrap();
        };
        data
    }

    /// read the parquet file into a record batch
    fn read_from_parquet(data: Vec<u8>) -> Result<Vec<RecordBatch>, ArrowError> {
        let reader = ArrowReaderBuilder::try_new(bytes::Bytes::from(data))
            .unwrap()
            .build()
            .unwrap();

        reader.collect()
    }

    #[test]
    fn test_dictionary_preservation() {
        let fields = vec![Arc::new(
            Type::primitive_type_builder("leaf", PhysicalType::BYTE_ARRAY)
                .with_repetition(Repetition::OPTIONAL)
                .with_converted_type(ConvertedType::UTF8)
                .build()
                .unwrap(),
        )];

        let schema = Arc::new(
            Type::group_type_builder("test_schema")
                .with_fields(fields)
                .build()
                .unwrap(),
        );

        let dict_type = ArrowDataType::Dictionary(
            Box::new(ArrowDataType::Int32),
            Box::new(ArrowDataType::Utf8),
        );

        let arrow_field = Field::new("leaf", dict_type, true);

        let mut file = tempfile::tempfile().unwrap();

        let values = vec![
            vec![
                ByteArray::from("hello"),
                ByteArray::from("a"),
                ByteArray::from("b"),
                ByteArray::from("d"),
            ],
            vec![
                ByteArray::from("c"),
                ByteArray::from("a"),
                ByteArray::from("b"),
            ],
        ];

        let def_levels = vec![
            vec![1, 0, 0, 1, 0, 0, 1, 1],
            vec![0, 0, 1, 1, 0, 0, 1, 0, 0],
        ];

        let opts = TestOptions {
            encoding: Encoding::RLE_DICTIONARY,
            ..Default::default()
        };

        generate_single_column_file_with_data::<ByteArrayType>(
            &values,
            Some(&def_levels),
            file.try_clone().unwrap(), // Cannot use &mut File (#1163)
            schema,
            Some(arrow_field),
            &opts,
        )
        .unwrap();

        file.rewind().unwrap();

        let record_reader = ParquetRecordBatchReader::try_new(file, 3).unwrap();

        let batches = record_reader
            .collect::<Result<Vec<RecordBatch>, _>>()
            .unwrap();

        assert_eq!(batches.len(), 6);
        assert!(batches.iter().all(|x| x.num_columns() == 1));

        let row_counts = batches
            .iter()
            .map(|x| (x.num_rows(), x.column(0).null_count()))
            .collect::<Vec<_>>();

        assert_eq!(
            row_counts,
            vec![(3, 2), (3, 2), (3, 1), (3, 1), (3, 2), (2, 2)]
        );

        let get_dict = |batch: &RecordBatch| batch.column(0).to_data().child_data()[0].clone();

        // First and second batch in same row group -> same dictionary
        assert_eq!(get_dict(&batches[0]), get_dict(&batches[1]));
        // Third batch spans row group -> computed dictionary
        assert_ne!(get_dict(&batches[1]), get_dict(&batches[2]));
        assert_ne!(get_dict(&batches[2]), get_dict(&batches[3]));
        // Fourth, fifth and sixth from same row group -> same dictionary
        assert_eq!(get_dict(&batches[3]), get_dict(&batches[4]));
        assert_eq!(get_dict(&batches[4]), get_dict(&batches[5]));
    }

    #[test]
    fn test_read_null_list() {
        let testdata = arrow::util::test_util::parquet_test_data();
        let path = format!("{testdata}/null_list.parquet");
        let file = File::open(path).unwrap();
        let mut record_batch_reader = ParquetRecordBatchReader::try_new(file, 60).unwrap();

        let batch = record_batch_reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.column(0).len(), 1);

        let list = batch
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(list.len(), 1);
        assert!(list.is_valid(0));

        let val = list.value(0);
        assert_eq!(val.len(), 0);
    }

    #[test]
    fn test_null_schema_inference() {
        let testdata = arrow::util::test_util::parquet_test_data();
        let path = format!("{testdata}/null_list.parquet");
        let file = File::open(path).unwrap();

        let arrow_field = Field::new(
            "emptylist",
            ArrowDataType::List(Arc::new(Field::new_list_field(ArrowDataType::Null, true))),
            true,
        );

        let options = ArrowReaderOptions::new().with_skip_arrow_metadata(true);
        let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(file, options).unwrap();
        let schema = builder.schema();
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0), &arrow_field);
    }

    #[test]
    fn test_skip_metadata() {
        let col = Arc::new(TimestampNanosecondArray::from_iter_values(vec![0, 1, 2]));
        let field = Field::new("col", col.data_type().clone(), true);

        let schema_without_metadata = Arc::new(Schema::new(vec![field.clone()]));

        let metadata = [("key".to_string(), "value".to_string())]
            .into_iter()
            .collect();

        let schema_with_metadata = Arc::new(Schema::new(vec![field.with_metadata(metadata)]));

        assert_ne!(schema_with_metadata, schema_without_metadata);

        let batch =
            RecordBatch::try_new(schema_with_metadata.clone(), vec![col as ArrayRef]).unwrap();

        let file = |version: WriterVersion| {
            let props = WriterProperties::builder()
                .set_writer_version(version)
                .build();

            let file = tempfile().unwrap();
            let mut writer =
                ArrowWriter::try_new(file.try_clone().unwrap(), batch.schema(), Some(props))
                    .unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
            file
        };

        let skip_options = ArrowReaderOptions::new().with_skip_arrow_metadata(true);

        let v1_reader = file(WriterVersion::PARQUET_1_0);
        let v2_reader = file(WriterVersion::PARQUET_2_0);

        let arrow_reader =
            ParquetRecordBatchReader::try_new(v1_reader.try_clone().unwrap(), 1024).unwrap();
        assert_eq!(arrow_reader.schema(), schema_with_metadata);

        let reader =
            ParquetRecordBatchReaderBuilder::try_new_with_options(v1_reader, skip_options.clone())
                .unwrap()
                .build()
                .unwrap();
        assert_eq!(reader.schema(), schema_without_metadata);

        let arrow_reader =
            ParquetRecordBatchReader::try_new(v2_reader.try_clone().unwrap(), 1024).unwrap();
        assert_eq!(arrow_reader.schema(), schema_with_metadata);

        let reader = ParquetRecordBatchReaderBuilder::try_new_with_options(v2_reader, skip_options)
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(reader.schema(), schema_without_metadata);
    }

    fn write_parquet_from_iter<I, F>(value: I) -> File
    where
        I: IntoIterator<Item = (F, ArrayRef)>,
        F: AsRef<str>,
    {
        let batch = RecordBatch::try_from_iter(value).unwrap();
        let file = tempfile().unwrap();
        let mut writer =
            ArrowWriter::try_new(file.try_clone().unwrap(), batch.schema().clone(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        file
    }

    fn run_schema_test_with_error<I, F>(value: I, schema: SchemaRef, expected_error: &str)
    where
        I: IntoIterator<Item = (F, ArrayRef)>,
        F: AsRef<str>,
    {
        let file = write_parquet_from_iter(value);
        let options_with_schema = ArrowReaderOptions::new().with_schema(schema.clone());
        let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(
            file.try_clone().unwrap(),
            options_with_schema,
        );
        assert_eq!(builder.err().unwrap().to_string(), expected_error);
    }

    #[test]
    fn test_schema_too_few_columns() {
        run_schema_test_with_error(
            vec![
                ("int64", Arc::new(Int64Array::from(vec![0])) as ArrayRef),
                ("int32", Arc::new(Int32Array::from(vec![0])) as ArrayRef),
            ],
            Arc::new(Schema::new(vec![Field::new(
                "int64",
                ArrowDataType::Int64,
                false,
            )])),
            "Arrow: incompatible arrow schema, expected 2 struct fields got 1",
        );
    }

    #[test]
    fn test_schema_too_many_columns() {
        run_schema_test_with_error(
            vec![("int64", Arc::new(Int64Array::from(vec![0])) as ArrayRef)],
            Arc::new(Schema::new(vec![
                Field::new("int64", ArrowDataType::Int64, false),
                Field::new("int32", ArrowDataType::Int32, false),
            ])),
            "Arrow: incompatible arrow schema, expected 1 struct fields got 2",
        );
    }

    #[test]
    fn test_schema_mismatched_column_names() {
        run_schema_test_with_error(
            vec![("int64", Arc::new(Int64Array::from(vec![0])) as ArrayRef)],
            Arc::new(Schema::new(vec![Field::new(
                "other",
                ArrowDataType::Int64,
                false,
            )])),
            "Arrow: incompatible arrow schema, expected field named int64 got other",
        );
    }

    #[test]
    fn test_schema_incompatible_columns() {
        run_schema_test_with_error(
            vec![
                (
                    "col1_invalid",
                    Arc::new(Int64Array::from(vec![0])) as ArrayRef,
                ),
                (
                    "col2_valid",
                    Arc::new(Int32Array::from(vec![0])) as ArrayRef,
                ),
                (
                    "col3_invalid",
                    Arc::new(Date64Array::from(vec![0])) as ArrayRef,
                ),
            ],
            Arc::new(Schema::new(vec![
                Field::new("col1_invalid", ArrowDataType::Int32, false),
                Field::new("col2_valid", ArrowDataType::Int32, false),
                Field::new("col3_invalid", ArrowDataType::Int32, false),
            ])),
            "Arrow: incompatible arrow schema, the following fields could not be cast: [col1_invalid, col3_invalid]",
        );
    }

    #[test]
    fn test_one_incompatible_nested_column() {
        let nested_fields = Fields::from(vec![
            Field::new("nested1_valid", ArrowDataType::Utf8, false),
            Field::new("nested1_invalid", ArrowDataType::Int64, false),
        ]);
        let nested = StructArray::try_new(
            nested_fields,
            vec![
                Arc::new(StringArray::from(vec!["a"])) as ArrayRef,
                Arc::new(Int64Array::from(vec![0])) as ArrayRef,
            ],
            None,
        )
        .expect("struct array");
        let supplied_nested_fields = Fields::from(vec![
            Field::new("nested1_valid", ArrowDataType::Utf8, false),
            Field::new("nested1_invalid", ArrowDataType::Int32, false),
        ]);
        run_schema_test_with_error(
            vec![
                ("col1", Arc::new(Int64Array::from(vec![0])) as ArrayRef),
                ("col2", Arc::new(Int32Array::from(vec![0])) as ArrayRef),
                ("nested", Arc::new(nested) as ArrayRef),
            ],
            Arc::new(Schema::new(vec![
                Field::new("col1", ArrowDataType::Int64, false),
                Field::new("col2", ArrowDataType::Int32, false),
                Field::new(
                    "nested",
                    ArrowDataType::Struct(supplied_nested_fields),
                    false,
                ),
            ])),
            "Arrow: incompatible arrow schema, the following fields could not be cast: [nested]",
        );
    }

    #[test]
    fn test_read_binary_as_utf8() {
        let file = write_parquet_from_iter(vec![
            (
                "binary_to_utf8",
                Arc::new(BinaryArray::from(vec![
                    b"one".as_ref(),
                    b"two".as_ref(),
                    b"three".as_ref(),
                ])) as ArrayRef,
            ),
            (
                "large_binary_to_large_utf8",
                Arc::new(LargeBinaryArray::from(vec![
                    b"one".as_ref(),
                    b"two".as_ref(),
                    b"three".as_ref(),
                ])) as ArrayRef,
            ),
            (
                "binary_view_to_utf8_view",
                Arc::new(BinaryViewArray::from(vec![
                    b"one".as_ref(),
                    b"two".as_ref(),
                    b"three".as_ref(),
                ])) as ArrayRef,
            ),
        ]);
        let supplied_fields = Fields::from(vec![
            Field::new("binary_to_utf8", ArrowDataType::Utf8, false),
            Field::new(
                "large_binary_to_large_utf8",
                ArrowDataType::LargeUtf8,
                false,
            ),
            Field::new("binary_view_to_utf8_view", ArrowDataType::Utf8View, false),
        ]);

        let options = ArrowReaderOptions::new().with_schema(Arc::new(Schema::new(supplied_fields)));
        let mut arrow_reader = ParquetRecordBatchReaderBuilder::try_new_with_options(
            file.try_clone().unwrap(),
            options,
        )
        .expect("reader builder with schema")
        .build()
        .expect("reader with schema");

        let batch = arrow_reader.next().unwrap().unwrap();
        assert_eq!(batch.num_columns(), 3);
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(
            batch
                .column(0)
                .as_string::<i32>()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some("one"), Some("two"), Some("three")]
        );

        assert_eq!(
            batch
                .column(1)
                .as_string::<i64>()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some("one"), Some("two"), Some("three")]
        );

        assert_eq!(
            batch.column(2).as_string_view().iter().collect::<Vec<_>>(),
            vec![Some("one"), Some("two"), Some("three")]
        );
    }

    #[test]
    #[should_panic(expected = "Invalid UTF8 sequence at")]
    fn test_read_non_utf8_binary_as_utf8() {
        let file = write_parquet_from_iter(vec![(
            "non_utf8_binary",
            Arc::new(BinaryArray::from(vec![
                b"\xDE\x00\xFF".as_ref(),
                b"\xDE\x01\xAA".as_ref(),
                b"\xDE\x02\xFF".as_ref(),
            ])) as ArrayRef,
        )]);
        let supplied_fields = Fields::from(vec![Field::new(
            "non_utf8_binary",
            ArrowDataType::Utf8,
            false,
        )]);

        let options = ArrowReaderOptions::new().with_schema(Arc::new(Schema::new(supplied_fields)));
        let mut arrow_reader = ParquetRecordBatchReaderBuilder::try_new_with_options(
            file.try_clone().unwrap(),
            options,
        )
        .expect("reader builder with schema")
        .build()
        .expect("reader with schema");
        arrow_reader.next().unwrap().unwrap_err();
    }

    #[test]
    fn test_with_schema() {
        let nested_fields = Fields::from(vec![
            Field::new("utf8_to_dict", ArrowDataType::Utf8, false),
            Field::new("int64_to_ts_nano", ArrowDataType::Int64, false),
        ]);

        let nested_arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["a", "a", "a", "b"])) as ArrayRef,
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])) as ArrayRef,
        ];

        let nested = StructArray::try_new(nested_fields, nested_arrays, None).unwrap();

        let file = write_parquet_from_iter(vec![
            (
                "int32_to_ts_second",
                Arc::new(Int32Array::from(vec![0, 1, 2, 3])) as ArrayRef,
            ),
            (
                "date32_to_date64",
                Arc::new(Date32Array::from(vec![0, 1, 2, 3])) as ArrayRef,
            ),
            ("nested", Arc::new(nested) as ArrayRef),
        ]);

        let supplied_nested_fields = Fields::from(vec![
            Field::new(
                "utf8_to_dict",
                ArrowDataType::Dictionary(
                    Box::new(ArrowDataType::Int32),
                    Box::new(ArrowDataType::Utf8),
                ),
                false,
            ),
            Field::new(
                "int64_to_ts_nano",
                ArrowDataType::Timestamp(
                    arrow::datatypes::TimeUnit::Nanosecond,
                    Some("+10:00".into()),
                ),
                false,
            ),
        ]);

        let supplied_schema = Arc::new(Schema::new(vec![
            Field::new(
                "int32_to_ts_second",
                ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Second, Some("+01:00".into())),
                false,
            ),
            Field::new("date32_to_date64", ArrowDataType::Date64, false),
            Field::new(
                "nested",
                ArrowDataType::Struct(supplied_nested_fields),
                false,
            ),
        ]));

        let options = ArrowReaderOptions::new().with_schema(supplied_schema.clone());
        let mut arrow_reader = ParquetRecordBatchReaderBuilder::try_new_with_options(
            file.try_clone().unwrap(),
            options,
        )
        .expect("reader builder with schema")
        .build()
        .expect("reader with schema");

        assert_eq!(arrow_reader.schema(), supplied_schema);
        let batch = arrow_reader.next().unwrap().unwrap();
        assert_eq!(batch.num_columns(), 3);
        assert_eq!(batch.num_rows(), 4);
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .expect("downcast to timestamp second")
                .value_as_datetime_with_tz(0, "+01:00".parse().unwrap())
                .map(|v| v.to_string())
                .expect("value as datetime"),
            "1970-01-01 01:00:00 +01:00"
        );
        assert_eq!(
            batch
                .column(1)
                .as_any()
                .downcast_ref::<Date64Array>()
                .expect("downcast to date64")
                .value_as_date(0)
                .map(|v| v.to_string())
                .expect("value as date"),
            "1970-01-01"
        );

        let nested = batch
            .column(2)
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("downcast to struct");

        let nested_dict = nested
            .column(0)
            .as_any()
            .downcast_ref::<Int32DictionaryArray>()
            .expect("downcast to dictionary");

        assert_eq!(
            nested_dict
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("downcast to string")
                .iter()
                .collect::<Vec<_>>(),
            vec![Some("a"), Some("b")]
        );

        assert_eq!(
            nested_dict.keys().iter().collect::<Vec<_>>(),
            vec![Some(0), Some(0), Some(0), Some(1)]
        );

        assert_eq!(
            nested
                .column(1)
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .expect("downcast to timestamp nanosecond")
                .value_as_datetime_with_tz(0, "+10:00".parse().unwrap())
                .map(|v| v.to_string())
                .expect("value as datetime"),
            "1970-01-01 10:00:00.000000001 +10:00"
        );
    }

    #[test]
    fn test_empty_projection() {
        let testdata = arrow::util::test_util::parquet_test_data();
        let path = format!("{testdata}/alltypes_plain.parquet");
        let file = File::open(path).unwrap();

        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let file_metadata = builder.metadata().file_metadata();
        let expected_rows = file_metadata.num_rows() as usize;

        let mask = ProjectionMask::leaves(builder.parquet_schema(), []);
        let batch_reader = builder
            .with_projection(mask)
            .with_batch_size(2)
            .build()
            .unwrap();

        let mut total_rows = 0;
        for maybe_batch in batch_reader {
            let batch = maybe_batch.unwrap();
            total_rows += batch.num_rows();
            assert_eq!(batch.num_columns(), 0);
            assert!(batch.num_rows() <= 2);
        }

        assert_eq!(total_rows, expected_rows);
    }

    fn test_row_group_batch(row_group_size: usize, batch_size: usize) {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "list",
            ArrowDataType::List(Arc::new(Field::new_list_field(ArrowDataType::Int32, true))),
            true,
        )]));

        let mut buf = Vec::with_capacity(1024);

        let mut writer = ArrowWriter::try_new(
            &mut buf,
            schema.clone(),
            Some(
                WriterProperties::builder()
                    .set_max_row_group_size(row_group_size)
                    .build(),
            ),
        )
        .unwrap();
        for _ in 0..2 {
            let mut list_builder = ListBuilder::new(Int32Builder::with_capacity(batch_size));
            for _ in 0..(batch_size) {
                list_builder.append(true);
            }
            let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(list_builder.finish())])
                .unwrap();
            writer.write(&batch).unwrap();
        }
        writer.close().unwrap();

        let mut record_reader =
            ParquetRecordBatchReader::try_new(Bytes::from(buf), batch_size).unwrap();
        assert_eq!(
            batch_size,
            record_reader.next().unwrap().unwrap().num_rows()
        );
        assert_eq!(
            batch_size,
            record_reader.next().unwrap().unwrap().num_rows()
        );
    }

    #[test]
    fn test_row_group_exact_multiple() {
        const BATCH_SIZE: usize = REPETITION_LEVELS_BATCH_SIZE;
        test_row_group_batch(8, 8);
        test_row_group_batch(10, 8);
        test_row_group_batch(8, 10);
        test_row_group_batch(BATCH_SIZE, BATCH_SIZE);
        test_row_group_batch(BATCH_SIZE + 1, BATCH_SIZE);
        test_row_group_batch(BATCH_SIZE, BATCH_SIZE + 1);
        test_row_group_batch(BATCH_SIZE, BATCH_SIZE - 1);
        test_row_group_batch(BATCH_SIZE - 1, BATCH_SIZE);
    }

    /// Given a RecordBatch containing all the column data, return the expected batches given
    /// a `batch_size` and `selection`
    fn get_expected_batches(
        column: &RecordBatch,
        selection: &RowSelection,
        batch_size: usize,
    ) -> Vec<RecordBatch> {
        let mut expected_batches = vec![];

        let mut selection: VecDeque<_> = selection.clone().into();
        let mut row_offset = 0;
        let mut last_start = None;
        while row_offset < column.num_rows() && !selection.is_empty() {
            let mut batch_remaining = batch_size.min(column.num_rows() - row_offset);
            while batch_remaining > 0 && !selection.is_empty() {
                let (to_read, skip) = match selection.front_mut() {
                    Some(selection) if selection.row_count > batch_remaining => {
                        selection.row_count -= batch_remaining;
                        (batch_remaining, selection.skip)
                    }
                    Some(_) => {
                        let select = selection.pop_front().unwrap();
                        (select.row_count, select.skip)
                    }
                    None => break,
                };

                batch_remaining -= to_read;

                match skip {
                    true => {
                        if let Some(last_start) = last_start.take() {
                            expected_batches.push(column.slice(last_start, row_offset - last_start))
                        }
                        row_offset += to_read
                    }
                    false => {
                        last_start.get_or_insert(row_offset);
                        row_offset += to_read
                    }
                }
            }
        }

        if let Some(last_start) = last_start.take() {
            expected_batches.push(column.slice(last_start, row_offset - last_start))
        }

        // Sanity check, all batches except the final should be the batch size
        for batch in &expected_batches[..expected_batches.len() - 1] {
            assert_eq!(batch.num_rows(), batch_size);
        }

        expected_batches
    }

    fn create_test_selection(
        step_len: usize,
        total_len: usize,
        skip_first: bool,
    ) -> (RowSelection, usize) {
        let mut remaining = total_len;
        let mut skip = skip_first;
        let mut vec = vec![];
        let mut selected_count = 0;
        while remaining != 0 {
            let step = if remaining > step_len {
                step_len
            } else {
                remaining
            };
            vec.push(RowSelector {
                row_count: step,
                skip,
            });
            remaining -= step;
            if !skip {
                selected_count += step;
            }
            skip = !skip;
        }
        (vec.into(), selected_count)
    }

    #[test]
    fn test_scan_row_with_selection() {
        let testdata = arrow::util::test_util::parquet_test_data();
        let path = format!("{testdata}/alltypes_tiny_pages_plain.parquet");
        let test_file = File::open(&path).unwrap();

        let mut serial_reader =
            ParquetRecordBatchReader::try_new(File::open(&path).unwrap(), 7300).unwrap();
        let data = serial_reader.next().unwrap().unwrap();

        let do_test = |batch_size: usize, selection_len: usize| {
            for skip_first in [false, true] {
                let selections = create_test_selection(batch_size, data.num_rows(), skip_first).0;

                let expected = get_expected_batches(&data, &selections, batch_size);
                let skip_reader = create_skip_reader(&test_file, batch_size, selections);
                assert_eq!(
                    skip_reader.collect::<Result<Vec<_>, _>>().unwrap(),
                    expected,
                    "batch_size: {batch_size}, selection_len: {selection_len}, skip_first: {skip_first}"
                );
            }
        };

        // total row count 7300
        // 1. test selection len more than one page row count
        do_test(1000, 1000);

        // 2. test selection len less than one page row count
        do_test(20, 20);

        // 3. test selection_len less than batch_size
        do_test(20, 5);

        // 4. test selection_len more than batch_size
        // If batch_size < selection_len
        do_test(20, 5);

        fn create_skip_reader(
            test_file: &File,
            batch_size: usize,
            selections: RowSelection,
        ) -> ParquetRecordBatchReader {
            let options = ArrowReaderOptions::new().with_page_index(true);
            let file = test_file.try_clone().unwrap();
            ParquetRecordBatchReaderBuilder::try_new_with_options(file, options)
                .unwrap()
                .with_batch_size(batch_size)
                .with_row_selection(selections)
                .build()
                .unwrap()
        }
    }

    #[test]
    fn test_batch_size_overallocate() {
        let testdata = arrow::util::test_util::parquet_test_data();
        // `alltypes_plain.parquet` only have 8 rows
        let path = format!("{testdata}/alltypes_plain.parquet");
        let test_file = File::open(path).unwrap();

        let builder = ParquetRecordBatchReaderBuilder::try_new(test_file).unwrap();
        let num_rows = builder.metadata.file_metadata().num_rows();
        let reader = builder
            .with_batch_size(1024)
            .with_projection(ProjectionMask::all())
            .build()
            .unwrap();
        assert_ne!(1024, num_rows);
        assert_eq!(reader.batch_size, num_rows as usize);
    }

    #[test]
    fn test_read_with_page_index_enabled() {
        let testdata = arrow::util::test_util::parquet_test_data();

        {
            // `alltypes_tiny_pages.parquet` has page index
            let path = format!("{testdata}/alltypes_tiny_pages.parquet");
            let test_file = File::open(path).unwrap();
            let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(
                test_file,
                ArrowReaderOptions::new().with_page_index(true),
            )
            .unwrap();
            assert!(!builder.metadata().offset_index().unwrap()[0].is_empty());
            let reader = builder.build().unwrap();
            let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
            assert_eq!(batches.len(), 8);
        }

        {
            // `alltypes_plain.parquet` doesn't have page index
            let path = format!("{testdata}/alltypes_plain.parquet");
            let test_file = File::open(path).unwrap();
            let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(
                test_file,
                ArrowReaderOptions::new().with_page_index(true),
            )
            .unwrap();
            // Although `Vec<Vec<PageLoacation>>` of each row group is empty,
            // we should read the file successfully.
            assert!(builder.metadata().offset_index().is_none());
            let reader = builder.build().unwrap();
            let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
            assert_eq!(batches.len(), 1);
        }
    }

    #[test]
    fn test_raw_repetition() {
        const MESSAGE_TYPE: &str = "
            message Log {
              OPTIONAL INT32 eventType;
              REPEATED INT32 category;
              REPEATED group filter {
                OPTIONAL INT32 error;
              }
            }
        ";
        let schema = Arc::new(parse_message_type(MESSAGE_TYPE).unwrap());
        let props = Default::default();

        let mut buf = Vec::with_capacity(1024);
        let mut writer = SerializedFileWriter::new(&mut buf, schema, props).unwrap();
        let mut row_group_writer = writer.next_row_group().unwrap();

        // column 0
        let mut col_writer = row_group_writer.next_column().unwrap().unwrap();
        col_writer
            .typed::<Int32Type>()
            .write_batch(&[1], Some(&[1]), None)
            .unwrap();
        col_writer.close().unwrap();
        // column 1
        let mut col_writer = row_group_writer.next_column().unwrap().unwrap();
        col_writer
            .typed::<Int32Type>()
            .write_batch(&[1, 1], Some(&[1, 1]), Some(&[0, 1]))
            .unwrap();
        col_writer.close().unwrap();
        // column 2
        let mut col_writer = row_group_writer.next_column().unwrap().unwrap();
        col_writer
            .typed::<Int32Type>()
            .write_batch(&[1], Some(&[1]), Some(&[0]))
            .unwrap();
        col_writer.close().unwrap();

        let rg_md = row_group_writer.close().unwrap();
        assert_eq!(rg_md.num_rows(), 1);
        writer.close().unwrap();

        let bytes = Bytes::from(buf);

        let mut no_mask = ParquetRecordBatchReader::try_new(bytes.clone(), 1024).unwrap();
        let full = no_mask.next().unwrap().unwrap();

        assert_eq!(full.num_columns(), 3);

        for idx in 0..3 {
            let b = ParquetRecordBatchReaderBuilder::try_new(bytes.clone()).unwrap();
            let mask = ProjectionMask::leaves(b.parquet_schema(), [idx]);
            let mut reader = b.with_projection(mask).build().unwrap();
            let projected = reader.next().unwrap().unwrap();

            assert_eq!(projected.num_columns(), 1);
            assert_eq!(full.column(idx), projected.column(0));
        }
    }

    #[test]
    fn test_read_lz4_raw() {
        let testdata = arrow::util::test_util::parquet_test_data();
        let path = format!("{testdata}/lz4_raw_compressed.parquet");
        let file = File::open(path).unwrap();

        let batches = ParquetRecordBatchReader::try_new(file, 1024)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];

        assert_eq!(batch.num_columns(), 3);
        assert_eq!(batch.num_rows(), 4);

        // https://github.com/apache/parquet-testing/pull/18
        let a: &Int64Array = batch.column(0).as_any().downcast_ref().unwrap();
        assert_eq!(
            a.values(),
            &[1593604800, 1593604800, 1593604801, 1593604801]
        );

        let a: &BinaryArray = batch.column(1).as_any().downcast_ref().unwrap();
        let a: Vec<_> = a.iter().flatten().collect();
        assert_eq!(a, &[b"abc", b"def", b"abc", b"def"]);

        let a: &Float64Array = batch.column(2).as_any().downcast_ref().unwrap();
        assert_eq!(a.values(), &[42.000000, 7.700000, 42.125000, 7.700000]);
    }

    // This test is to ensure backward compatibility, it test 2 files containing the LZ4 CompressionCodec
    // but different algorithms: LZ4_HADOOP and LZ4_RAW.
    // 1. hadoop_lz4_compressed.parquet -> It is a file with LZ4 CompressionCodec which uses
    //    LZ4_HADOOP algorithm for compression.
    // 2. non_hadoop_lz4_compressed.parquet -> It is a file with LZ4 CompressionCodec which uses
    //    LZ4_RAW algorithm for compression. This fallback is done to keep backward compatibility with
    //    older parquet-cpp versions.
    //
    // For more information, check: https://github.com/apache/arrow-rs/issues/2988
    #[test]
    fn test_read_lz4_hadoop_fallback() {
        for file in [
            "hadoop_lz4_compressed.parquet",
            "non_hadoop_lz4_compressed.parquet",
        ] {
            let testdata = arrow::util::test_util::parquet_test_data();
            let path = format!("{testdata}/{file}");
            let file = File::open(path).unwrap();
            let expected_rows = 4;

            let batches = ParquetRecordBatchReader::try_new(file, expected_rows)
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(batches.len(), 1);
            let batch = &batches[0];

            assert_eq!(batch.num_columns(), 3);
            assert_eq!(batch.num_rows(), expected_rows);

            let a: &Int64Array = batch.column(0).as_any().downcast_ref().unwrap();
            assert_eq!(
                a.values(),
                &[1593604800, 1593604800, 1593604801, 1593604801]
            );

            let b: &BinaryArray = batch.column(1).as_any().downcast_ref().unwrap();
            let b: Vec<_> = b.iter().flatten().collect();
            assert_eq!(b, &[b"abc", b"def", b"abc", b"def"]);

            let c: &Float64Array = batch.column(2).as_any().downcast_ref().unwrap();
            assert_eq!(c.values(), &[42.0, 7.7, 42.125, 7.7]);
        }
    }

    #[test]
    fn test_read_lz4_hadoop_large() {
        let testdata = arrow::util::test_util::parquet_test_data();
        let path = format!("{testdata}/hadoop_lz4_compressed_larger.parquet");
        let file = File::open(path).unwrap();
        let expected_rows = 10000;

        let batches = ParquetRecordBatchReader::try_new(file, expected_rows)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];

        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.num_rows(), expected_rows);

        let a: &StringArray = batch.column(0).as_any().downcast_ref().unwrap();
        let a: Vec<_> = a.iter().flatten().collect();
        assert_eq!(a[0], "c7ce6bef-d5b0-4863-b199-8ea8c7fb117b");
        assert_eq!(a[1], "e8fb9197-cb9f-4118-b67f-fbfa65f61843");
        assert_eq!(a[expected_rows - 2], "ab52a0cc-c6bb-4d61-8a8f-166dc4b8b13c");
        assert_eq!(a[expected_rows - 1], "85440778-460a-41ac-aa2e-ac3ee41696bf");
    }

    #[test]
    #[cfg(feature = "snap")]
    fn test_read_nested_lists() {
        let testdata = arrow::util::test_util::parquet_test_data();
        let path = format!("{testdata}/nested_lists.snappy.parquet");
        let file = File::open(path).unwrap();

        let f = file.try_clone().unwrap();
        let mut reader = ParquetRecordBatchReader::try_new(f, 60).unwrap();
        let expected = reader.next().unwrap().unwrap();
        assert_eq!(expected.num_rows(), 3);

        let selection = RowSelection::from(vec![
            RowSelector::skip(1),
            RowSelector::select(1),
            RowSelector::skip(1),
        ]);
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .with_row_selection(selection)
            .build()
            .unwrap();

        let actual = reader.next().unwrap().unwrap();
        assert_eq!(actual.num_rows(), 1);
        assert_eq!(actual.column(0), &expected.column(0).slice(1, 1));
    }

    #[test]
    fn test_arbitrary_decimal() {
        let values = [1, 2, 3, 4, 5, 6, 7, 8];
        let decimals_19_0 = Decimal128Array::from_iter_values(values)
            .with_precision_and_scale(19, 0)
            .unwrap();
        let decimals_12_0 = Decimal128Array::from_iter_values(values)
            .with_precision_and_scale(12, 0)
            .unwrap();
        let decimals_17_10 = Decimal128Array::from_iter_values(values)
            .with_precision_and_scale(17, 10)
            .unwrap();

        let written = RecordBatch::try_from_iter([
            ("decimal_values_19_0", Arc::new(decimals_19_0) as ArrayRef),
            ("decimal_values_12_0", Arc::new(decimals_12_0) as ArrayRef),
            ("decimal_values_17_10", Arc::new(decimals_17_10) as ArrayRef),
        ])
        .unwrap();

        let mut buffer = Vec::with_capacity(1024);
        let mut writer = ArrowWriter::try_new(&mut buffer, written.schema(), None).unwrap();
        writer.write(&written).unwrap();
        writer.close().unwrap();

        let read = ParquetRecordBatchReader::try_new(Bytes::from(buffer), 8)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(&written.slice(0, 8), &read[0]);
    }

    #[test]
    fn test_list_skip() {
        let mut list = ListBuilder::new(Int32Builder::new());
        list.append_value([Some(1), Some(2)]);
        list.append_value([Some(3)]);
        list.append_value([Some(4)]);
        let list = list.finish();
        let batch = RecordBatch::try_from_iter([("l", Arc::new(list) as _)]).unwrap();

        // First page contains 2 values but only 1 row
        let props = WriterProperties::builder()
            .set_data_page_row_count_limit(1)
            .set_write_batch_size(2)
            .build();

        let mut buffer = Vec::with_capacity(1024);
        let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let selection = vec![RowSelector::skip(2), RowSelector::select(1)];
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(buffer))
            .unwrap()
            .with_row_selection(selection.into())
            .build()
            .unwrap();
        let out = reader.next().unwrap().unwrap();
        assert_eq!(out.num_rows(), 1);
        assert_eq!(out, batch.slice(2, 1));
    }

    fn test_decimal_roundtrip<T: DecimalType>() {
        // Precision <= 9 -> INT32
        // Precision <= 18 -> INT64
        // Precision > 18 -> FIXED_LEN_BYTE_ARRAY

        let d = |values: Vec<usize>, p: u8| {
            let iter = values.into_iter().map(T::Native::usize_as);
            PrimitiveArray::<T>::from_iter_values(iter)
                .with_precision_and_scale(p, 2)
                .unwrap()
        };

        let d1 = d(vec![1, 2, 3, 4, 5], 9);
        let d2 = d(vec![1, 2, 3, 4, 10.pow(10) - 1], 10);
        let d3 = d(vec![1, 2, 3, 4, 10.pow(18) - 1], 18);
        let d4 = d(vec![1, 2, 3, 4, 10.pow(19) - 1], 19);

        let batch = RecordBatch::try_from_iter([
            ("d1", Arc::new(d1) as ArrayRef),
            ("d2", Arc::new(d2) as ArrayRef),
            ("d3", Arc::new(d3) as ArrayRef),
            ("d4", Arc::new(d4) as ArrayRef),
        ])
        .unwrap();

        let mut buffer = Vec::with_capacity(1024);
        let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(buffer)).unwrap();
        let t1 = builder.parquet_schema().columns()[0].physical_type();
        assert_eq!(t1, PhysicalType::INT32);
        let t2 = builder.parquet_schema().columns()[1].physical_type();
        assert_eq!(t2, PhysicalType::INT64);
        let t3 = builder.parquet_schema().columns()[2].physical_type();
        assert_eq!(t3, PhysicalType::INT64);
        let t4 = builder.parquet_schema().columns()[3].physical_type();
        assert_eq!(t4, PhysicalType::FIXED_LEN_BYTE_ARRAY);

        let mut reader = builder.build().unwrap();
        assert_eq!(batch.schema(), reader.schema());

        let out = reader.next().unwrap().unwrap();
        assert_eq!(batch, out);
    }

    #[test]
    fn test_decimal() {
        test_decimal_roundtrip::<Decimal128Type>();
        test_decimal_roundtrip::<Decimal256Type>();
    }

    #[test]
    fn test_list_selection() {
        let schema = Arc::new(Schema::new(vec![Field::new_list(
            "list",
            Field::new_list_field(ArrowDataType::Utf8, true),
            false,
        )]));
        let mut buf = Vec::with_capacity(1024);

        let mut writer = ArrowWriter::try_new(&mut buf, schema.clone(), None).unwrap();

        for i in 0..2 {
            let mut list_a_builder = ListBuilder::new(StringBuilder::new());
            for j in 0..1024 {
                list_a_builder.values().append_value(format!("{i} {j}"));
                list_a_builder.append(true);
            }
            let batch =
                RecordBatch::try_new(schema.clone(), vec![Arc::new(list_a_builder.finish())])
                    .unwrap();
            writer.write(&batch).unwrap();
        }
        let _metadata = writer.close().unwrap();

        let buf = Bytes::from(buf);
        let reader = ParquetRecordBatchReaderBuilder::try_new(buf)
            .unwrap()
            .with_row_selection(RowSelection::from(vec![
                RowSelector::skip(100),
                RowSelector::select(924),
                RowSelector::skip(100),
                RowSelector::select(924),
            ]))
            .build()
            .unwrap();

        let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
        let batch = concat_batches(&schema, &batches).unwrap();

        assert_eq!(batch.num_rows(), 924 * 2);
        let list = batch.column(0).as_list::<i32>();

        for w in list.value_offsets().windows(2) {
            assert_eq!(w[0] + 1, w[1])
        }
        let mut values = list.values().as_string::<i32>().iter();

        for i in 0..2 {
            for j in 100..1024 {
                let expected = format!("{i} {j}");
                assert_eq!(values.next().unwrap().unwrap(), &expected);
            }
        }
    }

    #[test]
    fn test_list_selection_fuzz() {
        let mut rng = rng();
        let schema = Arc::new(Schema::new(vec![Field::new_list(
            "list",
            Field::new_list(
                Field::LIST_FIELD_DEFAULT_NAME,
                Field::new_list_field(ArrowDataType::Int32, true),
                true,
            ),
            true,
        )]));
        let mut buf = Vec::with_capacity(1024);
        let mut writer = ArrowWriter::try_new(&mut buf, schema.clone(), None).unwrap();

        let mut list_a_builder = ListBuilder::new(ListBuilder::new(Int32Builder::new()));

        for _ in 0..2048 {
            if rng.random_bool(0.2) {
                list_a_builder.append(false);
                continue;
            }

            let list_a_len = rng.random_range(0..10);
            let list_b_builder = list_a_builder.values();

            for _ in 0..list_a_len {
                if rng.random_bool(0.2) {
                    list_b_builder.append(false);
                    continue;
                }

                let list_b_len = rng.random_range(0..10);
                let int_builder = list_b_builder.values();
                for _ in 0..list_b_len {
                    match rng.random_bool(0.2) {
                        true => int_builder.append_null(),
                        false => int_builder.append_value(rng.random()),
                    }
                }
                list_b_builder.append(true)
            }
            list_a_builder.append(true);
        }

        let array = Arc::new(list_a_builder.finish());
        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();

        writer.write(&batch).unwrap();
        let _metadata = writer.close().unwrap();

        let buf = Bytes::from(buf);

        let cases = [
            vec![
                RowSelector::skip(100),
                RowSelector::select(924),
                RowSelector::skip(100),
                RowSelector::select(924),
            ],
            vec![
                RowSelector::select(924),
                RowSelector::skip(100),
                RowSelector::select(924),
                RowSelector::skip(100),
            ],
            vec![
                RowSelector::skip(1023),
                RowSelector::select(1),
                RowSelector::skip(1023),
                RowSelector::select(1),
            ],
            vec![
                RowSelector::select(1),
                RowSelector::skip(1023),
                RowSelector::select(1),
                RowSelector::skip(1023),
            ],
        ];

        for batch_size in [100, 1024, 2048] {
            for selection in &cases {
                let selection = RowSelection::from(selection.clone());
                let reader = ParquetRecordBatchReaderBuilder::try_new(buf.clone())
                    .unwrap()
                    .with_row_selection(selection.clone())
                    .with_batch_size(batch_size)
                    .build()
                    .unwrap();

                let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
                let actual = concat_batches(batch.schema_ref(), &batches).unwrap();
                assert_eq!(actual.num_rows(), selection.row_count());

                let mut batch_offset = 0;
                let mut actual_offset = 0;
                for selector in selection.iter() {
                    if selector.skip {
                        batch_offset += selector.row_count;
                        continue;
                    }

                    assert_eq!(
                        batch.slice(batch_offset, selector.row_count),
                        actual.slice(actual_offset, selector.row_count)
                    );

                    batch_offset += selector.row_count;
                    actual_offset += selector.row_count;
                }
            }
        }
    }

    #[test]
    fn test_read_old_nested_list() {
        use arrow::datatypes::DataType;
        use arrow::datatypes::ToByteSlice;

        let testdata = arrow::util::test_util::parquet_test_data();
        // message my_record {
        //     REQUIRED group a (LIST) {
        //         REPEATED group array (LIST) {
        //             REPEATED INT32 array;
        //         }
        //     }
        // }
        // should be read as list<list<int32>>
        let path = format!("{testdata}/old_list_structure.parquet");
        let test_file = File::open(path).unwrap();

        // create expected ListArray
        let a_values = Int32Array::from(vec![1, 2, 3, 4]);

        // Construct a buffer for value offsets, for the nested array: [[1, 2], [3, 4]]
        let a_value_offsets = arrow::buffer::Buffer::from([0, 2, 4].to_byte_slice());

        // Construct a list array from the above two
        let a_list_data = ArrayData::builder(DataType::List(Arc::new(Field::new(
            "array",
            DataType::Int32,
            false,
        ))))
        .len(2)
        .add_buffer(a_value_offsets)
        .add_child_data(a_values.into_data())
        .build()
        .unwrap();
        let a = ListArray::from(a_list_data);

        let builder = ParquetRecordBatchReaderBuilder::try_new(test_file).unwrap();
        let mut reader = builder.build().unwrap();
        let out = reader.next().unwrap().unwrap();
        assert_eq!(out.num_rows(), 1);
        assert_eq!(out.num_columns(), 1);
        // grab first column
        let c0 = out.column(0);
        let c0arr = c0.as_any().downcast_ref::<ListArray>().unwrap();
        // get first row: [[1, 2], [3, 4]]
        let r0 = c0arr.value(0);
        let r0arr = r0.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(r0arr, &a);
    }

    #[test]
    fn test_map_no_value() {
        // File schema:
        // message schema {
        //   required group my_map (MAP) {
        //     repeated group key_value {
        //       required int32 key;
        //       optional int32 value;
        //     }
        //   }
        //   required group my_map_no_v (MAP) {
        //     repeated group key_value {
        //       required int32 key;
        //     }
        //   }
        //   required group my_list (LIST) {
        //     repeated group list {
        //       required int32 element;
        //     }
        //   }
        // }
        let testdata = arrow::util::test_util::parquet_test_data();
        let path = format!("{testdata}/map_no_value.parquet");
        let file = File::open(path).unwrap();

        let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let out = reader.next().unwrap().unwrap();
        assert_eq!(out.num_rows(), 3);
        assert_eq!(out.num_columns(), 3);
        // my_map_no_v and my_list columns should now be equivalent
        let c0 = out.column(1).as_list::<i32>();
        let c1 = out.column(2).as_list::<i32>();
        assert_eq!(c0.len(), c1.len());
        c0.iter().zip(c1.iter()).for_each(|(l, r)| assert_eq!(l, r));
    }
}
