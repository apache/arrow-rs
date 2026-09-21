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

use arrow_array::cast::AsArray;
use arrow_array::{BooleanArray, RecordBatch, RecordBatchReader};
use arrow_buffer::{BooleanBuffer, BooleanBufferBuilder};
use arrow_schema::{ArrowError, DataType as ArrowType, FieldRef, Schema, SchemaRef};
use arrow_select::filter::filter_record_batch;
pub use filter::{ArrowPredicate, ArrowPredicateFn, RowFilter};
use selection::MaskCursor;
pub use selection::{
    MaskRunIter, RowSelection, RowSelectionCursor, RowSelectionPolicy, RowSelector,
};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

pub use crate::arrow::array_reader::RowGroups;
use crate::arrow::array_reader::{ArrayReader, ArrayReaderBuilder};
use crate::arrow::schema::{
    ParquetField, parquet_to_arrow_schema_and_fields, virtual_type::is_virtual_column,
};
use crate::arrow::{FieldLevels, ProjectionMask, parquet_to_arrow_field_levels_with_virtual};
use crate::basic::{BloomFilterAlgorithm, BloomFilterCompression, BloomFilterHash};
use crate::bloom_filter::{
    SBBF_HEADER_SIZE_ESTIMATE, Sbbf, chunk_read_bloom_filter_header_and_offset,
};
use crate::column::page::{PageIterator, PageReader};
#[cfg(feature = "encryption")]
use crate::encryption::decrypt::FileDecryptionProperties;
use crate::errors::{ParquetError, Result};
use crate::file::metadata::{
    PageIndexPolicy, ParquetMetaData, ParquetMetaDataOptions, ParquetMetaDataReader,
    ParquetStatisticsPolicy, RowGroupMetaData,
};
use crate::file::reader::{ChunkReader, SerializedPageReader};
use crate::schema::types::SchemaDescriptor;

use crate::arrow::arrow_reader::metrics::ArrowReaderMetrics;
// Exposed so integration tests and benchmarks can temporarily override the threshold.
pub use read_plan::{PredicateOptions, ReadPlan, ReadPlanBuilder};

mod filter;
pub mod metrics;
mod read_plan;
pub(crate) mod selection;
pub mod statistics;

/// Default batch size for reading parquet files
pub const DEFAULT_BATCH_SIZE: usize = 1024;

/// A row group and its optional row-group-local [`RowSelection`].
///
/// A row-group-local selection is relative to the rows in this row group. For
/// example, an offset of 100 refers to the row at offset 100 within the row
/// group, not within the Parquet file.
///
/// A `None` selection reads the entire row group. Omitting a row group skips
/// it. Entries are decoded in the supplied order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowGroupSelection {
    pub(crate) row_group_index: usize,
    pub(crate) selection: Option<RowSelection>,
}

impl RowGroupSelection {
    /// Creates a row-group-local selection.
    pub fn new(row_group_index: usize, selection: Option<RowSelection>) -> Self {
        Self {
            row_group_index,
            selection,
        }
    }

    /// The index of the row group this selection applies to.
    pub fn row_group_index(&self) -> usize {
        self.row_group_index
    }

    /// The row-group-local selection, or `None` if the entire row group is
    /// read.
    pub fn selection(&self) -> Option<&RowSelection> {
        self.selection.as_ref()
    }
}

/// Row-selection configuration shared by the Arrow reader builders.
#[derive(Debug)]
pub(crate) enum RowGroupPlan {
    /// First select `row_groups`, if provided, and then apply `selection`
    /// across the concatenated rows from those row groups.
    ///
    /// This is formed by [`ArrowReaderBuilder::with_row_groups`] and
    /// [`ArrowReaderBuilder::with_row_selection`].
    Global {
        row_groups: Option<Vec<usize>>,
        selection: Option<RowSelection>,
    },
    /// Apply each row-group-local selection independently, in the order
    /// supplied.
    ///
    /// This is formed by `with_row_group_selections` on the push decoder and
    /// async stream builders.
    PerRowGroup(Vec<RowGroupSelection>),
    /// Mutually exclusive global and per-row-group configuration was supplied.
    /// This is reported as an error when the reader is built.
    Conflicting,
}

impl RowGroupPlan {
    fn set_row_groups(&mut self, new_row_groups: Vec<usize>) {
        match self {
            Self::Global { row_groups, .. } => *row_groups = Some(new_row_groups),
            Self::PerRowGroup(_) => *self = Self::Conflicting,
            Self::Conflicting => {}
        }
    }

    fn set_row_selection(&mut self, new_selection: RowSelection) {
        match self {
            Self::Global { selection, .. } => *selection = Some(new_selection),
            Self::PerRowGroup(_) => *self = Self::Conflicting,
            Self::Conflicting => {}
        }
    }

    pub(crate) fn set_row_group_selections(
        &mut self,
        row_group_selections: Vec<RowGroupSelection>,
    ) {
        match self {
            Self::Global {
                row_groups: None,
                selection: None,
            }
            | Self::PerRowGroup(_) => {
                *self = Self::PerRowGroup(row_group_selections);
            }
            Self::Global { .. } => *self = Self::Conflicting,
            Self::Conflicting => {}
        }
    }

    pub(crate) fn conflict_error() -> ParquetError {
        ParquetError::General(
            "with_row_group_selections cannot be combined with with_row_groups or with_row_selection"
                .to_string(),
        )
    }

    fn into_global(self) -> Result<(Option<Vec<usize>>, Option<RowSelection>)> {
        match self {
            Self::Global {
                row_groups,
                selection,
            } => Ok((row_groups, selection)),
            Self::PerRowGroup(_) => Err(ParquetError::General(
                "Row-group-local selections are not supported by the synchronous reader"
                    .to_string(),
            )),
            Self::Conflicting => Err(Self::conflict_error()),
        }
    }
}

/// Builder for constructing Parquet readers that decode into [Apache Arrow]
/// arrays.
///
/// Most users should use one of the following specializations:
///
/// * synchronous API: [`ParquetRecordBatchReaderBuilder`]
/// * `async` API: [`ParquetRecordBatchStreamBuilder`]
/// * decoder API: [`ParquetPushDecoderBuilder`]
///
/// # Features
/// * Projection pushdown: [`Self::with_projection`]
/// * Cached metadata: [`ArrowReaderMetadata::load`]
/// * Offset skipping: [`Self::with_offset`] and [`Self::with_limit`]
/// * Row group filtering: [`Self::with_row_groups`]
/// * Range filtering: [`Self::with_row_selection`]
/// * Row level filtering: [`Self::with_row_filter`]
///
/// # Implementing Predicate Pushdown
///
/// [`Self::with_row_filter`] permits filter evaluation *during* the decoding
/// process, which is efficient and allows the most low level optimizations.
///
/// However, most Parquet based systems will apply filters at many steps prior
/// to decoding such as pruning files, row groups and data pages. This crate
/// provides the low level APIs needed to implement such filtering, but does not
/// include any logic to actually evaluate predicates. For example:
///
/// * [`Self::with_row_groups`] for Row Group pruning
/// * [`Self::with_row_selection`] for data page pruning
/// * [`StatisticsConverter`] to convert Parquet statistics to Arrow arrays
///
/// The rationale for this design is that implementing predicate pushdown is a
/// complex topic and varies significantly from system to system. For example
///
/// 1. Predicates supported (do you support predicates like prefix matching, user defined functions, etc)
/// 2. Evaluating predicates on multiple files (with potentially different but compatible schemas)
/// 3. Evaluating predicates using information from an external metadata catalog (e.g. Apache Iceberg or similar)
/// 4. Interleaving fetching metadata, evaluating predicates, and decoding files
///
/// You can read more about this design in the [Querying Parquet with
/// Millisecond Latency] Arrow blog post.
///
/// [`ParquetRecordBatchStreamBuilder`]: crate::arrow::async_reader::ParquetRecordBatchStreamBuilder
/// [`ParquetPushDecoderBuilder`]: crate::arrow::push_decoder::ParquetPushDecoderBuilder
/// [Apache Arrow]: https://arrow.apache.org/
/// [`StatisticsConverter`]: statistics::StatisticsConverter
/// [Querying Parquet with Millisecond Latency]: https://arrow.apache.org/blog/2022/12/26/querying-parquet-with-millisecond-latency/
pub struct ArrowReaderBuilder<T> {
    /// The "input" to read parquet data from.
    ///
    /// Note in the case of the [`ParquetPushDecoderBuilder`] there is no
    /// underlying reader; the input is instead [`PushDecoderInput`], the buffer that
    /// caller-pushed bytes accumulate in.
    ///
    /// [`ParquetPushDecoderBuilder`]: crate::arrow::push_decoder::ParquetPushDecoderBuilder
    /// [`PushDecoderInput`]: crate::arrow::push_decoder::PushDecoderInput
    pub(crate) input: T,

    pub(crate) metadata: Arc<ParquetMetaData>,

    pub(crate) schema: SchemaRef,

    pub(crate) fields: Option<Arc<ParquetField>>,

    pub(crate) batch_size: usize,

    pub(crate) row_group_plan: RowGroupPlan,

    pub(crate) projection: ProjectionMask,

    pub(crate) filter: Option<RowFilter>,

    pub(crate) row_selection_policy: RowSelectionPolicy,

    pub(crate) limit: Option<usize>,

    pub(crate) offset: Option<usize>,

    pub(crate) metrics: ArrowReaderMetrics,

    pub(crate) max_predicate_cache_size: usize,
}

impl<T: Debug> Debug for ArrowReaderBuilder<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArrowReaderBuilder<T>")
            .field("input", &self.input)
            .field("metadata", &self.metadata)
            .field("schema", &self.schema)
            .field("fields", &self.fields)
            .field("batch_size", &self.batch_size)
            .field("row_group_plan", &self.row_group_plan)
            .field("projection", &self.projection)
            .field("filter", &self.filter)
            .field("row_selection_policy", &self.row_selection_policy)
            .field("limit", &self.limit)
            .field("offset", &self.offset)
            .field("metrics", &self.metrics)
            .finish()
    }
}

impl<T> ArrowReaderBuilder<T> {
    pub(crate) fn new_builder(input: T, metadata: ArrowReaderMetadata) -> Self {
        Self {
            input,
            metadata: metadata.metadata,
            schema: metadata.schema,
            fields: metadata.fields,
            batch_size: DEFAULT_BATCH_SIZE,
            row_group_plan: RowGroupPlan::Global {
                row_groups: None,
                selection: None,
            },
            projection: ProjectionMask::all(),
            filter: None,
            row_selection_policy: RowSelectionPolicy::default(),
            limit: None,
            offset: None,
            metrics: ArrowReaderMetrics::Disabled,
            max_predicate_cache_size: 100 * 1024 * 1024, // 100MB default cache size
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

    /// Set the size of [`RecordBatch`] to produce. Defaults to [`DEFAULT_BATCH_SIZE`].
    ///
    /// This may be used as a hint for internal allocations, but does not
    /// guarantee exact internal buffer capacities.
    ///
    /// If `batch_size` is more than the file row count, use the file row count.
    pub fn with_batch_size(self, batch_size: usize) -> Self {
        // Try to avoid allocate large buffer
        let batch_size = batch_size.min(self.metadata.file_metadata().num_rows() as usize);
        Self { batch_size, ..self }
    }

    /// Only read data from the provided row group indexes
    ///
    /// This is also called row group filtering
    ///
    /// On [`ParquetPushDecoderBuilder`] and [`ParquetRecordBatchStreamBuilder`],
    /// which additionally offer `with_row_group_selections`, this cannot be
    /// combined with that method; attempting to do so returns an error from
    /// `build`.
    ///
    /// [`ParquetPushDecoderBuilder`]: crate::arrow::push_decoder::ParquetPushDecoderBuilder
    /// [`ParquetRecordBatchStreamBuilder`]: crate::arrow::async_reader::ParquetRecordBatchStreamBuilder
    pub fn with_row_groups(mut self, row_groups: Vec<usize>) -> Self {
        self.row_group_plan.set_row_groups(row_groups);
        self
    }

    /// Only read data from the provided column indexes
    pub fn with_projection(self, mask: ProjectionMask) -> Self {
        Self {
            projection: mask,
            ..self
        }
    }

    /// Configure how row selections should be materialised during execution
    ///
    /// See [`RowSelectionPolicy`] for more details
    pub fn with_row_selection_policy(self, policy: RowSelectionPolicy) -> Self {
        Self {
            row_selection_policy: policy,
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
    /// On [`ParquetPushDecoderBuilder`] and [`ParquetRecordBatchStreamBuilder`],
    /// which additionally offer `with_row_group_selections`, this cannot be
    /// combined with that method; attempting to do so returns an error from
    /// `build`.
    ///
    /// [`ParquetPushDecoderBuilder`]: crate::arrow::push_decoder::ParquetPushDecoderBuilder
    /// [`ParquetRecordBatchStreamBuilder`]: crate::arrow::async_reader::ParquetRecordBatchStreamBuilder
    ///
    /// It is recommended to enable writing the page index if using this
    /// functionality, to allow more efficient skipping over data pages. See
    /// [`ArrowReaderOptions::with_page_index_policy`].
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
    /// [`Index`]: crate::file::page_index::column_index::ColumnIndexMetaData
    pub fn with_row_selection(mut self, selection: RowSelection) -> Self {
        self.row_group_plan.set_row_selection(selection);
        self
    }

    /// Provide a [`RowFilter`] to skip decoding rows
    ///
    /// Row filters are applied after row group selection and row selection
    ///
    /// It is recommended to enable reading the page index if using this functionality, to allow
    /// more efficient skipping over data pages. See [`ArrowReaderOptions::with_page_index_policy`].
    ///
    /// See the [blog post on late materialization] for a more technical explanation.
    ///
    /// [blog post on late materialization]: https://arrow.apache.org/blog/2025/12/11/parquet-late-materialization-deep-dive
    ///
    /// # Example
    /// ```rust
    /// # use std::fs::File;
    /// # use arrow_array::Int32Array;
    /// # use parquet::arrow::ProjectionMask;
    /// # use parquet::arrow::arrow_reader::{ArrowPredicateFn, ParquetRecordBatchReaderBuilder, RowFilter};
    /// # fn main() -> Result<(), parquet::errors::ParquetError> {
    /// # let testdata = arrow::util::test_util::parquet_test_data();
    /// # let path = format!("{testdata}/alltypes_plain.parquet");
    /// # let file = File::open(&path)?;
    /// let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    /// let schema_desc = builder.metadata().file_metadata().schema_descr_ptr();
    /// // Create predicate that evaluates `int_col != 1`.
    /// // `int_col` column has index 4 (zero based) in the schema
    /// let projection = ProjectionMask::leaves(&schema_desc, [4]);
    /// // Only the projection columns are passed to the predicate so
    /// // int_col is column 0 in the predicate
    /// let predicate = ArrowPredicateFn::new(projection, |batch| {
    ///     let int_col = batch.column(0);
    ///     arrow::compute::kernels::cmp::neq(int_col, &Int32Array::new_scalar(1))
    /// });
    /// let row_filter = RowFilter::new(vec![Box::new(predicate)]);
    /// // The filter will be invoked during the reading process
    /// let reader = builder.with_row_filter(row_filter).build()?;
    /// # for b in reader { let _ = b?; }
    /// # Ok(())
    /// # }
    /// ```
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
    /// more efficient skipping over data pages. See [`ArrowReaderOptions::with_page_index_policy`]
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
    /// more efficient skipping over data pages. See [`ArrowReaderOptions::with_page_index_policy`]
    pub fn with_offset(self, offset: usize) -> Self {
        Self {
            offset: Some(offset),
            ..self
        }
    }

    /// Specify metrics collection during reading
    ///
    /// To access the metrics, create an [`ArrowReaderMetrics`] and pass a
    /// clone of the provided metrics to the builder.
    ///
    /// For example:
    ///
    /// ```rust
    /// # use std::sync::Arc;
    /// # use bytes::Bytes;
    /// # use arrow_array::{Int32Array, RecordBatch};
    /// # use arrow_schema::{DataType, Field, Schema};
    /// # use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
    /// use parquet::arrow::arrow_reader::metrics::ArrowReaderMetrics;
    /// # use parquet::arrow::ArrowWriter;
    /// # let mut file: Vec<u8> = Vec::with_capacity(1024);
    /// # let schema = Arc::new(Schema::new(vec![Field::new("i32", DataType::Int32, false)]));
    /// # let mut writer = ArrowWriter::try_new(&mut file, schema.clone(), None).unwrap();
    /// # let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap();
    /// # writer.write(&batch).unwrap();
    /// # writer.close().unwrap();
    /// # let file = Bytes::from(file);
    /// // Create metrics object to pass into the reader
    /// let metrics = ArrowReaderMetrics::enabled();
    /// let reader = ParquetRecordBatchReaderBuilder::try_new(file).unwrap()
    ///   // Configure the builder to use the metrics by passing a clone
    ///   .with_metrics(metrics.clone())
    ///   // Build the reader
    ///   .build().unwrap();
    /// // .. read data from the reader ..
    ///
    /// // check the metrics
    /// assert!(metrics.records_read_from_inner().is_some());
    /// ```
    pub fn with_metrics(self, metrics: ArrowReaderMetrics) -> Self {
        Self { metrics, ..self }
    }

    /// Set the maximum size (per row group) of the predicate cache in bytes for
    /// the async decoder.
    ///
    /// Defaults to 100MB (across all columns). Set to `usize::MAX` to use
    /// unlimited cache size.
    ///
    /// This cache is used to store decoded arrays that are used in
    /// predicate evaluation ([`Self::with_row_filter`]).
    ///
    /// This cache is only used for the "async" decoder, [`ParquetRecordBatchStream`]. See
    /// [this ticket] for more details and alternatives.
    ///
    /// [`ParquetRecordBatchStream`]: https://docs.rs/parquet/latest/parquet/arrow/async_reader/struct.ParquetRecordBatchStream.html
    /// [this ticket]: https://github.com/apache/arrow-rs/issues/8000
    pub fn with_max_predicate_cache_size(self, max_predicate_cache_size: usize) -> Self {
        Self {
            max_predicate_cache_size,
            ..self
        }
    }
}

/// Options that control how [`ParquetMetaData`] is read when constructing
/// an Arrow reader.
///
/// To use these options, pass them to one of the following methods:
/// * [`ParquetRecordBatchReaderBuilder::try_new_with_options`]
/// * [`ParquetRecordBatchStreamBuilder::new_with_options`]
///
/// For fine-grained control over metadata loading, use
/// [`ArrowReaderMetadata::load`] to load metadata with these options,
///
/// See [`ArrowReaderBuilder`] for how to configure how the column data
/// is then read from the file, including projection and filter pushdown
///
/// [`ParquetRecordBatchStreamBuilder::new_with_options`]: crate::arrow::async_reader::ParquetRecordBatchStreamBuilder::new_with_options
#[derive(Debug, Clone, Default)]
pub struct ArrowReaderOptions {
    /// Should the reader strip any user defined metadata from the Arrow schema
    skip_arrow_metadata: bool,
    /// If provided, used as the schema hint when determining the Arrow schema,
    /// otherwise the schema hint is read from the [ARROW_SCHEMA_META_KEY]
    ///
    /// [ARROW_SCHEMA_META_KEY]: crate::arrow::ARROW_SCHEMA_META_KEY
    supplied_schema: Option<SchemaRef>,

    pub(crate) column_index: PageIndexPolicy,
    pub(crate) offset_index: PageIndexPolicy,

    /// Options to control reading of Parquet metadata
    metadata_options: ParquetMetaDataOptions,
    /// If encryption is enabled, the file decryption properties can be provided
    #[cfg(feature = "encryption")]
    pub(crate) file_decryption_properties: Option<Arc<FileDecryptionProperties>>,

    virtual_columns: Vec<FieldRef>,
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
    /// for example, see [ARROW-16184](https://issues.apache.org/jira/browse/ARROW-16184)
    pub fn with_skip_arrow_metadata(self, skip_arrow_metadata: bool) -> Self {
        Self {
            skip_arrow_metadata,
            ..self
        }
    }

    /// Provide a schema hint to use when reading the Parquet file.
    ///
    /// If provided, this schema takes precedence over any arrow schema embedded
    /// in the metadata (see the [`arrow`] documentation for more details).
    ///
    /// If the provided schema is not compatible with the data stored in the
    /// parquet file schema, an error will be returned when constructing the
    /// builder.
    ///
    /// This option is only required if you want to explicitly control the
    /// conversion of Parquet types to Arrow types, such as casting a column to
    /// a different type. For example, if you wanted to read an Int64 in
    /// a Parquet file to a [`TimestampMicrosecondArray`] in the Arrow schema.
    ///
    /// [`arrow`]: crate::arrow
    /// [`TimestampMicrosecondArray`]: arrow_array::TimestampMicrosecondArray
    ///
    /// # Notes
    ///
    /// The provided schema must have the same number of columns as the parquet schema and
    /// the column names must be the same.
    ///
    /// # Example
    /// ```
    /// # use std::sync::Arc;
    /// # use bytes::Bytes;
    /// # use arrow_array::{ArrayRef, Int32Array, RecordBatch};
    /// # use arrow_schema::{DataType, Field, Schema, TimeUnit};
    /// # use parquet::arrow::arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
    /// # use parquet::arrow::ArrowWriter;
    /// // Write data - schema is inferred from the data to be Int32
    /// let mut file = Vec::new();
    /// let batch = RecordBatch::try_from_iter(vec![
    ///     ("col_1", Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef),
    /// ]).unwrap();
    /// let mut writer = ArrowWriter::try_new(&mut file, batch.schema(), None).unwrap();
    /// writer.write(&batch).unwrap();
    /// writer.close().unwrap();
    /// let file = Bytes::from(file);
    ///
    /// // Read the file back.
    /// // Supply a schema that interprets the Int32 column as a Timestamp.
    /// let supplied_schema = Arc::new(Schema::new(vec![
    ///     Field::new("col_1", DataType::Timestamp(TimeUnit::Nanosecond, None), false)
    /// ]));
    /// let options = ArrowReaderOptions::new().with_schema(supplied_schema.clone());
    /// let mut builder = ParquetRecordBatchReaderBuilder::try_new_with_options(
    ///     file.clone(),
    ///     options
    /// ).expect("Error if the schema is not compatible with the parquet file schema.");
    ///
    /// // Create the reader and read the data using the supplied schema.
    /// let mut reader = builder.build().unwrap();
    /// let _batch = reader.next().unwrap().unwrap();
    /// ```
    ///
    /// # Example: Preserving Dictionary Encoding
    ///
    /// By default, Parquet string columns are read as `Utf8Array` (or `LargeUtf8Array`),
    /// even if the underlying Parquet data uses dictionary encoding. You can preserve
    /// the dictionary encoding by specifying a `Dictionary` type in the schema hint:
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use bytes::Bytes;
    /// # use arrow_array::{ArrayRef, RecordBatch, StringArray};
    /// # use arrow_schema::{DataType, Field, Schema};
    /// # use parquet::arrow::arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
    /// # use parquet::arrow::ArrowWriter;
    /// // Write a Parquet file with string data
    /// let mut file = Vec::new();
    /// let schema = Arc::new(Schema::new(vec![
    ///     Field::new("city", DataType::Utf8, false)
    /// ]));
    /// let cities = StringArray::from(vec!["Berlin", "Berlin", "Paris", "Berlin", "Paris"]);
    /// let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(cities)]).unwrap();
    ///
    /// let mut writer = ArrowWriter::try_new(&mut file, batch.schema(), None).unwrap();
    /// writer.write(&batch).unwrap();
    /// writer.close().unwrap();
    /// let file = Bytes::from(file);
    ///
    /// // Read the file back, requesting dictionary encoding preservation
    /// let dict_schema = Arc::new(Schema::new(vec![
    ///     Field::new("city", DataType::Dictionary(
    ///         Box::new(DataType::Int32),
    ///         Box::new(DataType::Utf8)
    ///     ), false)
    /// ]));
    /// let options = ArrowReaderOptions::new().with_schema(dict_schema);
    /// let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(
    ///     file.clone(),
    ///     options
    /// ).unwrap();
    ///
    /// let mut reader = builder.build().unwrap();
    /// let batch = reader.next().unwrap().unwrap();
    ///
    /// // The column is now a DictionaryArray
    /// assert!(matches!(
    ///     batch.column(0).data_type(),
    ///     DataType::Dictionary(_, _)
    /// ));
    /// ```
    ///
    /// **Note**: Dictionary encoding preservation works best when:
    /// 1. The original column was dictionary encoded (the default for string columns)
    /// 2. There are a small number of distinct values
    pub fn with_schema(self, schema: SchemaRef) -> Self {
        Self {
            supplied_schema: Some(schema),
            skip_arrow_metadata: true,
            ..self
        }
    }

    /// Sets the [`PageIndexPolicy`] for both the column and offset indexes.
    ///
    /// The `PageIndex` can be used to push down predicates to the parquet scan,
    /// potentially eliminating unnecessary IO, by some query engines.
    /// The `PageIndex` consists of two structures: the `ColumnIndex` and `OffsetIndex`.
    /// This method sets the same policy for both. For fine-grained control, use
    /// [`Self::with_column_index_policy`] and [`Self::with_offset_index_policy`].
    pub fn with_page_index_policy(self, policy: PageIndexPolicy) -> Self {
        self.with_column_index_policy(policy)
            .with_offset_index_policy(policy)
    }

    /// Sets the [`PageIndexPolicy`] for the Parquet [ColumnIndex] structure.
    ///
    /// The `ColumnIndex` contains min/max statistics for each page, which can be used
    /// for predicate pushdown and page-level pruning.
    ///
    /// [ColumnIndex]: https://github.com/apache/parquet-format/blob/master/PageIndex.md
    pub fn with_column_index_policy(mut self, policy: PageIndexPolicy) -> Self {
        self.column_index = policy;
        self
    }

    /// Sets the [`PageIndexPolicy`] for the Parquet [OffsetIndex] structure.
    ///
    /// The `OffsetIndex` contains the locations and sizes of each page, which enables
    /// efficient page-level skipping and random access within column chunks.
    ///
    /// [OffsetIndex]: https://github.com/apache/parquet-format/blob/master/PageIndex.md
    pub fn with_offset_index_policy(mut self, policy: PageIndexPolicy) -> Self {
        self.offset_index = policy;
        self
    }

    /// Provide a Parquet schema to use when decoding the metadata. The schema in the Parquet
    /// footer will be skipped.
    ///
    /// This can be used to avoid reparsing the schema from the file when it is
    /// already known.
    pub fn with_parquet_schema(mut self, schema: Arc<SchemaDescriptor>) -> Self {
        self.metadata_options.set_schema(schema);
        self
    }

    /// Set whether to convert the [`encoding_stats`] in the Parquet `ColumnMetaData` to a bitmask
    /// (defaults to `false`).
    ///
    /// See [`ColumnChunkMetaData::page_encoding_stats_mask`] for an explanation of why this
    /// might be desirable.
    ///
    /// [`ColumnChunkMetaData::page_encoding_stats_mask`]:
    /// crate::file::metadata::ColumnChunkMetaData::page_encoding_stats_mask
    /// [`encoding_stats`]:
    /// https://github.com/apache/parquet-format/blob/786142e26740487930ddc3ec5e39d780bd930907/src/main/thrift/parquet.thrift#L917
    pub fn with_encoding_stats_as_mask(mut self, val: bool) -> Self {
        self.metadata_options.set_encoding_stats_as_mask(val);
        self
    }

    /// Sets the decoding policy for [`encoding_stats`] in the Parquet `ColumnMetaData`.
    ///
    /// [`encoding_stats`]:
    /// https://github.com/apache/parquet-format/blob/786142e26740487930ddc3ec5e39d780bd930907/src/main/thrift/parquet.thrift#L917
    pub fn with_encoding_stats_policy(mut self, policy: ParquetStatisticsPolicy) -> Self {
        self.metadata_options.set_encoding_stats_policy(policy);
        self
    }

    /// Sets the decoding policy for [`statistics`] in the Parquet `ColumnMetaData`.
    ///
    /// [`statistics`]:
    /// https://github.com/apache/parquet-format/blob/786142e26740487930ddc3ec5e39d780bd930907/src/main/thrift/parquet.thrift#L912
    pub fn with_column_stats_policy(mut self, policy: ParquetStatisticsPolicy) -> Self {
        self.metadata_options.set_column_stats_policy(policy);
        self
    }

    /// Sets the decoding policy for [`size_statistics`] in the Parquet `ColumnMetaData`.
    ///
    /// [`size_statistics`]:
    /// https://github.com/apache/parquet-format/blob/786142e26740487930ddc3ec5e39d780bd930907/src/main/thrift/parquet.thrift#L936
    pub fn with_size_stats_policy(mut self, policy: ParquetStatisticsPolicy) -> Self {
        self.metadata_options.set_size_stats_policy(policy);
        self
    }

    /// Provide the file decryption properties to use when reading encrypted parquet files.
    ///
    /// If encryption is enabled and the file is encrypted, the `file_decryption_properties` must be provided.
    #[cfg(feature = "encryption")]
    pub fn with_file_decryption_properties(
        self,
        file_decryption_properties: Arc<FileDecryptionProperties>,
    ) -> Self {
        Self {
            file_decryption_properties: Some(file_decryption_properties),
            ..self
        }
    }

    /// Include virtual columns in the output.
    ///
    /// Virtual columns are columns that are not part of the Parquet schema, but are added to the output by the reader such as row numbers and row group indices.
    ///
    /// # Example
    /// ```
    /// # use std::sync::Arc;
    /// # use bytes::Bytes;
    /// # use arrow_array::{ArrayRef, Int64Array, RecordBatch};
    /// # use arrow_schema::{DataType, Field, Schema};
    /// # use parquet::arrow::{ArrowWriter, RowNumber};
    /// # use parquet::arrow::arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
    /// #
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// // Create a simple record batch with some data
    /// let values = Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef;
    /// let batch = RecordBatch::try_from_iter(vec![("value", values)])?;
    ///
    /// // Write the batch to an in-memory buffer
    /// let mut file = Vec::new();
    /// let mut writer = ArrowWriter::try_new(
    ///     &mut file,
    ///     batch.schema(),
    ///     None
    /// )?;
    /// writer.write(&batch)?;
    /// writer.close()?;
    /// let file = Bytes::from(file);
    ///
    /// // Create a virtual column for row numbers
    /// let row_number_field = Arc::new(Field::new("row_number", DataType::Int64, false)
    ///     .with_extension_type(RowNumber));
    ///
    /// // Configure options with virtual columns
    /// let options = ArrowReaderOptions::new()
    ///     .with_virtual_columns(vec![row_number_field])?;
    ///
    /// // Create a reader with the options
    /// let mut reader = ParquetRecordBatchReaderBuilder::try_new_with_options(
    ///     file,
    ///     options
    /// )?
    /// .build()?;
    ///
    /// // Read the batch - it will include both the original column and the virtual row_number column
    /// let result_batch = reader.next().unwrap()?;
    /// assert_eq!(result_batch.num_columns(), 2); // "value" + "row_number"
    /// assert_eq!(result_batch.num_rows(), 3);
    /// #
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_virtual_columns(self, virtual_columns: Vec<FieldRef>) -> Result<Self> {
        // Validate that all fields are virtual columns
        for field in &virtual_columns {
            if !is_virtual_column(field) {
                return Err(ParquetError::General(format!(
                    "Field '{}' is not a virtual column. Virtual columns must have extension type names starting with 'arrow.virtual.'",
                    field.name()
                )));
            }
        }
        Ok(Self {
            virtual_columns,
            ..self
        })
    }

    /// Retrieve the currently set [`PageIndexPolicy`] for the offset index.
    ///
    /// This can be set via [`with_offset_index_policy`][Self::with_offset_index_policy]
    /// or [`with_page_index_policy`][Self::with_page_index_policy].
    pub fn offset_index_policy(&self) -> PageIndexPolicy {
        self.offset_index
    }

    /// Retrieve the currently set [`PageIndexPolicy`] for the column index.
    ///
    /// This can be set via [`with_column_index_policy`][Self::with_column_index_policy]
    /// or [`with_page_index_policy`][Self::with_page_index_policy].
    pub fn column_index_policy(&self) -> PageIndexPolicy {
        self.column_index
    }

    /// Retrieve the currently set metadata decoding options.
    pub fn metadata_options(&self) -> &ParquetMetaDataOptions {
        &self.metadata_options
    }

    /// Retrieve the currently set file decryption properties.
    ///
    /// This can be set via
    /// [`file_decryption_properties`][Self::with_file_decryption_properties].
    #[cfg(feature = "encryption")]
    pub fn file_decryption_properties(&self) -> Option<&Arc<FileDecryptionProperties>> {
        self.file_decryption_properties.as_ref()
    }
}

impl ParquetMetaDataReader {
    /// Applies the metadata related settings from [`ArrowReaderOptions`],
    /// such as the [`ParquetMetaDataOptions`], decryption properties, and
    /// [`PageIndexPolicy`] to this reader.
    ///
    /// The page index policies are only applied if at least one of them is not
    /// [`PageIndexPolicy::Skip`], so policies previously configured on this
    /// reader (e.g. from a preload setting) are preserved when the options do
    /// not request the page index.
    ///
    /// This encodes the canonical way to construct a `ParquetMetaDataReader`
    /// inside `AsyncFileReader::get_metadata` (available with the `async`
    /// feature), so implementations outside this crate do not need to
    /// duplicate it.
    pub fn with_arrow_reader_options(mut self, options: Option<&ArrowReaderOptions>) -> Self {
        let Some(options) = options else { return self };

        self = self.with_metadata_options(Some(options.metadata_options().clone()));

        #[cfg(feature = "encryption")]
        {
            self = self.with_decryption_properties(
                options.file_decryption_properties.as_ref().map(Arc::clone),
            );
        }

        if options.column_index_policy() != PageIndexPolicy::Skip
            || options.offset_index_policy() != PageIndexPolicy::Skip
        {
            self = self
                .with_column_index_policy(options.column_index_policy())
                .with_offset_index_policy(options.offset_index_policy());
        }

        self
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
    /// The Parquet schema (root field)
    pub(crate) fields: Option<Arc<ParquetField>>,
}

impl ArrowReaderMetadata {
    /// Create [`ArrowReaderMetadata`] from the provided [`ArrowReaderOptions`]
    /// and [`ChunkReader`]
    ///
    /// See [`ParquetRecordBatchReaderBuilder::new_with_metadata`] for an
    /// example of how this can be used
    ///
    /// # Notes
    ///
    /// If `options` indicates the page index should be read, but
    /// `Self::metadata` is missing the page index, this function will attempt
    /// to load the page index by making an object store request.
    ///
    /// See [`ArrowReaderOptions::with_page_index_policy`] for more information on the page index.
    pub fn load<T: ChunkReader>(reader: &T, options: ArrowReaderOptions) -> Result<Self> {
        let metadata = ParquetMetaDataReader::new()
            .with_column_index_policy(options.column_index_policy())
            .with_offset_index_policy(options.offset_index_policy())
            .with_metadata_options(Some(options.metadata_options.clone()));
        #[cfg(feature = "encryption")]
        let metadata = metadata.with_decryption_properties(
            options.file_decryption_properties.as_ref().map(Arc::clone),
        );
        let metadata = metadata.parse_and_finish(reader)?;
        Self::try_new(Arc::new(metadata), options)
    }

    /// Create a new [`ArrowReaderMetadata`] from a pre-existing
    /// [`ParquetMetaData`] and [`ArrowReaderOptions`].
    ///
    /// # Notes
    ///
    /// This function will not attempt to load the PageIndex if not present in the metadata, regardless
    /// of the settings in `options`. See [`Self::load`] to load metadata including the page index if needed.
    pub fn try_new(metadata: Arc<ParquetMetaData>, options: ArrowReaderOptions) -> Result<Self> {
        match options.supplied_schema {
            Some(supplied_schema) => Self::with_supplied_schema(
                metadata,
                supplied_schema.clone(),
                &options.virtual_columns,
            ),
            None => {
                let kv_metadata = match options.skip_arrow_metadata {
                    true => None,
                    false => metadata.file_metadata().key_value_metadata(),
                };

                let (schema, fields) = parquet_to_arrow_schema_and_fields(
                    metadata.file_metadata().schema_descr(),
                    ProjectionMask::all(),
                    kv_metadata,
                    &options.virtual_columns,
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
        virtual_columns: &[FieldRef],
    ) -> Result<Self> {
        let parquet_schema = metadata.file_metadata().schema_descr();
        let field_levels = parquet_to_arrow_field_levels_with_virtual(
            parquet_schema,
            ProjectionMask::all(),
            Some(supplied_schema.fields()),
            virtual_columns,
        )?;
        let fields = field_levels.fields;
        let inferred_len = fields.len();
        let supplied_len = supplied_schema.fields().len() + virtual_columns.len();
        // Ensure the supplied schema has the same number of columns as the parquet schema.
        // parquet_to_arrow_field_levels is expected to throw an error if the schemas have
        // different lengths, but we check here to be safe.
        if inferred_len != supplied_len {
            return Err(arrow_err!(format!(
                "Incompatible supplied Arrow schema: expected {} columns received {}",
                inferred_len, supplied_len
            )));
        }

        let mut errors = Vec::new();

        let field_iter = supplied_schema.fields().iter().zip(fields.iter());

        for (field1, field2) in field_iter {
            if field1.data_type() != field2.data_type() {
                errors.push(format!(
                    "data type mismatch for field {}: requested {} but found {}",
                    field1.name(),
                    field1.data_type(),
                    field2.data_type()
                ));
            }
            if field1.is_nullable() != field2.is_nullable() {
                errors.push(format!(
                    "nullability mismatch for field {}: expected {:?} but found {:?}",
                    field1.name(),
                    field1.is_nullable(),
                    field2.is_nullable()
                ));
            }
            if field1.metadata() != field2.metadata() {
                errors.push(format!(
                    "metadata mismatch for field {}: expected {:?} but found {:?}",
                    field1.name(),
                    field1.metadata(),
                    field2.metadata()
                ));
            }
        }

        if !errors.is_empty() {
            let message = errors.join(", ");
            return Err(ParquetError::ArrowError(format!(
                "Incompatible supplied Arrow schema: {message}",
            )));
        }

        // `fields` is the supplied fields followed by the virtual columns, so the reported
        // schema has to be built from it or the virtual columns go missing.
        let schema = if virtual_columns.is_empty() {
            supplied_schema
        } else {
            Arc::new(Schema::new_with_metadata(
                fields,
                supplied_schema.metadata().clone(),
            ))
        };

        Ok(Self {
            metadata,
            schema,
            fields: field_levels.levels.map(Arc::new),
        })
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
// A newtype used within `ReaderOptionsBuilder` to distinguish sync readers from async
pub struct SyncReader<T: ChunkReader>(T);

impl<T: Debug + ChunkReader> Debug for SyncReader<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("SyncReader").field(&self.0).finish()
    }
}

/// Creates [`ParquetRecordBatchReader`] for reading Parquet files into Arrow [`RecordBatch`]es
///
/// # See Also
/// * [`crate::arrow::async_reader::ParquetRecordBatchStreamBuilder`] for an async API
/// * [`crate::arrow::push_decoder::ParquetPushDecoderBuilder`] for a SansIO decoder API
/// * [`ArrowReaderBuilder`] for additional member functions
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
    /// // Build the reader from anything that implements `ChunkReader`
    /// // such as a `File`, or `Bytes`
    /// let mut builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    /// // The builder has access to ParquetMetaData such
    /// // as the number and layout of row groups
    /// assert_eq!(builder.metadata().num_row_groups(), 1);
    /// // Call build to create the reader
    /// let mut reader: ParquetRecordBatchReader = builder.build().unwrap();
    /// // Read data
    /// while let Some(batch) = reader.next().transpose()? {
    ///     println!("Read {} rows", batch.num_rows());
    /// }
    /// # Ok::<(), parquet::errors::ParquetError>(())
    /// ```
    pub fn try_new(reader: T) -> Result<Self> {
        Self::try_new_with_options(reader, Default::default())
    }

    /// Create a new [`ParquetRecordBatchReaderBuilder`] with [`ArrowReaderOptions`]
    ///
    /// Use this method if you want to control the options for reading the
    /// [`ParquetMetaData`]
    pub fn try_new_with_options(reader: T, options: ArrowReaderOptions) -> Result<Self> {
        let metadata = ArrowReaderMetadata::load(&reader, options)?;
        Ok(Self::new_with_metadata(reader, metadata))
    }

    /// Create a [`ParquetRecordBatchReaderBuilder`] from the provided [`ArrowReaderMetadata`]
    ///
    /// Use this method if you already have [`ParquetMetaData`] for a file.
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
    /// #
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

    /// Read bloom filter for a column in a row group
    ///
    /// Returns `None` if the column does not have a bloom filter
    ///
    /// We should call this function after other forms pruning, such as projection and predicate pushdown.
    pub fn get_row_group_column_bloom_filter(
        &self,
        row_group_idx: usize,
        column_idx: usize,
    ) -> Result<Option<Sbbf>> {
        let metadata = self.metadata.row_group(row_group_idx);
        let column_metadata = metadata.column(column_idx);

        let offset: u64 = if let Some(offset) = column_metadata.bloom_filter_offset() {
            offset
                .try_into()
                .map_err(|_| ParquetError::General("Bloom filter offset is invalid".to_string()))?
        } else {
            return Ok(None);
        };

        let buffer = match column_metadata.bloom_filter_length() {
            Some(length) => self.input.0.get_bytes(offset, length as usize),
            None => self.input.0.get_bytes(offset, SBBF_HEADER_SIZE_ESTIMATE),
        }?;

        let (header, bitset_offset) =
            chunk_read_bloom_filter_header_and_offset(offset, buffer.clone())?;

        match header.algorithm {
            BloomFilterAlgorithm::BLOCK => {
                // this match exists to future proof the singleton algorithm enum
            }
        }
        match header.compression {
            BloomFilterCompression::UNCOMPRESSED => {
                // this match exists to future proof the singleton compression enum
            }
        }
        match header.hash {
            BloomFilterHash::XXHASH => {
                // this match exists to future proof the singleton hash enum
            }
        }

        let bitset = match column_metadata.bloom_filter_length() {
            Some(_) => {
                let bitset_start = bitset_offset
                    .checked_sub(offset)
                    .and_then(|start| usize::try_from(start).ok())
                    .ok_or_else(|| {
                        ParquetError::General("Bloom filter offset is invalid".to_string())
                    })?;
                buffer.slice(bitset_start..)
            }
            None => {
                let bitset_length: usize = header.num_bytes.try_into().map_err(|_| {
                    ParquetError::General("Bloom filter length is invalid".to_string())
                })?;
                self.input.0.get_bytes(bitset_offset, bitset_length)?
            }
        };
        Ok(Some(Sbbf::new(&bitset)))
    }

    /// Build a [`ParquetRecordBatchReader`]
    ///
    /// Note: this will eagerly evaluate any `RowFilter` before returning
    pub fn build(self) -> Result<ParquetRecordBatchReader> {
        let Self {
            input,
            metadata,
            schema: _,
            fields,
            batch_size,
            row_group_plan,
            projection,
            mut filter,
            row_selection_policy,
            limit,
            offset,
            metrics,
            // Not used for the sync reader, see https://github.com/apache/arrow-rs/issues/8000
            max_predicate_cache_size: _,
        } = self;

        // Try to avoid allocate large buffer
        let batch_size = batch_size.min(metadata.file_metadata().num_rows() as usize);

        let (row_groups, selection) = row_group_plan.into_global()?;

        let row_groups = row_groups.unwrap_or_else(|| (0..metadata.num_row_groups()).collect());

        let reader = ReaderRowGroups {
            reader: Arc::new(input.0),
            metadata,
            row_groups,
        };

        let mut plan_builder = ReadPlanBuilder::new(batch_size)
            .with_selection(selection)
            .with_row_selection_policy(row_selection_policy);

        // Update selection based on any filters
        if let Some(filter) = filter.as_mut() {
            for predicate in &mut filter.predicates {
                // break early if we have ruled out all rows
                if !plan_builder.selects_any() {
                    break;
                }

                let mut cache_projection = predicate.projection().clone();
                cache_projection.intersect(&projection);

                let array_reader = ArrayReaderBuilder::new(&reader, &metrics)
                    .with_batch_size(batch_size)
                    .with_parquet_metadata(&reader.metadata)
                    .build_array_reader(fields.as_deref(), predicate.projection())?;

                plan_builder = plan_builder.with_predicate(array_reader, predicate.as_mut())?;
            }
        }

        let array_reader = ArrayReaderBuilder::new(&reader, &metrics)
            .with_batch_size(batch_size)
            .with_parquet_metadata(&reader.metadata)
            .build_array_reader(fields.as_deref(), &projection)?;

        let read_plan = plan_builder
            .limited(reader.num_rows())
            .with_offset(offset)
            .with_limit(limit)
            .build_limited()
            .build();

        Ok(ParquetRecordBatchReader::new(array_reader, read_plan))
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

    fn row_groups(&self) -> Box<dyn Iterator<Item = &RowGroupMetaData> + '_> {
        Box::new(
            self.row_groups
                .iter()
                .map(move |i| self.metadata.row_group(*i)),
        )
    }

    fn metadata(&self) -> &ParquetMetaData {
        self.metadata.as_ref()
    }
}

struct ReaderPageIterator<T: ChunkReader> {
    reader: Arc<T>,
    column_idx: usize,
    row_groups: std::vec::IntoIter<usize>,
    metadata: Arc<ParquetMetaData>,
}

impl<T: ChunkReader + 'static> ReaderPageIterator<T> {
    /// Return the next SerializedPageReader
    fn next_page_reader(&self, rg_idx: usize) -> Result<SerializedPageReader<T>> {
        let rg = self.metadata.row_group(rg_idx);
        let column_chunk_metadata = rg.column(self.column_idx);
        let page_locations = self
            .metadata
            .page_index()
            .map(|i| i.page_locations(rg_idx, self.column_idx).cloned())
            .unwrap_or(None);
        let total_rows = rg.num_rows() as usize;
        let reader = self.reader.clone();

        SerializedPageReader::new(reader, column_chunk_metadata, total_rows, page_locations)?
            .add_crypto_context(
                rg_idx,
                self.column_idx,
                self.metadata.as_ref(),
                column_chunk_metadata,
            )
    }
}

impl<T: ChunkReader + 'static> Iterator for ReaderPageIterator<T> {
    type Item = Result<Box<dyn PageReader>>;

    fn next(&mut self) -> Option<Self::Item> {
        let rg_idx = self.row_groups.next()?;
        let page_reader = self
            .next_page_reader(rg_idx)
            .map(|page_reader| Box::new(page_reader) as _);
        Some(page_reader)
    }
}

impl<T: ChunkReader + 'static> PageIterator for ReaderPageIterator<T> {}

/// Reads Parquet data as Arrow [`RecordBatch`]es
///
/// This struct implements the [`RecordBatchReader`] trait and is an
/// `Iterator<Item = ArrowResult<RecordBatch>>` that yields [`RecordBatch`]es.
///
/// Typically, either reads from a file or an in memory buffer [`Bytes`]
///
/// Created by [`ParquetRecordBatchReaderBuilder`]
///
/// [`Bytes`]: bytes::Bytes
pub struct ParquetRecordBatchReader {
    array_reader: Box<dyn ArrayReader>,
    schema: SchemaRef,
    read_plan: ReadPlan,
}

/// Accumulates filter masks for decoded chunks in one logical output batch.
///
/// The first chunk keeps its [`BooleanBuffer`] without copying. A second chunk
/// promotes the accumulator to a [`BooleanBufferBuilder`], and later chunks are
/// appended to it. For example, chunks `1001` and `1` become `10011`:
///
/// ```text
///           append(1001)             append(1)
///   Empty ───────────────▶ Single ───────────────▶ Combined
///                          1001                    10011
///                          (zero copy)             (promoted to builder)
/// ```
///
/// The combined mask lines up with the rows that [`ArrayReader::read_records`]
/// buffered across the decoded chunks (see [`read_mask_batch`]). Consuming the
/// buffered batch and filtering it with the accumulated mask yields the output.
///
/// ```text
///   decoded rows:   0 1 2 3   11   <-- buffered by the array reader
///   chunk masks:   [1 0 0 1] [1]
///   finish():       1 0 0 1   1    <-- filters the whole batch in one pass
/// ```
#[derive(Default)]
enum FilterMaskAccumulator {
    #[default]
    Empty,
    Single(BooleanBuffer),
    Combined(BooleanBufferBuilder),
}

impl FilterMaskAccumulator {
    fn append(&mut self, mask: BooleanBuffer) {
        *self = match std::mem::take(self) {
            Self::Empty => Self::Single(mask),
            Self::Single(first) => {
                let mut combined = BooleanBufferBuilder::new(first.len() + mask.len());
                combined.append_buffer(&first);
                combined.append_buffer(&mask);
                Self::Combined(combined)
            }
            Self::Combined(mut combined) => {
                combined.append_buffer(&mask);
                Self::Combined(combined)
            }
        };
    }

    fn finish(self) -> Option<BooleanBuffer> {
        match self {
            Self::Empty => None,
            Self::Single(mask) => Some(mask),
            Self::Combined(combined) => Some(combined.build()),
        }
    }
}

/// Converts the projection buffered by `array_reader` into a record batch.
fn consume_record_batch(array_reader: &mut dyn ArrayReader) -> Result<RecordBatch> {
    let array = array_reader.consume_batch()?;
    let struct_array = array.as_struct_opt().ok_or_else(|| {
        ArrowError::ParquetError("Struct array reader should return struct array".to_string())
    })?;
    Ok(RecordBatch::from(struct_array))
}

/// Reads one logical Mask batch, potentially spanning multiple loaded ranges.
///
/// Each [`MaskCursor`] chunk is safe to decode because it stays within loaded
/// pages. Gaps are crossed with [`ArrayReader::skip_records`], while decoded
/// arrays and their mask fragments remain buffered. Once `batch_size` selected
/// rows have accumulated, this consumes the underlying batch and filters it
/// once with the combined mask.
fn read_mask_batch(
    array_reader: &mut dyn ArrayReader,
    mask_cursor: &mut MaskCursor,
    batch_size: usize,
) -> Result<Option<RecordBatch>> {
    let mut selected_rows = 0;
    let mut filter_mask = FilterMaskAccumulator::default();

    while selected_rows < batch_size && !mask_cursor.is_empty() {
        let mask_chunk = mask_cursor.next_chunk(batch_size - selected_rows)?;

        if mask_chunk.initial_skip > 0 {
            let skipped = array_reader.skip_records(mask_chunk.initial_skip)?;
            if skipped != mask_chunk.initial_skip {
                return Err(general_err!(
                    "failed to skip rows, expected {}, got {}",
                    mask_chunk.initial_skip,
                    skipped
                ));
            }
        }

        let mask = mask_cursor.mask_values_for(&mask_chunk)?;
        let read = array_reader.read_records(mask_chunk.chunk_rows)?;
        if read == 0 {
            return Err(general_err!(
                "reached end of column while expecting {} rows",
                mask_chunk.chunk_rows
            ));
        }
        if read != mask_chunk.chunk_rows {
            return Err(general_err!(
                "insufficient rows read from array reader - expected {}, got {}",
                mask_chunk.chunk_rows,
                read
            ));
        }

        filter_mask.append(mask.values().clone());
        selected_rows += mask_chunk.selected_rows;
    }

    if selected_rows == 0 {
        return Ok(None);
    }

    let filter_mask = filter_mask
        .finish()
        .ok_or_else(|| general_err!("Internal Error: decoded Mask batch has no filter values"))?;
    let batch = consume_record_batch(array_reader)?;
    let filtered_batch = filter_record_batch(&batch, &BooleanArray::from(filter_mask))?;
    if filtered_batch.num_rows() != selected_rows {
        return Err(general_err!(
            "filtered rows mismatch selection - expected {}, got {}",
            selected_rows,
            filtered_batch.num_rows()
        ));
    }

    Ok(Some(filtered_batch))
}

impl Debug for ParquetRecordBatchReader {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParquetRecordBatchReader")
            .field("array_reader", &"...")
            .field("schema", &self.schema)
            .field("read_plan", &self.read_plan)
            .finish()
    }
}

impl Iterator for ParquetRecordBatchReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_inner()
            .map_err(|arrow_err| arrow_err.into())
            .transpose()
    }
}

impl ParquetRecordBatchReader {
    /// Returns the next `RecordBatch` from the reader, or `None` if the reader
    /// has reached the end of the file.
    ///
    /// Returns `Result<Option<..>>` rather than `Option<Result<..>>` to
    /// simplify error handling with `?`
    fn next_inner(&mut self) -> Result<Option<RecordBatch>> {
        let mut read_records = 0;
        let batch_size = self.batch_size();
        if batch_size == 0 {
            return Ok(None);
        }
        match self.read_plan.row_selection_cursor_mut() {
            RowSelectionCursor::Mask(mask_cursor) => {
                return read_mask_batch(self.array_reader.as_mut(), mask_cursor, batch_size);
            }
            RowSelectionCursor::Selectors(selectors_cursor) => {
                while read_records < batch_size && !selectors_cursor.is_empty() {
                    let front = selectors_cursor.next_selector();
                    if front.skip {
                        let skipped = self.array_reader.skip_records(front.row_count)?;

                        if skipped != front.row_count {
                            return Err(general_err!(
                                "failed to skip rows, expected {}, got {}",
                                front.row_count,
                                skipped
                            ));
                        }
                        continue;
                    }

                    //Currently, when RowSelectors with row_count = 0 are included then its interpreted as end of reader.
                    //Fix is to skip such entries. See https://github.com/apache/arrow-rs/issues/2669
                    if front.row_count == 0 {
                        continue;
                    }

                    // try to read record
                    let need_read = batch_size - read_records;
                    let to_read = match front.row_count.checked_sub(need_read) {
                        Some(remaining) if remaining != 0 => {
                            // if page row count less than batch_size we must set batch size to page row count.
                            // add check avoid dead loop
                            selectors_cursor.return_selector(RowSelector::select(remaining));
                            need_read
                        }
                        _ => front.row_count,
                    };
                    match self.array_reader.read_records(to_read)? {
                        0 => break,
                        rec => read_records += rec,
                    }
                }
            }
            RowSelectionCursor::All => {
                self.array_reader.read_records(batch_size)?;
            }
        }

        let batch = consume_record_batch(self.array_reader.as_mut())?;
        Ok(if batch.num_rows() > 0 {
            Some(batch)
        } else {
            None
        })
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
        // note metrics are not supported in this API
        let metrics = ArrowReaderMetrics::disabled();
        let array_reader = ArrayReaderBuilder::new(row_groups, &metrics)
            .with_batch_size(batch_size)
            .with_parquet_metadata(row_groups.metadata())
            .build_array_reader(levels.levels.as_ref(), &ProjectionMask::all())?;

        let read_plan = ReadPlanBuilder::new(batch_size)
            .with_selection(selection)
            .build();

        Ok(Self {
            array_reader,
            schema: Arc::new(Schema::new(levels.fields.clone())),
            read_plan,
        })
    }

    /// Create a new [`ParquetRecordBatchReader`] that will read at most `batch_size` rows at
    /// a time from [`ArrayReader`] based on the configured `selection`. If `selection` is `None`
    /// all rows will be returned
    pub(crate) fn new(array_reader: Box<dyn ArrayReader>, read_plan: ReadPlan) -> Self {
        let schema = match array_reader.get_data_type() {
            ArrowType::Struct(fields) => Schema::new(fields.clone()),
            _ => unreachable!("Struct array reader's data type is not struct!"),
        };

        Self {
            array_reader,
            schema: Arc::new(schema),
            read_plan,
        }
    }

    #[inline(always)]
    pub(crate) fn batch_size(&self) -> usize {
        self.read_plan.batch_size()
    }
}

#[cfg(test)]
pub(crate) mod tests;
