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

//! Configuration via [`WriterProperties`] and [`ReaderProperties`]
use std::str::FromStr;
use std::{collections::HashMap, sync::Arc};

use crate::basic::{Compression, Encoding};
use crate::compression::{CodecOptions, CodecOptionsBuilder};
use crate::file::metadata::KeyValue;
use crate::format::SortingColumn;
use crate::schema::types::ColumnPath;

/// Default value for [`WriterProperties::data_page_size_limit`]
pub const DEFAULT_PAGE_SIZE: usize = 1024 * 1024;
/// Default value for [`WriterProperties::write_batch_size`]
pub const DEFAULT_WRITE_BATCH_SIZE: usize = 1024;
/// Default value for [`WriterProperties::writer_version`]
pub const DEFAULT_WRITER_VERSION: WriterVersion = WriterVersion::PARQUET_1_0;
/// Default value for [`WriterProperties::compression`]
pub const DEFAULT_COMPRESSION: Compression = Compression::UNCOMPRESSED;
/// Default value for [`WriterProperties::dictionary_enabled`]
pub const DEFAULT_DICTIONARY_ENABLED: bool = true;
/// Default value for [`WriterProperties::dictionary_page_size_limit`]
pub const DEFAULT_DICTIONARY_PAGE_SIZE_LIMIT: usize = DEFAULT_PAGE_SIZE;
/// Default value for [`WriterProperties::statistics_enabled`]
pub const DEFAULT_STATISTICS_ENABLED: EnabledStatistics = EnabledStatistics::Page;
/// Default value for [`WriterProperties::max_statistics_size`]
pub const DEFAULT_MAX_STATISTICS_SIZE: usize = 4096;
/// Default value for [`WriterProperties::max_row_group_size`]
pub const DEFAULT_MAX_ROW_GROUP_SIZE: usize = 1024 * 1024;
/// Default value for [`WriterProperties::created_by`]
pub const DEFAULT_CREATED_BY: &str = concat!("parquet-rs version ", env!("CARGO_PKG_VERSION"));
/// Default value for [`WriterProperties::column_index_truncate_length`]
pub const DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH: Option<usize> = Some(64);
/// Default value for [`BloomFilterProperties::fpp`]
pub const DEFAULT_BLOOM_FILTER_FPP: f64 = 0.05;
/// Default value for [`BloomFilterProperties::ndv`]
pub const DEFAULT_BLOOM_FILTER_NDV: u64 = 1_000_000_u64;
/// Default values for [`WriterProperties::statistics_truncate_length`]
pub const DEFAULT_STATISTICS_TRUNCATE_LENGTH: Option<usize> = None;

/// Parquet writer version.
///
/// Basic constant, which is not part of the Thrift definition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(non_camel_case_types)]
pub enum WriterVersion {
    PARQUET_1_0,
    PARQUET_2_0,
}

impl WriterVersion {
    /// Returns writer version as `i32`.
    pub fn as_num(&self) -> i32 {
        match self {
            WriterVersion::PARQUET_1_0 => 1,
            WriterVersion::PARQUET_2_0 => 2,
        }
    }
}

impl FromStr for WriterVersion {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "PARQUET_1_0" | "parquet_1_0" => Ok(WriterVersion::PARQUET_1_0),
            "PARQUET_2_0" | "parquet_2_0" => Ok(WriterVersion::PARQUET_2_0),
            _ => Err(format!("Invalid writer version: {}", s)),
        }
    }
}

/// Reference counted writer properties.
pub type WriterPropertiesPtr = Arc<WriterProperties>;

/// Configuration settings for writing parquet files.
///
/// All properties except the key-value metadata are immutable,
/// use [`WriterPropertiesBuilder`] to assemble these properties.
///
/// # Example
///
/// ```rust
/// use parquet::{
///     basic::{Compression, Encoding},
///     file::properties::*,
///     schema::types::ColumnPath,
/// };
///
/// // Create properties with default configuration.
/// let props = WriterProperties::default();
///
/// // Use properties builder to set certain options and assemble the configuration.
/// let props = WriterProperties::builder()
///     .set_writer_version(WriterVersion::PARQUET_1_0)
///     .set_encoding(Encoding::PLAIN)
///     .set_column_encoding(ColumnPath::from("col1"), Encoding::DELTA_BINARY_PACKED)
///     .set_compression(Compression::SNAPPY)
///     .build();
///
/// assert_eq!(props.writer_version(), WriterVersion::PARQUET_1_0);
/// assert_eq!(
///     props.encoding(&ColumnPath::from("col1")),
///     Some(Encoding::DELTA_BINARY_PACKED)
/// );
/// assert_eq!(
///     props.encoding(&ColumnPath::from("col2")),
///     Some(Encoding::PLAIN)
/// );
/// ```
#[derive(Debug, Clone)]
pub struct WriterProperties {
    data_page_size_limit: usize,
    dictionary_page_size_limit: usize,
    data_page_row_count_limit: usize,
    write_batch_size: usize,
    max_row_group_size: usize,
    writer_version: WriterVersion,
    created_by: String,
    pub(crate) key_value_metadata: Option<Vec<KeyValue>>,
    default_column_properties: ColumnProperties,
    column_properties: HashMap<ColumnPath, ColumnProperties>,
    sorting_columns: Option<Vec<SortingColumn>>,
    column_index_truncate_length: Option<usize>,
    statistics_truncate_length: Option<usize>,
}

impl Default for WriterProperties {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl WriterProperties {
    /// Create a new [`WriterProperties`] with the default settings
    ///
    /// See [`WriterProperties::builder`] for customising settings
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns builder for writer properties with default values.
    pub fn builder() -> WriterPropertiesBuilder {
        WriterPropertiesBuilder::with_defaults()
    }

    /// Returns data page size limit.
    ///
    /// Note: this is a best effort limit based on the write batch size
    #[deprecated(since = "41.0.0", note = "Use data_page_size_limit")]
    pub fn data_pagesize_limit(&self) -> usize {
        self.data_page_size_limit
    }

    /// Returns data page size limit.
    ///
    /// Note: this is a best effort limit based on the write batch size
    ///
    /// For more details see [`WriterPropertiesBuilder::set_data_page_size_limit`]
    pub fn data_page_size_limit(&self) -> usize {
        self.data_page_size_limit
    }

    /// Returns dictionary page size limit.
    ///
    /// Note: this is a best effort limit based on the write batch size
    #[deprecated(since = "41.0.0", note = "Use dictionary_page_size_limit")]
    pub fn dictionary_pagesize_limit(&self) -> usize {
        self.dictionary_page_size_limit
    }

    /// Returns dictionary page size limit.
    ///
    /// Note: this is a best effort limit based on the write batch size
    ///
    /// For more details see [`WriterPropertiesBuilder::set_dictionary_page_size_limit`]
    pub fn dictionary_page_size_limit(&self) -> usize {
        self.dictionary_page_size_limit
    }

    /// Returns the maximum page row count
    ///
    /// Note: this is a best effort limit based on the write batch size
    ///
    /// For more details see [`WriterPropertiesBuilder::set_data_page_row_count_limit`]
    pub fn data_page_row_count_limit(&self) -> usize {
        self.data_page_row_count_limit
    }

    /// Returns configured batch size for writes.
    ///
    /// When writing a batch of data, this setting allows to split it internally into
    /// smaller batches so we can better estimate the size of a page currently being
    /// written.
    pub fn write_batch_size(&self) -> usize {
        self.write_batch_size
    }

    /// Returns maximum number of rows in a row group.
    pub fn max_row_group_size(&self) -> usize {
        self.max_row_group_size
    }

    /// Returns configured writer version.
    pub fn writer_version(&self) -> WriterVersion {
        self.writer_version
    }

    /// Returns `created_by` string.
    pub fn created_by(&self) -> &str {
        &self.created_by
    }

    /// Returns `key_value_metadata` KeyValue pairs.
    pub fn key_value_metadata(&self) -> Option<&Vec<KeyValue>> {
        self.key_value_metadata.as_ref()
    }

    /// Returns sorting columns.
    pub fn sorting_columns(&self) -> Option<&Vec<SortingColumn>> {
        self.sorting_columns.as_ref()
    }

    /// Returns the maximum length of truncated min/max values in the column index.
    ///
    /// `None` if truncation is disabled, must be greater than 0 otherwise.
    pub fn column_index_truncate_length(&self) -> Option<usize> {
        self.column_index_truncate_length
    }

    /// Returns the maximum length of truncated min/max values in statistics.
    ///
    /// `None` if truncation is disabled, must be greater than 0 otherwise.
    pub fn statistics_truncate_length(&self) -> Option<usize> {
        self.statistics_truncate_length
    }

    /// Returns encoding for a data page, when dictionary encoding is enabled.
    /// This is not configurable.
    #[inline]
    pub fn dictionary_data_page_encoding(&self) -> Encoding {
        // PLAIN_DICTIONARY encoding is deprecated in writer version 1.
        // Dictionary values are encoded using RLE_DICTIONARY encoding.
        Encoding::RLE_DICTIONARY
    }

    /// Returns encoding for dictionary page, when dictionary encoding is enabled.
    /// This is not configurable.
    #[inline]
    pub fn dictionary_page_encoding(&self) -> Encoding {
        // PLAIN_DICTIONARY is deprecated in writer version 1.
        // Dictionary is encoded using plain encoding.
        Encoding::PLAIN
    }

    /// Returns encoding for a column, if set.
    /// In case when dictionary is enabled, returns fallback encoding.
    ///
    /// If encoding is not set, then column writer will choose the best encoding
    /// based on the column type.
    pub fn encoding(&self, col: &ColumnPath) -> Option<Encoding> {
        self.column_properties
            .get(col)
            .and_then(|c| c.encoding())
            .or_else(|| self.default_column_properties.encoding())
    }

    /// Returns compression codec for a column.
    pub fn compression(&self, col: &ColumnPath) -> Compression {
        self.column_properties
            .get(col)
            .and_then(|c| c.compression())
            .or_else(|| self.default_column_properties.compression())
            .unwrap_or(DEFAULT_COMPRESSION)
    }

    /// Returns `true` if dictionary encoding is enabled for a column.
    pub fn dictionary_enabled(&self, col: &ColumnPath) -> bool {
        self.column_properties
            .get(col)
            .and_then(|c| c.dictionary_enabled())
            .or_else(|| self.default_column_properties.dictionary_enabled())
            .unwrap_or(DEFAULT_DICTIONARY_ENABLED)
    }

    /// Returns `true` if statistics are enabled for a column.
    pub fn statistics_enabled(&self, col: &ColumnPath) -> EnabledStatistics {
        self.column_properties
            .get(col)
            .and_then(|c| c.statistics_enabled())
            .or_else(|| self.default_column_properties.statistics_enabled())
            .unwrap_or(DEFAULT_STATISTICS_ENABLED)
    }

    /// Returns max size for statistics.
    /// Only applicable if statistics are enabled.
    pub fn max_statistics_size(&self, col: &ColumnPath) -> usize {
        self.column_properties
            .get(col)
            .and_then(|c| c.max_statistics_size())
            .or_else(|| self.default_column_properties.max_statistics_size())
            .unwrap_or(DEFAULT_MAX_STATISTICS_SIZE)
    }

    /// Returns the [`BloomFilterProperties`] for the given column
    ///
    /// Returns `None` if bloom filter is disabled
    pub fn bloom_filter_properties(&self, col: &ColumnPath) -> Option<&BloomFilterProperties> {
        self.column_properties
            .get(col)
            .and_then(|c| c.bloom_filter_properties())
            .or_else(|| self.default_column_properties.bloom_filter_properties())
    }
}

/// Builder for parquet file writer configuration. See example on
/// [`WriterProperties`]
pub struct WriterPropertiesBuilder {
    data_page_size_limit: usize,
    dictionary_page_size_limit: usize,
    data_page_row_count_limit: usize,
    write_batch_size: usize,
    max_row_group_size: usize,
    writer_version: WriterVersion,
    created_by: String,
    key_value_metadata: Option<Vec<KeyValue>>,
    default_column_properties: ColumnProperties,
    column_properties: HashMap<ColumnPath, ColumnProperties>,
    sorting_columns: Option<Vec<SortingColumn>>,
    column_index_truncate_length: Option<usize>,
    statistics_truncate_length: Option<usize>,
}

impl WriterPropertiesBuilder {
    /// Returns default state of the builder.
    fn with_defaults() -> Self {
        Self {
            data_page_size_limit: DEFAULT_PAGE_SIZE,
            dictionary_page_size_limit: DEFAULT_DICTIONARY_PAGE_SIZE_LIMIT,
            data_page_row_count_limit: usize::MAX,
            write_batch_size: DEFAULT_WRITE_BATCH_SIZE,
            max_row_group_size: DEFAULT_MAX_ROW_GROUP_SIZE,
            writer_version: DEFAULT_WRITER_VERSION,
            created_by: DEFAULT_CREATED_BY.to_string(),
            key_value_metadata: None,
            default_column_properties: Default::default(),
            column_properties: HashMap::new(),
            sorting_columns: None,
            column_index_truncate_length: DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH,
            statistics_truncate_length: DEFAULT_STATISTICS_TRUNCATE_LENGTH,
        }
    }

    /// Finalizes the configuration and returns immutable writer properties struct.
    pub fn build(self) -> WriterProperties {
        WriterProperties {
            data_page_size_limit: self.data_page_size_limit,
            dictionary_page_size_limit: self.dictionary_page_size_limit,
            data_page_row_count_limit: self.data_page_row_count_limit,
            write_batch_size: self.write_batch_size,
            max_row_group_size: self.max_row_group_size,
            writer_version: self.writer_version,
            created_by: self.created_by,
            key_value_metadata: self.key_value_metadata,
            default_column_properties: self.default_column_properties,
            column_properties: self.column_properties,
            sorting_columns: self.sorting_columns,
            column_index_truncate_length: self.column_index_truncate_length,
            statistics_truncate_length: self.statistics_truncate_length,
        }
    }

    // ----------------------------------------------------------------------
    // Writer properties related to a file

    /// Sets writer version.
    pub fn set_writer_version(mut self, value: WriterVersion) -> Self {
        self.writer_version = value;
        self
    }

    /// Sets best effort maximum size of a data page in bytes.
    ///
    /// Note: this is a best effort limit based on value of
    /// [`set_write_batch_size`](Self::set_write_batch_size).
    #[deprecated(since = "41.0.0", note = "Use set_data_page_size_limit")]
    pub fn set_data_pagesize_limit(mut self, value: usize) -> Self {
        self.data_page_size_limit = value;
        self
    }

    /// Sets best effort maximum size of a data page in bytes.
    ///
    /// The parquet writer will attempt to limit the sizes of each
    /// `DataPage` to this many bytes. Reducing this value will result
    /// in larger parquet files, but may improve the effectiveness of
    /// page index based predicate pushdown during reading.
    ///
    /// Note: this is a best effort limit based on value of
    /// [`set_write_batch_size`](Self::set_write_batch_size).
    pub fn set_data_page_size_limit(mut self, value: usize) -> Self {
        self.data_page_size_limit = value;
        self
    }

    /// Sets best effort maximum number of rows in a data page.
    ///
    /// The parquet writer will attempt to limit the number of rows in
    /// each `DataPage` to this value. Reducing this value will result
    /// in larger parquet files, but may improve the effectiveness of
    /// page index based predicate pushdown during reading.
    ///
    /// Note: this is a best effort limit based on value of
    /// [`set_write_batch_size`](Self::set_write_batch_size).
    pub fn set_data_page_row_count_limit(mut self, value: usize) -> Self {
        self.data_page_row_count_limit = value;
        self
    }

    /// Sets best effort maximum dictionary page size, in bytes.
    ///
    /// Note: this is a best effort limit based on value of
    /// [`set_write_batch_size`](Self::set_write_batch_size).
    #[deprecated(since = "41.0.0", note = "Use set_dictionary_page_size_limit")]
    pub fn set_dictionary_pagesize_limit(mut self, value: usize) -> Self {
        self.dictionary_page_size_limit = value;
        self
    }

    /// Sets best effort maximum dictionary page size, in bytes.
    ///
    /// The parquet writer will attempt to limit the size of each
    /// `DataPage` used to store dictionaries to this many
    /// bytes. Reducing this value will result in larger parquet
    /// files, but may improve the effectiveness of page index based
    /// predicate pushdown during reading.
    ///
    /// Note: this is a best effort limit based on value of
    /// [`set_write_batch_size`](Self::set_write_batch_size).
    pub fn set_dictionary_page_size_limit(mut self, value: usize) -> Self {
        self.dictionary_page_size_limit = value;
        self
    }

    /// Sets write batch size.
    ///
    /// For performance reasons, data for each column is written in
    /// batches of this size.
    ///
    /// Additional limits such as such as
    /// [`set_data_page_row_count_limit`](Self::set_data_page_row_count_limit)
    /// are checked between batches, and thus the write batch size value acts as an
    /// upper-bound on the enforcement granularity of other limits.
    pub fn set_write_batch_size(mut self, value: usize) -> Self {
        self.write_batch_size = value;
        self
    }

    /// Sets maximum number of rows in a row group.
    pub fn set_max_row_group_size(mut self, value: usize) -> Self {
        assert!(value > 0, "Cannot have a 0 max row group size");
        self.max_row_group_size = value;
        self
    }

    /// Sets "created by" property.
    pub fn set_created_by(mut self, value: String) -> Self {
        self.created_by = value;
        self
    }

    /// Sets "key_value_metadata" property.
    pub fn set_key_value_metadata(mut self, value: Option<Vec<KeyValue>>) -> Self {
        self.key_value_metadata = value;
        self
    }

    /// Sets sorting order of rows in the row group if any
    pub fn set_sorting_columns(mut self, value: Option<Vec<SortingColumn>>) -> Self {
        self.sorting_columns = value;
        self
    }

    // ----------------------------------------------------------------------
    // Setters for any column (global)

    /// Sets encoding for any column.
    ///
    /// If dictionary is not enabled, this is treated as a primary encoding for all
    /// columns. In case when dictionary is enabled for any column, this value is
    /// considered to be a fallback encoding for that column.
    ///
    /// Panics if user tries to set dictionary encoding here, regardless of dictionary
    /// encoding flag being set.
    pub fn set_encoding(mut self, value: Encoding) -> Self {
        self.default_column_properties.set_encoding(value);
        self
    }

    /// Sets compression codec for any column.
    pub fn set_compression(mut self, value: Compression) -> Self {
        self.default_column_properties.set_compression(value);
        self
    }

    /// Sets flag to enable/disable dictionary encoding for any column.
    ///
    /// Use this method to set dictionary encoding, instead of explicitly specifying
    /// encoding in `set_encoding` method.
    pub fn set_dictionary_enabled(mut self, value: bool) -> Self {
        self.default_column_properties.set_dictionary_enabled(value);
        self
    }

    /// Sets flag to enable/disable statistics for any column.
    pub fn set_statistics_enabled(mut self, value: EnabledStatistics) -> Self {
        self.default_column_properties.set_statistics_enabled(value);
        self
    }

    /// Sets max statistics size for any column.
    /// Applicable only if statistics are enabled.
    pub fn set_max_statistics_size(mut self, value: usize) -> Self {
        self.default_column_properties
            .set_max_statistics_size(value);
        self
    }

    /// Sets whether bloom filter is enabled for any column.
    /// If the bloom filter is enabled previously then it is a no-op.
    /// If the bloom filter is not yet enabled, a default set of ndv and fpp value will be used.
    /// You can use [`set_bloom_filter_ndv`](Self::set_bloom_filter_ndv) and [`set_bloom_filter_fpp`](Self::set_bloom_filter_fpp) to further adjust the ndv and fpp.
    pub fn set_bloom_filter_enabled(mut self, value: bool) -> Self {
        self.default_column_properties
            .set_bloom_filter_enabled(value);
        self
    }

    /// Sets bloom filter false positive probability (fpp) for any column.
    /// Implicitly [`set_bloom_filter_enabled`](Self::set_bloom_filter_enabled).
    pub fn set_bloom_filter_fpp(mut self, value: f64) -> Self {
        self.default_column_properties.set_bloom_filter_fpp(value);
        self
    }

    /// Sets number of distinct values (ndv) for bloom filter for any column.
    /// Implicitly [`set_bloom_filter_enabled`](Self::set_bloom_filter_enabled).
    pub fn set_bloom_filter_ndv(mut self, value: u64) -> Self {
        self.default_column_properties.set_bloom_filter_ndv(value);
        self
    }

    // ----------------------------------------------------------------------
    // Setters for a specific column

    /// Helper method to get existing or new mutable reference of column properties.
    #[inline]
    fn get_mut_props(&mut self, col: ColumnPath) -> &mut ColumnProperties {
        self.column_properties.entry(col).or_default()
    }

    /// Sets encoding for a column.
    /// Takes precedence over globally defined settings.
    ///
    /// If dictionary is not enabled, this is treated as a primary encoding for this
    /// column. In case when dictionary is enabled for this column, either through
    /// global defaults or explicitly, this value is considered to be a fallback
    /// encoding for this column.
    ///
    /// Panics if user tries to set dictionary encoding here, regardless of dictionary
    /// encoding flag being set.
    pub fn set_column_encoding(mut self, col: ColumnPath, value: Encoding) -> Self {
        self.get_mut_props(col).set_encoding(value);
        self
    }

    /// Sets compression codec for a column.
    /// Takes precedence over globally defined settings.
    pub fn set_column_compression(mut self, col: ColumnPath, value: Compression) -> Self {
        self.get_mut_props(col).set_compression(value);
        self
    }

    /// Sets flag to enable/disable dictionary encoding for a column.
    /// Takes precedence over globally defined settings.
    pub fn set_column_dictionary_enabled(mut self, col: ColumnPath, value: bool) -> Self {
        self.get_mut_props(col).set_dictionary_enabled(value);
        self
    }

    /// Sets flag to enable/disable statistics for a column.
    /// Takes precedence over globally defined settings.
    pub fn set_column_statistics_enabled(
        mut self,
        col: ColumnPath,
        value: EnabledStatistics,
    ) -> Self {
        self.get_mut_props(col).set_statistics_enabled(value);
        self
    }

    /// Sets max size for statistics for a column.
    /// Takes precedence over globally defined settings.
    pub fn set_column_max_statistics_size(mut self, col: ColumnPath, value: usize) -> Self {
        self.get_mut_props(col).set_max_statistics_size(value);
        self
    }

    /// Sets whether a bloom filter should be created for a specific column.
    /// The behavior is similar to [`set_bloom_filter_enabled`](Self::set_bloom_filter_enabled).
    /// Takes precedence over globally defined settings.
    pub fn set_column_bloom_filter_enabled(mut self, col: ColumnPath, value: bool) -> Self {
        self.get_mut_props(col).set_bloom_filter_enabled(value);
        self
    }

    /// Sets the false positive probability for bloom filter for a specific column.
    /// The behavior is similar to [`set_bloom_filter_fpp`](Self::set_bloom_filter_fpp) but will
    /// override the default.
    pub fn set_column_bloom_filter_fpp(mut self, col: ColumnPath, value: f64) -> Self {
        self.get_mut_props(col).set_bloom_filter_fpp(value);
        self
    }

    /// Sets the number of distinct values for bloom filter for a specific column.
    /// The behavior is similar to [`set_bloom_filter_ndv`](Self::set_bloom_filter_ndv) but will
    /// override the default.
    pub fn set_column_bloom_filter_ndv(mut self, col: ColumnPath, value: u64) -> Self {
        self.get_mut_props(col).set_bloom_filter_ndv(value);
        self
    }

    /// Sets the max length of min/max value fields in the column index. Must be greater than 0.
    /// If set to `None` - there's no effective limit.
    pub fn set_column_index_truncate_length(mut self, max_length: Option<usize>) -> Self {
        if let Some(value) = max_length {
            assert!(value > 0, "Cannot have a 0 column index truncate length. If you wish to disable min/max value truncation, set it to `None`.");
        }

        self.column_index_truncate_length = max_length;
        self
    }

    /// Sets the max length of min/max value fields in statistics. Must be greater than 0.
    /// If set to `None` - there's no effective limit.
    pub fn set_statistics_truncate_length(mut self, max_length: Option<usize>) -> Self {
        if let Some(value) = max_length {
            assert!(value > 0, "Cannot have a 0 statistics truncate length. If you wish to disable min/max value truncation, set it to `None`.");
        }

        self.statistics_truncate_length = max_length;
        self
    }
}

/// Controls the level of statistics to be computed by the writer and stored in
/// the parquet file.
///
/// Enabling statistics makes the resulting Parquet file larger and requires
/// more time to read the parquet footer.
///
/// Statistics can be used to improve query performance by pruning row groups
/// and pages during query execution if the query engine supports evaluating the
/// predicate using the statistics.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum EnabledStatistics {
    /// Compute no statistics.
    None,
    /// Compute column chunk-level statistics but not page-level.
    ///
    /// Setting this option will store one set of statistics for each relevant
    /// column for each row group. The more row groups written, the more
    /// statistics will be stored.
    Chunk,
    /// Compute page-level and column chunk-level statistics.
    ///
    /// Setting this option will store one set of statistics for each relevant
    /// column for each page and row group. The more row groups and the more
    /// pages written, the more statistics will be stored.
    Page,
}

impl FromStr for EnabledStatistics {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "NONE" | "none" => Ok(EnabledStatistics::None),
            "CHUNK" | "chunk" => Ok(EnabledStatistics::Chunk),
            "PAGE" | "page" => Ok(EnabledStatistics::Page),
            _ => Err(format!("Invalid statistics arg: {}", s)),
        }
    }
}

impl Default for EnabledStatistics {
    fn default() -> Self {
        DEFAULT_STATISTICS_ENABLED
    }
}

/// Controls the bloom filter to be computed by the writer.
#[derive(Debug, Clone, PartialEq)]
pub struct BloomFilterProperties {
    /// False positive probability, should be always between 0 and 1 exclusive. Defaults to [`DEFAULT_BLOOM_FILTER_FPP`].
    ///
    /// You should set this value by calling [`WriterPropertiesBuilder::set_bloom_filter_fpp`].
    ///
    /// The bloom filter data structure is a trade of between disk and memory space versus fpp, the
    /// smaller the fpp, the more memory and disk space is required, thus setting it to a reasonable value
    /// e.g. 0.1, 0.05, or 0.001 is recommended.
    ///
    /// Setting to very small number diminishes the value of the filter itself, as the bitset size is
    /// even larger than just storing the whole value. You are also expected to set `ndv` if it can
    /// be known in advance in order to largely reduce space usage.
    pub fpp: f64,
    /// Number of distinct values, should be non-negative to be meaningful. Defaults to [`DEFAULT_BLOOM_FILTER_NDV`].
    ///
    /// You should set this value by calling [`WriterPropertiesBuilder::set_bloom_filter_ndv`].
    ///
    /// Usage of bloom filter is most beneficial for columns with large cardinality, so a good heuristic
    /// is to set ndv to number of rows. However it can reduce disk size if you know in advance a smaller
    /// number of distinct values. For very small ndv value it is probably not worth it to use bloom filter
    /// anyway.
    ///
    /// Increasing this value (without increasing fpp) will result in an increase in disk or memory size.
    pub ndv: u64,
}

impl Default for BloomFilterProperties {
    fn default() -> Self {
        BloomFilterProperties {
            fpp: DEFAULT_BLOOM_FILTER_FPP,
            ndv: DEFAULT_BLOOM_FILTER_NDV,
        }
    }
}

/// Container for column properties that can be changed as part of writer.
///
/// If a field is `None`, it means that no specific value has been set for this column,
/// so some subsequent or default value must be used.
#[derive(Debug, Clone, Default, PartialEq)]
struct ColumnProperties {
    encoding: Option<Encoding>,
    codec: Option<Compression>,
    dictionary_enabled: Option<bool>,
    statistics_enabled: Option<EnabledStatistics>,
    max_statistics_size: Option<usize>,
    /// bloom filter related properties
    bloom_filter_properties: Option<BloomFilterProperties>,
}

impl ColumnProperties {
    /// Sets encoding for this column.
    ///
    /// If dictionary is not enabled, this is treated as a primary encoding for a column.
    /// In case when dictionary is enabled for a column, this value is considered to
    /// be a fallback encoding.
    ///
    /// Panics if user tries to set dictionary encoding here, regardless of dictionary
    /// encoding flag being set. Use `set_dictionary_enabled` method to enable dictionary
    /// for a column.
    fn set_encoding(&mut self, value: Encoding) {
        if value == Encoding::PLAIN_DICTIONARY || value == Encoding::RLE_DICTIONARY {
            panic!("Dictionary encoding can not be used as fallback encoding");
        }
        self.encoding = Some(value);
    }

    /// Sets compression codec for this column.
    fn set_compression(&mut self, value: Compression) {
        self.codec = Some(value);
    }

    /// Sets whether or not dictionary encoding is enabled for this column.
    fn set_dictionary_enabled(&mut self, enabled: bool) {
        self.dictionary_enabled = Some(enabled);
    }

    /// Sets whether or not statistics are enabled for this column.
    fn set_statistics_enabled(&mut self, enabled: EnabledStatistics) {
        self.statistics_enabled = Some(enabled);
    }

    /// Sets max size for statistics for this column.
    fn set_max_statistics_size(&mut self, value: usize) {
        self.max_statistics_size = Some(value);
    }

    /// If `value` is `true`, sets bloom filter properties to default values if not previously set,
    /// otherwise it is a no-op.
    /// If `value` is `false`, resets bloom filter properties to `None`.
    fn set_bloom_filter_enabled(&mut self, value: bool) {
        if value && self.bloom_filter_properties.is_none() {
            self.bloom_filter_properties = Some(Default::default())
        } else if !value {
            self.bloom_filter_properties = None
        }
    }

    /// Sets the false positive probability for bloom filter for this column, and implicitly enables
    /// bloom filter if not previously enabled.
    ///
    /// # Panics
    ///
    /// Panics if the `value` is not between 0 and 1 exclusive
    fn set_bloom_filter_fpp(&mut self, value: f64) {
        assert!(
            value > 0. && value < 1.0,
            "fpp must be between 0 and 1 exclusive, got {value}"
        );

        self.bloom_filter_properties
            .get_or_insert_with(Default::default)
            .fpp = value;
    }

    /// Sets the number of distinct (unique) values for bloom filter for this column, and implicitly
    /// enables bloom filter if not previously enabled.
    fn set_bloom_filter_ndv(&mut self, value: u64) {
        self.bloom_filter_properties
            .get_or_insert_with(Default::default)
            .ndv = value;
    }

    /// Returns optional encoding for this column.
    fn encoding(&self) -> Option<Encoding> {
        self.encoding
    }

    /// Returns optional compression codec for this column.
    fn compression(&self) -> Option<Compression> {
        self.codec
    }

    /// Returns `Some(true)` if dictionary encoding is enabled for this column, if
    /// disabled then returns `Some(false)`. If result is `None`, then no setting has
    /// been provided.
    fn dictionary_enabled(&self) -> Option<bool> {
        self.dictionary_enabled
    }

    /// Returns `Some(true)` if statistics are enabled for this column, if disabled then
    /// returns `Some(false)`. If result is `None`, then no setting has been provided.
    fn statistics_enabled(&self) -> Option<EnabledStatistics> {
        self.statistics_enabled
    }

    /// Returns optional max size in bytes for statistics.
    fn max_statistics_size(&self) -> Option<usize> {
        self.max_statistics_size
    }

    /// Returns the bloom filter properties, or `None` if not enabled
    fn bloom_filter_properties(&self) -> Option<&BloomFilterProperties> {
        self.bloom_filter_properties.as_ref()
    }
}

/// Reference counted reader properties.
pub type ReaderPropertiesPtr = Arc<ReaderProperties>;

const DEFAULT_READ_BLOOM_FILTER: bool = false;

/// Configuration settings for reading parquet files.
///
/// All properties are immutable and `Send` + `Sync`.
/// Use [`ReaderPropertiesBuilder`] to assemble these properties.
///
/// # Example
///
/// ```rust
/// use parquet::file::properties::ReaderProperties;
///
/// // Create properties with default configuration.
/// let props = ReaderProperties::builder().build();
///
/// // Use properties builder to set certain options and assemble the configuration.
/// let props = ReaderProperties::builder()
///     .set_backward_compatible_lz4(false)
///     .build();
/// ```
pub struct ReaderProperties {
    codec_options: CodecOptions,
    read_bloom_filter: bool,
}

impl ReaderProperties {
    /// Returns builder for reader properties with default values.
    pub fn builder() -> ReaderPropertiesBuilder {
        ReaderPropertiesBuilder::with_defaults()
    }

    /// Returns codec options.
    pub(crate) fn codec_options(&self) -> &CodecOptions {
        &self.codec_options
    }

    /// Returns whether to read bloom filter
    pub(crate) fn read_bloom_filter(&self) -> bool {
        self.read_bloom_filter
    }
}

/// Builder for parquet file reader configuration. See example on
/// [`ReaderProperties`]
pub struct ReaderPropertiesBuilder {
    codec_options_builder: CodecOptionsBuilder,
    read_bloom_filter: Option<bool>,
}

/// Reader properties builder.
impl ReaderPropertiesBuilder {
    /// Returns default state of the builder.
    fn with_defaults() -> Self {
        Self {
            codec_options_builder: CodecOptionsBuilder::default(),
            read_bloom_filter: None,
        }
    }

    /// Finalizes the configuration and returns immutable reader properties struct.
    pub fn build(self) -> ReaderProperties {
        ReaderProperties {
            codec_options: self.codec_options_builder.build(),
            read_bloom_filter: self.read_bloom_filter.unwrap_or(DEFAULT_READ_BLOOM_FILTER),
        }
    }

    /// Enable/disable backward compatible LZ4.
    ///
    /// If backward compatible LZ4 is enable, on LZ4_HADOOP error it will fallback
    /// to the older versions LZ4 algorithms. That is LZ4_FRAME, for backward compatibility
    /// with files generated by older versions of this library, and LZ4_RAW, for backward
    /// compatibility with files generated by older versions of parquet-cpp.
    ///
    /// If backward compatible LZ4 is disabled, on LZ4_HADOOP error it will return the error.
    pub fn set_backward_compatible_lz4(mut self, value: bool) -> Self {
        self.codec_options_builder = self
            .codec_options_builder
            .set_backward_compatible_lz4(value);
        self
    }

    /// Enable/disable reading bloom filter
    ///
    /// If reading bloom filter is enabled, bloom filter will be read from the file.
    /// If reading bloom filter is disabled, bloom filter will not be read from the file.
    ///
    /// By default bloom filter is set to be read.
    pub fn set_read_bloom_filter(mut self, value: bool) -> Self {
        self.read_bloom_filter = Some(value);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_writer_version() {
        assert_eq!(WriterVersion::PARQUET_1_0.as_num(), 1);
        assert_eq!(WriterVersion::PARQUET_2_0.as_num(), 2);
    }

    #[test]
    fn test_writer_properties_default_settings() {
        let props = WriterProperties::default();
        assert_eq!(props.data_page_size_limit(), DEFAULT_PAGE_SIZE);
        assert_eq!(
            props.dictionary_page_size_limit(),
            DEFAULT_DICTIONARY_PAGE_SIZE_LIMIT
        );
        assert_eq!(props.write_batch_size(), DEFAULT_WRITE_BATCH_SIZE);
        assert_eq!(props.max_row_group_size(), DEFAULT_MAX_ROW_GROUP_SIZE);
        assert_eq!(props.writer_version(), DEFAULT_WRITER_VERSION);
        assert_eq!(props.created_by(), DEFAULT_CREATED_BY);
        assert_eq!(props.key_value_metadata(), None);
        assert_eq!(props.encoding(&ColumnPath::from("col")), None);
        assert_eq!(
            props.compression(&ColumnPath::from("col")),
            DEFAULT_COMPRESSION
        );
        assert_eq!(
            props.dictionary_enabled(&ColumnPath::from("col")),
            DEFAULT_DICTIONARY_ENABLED
        );
        assert_eq!(
            props.statistics_enabled(&ColumnPath::from("col")),
            DEFAULT_STATISTICS_ENABLED
        );
        assert_eq!(
            props.max_statistics_size(&ColumnPath::from("col")),
            DEFAULT_MAX_STATISTICS_SIZE
        );
        assert!(props
            .bloom_filter_properties(&ColumnPath::from("col"))
            .is_none());
    }

    #[test]
    fn test_writer_properties_dictionary_encoding() {
        // dictionary encoding is not configurable, and it should be the same for both
        // writer version 1 and 2.
        for version in &[WriterVersion::PARQUET_1_0, WriterVersion::PARQUET_2_0] {
            let props = WriterProperties::builder()
                .set_writer_version(*version)
                .build();
            assert_eq!(props.dictionary_page_encoding(), Encoding::PLAIN);
            assert_eq!(
                props.dictionary_data_page_encoding(),
                Encoding::RLE_DICTIONARY
            );
        }
    }

    #[test]
    #[should_panic(expected = "Dictionary encoding can not be used as fallback encoding")]
    fn test_writer_properties_panic_when_plain_dictionary_is_fallback() {
        // Should panic when user specifies dictionary encoding as fallback encoding.
        WriterProperties::builder()
            .set_encoding(Encoding::PLAIN_DICTIONARY)
            .build();
    }

    #[test]
    #[should_panic(expected = "Dictionary encoding can not be used as fallback encoding")]
    fn test_writer_properties_panic_when_rle_dictionary_is_fallback() {
        // Should panic when user specifies dictionary encoding as fallback encoding.
        WriterProperties::builder()
            .set_encoding(Encoding::RLE_DICTIONARY)
            .build();
    }

    #[test]
    #[should_panic(expected = "Dictionary encoding can not be used as fallback encoding")]
    fn test_writer_properties_panic_when_dictionary_is_enabled() {
        WriterProperties::builder()
            .set_dictionary_enabled(true)
            .set_column_encoding(ColumnPath::from("col"), Encoding::RLE_DICTIONARY)
            .build();
    }

    #[test]
    #[should_panic(expected = "Dictionary encoding can not be used as fallback encoding")]
    fn test_writer_properties_panic_when_dictionary_is_disabled() {
        WriterProperties::builder()
            .set_dictionary_enabled(false)
            .set_column_encoding(ColumnPath::from("col"), Encoding::RLE_DICTIONARY)
            .build();
    }

    #[test]
    fn test_writer_properties_builder() {
        let props = WriterProperties::builder()
            // file settings
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .set_data_page_size_limit(10)
            .set_dictionary_page_size_limit(20)
            .set_write_batch_size(30)
            .set_max_row_group_size(40)
            .set_created_by("default".to_owned())
            .set_key_value_metadata(Some(vec![KeyValue::new(
                "key".to_string(),
                "value".to_string(),
            )]))
            // global column settings
            .set_encoding(Encoding::DELTA_BINARY_PACKED)
            .set_compression(Compression::GZIP(Default::default()))
            .set_dictionary_enabled(false)
            .set_statistics_enabled(EnabledStatistics::None)
            .set_max_statistics_size(50)
            // specific column settings
            .set_column_encoding(ColumnPath::from("col"), Encoding::RLE)
            .set_column_compression(ColumnPath::from("col"), Compression::SNAPPY)
            .set_column_dictionary_enabled(ColumnPath::from("col"), true)
            .set_column_statistics_enabled(ColumnPath::from("col"), EnabledStatistics::Chunk)
            .set_column_max_statistics_size(ColumnPath::from("col"), 123)
            .set_column_bloom_filter_enabled(ColumnPath::from("col"), true)
            .set_column_bloom_filter_ndv(ColumnPath::from("col"), 100_u64)
            .set_column_bloom_filter_fpp(ColumnPath::from("col"), 0.1)
            .build();

        assert_eq!(props.writer_version(), WriterVersion::PARQUET_2_0);
        assert_eq!(props.data_page_size_limit(), 10);
        assert_eq!(props.dictionary_page_size_limit(), 20);
        assert_eq!(props.write_batch_size(), 30);
        assert_eq!(props.max_row_group_size(), 40);
        assert_eq!(props.created_by(), "default");
        assert_eq!(
            props.key_value_metadata(),
            Some(&vec![
                KeyValue::new("key".to_string(), "value".to_string(),)
            ])
        );

        assert_eq!(
            props.encoding(&ColumnPath::from("a")),
            Some(Encoding::DELTA_BINARY_PACKED)
        );
        assert_eq!(
            props.compression(&ColumnPath::from("a")),
            Compression::GZIP(Default::default())
        );
        assert!(!props.dictionary_enabled(&ColumnPath::from("a")));
        assert_eq!(
            props.statistics_enabled(&ColumnPath::from("a")),
            EnabledStatistics::None
        );
        assert_eq!(props.max_statistics_size(&ColumnPath::from("a")), 50);

        assert_eq!(
            props.encoding(&ColumnPath::from("col")),
            Some(Encoding::RLE)
        );
        assert_eq!(
            props.compression(&ColumnPath::from("col")),
            Compression::SNAPPY
        );
        assert!(props.dictionary_enabled(&ColumnPath::from("col")));
        assert_eq!(
            props.statistics_enabled(&ColumnPath::from("col")),
            EnabledStatistics::Chunk
        );
        assert_eq!(props.max_statistics_size(&ColumnPath::from("col")), 123);
        assert_eq!(
            props.bloom_filter_properties(&ColumnPath::from("col")),
            Some(&BloomFilterProperties { fpp: 0.1, ndv: 100 })
        );
    }

    #[test]
    fn test_writer_properties_builder_partial_defaults() {
        let props = WriterProperties::builder()
            .set_encoding(Encoding::DELTA_BINARY_PACKED)
            .set_compression(Compression::GZIP(Default::default()))
            .set_bloom_filter_enabled(true)
            .set_column_encoding(ColumnPath::from("col"), Encoding::RLE)
            .build();

        assert_eq!(
            props.encoding(&ColumnPath::from("col")),
            Some(Encoding::RLE)
        );
        assert_eq!(
            props.compression(&ColumnPath::from("col")),
            Compression::GZIP(Default::default())
        );
        assert_eq!(
            props.dictionary_enabled(&ColumnPath::from("col")),
            DEFAULT_DICTIONARY_ENABLED
        );
        assert_eq!(
            props.bloom_filter_properties(&ColumnPath::from("col")),
            Some(&BloomFilterProperties {
                fpp: 0.05,
                ndv: 1_000_000_u64
            })
        );
    }

    #[test]
    fn test_writer_properties_bloom_filter_ndv_fpp_set() {
        assert_eq!(
            WriterProperties::builder()
                .build()
                .bloom_filter_properties(&ColumnPath::from("col")),
            None
        );
        assert_eq!(
            WriterProperties::builder()
                .set_bloom_filter_ndv(100)
                .build()
                .bloom_filter_properties(&ColumnPath::from("col")),
            Some(&BloomFilterProperties {
                fpp: 0.05,
                ndv: 100
            })
        );
        assert_eq!(
            WriterProperties::builder()
                .set_bloom_filter_fpp(0.1)
                .build()
                .bloom_filter_properties(&ColumnPath::from("col")),
            Some(&BloomFilterProperties {
                fpp: 0.1,
                ndv: 1_000_000_u64
            })
        );
    }

    #[test]
    fn test_reader_properties_default_settings() {
        let props = ReaderProperties::builder().build();

        let codec_options = CodecOptionsBuilder::default()
            .set_backward_compatible_lz4(true)
            .build();

        assert_eq!(props.codec_options(), &codec_options);
        assert!(!props.read_bloom_filter());
    }

    #[test]
    fn test_reader_properties_builder() {
        let props = ReaderProperties::builder()
            .set_backward_compatible_lz4(false)
            .build();

        let codec_options = CodecOptionsBuilder::default()
            .set_backward_compatible_lz4(false)
            .build();

        assert_eq!(props.codec_options(), &codec_options);
    }

    #[test]
    fn test_parse_writerversion() {
        let mut writer_version = "PARQUET_1_0".parse::<WriterVersion>().unwrap();
        assert_eq!(writer_version, WriterVersion::PARQUET_1_0);
        writer_version = "PARQUET_2_0".parse::<WriterVersion>().unwrap();
        assert_eq!(writer_version, WriterVersion::PARQUET_2_0);

        // test lowercase
        writer_version = "parquet_1_0".parse::<WriterVersion>().unwrap();
        assert_eq!(writer_version, WriterVersion::PARQUET_1_0);

        // test invalid version
        match "PARQUET_-1_0".parse::<WriterVersion>() {
            Ok(_) => panic!("Should not be able to parse PARQUET_-1_0"),
            Err(e) => {
                assert_eq!(e, "Invalid writer version: PARQUET_-1_0");
            }
        }
    }

    #[test]
    fn test_parse_enabledstatistics() {
        let mut enabled_statistics = "NONE".parse::<EnabledStatistics>().unwrap();
        assert_eq!(enabled_statistics, EnabledStatistics::None);
        enabled_statistics = "CHUNK".parse::<EnabledStatistics>().unwrap();
        assert_eq!(enabled_statistics, EnabledStatistics::Chunk);
        enabled_statistics = "PAGE".parse::<EnabledStatistics>().unwrap();
        assert_eq!(enabled_statistics, EnabledStatistics::Page);

        // test lowercase
        enabled_statistics = "none".parse::<EnabledStatistics>().unwrap();
        assert_eq!(enabled_statistics, EnabledStatistics::None);

        //test invalid statistics
        match "ChunkAndPage".parse::<EnabledStatistics>() {
            Ok(_) => panic!("Should not be able to parse ChunkAndPage"),
            Err(e) => {
                assert_eq!(e, "Invalid statistics arg: ChunkAndPage");
            }
        }
    }
}
