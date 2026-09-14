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

//! Experimental reader for the Parquet modular footer.
//!
//! A modular footer separates scan-critical metadata from optional metadata. The root directory,
//! schema, and column-chunk placement can therefore fit in one speculative tail read even when
//! statistics and page indexes are large. Placement arrays are column-major and randomly
//! accessible, so this reader decodes values only for projected columns.

use std::collections::HashMap;
use std::io::Write;
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;

use crate::arrow::async_reader::{AsyncFooterLoader, MetadataFetch, ScanFooter};
use crate::basic::{BoundaryOrder, CompressionCodec, Encoding, Type};
use crate::data_type::{ByteArray, FixedLenByteArray, Int96};
use crate::errors::{ParquetError, Result};
use crate::file::metadata::page_index::{PageIndex, PageIndexBuilder};
use crate::file::metadata::thrift::parquet_schema_from_bytes;
use crate::file::metadata::{
    ColumnChunkMetaData, ColumnIndexBuilder, KeyValue, OffsetIndexBuilder, ParquetMetaData,
};
use crate::file::statistics::{Statistics, ValueStatistics};
use crate::parquet_thrift::{
    ElementType, FieldType, ReadThrift, ThriftCompactInputProtocol, ThriftCompactOutputProtocol,
    ThriftSliceInputProtocol, WriteThrift, WriteThriftField, read_thrift_vec,
};
use crate::schema::types::SchemaDescriptor;
use crate::{thrift_struct, thrift_union};
use futures::future::BoxFuture;

const TRAILER_SIZE: usize = 20;
const MAGIC: &[u8; 4] = b"MFP1";
const DEFAULT_PREFETCH_SIZE: usize = 64 * 1024;
const SCHEMA_MODULE: i32 = 0;
const PLACEMENT_MODULE: i32 = 1;
const ROW_GROUP_STATISTICS_MODULE: i32 = 2;
const OFFSET_INDEX_MODULE: i32 = 3;
const COLUMN_INDEX_MODULE: i32 = 4;
const FILE_METADATA_MODULE: i32 = 5;

thrift_struct!(
struct ModuleLocation {
  1: required i64 offset;
  2: required i64 length
}
);

thrift_struct!(
struct ModuleDirectoryEntry {
  1: required i32 kind;
  2: required ModuleLocation location
}
);

thrift_struct!(
struct ModularFooter {
  1: required i32 version;
  2: required i32 num_row_groups;
  3: required i32 num_columns;
  4: required i64 num_rows;
  5: required list<i64> row_group_num_rows;
  6: required list<ModuleDirectoryEntry> modules
}
);

thrift_struct!(
pub struct BitsetParameters {
  1: required i8 value_bit_width;
  2: required i32 num_present
}
);

thrift_struct!(
pub struct PresentIndexParameters {
  1: required i32 num_present;
  2: required i8 position_bit_width;
  3: required i8 value_bit_width
}
);

thrift_union!(
union ArrayEncodingParameters {
  1: (BitsetParameters) Bitset
  2: (PresentIndexParameters) PresentIndex
}
);

thrift_struct!(
struct ArrayPage<'a> {
  1: required binary<'a> data;
  2: required i32 encoding;
  3: required i32 num_values;
  4: required ArrayEncodingParameters parameters
}
);

thrift_struct!(
struct PlacementModule<'a> {
  1: required ArrayPage<'a> data_page_offsets;
  2: required ArrayPage<'a> first_dictionary_pages;
  3: required ArrayPage<'a> dictionary_page_offsets;
  4: required ArrayPage<'a> total_compressed_sizes;
  5: required ArrayPage<'a> total_uncompressed_sizes;
  6: required ArrayPage<'a> num_values;
  7: required ArrayPage<'a> codecs;
  8: required ArrayPage<'a> physical_types;
  9: required ArrayPage<'a> is_fully_dictionary_encoded
}
);

thrift_struct!(
struct RowGroupStatisticsModule<'a> {
  1: required ArrayPage<'a> column_offsets
}
);

thrift_struct!(
struct ColumnStatistics<'a> {
  1: optional ArrayPage<'a> null_counts;
  2: optional ArrayPage<'a> minmax_prefixes;
  3: optional ArrayPage<'a> min_suffixes;
  4: optional ArrayPage<'a> max_suffixes;
  5: optional ArrayPage<'a> min_is_exact;
  6: optional ArrayPage<'a> max_is_exact;
  7: optional ArrayPage<'a> nan_counts
}
);

thrift_struct!(
struct PageIndexModule<'a> {
  1: required ArrayPage<'a> chunk_offsets
}
);

thrift_struct!(
struct OffsetIndexChunk<'a> {
  1: required ArrayPage<'a> offsets;
  2: required ArrayPage<'a> compressed_page_sizes;
  3: required ArrayPage<'a> first_row_indexes
}
);

thrift_struct!(
struct ColumnIndexChunk<'a> {
  1: required i32 boundary_order;
  2: required ArrayPage<'a> null_pages;
  3: optional ArrayPage<'a> null_counts;
  4: optional ArrayPage<'a> minmax_prefixes;
  5: optional ArrayPage<'a> min_suffixes;
  6: optional ArrayPage<'a> max_suffixes;
  7: optional ArrayPage<'a> min_is_exact;
  8: optional ArrayPage<'a> max_is_exact;
  9: optional ArrayPage<'a> nan_counts
}
);

thrift_struct!(
struct FileMetadataModule {
  1: optional string created_by;
  2: optional list<KeyValue> key_value_metadata
}
);

/// Row-group minimum and maximum for a filter column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModularRowGroupStatistics {
    /// Row-group ordinal.
    pub row_group_index: usize,
    /// Encoded minimum value, if present.
    pub min: Option<Bytes>,
    /// Encoded maximum value, if present.
    pub max: Option<Bytes>,
    /// Number of null values, if recorded.
    pub null_count: Option<u64>,
    /// Number of NaN values, if recorded for a floating-point column.
    pub nan_count: Option<u64>,
    /// Whether the minimum is exact. False when the minimum is absent.
    pub min_is_exact: bool,
    /// Whether the maximum is exact. False when the maximum is absent.
    pub max_is_exact: bool,
}

/// Placement metadata for one projected column chunk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModularColumnChunk {
    /// Projected leaf-column ordinal.
    pub column_index: usize,
    /// Row-group ordinal.
    pub row_group_index: usize,
    /// Parquet physical type enum value.
    pub physical_type: u64,
    /// Parquet compression codec enum value.
    pub codec: u64,
    /// First data-page byte offset.
    pub data_page_offset: u64,
    /// Dictionary-page byte offsets for this chunk.
    pub dictionary_page_offsets: Vec<u64>,
    /// Total compressed size in bytes.
    pub total_compressed_size: u64,
    /// Total uncompressed size in bytes.
    pub total_uncompressed_size: u64,
    /// Number of values in the chunk.
    pub num_values: u64,
    /// Whether every data page uses dictionary encoding.
    pub is_fully_dictionary_encoded: bool,
}

/// Scan-critical metadata decoded from a modular footer.
#[derive(Debug, Clone)]
pub struct ModularFooterMetadata {
    /// Parquet file version.
    pub version: i32,
    /// Total number of rows in the file.
    pub num_rows: i64,
    /// Number of leaf columns in the file.
    pub num_columns: usize,
    /// Number of rows in each row group.
    pub row_group_num_rows: Vec<i64>,
    /// Decoded Parquet schema.
    pub schema: Arc<SchemaDescriptor>,
    /// Application that wrote the file, when descriptive metadata is present.
    pub created_by: Option<String>,
    /// File key/value metadata, including an embedded `ARROW:schema` when present.
    pub key_value_metadata: Option<Vec<KeyValue>>,
    /// Placement for only the requested columns, grouped in projection then row-group order.
    pub column_chunks: Vec<ModularColumnChunk>,
    statistics_directory: Option<Range<u64>>,
    offset_index_directory: Option<Range<u64>>,
    column_index_directory: Option<Range<u64>>,
    tail: Bytes,
    tail_start: u64,
    modular_start: u64,
    root_start: u64,
    relative_offsets: bool,
    statistics: HashMap<usize, Vec<Option<Statistics>>>,
}

impl ModularFooterMetadata {
    pub(crate) fn has_offset_indexes(&self) -> bool {
        self.offset_index_directory.is_some()
    }

    pub(crate) fn has_column_indexes(&self) -> bool {
        self.column_index_directory.is_some()
    }

    /// Materializes standard Parquet metadata for `columns`.
    ///
    /// The projected schema retains the ancestors and annotations of selected leaves and contains
    /// no placeholder metadata for unprojected columns.
    ///
    /// Statistics previously requested with [`Self::load_row_group_statistics`] are installed on
    /// the corresponding column chunks. This lets callers inspect the ordinary
    /// [`ParquetMetaData`] statistics, choose row groups, and then construct an async reader with
    /// [`crate::arrow::async_reader::ParquetRecordBatchStreamBuilder::new_with_metadata`].
    pub fn projected_parquet_metadata(&self, columns: &[usize]) -> Result<ParquetMetaData> {
        crate::arrow::async_reader::projected_parquet_metadata(self, columns)
    }

    /// Fetches and decodes row-group min/max values for one filter column.
    ///
    /// The statistics directory is normally served from the speculative tail read. Only the
    /// selected column's independently serialized descriptor is fetched from storage.
    pub async fn load_row_group_statistics<F: MetadataFetch>(
        &mut self,
        fetch: &mut F,
        column: usize,
    ) -> Result<Vec<ModularRowGroupStatistics>> {
        if column >= self.num_columns {
            return Err(general_err!("filter column {column} is out of range"));
        }
        let range = self
            .statistics_directory
            .clone()
            .ok_or_else(|| general_err!("modular footer has no row-group statistics"))?;
        let bytes = get_range(fetch, &self.tail, self.tail_start, range).await?;
        let directory: RowGroupStatisticsModule =
            decode_thrift(&bytes, "row-group statistics directory")?;
        validate_array(
            &directory.column_offsets,
            self.num_columns + 1,
            "column_offsets",
        )?;
        let mut start = required_value(&directory.column_offsets, column)?;
        let mut end = required_value(&directory.column_offsets, column + 1)?;
        // Early MFP1 writers used offsets relative to the modular region here, matching their
        // module-directory offsets. Accept both forms while the format is incubating.
        if self.relative_offsets {
            start = self
                .modular_start
                .checked_add(start)
                .ok_or_else(|| general_err!("column statistics offset overflow"))?;
            end = self
                .modular_start
                .checked_add(end)
                .ok_or_else(|| general_err!("column statistics offset overflow"))?;
        }
        if start < self.modular_start || end > self.root_start || end < start {
            return Err(general_err!("invalid column statistics range"));
        }
        if start == end {
            return Ok(vec![]);
        }
        let descriptor = get_range(fetch, &self.tail, self.tail_start, start..end).await?;
        let statistics: ColumnStatistics = decode_thrift(&descriptor, "column statistics")?;
        let decoded = decode_row_group_statistics(&statistics, self.row_group_num_rows.len())?;
        let physical_type = self.schema.column(column).physical_type();
        let standard = decoded
            .iter()
            .map(|value| value.to_parquet_statistics(physical_type))
            .collect::<Result<Vec<_>>>()?;
        self.statistics.insert(column, standard);
        Ok(decoded)
    }

    /// Fetches standard row-group statistics for the selected filter columns.
    ///
    /// No statistics descriptors are fetched until this method is called. Each selected column
    /// is independently addressable, so descriptors for other projected columns remain unread.
    pub async fn load_statistics<F: MetadataFetch>(
        &mut self,
        fetch: &mut F,
        filter_columns: &[usize],
    ) -> Result<()> {
        if filter_columns.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(general_err!("filter columns must be sorted and unique"));
        }
        for &column in filter_columns {
            self.load_row_group_statistics(fetch, column).await?;
        }
        Ok(())
    }

    /// Fetches and decodes page indexes for only the selected leaf columns.
    pub(crate) async fn load_page_indexes<F: MetadataFetch + Send>(
        &self,
        fetch: &mut F,
        columns: &[usize],
        load_column_indexes: bool,
        load_offset_indexes: bool,
    ) -> Result<PageIndex> {
        let row_groups = self.row_group_num_rows.len();
        let mut builder = PageIndexBuilder::default();
        if load_column_indexes {
            builder.allocate_column_indexes(row_groups, columns.len());
            let range = self
                .column_index_directory
                .clone()
                .ok_or_else(|| general_err!("modular footer has no column-index module"))?;
            let offsets = self.load_page_index_directory(fetch, range).await?;
            for (projected, row_group, bytes) in self
                .load_page_index_chunks(fetch, &offsets, columns)
                .await?
            {
                let chunk: ColumnIndexChunk = decode_thrift(&bytes, "column index chunk")?;
                builder.put_column_index(
                    decode_column_index_chunk(
                        &chunk,
                        self.schema.column(columns[projected]).physical_type(),
                    )?,
                    row_group,
                    projected,
                );
            }
        }
        if load_offset_indexes {
            builder.allocate_offset_indexes(row_groups, columns.len());
            let range = self
                .offset_index_directory
                .clone()
                .ok_or_else(|| general_err!("modular footer has no offset-index module"))?;
            let offsets = self.load_page_index_directory(fetch, range).await?;
            for (projected, row_group, bytes) in self
                .load_page_index_chunks(fetch, &offsets, columns)
                .await?
            {
                let chunk: OffsetIndexChunk = decode_thrift(&bytes, "offset index chunk")?;
                builder.put_offset_index(decode_offset_index_chunk(&chunk)?, row_group, projected);
            }
        }
        Ok(builder.build())
    }

    async fn load_page_index_directory<F: MetadataFetch>(
        &self,
        fetch: &mut F,
        range: Range<u64>,
    ) -> Result<Vec<u64>> {
        let bytes = get_range(fetch, &self.tail, self.tail_start, range).await?;
        let directory: PageIndexModule = decode_thrift(&bytes, "page index directory")?;
        let count = self.num_columns * self.row_group_num_rows.len() + 1;
        validate_array(&directory.chunk_offsets, count, "chunk_offsets")?;
        (0..count)
            .map(|index| required_value(&directory.chunk_offsets, index))
            .collect()
    }

    async fn load_page_index_chunks<F: MetadataFetch + Send>(
        &self,
        fetch: &mut F,
        offsets: &[u64],
        columns: &[usize],
    ) -> Result<Vec<(usize, usize, Bytes)>> {
        let row_groups = self.row_group_num_rows.len();
        let mut positions = Vec::new();
        let mut ranges = Vec::new();
        for (projected, &column) in columns.iter().enumerate() {
            for row_group in 0..row_groups {
                let chunk = column * row_groups + row_group;
                let (mut start, mut end) = (offsets[chunk], offsets[chunk + 1]);
                if self.relative_offsets {
                    start = self
                        .modular_start
                        .checked_add(start)
                        .ok_or_else(|| general_err!("page index chunk offset overflow"))?;
                    end = self
                        .modular_start
                        .checked_add(end)
                        .ok_or_else(|| general_err!("page index chunk offset overflow"))?;
                }
                if start > end || start < self.modular_start || end > self.root_start {
                    return Err(general_err!("invalid page index chunk range"));
                }
                if start != end {
                    positions.push((projected, row_group));
                    ranges.push(start..end);
                }
            }
        }
        let bytes = get_ranges(fetch, &self.tail, self.tail_start, ranges).await?;
        Ok(positions
            .into_iter()
            .zip(bytes)
            .map(|((projected, row_group), bytes)| (projected, row_group, bytes))
            .collect())
    }
}

#[cfg(test)]
pub(super) fn modularize_legacy_file(data: Bytes, metadata: &ParquetMetaData) -> Bytes {
    fn encode<T: WriteThrift>(value: &T) -> Vec<u8> {
        let mut output = Vec::new();
        value
            .write_thrift(&mut ThriftCompactOutputProtocol::new(&mut output))
            .unwrap();
        output
    }
    fn dense(values: &[u64]) -> ArrayPage<'static> {
        let width = values
            .iter()
            .copied()
            .max()
            .map_or(0, |value| 64 - value.leading_zeros() as i8);
        let mut bytes = vec![0; (values.len() * width as usize).div_ceil(8)];
        for (index, value) in values.iter().enumerate() {
            for bit in 0..width as usize {
                let offset = index * width as usize + bit;
                bytes[offset / 8] |= (((value >> bit) & 1) as u8) << (offset % 8);
            }
        }
        ArrayPage {
            data: Box::leak(bytes.into_boxed_slice()),
            encoding: 0,
            num_values: values.len() as i32,
            parameters: ArrayEncodingParameters::Bitset(BitsetParameters {
                value_bit_width: width,
                num_present: values.len() as i32,
            }),
        }
    }
    fn dense_bytes(values: &[Vec<u8>]) -> ArrayPage<'static> {
        let positions: Vec<_> = (0..values.len() as u64).collect();
        let mut offsets = Vec::with_capacity(values.len() + 1);
        offsets.push(0);
        for value in values {
            offsets.push(offsets.last().copied().unwrap() + value.len() as u64);
        }
        let position_width = positions.last().map_or(0, |x| 64 - x.leading_zeros() as i8);
        let offset_width = offsets.last().map_or(0, |x| 64 - x.leading_zeros() as i8);
        let positions = dense(&positions);
        let offsets = dense(&offsets);
        let mut data = positions.data.to_vec();
        data.extend_from_slice(offsets.data);
        for value in values {
            data.extend_from_slice(value);
        }
        ArrayPage {
            data: Box::leak(data.into_boxed_slice()),
            encoding: 1,
            num_values: values.len() as i32,
            parameters: ArrayEncodingParameters::PresentIndex(PresentIndexParameters {
                num_present: values.len() as i32,
                position_bit_width: position_width,
                value_bit_width: offset_width,
            }),
        }
    }

    let footer_len =
        u32::from_le_bytes(data[data.len() - 8..data.len() - 4].try_into().unwrap()) as usize;
    let legacy_footer = &data[data.len() - 8 - footer_len..data.len() - 8];
    let schema_start = legacy_footer
        .iter()
        .position(|&byte| byte == 0x19)
        .expect("legacy footer has a schema field");
    let schema = &legacy_footer[schema_start..];
    let row_groups = metadata.num_row_groups();
    let columns = metadata.file_metadata().schema_descr().num_columns();
    let chunks = columns * row_groups;
    let mut data_page_offsets = Vec::with_capacity(chunks);
    let mut dictionary_page_offsets = Vec::new();
    let mut first_dictionary_pages = Vec::with_capacity(chunks + 1);
    let mut compressed_sizes = Vec::with_capacity(chunks);
    let mut uncompressed_sizes = Vec::with_capacity(chunks);
    let mut num_values = Vec::with_capacity(chunks);
    let mut codecs = Vec::with_capacity(chunks);
    let mut dictionary_encoded = Vec::with_capacity(chunks);
    first_dictionary_pages.push(0);
    for column in 0..columns {
        for row_group in 0..row_groups {
            let chunk = metadata.row_group(row_group).column(column);
            data_page_offsets.push(chunk.data_page_offset() as u64);
            if let Some(offset) = chunk.dictionary_page_offset() {
                dictionary_page_offsets.push(offset as u64);
            }
            first_dictionary_pages.push(dictionary_page_offsets.len() as u64);
            compressed_sizes.push(chunk.compressed_size() as u64);
            uncompressed_sizes.push(chunk.uncompressed_size() as u64);
            num_values.push(chunk.num_values() as u64);
            codecs.push(chunk.compression_codec() as i32 as u64);
            dictionary_encoded.push(0);
        }
    }
    let physical_types = metadata
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .map(|column| column.physical_type() as i32 as u64)
        .collect::<Vec<_>>();
    let placement = encode(&PlacementModule {
        data_page_offsets: dense(&data_page_offsets),
        first_dictionary_pages: dense(&first_dictionary_pages),
        dictionary_page_offsets: dense(&dictionary_page_offsets),
        total_compressed_sizes: dense(&compressed_sizes),
        total_uncompressed_sizes: dense(&uncompressed_sizes),
        num_values: dense(&num_values),
        codecs: dense(&codecs),
        physical_types: dense(&physical_types),
        is_fully_dictionary_encoded: dense(&dictionary_encoded),
    });
    let file_metadata = encode(&FileMetadataModule {
        created_by: metadata.file_metadata().created_by().map(str::to_owned),
        key_value_metadata: metadata.file_metadata().key_value_metadata().cloned(),
    });

    // The test converter emits statistics for the first leaf. This is sufficient to exercise a
    // predicate column that is independent of the projected data column.
    let mut prefixes = Vec::with_capacity(row_groups);
    let mut min_suffixes = Vec::with_capacity(row_groups);
    let mut max_suffixes = Vec::with_capacity(row_groups);
    let mut null_counts = Vec::with_capacity(row_groups);
    let mut min_is_exact = Vec::with_capacity(row_groups);
    let mut max_is_exact = Vec::with_capacity(row_groups);
    for row_group in metadata.row_groups() {
        let statistics = row_group
            .column(0)
            .statistics()
            .expect("test data has statistics for its first column");
        let min = statistics.min_bytes_opt().expect("test data has a minimum");
        let max = statistics.max_bytes_opt().expect("test data has a maximum");
        let prefix = min.iter().zip(max).take_while(|(a, b)| a == b).count();
        prefixes.push(min[..prefix].to_vec());
        min_suffixes.push(min[prefix..].to_vec());
        max_suffixes.push(max[prefix..].to_vec());
        null_counts.push(statistics.null_count_opt().unwrap_or(0));
        min_is_exact.push(u64::from(statistics.min_is_exact()));
        max_is_exact.push(u64::from(statistics.max_is_exact()));
    }
    let statistics = encode(&ColumnStatistics {
        null_counts: Some(dense(&null_counts)),
        minmax_prefixes: Some(dense_bytes(&prefixes)),
        min_suffixes: Some(dense_bytes(&min_suffixes)),
        max_suffixes: Some(dense_bytes(&max_suffixes)),
        min_is_exact: Some(dense(&min_is_exact)),
        max_is_exact: Some(dense(&max_is_exact)),
        nan_counts: None,
    });

    let mut output = data.to_vec();
    let modular_start = output.len() as u64;
    output.extend_from_slice(&statistics);
    let schema_start = output.len() as u64;
    output.extend_from_slice(schema);
    let placement_start = output.len() as u64;
    output.extend_from_slice(&placement);
    let file_metadata_start = output.len() as u64;
    output.extend_from_slice(&file_metadata);
    let statistics_directory_start = output.len() as u64;
    let statistics_end = modular_start + statistics.len() as u64;
    let mut statistics_offsets = vec![statistics_end; columns + 1];
    statistics_offsets[0] = modular_start;
    let statistics_directory = encode(&RowGroupStatisticsModule {
        column_offsets: dense(&statistics_offsets),
    });
    output.extend_from_slice(&statistics_directory);
    let root_start = output.len() as u64;
    let root = encode(&ModularFooter {
        version: metadata.file_metadata().version(),
        num_row_groups: row_groups as i32,
        num_columns: columns as i32,
        num_rows: metadata.file_metadata().num_rows(),
        row_group_num_rows: metadata
            .row_groups()
            .iter()
            .map(|row_group| row_group.num_rows())
            .collect(),
        modules: vec![
            ModuleDirectoryEntry {
                kind: SCHEMA_MODULE,
                location: ModuleLocation {
                    offset: schema_start as i64,
                    length: schema.len() as i64,
                },
            },
            ModuleDirectoryEntry {
                kind: PLACEMENT_MODULE,
                location: ModuleLocation {
                    offset: placement_start as i64,
                    length: placement.len() as i64,
                },
            },
            ModuleDirectoryEntry {
                kind: FILE_METADATA_MODULE,
                location: ModuleLocation {
                    offset: file_metadata_start as i64,
                    length: file_metadata.len() as i64,
                },
            },
            ModuleDirectoryEntry {
                kind: ROW_GROUP_STATISTICS_MODULE,
                location: ModuleLocation {
                    offset: statistics_directory_start as i64,
                    length: statistics_directory.len() as i64,
                },
            },
        ],
    });
    output.extend_from_slice(&root);
    output.extend_from_slice(&(modular_start as i64).to_le_bytes());
    output.extend_from_slice(&((root_start - modular_start) as i64).to_le_bytes());
    output.extend_from_slice(MAGIC);
    output.into()
}

impl ScanFooter for ModularFooterMetadata {
    fn version(&self) -> i32 {
        self.version
    }

    fn num_rows(&self) -> i64 {
        self.num_rows
    }

    fn schema(&self) -> &SchemaDescriptor {
        &self.schema
    }

    fn row_group_num_rows(&self) -> &[i64] {
        &self.row_group_num_rows
    }

    fn created_by(&self) -> Option<&str> {
        self.created_by.as_deref()
    }

    fn key_value_metadata(&self) -> Option<&[KeyValue]> {
        self.key_value_metadata.as_deref()
    }

    fn column_chunk(
        &self,
        row_group_index: usize,
        column_index: usize,
        projected_column: Arc<crate::schema::types::ColumnDescriptor>,
    ) -> Result<ColumnChunkMetaData> {
        let chunk = self
            .column_chunks
            .iter()
            .find(|chunk| {
                chunk.column_index == column_index && chunk.row_group_index == row_group_index
            })
            .ok_or_else(|| {
                general_err!("column {column_index} in row group {row_group_index} was not loaded")
            })?;
        let mut metadata =
            chunk.to_column_chunk_metadata(self.schema.column(column_index), projected_column)?;
        if let Some(statistics) = self
            .statistics
            .get(&column_index)
            .and_then(|statistics| statistics.get(row_group_index))
            .cloned()
            .flatten()
        {
            metadata = metadata.into_builder().set_statistics(statistics).build()?;
        }
        Ok(metadata)
    }
}

impl ModularRowGroupStatistics {
    fn to_parquet_statistics(&self, physical_type: Type) -> Result<Option<Statistics>> {
        let (Some(min), Some(max)) = (&self.min, &self.max) else {
            if self.min.is_some() != self.max.is_some() {
                return Err(general_err!(
                    "modular statistics has only one min/max bound"
                ));
            }
            if self.null_count.is_none() && self.nan_count.is_none() {
                return Ok(None);
            }
            let statistics = match physical_type {
                Type::BOOLEAN => Statistics::new::<bool>(None, None, None, self.null_count, false),
                Type::INT32 => Statistics::new::<i32>(None, None, None, self.null_count, false),
                Type::INT64 => Statistics::new::<i64>(None, None, None, self.null_count, false),
                Type::INT96 => Statistics::new::<Int96>(None, None, None, self.null_count, false),
                Type::FLOAT => {
                    let Statistics::Float(value) =
                        Statistics::new::<f32>(None, None, None, self.null_count, false)
                    else {
                        unreachable!()
                    };
                    Statistics::Float(value.with_nan_count(self.nan_count))
                }
                Type::DOUBLE => {
                    let Statistics::Double(value) =
                        Statistics::new::<f64>(None, None, None, self.null_count, false)
                    else {
                        unreachable!()
                    };
                    Statistics::Double(value.with_nan_count(self.nan_count))
                }
                Type::BYTE_ARRAY => {
                    Statistics::new::<ByteArray>(None, None, None, self.null_count, false)
                }
                Type::FIXED_LEN_BYTE_ARRAY => {
                    Statistics::new::<FixedLenByteArray>(None, None, None, self.null_count, false)
                }
            };
            return Ok(Some(statistics));
        };
        macro_rules! primitive {
            ($type:ty, $variant:ident) => {{
                let min = <$type>::from_le_bytes(
                    min.as_ref()
                        .try_into()
                        .map_err(|_| general_err!("invalid modular statistics byte width"))?,
                );
                let max = <$type>::from_le_bytes(
                    max.as_ref()
                        .try_into()
                        .map_err(|_| general_err!("invalid modular statistics byte width"))?,
                );
                Statistics::$variant(
                    ValueStatistics::new(Some(min), Some(max), None, self.null_count, false)
                        .with_min_is_exact(self.min_is_exact)
                        .with_max_is_exact(self.max_is_exact),
                )
            }};
        }
        let statistics = match physical_type {
            Type::BOOLEAN => {
                if min.len() != 1 || max.len() != 1 || min[0] > 1 || max[0] > 1 {
                    return Err(general_err!("invalid BOOLEAN modular statistics value"));
                }
                Statistics::Boolean(
                    ValueStatistics::new(
                        Some(min[0] != 0),
                        Some(max[0] != 0),
                        None,
                        self.null_count,
                        false,
                    )
                    .with_min_is_exact(self.min_is_exact)
                    .with_max_is_exact(self.max_is_exact),
                )
            }
            Type::INT32 => primitive!(i32, Int32),
            Type::INT64 => primitive!(i64, Int64),
            Type::FLOAT => {
                let Statistics::Float(value) = primitive!(f32, Float) else {
                    unreachable!()
                };
                Statistics::Float(value.with_nan_count(self.nan_count))
            }
            Type::DOUBLE => {
                let Statistics::Double(value) = primitive!(f64, Double) else {
                    unreachable!()
                };
                Statistics::Double(value.with_nan_count(self.nan_count))
            }
            Type::INT96 => {
                if min.len() != 12 || max.len() != 12 {
                    return Err(general_err!("invalid INT96 modular statistics byte width"));
                }
                let decode = |value: &Bytes| {
                    let mut decoded = Int96::new();
                    decoded.set_data(
                        u32::from_le_bytes(value[0..4].try_into().unwrap()),
                        u32::from_le_bytes(value[4..8].try_into().unwrap()),
                        u32::from_le_bytes(value[8..12].try_into().unwrap()),
                    );
                    decoded
                };
                Statistics::Int96(
                    ValueStatistics::new(
                        Some(decode(min)),
                        Some(decode(max)),
                        None,
                        self.null_count,
                        false,
                    )
                    .with_min_is_exact(self.min_is_exact)
                    .with_max_is_exact(self.max_is_exact),
                )
            }
            Type::BYTE_ARRAY => Statistics::ByteArray(
                ValueStatistics::new(
                    Some(ByteArray::from(min.clone())),
                    Some(ByteArray::from(max.clone())),
                    None,
                    self.null_count,
                    false,
                )
                .with_min_is_exact(self.min_is_exact)
                .with_max_is_exact(self.max_is_exact),
            ),
            Type::FIXED_LEN_BYTE_ARRAY => Statistics::FixedLenByteArray(
                ValueStatistics::new(
                    Some(FixedLenByteArray::from(ByteArray::from(min.clone()))),
                    Some(FixedLenByteArray::from(ByteArray::from(max.clone()))),
                    None,
                    self.null_count,
                    false,
                )
                .with_min_is_exact(self.min_is_exact)
                .with_max_is_exact(self.max_is_exact),
            ),
        };
        Ok(Some(statistics))
    }
}

impl ModularColumnChunk {
    fn to_column_chunk_metadata(
        &self,
        source_column: Arc<crate::schema::types::ColumnDescriptor>,
        target_column: Arc<crate::schema::types::ColumnDescriptor>,
    ) -> Result<ColumnChunkMetaData> {
        let num_values = i64::try_from(self.num_values)
            .map_err(|_| general_err!("column chunk num_values exceeds i64"))?;
        let compressed_size = i64::try_from(self.total_compressed_size)
            .map_err(|_| general_err!("column chunk compressed size exceeds i64"))?;
        let uncompressed_size = i64::try_from(self.total_uncompressed_size)
            .map_err(|_| general_err!("column chunk uncompressed size exceeds i64"))?;
        let data_page_offset = i64::try_from(self.data_page_offset)
            .map_err(|_| general_err!("column chunk data-page offset exceeds i64"))?;
        let physical_type = match self.physical_type {
            0 => Type::BOOLEAN,
            1 => Type::INT32,
            2 => Type::INT64,
            3 => Type::INT96,
            4 => Type::FLOAT,
            5 => Type::DOUBLE,
            6 => Type::BYTE_ARRAY,
            7 => Type::FIXED_LEN_BYTE_ARRAY,
            value => return Err(general_err!("invalid Parquet physical type {value}")),
        };
        if physical_type != source_column.physical_type() {
            return Err(general_err!(
                "column chunk physical type does not match schema"
            ));
        }
        let codec = match self.codec {
            0 => CompressionCodec::UNCOMPRESSED,
            1 => CompressionCodec::SNAPPY,
            2 => CompressionCodec::GZIP,
            3 => CompressionCodec::LZO,
            4 => CompressionCodec::BROTLI,
            5 => CompressionCodec::LZ4,
            6 => CompressionCodec::ZSTD,
            7 => CompressionCodec::LZ4_RAW,
            value => return Err(general_err!("invalid Parquet compression codec {value}")),
        };
        let dictionary_page_offset = self
            .dictionary_page_offsets
            .first()
            .copied()
            .map(i64::try_from)
            .transpose()
            .map_err(|_| general_err!("dictionary-page offset exceeds i64"))?;
        let encodings = if self.is_fully_dictionary_encoded {
            vec![Encoding::RLE_DICTIONARY, Encoding::RLE]
        } else {
            vec![Encoding::PLAIN, Encoding::RLE]
        };

        ColumnChunkMetaData::builder(target_column)
            .set_encodings(encodings)
            .set_num_values(num_values)
            .set_compression_codec(codec)
            .set_total_compressed_size(compressed_size)
            .set_total_uncompressed_size(uncompressed_size)
            .set_data_page_offset(data_page_offset)
            .set_dictionary_page_offset(dictionary_page_offset)
            .build()
            .map_err(|error| general_err!("invalid modular-footer column placement: {error}"))
    }
}

/// Loads scan-critical modular-footer metadata using a speculative tail read.
#[derive(Debug, Clone)]
pub struct ModularFooterReader {
    prefetch_size: usize,
    projection: Vec<usize>,
    file_metadata: bool,
}

impl ModularFooterReader {
    /// Creates a reader for the specified leaf-column ordinals.
    pub fn new(projection: Vec<usize>) -> Self {
        Self {
            prefetch_size: DEFAULT_PREFETCH_SIZE,
            projection,
            file_metadata: true,
        }
    }

    /// Sets the speculative tail-read size.
    pub fn with_prefetch_size(mut self, prefetch_size: usize) -> Self {
        self.prefetch_size = prefetch_size.max(TRAILER_SIZE);
        self
    }

    /// Controls whether the optional descriptive file-metadata module is loaded.
    ///
    /// Disable this when the caller supplies an Arrow schema or explicitly ignores embedded Arrow
    /// metadata. This avoids an extra request when the optional module is outside the tail read.
    pub fn with_file_metadata(mut self, file_metadata: bool) -> Self {
        self.file_metadata = file_metadata;
        self
    }

    /// Fetches and decodes the root, schema, and projected placement metadata.
    ///
    /// No additional request is issued when the requested modules are present in the speculative
    /// tail read. Otherwise only missing critical ranges and, when enabled, the descriptive file
    /// metadata range are fetched. Statistics and page indexes remain unloaded until requested.
    pub async fn load<F: MetadataFetch>(
        &self,
        fetch: &mut F,
        file_size: u64,
    ) -> Result<ModularFooterMetadata> {
        if file_size < TRAILER_SIZE as u64 {
            return Err(eof_err!(
                "file size of {file_size} is less than modular footer trailer"
            ));
        }

        let tail_start = file_size.saturating_sub(self.prefetch_size as u64);
        let tail = fetch_exact(fetch, tail_start..file_size).await?;
        let trailer = &tail[tail.len() - TRAILER_SIZE..];
        if &trailer[16..] != MAGIC {
            return Err(general_err!("file does not contain an MFP1 modular footer"));
        }

        let modular_start = read_i64(&trailer[..8], "modular footer start")?;
        let root_offset = read_i64(&trailer[8..16], "modular footer root offset")?;
        let modular_start = non_negative_u64(modular_start, "modular footer start")?;
        let root_offset = non_negative_u64(root_offset, "modular footer root offset")?;
        let root_start = modular_start
            .checked_add(root_offset)
            .ok_or_else(|| general_err!("modular footer root offset overflow"))?;
        let root_end = file_size - TRAILER_SIZE as u64;
        if root_start >= root_end {
            return Err(general_err!("invalid modular footer root range"));
        }

        let root_bytes = get_range(fetch, &tail, tail_start, root_start..root_end).await?;
        let root: ModularFooter = decode_thrift(&root_bytes, "modular footer root")?;
        validate_root(&root)?;
        let relative_offsets = root
            .modules
            .iter()
            .find(|entry| entry.kind == SCHEMA_MODULE)
            .is_some_and(|entry| entry.location.offset < modular_start as i64);

        let schema_range = module_range(&root, SCHEMA_MODULE, modular_start, root_start)?;
        let placement_range = module_range(&root, PLACEMENT_MODULE, modular_start, root_start)?;
        let statistics_directory = optional_module_range(
            &root,
            ROW_GROUP_STATISTICS_MODULE,
            modular_start,
            root_start,
        )?;
        let offset_index_directory =
            optional_module_range(&root, OFFSET_INDEX_MODULE, modular_start, root_start)?;
        let column_index_directory =
            optional_module_range(&root, COLUMN_INDEX_MODULE, modular_start, root_start)?;
        let file_metadata_range = self
            .file_metadata
            .then(|| optional_module_range(&root, FILE_METADATA_MODULE, modular_start, root_start))
            .transpose()?
            .flatten();
        let schema_bytes = get_range(fetch, &tail, tail_start, schema_range).await?;
        let schema = Arc::new(decode_schema(&schema_bytes)?);
        let placement_bytes = get_range(fetch, &tail, tail_start, placement_range).await?;
        let placement: PlacementModule =
            decode_thrift(&placement_bytes, "modular footer placement module")?;
        let column_chunks = decode_projection(&root, &placement, &self.projection)?;
        let file_metadata = match file_metadata_range {
            Some(range) => {
                let bytes = get_range(fetch, &tail, tail_start, range).await?;
                decode_thrift::<FileMetadataModule>(&bytes, "modular footer file metadata")?
            }
            None => FileMetadataModule {
                created_by: None,
                key_value_metadata: None,
            },
        };

        Ok(ModularFooterMetadata {
            version: root.version,
            num_rows: root.num_rows,
            num_columns: root.num_columns as usize,
            row_group_num_rows: root.row_group_num_rows,
            schema,
            created_by: file_metadata.created_by,
            key_value_metadata: file_metadata.key_value_metadata,
            column_chunks,
            statistics_directory,
            offset_index_directory,
            column_index_directory,
            tail,
            tail_start,
            modular_start,
            root_start,
            relative_offsets,
            statistics: HashMap::new(),
        })
    }
}

impl AsyncFooterLoader for ModularFooterReader {
    type Footer = ModularFooterMetadata;

    fn load_footer<'a, F>(
        &'a self,
        fetch: &'a mut F,
        file_size: u64,
    ) -> BoxFuture<'a, Result<Self::Footer>>
    where
        F: MetadataFetch + Send + 'a,
    {
        Box::pin(self.load(fetch, file_size))
    }
}

fn optional_module_range(
    root: &ModularFooter,
    kind: i32,
    modular_start: u64,
    root_start: u64,
) -> Result<Option<Range<u64>>> {
    if root.modules.iter().all(|entry| entry.kind != kind) {
        return Ok(None);
    }
    module_range(root, kind, modular_start, root_start).map(Some)
}

#[cfg(feature = "test_common")]
#[doc(hidden)]
pub fn modular_footer_benchmark_file(
    num_columns: usize,
    num_row_groups: usize,
    optional_metadata_size: usize,
) -> Bytes {
    fn encode<T: WriteThrift>(value: &T) -> Vec<u8> {
        let mut output = Vec::new();
        value
            .write_thrift(&mut ThriftCompactOutputProtocol::new(&mut output))
            .unwrap();
        output
    }
    fn packed(values: &[u64]) -> ArrayPage<'static> {
        let width = values
            .iter()
            .copied()
            .max()
            .map_or(0, |x| 64 - x.leading_zeros() as i8);
        let mut data = vec![0; (values.len() * width as usize).div_ceil(8)];
        for (index, value) in values.iter().enumerate() {
            for bit in 0..width as usize {
                let offset = index * width as usize + bit;
                data[offset / 8] |= (((value >> bit) & 1) as u8) << (offset % 8);
            }
        }
        ArrayPage {
            data: Box::leak(data.into_boxed_slice()),
            encoding: 0,
            num_values: values.len() as i32,
            parameters: ArrayEncodingParameters::Bitset(BitsetParameters {
                value_bit_width: width,
                num_present: values.len() as i32,
            }),
        }
    }
    fn packed_bytes(values: &[Vec<u8>]) -> ArrayPage<'static> {
        let positions: Vec<_> = (0..values.len() as u64).collect();
        let mut offsets = Vec::with_capacity(values.len() + 1);
        offsets.push(0);
        for value in values {
            offsets.push(offsets.last().copied().unwrap() + value.len() as u64);
        }
        let position_width = positions.last().map_or(0, |x| 64 - x.leading_zeros() as i8);
        let offset_width = offsets.last().map_or(0, |x| 64 - x.leading_zeros() as i8);
        let positions = packed(&positions);
        let offsets_page = packed(&offsets);
        let mut data = positions.data.to_vec();
        data.extend_from_slice(offsets_page.data);
        for value in values {
            data.extend_from_slice(value);
        }
        ArrayPage {
            data: Box::leak(data.into_boxed_slice()),
            encoding: 1,
            num_values: values.len() as i32,
            parameters: ArrayEncodingParameters::PresentIndex(PresentIndexParameters {
                num_present: values.len() as i32,
                position_bit_width: position_width,
                value_bit_width: offset_width,
            }),
        }
    }
    fn put_varint(mut value: usize, output: &mut Vec<u8>) {
        loop {
            let byte = (value & 0x7f) as u8;
            value >>= 7;
            output.push(byte | if value == 0 { 0 } else { 0x80 });
            if value == 0 {
                return;
            }
        }
    }
    fn schema(num_columns: usize) -> Vec<u8> {
        let mut output = vec![0x19, 0xfc];
        put_varint(num_columns + 1, &mut output);
        output.extend_from_slice(&[0x48, 0x06]);
        output.extend_from_slice(b"schema");
        output.push(0x15);
        put_varint(num_columns * 2, &mut output);
        output.push(0);
        for column in 0..num_columns {
            let name = column.to_string();
            output.extend_from_slice(&[0x15, 0x08, 0x25, 0x00, 0x18]);
            put_varint(name.len(), &mut output);
            output.extend_from_slice(name.as_bytes());
            output.push(0);
        }
        output.push(0);
        output
    }

    let chunks = num_columns * num_row_groups;
    let offsets: Vec<_> = (0..chunks).map(|x| (x * 1024) as u64).collect();
    let placement = encode(&PlacementModule {
        data_page_offsets: packed(&offsets),
        first_dictionary_pages: packed(&vec![0; chunks + 1]),
        dictionary_page_offsets: packed(&[]),
        total_compressed_sizes: packed(&vec![1024; chunks]),
        total_uncompressed_sizes: packed(&vec![2048; chunks]),
        num_values: packed(&vec![1000; chunks]),
        codecs: packed(&vec![0; chunks]),
        physical_types: packed(&vec![4; num_columns]),
        is_fully_dictionary_encoded: packed(&vec![0; chunks]),
    });
    let mut prefixes = Vec::with_capacity(num_row_groups);
    let mut min_suffixes = Vec::with_capacity(num_row_groups);
    let mut max_suffixes = Vec::with_capacity(num_row_groups);
    for row_group in 0..num_row_groups {
        let min = ((row_group * 1000) as f32).to_le_bytes();
        let max = ((row_group * 1000 + 999) as f32).to_le_bytes();
        let prefix_len = min.iter().zip(&max).take_while(|(a, b)| a == b).count();
        prefixes.push(min[..prefix_len].to_vec());
        min_suffixes.push(min[prefix_len..].to_vec());
        max_suffixes.push(max[prefix_len..].to_vec());
    }
    let column_statistics = encode(&ColumnStatistics {
        null_counts: Some(packed(&vec![3; num_row_groups])),
        minmax_prefixes: Some(packed_bytes(&prefixes)),
        min_suffixes: Some(packed_bytes(&min_suffixes)),
        max_suffixes: Some(packed_bytes(&max_suffixes)),
        min_is_exact: Some(packed(&vec![1; num_row_groups])),
        max_is_exact: Some(packed(&vec![1; num_row_groups])),
        nan_counts: Some(packed(&vec![2; num_row_groups])),
    });
    assert!(column_statistics.len() <= optional_metadata_size);
    let schema = schema(num_columns);
    let modular_start = 4_u64;
    let schema_start = modular_start + optional_metadata_size as u64;
    let placement_start = schema_start + schema.len() as u64;
    let statistics_start = placement_start + placement.len() as u64;
    let descriptor_end = modular_start + column_statistics.len() as u64;
    let mut column_offsets = vec![descriptor_end; num_columns + 1];
    column_offsets[0] = modular_start;
    let statistics_directory = encode(&RowGroupStatisticsModule {
        column_offsets: packed(&column_offsets),
    });
    let root_start = statistics_start + statistics_directory.len() as u64;
    let root = encode(&ModularFooter {
        version: 1,
        num_row_groups: num_row_groups as i32,
        num_columns: num_columns as i32,
        num_rows: (num_row_groups * 1000) as i64,
        row_group_num_rows: vec![1000; num_row_groups],
        modules: vec![
            ModuleDirectoryEntry {
                kind: SCHEMA_MODULE,
                location: ModuleLocation {
                    offset: schema_start as i64,
                    length: schema.len() as i64,
                },
            },
            ModuleDirectoryEntry {
                kind: PLACEMENT_MODULE,
                location: ModuleLocation {
                    offset: placement_start as i64,
                    length: placement.len() as i64,
                },
            },
            ModuleDirectoryEntry {
                kind: ROW_GROUP_STATISTICS_MODULE,
                location: ModuleLocation {
                    offset: statistics_start as i64,
                    length: statistics_directory.len() as i64,
                },
            },
        ],
    });
    let mut file = b"PAR1".to_vec();
    file.extend_from_slice(&column_statistics);
    file.resize(4 + optional_metadata_size, 0xaa);
    file.extend_from_slice(&schema);
    file.extend_from_slice(&placement);
    file.extend_from_slice(&statistics_directory);
    file.extend_from_slice(&root);
    file.extend_from_slice(&(modular_start as i64).to_le_bytes());
    file.extend_from_slice(&((root_start - modular_start) as i64).to_le_bytes());
    file.extend_from_slice(MAGIC);
    file.into()
}

fn decode_schema(module: &[u8]) -> Result<SchemaDescriptor> {
    // SchemaModule field 1 is the same list<SchemaElement> as FileMetaData field 2. Rewrite only
    // its compact-protocol field header and reuse the production Parquet schema decoder. A second
    // SchemaModule field (column_orders) becomes unknown FileMetaData field 3 and is never reached,
    // because parquet_schema_from_bytes returns immediately after decoding the schema.
    let Some((&header, rest)) = module.split_first() else {
        return Err(general_err!("modular footer schema module is empty"));
    };
    if header != 0x19 {
        return Err(general_err!(
            "modular footer schema module does not start with its required schema field"
        ));
    }
    let mut file_metadata = Vec::with_capacity(module.len());
    file_metadata.push(0x29);
    file_metadata.extend_from_slice(rest);
    parquet_schema_from_bytes(&file_metadata)
}

fn validate_root(root: &ModularFooter) -> Result<()> {
    if root.num_row_groups < 0 || root.num_columns < 0 || root.num_rows < 0 {
        return Err(general_err!("modular footer contains a negative count"));
    }
    if root.row_group_num_rows.len() != root.num_row_groups as usize {
        return Err(general_err!(
            "modular footer row-group count does not match row-group row counts"
        ));
    }
    if root.row_group_num_rows.iter().any(|x| *x < 0) {
        return Err(general_err!("modular footer contains a negative row count"));
    }
    Ok(())
}

fn module_range(
    root: &ModularFooter,
    kind: i32,
    modular_start: u64,
    root_start: u64,
) -> Result<Range<u64>> {
    let entry = root
        .modules
        .iter()
        .find(|entry| entry.kind == kind)
        .ok_or_else(|| general_err!("modular footer is missing required module {kind}"))?;
    let offset = non_negative_u64(entry.location.offset, "module offset")?;
    let length = non_negative_u64(entry.location.length, "module length")?;

    // MFP1 initially used offsets relative to modular_start. The schema is the first module in
    // those files, so use its location to choose one offset mode consistently for every module.
    let relative_offsets = root
        .modules
        .iter()
        .find(|entry| entry.kind == SCHEMA_MODULE)
        .is_some_and(|entry| entry.location.offset < modular_start as i64);
    let start = if relative_offsets {
        modular_start
            .checked_add(offset)
            .ok_or_else(|| general_err!("modular footer module offset overflow"))?
    } else {
        offset
    };
    let end = start
        .checked_add(length)
        .ok_or_else(|| general_err!("modular footer module length overflow"))?;
    if start < modular_start || end > root_start || start >= end {
        return Err(general_err!("invalid modular footer module range"));
    }
    Ok(start..end)
}

async fn get_range<F: MetadataFetch>(
    fetch: &mut F,
    tail: &Bytes,
    tail_start: u64,
    range: Range<u64>,
) -> Result<Bytes> {
    if range.start >= tail_start {
        let start: usize = (range.start - tail_start).try_into()?;
        let end: usize = (range.end - tail_start).try_into()?;
        if end > tail.len() {
            return Err(eof_err!("modular footer range extends past the file"));
        }
        return Ok(tail.slice(start..end));
    }
    fetch_exact(fetch, range).await
}

async fn get_ranges<F: MetadataFetch + Send>(
    fetch: &mut F,
    tail: &Bytes,
    tail_start: u64,
    ranges: Vec<Range<u64>>,
) -> Result<Vec<Bytes>> {
    let mut result = vec![None; ranges.len()];
    let mut remote_positions = Vec::new();
    let mut remote_ranges = Vec::new();
    for (position, range) in ranges.iter().enumerate() {
        if range.start >= tail_start {
            let start: usize = (range.start - tail_start).try_into()?;
            let end: usize = (range.end - tail_start).try_into()?;
            if end > tail.len() {
                return Err(eof_err!("modular footer range extends past the file"));
            }
            result[position] = Some(tail.slice(start..end));
        } else {
            remote_positions.push(position);
            remote_ranges.push(range.clone());
        }
    }
    if !remote_ranges.is_empty() {
        let fetched = fetch.fetch_ranges(remote_ranges.clone()).await?;
        if fetched.len() != remote_ranges.len() {
            return Err(eof_err!(
                "modular footer multi-range response count mismatch"
            ));
        }
        for ((position, range), bytes) in
            remote_positions.into_iter().zip(remote_ranges).zip(fetched)
        {
            let expected: usize = (range.end - range.start).try_into()?;
            if bytes.len() != expected {
                return Err(eof_err!(
                    "modular footer requires {expected} bytes, but only read {}",
                    bytes.len()
                ));
            }
            result[position] = Some(bytes);
        }
    }
    result
        .into_iter()
        .map(|bytes| bytes.ok_or_else(|| eof_err!("missing modular footer range response")))
        .collect()
}

async fn fetch_exact<F: MetadataFetch>(fetch: &mut F, range: Range<u64>) -> Result<Bytes> {
    let expected: usize = (range.end - range.start).try_into()?;
    let bytes = fetch.fetch(range).await?;
    if bytes.len() != expected {
        return Err(eof_err!(
            "modular footer requires {expected} bytes, but only read {}",
            bytes.len()
        ));
    }
    Ok(bytes)
}

fn decode_projection(
    root: &ModularFooter,
    placement: &PlacementModule,
    projection: &[usize],
) -> Result<Vec<ModularColumnChunk>> {
    let row_groups = root.num_row_groups as usize;
    let columns = root.num_columns as usize;
    let chunks = row_groups
        .checked_mul(columns)
        .ok_or_else(|| general_err!("modular footer chunk count overflow"))?;

    validate_array(&placement.data_page_offsets, chunks, "data_page_offsets")?;
    validate_array(
        &placement.first_dictionary_pages,
        chunks + 1,
        "first_dictionary_pages",
    )?;
    validate_array(
        &placement.total_compressed_sizes,
        chunks,
        "total_compressed_sizes",
    )?;
    validate_array(
        &placement.total_uncompressed_sizes,
        chunks,
        "total_uncompressed_sizes",
    )?;
    validate_array(&placement.num_values, chunks, "num_values")?;
    validate_array(&placement.codecs, chunks, "codecs")?;
    validate_array(&placement.physical_types, columns, "physical_types")?;
    validate_array(
        &placement.is_fully_dictionary_encoded,
        chunks,
        "is_fully_dictionary_encoded",
    )?;

    let mut out = Vec::with_capacity(projection.len().saturating_mul(row_groups));
    for &column in projection {
        if column >= columns {
            return Err(general_err!(
                "projected column {column} out of range for {columns} columns"
            ));
        }
        let physical_type = value_at(&placement.physical_types, column)?
            .ok_or_else(|| general_err!("physical type is absent for column {column}"))?;
        for row_group in 0..row_groups {
            let chunk = column * row_groups + row_group;
            let first = required_value(&placement.first_dictionary_pages, chunk)?;
            let last = required_value(&placement.first_dictionary_pages, chunk + 1)?;
            if last < first || last > placement.dictionary_page_offsets.num_values as u64 {
                return Err(general_err!("invalid modular footer dictionary-page range"));
            }
            let mut dictionary_page_offsets = Vec::with_capacity((last - first) as usize);
            for index in first..last {
                dictionary_page_offsets.push(required_value(
                    &placement.dictionary_page_offsets,
                    index as usize,
                )?);
            }
            out.push(ModularColumnChunk {
                column_index: column,
                row_group_index: row_group,
                physical_type,
                codec: required_value(&placement.codecs, chunk)?,
                data_page_offset: required_value(&placement.data_page_offsets, chunk)?,
                dictionary_page_offsets,
                total_compressed_size: required_value(&placement.total_compressed_sizes, chunk)?,
                total_uncompressed_size: required_value(
                    &placement.total_uncompressed_sizes,
                    chunk,
                )?,
                num_values: required_value(&placement.num_values, chunk)?,
                is_fully_dictionary_encoded: required_value(
                    &placement.is_fully_dictionary_encoded,
                    chunk,
                )? != 0,
            });
        }
    }
    Ok(out)
}

fn validate_array(array: &ArrayPage, expected: usize, name: &str) -> Result<()> {
    if array.num_values < 0 || array.num_values as usize != expected {
        return Err(general_err!(
            "modular footer {name} has {} values, expected {expected}",
            array.num_values
        ));
    }
    Ok(())
}

fn required_value(array: &ArrayPage, index: usize) -> Result<u64> {
    value_at(array, index)?.ok_or_else(|| general_err!("required modular footer value is absent"))
}

fn value_at(array: &ArrayPage, index: usize) -> Result<Option<u64>> {
    if array.num_values < 0 || index >= array.num_values as usize {
        return Err(general_err!("modular footer array index out of bounds"));
    }
    match (&array.encoding, &array.parameters) {
        (0, ArrayEncodingParameters::Bitset(params)) => {
            let width = bit_width(params.value_bit_width)?;
            if params.num_present < 0 || params.num_present > array.num_values {
                return Err(general_err!("invalid modular footer bitset present count"));
            }
            let bitmap_len = if params.num_present == array.num_values {
                0
            } else {
                (array.num_values as usize).div_ceil(8)
            };
            if array.data.len() < bitmap_len {
                return Err(general_err!("truncated modular footer validity bitmap"));
            }
            if bitmap_len != 0 && array.data[index / 8] & (1 << (index % 8)) == 0 {
                return Ok(None);
            }
            packed_at(&array.data[bitmap_len..], index, width).map(Some)
        }
        (1, ArrayEncodingParameters::PresentIndex(params)) => {
            if params.num_present < 0 || params.num_present > array.num_values {
                return Err(general_err!("invalid modular footer present-index count"));
            }
            let count = params.num_present as usize;
            let position_width = bit_width(params.position_bit_width)?;
            let value_width = bit_width(params.value_bit_width)?;
            let positions_len = packed_len(count, position_width)?;
            if positions_len > array.data.len() {
                return Err(general_err!("truncated modular footer present index"));
            }
            let positions = &array.data[..positions_len];
            let values = &array.data[positions_len..];
            let mut low = 0;
            let mut high = count;
            while low < high {
                let middle = low + (high - low) / 2;
                let position = packed_at(positions, middle, position_width)? as usize;
                match position.cmp(&index) {
                    std::cmp::Ordering::Less => low = middle + 1,
                    std::cmp::Ordering::Greater => high = middle,
                    std::cmp::Ordering::Equal => {
                        return packed_at(values, middle, value_width).map(Some);
                    }
                }
            }
            Ok(None)
        }
        _ => Err(general_err!(
            "modular footer array encoding does not match its parameters"
        )),
    }
}

fn decode_offset_index_chunk(
    chunk: &OffsetIndexChunk<'_>,
) -> Result<crate::file::page_index::offset_index::OffsetIndexMetaData> {
    let pages = usize::try_from(chunk.offsets.num_values)
        .map_err(|_| general_err!("negative offset-index page count"))?;
    validate_array(&chunk.compressed_page_sizes, pages, "compressed_page_sizes")?;
    validate_array(&chunk.first_row_indexes, pages, "first_row_indexes")?;
    let mut builder = OffsetIndexBuilder::new();
    for page in 0..pages {
        let offset = i64::try_from(required_value(&chunk.offsets, page)?)
            .map_err(|_| general_err!("page offset exceeds i64"))?;
        let size = i32::try_from(required_value(&chunk.compressed_page_sizes, page)?)
            .map_err(|_| general_err!("compressed page size exceeds i32"))?;
        builder.append_offset_and_size(offset, size);
        let first_row = required_value(&chunk.first_row_indexes, page)?;
        let next_row = if page + 1 == pages {
            first_row
        } else {
            required_value(&chunk.first_row_indexes, page + 1)?
        };
        if next_row < first_row {
            return Err(general_err!("first row indexes are not ordered"));
        }
        builder.append_row_count(
            i64::try_from(next_row - first_row)
                .map_err(|_| general_err!("page row count exceeds i64"))?,
        );
    }
    Ok(builder.build())
}

fn decode_column_index_chunk(
    chunk: &ColumnIndexChunk<'_>,
    physical_type: Type,
) -> Result<crate::file::page_index::column_index::ColumnIndexMetaData> {
    let pages = usize::try_from(chunk.null_pages.num_values)
        .map_err(|_| general_err!("negative column-index page count"))?;
    let boundary_order = match chunk.boundary_order {
        0 => BoundaryOrder::UNORDERED,
        1 => BoundaryOrder::ASCENDING,
        2 => BoundaryOrder::DESCENDING,
        value => return Err(general_err!("invalid boundary order {value}")),
    };
    for (array, name) in [
        (chunk.null_counts.as_ref(), "null_counts"),
        (chunk.minmax_prefixes.as_ref(), "minmax_prefixes"),
        (chunk.min_suffixes.as_ref(), "min_suffixes"),
        (chunk.max_suffixes.as_ref(), "max_suffixes"),
        (chunk.nan_counts.as_ref(), "nan_counts"),
    ] {
        if let Some(array) = array {
            validate_array(array, pages, name)?;
        }
    }
    let mut builder = ColumnIndexBuilder::new(physical_type);
    builder.set_boundary_order(boundary_order);
    for page in 0..pages {
        let null_page = required_value(&chunk.null_pages, page)? != 0;
        let null_count = chunk
            .null_counts
            .as_ref()
            .map(|array| value_at(array, page))
            .transpose()?
            .flatten()
            .unwrap_or(0);
        let nan_count = chunk
            .nan_counts
            .as_ref()
            .map(|array| value_at(array, page))
            .transpose()?
            .flatten();
        let prefix = chunk
            .minmax_prefixes
            .as_ref()
            .map(|array| byte_array_at(array, page))
            .transpose()?
            .flatten();
        let min_suffix = chunk
            .min_suffixes
            .as_ref()
            .map(|array| byte_array_at(array, page))
            .transpose()?
            .flatten();
        let max_suffix = chunk
            .max_suffixes
            .as_ref()
            .map(|array| byte_array_at(array, page))
            .transpose()?
            .flatten();
        let (min, max) = match (prefix, min_suffix, max_suffix) {
            (Some(prefix), Some(min_suffix), Some(max_suffix)) => {
                let mut min = prefix.to_vec();
                min.extend_from_slice(min_suffix);
                let mut max = prefix.to_vec();
                max.extend_from_slice(max_suffix);
                (min, max)
            }
            (None, None, None) if null_page => (vec![], vec![]),
            (None, None, None) => {
                return Err(general_err!("non-null page has no min/max values"));
            }
            _ => return Err(general_err!("inconsistent modular page min/max presence")),
        };
        builder.append(
            null_page,
            min,
            max,
            i64::try_from(null_count).map_err(|_| general_err!("null count exceeds i64"))?,
            nan_count
                .map(i64::try_from)
                .transpose()
                .map_err(|_| general_err!("NaN count exceeds i64"))?,
        );
    }
    builder.build()
}

fn decode_row_group_statistics(
    statistics: &ColumnStatistics,
    row_groups: usize,
) -> Result<Vec<ModularRowGroupStatistics>> {
    for (array, name) in [
        (statistics.null_counts.as_ref(), "null_counts"),
        (statistics.min_is_exact.as_ref(), "min_is_exact"),
        (statistics.max_is_exact.as_ref(), "max_is_exact"),
        (statistics.nan_counts.as_ref(), "nan_counts"),
    ] {
        if let Some(array) = array {
            validate_array(array, row_groups, name)?;
        }
    }
    let (Some(prefixes), Some(min_suffixes), Some(max_suffixes)) = (
        statistics.minmax_prefixes.as_ref(),
        statistics.min_suffixes.as_ref(),
        statistics.max_suffixes.as_ref(),
    ) else {
        return (0..row_groups)
            .map(|row_group_index| {
                Ok(ModularRowGroupStatistics {
                    row_group_index,
                    min: None,
                    max: None,
                    null_count: statistics
                        .null_counts
                        .as_ref()
                        .map(|array| value_at(array, row_group_index))
                        .transpose()?
                        .flatten(),
                    nan_count: statistics
                        .nan_counts
                        .as_ref()
                        .map(|array| value_at(array, row_group_index))
                        .transpose()?
                        .flatten(),
                    min_is_exact: false,
                    max_is_exact: false,
                })
            })
            .collect();
    };
    for (array, name) in [
        (prefixes, "minmax_prefixes"),
        (min_suffixes, "min_suffixes"),
        (max_suffixes, "max_suffixes"),
    ] {
        validate_array(array, row_groups, name)?;
    }
    (0..row_groups)
        .map(|row_group_index| {
            let prefix = byte_array_at(prefixes, row_group_index)?;
            let min_suffix = byte_array_at(min_suffixes, row_group_index)?;
            let max_suffix = byte_array_at(max_suffixes, row_group_index)?;
            let (min, max) = match (prefix, min_suffix, max_suffix) {
                (Some(prefix), Some(min_suffix), Some(max_suffix)) => {
                    let mut min = Vec::with_capacity(prefix.len() + min_suffix.len());
                    min.extend_from_slice(prefix);
                    min.extend_from_slice(min_suffix);
                    let mut max = Vec::with_capacity(prefix.len() + max_suffix.len());
                    max.extend_from_slice(prefix);
                    max.extend_from_slice(max_suffix);
                    (Some(min.into()), Some(max.into()))
                }
                (None, None, None) => (None, None),
                _ => return Err(general_err!("inconsistent modular min/max presence")),
            };
            let min_is_exact = match statistics.min_is_exact.as_ref() {
                Some(array) => value_at(array, row_group_index)?.unwrap_or(0) != 0,
                None => true,
            } && min.is_some();
            let max_is_exact = match statistics.max_is_exact.as_ref() {
                Some(array) => value_at(array, row_group_index)?.unwrap_or(0) != 0,
                None => true,
            } && max.is_some();
            Ok(ModularRowGroupStatistics {
                row_group_index,
                min,
                max,
                null_count: statistics
                    .null_counts
                    .as_ref()
                    .map(|array| value_at(array, row_group_index))
                    .transpose()?
                    .flatten(),
                nan_count: statistics
                    .nan_counts
                    .as_ref()
                    .map(|array| value_at(array, row_group_index))
                    .transpose()?
                    .flatten(),
                min_is_exact,
                max_is_exact,
            })
        })
        .collect()
}

fn byte_array_at<'a>(array: &'a ArrayPage<'a>, index: usize) -> Result<Option<&'a [u8]>> {
    let ArrayEncodingParameters::PresentIndex(params) = &array.parameters else {
        return Err(general_err!(
            "modular BYTE_ARRAY requires PRESENT_INDEX encoding"
        ));
    };
    if array.encoding != 1
        || params.num_present < 0
        || array.num_values < 0
        || index >= array.num_values as usize
    {
        return Err(general_err!("invalid modular BYTE_ARRAY encoding"));
    }
    let count = params.num_present as usize;
    let position_width = bit_width(params.position_bit_width)?;
    let offset_width = bit_width(params.value_bit_width)?;
    let positions_len = packed_len(count, position_width)?;
    let offsets_len = packed_len(count + 1, offset_width)?;
    if positions_len + offsets_len > array.data.len() {
        return Err(general_err!("truncated modular BYTE_ARRAY"));
    }
    let positions = &array.data[..positions_len];
    let mut low = 0;
    let mut high = count;
    let rank = loop {
        if low == high {
            return Ok(None);
        }
        let middle = low + (high - low) / 2;
        match (packed_at(positions, middle, position_width)? as usize).cmp(&index) {
            std::cmp::Ordering::Less => low = middle + 1,
            std::cmp::Ordering::Greater => high = middle,
            std::cmp::Ordering::Equal => break middle,
        }
    };
    let offsets = &array.data[positions_len..positions_len + offsets_len];
    let start = packed_at(offsets, rank, offset_width)? as usize;
    let end = packed_at(offsets, rank + 1, offset_width)? as usize;
    let values = &array.data[positions_len + offsets_len..];
    if start > end || end > values.len() {
        return Err(general_err!("invalid modular BYTE_ARRAY offsets"));
    }
    Ok(Some(&values[start..end]))
}

fn bit_width(width: i8) -> Result<usize> {
    if !(0..=64).contains(&width) {
        return Err(general_err!("invalid modular footer bit width {width}"));
    }
    Ok(width as usize)
}

fn packed_len(count: usize, width: usize) -> Result<usize> {
    count
        .checked_mul(width)
        .and_then(|bits| bits.checked_add(7))
        .map(|bits| bits / 8)
        .ok_or_else(|| general_err!("modular footer packed-array length overflow"))
}

fn packed_at(data: &[u8], index: usize, width: usize) -> Result<u64> {
    if width == 0 {
        return Ok(0);
    }
    let bit = index
        .checked_mul(width)
        .ok_or_else(|| general_err!("modular footer packed-array offset overflow"))?;
    let end_bit = bit
        .checked_add(width)
        .ok_or_else(|| general_err!("modular footer packed-array offset overflow"))?;
    if end_bit > data.len().saturating_mul(8) {
        return Err(general_err!("truncated modular footer packed array"));
    }
    let mut value = 0_u64;
    for output_bit in 0..width {
        let source_bit = bit + output_bit;
        value |= (((data[source_bit / 8] >> (source_bit % 8)) & 1) as u64) << output_bit;
    }
    Ok(value)
}

fn decode_thrift<'a, T>(bytes: &'a [u8], what: &str) -> Result<T>
where
    T: ReadThrift<'a, ThriftSliceInputProtocol<'a>>,
{
    let mut protocol = ThriftSliceInputProtocol::new(bytes);
    T::read_thrift(&mut protocol).map_err(|error| general_err!("failed to decode {what}: {error}"))
}

fn read_i64(bytes: &[u8], what: &str) -> Result<i64> {
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| general_err!("invalid {what}"))?;
    Ok(i64::from_le_bytes(bytes))
}

fn non_negative_u64(value: i64, what: &str) -> Result<u64> {
    value
        .try_into()
        .map_err(|_| general_err!("negative {what}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;
    use futures::future::BoxFuture;

    #[derive(Debug)]
    struct RecordingFetch {
        data: Bytes,
        ranges: Vec<Range<u64>>,
        range_batches: Vec<Vec<Range<u64>>>,
    }

    impl MetadataFetch for RecordingFetch {
        fn fetch(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes>> {
            self.ranges.push(range.clone());
            let bytes = self.data.slice(range.start as usize..range.end as usize);
            async move { Ok(bytes) }.boxed()
        }

        fn fetch_ranges(&mut self, ranges: Vec<Range<u64>>) -> BoxFuture<'_, Result<Vec<Bytes>>> {
            self.range_batches.push(ranges.clone());
            let bytes = ranges
                .iter()
                .map(|range| self.data.slice(range.start as usize..range.end as usize))
                .collect();
            async move { Ok(bytes) }.boxed()
        }
    }

    #[tokio::test]
    async fn multi_range_fetch_batches_remote_ranges_and_reuses_tail() {
        let data = Bytes::from_static(b"0123456789abcdefghij");
        let mut fetch = RecordingFetch {
            data: data.clone(),
            ranges: vec![],
            range_batches: vec![],
        };
        let bytes = get_ranges(&mut fetch, &data.slice(10..), 10, vec![1..3, 12..14, 5..7])
            .await
            .unwrap();
        assert_eq!(
            bytes,
            vec![Bytes::from("12"), Bytes::from("cd"), Bytes::from("56")]
        );
        assert!(fetch.ranges.is_empty());
        assert_eq!(fetch.range_batches, vec![vec![1..3, 5..7]]);
    }

    fn dense(values: &[u64]) -> ArrayPage<'static> {
        let width = values
            .iter()
            .copied()
            .max()
            .map(|x| 64 - x.leading_zeros() as i8)
            .unwrap_or(0);
        let mut data = vec![0; packed_len(values.len(), width as usize).unwrap()];
        for (index, value) in values.iter().enumerate() {
            for bit in 0..width as usize {
                data[(index * width as usize + bit) / 8] |=
                    (((value >> bit) & 1) as u8) << ((index * width as usize + bit) % 8);
            }
        }
        ArrayPage {
            data: Box::leak(data.into_boxed_slice()),
            encoding: 0,
            num_values: values.len() as i32,
            parameters: ArrayEncodingParameters::Bitset(BitsetParameters {
                value_bit_width: width,
                num_present: values.len() as i32,
            }),
        }
    }

    fn encode<T: WriteThrift>(value: &T) -> Vec<u8> {
        let mut output = Vec::new();
        value
            .write_thrift(&mut ThriftCompactOutputProtocol::new(&mut output))
            .unwrap();
        output
    }

    fn single_byte_array(value: &'static [u8]) -> ArrayPage<'static> {
        let width = (usize::BITS - value.len().leading_zeros()) as i8;
        let mut offsets = vec![0; packed_len(2, width as usize).unwrap()];
        for bit in 0..width as usize {
            offsets[(width as usize + bit) / 8] |=
                (((value.len() >> bit) & 1) as u8) << ((width as usize + bit) % 8);
        }
        offsets.extend_from_slice(value);
        ArrayPage {
            data: Box::leak(offsets.into_boxed_slice()),
            encoding: 1,
            num_values: 1,
            parameters: ArrayEncodingParameters::PresentIndex(PresentIndexParameters {
                num_present: 1,
                position_bit_width: 0,
                value_bit_width: width,
            }),
        }
    }

    #[test]
    fn decodes_modular_page_index_chunks() {
        let offset = OffsetIndexChunk {
            offsets: dense(&[100, 250]),
            compressed_page_sizes: dense(&[50, 60]),
            first_row_indexes: dense(&[0, 10]),
        };
        let offset = decode_offset_index_chunk(&offset).unwrap();
        assert_eq!(offset.page_locations[0].offset, 100);
        assert_eq!(offset.page_locations[1].first_row_index, 10);

        let min = Box::leak(Box::new(1_i32.to_le_bytes()));
        let max = Box::leak(Box::new(9_i32.to_le_bytes()));
        let column = ColumnIndexChunk {
            boundary_order: 1,
            null_pages: dense(&[0]),
            null_counts: Some(dense(&[0])),
            minmax_prefixes: Some(single_byte_array(&[])),
            min_suffixes: Some(single_byte_array(min)),
            max_suffixes: Some(single_byte_array(max)),
            min_is_exact: None,
            max_is_exact: None,
            nan_counts: None,
        };
        let column = decode_column_index_chunk(&column, Type::INT32).unwrap();
        let crate::file::page_index::column_index::ColumnIndexMetaData::INT32(column) = column
        else {
            panic!("expected INT32 column index")
        };
        assert_eq!(column.min_values, vec![1]);
        assert_eq!(column.max_values, vec![9]);
    }

    fn test_file(optional_prefix: usize) -> Bytes {
        // SchemaModule containing root schema { required INT32 c0, required INT64 c1 }.
        let schema = vec![
            0x19, 0x3c, // field 1, list<struct> of length 3
            0x48, 0x06, b's', b'c', b'h', b'e', b'm', b'a', 0x15, 0x04, 0x00, // root
            0x15, 0x02, 0x25, 0x00, 0x18, 0x02, b'c', b'0', 0x00, // INT32 c0
            0x15, 0x04, 0x25, 0x00, 0x18, 0x02, b'c', b'1', 0x00, // INT64 c1
            0x00,
        ];
        let placement = encode(&PlacementModule {
            data_page_offsets: dense(&[10, 20, 30, 40]),
            first_dictionary_pages: dense(&[0, 0, 0, 0, 0]),
            dictionary_page_offsets: dense(&[]),
            total_compressed_sizes: dense(&[1, 2, 3, 4]),
            total_uncompressed_sizes: dense(&[2, 4, 6, 8]),
            num_values: dense(&[5, 6, 7, 8]),
            codecs: dense(&[0, 1, 2, 3]),
            physical_types: dense(&[1, 2]),
            is_fully_dictionary_encoded: dense(&[0, 1, 0, 1]),
        });
        let file_metadata = encode(&FileMetadataModule {
            created_by: Some("modular-test-writer".to_string()),
            key_value_metadata: Some(vec![KeyValue::new(
                "custom".to_string(),
                "value".to_string(),
            )]),
        });

        let modular_start = 4_u64;
        let schema_start = modular_start + optional_prefix as u64;
        let placement_start = schema_start + schema.len() as u64;
        let file_metadata_start = placement_start + placement.len() as u64;
        let root_start = file_metadata_start + file_metadata.len() as u64;
        let root = encode(&ModularFooter {
            version: 2,
            num_row_groups: 2,
            num_columns: 2,
            num_rows: 12,
            row_group_num_rows: vec![5, 7],
            modules: vec![
                ModuleDirectoryEntry {
                    kind: SCHEMA_MODULE,
                    location: ModuleLocation {
                        offset: schema_start as i64,
                        length: schema.len() as i64,
                    },
                },
                ModuleDirectoryEntry {
                    kind: PLACEMENT_MODULE,
                    location: ModuleLocation {
                        offset: placement_start as i64,
                        length: placement.len() as i64,
                    },
                },
                ModuleDirectoryEntry {
                    kind: FILE_METADATA_MODULE,
                    location: ModuleLocation {
                        offset: file_metadata_start as i64,
                        length: file_metadata.len() as i64,
                    },
                },
            ],
        });

        let mut file = b"PAR1".to_vec();
        file.resize(file.len() + optional_prefix, 0xaa);
        file.extend_from_slice(&schema);
        file.extend_from_slice(&placement);
        file.extend_from_slice(&file_metadata);
        file.extend_from_slice(&root);
        file.extend_from_slice(&(modular_start as i64).to_le_bytes());
        file.extend_from_slice(&((root_start - modular_start) as i64).to_le_bytes());
        file.extend_from_slice(MAGIC);
        file.into()
    }

    #[tokio::test]
    async fn projected_placement_uses_one_tail_fetch() {
        let data = test_file(128 * 1024);
        let mut fetch = RecordingFetch {
            data: data.clone(),
            ranges: vec![],
            range_batches: vec![],
        };
        let metadata = ModularFooterReader::new(vec![1])
            .with_prefetch_size(4096)
            .load(&mut fetch, data.len() as u64)
            .await
            .unwrap();

        assert_eq!(fetch.ranges.len(), 1);
        assert_eq!(metadata.num_columns, 2);
        assert_eq!(metadata.schema.num_columns(), 2);
        assert_eq!(metadata.column_chunks.len(), 2);
        assert_eq!(metadata.column_chunks[0].data_page_offset, 30);
        assert_eq!(metadata.column_chunks[1].data_page_offset, 40);
        assert_eq!(metadata.column_chunks[1].codec, 3);
        assert!(metadata.column_chunks[1].is_fully_dictionary_encoded);
        assert_eq!(metadata.created_by.as_deref(), Some("modular-test-writer"));
        assert_eq!(
            metadata.key_value_metadata.as_deref(),
            Some([KeyValue::new("custom".to_string(), "value".to_string())].as_slice())
        );

        let projected = metadata.projected_parquet_metadata(&[1]).unwrap();
        assert_eq!(
            projected.file_metadata().created_by(),
            Some("modular-test-writer")
        );
        assert_eq!(
            projected.file_metadata().key_value_metadata(),
            metadata.key_value_metadata.as_ref()
        );
        assert_eq!(projected.row_group(0).num_rows(), 5);
        assert_eq!(projected.row_group(1).num_rows(), 7);
        let chunk = projected.row_group(1).column(0);
        assert_eq!(chunk.column_type(), Type::INT64);
        assert_eq!(chunk.data_page_offset(), 40);
        assert_eq!(chunk.compressed_size(), 4);
        assert_eq!(chunk.num_values(), 8);
    }

    #[tokio::test]
    async fn file_metadata_can_be_skipped() {
        let data = test_file(128 * 1024);
        let mut fetch = RecordingFetch {
            data: data.clone(),
            ranges: vec![],
            range_batches: vec![],
        };
        let metadata = ModularFooterReader::new(vec![0])
            .with_file_metadata(false)
            .load(&mut fetch, data.len() as u64)
            .await
            .unwrap();

        assert_eq!(fetch.ranges.len(), 1);
        assert_eq!(metadata.created_by, None);
        assert_eq!(metadata.key_value_metadata, None);
        let projected = metadata.projected_parquet_metadata(&[0]).unwrap();
        assert_eq!(projected.file_metadata().created_by(), None);
        assert_eq!(projected.file_metadata().key_value_metadata(), None);
    }

    #[tokio::test]
    async fn fetches_only_missing_critical_ranges() {
        let data = test_file(128 * 1024);
        let mut fetch = RecordingFetch {
            data: data.clone(),
            ranges: vec![],
            range_batches: vec![],
        };
        let metadata = ModularFooterReader::new(vec![0])
            .with_prefetch_size(TRAILER_SIZE + 64)
            .load(&mut fetch, data.len() as u64)
            .await
            .unwrap();

        assert!(fetch.ranges.len() <= 4);
        assert!(fetch.ranges.iter().all(|range| range.start >= 128 * 1024));
        assert_eq!(metadata.column_chunks[0].data_page_offset, 10);
        assert_eq!(metadata.column_chunks[1].data_page_offset, 20);
    }

    #[cfg(feature = "test_common")]
    #[tokio::test]
    async fn fetches_statistics_only_for_filter_column() {
        let data = modular_footer_benchmark_file(10, 10, 128 * 1024);
        let mut fetch = RecordingFetch {
            data: data.clone(),
            ranges: vec![],
            range_batches: vec![],
        };
        let mut metadata = ModularFooterReader::new(vec![0, 1])
            .with_prefetch_size(64 * 1024)
            .load(&mut fetch, data.len() as u64)
            .await
            .unwrap();
        assert_eq!(fetch.ranges.len(), 1);

        let statistics = metadata
            .load_row_group_statistics(&mut fetch, 0)
            .await
            .unwrap();
        assert_eq!(fetch.ranges.len(), 2);
        assert_eq!(statistics.len(), 10);
        assert_eq!(
            statistics[5].min.as_deref(),
            Some(5000_f32.to_le_bytes().as_slice())
        );
        assert_eq!(
            statistics[5].max.as_deref(),
            Some(5999_f32.to_le_bytes().as_slice())
        );
        let projected = metadata.projected_parquet_metadata(&[0, 1]).unwrap();
        let standard = projected.row_group(5).column(0).statistics().unwrap();
        assert_eq!(
            standard.min_bytes_opt(),
            Some(5000_f32.to_le_bytes().as_slice())
        );
        assert_eq!(
            standard.max_bytes_opt(),
            Some(5999_f32.to_le_bytes().as_slice())
        );
        assert_eq!(standard.null_count_opt(), Some(3));
        assert_eq!(standard.nan_count_opt(), Some(2));
        assert!(standard.min_is_exact());
        assert!(standard.max_is_exact());
        assert!(projected.row_group(5).column(1).statistics().is_none());
    }

    #[test]
    fn projected_metadata_preserves_nested_schema_semantics() {
        use crate::arrow::{ProjectionMask, parquet_to_arrow_schema_by_columns};
        use crate::schema::parser::parse_message_type;

        let root = parse_message_type(
            "message schema {
                optional group s {
                    required int32 a;
                    optional binary b (STRING);
                }
                repeated int64 values;
                required int32 c;
            }",
        )
        .unwrap();
        let schema = Arc::new(SchemaDescriptor::new(Arc::new(root)));
        let chunks = [(1, 6), (2, 2), (3, 1)]
            .into_iter()
            .map(|(column_index, physical_type)| ModularColumnChunk {
                column_index,
                row_group_index: 0,
                physical_type,
                codec: 0,
                data_page_offset: 100 + column_index as u64 * 10,
                dictionary_page_offsets: vec![],
                total_compressed_size: 10,
                total_uncompressed_size: 10,
                num_values: 3,
                is_fully_dictionary_encoded: false,
            })
            .collect();
        let modular = ModularFooterMetadata {
            version: 2,
            num_rows: 3,
            num_columns: 4,
            row_group_num_rows: vec![3],
            schema: Arc::clone(&schema),
            created_by: None,
            key_value_metadata: None,
            column_chunks: chunks,
            statistics_directory: None,
            offset_index_directory: None,
            column_index_directory: None,
            tail: Bytes::new(),
            tail_start: 0,
            modular_start: 0,
            root_start: 0,
            relative_offsets: false,
            statistics: HashMap::new(),
        };

        let projected = modular.projected_parquet_metadata(&[1, 2, 3]).unwrap();
        let projected_schema = projected.file_metadata().schema_descr();
        assert_eq!(projected_schema.num_columns(), 3);
        assert_eq!(projected_schema.column(0).path().string(), "s.b");
        assert_eq!(projected_schema.column(1).path().string(), "values");
        assert_eq!(projected_schema.column(2).path().string(), "c");
        assert_eq!(projected_schema.column(0).max_def_level(), 2);
        assert_eq!(projected_schema.column(0).max_rep_level(), 0);
        assert_eq!(projected_schema.column(1).max_def_level(), 1);
        assert_eq!(projected_schema.column(1).max_rep_level(), 1);

        let expected = parquet_to_arrow_schema_by_columns(
            &schema,
            ProjectionMask::leaves(&schema, [1, 2, 3]),
            None,
        )
        .unwrap();
        let actual =
            parquet_to_arrow_schema_by_columns(projected_schema, ProjectionMask::all(), None)
                .unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn decodes_sparse_present_index_without_materializing_array() {
        let page = ArrayPage {
            data: &[0b0000_1101, 42, 99],
            encoding: 1,
            num_values: 4,
            parameters: ArrayEncodingParameters::PresentIndex(PresentIndexParameters {
                num_present: 2,
                position_bit_width: 2,
                value_bit_width: 8,
            }),
        };
        assert_eq!(value_at(&page, 1).unwrap(), Some(42));
        assert_eq!(value_at(&page, 2).unwrap(), None);
        assert_eq!(value_at(&page, 3).unwrap(), Some(99));
    }
}
