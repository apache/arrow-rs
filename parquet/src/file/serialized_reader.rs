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

//! Contains implementations of the reader traits FileReader, RowGroupReader and PageReader
//! Also contains implementations of the ChunkReader for files (with buffering) and byte arrays (RAM)

use crate::basic::{PageType, Type};
use crate::bloom_filter::Sbbf;
use crate::column::page::{Page, PageMetadata, PageReader};
use crate::compression::{Codec, create_codec};
#[cfg(feature = "encryption")]
use crate::encryption::decrypt::{CryptoContext, read_and_decrypt};
use crate::errors::{ParquetError, Result};
use crate::file::metadata::thrift::PageHeader;
use crate::file::page_index::offset_index::{OffsetIndexMetaData, PageLocation};
use crate::file::statistics;
use crate::file::{
    metadata::*,
    properties::{ReaderProperties, ReaderPropertiesPtr},
    reader::*,
};
#[cfg(feature = "encryption")]
use crate::parquet_thrift::ThriftSliceInputProtocol;
use crate::parquet_thrift::{ReadThrift, ThriftReadInputProtocol};
use crate::record::Row;
use crate::record::reader::RowIter;
use crate::schema::types::{SchemaDescPtr, Type as SchemaType};
use bytes::Bytes;
use std::collections::VecDeque;
use std::{fs::File, io::Read, path::Path, sync::Arc};

impl TryFrom<File> for SerializedFileReader<File> {
    type Error = ParquetError;

    fn try_from(file: File) -> Result<Self> {
        Self::new(file)
    }
}

impl TryFrom<&Path> for SerializedFileReader<File> {
    type Error = ParquetError;

    fn try_from(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        Self::try_from(file)
    }
}

impl TryFrom<String> for SerializedFileReader<File> {
    type Error = ParquetError;

    fn try_from(path: String) -> Result<Self> {
        Self::try_from(Path::new(&path))
    }
}

impl TryFrom<&str> for SerializedFileReader<File> {
    type Error = ParquetError;

    fn try_from(path: &str) -> Result<Self> {
        Self::try_from(Path::new(&path))
    }
}

/// Conversion into a [`RowIter`]
/// using the full file schema over all row groups.
impl IntoIterator for SerializedFileReader<File> {
    type Item = Result<Row>;
    type IntoIter = RowIter<'static>;

    fn into_iter(self) -> Self::IntoIter {
        RowIter::from_file_into(Box::new(self))
    }
}

// ----------------------------------------------------------------------
// Implementations of file & row group readers

/// A serialized implementation for Parquet [`FileReader`].
pub struct SerializedFileReader<R: ChunkReader> {
    chunk_reader: Arc<R>,
    metadata: Arc<ParquetMetaData>,
    props: ReaderPropertiesPtr,
}

/// A predicate for filtering row groups, invoked with the metadata and index
/// of each row group in the file. Only row groups for which the predicate
/// evaluates to `true` will be scanned
pub type ReadGroupPredicate = Box<dyn FnMut(&RowGroupMetaData, usize) -> bool>;

/// A builder for [`ReadOptions`].
/// For the predicates that are added to the builder,
/// they will be chained using 'AND' to filter the row groups.
#[derive(Default)]
pub struct ReadOptionsBuilder {
    predicates: Vec<ReadGroupPredicate>,
    enable_page_index: bool,
    props: Option<ReaderProperties>,
    metadata_options: ParquetMetaDataOptions,
}

impl ReadOptionsBuilder {
    /// New builder
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a predicate on row group metadata to the reading option,
    /// Filter only row groups that match the predicate criteria
    pub fn with_predicate(mut self, predicate: ReadGroupPredicate) -> Self {
        self.predicates.push(predicate);
        self
    }

    /// Add a range predicate on filtering row groups if their midpoints are within
    /// the Closed-Open range `[start..end) {x | start <= x < end}`
    pub fn with_range(mut self, start: i64, end: i64) -> Self {
        assert!(start < end);
        let predicate = move |rg: &RowGroupMetaData, _: usize| {
            let mid = get_midpoint_offset(rg);
            mid >= start && mid < end
        };
        self.predicates.push(Box::new(predicate));
        self
    }

    /// Enable reading the page index structures described in
    /// "[Column Index] Layout to Support Page Skipping"
    ///
    /// [Column Index]: https://github.com/apache/parquet-format/blob/master/PageIndex.md
    pub fn with_page_index(mut self) -> Self {
        self.enable_page_index = true;
        self
    }

    /// Set the [`ReaderProperties`] configuration.
    pub fn with_reader_properties(mut self, properties: ReaderProperties) -> Self {
        self.props = Some(properties);
        self
    }

    /// Provide a Parquet schema to use when decoding the metadata. The schema in the Parquet
    /// footer will be skipped.
    pub fn with_parquet_schema(mut self, schema: SchemaDescPtr) -> Self {
        self.metadata_options.set_schema(schema);
        self
    }

    /// Set whether to convert the [`encoding_stats`] in the Parquet `ColumnMetaData` to a bitmask
    /// (defaults to `false`).
    ///
    /// See [`ColumnChunkMetaData::page_encoding_stats_mask`] for an explanation of why this
    /// might be desirable.
    ///
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

    /// Seal the builder and return the read options
    pub fn build(self) -> ReadOptions {
        let props = self
            .props
            .unwrap_or_else(|| ReaderProperties::builder().build());
        ReadOptions {
            predicates: self.predicates,
            enable_page_index: self.enable_page_index,
            props,
            metadata_options: self.metadata_options,
        }
    }
}

/// A collection of options for reading a Parquet file.
///
/// Predicates are currently only supported on row group metadata.
/// All predicates will be chained using 'AND' to filter the row groups.
pub struct ReadOptions {
    predicates: Vec<ReadGroupPredicate>,
    enable_page_index: bool,
    props: ReaderProperties,
    metadata_options: ParquetMetaDataOptions,
}

impl<R: 'static + ChunkReader> SerializedFileReader<R> {
    /// Creates file reader from a Parquet file.
    /// Returns an error if the Parquet file does not exist or is corrupt.
    pub fn new(chunk_reader: R) -> Result<Self> {
        let metadata = ParquetMetaDataReader::new().parse_and_finish(&chunk_reader)?;
        let props = Arc::new(ReaderProperties::builder().build());
        Ok(Self {
            chunk_reader: Arc::new(chunk_reader),
            metadata: Arc::new(metadata),
            props,
        })
    }

    /// Creates file reader from a Parquet file with read options.
    /// Returns an error if the Parquet file does not exist or is corrupt.
    #[allow(deprecated)]
    pub fn new_with_options(chunk_reader: R, options: ReadOptions) -> Result<Self> {
        let mut metadata_builder = ParquetMetaDataReader::new()
            .with_metadata_options(Some(options.metadata_options.clone()))
            .parse_and_finish(&chunk_reader)?
            .into_builder();
        let mut predicates = options.predicates;

        // Filter row groups based on the predicates
        for (i, rg_meta) in metadata_builder.take_row_groups().into_iter().enumerate() {
            let mut keep = true;
            for predicate in &mut predicates {
                if !predicate(&rg_meta, i) {
                    keep = false;
                    break;
                }
            }
            if keep {
                metadata_builder = metadata_builder.add_row_group(rg_meta);
            }
        }

        let mut metadata = metadata_builder.build();

        // If page indexes are desired, build them with the filtered set of row groups
        if options.enable_page_index {
            let mut reader =
                ParquetMetaDataReader::new_with_metadata(metadata).with_page_indexes(true);
            reader.read_page_indexes(&chunk_reader)?;
            metadata = reader.finish()?;
        }

        Ok(Self {
            chunk_reader: Arc::new(chunk_reader),
            metadata: Arc::new(metadata),
            props: Arc::new(options.props),
        })
    }
}

/// Get midpoint offset for a row group
fn get_midpoint_offset(meta: &RowGroupMetaData) -> i64 {
    let col = meta.column(0);
    let mut offset = col.data_page_offset();
    if let Some(dic_offset) = col.dictionary_page_offset() {
        if offset > dic_offset {
            offset = dic_offset
        }
    };
    offset + meta.compressed_size() / 2
}

impl<R: 'static + ChunkReader> FileReader for SerializedFileReader<R> {
    fn metadata(&self) -> &ParquetMetaData {
        &self.metadata
    }

    fn num_row_groups(&self) -> usize {
        self.metadata.num_row_groups()
    }

    fn get_row_group(&self, i: usize) -> Result<Box<dyn RowGroupReader + '_>> {
        let row_group_metadata = self.metadata.row_group(i);
        // Row groups should be processed sequentially.
        let props = Arc::clone(&self.props);
        let f = Arc::clone(&self.chunk_reader);
        Ok(Box::new(SerializedRowGroupReader::new(
            f,
            row_group_metadata,
            self.metadata.offset_index().map(|x| x[i].as_slice()),
            props,
        )?))
    }

    fn get_row_iter(&self, projection: Option<SchemaType>) -> Result<RowIter<'_>> {
        RowIter::from_file(projection, self)
    }
}

/// A serialized implementation for Parquet [`RowGroupReader`].
pub struct SerializedRowGroupReader<'a, R: ChunkReader> {
    chunk_reader: Arc<R>,
    metadata: &'a RowGroupMetaData,
    offset_index: Option<&'a [OffsetIndexMetaData]>,
    props: ReaderPropertiesPtr,
    bloom_filters: Vec<Option<Sbbf>>,
}

impl<'a, R: ChunkReader> SerializedRowGroupReader<'a, R> {
    /// Creates new row group reader from a file, row group metadata and custom config.
    pub fn new(
        chunk_reader: Arc<R>,
        metadata: &'a RowGroupMetaData,
        offset_index: Option<&'a [OffsetIndexMetaData]>,
        props: ReaderPropertiesPtr,
    ) -> Result<Self> {
        let bloom_filters = if props.read_bloom_filter() {
            metadata
                .columns()
                .iter()
                .map(|col| Sbbf::read_from_column_chunk(col, &*chunk_reader))
                .collect::<Result<Vec<_>>>()?
        } else {
            std::iter::repeat_n(None, metadata.columns().len()).collect()
        };
        Ok(Self {
            chunk_reader,
            metadata,
            offset_index,
            props,
            bloom_filters,
        })
    }
}

impl<R: 'static + ChunkReader> RowGroupReader for SerializedRowGroupReader<'_, R> {
    fn metadata(&self) -> &RowGroupMetaData {
        self.metadata
    }

    fn num_columns(&self) -> usize {
        self.metadata.num_columns()
    }

    // TODO: fix PARQUET-816
    fn get_column_page_reader(&self, i: usize) -> Result<Box<dyn PageReader>> {
        let col = self.metadata.column(i);

        let page_locations = self.offset_index.map(|x| x[i].page_locations.clone());

        let props = Arc::clone(&self.props);
        Ok(Box::new(SerializedPageReader::new_with_properties(
            Arc::clone(&self.chunk_reader),
            col,
            usize::try_from(self.metadata.num_rows())?,
            page_locations,
            props,
        )?))
    }

    /// get bloom filter for the `i`th column
    fn get_column_bloom_filter(&self, i: usize) -> Option<&Sbbf> {
        self.bloom_filters[i].as_ref()
    }

    fn get_row_iter(&self, projection: Option<SchemaType>) -> Result<RowIter<'_>> {
        RowIter::from_row_group(projection, self)
    }
}

/// Decodes a [`Page`] from the provided `buffer`
pub(crate) fn decode_page(
    page_header: PageHeader,
    buffer: Bytes,
    physical_type: Type,
    decompressor: Option<&mut Box<dyn Codec>>,
) -> Result<Page> {
    // Verify the 32-bit CRC checksum of the page
    #[cfg(feature = "crc")]
    if let Some(expected_crc) = page_header.crc {
        let crc = crc32fast::hash(&buffer);
        if crc != expected_crc as u32 {
            return Err(general_err!("Page CRC checksum mismatch"));
        }
    }

    // When processing data page v2, depending on enabled compression for the
    // page, we should account for uncompressed data ('offset') of
    // repetition and definition levels.
    //
    // We always use 0 offset for other pages other than v2, `true` flag means
    // that compression will be applied if decompressor is defined
    let mut offset: usize = 0;
    let mut can_decompress = true;

    if let Some(ref header_v2) = page_header.data_page_header_v2 {
        if header_v2.definition_levels_byte_length < 0
            || header_v2.repetition_levels_byte_length < 0
            || header_v2.definition_levels_byte_length + header_v2.repetition_levels_byte_length
                > page_header.uncompressed_page_size
        {
            return Err(general_err!(
                "DataPage v2 header contains implausible values \
                    for definition_levels_byte_length ({}) \
                    and repetition_levels_byte_length ({}) \
                    given DataPage header provides uncompressed_page_size ({})",
                header_v2.definition_levels_byte_length,
                header_v2.repetition_levels_byte_length,
                page_header.uncompressed_page_size
            ));
        }
        offset = usize::try_from(
            header_v2.definition_levels_byte_length + header_v2.repetition_levels_byte_length,
        )?;
        // When is_compressed flag is missing the page is considered compressed
        can_decompress = header_v2.is_compressed.unwrap_or(true);
    }

    let buffer = match decompressor {
        Some(decompressor) if can_decompress => {
            let uncompressed_page_size = usize::try_from(page_header.uncompressed_page_size)?;
            if offset > buffer.len() || offset > uncompressed_page_size {
                return Err(general_err!("Invalid page header"));
            }
            let decompressed_size = uncompressed_page_size - offset;
            let mut decompressed = Vec::with_capacity(uncompressed_page_size);
            decompressed.extend_from_slice(&buffer[..offset]);
            // decompressed size of zero corresponds to a page with no non-null values
            // see https://github.com/apache/parquet-format/blob/master/README.md#data-pages
            if decompressed_size > 0 {
                let compressed = &buffer[offset..];
                decompressor.decompress(compressed, &mut decompressed, Some(decompressed_size))?;
            }

            if decompressed.len() != uncompressed_page_size {
                return Err(general_err!(
                    "Actual decompressed size doesn't match the expected one ({} vs {})",
                    decompressed.len(),
                    uncompressed_page_size
                ));
            }

            Bytes::from(decompressed)
        }
        _ => buffer,
    };

    let result = match page_header.r#type {
        PageType::DICTIONARY_PAGE => {
            let dict_header = page_header.dictionary_page_header.as_ref().ok_or_else(|| {
                ParquetError::General("Missing dictionary page header".to_string())
            })?;
            let is_sorted = dict_header.is_sorted.unwrap_or(false);
            Page::DictionaryPage {
                buf: buffer,
                num_values: dict_header.num_values.try_into()?,
                encoding: dict_header.encoding,
                is_sorted,
            }
        }
        PageType::DATA_PAGE => {
            let header = page_header
                .data_page_header
                .ok_or_else(|| ParquetError::General("Missing V1 data page header".to_string()))?;
            Page::DataPage {
                buf: buffer,
                num_values: header.num_values.try_into()?,
                encoding: header.encoding,
                def_level_encoding: header.definition_level_encoding,
                rep_level_encoding: header.repetition_level_encoding,
                statistics: statistics::from_thrift_page_stats(physical_type, header.statistics)?,
            }
        }
        PageType::DATA_PAGE_V2 => {
            let header = page_header
                .data_page_header_v2
                .ok_or_else(|| ParquetError::General("Missing V2 data page header".to_string()))?;
            let is_compressed = header.is_compressed.unwrap_or(true);
            Page::DataPageV2 {
                buf: buffer,
                num_values: header.num_values.try_into()?,
                encoding: header.encoding,
                num_nulls: header.num_nulls.try_into()?,
                num_rows: header.num_rows.try_into()?,
                def_levels_byte_len: header.definition_levels_byte_length.try_into()?,
                rep_levels_byte_len: header.repetition_levels_byte_length.try_into()?,
                is_compressed,
                statistics: statistics::from_thrift_page_stats(physical_type, header.statistics)?,
            }
        }
        _ => {
            // For unknown page type (e.g., INDEX_PAGE), skip and read next.
            return Err(general_err!(
                "Page type {:?} is not supported",
                page_header.r#type
            ));
        }
    };

    Ok(result)
}

enum SerializedPageReaderState {
    Values {
        /// The current byte offset in the reader
        /// Note that offset is u64 (i.e., not usize) to support 32-bit architectures such as WASM
        offset: u64,

        /// The length of the chunk in bytes
        /// Note that remaining_bytes is u64 (i.e., not usize) to support 32-bit architectures such as WASM
        remaining_bytes: u64,

        // If the next page header has already been "peeked", we will cache it and it`s length here
        next_page_header: Option<Box<PageHeader>>,

        /// The index of the data page within this column chunk
        page_index: usize,

        /// Whether the next page is expected to be a dictionary page
        require_dictionary: bool,
    },
    Pages {
        /// Remaining page locations
        page_locations: VecDeque<PageLocation>,
        /// Remaining dictionary location if any
        dictionary_page: Option<PageLocation>,
        /// The total number of rows in this column chunk
        total_rows: usize,
        /// The index of the data page within this column chunk
        page_index: usize,
    },
}

#[derive(Default)]
struct SerializedPageReaderContext {
    /// Controls decoding of page-level statistics
    read_stats: bool,
    /// Crypto context carrying objects required for decryption
    #[cfg(feature = "encryption")]
    crypto_context: Option<Arc<CryptoContext>>,
}

/// A serialized implementation for Parquet [`PageReader`].
pub struct SerializedPageReader<R: ChunkReader> {
    /// The chunk reader
    reader: Arc<R>,

    /// The compression codec for this column chunk. Only set for non-PLAIN codec.
    decompressor: Option<Box<dyn Codec>>,

    /// Column chunk type.
    physical_type: Type,

    state: SerializedPageReaderState,

    context: SerializedPageReaderContext,
}

impl<R: ChunkReader> SerializedPageReader<R> {
    /// Creates a new serialized page reader from a chunk reader and metadata
    pub fn new(
        reader: Arc<R>,
        column_chunk_metadata: &ColumnChunkMetaData,
        total_rows: usize,
        page_locations: Option<Vec<PageLocation>>,
    ) -> Result<Self> {
        let props = Arc::new(ReaderProperties::builder().build());
        SerializedPageReader::new_with_properties(
            reader,
            column_chunk_metadata,
            total_rows,
            page_locations,
            props,
        )
    }

    /// Stub No-op implementation when encryption is disabled.
    #[cfg(all(feature = "arrow", not(feature = "encryption")))]
    pub(crate) fn add_crypto_context(
        self,
        _rg_idx: usize,
        _column_idx: usize,
        _parquet_meta_data: &ParquetMetaData,
        _column_chunk_metadata: &ColumnChunkMetaData,
    ) -> Result<SerializedPageReader<R>> {
        Ok(self)
    }

    /// Adds any necessary crypto context to this page reader, if encryption is enabled.
    #[cfg(feature = "encryption")]
    pub(crate) fn add_crypto_context(
        mut self,
        rg_idx: usize,
        column_idx: usize,
        parquet_meta_data: &ParquetMetaData,
        column_chunk_metadata: &ColumnChunkMetaData,
    ) -> Result<SerializedPageReader<R>> {
        let Some(file_decryptor) = parquet_meta_data.file_decryptor() else {
            return Ok(self);
        };
        let Some(crypto_metadata) = column_chunk_metadata.crypto_metadata() else {
            return Ok(self);
        };
        let crypto_context =
            CryptoContext::for_column(file_decryptor, crypto_metadata, rg_idx, column_idx)?;
        self.context.crypto_context = Some(Arc::new(crypto_context));
        Ok(self)
    }

    /// Creates a new serialized page with custom options.
    pub fn new_with_properties(
        reader: Arc<R>,
        meta: &ColumnChunkMetaData,
        total_rows: usize,
        page_locations: Option<Vec<PageLocation>>,
        props: ReaderPropertiesPtr,
    ) -> Result<Self> {
        let decompressor = create_codec(meta.compression(), props.codec_options())?;
        let (start, len) = meta.byte_range();

        let state = match page_locations {
            Some(locations) => {
                // If the offset of the first page doesn't match the start of the column chunk
                // then the preceding space must contain a dictionary page.
                let dictionary_page = match locations.first() {
                    Some(dict_offset) if dict_offset.offset as u64 != start => Some(PageLocation {
                        offset: start as i64,
                        compressed_page_size: (dict_offset.offset as u64 - start) as i32,
                        first_row_index: 0,
                    }),
                    _ => None,
                };

                SerializedPageReaderState::Pages {
                    page_locations: locations.into(),
                    dictionary_page,
                    total_rows,
                    page_index: 0,
                }
            }
            None => SerializedPageReaderState::Values {
                offset: start,
                remaining_bytes: len,
                next_page_header: None,
                page_index: 0,
                require_dictionary: meta.dictionary_page_offset().is_some(),
            },
        };
        let mut context = SerializedPageReaderContext::default();
        if props.read_page_stats() {
            context.read_stats = true;
        }
        Ok(Self {
            reader,
            decompressor,
            state,
            physical_type: meta.column_type(),
            context,
        })
    }

    /// Similar to `peek_next_page`, but returns the offset of the next page instead of the page metadata.
    /// Unlike page metadata, an offset can uniquely identify a page.
    ///
    /// This is used when we need to read parquet with row-filter, and we don't want to decompress the page twice.
    /// This function allows us to check if the next page is being cached or read previously.
    #[cfg(test)]
    fn peek_next_page_offset(&mut self) -> Result<Option<u64>> {
        match &mut self.state {
            SerializedPageReaderState::Values {
                offset,
                remaining_bytes,
                next_page_header,
                page_index,
                require_dictionary,
            } => {
                loop {
                    if *remaining_bytes == 0 {
                        return Ok(None);
                    }
                    return if let Some(header) = next_page_header.as_ref() {
                        if let Ok(_page_meta) = PageMetadata::try_from(&**header) {
                            Ok(Some(*offset))
                        } else {
                            // For unknown page type (e.g., INDEX_PAGE), skip and read next.
                            *next_page_header = None;
                            continue;
                        }
                    } else {
                        let mut read = self.reader.get_read(*offset)?;
                        let (header_len, header) = Self::read_page_header_len(
                            &self.context,
                            &mut read,
                            *page_index,
                            *require_dictionary,
                        )?;
                        *offset += header_len as u64;
                        *remaining_bytes -= header_len as u64;
                        let page_meta = if let Ok(_page_meta) = PageMetadata::try_from(&header) {
                            Ok(Some(*offset))
                        } else {
                            // For unknown page type (e.g., INDEX_PAGE), skip and read next.
                            continue;
                        };
                        *next_page_header = Some(Box::new(header));
                        page_meta
                    };
                }
            }
            SerializedPageReaderState::Pages {
                page_locations,
                dictionary_page,
                ..
            } => {
                if let Some(page) = dictionary_page {
                    Ok(Some(page.offset as u64))
                } else if let Some(page) = page_locations.front() {
                    Ok(Some(page.offset as u64))
                } else {
                    Ok(None)
                }
            }
        }
    }

    fn read_page_header_len<T: Read>(
        context: &SerializedPageReaderContext,
        input: &mut T,
        page_index: usize,
        dictionary_page: bool,
    ) -> Result<(usize, PageHeader)> {
        /// A wrapper around a [`std::io::Read`] that keeps track of the bytes read
        struct TrackedRead<R> {
            inner: R,
            bytes_read: usize,
        }

        impl<R: Read> Read for TrackedRead<R> {
            fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                let v = self.inner.read(buf)?;
                self.bytes_read += v;
                Ok(v)
            }
        }

        let mut tracked = TrackedRead {
            inner: input,
            bytes_read: 0,
        };
        let header = context.read_page_header(&mut tracked, page_index, dictionary_page)?;
        Ok((tracked.bytes_read, header))
    }

    fn read_page_header_len_from_bytes(
        context: &SerializedPageReaderContext,
        buffer: &[u8],
        page_index: usize,
        dictionary_page: bool,
    ) -> Result<(usize, PageHeader)> {
        let mut input = std::io::Cursor::new(buffer);
        let header = context.read_page_header(&mut input, page_index, dictionary_page)?;
        let header_len = input.position() as usize;
        Ok((header_len, header))
    }
}

#[cfg(not(feature = "encryption"))]
impl SerializedPageReaderContext {
    fn read_page_header<T: Read>(
        &self,
        input: &mut T,
        _page_index: usize,
        _dictionary_page: bool,
    ) -> Result<PageHeader> {
        let mut prot = ThriftReadInputProtocol::new(input);
        if self.read_stats {
            Ok(PageHeader::read_thrift(&mut prot)?)
        } else {
            Ok(PageHeader::read_thrift_without_stats(&mut prot)?)
        }
    }

    fn decrypt_page_data<T>(
        &self,
        buffer: T,
        _page_index: usize,
        _dictionary_page: bool,
    ) -> Result<T> {
        Ok(buffer)
    }
}

#[cfg(feature = "encryption")]
impl SerializedPageReaderContext {
    fn read_page_header<T: Read>(
        &self,
        input: &mut T,
        page_index: usize,
        dictionary_page: bool,
    ) -> Result<PageHeader> {
        match self.page_crypto_context(page_index, dictionary_page) {
            None => {
                let mut prot = ThriftReadInputProtocol::new(input);
                if self.read_stats {
                    Ok(PageHeader::read_thrift(&mut prot)?)
                } else {
                    use crate::file::metadata::thrift::PageHeader;

                    Ok(PageHeader::read_thrift_without_stats(&mut prot)?)
                }
            }
            Some(page_crypto_context) => {
                let data_decryptor = page_crypto_context.data_decryptor();
                let aad = page_crypto_context.create_page_header_aad()?;

                let buf = read_and_decrypt(data_decryptor, input, aad.as_ref()).map_err(|_| {
                    ParquetError::General(format!(
                        "Error decrypting page header for column {}, decryption key may be wrong",
                        page_crypto_context.column_ordinal
                    ))
                })?;

                let mut prot = ThriftSliceInputProtocol::new(buf.as_slice());
                if self.read_stats {
                    Ok(PageHeader::read_thrift(&mut prot)?)
                } else {
                    Ok(PageHeader::read_thrift_without_stats(&mut prot)?)
                }
            }
        }
    }

    fn decrypt_page_data<T>(&self, buffer: T, page_index: usize, dictionary_page: bool) -> Result<T>
    where
        T: AsRef<[u8]>,
        T: From<Vec<u8>>,
    {
        let page_crypto_context = self.page_crypto_context(page_index, dictionary_page);
        if let Some(page_crypto_context) = page_crypto_context {
            let decryptor = page_crypto_context.data_decryptor();
            let aad = page_crypto_context.create_page_aad()?;
            let decrypted = decryptor.decrypt(buffer.as_ref(), &aad)?;
            Ok(T::from(decrypted))
        } else {
            Ok(buffer)
        }
    }

    fn page_crypto_context(
        &self,
        page_index: usize,
        dictionary_page: bool,
    ) -> Option<Arc<CryptoContext>> {
        self.crypto_context.as_ref().map(|c| {
            Arc::new(if dictionary_page {
                c.for_dictionary_page()
            } else {
                c.with_page_ordinal(page_index)
            })
        })
    }
}

impl<R: ChunkReader> Iterator for SerializedPageReader<R> {
    type Item = Result<Page>;

    fn next(&mut self) -> Option<Self::Item> {
        self.get_next_page().transpose()
    }
}

fn verify_page_header_len(header_len: usize, remaining_bytes: u64) -> Result<()> {
    if header_len as u64 > remaining_bytes {
        return Err(eof_err!("Invalid page header"));
    }
    Ok(())
}

fn verify_page_size(
    compressed_size: i32,
    uncompressed_size: i32,
    remaining_bytes: u64,
) -> Result<()> {
    // The page's compressed size should not exceed the remaining bytes that are
    // available to read. The page's uncompressed size is the expected size
    // after decompression, which can never be negative.
    if compressed_size < 0 || compressed_size as u64 > remaining_bytes || uncompressed_size < 0 {
        return Err(eof_err!("Invalid page header"));
    }
    Ok(())
}

impl<R: ChunkReader> PageReader for SerializedPageReader<R> {
    fn get_next_page(&mut self) -> Result<Option<Page>> {
        loop {
            let page = match &mut self.state {
                SerializedPageReaderState::Values {
                    offset,
                    remaining_bytes: remaining,
                    next_page_header,
                    page_index,
                    require_dictionary,
                } => {
                    if *remaining == 0 {
                        return Ok(None);
                    }

                    let mut read = self.reader.get_read(*offset)?;
                    let header = if let Some(header) = next_page_header.take() {
                        *header
                    } else {
                        let (header_len, header) = Self::read_page_header_len(
                            &self.context,
                            &mut read,
                            *page_index,
                            *require_dictionary,
                        )?;
                        verify_page_header_len(header_len, *remaining)?;
                        *offset += header_len as u64;
                        *remaining -= header_len as u64;
                        header
                    };
                    verify_page_size(
                        header.compressed_page_size,
                        header.uncompressed_page_size,
                        *remaining,
                    )?;
                    let data_len = header.compressed_page_size as usize;
                    let data_start = *offset;
                    *offset += data_len as u64;
                    *remaining -= data_len as u64;

                    if header.r#type == PageType::INDEX_PAGE {
                        continue;
                    }

                    let buffer = self.reader.get_bytes(data_start, data_len)?;

                    let buffer =
                        self.context
                            .decrypt_page_data(buffer, *page_index, *require_dictionary)?;

                    let page = decode_page(
                        header,
                        buffer,
                        self.physical_type,
                        self.decompressor.as_mut(),
                    )?;
                    if page.is_data_page() {
                        *page_index += 1;
                    } else if page.is_dictionary_page() {
                        *require_dictionary = false;
                    }
                    page
                }
                SerializedPageReaderState::Pages {
                    page_locations,
                    dictionary_page,
                    page_index,
                    ..
                } => {
                    let (front, is_dictionary_page) = match dictionary_page.take() {
                        Some(front) => (front, true),
                        None => match page_locations.pop_front() {
                            Some(front) => (front, false),
                            None => return Ok(None),
                        },
                    };

                    let page_len = usize::try_from(front.compressed_page_size)?;
                    let buffer = self.reader.get_bytes(front.offset as u64, page_len)?;

                    let (offset, header) = Self::read_page_header_len_from_bytes(
                        &self.context,
                        buffer.as_ref(),
                        *page_index,
                        is_dictionary_page,
                    )?;
                    let bytes = buffer.slice(offset..);
                    let bytes =
                        self.context
                            .decrypt_page_data(bytes, *page_index, is_dictionary_page)?;

                    if !is_dictionary_page {
                        *page_index += 1;
                    }
                    decode_page(
                        header,
                        bytes,
                        self.physical_type,
                        self.decompressor.as_mut(),
                    )?
                }
            };

            return Ok(Some(page));
        }
    }

    fn peek_next_page(&mut self) -> Result<Option<PageMetadata>> {
        match &mut self.state {
            SerializedPageReaderState::Values {
                offset,
                remaining_bytes,
                next_page_header,
                page_index,
                require_dictionary,
            } => {
                loop {
                    if *remaining_bytes == 0 {
                        return Ok(None);
                    }
                    return if let Some(header) = next_page_header.as_ref() {
                        if let Ok(page_meta) = (&**header).try_into() {
                            Ok(Some(page_meta))
                        } else {
                            // For unknown page type (e.g., INDEX_PAGE), skip and read next.
                            *next_page_header = None;
                            continue;
                        }
                    } else {
                        let mut read = self.reader.get_read(*offset)?;
                        let (header_len, header) = Self::read_page_header_len(
                            &self.context,
                            &mut read,
                            *page_index,
                            *require_dictionary,
                        )?;
                        verify_page_header_len(header_len, *remaining_bytes)?;
                        *offset += header_len as u64;
                        *remaining_bytes -= header_len as u64;
                        let page_meta = if let Ok(page_meta) = (&header).try_into() {
                            Ok(Some(page_meta))
                        } else {
                            // For unknown page type (e.g., INDEX_PAGE), skip and read next.
                            continue;
                        };
                        *next_page_header = Some(Box::new(header));
                        page_meta
                    };
                }
            }
            SerializedPageReaderState::Pages {
                page_locations,
                dictionary_page,
                total_rows,
                page_index: _,
            } => {
                if dictionary_page.is_some() {
                    Ok(Some(PageMetadata {
                        num_rows: None,
                        num_levels: None,
                        is_dict: true,
                    }))
                } else if let Some(page) = page_locations.front() {
                    let next_rows = page_locations
                        .get(1)
                        .map(|x| x.first_row_index as usize)
                        .unwrap_or(*total_rows);

                    Ok(Some(PageMetadata {
                        num_rows: Some(next_rows - page.first_row_index as usize),
                        num_levels: None,
                        is_dict: false,
                    }))
                } else {
                    Ok(None)
                }
            }
        }
    }

    fn skip_next_page(&mut self) -> Result<()> {
        match &mut self.state {
            SerializedPageReaderState::Values {
                offset,
                remaining_bytes,
                next_page_header,
                page_index,
                require_dictionary,
            } => {
                if let Some(buffered_header) = next_page_header.take() {
                    verify_page_size(
                        buffered_header.compressed_page_size,
                        buffered_header.uncompressed_page_size,
                        *remaining_bytes,
                    )?;
                    // The next page header has already been peeked, so just advance the offset
                    *offset += buffered_header.compressed_page_size as u64;
                    *remaining_bytes -= buffered_header.compressed_page_size as u64;
                } else {
                    let mut read = self.reader.get_read(*offset)?;
                    let (header_len, header) = Self::read_page_header_len(
                        &self.context,
                        &mut read,
                        *page_index,
                        *require_dictionary,
                    )?;
                    verify_page_header_len(header_len, *remaining_bytes)?;
                    verify_page_size(
                        header.compressed_page_size,
                        header.uncompressed_page_size,
                        *remaining_bytes,
                    )?;
                    let data_page_size = header.compressed_page_size as u64;
                    *offset += header_len as u64 + data_page_size;
                    *remaining_bytes -= header_len as u64 + data_page_size;
                }
                if *require_dictionary {
                    *require_dictionary = false;
                } else {
                    *page_index += 1;
                }
                Ok(())
            }
            SerializedPageReaderState::Pages {
                page_locations,
                dictionary_page,
                page_index,
                ..
            } => {
                if dictionary_page.is_some() {
                    // If a dictionary page exists, consume it by taking it (sets to None)
                    dictionary_page.take();
                } else {
                    // If no dictionary page exists, simply pop the data page from page_locations
                    if page_locations.pop_front().is_some() {
                        *page_index += 1;
                    }
                }

                Ok(())
            }
        }
    }

    fn at_record_boundary(&mut self) -> Result<bool> {
        match &mut self.state {
            SerializedPageReaderState::Values { .. } => Ok(self.peek_next_page()?.is_none()),
            SerializedPageReaderState::Pages { .. } => Ok(true),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use bytes::Buf;

    use crate::file::page_index::column_index::{
        ByteArrayColumnIndex, ColumnIndexMetaData, PrimitiveColumnIndex,
    };
    use crate::file::properties::{EnabledStatistics, WriterProperties};

    use crate::basic::{self, BoundaryOrder, ColumnOrder, Encoding, SortOrder};
    use crate::column::reader::ColumnReader;
    use crate::data_type::private::ParquetValueType;
    use crate::data_type::{AsBytes, FixedLenByteArrayType, Int32Type};
    use crate::file::metadata::thrift::DataPageHeaderV2;
    #[allow(deprecated)]
    use crate::file::page_index::index_reader::{read_columns_indexes, read_offset_indexes};
    use crate::file::writer::SerializedFileWriter;
    use crate::record::RowAccessor;
    use crate::schema::parser::parse_message_type;
    use crate::util::test_common::file_util::{get_test_file, get_test_path};

    use super::*;

    #[test]
    fn test_decode_page_invalid_offset() {
        let page_header = PageHeader {
            r#type: PageType::DATA_PAGE_V2,
            uncompressed_page_size: 10,
            compressed_page_size: 10,
            data_page_header: None,
            index_page_header: None,
            dictionary_page_header: None,
            crc: None,
            data_page_header_v2: Some(DataPageHeaderV2 {
                num_nulls: 0,
                num_rows: 0,
                num_values: 0,
                encoding: Encoding::PLAIN,
                definition_levels_byte_length: 11,
                repetition_levels_byte_length: 0,
                is_compressed: None,
                statistics: None,
            }),
        };

        let buffer = Bytes::new();
        let err = decode_page(page_header, buffer, Type::INT32, None).unwrap_err();
        assert!(
            err.to_string()
                .contains("DataPage v2 header contains implausible values")
        );
    }

    #[test]
    fn test_decode_unsupported_page() {
        let mut page_header = PageHeader {
            r#type: PageType::INDEX_PAGE,
            uncompressed_page_size: 10,
            compressed_page_size: 10,
            data_page_header: None,
            index_page_header: None,
            dictionary_page_header: None,
            crc: None,
            data_page_header_v2: None,
        };
        let buffer = Bytes::new();
        let err = decode_page(page_header.clone(), buffer.clone(), Type::INT32, None).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Parquet error: Page type INDEX_PAGE is not supported"
        );

        page_header.data_page_header_v2 = Some(DataPageHeaderV2 {
            num_nulls: 0,
            num_rows: 0,
            num_values: 0,
            encoding: Encoding::PLAIN,
            definition_levels_byte_length: 11,
            repetition_levels_byte_length: 0,
            is_compressed: None,
            statistics: None,
        });
        let err = decode_page(page_header, buffer, Type::INT32, None).unwrap_err();
        assert!(
            err.to_string()
                .contains("DataPage v2 header contains implausible values")
        );
    }

    #[test]
    fn test_cursor_and_file_has_the_same_behaviour() {
        let mut buf: Vec<u8> = Vec::new();
        get_test_file("alltypes_plain.parquet")
            .read_to_end(&mut buf)
            .unwrap();
        let cursor = Bytes::from(buf);
        let read_from_cursor = SerializedFileReader::new(cursor).unwrap();

        let test_file = get_test_file("alltypes_plain.parquet");
        let read_from_file = SerializedFileReader::new(test_file).unwrap();

        let file_iter = read_from_file.get_row_iter(None).unwrap();
        let cursor_iter = read_from_cursor.get_row_iter(None).unwrap();

        for (a, b) in file_iter.zip(cursor_iter) {
            assert_eq!(a.unwrap(), b.unwrap())
        }
    }

    #[test]
    fn test_file_reader_try_from() {
        // Valid file path
        let test_file = get_test_file("alltypes_plain.parquet");
        let test_path_buf = get_test_path("alltypes_plain.parquet");
        let test_path = test_path_buf.as_path();
        let test_path_str = test_path.to_str().unwrap();

        let reader = SerializedFileReader::try_from(test_file);
        assert!(reader.is_ok());

        let reader = SerializedFileReader::try_from(test_path);
        assert!(reader.is_ok());

        let reader = SerializedFileReader::try_from(test_path_str);
        assert!(reader.is_ok());

        let reader = SerializedFileReader::try_from(test_path_str.to_string());
        assert!(reader.is_ok());

        // Invalid file path
        let test_path = Path::new("invalid.parquet");
        let test_path_str = test_path.to_str().unwrap();

        let reader = SerializedFileReader::try_from(test_path);
        assert!(reader.is_err());

        let reader = SerializedFileReader::try_from(test_path_str);
        assert!(reader.is_err());

        let reader = SerializedFileReader::try_from(test_path_str.to_string());
        assert!(reader.is_err());
    }

    #[test]
    fn test_file_reader_into_iter() {
        let path = get_test_path("alltypes_plain.parquet");
        let reader = SerializedFileReader::try_from(path.as_path()).unwrap();
        let iter = reader.into_iter();
        let values: Vec<_> = iter.flat_map(|x| x.unwrap().get_int(0)).collect();

        assert_eq!(values, &[4, 5, 6, 7, 2, 3, 0, 1]);
    }

    #[test]
    fn test_file_reader_into_iter_project() {
        let path = get_test_path("alltypes_plain.parquet");
        let reader = SerializedFileReader::try_from(path.as_path()).unwrap();
        let schema = "message schema { OPTIONAL INT32 id; }";
        let proj = parse_message_type(schema).ok();
        let iter = reader.into_iter().project(proj).unwrap();
        let values: Vec<_> = iter.flat_map(|x| x.unwrap().get_int(0)).collect();

        assert_eq!(values, &[4, 5, 6, 7, 2, 3, 0, 1]);
    }

    #[test]
    fn test_reuse_file_chunk() {
        // This test covers the case of maintaining the correct start position in a file
        // stream for each column reader after initializing and moving to the next one
        // (without necessarily reading the entire column).
        let test_file = get_test_file("alltypes_plain.parquet");
        let reader = SerializedFileReader::new(test_file).unwrap();
        let row_group = reader.get_row_group(0).unwrap();

        let mut page_readers = Vec::new();
        for i in 0..row_group.num_columns() {
            page_readers.push(row_group.get_column_page_reader(i).unwrap());
        }

        // Now buffer each col reader, we do not expect any failures like:
        // General("underlying Thrift error: end of file")
        for mut page_reader in page_readers {
            assert!(page_reader.get_next_page().is_ok());
        }
    }

    #[test]
    fn test_file_reader() {
        let test_file = get_test_file("alltypes_plain.parquet");
        let reader_result = SerializedFileReader::new(test_file);
        assert!(reader_result.is_ok());
        let reader = reader_result.unwrap();

        // Test contents in Parquet metadata
        let metadata = reader.metadata();
        assert_eq!(metadata.num_row_groups(), 1);

        // Test contents in file metadata
        let file_metadata = metadata.file_metadata();
        assert!(file_metadata.created_by().is_some());
        assert_eq!(
            file_metadata.created_by().unwrap(),
            "impala version 1.3.0-INTERNAL (build 8a48ddb1eff84592b3fc06bc6f51ec120e1fffc9)"
        );
        assert!(file_metadata.key_value_metadata().is_none());
        assert_eq!(file_metadata.num_rows(), 8);
        assert_eq!(file_metadata.version(), 1);
        assert_eq!(file_metadata.column_orders(), None);

        // Test contents in row group metadata
        let row_group_metadata = metadata.row_group(0);
        assert_eq!(row_group_metadata.num_columns(), 11);
        assert_eq!(row_group_metadata.num_rows(), 8);
        assert_eq!(row_group_metadata.total_byte_size(), 671);
        // Check each column order
        for i in 0..row_group_metadata.num_columns() {
            assert_eq!(file_metadata.column_order(i), ColumnOrder::UNDEFINED);
        }

        // Test row group reader
        let row_group_reader_result = reader.get_row_group(0);
        assert!(row_group_reader_result.is_ok());
        let row_group_reader: Box<dyn RowGroupReader> = row_group_reader_result.unwrap();
        assert_eq!(
            row_group_reader.num_columns(),
            row_group_metadata.num_columns()
        );
        assert_eq!(
            row_group_reader.metadata().total_byte_size(),
            row_group_metadata.total_byte_size()
        );

        // Test page readers
        // TODO: test for every column
        let page_reader_0_result = row_group_reader.get_column_page_reader(0);
        assert!(page_reader_0_result.is_ok());
        let mut page_reader_0: Box<dyn PageReader> = page_reader_0_result.unwrap();
        let mut page_count = 0;
        while let Some(page) = page_reader_0.get_next_page().unwrap() {
            let is_expected_page = match page {
                Page::DictionaryPage {
                    buf,
                    num_values,
                    encoding,
                    is_sorted,
                } => {
                    assert_eq!(buf.len(), 32);
                    assert_eq!(num_values, 8);
                    assert_eq!(encoding, Encoding::PLAIN_DICTIONARY);
                    assert!(!is_sorted);
                    true
                }
                Page::DataPage {
                    buf,
                    num_values,
                    encoding,
                    def_level_encoding,
                    rep_level_encoding,
                    statistics,
                } => {
                    assert_eq!(buf.len(), 11);
                    assert_eq!(num_values, 8);
                    assert_eq!(encoding, Encoding::PLAIN_DICTIONARY);
                    assert_eq!(def_level_encoding, Encoding::RLE);
                    #[allow(deprecated)]
                    let expected_rep_level_encoding = Encoding::BIT_PACKED;
                    assert_eq!(rep_level_encoding, expected_rep_level_encoding);
                    assert!(statistics.is_none());
                    true
                }
                _ => false,
            };
            assert!(is_expected_page);
            page_count += 1;
        }
        assert_eq!(page_count, 2);
    }

    #[test]
    fn test_file_reader_datapage_v2() {
        let test_file = get_test_file("datapage_v2.snappy.parquet");
        let reader_result = SerializedFileReader::new(test_file);
        assert!(reader_result.is_ok());
        let reader = reader_result.unwrap();

        // Test contents in Parquet metadata
        let metadata = reader.metadata();
        assert_eq!(metadata.num_row_groups(), 1);

        // Test contents in file metadata
        let file_metadata = metadata.file_metadata();
        assert!(file_metadata.created_by().is_some());
        assert_eq!(
            file_metadata.created_by().unwrap(),
            "parquet-mr version 1.8.1 (build 4aba4dae7bb0d4edbcf7923ae1339f28fd3f7fcf)"
        );
        assert!(file_metadata.key_value_metadata().is_some());
        assert_eq!(
            file_metadata.key_value_metadata().to_owned().unwrap().len(),
            1
        );

        assert_eq!(file_metadata.num_rows(), 5);
        assert_eq!(file_metadata.version(), 1);
        assert_eq!(file_metadata.column_orders(), None);

        let row_group_metadata = metadata.row_group(0);

        // Check each column order
        for i in 0..row_group_metadata.num_columns() {
            assert_eq!(file_metadata.column_order(i), ColumnOrder::UNDEFINED);
        }

        // Test row group reader
        let row_group_reader_result = reader.get_row_group(0);
        assert!(row_group_reader_result.is_ok());
        let row_group_reader: Box<dyn RowGroupReader> = row_group_reader_result.unwrap();
        assert_eq!(
            row_group_reader.num_columns(),
            row_group_metadata.num_columns()
        );
        assert_eq!(
            row_group_reader.metadata().total_byte_size(),
            row_group_metadata.total_byte_size()
        );

        // Test page readers
        // TODO: test for every column
        let page_reader_0_result = row_group_reader.get_column_page_reader(0);
        assert!(page_reader_0_result.is_ok());
        let mut page_reader_0: Box<dyn PageReader> = page_reader_0_result.unwrap();
        let mut page_count = 0;
        while let Some(page) = page_reader_0.get_next_page().unwrap() {
            let is_expected_page = match page {
                Page::DictionaryPage {
                    buf,
                    num_values,
                    encoding,
                    is_sorted,
                } => {
                    assert_eq!(buf.len(), 7);
                    assert_eq!(num_values, 1);
                    assert_eq!(encoding, Encoding::PLAIN);
                    assert!(!is_sorted);
                    true
                }
                Page::DataPageV2 {
                    buf,
                    num_values,
                    encoding,
                    num_nulls,
                    num_rows,
                    def_levels_byte_len,
                    rep_levels_byte_len,
                    is_compressed,
                    statistics,
                } => {
                    assert_eq!(buf.len(), 4);
                    assert_eq!(num_values, 5);
                    assert_eq!(encoding, Encoding::RLE_DICTIONARY);
                    assert_eq!(num_nulls, 1);
                    assert_eq!(num_rows, 5);
                    assert_eq!(def_levels_byte_len, 2);
                    assert_eq!(rep_levels_byte_len, 0);
                    assert!(is_compressed);
                    assert!(statistics.is_none()); // page stats are no longer read
                    true
                }
                _ => false,
            };
            assert!(is_expected_page);
            page_count += 1;
        }
        assert_eq!(page_count, 2);
    }

    #[test]
    fn test_file_reader_empty_compressed_datapage_v2() {
        // this file has a compressed datapage that un-compresses to 0 bytes
        let test_file = get_test_file("page_v2_empty_compressed.parquet");
        let reader_result = SerializedFileReader::new(test_file);
        assert!(reader_result.is_ok());
        let reader = reader_result.unwrap();

        // Test contents in Parquet metadata
        let metadata = reader.metadata();
        assert_eq!(metadata.num_row_groups(), 1);

        // Test contents in file metadata
        let file_metadata = metadata.file_metadata();
        assert!(file_metadata.created_by().is_some());
        assert_eq!(
            file_metadata.created_by().unwrap(),
            "parquet-cpp-arrow version 14.0.2"
        );
        assert!(file_metadata.key_value_metadata().is_some());
        assert_eq!(
            file_metadata.key_value_metadata().to_owned().unwrap().len(),
            1
        );

        assert_eq!(file_metadata.num_rows(), 10);
        assert_eq!(file_metadata.version(), 2);
        let expected_order = ColumnOrder::TYPE_DEFINED_ORDER(SortOrder::SIGNED);
        assert_eq!(
            file_metadata.column_orders(),
            Some(vec![expected_order].as_ref())
        );

        let row_group_metadata = metadata.row_group(0);

        // Check each column order
        for i in 0..row_group_metadata.num_columns() {
            assert_eq!(file_metadata.column_order(i), expected_order);
        }

        // Test row group reader
        let row_group_reader_result = reader.get_row_group(0);
        assert!(row_group_reader_result.is_ok());
        let row_group_reader: Box<dyn RowGroupReader> = row_group_reader_result.unwrap();
        assert_eq!(
            row_group_reader.num_columns(),
            row_group_metadata.num_columns()
        );
        assert_eq!(
            row_group_reader.metadata().total_byte_size(),
            row_group_metadata.total_byte_size()
        );

        // Test page readers
        let page_reader_0_result = row_group_reader.get_column_page_reader(0);
        assert!(page_reader_0_result.is_ok());
        let mut page_reader_0: Box<dyn PageReader> = page_reader_0_result.unwrap();
        let mut page_count = 0;
        while let Some(page) = page_reader_0.get_next_page().unwrap() {
            let is_expected_page = match page {
                Page::DictionaryPage {
                    buf,
                    num_values,
                    encoding,
                    is_sorted,
                } => {
                    assert_eq!(buf.len(), 0);
                    assert_eq!(num_values, 0);
                    assert_eq!(encoding, Encoding::PLAIN);
                    assert!(!is_sorted);
                    true
                }
                Page::DataPageV2 {
                    buf,
                    num_values,
                    encoding,
                    num_nulls,
                    num_rows,
                    def_levels_byte_len,
                    rep_levels_byte_len,
                    is_compressed,
                    statistics,
                } => {
                    assert_eq!(buf.len(), 3);
                    assert_eq!(num_values, 10);
                    assert_eq!(encoding, Encoding::RLE_DICTIONARY);
                    assert_eq!(num_nulls, 10);
                    assert_eq!(num_rows, 10);
                    assert_eq!(def_levels_byte_len, 2);
                    assert_eq!(rep_levels_byte_len, 0);
                    assert!(is_compressed);
                    assert!(statistics.is_none()); // page stats are no longer read
                    true
                }
                _ => false,
            };
            assert!(is_expected_page);
            page_count += 1;
        }
        assert_eq!(page_count, 2);
    }

    #[test]
    fn test_file_reader_empty_datapage_v2() {
        // this file has 0 bytes compressed datapage that un-compresses to 0 bytes
        let test_file = get_test_file("datapage_v2_empty_datapage.snappy.parquet");
        let reader_result = SerializedFileReader::new(test_file);
        assert!(reader_result.is_ok());
        let reader = reader_result.unwrap();

        // Test contents in Parquet metadata
        let metadata = reader.metadata();
        assert_eq!(metadata.num_row_groups(), 1);

        // Test contents in file metadata
        let file_metadata = metadata.file_metadata();
        assert!(file_metadata.created_by().is_some());
        assert_eq!(
            file_metadata.created_by().unwrap(),
            "parquet-mr version 1.13.1 (build db4183109d5b734ec5930d870cdae161e408ddba)"
        );
        assert!(file_metadata.key_value_metadata().is_some());
        assert_eq!(
            file_metadata.key_value_metadata().to_owned().unwrap().len(),
            2
        );

        assert_eq!(file_metadata.num_rows(), 1);
        assert_eq!(file_metadata.version(), 1);
        let expected_order = ColumnOrder::TYPE_DEFINED_ORDER(SortOrder::SIGNED);
        assert_eq!(
            file_metadata.column_orders(),
            Some(vec![expected_order].as_ref())
        );

        let row_group_metadata = metadata.row_group(0);

        // Check each column order
        for i in 0..row_group_metadata.num_columns() {
            assert_eq!(file_metadata.column_order(i), expected_order);
        }

        // Test row group reader
        let row_group_reader_result = reader.get_row_group(0);
        assert!(row_group_reader_result.is_ok());
        let row_group_reader: Box<dyn RowGroupReader> = row_group_reader_result.unwrap();
        assert_eq!(
            row_group_reader.num_columns(),
            row_group_metadata.num_columns()
        );
        assert_eq!(
            row_group_reader.metadata().total_byte_size(),
            row_group_metadata.total_byte_size()
        );

        // Test page readers
        let page_reader_0_result = row_group_reader.get_column_page_reader(0);
        assert!(page_reader_0_result.is_ok());
        let mut page_reader_0: Box<dyn PageReader> = page_reader_0_result.unwrap();
        let mut page_count = 0;
        while let Some(page) = page_reader_0.get_next_page().unwrap() {
            let is_expected_page = match page {
                Page::DataPageV2 {
                    buf,
                    num_values,
                    encoding,
                    num_nulls,
                    num_rows,
                    def_levels_byte_len,
                    rep_levels_byte_len,
                    is_compressed,
                    statistics,
                } => {
                    assert_eq!(buf.len(), 2);
                    assert_eq!(num_values, 1);
                    assert_eq!(encoding, Encoding::PLAIN);
                    assert_eq!(num_nulls, 1);
                    assert_eq!(num_rows, 1);
                    assert_eq!(def_levels_byte_len, 2);
                    assert_eq!(rep_levels_byte_len, 0);
                    assert!(is_compressed);
                    assert!(statistics.is_none());
                    true
                }
                _ => false,
            };
            assert!(is_expected_page);
            page_count += 1;
        }
        assert_eq!(page_count, 1);
    }

    fn get_serialized_page_reader<R: ChunkReader>(
        file_reader: &SerializedFileReader<R>,
        row_group: usize,
        column: usize,
    ) -> Result<SerializedPageReader<R>> {
        let row_group = {
            let row_group_metadata = file_reader.metadata.row_group(row_group);
            let props = Arc::clone(&file_reader.props);
            let f = Arc::clone(&file_reader.chunk_reader);
            SerializedRowGroupReader::new(
                f,
                row_group_metadata,
                file_reader
                    .metadata
                    .offset_index()
                    .map(|x| x[row_group].as_slice()),
                props,
            )?
        };

        let col = row_group.metadata.column(column);

        let page_locations = row_group
            .offset_index
            .map(|x| x[column].page_locations.clone());

        let props = Arc::clone(&row_group.props);
        SerializedPageReader::new_with_properties(
            Arc::clone(&row_group.chunk_reader),
            col,
            usize::try_from(row_group.metadata.num_rows())?,
            page_locations,
            props,
        )
    }

    #[test]
    fn test_peek_next_page_offset_matches_actual() -> Result<()> {
        let test_file = get_test_file("alltypes_plain.parquet");
        let reader = SerializedFileReader::new(test_file)?;

        let mut offset_set = HashSet::new();
        let num_row_groups = reader.metadata.num_row_groups();
        for row_group in 0..num_row_groups {
            let num_columns = reader.metadata.row_group(row_group).num_columns();
            for column in 0..num_columns {
                let mut page_reader = get_serialized_page_reader(&reader, row_group, column)?;

                while let Ok(Some(page_offset)) = page_reader.peek_next_page_offset() {
                    match &page_reader.state {
                        SerializedPageReaderState::Pages {
                            page_locations,
                            dictionary_page,
                            ..
                        } => {
                            if let Some(page) = dictionary_page {
                                assert_eq!(page.offset as u64, page_offset);
                            } else if let Some(page) = page_locations.front() {
                                assert_eq!(page.offset as u64, page_offset);
                            } else {
                                unreachable!()
                            }
                        }
                        SerializedPageReaderState::Values {
                            offset,
                            next_page_header,
                            ..
                        } => {
                            assert!(next_page_header.is_some());
                            assert_eq!(*offset, page_offset);
                        }
                    }
                    let page = page_reader.get_next_page()?;
                    assert!(page.is_some());
                    let newly_inserted = offset_set.insert(page_offset);
                    assert!(newly_inserted);
                }
            }
        }

        Ok(())
    }

    #[test]
    fn test_page_iterator() {
        let file = get_test_file("alltypes_plain.parquet");
        let file_reader = Arc::new(SerializedFileReader::new(file).unwrap());

        let mut page_iterator = FilePageIterator::new(0, file_reader.clone()).unwrap();

        // read first page
        let page = page_iterator.next();
        assert!(page.is_some());
        assert!(page.unwrap().is_ok());

        // reach end of file
        let page = page_iterator.next();
        assert!(page.is_none());

        let row_group_indices = Box::new(0..1);
        let mut page_iterator =
            FilePageIterator::with_row_groups(0, row_group_indices, file_reader).unwrap();

        // read first page
        let page = page_iterator.next();
        assert!(page.is_some());
        assert!(page.unwrap().is_ok());

        // reach end of file
        let page = page_iterator.next();
        assert!(page.is_none());
    }

    #[test]
    fn test_file_reader_key_value_metadata() {
        let file = get_test_file("binary.parquet");
        let file_reader = Arc::new(SerializedFileReader::new(file).unwrap());

        let metadata = file_reader
            .metadata
            .file_metadata()
            .key_value_metadata()
            .unwrap();

        assert_eq!(metadata.len(), 3);

        assert_eq!(metadata[0].key, "parquet.proto.descriptor");

        assert_eq!(metadata[1].key, "writer.model.name");
        assert_eq!(metadata[1].value, Some("protobuf".to_owned()));

        assert_eq!(metadata[2].key, "parquet.proto.class");
        assert_eq!(metadata[2].value, Some("foo.baz.Foobaz$Event".to_owned()));
    }

    #[test]
    fn test_file_reader_optional_metadata() {
        // file with optional metadata: bloom filters, encoding stats, column index and offset index.
        let file = get_test_file("data_index_bloom_encoding_stats.parquet");
        let file_reader = Arc::new(SerializedFileReader::new(file).unwrap());

        let row_group_metadata = file_reader.metadata.row_group(0);
        let col0_metadata = row_group_metadata.column(0);

        // test optional bloom filter offset
        assert_eq!(col0_metadata.bloom_filter_offset().unwrap(), 192);

        // test page encoding stats
        let page_encoding_stats = &col0_metadata.page_encoding_stats().unwrap()[0];

        assert_eq!(page_encoding_stats.page_type, basic::PageType::DATA_PAGE);
        assert_eq!(page_encoding_stats.encoding, Encoding::PLAIN);
        assert_eq!(page_encoding_stats.count, 1);

        // test optional column index offset
        assert_eq!(col0_metadata.column_index_offset().unwrap(), 156);
        assert_eq!(col0_metadata.column_index_length().unwrap(), 25);

        // test optional offset index offset
        assert_eq!(col0_metadata.offset_index_offset().unwrap(), 181);
        assert_eq!(col0_metadata.offset_index_length().unwrap(), 11);
    }

    #[test]
    fn test_file_reader_page_stats_mask() {
        let file = get_test_file("alltypes_tiny_pages.parquet");
        let options = ReadOptionsBuilder::new()
            .with_encoding_stats_as_mask(true)
            .build();
        let file_reader = Arc::new(SerializedFileReader::new_with_options(file, options).unwrap());

        let row_group_metadata = file_reader.metadata.row_group(0);

        // test page encoding stats
        let page_encoding_stats = row_group_metadata
            .column(0)
            .page_encoding_stats_mask()
            .unwrap();
        assert!(page_encoding_stats.is_only(Encoding::PLAIN));
        let page_encoding_stats = row_group_metadata
            .column(2)
            .page_encoding_stats_mask()
            .unwrap();
        assert!(page_encoding_stats.is_only(Encoding::PLAIN_DICTIONARY));
    }

    #[test]
    fn test_file_reader_page_stats_skipped() {
        let file = get_test_file("alltypes_tiny_pages.parquet");

        // test skipping all
        let options = ReadOptionsBuilder::new()
            .with_encoding_stats_policy(ParquetStatisticsPolicy::SkipAll)
            .build();
        let file_reader = Arc::new(
            SerializedFileReader::new_with_options(file.try_clone().unwrap(), options).unwrap(),
        );

        let row_group_metadata = file_reader.metadata.row_group(0);
        for column in row_group_metadata.columns() {
            assert!(column.page_encoding_stats().is_none());
            assert!(column.page_encoding_stats_mask().is_none());
        }

        // test skipping all but one column
        let options = ReadOptionsBuilder::new()
            .with_encoding_stats_as_mask(true)
            .with_encoding_stats_policy(ParquetStatisticsPolicy::skip_except(&[0]))
            .build();
        let file_reader = Arc::new(
            SerializedFileReader::new_with_options(file.try_clone().unwrap(), options).unwrap(),
        );

        let row_group_metadata = file_reader.metadata.row_group(0);
        for (idx, column) in row_group_metadata.columns().iter().enumerate() {
            assert!(column.page_encoding_stats().is_none());
            assert_eq!(column.page_encoding_stats_mask().is_some(), idx == 0);
        }
    }

    #[test]
    fn test_file_reader_with_no_filter() -> Result<()> {
        let test_file = get_test_file("alltypes_plain.parquet");
        let origin_reader = SerializedFileReader::new(test_file)?;
        // test initial number of row groups
        let metadata = origin_reader.metadata();
        assert_eq!(metadata.num_row_groups(), 1);
        Ok(())
    }

    #[test]
    fn test_file_reader_filter_row_groups_with_predicate() -> Result<()> {
        let test_file = get_test_file("alltypes_plain.parquet");
        let read_options = ReadOptionsBuilder::new()
            .with_predicate(Box::new(|_, _| false))
            .build();
        let reader = SerializedFileReader::new_with_options(test_file, read_options)?;
        let metadata = reader.metadata();
        assert_eq!(metadata.num_row_groups(), 0);
        Ok(())
    }

    #[test]
    fn test_file_reader_filter_row_groups_with_range() -> Result<()> {
        let test_file = get_test_file("alltypes_plain.parquet");
        let origin_reader = SerializedFileReader::new(test_file)?;
        // test initial number of row groups
        let metadata = origin_reader.metadata();
        assert_eq!(metadata.num_row_groups(), 1);
        let mid = get_midpoint_offset(metadata.row_group(0));

        let test_file = get_test_file("alltypes_plain.parquet");
        let read_options = ReadOptionsBuilder::new().with_range(0, mid + 1).build();
        let reader = SerializedFileReader::new_with_options(test_file, read_options)?;
        let metadata = reader.metadata();
        assert_eq!(metadata.num_row_groups(), 1);

        let test_file = get_test_file("alltypes_plain.parquet");
        let read_options = ReadOptionsBuilder::new().with_range(0, mid).build();
        let reader = SerializedFileReader::new_with_options(test_file, read_options)?;
        let metadata = reader.metadata();
        assert_eq!(metadata.num_row_groups(), 0);
        Ok(())
    }

    #[test]
    fn test_file_reader_filter_row_groups_and_range() -> Result<()> {
        let test_file = get_test_file("alltypes_tiny_pages.parquet");
        let origin_reader = SerializedFileReader::new(test_file)?;
        let metadata = origin_reader.metadata();
        let mid = get_midpoint_offset(metadata.row_group(0));

        // true, true predicate
        let test_file = get_test_file("alltypes_tiny_pages.parquet");
        let read_options = ReadOptionsBuilder::new()
            .with_page_index()
            .with_predicate(Box::new(|_, _| true))
            .with_range(mid, mid + 1)
            .build();
        let reader = SerializedFileReader::new_with_options(test_file, read_options)?;
        let metadata = reader.metadata();
        assert_eq!(metadata.num_row_groups(), 1);
        assert_eq!(metadata.column_index().unwrap().len(), 1);
        assert_eq!(metadata.offset_index().unwrap().len(), 1);

        // true, false predicate
        let test_file = get_test_file("alltypes_tiny_pages.parquet");
        let read_options = ReadOptionsBuilder::new()
            .with_page_index()
            .with_predicate(Box::new(|_, _| true))
            .with_range(0, mid)
            .build();
        let reader = SerializedFileReader::new_with_options(test_file, read_options)?;
        let metadata = reader.metadata();
        assert_eq!(metadata.num_row_groups(), 0);
        assert!(metadata.column_index().is_none());
        assert!(metadata.offset_index().is_none());

        // false, true predicate
        let test_file = get_test_file("alltypes_tiny_pages.parquet");
        let read_options = ReadOptionsBuilder::new()
            .with_page_index()
            .with_predicate(Box::new(|_, _| false))
            .with_range(mid, mid + 1)
            .build();
        let reader = SerializedFileReader::new_with_options(test_file, read_options)?;
        let metadata = reader.metadata();
        assert_eq!(metadata.num_row_groups(), 0);
        assert!(metadata.column_index().is_none());
        assert!(metadata.offset_index().is_none());

        // false, false predicate
        let test_file = get_test_file("alltypes_tiny_pages.parquet");
        let read_options = ReadOptionsBuilder::new()
            .with_page_index()
            .with_predicate(Box::new(|_, _| false))
            .with_range(0, mid)
            .build();
        let reader = SerializedFileReader::new_with_options(test_file, read_options)?;
        let metadata = reader.metadata();
        assert_eq!(metadata.num_row_groups(), 0);
        assert!(metadata.column_index().is_none());
        assert!(metadata.offset_index().is_none());
        Ok(())
    }

    #[test]
    fn test_file_reader_invalid_metadata() {
        let data = [
            255, 172, 1, 0, 50, 82, 65, 73, 1, 0, 0, 0, 169, 168, 168, 162, 87, 255, 16, 0, 0, 0,
            80, 65, 82, 49,
        ];
        let ret = SerializedFileReader::new(Bytes::copy_from_slice(&data));
        assert_eq!(
            ret.err().unwrap().to_string(),
            "Parquet error: Received empty union from remote ColumnOrder"
        );
    }

    #[test]
    // Use java parquet-tools get below pageIndex info
    // !```
    // parquet-tools column-index ./data_index_bloom_encoding_stats.parquet
    // row group 0:
    // column index for column String:
    // Boundary order: ASCENDING
    // page-0  :
    // null count                 min                                  max
    // 0                          Hello                                today
    //
    // offset index for column String:
    // page-0   :
    // offset   compressed size       first row index
    // 4               152                     0
    ///```
    //
    fn test_page_index_reader() {
        let test_file = get_test_file("data_index_bloom_encoding_stats.parquet");
        let builder = ReadOptionsBuilder::new();
        //enable read page index
        let options = builder.with_page_index().build();
        let reader_result = SerializedFileReader::new_with_options(test_file, options);
        let reader = reader_result.unwrap();

        // Test contents in Parquet metadata
        let metadata = reader.metadata();
        assert_eq!(metadata.num_row_groups(), 1);

        let column_index = metadata.column_index().unwrap();

        // only one row group
        assert_eq!(column_index.len(), 1);
        let index = if let ColumnIndexMetaData::BYTE_ARRAY(index) = &column_index[0][0] {
            index
        } else {
            unreachable!()
        };

        assert_eq!(index.boundary_order, BoundaryOrder::ASCENDING);

        //only one page group
        assert_eq!(index.num_pages(), 1);

        let min = index.min_value(0).unwrap();
        let max = index.max_value(0).unwrap();
        assert_eq!(b"Hello", min.as_bytes());
        assert_eq!(b"today", max.as_bytes());

        let offset_indexes = metadata.offset_index().unwrap();
        // only one row group
        assert_eq!(offset_indexes.len(), 1);
        let offset_index = &offset_indexes[0];
        let page_offset = &offset_index[0].page_locations()[0];

        assert_eq!(4, page_offset.offset);
        assert_eq!(152, page_offset.compressed_page_size);
        assert_eq!(0, page_offset.first_row_index);
    }

    #[test]
    #[allow(deprecated)]
    fn test_page_index_reader_out_of_order() {
        let test_file = get_test_file("alltypes_tiny_pages_plain.parquet");
        let options = ReadOptionsBuilder::new().with_page_index().build();
        let reader = SerializedFileReader::new_with_options(test_file, options).unwrap();
        let metadata = reader.metadata();

        let test_file = get_test_file("alltypes_tiny_pages_plain.parquet");
        let columns = metadata.row_group(0).columns();
        let reversed: Vec<_> = columns.iter().cloned().rev().collect();

        let a = read_columns_indexes(&test_file, columns).unwrap().unwrap();
        let mut b = read_columns_indexes(&test_file, &reversed)
            .unwrap()
            .unwrap();
        b.reverse();
        assert_eq!(a, b);

        let a = read_offset_indexes(&test_file, columns).unwrap().unwrap();
        let mut b = read_offset_indexes(&test_file, &reversed).unwrap().unwrap();
        b.reverse();
        assert_eq!(a, b);
    }

    #[test]
    fn test_page_index_reader_all_type() {
        let test_file = get_test_file("alltypes_tiny_pages_plain.parquet");
        let builder = ReadOptionsBuilder::new();
        //enable read page index
        let options = builder.with_page_index().build();
        let reader_result = SerializedFileReader::new_with_options(test_file, options);
        let reader = reader_result.unwrap();

        // Test contents in Parquet metadata
        let metadata = reader.metadata();
        assert_eq!(metadata.num_row_groups(), 1);

        let column_index = metadata.column_index().unwrap();
        let row_group_offset_indexes = &metadata.offset_index().unwrap()[0];

        // only one row group
        assert_eq!(column_index.len(), 1);
        let row_group_metadata = metadata.row_group(0);

        //col0->id: INT32 UNCOMPRESSED DO:0 FPO:4 SZ:37325/37325/1.00 VC:7300 ENC:BIT_PACKED,RLE,PLAIN ST:[min: 0, max: 7299, num_nulls: 0]
        assert!(!&column_index[0][0].is_sorted());
        let boundary_order = &column_index[0][0].get_boundary_order();
        assert!(boundary_order.is_some());
        matches!(boundary_order.unwrap(), BoundaryOrder::UNORDERED);
        if let ColumnIndexMetaData::INT32(index) = &column_index[0][0] {
            check_native_page_index(
                index,
                325,
                get_row_group_min_max_bytes(row_group_metadata, 0),
                BoundaryOrder::UNORDERED,
            );
            assert_eq!(row_group_offset_indexes[0].page_locations.len(), 325);
        } else {
            unreachable!()
        };
        //col1->bool_col:BOOLEAN UNCOMPRESSED DO:0 FPO:37329 SZ:3022/3022/1.00 VC:7300 ENC:BIT_PACKED,RLE,PLAIN ST:[min: false, max: true, num_nulls: 0]
        assert!(&column_index[0][1].is_sorted());
        if let ColumnIndexMetaData::BOOLEAN(index) = &column_index[0][1] {
            assert_eq!(index.num_pages(), 82);
            assert_eq!(row_group_offset_indexes[1].page_locations.len(), 82);
        } else {
            unreachable!()
        };
        //col2->tinyint_col: INT32 UNCOMPRESSED DO:0 FPO:40351 SZ:37325/37325/1.00 VC:7300 ENC:BIT_PACKED,RLE,PLAIN ST:[min: 0, max: 9, num_nulls: 0]
        assert!(&column_index[0][2].is_sorted());
        if let ColumnIndexMetaData::INT32(index) = &column_index[0][2] {
            check_native_page_index(
                index,
                325,
                get_row_group_min_max_bytes(row_group_metadata, 2),
                BoundaryOrder::ASCENDING,
            );
            assert_eq!(row_group_offset_indexes[2].page_locations.len(), 325);
        } else {
            unreachable!()
        };
        //col4->smallint_col: INT32 UNCOMPRESSED DO:0 FPO:77676 SZ:37325/37325/1.00 VC:7300 ENC:BIT_PACKED,RLE,PLAIN ST:[min: 0, max: 9, num_nulls: 0]
        assert!(&column_index[0][3].is_sorted());
        if let ColumnIndexMetaData::INT32(index) = &column_index[0][3] {
            check_native_page_index(
                index,
                325,
                get_row_group_min_max_bytes(row_group_metadata, 3),
                BoundaryOrder::ASCENDING,
            );
            assert_eq!(row_group_offset_indexes[3].page_locations.len(), 325);
        } else {
            unreachable!()
        };
        //col5->smallint_col: INT32 UNCOMPRESSED DO:0 FPO:77676 SZ:37325/37325/1.00 VC:7300 ENC:BIT_PACKED,RLE,PLAIN ST:[min: 0, max: 9, num_nulls: 0]
        assert!(&column_index[0][4].is_sorted());
        if let ColumnIndexMetaData::INT32(index) = &column_index[0][4] {
            check_native_page_index(
                index,
                325,
                get_row_group_min_max_bytes(row_group_metadata, 4),
                BoundaryOrder::ASCENDING,
            );
            assert_eq!(row_group_offset_indexes[4].page_locations.len(), 325);
        } else {
            unreachable!()
        };
        //col6->bigint_col: INT64 UNCOMPRESSED DO:0 FPO:152326 SZ:71598/71598/1.00 VC:7300 ENC:BIT_PACKED,RLE,PLAIN ST:[min: 0, max: 90, num_nulls: 0]
        assert!(!&column_index[0][5].is_sorted());
        if let ColumnIndexMetaData::INT64(index) = &column_index[0][5] {
            check_native_page_index(
                index,
                528,
                get_row_group_min_max_bytes(row_group_metadata, 5),
                BoundaryOrder::UNORDERED,
            );
            assert_eq!(row_group_offset_indexes[5].page_locations.len(), 528);
        } else {
            unreachable!()
        };
        //col7->float_col: FLOAT UNCOMPRESSED DO:0 FPO:223924 SZ:37325/37325/1.00 VC:7300 ENC:BIT_PACKED,RLE,PLAIN ST:[min: -0.0, max: 9.9, num_nulls: 0]
        assert!(&column_index[0][6].is_sorted());
        if let ColumnIndexMetaData::FLOAT(index) = &column_index[0][6] {
            check_native_page_index(
                index,
                325,
                get_row_group_min_max_bytes(row_group_metadata, 6),
                BoundaryOrder::ASCENDING,
            );
            assert_eq!(row_group_offset_indexes[6].page_locations.len(), 325);
        } else {
            unreachable!()
        };
        //col8->double_col: DOUBLE UNCOMPRESSED DO:0 FPO:261249 SZ:71598/71598/1.00 VC:7300 ENC:BIT_PACKED,RLE,PLAIN ST:[min: -0.0, max: 90.89999999999999, num_nulls: 0]
        assert!(!&column_index[0][7].is_sorted());
        if let ColumnIndexMetaData::DOUBLE(index) = &column_index[0][7] {
            check_native_page_index(
                index,
                528,
                get_row_group_min_max_bytes(row_group_metadata, 7),
                BoundaryOrder::UNORDERED,
            );
            assert_eq!(row_group_offset_indexes[7].page_locations.len(), 528);
        } else {
            unreachable!()
        };
        //col9->date_string_col: BINARY UNCOMPRESSED DO:0 FPO:332847 SZ:111948/111948/1.00 VC:7300 ENC:BIT_PACKED,RLE,PLAIN ST:[min: 01/01/09, max: 12/31/10, num_nulls: 0]
        assert!(!&column_index[0][8].is_sorted());
        if let ColumnIndexMetaData::BYTE_ARRAY(index) = &column_index[0][8] {
            check_byte_array_page_index(
                index,
                974,
                get_row_group_min_max_bytes(row_group_metadata, 8),
                BoundaryOrder::UNORDERED,
            );
            assert_eq!(row_group_offset_indexes[8].page_locations.len(), 974);
        } else {
            unreachable!()
        };
        //col10->string_col: BINARY UNCOMPRESSED DO:0 FPO:444795 SZ:45298/45298/1.00 VC:7300 ENC:BIT_PACKED,RLE,PLAIN ST:[min: 0, max: 9, num_nulls: 0]
        assert!(&column_index[0][9].is_sorted());
        if let ColumnIndexMetaData::BYTE_ARRAY(index) = &column_index[0][9] {
            check_byte_array_page_index(
                index,
                352,
                get_row_group_min_max_bytes(row_group_metadata, 9),
                BoundaryOrder::ASCENDING,
            );
            assert_eq!(row_group_offset_indexes[9].page_locations.len(), 352);
        } else {
            unreachable!()
        };
        //col11->timestamp_col: INT96 UNCOMPRESSED DO:0 FPO:490093 SZ:111948/111948/1.00 VC:7300 ENC:BIT_PACKED,RLE,PLAIN ST:[num_nulls: 0, min/max not defined]
        //Notice: min_max values for each page for this col not exits.
        assert!(!&column_index[0][10].is_sorted());
        if let ColumnIndexMetaData::NONE = &column_index[0][10] {
            assert_eq!(row_group_offset_indexes[10].page_locations.len(), 974);
        } else {
            unreachable!()
        };
        //col12->year: INT32 UNCOMPRESSED DO:0 FPO:602041 SZ:37325/37325/1.00 VC:7300 ENC:BIT_PACKED,RLE,PLAIN ST:[min: 2009, max: 2010, num_nulls: 0]
        assert!(&column_index[0][11].is_sorted());
        if let ColumnIndexMetaData::INT32(index) = &column_index[0][11] {
            check_native_page_index(
                index,
                325,
                get_row_group_min_max_bytes(row_group_metadata, 11),
                BoundaryOrder::ASCENDING,
            );
            assert_eq!(row_group_offset_indexes[11].page_locations.len(), 325);
        } else {
            unreachable!()
        };
        //col13->month: INT32 UNCOMPRESSED DO:0 FPO:639366 SZ:37325/37325/1.00 VC:7300 ENC:BIT_PACKED,RLE,PLAIN ST:[min: 1, max: 12, num_nulls: 0]
        assert!(!&column_index[0][12].is_sorted());
        if let ColumnIndexMetaData::INT32(index) = &column_index[0][12] {
            check_native_page_index(
                index,
                325,
                get_row_group_min_max_bytes(row_group_metadata, 12),
                BoundaryOrder::UNORDERED,
            );
            assert_eq!(row_group_offset_indexes[12].page_locations.len(), 325);
        } else {
            unreachable!()
        };
    }

    fn check_native_page_index<T: ParquetValueType>(
        row_group_index: &PrimitiveColumnIndex<T>,
        page_size: usize,
        min_max: (&[u8], &[u8]),
        boundary_order: BoundaryOrder,
    ) {
        assert_eq!(row_group_index.num_pages() as usize, page_size);
        assert_eq!(row_group_index.boundary_order, boundary_order);
        assert!(row_group_index.min_values().iter().all(|x| {
            x >= &T::try_from_le_slice(min_max.0).unwrap()
                && x <= &T::try_from_le_slice(min_max.1).unwrap()
        }));
    }

    fn check_byte_array_page_index(
        row_group_index: &ByteArrayColumnIndex,
        page_size: usize,
        min_max: (&[u8], &[u8]),
        boundary_order: BoundaryOrder,
    ) {
        assert_eq!(row_group_index.num_pages() as usize, page_size);
        assert_eq!(row_group_index.boundary_order, boundary_order);
        for i in 0..row_group_index.num_pages() as usize {
            let x = row_group_index.min_value(i).unwrap();
            assert!(x >= min_max.0 && x <= min_max.1);
        }
    }

    fn get_row_group_min_max_bytes(r: &RowGroupMetaData, col_num: usize) -> (&[u8], &[u8]) {
        let statistics = r.column(col_num).statistics().unwrap();
        (
            statistics.min_bytes_opt().unwrap_or_default(),
            statistics.max_bytes_opt().unwrap_or_default(),
        )
    }

    #[test]
    fn test_skip_next_page_with_dictionary_page() {
        let test_file = get_test_file("alltypes_tiny_pages.parquet");
        let builder = ReadOptionsBuilder::new();
        // enable read page index
        let options = builder.with_page_index().build();
        let reader_result = SerializedFileReader::new_with_options(test_file, options);
        let reader = reader_result.unwrap();

        let row_group_reader = reader.get_row_group(0).unwrap();

        // use 'string_col', Boundary order: UNORDERED, total 352 data pages and 1 dictionary page.
        let mut column_page_reader = row_group_reader.get_column_page_reader(9).unwrap();

        let mut vec = vec![];

        // Step 1: Peek and ensure dictionary page is correctly identified
        let meta = column_page_reader.peek_next_page().unwrap().unwrap();
        assert!(meta.is_dict);

        // Step 2: Call skip_next_page to skip the dictionary page
        column_page_reader.skip_next_page().unwrap();

        // Step 3: Read the next data page after skipping the dictionary page
        let page = column_page_reader.get_next_page().unwrap().unwrap();
        assert!(matches!(page.page_type(), basic::PageType::DATA_PAGE));

        // Step 4: Continue reading remaining data pages and verify correctness
        for _i in 0..351 {
            // 352 total pages, 1 dictionary page is skipped
            let meta = column_page_reader.peek_next_page().unwrap().unwrap();
            assert!(!meta.is_dict); // Verify no dictionary page here
            vec.push(meta);

            let page = column_page_reader.get_next_page().unwrap().unwrap();
            assert!(matches!(page.page_type(), basic::PageType::DATA_PAGE));
        }

        // Step 5: Check if all pages are read
        assert!(column_page_reader.peek_next_page().unwrap().is_none());
        assert!(column_page_reader.get_next_page().unwrap().is_none());

        // Step 6: Verify the number of data pages read (should be 351 data pages)
        assert_eq!(vec.len(), 351);
    }

    #[test]
    fn test_skip_page_with_offset_index() {
        let test_file = get_test_file("alltypes_tiny_pages_plain.parquet");
        let builder = ReadOptionsBuilder::new();
        //enable read page index
        let options = builder.with_page_index().build();
        let reader_result = SerializedFileReader::new_with_options(test_file, options);
        let reader = reader_result.unwrap();

        let row_group_reader = reader.get_row_group(0).unwrap();

        //use 'int_col', Boundary order: ASCENDING, total 325 pages.
        let mut column_page_reader = row_group_reader.get_column_page_reader(4).unwrap();

        let mut vec = vec![];

        for i in 0..325 {
            if i % 2 == 0 {
                vec.push(column_page_reader.get_next_page().unwrap().unwrap());
            } else {
                column_page_reader.skip_next_page().unwrap();
            }
        }
        //check read all pages.
        assert!(column_page_reader.peek_next_page().unwrap().is_none());
        assert!(column_page_reader.get_next_page().unwrap().is_none());

        assert_eq!(vec.len(), 163);
    }

    #[test]
    fn test_skip_page_without_offset_index() {
        let test_file = get_test_file("alltypes_tiny_pages_plain.parquet");

        // use default SerializedFileReader without read offsetIndex
        let reader_result = SerializedFileReader::new(test_file);
        let reader = reader_result.unwrap();

        let row_group_reader = reader.get_row_group(0).unwrap();

        //use 'int_col', Boundary order: ASCENDING, total 325 pages.
        let mut column_page_reader = row_group_reader.get_column_page_reader(4).unwrap();

        let mut vec = vec![];

        for i in 0..325 {
            if i % 2 == 0 {
                vec.push(column_page_reader.get_next_page().unwrap().unwrap());
            } else {
                column_page_reader.peek_next_page().unwrap().unwrap();
                column_page_reader.skip_next_page().unwrap();
            }
        }
        //check read all pages.
        assert!(column_page_reader.peek_next_page().unwrap().is_none());
        assert!(column_page_reader.get_next_page().unwrap().is_none());

        assert_eq!(vec.len(), 163);
    }

    #[test]
    fn test_peek_page_with_dictionary_page() {
        let test_file = get_test_file("alltypes_tiny_pages.parquet");
        let builder = ReadOptionsBuilder::new();
        //enable read page index
        let options = builder.with_page_index().build();
        let reader_result = SerializedFileReader::new_with_options(test_file, options);
        let reader = reader_result.unwrap();
        let row_group_reader = reader.get_row_group(0).unwrap();

        //use 'string_col', Boundary order: UNORDERED, total 352 data ages and 1 dictionary page.
        let mut column_page_reader = row_group_reader.get_column_page_reader(9).unwrap();

        let mut vec = vec![];

        let meta = column_page_reader.peek_next_page().unwrap().unwrap();
        assert!(meta.is_dict);
        let page = column_page_reader.get_next_page().unwrap().unwrap();
        assert!(matches!(page.page_type(), basic::PageType::DICTIONARY_PAGE));

        for i in 0..352 {
            let meta = column_page_reader.peek_next_page().unwrap().unwrap();
            // have checked with `parquet-tools column-index   -c string_col  ./alltypes_tiny_pages.parquet`
            // page meta has two scenarios(21, 20) of num_rows expect last page has 11 rows.
            if i != 351 {
                assert!((meta.num_rows == Some(21)) || (meta.num_rows == Some(20)));
            } else {
                // last page first row index is 7290, total row count is 7300
                // because first row start with zero, last page row count should be 10.
                assert_eq!(meta.num_rows, Some(10));
            }
            assert!(!meta.is_dict);
            vec.push(meta);
            let page = column_page_reader.get_next_page().unwrap().unwrap();
            assert!(matches!(page.page_type(), basic::PageType::DATA_PAGE));
        }

        //check read all pages.
        assert!(column_page_reader.peek_next_page().unwrap().is_none());
        assert!(column_page_reader.get_next_page().unwrap().is_none());

        assert_eq!(vec.len(), 352);
    }

    #[test]
    fn test_peek_page_with_dictionary_page_without_offset_index() {
        let test_file = get_test_file("alltypes_tiny_pages.parquet");

        let reader_result = SerializedFileReader::new(test_file);
        let reader = reader_result.unwrap();
        let row_group_reader = reader.get_row_group(0).unwrap();

        //use 'string_col', Boundary order: UNORDERED, total 352 data ages and 1 dictionary page.
        let mut column_page_reader = row_group_reader.get_column_page_reader(9).unwrap();

        let mut vec = vec![];

        let meta = column_page_reader.peek_next_page().unwrap().unwrap();
        assert!(meta.is_dict);
        let page = column_page_reader.get_next_page().unwrap().unwrap();
        assert!(matches!(page.page_type(), basic::PageType::DICTIONARY_PAGE));

        for i in 0..352 {
            let meta = column_page_reader.peek_next_page().unwrap().unwrap();
            // have checked with `parquet-tools column-index   -c string_col  ./alltypes_tiny_pages.parquet`
            // page meta has two scenarios(21, 20) of num_rows expect last page has 11 rows.
            if i != 351 {
                assert!((meta.num_levels == Some(21)) || (meta.num_levels == Some(20)));
            } else {
                // last page first row index is 7290, total row count is 7300
                // because first row start with zero, last page row count should be 10.
                assert_eq!(meta.num_levels, Some(10));
            }
            assert!(!meta.is_dict);
            vec.push(meta);
            let page = column_page_reader.get_next_page().unwrap().unwrap();
            assert!(matches!(page.page_type(), basic::PageType::DATA_PAGE));
        }

        //check read all pages.
        assert!(column_page_reader.peek_next_page().unwrap().is_none());
        assert!(column_page_reader.get_next_page().unwrap().is_none());

        assert_eq!(vec.len(), 352);
    }

    #[test]
    fn test_fixed_length_index() {
        let message_type = "
        message test_schema {
          OPTIONAL FIXED_LEN_BYTE_ARRAY (11) value (DECIMAL(25,2));
        }
        ";

        let schema = parse_message_type(message_type).unwrap();
        let mut out = Vec::with_capacity(1024);
        let mut writer =
            SerializedFileWriter::new(&mut out, Arc::new(schema), Default::default()).unwrap();

        let mut r = writer.next_row_group().unwrap();
        let mut c = r.next_column().unwrap().unwrap();
        c.typed::<FixedLenByteArrayType>()
            .write_batch(
                &[vec![0; 11].into(), vec![5; 11].into(), vec![3; 11].into()],
                Some(&[1, 1, 0, 1]),
                None,
            )
            .unwrap();
        c.close().unwrap();
        r.close().unwrap();
        writer.close().unwrap();

        let b = Bytes::from(out);
        let options = ReadOptionsBuilder::new().with_page_index().build();
        let reader = SerializedFileReader::new_with_options(b, options).unwrap();
        let index = reader.metadata().column_index().unwrap();

        // 1 row group
        assert_eq!(index.len(), 1);
        let c = &index[0];
        // 1 column
        assert_eq!(c.len(), 1);

        match &c[0] {
            ColumnIndexMetaData::FIXED_LEN_BYTE_ARRAY(v) => {
                assert_eq!(v.num_pages(), 1);
                assert_eq!(v.null_count(0).unwrap(), 1);
                assert_eq!(v.min_value(0).unwrap(), &[0; 11]);
                assert_eq!(v.max_value(0).unwrap(), &[5; 11]);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_multi_gz() {
        let file = get_test_file("concatenated_gzip_members.parquet");
        let reader = SerializedFileReader::new(file).unwrap();
        let row_group_reader = reader.get_row_group(0).unwrap();
        match row_group_reader.get_column_reader(0).unwrap() {
            ColumnReader::Int64ColumnReader(mut reader) => {
                let mut buffer = Vec::with_capacity(1024);
                let mut def_levels = Vec::with_capacity(1024);
                let (num_records, num_values, num_levels) = reader
                    .read_records(1024, Some(&mut def_levels), None, &mut buffer)
                    .unwrap();

                assert_eq!(num_records, 513);
                assert_eq!(num_values, 513);
                assert_eq!(num_levels, 513);

                let expected: Vec<i64> = (1..514).collect();
                assert_eq!(&buffer, &expected);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_byte_stream_split_extended() {
        let path = format!(
            "{}/byte_stream_split_extended.gzip.parquet",
            arrow::util::test_util::parquet_test_data(),
        );
        let file = File::open(path).unwrap();
        let reader = Box::new(SerializedFileReader::new(file).expect("Failed to create reader"));

        // Use full schema as projected schema
        let mut iter = reader
            .get_row_iter(None)
            .expect("Failed to create row iterator");

        let mut start = 0;
        let end = reader.metadata().file_metadata().num_rows();

        let check_row = |row: Result<Row, ParquetError>| {
            assert!(row.is_ok());
            let r = row.unwrap();
            assert_eq!(r.get_float16(0).unwrap(), r.get_float16(1).unwrap());
            assert_eq!(r.get_float(2).unwrap(), r.get_float(3).unwrap());
            assert_eq!(r.get_double(4).unwrap(), r.get_double(5).unwrap());
            assert_eq!(r.get_int(6).unwrap(), r.get_int(7).unwrap());
            assert_eq!(r.get_long(8).unwrap(), r.get_long(9).unwrap());
            assert_eq!(r.get_bytes(10).unwrap(), r.get_bytes(11).unwrap());
            assert_eq!(r.get_decimal(12).unwrap(), r.get_decimal(13).unwrap());
        };

        while start < end {
            match iter.next() {
                Some(row) => check_row(row),
                None => break,
            };
            start += 1;
        }
    }

    #[test]
    fn test_filtered_rowgroup_metadata() {
        let message_type = "
            message test_schema {
                REQUIRED INT32 a;
            }
        ";
        let schema = Arc::new(parse_message_type(message_type).unwrap());
        let props = Arc::new(
            WriterProperties::builder()
                .set_statistics_enabled(EnabledStatistics::Page)
                .build(),
        );
        let mut file: File = tempfile::tempfile().unwrap();
        let mut file_writer = SerializedFileWriter::new(&mut file, schema, props).unwrap();
        let data = [1, 2, 3, 4, 5];

        // write 5 row groups
        for idx in 0..5 {
            let data_i: Vec<i32> = data.iter().map(|x| x * (idx + 1)).collect();
            let mut row_group_writer = file_writer.next_row_group().unwrap();
            if let Some(mut writer) = row_group_writer.next_column().unwrap() {
                writer
                    .typed::<Int32Type>()
                    .write_batch(data_i.as_slice(), None, None)
                    .unwrap();
                writer.close().unwrap();
            }
            row_group_writer.close().unwrap();
            file_writer.flushed_row_groups();
        }
        let file_metadata = file_writer.close().unwrap();

        assert_eq!(file_metadata.file_metadata().num_rows(), 25);
        assert_eq!(file_metadata.num_row_groups(), 5);

        // read only the 3rd row group
        let read_options = ReadOptionsBuilder::new()
            .with_page_index()
            .with_predicate(Box::new(|rgmeta, _| rgmeta.ordinal().unwrap_or(0) == 2))
            .build();
        let reader =
            SerializedFileReader::new_with_options(file.try_clone().unwrap(), read_options)
                .unwrap();
        let metadata = reader.metadata();

        // check we got the expected row group
        assert_eq!(metadata.num_row_groups(), 1);
        assert_eq!(metadata.row_group(0).ordinal(), Some(2));

        // check we only got the relevant page indexes
        assert!(metadata.column_index().is_some());
        assert!(metadata.offset_index().is_some());
        assert_eq!(metadata.column_index().unwrap().len(), 1);
        assert_eq!(metadata.offset_index().unwrap().len(), 1);
        let col_idx = metadata.column_index().unwrap();
        let off_idx = metadata.offset_index().unwrap();
        let col_stats = metadata.row_group(0).column(0).statistics().unwrap();
        let pg_idx = &col_idx[0][0];
        let off_idx_i = &off_idx[0][0];

        // test that we got the index matching the row group
        match pg_idx {
            ColumnIndexMetaData::INT32(int_idx) => {
                let min = col_stats.min_bytes_opt().unwrap().get_i32_le();
                let max = col_stats.max_bytes_opt().unwrap().get_i32_le();
                assert_eq!(int_idx.min_value(0), Some(min).as_ref());
                assert_eq!(int_idx.max_value(0), Some(max).as_ref());
            }
            _ => panic!("wrong stats type"),
        }

        // check offset index matches too
        assert_eq!(
            off_idx_i.page_locations[0].offset,
            metadata.row_group(0).column(0).data_page_offset()
        );

        // read non-contiguous row groups
        let read_options = ReadOptionsBuilder::new()
            .with_page_index()
            .with_predicate(Box::new(|rgmeta, _| rgmeta.ordinal().unwrap_or(0) % 2 == 1))
            .build();
        let reader =
            SerializedFileReader::new_with_options(file.try_clone().unwrap(), read_options)
                .unwrap();
        let metadata = reader.metadata();

        // check we got the expected row groups
        assert_eq!(metadata.num_row_groups(), 2);
        assert_eq!(metadata.row_group(0).ordinal(), Some(1));
        assert_eq!(metadata.row_group(1).ordinal(), Some(3));

        // check we only got the relevant page indexes
        assert!(metadata.column_index().is_some());
        assert!(metadata.offset_index().is_some());
        assert_eq!(metadata.column_index().unwrap().len(), 2);
        assert_eq!(metadata.offset_index().unwrap().len(), 2);
        let col_idx = metadata.column_index().unwrap();
        let off_idx = metadata.offset_index().unwrap();

        for (i, col_idx_i) in col_idx.iter().enumerate().take(metadata.num_row_groups()) {
            let col_stats = metadata.row_group(i).column(0).statistics().unwrap();
            let pg_idx = &col_idx_i[0];
            let off_idx_i = &off_idx[i][0];

            // test that we got the index matching the row group
            match pg_idx {
                ColumnIndexMetaData::INT32(int_idx) => {
                    let min = col_stats.min_bytes_opt().unwrap().get_i32_le();
                    let max = col_stats.max_bytes_opt().unwrap().get_i32_le();
                    assert_eq!(int_idx.min_value(0), Some(min).as_ref());
                    assert_eq!(int_idx.max_value(0), Some(max).as_ref());
                }
                _ => panic!("wrong stats type"),
            }

            // check offset index matches too
            assert_eq!(
                off_idx_i.page_locations[0].offset,
                metadata.row_group(i).column(0).data_page_offset()
            );
        }
    }

    #[test]
    fn test_reuse_schema() {
        let file = get_test_file("alltypes_plain.parquet");
        let file_reader = SerializedFileReader::new(file.try_clone().unwrap()).unwrap();
        let schema = file_reader.metadata().file_metadata().schema_descr_ptr();
        let expected = file_reader.metadata;

        let options = ReadOptionsBuilder::new()
            .with_parquet_schema(schema)
            .build();
        let file_reader = SerializedFileReader::new_with_options(file, options).unwrap();

        assert_eq!(expected.as_ref(), file_reader.metadata.as_ref());
        // Should have used the same schema instance
        assert!(Arc::ptr_eq(
            &expected.file_metadata().schema_descr_ptr(),
            &file_reader.metadata.file_metadata().schema_descr_ptr()
        ));
    }

    #[test]
    fn test_read_unknown_logical_type() {
        let file = get_test_file("unknown-logical-type.parquet");
        let reader = SerializedFileReader::new(file).expect("Error opening file");

        let schema = reader.metadata().file_metadata().schema_descr();
        assert_eq!(
            schema.column(0).logical_type_ref(),
            Some(&basic::LogicalType::String)
        );
        assert_eq!(
            schema.column(1).logical_type_ref(),
            Some(&basic::LogicalType::_Unknown { field_id: 2555 })
        );
        assert_eq!(schema.column(1).physical_type(), Type::BYTE_ARRAY);

        let mut iter = reader
            .get_row_iter(None)
            .expect("Failed to create row iterator");

        let mut num_rows = 0;
        while iter.next().is_some() {
            num_rows += 1;
        }
        assert_eq!(num_rows, reader.metadata().file_metadata().num_rows());
    }
}
