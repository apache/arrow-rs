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

use std::collections::VecDeque;
use std::iter;
use std::{fs::File, io::Read, path::Path, sync::Arc};

use crate::basic::{Encoding, Type};
use crate::bloom_filter::Sbbf;
use crate::column::page::{Page, PageMetadata, PageReader};
use crate::compression::{create_codec, Codec};
use crate::errors::{ParquetError, Result};
use crate::file::page_index::index_reader;
use crate::file::page_index::offset_index::OffsetIndexMetaData;
use crate::file::{
    metadata::*,
    properties::{ReaderProperties, ReaderPropertiesPtr},
    reader::*,
    statistics,
};
use crate::format::{PageHeader, PageLocation, PageType};
use crate::record::reader::RowIter;
use crate::record::Row;
use crate::schema::types::Type as SchemaType;
use crate::thrift::{TCompactSliceInputProtocol, TSerializable};
use bytes::Bytes;
use thrift::protocol::TCompactInputProtocol;

impl TryFrom<File> for SerializedFileReader<File> {
    type Error = ParquetError;

    fn try_from(file: File) -> Result<Self> {
        Self::new(file)
    }
}

impl<'a> TryFrom<&'a Path> for SerializedFileReader<File> {
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

impl<'a> TryFrom<&'a str> for SerializedFileReader<File> {
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

    /// Seal the builder and return the read options
    pub fn build(self) -> ReadOptions {
        let props = self
            .props
            .unwrap_or_else(|| ReaderProperties::builder().build());
        ReadOptions {
            predicates: self.predicates,
            enable_page_index: self.enable_page_index,
            props,
        }
    }
}

/// A collection of options for reading a Parquet file.
///
/// Currently, only predicates on row group metadata are supported.
/// All predicates will be chained using 'AND' to filter the row groups.
pub struct ReadOptions {
    predicates: Vec<ReadGroupPredicate>,
    enable_page_index: bool,
    props: ReaderProperties,
}

impl<R: 'static + ChunkReader> SerializedFileReader<R> {
    /// Creates file reader from a Parquet file.
    /// Returns error if Parquet file does not exist or is corrupt.
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
    /// Returns error if Parquet file does not exist or is corrupt.
    pub fn new_with_options(chunk_reader: R, options: ReadOptions) -> Result<Self> {
        let mut metadata_builder = ParquetMetaDataReader::new()
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

        if options.enable_page_index {
            let mut columns_indexes = vec![];
            let mut offset_indexes = vec![];

            for rg in metadata_builder.row_groups().iter() {
                let column_index = index_reader::read_columns_indexes(&chunk_reader, rg.columns())?;
                let offset_index = index_reader::read_offset_indexes(&chunk_reader, rg.columns())?;
                columns_indexes.push(column_index);
                offset_indexes.push(offset_index);
            }
            metadata_builder = metadata_builder
                .set_column_index(Some(columns_indexes))
                .set_offset_index(Some(offset_indexes));
        }

        Ok(Self {
            chunk_reader: Arc::new(chunk_reader),
            metadata: Arc::new(metadata_builder.build()),
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

    fn get_row_iter(&self, projection: Option<SchemaType>) -> Result<RowIter> {
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
                .map(|col| Sbbf::read_from_column_chunk(col, chunk_reader.clone()))
                .collect::<Result<Vec<_>>>()?
        } else {
            iter::repeat(None).take(metadata.columns().len()).collect()
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

impl<'a, R: 'static + ChunkReader> RowGroupReader for SerializedRowGroupReader<'a, R> {
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
            self.metadata.num_rows() as usize,
            page_locations,
            props,
        )?))
    }

    /// get bloom filter for the `i`th column
    fn get_column_bloom_filter(&self, i: usize) -> Option<&Sbbf> {
        self.bloom_filters[i].as_ref()
    }

    fn get_row_iter(&self, projection: Option<SchemaType>) -> Result<RowIter> {
        RowIter::from_row_group(projection, self)
    }
}

/// Reads a [`PageHeader`] from the provided [`Read`]
pub(crate) fn read_page_header<T: Read>(input: &mut T) -> Result<PageHeader> {
    let mut prot = TCompactInputProtocol::new(input);
    let page_header = PageHeader::read_from_in_protocol(&mut prot)?;
    Ok(page_header)
}

/// Reads a [`PageHeader`] from the provided [`Read`] returning the number of bytes read
fn read_page_header_len<T: Read>(input: &mut T) -> Result<(usize, PageHeader)> {
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
    let header = read_page_header(&mut tracked)?;
    Ok((tracked.bytes_read, header))
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
        offset = (header_v2.definition_levels_byte_length + header_v2.repetition_levels_byte_length)
            as usize;
        // When is_compressed flag is missing the page is considered compressed
        can_decompress = header_v2.is_compressed.unwrap_or(true);
    }

    // TODO: page header could be huge because of statistics. We should set a
    // maximum page header size and abort if that is exceeded.
    let buffer = match decompressor {
        Some(decompressor) if can_decompress => {
            let uncompressed_size = page_header.uncompressed_page_size as usize;
            let mut decompressed = Vec::with_capacity(uncompressed_size);
            let compressed = &buffer.as_ref()[offset..];
            decompressed.extend_from_slice(&buffer.as_ref()[..offset]);
            decompressor.decompress(
                compressed,
                &mut decompressed,
                Some(uncompressed_size - offset),
            )?;

            if decompressed.len() != uncompressed_size {
                return Err(general_err!(
                    "Actual decompressed size doesn't match the expected one ({} vs {})",
                    decompressed.len(),
                    uncompressed_size
                ));
            }

            Bytes::from(decompressed)
        }
        _ => buffer,
    };

    let result = match page_header.type_ {
        PageType::DICTIONARY_PAGE => {
            let dict_header = page_header.dictionary_page_header.as_ref().ok_or_else(|| {
                ParquetError::General("Missing dictionary page header".to_string())
            })?;
            let is_sorted = dict_header.is_sorted.unwrap_or(false);
            Page::DictionaryPage {
                buf: buffer,
                num_values: dict_header.num_values as u32,
                encoding: Encoding::try_from(dict_header.encoding)?,
                is_sorted,
            }
        }
        PageType::DATA_PAGE => {
            let header = page_header
                .data_page_header
                .ok_or_else(|| ParquetError::General("Missing V1 data page header".to_string()))?;
            Page::DataPage {
                buf: buffer,
                num_values: header.num_values as u32,
                encoding: Encoding::try_from(header.encoding)?,
                def_level_encoding: Encoding::try_from(header.definition_level_encoding)?,
                rep_level_encoding: Encoding::try_from(header.repetition_level_encoding)?,
                statistics: statistics::from_thrift(physical_type, header.statistics)?,
            }
        }
        PageType::DATA_PAGE_V2 => {
            let header = page_header
                .data_page_header_v2
                .ok_or_else(|| ParquetError::General("Missing V2 data page header".to_string()))?;
            let is_compressed = header.is_compressed.unwrap_or(true);
            Page::DataPageV2 {
                buf: buffer,
                num_values: header.num_values as u32,
                encoding: Encoding::try_from(header.encoding)?,
                num_nulls: header.num_nulls as u32,
                num_rows: header.num_rows as u32,
                def_levels_byte_len: header.definition_levels_byte_length as u32,
                rep_levels_byte_len: header.repetition_levels_byte_length as u32,
                is_compressed,
                statistics: statistics::from_thrift(physical_type, header.statistics)?,
            }
        }
        _ => {
            // For unknown page type (e.g., INDEX_PAGE), skip and read next.
            unimplemented!("Page type {:?} is not supported", page_header.type_)
        }
    };

    Ok(result)
}

enum SerializedPageReaderState {
    Values {
        /// The current byte offset in the reader
        offset: usize,

        /// The length of the chunk in bytes
        remaining_bytes: usize,

        // If the next page header has already been "peeked", we will cache it and it`s length here
        next_page_header: Option<Box<PageHeader>>,
    },
    Pages {
        /// Remaining page locations
        page_locations: VecDeque<PageLocation>,
        /// Remaining dictionary location if any
        dictionary_page: Option<PageLocation>,
        /// The total number of rows in this column chunk
        total_rows: usize,
    },
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
}

impl<R: ChunkReader> SerializedPageReader<R> {
    /// Creates a new serialized page reader from a chunk reader and metadata
    pub fn new(
        reader: Arc<R>,
        meta: &ColumnChunkMetaData,
        total_rows: usize,
        page_locations: Option<Vec<PageLocation>>,
    ) -> Result<Self> {
        let props = Arc::new(ReaderProperties::builder().build());
        SerializedPageReader::new_with_properties(reader, meta, total_rows, page_locations, props)
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
                }
            }
            None => SerializedPageReaderState::Values {
                offset: start as usize,
                remaining_bytes: len as usize,
                next_page_header: None,
            },
        };

        Ok(Self {
            reader,
            decompressor,
            state,
            physical_type: meta.column_type(),
        })
    }
}

impl<R: ChunkReader> Iterator for SerializedPageReader<R> {
    type Item = Result<Page>;

    fn next(&mut self) -> Option<Self::Item> {
        self.get_next_page().transpose()
    }
}

impl<R: ChunkReader> PageReader for SerializedPageReader<R> {
    fn get_next_page(&mut self) -> Result<Option<Page>> {
        loop {
            let page = match &mut self.state {
                SerializedPageReaderState::Values {
                    offset,
                    remaining_bytes: remaining,
                    next_page_header,
                } => {
                    if *remaining == 0 {
                        return Ok(None);
                    }

                    let mut read = self.reader.get_read(*offset as u64)?;
                    let header = if let Some(header) = next_page_header.take() {
                        *header
                    } else {
                        let (header_len, header) = read_page_header_len(&mut read)?;
                        *offset += header_len;
                        *remaining -= header_len;
                        header
                    };
                    let data_len = header.compressed_page_size as usize;
                    *offset += data_len;
                    *remaining -= data_len;

                    if header.type_ == PageType::INDEX_PAGE {
                        continue;
                    }

                    let mut buffer = Vec::with_capacity(data_len);
                    let read = read.take(data_len as u64).read_to_end(&mut buffer)?;

                    if read != data_len {
                        return Err(eof_err!(
                            "Expected to read {} bytes of page, read only {}",
                            data_len,
                            read
                        ));
                    }

                    decode_page(
                        header,
                        Bytes::from(buffer),
                        self.physical_type,
                        self.decompressor.as_mut(),
                    )?
                }
                SerializedPageReaderState::Pages {
                    page_locations,
                    dictionary_page,
                    ..
                } => {
                    let front = match dictionary_page
                        .take()
                        .or_else(|| page_locations.pop_front())
                    {
                        Some(front) => front,
                        None => return Ok(None),
                    };

                    let page_len = front.compressed_page_size as usize;

                    let buffer = self.reader.get_bytes(front.offset as u64, page_len)?;

                    let mut prot = TCompactSliceInputProtocol::new(buffer.as_ref());
                    let header = PageHeader::read_from_in_protocol(&mut prot)?;
                    let offset = buffer.len() - prot.as_slice().len();

                    let bytes = buffer.slice(offset..);
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
                        let mut read = self.reader.get_read(*offset as u64)?;
                        let (header_len, header) = read_page_header_len(&mut read)?;
                        *offset += header_len;
                        *remaining_bytes -= header_len;
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
            } => {
                if let Some(buffered_header) = next_page_header.take() {
                    // The next page header has already been peeked, so just advance the offset
                    *offset += buffered_header.compressed_page_size as usize;
                    *remaining_bytes -= buffered_header.compressed_page_size as usize;
                } else {
                    let mut read = self.reader.get_read(*offset as u64)?;
                    let (header_len, header) = read_page_header_len(&mut read)?;
                    let data_page_size = header.compressed_page_size as usize;
                    *offset += header_len + data_page_size;
                    *remaining_bytes -= header_len + data_page_size;
                }
                Ok(())
            }
            SerializedPageReaderState::Pages { page_locations, .. } => {
                page_locations.pop_front();

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
    use crate::format::BoundaryOrder;

    use crate::basic::{self, ColumnOrder};
    use crate::column::reader::ColumnReader;
    use crate::data_type::private::ParquetValueType;
    use crate::data_type::{AsBytes, FixedLenByteArrayType};
    use crate::file::page_index::index::{Index, NativeIndex};
    use crate::file::page_index::index_reader::{read_columns_indexes, read_offset_indexes};
    use crate::file::writer::SerializedFileWriter;
    use crate::record::RowAccessor;
    use crate::schema::parser::parse_message_type;
    use crate::util::test_common::file_util::{get_test_file, get_test_path};

    use super::*;

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
        while let Ok(Some(page)) = page_reader_0.get_next_page() {
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
        while let Ok(Some(page)) = page_reader_0.get_next_page() {
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
                    assert!(statistics.is_some());
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
        let test_file = get_test_file("alltypes_plain.parquet");
        let origin_reader = SerializedFileReader::new(test_file)?;
        let metadata = origin_reader.metadata();
        let mid = get_midpoint_offset(metadata.row_group(0));

        // true, true predicate
        let test_file = get_test_file("alltypes_plain.parquet");
        let read_options = ReadOptionsBuilder::new()
            .with_predicate(Box::new(|_, _| true))
            .with_range(mid, mid + 1)
            .build();
        let reader = SerializedFileReader::new_with_options(test_file, read_options)?;
        let metadata = reader.metadata();
        assert_eq!(metadata.num_row_groups(), 1);

        // true, false predicate
        let test_file = get_test_file("alltypes_plain.parquet");
        let read_options = ReadOptionsBuilder::new()
            .with_predicate(Box::new(|_, _| true))
            .with_range(0, mid)
            .build();
        let reader = SerializedFileReader::new_with_options(test_file, read_options)?;
        let metadata = reader.metadata();
        assert_eq!(metadata.num_row_groups(), 0);

        // false, true predicate
        let test_file = get_test_file("alltypes_plain.parquet");
        let read_options = ReadOptionsBuilder::new()
            .with_predicate(Box::new(|_, _| false))
            .with_range(mid, mid + 1)
            .build();
        let reader = SerializedFileReader::new_with_options(test_file, read_options)?;
        let metadata = reader.metadata();
        assert_eq!(metadata.num_row_groups(), 0);

        // false, false predicate
        let test_file = get_test_file("alltypes_plain.parquet");
        let read_options = ReadOptionsBuilder::new()
            .with_predicate(Box::new(|_, _| false))
            .with_range(0, mid)
            .build();
        let reader = SerializedFileReader::new_with_options(test_file, read_options)?;
        let metadata = reader.metadata();
        assert_eq!(metadata.num_row_groups(), 0);
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
            "Parquet error: Could not parse metadata: bad data"
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
        let index = if let Index::BYTE_ARRAY(index) = &column_index[0][0] {
            index
        } else {
            unreachable!()
        };

        assert_eq!(index.boundary_order, BoundaryOrder::ASCENDING);
        let index_in_pages = &index.indexes;

        //only one page group
        assert_eq!(index_in_pages.len(), 1);

        let page0 = &index_in_pages[0];
        let min = page0.min.as_ref().unwrap();
        let max = page0.max.as_ref().unwrap();
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
    fn test_page_index_reader_out_of_order() {
        let test_file = get_test_file("alltypes_tiny_pages_plain.parquet");
        let options = ReadOptionsBuilder::new().with_page_index().build();
        let reader = SerializedFileReader::new_with_options(test_file, options).unwrap();
        let metadata = reader.metadata();

        let test_file = get_test_file("alltypes_tiny_pages_plain.parquet");
        let columns = metadata.row_group(0).columns();
        let reversed: Vec<_> = columns.iter().cloned().rev().collect();

        let a = read_columns_indexes(&test_file, columns).unwrap();
        let mut b = read_columns_indexes(&test_file, &reversed).unwrap();
        b.reverse();
        assert_eq!(a, b);

        let a = read_offset_indexes(&test_file, columns).unwrap();
        let mut b = read_offset_indexes(&test_file, &reversed).unwrap();
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
        if let Index::INT32(index) = &column_index[0][0] {
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
        if let Index::BOOLEAN(index) = &column_index[0][1] {
            assert_eq!(index.indexes.len(), 82);
            assert_eq!(row_group_offset_indexes[1].page_locations.len(), 82);
        } else {
            unreachable!()
        };
        //col2->tinyint_col: INT32 UNCOMPRESSED DO:0 FPO:40351 SZ:37325/37325/1.00 VC:7300 ENC:BIT_PACKED,RLE,PLAIN ST:[min: 0, max: 9, num_nulls: 0]
        assert!(&column_index[0][2].is_sorted());
        if let Index::INT32(index) = &column_index[0][2] {
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
        if let Index::INT32(index) = &column_index[0][3] {
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
        if let Index::INT32(index) = &column_index[0][4] {
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
        if let Index::INT64(index) = &column_index[0][5] {
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
        if let Index::FLOAT(index) = &column_index[0][6] {
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
        if let Index::DOUBLE(index) = &column_index[0][7] {
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
        if let Index::BYTE_ARRAY(index) = &column_index[0][8] {
            check_native_page_index(
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
        if let Index::BYTE_ARRAY(index) = &column_index[0][9] {
            check_native_page_index(
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
        if let Index::NONE = &column_index[0][10] {
            assert_eq!(row_group_offset_indexes[10].page_locations.len(), 974);
        } else {
            unreachable!()
        };
        //col12->year: INT32 UNCOMPRESSED DO:0 FPO:602041 SZ:37325/37325/1.00 VC:7300 ENC:BIT_PACKED,RLE,PLAIN ST:[min: 2009, max: 2010, num_nulls: 0]
        assert!(&column_index[0][11].is_sorted());
        if let Index::INT32(index) = &column_index[0][11] {
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
        if let Index::INT32(index) = &column_index[0][12] {
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
        row_group_index: &NativeIndex<T>,
        page_size: usize,
        min_max: (&[u8], &[u8]),
        boundary_order: BoundaryOrder,
    ) {
        assert_eq!(row_group_index.indexes.len(), page_size);
        assert_eq!(row_group_index.boundary_order, boundary_order);
        row_group_index.indexes.iter().all(|x| {
            x.min.as_ref().unwrap() >= &T::try_from_le_slice(min_max.0).unwrap()
                && x.max.as_ref().unwrap() <= &T::try_from_le_slice(min_max.1).unwrap()
        });
    }

    fn get_row_group_min_max_bytes(r: &RowGroupMetaData, col_num: usize) -> (&[u8], &[u8]) {
        let statistics = r.column(col_num).statistics().unwrap();
        (
            statistics.min_bytes_opt().unwrap_or_default(),
            statistics.max_bytes_opt().unwrap_or_default(),
        )
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
            Index::FIXED_LEN_BYTE_ARRAY(v) => {
                assert_eq!(v.indexes.len(), 1);
                let page_idx = &v.indexes[0];
                assert_eq!(page_idx.null_count.unwrap(), 1);
                assert_eq!(page_idx.min.as_ref().unwrap().as_ref(), &[0; 11]);
                assert_eq!(page_idx.max.as_ref().unwrap().as_ref(), &[5; 11]);
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
}
