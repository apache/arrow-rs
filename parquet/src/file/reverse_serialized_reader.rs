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

//! Reverse page-order [`PageReader`] backed by [`OffsetIndexMetaData`].
//!
//! Tracking issue: <https://github.com/apache/arrow-rs/issues/9934>.
//!
//! This reader emits pages from a Parquet column chunk in **reverse page order**:
//! the dictionary page (if any) is emitted first because data pages depend on it,
//! followed by data pages from the last `PageLocation` to the first.
//!
//! Pages are still **decoded in their native forward direction** — only the page
//! traversal order is reversed. Reversing rows *within* a page is impossible
//! because Parquet's RLE / bit-packing / delta / dictionary encodings are
//! forward streams; that responsibility belongs to a higher layer.
//!
//! ## Limitations (Phase 1 POC)
//!
//! * Encryption is not supported.
//! * `peek_next_page` does not populate `num_rows`.
//! * Requires the column chunk to have an `OffsetIndex` (i.e. page index).

use std::sync::Arc;

use crate::basic::Type;
use crate::column::page::{Page, PageMetadata, PageReader};
use crate::compression::{Codec, create_codec};
use crate::errors::{ParquetError, Result};
use crate::file::metadata::ColumnChunkMetaData;
use crate::file::metadata::thrift::PageHeader;
use crate::file::page_index::offset_index::{OffsetIndexMetaData, PageLocation};
use crate::file::properties::{ReaderProperties, ReaderPropertiesPtr};
use crate::file::reader::ChunkReader;
use crate::file::serialized_reader::decode_page;
use crate::parquet_thrift::{ReadThrift, ThriftSliceInputProtocol};

/// A [`PageReader`] that emits pages in reverse order using `OffsetIndex`.
///
/// See the module-level documentation for details.
pub struct ReverseSerializedPageReader<R: ChunkReader> {
    reader: Arc<R>,
    decompressor: Option<Box<dyn Codec>>,
    physical_type: Type,
    state: ReverseState,
    read_stats: bool,
}

enum ReverseState {
    /// Initial state. The dictionary page (if any) is emitted on the next call,
    /// then the state transitions to [`ReverseState::Data`].
    NeedDict {
        dictionary_page: Option<PageLocation>,
        page_locations: Vec<PageLocation>,
    },
    /// Iterate `page_locations[..cursor]` from the back: emit
    /// `page_locations[cursor - 1]` and decrement `cursor`.
    Data {
        page_locations: Vec<PageLocation>,
        cursor: usize,
    },
    Exhausted,
}

impl<R: ChunkReader> ReverseSerializedPageReader<R> {
    /// Create a new [`ReverseSerializedPageReader`] with default
    /// [`ReaderProperties`].
    pub fn new(
        reader: Arc<R>,
        meta: &ColumnChunkMetaData,
        offset_index: &OffsetIndexMetaData,
    ) -> Result<Self> {
        let props = Arc::new(ReaderProperties::builder().build());
        Self::new_with_properties(reader, meta, offset_index, props)
    }

    /// Create a new [`ReverseSerializedPageReader`] with explicit
    /// [`ReaderProperties`].
    pub fn new_with_properties(
        reader: Arc<R>,
        meta: &ColumnChunkMetaData,
        offset_index: &OffsetIndexMetaData,
        props: ReaderPropertiesPtr,
    ) -> Result<Self> {
        let decompressor = create_codec(meta.compression(), props.codec_options())?;
        let (chunk_start, _chunk_len) = meta.byte_range();
        let locations = offset_index.page_locations().clone();

        // If the first data page does not start at the column chunk's start, a
        // dictionary page sits in front. Synthesize a `PageLocation` for it
        // (mirrors `SerializedPageReader::new_with_properties`).
        let dictionary_page = match locations.first() {
            Some(first) if (first.offset as u64) != chunk_start => Some(PageLocation {
                offset: chunk_start as i64,
                compressed_page_size: (first.offset as u64 - chunk_start) as i32,
                first_row_index: 0,
            }),
            _ => None,
        };

        Ok(Self {
            reader,
            decompressor,
            physical_type: meta.column_type(),
            state: ReverseState::NeedDict {
                dictionary_page,
                page_locations: locations,
            },
            read_stats: props.read_page_stats(),
        })
    }

    fn read_page_at(&mut self, loc: &PageLocation) -> Result<Page> {
        let page_len = usize::try_from(loc.compressed_page_size).map_err(|e| {
            ParquetError::General(format!("invalid compressed_page_size: {e}"))
        })?;
        let buffer = self.reader.get_bytes(loc.offset as u64, page_len)?;

        let mut prot = ThriftSliceInputProtocol::new(buffer.as_ref());
        let header = if self.read_stats {
            PageHeader::read_thrift(&mut prot)?
        } else {
            PageHeader::read_thrift_without_stats(&mut prot)?
        };
        let header_len = buffer.len() - prot.as_slice().len();
        let payload = buffer.slice(header_len..);

        decode_page(
            header,
            payload,
            self.physical_type,
            self.decompressor.as_mut(),
        )
    }
}

impl<R: ChunkReader> Iterator for ReverseSerializedPageReader<R> {
    type Item = Result<Page>;

    fn next(&mut self) -> Option<Self::Item> {
        self.get_next_page().transpose()
    }
}

impl<R: ChunkReader> PageReader for ReverseSerializedPageReader<R> {
    fn get_next_page(&mut self) -> Result<Option<Page>> {
        loop {
            match std::mem::replace(&mut self.state, ReverseState::Exhausted) {
                ReverseState::NeedDict {
                    dictionary_page,
                    page_locations,
                } => {
                    let cursor = page_locations.len();
                    self.state = ReverseState::Data {
                        page_locations,
                        cursor,
                    };
                    if let Some(loc) = dictionary_page {
                        return self.read_page_at(&loc).map(Some);
                    }
                    // No dictionary; fall through to read the first reverse data page.
                }
                ReverseState::Data {
                    page_locations,
                    cursor,
                } => {
                    if cursor == 0 {
                        return Ok(None);
                    }
                    let loc = page_locations[cursor - 1].clone();
                    self.state = ReverseState::Data {
                        page_locations,
                        cursor: cursor - 1,
                    };
                    return self.read_page_at(&loc).map(Some);
                }
                ReverseState::Exhausted => return Ok(None),
            }
        }
    }

    fn peek_next_page(&mut self) -> Result<Option<PageMetadata>> {
        match &self.state {
            ReverseState::NeedDict {
                dictionary_page, ..
            } => Ok(Some(PageMetadata {
                num_rows: None,
                num_levels: None,
                is_dict: dictionary_page.is_some(),
            })),
            ReverseState::Data { cursor, .. } => {
                if *cursor == 0 {
                    Ok(None)
                } else {
                    // num_rows precise computation requires `total_rows`, which
                    // is not currently threaded through. Phase 1 returns None.
                    Ok(Some(PageMetadata {
                        num_rows: None,
                        num_levels: None,
                        is_dict: false,
                    }))
                }
            }
            ReverseState::Exhausted => Ok(None),
        }
    }

    fn skip_next_page(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, ReverseState::Exhausted) {
            ReverseState::NeedDict { page_locations, .. } => {
                let cursor = page_locations.len();
                self.state = ReverseState::Data {
                    page_locations,
                    cursor,
                };
                Ok(())
            }
            ReverseState::Data {
                page_locations,
                cursor,
            } => {
                if cursor > 0 {
                    self.state = ReverseState::Data {
                        page_locations,
                        cursor: cursor - 1,
                    };
                }
                Ok(())
            }
            ReverseState::Exhausted => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::basic::Compression;
    use crate::column::page::Page;
    use crate::data_type::Int32Type;
    use crate::file::metadata::{PageIndexPolicy, ParquetMetaDataReader};
    use crate::file::page_index::offset_index::OffsetIndexMetaData;
    use crate::file::properties::WriterProperties;
    use crate::file::reader::SerializedPageReader;
    use crate::file::writer::SerializedFileWriter;
    use crate::schema::parser::parse_message_type;
    use bytes::Bytes;
    use std::sync::Arc;

    /// Write a single-column INT32 parquet file to memory with a small data
    /// page size, so we get multiple data pages per row group. Returns the
    /// file bytes and the values written.
    fn build_test_file(
        num_values: usize,
        compression: Compression,
        enable_dict: bool,
    ) -> (Bytes, Vec<i32>) {
        let message_type = "
            message schema {
                REQUIRED INT32 value;
            }
        ";
        let schema = Arc::new(parse_message_type(message_type).unwrap());

        let props = Arc::new(
            WriterProperties::builder()
                .set_compression(compression)
                .set_dictionary_enabled(enable_dict)
                .set_data_page_row_count_limit(64)
                .set_data_page_size_limit(256)
                .build(),
        );

        let values: Vec<i32> = (0..num_values as i32).collect();

        let mut buf: Vec<u8> = Vec::new();
        {
            let mut writer = SerializedFileWriter::new(&mut buf, schema, props).unwrap();
            let mut row_group_writer = writer.next_row_group().unwrap();
            let mut col_writer = row_group_writer.next_column().unwrap().unwrap();
            col_writer
                .typed::<Int32Type>()
                .write_batch(&values, None, None)
                .unwrap();
            col_writer.close().unwrap();
            row_group_writer.close().unwrap();
            writer.close().unwrap();
        }
        (Bytes::from(buf), values)
    }

    /// Collect all data pages from a reader, return them in the order the
    /// reader emits them.
    fn collect_data_pages(reader: &mut dyn PageReader) -> Vec<Page> {
        let mut out = Vec::new();
        while let Some(page) = reader.get_next_page().unwrap() {
            if !page.is_dictionary_page() {
                out.push(page);
            }
        }
        out
    }

    fn open_with_offset_index(bytes: Bytes) -> (Bytes, Arc<crate::file::metadata::ParquetMetaData>) {
        let mut metadata_reader = ParquetMetaDataReader::new()
            .with_page_index_policy(PageIndexPolicy::Required);
        metadata_reader.try_parse(&bytes).unwrap();
        let metadata = Arc::new(metadata_reader.finish().unwrap());
        (bytes, metadata)
    }

    fn run_forward_reverse_match(num_values: usize, compression: Compression, with_dict: bool) {
        let (bytes, _values) = build_test_file(num_values, compression, with_dict);
        let (bytes, metadata) = open_with_offset_index(bytes);

        let chunk_reader: Arc<Bytes> = Arc::new(bytes);
        let rg = metadata.row_group(0);
        let column_chunk = rg.column(0);
        let total_rows = rg.num_rows() as usize;

        let offset_index_per_rg = metadata
            .offset_index()
            .expect("offset index must be present");
        let offset_index: &OffsetIndexMetaData = &offset_index_per_rg[0][0];
        let page_locations = offset_index.page_locations().clone();

        // Forward reader: SerializedPageReader (Pages mode).
        let mut forward = SerializedPageReader::new(
            chunk_reader.clone(),
            column_chunk,
            total_rows,
            Some(page_locations.clone()),
        )
        .unwrap();
        let forward_pages = collect_data_pages(&mut forward);

        // Reverse reader.
        let mut reverse = ReverseSerializedPageReader::new(
            chunk_reader.clone(),
            column_chunk,
            offset_index,
        )
        .unwrap();
        // Drop the dictionary page first if there is one.
        let first = reverse.get_next_page().unwrap().unwrap();
        let reverse_pages: Vec<Page> = if first.is_dictionary_page() {
            collect_data_pages(&mut reverse)
        } else {
            let mut v = vec![first];
            v.extend(collect_data_pages(&mut reverse));
            v
        };

        assert_eq!(
            forward_pages.len(),
            reverse_pages.len(),
            "page count mismatch (forward={}, reverse={})",
            forward_pages.len(),
            reverse_pages.len()
        );
        assert!(
            forward_pages.len() > 1,
            "test setup should produce more than one data page; got {}",
            forward_pages.len()
        );

        for (i, (fwd, rev)) in forward_pages
            .iter()
            .zip(reverse_pages.iter().rev())
            .enumerate()
        {
            assert_eq!(
                fwd.buffer().as_ref(),
                rev.buffer().as_ref(),
                "page {i} buffer mismatch"
            );
            assert_eq!(fwd.num_values(), rev.num_values(), "page {i} num_values");
            assert_eq!(fwd.encoding(), rev.encoding(), "page {i} encoding");
        }
    }

    #[test]
    fn reverse_pages_match_forward_uncompressed_no_dict() {
        run_forward_reverse_match(2_000, Compression::UNCOMPRESSED, false);
    }

    #[test]
    fn reverse_pages_match_forward_uncompressed_with_dict() {
        run_forward_reverse_match(2_000, Compression::UNCOMPRESSED, true);
    }

    #[test]
    fn reverse_pages_match_forward_snappy_with_dict() {
        run_forward_reverse_match(2_000, Compression::SNAPPY, true);
    }

    #[test]
    fn iterator_impl_works() {
        let (bytes, _values) = build_test_file(2_000, Compression::UNCOMPRESSED, false);
        let (bytes, metadata) = open_with_offset_index(bytes);
        let chunk_reader: Arc<Bytes> = Arc::new(bytes);
        let rg = metadata.row_group(0);
        let column_chunk = rg.column(0);
        let offset_index = &metadata.offset_index().unwrap()[0][0];

        let reverse =
            ReverseSerializedPageReader::new(chunk_reader, column_chunk, offset_index).unwrap();

        let pages: Vec<Page> = reverse.collect::<Result<Vec<_>>>().unwrap();
        assert!(pages.len() > 1);
    }
}
