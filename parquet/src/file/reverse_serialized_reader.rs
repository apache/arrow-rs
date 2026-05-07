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
///
/// # Stability
///
/// This type is part of an experimental API surface (see #9934) and is
/// subject to change without a major-version bump. It is currently
/// `#[doc(hidden)]` for that reason.
#[doc(hidden)]
pub struct ReverseSerializedPageReader<R: ChunkReader> {
    reader: Arc<R>,
    decompressor: Option<Box<dyn Codec>>,
    physical_type: Type,
    /// Data page locations, in forward order. Iteration walks this slice
    /// from the back via `state.cursor`.
    page_locations: Vec<PageLocation>,
    /// Synthesized dictionary-page location, if the column chunk has one.
    dictionary_page: Option<PageLocation>,
    state: ReverseState,
    read_stats: bool,
}

#[derive(Debug, Clone, Copy)]
enum ReverseState {
    /// Initial state. The dictionary page (if any) is emitted on the next
    /// `get_next_page` call, then the state transitions to `Data`.
    NeedDict,
    /// Emit `page_locations[cursor - 1]` next, then decrement `cursor`.
    Data {
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
        let page_locations = offset_index.page_locations().clone();

        // If the first data page does not start at the column chunk's start, a
        // dictionary page sits in front. Synthesize a `PageLocation` for it
        // (mirrors `SerializedPageReader::new_with_properties`).
        let dictionary_page = match page_locations.first() {
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
            page_locations,
            dictionary_page,
            state: ReverseState::NeedDict,
            read_stats: props.read_page_stats(),
        })
    }

    fn read_page_at(&mut self, loc: &PageLocation) -> Result<Page> {
        let page_len = usize::try_from(loc.compressed_page_size).map_err(|e| {
            ParquetError::General(format!(
                "invalid compressed_page_size {} at offset {}: {e}",
                loc.compressed_page_size, loc.offset
            ))
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
            match self.state {
                ReverseState::NeedDict => {
                    if let Some(loc) = self.dictionary_page.clone() {
                        // Read first; only commit the state transition on success.
                        let page = self.read_page_at(&loc)?;
                        self.state = ReverseState::Data {
                            cursor: self.page_locations.len(),
                        };
                        return Ok(Some(page));
                    }
                    // No dictionary; transition to Data and loop to emit the
                    // back-most data page (or terminate if there are none).
                    self.state = ReverseState::Data {
                        cursor: self.page_locations.len(),
                    };
                }
                ReverseState::Data { cursor } => {
                    if cursor == 0 {
                        self.state = ReverseState::Exhausted;
                        return Ok(None);
                    }
                    let loc = self.page_locations[cursor - 1].clone();
                    // Read first; only commit the cursor decrement on success
                    // so a transient I/O error does not silently skip a page.
                    let page = self.read_page_at(&loc)?;
                    self.state = ReverseState::Data { cursor: cursor - 1 };
                    return Ok(Some(page));
                }
                ReverseState::Exhausted => return Ok(None),
            }
        }
    }

    fn peek_next_page(&mut self) -> Result<Option<PageMetadata>> {
        match self.state {
            ReverseState::NeedDict => {
                if self.dictionary_page.is_some() {
                    Ok(Some(PageMetadata {
                        num_rows: None,
                        num_levels: None,
                        is_dict: true,
                    }))
                } else if !self.page_locations.is_empty() {
                    // num_rows precise computation requires `total_rows`,
                    // which is not currently threaded through; Phase 1
                    // returns None.
                    Ok(Some(PageMetadata {
                        num_rows: None,
                        num_levels: None,
                        is_dict: false,
                    }))
                } else {
                    // Empty column chunk with no dictionary.
                    Ok(None)
                }
            }
            ReverseState::Data { cursor } => {
                if cursor == 0 {
                    Ok(None)
                } else {
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
        match self.state {
            ReverseState::NeedDict => {
                // If a dictionary page is pending, skipping consumes only
                // the dictionary slot (no data page is dropped) — matching
                // `SerializedPageReader::skip_next_page` in `Pages` mode.
                // If there is no dictionary, skipping drops the back-most
                // data page.
                let mut cursor = self.page_locations.len();
                if self.dictionary_page.is_none() {
                    cursor = cursor.saturating_sub(1);
                }
                self.state = ReverseState::Data { cursor };
                Ok(())
            }
            ReverseState::Data { cursor } => {
                if cursor > 0 {
                    self.state = ReverseState::Data { cursor: cursor - 1 };
                } else {
                    self.state = ReverseState::Exhausted;
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
    use crate::basic::{Compression, Repetition, Type as PhysicalType};
    use crate::column::page::Page;
    use crate::column::reader::ColumnReaderImpl;
    use crate::data_type::{ByteArray, ByteArrayType, DataType, Int32Type, Int64Type};
    use crate::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};
    use crate::file::page_index::offset_index::OffsetIndexMetaData;
    use crate::file::properties::{WriterProperties, WriterVersion};
    use crate::file::reader::SerializedPageReader;
    use crate::file::writer::SerializedFileWriter;
    use crate::schema::types::Type;
    use bytes::Bytes;
    use std::sync::Arc;

    // -------------------------------------------------------------------- //
    // Test fixtures
    // -------------------------------------------------------------------- //

    /// Configuration for building a test parquet file.
    #[derive(Clone)]
    struct TestFileSpec {
        compression: Compression,
        enable_dict: bool,
        writer_version: WriterVersion,
        data_page_row_count_limit: usize,
        data_page_size_limit: usize,
    }

    impl Default for TestFileSpec {
        fn default() -> Self {
            Self {
                compression: Compression::UNCOMPRESSED,
                enable_dict: false,
                writer_version: WriterVersion::PARQUET_1_0,
                data_page_row_count_limit: 64,
                data_page_size_limit: 256,
            }
        }
    }

    impl TestFileSpec {
        fn build_props(&self) -> Arc<WriterProperties> {
            Arc::new(
                WriterProperties::builder()
                    .set_compression(self.compression)
                    .set_dictionary_enabled(self.enable_dict)
                    .set_writer_version(self.writer_version)
                    .set_data_page_row_count_limit(self.data_page_row_count_limit)
                    .set_data_page_size_limit(self.data_page_size_limit)
                    .build(),
            )
        }
    }

    fn schema_of(physical: PhysicalType, optional: bool) -> Arc<Type> {
        let repetition = if optional {
            Repetition::OPTIONAL
        } else {
            Repetition::REQUIRED
        };
        let leaf = Type::primitive_type_builder("value", physical)
            .with_repetition(repetition)
            .build()
            .unwrap();
        Arc::new(
            Type::group_type_builder("schema")
                .with_fields(vec![Arc::new(leaf)])
                .build()
                .unwrap(),
        )
    }

    /// Write an INT32 parquet file with the given values; returns file bytes.
    fn write_int32_file(spec: &TestFileSpec, values: &[i32], def_levels: Option<&[i16]>) -> Bytes {
        let schema = schema_of(PhysicalType::INT32, def_levels.is_some());
        let props = spec.build_props();
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut writer = SerializedFileWriter::new(&mut buf, schema, props).unwrap();
            let mut row_group_writer = writer.next_row_group().unwrap();
            let mut col_writer = row_group_writer.next_column().unwrap().unwrap();
            col_writer
                .typed::<Int32Type>()
                .write_batch(values, def_levels, None)
                .unwrap();
            col_writer.close().unwrap();
            row_group_writer.close().unwrap();
            writer.close().unwrap();
        }
        Bytes::from(buf)
    }

    fn write_int64_file(spec: &TestFileSpec, values: &[i64]) -> Bytes {
        let schema = schema_of(PhysicalType::INT64, false);
        let props = spec.build_props();
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut writer = SerializedFileWriter::new(&mut buf, schema, props).unwrap();
            let mut row_group_writer = writer.next_row_group().unwrap();
            let mut col_writer = row_group_writer.next_column().unwrap().unwrap();
            col_writer
                .typed::<Int64Type>()
                .write_batch(values, None, None)
                .unwrap();
            col_writer.close().unwrap();
            row_group_writer.close().unwrap();
            writer.close().unwrap();
        }
        Bytes::from(buf)
    }

    fn write_byte_array_file(spec: &TestFileSpec, values: &[ByteArray]) -> Bytes {
        let schema = schema_of(PhysicalType::BYTE_ARRAY, false);
        let props = spec.build_props();
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut writer = SerializedFileWriter::new(&mut buf, schema, props).unwrap();
            let mut row_group_writer = writer.next_row_group().unwrap();
            let mut col_writer = row_group_writer.next_column().unwrap().unwrap();
            col_writer
                .typed::<ByteArrayType>()
                .write_batch(values, None, None)
                .unwrap();
            col_writer.close().unwrap();
            row_group_writer.close().unwrap();
            writer.close().unwrap();
        }
        Bytes::from(buf)
    }

    fn open_with_offset_index(bytes: Bytes) -> (Bytes, Arc<ParquetMetaData>) {
        let mut metadata_reader =
            ParquetMetaDataReader::new().with_page_index_policy(PageIndexPolicy::Required);
        metadata_reader.try_parse(&bytes).unwrap();
        let metadata = Arc::new(metadata_reader.finish().unwrap());
        (bytes, metadata)
    }

    /// Per-page row counts derived from `OffsetIndex` + total rows.
    fn page_row_counts(offset_index: &OffsetIndexMetaData, total_rows: usize) -> Vec<usize> {
        let locs = offset_index.page_locations();
        let mut counts = Vec::with_capacity(locs.len());
        for i in 0..locs.len() {
            let next = locs
                .get(i + 1)
                .map(|x| x.first_row_index as usize)
                .unwrap_or(total_rows);
            counts.push(next - locs[i].first_row_index as usize);
        }
        counts
    }

    // -------------------------------------------------------------------- //
    // Page-level (buffer / metadata) verification
    // -------------------------------------------------------------------- //

    /// Collect all data pages from a reader.
    fn collect_data_pages(reader: &mut dyn PageReader) -> Vec<Page> {
        let mut out = Vec::new();
        while let Some(page) = reader.get_next_page().unwrap() {
            if !page.is_dictionary_page() {
                out.push(page);
            }
        }
        out
    }

    /// Assert that data pages emitted by reverse reader (in reverse) equal
    /// data pages emitted by forward reader, byte-for-byte.
    fn assert_pages_match_forward_reverse(bytes: Bytes) {
        let (bytes, metadata) = open_with_offset_index(bytes);
        let chunk_reader: Arc<Bytes> = Arc::new(bytes);

        let rg = metadata.row_group(0);
        let column_chunk = rg.column(0);
        let total_rows = rg.num_rows() as usize;
        let offset_index = &metadata.offset_index().unwrap()[0][0];
        let page_locations = offset_index.page_locations().clone();

        let mut forward = SerializedPageReader::new(
            chunk_reader.clone(),
            column_chunk,
            total_rows,
            Some(page_locations),
        )
        .unwrap();
        let forward_pages = collect_data_pages(&mut forward);

        let mut reverse =
            ReverseSerializedPageReader::new(chunk_reader, column_chunk, offset_index).unwrap();
        let mut reverse_pages = Vec::new();
        while let Some(page) = reverse.get_next_page().unwrap() {
            if !page.is_dictionary_page() {
                reverse_pages.push(page);
            }
        }

        assert_eq!(
            forward_pages.len(),
            reverse_pages.len(),
            "page count mismatch"
        );
        assert!(
            forward_pages.len() > 1,
            "expected >1 data page, got {}",
            forward_pages.len()
        );

        for (i, (f, r)) in forward_pages
            .iter()
            .zip(reverse_pages.iter().rev())
            .enumerate()
        {
            assert_eq!(f.buffer().as_ref(), r.buffer().as_ref(), "page {i} buffer");
            assert_eq!(f.num_values(), r.num_values(), "page {i} num_values");
            assert_eq!(f.encoding(), r.encoding(), "page {i} encoding");
        }
    }

    // -------------------------------------------------------------------- //
    // Value-level verification (the strong correctness guarantee)
    // -------------------------------------------------------------------- //

    /// Decode all values from a column chunk one page at a time, returning a
    /// `Vec<Vec<T>>` where each inner vec holds the values of a single page,
    /// in page-emission order.
    ///
    /// `page_row_counts` is the expected row count per page in the order the
    /// reader will emit them.
    fn decode_values_per_page<T: DataType>(
        page_reader: Box<dyn PageReader>,
        column_descr: crate::schema::types::ColumnDescPtr,
        page_row_counts: &[usize],
    ) -> Vec<Vec<T::T>>
    where
        T::T: Default + Clone,
    {
        let mut col_reader: ColumnReaderImpl<T> = ColumnReaderImpl::new(column_descr, page_reader);
        let mut chunks = Vec::with_capacity(page_row_counts.len());
        for &want in page_row_counts {
            // The column-value decoder *appends* to the output buffer, so it
            // must start empty. Pre-allocating with `vec![default; want]`
            // would keep the zero-valued prefix and discard the real values.
            let mut values: Vec<T::T> = Vec::with_capacity(want);
            let (records, _levels, _vals) = col_reader
                .read_records(want, None, None, &mut values)
                .unwrap();
            assert_eq!(records, want, "expected {want} records, got {records}");
            chunks.push(values);
        }
        chunks
    }

    /// Decode the entire column chunk in a single call, returning every value
    /// and (for OPTIONAL columns) every def_level. Useful when per-page row
    /// boundaries cannot be precomputed reliably (e.g. across writer versions).
    fn decode_all<T: DataType>(
        page_reader: Box<dyn PageReader>,
        column_descr: crate::schema::types::ColumnDescPtr,
        total_rows: usize,
        capture_defs: bool,
    ) -> (Vec<T::T>, Vec<i16>)
    where
        T::T: Default + Clone,
    {
        let mut col_reader: ColumnReaderImpl<T> = ColumnReaderImpl::new(column_descr, page_reader);
        let mut values: Vec<T::T> = Vec::new();
        let mut defs: Vec<i16> = Vec::new();
        let mut total_records = 0usize;
        while total_records < total_rows {
            let want = total_rows - total_records;
            let (records, _levels, _vals) = if capture_defs {
                col_reader
                    .read_records(want, Some(&mut defs), None, &mut values)
                    .unwrap()
            } else {
                col_reader
                    .read_records(want, None, None, &mut values)
                    .unwrap()
            };
            if records == 0 {
                break;
            }
            total_records += records;
        }
        assert_eq!(
            total_records, total_rows,
            "did not fully drain column chunk"
        );
        (values, defs)
    }

    fn run_value_decode_match<T: DataType>(bytes: Bytes)
    where
        T::T: Default + Clone + std::fmt::Debug + PartialEq,
    {
        let (bytes, metadata) = open_with_offset_index(bytes);
        let chunk_reader: Arc<Bytes> = Arc::new(bytes);

        let rg = metadata.row_group(0);
        let column_chunk = rg.column(0);
        let total_rows = rg.num_rows() as usize;
        let offset_index = &metadata.offset_index().unwrap()[0][0];
        let page_locations = offset_index.page_locations().clone();
        let column_descr = metadata.file_metadata().schema_descr().column(0);

        let counts = page_row_counts(offset_index, total_rows);
        let counts_reversed: Vec<usize> = counts.iter().rev().cloned().collect();

        // Forward.
        let forward = Box::new(
            SerializedPageReader::new(
                chunk_reader.clone(),
                column_chunk,
                total_rows,
                Some(page_locations),
            )
            .unwrap(),
        );
        let forward_chunks = decode_values_per_page::<T>(forward, column_descr.clone(), &counts);

        // Reverse.
        let reverse =
            ReverseSerializedPageReader::new(chunk_reader, column_chunk, offset_index).unwrap();
        let reverse_chunks =
            decode_values_per_page::<T>(Box::new(reverse), column_descr, &counts_reversed);

        // Reverse the chunk order of `reverse_chunks` and compare to forward.
        let normalized: Vec<Vec<T::T>> = reverse_chunks.into_iter().rev().collect();
        assert_eq!(
            normalized, forward_chunks,
            "decoded values mismatch between forward and reverse readers"
        );
    }

    // -------------------------------------------------------------------- //
    // Tests: page-level buffer / metadata
    // -------------------------------------------------------------------- //

    #[test]
    fn pages_match_uncompressed_no_dict() {
        let spec = TestFileSpec::default();
        let values: Vec<i32> = (0..2_000).collect();
        assert_pages_match_forward_reverse(write_int32_file(&spec, &values, None));
    }

    #[test]
    fn pages_match_uncompressed_with_dict() {
        let spec = TestFileSpec {
            enable_dict: true,
            ..TestFileSpec::default()
        };
        let values: Vec<i32> = (0..2_000).collect();
        assert_pages_match_forward_reverse(write_int32_file(&spec, &values, None));
    }

    #[test]
    fn pages_match_snappy() {
        let spec = TestFileSpec {
            compression: Compression::SNAPPY,
            enable_dict: true,
            ..TestFileSpec::default()
        };
        let values: Vec<i32> = (0..2_000).collect();
        assert_pages_match_forward_reverse(write_int32_file(&spec, &values, None));
    }

    #[test]
    fn pages_match_gzip() {
        let spec = TestFileSpec {
            compression: Compression::GZIP(Default::default()),
            enable_dict: false,
            ..TestFileSpec::default()
        };
        let values: Vec<i32> = (0..2_000).collect();
        assert_pages_match_forward_reverse(write_int32_file(&spec, &values, None));
    }

    #[test]
    fn pages_match_zstd() {
        let spec = TestFileSpec {
            compression: Compression::ZSTD(Default::default()),
            enable_dict: false,
            ..TestFileSpec::default()
        };
        let values: Vec<i32> = (0..2_000).collect();
        assert_pages_match_forward_reverse(write_int32_file(&spec, &values, None));
    }

    #[test]
    fn pages_match_data_page_v2() {
        let spec = TestFileSpec {
            writer_version: WriterVersion::PARQUET_2_0,
            enable_dict: false,
            ..TestFileSpec::default()
        };
        let values: Vec<i32> = (0..2_000).collect();
        assert_pages_match_forward_reverse(write_int32_file(&spec, &values, None));
    }

    // -------------------------------------------------------------------- //
    // Tests: value-level decode equivalence
    // -------------------------------------------------------------------- //

    #[test]
    fn decode_values_match_int32() {
        let spec = TestFileSpec::default();
        let values: Vec<i32> = (0..2_000).collect();
        run_value_decode_match::<Int32Type>(write_int32_file(&spec, &values, None));
    }

    #[test]
    fn decode_values_match_int32_with_dict() {
        let spec = TestFileSpec {
            enable_dict: true,
            ..TestFileSpec::default()
        };
        // Use a small value range so dict encoding actually engages.
        let values: Vec<i32> = (0..2_000).map(|i| i % 50).collect();
        run_value_decode_match::<Int32Type>(write_int32_file(&spec, &values, None));
    }

    #[test]
    fn decode_values_match_int32_data_page_v2() {
        let spec = TestFileSpec {
            writer_version: WriterVersion::PARQUET_2_0,
            ..TestFileSpec::default()
        };
        let values: Vec<i32> = (0..2_000).collect();
        run_value_decode_match::<Int32Type>(write_int32_file(&spec, &values, None));
    }

    #[test]
    fn decode_values_match_int64() {
        let spec = TestFileSpec::default();
        let values: Vec<i64> = (0..2_000).map(|i| (i as i64) * 7919).collect();
        run_value_decode_match::<Int64Type>(write_int64_file(&spec, &values));
    }

    #[test]
    fn decode_values_match_byte_array() {
        let spec = TestFileSpec {
            data_page_size_limit: 1024,
            ..TestFileSpec::default()
        };
        let values: Vec<ByteArray> = (0..1_000)
            .map(|i| ByteArray::from(format!("string-value-{i:05}").as_str()))
            .collect();
        run_value_decode_match::<ByteArrayType>(write_byte_array_file(&spec, &values));
    }

    #[test]
    fn decode_values_match_byte_array_with_dict() {
        let spec = TestFileSpec {
            enable_dict: true,
            data_page_size_limit: 1024,
            ..TestFileSpec::default()
        };
        // Repeated strings → dict encoding.
        let values: Vec<ByteArray> = (0..2_000)
            .map(|i| ByteArray::from(format!("v{}", i % 20).as_str()))
            .collect();
        run_value_decode_match::<ByteArrayType>(write_byte_array_file(&spec, &values));
    }

    // -------------------------------------------------------------------- //
    // Tests: NULLable column
    // -------------------------------------------------------------------- //

    /// Decode the entire chunk forward and reverse, slicing the reverse output
    /// at page boundaries derived from `OffsetIndex`, then reversing the page
    /// slices to recover forward-equivalent order. Forward and re-arranged
    /// reverse streams must match exactly.
    fn run_value_decode_match_with_nulls(bytes: Bytes) {
        let (bytes, metadata) = open_with_offset_index(bytes);
        let chunk_reader: Arc<Bytes> = Arc::new(bytes);
        let rg = metadata.row_group(0);
        let column_chunk = rg.column(0);
        let total_rows = rg.num_rows() as usize;
        let offset_index = &metadata.offset_index().unwrap()[0][0];
        let page_locations = offset_index.page_locations().clone();
        let column_descr = metadata.file_metadata().schema_descr().column(0);
        let counts = page_row_counts(offset_index, total_rows);
        assert!(counts.len() > 1, "test must produce multiple pages");

        let forward = Box::new(
            SerializedPageReader::new(
                chunk_reader.clone(),
                column_chunk,
                total_rows,
                Some(page_locations),
            )
            .unwrap(),
        );
        let (fwd_values, fwd_defs) =
            decode_all::<Int32Type>(forward, column_descr.clone(), total_rows, true);

        let reverse =
            ReverseSerializedPageReader::new(chunk_reader, column_chunk, offset_index).unwrap();
        let (rev_values, rev_defs) =
            decode_all::<Int32Type>(Box::new(reverse), column_descr, total_rows, true);

        assert_eq!(fwd_defs.len(), total_rows);
        assert_eq!(rev_defs.len(), total_rows);

        // Slice each stream at page boundaries.
        // Forward emits pages 0..N (counts[0], counts[1], ..., counts[N-1]).
        let fwd_def_pages: Vec<Vec<i16>> = slice_in_order(&fwd_defs, &counts);
        // Reverse emits pages N-1..=0 (counts[N-1], counts[N-2], ..., counts[0]).
        let counts_rev_emit: Vec<usize> = counts.iter().rev().copied().collect();
        let rev_def_pages_emit_order: Vec<Vec<i16>> = slice_in_order(&rev_defs, &counts_rev_emit);
        let rev_def_pages_page_order: Vec<Vec<i16>> =
            rev_def_pages_emit_order.iter().rev().cloned().collect();
        assert_eq!(
            fwd_def_pages, rev_def_pages_page_order,
            "per-page def_levels diverged"
        );

        // For values, count non-nulls per page from the def slices.
        let fwd_val_counts: Vec<usize> = fwd_def_pages
            .iter()
            .map(|s| s.iter().filter(|&&d| d == 1).count())
            .collect();
        let rev_val_counts_emit: Vec<usize> = rev_def_pages_emit_order
            .iter()
            .map(|s| s.iter().filter(|&&d| d == 1).count())
            .collect();
        let fwd_val_pages: Vec<Vec<i32>> = slice_in_order(&fwd_values, &fwd_val_counts);
        let rev_val_pages_emit: Vec<Vec<i32>> = slice_in_order(&rev_values, &rev_val_counts_emit);
        let rev_val_pages_page_order: Vec<Vec<i32>> =
            rev_val_pages_emit.iter().rev().cloned().collect();
        assert_eq!(
            fwd_val_pages, rev_val_pages_page_order,
            "per-page values diverged"
        );
    }

    fn slice_in_order<T: Clone>(input: &[T], counts: &[usize]) -> Vec<Vec<T>> {
        let mut out = Vec::with_capacity(counts.len());
        let mut off = 0;
        for &c in counts {
            out.push(input[off..off + c].to_vec());
            off += c;
        }
        out
    }

    #[test]
    fn decode_values_match_with_nulls_v1() {
        let spec = TestFileSpec {
            enable_dict: true,
            data_page_row_count_limit: 128,
            data_page_size_limit: 4096,
            ..TestFileSpec::default()
        };
        let total = 2_000usize;
        let mut values: Vec<i32> = Vec::new();
        let mut defs: Vec<i16> = Vec::with_capacity(total);
        for i in 0..total {
            if i % 3 == 0 {
                defs.push(0);
            } else {
                defs.push(1);
                values.push(i as i32);
            }
        }
        run_value_decode_match_with_nulls(write_int32_file(&spec, &values, Some(&defs)));
    }

    #[test]
    fn decode_values_match_with_nulls_v2() {
        let spec = TestFileSpec {
            writer_version: WriterVersion::PARQUET_2_0,
            enable_dict: true,
            data_page_row_count_limit: 128,
            data_page_size_limit: 4096,
            ..TestFileSpec::default()
        };
        let total = 2_000usize;
        let mut values: Vec<i32> = Vec::new();
        let mut defs: Vec<i16> = Vec::with_capacity(total);
        for i in 0..total {
            if i % 4 == 0 {
                defs.push(0);
            } else {
                defs.push(1);
                values.push(i as i32);
            }
        }
        run_value_decode_match_with_nulls(write_int32_file(&spec, &values, Some(&defs)));
    }

    // -------------------------------------------------------------------- //
    // Tests: state-machine API surface
    // -------------------------------------------------------------------- //

    #[test]
    fn peek_next_page_state_machine() {
        let spec = TestFileSpec {
            enable_dict: true,
            ..TestFileSpec::default()
        };
        let values: Vec<i32> = (0..1_000).map(|i| i % 50).collect();
        let bytes = write_int32_file(&spec, &values, None);
        let (bytes, metadata) = open_with_offset_index(bytes);
        let chunk_reader: Arc<Bytes> = Arc::new(bytes);
        let rg = metadata.row_group(0);
        let column_chunk = rg.column(0);
        let offset_index = &metadata.offset_index().unwrap()[0][0];
        let num_data_pages = offset_index.page_locations().len();

        let mut reverse =
            ReverseSerializedPageReader::new(chunk_reader, column_chunk, offset_index).unwrap();

        // First peek must report a dictionary page (chunk has dict enabled).
        let peek = reverse.peek_next_page().unwrap().unwrap();
        assert!(peek.is_dict, "first peek should be dictionary page");

        // Consume dict page.
        let dict = reverse.get_next_page().unwrap().unwrap();
        assert!(dict.is_dictionary_page());

        // Subsequent peeks should report non-dict pages until exhaustion.
        for i in 0..num_data_pages {
            let peek = reverse.peek_next_page().unwrap().unwrap();
            assert!(!peek.is_dict, "data page {i} unexpectedly marked is_dict");
            let page = reverse.get_next_page().unwrap().unwrap();
            assert!(
                !page.is_dictionary_page(),
                "got unexpected dict page at index {i}"
            );
        }

        // Now exhausted.
        assert!(reverse.peek_next_page().unwrap().is_none());
        assert!(reverse.get_next_page().unwrap().is_none());
    }

    #[test]
    fn skip_next_page_advances_state() {
        // Use enough data with a tight row-count limit to guarantee many pages.
        let spec = TestFileSpec {
            data_page_row_count_limit: 100,
            data_page_size_limit: 16 * 1024 * 1024,
            ..TestFileSpec::default()
        };
        let values: Vec<i32> = (0..10_000).collect();
        let bytes = write_int32_file(&spec, &values, None);
        let (bytes, metadata) = open_with_offset_index(bytes);
        let chunk_reader: Arc<Bytes> = Arc::new(bytes);
        let rg = metadata.row_group(0);
        let column_chunk = rg.column(0);
        let offset_index = &metadata.offset_index().unwrap()[0][0];
        let num_data_pages = offset_index.page_locations().len();
        assert!(
            num_data_pages >= 3,
            "expected >=3 data pages for skip test, got {num_data_pages}"
        );

        let mut reverse =
            ReverseSerializedPageReader::new(chunk_reader, column_chunk, offset_index).unwrap();

        // No dict (disabled). Skipping num_data_pages times should drain the
        // reader; the next get_next_page must return None.
        for _ in 0..num_data_pages {
            reverse.skip_next_page().unwrap();
        }
        assert!(reverse.get_next_page().unwrap().is_none());
    }

    #[test]
    fn skip_next_page_with_dict_consumes_dict_first() {
        let spec = TestFileSpec {
            enable_dict: true,
            data_page_row_count_limit: 100,
            ..TestFileSpec::default()
        };
        let values: Vec<i32> = (0..2_000).map(|i| i % 50).collect();
        let bytes = write_int32_file(&spec, &values, None);
        let (bytes, metadata) = open_with_offset_index(bytes);
        let chunk_reader: Arc<Bytes> = Arc::new(bytes);
        let rg = metadata.row_group(0);
        let column_chunk = rg.column(0);
        let offset_index = &metadata.offset_index().unwrap()[0][0];
        let num_data_pages = offset_index.page_locations().len();

        let mut reverse =
            ReverseSerializedPageReader::new(chunk_reader, column_chunk, offset_index).unwrap();

        // First skip drops the dictionary; subsequent skips drop data pages.
        reverse.skip_next_page().unwrap();
        for _ in 0..num_data_pages {
            reverse.skip_next_page().unwrap();
        }
        assert!(reverse.get_next_page().unwrap().is_none());
    }

    // -------------------------------------------------------------------- //
    // Tests: edge cases
    // -------------------------------------------------------------------- //

    #[test]
    fn single_data_page_works() {
        // Use lax page-size limits to force exactly one data page.
        let spec = TestFileSpec {
            data_page_row_count_limit: usize::MAX,
            data_page_size_limit: 16 * 1024 * 1024,
            ..TestFileSpec::default()
        };
        let values: Vec<i32> = (0..100).collect();
        let bytes = write_int32_file(&spec, &values, None);
        let (bytes, metadata) = open_with_offset_index(bytes);
        let chunk_reader: Arc<Bytes> = Arc::new(bytes);
        let rg = metadata.row_group(0);
        let column_chunk = rg.column(0);
        let offset_index = &metadata.offset_index().unwrap()[0][0];
        assert_eq!(offset_index.page_locations().len(), 1);

        let total_rows = rg.num_rows() as usize;
        let column_descr = metadata.file_metadata().schema_descr().column(0);

        let reverse =
            ReverseSerializedPageReader::new(chunk_reader, column_chunk, offset_index).unwrap();
        let chunks =
            decode_values_per_page::<Int32Type>(Box::new(reverse), column_descr, &[total_rows]);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0], values);
    }

    #[test]
    fn large_file_50k_rows() {
        let spec = TestFileSpec {
            compression: Compression::SNAPPY,
            enable_dict: true,
            ..TestFileSpec::default()
        };
        let values: Vec<i32> = (0..50_000).map(|i| i % 1024).collect();
        let bytes = write_int32_file(&spec, &values, None);

        let (bytes2, metadata) = open_with_offset_index(bytes.clone());
        let offset_index = &metadata.offset_index().unwrap()[0][0];
        // Sanity check: the test setup must produce many pages.
        assert!(
            offset_index.page_locations().len() > 20,
            "expected many pages, got {}",
            offset_index.page_locations().len()
        );

        // Reuse the byte/metadata-level test for buffer equivalence.
        assert_pages_match_forward_reverse(bytes2);
        // And the value-level decode comparison.
        run_value_decode_match::<Int32Type>(bytes);
    }

    #[test]
    fn peek_returns_none_for_empty_chunk() {
        // Write a column chunk with zero rows. The resulting OffsetIndex
        // either has no page_locations or the location count matches the
        // empty data; either way `peek_next_page` and `get_next_page` must
        // agree on emptiness.
        let spec = TestFileSpec {
            enable_dict: false,
            ..TestFileSpec::default()
        };
        let values: Vec<i32> = vec![];
        let bytes = write_int32_file(&spec, &values, None);
        let (bytes, metadata) = open_with_offset_index(bytes);
        let chunk_reader: Arc<Bytes> = Arc::new(bytes);
        let rg = metadata.row_group(0);
        let column_chunk = rg.column(0);
        let offset_index = match metadata.offset_index() {
            Some(oi) if !oi[0].is_empty() => &oi[0][0],
            // Without an OffsetIndex we cannot construct the reader; this
            // test is then a no-op for that writer configuration.
            _ => return,
        };

        let mut reverse =
            ReverseSerializedPageReader::new(chunk_reader, column_chunk, offset_index).unwrap();

        // Whatever peek says must agree with get_next_page on first call.
        let peek1 = reverse.peek_next_page().unwrap();
        let page1 = reverse.get_next_page().unwrap();
        assert_eq!(peek1.is_some(), page1.is_some());

        // Drain to exhaustion and verify peek == None and get == None.
        while reverse.get_next_page().unwrap().is_some() {}
        assert!(reverse.peek_next_page().unwrap().is_none());
        assert!(reverse.get_next_page().unwrap().is_none());
    }

    #[test]
    fn iterator_collects_all_pages() {
        let spec = TestFileSpec::default();
        let values: Vec<i32> = (0..2_000).collect();
        let bytes = write_int32_file(&spec, &values, None);
        let (bytes, metadata) = open_with_offset_index(bytes);
        let chunk_reader: Arc<Bytes> = Arc::new(bytes);
        let rg = metadata.row_group(0);
        let column_chunk = rg.column(0);
        let offset_index = &metadata.offset_index().unwrap()[0][0];
        let expected = offset_index.page_locations().len();

        let reverse =
            ReverseSerializedPageReader::new(chunk_reader, column_chunk, offset_index).unwrap();
        let pages: Vec<Page> = reverse.collect::<Result<Vec<_>>>().unwrap();
        // No dict in this spec, so total emitted == data page count.
        assert_eq!(pages.len(), expected);
    }
}
