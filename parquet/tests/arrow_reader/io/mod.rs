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

//! Tests for IO read patterns in the Parquet Reader
//!
//! Each test:
//! 1. Creates a temporary Parquet file with a known row group structure
//! 2. Reads data from that file using the Arrow Parquet Reader, recording the IO operations
//! 3. Asserts the expected IO patterns based on the read operations
//!
//! Note this module contains test infrastructure only. The actual tests are in the
//! sub-modules [`sync_reader`] and [`async_reader`].
//!
//! Key components:
//! - [`TestParquetFile`] - Represents a Parquet file and its layout
//! - [`OperationLog`] - Records IO operations performed on the file
//! - [`LogEntry`] - Represents a single IO operation in the log

mod sync_reader;

#[cfg(feature = "async")]
mod async_reader;

use arrow::compute::and;
use arrow::compute::kernels::cmp::{gt, lt};
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{ArrayRef, BooleanArray, Int64Array, RecordBatch, StringViewArray};
use bytes::Bytes;
use parquet::arrow::arrow_reader::{
    ArrowPredicateFn, ArrowReaderOptions, ParquetRecordBatchReaderBuilder, RowFilter,
};
use parquet::arrow::{ArrowWriter, ProjectionMask};
use parquet::data_type::AsBytes;
use parquet::file::FOOTER_SIZE;
use parquet::file::metadata::{FooterTail, ParquetMetaData, ParquetOffsetIndex};
use parquet::file::page_index::offset_index::PageLocation;
use parquet::file::properties::WriterProperties;
use parquet::schema::types::SchemaDescriptor;
use std::collections::BTreeMap;
use std::fmt::Display;
use std::ops::Range;
use std::sync::{Arc, LazyLock, Mutex};

/// Create a new `TestParquetFile` with:
/// 3 columns: "a", "b", "c"
///
/// 2 row groups, each with 200 rows
/// each data page has 100 rows
///
/// Values of column "a" are 0..399
/// Values of column "b" are 400..799
/// Values of column "c" are alternating strings of length 12 and longer
fn test_file() -> TestParquetFile {
    TestParquetFile::new(TEST_FILE_DATA.clone())
}

/// Default options for tests
///
/// Note these tests use the PageIndex to reduce IO
fn test_options() -> ArrowReaderOptions {
    ArrowReaderOptions::default().with_page_index(true)
}

/// Return a row filter that evaluates "b > 575" AND "b < 625"
///
/// last data page in Row Group 0 and first DataPage in Row Group 1
fn filter_b_575_625(schema_descr: &SchemaDescriptor) -> RowFilter {
    // "b" > 575 and "b" < 625
    let predicate = ArrowPredicateFn::new(
        ProjectionMask::columns(schema_descr, ["b"]),
        |batch: RecordBatch| {
            let scalar_575 = Int64Array::new_scalar(575);
            let scalar_625 = Int64Array::new_scalar(625);
            let column = batch.column(0).as_primitive::<Int64Type>();
            and(&gt(column, &scalar_575)?, &lt(column, &scalar_625)?)
        },
    );
    RowFilter::new(vec![Box::new(predicate)])
}

/// Filter a > 175 and b < 625
/// First filter: "a" > 175  (last data page in Row Group 0)
/// Second filter: "b" < 625 (last data page in Row Group 0 and first DataPage in RowGroup 1)
fn filter_a_175_b_625(schema_descr: &SchemaDescriptor) -> RowFilter {
    // "a" > 175 and "b" < 625
    let predicate_a = ArrowPredicateFn::new(
        ProjectionMask::columns(schema_descr, ["a"]),
        |batch: RecordBatch| {
            let scalar_175 = Int64Array::new_scalar(175);
            let column = batch.column(0).as_primitive::<Int64Type>();
            gt(column, &scalar_175)
        },
    );

    let predicate_b = ArrowPredicateFn::new(
        ProjectionMask::columns(schema_descr, ["b"]),
        |batch: RecordBatch| {
            let scalar_625 = Int64Array::new_scalar(625);
            let column = batch.column(0).as_primitive::<Int64Type>();
            lt(column, &scalar_625)
        },
    );

    RowFilter::new(vec![Box::new(predicate_a), Box::new(predicate_b)])
}

/// Filter FALSE (no rows) with b
/// Entirely filters out both row groups
/// Note it selects "b"
fn filter_b_false(schema_descr: &SchemaDescriptor) -> RowFilter {
    // "false"
    let predicate = ArrowPredicateFn::new(
        ProjectionMask::columns(schema_descr, ["b"]),
        |batch: RecordBatch| {
            let result =
                BooleanArray::from_iter(std::iter::repeat_n(Some(false), batch.num_rows()));
            Ok(result)
        },
    );
    RowFilter::new(vec![Box::new(predicate)])
}

/// Create a parquet file in memory for testing. See [`test_file`] for details.
static TEST_FILE_DATA: LazyLock<Bytes> = LazyLock::new(|| {
    // Input batch has 400 rows, with 3 columns: "a", "b", "c"
    // Note c is a different types (so the data page sizes will be different)
    let a: ArrayRef = Arc::new(Int64Array::from_iter_values(0..400));
    let b: ArrayRef = Arc::new(Int64Array::from_iter_values(400..800));
    let c: ArrayRef = Arc::new(StringViewArray::from_iter_values((0..400).map(|i| {
        if i % 2 == 0 {
            format!("string_{i}")
        } else {
            format!("A string larger than 12 bytes and thus not inlined {i}")
        }
    })));

    let input_batch = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

    let mut output = Vec::new();

    let writer_options = WriterProperties::builder()
        .set_max_row_group_size(200)
        .set_data_page_row_count_limit(100)
        .build();
    let mut writer =
        ArrowWriter::try_new(&mut output, input_batch.schema(), Some(writer_options)).unwrap();

    // since the limits are only enforced on batch boundaries, write the input
    // batch in chunks of 50
    let mut row_remain = input_batch.num_rows();
    while row_remain > 0 {
        let chunk_size = row_remain.min(50);
        let chunk = input_batch.slice(input_batch.num_rows() - row_remain, chunk_size);
        writer.write(&chunk).unwrap();
        row_remain -= chunk_size;
    }
    writer.close().unwrap();
    Bytes::from(output)
});

/// A test parquet file and its layout.
struct TestParquetFile {
    bytes: Bytes,
    /// The operation log for IO operations performed on this file
    ops: Arc<OperationLog>,
    /// The (pre-parsed) parquet metadata for this file
    parquet_metadata: Arc<ParquetMetaData>,
}

impl TestParquetFile {
    /// Create a new `TestParquetFile` with the specified temporary directory and path
    /// and determines the row group layout.
    fn new(bytes: Bytes) -> Self {
        // Read the parquet file to determine its layout
        let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(
            bytes.clone(),
            ArrowReaderOptions::default().with_page_index(true),
        )
        .unwrap();

        let parquet_metadata = Arc::clone(builder.metadata());

        let offset_index = parquet_metadata
            .offset_index()
            .expect("Parquet metadata should have a page index");

        let row_groups = TestRowGroups::new(&parquet_metadata, offset_index);

        // figure out the footer location in the file
        let footer_location = bytes.len() - FOOTER_SIZE..bytes.len();
        let footer = bytes.slice(footer_location.clone());
        let footer: &[u8; FOOTER_SIZE] = footer
            .as_bytes()
            .try_into() // convert to a fixed size array
            .unwrap();

        // figure out the metadata location
        let footer = FooterTail::try_new(footer).unwrap();
        let metadata_len = footer.metadata_length();
        let metadata_location = footer_location.start - metadata_len..footer_location.start;

        let ops = Arc::new(OperationLog::new(
            footer_location,
            metadata_location,
            row_groups,
        ));

        TestParquetFile {
            bytes,
            ops,
            parquet_metadata,
        }
    }

    /// Return the internal bytes of the parquet file
    fn bytes(&self) -> &Bytes {
        &self.bytes
    }

    /// Return the operation log for this file
    fn ops(&self) -> &Arc<OperationLog> {
        &self.ops
    }

    /// Return the parquet metadata for this file
    fn parquet_metadata(&self) -> &Arc<ParquetMetaData> {
        &self.parquet_metadata
    }
}

/// Information about a column chunk
#[derive(Debug)]
struct TestColumnChunk {
    /// The name of the column
    name: String,

    /// The location of the entire column chunk in the file including dictionary pages
    /// and data pages.
    location: Range<usize>,

    /// The offset of the start of of the dictionary page if any
    dictionary_page_location: Option<i64>,

    /// The location of the data pages in the file
    page_locations: Vec<PageLocation>,
}

/// Information about the pages in a single row group
#[derive(Debug)]
struct TestRowGroup {
    /// Maps column_name -> Information about the column chunk
    columns: BTreeMap<String, TestColumnChunk>,
}

/// Information about all the row groups in a Parquet file, extracted from its metadata
#[derive(Debug)]
struct TestRowGroups {
    /// List of row groups, each containing information about its columns and page locations
    row_groups: Vec<TestRowGroup>,
}

impl TestRowGroups {
    fn new(parquet_metadata: &ParquetMetaData, offset_index: &ParquetOffsetIndex) -> Self {
        let row_groups = parquet_metadata
            .row_groups()
            .iter()
            .enumerate()
            .map(|(rg_index, rg_meta)| {
                let columns = rg_meta
                    .columns()
                    .iter()
                    .enumerate()
                    .map(|(col_idx, col_meta)| {
                        let column_name = col_meta.column_descr().name().to_string();
                        let page_locations = offset_index[rg_index][col_idx].page_locations();
                        let dictionary_page_location = col_meta.dictionary_page_offset();

                        // We can find the byte range of the entire column chunk
                        let (start_offset, length) = col_meta.byte_range();
                        let start_offset = start_offset as usize;
                        let end_offset = start_offset + length as usize;

                        TestColumnChunk {
                            name: column_name.clone(),
                            location: start_offset..end_offset,
                            dictionary_page_location,
                            page_locations: page_locations.clone(),
                        }
                    })
                    .map(|test_column_chunk| {
                        // make key=value pairs to insert into the BTreeMap
                        (test_column_chunk.name.clone(), test_column_chunk)
                    })
                    .collect::<BTreeMap<_, _>>();
                TestRowGroup { columns }
            })
            .collect();

        Self { row_groups }
    }

    fn iter(&self) -> impl Iterator<Item = &TestRowGroup> {
        self.row_groups.iter()
    }
}

/// Type of data read
#[derive(Debug, PartialEq)]
enum PageType {
    /// The data page with the specified index
    Data {
        data_page_index: usize,
    },
    Dictionary,
    /// Multiple pages read together
    Multi {
        /// Was the dictionary page included?
        dictionary_page: bool,
        /// The data pages included
        data_page_indices: Vec<usize>,
    },
}

impl Display for PageType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PageType::Data { data_page_index } => {
                write!(f, "DataPage({data_page_index})")
            }
            PageType::Dictionary => write!(f, "DictionaryPage"),
            PageType::Multi {
                dictionary_page,
                data_page_indices,
            } => {
                let dictionary_page = if *dictionary_page {
                    "dictionary_page: true, "
                } else {
                    ""
                };
                write!(
                    f,
                    "MultiPage({dictionary_page}data_pages: {data_page_indices:?})",
                )
            }
        }
    }
}

/// Read single logical data object (data page or dictionary page)
/// in one or more requests
#[derive(Debug)]
struct ReadInfo {
    row_group_index: usize,
    column_name: String,
    range: Range<usize>,
    read_type: PageType,
    /// Number of distinct requests (function calls) that were used
    num_requests: usize,
}

impl Display for ReadInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            row_group_index,
            column_name,
            range,
            read_type,
            num_requests,
        } = self;

        // If the average read size is less than 10 bytes, assume it is the thrift
        // decoder reading the page headers and add an annotation
        let annotation = if (range.len() / num_requests) < 10 {
            " [header]"
        } else {
            " [data]"
        };

        // align the read type to 20 characters for better readability, not sure why
        // this does not work inline with write! macro below
        write!(
            f,
            "Row Group {row_group_index}, column '{column_name}': {:15}  ({:10}, {:8}){annotation}",
            // convert to strings so alignment works
            format!("{read_type}"),
            format!("{} bytes", range.len()),
            format!("{num_requests} requests"),
        )
    }
}

/// Store structured entries in the log to make it easier to combine multiple entries
#[derive(Debug)]
enum LogEntry {
    /// Read the footer (last 8 bytes) of the parquet file
    ReadFooter(Range<usize>),
    /// Read the metadata of the parquet file
    ReadMetadata(Range<usize>),
    /// Access previously parsed metadata
    GetProvidedMetadata,
    /// Read a single logical data object
    ReadData(ReadInfo),
    /// Read one or more logical data objects in a single operation
    ReadMultipleData(Vec<LogEntry>),
    /// Not known where the read came from
    Unknown(Range<usize>),
    /// A user defined event
    Event(String),
}

impl LogEntry {
    fn event(event: impl Into<String>) -> Self {
        LogEntry::Event(event.into())
    }

    /// Appends a string representation of this log entry to the output vector
    fn append_string(&self, output: &mut Vec<String>, indent: usize) {
        let indent_str = " ".repeat(indent);
        match self {
            LogEntry::ReadFooter(range) => {
                output.push(format!("{indent_str}Footer: {} bytes", range.len()))
            }
            LogEntry::ReadMetadata(range) => {
                output.push(format!("{indent_str}Metadata: {}", range.len()))
            }
            LogEntry::GetProvidedMetadata => {
                output.push(format!("{indent_str}Get Provided Metadata"))
            }
            LogEntry::ReadData(read_info) => output.push(format!("{indent_str}{read_info}")),
            LogEntry::ReadMultipleData(read_infos) => {
                output.push(format!("{indent_str}Read Multi:"));
                for read_info in read_infos {
                    let new_indent = indent + 2;
                    read_info.append_string(output, new_indent);
                }
            }
            LogEntry::Unknown(range) => {
                output.push(format!("{indent_str}UNKNOWN: {range:?} (maybe Page Index)"))
            }
            LogEntry::Event(event) => output.push(format!("Event: {event}")),
        }
    }
}

#[derive(Debug)]
struct OperationLog {
    /// The operations performed on the file
    ops: Mutex<Vec<LogEntry>>,

    /// Footer location in the parquet file
    footer_location: Range<usize>,

    /// Metadata location in the parquet file
    metadata_location: Range<usize>,

    /// Information about the row group layout in the parquet file, used to
    /// translate read operations into human understandable IO operations
    /// Path to the parquet file
    row_groups: TestRowGroups,
}

impl OperationLog {
    fn new(
        footer_location: Range<usize>,
        metadata_location: Range<usize>,
        row_groups: TestRowGroups,
    ) -> Self {
        OperationLog {
            ops: Mutex::new(Vec::new()),
            metadata_location,
            footer_location,
            row_groups,
        }
    }

    /// Add an operation to the log
    fn add_entry(&self, entry: LogEntry) {
        let mut ops = self.ops.lock().unwrap();
        ops.push(entry);
    }

    /// Adds an entry to the operation log for the interesting object that is
    /// accessed by the specified range
    ///
    /// This function checks the ranges in order against possible locations
    /// and adds the appropriate operation to the log for the first match found.
    fn add_entry_for_range(&self, range: &Range<usize>) {
        self.add_entry(self.entry_for_range(range));
    }

    /// Adds entries to the operation log for each interesting object that is
    /// accessed by the specified range
    ///
    /// It behaves the same as [`add_entry_for_range`] but for multiple ranges.
    fn add_entry_for_ranges<'a>(&self, ranges: impl IntoIterator<Item = &'a Range<usize>>) {
        let entries = ranges
            .into_iter()
            .map(|range| self.entry_for_range(range))
            .collect::<Vec<_>>();
        self.add_entry(LogEntry::ReadMultipleData(entries));
    }

    /// Create an appropriate LogEntry for the specified range
    fn entry_for_range(&self, range: &Range<usize>) -> LogEntry {
        let start = range.start as i64;
        let end = range.end as i64;

        // figure out what logical part of the file this range corresponds to
        if self.metadata_location.contains(&range.start)
            || self.metadata_location.contains(&(range.end - 1))
        {
            return LogEntry::ReadMetadata(range.clone());
        }

        if self.footer_location.contains(&range.start)
            || self.footer_location.contains(&(range.end - 1))
        {
            return LogEntry::ReadFooter(range.clone());
        }

        // Search for the location in each column chunk.
        //
        // The actual parquet reader must in general decode the page headers
        // and determine the byte ranges of the pages. However, for this test
        // we assume the following layout:
        //
        // ```text
        // (Dictionary Page)
        // (Data Page)
        // ...
        // (Data Page)
        // ```
        //
        // We also assume that `self.page_locations` holds the location of all
        // data pages, so any read operation that overlaps with a data page
        // location is considered a read of that page, and any other read must
        // be a dictionary page read.
        for (row_group_index, row_group) in self.row_groups.iter().enumerate() {
            for (column_name, test_column_chunk) in &row_group.columns {
                // Check if the range overlaps with any data page locations
                let page_locations = test_column_chunk.page_locations.iter();

                // What data pages does this range overlap with?
                let mut data_page_indices = vec![];

                for (data_page_index, page_location) in page_locations.enumerate() {
                    let page_offset = page_location.offset;
                    let page_end = page_offset + page_location.compressed_page_size as i64;

                    // if the range fully contains the page, consider it a read of that page
                    if start >= page_offset && end <= page_end {
                        let read_info = ReadInfo {
                            row_group_index,
                            column_name: column_name.clone(),
                            range: range.clone(),
                            read_type: PageType::Data { data_page_index },
                            num_requests: 1,
                        };
                        return LogEntry::ReadData(read_info);
                    }

                    // if the range overlaps with the page, add it to the list of overlapping pages
                    if start < page_end && end > page_offset {
                        data_page_indices.push(data_page_index);
                    }
                }

                // was the dictionary page read?
                let mut dictionary_page = false;

                // Check if the range overlaps with the dictionary page location
                if let Some(dict_page_offset) = test_column_chunk.dictionary_page_location {
                    let dict_page_end = dict_page_offset + test_column_chunk.location.len() as i64;
                    if start >= dict_page_offset && end < dict_page_end {
                        let read_info = ReadInfo {
                            row_group_index,
                            column_name: column_name.clone(),
                            range: range.clone(),
                            read_type: PageType::Dictionary,
                            num_requests: 1,
                        };

                        return LogEntry::ReadData(read_info);
                    }

                    // if the range overlaps with the dictionary page, add it to the list of overlapping pages
                    if start < dict_page_end && end > dict_page_offset {
                        dictionary_page = true;
                    }
                }

                // If we can't find a page, but the range overlaps with the
                // column chunk location, use the column chunk location
                let column_byte_range = &test_column_chunk.location;
                if column_byte_range.contains(&range.start)
                    && column_byte_range.contains(&(range.end - 1))
                {
                    let read_data_entry = ReadInfo {
                        row_group_index,
                        column_name: column_name.clone(),
                        range: range.clone(),
                        read_type: PageType::Multi {
                            data_page_indices,
                            dictionary_page,
                        },
                        num_requests: 1,
                    };

                    return LogEntry::ReadData(read_data_entry);
                }
            }
        }

        // If we reach here, the range does not match any known logical part of the file
        LogEntry::Unknown(range.clone())
    }

    // Combine entries in the log that are similar to reduce noise in the log.
    fn coalesce_entries(&self) {
        let mut ops = self.ops.lock().unwrap();

        // Coalesce entries with the same read type
        let prev_ops = std::mem::take(&mut *ops);
        for entry in prev_ops {
            let Some(last) = ops.last_mut() else {
                ops.push(entry);
                continue;
            };

            let LogEntry::ReadData(ReadInfo {
                row_group_index: last_rg_index,
                column_name: last_column_name,
                range: last_range,
                read_type: last_read_type,
                num_requests: last_num_reads,
            }) = last
            else {
                // If the last entry is not a ReadColumnChunk, just push it
                ops.push(entry);
                continue;
            };

            // If the entry is not a ReadColumnChunk, just push it
            let LogEntry::ReadData(ReadInfo {
                row_group_index,
                column_name,
                range,
                read_type,
                num_requests: num_reads,
            }) = &entry
            else {
                ops.push(entry);
                continue;
            };

            // Combine the entries if they are the same and this read is less than 10b.
            //
            // This heuristic is used to combine small reads (typically 1-2
            // byte) made by the thrift decoder when reading the data/dictionary
            // page headers.
            if *row_group_index != *last_rg_index
                || column_name != last_column_name
                || read_type != last_read_type
                || (range.start > last_range.end)
                || (range.end < last_range.start)
                || range.len() > 10
            {
                ops.push(entry);
                continue;
            }
            // combine
            *last_range = last_range.start.min(range.start)..last_range.end.max(range.end);
            *last_num_reads += num_reads;
        }
    }

    /// return a snapshot of the current operations in the log.
    fn snapshot(&self) -> Vec<String> {
        self.coalesce_entries();
        let ops = self.ops.lock().unwrap();
        let mut actual = vec![];
        let indent = 0;
        ops.iter()
            .for_each(|s| s.append_string(&mut actual, indent));
        actual
    }
}
