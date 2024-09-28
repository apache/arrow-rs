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

//! Binary that prints the physical layout of a parquet file
//!
//! # Install
//!
//! `parquet-layout` can be installed using `cargo`:
//! ```
//! cargo install parquet --features=cli
//! ```
//! After this `parquet-layout` should be available:
//! ```
//! parquet-layout XYZ.parquet
//! ```
//!
//! The binary can also be built from the source code and run as follows:
//! ```
//! cargo run --features=cli --bin parquet-layout XYZ.parquet
//! ```

use std::fs::File;
use std::io::Read;

use clap::Parser;
use parquet::file::metadata::ParquetMetaDataReader;
use serde::Serialize;
use thrift::protocol::TCompactInputProtocol;

use parquet::basic::{Compression, Encoding};
use parquet::errors::Result;
use parquet::file::reader::ChunkReader;
use parquet::format::PageHeader;
use parquet::thrift::TSerializable;

#[derive(Serialize, Debug)]
struct ParquetFile {
    row_groups: Vec<RowGroup>,
}

#[derive(Serialize, Debug)]
struct RowGroup {
    columns: Vec<ColumnChunk>,
    row_count: i64,
}

#[derive(Serialize, Debug)]
struct ColumnChunk {
    path: String,
    has_offset_index: bool,
    has_column_index: bool,
    has_bloom_filter: bool,
    pages: Vec<Page>,
}

#[derive(Serialize, Debug)]
struct Page {
    compression: Option<&'static str>,
    encoding: &'static str,
    page_type: &'static str,
    offset: u64,
    compressed_bytes: i32,
    uncompressed_bytes: i32,
    header_bytes: i32,
    num_values: i32,
}

fn do_layout<C: ChunkReader>(reader: &C) -> Result<ParquetFile> {
    let metadata = ParquetMetaDataReader::new().parse_and_finish(reader)?;
    let schema = metadata.file_metadata().schema_descr();

    let row_groups = (0..metadata.num_row_groups())
        .map(|row_group_idx| {
            let row_group = metadata.row_group(row_group_idx);
            let columns = row_group
                .columns()
                .iter()
                .zip(schema.columns())
                .map(|(column, column_schema)| {
                    let compression = compression(column.compression());
                    let mut pages = vec![];

                    let mut start = column
                        .dictionary_page_offset()
                        .unwrap_or_else(|| column.data_page_offset())
                        as u64;

                    let end = start + column.compressed_size() as u64;
                    while start != end {
                        let (header_len, header) = read_page_header(reader, start)?;
                        if let Some(dictionary) = header.dictionary_page_header {
                            pages.push(Page {
                                compression,
                                encoding: encoding(dictionary.encoding),
                                page_type: "dictionary",
                                offset: start,
                                compressed_bytes: header.compressed_page_size,
                                uncompressed_bytes: header.uncompressed_page_size,
                                header_bytes: header_len as _,
                                num_values: dictionary.num_values,
                            })
                        } else if let Some(data_page) = header.data_page_header {
                            pages.push(Page {
                                compression,
                                encoding: encoding(data_page.encoding),
                                page_type: "data_page_v1",
                                offset: start,
                                compressed_bytes: header.compressed_page_size,
                                uncompressed_bytes: header.uncompressed_page_size,
                                header_bytes: header_len as _,
                                num_values: data_page.num_values,
                            })
                        } else if let Some(data_page) = header.data_page_header_v2 {
                            let is_compressed = data_page.is_compressed.unwrap_or(true);

                            pages.push(Page {
                                compression: compression.filter(|_| is_compressed),
                                encoding: encoding(data_page.encoding),
                                page_type: "data_page_v2",
                                offset: start,
                                compressed_bytes: header.compressed_page_size,
                                uncompressed_bytes: header.uncompressed_page_size,
                                header_bytes: header_len as _,
                                num_values: data_page.num_values,
                            })
                        }
                        start += header.compressed_page_size as u64 + header_len as u64;
                    }

                    Ok(ColumnChunk {
                        path: column_schema.path().parts().join("."),
                        has_offset_index: column.offset_index_offset().is_some(),
                        has_column_index: column.column_index_offset().is_some(),
                        has_bloom_filter: column.bloom_filter_offset().is_some(),
                        pages,
                    })
                })
                .collect::<Result<Vec<_>>>()?;

            Ok(RowGroup {
                columns,
                row_count: row_group.num_rows(),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(ParquetFile { row_groups })
}

/// Reads the page header at `offset` from `reader`, returning
/// both the `PageHeader` and its length in bytes
fn read_page_header<C: ChunkReader>(reader: &C, offset: u64) -> Result<(usize, PageHeader)> {
    struct TrackedRead<R>(R, usize);

    impl<R: Read> Read for TrackedRead<R> {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            let v = self.0.read(buf)?;
            self.1 += v;
            Ok(v)
        }
    }

    let input = reader.get_read(offset)?;
    let mut tracked = TrackedRead(input, 0);
    let mut prot = TCompactInputProtocol::new(&mut tracked);
    let header = PageHeader::read_from_in_protocol(&mut prot)?;
    Ok((tracked.1, header))
}

/// Returns a string representation for a given compression
fn compression(compression: Compression) -> Option<&'static str> {
    match compression {
        Compression::UNCOMPRESSED => None,
        Compression::SNAPPY => Some("snappy"),
        Compression::GZIP(_) => Some("gzip"),
        Compression::LZO => Some("lzo"),
        Compression::BROTLI(_) => Some("brotli"),
        Compression::LZ4 => Some("lz4"),
        Compression::ZSTD(_) => Some("zstd"),
        Compression::LZ4_RAW => Some("lz4_raw"),
    }
}

/// Returns a string representation for a given encoding
fn encoding(encoding: parquet::format::Encoding) -> &'static str {
    match Encoding::try_from(encoding) {
        Ok(Encoding::PLAIN) => "plain",
        Ok(Encoding::PLAIN_DICTIONARY) => "plain_dictionary",
        Ok(Encoding::RLE) => "rle",
        #[allow(deprecated)]
        Ok(Encoding::BIT_PACKED) => "bit_packed",
        Ok(Encoding::DELTA_BINARY_PACKED) => "delta_binary_packed",
        Ok(Encoding::DELTA_LENGTH_BYTE_ARRAY) => "delta_length_byte_array",
        Ok(Encoding::DELTA_BYTE_ARRAY) => "delta_byte_array",
        Ok(Encoding::RLE_DICTIONARY) => "rle_dictionary",
        Ok(Encoding::BYTE_STREAM_SPLIT) => "byte_stream_split",
        Err(_) => "unknown",
    }
}

#[derive(Debug, Parser)]
#[clap(author, version, about("Prints the physical layout of a parquet file"), long_about = None)]
struct Args {
    #[clap(help("Path to a parquet file"))]
    file: String,
}

impl Args {
    fn run(&self) -> Result<()> {
        let file = File::open(&self.file)?;
        let layout = do_layout(&file)?;

        let out = std::io::stdout();
        let writer = out.lock();

        serde_json::to_writer_pretty(writer, &layout).unwrap();
        Ok(())
    }
}

fn main() -> Result<()> {
    Args::parse().run()
}
