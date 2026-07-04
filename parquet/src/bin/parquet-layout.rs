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
//! Alternatives to this binary include [`parquet-cli`] and [`parquet-viewer`].
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
//!
//! [`parquet-cli`]: https://github.com/apache/parquet-java/tree/master/parquet-cli
//! [`parquet-viewer`]: https://github.com/xiangpenghao/parquet-viewer

use std::fs::File;

use clap::Parser;
use parquet::file::metadata::ParquetMetaDataReader;
use serde::{Serialize, Serializer};

use parquet::basic::{CompressionCodec, Encoding};
use parquet::errors::Result;
use parquet::file::reader::ChunkReader;

#[derive(Serialize, Debug)]
struct Index {
    offset: i64,
    length: Option<i32>,
}

#[derive(Serialize, Debug)]
struct Footer {
    metadata_size: Option<usize>,
}

#[derive(Serialize, Debug)]
struct ParquetFile {
    row_groups: Vec<RowGroup>,
    footer: Footer,
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
    offset_index: Option<Index>,
    column_index: Option<Index>,
    bloom_filter: Option<Index>,
    compression: DebugSerialize<CompressionCodec>,
    encodings: Vec<DebugSerialize<Encoding>>,
}

#[derive(Debug)]
struct DebugSerialize<T: std::fmt::Debug>(T);

impl<T: std::fmt::Debug> Serialize for DebugSerialize<T> {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&format!("{:?}", &self.0))
    }
}

fn do_layout<C: ChunkReader>(reader: &C) -> Result<ParquetFile> {
    let mut metadata_reader = ParquetMetaDataReader::new();
    metadata_reader.try_parse(reader)?;
    let metadata_size = metadata_reader.metadata_size();
    let metadata = metadata_reader.finish()?;
    let schema = metadata.file_metadata().schema_descr();

    let row_groups = (0..metadata.num_row_groups())
        .map(|row_group_idx| {
            let row_group = metadata.row_group(row_group_idx);
            let columns = row_group
                .columns()
                .iter()
                .zip(schema.columns())
                .map(|(column, column_schema)| {
                    let compression = DebugSerialize(column.compression_codec());
                    let encodings = column.encodings().map(DebugSerialize).collect();

                    Ok(ColumnChunk {
                        path: column_schema.path().parts().join("."),
                        has_offset_index: column.offset_index_offset().is_some(),
                        has_column_index: column.column_index_offset().is_some(),
                        has_bloom_filter: column.bloom_filter_offset().is_some(),
                        offset_index: column.offset_index_offset().map(|offset| Index {
                            offset,
                            length: column.offset_index_length(),
                        }),
                        column_index: column.column_index_offset().map(|offset| Index {
                            offset,
                            length: column.column_index_length(),
                        }),
                        bloom_filter: column.bloom_filter_offset().map(|offset| Index {
                            offset,
                            length: column.bloom_filter_length(),
                        }),
                        compression,
                        encodings,
                    })
                })
                .collect::<Result<Vec<_>>>()?;

            Ok(RowGroup {
                columns,
                row_count: row_group.num_rows(),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(ParquetFile {
        row_groups,
        footer: Footer { metadata_size },
    })
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
