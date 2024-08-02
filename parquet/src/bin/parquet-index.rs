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

//! Binary that prints the [page index] of a parquet file
//!
//! # Install
//!
//! `parquet-layout` can be installed using `cargo`:
//! ```
//! cargo install parquet --features=cli
//! ```
//! After this `parquet-index` should be available:
//! ```
//! parquet-index XYZ.parquet COLUMN_NAME
//! ```
//!
//! The binary can also be built from the source code and run as follows:
//! ```
//! cargo run --features=cli --bin parquet-index XYZ.parquet COLUMN_NAME
//!
//! [page index]: https://github.com/apache/parquet-format/blob/master/PageIndex.md

use clap::Parser;
use parquet::errors::{ParquetError, Result};
use parquet::file::page_index::index::{Index, PageIndex};
use parquet::file::page_index::offset_index::OffsetIndexMetaData;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::serialized_reader::ReadOptionsBuilder;
use parquet::format::PageLocation;
use std::fs::File;

#[derive(Debug, Parser)]
#[clap(author, version, about("Prints the page index of a parquet file"), long_about = None)]
struct Args {
    #[clap(help("Path to a parquet file"))]
    file: String,

    #[clap(help("Column name to print"))]
    column: String,
}

impl Args {
    fn run(&self) -> Result<()> {
        let file = File::open(&self.file)?;
        let options = ReadOptionsBuilder::new().with_page_index().build();
        let reader = SerializedFileReader::new_with_options(file, options)?;

        let schema = reader.metadata().file_metadata().schema_descr();
        let column_idx = schema
            .columns()
            .iter()
            .position(|x| x.name() == self.column.as_str())
            .ok_or_else(|| {
                ParquetError::General(format!("Failed to find column {}", self.column))
            })?;

        // Column index data for all row groups and columns
        let column_index = reader
            .metadata()
            .column_index()
            .ok_or_else(|| ParquetError::General("Column index not found".to_string()))?;

        // Offset index data for all row groups and columns
        let offset_index = reader
            .metadata()
            .offset_index()
            .ok_or_else(|| ParquetError::General("Offset index not found".to_string()))?;

        // Iterate through each row group
        for (row_group_idx, ((column_indices, offset_indices), row_group)) in column_index
            .iter()
            .zip(offset_index)
            .zip(reader.metadata().row_groups())
            .enumerate()
        {
            println!("Row Group: {row_group_idx}");
            let offset_index = offset_indices.get(column_idx).ok_or_else(|| {
                ParquetError::General(format!(
                    "No offset index for row group {row_group_idx} column chunk {column_idx}"
                ))
            })?;

            let row_counts =
                compute_row_counts(offset_index.page_locations.as_slice(), row_group.num_rows());
            match &column_indices[column_idx] {
                Index::NONE => println!("NO INDEX"),
                Index::BOOLEAN(v) => print_index(&v.indexes, offset_index, &row_counts)?,
                Index::INT32(v) => print_index(&v.indexes, offset_index, &row_counts)?,
                Index::INT64(v) => print_index(&v.indexes, offset_index, &row_counts)?,
                Index::INT96(v) => print_index(&v.indexes, offset_index, &row_counts)?,
                Index::FLOAT(v) => print_index(&v.indexes, offset_index, &row_counts)?,
                Index::DOUBLE(v) => print_index(&v.indexes, offset_index, &row_counts)?,
                Index::BYTE_ARRAY(v) => print_index(&v.indexes, offset_index, &row_counts)?,
                Index::FIXED_LEN_BYTE_ARRAY(v) => {
                    print_index(&v.indexes, offset_index, &row_counts)?
                }
            }
        }
        Ok(())
    }
}

/// Computes the number of rows in each page within a column chunk
fn compute_row_counts(offset_index: &[PageLocation], rows: i64) -> Vec<i64> {
    if offset_index.is_empty() {
        return vec![];
    }

    let mut last = offset_index[0].first_row_index;
    let mut out = Vec::with_capacity(offset_index.len());
    for o in offset_index.iter().skip(1) {
        out.push(o.first_row_index - last);
        last = o.first_row_index;
    }
    out.push(rows - last);
    out
}

/// Prints index information for a single column chunk
fn print_index<T: std::fmt::Display>(
    column_index: &[PageIndex<T>],
    offset_index: &OffsetIndexMetaData,
    row_counts: &[i64],
) -> Result<()> {
    if column_index.len() != offset_index.page_locations.len() {
        return Err(ParquetError::General(format!(
            "Index length mismatch, got {} and {}",
            column_index.len(),
            offset_index.page_locations.len()
        )));
    }

    for (idx, ((c, o), row_count)) in column_index
        .iter()
        .zip(offset_index.page_locations())
        .zip(row_counts)
        .enumerate()
    {
        print!(
            "Page {:>5} at offset {:#010x} with length {:>10} and row count {:>10}",
            idx, o.offset, o.compressed_page_size, row_count
        );
        match &c.min {
            Some(m) => print!(", min {m:>10}"),
            None => print!(", min {:>10}", "NONE"),
        }

        match &c.max {
            Some(m) => print!(", max {m:>10}"),
            None => print!(", max {:>10}", "NONE"),
        }
        println!()
    }

    Ok(())
}

fn main() -> Result<()> {
    Args::parse().run()
}
