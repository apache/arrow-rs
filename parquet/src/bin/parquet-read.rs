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

//! Binary file to read data from a Parquet file.
//!
//! # Install
//!
//! `parquet-read` can be installed using `cargo`:
//! ```
//! cargo install parquet --features=cli
//! ```
//! After this `parquet-read` should be globally available:
//! ```
//! parquet-read XYZ.parquet
//! ```
//!
//! The binary can also be built from the source code and run as follows:
//! ```
//! cargo run --features=cli --bin parquet-read XYZ.parquet
//! ```
//!
//! Note that `parquet-read` reads full file schema, no projection or filtering is
//! applied.

extern crate parquet;

use clap::Parser;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::Row;
use std::{fs::File, path::Path};

#[derive(Debug, Parser)]
#[clap(author, version, about("Binary file to read data from a Parquet file"), long_about = None)]
struct Args {
    #[clap(short, long, help("Path to a parquet file"))]
    file_name: String,
    #[clap(
        short,
        long,
        default_value_t = 0_usize,
        help("Number of records to read. When not provided or 0, all records are read")
    )]
    num_records: usize,
    #[clap(short, long, help("Print Parquet file in JSON lines format"))]
    json: bool,
}

fn main() {
    let args = Args::parse();

    let filename = args.file_name;
    let num_records = args.num_records;
    let json = args.json;

    let path = Path::new(&filename);
    let file = File::open(&path).expect("Unable to open file");
    let parquet_reader =
        SerializedFileReader::new(file).expect("Failed to create reader");

    // Use full schema as projected schema
    let mut iter = parquet_reader
        .get_row_iter(None)
        .expect("Failed to create row iterator");

    let mut start = 0;
    let end = num_records;
    let all_records = end == 0;

    while all_records || start < end {
        match iter.next() {
            Some(row) => print_row(&row, json),
            None => break,
        }
        start += 1;
    }
}

fn print_row(row: &Row, json: bool) {
    if json {
        println!("{}", row.to_json_value())
    } else {
        println!("{}", row.to_string());
    }
}
