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

//! Binary file to print the schema and metadata of a Parquet file.
//!
//! # Install
//!
//! `parquet-schema` can be installed using `cargo`:
//! ```
//! cargo install parquet --features=cli
//! ```
//! After this `parquet-schema` should be available:
//! ```
//! parquet-schema XYZ.parquet
//! ```
//!
//! The binary can also be built from the source code and run as follows:
//! ```
//! cargo run --features=cli --bin parquet-schema XYZ.parquet
//! ```
//!
//! Note that `verbose` is an optional boolean flag that allows to print schema only,
//! when not provided or print full file metadata when provided.

use clap::Parser;
use parquet::{
    file::reader::{FileReader, SerializedFileReader},
    schema::printer::{print_file_metadata, print_parquet_metadata},
};
use std::{fs::File, path::Path};

#[derive(Debug, Parser)]
#[clap(author, version, about("Binary file to print the schema and metadata of a Parquet file"), long_about = None)]
struct Args {
    #[clap(help("Path to the parquet file"))]
    file_path: String,
    #[clap(short, long, help("Enable printing full file metadata"))]
    verbose: bool,
}

fn main() {
    let args = Args::parse();
    let filename = args.file_path;
    let path = Path::new(&filename);
    let file = File::open(path).expect("Unable to open file");
    let verbose = args.verbose;

    match SerializedFileReader::new(file) {
        Err(e) => panic!("Error when parsing Parquet file: {e}"),
        Ok(parquet_reader) => {
            let metadata = parquet_reader.metadata();
            println!("Metadata for file: {}", &filename);
            println!();
            if verbose {
                print_parquet_metadata(&mut std::io::stdout(), metadata);
            } else {
                print_file_metadata(&mut std::io::stdout(), metadata.file_metadata());
            }
        }
    }
}
