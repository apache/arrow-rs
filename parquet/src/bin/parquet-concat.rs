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

//! Binary that concatenates the column data of one or more parquet files
//!
//! # Install
//!
//! `parquet-concat` can be installed using `cargo`:
//! ```
//! cargo install parquet --features=cli
//! ```
//! After this `parquet-concat` should be available:
//! ```
//! parquet-concat out.parquet a.parquet b.parquet
//! ```
//!
//! The binary can also be built from the source code and run as follows:
//! ```
//! cargo run --features=cli --bin parquet-concat out.parquet a.parquet b.parquet
//! ```
//!
//! Note: this does not currently support preserving the page index or bloom filters
//!

use clap::Parser;
use parquet::column::writer::ColumnCloseResult;
use parquet::errors::{ParquetError, Result};
use parquet::file::metadata::ParquetMetaDataReader;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use std::fs::File;
use std::sync::Arc;

#[derive(Debug, Parser)]
#[clap(author, version)]
/// Concatenates one or more parquet files
struct Args {
    /// Path to output
    output: String,

    /// Path to input files
    input: Vec<String>,
}

impl Args {
    fn run(&self) -> Result<()> {
        if self.input.is_empty() {
            return Err(ParquetError::General(
                "Must provide at least one input file".into(),
            ));
        }

        let output = File::create(&self.output)?;

        let inputs = self
            .input
            .iter()
            .map(|x| {
                let reader = File::open(x)?;
                let metadata = ParquetMetaDataReader::new().parse_and_finish(&reader)?;
                Ok((reader, metadata))
            })
            .collect::<Result<Vec<_>>>()?;

        let expected = inputs[0].1.file_metadata().schema();
        for (_, metadata) in inputs.iter().skip(1) {
            let actual = metadata.file_metadata().schema();
            if expected != actual {
                return Err(ParquetError::General(format!(
                    "inputs must have the same schema, {expected:#?} vs {actual:#?}"
                )));
            }
        }

        let props = Arc::new(WriterProperties::builder().build());
        let schema = inputs[0].1.file_metadata().schema_descr().root_schema_ptr();
        let mut writer = SerializedFileWriter::new(output, schema, props)?;

        for (input, metadata) in inputs {
            for rg in metadata.row_groups() {
                let mut rg_out = writer.next_row_group()?;
                for column in rg.columns() {
                    let result = ColumnCloseResult {
                        bytes_written: column.compressed_size() as _,
                        rows_written: rg.num_rows() as _,
                        metadata: column.clone(),
                        bloom_filter: None,
                        column_index: None,
                        offset_index: None,
                    };
                    rg_out.append_column(&input, result)?;
                }
                rg_out.close()?;
            }
        }

        writer.close()?;

        Ok(())
    }
}

fn main() -> Result<()> {
    Args::parse().run()
}
