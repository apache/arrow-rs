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
use parquet::schema::types::Type;
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

fn schema_from_file(path: &String) -> Result<Arc<Type>> {
    let reader = File::open(path)?;
    let metadata = ParquetMetaDataReader::new().parse_and_finish(&reader)?;
    Ok::<Arc<parquet::schema::types::Type>, ParquetError>(
        metadata.file_metadata().schema_descr().root_schema_ptr(),
    )
}

impl Args {
    fn run(&self) -> Result<()> {
        if self.input.is_empty() {
            return Err(ParquetError::General(
                "Must provide at least one input file".into(),
            ));
        }

        // Compare schemas in a first pass to make sure they all match
        let expected = schema_from_file(&self.input[0])?;
        let other_schemas = self.input[1..].iter().map(|x| (x, schema_from_file(x)));
        for res_other in other_schemas {
            let path = res_other.0;
            let actual = res_other.1?;
            if actual != expected {
                return Err(ParquetError::General(format!(
                    "inputs must have the same schema: file {path}: expected schema {expected:#?} got {actual:#?}"
                )));
            }
        }

        // Copy to output file
        let output = File::create(&self.output)?;
        let props = Arc::new(WriterProperties::builder().build());
        let mut writer = SerializedFileWriter::new(output, expected, props)?;

        self.input
            .iter()
            .map(|x| {
                let input = File::open(x)?;
                let metadata = ParquetMetaDataReader::new().parse_and_finish(&input)?;
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
                Ok(())
            })
            .collect::<Result<Vec<_>>>()?;

        writer.close()?;

        Ok(())
    }
}

fn main() -> Result<()> {
    Args::parse().run()
}

#[cfg(test)]
mod tests {

    use super::*;
    use tempfile::TempDir;

    #[test]
    #[should_panic]
    fn test_parse_args0() {
        Args::try_parse_from(vec![""]).unwrap();
    }

    #[test]
    #[should_panic]
    fn test_parse_args1() {
        Args::try_parse_from(vec!["parquet-concat"]).unwrap();
    }

    #[test]
    fn test_parse_args2() -> Result<()> {
        let tmpf = TempDir::new().unwrap();
        let msg = "Must provide at least one input file".to_string();
        let out_name = tmpf
            .path()
            .join("out.parquet")
            .into_os_string()
            .into_string()
            .unwrap();
        let args = Args::try_parse_from(vec!["parquet-concat", &out_name]).unwrap();
        match args.run() {
            Err(ParquetError::General(msg)) => (),
            _ => assert!(false, "expected General ParquetError"),
        }
        Ok(())
    }
}
