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

use arrow::util::display::array_value_to_string;
use arrow::util::pretty::pretty_format_columns;
use parquet::arrow::arrow_reader::ArrowReaderBuilder;
use pretty_assertions::assert_eq;
use serde::Serialize;
use serde_json::{json, Value};
/// Test driver for parquet-integration testing
use std::fs::{canonicalize, File};
use std::path::{Path, PathBuf};
use parquet::file::metadata::ColumnChunkMetaData;
use parquet::format::ColumnMetaData;

fn main() {
    let integration_test = IntegrationTest::new();

    let filenames = vec![
        "alltypes_plain.parquet",
    ];

    for filename in filenames {
        integration_test.data_test(filename);
        integration_test.metadata_test(filename)
    }
}

// prototype demonstration of checking type support for parquet-rs encoding
// check read support by reading a file with the specified encoding correctly
#[derive(Debug)]
struct IntegrationTest {
    parquet_data_path: PathBuf,
    expected_data_path: PathBuf,
    output_data_path: PathBuf,
}

impl IntegrationTest {
    pub fn new() -> Self {
        // TODO error handling

        // paths are relative to arrow-rs/parquet-integration-testing
        let parquet_data_path = PathBuf::from("../parquet-testing/data")
            .canonicalize()
            .unwrap();
        let expected_data_path = PathBuf::from("data").canonicalize().unwrap();
        let output_data_path = PathBuf::from("out").canonicalize().unwrap();

        std::fs::create_dir_all(&output_data_path).unwrap();

        Self {
            parquet_data_path,
            expected_data_path,
            output_data_path,
        }
    }

    /// Read a parquet file, create a JSON representation, and compare to the
    /// known good value in data
    ///
    /// The output JSON looks like this:
    ///
    /// ```text
    /// {
    ///   filename: "filename.parquet",
    ///   rows: [
    ///     {
    ///       "column1": "value1",
    ///       "column2": 123,
    ///       "column3": null
    ///     },
    ///     ..
    ///     {
    ///       "column1": "value2",
    ///       "column2": 456,
    ///       "column3": "value3"
    ///     }
    ///   ]
    /// }
    /// ```
    fn data_test(&self, filename: &str) {
        let parquet_file_path = self.parquet_data_path.join(filename);
        let expected_file_path = self.expected_data_path.join(format!("{filename}.data.json"));

        // For ease of development, write the actual parsed value to a file (to
        // permit easy updates, for example)
        let output_file_path = self.output_data_path.join(format!("{filename}.data.json"));

        println!("Begin data test: {filename}");
        println!("  Input parquet file: {parquet_file_path:?}");
        println!("  Expected JSON file: {expected_file_path:?}");
        println!("  Output JSON file: {output_file_path:?}");

        let parquet_json = read_parquet_data(&parquet_file_path);
        let output_file = File::create(&output_file_path).unwrap();
        serde_json::to_writer_pretty(output_file, &parquet_json).unwrap();

        // read expected file if present, default to {} if not
        let expected_json = if let Ok(expected_file) = File::open(expected_file_path) {
            serde_json::from_reader(expected_file).unwrap()
        } else {
            json!({})
        };
        assert_eq!(parquet_json, expected_json)
    }

    /// Read a parquet file, create a JSON representation of its metadata, and compares to the
    /// known good value in data
    ///
    /// The output JSON looks like this:
    ///
    /// ```text
    /// {
    ///   filename: "filename.parquet",
    ///   ..
    ///     ..
    /// }
    /// ```
    fn metadata_test(&self, filename: &str) {
        let parquet_file_path = self.parquet_data_path.join(filename);
        let expected_file_path = self.expected_data_path.join(format!("{filename}.metadata.json"));

        // For ease of development, write the actual parsed value to a file (to
        // permit easy updates, for example)
        let output_file_path = self.output_data_path.join(format!("{filename}.metadata.json"));

        println!("Begin metadata test: {filename}");
        println!("  Input parquet file: {parquet_file_path:?}");
        println!("  Expected JSON file: {expected_file_path:?}");
        println!("  Output JSON file: {output_file_path:?}");

        let parquet_json = read_parquet_metadata(&parquet_file_path);
        let output_file = File::create(&output_file_path).unwrap();
        serde_json::to_writer_pretty(output_file, &parquet_json).unwrap();

        // read expected file if present, default to {} if not
        let expected_json = if let Ok(expected_file) = File::open(expected_file_path) {
            serde_json::from_reader(expected_file).unwrap()
        } else {
            json!({})
        };
        assert_eq!(parquet_json, expected_json)
    }
}



/// The function reads a parquet file and returns a JSON representation of the
/// data within
fn read_parquet_data(parquet_data_path: &Path) -> Value {
    let file = File::open(parquet_data_path).unwrap();
    let mut reader = ArrowReaderBuilder::try_new(file).unwrap().build().unwrap();

    let mut rows = vec![];
    while let Some(batch) = reader.next() {
        let batch = batch.unwrap();
        let columns = batch.columns();
        let schema = batch.schema();
        for i in 0..batch.num_rows() {
            let mut row = vec![];
            for (field, column) in schema.fields.iter().zip(columns.iter()) {
                let name = field.name();
                let value = array_value_to_string(column.as_ref(), i).unwrap();
                row.push(json!({name: value}));
            }
            rows.push(json!(row));
        }
    }

    let filename = parquet_data_path.file_name().unwrap().to_string_lossy();

    json!({
        "filename": filename,
        "rows": rows
    })
}


/// The function reads a parquet file and writes a JSON representation of the
/// metatadata (thrift encoded) within
fn read_parquet_metadata(parquet_data_path: &Path) -> Value {
    let file = File::open(parquet_data_path).unwrap();
    let metadata = ArrowReaderBuilder::try_new(file).unwrap().metadata().clone();

    // todo print out schema
    let row_groups : Vec<_> = metadata.row_groups().iter()
        .map(|rg| {
            let columns: Vec<_> = rg.columns().iter()
                .map(column_metadata_to_json)
                .collect();
        json!({
            "num_rows": rg.num_rows(),
            "total_byte_size": rg.total_byte_size(),
            "file_offset": rg.file_offset(),
            "ordinal": rg.ordinal(),
            "columns": columns,
        })
    }).collect();;

    let filename = parquet_data_path.file_name().unwrap().to_string_lossy();

    json!({
        "filename33": filename,
        "row_goups": row_groups,
    })
}

fn column_metadata_to_json(column_metadata: &ColumnChunkMetaData) -> Value {

    json!({
        "file_path": column_metadata.file_path(),
        "file_offset": column_metadata.file_offset(),
        // todo: column metadata
        // "num_values": column_metadata.num_values(),
        // todo column-type/ column-path/descr
        //"file_path": column_metadata.file_path(),

    })

}