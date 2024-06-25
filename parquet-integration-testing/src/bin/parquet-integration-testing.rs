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

fn main() {
    // paths are relative to arrow-rs/parquet-integration-testing
    let parquet_data_path =
        PathBuf::from("../parquet-testing/data").canonicalize().unwrap();
    let expected_data_path =
        PathBuf::from("data").canonicalize().unwrap();
    let output_data_path =
        PathBuf::from("out").canonicalize().unwrap();

    std::fs::create_dir_all(&output_data_path).unwrap();

    let filenames = vec![
        "alltypes_plain.parquet",
        //"alltypes_plain_dictionary.parquet",
    ];

    for filename in filenames {
        let parquet_file_path = parquet_data_path.join(filename);

        let expected_file_path = expected_data_path
            .join(format!("{filename}.json"));

        // For development, also write the actual parsed value to a file
        let output_file_path = output_data_path
            .join(format!("{filename}.json"));



        println!("Begin test: {filename}");
        println!("  Input parquet file: {parquet_file_path:?}");
        println!("  Expected JSON file: {expected_file_path:?}");
        println!("  Output JSON file: {output_file_path:?}");

        let parquet_json = read_parquet_data(&parquet_file_path);
        let output_file = File::create(&output_file_path).unwrap();
        serde_json::to_writer(output_file, &parquet_json).unwrap();

        let expected_file = File::open(expected_file_path).unwrap();
        let expected_json: Value = serde_json::from_reader(expected_file).unwrap();
        assert_eq!(parquet_json, expected_json)
    }
}

// prototype demonstration of checking type support for parquet-rs encoding
// check read support by reading a file with the specified encoding correctly

// | PLAIN                                     |       |        |       |       |
// | PLAIN_DICTIONARY                          |       |        |       |       |
// | RLE_DICTIONARY                            |       |        |       |       |
// | RLE                                       |       |        |       |       |
// | BIT_PACKED (deprecated)                   |       |        |       |       |
// | DELTA_BINARY_PACKED                       |       |        |       |       |

// The idea is to produce a file like this:
// ```text
// {
//   filename: "filename.parquet",
//   rows: [
//     {
//       "column1": "value1",
//       "column2": 123,
//       "column3": null
//     },
//     ..
//     {
//       "column1": "value2",
//       "column2": 456,
//       "column3": "value3"
//     }
//   ]
// }
// ```

/// The function reads a parquet file and writes a JSON representation of the data within
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

    json!({
        "filename": parquet_data_path,
        "rows": rows
    })
}
