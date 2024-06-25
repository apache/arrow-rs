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


// Draft program for testing the parquet-rs library

use std::fs::{canonicalize, File};
use std::path::Path;
use arrow::util::display::array_value_to_string;
use arrow::util::pretty::pretty_format_columns;
use parquet::arrow::arrow_reader::ArrowReaderBuilder;
use serde::Serialize;
use serde_json::json;

fn main() {
    println!("PWD: {:?}", std::env::var("PWD"));
    let parquet_data_path = "/Users/andrewlamb/Software/arrow-rs/parquet-testing/data";
    let expected_data_path = "/Users/andrewlamb/Software/arrow-rs/parquet-integration-testing/data";

    let filenames = vec!["alltypes_plain.parquet", "alltypes_plain_dictionary.parquet"];

    for filename in filenames {
        let parquet_file_path = Path::from(parquet_data_path).join(filename).canonicalize().unwrap();
        let expected_file_path = Path::from(expected_data_path).join(format!("{}filename}.json"));

        println!("Begin test: {filename}");
        println!("  Reading parquet file: {parquet_file_path}");
        println!("  Expected JSON file: {expected_file_path}");
        let parquet_json = read_parquet_data(&parquet_file_path);
        let expected_json = std::fs::read_to_string(expected_file_path);
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
fn read_parquet_data(parquet_data_path: &str) -> String {
    let file = File::open(&parquet_data_path).unwrap();
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

    let value = json!({
        "filename": parquet_data_path,
        "rows": rows
    });

    serde_json::to_string_pretty(&value).unwrap();
}