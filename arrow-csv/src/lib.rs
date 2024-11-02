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

//! Transfer data between the Arrow memory format and CSV (comma-separated values).

#![warn(missing_docs)]

pub mod reader;
pub mod writer;

pub use self::reader::infer_schema_from_files;
pub use self::reader::Reader;
pub use self::reader::ReaderBuilder;
pub use self::writer::Writer;
pub use self::writer::WriterBuilder;
use arrow_schema::ArrowError;

fn map_csv_error(error: csv::Error) -> ArrowError {
    match error.kind() {
        csv::ErrorKind::Io(error) => ArrowError::CsvError(error.to_string()),
        csv::ErrorKind::Utf8 { pos, err } => ArrowError::CsvError(format!(
            "Encountered UTF-8 error while reading CSV file: {}{}",
            err,
            pos.as_ref()
                .map(|pos| format!(" at line {}", pos.line()))
                .unwrap_or_default(),
        )),
        csv::ErrorKind::UnequalLengths {
            pos,
            expected_len,
            len,
        } => ArrowError::CsvError(format!(
            "Encountered unequal lengths between records on CSV file. Expected {} \
                 records, found {} records{}",
            len,
            expected_len,
            pos.as_ref()
                .map(|pos| format!(" at line {}", pos.line()))
                .unwrap_or_default(),
        )),
        _ => ArrowError::CsvError("Error reading CSV file".to_string()),
    }
}
