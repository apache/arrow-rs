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

//! Tests that reading invalid parquet files returns an error

use arrow::util::test_util::parquet_test_data;
use parquet::arrow::arrow_reader::ArrowReaderBuilder;
use parquet::errors::ParquetError;
use std::collections::HashSet;
use std::path::PathBuf;

static KNOWN_FILES: &[&str] = &[
    "PARQUET-1481.parquet",
    "ARROW-GH-41317.parquet",
    "ARROW-GH-41321.parquet",
    "ARROW-GH-43605.parquet",
    "ARROW-RS-GH-6229-DICTHEADER.parquet",
    "ARROW-RS-GH-6229-LEVELS.parquet",
    "README.md",
];

/// Returns the path to 'parquet-testing/bad_data'
fn bad_data_dir() -> PathBuf {
    // points to parquet-testing/data
    let parquet_testing_data = parquet_test_data();
    PathBuf::from(parquet_testing_data)
        .parent()
        .expect("was in parquet-testing/data")
        .join("bad_data")
}

#[test]
// Ensure that if we add a new test the files are added to the tests.
fn test_invalid_files() {
    let known_files: HashSet<_> = KNOWN_FILES.iter().cloned().collect();
    let mut seen_files = HashSet::new();

    let files = std::fs::read_dir(bad_data_dir()).unwrap();

    for file in files {
        let file_name = file
            .unwrap()
            .path()
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        // If you see this error, please add a test for the new file following the model below
        assert!(
            known_files.contains(file_name.as_str()),
            "Found new file in bad_data, please add test: {file_name}"
        );
        seen_files.insert(file_name);
    }
    for expected_file in known_files {
        assert!(
            seen_files.contains(expected_file),
            "Expected file not found in bad_data directory: {expected_file}"
        );
    }
}

#[test]
fn test_parquet_1481() {
    let err = read_file("PARQUET-1481.parquet").unwrap_err();
    assert_eq!(
        err.to_string(),
        "Parquet error: unexpected parquet type: -7"
    );
}

#[test]
#[should_panic(expected = "assertion failed: self.current_value.is_some()")]
fn test_arrow_gh_41321() {
    let err = read_file("ARROW-GH-41321.parquet").unwrap_err();
    assert_eq!(err.to_string(), "TBD (currently panics)");
}

#[test]
fn test_arrow_gh_41317() {
    let err = read_file("ARROW-GH-41317.parquet").unwrap_err();
    assert_eq!(
        err.to_string(),
        "External: Parquet argument error: External: bad data"
    );
}

#[test]
fn test_arrow_rs_gh_6229_dict_header() {
    let err = read_file("ARROW-RS-GH-6229-DICTHEADER.parquet").unwrap_err();
    assert_eq!(
        err.to_string(),
        "External: Parquet argument error: EOF: eof decoding byte array"
    );
}

#[test]
#[cfg(feature = "snap")]
fn test_arrow_rs_gh_6229_dict_levels() {
    let err = read_file("ARROW-RS-GH-6229-LEVELS.parquet").unwrap_err();
    assert_eq!(
        err.to_string(),
        "External: Parquet argument error: Parquet error: Insufficient repetition levels read from column"
    );
}

/// Reads the file and tries to return the total row count
/// Returns an error if the file is invalid
fn read_file(name: &str) -> Result<usize, ParquetError> {
    let path = bad_data_dir().join(name);
    println!("Reading file: {:?}", path);

    let file = std::fs::File::open(&path).unwrap();
    let reader = ArrowReaderBuilder::try_new(file)?.build()?;

    let mut num_rows = 0;
    for batch in reader {
        let batch = batch?;
        num_rows += batch.num_rows();
    }
    Ok(num_rows)
}

#[cfg(feature = "async")]
#[tokio::test]
async fn bad_metadata_err() {
    use bytes::Bytes;
    use parquet::file::metadata::ParquetMetaDataReader;

    let metadata_buffer = Bytes::from_static(include_bytes!("bad_raw_metadata.bin"));

    let metadata_length = metadata_buffer.len();

    let mut reader = std::io::Cursor::new(&metadata_buffer);
    let mut loader = ParquetMetaDataReader::new();
    loader.try_load(&mut reader, metadata_length).await.unwrap();
    loader = loader.with_page_indexes(false);
    loader.load_page_index(&mut reader).await.unwrap();

    loader = loader.with_offset_indexes(true);
    loader.load_page_index(&mut reader).await.unwrap();

    loader = loader.with_column_indexes(true);
    let err = loader.load_page_index(&mut reader).await.unwrap_err();

    assert_eq!(
        err.to_string(),
        "Parquet error: error converting value, expected 4 bytes got 0"
    );
}
