// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Regression tests for malformed Parquet files.
//! Historical FLBA compatibility is tested in `legacy_fixed_len_byte_array`.

use arrow::compute::concat_batches;
use arrow::util::test_util::parquet_test_data;
use arrow_array::RecordBatchReader;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ArrowReaderBuilder;
use parquet::errors::ParquetError;
use parquet::file::metadata::ParquetMetaDataReader;
use std::collections::HashSet;
use std::path::PathBuf;

static KNOWN_FILES: &[&str] = &[
    "PARQUET-1481.parquet",
    "ARROW-GH-41317.parquet",
    "ARROW-GH-41321.parquet",
    "ARROW-GH-43605.parquet",
    "ARROW-RS-GH-6229-DICTHEADER.parquet",
    "ARROW-RS-GH-6229-LEVELS.parquet",
    "ARROW-RS-GH-11261-FLBA-DICT.parquet",
    "ARROW-GH-45185.parquet",
    "ARROW-GH-47662.parquet",
    "README.md",
    "variants",
];

/// Returns the path to 'parquet-testing/bad_data'
pub(super) fn bad_data_dir() -> PathBuf {
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
    let known_files: HashSet<_> = KNOWN_FILES.iter().copied().collect();
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
    assert_eq!(err.to_string(), "Parquet error: Unexpected Type -7");
}

#[test]
#[cfg_attr(miri, ignore)] // Takes too long
fn test_arrow_gh_41321() {
    let err = read_file("ARROW-GH-41321.parquet").unwrap_err();
    assert_eq!(
        err.to_string(),
        "External: Parquet argument error: Parquet error: Invalid or corrupted RLE bit width 254. Max allowed is 32"
    );
}

#[test]
fn test_arrow_gh_41317() {
    let err = read_file("ARROW-GH-41317.parquet").unwrap_err();
    assert_eq!(
        err.to_string(),
        "Parquet error: Expected list element type of I32 but got I16"
    );
}

#[test]
fn test_arrow_rs_gh_6229_dict_header() {
    let err = read_file("ARROW-RS-GH-6229-DICTHEADER.parquet").unwrap_err();
    let message = err.to_string();
    assert!(
        message.starts_with("External: Parquet argument error: Parquet error: Integer overflow:"),
        "unexpected error: {message}"
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

#[test]
#[cfg(feature = "snap")]
fn test_arrow_rs_gh_45185_dict_levels() {
    let err = read_file("ARROW-GH-45185.parquet").unwrap_err();
    assert_eq!(
        err.to_string(),
        "External: Parquet argument error: Parquet error: first repetition level of batch must be 0"
    );
}

#[test]
fn test_arrow_gh_47662() {
    let err = read_file("ARROW-GH-47662.parquet").unwrap_err();
    assert_eq!(
        err.to_string(),
        "External: Parquet argument error: Parquet error: Invalid FIXED_LEN_BYTE_ARRAY PLAIN data page payload length: expected 400 bytes (100 values of 4 bytes), got 364"
    );
}

/// A primitive column whose data page claims dictionary encoding but whose
/// dictionary page is missing (its header type is mutated to INDEX_PAGE, so
/// the page reader skips it) must error on every read attempt instead of
/// panicking, matching the byte-array readers.
///
/// The file is generated and mutated in memory because it cannot be
/// constructed without breaking a valid file.
#[test]
fn test_arrow_rs_gh_12129_missing_dictionary_page() {
    use arrow_array::{Int32Array, RecordBatch};
    use parquet::arrow::ArrowWriter;
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use std::sync::Arc;

    let column = Int32Array::from(vec![7, 8, 7, 8]);
    let batch = RecordBatch::try_from_iter([("i", Arc::new(column) as _)]).unwrap();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let reader = SerializedFileReader::new(Bytes::from(bytes.clone())).unwrap();
    let dict_offset = reader
        .metadata()
        .row_group(0)
        .column(0)
        .dictionary_page_offset()
        .expect("writer must emit a dictionary page for dictionary-encoded data")
        as usize;
    // In the compact protocol the PageHeader starts with field 1 (type, i32):
    // 0x15, then zigzag(DICTIONARY_PAGE = 2) = 0x04.
    assert_eq!(
        &bytes[dict_offset..dict_offset + 2],
        &[0x15, 0x04],
        "dictionary page header"
    );
    bytes[dict_offset + 1] = 0x02; // zigzag(INDEX_PAGE = 1)

    let mut reader = ArrowReaderBuilder::try_new(Bytes::from(bytes))
        .unwrap()
        .build()
        .unwrap();
    let err = reader.next().unwrap().unwrap_err().to_string();
    assert!(
        err.contains("missing dictionary page for column"),
        "unexpected error: {err}"
    );
    // A retry must also error instead of panicking. The iterator errors on
    // every attempt and never ends on its own, so pin further attempts
    // rather than asserting natural termination.
    for _ in 0..2 {
        let err = reader.next().unwrap().unwrap_err().to_string();
        assert!(
            err.contains("the current data page failed to decode"),
            "unexpected error: {err}"
        );
    }
}

/// Reads the file and tries to return the total row count
/// Returns an error if the file is invalid
fn read_file(name: &str) -> Result<usize, ParquetError> {
    let path = bad_data_dir().join(name);
    println!("Reading file: {path:?}");

    let file = std::fs::File::open(&path).unwrap();
    let reader = ArrowReaderBuilder::try_new(file)?.build()?;

    let mut num_rows = 0;
    for batch in reader {
        let batch = batch?;
        num_rows += batch.num_rows();
    }
    Ok(num_rows)
}

#[cfg_attr(miri, ignore)] // calls native Zstd code unsupported by Miri
#[test]
fn non_standard_delta_blocks() {
    let file = Bytes::from_static(include_bytes!("bigdelta.parquet"));
    use parquet::arrow::arrow_reader::{RowSelection, RowSelector};

    let selectors = vec![RowSelector::skip(1000), RowSelector::select(5)];

    let selection: RowSelection = selectors.into();
    let reader = ArrowReaderBuilder::try_new(file.clone())
        .unwrap()
        .with_row_selection(selection)
        .build()
        .unwrap();

    let selected = concat_batches(
        &reader.schema(),
        &reader.collect::<Result<Vec<_>, _>>().unwrap(),
    )
    .unwrap();
    assert_eq!(selected.num_rows(), 5);

    let reader = ArrowReaderBuilder::try_new(file).unwrap().build().unwrap();
    let all = concat_batches(
        &reader.schema(),
        &reader.collect::<Result<Vec<_>, _>>().unwrap(),
    )
    .unwrap();
    assert_eq!(selected, all.slice(1000, 5));
}

#[test]
fn skip_unknown_types() {
    // test file contains a FileMetaData with unknown fields with
    // types not currently used by Parquet (uuid, set, map). The
    // parser should be able to skip these unknown fields without
    // erroring.
    let file = Bytes::from_static(include_bytes!("new_types.bin"));
    ParquetMetaDataReader::decode_metadata(&file).unwrap();
}

#[cfg(feature = "async")]
#[tokio::test]
async fn bad_metadata_err() {
    use parquet::file::metadata::{PageIndexPolicy, ParquetMetaDataReader};

    let metadata_buffer = Bytes::from_static(include_bytes!("bad_raw_metadata.bin"));

    let metadata_length = metadata_buffer.len() as u64;

    let mut reader = std::io::Cursor::new(&metadata_buffer);
    let mut loader = ParquetMetaDataReader::new();
    loader.try_load(&mut reader, metadata_length).await.unwrap();
    loader = loader.with_page_index_policy(PageIndexPolicy::Skip);
    loader.load_page_index(&mut reader).await.unwrap();

    loader = loader.with_offset_index_policy(PageIndexPolicy::Required);
    loader.load_page_index(&mut reader).await.unwrap();

    loader = loader.with_column_index_policy(PageIndexPolicy::Required);
    let err = loader.load_page_index(&mut reader).await.unwrap_err();

    assert_eq!(
        err.to_string(),
        "Parquet error: error converting value, expected 4 bytes got 0"
    );
}
