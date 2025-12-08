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

//! Tests for reading the content of  [`FileReader`] and [`StreamReader`]
//! in `testing/arrow-ipc-stream/integration/...`

use arrow::error::ArrowError;
use arrow::ipc::reader::{FileReader, StreamDecoder, StreamReader};
use arrow::util::test_util::arrow_test_data;
use arrow_buffer::Buffer;
use arrow_integration_testing::read_gzip_json;
use std::fs::File;
use std::io::Read;

#[test]
fn read_0_1_4() {
    let testdata = arrow_test_data();
    let version = "0.14.1";
    let paths = [
        "generated_interval",
        "generated_datetime",
        "generated_dictionary",
        "generated_map",
        "generated_nested",
        "generated_primitive_no_batches",
        "generated_primitive_zerolength",
        "generated_primitive",
        "generated_decimal",
    ];
    paths.iter().for_each(|path| {
        verify_arrow_file(&testdata, version, path);
        verify_arrow_stream(&testdata, version, path);
    });
}

#[test]
fn read_0_1_7() {
    let testdata = arrow_test_data();
    let version = "0.17.1";
    let paths = ["generated_union"];
    paths.iter().for_each(|path| {
        verify_arrow_file(&testdata, version, path);
        verify_arrow_stream(&testdata, version, path);
    });
}

#[test]
fn read_1_0_0_bigendian() {
    let testdata = arrow_test_data();
    let paths = [
        "generated_decimal",
        "generated_dictionary",
        "generated_interval",
        "generated_datetime",
        "generated_map",
        "generated_nested",
        "generated_null_trivial",
        "generated_null",
        "generated_primitive_no_batches",
        "generated_primitive_zerolength",
        "generated_primitive",
    ];
    paths.iter().for_each(|path| {
        let file = File::open(format!(
            "{testdata}/arrow-ipc-stream/integration/1.0.0-bigendian/{path}.arrow_file"
        ))
        .unwrap();

        let reader = FileReader::try_new(file, None);

        assert!(reader.is_err());
        let err = reader.err().unwrap();
        assert!(matches!(err, ArrowError::IpcError(_)));
        assert_eq!(err.to_string(), "Ipc error: the endianness of the source system does not match the endianness of the target system.");
    });
}

#[test]
fn read_1_0_0_littleendian() {
    let testdata = arrow_test_data();
    let version = "1.0.0-littleendian";
    let paths = vec![
        "generated_datetime",
        "generated_custom_metadata",
        "generated_decimal",
        "generated_decimal256",
        "generated_dictionary",
        "generated_dictionary_unsigned",
        "generated_duplicate_fieldnames",
        "generated_extension",
        "generated_interval",
        "generated_map",
        // https://github.com/apache/arrow-rs/issues/3460
        //"generated_map_non_canonical",
        "generated_nested",
        "generated_nested_dictionary",
        "generated_nested_large_offsets",
        "generated_null",
        "generated_null_trivial",
        "generated_primitive",
        "generated_primitive_large_offsets",
        "generated_primitive_no_batches",
        "generated_primitive_zerolength",
        "generated_recursive_nested",
        "generated_union",
    ];
    paths.iter().for_each(|path| {
        verify_arrow_file(&testdata, version, path);
        verify_arrow_stream(&testdata, version, path);
    });
}

#[test]
fn read_2_0_0_compression() {
    let testdata = arrow_test_data();
    let version = "2.0.0-compression";

    // the test is repetitive, thus we can read all supported files at once
    let paths = ["generated_lz4", "generated_zstd"];
    paths.iter().for_each(|path| {
        verify_arrow_file(&testdata, version, path);
        verify_arrow_stream(&testdata, version, path);
    });
}

/// Verifies the arrow file format integration test
///
/// Input file:
/// `arrow-ipc-stream/integration/<version>/<path>.arrow_file
///
/// Verification json file
/// `arrow-ipc-stream/integration/<version>/<path>.json.gz
fn verify_arrow_file(testdata: &str, version: &str, path: &str) {
    let filename = format!("{testdata}/arrow-ipc-stream/integration/{version}/{path}.arrow_file");
    println!("Verifying {filename}");

    // Compare contents to the expected output format in JSON
    {
        println!("  verifying content");
        let file = File::open(&filename).unwrap();
        let mut reader = FileReader::try_new(file, None).unwrap();

        // read expected JSON output
        let arrow_json = read_gzip_json(version, path);
        assert!(arrow_json.equals_reader(&mut reader).unwrap());
    }

    // Verify that projection works by selecting the first column
    {
        println!("  verifying projection");
        let file = File::open(&filename).unwrap();
        let reader = FileReader::try_new(file, Some(vec![0])).unwrap();
        let datatype_0 = reader.schema().fields()[0].data_type().clone();
        reader.for_each(|batch| {
            let batch = batch.unwrap();
            assert_eq!(batch.columns().len(), 1);
            assert_eq!(datatype_0, batch.schema().fields()[0].data_type().clone());
        });
    }
}

/// Verifies the arrow stream integration test
///
/// Input file:
/// `arrow-ipc-stream/integration/<version>/<path>.stream
///
/// Verification json file
/// `arrow-ipc-stream/integration/<version>/<path>.json.gz
fn verify_arrow_stream(testdata: &str, version: &str, path: &str) {
    let filename = format!("{testdata}/arrow-ipc-stream/integration/{version}/{path}.stream");
    println!("Verifying {filename}");

    // read expected JSON output
    let arrow_json = read_gzip_json(version, path);

    // Compare contents to the expected output format in JSON
    {
        println!("  verifying content");
        let file = File::open(&filename).unwrap();
        let mut reader = StreamReader::try_new(file, None).unwrap();

        assert!(arrow_json.equals_reader(&mut reader).unwrap());
        // the next batch must be empty
        assert!(reader.next().is_none());
        // the stream must indicate that it's finished
        assert!(reader.is_finished());
    }

    // Test stream decoder
    let expected = arrow_json.get_record_batches().unwrap();
    for chunk_sizes in [1, 2, 8, 123] {
        let mut decoder = StreamDecoder::new();
        let stream = chunked_file(&filename, chunk_sizes);
        let mut actual = Vec::with_capacity(expected.len());
        for mut x in stream {
            while !x.is_empty() {
                if let Some(x) = decoder.decode(&mut x).unwrap() {
                    actual.push(x);
                }
            }
        }
        decoder.finish().unwrap();
        assert_eq!(expected, actual);
    }
}

fn chunked_file(filename: &str, chunk_size: u64) -> impl Iterator<Item = Buffer> {
    let mut file = File::open(filename).unwrap();
    std::iter::from_fn(move || {
        let mut buf = vec![];
        let read = (&mut file).take(chunk_size).read_to_end(&mut buf).unwrap();
        (read != 0).then(|| Buffer::from_vec(buf))
    })
}
