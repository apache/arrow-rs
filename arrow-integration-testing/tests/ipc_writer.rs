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

use arrow::ipc;
use arrow::ipc::reader::{FileReader, StreamReader};
use arrow::ipc::writer::{FileWriter, IpcWriteOptions, StreamWriter};
use arrow::util::test_util::arrow_test_data;
use arrow_integration_testing::read_gzip_json;
use std::fs::File;
use std::io::Seek;

#[test]
fn write_0_1_4() {
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
        roundtrip_arrow_file(&testdata, version, path);
        roundtrip_arrow_stream(&testdata, version, path);
    });
}

#[test]
fn write_0_1_7() {
    let testdata = arrow_test_data();
    let version = "0.17.1";
    let paths = ["generated_union"];
    paths.iter().for_each(|path| {
        roundtrip_arrow_file(&testdata, version, path);
        roundtrip_arrow_stream(&testdata, version, path);
    });
}

#[test]
fn write_1_0_0_littleendian() {
    let testdata = arrow_test_data();
    let version = "1.0.0-littleendian";
    let paths = [
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
        // "generated_map_non_canonical",
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
        roundtrip_arrow_file(&testdata, version, path);
        roundtrip_arrow_stream(&testdata, version, path);
    });
}

#[test]
fn write_2_0_0_compression() {
    let testdata = arrow_test_data();
    let version = "2.0.0-compression";
    let paths = ["generated_lz4", "generated_zstd"];

    // writer options for each compression type
    let all_options = [
        IpcWriteOptions::try_new(8, false, ipc::MetadataVersion::V5)
            .unwrap()
            .try_with_compression(Some(ipc::CompressionType::LZ4_FRAME))
            .unwrap(),
        // write IPC version 5 with zstd
        IpcWriteOptions::try_new(8, false, ipc::MetadataVersion::V5)
            .unwrap()
            .try_with_compression(Some(ipc::CompressionType::ZSTD))
            .unwrap(),
    ];

    paths.iter().for_each(|path| {
        for options in &all_options {
            println!("Using options {options:?}");
            roundtrip_arrow_file_with_options(&testdata, version, path, options.clone());
            roundtrip_arrow_stream_with_options(&testdata, version, path, options.clone());
        }
    });
}

/// Verifies the arrow file writer by reading the contents of an
/// arrow_file, writing it to a file, and then ensuring the contents
/// match the expected json contents. It also verifies that
/// RecordBatches read from the new file matches the original.
///
/// Input file:
/// `arrow-ipc-stream/integration/<version>/<path>.arrow_file
///
/// Verification json file
/// `arrow-ipc-stream/integration/<version>/<path>.json.gz
fn roundtrip_arrow_file(testdata: &str, version: &str, path: &str) {
    roundtrip_arrow_file_with_options(testdata, version, path, IpcWriteOptions::default())
}

fn roundtrip_arrow_file_with_options(
    testdata: &str,
    version: &str,
    path: &str,
    options: IpcWriteOptions,
) {
    let filename = format!("{testdata}/arrow-ipc-stream/integration/{version}/{path}.arrow_file");
    println!("Verifying {filename}");

    let mut tempfile = tempfile::tempfile().unwrap();

    {
        println!("  writing to tempfile {tempfile:?}");
        let file = File::open(&filename).unwrap();
        let mut reader = FileReader::try_new(file, None).unwrap();

        // read and rewrite the file to a temp location
        {
            let mut writer =
                FileWriter::try_new_with_options(&mut tempfile, &reader.schema(), options).unwrap();
            while let Some(Ok(batch)) = reader.next() {
                writer.write(&batch).unwrap();
            }
            writer.finish().unwrap();
        }
    }

    {
        println!("  checking rewrite to with json");
        tempfile.rewind().unwrap();
        let mut reader = FileReader::try_new(&tempfile, None).unwrap();

        let arrow_json = read_gzip_json(version, path);
        assert!(arrow_json.equals_reader(&mut reader).unwrap());
    }

    {
        println!("  checking rewrite with original");
        let file = File::open(&filename).unwrap();
        let reader = FileReader::try_new(file, None).unwrap();

        tempfile.rewind().unwrap();
        let rewrite_reader = FileReader::try_new(&tempfile, None).unwrap();

        // Compare to original reader
        reader
            .into_iter()
            .zip(rewrite_reader)
            .for_each(|(batch1, batch2)| {
                assert_eq!(batch1.unwrap(), batch2.unwrap());
            });
    }
}

/// Verifies the arrow file writer by reading the contents of an
/// arrow_file, writing it to a file, and then ensuring the contents
/// match the expected json contents. It also verifies that
/// RecordBatches read from the new file matches the original.
///
/// Input file:
/// `arrow-ipc-stream/integration/<version>/<path>.stream
///
/// Verification json file
/// `arrow-ipc-stream/integration/<version>/<path>.json.gz
fn roundtrip_arrow_stream(testdata: &str, version: &str, path: &str) {
    roundtrip_arrow_stream_with_options(testdata, version, path, IpcWriteOptions::default())
}

fn roundtrip_arrow_stream_with_options(
    testdata: &str,
    version: &str,
    path: &str,
    options: IpcWriteOptions,
) {
    let filename = format!("{testdata}/arrow-ipc-stream/integration/{version}/{path}.stream");
    println!("Verifying {filename}");

    let mut tempfile = tempfile::tempfile().unwrap();

    {
        println!("  writing to tempfile {tempfile:?}");
        let file = File::open(&filename).unwrap();
        let mut reader = StreamReader::try_new(file, None).unwrap();

        // read and rewrite the file to a temp location
        {
            let mut writer =
                StreamWriter::try_new_with_options(&mut tempfile, &reader.schema(), options)
                    .unwrap();
            while let Some(Ok(batch)) = reader.next() {
                writer.write(&batch).unwrap();
            }
            writer.finish().unwrap();
        }
    }

    {
        println!("  checking rewrite to with json");
        tempfile.rewind().unwrap();
        let mut reader = StreamReader::try_new(&tempfile, None).unwrap();

        let arrow_json = read_gzip_json(version, path);
        assert!(arrow_json.equals_reader(&mut reader).unwrap());
    }

    {
        println!("  checking rewrite with original");
        let file = File::open(&filename).unwrap();
        let reader = StreamReader::try_new(file, None).unwrap();

        tempfile.rewind().unwrap();
        let rewrite_reader = StreamReader::try_new(&tempfile, None).unwrap();

        // Compare to original reader
        reader
            .into_iter()
            .zip(rewrite_reader)
            .for_each(|(batch1, batch2)| {
                assert_eq!(batch1.unwrap(), batch2.unwrap());
            });
    }
}
