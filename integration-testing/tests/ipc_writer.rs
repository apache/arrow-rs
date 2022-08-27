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
fn read_and_rewrite_generated_files_014() {
    let testdata = arrow_test_data();
    let version = "0.14.1";
    // the test is repetitive, thus we can read all supported files at once
    let paths = vec![
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
        let file = File::open(format!(
            "{}/arrow-ipc-stream/integration/{}/{}.arrow_file",
            testdata, version, path
        ))
        .unwrap();

        let mut reader = FileReader::try_new(file, None).unwrap();

        let mut file = tempfile::tempfile().unwrap();

        // read and rewrite the file to a temp location
        {
            let mut writer = FileWriter::try_new(&mut file, &reader.schema()).unwrap();
            while let Some(Ok(batch)) = reader.next() {
                writer.write(&batch).unwrap();
            }
            writer.finish().unwrap();
        }
        file.rewind().unwrap();

        let mut reader = FileReader::try_new(file, None).unwrap();

        // read expected JSON output
        let arrow_json = read_gzip_json(version, path);
        assert!(arrow_json.equals_reader(&mut reader).unwrap());
    });
}

#[test]
fn read_and_rewrite_generated_streams_014() {
    let testdata = arrow_test_data();
    let version = "0.14.1";
    // the test is repetitive, thus we can read all supported files at once
    let paths = vec![
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
        let file = File::open(format!(
            "{}/arrow-ipc-stream/integration/{}/{}.stream",
            testdata, version, path
        ))
        .unwrap();

        let reader = StreamReader::try_new(file, None).unwrap();

        let mut file = tempfile::tempfile().unwrap();

        // read and rewrite the stream to a temp location
        {
            let mut writer = StreamWriter::try_new(&mut file, &reader.schema()).unwrap();
            reader.for_each(|batch| {
                writer.write(&batch.unwrap()).unwrap();
            });
            writer.finish().unwrap();
        }

        file.rewind().unwrap();
        let mut reader = StreamReader::try_new(file, None).unwrap();

        // read expected JSON output
        let arrow_json = read_gzip_json(version, path);
        assert!(arrow_json.equals_reader(&mut reader).unwrap());
    });
}

#[test]
fn read_and_rewrite_generated_files_100() {
    let testdata = arrow_test_data();
    let version = "1.0.0-littleendian";
    // the test is repetitive, thus we can read all supported files at once
    let paths = vec![
        "generated_custom_metadata",
        "generated_datetime",
        "generated_dictionary_unsigned",
        "generated_dictionary",
        // "generated_duplicate_fieldnames",
        "generated_interval",
        "generated_map",
        "generated_nested",
        // "generated_nested_large_offsets",
        "generated_null_trivial",
        "generated_null",
        "generated_primitive_large_offsets",
        "generated_primitive_no_batches",
        "generated_primitive_zerolength",
        "generated_primitive",
        // "generated_recursive_nested",
    ];
    paths.iter().for_each(|path| {
        let file = File::open(format!(
            "{}/arrow-ipc-stream/integration/{}/{}.arrow_file",
            testdata, version, path
        ))
        .unwrap();

        let mut reader = FileReader::try_new(file, None).unwrap();

        let mut file = tempfile::tempfile().unwrap();

        // read and rewrite the file to a temp location
        {
            // write IPC version 5
            let options =
                IpcWriteOptions::try_new(8, false, ipc::MetadataVersion::V5).unwrap();
            let mut writer =
                FileWriter::try_new_with_options(&mut file, &reader.schema(), options)
                    .unwrap();
            while let Some(Ok(batch)) = reader.next() {
                writer.write(&batch).unwrap();
            }
            writer.finish().unwrap();
        }

        file.rewind().unwrap();
        let mut reader = FileReader::try_new(file, None).unwrap();

        // read expected JSON output
        let arrow_json = read_gzip_json(version, path);
        assert!(arrow_json.equals_reader(&mut reader).unwrap());
    });
}

#[test]
fn read_and_rewrite_generated_streams_100() {
    let testdata = arrow_test_data();
    let version = "1.0.0-littleendian";
    // the test is repetitive, thus we can read all supported files at once
    let paths = vec![
        "generated_custom_metadata",
        "generated_datetime",
        "generated_dictionary_unsigned",
        "generated_dictionary",
        // "generated_duplicate_fieldnames",
        "generated_interval",
        "generated_map",
        "generated_nested",
        // "generated_nested_large_offsets",
        "generated_null_trivial",
        "generated_null",
        "generated_primitive_large_offsets",
        "generated_primitive_no_batches",
        "generated_primitive_zerolength",
        "generated_primitive",
        // "generated_recursive_nested",
    ];
    paths.iter().for_each(|path| {
        let file = File::open(format!(
            "{}/arrow-ipc-stream/integration/{}/{}.stream",
            testdata, version, path
        ))
        .unwrap();

        let reader = StreamReader::try_new(file, None).unwrap();

        let mut file = tempfile::tempfile().unwrap();

        // read and rewrite the stream to a temp location
        {
            let options =
                IpcWriteOptions::try_new(8, false, ipc::MetadataVersion::V5).unwrap();
            let mut writer =
                StreamWriter::try_new_with_options(&mut file, &reader.schema(), options)
                    .unwrap();
            reader.for_each(|batch| {
                writer.write(&batch.unwrap()).unwrap();
            });
            writer.finish().unwrap();
        }

        file.rewind().unwrap();

        let mut reader = StreamReader::try_new(file, None).unwrap();

        // read expected JSON output
        let arrow_json = read_gzip_json(version, path);
        assert!(arrow_json.equals_reader(&mut reader).unwrap());
    });
}

#[test]
fn read_and_rewrite_compression_files_200() {
    let testdata = arrow_test_data();
    let version = "2.0.0-compression";
    // the test is repetitive, thus we can read all supported files at once
    let paths = vec!["generated_lz4", "generated_zstd"];
    paths.iter().for_each(|path| {
        let file = File::open(format!(
            "{}/arrow-ipc-stream/integration/{}/{}.arrow_file",
            testdata, version, path
        ))
        .unwrap();

        let mut reader = FileReader::try_new(file, None).unwrap();

        let mut file = tempfile::tempfile().unwrap();

        // read and rewrite the file to a temp location
        {
            // write IPC version 5
            let options = IpcWriteOptions::try_new(8, false, ipc::MetadataVersion::V5)
                .unwrap()
                .try_with_compression(Some(ipc::CompressionType::LZ4_FRAME))
                .unwrap();

            let mut writer =
                FileWriter::try_new_with_options(&mut file, &reader.schema(), options)
                    .unwrap();
            while let Some(Ok(batch)) = reader.next() {
                writer.write(&batch).unwrap();
            }
            writer.finish().unwrap();
        }

        file.rewind().unwrap();
        let mut reader = FileReader::try_new(file, None).unwrap();

        // read expected JSON output
        let arrow_json = read_gzip_json(version, path);
        assert!(arrow_json.equals_reader(&mut reader).unwrap());
    });
}

#[test]
fn read_and_rewrite_compression_stream_200() {
    let testdata = arrow_test_data();
    let version = "2.0.0-compression";
    // the test is repetitive, thus we can read all supported files at once
    let paths = vec!["generated_lz4", "generated_zstd"];
    paths.iter().for_each(|path| {
        let file = File::open(format!(
            "{}/arrow-ipc-stream/integration/{}/{}.stream",
            testdata, version, path
        ))
        .unwrap();

        let reader = StreamReader::try_new(file, None).unwrap();

        let mut file = tempfile::tempfile().unwrap();

        // read and rewrite the stream to a temp location
        {
            let options = IpcWriteOptions::try_new(8, false, ipc::MetadataVersion::V5)
                .unwrap()
                .try_with_compression(Some(ipc::CompressionType::ZSTD))
                .unwrap();

            let mut writer =
                StreamWriter::try_new_with_options(&mut file, &reader.schema(), options)
                    .unwrap();
            reader.for_each(|batch| {
                writer.write(&batch.unwrap()).unwrap();
            });
            writer.finish().unwrap();
        }

        file.rewind().unwrap();

        let mut reader = StreamReader::try_new(file, None).unwrap();

        // read expected JSON output
        let arrow_json = read_gzip_json(version, path);
        assert!(arrow_json.equals_reader(&mut reader).unwrap());
    });
}
