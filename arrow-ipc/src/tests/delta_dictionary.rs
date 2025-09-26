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

use crate::{
    reader::IpcMessage,
    writer::{DictionaryHandling, IpcWriteOptions, StreamWriter},
};
use crate::{
    reader::{FileReader, StreamReader},
    writer::FileWriter,
};
use arrow_array::{
    Array, ArrayRef, DictionaryArray, RecordBatch, StringArray, builder::StringDictionaryBuilder,
    types::Int32Type,
};
use arrow_schema::{DataType, Field, Schema};
use std::io::Cursor;
use std::sync::Arc;

#[test]
fn test_zero_row_dict() {
    let batches: &[&[&str]] = &[&[], &["A"], &[], &["B", "C"], &[]];
    run_delta_sequence_test(
        batches,
        &[
            MessageType::Dict(vec![]),
            MessageType::RecordBatch,
            MessageType::DeltaDict(str_vec(&["A"])),
            MessageType::RecordBatch,
            MessageType::RecordBatch,
            MessageType::DeltaDict(str_vec(&["B", "C"])),
            MessageType::RecordBatch,
        ],
    );

    run_resend_sequence_test(
        batches,
        &[
            MessageType::Dict(vec![]),
            MessageType::RecordBatch,
            MessageType::Dict(str_vec(&["A"])),
            MessageType::RecordBatch,
            MessageType::RecordBatch,
            MessageType::Dict(str_vec(&["A", "B", "C"])),
            MessageType::RecordBatch,
        ],
    );
}

#[test]
fn test_mixed_delta() {
    let batches: &[&[&str]] = &[
        &["A"],
        &["A", "B"],
        &["C"],
        &["D", "E"],
        &["A", "B", "C", "D", "E"],
    ];

    run_delta_sequence_test(
        batches,
        &[
            MessageType::Dict(str_vec(&["A"])),
            MessageType::RecordBatch,
            MessageType::DeltaDict(str_vec(&["B"])),
            MessageType::RecordBatch,
            MessageType::DeltaDict(str_vec(&["C"])),
            MessageType::RecordBatch,
            MessageType::DeltaDict(str_vec(&["D", "E"])),
            MessageType::RecordBatch,
            MessageType::RecordBatch,
        ],
    );

    run_resend_sequence_test(
        batches,
        &[
            MessageType::Dict(str_vec(&["A"])),
            MessageType::RecordBatch,
            MessageType::Dict(str_vec(&["A", "B"])),
            MessageType::RecordBatch,
            MessageType::Dict(str_vec(&["A", "B", "C"])),
            MessageType::RecordBatch,
            MessageType::Dict(str_vec(&["A", "B", "C", "D", "E"])),
            MessageType::RecordBatch,
            MessageType::RecordBatch,
        ],
    );
}

#[test]
fn test_disjoint_delta() {
    let batches: &[&[&str]] = &[&["A"], &["B"], &["C", "E"]];
    run_delta_sequence_test(
        batches,
        &[
            MessageType::Dict(str_vec(&["A"])),
            MessageType::RecordBatch,
            MessageType::DeltaDict(str_vec(&["B"])),
            MessageType::RecordBatch,
            MessageType::DeltaDict(str_vec(&["C", "E"])),
            MessageType::RecordBatch,
        ],
    );

    run_resend_sequence_test(
        batches,
        &[
            MessageType::Dict(str_vec(&["A"])),
            MessageType::RecordBatch,
            MessageType::Dict(str_vec(&["A", "B"])),
            MessageType::RecordBatch,
            MessageType::Dict(str_vec(&["A", "B", "C", "E"])),
            MessageType::RecordBatch,
        ],
    );
}

#[test]
fn test_increasing_delta() {
    let batches: &[&[&str]] = &[&["A"], &["A", "B"], &["A", "B", "C"]];
    run_delta_sequence_test(
        batches,
        &[
            MessageType::Dict(str_vec(&["A"])),
            MessageType::RecordBatch,
            MessageType::DeltaDict(str_vec(&["B"])),
            MessageType::RecordBatch,
            MessageType::DeltaDict(str_vec(&["C"])),
            MessageType::RecordBatch,
        ],
    );

    run_resend_sequence_test(
        batches,
        &[
            MessageType::Dict(str_vec(&["A"])),
            MessageType::RecordBatch,
            MessageType::Dict(str_vec(&["A", "B"])),
            MessageType::RecordBatch,
            MessageType::Dict(str_vec(&["A", "B", "C"])),
            MessageType::RecordBatch,
        ],
    );
}

#[test]
fn test_single_delta() {
    let batches: &[&[&str]] = &[&["A", "B", "C"], &["D"]];
    run_delta_sequence_test(
        batches,
        &[
            MessageType::Dict(str_vec(&["A", "B", "C"])),
            MessageType::RecordBatch,
            MessageType::DeltaDict(str_vec(&["D"])),
            MessageType::RecordBatch,
        ],
    );

    run_resend_sequence_test(
        batches,
        &[
            MessageType::Dict(str_vec(&["A", "B", "C"])),
            MessageType::RecordBatch,
            MessageType::Dict(str_vec(&["A", "B", "C", "D"])),
            MessageType::RecordBatch,
        ],
    );
}

#[test]
fn test_single_same_value_sequence() {
    let batches: &[&[&str]] = &[&["A"], &["A"], &["A"], &["A"]];
    run_delta_sequence_test(
        batches,
        &[
            MessageType::Dict(str_vec(&["A"])),
            MessageType::RecordBatch,
            MessageType::RecordBatch,
            MessageType::RecordBatch,
            MessageType::RecordBatch,
        ],
    );

    run_resend_sequence_test(
        batches,
        &[
            MessageType::Dict(str_vec(&["A"])),
            MessageType::RecordBatch,
            MessageType::RecordBatch,
            MessageType::RecordBatch,
            MessageType::RecordBatch,
        ],
    );
}

fn str_vec(strings: &[&str]) -> Vec<String> {
    strings.iter().map(|s| s.to_string()).collect()
}

#[test]
fn test_multi_same_value_sequence() {
    let batches: &[&[&str]] = &[&["A", "B", "C"], &["A", "B", "C"]];
    run_delta_sequence_test(
        batches,
        &[
            MessageType::Dict(str_vec(&["A", "B", "C"])),
            MessageType::RecordBatch,
        ],
    );
}

#[derive(Debug, PartialEq)]
enum MessageType {
    Schema,
    Dict(Vec<String>),
    DeltaDict(Vec<String>),
    RecordBatch,
}

fn run_resend_sequence_test(batches: &[&[&str]], sequence: &[MessageType]) {
    let opts = IpcWriteOptions::default().with_dictionary_handling(DictionaryHandling::Resend);
    run_sequence_test(batches, sequence, opts);
}

fn run_delta_sequence_test(batches: &[&[&str]], sequence: &[MessageType]) {
    let opts = IpcWriteOptions::default().with_dictionary_handling(DictionaryHandling::Delta);
    run_sequence_test(batches, sequence, opts);
}

fn run_sequence_test(batches: &[&[&str]], sequence: &[MessageType], options: IpcWriteOptions) {
    let stream_buf = write_all_to_stream(options.clone(), batches);
    let ipc_stream = get_ipc_message_stream(stream_buf);
    for (message, expected) in ipc_stream.iter().zip(sequence.iter()) {
        match message {
            IpcMessage::Schema(_) => {
                assert_eq!(expected, &MessageType::Schema, "Expected schema message");
            }
            IpcMessage::RecordBatch(_) => {
                assert_eq!(
                    expected,
                    &MessageType::RecordBatch,
                    "Expected record batch message"
                );
            }
            IpcMessage::DictionaryBatch {
                id: _,
                is_delta,
                values,
            } => {
                let expected_values = if *is_delta {
                    let MessageType::DeltaDict(values) = expected else {
                        panic!("Expected DeltaDict message type");
                    };

                    values
                } else {
                    let MessageType::Dict(values) = expected else {
                        panic!("Expected Dict message type");
                    };
                    values
                };

                let values: Vec<String> = values
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .map(|v| v.map(|s| s.to_string()).unwrap_or_default())
                    .collect();

                assert_eq!(*expected_values, values)
            }
        }
    }
}

fn get_ipc_message_stream(buf: Vec<u8>) -> Vec<IpcMessage> {
    let mut reader = StreamReader::try_new(Cursor::new(buf), None).unwrap();
    let mut results = vec![];

    loop {
        match reader.next_ipc_message() {
            Ok(Some(message)) => results.push(message),
            Ok(None) => break, // End of stream
            Err(e) => panic!("Error reading IPC message: {e:?}"),
        }
    }

    results
}

#[test]
fn test_replace_same_length() {
    let batches: &[&[&str]] = &[
        &["A", "B", "C", "D", "E", "F"],
        &["A", "G", "H", "I", "J", "K"],
    ];
    run_parity_test(batches);
}

#[test]
fn test_sparse_deltas() {
    let batches: &[&[&str]] = &[
        &["A"],
        &["C"],
        &["E", "F", "D"],
        &["FOO"],
        &["parquet", "B"],
        &["123", "B", "C"],
    ];
    run_parity_test(batches);
}

#[test]
fn test_deltas_with_reset() {
    // Dictionary resets at ["C", "D"]
    let batches: &[&[&str]] = &[&["A"], &["A", "B"], &["C", "D"], &["A", "B", "C", "D"]];
    run_parity_test(batches);
}

/// FileWriter can only tolerate very specific patterns of delta dictionaries,
/// because the dictionary cannot be replaced/reset.
#[test]
fn test_deltas_with_file() {
    let batches: &[&[&str]] = &[&["A"], &["A", "B"], &["A", "B", "C"], &["A", "B", "C", "D"]];
    run_parity_test(batches);
}

/// Encode all batches three times and compare all three for the same results
/// on the other end.
///
/// - Stream encoding with delta
/// - Stream encoding without delta
/// - File encoding with delta (File format does not allow replacement
///   dictionaries)
fn run_parity_test(batches: &[&[&str]]) {
    let delta_options =
        IpcWriteOptions::default().with_dictionary_handling(DictionaryHandling::Delta);
    let delta_stream_buf = write_all_to_stream(delta_options.clone(), batches);

    let resend_options =
        IpcWriteOptions::default().with_dictionary_handling(DictionaryHandling::Resend);
    let resend_stream_buf = write_all_to_stream(resend_options.clone(), batches);

    let delta_file_buf = write_all_to_file(delta_options, batches);

    let mut streams = [
        get_stream_batches(delta_stream_buf),
        get_stream_batches(resend_stream_buf),
        get_file_batches(delta_file_buf),
    ];

    let (first_stream, other_streams) = streams.split_first_mut().unwrap();

    for (idx, batch) in first_stream.by_ref().enumerate() {
        let first_dict = extract_dictionary(batch);
        let expected_values = batches[idx];
        assert_eq!(expected_values, &dict_to_vec(first_dict.clone()));

        for stream in other_streams.iter_mut() {
            let next_batch = stream
                .next()
                .expect("All streams should yield same number of elements");
            let next_dict = extract_dictionary(next_batch);
            assert_eq!(expected_values, &dict_to_vec(next_dict.clone()));
            assert_eq!(first_dict, next_dict);
        }
    }

    for stream in other_streams.iter_mut() {
        assert!(
            stream.next().is_none(),
            "All streams should yield same number of elements"
        );
    }
}

fn dict_to_vec(dict: DictionaryArray<Int32Type>) -> Vec<String> {
    dict.downcast_dict::<StringArray>()
        .unwrap()
        .into_iter()
        .map(|v| v.unwrap_or_default().to_string())
        .collect()
}

fn get_stream_batches(buf: Vec<u8>) -> Box<dyn Iterator<Item = RecordBatch>> {
    let reader = StreamReader::try_new(Cursor::new(buf), None).unwrap();
    Box::new(
        reader
            .collect::<Vec<Result<_, _>>>()
            .into_iter()
            .map(|r| r.unwrap()),
    )
}

fn get_file_batches(buf: Vec<u8>) -> Box<dyn Iterator<Item = RecordBatch>> {
    let reader = FileReader::try_new(Cursor::new(buf), None).unwrap();
    Box::new(
        reader
            .collect::<Vec<Result<_, _>>>()
            .into_iter()
            .map(|r| r.unwrap()),
    )
}

fn extract_dictionary(batch: RecordBatch) -> DictionaryArray<arrow_array::types::Int32Type> {
    batch
        .column(0)
        .as_any()
        .downcast_ref::<DictionaryArray<arrow_array::types::Int32Type>>()
        .unwrap()
        .clone()
}

fn write_all_to_file(options: IpcWriteOptions, vals: &[&[&str]]) -> Vec<u8> {
    let batches = build_batches(vals);
    let mut buf: Vec<u8> = Vec::new();
    let mut writer =
        FileWriter::try_new_with_options(&mut buf, &batches[0].schema(), options).unwrap();
    for batch in batches {
        writer.write(&batch).unwrap();
    }
    writer.finish().unwrap();
    buf
}

fn write_all_to_stream(options: IpcWriteOptions, vals: &[&[&str]]) -> Vec<u8> {
    let batches = build_batches(vals);

    let mut buf: Vec<u8> = Vec::new();
    let mut writer =
        StreamWriter::try_new_with_options(&mut buf, &batches[0].schema(), options).unwrap();
    for batch in batches {
        writer.write(&batch).unwrap();
    }

    writer.finish().unwrap();

    buf
}

fn build_batches(vals: &[&[&str]]) -> Vec<RecordBatch> {
    let mut builder = StringDictionaryBuilder::<arrow_array::types::Int32Type>::new();
    vals.iter().map(|v| build_batch(v, &mut builder)).collect()
}

fn build_batch(
    vals: &[&str],
    builder: &mut StringDictionaryBuilder<arrow_array::types::Int32Type>,
) -> RecordBatch {
    for &val in vals {
        builder.append_value(val);
    }

    let array = builder.finish_preserve_values();

    let schema = Arc::new(Schema::new(vec![Field::new(
        "dict",
        DataType::Dictionary(Box::from(DataType::Int32), Box::from(DataType::Utf8)),
        true,
    )]));

    RecordBatch::try_new(schema.clone(), vec![Arc::new(array) as ArrayRef]).unwrap()
}
