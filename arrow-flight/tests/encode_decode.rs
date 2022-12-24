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

//! Tests for round trip encoding / decoding

use std::sync::Arc;

use arrow::{compute::concat_batches, datatypes::Int32Type};
use arrow_array::{ArrayRef, DictionaryArray, Float64Array, RecordBatch, UInt8Array};
use arrow_flight::{
    decode::{DecodedPayload, FlightRecordBatchStream},
    encode::{
        prepare_batch_for_flight, prepare_schema_for_flight, FlightDataEncoderBuilder,
    },
    error::FlightError,
};
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};

#[tokio::test]
async fn test_empty() {
    roundtrip(vec![]).await;
}

#[tokio::test]
async fn test_empty_batch() {
    let batch = make_primative_batch(5);
    let empty = RecordBatch::new_empty(batch.schema());
    roundtrip(vec![empty]).await;
}

#[tokio::test]
async fn test_error() {
    let input_batch_stream =
        futures::stream::iter(vec![Err(FlightError::NotYetImplemented("foo".into()))]);

    let encoder = FlightDataEncoderBuilder::default();
    let encode_stream = encoder.build(input_batch_stream);

    let decode_stream = FlightRecordBatchStream::new_from_flight_data(encode_stream);
    let result: Result<Vec<_>, _> = decode_stream.try_collect().await;

    let result = result.unwrap_err();
    assert_eq!(result.to_string(), r#"NotYetImplemented("foo")"#);
}

#[tokio::test]
async fn test_primative_one() {
    roundtrip(vec![make_primative_batch(5)]).await;
}

#[tokio::test]
async fn test_primative_many() {
    roundtrip(vec![
        make_primative_batch(1),
        make_primative_batch(7),
        make_primative_batch(32),
    ])
    .await;
}

#[tokio::test]
async fn test_primative_empty() {
    let batch = make_primative_batch(5);
    let empty = RecordBatch::new_empty(batch.schema());

    roundtrip(vec![batch, empty]).await;
}

#[tokio::test]
async fn test_dictionary_one() {
    roundtrip_dictionary(vec![make_dictionary_batch(5)]).await;
}

#[tokio::test]
async fn test_dictionary_many() {
    roundtrip_dictionary(vec![
        make_dictionary_batch(5),
        make_dictionary_batch(9),
        make_dictionary_batch(5),
        make_dictionary_batch(5),
    ])
    .await;
}

#[tokio::test]
async fn test_app_metadata() {
    let input_batch_stream = futures::stream::iter(vec![Ok(make_primative_batch(78))]);

    let app_metadata = Bytes::from("My Metadata");
    let encoder = FlightDataEncoderBuilder::default().with_metadata(app_metadata.clone());

    let encode_stream = encoder.build(input_batch_stream);

    // use lower level stream to get access to app metadata
    let decode_stream =
        FlightRecordBatchStream::new_from_flight_data(encode_stream).into_inner();

    let mut messages: Vec<_> = decode_stream.try_collect().await.expect("encode fails");

    println!("{messages:#?}");

    // expect that the app metadata made it through on the schema message
    assert_eq!(messages.len(), 2);
    let message2 = messages.pop().unwrap();
    let message1 = messages.pop().unwrap();

    assert_eq!(message1.app_metadata(), app_metadata);
    assert!(matches!(message1.payload, DecodedPayload::Schema(_)));

    // but not on the data
    assert_eq!(message2.app_metadata(), Bytes::new());
    assert!(matches!(message2.payload, DecodedPayload::RecordBatch(_)));
}

#[tokio::test]
async fn test_max_message_size() {
    let input_batch_stream = futures::stream::iter(vec![Ok(make_primative_batch(5))]);

    // 5 input rows, with a very small limit should result in 5 batch messages
    let encoder = FlightDataEncoderBuilder::default().with_max_message_size(1);

    let encode_stream = encoder.build(input_batch_stream);

    // use lower level stream to get access to app metadata
    let decode_stream =
        FlightRecordBatchStream::new_from_flight_data(encode_stream).into_inner();

    let messages: Vec<_> = decode_stream.try_collect().await.expect("encode fails");

    println!("{messages:#?}");

    assert_eq!(messages.len(), 6);
    assert!(matches!(messages[0].payload, DecodedPayload::Schema(_)));
    for message in messages.iter().skip(1) {
        assert!(matches!(message.payload, DecodedPayload::RecordBatch(_)));
    }
}

#[tokio::test]
async fn test_max_message_size_fuzz() {
    // send through batches of varying sizes with various max
    // batch sizes and ensure the data gets through ok
    let input = vec![
        make_primative_batch(123),
        make_primative_batch(17),
        make_primative_batch(201),
        make_primative_batch(2),
        make_primative_batch(1),
        make_primative_batch(11),
        make_primative_batch(127),
    ];

    for max_message_size in [10, 1024, 2048, 6400, 3211212] {
        let encoder =
            FlightDataEncoderBuilder::default().with_max_message_size(max_message_size);

        let input_batch_stream = futures::stream::iter(input.clone()).map(Ok);

        let encode_stream = encoder.build(input_batch_stream);

        let decode_stream = FlightRecordBatchStream::new_from_flight_data(encode_stream);
        let output: Vec<_> = decode_stream.try_collect().await.expect("encode / decode");

        let input_batch = concat_batches(&input[0].schema(), &input).unwrap();
        let output_batch = concat_batches(&output[0].schema(), &output).unwrap();
        assert_eq!(input_batch, output_batch);
    }
}

/// Make a primtive batch for testing
///
/// Example:
/// i: 0, 1, None, 3, 4
/// f: 5.0, 4.0, None, 2.0, 1.0
fn make_primative_batch(num_rows: usize) -> RecordBatch {
    let i: UInt8Array = (0..num_rows)
        .map(|i| {
            if i == num_rows / 2 {
                None
            } else {
                Some(i.try_into().unwrap())
            }
        })
        .collect();

    let f: Float64Array = (0..num_rows)
        .map(|i| {
            if i == num_rows / 2 {
                None
            } else {
                Some((num_rows - i) as f64)
            }
        })
        .collect();

    RecordBatch::try_from_iter(vec![("i", Arc::new(i) as ArrayRef), ("f", Arc::new(f))])
        .unwrap()
}

fn make_dictionary_batch(num_rows: usize) -> RecordBatch {
    let values: Vec<_> = (0..num_rows)
        .map(|i| {
            if i == i / 2 {
                None
            } else {
                // repeat some values for low cardinality
                let v = i / 3;
                Some(format!("value{v}"))
            }
        })
        .collect();

    let a: DictionaryArray<Int32Type> = values
        .iter()
        .map(|s| s.as_ref().map(|s| s.as_str()))
        .collect();

    RecordBatch::try_from_iter(vec![("a", Arc::new(a) as ArrayRef)]).unwrap()
}

/// Encodes input as a FlightData stream, and then decodes it using
/// FlightRecordBatchStream and valides the decoded record batches
/// match the input.
async fn roundtrip(input: Vec<RecordBatch>) {
    let expected_output = input.clone();
    roundtrip_with_encoder(FlightDataEncoderBuilder::default(), input, expected_output)
        .await
}

/// Encodes input as a FlightData stream, and then decodes it using
/// FlightRecordBatchStream and valides the decoded record batches
/// match the expected input.
///
/// When <https://github.com/apache/arrow-rs/issues/3389> is resolved,
/// it should be possible to use `roundtrip`
async fn roundtrip_dictionary(input: Vec<RecordBatch>) {
    let schema = Arc::new(prepare_schema_for_flight(&input[0].schema()));
    let expected_output: Vec<_> = input
        .iter()
        .map(|batch| prepare_batch_for_flight(batch, schema.clone()).unwrap())
        .collect();
    roundtrip_with_encoder(FlightDataEncoderBuilder::default(), input, expected_output)
        .await
}

async fn roundtrip_with_encoder(
    encoder: FlightDataEncoderBuilder,
    input_batches: Vec<RecordBatch>,
    expected_batches: Vec<RecordBatch>,
) {
    println!("Round tripping with encoder:\n{encoder:#?}");

    let input_batch_stream = futures::stream::iter(input_batches.clone()).map(Ok);

    let encode_stream = encoder.build(input_batch_stream);

    let decode_stream = FlightRecordBatchStream::new_from_flight_data(encode_stream);
    let output_batches: Vec<_> =
        decode_stream.try_collect().await.expect("encode / decode");

    // remove any empty batches from input as they are not transmitted
    let expected_batches: Vec<_> = expected_batches
        .into_iter()
        .filter(|b| b.num_rows() > 0)
        .collect();

    assert_eq!(expected_batches, output_batches);
}
