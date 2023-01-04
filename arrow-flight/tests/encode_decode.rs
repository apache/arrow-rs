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
    decode::{DecodedPayload, FlightDataDecoder, FlightRecordBatchStream},
    encode::FlightDataEncoderBuilder,
    error::FlightError,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
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
    let encoder = FlightDataEncoderBuilder::default().with_max_flight_data_size(1);

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

    for max_message_size_bytes in [10, 1024, 2048, 6400, 3211212] {
        let encoder = FlightDataEncoderBuilder::default()
            .with_max_flight_data_size(max_message_size_bytes);

        let input_batch_stream = futures::stream::iter(input.clone()).map(Ok);

        let encode_stream = encoder.build(input_batch_stream);

        let decode_stream = FlightRecordBatchStream::new_from_flight_data(encode_stream);
        let output: Vec<_> = decode_stream.try_collect().await.expect("encode / decode");

        let input_batch = concat_batches(&input[0].schema(), &input).unwrap();
        let output_batch = concat_batches(&output[0].schema(), &output).unwrap();
        assert_eq!(input_batch, output_batch);
    }
}

#[tokio::test]
async fn test_mismatched_record_batch_schema() {
    // send 2 batches with different schemas
    let input_batch_stream = futures::stream::iter(vec![
        Ok(make_primative_batch(5)),
        Ok(make_dictionary_batch(3)),
    ]);

    let encoder = FlightDataEncoderBuilder::default();
    let encode_stream = encoder.build(input_batch_stream);

    let result: Result<Vec<_>, FlightError> = encode_stream.try_collect().await;
    let err = result.unwrap_err();
    assert_eq!(
        err.to_string(),
        "Arrow(InvalidArgumentError(\"number of columns(1) must match number of fields(2) in schema\"))"
    );
}

#[tokio::test]
async fn test_chained_streams_batch_decoder() {
    let batch1 = make_primative_batch(5);
    let batch2 = make_dictionary_batch(3);

    // Model sending two flight streams back to back, with different schemas
    let encode_stream1 = FlightDataEncoderBuilder::default()
        .build(futures::stream::iter(vec![Ok(batch1.clone())]));
    let encode_stream2 = FlightDataEncoderBuilder::default()
        .build(futures::stream::iter(vec![Ok(batch2.clone())]));

    // append the two streams (so they will have two different schema messages)
    let encode_stream = encode_stream1.chain(encode_stream2);

    // FlightRecordBatchStream errors if the schema changes
    let decode_stream = FlightRecordBatchStream::new_from_flight_data(encode_stream);
    let result: Result<Vec<_>, FlightError> = decode_stream.try_collect().await;

    let err = result.unwrap_err();
    assert_eq!(
        err.to_string(),
        "ProtocolError(\"Unexpectedly saw multiple Schema messages in FlightData stream\")"
    );
}

#[tokio::test]
async fn test_chained_streams_data_decoder() {
    let batch1 = make_primative_batch(5);
    let batch2 = make_dictionary_batch(3);

    // Model sending two flight streams back to back, with different schemas
    let encode_stream1 = FlightDataEncoderBuilder::default()
        .build(futures::stream::iter(vec![Ok(batch1.clone())]));
    let encode_stream2 = FlightDataEncoderBuilder::default()
        .build(futures::stream::iter(vec![Ok(batch2.clone())]));

    // append the two streams (so they will have two different schema messages)
    let encode_stream = encode_stream1.chain(encode_stream2);

    // lower level decode stream can handle multiple schema messages
    let decode_stream = FlightDataDecoder::new(encode_stream);

    let decoded_data: Vec<_> =
        decode_stream.try_collect().await.expect("encode / decode");

    println!("decoded data: {decoded_data:#?}");

    // expect two schema messages with the data
    assert_eq!(decoded_data.len(), 4);
    assert!(matches!(decoded_data[0].payload, DecodedPayload::Schema(_)));
    assert!(matches!(
        decoded_data[1].payload,
        DecodedPayload::RecordBatch(_)
    ));
    assert!(matches!(decoded_data[2].payload, DecodedPayload::Schema(_)));
    assert!(matches!(
        decoded_data[3].payload,
        DecodedPayload::RecordBatch(_)
    ));
}

#[tokio::test]
async fn test_mismatched_schema_message() {
    // Model sending schema that is mismatched with the data
    // and expect an error
    async fn do_test(batch1: RecordBatch, batch2: RecordBatch, expected: &str) {
        let encode_stream1 = FlightDataEncoderBuilder::default()
            .build(futures::stream::iter(vec![Ok(batch1.clone())]))
            // take only schema message from first stream
            .take(1);
        let encode_stream2 = FlightDataEncoderBuilder::default()
            .build(futures::stream::iter(vec![Ok(batch2.clone())]))
            // take only data message from second
            .skip(1);

        // append the two streams
        let encode_stream = encode_stream1.chain(encode_stream2);

        // FlightRecordBatchStream errors if the schema changes
        let decode_stream = FlightRecordBatchStream::new_from_flight_data(encode_stream);
        let result: Result<Vec<_>, FlightError> = decode_stream.try_collect().await;

        let err = result.unwrap_err().to_string();
        assert!(
            err.contains(expected),
            "could not find '{expected}' in '{err}'"
        );
    }

    // primitive batch first (has more columns)
    do_test(
        make_primative_batch(5),
        make_dictionary_batch(3),
        "Error decoding ipc RecordBatch: Io error: Invalid data for schema",
    )
    .await;

    // dictioanry batch first
    do_test(
        make_dictionary_batch(3),
        make_primative_batch(5),
        "Error decoding ipc RecordBatch: Invalid argument error",
    )
    .await;
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

/// Make a dictionary batch for testing
///
/// Example:
/// a: value0, value1, value2, None, value1, value2
fn make_dictionary_batch(num_rows: usize) -> RecordBatch {
    let values: Vec<_> = (0..num_rows)
        .map(|i| {
            if i == num_rows / 2 {
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

/// Workaround for https://github.com/apache/arrow-rs/issues/1206
fn prepare_schema_for_flight(schema: &Schema) -> Schema {
    let fields = schema
        .fields()
        .iter()
        .map(|field| match field.data_type() {
            DataType::Dictionary(_, value_type) => Field::new(
                field.name(),
                value_type.as_ref().clone(),
                field.is_nullable(),
            )
            .with_metadata(field.metadata().clone()),
            _ => field.clone(),
        })
        .collect();

    Schema::new(fields)
}

/// Workaround for https://github.com/apache/arrow-rs/issues/1206
fn prepare_batch_for_flight(
    batch: &RecordBatch,
    schema: SchemaRef,
) -> Result<RecordBatch, FlightError> {
    let columns = batch
        .columns()
        .iter()
        .map(hydrate_dictionary)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(RecordBatch::try_new(schema, columns)?)
}

fn hydrate_dictionary(array: &ArrayRef) -> Result<ArrayRef, FlightError> {
    let arr = if let DataType::Dictionary(_, value) = array.data_type() {
        arrow_cast::cast(array, value)?
    } else {
        Arc::clone(array)
    };
    Ok(arr)
}
