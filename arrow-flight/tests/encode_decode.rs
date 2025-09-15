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

use std::{collections::HashMap, sync::Arc};

use arrow_array::{ArrayRef, RecordBatch};
use arrow_cast::pretty::pretty_format_batches;
use arrow_flight::flight_descriptor::DescriptorType;
use arrow_flight::FlightDescriptor;
use arrow_flight::{
    decode::{DecodedPayload, FlightDataDecoder, FlightRecordBatchStream},
    encode::FlightDataEncoderBuilder,
    error::FlightError,
};
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};

mod common;
use common::utils::{make_dictionary_batch, make_primitive_batch, make_view_batches};

#[tokio::test]
async fn test_empty() {
    roundtrip(vec![]).await;
}

#[tokio::test]
async fn test_empty_batch() {
    let batch = make_primitive_batch(5);
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
    assert_eq!(result.to_string(), "Not yet implemented: foo");
}

#[tokio::test]
async fn test_primitive_one() {
    roundtrip(vec![make_primitive_batch(5)]).await;
}

#[tokio::test]
async fn test_schema_metadata() {
    let batch = make_primitive_batch(5);
    let metadata = HashMap::from([("some_key".to_owned(), "some_value".to_owned())]);

    // create a batch that has schema level metadata
    let schema = Arc::new(batch.schema().as_ref().clone().with_metadata(metadata));
    let batch = RecordBatch::try_new(schema, batch.columns().to_vec()).unwrap();

    roundtrip(vec![batch]).await;
}

#[tokio::test]
async fn test_primitive_many() {
    roundtrip(vec![
        make_primitive_batch(1),
        make_primitive_batch(7),
        make_primitive_batch(32),
    ])
    .await;
}

#[tokio::test]
async fn test_primitive_empty() {
    let batch = make_primitive_batch(5);
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
async fn test_view_types_one() {
    roundtrip(vec![make_view_batches(5)]).await;
}

#[tokio::test]
async fn test_view_types_many() {
    roundtrip(vec![
        make_view_batches(5),
        make_view_batches(9),
        make_view_batches(5),
        make_view_batches(5),
    ])
    .await;
}

#[tokio::test]
async fn test_zero_batches_no_schema() {
    let stream = FlightDataEncoderBuilder::default().build(futures::stream::iter(vec![]));

    let mut decoder = FlightRecordBatchStream::new_from_flight_data(stream);
    assert!(decoder.schema().is_none());
    // No batches come out
    assert!(decoder.next().await.is_none());
    // schema has not been received
    assert!(decoder.schema().is_none());
}

#[tokio::test]
async fn test_zero_batches_schema_specified() {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
    let stream = FlightDataEncoderBuilder::default()
        .with_schema(schema.clone())
        .build(futures::stream::iter(vec![]));

    let mut decoder = FlightRecordBatchStream::new_from_flight_data(stream);
    assert!(decoder.schema().is_none());
    // No batches come out
    assert!(decoder.next().await.is_none());
    // But schema has been received correctly
    assert_eq!(decoder.schema(), Some(&schema));
}

#[tokio::test]
async fn test_with_flight_descriptor() {
    let stream = futures::stream::iter(vec![Ok(make_dictionary_batch(5))]);
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, true)]));

    let descriptor = Some(FlightDescriptor {
        r#type: DescriptorType::Path.into(),
        path: vec!["table_name".to_string()],
        cmd: Bytes::default(),
    });

    let encoder = FlightDataEncoderBuilder::default()
        .with_schema(schema.clone())
        .with_flight_descriptor(descriptor.clone());

    let mut encoder = encoder.build(stream);

    // First batch should be the schema
    let first_batch = encoder.next().await.unwrap().unwrap();

    assert_eq!(first_batch.flight_descriptor, descriptor);
}

#[tokio::test]
async fn test_zero_batches_dictionary_schema_specified() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new_dictionary("b", DataType::Int32, DataType::Utf8, false),
    ]));

    // Expect dictionary to be hydrated in output (#3389)
    let expected_schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Utf8, false),
    ]));
    let stream = FlightDataEncoderBuilder::default()
        .with_schema(schema.clone())
        .build(futures::stream::iter(vec![]));

    let mut decoder = FlightRecordBatchStream::new_from_flight_data(stream);
    assert!(decoder.schema().is_none());
    // No batches come out
    assert!(decoder.next().await.is_none());
    // But schema has been received correctly
    assert_eq!(decoder.schema(), Some(&expected_schema));
}

#[tokio::test]
async fn test_app_metadata() {
    let input_batch_stream = futures::stream::iter(vec![Ok(make_primitive_batch(78))]);

    let app_metadata = Bytes::from("My Metadata");
    let encoder = FlightDataEncoderBuilder::default().with_metadata(app_metadata.clone());

    let encode_stream = encoder.build(input_batch_stream);

    // use lower level stream to get access to app metadata
    let decode_stream = FlightRecordBatchStream::new_from_flight_data(encode_stream).into_inner();

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
    let input_batch_stream = futures::stream::iter(vec![Ok(make_primitive_batch(5))]);

    // 5 input rows, with a very small limit should result in 5 batch messages
    let encoder = FlightDataEncoderBuilder::default().with_max_flight_data_size(1);

    let encode_stream = encoder.build(input_batch_stream);

    // use lower level stream to get access to app metadata
    let decode_stream = FlightRecordBatchStream::new_from_flight_data(encode_stream).into_inner();

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
        make_primitive_batch(123),
        make_primitive_batch(17),
        make_primitive_batch(201),
        make_primitive_batch(2),
        make_primitive_batch(1),
        make_primitive_batch(11),
        make_primitive_batch(127),
    ];

    for max_message_size_bytes in [10, 1024, 2048, 6400, 3211212] {
        let encoder =
            FlightDataEncoderBuilder::default().with_max_flight_data_size(max_message_size_bytes);

        let input_batch_stream = futures::stream::iter(input.clone()).map(Ok);

        let encode_stream = encoder.build(input_batch_stream);

        let decode_stream = FlightRecordBatchStream::new_from_flight_data(encode_stream);
        let output: Vec<_> = decode_stream.try_collect().await.expect("encode / decode");

        for b in &output {
            assert_eq!(b.schema(), input[0].schema());
        }

        let a = pretty_format_batches(&input).unwrap().to_string();
        let b = pretty_format_batches(&output).unwrap().to_string();
        assert_eq!(a, b);
    }
}

#[tokio::test]
async fn test_mismatched_record_batch_schema() {
    // send 2 batches with different schemas
    let input_batch_stream = futures::stream::iter(vec![
        Ok(make_primitive_batch(5)),
        Ok(make_dictionary_batch(3)),
    ]);

    let encoder = FlightDataEncoderBuilder::default();
    let encode_stream = encoder.build(input_batch_stream);

    let result: Result<Vec<_>, FlightError> = encode_stream.try_collect().await;
    let err = result.unwrap_err();
    assert_eq!(
        err.to_string(),
        "Arrow error: Invalid argument error: number of columns(1) must match number of fields(2) in schema"
    );
}

#[tokio::test]
async fn test_chained_streams_batch_decoder() {
    let batch1 = make_primitive_batch(5);
    let batch2 = make_dictionary_batch(3);

    // Model sending two flight streams back to back, with different schemas
    let encode_stream1 =
        FlightDataEncoderBuilder::default().build(futures::stream::iter(vec![Ok(batch1.clone())]));
    let encode_stream2 =
        FlightDataEncoderBuilder::default().build(futures::stream::iter(vec![Ok(batch2.clone())]));

    // append the two streams (so they will have two different schema messages)
    let encode_stream = encode_stream1.chain(encode_stream2);

    // FlightRecordBatchStream errors if the schema changes
    let decode_stream = FlightRecordBatchStream::new_from_flight_data(encode_stream);
    let result: Result<Vec<_>, FlightError> = decode_stream.try_collect().await;

    let err = result.unwrap_err();
    assert_eq!(
        err.to_string(),
        "Protocol error: Unexpectedly saw multiple Schema messages in FlightData stream"
    );
}

#[tokio::test]
async fn test_chained_streams_data_decoder() {
    let batch1 = make_primitive_batch(5);
    let batch2 = make_dictionary_batch(3);

    // Model sending two flight streams back to back, with different schemas
    let encode_stream1 =
        FlightDataEncoderBuilder::default().build(futures::stream::iter(vec![Ok(batch1.clone())]));
    let encode_stream2 =
        FlightDataEncoderBuilder::default().build(futures::stream::iter(vec![Ok(batch2.clone())]));

    // append the two streams (so they will have two different schema messages)
    let encode_stream = encode_stream1.chain(encode_stream2);

    // lower level decode stream can handle multiple schema messages
    let decode_stream = FlightDataDecoder::new(encode_stream);

    let decoded_data: Vec<_> = decode_stream.try_collect().await.expect("encode / decode");

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
        make_primitive_batch(5),
        make_dictionary_batch(3),
        "Error decoding ipc RecordBatch: Schema error: Invalid data for schema",
    )
    .await;

    // dictionary batch first
    do_test(
        make_dictionary_batch(3),
        make_primitive_batch(5),
        "Error decoding ipc RecordBatch: Invalid argument error",
    )
    .await;
}

/// Encodes input as a FlightData stream, and then decodes it using
/// FlightRecordBatchStream and validates the decoded record batches
/// match the input.
async fn roundtrip(input: Vec<RecordBatch>) {
    let expected_output = input.clone();
    roundtrip_with_encoder(FlightDataEncoderBuilder::default(), input, expected_output).await
}

/// Encodes input as a FlightData stream, and then decodes it using
/// FlightRecordBatchStream and validates the decoded record batches
/// match the expected input.
///
/// When <https://github.com/apache/arrow-rs/issues/3389> is resolved,
/// it should be possible to use `roundtrip`
async fn roundtrip_dictionary(input: Vec<RecordBatch>) {
    let schema = Arc::new(prepare_schema_for_flight(input[0].schema_ref()));
    let expected_output: Vec<_> = input
        .iter()
        .map(|batch| prepare_batch_for_flight(batch, schema.clone()).unwrap())
        .collect();
    roundtrip_with_encoder(FlightDataEncoderBuilder::default(), input, expected_output).await
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
    let output_batches: Vec<_> = decode_stream.try_collect().await.expect("encode / decode");

    // remove any empty batches from input as they are not transmitted
    let expected_batches: Vec<_> = expected_batches
        .into_iter()
        .filter(|b| b.num_rows() > 0)
        .collect();

    assert_eq!(expected_batches, output_batches);
}

/// Workaround for https://github.com/apache/arrow-rs/issues/1206
fn prepare_schema_for_flight(schema: &Schema) -> Schema {
    let fields: Fields = schema
        .fields()
        .iter()
        .map(|field| match field.data_type() {
            DataType::Dictionary(_, value_type) => Field::new(
                field.name(),
                value_type.as_ref().clone(),
                field.is_nullable(),
            )
            .with_metadata(field.metadata().clone()),
            _ => field.as_ref().clone(),
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
