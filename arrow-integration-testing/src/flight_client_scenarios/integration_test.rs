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

//! Integration tests for the Flight client.

use crate::open_json_file;
use std::collections::HashMap;

use arrow::{
    array::ArrayRef,
    buffer::Buffer,
    datatypes::SchemaRef,
    ipc::{self, reader, writer},
    record_batch::RecordBatch,
};
use arrow_flight::{
    flight_descriptor::DescriptorType, flight_service_client::FlightServiceClient,
    utils::flight_data_to_arrow_batch, FlightData, FlightDescriptor, Location, SchemaAsIpc, Ticket,
};
use futures::{channel::mpsc, sink::SinkExt, stream, StreamExt};
use tonic::{Request, Streaming};

use arrow::datatypes::Schema;
use std::sync::Arc;

type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
type Result<T = (), E = Error> = std::result::Result<T, E>;

type Client = FlightServiceClient<tonic::transport::Channel>;

/// Run a scenario that uploads data to a Flight server and then downloads it back
pub async fn run_scenario(host: &str, port: u16, path: &str) -> Result {
    let url = format!("http://{host}:{port}");

    let client = FlightServiceClient::connect(url).await?;

    let json_file = open_json_file(path)?;

    let batches = json_file.read_batches()?;
    let schema = Arc::new(json_file.schema);

    let mut descriptor = FlightDescriptor::default();
    descriptor.set_type(DescriptorType::Path);
    descriptor.path = vec![path.to_string()];

    upload_data(client.clone(), schema, descriptor.clone(), batches.clone()).await?;
    verify_data(client, descriptor, &batches).await?;

    Ok(())
}

async fn upload_data(
    mut client: Client,
    schema: SchemaRef,
    descriptor: FlightDescriptor,
    original_data: Vec<RecordBatch>,
) -> Result {
    let (mut upload_tx, upload_rx) = mpsc::channel(10);

    let options = arrow::ipc::writer::IpcWriteOptions::default();
    let mut schema_flight_data: FlightData = SchemaAsIpc::new(&schema, &options).into();
    // arrow_flight::utils::flight_data_from_arrow_schema(&schema, &options);
    schema_flight_data.flight_descriptor = Some(descriptor.clone());
    upload_tx.send(schema_flight_data).await?;

    let mut original_data_iter = original_data.iter().enumerate();

    if let Some((counter, first_batch)) = original_data_iter.next() {
        let metadata = counter.to_string().into_bytes();
        // Preload the first batch into the channel before starting the request
        send_batch(&mut upload_tx, &metadata, first_batch, &options).await?;

        let outer = client.do_put(Request::new(upload_rx)).await?;
        let mut inner = outer.into_inner();

        let r = inner
            .next()
            .await
            .expect("No response received")
            .expect("Invalid response received");
        assert_eq!(metadata, r.app_metadata);

        // Stream the rest of the batches
        for (counter, batch) in original_data_iter {
            let metadata = counter.to_string().into_bytes();
            send_batch(&mut upload_tx, &metadata, batch, &options).await?;

            let r = inner
                .next()
                .await
                .expect("No response received")
                .expect("Invalid response received");
            assert_eq!(metadata, r.app_metadata);
        }
        drop(upload_tx);
        assert!(
            inner.next().await.is_none(),
            "Should not receive more results"
        );
    } else {
        drop(upload_tx);
        client.do_put(Request::new(upload_rx)).await?;
    }

    Ok(())
}

async fn send_batch(
    upload_tx: &mut mpsc::Sender<FlightData>,
    metadata: &[u8],
    batch: &RecordBatch,
    options: &writer::IpcWriteOptions,
) -> Result {
    let data_gen = writer::IpcDataGenerator::default();
    let mut dictionary_tracker = writer::DictionaryTracker::new_with_preserve_dict_id(false, true);

    let (encoded_dictionaries, encoded_batch) = data_gen
        .encoded_batch(batch, &mut dictionary_tracker, options)
        .expect("DictionaryTracker configured above to not error on replacement");

    let dictionary_flight_data: Vec<FlightData> =
        encoded_dictionaries.into_iter().map(Into::into).collect();
    let mut batch_flight_data: FlightData = encoded_batch.into();

    upload_tx
        .send_all(&mut stream::iter(dictionary_flight_data).map(Ok))
        .await?;

    // Only the record batch's FlightData gets app_metadata
    batch_flight_data.app_metadata = metadata.to_vec().into();
    upload_tx.send(batch_flight_data).await?;
    Ok(())
}

async fn verify_data(
    mut client: Client,
    descriptor: FlightDescriptor,
    expected_data: &[RecordBatch],
) -> Result {
    let resp = client.get_flight_info(Request::new(descriptor)).await?;
    let info = resp.into_inner();

    assert!(
        !info.endpoint.is_empty(),
        "No endpoints returned from Flight server",
    );
    for endpoint in info.endpoint {
        let ticket = endpoint
            .ticket
            .expect("No ticket returned from Flight server");

        assert!(
            !endpoint.location.is_empty(),
            "No locations returned from Flight server",
        );
        for location in endpoint.location {
            consume_flight_location(location, ticket.clone(), expected_data).await?;
        }
    }

    Ok(())
}

async fn consume_flight_location(
    location: Location,
    ticket: Ticket,
    expected_data: &[RecordBatch],
) -> Result {
    let mut location = location;
    // The other Flight implementations use the `grpc+tcp` scheme, but the Rust http libs
    // don't recognize this as valid.
    // more details: https://github.com/apache/arrow-rs/issues/1398
    location.uri = location.uri.replace("grpc+tcp://", "http://");

    let mut client = FlightServiceClient::connect(location.uri).await?;
    let resp = client.do_get(ticket).await?;
    let mut resp = resp.into_inner();

    let flight_schema = receive_schema_flight_data(&mut resp)
        .await
        .unwrap_or_else(|| panic!("Failed to receive flight schema"));
    let actual_schema = Arc::new(flight_schema);

    let mut dictionaries_by_id = HashMap::new();

    for (counter, expected_batch) in expected_data.iter().enumerate() {
        let data =
            receive_batch_flight_data(&mut resp, actual_schema.clone(), &mut dictionaries_by_id)
                .await
                .unwrap_or_else(|| {
                    panic!(
                        "Got fewer batches than expected, received so far: {} expected: {}",
                        counter,
                        expected_data.len(),
                    )
                });

        let metadata = counter.to_string().into_bytes();
        assert_eq!(metadata, data.app_metadata);

        let actual_batch =
            flight_data_to_arrow_batch(&data, actual_schema.clone(), &dictionaries_by_id)
                .expect("Unable to convert flight data to Arrow batch");

        assert_eq!(actual_schema, actual_batch.schema());
        assert_eq!(expected_batch.num_columns(), actual_batch.num_columns());
        assert_eq!(expected_batch.num_rows(), actual_batch.num_rows());
        let schema = expected_batch.schema();
        for i in 0..expected_batch.num_columns() {
            let field = schema.field(i);
            let field_name = field.name();

            let expected_data = expected_batch.column(i).as_ref();
            let actual_data = actual_batch.column(i).as_ref();

            assert_eq!(expected_data, actual_data, "Data for field {field_name}");
        }
    }

    assert!(
        resp.next().await.is_none(),
        "Got more batches than the expected: {}",
        expected_data.len(),
    );

    Ok(())
}

async fn receive_schema_flight_data(resp: &mut Streaming<FlightData>) -> Option<Schema> {
    let data = resp.next().await?.ok()?;
    let message =
        arrow::ipc::root_as_message(&data.data_header[..]).expect("Error parsing message");

    // message header is a Schema, so read it
    let ipc_schema: ipc::Schema = message
        .header_as_schema()
        .expect("Unable to read IPC message as schema");
    let schema = ipc::convert::fb_to_schema(ipc_schema);

    Some(schema)
}

async fn receive_batch_flight_data(
    resp: &mut Streaming<FlightData>,
    schema: SchemaRef,
    dictionaries_by_id: &mut HashMap<i64, ArrayRef>,
) -> Option<FlightData> {
    let mut data = resp.next().await?.ok()?;
    let mut message =
        arrow::ipc::root_as_message(&data.data_header[..]).expect("Error parsing first message");

    while message.header_type() == ipc::MessageHeader::DictionaryBatch {
        reader::read_dictionary(
            &Buffer::from(data.data_body.as_ref()),
            message
                .header_as_dictionary_batch()
                .expect("Error parsing dictionary"),
            &schema,
            dictionaries_by_id,
            &message.version(),
        )
        .expect("Error reading dictionary");

        data = resp.next().await?.ok()?;
        message =
            arrow::ipc::root_as_message(&data.data_header[..]).expect("Error parsing message");
    }

    Some(data)
}
