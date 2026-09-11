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

use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

use arrow_buffer::Buffer;
use arrow_ipc::reader::{StreamReader, read_record_batch};
use arrow_ipc::writer::StreamWriter;
use arrow_ipc::{MetadataVersion, root_as_message};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

// Encode malformed IPC directly, without constructing an invalid Arrow array.
fn short_validity_batch() -> (Vec<u8>, Buffer, SchemaRef) {
    let mut fbb = flatbuffers::FlatBufferBuilder::new();
    let nodes = fbb.create_vector(&[arrow_ipc::FieldNode::new(8000, 8000)]);
    let buffers = fbb.create_vector(&[
        arrow_ipc::Buffer::new(0, 1),
        arrow_ipc::Buffer::new(8, 32000),
    ]);
    let batch = arrow_ipc::RecordBatch::create(
        &mut fbb,
        &arrow_ipc::RecordBatchArgs {
            length: 8000,
            nodes: Some(nodes),
            buffers: Some(buffers),
            ..Default::default()
        },
    );
    let message = arrow_ipc::Message::create(
        &mut fbb,
        &arrow_ipc::MessageArgs {
            version: MetadataVersion::V5,
            header_type: arrow_ipc::MessageHeader::RecordBatch,
            header: Some(batch.as_union_value()),
            bodyLength: 32008,
            ..Default::default()
        },
    );
    fbb.finish(message, None);
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
    (
        fbb.finished_data().to_vec(),
        Buffer::from(vec![0_u8; 32008]),
        schema,
    )
}

#[test]
fn record_batch_decoder_rejects_short_validity_buffer() {
    let (metadata, body, schema) = short_validity_batch();
    let message = root_as_message(&metadata).unwrap();
    let err = read_record_batch(
        &body,
        message.header_as_record_batch().unwrap(),
        schema,
        &HashMap::new(),
        None,
        &MetadataVersion::V5,
    )
    .unwrap_err();
    assert_eq!(
        err.to_string(),
        "Invalid argument error: null_bit_buffer size too small. got 1 needed 1000"
    );
}

#[test]
fn stream_reader_rejects_short_validity_buffer() {
    let (metadata, body, schema) = short_validity_batch();
    let mut stream = Vec::new();
    {
        // Write the schema message, leaving the stream open for the malformed batch.
        let _writer = StreamWriter::try_new(&mut stream, &schema).unwrap();
    }
    let padded_len = metadata.len().div_ceil(8) * 8;
    stream.extend_from_slice(&[255; 4]);
    stream.extend_from_slice(&(padded_len as i32).to_le_bytes());
    stream.extend_from_slice(&metadata);
    stream.extend(std::iter::repeat_n(0, padded_len - metadata.len()));
    stream.extend_from_slice(body.as_slice());
    stream.extend_from_slice(&[255, 255, 255, 255, 0, 0, 0, 0]);

    let mut reader = StreamReader::try_new(Cursor::new(stream), None).unwrap();
    let err = reader.next().unwrap().unwrap_err();
    assert_eq!(
        err.to_string(),
        "Invalid argument error: null_bit_buffer size too small. got 1 needed 1000"
    );
}
