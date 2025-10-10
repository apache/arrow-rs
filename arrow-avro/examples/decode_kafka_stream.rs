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

//! Decode **Confluent Schema Registry - framed** Avro messages into Arrow [`RecordBatch`]es,
//! resolving **older writer schemas** against a **current reader schema** without adding
//! any new reader‑only fields.
//!
//! What this example shows:
//! * A **reader schema** for the current topic version with fields: `{ id: long, name: string }`.
//! * Two older **writer schemas** (Confluent IDs **0** and **1**):
//!   - v0: `{ id: int, name: string }` (older type for `id`)
//!   - v1: `{ id: long, name: string, email: ["null","string"] }` (extra writer field `email`)
//! * Streaming decode with `ReaderBuilder::with_reader_schema(...)` so that:
//!   - v0's `id:int` is **promoted** to `long` for the reader
//!   - v1's extra `email` field is **ignored** by the reader (projection)
//!
//! Wire format reminder (message value bytes):
//! `0x00` magic byte + 4‑byte **big‑endian** schema ID + Avro **binary** body.
//!

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_avro::reader::ReaderBuilder;
use arrow_avro::schema::{
    AvroSchema, CONFLUENT_MAGIC, Fingerprint, FingerprintAlgorithm, SchemaStore,
};
use arrow_schema::ArrowError;

fn encode_long(value: i64, out: &mut Vec<u8>) {
    let mut n = ((value << 1) ^ (value >> 63)) as u64;
    while (n & !0x7F) != 0 {
        out.push(((n as u8) & 0x7F) | 0x80);
        n >>= 7;
    }
    out.push(n as u8);
}

fn encode_len(len: usize, out: &mut Vec<u8>) {
    encode_long(len as i64, out)
}

fn encode_string(s: &str, out: &mut Vec<u8>) {
    encode_len(s.len(), out);
    out.extend_from_slice(s.as_bytes());
}

fn encode_union_index(index: i64, out: &mut Vec<u8>) {
    encode_long(index, out);
}

// Writer v0 (ID=0):
//   {"type":"record","name":"User","fields":[
//     {"name":"id","type":"int"},
//     {"name":"name","type":"string"}]}
fn encode_user_v0_body(id: i32, name: &str) -> Vec<u8> {
    let mut v = Vec::with_capacity(16 + name.len());
    encode_long(id as i64, &mut v);
    encode_string(name, &mut v);
    v
}

// Writer v1 (ID=1):
//   {"type":"record","name":"User","fields":[
//     {"name":"id","type":"long"},
//     {"name":"name","type":"string"},
//     {"name":"email","type":["null","string"],"default":null}]}
fn encode_user_v1_body(id: i64, name: &str, email: Option<&str>) -> Vec<u8> {
    let mut v = Vec::with_capacity(24 + name.len() + email.map(|s| s.len()).unwrap_or(0));
    encode_long(id, &mut v); // id: long
    encode_string(name, &mut v); // name: string
    match email {
        None => {
            // union index 0 => null
            encode_union_index(0, &mut v);
            // no value bytes follow for null
        }
        Some(s) => {
            // union index 1 => string, followed by the string payload
            encode_union_index(1, &mut v);
            encode_string(s, &mut v);
        }
    }
    v
}

fn frame_confluent(id_be: u32, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + body.len());
    out.extend_from_slice(&CONFLUENT_MAGIC); // 0x00
    out.extend_from_slice(&id_be.to_be_bytes());
    out.extend_from_slice(body);
    out
}

fn print_arrow_schema(schema: &arrow_schema::Schema) {
    println!("Resolved Arrow schema (via reader schema):");
    for (i, f) in schema.fields().iter().enumerate() {
        println!(
            "  {i:>2}: {}: {:?} (nullable: {})",
            f.name(),
            f.data_type(),
            f.is_nullable()
        );
    }
    if !schema.metadata.is_empty() {
        println!("  metadata: {:?}", schema.metadata());
    }
}

fn print_rows(batch: &RecordBatch) -> Result<(), ArrowError> {
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| ArrowError::ComputeError("col 0 not Int64".into()))?;
    let names = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| ArrowError::ComputeError("col 1 not Utf8".into()))?;
    for row in 0..batch.num_rows() {
        let id = ids.value(row);
        let name = names.value(row);
        println!("    row {row}: id={id}, name={name}");
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The current topic schema as a READER schema
    let reader_schema = AvroSchema::new(
        r#"{
            "type":"record","name":"User","fields":[
                {"name":"id","type":"long"},
                {"name":"name","type":"string"}
            ]}"#
        .to_string(),
    );

    // Two prior WRITER schemas versions under Confluent IDs 0 and 1
    let writer_v0 = AvroSchema::new(
        r#"{
            "type":"record","name":"User","fields":[
                {"name":"id","type":"int"},
                {"name":"name","type":"string"}
            ]}"#
        .to_string(),
    );
    let writer_v1 = AvroSchema::new(
        r#"{
            "type":"record","name":"User","fields":[
                {"name":"id","type":"long"},
                {"name":"name","type":"string"},
                {"name":"email","type":["null","string"],"default":null}
            ]}"#
        .to_string(),
    );

    let id_v0: u32 = 0;
    let id_v1: u32 = 1;

    // Confluent SchemaStore keyed by integer IDs (FingerprintAlgorithm::Id)
    let mut store = SchemaStore::new_with_type(FingerprintAlgorithm::Id);
    store.set(Fingerprint::Id(id_v0), writer_v0.clone())?;
    store.set(Fingerprint::Id(id_v1), writer_v1.clone())?;

    // Build a streaming Decoder with the READER schema
    let mut decoder = ReaderBuilder::new()
        .with_reader_schema(reader_schema)
        .with_writer_schema_store(store)
        .with_batch_size(8) // small batches for demo output
        .build_decoder()?;

    // Print the resolved Arrow schema (derived from reader and writer)
    let resolved = decoder.schema();
    print_arrow_schema(resolved.as_ref());
    println!();

    // Simulate an interleaved Kafka stream (IDs 0 and 1)
    //    - v0: {id:int, name:string} --> reader: id promoted to long
    //    - v1: {id:long, name:string, email: ...} --> reader ignores extra field
    let mut frames: Vec<(u32, Vec<u8>)> = Vec::new();

    // Some v0 messages
    for (i, name) in ["v0-alice", "v0-bob", "v0-carol"].iter().enumerate() {
        let body = encode_user_v0_body(1000 + i as i32, name);
        frames.push((id_v0, frame_confluent(id_v0, &body)));
    }

    // Some v1 messages (may include optional email on the writer side)
    let v1_rows = [
        (2001_i64, "v1-dave", Some("dave@example.com")),
        (2002_i64, "v1-erin", None),
        (2003_i64, "v1-frank", Some("frank@example.com")),
    ];
    for (id, name, email) in v1_rows {
        let body = encode_user_v1_body(id, name, email);
        frames.push((id_v1, frame_confluent(id_v1, &body)));
    }

    // Interleave to show mid-stream schema ID changes (0,1,0,1, ...)
    frames.swap(1, 3); // crude interleave for demo

    // Decode frames as if they were Kafka record values
    for (schema_id, frame) in frames {
        println!("Decoding record framed with Confluent schema id = {schema_id}");
        let _consumed = decoder.decode(&frame)?;
        while let Some(batch) = decoder.flush()? {
            println!(
                "  -> Emitted batch: rows = {}, cols = {}",
                batch.num_rows(),
                batch.num_columns()
            );
            print_rows(&batch)?;
        }
        println!();
    }

    println!("Done decoding Kafka-style stream with schema resolution (no reader-added fields).");
    Ok(())
}
