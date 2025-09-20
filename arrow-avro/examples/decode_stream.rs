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

//! Decode Avro **stream-framed** bytes into Arrow [`RecordBatch`]es.
//!
//! This example demonstrates how to:
//! * Build a streaming `Decoder` via `ReaderBuilder::build_decoder`
//! * Register a writer schema keyed by a **Single‑Object** Rabin fingerprint
//! * Generate a few **Single‑Object** frames in‑memory and decode them

use arrow_avro::reader::ReaderBuilder;
use arrow_avro::schema::{AvroSchema, Fingerprint, SchemaStore, SINGLE_OBJECT_MAGIC};

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

fn encode_user_body(id: i64, name: &str) -> Vec<u8> {
    let mut v = Vec::with_capacity(16 + name.len());
    encode_long(id, &mut v);
    encode_string(name, &mut v);
    v
}

// Frame a body as Avro Single‑Object: magic + 8-byte little‑endian fingerprint + body
fn frame_single_object(fp_rabin: u64, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(2 + 8 + body.len());
    out.extend_from_slice(&SINGLE_OBJECT_MAGIC);
    out.extend_from_slice(&fp_rabin.to_le_bytes());
    out.extend_from_slice(body);
    out
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // A tiny Avro writer schema used to generate a few messages
    let avro = AvroSchema::new(
        r#"{"type":"record","name":"User","fields":[
            {"name":"id","type":"long"},
            {"name":"name","type":"string"}]}"#
            .to_string(),
    );

    // Register the writer schema in a store (keyed by Rabin fingerprint).
    // Keep the fingerprint to seed the decoder and to frame generated messages.
    let mut store = SchemaStore::new();
    let fp = store.register(avro.clone())?;
    let rabin = match fp {
        Fingerprint::Rabin(v) => v,
        _ => unreachable!("Single‑Object framing uses Rabin fingerprints"),
    };

    // Build a streaming decoder configured for Single‑Object framing.
    let mut decoder = ReaderBuilder::new()
        .with_writer_schema_store(store)
        .with_active_fingerprint(fp)
        .build_decoder()?;

    // Generate 5 Single‑Object frames for the "User" schema.
    let mut bytes = Vec::new();
    for i in 0..5 {
        let body = encode_user_body(i as i64, &format!("user-{i}"));
        bytes.extend_from_slice(&frame_single_object(rabin, &body));
    }

    // Feed all bytes at once, then flush completed batches.
    let _consumed = decoder.decode(&bytes)?;
    while let Some(batch) = decoder.flush()? {
        println!(
            "Batch: rows = {:>3}, cols = {}",
            batch.num_rows(),
            batch.num_columns()
        );
    }

    Ok(())
}
