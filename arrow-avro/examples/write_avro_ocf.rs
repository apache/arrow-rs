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

//! # Write an Avro Object Container File (OCF) from an Arrow `RecordBatch`
//!
//! This example builds a small Arrow `RecordBatch` and persists it to an
//! **Avro Object Container File (OCF)** using
//! `arrow_avro::writer::{Writer, WriterBuilder}`.
//!
//! ## What this example does
//! - Define an Arrow schema with supported types (`Int64`, `Utf8`, `Boolean`,
//!   `Float64`, `Binary`, and `Timestamp (Microsecond, "UTC")`).
//! - Constructs arrays and a `RecordBatch`, ensuring each column’s data type
//!   **exactly** matches the schema (timestamps include the `"UTC"` timezone).
//! - Writes a single batch to `target/write_avro_ocf_example.avro` as an OCF,
//!   using Snappy block compression (you can disable or change the codec).
//! - Prints the file’s 16‑byte sync marker (used by OCF to delimit blocks).

use std::fs::File;
use std::io::BufWriter;
use std::sync::Arc;

use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use arrow_avro::compression::CompressionCodec;
use arrow_avro::writer::format::AvroOcfFormat;
use arrow_avro::writer::{Writer, WriterBuilder};
use arrow_schema::{DataType, Field, Schema, TimeUnit};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Arrow schema
    // id:         Int64 (non-null)
    // name:       Utf8  (nullable)
    // active:     Boolean (non-null)
    // score:      Float64 (nullable)
    // payload:    Binary (nullable)
    // created_at: Timestamp(Microsecond, Some("UTC")) (non-null)
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("active", DataType::Boolean, false),
        Field::new("score", DataType::Float64, true),
        Field::new("payload", DataType::Binary, true),
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC".to_string()))),
            false,
        ),
    ]);

    let schema_ref = Arc::new(schema.clone());
    let ids = Int64Array::from(vec![1_i64, 2, 3]);
    let names = StringArray::from(vec![Some("alpha"), None, Some("gamma")]);
    let active = BooleanArray::from(vec![true, false, true]);
    let scores = Float64Array::from(vec![Some(1.5_f64), None, Some(3.0)]);

    // BinaryArray: include a null
    let payload = BinaryArray::from_opt_vec(vec![Some(&b"abc"[..]), None, Some(&[0u8, 1, 2][..])]);

    // Timestamp in microseconds since UNIX epoch
    let created_at = TimestampMicrosecondArray::from(vec![
        Some(1_722_000_000_000_000_i64),
        Some(1_722_000_123_456_000_i64),
        Some(1_722_000_999_999_000_i64),
    ])
    .with_timezone("UTC".to_string());

    let columns: Vec<ArrayRef> = vec![
        Arc::new(ids),
        Arc::new(names),
        Arc::new(active),
        Arc::new(scores),
        Arc::new(payload),
        Arc::new(created_at),
    ];

    let batch = RecordBatch::try_new(schema_ref, columns)?;

    // Build an OCF writer with optional compression
    let out_path = "target/write_avro_ocf_example.avro";
    let file = File::create(out_path)?;
    let mut writer: Writer<_, AvroOcfFormat> = WriterBuilder::new(schema)
        .with_compression(Some(CompressionCodec::Snappy))
        .build(BufWriter::new(file))?;

    // Write a single batch (use `write_batches` for multiple)
    writer.write(&batch)?;
    writer.finish()?; // flush and finalize

    if let Some(sync) = writer.sync_marker() {
        println!("Wrote OCF to {out_path} (sync marker: {:02x?})", &sync[..]);
    } else {
        println!("Wrote OCF to {out_path}");
    }

    Ok(())
}
