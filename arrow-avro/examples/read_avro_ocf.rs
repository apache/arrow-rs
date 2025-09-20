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

//! Read an Avro **Object Container File (OCF)** into Arrow [`RecordBatch`] values.
//!
//! This example demonstrates how to:
//! * Construct a [`Reader`] using [`ReaderBuilder::build`]
//! * Iterate `RecordBatch`es and print a brief summary

use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;

use arrow_array::RecordBatch;
use arrow_avro::reader::ReaderBuilder;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ocf_path: PathBuf = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("test")
        .join("data")
        .join("skippable_types.avro");

    let reader = BufReader::new(File::open(&ocf_path)?);
    // Build a high-level OCF Reader with default settings
    let avro_reader = ReaderBuilder::new().build(reader)?;
    let schema = avro_reader.schema();
    println!(
        "Discovered Arrow schema with {} fields",
        schema.fields().len()
    );

    let mut total_batches = 0usize;
    let mut total_rows = 0usize;
    let mut total_columns = schema.fields().len();

    for result in avro_reader {
        let batch: RecordBatch = result?;
        total_batches += 1;
        total_rows += batch.num_rows();
        total_columns = batch.num_columns();

        println!(
            "Batch {:>3}: rows = {:>6}, cols = {:>3}",
            total_batches,
            batch.num_rows(),
            batch.num_columns()
        );
    }

    println!();
    println!("Done.");
    println!("  Batches : {total_batches}");
    println!("  Rows    : {total_rows}");
    println!("  Columns : {total_columns}");

    Ok(())
}
