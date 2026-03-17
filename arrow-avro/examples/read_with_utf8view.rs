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

//! This example demonstrates how to use Utf8View support in the Arrow Avro reader
//!
//! It reads an Avro file with string data twice - once with regular StringArray
//! and once with StringViewArray - and compares the performance.

use std::env;
use std::fs::File;
use std::io::{BufReader, Seek, SeekFrom};
use std::time::Instant;

use arrow_array::{RecordBatch, StringArray, StringViewArray};
use arrow_avro::reader::ReaderBuilder;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let file_path = if args.len() > 1 {
        &args[1]
    } else {
        eprintln!("No file specified, please provide an Avro file path");
        eprintln!("Usage: {} <avro_file_path>", args[0]);
        return Ok(());
    };

    let file = File::open(file_path)?;
    let mut file_for_view = file.try_clone()?;

    let start = Instant::now();
    let reader = BufReader::new(file);
    let avro_reader = ReaderBuilder::new().build(reader)?;
    let schema = avro_reader.schema();
    let batches: Vec<RecordBatch> = avro_reader.collect::<Result<_, _>>()?;
    let regular_duration = start.elapsed();

    file_for_view.seek(SeekFrom::Start(0))?;
    let start = Instant::now();
    let reader_view = BufReader::new(file_for_view);
    let avro_reader_view = ReaderBuilder::new()
        .with_utf8_view(true)
        .build(reader_view)?;
    let batches_view: Vec<RecordBatch> = avro_reader_view.collect::<Result<_, _>>()?;
    let view_duration = start.elapsed();

    let num_rows = batches.iter().map(|b| b.num_rows()).sum::<usize>();

    println!("Read {num_rows} rows from {file_path}");
    println!("Reading with StringArray: {regular_duration:?}");
    println!("Reading with StringViewArray: {view_duration:?}");

    if regular_duration > view_duration {
        println!(
            "StringViewArray was {:.2}x faster",
            regular_duration.as_secs_f64() / view_duration.as_secs_f64()
        );
    } else {
        println!(
            "StringArray was {:.2}x faster",
            view_duration.as_secs_f64() / regular_duration.as_secs_f64()
        );
    }

    if batches.is_empty() {
        println!("No data read from file.");
        return Ok(());
    }

    // Inspect the first batch from each run to show the array types
    let batch = &batches[0];
    let batch_view = &batches_view[0];

    for (i, field) in schema.fields().iter().enumerate() {
        let col = batch.column(i);
        let col_view = batch_view.column(i);

        if col.as_any().is::<StringArray>() {
            println!(
                "Column {} '{}' is StringArray in regular version",
                i,
                field.name()
            );
        }

        if col_view.as_any().is::<StringViewArray>() {
            println!(
                "Column {} '{}' is StringViewArray in utf8view version",
                i,
                field.name()
            );
        }
    }

    Ok(())
}
