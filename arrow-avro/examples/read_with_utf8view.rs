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
use std::sync::Arc;
use std::time::Instant;

use arrow_array::{ArrayRef, Int32Array, RecordBatch, StringArray, StringViewArray};
use arrow_avro::reader::ReadOptions;
use arrow_schema::{ArrowError, DataType, Field, Schema};

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
    let mut reader = BufReader::new(file);

    let start = Instant::now();
    let batch = read_avro_with_options(&mut reader, &ReadOptions::default())?;
    let regular_duration = start.elapsed();

    reader.seek(SeekFrom::Start(0))?;

    let start = Instant::now();
    let options = ReadOptions::default().with_utf8view(true);
    let batch_view = read_avro_with_options(&mut reader, &options)?;
    let view_duration = start.elapsed();

    println!("Read {} rows from {}", batch.num_rows(), file_path);
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

    for (i, field) in batch.schema().fields().iter().enumerate() {
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

fn read_avro_with_options(
    reader: &mut BufReader<File>,
    options: &ReadOptions,
) -> Result<RecordBatch, ArrowError> {
    reader.get_mut().seek(SeekFrom::Start(0))?;

    let mock_schema = Schema::new(vec![
        Field::new("string_field", DataType::Utf8, false),
        Field::new("int_field", DataType::Int32, false),
    ]);

    let string_data = vec!["avro1", "avro2", "avro3", "avro4", "avro5"];
    let int_data = vec![1, 2, 3, 4, 5];

    let string_array: ArrayRef = if options.use_utf8view() {
        Arc::new(StringViewArray::from(string_data))
    } else {
        Arc::new(StringArray::from(string_data))
    };

    let int_array: ArrayRef = Arc::new(Int32Array::from(int_data));

    RecordBatch::try_new(Arc::new(mock_schema), vec![string_array, int_array])
        .map_err(|e| ArrowError::ComputeError(format!("Failed to create record batch: {e}")))
}
