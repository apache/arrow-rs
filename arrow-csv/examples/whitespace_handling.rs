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

use arrow_array::*;
use arrow_csv::WriterBuilder;
use arrow_schema::*;
use std::sync::Arc;

fn main() {
    // Create a sample schema with string columns
    let schema = Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("city", DataType::Utf8, false),
        Field::new("country", DataType::Utf8, false),
    ]);

    // Create sample data with leading and trailing whitespace
    let name = StringArray::from(vec![
        "  John Doe  ",
        "  Jane Smith",
        "Bob Johnson  ",
        "Alice Williams",
    ]);
    let city = StringArray::from(vec![
        "  New York  ",
        "Los Angeles  ",
        "  Chicago",
        "Houston",
    ]);
    let country = StringArray::from(vec![
        "  USA  ",
        "  USA  ",
        "  USA  ",
        "  USA  ",
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(name), Arc::new(city), Arc::new(country)],
    )
    .unwrap();

    println!("Original CSV (with whitespace):");
    let mut buf = Vec::new();
    let mut writer = WriterBuilder::new().build(&mut buf);
    writer.write(&batch).unwrap();
    drop(writer);
    println!("{}", String::from_utf8(buf).unwrap());

    println!("\nCSV with ignore_leading_whitespace:");
    let mut buf = Vec::new();
    let mut writer = WriterBuilder::new()
        .with_ignore_leading_whitespace(true)
        .build(&mut buf);
    writer.write(&batch).unwrap();
    drop(writer);
    println!("{}", String::from_utf8(buf).unwrap());

    println!("\nCSV with ignore_trailing_whitespace:");
    let mut buf = Vec::new();
    let mut writer = WriterBuilder::new()
        .with_ignore_trailing_whitespace(true)
        .build(&mut buf);
    writer.write(&batch).unwrap();
    drop(writer);
    println!("{}", String::from_utf8(buf).unwrap());

    println!("\nCSV with both ignore_leading_whitespace and ignore_trailing_whitespace:");
    let mut buf = Vec::new();
    let mut writer = WriterBuilder::new()
        .with_ignore_leading_whitespace(true)
        .with_ignore_trailing_whitespace(true)
        .build(&mut buf);
    writer.write(&batch).unwrap();
    drop(writer);
    println!("{}", String::from_utf8(buf).unwrap());
}

