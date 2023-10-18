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

use arrow_array::cast::AsArray;
use arrow_array::types::Int16Type;
use arrow_csv::ReaderBuilder;

use arrow_schema::{DataType, Field, Schema};
use std::fs::File;
use std::sync::Arc;

fn main() {
    // read csv from file
    let file = File::open("arrow-csv/test/data/example.csv").unwrap();
    let csv_schema = Schema::new(vec![
        Field::new("c1", DataType::Int16, true),
        Field::new("c2", DataType::Float32, true),
        Field::new("c3", DataType::Utf8, true),
        Field::new("c4", DataType::Boolean, true),
    ]);
    let mut reader = ReaderBuilder::new(Arc::new(csv_schema))
        .with_header(true)
        .build(file)
        .unwrap();

    match reader.next() {
        Some(r) => match r {
            Ok(r) => {
                // get the column(0) max value
                let col = r.column(0).as_primitive::<Int16Type>();
                let max = col.iter().max().flatten();
                println!("max value column(0): {max:?}")
            }
            Err(e) => {
                println!("{e:?}");
            }
        },
        None => {
            println!("csv is empty");
        }
    }
}
