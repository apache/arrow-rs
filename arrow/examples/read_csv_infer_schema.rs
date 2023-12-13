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

extern crate arrow;

use arrow::csv;
use arrow::util::pretty::print_batches;
use arrow_csv::reader::Format;
use std::fs::File;
use std::io::Seek;
use std::sync::Arc;

fn main() {
    let path = format!(
        "{}/../arrow-csv/test/data/uk_cities_with_headers.csv",
        env!("CARGO_MANIFEST_DIR")
    );
    let mut file = File::open(path).unwrap();
    let format = Format::default().with_header(true);
    let (schema, _) = format.infer_schema(&mut file, Some(100)).unwrap();
    file.rewind().unwrap();

    let builder = csv::ReaderBuilder::new(Arc::new(schema)).with_format(format);
    let mut csv = builder.build(file).unwrap();
    let batch = csv.next().unwrap().unwrap();
    print_batches(&[batch]).unwrap();
}
