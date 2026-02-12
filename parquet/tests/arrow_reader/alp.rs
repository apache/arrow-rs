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

use arrow::util::test_util::parquet_test_data;
use parquet::arrow::arrow_reader::ArrowReaderBuilder;

#[test]
fn test_read_single_f64_alp_page_layout() {
    let path = std::path::PathBuf::from(parquet_test_data()).join("single_f64_ALP.parquet");
    if !path.exists() {
        eprintln!("Skipping ALP test file not found: {}", path.display());
        return;
    }

    let file = std::fs::File::open(path).unwrap();
    let mut reader = ArrowReaderBuilder::try_new(file).unwrap().build().unwrap();

    let err = loop {
        match reader.next() {
            Some(Ok(_)) => continue,
            Some(Err(err)) => break err,
            None => panic!("Expected ALP decode to fail with NYI"),
        }
    };

    assert!(err.to_string().contains("Encoding ALP page layout parsed"));
}
