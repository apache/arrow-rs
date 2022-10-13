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

//! Common code used in the integration test binaries

use serde_json::Value;

use arrow::datatypes::Schema;
use arrow::error::Result;
use arrow::record_batch::RecordBatch;
use arrow::util::test_util::arrow_test_data;
use arrow_integration_test::*;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;

/// The expected username for the basic auth integration test.
pub const AUTH_USERNAME: &str = "arrow";
/// The expected password for the basic auth integration test.
pub const AUTH_PASSWORD: &str = "flight";

pub mod flight_client_scenarios;
pub mod flight_server_scenarios;

pub struct ArrowFile {
    pub schema: Schema,
    // we can evolve this into a concrete Arrow type
    // this is temporarily not being read from
    pub _dictionaries: HashMap<i64, ArrowJsonDictionaryBatch>,
    pub batches: Vec<RecordBatch>,
}

pub fn read_json_file(json_name: &str) -> Result<ArrowFile> {
    let json_file = File::open(json_name)?;
    let reader = BufReader::new(json_file);
    let arrow_json: Value = serde_json::from_reader(reader).unwrap();
    let schema = schema_from_json(&arrow_json["schema"])?;
    // read dictionaries
    let mut dictionaries = HashMap::new();
    if let Some(dicts) = arrow_json.get("dictionaries") {
        for d in dicts
            .as_array()
            .expect("Unable to get dictionaries as array")
        {
            let json_dict: ArrowJsonDictionaryBatch = serde_json::from_value(d.clone())
                .expect("Unable to get dictionary from JSON");
            // TODO: convert to a concrete Arrow type
            dictionaries.insert(json_dict.id, json_dict);
        }
    }

    let mut batches = vec![];
    for b in arrow_json["batches"].as_array().unwrap() {
        let json_batch: ArrowJsonBatch = serde_json::from_value(b.clone()).unwrap();
        let batch = record_batch_from_json(&schema, json_batch, Some(&dictionaries))?;
        batches.push(batch);
    }
    Ok(ArrowFile {
        schema,
        _dictionaries: dictionaries,
        batches,
    })
}

/// Read gzipped JSON test file
pub fn read_gzip_json(version: &str, path: &str) -> ArrowJson {
    use flate2::read::GzDecoder;
    use std::io::Read;

    let testdata = arrow_test_data();
    let file = File::open(format!(
        "{}/arrow-ipc-stream/integration/{}/{}.json.gz",
        testdata, version, path
    ))
    .unwrap();
    let mut gz = GzDecoder::new(&file);
    let mut s = String::new();
    gz.read_to_string(&mut s).unwrap();
    // convert to Arrow JSON
    let arrow_json: ArrowJson = serde_json::from_str(&s).unwrap();
    arrow_json
}
