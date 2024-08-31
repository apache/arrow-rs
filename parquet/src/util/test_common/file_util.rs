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

use std::{fs, path::PathBuf, str::FromStr};

/// Returns path to the test parquet file in 'data' directory
pub fn get_test_path(file_name: &str) -> PathBuf {
    let mut pathbuf = PathBuf::from_str(&arrow::util::test_util::parquet_test_data()).unwrap();
    pathbuf.push(file_name);
    pathbuf
}

/// Returns file handle for a test parquet file from 'data' directory
pub fn get_test_file(file_name: &str) -> fs::File {
    let path = get_test_path(file_name);
    fs::File::open(path.as_path()).unwrap_or_else(|err| {
        panic!(
            "Test file {} could not be opened, did you do `git submodule update`?: {}",
            path.display(),
            err
        )
    })
}
