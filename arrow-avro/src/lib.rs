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

//! Convert data to / from the [Apache Arrow] memory format and [Apache Avro]
//!
//! [Apache Arrow]: https://arrow.apache.org
//! [Apache Avro]: https://avro.apache.org/

#![doc(
    html_logo_url = "https://arrow.apache.org/img/arrow-logo_chevrons_black-txt_white-bg.svg",
    html_favicon_url = "https://arrow.apache.org/img/arrow-logo_chevrons_black-txt_transparent-bg.svg"
)]
#![cfg_attr(docsrs, feature(doc_auto_cfg))]
#![warn(missing_docs)]
#![allow(unused)] // Temporary

pub mod reader;
mod schema;

mod compression;

mod codec;

#[cfg(test)]
mod test_util {
    pub fn arrow_test_data(path: &str) -> String {
        match std::env::var("ARROW_TEST_DATA") {
            Ok(dir) => format!("{dir}/{path}"),
            Err(_) => format!("../testing/data/{path}"),
        }
    }
}
