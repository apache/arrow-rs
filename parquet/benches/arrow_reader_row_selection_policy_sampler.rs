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

//! Offline paired sampler for the per-column row-selection cost model.
//!
//! This is intentionally separate from Criterion benchmarks: it emits raw,
//! resumable observations for offline analysis instead of producing a single
//! benchmark summary.

mod row_selection_policy_sampler;

fn main() {
    if let Err(error) = row_selection_policy_sampler::run() {
        eprintln!("row-selection sampler failed: {error}");
        std::process::exit(1);
    }
}
