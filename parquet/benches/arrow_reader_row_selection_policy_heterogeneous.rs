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

//! Baseline benchmark for row-selection execution over a heterogeneous
//! projection. The fixture combines fixed-width, variable-width, dictionary,
//! and fixed-length byte-array decoding while keeping the logical selection
//! identical for every output column.
//!
//! Page indexes are intentionally disabled so this benchmark isolates
//! row-selection execution from page-level I/O pruning.

mod row_selection_policy_common;

use criterion::{Criterion, criterion_group, criterion_main};
use row_selection_policy_common::cases::HETEROGENEOUS_CASES;
use row_selection_policy_common::register::register_heterogeneous_group;
use row_selection_policy_common::shapes::assert_shape_contracts;

fn benchmark_heterogeneous(c: &mut Criterion) {
    assert_shape_contracts();
    register_heterogeneous_group(
        c,
        "arrow_reader_row_selection_policy/heterogeneous",
        HETEROGENEOUS_CASES,
    );
}

criterion_group!(benches, benchmark_heterogeneous);
criterion_main!(benches);
