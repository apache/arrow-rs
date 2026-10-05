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

//! Auto-policy benchmark for async Parquet `RowSelection` execution.
//!
//! This benchmark is intended for direct comparisons between `main` and
//! candidate branches. Forced row-selection policies and decode-then-filter
//! diagnostics intentionally belong in separate oracle benchmarks.
//! Page indexes are intentionally disabled so this benchmark isolates
//! row-selection execution from page-level I/O pruning.

mod row_selection_policy_common;

use criterion::{Criterion, criterion_group, criterion_main};
use row_selection_policy_common::cases::{
    ORDER_CASES, SCALE_CASES, SHAPE_CASES, assert_order_cases_are_reverses,
};
use row_selection_policy_common::register::register_auto_group;
use row_selection_policy_common::shapes::assert_shape_contracts;

fn benchmark_auto(c: &mut Criterion) {
    assert_shape_contracts();
    assert_order_cases_are_reverses();

    register_auto_group(
        c,
        "arrow_reader_row_selection_policy/auto/shape",
        SHAPE_CASES,
    );
    register_auto_group(
        c,
        "arrow_reader_row_selection_policy/auto/order",
        ORDER_CASES,
    );
    register_auto_group(
        c,
        "arrow_reader_row_selection_policy/auto/scale",
        SCALE_CASES,
    );
}

criterion_group!(benches, benchmark_auto);
criterion_main!(benches);
