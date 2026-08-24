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

use std::hint;
use std::sync::{Arc, OnceLock};

use criterion::{Criterion, Throughput};

use super::assertions::{preflight_auto, preflight_heterogeneous};
use super::fixture::{CaseFixture, build_fixture, build_heterogeneous_fixture};
use super::model::CaseSpec;
use super::runner::{run, run_auto};
use parquet::arrow::arrow_reader::RowSelectionPolicy;

pub(crate) fn register_auto_group(c: &mut Criterion, group_name: &str, cases: &'static [CaseSpec]) {
    let runtime = Arc::new(
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap(),
    );
    let mut group = c.benchmark_group(group_name);

    for case in cases {
        group.throughput(Throughput::Elements(case.total_rows() as u64));

        let fixture: Arc<OnceLock<Arc<CaseFixture>>> = Arc::new(OnceLock::new());
        let preflight = Arc::new(OnceLock::new());
        let runtime = Arc::clone(&runtime);

        group.bench_function(case.name, move |b| {
            let fixture =
                Arc::clone(fixture.get_or_init(|| Arc::new(build_fixture(case).unwrap())));
            preflight.get_or_init(|| {
                runtime.block_on(preflight_auto(case, &fixture));
            });

            b.iter(|| {
                let rows = runtime.block_on(run_auto(&fixture));
                hint::black_box(rows);
            });
        });
    }

    group.finish();
}

pub(crate) fn register_heterogeneous_group(
    c: &mut Criterion,
    group_name: &str,
    cases: &'static [CaseSpec],
) {
    let runtime = Arc::new(
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap(),
    );
    let mut group = c.benchmark_group(group_name);

    for case in cases {
        group.throughput(Throughput::Elements(case.total_rows() as u64));

        let fixture: Arc<OnceLock<Arc<CaseFixture>>> = Arc::new(OnceLock::new());
        let preflight = Arc::new(OnceLock::new());

        for (policy_name, policy) in [
            ("auto", RowSelectionPolicy::default()),
            ("auto_per_column", RowSelectionPolicy::AutoPerColumn),
            ("selectors", RowSelectionPolicy::Selectors),
            ("mask", RowSelectionPolicy::Mask),
        ] {
            let fixture = Arc::clone(&fixture);
            let preflight = Arc::clone(&preflight);
            let runtime = Arc::clone(&runtime);

            group.bench_function(format!("{}/{policy_name}", case.name), move |b| {
                let fixture = Arc::clone(
                    fixture.get_or_init(|| Arc::new(build_heterogeneous_fixture(case).unwrap())),
                );
                preflight.get_or_init(|| {
                    runtime.block_on(preflight_heterogeneous(case, &fixture));
                });

                b.iter(|| {
                    let rows = runtime.block_on(run(&fixture, policy));
                    hint::black_box(rows);
                });
            });
        }
    }

    group.finish();
}
