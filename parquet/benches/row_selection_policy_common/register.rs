// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::hint;
use std::sync::{Arc, OnceLock};

use criterion::{Criterion, Throughput};

use super::assertions::preflight_auto;
use super::fixture::{CaseFixture, build_fixture};
use super::model::CaseSpec;
use super::runner::run_auto;

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
