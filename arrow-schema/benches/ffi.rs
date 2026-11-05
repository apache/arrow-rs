// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use arrow_schema::ffi::FFI_ArrowSchema;
use arrow_schema::{DataType, Field};
use criterion::*;
use std::sync::Arc;

fn criterion_benchmark(c: &mut Criterion) {
    let fields = vec![
        Arc::new(Field::new("c1", DataType::Utf8, false)),
        Arc::new(Field::new("c2", DataType::Utf8, false)),
        Arc::new(Field::new("c3", DataType::Utf8, false)),
        Arc::new(Field::new("c4", DataType::Utf8, false)),
        Arc::new(Field::new("c5", DataType::Utf8, false)),
    ];
    let data_type = DataType::Struct(fields.into());
    c.bench_function("ffi_arrow_schema_try_from", |b| {
        b.iter(|| FFI_ArrowSchema::try_from(&data_type));
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
