// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use arrow_array::{DictionaryArray, StringArray, UInt16Array};
use std::sync::Arc;

use criterion::*;

fn criterion_benchmark(c: &mut Criterion) {
    let dict_vals = Arc::new(StringArray::from_iter_values(["a", "b", "c"]));
    for len in [128, 1536, 8092] {
        c.bench_function(&format!("null_dict/len={len}"), |b| {
            b.iter_batched(
                || dict_vals.clone(),
                |dict_vals| {
                    std::hint::black_box(DictionaryArray::new(
                        UInt16Array::new_null(len),
                        dict_vals.clone(),
                    ))
                },
                BatchSize::SmallInput,
            );
        });
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
