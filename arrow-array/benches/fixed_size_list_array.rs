// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use arrow_array::{Array, FixedSizeListArray, Int32Array};
use arrow_schema::Field;
use criterion::*;
use rand::{RngExt, rng};
use std::{hint, sync::Arc};

fn gen_fsl(len: usize, value_len: usize) -> FixedSizeListArray {
    let mut rng = rng();
    let values = Arc::new(Int32Array::from(
        (0..len).map(|_| rng.random::<i32>()).collect::<Vec<_>>(),
    ));
    let field = Arc::new(Field::new_list_field(values.data_type().clone(), true));
    FixedSizeListArray::new(field, value_len as i32, values, None)
}

fn criterion_benchmark(c: &mut Criterion) {
    let len = 4096;
    for value_len in [1, 32, 1024] {
        let fsl = gen_fsl(len, value_len);
        c.bench_function(
            &format!("fixed_size_list_array(len: {len}, value_len: {value_len})"),
            |b| {
                b.iter(|| {
                    for i in 0..len / value_len {
                        hint::black_box(fsl.value(i));
                    }
                });
            },
        );
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
