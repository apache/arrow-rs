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
use std::sync::Arc;

use arrow::array::ArrayRef;
use criterion::{criterion_group, criterion_main, Criterion};
use parquet_variant::{Variant, VariantBuilder};
use parquet_variant_compute::{
    variant_get::{variant_get, GetOptions},
    VariantArray, VariantArrayBuilder,
};
use rand::{rngs::StdRng, Rng, SeedableRng};

fn create_primitive_variant(size: usize) -> VariantArray {
    let mut rng = StdRng::seed_from_u64(42);

    let mut variant_builder = VariantArrayBuilder::new(1);

    for _ in 0..size {
        let mut builder = VariantBuilder::new();
        builder.append_value(rng.random::<i64>());
        let (metadata, value) = builder.finish();
        variant_builder.append_variant(Variant::try_new(&metadata, &value).unwrap());
    }

    variant_builder.build()
}

pub fn variant_get_bench(c: &mut Criterion) {
    let variant_array = create_primitive_variant(8192);
    let input: ArrayRef = Arc::new(variant_array);

    let options = GetOptions {
        path: vec![].into(),
        as_type: None,
        cast_options: Default::default(),
    };

    c.bench_function("variant_get_primitive", |b| {
        b.iter(|| variant_get(&input.clone(), options.clone()))
    });
}

criterion_group!(benches, variant_get_bench);
criterion_main!(benches);
