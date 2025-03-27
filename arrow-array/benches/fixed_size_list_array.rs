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

use arrow_array::{Array, FixedSizeListArray, Int32Array};
use arrow_schema::Field;
use criterion::*;
use rand::{rng, Rng};
use std::sync::Arc;

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
                        black_box(fsl.value(i));
                    }
                });
            },
        );
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
