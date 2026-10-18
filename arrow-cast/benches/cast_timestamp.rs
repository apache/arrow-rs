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

use std::hint::black_box;

use arrow_array::{PrimitiveArray, types::*};
use arrow_cast::cast;
use arrow_schema::{DataType, TimeUnit};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

const SIZE: usize = 8192;

fn benchmark<T: ArrowTimestampType>(c: &mut Criterion, unit: &str, units_per_second: i64) {
    // Timestamps spread over roughly 63 years either side of the epoch.
    let mut rng = StdRng::seed_from_u64(42);
    let range = -2_000_000_000 * units_per_second..2_000_000_000 * units_per_second;
    let values: Vec<i64> = (0..SIZE).map(|_| rng.random_range(range.clone())).collect();
    let mixed_nulls = PrimitiveArray::<T>::from_iter(
        values
            .iter()
            .enumerate()
            .map(|(i, &v)| (i % 5 != 0).then_some(v)),
    );
    let no_nulls = PrimitiveArray::<T>::new(values.into(), None);
    // Timezone-aware casts go through Chrono and serve as a control.
    let zoned = no_nulls.clone().with_timezone("+05:45");
    let inputs = [
        ("no_nulls", &no_nulls),
        ("mixed_nulls", &mixed_nulls),
        ("timezone_control", &zoned),
    ];
    let targets = [
        ("date32", DataType::Date32),
        ("time32_s", DataType::Time32(TimeUnit::Second)),
        ("time32_ms", DataType::Time32(TimeUnit::Millisecond)),
        ("time64_us", DataType::Time64(TimeUnit::Microsecond)),
        ("time64_ns", DataType::Time64(TimeUnit::Nanosecond)),
    ];
    let mut group = c.benchmark_group(format!("timestamp_{unit}"));
    group.throughput(Throughput::Elements(SIZE as u64));
    for (name, target) in &targets {
        for (input, array) in inputs {
            group.bench_function(format!("{name}/{input}"), |b| {
                b.iter(|| black_box(cast(black_box(array), target).unwrap()))
            });
        }
    }
    group.finish();
}

fn cast_timestamp(c: &mut Criterion) {
    benchmark::<TimestampSecondType>(c, "s", 1);
    benchmark::<TimestampMillisecondType>(c, "ms", 1_000);
    benchmark::<TimestampMicrosecondType>(c, "us", 1_000_000);
    benchmark::<TimestampNanosecondType>(c, "ns", 1_000_000_000);
}

criterion_group!(benches, cast_timestamp);
criterion_main!(benches);
