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

use arrow_array::{Array, TimestampSecondArray};
use arrow_cast::cast;
use arrow_schema::{DataType, TimeUnit};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

const ARRAY_LEN: usize = 8192;

fn cast_temporal(c: &mut Criterion) {
    // Start at 2024-01-01 noon, visiting every day of the leap year. Local
    // noon avoids ambiguous/nonexistent New York times while spanning DST.
    let timestamp: TimestampSecondArray = (0..ARRAY_LEN)
        .map(|i| {
            (i % 10 != 0)
                .then_some(1_704_110_400 + (i % 366) as i64 * 86_400 + (i / 366) as i64 * 61)
        })
        .collect();

    let mut group = c.benchmark_group("cast_temporal");
    for timezone in ["+05:30", "UTC", "Etc/GMT+5", "America/New_York"] {
        let timestamp_tz = timestamp.clone().with_timezone(timezone);
        for (operation, input, target) in [
            (
                "timestamp_to_date32",
                &timestamp_tz as &dyn Array,
                DataType::Date32,
            ),
            (
                "timestamp_to_time32",
                &timestamp_tz as &dyn Array,
                DataType::Time32(TimeUnit::Second),
            ),
            (
                "timestamp_to_utf8",
                &timestamp_tz as &dyn Array,
                DataType::Utf8,
            ),
            (
                "naive_timestamp_to_timezone",
                &timestamp as &dyn Array,
                DataType::Timestamp(TimeUnit::Second, Some(timezone.into())),
            ),
        ] {
            let output = cast(input, &target).unwrap();
            assert_eq!(output.data_type(), &target);
            assert_eq!(output.len(), ARRAY_LEN);
            assert_eq!(output.null_count(), input.null_count());

            group.bench_function(BenchmarkId::new(operation, timezone), |b| {
                b.iter(|| black_box(cast(black_box(input), black_box(&target)).unwrap()));
            });
        }
    }
    group.finish();
}

criterion_group!(benches, cast_temporal);
criterion_main!(benches);
