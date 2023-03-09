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

use arrow_cast::parse::string_to_timestamp_nanos;
use criterion::*;

fn criterion_benchmark(c: &mut Criterion) {
    let timestamps = [
        "2020-09-08",
        "2020-09-08T13:42:29",
        "2020-09-08T13:42:29.190",
        "2020-09-08T13:42:29.190855",
        "2020-09-08T13:42:29.190855999",
        "2020-09-08T13:42:29+00:00",
        "2020-09-08T13:42:29.190+00:00",
        "2020-09-08T13:42:29.190855+00:00",
        "2020-09-08T13:42:29.190855999-05:00",
        "2020-09-08T13:42:29.190855Z",
    ];

    for timestamp in timestamps {
        let t = black_box(timestamp);
        c.bench_function(t, |b| {
            b.iter(|| string_to_timestamp_nanos(t).unwrap());
        });
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
