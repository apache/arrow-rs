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

use bytes::Bytes;
use criterion::*;
use parquet::file::reader::SerializedFileReader;
use parquet::file::serialized_reader::ReadOptionsBuilder;

fn criterion_benchmark(c: &mut Criterion) {
    // Read file into memory to isolate filesystem performance
    let file = "../parquet-testing/data/alltypes_tiny_pages.parquet";
    let data = std::fs::read(file).unwrap();
    let data = Bytes::from(data);

    c.bench_function("open(default)", |b| {
        b.iter(|| SerializedFileReader::new(data.clone()).unwrap())
    });

    c.bench_function("open(page index)", |b| {
        b.iter(|| {
            let options = ReadOptionsBuilder::new().with_page_index().build();
            SerializedFileReader::new_with_options(data.clone(), options).unwrap()
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
