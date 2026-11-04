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

use criterion::*;

use parquet_variant::VariantPath;

fn bench_parse_path(c: &mut Criterion) {
    let mut group = c.benchmark_group("variant_path_parse");
    for (name, path) in [
        // Dot notation never enters the bracket parser
        ("dot_fields", "foo.bar.baz"),
        ("bracket_quoted_fields", "['foo']['bar']['baz']"),
        ("bracket_indexes", "[0][1][2]"),
        ("bracket_wildcards", "[*][*][*]"),
        // Escapes require building an unescaped copy of the field name
        ("bracket_escaped_field", r"['escaped\]field']"),
        ("mixed", "a['b.c'].d[2]['3']"),
    ] {
        group.bench_function(name, |b| {
            b.iter(|| VariantPath::try_from(std::hint::black_box(path)).unwrap());
        });
    }
    group.finish();
}

criterion_group!(benches, bench_parse_path);

criterion_main!(benches);
