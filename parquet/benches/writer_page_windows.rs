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

//! Writer benchmarks for the shapes that drive *mini-batch windowing*.
//!
//! When a chunk's values do not all fit in the page byte budget, the column
//! writer splits the chunk into mini-batches. How wide those windows are is
//! decided per chunk, and on a nullable column the decision depends on the
//! ratio between the value size and `data_page_size_limit`.
//!
//! `arrow_writer` already covers the two ends of that range: values several
//! times larger than the limit (`large_string_*`, 2 MiB against the 1 MiB
//! default) and values comfortably below it (`small_string_*`, 1 KiB). The
//! shapes here sit in between, or move the limit itself, which is where the
//! window arithmetic actually varies:
//!
//! * **Sub-page values** (128 KiB and 512 KiB against the 1 MiB default), where
//!   several values share a page budget rather than one overrunning it. The
//!   `arrow_writer` case at this size (`medium_string_shared_prefix_nullable`)
//!   only covers a *shared* prefix, so the distinct case — where deduplication
//!   saves nothing and the window cost is not offset — is uncovered.
//! * **A non-default `data_page_size_limit`.** Every existing benchmark runs
//!   against the 1 MiB default, so reaching a one-value window needs multi-MiB
//!   values. A 64 KiB limit reaches the same window with far less encoding work
//!   per value to amortise the per-mini-batch overhead against, and is a
//!   realistic setting for selective reads.
//! * **Dictionary fallback into `DELTA_BYTE_ARRAY`.** Every DBA benchmark pins
//!   the encoding with `set_dictionary_enabled(false)`, so the encoder is DBA
//!   from the first value. In practice a byte-array column starts
//!   dictionary-encoded and only becomes DBA when the dictionary spills, which
//!   changes the writer's windowing part-way through a column.
//!
//! `plain` variants accompany the `delta_byte_array` ones throughout. `PLAIN`
//! does not compress a value against its predecessor, so it is insensitive to
//! where a page boundary falls: it is the control that says whether a movement
//! is windowing or the machine.

use std::io::Empty;
use std::sync::Arc;

use arrow_array::{RecordBatch, StringArray};
use criterion::{Bencher, Criterion, Throughput, criterion_group, criterion_main};
use parquet::arrow::ArrowWriter;
use parquet::basic::Encoding;
use parquet::errors::Result;
use parquet::file::properties::WriterProperties;
use std::hint::black_box;

/// Total bytes each benchmark writes per iteration, so times are comparable
/// across value sizes.
const BYTES_PER_ITER: usize = 64 * 1024 * 1024;

/// `size` rows of `value_size`-byte strings with one null every `null_every`
/// rows.
///
/// With `shared_prefix` the distinguishing counter trails the value, so
/// consecutive values share all but their last eight bytes and
/// `DELTA_BYTE_ARRAY` stores a prefix length plus a short suffix. Otherwise the
/// counter leads and every prefix length is zero, so the encoding stores each
/// value in full and deduplication buys nothing.
///
/// Values are distinct under both settings, so a dictionary keeps growing and
/// eventually spills — which is what [`bench_dictionary_fallback`] relies on.
fn nullable_batch(
    size: usize,
    value_size: usize,
    shared_prefix: bool,
    null_every: usize,
) -> Result<RecordBatch> {
    let filler = "x".repeat(value_size - 8);
    let array = Arc::new(StringArray::from_iter((0..size).map(|i| {
        (i % null_every != null_every - 1).then(|| {
            if shared_prefix {
                format!("{filler}{i:08}")
            } else {
                format!("{i:08}{filler}")
            }
        })
    }))) as _;
    Ok(RecordBatch::try_from_iter([("col", array)])?)
}

fn write_batch(bench: &mut Bencher, batch: &RecordBatch, props: &WriterProperties) {
    bench.iter(|| {
        let mut file = Empty::default();
        let mut writer =
            ArrowWriter::try_new(&mut file, batch.schema(), Some(props.clone())).unwrap();
        writer.write(black_box(batch)).unwrap();
        black_box(writer.close()).unwrap();
    });
}

fn group(c: &mut Criterion, name: &str, batch: &RecordBatch, cases: &[(&str, &WriterProperties)]) {
    let mut group = c.benchmark_group(name);
    group.throughput(Throughput::Bytes(
        batch
            .columns()
            .iter()
            .map(|f| f.get_array_memory_size() as u64)
            .sum(),
    ));
    for (case, props) in cases {
        group.bench_function(*case, |b| write_batch(b, batch, props));
    }
    group.finish();
}

fn delta() -> WriterProperties {
    WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_encoding(Encoding::DELTA_BYTE_ARRAY)
        .build()
}

fn plain() -> WriterProperties {
    WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_encoding(Encoding::PLAIN)
        .build()
}

/// Values smaller than `data_page_size_limit`, so several share a page budget.
///
/// 128 KiB and 512 KiB against the 1 MiB default bracket the range between
/// `small_string_*` (1 KiB, many values to a page) and `large_string_*` (2 MiB,
/// one value overrunning a page). The distinct and shared-prefix variants at
/// 128 KiB differ only in what deduplication has to work with, so a movement
/// present in one and absent in the other is attributable to that.
fn bench_subpage_values(c: &mut Criterion) {
    let (delta, plain) = (delta(), plain());

    for kib in [128usize, 512] {
        let value_size = kib * 1024;
        let rows = BYTES_PER_ITER / value_size;
        let batch = nullable_batch(rows, value_size, false, 16).unwrap();
        group(
            c,
            &format!("subpage_string_distinct_nullable_{kib}kib"),
            &batch,
            &[("delta_byte_array", &delta), ("plain", &plain)],
        );
    }

    let batch = nullable_batch(BYTES_PER_ITER / (128 * 1024), 128 * 1024, true, 16).unwrap();
    group(
        c,
        "subpage_string_shared_prefix_nullable_128kib",
        &batch,
        &[("delta_byte_array", &delta), ("plain", &plain)],
    );
}

/// A 64 KiB `data_page_size_limit` with 64 KiB values.
///
/// Each value fills the budget on its own, the same window the 1 MiB default
/// reaches only at multi-MiB values — but with far less encoding work per value
/// to amortise the extra mini-batch against. The shared-prefix variant is the
/// one where a narrower window can pay for itself, by keeping more values on a
/// page and so more suffixes deduplicated against a single stored value.
fn bench_small_page_limit(c: &mut Criterion) {
    const VALUE_SIZE: usize = 64 * 1024;
    const PAGE_LIMIT: usize = 64 * 1024;
    let rows = BYTES_PER_ITER / VALUE_SIZE;

    let delta = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_encoding(Encoding::DELTA_BYTE_ARRAY)
        .set_data_page_size_limit(PAGE_LIMIT)
        .build();
    let plain = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_encoding(Encoding::PLAIN)
        .set_data_page_size_limit(PAGE_LIMIT)
        .build();

    for (name, shared_prefix) in [
        ("small_page_limit_string_distinct_nullable", false),
        ("small_page_limit_string_shared_prefix_nullable", true),
    ] {
        let batch = nullable_batch(rows, VALUE_SIZE, shared_prefix, 16).unwrap();
        group(
            c,
            name,
            &batch,
            &[("delta_byte_array", &delta), ("plain", &plain)],
        );
    }
}

/// A column that starts dictionary-encoded and falls back to
/// `DELTA_BYTE_ARRAY` when the dictionary spills.
///
/// 256 KiB distinct values against the 1 MiB default dictionary page limit, so
/// the dictionary holds a handful of values before falling back and the rest of
/// the column is written by the fallback encoder. `pinned_delta` writes the
/// same data with the dictionary disabled: the difference between the two is
/// the dictionary phase and the transition, which no other benchmark covers.
fn bench_dictionary_fallback(c: &mut Criterion) {
    const VALUE_SIZE: usize = 256 * 1024;
    let rows = BYTES_PER_ITER / VALUE_SIZE;

    let fallback_delta = WriterProperties::builder()
        .set_dictionary_enabled(true)
        .set_encoding(Encoding::DELTA_BYTE_ARRAY)
        .build();
    let fallback_plain = WriterProperties::builder()
        .set_dictionary_enabled(true)
        .set_encoding(Encoding::PLAIN)
        .build();
    let pinned_delta = delta();

    let batch = nullable_batch(rows, VALUE_SIZE, false, 16).unwrap();
    group(
        c,
        "dictionary_fallback_string_distinct_nullable",
        &batch,
        &[
            ("fallback_delta_byte_array", &fallback_delta),
            ("fallback_plain", &fallback_plain),
            ("pinned_delta_byte_array", &pinned_delta),
        ],
    );
}

criterion_group!(
    benches,
    bench_subpage_values,
    bench_small_page_limit,
    bench_dictionary_fallback
);
criterion_main!(benches);
