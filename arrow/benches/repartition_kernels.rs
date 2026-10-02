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

//! End-to-end benchmark that mimics the hot loop of DataFusion's `RepartitionExec`.
//!
//! Each criterion iteration processes `NUM_ITERATIONS` record batches in sequence,
//! calling `repartition_take_push` once per batch:
//!   1. Map pre-computed hashes → partition assignments
//!   2. Single-pass scatter: build per-partition `Vec<u32>` of row indices
//!   3. `take_record_batch` for each non-empty partition
//!   4. `push_batch` into the partition's `BatchCoalescer`
//!
//! A second benchmark ("v2") will call a different implementation for the same
//! 10-batch loop, enabling apples-to-apples comparison.

use arrow::compute::take_record_batch;
use arrow::util::bench_util::*;
use arrow_array::types::{Float64Type, Int32Type, TimestampNanosecondType};
use arrow_array::{ArrayRef, RecordBatch, UInt32Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow_select::coalesce::BatchCoalescer;
use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;
use foldhash::fast::FixedState;
use std::hash::BuildHasher;
use std::mem;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Benchmark parameters
// ---------------------------------------------------------------------------

/// Number of batches to process per criterion iteration.
const NUM_ITERATIONS: usize = 100;

/// Rows per batch — DataFusion's default batch size.
const BATCH_SIZE: usize = 8_192;

/// Target output batch size for each partition's coalescer.
const COALESCE_BATCH_SIZE: usize = 8_192;

/// Partition counts to exercise.  Includes one non-power-of-two (30) so that
/// modulo-based partition assignment is exercised alongside power-of-two counts.
const OUTPUT_PARTITION_COUNTS: &[usize] = &[4, 8, 16, 30, 64, 128];

// ---------------------------------------------------------------------------
// Hash function — ported from DataFusion's create_hashes
// ---------------------------------------------------------------------------

/// Fold a second column's hash into a running hash.
///
/// Verbatim copy of `combine_hashes` in `datafusion/common/src/hash_utils.rs`.
#[inline(always)]
fn combine_hashes(l: u64, r: u64) -> u64 {
    let hash = (17 * 37u64).wrapping_add(l);
    hash.wrapping_mul(37).wrapping_add(r)
}

/// Build `len` hashes using `foldhash::fast::FixedState` — the same hasher
/// DataFusion aliases as `RandomState` in `create_hashes`.
///
/// `batch_seed` gives each of the `NUM_ITERATIONS` arrays distinct values while
/// remaining fully deterministic across criterion iterations.
fn make_hashes(len: usize, batch_seed: u64) -> Vec<u64> {
    let state = FixedState::with_seed(batch_seed);
    (0..len as u64)
        .map(|row| combine_hashes(state.hash_one(row), state.hash_one(batch_seed)))
        .collect()
}

// ---------------------------------------------------------------------------
// Batch creation helpers
// ---------------------------------------------------------------------------

fn make_column(data_type: &DataType, null_density: f32, max_str_len: usize, seed: u64) -> ArrayRef {
    match data_type {
        DataType::Int32 => Arc::new(create_primitive_array_with_seed::<Int32Type>(
            BATCH_SIZE,
            null_density,
            seed,
        )),
        DataType::Float64 => Arc::new(create_primitive_array_with_seed::<Float64Type>(
            BATCH_SIZE,
            null_density,
            seed,
        )),
        DataType::Timestamp(TimeUnit::Nanosecond, Some(tz)) => Arc::new(
            create_primitive_array_with_seed::<TimestampNanosecondType>(
                BATCH_SIZE,
                null_density,
                seed,
            )
            .with_timezone(Arc::clone(tz)),
        ),
        DataType::Utf8View => Arc::new(create_string_view_array_with_max_len(
            BATCH_SIZE,
            null_density,
            max_str_len,
        )),
        DataType::Utf8 => Arc::new(create_string_array_with_max_len::<i32>(
            BATCH_SIZE,
            null_density,
            max_str_len,
        )),
        DataType::FixedSizeBinary(n) => {
            Arc::new(create_fsb_array(BATCH_SIZE, null_density, *n as usize))
        }
        _ => panic!("unsupported data type in repartition benchmark: {data_type}"),
    }
}

fn make_batch(schema: &SchemaRef, null_density: f32, max_str_len: usize, seed: u64) -> RecordBatch {
    let columns = schema
        .fields()
        .iter()
        .map(|f| make_column(f.data_type(), null_density, max_str_len, seed))
        .collect::<Vec<_>>();
    RecordBatch::try_new(Arc::clone(schema), columns).unwrap()
}

// ---------------------------------------------------------------------------
// Core repartition function (v1)
// ---------------------------------------------------------------------------

/// Process one batch through the repartition pipeline (v1 — take then push).
///
/// Matches DataFusion's `RepartitionExec` hot loop:
///   1. Single-pass scatter: assign every row to its output partition bucket.
///   2. For each non-empty bucket: materialise rows via `take_record_batch`.
///   3. Hand the slice to the partition's `BatchCoalescer`.
///
/// ### `partition_indices` reuse
/// Callers allocate `partition_indices` once (length == `output_partitions`) and
/// pass it in on every call.  Inside, each bucket's `Vec<u32>` is moved into a
/// `UInt32Array` via `mem::take` — zero-copy, no duplicate allocation.  The slot
/// is left empty and regrows naturally during the next call's scatter step,
/// giving a uniform per-call allocation profile across all `NUM_ITERATIONS` calls.
///
/// ### Draining
/// On non-final calls a single `next_completed_batch` is issued per partition —
/// enough to keep memory bounded without blocking the next batch.
/// Set `is_final = true` on the last batch to flush in-progress coalescer
/// buffers via `finish_buffered_batch` and drain all remaining completed batches.
#[inline(never)]
pub fn repartition_take_push(
    batch: &RecordBatch,
    hashes: &[u64],
    output_partitions: usize,
    partition_indices: &mut [Vec<u32>],
    coalescers: &mut [BatchCoalescer],
    is_final: bool,
) {
    debug_assert_eq!(partition_indices.len(), output_partitions);
    debug_assert_eq!(hashes.len(), batch.num_rows());

    // Retain heap capacity from the previous call; reset length only.
    for bucket in partition_indices.iter_mut() {
        bucket.clear();
    }

    // Single-pass scatter: one pass over hashes, O(n) with no branch per partition.
    for (row_idx, &hash) in hashes.iter().enumerate() {
        let partition = (hash % output_partitions as u64) as usize;
        partition_indices[partition].push(row_idx as u32);
    }

    // Materialise each non-empty partition and hand it to its coalescer.
    for partition in 0..output_partitions {
        if partition_indices[partition].is_empty() {
            continue;
        }
        let index_array = UInt32Array::from(mem::replace(
            &mut partition_indices[partition],
            Vec::with_capacity(BATCH_SIZE / output_partitions + 1),
        ));
        let partition_batch = black_box(take_record_batch(batch, &index_array).unwrap());
        coalescers[partition].push_batch(partition_batch).unwrap();

        // Non-final: drain all completed batches before moving to the next
        // incoming batch so the coalescer does not hold onto unbounded memory.
        if !is_final {
            while coalescers[partition].next_completed_batch().is_some() {}
        }
    }

    // Final batch: drain completed batches first, then flush the in-progress
    // partial buffer so the coalescer is fully consumed.
    if is_final {
        for coalescer in coalescers.iter_mut() {
            while coalescer.next_completed_batch().is_some() {}
            coalescer.finish_buffered_batch().unwrap();
        }
    }
}

// ---------------------------------------------------------------------------
// Benchmark driver
// ---------------------------------------------------------------------------

struct RepartitionScenario {
    name: &'static str,
    schema: SchemaRef,
    /// Maximum string length for variable-width columns (0 for non-string schemas).
    max_str_len: usize,
}

fn add_repartition_benchmarks(c: &mut Criterion) {
    let primitive_schema = SchemaRef::new(Schema::new(vec![
        Field::new("i32", DataType::Int32, true),
        Field::new("f64", DataType::Float64, true),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            true,
        ),
    ]));

    let utf8view_schema = SchemaRef::new(Schema::new(vec![Field::new(
        "label",
        DataType::Utf8View,
        true,
    )]));

    let fsb_schema = SchemaRef::new(Schema::new(vec![Field::new(
        "id",
        DataType::FixedSizeBinary(16),
        true,
    )]));

    let utf8_schema = SchemaRef::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]));

    // Mixed: primitive numeric columns plus a view string — representative of a
    // wide fact table with IDs, measurements, and a category label.
    let mixed_schema = SchemaRef::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("amount", DataType::Float64, true),
        Field::new("label", DataType::Utf8View, true),
    ]));

    let scenarios = [
        RepartitionScenario {
            name: "primitive",
            schema: primitive_schema,
            max_str_len: 0,
        },
        // Utf8View: strings ≤12 bytes are stored inline; >12 bytes spill to a
        // separate buffer.  Benchmarking both characterises the two paths.
        RepartitionScenario {
            name: "utf8view/short",
            schema: Arc::clone(&utf8view_schema),
            max_str_len: 8,
        },
        RepartitionScenario {
            name: "utf8view/long",
            schema: utf8view_schema,
            max_str_len: 30,
        },
        RepartitionScenario {
            name: "fsb16",
            schema: fsb_schema,
            max_str_len: 0,
        },
        RepartitionScenario {
            name: "utf8",
            schema: utf8_schema,
            max_str_len: 30,
        },
        RepartitionScenario {
            name: "mixed/short",
            schema: Arc::clone(&mixed_schema),
            max_str_len: 8,
        },
        RepartitionScenario {
            name: "mixed/long",
            schema: mixed_schema,
            max_str_len: 30,
        },
    ];

    for scenario in &scenarios {
        // Pre-generate batches once — batch data creation is excluded from timing.
        // Hashes are computed inside b.iter() so their cost is included.
        let batches: Vec<RecordBatch> = (0..NUM_ITERATIONS)
            .map(|seed| make_batch(&scenario.schema, 0.0, scenario.max_str_len, seed as u64))
            .collect();
        // Hashes precomputed outside b.iter() — excluded from timing.
        // Swap with the inside-iter version below to measure hash cost.
        let hashes: Vec<Vec<u64>> = (0..NUM_ITERATIONS)
            .map(|s| make_hashes(BATCH_SIZE, s as u64))
            .collect();
        let schema = Arc::clone(&scenario.schema);

        for &output_partitions in OUTPUT_PARTITION_COUNTS {
            let id = format!(
                "repartition/take_push/{}/output_partitions={output_partitions}/rows={BATCH_SIZE}",
                scenario.name
            );

            let mut partition_indices: Vec<Vec<u32>> =
                vec![Vec::with_capacity(BATCH_SIZE / output_partitions + 1); output_partitions];

            c.bench_function(&id, |b| {
                b.iter(|| {
                    let mut coalescers: Vec<BatchCoalescer> = (0..output_partitions)
                        .map(|_| BatchCoalescer::new(Arc::clone(&schema), COALESCE_BATCH_SIZE))
                        .collect();

                    for (batch_idx, batch) in batches.iter().enumerate() {
                        let is_final = batch_idx == NUM_ITERATIONS - 1;
                        repartition_take_push(
                            batch,
                            &hashes[batch_idx],
                            output_partitions,
                            &mut partition_indices,
                            &mut coalescers,
                            is_final,
                        );
                    }
                    black_box(&coalescers);
                })
            });
        }
    }
}

criterion_group!(benches, add_repartition_benchmarks);
criterion_main!(benches);
