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

use std::hint;
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};

const INNER_ITERS: usize = 100;
const BATCH_SIZES: &[usize] = &[8_192, 16_384];
const PARTITION_COUNTS: &[usize] = &[4, 8, 16, 32, 64, 128, 256, 512];

fn fnv1a(mut x: u64) -> u64 {
    const PRIME: u64 = 0x00000100000001B3;
    const BASIS: u64 = 0xcbf29ce484222325;
    let mut h = BASIS;
    for _ in 0..8 {
        h ^= x & 0xFF;
        h = h.wrapping_mul(PRIME);
        x >>= 8;
    }
    h
}

/// `assignment[i]` = which output partition row i belongs to.
fn make_partition_assignment(num_rows: usize, num_partitions: usize) -> Vec<u32> {
    let mask = (num_partitions - 1) as u64;
    let mut out = Vec::with_capacity(num_rows);
    unsafe { out.set_len(num_rows) };
    for i in 0..num_rows {
        out[i] = (fnv1a(i as u64) & mask) as u32;
    }
    out
}

// ---------------------------------------------------------------------------
// Scan implementations
// ---------------------------------------------------------------------------

/// Baseline: iterator chain filter + collect.
fn scan_scalar(assignment: &[u32], p: u32) -> Vec<u32> {
    assignment
        .iter()
        .enumerate()
        .filter(|&(_, &v)| v == p)
        .map(|(i, _)| i as u32)
        .collect()
}

/// Branchless: write unconditionally, advance output pointer only on match.
/// Pre-allocates the full array length to avoid any realloc / branch on push.
fn scan_branchless(assignment: &[u32], p: u32) -> Vec<u32> {
    let mut out = Vec::with_capacity(assignment.len());
    // SAFETY: we initialise exactly `count` elements before truncating.
    unsafe { out.set_len(assignment.len()) };
    let mut count = 0usize;
    for (i, &v) in assignment.iter().enumerate() {
        unsafe { *out.get_unchecked_mut(count) = i as u32 };
        count += (v == p) as usize;
    }
    unsafe { out.set_len(count) };
    out
}

/// AVX2: broadcast target into 256-bit register, compare 8 u32s per cycle,
/// extract 8-bit match mask, emit hit positions via tzcnt.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn scan_avx2_inner(assignment: &[u32], p: u32) -> Vec<u32> {
    use std::arch::x86_64::*;
    let mut out = Vec::with_capacity(assignment.len());
    let target = _mm256_set1_epi32(p as i32);
    let chunks = assignment.len() / 8;
    for c in 0..chunks {
        let ptr = assignment.as_ptr().add(c * 8) as *const __m256i;
        let data = _mm256_loadu_si256(ptr);
        let eq = _mm256_cmpeq_epi32(data, target);
        let mut mask = _mm256_movemask_ps(_mm256_castsi256_ps(eq)) as u8;
        let base = (c * 8) as u32;
        while mask != 0 {
            let bit = mask.trailing_zeros();
            out.push(base + bit);
            mask &= mask - 1;
        }
    }
    // scalar tail
    for i in (chunks * 8)..assignment.len() {
        if *assignment.get_unchecked(i) == p {
            out.push(i as u32);
        }
    }
    out
}

fn scan_simd(assignment: &[u32], p: u32) -> Vec<u32> {
    #[cfg(target_arch = "x86_64")]
    if is_x86_feature_detected!("avx2") {
        return unsafe { scan_avx2_inner(assignment, p) };
    }
    scan_scalar(assignment, p)
}

/// Software prefetch: issue a prefetch hint 16 elements (~1 cache line) ahead
/// of the current position before doing the scalar comparison.
fn scan_prefetch(assignment: &[u32], p: u32) -> Vec<u32> {
    const DIST: usize = 16;
    let mut out = Vec::with_capacity(assignment.len());
    let len = assignment.len();
    for i in 0..len {
        #[cfg(target_arch = "x86_64")]
        if i + DIST < len {
            unsafe {
                std::arch::x86_64::_mm_prefetch(
                    assignment.as_ptr().add(i + DIST) as *const i8,
                    std::arch::x86_64::_MM_HINT_T0,
                );
            }
        }
        if assignment[i] == p {
            out.push(i as u32);
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Write benchmark (unchanged)
// ---------------------------------------------------------------------------

fn bench_repartition(c: &mut Criterion) {
    let mut group = c.benchmark_group("repartition_write");
    for &num_rows in BATCH_SIZES {
        for &num_partitions in PARTITION_COUNTS {
            let id = format!("rows={num_rows}/partitions={num_partitions}");

            group.bench_with_input(
                BenchmarkId::new("clone_indices", &id),
                &(num_rows, num_partitions),
                |b, &(num_rows, num_partitions)| {
                    b.iter(|| {
                        for _ in 0..INNER_ITERS {
                            let assignment =
                                Arc::new(make_partition_assignment(num_rows, num_partitions));
                            for _ in 0..num_partitions {
                                hint::black_box(Arc::clone(&assignment));
                            }
                        }
                    })
                },
            );
        }
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Read benchmarks: scalar / branchless / simd / prefetch
// ---------------------------------------------------------------------------

fn bench_repartition_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("repartition_read");
    for &num_rows in BATCH_SIZES {
        for &num_partitions in PARTITION_COUNTS {
            let assignment = make_partition_assignment(num_rows, num_partitions);
            let id = format!("rows={num_rows}/partitions={num_partitions}");

            group.bench_with_input(
                BenchmarkId::new("scalar", &id),
                &num_partitions,
                |b, &num_partitions| {
                    b.iter(|| {
                        for _ in 0..INNER_ITERS {
                            for p in 0..num_partitions as u32 {
                                hint::black_box(scan_scalar(&assignment, p));
                            }
                        }
                    })
                },
            );

            group.bench_with_input(
                BenchmarkId::new("branchless", &id),
                &num_partitions,
                |b, &num_partitions| {
                    b.iter(|| {
                        for _ in 0..INNER_ITERS {
                            for p in 0..num_partitions as u32 {
                                hint::black_box(scan_branchless(&assignment, p));
                            }
                        }
                    })
                },
            );

            group.bench_with_input(
                BenchmarkId::new("simd", &id),
                &num_partitions,
                |b, &num_partitions| {
                    b.iter(|| {
                        for _ in 0..INNER_ITERS {
                            for p in 0..num_partitions as u32 {
                                hint::black_box(scan_simd(&assignment, p));
                            }
                        }
                    })
                },
            );

            group.bench_with_input(
                BenchmarkId::new("prefetch", &id),
                &num_partitions,
                |b, &num_partitions| {
                    b.iter(|| {
                        for _ in 0..INNER_ITERS {
                            for p in 0..num_partitions as u32 {
                                hint::black_box(scan_prefetch(&assignment, p));
                            }
                        }
                    })
                },
            );
        }
    }
    group.finish();
}

criterion_group!(benches, bench_repartition, bench_repartition_read);
criterion_main!(benches);
