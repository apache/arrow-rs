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

//! Allocation-only companion to the uninstrumented Criterion benchmark.

#[path = "../benches/variant_get/mod.rs"]
mod variant_get;

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::hint::black_box;

use parquet_variant_compute::variant_get as get;

#[derive(Clone, Copy, Default, Debug, PartialEq)]
struct Stats {
    allocations: usize,
    allocated_bytes: usize,
    live: isize,
    peak: isize,
}

thread_local! {
    static STATS: Cell<Option<Stats>> = const { Cell::new(None) };
}

struct TrackingAllocator;

#[global_allocator]
static GLOBAL: TrackingAllocator = TrackingAllocator;

fn record(allocated: usize, freed: usize, allocation: bool) {
    STATS.with(|cell| {
        if let Some(mut stats) = cell.get() {
            stats.allocations += usize::from(allocation);
            stats.allocated_bytes += allocated;
            stats.live += allocated as isize - freed as isize;
            stats.peak = stats.peak.max(stats.live);
            cell.set(Some(stats));
        }
    });
}

// As in arrow_reader_peak_memory: forward the original pointer/layout to System.
// Only the synchronous kernel call on this thread is tracked. The caller keeps
// all input/options owners alive, so no pre-measurement allocation is released.
#[expect(unsafe_code)]
unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            record(layout.size(), 0, true);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        record(0, layout.size(), false);
        unsafe { System.dealloc(ptr, layout) };
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let result = unsafe { System.realloc(ptr, layout, new_size) };
        if !result.is_null() {
            // Count a successful realloc once, with its entire requested size.
            record(new_size, layout.size(), true);
        }
        result
    }
}

fn main() {
    // Check the tracker with a known allocation/reallocation/deallocation before
    // trusting kernel measurements. No formatting or assertions inside tracking.
    STATS.with(|s| s.set(Some(Stats::default())));
    let mut control = Vec::<u8>::with_capacity(black_box(16));
    control.push(1);
    control.reserve_exact(black_box(31));
    black_box(&control);
    let held = STATS.with(|s| s.get().unwrap());
    drop(control);
    let released = STATS.with(|s| s.replace(None).unwrap());
    assert_eq!(
        held,
        Stats {
            allocations: 2,
            allocated_bytes: 48,
            live: 32,
            peak: 32
        }
    );
    assert_eq!(released.live, 0);

    println!(
        "case,rows,sample,allocations,allocated_bytes,peak_additional_live,new_live_retained,new_live_after_drop,known_missing_as_null"
    );
    variant_get::for_each_fixture(|fixture, rows| {
        let mut first = None;
        for sample in 0..5 {
            STATS.with(|s| s.set(Some(Stats::default())));
            let output =
                black_box(get(black_box(&fixture.input), fixture.options.clone()).unwrap());
            let held = STATS.with(|s| s.get().unwrap());
            drop(output);
            let released = STATS.with(|s| s.replace(None).unwrap());
            assert!(held.live >= 0);
            assert_eq!(
                released.live, 0,
                "{}: output did not release new memory",
                fixture.name
            );
            if let Some(first) = first {
                assert_eq!(held, first, "{}: unstable allocation sample", fixture.name);
            }
            first = Some(held);
            println!(
                "{},{rows},{sample},{},{},{},{},{},{}",
                fixture.name,
                held.allocations,
                held.allocated_bytes,
                held.peak,
                held.live,
                released.live,
                fixture.known_missing_as_null
            );
        }
    });
}
