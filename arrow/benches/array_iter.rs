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

extern crate arrow;
#[macro_use]
extern crate criterion;

use criterion::{Criterion, Throughput};
use std::hint;

use arrow::array::*;
use arrow::util::bench_util::*;
use arrow_array::types::{Int8Type, Int16Type, Int32Type, Int64Type};

const BATCH_SIZE: usize = 64 * 1024;

/// Run [`ArrayIter::fold`] while using black_box on each item and the result of the cb to prevent compiler optimizations.
fn fold_black_box_item_and_cb_res<ArrayAcc, F, B>(array: ArrayAcc, init: B, mut f: F)
where
    ArrayAcc: ArrayAccessor,
    F: FnMut(B, Option<ArrayAcc::Item>) -> B,
{
    let result = ArrayIter::new(array).fold(hint::black_box(init), |acc, item| {
        let res = f(acc, hint::black_box(item));
        hint::black_box(res)
    });

    hint::black_box(result);
}
/// Run [`ArrayIter::fold`] while using black_box on each item to prevent compiler optimizations.
fn fold_black_box_item<ArrayAcc, F, B>(array: ArrayAcc, init: B, mut f: F)
where
    ArrayAcc: ArrayAccessor,
    F: FnMut(B, Option<ArrayAcc::Item>) -> B,
{
    let result = ArrayIter::new(array).fold(hint::black_box(init), |acc, item| {
        f(acc, hint::black_box(item))
    });

    hint::black_box(result);
}

/// Run [`ArrayIter::fold`] without using black_box on each item, but only on the result
/// to see if the compiler can do more optimizations.
fn fold_black_box_result<ArrayAcc, F, B>(array: ArrayAcc, init: B, f: F)
where
    ArrayAcc: ArrayAccessor,
    F: FnMut(B, Option<ArrayAcc::Item>) -> B,
{
    let result = ArrayIter::new(array).fold(hint::black_box(init), f);

    hint::black_box(result);
}

/// Run [`ArrayIter::any`] while using black_box on each item and the predicate return value to prevent compiler optimizations.
fn any_black_box_item_and_predicate<ArrayAcc>(
    array: ArrayAcc,
    mut any_predicate: impl FnMut(Option<ArrayAcc::Item>) -> bool,
) where
    ArrayAcc: ArrayAccessor,
{
    let any_res = ArrayIter::new(array).any(|item| {
        let item = hint::black_box(item);
        let res = any_predicate(item);
        hint::black_box(res)
    });

    hint::black_box(any_res);
}

/// Run [`ArrayIter::any`] without using black_box in the loop, but only on the result
/// to see if the compiler can do more optimizations.
fn any_black_box_result<ArrayAcc>(
    array: ArrayAcc,
    any_predicate: impl FnMut(Option<ArrayAcc::Item>) -> bool,
) where
    ArrayAcc: ArrayAccessor,
{
    let any_res = ArrayIter::new(array).any(any_predicate);

    hint::black_box(any_res);
}

/// Benchmark [`ArrayIter`] functions,
///
/// The passed `predicate_that_will_always_evaluate_to_false` function should be a predicate
/// that always returns `false` to ensure that the full array is always iterated over.
///
/// The predicate function should:
/// 1. always return false
/// 2. be impossible for the compiler to optimize away
/// 3. not use `hint::black_box` internally (unless impossible) to allow for more compiler optimizations
///
/// the way to achieve this is to make the predicate check for a value that is not presented in the array.
///
/// The reason for these requirements is that we want to iterate over the entire array while
/// letting the compiler have room for optimizations so it will be more representative of real world usage.
fn benchmark_array_iter<ArrayAcc, FoldFn, FoldInit>(
    c: &mut Criterion,
    name: &str,
    nonnull_array: ArrayAcc,
    nullable_array: ArrayAcc,
    fold_init: FoldInit,
    fold_fn: FoldFn,
    predicate_that_will_always_evaluate_to_false: impl Fn(Option<ArrayAcc::Item>) -> bool,
) where
    ArrayAcc: ArrayAccessor + Copy,
    FoldInit: Copy,
    FoldFn: Fn(FoldInit, Option<ArrayAcc::Item>) -> FoldInit,
{
    let predicate_that_will_always_evaluate_to_false =
        &predicate_that_will_always_evaluate_to_false;
    let fold_fn = &fold_fn;

    // Assert always false return false
    {
        let found = ArrayIter::new(nonnull_array).any(predicate_that_will_always_evaluate_to_false);
        assert!(!found, "The predicate must always evaluate to false");
    }
    {
        let found =
            ArrayIter::new(nullable_array).any(predicate_that_will_always_evaluate_to_false);
        assert!(!found, "The predicate must always evaluate to false");
    }

    c.benchmark_group(name)
        .throughput(Throughput::Elements(BATCH_SIZE as u64))
        // Most of the Rust default iterator functions are implemented on top of 2 functions:
        // `fold` and `try_fold`
        // so we are benchmarking `fold` first
        .bench_function("nonnull fold black box item and fold result", |b| {
            b.iter(|| fold_black_box_item_and_cb_res(nonnull_array, fold_init, fold_fn))
        })
        .bench_function("nonnull fold black box item", |b| {
            b.iter(|| fold_black_box_item(nonnull_array, fold_init, fold_fn))
        })
        .bench_function("nonnull fold black box only result", |b| {
            b.iter(|| fold_black_box_result(nonnull_array, fold_init, fold_fn))
        })
        .bench_function("null fold black box item and fold result", |b| {
            b.iter(|| fold_black_box_item_and_cb_res(nullable_array, fold_init, fold_fn))
        })
        .bench_function("null fold black box item", |b| {
            b.iter(|| fold_black_box_item(nullable_array, fold_init, fold_fn))
        })
        .bench_function("null fold black box only result", |b| {
            b.iter(|| fold_black_box_result(nullable_array, fold_init, fold_fn))
        })
        // Due to `try_fold` not being available in stable Rust,
        // we are benchmarking `any` instead which the default Rust implementation
        // uses `try_fold` under the hood.
        .bench_function("nonnull any black box item and predicate", |b| {
            b.iter(|| {
                any_black_box_item_and_predicate(
                    nonnull_array,
                    predicate_that_will_always_evaluate_to_false,
                )
            })
        })
        .bench_function("nonnull any black box only result", |b| {
            b.iter(|| {
                any_black_box_result(nonnull_array, predicate_that_will_always_evaluate_to_false)
            })
        })
        .bench_function("null any black box item and predicate", |b| {
            b.iter(|| {
                any_black_box_item_and_predicate(
                    nullable_array,
                    predicate_that_will_always_evaluate_to_false,
                )
            })
        })
        .bench_function("null any black box only result", |b| {
            b.iter(|| {
                any_black_box_result(nullable_array, predicate_that_will_always_evaluate_to_false)
            })
        });
}

/// Replace all occurrences of `item_to_replace` with `replace_with` in the given `PrimitiveArray`.
/// will make it so we can filter by missing value
fn replace_primitive_value<T>(
    array: PrimitiveArray<T>,
    item_to_replace: T::Native,
    replace_with: T::Native,
) -> PrimitiveArray<T>
where
    T: ArrowPrimitiveType,
    <T as ArrowPrimitiveType>::Native: Eq,
{
    array.unary(|item| {
        if item == item_to_replace {
            replace_with
        } else {
            item
        }
    })
}

fn add_benchmark(c: &mut Criterion) {
    benchmark_array_iter(
        c,
        "int8",
        &replace_primitive_value(create_primitive_array::<Int8Type>(BATCH_SIZE, 0.0), 42, 1),
        &replace_primitive_value(create_primitive_array::<Int8Type>(BATCH_SIZE, 0.5), 42, 1),
        // fold init
        0i8,
        // fold function
        |acc, item| acc.wrapping_add(item.unwrap_or_default()),
        // predicate that will always evaluate to false while allowing us to avoid using hint::black_box and let the compiler optimize more
        |item| item == Some(42),
    );
    benchmark_array_iter(
        c,
        "int16",
        &replace_primitive_value(create_primitive_array::<Int16Type>(BATCH_SIZE, 0.0), 42, 1),
        &replace_primitive_value(create_primitive_array::<Int16Type>(BATCH_SIZE, 0.5), 42, 1),
        // fold init
        0i16,
        // fold function
        |acc, item| acc.wrapping_add(item.unwrap_or_default()),
        // predicate that will always evaluate to false while allowing us to avoid using hint::black_box and let the compiler optimize more
        |item| item == Some(42),
    );
    benchmark_array_iter(
        c,
        "int32",
        &replace_primitive_value(create_primitive_array::<Int32Type>(BATCH_SIZE, 0.0), 42, 1),
        &replace_primitive_value(create_primitive_array::<Int32Type>(BATCH_SIZE, 0.5), 42, 1),
        // fold init
        0i32,
        // fold function
        |acc, item| acc.wrapping_add(item.unwrap_or_default()),
        // predicate that will always evaluate to false while allowing us to avoid using hint::black_box and let the compiler optimize more
        |item| item == Some(42),
    );
    benchmark_array_iter(
        c,
        "int64",
        &replace_primitive_value(create_primitive_array::<Int64Type>(BATCH_SIZE, 0.0), 42, 1),
        &replace_primitive_value(create_primitive_array::<Int64Type>(BATCH_SIZE, 0.5), 42, 1),
        // fold init
        0i64,
        // fold function
        |acc, item| acc.wrapping_add(item.unwrap_or_default()),
        // predicate that will always evaluate to false while allowing us to avoid using hint::black_box and let the compiler optimize more
        |item| item == Some(42),
    );

    benchmark_array_iter(
        c,
        "string with len 16",
        &create_string_array_with_len::<i32>(BATCH_SIZE, 0.0, 16),
        &create_string_array_with_len::<i32>(BATCH_SIZE, 0.5, 16),
        // fold init
        0_usize,
        // fold function
        |acc, item| acc.wrapping_add(item.map(|item| item.len()).unwrap_or_default()),
        // predicate that will always evaluate to false while allowing us to avoid using hint::black_box and let the compiler optimize more
        |item| item.is_some_and(|item| item.is_empty()),
    );

    benchmark_array_iter(
        c,
        "string view with len 16",
        &create_string_view_array_with_len(BATCH_SIZE, 0.0, 16, false),
        &create_string_view_array_with_len(BATCH_SIZE, 0.5, 16, false),
        // fold init
        0_usize,
        // fold function
        |acc, item| acc.wrapping_add(item.map(|item| item.len()).unwrap_or_default()),
        // predicate that will always evaluate to false while allowing us to avoid using hint::black_box and let the compiler optimize more
        |item| item.is_some_and(|item| item.is_empty()),
    );

    benchmark_array_iter(
        c,
        "boolean mixed true and false",
        &create_boolean_array(BATCH_SIZE, 0.0, 0.5),
        &create_boolean_array(BATCH_SIZE, 0.5, 0.5),
        // fold init
        0_usize,
        // fold function
        |acc, item| acc.wrapping_add(item.unwrap_or_default() as usize),
        // Must use black_box here as this can be optimized away
        |_item| hint::black_box(false),
    );
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
