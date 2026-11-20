// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Cost of `finish` for every array builder.
//!
//! These builders build their input in the criterion setup closure, so the
//! measured time is `finish` alone. Sizes are small so the fixed per-array
//! cost of finishing dominates, and the input is always valid, which is the
//! common case for freshly built arrays.

use std::hint::black_box;

use arrow_array::Array;
use arrow_array::builder::{
    ArrayBuilder, BooleanBuilder, FixedSizeBinaryBuilder, FixedSizeBinaryDictionaryBuilder,
    FixedSizeListBuilder, GenericByteDictionaryBuilder, GenericByteRunBuilder, Int32Builder,
    ListBuilder, ListViewBuilder, MapBuilder, NullBuilder, PrimitiveBuilder,
    PrimitiveDictionaryBuilder, PrimitiveRunBuilder, StringBuilder, StringViewBuilder,
    StructBuilder, UnionBuilder,
};
use arrow_array::types::{Float64Type, Int8Type, Int32Type, UInt32Type, Utf8Type};
use arrow_schema::{DataType, Field, Fields};
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};

const SIZES: [usize; 3] = [1, 8, 64];

/// Benchmarks `ArrayBuilder::finish` for `make`-produced builders.
fn bench_finish<B: ArrayBuilder>(c: &mut Criterion, name: &str, make: fn(usize) -> B) {
    let mut group = c.benchmark_group(name);
    for len in SIZES {
        group.bench_function(format!("len:{len}"), |b| {
            b.iter_batched(
                || make(len),
                |mut builder| black_box(builder.finish()).len(),
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

fn make_primitive(len: usize) -> PrimitiveBuilder<Int32Type> {
    let mut builder = PrimitiveBuilder::<Int32Type>::with_capacity(len);
    for i in 0..len {
        builder.append_value(i as i32);
    }
    builder
}

fn make_boolean(len: usize) -> BooleanBuilder {
    let mut builder = BooleanBuilder::with_capacity(len);
    for i in 0..len {
        builder.append_value(i % 2 == 0);
    }
    builder
}

fn make_byte(len: usize) -> StringBuilder {
    let mut builder = StringBuilder::with_capacity(len, len * 8);
    for i in 0..len {
        builder.append_value(format!("value{i}"));
    }
    builder
}

fn make_fixed_size_binary(len: usize) -> FixedSizeBinaryBuilder {
    let mut builder = FixedSizeBinaryBuilder::with_capacity(len, 3);
    for _ in 0..len {
        builder.append_value(b"abc").unwrap();
    }
    builder
}

fn make_byte_view(len: usize) -> StringViewBuilder {
    let mut builder = StringViewBuilder::with_capacity(len);
    for i in 0..len {
        builder.append_value(format!("value{i}"));
    }
    builder
}

fn make_null(len: usize) -> NullBuilder {
    let mut builder = NullBuilder::new();
    builder.append_nulls(len);
    builder
}

fn make_map(len: usize) -> MapBuilder<StringBuilder, Int32Builder> {
    let mut builder = MapBuilder::new(
        None,
        StringBuilder::with_capacity(len, len * 8),
        Int32Builder::with_capacity(len),
    );
    for i in 0..len {
        builder.keys().append_value("key");
        builder.values().append_value(i as i32);
        builder.append(true).unwrap();
    }
    builder
}

fn make_primitive_dictionary(len: usize) -> PrimitiveDictionaryBuilder<Int32Type, UInt32Type> {
    let mut builder = PrimitiveDictionaryBuilder::<Int32Type, UInt32Type>::with_capacity(len, len);
    for i in 0..len {
        builder.append(i as u32).unwrap();
    }
    builder
}

fn make_byte_dictionary(len: usize) -> GenericByteDictionaryBuilder<Int8Type, Utf8Type> {
    let mut builder =
        GenericByteDictionaryBuilder::<Int8Type, Utf8Type>::with_capacity(len, len, len * 8);
    for i in 0..len {
        builder.append(format!("value{i}")).unwrap();
    }
    builder
}

fn make_fixed_size_binary_dictionary(len: usize) -> FixedSizeBinaryDictionaryBuilder<Int8Type> {
    let mut builder = FixedSizeBinaryDictionaryBuilder::<Int8Type>::new(3);
    for _ in 0..len {
        builder.append(b"abc").unwrap();
    }
    builder
}

fn make_struct(len: usize) -> StructBuilder {
    let fields = Fields::from(vec![
        Field::new("key", DataType::Utf8, true),
        Field::new("value", DataType::Int32, true),
    ]);
    let mut builder = StructBuilder::from_fields(fields, len);
    for i in 0..len {
        builder
            .field_builder::<StringBuilder>(0)
            .unwrap()
            .append_value("key");
        builder
            .field_builder::<Int32Builder>(1)
            .unwrap()
            .append_value(i as i32);
        builder.append(true);
    }
    builder
}

fn make_list(len: usize) -> ListBuilder<Int32Builder> {
    let mut builder = ListBuilder::new(Int32Builder::with_capacity(len));
    for i in 0..len {
        builder.values().append_value(i as i32);
        builder.append(true);
    }
    builder
}

fn make_fixed_size_list(len: usize) -> FixedSizeListBuilder<Int32Builder> {
    let mut builder = FixedSizeListBuilder::new(Int32Builder::with_capacity(len * 3), 3);
    for i in 0..len {
        for j in 0..3 {
            builder.values().append_value((i * 3 + j) as i32);
        }
        builder.append(true);
    }
    builder
}

fn make_list_view(len: usize) -> ListViewBuilder<Int32Builder> {
    let mut builder = ListViewBuilder::new(Int32Builder::with_capacity(len));
    for i in 0..len {
        builder.values().append_value(i as i32);
        builder.append(true);
    }
    builder
}

fn make_primitive_run(len: usize) -> PrimitiveRunBuilder<Int32Type, Int32Type> {
    // Every fourth value repeats, so the encoded arrays have both runs and
    // run ends.
    let mut builder = PrimitiveRunBuilder::<Int32Type, Int32Type>::new();
    for i in 0..len {
        builder.append_value((i / 4) as i32);
    }
    builder
}

fn make_byte_run(len: usize) -> GenericByteRunBuilder<Int32Type, Utf8Type> {
    let mut builder = GenericByteRunBuilder::<Int32Type, Utf8Type>::new();
    for i in 0..len {
        builder.append_value(format!("value{}", i / 4));
    }
    builder
}

fn make_union_dense(len: usize) -> UnionBuilder {
    let mut builder = UnionBuilder::new_dense();
    for i in 0..len {
        if i % 2 == 0 {
            builder.append::<Int32Type>("a", i as i32).unwrap();
        } else {
            builder.append::<Float64Type>("b", i as f64).unwrap();
        }
    }
    builder
}

fn make_union_sparse(len: usize) -> UnionBuilder {
    let mut builder = UnionBuilder::new_sparse();
    for i in 0..len {
        if i % 2 == 0 {
            builder.append::<Int32Type>("a", i as i32).unwrap();
        } else {
            builder.append::<Float64Type>("b", i as f64).unwrap();
        }
    }
    builder
}

fn bench_primitive_builder(c: &mut Criterion) {
    bench_finish(c, "primitive_builder_finish", make_primitive);
}

fn bench_boolean_builder(c: &mut Criterion) {
    bench_finish(c, "boolean_builder_finish", make_boolean);
}

fn bench_byte_builder(c: &mut Criterion) {
    bench_finish(c, "byte_builder_finish", make_byte);
}

fn bench_fixed_size_binary_builder(c: &mut Criterion) {
    bench_finish(
        c,
        "fixed_size_binary_builder_finish",
        make_fixed_size_binary,
    );
}

fn bench_byte_view_builder(c: &mut Criterion) {
    bench_finish(c, "byte_view_builder_finish", make_byte_view);
}

fn bench_null_builder(c: &mut Criterion) {
    bench_finish(c, "null_builder_finish", make_null);
}

fn bench_map_builder(c: &mut Criterion) {
    bench_finish(c, "map_builder_finish", make_map);
}

fn bench_primitive_dictionary_builder(c: &mut Criterion) {
    bench_finish(
        c,
        "primitive_dictionary_builder_finish",
        make_primitive_dictionary,
    );
}

fn bench_byte_dictionary_builder(c: &mut Criterion) {
    bench_finish(c, "byte_dictionary_builder_finish", make_byte_dictionary);
}

fn bench_fixed_size_binary_dictionary_builder(c: &mut Criterion) {
    bench_finish(
        c,
        "fixed_size_binary_dictionary_builder_finish",
        make_fixed_size_binary_dictionary,
    );
}

fn bench_struct_builder(c: &mut Criterion) {
    bench_finish(c, "struct_builder_finish", make_struct);
}

fn bench_list_builder(c: &mut Criterion) {
    bench_finish(c, "list_builder_finish", make_list);
}

fn bench_fixed_size_list_builder(c: &mut Criterion) {
    bench_finish(c, "fixed_size_list_builder_finish", make_fixed_size_list);
}

fn bench_list_view_builder(c: &mut Criterion) {
    bench_finish(c, "list_view_builder_finish", make_list_view);
}

fn bench_primitive_run_builder(c: &mut Criterion) {
    bench_finish(c, "primitive_run_builder_finish", make_primitive_run);
}

fn bench_byte_run_builder(c: &mut Criterion) {
    bench_finish(c, "byte_run_builder_finish", make_byte_run);
}

fn bench_union_dense_builder(c: &mut Criterion) {
    bench_finish(c, "union_dense_builder_finish", make_union_dense);
}

fn bench_union_sparse_builder(c: &mut Criterion) {
    bench_finish(c, "union_sparse_builder_finish", make_union_sparse);
}

criterion_group!(
    benches,
    bench_primitive_builder,
    bench_boolean_builder,
    bench_byte_builder,
    bench_fixed_size_binary_builder,
    bench_byte_view_builder,
    bench_null_builder,
    bench_map_builder,
    bench_primitive_dictionary_builder,
    bench_byte_dictionary_builder,
    bench_fixed_size_binary_dictionary_builder,
    bench_struct_builder,
    bench_list_builder,
    bench_fixed_size_list_builder,
    bench_list_view_builder,
    bench_primitive_run_builder,
    bench_byte_run_builder,
    bench_union_dense_builder,
    bench_union_sparse_builder,
);
criterion_main!(benches);
