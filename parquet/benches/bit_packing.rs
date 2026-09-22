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
use std::sync::Arc;

use arrow_array::{ArrayRef, StringArray};
use arrow_schema::{DataType as ArrowDataType, Field, Schema};
use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use parquet::arrow::ArrowSchemaConverter;
use parquet::arrow::arrow_writer::{ArrowRowGroupWriterFactory, compute_leaves};
use parquet::basic::Encoding;
use parquet::data_type::{BoolType, ByteArray, ByteArrayType, DataType, Int32Type, Int64Type};
use parquet::encoding::{DictEncoder, Encoder, get_encoder};
use parquet::encodings::levels::LevelEncoder;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::types::{ColumnDescPtr, ColumnDescriptor, ColumnPath, Type};
use rand::prelude::*;

const NUM_VALUES: usize = 16 * 1024;

fn column_desc<T: DataType>() -> ColumnDescPtr {
    ColumnDescPtr::new(ColumnDescriptor::new(
        Arc::new(
            Type::primitive_type_builder("", T::get_physical_type())
                .build()
                .unwrap(),
        ),
        0,
        0,
        ColumnPath::new(vec![]),
    ))
}

fn bench_encoding<T: DataType>(c: &mut Criterion, name: &str, values: &[T::T], encoding: Encoding) {
    let column_desc = column_desc::<T>();
    let mut group = c.benchmark_group("bit_packing");
    group.throughput(Throughput::Elements(values.len() as u64));
    group.bench_function(name, |b| {
        b.iter(|| {
            let mut encoder = get_encoder::<T>(encoding, &column_desc).unwrap();
            encoder.put(black_box(values)).unwrap();
            black_box(encoder.flush_buffer().unwrap());
        });
    });
    group.finish();
}

/// Dictionary values are encoded separately from their indices. Populate the
/// dictionary outside the timed section and measure the affected RLE/bit-packed
/// index-writing path.
fn bench_dictionary_indices(c: &mut Criterion, values: &[i32]) {
    let column_desc = column_desc::<Int32Type>();
    let mut group = c.benchmark_group("bit_packing");
    group.throughput(Throughput::Elements(values.len() as u64));
    group.bench_function("rle_dictionary/i32/256_values", |b| {
        b.iter_batched(
            || {
                let mut encoder = DictEncoder::<Int32Type>::new(column_desc.clone());
                encoder.put(values).unwrap();
                encoder
            },
            |mut encoder| black_box(encoder.write_indices().unwrap()),
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

/// Arrow byte arrays have a specialized dictionary encoder. Construct and
/// populate a fresh column writer outside the timed section, then measure the
/// close operation that writes its dictionary indices.
fn bench_arrow_dictionary_indices(c: &mut Criterion, values: &[i32]) {
    let array = Arc::new(StringArray::from_iter_values(
        values.iter().map(|value| format!("value-{value:03}")),
    )) as ArrayRef;
    let field = Arc::new(Field::new("value", ArrowDataType::Utf8, false));
    let arrow_schema = Arc::new(Schema::new(vec![field.clone()]));
    let props = Arc::new(
        WriterProperties::builder()
            .set_dictionary_enabled(true)
            .build(),
    );
    let parquet_schema = ArrowSchemaConverter::new()
        .with_coerce_types(props.coerce_types())
        .convert(&arrow_schema)
        .unwrap();
    let file_writer =
        SerializedFileWriter::new(Vec::new(), parquet_schema.root_schema_ptr(), props).unwrap();
    let factory = ArrowRowGroupWriterFactory::new(&file_writer, arrow_schema);
    let mut leaves = compute_leaves(&field, &array).unwrap();
    let leaf = leaves.pop().unwrap();

    let mut group = c.benchmark_group("bit_packing");
    group.throughput(Throughput::Elements(values.len() as u64));
    group.bench_function("rle_dictionary/byte_array_arrow/256_values", |b| {
        b.iter_batched(
            || {
                let mut writer = factory.create_column_writers(0).unwrap().pop().unwrap();
                writer.write(&leaf).unwrap();
                writer
            },
            |writer| black_box(writer.close().unwrap()),
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

/// Definition and repetition levels use the same RLE/bit-packed hybrid as
/// dictionary indices, but enter it through the streaming level API.
fn bench_levels(c: &mut Criterion, levels: &[i16]) {
    let mut group = c.benchmark_group("bit_packing");
    group.throughput(Throughput::Elements(levels.len() as u64));
    group.bench_function("rle/levels/bit_packed", |b| {
        b.iter(|| {
            let mut encoder = LevelEncoder::v2_streaming(3);
            encoder.put_with_observer(black_box(levels), |_, _| {});
            black_box(encoder.consume());
        });
    });
    group.finish();
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut rng = StdRng::seed_from_u64(0);
    let mut bools = Vec::with_capacity(NUM_VALUES);
    let mut i32s = Vec::with_capacity(NUM_VALUES);
    let mut i64s = Vec::with_capacity(NUM_VALUES);
    let mut byte_arrays = Vec::with_capacity(NUM_VALUES);
    let mut dictionary_values = Vec::with_capacity(NUM_VALUES);
    let mut levels = Vec::with_capacity(NUM_VALUES);

    for _ in 0..NUM_VALUES {
        bools.push(rng.random::<bool>());
        i32s.push(rng.random::<i32>());
        // Keep deltas below 32 bits, mimicking timestamp-like data.
        i64s.push(rng.random_range(0..1_i64 << 28));

        let id = rng.random_range(0..4096_u32);
        let suffix_len = rng.random_range(4..20);
        byte_arrays.push(ByteArray::from(
            format!("prefix/{id:04x}/{}", "x".repeat(suffix_len)).into_bytes(),
        ));

        dictionary_values.push(rng.random_range(0..256));
        levels.push(rng.random_range(0..=3));
    }

    // Direct BitWriter and RLE/bit-packed hybrid users.
    bench_encoding::<BoolType>(c, "plain/bool/bit_packed", &bools, Encoding::PLAIN);
    bench_encoding::<BoolType>(c, "rle/bool/bit_packed", &bools, Encoding::RLE);

    // DELTA_BINARY_PACKED writes each miniblock through BitWriter.
    bench_encoding::<Int32Type>(
        c,
        "delta_binary_packed/i32/random",
        &i32s,
        Encoding::DELTA_BINARY_PACKED,
    );
    bench_encoding::<Int64Type>(
        c,
        "delta_binary_packed/i64/timestamp_like",
        &i64s,
        Encoding::DELTA_BINARY_PACKED,
    );

    // These byte-array encodings transitively use DELTA_BINARY_PACKED for
    // lengths, prefix lengths, and suffix lengths.
    bench_encoding::<ByteArrayType>(
        c,
        "delta_length_byte_array/variable_length",
        &byte_arrays,
        Encoding::DELTA_LENGTH_BYTE_ARRAY,
    );
    bench_encoding::<ByteArrayType>(
        c,
        "delta_byte_array/shared_prefix",
        &byte_arrays,
        Encoding::DELTA_BYTE_ARRAY,
    );

    bench_dictionary_indices(c, &dictionary_values);
    bench_arrow_dictionary_indices(c, &dictionary_values);
    bench_levels(c, &levels);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
