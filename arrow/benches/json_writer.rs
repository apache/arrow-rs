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

use arrow::datatypes::*;
use arrow::util::bench_util::{
    create_primitive_array, create_string_array, create_string_array_with_len,
    create_string_dict_array,
};
use arrow::util::test_util::seedable_rng;
use arrow_array::{Array, ListArray, RecordBatch, StructArray};
use arrow_buffer::{BooleanBuffer, NullBuffer, OffsetBuffer};
use arrow_json::{LineDelimitedWriter, ReaderBuilder};
use rand::Rng;
use serde::Serialize;
use std::sync::Arc;

const NUM_ROWS: usize = 65536;

fn do_bench(c: &mut Criterion, name: &str, batch: &RecordBatch) {
    c.bench_function(name, |b| {
        b.iter(|| {
            let mut out = Vec::with_capacity(1024);
            LineDelimitedWriter::new(&mut out).write(batch).unwrap();
            out
        })
    });
}

fn create_mixed(len: usize) -> RecordBatch {
    let c1 = Arc::new(create_string_array::<i32>(len, 0.));
    let c2 = Arc::new(create_primitive_array::<Int32Type>(len, 0.));
    let c3 = Arc::new(create_primitive_array::<UInt32Type>(len, 0.));
    let c4 = Arc::new(create_string_array_with_len::<i32>(len, 0.2, 10));
    let c5 = Arc::new(create_string_array_with_len::<i32>(len, 0.2, 20));
    let c6 = Arc::new(create_primitive_array::<Float32Type>(len, 0.2));
    RecordBatch::try_from_iter([
        ("c1", c1 as _),
        ("c2", c2 as _),
        ("c3", c3 as _),
        ("c4", c4 as _),
        ("c5", c5 as _),
        ("c6", c6 as _),
    ])
    .unwrap()
}

fn create_nulls(len: usize) -> NullBuffer {
    let mut rng = seedable_rng();
    BooleanBuffer::from_iter((0..len).map(|_| rng.random_bool(0.2))).into()
}

fn create_offsets(len: usize) -> (usize, OffsetBuffer<i32>) {
    let mut rng = seedable_rng();
    let mut last_offset = 0;
    let mut offsets = Vec::with_capacity(len + 1);
    offsets.push(0);
    for _ in 0..len {
        let len = rng.random_range(0..10);
        offsets.push(last_offset + len);
        last_offset += len;
    }
    (
        *offsets.last().unwrap() as _,
        OffsetBuffer::new(offsets.into()),
    )
}

fn create_nullable_struct(len: usize) -> StructArray {
    let c2 = StructArray::from(create_mixed(len));
    StructArray::new(
        c2.fields().clone(),
        c2.columns().to_vec(),
        Some(create_nulls(c2.len())),
    )
}

fn bench_float(c: &mut Criterion) {
    let c1 = Arc::new(create_primitive_array::<Float32Type>(NUM_ROWS, 0.));
    let c2 = Arc::new(create_primitive_array::<Float64Type>(NUM_ROWS, 0.));

    let batch = RecordBatch::try_from_iter([("c1", c1 as _), ("c2", c2 as _)]).unwrap();

    do_bench(c, "bench_float", &batch)
}

fn bench_integer(c: &mut Criterion) {
    let c1 = Arc::new(create_primitive_array::<UInt64Type>(NUM_ROWS, 0.));
    let c2 = Arc::new(create_primitive_array::<Int32Type>(NUM_ROWS, 0.));
    let c3 = Arc::new(create_primitive_array::<UInt32Type>(NUM_ROWS, 0.));

    let batch =
        RecordBatch::try_from_iter([("c1", c1 as _), ("c2", c2 as _), ("c3", c3 as _)]).unwrap();

    do_bench(c, "bench_integer", &batch)
}

fn bench_mixed(c: &mut Criterion) {
    let batch = create_mixed(NUM_ROWS);
    do_bench(c, "bench_mixed", &batch)
}

fn bench_dict_array(c: &mut Criterion) {
    let c1 = Arc::new(create_string_dict_array::<Int32Type>(NUM_ROWS, 0., 30));
    let c2 = Arc::new(create_string_dict_array::<Int32Type>(NUM_ROWS, 0., 20));
    let c3 = Arc::new(create_string_dict_array::<Int32Type>(NUM_ROWS, 0.1, 20));

    let batch =
        RecordBatch::try_from_iter([("c1", c1 as _), ("c2", c2 as _), ("c3", c3 as _)]).unwrap();

    do_bench(c, "bench_dict_array", &batch)
}

fn bench_string(c: &mut Criterion) {
    let c1 = Arc::new(create_string_array::<i32>(NUM_ROWS, 0.));
    let c2 = Arc::new(create_string_array_with_len::<i32>(NUM_ROWS, 0., 10));
    let c3 = Arc::new(create_string_array_with_len::<i32>(NUM_ROWS, 0.1, 20));

    let batch =
        RecordBatch::try_from_iter([("c1", c1 as _), ("c2", c2 as _), ("c3", c3 as _)]).unwrap();

    do_bench(c, "bench_string", &batch)
}

fn bench_struct(c: &mut Criterion) {
    let c1 = Arc::new(create_string_array::<i32>(NUM_ROWS, 0.));
    let c2 = Arc::new(StructArray::from(create_mixed(NUM_ROWS)));
    let batch = RecordBatch::try_from_iter([("c1", c1 as _), ("c2", c2 as _)]).unwrap();

    do_bench(c, "bench_struct", &batch)
}

fn bench_nullable_struct(c: &mut Criterion) {
    let c1 = Arc::new(create_string_array::<i32>(NUM_ROWS, 0.));
    let c2 = Arc::new(create_nullable_struct(NUM_ROWS));
    let batch = RecordBatch::try_from_iter([("c1", c1 as _), ("c2", c2 as _)]).unwrap();

    do_bench(c, "bench_nullable_struct", &batch)
}

fn bench_list(c: &mut Criterion) {
    let (values_len, offsets) = create_offsets(NUM_ROWS);
    let c1_values = Arc::new(create_string_array::<i32>(values_len, 0.));
    let c1_field = Arc::new(Field::new_list_field(c1_values.data_type().clone(), false));
    let c1 = Arc::new(ListArray::new(c1_field, offsets, c1_values, None));
    let batch = RecordBatch::try_from_iter([("c1", c1 as _)]).unwrap();
    do_bench(c, "bench_list", &batch)
}

fn bench_nullable_list(c: &mut Criterion) {
    let (values_len, offsets) = create_offsets(NUM_ROWS);
    let c1_values = Arc::new(create_string_array::<i32>(values_len, 0.1));
    let c1_field = Arc::new(Field::new_list_field(c1_values.data_type().clone(), true));
    let c1_nulls = create_nulls(NUM_ROWS);
    let c1 = Arc::new(ListArray::new(c1_field, offsets, c1_values, Some(c1_nulls)));
    let batch = RecordBatch::try_from_iter([("c1", c1 as _)]).unwrap();
    do_bench(c, "bench_nullable_list", &batch)
}

fn bench_struct_list(c: &mut Criterion) {
    let (values_len, offsets) = create_offsets(NUM_ROWS);
    let c1_values = Arc::new(create_nullable_struct(values_len));
    let c1_field = Arc::new(Field::new_list_field(c1_values.data_type().clone(), true));
    let c1_nulls = create_nulls(NUM_ROWS);
    let c1 = Arc::new(ListArray::new(c1_field, offsets, c1_values, Some(c1_nulls)));
    let batch = RecordBatch::try_from_iter([("c1", c1 as _)]).unwrap();
    do_bench(c, "bench_struct_list", &batch)
}

fn do_number_to_string_bench<S: Serialize>(
    name: &str,
    c: &mut Criterion,
    schema: Arc<Schema>,
    rows: Vec<S>,
) {
    c.bench_function(name, |b| {
        b.iter(|| {
            let mut decoder = ReaderBuilder::new(schema.clone())
                .with_coerce_primitive(true) // important for coercion
                .build_decoder()
                .expect("Failed to build decoder");

            decoder.serialize(&rows).expect("Failed to serialize rows");

            decoder
                .flush()
                .expect("Failed to flush")
                .expect("No RecordBatch produced");
        })
    });
}

fn bench_i64_to_string(c: &mut Criterion) {
    #[derive(Serialize)]
    struct TestRow {
        val: i64,
    }

    let schema = Arc::new(Schema::new(vec![Field::new("val", DataType::Utf8, false)]));

    let a_bunch_of_numbers = create_primitive_array::<Int64Type>(NUM_ROWS, 0.0);

    let rows: Vec<TestRow> = (0..NUM_ROWS)
        .map(|i| TestRow {
            val: a_bunch_of_numbers.value(i),
        })
        .collect();

    do_number_to_string_bench("i64_to_string", c, schema, rows)
}

fn bench_i32_to_string(c: &mut Criterion) {
    #[derive(Serialize)]
    struct TestRow {
        val: i32,
    }

    let schema = Arc::new(Schema::new(vec![Field::new("val", DataType::Utf8, false)]));

    let a_bunch_of_numbers = create_primitive_array::<Int32Type>(NUM_ROWS, 0.0);

    let rows: Vec<TestRow> = (0..NUM_ROWS)
        .map(|i| TestRow {
            val: a_bunch_of_numbers.value(i),
        })
        .collect();

    do_number_to_string_bench("i32_to_string", c, schema, rows)
}

fn bench_f32_to_string(c: &mut Criterion) {
    #[derive(Serialize)]
    struct TestRow {
        val: f32,
    }

    let schema = Arc::new(Schema::new(vec![Field::new("val", DataType::Utf8, false)]));

    let a_bunch_of_numbers = create_primitive_array::<Float32Type>(NUM_ROWS, 0.0);

    let rows: Vec<TestRow> = (0..NUM_ROWS)
        .map(|i| TestRow {
            val: a_bunch_of_numbers.value(i),
        })
        .collect();

    do_number_to_string_bench("f32_to_string", c, schema, rows)
}

fn bench_f64_to_string(c: &mut Criterion) {
    #[derive(Serialize)]
    struct TestRow {
        val: f64,
    }

    let schema = Arc::new(Schema::new(vec![Field::new("val", DataType::Utf8, false)]));

    let a_bunch_of_numbers = create_primitive_array::<Float64Type>(NUM_ROWS, 0.0);

    let rows: Vec<TestRow> = (0..NUM_ROWS)
        .map(|i| TestRow {
            val: a_bunch_of_numbers.value(i),
        })
        .collect();

    do_number_to_string_bench("f64_to_string", c, schema, rows)
}

fn bench_mixed_numbers_to_string(c: &mut Criterion) {
    #[derive(Serialize)]
    struct TestRow {
        val1: f64,
        val2: f32,
        val3: i64,
        val4: i32,
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("val1", DataType::Utf8, false),
        Field::new("val2", DataType::Utf8, false),
        Field::new("val3", DataType::Utf8, false),
        Field::new("val4", DataType::Utf8, false),
    ]));

    let f64_array = create_primitive_array::<Float64Type>(NUM_ROWS, 0.0);
    let f32_array = create_primitive_array::<Float32Type>(NUM_ROWS, 0.0);
    let i64_array = create_primitive_array::<Int64Type>(NUM_ROWS, 0.0);
    let i32_array = create_primitive_array::<Int32Type>(NUM_ROWS, 0.0);

    let rows: Vec<TestRow> = (0..NUM_ROWS)
        .map(|i| TestRow {
            val1: f64_array.value(i),
            val2: f32_array.value(i),
            val3: i64_array.value(i),
            val4: i32_array.value(i),
        })
        .collect();

    do_number_to_string_bench("mixed_numbers_to_string", c, schema, rows)
}

fn criterion_benchmark(c: &mut Criterion) {
    bench_integer(c);
    bench_float(c);
    bench_string(c);
    bench_mixed(c);
    bench_dict_array(c);
    bench_struct(c);
    bench_nullable_struct(c);
    bench_list(c);
    bench_nullable_list(c);
    bench_struct_list(c);
    bench_f64_to_string(c);
    bench_f32_to_string(c);
    bench_i64_to_string(c);
    bench_i32_to_string(c);
    bench_mixed_numbers_to_string(c);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
