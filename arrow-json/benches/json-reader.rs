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

use arbitrary::{Arbitrary, Unstructured};
use arrow_json::ReaderBuilder;
use arrow_json::reader::{Decoder, infer_json_schema};
use arrow_schema::{DataType, Field, Schema};
use criterion::{
    BenchmarkId, Criterion, SamplingMode, Throughput, criterion_group, criterion_main,
};
use serde::Serialize;
use serde_json::{Map, Number, Value};
use std::fmt::Write;
use std::hint::black_box;
use std::sync::Arc;

const ROWS: usize = 1 << 17; // 128K rows
const BATCH_SIZE: usize = 1 << 13; // 8K rows per batch

const WIDE_FIELDS: usize = 64;
const BINARY_BYTES: usize = 64;
const WIDE_PROJECTION_TOTAL_FIELDS: usize = 100; // 100 fields total, select only 3

fn decode_and_flush(decoder: &mut Decoder, data: &[u8]) {
    let mut offset = 0;
    while offset < data.len() {
        let read = decoder.decode(black_box(&data[offset..])).unwrap();
        if read == 0 {
            break;
        }
        offset += read;
        while let Some(_batch) = decoder.flush().unwrap() {}
    }
}

fn build_schema(field_count: usize) -> Arc<Schema> {
    // Builds a schema with fields named f0..f{field_count-1}, all Int64 and non-nullable.
    let fields: Vec<Field> = (0..field_count)
        .map(|i| Field::new(format!("f{i}"), DataType::Int64, false))
        .collect();
    Arc::new(Schema::new(fields))
}

fn build_projection_schema(indices: &[usize]) -> Arc<Schema> {
    let fields: Vec<Field> = indices
        .iter()
        .map(|i| Field::new(format!("f{i}"), DataType::Int64, false))
        .collect();
    Arc::new(Schema::new(fields))
}

fn build_wide_json(rows: usize, fields: usize) -> Vec<u8> {
    // Builds newline-delimited JSON objects with "wide" schema.
    // Example (rows=2, fields=3):
    // {"f0":0,"f1":1,"f2":2}
    // {"f0":1,"f1":2,"f2":3}
    let mut out = String::with_capacity(rows * fields * 12);
    for row in 0..rows {
        out.push('{');
        for field in 0..fields {
            if field > 0 {
                out.push(',');
            }
            let value = row as i64 + field as i64;
            write!(&mut out, "\"f{field}\":{value}").unwrap();
        }
        out.push('}');
        out.push('\n');
    }
    out.into_bytes()
}

fn build_wide_values(rows: usize, fields: usize) -> Vec<Value> {
    // Mirrors build_wide_json but returns structured serde_json::Value objects.
    let mut out = Vec::with_capacity(rows);
    for row in 0..rows {
        let mut map = Map::with_capacity(fields);
        for field in 0..fields {
            let key = format!("f{field}");
            let value = Number::from((row + field) as i64);
            map.insert(key, Value::Number(value));
        }
        out.push(Value::Object(map));
    }
    out
}

fn bench_decode_wide_object(c: &mut Criterion) {
    let data = build_wide_json(ROWS, WIDE_FIELDS);
    let schema = build_schema(WIDE_FIELDS);

    c.bench_function("decode_wide_object_i64_json", |b| {
        b.iter(|| {
            let mut decoder = ReaderBuilder::new(schema.clone())
                .with_batch_size(BATCH_SIZE)
                .build_decoder()
                .unwrap();
            decode_and_flush(&mut decoder, &data);
        })
    });
}

fn bench_serialize_wide_object(c: &mut Criterion) {
    let values = build_wide_values(ROWS, WIDE_FIELDS);
    let schema = build_schema(WIDE_FIELDS);

    c.bench_function("decode_wide_object_i64_serialize", |b| {
        b.iter(|| {
            let mut decoder = ReaderBuilder::new(schema.clone())
                .with_batch_size(BATCH_SIZE)
                .build_decoder()
                .unwrap();

            decoder.serialize(&values).unwrap();
            while let Some(_batch) = decoder.flush().unwrap() {}
        })
    });
}

fn bench_decode_binary(c: &mut Criterion, name: &str, data: &[u8], field: Arc<Field>) {
    c.bench_function(name, |b| {
        b.iter(|| {
            let mut decoder = ReaderBuilder::new_with_field(field.clone())
                .with_batch_size(BATCH_SIZE)
                .build_decoder()
                .unwrap();
            decode_and_flush(&mut decoder, data);
        })
    });
}

#[inline]
fn append_hex_byte(buf: &mut String, byte: u8) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    buf.push(HEX[(byte >> 4) as usize] as char);
    buf.push(HEX[(byte & 0x0f) as usize] as char);
}

fn build_hex_lines(rows: usize, bytes_per_row: usize) -> Vec<u8> {
    let mut data = String::with_capacity(rows * (bytes_per_row * 2 + 3));
    for row in 0..rows {
        data.push('"');
        for i in 0..bytes_per_row {
            let byte = ((row + i) & 0xff) as u8;
            append_hex_byte(&mut data, byte);
        }
        data.push('"');
        data.push('\n');
    }
    data.into_bytes()
}

fn bench_binary_hex(c: &mut Criterion) {
    let binary_data = build_hex_lines(ROWS, BINARY_BYTES);

    let binary_field = Arc::new(Field::new("item", DataType::Binary, false));
    bench_decode_binary(c, "decode_binary_hex_json", &binary_data, binary_field);

    let fixed_field = Arc::new(Field::new(
        "item",
        DataType::FixedSizeBinary(BINARY_BYTES as i32),
        false,
    ));
    bench_decode_binary(c, "decode_fixed_binary_hex_json", &binary_data, fixed_field);

    let view_field = Arc::new(Field::new("item", DataType::BinaryView, false));
    bench_decode_binary(c, "decode_binary_view_hex_json", &binary_data, view_field);
}

fn bench_decode_schema(c: &mut Criterion, name: &str, data: &[u8], schema: Arc<Schema>) {
    let mut group = c.benchmark_group(name);
    group.throughput(Throughput::Bytes(data.len() as u64));
    group.sample_size(50);
    group.measurement_time(std::time::Duration::from_secs(5));
    group.warm_up_time(std::time::Duration::from_secs(2));
    group.sampling_mode(SamplingMode::Flat);
    group.bench_function(BenchmarkId::from_parameter(ROWS), |b| {
        b.iter(|| {
            let mut decoder = ReaderBuilder::new(schema.clone())
                .with_batch_size(BATCH_SIZE)
                .build_decoder()
                .unwrap();
            decode_and_flush(&mut decoder, data);
        })
    });
    group.finish();
}

fn build_wide_projection_json(rows: usize, total_fields: usize) -> Vec<u8> {
    // Estimate: each field ~15 bytes ("fXX":VVVVVVV,), total ~15*100 + overhead
    let per_row_size = total_fields * 15 + 10;
    let mut data = String::with_capacity(rows * per_row_size);

    for _row in 0..rows {
        data.push('{');
        for i in 0..total_fields {
            if i > 0 {
                data.push(',');
            }
            // Use fixed-width values for stable benchmarks: 7 digits
            let _ = write!(data, "\"f{}\":{:07}", i, i);
        }
        data.push('}');
        data.push('\n');
    }
    data.into_bytes()
}

fn bench_wide_projection(c: &mut Criterion) {
    // Wide projection workload: tests overhead of parsing unused fields
    let wide_projection_data = build_wide_projection_json(ROWS, WIDE_PROJECTION_TOTAL_FIELDS);

    let full_schema = build_schema(WIDE_PROJECTION_TOTAL_FIELDS);
    bench_decode_schema(
        c,
        "decode_wide_projection_full_json",
        &wide_projection_data,
        full_schema,
    );

    // Projected schema: only 3 fields (f0, f10, f50) out of 100
    let projected_schema = build_projection_schema(&[0, 10, 50]);
    bench_decode_schema(
        c,
        "decode_wide_projection_narrow_json",
        &wide_projection_data,
        projected_schema,
    );
}

fn bench_schema_inference(c: &mut Criterion) {
    const ROWS: usize = 1000;

    #[derive(Serialize, Arbitrary, Debug)]
    struct Row {
        a: Option<i16>,
        b: Option<String>,
        c: Option<[i16; 8]>,
        d: Option<[bool; 8]>,
        e: Option<Inner>,
        f: f64,
    }

    #[derive(Serialize, Arbitrary, Debug)]
    struct Inner {
        a: Option<i16>,
        b: Option<String>,
        c: Option<bool>,
    }

    let mut data = vec![];
    for row in pseudorandom_sequence::<Row>(ROWS) {
        serde_json::to_writer(&mut data, &row).unwrap();
        data.push(b'\n');
    }

    let mut group = c.benchmark_group("infer_json_schema");
    group.throughput(Throughput::Bytes(data.len() as u64));
    group.sample_size(50);
    group.measurement_time(std::time::Duration::from_secs(5));
    group.warm_up_time(std::time::Duration::from_secs(2));
    group.sampling_mode(SamplingMode::Flat);
    group.bench_function(BenchmarkId::from_parameter(ROWS), |b| {
        b.iter(|| infer_json_schema(black_box(&data[..]), None).unwrap())
    });
    group.finish();
}

fn pseudorandom_sequence<T: for<'a> Arbitrary<'a>>(len: usize) -> Vec<T> {
    static RAND_BYTES: &[u8; 255] = &[
        12, 135, 254, 243, 18, 5, 38, 175, 60, 58, 204, 103, 15, 88, 201, 199, 57, 63, 56, 234,
        106, 111, 238, 119, 214, 50, 110, 89, 129, 185, 112, 115, 35, 239, 188, 189, 49, 184, 194,
        146, 108, 131, 213, 43, 236, 81, 61, 20, 21, 52, 223, 220, 215, 74, 210, 27, 190, 107, 174,
        142, 237, 66, 75, 1, 53, 181, 82, 158, 68, 134, 176, 229, 157, 116, 233, 153, 84, 139, 151,
        8, 171, 59, 105, 242, 40, 69, 94, 170, 4, 187, 212, 156, 65, 90, 192, 216, 29, 222, 122,
        230, 198, 154, 155, 245, 45, 178, 123, 23, 117, 168, 149, 17, 177, 48, 54, 241, 202, 44,
        232, 64, 221, 252, 161, 91, 93, 143, 240, 102, 172, 209, 224, 186, 197, 219, 247, 71, 36,
        101, 133, 113, 6, 137, 231, 162, 31, 7, 22, 138, 47, 136, 2, 244, 141, 173, 99, 25, 95, 96,
        85, 249, 42, 251, 217, 16, 205, 98, 203, 92, 114, 14, 163, 150, 144, 10, 125, 13, 195, 72,
        41, 67, 246, 11, 77, 132, 83, 37, 24, 183, 226, 250, 109, 248, 33, 76, 9, 55, 159, 34, 62,
        196, 87, 3, 39, 28, 166, 167, 255, 206, 79, 191, 228, 193, 179, 97, 182, 148, 73, 120, 211,
        253, 70, 227, 51, 169, 130, 145, 218, 78, 180, 165, 46, 127, 152, 26, 140, 207, 19, 100,
        104, 80, 164, 126, 118, 200, 128, 86, 160, 32, 30, 225, 147, 124, 121, 235, 208,
    ];

    let bytes: Vec<u8> = RAND_BYTES
        .iter()
        .flat_map(|i| RAND_BYTES.map(|j| i.wrapping_add(j)))
        .take(1000 * len)
        .collect();

    let mut u = Unstructured::new(&bytes);

    (0..len)
        .map(|_| u.arbitrary::<T>().unwrap())
        .take(len)
        .collect()
}

criterion_group!(
    benches,
    bench_decode_wide_object,
    bench_serialize_wide_object,
    bench_binary_hex,
    bench_wide_projection,
    bench_schema_inference
);
criterion_main!(benches);
