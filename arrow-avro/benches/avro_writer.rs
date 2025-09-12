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

//! Benchmarks for `arrow-avro` Writer (Avro Object Container File)

extern crate arrow_avro;
extern crate criterion;
extern crate once_cell;

use arrow_array::{
    builder::{ListBuilder, StringBuilder},
    types::{Int32Type, Int64Type, IntervalMonthDayNanoType, TimestampMicrosecondType},
    ArrayRef, BinaryArray, BooleanArray, Decimal128Array, Decimal256Array, Decimal32Array,
    Decimal64Array, FixedSizeBinaryArray, Float32Array, Float64Array, ListArray, PrimitiveArray,
    RecordBatch, StringArray, StructArray,
};
use arrow_avro::writer::AvroWriter;
use arrow_buffer::i256;
use arrow_schema::{DataType, Field, IntervalUnit, Schema, TimeUnit};
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use once_cell::sync::Lazy;
use rand::{
    distr::uniform::{SampleRange, SampleUniform},
    rngs::StdRng,
    Rng, SeedableRng,
};
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempfile;

const SIZES: [usize; 4] = [4_096, 8_192, 100_000, 1_000_000];
const BASE_SEED: u64 = 0x5EED_1234_ABCD_EF01;
const MIX_CONST_1: u64 = 0x9E37_79B1_85EB_CA87;
const MIX_CONST_2: u64 = 0xC2B2_AE3D_27D4_EB4F;

#[inline]
fn rng_for(tag: u64, n: usize) -> StdRng {
    let seed = BASE_SEED ^ tag.wrapping_mul(MIX_CONST_1) ^ (n as u64).wrapping_mul(MIX_CONST_2);
    StdRng::seed_from_u64(seed)
}

#[inline]
fn sample_in<T, Rg>(rng: &mut StdRng, range: Rg) -> T
where
    T: SampleUniform,
    Rg: SampleRange<T>,
{
    rng.random_range(range)
}

#[inline]
fn make_bool_array_with_tag(n: usize, tag: u64) -> BooleanArray {
    let mut rng = rng_for(tag, n);
    // Can't use SampleUniform for bool; use the RNG's boolean helper
    let values = (0..n).map(|_| rng.random_bool(0.5));
    // This repo exposes `from_iter`, not `from_iter_values` for BooleanArray
    BooleanArray::from_iter(values.map(Some))
}

#[inline]
fn make_i32_array_with_tag(n: usize, tag: u64) -> PrimitiveArray<Int32Type> {
    let mut rng = rng_for(tag, n);
    let values = (0..n).map(|_| rng.random::<i32>());
    PrimitiveArray::<Int32Type>::from_iter_values(values)
}

#[inline]
fn make_i64_array_with_tag(n: usize, tag: u64) -> PrimitiveArray<Int64Type> {
    let mut rng = rng_for(tag, n);
    let values = (0..n).map(|_| rng.random::<i64>());
    PrimitiveArray::<Int64Type>::from_iter_values(values)
}

#[inline]
fn rand_ascii_string(rng: &mut StdRng, min_len: usize, max_len: usize) -> String {
    let len = rng.random_range(min_len..=max_len);
    (0..len)
        .map(|_| (rng.random_range(b'a'..=b'z') as char))
        .collect()
}

#[inline]
fn make_utf8_array_with_tag(n: usize, tag: u64) -> StringArray {
    let mut rng = rng_for(tag, n);
    let data: Vec<String> = (0..n).map(|_| rand_ascii_string(&mut rng, 3, 16)).collect();
    StringArray::from_iter_values(data)
}

#[inline]
fn make_f32_array_with_tag(n: usize, tag: u64) -> Float32Array {
    let mut rng = rng_for(tag, n);
    let values = (0..n).map(|_| rng.random::<f32>());
    Float32Array::from_iter_values(values)
}

#[inline]
fn make_f64_array_with_tag(n: usize, tag: u64) -> Float64Array {
    let mut rng = rng_for(tag, n);
    let values = (0..n).map(|_| rng.random::<f64>());
    Float64Array::from_iter_values(values)
}

#[inline]
fn make_binary_array_with_tag(n: usize, tag: u64) -> BinaryArray {
    let mut rng = rng_for(tag, n);
    let mut payloads: Vec<Vec<u8>> = Vec::with_capacity(n);
    for _ in 0..n {
        let len = rng.random_range(1..=16);
        let mut p = vec![0u8; len];
        rng.fill(&mut p[..]);
        payloads.push(p);
    }
    let views: Vec<&[u8]> = payloads.iter().map(|p| &p[..]).collect();
    // This repo exposes a simple `from_vec` for BinaryArray
    BinaryArray::from_vec(views)
}

#[inline]
fn make_fixed16_array_with_tag(n: usize, tag: u64) -> FixedSizeBinaryArray {
    let mut rng = rng_for(tag, n);
    let payloads = (0..n)
        .map(|_| {
            let mut b = [0u8; 16];
            rng.fill(&mut b);
            b
        })
        .collect::<Vec<[u8; 16]>>();
    // Fixed-size constructor available in this repo
    FixedSizeBinaryArray::try_from_iter(payloads.into_iter()).expect("build FixedSizeBinaryArray")
}

/// Make an Arrow `Interval(IntervalUnit::MonthDayNano)` array with **non-negative**
/// (months, days, nanos) values, and nanos as **multiples of 1_000_000** (whole ms),
/// per Avro `duration` constraints used by the writer.
#[inline]
fn make_interval_mdn_array_with_tag(
    n: usize,
    tag: u64,
) -> PrimitiveArray<IntervalMonthDayNanoType> {
    let mut rng = rng_for(tag, n);
    let values = (0..n).map(|_| {
        let months: i32 = rng.random_range(0..=120);
        let days: i32 = rng.random_range(0..=31);
        // pick millis within a day (safe within u32::MAX and realistic)
        let millis: u32 = rng.random_range(0..=86_400_000);
        let nanos: i64 = (millis as i64) * 1_000_000;
        IntervalMonthDayNanoType::make_value(months, days, nanos)
    });
    PrimitiveArray::<IntervalMonthDayNanoType>::from_iter_values(values)
}

#[inline]
fn make_ts_micros_array_with_tag(n: usize, tag: u64) -> PrimitiveArray<TimestampMicrosecondType> {
    let mut rng = rng_for(tag, n);
    let base: i64 = 1_600_000_000_000_000;
    let year_us: i64 = 31_536_000_000_000;
    let values = (0..n).map(|_| base + sample_in::<i64, _>(&mut rng, 0..year_us));
    PrimitiveArray::<TimestampMicrosecondType>::from_iter_values(values)
}

// === Decimal helpers & generators ===

#[inline]
fn pow10_i32(p: u8) -> i32 {
    (0..p).fold(1i32, |acc, _| acc.saturating_mul(10))
}

#[inline]
fn pow10_i64(p: u8) -> i64 {
    (0..p).fold(1i64, |acc, _| acc.saturating_mul(10))
}

#[inline]
fn pow10_i128(p: u8) -> i128 {
    (0..p).fold(1i128, |acc, _| acc.saturating_mul(10))
}

#[inline]
fn make_decimal32_array_with_tag(n: usize, tag: u64, precision: u8, scale: i8) -> Decimal32Array {
    let mut rng = rng_for(tag, n);
    let max = pow10_i32(precision).saturating_sub(1);
    let values = (0..n).map(|_| rng.random_range(-max..=max));
    Decimal32Array::from_iter_values(values)
        .with_precision_and_scale(precision, scale)
        .expect("set precision/scale on Decimal32Array")
}

#[inline]
fn make_decimal64_array_with_tag(n: usize, tag: u64, precision: u8, scale: i8) -> Decimal64Array {
    let mut rng = rng_for(tag, n);
    let max = pow10_i64(precision).saturating_sub(1);
    let values = (0..n).map(|_| rng.random_range(-max..=max));
    Decimal64Array::from_iter_values(values)
        .with_precision_and_scale(precision, scale)
        .expect("set precision/scale on Decimal64Array")
}

#[inline]
fn make_decimal128_array_with_tag(n: usize, tag: u64, precision: u8, scale: i8) -> Decimal128Array {
    let mut rng = rng_for(tag, n);
    let max = pow10_i128(precision).saturating_sub(1);
    let values = (0..n).map(|_| rng.random_range(-max..=max));
    Decimal128Array::from_iter_values(values)
        .with_precision_and_scale(precision, scale)
        .expect("set precision/scale on Decimal128Array")
}

#[inline]
fn make_decimal256_array_with_tag(n: usize, tag: u64, precision: u8, scale: i8) -> Decimal256Array {
    // Generate within i128 range and widen to i256 to keep generation cheap and portable
    let mut rng = rng_for(tag, n);
    let max128 = pow10_i128(30).saturating_sub(1);
    let values = (0..n).map(|_| {
        let v: i128 = rng.random_range(-max128..=max128);
        i256::from_i128(v)
    });
    Decimal256Array::from_iter_values(values)
        .with_precision_and_scale(precision, scale)
        .expect("set precision/scale on Decimal256Array")
}

#[inline]
fn make_fixed16_array(n: usize) -> FixedSizeBinaryArray {
    make_fixed16_array_with_tag(n, 0xF15E_D016)
}

#[inline]
fn make_interval_mdn_array(n: usize) -> PrimitiveArray<IntervalMonthDayNanoType> {
    make_interval_mdn_array_with_tag(n, 0xD0_1E_AD)
}

#[inline]
fn make_bool_array(n: usize) -> BooleanArray {
    make_bool_array_with_tag(n, 0xB001)
}
#[inline]
fn make_i32_array(n: usize) -> PrimitiveArray<Int32Type> {
    make_i32_array_with_tag(n, 0x1337_0032)
}
#[inline]
fn make_i64_array(n: usize) -> PrimitiveArray<Int64Type> {
    make_i64_array_with_tag(n, 0x1337_0064)
}
#[inline]
fn make_f32_array(n: usize) -> Float32Array {
    make_f32_array_with_tag(n, 0xF0_0032)
}
#[inline]
fn make_f64_array(n: usize) -> Float64Array {
    make_f64_array_with_tag(n, 0xF0_0064)
}
#[inline]
fn make_binary_array(n: usize) -> BinaryArray {
    make_binary_array_with_tag(n, 0xB1_0001)
}
#[inline]
fn make_ts_micros_array(n: usize) -> PrimitiveArray<TimestampMicrosecondType> {
    make_ts_micros_array_with_tag(n, 0x7157_0001)
}
#[inline]
fn make_utf8_array(n: usize) -> StringArray {
    make_utf8_array_with_tag(n, 0x5712_07F8)
}
#[inline]
fn make_list_utf8_array(n: usize) -> ListArray {
    make_list_utf8_array_with_tag(n, 0x0A11_57ED)
}
#[inline]
fn make_struct_array(n: usize) -> StructArray {
    make_struct_array_with_tag(n, 0x57_AB_C7)
}

#[inline]
fn make_list_utf8_array_with_tag(n: usize, tag: u64) -> ListArray {
    let mut rng = rng_for(tag, n);
    let mut builder = ListBuilder::new(StringBuilder::new());
    for _ in 0..n {
        let items = rng.random_range(0..=5);
        for _ in 0..items {
            let s = rand_ascii_string(&mut rng, 1, 12);
            builder.values().append_value(s.as_str());
        }
        builder.append(true);
    }
    builder.finish()
}

#[inline]
fn make_struct_array_with_tag(n: usize, tag: u64) -> StructArray {
    let s_tag = tag ^ 0x5u64;
    let i_tag = tag ^ 0x6u64;
    let f_tag = tag ^ 0x7u64;
    let s_col: ArrayRef = Arc::new(make_utf8_array_with_tag(n, s_tag));
    let i_col: ArrayRef = Arc::new(make_i32_array_with_tag(n, i_tag));
    let f_col: ArrayRef = Arc::new(make_f64_array_with_tag(n, f_tag));
    StructArray::from(vec![
        (
            Arc::new(Field::new("s1", DataType::Utf8, false)),
            s_col.clone(),
        ),
        (
            Arc::new(Field::new("s2", DataType::Int32, false)),
            i_col.clone(),
        ),
        (
            Arc::new(Field::new("s3", DataType::Float64, false)),
            f_col.clone(),
        ),
    ])
}

#[inline]
fn schema_single(name: &str, dt: DataType) -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(name, dt, false)]))
}

#[inline]
fn schema_mixed() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("f1", DataType::Int32, false),
        Field::new("f2", DataType::Int64, false),
        Field::new("f3", DataType::Binary, false),
        Field::new("f4", DataType::Float64, false),
    ]))
}

#[inline]
fn schema_fixed16() -> Arc<Schema> {
    schema_single("field1", DataType::FixedSizeBinary(16))
}

#[inline]
fn schema_uuid16() -> Arc<Schema> {
    let mut md = HashMap::new();
    md.insert("logicalType".to_string(), "uuid".to_string());
    let field = Field::new("uuid", DataType::FixedSizeBinary(16), false).with_metadata(md);
    Arc::new(Schema::new(vec![field]))
}

#[inline]
fn schema_interval_mdn() -> Arc<Schema> {
    schema_single("duration", DataType::Interval(IntervalUnit::MonthDayNano))
}

#[inline]
fn schema_decimal_with_size(name: &str, dt: DataType, size_meta: Option<usize>) -> Arc<Schema> {
    let field = if let Some(size) = size_meta {
        let mut md = HashMap::new();
        md.insert("size".to_string(), size.to_string());
        Field::new(name, dt, false).with_metadata(md)
    } else {
        Field::new(name, dt, false)
    };
    Arc::new(Schema::new(vec![field]))
}

static BOOLEAN_DATA: Lazy<Vec<RecordBatch>> = Lazy::new(|| {
    let schema = schema_single("field1", DataType::Boolean);
    SIZES
        .iter()
        .map(|&n| {
            let col: ArrayRef = Arc::new(make_bool_array(n));
            RecordBatch::try_new(schema.clone(), vec![col]).unwrap()
        })
        .collect()
});

static INT32_DATA: Lazy<Vec<RecordBatch>> = Lazy::new(|| {
    let schema = schema_single("field1", DataType::Int32);
    SIZES
        .iter()
        .map(|&n| {
            let col: ArrayRef = Arc::new(make_i32_array(n));
            RecordBatch::try_new(schema.clone(), vec![col]).unwrap()
        })
        .collect()
});

static INT64_DATA: Lazy<Vec<RecordBatch>> = Lazy::new(|| {
    let schema = schema_single("field1", DataType::Int64);
    SIZES
        .iter()
        .map(|&n| {
            let col: ArrayRef = Arc::new(make_i64_array(n));
            RecordBatch::try_new(schema.clone(), vec![col]).unwrap()
        })
        .collect()
});

static FLOAT32_DATA: Lazy<Vec<RecordBatch>> = Lazy::new(|| {
    let schema = schema_single("field1", DataType::Float32);
    SIZES
        .iter()
        .map(|&n| {
            let col: ArrayRef = Arc::new(make_f32_array(n));
            RecordBatch::try_new(schema.clone(), vec![col]).unwrap()
        })
        .collect()
});

static FLOAT64_DATA: Lazy<Vec<RecordBatch>> = Lazy::new(|| {
    let schema = schema_single("field1", DataType::Float64);
    SIZES
        .iter()
        .map(|&n| {
            let col: ArrayRef = Arc::new(make_f64_array(n));
            RecordBatch::try_new(schema.clone(), vec![col]).unwrap()
        })
        .collect()
});

static BINARY_DATA: Lazy<Vec<RecordBatch>> = Lazy::new(|| {
    let schema = schema_single("field1", DataType::Binary);
    SIZES
        .iter()
        .map(|&n| {
            let col: ArrayRef = Arc::new(make_binary_array(n));
            RecordBatch::try_new(schema.clone(), vec![col]).unwrap()
        })
        .collect()
});

static FIXED16_DATA: Lazy<Vec<RecordBatch>> = Lazy::new(|| {
    let schema = schema_fixed16();
    SIZES
        .iter()
        .map(|&n| {
            let col: ArrayRef = Arc::new(make_fixed16_array(n));
            RecordBatch::try_new(schema.clone(), vec![col]).unwrap()
        })
        .collect()
});

static UUID16_DATA: Lazy<Vec<RecordBatch>> = Lazy::new(|| {
    let schema = schema_uuid16();
    SIZES
        .iter()
        .map(|&n| {
            // Same values as Fixed16; writer path differs because of field metadata
            let col: ArrayRef = Arc::new(make_fixed16_array_with_tag(n, 0x7575_6964_7575_6964));
            RecordBatch::try_new(schema.clone(), vec![col]).unwrap()
        })
        .collect()
});

static INTERVAL_MDN_DATA: Lazy<Vec<RecordBatch>> = Lazy::new(|| {
    let schema = schema_interval_mdn();
    SIZES
        .iter()
        .map(|&n| {
            let col: ArrayRef = Arc::new(make_interval_mdn_array(n));
            RecordBatch::try_new(schema.clone(), vec![col]).unwrap()
        })
        .collect()
});

static TIMESTAMP_US_DATA: Lazy<Vec<RecordBatch>> = Lazy::new(|| {
    let schema = schema_single("field1", DataType::Timestamp(TimeUnit::Microsecond, None));
    SIZES
        .iter()
        .map(|&n| {
            let col: ArrayRef = Arc::new(make_ts_micros_array(n));
            RecordBatch::try_new(schema.clone(), vec![col]).unwrap()
        })
        .collect()
});

static MIXED_DATA: Lazy<Vec<RecordBatch>> = Lazy::new(|| {
    let schema = schema_mixed();
    SIZES
        .iter()
        .map(|&n| {
            let f1: ArrayRef = Arc::new(make_i32_array_with_tag(n, 0xA1));
            let f2: ArrayRef = Arc::new(make_i64_array_with_tag(n, 0xA2));
            let f3: ArrayRef = Arc::new(make_binary_array_with_tag(n, 0xA3));
            let f4: ArrayRef = Arc::new(make_f64_array_with_tag(n, 0xA4));
            RecordBatch::try_new(schema.clone(), vec![f1, f2, f3, f4]).unwrap()
        })
        .collect()
});

static UTF8_DATA: Lazy<Vec<RecordBatch>> = Lazy::new(|| {
    let schema = schema_single("field1", DataType::Utf8);
    SIZES
        .iter()
        .map(|&n| {
            let col: ArrayRef = Arc::new(make_utf8_array(n));
            RecordBatch::try_new(schema.clone(), vec![col]).unwrap()
        })
        .collect()
});

static LIST_UTF8_DATA: Lazy<Vec<RecordBatch>> = Lazy::new(|| {
    // IMPORTANT: ListBuilder creates a child field named "item" that is nullable by default.
    // Make the schema's list item nullable to match the array we construct.
    let item_field = Arc::new(Field::new("item", DataType::Utf8, true));
    let schema = schema_single("field1", DataType::List(item_field));
    SIZES
        .iter()
        .map(|&n| {
            let col: ArrayRef = Arc::new(make_list_utf8_array(n));
            RecordBatch::try_new(schema.clone(), vec![col]).unwrap()
        })
        .collect()
});

static STRUCT_DATA: Lazy<Vec<RecordBatch>> = Lazy::new(|| {
    let struct_dt = DataType::Struct(
        vec![
            Field::new("s1", DataType::Utf8, false),
            Field::new("s2", DataType::Int32, false),
            Field::new("s3", DataType::Float64, false),
        ]
        .into(),
    );
    let schema = schema_single("field1", struct_dt);
    SIZES
        .iter()
        .map(|&n| {
            let col: ArrayRef = Arc::new(make_struct_array(n));
            RecordBatch::try_new(schema.clone(), vec![col]).unwrap()
        })
        .collect()
});

// === NEW: Decimal datasets ===

static DECIMAL32_DATA: Lazy<Vec<RecordBatch>> = Lazy::new(|| {
    // Choose a representative precision/scale within Decimal32 limits
    let precision: u8 = 7;
    let scale: i8 = 2;
    let schema = schema_single("amount", DataType::Decimal32(precision, scale));
    SIZES
        .iter()
        .map(|&n| {
            let arr = make_decimal32_array_with_tag(n, 0xDEC_0032, precision, scale);
            let col: ArrayRef = Arc::new(arr);
            RecordBatch::try_new(schema.clone(), vec![col]).unwrap()
        })
        .collect()
});

static DECIMAL64_DATA: Lazy<Vec<RecordBatch>> = Lazy::new(|| {
    let precision: u8 = 13;
    let scale: i8 = 3;
    let schema = schema_single("amount", DataType::Decimal64(precision, scale));
    SIZES
        .iter()
        .map(|&n| {
            let arr = make_decimal64_array_with_tag(n, 0xDEC_0064, precision, scale);
            let col: ArrayRef = Arc::new(arr);
            RecordBatch::try_new(schema.clone(), vec![col]).unwrap()
        })
        .collect()
});

static DECIMAL128_BYTES_DATA: Lazy<Vec<RecordBatch>> = Lazy::new(|| {
    let precision: u8 = 25;
    let scale: i8 = 6;
    let schema = schema_single("amount", DataType::Decimal128(precision, scale));
    SIZES
        .iter()
        .map(|&n| {
            let arr = make_decimal128_array_with_tag(n, 0xDEC_0128, precision, scale);
            let col: ArrayRef = Arc::new(arr);
            RecordBatch::try_new(schema.clone(), vec![col]).unwrap()
        })
        .collect()
});

static DECIMAL128_FIXED16_DATA: Lazy<Vec<RecordBatch>> = Lazy::new(|| {
    // Same logical type as above but force Avro fixed(16) via metadata "size": "16"
    let precision: u8 = 25;
    let scale: i8 = 6;
    let schema =
        schema_decimal_with_size("amount", DataType::Decimal128(precision, scale), Some(16));
    SIZES
        .iter()
        .map(|&n| {
            let arr = make_decimal128_array_with_tag(n, 0xDEC_F128, precision, scale);
            let col: ArrayRef = Arc::new(arr);
            RecordBatch::try_new(schema.clone(), vec![col]).unwrap()
        })
        .collect()
});

static DECIMAL256_DATA: Lazy<Vec<RecordBatch>> = Lazy::new(|| {
    // Use a higher precision typical of 256-bit decimals
    let precision: u8 = 50;
    let scale: i8 = 10;
    let schema = schema_single("amount", DataType::Decimal256(precision, scale));
    SIZES
        .iter()
        .map(|&n| {
            let arr = make_decimal256_array_with_tag(n, 0xDEC_0256, precision, scale);
            let col: ArrayRef = Arc::new(arr);
            RecordBatch::try_new(schema.clone(), vec![col]).unwrap()
        })
        .collect()
});

fn ocf_size_for_batch(batch: &RecordBatch) -> usize {
    let schema_owned: Schema = (*batch.schema()).clone();
    let cursor = Cursor::new(Vec::<u8>::with_capacity(1024));
    let mut writer = AvroWriter::new(cursor, schema_owned).expect("create writer");
    writer.write(batch).expect("write batch");
    writer.finish().expect("finish writer");
    let inner = writer.into_inner();
    inner.into_inner().len()
}

fn bench_writer_scenario(c: &mut Criterion, name: &str, data_sets: &[RecordBatch]) {
    let mut group = c.benchmark_group(name);
    let schema_owned: Schema = (*data_sets[0].schema()).clone();
    for (idx, &rows) in SIZES.iter().enumerate() {
        let batch = &data_sets[idx];
        let bytes = ocf_size_for_batch(batch);
        group.throughput(Throughput::Bytes(bytes as u64));
        match rows {
            4_096 | 8_192 => {
                group
                    .sample_size(40)
                    .measurement_time(Duration::from_secs(10))
                    .warm_up_time(Duration::from_secs(3));
            }
            100_000 => {
                group
                    .sample_size(20)
                    .measurement_time(Duration::from_secs(10))
                    .warm_up_time(Duration::from_secs(3));
            }
            1_000_000 => {
                group
                    .sample_size(10)
                    .measurement_time(Duration::from_secs(10))
                    .warm_up_time(Duration::from_secs(3));
            }
            _ => {}
        }
        group.bench_function(BenchmarkId::from_parameter(rows), |b| {
            b.iter_batched_ref(
                || {
                    let file = tempfile().expect("create temp file");
                    AvroWriter::new(file, schema_owned.clone()).expect("create writer")
                },
                |writer| {
                    writer.write(batch).unwrap();
                    writer.finish().unwrap();
                },
                BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

fn criterion_benches(c: &mut Criterion) {
    bench_writer_scenario(c, "write-Boolean", &BOOLEAN_DATA);
    bench_writer_scenario(c, "write-Int32", &INT32_DATA);
    bench_writer_scenario(c, "write-Int64", &INT64_DATA);
    bench_writer_scenario(c, "write-Float32", &FLOAT32_DATA);
    bench_writer_scenario(c, "write-Float64", &FLOAT64_DATA);
    bench_writer_scenario(c, "write-Binary(Bytes)", &BINARY_DATA);
    bench_writer_scenario(c, "write-TimestampMicros", &TIMESTAMP_US_DATA);
    bench_writer_scenario(c, "write-Mixed", &MIXED_DATA);
    bench_writer_scenario(c, "write-Utf8", &UTF8_DATA);
    bench_writer_scenario(c, "write-List<Utf8>", &LIST_UTF8_DATA);
    bench_writer_scenario(c, "write-Struct", &STRUCT_DATA);
    bench_writer_scenario(c, "write-FixedSizeBinary16", &FIXED16_DATA);
    bench_writer_scenario(c, "write-UUID(logicalType)", &UUID16_DATA);
    bench_writer_scenario(c, "write-IntervalMonthDayNanoDuration", &INTERVAL_MDN_DATA);
    bench_writer_scenario(c, "write-Decimal32(bytes)", &DECIMAL32_DATA);
    bench_writer_scenario(c, "write-Decimal64(bytes)", &DECIMAL64_DATA);
    bench_writer_scenario(c, "write-Decimal128(bytes)", &DECIMAL128_BYTES_DATA);
    bench_writer_scenario(c, "write-Decimal128(fixed16)", &DECIMAL128_FIXED16_DATA);
    bench_writer_scenario(c, "write-Decimal256(bytes)", &DECIMAL256_DATA);
}

criterion_group! {
    name = avro_writer;
    config = Criterion::default().configure_from_args();
    targets = criterion_benches
}
criterion_main!(avro_writer);
