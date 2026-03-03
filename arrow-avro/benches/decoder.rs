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

//! Benchmarks for `arrow‑avro` **Decoder**
//!

extern crate apache_avro;
extern crate arrow_avro;
extern crate criterion;
extern crate num_bigint;
extern crate once_cell;
extern crate uuid;

use apache_avro::types::Value;
use apache_avro::{Decimal, Schema as ApacheSchema, to_avro_datum};
use arrow_avro::schema::{CONFLUENT_MAGIC, Fingerprint, FingerprintAlgorithm, SINGLE_OBJECT_MAGIC};
use arrow_avro::{reader::ReaderBuilder, schema::AvroSchema};
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use once_cell::sync::Lazy;
use std::{hint::black_box, time::Duration};
use uuid::Uuid;

fn make_prefix(fp: Fingerprint) -> Vec<u8> {
    match fp {
        Fingerprint::Rabin(val) => {
            let mut buf = Vec::with_capacity(SINGLE_OBJECT_MAGIC.len() + size_of::<u64>());
            buf.extend_from_slice(&SINGLE_OBJECT_MAGIC); // C3 01
            buf.extend_from_slice(&val.to_le_bytes()); // little-endian
            buf
        }
        Fingerprint::Id(id) => {
            let mut buf = Vec::with_capacity(CONFLUENT_MAGIC.len() + size_of::<u32>());
            buf.extend_from_slice(&CONFLUENT_MAGIC); // 00
            buf.extend_from_slice(&id.to_be_bytes()); // big-endian
            buf
        }
        Fingerprint::Id64(id) => {
            let mut buf = Vec::with_capacity(CONFLUENT_MAGIC.len() + size_of::<u64>());
            buf.extend_from_slice(&CONFLUENT_MAGIC); // 00
            buf.extend_from_slice(&id.to_be_bytes()); // big-endian
            buf
        }
        #[cfg(feature = "md5")]
        Fingerprint::MD5(val) => {
            let mut buf = Vec::with_capacity(SINGLE_OBJECT_MAGIC.len() + size_of_val(&val));
            buf.extend_from_slice(&SINGLE_OBJECT_MAGIC); // C3 01
            buf.extend_from_slice(&val);
            buf
        }
        #[cfg(feature = "sha256")]
        Fingerprint::SHA256(val) => {
            let mut buf = Vec::with_capacity(SINGLE_OBJECT_MAGIC.len() + size_of_val(&val));
            buf.extend_from_slice(&SINGLE_OBJECT_MAGIC); // C3 01
            buf.extend_from_slice(&val);
            buf
        }
    }
}

fn encode_records_with_prefix(
    schema: &ApacheSchema,
    prefix: &[u8],
    rows: impl Iterator<Item = Value>,
) -> Vec<u8> {
    let mut out = Vec::new();
    for v in rows {
        out.extend_from_slice(prefix);
        out.extend_from_slice(&to_avro_datum(schema, v).expect("encode datum failed"));
    }
    out
}

fn gen_int(sc: &ApacheSchema, n: usize, prefix: &[u8]) -> Vec<u8> {
    encode_records_with_prefix(
        sc,
        prefix,
        (0..n).map(|i| Value::Record(vec![("field1".into(), Value::Int(i as i32))])),
    )
}

fn gen_long(sc: &ApacheSchema, n: usize, prefix: &[u8]) -> Vec<u8> {
    encode_records_with_prefix(
        sc,
        prefix,
        (0..n).map(|i| Value::Record(vec![("field1".into(), Value::Long(i as i64))])),
    )
}

fn gen_float(sc: &ApacheSchema, n: usize, prefix: &[u8]) -> Vec<u8> {
    encode_records_with_prefix(
        sc,
        prefix,
        (0..n).map(|i| Value::Record(vec![("field1".into(), Value::Float(i as f32 + 0.5678))])),
    )
}

fn gen_bool(sc: &ApacheSchema, n: usize, prefix: &[u8]) -> Vec<u8> {
    encode_records_with_prefix(
        sc,
        prefix,
        (0..n).map(|i| Value::Record(vec![("field1".into(), Value::Boolean(i % 2 == 0))])),
    )
}

fn gen_double(sc: &ApacheSchema, n: usize, prefix: &[u8]) -> Vec<u8> {
    encode_records_with_prefix(
        sc,
        prefix,
        (0..n).map(|i| Value::Record(vec![("field1".into(), Value::Double(i as f64 + 0.1234))])),
    )
}

fn gen_bytes(sc: &ApacheSchema, n: usize, prefix: &[u8]) -> Vec<u8> {
    encode_records_with_prefix(
        sc,
        prefix,
        (0..n).map(|i| {
            let payload = vec![(i & 0xFF) as u8; 16];
            Value::Record(vec![("field1".into(), Value::Bytes(payload))])
        }),
    )
}

fn gen_string(sc: &ApacheSchema, n: usize, prefix: &[u8]) -> Vec<u8> {
    encode_records_with_prefix(
        sc,
        prefix,
        (0..n).map(|i| {
            let s = if i % 3 == 0 {
                format!("value-{i}")
            } else {
                "abcdefghij".into()
            };
            Value::Record(vec![("field1".into(), Value::String(s))])
        }),
    )
}

fn gen_date(sc: &ApacheSchema, n: usize, prefix: &[u8]) -> Vec<u8> {
    encode_records_with_prefix(
        sc,
        prefix,
        (0..n).map(|i| Value::Record(vec![("field1".into(), Value::Int(i as i32))])),
    )
}

fn gen_timemillis(sc: &ApacheSchema, n: usize, prefix: &[u8]) -> Vec<u8> {
    encode_records_with_prefix(
        sc,
        prefix,
        (0..n).map(|i| Value::Record(vec![("field1".into(), Value::Int((i * 37) as i32))])),
    )
}

fn gen_timemicros(sc: &ApacheSchema, n: usize, prefix: &[u8]) -> Vec<u8> {
    encode_records_with_prefix(
        sc,
        prefix,
        (0..n).map(|i| Value::Record(vec![("field1".into(), Value::Long((i * 1_001) as i64))])),
    )
}

fn gen_ts_millis(sc: &ApacheSchema, n: usize, prefix: &[u8]) -> Vec<u8> {
    encode_records_with_prefix(
        sc,
        prefix,
        (0..n).map(|i| {
            Value::Record(vec![(
                "field1".into(),
                Value::Long(1_600_000_000_000 + i as i64),
            )])
        }),
    )
}

fn gen_ts_micros(sc: &ApacheSchema, n: usize, prefix: &[u8]) -> Vec<u8> {
    encode_records_with_prefix(
        sc,
        prefix,
        (0..n).map(|i| {
            Value::Record(vec![(
                "field1".into(),
                Value::Long(1_600_000_000_000_000 + i as i64),
            )])
        }),
    )
}

fn gen_map(sc: &ApacheSchema, n: usize, prefix: &[u8]) -> Vec<u8> {
    use std::collections::HashMap;
    encode_records_with_prefix(
        sc,
        prefix,
        (0..n).map(|i| {
            let mut m = HashMap::new();
            let int_val = |v: i32| Value::Union(0, Box::new(Value::Int(v)));
            m.insert("key1".into(), int_val(i as i32));
            let key2_val = if i % 5 == 0 {
                Value::Union(1, Box::new(Value::Null))
            } else {
                int_val(i as i32 + 1)
            };
            m.insert("key2".into(), key2_val);
            m.insert("key3".into(), int_val(42));
            Value::Record(vec![("field1".into(), Value::Map(m))])
        }),
    )
}

fn gen_array(sc: &ApacheSchema, n: usize, prefix: &[u8]) -> Vec<u8> {
    encode_records_with_prefix(
        sc,
        prefix,
        (0..n).map(|i| {
            let items = (0..5).map(|j| Value::Int(i as i32 + j)).collect();
            Value::Record(vec![("field1".into(), Value::Array(items))])
        }),
    )
}

fn trim_i128_be(v: i128) -> Vec<u8> {
    let full = v.to_be_bytes();
    let first = full
        .iter()
        .enumerate()
        .take_while(|(i, b)| {
            *i < 15
                && ((**b == 0x00 && full[i + 1] & 0x80 == 0)
                    || (**b == 0xFF && full[i + 1] & 0x80 != 0))
        })
        .count();
    full[first..].to_vec()
}

fn gen_decimal(sc: &ApacheSchema, n: usize, prefix: &[u8]) -> Vec<u8> {
    encode_records_with_prefix(
        sc,
        prefix,
        (0..n).map(|i| {
            let unscaled = if i % 2 == 0 { i as i128 } else { -(i as i128) };
            Value::Record(vec![(
                "field1".into(),
                Value::Decimal(Decimal::from(trim_i128_be(unscaled))),
            )])
        }),
    )
}

fn gen_uuid(sc: &ApacheSchema, n: usize, prefix: &[u8]) -> Vec<u8> {
    encode_records_with_prefix(
        sc,
        prefix,
        (0..n).map(|i| {
            let mut raw = (i as u128).to_be_bytes();
            raw[6] = (raw[6] & 0x0F) | 0x40;
            raw[8] = (raw[8] & 0x3F) | 0x80;
            Value::Record(vec![("field1".into(), Value::Uuid(Uuid::from_bytes(raw)))])
        }),
    )
}

fn gen_fixed(sc: &ApacheSchema, n: usize, prefix: &[u8]) -> Vec<u8> {
    encode_records_with_prefix(
        sc,
        prefix,
        (0..n).map(|i| {
            let mut buf = vec![0u8; 16];
            buf[..8].copy_from_slice(&(i as u64).to_be_bytes());
            Value::Record(vec![("field1".into(), Value::Fixed(16, buf))])
        }),
    )
}

fn gen_interval(sc: &ApacheSchema, n: usize, prefix: &[u8]) -> Vec<u8> {
    encode_records_with_prefix(
        sc,
        prefix,
        (0..n).map(|i| {
            let months = (i % 24) as u32;
            let days = (i % 32) as u32;
            let millis = (i * 10) as u32;
            let mut buf = Vec::with_capacity(12);
            buf.extend_from_slice(&months.to_le_bytes());
            buf.extend_from_slice(&days.to_le_bytes());
            buf.extend_from_slice(&millis.to_le_bytes());
            Value::Record(vec![("field1".into(), Value::Fixed(12, buf))])
        }),
    )
}

fn gen_enum(sc: &ApacheSchema, n: usize, prefix: &[u8]) -> Vec<u8> {
    const SYMBOLS: [&str; 3] = ["A", "B", "C"];
    encode_records_with_prefix(
        sc,
        prefix,
        (0..n).map(|i| {
            let idx = i % 3;
            Value::Record(vec![(
                "field1".into(),
                Value::Enum(idx as u32, SYMBOLS[idx].into()),
            )])
        }),
    )
}

fn gen_mixed(sc: &ApacheSchema, n: usize, prefix: &[u8]) -> Vec<u8> {
    encode_records_with_prefix(
        sc,
        prefix,
        (0..n).map(|i| {
            Value::Record(vec![
                ("f1".into(), Value::Int(i as i32)),
                ("f2".into(), Value::Long(i as i64)),
                ("f3".into(), Value::String(format!("name-{i}"))),
                ("f4".into(), Value::Double(i as f64 * 1.5)),
            ])
        }),
    )
}

fn gen_nested(sc: &ApacheSchema, n: usize, prefix: &[u8]) -> Vec<u8> {
    encode_records_with_prefix(
        sc,
        prefix,
        (0..n).map(|i| {
            let sub = Value::Record(vec![
                ("x".into(), Value::Int(i as i32)),
                ("y".into(), Value::String("constant".into())),
            ]);
            Value::Record(vec![("sub".into(), sub)])
        }),
    )
}

const LARGE_BATCH: usize = 65_536;
const SMALL_BATCH: usize = 4096;

fn new_decoder(
    schema_json: &'static str,
    batch_size: usize,
    utf8view: bool,
) -> arrow_avro::reader::Decoder {
    let schema = AvroSchema::new(schema_json.parse().unwrap());
    let mut store = arrow_avro::schema::SchemaStore::new();
    store.register(schema.clone()).unwrap();
    ReaderBuilder::new()
        .with_writer_schema_store(store)
        .with_batch_size(batch_size)
        .with_utf8_view(utf8view)
        .build_decoder()
        .expect("failed to build decoder")
}

fn new_decoder_id(
    schema_json: &'static str,
    batch_size: usize,
    utf8view: bool,
    id: u32,
) -> arrow_avro::reader::Decoder {
    let schema = AvroSchema::new(schema_json.parse().unwrap());
    let mut store = arrow_avro::schema::SchemaStore::new_with_type(FingerprintAlgorithm::Id);
    // Register the schema with a provided Confluent-style ID
    store
        .set(Fingerprint::Id(id), schema.clone())
        .expect("failed to set schema with id");
    ReaderBuilder::new()
        .with_writer_schema_store(store)
        .with_active_fingerprint(Fingerprint::Id(id))
        .with_batch_size(batch_size)
        .with_utf8_view(utf8view)
        .build_decoder()
        .expect("failed to build decoder for id")
}

const SIZES: [usize; 3] = [100, 10_000, 1_000_000];

const INT_SCHEMA: &str =
    r#"{"type":"record","name":"IntRec","fields":[{"name":"field1","type":"int"}]}"#;
const LONG_SCHEMA: &str =
    r#"{"type":"record","name":"LongRec","fields":[{"name":"field1","type":"long"}]}"#;
const FLOAT_SCHEMA: &str =
    r#"{"type":"record","name":"FloatRec","fields":[{"name":"field1","type":"float"}]}"#;
const BOOL_SCHEMA: &str =
    r#"{"type":"record","name":"BoolRec","fields":[{"name":"field1","type":"boolean"}]}"#;
const DOUBLE_SCHEMA: &str =
    r#"{"type":"record","name":"DoubleRec","fields":[{"name":"field1","type":"double"}]}"#;
const BYTES_SCHEMA: &str =
    r#"{"type":"record","name":"BytesRec","fields":[{"name":"field1","type":"bytes"}]}"#;
const STRING_SCHEMA: &str =
    r#"{"type":"record","name":"StrRec","fields":[{"name":"field1","type":"string"}]}"#;
const DATE_SCHEMA: &str = r#"{"type":"record","name":"DateRec","fields":[{"name":"field1","type":{"type":"int","logicalType":"date"}}]}"#;
const TMILLIS_SCHEMA: &str = r#"{"type":"record","name":"TimeMsRec","fields":[{"name":"field1","type":{"type":"int","logicalType":"time-millis"}}]}"#;
const TMICROS_SCHEMA: &str = r#"{"type":"record","name":"TimeUsRec","fields":[{"name":"field1","type":{"type":"long","logicalType":"time-micros"}}]}"#;
const TSMILLIS_SCHEMA: &str = r#"{"type":"record","name":"TsMsRec","fields":[{"name":"field1","type":{"type":"long","logicalType":"timestamp-millis"}}]}"#;
const TSMICROS_SCHEMA: &str = r#"{"type":"record","name":"TsUsRec","fields":[{"name":"field1","type":{"type":"long","logicalType":"timestamp-micros"}}]}"#;
const MAP_SCHEMA: &str = r#"{"type":"record","name":"MapRec","fields":[{"name":"field1","type":{"type":"map","values":["int","null"]}}]}"#;
const ARRAY_SCHEMA: &str = r#"{"type":"record","name":"ArrRec","fields":[{"name":"field1","type":{"type":"array","items":"int"}}]}"#;
const DECIMAL_SCHEMA: &str = r#"{"type":"record","name":"DecRec","fields":[{"name":"field1","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":3}}]}"#;
const UUID_SCHEMA: &str = r#"{"type":"record","name":"UuidRec","fields":[{"name":"field1","type":{"type":"string","logicalType":"uuid"}}]}"#;
const FIXED_SCHEMA: &str = r#"{"type":"record","name":"FixRec","fields":[{"name":"field1","type":{"type":"fixed","name":"Fixed16","size":16}}]}"#;
const INTERVAL_SCHEMA: &str = r#"{"type":"record","name":"DurRec","fields":[{"name":"field1","type":{"type":"fixed","name":"Duration12","size":12,"logicalType":"duration"}}]}"#;
const INTERVAL_SCHEMA_ENCODE: &str = r#"{"type":"record","name":"DurRec","fields":[{"name":"field1","type":{"type":"fixed","name":"Duration12","size":12}}]}"#;
const ENUM_SCHEMA: &str = r#"{"type":"record","name":"EnumRec","fields":[{"name":"field1","type":{"type":"enum","name":"MyEnum","symbols":["A","B","C"]}}]}"#;
const MIX_SCHEMA: &str = r#"{"type":"record","name":"MixRec","fields":[{"name":"f1","type":"int"},{"name":"f2","type":"long"},{"name":"f3","type":"string"},{"name":"f4","type":"double"}]}"#;
const NEST_SCHEMA: &str = r#"{"type":"record","name":"NestRec","fields":[{"name":"sub","type":{"type":"record","name":"Sub","fields":[{"name":"x","type":"int"},{"name":"y","type":"string"}]}}]}"#;

macro_rules! dataset {
    ($name:ident, $schema_json:expr, $gen_fn:ident) => {
        static $name: Lazy<Vec<Vec<u8>>> = Lazy::new(|| {
            let schema =
                ApacheSchema::parse_str($schema_json).expect("invalid schema for generator");
            let arrow_schema = AvroSchema::new($schema_json.parse().unwrap());
            let fingerprint = arrow_schema
                .fingerprint(FingerprintAlgorithm::Rabin)
                .expect("fingerprint failed");
            let prefix = make_prefix(fingerprint);
            SIZES
                .iter()
                .map(|&n| $gen_fn(&schema, n, &prefix))
                .collect()
        });
    };
}

/// Additional helper for Confluent's ID-based wire format (00 + BE u32).
macro_rules! dataset_id {
    ($name:ident, $schema_json:expr, $gen_fn:ident, $id:expr) => {
        static $name: Lazy<Vec<Vec<u8>>> = Lazy::new(|| {
            let schema =
                ApacheSchema::parse_str($schema_json).expect("invalid schema for generator");
            let prefix = make_prefix(Fingerprint::Id($id));
            SIZES
                .iter()
                .map(|&n| $gen_fn(&schema, n, &prefix))
                .collect()
        });
    };
}

const ID_BENCH_ID: u32 = 7;

dataset_id!(INT_DATA_ID, INT_SCHEMA, gen_int, ID_BENCH_ID);
dataset!(INT_DATA, INT_SCHEMA, gen_int);
dataset!(LONG_DATA, LONG_SCHEMA, gen_long);
dataset!(FLOAT_DATA, FLOAT_SCHEMA, gen_float);
dataset!(BOOL_DATA, BOOL_SCHEMA, gen_bool);
dataset!(DOUBLE_DATA, DOUBLE_SCHEMA, gen_double);
dataset!(BYTES_DATA, BYTES_SCHEMA, gen_bytes);
dataset!(STRING_DATA, STRING_SCHEMA, gen_string);
dataset!(DATE_DATA, DATE_SCHEMA, gen_date);
dataset!(TMILLIS_DATA, TMILLIS_SCHEMA, gen_timemillis);
dataset!(TMICROS_DATA, TMICROS_SCHEMA, gen_timemicros);
dataset!(TSMILLIS_DATA, TSMILLIS_SCHEMA, gen_ts_millis);
dataset!(TSMICROS_DATA, TSMICROS_SCHEMA, gen_ts_micros);
dataset!(MAP_DATA, MAP_SCHEMA, gen_map);
dataset!(ARRAY_DATA, ARRAY_SCHEMA, gen_array);
dataset!(DECIMAL_DATA, DECIMAL_SCHEMA, gen_decimal);
dataset!(UUID_DATA, UUID_SCHEMA, gen_uuid);
dataset!(FIXED_DATA, FIXED_SCHEMA, gen_fixed);
dataset!(INTERVAL_DATA, INTERVAL_SCHEMA_ENCODE, gen_interval);
dataset!(ENUM_DATA, ENUM_SCHEMA, gen_enum);
dataset!(MIX_DATA, MIX_SCHEMA, gen_mixed);
dataset!(NEST_DATA, NEST_SCHEMA, gen_nested);

fn bench_with_decoder<F>(
    c: &mut Criterion,
    name: &str,
    data_sets: &[Vec<u8>],
    rows: &[usize],
    mut new_decoder: F,
) where
    F: FnMut() -> arrow_avro::reader::Decoder,
{
    let mut group = c.benchmark_group(name);
    for (idx, &row_count) in rows.iter().enumerate() {
        let datum = &data_sets[idx];
        group.throughput(Throughput::Bytes(datum.len() as u64));
        match row_count {
            10_000 => {
                group
                    .sample_size(25)
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
        group.bench_function(BenchmarkId::from_parameter(row_count), |b| {
            b.iter_batched_ref(
                &mut new_decoder,
                |decoder| {
                    black_box(decoder.decode(datum).unwrap());
                    black_box(decoder.flush().unwrap().unwrap());
                },
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

fn criterion_benches(c: &mut Criterion) {
    for &batch_size in &[SMALL_BATCH, LARGE_BATCH] {
        bench_with_decoder(c, "Interval", &INTERVAL_DATA, &SIZES, || {
            new_decoder(INTERVAL_SCHEMA, batch_size, false)
        });
        bench_with_decoder(c, "Int32", &INT_DATA, &SIZES, || {
            new_decoder(INT_SCHEMA, batch_size, false)
        });
        bench_with_decoder(c, "Int32_Id", &INT_DATA_ID, &SIZES, || {
            new_decoder_id(INT_SCHEMA, batch_size, false, ID_BENCH_ID)
        });
        bench_with_decoder(c, "Int64", &LONG_DATA, &SIZES, || {
            new_decoder(LONG_SCHEMA, batch_size, false)
        });
        bench_with_decoder(c, "Float32", &FLOAT_DATA, &SIZES, || {
            new_decoder(FLOAT_SCHEMA, batch_size, false)
        });
        bench_with_decoder(c, "Boolean", &BOOL_DATA, &SIZES, || {
            new_decoder(BOOL_SCHEMA, batch_size, false)
        });
        bench_with_decoder(c, "Float64", &DOUBLE_DATA, &SIZES, || {
            new_decoder(DOUBLE_SCHEMA, batch_size, false)
        });
        bench_with_decoder(c, "Binary(Bytes)", &BYTES_DATA, &SIZES, || {
            new_decoder(BYTES_SCHEMA, batch_size, false)
        });
        bench_with_decoder(c, "String", &STRING_DATA, &SIZES, || {
            new_decoder(STRING_SCHEMA, batch_size, false)
        });
        bench_with_decoder(c, "StringView", &STRING_DATA, &SIZES, || {
            new_decoder(STRING_SCHEMA, batch_size, true)
        });
        bench_with_decoder(c, "Date32", &DATE_DATA, &SIZES, || {
            new_decoder(DATE_SCHEMA, batch_size, false)
        });
        bench_with_decoder(c, "TimeMillis", &TMILLIS_DATA, &SIZES, || {
            new_decoder(TMILLIS_SCHEMA, batch_size, false)
        });
        bench_with_decoder(c, "TimeMicros", &TMICROS_DATA, &SIZES, || {
            new_decoder(TMICROS_SCHEMA, batch_size, false)
        });
        bench_with_decoder(c, "TimestampMillis", &TSMILLIS_DATA, &SIZES, || {
            new_decoder(TSMILLIS_SCHEMA, batch_size, false)
        });
        bench_with_decoder(c, "TimestampMicros", &TSMICROS_DATA, &SIZES, || {
            new_decoder(TSMICROS_SCHEMA, batch_size, false)
        });
        bench_with_decoder(c, "Map", &MAP_DATA, &SIZES, || {
            new_decoder(MAP_SCHEMA, batch_size, false)
        });
        bench_with_decoder(c, "Array", &ARRAY_DATA, &SIZES, || {
            new_decoder(ARRAY_SCHEMA, batch_size, false)
        });
        bench_with_decoder(c, "Decimal128", &DECIMAL_DATA, &SIZES, || {
            new_decoder(DECIMAL_SCHEMA, batch_size, false)
        });
        bench_with_decoder(c, "UUID", &UUID_DATA, &SIZES, || {
            new_decoder(UUID_SCHEMA, batch_size, false)
        });
        bench_with_decoder(c, "FixedSizeBinary", &FIXED_DATA, &SIZES, || {
            new_decoder(FIXED_SCHEMA, batch_size, false)
        });
        bench_with_decoder(c, "Enum(Dictionary)", &ENUM_DATA, &SIZES, || {
            new_decoder(ENUM_SCHEMA, batch_size, false)
        });
        bench_with_decoder(c, "Mixed", &MIX_DATA, &SIZES, || {
            new_decoder(MIX_SCHEMA, batch_size, false)
        });
        bench_with_decoder(c, "Nested(Struct)", &NEST_DATA, &SIZES, || {
            new_decoder(NEST_SCHEMA, batch_size, false)
        });
    }
}

criterion_group! {
    name = avro_decoder;
    config = Criterion::default().configure_from_args();
    targets = criterion_benches
}
criterion_main!(avro_decoder);
