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

use apache_avro::types::Value;
use apache_avro::{Schema as ApacheSchema, to_avro_datum};
use arrow_avro::reader::{Decoder, ReaderBuilder};
use arrow_avro::schema::{
    AvroSchema, Fingerprint, FingerprintAlgorithm, SINGLE_OBJECT_MAGIC, SchemaStore,
};
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput};
use criterion::{criterion_group, criterion_main};
use rand::Rng;
use rand::rngs::ThreadRng;
use std::hint::black_box;

const BATCH_SIZE: usize = 8192;

const NUM_ROWS: usize = 10_000;

fn make_prefix(fp: Fingerprint) -> Vec<u8> {
    match fp {
        Fingerprint::Rabin(val) => {
            let mut buf = Vec::with_capacity(SINGLE_OBJECT_MAGIC.len() + size_of::<u64>());
            buf.extend_from_slice(&SINGLE_OBJECT_MAGIC); // C3 01
            buf.extend_from_slice(&val.to_le_bytes()); // little-endian
            buf
        }
        other => panic!("Unexpected fingerprint {other:?}"),
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

fn gen_avro_data_with<F>(schema_json: &str, num_rows: usize, gen_fn: F) -> Vec<u8>
where
    F: FnOnce(ThreadRng, &ApacheSchema, usize, &[u8]) -> Vec<u8>,
{
    let schema = ApacheSchema::parse_str(schema_json).expect("invalid schema for generator");
    let arrow_schema = AvroSchema::new(schema_json.to_owned());
    let fingerprint = arrow_schema
        .fingerprint(FingerprintAlgorithm::Rabin)
        .expect("fingerprint failed");
    let prefix = make_prefix(fingerprint);
    gen_fn(rand::rng(), &schema, num_rows, &prefix)
}

fn gen_int(mut rng: impl Rng, sc: &ApacheSchema, n: usize, prefix: &[u8]) -> Vec<u8> {
    encode_records_with_prefix(
        sc,
        prefix,
        (0..n).map(|i| {
            Value::Record(vec![
                ("id".into(), Value::Int(i as i32)),
                ("field1".into(), Value::Int(rng.random())),
            ])
        }),
    )
}

fn gen_long(mut rng: impl Rng, sc: &ApacheSchema, n: usize, prefix: &[u8]) -> Vec<u8> {
    encode_records_with_prefix(
        sc,
        prefix,
        (0..n).map(|i| {
            Value::Record(vec![
                ("id".into(), Value::Int(i as i32)),
                ("field1".into(), Value::Long(rng.random())),
            ])
        }),
    )
}

fn gen_float(mut rng: impl Rng, sc: &ApacheSchema, n: usize, prefix: &[u8]) -> Vec<u8> {
    encode_records_with_prefix(
        sc,
        prefix,
        (0..n).map(|i| {
            Value::Record(vec![
                ("id".into(), Value::Int(i as i32)),
                ("field1".into(), Value::Float(rng.random())),
            ])
        }),
    )
}

fn gen_double(mut rng: impl Rng, sc: &ApacheSchema, n: usize, prefix: &[u8]) -> Vec<u8> {
    encode_records_with_prefix(
        sc,
        prefix,
        (0..n).map(|i| {
            Value::Record(vec![
                ("id".into(), Value::Int(i as i32)),
                ("field1".into(), Value::Double(rng.random())),
            ])
        }),
    )
}

const READER_SCHEMA: &str = r#"
    {
        "type":"record",
        "name":"table",
        "fields": [
            { "name": "id", "type": "int" }
        ]
    }
    "#;

const INT_SCHEMA: &str = r#"
    {
        "type":"record",
        "name":"table",
        "fields": [
            { "name": "id", "type": "int" },
            { "name": "field1", "type": "int" }
        ]
    }
    "#;

const LONG_SCHEMA: &str = r#"
    {
        "type":"record",
        "name":"table",
        "fields": [
            { "name": "id", "type": "int" },
            { "name": "field1", "type": "long" }
        ]
    }
    "#;

const FLOAT_SCHEMA: &str = r#"
    {
        "type":"record",
        "name":"table",
        "fields": [
            { "name": "id", "type": "int" },
            { "name": "field1", "type": "float" }
        ]
    }
    "#;

const DOUBLE_SCHEMA: &str = r#"
    {
        "type":"record",
        "name":"table",
        "fields": [
            { "name": "id", "type": "int" },
            { "name": "field1", "type": "double" }
        ]
    }
    "#;

fn new_decoder(schema_json: &'static str, batch_size: usize) -> Decoder {
    let schema = AvroSchema::new(schema_json.to_owned());
    let mut store = SchemaStore::new();
    store.register(schema).unwrap();
    let reader_schema = AvroSchema::new(READER_SCHEMA.to_owned());
    ReaderBuilder::new()
        .with_writer_schema_store(store)
        .with_batch_size(batch_size)
        .with_reader_schema(reader_schema)
        .build_decoder()
        .expect("failed to build decoder")
}

fn bench_with_decoder<F>(
    c: &mut Criterion,
    name: &str,
    data: &[u8],
    num_rows: usize,
    mut new_decoder: F,
) where
    F: FnMut() -> Decoder,
{
    let mut group = c.benchmark_group(name);
    group.throughput(Throughput::Bytes(data.len() as u64));
    group.bench_function(BenchmarkId::from_parameter(num_rows), |b| {
        b.iter_batched_ref(
            &mut new_decoder,
            |decoder| {
                black_box(decoder.decode(data).unwrap());
                black_box(decoder.flush().unwrap().unwrap());
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

fn criterion_benches(c: &mut Criterion) {
    let data = gen_avro_data_with(INT_SCHEMA, NUM_ROWS, gen_int);
    bench_with_decoder(c, "skip_int", &data, NUM_ROWS, || {
        new_decoder(INT_SCHEMA, BATCH_SIZE)
    });
    let data = gen_avro_data_with(LONG_SCHEMA, NUM_ROWS, gen_long);
    bench_with_decoder(c, "skip_long", &data, NUM_ROWS, || {
        new_decoder(LONG_SCHEMA, BATCH_SIZE)
    });
    let data = gen_avro_data_with(FLOAT_SCHEMA, NUM_ROWS, gen_float);
    bench_with_decoder(c, "skip_float", &data, NUM_ROWS, || {
        new_decoder(FLOAT_SCHEMA, BATCH_SIZE)
    });
    let data = gen_avro_data_with(DOUBLE_SCHEMA, NUM_ROWS, gen_double);
    bench_with_decoder(c, "skip_double", &data, NUM_ROWS, || {
        new_decoder(DOUBLE_SCHEMA, BATCH_SIZE)
    });
}

criterion_group! {
    name = avro_project_record;
    config = Criterion::default().configure_from_args();
    targets = criterion_benches
}
criterion_main!(avro_project_record);
