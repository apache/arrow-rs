// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use criterion::*;
use half::f16;
use parquet::basic::{Encoding, Type as ParquetType};
use parquet::data_type::{
    ByteArray, ByteArrayType, DataType, DoubleType, FixedLenByteArray, FixedLenByteArrayType,
    FloatType, Int32Type, Int64Type,
};
use parquet::decoding::{Decoder, get_decoder};
use parquet::encoding::get_encoder;
use parquet::schema::types::{ColumnDescPtr, ColumnDescriptor, ColumnPath, Type};
use rand::prelude::*;
use std::sync::Arc;

fn bench_typed<T: DataType>(
    c: &mut Criterion,
    values: &[T::T],
    encoding: Encoding,
    type_length: i32,
) {
    let name = format!(
        "dtype={}, encoding={:?}",
        match T::get_physical_type() {
            ParquetType::FIXED_LEN_BYTE_ARRAY => format!("FixedLenByteArray({type_length})"),
            _ => std::any::type_name::<T::T>().to_string(),
        },
        encoding
    );
    let column_desc_ptr = column_descriptor::<T>(type_length);
    c.bench_function(&format!("encoding: {name}"), |b| {
        b.iter(|| {
            let mut encoder = get_encoder::<T>(encoding, &column_desc_ptr).unwrap();
            encoder.put(values).unwrap();
            encoder.flush_buffer().unwrap();
        });
    });

    let mut encoder = get_encoder::<T>(encoding, &column_desc_ptr).unwrap();
    encoder.put(values).unwrap();
    let encoded = encoder.flush_buffer().unwrap();
    println!("{} encoded as {} bytes", name, encoded.len());

    let mut buffer = vec![T::T::default(); values.len()];
    c.bench_function(&format!("decoding: {name}"), |b| {
        b.iter(|| {
            let mut decoder: Box<dyn Decoder<T>> =
                get_decoder(column_desc_ptr.clone(), encoding).unwrap();
            decoder.set_data(encoded.clone(), values.len()).unwrap();
            decoder.get(&mut buffer).unwrap();
        });
    });
}

/// Like [`bench_typed`], but reuses one encoder and one decoder across iterations,
/// as the column writer and reader do across the pages of a column chunk, so the
/// steady state is measured rather than buffer growth in fresh encoders.
fn bench_page<T: DataType>(c: &mut Criterion, values: &[T::T], encoding: Encoding, data: &str) {
    let name = format!(
        "dtype={}, encoding={encoding:?}, data={data}",
        std::any::type_name::<T::T>()
    );
    let column_desc_ptr = column_descriptor::<T>(0);
    let mut encoder = get_encoder::<T>(encoding, &column_desc_ptr).unwrap();
    c.bench_function(&format!("encoding: {name}"), |b| {
        b.iter(|| {
            encoder.put(values).unwrap();
            encoder.flush_buffer().unwrap()
        });
    });

    encoder.put(values).unwrap();
    let encoded = encoder.flush_buffer().unwrap();
    println!("{} encoded as {} bytes", name, encoded.len());

    let mut buffer = vec![T::T::default(); values.len()];
    let mut decoder: Box<dyn Decoder<T>> = get_decoder(column_desc_ptr, encoding).unwrap();
    c.bench_function(&format!("decoding: {name}"), |b| {
        b.iter(|| {
            decoder.set_data(encoded.clone(), values.len()).unwrap();
            decoder.get(&mut buffer).unwrap();
        });
    });
}

fn column_descriptor<T: DataType>(type_length: i32) -> ColumnDescPtr {
    ColumnDescPtr::new(ColumnDescriptor::new(
        Arc::new(
            Type::primitive_type_builder("", T::get_physical_type())
                .with_length(type_length)
                .build()
                .unwrap(),
        ),
        0,
        0,
        ColumnPath::new(vec![]),
    ))
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut rng = StdRng::seed_from_u64(0);
    let n = 16 * 1024;

    let mut f16s = Vec::new();
    let mut f32s = Vec::new();
    let mut f64s = Vec::new();
    let mut d128s = Vec::new();
    for _ in 0..n {
        f16s.push(FixedLenByteArray::from(
            f16::from_f32(rng.random::<f32>()).to_le_bytes().to_vec(),
        ));
        f32s.push(rng.random::<f32>());
        f64s.push(rng.random::<f64>());
        d128s.push(FixedLenByteArray::from(
            rng.random::<i128>().to_be_bytes().to_vec(),
        ));
    }

    bench_typed::<FloatType>(c, &f32s, Encoding::BYTE_STREAM_SPLIT, 0);
    bench_typed::<DoubleType>(c, &f64s, Encoding::BYTE_STREAM_SPLIT, 0);
    bench_typed::<FixedLenByteArrayType>(c, &f16s, Encoding::BYTE_STREAM_SPLIT, 2);
    bench_typed::<FixedLenByteArrayType>(c, &d128s, Encoding::BYTE_STREAM_SPLIT, 16);

    bench_delta_encodings(c, &mut rng, n);
}

/// DELTA_BINARY_PACKED on well-predicted (sorted, small gaps) and poorly predicted
/// (uniformly random, full-width deltas) integers, and the two delta byte array
/// encodings on sorted keys that share prefixes, on random strings, and on large values.
fn bench_delta_encodings(c: &mut Criterion, rng: &mut StdRng, n: usize) {
    let mut acc = 0i32;
    let sorted_i32: Vec<i32> = (0..n)
        .map(|_| {
            acc += rng.random_range(0..64);
            acc
        })
        .collect();
    let random_i32: Vec<i32> = (0..n).map(|_| rng.random()).collect();
    let mut ts = 1_700_000_000_000_000_000i64;
    let timestamps_i64: Vec<i64> = (0..n)
        .map(|_| {
            ts += 1_000_000 + rng.random_range(-1000..1000);
            ts
        })
        .collect();
    let random_i64: Vec<i64> = (0..n).map(|_| rng.random()).collect();

    let delta = Encoding::DELTA_BINARY_PACKED;
    bench_page::<Int32Type>(c, &sorted_i32, delta, "sorted");
    bench_page::<Int32Type>(c, &random_i32, delta, "random");
    bench_page::<Int64Type>(c, &timestamps_i64, delta, "timestamps");
    bench_page::<Int64Type>(c, &random_i64, delta, "random");

    let sorted_keys: Vec<ByteArray> = (0..n)
        .map(|i| ByteArray::from(format!("customer-{:012}", i * 7).into_bytes()))
        .collect();
    let random_strings: Vec<ByteArray> = (0..n)
        .map(|_| {
            let len = rng.random_range(8..32);
            let bytes: Vec<u8> = (0..len).map(|_| rng.random_range(b'a'..=b'z')).collect();
            ByteArray::from(bytes)
        })
        .collect();
    // Few large values sharing a long prefix, where copying value bytes dominates.
    let large_values: Vec<ByteArray> = (0..n / 16)
        .map(|i| {
            let mut bytes = vec![b'x'; 1024];
            bytes.extend_from_slice(format!("{i:08}").as_bytes());
            ByteArray::from(bytes)
        })
        .collect();
    for encoding in [
        Encoding::DELTA_LENGTH_BYTE_ARRAY,
        Encoding::DELTA_BYTE_ARRAY,
    ] {
        bench_page::<ByteArrayType>(c, &sorted_keys, encoding, "sorted_keys");
        bench_page::<ByteArrayType>(c, &random_strings, encoding, "random");
        bench_page::<ByteArrayType>(c, &large_values, encoding, "large_1k");
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
