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
use half::f16;
use parquet::basic::{Encoding, Type as ParquetType};
use parquet::data_type::{
    DataType, DoubleType, FixedLenByteArray, FixedLenByteArrayType, FloatType,
};
use parquet::decoding::{get_decoder, Decoder};
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
    let column_desc_ptr = ColumnDescPtr::new(ColumnDescriptor::new(
        Arc::new(
            Type::primitive_type_builder("", T::get_physical_type())
                .with_length(type_length)
                .build()
                .unwrap(),
        ),
        0,
        0,
        ColumnPath::new(vec![]),
    ));
    c.bench_function(&format!("encoding: {}", name), |b| {
        b.iter(|| {
            let mut encoder = get_encoder::<T>(encoding, &column_desc_ptr).unwrap();
            encoder.put(values).unwrap();
            encoder.flush_buffer().unwrap();
        });
    });

    let mut encoder = get_encoder::<T>(encoding, &column_desc_ptr).unwrap();
    encoder.put(values).unwrap();
    let encoded = encoder.flush_buffer().unwrap();
    println!("{} encoded as {} bytes", name, encoded.len(),);

    let mut buffer = vec![T::T::default(); values.len()];
    c.bench_function(&format!("decoding: {}", name), |b| {
        b.iter(|| {
            let mut decoder: Box<dyn Decoder<T>> =
                get_decoder(column_desc_ptr.clone(), encoding).unwrap();
            decoder.set_data(encoded.clone(), values.len()).unwrap();
            decoder.get(&mut buffer).unwrap();
        });
    });
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
            f16::from_f32(rng.gen::<f32>()).to_le_bytes().to_vec(),
        ));
        f32s.push(rng.gen::<f32>());
        f64s.push(rng.gen::<f64>());
        d128s.push(FixedLenByteArray::from(
            rng.gen::<i128>().to_be_bytes().to_vec(),
        ));
    }

    bench_typed::<FloatType>(c, &f32s, Encoding::BYTE_STREAM_SPLIT, 0);
    bench_typed::<DoubleType>(c, &f64s, Encoding::BYTE_STREAM_SPLIT, 0);
    bench_typed::<FixedLenByteArrayType>(c, &f16s, Encoding::BYTE_STREAM_SPLIT, 2);
    bench_typed::<FixedLenByteArrayType>(c, &d128s, Encoding::BYTE_STREAM_SPLIT, 16);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
