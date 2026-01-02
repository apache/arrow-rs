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

use arrow_json::ReaderBuilder;
use arrow_schema::{DataType, Field};
use criterion::{Criterion, criterion_group, criterion_main};
use std::fmt::Write;
use std::hint::black_box;
use std::sync::Arc;

const BINARY_ROWS: usize = 1 << 15;
const LIST_ROWS: usize = 1 << 14;
const BINARY_BYTES: usize = 64;
const LIST_LEN: usize = 32;

fn bench_decode(c: &mut Criterion, name: &str, data: &[u8], field: Arc<Field>, rows: usize) {
    c.bench_function(name, |b| {
        b.iter(|| {
            let mut decoder = ReaderBuilder::new_with_field(field.clone())
                .with_batch_size(rows)
                .build_decoder()
                .unwrap();

            let mut offset = 0;
            while offset < data.len() {
                let read = decoder.decode(black_box(&data[offset..])).unwrap();
                if read == 0 {
                    break;
                }
                offset += read;
            }

            let batch = decoder.flush().unwrap();
            black_box(batch);
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

fn build_numeric_list_lines(rows: usize, list_len: usize) -> Vec<u8> {
    let mut data = String::with_capacity(rows * (list_len * 6 + 3));
    for row in 0..rows {
        data.push('[');
        for i in 0..list_len {
            let value = (row as i64) * (list_len as i64) + i as i64;
            let _ = write!(data, "{value}");
            if i + 1 != list_len {
                data.push(',');
            }
        }
        data.push(']');
        data.push('\n');
    }
    data.into_bytes()
}

fn criterion_benchmark(c: &mut Criterion) {
    let binary_data = build_hex_lines(BINARY_ROWS, BINARY_BYTES);

    let binary_field = Arc::new(Field::new("item", DataType::Binary, false));
    bench_decode(
        c,
        "decode_binary_hex_json",
        &binary_data,
        binary_field,
        BINARY_ROWS,
    );

    let fixed_field = Arc::new(Field::new(
        "item",
        DataType::FixedSizeBinary(BINARY_BYTES as i32),
        false,
    ));
    bench_decode(
        c,
        "decode_fixed_binary_hex_json",
        &binary_data,
        fixed_field,
        BINARY_ROWS,
    );

    let view_field = Arc::new(Field::new("item", DataType::BinaryView, false));
    bench_decode(
        c,
        "decode_binary_view_hex_json",
        &binary_data,
        view_field,
        BINARY_ROWS,
    );
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
