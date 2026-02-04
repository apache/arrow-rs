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

use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::{DataType, Field, Float32Type, Float64Type, Int32Type, Int64Type, Schema};
use arrow::util::bench_util::{
    create_binary_array_with_len_range_and_prefix_and_seed, create_primitive_array_with_seed,
    create_string_array_with_len_range_and_prefix_and_seed,
};
use arrow_array::{FixedSizeBinaryArray, StringViewArray};
use bytes::Bytes;
use criterion::{Criterion, criterion_group, criterion_main};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Encoding;
use parquet::file::properties::WriterProperties;
use rand::{
    Rng, SeedableRng,
    distr::{Alphanumeric, StandardUniform},
    prelude::StdRng,
};
use std::sync::Arc;

#[derive(Copy, Clone)]
pub enum ColumnType {
    String(usize),
    StringView(usize),
    Binary(usize),
    FixedLen(i32),
    Int32,
    Int64,
    Float,
    Double,
}

// arrow::util::bench_util::create_fsb_array with a seed

/// Creates a random (but fixed-seeded) array of fixed size with a given null density and length
fn create_fsb_array_with_seed(
    size: usize,
    null_density: f32,
    fixed_len: i32,
    seed: u64,
) -> FixedSizeBinaryArray {
    let mut rng = StdRng::seed_from_u64(seed);

    let rng = &mut rng;
    FixedSizeBinaryArray::try_from_sparse_iter_with_size(
        (0..size).map(|_| {
            if rng.random::<f32>() < null_density {
                None
            } else {
                let value = rng
                    .sample_iter::<u8, _>(StandardUniform)
                    .take(fixed_len as usize)
                    .collect::<Vec<u8>>();
                Some(value)
            }
        }),
        fixed_len,
    )
    .unwrap()
}

// arrow::util::bench_util::create_string_view_array_with_max_len with a seed

/// Creates a random (but fixed-seeded) array of rand size with a given max size, null density and length
pub fn create_string_view_array_with_seed(
    size: usize,
    null_density: f32,
    max_str_len: usize,
    seed: u64,
) -> StringViewArray {
    let mut rng = StdRng::seed_from_u64(seed);

    let rng = &mut rng;
    (0..size)
        .map(|_| {
            if rng.random::<f32>() < null_density {
                None
            } else {
                let str_len = rng.random_range(max_str_len / 2..max_str_len);
                let value = rng.sample_iter(&Alphanumeric).take(str_len).collect();
                let value = String::from_utf8(value).unwrap();
                Some(value)
            }
        })
        .collect()
}

fn schema(column_type: ColumnType, num_columns: usize) -> Arc<Schema> {
    let field_type = match column_type {
        ColumnType::Binary(_) => DataType::Binary,
        ColumnType::String(_) => DataType::Utf8,
        ColumnType::StringView(_) => DataType::Utf8View,
        ColumnType::FixedLen(size) => DataType::FixedSizeBinary(size),
        ColumnType::Int32 => DataType::Int32,
        ColumnType::Int64 => DataType::Int64,
        ColumnType::Float => DataType::Float32,
        ColumnType::Double => DataType::Float64,
    };

    let fields: Vec<Field> = (0..num_columns)
        .map(|i| Field::new(format!("col_{i}"), field_type.clone(), true))
        .collect();
    Arc::new(Schema::new(fields))
}

fn create_batch(
    schema: &Arc<Schema>,
    column_type: ColumnType,
    seed: usize,
    num_columns: usize,
    num_rows: usize,
) -> RecordBatch {
    let null_density = 0.0001;
    let mut arrays: Vec<ArrayRef> = vec![];
    match column_type {
        ColumnType::Binary(max_len) => {
            for i in 0..num_columns {
                let array_seed = seed * num_columns + i;
                let array = create_binary_array_with_len_range_and_prefix_and_seed::<i32>(
                    num_rows,
                    null_density,
                    max_len / 2,
                    max_len,
                    &[],
                    array_seed as u64,
                );
                arrays.push(Arc::new(array));
            }
        }
        ColumnType::String(max_str_len) => {
            for i in 0..num_columns {
                let array_seed = seed * num_columns + i;
                let array = create_string_array_with_len_range_and_prefix_and_seed::<i32>(
                    num_rows,
                    null_density,
                    max_str_len / 2,
                    max_str_len,
                    "",
                    array_seed as u64,
                );
                arrays.push(Arc::new(array));
            }
        }
        ColumnType::StringView(max_str_len) => {
            for i in 0..num_columns {
                let array_seed = seed * num_columns + i;
                let array = create_string_view_array_with_seed(
                    num_rows,
                    null_density,
                    max_str_len,
                    array_seed as u64,
                );
                arrays.push(Arc::new(array));
            }
        }
        ColumnType::FixedLen(size) => {
            for i in 0..num_columns {
                let array_seed = seed * num_columns + i;
                let array =
                    create_fsb_array_with_seed(num_rows, null_density, size, array_seed as u64);
                arrays.push(Arc::new(array));
            }
        }
        ColumnType::Int32 => {
            for i in 0..num_columns {
                let array_seed = seed * num_columns + i;
                let array = create_primitive_array_with_seed::<Int32Type>(
                    num_rows,
                    null_density,
                    array_seed as u64,
                );
                arrays.push(Arc::new(array));
            }
        }
        ColumnType::Int64 => {
            for i in 0..num_columns {
                let array_seed = seed * num_columns + i;
                let array = create_primitive_array_with_seed::<Int64Type>(
                    num_rows,
                    null_density,
                    array_seed as u64,
                );
                arrays.push(Arc::new(array));
            }
        }
        ColumnType::Float => {
            for i in 0..num_columns {
                let array_seed = seed * num_columns + i;
                let array = create_primitive_array_with_seed::<Float32Type>(
                    num_rows,
                    null_density,
                    array_seed as u64,
                );
                arrays.push(Arc::new(array));
            }
        }
        ColumnType::Double => {
            for i in 0..num_columns {
                let array_seed = seed * num_columns + i;
                let array = create_primitive_array_with_seed::<Float64Type>(
                    num_rows,
                    null_density,
                    array_seed as u64,
                );
                arrays.push(Arc::new(array));
            }
        }
    }
    RecordBatch::try_new(schema.clone(), arrays).unwrap()
}

#[derive(Copy, Clone)]
pub struct ParquetFileSpec {
    column_type: ColumnType,
    num_columns: usize,
    num_row_groups: usize,
    rows_per_row_group: usize,
    rows_per_page: usize,
    encoding: Encoding,
    use_dict: bool,
}

const DEFAULT_NUM_COLUMNS: usize = 10;
const DEFAULT_NUM_ROWGROUPS: usize = 10;
const DEFAULT_ROWS_PER_PAGE: usize = 2_000;
const DEFAULT_ROWS_PER_ROWGROUP: usize = 10_000;

impl ParquetFileSpec {
    pub fn new(column_type: ColumnType) -> Self {
        Self {
            column_type,
            num_columns: DEFAULT_NUM_COLUMNS,
            num_row_groups: DEFAULT_NUM_ROWGROUPS,
            rows_per_row_group: DEFAULT_ROWS_PER_ROWGROUP,
            rows_per_page: DEFAULT_ROWS_PER_PAGE,
            encoding: Encoding::PLAIN,
            use_dict: true,
        }
    }

    pub fn with_num_columns(self, num_columns: usize) -> Self {
        Self {
            num_columns,
            ..self
        }
    }

    pub fn with_num_row_groups(self, num_row_groups: usize) -> Self {
        Self {
            num_row_groups,
            ..self
        }
    }

    pub fn with_rows_per_row_group(self, rows_per_row_group: usize) -> Self {
        Self {
            rows_per_row_group,
            ..self
        }
    }

    pub fn with_rows_per_page(self, rows_per_page: usize) -> Self {
        Self {
            rows_per_page,
            ..self
        }
    }

    pub fn with_encoding(self, encoding: Encoding) -> Self {
        Self { encoding, ..self }
    }

    pub fn with_use_dict(self, use_dict: bool) -> Self {
        Self { use_dict, ..self }
    }
}

fn file_from_spec(spec: ParquetFileSpec, buffer: &mut Vec<u8>) {
    const SEED: usize = 31;
    let num_rows = spec.rows_per_row_group.min(100);
    let rows_to_write = spec.num_row_groups * spec.rows_per_row_group;

    let schema = schema(spec.column_type, spec.num_columns);
    let props = WriterProperties::builder()
        .set_max_row_group_size(spec.rows_per_row_group)
        .set_data_page_row_count_limit(spec.rows_per_page)
        .set_encoding(spec.encoding)
        .set_dictionary_enabled(spec.use_dict)
        .set_compression(parquet::basic::Compression::UNCOMPRESSED)
        .build();

    let mut writer = ArrowWriter::try_new(buffer, schema.clone(), Some(props)).unwrap();

    // use the same batch repeatedly otherwise the data generation will dominate the time
    let batch = create_batch(&schema, spec.column_type, SEED, spec.num_columns, num_rows);

    let mut rows_written = 0;
    while rows_written < rows_to_write {
        writer.write(&batch).unwrap();
        rows_written += num_rows;
    }

    let parquet_metadata = writer.close().unwrap();
    assert_eq!(parquet_metadata.num_row_groups(), spec.num_row_groups);
    assert_eq!(
        parquet_metadata.file_metadata().num_rows() as usize,
        rows_to_write
    );
}

fn read_write(c: &mut Criterion, spec: ParquetFileSpec, msg: &str) {
    let mut buffer = Vec::with_capacity(1_000_000);

    // read once to size the buffer
    file_from_spec(spec, &mut buffer);

    c.bench_function(&format!("write {msg}"), |b| {
        buffer.clear();
        b.iter(|| file_from_spec(spec, &mut buffer))
    });

    let file_bytes = Bytes::from(buffer);
    c.bench_function(&format!("read {msg}"), |b| {
        b.iter(|| {
            let record_reader = ParquetRecordBatchReaderBuilder::try_new(file_bytes.clone())
                .unwrap()
                .build()
                .unwrap();
            let mut num_rows = 0;
            for maybe_batch in record_reader {
                let batch = maybe_batch.unwrap();
                num_rows += batch.num_rows();
            }
            assert_eq!(num_rows, spec.num_row_groups * spec.rows_per_row_group);
        })
    });
}

fn int_benches(c: &mut Criterion, column_type: ColumnType) {
    let ctype = match column_type {
        ColumnType::Int32 => "int32",
        ColumnType::Int64 => "int64",
        _ => unreachable!(),
    };

    let spec = ParquetFileSpec::new(column_type).with_use_dict(true);
    read_write(c, spec, &format!("{ctype} dict"));

    let spec = spec.with_use_dict(false).with_encoding(Encoding::PLAIN);
    read_write(c, spec, &format!("{ctype} plain"));

    let spec = spec.with_encoding(Encoding::DELTA_BINARY_PACKED);
    read_write(c, spec, &format!("{ctype} delta_binary"));

    let spec = spec.with_encoding(Encoding::BYTE_STREAM_SPLIT);
    read_write(c, spec, &format!("{ctype} byte_stream_split"));
}

fn float_benches(c: &mut Criterion, column_type: ColumnType) {
    let ctype = match column_type {
        ColumnType::Float => "f32",
        ColumnType::Double => "f64",
        _ => unreachable!(),
    };

    let spec = ParquetFileSpec::new(column_type).with_use_dict(true);
    read_write(c, spec, &format!("{ctype} dict"));

    let spec = spec.with_use_dict(false).with_encoding(Encoding::PLAIN);
    read_write(c, spec, &format!("{ctype} plain"));

    let spec = spec.with_encoding(Encoding::BYTE_STREAM_SPLIT);
    read_write(c, spec, &format!("{ctype} byte_stream_split"));
}

fn string_benches(c: &mut Criterion, max_str_len: usize) {
    let spec = ParquetFileSpec::new(ColumnType::String(max_str_len))
        .with_num_columns(5)
        .with_use_dict(true);
    read_write(c, spec, &format!("String({max_str_len}) dict"));

    let spec = spec.with_use_dict(false).with_encoding(Encoding::PLAIN);
    read_write(c, spec, &format!("String({max_str_len}) plain"));

    let spec = spec.with_encoding(Encoding::DELTA_LENGTH_BYTE_ARRAY);
    read_write(c, spec, &format!("String({max_str_len}) delta_length"));

    let spec = spec.with_encoding(Encoding::DELTA_BYTE_ARRAY);
    read_write(c, spec, &format!("String({max_str_len}) delta_byte_array"));

    let spec = ParquetFileSpec::new(ColumnType::StringView(max_str_len))
        .with_num_columns(5)
        .with_use_dict(true);
    read_write(c, spec, &format!("StringView({max_str_len}) dict"));

    let spec = spec.with_use_dict(false).with_encoding(Encoding::PLAIN);
    read_write(c, spec, &format!("StringView({max_str_len}) plain"));

    let spec = spec.with_encoding(Encoding::DELTA_LENGTH_BYTE_ARRAY);
    read_write(c, spec, &format!("StringView({max_str_len}) delta_length"));

    let spec = spec.with_encoding(Encoding::DELTA_BYTE_ARRAY);
    read_write(
        c,
        spec,
        &format!("StringView({max_str_len}) delta_byte_array"),
    );
}

fn binary_benches(c: &mut Criterion, max_len: usize) {
    let spec = ParquetFileSpec::new(ColumnType::Binary(max_len))
        .with_num_columns(5)
        .with_use_dict(true);
    read_write(c, spec, &format!("Binary({max_len}) dict"));

    let spec = spec.with_use_dict(false).with_encoding(Encoding::PLAIN);
    read_write(c, spec, &format!("Binary({max_len}) plain"));

    let spec = spec.with_encoding(Encoding::DELTA_LENGTH_BYTE_ARRAY);
    read_write(c, spec, &format!("Binary({max_len}) delta_length"));

    let spec = spec.with_encoding(Encoding::DELTA_BYTE_ARRAY);
    read_write(c, spec, &format!("Binary({max_len}) delta_byte_array"));
}

fn flba_benches(c: &mut Criterion, len: i32) {
    let spec = ParquetFileSpec::new(ColumnType::FixedLen(len))
        .with_num_columns(5)
        .with_use_dict(true);
    read_write(c, spec, &format!("Fixed({len}) dict"));

    let spec = spec.with_use_dict(false).with_encoding(Encoding::PLAIN);
    read_write(c, spec, &format!("Fixed({len}) plain"));

    let spec = spec.with_encoding(Encoding::BYTE_STREAM_SPLIT);
    read_write(c, spec, &format!("Fixed({len}) byte_stream_split"));

    let spec = spec.with_encoding(Encoding::DELTA_BYTE_ARRAY);
    read_write(c, spec, &format!("Fixed({len}) delta_byte_array"));
}

fn criterion_benchmark(c: &mut Criterion) {
    int_benches(c, ColumnType::Int32);
    int_benches(c, ColumnType::Int64);
    float_benches(c, ColumnType::Float);
    float_benches(c, ColumnType::Double);
    string_benches(c, 20);
    string_benches(c, 100);
    binary_benches(c, 20);
    binary_benches(c, 100);
    flba_benches(c, 2);
    flba_benches(c, 16);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
