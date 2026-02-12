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

//! Benchmarks for RowGroupIndexReader performance
//!
//! This benchmark tests the performance of reading row group indices,
//! comparing single row group reads vs multiple row group reads.

use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_schema::extension::ExtensionType;
use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::sync::Arc;

#[derive(Debug, Default, Clone, Copy, PartialEq)]
struct RowGroupIndex;

impl ExtensionType for RowGroupIndex {
    const NAME: &'static str = "parquet.virtual.row_group_index";
    type Metadata = &'static str;

    fn metadata(&self) -> &Self::Metadata {
        &""
    }

    fn serialize_metadata(&self) -> Option<String> {
        Some(String::default())
    }

    fn deserialize_metadata(
        metadata: Option<&str>,
    ) -> Result<Self::Metadata, arrow_schema::ArrowError> {
        if metadata.is_some_and(str::is_empty) {
            Ok("")
        } else {
            Err(arrow_schema::ArrowError::InvalidArgumentError(
                "Virtual column extension type expects an empty string as metadata".to_owned(),
            ))
        }
    }

    fn supports_data_type(
        &self,
        data_type: &ArrowDataType,
    ) -> Result<(), arrow_schema::ArrowError> {
        match data_type {
            ArrowDataType::Int64 => Ok(()),
            data_type => Err(arrow_schema::ArrowError::InvalidArgumentError(format!(
                "Virtual column data type mismatch, expected Int64, found {data_type}"
            ))),
        }
    }

    fn try_new(
        data_type: &ArrowDataType,
        _metadata: Self::Metadata,
    ) -> Result<Self, arrow_schema::ArrowError> {
        RowGroupIndex.supports_data_type(data_type).map(|_| Self)
    }
}

fn create_test_file(num_row_groups: usize, rows_per_group: usize) -> Bytes {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", ArrowDataType::Int64, false),
        Field::new("value", ArrowDataType::Int64, false),
    ]));

    let mut buffer = Vec::new();
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_max_row_group_row_count(Some(rows_per_group))
        .build();

    let mut writer = ArrowWriter::try_new(&mut buffer, schema.clone(), Some(props)).unwrap();

    for group_idx in 0..num_row_groups {
        let mut id_values = Vec::with_capacity(rows_per_group);
        let mut value_values = Vec::with_capacity(rows_per_group);

        for i in 0..rows_per_group {
            id_values.push((group_idx * rows_per_group + i) as i64);
            value_values.push((i * 2) as i64);
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow::array::Int64Array::from(id_values)),
                Arc::new(arrow::array::Int64Array::from(value_values)),
            ],
        )
        .unwrap();

        writer.write(&batch).unwrap();
    }

    writer.close().unwrap();
    Bytes::from(buffer)
}

fn bench_single_row_group_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("row_group_index_single");
    group.sample_size(50);

    // test different row counts to see how the optimization scales
    // these numbers were tuned to ensure ms-range measurements
    for rows_per_group in [100_000, 500_000, 1_000_000, 5_000_000] {
        let file = create_test_file(3, rows_per_group);

        let row_group_index_field = Arc::new(
            Field::new("row_group_index", ArrowDataType::Int64, false)
                .with_extension_type(RowGroupIndex),
        );

        group.bench_with_input(
            BenchmarkId::new("single_row_group", rows_per_group),
            &rows_per_group,
            |b, &_rows| {
                b.iter(|| {
                    let options = ArrowReaderOptions::new()
                        .with_virtual_columns(vec![row_group_index_field.clone()])
                        .unwrap();

                    let reader = ParquetRecordBatchReaderBuilder::try_new_with_options(
                        file.clone(),
                        options,
                    )
                    .unwrap()
                    .with_row_groups(vec![1])
                    .build()
                    .unwrap();

                    for batch in reader {
                        let batch = batch.unwrap();
                        let _ = batch.column_by_name("row_group_index").unwrap();
                    }
                });
            },
        );
    }

    group.finish();
}

fn bench_multiple_row_group_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("row_group_index_multiple");
    group.sample_size(50);

    for num_row_groups in [2, 5, 10, 20] {
        let rows_per_group = 100_000;
        let file = create_test_file(num_row_groups, rows_per_group);

        let row_group_index_field = Arc::new(
            Field::new("row_group_index", ArrowDataType::Int64, false)
                .with_extension_type(RowGroupIndex),
        );

        group.bench_with_input(
            BenchmarkId::new("num_row_groups", num_row_groups),
            &num_row_groups,
            |b, &num_rg| {
                b.iter(|| {
                    let options = ArrowReaderOptions::new()
                        .with_virtual_columns(vec![row_group_index_field.clone()])
                        .unwrap();

                    let row_groups: Vec<usize> = (0..num_rg).collect();
                    let reader = ParquetRecordBatchReaderBuilder::try_new_with_options(
                        file.clone(),
                        options,
                    )
                    .unwrap()
                    .with_row_groups(row_groups)
                    .build()
                    .unwrap();

                    for batch in reader {
                        let batch = batch.unwrap();
                        let _ = batch.column_by_name("row_group_index").unwrap();
                    }
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_single_row_group_read,
    bench_multiple_row_group_read
);
criterion_main!(benches);
