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

//! Benchmarks of benchmark for extracting arrow statistics from parquet

use arrow::array::{ArrayRef, DictionaryArray, Float64Array, StringArray, UInt64Array};
use arrow_array::{Decimal128Array, Int32Array, Int64Array, RecordBatch, StringViewArray};
use arrow_schema::{
    DataType::{self, *},
    Field, Schema,
};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use parquet::{
    arrow::arrow_reader::ArrowReaderOptions,
    file::{
        metadata::{PageIndexPolicy, page_index::PageIndexBuilder},
        page_index::index_reader::decode_column_index,
        properties::WriterProperties,
    },
};
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ArrowReaderBuilder},
    file::properties::EnabledStatistics,
};
use std::sync::Arc;
use tempfile::NamedTempFile;
#[derive(Debug, Clone)]
enum TestTypes {
    UInt64,
    Int64,
    F64,
    String,
    Dictionary,
}

use parquet::arrow::arrow_reader::statistics::StatisticsConverter;
use std::fmt;

impl fmt::Display for TestTypes {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TestTypes::UInt64 => write!(f, "UInt64"),
            TestTypes::Int64 => write!(f, "Int64"),
            TestTypes::F64 => write!(f, "F64"),
            TestTypes::String => write!(f, "String"),
            TestTypes::Dictionary => write!(f, "Dictionary(Int32, String)"),
        }
    }
}

fn create_parquet_file(
    dtype: TestTypes,
    row_groups: usize,
    data_page_row_count_limit: Option<usize>,
) -> NamedTempFile {
    let schema = match dtype {
        TestTypes::UInt64 => Arc::new(Schema::new(vec![Field::new("col", DataType::UInt64, true)])),
        TestTypes::Int64 => Arc::new(Schema::new(vec![Field::new("col", DataType::Int64, true)])),
        TestTypes::F64 => Arc::new(Schema::new(vec![Field::new(
            "col",
            DataType::Float64,
            true,
        )])),
        TestTypes::String => Arc::new(Schema::new(vec![Field::new("col", DataType::Utf8, true)])),
        TestTypes::Dictionary => Arc::new(Schema::new(vec![Field::new(
            "col",
            DataType::Dictionary(Box::new(Int32), Box::new(Utf8)),
            true,
        )])),
    };

    let mut props = WriterProperties::builder().set_max_row_group_row_count(Some(row_groups));
    if let Some(limit) = data_page_row_count_limit {
        props = props
            .set_data_page_row_count_limit(limit)
            .set_statistics_enabled(EnabledStatistics::Page);
    }
    let props = props.build();

    let file = tempfile::Builder::new()
        .suffix(".parquet")
        .tempfile()
        .unwrap();
    let mut writer =
        ArrowWriter::try_new(file.reopen().unwrap(), schema.clone(), Some(props)).unwrap();

    for _ in 0..row_groups {
        let batch = match dtype {
            TestTypes::UInt64 => make_uint64_batch(),
            TestTypes::Int64 => make_int64_batch(),
            TestTypes::F64 => make_f64_batch(),
            TestTypes::String => make_string_batch(),
            TestTypes::Dictionary => make_dict_batch(),
        };
        if data_page_row_count_limit.is_some() {
            // Send batches one at a time. This allows the
            // writer to apply the page limit, that is only
            // checked on RecordBatch boundaries.
            for i in 0..batch.num_rows() {
                writer.write(&batch.slice(i, 1)).unwrap();
            }
        } else {
            writer.write(&batch).unwrap();
        }
    }
    writer.close().unwrap();
    file
}

fn make_uint64_batch() -> RecordBatch {
    let array: ArrayRef = Arc::new(UInt64Array::from(vec![
        Some(1),
        Some(2),
        Some(3),
        Some(4),
        Some(5),
    ]));
    RecordBatch::try_new(
        Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("col", UInt64, false),
        ])),
        vec![array],
    )
    .unwrap()
}

fn make_int64_batch() -> RecordBatch {
    let array: ArrayRef = Arc::new(Int64Array::from(vec![
        Some(1),
        Some(2),
        Some(3),
        Some(4),
        Some(5),
    ]));
    RecordBatch::try_new(
        Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("col", Int64, false),
        ])),
        vec![array],
    )
    .unwrap()
}

fn make_f64_batch() -> RecordBatch {
    let array: ArrayRef = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0]));
    RecordBatch::try_new(
        Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("col", Float64, false),
        ])),
        vec![array],
    )
    .unwrap()
}

fn make_string_batch() -> RecordBatch {
    let array: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"]));
    RecordBatch::try_new(
        Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("col", Utf8, false),
        ])),
        vec![array],
    )
    .unwrap()
}

fn make_dict_batch() -> RecordBatch {
    let keys = Int32Array::from(vec![0, 1, 2, 3, 4]);
    let values = StringArray::from(vec!["a", "b", "c", "d", "e"]);
    let array: ArrayRef = Arc::new(DictionaryArray::try_new(keys, Arc::new(values)).unwrap());
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "col",
            Dictionary(Box::new(Int32), Box::new(Utf8)),
            false,
        )])),
        vec![array],
    )
    .unwrap()
}

fn criterion_benchmark(c: &mut Criterion) {
    let row_groups = 100;
    use TestTypes::*;
    let types = vec![Int64, UInt64, F64, String, Dictionary];
    let data_page_row_count_limits = vec![None, Some(1)];

    for dtype in types {
        for data_page_row_count_limit in &data_page_row_count_limits {
            let file = create_parquet_file(dtype.clone(), row_groups, *data_page_row_count_limit);
            let file = file.reopen().unwrap();
            let options =
                ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::from(true));
            let reader = ArrowReaderBuilder::try_new_with_options(file, options).unwrap();
            let metadata = reader.metadata();
            let row_groups = metadata.row_groups();
            let row_group_indices: Vec<_> = (0..row_groups.len()).collect();

            let statistic_type = if data_page_row_count_limit.is_some() {
                "data page"
            } else {
                "row group"
            };

            let mut group = c.benchmark_group(format!(
                "Extract {} statistics for {}",
                statistic_type,
                dtype.clone()
            ));
            group.bench_function(BenchmarkId::new("extract_statistics", dtype.clone()), |b| {
                b.iter(|| {
                    let converter = StatisticsConverter::try_new(
                        "col",
                        reader.schema(),
                        reader.parquet_schema(),
                    )
                    .unwrap();

                    if data_page_row_count_limit.is_some() {
                        let page_index = reader
                            .metadata()
                            .page_index()
                            .expect("File should have page indices")
                            .as_ref();

                        let _ = converter.data_page_mins(page_index, &row_group_indices);
                        let _ = converter.data_page_maxes(page_index, &row_group_indices);
                        let _ = converter.data_page_null_counts(page_index, &row_group_indices);
                        let _ = converter.data_page_row_counts(
                            page_index,
                            row_groups,
                            &row_group_indices,
                        );
                    } else {
                        let _ = converter.row_group_mins(row_groups.iter()).unwrap();
                        let _ = converter.row_group_maxes(row_groups.iter()).unwrap();
                        let _ = converter.row_group_null_counts(row_groups.iter()).unwrap();
                        let _ = converter.row_group_row_counts(row_groups.iter()).unwrap();
                    }
                })
            });
            group.finish();
        }
    }
}

/// Makes one column with `rows` values, where every 7th value is null.
fn make_page_index_column(data_type: &DataType, rows: usize) -> ArrayRef {
    let valid = |i: usize| !i.is_multiple_of(7);
    match data_type {
        Int64 => Arc::new(Int64Array::from_iter(
            (0..rows).map(|i| valid(i).then_some(i as i64 * 3)),
        )),
        Utf8 => Arc::new(StringArray::from_iter(
            (0..rows).map(|i| valid(i).then(|| format!("value-{i:08}"))),
        )),
        Utf8View => Arc::new(StringViewArray::from_iter(
            (0..rows).map(|i| valid(i).then(|| format!("value-{i:08}"))),
        )),
        Decimal128(precision, scale) => Arc::new(
            Decimal128Array::from_iter((0..rows).map(|i| valid(i).then_some(i as i128 * 1001)))
                .with_precision_and_scale(*precision, *scale)
                .unwrap(),
        ),
        _ => unimplemented!("{data_type}"),
    }
}

/// Writes a file with many small data pages and returns its bytes.
fn create_page_index_file(
    data_type: &DataType,
    row_groups: usize,
    rows_per_group: usize,
) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "col",
        data_type.clone(),
        true,
    )]));
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(rows_per_group))
        .set_data_page_row_count_limit(10)
        .set_write_batch_size(10)
        .set_statistics_enabled(EnabledStatistics::Page)
        .build();
    let mut buffer = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buffer, schema.clone(), Some(props)).unwrap();
    let column = make_page_index_column(data_type, row_groups * rows_per_group);
    let batch = RecordBatch::try_new(schema, vec![column]).unwrap();
    // The page row limit is only checked between writes, so write in small slices
    for offset in (0..batch.num_rows()).step_by(10) {
        writer.write(&batch.slice(offset, 10)).unwrap();
    }
    writer.close().unwrap();
    buffer
}

/// Measures getting page statistics from the stored column index bytes by
/// building `ColumnIndexMetaData` and converting it to Arrow arrays.
fn page_index_benchmark(c: &mut Criterion) {
    let row_groups = 20;
    let data_types = [Int64, Utf8, Utf8View, Decimal128(20, 2)];
    // 10 rows per page, so 100 or 500 pages per row group: 2000 or 10000 pages
    let rows_per_group_options = [1000, 5000];

    for (data_type, rows_per_group) in data_types
        .iter()
        .flat_map(|t| rows_per_group_options.map(|rows| (t.clone(), rows)))
    {
        let data = bytes::Bytes::from(create_page_index_file(
            &data_type,
            row_groups,
            rows_per_group,
        ));
        let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::from(true));
        let reader = ArrowReaderBuilder::try_new_with_options(data.clone(), options).unwrap();
        let metadata = reader.metadata().clone();
        let converter =
            StatisticsConverter::try_new("col", reader.schema(), reader.parquet_schema()).unwrap();
        let column = converter.parquet_column_index().unwrap();
        let physical_type = reader.parquet_schema().column(column).physical_type();

        // (number of pages, stored column index bytes) for each row group
        let column_indexes: Vec<(usize, &[u8])> = metadata
            .row_groups()
            .iter()
            .enumerate()
            .map(|(rg, row_group)| {
                let range = row_group.column(column).column_index_range().unwrap();
                let num_pages = metadata
                    .page_index_for_row_group(rg)
                    .num_data_pages(column)
                    .unwrap();
                (num_pages, &data[range.start as usize..range.end as usize])
            })
            .collect();
        let row_group_indices: Vec<usize> = (0..row_groups).collect();
        let num_columns = reader.parquet_schema().num_columns();

        let mut group = c.benchmark_group(format!(
            "Decode page index statistics for {data_type} ({} pages)",
            column_indexes.iter().map(|(n, _)| n).sum::<usize>()
        ));
        group.bench_function("full page index", |b| {
            b.iter(|| {
                let mut builder = PageIndexBuilder::new(row_groups, num_columns);
                for (rg, (_, bytes)) in column_indexes.iter().enumerate() {
                    let index = decode_column_index(bytes, physical_type).unwrap();
                    builder.put_column_index(index, rg, column);
                }
                let page_index = builder.build();
                let _ = converter
                    .data_page_mins(&page_index, &row_group_indices)
                    .unwrap();
                let _ = converter
                    .data_page_maxes(&page_index, &row_group_indices)
                    .unwrap();
                let _ = converter
                    .data_page_null_counts(&page_index, &row_group_indices)
                    .unwrap();
                let _ = converter
                    .data_page_nan_counts(&page_index, &row_group_indices)
                    .unwrap();
            })
        });
        group.finish();
    }
}

criterion_group!(benches, criterion_benchmark, page_index_benchmark);
criterion_main!(benches);
