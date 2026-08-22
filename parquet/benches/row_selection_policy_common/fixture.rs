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

use std::ops::Range;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, DictionaryArray, FixedSizeBinaryArray, Int32Array, RecordBatch, StringArray,
    StringViewArray,
};
use arrow::datatypes::{DataType, Field, Int32Type, Schema, SchemaRef};
use bytes::Bytes;
use futures::FutureExt;
use futures::future::BoxFuture;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::basic::{Compression, Encoding, Type};
use parquet::errors::Result;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};
use parquet::file::properties::WriterProperties;
use parquet::schema::types::ColumnPath;

use super::model::{CaseSpec, PAYLOAD_COLUMNS, PAYLOAD_VALUE_MODULUS, ROWS_PER_GROUP};
use super::shapes::{expand_pattern, selected_rows};

pub(crate) const HETEROGENEOUS_STRING_WIDTH: usize = 64;
pub(crate) const HETEROGENEOUS_DICTIONARY_CARDINALITY: usize = 1_024;
pub(crate) const HETEROGENEOUS_FIXED_BINARY_WIDTH: usize = 32;

#[derive(Debug)]
pub(crate) struct CaseFixture {
    bytes: Bytes,
    metadata: Arc<ParquetMetaData>,
    pub(crate) expected_rows: usize,
}

impl CaseFixture {
    pub(crate) fn reader(&self) -> InMemoryAsyncReader {
        InMemoryAsyncReader {
            bytes: self.bytes.clone(),
            metadata: Arc::clone(&self.metadata),
        }
    }

    pub(crate) fn schema_descr(&self) -> &parquet::schema::types::SchemaDescriptor {
        self.metadata.file_metadata().schema_descr()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct InMemoryAsyncReader {
    bytes: Bytes,
    metadata: Arc<ParquetMetaData>,
}

impl AsyncFileReader for InMemoryAsyncReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes>> {
        let bytes = self.bytes.slice(range.start as usize..range.end as usize);
        async move { Ok(bytes) }.boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        _options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, Result<Arc<ParquetMetaData>>> {
        let metadata = Arc::clone(&self.metadata);
        async move { Ok(metadata) }.boxed()
    }
}

pub(crate) fn build_fixture(case: &CaseSpec) -> Result<CaseFixture> {
    assert!(
        !case.row_groups.is_empty(),
        "benchmark case must contain at least one row group"
    );

    let schema = build_schema();
    let properties = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .set_max_row_group_row_count(Some(ROWS_PER_GROUP))
        .build();

    let mut encoded = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut encoded, Arc::clone(&schema), Some(properties))?;
        for (row_group_idx, pattern) in case.row_groups.iter().copied().enumerate() {
            writer.write(&build_row_group_batch(
                Arc::clone(&schema),
                pattern,
                row_group_idx,
            )?)?;
        }
        writer.close()?;
    }

    finish_fixture(case, encoded)
}

pub(crate) fn build_heterogeneous_fixture(case: &CaseSpec) -> Result<CaseFixture> {
    assert!(
        !case.row_groups.is_empty(),
        "benchmark case must contain at least one row group"
    );

    let schema = build_heterogeneous_schema();
    let properties = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .set_column_dictionary_enabled(ColumnPath::from("payload_4"), true)
        .set_column_dictionary_enabled(ColumnPath::from("payload_5"), true)
        .set_max_row_group_row_count(Some(ROWS_PER_GROUP))
        .build();

    let mut encoded = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut encoded, Arc::clone(&schema), Some(properties))?;
        for (row_group_idx, pattern) in case.row_groups.iter().copied().enumerate() {
            writer.write(&build_heterogeneous_row_group_batch(
                Arc::clone(&schema),
                pattern,
                row_group_idx,
            )?)?;
        }
        writer.close()?;
    }

    let fixture = finish_fixture(case, encoded)?;
    validate_heterogeneous_metadata(&fixture.metadata);
    Ok(fixture)
}

fn finish_fixture(case: &CaseSpec, encoded: Vec<u8>) -> Result<CaseFixture> {
    let bytes = Bytes::from(encoded);
    let mut metadata_reader =
        ParquetMetaDataReader::new().with_page_index_policy(PageIndexPolicy::Skip);
    metadata_reader.try_parse(&bytes)?;
    let metadata = Arc::new(metadata_reader.finish()?);

    assert_eq!(
        metadata.num_row_groups(),
        case.row_groups.len(),
        "writer did not preserve the requested row-group layout"
    );
    for row_group in metadata.row_groups() {
        assert_eq!(row_group.num_rows() as usize, ROWS_PER_GROUP);
    }

    let expected_rows = case
        .row_groups
        .iter()
        .copied()
        .map(|pattern| selected_rows(pattern, ROWS_PER_GROUP))
        .sum();

    Ok(CaseFixture {
        bytes,
        metadata,
        expected_rows,
    })
}

fn build_schema() -> SchemaRef {
    let mut fields = Vec::with_capacity(PAYLOAD_COLUMNS + 1);
    fields.push(Field::new("predicate", DataType::Int32, false));
    fields.extend(
        (0..PAYLOAD_COLUMNS)
            .map(|column_idx| Field::new(format!("payload_{column_idx}"), DataType::Int32, false)),
    );
    Arc::new(Schema::new(fields))
}

fn build_heterogeneous_schema() -> SchemaRef {
    let mut fields = Vec::with_capacity(PAYLOAD_COLUMNS + 1);
    fields.push(Field::new("predicate", DataType::Int32, false));
    fields.extend(
        [
            DataType::Int32,
            DataType::Int32,
            DataType::Utf8View,
            DataType::Utf8View,
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            DataType::FixedSizeBinary(HETEROGENEOUS_FIXED_BINARY_WIDTH as i32),
            DataType::FixedSizeBinary(HETEROGENEOUS_FIXED_BINARY_WIDTH as i32),
        ]
        .into_iter()
        .enumerate()
        .map(|(column_idx, data_type)| {
            Field::new(format!("payload_{column_idx}"), data_type, false)
        }),
    );
    Arc::new(Schema::new(fields))
}

fn build_row_group_batch(
    schema: SchemaRef,
    pattern: super::model::RowGroupPattern,
    row_group_idx: usize,
) -> Result<RecordBatch> {
    let predicate = expand_pattern(pattern, ROWS_PER_GROUP);
    let mut columns = Vec::with_capacity(PAYLOAD_COLUMNS + 1);
    columns.push(Arc::new(Int32Array::from(predicate)) as ArrayRef);

    for column_idx in 0..PAYLOAD_COLUMNS {
        let values = Int32Array::from_iter_values((0..ROWS_PER_GROUP).map(|row_idx| {
            let global_row = row_group_idx * ROWS_PER_GROUP + row_idx;
            global_row
                .wrapping_add(column_idx * 17)
                .wrapping_rem(PAYLOAD_VALUE_MODULUS) as i32
        }));
        columns.push(Arc::new(values) as ArrayRef);
    }

    Ok(RecordBatch::try_new(schema, columns)?)
}

fn build_heterogeneous_row_group_batch(
    schema: SchemaRef,
    pattern: super::model::RowGroupPattern,
    row_group_idx: usize,
) -> Result<RecordBatch> {
    let predicate = expand_pattern(pattern, ROWS_PER_GROUP);
    let mut columns = Vec::with_capacity(PAYLOAD_COLUMNS + 1);
    columns.push(Arc::new(Int32Array::from(predicate)) as ArrayRef);

    for column_idx in 0..2 {
        let values = Int32Array::from_iter_values((0..ROWS_PER_GROUP).map(|row_idx| {
            heterogeneous_int32_value(column_idx, global_row(row_group_idx, row_idx))
        }));
        columns.push(Arc::new(values) as ArrayRef);
    }

    for column_idx in 2..4 {
        let values = StringViewArray::from_iter_values((0..ROWS_PER_GROUP).map(|row_idx| {
            heterogeneous_string_value(column_idx, global_row(row_group_idx, row_idx))
        }));
        columns.push(Arc::new(values) as ArrayRef);
    }

    for column_idx in 4..6 {
        let keys = Int32Array::from_iter_values((0..ROWS_PER_GROUP).map(|row_idx| {
            heterogeneous_dictionary_key(column_idx, global_row(row_group_idx, row_idx)) as i32
        }));
        let values = StringArray::from_iter_values(
            (0..HETEROGENEOUS_DICTIONARY_CARDINALITY)
                .map(|key| heterogeneous_dictionary_value(column_idx, key)),
        );
        let dictionary = DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values))?;
        columns.push(Arc::new(dictionary) as ArrayRef);
    }

    for column_idx in 6..8 {
        let values = (0..ROWS_PER_GROUP)
            .flat_map(|row_idx| {
                heterogeneous_fixed_binary_value(column_idx, global_row(row_group_idx, row_idx))
            })
            .collect::<Vec<_>>();
        let values = FixedSizeBinaryArray::try_new(
            HETEROGENEOUS_FIXED_BINARY_WIDTH as i32,
            values.into(),
            None,
        )?;
        columns.push(Arc::new(values) as ArrayRef);
    }

    Ok(RecordBatch::try_new(schema, columns)?)
}

fn validate_heterogeneous_metadata(metadata: &ParquetMetaData) {
    let expected_types = [
        Type::INT32,
        Type::INT32,
        Type::INT32,
        Type::BYTE_ARRAY,
        Type::BYTE_ARRAY,
        Type::BYTE_ARRAY,
        Type::BYTE_ARRAY,
        Type::FIXED_LEN_BYTE_ARRAY,
        Type::FIXED_LEN_BYTE_ARRAY,
    ];

    for row_group in metadata.row_groups() {
        assert_eq!(row_group.num_columns(), expected_types.len());
        for (column_idx, (column, expected_type)) in
            row_group.columns().iter().zip(expected_types).enumerate()
        {
            assert_eq!(
                column.column_type(),
                expected_type,
                "unexpected physical type for column {column_idx}"
            );

            let dictionary_encoded = column.encodings_mask().is_set(Encoding::RLE_DICTIONARY);
            assert_eq!(
                dictionary_encoded,
                matches!(column_idx, 5 | 6),
                "unexpected dictionary encoding for column {column_idx}"
            );
        }
    }
}

fn global_row(row_group_idx: usize, row_idx: usize) -> usize {
    row_group_idx * ROWS_PER_GROUP + row_idx
}

pub(crate) fn heterogeneous_int32_value(column_idx: usize, global_row: usize) -> i32 {
    global_row
        .wrapping_add(column_idx * 17)
        .wrapping_rem(PAYLOAD_VALUE_MODULUS) as i32
}

pub(crate) fn heterogeneous_string_value(column_idx: usize, global_row: usize) -> String {
    let mixed = mix64((global_row as u64) ^ ((column_idx as u64) << 48));
    let remixed = mix64(mixed ^ 0x9e37_79b9_7f4a_7c15);
    let value = format!("payload_{column_idx}:{global_row:016x}:{mixed:016x}:{remixed:016x}:end");
    debug_assert_eq!(value.len(), HETEROGENEOUS_STRING_WIDTH);
    value
}

pub(crate) fn heterogeneous_dictionary_key(column_idx: usize, global_row: usize) -> usize {
    global_row
        .wrapping_mul(31)
        .wrapping_add(column_idx * 101)
        .wrapping_rem(HETEROGENEOUS_DICTIONARY_CARDINALITY)
}

pub(crate) fn heterogeneous_dictionary_value(column_idx: usize, key: usize) -> String {
    let mixed = mix64((key as u64) ^ ((column_idx as u64) << 48));
    format!("dict_{column_idx}:{key:04x}:{mixed:016x}")
}

pub(crate) fn heterogeneous_fixed_binary_value(
    column_idx: usize,
    global_row: usize,
) -> [u8; HETEROGENEOUS_FIXED_BINARY_WIDTH] {
    let seed = (global_row as u64) ^ ((column_idx as u64) << 48);
    let mut value = [0; HETEROGENEOUS_FIXED_BINARY_WIDTH];
    for (lane, chunk) in value.chunks_exact_mut(8).enumerate() {
        let mixed = mix64(seed ^ (lane as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15));
        chunk.copy_from_slice(&mixed.to_le_bytes());
    }
    value
}

fn mix64(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
