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

use arrow::array::{ArrayRef, Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use bytes::Bytes;
use futures::FutureExt;
use futures::future::BoxFuture;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::basic::Compression;
use parquet::errors::Result;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};
use parquet::file::properties::WriterProperties;

use super::model::{CaseSpec, PAYLOAD_COLUMNS, PAYLOAD_VALUE_MODULUS, ROWS_PER_GROUP};
use super::shapes::{expand_pattern, selected_rows};

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
