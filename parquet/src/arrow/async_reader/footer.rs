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

//! Query-oriented footer metadata used by the async reader.

use std::sync::Arc;

use futures::future::BoxFuture;

use crate::arrow::async_reader::MetadataFetch;
use crate::errors::{ParquetError, Result};
use crate::file::metadata::{
    ColumnChunkMetaData, FileMetaData, KeyValue, ParquetMetaData, RowGroupMetaData,
};
use crate::schema::types::{ColumnDescPtr, SchemaDescriptor, Type as SchemaType};

/// The metadata contract between a footer decoder and the existing page decoder.
///
/// A legacy implementation can decode eagerly, while a modular implementation can retain
/// independently addressable modules and decode only chunks requested by the query.
pub(crate) trait ScanFooter {
    fn version(&self) -> i32;
    fn num_rows(&self) -> i64;
    fn schema(&self) -> &SchemaDescriptor;
    fn row_group_num_rows(&self) -> &[i64];
    fn created_by(&self) -> Option<&str>;
    fn key_value_metadata(&self) -> Option<&[KeyValue]>;

    fn column_chunk(
        &self,
        row_group_index: usize,
        column_index: usize,
        projected_column: ColumnDescPtr,
    ) -> Result<ColumnChunkMetaData>;
}

/// Asynchronously loads a physical footer while retaining state for later optional fetches.
pub(crate) trait AsyncFooterLoader {
    type Footer: ScanFooter;

    fn load_footer<'a, F>(
        &'a self,
        fetch: &'a mut F,
        file_size: u64,
    ) -> BoxFuture<'a, Result<Self::Footer>>
    where
        F: MetadataFetch + Send + 'a;
}

/// Builds the projected metadata expected by the existing page decoder.
///
/// This is dense only within the query projection; unprojected chunks have no entries.
pub(crate) fn projected_parquet_metadata<F: ScanFooter>(
    footer: &F,
    columns: &[usize],
) -> Result<ParquetMetaData> {
    if columns.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(general_err!("projected columns must be sorted and unique"));
    }
    if columns
        .last()
        .is_some_and(|&column| column >= footer.schema().num_columns())
    {
        return Err(general_err!("projected column is out of range"));
    }
    let mut leaf_index = 0;
    let root = project_type(footer.schema().root_schema(), columns, &mut leaf_index)?
        .ok_or_else(|| general_err!("footer projection selects no columns"))?;
    let schema = Arc::new(SchemaDescriptor::new(root));
    let mut row_groups = Vec::with_capacity(footer.row_group_num_rows().len());
    for (row_group_index, &num_rows) in footer.row_group_num_rows().iter().enumerate() {
        let mut chunks = Vec::with_capacity(columns.len());
        for (projected_index, &column_index) in columns.iter().enumerate() {
            chunks.push(footer.column_chunk(
                row_group_index,
                column_index,
                schema.column(projected_index),
            )?);
        }
        let total_byte_size = chunks.iter().map(|chunk| chunk.uncompressed_size()).sum();
        row_groups.push(
            RowGroupMetaData::builder(Arc::clone(&schema))
                .set_num_rows(num_rows)
                .set_total_byte_size(total_byte_size)
                .set_column_metadata(chunks)
                .set_ordinal(row_group_index as i32)
                .build()?,
        );
    }
    let file = FileMetaData::new(
        footer.version(),
        footer.num_rows(),
        footer.created_by().map(str::to_owned),
        footer.key_value_metadata().map(|metadata| {
            metadata
                .iter()
                // The embedded schema describes the unprojected file and cannot be attached to
                // projected metadata. Parquet logical annotations still preserve scan semantics.
                .filter(|entry| entry.key != crate::arrow::ARROW_SCHEMA_META_KEY)
                .cloned()
                .collect()
        }),
        schema,
        None,
    );
    Ok(ParquetMetaData::new(file, row_groups))
}

fn project_type(
    field: &SchemaType,
    columns: &[usize],
    leaf_index: &mut usize,
) -> Result<Option<Arc<SchemaType>>> {
    if field.is_primitive() {
        let selected = columns.binary_search(leaf_index).is_ok();
        *leaf_index += 1;
        return Ok(selected.then(|| Arc::new(field.clone())));
    }
    let mut children = Vec::new();
    for child in field.get_fields() {
        if let Some(child) = project_type(child, columns, leaf_index)? {
            children.push(child);
        }
    }
    if children.is_empty() {
        return Ok(None);
    }
    let info = field.get_basic_info();
    let mut builder = SchemaType::group_type_builder(info.name())
        .with_fields(children)
        .with_converted_type(info.converted_type())
        .with_logical_type(info.logical_type_ref().cloned())
        .with_id(info.has_id().then(|| info.id()));
    if info.has_repetition() {
        builder = builder.with_repetition(info.repetition());
    }
    Ok(Some(Arc::new(builder.build()?)))
}

/// Adapter preserving the existing eager legacy-footer behavior.
#[cfg_attr(not(test), expect(dead_code, reason = "legacy adapter migration seam"))]
pub(crate) struct LegacyFooter {
    metadata: Arc<ParquetMetaData>,
    row_group_num_rows: Vec<i64>,
}

impl LegacyFooter {
    #[cfg_attr(not(test), expect(dead_code, reason = "legacy adapter migration seam"))]
    pub(crate) fn new(metadata: Arc<ParquetMetaData>) -> Self {
        let row_group_num_rows = metadata
            .row_groups()
            .iter()
            .map(|row_group| row_group.num_rows())
            .collect();
        Self {
            metadata,
            row_group_num_rows,
        }
    }
}

impl ScanFooter for LegacyFooter {
    fn version(&self) -> i32 {
        self.metadata.file_metadata().version()
    }
    fn num_rows(&self) -> i64 {
        self.metadata.file_metadata().num_rows()
    }
    fn schema(&self) -> &SchemaDescriptor {
        self.metadata.file_metadata().schema_descr()
    }
    fn row_group_num_rows(&self) -> &[i64] {
        &self.row_group_num_rows
    }
    fn created_by(&self) -> Option<&str> {
        self.metadata.file_metadata().created_by()
    }
    fn key_value_metadata(&self) -> Option<&[KeyValue]> {
        self.metadata
            .file_metadata()
            .key_value_metadata()
            .map(Vec::as_slice)
    }
    fn column_chunk(
        &self,
        row_group_index: usize,
        column_index: usize,
        _projected_column: ColumnDescPtr,
    ) -> Result<ColumnChunkMetaData> {
        Ok(self
            .metadata
            .row_group(row_group_index)
            .column(column_index)
            .clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::basic::Type;

    #[test]
    fn legacy_footer_uses_projection_contract() {
        let field = SchemaType::primitive_type_builder("a", Type::INT32)
            .build()
            .unwrap();
        let schema = Arc::new(SchemaDescriptor::new(Arc::new(
            SchemaType::group_type_builder("schema")
                .with_fields(vec![Arc::new(field)])
                .build()
                .unwrap(),
        )));
        let column = ColumnChunkMetaData::builder(schema.column(0))
            .build()
            .unwrap();
        let row_group = RowGroupMetaData::builder(Arc::clone(&schema))
            .set_num_rows(3)
            .set_column_metadata(vec![column])
            .build()
            .unwrap();
        let file = FileMetaData::new(1, 3, None, None, schema, None);
        let footer = LegacyFooter::new(Arc::new(ParquetMetaData::new(file, vec![row_group])));

        let projected = projected_parquet_metadata(&footer, &[0]).unwrap();
        assert_eq!(projected.row_group(0).num_columns(), 1);
        assert_eq!(projected.row_group(0).num_rows(), 3);
    }
}
