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

//! Scan-oriented footer metadata.
//!
//! Unlike [`ParquetMetaData`], this representation does not require metadata for every leaf
//! column in every row group. This is important for footer formats that can retrieve placement
//! metadata independently for each projected column.

#![cfg_attr(
    not(test),
    expect(dead_code, reason = "staged sparse async decoder integration")
)]

use std::sync::Arc;

use crate::errors::Result;
use crate::file::metadata::{ColumnChunkMetaData, ParquetMetaData};

/// Metadata for one column chunk selected by a scan.
#[derive(Debug, Clone)]
pub(crate) struct ScanColumnChunk {
    /// Ordinal of the row group in the source file.
    pub(crate) row_group_index: usize,
    /// Leaf-column ordinal in the source schema.
    pub(crate) column_index: usize,
    /// Metadata consumed by the existing Parquet page readers.
    pub(crate) metadata: ColumnChunkMetaData,
}

/// The subset of footer metadata needed to plan and decode a scan.
#[derive(Debug, Clone)]
pub(crate) struct ScanMetadata {
    pub(crate) row_group_num_rows: Vec<i64>,
    pub(crate) column_chunks: Vec<ScanColumnChunk>,
}

impl ScanMetadata {
    /// Returns selected chunk metadata without assuming a dense row-group layout.
    pub(crate) fn column_chunk(
        &self,
        row_group_index: usize,
        column_index: usize,
    ) -> Option<&ColumnChunkMetaData> {
        self.column_chunks
            .iter()
            .find(|chunk| {
                chunk.row_group_index == row_group_index && chunk.column_index == column_index
            })
            .map(|chunk| &chunk.metadata)
    }
}

/// A decoded footer that can produce metadata for a query projection.
pub(crate) trait ScanFooter {
    /// Materializes placement metadata only for the selected row groups and columns.
    fn scan_metadata(&self, row_groups: &[usize], columns: &[usize]) -> Result<ScanMetadata>;
}

/// Adapter for the legacy monolithic footer.
pub(crate) struct LegacyFooter {
    metadata: Arc<ParquetMetaData>,
}

impl LegacyFooter {
    pub(crate) fn new(metadata: Arc<ParquetMetaData>) -> Self {
        Self { metadata }
    }
}

impl ScanFooter for LegacyFooter {
    fn scan_metadata(&self, row_groups: &[usize], columns: &[usize]) -> Result<ScanMetadata> {
        let row_group_num_rows = self
            .metadata
            .row_groups()
            .iter()
            .map(|row_group| row_group.num_rows())
            .collect();
        let mut column_chunks = Vec::with_capacity(row_groups.len() * columns.len());
        for &column_index in columns {
            for &row_group_index in row_groups {
                let metadata = self
                    .metadata
                    .row_group(row_group_index)
                    .column(column_index);
                column_chunks.push(ScanColumnChunk {
                    row_group_index,
                    column_index,
                    metadata: metadata.clone(),
                });
            }
        }
        Ok(ScanMetadata {
            row_group_num_rows,
            column_chunks,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::basic::Type;
    use crate::file::metadata::{FileMetaData, RowGroupMetaData};
    use crate::schema::types::{SchemaDescriptor, Type as SchemaType};

    #[test]
    fn legacy_scan_metadata_is_sparse() {
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

        let scan = footer.scan_metadata(&[0], &[0]).unwrap();
        assert_eq!(scan.row_group_num_rows, vec![3]);
        assert!(scan.column_chunk(0, 0).is_some());
        assert!(scan.column_chunk(0, 1).is_none());
    }
}
