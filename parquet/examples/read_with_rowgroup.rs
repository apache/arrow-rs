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

use arrow::util::pretty::print_batches;
use bytes::{Buf, Bytes};
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, RowGroups, RowSelection};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::{parquet_to_arrow_field_levels, ProjectionMask};
use parquet::column::page::{PageIterator, PageReader};
use parquet::errors::{ParquetError, Result};
use parquet::file::metadata::RowGroupMetaData;
use parquet::file::reader::{ChunkReader, Length};
use parquet::file::serialized_reader::SerializedPageReader;
use std::sync::Arc;
use tokio::fs::File;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let testdata = arrow::util::test_util::parquet_test_data();
    let path = format!("{testdata}/alltypes_plain.parquet");
    let mut file = File::open(&path).await.unwrap();

    // The metadata could be cached in other places, this example only shows how to read
    let metadata = file.get_metadata().await?;

    for rg in metadata.row_groups() {
        let mut rowgroup = InMemoryRowGroup::create(rg.clone(), ProjectionMask::all());
        rowgroup.async_fetch_data(&mut file, None).await?;
        let reader = rowgroup.build_reader(1024, None)?;

        for batch in reader {
            let batch = batch?;
            print_batches(&[batch])?;
        }
    }

    Ok(())
}

/// Implements [`PageIterator`] for a single column chunk, yielding a single [`PageReader`]
struct ColumnChunkIterator {
    reader: Option<Result<Box<dyn PageReader>>>,
}

impl Iterator for ColumnChunkIterator {
    type Item = Result<Box<dyn PageReader>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.reader.take()
    }
}

impl PageIterator for ColumnChunkIterator {}

/// An in-memory column chunk
#[derive(Clone)]
pub struct ColumnChunkData {
    offset: usize,
    data: Bytes,
}

impl ColumnChunkData {
    fn get(&self, start: u64) -> Result<Bytes> {
        let start = start as usize - self.offset;
        Ok(self.data.slice(start..))
    }
}

impl Length for ColumnChunkData {
    fn len(&self) -> u64 {
        self.data.len() as u64
    }
}

impl ChunkReader for ColumnChunkData {
    type T = bytes::buf::Reader<Bytes>;

    fn get_read(&self, start: u64) -> Result<Self::T> {
        Ok(self.get(start)?.reader())
    }

    fn get_bytes(&self, start: u64, length: usize) -> Result<Bytes> {
        Ok(self.get(start)?.slice(..length))
    }
}

#[derive(Clone)]
pub struct InMemoryRowGroup {
    pub metadata: RowGroupMetaData,
    mask: ProjectionMask,
    column_chunks: Vec<Option<Arc<ColumnChunkData>>>,
}

impl RowGroups for InMemoryRowGroup {
    fn num_rows(&self) -> usize {
        self.metadata.num_rows() as usize
    }

    fn column_chunks(&self, i: usize) -> Result<Box<dyn PageIterator>> {
        match &self.column_chunks[i] {
            None => Err(ParquetError::General(format!(
                "Invalid column index {i}, column was not fetched"
            ))),
            Some(data) => {
                let page_reader: Box<dyn PageReader> = Box::new(SerializedPageReader::new(
                    data.clone(),
                    self.metadata.column(i),
                    self.num_rows(),
                    None,
                )?);

                Ok(Box::new(ColumnChunkIterator {
                    reader: Some(Ok(page_reader)),
                }))
            }
        }
    }
}

impl InMemoryRowGroup {
    pub fn create(metadata: RowGroupMetaData, mask: ProjectionMask) -> Self {
        let column_chunks = metadata.columns().iter().map(|_| None).collect::<Vec<_>>();

        Self {
            metadata,
            mask,
            column_chunks,
        }
    }

    pub fn build_reader(
        &self,
        batch_size: usize,
        selection: Option<RowSelection>,
    ) -> Result<ParquetRecordBatchReader> {
        let levels = parquet_to_arrow_field_levels(
            &self.metadata.schema_descr_ptr(),
            self.mask.clone(),
            None,
        )?;

        ParquetRecordBatchReader::try_new_with_row_groups(&levels, self, batch_size, selection)
    }

    /// fetch data from a reader in sync mode
    pub async fn async_fetch_data<R: AsyncFileReader>(
        &mut self,
        reader: &mut R,
        _selection: Option<&RowSelection>,
    ) -> Result<()> {
        let mut vs = std::mem::take(&mut self.column_chunks);
        for (leaf_idx, meta) in self.metadata.columns().iter().enumerate() {
            if self.mask.leaf_included(leaf_idx) {
                let (start, len) = meta.byte_range();
                let data = reader
                    .get_bytes(start as usize..(start + len) as usize)
                    .await?;

                vs[leaf_idx] = Some(Arc::new(ColumnChunkData {
                    offset: start as usize,
                    data,
                }));
            }
        }
        self.column_chunks = std::mem::take(&mut vs);
        Ok(())
    }
}
