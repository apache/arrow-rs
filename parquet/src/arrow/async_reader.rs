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

//! Contains asynchronous APIs for reading parquet files into
//! arrow [`RecordBatch`]

use std::collections::VecDeque;
use std::fmt::Formatter;
use std::io::{Cursor, SeekFrom};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use byteorder::{ByteOrder, LittleEndian};
use futures::future::{BoxFuture, FutureExt};
use futures::stream::Stream;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use crate::arrow::array_reader::{build_array_reader, RowGroupCollection};
use crate::arrow::arrow_reader::ParquetRecordBatchReader;
use crate::arrow::schema::parquet_to_arrow_schema;
use crate::basic::Compression;
use crate::column::page::{PageIterator, PageReader};
use crate::errors::{ParquetError, Result};
use crate::file::footer::parse_metadata_buffer;
use crate::file::metadata::ParquetMetaData;
use crate::file::reader::SerializedPageReader;
use crate::file::PARQUET_MAGIC;
use crate::schema::types::{ColumnDescPtr, SchemaDescPtr};
use crate::util::memory::ByteBufferPtr;

/// A builder used to construct a [`ParquetRecordBatchStream`] for a parquet file
///
/// In particular, this handles reading the parquet file metadata, allowing consumers
/// to use this information to select what specific columns, row groups, etc...
/// they wish to be read by the resulting stream
///
pub struct ParquetRecordBatchStreamBuilder<T> {
    input: T,

    metadata: Arc<ParquetMetaData>,

    schema: SchemaRef,

    batch_size: usize,

    row_groups: Option<Vec<usize>>,

    projection: Option<Vec<usize>>,
}

impl<T: AsyncRead + AsyncSeek + Unpin> ParquetRecordBatchStreamBuilder<T> {
    /// Create a new [`ParquetRecordBatchStreamBuilder`] with the provided parquet file
    pub async fn new(mut input: T) -> Result<Self> {
        let metadata = Arc::new(read_footer(&mut input).await?);

        let schema = Arc::new(parquet_to_arrow_schema(
            metadata.file_metadata().schema_descr(),
            metadata.file_metadata().key_value_metadata(),
        )?);

        Ok(Self {
            input,
            metadata,
            schema,
            batch_size: 1024,
            row_groups: None,
            projection: None,
        })
    }

    /// Returns a reference to the [`ParquetMetaData`] for this parquet file
    pub fn metadata(&self) -> &Arc<ParquetMetaData> {
        &self.metadata
    }

    /// Returns the arrow [`SchemaRef`] for this parquet file
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Set the size of [`RecordBatch`] to produce
    pub fn with_batch_size(self, batch_size: usize) -> Self {
        Self { batch_size, ..self }
    }

    /// Only read data from the provided row group indexes
    pub fn with_row_groups(self, row_groups: Vec<usize>) -> Self {
        Self {
            row_groups: Some(row_groups),
            ..self
        }
    }

    /// Only read data from the provided column indexes
    pub fn with_projection(self, projection: Vec<usize>) -> Self {
        Self {
            projection: Some(projection.into()),
            ..self
        }
    }

    /// Build a new [`ParquetRecordBatchStream`]
    pub fn build(self) -> Result<ParquetRecordBatchStream<T>> {
        let num_columns = self.schema.fields().len();
        let num_row_groups = self.metadata.row_groups().len();

        let columns = match self.projection {
            Some(projection) => {
                if let Some(col) = projection.iter().find(|x| **x >= num_columns) {
                    return Err(general_err!(
                        "column projection {} outside bounds of schema 0..{}",
                        col,
                        num_columns
                    ));
                }
                projection
            }
            None => (0..num_columns).collect::<Vec<_>>(),
        };

        let row_groups = match self.row_groups {
            Some(row_groups) => {
                if let Some(col) = row_groups.iter().find(|x| **x >= num_row_groups) {
                    return Err(general_err!(
                        "row group {} out of bounds 0..{}",
                        col,
                        num_row_groups
                    ));
                }
                row_groups.into()
            }
            None => (0..self.metadata.row_groups().len()).collect(),
        };

        Ok(ParquetRecordBatchStream {
            row_groups,
            columns: columns.into(),
            batch_size: self.batch_size,
            metadata: self.metadata,
            schema: self.schema,
            input: Some(self.input),
            state: StreamState::Init,
        })
    }
}

enum StreamState<T> {
    Init,
    /// Decoding a batch
    Decoding(ParquetRecordBatchReader),
    /// Reading data from input
    Reading(BoxFuture<'static, Result<(T, InMemoryRowGroup)>>),
    /// Error
    Error,
}

impl<T> std::fmt::Debug for StreamState<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            StreamState::Init => write!(f, "StreamState::Init"),
            StreamState::Decoding(_) => write!(f, "StreamState::Decoding"),
            StreamState::Reading(_) => write!(f, "StreamState::Reading"),
            StreamState::Error => write!(f, "StreamState::Error"),
        }
    }
}

/// A [`Stream`] of [`RecordBatch`] for a parquet file
pub struct ParquetRecordBatchStream<T> {
    metadata: Arc<ParquetMetaData>,

    schema: SchemaRef,

    batch_size: usize,

    columns: Arc<[usize]>,

    row_groups: VecDeque<usize>,

    /// This is an option so it can be moved into a future
    input: Option<T>,

    state: StreamState<T>,
}

impl<T> std::fmt::Debug for ParquetRecordBatchStream<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParquetRecordBatchStream")
            .field("metadata", &self.metadata)
            .field("schema", &self.schema)
            .field("batch_size", &self.batch_size)
            .field("columns", &self.columns)
            .field("state", &self.state)
            .finish()
    }
}

impl<T> ParquetRecordBatchStream<T> {
    /// Returns the [`SchemaRef`] for this parquet file
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }
}

impl<T: AsyncRead + AsyncSeek + Unpin + Send + 'static> Stream
    for ParquetRecordBatchStream<T>
{
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            match &mut self.state {
                StreamState::Decoding(batch_reader) => match batch_reader.next() {
                    Some(Ok(batch)) => return Poll::Ready(Some(Ok(batch))),
                    Some(Err(e)) => {
                        self.state = StreamState::Error;
                        return Poll::Ready(Some(Err(ParquetError::ArrowError(
                            e.to_string(),
                        ))));
                    }
                    None => self.state = StreamState::Init,
                },
                StreamState::Init => {
                    let row_group_idx = match self.row_groups.pop_front() {
                        Some(idx) => idx,
                        None => return Poll::Ready(None),
                    };

                    let metadata = self.metadata.clone();
                    let mut input = match self.input.take() {
                        Some(input) => input,
                        None => {
                            self.state = StreamState::Error;
                            return Poll::Ready(Some(Err(general_err!(
                                "input stream lost"
                            ))));
                        }
                    };

                    let columns = Arc::clone(&self.columns);

                    self.state = StreamState::Reading(
                        async move {
                            let row_group_metadata = metadata.row_group(row_group_idx);
                            let mut column_chunks =
                                vec![None; row_group_metadata.columns().len()];

                            for column_idx in columns.iter() {
                                let column = row_group_metadata.column(*column_idx);
                                let (start, length) = column.byte_range();
                                let end = start + length;

                                input.seek(SeekFrom::Start(start)).await?;

                                let mut buffer = vec![0_u8; (end - start) as usize];
                                input.read_exact(buffer.as_mut_slice()).await?;

                                column_chunks[*column_idx] = Some(InMemoryColumnChunk {
                                    num_values: column.num_values(),
                                    compression: column.compression(),
                                    physical_type: column.column_type(),
                                    data: ByteBufferPtr::new(buffer),
                                });
                            }

                            Ok((
                                input,
                                InMemoryRowGroup {
                                    schema: metadata.file_metadata().schema_descr_ptr(),
                                    column_chunks,
                                },
                            ))
                        }
                        .boxed(),
                    )
                }
                StreamState::Reading(f) => {
                    let result = futures::ready!(f.poll_unpin(cx));
                    self.state = StreamState::Init;

                    let row_group: Box<dyn RowGroupCollection> = match result {
                        Ok((input, row_group)) => {
                            self.input = Some(input);
                            Box::new(row_group)
                        }
                        Err(e) => {
                            self.state = StreamState::Error;
                            return Poll::Ready(Some(Err(e)));
                        }
                    };

                    let parquet_schema = self.metadata.file_metadata().schema_descr_ptr();

                    let array_reader = build_array_reader(
                        parquet_schema,
                        self.schema.clone(),
                        self.columns.iter().cloned(),
                        row_group,
                    )?;

                    let batch_reader =
                        ParquetRecordBatchReader::try_new(self.batch_size, array_reader)
                            .expect("reader");

                    self.state = StreamState::Decoding(batch_reader)
                }
                StreamState::Error => return Poll::Pending,
            }
        }
    }
}

async fn read_footer<T: AsyncRead + AsyncSeek + Unpin>(
    input: &mut T,
) -> Result<ParquetMetaData> {
    input.seek(SeekFrom::End(-8)).await?;

    let mut buf = [0_u8; 8];
    input.read_exact(&mut buf).await?;

    if &buf[4..] != PARQUET_MAGIC {
        return Err(general_err!("Invalid Parquet file. Corrupt footer"));
    }

    let metadata_len = LittleEndian::read_i32(&buf[..4]) as i64;
    if metadata_len < 0 {
        return Err(general_err!(
            "Invalid Parquet file. Metadata length is less than zero ({})",
            metadata_len
        ));
    }

    input.seek(SeekFrom::End(-8 - metadata_len)).await?;

    let mut buf = Vec::with_capacity(metadata_len as usize + 8);
    input.read_to_end(&mut buf).await?;

    parse_metadata_buffer(&mut Cursor::new(buf))
}

struct InMemoryRowGroup {
    schema: SchemaDescPtr,
    column_chunks: Vec<Option<InMemoryColumnChunk>>,
}

impl RowGroupCollection for InMemoryRowGroup {
    fn schema(&self) -> Result<SchemaDescPtr> {
        Ok(self.schema.clone())
    }

    fn column_chunks(&self, i: usize) -> Result<Box<dyn PageIterator>> {
        let page_reader = self.column_chunks[i].as_ref().unwrap().pages();

        Ok(Box::new(ColumnChunkIterator {
            schema: self.schema.clone(),
            column_schema: self.schema.columns()[i].clone(),
            reader: Some(page_reader),
        }))
    }
}

#[derive(Clone)]
struct InMemoryColumnChunk {
    num_values: i64,
    compression: Compression,
    physical_type: crate::basic::Type,
    data: ByteBufferPtr,
}

impl InMemoryColumnChunk {
    fn pages(&self) -> Result<Box<dyn PageReader>> {
        let page_reader = SerializedPageReader::new(
            Cursor::new(self.data.clone()),
            self.num_values,
            self.compression,
            self.physical_type,
        )?;

        Ok(Box::new(page_reader))
    }
}

struct ColumnChunkIterator {
    schema: SchemaDescPtr,
    column_schema: ColumnDescPtr,
    reader: Option<Result<Box<dyn PageReader>>>,
}

impl Iterator for ColumnChunkIterator {
    type Item = Result<Box<dyn PageReader>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.reader.take()
    }
}

impl PageIterator for ColumnChunkIterator {
    fn schema(&mut self) -> Result<SchemaDescPtr> {
        Ok(self.schema.clone())
    }

    fn column_schema(&mut self) -> Result<ColumnDescPtr> {
        Ok(self.column_schema.clone())
    }
}

#[cfg(test)]
mod tests {
    use futures::TryStreamExt;
    use tokio::fs::File;

    use super::*;

    #[tokio::test]
    async fn test_parquet_stream() {
        let testdata = arrow::util::test_util::parquet_test_data();
        let path = format!("{}/nested_structs.rust.parquet", testdata);
        let file = File::open(path).await.unwrap();

        let stream = ParquetRecordBatchStreamBuilder::new(file)
            .await
            .unwrap()
            .build()
            .unwrap();

        let results = stream.try_collect::<Vec<_>>().await.unwrap();
        println!("{:?}", results);
    }
}
