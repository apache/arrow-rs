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

//! Provides `async` API for reading parquet files as
//! [`RecordBatch`]es
//!
//! ```
//! # #[tokio::main(flavor="current_thread")]
//! # async fn main() {
//! #
//! use arrow::record_batch::RecordBatch;
//! use arrow::util::pretty::pretty_format_batches;
//! use futures::TryStreamExt;
//! use std::fs::File;
//!
//! use parquet::arrow::ParquetRecordBatchStreamBuilder;
//!
//! # fn assert_batches_eq(batches: &[RecordBatch], expected_lines: &[&str]) {
//! #     let formatted = pretty_format_batches(batches).unwrap().to_string();
//! #     let actual_lines: Vec<_> = formatted.trim().lines().collect();
//! #     assert_eq!(
//! #          &actual_lines, expected_lines,
//! #          "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
//! #          expected_lines, actual_lines
//! #      );
//! #  }
//!
//! let testdata = arrow::util::test_util::parquet_test_data();
//! let path = format!("{}/alltypes_plain.parquet", testdata);
//! let file = File::open(path).unwrap();
//!
//! let builder = ParquetRecordBatchStreamBuilder::new(file)
//!     .await
//!     .unwrap()
//!     .with_projection(vec![1, 2, 6])
//!     .with_batch_size(3);
//!
//! let stream = builder.build().unwrap();
//!
//! let results = stream.try_collect::<Vec<_>>().await.unwrap();
//! assert_eq!(results.len(), 3);
//!
//! assert_batches_eq(
//!     &results,
//!     &[
//!         "+----------+-------------+-----------+",
//!         "| bool_col | tinyint_col | float_col |",
//!         "+----------+-------------+-----------+",
//!         "| true     | 0           | 0         |",
//!         "| false    | 1           | 1.1       |",
//!         "| true     | 0           | 0         |",
//!         "| false    | 1           | 1.1       |",
//!         "| true     | 0           | 0         |",
//!         "| false    | 1           | 1.1       |",
//!         "| true     | 0           | 0         |",
//!         "| false    | 1           | 1.1       |",
//!         "+----------+-------------+-----------+",
//!      ],
//!  );
//! # }
//! ```

use std::fmt::Formatter;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::future::FutureExt;
use futures::ready;
use futures::stream::Stream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use crate::arrow::schema::parquet_to_arrow_schema;
use crate::arrow::{ArrowReader, ParquetFileArrowReader};
use crate::errors::{ParquetError, Result};
use crate::file::metadata::ParquetMetaData;
use crate::file::reader::{ChunkReader, FileReader, SerializedFileReader};
use crate::file::serialized_reader::{ReadOptions, ReadOptionsBuilder};

/// A builder used to construct a [`ParquetRecordBatchStream`] for a parquet file
///
/// In particular, this handles reading the parquet file metadata, allowing consumers
/// to use this information to select what specific columns, row groups, etc...
/// they wish to be read by the resulting stream
///
pub struct ParquetRecordBatchStreamBuilder<T: ChunkReader> {
    batch_size: usize,

    reader: SerializedFileReader<T>,

    file_schema: SchemaRef,

    projection: Option<Vec<usize>>,
}

impl<T: ChunkReader + 'static> ParquetRecordBatchStreamBuilder<T> {
    /// Create a new [`ParquetRecordBatchStreamBuilder`] with the provided parquet file
    pub async fn new(input: T) -> Result<Self> {
        Self::new_with_options(input, ReadOptionsBuilder::new().build()).await
    }

    pub async fn new_with_options(input: T, options: ReadOptions) -> Result<Self> {
        let maybe_reader = tokio::task::spawn_blocking(move || {
            SerializedFileReader::new_with_options(input, options)
        })
        .await;

        let reader = match maybe_reader {
            Ok(maybe_reader) => maybe_reader?,
            Err(e) => {
                return Err(general_err!("error joining file creation task: {}", e))
            }
        };

        let file_schema = parquet_to_arrow_schema(
            reader.metadata().file_metadata().schema_descr(),
            reader.metadata().file_metadata().key_value_metadata(),
        )?;

        Ok(Self {
            reader,
            batch_size: 1024,
            file_schema: Arc::new(file_schema),
            projection: None,
        })
    }

    /// Returns a reference to the [`ParquetMetaData`] for this parquet file
    pub fn metadata(&self) -> &ParquetMetaData {
        self.reader.metadata()
    }

    /// Returns the arrow [`SchemaRef`] for this parquet file
    pub fn schema(&self) -> &SchemaRef {
        &self.file_schema
    }

    /// Set the size of [`RecordBatch`] to produce
    pub fn with_batch_size(self, batch_size: usize) -> Self {
        Self { batch_size, ..self }
    }

    /// Only read data from the provided column indexes
    pub fn with_projection(self, projection: Vec<usize>) -> Self {
        Self {
            projection: Some(projection),
            ..self
        }
    }

    /// Build a new [`ParquetRecordBatchStream`]
    pub fn build(self) -> Result<ParquetRecordBatchStream> {
        let (columns, schema) = match self.projection {
            Some(projection) => {
                let schema = self.file_schema.project(&projection)?;
                (projection, Arc::new(schema))
            }
            None => (
                (0..self.file_schema.fields().len()).collect::<Vec<_>>(),
                self.file_schema.clone(),
            ),
        };

        let (sender, receiver) = mpsc::channel(1);
        let handle = tokio::task::spawn_blocking(move || {
            let file_reader = Arc::new(self.reader);
            let mut arrow_reader = ParquetFileArrowReader::new(file_reader);
            let reader =
                arrow_reader.get_record_reader_by_columns(columns, self.batch_size)?;

            for result in reader {
                if let Err(_) = sender.blocking_send(result.map_err(Into::into)) {
                    // Receiver hung up
                    break;
                }
            }

            Ok(())
        });

        Ok(ParquetRecordBatchStream {
            receiver,
            schema,
            handle,
        })
    }
}

/// An asynchronous [`Stream`] of [`RecordBatch`] for a parquet file
pub struct ParquetRecordBatchStream {
    schema: SchemaRef,

    receiver: mpsc::Receiver<Result<RecordBatch>>,

    handle: JoinHandle<Result<()>>,
}

impl std::fmt::Debug for ParquetRecordBatchStream {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParquetRecordBatchStream")
            .field("schema", &self.schema)
            .finish()
    }
}

impl ParquetRecordBatchStream {
    /// Returns the [`SchemaRef`] for this parquet file
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }
}

impl Stream for ParquetRecordBatchStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match ready!(self.receiver.poll_recv(cx)) {
            Some(r) => Poll::Ready(Some(r)),
            None => match ready!(self.handle.poll_unpin(cx)) {
                Ok(r) => Poll::Ready(r.err().map(Err)),
                Err(e) => Poll::Ready(Some(Err(general_err!(
                    "error joining ParquetRecordBatchStream background task: {}",
                    e
                )))),
            },
        }
    }
}
