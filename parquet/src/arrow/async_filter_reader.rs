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

use crate::arrow::array_reader::{build_array_reader, RowGroupCollection};
use crate::arrow::arrow_reader::{ParquetRecordBatchReader, RowSelection};
use crate::arrow::async_reader::{make_reader, AsyncFileReader, InMemoryRowGroup};
use crate::arrow::{parquet_to_arrow_schema, ProjectionMask};

use crate::errors::{ParquetError, Result};
use crate::file::metadata::ParquetMetaData;
use crate::schema::types::SchemaDescriptor;
use arrow::array::BooleanArray;
use arrow::compute::{filter_record_batch, SlicesIterator};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::{RecordBatch, RecordBatchReader};

use futures::future::BoxFuture;
use futures::FutureExt;
use futures::Stream;
use std::cmp::Ordering;
use std::collections::VecDeque;
use std::fmt::Formatter;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

pub struct RowFilter {
    projection: ProjectionMask,
    predicate: Box<dyn Fn(&RecordBatch) -> Result<BooleanArray>>,
}

impl RowFilter {
    pub fn new<F>(projection: ProjectionMask, f: F) -> Self
    where
        F: Fn(&RecordBatch) -> Result<BooleanArray> + 'static,
    {
        Self {
            projection,
            predicate: Box::new(f),
        }
    }
}

impl std::fmt::Debug for RowFilter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RowFilter")
            .field("projection", &self.projection)
            .finish()
    }
}

pub struct FilteredParquetRecordBatchStreamBuilder<T> {
    input: T,

    metadata: Arc<ParquetMetaData>,

    schema: SchemaRef,

    batch_size: usize,

    row_groups: Option<Vec<usize>>,

    projection: ProjectionMask,

    row_filter: RowFilter,
}

impl<T: AsyncFileReader> FilteredParquetRecordBatchStreamBuilder<T> {
    /// Create a new [`ParquetRecordBatchStreamBuilder`] with the provided parquet file
    pub async fn new(mut input: T, filter: RowFilter) -> Result<Self> {
        let metadata = input.get_metadata().await?;

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
            projection: ProjectionMask::all(),
            row_filter: filter,
        })
    }

    /// Returns a reference to the [`ParquetMetaData`] for this parquet file
    pub fn metadata(&self) -> &Arc<ParquetMetaData> {
        &self.metadata
    }

    /// Returns the parquet [`SchemaDescriptor`] for this parquet file
    pub fn parquet_schema(&self) -> &SchemaDescriptor {
        self.metadata.file_metadata().schema_descr()
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
    pub fn with_projection(self, mask: ProjectionMask) -> Self {
        Self {
            projection: mask,
            ..self
        }
    }

    /// Build a new [`ParquetRecordBatchStream`]
    pub fn build(self) -> Result<FilteredParquetRecordBatchStream<T>> {
        let schema_desc = self.metadata.file_metadata().schema_descr();

        let mut selection_indices = vec![];
        let mut output_indices = vec![];

        for idx in 0..schema_desc.num_columns() {
            if self.projection.leaf_included(idx) {
                output_indices.push(idx);
                if !self.row_filter.projection.leaf_included(idx) {
                    selection_indices.push(idx);
                }
            }
        }

        let projection = ProjectionMask::leaves(schema_desc, selection_indices);
        let out_schema = Arc::new(self.schema().project(&output_indices)?);

        let num_row_groups = self.metadata.row_groups().len();

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

        Ok(FilteredParquetRecordBatchStream {
            row_groups,
            projection,
            batch_size: self.batch_size,
            metadata: self.metadata,
            file_schema: self.schema,
            schema: out_schema,
            input: Some(self.input),
            state: FilterStreamState::Init,
            row_filter: self.row_filter,
        })
    }
}

struct FilterState {
    batch: RecordBatch,
    selection: Option<VecDeque<RowSelection>>,
}

enum FilterStreamState<T> {
    Init,
    Filtering(ParquetRecordBatchReader, usize),
    Reading(
        BoxFuture<'static, Result<(T, InMemoryRowGroup)>>,
        Option<FilterState>,
        usize,
    ),
    Selecting(ParquetRecordBatchReader, RecordBatch),
    Error,
}

impl<T> std::fmt::Debug for FilterStreamState<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            FilterStreamState::Init => write!(f, "FilterStreamState::Init"),
            FilterStreamState::Filtering(_, _) => {
                write!(f, "FilterStreamState::Filtering")
            }
            FilterStreamState::Reading(_, _, _) => {
                write!(f, "FilterStreamState::Reading")
            }
            FilterStreamState::Selecting(_, _) => {
                write!(f, "FilterStreamState::Selecting")
            }
            FilterStreamState::Error => write!(f, "FilterStreamState::Error"),
        }
    }
}

pub struct FilteredParquetRecordBatchStream<T> {
    metadata: Arc<ParquetMetaData>,

    schema: SchemaRef,

    file_schema: SchemaRef,

    batch_size: usize,

    projection: ProjectionMask,

    row_groups: VecDeque<usize>,

    /// This is an option so it can be moved into a future
    input: Option<T>,

    state: FilterStreamState<T>,

    row_filter: RowFilter,
}

impl<T> std::fmt::Debug for FilteredParquetRecordBatchStream<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilteredParquetRecordBatchStream")
            .field("metadata", &self.metadata)
            .field("schema", &self.schema)
            .field("batch_size", &self.batch_size)
            .field("projection", &self.projection)
            .field("state", &self.state)
            .field("row_filter", &self.row_filter)
            .finish()
    }
}

impl<T> FilteredParquetRecordBatchStream<T>
where
    T: AsyncFileReader + Unpin + Send + 'static,
{
    /// Returns the [`SchemaRef`] for this parquet file
    pub fn schema(&self) -> &SchemaRef {
        &self.file_schema
    }
}

impl<T> Stream for FilteredParquetRecordBatchStream<T>
where
    T: AsyncFileReader + Unpin + Send + 'static,
{
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let schema = self.schema.clone();
        loop {
            match &mut self.state {
                FilterStreamState::Filtering(batch_reader, row_group_idx) => {
                    let row_group_idx = *row_group_idx;
                    let mut buffer = vec![];
                    loop {
                        match batch_reader.next() {
                            Some(Ok(batch)) => buffer.push(batch),
                            Some(Err(e)) => {
                                self.state = FilterStreamState::Error;
                                return Poll::Ready(Some(Err(ParquetError::ArrowError(
                                    e.to_string(),
                                ))));
                            }
                            None => break,
                        }
                    }

                    match RecordBatch::concat(&batch_reader.schema(), &buffer) {
                        Ok(batch) => {
                            if batch.num_rows() == 0 {
                                self.state = FilterStreamState::Init;
                            } else {
                                let input = match self.input.take() {
                                    Some(input) => input,
                                    None => {
                                        self.state = FilterStreamState::Error;
                                        return Poll::Ready(Some(Err(general_err!(
                                            "input stream lost"
                                        ))));
                                    }
                                };

                                match (self.row_filter.predicate)(&batch) {
                                    Ok(mask) => {
                                        let selection = create_selection(&mask);

                                        let filtered_batch = match filter_record_batch(
                                            &batch, &mask,
                                        ) {
                                            Ok(batch) => batch,
                                            Err(e) => {
                                                self.state = FilterStreamState::Error;
                                                return Poll::Ready(Some(Err(ParquetError::ArrowError(format!("Errpr filtering batch with mask: {:?}", e)))));
                                            }
                                        };

                                        self.state = FilterStreamState::Reading(
                                            make_reader(
                                                input,
                                                self.metadata.clone(),
                                                row_group_idx,
                                                self.projection.clone(),
                                            ),
                                            Some(FilterState {
                                                batch: filtered_batch,
                                                selection: Some(selection),
                                            }),
                                            row_group_idx,
                                        );
                                    }
                                    Err(e) => {
                                        self.state = FilterStreamState::Error;
                                        return Poll::Ready(Some(Err(e)));
                                    }
                                };
                            }
                        }
                        Err(e) => {
                            self.state = FilterStreamState::Error;
                            return Poll::Ready(Some(Err(ParquetError::ArrowError(
                                e.to_string(),
                            ))));
                        }
                    }
                }
                FilterStreamState::Selecting(batch_reader, filter_batch) => {
                    match batch_reader.next() {
                        Some(Ok(batch)) => {
                            let size = batch.num_rows();
                            match merge_batches(
                                schema,
                                &filter_batch.slice(0, size),
                                &batch,
                            ) {
                                Ok(batch) => {
                                    let remaining = filter_batch.num_rows();
                                    *filter_batch =
                                        filter_batch.slice(size, remaining - size);
                                    return Poll::Ready(Some(Ok(batch)));
                                }
                                Err(e) => {
                                    self.state = FilterStreamState::Error;
                                    return Poll::Ready(Some(Err(
                                        ParquetError::ArrowError(e.to_string()),
                                    )));
                                }
                            }
                        }
                        Some(Err(e)) => {
                            self.state = FilterStreamState::Error;
                            return Poll::Ready(Some(Err(ParquetError::ArrowError(
                                e.to_string(),
                            ))));
                        }
                        None => self.state = FilterStreamState::Init,
                    }
                }
                FilterStreamState::Init => {
                    let row_group_idx = match self.row_groups.pop_front() {
                        Some(idx) => idx,
                        None => return Poll::Ready(None),
                    };

                    let metadata = self.metadata.clone();
                    let input = match self.input.take() {
                        Some(input) => input,
                        None => {
                            self.state = FilterStreamState::Error;
                            return Poll::Ready(Some(Err(general_err!(
                                "input stream lost"
                            ))));
                        }
                    };

                    let projection = self.row_filter.projection.clone();
                    self.state = FilterStreamState::Reading(
                        make_reader(input, metadata, row_group_idx, projection),
                        None,
                        row_group_idx,
                    );
                }
                FilterStreamState::Reading(f, filter_state, row_group_idx) => {
                    let result = futures::ready!(f.poll_unpin(cx));
                    let row_group_idx = *row_group_idx;
                    let filter_state = filter_state.take();
                    self.state = FilterStreamState::Init;

                    let row_group: Box<dyn RowGroupCollection> = match result {
                        Ok((input, row_group)) => {
                            self.input = Some(input);
                            Box::new(row_group)
                        }
                        Err(e) => {
                            self.state = FilterStreamState::Error;
                            return Poll::Ready(Some(Err(e)));
                        }
                    };

                    let parquet_schema = self.metadata.file_metadata().schema_descr_ptr();

                    match filter_state {
                        Some(FilterState { batch, selection }) => {
                            let array_reader = build_array_reader(
                                parquet_schema,
                                self.file_schema.clone(),
                                self.projection.clone(),
                                row_group,
                            )?;

                            let batch_reader = ParquetRecordBatchReader::new(
                                self.batch_size,
                                array_reader,
                                selection,
                            );

                            self.state = FilterStreamState::Selecting(batch_reader, batch)
                        }
                        None => {
                            // Build reader for filter columns
                            let array_reader = build_array_reader(
                                parquet_schema,
                                self.file_schema.clone(),
                                self.row_filter.projection.clone(),
                                row_group,
                            )?;

                            let batch_reader = ParquetRecordBatchReader::new(
                                self.batch_size,
                                array_reader,
                                None,
                            );

                            self.state =
                                FilterStreamState::Filtering(batch_reader, row_group_idx)
                        }
                    }
                }
                FilterStreamState::Error => return Poll::Pending,
            }
        }
    }
}

fn merge_batches(
    schema: SchemaRef,
    filter_batch: &RecordBatch,
    selection_batch: &RecordBatch,
) -> Result<RecordBatch> {
    let mut arrays = vec![];

    for field in schema.fields() {
        if let Some((idx, _)) = filter_batch
            .schema()
            .column_with_name(field.name().as_str())
        {
            arrays.push(filter_batch.column(idx).clone());
        } else if let Some((idx, _)) = selection_batch
            .schema()
            .column_with_name(field.name().as_str())
        {
            arrays.push(selection_batch.column(idx).clone());
        }
    }

    RecordBatch::try_new(schema, arrays)
        .map_err(|e| ParquetError::ArrowError(format!("Error merging batches: {:?}", e)))
}

fn create_selection(mask: &BooleanArray) -> VecDeque<RowSelection> {
    let ranges: Vec<Range<usize>> = SlicesIterator::new(mask)
        .map(|(start, end)| start..end)
        .collect();

    let total_rows = mask.len();

    let mut selection: VecDeque<RowSelection> = VecDeque::with_capacity(ranges.len() * 2);
    let mut last_end = 0;
    for range in ranges {
        let len = range.end - range.start;

        match range.start.cmp(&last_end) {
            Ordering::Equal => match selection.pop_back() {
                Some(mut last) => last.row_count += len,
                None => selection.push_back(RowSelection::select(len)),
            },
            Ordering::Greater => {
                selection.push_back(RowSelection::skip(range.start - last_end));
                selection.push_back(RowSelection::select(len))
            }
            Ordering::Less => panic!("out of order"),
        }
        last_end = range.end;
    }

    if last_end != total_rows {
        selection.push_back(RowSelection::skip(total_rows - last_end))
    }

    selection
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::arrow::{ArrowReader, ParquetFileArrowReader};
    use arrow::array::Array;
    use arrow::error::Result as ArrowResult;
    use arrow::util::pretty::pretty_format_batches;
    use bytes::Bytes;
    use futures::TryStreamExt;
    use std::sync::Mutex;

    struct TestReader {
        data: Bytes,
        metadata: Arc<ParquetMetaData>,
        requests: Arc<Mutex<Vec<Range<usize>>>>,
    }

    impl AsyncFileReader for TestReader {
        fn get_bytes(&mut self, range: Range<usize>) -> BoxFuture<'_, Result<Bytes>> {
            self.requests.lock().unwrap().push(range.clone());
            futures::future::ready(Ok(self.data.slice(range))).boxed()
        }

        fn get_metadata(&mut self) -> BoxFuture<'_, Result<Arc<ParquetMetaData>>> {
            futures::future::ready(Ok(self.metadata.clone())).boxed()
        }
    }

    #[tokio::test]
    async fn test_async_filtered_reader_filter_none() {
        let testdata = arrow::util::test_util::parquet_test_data();
        let path = format!("{}/alltypes_plain.parquet", testdata);
        let data = Bytes::from(std::fs::read(path).unwrap());

        let metadata = crate::file::footer::parse_metadata(&data).unwrap();
        let metadata = Arc::new(metadata);

        assert_eq!(metadata.num_row_groups(), 1);

        let async_reader = TestReader {
            data: data.clone(),
            metadata: metadata.clone(),
            requests: Default::default(),
        };

        let schema_desc = metadata.file_metadata().schema_descr();

        let filter_none =
            RowFilter::new(ProjectionMask::leaves(schema_desc, vec![1]), |batch| {
                Ok(BooleanArray::from(vec![true; batch.num_rows()]))
            });

        let requests = async_reader.requests.clone();
        let builder =
            FilteredParquetRecordBatchStreamBuilder::new(async_reader, filter_none)
                .await
                .unwrap();

        let mask = ProjectionMask::leaves(builder.parquet_schema(), vec![1, 2]);
        let stream = builder
            .with_projection(mask.clone())
            .with_batch_size(1024)
            .build()
            .unwrap();

        let async_batches: Vec<_> = stream.try_collect().await.unwrap();

        println!("{}", pretty_format_batches(&async_batches).unwrap());

        let mut sync_reader = ParquetFileArrowReader::try_new(data).unwrap();
        let sync_batches = sync_reader
            .get_record_reader_by_columns(mask, 1024)
            .unwrap()
            .collect::<ArrowResult<Vec<_>>>()
            .unwrap();

        assert_eq!(async_batches, sync_batches);

        let requests = requests.lock().unwrap();
        let (offset_1, length_1) = metadata.row_group(0).column(1).byte_range();
        let (offset_2, length_2) = metadata.row_group(0).column(2).byte_range();

        assert_eq!(
            &requests[..],
            &[
                offset_1 as usize..(offset_1 + length_1) as usize,
                offset_2 as usize..(offset_2 + length_2) as usize
            ]
        );
    }

    #[tokio::test]
    async fn test_async_filtered_reader_filter_all() {
        let testdata = arrow::util::test_util::parquet_test_data();
        let path = format!("{}/alltypes_plain.parquet", testdata);
        let data = Bytes::from(std::fs::read(path).unwrap());

        let metadata = crate::file::footer::parse_metadata(&data).unwrap();
        let metadata = Arc::new(metadata);

        assert_eq!(metadata.num_row_groups(), 1);

        let async_reader = TestReader {
            data: data.clone(),
            metadata: metadata.clone(),
            requests: Default::default(),
        };

        let schema_desc = metadata.file_metadata().schema_descr();

        let filter_all =
            RowFilter::new(ProjectionMask::leaves(schema_desc, vec![1]), |batch| {
                Ok(BooleanArray::from(vec![false; batch.num_rows()]))
            });

        let builder =
            FilteredParquetRecordBatchStreamBuilder::new(async_reader, filter_all)
                .await
                .unwrap();

        let mask = ProjectionMask::leaves(builder.parquet_schema(), vec![1, 2]);
        let stream = builder
            .with_projection(mask.clone())
            .with_batch_size(1024)
            .build()
            .unwrap();

        let async_batches: Vec<_> = stream.try_collect().await.unwrap();

        assert!(async_batches.is_empty());
    }

    #[tokio::test]
    async fn test_async_filtered_reader_filter_some() {
        let testdata = arrow::util::test_util::parquet_test_data();
        let path = format!("{}/alltypes_plain.parquet", testdata);
        let data = Bytes::from(std::fs::read(path).unwrap());

        let metadata = crate::file::footer::parse_metadata(&data).unwrap();
        let metadata = Arc::new(metadata);

        assert_eq!(metadata.num_row_groups(), 1);

        let async_reader = TestReader {
            data: data.clone(),
            metadata: metadata.clone(),
            requests: Default::default(),
        };

        let schema_desc = metadata.file_metadata().schema_descr();

        let filter_all =
            RowFilter::new(ProjectionMask::leaves(schema_desc, vec![1]), |batch| {
                let mut mask = vec![];
                for idx in 0..batch.num_rows() {
                    mask.push(idx % 2 == 0);
                }

                Ok(BooleanArray::from(mask))
            });

        let requests = async_reader.requests.clone();
        let builder =
            FilteredParquetRecordBatchStreamBuilder::new(async_reader, filter_all)
                .await
                .unwrap();

        let stream = builder.with_batch_size(1024).build().unwrap();

        let schema = stream.schema().clone();

        let async_batches: Vec<_> = stream.try_collect().await.unwrap();

        let result =
            RecordBatch::concat(&schema, &async_batches).expect("concat results");

        println!("{}", pretty_format_batches(&async_batches).unwrap());

        let bool_col = result
            .column(1)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("get bool array");

        assert!(bool_col.iter().all(|b| b.expect("missing row value")));
    }
}
