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

//! Test for predicate cache in Parquet Arrow reader

use arrow::array::ArrayRef;
use arrow::array::Int64Array;
use arrow::compute::and;
use arrow::compute::kernels::cmp::{gt, lt};
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{RecordBatch, StringViewArray};
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt};
use parquet::arrow::arrow_reader::metrics::ArrowReaderMetrics;
use parquet::arrow::arrow_reader::{ArrowPredicateFn, ArrowReaderOptions, RowFilter};
use parquet::arrow::arrow_reader::{ArrowReaderBuilder, ParquetRecordBatchReaderBuilder};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::{ArrowWriter, ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use parquet::file::properties::WriterProperties;
use std::ops::Range;
use std::sync::Arc;
use std::sync::LazyLock;

#[tokio::test]
async fn test_default_read() {
    // The cache is not used without predicates, so we expect 0 records read from cache
    let test = ParquetPredicateCacheTest::new().with_expected_records_read_from_cache(0);
    let sync_builder = test.sync_builder();
    test.run_sync(sync_builder);
    let async_builder = test.async_builder().await;
    test.run_async(async_builder).await;
}

#[tokio::test]
async fn test_async_cache_with_filters() {
    let test = ParquetPredicateCacheTest::new().with_expected_records_read_from_cache(49);
    let async_builder = test.async_builder().await;
    let async_builder = test.add_project_ab_and_filter_b(async_builder);
    test.run_async(async_builder).await;
}

#[tokio::test]
async fn test_sync_cache_with_filters() {
    let test = ParquetPredicateCacheTest::new()
        // The sync reader does not use the cache. See https://github.com/apache/arrow-rs/issues/8000
        .with_expected_records_read_from_cache(0);

    let sync_builder = test.sync_builder();
    let sync_builder = test.add_project_ab_and_filter_b(sync_builder);
    test.run_sync(sync_builder);
}

#[tokio::test]
async fn test_cache_disabled_with_filters() {
    // expect no records to be read from cache, because the cache is disabled
    let test = ParquetPredicateCacheTest::new().with_expected_records_read_from_cache(0);
    let sync_builder = test.sync_builder().with_max_predicate_cache_size(0);
    let sync_builder = test.add_project_ab_and_filter_b(sync_builder);
    test.run_sync(sync_builder);

    let async_builder = test.async_builder().await.with_max_predicate_cache_size(0);
    let async_builder = test.add_project_ab_and_filter_b(async_builder);
    test.run_async(async_builder).await;
}

// --  Begin test infrastructure --

/// A test parquet file
struct ParquetPredicateCacheTest {
    bytes: Bytes,
    expected_records_read_from_cache: usize,
}
impl ParquetPredicateCacheTest {
    /// Create a new `TestParquetFile` with:
    /// 3 columns: "a", "b", "c"
    ///
    /// 2 row groups, each with 200 rows
    /// each data page has 100 rows
    ///
    /// Values of column "a" are 0..399
    /// Values of column "b" are 400..799
    /// Values of column "c" are alternating strings of length 12 and longer
    fn new() -> Self {
        Self {
            bytes: TEST_FILE_DATA.clone(),
            expected_records_read_from_cache: 0,
        }
    }

    /// Set the expected number of records read from the cache
    fn with_expected_records_read_from_cache(
        mut self,
        expected_records_read_from_cache: usize,
    ) -> Self {
        self.expected_records_read_from_cache = expected_records_read_from_cache;
        self
    }

    /// Return a [`ParquetRecordBatchReaderBuilder`] for reading this file
    fn sync_builder(&self) -> ParquetRecordBatchReaderBuilder<Bytes> {
        let reader = self.bytes.clone();
        ParquetRecordBatchReaderBuilder::try_new_with_options(reader, ArrowReaderOptions::default())
            .expect("ParquetRecordBatchReaderBuilder")
    }

    /// Return a [`ParquetRecordBatchReaderBuilder`] for reading this file
    async fn async_builder(&self) -> ParquetRecordBatchStreamBuilder<TestReader> {
        let reader = TestReader::new(self.bytes.clone());
        ParquetRecordBatchStreamBuilder::new_with_options(reader, ArrowReaderOptions::default())
            .await
            .unwrap()
    }

    /// Return a [`ParquetRecordBatchReaderBuilder`] for reading the file with
    ///
    /// 1. a projection selecting the "a" and "b" column
    /// 2. a row_filter applied to "b": 575 < "b" < 625 (select 1 data page from each row group)
    fn add_project_ab_and_filter_b<T>(
        &self,
        builder: ArrowReaderBuilder<T>,
    ) -> ArrowReaderBuilder<T> {
        let schema_descr = builder.metadata().file_metadata().schema_descr_ptr();

        // "b" > 575 and "b" < 625
        let row_filter = ArrowPredicateFn::new(
            ProjectionMask::columns(&schema_descr, ["b"]),
            |batch: RecordBatch| {
                let scalar_575 = Int64Array::new_scalar(575);
                let scalar_625 = Int64Array::new_scalar(625);
                let column = batch.column(0).as_primitive::<Int64Type>();
                and(&gt(column, &scalar_575)?, &lt(column, &scalar_625)?)
            },
        );

        builder
            .with_projection(ProjectionMask::columns(&schema_descr, ["a", "b"]))
            .with_row_filter(RowFilter::new(vec![Box::new(row_filter)]))
    }

    /// Build the reader from the specified builder, reading all batches from it,
    /// and asserts the
    fn run_sync(&self, builder: ParquetRecordBatchReaderBuilder<Bytes>) {
        let metrics = ArrowReaderMetrics::enabled();

        let reader = builder.with_metrics(metrics.clone()).build().unwrap();
        for batch in reader {
            match batch {
                Ok(_) => {}
                Err(e) => panic!("Error reading batch: {e}"),
            }
        }
        self.verify_metrics(metrics)
    }

    /// Build the reader from the specified builder, reading all batches from it,
    /// and asserts the
    async fn run_async(&self, builder: ParquetRecordBatchStreamBuilder<TestReader>) {
        let metrics = ArrowReaderMetrics::enabled();

        let mut stream = builder.with_metrics(metrics.clone()).build().unwrap();
        while let Some(batch) = stream.next().await {
            match batch {
                Ok(_) => {}
                Err(e) => panic!("Error reading batch: {e}"),
            }
        }
        self.verify_metrics(metrics)
    }

    fn verify_metrics(&self, metrics: ArrowReaderMetrics) {
        let Self {
            bytes: _,
            expected_records_read_from_cache,
        } = self;

        let read_from_cache = metrics
            .records_read_from_cache()
            .expect("Metrics enabled, so should have metrics");

        assert_eq!(
            &read_from_cache, expected_records_read_from_cache,
                   "Expected {expected_records_read_from_cache} records read from cache, but got {read_from_cache}"
        );
    }
}

/// Create a parquet file in memory for testing. See [`test_file`] for details.
static TEST_FILE_DATA: LazyLock<Bytes> = LazyLock::new(|| {
    // Input batch has 400 rows, with 3 columns: "a", "b", "c"
    // Note c is a different types (so the data page sizes will be different)
    let a: ArrayRef = Arc::new(Int64Array::from_iter_values(0..400));
    let b: ArrayRef = Arc::new(Int64Array::from_iter_values(400..800));
    let c: ArrayRef = Arc::new(StringViewArray::from_iter_values((0..400).map(|i| {
        if i % 2 == 0 {
            format!("string_{i}")
        } else {
            format!("A string larger than 12 bytes and thus not inlined {i}")
        }
    })));

    let input_batch = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

    let mut output = Vec::new();

    let writer_options = WriterProperties::builder()
        .set_max_row_group_size(200)
        .set_data_page_row_count_limit(100)
        .build();
    let mut writer =
        ArrowWriter::try_new(&mut output, input_batch.schema(), Some(writer_options)).unwrap();

    // since the limits are only enforced on batch boundaries, write the input
    // batch in chunks of 50
    let mut row_remain = input_batch.num_rows();
    while row_remain > 0 {
        let chunk_size = row_remain.min(50);
        let chunk = input_batch.slice(input_batch.num_rows() - row_remain, chunk_size);
        writer.write(&chunk).unwrap();
        row_remain -= chunk_size;
    }
    writer.close().unwrap();
    Bytes::from(output)
});

/// Copy paste version of the `AsyncFileReader` trait for testing purposes 🤮
/// TODO put this in a common place
#[derive(Clone)]
struct TestReader {
    data: Bytes,
    metadata: Option<Arc<ParquetMetaData>>,
}

impl TestReader {
    fn new(data: Bytes) -> Self {
        Self {
            data,
            metadata: Default::default(),
        }
    }
}

impl AsyncFileReader for TestReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        let range = range.clone();
        futures::future::ready(Ok(self
            .data
            .slice(range.start as usize..range.end as usize)))
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
        let metadata_reader =
            ParquetMetaDataReader::new().with_page_indexes(options.is_some_and(|o| o.page_index()));
        self.metadata = Some(Arc::new(
            metadata_reader.parse_and_finish(&self.data).unwrap(),
        ));
        futures::future::ready(Ok(self.metadata.clone().unwrap().clone())).boxed()
    }
}
