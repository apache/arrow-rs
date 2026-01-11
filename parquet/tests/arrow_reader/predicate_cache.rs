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
use arrow::compute::kernels::cmp::{eq, gt, lt};
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{RecordBatch, StringArray, StringViewArray, StructArray};
use arrow_schema::{DataType, Field};
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt};
use parquet::arrow::arrow_reader::metrics::ArrowReaderMetrics;
use parquet::arrow::arrow_reader::{ArrowPredicateFn, ArrowReaderOptions, RowFilter};
use parquet::arrow::arrow_reader::{ArrowReaderBuilder, ParquetRecordBatchReaderBuilder};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::{ArrowWriter, ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};
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
    let async_builder = test.async_builder().await.add_project_ab_and_filter_b();
    test.run_async(async_builder).await;
}

#[tokio::test]
async fn test_sync_cache_with_filters() {
    let test = ParquetPredicateCacheTest::new()
        // The sync reader does not use the cache. See https://github.com/apache/arrow-rs/issues/8000
        .with_expected_records_read_from_cache(0);

    let sync_builder = test.sync_builder().add_project_ab_and_filter_b();
    test.run_sync(sync_builder);
}

#[tokio::test]
async fn test_cache_disabled_with_filters() {
    // expect no records to be read from cache, because the cache is disabled
    let test = ParquetPredicateCacheTest::new().with_expected_records_read_from_cache(0);
    let sync_builder = test
        .sync_builder()
        .with_max_predicate_cache_size(0)
        .add_project_ab_and_filter_b();
    test.run_sync(sync_builder);

    let async_builder = test
        .async_builder()
        .await
        .with_max_predicate_cache_size(0)
        .add_project_ab_and_filter_b();
    test.run_async(async_builder).await;
}

#[tokio::test]
async fn test_async_cache_with_nested_columns() {
    // Nested columns now work with cache - expect records from cache
    // 100 rows × 2 leaf columns (b.aa, b.bb) = 200 records
    let test = ParquetPredicateCacheTest::new_nested().with_expected_records_read_from_cache(200);
    let async_builder = test.async_builder().await.add_nested_root_filter();
    test.run_async(async_builder).await;
}

/// Test RowSelectionPolicy impact on cache reads with a struct filter that selects sparse rows.
///
/// Filter: `bb % 2 == 0 AND (id < 25 OR id > 75)` selects rows at both ends (sparse, non-contiguous)
/// Filter mask: `[id, b]` (3 leaf columns: id, b.aa, b.bb)
/// Projection: `[id, b]`
///
/// Both policies must return identical row counts and data, but differ in cache reads:
/// - Auto (Mask strategy): reads more rows due to covering the range
/// - Selectors: reads only the selected rows
#[tokio::test]
async fn test_struct_filter_sparse_selection_policy() {
    use arrow::compute::kernels::numeric::rem;
    use parquet::arrow::arrow_reader::RowSelectionPolicy;

    // Helper to create the filter: bb % 2 == 0 AND (id < 25 OR id > 75)
    fn make_filter(
        schema_descr: &std::sync::Arc<parquet::schema::types::SchemaDescriptor>,
    ) -> RowFilter {
        let filter_mask = ProjectionMask::roots(schema_descr, [0, 1]); // id and b
        let row_filter = ArrowPredicateFn::new(filter_mask, |batch: RecordBatch| {
            let id = batch.column(0).as_primitive::<Int64Type>();
            // id < 25 OR id > 75 (sparse: both ends)
            let id_sparse = arrow::compute::or(
                &lt(id, &Int64Array::new_scalar(25))?,
                &gt(id, &Int64Array::new_scalar(75))?,
            )?;

            let struct_col = batch.column(1).as_struct();
            let bb = struct_col.column_by_name("bb").unwrap();
            let bb = bb.as_primitive::<arrow_array::types::Int32Type>();
            let remainder = rem(bb, &arrow_array::Int32Array::new_scalar(2))?;
            let bb_even = eq(&remainder, &arrow_array::Int32Array::new_scalar(0))?;

            and(&id_sparse, &bb_even)
        });
        RowFilter::new(vec![Box::new(row_filter)])
    }

    // Test with Auto policy
    let (auto_rows, auto_cache) = {
        let test = ParquetPredicateCacheTest::new_nested_nullable();
        let async_builder = test.async_builder().await;
        let schema_descr = async_builder.metadata().file_metadata().schema_descr_ptr();
        let projection = ProjectionMask::roots(&schema_descr, [0, 1]);

        let async_builder = async_builder
            .with_projection(projection)
            .with_row_filter(make_filter(&schema_descr))
            .with_row_selection_policy(RowSelectionPolicy::default()); // Auto

        let metrics = ArrowReaderMetrics::enabled();
        let mut stream = async_builder.with_metrics(metrics.clone()).build().unwrap();
        let mut total_rows = 0;
        while let Some(batch) = stream.next().await {
            total_rows += batch.expect("Error").num_rows();
        }
        (total_rows, metrics.records_read_from_cache().unwrap())
    };

    // Test with Selectors policy
    let (selectors_rows, selectors_cache) = {
        let test = ParquetPredicateCacheTest::new_nested_nullable();
        let async_builder = test.async_builder().await;
        let schema_descr = async_builder.metadata().file_metadata().schema_descr_ptr();
        let projection = ProjectionMask::roots(&schema_descr, [0, 1]);

        let async_builder = async_builder
            .with_projection(projection)
            .with_row_filter(make_filter(&schema_descr))
            .with_row_selection_policy(RowSelectionPolicy::Selectors);

        let metrics = ArrowReaderMetrics::enabled();
        let mut stream = async_builder.with_metrics(metrics.clone()).build().unwrap();
        let mut total_rows = 0;
        while let Some(batch) = stream.next().await {
            total_rows += batch.expect("Error").num_rows();
        }
        (total_rows, metrics.records_read_from_cache().unwrap())
    };

    eprintln!("Sparse struct filter:");
    eprintln!("  Auto policy: rows={auto_rows}, cache={auto_cache}");
    eprintln!("  Selectors policy: rows={selectors_rows}, cache={selectors_cache}");

    // Both policies must return identical row counts
    assert_eq!(
        auto_rows, selectors_rows,
        "Both policies must return same row count"
    );

    // Selectors policy reads exactly: filtered_rows × 3 leaf columns
    assert_eq!(
        selectors_cache,
        selectors_rows * 3,
        "Selectors policy: cache reads = rows × 3 columns"
    );

    // Auto policy reads more due to Mask strategy covering the range
    assert!(
        auto_cache >= selectors_cache,
        "Auto policy should read >= Selectors policy"
    );
}

/// Test struct field projections with a filter on id (non-struct column).
///
/// Filter: `id > 50` (filter mask includes only `id`)
/// Tests various projections to verify cache behavior:
/// - Only columns in (filter_mask ∩ projection) are read from cache
/// - Since filter only uses `id`, struct columns are never cached
///
/// Schema leaf columns: 0=id, 1=b.aa, 2=b.bb
#[tokio::test]
async fn test_struct_field_projections_filter_on_id() {
    // Expected: 49 rows (id 51-99)
    const EXPECTED_ROWS: usize = 49;

    // Test cases: (projection_leaves, expected_cache_reads, description)
    let test_cases: &[(&[usize], usize, &str)] = &[
        (
            &[0, 2],
            EXPECTED_ROWS,
            "[id, b.bb]: id in filter mask → cached",
        ),
        (
            &[1, 2],
            0,
            "[b.aa, b.bb]: neither in filter mask → not cached",
        ),
        (
            &[0, 1, 2],
            EXPECTED_ROWS,
            "[id, b]: id in filter mask → cached",
        ),
        (&[2], 0, "[b.bb]: not in filter mask → not cached"),
        (&[0], EXPECTED_ROWS, "[id]: id in filter mask → cached"),
    ];

    for (projection_leaves, expected_cache, description) in test_cases {
        let test = ParquetPredicateCacheTest::new_nested_nullable();
        let async_builder = test.async_builder().await;
        let schema_descr = async_builder.metadata().file_metadata().schema_descr_ptr();

        // Filter on id only: id > 50
        let filter_mask = ProjectionMask::leaves(&schema_descr, [0]); // only id
        let row_filter = ArrowPredicateFn::new(filter_mask, |batch: RecordBatch| {
            let id = batch.column(0).as_primitive::<Int64Type>();
            gt(id, &Int64Array::new_scalar(50))
        });

        let projection = ProjectionMask::leaves(&schema_descr, projection_leaves.iter().copied());
        let async_builder = async_builder
            .with_projection(projection)
            .with_row_filter(RowFilter::new(vec![Box::new(row_filter)]));

        let metrics = ArrowReaderMetrics::enabled();
        let mut stream = async_builder.with_metrics(metrics.clone()).build().unwrap();
        let mut total_rows = 0;
        while let Some(batch) = stream.next().await {
            total_rows += batch.expect("Error").num_rows();
        }

        let cache_reads = metrics.records_read_from_cache().unwrap();
        eprintln!(
            "Filter id>50, projection {:?}: rows={total_rows}, cache={cache_reads} ({})",
            projection_leaves, description
        );

        assert_eq!(
            total_rows, EXPECTED_ROWS,
            "Expected {EXPECTED_ROWS} rows for {description}"
        );
        assert_eq!(
            cache_reads, *expected_cache,
            "Cache reads mismatch for {description}"
        );
    }
}

/// Test struct field projections with a filter on bb (struct field).
///
/// Filter: `bb > 50` (filter mask includes `b` which means both b.aa and b.bb leaves)
/// Tests various projections with both RowSelectionPolicy options.
///
/// Schema leaf columns: 0=id, 1=b.aa, 2=b.bb
/// Filter mask: roots([1]) = leaves([1, 2])
///
/// Cache reads (Selectors policy) = rows × (filter_mask ∩ projection).len()
#[tokio::test]
async fn test_struct_field_projections_filter_on_bb() {
    use parquet::arrow::arrow_reader::RowSelectionPolicy;

    // Helper to create the filter: bb > 50
    // Filter mask includes the entire struct `b` (both b.aa and b.bb)
    fn make_filter(
        schema_descr: &std::sync::Arc<parquet::schema::types::SchemaDescriptor>,
    ) -> RowFilter {
        let filter_mask = ProjectionMask::roots(schema_descr, [1]); // struct b (includes b.aa, b.bb)
        let row_filter = ArrowPredicateFn::new(filter_mask, |batch: RecordBatch| {
            let struct_col = batch.column(0).as_struct();
            let bb = struct_col.column_by_name("bb").unwrap();
            let bb = bb.as_primitive::<arrow_array::types::Int32Type>();
            gt(bb, &arrow_array::Int32Array::new_scalar(50))
        });
        RowFilter::new(vec![Box::new(row_filter)])
    }

    // Test cases: (projection_leaves, cached_leaf_count, description)
    // cached_leaf_count = number of leaves in (filter_mask ∩ projection)
    // filter_mask = leaves [1, 2] (b.aa, b.bb)
    let test_cases: &[(&[usize], usize, &str)] = &[
        (&[0, 2], 1, "[id, b.bb]: b.bb in filter mask → 1 cached"),
        (&[1, 2], 2, "[b.aa, b.bb]: both in filter mask → 2 cached"),
        (
            &[0, 1, 2],
            2,
            "[id, b]: b.aa, b.bb in filter mask → 2 cached",
        ),
        (&[2], 1, "[b.bb]: in filter mask → 1 cached"),
        (&[0], 0, "[id]: not in filter mask → 0 cached"),
    ];

    for (projection_leaves, cached_leaf_count, description) in test_cases {
        // Test with Selectors policy (exact cache reads)
        let (selectors_rows, selectors_cache) = {
            let test = ParquetPredicateCacheTest::new_nested_nullable();
            let async_builder = test.async_builder().await;
            let schema_descr = async_builder.metadata().file_metadata().schema_descr_ptr();

            let projection =
                ProjectionMask::leaves(&schema_descr, projection_leaves.iter().copied());
            let async_builder = async_builder
                .with_projection(projection)
                .with_row_filter(make_filter(&schema_descr))
                .with_row_selection_policy(RowSelectionPolicy::Selectors);

            let metrics = ArrowReaderMetrics::enabled();
            let mut stream = async_builder.with_metrics(metrics.clone()).build().unwrap();
            let mut total_rows = 0;
            while let Some(batch) = stream.next().await {
                total_rows += batch.expect("Error").num_rows();
            }
            (total_rows, metrics.records_read_from_cache().unwrap())
        };

        // Test with Auto policy
        let (auto_rows, auto_cache) = {
            let test = ParquetPredicateCacheTest::new_nested_nullable();
            let async_builder = test.async_builder().await;
            let schema_descr = async_builder.metadata().file_metadata().schema_descr_ptr();

            let projection =
                ProjectionMask::leaves(&schema_descr, projection_leaves.iter().copied());
            let async_builder = async_builder
                .with_projection(projection)
                .with_row_filter(make_filter(&schema_descr))
                .with_row_selection_policy(RowSelectionPolicy::default());

            let metrics = ArrowReaderMetrics::enabled();
            let mut stream = async_builder.with_metrics(metrics.clone()).build().unwrap();
            let mut total_rows = 0;
            while let Some(batch) = stream.next().await {
                total_rows += batch.expect("Error").num_rows();
            }
            (total_rows, metrics.records_read_from_cache().unwrap())
        };

        eprintln!(
            "Filter bb>50, projection {:?}: Selectors(rows={}, cache={}), Auto(rows={}, cache={}) ({})",
            projection_leaves, selectors_rows, selectors_cache, auto_rows, auto_cache, description
        );

        // Both policies must return identical row counts
        assert_eq!(
            selectors_rows, auto_rows,
            "Row count mismatch between policies for {description}"
        );

        // Selectors policy: exact cache reads = rows × cached_leaf_count
        let expected_selectors_cache = selectors_rows * cached_leaf_count;
        assert_eq!(
            selectors_cache, expected_selectors_cache,
            "Selectors cache mismatch for {description}: expected {expected_selectors_cache}"
        );

        // Auto policy reads >= Selectors due to Mask strategy
        assert!(
            auto_cache >= selectors_cache,
            "Auto should read >= Selectors for {description}"
        );
    }
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

    /// Create a new `TestParquetFile` with
    /// 2 columns:
    ///
    /// * string column `a`
    /// * nested struct column `b { aa, bb }`
    fn new_nested() -> Self {
        Self {
            bytes: NESTED_TEST_FILE_DATA.clone(),
            expected_records_read_from_cache: 0,
        }
    }

    /// Create a new test file with nullable nested struct.
    ///
    /// See [`NESTED_NULLABLE_TEST_FILE_DATA`] for data details.
    fn new_nested_nullable() -> Self {
        Self {
            bytes: NESTED_NULLABLE_TEST_FILE_DATA.clone(),
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

/// Build a ParquetFile with nullable nested struct for testing row filters with definition levels.
///
/// Schema:
/// * `id: Int64`
/// * `b: Struct { aa: String (nullable), bb: Int32 (nullable) }` (nullable struct)
///
/// Null patterns:
/// - Every 3rd row (i % 3 == 0): aa is null
/// - Every 5th row (i % 5 == 0): bb is null
/// - Every 7th row (i % 7 == 0): entire struct is null (overrides field nulls)
///
/// The non-null values are:
/// - aa = "v{i}"
/// - bb = i as i32
///
/// Where i is the row index from 0 to 99
static NESTED_NULLABLE_TEST_FILE_DATA: LazyLock<Bytes> = LazyLock::new(|| {
    use arrow_array::Int32Array;
    use arrow_buffer::NullBuffer;
    use arrow_schema::Fields;

    const NUM_ROWS: usize = 100;

    // id column
    let id: Int64Array = (0..NUM_ROWS as i64).collect();

    // aa: String column - null every 3rd row
    let aa: StringArray = (0..NUM_ROWS)
        .map(|i| {
            if i % 3 == 0 {
                None
            } else {
                Some(format!("v{i}"))
            }
        })
        .collect();

    // bb: Int32 column - null every 5th row
    let bb: Int32Array = (0..NUM_ROWS)
        .map(|i| if i % 5 == 0 { None } else { Some(i as i32) })
        .collect();

    // Struct null buffer - null every 7th row
    let struct_nulls: Vec<bool> = (0..NUM_ROWS).map(|i| i % 7 != 0).collect();
    let struct_null_buffer = NullBuffer::from(struct_nulls);

    // Create struct with null buffer
    let b = StructArray::new(
        Fields::from(vec![
            Field::new("aa", DataType::Utf8, true),
            Field::new("bb", DataType::Int32, true),
        ]),
        vec![Arc::new(aa) as ArrayRef, Arc::new(bb) as ArrayRef],
        Some(struct_null_buffer),
    );

    let input_batch = RecordBatch::try_from_iter([
        ("id", Arc::new(id) as ArrayRef),
        ("b", Arc::new(b) as ArrayRef),
    ])
    .unwrap();

    let mut output = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut output, input_batch.schema(), None).unwrap();
    writer.write(&input_batch).unwrap();
    writer.close().unwrap();
    Bytes::from(output)
});

/// Build a ParquetFile with a
///
/// * string column `a`
/// * nested struct column `b { aa, bb }`
static NESTED_TEST_FILE_DATA: LazyLock<Bytes> = LazyLock::new(|| {
    const NUM_ROWS: usize = 100;
    let a: StringArray = (0..NUM_ROWS).map(|i| Some(format!("r{i}"))).collect();

    let aa: StringArray = (0..NUM_ROWS).map(|i| Some(format!("v{i}"))).collect();
    let bb: StringArray = (0..NUM_ROWS).map(|i| Some(format!("w{i}"))).collect();
    let b = StructArray::from(vec![
        (
            Arc::new(Field::new("aa", DataType::Utf8, true)),
            Arc::new(aa) as ArrayRef,
        ),
        (
            Arc::new(Field::new("bb", DataType::Utf8, true)),
            Arc::new(bb) as ArrayRef,
        ),
    ]);

    let input_batch = RecordBatch::try_from_iter([
        ("a", Arc::new(a) as ArrayRef),
        ("b", Arc::new(b) as ArrayRef),
    ])
    .unwrap();

    let mut output = Vec::new();
    let writer_options = None;
    let mut writer =
        ArrowWriter::try_new(&mut output, input_batch.schema(), writer_options).unwrap();
    writer.write(&input_batch).unwrap();
    writer.close().unwrap();
    Bytes::from(output)
});

trait ArrowReaderBuilderExt {
    /// Applies the following:
    /// 1. a projection selecting the "a" and "b" column
    /// 2. a row_filter applied to "b": 575 < "b" < 625 (select 1 data page from each row group)
    fn add_project_ab_and_filter_b(self) -> Self;

    /// Adds a row filter that projects the nested ROOT column "b" and
    /// returns true for all rows.
    fn add_nested_root_filter(self) -> Self;
}

impl<T> ArrowReaderBuilderExt for ArrowReaderBuilder<T> {
    fn add_project_ab_and_filter_b(self) -> Self {
        let schema_descr = self.metadata().file_metadata().schema_descr_ptr();

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

        self.with_projection(ProjectionMask::columns(&schema_descr, ["a", "b"]))
            .with_row_filter(RowFilter::new(vec![Box::new(row_filter)]))
    }

    fn add_nested_root_filter(self) -> Self {
        let schema_descr = self.metadata().file_metadata().schema_descr_ptr();

        // Project the ROOT struct column "b", not just leaf "b.aa"
        let root_mask = ProjectionMask::roots(&schema_descr, [1]); // column index 1 = "b"

        let always_true = ArrowPredicateFn::new(root_mask.clone(), |batch: RecordBatch| {
            Ok(arrow_array::BooleanArray::from(vec![
                true;
                batch.num_rows()
            ]))
        });
        let row_filter = RowFilter::new(vec![Box::new(always_true)]);

        self.with_projection(root_mask).with_row_filter(row_filter)
    }
}

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
        let metadata_reader = ParquetMetaDataReader::new().with_page_index_policy(
            PageIndexPolicy::from(options.is_some_and(|o| o.page_index())),
        );
        self.metadata = Some(Arc::new(
            metadata_reader.parse_and_finish(&self.data).unwrap(),
        ));
        futures::future::ready(Ok(self.metadata.clone().unwrap().clone())).boxed()
    }
}
