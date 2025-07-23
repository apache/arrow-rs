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
use std::sync::Arc;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use std::sync::LazyLock;
use arrow::array::Int64Array;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use bytes::Bytes;
use arrow_array::{RecordBatch, StringViewArray};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;


// 1. the predicate cache is not used when there are no filters
#[test]
fn test() {
    let test = ParquetPredicateCacheTest::new()
        .with_expected_cache_used(false);
    let builder = test.sync_builder(ArrowReaderOptions::default());
    test.run(builder);
}


// Test:
// 2. the predicate cache is used when there are filters but the cache size is 0
// 3. the predicate cache is used when there are filters and the cache size is greater than 0





/// A test parquet file
struct ParquetPredicateCacheTest {
    bytes: Bytes,
    expected_cache_used: bool,
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
            expected_cache_used: false,
        }
    }

    /// Set whether the predicate cache is expected to be used
    fn with_expected_cache_used(mut self, used: bool) -> Self{
        self.expected_cache_used = used;
        self
    }

    /// Return a [`ParquetRecordBatchReaderBuilder`] for reading this file
    fn sync_builder(
        &self,
        options: ArrowReaderOptions,
    ) -> ParquetRecordBatchReaderBuilder<Bytes> {
        let reader = self.bytes.clone();
        ParquetRecordBatchReaderBuilder::try_new_with_options(reader, options)
            .expect("ParquetRecordBatchReaderBuilder")
    }


    /// Build the reader from the specified builder, reading all batches from it,
    /// and asserts the
    fn run(
        &self,
        builder: ParquetRecordBatchReaderBuilder<Bytes>,
    ) {
        let reader = builder.build().unwrap();
        for batch in reader {
            match batch {
                Ok(_) => {}
                Err(e) => panic!("Error reading batch: {e}"),
            }
        }
        // TODO check if the cache was used
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
