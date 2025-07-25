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

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringViewArray};
use arrow_select::concat::concat_batches;
use bytes::Bytes;
use parquet::arrow::arrow_reader::decoder::{DecodeResult, ParquetDecoderBuilder};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::sync::{Arc, LazyLock};

#[test]
fn test_decoder_all_data() {
    // It is possible to give the decoder all the data at once, and it will decode it without any additional requests
    let file_len = TEST_FILE_DATA.len() as u64;
    let mut decoder = ParquetDecoderBuilder::new(file_len).build().unwrap();
    decoder
        .push_data(
            vec![0..TEST_FILE_DATA.len() as u64],
            vec![TEST_FILE_DATA.clone()],
        )
        .unwrap();

    // In a loop, ask the decoder what it needs next, and provide it with the required data
    let mut results = vec![];
    loop {
        match decoder.try_decode().unwrap() {
            DecodeResult::NeedsData { ranges } => {
                panic!("Decoder should not need more data, all data was provided at once");
            }
            DecodeResult::Batch(batch) => {
                results.push(batch);
            }
            DecodeResult::Finished => {
                // The decoder has finished decoding, we can exit the loop
                break;
            }
        }
    }

    let all_output = concat_batches(&TEST_BATCH.schema(), &results).unwrap();
    // Check that the output matches the input batch
    assert_eq!(all_output, *TEST_BATCH);
}

#[test]
fn test_decoder_incremental() {
    // Create a decoder for decoding parquet data (note it does do have any IO / readers)
    let file_len = TEST_FILE_DATA.len() as u64;
    let mut decoder = ParquetDecoderBuilder::new(file_len).build().unwrap();

    let mut results = vec![];
    // In a loop, ask the decoder what it needs next, and provide it with the required data
    loop {
        match decoder.try_decode().unwrap() {
            DecodeResult::NeedsData { ranges } => {
                // decoder needs more data to decode the next batch, so provide it
                let data = ranges
                    .iter()
                    .map(|range| {
                        // Fetch the data for the given range from the in-memory test file
                        // In a real implementation, this would read from a file or network
                        let start: usize = range.start.try_into().unwrap();
                        let end: usize = range.end.try_into().unwrap();
                        TEST_FILE_DATA.slice(start..end)
                    })
                    .collect::<Vec<_>>();

                // give the decoder the data it just asked for
                decoder.push_data(ranges, data).unwrap();
            }
            DecodeResult::Batch(batch) => {
                results.push(batch);
            }
            DecodeResult::Finished => {
                // The decoder has finished decoding, we can exit the loop
                break;
            }
        }
    }

    let all_output = concat_batches(&TEST_BATCH.schema(), &results).unwrap();
    // Check that the output matches the input batch
    assert_eq!(all_output, *TEST_BATCH);
}

#[test]
fn test_decoder_provided_metadata() {
    // test providing the metadata to the decoder
    // and ensure it does not need to decode the metadata again
}

static TEST_BATCH: LazyLock<RecordBatch> = LazyLock::new(|| {
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

    RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap()
});

/// Create a parquet file in memory for testing. See [`test_file`] for details.
static TEST_FILE_DATA: LazyLock<Bytes> = LazyLock::new(|| {
    let input_batch = &TEST_BATCH;
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
