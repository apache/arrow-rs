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

use crate::errors::ParquetError;
use crate::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};
use crate::DecodeResult;

/// A push decoder for [`ParquetMetaData`].
///
/// This structure implements a push API based version of the [`ParquetMetaDataReader`], which
/// decouples the IO from the metadata decoding logic.
///
/// You can use this decoder to customize your IO operations, as shown in the
/// examples below for minimizing bytes read, prefetching data, or
/// using async IO.
///
/// # Example
///
/// The most basic usage is to feed the decoder with the necessary byte ranges
/// as requested as shown below.
///
/// ```rust
/// # use std::ops::Range;
/// # use bytes::Bytes;
/// # use arrow_array::record_batch;
/// # use parquet::DecodeResult;
/// # use parquet::arrow::ArrowWriter;
/// # use parquet::errors::ParquetError;
/// # use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataPushDecoder};
/// #
/// # fn decode_metadata() -> Result<ParquetMetaData, ParquetError> {
/// # let file_bytes = {
/// #   let mut buffer = vec![0];
/// #   let batch = record_batch!(("a", Int32, [1, 2, 3])).unwrap();
/// #   let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), None).unwrap();
/// #   writer.write(&batch).unwrap();
/// #   writer.close().unwrap();
/// #   Bytes::from(buffer)
/// # };
/// # // mimic IO by returning a function that returns the bytes for a given range
/// # let get_range = |range: &Range<u64>| -> Bytes {
/// #    let start = range.start as usize;
/// #     let end = range.end as usize;
/// #    file_bytes.slice(start..end)
/// # };
/// #
/// # let file_len = file_bytes.len() as u64;
/// // The `ParquetMetaDataPushDecoder` needs to know the file length.
/// let mut decoder = ParquetMetaDataPushDecoder::try_new(file_len).unwrap();
/// // try to decode the metadata. If more data is needed, the decoder will tell you what ranges
/// loop {
///     match decoder.try_decode() {
///        Ok(DecodeResult::Data(metadata)) => { return Ok(metadata); } // decode successful
///        Ok(DecodeResult::NeedsData(ranges)) => {
///           // The decoder needs more data
///           //
///           // In this example, we call a function that returns the bytes for each given range.
///           // In a real application, you would likely read the data from a file or network.
///           let data = ranges.iter().map(|range| get_range(range)).collect();
///           // Push the data into the decoder and try to decode again on the next iteration.
///           decoder.push_ranges(ranges, data).unwrap();
///        }
///        Ok(DecodeResult::Finished) => { unreachable!("returned metadata in previous match arm") }
///        Err(e) => return Err(e),
///     }
/// }
/// # }
/// ```
///
/// # Example with "prefetching"
///
/// By default, the [`ParquetMetaDataPushDecoder`] will request only the exact byte
/// ranges it needs. This minimizes the number of bytes read, however it
/// requires at least two IO operations to read the metadata - one to read the
/// footer and then one to read the metadata.
///
/// If the file has a "Page Index" (see [Self::with_page_index_policy]), three
/// IO operations are required to read the metadata, as the page index is
/// not part of the normal metadata footer.
///
/// To reduce the number of IO operations in systems with high per operation
/// overhead (e.g. cloud storage), you can "prefetch" the data and then push
/// the data into the decoder before calling [`Self::try_decode`]. If you do
/// not push enough bytes, the decoder will return the ranges that are still
/// needed.
///
/// This approach can also be used when you have the entire file already in memory
/// for other reasons.
///
/// ```rust
/// # use std::ops::Range;
/// # use bytes::Bytes;
/// # use arrow_array::record_batch;
/// # use parquet::DecodeResult;
/// # use parquet::arrow::ArrowWriter;
/// # use parquet::errors::ParquetError;
/// # use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataPushDecoder};
/// #
/// # fn decode_metadata() -> Result<ParquetMetaData, ParquetError> {
/// # let file_bytes = {
/// #   let mut buffer = vec![0];
/// #   let batch = record_batch!(("a", Int32, [1, 2, 3])).unwrap();
/// #   let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), None).unwrap();
/// #   writer.write(&batch).unwrap();
/// #   writer.close().unwrap();
/// #   Bytes::from(buffer)
/// # };
/// #
/// let file_len = file_bytes.len() as u64;
/// // For this example, we "prefetch" all the bytes which we have in memory,
/// // but in a real application, you would likely read a chunk from the end
/// // for example 1MB.
/// let prefetched_bytes = file_bytes.clone();
/// let mut decoder = ParquetMetaDataPushDecoder::try_new(file_len).unwrap();
/// // push the prefetched bytes into the decoder
/// decoder.push_ranges(vec![0..file_len], vec![prefetched_bytes]).unwrap();
/// // The decoder will now be able to decode the metadata. Note in a real application,
/// // unless you can guarantee that the pushed data is enough to decode the metadata,
/// // you still need to call `try_decode` in a loop until it returns `DecodeResult::Data`
/// // as shown in  the previous example
///  match decoder.try_decode() {
///      Ok(DecodeResult::Data(metadata)) => { return Ok(metadata); } // decode successful
///      other => { panic!("expected DecodeResult::Data, got: {other:?}") }
///  }
/// # }
/// ```
///
/// # Example using [`AsyncRead`]
///
/// [`ParquetMetaDataPushDecoder`] is designed to work with any data source that can
/// provide byte ranges, including async IO sources. However, it does not
/// implement async IO itself. To use async IO, you simply write an async
/// wrapper around it that reads the required byte ranges and pushes them into the
/// decoder.
///
/// ```rust
/// # use std::ops::Range;
/// # use bytes::Bytes;
/// use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt};
/// # use arrow_array::record_batch;
/// # use parquet::DecodeResult;
/// # use parquet::arrow::ArrowWriter;
/// # use parquet::errors::ParquetError;
/// # use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataPushDecoder};
/// #
/// // This function decodes Parquet Metadata from anything that implements
/// // [`AsyncRead`] and [`AsyncSeek`] such as a tokio::fs::File
/// async fn decode_metadata(
///   file_len: u64,
///   mut async_source: impl AsyncRead + AsyncSeek + Unpin
/// ) -> Result<ParquetMetaData, ParquetError> {
///   // We need a ParquetMetaDataPushDecoder to decode the metadata.
///   let mut decoder = ParquetMetaDataPushDecoder::try_new(file_len).unwrap();
///   loop {
///     match decoder.try_decode() {
///        Ok(DecodeResult::Data(metadata)) => { return Ok(metadata); } // decode successful
///        Ok(DecodeResult::NeedsData(ranges)) => {
///           // The decoder needs more data
///           //
///           // In this example we use the AsyncRead and AsyncSeek traits to read the
///           // required ranges from the async source.
///           let mut data = Vec::with_capacity(ranges.len());
///           for range in &ranges {
///             let mut buffer = vec![0; (range.end - range.start) as usize];
///             async_source.seek(std::io::SeekFrom::Start(range.start)).await?;
///             async_source.read_exact(&mut buffer).await?;
///             data.push(Bytes::from(buffer));
///           }
///           // Push the data into the decoder and try to decode again on the next iteration.
///           decoder.push_ranges(ranges, data).unwrap();
///        }
///        Ok(DecodeResult::Finished) => { unreachable!("returned metadata in previous match arm") }
///        Err(e) => return Err(e),
///     }
///   }
/// }
/// ```
/// [`AsyncRead`]: tokio::io::AsyncRead
#[derive(Debug)]
pub struct ParquetMetaDataPushDecoder {
    done: bool,
    metadata_reader: ParquetMetaDataReader,
    buffers: crate::util::push_buffers::PushBuffers,
}

impl ParquetMetaDataPushDecoder {
    /// Create a new `ParquetMetaDataPushDecoder` with the given file length.
    ///
    /// By default, this will read page indexes and column indexes. See
    /// [`ParquetMetaDataPushDecoder::with_page_index_policy`] for more detail.
    ///
    /// See examples on [`ParquetMetaDataPushDecoder`].
    pub fn try_new(file_len: u64) -> Result<Self, ParquetError> {
        if file_len < 8 {
            return Err(ParquetError::General(format!(
                "Parquet files are at least 8 bytes long, but file length is {file_len}"
            )));
        };

        let metadata_reader =
            ParquetMetaDataReader::new().with_page_index_policy(PageIndexPolicy::Optional);

        Ok(Self {
            done: false,
            metadata_reader,
            buffers: crate::util::push_buffers::PushBuffers::new(file_len),
        })
    }

    /// Enable or disable reading the page index structures described in
    /// "[Parquet page index] Layout to Support Page Skipping".
    ///
    /// Defaults to [`PageIndexPolicy::Optional`]
    ///
    /// This requires
    /// 1. The Parquet file to have been written with page indexes
    /// 2. Additional data to be pushed into the decoder (as the page indexes are not part of the thrift footer)
    ///
    /// [Parquet page index]: https://github.com/apache/parquet-format/blob/master/PageIndex.md
    pub fn with_page_index_policy(mut self, page_index_policy: PageIndexPolicy) -> Self {
        self.metadata_reader = self
            .metadata_reader
            .with_page_index_policy(page_index_policy);
        self
    }

    /// Push the data into the decoder's buffer.
    ///
    /// The decoder does not immediately attempt to decode the metadata
    /// after pushing data. Instead, it accumulates the pushed data until you
    /// call [`Self::try_decode`].
    ///
    /// # Determining required data:
    ///
    /// To determine what ranges are required to decode the metadata, you can
    /// either:
    ///
    /// 1. Call [`Self::try_decode`] first to get the exact ranges required (see
    ///    example on [`Self`])
    ///
    /// 2. Speculatively push any data that you have available, which may
    ///    include more than the footer data or requested bytes.
    ///
    /// Speculatively pushing data can be used when  "prefetching" data. See
    /// example on [`Self`]
    pub fn push_ranges(
        &mut self,
        ranges: Vec<std::ops::Range<u64>>,
        buffers: Vec<bytes::Bytes>,
    ) -> std::result::Result<(), String> {
        if self.done {
            return Err(
                "ParquetMetaDataPushDecoder: cannot push data after decoding is finished"
                    .to_string(),
            );
        }
        self.buffers.push_ranges(ranges, buffers);
        Ok(())
    }

    /// Try to decode the metadata from the pushed data, returning the
    /// decoded metadata or an error if not enough data is available.
    pub fn try_decode(
        &mut self,
    ) -> std::result::Result<DecodeResult<ParquetMetaData>, ParquetError> {
        if self.done {
            return Ok(DecodeResult::Finished);
        }

        // need to have the last 8 bytes of the file to decode the metadata
        let file_len = self.buffers.file_len();
        if !self.buffers.has_range(&(file_len - 8..file_len)) {
            #[expect(clippy::single_range_in_vec_init)]
            return Ok(DecodeResult::NeedsData(vec![file_len - 8..file_len]));
        }

        // Try to parse the metadata from the buffers we have.
        //
        // If we don't have enough data, returns a `ParquetError::NeedMoreData`
        // with the number of bytes needed to complete the metadata parsing.
        //
        // If we have enough data, returns `Ok(())` and we can complete
        // the metadata parsing.
        let maybe_metadata = self
            .metadata_reader
            .try_parse_sized(&self.buffers, self.buffers.file_len());

        match maybe_metadata {
            Ok(()) => {
                // Metadata successfully parsed, proceed to decode the row groups
                let metadata = self.metadata_reader.finish()?;
                self.done = true;
                Ok(DecodeResult::Data(metadata))
            }

            Err(ParquetError::NeedMoreData(needed)) => {
                let needed = needed as u64;
                let Some(start_offset) = file_len.checked_sub(needed) else {
                    return Err(ParquetError::General(format!(
                        "Parquet metadata reader needs at least {needed} bytes, but file length is only {file_len}"
                    )));
                };
                let needed_range = start_offset..start_offset + needed;
                // needs `needed_range` bytes at the end of the file
                Ok(DecodeResult::NeedsData(vec![needed_range]))
            }
            Err(ParquetError::NeedMoreDataRange(range)) => Ok(DecodeResult::NeedsData(vec![range])),

            Err(e) => Err(e), // some other error, pass back
        }
    }
}

// These tests use the arrow writer to create a parquet file in memory
// so they need the arrow feature and the test feature
#[cfg(all(test, feature = "arrow"))]
mod tests {
    use super::*;
    use crate::arrow::ArrowWriter;
    use crate::file::properties::WriterProperties;
    use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringViewArray};
    use bytes::Bytes;
    use std::fmt::Debug;
    use std::ops::Range;
    use std::sync::{Arc, LazyLock};

    /// It is possible to decode the metadata from the entire file at once before being asked
    #[test]
    fn test_metadata_decoder_all_data() {
        let file_len = test_file_len();
        let mut metadata_decoder = ParquetMetaDataPushDecoder::try_new(file_len).unwrap();
        // Push the entire file data into the metadata decoder
        push_ranges_to_metadata_decoder(&mut metadata_decoder, vec![test_file_range()]);

        // should be able to decode the metadata without needing more data
        let metadata = expect_data(metadata_decoder.try_decode());

        assert_eq!(metadata.num_row_groups(), 2);
        assert_eq!(metadata.row_group(0).num_rows(), 200);
        assert_eq!(metadata.row_group(1).num_rows(), 200);
        assert!(metadata.column_index().is_some());
        assert!(metadata.offset_index().is_some());
    }

    /// It is possible to feed some, but not all, of the footer into the metadata decoder
    /// before asked. This avoids multiple IO requests
    #[test]
    fn test_metadata_decoder_prefetch_success() {
        let file_len = test_file_len();
        let mut metadata_decoder = ParquetMetaDataPushDecoder::try_new(file_len).unwrap();
        // simulate pre-fetching the last 2k bytes of the file without asking the decoder
        let prefetch_range = (file_len - 2 * 1024)..file_len;
        push_ranges_to_metadata_decoder(&mut metadata_decoder, vec![prefetch_range]);

        // expect the decoder has enough data to decode the metadata
        let metadata = expect_data(metadata_decoder.try_decode());
        expect_finished(metadata_decoder.try_decode());
        assert_eq!(metadata.num_row_groups(), 2);
        assert_eq!(metadata.row_group(0).num_rows(), 200);
        assert_eq!(metadata.row_group(1).num_rows(), 200);
        assert!(metadata.column_index().is_some());
        assert!(metadata.offset_index().is_some());
    }

    /// It is possible to pre-fetch some, but not all, of the necessary data
    /// data
    #[test]
    fn test_metadata_decoder_prefetch_retry() {
        let file_len = test_file_len();
        let mut metadata_decoder = ParquetMetaDataPushDecoder::try_new(file_len).unwrap();
        // simulate pre-fetching the last 1500 bytes of the file.
        // this is enough to read the footer thrift metadata, but not the offset indexes
        let prefetch_range = (file_len - 1500)..file_len;
        push_ranges_to_metadata_decoder(&mut metadata_decoder, vec![prefetch_range]);

        // expect another request is needed to read the offset indexes (note
        // try_decode only returns NeedsData once, whereas without any prefetching it would
        // return NeedsData three times)
        let ranges = expect_needs_data(metadata_decoder.try_decode());
        push_ranges_to_metadata_decoder(&mut metadata_decoder, ranges);

        // expect the decoder has enough data to decode the metadata
        let metadata = expect_data(metadata_decoder.try_decode());
        expect_finished(metadata_decoder.try_decode());

        assert_eq!(metadata.num_row_groups(), 2);
        assert_eq!(metadata.row_group(0).num_rows(), 200);
        assert_eq!(metadata.row_group(1).num_rows(), 200);
        assert!(metadata.column_index().is_some());
        assert!(metadata.offset_index().is_some());
    }

    /// Decode the metadata incrementally, simulating a scenario where exactly the data needed
    /// is read in each step
    #[test]
    fn test_metadata_decoder_incremental() {
        let file_len = TEST_FILE_DATA.len() as u64;
        let mut metadata_decoder = ParquetMetaDataPushDecoder::try_new(file_len).unwrap();
        let ranges = expect_needs_data(metadata_decoder.try_decode());
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0], test_file_len() - 8..test_file_len());
        push_ranges_to_metadata_decoder(&mut metadata_decoder, ranges);

        // expect the first request to read the footer
        let ranges = expect_needs_data(metadata_decoder.try_decode());
        push_ranges_to_metadata_decoder(&mut metadata_decoder, ranges);

        // expect the second request to read the offset indexes
        let ranges = expect_needs_data(metadata_decoder.try_decode());
        push_ranges_to_metadata_decoder(&mut metadata_decoder, ranges);

        // expect the third request to read the actual data
        let metadata = expect_data(metadata_decoder.try_decode());
        expect_finished(metadata_decoder.try_decode());

        assert_eq!(metadata.num_row_groups(), 2);
        assert_eq!(metadata.row_group(0).num_rows(), 200);
        assert_eq!(metadata.row_group(1).num_rows(), 200);
        assert!(metadata.column_index().is_some());
        assert!(metadata.offset_index().is_some());
    }

    /// Decode the metadata incrementally, but without reading the page indexes
    /// (so only two requests)
    #[test]
    fn test_metadata_decoder_incremental_no_page_index() {
        let file_len = TEST_FILE_DATA.len() as u64;
        let mut metadata_decoder = ParquetMetaDataPushDecoder::try_new(file_len)
            .unwrap()
            .with_page_index_policy(PageIndexPolicy::Skip);
        let ranges = expect_needs_data(metadata_decoder.try_decode());
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0], test_file_len() - 8..test_file_len());
        push_ranges_to_metadata_decoder(&mut metadata_decoder, ranges);

        // expect the first request to read the footer
        let ranges = expect_needs_data(metadata_decoder.try_decode());
        push_ranges_to_metadata_decoder(&mut metadata_decoder, ranges);

        // expect NO second request to read the offset indexes, should just cough up the metadata
        let metadata = expect_data(metadata_decoder.try_decode());
        expect_finished(metadata_decoder.try_decode());

        assert_eq!(metadata.num_row_groups(), 2);
        assert_eq!(metadata.row_group(0).num_rows(), 200);
        assert_eq!(metadata.row_group(1).num_rows(), 200);
        assert!(metadata.column_index().is_none()); // of course, we did not read the column index
        assert!(metadata.offset_index().is_none()); // or the offset index
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

    /// Create a parquet file in memory for testing. See [`test_file_range`] for details.
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

    /// Return the length of the test file in bytes
    fn test_file_len() -> u64 {
        TEST_FILE_DATA.len() as u64
    }

    /// Return the range of the entire test file
    fn test_file_range() -> Range<u64> {
        0..test_file_len()
    }

    /// Return a slice of the test file data from the given range
    pub fn test_file_slice(range: Range<u64>) -> Bytes {
        let start: usize = range.start.try_into().unwrap();
        let end: usize = range.end.try_into().unwrap();
        TEST_FILE_DATA.slice(start..end)
    }

    /// Push the given ranges to the metadata decoder, simulating reading from a file
    fn push_ranges_to_metadata_decoder(
        metadata_decoder: &mut ParquetMetaDataPushDecoder,
        ranges: Vec<Range<u64>>,
    ) {
        let data = ranges
            .iter()
            .map(|range| test_file_slice(range.clone()))
            .collect::<Vec<_>>();
        metadata_decoder.push_ranges(ranges, data).unwrap();
    }

    /// Expect that the [`DecodeResult`] is a [`DecodeResult::Data`] and return the corresponding element
    fn expect_data<T: Debug>(result: Result<DecodeResult<T>, ParquetError>) -> T {
        match result.expect("Expected Ok(DecodeResult::Data(T))") {
            DecodeResult::Data(data) => data,
            result => panic!("Expected DecodeResult::Data, got {result:?}"),
        }
    }

    /// Expect that the [`DecodeResult`] is a [`DecodeResult::NeedsData`] and return the corresponding ranges
    fn expect_needs_data<T: Debug>(
        result: Result<DecodeResult<T>, ParquetError>,
    ) -> Vec<Range<u64>> {
        match result.expect("Expected Ok(DecodeResult::NeedsData{ranges})") {
            DecodeResult::NeedsData(ranges) => ranges,
            result => panic!("Expected DecodeResult::NeedsData, got {result:?}"),
        }
    }

    fn expect_finished<T: Debug>(result: Result<DecodeResult<T>, ParquetError>) {
        match result.expect("Expected Ok(DecodeResult::Finished)") {
            DecodeResult::Finished => {}
            result => panic!("Expected DecodeResult::Finished, got {result:?}"),
        }
    }
}
