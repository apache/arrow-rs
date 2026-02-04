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

use crate::DecodeResult;
#[cfg(feature = "encryption")]
use crate::encryption::decrypt::FileDecryptionProperties;
use crate::errors::{ParquetError, Result};
use crate::file::FOOTER_SIZE;
use crate::file::metadata::parser::{MetadataParser, parse_column_index, parse_offset_index};
use crate::file::metadata::{FooterTail, PageIndexPolicy, ParquetMetaData, ParquetMetaDataOptions};
use crate::file::page_index::index_reader::acc_range;
use crate::file::reader::ChunkReader;
use bytes::Bytes;
use std::ops::Range;
use std::sync::Arc;

/// A push decoder for [`ParquetMetaData`].
///
/// This structure implements a push API for decoding Parquet metadata, which
/// decouples IO from the metadata decoding logic (sometimes referred to as
/// [Sans-IO]).
///
/// See [`ParquetMetaDataReader`] for a pull-based API that incorporates IO and
/// is simpler to use for basic use cases. This decoder is best for customizing
/// your IO operations to minimize bytes read, prefetch data, or use async IO.
///
/// [Sans-IO]: https://sans-io.readthedocs.io
/// [`ParquetMetaDataReader`]: crate::file::metadata::ParquetMetaDataReader
///
/// # Example
///
/// The most basic usage is to feed the decoder with the necessary byte ranges
/// as requested as shown below. This minimizes the number of bytes read, but
/// requires the most IO operations - one to read the footer and then one
/// to read the metadata, and possibly more if page indexes are requested.
///
#[cfg_attr(
    feature = "arrow",
    doc = r##"
```rust
# use std::ops::Range;
# use bytes::Bytes;
# use arrow_array::record_batch;
# use parquet::DecodeResult;
# use parquet::arrow::ArrowWriter;
# use parquet::errors::ParquetError;
# use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataPushDecoder};
#
# fn decode_metadata() -> Result<ParquetMetaData, ParquetError> {
# let file_bytes = {
#   let mut buffer = vec![0];
#   let batch = record_batch!(("a", Int32, [1, 2, 3])).unwrap();
#   let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), None).unwrap();
#   writer.write(&batch).unwrap();
#   writer.close().unwrap();
#   Bytes::from(buffer)
# };
# // mimic IO by returning a function that returns the bytes for a given range
# let get_range = |range: &Range<u64>| -> Bytes {
#    let start = range.start as usize;
#     let end = range.end as usize;
#    file_bytes.slice(start..end)
# };
#
# let file_len = file_bytes.len() as u64;
// The `ParquetMetaDataPushDecoder` needs to know the file length.
let mut decoder = ParquetMetaDataPushDecoder::try_new(file_len).unwrap();
// try to decode the metadata. If more data is needed, the decoder will tell you what ranges
loop {
    match decoder.try_decode() {
       Ok(DecodeResult::Data(metadata)) => { return Ok(metadata); } // decode successful
       Ok(DecodeResult::NeedsData(ranges)) => {
          // The decoder needs more data
          //
          // In this example, we call a function that returns the bytes for each given range.
          // In a real application, you would likely read the data from a file or network.
          let data = ranges.iter().map(|range| get_range(range)).collect();
          // Push the data into the decoder and try to decode again on the next iteration.
          decoder.push_ranges(ranges, data).unwrap();
       }
       Ok(DecodeResult::Finished) => { unreachable!("returned metadata in previous match arm") }
       Err(e) => return Err(e),
    }
}
# }
```
"##
)]
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
#[cfg_attr(
    feature = "arrow",
    doc = r##"
```rust
# use std::ops::Range;
# use bytes::Bytes;
# use arrow_array::record_batch;
# use parquet::DecodeResult;
# use parquet::arrow::ArrowWriter;
# use parquet::errors::ParquetError;
# use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataPushDecoder};
#
# fn decode_metadata() -> Result<ParquetMetaData, ParquetError> {
# let file_bytes = {
#   let mut buffer = vec![0];
#   let batch = record_batch!(("a", Int32, [1, 2, 3])).unwrap();
#   let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), None).unwrap();
#   writer.write(&batch).unwrap();
#   writer.close().unwrap();
#   Bytes::from(buffer)
# };
#
let file_len = file_bytes.len() as u64;
// For this example, we "prefetch" all the bytes which we have in memory,
// but in a real application, you would likely read a chunk from the end
// for example 1MB.
let prefetched_bytes = file_bytes.clone();
let mut decoder = ParquetMetaDataPushDecoder::try_new(file_len).unwrap();
// push the prefetched bytes into the decoder
decoder.push_ranges(vec![0..file_len], vec![prefetched_bytes]).unwrap();
// The decoder will now be able to decode the metadata. Note in a real application,
// unless you can guarantee that the pushed data is enough to decode the metadata,
// you still need to call `try_decode` in a loop until it returns `DecodeResult::Data`
// as shown in  the previous example
    match decoder.try_decode() {
        Ok(DecodeResult::Data(metadata)) => { return Ok(metadata); } // decode successful
        other => { panic!("expected DecodeResult::Data, got: {other:?}") }
    }
# }
```
"##
)]
///
/// # Example using [`AsyncRead`]
///
/// [`ParquetMetaDataPushDecoder`] is designed to work with any data source that can
/// provide byte ranges, including async IO sources. However, it does not
/// implement async IO itself. To use async IO, you simply write an async
/// wrapper around it that reads the required byte ranges and pushes them into the
/// decoder.
#[cfg_attr(
    feature = "arrow",
    doc = r##"
```rust
# use std::ops::Range;
# use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt};
# use arrow_array::record_batch;
# use parquet::DecodeResult;
# use parquet::arrow::ArrowWriter;
# use parquet::errors::ParquetError;
# use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataPushDecoder};
#
// This function decodes Parquet Metadata from anything that implements
// [`AsyncRead`] and [`AsyncSeek`] such as a tokio::fs::File
async fn decode_metadata(
  file_len: u64,
  mut async_source: impl AsyncRead + AsyncSeek + Unpin
) -> Result<ParquetMetaData, ParquetError> {
  // We need a ParquetMetaDataPushDecoder to decode the metadata.
  let mut decoder = ParquetMetaDataPushDecoder::try_new(file_len).unwrap();
  loop {
    match decoder.try_decode() {
       Ok(DecodeResult::Data(metadata)) => { return Ok(metadata); } // decode successful
       Ok(DecodeResult::NeedsData(ranges)) => {
          // The decoder needs more data
          //
          // In this example we use the AsyncRead and AsyncSeek traits to read the
          // required ranges from the async source.
          let mut data = Vec::with_capacity(ranges.len());
          for range in &ranges {
            let mut buffer = vec![0; (range.end - range.start) as usize];
            async_source.seek(std::io::SeekFrom::Start(range.start)).await?;
            async_source.read_exact(&mut buffer).await?;
            data.push(Bytes::from(buffer));
          }
          // Push the data into the decoder and try to decode again on the next iteration.
          decoder.push_ranges(ranges, data).unwrap();
       }
       Ok(DecodeResult::Finished) => { unreachable!("returned metadata in previous match arm") }
       Err(e) => return Err(e),
    }
  }
}
```
"##
)]
/// [`AsyncRead`]: tokio::io::AsyncRead
#[derive(Debug)]
pub struct ParquetMetaDataPushDecoder {
    /// Decoding state
    state: DecodeState,
    /// policy for loading ColumnIndex (part of the PageIndex)
    column_index_policy: PageIndexPolicy,
    /// policy for loading OffsetIndex (part of the PageIndex)
    offset_index_policy: PageIndexPolicy,
    /// Underlying buffers
    buffers: crate::util::push_buffers::PushBuffers,
    /// Encryption API
    metadata_parser: MetadataParser,
}

impl ParquetMetaDataPushDecoder {
    /// Create a new `ParquetMetaDataPushDecoder` with the given file length.
    ///
    /// By default, this will read page indexes and column indexes. See
    /// [`ParquetMetaDataPushDecoder::with_page_index_policy`] for more detail.
    ///
    /// See examples on [`ParquetMetaDataPushDecoder`].
    pub fn try_new(file_len: u64) -> Result<Self> {
        if file_len < 8 {
            return Err(ParquetError::General(format!(
                "Parquet files are at least 8 bytes long, but file length is {file_len}"
            )));
        };

        Ok(Self {
            state: DecodeState::ReadingFooter,
            column_index_policy: PageIndexPolicy::Optional,
            offset_index_policy: PageIndexPolicy::Optional,
            buffers: crate::util::push_buffers::PushBuffers::new(file_len),
            metadata_parser: MetadataParser::new(),
        })
    }

    /// Begin decoding from the given footer tail.
    pub(crate) fn try_new_with_footer_tail(file_len: u64, footer_tail: FooterTail) -> Result<Self> {
        let mut new_self = Self::try_new(file_len)?;
        new_self.state = DecodeState::ReadingMetadata(footer_tail);
        Ok(new_self)
    }

    /// Create a decoder with the given `ParquetMetaData` already known.
    ///
    /// This can be used to parse and populate the page index structures
    /// after the metadata has already been decoded.
    pub fn try_new_with_metadata(file_len: u64, metadata: ParquetMetaData) -> Result<Self> {
        let mut new_self = Self::try_new(file_len)?;
        new_self.state = DecodeState::ReadingPageIndex(Box::new(metadata));
        Ok(new_self)
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
        self.column_index_policy = page_index_policy;
        self.offset_index_policy = page_index_policy;
        self
    }

    /// Set the policy for reading the ColumnIndex (part of the PageIndex)
    pub fn with_column_index_policy(mut self, column_index_policy: PageIndexPolicy) -> Self {
        self.column_index_policy = column_index_policy;
        self
    }

    /// Set the policy for reading the OffsetIndex (part of the PageIndex)
    pub fn with_offset_index_policy(mut self, offset_index_policy: PageIndexPolicy) -> Self {
        self.offset_index_policy = offset_index_policy;
        self
    }

    /// Set the options to use when decoding the Parquet metadata.
    pub fn with_metadata_options(mut self, options: Option<Arc<ParquetMetaDataOptions>>) -> Self {
        self.metadata_parser = self.metadata_parser.with_metadata_options(options);
        self
    }

    #[cfg(feature = "encryption")]
    /// Provide decryption properties for decoding encrypted Parquet files
    pub(crate) fn with_file_decryption_properties(
        mut self,
        file_decryption_properties: Option<std::sync::Arc<FileDecryptionProperties>>,
    ) -> Self {
        self.metadata_parser = self
            .metadata_parser
            .with_file_decryption_properties(file_decryption_properties);
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
    pub fn push_ranges(&mut self, ranges: Vec<Range<u64>>, buffers: Vec<Bytes>) -> Result<()> {
        if matches!(&self.state, DecodeState::Finished) {
            return Err(general_err!(
                "ParquetMetaDataPushDecoder: cannot push data after decoding is finished"
            ));
        }
        self.buffers.push_ranges(ranges, buffers);
        Ok(())
    }

    /// Pushes a single range of data into the decoder's buffer.
    pub fn push_range(&mut self, range: Range<u64>, buffer: Bytes) -> Result<()> {
        if matches!(&self.state, DecodeState::Finished) {
            return Err(general_err!(
                "ParquetMetaDataPushDecoder: cannot push data after decoding is finished"
            ));
        }
        self.buffers.push_range(range, buffer);
        Ok(())
    }

    /// Try to decode the metadata from the pushed data, returning the
    /// decoded metadata or an error if not enough data is available.
    pub fn try_decode(&mut self) -> Result<DecodeResult<ParquetMetaData>> {
        let file_len = self.buffers.file_len();
        let footer_len = FOOTER_SIZE as u64;
        loop {
            match std::mem::replace(&mut self.state, DecodeState::Intermediate) {
                DecodeState::ReadingFooter => {
                    // need to have the last 8 bytes of the file to decode the metadata
                    let footer_start = file_len.saturating_sub(footer_len);
                    let footer_range = footer_start..file_len;

                    if !self.buffers.has_range(&footer_range) {
                        self.state = DecodeState::ReadingFooter;
                        return Ok(needs_range(footer_range));
                    }
                    let footer_bytes = self.get_bytes(&footer_range)?;
                    let footer_tail = FooterTail::try_from(footer_bytes.as_ref())?;

                    self.state = DecodeState::ReadingMetadata(footer_tail);
                    continue;
                }

                DecodeState::ReadingMetadata(footer_tail) => {
                    let metadata_len: u64 = footer_tail.metadata_length() as u64;
                    let metadata_start = file_len - footer_len - metadata_len;
                    let metadata_end = metadata_start + metadata_len;
                    let metadata_range = metadata_start..metadata_end;

                    if !self.buffers.has_range(&metadata_range) {
                        self.state = DecodeState::ReadingMetadata(footer_tail);
                        return Ok(needs_range(metadata_range));
                    }

                    let metadata = self.metadata_parser.decode_metadata(
                        &self.get_bytes(&metadata_range)?,
                        footer_tail.is_encrypted_footer(),
                    )?;
                    // Note: ReadingPageIndex first checks if page indexes are needed
                    // and is a no-op if not
                    self.state = DecodeState::ReadingPageIndex(Box::new(metadata));
                    continue;
                }

                DecodeState::ReadingPageIndex(mut metadata) => {
                    // First determine if any page indexes are needed based on
                    // the specified policies
                    let range = range_for_page_index(
                        &metadata,
                        self.column_index_policy,
                        self.offset_index_policy,
                    );

                    let Some(page_index_range) = range else {
                        self.state = DecodeState::Finished;
                        return Ok(DecodeResult::Data(*metadata));
                    };

                    if !self.buffers.has_range(&page_index_range) {
                        self.state = DecodeState::ReadingPageIndex(metadata);
                        return Ok(needs_range(page_index_range));
                    }

                    let buffer = self.get_bytes(&page_index_range)?;
                    let offset = page_index_range.start;
                    parse_column_index(&mut metadata, self.column_index_policy, &buffer, offset)?;
                    parse_offset_index(&mut metadata, self.offset_index_policy, &buffer, offset)?;
                    self.state = DecodeState::Finished;
                    return Ok(DecodeResult::Data(*metadata));
                }

                DecodeState::Finished => return Ok(DecodeResult::Finished),
                DecodeState::Intermediate => {
                    return Err(general_err!(
                        "ParquetMetaDataPushDecoder: internal error, invalid state"
                    ));
                }
            }
        }
    }

    /// Returns the bytes for the given range from the internal buffer
    fn get_bytes(&self, range: &Range<u64>) -> Result<Bytes> {
        let start = range.start;
        let raw_len = range.end - range.start;
        let len: usize = raw_len.try_into().map_err(|_| {
            ParquetError::General(format!(
                "ParquetMetaDataPushDecoder: Range length too large to fit in usize: {raw_len}",
            ))
        })?;
        self.buffers.get_bytes(start, len)
    }
}

/// returns a DecodeResults that describes needing the given range
fn needs_range(range: Range<u64>) -> DecodeResult<ParquetMetaData> {
    DecodeResult::NeedsData(vec![range])
}

/// Decoding state machine
#[derive(Debug)]
enum DecodeState {
    /// Reading the last 8 bytes of the file
    ReadingFooter,
    /// Reading the metadata thrift structure
    ReadingMetadata(FooterTail),
    // Actively reading the page index
    ReadingPageIndex(Box<ParquetMetaData>),
    // Decoding is complete
    Finished,
    /// State left during the `try_decode` method so something valid is present.
    /// This state should never be observed.
    Intermediate,
}

/// Returns the byte range needed to read the offset/page indexes, based on the
/// specified policies
///
/// Returns None if no page indexes are needed
pub fn range_for_page_index(
    metadata: &ParquetMetaData,
    column_index_policy: PageIndexPolicy,
    offset_index_policy: PageIndexPolicy,
) -> Option<Range<u64>> {
    let mut range = None;
    for c in metadata.row_groups().iter().flat_map(|r| r.columns()) {
        if column_index_policy != PageIndexPolicy::Skip {
            range = acc_range(range, c.column_index_range());
        }
        if offset_index_policy != PageIndexPolicy::Skip {
            range = acc_range(range, c.offset_index_range());
        }
    }
    range
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
    fn expect_data<T: Debug>(result: Result<DecodeResult<T>>) -> T {
        match result.expect("Expected Ok(DecodeResult::Data(T))") {
            DecodeResult::Data(data) => data,
            result => panic!("Expected DecodeResult::Data, got {result:?}"),
        }
    }

    /// Expect that the [`DecodeResult`] is a [`DecodeResult::NeedsData`] and return the corresponding ranges
    fn expect_needs_data<T: Debug>(result: Result<DecodeResult<T>>) -> Vec<Range<u64>> {
        match result.expect("Expected Ok(DecodeResult::NeedsData{ranges})") {
            DecodeResult::NeedsData(ranges) => ranges,
            result => panic!("Expected DecodeResult::NeedsData, got {result:?}"),
        }
    }

    fn expect_finished<T: Debug>(result: Result<DecodeResult<T>>) {
        match result.expect("Expected Ok(DecodeResult::Finished)") {
            DecodeResult::Finished => {}
            result => panic!("Expected DecodeResult::Finished, got {result:?}"),
        }
    }
}
