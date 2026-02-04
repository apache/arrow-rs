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

//! [`ParquetPushDecoder`]: decodes Parquet data with data provided by the
//! caller (rather than from an underlying reader).

mod reader_builder;
mod remaining;

use crate::DecodeResult;
use crate::arrow::arrow_reader::{
    ArrowReaderBuilder, ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReader,
};
use crate::errors::ParquetError;
use crate::file::metadata::ParquetMetaData;
use crate::util::push_buffers::PushBuffers;
use arrow_array::RecordBatch;
use bytes::Bytes;
use reader_builder::RowGroupReaderBuilder;
use remaining::RemainingRowGroups;
use std::ops::Range;
use std::sync::Arc;

/// A builder for [`ParquetPushDecoder`].
///
/// To create a new decoder, use [`ParquetPushDecoderBuilder::try_new_decoder`].
///
/// You can decode the metadata from a Parquet file using either
/// [`ParquetMetadataReader`] or [`ParquetMetaDataPushDecoder`].
///
/// [`ParquetMetadataReader`]: crate::file::metadata::ParquetMetaDataReader
/// [`ParquetMetaDataPushDecoder`]: crate::file::metadata::ParquetMetaDataPushDecoder
///
/// Note the "input" type is `u64` which represents the length of the Parquet file
/// being decoded. This is needed to initialize the internal buffers that track
/// what data has been provided to the decoder.
///
/// # Example
/// ```
/// # use std::ops::Range;
/// # use std::sync::Arc;
/// # use bytes::Bytes;
/// # use arrow_array::record_batch;
/// # use parquet::DecodeResult;
/// # use parquet::arrow::push_decoder::ParquetPushDecoderBuilder;
/// # use parquet::arrow::ArrowWriter;
/// # use parquet::file::metadata::ParquetMetaDataPushDecoder;
/// # let file_bytes = {
/// #   let mut buffer = vec![];
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
/// # let file_length = file_bytes.len() as u64;
/// # let mut metadata_decoder = ParquetMetaDataPushDecoder::try_new(file_length).unwrap();
/// # metadata_decoder.push_ranges(vec![0..file_length], vec![file_bytes.clone()]).unwrap();
/// # let DecodeResult::Data(parquet_metadata) = metadata_decoder.try_decode().unwrap() else { panic!("failed to decode metadata") };
/// # let parquet_metadata = Arc::new(parquet_metadata);
/// // The file length and metadata are required to create the decoder
/// let mut decoder =
///     ParquetPushDecoderBuilder::try_new_decoder(parquet_metadata)
///       .unwrap()
///       // Optionally configure the decoder, e.g. batch size
///       .with_batch_size(1024)
///       // Build the decoder
///       .build()
///       .unwrap();
///
///     // In a loop, ask the decoder what it needs next, and provide it with the required data
///     loop {
///         match decoder.try_decode().unwrap() {
///             DecodeResult::NeedsData(ranges) => {
///                 // The decoder needs more data. Fetch the data for the given ranges
///                 let data = ranges.iter().map(|r| get_range(r)).collect::<Vec<_>>();
///                 // Push the data to the decoder
///                 decoder.push_ranges(ranges, data).unwrap();
///                 // After pushing the data, we can try to decode again on the next iteration
///             }
///             DecodeResult::Data(batch) => {
///                 // Successfully decoded a batch of data
///                 assert!(batch.num_rows() > 0);
///             }
///             DecodeResult::Finished => {
///                 // The decoder has finished decoding exit the loop
///                 break;
///             }
///         }
///     }
/// ```
pub type ParquetPushDecoderBuilder = ArrowReaderBuilder<NoInput>;

/// Type that represents "No input" for the [`ParquetPushDecoderBuilder`]
///
/// There is no "input" for the push decoder by design (the idea is that
/// the caller pushes data to the decoder as needed)..
///
/// However, [`ArrowReaderBuilder`] is shared with the sync and async readers,
/// which DO have an `input`. To support reusing the same builder code for
/// all three types of decoders, we define this `NoInput` for the push decoder to
/// denote in the type system there is no type.
#[derive(Debug, Clone, Copy)]
pub struct NoInput;

/// Methods for building a ParquetDecoder. See the base [`ArrowReaderBuilder`] for
/// more options that can be configured.
impl ParquetPushDecoderBuilder {
    /// Create a new `ParquetDecoderBuilder` for configuring a Parquet decoder for the given file.
    ///
    /// See [`ParquetMetadataDecoder`] for a builder that can read the metadata from a Parquet file.
    ///
    /// [`ParquetMetadataDecoder`]: crate::file::metadata::ParquetMetaDataPushDecoder
    ///
    /// See example on [`ParquetPushDecoderBuilder`]
    pub fn try_new_decoder(parquet_metadata: Arc<ParquetMetaData>) -> Result<Self, ParquetError> {
        Self::try_new_decoder_with_options(parquet_metadata, ArrowReaderOptions::default())
    }

    /// Create a new `ParquetDecoderBuilder` for configuring a Parquet decoder for the given file
    /// with the given reader options.
    ///
    /// This is similar to [`Self::try_new_decoder`] but allows configuring
    /// options such as Arrow schema
    pub fn try_new_decoder_with_options(
        parquet_metadata: Arc<ParquetMetaData>,
        arrow_reader_options: ArrowReaderOptions,
    ) -> Result<Self, ParquetError> {
        let arrow_reader_metadata =
            ArrowReaderMetadata::try_new(parquet_metadata, arrow_reader_options)?;
        Ok(Self::new_with_metadata(arrow_reader_metadata))
    }

    /// Create a new `ParquetDecoderBuilder` given [`ArrowReaderMetadata`].
    ///
    /// See [`ArrowReaderMetadata::try_new`] for how to create the metadata from
    /// the Parquet metadata and reader options.
    pub fn new_with_metadata(arrow_reader_metadata: ArrowReaderMetadata) -> Self {
        Self::new_builder(NoInput, arrow_reader_metadata)
    }

    /// Create a [`ParquetPushDecoder`] with the configured options
    pub fn build(self) -> Result<ParquetPushDecoder, ParquetError> {
        let Self {
            input: NoInput,
            metadata: parquet_metadata,
            schema: _,
            fields,
            batch_size,
            row_groups,
            projection,
            filter,
            selection,
            limit,
            offset,
            metrics,
            row_selection_policy,
            max_predicate_cache_size,
        } = self;

        // If no row groups were specified, read all of them
        let row_groups =
            row_groups.unwrap_or_else(|| (0..parquet_metadata.num_row_groups()).collect());

        // Prepare to build RowGroup readers
        let file_len = 0; // not used in push decoder
        let buffers = PushBuffers::new(file_len);
        let row_group_reader_builder = RowGroupReaderBuilder::new(
            batch_size,
            projection,
            Arc::clone(&parquet_metadata),
            fields,
            filter,
            limit,
            offset,
            metrics,
            max_predicate_cache_size,
            buffers,
            row_selection_policy,
        );

        // Initialize the decoder with the configured options
        let remaining_row_groups = RemainingRowGroups::new(
            parquet_metadata,
            row_groups,
            selection,
            row_group_reader_builder,
        );

        Ok(ParquetPushDecoder {
            state: ParquetDecoderState::ReadingRowGroup {
                remaining_row_groups: Box::new(remaining_row_groups),
            },
        })
    }
}

/// A push based Parquet Decoder
///
/// See [`ParquetPushDecoderBuilder`] for an example of how to build and use the decoder.
///
/// [`ParquetPushDecoder`] is a low level API for decoding Parquet data without an
/// underlying reader for performing IO, and thus offers fine grained control
/// over how data is fetched and decoded.
///
/// When more data is needed to make progress, instead of reading data directly
/// from a reader, the decoder returns [`DecodeResult`] indicating what ranges
/// are needed. Once the caller provides the requested ranges via
/// [`Self::push_ranges`], they try to decode again by calling
/// [`Self::try_decode`].
///
/// The decoder's internal state tracks what has been already decoded and what
/// is needed next.
#[derive(Debug)]
pub struct ParquetPushDecoder {
    /// The inner state.
    ///
    /// This state is consumed on every transition and a new state is produced
    /// so the Rust compiler can ensure that the state is always valid and
    /// transitions are not missed.
    state: ParquetDecoderState,
}

impl ParquetPushDecoder {
    /// Attempt to decode the next batch of data, or return what data is needed
    ///
    /// The the decoder communicates the next state with a [`DecodeResult`]
    ///
    /// See full example in [`ParquetPushDecoderBuilder`]
    ///
    /// ```no_run
    /// # use parquet::arrow::push_decoder::ParquetPushDecoder;
    /// use parquet::DecodeResult;
    /// # fn get_decoder() -> ParquetPushDecoder { unimplemented!() }
    /// # fn push_data(decoder: &mut ParquetPushDecoder, ranges: Vec<std::ops::Range<u64>>) { unimplemented!() }
    /// let mut decoder = get_decoder();
    /// loop {
    ///    match decoder.try_decode().unwrap() {
    ///       DecodeResult::NeedsData(ranges) => {
    ///         // The decoder needs more data. Fetch the data for the given ranges
    ///         // call decoder.push_ranges(ranges, data) and call again
    ///         push_data(&mut decoder, ranges);
    ///       }
    ///       DecodeResult::Data(batch) => {
    ///         // Successfully decoded the next batch of data
    ///         println!("Got batch with {} rows", batch.num_rows());
    ///       }
    ///       DecodeResult::Finished => {
    ///         // The decoder has finished decoding all data
    ///         break;
    ///       }
    ///    }
    /// }
    ///```
    pub fn try_decode(&mut self) -> Result<DecodeResult<RecordBatch>, ParquetError> {
        let current_state = std::mem::replace(&mut self.state, ParquetDecoderState::Finished);
        let (new_state, decode_result) = current_state.try_next_batch()?;
        self.state = new_state;
        Ok(decode_result)
    }

    /// Return a [`ParquetRecordBatchReader`] that reads the next set of rows, or
    /// return what data is needed to produce it.
    ///
    /// This API can be used to get a reader for decoding the next set of
    /// RecordBatches while proceeding to begin fetching data for the set (e.g
    /// row group)
    ///
    /// Example
    /// ```no_run
    /// # use parquet::arrow::push_decoder::ParquetPushDecoder;
    /// use parquet::DecodeResult;
    /// # fn get_decoder() -> ParquetPushDecoder { unimplemented!() }
    /// # fn push_data(decoder: &mut ParquetPushDecoder, ranges: Vec<std::ops::Range<u64>>) { unimplemented!() }
    /// let mut decoder = get_decoder();
    /// loop {
    ///    match decoder.try_next_reader().unwrap() {
    ///       DecodeResult::NeedsData(ranges) => {
    ///         // The decoder needs more data. Fetch the data for the given ranges
    ///         // call decoder.push_ranges(ranges, data) and call again
    ///         push_data(&mut decoder, ranges);
    ///       }
    ///       DecodeResult::Data(reader) => {
    ///          // spawn a thread to read the batches in parallel
    ///          // with fetching the next row group / data
    ///          std::thread::spawn(move || {
    ///            for batch in reader {
    ///              let batch = batch.unwrap();
    ///              println!("Got batch with {} rows", batch.num_rows());
    ///            }
    ///         });
    ///       }
    ///       DecodeResult::Finished => {
    ///         // The decoder has finished decoding all data
    ///         break;
    ///       }
    ///    }
    /// }
    ///```
    pub fn try_next_reader(
        &mut self,
    ) -> Result<DecodeResult<ParquetRecordBatchReader>, ParquetError> {
        let current_state = std::mem::replace(&mut self.state, ParquetDecoderState::Finished);
        let (new_state, decode_result) = current_state.try_next_reader()?;
        self.state = new_state;
        Ok(decode_result)
    }

    /// Push data into the decoder for processing
    ///
    /// This is a convenience wrapper around [`Self::push_ranges`] for pushing a
    /// single range of data.
    ///
    /// Note this can be the entire file or just a part of it. If it is part of the file,
    /// the ranges should correspond to the data ranges requested by the decoder.
    ///
    /// See example in [`ParquetPushDecoderBuilder`]
    pub fn push_range(&mut self, range: Range<u64>, data: Bytes) -> Result<(), ParquetError> {
        self.push_ranges(vec![range], vec![data])
    }

    /// Push data into the decoder for processing
    ///
    /// This should correspond to the data ranges requested by the decoder
    pub fn push_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
        data: Vec<Bytes>,
    ) -> Result<(), ParquetError> {
        let current_state = std::mem::replace(&mut self.state, ParquetDecoderState::Finished);
        self.state = current_state.push_data(ranges, data)?;
        Ok(())
    }

    /// Returns the total number of buffered bytes in the decoder
    ///
    /// This is the sum of the size of all [`Bytes`] that has been pushed to the
    /// decoder but not yet consumed.
    ///
    /// Note that this does not include any overhead of the internal data
    /// structures and that since [`Bytes`] are ref counted memory, this may not
    /// reflect additional memory usage.
    ///
    /// This can be used to monitor memory usage of the decoder.
    pub fn buffered_bytes(&self) -> u64 {
        self.state.buffered_bytes()
    }
}

/// Internal state machine for the [`ParquetPushDecoder`]
#[derive(Debug)]
enum ParquetDecoderState {
    /// Waiting for data needed to decode the next RowGroup
    ReadingRowGroup {
        remaining_row_groups: Box<RemainingRowGroups>,
    },
    /// The decoder is actively decoding a RowGroup
    DecodingRowGroup {
        /// Current active reader
        record_batch_reader: Box<ParquetRecordBatchReader>,
        remaining_row_groups: Box<RemainingRowGroups>,
    },
    /// The decoder has finished processing all data
    Finished,
}

impl ParquetDecoderState {
    /// If actively reading a RowGroup, return the currently active
    /// ParquetRecordBatchReader and advance to the next group.
    fn try_next_reader(
        self,
    ) -> Result<(Self, DecodeResult<ParquetRecordBatchReader>), ParquetError> {
        let mut current_state = self;
        loop {
            let (next_state, decode_result) = current_state.transition()?;
            // if more data is needed to transition, can't proceed further without it
            match decode_result {
                DecodeResult::NeedsData(ranges) => {
                    return Ok((next_state, DecodeResult::NeedsData(ranges)));
                }
                // act next based on state
                DecodeResult::Data(()) | DecodeResult::Finished => {}
            }
            match next_state {
                // not ready to read yet, continue transitioning
                Self::ReadingRowGroup { .. } => current_state = next_state,
                // have a reader ready, so return it and set ourself to ReadingRowGroup
                Self::DecodingRowGroup {
                    record_batch_reader,
                    remaining_row_groups,
                } => {
                    let result = DecodeResult::Data(*record_batch_reader);
                    let next_state = Self::ReadingRowGroup {
                        remaining_row_groups,
                    };
                    return Ok((next_state, result));
                }
                Self::Finished => {
                    return Ok((Self::Finished, DecodeResult::Finished));
                }
            }
        }
    }

    /// Current state --> next state + output
    ///
    /// This function is called to get the next RecordBatch
    ///
    /// This structure is used to reduce the indentation level of the main loop
    /// in try_build
    fn try_next_batch(self) -> Result<(Self, DecodeResult<RecordBatch>), ParquetError> {
        let mut current_state = self;
        loop {
            let (new_state, decode_result) = current_state.transition()?;
            // if more data is needed to transition, can't proceed further without it
            match decode_result {
                DecodeResult::NeedsData(ranges) => {
                    return Ok((new_state, DecodeResult::NeedsData(ranges)));
                }
                // act next based on state
                DecodeResult::Data(()) | DecodeResult::Finished => {}
            }
            match new_state {
                // not ready to read yet, continue transitioning
                Self::ReadingRowGroup { .. } => current_state = new_state,
                // have a reader ready, so decode the next batch
                Self::DecodingRowGroup {
                    mut record_batch_reader,
                    remaining_row_groups,
                } => {
                    match record_batch_reader.next() {
                        // Successfully decoded a batch, return it
                        Some(Ok(batch)) => {
                            let result = DecodeResult::Data(batch);
                            let next_state = Self::DecodingRowGroup {
                                record_batch_reader,
                                remaining_row_groups,
                            };
                            return Ok((next_state, result));
                        }
                        // No more batches in this row group, move to the next row group
                        None => {
                            current_state = Self::ReadingRowGroup {
                                remaining_row_groups,
                            }
                        }
                        // some error occurred while decoding, so return that
                        Some(Err(e)) => {
                            // TODO: preserve ArrowError in ParquetError (rather than convert to a string)
                            return Err(ParquetError::ArrowError(e.to_string()));
                        }
                    }
                }
                Self::Finished => {
                    return Ok((Self::Finished, DecodeResult::Finished));
                }
            }
        }
    }

    /// Transition to the next state with a reader (data can be produced), if not end of stream
    ///
    /// This function is called in a loop until the decoder is ready to return
    /// data (has the required pages buffered) or is finished.
    fn transition(self) -> Result<(Self, DecodeResult<()>), ParquetError> {
        // result returned when there is data ready
        let data_ready = DecodeResult::Data(());
        match self {
            Self::ReadingRowGroup {
                mut remaining_row_groups,
            } => {
                match remaining_row_groups.try_next_reader()? {
                    // If we have a next reader, we can transition to decoding it
                    DecodeResult::Data(record_batch_reader) => {
                        // Transition to decoding the row group
                        Ok((
                            Self::DecodingRowGroup {
                                record_batch_reader: Box::new(record_batch_reader),
                                remaining_row_groups,
                            },
                            data_ready,
                        ))
                    }
                    DecodeResult::NeedsData(ranges) => {
                        // If we need more data, we return the ranges needed and stay in Reading
                        // RowGroup state
                        Ok((
                            Self::ReadingRowGroup {
                                remaining_row_groups,
                            },
                            DecodeResult::NeedsData(ranges),
                        ))
                    }
                    // If there are no more readers, we are finished
                    DecodeResult::Finished => {
                        // No more row groups to read, we are finished
                        Ok((Self::Finished, DecodeResult::Finished))
                    }
                }
            }
            // if we are already in DecodingRowGroup, just return data ready
            Self::DecodingRowGroup { .. } => Ok((self, data_ready)),
            // if finished, just return finished
            Self::Finished => Ok((self, DecodeResult::Finished)),
        }
    }

    /// Push data, and transition state if needed
    ///
    /// This should correspond to the data ranges requested by the decoder
    pub fn push_data(
        self,
        ranges: Vec<Range<u64>>,
        data: Vec<Bytes>,
    ) -> Result<Self, ParquetError> {
        match self {
            ParquetDecoderState::ReadingRowGroup {
                mut remaining_row_groups,
            } => {
                // Push data to the RowGroupReaderBuilder
                remaining_row_groups.push_data(ranges, data);
                Ok(ParquetDecoderState::ReadingRowGroup {
                    remaining_row_groups,
                })
            }
            // it is ok to get data before we asked for it
            ParquetDecoderState::DecodingRowGroup {
                record_batch_reader,
                mut remaining_row_groups,
            } => {
                remaining_row_groups.push_data(ranges, data);
                Ok(ParquetDecoderState::DecodingRowGroup {
                    record_batch_reader,
                    remaining_row_groups,
                })
            }
            ParquetDecoderState::Finished => Err(ParquetError::General(
                "Cannot push data to a finished decoder".to_string(),
            )),
        }
    }

    /// How many bytes are currently buffered in the decoder?
    fn buffered_bytes(&self) -> u64 {
        match self {
            ParquetDecoderState::ReadingRowGroup {
                remaining_row_groups,
            } => remaining_row_groups.buffered_bytes(),
            ParquetDecoderState::DecodingRowGroup {
                record_batch_reader: _,
                remaining_row_groups,
            } => remaining_row_groups.buffered_bytes(),
            ParquetDecoderState::Finished => 0,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::DecodeResult;
    use crate::arrow::arrow_reader::{ArrowPredicateFn, RowFilter, RowSelection, RowSelector};
    use crate::arrow::push_decoder::{ParquetPushDecoder, ParquetPushDecoderBuilder};
    use crate::arrow::{ArrowWriter, ProjectionMask};
    use crate::errors::ParquetError;
    use crate::file::metadata::ParquetMetaDataPushDecoder;
    use crate::file::properties::WriterProperties;
    use arrow::compute::kernels::cmp::{gt, lt};
    use arrow_array::cast::AsArray;
    use arrow_array::types::Int64Type;
    use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringViewArray};
    use arrow_select::concat::concat_batches;
    use bytes::Bytes;
    use std::fmt::Debug;
    use std::ops::Range;
    use std::sync::{Arc, LazyLock};

    /// Test decoder struct size (as they are copied around on each transition, they
    /// should not grow too large)
    #[test]
    fn test_decoder_size() {
        assert_eq!(std::mem::size_of::<ParquetDecoderState>(), 24);
    }

    /// Decode the entire file at once, simulating a scenario where all data is
    /// available in memory
    #[test]
    fn test_decoder_all_data() {
        let mut decoder = ParquetPushDecoderBuilder::try_new_decoder(test_file_parquet_metadata())
            .unwrap()
            .build()
            .unwrap();

        decoder
            .push_range(test_file_range(), TEST_FILE_DATA.clone())
            .unwrap();

        let results = vec![
            // first row group should be decoded without needing more data
            expect_data(decoder.try_decode()),
            // second row group should be decoded without needing more data
            expect_data(decoder.try_decode()),
        ];
        expect_finished(decoder.try_decode());

        let all_output = concat_batches(&TEST_BATCH.schema(), &results).unwrap();
        // Check that the output matches the input batch
        assert_eq!(all_output, *TEST_BATCH);
    }

    /// Decode the entire file incrementally, simulating a scenario where data is
    /// fetched as needed
    #[test]
    fn test_decoder_incremental() {
        let mut decoder = ParquetPushDecoderBuilder::try_new_decoder(test_file_parquet_metadata())
            .unwrap()
            .build()
            .unwrap();

        let mut results = vec![];

        // First row group, expect a single request
        let ranges = expect_needs_data(decoder.try_decode());
        let num_bytes_requested: u64 = ranges.iter().map(|r| r.end - r.start).sum();
        push_ranges_to_decoder(&mut decoder, ranges);
        // The decoder should currently only store the data it needs to decode the first row group
        assert_eq!(decoder.buffered_bytes(), num_bytes_requested);
        results.push(expect_data(decoder.try_decode()));
        // the decoder should have consumed the data for the first row group and freed it
        assert_eq!(decoder.buffered_bytes(), 0);

        // Second row group,
        let ranges = expect_needs_data(decoder.try_decode());
        let num_bytes_requested: u64 = ranges.iter().map(|r| r.end - r.start).sum();
        push_ranges_to_decoder(&mut decoder, ranges);
        // The decoder should currently only store the data it needs to decode the second row group
        assert_eq!(decoder.buffered_bytes(), num_bytes_requested);
        results.push(expect_data(decoder.try_decode()));
        // the decoder should have consumed the data for the second row group and freed it
        assert_eq!(decoder.buffered_bytes(), 0);
        expect_finished(decoder.try_decode());

        // Check that the output matches the input batch
        let all_output = concat_batches(&TEST_BATCH.schema(), &results).unwrap();
        assert_eq!(all_output, *TEST_BATCH);
    }

    /// Decode the entire file incrementally, simulating partial reads
    #[test]
    fn test_decoder_partial() {
        let mut decoder = ParquetPushDecoderBuilder::try_new_decoder(test_file_parquet_metadata())
            .unwrap()
            .build()
            .unwrap();

        // First row group, expect a single request for all data needed to read "a" and "b"
        let ranges = expect_needs_data(decoder.try_decode());
        push_ranges_to_decoder(&mut decoder, ranges);

        let batch1 = expect_data(decoder.try_decode());
        let expected1 = TEST_BATCH.slice(0, 200);
        assert_eq!(batch1, expected1);

        // Second row group, this time provide the data in two steps
        let ranges = expect_needs_data(decoder.try_decode());
        let (ranges1, ranges2) = ranges.split_at(ranges.len() / 2);
        assert!(!ranges1.is_empty());
        assert!(!ranges2.is_empty());
        // push first half to simulate partial read
        push_ranges_to_decoder(&mut decoder, ranges1.to_vec());

        // still expect more data
        let ranges = expect_needs_data(decoder.try_decode());
        assert_eq!(ranges, ranges2); // should be the remaining ranges
        // push empty ranges should be a no-op
        push_ranges_to_decoder(&mut decoder, vec![]);
        let ranges = expect_needs_data(decoder.try_decode());
        assert_eq!(ranges, ranges2); // should be the remaining ranges
        push_ranges_to_decoder(&mut decoder, ranges);

        let batch2 = expect_data(decoder.try_decode());
        let expected2 = TEST_BATCH.slice(200, 200);
        assert_eq!(batch2, expected2);

        expect_finished(decoder.try_decode());
    }

    /// Decode multiple columns "a" and "b", expect that the decoder requests
    /// only a single request per row group
    #[test]
    fn test_decoder_selection_does_one_request() {
        let builder =
            ParquetPushDecoderBuilder::try_new_decoder(test_file_parquet_metadata()).unwrap();

        let schema_descr = builder.metadata().file_metadata().schema_descr_ptr();

        let mut decoder = builder
            .with_projection(
                ProjectionMask::columns(&schema_descr, ["a", "b"]), // read "a", "b"
            )
            .build()
            .unwrap();

        // First row group, expect a single request for all data needed to read "a" and "b"
        let ranges = expect_needs_data(decoder.try_decode());
        push_ranges_to_decoder(&mut decoder, ranges);

        let batch1 = expect_data(decoder.try_decode());
        let expected1 = TEST_BATCH.slice(0, 200).project(&[0, 1]).unwrap();
        assert_eq!(batch1, expected1);

        // Second row group, similarly expect a single request for all data needed to read "a" and "b"
        let ranges = expect_needs_data(decoder.try_decode());
        push_ranges_to_decoder(&mut decoder, ranges);

        let batch2 = expect_data(decoder.try_decode());
        let expected2 = TEST_BATCH.slice(200, 200).project(&[0, 1]).unwrap();
        assert_eq!(batch2, expected2);

        expect_finished(decoder.try_decode());
    }

    /// Decode with a filter that requires multiple requests, but only provide part
    /// of the data needed for the filter at a time simulating partial reads.
    #[test]
    fn test_decoder_single_filter_partial() {
        let builder =
            ParquetPushDecoderBuilder::try_new_decoder(test_file_parquet_metadata()).unwrap();

        // Values in column "a" range 0..399
        // First filter: "a" > 250  (nothing in Row Group 0, both data pages in Row Group 1)
        let schema_descr = builder.metadata().file_metadata().schema_descr_ptr();

        // a > 250
        let row_filter_a = ArrowPredicateFn::new(
            // claim to use both a and b so we get two ranges requests for the filter pages
            ProjectionMask::columns(&schema_descr, ["a", "b"]),
            |batch: RecordBatch| {
                let scalar_250 = Int64Array::new_scalar(250);
                let column = batch.column(0).as_primitive::<Int64Type>();
                gt(column, &scalar_250)
            },
        );

        let mut decoder = builder
            .with_projection(
                // read only column "a" to test that filter pages are reused
                ProjectionMask::columns(&schema_descr, ["a"]), // read "a"
            )
            .with_row_filter(RowFilter::new(vec![Box::new(row_filter_a)]))
            .build()
            .unwrap();

        // First row group, evaluating filters
        let ranges = expect_needs_data(decoder.try_decode());
        // only provide half the ranges
        let (ranges1, ranges2) = ranges.split_at(ranges.len() / 2);
        assert!(!ranges1.is_empty());
        assert!(!ranges2.is_empty());
        push_ranges_to_decoder(&mut decoder, ranges1.to_vec());
        // still expect more data
        let ranges = expect_needs_data(decoder.try_decode());
        assert_eq!(ranges, ranges2); // should be the remaining ranges
        let ranges = expect_needs_data(decoder.try_decode());
        assert_eq!(ranges, ranges2); // should be the remaining ranges
        push_ranges_to_decoder(&mut decoder, ranges2.to_vec());

        // Since no rows in the first row group pass the filters, there is no
        // additional requests to read data pages for "b" here

        // Second row group
        let ranges = expect_needs_data(decoder.try_decode());
        push_ranges_to_decoder(&mut decoder, ranges);

        let batch = expect_data(decoder.try_decode());
        let expected = TEST_BATCH.slice(251, 149).project(&[0]).unwrap();
        assert_eq!(batch, expected);

        expect_finished(decoder.try_decode());
    }

    /// Decode with a filter where we also skip one of the RowGroups via a RowSelection
    #[test]
    fn test_decoder_single_filter_and_row_selection() {
        let builder =
            ParquetPushDecoderBuilder::try_new_decoder(test_file_parquet_metadata()).unwrap();

        // Values in column "a" range 0..399
        // First filter: "a" > 250  (nothing in Row Group 0, last data page in Row Group 1)
        let schema_descr = builder.metadata().file_metadata().schema_descr_ptr();

        // a > 250
        let row_filter_a = ArrowPredicateFn::new(
            ProjectionMask::columns(&schema_descr, ["a"]),
            |batch: RecordBatch| {
                let scalar_250 = Int64Array::new_scalar(250);
                let column = batch.column(0).as_primitive::<Int64Type>();
                gt(column, &scalar_250)
            },
        );

        let mut decoder = builder
            .with_projection(
                // read only column "a" to test that filter pages are reused
                ProjectionMask::columns(&schema_descr, ["b"]), // read "b"
            )
            .with_row_filter(RowFilter::new(vec![Box::new(row_filter_a)]))
            .with_row_selection(RowSelection::from(vec![
                RowSelector::skip(200),   // skip first row group
                RowSelector::select(100), // first 100 rows of second row group
                RowSelector::skip(100),
            ]))
            .build()
            .unwrap();

        // expect the first row group to be filtered out (no filter is evaluated due to row selection)

        // First row group, first filter (a > 250)
        let ranges = expect_needs_data(decoder.try_decode());
        push_ranges_to_decoder(&mut decoder, ranges);

        // Second row group
        let ranges = expect_needs_data(decoder.try_decode());
        push_ranges_to_decoder(&mut decoder, ranges);

        let batch = expect_data(decoder.try_decode());
        let expected = TEST_BATCH.slice(251, 49).project(&[1]).unwrap();
        assert_eq!(batch, expected);

        expect_finished(decoder.try_decode());
    }

    /// Decode with multiple filters that require multiple requests
    #[test]
    fn test_decoder_multi_filters() {
        // Create a decoder for decoding parquet data (note it does not have any IO / readers)
        let builder =
            ParquetPushDecoderBuilder::try_new_decoder(test_file_parquet_metadata()).unwrap();

        // Values in column "a" range 0..399
        // Values in column "b" range 400..799
        // First filter: "a" > 175  (last data page in Row Group 0)
        // Second filter: "b" < 625 (last data page in Row Group 0 and first DataPage in RowGroup 1)
        let schema_descr = builder.metadata().file_metadata().schema_descr_ptr();

        // a > 175
        let row_filter_a = ArrowPredicateFn::new(
            ProjectionMask::columns(&schema_descr, ["a"]),
            |batch: RecordBatch| {
                let scalar_175 = Int64Array::new_scalar(175);
                let column = batch.column(0).as_primitive::<Int64Type>();
                gt(column, &scalar_175)
            },
        );

        // b < 625
        let row_filter_b = ArrowPredicateFn::new(
            ProjectionMask::columns(&schema_descr, ["b"]),
            |batch: RecordBatch| {
                let scalar_625 = Int64Array::new_scalar(625);
                let column = batch.column(0).as_primitive::<Int64Type>();
                lt(column, &scalar_625)
            },
        );

        let mut decoder = builder
            .with_projection(
                ProjectionMask::columns(&schema_descr, ["c"]), // read "c"
            )
            .with_row_filter(RowFilter::new(vec![
                Box::new(row_filter_a),
                Box::new(row_filter_b),
            ]))
            .build()
            .unwrap();

        // First row group, first filter (a > 175)
        let ranges = expect_needs_data(decoder.try_decode());
        push_ranges_to_decoder(&mut decoder, ranges);

        // first row group, second filter (b < 625)
        let ranges = expect_needs_data(decoder.try_decode());
        push_ranges_to_decoder(&mut decoder, ranges);

        // first row group, data pages for "c"
        let ranges = expect_needs_data(decoder.try_decode());
        push_ranges_to_decoder(&mut decoder, ranges);

        // expect the first batch to be decoded: rows 176..199, column "c"
        let batch1 = expect_data(decoder.try_decode());
        let expected1 = TEST_BATCH.slice(176, 24).project(&[2]).unwrap();
        assert_eq!(batch1, expected1);

        // Second row group, first filter (a > 175)
        let ranges = expect_needs_data(decoder.try_decode());
        push_ranges_to_decoder(&mut decoder, ranges);

        // Second row group, second filter (b < 625)
        let ranges = expect_needs_data(decoder.try_decode());
        push_ranges_to_decoder(&mut decoder, ranges);

        // Second row group, data pages for "c"
        let ranges = expect_needs_data(decoder.try_decode());
        push_ranges_to_decoder(&mut decoder, ranges);

        // expect the second batch to be decoded: rows 200..224, column "c"
        let batch2 = expect_data(decoder.try_decode());
        let expected2 = TEST_BATCH.slice(200, 25).project(&[2]).unwrap();
        assert_eq!(batch2, expected2);

        expect_finished(decoder.try_decode());
    }

    /// Decode with a filter that uses a column that is also projected, and expect
    /// that the filter pages are reused (don't refetch them)
    #[test]
    fn test_decoder_reuses_filter_pages() {
        // Create a decoder for decoding parquet data (note it does not have any IO / readers)
        let builder =
            ParquetPushDecoderBuilder::try_new_decoder(test_file_parquet_metadata()).unwrap();

        // Values in column "a" range 0..399
        // First filter: "a" > 250  (nothing in Row Group 0, last data page in Row Group 1)
        let schema_descr = builder.metadata().file_metadata().schema_descr_ptr();

        // a > 250
        let row_filter_a = ArrowPredicateFn::new(
            ProjectionMask::columns(&schema_descr, ["a"]),
            |batch: RecordBatch| {
                let scalar_250 = Int64Array::new_scalar(250);
                let column = batch.column(0).as_primitive::<Int64Type>();
                gt(column, &scalar_250)
            },
        );

        let mut decoder = builder
            .with_projection(
                // read only column "a" to test that filter pages are reused
                ProjectionMask::columns(&schema_descr, ["a"]), // read "a"
            )
            .with_row_filter(RowFilter::new(vec![Box::new(row_filter_a)]))
            .build()
            .unwrap();

        // First row group, first filter (a > 175)
        let ranges = expect_needs_data(decoder.try_decode());
        push_ranges_to_decoder(&mut decoder, ranges);

        // expect the first row group to be filtered out (no rows match)

        // Second row group, first filter (a > 250)
        let ranges = expect_needs_data(decoder.try_decode());
        push_ranges_to_decoder(&mut decoder, ranges);

        // expect that the second row group is decoded: rows 251..399, column "a"
        // Note that the filter pages for "a" should be reused and no additional data
        // should be requested
        let batch = expect_data(decoder.try_decode());
        let expected = TEST_BATCH.slice(251, 149).project(&[0]).unwrap();
        assert_eq!(batch, expected);

        expect_finished(decoder.try_decode());
    }

    #[test]
    fn test_decoder_empty_filters() {
        let builder =
            ParquetPushDecoderBuilder::try_new_decoder(test_file_parquet_metadata()).unwrap();
        let schema_descr = builder.metadata().file_metadata().schema_descr_ptr();

        // only read column "c", but with empty filters
        let mut decoder = builder
            .with_projection(
                ProjectionMask::columns(&schema_descr, ["c"]), // read "c"
            )
            .with_row_filter(RowFilter::new(vec![
                // empty filters should be ignored
            ]))
            .build()
            .unwrap();

        // First row group
        let ranges = expect_needs_data(decoder.try_decode());
        push_ranges_to_decoder(&mut decoder, ranges);

        // expect the first batch to be decoded: rows 0..199, column "c"
        let batch1 = expect_data(decoder.try_decode());
        let expected1 = TEST_BATCH.slice(0, 200).project(&[2]).unwrap();
        assert_eq!(batch1, expected1);

        // Second row group,
        let ranges = expect_needs_data(decoder.try_decode());
        push_ranges_to_decoder(&mut decoder, ranges);

        // expect the second batch to be decoded: rows 200..399, column "c"
        let batch2 = expect_data(decoder.try_decode());
        let expected2 = TEST_BATCH.slice(200, 200).project(&[2]).unwrap();

        assert_eq!(batch2, expected2);

        expect_finished(decoder.try_decode());
    }

    #[test]
    fn test_decoder_offset_limit() {
        let mut decoder = ParquetPushDecoderBuilder::try_new_decoder(test_file_parquet_metadata())
            .unwrap()
            // skip entire first row group (200 rows) and first 25 rows of second row group
            .with_offset(225)
            // and limit to 20 rows
            .with_limit(20)
            .build()
            .unwrap();

        // First row group should be skipped,

        // Second row group
        let ranges = expect_needs_data(decoder.try_decode());
        push_ranges_to_decoder(&mut decoder, ranges);

        // expect the first and only batch to be decoded
        let batch1 = expect_data(decoder.try_decode());
        let expected1 = TEST_BATCH.slice(225, 20);
        assert_eq!(batch1, expected1);

        expect_finished(decoder.try_decode());
    }

    #[test]
    fn test_decoder_row_group_selection() {
        // take only the second row group
        let mut decoder = ParquetPushDecoderBuilder::try_new_decoder(test_file_parquet_metadata())
            .unwrap()
            .with_row_groups(vec![1])
            .build()
            .unwrap();

        // First row group should be skipped,

        // Second row group
        let ranges = expect_needs_data(decoder.try_decode());
        push_ranges_to_decoder(&mut decoder, ranges);

        // expect the first and only batch to be decoded
        let batch1 = expect_data(decoder.try_decode());
        let expected1 = TEST_BATCH.slice(200, 200);
        assert_eq!(batch1, expected1);

        expect_finished(decoder.try_decode());
    }

    #[test]
    fn test_decoder_row_selection() {
        // take only the second row group
        let mut decoder = ParquetPushDecoderBuilder::try_new_decoder(test_file_parquet_metadata())
            .unwrap()
            .with_row_selection(RowSelection::from(vec![
                RowSelector::skip(225),  // skip first row group and 25 rows of second])
                RowSelector::select(20), // take 20 rows
            ]))
            .build()
            .unwrap();

        // First row group should be skipped,

        // Second row group
        let ranges = expect_needs_data(decoder.try_decode());
        push_ranges_to_decoder(&mut decoder, ranges);

        // expect the first ane only batch to be decoded
        let batch1 = expect_data(decoder.try_decode());
        let expected1 = TEST_BATCH.slice(225, 20);
        assert_eq!(batch1, expected1);

        expect_finished(decoder.try_decode());
    }

    /// Returns a batch with 400 rows, with 3 columns: "a", "b", "c"
    ///
    /// Note c is a different types (so the data page sizes will be different)
    static TEST_BATCH: LazyLock<RecordBatch> = LazyLock::new(|| {
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

    /// Create a parquet file in memory for testing.
    ///
    /// See [`TEST_BATCH`] for the data in the file.
    ///
    /// Each column is written in 4 data pages, each with 100 rows, across 2
    /// row groups. Each column in each row group has two data pages.
    ///
    /// The data is split across row groups like this
    ///
    /// Column |   Values                | Data Page | Row Group
    /// -------|------------------------|-----------|-----------
    /// a      | 0..99                  | 1         | 0
    /// a      | 100..199               | 2         | 0
    /// a      | 200..299               | 1         | 1
    /// a      | 300..399               | 2         | 1
    ///
    /// b      | 400..499               | 1         | 0
    /// b      | 500..599               | 2         | 0
    /// b      | 600..699               | 1         | 1
    /// b      | 700..799               | 2         | 1
    ///
    /// c      | "string_0".."string_99"        | 1         | 0
    /// c      | "string_100".."string_199"     | 2         | 0
    /// c      | "string_200".."string_299"     | 1         | 1
    /// c      | "string_300".."string_399"     | 2         | 1
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

    /// Return the length of [`TEST_FILE_DATA`], in bytes
    fn test_file_len() -> u64 {
        TEST_FILE_DATA.len() as u64
    }

    /// Return a range that covers the entire [`TEST_FILE_DATA`]
    fn test_file_range() -> Range<u64> {
        0..test_file_len()
    }

    /// Return a slice of the test file data from the given range
    pub fn test_file_slice(range: Range<u64>) -> Bytes {
        let start: usize = range.start.try_into().unwrap();
        let end: usize = range.end.try_into().unwrap();
        TEST_FILE_DATA.slice(start..end)
    }

    /// return the metadata for the test file
    pub fn test_file_parquet_metadata() -> Arc<crate::file::metadata::ParquetMetaData> {
        let mut metadata_decoder = ParquetMetaDataPushDecoder::try_new(test_file_len()).unwrap();
        push_ranges_to_metadata_decoder(&mut metadata_decoder, vec![test_file_range()]);
        let metadata = metadata_decoder.try_decode().unwrap();
        let DecodeResult::Data(metadata) = metadata else {
            panic!("Expected metadata to be decoded successfully");
        };
        Arc::new(metadata)
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

    fn push_ranges_to_decoder(decoder: &mut ParquetPushDecoder, ranges: Vec<Range<u64>>) {
        let data = ranges
            .iter()
            .map(|range| test_file_slice(range.clone()))
            .collect::<Vec<_>>();
        decoder.push_ranges(ranges, data).unwrap();
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
