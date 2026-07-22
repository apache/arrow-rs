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
pub use crate::util::push_buffers::PushBuffers;
use arrow_array::RecordBatch;
use bytes::Bytes;
use reader_builder::{RowBudget, RowGroupReaderBuilder, RowGroupReaderBuilderParts};
use remaining::{RemainingRowGroups, RemainingRowGroupsParts};
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
///
/// # Adaptive scans
///
/// The scan strategy is not fixed once [`build`](Self::build) is called: it
/// can be changed *while decoding*, at row-group boundaries.
///
/// The important API for this is [`ParquetPushDecoder::try_next_reader`].
/// Unlike [`try_decode`](ParquetPushDecoder::try_decode), which barrels
/// straight through row-group boundaries, `try_next_reader` returns once per
/// row group — leaving a clean window *between* row groups. At any such
/// boundary, [`ParquetPushDecoder::into_builder`] hands back a
/// `ParquetPushDecoderBuilder` for the row groups not yet decoded. Change any
/// option on it (projection, row filter, row selection policy, …) and
/// [`build`](Self::build) a fresh decoder that resumes from the next row
/// group. This is how a query engine promotes or demotes filters — for
/// example turning a row filter on or off — based on the selectivity observed
/// in the row groups decoded so far.
///
/// ```
/// # use std::ops::Range;
/// # use std::sync::Arc;
/// # use bytes::Bytes;
/// # use arrow_array::record_batch;
/// # use parquet::DecodeResult;
/// # use parquet::arrow::ProjectionMask;
/// # use parquet::arrow::push_decoder::ParquetPushDecoderBuilder;
/// # use parquet::arrow::ArrowWriter;
/// # use parquet::file::metadata::ParquetMetaDataPushDecoder;
/// # use parquet::file::properties::WriterProperties;
/// # let file_bytes = {
/// #   let batch = record_batch!(
/// #       ("a", Int32, [1, 2, 3, 4, 5, 6]),
/// #       ("b", Int32, [6, 5, 4, 3, 2, 1])
/// #   ).unwrap();
/// #   // Small row groups so the test file has two of them.
/// #   let props = WriterProperties::builder().set_max_row_group_row_count(Some(3)).build();
/// #   let mut buffer = vec![];
/// #   let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props)).unwrap();
/// #   writer.write(&batch).unwrap();
/// #   writer.close().unwrap();
/// #   Bytes::from(buffer)
/// # };
/// # let get_range = |r: &Range<u64>| file_bytes.slice(r.start as usize..r.end as usize);
/// # let file_length = file_bytes.len() as u64;
/// # let mut metadata_decoder = ParquetMetaDataPushDecoder::try_new(file_length).unwrap();
/// # metadata_decoder.push_ranges(vec![0..file_length], vec![file_bytes.clone()]).unwrap();
/// # let DecodeResult::Data(parquet_metadata) = metadata_decoder.try_decode().unwrap() else { panic!() };
/// # let parquet_metadata = Arc::new(parquet_metadata);
/// let mut decoder = ParquetPushDecoderBuilder::try_new_decoder(parquet_metadata)
///     .unwrap()
///     .build()
///     .unwrap();
///
/// // Drive the decoder one row group at a time with `try_next_reader`.
/// loop {
///     match decoder.try_next_reader().unwrap() {
///         DecodeResult::NeedsData(ranges) => {
///             // Fetch and hand over the bytes the decoder asked for.
///             let data = ranges.iter().map(|r| get_range(r)).collect();
///             decoder.push_ranges(ranges, data).unwrap();
///         }
///         DecodeResult::Data(reader) => {
///             // Decode this row group's batches.
///             for batch in reader {
///                 assert!(batch.unwrap().num_rows() > 0);
///             }
///             // We are now at a row-group boundary. Based on whatever stats
///             // were gathered, optionally change strategy for the row groups
///             // still to come: drop or promote a row filter, narrow or widen
///             // the projection, etc.
///             if decoder.is_at_row_group_boundary() && decoder.row_groups_remaining() > 0 {
///                 let builder = decoder.into_builder().unwrap();
///                 // e.g. column "b" turned out not to be needed.
///                 let projection = ProjectionMask::columns(builder.parquet_schema(), ["a"]);
///                 decoder = builder.with_projection(projection).build().unwrap();
///             }
///         }
///         DecodeResult::Finished => break,
///     }
/// }
/// ```
pub type ParquetPushDecoderBuilder = ArrowReaderBuilder<PushDecoderInput>;

/// The `input` of a [`ParquetPushDecoderBuilder`].
///
/// The shared [`ArrowReaderBuilder`] is generic over an `input`. The sync and
/// async builders read from a file or async reader; the push decoder has no
/// reader, so its input is the [`PushBuffers`] that caller-pushed bytes
/// accumulate in (empty for a fresh builder).
#[derive(Debug, Default)]
pub struct PushDecoderInput {
    /// Bytes pushed into the decoder, awaiting decode.
    buffers: PushBuffers,
}

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
        Self::new_builder(PushDecoderInput::default(), arrow_reader_metadata)
    }

    /// Provide a preexisting [`PushBuffers`] for the built decoder to read
    /// from, so bytes already fetched are not requested again.
    pub fn with_buffers(self, buffers: PushBuffers) -> Self {
        Self {
            input: PushDecoderInput { buffers },
            ..self
        }
    }

    /// Create a [`ParquetPushDecoder`] with the configured options
    pub fn build(self) -> Result<ParquetPushDecoder, ParquetError> {
        let Self {
            input: PushDecoderInput { buffers },
            metadata: parquet_metadata,
            schema,
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
        let has_predicates = filter
            .as_ref()
            .is_some_and(|filter| !filter.predicates.is_empty());

        // Prepare to build RowGroup readers. `buffers` carries any bytes the
        // caller already pushed (preserved across `into_builder`); a fresh
        // builder supplies an empty `PushBuffers`.
        let row_group_reader_builder = RowGroupReaderBuilder::new(
            batch_size,
            projection,
            Arc::clone(&parquet_metadata),
            fields,
            filter,
            metrics,
            max_predicate_cache_size,
            buffers,
            row_selection_policy,
        );

        // Initialize the decoder with the configured options
        let remaining_row_groups = RemainingRowGroups::new(
            schema,
            parquet_metadata,
            row_groups,
            selection,
            RowBudget::new(offset, limit),
            has_predicates,
            row_group_reader_builder,
        );

        Ok(ParquetPushDecoder {
            state: ParquetDecoderState::ReadingRowGroup {
                remaining_row_groups: Box::new(remaining_row_groups),
            },
        })
    }
}

/// Reassemble a [`ParquetPushDecoderBuilder`] from a decoder's not-yet-decoded
/// state — the inverse of [`ParquetPushDecoderBuilder::build`]. The rebuilt
/// builder pins the remaining row groups and carries the remaining row
/// selection, offset/limit budget, and buffered bytes.
fn builder_from_remaining(parts: RemainingRowGroupsParts) -> ParquetPushDecoderBuilder {
    let RemainingRowGroupsParts {
        metadata,
        schema,
        row_groups,
        selection,
        offset,
        limit,
        reader_builder,
    } = parts;
    let RowGroupReaderBuilderParts {
        batch_size,
        projection,
        fields,
        filter,
        max_predicate_cache_size,
        metrics,
        row_selection_policy,
        buffers,
    } = reader_builder;

    ArrowReaderBuilder {
        input: PushDecoderInput::default(),
        metadata,
        schema,
        fields,
        batch_size,
        // The frontier tracks remaining row groups explicitly, so the rebuilt
        // builder always pins them (even if the original left `row_groups` as
        // `None` meaning "all").
        row_groups: Some(row_groups),
        projection,
        filter,
        selection,
        row_selection_policy,
        limit,
        offset,
        metrics,
        max_predicate_cache_size,
    }
    // Carry the decoder's already-fetched bytes across the rebuild so the new
    // decoder does not re-request them.
    .with_buffers(buffers)
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

    /// Clear any staged byte ranges currently buffered for future decode work.
    ///
    /// This clears byte ranges still owned by the decoder's internal
    /// `PushBuffers`. It does not affect any data that has already been handed
    /// off to an active [`ParquetRecordBatchReader`].
    pub fn clear_all_ranges(&mut self) {
        self.state.clear_all_ranges();
    }

    /// True iff the decoder is at a row-group boundary, where
    /// [`Self::into_builder`] can reconfigure the scan.
    ///
    /// A boundary is "between row groups": the previous row group's
    /// [`ParquetRecordBatchReader`] has been fully extracted (via
    /// [`Self::try_next_reader`]) or fully drained (via [`Self::try_decode`]),
    /// and the next row group has not yet been planned. While
    /// [`Self::try_decode`] is iterating an active row group's reader this
    /// returns `false`; with [`Self::try_next_reader`] there is a clean
    /// window between two consecutive returns where this is `true`.
    pub fn is_at_row_group_boundary(&self) -> bool {
        self.state.is_at_row_group_boundary()
    }

    /// Number of row groups left to decode after the one currently in flight.
    /// Useful as a "should I bother reconfiguring the scan?" signal.
    pub fn row_groups_remaining(&self) -> usize {
        self.state.row_groups_remaining()
    }

    /// Returns the row-group index that the next call to
    /// [`Self::try_next_reader`] will yield a reader for, after applying
    /// any internal skipping (row selection emptiness, exhausted budget,
    /// finished state).
    ///
    /// Safe to call at any time. When called mid-row-group (i.e. while a
    /// previously-emitted [`ParquetRecordBatchReader`] is still being
    /// drained), the returned index refers to the row group that
    /// [`Self::try_next_reader`] will produce *after* the current one.
    ///
    /// Returns `Ok(None)` when:
    /// - the decoder has no more row groups to read, or
    /// - every remaining row group would be skipped.
    ///
    /// This method does not mutate decoder state. It is useful for
    /// callers that maintain per-row-group state in lock-step with the
    /// decoder (e.g. dynamic row-group pruners) to determine which row
    /// group the next reader corresponds to, since
    /// [`Self::try_next_reader`] may silently advance past row groups
    /// based on filtering and other criteria.
    pub fn peek_next_row_group(&self) -> Result<Option<usize>, ParquetError> {
        self.state.peek_next_row_group()
    }

    /// Decompose this decoder back into a [`ParquetPushDecoderBuilder`] for the
    /// row groups that have *not* yet been decoded.
    ///
    /// This is the API for *adaptive* scans. Drive the decoder with
    /// [`Self::try_next_reader`]; at any row-group boundary, call
    /// `into_builder` to recover a builder, adjust it with the usual
    /// [`ParquetPushDecoderBuilder`] setters, and
    /// [`build`](ParquetPushDecoderBuilder::build) a fresh decoder that resumes
    /// from the next row group:
    ///
    /// ```no_run
    /// # use parquet::arrow::push_decoder::ParquetPushDecoder;
    /// # use parquet::arrow::arrow_reader::RowFilter;
    /// # fn get_decoder() -> ParquetPushDecoder { unimplemented!() }
    /// # fn new_filter() -> RowFilter { unimplemented!() }
    /// let mut decoder = get_decoder();
    /// // ... drive `decoder.try_next_reader()` for a few row groups ...
    /// if decoder.is_at_row_group_boundary() && decoder.row_groups_remaining() > 0 {
    ///     decoder = decoder
    ///         .into_builder()
    ///         .unwrap()
    ///         // any builder option can be changed here, e.g. promote a
    ///         // filter into a row filter based on observed selectivity
    ///         .with_row_filter(new_filter())
    ///         .build()
    ///         .unwrap();
    /// }
    /// ```
    ///
    /// The returned builder pins the not-yet-decoded row groups (via
    /// [`with_row_groups`](ArrowReaderBuilder::with_row_groups)) and carries the
    /// not-yet-consumed row selection and offset/limit budget, so rows from
    /// already-decoded row groups are not produced again. Every other option —
    /// projection, row filter, row selection policy, batch size, metrics,
    /// predicate-cache size — is left exactly as the decoder had it and can be
    /// overridden before [`build`](ParquetPushDecoderBuilder::build).
    ///
    /// # Errors
    ///
    /// Returns `Err(ParquetError::General)` when the decoder is not at a
    /// row-group boundary (check [`Self::is_at_row_group_boundary`] first) or
    /// has already finished. The decoder is consumed either way.
    ///
    /// # Buffered bytes
    ///
    /// The decoder's buffered bytes are carried across the rebuild: bytes
    /// already fetched for row groups the new configuration still reads are
    /// not re-requested. Bytes the new configuration no longer needs stay
    /// buffered until [`clear_all_ranges`](Self::clear_all_ranges) is called
    /// or the rebuilt decoder is dropped.
    pub fn into_builder(self) -> Result<ParquetPushDecoderBuilder, ParquetError> {
        self.state.into_builder()
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

    /// Clear any staged ranges currently buffered in the decoder.
    fn clear_all_ranges(&mut self) {
        match self {
            ParquetDecoderState::ReadingRowGroup {
                remaining_row_groups,
            } => remaining_row_groups.clear_all_ranges(),
            ParquetDecoderState::DecodingRowGroup {
                record_batch_reader: _,
                remaining_row_groups,
            } => remaining_row_groups.clear_all_ranges(),
            ParquetDecoderState::Finished => {}
        }
    }

    fn is_at_row_group_boundary(&self) -> bool {
        match self {
            ParquetDecoderState::ReadingRowGroup {
                remaining_row_groups,
            } => remaining_row_groups.is_at_row_group_boundary(),
            // Mid-row-group: the active reader holds an `ArrayReader` and
            // `ReadPlan` keyed to the *current* projection/filter; rebuilding
            // would require throwing that work away.
            ParquetDecoderState::DecodingRowGroup { .. } => false,
            ParquetDecoderState::Finished => false,
        }
    }

    fn row_groups_remaining(&self) -> usize {
        match self {
            ParquetDecoderState::ReadingRowGroup {
                remaining_row_groups,
            } => remaining_row_groups.row_groups_remaining(),
            ParquetDecoderState::DecodingRowGroup {
                remaining_row_groups,
                ..
            } => remaining_row_groups.row_groups_remaining(),
            ParquetDecoderState::Finished => 0,
        }
    }

    /// See [`ParquetPushDecoder::peek_next_row_group`] for the public
    /// API contract. This inner method delegates to the underlying
    /// [`RemainingRowGroups`] for both `ReadingRowGroup` and
    /// `DecodingRowGroup`: mid-row-group the answer is the row group
    /// `try_next_reader` will produce *after* the active one finishes,
    /// which is exactly what `RemainingRowGroups::peek_next_row_group`
    /// computes from the queued frontier.
    fn peek_next_row_group(&self) -> Result<Option<usize>, ParquetError> {
        match self {
            ParquetDecoderState::ReadingRowGroup {
                remaining_row_groups,
            } => remaining_row_groups.peek_next_row_group(),
            ParquetDecoderState::DecodingRowGroup {
                remaining_row_groups,
                ..
            } => remaining_row_groups.peek_next_row_group(),
            ParquetDecoderState::Finished => Ok(None),
        }
    }

    fn into_builder(self) -> Result<ParquetPushDecoderBuilder, ParquetError> {
        let remaining_row_groups = match self {
            ParquetDecoderState::ReadingRowGroup {
                remaining_row_groups,
            } => remaining_row_groups,
            ParquetDecoderState::DecodingRowGroup { .. } => {
                return Err(ParquetError::General(
                    "into_builder called while a row group is being decoded; \
                     check is_at_row_group_boundary() first"
                        .to_string(),
                ));
            }
            ParquetDecoderState::Finished => {
                return Err(ParquetError::General(
                    "into_builder called on a finished decoder".to_string(),
                ));
            }
        };
        if !remaining_row_groups.is_at_row_group_boundary() {
            return Err(ParquetError::General(
                "into_builder called mid-row-group; check is_at_row_group_boundary() first"
                    .to_string(),
            ));
        }
        Ok(builder_from_remaining(remaining_row_groups.into_parts()))
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

    /// Releasing staged ranges should free speculative buffers without affecting
    /// the active row group reader.
    #[test]
    fn test_decoder_clear_all_ranges() {
        let mut decoder = ParquetPushDecoderBuilder::try_new_decoder(test_file_parquet_metadata())
            .unwrap()
            .with_batch_size(100)
            .build()
            .unwrap();

        decoder
            .push_range(test_file_range(), TEST_FILE_DATA.clone())
            .unwrap();
        assert_eq!(decoder.buffered_bytes(), test_file_len());

        // The current row group reader is built from the prefetched bytes, but
        // the speculative full-file range remains staged in the decoder.
        let batch1 = expect_data(decoder.try_decode());
        assert_eq!(batch1, TEST_BATCH.slice(0, 100));
        assert_eq!(decoder.buffered_bytes(), test_file_len());

        // All of the buffer is released
        decoder.clear_all_ranges();
        assert_eq!(decoder.buffered_bytes(), 0);

        // The active reader still owns the current row group's bytes, so it can
        // continue decoding without consulting PushBuffers.
        let batch2 = expect_data(decoder.try_decode());
        assert_eq!(batch2, TEST_BATCH.slice(100, 100));
        assert_eq!(decoder.buffered_bytes(), 0);

        // Moving to the next row group now requires the decoder to ask for data
        // again because the staged speculative ranges were released.
        let ranges = expect_needs_data(decoder.try_decode());
        let num_bytes_requested: u64 = ranges.iter().map(|r| r.end - r.start).sum();
        push_ranges_to_decoder(&mut decoder, ranges);
        assert_eq!(decoder.buffered_bytes(), num_bytes_requested);

        let batch3 = expect_data(decoder.try_decode());
        assert_eq!(batch3, TEST_BATCH.slice(200, 100));
        assert_eq!(decoder.buffered_bytes(), 0);

        let batch4 = expect_data(decoder.try_decode());
        assert_eq!(batch4, TEST_BATCH.slice(300, 100));
        assert_eq!(decoder.buffered_bytes(), 0);

        expect_finished(decoder.try_decode());
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

    /// When filter pushdown is combined with a `LIMIT`, the predicate must
    /// not be evaluated for rows beyond the `limit`-th match.
    ///
    /// Filter `a > 175` produces 24 matches in row group 0 (rows 176..199).
    /// With `limit = 10`, only the first 10 matches (rows 176..185) should be
    /// emitted, AND the predicate counter should observe that evaluation was
    /// short-circuited.
    #[test]
    fn test_decoder_filter_with_limit_short_circuits_within_row_group() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let builder =
            ParquetPushDecoderBuilder::try_new_decoder(test_file_parquet_metadata()).unwrap();
        let schema_descr = builder.metadata().file_metadata().schema_descr_ptr();

        let rows_filtered = Arc::new(AtomicUsize::new(0));
        let rows_filtered_for_predicate = Arc::clone(&rows_filtered);

        let row_filter_a = ArrowPredicateFn::new(
            ProjectionMask::columns(&schema_descr, ["a"]),
            move |batch: RecordBatch| {
                rows_filtered_for_predicate.fetch_add(batch.num_rows(), Ordering::Relaxed);
                let scalar_175 = Int64Array::new_scalar(175);
                let column = batch.column(0).as_primitive::<Int64Type>();
                gt(column, &scalar_175)
            },
        );

        // Use a small batch size so the row group is evaluated across
        // multiple predicate batches; that is the regime where Layer 2's
        // short-circuit saves predicate evaluation work. Matching rows are
        // 176..199 (24 rows); with batch_size = 10 those span batches 17, 18,
        // and 19 (rows 170..199). A limit of 10 should stop filter evaluation
        // in the middle of batch 18.
        let mut decoder = builder
            .with_projection(ProjectionMask::columns(&schema_descr, ["a"]))
            .with_row_filter(RowFilter::new(vec![Box::new(row_filter_a)]))
            .with_batch_size(10)
            .with_limit(10)
            .build()
            .unwrap();

        // First row group: filter columns fetch (predicate is evaluated here)
        let ranges = expect_needs_data(decoder.try_decode());
        push_ranges_to_decoder(&mut decoder, ranges);

        // The first 10 matching rows come out: 176..185, column "a"
        let batch = expect_data(decoder.try_decode());
        let expected = TEST_BATCH.slice(176, 10).project(&[0]).unwrap();
        assert_eq!(batch, expected);

        // no data for row group 1 should be requested — the limit
        // was satisfied by row group 0 and the `Start` state for row group 1
        // short-circuits to `Finished`.
        expect_finished(decoder.try_decode());

        // Row 186 is the 11th match; the scan should stop no later than the
        // batch containing it (batch 18 of 10 rows = rows 180..189), so at
        // most 190 rows are evaluated.
        let evaluated = rows_filtered.load(Ordering::Relaxed);
        assert!(
            evaluated <= 190,
            "predicate evaluated {evaluated} rows; expected ≤ 190 (stop within batch containing 11th match)"
        );
    }

    /// Once the limit has been satisfied by a prior row group, subsequent
    /// row groups should be skipped entirely — no data request for their
    /// filter columns.
    #[test]
    fn test_decoder_filter_with_limit_skips_later_row_groups() {
        let builder =
            ParquetPushDecoderBuilder::try_new_decoder(test_file_parquet_metadata()).unwrap();
        let schema_descr = builder.metadata().file_metadata().schema_descr_ptr();

        // `a > 175` matches rows 176..199 in row group 0 (24 matches) and
        // 200..399 in row group 1 (200 matches). With limit = 5, all matches
        // should come from row group 0.
        let row_filter_a = ArrowPredicateFn::new(
            ProjectionMask::columns(&schema_descr, ["a"]),
            |batch: RecordBatch| {
                let scalar_175 = Int64Array::new_scalar(175);
                let column = batch.column(0).as_primitive::<Int64Type>();
                gt(column, &scalar_175)
            },
        );

        let mut decoder = builder
            .with_projection(ProjectionMask::columns(&schema_descr, ["a"]))
            .with_row_filter(RowFilter::new(vec![Box::new(row_filter_a)]))
            .with_limit(5)
            .build()
            .unwrap();

        // Row group 0: fetch filter pages
        let ranges = expect_needs_data(decoder.try_decode());
        push_ranges_to_decoder(&mut decoder, ranges);

        // First 5 matches: 176..180
        let batch = expect_data(decoder.try_decode());
        let expected = TEST_BATCH.slice(176, 5).project(&[0]).unwrap();
        assert_eq!(batch, expected);

        // Row group 1 must NOT request data — the limit is already satisfied
        // so `Start` in row group 1 short-circuits to `Finished`.
        expect_finished(decoder.try_decode());
    }

    /// The predicate short-circuit must account for `self.offset` as well as
    /// `self.limit`. The post-predicate `with_offset` step skips that many
    /// already-selected rows before `with_limit` counts output rows — so the
    /// predicate must retain at least `offset + limit` matches. Without the
    /// fix, Layer 2 caps at just `limit` and the later `with_offset` consumes
    /// all of them, producing 0 rows instead of `limit`.
    ///
    /// `a > 175` matches rows 176..199 in row group 0 (24 matches). With
    /// `offset = 10, limit = 5`, the expected output is rows 186..190 (the
    /// 11th through 15th matches).
    #[test]
    fn test_decoder_filter_with_offset_and_limit() {
        let builder =
            ParquetPushDecoderBuilder::try_new_decoder(test_file_parquet_metadata()).unwrap();
        let schema_descr = builder.metadata().file_metadata().schema_descr_ptr();

        let row_filter_a = ArrowPredicateFn::new(
            ProjectionMask::columns(&schema_descr, ["a"]),
            |batch: RecordBatch| {
                let scalar_175 = Int64Array::new_scalar(175);
                let column = batch.column(0).as_primitive::<Int64Type>();
                gt(column, &scalar_175)
            },
        );

        let mut decoder = builder
            .with_projection(ProjectionMask::columns(&schema_descr, ["a"]))
            .with_row_filter(RowFilter::new(vec![Box::new(row_filter_a)]))
            .with_offset(10)
            .with_limit(5)
            .build()
            .unwrap();

        let ranges = expect_needs_data(decoder.try_decode());
        push_ranges_to_decoder(&mut decoder, ranges);

        let batch = expect_data(decoder.try_decode());
        let expected = TEST_BATCH.slice(186, 5).project(&[0]).unwrap();
        assert_eq!(batch, expected);

        expect_finished(decoder.try_decode());
    }

    /// The limit short-circuit must also be correct when the limited predicate
    /// is the last predicate in a multi-predicate chain.
    ///
    /// `a > 175` first narrows row group 0 to rows 176..199. The final
    /// predicate `b < 625` is then evaluated only over those 24 rows, all of
    /// which match. With `limit = 10`, the final output should still be rows
    /// 176..185, and the second predicate should stop before consuming all 24
    /// selected rows.
    #[test]
    fn test_decoder_multi_filters_with_limit() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let builder =
            ParquetPushDecoderBuilder::try_new_decoder(test_file_parquet_metadata()).unwrap();
        let schema_descr = builder.metadata().file_metadata().schema_descr_ptr();

        let first_predicate_rows = Arc::new(AtomicUsize::new(0));
        let second_predicate_rows = Arc::new(AtomicUsize::new(0));

        let first_predicate_rows_for_filter = Arc::clone(&first_predicate_rows);
        let row_filter_a = ArrowPredicateFn::new(
            ProjectionMask::columns(&schema_descr, ["a"]),
            move |batch: RecordBatch| {
                first_predicate_rows_for_filter.fetch_add(batch.num_rows(), Ordering::Relaxed);
                let scalar_175 = Int64Array::new_scalar(175);
                let column = batch.column(0).as_primitive::<Int64Type>();
                gt(column, &scalar_175)
            },
        );

        let second_predicate_rows_for_filter = Arc::clone(&second_predicate_rows);
        let row_filter_b = ArrowPredicateFn::new(
            ProjectionMask::columns(&schema_descr, ["b"]),
            move |batch: RecordBatch| {
                second_predicate_rows_for_filter.fetch_add(batch.num_rows(), Ordering::Relaxed);
                let scalar_625 = Int64Array::new_scalar(625);
                let column = batch.column(0).as_primitive::<Int64Type>();
                lt(column, &scalar_625)
            },
        );

        let mut decoder = builder
            .with_projection(ProjectionMask::columns(&schema_descr, ["c"]))
            .with_row_filter(RowFilter::new(vec![
                Box::new(row_filter_a),
                Box::new(row_filter_b),
            ]))
            .with_batch_size(10)
            .with_limit(10)
            .build()
            .unwrap();

        // Row group 0, first predicate
        let ranges = expect_needs_data(decoder.try_decode());
        push_ranges_to_decoder(&mut decoder, ranges);

        // Row group 0, second predicate
        let ranges = expect_needs_data(decoder.try_decode());
        push_ranges_to_decoder(&mut decoder, ranges);

        // Final projected data
        let ranges = expect_needs_data(decoder.try_decode());
        push_ranges_to_decoder(&mut decoder, ranges);

        let batch = expect_data(decoder.try_decode());
        let expected = TEST_BATCH.slice(176, 10).project(&[2]).unwrap();
        assert_eq!(batch, expected);

        // The overall limit was satisfied by row group 0.
        expect_finished(decoder.try_decode());

        assert_eq!(first_predicate_rows.load(Ordering::Relaxed), 200);
        assert!(
            second_predicate_rows.load(Ordering::Relaxed) < 24,
            "final predicate should short-circuit before consuming all 24 rows selected by the first predicate"
        );
    }

    /// When a row selection already exists, limiting the predicate must still
    /// preserve alignment with that prior selection.
    ///
    /// The explicit selection narrows row group 0 to rows 150..199. Applying
    /// `a > 175` over that selection yields rows 176..199. With `limit = 10`,
    /// the decoder should emit rows 176..185 and stop without evaluating the
    /// remaining selected rows.
    #[test]
    fn test_decoder_filter_with_row_selection_and_limit() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let builder =
            ParquetPushDecoderBuilder::try_new_decoder(test_file_parquet_metadata()).unwrap();
        let schema_descr = builder.metadata().file_metadata().schema_descr_ptr();

        let rows_filtered = Arc::new(AtomicUsize::new(0));
        let rows_filtered_for_predicate = Arc::clone(&rows_filtered);

        let row_filter_a = ArrowPredicateFn::new(
            ProjectionMask::columns(&schema_descr, ["a"]),
            move |batch: RecordBatch| {
                rows_filtered_for_predicate.fetch_add(batch.num_rows(), Ordering::Relaxed);
                let scalar_175 = Int64Array::new_scalar(175);
                let column = batch.column(0).as_primitive::<Int64Type>();
                gt(column, &scalar_175)
            },
        );

        let mut decoder = builder
            .with_projection(ProjectionMask::columns(&schema_descr, ["a"]))
            .with_row_selection(RowSelection::from(vec![
                RowSelector::skip(150),
                RowSelector::select(50),
            ]))
            .with_row_filter(RowFilter::new(vec![Box::new(row_filter_a)]))
            .with_batch_size(10)
            .with_limit(10)
            .build()
            .unwrap();

        let ranges = expect_needs_data(decoder.try_decode());
        push_ranges_to_decoder(&mut decoder, ranges);

        let batch = expect_data(decoder.try_decode());
        let expected = TEST_BATCH.slice(176, 10).project(&[0]).unwrap();
        assert_eq!(batch, expected);

        expect_finished(decoder.try_decode());

        assert!(
            rows_filtered.load(Ordering::Relaxed) < 50,
            "predicate should short-circuit before consuming all 50 rows from the explicit row selection"
        );
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
    fn test_decoder_try_next_reader_offset_limit() {
        let mut decoder = ParquetPushDecoderBuilder::try_new_decoder(test_file_parquet_metadata())
            .unwrap()
            .with_offset(225)
            .with_limit(20)
            .build()
            .unwrap();

        let ranges = expect_needs_data(decoder.try_next_reader());
        push_ranges_to_decoder(&mut decoder, ranges);

        let reader = expect_data(decoder.try_next_reader());
        let batches = reader
            .map(|batch| batch.expect("expected decoded batch"))
            .collect::<Vec<_>>();
        let output = concat_batches(&TEST_BATCH.schema(), &batches).unwrap();
        assert_eq!(output, TEST_BATCH.slice(225, 20));

        expect_finished(decoder.try_next_reader());
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

    /// `peek_next_row_group` reports the index of the row group the
    /// next `try_next_reader` call will hand back, matching the
    /// frontier's internal skip logic.
    #[test]
    fn test_peek_next_row_group_basic() {
        let mut decoder = ParquetPushDecoderBuilder::try_new_decoder(test_file_parquet_metadata())
            .unwrap()
            .build()
            .unwrap();

        // Two row groups (0, 1). At boundary before any read, peek should
        // see RG 0.
        assert_eq!(decoder.peek_next_row_group().unwrap(), Some(0));
        assert!(decoder.is_at_row_group_boundary());

        let ranges = expect_needs_data(decoder.try_next_reader());
        push_ranges_to_decoder(&mut decoder, ranges);
        let reader = expect_data(decoder.try_next_reader());
        // Once the reader for RG 0 has been handed off, the decoder is
        // back at a boundary waiting for RG 1 — peek must reflect that
        // (the active reader lives outside the decoder).
        assert!(decoder.is_at_row_group_boundary());
        assert_eq!(decoder.peek_next_row_group().unwrap(), Some(1));

        // Drain RG 0's reader and consume RG 1.
        for batch in reader {
            let _ = batch.unwrap();
        }
        let ranges = expect_needs_data(decoder.try_next_reader());
        push_ranges_to_decoder(&mut decoder, ranges);
        let reader = expect_data(decoder.try_next_reader());
        for batch in reader {
            let _ = batch.unwrap();
        }

        // No row groups left.
        assert_eq!(decoder.peek_next_row_group().unwrap(), None);
    }

    /// `peek_next_row_group` honors `with_row_groups` — restricting the
    /// scan to a single row group means peek reports only that one and
    /// then `None`.
    #[test]
    fn test_peek_next_row_group_respects_with_row_groups() {
        let decoder = ParquetPushDecoderBuilder::try_new_decoder(test_file_parquet_metadata())
            .unwrap()
            .with_row_groups(vec![1])
            .build()
            .unwrap();

        assert_eq!(decoder.peek_next_row_group().unwrap(), Some(1));
    }

    /// When a row-selection segment leaves the next row group with zero
    /// selected rows, `peek_next_row_group` mirrors
    /// `next_readable_row_group`'s skip: it returns the *following*
    /// row group instead of the empty one.
    #[test]
    fn test_peek_next_row_group_skips_empty_selection() {
        // Each row group has 200 rows. Skip all 200 of RG 0 plus 50 of
        // RG 1; the next reader will be for RG 1, not RG 0.
        let decoder = ParquetPushDecoderBuilder::try_new_decoder(test_file_parquet_metadata())
            .unwrap()
            .with_row_selection(RowSelection::from(vec![
                RowSelector::skip(250),
                RowSelector::select(100),
            ]))
            .build()
            .unwrap();

        assert_eq!(decoder.peek_next_row_group().unwrap(), Some(1));
    }

    /// `peek_next_row_group` returns `None` on a finished decoder.
    #[test]
    fn test_peek_next_row_group_finished() {
        let mut decoder = ParquetPushDecoderBuilder::try_new_decoder(test_file_parquet_metadata())
            .unwrap()
            .with_row_groups(vec![])
            .build()
            .unwrap();

        // No row groups requested ⇒ already finished, no peek.
        expect_finished(decoder.try_next_reader());
        assert_eq!(decoder.peek_next_row_group().unwrap(), None);
    }

    /// `peek_next_row_group` mirrors `next_readable_row_group`'s
    /// budget-skipping logic: with an `OFFSET` that consumes the
    /// entire first row group, peek must return the *following*
    /// row group, not RG 0 (no predicates, so budget skips apply).
    #[test]
    fn test_peek_next_row_group_skips_for_budget() {
        // Each row group has 200 rows. OFFSET 200 drains RG 0 entirely
        // and lands the decoder's first emitted reader at RG 1.
        let decoder = ParquetPushDecoderBuilder::try_new_decoder(test_file_parquet_metadata())
            .unwrap()
            .with_offset(200)
            .build()
            .unwrap();

        assert_eq!(decoder.peek_next_row_group().unwrap(), Some(1));
    }

    /// OFFSET larger than every remaining row group's row count
    /// exhausts the budget, so peek should report `None` — matching
    /// `next_readable_row_group`'s behavior of producing `Finished`.
    #[test]
    fn test_peek_next_row_group_budget_drains_all() {
        // 2 row groups × 200 rows = 400 total. OFFSET 500 cannot land
        // anywhere — every RG is skipped under the budget.
        let decoder = ParquetPushDecoderBuilder::try_new_decoder(test_file_parquet_metadata())
            .unwrap()
            .with_offset(500)
            .build()
            .unwrap();

        assert_eq!(decoder.peek_next_row_group().unwrap(), None);
    }

    /// `peek_next_row_group` is safe to call while a row group's reader
    /// is still being drained via `try_decode`. In `DecodingRowGroup`
    /// state it must report the row group `try_next_reader` will yield
    /// a reader for *after* the active one — never `None` just because
    /// the decoder is mid-row-group.
    #[test]
    fn test_peek_next_row_group_during_decoding_row_group() {
        // Two row groups (200 rows each), small batch size so the
        // decoder stays in `DecodingRowGroup` across multiple
        // `try_decode` calls within RG 0.
        let mut decoder = ParquetPushDecoderBuilder::try_new_decoder(test_file_parquet_metadata())
            .unwrap()
            .with_batch_size(100)
            .build()
            .unwrap();
        decoder
            .push_range(test_file_range(), TEST_FILE_DATA.clone())
            .unwrap();

        // First batch of RG 0 — after this, the decoder is in
        // `DecodingRowGroup` state with the reader retained internally
        // and one more 100-row batch still owed for RG 0. Peek must
        // therefore look past the active RG to RG 1.
        let _batch0 = expect_data(decoder.try_decode());
        assert!(!decoder.is_at_row_group_boundary());
        assert_eq!(decoder.peek_next_row_group().unwrap(), Some(1));

        // Drain the rest of RG 0; peek still reports RG 1.
        let _batch1 = expect_data(decoder.try_decode());
        assert_eq!(decoder.peek_next_row_group().unwrap(), Some(1));

        // Move into RG 1 — peek now sees no further row groups.
        let _batch2 = expect_data(decoder.try_decode());
        assert!(!decoder.is_at_row_group_boundary());
        assert_eq!(decoder.peek_next_row_group().unwrap(), None);

        let _batch3 = expect_data(decoder.try_decode());
        expect_finished(decoder.try_decode());
    }

    /// Peeking is a read-only operation: calling it repeatedly between
    /// `try_next_reader` calls must never change which row group the
    /// reader path actually produces. Drives the decoder all the way
    /// through and asserts each peek/read pair agrees.
    #[test]
    fn test_peek_next_row_group_does_not_mutate_state() {
        let mut decoder = ParquetPushDecoderBuilder::try_new_decoder(test_file_parquet_metadata())
            .unwrap()
            .build()
            .unwrap();

        // Two row groups expected — drive both, asserting peek/read agree.
        for expected_rg in [0usize, 1usize] {
            assert!(decoder.is_at_row_group_boundary());

            // Multiple peeks before reading must all agree, and must not
            // disturb the upcoming read.
            let first_peek = decoder.peek_next_row_group().unwrap();
            let second_peek = decoder.peek_next_row_group().unwrap();
            let third_peek = decoder.peek_next_row_group().unwrap();
            assert_eq!(first_peek, Some(expected_rg));
            assert_eq!(first_peek, second_peek);
            assert_eq!(second_peek, third_peek);

            // Now read for real and confirm the decoder hands back exactly
            // what peek promised.
            let ranges = expect_needs_data(decoder.try_next_reader());
            push_ranges_to_decoder(&mut decoder, ranges);
            let reader = expect_data(decoder.try_next_reader());
            for batch in reader {
                let _ = batch.unwrap();
            }
        }

        // Decoder is drained. Peek must agree with `try_next_reader`'s
        // terminal state.
        assert_eq!(decoder.peek_next_row_group().unwrap(), None);
        expect_finished(decoder.try_next_reader());
    }

    /// `into_builder` between row groups recovers a builder for the
    /// not-yet-decoded row groups; rebuilding it with a new row filter
    /// applies that filter to the subsequent row groups while leaving the
    /// already-decoded row group's results untouched.
    ///
    /// See the "Adaptive scans" section of [`ParquetPushDecoderBuilder`] for
    /// the high-level overview.
    #[test]
    fn test_into_builder_installs_filter_between_row_groups() {
        let schema_descr = test_file_parquet_metadata()
            .file_metadata()
            .schema_descr_ptr();
        let mut decoder = prefetched_decoder(1024);

        // Reader for row group 0 — no filter.
        let reader0 = expect_data(decoder.try_next_reader());
        let batches0: Vec<_> = reader0.collect::<Result<_, _>>().unwrap();
        let batch0 = concat_batches(&TEST_BATCH.schema(), &batches0).unwrap();
        assert_eq!(batch0, TEST_BATCH.slice(0, 200));

        // We're between row groups now. Rebuild with a filter on column "a".
        assert!(decoder.is_at_row_group_boundary());
        assert_eq!(decoder.row_groups_remaining(), 1);
        let filter =
            ArrowPredicateFn::new(ProjectionMask::columns(&schema_descr, ["a"]), |batch| {
                gt(batch.column(0), &Int64Array::new_scalar(250))
            });
        let mut decoder = decoder
            .into_builder()
            .unwrap()
            .with_row_filter(RowFilter::new(vec![Box::new(filter)]))
            .build()
            .unwrap();

        // Reader for row group 1 — filter applied. The rebuilt decoder kept
        // the buffered bytes (see `test_into_builder_preserves_buffered_bytes`)
        // so no data needs to be re-supplied. Column "a" in RG1 has values
        // 200..399; `a > 250` keeps 251..399 = 149 rows.
        let reader1 = expect_data(decoder.try_next_reader());
        let batches1: Vec<_> = reader1.collect::<Result<_, _>>().unwrap();
        let batch1 = concat_batches(&TEST_BATCH.schema(), &batches1).unwrap();
        assert_eq!(batch1, TEST_BATCH.slice(251, 149));
        expect_finished(decoder.try_next_reader());
    }

    /// `into_builder` is rejected while a row group's reader is being
    /// drained (`DecodingRowGroup`); the error points at
    /// `is_at_row_group_boundary`.
    #[test]
    fn test_into_builder_rejected_mid_row_group() {
        let mut decoder = prefetched_decoder(50);

        // Decode one batch to land mid-row-group, inside `DecodingRowGroup`
        // with an active reader — not a boundary.
        expect_data(decoder.try_decode());
        assert!(!decoder.is_at_row_group_boundary());

        let err = decoder.into_builder().unwrap_err();
        let err_msg = format!("{err}");
        assert!(
            err_msg.contains("is_at_row_group_boundary"),
            "unexpected error: {err_msg}"
        );
    }

    /// `into_builder` is rejected once the decoder has finished.
    #[test]
    fn test_into_builder_rejected_on_finished_decoder() {
        let mut decoder = prefetched_decoder(1024);
        expect_data(decoder.try_decode());
        expect_data(decoder.try_decode());
        expect_finished(decoder.try_decode());
        assert!(!decoder.is_at_row_group_boundary());

        let err = decoder.into_builder().unwrap_err();
        assert!(
            format!("{err}").contains("finished"),
            "unexpected error: {err}"
        );
    }

    /// `try_next_reader` hands the active reader off to the caller and
    /// transitions the decoder back to `ReadingRowGroup` — so the caller
    /// can call `into_builder` even while still holding the returned
    /// reader. (The handed-off reader has no link back to the decoder's
    /// projection/filter; it has its own `ArrayReader` and `ReadPlan`.)
    #[test]
    fn test_into_builder_allowed_while_iterating_handed_off_reader() {
        let mut decoder = prefetched_decoder(1024);

        let reader0 = expect_data(decoder.try_next_reader());
        // Decoder no longer owns the reader, so it considers itself
        // "between row groups".
        assert!(decoder.is_at_row_group_boundary());
        // Recovering the builder consumes the decoder but leaves `reader0`
        // valid: iterating it is independent of the decoder's state.
        let _builder = decoder.into_builder().unwrap();
        let batches: Vec<_> = reader0.collect::<Result<_, _>>().unwrap();
        let batch0 = concat_batches(&TEST_BATCH.schema(), &batches).unwrap();
        assert_eq!(batch0, TEST_BATCH.slice(0, 200));
    }

    /// `into_builder` recovers a builder for the *remaining* row groups and
    /// carries the not-yet-consumed offset/limit budget, so a rebuilt
    /// decoder resumes where the original left off rather than restarting.
    #[test]
    fn test_into_builder_resumes_remaining_budget() {
        // limit = 250 spans both 200-row row groups: all 200 rows of RG0
        // plus the first 50 rows of RG1.
        let mut decoder = ParquetPushDecoderBuilder::try_new_decoder(test_file_parquet_metadata())
            .unwrap()
            .with_batch_size(1024)
            .with_limit(250)
            .build()
            .unwrap();
        prefetch_test_file(&mut decoder);

        // RG0 contributes all 200 of its rows.
        let reader0 = expect_data(decoder.try_next_reader());
        let batches0: Vec<_> = reader0.collect::<Result<_, _>>().unwrap();
        let batch0 = concat_batches(&TEST_BATCH.schema(), &batches0).unwrap();
        assert_eq!(batch0, TEST_BATCH.slice(0, 200));

        // Rebuild without changing anything: the remaining 50-row limit and
        // the not-yet-decoded RG1 must carry through (as do the buffers, so
        // no data needs re-supplying).
        assert!(decoder.is_at_row_group_boundary());
        let mut decoder = decoder.into_builder().unwrap().build().unwrap();

        let reader1 = expect_data(decoder.try_next_reader());
        let batches1: Vec<_> = reader1.collect::<Result<_, _>>().unwrap();
        let batch1 = concat_batches(&TEST_BATCH.schema(), &batches1).unwrap();
        // Only the first 50 rows of RG1 (200..249) — the rest of the limit.
        assert_eq!(batch1, TEST_BATCH.slice(200, 50));
        expect_finished(decoder.try_next_reader());
    }

    /// `into_builder` carries the decoder's buffered bytes across the
    /// rebuild: the rebuilt decoder keeps them and does not re-request data
    /// it already holds.
    #[test]
    fn test_into_builder_preserves_buffered_bytes() {
        let mut decoder = prefetched_decoder(1024);
        assert_eq!(decoder.buffered_bytes(), test_file_len());

        // Drain RG0.
        let reader0 = expect_data(decoder.try_next_reader());
        let _: Vec<_> = reader0.collect::<Result<_, _>>().unwrap();
        // RG1's bytes are still staged inside the decoder.
        let buffered = decoder.buffered_bytes();
        assert!(buffered > 0);

        // Rebuilding via into_builder keeps the staged bytes.
        let mut decoder = decoder.into_builder().unwrap().build().unwrap();
        assert_eq!(decoder.buffered_bytes(), buffered);

        // RG1's bytes are already buffered, so it decodes without a
        // `NeedsData` round-trip.
        let reader1 = expect_data(decoder.try_next_reader());
        let batches1: Vec<_> = reader1.collect::<Result<_, _>>().unwrap();
        let batch1 = concat_batches(&TEST_BATCH.schema(), &batches1).unwrap();
        assert_eq!(batch1, TEST_BATCH.slice(200, 200));
        expect_finished(decoder.try_next_reader());
    }

    /// Drive the decoder incrementally. Start with a narrow projection,
    /// drain RG0, then `into_builder` and widen the projection to all three
    /// columns. The rebuilt decoder's `NeedsData` for RG1 must request
    /// bytes for *all three* columns, not just the originally-projected
    /// "a". The expected ranges are hardcoded because `TEST_BATCH` and the
    /// writer settings are static; this pins the layout cleanly without a
    /// parallel reference decoder.
    #[test]
    fn test_into_builder_expand_projection_requests_new_bytes() {
        let metadata = test_file_parquet_metadata();
        let schema_descr = metadata.file_metadata().schema_descr_ptr();

        let mut decoder = ParquetPushDecoderBuilder::try_new_decoder(metadata)
            .unwrap()
            .with_batch_size(1024)
            .with_projection(ProjectionMask::columns(&schema_descr, ["a"]))
            .build()
            .unwrap();

        // RG0: incrementally satisfy the narrow request — a single
        // contiguous range for the "a"-only projection.
        let ranges_rg0 = expect_needs_data(decoder.try_next_reader());
        assert_eq!(ranges_rg0, vec![4..1860]);
        push_ranges_to_decoder(&mut decoder, ranges_rg0);

        let reader0 = expect_data(decoder.try_next_reader());
        let batches0: Vec<_> = reader0.collect::<Result<_, _>>().unwrap();
        let batch0 = concat_batches(&batches0[0].schema(), &batches0).unwrap();
        assert_eq!(batch0, TEST_BATCH.slice(0, 200).project(&[0]).unwrap());

        // Widen the projection at the boundary.
        assert!(decoder.is_at_row_group_boundary());
        let mut decoder = decoder
            .into_builder()
            .unwrap()
            .with_projection(ProjectionMask::columns(&schema_descr, ["a", "b", "c"]))
            .build()
            .unwrap();

        // RG1 now requests "a", "b", and "c" column chunks: ~1.8KiB each
        // for "a" and "b", ~7.5KiB for the StringView column "c".
        let ranges_rg1 = expect_needs_data(decoder.try_next_reader());
        assert_eq!(ranges_rg1, vec![11062..12918, 12918..14774, 14774..22230]);
        push_ranges_to_decoder(&mut decoder, ranges_rg1);

        let reader1 = expect_data(decoder.try_next_reader());
        let batches1: Vec<_> = reader1.collect::<Result<_, _>>().unwrap();
        let batch1 = concat_batches(&TEST_BATCH.schema(), &batches1).unwrap();
        assert_eq!(batch1, TEST_BATCH.slice(200, 200));
        expect_finished(decoder.try_next_reader());
    }

    /// Mirror of [`test_into_builder_expand_projection_requests_new_bytes`]:
    /// start with the full projection, drain RG0, then `into_builder` and
    /// narrow the projection to just column "a". RG1's `NeedsData` must
    /// request only the single "a" column-chunk range, not the three a
    /// wide projection would.
    #[test]
    fn test_into_builder_narrow_projection_requests_fewer_bytes() {
        let metadata = test_file_parquet_metadata();
        let schema_descr = metadata.file_metadata().schema_descr_ptr();

        let mut decoder = ParquetPushDecoderBuilder::try_new_decoder(metadata)
            .unwrap()
            .with_batch_size(1024)
            .build()
            .unwrap();

        // RG0 with the default (full) projection — three column ranges.
        let ranges_rg0 = expect_needs_data(decoder.try_next_reader());
        assert_eq!(ranges_rg0, vec![4..1860, 1860..3716, 3716..11062]);
        push_ranges_to_decoder(&mut decoder, ranges_rg0);

        let reader0 = expect_data(decoder.try_next_reader());
        let batches0: Vec<_> = reader0.collect::<Result<_, _>>().unwrap();
        let batch0 = concat_batches(&TEST_BATCH.schema(), &batches0).unwrap();
        assert_eq!(batch0, TEST_BATCH.slice(0, 200));

        // Narrow the projection at the boundary.
        assert!(decoder.is_at_row_group_boundary());
        let mut decoder = decoder
            .into_builder()
            .unwrap()
            .with_projection(ProjectionMask::columns(&schema_descr, ["a"]))
            .build()
            .unwrap();

        // RG1 now requests column "a" only — a single 1856-byte range.
        let ranges_rg1 = expect_needs_data(decoder.try_next_reader());
        assert_eq!(ranges_rg1, vec![11062..12918]);
        push_ranges_to_decoder(&mut decoder, ranges_rg1);

        let reader1 = expect_data(decoder.try_next_reader());
        let batches1: Vec<_> = reader1.collect::<Result<_, _>>().unwrap();
        let batch1 = concat_batches(&batches1[0].schema(), &batches1).unwrap();
        assert_eq!(batch1, TEST_BATCH.slice(200, 200).project(&[0]).unwrap());
        expect_finished(decoder.try_next_reader());
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
            .set_max_row_group_row_count(Some(200))
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

    /// Push the entire test file into `decoder`.
    fn prefetch_test_file(decoder: &mut ParquetPushDecoder) {
        decoder
            .push_range(test_file_range(), TEST_FILE_DATA.clone())
            .unwrap();
    }

    /// Build a decoder over the test file with the given batch size and
    /// prefetch the whole file into it.
    fn prefetched_decoder(batch_size: usize) -> ParquetPushDecoder {
        let mut decoder = ParquetPushDecoderBuilder::try_new_decoder(test_file_parquet_metadata())
            .unwrap()
            .with_batch_size(batch_size)
            .build()
            .unwrap();
        prefetch_test_file(&mut decoder);
        decoder
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
