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

use crate::compression::CompressionCodec;
use crate::reader::Decoder;
use crate::reader::block::{BlockDecoder, BlockDecoderState};
use arrow_array::RecordBatch;
use arrow_schema::ArrowError;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{FutureExt, Stream};
use std::mem;
use std::ops::Range;
use std::pin::Pin;
use std::task::{Context, Poll};

mod async_file_reader;
mod builder;

pub use async_file_reader::AsyncFileReader;
pub use builder::AsyncAvroFileReaderBuilder;

#[cfg(feature = "object_store")]
mod store;

use crate::errors::AvroError;
#[cfg(feature = "object_store")]
pub use store::AvroObjectReader;

enum FetchNextBehaviour {
    /// Initial read: scan for sync marker, then move to decoding blocks
    ReadSyncMarker,
    /// Parse VLQ header bytes one at a time until Data state, then continue decoding
    DecodeVLQHeader,
    /// Continue decoding the current block with the fetched data
    ContinueDecoding,
}

enum ReaderState<R> {
    /// Intermediate state to fix ownership issues
    InvalidState,
    /// Initial state, fetch initial range
    Idle { reader: R },
    /// Fetching data from the reader
    FetchingData {
        future: BoxFuture<'static, Result<(R, Bytes), AvroError>>,
        next_behaviour: FetchNextBehaviour,
    },
    /// Decode a block in a loop until completion
    DecodingBlock { data: Bytes, reader: R },
    /// Output batches from a decoded block
    ReadingBatches {
        data: Bytes,
        block_data: Bytes,
        remaining_in_block: usize,
        reader: R,
    },
    /// Successfully finished reading file contents; drain any remaining buffered records
    /// from the decoder into (possibly partial) output batches.
    Flushing,
    /// Done, flush decoder and return
    Finished,
}

/// An asynchronous Avro file reader that implements `Stream<Item = Result<RecordBatch, ArrowError>>`.
/// This uses an [`AsyncFileReader`] to fetch data ranges as needed, starting with fetching the header,
/// then reading all the blocks in the provided range where:
/// 1. Reads and decodes data until the header is fully decoded.
/// 2. Searching from `range.start` for the first sync marker, and starting with the following block.
///    (If `range.start` is less than the header length, we start at the header length minus the sync marker bytes)
/// 3. Reading blocks sequentially, decoding them into RecordBatches.
/// 4. If a block is incomplete (due to range ending mid-block), fetching the remaining bytes from the [`AsyncFileReader`].
/// 5. If no range was originally provided, reads the full file.
/// 6. If the range is 0, file_size is 0, or `range.end` is less than the header length, finish immediately.
pub struct AsyncAvroFileReader<R> {
    // Members required to fetch data
    range: Range<u64>,
    file_size: u64,

    // Members required to actually decode and read data
    decoder: Decoder,
    block_decoder: BlockDecoder,
    codec: Option<CompressionCodec>,
    sync_marker: [u8; 16],

    // Members keeping the current state of the reader
    reader_state: ReaderState<R>,
    finishing_partial_block: bool,
}

impl<R> AsyncAvroFileReader<R> {
    /// Returns a builder for a new [`Self`], allowing some optional parameters.
    pub fn builder(reader: R, file_size: u64, batch_size: usize) -> AsyncAvroFileReaderBuilder<R> {
        AsyncAvroFileReaderBuilder::new(reader, file_size, batch_size)
    }

    fn new(
        range: Range<u64>,
        file_size: u64,
        decoder: Decoder,
        codec: Option<CompressionCodec>,
        sync_marker: [u8; 16],
        reader_state: ReaderState<R>,
    ) -> Self {
        Self {
            range,
            file_size,

            decoder,
            block_decoder: Default::default(),
            codec,
            sync_marker,

            reader_state,
            finishing_partial_block: false,
        }
    }

    /// Calculate the byte range needed to complete the current block.
    /// Only valid when block_decoder is in Data or Sync state.
    /// Returns the range to fetch, or an error if EOF would be reached.
    fn remaining_block_range(&self) -> Result<Range<u64>, AvroError> {
        let remaining = self.block_decoder.bytes_remaining() as u64
            + match self.block_decoder.state() {
                BlockDecoderState::Data => 16, // Include sync marker
                BlockDecoderState::Sync => 0,
                state => {
                    return Err(AvroError::General(format!(
                        "remaining_block_range called in unexpected state: {state:?}"
                    )));
                }
            };

        let fetch_end = self.range.end + remaining;
        if fetch_end > self.file_size {
            return Err(AvroError::EOF(
                "Avro block requires more bytes than what exists in the file".into(),
            ));
        }

        Ok(self.range.end..fetch_end)
    }

    /// Terminate the stream after returning this error once.
    #[inline]
    fn finish_with_error(
        &mut self,
        error: AvroError,
    ) -> Poll<Option<Result<RecordBatch, AvroError>>> {
        self.reader_state = ReaderState::Finished;
        Poll::Ready(Some(Err(error)))
    }

    #[inline]
    fn start_flushing(&mut self) {
        self.reader_state = ReaderState::Flushing;
    }

    /// Drain any remaining buffered records from the decoder.
    #[inline]
    fn poll_flush(&mut self) -> Poll<Option<Result<RecordBatch, AvroError>>> {
        match self.decoder.flush() {
            Ok(Some(batch)) => {
                self.reader_state = ReaderState::Flushing;
                Poll::Ready(Some(Ok(batch)))
            }
            Ok(None) => {
                self.reader_state = ReaderState::Finished;
                Poll::Ready(None)
            }
            Err(e) => self.finish_with_error(e),
        }
    }
}

impl<R: AsyncFileReader + Unpin + 'static> AsyncAvroFileReader<R> {
    // The forbid question mark thing shouldn't apply here, as it is within the future,
    // so exported this to a separate function.
    async fn fetch_bytes(mut reader: R, range: Range<u64>) -> Result<(R, Bytes), AvroError> {
        let data = reader.get_bytes(range).await?;
        Ok((reader, data))
    }

    #[forbid(clippy::question_mark_used)]
    fn read_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<RecordBatch, AvroError>>> {
        loop {
            match mem::replace(&mut self.reader_state, ReaderState::InvalidState) {
                ReaderState::Idle { reader } => {
                    let range = self.range.clone();
                    if range.start >= range.end {
                        return self.finish_with_error(AvroError::InvalidArgument(format!(
                            "Invalid range specified for Avro file: start {} >= end {}, file_size: {}",
                            range.start, range.end, self.file_size
                        )));
                    }

                    let future = Self::fetch_bytes(reader, range).boxed();
                    self.reader_state = ReaderState::FetchingData {
                        future,
                        next_behaviour: FetchNextBehaviour::ReadSyncMarker,
                    };
                }
                ReaderState::FetchingData {
                    mut future,
                    next_behaviour,
                } => {
                    let (reader, data_chunk) = match future.poll_unpin(cx) {
                        Poll::Ready(Ok(data)) => data,
                        Poll::Ready(Err(e)) => return self.finish_with_error(e),
                        Poll::Pending => {
                            self.reader_state = ReaderState::FetchingData {
                                future,
                                next_behaviour,
                            };
                            return Poll::Pending;
                        }
                    };

                    match next_behaviour {
                        FetchNextBehaviour::ReadSyncMarker => {
                            let sync_marker_pos = data_chunk
                                .windows(16)
                                .position(|slice| slice == self.sync_marker);
                            let block_start = match sync_marker_pos {
                                Some(pos) => pos + 16, // Move past the sync marker
                                None => {
                                    // Sync marker not found, valid if we arbitrarily split the file at its end.
                                    self.reader_state = ReaderState::Finished;
                                    return Poll::Ready(None);
                                }
                            };

                            self.reader_state = ReaderState::DecodingBlock {
                                reader,
                                data: data_chunk.slice(block_start..),
                            };
                        }
                        FetchNextBehaviour::DecodeVLQHeader => {
                            let mut data = data_chunk;

                            // Feed bytes one at a time until we reach Data state (VLQ header complete)
                            while !matches!(self.block_decoder.state(), BlockDecoderState::Data) {
                                if data.is_empty() {
                                    return self.finish_with_error(AvroError::EOF(
                                        "Unexpected EOF while reading Avro block header".into(),
                                    ));
                                }
                                let consumed = match self.block_decoder.decode(&data[..1]) {
                                    Ok(consumed) => consumed,
                                    Err(e) => return self.finish_with_error(e),
                                };
                                if consumed == 0 {
                                    return self.finish_with_error(AvroError::General(
                                        "BlockDecoder failed to consume byte during VLQ header parsing"
                                            .into(),
                                    ));
                                }
                                data = data.slice(consumed..);
                            }

                            // Now we know the block size. Slice remaining data to what we need.
                            let bytes_remaining = self.block_decoder.bytes_remaining();
                            let data_to_use = data.slice(..data.len().min(bytes_remaining));
                            let consumed = match self.block_decoder.decode(&data_to_use) {
                                Ok(consumed) => consumed,
                                Err(e) => return self.finish_with_error(e),
                            };
                            if consumed != data_to_use.len() {
                                return self.finish_with_error(AvroError::General(
                                    "BlockDecoder failed to consume all bytes after VLQ header parsing"
                                        .into(),
                                ));
                            }

                            // May need more data to finish the block.
                            let range_to_fetch = match self.remaining_block_range() {
                                Ok(range) if range.is_empty() => {
                                    // All bytes fetched, move to decoding block directly
                                    self.reader_state = ReaderState::DecodingBlock {
                                        reader,
                                        data: Bytes::new(),
                                    };
                                    continue;
                                }
                                Ok(range) => range,
                                Err(e) => return self.finish_with_error(e),
                            };

                            let future = Self::fetch_bytes(reader, range_to_fetch).boxed();
                            self.reader_state = ReaderState::FetchingData {
                                future,
                                next_behaviour: FetchNextBehaviour::ContinueDecoding,
                            };
                            continue;
                        }
                        FetchNextBehaviour::ContinueDecoding => {
                            self.reader_state = ReaderState::DecodingBlock {
                                reader,
                                data: data_chunk,
                            };
                        }
                    }
                }
                ReaderState::InvalidState => {
                    return self.finish_with_error(AvroError::General(
                        "AsyncAvroFileReader in invalid state".into(),
                    ));
                }
                ReaderState::DecodingBlock { reader, mut data } => {
                    // Try to decode another block from the buffered reader.
                    let consumed = match self.block_decoder.decode(&data) {
                        Ok(consumed) => consumed,
                        Err(e) => return self.finish_with_error(e),
                    };
                    data = data.slice(consumed..);

                    // If we reached the end of the block, flush it, and move to read batches.
                    if let Some(block) = self.block_decoder.flush() {
                        // Successfully decoded a block.
                        let block_count = block.count;
                        let block_data = Bytes::from_owner(if let Some(ref codec) = self.codec {
                            match codec.decompress(&block.data) {
                                Ok(decompressed) => decompressed,
                                Err(e) => return self.finish_with_error(e),
                            }
                        } else {
                            block.data
                        });

                        // Since we have an active block, move to reading batches
                        self.reader_state = ReaderState::ReadingBatches {
                            reader,
                            data,
                            block_data,
                            remaining_in_block: block_count,
                        };
                        continue;
                    }

                    // data should always be consumed unless Finished, if it wasn't, something went wrong
                    if !data.is_empty() {
                        return self.finish_with_error(AvroError::General(
                            "BlockDecoder failed to make progress decoding Avro block".into(),
                        ));
                    }

                    if matches!(self.block_decoder.state(), BlockDecoderState::Finished) {
                        // We've already flushed, so if no batch was produced, we are simply done.
                        self.finishing_partial_block = false;
                        self.start_flushing();
                        continue;
                    }

                    // If we've tried the following stage before, and still can't decode,
                    // this means the file is truncated or corrupted.
                    if self.finishing_partial_block {
                        return self.finish_with_error(AvroError::EOF(
                            "Unexpected EOF while reading last Avro block".into(),
                        ));
                    }

                    // Avro splitting case: block is incomplete, we need to:
                    // 1. Parse the length so we know how much to read
                    // 2. Fetch more data from the object store
                    // 3. Create a new block data from the remaining slice and the newly fetched data
                    // 4. Continue decoding until end of block
                    self.finishing_partial_block = true;

                    // Mid-block, but we don't know how many bytes are missing yet
                    if matches!(
                        self.block_decoder.state(),
                        BlockDecoderState::Count | BlockDecoderState::Size
                    ) {
                        // Max VLQ header is 20 bytes (10 bytes each for count and size).
                        // Fetch just enough to complete it.
                        const MAX_VLQ_HEADER_SIZE: u64 = 20;
                        let fetch_end = (self.range.end + MAX_VLQ_HEADER_SIZE).min(self.file_size);

                        // If there is nothing more to fetch, error out
                        if fetch_end == self.range.end {
                            return self.finish_with_error(AvroError::EOF(
                                "Unexpected EOF while reading Avro block header".into(),
                            ));
                        }

                        let range_to_fetch = self.range.end..fetch_end;
                        self.range.end = fetch_end; // Track that we've fetched these bytes

                        let future = Self::fetch_bytes(reader, range_to_fetch).boxed();
                        self.reader_state = ReaderState::FetchingData {
                            future,
                            next_behaviour: FetchNextBehaviour::DecodeVLQHeader,
                        };
                        continue;
                    }

                    // Otherwise, we're mid-block but know how many bytes are remaining to fetch.
                    let range_to_fetch = match self.remaining_block_range() {
                        Ok(range) => range,
                        Err(e) => return self.finish_with_error(e),
                    };

                    let future = Self::fetch_bytes(reader, range_to_fetch).boxed();
                    self.reader_state = ReaderState::FetchingData {
                        future,
                        next_behaviour: FetchNextBehaviour::ContinueDecoding,
                    };
                    continue;
                }
                ReaderState::ReadingBatches {
                    reader,
                    data,
                    mut block_data,
                    mut remaining_in_block,
                } => {
                    let (consumed, records_decoded) =
                        match self.decoder.decode_block(&block_data, remaining_in_block) {
                            Ok((consumed, records_decoded)) => (consumed, records_decoded),
                            Err(e) => return self.finish_with_error(e),
                        };

                    remaining_in_block -= records_decoded;

                    if remaining_in_block == 0 {
                        if data.is_empty() {
                            // No more data to read, drain remaining buffered records
                            self.start_flushing();
                        } else {
                            // Finished this block, move to decode next block in the next iteration
                            self.reader_state = ReaderState::DecodingBlock { reader, data };
                        }
                    } else {
                        // Still more records to decode in this block, slice the already-read data and stay in this state
                        block_data = block_data.slice(consumed..);
                        self.reader_state = ReaderState::ReadingBatches {
                            reader,
                            data,
                            block_data,
                            remaining_in_block,
                        };
                    }

                    // We have a full batch ready, emit it
                    // (This is not mutually exclusive with the block being finished, so the state change is valid)
                    if self.decoder.batch_is_full() {
                        return match self.decoder.flush() {
                            Ok(Some(batch)) => Poll::Ready(Some(Ok(batch))),
                            Ok(None) => self.finish_with_error(AvroError::General(
                                "Decoder reported a full batch, but flush returned None".into(),
                            )),
                            Err(e) => self.finish_with_error(e),
                        };
                    }
                }
                ReaderState::Flushing => {
                    return self.poll_flush();
                }
                ReaderState::Finished => {
                    // Terminal: once finished (including after an error), always yield None
                    self.reader_state = ReaderState::Finished;
                    return Poll::Ready(None);
                }
            }
        }
    }
}

// To maintain compatibility with the expected stream results in the ecosystem, this returns ArrowError.
impl<R: AsyncFileReader + Unpin + 'static> Stream for AsyncAvroFileReader<R> {
    type Item = Result<RecordBatch, ArrowError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.read_next(cx).map_err(Into::into)
    }
}

#[cfg(all(test, feature = "object_store"))]
mod tests {
    use super::*;
    use crate::schema::{AvroSchema, SCHEMA_METADATA_KEY};
    use arrow_array::cast::AsArray;
    use arrow_array::types::{Int32Type, Int64Type};
    use arrow_array::*;
    use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
    use futures::{StreamExt, TryStreamExt};
    use object_store::ObjectStore;
    use object_store::local::LocalFileSystem;
    use object_store::path::Path;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn arrow_test_data(file: &str) -> String {
        let base =
            std::env::var("ARROW_TEST_DATA").unwrap_or_else(|_| "../testing/data".to_string());
        format!("{}/{}", base, file)
    }

    fn get_alltypes_schema() -> SchemaRef {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("bool_col", DataType::Boolean, true),
            Field::new("tinyint_col", DataType::Int32, true),
            Field::new("smallint_col", DataType::Int32, true),
            Field::new("int_col", DataType::Int32, true),
            Field::new("bigint_col", DataType::Int64, true),
            Field::new("float_col", DataType::Float32, true),
            Field::new("double_col", DataType::Float64, true),
            Field::new("date_string_col", DataType::Binary, true),
            Field::new("string_col", DataType::Binary, true),
            Field::new(
                "timestamp_col",
                DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
                true,
            ),
        ])
        .with_metadata(HashMap::from([(
            SCHEMA_METADATA_KEY.into(),
            r#"{
    "type": "record",
    "name": "topLevelRecord",
    "fields": [
        {
            "name": "id",
            "type": [
                "int",
                "null"
            ]
        },
        {
            "name": "bool_col",
            "type": [
                "boolean",
                "null"
            ]
        },
        {
            "name": "tinyint_col",
            "type": [
                "int",
                "null"
            ]
        },
        {
            "name": "smallint_col",
            "type": [
                "int",
                "null"
            ]
        },
        {
            "name": "int_col",
            "type": [
                "int",
                "null"
            ]
        },
        {
            "name": "bigint_col",
            "type": [
                "long",
                "null"
            ]
        },
        {
            "name": "float_col",
            "type": [
                "float",
                "null"
            ]
        },
        {
            "name": "double_col",
            "type": [
                "double",
                "null"
            ]
        },
        {
            "name": "date_string_col",
            "type": [
                "bytes",
                "null"
            ]
        },
        {
            "name": "string_col",
            "type": [
                "bytes",
                "null"
            ]
        },
        {
            "name": "timestamp_col",
            "type": [
                {
                    "type": "long",
                    "logicalType": "timestamp-micros"
                },
                "null"
            ]
        }
    ]
}
"#
            .into(),
        )]));
        Arc::new(schema)
    }

    fn get_alltypes_with_nulls_schema() -> SchemaRef {
        let schema = Schema::new(vec![
            Field::new("string_col", DataType::Binary, true),
            Field::new("int_col", DataType::Int32, true),
            Field::new("bool_col", DataType::Boolean, true),
            Field::new("bigint_col", DataType::Int64, true),
            Field::new("float_col", DataType::Float32, true),
            Field::new("double_col", DataType::Float64, true),
            Field::new("bytes_col", DataType::Binary, true),
        ])
        .with_metadata(HashMap::from([(
            SCHEMA_METADATA_KEY.into(),
            r#"{
    "type": "record",
    "name": "topLevelRecord",
    "fields": [
        {
            "name": "string_col",
            "type": [
                "null",
                "string"
            ],
            "default": null
        },
        {
            "name": "int_col",
            "type": [
                "null",
                "int"
            ],
            "default": null
        },
        {
            "name": "bool_col",
            "type": [
                "null",
                "boolean"
            ],
            "default": null
        },
        {
            "name": "bigint_col",
            "type": [
                "null",
                "long"
            ],
            "default": null
        },
        {
            "name": "float_col",
            "type": [
                "null",
                "float"
            ],
            "default": null
        },
        {
            "name": "double_col",
            "type": [
                "null",
                "double"
            ],
            "default": null
        },
        {
            "name": "bytes_col",
            "type": [
                "null",
                "bytes"
            ],
            "default": null
        }
    ]
}"#
            .into(),
        )]));

        Arc::new(schema)
    }

    fn get_nested_records_schema() -> SchemaRef {
        let schema = Schema::new(vec![
            Field::new(
                "f1",
                DataType::Struct(
                    vec![
                        Field::new("f1_1", DataType::Utf8, false),
                        Field::new("f1_2", DataType::Int32, false),
                        Field::new(
                            "f1_3",
                            DataType::Struct(
                                vec![Field::new("f1_3_1", DataType::Float64, false)].into(),
                            ),
                            false,
                        ),
                    ]
                    .into(),
                ),
                false,
            ),
            Field::new(
                "f2",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Struct(
                        vec![
                            Field::new("f2_1", DataType::Boolean, false),
                            Field::new("f2_2", DataType::Float32, false),
                        ]
                        .into(),
                    ),
                    false,
                ))),
                false,
            ),
            Field::new(
                "f3",
                DataType::Struct(vec![Field::new("f3_1", DataType::Utf8, false)].into()),
                true,
            ),
            Field::new(
                "f4",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Struct(vec![Field::new("f4_1", DataType::Int64, false)].into()),
                    true,
                ))),
                false,
            ),
        ])
        .with_metadata(HashMap::from([(
            SCHEMA_METADATA_KEY.into(),
            r#"{
    "type": "record",
    "namespace": "ns1",
    "name": "record1",
    "fields": [
        {
            "name": "f1",
            "type": {
                "type": "record",
                "namespace": "ns2",
                "name": "record2",
                "fields": [
                    {
                        "name": "f1_1",
                        "type": "string"
                    },
                    {
                        "name": "f1_2",
                        "type": "int"
                    },
                    {
                        "name": "f1_3",
                        "type": {
                            "type": "record",
                            "namespace": "ns3",
                            "name": "record3",
                            "fields": [
                                {
                                    "name": "f1_3_1",
                                    "type": "double"
                                }
                            ]
                        }
                    }
                ]
            }
        },
        {
            "name": "f2",
            "type": {
                "type": "array",
                "items": {
                    "type": "record",
                    "namespace": "ns4",
                    "name": "record4",
                    "fields": [
                        {
                            "name": "f2_1",
                            "type": "boolean"
                        },
                        {
                            "name": "f2_2",
                            "type": "float"
                        }
                    ]
                }
            }
        },
        {
            "name": "f3",
            "type": [
                "null",
                {
                    "type": "record",
                    "namespace": "ns5",
                    "name": "record5",
                    "fields": [
                        {
                            "name": "f3_1",
                            "type": "string"
                        }
                    ]
                }
            ],
            "default": null
        },
        {
            "name": "f4",
            "type": {
                "type": "array",
                "items": [
                    "null",
                    {
                        "type": "record",
                        "namespace": "ns6",
                        "name": "record6",
                        "fields": [
                            {
                                "name": "f4_1",
                                "type": "long"
                            }
                        ]
                    }
                ]
            }
        }
    ]
}
"#
            .into(),
        )]));

        Arc::new(schema)
    }

    async fn read_async_file(
        path: &str,
        batch_size: usize,
        range: Option<Range<u64>>,
        schema: Option<SchemaRef>,
        projection: Option<Vec<usize>>,
    ) -> Result<Vec<RecordBatch>, ArrowError> {
        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
        let location = Path::from_filesystem_path(path).unwrap();

        let file_size = store.head(&location).await.unwrap().size;

        let file_reader = AvroObjectReader::new(store, location);
        let mut builder = AsyncAvroFileReader::builder(file_reader, file_size, batch_size);

        if let Some(s) = schema {
            let reader_schema = AvroSchema::try_from(s.as_ref())?;
            builder = builder.with_reader_schema(reader_schema);
        }

        if let Some(proj) = projection {
            builder = builder.with_projection(proj);
        }

        if let Some(range) = range {
            builder = builder.with_range(range);
        }

        let reader = builder.try_build().await?;
        reader.try_collect().await
    }

    #[tokio::test]
    async fn test_full_file_read() {
        let file = arrow_test_data("avro/alltypes_plain.avro");
        let schema = get_alltypes_schema();
        let batches = read_async_file(&file, 1024, None, Some(schema), None)
            .await
            .unwrap();
        let batch = &batches[0];

        assert_eq!(batch.num_rows(), 8);
        assert_eq!(batch.num_columns(), 11);

        let id_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id_array.value(0), 4);
        assert_eq!(id_array.value(7), 1);
    }

    #[tokio::test]
    async fn test_small_batch_size() {
        let file = arrow_test_data("avro/alltypes_plain.avro");
        let schema = get_alltypes_schema();
        let batches = read_async_file(&file, 2, None, Some(schema), None)
            .await
            .unwrap();
        assert_eq!(batches.len(), 4);

        let batch = &batches[0];

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 11);
    }

    #[tokio::test]
    async fn test_batch_size_one() {
        let file = arrow_test_data("avro/alltypes_plain.avro");
        let schema = get_alltypes_schema();
        let batches = read_async_file(&file, 1, None, Some(schema), None)
            .await
            .unwrap();
        let batch = &batches[0];

        assert_eq!(batches.len(), 8);
        assert_eq!(batch.num_rows(), 1);
    }

    #[tokio::test]
    async fn test_batch_size_larger_than_file() {
        let file = arrow_test_data("avro/alltypes_plain.avro");
        let schema = get_alltypes_schema();
        let batches = read_async_file(&file, 10000, None, Some(schema), None)
            .await
            .unwrap();
        let batch = &batches[0];

        assert_eq!(batch.num_rows(), 8);
    }

    #[tokio::test]
    async fn test_empty_range() {
        let file = arrow_test_data("avro/alltypes_plain.avro");
        let range = 100..100;
        let schema = get_alltypes_schema();
        let batches = read_async_file(&file, 1024, Some(range), Some(schema), None)
            .await
            .unwrap();
        assert_eq!(batches.len(), 0);
    }

    #[tokio::test]
    async fn test_range_starting_at_zero() {
        // Tests that range starting at 0 correctly skips header
        let file = arrow_test_data("avro/alltypes_plain.avro");
        let store = Arc::new(LocalFileSystem::new());
        let location = Path::from_filesystem_path(&file).unwrap();
        let meta = store.head(&location).await.unwrap();

        let range = 0..meta.size;
        let schema = get_alltypes_schema();
        let batches = read_async_file(&file, 1024, Some(range), Some(schema), None)
            .await
            .unwrap();
        let batch = &batches[0];

        assert_eq!(batch.num_rows(), 8);
    }

    #[tokio::test]
    async fn test_range_after_header() {
        let file = arrow_test_data("avro/alltypes_plain.avro");
        let store = Arc::new(LocalFileSystem::new());
        let location = Path::from_filesystem_path(&file).unwrap();
        let meta = store.head(&location).await.unwrap();

        let range = 100..meta.size;
        let schema = get_alltypes_schema();
        let batches = read_async_file(&file, 1024, Some(range), Some(schema), None)
            .await
            .unwrap();
        let batch = &batches[0];

        assert!(batch.num_rows() > 0);
    }

    #[tokio::test]
    async fn test_range_no_sync_marker() {
        // Small range unlikely to contain sync marker
        let file = arrow_test_data("avro/alltypes_plain.avro");
        let range = 50..150;
        let schema = get_alltypes_schema();
        let batches = read_async_file(&file, 1024, Some(range), Some(schema), None)
            .await
            .unwrap();
        assert_eq!(batches.len(), 0);
    }

    #[tokio::test]
    async fn test_range_starting_mid_file() {
        let file = arrow_test_data("avro/alltypes_plain.avro");

        let range = 700..768; // Header ends at 675, so this should be mid-block
        let schema = get_alltypes_schema();
        let batches = read_async_file(&file, 1024, Some(range), Some(schema), None)
            .await
            .unwrap();
        assert_eq!(batches.len(), 0);
    }

    #[tokio::test]
    async fn test_range_ending_at_file_size() {
        let file = arrow_test_data("avro/alltypes_plain.avro");
        let store = Arc::new(LocalFileSystem::new());
        let location = Path::from_filesystem_path(&file).unwrap();
        let meta = store.head(&location).await.unwrap();

        let range = 200..meta.size;
        let schema = get_alltypes_schema();
        let batches = read_async_file(&file, 1024, Some(range), Some(schema), None)
            .await
            .unwrap();
        let batch = &batches[0];

        assert_eq!(batch.num_rows(), 8);
    }

    #[tokio::test]
    async fn test_incomplete_block_requires_fetch() {
        // Range ends mid-block, should trigger fetching_rem_block logic
        let file = arrow_test_data("avro/alltypes_plain.avro");
        let range = 0..1200;
        let schema = get_alltypes_schema();
        let batches = read_async_file(&file, 1024, Some(range), Some(schema), None)
            .await
            .unwrap();
        let batch = &batches[0];

        assert_eq!(batch.num_rows(), 8)
    }

    #[tokio::test]
    async fn test_partial_vlq_header_requires_fetch() {
        // Range ends mid-VLQ header, triggering the Count|Size partial fetch logic.
        let file = arrow_test_data("avro/alltypes_plain.avro");
        let range = 16..676; // Header should end at 675
        let schema = get_alltypes_schema();
        let batches = read_async_file(&file, 1024, Some(range), Some(schema), None)
            .await
            .unwrap();
        let batch = &batches[0];

        assert_eq!(batch.num_rows(), 8)
    }

    #[cfg(feature = "snappy")]
    #[tokio::test]
    async fn test_snappy_compressed_with_range() {
        {
            let file = arrow_test_data("avro/alltypes_plain.snappy.avro");
            let store = Arc::new(LocalFileSystem::new());
            let location = Path::from_filesystem_path(&file).unwrap();
            let meta = store.head(&location).await.unwrap();

            let range = 200..meta.size;
            let schema = get_alltypes_schema();
            let batches = read_async_file(&file, 1024, Some(range), Some(schema), None)
                .await
                .unwrap();
            let batch = &batches[0];

            assert!(batch.num_rows() > 0);
        }
    }

    #[tokio::test]
    async fn test_nulls() {
        let file = arrow_test_data("avro/alltypes_nulls_plain.avro");
        let schema = get_alltypes_with_nulls_schema();
        let batches = read_async_file(&file, 1024, None, Some(schema), None)
            .await
            .unwrap();
        let batch = &batches[0];

        assert_eq!(batch.num_rows(), 1);
        for col in batch.columns() {
            assert!(col.is_null(0));
        }
    }

    #[tokio::test]
    async fn test_nested_records() {
        let file = arrow_test_data("avro/nested_records.avro");
        let schema = get_nested_records_schema();
        let batches = read_async_file(&file, 1024, None, Some(schema), None)
            .await
            .unwrap();
        let batch = &batches[0];

        assert_eq!(batch.num_rows(), 2);
        assert!(batch.num_columns() > 0);
    }

    #[tokio::test]
    async fn test_stream_produces_multiple_batches() {
        let file = arrow_test_data("avro/alltypes_plain.avro");
        let store = Arc::new(LocalFileSystem::new());
        let location = Path::from_filesystem_path(&file).unwrap();

        let file_size = store.head(&location).await.unwrap().size;

        let file_reader = AvroObjectReader::new(store, location);
        let schema = get_alltypes_schema();
        let reader_schema = AvroSchema::try_from(schema.as_ref()).unwrap();
        let reader = AsyncAvroFileReader::builder(
            file_reader,
            file_size,
            2, // Small batch size to force multiple batches
        )
        .with_reader_schema(reader_schema)
        .try_build()
        .await
        .unwrap();

        let batches: Vec<RecordBatch> = reader.try_collect().await.unwrap();

        assert!(batches.len() > 1);
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 8);
    }

    #[tokio::test]
    async fn test_stream_early_termination() {
        let file = arrow_test_data("avro/alltypes_plain.avro");
        let store = Arc::new(LocalFileSystem::new());
        let location = Path::from_filesystem_path(&file).unwrap();

        let file_size = store.head(&location).await.unwrap().size;

        let file_reader = AvroObjectReader::new(store, location);
        let schema = get_alltypes_schema();
        let reader_schema = AvroSchema::try_from(schema.as_ref()).unwrap();
        let reader = AsyncAvroFileReader::builder(file_reader, file_size, 1)
            .with_reader_schema(reader_schema)
            .try_build()
            .await
            .unwrap();

        let first_batch = reader.take(1).try_collect::<Vec<_>>().await.unwrap();

        assert_eq!(first_batch.len(), 1);
        assert!(first_batch[0].num_rows() > 0);
    }

    #[tokio::test]
    async fn test_various_batch_sizes() {
        let file = arrow_test_data("avro/alltypes_plain.avro");

        for batch_size in [1, 2, 3, 5, 7, 11, 100] {
            let schema = get_alltypes_schema();
            let batches = read_async_file(&file, batch_size, None, Some(schema), None)
                .await
                .unwrap();
            let batch = &batches[0];

            // Size should be what was provided, to the limit of the batch in the file
            assert_eq!(
                batch.num_rows(),
                batch_size.min(8),
                "Failed with batch_size={}",
                batch_size
            );
        }
    }

    #[tokio::test]
    async fn test_range_larger_than_file() {
        let file = arrow_test_data("avro/alltypes_plain.avro");
        let store = Arc::new(LocalFileSystem::new());
        let location = Path::from_filesystem_path(&file).unwrap();
        let meta = store.head(&location).await.unwrap();

        // Range extends beyond file size
        let range = 100..(meta.size + 1000);
        let schema = get_alltypes_schema();
        let batches = read_async_file(&file, 1024, Some(range), Some(schema), None)
            .await
            .unwrap();
        let batch = &batches[0];

        // Should clamp to file size
        assert_eq!(batch.num_rows(), 8);
    }

    #[tokio::test]
    async fn test_roundtrip_write_then_async_read() {
        use crate::writer::AvroWriter;
        use arrow_array::{Float64Array, StringArray};
        use std::fs::File;
        use std::io::BufWriter;
        use tempfile::tempdir;

        // Schema with nullable and non-nullable fields of various types
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
            Field::new("count", DataType::Int64, false),
        ]));

        let dir = tempdir().unwrap();
        let file_path = dir.path().join("roundtrip_test.avro");

        // Write multiple batches with nulls
        {
            let file = File::create(&file_path).unwrap();
            let writer = BufWriter::new(file);
            let mut avro_writer = AvroWriter::new(writer, schema.as_ref().clone()).unwrap();

            // First batch: 3 rows with some nulls
            let batch1 = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![1, 2, 3])),
                    Arc::new(StringArray::from(vec![
                        Some("alice"),
                        None,
                        Some("charlie"),
                    ])),
                    Arc::new(Float64Array::from(vec![Some(95.5), Some(87.3), None])),
                    Arc::new(Int64Array::from(vec![10, 20, 30])),
                ],
            )
            .unwrap();
            avro_writer.write(&batch1).unwrap();

            // Second batch: 2 rows
            let batch2 = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![4, 5])),
                    Arc::new(StringArray::from(vec![Some("diana"), Some("eve")])),
                    Arc::new(Float64Array::from(vec![None, Some(88.0)])),
                    Arc::new(Int64Array::from(vec![40, 50])),
                ],
            )
            .unwrap();
            avro_writer.write(&batch2).unwrap();

            avro_writer.finish().unwrap();
        }

        // Read back with small batch size to produce multiple output batches
        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
        let location = Path::from_filesystem_path(&file_path).unwrap();
        let file_size = store.head(&location).await.unwrap().size;

        let file_reader = AvroObjectReader::new(store, location);
        let reader = AsyncAvroFileReader::builder(file_reader, file_size, 2)
            .try_build()
            .await
            .unwrap();

        let batches: Vec<RecordBatch> = reader.try_collect().await.unwrap();

        // Verify we got multiple output batches due to small batch_size
        assert!(
            batches.len() > 1,
            "Expected multiple batches with batch_size=2"
        );

        // Verify total row count
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 5);

        // Concatenate all batches to verify data
        let combined = arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap();
        assert_eq!(combined.num_rows(), 5);
        assert_eq!(combined.num_columns(), 4);

        // Check id column (non-nullable)
        let id_array = combined
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id_array.values(), &[1, 2, 3, 4, 5]);

        // Check name column (nullable) - verify nulls are preserved
        // Avro strings are read as Binary by default
        let name_col = combined.column(1);
        let name_array = name_col.as_string::<i32>();
        assert_eq!(name_array.value(0), "alice");
        assert!(name_col.is_null(1)); // second row has null name
        assert_eq!(name_array.value(2), "charlie");

        // Check score column (nullable) - verify nulls are preserved
        let score_array = combined
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(!score_array.is_null(0));
        assert!((score_array.value(0) - 95.5).abs() < f64::EPSILON);
        assert!(score_array.is_null(2)); // third row has null score
        assert!(score_array.is_null(3)); // fourth row has null score
        assert!(!score_array.is_null(4));
        assert!((score_array.value(4) - 88.0).abs() < f64::EPSILON);

        // Check count column (non-nullable)
        let count_array = combined
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(count_array.values(), &[10, 20, 30, 40, 50]);
    }

    #[tokio::test]
    async fn test_alltypes_no_schema_no_projection() {
        // No reader schema, no projection - uses writer schema from file
        let file = arrow_test_data("avro/alltypes_plain.avro");
        let batches = read_async_file(&file, 1024, None, None, None)
            .await
            .unwrap();
        let batch = &batches[0];

        assert_eq!(batch.num_rows(), 8);
        assert_eq!(batch.num_columns(), 11);
        assert_eq!(batch.schema().field(0).name(), "id");
    }

    #[tokio::test]
    async fn test_alltypes_no_schema_with_projection() {
        // No reader schema, with projection - project writer schema
        let file = arrow_test_data("avro/alltypes_plain.avro");
        // Project [tinyint_col, id, bigint_col] = indices [2, 0, 5]
        let batches = read_async_file(&file, 1024, None, None, Some(vec![2, 0, 5]))
            .await
            .unwrap();
        let batch = &batches[0];

        assert_eq!(batch.num_rows(), 8);
        assert_eq!(batch.num_columns(), 3);
        assert_eq!(batch.schema().field(0).name(), "tinyint_col");
        assert_eq!(batch.schema().field(1).name(), "id");
        assert_eq!(batch.schema().field(2).name(), "bigint_col");

        // Verify data values
        let tinyint_col = batch.column(0).as_primitive::<Int32Type>();
        assert_eq!(tinyint_col.values(), &[0, 1, 0, 1, 0, 1, 0, 1]);

        let id = batch.column(1).as_primitive::<Int32Type>();
        assert_eq!(id.values(), &[4, 5, 6, 7, 2, 3, 0, 1]);

        let bigint_col = batch.column(2).as_primitive::<Int64Type>();
        assert_eq!(bigint_col.values(), &[0, 10, 0, 10, 0, 10, 0, 10]);
    }

    #[tokio::test]
    async fn test_alltypes_with_schema_no_projection() {
        // With reader schema, no projection
        let file = arrow_test_data("avro/alltypes_plain.avro");
        let schema = get_alltypes_schema();
        let batches = read_async_file(&file, 1024, None, Some(schema), None)
            .await
            .unwrap();
        let batch = &batches[0];

        assert_eq!(batch.num_rows(), 8);
        assert_eq!(batch.num_columns(), 11);
    }

    #[tokio::test]
    async fn test_alltypes_with_schema_with_projection() {
        // With reader schema, with projection
        let file = arrow_test_data("avro/alltypes_plain.avro");
        let schema = get_alltypes_schema();
        // Project [bool_col, id] = indices [1, 0]
        let batches = read_async_file(&file, 1024, None, Some(schema), Some(vec![1, 0]))
            .await
            .unwrap();
        let batch = &batches[0];

        assert_eq!(batch.num_rows(), 8);
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.schema().field(0).name(), "bool_col");
        assert_eq!(batch.schema().field(1).name(), "id");

        let bool_col = batch.column(0).as_boolean();
        assert!(bool_col.value(0));
        assert!(!bool_col.value(1));

        let id = batch.column(1).as_primitive::<Int32Type>();
        assert_eq!(id.values(), &[4, 5, 6, 7, 2, 3, 0, 1]);
    }

    #[tokio::test]
    async fn test_nested_no_schema_no_projection() {
        // No reader schema, no projection
        let file = arrow_test_data("avro/nested_records.avro");
        let batches = read_async_file(&file, 1024, None, None, None)
            .await
            .unwrap();
        let batch = &batches[0];

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 4);
        assert_eq!(batch.schema().field(0).name(), "f1");
        assert_eq!(batch.schema().field(1).name(), "f2");
        assert_eq!(batch.schema().field(2).name(), "f3");
        assert_eq!(batch.schema().field(3).name(), "f4");
    }

    #[tokio::test]
    async fn test_nested_no_schema_with_projection() {
        // No reader schema, with projection - reorder nested fields
        let file = arrow_test_data("avro/nested_records.avro");
        // Project [f3, f1] = indices [2, 0]
        let batches = read_async_file(&file, 1024, None, None, Some(vec![2, 0]))
            .await
            .unwrap();
        let batch = &batches[0];

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.schema().field(0).name(), "f3");
        assert_eq!(batch.schema().field(1).name(), "f1");
    }

    #[tokio::test]
    async fn test_nested_with_schema_no_projection() {
        // With reader schema, no projection
        let file = arrow_test_data("avro/nested_records.avro");
        let schema = get_nested_records_schema();
        let batches = read_async_file(&file, 1024, None, Some(schema), None)
            .await
            .unwrap();
        let batch = &batches[0];

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 4);
    }

    #[tokio::test]
    async fn test_nested_with_schema_with_projection() {
        // With reader schema, with projection
        let file = arrow_test_data("avro/nested_records.avro");
        let schema = get_nested_records_schema();
        // Project [f4, f2, f1] = indices [3, 1, 0]
        let batches = read_async_file(&file, 1024, None, Some(schema), Some(vec![3, 1, 0]))
            .await
            .unwrap();
        let batch = &batches[0];

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3);
        assert_eq!(batch.schema().field(0).name(), "f4");
        assert_eq!(batch.schema().field(1).name(), "f2");
        assert_eq!(batch.schema().field(2).name(), "f1");
    }

    #[tokio::test]
    async fn test_projection_error_out_of_bounds() {
        let file = arrow_test_data("avro/alltypes_plain.avro");
        // Index 100 is out of bounds for the 11-field schema
        let err = read_async_file(&file, 1024, None, None, Some(vec![100]))
            .await
            .unwrap_err();
        assert!(matches!(err, ArrowError::AvroError(_)));
        assert!(err.to_string().contains("out of bounds"));
    }

    #[tokio::test]
    async fn test_projection_error_duplicate_index() {
        let file = arrow_test_data("avro/alltypes_plain.avro");
        // Duplicate index 0
        let err = read_async_file(&file, 1024, None, None, Some(vec![0, 0]))
            .await
            .unwrap_err();
        assert!(matches!(err, ArrowError::AvroError(_)));
        assert!(err.to_string().contains("Duplicate projection index"));
    }

    #[tokio::test]
    async fn test_with_header_size_hint_small() {
        // Use a very small header size hint to force multiple fetches
        let file = arrow_test_data("avro/alltypes_plain.avro");
        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
        let location = Path::from_filesystem_path(&file).unwrap();
        let file_size = store.head(&location).await.unwrap().size;

        let file_reader = AvroObjectReader::new(store, location);
        let schema = get_alltypes_schema();
        let reader_schema = AvroSchema::try_from(schema.as_ref()).unwrap();

        // Use a tiny header hint (64 bytes) - header is much larger
        let reader = AsyncAvroFileReader::builder(file_reader, file_size, 1024)
            .with_reader_schema(reader_schema)
            .with_header_size_hint(64)
            .try_build()
            .await
            .unwrap();

        let batches: Vec<RecordBatch> = reader.try_collect().await.unwrap();
        let batch = &batches[0];

        assert_eq!(batch.num_rows(), 8);
        assert_eq!(batch.num_columns(), 11);
    }

    #[tokio::test]
    async fn test_with_header_size_hint_large() {
        // Use a larger header size hint than needed
        let file = arrow_test_data("avro/alltypes_plain.avro");
        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
        let location = Path::from_filesystem_path(&file).unwrap();
        let file_size = store.head(&location).await.unwrap().size;

        let file_reader = AvroObjectReader::new(store, location);
        let schema = get_alltypes_schema();
        let reader_schema = AvroSchema::try_from(schema.as_ref()).unwrap();

        // Use a large header hint (64KB)
        let reader = AsyncAvroFileReader::builder(file_reader, file_size, 1024)
            .with_reader_schema(reader_schema)
            .with_header_size_hint(64 * 1024)
            .try_build()
            .await
            .unwrap();

        let batches: Vec<RecordBatch> = reader.try_collect().await.unwrap();
        let batch = &batches[0];

        assert_eq!(batch.num_rows(), 8);
        assert_eq!(batch.num_columns(), 11);
    }

    #[tokio::test]
    async fn test_with_utf8_view_enabled() {
        // Test that utf8_view produces StringViewArray instead of StringArray
        let file = arrow_test_data("avro/nested_records.avro");
        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
        let location = Path::from_filesystem_path(&file).unwrap();
        let file_size = store.head(&location).await.unwrap().size;

        let file_reader = AvroObjectReader::new(store, location);

        let reader = AsyncAvroFileReader::builder(file_reader, file_size, 1024)
            .with_utf8_view(true)
            .try_build()
            .await
            .unwrap();

        let batches: Vec<RecordBatch> = reader.try_collect().await.unwrap();
        let batch = &batches[0];

        assert_eq!(batch.num_rows(), 2);

        // The f1 struct contains f1_1 which is a string field
        // With utf8_view enabled, it should be Utf8View type
        let f1_col = batch.column(0);
        let f1_struct = f1_col.as_struct();
        let f1_1_field = f1_struct.column_by_name("f1_1").unwrap();

        // Check that the data type is Utf8View
        assert_eq!(f1_1_field.data_type(), &DataType::Utf8View);
    }

    #[tokio::test]
    async fn test_with_utf8_view_disabled() {
        // Test that without utf8_view, we get regular Utf8
        let file = arrow_test_data("avro/nested_records.avro");
        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
        let location = Path::from_filesystem_path(&file).unwrap();
        let file_size = store.head(&location).await.unwrap().size;

        let file_reader = AvroObjectReader::new(store, location);

        let reader = AsyncAvroFileReader::builder(file_reader, file_size, 1024)
            .with_utf8_view(false)
            .try_build()
            .await
            .unwrap();

        let batches: Vec<RecordBatch> = reader.try_collect().await.unwrap();
        let batch = &batches[0];

        assert_eq!(batch.num_rows(), 2);

        // The f1 struct contains f1_1 which is a string field
        // Without utf8_view, it should be regular Utf8
        let f1_col = batch.column(0);
        let f1_struct = f1_col.as_struct();
        let f1_1_field = f1_struct.column_by_name("f1_1").unwrap();

        assert_eq!(f1_1_field.data_type(), &DataType::Utf8);
    }

    #[tokio::test]
    async fn test_with_strict_mode_disabled_allows_null_second() {
        // Test that with strict_mode disabled, unions of ['T', 'null'] are allowed
        // The alltypes_nulls_plain.avro file has unions with null second
        let file = arrow_test_data("avro/alltypes_nulls_plain.avro");
        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
        let location = Path::from_filesystem_path(&file).unwrap();
        let file_size = store.head(&location).await.unwrap().size;

        let file_reader = AvroObjectReader::new(store, location);

        // Without strict mode, this should succeed
        let reader = AsyncAvroFileReader::builder(file_reader, file_size, 1024)
            .with_strict_mode(false)
            .try_build()
            .await
            .unwrap();

        let batches: Vec<RecordBatch> = reader.try_collect().await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn test_with_strict_mode_enabled_rejects_null_second() {
        // Test that with strict_mode enabled, unions of ['T', 'null'] are rejected
        // The alltypes_plain.avro file has unions like ["int", "null"] (null second)
        let file = arrow_test_data("avro/alltypes_plain.avro");
        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
        let location = Path::from_filesystem_path(&file).unwrap();
        let file_size = store.head(&location).await.unwrap().size;

        let file_reader = AvroObjectReader::new(store, location);

        // With strict mode, this should fail because of ['T', 'null'] unions
        let result = AsyncAvroFileReader::builder(file_reader, file_size, 1024)
            .with_strict_mode(true)
            .try_build()
            .await;

        match result {
            Ok(_) => panic!("Expected error for strict_mode with ['T', 'null'] union"),
            Err(err) => {
                assert!(
                    err.to_string().contains("disallowed in strict_mode"),
                    "Expected strict_mode error, got: {}",
                    err
                );
            }
        }
    }

    #[tokio::test]
    async fn test_with_strict_mode_enabled_valid_schema() {
        // Test that strict_mode works with schemas that have proper ['null', 'T'] unions
        // The nested_records.avro file has properly ordered unions
        let file = arrow_test_data("avro/nested_records.avro");
        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
        let location = Path::from_filesystem_path(&file).unwrap();
        let file_size = store.head(&location).await.unwrap().size;

        let file_reader = AvroObjectReader::new(store, location);

        // With strict mode, properly ordered unions should still work
        let reader = AsyncAvroFileReader::builder(file_reader, file_size, 1024)
            .with_strict_mode(true)
            .try_build()
            .await
            .unwrap();

        let batches: Vec<RecordBatch> = reader.try_collect().await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
    }

    #[tokio::test]
    async fn test_builder_options_combined() {
        // Test combining multiple builder options
        let file = arrow_test_data("avro/nested_records.avro");
        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
        let location = Path::from_filesystem_path(&file).unwrap();
        let file_size = store.head(&location).await.unwrap().size;

        let file_reader = AvroObjectReader::new(store, location);

        let reader = AsyncAvroFileReader::builder(file_reader, file_size, 2)
            .with_header_size_hint(128)
            .with_utf8_view(true)
            .with_strict_mode(true)
            .with_projection(vec![0, 2]) // f1 and f3
            .try_build()
            .await
            .unwrap();

        let batches: Vec<RecordBatch> = reader.try_collect().await.unwrap();
        let batch = &batches[0];

        // Should have 2 columns (f1 and f3) due to projection
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.schema().field(0).name(), "f1");
        assert_eq!(batch.schema().field(1).name(), "f3");

        // Verify utf8_view is applied
        let f1_col = batch.column(0);
        let f1_struct = f1_col.as_struct();
        let f1_1_field = f1_struct.column_by_name("f1_1").unwrap();
        assert_eq!(f1_1_field.data_type(), &DataType::Utf8View);
    }
}
