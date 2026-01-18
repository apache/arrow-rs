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

enum ReaderState<R: AsyncFileReader> {
    /// Intermediate state to fix ownership issues
    Limbo,
    /// Initial state, fetch initial range
    Idle { reader: R },
    /// Fetching data from the reader
    FetchingData {
        future: BoxFuture<'static, Result<(R, Bytes), ArrowError>>,
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
pub struct AsyncAvroFileReader<R: AsyncFileReader> {
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

impl<R: AsyncFileReader + Unpin + 'static> AsyncAvroFileReader<R> {
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
    fn remaining_block_range(&self) -> Result<Range<u64>, ArrowError> {
        let remaining = self.block_decoder.bytes_remaining() as u64
            + match self.block_decoder.state() {
                BlockDecoderState::Data => 16, // Include sync marker
                BlockDecoderState::Sync => 0,
                state => {
                    return Err(ArrowError::AvroError(format!(
                        "remaining_block_range called in unexpected state: {state:?}"
                    )));
                }
            };

        let fetch_end = self.range.end + remaining;
        if fetch_end > self.file_size {
            return Err(ArrowError::AvroError(
                "Avro block requires more bytes than what exists in the file".into(),
            ));
        }

        Ok(self.range.end..fetch_end)
    }

    fn read_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<RecordBatch, ArrowError>>> {
        loop {
            match mem::replace(&mut self.reader_state, ReaderState::Limbo) {
                ReaderState::Idle { mut reader } => {
                    let range = self.range.clone();
                    if range.start >= range.end {
                        return Poll::Ready(Some(Err(ArrowError::AvroError(format!(
                            "Invalid range specified for Avro file: start {} >= end {}, file_size: {}",
                            range.start, range.end, self.file_size
                        )))));
                    }

                    let future = async move {
                        let data = reader.get_bytes(range).await?;
                        Ok((reader, data))
                    }
                    .boxed();

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
                        Poll::Ready(Err(e)) => {
                            return Poll::Ready(Some(Err(e)));
                        }
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
                            let mut reader = reader;
                            let mut data = data_chunk;

                            // Feed bytes one at a time until we reach Data state (VLQ header complete)
                            while !matches!(self.block_decoder.state(), BlockDecoderState::Data) {
                                if data.is_empty() {
                                    return Poll::Ready(Some(Err(ArrowError::AvroError(
                                        "Unexpected EOF while reading Avro block header".into(),
                                    ))));
                                }
                                let consumed = self.block_decoder.decode(&data[..1])?;
                                if consumed == 0 {
                                    return Poll::Ready(Some(Err(ArrowError::AvroError(
                                        "BlockDecoder failed to consume byte during VLQ header parsing"
                                            .into(),
                                    ))));
                                }
                                data = data.slice(consumed..);
                            }

                            // Now we know the block size. Slice remaining data to what we need.
                            let bytes_remaining = self.block_decoder.bytes_remaining();
                            let data_to_use = data.slice(..data.len().min(bytes_remaining));
                            let consumed = self.block_decoder.decode(&data_to_use)?;
                            if consumed != data_to_use.len() {
                                return Poll::Ready(Some(Err(ArrowError::AvroError(
                                    "BlockDecoder failed to consume all bytes after VLQ header parsing"
                                        .into(),
                                ))));
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
                                Err(e) => return Poll::Ready(Some(Err(e))),
                            };

                            let future = async move {
                                let data = reader.get_bytes(range_to_fetch).await?;
                                Ok((reader, data))
                            }
                            .boxed();
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
                ReaderState::Limbo => {
                    unreachable!("ReaderState::Limbo should never be observed");
                }
                ReaderState::DecodingBlock {
                    mut reader,
                    mut data,
                } => {
                    // Try to decode another block from the buffered reader.
                    let consumed = self.block_decoder.decode(&data)?;
                    data = data.slice(consumed..);

                    // If we reached the end of the block, flush it, and move to read batches.
                    if let Some(block) = self.block_decoder.flush() {
                        // Successfully decoded a block.
                        let block_count = block.count;
                        let block_data = Bytes::from_owner(if let Some(ref codec) = self.codec {
                            codec.decompress(&block.data)?
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
                        return Poll::Ready(Some(Err(ArrowError::AvroError(
                            "Unable to make progress decoding Avro block, data may be corrupted"
                                .into(),
                        ))));
                    }

                    if matches!(self.block_decoder.state(), BlockDecoderState::Finished) {
                        // We've already flushed, so if no batch was produced, we are simply done.
                        self.reader_state = ReaderState::Finished;
                        continue;
                    }

                    // If we've tried the following stage before, and still can't decode,
                    // this means the file is truncated or corrupted.
                    if self.finishing_partial_block {
                        return Poll::Ready(Some(Err(ArrowError::AvroError(
                            "Unexpected EOF while reading last Avro block".into(),
                        ))));
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
                            return Poll::Ready(Some(Err(ArrowError::AvroError(
                                "Unexpected EOF while reading Avro block header".into(),
                            ))));
                        }

                        let range_to_fetch = self.range.end..fetch_end;
                        self.range.end = fetch_end; // Track that we've fetched these bytes

                        let future = async move {
                            let data = reader.get_bytes(range_to_fetch).await?;
                            Ok((reader, data))
                        }
                        .boxed();
                        self.reader_state = ReaderState::FetchingData {
                            future,
                            next_behaviour: FetchNextBehaviour::DecodeVLQHeader,
                        };
                        continue;
                    }

                    // Otherwise, we're mid-block but know how many bytes are remaining to fetch.
                    let range_to_fetch = match self.remaining_block_range() {
                        Ok(range) => range,
                        Err(e) => return Poll::Ready(Some(Err(e))),
                    };

                    let future = async move {
                        let data = reader.get_bytes(range_to_fetch).await?;
                        Ok((reader, data))
                    }
                    .boxed();
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
                        self.decoder.decode_block(&block_data, remaining_in_block)?;

                    remaining_in_block -= records_decoded;

                    if remaining_in_block == 0 {
                        if data.is_empty() {
                            // No more data to read, we are finished
                            self.reader_state = ReaderState::Finished;
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
                        let batch_res = self.decoder.flush();
                        return Poll::Ready(batch_res.transpose());
                    }
                }
                // No more batches to emit
                ReaderState::Finished => {
                    // Flush any remaining records that haven't been emitted yet
                    let batch_res = self.decoder.flush();
                    self.reader_state = ReaderState::Finished;
                    return Poll::Ready(batch_res.transpose());
                }
            }
        }
    }
}

impl<R: AsyncFileReader + Unpin + 'static> Stream for AsyncAvroFileReader<R> {
    type Item = Result<RecordBatch, ArrowError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.read_next(cx)
    }
}

#[cfg(all(test, feature = "object_store"))]
mod tests {
    use super::*;
    use crate::schema::{AvroSchema, SCHEMA_METADATA_KEY};
    use arrow_array::cast::AsArray;
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
        schema: SchemaRef,
    ) -> Result<Vec<RecordBatch>, ArrowError> {
        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
        let location = Path::from_filesystem_path(path).unwrap();

        let file_size = store.head(&location).await.unwrap().size;

        let file_reader = AvroObjectReader::new(store, location);
        let reader_schema = AvroSchema::try_from(schema.as_ref())?;
        let builder = AsyncAvroFileReader::builder(file_reader, file_size, batch_size)
            .with_reader_schema(reader_schema);
        let reader = if let Some(range) = range {
            builder.with_range(range)
        } else {
            builder
        }
        .try_build()
        .await?;

        let batches: Vec<RecordBatch> = reader.try_collect().await?;
        Ok(batches)
    }

    // ============================================================================
    // CORE FUNCTIONALITY TESTS
    // ============================================================================

    #[tokio::test]
    async fn test_full_file_read() {
        let file = arrow_test_data("avro/alltypes_plain.avro");
        let schema = get_alltypes_schema();
        let batches = read_async_file(&file, 1024, None, schema).await.unwrap();
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
        let batches = read_async_file(&file, 2, None, schema).await.unwrap();
        assert_eq!(batches.len(), 4);

        let batch = &batches[0];

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 11);
    }

    #[tokio::test]
    async fn test_batch_size_one() {
        let file = arrow_test_data("avro/alltypes_plain.avro");
        let schema = get_alltypes_schema();
        let batches = read_async_file(&file, 1, None, schema).await.unwrap();
        let batch = &batches[0];

        assert_eq!(batches.len(), 8);
        assert_eq!(batch.num_rows(), 1);
    }

    #[tokio::test]
    async fn test_batch_size_larger_than_file() {
        let file = arrow_test_data("avro/alltypes_plain.avro");
        let schema = get_alltypes_schema();
        let batches = read_async_file(&file, 10000, None, schema).await.unwrap();
        let batch = &batches[0];

        assert_eq!(batch.num_rows(), 8);
    }

    // ============================================================================
    // RANGE HANDLING TESTS
    // ============================================================================

    #[tokio::test]
    async fn test_empty_range() {
        let file = arrow_test_data("avro/alltypes_plain.avro");
        let range = 100..100;
        let schema = get_alltypes_schema();
        let batches = read_async_file(&file, 1024, Some(range), schema)
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
        let batches = read_async_file(&file, 1024, Some(range), schema)
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
        let batches = read_async_file(&file, 1024, Some(range), schema)
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
        let batches = read_async_file(&file, 1024, Some(range), schema)
            .await
            .unwrap();
        assert_eq!(batches.len(), 0);
    }

    #[tokio::test]
    async fn test_range_starting_mid_file() {
        let file = arrow_test_data("avro/alltypes_plain.avro");

        let range = 700..768; // Header ends at 675, so this should be mid-block
        let schema = get_alltypes_schema();
        let batches = read_async_file(&file, 1024, Some(range), schema)
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
        let batches = read_async_file(&file, 1024, Some(range), schema)
            .await
            .unwrap();
        let batch = &batches[0];

        assert_eq!(batch.num_rows(), 8);
    }

    // ============================================================================
    // INCOMPLETE BLOCK HANDLING TESTS
    // ============================================================================

    #[tokio::test]
    async fn test_incomplete_block_requires_fetch() {
        // Range ends mid-block, should trigger fetching_rem_block logic
        let file = arrow_test_data("avro/alltypes_plain.avro");
        let range = 0..1200;
        let schema = get_alltypes_schema();
        let batches = read_async_file(&file, 1024, Some(range), schema)
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
        let batches = read_async_file(&file, 1024, Some(range), schema)
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
            let batches = read_async_file(&file, 1024, Some(range), schema)
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
        let batches = read_async_file(&file, 1024, None, schema).await.unwrap();
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
        let batches = read_async_file(&file, 1024, None, schema).await.unwrap();
        let batch = &batches[0];

        assert_eq!(batch.num_rows(), 2);
        assert!(batch.num_columns() > 0);
    }

    // ============================================================================
    // STREAM BEHAVIOR TESTS
    // ============================================================================

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

    // ============================================================================
    // EDGE CASE TESTS
    // ============================================================================

    #[tokio::test]
    async fn test_various_batch_sizes() {
        let file = arrow_test_data("avro/alltypes_plain.avro");

        for batch_size in [1, 2, 3, 5, 7, 11, 100] {
            let schema = get_alltypes_schema();
            let batches = read_async_file(&file, batch_size, None, schema)
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
        let batches = read_async_file(&file, 1024, Some(range), schema)
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
}
