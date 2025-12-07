use crate::compression::CompressionCodec;
use crate::reader::Decoder;
use crate::reader::block::BlockDecoder;
use crate::reader::vlq::VLQDecoder;
use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, SchemaRef};
use bytes::Bytes;
use futures::FutureExt;
use futures::Stream;
use futures::future::BoxFuture;
use std::mem;
use std::ops::Range;
use std::pin::Pin;
use std::task::{Context, Poll};

mod async_file_reader;
mod builder;

pub use async_file_reader::AsyncFileReader;
pub use builder::AsyncAvroReaderBuilder;

#[cfg(feature = "object_store")]
mod object_store_reader;

#[cfg(feature = "object_store")]
pub use object_store_reader::ObjectStoreFileReader;

type DataFetchFutureBoxed = BoxFuture<'static, Result<Bytes, ArrowError>>;

enum ReaderState {
    Idle,
    Limbo,
    FetchingData {
        existing_data: Option<Bytes>,
        fetch_future: DataFetchFutureBoxed,
    },
    DecodingBlock {
        data: Bytes,
    },
    ReadingBatches {
        data: Bytes,
        block_data: Bytes,
        remaining_in_block: usize,
    },
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
pub struct AsyncAvroReader<R: AsyncFileReader> {
    // Members required to fetch data
    reader: R,
    range: Range<u64>,
    file_size: u64,

    // Members required to actually decode and read data
    decoder: Decoder,
    block_decoder: BlockDecoder,
    codec: Option<CompressionCodec>,
    sync_marker: [u8; 16],

    // Members keeping the current state of the reader
    reader_state: ReaderState,
    finishing_partial_block: bool,
}

impl<R: AsyncFileReader> AsyncAvroReader<R> {
    /// Returns a builder for a new [`Self`], allowing some optional parameters.
    pub fn builder(
        reader: R,
        file_size: u64,
        schema: SchemaRef,
        batch_size: usize,
    ) -> AsyncAvroReaderBuilder<R> {
        AsyncAvroReaderBuilder {
            reader,
            file_size,
            schema,
            batch_size,
            range: None,
            reader_schema: None,
        }
    }

    /// Create a new asynchronous Avro reader for the given file location in the object store,
    /// reading the specified byte range (if any), with the provided reader schema and batch size.
    /// If no range is provided, the full file is read (file_size must be provided in this case).
    fn new(
        reader: R,
        range: Range<u64>,
        file_size: u64,
        decoder: Decoder,
        codec: Option<CompressionCodec>,
        sync_marker: [u8; 16],
        reader_state: ReaderState,
    ) -> Self {
        Self {
            reader,
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

    fn fetch_data_future(&mut self, range: Range<u64>) -> Result<DataFetchFutureBoxed, ArrowError> {
        if range.start >= range.end || range.end > self.file_size {
            return Err(ArrowError::AvroError(format!(
                "Invalid range specified for Avro file: start {} >= end {}, file_size: {}",
                range.start, range.end, self.file_size
            )));
        }

        Ok(self.reader.fetch_range(range))
    }

    fn read_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<RecordBatch, ArrowError>>> {
        loop {
            match mem::replace(&mut self.reader_state, ReaderState::Limbo) {
                ReaderState::Idle => {
                    let fetch_future = self.fetch_data_future(self.range.clone())?;
                    self.reader_state = ReaderState::FetchingData {
                        existing_data: None,
                        fetch_future,
                    };
                }
                ReaderState::Limbo => {
                    unreachable!("ReaderState::Limbo should never be observed");
                }
                ReaderState::FetchingData {
                    existing_data,
                    mut fetch_future,
                } => {
                    let data_chunk = match fetch_future.poll_unpin(cx)? {
                        Poll::Ready(data) => data,
                        Poll::Pending => {
                            // Return control to executor
                            self.reader_state = ReaderState::FetchingData {
                                existing_data,
                                fetch_future,
                            };
                            return Poll::Pending;
                        }
                    };

                    if let Some(current_data) = existing_data {
                        // If data already exists, it means we have a partial block,
                        // Append the newly fetched chunk to the existing buffered data.
                        let combined =
                            Bytes::from_owner([current_data.clone(), data_chunk].concat());
                        self.reader_state = ReaderState::DecodingBlock { data: combined };
                    } else {
                        let sync_marker_pos = data_chunk
                            .windows(16)
                            .position(|slice| slice == self.sync_marker);
                        let block_start = match sync_marker_pos {
                            Some(pos) => pos + 16, // Move past the sync marker
                            None => {
                                // Sync marker not found, this is actually valid if we arbitrarily split the file at its end.
                                self.reader_state = ReaderState::Finished;
                                return Poll::Ready(None);
                            }
                        };

                        // This is the first time we read data, so try and find the sync marker.
                        self.reader_state = ReaderState::DecodingBlock {
                            data: data_chunk.slice(block_start..),
                        };
                    }
                }
                ReaderState::DecodingBlock { mut data } => {
                    // Try to decode another block from the buffered reader.
                    let consumed = self.block_decoder.decode(&data)?;
                    if consumed == 0 {
                        // If the last block was exactly at the end of the file,
                        // we're simply done reading.
                        if data.is_empty() {
                            let final_batch = self.decoder.flush();
                            self.reader_state = ReaderState::Finished;
                            return Poll::Ready(final_batch.transpose());
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

                        let (size, vlq_header_len) = {
                            let mut vlq = VLQDecoder::default();
                            let mut vlq_buf = &data[..];
                            let original_len = vlq_buf.len();

                            let _ = vlq.long(&mut vlq_buf).ok_or_else(|| {
                                ArrowError::AvroError(
                                    "Unexpected EOF while reading Avro block count".into(),
                                )
                            })?;

                            let size = vlq.long(&mut vlq_buf).ok_or_else(|| {
                                ArrowError::AvroError(
                                    "Unexpected EOF while reading Avro block size".into(),
                                )
                            })? as u64;

                            // Calculate how many bytes were consumed by the two VLQ integers
                            let header_len = original_len.checked_sub(vlq_buf.len()).unwrap();

                            (size, header_len as u64)
                        };

                        // Two longs: count and size have already been read, but using our vlq,
                        // meaning they were not consumed.
                        let total_block_size = size + vlq_header_len;
                        let remaining_to_fetch =
                            total_block_size.checked_sub(data.len() as u64).unwrap();

                        let range_to_fetch = self.range.end..(self.range.end + remaining_to_fetch);
                        self.reader_state = ReaderState::FetchingData {
                            existing_data: Some(data),
                            fetch_future: self.fetch_data_future(range_to_fetch)?,
                        };
                        continue;
                    }

                    // Slice off the consumed data
                    data = data.slice(consumed..);

                    // Decompress the block if needed, prepare it for decoding.
                    if let Some(block) = self.block_decoder.flush() {
                        // Successfully decoded a block.
                        let block_data = Bytes::from_owner(if let Some(ref codec) = self.codec {
                            codec.decompress(&block.data)?
                        } else {
                            block.data
                        });

                        // Since we have an active block, move to reading batches
                        self.reader_state = ReaderState::ReadingBatches {
                            data,
                            block_data,
                            remaining_in_block: block.count,
                        };
                    } else {
                        // Block not finished yet, try to decode more in the next iteration
                        self.reader_state = ReaderState::DecodingBlock { data };
                    }
                }
                ReaderState::ReadingBatches {
                    data,
                    mut block_data,
                    mut remaining_in_block,
                } => {
                    let (consumed, records_decoded) =
                        self.decoder.decode_block(&block_data, remaining_in_block)?;

                    remaining_in_block -= records_decoded;

                    if remaining_in_block == 0 {
                        // Finished this block, move to decode next block in the next iteration
                        self.reader_state = ReaderState::DecodingBlock { data };
                    } else {
                        // Still more records to decode in this block, slice the already-read data and stay in this state
                        block_data = block_data.slice(consumed..);
                        self.reader_state = ReaderState::ReadingBatches {
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
                ReaderState::Finished => return Poll::Ready(None),
            }
        }
    }
}

impl<R: AsyncFileReader> Stream for AsyncAvroReader<R> {
    type Item = Result<RecordBatch, ArrowError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().read_next(cx)
    }
}

#[cfg(all(test, feature = "object_store"))]
mod tests {
    use super::*;
    use crate::schema::{AvroSchema, SCHEMA_METADATA_KEY};
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

        let file_reader = ObjectStoreFileReader::new(store, location);
        let reader_schema = AvroSchema::try_from(schema.as_ref())?;
        let builder = AsyncAvroReader::builder(file_reader, file_size, schema, batch_size)
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

        let file_reader = ObjectStoreFileReader::new(store, location);
        let schema = get_alltypes_schema();
        let reader_schema = AvroSchema::try_from(schema.as_ref()).unwrap();
        let reader = AsyncAvroReader::builder(
            file_reader,
            file_size,
            schema,
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

        let file_reader = ObjectStoreFileReader::new(store, location);
        let schema = get_alltypes_schema();
        let reader_schema = AvroSchema::try_from(schema.as_ref()).unwrap();
        let reader = AsyncAvroReader::builder(file_reader, file_size, schema, 1)
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
}
