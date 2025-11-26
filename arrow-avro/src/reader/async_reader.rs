use crate::codec::AvroFieldBuilder;
use crate::reader::Decoder;
use crate::reader::block::BlockDecoder;
use crate::reader::header::{Header, HeaderDecoder};
use crate::reader::record::RecordDecoder;
use crate::reader::vlq::VLQDecoder;
use crate::schema::{AvroSchema, FingerprintAlgorithm};
use arrow_array::RecordBatch;
use arrow_schema::ArrowError;
use bytes::Bytes;
use futures::FutureExt;
use futures::Stream;
use indexmap::IndexMap;
use object_store::path::Path;
use std::future::Future;
use std::mem;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

type DataFetchFutureBoxed = Pin<Box<dyn Future<Output = Result<Vec<Bytes>, ArrowError>> + Send>>;

/// Read the Avro file header (magic, metadata, sync marker) from `reader`.
async fn read_header(
    store: &Arc<dyn object_store::ObjectStore>,
    location: &Path,
    file_size: u64,
) -> Result<(Header, u64), ArrowError> {
    let mut decoder = HeaderDecoder::default();
    let mut position = 0;
    loop {
        let range_to_fetch = position..(position + 64 * 1024).min(file_size);
        let current_data = store
            .get_range(location, range_to_fetch)
            .await
            .map_err(|err| {
                ArrowError::AvroError(format!(
                    "Error fetching Avro header from object store: {err}"
                ))
            })?;
        if current_data.is_empty() {
            break;
        }
        let read = current_data.len();
        let decoded = decoder.decode(&current_data)?;
        if decoded != read {
            position += decoded as u64;
            break;
        }
        position += read as u64;
    }

    decoder
        .flush()
        .map(|header| (header, position))
        .ok_or_else(|| ArrowError::AvroError("Unexpected EOF while reading Avro header".into()))
}

enum ReaderState {
    Idle,
    Limbo,
    FetchingData(Pin<Box<dyn Future<Output = Result<Vec<Bytes>, ArrowError>> + Send>>),
    DecodingBlock,
    ReadingBatches,
    Finished,
}

/// An asynchronous Avro file reader that implements `Stream<Item = Result<RecordBatch, ArrowError>>`.
/// This uses ObjectStore to fetch data ranges as needed, starting with fetching the header,
/// then reading all the blocks in the provided range where:
/// 1. Searching from `range.start` for the first sync marker, and starting with the following block.
/// 2. Reading blocks sequentially, decoding them into RecordBatches.
/// 3. If a block is incomplete (due to range ending mid-block), fetching the remaining bytes from ObjectStore.
/// 4. If no range was originally provided, reads the full file.
pub struct AsyncAvroReader {
    store: Arc<dyn object_store::ObjectStore>,
    location: Path,
    range: Range<u64>,
    file_data: Bytes,
    file_size: u64,

    decoder: Decoder,
    header: Header,
    reader_state: ReaderState,
    fetching_rem_block: bool,

    block_count: usize,
    block_cursor: usize,
    block_data: Vec<u8>,
    block_decoder: BlockDecoder,
}

impl AsyncAvroReader {
    /// Create a new asynchronous Avro reader for the given file location in the object store,
    /// reading the specified byte range (if any), with the provided reader schema and batch size.
    /// If no range is provided, the full file is read (file_size must be provided in this case).
    pub async fn try_new(
        store: Arc<dyn object_store::ObjectStore>,
        location: Path,
        range: Option<Range<u64>>,
        file_size: Option<u64>,
        reader_schema: Option<AvroSchema>,
        batch_size: usize,
    ) -> Result<Self, ArrowError> {
        let file_size = match file_size {
            None | Some(0) => {
                store
                    .head(&location)
                    .await
                    .map_err(|err| {
                        ArrowError::AvroError(format!("HEAD request failed for file, {err}"))
                    })?
                    .size
            }
            Some(size) => size,
        };

        // Start by reading the header from the beginning of the avro file
        let (header, header_len) = read_header(&store, &location, file_size).await?;
        let writer_schema = header
            .schema()
            .map_err(|e| ArrowError::ExternalError(Box::new(e)))?
            .ok_or_else(|| {
                ArrowError::ParseError("No Avro schema present in file header".into())
            })?;

        let root = {
            let mut field_builder = AvroFieldBuilder::new(&writer_schema);
            let reader_schema = reader_schema
                .as_ref()
                .map(|reader_schema| reader_schema.schema())
                .transpose()?;
            if let Some(reader_schema) = reader_schema.as_ref() {
                field_builder = field_builder.with_reader_schema(reader_schema)
            }
            field_builder.build()
        }?;

        let record_decoder = RecordDecoder::try_new_with_options(root.data_type())?;

        let decoder = Decoder::from_parts(
            batch_size,
            record_decoder,
            None,
            IndexMap::new(),
            FingerprintAlgorithm::Rabin,
        );
        let range = match range {
            Some(r) => {
                // If this PartitionedFile's range starts at 0, we need to skip the header bytes.
                // But then we need to seek back 16 bytes to include the sync marker for the first block,
                // as the logic in this reader searches the data for the first sync marker(after which a block starts),
                // then reads blocks from the count, size etc.
                let start = r.start.max(header_len.checked_sub(16).unwrap());
                let end = r.end.max(start).min(file_size); // Ensure end is not less than start, worst case range is empty
                start..end
            }
            None => 0..file_size,
        };

        let reader_state = if range.start == range.end {
            ReaderState::Finished
        } else {
            ReaderState::Idle
        };
        Ok(Self {
            store,
            location,
            range,
            file_data: Bytes::default(),
            file_size,

            header,
            decoder,
            reader_state,
            fetching_rem_block: false,

            block_count: 0,
            block_cursor: 0,
            block_data: Vec::new(),
            block_decoder: Default::default(),
        })
    }

    fn fetch_data_future(&self, range: Range<u64>) -> Result<DataFetchFutureBoxed, ArrowError> {
        if range.start >= range.end || range.end > self.file_size {
            return Err(ArrowError::AvroError(format!(
                "Invalid range specified for Avro file {}: start {} >= end {}, file_size: {}",
                self.location, range.start, range.end, self.file_size
            )));
        }

        let location = self.location.clone();
        let store = Arc::clone(&self.store);
        Ok(Box::pin(async move {
            store.get_ranges(&location, &[range]).await.map_err(|err| {
                ArrowError::AvroError(format!("Error fetching Avro data from object store: {err}"))
            })
        }))
    }

    fn read_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<RecordBatch, ArrowError>>> {
        loop {
            match mem::replace(&mut self.reader_state, ReaderState::Limbo) {
                ReaderState::Idle => {
                    self.reader_state =
                        ReaderState::FetchingData(self.fetch_data_future(self.range.clone())?);
                }
                ReaderState::Limbo => {
                    unreachable!("ReaderState::Limbo should never be observed");
                }
                ReaderState::FetchingData(mut future) => {
                    // Done this way(passing the future in the enum etc.) so in the future this can be replaced with proper polling.
                    let data_chunks = match future.poll_unpin(cx)? {
                        Poll::Ready(data) => data,
                        Poll::Pending => {
                            self.reader_state = ReaderState::FetchingData(future);
                            return Poll::Pending;
                        }
                    };

                    // We only requested one range, so we expect one chunk.
                    let chunk = data_chunks.into_iter().next().unwrap();
                    // This is the first time we read data, so try and find the sync marker.
                    if self.file_data.is_empty() {
                        self.file_data = chunk;
                        self.reader_state = ReaderState::DecodingBlock;

                        let sync_marker_pos = self
                            .file_data
                            .windows(16)
                            .position(|slice| slice == self.header.sync());
                        let block_start = match sync_marker_pos {
                            Some(pos) => pos + 16, // Move past the sync marker
                            None => {
                                // Sync marker not found, this is actually valid if Spark arbitrarily split the file at its end.s
                                self.reader_state = ReaderState::Finished;
                                return Poll::Ready(None);
                            }
                        };

                        self.file_data = self.file_data.slice(block_start..);
                    } else {
                        // If data already exists, it means we have a partial block,
                        // Append the newly fetched chunk to the existing buffered data.
                        let combined = [self.file_data.clone(), chunk].concat();
                        self.file_data = Bytes::from(combined);
                        self.reader_state = ReaderState::DecodingBlock;
                    }
                }
                ReaderState::DecodingBlock => {
                    // Try to decode another block from the buffered reader.
                    let consumed = self.block_decoder.decode(&self.file_data)?;
                    if consumed == 0 {
                        // If the last block was exactly at the end of the file,
                        // we're simply done reading.
                        if self.file_data.is_empty() {
                            let final_batch = self.decoder.flush();
                            self.reader_state = ReaderState::Finished;
                            return Poll::Ready(final_batch.transpose());
                        }

                        // If we've tried the following stage before, and still can't decode,
                        // this means the file is truncated or corrupted.
                        if self.fetching_rem_block {
                            return Poll::Ready(Some(Err(ArrowError::AvroError(
                                "Unexpected EOF while reading last Avro block".into(),
                            ))));
                        }

                        // Avro splitting case: block is incomplete, we need to:
                        // 1. Parse the length so we know how much to read
                        // 2. Fetch more data from the object store
                        // 3. Create a new block data from the remaining slice and the newly fetched data
                        // 4. Continue decoding until end of block
                        self.fetching_rem_block = true;

                        let (size, vlq_header_len) = {
                            let mut vlq = VLQDecoder::default();
                            let mut vlq_buf = &self.file_data[..];
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
                        let remaining_to_fetch = total_block_size
                            .checked_sub(self.file_data.len() as u64)
                            .unwrap();

                        let range_to_fetch = self.range.end..(self.range.end + remaining_to_fetch);
                        self.reader_state =
                            ReaderState::FetchingData(self.fetch_data_future(range_to_fetch)?);
                        continue;
                    }

                    // Slice off the consumed data
                    self.file_data = self.file_data.slice(consumed..);

                    // Decompress the block if needed, prepare it for decoding.
                    if let Some(block) = self.block_decoder.flush() {
                        // Successfully decoded a block.
                        self.block_data = if let Some(ref codec) = self.header.compression()? {
                            codec.decompress(&block.data)?
                        } else {
                            block.data
                        };
                        self.block_count = block.count;
                        self.block_cursor = 0;

                        // Since we have an active block, move to reading batches
                        self.reader_state = ReaderState::ReadingBatches;
                    } else {
                        // Block not finished yet, try to decode more in the next iteration
                        self.reader_state = ReaderState::DecodingBlock;
                    }
                }
                ReaderState::ReadingBatches => {
                    let (consumed, records_decoded) = self
                        .decoder
                        .decode_block(&self.block_data[self.block_cursor..], self.block_count)?;

                    self.block_cursor += consumed;
                    self.block_count -= records_decoded;

                    if self.block_count == 0 {
                        // Finished this block, move to decode next block in the next iteration
                        self.reader_state = ReaderState::DecodingBlock;
                    } else {
                        // Still more records to decode in this block, stay in this state
                        self.reader_state = ReaderState::ReadingBatches;
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

impl Stream for AsyncAvroReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.read_next(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reader::async_reader::AsyncAvroReader;
    use crate::schema::{AvroSchema, SCHEMA_METADATA_KEY};
    use arrow_array::*;
    use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
    use futures::{StreamExt, TryStreamExt};
    use object_store::{ObjectStore, local::LocalFileSystem, path::Path as ObjectPath};
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
    ) -> Result<Vec<RecordBatch>, ArrowError> {
        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
        let location = ObjectPath::from_filesystem_path(path).unwrap();

        let reader_schema = schema.map(|schema| AvroSchema::try_from(schema.as_ref()).unwrap());
        let reader =
            AsyncAvroReader::try_new(store, location, range, None, reader_schema, batch_size)
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
        let batches = read_async_file(&file, 1024, None, Some(schema))
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
        let batches = read_async_file(&file, 2, None, Some(schema)).await.unwrap();
        assert_eq!(batches.len(), 4);

        let batch = &batches[0];

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 11);
    }

    #[tokio::test]
    async fn test_batch_size_one() {
        let file = arrow_test_data("avro/alltypes_plain.avro");
        let schema = get_alltypes_schema();
        let batches = read_async_file(&file, 1, None, Some(schema)).await.unwrap();
        let batch = &batches[0];

        assert_eq!(batches.len(), 8);
        assert_eq!(batch.num_rows(), 1);
    }

    #[tokio::test]
    async fn test_batch_size_larger_than_file() {
        let file = arrow_test_data("avro/alltypes_plain.avro");
        let schema = get_alltypes_schema();
        let batches = read_async_file(&file, 10000, None, Some(schema))
            .await
            .unwrap();
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
        let batches = read_async_file(&file, 1024, Some(range), Some(schema))
            .await
            .unwrap();
        assert_eq!(batches.len(), 0);
    }

    #[tokio::test]
    async fn test_range_starting_at_zero() {
        // Tests that range starting at 0 correctly skips header
        let file = arrow_test_data("avro/alltypes_plain.avro");
        let store = Arc::new(LocalFileSystem::new());
        let location = ObjectPath::from_filesystem_path(&file).unwrap();
        let meta = store.head(&location).await.unwrap();

        let range = 0..meta.size;
        let schema = get_alltypes_schema();
        let batches = read_async_file(&file, 1024, Some(range), Some(schema))
            .await
            .unwrap();
        let batch = &batches[0];

        assert_eq!(batch.num_rows(), 8);
    }

    #[tokio::test]
    async fn test_range_after_header() {
        let file = arrow_test_data("avro/alltypes_plain.avro");
        let store = Arc::new(LocalFileSystem::new());
        let location = ObjectPath::from_filesystem_path(&file).unwrap();
        let meta = store.head(&location).await.unwrap();

        let range = 100..meta.size;
        let schema = get_alltypes_schema();
        let batches = read_async_file(&file, 1024, Some(range), Some(schema))
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
        let batches = read_async_file(&file, 1024, Some(range), Some(schema))
            .await
            .unwrap();
        assert_eq!(batches.len(), 0);
    }

    #[tokio::test]
    async fn test_range_starting_mid_file() {
        let file = arrow_test_data("avro/alltypes_plain.avro");

        let range = 700..768; // Header ends at 675, so this should be mid-block
        let schema = get_alltypes_schema();
        let batches = read_async_file(&file, 1024, Some(range), Some(schema))
            .await
            .unwrap();
        assert_eq!(batches.len(), 0);
    }

    #[tokio::test]
    async fn test_range_ending_at_file_size() {
        let file = arrow_test_data("avro/alltypes_plain.avro");
        let store = Arc::new(LocalFileSystem::new());
        let location = ObjectPath::from_filesystem_path(&file).unwrap();
        let meta = store.head(&location).await.unwrap();

        let range = 200..meta.size;
        let schema = get_alltypes_schema();
        let batches = read_async_file(&file, 1024, Some(range), Some(schema))
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
        let batches = read_async_file(&file, 1024, Some(range), Some(schema))
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
            let location = ObjectPath::from_filesystem_path(&file).unwrap();
            let meta = store.head(&location).await.unwrap();

            let range = 200..meta.size;
            let schema = get_alltypes_schema();
            let batches = read_async_file(&file, 1024, Some(range), Some(schema))
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
        let batches = read_async_file(&file, 1024, None, Some(schema))
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
        let batches = read_async_file(&file, 1024, None, Some(schema))
            .await
            .unwrap();
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
        let location = ObjectPath::from_filesystem_path(&file).unwrap();

        let schema = get_alltypes_schema();
        let reader_schema = AvroSchema::try_from(schema.as_ref()).unwrap();
        let reader = AsyncAvroReader::try_new(
            store,
            location,
            None,
            None,
            Some(reader_schema),
            2, // Small batch size to force multiple batches
        )
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
        let location = ObjectPath::from_filesystem_path(&file).unwrap();

        let schema = get_alltypes_schema();
        let reader_schema = AvroSchema::try_from(schema.as_ref()).unwrap();
        let reader = AsyncAvroReader::try_new(store, location, None, None, Some(reader_schema), 1)
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
            let batches = read_async_file(&file, batch_size, None, Some(schema))
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
        let location = ObjectPath::from_filesystem_path(&file).unwrap();
        let meta = store.head(&location).await.unwrap();

        // Range extends beyond file size
        let range = 100..(meta.size + 1000);
        let schema = get_alltypes_schema();
        let batches = read_async_file(&file, 1024, Some(range), Some(schema))
            .await
            .unwrap();
        let batch = &batches[0];

        // Should clamp to file size
        assert_eq!(batch.num_rows(), 8);
    }
}
