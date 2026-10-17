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

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_buffer::{Buffer, MutableBuffer};
use arrow_data::UnsafeFlag;
use arrow_schema::{ArrowError, SchemaRef};

use crate::convert::MessageBuffer;
use crate::reader::{RecordBatchDecoder, read_dictionary_impl};
use crate::{CONTINUATION_MARKER, MessageHeader};

/// A low-level interface for reading [`RecordBatch`] data from a stream of bytes
///
/// See [StreamReader](crate::reader::StreamReader) for a higher-level interface
#[derive(Debug, Default)]
pub struct StreamDecoder {
    /// The schema of this decoder, if read
    schema: Option<SchemaRef>,
    /// Lookup table for dictionaries by ID
    dictionaries: HashMap<i64, ArrayRef>,
    /// The decoder state
    state: DecoderState,
    /// A scratch buffer when a read is split across multiple `Buffer`
    buf: MutableBuffer,
    /// Whether or not array data in input buffers are required to be aligned
    require_alignment: bool,
    /// Should validation be skipped when reading data? Defaults to false.
    ///
    /// See [`StreamDecoder::with_skip_validation`] for details.
    ///
    skip_validation: UnsafeFlag,
}

#[derive(Debug)]
enum DecoderState {
    /// Decoding the message header
    Header {
        /// Temporary buffer
        buf: [u8; 4],
        /// Number of bytes read into buf
        read: u8,
        /// If we have read a continuation token
        continuation: bool,
    },
    /// Decoding the message flatbuffer
    Message {
        /// The size of the message flatbuffer
        size: u32,
    },
    /// Decoding the message body
    Body {
        /// The message flatbuffer
        message: MessageBuffer,
    },
    /// Reached the end of the stream
    Finished,
}

impl Default for DecoderState {
    fn default() -> Self {
        Self::Header {
            buf: [0; 4],
            read: 0,
            continuation: false,
        }
    }
}

impl StreamDecoder {
    /// Create a new [`StreamDecoder`]
    pub fn new() -> Self {
        Self::default()
    }

    /// Specifies whether or not array data in input buffers is required to be properly aligned.
    ///
    /// If `require_alignment` is true, this decoder will return an error if any array data in the
    /// input `buf` is not properly aligned.
    /// Under the hood it will use [`arrow_data::ArrayDataBuilder::build`] to construct
    /// [`arrow_data::ArrayData`].
    ///
    /// If `require_alignment` is false (the default), this decoder will automatically allocate a
    /// new aligned buffer and copy over the data if any array data in the input `buf` is not
    /// properly aligned. (Properly aligned array data will remain zero-copy.)
    /// Under the hood it will use [`arrow_data::ArrayDataBuilder::align_buffers`] to construct
    /// [`arrow_data::ArrayData`].
    pub fn with_require_alignment(mut self, require_alignment: bool) -> Self {
        self.require_alignment = require_alignment;
        self
    }

    /// Return the schema if decoded, else None.
    pub fn schema(&self) -> Option<SchemaRef> {
        self.schema.as_ref().map(|schema| schema.clone())
    }

    /// Specifies if validation should be skipped when reading data (defaults to `false`)
    ///
    /// # Safety
    ///
    /// This flag must only be set to `true` when you trust the input data and are
    /// sure the data you are reading is valid Arrow IPC stream data, otherwise
    /// undefined behavior may result.
    ///
    /// For example, DataFusion uses this when reading spill files it wrote itself.
    pub unsafe fn with_skip_validation(mut self, skip_validation: bool) -> Self {
        unsafe { self.skip_validation.set(skip_validation) };
        self
    }

    /// Try to read the next [`RecordBatch`] from the provided [`Buffer`]
    ///
    /// [`Buffer::advance`] will be called on `buffer` for any consumed bytes.
    ///
    /// The push-based interface facilitates integration with sources that yield arbitrarily
    /// delimited bytes ranges, such as a chunked byte stream received from object storage
    ///
    /// ```
    /// # use arrow_array::RecordBatch;
    /// # use arrow_buffer::Buffer;
    /// # use arrow_ipc::reader::StreamDecoder;
    /// # use arrow_schema::ArrowError;
    /// #
    /// fn print_stream<I>(src: impl Iterator<Item = Buffer>) -> Result<(), ArrowError> {
    ///     let mut decoder = StreamDecoder::new();
    ///     for mut x in src {
    ///         while !x.is_empty() {
    ///             if let Some(x) = decoder.decode(&mut x)? {
    ///                 println!("{x:?}");
    ///             }
    ///             if let Some(schema) = decoder.schema() {
    ///                 println!("Schema: {schema:?}");
    ///             }
    ///         }
    ///     }
    ///     decoder.finish().unwrap();
    ///     Ok(())
    /// }
    /// ```
    pub fn decode(&mut self, buffer: &mut Buffer) -> Result<Option<RecordBatch>, ArrowError> {
        while !buffer.is_empty() {
            match &mut self.state {
                DecoderState::Header {
                    buf,
                    read,
                    continuation,
                } => {
                    let offset_buf = &mut buf[*read as usize..];
                    let to_read = buffer.len().min(offset_buf.len());
                    offset_buf[..to_read].copy_from_slice(&buffer[..to_read]);
                    *read += to_read as u8;
                    buffer.advance(to_read);
                    if *read == 4 {
                        if !*continuation && buf == &CONTINUATION_MARKER {
                            *continuation = true;
                            *read = 0;
                            continue;
                        }
                        let size = u32::from_le_bytes(*buf);

                        if size == 0 {
                            self.state = DecoderState::Finished;
                            continue;
                        }
                        self.state = DecoderState::Message { size };
                    }
                }
                DecoderState::Message { size } => {
                    let len = *size as usize;
                    if self.buf.is_empty() && buffer.len() > len {
                        let message = MessageBuffer::try_new(buffer.slice_with_length(0, len))?;
                        self.state = DecoderState::Body { message };
                        buffer.advance(len);
                        continue;
                    }

                    let to_read = buffer.len().min(len - self.buf.len());
                    self.buf
                        .try_extend_from_slice(&buffer[..to_read])
                        .map_err(|e| ArrowError::MemoryError(e.to_string()))?;
                    buffer.advance(to_read);
                    if self.buf.len() == len {
                        let message = MessageBuffer::try_new(std::mem::take(&mut self.buf).into())?;
                        self.state = DecoderState::Body { message };
                    }
                }
                DecoderState::Body { message } => {
                    let message = message.as_ref();
                    let body_length = message.bodyLength() as usize;

                    let body = if self.buf.is_empty() && buffer.len() >= body_length {
                        let body = buffer.slice_with_length(0, body_length);
                        buffer.advance(body_length);
                        body
                    } else {
                        let to_read = buffer.len().min(body_length - self.buf.len());
                        self.buf
                            .try_extend_from_slice(&buffer[..to_read])
                            .map_err(|e| ArrowError::MemoryError(e.to_string()))?;
                        buffer.advance(to_read);

                        if self.buf.len() != body_length {
                            continue;
                        }
                        std::mem::take(&mut self.buf).into()
                    };

                    let version = message.version();
                    match message.header_type() {
                        MessageHeader::Schema => {
                            if self.schema.is_some() {
                                return Err(ArrowError::IpcError(
                                    "Not expecting a schema when messages are read".to_string(),
                                ));
                            }

                            let ipc_schema = message.header_as_schema().ok_or_else(|| {
                                ArrowError::ParseError(
                                    "Unable to read IPC message as schema".to_string(),
                                )
                            })?;
                            let schema = crate::convert::try_fb_to_schema(ipc_schema)?;
                            self.state = DecoderState::default();
                            self.schema = Some(Arc::new(schema));
                        }
                        MessageHeader::RecordBatch => {
                            let batch = message.header_as_record_batch().unwrap();
                            let schema = self.schema.clone().ok_or_else(|| {
                                ArrowError::IpcError("Missing schema".to_string())
                            })?;
                            let batch = RecordBatchDecoder::try_new(
                                &body,
                                batch,
                                schema,
                                &self.dictionaries,
                                &version,
                            )?
                            .with_require_alignment(self.require_alignment)
                            .read_record_batch()?;
                            self.state = DecoderState::default();
                            return Ok(Some(batch));
                        }
                        MessageHeader::DictionaryBatch => {
                            let dictionary = message.header_as_dictionary_batch().unwrap();
                            let schema = self.schema.as_deref().ok_or_else(|| {
                                ArrowError::IpcError("Missing schema".to_string())
                            })?;
                            read_dictionary_impl(
                                &body,
                                dictionary,
                                schema,
                                &mut self.dictionaries,
                                &version,
                                self.require_alignment,
                                self.skip_validation.clone(),
                            )?;
                            self.state = DecoderState::default();
                        }
                        MessageHeader::NONE => {
                            self.state = DecoderState::default();
                        }
                        t => {
                            return Err(ArrowError::IpcError(format!(
                                "Message type unsupported by StreamDecoder: {t:?}"
                            )));
                        }
                    }
                }
                DecoderState::Finished => {
                    return Err(ArrowError::IpcError("Unexpected EOS".to_string()));
                }
            }
        }
        Ok(None)
    }

    /// Signal the end of stream
    ///
    /// Returns an error if any partial data remains in the stream
    pub fn finish(&mut self) -> Result<(), ArrowError> {
        match self.state {
            DecoderState::Finished
            | DecoderState::Header {
                read: 0,
                continuation: false,
                ..
            } => Ok(()),
            _ => Err(ArrowError::IpcError("Unexpected End of Stream".to_string())),
        }
    }

    /// Returns `true` once the stream's EOS marker has been consumed.
    pub fn is_finished(&self) -> bool {
        matches!(self.state, DecoderState::Finished)
    }
}

/// Pull-based, zero-copy IPC stream reader over an owned in-memory [`Buffer`].
///
/// Where [`StreamReader`](crate::reader::StreamReader) reads from an arbitrary
/// `R: Read` and copies each message body into a fresh allocation, this reader
/// takes a fully-materialized [`Buffer`] and yields [`RecordBatch`]es whose
/// column [`Buffer`]s alias the input — no per-message memcpy. Use this when
/// the entire IPC stream is already in memory (HTTP body, mmap, byte slice,
/// shared-memory segment, `bytes::Bytes` from object storage, …).
///
/// For a streaming push-based interface that handles partial buffers (e.g. a
/// chunked byte stream from object storage), use [`StreamDecoder`] directly.
///
/// # Example
///
/// ```
/// # use std::sync::Arc;
/// # use arrow_array::{Int32Array, RecordBatch};
/// # use arrow_buffer::Buffer;
/// # use arrow_ipc::reader::BufferStreamReader;
/// # use arrow_ipc::writer::StreamWriter;
/// # use arrow_schema::{DataType, Field, Schema};
/// let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
/// let batch = RecordBatch::try_new(
///     schema.clone(),
///     vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
/// ).unwrap();
///
/// let mut bytes = Vec::new();
/// let mut w = StreamWriter::try_new(&mut bytes, schema.as_ref()).unwrap();
/// w.write(&batch).unwrap();
/// w.finish().unwrap();
/// drop(w);
///
/// let mut reader = BufferStreamReader::try_new(Buffer::from(bytes)).unwrap();
/// assert_eq!(reader.schema().as_ref(), schema.as_ref());
/// let read = reader.next().unwrap().unwrap();
/// assert_eq!(read, batch);
/// assert!(reader.next().is_none());
/// ```
#[derive(Debug)]
pub struct BufferStreamReader {
    decoder: StreamDecoder,
    buffer: Buffer,
    schema: SchemaRef,
    /// `StreamDecoder::decode` may parse the schema and the first
    /// record batch in a single call (its inner loop only exits on a
    /// `RecordBatch` header or empty input). When that happens we stash
    /// the eager batch here so it's returned by the first `next()`.
    pending: Option<RecordBatch>,
    /// Set once the iterator has returned its final item.
    finished: bool,
}

impl BufferStreamReader {
    /// Create a new reader, eagerly draining the schema message (and any
    /// leading dictionary messages) so [`schema`](Self::schema) is cheap
    /// and infallible afterwards.
    ///
    /// # Errors
    ///
    /// Returns an error if `buffer` does not start with a valid IPC stream
    /// schema message.
    pub fn try_new(buffer: Buffer) -> Result<Self, ArrowError> {
        let mut decoder = StreamDecoder::new();
        let mut working = buffer;
        let mut pending: Option<RecordBatch> = None;
        // Drive the decoder until the schema is parsed. `StreamDecoder`
        // may consume the schema *and* the first record batch in a
        // single `decode()` call (its inner loop only exits on a
        // RecordBatch or on empty input); stash that eager batch.
        while decoder.schema().is_none() {
            if working.is_empty() {
                return Err(ArrowError::IpcError(
                    "Expected schema message, found empty stream.".to_string(),
                ));
            }
            if let Some(batch) = decoder.decode(&mut working)? {
                if decoder.schema().is_none() {
                    return Err(ArrowError::IpcError(
                        "Expected schema as first IPC message, got record batch".to_string(),
                    ));
                }
                pending = Some(batch);
                break;
            }
        }
        let schema = decoder.schema().expect("schema decoded above");
        Ok(Self {
            decoder,
            buffer: working,
            schema,
            pending,
            finished: false,
        })
    }

    /// Create a new reader from a [`bytes::Bytes`] (cheap zero-copy
    /// conversion via the existing `impl From<bytes::Bytes> for Buffer`).
    pub fn try_new_from_bytes(bytes: bytes::Bytes) -> Result<Self, ArrowError> {
        Self::try_new(Buffer::from(bytes))
    }

    /// The schema of the stream.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Returns `true` if the stream's EOS marker has been consumed and no
    /// further batches will be produced.
    pub fn is_finished(&self) -> bool {
        self.finished || self.decoder.is_finished()
    }
}

impl Iterator for BufferStreamReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(batch) = self.pending.take() {
            return Some(Ok(batch));
        }
        loop {
            if self.finished || self.decoder.is_finished() {
                return None;
            }
            if self.buffer.is_empty() {
                self.finished = true;
                // Like `StreamReader`, a stream that ends on a message boundary
                // without an EOS marker is accepted, but a truncated message is an error
                return self.decoder.finish().err().map(Err);
            }
            match self.decoder.decode(&mut self.buffer) {
                Ok(Some(batch)) => return Some(Ok(batch)),
                Ok(None) => {}
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::writer::{IpcWriteOptions, StreamWriter};
    use arrow_array::{
        DictionaryArray, Int32Array, Int64Array, RecordBatch, RunArray, types::Int32Type,
    };
    use arrow_schema::{DataType, Field, Schema};

    // Further tests in arrow-integration-testing/tests/ipc_reader.rs

    #[test]
    fn test_eos() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("int32", DataType::Int32, false),
            Field::new("int64", DataType::Int64, false),
        ]));

        let input = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])) as _,
                Arc::new(Int64Array::from(vec![1, 2, 3])) as _,
            ],
        )
        .unwrap();

        let mut buf = Vec::with_capacity(1024);
        let mut s = StreamWriter::try_new(&mut buf, &schema).unwrap();
        s.write(&input).unwrap();
        s.finish().unwrap();
        drop(s);

        let buffer = Buffer::from_vec(buf);

        let mut b = buffer.slice_with_length(0, buffer.len() - 1);
        let mut decoder = StreamDecoder::new();
        let output = decoder.decode(&mut b).unwrap().unwrap();
        assert_eq!(output, input);
        assert_eq!(b.len(), 7); // 8 byte EOS truncated by 1 byte
        assert!(decoder.decode(&mut b).unwrap().is_none());

        let err = decoder.finish().unwrap_err().to_string();
        assert_eq!(err, "Ipc error: Unexpected End of Stream");
    }

    #[test]
    fn test_schema() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("int32", DataType::Int32, false),
            Field::new("int64", DataType::Int64, false),
        ]));

        let mut buf = Vec::with_capacity(1024);
        let mut s = StreamWriter::try_new(&mut buf, &schema).unwrap();
        s.finish().unwrap();
        drop(s);

        let buffer = Buffer::from_vec(buf);

        let mut b = buffer.slice_with_length(0, buffer.len() - 1);
        let mut decoder = StreamDecoder::new();
        let output = decoder.decode(&mut b).unwrap();
        assert!(output.is_none());
        let decoded_schema = decoder.schema().unwrap();
        assert_eq!(schema, decoded_schema);

        let err = decoder.finish().unwrap_err().to_string();
        assert_eq!(err, "Ipc error: Unexpected End of Stream");
    }

    #[test]
    fn test_read_ree_dict_record_batches_from_buffer() {
        let schema = Schema::new(vec![Field::new(
            "test1",
            DataType::RunEndEncoded(
                Arc::new(Field::new(
                    Field::REE_RUN_ENDS_FIELD_DEFAULT_NAME,
                    DataType::Int32,
                    false,
                )),
                #[expect(deprecated)]
                Arc::new(Field::new_dict(
                    Field::REE_VALUES_FIELD_DEFAULT_NAME,
                    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                    true,
                    0,
                    false,
                )),
            ),
            true,
        )]);
        let batch = RecordBatch::try_new(
            schema.clone().into(),
            vec![Arc::new(
                RunArray::try_new(
                    &Int32Array::from(vec![1, 2, 3]),
                    &vec![Some("a"), None, Some("a")]
                        .into_iter()
                        .collect::<DictionaryArray<Int32Type>>(),
                )
                .expect("Failed to create RunArray"),
            )],
        )
        .expect("Failed to create RecordBatch");

        let mut buffer = vec![];
        {
            let mut writer = StreamWriter::try_new_with_options(
                &mut buffer,
                &schema,
                IpcWriteOptions::default(),
            )
            .expect("Failed to create StreamWriter");
            writer.write(&batch).expect("Failed to write RecordBatch");
            writer.finish().expect("Failed to finish StreamWriter");
        }

        let mut decoder = StreamDecoder::new();
        let buf = &mut Buffer::from(buffer.as_slice());
        while let Some(batch) = decoder
            .decode(buf)
            .map_err(|e| {
                ArrowError::ExternalError(format!("Failed to decode record batch: {e}").into())
            })
            .expect("Failed to decode record batch")
        {
            assert_eq!(batch, batch);
        }

        decoder.finish().expect("Failed to finish decoder");
    }

    // ---------------------------------------------------------------
    // BufferStreamReader
    // ---------------------------------------------------------------

    fn make_int_batch(rows: usize) -> (RecordBatch, Arc<Schema>) {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(
                (0..rows as i32).collect::<Vec<_>>(),
            ))],
        )
        .unwrap();
        (batch, schema)
    }

    fn serialize_stream(batches: &[RecordBatch], schema: &Schema) -> Vec<u8> {
        let mut buf = Vec::new();
        {
            let mut w = StreamWriter::try_new(&mut buf, schema).unwrap();
            for b in batches {
                w.write(b).unwrap();
            }
            w.finish().unwrap();
        }
        buf
    }

    #[test]
    fn buffer_stream_reader_roundtrip_single_batch() {
        let (batch, schema) = make_int_batch(64);
        let bytes = serialize_stream(std::slice::from_ref(&batch), schema.as_ref());
        let mut r = BufferStreamReader::try_new(Buffer::from_vec(bytes)).unwrap();
        assert_eq!(r.schema().as_ref(), schema.as_ref());
        let got = r.next().unwrap().unwrap();
        assert_eq!(got, batch);
        assert!(r.next().is_none());
        assert!(r.is_finished());
    }

    #[test]
    fn buffer_stream_reader_multiple_batches() {
        let (a, schema) = make_int_batch(8);
        let (b, _) = make_int_batch(16);
        let (c, _) = make_int_batch(32);
        let bytes = serialize_stream(&[a.clone(), b.clone(), c.clone()], schema.as_ref());
        let r = BufferStreamReader::try_new(Buffer::from_vec(bytes)).unwrap();
        let collected: Vec<_> = r.map(Result::unwrap).collect();
        assert_eq!(collected, vec![a, b, c]);
    }

    #[test]
    fn buffer_stream_reader_aliases_input() {
        // The IPC reader builds each column buffer by slicing one shared
        // owned-bytes allocation, so the resulting batch must NOT have
        // allocated fresh memory for column data — it should alias the
        // input buffer's underlying storage.
        let (batch, schema) = make_int_batch(1024);
        let bytes = serialize_stream(std::slice::from_ref(&batch), schema.as_ref());
        let owned = Buffer::from_vec(bytes);
        let owned_ptr = owned.as_ptr();
        let owned_len = owned.len();
        let mut r = BufferStreamReader::try_new(owned).unwrap();
        let got = r.next().unwrap().unwrap();
        // The Int32 values column buffer should point inside the input
        // allocation: same base ptr range, no new heap region.
        let col_buf = got.column(0).to_data().buffers()[0].clone();
        let col_ptr = col_buf.as_ptr();
        let inside = (col_ptr as usize) >= (owned_ptr as usize)
            && (col_ptr as usize) < (owned_ptr as usize) + owned_len;
        assert!(
            inside,
            "column buffer ptr {col_ptr:?} not inside input allocation \
             {owned_ptr:?}..{:?}",
            unsafe { owned_ptr.add(owned_len) }
        );
    }

    #[test]
    fn buffer_stream_reader_empty_buffer_errors() {
        let err = BufferStreamReader::try_new(Buffer::from_vec::<u8>(vec![])).unwrap_err();
        assert!(matches!(err, ArrowError::IpcError(_)), "{err:?}");
    }

    #[test]
    fn buffer_stream_reader_schema_only() {
        let (_, schema) = make_int_batch(0);
        let bytes = serialize_stream(&[], schema.as_ref());
        let mut r = BufferStreamReader::try_new(Buffer::from_vec(bytes)).unwrap();
        assert_eq!(r.schema().as_ref(), schema.as_ref());
        assert!(r.next().is_none());
    }

    #[test]
    fn buffer_stream_reader_dict_batch() {
        let dict_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let schema = Arc::new(Schema::new(vec![Field::new("d", dict_type, false)]));
        let values = arrow_array::StringArray::from(vec!["a", "b"]);
        let keys = Int32Array::from(vec![0, 1, 0, 1, 0]);
        let dict = DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).unwrap();
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(dict)]).unwrap();
        let bytes = serialize_stream(std::slice::from_ref(&batch), schema.as_ref());
        let mut r = BufferStreamReader::try_new(Buffer::from_vec(bytes)).unwrap();
        let got = r.next().unwrap().unwrap();
        assert_eq!(got, batch);
        assert!(r.next().is_none());
    }

    #[test]
    fn buffer_stream_reader_no_eos_marker() {
        let (batch, schema) = make_int_batch(4);
        let mut bytes = serialize_stream(std::slice::from_ref(&batch), schema.as_ref());
        bytes.truncate(bytes.len() - 8); // strip the EOS marker
        let r = BufferStreamReader::try_new(Buffer::from_vec(bytes)).unwrap();
        let collected: Vec<_> = r.map(Result::unwrap).collect();
        assert_eq!(collected, vec![batch]);
    }

    #[test]
    fn buffer_stream_reader_truncated_message_errors() {
        let (a, schema) = make_int_batch(4);
        let (b, _) = make_int_batch(8);
        let mut bytes = serialize_stream(&[a.clone(), b], schema.as_ref());
        bytes.truncate(bytes.len() - 16); // cut into the last batch's body
        let mut r = BufferStreamReader::try_new(Buffer::from_vec(bytes)).unwrap();
        assert_eq!(r.next().unwrap().unwrap(), a);
        let err = r.next().unwrap().unwrap_err();
        assert!(matches!(err, ArrowError::IpcError(_)), "{err:?}");
        assert!(r.next().is_none());
        assert!(r.is_finished());
    }

    #[test]
    fn buffer_stream_reader_from_bytes() {
        let (batch, schema) = make_int_batch(4);
        let bytes = serialize_stream(std::slice::from_ref(&batch), schema.as_ref());
        let mut r = BufferStreamReader::try_new_from_bytes(bytes::Bytes::from(bytes)).unwrap();
        assert_eq!(r.next().unwrap().unwrap(), batch);
    }
}
