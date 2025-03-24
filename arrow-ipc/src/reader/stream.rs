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
use crate::reader::{read_dictionary_impl, RecordBatchDecoder};
use crate::{MessageHeader, CONTINUATION_MARKER};

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
    /// See [`FileDecoder::with_skip_validation`] for details.
    ///
    /// [`FileDecoder::with_skip_validation`]: crate::reader::FileDecoder::with_skip_validation
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
    /// Under the hood it will use [`arrow_data::ArrayDataBuilder::build_aligned`] to construct
    /// [`arrow_data::ArrayData`].
    pub fn with_require_alignment(mut self, require_alignment: bool) -> Self {
        self.require_alignment = require_alignment;
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
                    self.buf.extend_from_slice(&buffer[..to_read]);
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
                        self.buf.extend_from_slice(&buffer[..to_read]);
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

                            let ipc_schema = message.header_as_schema().unwrap();
                            let schema = crate::convert::fb_to_schema(ipc_schema);
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
                            )))
                        }
                    }
                }
                DecoderState::Finished => {
                    return Err(ArrowError::IpcError("Unexpected EOS".to_string()))
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::writer::{IpcWriteOptions, StreamWriter};
    use arrow_array::{
        types::Int32Type, DictionaryArray, Int32Array, Int64Array, RecordBatch, RunArray,
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
    fn test_read_ree_dict_record_batches_from_buffer() {
        let schema = Schema::new(vec![Field::new(
            "test1",
            DataType::RunEndEncoded(
                Arc::new(Field::new("run_ends".to_string(), DataType::Int32, false)),
                #[allow(deprecated)]
                Arc::new(Field::new_dict(
                    "values".to_string(),
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
                #[allow(deprecated)]
                IpcWriteOptions::default().with_preserve_dict_id(false),
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
                ArrowError::ExternalError(format!("Failed to decode record batch: {}", e).into())
            })
            .expect("Failed to decode record batch")
        {
            assert_eq!(batch, batch);
        }

        decoder.finish().expect("Failed to finish decoder");
    }
}
