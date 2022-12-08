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

//! Arrow IPC File and Stream Writers
//!
//! The `FileWriter` and `StreamWriter` have similar interfaces,
//! however the `FileWriter` expects a reader that supports `Seek`ing

use std::cmp::min;
use std::collections::HashMap;
use std::io::{BufWriter, Write};

use flatbuffers::FlatBufferBuilder;

use arrow_array::builder::BufferBuilder;
use arrow_array::cast::*;
use arrow_array::*;
use arrow_buffer::bit_util;
use arrow_buffer::{Buffer, MutableBuffer};
use arrow_data::{layout, ArrayData, BufferSpec};
use arrow_schema::*;

use crate::compression::CompressionCodec;
use crate::CONTINUATION_MARKER;

/// IPC write options used to control the behaviour of the writer
#[derive(Debug, Clone)]
pub struct IpcWriteOptions {
    /// Write padding after memory buffers to this multiple of bytes.
    /// Generally 8 or 64, defaults to 64
    alignment: usize,
    /// The legacy format is for releases before 0.15.0, and uses metadata V4
    write_legacy_ipc_format: bool,
    /// The metadata version to write. The Rust IPC writer supports V4+
    ///
    /// *Default versions per crate*
    ///
    /// When creating the default IpcWriteOptions, the following metadata versions are used:
    ///
    /// version 2.0.0: V4, with legacy format enabled
    /// version 4.0.0: V5
    metadata_version: crate::MetadataVersion,
    /// Compression, if desired. Will result in a runtime error
    /// if the corresponding feature is not enabled
    batch_compression_type: Option<crate::CompressionType>,
}

impl IpcWriteOptions {
    /// Configures compression when writing IPC files.
    ///
    /// Will result in a runtime error if the corresponding feature
    /// is not enabled
    pub fn try_with_compression(
        mut self,
        batch_compression_type: Option<crate::CompressionType>,
    ) -> Result<Self, ArrowError> {
        self.batch_compression_type = batch_compression_type;

        if self.batch_compression_type.is_some()
            && self.metadata_version < crate::MetadataVersion::V5
        {
            return Err(ArrowError::InvalidArgumentError(
                "Compression only supported in metadata v5 and above".to_string(),
            ));
        }
        Ok(self)
    }
    /// Try create IpcWriteOptions, checking for incompatible settings
    pub fn try_new(
        alignment: usize,
        write_legacy_ipc_format: bool,
        metadata_version: crate::MetadataVersion,
    ) -> Result<Self, ArrowError> {
        if alignment == 0 || alignment % 8 != 0 {
            return Err(ArrowError::InvalidArgumentError(
                "Alignment should be greater than 0 and be a multiple of 8".to_string(),
            ));
        }
        match metadata_version {
            crate::MetadataVersion::V1
            | crate::MetadataVersion::V2
            | crate::MetadataVersion::V3 => Err(ArrowError::InvalidArgumentError(
                "Writing IPC metadata version 3 and lower not supported".to_string(),
            )),
            crate::MetadataVersion::V4 => Ok(Self {
                alignment,
                write_legacy_ipc_format,
                metadata_version,
                batch_compression_type: None,
            }),
            crate::MetadataVersion::V5 => {
                if write_legacy_ipc_format {
                    Err(ArrowError::InvalidArgumentError(
                        "Legacy IPC format only supported on metadata version 4"
                            .to_string(),
                    ))
                } else {
                    Ok(Self {
                        alignment,
                        write_legacy_ipc_format,
                        metadata_version,
                        batch_compression_type: None,
                    })
                }
            }
            z => Err(ArrowError::InvalidArgumentError(format!(
                "Unsupported crate::MetadataVersion {:?}",
                z
            ))),
        }
    }
}

impl Default for IpcWriteOptions {
    fn default() -> Self {
        Self {
            alignment: 64,
            write_legacy_ipc_format: false,
            metadata_version: crate::MetadataVersion::V5,
            batch_compression_type: None,
        }
    }
}

#[derive(Debug, Default)]
pub struct IpcDataGenerator {}

impl IpcDataGenerator {
    pub fn schema_to_bytes(
        &self,
        schema: &Schema,
        write_options: &IpcWriteOptions,
    ) -> EncodedData {
        let mut fbb = FlatBufferBuilder::new();
        let schema = {
            let fb = crate::convert::schema_to_fb_offset(&mut fbb, schema);
            fb.as_union_value()
        };

        let mut message = crate::MessageBuilder::new(&mut fbb);
        message.add_version(write_options.metadata_version);
        message.add_header_type(crate::MessageHeader::Schema);
        message.add_bodyLength(0);
        message.add_header(schema);
        // TODO: custom metadata
        let data = message.finish();
        fbb.finish(data, None);

        let data = fbb.finished_data();
        EncodedData {
            ipc_message: data.to_vec(),
            arrow_data: vec![],
        }
    }

    fn _encode_dictionaries(
        &self,
        column: &ArrayRef,
        encoded_dictionaries: &mut Vec<EncodedData>,
        dictionary_tracker: &mut DictionaryTracker,
        write_options: &IpcWriteOptions,
    ) -> Result<(), ArrowError> {
        match column.data_type() {
            DataType::Struct(fields) => {
                let s = as_struct_array(column);
                for (field, column) in fields.iter().zip(s.columns()) {
                    self.encode_dictionaries(
                        field,
                        column,
                        encoded_dictionaries,
                        dictionary_tracker,
                        write_options,
                    )?;
                }
            }
            DataType::List(field) => {
                let list = as_list_array(column);
                self.encode_dictionaries(
                    field,
                    &list.values(),
                    encoded_dictionaries,
                    dictionary_tracker,
                    write_options,
                )?;
            }
            DataType::LargeList(field) => {
                let list = as_large_list_array(column);
                self.encode_dictionaries(
                    field,
                    &list.values(),
                    encoded_dictionaries,
                    dictionary_tracker,
                    write_options,
                )?;
            }
            DataType::FixedSizeList(field, _) => {
                let list = column
                    .as_any()
                    .downcast_ref::<FixedSizeListArray>()
                    .expect("Unable to downcast to fixed size list array");
                self.encode_dictionaries(
                    field,
                    &list.values(),
                    encoded_dictionaries,
                    dictionary_tracker,
                    write_options,
                )?;
            }
            DataType::Map(field, _) => {
                let map_array = as_map_array(column);

                let (keys, values) = match field.data_type() {
                    DataType::Struct(fields) if fields.len() == 2 => {
                        (&fields[0], &fields[1])
                    }
                    _ => panic!("Incorrect field data type {:?}", field.data_type()),
                };

                // keys
                self.encode_dictionaries(
                    keys,
                    &map_array.keys(),
                    encoded_dictionaries,
                    dictionary_tracker,
                    write_options,
                )?;

                // values
                self.encode_dictionaries(
                    values,
                    &map_array.values(),
                    encoded_dictionaries,
                    dictionary_tracker,
                    write_options,
                )?;
            }
            DataType::Union(fields, _, _) => {
                let union = as_union_array(column);
                for (field, column) in fields
                    .iter()
                    .enumerate()
                    .map(|(n, f)| (f, union.child(n as i8)))
                {
                    self.encode_dictionaries(
                        field,
                        column,
                        encoded_dictionaries,
                        dictionary_tracker,
                        write_options,
                    )?;
                }
            }
            _ => (),
        }

        Ok(())
    }

    fn encode_dictionaries(
        &self,
        field: &Field,
        column: &ArrayRef,
        encoded_dictionaries: &mut Vec<EncodedData>,
        dictionary_tracker: &mut DictionaryTracker,
        write_options: &IpcWriteOptions,
    ) -> Result<(), ArrowError> {
        match column.data_type() {
            DataType::Dictionary(_key_type, _value_type) => {
                let dict_id = field
                    .dict_id()
                    .expect("All Dictionary types have `dict_id`");
                let dict_data = column.data();
                let dict_values = &dict_data.child_data()[0];

                let values = make_array(dict_data.child_data()[0].clone());

                self._encode_dictionaries(
                    &values,
                    encoded_dictionaries,
                    dictionary_tracker,
                    write_options,
                )?;

                let emit = dictionary_tracker.insert(dict_id, column)?;

                if emit {
                    encoded_dictionaries.push(self.dictionary_batch_to_bytes(
                        dict_id,
                        dict_values,
                        write_options,
                    )?);
                }
            }
            _ => self._encode_dictionaries(
                column,
                encoded_dictionaries,
                dictionary_tracker,
                write_options,
            )?,
        }

        Ok(())
    }

    pub fn encoded_batch(
        &self,
        batch: &RecordBatch,
        dictionary_tracker: &mut DictionaryTracker,
        write_options: &IpcWriteOptions,
    ) -> Result<(Vec<EncodedData>, EncodedData), ArrowError> {
        let schema = batch.schema();
        let mut encoded_dictionaries = Vec::with_capacity(schema.all_fields().len());

        for (i, field) in schema.fields().iter().enumerate() {
            let column = batch.column(i);
            self.encode_dictionaries(
                field,
                column,
                &mut encoded_dictionaries,
                dictionary_tracker,
                write_options,
            )?;
        }

        let encoded_message = self.record_batch_to_bytes(batch, write_options)?;
        Ok((encoded_dictionaries, encoded_message))
    }

    /// Write a `RecordBatch` into two sets of bytes, one for the header (crate::Message) and the
    /// other for the batch's data
    fn record_batch_to_bytes(
        &self,
        batch: &RecordBatch,
        write_options: &IpcWriteOptions,
    ) -> Result<EncodedData, ArrowError> {
        let mut fbb = FlatBufferBuilder::new();

        let mut nodes: Vec<crate::FieldNode> = vec![];
        let mut buffers: Vec<crate::Buffer> = vec![];
        let mut arrow_data: Vec<u8> = vec![];
        let mut offset = 0;

        // get the type of compression
        let batch_compression_type = write_options.batch_compression_type;

        let compression = batch_compression_type.map(|batch_compression_type| {
            let mut c = crate::BodyCompressionBuilder::new(&mut fbb);
            c.add_method(crate::BodyCompressionMethod::BUFFER);
            c.add_codec(batch_compression_type);
            c.finish()
        });

        let compression_codec: Option<CompressionCodec> =
            batch_compression_type.map(TryInto::try_into).transpose()?;

        for array in batch.columns() {
            let array_data = array.data();
            offset = write_array_data(
                array_data,
                &mut buffers,
                &mut arrow_data,
                &mut nodes,
                offset,
                array.len(),
                array.null_count(),
                compression_codec,
                write_options,
            )?;
        }
        // pad the tail of body data
        let len = arrow_data.len();
        let pad_len = pad_to_8(len as u32);
        arrow_data.extend_from_slice(&vec![0u8; pad_len][..]);

        // write data
        let buffers = fbb.create_vector(&buffers);
        let nodes = fbb.create_vector(&nodes);
        let root = {
            let mut batch_builder = crate::RecordBatchBuilder::new(&mut fbb);
            batch_builder.add_length(batch.num_rows() as i64);
            batch_builder.add_nodes(nodes);
            batch_builder.add_buffers(buffers);
            if let Some(c) = compression {
                batch_builder.add_compression(c);
            }
            let b = batch_builder.finish();
            b.as_union_value()
        };
        // create an crate::Message
        let mut message = crate::MessageBuilder::new(&mut fbb);
        message.add_version(write_options.metadata_version);
        message.add_header_type(crate::MessageHeader::RecordBatch);
        message.add_bodyLength(arrow_data.len() as i64);
        message.add_header(root);
        let root = message.finish();
        fbb.finish(root, None);
        let finished_data = fbb.finished_data();

        Ok(EncodedData {
            ipc_message: finished_data.to_vec(),
            arrow_data,
        })
    }

    /// Write dictionary values into two sets of bytes, one for the header (crate::Message) and the
    /// other for the data
    fn dictionary_batch_to_bytes(
        &self,
        dict_id: i64,
        array_data: &ArrayData,
        write_options: &IpcWriteOptions,
    ) -> Result<EncodedData, ArrowError> {
        let mut fbb = FlatBufferBuilder::new();

        let mut nodes: Vec<crate::FieldNode> = vec![];
        let mut buffers: Vec<crate::Buffer> = vec![];
        let mut arrow_data: Vec<u8> = vec![];

        // get the type of compression
        let batch_compression_type = write_options.batch_compression_type;

        let compression = batch_compression_type.map(|batch_compression_type| {
            let mut c = crate::BodyCompressionBuilder::new(&mut fbb);
            c.add_method(crate::BodyCompressionMethod::BUFFER);
            c.add_codec(batch_compression_type);
            c.finish()
        });

        let compression_codec: Option<CompressionCodec> = batch_compression_type
            .map(|batch_compression_type| batch_compression_type.try_into())
            .transpose()?;

        write_array_data(
            array_data,
            &mut buffers,
            &mut arrow_data,
            &mut nodes,
            0,
            array_data.len(),
            array_data.null_count(),
            compression_codec,
            write_options,
        )?;

        // pad the tail of body data
        let len = arrow_data.len();
        let pad_len = pad_to_8(len as u32);
        arrow_data.extend_from_slice(&vec![0u8; pad_len][..]);

        // write data
        let buffers = fbb.create_vector(&buffers);
        let nodes = fbb.create_vector(&nodes);

        let root = {
            let mut batch_builder = crate::RecordBatchBuilder::new(&mut fbb);
            batch_builder.add_length(array_data.len() as i64);
            batch_builder.add_nodes(nodes);
            batch_builder.add_buffers(buffers);
            if let Some(c) = compression {
                batch_builder.add_compression(c);
            }
            batch_builder.finish()
        };

        let root = {
            let mut batch_builder = crate::DictionaryBatchBuilder::new(&mut fbb);
            batch_builder.add_id(dict_id);
            batch_builder.add_data(root);
            batch_builder.finish().as_union_value()
        };

        let root = {
            let mut message_builder = crate::MessageBuilder::new(&mut fbb);
            message_builder.add_version(write_options.metadata_version);
            message_builder.add_header_type(crate::MessageHeader::DictionaryBatch);
            message_builder.add_bodyLength(arrow_data.len() as i64);
            message_builder.add_header(root);
            message_builder.finish()
        };

        fbb.finish(root, None);
        let finished_data = fbb.finished_data();

        Ok(EncodedData {
            ipc_message: finished_data.to_vec(),
            arrow_data,
        })
    }
}

/// Keeps track of dictionaries that have been written, to avoid emitting the same dictionary
/// multiple times. Can optionally error if an update to an existing dictionary is attempted, which
/// isn't allowed in the `FileWriter`.
pub struct DictionaryTracker {
    written: HashMap<i64, ArrayRef>,
    error_on_replacement: bool,
}

impl DictionaryTracker {
    pub fn new(error_on_replacement: bool) -> Self {
        Self {
            written: HashMap::new(),
            error_on_replacement,
        }
    }

    /// Keep track of the dictionary with the given ID and values. Behavior:
    ///
    /// * If this ID has been written already and has the same data, return `Ok(false)` to indicate
    ///   that the dictionary was not actually inserted (because it's already been seen).
    /// * If this ID has been written already but with different data, and this tracker is
    ///   configured to return an error, return an error.
    /// * If the tracker has not been configured to error on replacement or this dictionary
    ///   has never been seen before, return `Ok(true)` to indicate that the dictionary was just
    ///   inserted.
    pub fn insert(
        &mut self,
        dict_id: i64,
        column: &ArrayRef,
    ) -> Result<bool, ArrowError> {
        let dict_data = column.data();
        let dict_values = &dict_data.child_data()[0];

        // If a dictionary with this id was already emitted, check if it was the same.
        if let Some(last) = self.written.get(&dict_id) {
            if last.data().child_data()[0] == *dict_values {
                // Same dictionary values => no need to emit it again
                return Ok(false);
            } else if self.error_on_replacement {
                return Err(ArrowError::InvalidArgumentError(
                    "Dictionary replacement detected when writing IPC file format. \
                     Arrow IPC files only support a single dictionary for a given field \
                     across all batches."
                        .to_string(),
                ));
            }
        }

        self.written.insert(dict_id, column.clone());
        Ok(true)
    }
}

pub struct FileWriter<W: Write> {
    /// The object to write to
    writer: BufWriter<W>,
    /// IPC write options
    write_options: IpcWriteOptions,
    /// A reference to the schema, used in validating record batches
    schema: Schema,
    /// The number of bytes between each block of bytes, as an offset for random access
    block_offsets: usize,
    /// Dictionary blocks that will be written as part of the IPC footer
    dictionary_blocks: Vec<crate::Block>,
    /// Record blocks that will be written as part of the IPC footer
    record_blocks: Vec<crate::Block>,
    /// Whether the writer footer has been written, and the writer is finished
    finished: bool,
    /// Keeps track of dictionaries that have been written
    dictionary_tracker: DictionaryTracker,

    data_gen: IpcDataGenerator,
}

impl<W: Write> FileWriter<W> {
    /// Try create a new writer, with the schema written as part of the header
    pub fn try_new(writer: W, schema: &Schema) -> Result<Self, ArrowError> {
        let write_options = IpcWriteOptions::default();
        Self::try_new_with_options(writer, schema, write_options)
    }

    /// Try create a new writer with IpcWriteOptions
    pub fn try_new_with_options(
        writer: W,
        schema: &Schema,
        write_options: IpcWriteOptions,
    ) -> Result<Self, ArrowError> {
        let data_gen = IpcDataGenerator::default();
        let mut writer = BufWriter::new(writer);
        // write magic to header aligned on 8 byte boundary
        let header_size = super::ARROW_MAGIC.len() + 2;
        assert_eq!(header_size, 8);
        writer.write_all(&super::ARROW_MAGIC[..])?;
        writer.write_all(&[0, 0])?;
        // write the schema, set the written bytes to the schema + header
        let encoded_message = data_gen.schema_to_bytes(schema, &write_options);
        let (meta, data) = write_message(&mut writer, encoded_message, &write_options)?;
        Ok(Self {
            writer,
            write_options,
            schema: schema.clone(),
            block_offsets: meta + data + header_size,
            dictionary_blocks: vec![],
            record_blocks: vec![],
            finished: false,
            dictionary_tracker: DictionaryTracker::new(true),
            data_gen,
        })
    }

    /// Write a record batch to the file
    pub fn write(&mut self, batch: &RecordBatch) -> Result<(), ArrowError> {
        if self.finished {
            return Err(ArrowError::IoError(
                "Cannot write record batch to file writer as it is closed".to_string(),
            ));
        }

        let (encoded_dictionaries, encoded_message) = self.data_gen.encoded_batch(
            batch,
            &mut self.dictionary_tracker,
            &self.write_options,
        )?;

        for encoded_dictionary in encoded_dictionaries {
            let (meta, data) =
                write_message(&mut self.writer, encoded_dictionary, &self.write_options)?;

            let block =
                crate::Block::new(self.block_offsets as i64, meta as i32, data as i64);
            self.dictionary_blocks.push(block);
            self.block_offsets += meta + data;
        }

        let (meta, data) =
            write_message(&mut self.writer, encoded_message, &self.write_options)?;
        // add a record block for the footer
        let block = crate::Block::new(
            self.block_offsets as i64,
            meta as i32, // TODO: is this still applicable?
            data as i64,
        );
        self.record_blocks.push(block);
        self.block_offsets += meta + data;
        Ok(())
    }

    /// Write footer and closing tag, then mark the writer as done
    pub fn finish(&mut self) -> Result<(), ArrowError> {
        if self.finished {
            return Err(ArrowError::IoError(
                "Cannot write footer to file writer as it is closed".to_string(),
            ));
        }

        // write EOS
        write_continuation(&mut self.writer, &self.write_options, 0)?;

        let mut fbb = FlatBufferBuilder::new();
        let dictionaries = fbb.create_vector(&self.dictionary_blocks);
        let record_batches = fbb.create_vector(&self.record_blocks);
        let schema = crate::convert::schema_to_fb_offset(&mut fbb, &self.schema);

        let root = {
            let mut footer_builder = crate::FooterBuilder::new(&mut fbb);
            footer_builder.add_version(self.write_options.metadata_version);
            footer_builder.add_schema(schema);
            footer_builder.add_dictionaries(dictionaries);
            footer_builder.add_recordBatches(record_batches);
            footer_builder.finish()
        };
        fbb.finish(root, None);
        let footer_data = fbb.finished_data();
        self.writer.write_all(footer_data)?;
        self.writer
            .write_all(&(footer_data.len() as i32).to_le_bytes())?;
        self.writer.write_all(&super::ARROW_MAGIC)?;
        self.writer.flush()?;
        self.finished = true;

        Ok(())
    }

    /// Unwraps the BufWriter housed in FileWriter.writer, returning the underlying
    /// writer
    ///
    /// The buffer is flushed and the FileWriter is finished before returning the
    /// writer.
    pub fn into_inner(mut self) -> Result<W, ArrowError> {
        if !self.finished {
            self.finish()?;
        }
        self.writer.into_inner().map_err(ArrowError::from)
    }
}

pub struct StreamWriter<W: Write> {
    /// The object to write to
    writer: BufWriter<W>,
    /// IPC write options
    write_options: IpcWriteOptions,
    /// Whether the writer footer has been written, and the writer is finished
    finished: bool,
    /// Keeps track of dictionaries that have been written
    dictionary_tracker: DictionaryTracker,

    data_gen: IpcDataGenerator,
}

impl<W: Write> StreamWriter<W> {
    /// Try create a new writer, with the schema written as part of the header
    pub fn try_new(writer: W, schema: &Schema) -> Result<Self, ArrowError> {
        let write_options = IpcWriteOptions::default();
        Self::try_new_with_options(writer, schema, write_options)
    }

    pub fn try_new_with_options(
        writer: W,
        schema: &Schema,
        write_options: IpcWriteOptions,
    ) -> Result<Self, ArrowError> {
        let data_gen = IpcDataGenerator::default();
        let mut writer = BufWriter::new(writer);
        // write the schema, set the written bytes to the schema
        let encoded_message = data_gen.schema_to_bytes(schema, &write_options);
        write_message(&mut writer, encoded_message, &write_options)?;
        Ok(Self {
            writer,
            write_options,
            finished: false,
            dictionary_tracker: DictionaryTracker::new(false),
            data_gen,
        })
    }

    /// Write a record batch to the stream
    pub fn write(&mut self, batch: &RecordBatch) -> Result<(), ArrowError> {
        if self.finished {
            return Err(ArrowError::IoError(
                "Cannot write record batch to stream writer as it is closed".to_string(),
            ));
        }

        let (encoded_dictionaries, encoded_message) = self
            .data_gen
            .encoded_batch(batch, &mut self.dictionary_tracker, &self.write_options)
            .expect("StreamWriter is configured to not error on dictionary replacement");

        for encoded_dictionary in encoded_dictionaries {
            write_message(&mut self.writer, encoded_dictionary, &self.write_options)?;
        }

        write_message(&mut self.writer, encoded_message, &self.write_options)?;
        Ok(())
    }

    /// Write continuation bytes, and mark the stream as done
    pub fn finish(&mut self) -> Result<(), ArrowError> {
        if self.finished {
            return Err(ArrowError::IoError(
                "Cannot write footer to stream writer as it is closed".to_string(),
            ));
        }

        write_continuation(&mut self.writer, &self.write_options, 0)?;

        self.finished = true;

        Ok(())
    }

    /// Unwraps the BufWriter housed in StreamWriter.writer, returning the underlying
    /// writer
    ///
    /// The buffer is flushed and the StreamWriter is finished before returning the
    /// writer.
    ///
    /// # Errors
    ///
    /// An ['Err'] may be returned if an error occurs while finishing the StreamWriter
    /// or while flushing the buffer.
    ///
    /// # Example
    ///
    /// ```
    /// # use arrow_ipc::writer::{StreamWriter, IpcWriteOptions};
    /// # use arrow_ipc::MetadataVersion;
    /// # use arrow_schema::{ArrowError, Schema};
    /// # fn main() -> Result<(), ArrowError> {
    /// // The result we expect from an empty schema
    /// let expected = vec![
    ///     255, 255, 255, 255,  48,   0,   0,   0,
    ///      16,   0,   0,   0,   0,   0,  10,   0,
    ///      12,   0,  10,   0,   9,   0,   4,   0,
    ///      10,   0,   0,   0,  16,   0,   0,   0,
    ///       0,   1,   4,   0,   8,   0,   8,   0,
    ///       0,   0,   4,   0,   8,   0,   0,   0,
    ///       4,   0,   0,   0,   0,   0,   0,   0,
    ///     255, 255, 255, 255,   0,   0,   0,   0
    /// ];
    ///
    /// let schema = Schema::new(vec![]);
    /// let buffer: Vec<u8> = Vec::new();
    /// let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5)?;
    /// let stream_writer = StreamWriter::try_new_with_options(buffer, &schema, options)?;
    ///
    /// assert_eq!(stream_writer.into_inner()?, expected);
    /// # Ok(())
    /// # }
    /// ```
    pub fn into_inner(mut self) -> Result<W, ArrowError> {
        if !self.finished {
            self.finish()?;
        }
        self.writer.into_inner().map_err(ArrowError::from)
    }
}

/// Stores the encoded data, which is an crate::Message, and optional Arrow data
pub struct EncodedData {
    /// An encoded crate::Message
    pub ipc_message: Vec<u8>,
    /// Arrow buffers to be written, should be an empty vec for schema messages
    pub arrow_data: Vec<u8>,
}
/// Write a message's IPC data and buffers, returning metadata and buffer data lengths written
pub fn write_message<W: Write>(
    mut writer: W,
    encoded: EncodedData,
    write_options: &IpcWriteOptions,
) -> Result<(usize, usize), ArrowError> {
    let arrow_data_len = encoded.arrow_data.len();
    if arrow_data_len % 8 != 0 {
        return Err(ArrowError::MemoryError(
            "Arrow data not aligned".to_string(),
        ));
    }

    let a = write_options.alignment - 1;
    let buffer = encoded.ipc_message;
    let flatbuf_size = buffer.len();
    let prefix_size = if write_options.write_legacy_ipc_format {
        4
    } else {
        8
    };
    let aligned_size = (flatbuf_size + prefix_size + a) & !a;
    let padding_bytes = aligned_size - flatbuf_size - prefix_size;

    write_continuation(
        &mut writer,
        write_options,
        (aligned_size - prefix_size) as i32,
    )?;

    // write the flatbuf
    if flatbuf_size > 0 {
        writer.write_all(&buffer)?;
    }
    // write padding
    writer.write_all(&vec![0; padding_bytes])?;

    // write arrow data
    let body_len = if arrow_data_len > 0 {
        write_body_buffers(&mut writer, &encoded.arrow_data)?
    } else {
        0
    };

    Ok((aligned_size, body_len))
}

fn write_body_buffers<W: Write>(mut writer: W, data: &[u8]) -> Result<usize, ArrowError> {
    let len = data.len() as u32;
    let pad_len = pad_to_8(len) as u32;
    let total_len = len + pad_len;

    // write body buffer
    writer.write_all(data)?;
    if pad_len > 0 {
        writer.write_all(&vec![0u8; pad_len as usize][..])?;
    }

    writer.flush()?;
    Ok(total_len as usize)
}

/// Write a record batch to the writer, writing the message size before the message
/// if the record batch is being written to a stream
fn write_continuation<W: Write>(
    mut writer: W,
    write_options: &IpcWriteOptions,
    total_len: i32,
) -> Result<usize, ArrowError> {
    let mut written = 8;

    // the version of the writer determines whether continuation markers should be added
    match write_options.metadata_version {
        crate::MetadataVersion::V1
        | crate::MetadataVersion::V2
        | crate::MetadataVersion::V3 => {
            unreachable!("Options with the metadata version cannot be created")
        }
        crate::MetadataVersion::V4 => {
            if !write_options.write_legacy_ipc_format {
                // v0.15.0 format
                writer.write_all(&CONTINUATION_MARKER)?;
                written = 4;
            }
            writer.write_all(&total_len.to_le_bytes()[..])?;
        }
        crate::MetadataVersion::V5 => {
            // write continuation marker and message length
            writer.write_all(&CONTINUATION_MARKER)?;
            writer.write_all(&total_len.to_le_bytes()[..])?;
        }
        z => panic!("Unsupported crate::MetadataVersion {:?}", z),
    };

    writer.flush()?;

    Ok(written)
}

/// In V4, null types have no validity bitmap
/// In V5 and later, null and union types have no validity bitmap
fn has_validity_bitmap(data_type: &DataType, write_options: &IpcWriteOptions) -> bool {
    if write_options.metadata_version < crate::MetadataVersion::V5 {
        !matches!(data_type, DataType::Null)
    } else {
        !matches!(data_type, DataType::Null | DataType::Union(_, _, _))
    }
}

/// Whether to truncate the buffer
#[inline]
fn buffer_need_truncate(
    array_offset: usize,
    buffer: &Buffer,
    spec: &BufferSpec,
    min_length: usize,
) -> bool {
    spec != &BufferSpec::AlwaysNull && (array_offset != 0 || min_length < buffer.len())
}

/// Returns byte width for a buffer spec. Only for `BufferSpec::FixedWidth`.
#[inline]
fn get_buffer_element_width(spec: &BufferSpec) -> usize {
    match spec {
        BufferSpec::FixedWidth { byte_width } => *byte_width,
        _ => 0,
    }
}

/// Returns byte width for binary value_offset buffer spec.
#[inline]
fn get_value_offset_byte_width(data_type: &DataType) -> usize {
    match data_type {
        DataType::Binary | DataType::Utf8 => 4,
        DataType::LargeBinary | DataType::LargeUtf8 => 8,
        _ => unreachable!(),
    }
}

/// Returns the number of total bytes in base binary arrays.
fn get_binary_buffer_len(array_data: &ArrayData) -> usize {
    if array_data.is_empty() {
        return 0;
    }
    match array_data.data_type() {
        DataType::Binary => {
            let array: BinaryArray = array_data.clone().into();
            let offsets = array.value_offsets();
            (offsets[array_data.len()] - offsets[0]) as usize
        }
        DataType::LargeBinary => {
            let array: LargeBinaryArray = array_data.clone().into();
            let offsets = array.value_offsets();
            (offsets[array_data.len()] - offsets[0]) as usize
        }
        DataType::Utf8 => {
            let array: StringArray = array_data.clone().into();
            let offsets = array.value_offsets();
            (offsets[array_data.len()] - offsets[0]) as usize
        }
        DataType::LargeUtf8 => {
            let array: LargeStringArray = array_data.clone().into();
            let offsets = array.value_offsets();
            (offsets[array_data.len()] - offsets[0]) as usize
        }
        _ => unreachable!(),
    }
}

/// Rebase value offsets for given ArrayData to zero-based.
fn get_zero_based_value_offsets<OffsetSize: OffsetSizeTrait>(
    array_data: &ArrayData,
) -> Buffer {
    match array_data.data_type() {
        DataType::Binary | DataType::LargeBinary => {
            let array: GenericBinaryArray<OffsetSize> = array_data.clone().into();
            let offsets = array.value_offsets();
            let start_offset = offsets[0];

            let mut builder = BufferBuilder::<OffsetSize>::new(array_data.len() + 1);
            for x in offsets {
                builder.append(*x - start_offset);
            }

            builder.finish()
        }
        DataType::Utf8 | DataType::LargeUtf8 => {
            let array: GenericStringArray<OffsetSize> = array_data.clone().into();
            let offsets = array.value_offsets();
            let start_offset = offsets[0];

            let mut builder = BufferBuilder::<OffsetSize>::new(array_data.len() + 1);
            for x in offsets {
                builder.append(*x - start_offset);
            }

            builder.finish()
        }
        _ => unreachable!(),
    }
}

/// Returns the start offset of base binary array.
fn get_buffer_offset<OffsetSize: OffsetSizeTrait>(array_data: &ArrayData) -> OffsetSize {
    match array_data.data_type() {
        DataType::Binary | DataType::LargeBinary => {
            let array: GenericBinaryArray<OffsetSize> = array_data.clone().into();
            let offsets = array.value_offsets();
            offsets[0]
        }
        DataType::Utf8 | DataType::LargeUtf8 => {
            let array: GenericStringArray<OffsetSize> = array_data.clone().into();
            let offsets = array.value_offsets();
            offsets[0]
        }
        _ => unreachable!(),
    }
}

/// Write array data to a vector of bytes
#[allow(clippy::too_many_arguments)]
fn write_array_data(
    array_data: &ArrayData,
    buffers: &mut Vec<crate::Buffer>,
    arrow_data: &mut Vec<u8>,
    nodes: &mut Vec<crate::FieldNode>,
    offset: i64,
    num_rows: usize,
    null_count: usize,
    compression_codec: Option<CompressionCodec>,
    write_options: &IpcWriteOptions,
) -> Result<i64, ArrowError> {
    let mut offset = offset;
    if !matches!(array_data.data_type(), DataType::Null) {
        nodes.push(crate::FieldNode::new(num_rows as i64, null_count as i64));
    } else {
        // NullArray's null_count equals to len, but the `null_count` passed in is from ArrayData
        // where null_count is always 0.
        nodes.push(crate::FieldNode::new(num_rows as i64, num_rows as i64));
    }
    if has_validity_bitmap(array_data.data_type(), write_options) {
        // write null buffer if exists
        let null_buffer = match array_data.null_buffer() {
            None => {
                // create a buffer and fill it with valid bits
                let num_bytes = bit_util::ceil(num_rows, 8);
                let buffer = MutableBuffer::new(num_bytes);
                let buffer = buffer.with_bitset(num_bytes, true);
                buffer.into()
            }
            Some(buffer) => buffer.bit_slice(array_data.offset(), array_data.len()),
        };

        offset = write_buffer(
            null_buffer.as_slice(),
            buffers,
            arrow_data,
            offset,
            compression_codec,
        )?;
    }

    let data_type = array_data.data_type();
    if matches!(
        data_type,
        DataType::Binary | DataType::LargeBinary | DataType::Utf8 | DataType::LargeUtf8
    ) {
        let offset_buffer = &array_data.buffers()[0];
        let value_offset_byte_width = get_value_offset_byte_width(data_type);
        let min_length = (array_data.len() + 1) * value_offset_byte_width;
        if buffer_need_truncate(
            array_data.offset(),
            offset_buffer,
            &BufferSpec::FixedWidth {
                byte_width: value_offset_byte_width,
            },
            min_length,
        ) {
            // Rebase offsets and truncate values
            let (new_offsets, byte_offset) =
                if matches!(data_type, DataType::Binary | DataType::Utf8) {
                    (
                        get_zero_based_value_offsets::<i32>(array_data),
                        get_buffer_offset::<i32>(array_data) as usize,
                    )
                } else {
                    (
                        get_zero_based_value_offsets::<i64>(array_data),
                        get_buffer_offset::<i64>(array_data) as usize,
                    )
                };

            offset = write_buffer(
                new_offsets.as_slice(),
                buffers,
                arrow_data,
                offset,
                compression_codec,
            )?;

            let total_bytes = get_binary_buffer_len(array_data);
            let value_buffer = &array_data.buffers()[1];
            let buffer_length = min(total_bytes, value_buffer.len() - byte_offset);
            let buffer_slice =
                &value_buffer.as_slice()[byte_offset..(byte_offset + buffer_length)];
            offset = write_buffer(
                buffer_slice,
                buffers,
                arrow_data,
                offset,
                compression_codec,
            )?;
        } else {
            for buffer in array_data.buffers() {
                offset = write_buffer(
                    buffer.as_slice(),
                    buffers,
                    arrow_data,
                    offset,
                    compression_codec,
                )?;
            }
        }
    } else if DataType::is_numeric(data_type)
        || DataType::is_temporal(data_type)
        || matches!(
            array_data.data_type(),
            DataType::FixedSizeBinary(_) | DataType::Dictionary(_, _)
        )
    {
        // Truncate values
        assert!(array_data.buffers().len() == 1);

        let buffer = &array_data.buffers()[0];
        let layout = layout(data_type);
        let spec = &layout.buffers[0];

        let byte_width = get_buffer_element_width(spec);
        let min_length = array_data.len() * byte_width;
        if buffer_need_truncate(array_data.offset(), buffer, spec, min_length) {
            let byte_offset = array_data.offset() * byte_width;
            let buffer_length = min(min_length, buffer.len() - byte_offset);
            let buffer_slice =
                &buffer.as_slice()[byte_offset..(byte_offset + buffer_length)];
            offset = write_buffer(
                buffer_slice,
                buffers,
                arrow_data,
                offset,
                compression_codec,
            )?;
        } else {
            offset = write_buffer(
                buffer.as_slice(),
                buffers,
                arrow_data,
                offset,
                compression_codec,
            )?;
        }
    } else {
        for buffer in array_data.buffers() {
            offset =
                write_buffer(buffer, buffers, arrow_data, offset, compression_codec)?;
        }
    }

    if !matches!(array_data.data_type(), DataType::Dictionary(_, _)) {
        // recursively write out nested structures
        for data_ref in array_data.child_data() {
            // write the nested data (e.g list data)
            offset = write_array_data(
                data_ref,
                buffers,
                arrow_data,
                nodes,
                offset,
                data_ref.len(),
                data_ref.null_count(),
                compression_codec,
                write_options,
            )?;
        }
    }

    Ok(offset)
}

/// Write a buffer into `arrow_data`, a vector of bytes, and adds its
/// [`crate::Buffer`] to `buffers`. Returns the new offset in `arrow_data`
///
///
/// From <https://github.com/apache/arrow/blob/6a936c4ff5007045e86f65f1a6b6c3c955ad5103/format/Message.fbs#L58>
/// Each constituent buffer is first compressed with the indicated
/// compressor, and then written with the uncompressed length in the first 8
/// bytes as a 64-bit little-endian signed integer followed by the compressed
/// buffer bytes (and then padding as required by the protocol). The
/// uncompressed length may be set to -1 to indicate that the data that
/// follows is not compressed, which can be useful for cases where
/// compression does not yield appreciable savings.
fn write_buffer(
    buffer: &[u8],                    // input
    buffers: &mut Vec<crate::Buffer>, // output buffer descriptors
    arrow_data: &mut Vec<u8>,         // output stream
    offset: i64,                      // current output stream offset
    compression_codec: Option<CompressionCodec>,
) -> Result<i64, ArrowError> {
    let len: i64 = match compression_codec {
        Some(compressor) => compressor.compress_to_vec(buffer, arrow_data)?,
        None => {
            arrow_data.extend_from_slice(buffer);
            buffer.len()
        }
    }
    .try_into()
    .map_err(|e| {
        ArrowError::InvalidArgumentError(format!(
            "Could not convert compressed size to i64: {}",
            e
        ))
    })?;

    // make new index entry
    buffers.push(crate::Buffer::new(offset, len));
    // padding and make offset 8 bytes aligned
    let pad_len = pad_to_8(len as u32) as i64;
    arrow_data.extend_from_slice(&vec![0u8; pad_len as usize][..]);

    Ok(offset + len + pad_len)
}

/// Calculate an 8-byte boundary and return the number of bytes needed to pad to 8 bytes
#[inline]
fn pad_to_8(len: u32) -> usize {
    (((len + 7) & !7) - len) as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::Seek;
    use std::sync::Arc;

    use crate::MetadataVersion;

    use crate::reader::*;
    use arrow_array::builder::UnionBuilder;
    use arrow_array::types::*;
    use arrow_schema::DataType;

    #[test]
    #[cfg(feature = "lz4")]
    fn test_write_empty_record_batch_lz4_compression() {
        let schema = Schema::new(vec![Field::new("field1", DataType::Int32, true)]);
        let values: Vec<Option<i32>> = vec![];
        let array = Int32Array::from(values);
        let record_batch =
            RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(array)])
                .unwrap();

        let mut file = tempfile::tempfile().unwrap();

        {
            let write_option =
                IpcWriteOptions::try_new(8, false, crate::MetadataVersion::V5)
                    .unwrap()
                    .try_with_compression(Some(crate::CompressionType::LZ4_FRAME))
                    .unwrap();

            let mut writer =
                FileWriter::try_new_with_options(&mut file, &schema, write_option)
                    .unwrap();
            writer.write(&record_batch).unwrap();
            writer.finish().unwrap();
        }
        file.rewind().unwrap();
        {
            // read file
            let mut reader = FileReader::try_new(file, None).unwrap();
            loop {
                match reader.next() {
                    Some(Ok(read_batch)) => {
                        read_batch
                            .columns()
                            .iter()
                            .zip(record_batch.columns())
                            .for_each(|(a, b)| {
                                assert_eq!(a.data_type(), b.data_type());
                                assert_eq!(a.len(), b.len());
                                assert_eq!(a.null_count(), b.null_count());
                            });
                    }
                    Some(Err(e)) => {
                        panic!("{}", e);
                    }
                    None => {
                        break;
                    }
                }
            }
        }
    }

    #[test]
    #[cfg(feature = "lz4")]
    fn test_write_file_with_lz4_compression() {
        let schema = Schema::new(vec![Field::new("field1", DataType::Int32, true)]);
        let values: Vec<Option<i32>> = vec![Some(12), Some(1)];
        let array = Int32Array::from(values);
        let record_batch =
            RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(array)])
                .unwrap();

        let mut file = tempfile::tempfile().unwrap();
        {
            let write_option =
                IpcWriteOptions::try_new(8, false, crate::MetadataVersion::V5)
                    .unwrap()
                    .try_with_compression(Some(crate::CompressionType::LZ4_FRAME))
                    .unwrap();

            let mut writer =
                FileWriter::try_new_with_options(&mut file, &schema, write_option)
                    .unwrap();
            writer.write(&record_batch).unwrap();
            writer.finish().unwrap();
        }
        file.rewind().unwrap();
        {
            // read file
            let mut reader = FileReader::try_new(file, None).unwrap();
            loop {
                match reader.next() {
                    Some(Ok(read_batch)) => {
                        read_batch
                            .columns()
                            .iter()
                            .zip(record_batch.columns())
                            .for_each(|(a, b)| {
                                assert_eq!(a.data_type(), b.data_type());
                                assert_eq!(a.len(), b.len());
                                assert_eq!(a.null_count(), b.null_count());
                            });
                    }
                    Some(Err(e)) => {
                        panic!("{}", e);
                    }
                    None => {
                        break;
                    }
                }
            }
        }
    }

    #[test]
    #[cfg(feature = "zstd")]
    fn test_write_file_with_zstd_compression() {
        let schema = Schema::new(vec![Field::new("field1", DataType::Int32, true)]);
        let values: Vec<Option<i32>> = vec![Some(12), Some(1)];
        let array = Int32Array::from(values);
        let record_batch =
            RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(array)])
                .unwrap();
        let mut file = tempfile::tempfile().unwrap();
        {
            let write_option =
                IpcWriteOptions::try_new(8, false, crate::MetadataVersion::V5)
                    .unwrap()
                    .try_with_compression(Some(crate::CompressionType::ZSTD))
                    .unwrap();

            let mut writer =
                FileWriter::try_new_with_options(&mut file, &schema, write_option)
                    .unwrap();
            writer.write(&record_batch).unwrap();
            writer.finish().unwrap();
        }
        file.rewind().unwrap();
        {
            // read file
            let mut reader = FileReader::try_new(file, None).unwrap();
            loop {
                match reader.next() {
                    Some(Ok(read_batch)) => {
                        read_batch
                            .columns()
                            .iter()
                            .zip(record_batch.columns())
                            .for_each(|(a, b)| {
                                assert_eq!(a.data_type(), b.data_type());
                                assert_eq!(a.len(), b.len());
                                assert_eq!(a.null_count(), b.null_count());
                            });
                    }
                    Some(Err(e)) => {
                        panic!("{}", e);
                    }
                    None => {
                        break;
                    }
                }
            }
        }
    }

    #[test]
    fn test_write_file() {
        let schema = Schema::new(vec![Field::new("field1", DataType::UInt32, true)]);
        let values: Vec<Option<u32>> = vec![
            Some(999),
            None,
            Some(235),
            Some(123),
            None,
            None,
            None,
            None,
            None,
        ];
        let array1 = UInt32Array::from(values);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(array1) as ArrayRef],
        )
        .unwrap();
        let mut file = tempfile::tempfile().unwrap();
        {
            let mut writer = FileWriter::try_new(&mut file, &schema).unwrap();

            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        file.rewind().unwrap();

        {
            let mut reader = FileReader::try_new(file, None).unwrap();
            while let Some(Ok(read_batch)) = reader.next() {
                read_batch
                    .columns()
                    .iter()
                    .zip(batch.columns())
                    .for_each(|(a, b)| {
                        assert_eq!(a.data_type(), b.data_type());
                        assert_eq!(a.len(), b.len());
                        assert_eq!(a.null_count(), b.null_count());
                    });
            }
        }
    }

    fn write_null_file(options: IpcWriteOptions) {
        let schema = Schema::new(vec![
            Field::new("nulls", DataType::Null, true),
            Field::new("int32s", DataType::Int32, false),
            Field::new("nulls2", DataType::Null, true),
            Field::new("f64s", DataType::Float64, false),
        ]);
        let array1 = NullArray::new(32);
        let array2 = Int32Array::from(vec![1; 32]);
        let array3 = NullArray::new(32);
        let array4 = Float64Array::from(vec![std::f64::NAN; 32]);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(array1) as ArrayRef,
                Arc::new(array2) as ArrayRef,
                Arc::new(array3) as ArrayRef,
                Arc::new(array4) as ArrayRef,
            ],
        )
        .unwrap();
        let mut file = tempfile::tempfile().unwrap();
        {
            let mut writer =
                FileWriter::try_new_with_options(&mut file, &schema, options).unwrap();

            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        file.rewind().unwrap();

        {
            let reader = FileReader::try_new(file, None).unwrap();
            reader.for_each(|maybe_batch| {
                maybe_batch
                    .unwrap()
                    .columns()
                    .iter()
                    .zip(batch.columns())
                    .for_each(|(a, b)| {
                        assert_eq!(a.data_type(), b.data_type());
                        assert_eq!(a.len(), b.len());
                        assert_eq!(a.null_count(), b.null_count());
                    });
            });
        }
    }
    #[test]
    fn test_write_null_file_v4() {
        write_null_file(IpcWriteOptions::try_new(8, false, MetadataVersion::V4).unwrap());
        write_null_file(IpcWriteOptions::try_new(8, true, MetadataVersion::V4).unwrap());
        write_null_file(
            IpcWriteOptions::try_new(64, false, MetadataVersion::V4).unwrap(),
        );
        write_null_file(IpcWriteOptions::try_new(64, true, MetadataVersion::V4).unwrap());
    }

    #[test]
    fn test_write_null_file_v5() {
        write_null_file(IpcWriteOptions::try_new(8, false, MetadataVersion::V5).unwrap());
        write_null_file(
            IpcWriteOptions::try_new(64, false, MetadataVersion::V5).unwrap(),
        );
    }

    #[test]
    fn track_union_nested_dict() {
        let inner: DictionaryArray<Int32Type> = vec!["a", "b", "a"].into_iter().collect();

        let array = Arc::new(inner) as ArrayRef;

        // Dict field with id 2
        let dctfield =
            Field::new_dict("dict", array.data_type().clone(), false, 2, false);

        let types = Buffer::from_slice_ref([0_i8, 0, 0]);
        let offsets = Buffer::from_slice_ref([0_i32, 1, 2]);

        let union =
            UnionArray::try_new(&[0], types, Some(offsets), vec![(dctfield, array)])
                .unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "union",
            union.data_type().clone(),
            false,
        )]));

        let batch = RecordBatch::try_new(schema, vec![Arc::new(union)]).unwrap();

        let gen = IpcDataGenerator {};
        let mut dict_tracker = DictionaryTracker::new(false);
        gen.encoded_batch(&batch, &mut dict_tracker, &Default::default())
            .unwrap();

        // Dictionary with id 2 should have been written to the dict tracker
        assert!(dict_tracker.written.contains_key(&2));
    }

    #[test]
    fn track_struct_nested_dict() {
        let inner: DictionaryArray<Int32Type> = vec!["a", "b", "a"].into_iter().collect();

        let array = Arc::new(inner) as ArrayRef;

        // Dict field with id 2
        let dctfield =
            Field::new_dict("dict", array.data_type().clone(), false, 2, false);

        let s = StructArray::from(vec![(dctfield, array)]);
        let struct_array = Arc::new(s) as ArrayRef;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "struct",
            struct_array.data_type().clone(),
            false,
        )]));

        let batch = RecordBatch::try_new(schema, vec![struct_array]).unwrap();

        let gen = IpcDataGenerator {};
        let mut dict_tracker = DictionaryTracker::new(false);
        gen.encoded_batch(&batch, &mut dict_tracker, &Default::default())
            .unwrap();

        // Dictionary with id 2 should have been written to the dict tracker
        assert!(dict_tracker.written.contains_key(&2));
    }

    fn write_union_file(options: IpcWriteOptions) {
        let schema = Schema::new(vec![Field::new(
            "union",
            DataType::Union(
                vec![
                    Field::new("a", DataType::Int32, false),
                    Field::new("c", DataType::Float64, false),
                ],
                vec![0, 1],
                UnionMode::Sparse,
            ),
            true,
        )]);
        let mut builder = UnionBuilder::with_capacity_sparse(5);
        builder.append::<Int32Type>("a", 1).unwrap();
        builder.append_null::<Int32Type>("a").unwrap();
        builder.append::<Float64Type>("c", 3.0).unwrap();
        builder.append_null::<Float64Type>("c").unwrap();
        builder.append::<Int32Type>("a", 4).unwrap();
        let union = builder.build().unwrap();

        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(union) as ArrayRef],
        )
        .unwrap();

        let mut file = tempfile::tempfile().unwrap();
        {
            let mut writer =
                FileWriter::try_new_with_options(&mut file, &schema, options).unwrap();

            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        file.rewind().unwrap();

        {
            let reader = FileReader::try_new(file, None).unwrap();
            reader.for_each(|maybe_batch| {
                maybe_batch
                    .unwrap()
                    .columns()
                    .iter()
                    .zip(batch.columns())
                    .for_each(|(a, b)| {
                        assert_eq!(a.data_type(), b.data_type());
                        assert_eq!(a.len(), b.len());
                        assert_eq!(a.null_count(), b.null_count());
                    });
            });
        }
    }

    #[test]
    fn test_write_union_file_v4_v5() {
        write_union_file(
            IpcWriteOptions::try_new(8, false, MetadataVersion::V4).unwrap(),
        );
        write_union_file(
            IpcWriteOptions::try_new(8, false, MetadataVersion::V5).unwrap(),
        );
    }

    fn serialize(record: &RecordBatch) -> Vec<u8> {
        let buffer: Vec<u8> = Vec::new();
        let mut stream_writer = StreamWriter::try_new(buffer, &record.schema()).unwrap();
        stream_writer.write(record).unwrap();
        stream_writer.finish().unwrap();
        stream_writer.into_inner().unwrap()
    }

    fn deserialize(bytes: Vec<u8>) -> RecordBatch {
        let mut stream_reader =
            crate::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
                .unwrap();
        stream_reader.next().unwrap().unwrap()
    }

    #[test]
    fn truncate_ipc_record_batch() {
        fn create_batch(rows: usize) -> RecordBatch {
            let schema = Schema::new(vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Utf8, false),
            ]);

            let a = Int32Array::from_iter_values(0..rows as i32);
            let b = StringArray::from_iter_values((0..rows).map(|i| i.to_string()));

            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a), Arc::new(b)])
                .unwrap()
        }

        let big_record_batch = create_batch(65536);

        let length = 5;
        let small_record_batch = create_batch(length);

        let offset = 2;
        let record_batch_slice = big_record_batch.slice(offset, length);
        assert!(
            serialize(&big_record_batch).len() > serialize(&small_record_batch).len()
        );
        assert_eq!(
            serialize(&small_record_batch).len(),
            serialize(&record_batch_slice).len()
        );

        assert_eq!(
            deserialize(serialize(&record_batch_slice)),
            record_batch_slice
        );
    }

    #[test]
    fn truncate_ipc_record_batch_with_nulls() {
        fn create_batch() -> RecordBatch {
            let schema = Schema::new(vec![
                Field::new("a", DataType::Int32, true),
                Field::new("b", DataType::Utf8, true),
            ]);

            let a = Int32Array::from(vec![Some(1), None, Some(1), None, Some(1)]);
            let b = StringArray::from(vec![None, Some("a"), Some("a"), None, Some("a")]);

            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a), Arc::new(b)])
                .unwrap()
        }

        let record_batch = create_batch();
        let record_batch_slice = record_batch.slice(1, 2);
        let deserialized_batch = deserialize(serialize(&record_batch_slice));

        assert!(serialize(&record_batch).len() > serialize(&record_batch_slice).len());

        assert!(deserialized_batch.column(0).is_null(0));
        assert!(deserialized_batch.column(0).is_valid(1));
        assert!(deserialized_batch.column(1).is_valid(0));
        assert!(deserialized_batch.column(1).is_valid(1));

        assert_eq!(record_batch_slice, deserialized_batch);
    }

    #[test]
    fn truncate_ipc_dictionary_array() {
        fn create_batch() -> RecordBatch {
            let values: StringArray = [Some("foo"), Some("bar"), Some("baz")]
                .into_iter()
                .collect();
            let keys: Int32Array =
                [Some(0), Some(2), None, Some(1)].into_iter().collect();

            let array = DictionaryArray::<Int32Type>::try_new(&keys, &values).unwrap();

            let schema =
                Schema::new(vec![Field::new("dict", array.data_type().clone(), true)]);

            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap()
        }

        let record_batch = create_batch();
        let record_batch_slice = record_batch.slice(1, 2);
        let deserialized_batch = deserialize(serialize(&record_batch_slice));

        assert!(serialize(&record_batch).len() > serialize(&record_batch_slice).len());

        assert!(deserialized_batch.column(0).is_valid(0));
        assert!(deserialized_batch.column(0).is_null(1));

        assert_eq!(record_batch_slice, deserialized_batch);
    }

    #[test]
    fn truncate_ipc_struct_array() {
        fn create_batch() -> RecordBatch {
            let strings: StringArray = [Some("foo"), None, Some("bar"), Some("baz")]
                .into_iter()
                .collect();
            let ints: Int32Array =
                [Some(0), Some(2), None, Some(1)].into_iter().collect();

            let struct_array = StructArray::from(vec![
                (
                    Field::new("s", DataType::Utf8, true),
                    Arc::new(strings) as ArrayRef,
                ),
                (
                    Field::new("c", DataType::Int32, true),
                    Arc::new(ints) as ArrayRef,
                ),
            ]);

            let schema = Schema::new(vec![Field::new(
                "struct_array",
                struct_array.data_type().clone(),
                true,
            )]);

            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(struct_array)]).unwrap()
        }

        let record_batch = create_batch();
        let record_batch_slice = record_batch.slice(1, 2);
        let deserialized_batch = deserialize(serialize(&record_batch_slice));

        assert!(serialize(&record_batch).len() > serialize(&record_batch_slice).len());

        let structs = deserialized_batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();

        assert!(structs.column(0).is_null(0));
        assert!(structs.column(0).is_valid(1));
        assert!(structs.column(1).is_valid(0));
        assert!(structs.column(1).is_null(1));
        assert_eq!(record_batch_slice, deserialized_batch);
    }

    #[test]
    fn truncate_ipc_string_array_with_all_empty_string() {
        fn create_batch() -> RecordBatch {
            let schema = Schema::new(vec![Field::new("a", DataType::Utf8, true)]);
            let a =
                StringArray::from(vec![Some(""), Some(""), Some(""), Some(""), Some("")]);
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)]).unwrap()
        }

        let record_batch = create_batch();
        let record_batch_slice = record_batch.slice(0, 1);
        let deserialized_batch = deserialize(serialize(&record_batch_slice));

        assert!(serialize(&record_batch).len() > serialize(&record_batch_slice).len());
        assert_eq!(record_batch_slice, deserialized_batch);
    }
}
