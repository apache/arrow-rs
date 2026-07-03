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
//! # Notes
//!
//! [`FileWriter`] and [`StreamWriter`] have similar interfaces,
//! however the [`FileWriter`] expects a reader that supports [`Seek`]ing
//!
//! [`Seek`]: std::io::Seek

use std::cmp::min;
use std::collections::HashMap;
use std::io::{BufWriter, Write};
use std::mem::size_of;
use std::sync::Arc;

use flatbuffers::FlatBufferBuilder;

use arrow_array::builder::BufferBuilder;
use arrow_array::cast::*;
use arrow_array::types::{Int16Type, Int32Type, Int64Type, RunEndIndexType};
use arrow_array::*;
use arrow_buffer::bit_util;
use arrow_buffer::{ArrowNativeType, Buffer, MutableBuffer, ToByteSlice};
use arrow_data::{ArrayData, ArrayDataBuilder, BufferSpec, layout};
use arrow_schema::*;

use crate::CONTINUATION_MARKER;
use crate::compression::CompressionCodec;
#[expect(deprecated)]
pub use crate::compression::{CompressionContext, IpcWriteContext};
use crate::convert::IpcSchemaEncoder;

/// IPC write options used to control the behaviour of the [`IpcDataGenerator`]
#[derive(Debug, Clone)]
pub struct IpcWriteOptions {
    /// Write padding after memory buffers to this multiple of bytes.
    /// Must be 8, 16, 32, or 64 - defaults to 64.
    alignment: u8,
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
    // Compression level
    batch_compression_level: Option<i32>,
    /// How to handle updating dictionaries in IPC messages
    dictionary_handling: DictionaryHandling,
}

/// A single buffer segment ready to be written to the output stream.
///
/// For the uncompressed path the original Arc-backed [`Buffer`] is stored
/// directly (zero copy). For the compressed path the compressed bytes are
/// owned by a scratch `Vec<u8>`.
enum EncodedBuffer {
    /// Uncompressed: Arc-backed reference to the original array buffer.
    Raw(Buffer),
    /// Compressed: owned scratch bytes produced by the codec.
    Compressed(Vec<u8>),
}

impl EncodedBuffer {
    fn as_slice(&self) -> &[u8] {
        match self {
            EncodedBuffer::Raw(b) => b.as_slice(),
            EncodedBuffer::Compressed(v) => v.as_slice(),
        }
    }

    fn len(&self) -> usize {
        match self {
            EncodedBuffer::Raw(b) => b.len(),
            EncodedBuffer::Compressed(v) => v.len(),
        }
    }
}
/// Accumulates the IPC metadata produced by [`write_array_data`].
///
/// `nodes` and `buffers` are serialised into the flatbuffer `RecordBatch` (or `DictionaryBatch`)
/// header. The companion [`IpcBodySink`] holds the actual encoded bytes.
#[derive(Default)]
struct IpcMetadataBuilder {
    nodes: Vec<crate::FieldNode>,
    buffers: Vec<crate::Buffer>,
}

/// Destination for the raw Arrow data bytes (the IPC message body) produced by [`write_array_data`].
///
/// The companion [`IpcMetadataBuilder`] accumulates the flatbuffer metadata
/// (offset + length of each buffer in the body); together they form a complete IPC message.
enum IpcBodySink<'a> {
    /// Serialize buffer bytes (with padding) into a contiguous byte vec.
    Write(&'a mut Vec<u8>),
    /// Accumulate pre-encoded buffer segments for deferred zero-copy streaming.
    Collect(&'a mut Vec<EncodedBuffer>),
}
impl<'a> IpcBodySink<'a> {
    /// Writes the encoded buffer to the sink.
    pub fn write(&mut self, pad_len: usize, buffer: EncodedBuffer) {
        match self {
            IpcBodySink::Write(vec) => {
                vec.extend_from_slice(buffer.as_slice());
                vec.extend_from_slice(&PADDING[..pad_len]);
            }
            IpcBodySink::Collect(vec) => {
                vec.push(buffer);
            }
        }
    }
}

/// Per-message sizes produced by [`IpcDataGenerator::write`].
///
/// [`FileWriter`] uses these to build the Block index entries required by the IPC footer for
/// random-access reads.
struct IpcWriteMetadata {
    /// Per-dictionary `(padded_header_len, body_len)` for each dictionary batch written
    /// before the record batch.
    dictionary_block_sizes: Vec<(usize, usize)>,
    /// Flatbuffer header size including continuation prefix and alignment padding.
    padded_header_len: usize,
    /// Total length of the record-batch body including trailing alignment padding.
    body_len: usize,
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

    /// Configures the compression level used when writing compressed IPC batches.
    ///
    /// Compression levels require metadata V5 or newer and are currently only
    /// supported for ZSTD compression.
    pub fn try_with_compression_level(
        mut self,
        batch_compression_level: Option<i32>,
    ) -> Result<Self, ArrowError> {
        self.batch_compression_level = batch_compression_level;

        if self.batch_compression_level.is_some()
            && self.metadata_version < crate::MetadataVersion::V5
        {
            return Err(ArrowError::InvalidArgumentError(
                "Compression only supported in metadata v5 and above".to_string(),
            ));
        }

        match (self.batch_compression_type, self.batch_compression_level) {
            (Some(crate::CompressionType::ZSTD), Some(level)) => {
                return self.check_zstd_level(level);
            }
            (Some(crate::CompressionType::LZ4_FRAME), Some(_)) => {
                return Err(ArrowError::InvalidArgumentError(
                    "LZ4 Frame compression does not support configurable compression levels"
                        .to_string(),
                ));
            }
            _ => {}
        }

        Ok(self)
    }

    #[cfg(not(feature = "zstd"))]
    fn check_zstd_level(self, _level: i32) -> Result<Self, ArrowError> {
        Err(ArrowError::InvalidArgumentError(
            "zstd IPC compression requires the zstd feature".to_string(),
        ))
    }

    #[cfg(feature = "zstd")]
    fn check_zstd_level(self, level: i32) -> Result<Self, ArrowError> {
        let range = zstd::compression_level_range();
        if !range.contains(&(level as zstd::zstd_safe::CompressionLevel)) {
            return Err(ArrowError::InvalidArgumentError(format!(
                "ZSTD compression level must be between {} and {}, got {}",
                range.start(),
                range.end(),
                level,
            )));
        }

        Ok(self)
    }

    /// Try to create IpcWriteOptions, checking for incompatible settings
    pub fn try_new(
        alignment: usize,
        write_legacy_ipc_format: bool,
        metadata_version: crate::MetadataVersion,
    ) -> Result<Self, ArrowError> {
        let is_alignment_valid =
            alignment == 8 || alignment == 16 || alignment == 32 || alignment == 64;
        if !is_alignment_valid {
            return Err(ArrowError::InvalidArgumentError(
                "Alignment should be 8, 16, 32, or 64.".to_string(),
            ));
        }
        let alignment: u8 = u8::try_from(alignment).expect("range already checked");
        match metadata_version {
            crate::MetadataVersion::V1
            | crate::MetadataVersion::V2
            | crate::MetadataVersion::V3 => Err(ArrowError::InvalidArgumentError(
                "Writing IPC metadata version 3 and lower not supported".to_string(),
            )),
            #[allow(deprecated)]
            crate::MetadataVersion::V4 => Ok(Self {
                alignment,
                write_legacy_ipc_format,
                metadata_version,
                batch_compression_type: None,
                batch_compression_level: None,
                dictionary_handling: DictionaryHandling::default(),
            }),
            crate::MetadataVersion::V5 => {
                if write_legacy_ipc_format {
                    Err(ArrowError::InvalidArgumentError(
                        "Legacy IPC format only supported on metadata version 4".to_string(),
                    ))
                } else {
                    Ok(Self {
                        alignment,
                        write_legacy_ipc_format,
                        metadata_version,
                        batch_compression_type: None,
                        batch_compression_level: None,
                        dictionary_handling: DictionaryHandling::default(),
                    })
                }
            }
            z => Err(ArrowError::InvalidArgumentError(format!(
                "Unsupported crate::MetadataVersion {z:?}"
            ))),
        }
    }

    /// Configure how dictionaries are handled in IPC messages
    pub fn with_dictionary_handling(mut self, dictionary_handling: DictionaryHandling) -> Self {
        self.dictionary_handling = dictionary_handling;
        self
    }
}

impl Default for IpcWriteOptions {
    fn default() -> Self {
        Self {
            alignment: 64,
            write_legacy_ipc_format: false,
            metadata_version: crate::MetadataVersion::V5,
            batch_compression_type: None,
            batch_compression_level: None,
            dictionary_handling: DictionaryHandling::default(),
        }
    }
}

#[derive(Debug, Default)]
/// Handles low level details of encoding [`Array`] and [`Schema`] into the
/// [Arrow IPC Format].
///
/// # Example
/// ```
/// # fn run() {
/// # use std::sync::Arc;
/// # use arrow_array::UInt64Array;
/// # use arrow_array::RecordBatch;
/// # use arrow_ipc::writer::{IpcWriteContext, DictionaryTracker, IpcDataGenerator, IpcWriteOptions};
///
/// // Create a record batch
/// let batch = RecordBatch::try_from_iter(vec![
///  ("col2", Arc::new(UInt64Array::from_iter([10, 23, 33])) as _)
/// ]).unwrap();
///
/// // Error of dictionary ids are replaced.
/// let error_on_replacement = true;
/// let options = IpcWriteOptions::default();
/// let mut dictionary_tracker = DictionaryTracker::new(error_on_replacement);
///
/// let mut ipc_write_context = IpcWriteContext::default();
///
/// // encode the batch into zero or more encoded dictionaries
/// // and the data for the actual array.
/// let data_gen = IpcDataGenerator::default();
/// let (encoded_dictionaries, encoded_message) = data_gen
///   .encode(&batch, &mut dictionary_tracker, &options, &mut ipc_write_context)
///   .unwrap();
/// # }
/// ```
///
/// [Arrow IPC Format]: https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc
pub struct IpcDataGenerator {}

impl IpcDataGenerator {
    /// Converts a schema to an IPC message along with `dictionary_tracker`
    /// and returns it encoded inside [EncodedData] as a flatbuffer.
    pub fn schema_to_bytes_with_dictionary_tracker(
        &self,
        schema: &Schema,
        dictionary_tracker: &mut DictionaryTracker,
        write_options: &IpcWriteOptions,
    ) -> EncodedData {
        let mut fbb = FlatBufferBuilder::new();
        let schema = {
            let fb = IpcSchemaEncoder::new()
                .with_dictionary_tracker(dictionary_tracker)
                .schema_to_fb_offset(&mut fbb, schema);
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

    fn _encode_dictionaries<I: Iterator<Item = i64>>(
        &self,
        column: &ArrayRef,
        encoded_dictionaries: &mut Vec<EncodedData>,
        dictionary_tracker: &mut DictionaryTracker,
        write_options: &IpcWriteOptions,
        dict_id: &mut I,
        ipc_write_context: &mut IpcWriteContext,
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
                        dict_id,
                        ipc_write_context,
                    )?;
                }
            }
            DataType::RunEndEncoded(_, values) => {
                let data = column.to_data();
                if data.child_data().len() != 2 {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "The run encoded array should have exactly two child arrays. Found {}",
                        data.child_data().len()
                    )));
                }
                // The run_ends array is not expected to be dictionary encoded. Hence encode dictionaries
                // only for values array.
                let values_array = make_array(data.child_data()[1].clone());
                self.encode_dictionaries(
                    values,
                    &values_array,
                    encoded_dictionaries,
                    dictionary_tracker,
                    write_options,
                    dict_id,
                    ipc_write_context,
                )?;
            }
            DataType::List(field) => {
                let list = as_list_array(column);
                self.encode_dictionaries(
                    field,
                    list.values(),
                    encoded_dictionaries,
                    dictionary_tracker,
                    write_options,
                    dict_id,
                    ipc_write_context,
                )?;
            }
            DataType::LargeList(field) => {
                let list = as_large_list_array(column);
                self.encode_dictionaries(
                    field,
                    list.values(),
                    encoded_dictionaries,
                    dictionary_tracker,
                    write_options,
                    dict_id,
                    ipc_write_context,
                )?;
            }
            DataType::ListView(field) => {
                let list = column.as_list_view::<i32>();
                self.encode_dictionaries(
                    field,
                    list.values(),
                    encoded_dictionaries,
                    dictionary_tracker,
                    write_options,
                    dict_id,
                    ipc_write_context,
                )?;
            }
            DataType::LargeListView(field) => {
                let list = column.as_list_view::<i64>();
                self.encode_dictionaries(
                    field,
                    list.values(),
                    encoded_dictionaries,
                    dictionary_tracker,
                    write_options,
                    dict_id,
                    ipc_write_context,
                )?;
            }
            DataType::FixedSizeList(field, _) => {
                let list = column
                    .as_any()
                    .downcast_ref::<FixedSizeListArray>()
                    .expect("Unable to downcast to fixed size list array");
                self.encode_dictionaries(
                    field,
                    list.values(),
                    encoded_dictionaries,
                    dictionary_tracker,
                    write_options,
                    dict_id,
                    ipc_write_context,
                )?;
            }
            DataType::Map(field, _) => {
                let map_array = as_map_array(column);

                let (keys, values) = match field.data_type() {
                    DataType::Struct(fields) if fields.len() == 2 => (&fields[0], &fields[1]),
                    _ => panic!("Incorrect field data type {:?}", field.data_type()),
                };

                // keys
                self.encode_dictionaries(
                    keys,
                    map_array.keys(),
                    encoded_dictionaries,
                    dictionary_tracker,
                    write_options,
                    dict_id,
                    ipc_write_context,
                )?;

                // values
                self.encode_dictionaries(
                    values,
                    map_array.values(),
                    encoded_dictionaries,
                    dictionary_tracker,
                    write_options,
                    dict_id,
                    ipc_write_context,
                )?;
            }
            DataType::Union(fields, _) => {
                let union = as_union_array(column);
                for (type_id, field) in fields.iter() {
                    let column = union.child(type_id);
                    self.encode_dictionaries(
                        field,
                        column,
                        encoded_dictionaries,
                        dictionary_tracker,
                        write_options,
                        dict_id,
                        ipc_write_context,
                    )?;
                }
            }
            _ => (),
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn encode_dictionaries<I: Iterator<Item = i64>>(
        &self,
        field: &Field,
        column: &ArrayRef,
        encoded_dictionaries: &mut Vec<EncodedData>,
        dictionary_tracker: &mut DictionaryTracker,
        write_options: &IpcWriteOptions,
        dict_id_seq: &mut I,
        ipc_write_context: &mut IpcWriteContext,
    ) -> Result<(), ArrowError> {
        match column.data_type() {
            DataType::Dictionary(_key_type, value_type) => {
                if matches!(value_type.as_ref(), DataType::Dictionary(_, _)) {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "Arrow IPC field metadata cannot encode direct dictionary-of-dictionary values for field {:?}",
                        field.name()
                    )));
                }

                let dict_data = column.to_data();
                let dict_values = &dict_data.child_data()[0];

                let values = make_array(dict_data.child_data()[0].clone());

                self._encode_dictionaries(
                    &values,
                    encoded_dictionaries,
                    dictionary_tracker,
                    write_options,
                    dict_id_seq,
                    ipc_write_context,
                )?;

                // It's important to only take the dict_id at this point, because the dict ID
                // sequence is assigned depth-first, so we need to first encode children and have
                // them take their assigned dict IDs before we take the dict ID for this field.
                let dict_id = dict_id_seq.next().ok_or_else(|| {
                    ArrowError::IpcError(format!(
                        "no dict id for field {:?}: field.data_type={:?}, column.data_type={:?}",
                        field.name(),
                        field.data_type(),
                        column.data_type()
                    ))
                })?;

                match dictionary_tracker.insert_column(
                    dict_id,
                    column,
                    write_options.dictionary_handling,
                )? {
                    DictionaryUpdate::None => {}
                    DictionaryUpdate::New | DictionaryUpdate::Replaced => {
                        encoded_dictionaries.push(self.dictionary_batch_to_bytes(
                            dict_id,
                            dict_values,
                            write_options,
                            false,
                            ipc_write_context,
                        )?);
                    }
                    DictionaryUpdate::Delta(data) => {
                        encoded_dictionaries.push(self.dictionary_batch_to_bytes(
                            dict_id,
                            &data,
                            write_options,
                            true,
                            ipc_write_context,
                        )?);
                    }
                }
            }
            _ => self._encode_dictionaries(
                column,
                encoded_dictionaries,
                dictionary_tracker,
                write_options,
                dict_id_seq,
                ipc_write_context,
            )?,
        }

        Ok(())
    }

    /// Encodes a batch to a number of [EncodedData] items (dictionary batches + the record batch).
    /// The [DictionaryTracker] keeps track of dictionaries with new `dict_id`s  (so they are only sent once)
    /// Make sure the [DictionaryTracker] is initialized at the start of the stream.
    pub fn encode(
        &self,
        batch: &RecordBatch,
        dictionary_tracker: &mut DictionaryTracker,
        write_options: &IpcWriteOptions,
        ipc_write_context: &mut IpcWriteContext,
    ) -> Result<(Vec<EncodedData>, EncodedData), ArrowError> {
        let encoded_dictionaries =
            self.encode_all_dicts(batch, dictionary_tracker, write_options, ipc_write_context)?;
        let mut arrow_data = Vec::new();
        let (ipc_message, _, tail_pad) = self.record_batch_to_bytes(
            batch,
            write_options,
            ipc_write_context,
            &mut IpcBodySink::Write(&mut arrow_data),
        )?;
        arrow_data.extend_from_slice(&PADDING[..tail_pad]);
        Ok((
            encoded_dictionaries,
            EncodedData {
                ipc_message,
                arrow_data,
            },
        ))
    }

    /// Encode dictionary batches for all columns in `batch`.
    fn encode_all_dicts(
        &self,
        batch: &RecordBatch,
        dictionary_tracker: &mut DictionaryTracker,
        write_options: &IpcWriteOptions,
        ipc_write_context: &mut IpcWriteContext,
    ) -> Result<Vec<EncodedData>, ArrowError> {
        let schema = batch.schema();
        let mut encoded_dictionaries = Vec::with_capacity(schema.flattened_fields().len());
        let mut dict_id = dictionary_tracker.dict_ids.clone().into_iter();
        for (i, field) in schema.fields().iter().enumerate() {
            self.encode_dictionaries(
                field,
                batch.column(i),
                &mut encoded_dictionaries,
                dictionary_tracker,
                write_options,
                &mut dict_id,
                ipc_write_context,
            )?;
        }
        Ok(encoded_dictionaries)
    }

    /// Write dictionary batches and the record batch directly to `writer`, skipping the
    /// intermediate body `Vec<u8>` allocations
    /// Returns [`IpcWriteMetadata`] with the sizes needed to build footer blocks.
    fn write<W: Write>(
        &self,
        batch: &RecordBatch,
        dictionary_tracker: &mut DictionaryTracker,
        write_options: &IpcWriteOptions,
        ipc_write_context: &mut IpcWriteContext,
        writer: &mut W,
    ) -> Result<IpcWriteMetadata, ArrowError> {
        let encoded_dictionaries =
            self.encode_all_dicts(batch, dictionary_tracker, write_options, ipc_write_context)?;

        let mut dictionary_block_sizes = Vec::with_capacity(encoded_dictionaries.len());
        for dict in encoded_dictionaries {
            dictionary_block_sizes.push(write_message(&mut *writer, dict, write_options)?);
        }

        let capacity = batch
            .columns()
            .iter()
            .map(|a| estimate_encoded_buffer_count(a.data_type()))
            .sum();
        let mut encoded_buffers: Vec<EncodedBuffer> = Vec::with_capacity(capacity);
        let (ipc_message, body_len, tail_pad) = self.record_batch_to_bytes(
            batch,
            write_options,
            ipc_write_context,
            &mut IpcBodySink::Collect(&mut encoded_buffers),
        )?;

        let alignment = write_options.alignment;
        let a = usize::from(alignment - 1);
        let prefix_size = if write_options.write_legacy_ipc_format {
            4
        } else {
            8
        };
        let aligned_size = (ipc_message.len() + prefix_size + a) & !a;
        write_continuation(
            &mut *writer,
            write_options,
            (aligned_size - prefix_size) as i32,
        )?;
        writer.write_all(&ipc_message)?;
        writer.write_all(&PADDING[..aligned_size - ipc_message.len() - prefix_size])?;
        for enc in &encoded_buffers {
            writer.write_all(enc.as_slice())?;
            writer.write_all(&PADDING[..pad_to_alignment(alignment, enc.len())])?;
        }
        writer.write_all(&PADDING[..tail_pad])?;

        Ok(IpcWriteMetadata {
            dictionary_block_sizes,
            padded_header_len: aligned_size,
            body_len,
        })
    }

    /// Encodes a batch to a number of [EncodedData] items (dictionary batches + the record batch).
    /// The [DictionaryTracker] keeps track of dictionaries with new `dict_id`s  (so they are only sent once)
    /// Make sure the [DictionaryTracker] is initialized at the start of the stream.
    #[deprecated(since = "57.0.0", note = "Use `encode` instead")]
    pub fn encoded_batch(
        &self,
        batch: &RecordBatch,
        dictionary_tracker: &mut DictionaryTracker,
        write_options: &IpcWriteOptions,
    ) -> Result<(Vec<EncodedData>, EncodedData), ArrowError> {
        self.encode(
            batch,
            dictionary_tracker,
            write_options,
            &mut Default::default(),
        )
    }

    /// Encodes a `RecordBatch` into a flatbuffer IPC message and fills `sink` with the
    /// serialised buffer data.
    ///
    /// Returns `(ipc_message, body_len, tail_pad)`: the flatbuffer header bytes, the
    /// total body length including trailing padding, and the trailing alignment padding byte count.
    fn record_batch_to_bytes(
        &self,
        batch: &RecordBatch,
        write_options: &IpcWriteOptions,
        ipc_write_context: &mut IpcWriteContext,
        sink: &mut IpcBodySink<'_>,
    ) -> Result<(Vec<u8>, usize, usize), ArrowError> {
        let batch_compression_type = write_options.batch_compression_type;

        let compression = batch_compression_type.map(|batch_compression_type| {
            let fbb = ipc_write_context.mut_fbb();
            let mut c = crate::BodyCompressionBuilder::new(fbb);
            c.add_method(crate::BodyCompressionMethod::BUFFER);
            c.add_codec(batch_compression_type);
            c.finish()
        });

        let batch_compression_level = write_options.batch_compression_level;
        let compression_codec: Option<CompressionCodec> = batch_compression_type
            .map(|compression_type| match batch_compression_level {
                Some(level) => {
                    CompressionCodec::try_new_with_compression_level(compression_type, level)
                }
                None => compression_type.try_into(),
            })
            .transpose()?;

        let alignment = write_options.alignment;
        let mut variadic_buffer_counts = vec![];
        let mut meta = IpcMetadataBuilder::default();
        let mut offset = 0i64;

        for array in batch.columns() {
            let array_data = array.to_data();
            offset = write_array_data(
                &array_data,
                &mut meta,
                sink,
                offset,
                compression_codec,
                ipc_write_context,
                write_options,
            )?;
            append_variadic_buffer_counts(&mut variadic_buffer_counts, &array_data);
        }

        let tail_pad = pad_to_alignment(alignment, offset as usize);
        let body_len = offset as usize + tail_pad;

        let fbb = ipc_write_context.mut_fbb();
        let buffers = fbb.create_vector(&meta.buffers);
        let nodes = fbb.create_vector(&meta.nodes);
        let variadic_buffer = if variadic_buffer_counts.is_empty() {
            None
        } else {
            Some(fbb.create_vector(&variadic_buffer_counts))
        };

        let root = {
            let mut batch_builder = crate::RecordBatchBuilder::new(fbb);
            batch_builder.add_length(batch.num_rows() as i64);
            batch_builder.add_nodes(nodes);
            batch_builder.add_buffers(buffers);
            if let Some(c) = compression {
                batch_builder.add_compression(c);
            }
            if let Some(v) = variadic_buffer {
                batch_builder.add_variadicBufferCounts(v);
            }
            batch_builder.finish().as_union_value()
        };
        let mut message = crate::MessageBuilder::new(fbb);
        message.add_version(write_options.metadata_version);
        message.add_header_type(crate::MessageHeader::RecordBatch);
        message.add_bodyLength(body_len as i64);
        message.add_header(root);
        let root = message.finish();
        fbb.finish(root, None);

        let ipc_message = fbb.finished_data().to_vec();
        fbb.reset();
        Ok((ipc_message, body_len, tail_pad))
    }

    /// Write dictionary values into two sets of bytes, one for the header (crate::Message) and the
    /// other for the data
    fn dictionary_batch_to_bytes(
        &self,
        dict_id: i64,
        array_data: &ArrayData,
        write_options: &IpcWriteOptions,
        is_delta: bool,
        ipc_write_context: &mut IpcWriteContext,
    ) -> Result<EncodedData, ArrowError> {
        let mut arrow_data: Vec<u8> = vec![];

        // get the type of compression
        let batch_compression_type = write_options.batch_compression_type;

        let compression = batch_compression_type.map(|batch_compression_type| {
            let fbb = ipc_write_context.mut_fbb();
            let mut c = crate::BodyCompressionBuilder::new(fbb);
            c.add_method(crate::BodyCompressionMethod::BUFFER);
            c.add_codec(batch_compression_type);
            c.finish()
        });

        let batch_compression_level = write_options.batch_compression_level;
        let compression_codec: Option<CompressionCodec> = batch_compression_type
            .map(|batch_compression_type| match batch_compression_level {
                Some(level) => {
                    CompressionCodec::try_new_with_compression_level(batch_compression_type, level)
                }
                None => batch_compression_type.try_into(),
            })
            .transpose()?;

        let alignment = write_options.alignment;
        let mut meta = IpcMetadataBuilder::default();
        let mut sink = IpcBodySink::Write(&mut arrow_data);
        let offset = write_array_data(
            array_data,
            &mut meta,
            &mut sink,
            0,
            compression_codec,
            ipc_write_context,
            write_options,
        )?;

        let mut variadic_buffer_counts = vec![];
        append_variadic_buffer_counts(&mut variadic_buffer_counts, array_data);

        // pad the tail of body data
        let tail_pad = pad_to_alignment(alignment, offset as usize);
        let body_len = offset as usize + tail_pad;
        arrow_data.extend_from_slice(&PADDING[..tail_pad]);

        let fbb = ipc_write_context.mut_fbb();
        let buffers = fbb.create_vector(&meta.buffers);
        let nodes = fbb.create_vector(&meta.nodes);
        let variadic_buffer = if variadic_buffer_counts.is_empty() {
            None
        } else {
            Some(fbb.create_vector(&variadic_buffer_counts))
        };

        let root = {
            let mut batch_builder = crate::RecordBatchBuilder::new(fbb);
            batch_builder.add_length(array_data.len() as i64);
            batch_builder.add_nodes(nodes);
            batch_builder.add_buffers(buffers);
            if let Some(c) = compression {
                batch_builder.add_compression(c);
            }
            if let Some(v) = variadic_buffer {
                batch_builder.add_variadicBufferCounts(v);
            }
            batch_builder.finish()
        };

        let root = {
            let mut batch_builder = crate::DictionaryBatchBuilder::new(fbb);
            batch_builder.add_id(dict_id);
            batch_builder.add_data(root);
            batch_builder.add_isDelta(is_delta);
            batch_builder.finish().as_union_value()
        };

        let root = {
            let mut message_builder = crate::MessageBuilder::new(fbb);
            message_builder.add_version(write_options.metadata_version);
            message_builder.add_header_type(crate::MessageHeader::DictionaryBatch);
            message_builder.add_bodyLength(body_len as i64);
            message_builder.add_header(root);
            message_builder.finish()
        };

        fbb.finish(root, None);
        let ipc_message = fbb.finished_data().to_vec();
        fbb.reset();

        Ok(EncodedData {
            ipc_message,
            arrow_data,
        })
    }
}

fn ensure_supported_ipc_schema(schema: &Schema) -> Result<(), ArrowError> {
    schema
        .fields()
        .iter()
        .try_for_each(|field| ensure_supported_ipc_data_type(field.name(), field.data_type()))
}

fn ensure_supported_ipc_data_type(
    field_name: &str,
    data_type: &DataType,
) -> Result<(), ArrowError> {
    match data_type {
        DataType::Dictionary(_, value_type)
            if matches!(value_type.as_ref(), DataType::Dictionary(_, _)) =>
        {
            Err(ArrowError::InvalidArgumentError(format!(
                "Arrow IPC field metadata cannot encode direct dictionary-of-dictionary values for field {field_name:?}"
            )))
        }
        DataType::Dictionary(_, value_type) => {
            ensure_supported_ipc_data_type(field_name, value_type)
        }
        DataType::Struct(fields) => fields
            .iter()
            .try_for_each(|field| ensure_supported_ipc_data_type(field.name(), field.data_type())),
        DataType::RunEndEncoded(_, field)
        | DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => {
            ensure_supported_ipc_data_type(field.name(), field.data_type())
        }
        DataType::Union(fields, _) => fields.iter().try_for_each(|(_, field)| {
            ensure_supported_ipc_data_type(field.name(), field.data_type())
        }),
        _ => Ok(()),
    }
}

fn append_variadic_buffer_counts(counts: &mut Vec<i64>, array: &ArrayData) {
    match array.data_type() {
        DataType::BinaryView | DataType::Utf8View => {
            // The spec documents the counts only includes the variadic buffers, not the view/null buffers.
            // https://arrow.apache.org/docs/format/Columnar.html#variadic-buffers
            counts.push(array.buffers().len() as i64 - 1);
        }
        DataType::Dictionary(_, _) => {
            // Do nothing
            // Dictionary types are handled in `encode_dictionaries`.
        }
        _ => {
            for child in array.child_data() {
                append_variadic_buffer_counts(counts, child)
            }
        }
    }
}

pub(crate) fn unslice_run_array(arr: ArrayData) -> Result<ArrayData, ArrowError> {
    match arr.data_type() {
        DataType::RunEndEncoded(k, _) => match k.data_type() {
            DataType::Int16 => {
                Ok(into_zero_offset_run_array(RunArray::<Int16Type>::from(arr))?.into_data())
            }
            DataType::Int32 => {
                Ok(into_zero_offset_run_array(RunArray::<Int32Type>::from(arr))?.into_data())
            }
            DataType::Int64 => {
                Ok(into_zero_offset_run_array(RunArray::<Int64Type>::from(arr))?.into_data())
            }
            d => unreachable!("Unexpected data type {d}"),
        },
        d => Err(ArrowError::InvalidArgumentError(format!(
            "The given array is not a run array. Data type of given array: {d}"
        ))),
    }
}

// Returns a `RunArray` with zero offset and length matching the last value
// in run_ends array.
fn into_zero_offset_run_array<R: RunEndIndexType>(
    run_array: RunArray<R>,
) -> Result<RunArray<R>, ArrowError> {
    let run_ends = run_array.run_ends();
    if run_ends.offset() == 0 && run_ends.max_value() == run_ends.len() {
        return Ok(run_array);
    }

    // The physical index of original run_ends array from which the `ArrayData`is sliced.
    let start_physical_index = run_ends.get_start_physical_index();

    // The physical index of original run_ends array until which the `ArrayData`is sliced.
    let end_physical_index = run_ends.get_end_physical_index();

    let physical_length = end_physical_index - start_physical_index + 1;

    // build new run_ends array by subtracting offset from run ends.
    let offset = R::Native::usize_as(run_ends.offset());
    let mut builder = BufferBuilder::<R::Native>::new(physical_length);
    for run_end_value in &run_ends.values()[start_physical_index..end_physical_index] {
        builder.append(run_end_value.sub_wrapping(offset));
    }
    builder.append(R::Native::from_usize(run_array.len()).unwrap());
    let new_run_ends = unsafe {
        // Safety:
        // The function builds a valid run_ends array and hence need not be validated.
        ArrayDataBuilder::new(R::DATA_TYPE)
            .len(physical_length)
            .add_buffer(builder.finish())
            .build_unchecked()
    };

    // build new values by slicing physical indices.
    let new_values = run_array
        .values()
        .slice(start_physical_index, physical_length)
        .into_data();

    let builder = ArrayDataBuilder::new(run_array.data_type().clone())
        .len(run_array.len())
        .add_child_data(new_run_ends)
        .add_child_data(new_values);
    let array_data = unsafe {
        // Safety:
        //  This function builds a valid run array and hence can skip validation.
        builder.build_unchecked()
    };
    Ok(array_data.into())
}

/// Controls how dictionaries are handled in Arrow IPC messages
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DictionaryHandling {
    /// Send the entire dictionary every time it is encountered (default)
    #[default]
    Resend,
    /// Send only new dictionary values since the last batch (delta encoding)
    ///
    /// When a dictionary is first encountered, the entire dictionary is sent.
    /// For subsequent batches, only values that are new (not previously sent)
    /// are transmitted with the `isDelta` flag set to true.
    Delta,
}

/// Describes what kind of update took place after a call to [`DictionaryTracker::insert`].
#[derive(Debug, Clone)]
pub enum DictionaryUpdate {
    /// No dictionary was written, the dictionary was identical to what was already
    /// in the tracker.
    None,
    /// No dictionary was present in the tracker
    New,
    /// Dictionary was replaced with the new data
    Replaced,
    /// Dictionary was updated, ArrayData is the delta between old and new
    Delta(ArrayData),
}

/// Keeps track of dictionaries that have been written, to avoid emitting the same dictionary
/// multiple times.
///
/// Can optionally error if an update to an existing dictionary is attempted, which
/// isn't allowed in the `FileWriter`.
#[derive(Debug)]
pub struct DictionaryTracker {
    // NOTE: When adding fields, update the clear() method accordingly.
    written: HashMap<i64, ArrayData>,
    dict_ids: Vec<i64>,
    error_on_replacement: bool,
}

impl DictionaryTracker {
    /// Create a new [`DictionaryTracker`].
    ///
    /// If `error_on_replacement`
    /// is true, an error will be generated if an update to an
    /// existing dictionary is attempted.
    pub fn new(error_on_replacement: bool) -> Self {
        #[allow(deprecated)]
        Self {
            written: HashMap::new(),
            dict_ids: Vec::new(),
            error_on_replacement,
        }
    }

    /// Record and return the next dictionary ID.
    pub fn next_dict_id(&mut self) -> i64 {
        let next = self
            .dict_ids
            .last()
            .copied()
            .map(|i| i + 1)
            .unwrap_or_default();

        self.dict_ids.push(next);
        next
    }

    /// Return the sequence of dictionary IDs in the order they should be observed while
    /// traversing the schema
    pub fn dict_id(&mut self) -> &[i64] {
        &self.dict_ids
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
    #[deprecated(since = "56.1.0", note = "Use `insert_column` instead")]
    pub fn insert(&mut self, dict_id: i64, column: &ArrayRef) -> Result<bool, ArrowError> {
        let dict_data = column.to_data();
        let dict_values = &dict_data.child_data()[0];

        // If a dictionary with this id was already emitted, check if it was the same.
        if let Some(last) = self.written.get(&dict_id) {
            if ArrayData::ptr_eq(&last.child_data()[0], dict_values) {
                // Same dictionary values => no need to emit it again
                return Ok(false);
            }
            if self.error_on_replacement {
                // If error on replacement perform a logical comparison
                if last.child_data()[0] == *dict_values {
                    // Same dictionary values => no need to emit it again
                    return Ok(false);
                }
                return Err(ArrowError::InvalidArgumentError(
                    "Dictionary replacement detected when writing IPC file format. \
                     Arrow IPC files only support a single dictionary for a given field \
                     across all batches."
                        .to_string(),
                ));
            }
        }

        self.written.insert(dict_id, dict_data);
        Ok(true)
    }

    /// Keep track of the dictionary with the given ID and values. The return
    /// value indicates what, if any, update to the internal map took place
    /// and how it should be interpreted based on the `dict_handling` parameter.
    ///
    /// # Returns
    ///
    /// * `Ok(Dictionary::New)` - If the dictionary was not previously written
    /// * `Ok(Dictionary::Replaced)` - If the dictionary was previously written
    ///   with completely different data, or if the data is a delta of the existing,
    ///   but with `dict_handling` set to `DictionaryHandling::Resend`
    /// * `Ok(Dictionary::Delta)` - If the dictionary was previously written, but
    ///   the new data is a delta of the old and the `dict_handling` is set to
    ///   `DictionaryHandling::Delta`
    /// * `Err(e)` - If the dictionary was previously written with different data,
    ///   and `error_on_replacement` is set to `true`.
    pub fn insert_column(
        &mut self,
        dict_id: i64,
        column: &ArrayRef,
        dict_handling: DictionaryHandling,
    ) -> Result<DictionaryUpdate, ArrowError> {
        let new_data = column.to_data();
        let new_values = &new_data.child_data()[0];

        // If there is no existing dictionary with this ID, we always insert
        let Some(old) = self.written.get(&dict_id) else {
            self.written.insert(dict_id, new_data);
            return Ok(DictionaryUpdate::New);
        };

        // Fast path - If the array data points to the same buffer as the
        // existing then they're the same.
        let old_values = &old.child_data()[0];
        if ArrayData::ptr_eq(old_values, new_values) {
            return Ok(DictionaryUpdate::None);
        }

        // Slow path - Compare the dictionaries value by value
        let comparison = compare_dictionaries(old_values, new_values);
        if matches!(comparison, DictionaryComparison::Equal) {
            return Ok(DictionaryUpdate::None);
        }

        const REPLACEMENT_ERROR: &str = "Dictionary replacement detected when writing IPC file format. \
                 Arrow IPC files only support a single dictionary for a given field \
                 across all batches.";

        match comparison {
            DictionaryComparison::NotEqual => {
                if self.error_on_replacement {
                    return Err(ArrowError::InvalidArgumentError(
                        REPLACEMENT_ERROR.to_string(),
                    ));
                }

                self.written.insert(dict_id, new_data);
                Ok(DictionaryUpdate::Replaced)
            }
            DictionaryComparison::Delta => match dict_handling {
                DictionaryHandling::Resend => {
                    if self.error_on_replacement {
                        return Err(ArrowError::InvalidArgumentError(
                            REPLACEMENT_ERROR.to_string(),
                        ));
                    }

                    self.written.insert(dict_id, new_data);
                    Ok(DictionaryUpdate::Replaced)
                }
                DictionaryHandling::Delta => {
                    let delta =
                        new_values.slice(old_values.len(), new_values.len() - old_values.len());
                    self.written.insert(dict_id, new_data);
                    Ok(DictionaryUpdate::Delta(delta))
                }
            },
            DictionaryComparison::Equal => unreachable!("Already checked equal case"),
        }
    }

    /// Clears the state of the dictionary tracker.
    ///
    /// This allows the dictionary tracker to be reused for a new IPC stream while avoiding the
    /// allocation cost of creating a new instance. This method should not be called if
    /// the dictionary tracker will be used to continue writing to an existing IPC stream.
    pub fn clear(&mut self) {
        self.dict_ids.clear();
        self.written.clear();
    }
}

/// Describes how two dictionary arrays compare to each other.
#[derive(Debug, Clone)]
enum DictionaryComparison {
    /// Neither a delta, nor an exact match
    NotEqual,
    /// Exact element-wise match
    Equal,
    /// The two arrays are dictionary deltas of each other, meaning the first
    /// is a prefix of the second.
    Delta,
}

// Compares two dictionaries and returns a [`DictionaryComparison`].
fn compare_dictionaries(old: &ArrayData, new: &ArrayData) -> DictionaryComparison {
    // Check for exact match
    let existing_len = old.len();
    let new_len = new.len();
    if existing_len == new_len {
        if *old == *new {
            return DictionaryComparison::Equal;
        } else {
            return DictionaryComparison::NotEqual;
        }
    }

    // Can't be a delta if the new is shorter than the existing
    if new_len < existing_len {
        return DictionaryComparison::NotEqual;
    }

    // Check for delta
    if new.slice(0, existing_len) == *old {
        return DictionaryComparison::Delta;
    }

    DictionaryComparison::NotEqual
}

/// Arrow File Writer
///
/// Writes Arrow [`RecordBatch`]es in the [IPC File Format].
///
/// # See Also
///
/// * [`StreamWriter`] for writing IPC Streams
///
/// # Example
/// ```
/// # use arrow_array::record_batch;
/// # use arrow_ipc::writer::FileWriter;
/// # let mut file = vec![]; // mimic a file for the example
/// let batch = record_batch!(("a", Int32, [1, 2, 3])).unwrap();
/// // create a new writer, the schema must be known in advance
/// let mut writer = FileWriter::try_new(&mut file, &batch.schema()).unwrap();
/// // write each batch to the underlying writer
/// writer.write(&batch).unwrap();
/// // When all batches are written, call finish to flush all buffers
/// writer.finish().unwrap();
/// ```
/// [IPC File Format]: https://arrow.apache.org/docs/format/Columnar.html#ipc-file-format
pub struct FileWriter<W> {
    /// The object to write to
    writer: W,
    /// IPC write options
    write_options: IpcWriteOptions,
    /// A reference to the schema, used in validating record batches
    schema: SchemaRef,
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
    /// User level customized metadata
    custom_metadata: HashMap<String, String>,

    data_gen: IpcDataGenerator,

    ipc_write_context: IpcWriteContext,
}

impl<W: Write> FileWriter<BufWriter<W>> {
    /// Try to create a new file writer with the writer wrapped in a BufWriter.
    ///
    /// See [`FileWriter::try_new`] for an unbuffered version.
    pub fn try_new_buffered(writer: W, schema: &Schema) -> Result<Self, ArrowError> {
        Self::try_new(BufWriter::new(writer), schema)
    }
}

impl<W: Write> FileWriter<W> {
    /// Try to create a new writer, with the schema written as part of the header
    ///
    /// Note the created writer is not buffered. See [`FileWriter::try_new_buffered`] for details.
    ///
    /// # Errors
    ///
    /// An ['Err'](Result::Err) may be returned if writing the header to the writer fails.
    pub fn try_new(writer: W, schema: &Schema) -> Result<Self, ArrowError> {
        let write_options = IpcWriteOptions::default();
        Self::try_new_with_options(writer, schema, write_options)
    }

    /// Try to create a new writer with IpcWriteOptions
    ///
    /// Note the created writer is not buffered. See [`FileWriter::try_new_buffered`] for details.
    ///
    /// # Errors
    ///
    /// An ['Err'](Result::Err) may be returned if writing the header to the writer fails.
    pub fn try_new_with_options(
        mut writer: W,
        schema: &Schema,
        write_options: IpcWriteOptions,
    ) -> Result<Self, ArrowError> {
        ensure_supported_ipc_schema(schema)?;

        let data_gen = IpcDataGenerator::default();
        // write magic to header aligned on alignment boundary
        let pad_len = pad_to_alignment(write_options.alignment, super::ARROW_MAGIC.len());
        let header_size = super::ARROW_MAGIC.len() + pad_len;
        writer.write_all(&super::ARROW_MAGIC)?;
        writer.write_all(&PADDING[..pad_len])?;
        // write the schema, set the written bytes to the schema + header
        let mut dictionary_tracker = DictionaryTracker::new(true);
        let encoded_message = data_gen.schema_to_bytes_with_dictionary_tracker(
            schema,
            &mut dictionary_tracker,
            &write_options,
        );
        let (meta, data) = write_message(&mut writer, encoded_message, &write_options)?;
        Ok(Self {
            writer,
            write_options,
            schema: Arc::new(schema.clone()),
            block_offsets: meta + data + header_size,
            dictionary_blocks: vec![],
            record_blocks: vec![],
            finished: false,
            dictionary_tracker,
            custom_metadata: HashMap::new(),
            data_gen,
            ipc_write_context: IpcWriteContext::default(),
        })
    }

    /// Adds a key-value pair to the [FileWriter]'s custom metadata
    pub fn write_metadata(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.custom_metadata.insert(key.into(), value.into());
    }

    /// Write a record batch to the file
    pub fn write(&mut self, batch: &RecordBatch) -> Result<(), ArrowError> {
        if self.finished {
            return Err(ArrowError::IpcError(
                "Cannot write record batch to file writer as it is closed".to_string(),
            ));
        }

        let meta = self.data_gen.write(
            batch,
            &mut self.dictionary_tracker,
            &self.write_options,
            &mut self.ipc_write_context,
            &mut self.writer,
        )?;

        for (header_len, body_len) in meta.dictionary_block_sizes {
            let block = crate::Block::new(
                self.block_offsets as i64,
                header_len as i32,
                body_len as i64,
            );
            self.dictionary_blocks.push(block);
            self.block_offsets += header_len + body_len;
        }

        // add a record block for the footer
        let block = crate::Block::new(
            self.block_offsets as i64,
            meta.padded_header_len as i32,
            meta.body_len as i64,
        );
        self.record_blocks.push(block);
        self.block_offsets += meta.padded_header_len + meta.body_len;
        Ok(())
    }

    /// Write footer and closing tag, then mark the writer as done
    pub fn finish(&mut self) -> Result<(), ArrowError> {
        if self.finished {
            return Err(ArrowError::IpcError(
                "Cannot write footer to file writer as it is closed".to_string(),
            ));
        }

        // write EOS
        write_continuation(&mut self.writer, &self.write_options, 0)?;

        let mut fbb = FlatBufferBuilder::new();
        let dictionaries = fbb.create_vector(&self.dictionary_blocks);
        let record_batches = fbb.create_vector(&self.record_blocks);

        // dictionaries are already written, so we can reset dictionary tracker to reuse for schema
        self.dictionary_tracker.clear();
        let schema = IpcSchemaEncoder::new()
            .with_dictionary_tracker(&mut self.dictionary_tracker)
            .schema_to_fb_offset(&mut fbb, &self.schema);
        let fb_custom_metadata = (!self.custom_metadata.is_empty())
            .then(|| crate::convert::metadata_to_fb(&mut fbb, &self.custom_metadata));

        let root = {
            let mut footer_builder = crate::FooterBuilder::new(&mut fbb);
            footer_builder.add_version(self.write_options.metadata_version);
            footer_builder.add_schema(schema);
            footer_builder.add_dictionaries(dictionaries);
            footer_builder.add_recordBatches(record_batches);
            if let Some(fb_custom_metadata) = fb_custom_metadata {
                footer_builder.add_custom_metadata(fb_custom_metadata);
            }
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

    /// Returns the arrow [`SchemaRef`] for this arrow file.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Gets a reference to the underlying writer.
    pub fn get_ref(&self) -> &W {
        &self.writer
    }

    /// Gets a mutable reference to the underlying writer.
    ///
    /// It is inadvisable to directly write to the underlying writer.
    pub fn get_mut(&mut self) -> &mut W {
        &mut self.writer
    }

    /// Flush the underlying writer.
    ///
    /// Both the BufWriter and the underlying writer are flushed.
    pub fn flush(&mut self) -> Result<(), ArrowError> {
        self.writer.flush()?;
        Ok(())
    }

    /// Unwraps the underlying writer.
    ///
    /// The writer is flushed and the FileWriter is finished before returning.
    ///
    /// # Errors
    ///
    /// An ['Err'](Result::Err) may be returned if an error occurs while finishing the StreamWriter
    /// or while flushing the writer.
    pub fn into_inner(mut self) -> Result<W, ArrowError> {
        if !self.finished {
            // `finish` flushes the writer.
            self.finish()?;
        }
        Ok(self.writer)
    }
}

impl<W: Write> RecordBatchWriter for FileWriter<W> {
    fn write(&mut self, batch: &RecordBatch) -> Result<(), ArrowError> {
        self.write(batch)
    }

    fn close(mut self) -> Result<(), ArrowError> {
        self.finish()
    }
}

/// Arrow Stream Writer
///
/// Writes Arrow [`RecordBatch`]es to bytes using the [IPC Streaming Format].
///
/// # See Also
///
/// * [`FileWriter`] for writing IPC Files
///
/// # Example - Basic usage
/// ```
/// # use arrow_array::record_batch;
/// # use arrow_ipc::writer::StreamWriter;
/// # let mut stream = vec![]; // mimic a stream for the example
/// let batch = record_batch!(("a", Int32, [1, 2, 3])).unwrap();
/// // create a new writer, the schema must be known in advance
/// let mut writer = StreamWriter::try_new(&mut stream, &batch.schema()).unwrap();
/// // write each batch to the underlying stream
/// writer.write(&batch).unwrap();
/// // When all batches are written, call finish to flush all buffers
/// writer.finish().unwrap();
/// ```
/// # Example - Efficient delta dictionaries
/// ```
/// # use arrow_array::record_batch;
/// # use arrow_ipc::writer::{StreamWriter, IpcWriteOptions};
/// # use arrow_ipc::writer::DictionaryHandling;
/// # use arrow_schema::{DataType, Field, Schema, SchemaRef};
/// # use arrow_array::{
/// #    builder::StringDictionaryBuilder, types::Int32Type, Array, ArrayRef, DictionaryArray,
/// #    RecordBatch, StringArray,
/// # };
/// # use std::sync::Arc;
///
/// let schema = Arc::new(Schema::new(vec![Field::new(
///    "col1",
///    DataType::Dictionary(Box::from(DataType::Int32), Box::from(DataType::Utf8)),
///    true,
/// )]));
///
/// let mut builder = StringDictionaryBuilder::<arrow_array::types::Int32Type>::new();
///
/// // `finish_preserve_values` will keep the dictionary values along with their
/// // key assignments so that they can be re-used in the next batch.
/// builder.append("a").unwrap();
/// builder.append("b").unwrap();
/// let array1 = builder.finish_preserve_values();
/// let batch1 = RecordBatch::try_new(schema.clone(), vec![Arc::new(array1) as ArrayRef]).unwrap();
///
/// // In this batch, 'a' will have the same dictionary key as 'a' in the previous batch,
/// // and 'd' will take the next available key.
/// builder.append("a").unwrap();
/// builder.append("d").unwrap();
/// let array2 = builder.finish_preserve_values();
/// let batch2 = RecordBatch::try_new(schema.clone(), vec![Arc::new(array2) as ArrayRef]).unwrap();
///
/// let mut stream = vec![];
/// // You must set `.with_dictionary_handling(DictionaryHandling::Delta)` to
/// // enable delta dictionaries in the writer
/// let options = IpcWriteOptions::default().with_dictionary_handling(DictionaryHandling::Delta);
/// let mut writer = StreamWriter::try_new_with_options(&mut stream, &schema, options).unwrap();
///
/// // When writing the first batch, a dictionary message with 'a' and 'b' will be written
/// // prior to the record batch.
/// writer.write(&batch1).unwrap();
/// // With the second batch only a delta dictionary with 'd' will be written
/// // prior to the record batch. This is only possible with `finish_preserve_values`.
/// // Without it, 'a' and 'd' in this batch would have different keys than the
/// // first batch and so we'd have to send a replacement dictionary with new keys
/// // for both.
/// writer.write(&batch2).unwrap();
/// writer.finish().unwrap();
/// ```
/// [IPC Streaming Format]: https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format
pub struct StreamWriter<W> {
    /// The object to write to
    writer: W,
    /// IPC write options
    write_options: IpcWriteOptions,
    /// Whether the writer footer has been written, and the writer is finished
    finished: bool,
    /// Keeps track of dictionaries that have been written
    dictionary_tracker: DictionaryTracker,

    data_gen: IpcDataGenerator,

    ipc_write_context: IpcWriteContext,
}

impl<W: Write> StreamWriter<BufWriter<W>> {
    /// Try to create a new stream writer with the writer wrapped in a BufWriter.
    ///
    /// See [`StreamWriter::try_new`] for an unbuffered version.
    pub fn try_new_buffered(writer: W, schema: &Schema) -> Result<Self, ArrowError> {
        Self::try_new(BufWriter::new(writer), schema)
    }
}

impl<W: Write> StreamWriter<W> {
    /// Try to create a new writer, with the schema written as part of the header.
    ///
    /// Note that there is no internal buffering. See also [`StreamWriter::try_new_buffered`].
    ///
    /// # Errors
    ///
    /// An ['Err'](Result::Err) may be returned if writing the header to the writer fails.
    pub fn try_new(writer: W, schema: &Schema) -> Result<Self, ArrowError> {
        let write_options = IpcWriteOptions::default();
        Self::try_new_with_options(writer, schema, write_options)
    }

    /// Try to create a new writer with [`IpcWriteOptions`].
    ///
    /// # Errors
    ///
    /// An ['Err'](Result::Err) may be returned if writing the header to the writer fails.
    pub fn try_new_with_options(
        mut writer: W,
        schema: &Schema,
        write_options: IpcWriteOptions,
    ) -> Result<Self, ArrowError> {
        ensure_supported_ipc_schema(schema)?;

        let data_gen = IpcDataGenerator::default();
        let mut dictionary_tracker = DictionaryTracker::new(false);

        // write the schema, set the written bytes to the schema
        let encoded_message = data_gen.schema_to_bytes_with_dictionary_tracker(
            schema,
            &mut dictionary_tracker,
            &write_options,
        );
        write_message(&mut writer, encoded_message, &write_options)?;
        Ok(Self {
            writer,
            write_options,
            finished: false,
            dictionary_tracker,
            data_gen,
            ipc_write_context: IpcWriteContext::default(),
        })
    }

    /// Write a record batch to the stream
    pub fn write(&mut self, batch: &RecordBatch) -> Result<(), ArrowError> {
        if self.finished {
            return Err(ArrowError::IpcError(
                "Cannot write record batch to stream writer as it is closed".to_string(),
            ));
        }

        self.data_gen.write(
            batch,
            &mut self.dictionary_tracker,
            &self.write_options,
            &mut self.ipc_write_context,
            &mut self.writer,
        )?;
        Ok(())
    }

    /// Write continuation bytes, and mark the stream as done
    pub fn finish(&mut self) -> Result<(), ArrowError> {
        if self.finished {
            return Err(ArrowError::IpcError(
                "Cannot write footer to stream writer as it is closed".to_string(),
            ));
        }

        write_continuation(&mut self.writer, &self.write_options, 0)?;
        self.writer.flush()?;

        self.finished = true;

        Ok(())
    }

    /// Gets a reference to the underlying writer.
    pub fn get_ref(&self) -> &W {
        &self.writer
    }

    /// Gets a mutable reference to the underlying writer.
    ///
    /// It is inadvisable to directly write to the underlying writer.
    pub fn get_mut(&mut self) -> &mut W {
        &mut self.writer
    }

    /// Flush the underlying writer.
    ///
    /// Both the BufWriter and the underlying writer are flushed.
    pub fn flush(&mut self) -> Result<(), ArrowError> {
        self.writer.flush()?;
        Ok(())
    }

    /// Unwraps the the underlying writer.
    ///
    /// The writer is flushed and the StreamWriter is finished before returning.
    ///
    /// # Errors
    ///
    /// An ['Err'](Result::Err) may be returned if an error occurs while finishing the StreamWriter
    /// or while flushing the writer.
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
    /// let schema = Schema::empty();
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
            // `finish` flushes.
            self.finish()?;
        }
        Ok(self.writer)
    }
}

impl<W: Write> RecordBatchWriter for StreamWriter<W> {
    fn write(&mut self, batch: &RecordBatch) -> Result<(), ArrowError> {
        self.write(batch)
    }

    fn close(mut self) -> Result<(), ArrowError> {
        self.finish()
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
    if arrow_data_len % usize::from(write_options.alignment) != 0 {
        return Err(ArrowError::MemoryError(
            "Arrow data not aligned".to_string(),
        ));
    }

    let a = usize::from(write_options.alignment - 1);
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
    writer.write_all(&PADDING[..padding_bytes])?;

    // write arrow data
    let body_len = if arrow_data_len > 0 {
        write_body_buffers(&mut writer, &encoded.arrow_data, write_options.alignment)?
    } else {
        0
    };

    Ok((aligned_size, body_len))
}

fn write_body_buffers<W: Write>(
    mut writer: W,
    data: &[u8],
    alignment: u8,
) -> Result<usize, ArrowError> {
    let len = data.len();
    let pad_len = pad_to_alignment(alignment, len);
    let total_len = len + pad_len;

    // write body buffer
    writer.write_all(data)?;
    if pad_len > 0 {
        writer.write_all(&PADDING[..pad_len])?;
    }

    Ok(total_len)
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
        crate::MetadataVersion::V1 | crate::MetadataVersion::V2 | crate::MetadataVersion::V3 => {
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
        z => panic!("Unsupported crate::MetadataVersion {z:?}"),
    };

    Ok(written)
}

/// In V4, null types have no validity bitmap
/// In V5 and later, null and union types have no validity bitmap
/// Run end encoded type has no validity bitmap.
fn has_validity_bitmap(data_type: &DataType, write_options: &IpcWriteOptions) -> bool {
    if write_options.metadata_version < crate::MetadataVersion::V5 {
        !matches!(data_type, DataType::Null)
    } else {
        !matches!(
            data_type,
            DataType::Null | DataType::Union(_, _) | DataType::RunEndEncoded(_, _)
        )
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
        BufferSpec::FixedWidth { byte_width, .. } => *byte_width,
        _ => 0,
    }
}

/// Common functionality for re-encoding offsets. Returns the new offsets as well as
/// original start offset and length for use in slicing child data.
fn reencode_offsets<O: OffsetSizeTrait>(
    offsets: &Buffer,
    data: &ArrayData,
) -> (Buffer, usize, usize) {
    let offsets_slice: &[O] = offsets.typed_data::<O>();
    let offset_slice = &offsets_slice[data.offset()..data.offset() + data.len() + 1];

    let start_offset = offset_slice.first().unwrap();
    let end_offset = offset_slice.last().unwrap();

    let offsets = match start_offset.as_usize() {
        0 => {
            let size = size_of::<O>();
            offsets.slice_with_length(data.offset() * size, (data.len() + 1) * size)
        }
        _ => offset_slice.iter().map(|x| *x - *start_offset).collect(),
    };

    let start_offset = start_offset.as_usize();
    let end_offset = end_offset.as_usize();

    (offsets, start_offset, end_offset - start_offset)
}

/// Returns the values and offsets [`Buffer`] for a ByteArray with offset type `O`
///
/// In particular, this handles re-encoding the offsets if they don't start at `0`,
/// slicing the values buffer as appropriate. This helps reduce the encoded
/// size of sliced arrays, as values that have been sliced away are not encoded
fn get_byte_array_buffers<O: OffsetSizeTrait>(data: &ArrayData) -> (Buffer, Buffer) {
    if data.is_empty() {
        // As per specification, offsets buffer has N+1 elements.
        // So an empty array should still be encoded with a single 0 offset.
        let mut offsets = MutableBuffer::new(size_of::<O>());
        offsets.extend_from_slice(O::usize_as(0).to_byte_slice());
        return (offsets.into(), MutableBuffer::new(0).into());
    }

    let (offsets, original_start_offset, len) = reencode_offsets::<O>(&data.buffers()[0], data);
    let values = data.buffers()[1].slice_with_length(original_start_offset, len);
    (offsets, values)
}

/// Similar logic as [`get_byte_array_buffers()`] but slices the child array instead
/// of a values buffer.
fn get_list_array_buffers<O: OffsetSizeTrait>(data: &ArrayData) -> (Buffer, ArrayData) {
    if data.is_empty() {
        // As per specification, offsets buffer has N+1 elements.
        // So an empty array should still be encoded with a single 0 offset.
        let mut offsets = MutableBuffer::new(size_of::<O>());
        offsets.extend_from_slice(O::usize_as(0).to_byte_slice());
        return (offsets.into(), data.child_data()[0].slice(0, 0));
    }

    let (offsets, original_start_offset, len) = reencode_offsets::<O>(&data.buffers()[0], data);
    let child_data = data.child_data()[0].slice(original_start_offset, len);
    (offsets, child_data)
}

/// Returns the offsets, sizes, and child data buffers for a ListView array.
///
/// Unlike List arrays, ListView arrays store both offsets and sizes explicitly,
/// and offsets can be non-monotonic. When slicing, we simply pass through the
/// offsets and sizes without re-encoding, and do not slice the child data.
fn get_list_view_array_buffers<O: OffsetSizeTrait>(
    data: &ArrayData,
) -> (Buffer, Buffer, ArrayData) {
    if data.is_empty() {
        return (
            MutableBuffer::new(0).into(),
            MutableBuffer::new(0).into(),
            data.child_data()[0].slice(0, 0),
        );
    }

    let offsets = &data.buffers()[0];
    let sizes = &data.buffers()[1];

    let element_size = std::mem::size_of::<O>();
    let offsets_slice =
        offsets.slice_with_length(data.offset() * element_size, data.len() * element_size);
    let sizes_slice =
        sizes.slice_with_length(data.offset() * element_size, data.len() * element_size);

    let child_data = data.child_data()[0].clone();

    (offsets_slice, sizes_slice, child_data)
}

/// Returns the sliced views [`Buffer`] for a BinaryView/Utf8View array.
///
/// The views buffer is sliced to only include views in the valid range based on
/// the array's offset and length. This helps reduce the encoded size of sliced
/// arrays
///
fn get_or_truncate_buffer(array_data: &ArrayData) -> Buffer {
    let buffer = &array_data.buffers()[0];
    let layout = layout(array_data.data_type());
    let spec = &layout.buffers[0];

    let byte_width = get_buffer_element_width(spec);
    let min_length = array_data.len() * byte_width;
    if buffer_need_truncate(array_data.offset(), buffer, spec, min_length) {
        let byte_offset = array_data.offset() * byte_width;
        let buffer_length = min(min_length, buffer.len() - byte_offset);
        buffer.slice_with_length(byte_offset, buffer_length)
    } else {
        buffer.clone()
    }
}

/// Recursively encodes `array_data` into its IPC representation.
///
/// Output goes to two separate channels:
/// - `meta`: accumulates IPC metadata (`nodes` and `buffers`) for the flatbuffer header.
/// - `sink`: the raw Arrow data bytes that form the IPC message body.
fn write_array_data(
    array_data: &ArrayData,
    meta: &mut IpcMetadataBuilder,
    sink: &mut IpcBodySink<'_>,
    offset: i64,
    compression_codec: Option<CompressionCodec>,
    ipc_write_context: &mut IpcWriteContext,
    write_options: &IpcWriteOptions,
) -> Result<i64, ArrowError> {
    let mut offset = offset;
    let num_rows = array_data.len();
    if !matches!(array_data.data_type(), DataType::Null) {
        meta.nodes.push(crate::FieldNode::new(
            num_rows as i64,
            array_data.null_count() as i64,
        ));
    } else {
        // NullArray's null_count equals to len, but ArrayData null_count is always 0.
        meta.nodes
            .push(crate::FieldNode::new(num_rows as i64, num_rows as i64));
    }
    if has_validity_bitmap(array_data.data_type(), write_options) {
        // write null buffer if exists
        let null_buffer = match array_data.nulls() {
            None => {
                // create a buffer and fill it with valid bits
                let num_bytes = bit_util::ceil(num_rows, 8);
                let buffer = MutableBuffer::new(num_bytes);
                let buffer = buffer.with_bitset(num_bytes, true);
                buffer.into()
            }
            Some(buffer) => buffer.inner().sliced(),
        };

        offset = encode_sink_buffer(
            null_buffer,
            meta,
            sink,
            offset,
            compression_codec,
            ipc_write_context,
            write_options.alignment,
        )?;
    }

    let data_type = array_data.data_type();
    if matches!(data_type, DataType::Binary | DataType::Utf8) {
        let (offsets, values) = get_byte_array_buffers::<i32>(array_data);
        for buffer in [offsets, values] {
            offset = encode_sink_buffer(
                buffer,
                meta,
                sink,
                offset,
                compression_codec,
                ipc_write_context,
                write_options.alignment,
            )?;
        }
    } else if matches!(data_type, DataType::BinaryView | DataType::Utf8View) {
        // Slicing the views buffer is safe and easy,
        // but pruning unneeded data buffers is much more nuanced since it's complicated to prove that no views reference the pruned buffers
        //
        // Current implementation just serialize the raw arrays as given and not try to optimize anything.
        // If users wants to "compact" the arrays prior to sending them over IPC,
        // they should consider the gc API suggested in #5513
        let views = get_or_truncate_buffer(array_data);
        offset = encode_sink_buffer(
            views,
            meta,
            sink,
            offset,
            compression_codec,
            ipc_write_context,
            write_options.alignment,
        )?;

        for buffer in array_data.buffers().iter().skip(1) {
            offset = encode_sink_buffer(
                buffer.clone(),
                meta,
                sink,
                offset,
                compression_codec,
                ipc_write_context,
                write_options.alignment,
            )?;
        }
    } else if matches!(data_type, DataType::LargeBinary | DataType::LargeUtf8) {
        let (offsets, values) = get_byte_array_buffers::<i64>(array_data);
        for buffer in [offsets, values] {
            offset = encode_sink_buffer(
                buffer,
                meta,
                sink,
                offset,
                compression_codec,
                ipc_write_context,
                write_options.alignment,
            )?;
        }
    } else if DataType::is_numeric(data_type)
        || DataType::is_temporal(data_type)
        || matches!(
            array_data.data_type(),
            DataType::FixedSizeBinary(_) | DataType::Dictionary(_, _)
        )
    {
        // Truncate values
        assert_eq!(array_data.buffers().len(), 1);

        let buffer = get_or_truncate_buffer(array_data);
        offset = encode_sink_buffer(
            buffer,
            meta,
            sink,
            offset,
            compression_codec,
            ipc_write_context,
            write_options.alignment,
        )?;
    } else if matches!(data_type, DataType::Boolean) {
        // Bools are special because the payload (= 1 bit) is smaller than the physical container elements (= bytes).
        // The array data may not start at the physical boundary of the underlying buffer, so we need to shift bits around.
        assert_eq!(array_data.buffers().len(), 1);

        let buffer = &array_data.buffers()[0];
        let buffer = buffer.bit_slice(array_data.offset(), array_data.len());
        offset = encode_sink_buffer(
            buffer,
            meta,
            sink,
            offset,
            compression_codec,
            ipc_write_context,
            write_options.alignment,
        )?;
    } else if matches!(
        data_type,
        DataType::List(_) | DataType::LargeList(_) | DataType::Map(_, _)
    ) {
        assert_eq!(array_data.buffers().len(), 1);
        assert_eq!(array_data.child_data().len(), 1);

        // Truncate offsets and the child data to avoid writing unnecessary data
        let (offsets, sliced_child_data) = match data_type {
            DataType::List(_) => get_list_array_buffers::<i32>(array_data),
            DataType::Map(_, _) => get_list_array_buffers::<i32>(array_data),
            DataType::LargeList(_) => get_list_array_buffers::<i64>(array_data),
            _ => unreachable!(),
        };
        offset = encode_sink_buffer(
            offsets,
            meta,
            sink,
            offset,
            compression_codec,
            ipc_write_context,
            write_options.alignment,
        )?;
        offset = write_array_data(
            &sliced_child_data,
            meta,
            sink,
            offset,
            compression_codec,
            ipc_write_context,
            write_options,
        )?;
        return Ok(offset);
    } else if matches!(
        data_type,
        DataType::ListView(_) | DataType::LargeListView(_)
    ) {
        assert_eq!(array_data.buffers().len(), 2); // offsets + sizes
        assert_eq!(array_data.child_data().len(), 1);

        let (offsets, sizes, child_data) = match data_type {
            DataType::ListView(_) => get_list_view_array_buffers::<i32>(array_data),
            DataType::LargeListView(_) => get_list_view_array_buffers::<i64>(array_data),
            _ => unreachable!(),
        };

        offset = encode_sink_buffer(
            offsets,
            meta,
            sink,
            offset,
            compression_codec,
            ipc_write_context,
            write_options.alignment,
        )?;
        offset = encode_sink_buffer(
            sizes,
            meta,
            sink,
            offset,
            compression_codec,
            ipc_write_context,
            write_options.alignment,
        )?;

        offset = write_array_data(
            &child_data,
            meta,
            sink,
            offset,
            compression_codec,
            ipc_write_context,
            write_options,
        )?;
        return Ok(offset);
    } else if let DataType::FixedSizeList(_, fixed_size) = data_type {
        assert_eq!(array_data.child_data().len(), 1);
        let fixed_size = *fixed_size as usize;

        let child_offset = array_data.offset() * fixed_size;
        let child_length = array_data.len() * fixed_size;
        let child_data = array_data.child_data()[0].slice(child_offset, child_length);

        offset = write_array_data(
            &child_data,
            meta,
            sink,
            offset,
            compression_codec,
            ipc_write_context,
            write_options,
        )?;
        return Ok(offset);
    } else {
        for buffer in array_data.buffers() {
            offset = encode_sink_buffer(
                buffer.clone(),
                meta,
                sink,
                offset,
                compression_codec,
                ipc_write_context,
                write_options.alignment,
            )?;
        }
    }

    match array_data.data_type() {
        DataType::Dictionary(_, _) => {}
        DataType::RunEndEncoded(_, _) => {
            // unslice the run encoded array.
            let arr = unslice_run_array(array_data.clone())?;
            // recursively write out nested structures
            for data_ref in arr.child_data() {
                // write the nested data (e.g list data)
                offset = write_array_data(
                    data_ref,
                    meta,
                    sink,
                    offset,
                    compression_codec,
                    ipc_write_context,
                    write_options,
                )?;
            }
        }
        _ => {
            // recursively write out nested structures
            for data_ref in array_data.child_data() {
                // write the nested data (e.g list data)
                offset = write_array_data(
                    data_ref,
                    meta,
                    sink,
                    offset,
                    compression_codec,
                    ipc_write_context,
                    write_options,
                )?;
            }
        }
    }
    Ok(offset)
}

/// Encodes a single Arrow [`Buffer`] into the IPC body and records its metadata.
///
/// - `buffer`: the Arrow data buffer to encode (validity bitmap, offsets, values, etc.)
/// - `buffers`: in-progress list of IPC `Buffer` metadata entries (body offset + length) that
///   will eventually be serialised into the flatbuffer `RecordBatch` header.
/// - `sink`: destination for the actual encoded bytes; either a contiguous `Vec<u8>` for
///   in-memory writes, or a list of [`EncodedBuffer`] segments for deferred zero-copy streaming.
/// - `offset`: running byte offset into the IPC message body, used to compute the metadata entry.
/// - `compression_codec` / `ipc_write_context`: if `Some`, the buffer is compressed before
///   writing; `ipc_write_context` provides reusable scratch space across calls.
/// - `alignment`: each buffer is padded to this many bytes so the next buffer starts aligned.
///
/// Returns the updated `offset` (advanced by the encoded length plus any alignment padding).
fn encode_sink_buffer(
    buffer: Buffer,
    ipc_meta_data: &mut IpcMetadataBuilder,
    sink: &mut IpcBodySink<'_>,
    offset: i64,
    compression_codec: Option<CompressionCodec>,
    ipc_write_context: &mut IpcWriteContext,
    alignment: u8,
) -> Result<i64, ArrowError> {
    let (encoded, len) = match compression_codec {
        None => {
            let len = buffer.len() as i64;
            (EncodedBuffer::Raw(buffer), len)
        }
        Some(codec) => {
            let mut scratch = Vec::new();
            let written =
                codec.compress_to_vec(buffer.as_slice(), &mut scratch, ipc_write_context)?;
            let len = i64::try_from(written)
                .map_err(|e| ArrowError::InvalidArgumentError(format!("{e}")))?;
            (EncodedBuffer::Compressed(scratch), len)
        }
    };

    let pad_len = pad_to_alignment(alignment, len as usize);
    sink.write(pad_len, encoded);
    ipc_meta_data.buffers.push(crate::Buffer::new(offset, len));
    Ok(offset + len + pad_len as i64)
}

const PADDING: [u8; 64] = [0; 64];

/// Estimates the number of [`EncodedBuffer`] segments that [`write_array_data`]
/// will produce for a column of the given type.
///
/// Based on the Arrow IPC buffer layout
/// (<https://arrow.apache.org/docs/format/Columnar.html#recordbatch-message>):
#[inline]
fn estimate_encoded_buffer_count(dt: &DataType) -> usize {
    match dt {
        DataType::Null => 0,

        DataType::Binary | DataType::Utf8 | DataType::LargeBinary | DataType::LargeUtf8 => 3,

        DataType::BinaryView | DataType::Utf8View => 3,

        DataType::List(f) | DataType::LargeList(f) | DataType::Map(f, _) => {
            2 + estimate_encoded_buffer_count(f.data_type())
        }

        DataType::ListView(f) | DataType::LargeListView(f) => {
            3 + estimate_encoded_buffer_count(f.data_type())
        }

        DataType::FixedSizeList(f, _) => 1 + estimate_encoded_buffer_count(f.data_type()),

        DataType::Struct(fields) => {
            1 + fields
                .iter()
                .map(|f| estimate_encoded_buffer_count(f.data_type()))
                .sum::<usize>()
        }

        // Dictionary indices only; dictionary body is a separate IPC message.
        DataType::Dictionary(_, _) => 2,

        DataType::Union(fields, UnionMode::Sparse) => {
            1 + fields
                .iter()
                .map(|(_, f)| estimate_encoded_buffer_count(f.data_type()))
                .sum::<usize>()
        }
        DataType::Union(fields, UnionMode::Dense) => {
            2 + fields
                .iter()
                .map(|(_, f)| estimate_encoded_buffer_count(f.data_type()))
                .sum::<usize>()
        }

        DataType::RunEndEncoded(run_ends, values) => {
            estimate_encoded_buffer_count(run_ends.data_type())
                + estimate_encoded_buffer_count(values.data_type())
        }
        // Primitive, Bool, temporal, Decimal*, FixedSizeBinary: validity + values.
        _ => 2,
    }
}

/// Calculate an alignment boundary and return the number of bytes needed to pad to the alignment boundary
#[inline]
fn pad_to_alignment(alignment: u8, len: usize) -> usize {
    let a = usize::from(alignment - 1);
    ((len + a) & !a) - len
}

#[cfg(test)]
mod tests {
    use std::hash::Hasher;
    use std::io::Cursor;
    use std::io::Seek;

    use arrow_array::builder::FixedSizeListBuilder;
    use arrow_array::builder::Float32Builder;
    use arrow_array::builder::Int64Builder;
    use arrow_array::builder::MapBuilder;
    use arrow_array::builder::StringViewBuilder;
    use arrow_array::builder::UnionBuilder;
    use arrow_array::builder::{
        GenericListBuilder, GenericListViewBuilder, ListBuilder, StringBuilder,
    };
    use arrow_array::builder::{PrimitiveRunBuilder, UInt32Builder};
    use arrow_array::types::*;
    use arrow_buffer::ScalarBuffer;

    use crate::MetadataVersion;
    use crate::convert::fb_to_schema;
    use crate::reader::*;
    use crate::root_as_footer;

    use super::*;

    fn serialize_file(rb: &RecordBatch) -> Vec<u8> {
        let mut writer = FileWriter::try_new(vec![], rb.schema_ref()).unwrap();
        writer.write(rb).unwrap();
        writer.finish().unwrap();
        writer.into_inner().unwrap()
    }

    fn deserialize_file(bytes: Vec<u8>) -> RecordBatch {
        let mut reader = FileReader::try_new(Cursor::new(bytes), None).unwrap();
        reader.next().unwrap().unwrap()
    }

    fn serialize_stream(record: &RecordBatch) -> Vec<u8> {
        // Use 8-byte alignment so that the various `truncate_*` tests can be compactly written,
        // without needing to construct a giant array to spill over the 64-byte default alignment
        // boundary.
        const IPC_ALIGNMENT: usize = 8;

        let mut stream_writer = StreamWriter::try_new_with_options(
            vec![],
            record.schema_ref(),
            IpcWriteOptions::try_new(IPC_ALIGNMENT, false, MetadataVersion::V5).unwrap(),
        )
        .unwrap();
        stream_writer.write(record).unwrap();
        stream_writer.finish().unwrap();
        stream_writer.into_inner().unwrap()
    }

    fn deserialize_stream(bytes: Vec<u8>) -> RecordBatch {
        let mut stream_reader = StreamReader::try_new(Cursor::new(bytes), None).unwrap();
        stream_reader.next().unwrap().unwrap()
    }

    #[test]
    #[cfg(feature = "lz4")]
    fn test_write_empty_record_batch_lz4_compression() {
        let schema = Schema::new(vec![Field::new("field1", DataType::Int32, true)]);
        let values: Vec<Option<i32>> = vec![];
        let array = Int32Array::from(values);
        let record_batch =
            RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(array)]).unwrap();

        let mut file = tempfile::tempfile().unwrap();

        {
            let write_option = IpcWriteOptions::try_new(8, false, crate::MetadataVersion::V5)
                .unwrap()
                .try_with_compression(Some(crate::CompressionType::LZ4_FRAME))
                .unwrap();

            let mut writer =
                FileWriter::try_new_with_options(&mut file, &schema, write_option).unwrap();
            writer.write(&record_batch).unwrap();
            writer.finish().unwrap();
        }
        file.rewind().unwrap();
        {
            // read file
            let reader = FileReader::try_new(file, None).unwrap();
            for read_batch in reader {
                read_batch
                    .unwrap()
                    .columns()
                    .iter()
                    .zip(record_batch.columns())
                    .for_each(|(a, b)| {
                        assert_eq!(a.data_type(), b.data_type());
                        assert_eq!(a.len(), b.len());
                        assert_eq!(a.null_count(), b.null_count());
                    });
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
            RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(array)]).unwrap();

        let mut file = tempfile::tempfile().unwrap();
        {
            let write_option = IpcWriteOptions::try_new(8, false, crate::MetadataVersion::V5)
                .unwrap()
                .try_with_compression(Some(crate::CompressionType::LZ4_FRAME))
                .unwrap();

            let mut writer =
                FileWriter::try_new_with_options(&mut file, &schema, write_option).unwrap();
            writer.write(&record_batch).unwrap();
            writer.finish().unwrap();
        }
        file.rewind().unwrap();
        {
            // read file
            let reader = FileReader::try_new(file, None).unwrap();
            for read_batch in reader {
                read_batch
                    .unwrap()
                    .columns()
                    .iter()
                    .zip(record_batch.columns())
                    .for_each(|(a, b)| {
                        assert_eq!(a.data_type(), b.data_type());
                        assert_eq!(a.len(), b.len());
                        assert_eq!(a.null_count(), b.null_count());
                    });
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
            RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(array)]).unwrap();
        let mut file = tempfile::tempfile().unwrap();
        {
            let write_option = IpcWriteOptions::try_new(8, false, crate::MetadataVersion::V5)
                .unwrap()
                .try_with_compression(Some(crate::CompressionType::ZSTD))
                .unwrap()
                .try_with_compression_level(Some(1))
                .unwrap();

            let mut writer =
                FileWriter::try_new_with_options(&mut file, &schema, write_option).unwrap();
            writer.write(&record_batch).unwrap();
            writer.finish().unwrap();
        }
        file.rewind().unwrap();
        {
            // read file
            let reader = FileReader::try_new(file, None).unwrap();
            for read_batch in reader {
                read_batch
                    .unwrap()
                    .columns()
                    .iter()
                    .zip(record_batch.columns())
                    .for_each(|(a, b)| {
                        assert_eq!(a.data_type(), b.data_type());
                        assert_eq!(a.len(), b.len());
                        assert_eq!(a.null_count(), b.null_count());
                    });
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
        let batch =
            RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(array1) as ArrayRef])
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

    #[test]
    fn test_empty_utf8_ipc_writes_nonempty_offsets_buffer() {
        let name = StringArray::from(Vec::<String>::new());
        let (offsets, values) = get_byte_array_buffers::<i32>(&name.to_data());

        assert_eq!(name.len(), 0);
        assert_eq!(
            offsets.len(),
            std::mem::size_of::<i32>(),
            "offsets buffer should contain one zero i32 offset"
        );
        assert_eq!(values.len(), 0, "values buffer should remain empty");
    }

    #[test]
    fn test_empty_large_utf8_ipc_writes_nonempty_offsets_buffer() {
        let name = LargeStringArray::from(Vec::<String>::new());
        let (offsets, values) = get_byte_array_buffers::<i64>(&name.to_data());

        assert_eq!(name.len(), 0);
        assert_eq!(
            offsets.len(),
            std::mem::size_of::<i64>(),
            "offsets buffer should contain one zero i64 offset"
        );
        assert_eq!(values.len(), 0, "values buffer should remain empty");
    }

    #[test]
    fn test_empty_list_ipc_writes_nonempty_offsets_buffer() {
        let list = GenericListBuilder::<i32, _>::new(UInt32Builder::new()).finish();
        let (offsets, child_data) = get_list_array_buffers::<i32>(&list.to_data());

        assert_eq!(list.len(), 0);
        assert_eq!(
            offsets.len(),
            std::mem::size_of::<i32>(),
            "offsets buffer should contain one zero i32 offset"
        );
        assert_eq!(child_data.len(), 0, "child data should remain empty");
    }

    #[test]
    fn test_empty_large_list_ipc_writes_nonempty_offsets_buffer() {
        let list = GenericListBuilder::<i64, _>::new(UInt32Builder::new()).finish();
        let (offsets, child_data) = get_list_array_buffers::<i64>(&list.to_data());

        assert_eq!(list.len(), 0);
        assert_eq!(
            offsets.len(),
            std::mem::size_of::<i64>(),
            "offsets buffer should contain one zero i64 offset"
        );
        assert_eq!(child_data.len(), 0, "child data should remain empty");
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
        let array4 = Float64Array::from(vec![f64::NAN; 32]);
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
            let mut writer = FileWriter::try_new_with_options(&mut file, &schema, options).unwrap();

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
        write_null_file(IpcWriteOptions::try_new(64, false, MetadataVersion::V4).unwrap());
        write_null_file(IpcWriteOptions::try_new(64, true, MetadataVersion::V4).unwrap());
    }

    #[test]
    fn test_write_null_file_v5() {
        write_null_file(IpcWriteOptions::try_new(8, false, MetadataVersion::V5).unwrap());
        write_null_file(IpcWriteOptions::try_new(64, false, MetadataVersion::V5).unwrap());
    }

    #[test]
    fn track_union_nested_dict() {
        let inner: DictionaryArray<Int32Type> = vec!["a", "b", "a"].into_iter().collect();

        let array = Arc::new(inner) as ArrayRef;

        // Dict field with id 2
        #[allow(deprecated)]
        let dctfield = Field::new_dict("dict", array.data_type().clone(), false, 0, false);
        let union_fields = [(0, Arc::new(dctfield))].into_iter().collect();

        let types = [0, 0, 0].into_iter().collect::<ScalarBuffer<i8>>();
        let offsets = [0, 1, 2].into_iter().collect::<ScalarBuffer<i32>>();

        let union = UnionArray::try_new(union_fields, types, Some(offsets), vec![array]).unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "union",
            union.data_type().clone(),
            false,
        )]));

        let r#gen = IpcDataGenerator::default();
        let mut dict_tracker = DictionaryTracker::new(false);
        r#gen.schema_to_bytes_with_dictionary_tracker(
            &schema,
            &mut dict_tracker,
            &IpcWriteOptions::default(),
        );

        let batch = RecordBatch::try_new(schema, vec![Arc::new(union)]).unwrap();

        r#gen
            .encode(
                &batch,
                &mut dict_tracker,
                &Default::default(),
                &mut Default::default(),
            )
            .unwrap();

        // The encoder will assign dict IDs itself to ensure uniqueness and ignore the dict ID in the schema
        // so we expect the dict will be keyed to 0
        assert!(dict_tracker.written.contains_key(&0));
    }

    #[test]
    fn track_struct_nested_dict() {
        let inner: DictionaryArray<Int32Type> = vec!["a", "b", "a"].into_iter().collect();

        let array = Arc::new(inner) as ArrayRef;

        // Dict field with id 2
        #[allow(deprecated)]
        let dctfield = Arc::new(Field::new_dict(
            "dict",
            array.data_type().clone(),
            false,
            2,
            false,
        ));

        let s = StructArray::from(vec![(dctfield, array)]);
        let struct_array = Arc::new(s) as ArrayRef;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "struct",
            struct_array.data_type().clone(),
            false,
        )]));

        let r#gen = IpcDataGenerator::default();
        let mut dict_tracker = DictionaryTracker::new(false);
        r#gen.schema_to_bytes_with_dictionary_tracker(
            &schema,
            &mut dict_tracker,
            &IpcWriteOptions::default(),
        );

        let batch = RecordBatch::try_new(schema, vec![struct_array]).unwrap();

        r#gen
            .encode(
                &batch,
                &mut dict_tracker,
                &Default::default(),
                &mut Default::default(),
            )
            .unwrap();

        assert!(dict_tracker.written.contains_key(&0));
    }

    fn write_union_file(options: IpcWriteOptions) {
        let schema = Schema::new(vec![Field::new_union(
            "union",
            vec![0, 1],
            vec![
                Field::new("a", DataType::Int32, false),
                Field::new("c", DataType::Float64, false),
            ],
            UnionMode::Sparse,
        )]);
        let mut builder = UnionBuilder::with_capacity_sparse(5);
        builder.append::<Int32Type>("a", 1).unwrap();
        builder.append_null::<Int32Type>("a").unwrap();
        builder.append::<Float64Type>("c", 3.0).unwrap();
        builder.append_null::<Float64Type>("c").unwrap();
        builder.append::<Int32Type>("a", 4).unwrap();
        let union = builder.build().unwrap();

        let batch =
            RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(union) as ArrayRef])
                .unwrap();

        let mut file = tempfile::tempfile().unwrap();
        {
            let mut writer = FileWriter::try_new_with_options(&mut file, &schema, options).unwrap();

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
        write_union_file(IpcWriteOptions::try_new(8, false, MetadataVersion::V4).unwrap());
        write_union_file(IpcWriteOptions::try_new(8, false, MetadataVersion::V5).unwrap());
    }

    #[test]
    fn test_write_view_types() {
        const LONG_TEST_STRING: &str =
            "This is a long string to make sure binary view array handles it";
        let schema = Schema::new(vec![
            Field::new("field1", DataType::BinaryView, true),
            Field::new("field2", DataType::Utf8View, true),
        ]);
        let values: Vec<Option<&[u8]>> = vec![
            Some(b"foo"),
            Some(b"bar"),
            Some(LONG_TEST_STRING.as_bytes()),
        ];
        let binary_array = BinaryViewArray::from_iter(values);
        let utf8_array =
            StringViewArray::from_iter(vec![Some("foo"), Some("bar"), Some(LONG_TEST_STRING)]);
        let record_batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(binary_array), Arc::new(utf8_array)],
        )
        .unwrap();

        let mut file = tempfile::tempfile().unwrap();
        {
            let mut writer = FileWriter::try_new(&mut file, &schema).unwrap();
            writer.write(&record_batch).unwrap();
            writer.finish().unwrap();
        }
        file.rewind().unwrap();
        {
            let mut reader = FileReader::try_new(&file, None).unwrap();
            let read_batch = reader.next().unwrap().unwrap();
            read_batch
                .columns()
                .iter()
                .zip(record_batch.columns())
                .for_each(|(a, b)| {
                    assert_eq!(a, b);
                });
        }
        file.rewind().unwrap();
        {
            let mut reader = FileReader::try_new(&file, Some(vec![0])).unwrap();
            let read_batch = reader.next().unwrap().unwrap();
            assert_eq!(read_batch.num_columns(), 1);
            let read_array = read_batch.column(0);
            let write_array = record_batch.column(0);
            assert_eq!(read_array, write_array);
        }
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

            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a), Arc::new(b)]).unwrap()
        }

        let big_record_batch = create_batch(65536);

        let length = 5;
        let small_record_batch = create_batch(length);

        let offset = 2;
        let record_batch_slice = big_record_batch.slice(offset, length);
        assert!(
            serialize_stream(&big_record_batch).len() > serialize_stream(&small_record_batch).len()
        );
        assert_eq!(
            serialize_stream(&small_record_batch).len(),
            serialize_stream(&record_batch_slice).len()
        );

        assert_eq!(
            deserialize_stream(serialize_stream(&record_batch_slice)),
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

            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a), Arc::new(b)]).unwrap()
        }

        let record_batch = create_batch();
        let record_batch_slice = record_batch.slice(1, 2);
        let deserialized_batch = deserialize_stream(serialize_stream(&record_batch_slice));

        assert!(
            serialize_stream(&record_batch).len() > serialize_stream(&record_batch_slice).len()
        );

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
            let keys: Int32Array = [Some(0), Some(2), None, Some(1)].into_iter().collect();

            let array = DictionaryArray::new(keys, Arc::new(values));

            let schema = Schema::new(vec![Field::new("dict", array.data_type().clone(), true)]);

            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap()
        }

        let record_batch = create_batch();
        let record_batch_slice = record_batch.slice(1, 2);
        let deserialized_batch = deserialize_stream(serialize_stream(&record_batch_slice));

        assert!(
            serialize_stream(&record_batch).len() > serialize_stream(&record_batch_slice).len()
        );

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
            let ints: Int32Array = [Some(0), Some(2), None, Some(1)].into_iter().collect();

            let struct_array = StructArray::from(vec![
                (
                    Arc::new(Field::new("s", DataType::Utf8, true)),
                    Arc::new(strings) as ArrayRef,
                ),
                (
                    Arc::new(Field::new("c", DataType::Int32, true)),
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
        let deserialized_batch = deserialize_stream(serialize_stream(&record_batch_slice));

        assert!(
            serialize_stream(&record_batch).len() > serialize_stream(&record_batch_slice).len()
        );

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
            let a = StringArray::from(vec![Some(""), Some(""), Some(""), Some(""), Some("")]);
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)]).unwrap()
        }

        let record_batch = create_batch();
        let record_batch_slice = record_batch.slice(0, 1);
        let deserialized_batch = deserialize_stream(serialize_stream(&record_batch_slice));

        assert!(
            serialize_stream(&record_batch).len() > serialize_stream(&record_batch_slice).len()
        );
        assert_eq!(record_batch_slice, deserialized_batch);
    }

    #[test]
    fn test_stream_writer_writes_array_slice() {
        let array = UInt32Array::from(vec![Some(1), Some(2), Some(3)]);
        assert_eq!(
            vec![Some(1), Some(2), Some(3)],
            array.iter().collect::<Vec<_>>()
        );

        let sliced = array.slice(1, 2);
        assert_eq!(vec![Some(2), Some(3)], sliced.iter().collect::<Vec<_>>());

        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::UInt32, true)])),
            vec![Arc::new(sliced)],
        )
        .expect("new batch");

        let mut writer = StreamWriter::try_new(vec![], batch.schema_ref()).expect("new writer");
        writer.write(&batch).expect("write");
        let outbuf = writer.into_inner().expect("inner");

        let mut reader = StreamReader::try_new(&outbuf[..], None).expect("new reader");
        let read_batch = reader.next().unwrap().expect("read batch");

        let read_array: &UInt32Array = read_batch.column(0).as_primitive();
        assert_eq!(
            vec![Some(2), Some(3)],
            read_array.iter().collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_large_slice_uint32() {
        ensure_roundtrip(Arc::new(UInt32Array::from_iter(
            (0..8000).map(|i| if i % 2 == 0 { Some(i) } else { None }),
        )));
    }

    #[test]
    fn test_large_slice_string() {
        let strings: Vec<_> = (0..8000)
            .map(|i| {
                if i % 2 == 0 {
                    Some(format!("value{i}"))
                } else {
                    None
                }
            })
            .collect();

        ensure_roundtrip(Arc::new(StringArray::from(strings)));
    }

    #[test]
    fn test_large_slice_string_list() {
        let mut ls = ListBuilder::new(StringBuilder::new());

        let mut s = String::new();
        for row_number in 0..8000 {
            if row_number % 2 == 0 {
                for list_element in 0..1000 {
                    s.clear();
                    use std::fmt::Write;
                    write!(&mut s, "value{row_number}-{list_element}").unwrap();
                    ls.values().append_value(&s);
                }
                ls.append(true)
            } else {
                ls.append(false); // null
            }
        }

        ensure_roundtrip(Arc::new(ls.finish()));
    }

    #[test]
    fn test_large_slice_string_list_of_lists() {
        // The reason for the special test is to verify reencode_offsets which looks both at
        // the starting offset and the data offset.  So need a dataset where the starting_offset
        // is zero but the data offset is not.
        let mut ls = ListBuilder::new(ListBuilder::new(StringBuilder::new()));

        for _ in 0..4000 {
            ls.values().append(true);
            ls.append(true)
        }

        let mut s = String::new();
        for row_number in 0..4000 {
            if row_number % 2 == 0 {
                for list_element in 0..1000 {
                    s.clear();
                    use std::fmt::Write;
                    write!(&mut s, "value{row_number}-{list_element}").unwrap();
                    ls.values().values().append_value(&s);
                }
                ls.values().append(true);
                ls.append(true)
            } else {
                ls.append(false); // null
            }
        }

        ensure_roundtrip(Arc::new(ls.finish()));
    }

    /// Read/write a record batch to a File and Stream and ensure it is the same at the outout
    fn ensure_roundtrip(array: ArrayRef) {
        let num_rows = array.len();
        let orig_batch = RecordBatch::try_from_iter(vec![("a", array)]).unwrap();
        // take off the first element
        let sliced_batch = orig_batch.slice(1, num_rows - 1);

        let schema = orig_batch.schema();
        let stream_data = {
            let mut writer = StreamWriter::try_new(vec![], &schema).unwrap();
            writer.write(&sliced_batch).unwrap();
            writer.into_inner().unwrap()
        };
        let read_batch = {
            let projection = None;
            let mut reader = StreamReader::try_new(Cursor::new(stream_data), projection).unwrap();
            reader
                .next()
                .expect("expect no errors reading batch")
                .expect("expect batch")
        };
        assert_eq!(sliced_batch, read_batch);

        let file_data = {
            let mut writer = FileWriter::try_new_buffered(vec![], &schema).unwrap();
            writer.write(&sliced_batch).unwrap();
            writer.into_inner().unwrap().into_inner().unwrap()
        };
        let read_batch = {
            let projection = None;
            let mut reader = FileReader::try_new(Cursor::new(file_data), projection).unwrap();
            reader
                .next()
                .expect("expect no errors reading batch")
                .expect("expect batch")
        };
        assert_eq!(sliced_batch, read_batch);

        // TODO test file writer/reader
    }

    #[test]
    fn encode_bools_slice() {
        // Test case for https://github.com/apache/arrow-rs/issues/3496
        assert_bool_roundtrip([true, false], 1, 1);

        // slice somewhere in the middle
        assert_bool_roundtrip(
            [
                true, false, true, true, false, false, true, true, true, false, false, false, true,
                true, true, true, false, false, false, false, true, true, true, true, true, false,
                false, false, false, false,
            ],
            13,
            17,
        );

        // start at byte boundary, end in the middle
        assert_bool_roundtrip(
            [
                true, false, true, true, false, false, true, true, true, false, false, false,
            ],
            8,
            2,
        );

        // start and stop and byte boundary
        assert_bool_roundtrip(
            [
                true, false, true, true, false, false, true, true, true, false, false, false, true,
                true, true, true, true, false, false, false, false, false,
            ],
            8,
            8,
        );
    }

    fn assert_bool_roundtrip<const N: usize>(bools: [bool; N], offset: usize, length: usize) {
        let val_bool_field = Field::new("val", DataType::Boolean, false);

        let schema = Arc::new(Schema::new(vec![val_bool_field]));

        let bools = BooleanArray::from(bools.to_vec());

        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(bools)]).unwrap();
        let batch = batch.slice(offset, length);

        let data = serialize_stream(&batch);
        let batch2 = deserialize_stream(data);
        assert_eq!(batch, batch2);
    }

    #[test]
    fn test_run_array_unslice() {
        let total_len = 80;
        let vals: Vec<Option<i32>> = vec![Some(1), None, Some(2), Some(3), Some(4), None, Some(5)];
        let repeats: Vec<usize> = vec![3, 4, 1, 2];
        let mut input_array: Vec<Option<i32>> = Vec::with_capacity(total_len);
        for ix in 0_usize..32 {
            let repeat: usize = repeats[ix % repeats.len()];
            let val: Option<i32> = vals[ix % vals.len()];
            input_array.resize(input_array.len() + repeat, val);
        }

        // Encode the input_array to run array
        let mut builder =
            PrimitiveRunBuilder::<Int16Type, Int32Type>::with_capacity(input_array.len());
        builder.extend(input_array.iter().copied());
        let run_array = builder.finish();

        // test for all slice lengths.
        for slice_len in 1..=total_len {
            // test for offset = 0, slice length = slice_len
            let sliced_run_array: RunArray<Int16Type> =
                run_array.slice(0, slice_len).into_data().into();

            // Create unsliced run array.
            let unsliced_run_array = into_zero_offset_run_array(sliced_run_array).unwrap();
            let typed = unsliced_run_array
                .downcast::<PrimitiveArray<Int32Type>>()
                .unwrap();
            let expected: Vec<Option<i32>> = input_array.iter().take(slice_len).copied().collect();
            let actual: Vec<Option<i32>> = typed.into_iter().collect();
            assert_eq!(expected, actual);

            // test for offset = total_len - slice_len, length = slice_len
            let sliced_run_array: RunArray<Int16Type> = run_array
                .slice(total_len - slice_len, slice_len)
                .into_data()
                .into();

            // Create unsliced run array.
            let unsliced_run_array = into_zero_offset_run_array(sliced_run_array).unwrap();
            let typed = unsliced_run_array
                .downcast::<PrimitiveArray<Int32Type>>()
                .unwrap();
            let expected: Vec<Option<i32>> = input_array
                .iter()
                .skip(total_len - slice_len)
                .copied()
                .collect();
            let actual: Vec<Option<i32>> = typed.into_iter().collect();
            assert_eq!(expected, actual);
        }
    }

    fn generate_list_data<O: OffsetSizeTrait>() -> GenericListArray<O> {
        let mut ls = GenericListBuilder::<O, _>::new(UInt32Builder::new());

        for i in 0..100_000 {
            for value in [i, i, i] {
                ls.values().append_value(value);
            }
            ls.append(true)
        }

        ls.finish()
    }

    fn generate_utf8view_list_data<O: OffsetSizeTrait>() -> GenericListArray<O> {
        let mut ls = GenericListBuilder::<O, _>::new(StringViewBuilder::new());

        for i in 0..100_000 {
            for value in [
                format!("value{}", i),
                format!("value{}", i),
                format!("value{}", i),
            ] {
                ls.values().append_value(&value);
            }
            ls.append(true)
        }

        ls.finish()
    }

    fn generate_string_list_data<O: OffsetSizeTrait>() -> GenericListArray<O> {
        let mut ls = GenericListBuilder::<O, _>::new(StringBuilder::new());

        for i in 0..100_000 {
            for value in [
                format!("value{}", i),
                format!("value{}", i),
                format!("value{}", i),
            ] {
                ls.values().append_value(&value);
            }
            ls.append(true)
        }

        ls.finish()
    }

    fn generate_nested_list_data<O: OffsetSizeTrait>() -> GenericListArray<O> {
        let mut ls =
            GenericListBuilder::<O, _>::new(GenericListBuilder::<O, _>::new(UInt32Builder::new()));

        for _i in 0..10_000 {
            for j in 0..10 {
                for value in [j, j, j, j] {
                    ls.values().values().append_value(value);
                }
                ls.values().append(true)
            }
            ls.append(true);
        }

        ls.finish()
    }

    fn generate_nested_list_data_starting_at_zero<O: OffsetSizeTrait>() -> GenericListArray<O> {
        let mut ls =
            GenericListBuilder::<O, _>::new(GenericListBuilder::<O, _>::new(UInt32Builder::new()));

        for _i in 0..999 {
            ls.values().append(true);
            ls.append(true);
        }

        for j in 0..10 {
            for value in [j, j, j, j] {
                ls.values().values().append_value(value);
            }
            ls.values().append(true)
        }
        ls.append(true);

        for i in 0..9_000 {
            for j in 0..10 {
                for value in [i + j, i + j, i + j, i + j] {
                    ls.values().values().append_value(value);
                }
                ls.values().append(true)
            }
            ls.append(true);
        }

        ls.finish()
    }

    fn generate_map_array_data() -> MapArray {
        let keys_builder = UInt32Builder::new();
        let values_builder = UInt32Builder::new();

        let mut builder = MapBuilder::new(None, keys_builder, values_builder);

        for i in 0..100_000 {
            for _j in 0..3 {
                builder.keys().append_value(i);
                builder.values().append_value(i * 2);
            }
            builder.append(true).unwrap();
        }

        builder.finish()
    }

    #[test]
    fn reencode_offsets_when_first_offset_is_not_zero() {
        let original_list = generate_list_data::<i32>();
        let original_data = original_list.into_data();
        let slice_data = original_data.slice(75, 7);
        let (new_offsets, original_start, length) =
            reencode_offsets::<i32>(&slice_data.buffers()[0], &slice_data);
        assert_eq!(
            vec![0, 3, 6, 9, 12, 15, 18, 21],
            new_offsets.typed_data::<i32>()
        );
        assert_eq!(225, original_start);
        assert_eq!(21, length);
    }

    #[test]
    fn reencode_offsets_when_first_offset_is_zero() {
        let mut ls = GenericListBuilder::<i32, _>::new(UInt32Builder::new());
        // ls = [[], [35, 42]
        ls.append(true);
        ls.values().append_value(35);
        ls.values().append_value(42);
        ls.append(true);
        let original_list = ls.finish();
        let original_data = original_list.into_data();

        let slice_data = original_data.slice(1, 1);
        let (new_offsets, original_start, length) =
            reencode_offsets::<i32>(&slice_data.buffers()[0], &slice_data);
        assert_eq!(vec![0, 2], new_offsets.typed_data::<i32>());
        assert_eq!(0, original_start);
        assert_eq!(2, length);
    }

    /// Ensure when serde full & sliced versions they are equal to original input.
    /// Also ensure serialized sliced version is significantly smaller than serialized full.
    fn roundtrip_ensure_sliced_smaller(in_batch: RecordBatch, expected_size_factor: usize) {
        // test both full and sliced versions
        let in_sliced = in_batch.slice(999, 1);

        let bytes_batch = serialize_file(&in_batch);
        let bytes_sliced = serialize_file(&in_sliced);

        // serializing 1 row should be significantly smaller than serializing 100,000
        assert!(bytes_sliced.len() < (bytes_batch.len() / expected_size_factor));

        // ensure both are still valid and equal to originals
        let out_batch = deserialize_file(bytes_batch);
        assert_eq!(in_batch, out_batch);

        let out_sliced = deserialize_file(bytes_sliced);
        assert_eq!(in_sliced, out_sliced);
    }

    #[test]
    fn encode_lists() {
        let val_inner = Field::new_list_field(DataType::UInt32, true);
        let val_list_field = Field::new("val", DataType::List(Arc::new(val_inner)), false);
        let schema = Arc::new(Schema::new(vec![val_list_field]));

        let values = Arc::new(generate_list_data::<i32>());

        let in_batch = RecordBatch::try_new(schema, vec![values]).unwrap();
        roundtrip_ensure_sliced_smaller(in_batch, 1000);
    }

    #[test]
    fn encode_empty_list() {
        let val_inner = Field::new_list_field(DataType::UInt32, true);
        let val_list_field = Field::new("val", DataType::List(Arc::new(val_inner)), false);
        let schema = Arc::new(Schema::new(vec![val_list_field]));

        let values = Arc::new(generate_list_data::<i32>());

        let in_batch = RecordBatch::try_new(schema, vec![values])
            .unwrap()
            .slice(999, 0);
        let out_batch = deserialize_file(serialize_file(&in_batch));
        assert_eq!(in_batch, out_batch);
    }

    #[test]
    fn encode_large_lists() {
        let val_inner = Field::new_list_field(DataType::UInt32, true);
        let val_list_field = Field::new("val", DataType::LargeList(Arc::new(val_inner)), false);
        let schema = Arc::new(Schema::new(vec![val_list_field]));

        let values = Arc::new(generate_list_data::<i64>());

        // ensure when serde full & sliced versions they are equal to original input
        // also ensure serialized sliced version is significantly smaller than serialized full
        let in_batch = RecordBatch::try_new(schema, vec![values]).unwrap();
        roundtrip_ensure_sliced_smaller(in_batch, 1000);
    }

    #[test]
    fn encode_large_lists_non_zero_offset() {
        let val_inner = Field::new_list_field(DataType::UInt32, true);
        let val_list_field = Field::new("val", DataType::LargeList(Arc::new(val_inner)), false);
        let schema = Arc::new(Schema::new(vec![val_list_field]));

        let values = Arc::new(generate_list_data::<i64>());

        check_sliced_list_array(schema, values);
    }

    #[test]
    fn encode_large_lists_string_non_zero_offset() {
        let val_inner = Field::new_list_field(DataType::Utf8, true);
        let val_list_field = Field::new("val", DataType::LargeList(Arc::new(val_inner)), false);
        let schema = Arc::new(Schema::new(vec![val_list_field]));

        let values = Arc::new(generate_string_list_data::<i64>());

        check_sliced_list_array(schema, values);
    }

    #[test]
    fn encode_large_list_string_view_non_zero_offset() {
        let val_inner = Field::new_list_field(DataType::Utf8View, true);
        let val_list_field = Field::new("val", DataType::LargeList(Arc::new(val_inner)), false);
        let schema = Arc::new(Schema::new(vec![val_list_field]));

        let values = Arc::new(generate_utf8view_list_data::<i64>());

        check_sliced_list_array(schema, values);
    }

    fn check_sliced_list_array(schema: Arc<Schema>, values: Arc<GenericListArray<i64>>) {
        for (offset, len) in [(999, 1), (0, 13), (47, 12), (values.len() - 13, 13)] {
            let in_batch = RecordBatch::try_new(schema.clone(), vec![values.clone()])
                .unwrap()
                .slice(offset, len);
            let out_batch = deserialize_file(serialize_file(&in_batch));
            assert_eq!(in_batch, out_batch);
        }
    }

    #[test]
    fn encode_nested_lists() {
        let inner_int = Arc::new(Field::new_list_field(DataType::UInt32, true));
        let inner_list_field = Arc::new(Field::new_list_field(DataType::List(inner_int), true));
        let list_field = Field::new("val", DataType::List(inner_list_field), true);
        let schema = Arc::new(Schema::new(vec![list_field]));

        let values = Arc::new(generate_nested_list_data::<i32>());

        let in_batch = RecordBatch::try_new(schema, vec![values]).unwrap();
        roundtrip_ensure_sliced_smaller(in_batch, 1000);
    }

    #[test]
    fn encode_nested_lists_starting_at_zero() {
        let inner_int = Arc::new(Field::new("item", DataType::UInt32, true));
        let inner_list_field = Arc::new(Field::new("item", DataType::List(inner_int), true));
        let list_field = Field::new("val", DataType::List(inner_list_field), true);
        let schema = Arc::new(Schema::new(vec![list_field]));

        let values = Arc::new(generate_nested_list_data_starting_at_zero::<i32>());

        let in_batch = RecordBatch::try_new(schema, vec![values]).unwrap();
        roundtrip_ensure_sliced_smaller(in_batch, 1);
    }

    #[test]
    fn encode_map_array() {
        let keys = Arc::new(Field::new("keys", DataType::UInt32, false));
        let values = Arc::new(Field::new("values", DataType::UInt32, true));
        let map_field = Field::new_map("map", "entries", keys, values, false, true);
        let schema = Arc::new(Schema::new(vec![map_field]));

        let values = Arc::new(generate_map_array_data());

        let in_batch = RecordBatch::try_new(schema, vec![values]).unwrap();
        roundtrip_ensure_sliced_smaller(in_batch, 1000);
    }

    fn generate_list_view_data<O: OffsetSizeTrait>() -> GenericListViewArray<O> {
        let mut builder = GenericListViewBuilder::<O, _>::new(UInt32Builder::new());

        for i in 0u32..100_000 {
            if i.is_multiple_of(10_000) {
                builder.append(false);
                continue;
            }
            for value in [i, i, i] {
                builder.values().append_value(value);
            }
            builder.append(true);
        }

        builder.finish()
    }

    #[test]
    fn encode_list_view_arrays() {
        let val_inner = Field::new_list_field(DataType::UInt32, true);
        let val_field = Field::new("val", DataType::ListView(Arc::new(val_inner)), true);
        let schema = Arc::new(Schema::new(vec![val_field]));

        let values = Arc::new(generate_list_view_data::<i32>());

        let in_batch = RecordBatch::try_new(schema, vec![values]).unwrap();
        let out_batch = deserialize_file(serialize_file(&in_batch));
        assert_eq!(in_batch, out_batch);
    }

    #[test]
    fn encode_large_list_view_arrays() {
        let val_inner = Field::new_list_field(DataType::UInt32, true);
        let val_field = Field::new("val", DataType::LargeListView(Arc::new(val_inner)), true);
        let schema = Arc::new(Schema::new(vec![val_field]));

        let values = Arc::new(generate_list_view_data::<i64>());

        let in_batch = RecordBatch::try_new(schema, vec![values]).unwrap();
        let out_batch = deserialize_file(serialize_file(&in_batch));
        assert_eq!(in_batch, out_batch);
    }

    #[test]
    fn check_sliced_list_view_array() {
        let inner = Field::new_list_field(DataType::UInt32, true);
        let field = Field::new("val", DataType::ListView(Arc::new(inner)), true);
        let schema = Arc::new(Schema::new(vec![field]));
        let values = Arc::new(generate_list_view_data::<i32>());

        for (offset, len) in [(999, 1), (0, 13), (47, 12), (values.len() - 13, 13)] {
            let in_batch = RecordBatch::try_new(schema.clone(), vec![values.clone()])
                .unwrap()
                .slice(offset, len);
            let out_batch = deserialize_file(serialize_file(&in_batch));
            assert_eq!(in_batch, out_batch);
        }
    }

    #[test]
    fn check_sliced_large_list_view_array() {
        let inner = Field::new_list_field(DataType::UInt32, true);
        let field = Field::new("val", DataType::LargeListView(Arc::new(inner)), true);
        let schema = Arc::new(Schema::new(vec![field]));
        let values = Arc::new(generate_list_view_data::<i64>());

        for (offset, len) in [(999, 1), (0, 13), (47, 12), (values.len() - 13, 13)] {
            let in_batch = RecordBatch::try_new(schema.clone(), vec![values.clone()])
                .unwrap()
                .slice(offset, len);
            let out_batch = deserialize_file(serialize_file(&in_batch));
            assert_eq!(in_batch, out_batch);
        }
    }

    fn generate_nested_list_view_data<O: OffsetSizeTrait>() -> GenericListViewArray<O> {
        let inner_builder = UInt32Builder::new();
        let middle_builder = GenericListViewBuilder::<O, _>::new(inner_builder);
        let mut outer_builder = GenericListViewBuilder::<O, _>::new(middle_builder);

        for i in 0u32..10_000 {
            if i.is_multiple_of(1_000) {
                outer_builder.append(false);
                continue;
            }

            for _ in 0..3 {
                for value in [i, i + 1, i + 2] {
                    outer_builder.values().values().append_value(value);
                }
                outer_builder.values().append(true);
            }
            outer_builder.append(true);
        }

        outer_builder.finish()
    }

    #[test]
    fn encode_nested_list_views() {
        let inner_int = Arc::new(Field::new_list_field(DataType::UInt32, true));
        let inner_list_field = Arc::new(Field::new_list_field(DataType::ListView(inner_int), true));
        let list_field = Field::new("val", DataType::ListView(inner_list_field), true);
        let schema = Arc::new(Schema::new(vec![list_field]));

        let values = Arc::new(generate_nested_list_view_data::<i32>());

        let in_batch = RecordBatch::try_new(schema, vec![values]).unwrap();
        let out_batch = deserialize_file(serialize_file(&in_batch));
        assert_eq!(in_batch, out_batch);
    }

    fn test_roundtrip_list_view_of_dict_impl<OffsetSize: OffsetSizeTrait, U: ArrowNativeType>(
        list_data_type: DataType,
        offsets: &[U; 5],
        sizes: &[U; 4],
    ) {
        let values = StringArray::from(vec![Some("alpha"), None, Some("beta"), Some("gamma")]);
        let keys = Int32Array::from_iter_values([0, 0, 1, 2, 3, 0, 2]);
        let dict_array = DictionaryArray::new(keys, Arc::new(values));
        let dict_data = dict_array.to_data();

        let value_offsets = Buffer::from_slice_ref(offsets);
        let value_sizes = Buffer::from_slice_ref(sizes);

        let list_data = ArrayData::builder(list_data_type)
            .len(4)
            .add_buffer(value_offsets)
            .add_buffer(value_sizes)
            .add_child_data(dict_data)
            .build()
            .unwrap();
        let list_view_array = GenericListViewArray::<OffsetSize>::from(list_data);

        let schema = Arc::new(Schema::new(vec![Field::new(
            "f1",
            list_view_array.data_type().clone(),
            false,
        )]));
        let input_batch = RecordBatch::try_new(schema, vec![Arc::new(list_view_array)]).unwrap();

        let output_batch = deserialize_file(serialize_file(&input_batch));
        assert_eq!(input_batch, output_batch);

        let output_batch = deserialize_stream(serialize_stream(&input_batch));
        assert_eq!(input_batch, output_batch);
    }

    #[test]
    fn test_roundtrip_list_view_of_dict() {
        #[allow(deprecated)]
        let list_data_type = DataType::ListView(Arc::new(Field::new_dict(
            "item",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
            1,
            false,
        )));
        let offsets: &[i32; 5] = &[0, 2, 4, 4, 7];
        let sizes: &[i32; 4] = &[2, 2, 0, 3];
        test_roundtrip_list_view_of_dict_impl::<i32, i32>(list_data_type, offsets, sizes);
    }

    #[test]
    fn test_roundtrip_large_list_view_of_dict() {
        #[allow(deprecated)]
        let list_data_type = DataType::LargeListView(Arc::new(Field::new_dict(
            "item",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
            2,
            false,
        )));
        let offsets: &[i64; 5] = &[0, 2, 4, 4, 7];
        let sizes: &[i64; 4] = &[2, 2, 0, 3];
        test_roundtrip_list_view_of_dict_impl::<i64, i64>(list_data_type, offsets, sizes);
    }

    #[test]
    fn test_roundtrip_sliced_list_view_of_dict() {
        #[allow(deprecated)]
        let list_data_type = DataType::ListView(Arc::new(Field::new_dict(
            "item",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
            3,
            false,
        )));

        let values = StringArray::from(vec![Some("alpha"), None, Some("beta"), Some("gamma")]);
        let keys = Int32Array::from_iter_values([0, 0, 1, 2, 3, 0, 2, 1, 0, 3, 2, 1]);
        let dict_array = DictionaryArray::new(keys, Arc::new(values));
        let dict_data = dict_array.to_data();

        let offsets: &[i32; 7] = &[0, 2, 4, 4, 7, 9, 12];
        let sizes: &[i32; 6] = &[2, 2, 0, 3, 2, 3];
        let value_offsets = Buffer::from_slice_ref(offsets);
        let value_sizes = Buffer::from_slice_ref(sizes);

        let list_data = ArrayData::builder(list_data_type)
            .len(6)
            .add_buffer(value_offsets)
            .add_buffer(value_sizes)
            .add_child_data(dict_data)
            .build()
            .unwrap();
        let list_view_array = GenericListViewArray::<i32>::from(list_data);

        let schema = Arc::new(Schema::new(vec![Field::new(
            "f1",
            list_view_array.data_type().clone(),
            false,
        )]));
        let input_batch = RecordBatch::try_new(schema, vec![Arc::new(list_view_array)]).unwrap();

        let sliced_batch = input_batch.slice(1, 4);

        let output_batch = deserialize_file(serialize_file(&sliced_batch));
        assert_eq!(sliced_batch, output_batch);

        let output_batch = deserialize_stream(serialize_stream(&sliced_batch));
        assert_eq!(sliced_batch, output_batch);
    }

    #[test]
    fn test_roundtrip_dense_union_of_dict() {
        let values = StringArray::from(vec![Some("alpha"), None, Some("beta"), Some("gamma")]);
        let keys = Int32Array::from_iter_values([0, 0, 1, 2, 3, 0, 2]);
        let dict_array = DictionaryArray::new(keys, Arc::new(values));

        #[allow(deprecated)]
        let dict_field = Arc::new(Field::new_dict(
            "dict",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
            1,
            false,
        ));
        let int_field = Arc::new(Field::new("int", DataType::Int32, false));
        let union_fields = UnionFields::try_new(vec![0, 1], vec![dict_field, int_field]).unwrap();

        let types = ScalarBuffer::from(vec![0i8, 0, 1, 0, 1, 0, 0]);
        let offsets = ScalarBuffer::from(vec![0i32, 1, 0, 2, 1, 3, 4]);

        let int_array = Int32Array::from(vec![100, 200]);

        let union = UnionArray::try_new(
            union_fields.clone(),
            types,
            Some(offsets),
            vec![Arc::new(dict_array), Arc::new(int_array)],
        )
        .unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "union",
            DataType::Union(union_fields, UnionMode::Dense),
            false,
        )]));
        let input_batch = RecordBatch::try_new(schema, vec![Arc::new(union)]).unwrap();

        let output_batch = deserialize_file(serialize_file(&input_batch));
        assert_eq!(input_batch, output_batch);

        let output_batch = deserialize_stream(serialize_stream(&input_batch));
        assert_eq!(input_batch, output_batch);
    }

    #[test]
    fn test_roundtrip_sparse_union_of_dict() {
        let values = StringArray::from(vec![Some("alpha"), None, Some("beta"), Some("gamma")]);
        let keys = Int32Array::from_iter_values([0, 0, 1, 2, 3, 0, 2]);
        let dict_array = DictionaryArray::new(keys, Arc::new(values));

        #[allow(deprecated)]
        let dict_field = Arc::new(Field::new_dict(
            "dict",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
            2,
            false,
        ));
        let int_field = Arc::new(Field::new("int", DataType::Int32, false));
        let union_fields = UnionFields::try_new(vec![0, 1], vec![dict_field, int_field]).unwrap();

        let types = ScalarBuffer::from(vec![0i8, 0, 1, 0, 1, 0, 0]);

        let int_array = Int32Array::from(vec![0, 0, 100, 0, 200, 0, 0]);

        let union = UnionArray::try_new(
            union_fields.clone(),
            types,
            None,
            vec![Arc::new(dict_array), Arc::new(int_array)],
        )
        .unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "union",
            DataType::Union(union_fields, UnionMode::Sparse),
            false,
        )]));
        let input_batch = RecordBatch::try_new(schema, vec![Arc::new(union)]).unwrap();

        let output_batch = deserialize_file(serialize_file(&input_batch));
        assert_eq!(input_batch, output_batch);

        let output_batch = deserialize_stream(serialize_stream(&input_batch));
        assert_eq!(input_batch, output_batch);
    }

    #[test]
    fn test_roundtrip_map_with_dict_keys() {
        // Building a map array is a bit involved. We first build a struct arary that has a key and
        // value field and then use that to build the actual map array.
        let key_values = StringArray::from(vec!["key_a", "key_b", "key_c"]);
        let keys = Int32Array::from_iter_values([0, 1, 2, 0, 1, 0]);
        let dict_keys = DictionaryArray::new(keys, Arc::new(key_values));

        let values = Int32Array::from(vec![1, 2, 3, 4, 5, 6]);

        #[allow(deprecated)]
        let entries_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new_dict(
                        "key",
                        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                        false,
                        1,
                        false,
                    ),
                    Field::new("value", DataType::Int32, true),
                ]
                .into(),
            ),
            false,
        ));

        let entries = StructArray::from(vec![
            (
                Arc::new(Field::new(
                    "key",
                    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                    false,
                )),
                Arc::new(dict_keys) as ArrayRef,
            ),
            (
                Arc::new(Field::new("value", DataType::Int32, true)),
                Arc::new(values) as ArrayRef,
            ),
        ]);

        let offsets = Buffer::from_slice_ref([0i32, 2, 4, 6]);

        let map_data = ArrayData::builder(DataType::Map(entries_field, false))
            .len(3)
            .add_buffer(offsets)
            .add_child_data(entries.into_data())
            .build()
            .unwrap();
        let map_array = MapArray::from(map_data);

        let schema = Arc::new(Schema::new(vec![Field::new(
            "map",
            map_array.data_type().clone(),
            false,
        )]));
        let input_batch = RecordBatch::try_new(schema, vec![Arc::new(map_array)]).unwrap();

        let output_batch = deserialize_file(serialize_file(&input_batch));
        assert_eq!(input_batch, output_batch);

        let output_batch = deserialize_stream(serialize_stream(&input_batch));
        assert_eq!(input_batch, output_batch);
    }

    #[test]
    fn test_roundtrip_map_with_dict_values() {
        // Building a map array is a bit involved. We first build a struct arary that has a key and
        // value field and then use that to build the actual map array.
        let keys = StringArray::from(vec!["a", "b", "c", "d", "e", "f"]);

        let value_values = StringArray::from(vec!["val_x", "val_y", "val_z"]);
        let value_keys = Int32Array::from_iter_values([0, 1, 2, 0, 1, 0]);
        let dict_values = DictionaryArray::new(value_keys, Arc::new(value_values));

        #[allow(deprecated)]
        let entries_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new_dict(
                        "value",
                        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                        true,
                        2,
                        false,
                    ),
                ]
                .into(),
            ),
            false,
        ));

        let entries = StructArray::from(vec![
            (
                Arc::new(Field::new("key", DataType::Utf8, false)),
                Arc::new(keys) as ArrayRef,
            ),
            (
                Arc::new(Field::new(
                    "value",
                    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                    true,
                )),
                Arc::new(dict_values) as ArrayRef,
            ),
        ]);

        let offsets = Buffer::from_slice_ref([0i32, 2, 4, 6]);

        let map_data = ArrayData::builder(DataType::Map(entries_field, false))
            .len(3)
            .add_buffer(offsets)
            .add_child_data(entries.into_data())
            .build()
            .unwrap();
        let map_array = MapArray::from(map_data);

        let schema = Arc::new(Schema::new(vec![Field::new(
            "map",
            map_array.data_type().clone(),
            false,
        )]));
        let input_batch = RecordBatch::try_new(schema, vec![Arc::new(map_array)]).unwrap();

        let output_batch = deserialize_file(serialize_file(&input_batch));
        assert_eq!(input_batch, output_batch);

        let output_batch = deserialize_stream(serialize_stream(&input_batch));
        assert_eq!(input_batch, output_batch);
    }

    #[test]
    fn test_decimal128_alignment16_is_sufficient() {
        const IPC_ALIGNMENT: usize = 16;

        // Test a bunch of different dimensions to ensure alignment is never an issue.
        // For example, if we only test `num_cols = 1` then even with alignment 8 this
        // test would _happen_ to pass, even though for different dimensions like
        // `num_cols = 2` it would fail.
        for num_cols in [1, 2, 3, 17, 50, 73, 99] {
            let num_rows = (num_cols * 7 + 11) % 100; // Deterministic swizzle

            let mut fields = Vec::new();
            let mut arrays = Vec::new();
            for i in 0..num_cols {
                let field = Field::new(format!("col_{i}"), DataType::Decimal128(38, 10), true);
                let array = Decimal128Array::from(vec![num_cols as i128; num_rows]);
                fields.push(field);
                arrays.push(Arc::new(array) as Arc<dyn Array>);
            }
            let schema = Schema::new(fields);
            let batch = RecordBatch::try_new(Arc::new(schema), arrays).unwrap();

            let mut writer = FileWriter::try_new_with_options(
                Vec::new(),
                batch.schema_ref(),
                IpcWriteOptions::try_new(IPC_ALIGNMENT, false, MetadataVersion::V5).unwrap(),
            )
            .unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();

            let out: Vec<u8> = writer.into_inner().unwrap();

            let buffer = Buffer::from_vec(out);
            let trailer_start = buffer.len() - 10;
            let footer_len =
                read_footer_length(buffer[trailer_start..].try_into().unwrap()).unwrap();
            let footer =
                root_as_footer(&buffer[trailer_start - footer_len..trailer_start]).unwrap();

            let schema = fb_to_schema(footer.schema().unwrap());

            // Importantly we set `require_alignment`, checking that 16-byte alignment is sufficient
            // for `read_record_batch` later on to read the data in a zero-copy manner.
            let decoder =
                FileDecoder::new(Arc::new(schema), footer.version()).with_require_alignment(true);

            let batches = footer.recordBatches().unwrap();

            let block = batches.get(0);
            let block_len = block.bodyLength() as usize + block.metaDataLength() as usize;
            let data = buffer.slice_with_length(block.offset() as _, block_len);

            let batch2 = decoder.read_record_batch(block, &data).unwrap().unwrap();

            assert_eq!(batch, batch2);
        }
    }

    #[test]
    fn test_decimal128_alignment8_is_unaligned() {
        const IPC_ALIGNMENT: usize = 8;

        let num_cols = 2;
        let num_rows = 1;

        let mut fields = Vec::new();
        let mut arrays = Vec::new();
        for i in 0..num_cols {
            let field = Field::new(format!("col_{i}"), DataType::Decimal128(38, 10), true);
            let array = Decimal128Array::from(vec![num_cols as i128; num_rows]);
            fields.push(field);
            arrays.push(Arc::new(array) as Arc<dyn Array>);
        }
        let schema = Schema::new(fields);
        let batch = RecordBatch::try_new(Arc::new(schema), arrays).unwrap();

        let mut writer = FileWriter::try_new_with_options(
            Vec::new(),
            batch.schema_ref(),
            IpcWriteOptions::try_new(IPC_ALIGNMENT, false, MetadataVersion::V5).unwrap(),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();

        let out: Vec<u8> = writer.into_inner().unwrap();

        let buffer = Buffer::from_vec(out);
        let trailer_start = buffer.len() - 10;
        let footer_len = read_footer_length(buffer[trailer_start..].try_into().unwrap()).unwrap();
        let footer = root_as_footer(&buffer[trailer_start - footer_len..trailer_start]).unwrap();
        let schema = fb_to_schema(footer.schema().unwrap());

        // Importantly we set `require_alignment`, otherwise the error later is suppressed due to copying
        // to an aligned buffer in `ArrayDataBuilder.build_aligned`.
        let decoder =
            FileDecoder::new(Arc::new(schema), footer.version()).with_require_alignment(true);

        let batches = footer.recordBatches().unwrap();

        let block = batches.get(0);
        let block_len = block.bodyLength() as usize + block.metaDataLength() as usize;
        let data = buffer.slice_with_length(block.offset() as _, block_len);

        let result = decoder.read_record_batch(block, &data);

        let error = result.unwrap_err();
        assert_eq!(
            error.to_string(),
            "Invalid argument error: Misaligned buffers[0] in array of type Decimal128(38, 10), \
             offset from expected alignment of 16 by 8"
        );
    }

    #[test]
    fn test_flush() {
        // We write a schema which is small enough to fit into a buffer and not get flushed,
        // and then force the write with .flush().
        let num_cols = 2;
        let mut fields = Vec::new();
        let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5).unwrap();
        for i in 0..num_cols {
            let field = Field::new(format!("col_{i}"), DataType::Decimal128(38, 10), true);
            fields.push(field);
        }
        let schema = Schema::new(fields);
        let inner_stream_writer = BufWriter::with_capacity(1024, Vec::new());
        let inner_file_writer = BufWriter::with_capacity(1024, Vec::new());
        let mut stream_writer =
            StreamWriter::try_new_with_options(inner_stream_writer, &schema, options.clone())
                .unwrap();
        let mut file_writer =
            FileWriter::try_new_with_options(inner_file_writer, &schema, options).unwrap();

        let stream_bytes_written_on_new = stream_writer.get_ref().get_ref().len();
        let file_bytes_written_on_new = file_writer.get_ref().get_ref().len();
        stream_writer.flush().unwrap();
        file_writer.flush().unwrap();
        let stream_bytes_written_on_flush = stream_writer.get_ref().get_ref().len();
        let file_bytes_written_on_flush = file_writer.get_ref().get_ref().len();
        let stream_out = stream_writer.into_inner().unwrap().into_inner().unwrap();
        // Finishing a stream writes the continuation bytes in MetadataVersion::V5 (4 bytes)
        // and then a length of 0 (4 bytes) for a total of 8 bytes.
        // Everything before that should have been flushed in the .flush() call.
        let expected_stream_flushed_bytes = stream_out.len() - 8;
        // A file write is the same as the stream write except for the leading magic string
        // ARROW1 plus padding, which is 8 bytes.
        let expected_file_flushed_bytes = expected_stream_flushed_bytes + 8;

        assert!(
            stream_bytes_written_on_new < stream_bytes_written_on_flush,
            "this test makes no sense if flush is not actually required"
        );
        assert!(
            file_bytes_written_on_new < file_bytes_written_on_flush,
            "this test makes no sense if flush is not actually required"
        );
        assert_eq!(stream_bytes_written_on_flush, expected_stream_flushed_bytes);
        assert_eq!(file_bytes_written_on_flush, expected_file_flushed_bytes);
    }

    #[test]
    fn test_roundtrip_list_of_fixed_list() -> Result<(), ArrowError> {
        let l1_type =
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 3);
        let l2_type = DataType::List(Arc::new(Field::new("item", l1_type.clone(), false)));

        let l0_builder = Float32Builder::new();
        let l1_builder = FixedSizeListBuilder::new(l0_builder, 3).with_field(Arc::new(Field::new(
            "item",
            DataType::Float32,
            false,
        )));
        let mut l2_builder =
            ListBuilder::new(l1_builder).with_field(Arc::new(Field::new("item", l1_type, false)));

        for point in [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]] {
            l2_builder.values().values().append_value(point[0]);
            l2_builder.values().values().append_value(point[1]);
            l2_builder.values().values().append_value(point[2]);

            l2_builder.values().append(true);
        }
        l2_builder.append(true);

        let point = [10., 11., 12.];
        l2_builder.values().values().append_value(point[0]);
        l2_builder.values().values().append_value(point[1]);
        l2_builder.values().values().append_value(point[2]);

        l2_builder.values().append(true);
        l2_builder.append(true);

        let array = Arc::new(l2_builder.finish()) as ArrayRef;

        let schema = Arc::new(Schema::new_with_metadata(
            vec![Field::new("points", l2_type, false)],
            HashMap::default(),
        ));

        // Test a variety of combinations that include 0 and non-zero offsets
        // and also portions or the rest of the array
        test_slices(&array, &schema, 0, 1)?;
        test_slices(&array, &schema, 0, 2)?;
        test_slices(&array, &schema, 1, 1)?;

        Ok(())
    }

    #[test]
    fn test_roundtrip_list_of_fixed_list_w_nulls() -> Result<(), ArrowError> {
        let l0_builder = Float32Builder::new();
        let l1_builder = FixedSizeListBuilder::new(l0_builder, 3);
        let mut l2_builder = ListBuilder::new(l1_builder);

        for point in [
            [Some(1.0), Some(2.0), None],
            [Some(4.0), Some(5.0), Some(6.0)],
            [None, Some(8.0), Some(9.0)],
        ] {
            for p in point {
                match p {
                    Some(p) => l2_builder.values().values().append_value(p),
                    None => l2_builder.values().values().append_null(),
                }
            }

            l2_builder.values().append(true);
        }
        l2_builder.append(true);

        let point = [Some(10.), None, None];
        for p in point {
            match p {
                Some(p) => l2_builder.values().values().append_value(p),
                None => l2_builder.values().values().append_null(),
            }
        }

        l2_builder.values().append(true);
        l2_builder.append(true);

        let array = Arc::new(l2_builder.finish()) as ArrayRef;

        let schema = Arc::new(Schema::new_with_metadata(
            vec![Field::new(
                "points",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::FixedSizeList(
                        Arc::new(Field::new("item", DataType::Float32, true)),
                        3,
                    ),
                    true,
                ))),
                true,
            )],
            HashMap::default(),
        ));

        // Test a variety of combinations that include 0 and non-zero offsets
        // and also portions or the rest of the array
        test_slices(&array, &schema, 0, 1)?;
        test_slices(&array, &schema, 0, 2)?;
        test_slices(&array, &schema, 1, 1)?;

        Ok(())
    }

    fn test_slices(
        parent_array: &ArrayRef,
        schema: &SchemaRef,
        offset: usize,
        length: usize,
    ) -> Result<(), ArrowError> {
        let subarray = parent_array.slice(offset, length);
        let original_batch = RecordBatch::try_new(schema.clone(), vec![subarray])?;

        let mut bytes = Vec::new();
        let mut writer = StreamWriter::try_new(&mut bytes, schema)?;
        writer.write(&original_batch)?;
        writer.finish()?;

        let mut cursor = std::io::Cursor::new(bytes);
        let mut reader = StreamReader::try_new(&mut cursor, None)?;
        let returned_batch = reader.next().unwrap()?;

        assert_eq!(original_batch, returned_batch);

        Ok(())
    }

    #[test]
    fn test_roundtrip_fixed_list() -> Result<(), ArrowError> {
        let int_builder = Int64Builder::new();
        let mut fixed_list_builder = FixedSizeListBuilder::new(int_builder, 3)
            .with_field(Arc::new(Field::new("item", DataType::Int64, false)));

        for point in [[1, 2, 3], [4, 5, 6], [7, 8, 9], [10, 11, 12]] {
            fixed_list_builder.values().append_value(point[0]);
            fixed_list_builder.values().append_value(point[1]);
            fixed_list_builder.values().append_value(point[2]);

            fixed_list_builder.append(true);
        }

        let array = Arc::new(fixed_list_builder.finish()) as ArrayRef;

        let schema = Arc::new(Schema::new_with_metadata(
            vec![Field::new(
                "points",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int64, false)), 3),
                false,
            )],
            HashMap::default(),
        ));

        // Test a variety of combinations that include 0 and non-zero offsets
        // and also portions or the rest of the array
        test_slices(&array, &schema, 0, 4)?;
        test_slices(&array, &schema, 0, 2)?;
        test_slices(&array, &schema, 1, 3)?;
        test_slices(&array, &schema, 2, 1)?;

        Ok(())
    }

    #[test]
    fn test_roundtrip_fixed_list_w_nulls() -> Result<(), ArrowError> {
        let int_builder = Int64Builder::new();
        let mut fixed_list_builder = FixedSizeListBuilder::new(int_builder, 3);

        for point in [
            [Some(1), Some(2), None],
            [Some(4), Some(5), Some(6)],
            [None, Some(8), Some(9)],
            [Some(10), None, None],
        ] {
            for p in point {
                match p {
                    Some(p) => fixed_list_builder.values().append_value(p),
                    None => fixed_list_builder.values().append_null(),
                }
            }

            fixed_list_builder.append(true);
        }

        let array = Arc::new(fixed_list_builder.finish()) as ArrayRef;

        let schema = Arc::new(Schema::new_with_metadata(
            vec![Field::new(
                "points",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int64, true)), 3),
                true,
            )],
            HashMap::default(),
        ));

        // Test a variety of combinations that include 0 and non-zero offsets
        // and also portions or the rest of the array
        test_slices(&array, &schema, 0, 4)?;
        test_slices(&array, &schema, 0, 2)?;
        test_slices(&array, &schema, 1, 3)?;
        test_slices(&array, &schema, 2, 1)?;

        Ok(())
    }

    #[test]
    fn test_metadata_encoding_ordering() {
        fn create_hash() -> u64 {
            let metadata: HashMap<String, String> = [
                ("a", "1"), //
                ("b", "2"), //
                ("c", "3"), //
                ("d", "4"), //
                ("e", "5"), //
            ]
            .into_iter()
            .map(|(k, v)| (k.to_owned(), v.to_owned()))
            .collect();

            // Set metadata on both the schema and a field within it.
            let schema = Arc::new(
                Schema::new(vec![
                    Field::new("a", DataType::Int64, true).with_metadata(metadata.clone()),
                ])
                .with_metadata(metadata)
                .clone(),
            );
            let batch = RecordBatch::new_empty(schema.clone());

            let mut bytes = Vec::new();
            let mut w = StreamWriter::try_new(&mut bytes, batch.schema_ref()).unwrap();
            w.write(&batch).unwrap();
            w.finish().unwrap();

            let mut h = std::hash::DefaultHasher::new();
            h.write(&bytes);
            h.finish()
        }

        let expected = create_hash();

        // Since there is randomness in the HashMap and we cannot specify our
        // own Hasher for the implementation used for metadata, run the above
        // code 20x and verify it does not change. This is not perfect but it
        // should be good enough.
        let all_passed = (0..20).all(|_| create_hash() == expected);
        assert!(all_passed);
    }

    #[test]
    fn test_dictionary_tracker_reset() {
        let data_gen = IpcDataGenerator::default();
        let mut dictionary_tracker = DictionaryTracker::new(false);
        let writer_options = IpcWriteOptions::default();
        let mut compression_ctx = IpcWriteContext::default();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "a",
            DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8)),
            false,
        )]));

        let mut write_single_batch_stream =
            |batch: RecordBatch, dict_tracker: &mut DictionaryTracker| -> Vec<u8> {
                let mut buffer = Vec::new();

                // create a new IPC stream:
                let stream_header = data_gen.schema_to_bytes_with_dictionary_tracker(
                    &schema,
                    dict_tracker,
                    &writer_options,
                );
                _ = write_message(&mut buffer, stream_header, &writer_options).unwrap();

                let (encoded_dicts, encoded_batch) = data_gen
                    .encode(&batch, dict_tracker, &writer_options, &mut compression_ctx)
                    .unwrap();
                for encoded_dict in encoded_dicts {
                    _ = write_message(&mut buffer, encoded_dict, &writer_options).unwrap();
                }
                _ = write_message(&mut buffer, encoded_batch, &writer_options).unwrap();

                buffer
            };

        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(DictionaryArray::new(
                UInt8Array::from_iter_values([0]),
                Arc::new(StringArray::from_iter_values(["a"])),
            ))],
        )
        .unwrap();
        let buffer = write_single_batch_stream(batch1.clone(), &mut dictionary_tracker);

        // ensure we can read the stream back
        let mut reader = StreamReader::try_new(Cursor::new(buffer), None).unwrap();
        let read_batch = reader.next().unwrap().unwrap();
        assert_eq!(read_batch, batch1);

        // reset the dictionary tracker so it can be used for next stream
        dictionary_tracker.clear();

        // now write a 2nd stream and ensure we can also read it:
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(DictionaryArray::new(
                UInt8Array::from_iter_values([0]),
                Arc::new(StringArray::from_iter_values(["a"])),
            ))],
        )
        .unwrap();
        let buffer = write_single_batch_stream(batch2.clone(), &mut dictionary_tracker);
        let mut reader = StreamReader::try_new(Cursor::new(buffer), None).unwrap();
        let read_batch = reader.next().unwrap().unwrap();
        assert_eq!(read_batch, batch2);
    }
}
