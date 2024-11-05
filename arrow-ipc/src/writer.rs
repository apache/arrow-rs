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

use std::{
    borrow::Borrow,
    cmp::min,
    collections::HashMap,
    io::{BufWriter, Write},
    sync::Arc,
};

use flatbuffers::{FlatBufferBuilder, UnionWIPOffset, WIPOffset};

use arrow_array::builder::BufferBuilder;
use arrow_array::cast::*;
use arrow_array::types::{Int16Type, Int32Type, Int64Type, RunEndIndexType};
use arrow_array::*;
use arrow_buffer::bit_util;
use arrow_buffer::{ArrowNativeType, Buffer, MutableBuffer};
use arrow_data::{layout, ArrayData, ArrayDataBuilder, BufferSpec};
use arrow_schema::*;

use crate::compression::CompressionCodec;
use crate::convert::IpcSchemaEncoder;
use crate::{
    BodyCompressionBuilder, BodyCompressionMethod, DictionaryBatchBuilder, MessageBuilder,
    MessageHeader, RecordBatchBuilder, CONTINUATION_MARKER,
};

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
    /// Flag indicating whether the writer should preserve the dictionary IDs defined in the
    /// schema or generate unique dictionary IDs internally during encoding.
    ///
    /// Defaults to `true`
    preserve_dict_id: bool,
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
            crate::MetadataVersion::V4 => Ok(Self {
                alignment,
                write_legacy_ipc_format,
                metadata_version,
                batch_compression_type: None,
                preserve_dict_id: true,
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
                        preserve_dict_id: true,
                    })
                }
            }
            z => Err(ArrowError::InvalidArgumentError(format!(
                "Unsupported crate::MetadataVersion {z:?}"
            ))),
        }
    }

    /// Return whether the writer is configured to preserve the dictionary IDs
    /// defined in the schema
    pub fn preserve_dict_id(&self) -> bool {
        self.preserve_dict_id
    }

    /// Set whether the IPC writer should preserve the dictionary IDs in the schema
    /// or auto-assign unique dictionary IDs during encoding (defaults to true)
    ///
    /// If this option is true,  the application must handle assigning ids
    /// to the dictionary batches in order to encode them correctly
    ///
    /// The default will change to `false`  in future releases
    pub fn with_preserve_dict_id(mut self, preserve_dict_id: bool) -> Self {
        self.preserve_dict_id = preserve_dict_id;
        self
    }
}

impl Default for IpcWriteOptions {
    fn default() -> Self {
        Self {
            alignment: DEFAULT_ALIGNMENT,
            write_legacy_ipc_format: false,
            metadata_version: crate::MetadataVersion::V5,
            batch_compression_type: None,
            preserve_dict_id: true,
        }
    }
}

#[derive(Debug, Default)]
/// Handles low level details of encoding [`Array`] and [`Schema`] into the
/// [Arrow IPC Format].
///
/// # Example:
/// ```
/// # fn run() {
/// # use std::sync::Arc;
/// # use arrow_array::UInt64Array;
/// # use arrow_array::RecordBatch;
/// # use arrow_ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions};
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
/// // encode the batch into zero or more encoded dictionaries
/// // and the data for the actual array.
/// let data_gen = IpcDataGenerator::default();
/// let (encoded_dictionaries, encoded_message) = data_gen
///   .encoded_batch(&batch, &mut dictionary_tracker, &options)
///   .unwrap();
/// # }
/// ```
///
/// [Arrow IPC Format]: https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc
pub struct IpcDataGenerator {}

impl IpcDataGenerator {
    /// Converts a schema to an IPC message along with `dictionary_tracker`
    /// and returns it encoded inside [EncodedData] as a flatbuffer
    ///
    /// Preferred method over [IpcDataGenerator::schema_to_bytes] since it's
    /// deprecated since Arrow v54.0.0
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

    #[deprecated(
        since = "54.0.0",
        note = "Use `schema_to_bytes_with_dictionary_tracker` instead. This function signature of `schema_to_bytes_with_dictionary_tracker` in the next release."
    )]
    /// Converts a schema to an IPC message and returns it encoded inside [EncodedData] as a flatbuffer
    pub fn schema_to_bytes(&self, schema: &Schema, write_options: &IpcWriteOptions) -> EncodedData {
        let mut fbb = FlatBufferBuilder::new();
        let schema = {
            #[allow(deprecated)]
            // This will be replaced with the IpcSchemaConverter in the next release.
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

    fn _encode_dictionaries<I: Iterator<Item = i64>>(
        &self,
        column: &ArrayRef,
        encoded_dictionaries: &mut Vec<EncodedData>,
        dictionary_tracker: &mut DictionaryTracker,
        write_options: &IpcWriteOptions,
        dict_id: &mut I,
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
                )?;

                // values
                self.encode_dictionaries(
                    values,
                    map_array.values(),
                    encoded_dictionaries,
                    dictionary_tracker,
                    write_options,
                    dict_id,
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
                    )?;
                }
            }
            _ => (),
        }

        Ok(())
    }

    fn encode_dictionaries<I: Iterator<Item = i64>>(
        &self,
        field: &Field,
        column: &ArrayRef,
        encoded_dictionaries: &mut Vec<EncodedData>,
        dictionary_tracker: &mut DictionaryTracker,
        write_options: &IpcWriteOptions,
        dict_id_seq: &mut I,
    ) -> Result<(), ArrowError> {
        match column.data_type() {
            DataType::Dictionary(_key_type, _value_type) => {
                let dict_data = column.to_data();
                let dict_values = &dict_data.child_data()[0];

                let values = make_array(dict_data.child_data()[0].clone());

                self._encode_dictionaries(
                    &values,
                    encoded_dictionaries,
                    dictionary_tracker,
                    write_options,
                    dict_id_seq,
                )?;

                // It's important to only take the dict_id at this point, because the dict ID
                // sequence is assigned depth-first, so we need to first encode children and have
                // them take their assigned dict IDs before we take the dict ID for this field.
                let dict_id = dict_id_seq
                    .next()
                    .or_else(|| field.dict_id())
                    .ok_or_else(|| {
                        ArrowError::IpcError(format!("no dict id for field {}", field.name()))
                    })?;

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
                dict_id_seq,
            )?,
        }

        Ok(())
    }

    /// Calls [`Self::encoded_batch_with_size`] with no limit, returning the first (and only)
    /// [`EncodedData`] that is produced. This method should be used over
    /// [`Self::encoded_batch_with_size`] if the consumer has no concerns about encoded message
    /// size limits
    pub fn encoded_batch(
        &self,
        batch: &RecordBatch,
        dictionary_tracker: &mut DictionaryTracker,
        write_options: &IpcWriteOptions,
    ) -> Result<(Vec<EncodedData>, EncodedData), ArrowError> {
        let (encoded_dictionaries, mut encoded_messages) =
            self.encoded_batch_with_size(batch, dictionary_tracker, write_options, usize::MAX)?;

        assert_eq!(
            encoded_messages.len(),
            1,
            "encoded_batch with max size of usize::MAX should not be able to return more or less than 1 batch"
        );

        Ok((encoded_dictionaries, encoded_messages.pop().unwrap()))
    }

    /// Encodes a batch to a number of [EncodedData] items (dictionary batches + the record batch).
    /// The [DictionaryTracker] keeps track of dictionaries with new `dict_id`s  (so they are only sent once)
    /// Make sure the [DictionaryTracker] is initialized at the start of the stream.
    /// The `max_flight_data_size` is used to control how much space each encoded [`RecordBatch`] is
    /// allowed to take up.
    ///
    /// Each [`EncodedData`] in the second element of the returned tuple will be smaller than
    /// `max_flight_data_size` bytes, if possible at all. However, this API has no support for
    /// splitting rows into multiple [`EncodedData`]s, so if a row is larger, by itself, than
    /// `max_flight_data_size`, it will be encoded to a message which is larger than the limit.
    pub fn encoded_batch_with_size(
        &self,
        batch: &RecordBatch,
        dictionary_tracker: &mut DictionaryTracker,
        write_options: &IpcWriteOptions,
        max_flight_data_size: usize,
    ) -> Result<(Vec<EncodedData>, Vec<EncodedData>), ArrowError> {
        let schema = batch.schema();
        let mut encoded_dictionaries = Vec::with_capacity(schema.flattened_fields().len());

        let mut dict_id = dictionary_tracker.dict_ids.clone().into_iter();

        for (i, field) in schema.fields().iter().enumerate() {
            let column = batch.column(i);
            self.encode_dictionaries(
                field,
                column,
                &mut encoded_dictionaries,
                dictionary_tracker,
                write_options,
                &mut dict_id,
            )?;
        }

        let encoded_message =
            chunked_encoded_batch_bytes(batch, write_options, max_flight_data_size)?;
        Ok((encoded_dictionaries, encoded_message))
    }

    /// Write dictionary values into two sets of bytes, one for the header (crate::Message) and the
    /// other for the data
    fn dictionary_batch_to_bytes(
        &self,
        dict_id: i64,
        array_data: &ArrayData,
        write_options: &IpcWriteOptions,
    ) -> Result<EncodedData, ArrowError> {
        let mut encoded_datas = encode_array_datas(
            &[array_data.clone()],
            array_data.len(),
            |fbb, offset| {
                let mut builder = DictionaryBatchBuilder::new(&mut fbb.fbb);
                builder.add_id(dict_id);
                builder.add_data(offset);
                builder.finish().as_union_value()
            },
            MessageHeader::DictionaryBatch,
            // ASK: No maximum message size here?
            usize::MAX,
            write_options,
        )?;

        assert_eq!(
            encoded_datas.len(),
            1,
            "encode_array_datas with a max size of usize::MAX should not be able to return more or less than 1 batch"
        );

        Ok(encoded_datas.pop().unwrap())
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

/// Keeps track of dictionaries that have been written, to avoid emitting the same dictionary
/// multiple times.
///
/// Can optionally error if an update to an existing dictionary is attempted, which
/// isn't allowed in the `FileWriter`.
#[derive(Debug)]
pub struct DictionaryTracker {
    written: HashMap<i64, ArrayData>,
    dict_ids: Vec<i64>,
    error_on_replacement: bool,
    preserve_dict_id: bool,
}

impl DictionaryTracker {
    /// Create a new [`DictionaryTracker`].
    ///
    /// If `error_on_replacement`
    /// is true, an error will be generated if an update to an
    /// existing dictionary is attempted.
    ///
    /// If `preserve_dict_id` is true, the dictionary ID defined in the schema
    /// is used, otherwise a unique dictionary ID will be assigned by incrementing
    /// the last seen dictionary ID (or using `0` if no other dictionary IDs have been
    /// seen)
    pub fn new(error_on_replacement: bool) -> Self {
        Self {
            written: HashMap::new(),
            dict_ids: Vec::new(),
            error_on_replacement,
            preserve_dict_id: true,
        }
    }

    /// Create a new [`DictionaryTracker`].
    ///
    /// If `error_on_replacement`
    /// is true, an error will be generated if an update to an
    /// existing dictionary is attempted.
    pub fn new_with_preserve_dict_id(error_on_replacement: bool, preserve_dict_id: bool) -> Self {
        Self {
            written: HashMap::new(),
            dict_ids: Vec::new(),
            error_on_replacement,
            preserve_dict_id,
        }
    }

    /// Set the dictionary ID for `field`.
    ///
    /// If `preserve_dict_id` is true, this will return the `dict_id` in `field` (or panic if `field` does
    /// not have a `dict_id` defined).
    ///
    /// If `preserve_dict_id` is false, this will return the value of the last `dict_id` assigned incremented by 1
    /// or 0 in the case where no dictionary IDs have yet been assigned
    pub fn set_dict_id(&mut self, field: &Field) -> i64 {
        let next = if self.preserve_dict_id {
            field.dict_id().expect("no dict_id in field")
        } else {
            self.dict_ids
                .last()
                .copied()
                .map(|i| i + 1)
                .unwrap_or_default()
        };

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
}

/// Writer for an IPC file
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
        let data_gen = IpcDataGenerator::default();
        // write magic to header aligned on alignment boundary
        let pad_len = pad_to_alignment(write_options.alignment, super::ARROW_MAGIC.len());
        let header_size = super::ARROW_MAGIC.len() + pad_len;
        writer.write_all(&super::ARROW_MAGIC)?;
        writer.write_all(&PADDING[..pad_len])?;
        // write the schema, set the written bytes to the schema + header
        let preserve_dict_id = write_options.preserve_dict_id;
        let mut dictionary_tracker =
            DictionaryTracker::new_with_preserve_dict_id(true, preserve_dict_id);
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

        let (encoded_dictionaries, encoded_messages) = self.data_gen.encoded_batch_with_size(
            batch,
            &mut self.dictionary_tracker,
            &self.write_options,
            usize::MAX,
        )?;

        for encoded_dictionary in encoded_dictionaries {
            let (meta, data) =
                write_message(&mut self.writer, encoded_dictionary, &self.write_options)?;

            let block = crate::Block::new(self.block_offsets as i64, meta as i32, data as i64);
            self.dictionary_blocks.push(block);
            self.block_offsets += meta + data;
        }

        // theoretically, since the maximum size for encoding is usize::MAX, there should never be
        // more than 1 encoded message. However, since there's no need to assert that (i.e. if
        // someone changes usize::MAX above to be a lower message, that's fine), we just assume
        // there can be many messages
        for encoded_message in encoded_messages {
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
        }
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

    /// Unwraps the the underlying writer.
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

/// Writer for an IPC stream
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
        let data_gen = IpcDataGenerator::default();
        let preserve_dict_id = write_options.preserve_dict_id;
        let mut dictionary_tracker =
            DictionaryTracker::new_with_preserve_dict_id(false, preserve_dict_id);

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
        })
    }

    /// Write a record batch to the stream
    pub fn write(&mut self, batch: &RecordBatch) -> Result<(), ArrowError> {
        if self.finished {
            return Err(ArrowError::IpcError(
                "Cannot write record batch to stream writer as it is closed".to_string(),
            ));
        }

        let (encoded_dictionaries, encoded_messages) = self
            .data_gen
            .encoded_batch_with_size(
                batch,
                &mut self.dictionary_tracker,
                &self.write_options,
                usize::MAX,
            )
            .expect("StreamWriter is configured to not error on dictionary replacement");

        for encoded_dictionary in encoded_dictionaries {
            write_message(&mut self.writer, encoded_dictionary, &self.write_options)?;
        }

        for message in encoded_messages {
            write_message(&mut self.writer, message, &self.write_options)?;
        }
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

#[derive(Debug)]
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

    writer.flush()?;
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

    writer.flush()?;

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

/// Returns the offsets and values [`Buffer`]s for a ByteArray with offset type `O`
///
/// In particular, this handles re-encoding the offsets if they don't start at `0`,
/// slicing the values buffer as appropriate. This helps reduce the encoded
/// size of sliced arrays, as values that have been sliced away are not encoded
///
/// # Panics
///
/// Panics if self.buffers does not contain at least 2 buffers (this code expects that the
/// first will contain the offsets for this variable-length array and the other will contain
/// the values)
pub fn get_byte_array_buffers<O: OffsetSizeTrait>(data: &ArrayData) -> (Buffer, Buffer) {
    if data.is_empty() {
        return (MutableBuffer::new(0).into(), MutableBuffer::new(0).into());
    }

    // get the buffer of offsets, now shifted so they are shifted to be accurate to the slice
    // of values that we'll be taking (e.g. if they previously said [0, 3, 5, 7], but we slice
    // to only get the last offset, they'll be shifted to be [0, 2], since that would be the
    // offset pair for the last value in this shifted slice).
    // also, in this example, original_start_offset would be 5 and len would be 2.
    let (offsets, original_start_offset, len) = reencode_offsets::<O>(data);
    let values = data.buffers()[1].slice_with_length(original_start_offset, len);
    (offsets, values)
}

/// Common functionality for re-encoding offsets. Returns the new offsets as well as
/// original start offset and length for use in slicing child data.
///
/// # Panics
///
/// Will panic if you call this on an `ArrayData` that does not have a buffer of offsets as the
/// very first buffer (i.e. expects this to be a valid variable-length array)
pub fn reencode_offsets<O: OffsetSizeTrait>(data: &ArrayData) -> (Buffer, usize, usize) {
    // first we want to see: what is the offset of this `ArrayData` into the buffer (which is a
    // buffer of offsets into the buffer of data)
    let orig_offset = data.offset();
    // and also we need to get the buffer of offsets
    let offsets_buf = &data.buffers()[0];

    // then we have to turn it into a typed slice that we can read and manipulate below if
    // needed, and slice it according to the size that we need to return
    // we need to do `self.len + 1` instead of just `self.len` because the offsets are encoded
    // as overlapping pairs - e.g. an array of two items, starting at idx 0, and spanning two
    // each, would be encoded as [0, 2, 4].
    let offsets_slice = &offsets_buf.typed_data::<O>()[orig_offset..][..data.len() + 1];

    // and now we can see what the very first offset and the very last offset is
    let start_offset = offsets_slice.first().unwrap();
    let end_offset = offsets_slice.last().unwrap().as_usize();

    // if the start offset is just 0, i.e. it points to the very beginning of the values of
    // this `ArrayData`, then we don't need to shift anything to be a 'correct' offset 'cause
    // all the offsets in this buffer are already offset by 0.
    // But if it's not 0, then we need to shift them all so that the offsets don't start at
    // some weird value.
    let (start_offset, offsets) = match start_offset.as_usize() {
        0 => (0, offsets_slice.to_vec().into()),
        start => (
            start,
            offsets_slice.iter().map(|x| *x - *start_offset).collect(),
        ),
    };

    (offsets, start_offset, end_offset - start_offset)
}

/// Similar logic as [`get_byte_array_buffers()`] but slices the child array instead
/// of a values buffer.
fn get_list_array_buffers<O: OffsetSizeTrait>(data: &ArrayData) -> (Buffer, ArrayData) {
    if data.is_empty() {
        return (
            MutableBuffer::new(0).into(),
            data.child_data()[0].slice(0, 0),
        );
    }

    let (offsets, original_start_offset, len) = reencode_offsets::<O>(data);
    let child_data = data.child_data()[0].slice(original_start_offset, len);
    (offsets, child_data)
}

const DEFAULT_ALIGNMENT: u8 = 64;
const PADDING: [u8; DEFAULT_ALIGNMENT as usize] = [0; DEFAULT_ALIGNMENT as usize];

/// Calculate an alignment boundary and return the number of bytes needed to pad to the alignment boundary
#[inline]
fn pad_to_alignment(alignment: u8, len: usize) -> usize {
    let a = usize::from(alignment - 1);
    ((len + a) & !a) - len
}

fn chunked_encoded_batch_bytes(
    batch: &RecordBatch,
    write_options: &IpcWriteOptions,
    max_flight_data_size: usize,
) -> Result<Vec<EncodedData>, ArrowError> {
    encode_array_datas(
        &batch
            .columns()
            .iter()
            .map(ArrayRef::to_data)
            .collect::<Vec<_>>(),
        batch.num_rows(),
        |_, offset| offset.as_union_value(),
        MessageHeader::RecordBatch,
        max_flight_data_size,
        write_options,
    )
}

fn encode_array_datas(
    arr_datas: &[ArrayData],
    n_rows: usize,
    encode_root: impl Fn(
        &mut FlatBufferSizeTracker,
        WIPOffset<crate::gen::Message::RecordBatch>,
    ) -> WIPOffset<UnionWIPOffset>,
    header_type: MessageHeader,
    mut max_msg_size: usize,
    write_options: &IpcWriteOptions,
) -> Result<Vec<EncodedData>, ArrowError> {
    let mut fbb = FlatBufferSizeTracker::for_dry_run(arr_datas.len());
    fbb.encode_array_datas(
        arr_datas,
        n_rows as i64,
        &encode_root,
        header_type,
        write_options,
    )?;

    fn get_arr_batch_size<AD: Borrow<ArrayData>>(
        iter: impl Iterator<Item = AD>,
        write_options: &IpcWriteOptions,
    ) -> Result<usize, ArrowError> {
        iter.map(|arr| {
            let arr = arr.borrow();
            arr.get_slice_memory_size_with_alignment(Some(write_options.alignment))
                .map(|size| {
                    let didnt_count_nulls = arr.nulls().is_none();
                    let will_write_nulls = has_validity_bitmap(arr.data_type(), write_options);

                    if will_write_nulls && didnt_count_nulls {
                        let null_len = bit_util::ceil(arr.len(), 8);
                        size + null_len + pad_to_alignment(write_options.alignment, null_len)
                    } else {
                        size
                    }
                })
        })
        .sum()
    }

    let header_len = fbb.fbb.finished_data().len();
    max_msg_size = max_msg_size.saturating_sub(header_len).max(1);

    let total_size = get_arr_batch_size(arr_datas.iter(), write_options)?;

    let n_batches = bit_util::ceil(total_size, max_msg_size);
    let mut out = Vec::with_capacity(n_batches);

    let mut offset = 0;
    while offset < n_rows.max(1) {
        let slice_arrays = |len: usize| {
            arr_datas.iter().map(move |arr| {
                if len >= arr.len() {
                    arr.clone()
                } else {
                    arr.slice(offset, len)
                }
            })
        };

        let rows_left = n_rows - offset;
        // TODO? maybe this could be more efficient by continually approximating the maximum number
        // of rows based on (size / n_rows) of the current ArrayData slice until we've found the
        // maximum that can fit? e.g. 'oh, it's 200 bytes and 10 rows, so each row is probably 20
        // bytes - let's do (max_size / 20) rows and see if that fits'
        let length = (1..=rows_left)
            .find(|len| {
                // If we've exhausted the available length of the array datas, then just return -
                // we've got it.
                if offset + len > n_rows {
                    return true;
                }

                // we can unwrap this here b/c this only errors on malformed buffer-type/data-type
                // combinations, and if any of these arrays had that, this function would've
                // already short-circuited on an earlier call of this function
                get_arr_batch_size(slice_arrays(*len), write_options).unwrap() > max_msg_size
            })
            // If no rows fit in the given max size, we want to try to get the data across anyways,
            // so that just means doing a single row. Calling `max(2)` is how we ensure that - if
            // the very first item would go over the max size, giving us a length of 0, we want to
            // set this to `2` so that taking away 1 leaves us with one row to encode.
            .map(|len| len.max(2) - 1)
            // If all rows can comfortably fit in this given size, then just get them all
            .unwrap_or(rows_left);

        // We could get into a situtation where we were given all 0-row arrays to be sent over
        // flight - we do need to send a flight message to show that there is no data, but we also
        // can't have `length` be 0 at this point because it could also be that all rows are too
        // large to send with the provided limits and so we just want to try to send one now
        // anyways, so the checks in this fn are just how we cover our bases there.
        let new_arrs = slice_arrays(length).collect::<Vec<_>>();

        // If we've got more than one row to encode or if we have 0 rows to encode but we haven't
        // encoded anything yet, then continue with encoding. We don't need to do encoding, though,
        // if we've already encoded some rows and there's no rows left
        if length != 0 || offset == 0 {
            fbb.reset_for_real_run();
            fbb.encode_array_datas(
                &new_arrs,
                length as i64,
                &encode_root,
                header_type,
                write_options,
            )?;

            let finished_data = fbb.fbb.finished_data();

            out.push(EncodedData {
                ipc_message: finished_data.to_vec(),
                arrow_data: fbb.arrow_data.clone(),
            });
        }

        // If length == 0, that means they gave us ArrayData with no rows, so a single iteration is
        // always sufficient.
        if length == 0 {
            break;
        }

        offset += length;
    }

    Ok(out)
}

/// A struct to help ensure that the size of encoded flight messages never goes over a provided
/// limit (except in ridiculous cases like a limit of 1 byte). The way it does so is by first
/// running through a provided slice of [`ArrayData`], producing an IPC header message for that
/// slice, and then subtracting the size of that generated header from the message limit it has
/// been given. Because IPC header message sizes don't change due to a different amount of rows,
/// this header size will stay consistent throughout the entire time that we have to transmit a
/// chunk of rows, so we can just subtract it from the overall limit and use that to check
/// different slices of `ArrayData` against to know how many to transmit each time.
///
/// This whole process is done in [`encode_array_datas()`] above
#[derive(Default)]
struct FlatBufferSizeTracker<'fbb> {
    // the builder and backing flatbuffer that we use to write the arrow data into.
    fbb: FlatBufferBuilder<'fbb>,
    // tracks the data in `arrow_data` - `buffers` contains the offsets and length of different
    // buffers encoded within the big chunk that is `arrow_data`.
    buffers: Vec<crate::Buffer>,
    // the raw array data that we need to send across the wire
    arrow_data: Vec<u8>,
    nodes: Vec<crate::FieldNode>,
    dry_run: bool,
}

impl<'fbb> FlatBufferSizeTracker<'fbb> {
    /// Preferred initializer, as this should always be used with a dry-run before a real run to
    /// figure out the size of the IPC header.
    #[must_use]
    fn for_dry_run(capacity: usize) -> Self {
        Self {
            dry_run: true,
            buffers: Vec::with_capacity(capacity),
            nodes: Vec::with_capacity(capacity),
            ..Self::default()
        }
    }

    /// Should be called in-between calls to `encode_array_datas` to ensure we don't accidentally
    /// keep & encode old data each time.
    fn reset_for_real_run(&mut self) {
        self.fbb.reset();
        self.buffers.clear();
        self.arrow_data.clear();
        self.nodes.clear();
        self.dry_run = false;

        // this helps us avoid completely re-allocating the buffers by just creating a new `Self`.
        // So everything should be allocated correctly now besides arrow_data. If we're calling
        // this after only a dry run, `arrow_data` shouldn't have anything written into it, but
        // we call this after every real run loop, so we still need to clear it.
    }

    fn encode_array_datas(
        &mut self,
        arr_datas: &[ArrayData],
        n_rows: i64,
        encode_root: impl FnOnce(
            &mut FlatBufferSizeTracker,
            WIPOffset<crate::gen::Message::RecordBatch>,
        ) -> WIPOffset<UnionWIPOffset>,
        header_type: MessageHeader,
        write_options: &IpcWriteOptions,
    ) -> Result<(), ArrowError> {
        let batch_compression_type = write_options.batch_compression_type;

        let compression = batch_compression_type.map(|compression_type| {
            let mut builder = BodyCompressionBuilder::new(&mut self.fbb);
            builder.add_method(BodyCompressionMethod::BUFFER);
            builder.add_codec(compression_type);
            builder.finish()
        });

        let mut variadic_buffer_counts = Vec::<i64>::default();
        let mut offset = 0;

        for array in arr_datas {
            self.write_array_data(
                array,
                &mut offset,
                array.len(),
                array.null_count(),
                write_options,
            )?;

            append_variadic_buffer_counts(&mut variadic_buffer_counts, array);
        }

        // pad the tail of the body data
        let pad_len = pad_to_alignment(write_options.alignment, self.arrow_data.len());
        self.arrow_data.extend_from_slice(&PADDING[..pad_len]);

        let buffers = self.fbb.create_vector(&self.buffers);
        let nodes = self.fbb.create_vector(&self.nodes);
        let variadic_buffer = (!variadic_buffer_counts.is_empty())
            .then(|| self.fbb.create_vector(&variadic_buffer_counts));

        let root = {
            let mut builder = RecordBatchBuilder::new(&mut self.fbb);

            builder.add_length(n_rows);
            builder.add_nodes(nodes);
            builder.add_buffers(buffers);
            if let Some(c) = compression {
                builder.add_compression(c);
            }
            if let Some(v) = variadic_buffer {
                builder.add_variadicBufferCounts(v);
            }

            builder.finish()
        };

        let root = encode_root(self, root);

        let arrow_len = self.arrow_data.len() as i64;
        let msg = {
            let mut builder = MessageBuilder::new(&mut self.fbb);
            builder.add_version(write_options.metadata_version);
            builder.add_header_type(header_type);
            builder.add_bodyLength(arrow_len);

            builder.add_header(root);
            builder.finish()
        };

        self.fbb.finish(msg, None);
        Ok(())
    }

    fn write_array_data(
        &mut self,
        array_data: &ArrayData,
        offset: &mut i64,
        num_rows: usize,
        null_count: usize,
        write_options: &IpcWriteOptions,
    ) -> Result<(), ArrowError> {
        let compression_codec: Option<CompressionCodec> = write_options
            .batch_compression_type
            .map(TryInto::try_into)
            .transpose()?;

        // NullArray's null_count equals to len, but the `null_count` passed in is from ArrayData
        // where null_count is always 0.
        self.nodes.push(crate::FieldNode::new(
            num_rows as i64,
            match array_data.data_type() {
                DataType::Null => num_rows,
                _ => null_count,
            } as i64,
        ));

        if has_validity_bitmap(array_data.data_type(), write_options) {
            // write null buffer if exists
            let null_buffer = match array_data.nulls() {
                None => {
                    let num_bytes = bit_util::ceil(num_rows, 8);
                    // create a buffer and fill it with valid bits
                    MutableBuffer::new(num_bytes)
                        .with_bitset(num_bytes, true)
                        .into()
                }
                Some(buffer) => buffer.inner().sliced(),
            };

            self.write_buffer(
                &null_buffer,
                offset,
                compression_codec,
                write_options.alignment,
            )?;
        }

        let mut write_byte_array_byffers = |(offsets, values): (Buffer, Buffer)| {
            for buffer in [offsets, values] {
                self.write_buffer(&buffer, offset, compression_codec, write_options.alignment)?;
            }
            Ok::<_, ArrowError>(())
        };

        match array_data.data_type() {
            DataType::Binary | DataType::Utf8 => {
                write_byte_array_byffers(get_byte_array_buffers::<i32>(array_data))?
            }
            DataType::LargeBinary | DataType::LargeUtf8 => {
                write_byte_array_byffers(get_byte_array_buffers::<i64>(array_data))?
            }
            dt if DataType::is_numeric(dt)
                || DataType::is_temporal(dt)
                || matches!(
                    dt,
                    DataType::FixedSizeBinary(_) | DataType::Dictionary(_, _)
                ) =>
            {
                // Truncate values
                let [buffer] = array_data.buffers() else {
                    panic!("Temporal, Numeric, FixedSizeBinary, and Dictionary data types must contain only one buffer");
                };

                let layout = layout(dt);
                let spec = &layout.buffers[0];

                let byte_width = get_buffer_element_width(spec);
                let min_length = array_data.len() * byte_width;
                let mut buffer_slice = buffer.as_slice();

                if buffer_need_truncate(array_data.offset(), buffer, spec, min_length) {
                    let byte_offset = array_data.offset() * byte_width;
                    let buffer_length = min(min_length, buffer.len() - byte_offset);
                    buffer_slice = &buffer_slice[byte_offset..(byte_offset + buffer_length)];
                }

                self.write_buffer(
                    buffer_slice,
                    offset,
                    compression_codec,
                    write_options.alignment,
                )?;
            }
            DataType::Boolean => {
                // Bools are special because the payload (= 1 bit) is smaller than the physical container elements (= bytes).
                // The array data may not start at the physical boundary of the underlying buffer,
                // so we need to shift bits around.
                let [single_buf] = array_data.buffers() else {
                    panic!("ArrayData of type Boolean should only contain 1 buffer");
                };

                let buffer = &single_buf.bit_slice(array_data.offset(), array_data.len());
                self.write_buffer(buffer, offset, compression_codec, write_options.alignment)?;
            }
            dt @ (DataType::List(_) | DataType::LargeList(_) | DataType::Map(_, _)) => {
                assert_eq!(array_data.buffers().len(), 1);
                assert_eq!(array_data.child_data().len(), 1);

                // Truncate offsets and the child data to avoid writing unnecessary data
                let (offsets, sliced_child_data) = match dt {
                    DataType::List(_) | DataType::Map(_, _) => {
                        get_list_array_buffers::<i32>(array_data)
                    }
                    DataType::LargeList(_) => get_list_array_buffers::<i64>(array_data),
                    _ => unreachable!(),
                };
                self.write_buffer(&offsets, offset, compression_codec, write_options.alignment)?;
                self.write_array_data(
                    &sliced_child_data,
                    offset,
                    sliced_child_data.len(),
                    sliced_child_data.null_count(),
                    write_options,
                )?;
                return Ok(());
            }
            _ => {
                // This accommodates for even the `View` types (e.g. BinaryView and Utf8View):
                // Slicing the views buffer is safe and easy,
                // but pruning unneeded data buffers is much more nuanced since it's complicated
                // to prove that no views reference the pruned buffers
                //
                // Current implementation just serialize the raw arrays as given and not try to optimize anything.
                // If users wants to "compact" the arrays prior to sending them over IPC,
                // they should consider the gc API suggested in #5513
                for buffer in array_data.buffers() {
                    self.write_buffer(buffer, offset, compression_codec, write_options.alignment)?;
                }
            }
        }

        let mut write_arr = |arr: &ArrayData| {
            for data_ref in arr.child_data() {
                self.write_array_data(
                    data_ref,
                    offset,
                    data_ref.len(),
                    data_ref.null_count(),
                    write_options,
                )?;
            }
            Ok::<_, ArrowError>(())
        };

        match array_data.data_type() {
            DataType::Dictionary(_, _) => Ok(()),
            // unslice the run encoded array.
            DataType::RunEndEncoded(_, _) => write_arr(&unslice_run_array(array_data.clone())?),
            // recursively write out nested structures
            _ => write_arr(array_data),
        }
    }

    /// Write a buffer into `arrow_data`, a vector of bytes, and adds its
    /// [`crate::Buffer`] to `buffers`. Modifies the offset passed in to respect the new value.
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
        &mut self,
        // input
        buffer: &[u8],
        // current output stream offset
        offset: &mut i64,
        compression_codec: Option<CompressionCodec>,
        alignment: u8,
    ) -> Result<(), ArrowError> {
        let len: i64 = if self.dry_run {
            // Flatbuffers will essentially optimize this away if we say the len is 0 for all of
            // these, so to make sure the header size is the same in the dry run and in the real
            // thing, we need to set this to a non-zero value
            1
        } else {
            match compression_codec {
                Some(compressor) => compressor.compress_to_vec(buffer, &mut self.arrow_data)?,
                None => {
                    self.arrow_data.extend_from_slice(buffer);
                    buffer.len()
                }
            }
            .try_into()
            .map_err(|e| {
                ArrowError::InvalidArgumentError(format!(
                    "Could not convert compressed size to i64: {e}"
                ))
            })?
        };

        // make new index entry
        self.buffers.push(crate::Buffer::new(*offset, len));
        // padding and make offset aligned
        let pad_len = pad_to_alignment(alignment, len as usize);
        self.arrow_data.extend_from_slice(&PADDING[..pad_len]);

        *offset += len + (pad_len as i64);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::io::Seek;

    use arrow_array::builder::GenericListBuilder;
    use arrow_array::builder::MapBuilder;
    use arrow_array::builder::UnionBuilder;
    use arrow_array::builder::{PrimitiveRunBuilder, UInt32Builder};
    use arrow_array::types::*;
    use arrow_buffer::ScalarBuffer;

    use crate::convert::fb_to_schema;
    use crate::reader::*;
    use crate::root_as_footer;
    use crate::MetadataVersion;

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
        let dctfield = Field::new_dict("dict", array.data_type().clone(), false, 2, false);
        let union_fields = [(0, Arc::new(dctfield))].into_iter().collect();

        let types = [0, 0, 0].into_iter().collect::<ScalarBuffer<i8>>();
        let offsets = [0, 1, 2].into_iter().collect::<ScalarBuffer<i32>>();

        let union = UnionArray::try_new(union_fields, types, Some(offsets), vec![array]).unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "union",
            union.data_type().clone(),
            false,
        )]));

        let batch = RecordBatch::try_new(schema, vec![Arc::new(union)]).unwrap();

        let gen = IpcDataGenerator {};
        let mut dict_tracker = DictionaryTracker::new_with_preserve_dict_id(false, true);
        gen.encoded_batch(&batch, &mut dict_tracker, &Default::default())
            .unwrap();

        // The encoder will assign dict IDs itself to ensure uniqueness and ignore the dict ID in the schema
        // so we expect the dict will be keyed to 0
        assert!(dict_tracker.written.contains_key(&2));
    }

    #[test]
    fn track_struct_nested_dict() {
        let inner: DictionaryArray<Int32Type> = vec!["a", "b", "a"].into_iter().collect();

        let array = Arc::new(inner) as ArrayRef;

        // Dict field with id 2
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

        let batch = RecordBatch::try_new(schema, vec![struct_array]).unwrap();

        let gen = IpcDataGenerator {};
        let mut dict_tracker = DictionaryTracker::new_with_preserve_dict_id(false, true);
        gen.encoded_batch(&batch, &mut dict_tracker, &Default::default())
            .unwrap();

        assert!(dict_tracker.written.contains_key(&2));
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
        let val_inner = Field::new("item", DataType::UInt32, true);
        let val_list_field = Field::new("val", DataType::List(Arc::new(val_inner)), false);
        let schema = Arc::new(Schema::new(vec![val_list_field]));

        let values = Arc::new(generate_list_data::<i32>());

        let in_batch = RecordBatch::try_new(schema, vec![values]).unwrap();
        roundtrip_ensure_sliced_smaller(in_batch, 1000);
    }

    #[test]
    fn encode_empty_list() {
        let val_inner = Field::new("item", DataType::UInt32, true);
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
        let val_inner = Field::new("item", DataType::UInt32, true);
        let val_list_field = Field::new("val", DataType::LargeList(Arc::new(val_inner)), false);
        let schema = Arc::new(Schema::new(vec![val_list_field]));

        let values = Arc::new(generate_list_data::<i64>());

        // ensure when serde full & sliced versions they are equal to original input
        // also ensure serialized sliced version is significantly smaller than serialized full
        let in_batch = RecordBatch::try_new(schema, vec![values]).unwrap();
        roundtrip_ensure_sliced_smaller(in_batch, 1000);
    }

    #[test]
    fn encode_nested_lists() {
        let inner_int = Arc::new(Field::new("item", DataType::UInt32, true));
        let inner_list_field = Arc::new(Field::new("item", DataType::List(inner_int), true));
        let list_field = Field::new("val", DataType::List(inner_list_field), true);
        let schema = Arc::new(Schema::new(vec![list_field]));

        let values = Arc::new(generate_nested_list_data::<i32>());

        let in_batch = RecordBatch::try_new(schema, vec![values]).unwrap();
        roundtrip_ensure_sliced_smaller(in_batch, 1000);
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
                let field = Field::new(format!("col_{}", i), DataType::Decimal128(38, 10), true);
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
            let field = Field::new(format!("col_{}", i), DataType::Decimal128(38, 10), true);
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
            let field = Field::new(format!("col_{}", i), DataType::Decimal128(38, 10), true);
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
}
