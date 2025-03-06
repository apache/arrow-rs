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

//! Arrow IPC File and Stream Readers
//!
//! # Notes
//!
//! The [`FileReader`] and [`StreamReader`] have similar interfaces,
//! however the [`FileReader`] expects a reader that supports [`Seek`]ing
//!
//! [`Seek`]: std::io::Seek

mod stream;

pub use stream::*;

use flatbuffers::{VectorIter, VerifierOptions};
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::sync::Arc;

use arrow_array::*;
use arrow_buffer::{ArrowNativeType, BooleanBuffer, Buffer, MutableBuffer, ScalarBuffer};
use arrow_data::{ArrayData, ArrayDataBuilder, UnsafeFlag};
use arrow_schema::*;

use crate::compression::CompressionCodec;
use crate::{Block, FieldNode, Message, MetadataVersion, CONTINUATION_MARKER};
use DataType::*;

/// Read a buffer based on offset and length
/// From <https://github.com/apache/arrow/blob/6a936c4ff5007045e86f65f1a6b6c3c955ad5103/format/Message.fbs#L58>
/// Each constituent buffer is first compressed with the indicated
/// compressor, and then written with the uncompressed length in the first 8
/// bytes as a 64-bit little-endian signed integer followed by the compressed
/// buffer bytes (and then padding as required by the protocol). The
/// uncompressed length may be set to -1 to indicate that the data that
/// follows is not compressed, which can be useful for cases where
/// compression does not yield appreciable savings.
fn read_buffer(
    buf: &crate::Buffer,
    a_data: &Buffer,
    compression_codec: Option<CompressionCodec>,
) -> Result<Buffer, ArrowError> {
    let start_offset = buf.offset() as usize;
    let buf_data = a_data.slice_with_length(start_offset, buf.length() as usize);
    // corner case: empty buffer
    match (buf_data.is_empty(), compression_codec) {
        (true, _) | (_, None) => Ok(buf_data),
        (false, Some(decompressor)) => decompressor.decompress_to_buffer(&buf_data),
    }
}
impl RecordBatchDecoder<'_> {
    /// Coordinates reading arrays based on data types.
    ///
    /// `variadic_counts` encodes the number of buffers to read for variadic types (e.g., Utf8View, BinaryView)
    /// When encounter such types, we pop from the front of the queue to get the number of buffers to read.
    ///
    /// Notes:
    /// * In the IPC format, null buffers are always set, but may be empty. We discard them if an array has 0 nulls
    /// * Numeric values inside list arrays are often stored as 64-bit values regardless of their data type size.
    ///   We thus:
    ///     - check if the bit width of non-64-bit numbers is 64, and
    ///     - read the buffer as 64-bit (signed integer or float), and
    ///     - cast the 64-bit array to the appropriate data type
    fn create_array(
        &mut self,
        field: &Field,
        variadic_counts: &mut VecDeque<i64>,
    ) -> Result<ArrayRef, ArrowError> {
        let data_type = field.data_type();
        match data_type {
            Utf8 | Binary | LargeBinary | LargeUtf8 => {
                let field_node = self.next_node(field)?;
                let buffers = [
                    self.next_buffer()?,
                    self.next_buffer()?,
                    self.next_buffer()?,
                ];
                self.create_primitive_array(field_node, data_type, &buffers)
            }
            BinaryView | Utf8View => {
                let count = variadic_counts
                    .pop_front()
                    .ok_or(ArrowError::IpcError(format!(
                        "Missing variadic count for {data_type} column"
                    )))?;
                let count = count + 2; // view and null buffer.
                let buffers = (0..count)
                    .map(|_| self.next_buffer())
                    .collect::<Result<Vec<_>, _>>()?;
                let field_node = self.next_node(field)?;
                self.create_primitive_array(field_node, data_type, &buffers)
            }
            FixedSizeBinary(_) => {
                let field_node = self.next_node(field)?;
                let buffers = [self.next_buffer()?, self.next_buffer()?];
                self.create_primitive_array(field_node, data_type, &buffers)
            }
            List(ref list_field) | LargeList(ref list_field) | Map(ref list_field, _) => {
                let list_node = self.next_node(field)?;
                let list_buffers = [self.next_buffer()?, self.next_buffer()?];
                let values = self.create_array(list_field, variadic_counts)?;
                self.create_list_array(list_node, data_type, &list_buffers, values)
            }
            FixedSizeList(ref list_field, _) => {
                let list_node = self.next_node(field)?;
                let list_buffers = [self.next_buffer()?];
                let values = self.create_array(list_field, variadic_counts)?;
                self.create_list_array(list_node, data_type, &list_buffers, values)
            }
            Struct(struct_fields) => {
                let struct_node = self.next_node(field)?;
                let null_buffer = self.next_buffer()?;

                // read the arrays for each field
                let mut struct_arrays = vec![];
                // TODO investigate whether just knowing the number of buffers could
                // still work
                for struct_field in struct_fields {
                    let child = self.create_array(struct_field, variadic_counts)?;
                    struct_arrays.push(child);
                }
                self.create_struct_array(struct_node, null_buffer, struct_fields, struct_arrays)
            }
            RunEndEncoded(run_ends_field, values_field) => {
                let run_node = self.next_node(field)?;
                let run_ends = self.create_array(run_ends_field, variadic_counts)?;
                let values = self.create_array(values_field, variadic_counts)?;

                let run_array_length = run_node.length() as usize;
                let builder = ArrayData::builder(data_type.clone())
                    .len(run_array_length)
                    .offset(0)
                    .add_child_data(run_ends.into_data())
                    .add_child_data(values.into_data());
                self.create_array_from_builder(builder)
            }
            // Create dictionary array from RecordBatch
            Dictionary(_, _) => {
                let index_node = self.next_node(field)?;
                let index_buffers = [self.next_buffer()?, self.next_buffer()?];

                #[allow(deprecated)]
                let dict_id = field.dict_id().ok_or_else(|| {
                    ArrowError::ParseError(format!("Field {field} does not have dict id"))
                })?;

                let value_array = self.dictionaries_by_id.get(&dict_id).ok_or_else(|| {
                    ArrowError::ParseError(format!(
                        "Cannot find a dictionary batch with dict id: {dict_id}"
                    ))
                })?;

                self.create_dictionary_array(
                    index_node,
                    data_type,
                    &index_buffers,
                    value_array.clone(),
                )
            }
            Union(fields, mode) => {
                let union_node = self.next_node(field)?;
                let len = union_node.length() as usize;

                // In V4, union types has validity bitmap
                // In V5 and later, union types have no validity bitmap
                if self.version < MetadataVersion::V5 {
                    self.next_buffer()?;
                }

                let type_ids: ScalarBuffer<i8> =
                    self.next_buffer()?.slice_with_length(0, len).into();

                let value_offsets = match mode {
                    UnionMode::Dense => {
                        let offsets: ScalarBuffer<i32> =
                            self.next_buffer()?.slice_with_length(0, len * 4).into();
                        Some(offsets)
                    }
                    UnionMode::Sparse => None,
                };

                let mut children = Vec::with_capacity(fields.len());

                for (_id, field) in fields.iter() {
                    let child = self.create_array(field, variadic_counts)?;
                    children.push(child);
                }

                let array = if self.skip_validation.get() {
                    // safety: flag can only be set via unsafe code
                    unsafe {
                        UnionArray::new_unchecked(fields.clone(), type_ids, value_offsets, children)
                    }
                } else {
                    UnionArray::try_new(fields.clone(), type_ids, value_offsets, children)?
                };
                Ok(Arc::new(array))
            }
            Null => {
                let node = self.next_node(field)?;
                let length = node.length();
                let null_count = node.null_count();

                if length != null_count {
                    return Err(ArrowError::SchemaError(format!(
                        "Field {field} of NullArray has unequal null_count {null_count} and len {length}"
                    )));
                }

                let builder = ArrayData::builder(data_type.clone())
                    .len(length as usize)
                    .offset(0);
                self.create_array_from_builder(builder)
            }
            _ => {
                let field_node = self.next_node(field)?;
                let buffers = [self.next_buffer()?, self.next_buffer()?];
                self.create_primitive_array(field_node, data_type, &buffers)
            }
        }
    }

    /// Reads the correct number of buffers based on data type and null_count, and creates a
    /// primitive array ref
    fn create_primitive_array(
        &self,
        field_node: &FieldNode,
        data_type: &DataType,
        buffers: &[Buffer],
    ) -> Result<ArrayRef, ArrowError> {
        let length = field_node.length() as usize;
        let null_buffer = (field_node.null_count() > 0).then_some(buffers[0].clone());
        let builder = match data_type {
            Utf8 | Binary | LargeBinary | LargeUtf8 => {
                // read 3 buffers: null buffer (optional), offsets buffer and data buffer
                ArrayData::builder(data_type.clone())
                    .len(length)
                    .buffers(buffers[1..3].to_vec())
                    .null_bit_buffer(null_buffer)
            }
            BinaryView | Utf8View => ArrayData::builder(data_type.clone())
                .len(length)
                .buffers(buffers[1..].to_vec())
                .null_bit_buffer(null_buffer),
            _ if data_type.is_primitive() || matches!(data_type, Boolean | FixedSizeBinary(_)) => {
                // read 2 buffers: null buffer (optional) and data buffer
                ArrayData::builder(data_type.clone())
                    .len(length)
                    .add_buffer(buffers[1].clone())
                    .null_bit_buffer(null_buffer)
            }
            t => unreachable!("Data type {:?} either unsupported or not primitive", t),
        };

        self.create_array_from_builder(builder)
    }

    /// Update the ArrayDataBuilder based on settings in this decoder
    fn create_array_from_builder(&self, builder: ArrayDataBuilder) -> Result<ArrayRef, ArrowError> {
        let mut builder = builder.align_buffers(!self.require_alignment);
        if self.skip_validation.get() {
            // SAFETY: flag can only be set via unsafe code
            unsafe { builder = builder.skip_validation(true) }
        };
        Ok(make_array(builder.build()?))
    }

    /// Reads the correct number of buffers based on list type and null_count, and creates a
    /// list array ref
    fn create_list_array(
        &self,
        field_node: &FieldNode,
        data_type: &DataType,
        buffers: &[Buffer],
        child_array: ArrayRef,
    ) -> Result<ArrayRef, ArrowError> {
        let null_buffer = (field_node.null_count() > 0).then_some(buffers[0].clone());
        let length = field_node.length() as usize;
        let child_data = child_array.into_data();
        let builder = match data_type {
            List(_) | LargeList(_) | Map(_, _) => ArrayData::builder(data_type.clone())
                .len(length)
                .add_buffer(buffers[1].clone())
                .add_child_data(child_data)
                .null_bit_buffer(null_buffer),

            FixedSizeList(_, _) => ArrayData::builder(data_type.clone())
                .len(length)
                .add_child_data(child_data)
                .null_bit_buffer(null_buffer),

            _ => unreachable!("Cannot create list or map array from {:?}", data_type),
        };

        self.create_array_from_builder(builder)
    }

    fn create_struct_array(
        &self,
        struct_node: &FieldNode,
        null_buffer: Buffer,
        struct_fields: &Fields,
        struct_arrays: Vec<ArrayRef>,
    ) -> Result<ArrayRef, ArrowError> {
        let null_count = struct_node.null_count() as usize;
        let len = struct_node.length() as usize;

        let nulls = (null_count > 0).then(|| BooleanBuffer::new(null_buffer, 0, len).into());
        if struct_arrays.is_empty() {
            // `StructArray::from` can't infer the correct row count
            // if we have zero fields
            return Ok(Arc::new(StructArray::new_empty_fields(len, nulls)));
        }

        let struct_array = if self.skip_validation.get() {
            // safety: flag can only be set via unsafe code
            unsafe { StructArray::new_unchecked(struct_fields.clone(), struct_arrays, nulls) }
        } else {
            StructArray::try_new(struct_fields.clone(), struct_arrays, nulls)?
        };

        Ok(Arc::new(struct_array))
    }

    /// Reads the correct number of buffers based on list type and null_count, and creates a
    /// list array ref
    fn create_dictionary_array(
        &self,
        field_node: &FieldNode,
        data_type: &DataType,
        buffers: &[Buffer],
        value_array: ArrayRef,
    ) -> Result<ArrayRef, ArrowError> {
        if let Dictionary(_, _) = *data_type {
            let null_buffer = (field_node.null_count() > 0).then_some(buffers[0].clone());
            let builder = ArrayData::builder(data_type.clone())
                .len(field_node.length() as usize)
                .add_buffer(buffers[1].clone())
                .add_child_data(value_array.into_data())
                .null_bit_buffer(null_buffer);
            self.create_array_from_builder(builder)
        } else {
            unreachable!("Cannot create dictionary array from {:?}", data_type)
        }
    }
}

/// State for decoding Arrow arrays from an [IPC RecordBatch] structure to
/// [`RecordBatch`]
///
/// [IPC RecordBatch]: crate::RecordBatch
struct RecordBatchDecoder<'a> {
    /// The flatbuffers encoded record batch
    batch: crate::RecordBatch<'a>,
    /// The output schema
    schema: SchemaRef,
    /// Decoded dictionaries indexed by dictionary id
    dictionaries_by_id: &'a HashMap<i64, ArrayRef>,
    /// Optional compression codec
    compression: Option<CompressionCodec>,
    /// The format version
    version: MetadataVersion,
    /// The raw data buffer
    data: &'a Buffer,
    /// The fields comprising this array
    nodes: VectorIter<'a, FieldNode>,
    /// The buffers comprising this array
    buffers: VectorIter<'a, crate::Buffer>,
    /// Projection (subset of columns) to read, if any
    /// See [`RecordBatchDecoder::with_projection`] for details
    projection: Option<&'a [usize]>,
    /// Are buffers required to already be aligned? See
    /// [`RecordBatchDecoder::with_require_alignment`] for details
    require_alignment: bool,
    /// Should validation be skipped when reading data? Defaults to false.
    ///
    /// See [`FileDecoder::with_skip_validation`] for details.
    skip_validation: UnsafeFlag,
}

impl<'a> RecordBatchDecoder<'a> {
    /// Create a reader for decoding arrays from an encoded [`RecordBatch`]
    fn try_new(
        buf: &'a Buffer,
        batch: crate::RecordBatch<'a>,
        schema: SchemaRef,
        dictionaries_by_id: &'a HashMap<i64, ArrayRef>,
        metadata: &'a MetadataVersion,
    ) -> Result<Self, ArrowError> {
        let buffers = batch.buffers().ok_or_else(|| {
            ArrowError::IpcError("Unable to get buffers from IPC RecordBatch".to_string())
        })?;
        let field_nodes = batch.nodes().ok_or_else(|| {
            ArrowError::IpcError("Unable to get field nodes from IPC RecordBatch".to_string())
        })?;

        let batch_compression = batch.compression();
        let compression = batch_compression
            .map(|batch_compression| batch_compression.codec().try_into())
            .transpose()?;

        Ok(Self {
            batch,
            schema,
            dictionaries_by_id,
            compression,
            version: *metadata,
            data: buf,
            nodes: field_nodes.iter(),
            buffers: buffers.iter(),
            projection: None,
            require_alignment: false,
            skip_validation: UnsafeFlag::new(),
        })
    }

    /// Set the projection (default: None)
    ///
    /// If set, the projection is the list  of column indices
    /// that will be read
    pub fn with_projection(mut self, projection: Option<&'a [usize]>) -> Self {
        self.projection = projection;
        self
    }

    /// Set require_alignment (default: false)
    ///
    /// If true, buffers must be aligned appropriately or error will
    /// result. If false, buffers will be copied to aligned buffers
    /// if necessary.
    pub fn with_require_alignment(mut self, require_alignment: bool) -> Self {
        self.require_alignment = require_alignment;
        self
    }

    /// Specifies if validation should be skipped when reading data (defaults to `false`)
    ///
    /// Note this API is somewhat "funky" as it allows the caller to skip validation
    /// without having to use `unsafe` code. If this is ever made public
    /// it should be made clearer that this is a potentially unsafe by
    /// using an `unsafe` function that takes a boolean flag.
    ///
    /// # Safety
    ///
    /// Relies on the caller only passing a flag with `true` value if they are
    /// certain that the data is valid
    pub(crate) fn with_skip_validation(mut self, skip_validation: UnsafeFlag) -> Self {
        self.skip_validation = skip_validation;
        self
    }

    /// Read the record batch, consuming the reader
    fn read_record_batch(mut self) -> Result<RecordBatch, ArrowError> {
        let mut variadic_counts: VecDeque<i64> = self
            .batch
            .variadicBufferCounts()
            .into_iter()
            .flatten()
            .collect();

        let options = RecordBatchOptions::new().with_row_count(Some(self.batch.length() as usize));

        let schema = Arc::clone(&self.schema);
        if let Some(projection) = self.projection {
            let mut arrays = vec![];
            // project fields
            for (idx, field) in schema.fields().iter().enumerate() {
                // Create array for projected field
                if let Some(proj_idx) = projection.iter().position(|p| p == &idx) {
                    let child = self.create_array(field, &mut variadic_counts)?;
                    arrays.push((proj_idx, child));
                } else {
                    self.skip_field(field, &mut variadic_counts)?;
                }
            }
            assert!(variadic_counts.is_empty());
            arrays.sort_by_key(|t| t.0);
            RecordBatch::try_new_with_options(
                Arc::new(schema.project(projection)?),
                arrays.into_iter().map(|t| t.1).collect(),
                &options,
            )
        } else {
            let mut children = vec![];
            // keep track of index as lists require more than one node
            for field in schema.fields() {
                let child = self.create_array(field, &mut variadic_counts)?;
                children.push(child);
            }
            assert!(variadic_counts.is_empty());
            RecordBatch::try_new_with_options(schema, children, &options)
        }
    }

    fn next_buffer(&mut self) -> Result<Buffer, ArrowError> {
        read_buffer(self.buffers.next().unwrap(), self.data, self.compression)
    }

    fn skip_buffer(&mut self) {
        self.buffers.next().unwrap();
    }

    fn next_node(&mut self, field: &Field) -> Result<&'a FieldNode, ArrowError> {
        self.nodes.next().ok_or_else(|| {
            ArrowError::SchemaError(format!(
                "Invalid data for schema. {} refers to node not found in schema",
                field
            ))
        })
    }

    fn skip_field(
        &mut self,
        field: &Field,
        variadic_count: &mut VecDeque<i64>,
    ) -> Result<(), ArrowError> {
        self.next_node(field)?;

        match field.data_type() {
            Utf8 | Binary | LargeBinary | LargeUtf8 => {
                for _ in 0..3 {
                    self.skip_buffer()
                }
            }
            Utf8View | BinaryView => {
                let count = variadic_count
                    .pop_front()
                    .ok_or(ArrowError::IpcError(format!(
                        "Missing variadic count for {} column",
                        field.data_type()
                    )))?;
                let count = count + 2; // view and null buffer.
                for _i in 0..count {
                    self.skip_buffer()
                }
            }
            FixedSizeBinary(_) => {
                self.skip_buffer();
                self.skip_buffer();
            }
            List(list_field) | LargeList(list_field) | Map(list_field, _) => {
                self.skip_buffer();
                self.skip_buffer();
                self.skip_field(list_field, variadic_count)?;
            }
            FixedSizeList(list_field, _) => {
                self.skip_buffer();
                self.skip_field(list_field, variadic_count)?;
            }
            Struct(struct_fields) => {
                self.skip_buffer();

                // skip for each field
                for struct_field in struct_fields {
                    self.skip_field(struct_field, variadic_count)?
                }
            }
            RunEndEncoded(run_ends_field, values_field) => {
                self.skip_field(run_ends_field, variadic_count)?;
                self.skip_field(values_field, variadic_count)?;
            }
            Dictionary(_, _) => {
                self.skip_buffer(); // Nulls
                self.skip_buffer(); // Indices
            }
            Union(fields, mode) => {
                self.skip_buffer(); // Nulls

                match mode {
                    UnionMode::Dense => self.skip_buffer(),
                    UnionMode::Sparse => {}
                };

                for (_, field) in fields.iter() {
                    self.skip_field(field, variadic_count)?
                }
            }
            Null => {} // No buffer increases
            _ => {
                self.skip_buffer();
                self.skip_buffer();
            }
        };
        Ok(())
    }
}

/// Creates a record batch from binary data using the `crate::RecordBatch` indexes and the `Schema`.
///
/// If `require_alignment` is true, this function will return an error if any array data in the
/// input `buf` is not properly aligned.
/// Under the hood it will use [`arrow_data::ArrayDataBuilder::build`] to construct [`arrow_data::ArrayData`].
///
/// If `require_alignment` is false, this function will automatically allocate a new aligned buffer
/// and copy over the data if any array data in the input `buf` is not properly aligned.
/// (Properly aligned array data will remain zero-copy.)
/// Under the hood it will use [`arrow_data::ArrayDataBuilder::build_aligned`] to construct [`arrow_data::ArrayData`].
pub fn read_record_batch(
    buf: &Buffer,
    batch: crate::RecordBatch,
    schema: SchemaRef,
    dictionaries_by_id: &HashMap<i64, ArrayRef>,
    projection: Option<&[usize]>,
    metadata: &MetadataVersion,
) -> Result<RecordBatch, ArrowError> {
    RecordBatchDecoder::try_new(buf, batch, schema, dictionaries_by_id, metadata)?
        .with_projection(projection)
        .with_require_alignment(false)
        .read_record_batch()
}

/// Read the dictionary from the buffer and provided metadata,
/// updating the `dictionaries_by_id` with the resulting dictionary
pub fn read_dictionary(
    buf: &Buffer,
    batch: crate::DictionaryBatch,
    schema: &Schema,
    dictionaries_by_id: &mut HashMap<i64, ArrayRef>,
    metadata: &MetadataVersion,
) -> Result<(), ArrowError> {
    read_dictionary_impl(
        buf,
        batch,
        schema,
        dictionaries_by_id,
        metadata,
        false,
        UnsafeFlag::new(),
    )
}

fn read_dictionary_impl(
    buf: &Buffer,
    batch: crate::DictionaryBatch,
    schema: &Schema,
    dictionaries_by_id: &mut HashMap<i64, ArrayRef>,
    metadata: &MetadataVersion,
    require_alignment: bool,
    skip_validation: UnsafeFlag,
) -> Result<(), ArrowError> {
    if batch.isDelta() {
        return Err(ArrowError::InvalidArgumentError(
            "delta dictionary batches not supported".to_string(),
        ));
    }

    let id = batch.id();
    #[allow(deprecated)]
    let fields_using_this_dictionary = schema.fields_with_dict_id(id);
    let first_field = fields_using_this_dictionary.first().ok_or_else(|| {
        ArrowError::InvalidArgumentError(format!("dictionary id {id} not found in schema"))
    })?;

    // As the dictionary batch does not contain the type of the
    // values array, we need to retrieve this from the schema.
    // Get an array representing this dictionary's values.
    let dictionary_values: ArrayRef = match first_field.data_type() {
        DataType::Dictionary(_, ref value_type) => {
            // Make a fake schema for the dictionary batch.
            let value = value_type.as_ref().clone();
            let schema = Schema::new(vec![Field::new("", value, true)]);
            // Read a single column
            let record_batch = RecordBatchDecoder::try_new(
                buf,
                batch.data().unwrap(),
                Arc::new(schema),
                dictionaries_by_id,
                metadata,
            )?
            .with_require_alignment(require_alignment)
            .with_skip_validation(skip_validation)
            .read_record_batch()?;

            Some(record_batch.column(0).clone())
        }
        _ => None,
    }
    .ok_or_else(|| {
        ArrowError::InvalidArgumentError(format!("dictionary id {id} not found in schema"))
    })?;

    // We don't currently record the isOrdered field. This could be general
    // attributes of arrays.
    // Add (possibly multiple) array refs to the dictionaries array.
    dictionaries_by_id.insert(id, dictionary_values.clone());

    Ok(())
}

/// Read the data for a given block
fn read_block<R: Read + Seek>(mut reader: R, block: &Block) -> Result<Buffer, ArrowError> {
    reader.seek(SeekFrom::Start(block.offset() as u64))?;
    let body_len = block.bodyLength().to_usize().unwrap();
    let metadata_len = block.metaDataLength().to_usize().unwrap();
    let total_len = body_len.checked_add(metadata_len).unwrap();

    let mut buf = MutableBuffer::from_len_zeroed(total_len);
    reader.read_exact(&mut buf)?;
    Ok(buf.into())
}

/// Parse an encapsulated message
///
/// <https://arrow.apache.org/docs/format/Columnar.html#encapsulated-message-format>
fn parse_message(buf: &[u8]) -> Result<Message, ArrowError> {
    let buf = match buf[..4] == CONTINUATION_MARKER {
        true => &buf[8..],
        false => &buf[4..],
    };
    crate::root_as_message(buf)
        .map_err(|err| ArrowError::ParseError(format!("Unable to get root as message: {err:?}")))
}

/// Read the footer length from the last 10 bytes of an Arrow IPC file
///
/// Expects a 4 byte footer length followed by `b"ARROW1"`
pub fn read_footer_length(buf: [u8; 10]) -> Result<usize, ArrowError> {
    if buf[4..] != super::ARROW_MAGIC {
        return Err(ArrowError::ParseError(
            "Arrow file does not contain correct footer".to_string(),
        ));
    }

    // read footer length
    let footer_len = i32::from_le_bytes(buf[..4].try_into().unwrap());
    footer_len
        .try_into()
        .map_err(|_| ArrowError::ParseError(format!("Invalid footer length: {footer_len}")))
}

/// A low-level, push-based interface for reading an IPC file
///
/// For a higher-level interface see [`FileReader`]
///
/// For an example of using this API with `mmap` see the [`zero_copy_ipc`] example.
///
/// [`zero_copy_ipc`]: https://github.com/apache/arrow-rs/blob/main/arrow/examples/zero_copy_ipc.rs
///
/// ```
/// # use std::sync::Arc;
/// # use arrow_array::*;
/// # use arrow_array::types::Int32Type;
/// # use arrow_buffer::Buffer;
/// # use arrow_ipc::convert::fb_to_schema;
/// # use arrow_ipc::reader::{FileDecoder, read_footer_length};
/// # use arrow_ipc::root_as_footer;
/// # use arrow_ipc::writer::FileWriter;
/// // Write an IPC file
///
/// let batch = RecordBatch::try_from_iter([
///     ("a", Arc::new(Int32Array::from(vec![1, 2, 3])) as _),
///     ("b", Arc::new(Int32Array::from(vec![1, 2, 3])) as _),
///     ("c", Arc::new(DictionaryArray::<Int32Type>::from_iter(["hello", "hello", "world"])) as _),
/// ]).unwrap();
///
/// let schema = batch.schema();
///
/// let mut out = Vec::with_capacity(1024);
/// let mut writer = FileWriter::try_new(&mut out, schema.as_ref()).unwrap();
/// writer.write(&batch).unwrap();
/// writer.finish().unwrap();
///
/// drop(writer);
///
/// // Read IPC file
///
/// let buffer = Buffer::from_vec(out);
/// let trailer_start = buffer.len() - 10;
/// let footer_len = read_footer_length(buffer[trailer_start..].try_into().unwrap()).unwrap();
/// let footer = root_as_footer(&buffer[trailer_start - footer_len..trailer_start]).unwrap();
///
/// let back = fb_to_schema(footer.schema().unwrap());
/// assert_eq!(&back, schema.as_ref());
///
/// let mut decoder = FileDecoder::new(schema, footer.version());
///
/// // Read dictionaries
/// for block in footer.dictionaries().iter().flatten() {
///     let block_len = block.bodyLength() as usize + block.metaDataLength() as usize;
///     let data = buffer.slice_with_length(block.offset() as _, block_len);
///     decoder.read_dictionary(&block, &data).unwrap();
/// }
///
/// // Read record batch
/// let batches = footer.recordBatches().unwrap();
/// assert_eq!(batches.len(), 1); // Only wrote a single batch
///
/// let block = batches.get(0);
/// let block_len = block.bodyLength() as usize + block.metaDataLength() as usize;
/// let data = buffer.slice_with_length(block.offset() as _, block_len);
/// let back = decoder.read_record_batch(block, &data).unwrap().unwrap();
///
/// assert_eq!(batch, back);
/// ```
#[derive(Debug)]
pub struct FileDecoder {
    schema: SchemaRef,
    dictionaries: HashMap<i64, ArrayRef>,
    version: MetadataVersion,
    projection: Option<Vec<usize>>,
    require_alignment: bool,
    skip_validation: UnsafeFlag,
}

impl FileDecoder {
    /// Create a new [`FileDecoder`] with the given schema and version
    pub fn new(schema: SchemaRef, version: MetadataVersion) -> Self {
        Self {
            schema,
            version,
            dictionaries: Default::default(),
            projection: None,
            require_alignment: false,
            skip_validation: UnsafeFlag::new(),
        }
    }

    /// Specify a projection
    pub fn with_projection(mut self, projection: Vec<usize>) -> Self {
        self.projection = Some(projection);
        self
    }

    /// Specifies if the array data in input buffers is required to be properly aligned.
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

    /// Specifies if validation should be skipped when reading data (defaults to `false`)
    ///
    /// # Safety
    ///
    /// This flag must only be set to `true` when you trust the input data and are sure the data you are
    /// reading is a valid Arrow IPC file, otherwise undefined behavior may
    /// result.
    ///
    /// For example, some programs may wish to trust reading IPC files written
    /// by the same process that created the files.
    pub unsafe fn with_skip_validation(mut self, skip_validation: bool) -> Self {
        self.skip_validation.set(skip_validation);
        self
    }

    fn read_message<'a>(&self, buf: &'a [u8]) -> Result<Message<'a>, ArrowError> {
        let message = parse_message(buf)?;

        // some old test data's footer metadata is not set, so we account for that
        if self.version != MetadataVersion::V1 && message.version() != self.version {
            return Err(ArrowError::IpcError(
                "Could not read IPC message as metadata versions mismatch".to_string(),
            ));
        }
        Ok(message)
    }

    /// Read the dictionary with the given block and data buffer
    pub fn read_dictionary(&mut self, block: &Block, buf: &Buffer) -> Result<(), ArrowError> {
        let message = self.read_message(buf)?;
        match message.header_type() {
            crate::MessageHeader::DictionaryBatch => {
                let batch = message.header_as_dictionary_batch().unwrap();
                read_dictionary_impl(
                    &buf.slice(block.metaDataLength() as _),
                    batch,
                    &self.schema,
                    &mut self.dictionaries,
                    &message.version(),
                    self.require_alignment,
                    self.skip_validation.clone(),
                )
            }
            t => Err(ArrowError::ParseError(format!(
                "Expecting DictionaryBatch in dictionary blocks, found {t:?}."
            ))),
        }
    }

    /// Read the RecordBatch with the given block and data buffer
    pub fn read_record_batch(
        &self,
        block: &Block,
        buf: &Buffer,
    ) -> Result<Option<RecordBatch>, ArrowError> {
        let message = self.read_message(buf)?;
        match message.header_type() {
            crate::MessageHeader::Schema => Err(ArrowError::IpcError(
                "Not expecting a schema when messages are read".to_string(),
            )),
            crate::MessageHeader::RecordBatch => {
                let batch = message.header_as_record_batch().ok_or_else(|| {
                    ArrowError::IpcError("Unable to read IPC message as record batch".to_string())
                })?;
                // read the block that makes up the record batch into a buffer
                RecordBatchDecoder::try_new(
                    &buf.slice(block.metaDataLength() as _),
                    batch,
                    self.schema.clone(),
                    &self.dictionaries,
                    &message.version(),
                )?
                .with_projection(self.projection.as_deref())
                .with_require_alignment(self.require_alignment)
                .with_skip_validation(self.skip_validation.clone())
                .read_record_batch()
                .map(Some)
            }
            crate::MessageHeader::NONE => Ok(None),
            t => Err(ArrowError::InvalidArgumentError(format!(
                "Reading types other than record batches not yet supported, unable to read {t:?}"
            ))),
        }
    }
}

/// Build an Arrow [`FileReader`] with custom options.
#[derive(Debug)]
pub struct FileReaderBuilder {
    /// Optional projection for which columns to load (zero-based column indices)
    projection: Option<Vec<usize>>,
    /// Passed through to construct [`VerifierOptions`]
    max_footer_fb_tables: usize,
    /// Passed through to construct [`VerifierOptions`]
    max_footer_fb_depth: usize,
}

impl Default for FileReaderBuilder {
    fn default() -> Self {
        let verifier_options = VerifierOptions::default();
        Self {
            max_footer_fb_tables: verifier_options.max_tables,
            max_footer_fb_depth: verifier_options.max_depth,
            projection: None,
        }
    }
}

impl FileReaderBuilder {
    /// Options for creating a new [`FileReader`].
    ///
    /// To convert a builder into a reader, call [`FileReaderBuilder::build`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Optional projection for which columns to load (zero-based column indices).
    pub fn with_projection(mut self, projection: Vec<usize>) -> Self {
        self.projection = Some(projection);
        self
    }

    /// Flatbuffers option for parsing the footer. Controls the max number of fields and
    /// metadata key-value pairs that can be parsed from the schema of the footer.
    ///
    /// By default this is set to `1_000_000` which roughly translates to a schema with
    /// no metadata key-value pairs but 499,999 fields.
    ///
    /// This default limit is enforced to protect against malicious files with a massive
    /// amount of flatbuffer tables which could cause a denial of service attack.
    ///
    /// If you need to ingest a trusted file with a massive number of fields and/or
    /// metadata key-value pairs and are facing the error `"Unable to get root as
    /// footer: TooManyTables"` then increase this parameter as necessary.
    pub fn with_max_footer_fb_tables(mut self, max_footer_fb_tables: usize) -> Self {
        self.max_footer_fb_tables = max_footer_fb_tables;
        self
    }

    /// Flatbuffers option for parsing the footer. Controls the max depth for schemas with
    /// nested fields parsed from the footer.
    ///
    /// By default this is set to `64` which roughly translates to a schema with
    /// a field nested 60 levels down through other struct fields.
    ///
    /// This default limit is enforced to protect against malicious files with a extremely
    /// deep flatbuffer structure which could cause a denial of service attack.
    ///
    /// If you need to ingest a trusted file with a deeply nested field and are facing the
    /// error `"Unable to get root as footer: DepthLimitReached"` then increase this
    /// parameter as necessary.
    pub fn with_max_footer_fb_depth(mut self, max_footer_fb_depth: usize) -> Self {
        self.max_footer_fb_depth = max_footer_fb_depth;
        self
    }

    /// Build [`FileReader`] with given reader.
    pub fn build<R: Read + Seek>(self, mut reader: R) -> Result<FileReader<R>, ArrowError> {
        // Space for ARROW_MAGIC (6 bytes) and length (4 bytes)
        let mut buffer = [0; 10];
        reader.seek(SeekFrom::End(-10))?;
        reader.read_exact(&mut buffer)?;

        let footer_len = read_footer_length(buffer)?;

        // read footer
        let mut footer_data = vec![0; footer_len];
        reader.seek(SeekFrom::End(-10 - footer_len as i64))?;
        reader.read_exact(&mut footer_data)?;

        let verifier_options = VerifierOptions {
            max_tables: self.max_footer_fb_tables,
            max_depth: self.max_footer_fb_depth,
            ..Default::default()
        };
        let footer = crate::root_as_footer_with_opts(&verifier_options, &footer_data[..]).map_err(
            |err| ArrowError::ParseError(format!("Unable to get root as footer: {err:?}")),
        )?;

        let blocks = footer.recordBatches().ok_or_else(|| {
            ArrowError::ParseError("Unable to get record batches from IPC Footer".to_string())
        })?;

        let total_blocks = blocks.len();

        let ipc_schema = footer.schema().unwrap();
        if !ipc_schema.endianness().equals_to_target_endianness() {
            return Err(ArrowError::IpcError(
                "the endianness of the source system does not match the endianness of the target system.".to_owned()
            ));
        }

        let schema = crate::convert::fb_to_schema(ipc_schema);

        let mut custom_metadata = HashMap::new();
        if let Some(fb_custom_metadata) = footer.custom_metadata() {
            for kv in fb_custom_metadata.into_iter() {
                custom_metadata.insert(
                    kv.key().unwrap().to_string(),
                    kv.value().unwrap().to_string(),
                );
            }
        }

        let mut decoder = FileDecoder::new(Arc::new(schema), footer.version());
        if let Some(projection) = self.projection {
            decoder = decoder.with_projection(projection)
        }

        // Create an array of optional dictionary value arrays, one per field.
        if let Some(dictionaries) = footer.dictionaries() {
            for block in dictionaries {
                let buf = read_block(&mut reader, block)?;
                decoder.read_dictionary(block, &buf)?;
            }
        }

        Ok(FileReader {
            reader,
            blocks: blocks.iter().copied().collect(),
            current_block: 0,
            total_blocks,
            decoder,
            custom_metadata,
        })
    }
}

/// Arrow File Reader
///
/// Reads Arrow [`RecordBatch`]es from bytes in the [IPC File Format],
/// providing random access to the record batches.
///
/// # See Also
///
/// * [`Self::set_index`] for random access
/// * [`StreamReader`] for reading streaming data
///
/// # Example: Reading from a `File`
/// ```
/// # use std::io::Cursor;
/// use arrow_array::record_batch;
/// # use arrow_ipc::reader::FileReader;
/// # use arrow_ipc::writer::FileWriter;
/// # let batch = record_batch!(("a", Int32, [1, 2, 3])).unwrap();
/// # let mut file = vec![]; // mimic a stream for the example
/// # {
/// #  let mut writer = FileWriter::try_new(&mut file, &batch.schema()).unwrap();
/// #  writer.write(&batch).unwrap();
/// #  writer.write(&batch).unwrap();
/// #  writer.finish().unwrap();
/// # }
/// # let mut file = Cursor::new(&file);
/// let projection = None; // read all columns
/// let mut reader = FileReader::try_new(&mut file, projection).unwrap();
/// // Position the reader to the second batch
/// reader.set_index(1).unwrap();
/// // read batches from the reader using the Iterator trait
/// let mut num_rows = 0;
/// for batch in reader {
///    let batch = batch.unwrap();
///    num_rows += batch.num_rows();
/// }
/// assert_eq!(num_rows, 3);
/// ```
/// # Example: Reading from `mmap`ed file
///
/// For an example creating Arrays without copying using  memory mapped (`mmap`)
/// files see the [`zero_copy_ipc`] example.
///
/// [IPC File Format]: https://arrow.apache.org/docs/format/Columnar.html#ipc-file-format
/// [`zero_copy_ipc`]: https://github.com/apache/arrow-rs/blob/main/arrow/examples/zero_copy_ipc.rs
pub struct FileReader<R> {
    /// File reader that supports reading and seeking
    reader: R,

    /// The decoder
    decoder: FileDecoder,

    /// The blocks in the file
    ///
    /// A block indicates the regions in the file to read to get data
    blocks: Vec<Block>,

    /// A counter to keep track of the current block that should be read
    current_block: usize,

    /// The total number of blocks, which may contain record batches and other types
    total_blocks: usize,

    /// User defined metadata
    custom_metadata: HashMap<String, String>,
}

impl<R> fmt::Debug for FileReader<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.debug_struct("FileReader<R>")
            .field("decoder", &self.decoder)
            .field("blocks", &self.blocks)
            .field("current_block", &self.current_block)
            .field("total_blocks", &self.total_blocks)
            .finish_non_exhaustive()
    }
}

impl<R: Read + Seek> FileReader<BufReader<R>> {
    /// Try to create a new file reader with the reader wrapped in a BufReader.
    ///
    /// See [`FileReader::try_new`] for an unbuffered version.
    pub fn try_new_buffered(reader: R, projection: Option<Vec<usize>>) -> Result<Self, ArrowError> {
        Self::try_new(BufReader::new(reader), projection)
    }
}

impl<R: Read + Seek> FileReader<R> {
    /// Try to create a new file reader.
    ///
    /// There is no internal buffering. If buffered reads are needed you likely want to use
    /// [`FileReader::try_new_buffered`] instead.    
    ///
    /// # Errors
    ///
    /// An ['Err'](Result::Err) may be returned if:
    /// - the file does not meet the Arrow Format footer requirements, or
    /// - file endianness does not match the target endianness.
    pub fn try_new(reader: R, projection: Option<Vec<usize>>) -> Result<Self, ArrowError> {
        let builder = FileReaderBuilder {
            projection,
            ..Default::default()
        };
        builder.build(reader)
    }

    /// Return user defined customized metadata
    pub fn custom_metadata(&self) -> &HashMap<String, String> {
        &self.custom_metadata
    }

    /// Return the number of batches in the file
    pub fn num_batches(&self) -> usize {
        self.total_blocks
    }

    /// Return the schema of the file
    pub fn schema(&self) -> SchemaRef {
        self.decoder.schema.clone()
    }

    /// See to a specific [`RecordBatch`]
    ///
    /// Sets the current block to the index, allowing random reads
    pub fn set_index(&mut self, index: usize) -> Result<(), ArrowError> {
        if index >= self.total_blocks {
            Err(ArrowError::InvalidArgumentError(format!(
                "Cannot set batch to index {} from {} total batches",
                index, self.total_blocks
            )))
        } else {
            self.current_block = index;
            Ok(())
        }
    }

    fn maybe_next(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        let block = &self.blocks[self.current_block];
        self.current_block += 1;

        // read length
        let buffer = read_block(&mut self.reader, block)?;
        self.decoder.read_record_batch(block, &buffer)
    }

    /// Gets a reference to the underlying reader.
    ///
    /// It is inadvisable to directly read from the underlying reader.
    pub fn get_ref(&self) -> &R {
        &self.reader
    }

    /// Gets a mutable reference to the underlying reader.
    ///
    /// It is inadvisable to directly read from the underlying reader.
    pub fn get_mut(&mut self) -> &mut R {
        &mut self.reader
    }

    /// Specifies if validation should be skipped when reading data (defaults to `false`)
    ///
    /// # Safety
    ///
    /// See [`FileDecoder::with_skip_validation`]
    pub unsafe fn with_skip_validation(mut self, skip_validation: bool) -> Self {
        self.decoder = self.decoder.with_skip_validation(skip_validation);
        self
    }
}

impl<R: Read + Seek> Iterator for FileReader<R> {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        // get current block
        if self.current_block < self.total_blocks {
            self.maybe_next().transpose()
        } else {
            None
        }
    }
}

impl<R: Read + Seek> RecordBatchReader for FileReader<R> {
    fn schema(&self) -> SchemaRef {
        self.schema()
    }
}

/// Arrow Stream Reader
///
/// Reads Arrow [`RecordBatch`]es from bytes in the [IPC Streaming Format].
///
/// # See Also
///
/// * [`FileReader`] for random access.
///
/// # Example
/// ```
/// # use arrow_array::record_batch;
/// # use arrow_ipc::reader::StreamReader;
/// # use arrow_ipc::writer::StreamWriter;
/// # let batch = record_batch!(("a", Int32, [1, 2, 3])).unwrap();
/// # let mut stream = vec![]; // mimic a stream for the example
/// # {
/// #  let mut writer = StreamWriter::try_new(&mut stream, &batch.schema()).unwrap();
/// #  writer.write(&batch).unwrap();
/// #  writer.finish().unwrap();
/// # }
/// # let stream = stream.as_slice();
/// let projection = None; // read all columns
/// let mut reader = StreamReader::try_new(stream, projection).unwrap();
/// // read batches from the reader using the Iterator trait
/// let mut num_rows = 0;
/// for batch in reader {
///    let batch = batch.unwrap();
///    num_rows += batch.num_rows();
/// }
/// assert_eq!(num_rows, 3);
/// ```
///
/// [IPC Streaming Format]: https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format
pub struct StreamReader<R> {
    /// Stream reader
    reader: R,

    /// The schema that is read from the stream's first message
    schema: SchemaRef,

    /// Optional dictionaries for each schema field.
    ///
    /// Dictionaries may be appended to in the streaming format.
    dictionaries_by_id: HashMap<i64, ArrayRef>,

    /// An indicator of whether the stream is complete.
    ///
    /// This value is set to `true` the first time the reader's `next()` returns `None`.
    finished: bool,

    /// Optional projection
    projection: Option<(Vec<usize>, Schema)>,

    /// Should validation be skipped when reading data? Defaults to false.
    ///
    /// See [`FileDecoder::with_skip_validation`] for details.
    skip_validation: UnsafeFlag,
}

impl<R> fmt::Debug for StreamReader<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::result::Result<(), fmt::Error> {
        f.debug_struct("StreamReader<R>")
            .field("reader", &"R")
            .field("schema", &self.schema)
            .field("dictionaries_by_id", &self.dictionaries_by_id)
            .field("finished", &self.finished)
            .field("projection", &self.projection)
            .finish()
    }
}

impl<R: Read> StreamReader<BufReader<R>> {
    /// Try to create a new stream reader with the reader wrapped in a BufReader.
    ///
    /// See [`StreamReader::try_new`] for an unbuffered version.
    pub fn try_new_buffered(reader: R, projection: Option<Vec<usize>>) -> Result<Self, ArrowError> {
        Self::try_new(BufReader::new(reader), projection)
    }
}

impl<R: Read> StreamReader<R> {
    /// Try to create a new stream reader.
    ///
    /// To check if the reader is done, use [`is_finished(self)`](StreamReader::is_finished).
    ///
    /// There is no internal buffering. If buffered reads are needed you likely want to use
    /// [`StreamReader::try_new_buffered`] instead.
    ///
    /// # Errors
    ///
    /// An ['Err'](Result::Err) may be returned if the reader does not encounter a schema
    /// as the first message in the stream.
    pub fn try_new(
        mut reader: R,
        projection: Option<Vec<usize>>,
    ) -> Result<StreamReader<R>, ArrowError> {
        // determine metadata length
        let mut meta_size: [u8; 4] = [0; 4];
        reader.read_exact(&mut meta_size)?;
        let meta_len = {
            // If a continuation marker is encountered, skip over it and read
            // the size from the next four bytes.
            if meta_size == CONTINUATION_MARKER {
                reader.read_exact(&mut meta_size)?;
            }
            i32::from_le_bytes(meta_size)
        };

        let mut meta_buffer = vec![0; meta_len as usize];
        reader.read_exact(&mut meta_buffer)?;

        let message = crate::root_as_message(meta_buffer.as_slice()).map_err(|err| {
            ArrowError::ParseError(format!("Unable to get root as message: {err:?}"))
        })?;
        // message header is a Schema, so read it
        let ipc_schema: crate::Schema = message.header_as_schema().ok_or_else(|| {
            ArrowError::ParseError("Unable to read IPC message as schema".to_string())
        })?;
        let schema = crate::convert::fb_to_schema(ipc_schema);

        // Create an array of optional dictionary value arrays, one per field.
        let dictionaries_by_id = HashMap::new();

        let projection = match projection {
            Some(projection_indices) => {
                let schema = schema.project(&projection_indices)?;
                Some((projection_indices, schema))
            }
            _ => None,
        };
        Ok(Self {
            reader,
            schema: Arc::new(schema),
            finished: false,
            dictionaries_by_id,
            projection,
            skip_validation: UnsafeFlag::new(),
        })
    }

    /// Deprecated, use [`StreamReader::try_new`] instead.
    #[deprecated(since = "53.0.0", note = "use `try_new` instead")]
    pub fn try_new_unbuffered(
        reader: R,
        projection: Option<Vec<usize>>,
    ) -> Result<Self, ArrowError> {
        Self::try_new(reader, projection)
    }

    /// Return the schema of the stream
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Check if the stream is finished
    pub fn is_finished(&self) -> bool {
        self.finished
    }

    fn maybe_next(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        if self.finished {
            return Ok(None);
        }
        // determine metadata length
        let mut meta_size: [u8; 4] = [0; 4];

        match self.reader.read_exact(&mut meta_size) {
            Ok(()) => (),
            Err(e) => {
                return if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    // Handle EOF without the "0xFFFFFFFF 0x00000000"
                    // valid according to:
                    // https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format
                    self.finished = true;
                    Ok(None)
                } else {
                    Err(ArrowError::from(e))
                };
            }
        }

        let meta_len = {
            // If a continuation marker is encountered, skip over it and read
            // the size from the next four bytes.
            if meta_size == CONTINUATION_MARKER {
                self.reader.read_exact(&mut meta_size)?;
            }
            i32::from_le_bytes(meta_size)
        };

        if meta_len == 0 {
            // the stream has ended, mark the reader as finished
            self.finished = true;
            return Ok(None);
        }

        let mut meta_buffer = vec![0; meta_len as usize];
        self.reader.read_exact(&mut meta_buffer)?;

        let vecs = &meta_buffer.to_vec();
        let message = crate::root_as_message(vecs).map_err(|err| {
            ArrowError::ParseError(format!("Unable to get root as message: {err:?}"))
        })?;

        match message.header_type() {
            crate::MessageHeader::Schema => Err(ArrowError::IpcError(
                "Not expecting a schema when messages are read".to_string(),
            )),
            crate::MessageHeader::RecordBatch => {
                let batch = message.header_as_record_batch().ok_or_else(|| {
                    ArrowError::IpcError("Unable to read IPC message as record batch".to_string())
                })?;
                // read the block that makes up the record batch into a buffer
                let mut buf = MutableBuffer::from_len_zeroed(message.bodyLength() as usize);
                self.reader.read_exact(&mut buf)?;

                RecordBatchDecoder::try_new(
                    &buf.into(),
                    batch,
                    self.schema(),
                    &self.dictionaries_by_id,
                    &message.version(),
                )?
                .with_projection(self.projection.as_ref().map(|x| x.0.as_ref()))
                .with_require_alignment(false)
                .with_skip_validation(self.skip_validation.clone())
                .read_record_batch()
                .map(Some)
            }
            crate::MessageHeader::DictionaryBatch => {
                let batch = message.header_as_dictionary_batch().ok_or_else(|| {
                    ArrowError::IpcError(
                        "Unable to read IPC message as dictionary batch".to_string(),
                    )
                })?;
                // read the block that makes up the dictionary batch into a buffer
                let mut buf = MutableBuffer::from_len_zeroed(message.bodyLength() as usize);
                self.reader.read_exact(&mut buf)?;

                read_dictionary_impl(
                    &buf.into(),
                    batch,
                    &self.schema,
                    &mut self.dictionaries_by_id,
                    &message.version(),
                    false,
                    self.skip_validation.clone(),
                )?;

                // read the next message until we encounter a RecordBatch
                self.maybe_next()
            }
            crate::MessageHeader::NONE => Ok(None),
            t => Err(ArrowError::InvalidArgumentError(format!(
                "Reading types other than record batches not yet supported, unable to read {t:?} "
            ))),
        }
    }

    /// Gets a reference to the underlying reader.
    ///
    /// It is inadvisable to directly read from the underlying reader.
    pub fn get_ref(&self) -> &R {
        &self.reader
    }

    /// Gets a mutable reference to the underlying reader.
    ///
    /// It is inadvisable to directly read from the underlying reader.
    pub fn get_mut(&mut self) -> &mut R {
        &mut self.reader
    }

    /// Specifies if validation should be skipped when reading data (defaults to `false`)
    ///
    /// # Safety
    ///
    /// See [`FileDecoder::with_skip_validation`]
    pub unsafe fn with_skip_validation(mut self, skip_validation: bool) -> Self {
        self.skip_validation.set(skip_validation);
        self
    }
}

impl<R: Read> Iterator for StreamReader<R> {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.maybe_next().transpose()
    }
}

impl<R: Read> RecordBatchReader for StreamReader<R> {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use crate::writer::{unslice_run_array, DictionaryTracker, IpcDataGenerator, IpcWriteOptions};

    use super::*;

    use crate::convert::fb_to_schema;
    use crate::{root_as_footer, root_as_message};
    use arrow_array::builder::{PrimitiveRunBuilder, UnionBuilder};
    use arrow_array::types::*;
    use arrow_buffer::{NullBuffer, OffsetBuffer};
    use arrow_data::ArrayDataBuilder;

    fn create_test_projection_schema() -> Schema {
        // define field types
        let list_data_type = DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true)));

        let fixed_size_list_data_type =
            DataType::FixedSizeList(Arc::new(Field::new_list_field(DataType::Int32, false)), 3);

        let union_fields = UnionFields::new(
            vec![0, 1],
            vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Float64, false),
            ],
        );

        let union_data_type = DataType::Union(union_fields, UnionMode::Dense);

        let struct_fields = Fields::from(vec![
            Field::new("id", DataType::Int32, false),
            Field::new_list("list", Field::new_list_field(DataType::Int8, true), false),
        ]);
        let struct_data_type = DataType::Struct(struct_fields);

        let run_encoded_data_type = DataType::RunEndEncoded(
            Arc::new(Field::new("run_ends", DataType::Int16, false)),
            Arc::new(Field::new("values", DataType::Int32, true)),
        );

        // define schema
        Schema::new(vec![
            Field::new("f0", DataType::UInt32, false),
            Field::new("f1", DataType::Utf8, false),
            Field::new("f2", DataType::Boolean, false),
            Field::new("f3", union_data_type, true),
            Field::new("f4", DataType::Null, true),
            Field::new("f5", DataType::Float64, true),
            Field::new("f6", list_data_type, false),
            Field::new("f7", DataType::FixedSizeBinary(3), true),
            Field::new("f8", fixed_size_list_data_type, false),
            Field::new("f9", struct_data_type, false),
            Field::new("f10", run_encoded_data_type, false),
            Field::new("f11", DataType::Boolean, false),
            Field::new_dictionary("f12", DataType::Int8, DataType::Utf8, false),
            Field::new("f13", DataType::Utf8, false),
        ])
    }

    fn create_test_projection_batch_data(schema: &Schema) -> RecordBatch {
        // set test data for each column
        let array0 = UInt32Array::from(vec![1, 2, 3]);
        let array1 = StringArray::from(vec!["foo", "bar", "baz"]);
        let array2 = BooleanArray::from(vec![true, false, true]);

        let mut union_builder = UnionBuilder::new_dense();
        union_builder.append::<Int32Type>("a", 1).unwrap();
        union_builder.append::<Float64Type>("b", 10.1).unwrap();
        union_builder.append_null::<Float64Type>("b").unwrap();
        let array3 = union_builder.build().unwrap();

        let array4 = NullArray::new(3);
        let array5 = Float64Array::from(vec![Some(1.1), None, Some(3.3)]);
        let array6_values = vec![
            Some(vec![Some(10), Some(10), Some(10)]),
            Some(vec![Some(20), Some(20), Some(20)]),
            Some(vec![Some(30), Some(30)]),
        ];
        let array6 = ListArray::from_iter_primitive::<Int32Type, _, _>(array6_values);
        let array7_values = vec![vec![11, 12, 13], vec![22, 23, 24], vec![33, 34, 35]];
        let array7 = FixedSizeBinaryArray::try_from_iter(array7_values.into_iter()).unwrap();

        let array8_values = ArrayData::builder(DataType::Int32)
            .len(9)
            .add_buffer(Buffer::from_slice_ref([40, 41, 42, 43, 44, 45, 46, 47, 48]))
            .build()
            .unwrap();
        let array8_data = ArrayData::builder(schema.field(8).data_type().clone())
            .len(3)
            .add_child_data(array8_values)
            .build()
            .unwrap();
        let array8 = FixedSizeListArray::from(array8_data);

        let array9_id: ArrayRef = Arc::new(Int32Array::from(vec![1001, 1002, 1003]));
        let array9_list: ArrayRef =
            Arc::new(ListArray::from_iter_primitive::<Int8Type, _, _>(vec![
                Some(vec![Some(-10)]),
                Some(vec![Some(-20), Some(-20), Some(-20)]),
                Some(vec![Some(-30)]),
            ]));
        let array9 = ArrayDataBuilder::new(schema.field(9).data_type().clone())
            .add_child_data(array9_id.into_data())
            .add_child_data(array9_list.into_data())
            .len(3)
            .build()
            .unwrap();
        let array9: ArrayRef = Arc::new(StructArray::from(array9));

        let array10_input = vec![Some(1_i32), None, None];
        let mut array10_builder = PrimitiveRunBuilder::<Int16Type, Int32Type>::new();
        array10_builder.extend(array10_input);
        let array10 = array10_builder.finish();

        let array11 = BooleanArray::from(vec![false, false, true]);

        let array12_values = StringArray::from(vec!["x", "yy", "zzz"]);
        let array12_keys = Int8Array::from_iter_values([1, 1, 2]);
        let array12 = DictionaryArray::new(array12_keys, Arc::new(array12_values));

        let array13 = StringArray::from(vec!["a", "bb", "ccc"]);

        // create record batch
        RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(array0),
                Arc::new(array1),
                Arc::new(array2),
                Arc::new(array3),
                Arc::new(array4),
                Arc::new(array5),
                Arc::new(array6),
                Arc::new(array7),
                Arc::new(array8),
                Arc::new(array9),
                Arc::new(array10),
                Arc::new(array11),
                Arc::new(array12),
                Arc::new(array13),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_projection_array_values() {
        // define schema
        let schema = create_test_projection_schema();

        // create record batch with test data
        let batch = create_test_projection_batch_data(&schema);

        // write record batch in IPC format
        let mut buf = Vec::new();
        {
            let mut writer = crate::writer::FileWriter::try_new(&mut buf, &schema).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        // read record batch with projection
        for index in 0..12 {
            let projection = vec![index];
            let reader = FileReader::try_new(std::io::Cursor::new(buf.clone()), Some(projection));
            let read_batch = reader.unwrap().next().unwrap().unwrap();
            let projected_column = read_batch.column(0);
            let expected_column = batch.column(index);

            // check the projected column equals the expected column
            assert_eq!(projected_column.as_ref(), expected_column.as_ref());
        }

        {
            // read record batch with reversed projection
            let reader =
                FileReader::try_new(std::io::Cursor::new(buf.clone()), Some(vec![3, 2, 1]));
            let read_batch = reader.unwrap().next().unwrap().unwrap();
            let expected_batch = batch.project(&[3, 2, 1]).unwrap();
            assert_eq!(read_batch, expected_batch);
        }
    }

    #[test]
    fn test_arrow_single_float_row() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Float32, false),
            Field::new("b", DataType::Float32, false),
            Field::new("c", DataType::Int32, false),
            Field::new("d", DataType::Int32, false),
        ]);
        let arrays = vec![
            Arc::new(Float32Array::from(vec![1.23])) as ArrayRef,
            Arc::new(Float32Array::from(vec![-6.50])) as ArrayRef,
            Arc::new(Int32Array::from(vec![2])) as ArrayRef,
            Arc::new(Int32Array::from(vec![1])) as ArrayRef,
        ];
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), arrays).unwrap();
        // create stream writer
        let mut file = tempfile::tempfile().unwrap();
        let mut stream_writer = crate::writer::StreamWriter::try_new(&mut file, &schema).unwrap();
        stream_writer.write(&batch).unwrap();
        stream_writer.finish().unwrap();

        drop(stream_writer);

        file.rewind().unwrap();

        // read stream back
        let reader = StreamReader::try_new(&mut file, None).unwrap();

        reader.for_each(|batch| {
            let batch = batch.unwrap();
            assert!(
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .unwrap()
                    .value(0)
                    != 0.0
            );
            assert!(
                batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .unwrap()
                    .value(0)
                    != 0.0
            );
        });

        file.rewind().unwrap();

        // Read with projection
        let reader = StreamReader::try_new(file, Some(vec![0, 3])).unwrap();

        reader.for_each(|batch| {
            let batch = batch.unwrap();
            assert_eq!(batch.schema().fields().len(), 2);
            assert_eq!(batch.schema().fields()[0].data_type(), &DataType::Float32);
            assert_eq!(batch.schema().fields()[1].data_type(), &DataType::Int32);
        });
    }

    /// Write the record batch to an in-memory buffer in IPC File format
    fn write_ipc(rb: &RecordBatch) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut writer = crate::writer::FileWriter::try_new(&mut buf, rb.schema_ref()).unwrap();
        writer.write(rb).unwrap();
        writer.finish().unwrap();
        buf
    }

    /// Return the first record batch read from the IPC File buffer
    fn read_ipc(buf: &[u8]) -> Result<RecordBatch, ArrowError> {
        let mut reader = FileReader::try_new(std::io::Cursor::new(buf), None)?;
        reader.next().unwrap()
    }

    /// Return the first record batch read from the IPC File buffer, disabling
    /// validation
    fn read_ipc_skip_validation(buf: &[u8]) -> Result<RecordBatch, ArrowError> {
        let mut reader = unsafe {
            FileReader::try_new(std::io::Cursor::new(buf), None)?.with_skip_validation(true)
        };
        reader.next().unwrap()
    }

    fn roundtrip_ipc(rb: &RecordBatch) -> RecordBatch {
        let buf = write_ipc(rb);
        read_ipc(&buf).unwrap()
    }

    /// Return the first record batch read from the IPC File buffer
    /// using the FileDecoder API
    fn read_ipc_with_decoder(buf: Vec<u8>) -> Result<RecordBatch, ArrowError> {
        read_ipc_with_decoder_inner(buf, false)
    }

    /// Return the first record batch read from the IPC File buffer
    /// using the FileDecoder API, disabling validation
    fn read_ipc_with_decoder_skip_validation(buf: Vec<u8>) -> Result<RecordBatch, ArrowError> {
        read_ipc_with_decoder_inner(buf, true)
    }

    fn read_ipc_with_decoder_inner(
        buf: Vec<u8>,
        skip_validation: bool,
    ) -> Result<RecordBatch, ArrowError> {
        let buffer = Buffer::from_vec(buf);
        let trailer_start = buffer.len() - 10;
        let footer_len = read_footer_length(buffer[trailer_start..].try_into().unwrap())?;
        let footer = root_as_footer(&buffer[trailer_start - footer_len..trailer_start])
            .map_err(|e| ArrowError::InvalidArgumentError(format!("Invalid footer: {e}")))?;

        let schema = fb_to_schema(footer.schema().unwrap());

        let mut decoder = unsafe {
            FileDecoder::new(Arc::new(schema), footer.version())
                .with_skip_validation(skip_validation)
        };
        // Read dictionaries
        for block in footer.dictionaries().iter().flatten() {
            let block_len = block.bodyLength() as usize + block.metaDataLength() as usize;
            let data = buffer.slice_with_length(block.offset() as _, block_len);
            decoder.read_dictionary(block, &data)?
        }

        // Read record batch
        let batches = footer.recordBatches().unwrap();
        assert_eq!(batches.len(), 1); // Only wrote a single batch

        let block = batches.get(0);
        let block_len = block.bodyLength() as usize + block.metaDataLength() as usize;
        let data = buffer.slice_with_length(block.offset() as _, block_len);
        Ok(decoder.read_record_batch(block, &data)?.unwrap())
    }

    /// Write the record batch to an in-memory buffer in IPC Stream format
    fn write_stream(rb: &RecordBatch) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut writer = crate::writer::StreamWriter::try_new(&mut buf, rb.schema_ref()).unwrap();
        writer.write(rb).unwrap();
        writer.finish().unwrap();
        buf
    }

    /// Return the first record batch read from the IPC Stream buffer
    fn read_stream(buf: &[u8]) -> Result<RecordBatch, ArrowError> {
        let mut reader = StreamReader::try_new(std::io::Cursor::new(buf), None)?;
        reader.next().unwrap()
    }

    /// Return the first record batch read from the IPC Stream buffer,
    /// disabling validation
    fn read_stream_skip_validation(buf: &[u8]) -> Result<RecordBatch, ArrowError> {
        let mut reader = unsafe {
            StreamReader::try_new(std::io::Cursor::new(buf), None)?.with_skip_validation(true)
        };
        reader.next().unwrap()
    }

    fn roundtrip_ipc_stream(rb: &RecordBatch) -> RecordBatch {
        let buf = write_stream(rb);
        read_stream(&buf).unwrap()
    }

    #[test]
    fn test_roundtrip_with_custom_metadata() {
        let schema = Schema::new(vec![Field::new("dummy", DataType::Float64, false)]);
        let mut buf = Vec::new();
        let mut writer = crate::writer::FileWriter::try_new(&mut buf, &schema).unwrap();
        let mut test_metadata = HashMap::new();
        test_metadata.insert("abc".to_string(), "abc".to_string());
        test_metadata.insert("def".to_string(), "def".to_string());
        for (k, v) in &test_metadata {
            writer.write_metadata(k, v);
        }
        writer.finish().unwrap();
        drop(writer);

        let reader = crate::reader::FileReader::try_new(std::io::Cursor::new(buf), None).unwrap();
        assert_eq!(reader.custom_metadata(), &test_metadata);
    }

    #[test]
    fn test_roundtrip_nested_dict() {
        let inner: DictionaryArray<Int32Type> = vec!["a", "b", "a"].into_iter().collect();

        let array = Arc::new(inner) as ArrayRef;

        let dctfield = Arc::new(Field::new("dict", array.data_type().clone(), false));

        let s = StructArray::from(vec![(dctfield, array)]);
        let struct_array = Arc::new(s) as ArrayRef;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "struct",
            struct_array.data_type().clone(),
            false,
        )]));

        let batch = RecordBatch::try_new(schema, vec![struct_array]).unwrap();

        assert_eq!(batch, roundtrip_ipc(&batch));
    }

    #[test]
    fn test_roundtrip_nested_dict_no_preserve_dict_id() {
        let inner: DictionaryArray<Int32Type> = vec!["a", "b", "a"].into_iter().collect();

        let array = Arc::new(inner) as ArrayRef;

        let dctfield = Arc::new(Field::new("dict", array.data_type().clone(), false));

        let s = StructArray::from(vec![(dctfield, array)]);
        let struct_array = Arc::new(s) as ArrayRef;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "struct",
            struct_array.data_type().clone(),
            false,
        )]));

        let batch = RecordBatch::try_new(schema, vec![struct_array]).unwrap();

        let mut buf = Vec::new();
        let mut writer = crate::writer::FileWriter::try_new_with_options(
            &mut buf,
            batch.schema_ref(),
            #[allow(deprecated)]
            IpcWriteOptions::default().with_preserve_dict_id(false),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        drop(writer);

        let mut reader = FileReader::try_new(std::io::Cursor::new(buf), None).unwrap();

        assert_eq!(batch, reader.next().unwrap().unwrap());
    }

    fn check_union_with_builder(mut builder: UnionBuilder) {
        builder.append::<Int32Type>("a", 1).unwrap();
        builder.append_null::<Int32Type>("a").unwrap();
        builder.append::<Float64Type>("c", 3.0).unwrap();
        builder.append::<Int32Type>("a", 4).unwrap();
        builder.append::<Int64Type>("d", 11).unwrap();
        let union = builder.build().unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "union",
            union.data_type().clone(),
            false,
        )]));

        let union_array = Arc::new(union) as ArrayRef;

        let rb = RecordBatch::try_new(schema, vec![union_array]).unwrap();
        let rb2 = roundtrip_ipc(&rb);
        // TODO: equality not yet implemented for union, so we check that the length of the array is
        // the same and that all of the buffers are the same instead.
        assert_eq!(rb.schema(), rb2.schema());
        assert_eq!(rb.num_columns(), rb2.num_columns());
        assert_eq!(rb.num_rows(), rb2.num_rows());
        let union1 = rb.column(0);
        let union2 = rb2.column(0);

        assert_eq!(union1, union2);
    }

    #[test]
    fn test_roundtrip_dense_union() {
        check_union_with_builder(UnionBuilder::new_dense());
    }

    #[test]
    fn test_roundtrip_sparse_union() {
        check_union_with_builder(UnionBuilder::new_sparse());
    }

    #[test]
    fn test_roundtrip_struct_empty_fields() {
        let nulls = NullBuffer::from(&[true, true, false]);
        let rb = RecordBatch::try_from_iter([(
            "",
            Arc::new(StructArray::new_empty_fields(nulls.len(), Some(nulls))) as _,
        )])
        .unwrap();
        let rb2 = roundtrip_ipc(&rb);
        assert_eq!(rb, rb2);
    }

    #[test]
    fn test_roundtrip_stream_run_array_sliced() {
        let run_array_1: Int32RunArray = vec!["a", "a", "a", "b", "b", "c", "c", "c"]
            .into_iter()
            .collect();
        let run_array_1_sliced = run_array_1.slice(2, 5);

        let run_array_2_inupt = vec![Some(1_i32), None, None, Some(2), Some(2)];
        let mut run_array_2_builder = PrimitiveRunBuilder::<Int16Type, Int32Type>::new();
        run_array_2_builder.extend(run_array_2_inupt);
        let run_array_2 = run_array_2_builder.finish();

        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "run_array_1_sliced",
                run_array_1_sliced.data_type().clone(),
                false,
            ),
            Field::new("run_array_2", run_array_2.data_type().clone(), false),
        ]));
        let input_batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(run_array_1_sliced.clone()), Arc::new(run_array_2)],
        )
        .unwrap();
        let output_batch = roundtrip_ipc_stream(&input_batch);

        // As partial comparison not yet supported for run arrays, the sliced run array
        // has to be unsliced before comparing with the output. the second run array
        // can be compared as such.
        assert_eq!(input_batch.column(1), output_batch.column(1));

        let run_array_1_unsliced = unslice_run_array(run_array_1_sliced.into_data()).unwrap();
        assert_eq!(run_array_1_unsliced, output_batch.column(0).into_data());
    }

    #[test]
    fn test_roundtrip_stream_nested_dict() {
        let xs = vec!["AA", "BB", "AA", "CC", "BB"];
        let dict = Arc::new(
            xs.clone()
                .into_iter()
                .collect::<DictionaryArray<Int8Type>>(),
        );
        let string_array: ArrayRef = Arc::new(StringArray::from(xs.clone()));
        let struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new("f2.1", DataType::Utf8, false)),
                string_array,
            ),
            (
                Arc::new(Field::new("f2.2_struct", dict.data_type().clone(), false)),
                dict.clone() as ArrayRef,
            ),
        ]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("f1_string", DataType::Utf8, false),
            Field::new("f2_struct", struct_array.data_type().clone(), false),
        ]));
        let input_batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(xs.clone())),
                Arc::new(struct_array),
            ],
        )
        .unwrap();
        let output_batch = roundtrip_ipc_stream(&input_batch);
        assert_eq!(input_batch, output_batch);
    }

    #[test]
    fn test_roundtrip_stream_nested_dict_of_map_of_dict() {
        let values = StringArray::from(vec![Some("a"), None, Some("b"), Some("c")]);
        let values = Arc::new(values) as ArrayRef;
        let value_dict_keys = Int8Array::from_iter_values([0, 1, 1, 2, 3, 1]);
        let value_dict_array = DictionaryArray::new(value_dict_keys, values.clone());

        let key_dict_keys = Int8Array::from_iter_values([0, 0, 2, 1, 1, 3]);
        let key_dict_array = DictionaryArray::new(key_dict_keys, values);

        #[allow(deprecated)]
        let keys_field = Arc::new(Field::new_dict(
            "keys",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            true, // It is technically not legal for this field to be null.
            1,
            false,
        ));
        #[allow(deprecated)]
        let values_field = Arc::new(Field::new_dict(
            "values",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            true,
            2,
            false,
        ));
        let entry_struct = StructArray::from(vec![
            (keys_field, make_array(key_dict_array.into_data())),
            (values_field, make_array(value_dict_array.into_data())),
        ]);
        let map_data_type = DataType::Map(
            Arc::new(Field::new(
                "entries",
                entry_struct.data_type().clone(),
                false,
            )),
            false,
        );

        let entry_offsets = Buffer::from_slice_ref([0, 2, 4, 6]);
        let map_data = ArrayData::builder(map_data_type)
            .len(3)
            .add_buffer(entry_offsets)
            .add_child_data(entry_struct.into_data())
            .build()
            .unwrap();
        let map_array = MapArray::from(map_data);

        let dict_keys = Int8Array::from_iter_values([0, 1, 1, 2, 2, 1]);
        let dict_dict_array = DictionaryArray::new(dict_keys, Arc::new(map_array));

        let schema = Arc::new(Schema::new(vec![Field::new(
            "f1",
            dict_dict_array.data_type().clone(),
            false,
        )]));
        let input_batch = RecordBatch::try_new(schema, vec![Arc::new(dict_dict_array)]).unwrap();
        let output_batch = roundtrip_ipc_stream(&input_batch);
        assert_eq!(input_batch, output_batch);
    }

    fn test_roundtrip_stream_dict_of_list_of_dict_impl<
        OffsetSize: OffsetSizeTrait,
        U: ArrowNativeType,
    >(
        list_data_type: DataType,
        offsets: &[U; 5],
    ) {
        let values = StringArray::from(vec![Some("a"), None, Some("c"), None]);
        let keys = Int8Array::from_iter_values([0, 0, 1, 2, 0, 1, 3]);
        let dict_array = DictionaryArray::new(keys, Arc::new(values));
        let dict_data = dict_array.to_data();

        let value_offsets = Buffer::from_slice_ref(offsets);

        let list_data = ArrayData::builder(list_data_type)
            .len(4)
            .add_buffer(value_offsets)
            .add_child_data(dict_data)
            .build()
            .unwrap();
        let list_array = GenericListArray::<OffsetSize>::from(list_data);

        let keys_for_dict = Int8Array::from_iter_values([0, 3, 0, 1, 1, 2, 0, 1, 3]);
        let dict_dict_array = DictionaryArray::new(keys_for_dict, Arc::new(list_array));

        let schema = Arc::new(Schema::new(vec![Field::new(
            "f1",
            dict_dict_array.data_type().clone(),
            false,
        )]));
        let input_batch = RecordBatch::try_new(schema, vec![Arc::new(dict_dict_array)]).unwrap();
        let output_batch = roundtrip_ipc_stream(&input_batch);
        assert_eq!(input_batch, output_batch);
    }

    #[test]
    fn test_roundtrip_stream_dict_of_list_of_dict() {
        // list
        #[allow(deprecated)]
        let list_data_type = DataType::List(Arc::new(Field::new_dict(
            "item",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            true,
            1,
            false,
        )));
        let offsets: &[i32; 5] = &[0, 2, 4, 4, 6];
        test_roundtrip_stream_dict_of_list_of_dict_impl::<i32, i32>(list_data_type, offsets);

        // large list
        #[allow(deprecated)]
        let list_data_type = DataType::LargeList(Arc::new(Field::new_dict(
            "item",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            true,
            1,
            false,
        )));
        let offsets: &[i64; 5] = &[0, 2, 4, 4, 7];
        test_roundtrip_stream_dict_of_list_of_dict_impl::<i64, i64>(list_data_type, offsets);
    }

    #[test]
    fn test_roundtrip_stream_dict_of_fixed_size_list_of_dict() {
        let values = StringArray::from(vec![Some("a"), None, Some("c"), None]);
        let keys = Int8Array::from_iter_values([0, 0, 1, 2, 0, 1, 3, 1, 2]);
        let dict_array = DictionaryArray::new(keys, Arc::new(values));
        let dict_data = dict_array.into_data();

        #[allow(deprecated)]
        let list_data_type = DataType::FixedSizeList(
            Arc::new(Field::new_dict(
                "item",
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                true,
                1,
                false,
            )),
            3,
        );
        let list_data = ArrayData::builder(list_data_type)
            .len(3)
            .add_child_data(dict_data)
            .build()
            .unwrap();
        let list_array = FixedSizeListArray::from(list_data);

        let keys_for_dict = Int8Array::from_iter_values([0, 1, 0, 1, 1, 2, 0, 1, 2]);
        let dict_dict_array = DictionaryArray::new(keys_for_dict, Arc::new(list_array));

        let schema = Arc::new(Schema::new(vec![Field::new(
            "f1",
            dict_dict_array.data_type().clone(),
            false,
        )]));
        let input_batch = RecordBatch::try_new(schema, vec![Arc::new(dict_dict_array)]).unwrap();
        let output_batch = roundtrip_ipc_stream(&input_batch);
        assert_eq!(input_batch, output_batch);
    }

    const LONG_TEST_STRING: &str =
        "This is a long string to make sure binary view array handles it";

    #[test]
    fn test_roundtrip_view_types() {
        let schema = Schema::new(vec![
            Field::new("field_1", DataType::BinaryView, true),
            Field::new("field_2", DataType::Utf8, true),
            Field::new("field_3", DataType::Utf8View, true),
        ]);
        let bin_values: Vec<Option<&[u8]>> = vec![
            Some(b"foo"),
            None,
            Some(b"bar"),
            Some(LONG_TEST_STRING.as_bytes()),
        ];
        let utf8_values: Vec<Option<&str>> =
            vec![Some("foo"), None, Some("bar"), Some(LONG_TEST_STRING)];
        let bin_view_array = BinaryViewArray::from_iter(bin_values);
        let utf8_array = StringArray::from_iter(utf8_values.iter());
        let utf8_view_array = StringViewArray::from_iter(utf8_values);
        let record_batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(bin_view_array),
                Arc::new(utf8_array),
                Arc::new(utf8_view_array),
            ],
        )
        .unwrap();

        assert_eq!(record_batch, roundtrip_ipc(&record_batch));
        assert_eq!(record_batch, roundtrip_ipc_stream(&record_batch));

        let sliced_batch = record_batch.slice(1, 2);
        assert_eq!(sliced_batch, roundtrip_ipc(&sliced_batch));
        assert_eq!(sliced_batch, roundtrip_ipc_stream(&sliced_batch));
    }

    #[test]
    fn test_roundtrip_view_types_nested_dict() {
        let bin_values: Vec<Option<&[u8]>> = vec![
            Some(b"foo"),
            None,
            Some(b"bar"),
            Some(LONG_TEST_STRING.as_bytes()),
            Some(b"field"),
        ];
        let utf8_values: Vec<Option<&str>> = vec![
            Some("foo"),
            None,
            Some("bar"),
            Some(LONG_TEST_STRING),
            Some("field"),
        ];
        let bin_view_array = Arc::new(BinaryViewArray::from_iter(bin_values));
        let utf8_view_array = Arc::new(StringViewArray::from_iter(utf8_values));

        let key_dict_keys = Int8Array::from_iter_values([0, 0, 1, 2, 0, 1, 3]);
        let key_dict_array = DictionaryArray::new(key_dict_keys, utf8_view_array.clone());
        #[allow(deprecated)]
        let keys_field = Arc::new(Field::new_dict(
            "keys",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8View)),
            true,
            1,
            false,
        ));

        let value_dict_keys = Int8Array::from_iter_values([0, 3, 0, 1, 2, 0, 1]);
        let value_dict_array = DictionaryArray::new(value_dict_keys, bin_view_array);
        #[allow(deprecated)]
        let values_field = Arc::new(Field::new_dict(
            "values",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::BinaryView)),
            true,
            2,
            false,
        ));
        let entry_struct = StructArray::from(vec![
            (keys_field, make_array(key_dict_array.into_data())),
            (values_field, make_array(value_dict_array.into_data())),
        ]);

        let map_data_type = DataType::Map(
            Arc::new(Field::new(
                "entries",
                entry_struct.data_type().clone(),
                false,
            )),
            false,
        );
        let entry_offsets = Buffer::from_slice_ref([0, 2, 4, 7]);
        let map_data = ArrayData::builder(map_data_type)
            .len(3)
            .add_buffer(entry_offsets)
            .add_child_data(entry_struct.into_data())
            .build()
            .unwrap();
        let map_array = MapArray::from(map_data);

        let dict_keys = Int8Array::from_iter_values([0, 1, 0, 1, 1, 2, 0, 1, 2]);
        let dict_dict_array = DictionaryArray::new(dict_keys, Arc::new(map_array));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "f1",
            dict_dict_array.data_type().clone(),
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(dict_dict_array)]).unwrap();
        assert_eq!(batch, roundtrip_ipc(&batch));
        assert_eq!(batch, roundtrip_ipc_stream(&batch));

        let sliced_batch = batch.slice(1, 2);
        assert_eq!(sliced_batch, roundtrip_ipc(&sliced_batch));
        assert_eq!(sliced_batch, roundtrip_ipc_stream(&sliced_batch));
    }

    #[test]
    fn test_no_columns_batch() {
        let schema = Arc::new(Schema::empty());
        let options = RecordBatchOptions::new()
            .with_match_field_names(true)
            .with_row_count(Some(10));
        let input_batch = RecordBatch::try_new_with_options(schema, vec![], &options).unwrap();
        let output_batch = roundtrip_ipc_stream(&input_batch);
        assert_eq!(input_batch, output_batch);
    }

    #[test]
    fn test_unaligned() {
        let batch = RecordBatch::try_from_iter(vec![(
            "i32",
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as _,
        )])
        .unwrap();

        let gen = IpcDataGenerator {};
        #[allow(deprecated)]
        let mut dict_tracker = DictionaryTracker::new_with_preserve_dict_id(false, true);
        let (_, encoded) = gen
            .encoded_batch(&batch, &mut dict_tracker, &Default::default())
            .unwrap();

        let message = root_as_message(&encoded.ipc_message).unwrap();

        // Construct an unaligned buffer
        let mut buffer = MutableBuffer::with_capacity(encoded.arrow_data.len() + 1);
        buffer.push(0_u8);
        buffer.extend_from_slice(&encoded.arrow_data);
        let b = Buffer::from(buffer).slice(1);
        assert_ne!(b.as_ptr().align_offset(8), 0);

        let ipc_batch = message.header_as_record_batch().unwrap();
        let roundtrip = RecordBatchDecoder::try_new(
            &b,
            ipc_batch,
            batch.schema(),
            &Default::default(),
            &message.version(),
        )
        .unwrap()
        .with_require_alignment(false)
        .read_record_batch()
        .unwrap();
        assert_eq!(batch, roundtrip);
    }

    #[test]
    fn test_unaligned_throws_error_with_require_alignment() {
        let batch = RecordBatch::try_from_iter(vec![(
            "i32",
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as _,
        )])
        .unwrap();

        let gen = IpcDataGenerator {};
        #[allow(deprecated)]
        let mut dict_tracker = DictionaryTracker::new_with_preserve_dict_id(false, true);
        let (_, encoded) = gen
            .encoded_batch(&batch, &mut dict_tracker, &Default::default())
            .unwrap();

        let message = root_as_message(&encoded.ipc_message).unwrap();

        // Construct an unaligned buffer
        let mut buffer = MutableBuffer::with_capacity(encoded.arrow_data.len() + 1);
        buffer.push(0_u8);
        buffer.extend_from_slice(&encoded.arrow_data);
        let b = Buffer::from(buffer).slice(1);
        assert_ne!(b.as_ptr().align_offset(8), 0);

        let ipc_batch = message.header_as_record_batch().unwrap();
        let result = RecordBatchDecoder::try_new(
            &b,
            ipc_batch,
            batch.schema(),
            &Default::default(),
            &message.version(),
        )
        .unwrap()
        .with_require_alignment(true)
        .read_record_batch();

        let error = result.unwrap_err();
        assert_eq!(
            error.to_string(),
            "Invalid argument error: Misaligned buffers[0] in array of type Int32, \
             offset from expected alignment of 4 by 1"
        );
    }

    #[test]
    fn test_file_with_massive_column_count() {
        // 499_999 is upper limit for default settings (1_000_000)
        let limit = 600_000;

        let fields = (0..limit)
            .map(|i| Field::new(format!("{i}"), DataType::Boolean, false))
            .collect::<Vec<_>>();
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::new_empty(schema);

        let mut buf = Vec::new();
        let mut writer = crate::writer::FileWriter::try_new(&mut buf, batch.schema_ref()).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        drop(writer);

        let mut reader = FileReaderBuilder::new()
            .with_max_footer_fb_tables(1_500_000)
            .build(std::io::Cursor::new(buf))
            .unwrap();
        let roundtrip_batch = reader.next().unwrap().unwrap();

        assert_eq!(batch, roundtrip_batch);
    }

    #[test]
    fn test_file_with_deeply_nested_columns() {
        // 60 is upper limit for default settings (64)
        let limit = 61;

        let fields = (0..limit).fold(
            vec![Field::new("leaf", DataType::Boolean, false)],
            |field, index| vec![Field::new_struct(format!("{index}"), field, false)],
        );
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::new_empty(schema);

        let mut buf = Vec::new();
        let mut writer = crate::writer::FileWriter::try_new(&mut buf, batch.schema_ref()).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        drop(writer);

        let mut reader = FileReaderBuilder::new()
            .with_max_footer_fb_depth(65)
            .build(std::io::Cursor::new(buf))
            .unwrap();
        let roundtrip_batch = reader.next().unwrap().unwrap();

        assert_eq!(batch, roundtrip_batch);
    }

    #[test]
    fn test_invalid_struct_array_ipc_read_errors() {
        let a_field = Field::new("a", DataType::Int32, false);
        let b_field = Field::new("b", DataType::Int32, false);

        let schema = Arc::new(Schema::new(vec![Field::new_struct(
            "s",
            vec![a_field.clone(), b_field.clone()],
            false,
        )]));

        let a_array_data = ArrayData::builder(a_field.data_type().clone())
            .len(4)
            .add_buffer(Buffer::from_slice_ref([1, 2, 3, 4]))
            .build()
            .unwrap();
        let b_array_data = ArrayData::builder(b_field.data_type().clone())
            .len(3)
            .add_buffer(Buffer::from_slice_ref([5, 6, 7]))
            .build()
            .unwrap();

        let struct_data_type = schema.field(0).data_type();

        let invalid_struct_arr = unsafe {
            make_array(
                ArrayData::builder(struct_data_type.clone())
                    .len(4)
                    .add_child_data(a_array_data)
                    .add_child_data(b_array_data)
                    .build_unchecked(),
            )
        };
        expect_ipc_validation_error(
            Arc::new(invalid_struct_arr),
            "Invalid argument error: Incorrect array length for StructArray field \"b\", expected 4 got 3",
        );
    }

    #[test]
    fn test_invalid_nested_array_ipc_read_errors() {
        // one of the nested arrays has invalid data
        let a_field = Field::new("a", DataType::Int32, false);
        let b_field = Field::new("b", DataType::Utf8, false);

        let schema = Arc::new(Schema::new(vec![Field::new_struct(
            "s",
            vec![a_field.clone(), b_field.clone()],
            false,
        )]));

        let a_array_data = ArrayData::builder(a_field.data_type().clone())
            .len(4)
            .add_buffer(Buffer::from_slice_ref([1, 2, 3, 4]))
            .build()
            .unwrap();
        // invalid nested child array -- length is correct, but has invalid utf8 data
        let b_array_data = {
            let valid: &[u8] = b"   ";
            let mut invalid = vec![];
            invalid.extend_from_slice(b"ValidString");
            invalid.extend_from_slice(INVALID_UTF8_FIRST_CHAR);
            let binary_array =
                BinaryArray::from_iter(vec![None, Some(valid), None, Some(&invalid)]);
            let array = unsafe {
                StringArray::new_unchecked(
                    binary_array.offsets().clone(),
                    binary_array.values().clone(),
                    binary_array.nulls().cloned(),
                )
            };
            array.into_data()
        };
        let struct_data_type = schema.field(0).data_type();

        let invalid_struct_arr = unsafe {
            make_array(
                ArrayData::builder(struct_data_type.clone())
                    .len(4)
                    .add_child_data(a_array_data)
                    .add_child_data(b_array_data)
                    .build_unchecked(),
            )
        };
        expect_ipc_validation_error(
            Arc::new(invalid_struct_arr),
            "Invalid argument error: Invalid UTF8 sequence at string index 3 (3..18): invalid utf-8 sequence of 1 bytes from index 11",
        );
    }

    #[test]
    fn test_same_dict_id_without_preserve() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(
                ["a", "b"]
                    .iter()
                    .map(|name| {
                        #[allow(deprecated)]
                        Field::new_dict(
                            name.to_string(),
                            DataType::Dictionary(
                                Box::new(DataType::Int32),
                                Box::new(DataType::Utf8),
                            ),
                            true,
                            0,
                            false,
                        )
                    })
                    .collect::<Vec<Field>>(),
            )),
            vec![
                Arc::new(
                    vec![Some("c"), Some("d")]
                        .into_iter()
                        .collect::<DictionaryArray<Int32Type>>(),
                ) as ArrayRef,
                Arc::new(
                    vec![Some("e"), Some("f")]
                        .into_iter()
                        .collect::<DictionaryArray<Int32Type>>(),
                ) as ArrayRef,
            ],
        )
        .expect("Failed to create RecordBatch");

        // serialize the record batch as an IPC stream
        let mut buf = vec![];
        {
            let mut writer = crate::writer::StreamWriter::try_new_with_options(
                &mut buf,
                batch.schema().as_ref(),
                #[allow(deprecated)]
                crate::writer::IpcWriteOptions::default().with_preserve_dict_id(false),
            )
            .expect("Failed to create StreamWriter");
            writer.write(&batch).expect("Failed to write RecordBatch");
            writer.finish().expect("Failed to finish StreamWriter");
        }

        StreamReader::try_new(std::io::Cursor::new(buf), None)
            .expect("Failed to create StreamReader")
            .for_each(|decoded_batch| {
                assert_eq!(decoded_batch.expect("Failed to read RecordBatch"), batch);
            });
    }

    #[test]
    fn test_validation_of_invalid_list_array() {
        // ListArray with invalid offsets
        let array = unsafe {
            let values = Int32Array::from(vec![1, 2, 3]);
            let bad_offsets = ScalarBuffer::<i32>::from(vec![0, 2, 4, 2]); // offsets can't go backwards
            let offsets = OffsetBuffer::new_unchecked(bad_offsets); // INVALID array created
            let field = Field::new_list_field(DataType::Int32, true);
            let nulls = None;
            ListArray::new(Arc::new(field), offsets, Arc::new(values), nulls)
        };

        expect_ipc_validation_error(
            Arc::new(array),
            "Invalid argument error: Offset invariant failure: offset at position 2 out of bounds: 4 > 2"
        );
    }

    #[test]
    fn test_validation_of_invalid_string_array() {
        let valid: &[u8] = b"   ";
        let mut invalid = vec![];
        invalid.extend_from_slice(b"ThisStringIsCertainlyLongerThan12Bytes");
        invalid.extend_from_slice(INVALID_UTF8_FIRST_CHAR);
        let binary_array = BinaryArray::from_iter(vec![None, Some(valid), None, Some(&invalid)]);
        // data is not valid utf8 we can not construct a correct StringArray
        // safely, so purposely create an invalid StringArray
        let array = unsafe {
            StringArray::new_unchecked(
                binary_array.offsets().clone(),
                binary_array.values().clone(),
                binary_array.nulls().cloned(),
            )
        };
        expect_ipc_validation_error(
            Arc::new(array),
            "Invalid argument error: Invalid UTF8 sequence at string index 3 (3..45): invalid utf-8 sequence of 1 bytes from index 38"
        );
    }

    #[test]
    fn test_validation_of_invalid_string_view_array() {
        let valid: &[u8] = b"   ";
        let mut invalid = vec![];
        invalid.extend_from_slice(b"ThisStringIsCertainlyLongerThan12Bytes");
        invalid.extend_from_slice(INVALID_UTF8_FIRST_CHAR);
        let binary_view_array =
            BinaryViewArray::from_iter(vec![None, Some(valid), None, Some(&invalid)]);
        // data is not valid utf8 we can not construct a correct StringArray
        // safely, so purposely create an invalid StringArray
        let array = unsafe {
            StringViewArray::new_unchecked(
                binary_view_array.views().clone(),
                binary_view_array.data_buffers().to_vec(),
                binary_view_array.nulls().cloned(),
            )
        };
        expect_ipc_validation_error(
            Arc::new(array),
            "Invalid argument error: Encountered non-UTF-8 data at index 3: invalid utf-8 sequence of 1 bytes from index 38"
        );
    }

    /// return an invalid dictionary array (key is larger than values)
    /// ListArray with invalid offsets
    #[test]
    fn test_validation_of_invalid_dictionary_array() {
        let array = unsafe {
            let values = StringArray::from_iter_values(["a", "b", "c"]);
            let keys = Int32Array::from(vec![1, 200]); // keys are not valid for values
            DictionaryArray::new_unchecked(keys, Arc::new(values))
        };

        expect_ipc_validation_error(
            Arc::new(array),
            "Invalid argument error: Value at position 1 out of bounds: 200 (should be in [0, 2])",
        );
    }

    #[test]
    fn test_validation_of_invalid_union_array() {
        let array = unsafe {
            let fields = UnionFields::new(
                vec![1, 3], // typeids : type id 2 is not valid
                vec![
                    Field::new("a", DataType::Int32, false),
                    Field::new("b", DataType::Utf8, false),
                ],
            );
            let type_ids = ScalarBuffer::from(vec![1i8, 2, 3]); // 2 is invalid
            let offsets = None;
            let children: Vec<ArrayRef> = vec![
                Arc::new(Int32Array::from(vec![10, 20, 30])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")])),
            ];

            UnionArray::new_unchecked(fields, type_ids, offsets, children)
        };

        expect_ipc_validation_error(
            Arc::new(array),
            "Invalid argument error: Type Ids values must match one of the field type ids",
        );
    }

    /// Invalid Utf-8 sequence in the first character
    /// <https://stackoverflow.com/questions/1301402/example-invalid-utf8-string>
    const INVALID_UTF8_FIRST_CHAR: &[u8] = &[0xa0, 0xa1, 0x20, 0x20];

    /// Expect an error when reading the record batch using IPC or IPC Streams
    fn expect_ipc_validation_error(array: ArrayRef, expected_err: &str) {
        let rb = RecordBatch::try_from_iter([("a", array)]).unwrap();

        // IPC Stream format
        let buf = write_stream(&rb); // write is ok
        read_stream_skip_validation(&buf).unwrap();
        let err = read_stream(&buf).unwrap_err();
        assert_eq!(err.to_string(), expected_err);

        // IPC File format
        let buf = write_ipc(&rb); // write is ok
        read_ipc_skip_validation(&buf).unwrap();
        let err = read_ipc(&buf).unwrap_err();
        assert_eq!(err.to_string(), expected_err);

        // IPC Format with FileDecoder
        read_ipc_with_decoder_skip_validation(buf.clone()).unwrap();
        let err = read_ipc_with_decoder(buf).unwrap_err();
        assert_eq!(err.to_string(), expected_err);
    }
}
