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
//! The `FileReader` and `StreamReader` have similar interfaces,
//! however the `FileReader` expects a reader that supports `Seek`ing

use arrow_buffer::i256;
use std::collections::HashMap;
use std::fmt;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::sync::Arc;

use arrow_array::*;
use arrow_buffer::{Buffer, MutableBuffer};
use arrow_cast::cast;
use arrow_data::ArrayData;
use arrow_schema::*;

use crate::compression::CompressionCodec;
use crate::CONTINUATION_MARKER;
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
    compression_codec: &Option<CompressionCodec>,
) -> Result<Buffer, ArrowError> {
    let start_offset = buf.offset() as usize;
    let buf_data = a_data.slice_with_length(start_offset, buf.length() as usize);
    // corner case: empty buffer
    match (buf_data.is_empty(), compression_codec) {
        (true, _) | (_, None) => Ok(buf_data),
        (false, Some(decompressor)) => decompressor.decompress_to_buffer(&buf_data),
    }
}

/// Coordinates reading arrays based on data types.
///
/// Notes:
/// * In the IPC format, null buffers are always set, but may be empty. We discard them if an array has 0 nulls
/// * Numeric values inside list arrays are often stored as 64-bit values regardless of their data type size.
///   We thus:
///     - check if the bit width of non-64-bit numbers is 64, and
///     - read the buffer as 64-bit (signed integer or float), and
///     - cast the 64-bit array to the appropriate data type
#[allow(clippy::too_many_arguments)]
fn create_array(
    nodes: flatbuffers::Vector<'_, crate::FieldNode>,
    field: &Field,
    data: &Buffer,
    buffers: flatbuffers::Vector<'_, crate::Buffer>,
    dictionaries_by_id: &HashMap<i64, ArrayRef>,
    mut node_index: usize,
    mut buffer_index: usize,
    compression_codec: &Option<CompressionCodec>,
    metadata: &crate::MetadataVersion,
) -> Result<(ArrayRef, usize, usize), ArrowError> {
    let data_type = field.data_type();
    let array = match data_type {
        Utf8 | Binary | LargeBinary | LargeUtf8 => {
            let array = create_primitive_array(
                nodes.get(node_index),
                data_type,
                &[
                    read_buffer(buffers.get(buffer_index), data, compression_codec)?,
                    read_buffer(buffers.get(buffer_index + 1), data, compression_codec)?,
                    read_buffer(buffers.get(buffer_index + 2), data, compression_codec)?,
                ],
            )?;
            node_index += 1;
            buffer_index += 3;
            array
        }
        FixedSizeBinary(_) => {
            let array = create_primitive_array(
                nodes.get(node_index),
                data_type,
                &[
                    read_buffer(buffers.get(buffer_index), data, compression_codec)?,
                    read_buffer(buffers.get(buffer_index + 1), data, compression_codec)?,
                ],
            )?;
            node_index += 1;
            buffer_index += 2;
            array
        }
        List(ref list_field) | LargeList(ref list_field) | Map(ref list_field, _) => {
            let list_node = nodes.get(node_index);
            let list_buffers = [
                read_buffer(buffers.get(buffer_index), data, compression_codec)?,
                read_buffer(buffers.get(buffer_index + 1), data, compression_codec)?,
            ];
            node_index += 1;
            buffer_index += 2;
            let triple = create_array(
                nodes,
                list_field,
                data,
                buffers,
                dictionaries_by_id,
                node_index,
                buffer_index,
                compression_codec,
                metadata,
            )?;
            node_index = triple.1;
            buffer_index = triple.2;

            create_list_array(list_node, data_type, &list_buffers, triple.0)?
        }
        FixedSizeList(ref list_field, _) => {
            let list_node = nodes.get(node_index);
            let list_buffers = [read_buffer(
                buffers.get(buffer_index),
                data,
                compression_codec,
            )?];
            node_index += 1;
            buffer_index += 1;
            let triple = create_array(
                nodes,
                list_field,
                data,
                buffers,
                dictionaries_by_id,
                node_index,
                buffer_index,
                compression_codec,
                metadata,
            )?;
            node_index = triple.1;
            buffer_index = triple.2;

            create_list_array(list_node, data_type, &list_buffers, triple.0)?
        }
        Struct(struct_fields) => {
            let struct_node = nodes.get(node_index);
            let null_buffer =
                read_buffer(buffers.get(buffer_index), data, compression_codec)?;
            node_index += 1;
            buffer_index += 1;

            // read the arrays for each field
            let mut struct_arrays = vec![];
            // TODO investigate whether just knowing the number of buffers could
            // still work
            for struct_field in struct_fields {
                let triple = create_array(
                    nodes,
                    struct_field,
                    data,
                    buffers,
                    dictionaries_by_id,
                    node_index,
                    buffer_index,
                    compression_codec,
                    metadata,
                )?;
                node_index = triple.1;
                buffer_index = triple.2;
                struct_arrays.push((struct_field.clone(), triple.0));
            }
            let null_count = struct_node.null_count() as usize;
            let struct_array = if null_count > 0 {
                // create struct array from fields, arrays and null data
                StructArray::from((struct_arrays, null_buffer))
            } else {
                StructArray::from(struct_arrays)
            };
            Arc::new(struct_array)
        }
        // Create dictionary array from RecordBatch
        Dictionary(_, _) => {
            let index_node = nodes.get(node_index);
            let index_buffers = [
                read_buffer(buffers.get(buffer_index), data, compression_codec)?,
                read_buffer(buffers.get(buffer_index + 1), data, compression_codec)?,
            ];

            let dict_id = field.dict_id().ok_or_else(|| {
                ArrowError::IoError(format!("Field {} does not have dict id", field))
            })?;

            let value_array = dictionaries_by_id.get(&dict_id).ok_or_else(|| {
                ArrowError::IoError(format!(
                    "Cannot find a dictionary batch with dict id: {}",
                    dict_id
                ))
            })?;
            node_index += 1;
            buffer_index += 2;

            create_dictionary_array(
                index_node,
                data_type,
                &index_buffers,
                value_array.clone(),
            )?
        }
        Union(fields, field_type_ids, mode) => {
            let union_node = nodes.get(node_index);
            node_index += 1;

            let len = union_node.length() as usize;

            // In V4, union types has validity bitmap
            // In V5 and later, union types have no validity bitmap
            if metadata < &crate::MetadataVersion::V5 {
                read_buffer(buffers.get(buffer_index), data, compression_codec)?;
                buffer_index += 1;
            }

            let type_ids: Buffer =
                read_buffer(buffers.get(buffer_index), data, compression_codec)?[..len]
                    .into();

            buffer_index += 1;

            let value_offsets = match mode {
                UnionMode::Dense => {
                    let buffer =
                        read_buffer(buffers.get(buffer_index), data, compression_codec)?;
                    buffer_index += 1;
                    Some(buffer[..len * 4].into())
                }
                UnionMode::Sparse => None,
            };

            let mut children = vec![];

            for field in fields {
                let triple = create_array(
                    nodes,
                    field,
                    data,
                    buffers,
                    dictionaries_by_id,
                    node_index,
                    buffer_index,
                    compression_codec,
                    metadata,
                )?;

                node_index = triple.1;
                buffer_index = triple.2;

                children.push((field.clone(), triple.0));
            }

            let array =
                UnionArray::try_new(field_type_ids, type_ids, value_offsets, children)?;
            Arc::new(array)
        }
        Null => {
            let node = nodes.get(node_index);
            let length = node.length();
            let null_count = node.null_count();

            if length != null_count {
                return Err(ArrowError::IoError(format!(
                    "Field {} of NullArray has unequal null_count {} and len {}",
                    field, null_count, length
                )));
            }

            let data = ArrayData::builder(data_type.clone())
                .len(length as usize)
                .offset(0)
                .build()
                .unwrap();
            node_index += 1;
            // no buffer increases
            make_array(data)
        }
        _ => {
            let array = create_primitive_array(
                nodes.get(node_index),
                data_type,
                &[
                    read_buffer(buffers.get(buffer_index), data, compression_codec)?,
                    read_buffer(buffers.get(buffer_index + 1), data, compression_codec)?,
                ],
            )?;
            node_index += 1;
            buffer_index += 2;
            array
        }
    };
    Ok((array, node_index, buffer_index))
}

/// Skip fields based on data types to advance `node_index` and `buffer_index`.
/// This function should be called when doing projection in fn `read_record_batch`.
/// The advancement logic references fn `create_array`.
fn skip_field(
    data_type: &DataType,
    mut node_index: usize,
    mut buffer_index: usize,
) -> Result<(usize, usize), ArrowError> {
    match data_type {
        Utf8 | Binary | LargeBinary | LargeUtf8 => {
            node_index += 1;
            buffer_index += 3;
        }
        FixedSizeBinary(_) => {
            node_index += 1;
            buffer_index += 2;
        }
        List(ref list_field) | LargeList(ref list_field) | Map(ref list_field, _) => {
            node_index += 1;
            buffer_index += 2;
            let tuple = skip_field(list_field.data_type(), node_index, buffer_index)?;
            node_index = tuple.0;
            buffer_index = tuple.1;
        }
        FixedSizeList(ref list_field, _) => {
            node_index += 1;
            buffer_index += 1;
            let tuple = skip_field(list_field.data_type(), node_index, buffer_index)?;
            node_index = tuple.0;
            buffer_index = tuple.1;
        }
        Struct(struct_fields) => {
            node_index += 1;
            buffer_index += 1;

            // skip for each field
            for struct_field in struct_fields {
                let tuple =
                    skip_field(struct_field.data_type(), node_index, buffer_index)?;
                node_index = tuple.0;
                buffer_index = tuple.1;
            }
        }
        Dictionary(_, _) => {
            node_index += 1;
            buffer_index += 2;
        }
        Union(fields, _field_type_ids, mode) => {
            node_index += 1;
            buffer_index += 1;

            match mode {
                UnionMode::Dense => {
                    buffer_index += 1;
                }
                UnionMode::Sparse => {}
            };

            for field in fields {
                let tuple = skip_field(field.data_type(), node_index, buffer_index)?;

                node_index = tuple.0;
                buffer_index = tuple.1;
            }
        }
        Null => {
            node_index += 1;
            // no buffer increases
        }
        _ => {
            node_index += 1;
            buffer_index += 2;
        }
    };
    Ok((node_index, buffer_index))
}

/// Reads the correct number of buffers based on data type and null_count, and creates a
/// primitive array ref
fn create_primitive_array(
    field_node: &crate::FieldNode,
    data_type: &DataType,
    buffers: &[Buffer],
) -> Result<ArrayRef, ArrowError> {
    let length = field_node.length() as usize;
    let null_buffer = (field_node.null_count() > 0).then_some(buffers[0].clone());
    let array_data = match data_type {
        Utf8 | Binary | LargeBinary | LargeUtf8 => {
            // read 3 buffers: null buffer (optional), offsets buffer and data buffer
            ArrayData::builder(data_type.clone())
                .len(length)
                .buffers(buffers[1..3].to_vec())
                .null_bit_buffer(null_buffer)
                .build()?
        }
        FixedSizeBinary(_) => {
            // read 2 buffers: null buffer (optional) and data buffer
            ArrayData::builder(data_type.clone())
                .len(length)
                .add_buffer(buffers[1].clone())
                .null_bit_buffer(null_buffer)
                .build()?
        }
        Int8
        | Int16
        | Int32
        | UInt8
        | UInt16
        | UInt32
        | Time32(_)
        | Date32
        | Interval(IntervalUnit::YearMonth) => {
            if buffers[1].len() / 8 == length && length != 1 {
                // interpret as a signed i64, and cast appropriately
                let data = ArrayData::builder(DataType::Int64)
                    .len(length)
                    .add_buffer(buffers[1].clone())
                    .null_bit_buffer(null_buffer)
                    .build()?;
                let values = Arc::new(Int64Array::from(data)) as ArrayRef;
                let casted = cast(&values, data_type)?;
                casted.into_data()
            } else {
                ArrayData::builder(data_type.clone())
                    .len(length)
                    .add_buffer(buffers[1].clone())
                    .null_bit_buffer(null_buffer)
                    .build()?
            }
        }
        Float32 => {
            if buffers[1].len() / 8 == length && length != 1 {
                // interpret as a f64, and cast appropriately
                let data = ArrayData::builder(DataType::Float64)
                    .len(length)
                    .add_buffer(buffers[1].clone())
                    .null_bit_buffer(null_buffer)
                    .build()?;
                let values = Arc::new(Float64Array::from(data)) as ArrayRef;
                let casted = cast(&values, data_type)?;
                casted.into_data()
            } else {
                ArrayData::builder(data_type.clone())
                    .len(length)
                    .add_buffer(buffers[1].clone())
                    .null_bit_buffer(null_buffer)
                    .build()?
            }
        }
        Boolean
        | Int64
        | UInt64
        | Float64
        | Time64(_)
        | Timestamp(_, _)
        | Date64
        | Duration(_)
        | Interval(IntervalUnit::DayTime) => ArrayData::builder(data_type.clone())
            .len(length)
            .add_buffer(buffers[1].clone())
            .null_bit_buffer(null_buffer)
            .build()?,
        Interval(IntervalUnit::MonthDayNano) | Decimal128(_, _) => {
            let buffer = get_aligned_buffer::<i128>(&buffers[1], length);

            // read 2 buffers: null buffer (optional) and data buffer
            ArrayData::builder(data_type.clone())
                .len(length)
                .add_buffer(buffer)
                .null_bit_buffer(null_buffer)
                .build()?
        }
        Decimal256(_, _) => {
            let buffer = get_aligned_buffer::<i256>(&buffers[1], length);

            // read 2 buffers: null buffer (optional) and data buffer
            ArrayData::builder(data_type.clone())
                .len(length)
                .add_buffer(buffer)
                .null_bit_buffer(null_buffer)
                .build()?
        }
        t => unreachable!("Data type {:?} either unsupported or not primitive", t),
    };

    Ok(make_array(array_data))
}

/// Checks if given `Buffer` is properly aligned with `T`.
/// If not, copying the data and padded it for alignment.
fn get_aligned_buffer<T>(buffer: &Buffer, length: usize) -> Buffer {
    let ptr = buffer.as_ptr();
    let align_req = std::mem::align_of::<T>();
    let align_offset = ptr.align_offset(align_req);
    // The buffer is not aligned properly. The writer might use a smaller alignment
    // e.g. 8 bytes, but on some platform (e.g. ARM) i128 requires 16 bytes alignment.
    // We need to copy the buffer as fallback.
    if align_offset != 0 {
        let len_in_bytes = (length * std::mem::size_of::<T>()).min(buffer.len());
        let slice = &buffer.as_slice()[0..len_in_bytes];
        Buffer::from_slice_ref(slice)
    } else {
        buffer.clone()
    }
}

/// Reads the correct number of buffers based on list type and null_count, and creates a
/// list array ref
fn create_list_array(
    field_node: &crate::FieldNode,
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
    Ok(make_array(builder.build()?))
}

/// Reads the correct number of buffers based on list type and null_count, and creates a
/// list array ref
fn create_dictionary_array(
    field_node: &crate::FieldNode,
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

        Ok(make_array(builder.build()?))
    } else {
        unreachable!("Cannot create dictionary array from {:?}", data_type)
    }
}

/// Creates a record batch from binary data using the `crate::RecordBatch` indexes and the `Schema`
pub fn read_record_batch(
    buf: &Buffer,
    batch: crate::RecordBatch,
    schema: SchemaRef,
    dictionaries_by_id: &HashMap<i64, ArrayRef>,
    projection: Option<&[usize]>,
    metadata: &crate::MetadataVersion,
) -> Result<RecordBatch, ArrowError> {
    let buffers = batch.buffers().ok_or_else(|| {
        ArrowError::IoError("Unable to get buffers from IPC RecordBatch".to_string())
    })?;
    let field_nodes = batch.nodes().ok_or_else(|| {
        ArrowError::IoError("Unable to get field nodes from IPC RecordBatch".to_string())
    })?;
    let batch_compression = batch.compression();
    let compression_codec: Option<CompressionCodec> = batch_compression
        .map(|batch_compression| batch_compression.codec().try_into())
        .transpose()?;

    // keep track of buffer and node index, the functions that create arrays mutate these
    let mut buffer_index = 0;
    let mut node_index = 0;
    let mut arrays = vec![];

    let options = RecordBatchOptions::new().with_row_count(Some(batch.length() as usize));

    if let Some(projection) = projection {
        // project fields
        for (idx, field) in schema.fields().iter().enumerate() {
            // Create array for projected field
            if projection.contains(&idx) {
                let triple = create_array(
                    field_nodes,
                    field,
                    buf,
                    buffers,
                    dictionaries_by_id,
                    node_index,
                    buffer_index,
                    &compression_codec,
                    metadata,
                )?;
                node_index = triple.1;
                buffer_index = triple.2;
                arrays.push(triple.0);
            } else {
                // Skip field.
                // This must be called to advance `node_index` and `buffer_index`.
                let tuple = skip_field(field.data_type(), node_index, buffer_index)?;
                node_index = tuple.0;
                buffer_index = tuple.1;
            }
        }

        RecordBatch::try_new_with_options(
            Arc::new(schema.project(projection)?),
            arrays,
            &options,
        )
    } else {
        // keep track of index as lists require more than one node
        for field in schema.fields() {
            let triple = create_array(
                field_nodes,
                field,
                buf,
                buffers,
                dictionaries_by_id,
                node_index,
                buffer_index,
                &compression_codec,
                metadata,
            )?;
            node_index = triple.1;
            buffer_index = triple.2;
            arrays.push(triple.0);
        }
        RecordBatch::try_new_with_options(schema, arrays, &options)
    }
}

/// Read the dictionary from the buffer and provided metadata,
/// updating the `dictionaries_by_id` with the resulting dictionary
pub fn read_dictionary(
    buf: &Buffer,
    batch: crate::DictionaryBatch,
    schema: &Schema,
    dictionaries_by_id: &mut HashMap<i64, ArrayRef>,
    metadata: &crate::MetadataVersion,
) -> Result<(), ArrowError> {
    if batch.isDelta() {
        return Err(ArrowError::IoError(
            "delta dictionary batches not supported".to_string(),
        ));
    }

    let id = batch.id();
    let fields_using_this_dictionary = schema.fields_with_dict_id(id);
    let first_field = fields_using_this_dictionary.first().ok_or_else(|| {
        ArrowError::InvalidArgumentError("dictionary id not found in schema".to_string())
    })?;

    // As the dictionary batch does not contain the type of the
    // values array, we need to retrieve this from the schema.
    // Get an array representing this dictionary's values.
    let dictionary_values: ArrayRef = match first_field.data_type() {
        DataType::Dictionary(_, ref value_type) => {
            // Make a fake schema for the dictionary batch.
            let schema = Schema {
                fields: vec![Field::new("", value_type.as_ref().clone(), true)],
                metadata: HashMap::new(),
            };
            // Read a single column
            let record_batch = read_record_batch(
                buf,
                batch.data().unwrap(),
                Arc::new(schema),
                dictionaries_by_id,
                None,
                metadata,
            )?;
            Some(record_batch.column(0).clone())
        }
        _ => None,
    }
    .ok_or_else(|| {
        ArrowError::InvalidArgumentError("dictionary id not found in schema".to_string())
    })?;

    // We don't currently record the isOrdered field. This could be general
    // attributes of arrays.
    // Add (possibly multiple) array refs to the dictionaries array.
    dictionaries_by_id.insert(id, dictionary_values.clone());

    Ok(())
}

/// Arrow File reader
pub struct FileReader<R: Read + Seek> {
    /// Buffered file reader that supports reading and seeking
    reader: BufReader<R>,

    /// The schema that is read from the file header
    schema: SchemaRef,

    /// The blocks in the file
    ///
    /// A block indicates the regions in the file to read to get data
    blocks: Vec<crate::Block>,

    /// A counter to keep track of the current block that should be read
    current_block: usize,

    /// The total number of blocks, which may contain record batches and other types
    total_blocks: usize,

    /// Optional dictionaries for each schema field.
    ///
    /// Dictionaries may be appended to in the streaming format.
    dictionaries_by_id: HashMap<i64, ArrayRef>,

    /// Metadata version
    metadata_version: crate::MetadataVersion,

    /// Optional projection and projected_schema
    projection: Option<(Vec<usize>, Schema)>,
}

impl<R: Read + Seek> fmt::Debug for FileReader<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::result::Result<(), fmt::Error> {
        f.debug_struct("FileReader<R>")
            .field("reader", &"BufReader<..>")
            .field("schema", &self.schema)
            .field("blocks", &self.blocks)
            .field("current_block", &self.current_block)
            .field("total_blocks", &self.total_blocks)
            .field("dictionaries_by_id", &self.dictionaries_by_id)
            .field("metadata_version", &self.metadata_version)
            .field("projection", &self.projection)
            .finish()
    }
}

impl<R: Read + Seek> FileReader<R> {
    /// Try to create a new file reader
    ///
    /// Returns errors if the file does not meet the Arrow Format header and footer
    /// requirements
    pub fn try_new(
        reader: R,
        projection: Option<Vec<usize>>,
    ) -> Result<Self, ArrowError> {
        let mut reader = BufReader::new(reader);
        // check if header and footer contain correct magic bytes
        let mut magic_buffer: [u8; 6] = [0; 6];
        reader.read_exact(&mut magic_buffer)?;
        if magic_buffer != super::ARROW_MAGIC {
            return Err(ArrowError::IoError(
                "Arrow file does not contain correct header".to_string(),
            ));
        }
        reader.seek(SeekFrom::End(-6))?;
        reader.read_exact(&mut magic_buffer)?;
        if magic_buffer != super::ARROW_MAGIC {
            return Err(ArrowError::IoError(
                "Arrow file does not contain correct footer".to_string(),
            ));
        }
        // read footer length
        let mut footer_size: [u8; 4] = [0; 4];
        reader.seek(SeekFrom::End(-10))?;
        reader.read_exact(&mut footer_size)?;
        let footer_len = i32::from_le_bytes(footer_size);

        // read footer
        let mut footer_data = vec![0; footer_len as usize];
        reader.seek(SeekFrom::End(-10 - footer_len as i64))?;
        reader.read_exact(&mut footer_data)?;

        let footer = crate::root_as_footer(&footer_data[..]).map_err(|err| {
            ArrowError::IoError(format!("Unable to get root as footer: {:?}", err))
        })?;

        let blocks = footer.recordBatches().ok_or_else(|| {
            ArrowError::IoError(
                "Unable to get record batches from IPC Footer".to_string(),
            )
        })?;

        let total_blocks = blocks.len();

        let ipc_schema = footer.schema().unwrap();
        let schema = crate::convert::fb_to_schema(ipc_schema);

        // Create an array of optional dictionary value arrays, one per field.
        let mut dictionaries_by_id = HashMap::new();
        if let Some(dictionaries) = footer.dictionaries() {
            for block in dictionaries {
                // read length from end of offset
                let mut message_size: [u8; 4] = [0; 4];
                reader.seek(SeekFrom::Start(block.offset() as u64))?;
                reader.read_exact(&mut message_size)?;
                if message_size == CONTINUATION_MARKER {
                    reader.read_exact(&mut message_size)?;
                }
                let footer_len = i32::from_le_bytes(message_size);
                let mut block_data = vec![0; footer_len as usize];

                reader.read_exact(&mut block_data)?;

                let message = crate::root_as_message(&block_data[..]).map_err(|err| {
                    ArrowError::IoError(format!(
                        "Unable to get root as message: {:?}",
                        err
                    ))
                })?;

                match message.header_type() {
                    crate::MessageHeader::DictionaryBatch => {
                        let batch = message.header_as_dictionary_batch().unwrap();

                        // read the block that makes up the dictionary batch into a buffer
                        let mut buf =
                            MutableBuffer::from_len_zeroed(message.bodyLength() as usize);
                        reader.seek(SeekFrom::Start(
                            block.offset() as u64 + block.metaDataLength() as u64,
                        ))?;
                        reader.read_exact(&mut buf)?;

                        read_dictionary(
                            &buf.into(),
                            batch,
                            &schema,
                            &mut dictionaries_by_id,
                            &message.version(),
                        )?;
                    }
                    t => {
                        return Err(ArrowError::IoError(format!(
                            "Expecting DictionaryBatch in dictionary blocks, found {:?}.",
                            t
                        )));
                    }
                }
            }
        }
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
            blocks: blocks.iter().copied().collect(),
            current_block: 0,
            total_blocks,
            dictionaries_by_id,
            metadata_version: footer.version(),
            projection,
        })
    }

    /// Return the number of batches in the file
    pub fn num_batches(&self) -> usize {
        self.total_blocks
    }

    /// Return the schema of the file
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Read a specific record batch
    ///
    /// Sets the current block to the index, allowing random reads
    pub fn set_index(&mut self, index: usize) -> Result<(), ArrowError> {
        if index >= self.total_blocks {
            Err(ArrowError::IoError(format!(
                "Cannot set batch to index {} from {} total batches",
                index, self.total_blocks
            )))
        } else {
            self.current_block = index;
            Ok(())
        }
    }

    fn maybe_next(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        let block = self.blocks[self.current_block];
        self.current_block += 1;

        // read length
        self.reader.seek(SeekFrom::Start(block.offset() as u64))?;
        let mut meta_buf = [0; 4];
        self.reader.read_exact(&mut meta_buf)?;
        if meta_buf == CONTINUATION_MARKER {
            // continuation marker encountered, read message next
            self.reader.read_exact(&mut meta_buf)?;
        }
        let meta_len = i32::from_le_bytes(meta_buf);

        let mut block_data = vec![0; meta_len as usize];
        self.reader.read_exact(&mut block_data)?;
        let message = crate::root_as_message(&block_data[..]).map_err(|err| {
            ArrowError::IoError(format!("Unable to get root as footer: {:?}", err))
        })?;

        // some old test data's footer metadata is not set, so we account for that
        if self.metadata_version != crate::MetadataVersion::V1
            && message.version() != self.metadata_version
        {
            return Err(ArrowError::IoError(
                "Could not read IPC message as metadata versions mismatch".to_string(),
            ));
        }

        match message.header_type() {
            crate::MessageHeader::Schema => Err(ArrowError::IoError(
                "Not expecting a schema when messages are read".to_string(),
            )),
            crate::MessageHeader::RecordBatch => {
                let batch = message.header_as_record_batch().ok_or_else(|| {
                    ArrowError::IoError(
                        "Unable to read IPC message as record batch".to_string(),
                    )
                })?;
                // read the block that makes up the record batch into a buffer
                let mut buf = MutableBuffer::from_len_zeroed(message.bodyLength() as usize);
                self.reader.seek(SeekFrom::Start(
                    block.offset() as u64 + block.metaDataLength() as u64,
                ))?;
                self.reader.read_exact(&mut buf)?;

                read_record_batch(
                    &buf.into(),
                    batch,
                    self.schema(),
                    &self.dictionaries_by_id,
                    self.projection.as_ref().map(|x| x.0.as_ref()),
                    &message.version()

                ).map(Some)
            }
            crate::MessageHeader::NONE => {
                Ok(None)
            }
            t => Err(ArrowError::IoError(format!(
                "Reading types other than record batches not yet supported, unable to read {:?}", t
            ))),
        }
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
        self.schema.clone()
    }
}

/// Arrow Stream reader
pub struct StreamReader<R: Read> {
    /// Buffered stream reader
    reader: BufReader<R>,

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
}

impl<R: Read> fmt::Debug for StreamReader<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::result::Result<(), fmt::Error> {
        f.debug_struct("StreamReader<R>")
            .field("reader", &"BufReader<..>")
            .field("schema", &self.schema)
            .field("dictionaries_by_id", &self.dictionaries_by_id)
            .field("finished", &self.finished)
            .field("projection", &self.projection)
            .finish()
    }
}

impl<R: Read> StreamReader<R> {
    /// Try to create a new stream reader
    ///
    /// The first message in the stream is the schema, the reader will fail if it does not
    /// encounter a schema.
    /// To check if the reader is done, use `is_finished(self)`
    pub fn try_new(
        reader: R,
        projection: Option<Vec<usize>>,
    ) -> Result<Self, ArrowError> {
        let mut reader = BufReader::new(reader);
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
            ArrowError::IoError(format!("Unable to get root as message: {:?}", err))
        })?;
        // message header is a Schema, so read it
        let ipc_schema: crate::Schema = message.header_as_schema().ok_or_else(|| {
            ArrowError::IoError("Unable to read IPC message as schema".to_string())
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
        })
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
            ArrowError::IoError(format!("Unable to get root as message: {:?}", err))
        })?;

        match message.header_type() {
            crate::MessageHeader::Schema => Err(ArrowError::IoError(
                "Not expecting a schema when messages are read".to_string(),
            )),
            crate::MessageHeader::RecordBatch => {
                let batch = message.header_as_record_batch().ok_or_else(|| {
                    ArrowError::IoError(
                        "Unable to read IPC message as record batch".to_string(),
                    )
                })?;
                // read the block that makes up the record batch into a buffer
                let mut buf = MutableBuffer::from_len_zeroed(message.bodyLength() as usize);
                self.reader.read_exact(&mut buf)?;

                read_record_batch(&buf.into(), batch, self.schema(), &self.dictionaries_by_id, self.projection.as_ref().map(|x| x.0.as_ref()), &message.version()).map(Some)
            }
            crate::MessageHeader::DictionaryBatch => {
                let batch = message.header_as_dictionary_batch().ok_or_else(|| {
                    ArrowError::IoError(
                        "Unable to read IPC message as dictionary batch".to_string(),
                    )
                })?;
                // read the block that makes up the dictionary batch into a buffer
                let mut buf = MutableBuffer::from_len_zeroed(message.bodyLength() as usize);
                self.reader.read_exact(&mut buf)?;

                read_dictionary(
                    &buf.into(), batch, &self.schema, &mut self.dictionaries_by_id, &message.version()
                )?;

                // read the next message until we encounter a RecordBatch
                self.maybe_next()
            }
            crate::MessageHeader::NONE => {
                Ok(None)
            }
            t => Err(ArrowError::IoError(
                format!("Reading types other than record batches not yet supported, unable to read {:?} ", t)
            )),
        }
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
    use super::*;

    use arrow_array::builder::UnionBuilder;
    use arrow_array::types::*;
    use arrow_buffer::ArrowNativeType;
    use arrow_data::ArrayDataBuilder;

    fn create_test_projection_schema() -> Schema {
        // define field types
        let list_data_type =
            DataType::List(Box::new(Field::new("item", DataType::Int32, true)));

        let fixed_size_list_data_type = DataType::FixedSizeList(
            Box::new(Field::new("item", DataType::Int32, false)),
            3,
        );

        let key_type = DataType::Int8;
        let value_type = DataType::Utf8;
        let dict_data_type =
            DataType::Dictionary(Box::new(key_type), Box::new(value_type));

        let union_fileds = vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Float64, false),
        ];
        let union_data_type = DataType::Union(union_fileds, vec![0, 1], UnionMode::Dense);

        let struct_fields = vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "list",
                DataType::List(Box::new(Field::new("item", DataType::Int8, true))),
                false,
            ),
        ];
        let struct_data_type = DataType::Struct(struct_fields);

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
            Field::new("f10", DataType::Boolean, false),
            Field::new("f11", dict_data_type, false),
            Field::new("f12", DataType::Utf8, false),
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
        let array7 =
            FixedSizeBinaryArray::try_from_iter(array7_values.into_iter()).unwrap();

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

        let array10 = BooleanArray::from(vec![false, false, true]);

        let array11_values = StringArray::from(vec!["x", "yy", "zzz"]);
        let array11_keys = Int8Array::from_iter_values([1, 1, 2]);
        let array11 =
            DictionaryArray::<Int8Type>::try_new(&array11_keys, &array11_values).unwrap();

        let array12 = StringArray::from(vec!["a", "bb", "ccc"]);

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
            let mut writer =
                crate::writer::FileWriter::try_new(&mut buf, &schema).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        // read record batch with projection
        for index in 0..12 {
            let projection = vec![index];
            let reader =
                FileReader::try_new(std::io::Cursor::new(buf.clone()), Some(projection));
            let read_batch = reader.unwrap().next().unwrap().unwrap();
            let projected_column = read_batch.column(0);
            let expected_column = batch.column(index);

            // check the projected column equals the expected column
            assert_eq!(projected_column.as_ref(), expected_column.as_ref());
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
        let mut stream_writer =
            crate::writer::StreamWriter::try_new(&mut file, &schema).unwrap();
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

    fn roundtrip_ipc(rb: &RecordBatch) -> RecordBatch {
        let mut buf = Vec::new();
        let mut writer =
            crate::writer::FileWriter::try_new(&mut buf, &rb.schema()).unwrap();
        writer.write(rb).unwrap();
        writer.finish().unwrap();
        drop(writer);

        let mut reader =
            crate::reader::FileReader::try_new(std::io::Cursor::new(buf), None).unwrap();
        reader.next().unwrap().unwrap()
    }

    fn roundtrip_ipc_stream(rb: &RecordBatch) -> RecordBatch {
        let mut buf = Vec::new();
        let mut writer =
            crate::writer::StreamWriter::try_new(&mut buf, &rb.schema()).unwrap();
        writer.write(rb).unwrap();
        writer.finish().unwrap();
        drop(writer);

        let mut reader =
            crate::reader::StreamReader::try_new(std::io::Cursor::new(buf), None)
                .unwrap();
        reader.next().unwrap().unwrap()
    }

    #[test]
    fn test_roundtrip_nested_dict() {
        let inner: DictionaryArray<Int32Type> = vec!["a", "b", "a"].into_iter().collect();

        let array = Arc::new(inner) as ArrayRef;

        let dctfield = Field::new("dict", array.data_type().clone(), false);

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

        assert_eq!(union1.data().buffers(), union2.data().buffers());
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
    fn test_roundtrip_stream_nested_dict() {
        let xs = vec!["AA", "BB", "AA", "CC", "BB"];
        let dict = Arc::new(
            xs.clone()
                .into_iter()
                .collect::<DictionaryArray<Int8Type>>(),
        );
        let string_array: ArrayRef = Arc::new(StringArray::from(xs.clone()));
        let struct_array = StructArray::from(vec![
            (Field::new("f2.1", DataType::Utf8, false), string_array),
            (
                Field::new("f2.2_struct", dict.data_type().clone(), false),
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
        let value_dict_keys = Int8Array::from_iter_values([0, 1, 1, 2, 3, 1]);
        let value_dict_array =
            DictionaryArray::<Int8Type>::try_new(&value_dict_keys, &values).unwrap();

        let key_dict_keys = Int8Array::from_iter_values([0, 0, 2, 1, 1, 3]);
        let key_dict_array =
            DictionaryArray::<Int8Type>::try_new(&key_dict_keys, &values).unwrap();

        let keys_field = Field::new_dict(
            "keys",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            true,
            1,
            false,
        );
        let values_field = Field::new_dict(
            "values",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            true,
            1,
            false,
        );
        let entry_struct = StructArray::from(vec![
            (keys_field, make_array(key_dict_array.into_data())),
            (values_field, make_array(value_dict_array.into_data())),
        ]);
        let map_data_type = DataType::Map(
            Box::new(Field::new(
                "entries",
                entry_struct.data_type().clone(),
                true,
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
        let dict_dict_array =
            DictionaryArray::<Int8Type>::try_new(&dict_keys, &map_array).unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "f1",
            dict_dict_array.data_type().clone(),
            false,
        )]));
        let input_batch =
            RecordBatch::try_new(schema, vec![Arc::new(dict_dict_array)]).unwrap();
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
        let dict_array = DictionaryArray::<Int8Type>::try_new(&keys, &values).unwrap();
        let dict_data = dict_array.data();

        let value_offsets = Buffer::from_slice_ref(offsets);

        let list_data = ArrayData::builder(list_data_type)
            .len(4)
            .add_buffer(value_offsets)
            .add_child_data(dict_data.clone())
            .build()
            .unwrap();
        let list_array = GenericListArray::<OffsetSize>::from(list_data);

        let keys_for_dict = Int8Array::from_iter_values([0, 3, 0, 1, 1, 2, 0, 1, 3]);
        let dict_dict_array =
            DictionaryArray::<Int8Type>::try_new(&keys_for_dict, &list_array).unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "f1",
            dict_dict_array.data_type().clone(),
            false,
        )]));
        let input_batch =
            RecordBatch::try_new(schema, vec![Arc::new(dict_dict_array)]).unwrap();
        let output_batch = roundtrip_ipc_stream(&input_batch);
        assert_eq!(input_batch, output_batch);
    }

    #[test]
    fn test_roundtrip_stream_dict_of_list_of_dict() {
        // list
        let list_data_type = DataType::List(Box::new(Field::new_dict(
            "item",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            true,
            1,
            false,
        )));
        let offsets: &[i32; 5] = &[0, 2, 4, 4, 6];
        test_roundtrip_stream_dict_of_list_of_dict_impl::<i32, i32>(
            list_data_type,
            offsets,
        );

        // large list
        let list_data_type = DataType::LargeList(Box::new(Field::new_dict(
            "item",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            true,
            1,
            false,
        )));
        let offsets: &[i64; 5] = &[0, 2, 4, 4, 7];
        test_roundtrip_stream_dict_of_list_of_dict_impl::<i64, i64>(
            list_data_type,
            offsets,
        );
    }

    #[test]
    fn test_roundtrip_stream_dict_of_fixed_size_list_of_dict() {
        let values = StringArray::from(vec![Some("a"), None, Some("c"), None]);
        let keys = Int8Array::from_iter_values([0, 0, 1, 2, 0, 1, 3, 1, 2]);
        let dict_array = DictionaryArray::<Int8Type>::try_new(&keys, &values).unwrap();
        let dict_data = dict_array.data();

        let list_data_type = DataType::FixedSizeList(
            Box::new(Field::new_dict(
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
            .add_child_data(dict_data.clone())
            .build()
            .unwrap();
        let list_array = FixedSizeListArray::from(list_data);

        let keys_for_dict = Int8Array::from_iter_values([0, 1, 0, 1, 1, 2, 0, 1, 2]);
        let dict_dict_array =
            DictionaryArray::<Int8Type>::try_new(&keys_for_dict, &list_array).unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "f1",
            dict_dict_array.data_type().clone(),
            false,
        )]));
        let input_batch =
            RecordBatch::try_new(schema, vec![Arc::new(dict_dict_array)]).unwrap();
        let output_batch = roundtrip_ipc_stream(&input_batch);
        assert_eq!(input_batch, output_batch);
    }

    #[test]
    fn test_no_columns_batch() {
        let schema = Arc::new(Schema::new(vec![]));
        let options = RecordBatchOptions::new()
            .with_match_field_names(true)
            .with_row_count(Some(10));
        let input_batch =
            RecordBatch::try_new_with_options(schema, vec![], &options).unwrap();
        let output_batch = roundtrip_ipc_stream(&input_batch);
        assert_eq!(input_batch, output_batch);
    }
}
