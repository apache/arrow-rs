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

use crate::basic::Type;
use crate::data_type::Int96;
use crate::errors::ParquetError;
use crate::file::metadata::ColumnChunkMetaData;
use crate::file::page_index::index::{BooleanIndex, ByteArrayIndex, Index, NativeIndex};
use crate::file::reader::ChunkReader;
use crate::format::{ColumnIndex, OffsetIndex, PageLocation};
use std::io::{Cursor, Read};
use thrift::protocol::{TCompactInputProtocol, TSerializable};

/// Read on row group's all columns indexes and change into  [`Index`]
/// If not the format not available return an empty vector.
pub fn read_columns_indexes<R: ChunkReader>(
    reader: &R,
    chunks: &[ColumnChunkMetaData],
) -> Result<Vec<Index>, ParquetError> {
    let (offset, lengths) = get_index_offset_and_lengths(chunks)?;
    let length = lengths.iter().sum::<usize>();

    if length == 0 {
        return Ok(vec![Index::NONE; chunks.len()]);
    }

    //read all need data into buffer
    let mut reader = reader.get_read(offset, length)?;
    let mut data = vec![0; length];
    reader.read_exact(&mut data)?;

    let mut start = 0;
    let data = lengths.into_iter().map(|length| {
        let r = &data[start..start + length];
        start += length;
        r
    });

    chunks
        .iter()
        .zip(data)
        .map(|(chunk, data)| {
            let column_type = chunk.column_type();
            deserialize_column_index(data, column_type)
        })
        .collect()
}

/// Read on row group's all indexes and change into  [`Index`]
/// If not the format not available return an empty vector.
pub fn read_pages_locations<R: ChunkReader>(
    reader: &R,
    chunks: &[ColumnChunkMetaData],
) -> Result<Vec<Vec<PageLocation>>, ParquetError> {
    let (offset, total_length) = get_location_offset_and_total_length(chunks)?;

    if total_length == 0 {
        return Ok(vec![]);
    }

    //read all need data into buffer
    let mut reader = reader.get_read(offset, total_length)?;
    let mut data = vec![0; total_length];
    reader.read_exact(&mut data)?;

    let mut d = Cursor::new(data);
    let mut result = vec![];

    for _ in 0..chunks.len() {
        let mut prot = TCompactInputProtocol::new(&mut d);
        let offset = OffsetIndex::read_from_in_protocol(&mut prot)?;
        result.push(offset.page_locations);
    }
    Ok(result)
}

//Get File offsets of every ColumnChunk's page_index
//If there are invalid offset return a zero offset with empty lengths.
pub(crate) fn get_index_offset_and_lengths(
    chunks: &[ColumnChunkMetaData],
) -> Result<(u64, Vec<usize>), ParquetError> {
    let first_col_metadata = if let Some(chunk) = chunks.first() {
        chunk
    } else {
        return Ok((0, vec![]));
    };

    let offset: u64 = if let Some(offset) = first_col_metadata.column_index_offset() {
        offset.try_into().unwrap()
    } else {
        return Ok((0, vec![]));
    };

    let lengths = chunks
        .iter()
        .map(|x| x.column_index_length())
        .map(|maybe_length| {
            let index_length = maybe_length.unwrap_or(0);
            Ok(index_length.try_into().unwrap())
        })
        .collect::<Result<Vec<_>, ParquetError>>()?;

    Ok((offset, lengths))
}

//Get File offset of ColumnChunk's pages_locations
//If there are invalid offset return a zero offset with zero length.
pub(crate) fn get_location_offset_and_total_length(
    chunks: &[ColumnChunkMetaData],
) -> Result<(u64, usize), ParquetError> {
    let metadata = if let Some(chunk) = chunks.first() {
        chunk
    } else {
        return Ok((0, 0));
    };

    let offset: u64 = if let Some(offset) = metadata.offset_index_offset() {
        offset.try_into().unwrap()
    } else {
        return Ok((0, 0));
    };

    let total_length = chunks
        .iter()
        .map(|x| x.offset_index_length().unwrap())
        .sum::<i32>() as usize;
    Ok((offset, total_length))
}

pub(crate) fn deserialize_column_index(
    data: &[u8],
    column_type: Type,
) -> Result<Index, ParquetError> {
    if data.is_empty() {
        return Ok(Index::NONE);
    }
    let mut d = Cursor::new(data);
    let mut prot = TCompactInputProtocol::new(&mut d);

    let index = ColumnIndex::read_from_in_protocol(&mut prot)?;

    let index = match column_type {
        Type::BOOLEAN => Index::BOOLEAN(BooleanIndex::try_new(index)?),
        Type::INT32 => Index::INT32(NativeIndex::<i32>::try_new(index, column_type)?),
        Type::INT64 => Index::INT64(NativeIndex::<i64>::try_new(index, column_type)?),
        Type::INT96 => Index::INT96(NativeIndex::<Int96>::try_new(index, column_type)?),
        Type::FLOAT => Index::FLOAT(NativeIndex::<f32>::try_new(index, column_type)?),
        Type::DOUBLE => Index::DOUBLE(NativeIndex::<f64>::try_new(index, column_type)?),
        Type::BYTE_ARRAY => {
            Index::BYTE_ARRAY(ByteArrayIndex::try_new(index, column_type)?)
        }
        Type::FIXED_LEN_BYTE_ARRAY => {
            Index::FIXED_LEN_BYTE_ARRAY(ByteArrayIndex::try_new(index, column_type)?)
        }
    };

    Ok(index)
}
