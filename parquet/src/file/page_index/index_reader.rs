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
use crate::file::page_index::index::{BooleanIndex, ByteIndex, Index, NativeIndex};
use crate::file::reader::ChunkReader;
use parquet_format::{ColumnIndex, OffsetIndex, PageLocation};
use std::io::{Cursor, Read};
use std::sync::Arc;
use thrift::protocol::TCompactInputProtocol;

/// Read on row group's all columns indexes and change into  [`Index`]
/// If not the format not available return an empty vector.
pub fn read_columns_indexes<R: ChunkReader>(
    reader: &R,
    chunks: &[ColumnChunkMetaData],
) -> Result<Vec<Arc<dyn Index>>, ParquetError> {
    let (offset, lengths) = get_index_offset_and_lengths(chunks)?;
    let length = lengths.iter().sum::<usize>();

    //read all need data into buffer
    let mut reader = reader.get_read(offset, reader.len() as usize)?;
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
            deserialize(data, column_type)
        })
        .collect()
}

/// Read on row group's all indexes and change into  [`Index`]
/// If not the format not available return an empty vector.
pub fn read_pages_locations<R: ChunkReader>(
    reader: &R,
    chunks: &[ColumnChunkMetaData],
) -> Result<Vec<Vec<PageLocation>>, ParquetError> {
    let (offset, lengths) = get_location_offset_and_lengths(chunks)?;
    let total_length = lengths.iter().sum::<usize>();

    //read all need data into buffer
    let mut reader = reader.get_read(offset, reader.len() as usize)?;
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

fn get_index_offset_and_lengths(
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
            let index_length = maybe_length.ok_or_else(|| {
                ParquetError::General(
                    "The column_index_length must exist if offset_index_offset exists"
                        .to_string(),
                )
            })?;

            Ok(index_length.try_into().unwrap())
        })
        .collect::<Result<Vec<_>, ParquetError>>()?;

    Ok((offset, lengths))
}

fn get_location_offset_and_lengths(
    chunks: &[ColumnChunkMetaData],
) -> Result<(u64, Vec<usize>), ParquetError> {
    let metadata = if let Some(chunk) = chunks.first() {
        chunk
    } else {
        return Ok((0, vec![]));
    };

    let offset: u64 = if let Some(offset) = metadata.offset_index_offset() {
        offset.try_into().unwrap()
    } else {
        return Ok((0, vec![]));
    };

    let lengths = chunks
        .iter()
        .map(|x| x.offset_index_length())
        .map(|maybe_length| {
            let index_length = maybe_length.ok_or_else(|| {
                ParquetError::General(
                    "The offset_index_length must exist if offset_index_offset exists"
                        .to_string(),
                )
            })?;

            Ok(index_length.try_into().unwrap())
        })
        .collect::<Result<Vec<_>, ParquetError>>()?;

    Ok((offset, lengths))
}

fn deserialize(data: &[u8], column_type: Type) -> Result<Arc<dyn Index>, ParquetError> {
    let mut d = Cursor::new(data);
    let mut prot = TCompactInputProtocol::new(&mut d);

    let index = ColumnIndex::read_from_in_protocol(&mut prot)?;

    let index = match column_type {
        Type::BOOLEAN => Arc::new(BooleanIndex::try_new(index)?) as Arc<dyn Index>,
        Type::INT32 => Arc::new(NativeIndex::<i32>::try_new(index, column_type)?),
        Type::INT64 => Arc::new(NativeIndex::<i64>::try_new(index, column_type)?),
        Type::INT96 => Arc::new(NativeIndex::<Int96>::try_new(index, column_type)?),
        Type::FLOAT => Arc::new(NativeIndex::<f32>::try_new(index, column_type)?),
        Type::DOUBLE => Arc::new(NativeIndex::<f64>::try_new(index, column_type)?),
        Type::BYTE_ARRAY => Arc::new(ByteIndex::try_new(index, column_type)?),
        Type::FIXED_LEN_BYTE_ARRAY => Arc::new(ByteIndex::try_new(index, column_type)?),
    };

    Ok(index)
}
