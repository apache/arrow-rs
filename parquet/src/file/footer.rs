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

//! Module for working with Parquet file footers.

use crate::errors::Result;
use crate::file::{metadata::*, reader::ChunkReader, FOOTER_SIZE};

/// Reads the [ParquetMetaData] from the footer of the parquet file.
///
/// # Layout of Parquet file
/// ```text
/// +---------------------------+-----+---+
/// |      Rest of file         |  B  | A |
/// +---------------------------+-----+---+
/// ```
/// where
/// * `A`: parquet footer which stores the length of the metadata.
/// * `B`: parquet metadata.
///
/// # I/O
///
/// This method first reads the last 8 bytes of the file via
/// [`ChunkReader::get_read`] to get the the parquet footer which contains the
/// metadata length.
///
/// It then issues a second `get_read` to read the encoded metadata
/// metadata.
///
/// # See Also
/// [`decode_metadata`] for decoding the metadata from the bytes.
/// [`decode_footer`] for decoding the metadata length from the footer.
#[deprecated(since = "53.1.0", note = "Use ParquetMetaDataReader")]
pub fn parse_metadata<R: ChunkReader>(chunk_reader: &R) -> Result<ParquetMetaData> {
    ParquetMetaDataReader::new().parse_and_finish(chunk_reader)
}

/// Decodes [`ParquetMetaData`] from the provided bytes.
///
/// Typically this is used to decode the metadata from the end of a parquet
/// file. The format of `buf` is the Thift compact binary protocol, as specified
/// by the [Parquet Spec].
///
/// [Parquet Spec]: https://github.com/apache/parquet-format#metadata
#[deprecated(since = "53.1.0", note = "Use ParquetMetaDataReader::decode_metadata")]
pub fn decode_metadata(buf: &[u8]) -> Result<ParquetMetaData> {
    ParquetMetaDataReader::decode_metadata(buf)
}

/// Decodes the Parquet footer returning the metadata length in bytes
///
/// A parquet footer is 8 bytes long and has the following layout:
/// * 4 bytes for the metadata length
/// * 4 bytes for the magic bytes 'PAR1'
///
/// ```text
/// +-----+--------+
/// | len | 'PAR1' |
/// +-----+--------+
/// ```
#[deprecated(since = "53.1.0", note = "Use ParquetMetaDataReader::decode_footer")]
pub fn decode_footer(slice: &[u8; FOOTER_SIZE]) -> Result<usize> {
    ParquetMetaDataReader::decode_footer(slice)
}
