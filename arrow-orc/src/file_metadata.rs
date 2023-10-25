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

//! Reading ORC file metadata.

use arrow_schema::SchemaRef;
use prost::Message;

use crate::decompress::{CompressionType, Decompressor};
use crate::errors::Result;
use crate::proto;
use crate::reader::Reader;
use crate::schema::to_root_schema;

use std::borrow::Cow;
use std::io::Read;

/// How many bytes to read in first read from file.
/// Ideally will contain postscript, footer and metadata sections.
const FIRST_READ_BYTES_SIZE: u64 = 16 * 1024;

/// Given a reader over an ORC file, will seek to the end and read the metadata
/// located at the tail of the file.
pub fn parse_metadata<R: Reader>(reader: &mut R) -> Result<OrcMetadata> {
    let reader_length = reader.len();
    if reader_length == 0 {
        return Err(general_err!("Cannot read metadata from empty file"));
    }
    // in case file is smaller than expected
    let first_chunk_size = FIRST_READ_BYTES_SIZE.min(reader_length);

    let offset = reader_length - first_chunk_size;
    let bytes = reader.get_bytes(offset, first_chunk_size)?;

    // safe split since bytes isn't empty
    // postscript length is encoded as single last byte in file
    let (bytes, postscript_length_byte) = bytes.split_at(bytes.len() - 1);
    let postscript_length = postscript_length_byte[0] as usize;

    // if file is too small for stated postscript section length
    if postscript_length > bytes.len() {
        return Err(general_err!("Invalid postscript length"));
    }
    // safe split as here we're guaranteed we have enough bytes for the postscript
    let (bytes, postscript_bytes) = bytes.split_at(bytes.len() - postscript_length);
    let postscript = proto::PostScript::decode(postscript_bytes)?;

    let compression_type =
        CompressionType::from_proto(postscript.compression(), postscript.compression_block_size)?;
    let footer_length = postscript.footer_length();
    let metadata_length = postscript.metadata_length();

    let bytes_len = bytes.len() as u64;
    let bytes = if (footer_length + metadata_length) > bytes_len {
        // need to read more bytes as footer + metadata size exceeds initial read chunk
        let bytes_to_read = (footer_length + metadata_length) - bytes_len;
        let offset = reader_length - first_chunk_size - bytes_to_read;

        let mut extra_bytes = reader.get_bytes(offset, bytes_to_read)?;
        extra_bytes.extend_from_slice(bytes);
        Cow::Owned(extra_bytes)
    } else {
        Cow::Borrowed(bytes)
    };

    // here on we are guaranteed enough bytes for whatever we need
    let (bytes, footer_bytes) = bytes.split_at(bytes.len() - footer_length as usize);
    // footer and metadata may be optionally compressed
    // if compression was set in postscript
    let footer = match compression_type {
        Some(compression) => {
            let mut bytes = vec![];
            Decompressor::new(footer_bytes, compression).read_to_end(&mut bytes)?;
            proto::Footer::decode(bytes.as_ref())?
        }
        None => proto::Footer::decode(footer_bytes)?,
    };

    let (_, metadata_bytes) = bytes.split_at(bytes.len() - metadata_length as usize);
    // TODO: make use of metadata for statistics
    let _metadata = match compression_type {
        Some(compression) => {
            let mut bytes = vec![];
            Decompressor::new(metadata_bytes, compression).read_to_end(&mut bytes)?;
            proto::Metadata::decode(bytes.as_ref())?
        }
        None => proto::Metadata::decode(metadata_bytes)?,
    };

    let schema = to_root_schema(&footer.types)?;
    let number_of_rows = footer.number_of_rows();
    let stripes = footer
        .stripes
        .into_iter()
        .map(StripeInformation::from)
        .collect::<Vec<_>>();

    Ok(OrcMetadata {
        compression_type,
        stripes,
        schema,
        number_of_rows,
    })
}

/// Contains general metadata about entire ORC file.
#[derive(Debug)]
pub struct OrcMetadata {
    /// If ORC file has compression enabled or not
    pub compression_type: Option<CompressionType>,
    /// Information used for decoding each stripe
    pub stripes: Vec<StripeInformation>,
    /// Converted Arrow schema for entire file
    pub schema: SchemaRef,
    /// Total number of rows in the file
    pub number_of_rows: u64,
}

/// Contains information used to locate stripes and their sections
/// in the file.
#[derive(Debug, Copy, Clone)]
pub struct StripeInformation {
    pub start_offset: u64,
    pub index_length: u64,
    pub data_length: u64,
    pub footer_length: u64,
    pub number_of_rows: u64,
}

impl From<proto::StripeInformation> for StripeInformation {
    fn from(value: proto::StripeInformation) -> Self {
        Self {
            start_offset: value.offset(),
            index_length: value.index_length(),
            data_length: value.data_length(),
            footer_length: value.footer_length(),
            number_of_rows: value.number_of_rows(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;

    use super::*;

    #[test]
    fn test_parse_metadata() -> Result<()> {
        let file_name = "demo-12-zlib.orc";
        let mut file = File::open(format!("tests/data/{file_name}"))?;
        let _ = parse_metadata(&mut file)?;

        let file_name = "alltypes.none.orc";
        let mut file = File::open(format!("tests/data/{file_name}"))?;
        let _ = parse_metadata(&mut file)?;

        Ok(())
    }
}
