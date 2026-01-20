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
//   Unless required by applicable law or agreed to in writing,
//   software distributed under the License is distributed on an
//   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//   KIND, either express or implied.  See the License for the
//   specific language governing permissions and limitations
//   under the License.

//! Dictionary page decoding utilities

use arrow_array::ArrayRef;
use bytes::Bytes;

use crate::arrow::{ByteArrayDecoder, OffsetBuffer};
use crate::basic::{Compression, Encoding, PageType};
use crate::compression::{create_codec, CodecOptions};
use crate::errors::{ParquetError, Result};
use crate::file::metadata::thrift::PageHeader;
use crate::file::metadata::ColumnChunkMetaData;
use crate::parquet_thrift::{ReadThrift, ThriftSliceInputProtocol};
use crate::schema::types::ColumnDescriptor;

/// Decodes a dictionary page from raw bytes into an Arrow array.
///
/// This function parses a dictionary page (including Thrift header), handles
/// decompression if needed, and decodes PLAIN-encoded byte array values into
/// an Arrow StringArray or BinaryArray based on the column's logical type.
///
/// # Arguments
///
/// * `buffer` - Raw bytes containing the complete dictionary page (header + data)
/// * `column_metadata` - Column chunk metadata for compression information
/// * `column_descriptor` - Column schema information for type detection
///
/// # Returns
///
/// * `Ok(ArrayRef)` - The decoded dictionary as a StringArray (for UTF-8 columns)
///   or BinaryArray (for binary columns)
/// * `Err(ParquetError)` - If:
///   - The page is not a valid DICTIONARY_PAGE
///   - Thrift header parsing fails
///   - Decompression fails
///   - PLAIN decoding fails
///   - UTF-8 validation fails (for String columns)
///
/// # Example
///
/// ```ignore
/// let array = decode_dictionary_page(buffer, column_metadata, column_descriptor)?;
/// ```
pub fn decode_dictionary_page(
    buffer: Bytes,
    column_metadata: &ColumnChunkMetaData,
    column_descriptor: &ColumnDescriptor,
) -> Result<ArrayRef> {
    // Parse Thrift page header
    let mut prot = ThriftSliceInputProtocol::new(&buffer);
    let page_header = PageHeader::read_thrift(&mut prot)
        .map_err(|e| ParquetError::General(format!("Failed to read page header: {}", e)))?;
    let header_len = prot.as_slice().as_ptr() as usize - buffer.as_ptr() as usize;

    // Validate it's a dictionary page
    if page_header.r#type != PageType::DICTIONARY_PAGE {
        return Err(ParquetError::General(format!(
            "Expected DICTIONARY_PAGE, got {:?}",
            page_header.r#type
        )));
    }

    let dict_header = page_header
        .dictionary_page_header
        .ok_or_else(|| ParquetError::General("Missing dictionary page header".to_string()))?;

    let num_values = dict_header.num_values as usize;
    let compressed_page_size = page_header.compressed_page_size as usize;
    let uncompressed_page_size = page_header.uncompressed_page_size as usize;

    // Extract page data (after header)
    let page_data = buffer.slice(header_len..header_len + compressed_page_size);

    // Handle decompression if needed
    let decompressed_data = if column_metadata.compression() != Compression::UNCOMPRESSED {
        let codec_options = CodecOptions::default();
        let mut decompressor = create_codec(column_metadata.compression(), &codec_options)?
            .ok_or_else(|| ParquetError::General("Failed to create decompressor".to_string()))?;

        let mut decompressed = Vec::with_capacity(uncompressed_page_size);
        decompressor.decompress(&page_data, &mut decompressed, Some(uncompressed_page_size))?;
        Bytes::from(decompressed)
    } else {
        page_data
    };

    // Determine if this is String (Utf8) or Binary based on schema
    let validate_utf8 = is_utf8_column(column_descriptor);

    // Use ByteArrayDecoder to decode PLAIN-encoded values
    let mut decoder = ByteArrayDecoder::new(
        Encoding::PLAIN,
        decompressed_data,
        num_values,
        Some(num_values),
        validate_utf8,
    )?;

    let mut buffer = OffsetBuffer::<i32>::default();
    decoder.read(&mut buffer, num_values, None)?;

    // Convert to Arrow array (Utf8 or Binary)
    let value_type = if validate_utf8 {
        arrow_schema::DataType::Utf8
    } else {
        arrow_schema::DataType::Binary
    };

    Ok(buffer.into_array(None, value_type))
}

/// Determines if a column should be treated as UTF-8 based on its logical/converted type.
///
/// Returns `true` if the column has:
/// - `LogicalType::String` (preferred modern type annotation)
/// - `ConvertedType::UTF8` or `ConvertedType::JSON` (legacy type annotation)
///
/// # Arguments
///
/// * `column_descr` - The column descriptor to check
///
/// # Returns
///
/// `true` if the column should be validated as UTF-8, `false` otherwise
pub fn is_utf8_column(column_descr: &ColumnDescriptor) -> bool {
    use crate::basic::{ConvertedType, LogicalType};

    let basic_info = column_descr.self_type().get_basic_info();

    // Check logical type first (preferred)
    if let Some(logical_type) = basic_info.logical_type_ref() {
        return matches!(logical_type, LogicalType::String);
    }

    // Fall back to converted type
    matches!(
        basic_info.converted_type(),
        ConvertedType::UTF8 | ConvertedType::JSON
    )
}
