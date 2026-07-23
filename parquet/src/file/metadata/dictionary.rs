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

//! Decoding a column chunk's dictionary page directly into an Arrow array,
//! independent of the row-by-row [`ArrayReader`] machinery.
//!
//! This is useful for callers that want the *set* of distinct values stored
//! in a dictionary-encoded column chunk without reading any data pages, e.g.
//! to prune a row group when the query predicate's literals are known not to
//! be in the dictionary.
//!
//! [`ArrayReader`]: crate::arrow::array_reader::ArrayReader

use crate::arrow::{ByteArrayDecoderPlain, OffsetBuffer};
use crate::basic::{ConvertedType, LogicalType, PageType, Type as PhysicalType};
use crate::column::page::Page;
use crate::compression::{CodecOptions, create_codec};
#[cfg(feature = "encryption")]
use crate::encryption::decrypt::CryptoContext;
use crate::errors::{ParquetError, Result};
#[cfg(feature = "encryption")]
use crate::file::metadata::ColumnChunkMetaData;
use crate::file::metadata::ParquetMetaData;
use crate::file::serialized_reader::{
    SerializedPageReaderContext, decode_page, read_page_header_len_from_bytes, verify_page_size,
};
use crate::schema::types::ColumnDescriptor;
use arrow_array::ArrayRef;
use arrow_schema::DataType as ArrowType;
use bytes::Bytes;
#[cfg(feature = "encryption")]
use std::sync::Arc;

/// Decodes the dictionary page of a column chunk into an [`ArrayRef`].
///
/// `buffer` must contain the entire dictionary page, byte-for-byte, i.e. the
/// range `[dictionary_page_offset, data_page_offset)` of the column chunk.
///
/// Only `BYTE_ARRAY` columns are currently supported; other physical types
/// return an error. The returned array never contains nulls: dictionary
/// pages only store the distinct non-null values, with nulls represented via
/// definition levels in the data pages.
///
/// Note this only decodes whatever dictionary page is present -- it does
/// **not** verify that the entire column chunk is dictionary-encoded (i.e.
/// that every value in the chunk is drawn from this dictionary). Callers
/// that need that guarantee (for example, to use the dictionary as an exact
/// membership index) must check that themselves, e.g. via
/// [`crate::file::metadata::ColumnChunkMetaData::page_encoding_stats_mask`].
pub(crate) fn decode_dictionary_page(
    buffer: Bytes,
    parquet_meta_data: &ParquetMetaData,
    row_group_idx: usize,
    column_idx: usize,
) -> Result<ArrayRef> {
    let column_metadata = parquet_meta_data
        .row_group(row_group_idx)
        .column(column_idx);
    let column_descriptor = column_metadata.column_descr();

    if column_descriptor.physical_type() != PhysicalType::BYTE_ARRAY {
        return Err(ParquetError::General(format!(
            "decode_dictionary_page only supports BYTE_ARRAY columns, got {}",
            column_descriptor.physical_type()
        )));
    }

    // Dictionary pages are subject to the same modular encryption as data
    // pages: both the page header and the page body may be ciphertext, so
    // we must route through the same crypto-aware header/data path that
    // `SerializedPageReader` uses rather than parsing the header directly.
    let page_context = SerializedPageReaderContext {
        read_stats: true,
        #[cfg(feature = "encryption")]
        crypto_context: dictionary_page_crypto_context(
            parquet_meta_data,
            column_metadata,
            row_group_idx,
            column_idx,
        )?,
    };

    let (consumed, header) =
        read_page_header_len_from_bytes(&page_context, buffer.as_ref(), 0, true)?;
    if header.r#type != PageType::DICTIONARY_PAGE {
        return Err(ParquetError::General(format!(
            "Expected a dictionary page, found {:?}",
            header.r#type
        )));
    }

    // `compressed_page_size` comes from the (possibly maliciously crafted)
    // file header; `verify_page_size` bounds-checks it against what we
    // actually fetched before we slice, instead of trusting it blindly.
    let remaining = (buffer.len() - consumed) as u64;
    verify_page_size(
        header.compressed_page_size,
        header.uncompressed_page_size,
        remaining,
    )?;
    let compressed_size = header.compressed_page_size as usize;
    let page_buf = buffer.slice(consumed..consumed + compressed_size);
    let page_buf = page_context.decrypt_page_data(page_buf, 0, true)?;

    let mut decompressor = create_codec(column_metadata.compression(), &CodecOptions::default())?;
    let page = decode_page(
        header,
        page_buf,
        column_descriptor.physical_type(),
        decompressor.as_mut(),
    )?;
    let Page::DictionaryPage {
        buf, num_values, ..
    } = page
    else {
        return Err(ParquetError::General(
            "Expected a dictionary page".to_string(),
        ));
    };
    let num_values = num_values as usize;

    // The dictionary page is always PLAIN-encoded, regardless of what the
    // data pages' encoding is (RLE_DICTIONARY/PLAIN_DICTIONARY only describe
    // how *data* pages reference the dictionary by index).
    let is_utf8 = is_utf8(column_descriptor);
    let mut decoder = ByteArrayDecoderPlain::new(buf, num_values, Some(num_values), is_utf8);
    let mut offsets = OffsetBuffer::<i32>::with_capacity(num_values);
    decoder.read(&mut offsets, usize::MAX)?;

    let arrow_type = if is_utf8 {
        ArrowType::Utf8
    } else {
        ArrowType::Binary
    };
    Ok(offsets.into_array(None, arrow_type))
}

/// Builds the crypto context needed to decrypt the dictionary page of
/// `column_metadata`, or `None` if the file (or this column) isn't encrypted.
#[cfg(feature = "encryption")]
fn dictionary_page_crypto_context(
    parquet_meta_data: &ParquetMetaData,
    column_metadata: &ColumnChunkMetaData,
    row_group_idx: usize,
    column_idx: usize,
) -> Result<Option<Arc<CryptoContext>>> {
    let Some(file_decryptor) = parquet_meta_data.file_decryptor() else {
        return Ok(None);
    };
    let Some(crypto_metadata) = column_metadata.crypto_metadata() else {
        return Ok(None);
    };
    let crypto_context =
        CryptoContext::for_column(file_decryptor, crypto_metadata, row_group_idx, column_idx)?
            .for_dictionary_page();
    Ok(Some(Arc::new(crypto_context)))
}

/// Whether `column_descriptor` should be decoded as UTF-8 text (`Utf8`)
/// rather than raw `Binary`.
fn is_utf8(column_descriptor: &ColumnDescriptor) -> bool {
    matches!(
        column_descriptor.logical_type_ref(),
        Some(LogicalType::String) | Some(LogicalType::Json)
    ) || matches!(
        column_descriptor.converted_type(),
        ConvertedType::UTF8 | ConvertedType::JSON
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::ArrowWriter;
    use crate::file::properties::WriterProperties;
    use crate::file::reader::{ChunkReader, FileReader, SerializedFileReader};
    use arrow_array::{Array, RecordBatch, StringArray};
    use arrow_schema::{Field, Schema};
    use std::sync::Arc;

    fn write_dictionary_encoded_strings(values: &[&str]) -> Bytes {
        let schema = Arc::new(Schema::new(vec![Field::new("s", ArrowType::Utf8, false)]));
        let array = Arc::new(StringArray::from_iter_values(values.iter().copied()));
        let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();

        let props = WriterProperties::builder()
            .set_dictionary_enabled(true)
            .build();
        let mut buf = Vec::new();
        {
            let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props)).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }
        Bytes::from(buf)
    }

    #[test]
    fn decode_dictionary_page_round_trips_strings() {
        let distinct_values = ["alpha", "beta", "gamma"];
        // Repeat so the column is worth dictionary-encoding but the
        // dictionary itself only contains the distinct values.
        let values: Vec<&str> = distinct_values.iter().copied().cycle().take(30).collect();
        let data = write_dictionary_encoded_strings(&values);

        let reader = SerializedFileReader::new(data.clone()).unwrap();
        let metadata = reader.metadata();
        let column_metadata = metadata.row_group(0).column(0);

        assert!(
            column_metadata.dictionary_page_offset().is_some(),
            "expected the column chunk to be dictionary-encoded"
        );

        let start = column_metadata.dictionary_page_offset().unwrap() as u64;
        let end = column_metadata.data_page_offset() as u64;
        let buffer = data.get_bytes(start, (end - start) as usize).unwrap();

        let array = decode_dictionary_page(buffer, metadata, 0, 0).unwrap();
        let array = array.as_any().downcast_ref::<StringArray>().unwrap();
        let decoded: Vec<&str> = array.iter().map(|v| v.unwrap()).collect();
        assert_eq!(decoded, distinct_values);
    }

    #[test]
    fn decode_dictionary_page_errors_on_truncated_buffer() {
        let distinct_values = ["alpha", "beta", "gamma"];
        let values: Vec<&str> = distinct_values.iter().copied().cycle().take(30).collect();
        let data = write_dictionary_encoded_strings(&values);

        let reader = SerializedFileReader::new(data.clone()).unwrap();
        let metadata = reader.metadata();
        let column_metadata = metadata.row_group(0).column(0);

        let start = column_metadata.dictionary_page_offset().unwrap() as u64;
        let end = column_metadata.data_page_offset() as u64;
        let buffer = data.get_bytes(start, (end - start) as usize).unwrap();

        // Simulate a truncated/malformed file: the page header's declared
        // `compressed_page_size` no longer fits in what was actually
        // fetched. This must return an error rather than panic while
        // slicing (`Bytes::slice` panics on out-of-bounds ranges).
        let truncated = buffer.slice(..buffer.len() - 1);
        let err = decode_dictionary_page(truncated, metadata, 0, 0).unwrap_err();
        assert!(
            matches!(err, ParquetError::EOF(_)),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn decode_dictionary_page_rejects_non_byte_array() {
        let schema = Arc::new(Schema::new(vec![Field::new("i", ArrowType::Int32, false)]));
        let array = Arc::new(arrow_array::Int32Array::from(vec![1, 2, 3]));
        let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();
        let mut buf = Vec::new();
        {
            let mut writer = ArrowWriter::try_new(&mut buf, schema, None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }
        let data = Bytes::from(buf);

        let reader = SerializedFileReader::new(data).unwrap();
        let metadata = reader.metadata();

        let err = decode_dictionary_page(Bytes::new(), metadata, 0, 0).unwrap_err();
        assert!(err.to_string().contains("BYTE_ARRAY"));
    }

    #[test]
    fn read_column_dictionary_round_trips_via_metadata_reader() {
        use crate::file::metadata::ParquetMetaDataReader;

        let distinct_values = ["alpha", "beta", "gamma"];
        let values: Vec<&str> = distinct_values.iter().copied().cycle().take(30).collect();
        let data = write_dictionary_encoded_strings(&values);

        let reader = SerializedFileReader::new(data.clone()).unwrap();
        let metadata = reader.metadata();

        let array = ParquetMetaDataReader::read_column_dictionary(&data, metadata, 0, 0)
            .unwrap()
            .unwrap();
        let array = array.as_any().downcast_ref::<StringArray>().unwrap();
        let decoded: Vec<&str> = array.iter().map(|v| v.unwrap()).collect();
        assert_eq!(decoded, distinct_values);
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn read_column_dictionary_round_trips_with_encryption() {
        use crate::encryption::decrypt::FileDecryptionProperties;
        use crate::encryption::encrypt::FileEncryptionProperties;
        use crate::file::metadata::ParquetMetaDataReader;

        const FOOTER_KEY: &[u8] = b"0123456789012345";

        let distinct_values = ["alpha", "beta", "gamma"];
        let values: Vec<&str> = distinct_values.iter().copied().cycle().take(30).collect();

        let schema = Arc::new(Schema::new(vec![Field::new("s", ArrowType::Utf8, false)]));
        let array = Arc::new(StringArray::from_iter_values(values.iter().copied()));
        let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();

        let encryption_properties = FileEncryptionProperties::builder(FOOTER_KEY.to_vec())
            .build()
            .unwrap();
        let props = WriterProperties::builder()
            .set_dictionary_enabled(true)
            .with_file_encryption_properties(encryption_properties)
            .build();
        let mut buf = Vec::new();
        {
            let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props)).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }
        let data = Bytes::from(buf);

        let decryption_properties = FileDecryptionProperties::builder(FOOTER_KEY.to_vec())
            .build()
            .unwrap();
        let metadata = ParquetMetaDataReader::new()
            .with_decryption_properties(Some(decryption_properties))
            .parse_and_finish(&data)
            .unwrap();

        let array = ParquetMetaDataReader::read_column_dictionary(&data, &metadata, 0, 0)
            .unwrap()
            .unwrap();
        let array = array.as_any().downcast_ref::<StringArray>().unwrap();
        let decoded: Vec<&str> = array.iter().map(|v| v.unwrap()).collect();
        assert_eq!(decoded, distinct_values);
    }

    #[test]
    fn read_column_dictionary_returns_none_without_dictionary_page() {
        use crate::file::metadata::ParquetMetaDataReader;

        let schema = Arc::new(Schema::new(vec![Field::new("s", ArrowType::Utf8, false)]));
        let array = Arc::new(StringArray::from_iter_values(["a", "b", "c"]));
        let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();

        let props = WriterProperties::builder()
            .set_dictionary_enabled(false)
            .build();
        let mut buf = Vec::new();
        {
            let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props)).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }
        let data = Bytes::from(buf);

        let reader = SerializedFileReader::new(data.clone()).unwrap();
        let metadata = reader.metadata();
        assert!(
            metadata
                .row_group(0)
                .column(0)
                .dictionary_page_offset()
                .is_none()
        );

        let result = ParquetMetaDataReader::read_column_dictionary(&data, metadata, 0, 0).unwrap();
        assert!(result.is_none());
    }
}
