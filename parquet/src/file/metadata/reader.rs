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

use std::{io::Read, ops::Range, sync::Arc};

use bytes::Bytes;

use crate::basic::ColumnOrder;
use crate::errors::{ParquetError, Result};
use crate::file::metadata::{FileMetaData, ParquetMetaData, RowGroupMetaData};
use crate::file::page_index::index::Index;
use crate::file::page_index::index_reader::{acc_range, decode_column_index, decode_offset_index};
use crate::file::reader::ChunkReader;
use crate::file::{FOOTER_SIZE, PARQUET_MAGIC};
use crate::format::{ColumnOrder as TColumnOrder, FileMetaData as TFileMetaData};
use crate::schema::types;
use crate::schema::types::SchemaDescriptor;
use crate::thrift::{TCompactSliceInputProtocol, TSerializable};

#[cfg(feature = "async")]
use crate::arrow::async_reader::MetadataFetch;

/// Reads the [`ParquetMetaData``] from the footer of the parquet file.
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
pub fn parquet_metadata_from_file<R: ChunkReader>(
    file: &R,
    column_index: bool,
    offset_index: bool,
) -> Result<ParquetMetaData> {
    println!("pmff {column_index} {offset_index}");
    let mut reader = ParquetMetaDataReader::new()
        .with_column_indexes(column_index)
        .with_offset_indexes(offset_index);
    reader.try_parse(file)?;
    Ok(reader.finish()?)
}

pub struct ParquetMetaDataReader {
    metadata: Option<ParquetMetaData>,
    column_index: bool,
    offset_index: bool,
    prefetch_hint: Option<usize>,
}

impl ParquetMetaDataReader {
    /// Create a new [`ParquetMetaDataReader`]
    pub fn new() -> Self {
        Self {
            metadata: None,
            column_index: false,
            offset_index: false,
            prefetch_hint: None,
        }
    }

    pub fn new_with_metadata(metadata: ParquetMetaData) -> Self {
        Self {
            metadata: Some(metadata),
            column_index: false,
            offset_index: false,
            prefetch_hint: None,
        }
    }

    pub fn with_page_indexes(self, val: bool) -> Self {
        self.with_column_indexes(val).with_offset_indexes(val)
    }

    pub fn with_column_indexes(mut self, val: bool) -> Self {
        self.column_index = val;
        self
    }

    pub fn with_offset_indexes(mut self, val: bool) -> Self {
        self.offset_index = val;
        self
    }

    pub fn with_prefetch_hint(mut self, val: Option<usize>) -> Self {
        self.prefetch_hint = val;
        self
    }

    pub fn finish(&mut self) -> Result<ParquetMetaData> {
        if self.metadata.is_none() {
            return Err(general_err!("could not parse parquet metadata"));
        }
        Ok(self.metadata.take().unwrap())
    }

    /// Attempts to parse the footer (and optionally page indexes) given a [`ChunkReader`]. If the
    /// `ChunkReader` is [`Bytes`] based, then the buffer should contain the entire file. Since all
    /// bytes needed should be available, this will either succeed or return an error.
    pub fn try_parse<R: ChunkReader>(&mut self, reader: &R) -> Result<()> {
        self.metadata = Some(Self::parse_metadata(reader)?);

        // we can return if page indexes aren't requested
        if !self.column_index && !self.offset_index {
            return Ok(());
        }

        // Get bounds needed for page indexes (if any are present in the file).
        let range = self.range_for_page_index();
        let range = match range {
            Some(range) => range,
            None => return Ok(()),
        };

        let bytes_needed = range.end - range.start;
        if bytes_needed > reader.len() as usize {
            return Err(eof_err!(
                "Invalid Parquet file. Reported page index length of {} bytes, but file is only {} bytes",
                bytes_needed,
                reader.len()
            ));
        }

        let bytes = reader.get_bytes(range.start as u64, bytes_needed)?;
        let offset = range.start;

        self.parse_column_index(&bytes, offset)?;
        self.parse_offset_index(&bytes, offset)?;

        Ok(())
    }

    /// like [Self::try_parse] but async
    #[cfg(feature = "async")]
    pub async fn try_load<F: MetadataFetch>(
        &mut self,
        mut fetch: F,
        file_size: usize,
    ) -> Result<()> {
        let (metadata, remainder) =
            Self::load_metadata(&mut fetch, file_size, self.prefetch_hint).await?;

        self.metadata = Some(metadata);

        // we can return if page indexes aren't requested
        if !self.column_index && !self.offset_index {
            return Ok(());
        }

        self.load_page_index(fetch, remainder).await
    }

    // assuming the file metadata has been loaded already, just fetch the page indexes
    #[cfg(feature = "async")]
    pub async fn load_page_index<F: MetadataFetch>(
        &mut self,
        mut fetch: F,
        remainder: Option<(usize, Bytes)>,
    ) -> Result<()> {
        // Get bounds needed for page indexes (if any are present in the file).
        let range = self.range_for_page_index();
        let range = match range {
            Some(range) => range,
            None => return Ok(()),
        };

        let bytes = match &remainder {
            Some((remainder_start, remainder)) if *remainder_start <= range.start => {
                let offset = range.start - *remainder_start;
                remainder.slice(offset..range.end - *remainder_start + offset)
            }
            // Note: this will potentially fetch data already in remainder, this keeps things simple
            _ => fetch.fetch(range.start..range.end).await?,
        };

        // Sanity check
        assert_eq!(bytes.len(), range.end - range.start);
        let offset = range.start;

        self.parse_column_index(&bytes, offset)?;
        self.parse_offset_index(&bytes, offset)?;

        Ok(())
    }

    fn parse_column_index(&mut self, bytes: &Bytes, start_offset: usize) -> Result<()> {
        let metadata = self.metadata.as_mut().unwrap();
        if self.column_index {
            let index = metadata
                .row_groups()
                .iter()
                .map(|x| {
                    x.columns()
                        .iter()
                        .map(|c| match c.column_index_range() {
                            Some(r) => decode_column_index(
                                &bytes[r.start - start_offset..r.end - start_offset],
                                c.column_type(),
                            ),
                            None => Ok(Index::NONE),
                        })
                        .collect::<Result<Vec<_>>>()
                })
                .collect::<Result<Vec<_>>>()?;
            metadata.set_column_index(Some(index));
        }
        Ok(())
    }

    fn parse_offset_index(&mut self, bytes: &Bytes, start_offset: usize) -> Result<()> {
        let metadata = self.metadata.as_mut().unwrap();
        if self.offset_index {
            let index = metadata
                .row_groups()
                .iter()
                .map(|x| {
                    x.columns()
                        .iter()
                        .map(|c| match c.offset_index_range() {
                            Some(r) => decode_offset_index(
                                &bytes[r.start - start_offset..r.end - start_offset],
                            ),
                            None => Err(general_err!("missing offset index")),
                        })
                        .collect::<Result<Vec<_>>>()
                })
                .collect::<Result<Vec<_>>>()?;

            metadata.set_offset_index(Some(index));
        }
        Ok(())
    }

    fn range_for_page_index(&self) -> Option<Range<usize>> {
        // sanity check
        if self.metadata.is_none() {
            return None;
        }

        // Get bounds needed for page indexes (if any are present in the file).
        let mut range = None;
        let metadata = self.metadata.as_ref().unwrap();
        for c in metadata.row_groups().iter().flat_map(|r| r.columns()) {
            if self.column_index {
                range = acc_range(range, c.column_index_range());
            }
            if self.offset_index {
                range = acc_range(range, c.offset_index_range());
            }
        }
        range
    }

    // one-shot parse of footer
    fn parse_metadata<R: ChunkReader>(chunk_reader: &R) -> Result<ParquetMetaData> {
        // check file is large enough to hold footer
        let file_size = chunk_reader.len();
        if file_size < (FOOTER_SIZE as u64) {
            return Err(general_err!(
                "Invalid Parquet file. Size is smaller than footer"
            ));
        }

        let mut footer = [0_u8; 8];
        chunk_reader
            .get_read(file_size - 8)?
            .read_exact(&mut footer)?;

        let metadata_len = Self::decode_footer(&footer)?;
        let footer_metadata_len = FOOTER_SIZE + metadata_len;

        if footer_metadata_len > file_size as usize {
            return Err(general_err!(
                "Invalid Parquet file. Reported metadata length of {} + {} byte footer, but file is only {} bytes",
                metadata_len,
                FOOTER_SIZE,
                file_size
            ));
        }

        let start = file_size - footer_metadata_len as u64;
        Self::decode_metadata(chunk_reader.get_bytes(start, metadata_len)?.as_ref())
    }

    #[cfg(feature = "async")]
    async fn load_metadata<F: MetadataFetch>(
        fetch: &mut F,
        file_size: usize,
        prefetch: Option<usize>,
    ) -> Result<(ParquetMetaData, Option<(usize, Bytes)>)> {
        if file_size < FOOTER_SIZE {
            return Err(eof_err!("file size of {} is less than footer", file_size));
        }

        // If a size hint is provided, read more than the minimum size
        // to try and avoid a second fetch.
        let footer_start = if let Some(size_hint) = prefetch {
            file_size.saturating_sub(size_hint)
        } else {
            file_size - FOOTER_SIZE
        };

        let suffix = fetch.fetch(footer_start..file_size).await?;
        let suffix_len = suffix.len();
        let fetch_len = file_size - footer_start;
        if suffix_len < fetch_len {
            return Err(eof_err!(
                "metadata requires {} bytes, but could only read {}",
                fetch_len,
                suffix_len
            ));
        }

        let mut footer = [0; FOOTER_SIZE];
        footer.copy_from_slice(&suffix[suffix_len - 8..suffix_len]);

        let length = Self::decode_footer(&footer)?;

        if file_size < length + FOOTER_SIZE {
            return Err(eof_err!(
                "file size of {} is less than footer + metadata {}",
                file_size,
                length + FOOTER_SIZE
            ));
        }

        // Did not fetch the entire file metadata in the initial read, need to make a second request
        if length > suffix_len - FOOTER_SIZE {
            let metadata_start = file_size - length - FOOTER_SIZE;
            let meta = fetch.fetch(metadata_start..file_size - FOOTER_SIZE).await?;
            Ok((Self::decode_metadata(&meta)?, None))
        } else {
            let metadata_start = file_size - length - FOOTER_SIZE - footer_start;

            let slice = &suffix[metadata_start..suffix_len - FOOTER_SIZE];
            Ok((
                Self::decode_metadata(slice)?,
                Some((footer_start, suffix.slice(..metadata_start))),
            ))
        }
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
    pub fn decode_footer(slice: &[u8; FOOTER_SIZE]) -> Result<usize> {
        // check this is indeed a parquet file
        if slice[4..] != PARQUET_MAGIC {
            return Err(general_err!("Invalid Parquet file. Corrupt footer"));
        }

        // get the metadata length from the footer
        let metadata_len = u32::from_le_bytes(slice[..4].try_into().unwrap());
        // u32 won't be larger than usize in most cases
        Ok(metadata_len as usize)
    }

    /// Decodes [`ParquetMetaData`] from the provided bytes.
    ///
    /// Typically this is used to decode the metadata from the end of a parquet
    /// file. The format of `buf` is the Thift compact binary protocol, as specified
    /// by the [Parquet Spec].
    ///
    /// [Parquet Spec]: https://github.com/apache/parquet-format#metadata
    pub fn decode_metadata(buf: &[u8]) -> Result<ParquetMetaData> {
        // TODO: row group filtering
        let mut prot = TCompactSliceInputProtocol::new(buf);
        let t_file_metadata: TFileMetaData = TFileMetaData::read_from_in_protocol(&mut prot)
            .map_err(|e| general_err!("Could not parse metadata: {}", e))?;
        let schema = types::from_thrift(&t_file_metadata.schema)?;
        let schema_descr = Arc::new(SchemaDescriptor::new(schema));
        let mut row_groups = Vec::new();
        for rg in t_file_metadata.row_groups {
            row_groups.push(RowGroupMetaData::from_thrift(schema_descr.clone(), rg)?);
        }
        let column_orders = Self::parse_column_orders(t_file_metadata.column_orders, &schema_descr);

        let file_metadata = FileMetaData::new(
            t_file_metadata.version,
            t_file_metadata.num_rows,
            t_file_metadata.created_by,
            t_file_metadata.key_value_metadata,
            schema_descr,
            column_orders,
        );
        Ok(ParquetMetaData::new(file_metadata, row_groups))
    }

    /// Parses column orders from Thrift definition.
    /// If no column orders are defined, returns `None`.
    fn parse_column_orders(
        t_column_orders: Option<Vec<TColumnOrder>>,
        schema_descr: &SchemaDescriptor,
    ) -> Option<Vec<ColumnOrder>> {
        match t_column_orders {
            Some(orders) => {
                // Should always be the case
                assert_eq!(
                    orders.len(),
                    schema_descr.num_columns(),
                    "Column order length mismatch"
                );
                let mut res = Vec::new();
                for (i, column) in schema_descr.columns().iter().enumerate() {
                    match orders[i] {
                        TColumnOrder::TYPEORDER(_) => {
                            let sort_order = ColumnOrder::get_sort_order(
                                column.logical_type(),
                                column.converted_type(),
                                column.physical_type(),
                            );
                            res.push(ColumnOrder::TYPE_DEFINED_ORDER(sort_order));
                        }
                    }
                }
                Some(res)
            }
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    use crate::basic::SortOrder;
    use crate::basic::Type;
    use crate::format::TypeDefinedOrder;
    use crate::schema::types::Type as SchemaType;

    #[test]
    fn test_parse_metadata_size_smaller_than_footer() {
        let test_file = tempfile::tempfile().unwrap();
        let reader_result = ParquetMetaDataReader::parse_metadata(&test_file);
        assert_eq!(
            reader_result.unwrap_err().to_string(),
            "Parquet error: Invalid Parquet file. Size is smaller than footer"
        );
    }

    #[test]
    fn test_parse_metadata_corrupt_footer() {
        let data = Bytes::from(vec![1, 2, 3, 4, 5, 6, 7, 8]);
        let reader_result = ParquetMetaDataReader::parse_metadata(&data);
        assert_eq!(
            reader_result.unwrap_err().to_string(),
            "Parquet error: Invalid Parquet file. Corrupt footer"
        );
    }

    #[test]
    fn test_parse_metadata_invalid_start() {
        let test_file = Bytes::from(vec![255, 0, 0, 0, b'P', b'A', b'R', b'1']);
        let reader_result = ParquetMetaDataReader::parse_metadata(&test_file);
        assert_eq!(
            reader_result.unwrap_err().to_string(),
            "Parquet error: Invalid Parquet file. Reported metadata length of 255 + 8 byte footer, but file is only 8 bytes"
        );
    }

    #[test]
    fn test_metadata_column_orders_parse() {
        // Define simple schema, we do not need to provide logical types.
        let fields = vec![
            Arc::new(
                SchemaType::primitive_type_builder("col1", Type::INT32)
                    .build()
                    .unwrap(),
            ),
            Arc::new(
                SchemaType::primitive_type_builder("col2", Type::FLOAT)
                    .build()
                    .unwrap(),
            ),
        ];
        let schema = SchemaType::group_type_builder("schema")
            .with_fields(fields)
            .build()
            .unwrap();
        let schema_descr = SchemaDescriptor::new(Arc::new(schema));

        let t_column_orders = Some(vec![
            TColumnOrder::TYPEORDER(TypeDefinedOrder::new()),
            TColumnOrder::TYPEORDER(TypeDefinedOrder::new()),
        ]);

        assert_eq!(
            ParquetMetaDataReader::parse_column_orders(t_column_orders, &schema_descr),
            Some(vec![
                ColumnOrder::TYPE_DEFINED_ORDER(SortOrder::SIGNED),
                ColumnOrder::TYPE_DEFINED_ORDER(SortOrder::SIGNED)
            ])
        );

        // Test when no column orders are defined.
        assert_eq!(
            ParquetMetaDataReader::parse_column_orders(None, &schema_descr),
            None
        );
    }

    #[test]
    #[should_panic(expected = "Column order length mismatch")]
    fn test_metadata_column_orders_len_mismatch() {
        let schema = SchemaType::group_type_builder("schema").build().unwrap();
        let schema_descr = SchemaDescriptor::new(Arc::new(schema));

        let t_column_orders = Some(vec![TColumnOrder::TYPEORDER(TypeDefinedOrder::new())]);

        ParquetMetaDataReader::parse_column_orders(t_column_orders, &schema_descr);
    }
}
