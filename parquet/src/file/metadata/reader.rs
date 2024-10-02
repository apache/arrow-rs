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

/// Reads the [`ParquetMetaData`] from a byte stream.
///
/// See [`crate::file::metadata::ParquetMetaDataWriter#output-format`] for a description of
/// the Parquet metadata.
///
/// # Example
/// ```no_run
/// # use parquet::file::metadata::ParquetMetaDataReader;
/// # fn open_parquet_file(path: &str) -> std::fs::File { unimplemented!(); }
/// // read parquet metadata including page indexes
/// let file = open_parquet_file("some_path.parquet");
/// let mut reader = ParquetMetaDataReader::new()
///     .with_page_indexes(true);
/// reader.try_parse(&file).unwrap();
/// let metadata = reader.finish().unwrap();
/// assert!(metadata.column_index().is_some());
/// assert!(metadata.offset_index().is_some());
/// ```
#[derive(Default)]
pub struct ParquetMetaDataReader {
    metadata: Option<ParquetMetaData>,
    column_index: bool,
    offset_index: bool,
    prefetch_hint: Option<usize>,
}

impl ParquetMetaDataReader {
    /// Create a new [`ParquetMetaDataReader`]
    pub fn new() -> Self {
        Default::default()
    }

    /// Create a new [`ParquetMetaDataReader`] populated with a [`ParquetMetaData`] struct
    /// obtained via other means.
    pub fn new_with_metadata(metadata: ParquetMetaData) -> Self {
        Self {
            metadata: Some(metadata),
            ..Default::default()
        }
    }

    /// Enable or disable reading the page index structures described in
    /// "[Parquet page index]: Layout to Support Page Skipping". Equivalent to:
    /// `self.with_column_indexes(val).with_offset_indexes(val)`
    ///
    /// [Parquet page index]: https://github.com/apache/parquet-format/blob/master/PageIndex.md
    pub fn with_page_indexes(self, val: bool) -> Self {
        self.with_column_indexes(val).with_offset_indexes(val)
    }

    /// Enable or disable reading the Parquet [ColumnIndex] structure.
    ///
    /// [ColumnIndex]:  https://github.com/apache/parquet-format/blob/master/PageIndex.md
    pub fn with_column_indexes(mut self, val: bool) -> Self {
        self.column_index = val;
        self
    }

    /// Enable or disable reading the Parquet [OffsetIndex] structure.
    ///
    /// [OffsetIndex]:  https://github.com/apache/parquet-format/blob/master/PageIndex.md
    pub fn with_offset_indexes(mut self, val: bool) -> Self {
        self.offset_index = val;
        self
    }

    /// Provide a hint as to the number of bytes needed to fully parse the [`ParquetMetaData`].
    /// Only used for the asynchronous [`Self::try_load()`] method.
    ///
    /// By default, the reader will first fetch the last 8 bytes of the input file to obtain the
    /// size of the footer metadata. A second fetch will be performed to obtain the needed bytes.
    /// After parsing the footer metadata, a third fetch will be performed to obtain the bytes
    /// needed to decode the page index structures, if they have been requested. To avoid
    /// unnecessary fetches, `prefetch` can be set to an estimate of the number of bytes needed
    /// to fully decode the [`ParquetMetaData`], which can reduce the number of fetch requests and
    /// reduce latency. Setting `prefetch` too small will not trigger an error, but will result
    /// in extra fetches being performed.
    pub fn with_prefetch_hint(mut self, prefetch: Option<usize>) -> Self {
        self.prefetch_hint = prefetch;
        self
    }

    /// Indicates whether this reader has a [`ParquetMetaData`] internally.
    pub fn has_metadata(&self) -> bool {
        self.metadata.is_some()
    }

    /// Return the parsed [`ParquetMetaData`] struct, leaving `None` in its place.
    pub fn finish(&mut self) -> Result<ParquetMetaData> {
        self.metadata
            .take()
            .ok_or_else(|| general_err!("could not parse parquet metadata"))
    }

    /// Given a [`ChunkReader`], parse and return the [`ParquetMetaData`] in a single pass.
    ///
    /// If `reader` is [`Bytes`] based, then the buffer must contain sufficient bytes to complete
    /// the request, and must include the Parquet footer. If page indexes are desired, the buffer
    /// must contain the entire file, or [`Self::try_parse_sized()`] should be used.
    ///
    /// This call will consume `self`.
    ///
    /// # Example
    /// ```no_run
    /// # use parquet::file::metadata::ParquetMetaDataReader;
    /// # fn open_parquet_file(path: &str) -> std::fs::File { unimplemented!(); }
    /// // read parquet metadata including page indexes
    /// let file = open_parquet_file("some_path.parquet");
    /// let metadata = ParquetMetaDataReader::new()
    ///     .with_page_indexes(true)
    ///     .parse_and_finish(&file).unwrap();
    /// ```
    pub fn parse_and_finish<R: ChunkReader>(mut self, reader: &R) -> Result<ParquetMetaData> {
        self.try_parse(reader)?;
        self.finish()
    }

    /// Attempts to parse the footer metadata (and optionally page indexes) given a [`ChunkReader`].
    ///
    /// If `reader` is [`Bytes`] based, then the buffer must contain sufficient bytes to complete
    /// the request, and must include the Parquet footer. If page indexes are desired, the buffer
    /// must contain the entire file, or [`Self::try_parse_sized()`] should be used.
    pub fn try_parse<R: ChunkReader>(&mut self, reader: &R) -> Result<()> {
        self.try_parse_sized(reader, reader.len() as usize)
    }

    /// Same as [`Self::try_parse()`], but provide the original file size in the case that `reader`
    /// is a [`Bytes`] struct that does not contain the entire file. This information is necessary
    /// when the page indexes are desired. `reader` must have access to the Parquet footer.
    ///
    /// Using this function also allows for retrying with a larger buffer.
    ///
    /// # Errors
    ///
    /// This function will return [`ParquetError::IndexOutOfBound`] in the event `reader` does not
    /// provide enough data to fully parse the metadata (see example below).
    ///
    /// Other errors returned include [`ParquetError::General`] and [`ParquetError::EOF`].
    ///
    /// # Example
    /// ```no_run
    /// # use parquet::file::metadata::ParquetMetaDataReader;
    /// # use parquet::errors::ParquetError;
    /// # use crate::parquet::file::reader::Length;
    /// # fn get_bytes(file: &std::fs::File, range: std::ops::Range<usize>) -> bytes::Bytes { unimplemented!(); }
    /// # fn open_parquet_file(path: &str) -> std::fs::File { unimplemented!(); }
    /// let file = open_parquet_file("some_path.parquet");
    /// let len = file.len() as usize;
    /// let bytes = get_bytes(&file, 1000..len);
    /// let mut reader = ParquetMetaDataReader::new().with_page_indexes(true);
    /// match reader.try_parse_sized(&bytes, len) {
    ///     Ok(_) => (),
    ///     Err(ParquetError::IndexOutOfBound(needed, _)) => {
    ///         let bytes = get_bytes(&file, len - needed..len);
    ///         reader.try_parse_sized(&bytes, len).unwrap();
    ///     }
    ///     _ => panic!("unexpected error")
    /// }
    /// let metadata = reader.finish().unwrap();
    /// ```
    pub fn try_parse_sized<R: ChunkReader>(&mut self, reader: &R, file_size: usize) -> Result<()> {
        self.metadata = match Self::parse_metadata(reader) {
            Ok(metadata) => Some(metadata),
            // FIXME: throughout this module ParquetError::IndexOutOfBound is used to indicate the
            // need for more data. This is not it's intended use. The plan is to add a NeedMoreData
            // value to the enum, but this would be a breaking change. This will be done as
            // 54.0.0 draws nearer.
            // https://github.com/apache/arrow-rs/issues/6447
            Err(ParquetError::IndexOutOfBound(needed, _)) => {
                // If reader is the same length as `file_size` then presumably there is no more to
                // read, so return an EOF error.
                if file_size == reader.len() as usize || needed > file_size {
                    return Err(eof_err!(
                        "Parquet file too small. Size is {} but need {}",
                        file_size,
                        needed
                    ));
                } else {
                    // Ask for a larger buffer
                    return Err(ParquetError::IndexOutOfBound(needed, file_size));
                }
            }
            Err(e) => return Err(e),
        };

        // we can return if page indexes aren't requested
        if !self.column_index && !self.offset_index {
            return Ok(());
        }

        self.read_page_indexes_sized(reader, file_size)
    }

    /// Read the page index structures when a [`ParquetMetaData`] has already been obtained.
    /// See [`Self::new_with_metadata()`] and [`Self::has_metadata()`].
    pub fn read_page_indexes<R: ChunkReader>(&mut self, reader: &R) -> Result<()> {
        self.read_page_indexes_sized(reader, reader.len() as usize)
    }

    /// Read the page index structures when a [`ParquetMetaData`] has already been obtained.
    /// This variant is used when `reader` cannot access the entire Parquet file (e.g. it is
    /// a [`Bytes`] struct containing the tail of the file).
    /// See [`Self::new_with_metadata()`] and [`Self::has_metadata()`].
    pub fn read_page_indexes_sized<R: ChunkReader>(
        &mut self,
        reader: &R,
        file_size: usize,
    ) -> Result<()> {
        if self.metadata.is_none() {
            return Err(general_err!(
                "Tried to read page indexes without ParquetMetaData metadata"
            ));
        }

        // FIXME: there are differing implementations in the case where page indexes are missing
        // from the file. `MetadataLoader` will leave them as `None`, while the parser in
        // `index_reader::read_columns_indexes` returns a vector of empty vectors.
        // It is best for this function to replicate the latter behavior for now, but in a future
        // breaking release, the two paths to retrieve metadata should be made consistent. Note that this is only
        // an issue if the user requested page indexes, so there is no need to provide empty
        // vectors in `try_parse_sized()`.
        // https://github.com/apache/arrow-rs/issues/6447

        // Get bounds needed for page indexes (if any are present in the file).
        let Some(range) = self.range_for_page_index() else {
            self.empty_page_indexes();
            return Ok(());
        };

        // Check to see if needed range is within `file_range`. Checking `range.end` seems
        // redundant, but it guards against `range_for_page_index()` returning garbage.
        let file_range = file_size.saturating_sub(reader.len() as usize)..file_size;
        if !(file_range.contains(&range.start) && file_range.contains(&range.end)) {
            // Requested range starts beyond EOF
            if range.end > file_size {
                return Err(eof_err!(
                    "Parquet file too small. Range {:?} is beyond file bounds {file_size}",
                    range
                ));
            } else {
                // Ask for a larger buffer
                return Err(ParquetError::IndexOutOfBound(
                    file_size - range.start,
                    file_size,
                ));
            }
        }

        let bytes_needed = range.end - range.start;
        let bytes = reader.get_bytes((range.start - file_range.start) as u64, bytes_needed)?;
        let offset = range.start;

        self.parse_column_index(&bytes, offset)?;
        self.parse_offset_index(&bytes, offset)?;

        Ok(())
    }

    /// Given a [`MetadataFetch`], parse and return the [`ParquetMetaData`] in a single pass.
    ///
    /// This call will consume `self`.
    ///
    /// See [`Self::with_prefetch_hint`] for a discussion of how to reduce the number of fetches
    /// performed by this function.
    #[cfg(feature = "async")]
    pub async fn load_and_finish<F: MetadataFetch>(
        mut self,
        fetch: F,
        file_size: usize,
    ) -> Result<ParquetMetaData> {
        self.try_load(fetch, file_size).await?;
        self.finish()
    }

    /// Attempts to (asynchronously) parse the footer metadata (and optionally page indexes)
    /// given a [`MetadataFetch`].
    ///
    /// See [`Self::with_prefetch_hint`] for a discussion of how to reduce the number of fetches
    /// performed by this function.
    #[cfg(feature = "async")]
    pub async fn try_load<F: MetadataFetch>(
        &mut self,
        mut fetch: F,
        file_size: usize,
    ) -> Result<()> {
        let (metadata, remainder) =
            Self::load_metadata(&mut fetch, file_size, self.get_prefetch_size()).await?;

        self.metadata = Some(metadata);

        // we can return if page indexes aren't requested
        if !self.column_index && !self.offset_index {
            return Ok(());
        }

        self.load_page_index_with_remainder(fetch, remainder).await
    }

    /// Asynchronously fetch the page index structures when a [`ParquetMetaData`] has already
    /// been obtained. See [`Self::new_with_metadata()`].
    #[cfg(feature = "async")]
    pub async fn load_page_index<F: MetadataFetch>(&mut self, fetch: F) -> Result<()> {
        self.load_page_index_with_remainder(fetch, None).await
    }

    #[cfg(feature = "async")]
    async fn load_page_index_with_remainder<F: MetadataFetch>(
        &mut self,
        mut fetch: F,
        remainder: Option<(usize, Bytes)>,
    ) -> Result<()> {
        if self.metadata.is_none() {
            return Err(general_err!("Footer metadata is not present"));
        }

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

    /// Set the column_index and offset_indexes to empty `Vec` for backwards compatibility
    ///
    /// See <https://github.com/apache/arrow-rs/pull/6451>  for details
    fn empty_page_indexes(&mut self) {
        let metadata = self.metadata.as_mut().unwrap();
        let num_row_groups = metadata.num_row_groups();
        if self.column_index {
            metadata.set_column_index(Some(vec![vec![]; num_row_groups]));
        }
        if self.offset_index {
            metadata.set_offset_index(Some(vec![vec![]; num_row_groups]));
        }
    }

    fn range_for_page_index(&self) -> Option<Range<usize>> {
        // sanity check
        self.metadata.as_ref()?;

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
            return Err(ParquetError::IndexOutOfBound(
                FOOTER_SIZE,
                file_size as usize,
            ));
        }

        let mut footer = [0_u8; 8];
        chunk_reader
            .get_read(file_size - 8)?
            .read_exact(&mut footer)?;

        let metadata_len = Self::decode_footer(&footer)?;
        let footer_metadata_len = FOOTER_SIZE + metadata_len;

        if footer_metadata_len > file_size as usize {
            return Err(ParquetError::IndexOutOfBound(
                footer_metadata_len,
                file_size as usize,
            ));
        }

        let start = file_size - footer_metadata_len as u64;
        Self::decode_metadata(chunk_reader.get_bytes(start, metadata_len)?.as_ref())
    }

    /// Return the number of bytes to read in the initial pass. If `prefetch_size` has
    /// been provided, then return that value if it is larger than the size of the Parquet
    /// file footer (8 bytes). Otherwise returns `8`.
    #[cfg(feature = "async")]
    fn get_prefetch_size(&self) -> usize {
        if let Some(prefetch) = self.prefetch_hint {
            if prefetch > FOOTER_SIZE {
                return prefetch;
            }
        }
        FOOTER_SIZE
    }

    #[cfg(feature = "async")]
    async fn load_metadata<F: MetadataFetch>(
        fetch: &mut F,
        file_size: usize,
        prefetch: usize,
    ) -> Result<(ParquetMetaData, Option<(usize, Bytes)>)> {
        if file_size < FOOTER_SIZE {
            return Err(eof_err!("file size of {} is less than footer", file_size));
        }

        // If a size hint is provided, read more than the minimum size
        // to try and avoid a second fetch.
        // Note: prefetch > file_size is ok since we're using saturating_sub.
        let footer_start = file_size.saturating_sub(prefetch);

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
        footer.copy_from_slice(&suffix[suffix_len - FOOTER_SIZE..suffix_len]);

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
    use crate::file::reader::Length;
    use crate::format::TypeDefinedOrder;
    use crate::schema::types::Type as SchemaType;
    use crate::util::test_common::file_util::get_test_file;

    #[test]
    fn test_parse_metadata_size_smaller_than_footer() {
        let test_file = tempfile::tempfile().unwrap();
        let err = ParquetMetaDataReader::parse_metadata(&test_file).unwrap_err();
        assert!(matches!(err, ParquetError::IndexOutOfBound(8, _)));
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
        let err = ParquetMetaDataReader::parse_metadata(&test_file).unwrap_err();
        assert!(matches!(err, ParquetError::IndexOutOfBound(263, _)));
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

    #[test]
    fn test_try_parse() {
        let file = get_test_file("alltypes_tiny_pages.parquet");
        let len = file.len() as usize;

        let mut reader = ParquetMetaDataReader::new().with_page_indexes(true);

        let bytes_for_range = |range: Range<usize>| {
            file.get_bytes(range.start as u64, range.end - range.start)
                .unwrap()
        };

        // read entire file
        let bytes = bytes_for_range(0..len);
        reader.try_parse(&bytes).unwrap();
        let metadata = reader.finish().unwrap();
        assert!(metadata.column_index.is_some());
        assert!(metadata.offset_index.is_some());

        // read more than enough of file
        let bytes = bytes_for_range(320000..len);
        reader.try_parse_sized(&bytes, len).unwrap();
        let metadata = reader.finish().unwrap();
        assert!(metadata.column_index.is_some());
        assert!(metadata.offset_index.is_some());

        // exactly enough
        let bytes = bytes_for_range(323583..len);
        reader.try_parse_sized(&bytes, len).unwrap();
        let metadata = reader.finish().unwrap();
        assert!(metadata.column_index.is_some());
        assert!(metadata.offset_index.is_some());

        // not enough for page index
        let bytes = bytes_for_range(323584..len);
        // should fail
        match reader.try_parse_sized(&bytes, len).unwrap_err() {
            // expected error, try again with provided bounds
            ParquetError::IndexOutOfBound(needed, _) => {
                let bytes = bytes_for_range(len - needed..len);
                reader.try_parse_sized(&bytes, len).unwrap();
                let metadata = reader.finish().unwrap();
                assert!(metadata.column_index.is_some());
                assert!(metadata.offset_index.is_some());
            }
            _ => panic!("unexpected error"),
        };

        // not enough for page index but lie about file size
        let bytes = bytes_for_range(323584..len);
        let reader_result = reader.try_parse_sized(&bytes, len - 323584).unwrap_err();
        assert_eq!(
            reader_result.to_string(),
            "EOF: Parquet file too small. Range 323583..452504 is beyond file bounds 130649"
        );

        // not enough for file metadata
        let mut reader = ParquetMetaDataReader::new();
        let bytes = bytes_for_range(452505..len);
        // should fail
        match reader.try_parse_sized(&bytes, len).unwrap_err() {
            // expected error, try again with provided bounds
            ParquetError::IndexOutOfBound(needed, _) => {
                let bytes = bytes_for_range(len - needed..len);
                reader.try_parse_sized(&bytes, len).unwrap();
                reader.finish().unwrap();
            }
            _ => panic!("unexpected error"),
        };

        // not enough for file metadata but use try_parse()
        let reader_result = reader.try_parse(&bytes).unwrap_err();
        assert_eq!(
            reader_result.to_string(),
            "EOF: Parquet file too small. Size is 1728 but need 1729"
        );

        // read head of file rather than tail
        let bytes = bytes_for_range(0..1000);
        let reader_result = reader.try_parse_sized(&bytes, len).unwrap_err();
        assert_eq!(
            reader_result.to_string(),
            "Parquet error: Invalid Parquet file. Corrupt footer"
        );

        // lie about file size
        let bytes = bytes_for_range(452510..len);
        let reader_result = reader.try_parse_sized(&bytes, len - 452505).unwrap_err();
        assert_eq!(
            reader_result.to_string(),
            "EOF: Parquet file too small. Size is 1728 but need 1729"
        );
    }
}

#[cfg(feature = "async")]
#[cfg(test)]
mod async_tests {
    use super::*;
    use bytes::Bytes;
    use futures::future::BoxFuture;
    use futures::FutureExt;
    use std::fs::File;
    use std::future::Future;
    use std::io::{Read, Seek, SeekFrom};
    use std::ops::Range;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::arrow::async_reader::MetadataFetch;
    use crate::file::reader::Length;
    use crate::util::test_common::file_util::get_test_file;

    struct MetadataFetchFn<F>(F);

    impl<F, Fut> MetadataFetch for MetadataFetchFn<F>
    where
        F: FnMut(Range<usize>) -> Fut + Send,
        Fut: Future<Output = Result<Bytes>> + Send,
    {
        fn fetch(&mut self, range: Range<usize>) -> BoxFuture<'_, Result<Bytes>> {
            async move { self.0(range).await }.boxed()
        }
    }

    fn read_range(file: &mut File, range: Range<usize>) -> Result<Bytes> {
        file.seek(SeekFrom::Start(range.start as _))?;
        let len = range.end - range.start;
        let mut buf = Vec::with_capacity(len);
        file.take(len as _).read_to_end(&mut buf)?;
        Ok(buf.into())
    }

    #[tokio::test]
    async fn test_simple() {
        let mut file = get_test_file("nulls.snappy.parquet");
        let len = file.len() as usize;

        let expected = ParquetMetaDataReader::new()
            .parse_and_finish(&file)
            .unwrap();
        let expected = expected.file_metadata().schema();
        let fetch_count = AtomicUsize::new(0);

        let mut fetch = |range| {
            fetch_count.fetch_add(1, Ordering::SeqCst);
            futures::future::ready(read_range(&mut file, range))
        };

        let input = MetadataFetchFn(&mut fetch);
        let actual = ParquetMetaDataReader::new()
            .load_and_finish(input, len)
            .await
            .unwrap();
        assert_eq!(actual.file_metadata().schema(), expected);
        assert_eq!(fetch_count.load(Ordering::SeqCst), 2);

        // Metadata hint too small - below footer size
        fetch_count.store(0, Ordering::SeqCst);
        let input = MetadataFetchFn(&mut fetch);
        let actual = ParquetMetaDataReader::new()
            .with_prefetch_hint(Some(7))
            .load_and_finish(input, len)
            .await
            .unwrap();
        assert_eq!(actual.file_metadata().schema(), expected);
        assert_eq!(fetch_count.load(Ordering::SeqCst), 2);

        // Metadata hint too small
        fetch_count.store(0, Ordering::SeqCst);
        let input = MetadataFetchFn(&mut fetch);
        let actual = ParquetMetaDataReader::new()
            .with_prefetch_hint(Some(10))
            .load_and_finish(input, len)
            .await
            .unwrap();
        assert_eq!(actual.file_metadata().schema(), expected);
        assert_eq!(fetch_count.load(Ordering::SeqCst), 2);

        // Metadata hint too large
        fetch_count.store(0, Ordering::SeqCst);
        let input = MetadataFetchFn(&mut fetch);
        let actual = ParquetMetaDataReader::new()
            .with_prefetch_hint(Some(500))
            .load_and_finish(input, len)
            .await
            .unwrap();
        assert_eq!(actual.file_metadata().schema(), expected);
        assert_eq!(fetch_count.load(Ordering::SeqCst), 1);

        // Metadata hint exactly correct
        fetch_count.store(0, Ordering::SeqCst);
        let input = MetadataFetchFn(&mut fetch);
        let actual = ParquetMetaDataReader::new()
            .with_prefetch_hint(Some(428))
            .load_and_finish(input, len)
            .await
            .unwrap();
        assert_eq!(actual.file_metadata().schema(), expected);
        assert_eq!(fetch_count.load(Ordering::SeqCst), 1);

        let input = MetadataFetchFn(&mut fetch);
        let err = ParquetMetaDataReader::new()
            .load_and_finish(input, 4)
            .await
            .unwrap_err()
            .to_string();
        assert_eq!(err, "EOF: file size of 4 is less than footer");

        let input = MetadataFetchFn(&mut fetch);
        let err = ParquetMetaDataReader::new()
            .load_and_finish(input, 20)
            .await
            .unwrap_err()
            .to_string();
        assert_eq!(err, "Parquet error: Invalid Parquet file. Corrupt footer");
    }

    #[tokio::test]
    async fn test_page_index() {
        let mut file = get_test_file("alltypes_tiny_pages.parquet");
        let len = file.len() as usize;
        let fetch_count = AtomicUsize::new(0);
        let mut fetch = |range| {
            fetch_count.fetch_add(1, Ordering::SeqCst);
            futures::future::ready(read_range(&mut file, range))
        };

        let f = MetadataFetchFn(&mut fetch);
        let mut loader = ParquetMetaDataReader::new().with_page_indexes(true);
        loader.try_load(f, len).await.unwrap();
        assert_eq!(fetch_count.load(Ordering::SeqCst), 3);
        let metadata = loader.finish().unwrap();
        assert!(metadata.offset_index().is_some() && metadata.column_index().is_some());

        // Prefetch just footer exactly
        fetch_count.store(0, Ordering::SeqCst);
        let f = MetadataFetchFn(&mut fetch);
        let mut loader = ParquetMetaDataReader::new()
            .with_page_indexes(true)
            .with_prefetch_hint(Some(1729));
        loader.try_load(f, len).await.unwrap();
        assert_eq!(fetch_count.load(Ordering::SeqCst), 2);
        let metadata = loader.finish().unwrap();
        assert!(metadata.offset_index().is_some() && metadata.column_index().is_some());

        // Prefetch more than footer but not enough
        fetch_count.store(0, Ordering::SeqCst);
        let f = MetadataFetchFn(&mut fetch);
        let mut loader = ParquetMetaDataReader::new()
            .with_page_indexes(true)
            .with_prefetch_hint(Some(130649));
        loader.try_load(f, len).await.unwrap();
        assert_eq!(fetch_count.load(Ordering::SeqCst), 2);
        let metadata = loader.finish().unwrap();
        assert!(metadata.offset_index().is_some() && metadata.column_index().is_some());

        // Prefetch exactly enough
        fetch_count.store(0, Ordering::SeqCst);
        let f = MetadataFetchFn(&mut fetch);
        let metadata = ParquetMetaDataReader::new()
            .with_page_indexes(true)
            .with_prefetch_hint(Some(130650))
            .load_and_finish(f, len)
            .await
            .unwrap();
        assert_eq!(fetch_count.load(Ordering::SeqCst), 1);
        assert!(metadata.offset_index().is_some() && metadata.column_index().is_some());
    }
}
