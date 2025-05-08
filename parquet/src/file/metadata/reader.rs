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

use crate::basic::ColumnOrder;
#[cfg(feature = "encryption")]
use crate::encryption::{
    decrypt::{FileDecryptionProperties, FileDecryptor},
    modules::create_footer_aad,
};
use bytes::Bytes;

use crate::errors::{ParquetError, Result};
use crate::file::metadata::{ColumnChunkMetaData, FileMetaData, ParquetMetaData, RowGroupMetaData};
use crate::file::page_index::index::Index;
use crate::file::page_index::index_reader::{acc_range, decode_column_index, decode_offset_index};
use crate::file::reader::ChunkReader;
use crate::file::{FOOTER_SIZE, PARQUET_MAGIC, PARQUET_MAGIC_ENCR_FOOTER};
use crate::format::{ColumnOrder as TColumnOrder, FileMetaData as TFileMetaData};
#[cfg(feature = "encryption")]
use crate::format::{EncryptionAlgorithm, FileCryptoMetaData as TFileCryptoMetaData};
use crate::schema::types;
use crate::schema::types::SchemaDescriptor;
use crate::thrift::{TCompactSliceInputProtocol, TSerializable};

#[cfg(all(feature = "async", feature = "arrow"))]
use crate::arrow::async_reader::{MetadataFetch, MetadataSuffixFetch};
#[cfg(feature = "encryption")]
use crate::encryption::decrypt::CryptoContext;
use crate::file::page_index::offset_index::OffsetIndexMetaData;

/// Reads the [`ParquetMetaData`] from a byte stream.
///
/// See [`crate::file::metadata::ParquetMetaDataWriter#output-format`] for a description of
/// the Parquet metadata.
///
/// Parquet metadata is not necessarily contiguous in the files: part is stored
/// in the footer (the last bytes of the file), but other portions (such as the
/// PageIndex) can be stored elsewhere.
///
/// This reader handles reading the footer as well as the non contiguous parts
/// of the metadata such as the page indexes; excluding Bloom Filters.
///
/// # Example
/// ```no_run
/// # use parquet::file::metadata::ParquetMetaDataReader;
/// # fn open_parquet_file(path: &str) -> std::fs::File { unimplemented!(); }
/// // read parquet metadata including page indexes from a file
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
    // Size of the serialized thrift metadata plus the 8 byte footer. Only set if
    // `self.parse_metadata` is called.
    metadata_size: Option<usize>,
    #[cfg(feature = "encryption")]
    file_decryption_properties: Option<FileDecryptionProperties>,
}

/// Describes how the footer metadata is stored
///
/// This is parsed from the last 8 bytes of the Parquet file
pub struct FooterTail {
    metadata_length: usize,
    encrypted_footer: bool,
}

impl FooterTail {
    /// The length of the footer metadata in bytes
    pub fn metadata_length(&self) -> usize {
        self.metadata_length
    }

    /// Whether the footer metadata is encrypted
    pub fn is_encrypted_footer(&self) -> bool {
        self.encrypted_footer
    }
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

    /// Provide the FileDecryptionProperties to use when decrypting the file.
    ///
    /// This is only necessary when the file is encrypted.
    #[cfg(feature = "encryption")]
    pub fn with_decryption_properties(
        mut self,
        properties: Option<&FileDecryptionProperties>,
    ) -> Self {
        self.file_decryption_properties = properties.cloned();
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
        self.try_parse_sized(reader, reader.len())
    }

    /// Same as [`Self::try_parse()`], but provide the original file size in the case that `reader`
    /// is a [`Bytes`] struct that does not contain the entire file. This information is necessary
    /// when the page indexes are desired. `reader` must have access to the Parquet footer.
    ///
    /// Using this function also allows for retrying with a larger buffer.
    ///
    /// # Errors
    ///
    /// This function will return [`ParquetError::NeedMoreData`] in the event `reader` does not
    /// provide enough data to fully parse the metadata (see example below). The returned error
    /// will be populated with a `usize` field indicating the number of bytes required from the
    /// tail of the file to completely parse the requested metadata.
    ///
    /// Other errors returned include [`ParquetError::General`] and [`ParquetError::EOF`].
    ///
    /// # Example
    /// ```no_run
    /// # use parquet::file::metadata::ParquetMetaDataReader;
    /// # use parquet::errors::ParquetError;
    /// # use crate::parquet::file::reader::Length;
    /// # fn get_bytes(file: &std::fs::File, range: std::ops::Range<u64>) -> bytes::Bytes { unimplemented!(); }
    /// # fn open_parquet_file(path: &str) -> std::fs::File { unimplemented!(); }
    /// let file = open_parquet_file("some_path.parquet");
    /// let len = file.len();
    /// // Speculatively read 1 kilobyte from the end of the file
    /// let bytes = get_bytes(&file, len - 1024..len);
    /// let mut reader = ParquetMetaDataReader::new().with_page_indexes(true);
    /// match reader.try_parse_sized(&bytes, len) {
    ///     Ok(_) => (),
    ///     Err(ParquetError::NeedMoreData(needed)) => {
    ///         // Read the needed number of bytes from the end of the file
    ///         let bytes = get_bytes(&file, len - needed as u64..len);
    ///         reader.try_parse_sized(&bytes, len).unwrap();
    ///     }
    ///     _ => panic!("unexpected error")
    /// }
    /// let metadata = reader.finish().unwrap();
    /// ```
    ///
    /// Note that it is possible for the file metadata to be completely read, but there are
    /// insufficient bytes available to read the page indexes. [`Self::has_metadata()`] can be used
    /// to test for this. In the event the file metadata is present, re-parsing of the file
    /// metadata can be skipped by using [`Self::read_page_indexes_sized()`], as shown below.
    /// ```no_run
    /// # use parquet::file::metadata::ParquetMetaDataReader;
    /// # use parquet::errors::ParquetError;
    /// # use crate::parquet::file::reader::Length;
    /// # fn get_bytes(file: &std::fs::File, range: std::ops::Range<u64>) -> bytes::Bytes { unimplemented!(); }
    /// # fn open_parquet_file(path: &str) -> std::fs::File { unimplemented!(); }
    /// let file = open_parquet_file("some_path.parquet");
    /// let len = file.len();
    /// // Speculatively read 1 kilobyte from the end of the file
    /// let mut bytes = get_bytes(&file, len - 1024..len);
    /// let mut reader = ParquetMetaDataReader::new().with_page_indexes(true);
    /// // Loop until `bytes` is large enough
    /// loop {
    ///     match reader.try_parse_sized(&bytes, len) {
    ///         Ok(_) => break,
    ///         Err(ParquetError::NeedMoreData(needed)) => {
    ///             // Read the needed number of bytes from the end of the file
    ///             bytes = get_bytes(&file, len - needed as u64..len);
    ///             // If file metadata was read only read page indexes, otherwise continue loop
    ///             if reader.has_metadata() {
    ///                 reader.read_page_indexes_sized(&bytes, len);
    ///                 break;
    ///             }
    ///         }
    ///         _ => panic!("unexpected error")
    ///     }
    /// }
    /// let metadata = reader.finish().unwrap();
    /// ```
    pub fn try_parse_sized<R: ChunkReader>(&mut self, reader: &R, file_size: u64) -> Result<()> {
        self.metadata = match self.parse_metadata(reader) {
            Ok(metadata) => Some(metadata),
            Err(ParquetError::NeedMoreData(needed)) => {
                // If reader is the same length as `file_size` then presumably there is no more to
                // read, so return an EOF error.
                if file_size == reader.len() || needed as u64 > file_size {
                    return Err(eof_err!(
                        "Parquet file too small. Size is {} but need {}",
                        file_size,
                        needed
                    ));
                } else {
                    // Ask for a larger buffer
                    return Err(ParquetError::NeedMoreData(needed));
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
        self.read_page_indexes_sized(reader, reader.len())
    }

    /// Read the page index structures when a [`ParquetMetaData`] has already been obtained.
    /// This variant is used when `reader` cannot access the entire Parquet file (e.g. it is
    /// a [`Bytes`] struct containing the tail of the file).
    /// See [`Self::new_with_metadata()`] and [`Self::has_metadata()`]. Like
    /// [`Self::try_parse_sized()`] this function may return [`ParquetError::NeedMoreData`].
    pub fn read_page_indexes_sized<R: ChunkReader>(
        &mut self,
        reader: &R,
        file_size: u64,
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
            return Ok(());
        };

        // Check to see if needed range is within `file_range`. Checking `range.end` seems
        // redundant, but it guards against `range_for_page_index()` returning garbage.
        let file_range = file_size.saturating_sub(reader.len())..file_size;
        if !(file_range.contains(&range.start) && file_range.contains(&range.end)) {
            // Requested range starts beyond EOF
            if range.end > file_size {
                return Err(eof_err!(
                    "Parquet file too small. Range {:?} is beyond file bounds {file_size}",
                    range
                ));
            } else {
                // Ask for a larger buffer
                return Err(ParquetError::NeedMoreData(
                    (file_size - range.start).try_into()?,
                ));
            }
        }

        // Perform extra sanity check to make sure `range` and the footer metadata don't
        // overlap.
        if let Some(metadata_size) = self.metadata_size {
            let metadata_range = file_size.saturating_sub(metadata_size as u64)..file_size;
            if range.end > metadata_range.start {
                return Err(eof_err!(
                    "Parquet file too small. Page index range {:?} overlaps with file metadata {:?}",
                    range,
                    metadata_range
                ));
            }
        }

        let bytes_needed = usize::try_from(range.end - range.start)?;
        let bytes = reader.get_bytes(range.start - file_range.start, bytes_needed)?;
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
    #[cfg(all(feature = "async", feature = "arrow"))]
    pub async fn load_and_finish<F: MetadataFetch>(
        mut self,
        fetch: F,
        file_size: u64,
    ) -> Result<ParquetMetaData> {
        self.try_load(fetch, file_size).await?;
        self.finish()
    }

    /// Given a [`MetadataSuffixFetch`], parse and return the [`ParquetMetaData`] in a single pass.
    ///
    /// This call will consume `self`.
    ///
    /// See [`Self::with_prefetch_hint`] for a discussion of how to reduce the number of fetches
    /// performed by this function.
    #[cfg(all(feature = "async", feature = "arrow"))]
    pub async fn load_via_suffix_and_finish<F: MetadataSuffixFetch>(
        mut self,
        fetch: F,
    ) -> Result<ParquetMetaData> {
        self.try_load_via_suffix(fetch).await?;
        self.finish()
    }
    /// Attempts to (asynchronously) parse the footer metadata (and optionally page indexes)
    /// given a [`MetadataFetch`].
    ///
    /// See [`Self::with_prefetch_hint`] for a discussion of how to reduce the number of fetches
    /// performed by this function.
    #[cfg(all(feature = "async", feature = "arrow"))]
    pub async fn try_load<F: MetadataFetch>(&mut self, mut fetch: F, file_size: u64) -> Result<()> {
        let (metadata, remainder) = self.load_metadata(&mut fetch, file_size).await?;

        self.metadata = Some(metadata);

        // we can return if page indexes aren't requested
        if !self.column_index && !self.offset_index {
            return Ok(());
        }

        self.load_page_index_with_remainder(fetch, remainder).await
    }

    /// Attempts to (asynchronously) parse the footer metadata (and optionally page indexes)
    /// given a [`MetadataSuffixFetch`].
    ///
    /// See [`Self::with_prefetch_hint`] for a discussion of how to reduce the number of fetches
    /// performed by this function.
    #[cfg(all(feature = "async", feature = "arrow"))]
    pub async fn try_load_via_suffix<F: MetadataSuffixFetch>(
        &mut self,
        mut fetch: F,
    ) -> Result<()> {
        let (metadata, remainder) = self.load_metadata_via_suffix(&mut fetch).await?;

        self.metadata = Some(metadata);

        // we can return if page indexes aren't requested
        if !self.column_index && !self.offset_index {
            return Ok(());
        }

        self.load_page_index_with_remainder(fetch, remainder).await
    }

    /// Asynchronously fetch the page index structures when a [`ParquetMetaData`] has already
    /// been obtained. See [`Self::new_with_metadata()`].
    #[cfg(all(feature = "async", feature = "arrow"))]
    pub async fn load_page_index<F: MetadataFetch>(&mut self, fetch: F) -> Result<()> {
        self.load_page_index_with_remainder(fetch, None).await
    }

    #[cfg(all(feature = "async", feature = "arrow"))]
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
            Some((remainder_start, remainder)) if *remainder_start as u64 <= range.start => {
                let remainder_start = *remainder_start as u64;
                let offset = usize::try_from(range.start - remainder_start)?;
                let end = usize::try_from(range.end - remainder_start)?;
                assert!(end <= remainder.len());
                remainder.slice(offset..end)
            }
            // Note: this will potentially fetch data already in remainder, this keeps things simple
            _ => fetch.fetch(range.start..range.end).await?,
        };

        // Sanity check
        assert_eq!(bytes.len() as u64, range.end - range.start);

        self.parse_column_index(&bytes, range.start)?;
        self.parse_offset_index(&bytes, range.start)?;

        Ok(())
    }

    fn parse_column_index(&mut self, bytes: &Bytes, start_offset: u64) -> Result<()> {
        let metadata = self.metadata.as_mut().unwrap();
        if self.column_index {
            let index = metadata
                .row_groups()
                .iter()
                .enumerate()
                .map(|(rg_idx, x)| {
                    x.columns()
                        .iter()
                        .enumerate()
                        .map(|(col_idx, c)| match c.column_index_range() {
                            Some(r) => {
                                let r_start = usize::try_from(r.start - start_offset)?;
                                let r_end = usize::try_from(r.end - start_offset)?;
                                Self::parse_single_column_index(
                                    &bytes[r_start..r_end],
                                    metadata,
                                    c,
                                    rg_idx,
                                    col_idx,
                                )
                            }
                            None => Ok(Index::NONE),
                        })
                        .collect::<Result<Vec<_>>>()
                })
                .collect::<Result<Vec<_>>>()?;
            metadata.set_column_index(Some(index));
        }
        Ok(())
    }

    #[cfg(feature = "encryption")]
    fn parse_single_column_index(
        bytes: &[u8],
        metadata: &ParquetMetaData,
        column: &ColumnChunkMetaData,
        row_group_index: usize,
        col_index: usize,
    ) -> Result<Index> {
        match &column.column_crypto_metadata {
            Some(crypto_metadata) => {
                let file_decryptor = metadata.file_decryptor.as_ref().ok_or_else(|| {
                    general_err!("Cannot decrypt column index, no file decryptor set")
                })?;
                let crypto_context = CryptoContext::for_column(
                    file_decryptor,
                    crypto_metadata,
                    row_group_index,
                    col_index,
                )?;
                let column_decryptor = crypto_context.metadata_decryptor();
                let aad = crypto_context.create_column_index_aad()?;
                let plaintext = column_decryptor.decrypt(bytes, &aad)?;
                decode_column_index(&plaintext, column.column_type())
            }
            None => decode_column_index(bytes, column.column_type()),
        }
    }

    #[cfg(not(feature = "encryption"))]
    fn parse_single_column_index(
        bytes: &[u8],
        _metadata: &ParquetMetaData,
        column: &ColumnChunkMetaData,
        _row_group_index: usize,
        _col_index: usize,
    ) -> Result<Index> {
        decode_column_index(bytes, column.column_type())
    }

    fn parse_offset_index(&mut self, bytes: &Bytes, start_offset: u64) -> Result<()> {
        let metadata = self.metadata.as_mut().unwrap();
        if self.offset_index {
            let index = metadata
                .row_groups()
                .iter()
                .enumerate()
                .map(|(rg_idx, x)| {
                    x.columns()
                        .iter()
                        .enumerate()
                        .map(|(col_idx, c)| match c.offset_index_range() {
                            Some(r) => {
                                let r_start = usize::try_from(r.start - start_offset)?;
                                let r_end = usize::try_from(r.end - start_offset)?;
                                Self::parse_single_offset_index(
                                    &bytes[r_start..r_end],
                                    metadata,
                                    c,
                                    rg_idx,
                                    col_idx,
                                )
                            }
                            None => Err(general_err!("missing offset index")),
                        })
                        .collect::<Result<Vec<_>>>()
                })
                .collect::<Result<Vec<_>>>()?;

            metadata.set_offset_index(Some(index));
        }
        Ok(())
    }

    #[cfg(feature = "encryption")]
    fn parse_single_offset_index(
        bytes: &[u8],
        metadata: &ParquetMetaData,
        column: &ColumnChunkMetaData,
        row_group_index: usize,
        col_index: usize,
    ) -> Result<OffsetIndexMetaData> {
        match &column.column_crypto_metadata {
            Some(crypto_metadata) => {
                let file_decryptor = metadata.file_decryptor.as_ref().ok_or_else(|| {
                    general_err!("Cannot decrypt offset index, no file decryptor set")
                })?;
                let crypto_context = CryptoContext::for_column(
                    file_decryptor,
                    crypto_metadata,
                    row_group_index,
                    col_index,
                )?;
                let column_decryptor = crypto_context.metadata_decryptor();
                let aad = crypto_context.create_offset_index_aad()?;
                let plaintext = column_decryptor.decrypt(bytes, &aad)?;
                decode_offset_index(&plaintext)
            }
            None => decode_offset_index(bytes),
        }
    }

    #[cfg(not(feature = "encryption"))]
    fn parse_single_offset_index(
        bytes: &[u8],
        _metadata: &ParquetMetaData,
        _column: &ColumnChunkMetaData,
        _row_group_index: usize,
        _col_index: usize,
    ) -> Result<OffsetIndexMetaData> {
        decode_offset_index(bytes)
    }

    fn range_for_page_index(&self) -> Option<Range<u64>> {
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

    // One-shot parse of footer.
    // Side effect: this will set `self.metadata_size`
    fn parse_metadata<R: ChunkReader>(&mut self, chunk_reader: &R) -> Result<ParquetMetaData> {
        // check file is large enough to hold footer
        let file_size = chunk_reader.len();
        if file_size < (FOOTER_SIZE as u64) {
            return Err(ParquetError::NeedMoreData(FOOTER_SIZE));
        }

        let mut footer = [0_u8; 8];
        chunk_reader
            .get_read(file_size - 8)?
            .read_exact(&mut footer)?;

        let footer = Self::decode_footer_tail(&footer)?;
        let metadata_len = footer.metadata_length();
        let footer_metadata_len = FOOTER_SIZE + metadata_len;
        self.metadata_size = Some(footer_metadata_len);

        if footer_metadata_len as u64 > file_size {
            return Err(ParquetError::NeedMoreData(footer_metadata_len));
        }

        let start = file_size - footer_metadata_len as u64;
        self.decode_footer_metadata(
            chunk_reader.get_bytes(start, metadata_len)?.as_ref(),
            &footer,
        )
    }

    /// Return the number of bytes to read in the initial pass. If `prefetch_size` has
    /// been provided, then return that value if it is larger than the size of the Parquet
    /// file footer (8 bytes). Otherwise returns `8`.
    #[cfg(all(feature = "async", feature = "arrow"))]
    fn get_prefetch_size(&self) -> usize {
        if let Some(prefetch) = self.prefetch_hint {
            if prefetch > FOOTER_SIZE {
                return prefetch;
            }
        }
        FOOTER_SIZE
    }

    #[cfg(all(feature = "async", feature = "arrow"))]
    async fn load_metadata<F: MetadataFetch>(
        &self,
        fetch: &mut F,
        file_size: u64,
    ) -> Result<(ParquetMetaData, Option<(usize, Bytes)>)> {
        let prefetch = self.get_prefetch_size() as u64;

        if file_size < FOOTER_SIZE as u64 {
            return Err(eof_err!("file size of {} is less than footer", file_size));
        }

        // If a size hint is provided, read more than the minimum size
        // to try and avoid a second fetch.
        // Note: prefetch > file_size is ok since we're using saturating_sub.
        let footer_start = file_size.saturating_sub(prefetch);

        let suffix = fetch.fetch(footer_start..file_size).await?;
        let suffix_len = suffix.len();
        let fetch_len = (file_size - footer_start)
            .try_into()
            .expect("footer size should never be larger than u32");
        if suffix_len < fetch_len {
            return Err(eof_err!(
                "metadata requires {} bytes, but could only read {}",
                fetch_len,
                suffix_len
            ));
        }

        let mut footer = [0; FOOTER_SIZE];
        footer.copy_from_slice(&suffix[suffix_len - FOOTER_SIZE..suffix_len]);

        let footer = Self::decode_footer_tail(&footer)?;
        let length = footer.metadata_length();

        if file_size < (length + FOOTER_SIZE) as u64 {
            return Err(eof_err!(
                "file size of {} is less than footer + metadata {}",
                file_size,
                length + FOOTER_SIZE
            ));
        }

        // Did not fetch the entire file metadata in the initial read, need to make a second request
        if length > suffix_len - FOOTER_SIZE {
            let metadata_start = file_size - (length + FOOTER_SIZE) as u64;
            let meta = fetch
                .fetch(metadata_start..(file_size - FOOTER_SIZE as u64))
                .await?;
            Ok((self.decode_footer_metadata(&meta, &footer)?, None))
        } else {
            let metadata_start = (file_size - (length + FOOTER_SIZE) as u64 - footer_start)
                .try_into()
                .expect("metadata length should never be larger than u32");
            let slice = &suffix[metadata_start..suffix_len - FOOTER_SIZE];
            Ok((
                self.decode_footer_metadata(slice, &footer)?,
                Some((footer_start as usize, suffix.slice(..metadata_start))),
            ))
        }
    }

    #[cfg(all(feature = "async", feature = "arrow"))]
    async fn load_metadata_via_suffix<F: MetadataSuffixFetch>(
        &self,
        fetch: &mut F,
    ) -> Result<(ParquetMetaData, Option<(usize, Bytes)>)> {
        let prefetch = self.get_prefetch_size();

        let suffix = fetch.fetch_suffix(prefetch as _).await?;
        let suffix_len = suffix.len();

        if suffix_len < FOOTER_SIZE {
            return Err(eof_err!(
                "footer metadata requires {} bytes, but could only read {}",
                FOOTER_SIZE,
                suffix_len
            ));
        }

        let mut footer = [0; FOOTER_SIZE];
        footer.copy_from_slice(&suffix[suffix_len - FOOTER_SIZE..suffix_len]);

        let footer = Self::decode_footer_tail(&footer)?;
        let length = footer.metadata_length();

        // Did not fetch the entire file metadata in the initial read, need to make a second request
        let metadata_offset = length + FOOTER_SIZE;
        if length > suffix_len - FOOTER_SIZE {
            let meta = fetch.fetch_suffix(metadata_offset).await?;

            if meta.len() < metadata_offset {
                return Err(eof_err!(
                    "metadata requires {} bytes, but could only read {}",
                    metadata_offset,
                    meta.len()
                ));
            }

            Ok((
                // need to slice off the footer or decryption fails
                self.decode_footer_metadata(&meta.slice(0..length), &footer)?,
                None,
            ))
        } else {
            let metadata_start = suffix_len - metadata_offset;
            let slice = &suffix[metadata_start..suffix_len - FOOTER_SIZE];
            Ok((
                self.decode_footer_metadata(slice, &footer)?,
                Some((0, suffix.slice(..metadata_start))),
            ))
        }
    }

    /// Decodes the end of the Parquet footer
    ///
    /// There are 8 bytes at the end of the Parquet footer with the following layout:
    /// * 4 bytes for the metadata length
    /// * 4 bytes for the magic bytes 'PAR1' or 'PARE' (encrypted footer)
    ///
    /// ```text
    /// +-----+------------------+
    /// | len | 'PAR1' or 'PARE' |
    /// +-----+------------------+
    /// ```
    pub fn decode_footer_tail(slice: &[u8; FOOTER_SIZE]) -> Result<FooterTail> {
        let magic = &slice[4..];
        let encrypted_footer = if magic == PARQUET_MAGIC_ENCR_FOOTER {
            true
        } else if magic == PARQUET_MAGIC {
            false
        } else {
            return Err(general_err!("Invalid Parquet file. Corrupt footer"));
        };
        // get the metadata length from the footer
        let metadata_len = u32::from_le_bytes(slice[..4].try_into().unwrap());
        Ok(FooterTail {
            // u32 won't be larger than usize in most cases
            metadata_length: metadata_len as usize,
            encrypted_footer,
        })
    }

    /// Decodes the Parquet footer, returning the metadata length in bytes
    #[deprecated(note = "use decode_footer_tail instead")]
    pub fn decode_footer(slice: &[u8; FOOTER_SIZE]) -> Result<usize> {
        Self::decode_footer_tail(slice).map(|f| f.metadata_length)
    }

    /// Decodes [`ParquetMetaData`] from the provided bytes.
    ///
    /// Typically, this is used to decode the metadata from the end of a parquet
    /// file. The format of `buf` is the Thrift compact binary protocol, as specified
    /// by the [Parquet Spec].
    ///
    /// This method handles using either `decode_metadata` or
    /// `decode_metadata_with_encryption` depending on whether the encryption
    /// feature is enabled.
    ///
    /// [Parquet Spec]: https://github.com/apache/parquet-format#metadata
    pub(crate) fn decode_footer_metadata(
        &self,
        buf: &[u8],
        footer_tail: &FooterTail,
    ) -> Result<ParquetMetaData> {
        #[cfg(feature = "encryption")]
        let result = Self::decode_metadata_with_encryption(
            buf,
            footer_tail.is_encrypted_footer(),
            self.file_decryption_properties.as_ref(),
        );
        #[cfg(not(feature = "encryption"))]
        let result = {
            if footer_tail.is_encrypted_footer() {
                Err(general_err!(
                    "Parquet file has an encrypted footer but the encryption feature is disabled"
                ))
            } else {
                Self::decode_metadata(buf)
            }
        };
        result
    }

    /// Decodes [`ParquetMetaData`] from the provided bytes, handling metadata that may be encrypted.
    ///
    /// Typically this is used to decode the metadata from the end of a parquet
    /// file. The format of `buf` is the Thrift compact binary protocol, as specified
    /// by the [Parquet Spec]. Buffer can be encrypted with AES GCM or AES CTR
    /// ciphers as specfied in the [Parquet Encryption Spec].
    ///
    /// [Parquet Spec]: https://github.com/apache/parquet-format#metadata
    /// [Parquet Encryption Spec]: https://parquet.apache.org/docs/file-format/data-pages/encryption/
    #[cfg(feature = "encryption")]
    fn decode_metadata_with_encryption(
        buf: &[u8],
        encrypted_footer: bool,
        file_decryption_properties: Option<&FileDecryptionProperties>,
    ) -> Result<ParquetMetaData> {
        let mut prot = TCompactSliceInputProtocol::new(buf);
        let mut file_decryptor = None;
        let decrypted_fmd_buf;

        if encrypted_footer {
            if let Some(file_decryption_properties) = file_decryption_properties {
                let t_file_crypto_metadata: TFileCryptoMetaData =
                    TFileCryptoMetaData::read_from_in_protocol(&mut prot)
                        .map_err(|e| general_err!("Could not parse crypto metadata: {}", e))?;
                let supply_aad_prefix = match &t_file_crypto_metadata.encryption_algorithm {
                    EncryptionAlgorithm::AESGCMV1(algo) => algo.supply_aad_prefix,
                    _ => Some(false),
                }
                .unwrap_or(false);
                if supply_aad_prefix && file_decryption_properties.aad_prefix().is_none() {
                    return Err(general_err!(
                        "Parquet file was encrypted with an AAD prefix that is not stored in the file, \
                        but no AAD prefix was provided in the file decryption properties"
                    ));
                }
                let decryptor = get_file_decryptor(
                    t_file_crypto_metadata.encryption_algorithm,
                    t_file_crypto_metadata.key_metadata.as_deref(),
                    file_decryption_properties,
                )?;
                let footer_decryptor = decryptor.get_footer_decryptor();
                let aad_footer = create_footer_aad(decryptor.file_aad())?;

                decrypted_fmd_buf = footer_decryptor?
                    .decrypt(prot.as_slice().as_ref(), aad_footer.as_ref())
                    .map_err(|_| {
                        general_err!(
                            "Provided footer key and AAD were unable to decrypt parquet footer"
                        )
                    })?;
                prot = TCompactSliceInputProtocol::new(decrypted_fmd_buf.as_ref());

                file_decryptor = Some(decryptor);
            } else {
                return Err(general_err!("Parquet file has an encrypted footer but decryption properties were not provided"));
            }
        }

        let t_file_metadata: TFileMetaData = TFileMetaData::read_from_in_protocol(&mut prot)
            .map_err(|e| general_err!("Could not parse metadata: {}", e))?;
        let schema = types::from_thrift(&t_file_metadata.schema)?;
        let schema_descr = Arc::new(SchemaDescriptor::new(schema));

        if let (Some(algo), Some(file_decryption_properties)) = (
            t_file_metadata.encryption_algorithm,
            file_decryption_properties,
        ) {
            // File has a plaintext footer but encryption algorithm is set
            let file_decryptor_value = get_file_decryptor(
                algo,
                t_file_metadata.footer_signing_key_metadata.as_deref(),
                file_decryption_properties,
            )?;
            if file_decryption_properties.check_plaintext_footer_integrity() && !encrypted_footer {
                file_decryptor_value.verify_plaintext_footer_signature(buf)?;
            }
            file_decryptor = Some(file_decryptor_value);
        }

        let mut row_groups = Vec::new();
        for rg in t_file_metadata.row_groups {
            let r = RowGroupMetaData::from_encrypted_thrift(
                schema_descr.clone(),
                rg,
                file_decryptor.as_ref(),
            )?;
            row_groups.push(r);
        }
        let column_orders =
            Self::parse_column_orders(t_file_metadata.column_orders, &schema_descr)?;

        let file_metadata = FileMetaData::new(
            t_file_metadata.version,
            t_file_metadata.num_rows,
            t_file_metadata.created_by,
            t_file_metadata.key_value_metadata,
            schema_descr,
            column_orders,
        );
        let mut metadata = ParquetMetaData::new(file_metadata, row_groups);

        metadata.with_file_decryptor(file_decryptor);

        Ok(metadata)
    }

    /// Decodes [`ParquetMetaData`] from the provided bytes.
    ///
    /// Typically this is used to decode the metadata from the end of a parquet
    /// file. The format of `buf` is the Thrift compact binary protocol, as specified
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
        let column_orders =
            Self::parse_column_orders(t_file_metadata.column_orders, &schema_descr)?;

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
    ) -> Result<Option<Vec<ColumnOrder>>> {
        match t_column_orders {
            Some(orders) => {
                // Should always be the case
                if orders.len() != schema_descr.num_columns() {
                    return Err(general_err!("Column order length mismatch"));
                };
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
                Ok(Some(res))
            }
            None => Ok(None),
        }
    }
}

#[cfg(feature = "encryption")]
fn get_file_decryptor(
    encryption_algorithm: EncryptionAlgorithm,
    footer_key_metadata: Option<&[u8]>,
    file_decryption_properties: &FileDecryptionProperties,
) -> Result<FileDecryptor> {
    match encryption_algorithm {
        EncryptionAlgorithm::AESGCMV1(algo) => {
            let aad_file_unique = algo
                .aad_file_unique
                .ok_or_else(|| general_err!("AAD unique file identifier is not set"))?;
            let aad_prefix = if let Some(aad_prefix) = file_decryption_properties.aad_prefix() {
                aad_prefix.clone()
            } else {
                algo.aad_prefix.unwrap_or_default()
            };

            FileDecryptor::new(
                file_decryption_properties,
                footer_key_metadata,
                aad_file_unique,
                aad_prefix,
            )
        }
        EncryptionAlgorithm::AESGCMCTRV1(_) => Err(nyi_err!(
            "The AES_GCM_CTR_V1 encryption algorithm is not yet supported"
        )),
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
        let err = ParquetMetaDataReader::new()
            .parse_metadata(&test_file)
            .unwrap_err();
        assert!(matches!(err, ParquetError::NeedMoreData(8)));
    }

    #[test]
    fn test_parse_metadata_corrupt_footer() {
        let data = Bytes::from(vec![1, 2, 3, 4, 5, 6, 7, 8]);
        let reader_result = ParquetMetaDataReader::new().parse_metadata(&data);
        assert_eq!(
            reader_result.unwrap_err().to_string(),
            "Parquet error: Invalid Parquet file. Corrupt footer"
        );
    }

    #[test]
    fn test_parse_metadata_invalid_start() {
        let test_file = Bytes::from(vec![255, 0, 0, 0, b'P', b'A', b'R', b'1']);
        let err = ParquetMetaDataReader::new()
            .parse_metadata(&test_file)
            .unwrap_err();
        assert!(matches!(err, ParquetError::NeedMoreData(263)));
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
            ParquetMetaDataReader::parse_column_orders(t_column_orders, &schema_descr).unwrap(),
            Some(vec![
                ColumnOrder::TYPE_DEFINED_ORDER(SortOrder::SIGNED),
                ColumnOrder::TYPE_DEFINED_ORDER(SortOrder::SIGNED)
            ])
        );

        // Test when no column orders are defined.
        assert_eq!(
            ParquetMetaDataReader::parse_column_orders(None, &schema_descr).unwrap(),
            None
        );
    }

    #[test]
    fn test_metadata_column_orders_len_mismatch() {
        let schema = SchemaType::group_type_builder("schema").build().unwrap();
        let schema_descr = SchemaDescriptor::new(Arc::new(schema));

        let t_column_orders = Some(vec![TColumnOrder::TYPEORDER(TypeDefinedOrder::new())]);

        let res = ParquetMetaDataReader::parse_column_orders(t_column_orders, &schema_descr);
        assert!(res.is_err());
        assert!(format!("{:?}", res.unwrap_err()).contains("Column order length mismatch"));
    }

    #[test]
    fn test_try_parse() {
        let file = get_test_file("alltypes_tiny_pages.parquet");
        let len = file.len();

        let mut reader = ParquetMetaDataReader::new().with_page_indexes(true);

        let bytes_for_range = |range: Range<u64>| {
            file.get_bytes(range.start, (range.end - range.start).try_into().unwrap())
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
            ParquetError::NeedMoreData(needed) => {
                let bytes = bytes_for_range(len - needed as u64..len);
                reader.try_parse_sized(&bytes, len).unwrap();
                let metadata = reader.finish().unwrap();
                assert!(metadata.column_index.is_some());
                assert!(metadata.offset_index.is_some());
            }
            _ => panic!("unexpected error"),
        };

        // not enough for file metadata, but keep trying until page indexes are read
        let mut reader = ParquetMetaDataReader::new().with_page_indexes(true);
        let mut bytes = bytes_for_range(452505..len);
        loop {
            match reader.try_parse_sized(&bytes, len) {
                Ok(_) => break,
                Err(ParquetError::NeedMoreData(needed)) => {
                    bytes = bytes_for_range(len - needed as u64..len);
                    if reader.has_metadata() {
                        reader.read_page_indexes_sized(&bytes, len).unwrap();
                        break;
                    }
                }
                _ => panic!("unexpected error"),
            }
        }
        let metadata = reader.finish().unwrap();
        assert!(metadata.column_index.is_some());
        assert!(metadata.offset_index.is_some());

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
            ParquetError::NeedMoreData(needed) => {
                let bytes = bytes_for_range(len - needed as u64..len);
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

#[cfg(all(feature = "async", feature = "arrow", test))]
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

    use crate::file::reader::Length;
    use crate::util::test_common::file_util::get_test_file;

    struct MetadataFetchFn<F>(F);

    impl<F, Fut> MetadataFetch for MetadataFetchFn<F>
    where
        F: FnMut(Range<u64>) -> Fut + Send,
        Fut: Future<Output = Result<Bytes>> + Send,
    {
        fn fetch(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes>> {
            async move { self.0(range).await }.boxed()
        }
    }

    struct MetadataSuffixFetchFn<F1, F2>(F1, F2);

    impl<F1, Fut, F2> MetadataFetch for MetadataSuffixFetchFn<F1, F2>
    where
        F1: FnMut(Range<u64>) -> Fut + Send,
        Fut: Future<Output = Result<Bytes>> + Send,
        F2: Send,
    {
        fn fetch(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes>> {
            async move { self.0(range).await }.boxed()
        }
    }

    impl<F1, Fut, F2> MetadataSuffixFetch for MetadataSuffixFetchFn<F1, F2>
    where
        F1: FnMut(Range<u64>) -> Fut + Send,
        F2: FnMut(usize) -> Fut + Send,
        Fut: Future<Output = Result<Bytes>> + Send,
    {
        fn fetch_suffix(&mut self, suffix: usize) -> BoxFuture<'_, Result<Bytes>> {
            async move { self.1(suffix).await }.boxed()
        }
    }

    fn read_range(file: &mut File, range: Range<u64>) -> Result<Bytes> {
        file.seek(SeekFrom::Start(range.start as _))?;
        let len = range.end - range.start;
        let mut buf = Vec::with_capacity(len.try_into().unwrap());
        file.take(len as _).read_to_end(&mut buf)?;
        Ok(buf.into())
    }

    fn read_suffix(file: &mut File, suffix: usize) -> Result<Bytes> {
        let file_len = file.len();
        // Don't seek before beginning of file
        file.seek(SeekFrom::End(0 - suffix.min(file_len as _) as i64))?;
        let mut buf = Vec::with_capacity(suffix);
        file.take(suffix as _).read_to_end(&mut buf)?;
        Ok(buf.into())
    }

    #[tokio::test]
    async fn test_simple() {
        let mut file = get_test_file("nulls.snappy.parquet");
        let len = file.len();

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
    async fn test_suffix() {
        let mut file = get_test_file("nulls.snappy.parquet");
        let mut file2 = file.try_clone().unwrap();

        let expected = ParquetMetaDataReader::new()
            .parse_and_finish(&file)
            .unwrap();
        let expected = expected.file_metadata().schema();
        let fetch_count = AtomicUsize::new(0);
        let suffix_fetch_count = AtomicUsize::new(0);

        let mut fetch = |range| {
            fetch_count.fetch_add(1, Ordering::SeqCst);
            futures::future::ready(read_range(&mut file, range))
        };
        let mut suffix_fetch = |suffix| {
            suffix_fetch_count.fetch_add(1, Ordering::SeqCst);
            futures::future::ready(read_suffix(&mut file2, suffix))
        };

        let input = MetadataSuffixFetchFn(&mut fetch, &mut suffix_fetch);
        let actual = ParquetMetaDataReader::new()
            .load_via_suffix_and_finish(input)
            .await
            .unwrap();
        assert_eq!(actual.file_metadata().schema(), expected);
        assert_eq!(fetch_count.load(Ordering::SeqCst), 0);
        assert_eq!(suffix_fetch_count.load(Ordering::SeqCst), 2);

        // Metadata hint too small - below footer size
        fetch_count.store(0, Ordering::SeqCst);
        suffix_fetch_count.store(0, Ordering::SeqCst);
        let input = MetadataSuffixFetchFn(&mut fetch, &mut suffix_fetch);
        let actual = ParquetMetaDataReader::new()
            .with_prefetch_hint(Some(7))
            .load_via_suffix_and_finish(input)
            .await
            .unwrap();
        assert_eq!(actual.file_metadata().schema(), expected);
        assert_eq!(fetch_count.load(Ordering::SeqCst), 0);
        assert_eq!(suffix_fetch_count.load(Ordering::SeqCst), 2);

        // Metadata hint too small
        fetch_count.store(0, Ordering::SeqCst);
        suffix_fetch_count.store(0, Ordering::SeqCst);
        let input = MetadataSuffixFetchFn(&mut fetch, &mut suffix_fetch);
        let actual = ParquetMetaDataReader::new()
            .with_prefetch_hint(Some(10))
            .load_via_suffix_and_finish(input)
            .await
            .unwrap();
        assert_eq!(actual.file_metadata().schema(), expected);
        assert_eq!(fetch_count.load(Ordering::SeqCst), 0);
        assert_eq!(suffix_fetch_count.load(Ordering::SeqCst), 2);

        dbg!("test");
        // Metadata hint too large
        fetch_count.store(0, Ordering::SeqCst);
        suffix_fetch_count.store(0, Ordering::SeqCst);
        let input = MetadataSuffixFetchFn(&mut fetch, &mut suffix_fetch);
        let actual = ParquetMetaDataReader::new()
            .with_prefetch_hint(Some(500))
            .load_via_suffix_and_finish(input)
            .await
            .unwrap();
        assert_eq!(actual.file_metadata().schema(), expected);
        assert_eq!(fetch_count.load(Ordering::SeqCst), 0);
        assert_eq!(suffix_fetch_count.load(Ordering::SeqCst), 1);

        // Metadata hint exactly correct
        fetch_count.store(0, Ordering::SeqCst);
        suffix_fetch_count.store(0, Ordering::SeqCst);
        let input = MetadataSuffixFetchFn(&mut fetch, &mut suffix_fetch);
        let actual = ParquetMetaDataReader::new()
            .with_prefetch_hint(Some(428))
            .load_via_suffix_and_finish(input)
            .await
            .unwrap();
        assert_eq!(actual.file_metadata().schema(), expected);
        assert_eq!(fetch_count.load(Ordering::SeqCst), 0);
        assert_eq!(suffix_fetch_count.load(Ordering::SeqCst), 1);
    }

    #[cfg(feature = "encryption")]
    #[tokio::test]
    async fn test_suffix_with_encryption() {
        let mut file = get_test_file("uniform_encryption.parquet.encrypted");
        let mut file2 = file.try_clone().unwrap();

        let mut fetch = |range| futures::future::ready(read_range(&mut file, range));
        let mut suffix_fetch = |suffix| futures::future::ready(read_suffix(&mut file2, suffix));

        let input = MetadataSuffixFetchFn(&mut fetch, &mut suffix_fetch);

        let key_code: &[u8] = "0123456789012345".as_bytes();
        let decryption_properties = FileDecryptionProperties::builder(key_code.to_vec())
            .build()
            .unwrap();

        // just make sure the metadata is properly decrypted and read
        let expected = ParquetMetaDataReader::new()
            .with_decryption_properties(Some(&decryption_properties))
            .load_via_suffix_and_finish(input)
            .await
            .unwrap();
        assert_eq!(expected.num_row_groups(), 1);
    }

    #[tokio::test]
    async fn test_page_index() {
        let mut file = get_test_file("alltypes_tiny_pages.parquet");
        let len = file.len();
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

        // Prefetch more than enough but less than the entire file
        fetch_count.store(0, Ordering::SeqCst);
        let f = MetadataFetchFn(&mut fetch);
        let metadata = ParquetMetaDataReader::new()
            .with_page_indexes(true)
            .with_prefetch_hint(Some((len - 1000) as usize)) // prefetch entire file
            .load_and_finish(f, len)
            .await
            .unwrap();
        assert_eq!(fetch_count.load(Ordering::SeqCst), 1);
        assert!(metadata.offset_index().is_some() && metadata.column_index().is_some());

        // Prefetch the entire file
        fetch_count.store(0, Ordering::SeqCst);
        let f = MetadataFetchFn(&mut fetch);
        let metadata = ParquetMetaDataReader::new()
            .with_page_indexes(true)
            .with_prefetch_hint(Some(len as usize)) // prefetch entire file
            .load_and_finish(f, len)
            .await
            .unwrap();
        assert_eq!(fetch_count.load(Ordering::SeqCst), 1);
        assert!(metadata.offset_index().is_some() && metadata.column_index().is_some());

        // Prefetch more than the entire file
        fetch_count.store(0, Ordering::SeqCst);
        let f = MetadataFetchFn(&mut fetch);
        let metadata = ParquetMetaDataReader::new()
            .with_page_indexes(true)
            .with_prefetch_hint(Some((len + 1000) as usize)) // prefetch entire file
            .load_and_finish(f, len)
            .await
            .unwrap();
        assert_eq!(fetch_count.load(Ordering::SeqCst), 1);
        assert!(metadata.offset_index().is_some() && metadata.column_index().is_some());
    }
}
