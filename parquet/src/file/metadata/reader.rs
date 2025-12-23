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

#[cfg(feature = "encryption")]
use crate::encryption::decrypt::FileDecryptionProperties;
use crate::errors::{ParquetError, Result};
use crate::file::FOOTER_SIZE;
use crate::file::metadata::parser::decode_metadata;
use crate::file::metadata::thrift::parquet_schema_from_bytes;
use crate::file::metadata::{
    FooterTail, ParquetMetaData, ParquetMetaDataOptions, ParquetMetaDataPushDecoder,
};
use crate::file::reader::ChunkReader;
use crate::schema::types::SchemaDescriptor;
use bytes::Bytes;
use std::sync::Arc;
use std::{io::Read, ops::Range};

use crate::DecodeResult;
#[cfg(all(feature = "async", feature = "arrow"))]
use crate::arrow::async_reader::{MetadataFetch, MetadataSuffixFetch};

/// Reads [`ParquetMetaData`] from a byte stream, with either synchronous or
/// asynchronous I/O.
///
/// There are two flavors of APIs:
/// * Synchronous: [`Self::try_parse()`], [`Self::try_parse_sized()`], [`Self::parse_and_finish()`], etc.
/// * Asynchronous (requires `async` and `arrow` features): [`Self::try_load()`], etc
///
///  See the [`ParquetMetaDataPushDecoder`] for an API that does not require I/O.
///
/// # Format Notes
///
/// Parquet metadata is not necessarily contiguous in a Parquet file: a portion is stored
/// in the footer (the last bytes of the file), but other portions (such as the
/// PageIndex) can be stored elsewhere.
/// See [`crate::file::metadata::ParquetMetaDataWriter#output-format`] for more details of
/// Parquet metadata.
///
/// This reader handles reading the footer as well as the non contiguous parts
/// of the metadata (`PageIndex` and `ColumnIndex`). It does not handle reading Bloom Filters.
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
#[derive(Default, Debug)]
pub struct ParquetMetaDataReader {
    metadata: Option<ParquetMetaData>,
    column_index: PageIndexPolicy,
    offset_index: PageIndexPolicy,
    prefetch_hint: Option<usize>,
    metadata_options: Option<Arc<ParquetMetaDataOptions>>,
    // Size of the serialized thrift metadata plus the 8 byte footer. Only set if
    // `self.parse_metadata` is called.
    metadata_size: Option<usize>,
    #[cfg(feature = "encryption")]
    file_decryption_properties: Option<Arc<FileDecryptionProperties>>,
}

/// Describes the policy for reading page indexes
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PageIndexPolicy {
    /// Do not read the page index.
    #[default]
    Skip,
    /// Read the page index if it exists, otherwise do not error.
    Optional,
    /// Require the page index to exist, and error if it does not.
    Required,
}

impl From<bool> for PageIndexPolicy {
    fn from(value: bool) -> Self {
        match value {
            true => Self::Required,
            false => Self::Skip,
        }
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
    /// "[Parquet page index]: Layout to Support Page Skipping".
    ///
    /// [Parquet page index]: https://github.com/apache/parquet-format/blob/master/PageIndex.md
    #[deprecated(since = "56.1.0", note = "Use `with_page_index_policy` instead")]
    pub fn with_page_indexes(self, val: bool) -> Self {
        let policy = PageIndexPolicy::from(val);
        self.with_column_index_policy(policy)
            .with_offset_index_policy(policy)
    }

    /// Enable or disable reading the Parquet [ColumnIndex] structure.
    ///
    /// [ColumnIndex]:  https://github.com/apache/parquet-format/blob/master/PageIndex.md
    #[deprecated(since = "56.1.0", note = "Use `with_column_index_policy` instead")]
    pub fn with_column_indexes(self, val: bool) -> Self {
        let policy = PageIndexPolicy::from(val);
        self.with_column_index_policy(policy)
    }

    /// Enable or disable reading the Parquet [OffsetIndex] structure.
    ///
    /// [OffsetIndex]:  https://github.com/apache/parquet-format/blob/master/PageIndex.md
    #[deprecated(since = "56.1.0", note = "Use `with_offset_index_policy` instead")]
    pub fn with_offset_indexes(self, val: bool) -> Self {
        let policy = PageIndexPolicy::from(val);
        self.with_offset_index_policy(policy)
    }

    /// Sets the [`PageIndexPolicy`] for the column and offset indexes
    pub fn with_page_index_policy(self, policy: PageIndexPolicy) -> Self {
        self.with_column_index_policy(policy)
            .with_offset_index_policy(policy)
    }

    /// Sets the [`PageIndexPolicy`] for the column index
    pub fn with_column_index_policy(mut self, policy: PageIndexPolicy) -> Self {
        self.column_index = policy;
        self
    }

    /// Sets the [`PageIndexPolicy`] for the offset index
    pub fn with_offset_index_policy(mut self, policy: PageIndexPolicy) -> Self {
        self.offset_index = policy;
        self
    }

    /// Sets the [`ParquetMetaDataOptions`] to use when decoding
    pub fn with_metadata_options(mut self, options: Option<ParquetMetaDataOptions>) -> Self {
        self.metadata_options = options.map(Arc::new);
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
        properties: Option<std::sync::Arc<FileDecryptionProperties>>,
    ) -> Self {
        self.file_decryption_properties = properties;
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
    ///                 reader.read_page_indexes_sized(&bytes, len).unwrap();
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
        if self.column_index == PageIndexPolicy::Skip && self.offset_index == PageIndexPolicy::Skip
        {
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
        let Some(metadata) = self.metadata.take() else {
            return Err(general_err!(
                "Tried to read page indexes without ParquetMetaData metadata"
            ));
        };

        let push_decoder = ParquetMetaDataPushDecoder::try_new_with_metadata(file_size, metadata)?
            .with_offset_index_policy(self.offset_index)
            .with_column_index_policy(self.column_index)
            .with_metadata_options(self.metadata_options.clone());
        let mut push_decoder = self.prepare_push_decoder(push_decoder);

        // Get bounds needed for page indexes (if any are present in the file).
        let range = match needs_index_data(&mut push_decoder)? {
            NeedsIndexData::No(metadata) => {
                self.metadata = Some(metadata);
                return Ok(());
            }
            NeedsIndexData::Yes(range) => range,
        };

        // Check to see if needed range is within `file_range`. Checking `range.end` seems
        // redundant, but it guards against `range_for_page_index()` returning garbage.
        let file_range = file_size.saturating_sub(reader.len())..file_size;
        if !(file_range.contains(&range.start) && file_range.contains(&range.end)) {
            // Requested range starts beyond EOF
            if range.end > file_size {
                return Err(eof_err!(
                    "Parquet file too small. Range {range:?} is beyond file bounds {file_size}",
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
                    "Parquet file too small. Page index range {range:?} overlaps with file metadata {metadata_range:?}",
                ));
            }
        }

        // add the needed ranges to the decoder
        let bytes_needed = usize::try_from(range.end - range.start)?;
        let bytes = reader.get_bytes(range.start - file_range.start, bytes_needed)?;

        push_decoder.push_range(range, bytes)?;
        let metadata = parse_index_data(&mut push_decoder)?;
        self.metadata = Some(metadata);

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
        if self.column_index == PageIndexPolicy::Skip && self.offset_index == PageIndexPolicy::Skip
        {
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
        if self.column_index == PageIndexPolicy::Skip && self.offset_index == PageIndexPolicy::Skip
        {
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
        let Some(metadata) = self.metadata.take() else {
            return Err(general_err!("Footer metadata is not present"));
        };

        // in this case we don't actually know what the file size is, so just use u64::MAX
        // this is ok since the offsets in the metadata are always valid
        let file_size = u64::MAX;
        let push_decoder = ParquetMetaDataPushDecoder::try_new_with_metadata(file_size, metadata)?
            .with_offset_index_policy(self.offset_index)
            .with_column_index_policy(self.column_index)
            .with_metadata_options(self.metadata_options.clone());
        let mut push_decoder = self.prepare_push_decoder(push_decoder);

        // Get bounds needed for page indexes (if any are present in the file).
        let range = match needs_index_data(&mut push_decoder)? {
            NeedsIndexData::No(metadata) => {
                self.metadata = Some(metadata);
                return Ok(());
            }
            NeedsIndexData::Yes(range) => range,
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
        push_decoder.push_range(range.clone(), bytes)?;
        let metadata = parse_index_data(&mut push_decoder)?;
        self.metadata = Some(metadata);
        Ok(())
    }

    // One-shot parse of footer.
    // Side effect: this will set `self.metadata_size`
    fn parse_metadata<R: ChunkReader>(&mut self, chunk_reader: &R) -> Result<ParquetMetaData> {
        // check file is large enough to hold footer
        let file_size = chunk_reader.len();
        if file_size < (FOOTER_SIZE as u64) {
            return Err(ParquetError::NeedMoreData(FOOTER_SIZE));
        }

        let mut footer = [0_u8; FOOTER_SIZE];
        chunk_reader
            .get_read(file_size - FOOTER_SIZE as u64)?
            .read_exact(&mut footer)?;

        let footer = FooterTail::try_new(&footer)?;
        let metadata_len = footer.metadata_length();
        let footer_metadata_len = FOOTER_SIZE + metadata_len;
        self.metadata_size = Some(footer_metadata_len);

        if footer_metadata_len as u64 > file_size {
            return Err(ParquetError::NeedMoreData(footer_metadata_len));
        }

        let start = file_size - footer_metadata_len as u64;
        let bytes = chunk_reader.get_bytes(start, metadata_len)?;
        self.decode_footer_metadata(bytes, file_size, footer)
    }

    /// Size of the serialized thrift metadata plus the 8 byte footer. Only set if
    /// `self.parse_metadata` is called.
    pub fn metadata_size(&self) -> Option<usize> {
        self.metadata_size
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

        let footer = FooterTail::try_new(&footer)?;
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
            Ok((self.decode_footer_metadata(meta, file_size, footer)?, None))
        } else {
            let metadata_start = (file_size - (length + FOOTER_SIZE) as u64 - footer_start)
                .try_into()
                .expect("metadata length should never be larger than u32");
            let slice = suffix.slice(metadata_start..suffix_len - FOOTER_SIZE);
            Ok((
                self.decode_footer_metadata(slice, file_size, footer)?,
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

        let footer = FooterTail::try_new(&footer)?;
        let length = footer.metadata_length();
        // fake file size as we are only parsing the footer metadata here
        // (cant be parsing page indexes without the full file size)
        let file_size = (length + FOOTER_SIZE) as u64;

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

            // need to slice off the footer or decryption fails
            let meta = meta.slice(0..length);
            Ok((self.decode_footer_metadata(meta, file_size, footer)?, None))
        } else {
            let metadata_start = suffix_len - metadata_offset;
            let slice = suffix.slice(metadata_start..suffix_len - FOOTER_SIZE);
            Ok((
                self.decode_footer_metadata(slice, file_size, footer)?,
                Some((0, suffix.slice(..metadata_start))),
            ))
        }
    }

    /// Decodes a [`FooterTail`] from the provided 8-byte slice.
    #[deprecated(since = "57.0.0", note = "Use FooterTail::try_from instead")]
    pub fn decode_footer_tail(slice: &[u8; FOOTER_SIZE]) -> Result<FooterTail> {
        FooterTail::try_new(slice)
    }

    /// Decodes the Parquet footer, returning the metadata length in bytes
    #[deprecated(since = "54.3.0", note = "Use decode_footer_tail instead")]
    pub fn decode_footer(slice: &[u8; FOOTER_SIZE]) -> Result<usize> {
        FooterTail::try_new(slice).map(|f| f.metadata_length())
    }

    /// Decodes [`ParquetMetaData`] from the provided bytes.
    ///
    /// Typically, this is used to decode the metadata from the end of a parquet
    /// file. The format of `buf` is the Thrift compact binary protocol, as specified
    /// by the [Parquet Spec].
    ///
    /// It does **NOT** include the 8-byte footer.
    ///
    /// This method handles using either `decode_metadata` or
    /// `decode_metadata_with_encryption` depending on whether the encryption
    /// feature is enabled.
    ///
    /// [Parquet Spec]: https://github.com/apache/parquet-format#metadata
    pub(crate) fn decode_footer_metadata(
        &self,
        buf: Bytes,
        file_size: u64,
        footer_tail: FooterTail,
    ) -> Result<ParquetMetaData> {
        // The push decoder expects the metadata to be at the end of the file
        // (... data ...) + (metadata) + (footer)
        // so we need to provide the starting offset of the metadata
        // within the file.
        let ending_offset = file_size.checked_sub(FOOTER_SIZE as u64).ok_or_else(|| {
            general_err!(
                "file size {file_size} is smaller than footer size {}",
                FOOTER_SIZE
            )
        })?;

        let starting_offset = ending_offset.checked_sub(buf.len() as u64).ok_or_else(|| {
            general_err!(
                "file size {file_size} is smaller than buffer size {} + footer size {}",
                buf.len(),
                FOOTER_SIZE
            )
        })?;

        let range = starting_offset..ending_offset;

        let push_decoder =
            ParquetMetaDataPushDecoder::try_new_with_footer_tail(file_size, footer_tail)?
                // NOTE: DO NOT enable page indexes here, they are handled separately
                .with_page_index_policy(PageIndexPolicy::Skip)
                .with_metadata_options(self.metadata_options.clone());

        let mut push_decoder = self.prepare_push_decoder(push_decoder);
        push_decoder.push_range(range, buf)?;
        match push_decoder.try_decode()? {
            DecodeResult::Data(metadata) => Ok(metadata),
            DecodeResult::Finished => Err(general_err!(
                "could not parse parquet metadata -- previously finished"
            )),
            DecodeResult::NeedsData(ranges) => Err(general_err!(
                "could not parse parquet metadata, needs ranges {:?}",
                ranges
            )),
        }
    }

    /// Prepares a push decoder and runs it to decode the metadata.
    #[cfg(feature = "encryption")]
    fn prepare_push_decoder(
        &self,
        push_decoder: ParquetMetaDataPushDecoder,
    ) -> ParquetMetaDataPushDecoder {
        push_decoder.with_file_decryption_properties(
            self.file_decryption_properties
                .as_ref()
                .map(std::sync::Arc::clone),
        )
    }
    #[cfg(not(feature = "encryption"))]
    fn prepare_push_decoder(
        &self,
        push_decoder: ParquetMetaDataPushDecoder,
    ) -> ParquetMetaDataPushDecoder {
        push_decoder
    }

    /// Decodes [`ParquetMetaData`] from the provided bytes.
    ///
    /// Typically this is used to decode the metadata from the end of a parquet
    /// file. The format of `buf` is the Thrift compact binary protocol, as specified
    /// by the [Parquet Spec].
    ///
    /// [Parquet Spec]: https://github.com/apache/parquet-format#metadata
    pub fn decode_metadata(buf: &[u8]) -> Result<ParquetMetaData> {
        decode_metadata(buf, None)
    }

    /// Decodes [`ParquetMetaData`] from the provided bytes.
    ///
    /// Like [`Self::decode_metadata`] but this also accepts
    /// metadata parsing options.
    pub fn decode_metadata_with_options(
        buf: &[u8],
        options: Option<&ParquetMetaDataOptions>,
    ) -> Result<ParquetMetaData> {
        decode_metadata(buf, options)
    }

    /// Decodes the schema from the Parquet footer in `buf`. Returned as
    /// a [`SchemaDescriptor`].
    pub fn decode_schema(buf: &[u8]) -> Result<Arc<SchemaDescriptor>> {
        Ok(Arc::new(parquet_schema_from_bytes(buf)?))
    }
}

/// The bounds needed to read page indexes
// this is an internal enum, so it is ok to allow differences in enum size
#[allow(clippy::large_enum_variant)]
enum NeedsIndexData {
    /// no additional data is needed (e.g. the indexes weren't requested)
    No(ParquetMetaData),
    /// Additional data is needed, with the range that are required
    Yes(Range<u64>),
}

/// Determines a single combined range of bytes needed to read the page indexes,
/// or returns the metadata if no additional data is needed (e.g. if no page indexes are requested)
fn needs_index_data(push_decoder: &mut ParquetMetaDataPushDecoder) -> Result<NeedsIndexData> {
    match push_decoder.try_decode()? {
        DecodeResult::NeedsData(ranges) => {
            let range = ranges
                .into_iter()
                .reduce(|a, b| a.start.min(b.start)..a.end.max(b.end))
                .ok_or_else(|| general_err!("Internal error: no ranges provided"))?;
            Ok(NeedsIndexData::Yes(range))
        }
        DecodeResult::Data(metadata) => Ok(NeedsIndexData::No(metadata)),
        DecodeResult::Finished => Err(general_err!("Internal error: decoder was finished")),
    }
}

/// Given a push decoder that has had the needed ranges pushed to it,
/// attempt to decode indexes and return the updated metadata.
fn parse_index_data(push_decoder: &mut ParquetMetaDataPushDecoder) -> Result<ParquetMetaData> {
    match push_decoder.try_decode()? {
        DecodeResult::NeedsData(_) => Err(general_err!(
            "Internal error: decoder still needs data after reading required range"
        )),
        DecodeResult::Data(metadata) => Ok(metadata),
        DecodeResult::Finished => Err(general_err!("Internal error: decoder was finished")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::reader::Length;
    use crate::util::test_common::file_util::get_test_file;
    use std::ops::Range;

    #[test]
    fn test_parse_metadata_size_smaller_than_footer() {
        let test_file = tempfile::tempfile().unwrap();
        let err = ParquetMetaDataReader::new()
            .parse_metadata(&test_file)
            .unwrap_err();
        assert!(matches!(err, ParquetError::NeedMoreData(FOOTER_SIZE)));
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
    #[allow(deprecated)]
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

    use arrow::{array::Int32Array, datatypes::DataType};
    use arrow_array::RecordBatch;
    use arrow_schema::{Field, Schema};
    use bytes::Bytes;
    use futures::FutureExt;
    use futures::future::BoxFuture;
    use std::fs::File;
    use std::future::Future;
    use std::io::{Read, Seek, SeekFrom};
    use std::ops::Range;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::NamedTempFile;

    use crate::arrow::ArrowWriter;
    use crate::file::properties::WriterProperties;
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
            .with_decryption_properties(Some(decryption_properties))
            .load_via_suffix_and_finish(input)
            .await
            .unwrap();
        assert_eq!(expected.num_row_groups(), 1);
    }

    #[tokio::test]
    #[allow(deprecated)]
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

    fn write_parquet_file(offset_index_disabled: bool) -> Result<NamedTempFile> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )?;

        let file = NamedTempFile::new().unwrap();

        // Write properties with page index disabled
        let props = WriterProperties::builder()
            .set_offset_index_disabled(offset_index_disabled)
            .build();

        let mut writer = ArrowWriter::try_new(file.reopen()?, schema, Some(props))?;
        writer.write(&batch)?;
        writer.close()?;

        Ok(file)
    }

    fn read_and_check(file: &File, policy: PageIndexPolicy) -> Result<ParquetMetaData> {
        let mut reader = ParquetMetaDataReader::new().with_page_index_policy(policy);
        reader.try_parse(file)?;
        reader.finish()
    }

    #[test]
    fn test_page_index_policy() {
        // With page index
        let f = write_parquet_file(false).unwrap();
        read_and_check(f.as_file(), PageIndexPolicy::Required).unwrap();
        read_and_check(f.as_file(), PageIndexPolicy::Optional).unwrap();
        read_and_check(f.as_file(), PageIndexPolicy::Skip).unwrap();

        // Without page index
        let f = write_parquet_file(true).unwrap();
        let res = read_and_check(f.as_file(), PageIndexPolicy::Required);
        assert!(matches!(
            res,
            Err(ParquetError::General(e)) if e == "missing offset index"
        ));
        read_and_check(f.as_file(), PageIndexPolicy::Optional).unwrap();
        read_and_check(f.as_file(), PageIndexPolicy::Skip).unwrap();
    }
}
