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

use crate::arrow::async_reader::AsyncFileReader;
use crate::errors::{ParquetError, Result};
use crate::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use crate::file::page_index::index::Index;
use crate::file::page_index::index_reader::{acc_range, decode_column_index, decode_offset_index};
use crate::file::FOOTER_SIZE;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::FutureExt;
use std::future::Future;
use std::ops::Range;

/// A data source that can be used with [`MetadataLoader`] to load [`ParquetMetaData`]
pub trait MetadataFetch {
    /// Fetches a range of bytes asynchronously
    fn fetch(&mut self, range: Range<usize>) -> BoxFuture<'_, Result<Bytes>>;
}

impl<'a, T: AsyncFileReader> MetadataFetch for &'a mut T {
    fn fetch(&mut self, range: Range<usize>) -> BoxFuture<'_, Result<Bytes>> {
        self.get_bytes(range)
    }
}

/// An asynchronous interface to load [`ParquetMetaData`] from an async source
pub struct MetadataLoader<F> {
    /// Function that fetches byte ranges asynchronously
    fetch: F,
    /// The in-progress metadata
    metadata: ParquetMetaData,
    /// The offset and bytes of remaining unparsed data
    remainder: Option<(usize, Bytes)>,
}

impl<F: MetadataFetch> MetadataLoader<F> {
    /// Create a new [`MetadataLoader`] by reading the footer information
    ///
    /// See [`fetch_parquet_metadata`] for the meaning of the individual parameters
    #[deprecated(since = "53.1.0", note = "Use ParquetMetaDataReader")]
    pub async fn load(mut fetch: F, file_size: usize, prefetch: Option<usize>) -> Result<Self> {
        if file_size < FOOTER_SIZE {
            return Err(ParquetError::EOF(format!(
                "file size of {file_size} is less than footer"
            )));
        }

        // If a size hint is provided, read more than the minimum size
        // to try and avoid a second fetch.
        let footer_start = if let Some(size_hint) = prefetch {
            // check for hint smaller than footer
            let size_hint = std::cmp::max(size_hint, FOOTER_SIZE);
            file_size.saturating_sub(size_hint)
        } else {
            file_size - FOOTER_SIZE
        };

        let suffix = fetch.fetch(footer_start..file_size).await?;
        let suffix_len = suffix.len();

        let mut footer = [0; FOOTER_SIZE];
        footer.copy_from_slice(&suffix[suffix_len - FOOTER_SIZE..suffix_len]);

        let length = ParquetMetaDataReader::decode_footer(&footer)?;

        if file_size < length + FOOTER_SIZE {
            return Err(ParquetError::EOF(format!(
                "file size of {} is less than footer + metadata {}",
                file_size,
                length + 8
            )));
        }

        // Did not fetch the entire file metadata in the initial read, need to make a second request
        let (metadata, remainder) = if length > suffix_len - FOOTER_SIZE {
            let metadata_start = file_size - length - FOOTER_SIZE;
            let meta = fetch.fetch(metadata_start..file_size - FOOTER_SIZE).await?;
            (ParquetMetaDataReader::decode_metadata(&meta)?, None)
        } else {
            let metadata_start = file_size - length - FOOTER_SIZE - footer_start;

            let slice = &suffix[metadata_start..suffix_len - FOOTER_SIZE];
            (
                ParquetMetaDataReader::decode_metadata(slice)?,
                Some((footer_start, suffix.slice(..metadata_start))),
            )
        };

        Ok(Self {
            fetch,
            metadata,
            remainder,
        })
    }

    /// Create a new [`MetadataLoader`] from an existing [`ParquetMetaData`]
    #[deprecated(since = "53.1.0", note = "Use ParquetMetaDataReader")]
    pub fn new(fetch: F, metadata: ParquetMetaData) -> Self {
        Self {
            fetch,
            metadata,
            remainder: None,
        }
    }

    /// Loads the page index, if any
    ///
    /// * `column_index`: if true will load column index
    /// * `offset_index`: if true will load offset index
    #[deprecated(since = "53.1.0", note = "Use ParquetMetaDataReader")]
    pub async fn load_page_index(&mut self, column_index: bool, offset_index: bool) -> Result<()> {
        if !column_index && !offset_index {
            return Ok(());
        }

        let mut range = None;
        for c in self.metadata.row_groups().iter().flat_map(|r| r.columns()) {
            range = acc_range(range, c.column_index_range());
            range = acc_range(range, c.offset_index_range());
        }
        let range = match range {
            None => return Ok(()),
            Some(range) => range,
        };

        let data = match &self.remainder {
            Some((remainder_start, remainder)) if *remainder_start <= range.start => {
                let offset = range.start - *remainder_start;
                remainder.slice(offset..range.end - *remainder_start + offset)
            }
            // Note: this will potentially fetch data already in remainder, this keeps things simple
            _ => self.fetch.fetch(range.start..range.end).await?,
        };

        // Sanity check
        assert_eq!(data.len(), range.end - range.start);
        let offset = range.start;

        if column_index {
            let index = self
                .metadata
                .row_groups()
                .iter()
                .map(|x| {
                    x.columns()
                        .iter()
                        .map(|c| match c.column_index_range() {
                            Some(r) => decode_column_index(
                                &data[r.start - offset..r.end - offset],
                                c.column_type(),
                            ),
                            None => Ok(Index::NONE),
                        })
                        .collect::<Result<Vec<_>>>()
                })
                .collect::<Result<Vec<_>>>()?;

            self.metadata.set_column_index(Some(index));
        }

        if offset_index {
            let index = self
                .metadata
                .row_groups()
                .iter()
                .map(|x| {
                    x.columns()
                        .iter()
                        .map(|c| match c.offset_index_range() {
                            Some(r) => decode_offset_index(&data[r.start - offset..r.end - offset]),
                            None => Err(general_err!("missing offset index")),
                        })
                        .collect::<Result<Vec<_>>>()
                })
                .collect::<Result<Vec<_>>>()?;

            self.metadata.set_offset_index(Some(index));
        }

        Ok(())
    }

    /// Returns the finished [`ParquetMetaData`]
    pub fn finish(self) -> ParquetMetaData {
        self.metadata
    }
}

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

/// Fetches parquet metadata
///
/// Parameters:
/// * fetch: an async function that can fetch byte ranges
/// * file_size: the total size of the parquet file
/// * footer_size_hint: footer prefetch size (see comments below)
///
/// The length of the parquet footer, which contains file metadata, is not
/// known up front. Therefore this function will first issue a request to read
/// the last 8 bytes to determine the footer's precise length, before
/// issuing a second request to fetch the metadata bytes
///
/// If `prefetch` is `Some`, this will read the specified number of bytes
/// in the first request, instead of 8, and only issue further requests
/// if additional bytes are needed. Providing a `prefetch` hint can therefore
/// significantly reduce the number of `fetch` requests, and consequently latency
#[deprecated(since = "53.1.0", note = "Use ParquetMetaDataReader")]
pub async fn fetch_parquet_metadata<F, Fut>(
    fetch: F,
    file_size: usize,
    prefetch: Option<usize>,
) -> Result<ParquetMetaData>
where
    F: FnMut(Range<usize>) -> Fut + Send,
    Fut: Future<Output = Result<Bytes>> + Send,
{
    let fetch = MetadataFetchFn(fetch);
    ParquetMetaDataReader::new()
        .with_prefetch_hint(prefetch)
        .load_and_finish(fetch, file_size)
        .await
}

// these tests are all replicated in parquet::file::metadata::reader
#[allow(deprecated)]
#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::reader::{FileReader, Length, SerializedFileReader};
    use crate::util::test_common::file_util::get_test_file;
    use std::fs::File;
    use std::io::{Read, Seek, SeekFrom};
    use std::sync::atomic::{AtomicUsize, Ordering};

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

        let reader = SerializedFileReader::new(file.try_clone().unwrap()).unwrap();
        let expected = reader.metadata().file_metadata().schema();
        let fetch_count = AtomicUsize::new(0);

        let mut fetch = |range| {
            fetch_count.fetch_add(1, Ordering::SeqCst);
            futures::future::ready(read_range(&mut file, range))
        };

        let actual = fetch_parquet_metadata(&mut fetch, len, None).await.unwrap();
        assert_eq!(actual.file_metadata().schema(), expected);
        assert_eq!(fetch_count.load(Ordering::SeqCst), 2);

        // Metadata hint too small - below footer size
        fetch_count.store(0, Ordering::SeqCst);
        let actual = fetch_parquet_metadata(&mut fetch, len, Some(7))
            .await
            .unwrap();
        assert_eq!(actual.file_metadata().schema(), expected);
        assert_eq!(fetch_count.load(Ordering::SeqCst), 2);

        // Metadata hint too small
        fetch_count.store(0, Ordering::SeqCst);
        let actual = fetch_parquet_metadata(&mut fetch, len, Some(10))
            .await
            .unwrap();
        assert_eq!(actual.file_metadata().schema(), expected);
        assert_eq!(fetch_count.load(Ordering::SeqCst), 2);

        // Metadata hint too large
        fetch_count.store(0, Ordering::SeqCst);
        let actual = fetch_parquet_metadata(&mut fetch, len, Some(500))
            .await
            .unwrap();
        assert_eq!(actual.file_metadata().schema(), expected);
        assert_eq!(fetch_count.load(Ordering::SeqCst), 1);

        // Metadata hint exactly correct
        fetch_count.store(0, Ordering::SeqCst);
        let actual = fetch_parquet_metadata(&mut fetch, len, Some(428))
            .await
            .unwrap();
        assert_eq!(actual.file_metadata().schema(), expected);
        assert_eq!(fetch_count.load(Ordering::SeqCst), 1);

        let err = fetch_parquet_metadata(&mut fetch, 4, None)
            .await
            .unwrap_err()
            .to_string();
        assert_eq!(err, "EOF: file size of 4 is less than footer");

        let err = fetch_parquet_metadata(&mut fetch, 20, None)
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
        let mut loader = MetadataLoader::load(f, len, None).await.unwrap();
        assert_eq!(fetch_count.load(Ordering::SeqCst), 2);
        loader.load_page_index(true, true).await.unwrap();
        assert_eq!(fetch_count.load(Ordering::SeqCst), 3);
        let metadata = loader.finish();
        assert!(metadata.offset_index().is_some() && metadata.column_index().is_some());

        // Prefetch just footer exactly
        fetch_count.store(0, Ordering::SeqCst);
        let f = MetadataFetchFn(&mut fetch);
        let mut loader = MetadataLoader::load(f, len, Some(1729)).await.unwrap();
        assert_eq!(fetch_count.load(Ordering::SeqCst), 1);
        loader.load_page_index(true, true).await.unwrap();
        assert_eq!(fetch_count.load(Ordering::SeqCst), 2);
        let metadata = loader.finish();
        assert!(metadata.offset_index().is_some() && metadata.column_index().is_some());

        // Prefetch more than footer but not enough
        fetch_count.store(0, Ordering::SeqCst);
        let f = MetadataFetchFn(&mut fetch);
        let mut loader = MetadataLoader::load(f, len, Some(130649)).await.unwrap();
        assert_eq!(fetch_count.load(Ordering::SeqCst), 1);
        loader.load_page_index(true, true).await.unwrap();
        assert_eq!(fetch_count.load(Ordering::SeqCst), 2);
        let metadata = loader.finish();
        assert!(metadata.offset_index().is_some() && metadata.column_index().is_some());

        // Prefetch exactly enough
        fetch_count.store(0, Ordering::SeqCst);
        let f = MetadataFetchFn(&mut fetch);
        let mut loader = MetadataLoader::load(f, len, Some(130650)).await.unwrap();
        assert_eq!(fetch_count.load(Ordering::SeqCst), 1);
        loader.load_page_index(true, true).await.unwrap();
        assert_eq!(fetch_count.load(Ordering::SeqCst), 1);
        let metadata = loader.finish();
        assert!(metadata.offset_index().is_some() && metadata.column_index().is_some());
    }
}
