// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use crate::arrow::async_reader::AsyncFileReader;
use crate::errors::Result;
use bytes::Bytes;
use futures::future::BoxFuture;
use std::ops::Range;

/// A data source that can be used with [`ParquetMetaDataReader`] to load [`ParquetMetaData`]
///
/// Note that implementation is provided for [`AsyncFileReader`].
///
/// # Example `MetadataFetch` for a custom async data source
///
/// ```rust
/// # use parquet::errors::Result;
/// # use parquet::arrow::async_reader::MetadataFetch;
/// # use bytes::Bytes;
/// # use std::ops::Range;
/// # use std::io::SeekFrom;
/// # use futures::future::BoxFuture;
/// # use futures::FutureExt;
/// # use tokio::io::{AsyncReadExt, AsyncSeekExt};
/// // Adapter that implements the API for reading bytes from an async source (in
/// // this case a tokio::fs::File)
/// struct TokioFileMetadata {
///     file: tokio::fs::File,
/// }
/// impl MetadataFetch for TokioFileMetadata {
///     fn fetch(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes>> {
///         // return a future that fetches data in range
///         async move {
///             let len = (range.end - range.start).try_into().unwrap();
///             let mut buf = vec![0; len]; // target buffer
///             // seek to the start of the range and read the data
///             self.file.seek(SeekFrom::Start(range.start)).await?;
///             self.file.read_exact(&mut buf).await?;
///             Ok(Bytes::from(buf)) // convert to Bytes
///         }
///             .boxed() // turn into BoxedFuture, using FutureExt::boxed
///     }
/// }
///```
///
/// [`ParquetMetaDataReader`]: crate::file::metadata::reader::ParquetMetaDataReader
/// [`ParquetMetaData`]: crate::file::metadata::ParquetMetaData
pub trait MetadataFetch {
    /// Return a future that fetches the specified range of bytes asynchronously
    ///
    /// Note the returned type is a boxed future, often created by
    /// [`FutureExt::boxed`]. See the trait documentation for an example
    ///
    /// [`FutureExt::boxed`]: futures::FutureExt::boxed
    fn fetch(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes>>;
}

impl<T: AsyncFileReader> MetadataFetch for &mut T {
    fn fetch(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes>> {
        self.get_bytes(range)
    }
}

/// A data source that can be used with [`ParquetMetaDataReader`] to load [`ParquetMetaData`] via suffix
/// requests, without knowing the file size
///
/// [`ParquetMetaDataReader`]: crate::file::metadata::reader::ParquetMetaDataReader
/// [`ParquetMetaData`]: crate::file::metadata::ParquetMetaData
pub trait MetadataSuffixFetch: MetadataFetch {
    /// Return a future that fetches the last `n` bytes asynchronously
    ///
    /// Note the returned type is a boxed future, often created by
    /// [`FutureExt::boxed`]. See the trait documentation for an example
    ///
    /// [`FutureExt::boxed`]: futures::FutureExt::boxed
    fn fetch_suffix(&mut self, suffix: usize) -> BoxFuture<'_, Result<Bytes>>;
}
