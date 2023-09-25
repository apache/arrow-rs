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

#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    clippy::explicit_iter_loop,
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]

//! # object_store
//!
//! This crate provides a uniform API for interacting with object
//! storage services and local files via the [`ObjectStore`]
//! trait.
//!
//! Using this crate, the same binary and code can run in multiple
//! clouds and local test environments, via a simple runtime
//! configuration change.
//!
//! # Highlights
//!
//! 1. A focused, easy to use, idiomatic, well documented, high
//! performance, `async` API.
//!
//! 2. Production quality, leading this crate to be used in large
//! scale production systems, such as [crates.io] and [InfluxDB IOx].
//!
//! 3. Stable and predictable governance via the [Apache Arrow] project.
//!
//! Originally developed for [InfluxDB IOx] and subsequently donated
//! to [Apache Arrow].
//!
//! [Apache Arrow]: https://arrow.apache.org/
//! [InfluxDB IOx]: https://github.com/influxdata/influxdb_iox/
//! [crates.io]: https://github.com/rust-lang/crates.io
//!
//! # Available [`ObjectStore`] Implementations
//!
//! By default, this crate provides the following implementations:
//!
//! * Memory: [`InMemory`](memory::InMemory)
//! * Local filesystem: [`LocalFileSystem`](local::LocalFileSystem)
//!
//! Feature flags are used to enable support for other implementations:
//!
#![cfg_attr(
    feature = "gcp",
    doc = "* `gcp`: [Google Cloud Storage](https://cloud.google.com/storage/) support. See [`GoogleCloudStorageBuilder`](gcp::GoogleCloudStorageBuilder)"
)]
#![cfg_attr(
    feature = "aws",
    doc = "* `aws`: [Amazon S3](https://aws.amazon.com/s3/). See [`AmazonS3Builder`](aws::AmazonS3Builder)"
)]
#![cfg_attr(
    feature = "azure",
    doc = "* `azure`: [Azure Blob Storage](https://azure.microsoft.com/en-gb/services/storage/blobs/). See [`MicrosoftAzureBuilder`](azure::MicrosoftAzureBuilder)"
)]
#![cfg_attr(
    feature = "http",
    doc = "* `http`: [HTTP/WebDAV Storage](https://datatracker.ietf.org/doc/html/rfc2518). See [`HttpBuilder`](http::HttpBuilder)"
)]
//!
//! # Adapters
//!
//! [`ObjectStore`] instances can be composed with various adapters
//! which add additional functionality:
//!
//! * Rate Throttling: [`ThrottleConfig`](throttle::ThrottleConfig)
//! * Concurrent Request Limit: [`LimitStore`](limit::LimitStore)
//!
//!
//! # List objects:
//!
//! Use the [`ObjectStore::list`] method to iterate over objects in
//! remote storage or files in the local filesystem:
//!
//! ```
//! # use object_store::local::LocalFileSystem;
//! # // use LocalFileSystem for example
//! # fn get_object_store() -> LocalFileSystem {
//! #   LocalFileSystem::new_with_prefix("/tmp").unwrap()
//! # }
//!
//! # async fn example() {
//! use std::sync::Arc;
//! use object_store::{path::Path, ObjectStore};
//! use futures::stream::StreamExt;
//!
//! // create an ObjectStore
//! let object_store: Arc<dyn ObjectStore> = Arc::new(get_object_store());
//!
//! // Recursively list all files below the 'data' path.
//! // 1. On AWS S3 this would be the 'data/' prefix
//! // 2. On a local filesystem, this would be the 'data' directory
//! let prefix: Path = "data".try_into().unwrap();
//!
//! // Get an `async` stream of Metadata objects:
//!  let list_stream = object_store
//!      .list(Some(&prefix))
//!      .await
//!      .expect("Error listing files");
//!
//!  // Print a line about each object based on its metadata
//!  // using for_each from `StreamExt` trait.
//!  list_stream
//!      .for_each(move |meta|  {
//!          async {
//!              let meta = meta.expect("Error listing");
//!              println!("Name: {}, size: {}", meta.location, meta.size);
//!          }
//!      })
//!      .await;
//! # }
//! ```
//!
//! Which will print out something like the following:
//!
//! ```text
//! Name: data/file01.parquet, size: 112832
//! Name: data/file02.parquet, size: 143119
//! Name: data/child/file03.parquet, size: 100
//! ...
//! ```
//!
//! # Fetch objects
//!
//! Use the [`ObjectStore::get`] method to fetch the data bytes
//! from remote storage or files in the local filesystem as a stream.
//!
//! ```
//! # use object_store::local::LocalFileSystem;
//! # // use LocalFileSystem for example
//! # fn get_object_store() -> LocalFileSystem {
//! #   LocalFileSystem::new_with_prefix("/tmp").unwrap()
//! # }
//!
//! # async fn example() {
//! use std::sync::Arc;
//! use object_store::{path::Path, ObjectStore};
//! use futures::stream::StreamExt;
//!
//! // create an ObjectStore
//! let object_store: Arc<dyn ObjectStore> = Arc::new(get_object_store());
//!
//! // Retrieve a specific file
//! let path: Path = "data/file01.parquet".try_into().unwrap();
//!
//! // fetch the bytes from object store
//! let stream = object_store
//!     .get(&path)
//!     .await
//!     .unwrap()
//!     .into_stream();
//!
//! // Count the '0's using `map` from `StreamExt` trait
//! let num_zeros = stream
//!     .map(|bytes| {
//!         let bytes = bytes.unwrap();
//!        bytes.iter().filter(|b| **b == 0).count()
//!     })
//!     .collect::<Vec<usize>>()
//!     .await
//!     .into_iter()
//!     .sum::<usize>();
//!
//! println!("Num zeros in {} is {}", path, num_zeros);
//! # }
//! ```
//!
//! Which will print out something like the following:
//!
//! ```text
//! Num zeros in data/file01.parquet is 657
//! ```
//! #  Put object
//! Use the [`ObjectStore::put`] method to save data in remote storage or local filesystem.
//!
//! ```
//! # use object_store::local::LocalFileSystem;
//! # fn get_object_store() -> LocalFileSystem {
//! #     LocalFileSystem::new_with_prefix("/tmp").unwrap()
//! # }
//! # async fn put() {
//!  use object_store::ObjectStore;
//!  use std::sync::Arc;
//!  use bytes::Bytes;
//!  use object_store::path::Path;
//!
//!  let object_store: Arc<dyn ObjectStore> = Arc::new(get_object_store());
//!  let path: Path = "data/file1".try_into().unwrap();
//!  let bytes = Bytes::from_static(b"hello");
//!  object_store
//!      .put(&path, bytes)
//!      .await
//!      .unwrap();
//! # }
//! ```
//!
//! #  Multipart put object
//! Use the [`ObjectStore::put_multipart`] method to save large amount of data in chunks.
//!
//! ```
//! # use object_store::local::LocalFileSystem;
//! # fn get_object_store() -> LocalFileSystem {
//! #     LocalFileSystem::new_with_prefix("/tmp").unwrap()
//! # }
//! # async fn multi_upload() {
//!  use object_store::ObjectStore;
//!  use std::sync::Arc;
//!  use bytes::Bytes;
//!  use tokio::io::AsyncWriteExt;
//!  use object_store::path::Path;
//!
//!  let object_store: Arc<dyn ObjectStore> = Arc::new(get_object_store());
//!  let path: Path = "data/large_file".try_into().unwrap();
//!  let (_id, mut writer) =  object_store
//!      .put_multipart(&path)
//!      .await
//!      .unwrap();
//!  let bytes = Bytes::from_static(b"hello");
//!  writer.write_all(&bytes).await.unwrap();
//!  writer.flush().await.unwrap();
//!  writer.shutdown().await.unwrap();
//! # }
//! ```

#[cfg(all(
    target_arch = "wasm32",
    any(feature = "gcp", feature = "aws", feature = "azure", feature = "http")
))]
compile_error!("Features 'gcp', 'aws', 'azure', 'http' are not supported on wasm.");

#[cfg(feature = "aws")]
pub mod aws;
#[cfg(feature = "azure")]
pub mod azure;
pub mod buffered;
#[cfg(not(target_arch = "wasm32"))]
pub mod chunked;
pub mod delimited;
#[cfg(feature = "gcp")]
pub mod gcp;
#[cfg(feature = "http")]
pub mod http;
pub mod limit;
#[cfg(not(target_arch = "wasm32"))]
pub mod local;
pub mod memory;
pub mod path;
pub mod prefix;
pub mod throttle;

#[cfg(feature = "cloud")]
mod client;

#[cfg(feature = "cloud")]
pub use client::{
    backoff::BackoffConfig, retry::RetryConfig, ClientConfigKey, ClientOptions,
    CredentialProvider, StaticCredentialProvider,
};

#[cfg(feature = "cloud")]
mod config;

#[cfg(feature = "cloud")]
pub mod multipart;
mod parse;
mod util;

pub use parse::{parse_url, parse_url_opts};

use crate::path::Path;
#[cfg(not(target_arch = "wasm32"))]
use crate::util::maybe_spawn_blocking;
pub use crate::util::{coalesce_ranges, collect_bytes, OBJECT_STORE_COALESCE_DEFAULT};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use snafu::Snafu;
use std::fmt::{Debug, Formatter};
#[cfg(not(target_arch = "wasm32"))]
use std::io::{Read, Seek, SeekFrom};
use std::ops::Range;
use std::sync::Arc;
use tokio::io::AsyncWrite;

/// An alias for a dynamically dispatched object store implementation.
pub type DynObjectStore = dyn ObjectStore;

/// Id type for multi-part uploads.
pub type MultipartId = String;

/// Universal API to multiple object store services.
#[async_trait]
pub trait ObjectStore: std::fmt::Display + Send + Sync + Debug + 'static {
    /// Save the provided bytes to the specified location
    ///
    /// The operation is guaranteed to be atomic, it will either successfully
    /// write the entirety of `bytes` to `location`, or fail. No clients
    /// should be able to observe a partially written object
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<()>;

    /// Get a multi-part upload that allows writing data in chunks
    ///
    /// Most cloud-based uploads will buffer and upload parts in parallel.
    ///
    /// To complete the upload, [AsyncWrite::poll_shutdown] must be called
    /// to completion. This operation is guaranteed to be atomic, it will either
    /// make all the written data available at `location`, or fail. No clients
    /// should be able to observe a partially written object
    ///
    /// For some object stores (S3, GCS, and local in particular), if the
    /// writer fails or panics, you must call [ObjectStore::abort_multipart]
    /// to clean up partially written data.
    async fn put_multipart(
        &self,
        location: &Path,
    ) -> Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)>;

    /// Cleanup an aborted upload.
    ///
    /// See documentation for individual stores for exact behavior, as capabilities
    /// vary by object store.
    async fn abort_multipart(
        &self,
        location: &Path,
        multipart_id: &MultipartId,
    ) -> Result<()>;

    /// Returns an [`AsyncWrite`] that can be used to append to the object at `location`
    ///
    /// A new object will be created if it doesn't already exist, otherwise it will be
    /// opened, with subsequent writes appended to the end.
    ///
    /// This operation cannot be supported by all stores, most use-cases should prefer
    /// [`ObjectStore::put`] and [`ObjectStore::put_multipart`] for better portability
    /// and stronger guarantees
    ///
    /// This API is not guaranteed to be atomic, in particular
    ///
    /// * On error, `location` may contain partial data
    /// * Concurrent calls to [`ObjectStore::list`] may return partially written objects
    /// * Concurrent calls to [`ObjectStore::get`] may return partially written data
    /// * Concurrent calls to [`ObjectStore::put`] may result in data loss / corruption
    /// * Concurrent calls to [`ObjectStore::append`] may result in data loss / corruption
    ///
    /// Additionally some stores, such as Azure, may only support appending to objects created
    /// with [`ObjectStore::append`], and not with [`ObjectStore::put`], [`ObjectStore::copy`], or
    /// [`ObjectStore::put_multipart`]
    async fn append(
        &self,
        _location: &Path,
    ) -> Result<Box<dyn AsyncWrite + Unpin + Send>> {
        Err(Error::NotImplemented)
    }

    /// Return the bytes that are stored at the specified location.
    async fn get(&self, location: &Path) -> Result<GetResult> {
        self.get_opts(location, GetOptions::default()).await
    }

    /// Perform a get request with options
    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult>;

    /// Return the bytes that are stored at the specified location
    /// in the given byte range
    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
        let options = GetOptions {
            range: Some(range.clone()),
            ..Default::default()
        };
        self.get_opts(location, options).await?.bytes().await
    }

    /// Return the bytes that are stored at the specified location
    /// in the given byte ranges
    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<usize>],
    ) -> Result<Vec<Bytes>> {
        coalesce_ranges(
            ranges,
            |range| self.get_range(location, range),
            OBJECT_STORE_COALESCE_DEFAULT,
        )
        .await
    }

    /// Return the metadata for the specified location
    async fn head(&self, location: &Path) -> Result<ObjectMeta>;

    /// Delete the object at the specified location.
    async fn delete(&self, location: &Path) -> Result<()>;

    /// Delete all the objects at the specified locations
    ///
    /// When supported, this method will use bulk operations that delete more
    /// than one object per a request. The default implementation will call
    /// the single object delete method for each location, but with up to 10
    /// concurrent requests.
    ///
    /// The returned stream yields the results of the delete operations in the
    /// same order as the input locations. However, some errors will be from
    /// an overall call to a bulk delete operation, and not from a specific
    /// location.
    ///
    /// If the object did not exist, the result may be an error or a success,
    /// depending on the behavior of the underlying store. For example, local
    /// filesystems, GCP, and Azure return an error, while S3 and in-memory will
    /// return Ok. If it is an error, it will be [`Error::NotFound`].
    ///
    /// ```
    /// # use object_store::local::LocalFileSystem;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let root = tempfile::TempDir::new().unwrap();
    /// # let store = LocalFileSystem::new_with_prefix(root.path()).unwrap();
    /// use object_store::{ObjectStore, ObjectMeta};
    /// use object_store::path::Path;
    /// use futures::{StreamExt, TryStreamExt};
    /// use bytes::Bytes;
    ///
    /// // Create two objects
    /// store.put(&Path::from("foo"), Bytes::from("foo")).await?;
    /// store.put(&Path::from("bar"), Bytes::from("bar")).await?;
    ///
    /// // List object
    /// let locations = store.list(None).await?
    ///   .map(|meta: Result<ObjectMeta, _>| meta.map(|m| m.location))
    ///   .boxed();
    ///
    /// // Delete them
    /// store.delete_stream(locations).try_collect::<Vec<Path>>().await?;
    /// # Ok(())
    /// # }
    /// # let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
    /// # rt.block_on(example()).unwrap();
    /// ```
    fn delete_stream<'a>(
        &'a self,
        locations: BoxStream<'a, Result<Path>>,
    ) -> BoxStream<'a, Result<Path>> {
        locations
            .map(|location| async {
                let location = location?;
                self.delete(&location).await?;
                Ok(location)
            })
            .buffered(10)
            .boxed()
    }

    /// List all the objects with the given prefix.
    ///
    /// Prefixes are evaluated on a path segment basis, i.e. `foo/bar/` is a prefix of `foo/bar/x` but not of
    /// `foo/bar_baz/x`.
    ///
    /// Note: the order of returned [`ObjectMeta`] is not guaranteed
    async fn list(
        &self,
        prefix: Option<&Path>,
    ) -> Result<BoxStream<'_, Result<ObjectMeta>>>;

    /// List all the objects with the given prefix and a location greater than `offset`
    ///
    /// Some stores, such as S3 and GCS, may be able to push `offset` down to reduce
    /// the number of network requests required
    ///
    /// Note: the order of returned [`ObjectMeta`] is not guaranteed
    async fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> Result<BoxStream<'_, Result<ObjectMeta>>> {
        let offset = offset.clone();
        let stream = self
            .list(prefix)
            .await?
            .try_filter(move |f| futures::future::ready(f.location > offset))
            .boxed();
        Ok(stream)
    }

    /// List objects with the given prefix and an implementation specific
    /// delimiter. Returns common prefixes (directories) in addition to object
    /// metadata.
    ///
    /// Prefixes are evaluated on a path segment basis, i.e. `foo/bar/` is a prefix of `foo/bar/x` but not of
    /// `foo/bar_baz/x`.
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult>;

    /// Copy an object from one path to another in the same object store.
    ///
    /// If there exists an object at the destination, it will be overwritten.
    async fn copy(&self, from: &Path, to: &Path) -> Result<()>;

    /// Move an object from one path to another in the same object store.
    ///
    /// By default, this is implemented as a copy and then delete source. It may not
    /// check when deleting source that it was the same object that was originally copied.
    ///
    /// If there exists an object at the destination, it will be overwritten.
    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        self.copy(from, to).await?;
        self.delete(from).await
    }

    /// Copy an object from one path to another, only if destination is empty.
    ///
    /// Will return an error if the destination already has an object.
    ///
    /// Performs an atomic operation if the underlying object storage supports it.
    /// If atomic operations are not supported by the underlying object storage (like S3)
    /// it will return an error.
    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()>;

    /// Move an object from one path to another in the same object store.
    ///
    /// Will return an error if the destination already has an object.
    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.copy_if_not_exists(from, to).await?;
        self.delete(from).await
    }
}

macro_rules! as_ref_impl {
    ($type:ty) => {
        #[async_trait]
        impl ObjectStore for $type {
            async fn put(&self, location: &Path, bytes: Bytes) -> Result<()> {
                self.as_ref().put(location, bytes).await
            }

            async fn put_multipart(
                &self,
                location: &Path,
            ) -> Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
                self.as_ref().put_multipart(location).await
            }

            async fn abort_multipart(
                &self,
                location: &Path,
                multipart_id: &MultipartId,
            ) -> Result<()> {
                self.as_ref().abort_multipart(location, multipart_id).await
            }

            async fn append(
                &self,
                location: &Path,
            ) -> Result<Box<dyn AsyncWrite + Unpin + Send>> {
                self.as_ref().append(location).await
            }

            async fn get(&self, location: &Path) -> Result<GetResult> {
                self.as_ref().get(location).await
            }

            async fn get_opts(
                &self,
                location: &Path,
                options: GetOptions,
            ) -> Result<GetResult> {
                self.as_ref().get_opts(location, options).await
            }

            async fn get_range(
                &self,
                location: &Path,
                range: Range<usize>,
            ) -> Result<Bytes> {
                self.as_ref().get_range(location, range).await
            }

            async fn get_ranges(
                &self,
                location: &Path,
                ranges: &[Range<usize>],
            ) -> Result<Vec<Bytes>> {
                self.as_ref().get_ranges(location, ranges).await
            }

            async fn head(&self, location: &Path) -> Result<ObjectMeta> {
                self.as_ref().head(location).await
            }

            async fn delete(&self, location: &Path) -> Result<()> {
                self.as_ref().delete(location).await
            }

            fn delete_stream<'a>(
                &'a self,
                locations: BoxStream<'a, Result<Path>>,
            ) -> BoxStream<'a, Result<Path>> {
                self.as_ref().delete_stream(locations)
            }

            async fn list(
                &self,
                prefix: Option<&Path>,
            ) -> Result<BoxStream<'_, Result<ObjectMeta>>> {
                self.as_ref().list(prefix).await
            }

            async fn list_with_offset(
                &self,
                prefix: Option<&Path>,
                offset: &Path,
            ) -> Result<BoxStream<'_, Result<ObjectMeta>>> {
                self.as_ref().list_with_offset(prefix, offset).await
            }

            async fn list_with_delimiter(
                &self,
                prefix: Option<&Path>,
            ) -> Result<ListResult> {
                self.as_ref().list_with_delimiter(prefix).await
            }

            async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
                self.as_ref().copy(from, to).await
            }

            async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
                self.as_ref().rename(from, to).await
            }

            async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
                self.as_ref().copy_if_not_exists(from, to).await
            }

            async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
                self.as_ref().rename_if_not_exists(from, to).await
            }
        }
    };
}

as_ref_impl!(Arc<dyn ObjectStore>);
as_ref_impl!(Box<dyn ObjectStore>);

/// Result of a list call that includes objects, prefixes (directories) and a
/// token for the next set of results. Individual result sets may be limited to
/// 1,000 objects based on the underlying object storage's limitations.
#[derive(Debug)]
pub struct ListResult {
    /// Prefixes that are common (like directories)
    pub common_prefixes: Vec<Path>,
    /// Object metadata for the listing
    pub objects: Vec<ObjectMeta>,
}

/// The metadata that describes an object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMeta {
    /// The full path to the object
    pub location: Path,
    /// The last modified time
    pub last_modified: DateTime<Utc>,
    /// The size in bytes of the object
    pub size: usize,
    /// The unique identifier for the object
    pub e_tag: Option<String>,
}

/// Options for a get request, such as range
#[derive(Debug, Default)]
pub struct GetOptions {
    /// Request will succeed if the `ObjectMeta::e_tag` matches
    /// otherwise returning [`Error::Precondition`]
    ///
    /// <https://datatracker.ietf.org/doc/html/rfc9110#name-if-match>
    pub if_match: Option<String>,
    /// Request will succeed if the `ObjectMeta::e_tag` does not match
    /// otherwise returning [`Error::NotModified`]
    ///
    /// <https://datatracker.ietf.org/doc/html/rfc9110#section-13.1.2>
    pub if_none_match: Option<String>,
    /// Request will succeed if the object has been modified since
    ///
    /// <https://datatracker.ietf.org/doc/html/rfc9110#section-13.1.3>
    pub if_modified_since: Option<DateTime<Utc>>,
    /// Request will succeed if the object has not been modified since
    /// otherwise returning [`Error::Precondition`]
    ///
    /// Some stores, such as S3, will only return `NotModified` for exact
    /// timestamp matches, instead of for any timestamp greater than or equal.
    ///
    /// <https://datatracker.ietf.org/doc/html/rfc9110#section-13.1.4>
    pub if_unmodified_since: Option<DateTime<Utc>>,
    /// Request transfer of only the specified range of bytes
    /// otherwise returning [`Error::NotModified`]
    ///
    /// <https://datatracker.ietf.org/doc/html/rfc9110#name-range>
    pub range: Option<Range<usize>>,
}

impl GetOptions {
    /// Returns an error if the modification conditions on this request are not satisfied
    fn check_modified(
        &self,
        location: &Path,
        last_modified: DateTime<Utc>,
    ) -> Result<()> {
        if let Some(date) = self.if_modified_since {
            if last_modified <= date {
                return Err(Error::NotModified {
                    path: location.to_string(),
                    source: format!("{} >= {}", date, last_modified).into(),
                });
            }
        }

        if let Some(date) = self.if_unmodified_since {
            if last_modified > date {
                return Err(Error::Precondition {
                    path: location.to_string(),
                    source: format!("{} < {}", date, last_modified).into(),
                });
            }
        }
        Ok(())
    }
}

/// Result for a get request
#[derive(Debug)]
pub struct GetResult {
    /// The [`GetResultPayload`]
    pub payload: GetResultPayload,
    /// The [`ObjectMeta`] for this object
    pub meta: ObjectMeta,
    /// The range of bytes returned by this request
    pub range: Range<usize>,
}

/// The kind of a [`GetResult`]
///
/// This special cases the case of a local file, as some systems may
/// be able to optimise the case of a file already present on local disk
pub enum GetResultPayload {
    /// The file, path
    File(std::fs::File, std::path::PathBuf),
    /// An opaque stream of bytes
    Stream(BoxStream<'static, Result<Bytes>>),
}

impl Debug for GetResultPayload {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::File(_, _) => write!(f, "GetResultPayload(File)"),
            Self::Stream(_) => write!(f, "GetResultPayload(Stream)"),
        }
    }
}

impl GetResult {
    /// Collects the data into a [`Bytes`]
    pub async fn bytes(self) -> Result<Bytes> {
        let len = self.range.end - self.range.start;
        match self.payload {
            #[cfg(not(target_arch = "wasm32"))]
            GetResultPayload::File(mut file, path) => {
                maybe_spawn_blocking(move || {
                    file.seek(SeekFrom::Start(self.range.start as _)).map_err(
                        |source| local::Error::Seek {
                            source,
                            path: path.clone(),
                        },
                    )?;

                    let mut buffer = Vec::with_capacity(len);
                    file.take(len as _)
                        .read_to_end(&mut buffer)
                        .map_err(|source| local::Error::UnableToReadBytes {
                            source,
                            path,
                        })?;

                    Ok(buffer.into())
                })
                .await
            }
            GetResultPayload::Stream(s) => collect_bytes(s, Some(len)).await,
            #[cfg(target_arch = "wasm32")]
            _ => unimplemented!("File IO not implemented on wasm32."),
        }
    }

    /// Converts this into a byte stream
    ///
    /// If the `self.kind` is [`GetResultPayload::File`] will perform chunked reads of the file,
    /// otherwise will return the [`GetResultPayload::Stream`].
    ///
    /// # Tokio Compatibility
    ///
    /// Tokio discourages performing blocking IO on a tokio worker thread, however,
    /// no major operating systems have stable async file APIs. Therefore if called from
    /// a tokio context, this will use [`tokio::runtime::Handle::spawn_blocking`] to dispatch
    /// IO to a blocking thread pool, much like `tokio::fs` does under-the-hood.
    ///
    /// If not called from a tokio context, this will perform IO on the current thread with
    /// no additional complexity or overheads
    pub fn into_stream(self) -> BoxStream<'static, Result<Bytes>> {
        match self.payload {
            #[cfg(not(target_arch = "wasm32"))]
            GetResultPayload::File(file, path) => {
                const CHUNK_SIZE: usize = 8 * 1024;
                local::chunked_stream(file, path, self.range, CHUNK_SIZE)
            }
            GetResultPayload::Stream(s) => s,
            #[cfg(target_arch = "wasm32")]
            _ => unimplemented!("File IO not implemented on wasm32."),
        }
    }
}

/// A specialized `Result` for object store-related errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A specialized `Error` for object store-related errors
#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum Error {
    #[snafu(display("Generic {} error: {}", store, source))]
    Generic {
        store: &'static str,
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    #[snafu(display("Object at location {} not found: {}", path, source))]
    NotFound {
        path: String,
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    #[snafu(
        display("Encountered object with invalid path: {}", source),
        context(false)
    )]
    InvalidPath { source: path::Error },

    #[snafu(display("Error joining spawned task: {}", source), context(false))]
    JoinError { source: tokio::task::JoinError },

    #[snafu(display("Operation not supported: {}", source))]
    NotSupported {
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    #[snafu(display("Object at location {} already exists: {}", path, source))]
    AlreadyExists {
        path: String,
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    #[snafu(display("Request precondition failure for path {}: {}", path, source))]
    Precondition {
        path: String,
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    #[snafu(display("Object at location {} not modified: {}", path, source))]
    NotModified {
        path: String,
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    #[snafu(display("Operation not yet implemented."))]
    NotImplemented,

    #[snafu(display(
        "Configuration key: '{}' is not valid for store '{}'.",
        key,
        store
    ))]
    UnknownConfigurationKey { store: &'static str, key: String },
}

impl From<Error> for std::io::Error {
    fn from(e: Error) -> Self {
        let kind = match &e {
            Error::NotFound { .. } => std::io::ErrorKind::NotFound,
            _ => std::io::ErrorKind::Other,
        };
        Self::new(kind, e)
    }
}

#[cfg(test)]
mod test_util {
    use super::*;
    use futures::TryStreamExt;

    macro_rules! maybe_skip_integration {
        () => {
            if std::env::var("TEST_INTEGRATION").is_err() {
                eprintln!("Skipping integration test - set TEST_INTEGRATION");
                return;
            }
        };
    }
    pub(crate) use maybe_skip_integration;

    pub async fn flatten_list_stream(
        storage: &DynObjectStore,
        prefix: Option<&Path>,
    ) -> Result<Vec<Path>> {
        storage
            .list(prefix)
            .await?
            .map_ok(|meta| meta.location)
            .try_collect::<Vec<Path>>()
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::flatten_list_stream;
    use rand::{thread_rng, Rng};
    use tokio::io::AsyncWriteExt;

    pub(crate) async fn put_get_delete_list(storage: &DynObjectStore) {
        put_get_delete_list_opts(storage, false).await
    }

    pub(crate) async fn put_get_delete_list_opts(
        storage: &DynObjectStore,
        skip_list_with_spaces: bool,
    ) {
        delete_fixtures(storage).await;

        let content_list = flatten_list_stream(storage, None).await.unwrap();
        assert!(
            content_list.is_empty(),
            "Expected list to be empty; found: {content_list:?}"
        );

        let location = Path::from("test_dir/test_file.json");

        let data = Bytes::from("arbitrary data");
        let expected_data = data.clone();
        storage.put(&location, data).await.unwrap();

        let root = Path::from("/");

        // List everything
        let content_list = flatten_list_stream(storage, None).await.unwrap();
        assert_eq!(content_list, &[location.clone()]);

        // Should behave the same as no prefix
        let content_list = flatten_list_stream(storage, Some(&root)).await.unwrap();
        assert_eq!(content_list, &[location.clone()]);

        // List with delimiter
        let result = storage.list_with_delimiter(None).await.unwrap();
        assert_eq!(&result.objects, &[]);
        assert_eq!(result.common_prefixes.len(), 1);
        assert_eq!(result.common_prefixes[0], Path::from("test_dir"));

        // Should behave the same as no prefix
        let result = storage.list_with_delimiter(Some(&root)).await.unwrap();
        assert!(result.objects.is_empty());
        assert_eq!(result.common_prefixes.len(), 1);
        assert_eq!(result.common_prefixes[0], Path::from("test_dir"));

        // Should return not found
        let err = storage.get(&Path::from("test_dir")).await.unwrap_err();
        assert!(matches!(err, crate::Error::NotFound { .. }), "{}", err);

        // Should return not found
        let err = storage.head(&Path::from("test_dir")).await.unwrap_err();
        assert!(matches!(err, crate::Error::NotFound { .. }), "{}", err);

        // List everything starting with a prefix that should return results
        let prefix = Path::from("test_dir");
        let content_list = flatten_list_stream(storage, Some(&prefix)).await.unwrap();
        assert_eq!(content_list, &[location.clone()]);

        // List everything starting with a prefix that shouldn't return results
        let prefix = Path::from("something");
        let content_list = flatten_list_stream(storage, Some(&prefix)).await.unwrap();
        assert!(content_list.is_empty());

        let read_data = storage.get(&location).await.unwrap().bytes().await.unwrap();
        assert_eq!(&*read_data, expected_data);

        // Test range request
        let range = 3..7;
        let range_result = storage.get_range(&location, range.clone()).await;

        let out_of_range = 200..300;
        let out_of_range_result = storage.get_range(&location, out_of_range).await;

        let bytes = range_result.unwrap();
        assert_eq!(bytes, expected_data.slice(range));

        // Should be a non-fatal error
        out_of_range_result.unwrap_err();

        let ranges = vec![0..1, 2..3, 0..5];
        let bytes = storage.get_ranges(&location, &ranges).await.unwrap();
        for (range, bytes) in ranges.iter().zip(bytes) {
            assert_eq!(bytes, expected_data.slice(range.clone()))
        }

        let head = storage.head(&location).await.unwrap();
        assert_eq!(head.size, expected_data.len());

        storage.delete(&location).await.unwrap();

        let content_list = flatten_list_stream(storage, None).await.unwrap();
        assert!(content_list.is_empty());

        let err = storage.get(&location).await.unwrap_err();
        assert!(matches!(err, crate::Error::NotFound { .. }), "{}", err);

        let err = storage.head(&location).await.unwrap_err();
        assert!(matches!(err, crate::Error::NotFound { .. }), "{}", err);

        // Test handling of paths containing an encoded delimiter

        let file_with_delimiter = Path::from_iter(["a", "b/c", "foo.file"]);
        storage
            .put(&file_with_delimiter, Bytes::from("arbitrary"))
            .await
            .unwrap();

        let files = flatten_list_stream(storage, None).await.unwrap();
        assert_eq!(files, vec![file_with_delimiter.clone()]);

        let files = flatten_list_stream(storage, Some(&Path::from("a/b")))
            .await
            .unwrap();
        assert!(files.is_empty());

        let files = storage
            .list_with_delimiter(Some(&Path::from("a/b")))
            .await
            .unwrap();
        assert!(files.common_prefixes.is_empty());
        assert!(files.objects.is_empty());

        let files = storage
            .list_with_delimiter(Some(&Path::from("a")))
            .await
            .unwrap();
        assert_eq!(files.common_prefixes, vec![Path::from_iter(["a", "b/c"])]);
        assert!(files.objects.is_empty());

        let files = storage
            .list_with_delimiter(Some(&Path::from_iter(["a", "b/c"])))
            .await
            .unwrap();
        assert!(files.common_prefixes.is_empty());
        assert_eq!(files.objects.len(), 1);
        assert_eq!(files.objects[0].location, file_with_delimiter);

        storage.delete(&file_with_delimiter).await.unwrap();

        // Test handling of paths containing non-ASCII characters, e.g. emoji

        let emoji_prefix = Path::from("🙀");
        let emoji_file = Path::from("🙀/😀.parquet");
        storage
            .put(&emoji_file, Bytes::from("arbitrary"))
            .await
            .unwrap();

        storage.head(&emoji_file).await.unwrap();
        storage
            .get(&emoji_file)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();

        let files = flatten_list_stream(storage, Some(&emoji_prefix))
            .await
            .unwrap();

        assert_eq!(files, vec![emoji_file.clone()]);

        let dst = Path::from("foo.parquet");
        storage.copy(&emoji_file, &dst).await.unwrap();
        let mut files = flatten_list_stream(storage, None).await.unwrap();
        files.sort_unstable();
        assert_eq!(files, vec![emoji_file.clone(), dst.clone()]);

        let dst2 = Path::from("new/nested/foo.parquet");
        storage.copy(&emoji_file, &dst2).await.unwrap();
        let mut files = flatten_list_stream(storage, None).await.unwrap();
        files.sort_unstable();
        assert_eq!(files, vec![emoji_file.clone(), dst.clone(), dst2.clone()]);

        let dst3 = Path::from("new/nested2/bar.parquet");
        storage.rename(&dst, &dst3).await.unwrap();
        let mut files = flatten_list_stream(storage, None).await.unwrap();
        files.sort_unstable();
        assert_eq!(files, vec![emoji_file.clone(), dst2.clone(), dst3.clone()]);

        let err = storage.head(&dst).await.unwrap_err();
        assert!(matches!(err, Error::NotFound { .. }));

        storage.delete(&emoji_file).await.unwrap();
        storage.delete(&dst3).await.unwrap();
        storage.delete(&dst2).await.unwrap();
        let files = flatten_list_stream(storage, Some(&emoji_prefix))
            .await
            .unwrap();
        assert!(files.is_empty());

        // Test handling of paths containing percent-encoded sequences

        // "HELLO" percent encoded
        let hello_prefix = Path::parse("%48%45%4C%4C%4F").unwrap();
        let path = hello_prefix.child("foo.parquet");

        storage.put(&path, Bytes::from(vec![0, 1])).await.unwrap();
        let files = flatten_list_stream(storage, Some(&hello_prefix))
            .await
            .unwrap();
        assert_eq!(files, vec![path.clone()]);

        // Cannot list by decoded representation
        let files = flatten_list_stream(storage, Some(&Path::from("HELLO")))
            .await
            .unwrap();
        assert!(files.is_empty());

        // Cannot access by decoded representation
        let err = storage
            .head(&Path::from("HELLO/foo.parquet"))
            .await
            .unwrap_err();
        assert!(matches!(err, crate::Error::NotFound { .. }), "{}", err);

        storage.delete(&path).await.unwrap();

        // Can also write non-percent encoded sequences
        let path = Path::parse("%Q.parquet").unwrap();
        storage.put(&path, Bytes::from(vec![0, 1])).await.unwrap();

        let files = flatten_list_stream(storage, None).await.unwrap();
        assert_eq!(files, vec![path.clone()]);

        storage.delete(&path).await.unwrap();

        let path = Path::parse("foo bar/I contain spaces.parquet").unwrap();
        storage.put(&path, Bytes::from(vec![0, 1])).await.unwrap();
        storage.head(&path).await.unwrap();

        if !skip_list_with_spaces {
            let files = flatten_list_stream(storage, Some(&Path::from("foo bar")))
                .await
                .unwrap();
            assert_eq!(files, vec![path.clone()]);
        }
        storage.delete(&path).await.unwrap();

        let files = flatten_list_stream(storage, None).await.unwrap();
        assert!(files.is_empty(), "{files:?}");

        // Test list order
        let files = vec![
            Path::from("a a/b.file"),
            Path::parse("a%2Fa.file").unwrap(),
            Path::from("a/😀.file"),
            Path::from("a/a file"),
            Path::parse("a/a%2F.file").unwrap(),
            Path::from("a/a.file"),
            Path::from("a/a/b.file"),
            Path::from("a/b.file"),
            Path::from("aa/a.file"),
            Path::from("ab/a.file"),
        ];

        for file in &files {
            storage.put(file, "foo".into()).await.unwrap();
        }

        let cases = [
            (None, Path::from("a")),
            (None, Path::from("a/a file")),
            (None, Path::from("a/a/b.file")),
            (None, Path::from("ab/a.file")),
            (None, Path::from("a%2Fa.file")),
            (None, Path::from("a/😀.file")),
            (Some(Path::from("a")), Path::from("")),
            (Some(Path::from("a")), Path::from("a")),
            (Some(Path::from("a")), Path::from("a/😀")),
            (Some(Path::from("a")), Path::from("a/😀.file")),
            (Some(Path::from("a")), Path::from("a/b")),
            (Some(Path::from("a")), Path::from("a/a/b.file")),
        ];

        for (prefix, offset) in cases {
            let s = storage
                .list_with_offset(prefix.as_ref(), &offset)
                .await
                .unwrap();

            let mut actual: Vec<_> =
                s.map_ok(|x| x.location).try_collect().await.unwrap();

            actual.sort_unstable();

            let expected: Vec<_> = files
                .iter()
                .cloned()
                .filter(|x| {
                    let prefix_match =
                        prefix.as_ref().map(|p| x.prefix_matches(p)).unwrap_or(true);
                    prefix_match && x > &offset
                })
                .collect();

            assert_eq!(actual, expected, "{prefix:?} - {offset:?}");
        }

        // Test bulk delete
        let paths = vec![
            Path::from("a/a.file"),
            Path::from("a/a/b.file"),
            Path::from("aa/a.file"),
            Path::from("does_not_exist"),
            Path::from("I'm a < & weird path"),
            Path::from("ab/a.file"),
            Path::from("a/😀.file"),
        ];

        storage.put(&paths[4], "foo".into()).await.unwrap();

        let out_paths = storage
            .delete_stream(futures::stream::iter(paths.clone()).map(Ok).boxed())
            .collect::<Vec<_>>()
            .await;

        assert_eq!(out_paths.len(), paths.len());

        let expect_errors = [3];

        for (i, input_path) in paths.iter().enumerate() {
            let err = storage.head(input_path).await.unwrap_err();
            assert!(matches!(err, crate::Error::NotFound { .. }), "{}", err);

            if expect_errors.contains(&i) {
                // Some object stores will report NotFound, but others (such as S3) will
                // report success regardless.
                match &out_paths[i] {
                    Err(Error::NotFound { path: out_path, .. }) => {
                        assert!(out_path.ends_with(&input_path.to_string()));
                    }
                    Ok(out_path) => {
                        assert_eq!(out_path, input_path);
                    }
                    _ => panic!("unexpected error"),
                }
            } else {
                assert_eq!(out_paths[i].as_ref().unwrap(), input_path);
            }
        }

        delete_fixtures(storage).await;

        let path = Path::from("empty");
        storage.put(&path, Bytes::new()).await.unwrap();
        let meta = storage.head(&path).await.unwrap();
        assert_eq!(meta.size, 0);
        let data = storage.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(data.len(), 0);

        storage.delete(&path).await.unwrap();
    }

    pub(crate) async fn get_opts(storage: &dyn ObjectStore) {
        let path = Path::from("test");
        storage.put(&path, "foo".into()).await.unwrap();
        let meta = storage.head(&path).await.unwrap();

        let options = GetOptions {
            if_unmodified_since: Some(meta.last_modified),
            ..GetOptions::default()
        };
        match storage.get_opts(&path, options).await {
            Ok(_) | Err(Error::NotSupported { .. }) => {}
            Err(e) => panic!("{e}"),
        }

        let options = GetOptions {
            if_unmodified_since: Some(meta.last_modified + chrono::Duration::hours(10)),
            ..GetOptions::default()
        };
        match storage.get_opts(&path, options).await {
            Ok(_) | Err(Error::NotSupported { .. }) => {}
            Err(e) => panic!("{e}"),
        }

        let options = GetOptions {
            if_unmodified_since: Some(meta.last_modified - chrono::Duration::hours(10)),
            ..GetOptions::default()
        };
        match storage.get_opts(&path, options).await {
            Err(Error::Precondition { .. } | Error::NotSupported { .. }) => {}
            d => panic!("{d:?}"),
        }

        let options = GetOptions {
            if_modified_since: Some(meta.last_modified),
            ..GetOptions::default()
        };
        match storage.get_opts(&path, options).await {
            Err(Error::NotModified { .. } | Error::NotSupported { .. }) => {}
            d => panic!("{d:?}"),
        }

        let options = GetOptions {
            if_modified_since: Some(meta.last_modified - chrono::Duration::hours(10)),
            ..GetOptions::default()
        };
        match storage.get_opts(&path, options).await {
            Ok(_) | Err(Error::NotSupported { .. }) => {}
            Err(e) => panic!("{e}"),
        }

        if let Some(tag) = meta.e_tag {
            let options = GetOptions {
                if_match: Some(tag.clone()),
                ..GetOptions::default()
            };
            storage.get_opts(&path, options).await.unwrap();

            let options = GetOptions {
                if_match: Some("invalid".to_string()),
                ..GetOptions::default()
            };
            let err = storage.get_opts(&path, options).await.unwrap_err();
            assert!(matches!(err, Error::Precondition { .. }), "{err}");

            let options = GetOptions {
                if_none_match: Some(tag.clone()),
                ..GetOptions::default()
            };
            let err = storage.get_opts(&path, options).await.unwrap_err();
            assert!(matches!(err, Error::NotModified { .. }), "{err}");

            let options = GetOptions {
                if_none_match: Some("invalid".to_string()),
                ..GetOptions::default()
            };
            storage.get_opts(&path, options).await.unwrap();
        }
    }

    /// Returns a chunk of length `chunk_length`
    fn get_chunk(chunk_length: usize) -> Bytes {
        let mut data = vec![0_u8; chunk_length];
        let mut rng = thread_rng();
        // Set a random selection of bytes
        for _ in 0..1000 {
            data[rng.gen_range(0..chunk_length)] = rng.gen();
        }
        data.into()
    }

    /// Returns `num_chunks` of length `chunks`
    fn get_chunks(chunk_length: usize, num_chunks: usize) -> Vec<Bytes> {
        (0..num_chunks).map(|_| get_chunk(chunk_length)).collect()
    }

    pub(crate) async fn stream_get(storage: &DynObjectStore) {
        let location = Path::from("test_dir/test_upload_file.txt");

        // Can write to storage
        let data = get_chunks(5_000, 10);
        let bytes_expected = data.concat();
        let (_, mut writer) = storage.put_multipart(&location).await.unwrap();
        for chunk in &data {
            writer.write_all(chunk).await.unwrap();
        }

        // Object should not yet exist in store
        let meta_res = storage.head(&location).await;
        assert!(meta_res.is_err());
        assert!(matches!(
            meta_res.unwrap_err(),
            crate::Error::NotFound { .. }
        ));

        let files = flatten_list_stream(storage, None).await.unwrap();
        assert_eq!(&files, &[]);

        let result = storage.list_with_delimiter(None).await.unwrap();
        assert_eq!(&result.objects, &[]);

        writer.shutdown().await.unwrap();
        let bytes_written = storage.get(&location).await.unwrap().bytes().await.unwrap();
        assert_eq!(bytes_expected, bytes_written);

        // Can overwrite some storage
        // Sizes chosen to ensure we write three parts
        let data = get_chunks(3_200_000, 7);
        let bytes_expected = data.concat();
        let (_, mut writer) = storage.put_multipart(&location).await.unwrap();
        for chunk in &data {
            writer.write_all(chunk).await.unwrap();
        }
        writer.shutdown().await.unwrap();
        let bytes_written = storage.get(&location).await.unwrap().bytes().await.unwrap();
        assert_eq!(bytes_expected, bytes_written);

        // We can abort an empty write
        let location = Path::from("test_dir/test_abort_upload.txt");
        let (upload_id, writer) = storage.put_multipart(&location).await.unwrap();
        drop(writer);
        storage
            .abort_multipart(&location, &upload_id)
            .await
            .unwrap();
        let get_res = storage.get(&location).await;
        assert!(get_res.is_err());
        assert!(matches!(
            get_res.unwrap_err(),
            crate::Error::NotFound { .. }
        ));

        // We can abort an in-progress write
        let (upload_id, mut writer) = storage.put_multipart(&location).await.unwrap();
        if let Some(chunk) = data.get(0) {
            writer.write_all(chunk).await.unwrap();
            let _ = writer.write(chunk).await.unwrap();
        }
        drop(writer);

        storage
            .abort_multipart(&location, &upload_id)
            .await
            .unwrap();
        let get_res = storage.get(&location).await;
        assert!(get_res.is_err());
        assert!(matches!(
            get_res.unwrap_err(),
            crate::Error::NotFound { .. }
        ));
    }

    pub(crate) async fn list_uses_directories_correctly(storage: &DynObjectStore) {
        delete_fixtures(storage).await;

        let content_list = flatten_list_stream(storage, None).await.unwrap();
        assert!(
            content_list.is_empty(),
            "Expected list to be empty; found: {content_list:?}"
        );

        let location1 = Path::from("foo/x.json");
        let location2 = Path::from("foo.bar/y.json");

        let data = Bytes::from("arbitrary data");
        storage.put(&location1, data.clone()).await.unwrap();
        storage.put(&location2, data).await.unwrap();

        let prefix = Path::from("foo");
        let content_list = flatten_list_stream(storage, Some(&prefix)).await.unwrap();
        assert_eq!(content_list, &[location1.clone()]);

        let result = storage.list_with_delimiter(Some(&prefix)).await.unwrap();
        assert_eq!(result.objects.len(), 1);
        assert_eq!(result.objects[0].location, location1);
        assert_eq!(result.common_prefixes, &[]);

        // Listing an existing path (file) should return an empty list:
        // https://github.com/apache/arrow-rs/issues/3712
        let content_list = flatten_list_stream(storage, Some(&location1))
            .await
            .unwrap();
        assert_eq!(content_list, &[]);

        let list = storage.list_with_delimiter(Some(&location1)).await.unwrap();
        assert_eq!(list.objects, &[]);
        assert_eq!(list.common_prefixes, &[]);

        let prefix = Path::from("foo/x");
        let content_list = flatten_list_stream(storage, Some(&prefix)).await.unwrap();
        assert_eq!(content_list, &[]);

        let list = storage.list_with_delimiter(Some(&prefix)).await.unwrap();
        assert_eq!(list.objects, &[]);
        assert_eq!(list.common_prefixes, &[]);
    }

    pub(crate) async fn list_with_delimiter(storage: &DynObjectStore) {
        delete_fixtures(storage).await;

        // ==================== check: store is empty ====================
        let content_list = flatten_list_stream(storage, None).await.unwrap();
        assert!(content_list.is_empty());

        // ==================== do: create files ====================
        let data = Bytes::from("arbitrary data");

        let files: Vec<_> = [
            "test_file",
            "mydb/wb/000/000/000.segment",
            "mydb/wb/000/000/001.segment",
            "mydb/wb/000/000/002.segment",
            "mydb/wb/001/001/000.segment",
            "mydb/wb/foo.json",
            "mydb/wbwbwb/111/222/333.segment",
            "mydb/data/whatevs",
        ]
        .iter()
        .map(|&s| Path::from(s))
        .collect();

        for f in &files {
            let data = data.clone();
            storage.put(f, data).await.unwrap();
        }

        // ==================== check: prefix-list `mydb/wb` (directory) ====================
        let prefix = Path::from("mydb/wb");

        let expected_000 = Path::from("mydb/wb/000");
        let expected_001 = Path::from("mydb/wb/001");
        let expected_location = Path::from("mydb/wb/foo.json");

        let result = storage.list_with_delimiter(Some(&prefix)).await.unwrap();

        assert_eq!(result.common_prefixes, vec![expected_000, expected_001]);
        assert_eq!(result.objects.len(), 1);

        let object = &result.objects[0];

        assert_eq!(object.location, expected_location);
        assert_eq!(object.size, data.len());

        // ==================== check: prefix-list `mydb/wb/000/000/001` (partial filename doesn't match) ====================
        let prefix = Path::from("mydb/wb/000/000/001");

        let result = storage.list_with_delimiter(Some(&prefix)).await.unwrap();
        assert!(result.common_prefixes.is_empty());
        assert_eq!(result.objects.len(), 0);

        // ==================== check: prefix-list `not_there` (non-existing prefix) ====================
        let prefix = Path::from("not_there");

        let result = storage.list_with_delimiter(Some(&prefix)).await.unwrap();
        assert!(result.common_prefixes.is_empty());
        assert!(result.objects.is_empty());

        // ==================== do: remove all files ====================
        for f in &files {
            storage.delete(f).await.unwrap();
        }

        // ==================== check: store is empty ====================
        let content_list = flatten_list_stream(storage, None).await.unwrap();
        assert!(content_list.is_empty());
    }

    pub(crate) async fn get_nonexistent_object(
        storage: &DynObjectStore,
        location: Option<Path>,
    ) -> crate::Result<Bytes> {
        let location =
            location.unwrap_or_else(|| Path::from("this_file_should_not_exist"));

        let err = storage.head(&location).await.unwrap_err();
        assert!(matches!(err, crate::Error::NotFound { .. }));

        storage.get(&location).await?.bytes().await
    }

    pub(crate) async fn rename_and_copy(storage: &DynObjectStore) {
        // Create two objects
        let path1 = Path::from("test1");
        let path2 = Path::from("test2");
        let contents1 = Bytes::from("cats");
        let contents2 = Bytes::from("dogs");

        // copy() make both objects identical
        storage.put(&path1, contents1.clone()).await.unwrap();
        storage.put(&path2, contents2.clone()).await.unwrap();
        storage.copy(&path1, &path2).await.unwrap();
        let new_contents = storage.get(&path2).await.unwrap().bytes().await.unwrap();
        assert_eq!(&new_contents, &contents1);

        // rename() copies contents and deletes original
        storage.put(&path1, contents1.clone()).await.unwrap();
        storage.put(&path2, contents2.clone()).await.unwrap();
        storage.rename(&path1, &path2).await.unwrap();
        let new_contents = storage.get(&path2).await.unwrap().bytes().await.unwrap();
        assert_eq!(&new_contents, &contents1);
        let result = storage.get(&path1).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), crate::Error::NotFound { .. }));

        // Clean up
        storage.delete(&path2).await.unwrap();
    }

    pub(crate) async fn copy_if_not_exists(storage: &DynObjectStore) {
        // Create two objects
        let path1 = Path::from("test1");
        let path2 = Path::from("not_exists_nested/test2");
        let contents1 = Bytes::from("cats");
        let contents2 = Bytes::from("dogs");

        // copy_if_not_exists() errors if destination already exists
        storage.put(&path1, contents1.clone()).await.unwrap();
        storage.put(&path2, contents2.clone()).await.unwrap();
        let result = storage.copy_if_not_exists(&path1, &path2).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            crate::Error::AlreadyExists { .. }
        ));

        // copy_if_not_exists() copies contents and allows deleting original
        storage.delete(&path2).await.unwrap();
        storage.copy_if_not_exists(&path1, &path2).await.unwrap();
        storage.delete(&path1).await.unwrap();
        let new_contents = storage.get(&path2).await.unwrap().bytes().await.unwrap();
        assert_eq!(&new_contents, &contents1);
        let result = storage.get(&path1).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), crate::Error::NotFound { .. }));

        // Clean up
        storage.delete(&path2).await.unwrap();
    }

    async fn delete_fixtures(storage: &DynObjectStore) {
        let paths = storage
            .list(None)
            .await
            .unwrap()
            .map_ok(|meta| meta.location)
            .boxed();
        storage
            .delete_stream(paths)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
    }

    /// Test that the returned stream does not borrow the lifetime of Path
    async fn list_store<'a, 'b>(
        store: &'a dyn ObjectStore,
        path_str: &'b str,
    ) -> super::Result<BoxStream<'a, super::Result<ObjectMeta>>> {
        let path = Path::from(path_str);
        store.list(Some(&path)).await
    }

    #[tokio::test]
    async fn test_list_lifetimes() {
        let store = memory::InMemory::new();
        let mut stream = list_store(&store, "path").await.unwrap();
        assert!(stream.next().await.is_none());
    }

    // Tests TODO:
    // GET nonexisting location (in_memory/file)
    // DELETE nonexisting location
    // PUT overwriting
}
