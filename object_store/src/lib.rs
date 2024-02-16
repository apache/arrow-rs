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
//! 1. A high-performance async API focused on providing a consistent interface
//! mirroring that of object stores such as [S3]
//!
//! 2. Production quality, leading this crate to be used in large
//! scale production systems, such as [crates.io] and [InfluxDB IOx]
//!
//! 3. Support for advanced functionality, including atomic, conditional reads
//! and writes, vectored IO, bulk deletion, and more...
//!
//! 4. Stable and predictable governance via the [Apache Arrow] project
//!
//! 5. Small dependency footprint, depending on only a small number of common crates
//!
//! Originally developed by [InfluxData] and subsequently donated
//! to [Apache Arrow].
//!
//! [Apache Arrow]: https://arrow.apache.org/
//! [InfluxData]: https://www.influxdata.com/
//! [crates.io]: https://github.com/rust-lang/crates.io
//! [ACID]: https://en.wikipedia.org/wiki/ACID
//! [S3]: https://aws.amazon.com/s3/
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
    doc = "* [`gcp`]: [Google Cloud Storage](https://cloud.google.com/storage/) support. See [`GoogleCloudStorageBuilder`](gcp::GoogleCloudStorageBuilder)"
)]
#![cfg_attr(
    feature = "aws",
    doc = "* [`aws`]: [Amazon S3](https://aws.amazon.com/s3/). See [`AmazonS3Builder`](aws::AmazonS3Builder)"
)]
#![cfg_attr(
    feature = "azure",
    doc = "* [`azure`]: [Azure Blob Storage](https://azure.microsoft.com/en-gb/services/storage/blobs/). See [`MicrosoftAzureBuilder`](azure::MicrosoftAzureBuilder)"
)]
#![cfg_attr(
    feature = "http",
    doc = "* [`http`]: [HTTP/WebDAV Storage](https://datatracker.ietf.org/doc/html/rfc2518). See [`HttpBuilder`](http::HttpBuilder)"
)]
//!
//! # Why not a Filesystem Interface?
//!
//! Whilst this crate does provide a [`BufReader`], the [`ObjectStore`] interface mirrors the APIs
//! of object stores and not filesystems, opting to provide stateless APIs instead of the cursor
//! based interfaces such as [`Read`] or [`Seek`] favoured by filesystems.
//!
//! This provides some compelling advantages:
//!
//! * All operations are atomic, and readers cannot observe partial and/or failed writes
//! * Methods map directly to object store APIs, providing both efficiency and predictability
//! * Abstracts away filesystem and operating system specific quirks, ensuring portability
//! * Allows for functionality not native to filesystems, such as operation preconditions
//! and atomic multipart uploads
//!
//! [`BufReader`]: buffered::BufReader
//!
//! # Adapters
//!
//! [`ObjectStore`] instances can be composed with various adapters
//! which add additional functionality:
//!
//! * Rate Throttling: [`ThrottleConfig`](throttle::ThrottleConfig)
//! * Concurrent Request Limit: [`LimitStore`](limit::LimitStore)
//!
//! # Configuration System
//!
//! This crate provides a configuration system inspired by the APIs exposed by [fsspec],
//! [PyArrow FileSystem], and [Hadoop FileSystem], allowing creating a [`DynObjectStore`]
//! from a URL and an optional list of key value pairs. This provides a flexible interface
//! to support a wide variety of user-defined store configurations, with minimal additional
//! application complexity.
//!
//! ```no_run
//! # #[cfg(feature = "aws")] {
//! # use url::Url;
//! # use object_store::{parse_url, parse_url_opts};
//! # use object_store::aws::{AmazonS3, AmazonS3Builder};
//! #
//! #
//! // Can manually create a specific store variant using the appropriate builder
//! let store: AmazonS3 = AmazonS3Builder::from_env()
//!     .with_bucket_name("my-bucket").build().unwrap();
//!
//! // Alternatively can create an ObjectStore from an S3 URL
//! let url = Url::parse("s3://bucket/path").unwrap();
//! let (store, path) = parse_url(&url).unwrap();
//! assert_eq!(path.as_ref(), "path");
//!
//! // Potentially with additional options
//! let (store, path) = parse_url_opts(&url, vec![("aws_access_key_id", "...")]).unwrap();
//!
//! // Or with URLs that encode the bucket name in the URL path
//! let url = Url::parse("https://ACCOUNT_ID.r2.cloudflarestorage.com/bucket/path").unwrap();
//! let (store, path) = parse_url(&url).unwrap();
//! assert_eq!(path.as_ref(), "path");
//! # }
//! ```
//!
//! [PyArrow FileSystem]: https://arrow.apache.org/docs/python/generated/pyarrow.fs.FileSystem.html#pyarrow.fs.FileSystem.from_uri
//! [fsspec]: https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.filesystem
//! [Hadoop FileSystem]: https://hadoop.apache.org/docs/r3.0.0/api/org/apache/hadoop/fs/FileSystem.html#get-java.net.URI-org.apache.hadoop.conf.Configuration-
//!
//! # List objects
//!
//! Use the [`ObjectStore::list`] method to iterate over objects in
//! remote storage or files in the local filesystem:
//!
//! ```
//! # use object_store::local::LocalFileSystem;
//! # use std::sync::Arc;
//! # use object_store::{path::Path, ObjectStore};
//! # use futures::stream::StreamExt;
//! # // use LocalFileSystem for example
//! # fn get_object_store() -> Arc<dyn ObjectStore> {
//! #   Arc::new(LocalFileSystem::new())
//! # }
//! #
//! # async fn example() {
//! #
//! // create an ObjectStore
//! let object_store: Arc<dyn ObjectStore> = get_object_store();
//!
//! // Recursively list all files below the 'data' path.
//! // 1. On AWS S3 this would be the 'data/' prefix
//! // 2. On a local filesystem, this would be the 'data' directory
//! let prefix = Path::from("data");
//!
//! // Get an `async` stream of Metadata objects:
//! let mut list_stream = object_store.list(Some(&prefix));
//!
//! // Print a line about each object
//! while let Some(meta) = list_stream.next().await.transpose().unwrap() {
//!     println!("Name: {}, size: {}", meta.location, meta.size);
//! }
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
//! # use futures::TryStreamExt;
//! # use object_store::local::LocalFileSystem;
//! # use std::sync::Arc;
//! #  use bytes::Bytes;
//! # use object_store::{path::Path, ObjectStore, GetResult};
//! # fn get_object_store() -> Arc<dyn ObjectStore> {
//! #   Arc::new(LocalFileSystem::new())
//! # }
//! #
//! # async fn example() {
//! #
//! // Create an ObjectStore
//! let object_store: Arc<dyn ObjectStore> = get_object_store();
//!
//! // Retrieve a specific file
//! let path = Path::from("data/file01.parquet");
//!
//! // Fetch just the file metadata
//! let meta = object_store.head(&path).await.unwrap();
//! println!("{meta:?}");
//!
//! // Fetch the object including metadata
//! let result: GetResult = object_store.get(&path).await.unwrap();
//! assert_eq!(result.meta, meta);
//!
//! // Buffer the entire object in memory
//! let object: Bytes = result.bytes().await.unwrap();
//! assert_eq!(object.len(), meta.size);
//!
//! // Alternatively stream the bytes from object storage
//! let stream = object_store.get(&path).await.unwrap().into_stream();
//!
//! // Count the '0's using `try_fold` from `TryStreamExt` trait
//! let num_zeros = stream
//!     .try_fold(0, |acc, bytes| async move {
//!         Ok(acc + bytes.iter().filter(|b| **b == 0).count())
//!     }).await.unwrap();
//!
//! println!("Num zeros in {} is {}", path, num_zeros);
//! # }
//! ```
//!
//! #  Put Object
//!
//! Use the [`ObjectStore::put`] method to atomically write data.
//!
//! ```
//! # use object_store::local::LocalFileSystem;
//! # use object_store::ObjectStore;
//! # use std::sync::Arc;
//! # use bytes::Bytes;
//! # use object_store::path::Path;
//! # fn get_object_store() -> Arc<dyn ObjectStore> {
//! #   Arc::new(LocalFileSystem::new())
//! # }
//! # async fn put() {
//! #
//! let object_store: Arc<dyn ObjectStore> = get_object_store();
//! let path = Path::from("data/file1");
//! let bytes = Bytes::from_static(b"hello");
//! object_store.put(&path, bytes).await.unwrap();
//! # }
//! ```
//!
//! #  Multipart Upload
//!
//! Use the [`ObjectStore::put_multipart`] method to atomically write a large amount of data,
//! with implementations automatically handling parallel, chunked upload where appropriate.
//!
//! ```
//! # use object_store::local::LocalFileSystem;
//! # use object_store::ObjectStore;
//! # use std::sync::Arc;
//! # use bytes::Bytes;
//! # use tokio::io::AsyncWriteExt;
//! # use object_store::path::Path;
//! # fn get_object_store() -> Arc<dyn ObjectStore> {
//! #   Arc::new(LocalFileSystem::new())
//! # }
//! # async fn multi_upload() {
//! #
//! let object_store: Arc<dyn ObjectStore> = get_object_store();
//! let path = Path::from("data/large_file");
//! let (_id, mut writer) =  object_store.put_multipart(&path).await.unwrap();
//!
//! let bytes = Bytes::from_static(b"hello");
//! writer.write_all(&bytes).await.unwrap();
//! writer.flush().await.unwrap();
//! writer.shutdown().await.unwrap();
//! # }
//! ```
//!
//! # Vectored Read
//!
//! A common pattern, especially when reading structured datasets, is to need to fetch
//! multiple, potentially non-contiguous, ranges of a particular object.
//!
//! [`ObjectStore::get_ranges`] provides an efficient way to perform such vectored IO, and will
//! automatically coalesce adjacent ranges into an appropriate number of parallel requests.
//!
//! ```
//! # use object_store::local::LocalFileSystem;
//! # use object_store::ObjectStore;
//! # use std::sync::Arc;
//! # use bytes::Bytes;
//! # use tokio::io::AsyncWriteExt;
//! # use object_store::path::Path;
//! # fn get_object_store() -> Arc<dyn ObjectStore> {
//! #   Arc::new(LocalFileSystem::new())
//! # }
//! # async fn multi_upload() {
//! #
//! let object_store: Arc<dyn ObjectStore> = get_object_store();
//! let path = Path::from("data/large_file");
//! let ranges = object_store.get_ranges(&path, &[90..100, 400..600, 0..10]).await.unwrap();
//! assert_eq!(ranges.len(), 3);
//! assert_eq!(ranges[0].len(), 10);
//! # }
//! ```
//!
//! # Conditional Fetch
//!
//! More complex object retrieval can be supported by [`ObjectStore::get_opts`].
//!
//! For example, efficiently refreshing a cache without re-fetching the entire object
//! data if the object hasn't been modified.
//!
//! ```
//! # use std::collections::btree_map::Entry;
//! # use std::collections::HashMap;
//! # use object_store::{GetOptions, GetResult, ObjectStore, Result, Error};
//! # use std::sync::Arc;
//! # use std::time::{Duration, Instant};
//! # use bytes::Bytes;
//! # use tokio::io::AsyncWriteExt;
//! # use object_store::path::Path;
//! struct CacheEntry {
//!     /// Data returned by last request
//!     data: Bytes,
//!     /// ETag identifying the object returned by the server
//!     e_tag: String,
//!     /// Instant of last refresh
//!     refreshed_at: Instant,
//! }
//!
//! /// Example cache that checks entries after 10 seconds for a new version
//! struct Cache {
//!     entries: HashMap<Path, CacheEntry>,
//!     store: Arc<dyn ObjectStore>,
//! }
//!
//! impl Cache {
//!     pub async fn get(&mut self, path: &Path) -> Result<Bytes> {
//!         Ok(match self.entries.get_mut(path) {
//!             Some(e) => match e.refreshed_at.elapsed() < Duration::from_secs(10) {
//!                 true => e.data.clone(), // Return cached data
//!                 false => { // Check if remote version has changed
//!                     let opts = GetOptions {
//!                         if_none_match: Some(e.e_tag.clone()),
//!                         ..GetOptions::default()
//!                     };
//!                     match self.store.get_opts(&path, opts).await {
//!                         Ok(d) => e.data = d.bytes().await?,
//!                         Err(Error::NotModified { .. }) => {} // Data has not changed
//!                         Err(e) => return Err(e),
//!                     };
//!                     e.refreshed_at = Instant::now();
//!                     e.data.clone()
//!                 }
//!             },
//!             None => { // Not cached, fetch data
//!                 let get = self.store.get(&path).await?;
//!                 let e_tag = get.meta.e_tag.clone();
//!                 let data = get.bytes().await?;
//!                 if let Some(e_tag) = e_tag {
//!                     let entry = CacheEntry {
//!                         e_tag,
//!                         data: data.clone(),
//!                         refreshed_at: Instant::now(),
//!                     };
//!                     self.entries.insert(path.clone(), entry);
//!                 }
//!                 data
//!             }
//!         })
//!     }
//! }
//! ```
//!
//! # Conditional Put
//!
//! The default behaviour when writing data is to upsert any existing object at the given path,
//! overwriting any previous value. More complex behaviours can be achieved using [`PutMode`], and
//! can be used to build [Optimistic Concurrency Control] based transactions. This facilitates
//! building metadata catalogs, such as [Apache Iceberg] or [Delta Lake], directly on top of object
//! storage, without relying on a separate DBMS.
//!
//! ```
//! # use object_store::{Error, ObjectStore, PutMode, UpdateVersion};
//! # use std::sync::Arc;
//! # use bytes::Bytes;
//! # use tokio::io::AsyncWriteExt;
//! # use object_store::memory::InMemory;
//! # use object_store::path::Path;
//! # fn get_object_store() -> Arc<dyn ObjectStore> {
//! #   Arc::new(InMemory::new())
//! # }
//! # fn do_update(b: Bytes) -> Bytes {b}
//! # async fn conditional_put() {
//! let store = get_object_store();
//! let path = Path::from("test");
//!
//! // Perform a conditional update on path
//! loop {
//!     // Perform get request
//!     let r = store.get(&path).await.unwrap();
//!
//!     // Save version information fetched
//!     let version = UpdateVersion {
//!         e_tag: r.meta.e_tag.clone(),
//!         version: r.meta.version.clone(),
//!     };
//!
//!     // Compute new version of object contents
//!     let new = do_update(r.bytes().await.unwrap());
//!
//!     // Attempt to commit transaction
//!     match store.put_opts(&path, new, PutMode::Update(version).into()).await {
//!         Ok(_) => break, // Successfully committed
//!         Err(Error::Precondition { .. }) => continue, // Object has changed, try again
//!         Err(e) => panic!("{e}")
//!     }
//! }
//! # }
//! ```
//!
//! [Optimistic Concurrency Control]: https://en.wikipedia.org/wiki/Optimistic_concurrency_control
//! [Apache Iceberg]: https://iceberg.apache.org/
//! [Delta Lake]: https://delta.io/
//!
//! # TLS Certificates
//!
//! Stores that use HTTPS/TLS (this is true for most cloud stores) can choose the source of their [CA]
//! certificates. By default the system-bundled certificates are used (see
//! [`rustls-native-certs`]). The `tls-webpki-roots` feature switch can be used to also bundle Mozilla's
//! root certificates with the library/application (see [`webpki-roots`]).
//!
//! [CA]: https://en.wikipedia.org/wiki/Certificate_authority
//! [`rustls-native-certs`]: https://crates.io/crates/rustls-native-certs/
//! [`webpki-roots`]: https://crates.io/crates/webpki-roots
//!

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
#[cfg(feature = "cloud")]
pub mod signer;
pub mod throttle;

#[cfg(feature = "cloud")]
mod client;

#[cfg(feature = "cloud")]
pub use client::{
    backoff::BackoffConfig, retry::RetryConfig, ClientConfigKey, ClientOptions, CredentialProvider,
    StaticCredentialProvider,
};

#[cfg(feature = "cloud")]
mod config;

mod tags;

pub use tags::TagSet;

pub mod multipart;
mod parse;
mod util;

pub use parse::{parse_url, parse_url_opts};
pub use util::GetRange;

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
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<PutResult> {
        self.put_opts(location, bytes, PutOptions::default()).await
    }

    /// Save the provided bytes to the specified location with the given options
    async fn put_opts(&self, location: &Path, bytes: Bytes, opts: PutOptions) -> Result<PutResult>;

    /// Get a multi-part upload that allows writing data in chunks.
    ///
    /// Most cloud-based uploads will buffer and upload parts in parallel.
    ///
    /// To complete the upload, [AsyncWrite::poll_shutdown] must be called
    /// to completion. This operation is guaranteed to be atomic, it will either
    /// make all the written data available at `location`, or fail. No clients
    /// should be able to observe a partially written object.
    ///
    /// For some object stores (S3, GCS, and local in particular), if the
    /// writer fails or panics, you must call [ObjectStore::abort_multipart]
    /// to clean up partially written data.
    ///
    /// <div class="warning">
    /// It is recommended applications wait for any in-flight requests to complete by calling `flush`, if
    /// there may be a significant gap in time (> ~30s) before the next write.
    /// These gaps can include times where the function returns control to the
    /// caller while keeping the writer open. If `flush` is not called, futures
    /// for in-flight requests may be left unpolled long enough for the requests
    /// to time out, causing the write to fail.
    /// </div>
    ///
    /// For applications requiring fine-grained control of multipart uploads
    /// see [`MultiPartStore`], although note that this interface cannot be
    /// supported by all [`ObjectStore`] backends.
    ///
    /// For applications looking to implement this interface for a custom
    /// multipart API, see [`WriteMultiPart`] which handles the complexities
    /// of performing parallel uploads of fixed size parts.
    ///
    /// [`WriteMultiPart`]: multipart::WriteMultiPart
    /// [`MultiPartStore`]: multipart::MultiPartStore
    async fn put_multipart(
        &self,
        location: &Path,
    ) -> Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)>;

    /// Cleanup an aborted upload.
    ///
    /// See documentation for individual stores for exact behavior, as capabilities
    /// vary by object store.
    async fn abort_multipart(&self, location: &Path, multipart_id: &MultipartId) -> Result<()>;

    /// Return the bytes that are stored at the specified location.
    async fn get(&self, location: &Path) -> Result<GetResult> {
        self.get_opts(location, GetOptions::default()).await
    }

    /// Perform a get request with options
    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult>;

    /// Return the bytes that are stored at the specified location
    /// in the given byte range.
    ///
    /// See [`GetRange::Bounded`] for more details on how `range` gets interpreted
    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
        let options = GetOptions {
            range: Some(range.into()),
            ..Default::default()
        };
        self.get_opts(location, options).await?.bytes().await
    }

    /// Return the bytes that are stored at the specified location
    /// in the given byte ranges
    async fn get_ranges(&self, location: &Path, ranges: &[Range<usize>]) -> Result<Vec<Bytes>> {
        coalesce_ranges(
            ranges,
            |range| self.get_range(location, range),
            OBJECT_STORE_COALESCE_DEFAULT,
        )
        .await
    }

    /// Return the metadata for the specified location
    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        let options = GetOptions {
            head: true,
            ..Default::default()
        };
        Ok(self.get_opts(location, options).await?.meta)
    }

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
    /// # use futures::{StreamExt, TryStreamExt};
    /// # use object_store::local::LocalFileSystem;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let root = tempfile::TempDir::new().unwrap();
    /// # let store = LocalFileSystem::new_with_prefix(root.path()).unwrap();
    /// # use object_store::{ObjectStore, ObjectMeta};
    /// # use object_store::path::Path;
    /// # use futures::{StreamExt, TryStreamExt};
    /// # use bytes::Bytes;
    /// #
    /// // Create two objects
    /// store.put(&Path::from("foo"), Bytes::from("foo")).await?;
    /// store.put(&Path::from("bar"), Bytes::from("bar")).await?;
    ///
    /// // List object
    /// let locations = store.list(None).map_ok(|m| m.location).boxed();
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
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, Result<ObjectMeta>>;

    /// List all the objects with the given prefix and a location greater than `offset`
    ///
    /// Some stores, such as S3 and GCS, may be able to push `offset` down to reduce
    /// the number of network requests required
    ///
    /// Note: the order of returned [`ObjectMeta`] is not guaranteed
    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'_, Result<ObjectMeta>> {
        let offset = offset.clone();
        self.list(prefix)
            .try_filter(move |f| futures::future::ready(f.location > offset))
            .boxed()
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
            async fn put(&self, location: &Path, bytes: Bytes) -> Result<PutResult> {
                self.as_ref().put(location, bytes).await
            }

            async fn put_opts(
                &self,
                location: &Path,
                bytes: Bytes,
                opts: PutOptions,
            ) -> Result<PutResult> {
                self.as_ref().put_opts(location, bytes, opts).await
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

            async fn get(&self, location: &Path) -> Result<GetResult> {
                self.as_ref().get(location).await
            }

            async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
                self.as_ref().get_opts(location, options).await
            }

            async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
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

            fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, Result<ObjectMeta>> {
                self.as_ref().list(prefix)
            }

            fn list_with_offset(
                &self,
                prefix: Option<&Path>,
                offset: &Path,
            ) -> BoxStream<'_, Result<ObjectMeta>> {
                self.as_ref().list_with_offset(prefix, offset)
            }

            async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
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
    ///
    /// <https://datatracker.ietf.org/doc/html/rfc9110#name-etag>
    pub e_tag: Option<String>,
    /// A version indicator for this object
    pub version: Option<String>,
}

/// Options for a get request, such as range
#[derive(Debug, Default)]
pub struct GetOptions {
    /// Request will succeed if the `ObjectMeta::e_tag` matches
    /// otherwise returning [`Error::Precondition`]
    ///
    /// See <https://datatracker.ietf.org/doc/html/rfc9110#name-if-match>
    ///
    /// Examples:
    ///
    /// ```text
    /// If-Match: "xyzzy"
    /// If-Match: "xyzzy", "r2d2xxxx", "c3piozzzz"
    /// If-Match: *
    /// ```
    pub if_match: Option<String>,
    /// Request will succeed if the `ObjectMeta::e_tag` does not match
    /// otherwise returning [`Error::NotModified`]
    ///
    /// See <https://datatracker.ietf.org/doc/html/rfc9110#section-13.1.2>
    ///
    /// Examples:
    ///
    /// ```text
    /// If-None-Match: "xyzzy"
    /// If-None-Match: "xyzzy", "r2d2xxxx", "c3piozzzz"
    /// If-None-Match: *
    /// ```
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
    pub range: Option<GetRange>,
    /// Request a particular object version
    pub version: Option<String>,
    /// Request transfer of no content
    ///
    /// <https://datatracker.ietf.org/doc/html/rfc9110#name-head>
    pub head: bool,
}

impl GetOptions {
    /// Returns an error if the modification conditions on this request are not satisfied
    ///
    /// <https://datatracker.ietf.org/doc/html/rfc7232#section-6>
    fn check_preconditions(&self, meta: &ObjectMeta) -> Result<()> {
        // The use of the invalid etag "*" means no ETag is equivalent to never matching
        let etag = meta.e_tag.as_deref().unwrap_or("*");
        let last_modified = meta.last_modified;

        if let Some(m) = &self.if_match {
            if m != "*" && m.split(',').map(str::trim).all(|x| x != etag) {
                return Err(Error::Precondition {
                    path: meta.location.to_string(),
                    source: format!("{etag} does not match {m}").into(),
                });
            }
        } else if let Some(date) = self.if_unmodified_since {
            if last_modified > date {
                return Err(Error::Precondition {
                    path: meta.location.to_string(),
                    source: format!("{date} < {last_modified}").into(),
                });
            }
        }

        if let Some(m) = &self.if_none_match {
            if m == "*" || m.split(',').map(str::trim).any(|x| x == etag) {
                return Err(Error::NotModified {
                    path: meta.location.to_string(),
                    source: format!("{etag} matches {m}").into(),
                });
            }
        } else if let Some(date) = self.if_modified_since {
            if last_modified <= date {
                return Err(Error::NotModified {
                    path: meta.location.to_string(),
                    source: format!("{date} >= {last_modified}").into(),
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
                    file.seek(SeekFrom::Start(self.range.start as _))
                        .map_err(|source| local::Error::Seek {
                            source,
                            path: path.clone(),
                        })?;

                    let mut buffer = Vec::with_capacity(len);
                    file.take(len as _)
                        .read_to_end(&mut buffer)
                        .map_err(|source| local::Error::UnableToReadBytes { source, path })?;

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

/// Configure preconditions for the put operation
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum PutMode {
    /// Perform an atomic write operation, overwriting any object present at the provided path
    #[default]
    Overwrite,
    /// Perform an atomic write operation, returning [`Error::AlreadyExists`] if an
    /// object already exists at the provided path
    Create,
    /// Perform an atomic write operation if the current version of the object matches the
    /// provided [`UpdateVersion`], returning [`Error::Precondition`] otherwise
    Update(UpdateVersion),
}

/// Uniquely identifies a version of an object to update
///
/// Stores will use differing combinations of `e_tag` and `version` to provide conditional
/// updates, and it is therefore recommended applications preserve both
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateVersion {
    /// The unique identifier for the newly created object
    ///
    /// <https://datatracker.ietf.org/doc/html/rfc9110#name-etag>
    pub e_tag: Option<String>,
    /// A version indicator for the newly created object
    pub version: Option<String>,
}

impl From<PutResult> for UpdateVersion {
    fn from(value: PutResult) -> Self {
        Self {
            e_tag: value.e_tag,
            version: value.version,
        }
    }
}

/// Options for a put request
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PutOptions {
    /// Configure the [`PutMode`] for this operation
    pub mode: PutMode,
    /// Provide a [`TagSet`] for this object
    ///
    /// Implementations that don't support object tagging should ignore this
    pub tags: TagSet,
}

impl From<PutMode> for PutOptions {
    fn from(mode: PutMode) -> Self {
        Self {
            mode,
            ..Default::default()
        }
    }
}

impl From<TagSet> for PutOptions {
    fn from(tags: TagSet) -> Self {
        Self {
            tags,
            ..Default::default()
        }
    }
}

/// Result for a put request
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutResult {
    /// The unique identifier for the newly created object
    ///
    /// <https://datatracker.ietf.org/doc/html/rfc9110#name-etag>
    pub e_tag: Option<String>,
    /// A version indicator for the newly created object
    pub version: Option<String>,
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

    #[snafu(display("Configuration key: '{}' is not valid for store '{}'.", key, store))]
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
            .map_ok(|meta| meta.location)
            .try_collect::<Vec<Path>>()
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::multipart::MultiPartStore;
    use crate::test_util::flatten_list_stream;
    use chrono::TimeZone;
    use futures::stream::FuturesUnordered;
    use rand::distributions::Alphanumeric;
    use rand::{thread_rng, Rng};
    use std::future::Future;
    use tokio::io::AsyncWriteExt;

    pub(crate) async fn put_get_delete_list(storage: &DynObjectStore) {
        put_get_delete_list_opts(storage).await
    }

    pub(crate) async fn put_get_delete_list_opts(storage: &DynObjectStore) {
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

        let bytes = range_result.unwrap();
        assert_eq!(bytes, expected_data.slice(range.clone()));

        let opts = GetOptions {
            range: Some(GetRange::Bounded(2..5)),
            ..Default::default()
        };
        let result = storage.get_opts(&location, opts).await.unwrap();
        // Data is `"arbitrary data"`, length 14 bytes
        assert_eq!(result.meta.size, 14); // Should return full object size (#5272)
        assert_eq!(result.range, 2..5);
        let bytes = result.bytes().await.unwrap();
        assert_eq!(bytes, b"bit".as_ref());

        let out_of_range = 200..300;
        let out_of_range_result = storage.get_range(&location, out_of_range).await;

        // Should be a non-fatal error
        out_of_range_result.unwrap_err();

        let opts = GetOptions {
            range: Some(GetRange::Bounded(2..100)),
            ..Default::default()
        };
        let result = storage.get_opts(&location, opts).await.unwrap();
        assert_eq!(result.range, 2..14);
        assert_eq!(result.meta.size, 14);
        let bytes = result.bytes().await.unwrap();
        assert_eq!(bytes, b"bitrary data".as_ref());

        let opts = GetOptions {
            range: Some(GetRange::Suffix(2)),
            ..Default::default()
        };
        match storage.get_opts(&location, opts).await {
            Ok(result) => {
                assert_eq!(result.range, 12..14);
                assert_eq!(result.meta.size, 14);
                let bytes = result.bytes().await.unwrap();
                assert_eq!(bytes, b"ta".as_ref());
            }
            Err(Error::NotSupported { .. }) => {}
            Err(e) => panic!("{e}"),
        }

        let opts = GetOptions {
            range: Some(GetRange::Suffix(100)),
            ..Default::default()
        };
        match storage.get_opts(&location, opts).await {
            Ok(result) => {
                assert_eq!(result.range, 0..14);
                assert_eq!(result.meta.size, 14);
                let bytes = result.bytes().await.unwrap();
                assert_eq!(bytes, b"arbitrary data".as_ref());
            }
            Err(Error::NotSupported { .. }) => {}
            Err(e) => panic!("{e}"),
        }

        let opts = GetOptions {
            range: Some(GetRange::Offset(3)),
            ..Default::default()
        };
        let result = storage.get_opts(&location, opts).await.unwrap();
        assert_eq!(result.range, 3..14);
        assert_eq!(result.meta.size, 14);
        let bytes = result.bytes().await.unwrap();
        assert_eq!(bytes, b"itrary data".as_ref());

        let opts = GetOptions {
            range: Some(GetRange::Offset(100)),
            ..Default::default()
        };
        storage.get_opts(&location, opts).await.unwrap_err();

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

        // Test handling of unicode paths
        let path = Path::parse("🇦🇺/$shenanigans@@~.txt").unwrap();
        storage.put(&path, "test".into()).await.unwrap();

        let r = storage.get(&path).await.unwrap();
        assert_eq!(r.bytes().await.unwrap(), "test");

        let dir = Path::parse("🇦🇺").unwrap();
        let r = storage.list_with_delimiter(None).await.unwrap();
        assert!(r.common_prefixes.contains(&dir));

        let r = storage.list_with_delimiter(Some(&dir)).await.unwrap();
        assert_eq!(r.objects.len(), 1);
        assert_eq!(r.objects[0].location, path);

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

        let files = flatten_list_stream(storage, Some(&Path::from("foo bar")))
            .await
            .unwrap();
        assert_eq!(files, vec![path.clone()]);

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
            let s = storage.list_with_offset(prefix.as_ref(), &offset);
            let mut actual: Vec<_> = s.map_ok(|x| x.location).try_collect().await.unwrap();

            actual.sort_unstable();

            let expected: Vec<_> = files
                .iter()
                .filter(|x| {
                    let prefix_match = prefix.as_ref().map(|p| x.prefix_matches(p)).unwrap_or(true);
                    prefix_match && *x > &offset
                })
                .cloned()
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

        let tag = meta.e_tag.unwrap();
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

        let result = storage.put(&path, "test".into()).await.unwrap();
        let new_tag = result.e_tag.unwrap();
        assert_ne!(tag, new_tag);

        let meta = storage.head(&path).await.unwrap();
        assert_eq!(meta.e_tag.unwrap(), new_tag);

        let options = GetOptions {
            if_match: Some(new_tag),
            ..GetOptions::default()
        };
        storage.get_opts(&path, options).await.unwrap();

        let options = GetOptions {
            if_match: Some(tag),
            ..GetOptions::default()
        };
        let err = storage.get_opts(&path, options).await.unwrap_err();
        assert!(matches!(err, Error::Precondition { .. }), "{err}");

        if let Some(version) = meta.version {
            storage.put(&path, "bar".into()).await.unwrap();

            let options = GetOptions {
                version: Some(version),
                ..GetOptions::default()
            };

            // Can retrieve previous version
            let get_opts = storage.get_opts(&path, options).await.unwrap();
            let old = get_opts.bytes().await.unwrap();
            assert_eq!(old, b"test".as_slice());

            // Current version contains the updated data
            let current = storage.get(&path).await.unwrap().bytes().await.unwrap();
            assert_eq!(&current, b"bar".as_slice());
        }
    }

    pub(crate) async fn put_opts(storage: &dyn ObjectStore, supports_update: bool) {
        // When using DynamoCommit repeated runs of this test will produce the same sequence of records in DynamoDB
        // As a result each conditional operation will need to wait for the lease to timeout before proceeding
        // One solution would be to clear DynamoDB before each test, but this would require non-trivial additional code
        // so we instead just generate a random suffix for the filenames
        let rng = thread_rng();
        let suffix = String::from_utf8(rng.sample_iter(Alphanumeric).take(32).collect()).unwrap();

        delete_fixtures(storage).await;
        let path = Path::from(format!("put_opts_{suffix}"));
        let v1 = storage
            .put_opts(&path, "a".into(), PutMode::Create.into())
            .await
            .unwrap();

        let err = storage
            .put_opts(&path, "b".into(), PutMode::Create.into())
            .await
            .unwrap_err();
        assert!(matches!(err, Error::AlreadyExists { .. }), "{err}");

        let b = storage.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(b.as_ref(), b"a");

        if !supports_update {
            return;
        }

        let v2 = storage
            .put_opts(&path, "c".into(), PutMode::Update(v1.clone().into()).into())
            .await
            .unwrap();

        let b = storage.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(b.as_ref(), b"c");

        let err = storage
            .put_opts(&path, "d".into(), PutMode::Update(v1.into()).into())
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Precondition { .. }), "{err}");

        storage
            .put_opts(&path, "e".into(), PutMode::Update(v2.clone().into()).into())
            .await
            .unwrap();

        let b = storage.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(b.as_ref(), b"e");

        // Update not exists
        let path = Path::from("I don't exist");
        let err = storage
            .put_opts(&path, "e".into(), PutMode::Update(v2.into()).into())
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Precondition { .. }), "{err}");

        const NUM_WORKERS: usize = 5;
        const NUM_INCREMENTS: usize = 10;

        let path = Path::from(format!("RACE-{suffix}"));
        let mut futures: FuturesUnordered<_> = (0..NUM_WORKERS)
            .map(|_| async {
                for _ in 0..NUM_INCREMENTS {
                    loop {
                        match storage.get(&path).await {
                            Ok(r) => {
                                let mode = PutMode::Update(UpdateVersion {
                                    e_tag: r.meta.e_tag.clone(),
                                    version: r.meta.version.clone(),
                                });

                                let b = r.bytes().await.unwrap();
                                let v: usize = std::str::from_utf8(&b).unwrap().parse().unwrap();
                                let new = (v + 1).to_string();

                                match storage.put_opts(&path, new.into(), mode.into()).await {
                                    Ok(_) => break,
                                    Err(Error::Precondition { .. }) => continue,
                                    Err(e) => return Err(e),
                                }
                            }
                            Err(Error::NotFound { .. }) => {
                                let mode = PutMode::Create;
                                match storage.put_opts(&path, "1".into(), mode.into()).await {
                                    Ok(_) => break,
                                    Err(Error::AlreadyExists { .. }) => continue,
                                    Err(e) => return Err(e),
                                }
                            }
                            Err(e) => return Err(e),
                        }
                    }
                }
                Ok(())
            })
            .collect();

        while futures.next().await.transpose().unwrap().is_some() {}
        let b = storage.get(&path).await.unwrap().bytes().await.unwrap();
        let v = std::str::from_utf8(&b).unwrap().parse::<usize>().unwrap();
        assert_eq!(v, NUM_WORKERS * NUM_INCREMENTS);
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
        if let Some(chunk) = data.first() {
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
        let location = location.unwrap_or_else(|| Path::from("this_file_should_not_exist"));

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

    pub(crate) async fn multipart(storage: &dyn ObjectStore, multipart: &dyn MultiPartStore) {
        let path = Path::from("test_multipart");
        let chunk_size = 5 * 1024 * 1024;

        let chunks = get_chunks(chunk_size, 2);

        let id = multipart.create_multipart(&path).await.unwrap();

        let parts: Vec<_> = futures::stream::iter(chunks)
            .enumerate()
            .map(|(idx, b)| multipart.put_part(&path, &id, idx, b))
            .buffered(2)
            .try_collect()
            .await
            .unwrap();

        multipart
            .complete_multipart(&path, &id, parts)
            .await
            .unwrap();

        let meta = storage.head(&path).await.unwrap();
        assert_eq!(meta.size, chunk_size * 2);

        // Empty case
        let path = Path::from("test_empty_multipart");

        let id = multipart.create_multipart(&path).await.unwrap();

        let parts = vec![];

        multipart
            .complete_multipart(&path, &id, parts)
            .await
            .unwrap();

        let meta = storage.head(&path).await.unwrap();
        assert_eq!(meta.size, 0);
    }

    #[cfg(any(feature = "azure", feature = "aws"))]
    pub(crate) async fn signing<T>(integration: &T)
    where
        T: ObjectStore + crate::signer::Signer,
    {
        use reqwest::Method;
        use std::time::Duration;

        let data = Bytes::from("hello world");
        let path = Path::from("file.txt");
        integration.put(&path, data.clone()).await.unwrap();

        let signed = integration
            .signed_url(Method::GET, &path, Duration::from_secs(60))
            .await
            .unwrap();

        let resp = reqwest::get(signed).await.unwrap();
        let loaded = resp.bytes().await.unwrap();

        assert_eq!(data, loaded);
    }

    #[cfg(any(feature = "aws", feature = "azure"))]
    pub(crate) async fn tagging<F, Fut>(storage: &dyn ObjectStore, validate: bool, get_tags: F)
    where
        F: Fn(Path) -> Fut + Send + Sync,
        Fut: Future<Output = Result<reqwest::Response>> + Send,
    {
        use bytes::Buf;
        use serde::Deserialize;

        #[derive(Deserialize)]
        struct Tagging {
            #[serde(rename = "TagSet")]
            list: TagList,
        }

        #[derive(Debug, Deserialize)]
        struct TagList {
            #[serde(rename = "Tag")]
            tags: Vec<Tag>,
        }

        #[derive(Debug, Deserialize, Eq, PartialEq)]
        #[serde(rename_all = "PascalCase")]
        struct Tag {
            key: String,
            value: String,
        }

        let tags = vec![
            Tag {
                key: "foo.com=bar/s".to_string(),
                value: "bananas/foo.com-_".to_string(),
            },
            Tag {
                key: "namespace/key.foo".to_string(),
                value: "value with a space".to_string(),
            },
        ];
        let mut tag_set = TagSet::default();
        for t in &tags {
            tag_set.push(&t.key, &t.value)
        }

        let path = Path::from("tag_test");
        storage
            .put_opts(&path, "test".into(), tag_set.into())
            .await
            .unwrap();

        // Write should always succeed, but certain configurations may simply ignore tags
        if !validate {
            return;
        }

        let resp = get_tags(path.clone()).await.unwrap();
        let body = resp.bytes().await.unwrap();

        let mut resp: Tagging = quick_xml::de::from_reader(body.reader()).unwrap();
        resp.list.tags.sort_by(|a, b| a.key.cmp(&b.key));
        assert_eq!(resp.list.tags, tags);
    }

    async fn delete_fixtures(storage: &DynObjectStore) {
        let paths = storage.list(None).map_ok(|meta| meta.location).boxed();
        storage
            .delete_stream(paths)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
    }

    /// Test that the returned stream does not borrow the lifetime of Path
    fn list_store<'a>(
        store: &'a dyn ObjectStore,
        path_str: &str,
    ) -> BoxStream<'a, Result<ObjectMeta>> {
        let path = Path::from(path_str);
        store.list(Some(&path))
    }

    #[tokio::test]
    async fn test_list_lifetimes() {
        let store = memory::InMemory::new();
        let mut stream = list_store(&store, "path");
        assert!(stream.next().await.is_none());
    }

    #[test]
    fn test_preconditions() {
        let mut meta = ObjectMeta {
            location: Path::from("test"),
            last_modified: Utc.timestamp_nanos(100),
            size: 100,
            e_tag: Some("123".to_string()),
            version: None,
        };

        let mut options = GetOptions::default();
        options.check_preconditions(&meta).unwrap();

        options.if_modified_since = Some(Utc.timestamp_nanos(50));
        options.check_preconditions(&meta).unwrap();

        options.if_modified_since = Some(Utc.timestamp_nanos(100));
        options.check_preconditions(&meta).unwrap_err();

        options.if_modified_since = Some(Utc.timestamp_nanos(101));
        options.check_preconditions(&meta).unwrap_err();

        options = GetOptions::default();

        options.if_unmodified_since = Some(Utc.timestamp_nanos(50));
        options.check_preconditions(&meta).unwrap_err();

        options.if_unmodified_since = Some(Utc.timestamp_nanos(100));
        options.check_preconditions(&meta).unwrap();

        options.if_unmodified_since = Some(Utc.timestamp_nanos(101));
        options.check_preconditions(&meta).unwrap();

        options = GetOptions::default();

        options.if_match = Some("123".to_string());
        options.check_preconditions(&meta).unwrap();

        options.if_match = Some("123,354".to_string());
        options.check_preconditions(&meta).unwrap();

        options.if_match = Some("354, 123,".to_string());
        options.check_preconditions(&meta).unwrap();

        options.if_match = Some("354".to_string());
        options.check_preconditions(&meta).unwrap_err();

        options.if_match = Some("*".to_string());
        options.check_preconditions(&meta).unwrap();

        // If-Match takes precedence
        options.if_unmodified_since = Some(Utc.timestamp_nanos(200));
        options.check_preconditions(&meta).unwrap();

        options = GetOptions::default();

        options.if_none_match = Some("123".to_string());
        options.check_preconditions(&meta).unwrap_err();

        options.if_none_match = Some("*".to_string());
        options.check_preconditions(&meta).unwrap_err();

        options.if_none_match = Some("1232".to_string());
        options.check_preconditions(&meta).unwrap();

        options.if_none_match = Some("23, 123".to_string());
        options.check_preconditions(&meta).unwrap_err();

        // If-None-Match takes precedence
        options.if_modified_since = Some(Utc.timestamp_nanos(10));
        options.check_preconditions(&meta).unwrap_err();

        // Check missing ETag
        meta.e_tag = None;
        options = GetOptions::default();

        options.if_none_match = Some("*".to_string()); // Fails if any file exists
        options.check_preconditions(&meta).unwrap_err();

        options = GetOptions::default();
        options.if_match = Some("*".to_string()); // Passes if file exists
        options.check_preconditions(&meta).unwrap();
    }
}
