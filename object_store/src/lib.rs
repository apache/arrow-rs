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
//! This crate provides APIs for interacting with object storage services.
//!
//! It currently supports PUT, GET, DELETE, HEAD and list for:
//!
//! * [Google Cloud Storage](https://cloud.google.com/storage/)
//! * [Amazon S3](https://aws.amazon.com/s3/)
//! * [Azure Blob Storage](https://azure.microsoft.com/en-gb/services/storage/blobs/#overview)
//! * In-memory
//! * Local file storage
//!

#[cfg(feature = "aws")]
pub mod aws;
#[cfg(feature = "azure")]
pub mod azure;
#[cfg(feature = "gcp")]
pub mod gcp;
pub mod local;
pub mod memory;
pub mod path;
pub mod throttle;

#[cfg(feature = "gcp")]
mod oauth;

#[cfg(feature = "gcp")]
mod token;

mod util;

use crate::path::Path;
use crate::util::{collect_bytes, maybe_spawn_blocking};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{stream::BoxStream, StreamExt};
use snafu::Snafu;
use std::fmt::{Debug, Formatter};
use std::io::{Read, Seek, SeekFrom};
use std::ops::Range;

/// An alias for a dynamically dispatched object store implementation.
pub type DynObjectStore = dyn ObjectStore;

/// Universal API to multiple object store services.
#[async_trait]
pub trait ObjectStore: std::fmt::Display + Send + Sync + Debug + 'static {
    /// Save the provided bytes to the specified location.
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<()>;

    /// Return the bytes that are stored at the specified location.
    async fn get(&self, location: &Path) -> Result<GetResult>;

    /// Return the bytes that are stored at the specified location
    /// in the given byte range
    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes>;

    /// Return the metadata for the specified location
    async fn head(&self, location: &Path) -> Result<ObjectMeta>;

    /// Delete the object at the specified location.
    async fn delete(&self, location: &Path) -> Result<()>;

    /// List all the objects with the given prefix.
    ///
    /// Prefixes are evaluated on a path segment basis, i.e. `foo/bar/` is a prefix of `foo/bar/x` but not of
    /// `foo/bar_baz/x`.
    async fn list(&self, prefix: Option<&Path>) -> Result<BoxStream<'_, Result<ObjectMeta>>>;

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
    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()>;

    /// Move an object from one path to another in the same object store.
    ///
    /// Will return an error if the destination already has an object.
    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.copy_if_not_exists(from, to).await?;
        self.delete(from).await
    }
}

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
#[derive(Debug, Clone, PartialEq)]
pub struct ObjectMeta {
    /// The full path to the object
    pub location: Path,
    /// The last modified time
    pub last_modified: DateTime<Utc>,
    /// The size in bytes of the object
    pub size: usize,
}

/// Result for a get request
///
/// This special cases the case of a local file, as some systems may
/// be able to optimise the case of a file already present on local disk
pub enum GetResult {
    /// A file and its path on the local filesystem
    File(std::fs::File, std::path::PathBuf),
    /// An asynchronous stream
    Stream(BoxStream<'static, Result<Bytes>>),
}

impl Debug for GetResult {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::File(_, _) => write!(f, "GetResult(File)"),
            Self::Stream(_) => write!(f, "GetResult(Stream)"),
        }
    }
}

impl GetResult {
    /// Collects the data into a [`Bytes`]
    pub async fn bytes(self) -> Result<Bytes> {
        match self {
            Self::File(mut file, path) => {
                maybe_spawn_blocking(move || {
                    let len = file
                        .seek(SeekFrom::End(0))
                        .map_err(|source| local::Error::Seek {
                            source,
                            path: path.clone(),
                        })?;

                    file.seek(SeekFrom::Start(0))
                        .map_err(|source| local::Error::Seek {
                            source,
                            path: path.clone(),
                        })?;

                    let mut buffer = Vec::with_capacity(len as usize);
                    file.read_to_end(&mut buffer)
                        .map_err(|source| local::Error::UnableToReadBytes { source, path })?;

                    Ok(buffer.into())
                })
                .await
            }
            Self::Stream(s) => collect_bytes(s, None).await,
        }
    }

    /// Converts this into a byte stream
    ///
    /// If the result is [`Self::File`] will perform chunked reads of the file, otherwise
    /// will return the [`Self::Stream`].
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
        match self {
            Self::File(file, path) => {
                const CHUNK_SIZE: usize = 8 * 1024;

                futures::stream::try_unfold((file, path, false), |(mut file, path, finished)| {
                    maybe_spawn_blocking(move || {
                        if finished {
                            return Ok(None);
                        }

                        let mut buffer = Vec::with_capacity(CHUNK_SIZE);
                        let read = file
                            .by_ref()
                            .take(CHUNK_SIZE as u64)
                            .read_to_end(&mut buffer)
                            .map_err(|e| local::Error::UnableToReadBytes {
                                source: e,
                                path: path.clone(),
                            })?;

                        Ok(Some((buffer.into(), (file, path, read != CHUNK_SIZE))))
                    })
                })
                .boxed()
            }
            Self::Stream(s) => s,
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

    #[snafu(display("Operation not yet implemented."))]
    NotImplemented,

    #[cfg(feature = "gcp")]
    #[snafu(display("OAuth error: {}", source), context(false))]
    OAuth { source: oauth::Error },
}

#[cfg(test)]
mod test_util {
    use super::*;
    use futures::TryStreamExt;

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

    type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
    type Result<T, E = Error> = std::result::Result<T, E>;

    pub(crate) async fn put_get_delete_list(storage: &DynObjectStore) -> Result<()> {
        let store_str = storage.to_string();

        delete_fixtures(storage).await;

        let content_list = flatten_list_stream(storage, None).await?;
        assert!(
            content_list.is_empty(),
            "Expected list to be empty; found: {:?}",
            content_list
        );

        let location = Path::from("test_dir/test_file.json");

        let data = Bytes::from("arbitrary data");
        let expected_data = data.clone();
        storage.put(&location, data).await?;

        let root = Path::from("/");

        // List everything
        let content_list = flatten_list_stream(storage, None).await?;
        assert_eq!(content_list, &[location.clone()]);

        // Should behave the same as no prefix
        let content_list = flatten_list_stream(storage, Some(&root)).await?;
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

        // List everything starting with a prefix that should return results
        let prefix = Path::from("test_dir");
        let content_list = flatten_list_stream(storage, Some(&prefix)).await?;
        assert_eq!(content_list, &[location.clone()]);

        // List everything starting with a prefix that shouldn't return results
        let prefix = Path::from("something");
        let content_list = flatten_list_stream(storage, Some(&prefix)).await?;
        assert!(content_list.is_empty());

        let read_data = storage.get(&location).await?.bytes().await?;
        assert_eq!(&*read_data, expected_data);

        // Test range request
        let range = 3..7;
        let range_result = storage.get_range(&location, range.clone()).await;

        let out_of_range = 200..300;
        let out_of_range_result = storage.get_range(&location, out_of_range).await;

        if store_str.starts_with("MicrosoftAzureEmulator") {
            // Azurite doesn't support x-ms-range-get-content-crc64 set by Azure SDK
            // https://github.com/Azure/Azurite/issues/444
            let err = range_result.unwrap_err().to_string();
            assert!(err.contains("x-ms-range-get-content-crc64 header or parameter is not supported in Azurite strict mode"), "{}", err);

            let err = out_of_range_result.unwrap_err().to_string();
            assert!(err.contains("x-ms-range-get-content-crc64 header or parameter is not supported in Azurite strict mode"), "{}", err);
        } else {
            let bytes = range_result.unwrap();
            assert_eq!(bytes, expected_data.slice(range));

            // Should be a non-fatal error
            out_of_range_result.unwrap_err();
        }

        let head = storage.head(&location).await?;
        assert_eq!(head.size, expected_data.len());

        storage.delete(&location).await?;

        let content_list = flatten_list_stream(storage, None).await?;
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

        storage.delete(&emoji_file).await.unwrap();
        let files = flatten_list_stream(storage, Some(&emoji_prefix))
            .await
            .unwrap();
        assert!(files.is_empty());

        Ok(())
    }

    pub(crate) async fn list_uses_directories_correctly(storage: &DynObjectStore) -> Result<()> {
        delete_fixtures(storage).await;

        let content_list = flatten_list_stream(storage, None).await?;
        assert!(
            content_list.is_empty(),
            "Expected list to be empty; found: {:?}",
            content_list
        );

        let location1 = Path::from("foo/x.json");
        let location2 = Path::from("foo.bar/y.json");

        let data = Bytes::from("arbitrary data");
        storage.put(&location1, data.clone()).await?;
        storage.put(&location2, data).await?;

        let prefix = Path::from("foo");
        let content_list = flatten_list_stream(storage, Some(&prefix)).await?;
        assert_eq!(content_list, &[location1.clone()]);

        let prefix = Path::from("foo/x");
        let content_list = flatten_list_stream(storage, Some(&prefix)).await?;
        assert_eq!(content_list, &[]);

        Ok(())
    }

    pub(crate) async fn list_with_delimiter(storage: &DynObjectStore) -> Result<()> {
        delete_fixtures(storage).await;

        // ==================== check: store is empty ====================
        let content_list = flatten_list_stream(storage, None).await?;
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
        let content_list = flatten_list_stream(storage, None).await?;
        assert!(content_list.is_empty());

        Ok(())
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

    pub(crate) async fn rename_and_copy(storage: &DynObjectStore) -> Result<()> {
        // Create two objects
        let path1 = Path::from("test1");
        let path2 = Path::from("test2");
        let contents1 = Bytes::from("cats");
        let contents2 = Bytes::from("dogs");

        // copy() make both objects identical
        storage.put(&path1, contents1.clone()).await?;
        storage.put(&path2, contents2.clone()).await?;
        storage.copy(&path1, &path2).await?;
        let new_contents = storage.get(&path2).await?.bytes().await?;
        assert_eq!(&new_contents, &contents1);

        // rename() copies contents and deletes original
        storage.put(&path1, contents1.clone()).await?;
        storage.put(&path2, contents2.clone()).await?;
        storage.rename(&path1, &path2).await?;
        let new_contents = storage.get(&path2).await?.bytes().await?;
        assert_eq!(&new_contents, &contents1);
        let result = storage.get(&path1).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), crate::Error::NotFound { .. }));

        // Clean up
        storage.delete(&path2).await?;

        Ok(())
    }

    pub(crate) async fn copy_if_not_exists(storage: &DynObjectStore) -> Result<()> {
        // Create two objects
        let path1 = Path::from("test1");
        let path2 = Path::from("test2");
        let contents1 = Bytes::from("cats");
        let contents2 = Bytes::from("dogs");

        // copy_if_not_exists() errors if destination already exists
        storage.put(&path1, contents1.clone()).await?;
        storage.put(&path2, contents2.clone()).await?;
        let result = storage.copy_if_not_exists(&path1, &path2).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            crate::Error::AlreadyExists { .. }
        ));

        // copy_if_not_exists() copies contents and allows deleting original
        storage.delete(&path2).await?;
        storage.copy_if_not_exists(&path1, &path2).await?;
        storage.delete(&path1).await?;
        let new_contents = storage.get(&path2).await?.bytes().await?;
        assert_eq!(&new_contents, &contents1);
        let result = storage.get(&path1).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), crate::Error::NotFound { .. }));

        // Clean up
        storage.delete(&path2).await?;

        Ok(())
    }

    async fn delete_fixtures(storage: &DynObjectStore) {
        let paths = flatten_list_stream(storage, None).await.unwrap();

        for f in &paths {
            let _ = storage.delete(f).await;
        }
    }

    /// Test that the returned stream does not borrow the lifetime of Path
    async fn list_store<'a>(
        store: &'a dyn ObjectStore,
        path_str: &str,
    ) -> super::Result<BoxStream<'a, super::Result<ObjectMeta>>> {
        let path = Path::from(path_str);
        store.list(Some(&path)).await
    }

    #[tokio::test]
    async fn test_list_lifetimes() {
        let store = memory::InMemory::new();
        let stream = list_store(&store, "path").await.unwrap();
        assert_eq!(stream.count().await, 0);
    }

    // Tests TODO:
    // GET nonexisting location (in_memory/file)
    // DELETE nonexisting location
    // PUT overwriting
}
