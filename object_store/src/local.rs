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

//! An object store implementation for a local filesystem
use crate::{
    maybe_spawn_blocking,
    path::{filesystem_path_to_url, Path},
    GetResult, ListResult, ObjectMeta, ObjectStore, Result,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{stream::BoxStream, StreamExt};
use snafu::{ensure, OptionExt, ResultExt, Snafu};
use std::collections::VecDeque;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::sync::Arc;
use std::{collections::BTreeSet, convert::TryFrom, io};
use url::Url;
use walkdir::{DirEntry, WalkDir};

/// A specialized `Error` for filesystem object store-related errors
#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub(crate) enum Error {
    #[snafu(display("File size for {} did not fit in a usize: {}", path, source))]
    FileSizeOverflowedUsize {
        source: std::num::TryFromIntError,
        path: String,
    },

    #[snafu(display("Unable to walk dir: {}", source))]
    UnableToWalkDir {
        source: walkdir::Error,
    },

    #[snafu(display("Unable to access metadata for {}: {}", path, source))]
    UnableToAccessMetadata {
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
        path: String,
    },

    #[snafu(display("Unable to copy data to file: {}", source))]
    UnableToCopyDataToFile {
        source: io::Error,
    },

    #[snafu(display("Unable to create dir {}: {}", path.display(), source))]
    UnableToCreateDir {
        source: io::Error,
        path: std::path::PathBuf,
    },

    #[snafu(display("Unable to create file {}: {}", path.display(), err))]
    UnableToCreateFile {
        path: std::path::PathBuf,
        err: io::Error,
    },

    #[snafu(display("Unable to delete file {}: {}", path.display(), source))]
    UnableToDeleteFile {
        source: io::Error,
        path: std::path::PathBuf,
    },

    #[snafu(display("Unable to open file {}: {}", path.display(), source))]
    UnableToOpenFile {
        source: io::Error,
        path: std::path::PathBuf,
    },

    #[snafu(display("Unable to read data from file {}: {}", path.display(), source))]
    UnableToReadBytes {
        source: io::Error,
        path: std::path::PathBuf,
    },

    #[snafu(display("Out of range of file {}, expected: {}, actual: {}", path.display(), expected, actual))]
    OutOfRange {
        path: std::path::PathBuf,
        expected: usize,
        actual: usize,
    },

    #[snafu(display("Unable to copy file from {} to {}: {}", from.display(), to.display(), source))]
    UnableToCopyFile {
        from: std::path::PathBuf,
        to: std::path::PathBuf,
        source: io::Error,
    },

    NotFound {
        path: std::path::PathBuf,
        source: io::Error,
    },

    #[snafu(display("Error seeking file {}: {}", path.display(), source))]
    Seek {
        source: io::Error,
        path: std::path::PathBuf,
    },

    #[snafu(display("Unable to convert URL \"{}\" to filesystem path", url))]
    InvalidUrl {
        url: Url,
    },

    AlreadyExists {
        path: String,
        source: io::Error,
    },
}

impl From<Error> for super::Error {
    fn from(source: Error) -> Self {
        match source {
            Error::NotFound { path, source } => Self::NotFound {
                path: path.to_string_lossy().to_string(),
                source: source.into(),
            },
            Error::AlreadyExists { path, source } => Self::AlreadyExists {
                path,
                source: source.into(),
            },
            _ => Self::Generic {
                store: "LocalFileSystem",
                source: Box::new(source),
            },
        }
    }
}

/// Local filesystem storage providing an [`ObjectStore`] interface to files on
/// local disk. Can optionally be created with a directory prefix
///
/// # Path Semantics
///
/// This implementation follows the [file URI] scheme outlined in [RFC 3986]. In
/// particular paths are delimited by `/`
///
/// [file URI]: https://en.wikipedia.org/wiki/File_URI_scheme
/// [RFC 3986]: https://www.rfc-editor.org/rfc/rfc3986
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
#[derive(Debug)]
pub struct LocalFileSystem {
    config: Arc<Config>,
}

#[derive(Debug)]
struct Config {
    root: Url,
}

impl std::fmt::Display for LocalFileSystem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LocalFileSystem({})", self.config.root)
    }
}

impl Default for LocalFileSystem {
    fn default() -> Self {
        Self::new()
    }
}

impl LocalFileSystem {
    /// Create new filesystem storage with no prefix
    pub fn new() -> Self {
        Self {
            config: Arc::new(Config {
                root: Url::parse("file:///").unwrap(),
            }),
        }
    }

    /// Create new filesystem storage with `prefix` applied to all paths
    pub fn new_with_prefix(prefix: impl AsRef<std::path::Path>) -> Result<Self> {
        Ok(Self {
            config: Arc::new(Config {
                root: filesystem_path_to_url(prefix)?,
            }),
        })
    }
}

impl Config {
    /// Return filesystem path of the given location
    fn path_to_filesystem(&self, location: &Path) -> Result<std::path::PathBuf> {
        let mut url = self.root.clone();
        url.path_segments_mut()
            .expect("url path")
            .extend(location.parts());

        url.to_file_path()
            .map_err(|_| Error::InvalidUrl { url }.into())
    }

    fn filesystem_to_path(&self, location: &std::path::Path) -> Result<Path> {
        Ok(Path::from_filesystem_path_with_base(
            location,
            Some(&self.root),
        )?)
    }
}

#[async_trait]
impl ObjectStore for LocalFileSystem {
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<()> {
        let path = self.config.path_to_filesystem(location)?;

        maybe_spawn_blocking(move || {
            let mut file = match File::create(&path) {
                Ok(f) => f,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    let parent = path
                        .parent()
                        .context(UnableToCreateFileSnafu { path: &path, err })?;
                    std::fs::create_dir_all(&parent)
                        .context(UnableToCreateDirSnafu { path: parent })?;

                    match File::create(&path) {
                        Ok(f) => f,
                        Err(err) => {
                            return Err(Error::UnableToCreateFile { path, err }.into())
                        }
                    }
                }
                Err(err) => return Err(Error::UnableToCreateFile { path, err }.into()),
            };

            file.write_all(&bytes)
                .context(UnableToCopyDataToFileSnafu)?;

            Ok(())
        })
        .await
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        let path = self.config.path_to_filesystem(location)?;
        maybe_spawn_blocking(move || {
            let file = open_file(&path)?;
            Ok(GetResult::File(file, path))
        })
        .await
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
        let path = self.config.path_to_filesystem(location)?;
        maybe_spawn_blocking(move || {
            let mut file = open_file(&path)?;
            let to_read = range.end - range.start;
            file.seek(SeekFrom::Start(range.start as u64))
                .context(SeekSnafu { path: &path })?;

            let mut buf = Vec::with_capacity(to_read);
            let read = file
                .take(to_read as u64)
                .read_to_end(&mut buf)
                .context(UnableToReadBytesSnafu { path: &path })?;

            ensure!(
                read == to_read,
                OutOfRangeSnafu {
                    path: &path,
                    expected: to_read,
                    actual: read
                }
            );

            Ok(buf.into())
        })
        .await
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        let path = self.config.path_to_filesystem(location)?;
        let location = location.clone();

        maybe_spawn_blocking(move || {
            let file = open_file(&path)?;
            let metadata =
                file.metadata().map_err(|e| Error::UnableToAccessMetadata {
                    source: e.into(),
                    path: location.to_string(),
                })?;

            convert_metadata(metadata, location)
        })
        .await
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        let path = self.config.path_to_filesystem(location)?;
        maybe_spawn_blocking(move || {
            std::fs::remove_file(&path).context(UnableToDeleteFileSnafu { path })?;
            Ok(())
        })
        .await
    }

    async fn list(
        &self,
        prefix: Option<&Path>,
    ) -> Result<BoxStream<'_, Result<ObjectMeta>>> {
        let config = Arc::clone(&self.config);

        let root_path = match prefix {
            Some(prefix) => config.path_to_filesystem(prefix)?,
            None => self.config.root.to_file_path().unwrap(),
        };

        let walkdir = WalkDir::new(&root_path)
            // Don't include the root directory itself
            .min_depth(1);

        let s = walkdir.into_iter().flat_map(move |result_dir_entry| {
            match convert_walkdir_result(result_dir_entry) {
                Err(e) => Some(Err(e)),
                Ok(None) => None,
                Ok(entry @ Some(_)) => entry
                    .filter(|dir_entry| dir_entry.file_type().is_file())
                    .map(|entry| {
                        let location = config.filesystem_to_path(entry.path())?;
                        convert_entry(entry, location)
                    }),
            }
        });

        // If no tokio context, return iterator directly as no
        // need to perform chunked spawn_blocking reads
        if tokio::runtime::Handle::try_current().is_err() {
            return Ok(futures::stream::iter(s).boxed());
        }

        // Otherwise list in batches of CHUNK_SIZE
        const CHUNK_SIZE: usize = 1024;

        let buffer = VecDeque::with_capacity(CHUNK_SIZE);
        let stream =
            futures::stream::try_unfold((s, buffer), |(mut s, mut buffer)| async move {
                if buffer.is_empty() {
                    (s, buffer) = tokio::task::spawn_blocking(move || {
                        for _ in 0..CHUNK_SIZE {
                            match s.next() {
                                Some(r) => buffer.push_back(r),
                                None => break,
                            }
                        }
                        (s, buffer)
                    })
                    .await?;
                }

                match buffer.pop_front() {
                    Some(Err(e)) => Err(e),
                    Some(Ok(meta)) => Ok(Some((meta, (s, buffer)))),
                    None => Ok(None),
                }
            });

        Ok(stream.boxed())
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let config = Arc::clone(&self.config);

        let prefix = prefix.cloned().unwrap_or_default();
        let resolved_prefix = config.path_to_filesystem(&prefix)?;

        maybe_spawn_blocking(move || {
            let walkdir = WalkDir::new(&resolved_prefix).min_depth(1).max_depth(1);

            let mut common_prefixes = BTreeSet::new();
            let mut objects = Vec::new();

            for entry_res in walkdir.into_iter().map(convert_walkdir_result) {
                if let Some(entry) = entry_res? {
                    let is_directory = entry.file_type().is_dir();
                    let entry_location = config.filesystem_to_path(entry.path())?;

                    let mut parts = match entry_location.prefix_match(&prefix) {
                        Some(parts) => parts,
                        None => continue,
                    };

                    let common_prefix = match parts.next() {
                        Some(p) => p,
                        None => continue,
                    };

                    drop(parts);

                    if is_directory {
                        common_prefixes.insert(prefix.child(common_prefix));
                    } else {
                        objects.push(convert_entry(entry, entry_location)?);
                    }
                }
            }

            Ok(ListResult {
                common_prefixes: common_prefixes.into_iter().collect(),
                objects,
            })
        })
        .await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        let from = self.config.path_to_filesystem(from)?;
        let to = self.config.path_to_filesystem(to)?;

        maybe_spawn_blocking(move || {
            std::fs::copy(&from, &to).context(UnableToCopyFileSnafu { from, to })?;
            Ok(())
        })
        .await
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        let from = self.config.path_to_filesystem(from)?;
        let to = self.config.path_to_filesystem(to)?;
        maybe_spawn_blocking(move || {
            std::fs::rename(&from, &to).context(UnableToCopyFileSnafu { from, to })?;
            Ok(())
        })
        .await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        let from = self.config.path_to_filesystem(from)?;
        let to = self.config.path_to_filesystem(to)?;

        maybe_spawn_blocking(move || {
            std::fs::hard_link(&from, &to).map_err(|err| match err.kind() {
                io::ErrorKind::AlreadyExists => Error::AlreadyExists {
                    path: to.to_str().unwrap().to_string(),
                    source: err,
                }
                .into(),
                _ => Error::UnableToCopyFile {
                    from,
                    to,
                    source: err,
                }
                .into(),
            })
        })
        .await
    }
}

fn open_file(path: &std::path::PathBuf) -> Result<File> {
    let file = File::open(path).map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            Error::NotFound {
                path: path.clone(),
                source: e,
            }
        } else {
            Error::UnableToOpenFile {
                path: path.clone(),
                source: e,
            }
        }
    })?;
    Ok(file)
}

fn convert_entry(entry: DirEntry, location: Path) -> Result<ObjectMeta> {
    let metadata = entry
        .metadata()
        .map_err(|e| Error::UnableToAccessMetadata {
            source: e.into(),
            path: location.to_string(),
        })?;
    convert_metadata(metadata, location)
}

fn convert_metadata(metadata: std::fs::Metadata, location: Path) -> Result<ObjectMeta> {
    let last_modified = metadata
        .modified()
        .expect("Modified file time should be supported on this platform")
        .into();

    let size = usize::try_from(metadata.len()).context(FileSizeOverflowedUsizeSnafu {
        path: location.as_ref(),
    })?;

    Ok(ObjectMeta {
        location,
        last_modified,
        size,
    })
}

/// Convert walkdir results and converts not-found errors into `None`.
fn convert_walkdir_result(
    res: std::result::Result<walkdir::DirEntry, walkdir::Error>,
) -> Result<Option<walkdir::DirEntry>> {
    match res {
        Ok(entry) => Ok(Some(entry)),
        Err(walkdir_err) => match walkdir_err.io_error() {
            Some(io_err) => match io_err.kind() {
                io::ErrorKind::NotFound => Ok(None),
                _ => Err(Error::UnableToWalkDir {
                    source: walkdir_err,
                }
                .into()),
            },
            None => Err(Error::UnableToWalkDir {
                source: walkdir_err,
            }
            .into()),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::flatten_list_stream;
    use crate::{
        tests::{
            copy_if_not_exists, get_nonexistent_object, list_uses_directories_correctly,
            list_with_delimiter, put_get_delete_list, rename_and_copy,
        },
        Error as ObjectStoreError, ObjectStore,
    };
    use tempfile::TempDir;

    #[tokio::test]
    async fn file_test() {
        let root = TempDir::new().unwrap();
        let integration = LocalFileSystem::new_with_prefix(root.path()).unwrap();

        put_get_delete_list(&integration).await.unwrap();
        list_uses_directories_correctly(&integration).await.unwrap();
        list_with_delimiter(&integration).await.unwrap();
        rename_and_copy(&integration).await.unwrap();
        copy_if_not_exists(&integration).await.unwrap();
    }

    #[test]
    fn test_non_tokio() {
        let root = TempDir::new().unwrap();
        let integration = LocalFileSystem::new_with_prefix(root.path()).unwrap();
        futures::executor::block_on(async move {
            put_get_delete_list(&integration).await.unwrap();
            list_uses_directories_correctly(&integration).await.unwrap();
            list_with_delimiter(&integration).await.unwrap();
        });
    }

    #[tokio::test]
    async fn creates_dir_if_not_present() {
        let root = TempDir::new().unwrap();
        let integration = LocalFileSystem::new_with_prefix(root.path()).unwrap();

        let location = Path::from("nested/file/test_file");

        let data = Bytes::from("arbitrary data");
        let expected_data = data.clone();

        integration.put(&location, data).await.unwrap();

        let read_data = integration
            .get(&location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(&*read_data, expected_data);
    }

    #[tokio::test]
    async fn unknown_length() {
        let root = TempDir::new().unwrap();
        let integration = LocalFileSystem::new_with_prefix(root.path()).unwrap();

        let location = Path::from("some_file");

        let data = Bytes::from("arbitrary data");
        let expected_data = data.clone();

        integration.put(&location, data).await.unwrap();

        let read_data = integration
            .get(&location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(&*read_data, expected_data);
    }

    #[tokio::test]
    #[cfg(target_family = "unix")]
    // Fails on github actions runner (which runs the tests as root)
    #[ignore]
    async fn bubble_up_io_errors() {
        use std::{fs::set_permissions, os::unix::prelude::PermissionsExt};

        let root = TempDir::new().unwrap();

        // make non-readable
        let metadata = root.path().metadata().unwrap();
        let mut permissions = metadata.permissions();
        permissions.set_mode(0o000);
        set_permissions(root.path(), permissions).unwrap();

        let store = LocalFileSystem::new_with_prefix(root.path()).unwrap();

        // `list` must fail
        match store.list(None).await {
            Err(_) => {
                // ok, error found
            }
            Ok(mut stream) => {
                let mut any_err = false;
                while let Some(res) = stream.next().await {
                    if res.is_err() {
                        any_err = true;
                    }
                }
                assert!(any_err);
            }
        }

        // `list_with_delimiter
        assert!(store.list_with_delimiter(None).await.is_err());
    }

    const NON_EXISTENT_NAME: &str = "nonexistentname";

    #[tokio::test]
    async fn get_nonexistent_location() {
        let root = TempDir::new().unwrap();
        let integration = LocalFileSystem::new_with_prefix(root.path()).unwrap();

        let location = Path::from(NON_EXISTENT_NAME);

        let err = get_nonexistent_object(&integration, Some(location))
            .await
            .unwrap_err();
        if let ObjectStoreError::NotFound { path, source } = err {
            let source_variant = source.downcast_ref::<std::io::Error>();
            assert!(
                matches!(source_variant, Some(std::io::Error { .. }),),
                "got: {:?}",
                source_variant
            );
            assert!(path.ends_with(NON_EXISTENT_NAME), "{}", path);
        } else {
            panic!("unexpected error type: {:?}", err);
        }
    }

    #[tokio::test]
    async fn root() {
        let integration = LocalFileSystem::new();

        let canonical = std::path::Path::new("Cargo.toml").canonicalize().unwrap();
        let url = Url::from_directory_path(&canonical).unwrap();
        let path = Path::parse(url.path()).unwrap();

        let roundtrip = integration.config.path_to_filesystem(&path).unwrap();

        // Needed as on Windows canonicalize returns extended length path syntax
        // C:\Users\circleci -> \\?\C:\Users\circleci
        let roundtrip = roundtrip.canonicalize().unwrap();

        assert_eq!(roundtrip, canonical);

        integration.head(&path).await.unwrap();
    }

    #[tokio::test]
    #[cfg(target_os = "linux")]
    // macos has some magic in its root '/.VolumeIcon.icns"' which causes this test to fail
    async fn test_list_root() {
        let integration = LocalFileSystem::new();
        let result = integration.list_with_delimiter(None).await;
        if cfg!(target_family = "windows") {
            let r = result.unwrap_err().to_string();
            assert!(
                r.contains("Unable to convert URL \"file:///\" to filesystem path"),
                "{}",
                r
            );
        } else {
            result.unwrap();
        }
    }

    #[tokio::test]
    async fn invalid_path() {
        let root = TempDir::new().unwrap();
        let root = root.path().join("🙀");
        std::fs::create_dir(root.clone()).unwrap();

        // Invalid paths supported above root of store
        let integration = LocalFileSystem::new_with_prefix(root.clone()).unwrap();

        let directory = Path::from("directory");
        let object = directory.child("child.txt");
        let data = Bytes::from("arbitrary");
        integration.put(&object, data.clone()).await.unwrap();
        integration.head(&object).await.unwrap();
        let result = integration.get(&object).await.unwrap();
        assert_eq!(result.bytes().await.unwrap(), data);

        flatten_list_stream(&integration, None).await.unwrap();
        flatten_list_stream(&integration, Some(&directory))
            .await
            .unwrap();

        let result = integration
            .list_with_delimiter(Some(&directory))
            .await
            .unwrap();
        assert_eq!(result.objects.len(), 1);
        assert!(result.common_prefixes.is_empty());
        assert_eq!(result.objects[0].location, object);

        let illegal = root.join("💀");
        std::fs::write(illegal, "foo").unwrap();

        // Can list directory that doesn't contain illegal path
        flatten_list_stream(&integration, Some(&directory))
            .await
            .unwrap();

        // Cannot list illegal file
        let err = flatten_list_stream(&integration, None)
            .await
            .unwrap_err()
            .to_string();

        assert!(
            err.contains("Invalid path segment - got \"💀\" expected: \"%F0%9F%92%80\""),
            "{}",
            err
        );
    }
}
