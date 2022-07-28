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
    GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore, Result,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::FutureExt;
use futures::{stream::BoxStream, StreamExt};
use snafu::{ensure, OptionExt, ResultExt, Snafu};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use std::{convert::TryFrom, io};
use tokio::io::AsyncWrite;
use url::Url;

/// A specialized `Error` for filesystem object store-related errors
#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub(crate) enum Error {
    #[snafu(display("File size for {} did not fit in a usize: {}", path, source))]
    FileSizeOverflowedUsize {
        source: std::num::TryFromIntError,
        path: String,
    },

    #[snafu(display("Unable to read dir: {}", source))]
    UnableToReadDir {
        source: io::Error,
        path: PathBuf,
    },

    #[snafu(display("Unable to read dir entry: {}", source))]
    UnableToReadDirEntry {
        source: io::Error,
        path: PathBuf,
    },

    #[snafu(display("Unable to access metadata for {}: {}", path.display(), source))]
    UnableToAccessMetadata {
        source: io::Error,
        path: PathBuf,
    },

    #[snafu(display("Unable to copy data to file: {}", source))]
    UnableToCopyDataToFile {
        source: io::Error,
    },

    #[snafu(display("Unable to create dir {}: {}", path.display(), source))]
    UnableToCreateDir {
        source: io::Error,
        path: PathBuf,
    },

    #[snafu(display("Unable to create file {}: {}", path.display(), err))]
    UnableToCreateFile {
        path: PathBuf,
        err: io::Error,
    },

    #[snafu(display("Unable to delete file {}: {}", path.display(), source))]
    UnableToDeleteFile {
        source: io::Error,
        path: PathBuf,
    },

    #[snafu(display("Unable to open file {}: {}", path.display(), source))]
    UnableToOpenFile {
        source: io::Error,
        path: PathBuf,
    },

    #[snafu(display("Unable to read data from file {}: {}", path.display(), source))]
    UnableToReadBytes {
        source: io::Error,
        path: PathBuf,
    },

    #[snafu(display("Out of range of file {}, expected: {}, actual: {}", path.display(), expected, actual))]
    OutOfRange {
        path: PathBuf,
        expected: usize,
        actual: usize,
    },

    #[snafu(display("Unable to copy file from {} to {}: {}", from.display(), to.display(), source))]
    UnableToCopyFile {
        from: PathBuf,
        to: PathBuf,
        source: io::Error,
    },

    NotFound {
        path: PathBuf,
        source: io::Error,
    },

    #[snafu(display("Error seeking file {}: {}", path.display(), source))]
    Seek {
        source: io::Error,
        path: PathBuf,
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
/// particular paths are delimited by `/`.
///
/// Additionally any symlinks are ignored, as they create path ambiguity
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


    /// List the contents of a directory, adding any files and directories to
    /// `files` and `dirs` respectively, and then returning them
    ///
    /// The slightly unusual signature is necessary to give the future a static lifetime
    async fn list_dir(
        self: &Arc<Self>,
        dir: PathBuf,
        mut files: Vec<ObjectMeta>,
        mut dirs: Vec<PathBuf>,
    ) -> Result<(Vec<ObjectMeta>, Vec<PathBuf>)> {
        let config = self.clone();
        maybe_spawn_blocking(move || {
            let read_dir = match std::fs::read_dir(&dir) {
                Ok(read_dir) => read_dir,
                // Ignore not found directory prefixes
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    return Ok((files, dirs))
                }
                Err(source) => {
                    return Err(Error::UnableToReadDir { source, path: dir }.into())
                }
            };

            for result in read_dir {
                let dir = result.context(UnableToReadDirEntrySnafu { path: &dir })?;
                let path = dir.path();

                let metadata = std::fs::symlink_metadata(&path)
                    .context(UnableToAccessMetadataSnafu { path: &path })?;

                // Ignore symlinks
                if metadata.is_symlink() {
                    continue;
                }

                if metadata.is_dir() {
                    dirs.push(path);
                    continue;
                }

                // Ignore file names with # in them, since they might be in-progress uploads.
                // They would be rejected anyways by filesystem_to_path below.
                if path.to_string_lossy().contains('#') {
                    continue;
                }

                let location = config.filesystem_to_path(&path)?;
                files.push(convert_metadata(metadata, location)?);
            }

            Ok((files, dirs))
        })
        .await
    }
}

#[async_trait]
impl ObjectStore for LocalFileSystem {
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<()> {
        let path = self.config.path_to_filesystem(location)?;

        maybe_spawn_blocking(move || {
            let mut file = open_writable_file(&path)?;

            file.write_all(&bytes)
                .context(UnableToCopyDataToFileSnafu)?;

            Ok(())
        })
        .await
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        let dest = self.config.path_to_filesystem(location)?;

        // Generate an id in case of concurrent writes
        let mut multipart_id = 1;

        // Will write to a temporary path
        let staging_path = loop {
            let staging_path = get_upload_stage_path(&dest, &multipart_id.to_string());

            match std::fs::metadata(&staging_path) {
                Err(err) if err.kind() == io::ErrorKind::NotFound => break staging_path,
                Err(err) => {
                    return Err(Error::UnableToCopyDataToFile { source: err }.into())
                }
                Ok(_) => multipart_id += 1,
            }
        };
        let multipart_id = multipart_id.to_string();

        let file = open_writable_file(&staging_path)?;

        Ok((
            multipart_id.clone(),
            Box::new(LocalUpload::new(dest, multipart_id, Arc::new(file))),
        ))
    }

    async fn abort_multipart(
        &self,
        location: &Path,
        multipart_id: &MultipartId,
    ) -> Result<()> {
        let dest = self.config.path_to_filesystem(location)?;
        let staging_path: PathBuf = get_upload_stage_path(&dest, multipart_id);

        maybe_spawn_blocking(move || {
            std::fs::remove_file(&staging_path)
                .context(UnableToDeleteFileSnafu { path: staging_path })?;
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
            let metadata = file
                .metadata()
                .context(UnableToAccessMetadataSnafu { path: &path })?;

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
        let config = &self.config;

        let root_path = match prefix {
            Some(prefix) => config.path_to_filesystem(prefix)?,
            None => self.config.root.to_file_path().unwrap(),
        };

        let mut dirs = Vec::with_capacity(1024);
        dirs.push(root_path);
        let files = Vec::with_capacity(1024);

        let stream =
            futures::stream::try_unfold((files, dirs), |(mut files, mut dirs)| async {
                while files.is_empty() {
                    let dir = match dirs.pop() {
                        Some(dir) => dir,
                        None => return Ok(None),
                    };

                    (files, dirs) = config.list_dir(dir, files, dirs).await?;
                }

                match files.pop() {
                    Some(meta) => Ok(Some((meta, (files, dirs)))),
                    None => Ok(None),
                }
            });

        Ok(stream.boxed())
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let config = Arc::clone(&self.config);

        let prefix = prefix.cloned().unwrap_or_default();
        let resolved_prefix = config.path_to_filesystem(&prefix)?;

        let (objects, dirs) = config.list_dir(resolved_prefix, vec![], vec![]).await?;

        let mut common_prefixes = Vec::with_capacity(dirs.len());
        for dir in dirs {
            let path = config.filesystem_to_path(&dir)?;
            let mut parts = match path.prefix_match(&prefix) {
                Some(parts) => parts,
                None => continue,
            };

            let common_prefix = match parts.next() {
                Some(p) => p,
                None => continue,
            };

            common_prefixes.push(prefix.child(common_prefix))
        }

        common_prefixes.sort_unstable_by(|a, b| a.as_ref().cmp(b.as_ref()));

        Ok(ListResult {
            common_prefixes,
            objects,
        })
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

fn get_upload_stage_path(dest: &std::path::Path, multipart_id: &MultipartId) -> PathBuf {
    let mut staging_path = dest.as_os_str().to_owned();
    staging_path.push(format!("#{}", multipart_id));
    staging_path.into()
}

enum LocalUploadState {
    /// Upload is ready to send new data
    Idle(Arc<File>),
    /// In the middle of a write
    Writing(Arc<File>, BoxFuture<'static, Result<usize, io::Error>>),
    /// In the middle of syncing data and closing file.
    ///
    /// Future will contain last reference to file, so it will call drop on completion.
    ShuttingDown(BoxFuture<'static, Result<(), io::Error>>),
    /// File is being moved from it's temporary location to the final location
    Committing(BoxFuture<'static, Result<(), io::Error>>),
    /// Upload is complete
    Complete,
}

struct LocalUpload {
    inner_state: LocalUploadState,
    dest: PathBuf,
    multipart_id: MultipartId,
}

impl LocalUpload {
    pub fn new(dest: PathBuf, multipart_id: MultipartId, file: Arc<File>) -> Self {
        Self {
            inner_state: LocalUploadState::Idle(file),
            dest,
            multipart_id,
        }
    }
}

impl AsyncWrite for LocalUpload {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        let invalid_state = |condition: &str| -> Poll<Result<usize, io::Error>> {
            Poll::Ready(Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Tried to write to file {}.", condition),
            )))
        };

        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let mut data: Vec<u8> = buf.to_vec();
            let data_len = data.len();

            loop {
                match &mut self.inner_state {
                    LocalUploadState::Idle(file) => {
                        let file = Arc::clone(file);
                        let file2 = Arc::clone(&file);
                        let data: Vec<u8> = std::mem::take(&mut data);
                        self.inner_state = LocalUploadState::Writing(
                            file,
                            Box::pin(
                                runtime
                                    .spawn_blocking(move || (&*file2).write_all(&data))
                                    .map(move |res| match res {
                                        Err(err) => {
                                            Err(io::Error::new(io::ErrorKind::Other, err))
                                        }
                                        Ok(res) => res.map(move |_| data_len),
                                    }),
                            ),
                        );
                    }
                    LocalUploadState::Writing(file, inner_write) => {
                        match inner_write.poll_unpin(cx) {
                            Poll::Ready(res) => {
                                self.inner_state =
                                    LocalUploadState::Idle(Arc::clone(file));
                                return Poll::Ready(res);
                            }
                            Poll::Pending => {
                                return Poll::Pending;
                            }
                        }
                    }
                    LocalUploadState::ShuttingDown(_) => {
                        return invalid_state("when writer is shutting down");
                    }
                    LocalUploadState::Committing(_) => {
                        return invalid_state("when writer is committing data");
                    }
                    LocalUploadState::Complete => {
                        return invalid_state("when writer is complete");
                    }
                }
            }
        } else if let LocalUploadState::Idle(file) = &self.inner_state {
            let file = Arc::clone(file);
            (&*file).write_all(buf)?;
            Poll::Ready(Ok(buf.len()))
        } else {
            // If we are running on this thread, then only possible states are Idle and Complete.
            invalid_state("when writer is already complete.")
        }
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            loop {
                match &mut self.inner_state {
                    LocalUploadState::Idle(file) => {
                        // We are moving file into the future, and it will be dropped on it's completion, closing the file.
                        let file = Arc::clone(file);
                        self.inner_state = LocalUploadState::ShuttingDown(Box::pin(
                            runtime.spawn_blocking(move || (*file).sync_all()).map(
                                move |res| match res {
                                    Err(err) => {
                                        Err(io::Error::new(io::ErrorKind::Other, err))
                                    }
                                    Ok(res) => res,
                                },
                            ),
                        ));
                    }
                    LocalUploadState::ShuttingDown(fut) => match fut.poll_unpin(cx) {
                        Poll::Ready(res) => {
                            res?;
                            let staging_path =
                                get_upload_stage_path(&self.dest, &self.multipart_id);
                            let dest = self.dest.clone();
                            self.inner_state = LocalUploadState::Committing(Box::pin(
                                runtime
                                    .spawn_blocking(move || {
                                        std::fs::rename(&staging_path, &dest)
                                    })
                                    .map(move |res| match res {
                                        Err(err) => {
                                            Err(io::Error::new(io::ErrorKind::Other, err))
                                        }
                                        Ok(res) => res,
                                    }),
                            ));
                        }
                        Poll::Pending => {
                            return Poll::Pending;
                        }
                    },
                    LocalUploadState::Writing(_, _) => {
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "Tried to commit a file where a write is in progress.",
                        )));
                    }
                    LocalUploadState::Committing(fut) => match fut.poll_unpin(cx) {
                        Poll::Ready(res) => {
                            self.inner_state = LocalUploadState::Complete;
                            return Poll::Ready(res);
                        }
                        Poll::Pending => return Poll::Pending,
                    },
                    LocalUploadState::Complete => {
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::Other,
                            "Already complete",
                        )))
                    }
                }
            }
        } else {
            let staging_path = get_upload_stage_path(&self.dest, &self.multipart_id);
            match &mut self.inner_state {
                LocalUploadState::Idle(file) => {
                    let file = Arc::clone(file);
                    self.inner_state = LocalUploadState::Complete;
                    file.sync_all()?;
                    drop(file);
                    std::fs::rename(&staging_path, &self.dest)?;
                    Poll::Ready(Ok(()))
                }
                _ => {
                    // If we are running on this thread, then only possible states are Idle and Complete.
                    Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::Other,
                        "Already complete",
                    )))
                }
            }
        }
    }
}

fn open_file(path: &PathBuf) -> Result<File> {
    let file = File::open(path).map_err(|e| {
        if e.kind() == io::ErrorKind::NotFound {
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

fn open_writable_file(path: &PathBuf) -> Result<File> {
    match File::create(&path) {
        Ok(f) => Ok(f),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            let parent = path
                .parent()
                .context(UnableToCreateFileSnafu { path: &path, err })?;
            std::fs::create_dir_all(&parent)
                .context(UnableToCreateDirSnafu { path: parent })?;

            match File::create(&path) {
                Ok(f) => Ok(f),
                Err(err) => Err(Error::UnableToCreateFile {
                    path: path.to_path_buf(),
                    err,
                }
                .into()),
            }
        }
        Err(err) => Err(Error::UnableToCreateFile {
            path: path.to_path_buf(),
            err,
        }
        .into()),
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::flatten_list_stream;
    use crate::{
        tests::{
            copy_if_not_exists, get_nonexistent_object, list_uses_directories_correctly,
            list_with_delimiter, put_get_delete_list, rename_and_copy, stream_get,
        },
        Error as ObjectStoreError, ObjectStore,
    };
    use futures::TryStreamExt;
    use tempfile::{NamedTempFile, TempDir};
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn file_test() {
        let root = TempDir::new().unwrap();
        let integration = LocalFileSystem::new_with_prefix(root.path()).unwrap();

        put_get_delete_list(&integration).await.unwrap();
        list_uses_directories_correctly(&integration).await.unwrap();
        list_with_delimiter(&integration).await.unwrap();
        rename_and_copy(&integration).await.unwrap();
        copy_if_not_exists(&integration).await.unwrap();
        stream_get(&integration).await.unwrap();
    }

    #[test]
    fn test_non_tokio() {
        let root = TempDir::new().unwrap();
        let integration = LocalFileSystem::new_with_prefix(root.path()).unwrap();
        futures::executor::block_on(async move {
            put_get_delete_list(&integration).await.unwrap();
            list_uses_directories_correctly(&integration).await.unwrap();
            list_with_delimiter(&integration).await.unwrap();
            stream_get(&integration).await.unwrap();
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

    async fn check_list(integration: &LocalFileSystem, expected: &[&str]) {
        let result: Vec<_> = integration
            .list(None)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();

        let mut strings: Vec<_> = result.iter().map(|x| x.location.as_ref()).collect();
        strings.sort_unstable();
        assert_eq!(&strings, expected)
    }

    #[tokio::test]
    #[cfg(target_family = "unix")]
    async fn test_symlink() {
        let root = TempDir::new().unwrap();
        let integration = LocalFileSystem::new_with_prefix(root.path()).unwrap();

        let subdir = root.path().join("a");
        std::fs::create_dir(&subdir).unwrap();
        let file = subdir.join("file.parquet");
        std::fs::write(file, "test").unwrap();

        check_list(&integration, &["a/file.parquet"]).await;

        // Ignore out of tree symlink
        let other = NamedTempFile::new().unwrap();
        std::os::unix::fs::symlink(other.path(), root.path().join("test.parquet"))
            .unwrap();
        check_list(&integration, &["a/file.parquet"]).await;

        // Ignore in tree symlink
        std::os::unix::fs::symlink(&subdir, root.path().join("b")).unwrap();
        check_list(&integration, &["a/file.parquet"]).await;

        // Ignore broken symlink
        std::os::unix::fs::symlink(
            root.path().join("foo.parquet"),
            root.path().join("c"),
        )
        .unwrap();
        check_list(&integration, &["a/file.parquet"]).await;
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

    #[tokio::test]
    async fn list_hides_incomplete_uploads() {
        let root = TempDir::new().unwrap();
        let integration = LocalFileSystem::new_with_prefix(root.path()).unwrap();
        let location = Path::from("some_file");

        let data = Bytes::from("arbitrary data");
        let (multipart_id, mut writer) =
            integration.put_multipart(&location).await.unwrap();
        writer.write_all(&data).await.unwrap();

        let (multipart_id_2, mut writer_2) =
            integration.put_multipart(&location).await.unwrap();
        assert_ne!(multipart_id, multipart_id_2);
        writer_2.write_all(&data).await.unwrap();

        let list = flatten_list_stream(&integration, None).await.unwrap();
        assert_eq!(list.len(), 0);

        assert_eq!(
            integration
                .list_with_delimiter(None)
                .await
                .unwrap()
                .objects
                .len(),
            0
        );
    }
}
