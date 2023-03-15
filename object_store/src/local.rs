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
    path::{absolute_path_to_url, Path},
    GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore, Result,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::FutureExt;
use futures::{stream::BoxStream, StreamExt};
use snafu::{ensure, OptionExt, ResultExt, Snafu};
use std::fs::{metadata, symlink_metadata, File, OpenOptions};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use std::{collections::BTreeSet, convert::TryFrom, io};
use std::{collections::VecDeque, path::PathBuf};
use tokio::io::AsyncWrite;
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

    #[snafu(display("Unable to rename file: {}", source))]
    UnableToRenameFile {
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

    #[snafu(display("Unable to canonicalize filesystem root: {}", path.display()))]
    UnableToCanonicalize {
        path: PathBuf,
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
///
/// # Symlinks
///
/// [`LocalFileSystem`] will follow symlinks as normal, however, it is worth noting:
///
/// * Broken symlinks will be silently ignored by listing operations
/// * No effort is made to prevent breaking symlinks when deleting files
/// * Symlinks that resolve to paths outside the root **will** be followed
/// * Mutating a file through one or more symlinks will mutate the underlying file
/// * Deleting a path that resolves to a symlink will only delete the symlink
///
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
    ///
    /// Returns an error if the path does not exist
    ///
    pub fn new_with_prefix(prefix: impl AsRef<std::path::Path>) -> Result<Self> {
        let path = std::fs::canonicalize(&prefix).context(UnableToCanonicalizeSnafu {
            path: prefix.as_ref(),
        })?;

        Ok(Self {
            config: Arc::new(Config {
                root: absolute_path_to_url(path)?,
            }),
        })
    }
}

impl Config {
    /// Return an absolute filesystem path of the given location
    fn path_to_filesystem(&self, location: &Path) -> Result<PathBuf> {
        let mut url = self.root.clone();
        url.path_segments_mut()
            .expect("url path")
            // technically not necessary as Path ignores empty segments
            // but avoids creating paths with "//" which look odd in error messages.
            .pop_if_empty()
            .extend(location.parts());

        url.to_file_path()
            .map_err(|_| Error::InvalidUrl { url }.into())
    }

    /// Resolves the provided absolute filesystem path to a [`Path`] prefix
    fn filesystem_to_path(&self, location: &std::path::Path) -> Result<Path> {
        Ok(Path::from_absolute_path_with_base(
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
            let (mut file, suffix) = new_staged_upload(&path)?;
            let staging_path = staged_upload_path(&path, &suffix);

            file.write_all(&bytes)
                .context(UnableToCopyDataToFileSnafu)?;

            std::fs::rename(staging_path, path).context(UnableToRenameFileSnafu)?;

            Ok(())
        })
        .await
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        let dest = self.config.path_to_filesystem(location)?;

        let (file, suffix) = new_staged_upload(&dest)?;
        Ok((
            suffix.clone(),
            Box::new(LocalUpload::new(dest, suffix, Arc::new(file))),
        ))
    }

    async fn abort_multipart(
        &self,
        location: &Path,
        multipart_id: &MultipartId,
    ) -> Result<()> {
        let dest = self.config.path_to_filesystem(location)?;
        let staging_path: PathBuf = staged_upload_path(&dest, multipart_id);

        maybe_spawn_blocking(move || {
            std::fs::remove_file(&staging_path)
                .context(UnableToDeleteFileSnafu { path: staging_path })?;
            Ok(())
        })
        .await
    }

    async fn append(
        &self,
        location: &Path,
    ) -> Result<Box<dyn AsyncWrite + Unpin + Send>> {
        #[cfg(not(target_arch = "wasm32"))]
        // Get the path to the file from the configuration.
        let path = self.config.path_to_filesystem(location)?;
        loop {
            // Create new `OpenOptions`.
            let mut options = tokio::fs::OpenOptions::new();

            // Attempt to open the file with the given options.
            match options
                .truncate(false)
                .append(true)
                .create(true)
                .open(&path)
                .await
            {
                // If the file was successfully opened, return it wrapped in a boxed `AsyncWrite` trait object.
                Ok(file) => return Ok(Box::new(file)),
                // If the error is that the file was not found, attempt to create the file and any necessary parent directories.
                Err(err) if err.kind() == ErrorKind::NotFound => {
                    // Get the path to the parent directory of the file.
                    let parent = path
                        .parent()
                        // If the parent directory does not exist, return a `UnableToCreateFileSnafu` error.
                        .context(UnableToCreateFileSnafu { path: &path, err })?;

                    // Create the parent directory and any necessary ancestors.
                    tokio::fs::create_dir_all(parent)
                        .await
                        // If creating the directory fails, return a `UnableToCreateDirSnafu` error.
                        .context(UnableToCreateDirSnafu { path: parent })?;
                    // Try again to open the file.
                    continue;
                }
                // If any other error occurs, return a `UnableToOpenFile` error.
                Err(source) => {
                    return Err(Error::UnableToOpenFile { source, path }.into())
                }
            }
        }
        #[cfg(target_arch = "wasm32")]
        Err(super::Error::NotImplemented)
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
            read_range(&mut file, &path, range)
        })
        .await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<usize>],
    ) -> Result<Vec<Bytes>> {
        let path = self.config.path_to_filesystem(location)?;
        let ranges = ranges.to_vec();
        maybe_spawn_blocking(move || {
            // Vectored IO might be faster
            let mut file = open_file(&path)?;
            ranges
                .into_iter()
                .map(|r| read_range(&mut file, &path, r))
                .collect()
        })
        .await
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        let path = self.config.path_to_filesystem(location)?;
        let location = location.clone();

        maybe_spawn_blocking(move || {
            let metadata = match metadata(&path) {
                Err(e) => Err(if e.kind() == ErrorKind::NotFound {
                    Error::NotFound {
                        path: path.clone(),
                        source: e,
                    }
                } else {
                    Error::UnableToAccessMetadata {
                        source: e.into(),
                        path: location.to_string(),
                    }
                }),
                Ok(m) => Ok(m),
            }?;
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

        let walkdir = WalkDir::new(root_path)
            // Don't include the root directory itself
            .min_depth(1)
            .follow_links(true);

        let s = walkdir.into_iter().flat_map(move |result_dir_entry| {
            match convert_walkdir_result(result_dir_entry) {
                Err(e) => Some(Err(e)),
                Ok(None) => None,
                Ok(entry @ Some(_)) => entry
                    .filter(|dir_entry| {
                        dir_entry.file_type().is_file()
                            // Ignore file names with # in them, since they might be in-progress uploads.
                            // They would be rejected anyways by filesystem_to_path below.
                            && !dir_entry.file_name().to_string_lossy().contains('#')
                    })
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
            let walkdir = WalkDir::new(&resolved_prefix)
                .min_depth(1)
                .max_depth(1)
                .follow_links(true);

            let mut common_prefixes = BTreeSet::new();
            let mut objects = Vec::new();

            for entry_res in walkdir.into_iter().map(convert_walkdir_result) {
                if let Some(entry) = entry_res? {
                    if entry.file_type().is_file()
                        // Ignore file names with # in them, since they might be in-progress uploads.
                        // They would be rejected anyways by filesystem_to_path below.
                        && entry.file_name().to_string_lossy().contains('#')
                    {
                        continue;
                    }
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

/// Generates a unique file path `{base}#{suffix}`, returning the opened `File` and `suffix`
///
/// Creates any directories if necessary
fn new_staged_upload(base: &std::path::Path) -> Result<(File, String)> {
    let mut multipart_id = 1;
    loop {
        let suffix = multipart_id.to_string();
        let path = staged_upload_path(base, &suffix);
        let mut options = OpenOptions::new();
        match options.read(true).write(true).create_new(true).open(&path) {
            Ok(f) => return Ok((f, suffix)),
            Err(e) if e.kind() == ErrorKind::AlreadyExists => {
                multipart_id += 1;
            }
            Err(err) if err.kind() == ErrorKind::NotFound => {
                let parent = path
                    .parent()
                    .context(UnableToCreateFileSnafu { path: &path, err })?;

                std::fs::create_dir_all(parent)
                    .context(UnableToCreateDirSnafu { path: parent })?;

                continue;
            }
            Err(source) => return Err(Error::UnableToOpenFile { source, path }.into()),
        }
    }
}

/// Returns the unique upload for the given path and suffix
fn staged_upload_path(dest: &std::path::Path, suffix: &str) -> PathBuf {
    let mut staging_path = dest.as_os_str().to_owned();
    staging_path.push("#");
    staging_path.push(suffix);
    staging_path.into()
}

enum LocalUploadState {
    /// Upload is ready to send new data
    Idle(Arc<std::fs::File>),
    /// In the middle of a write
    Writing(
        Arc<std::fs::File>,
        BoxFuture<'static, Result<usize, io::Error>>,
    ),
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
    pub fn new(
        dest: PathBuf,
        multipart_id: MultipartId,
        file: Arc<std::fs::File>,
    ) -> Self {
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
    ) -> std::task::Poll<Result<usize, io::Error>> {
        let invalid_state =
            |condition: &str| -> std::task::Poll<Result<usize, io::Error>> {
                Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("Tried to write to file {condition}."),
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
    ) -> std::task::Poll<Result<(), io::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), io::Error>> {
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
                                staged_upload_path(&self.dest, &self.multipart_id);
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
            let staging_path = staged_upload_path(&self.dest, &self.multipart_id);
            match &mut self.inner_state {
                LocalUploadState::Idle(file) => {
                    let file = Arc::clone(file);
                    self.inner_state = LocalUploadState::Complete;
                    file.sync_all()?;
                    std::mem::drop(file);
                    std::fs::rename(staging_path, &self.dest)?;
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

fn read_range(file: &mut File, path: &PathBuf, range: Range<usize>) -> Result<Bytes> {
    let to_read = range.end - range.start;
    file.seek(SeekFrom::Start(range.start as u64))
        .context(SeekSnafu { path })?;

    let mut buf = Vec::with_capacity(to_read);
    let read = file
        .take(to_read as u64)
        .read_to_end(&mut buf)
        .context(UnableToReadBytesSnafu { path })?;

    ensure!(
        read == to_read,
        OutOfRangeSnafu {
            path,
            expected: to_read,
            actual: read
        }
    );
    Ok(buf.into())
}

fn open_file(path: &PathBuf) -> Result<File> {
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
/// Convert broken symlinks to `None`.
fn convert_walkdir_result(
    res: std::result::Result<walkdir::DirEntry, walkdir::Error>,
) -> Result<Option<walkdir::DirEntry>> {
    match res {
        Ok(entry) => {
            // To check for broken symlink: call symlink_metadata() - it does not traverse symlinks);
            // if ok: check if entry is symlink; and try to read it by calling metadata().
            match symlink_metadata(entry.path()) {
                Ok(attr) => {
                    if attr.is_symlink() {
                        let target_metadata = metadata(entry.path());
                        match target_metadata {
                            Ok(_) => {
                                // symlink is valid
                                Ok(Some(entry))
                            }
                            Err(_) => {
                                // this is a broken symlink, return None
                                Ok(None)
                            }
                        }
                    } else {
                        Ok(Some(entry))
                    }
                }
                Err(_) => Ok(None),
            }
        }

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

        put_get_delete_list(&integration).await;
        list_uses_directories_correctly(&integration).await;
        list_with_delimiter(&integration).await;
        rename_and_copy(&integration).await;
        copy_if_not_exists(&integration).await;
        stream_get(&integration).await;
    }

    #[test]
    fn test_non_tokio() {
        let root = TempDir::new().unwrap();
        let integration = LocalFileSystem::new_with_prefix(root.path()).unwrap();
        futures::executor::block_on(async move {
            put_get_delete_list(&integration).await;
            list_uses_directories_correctly(&integration).await;
            list_with_delimiter(&integration).await;
            stream_get(&integration).await;
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
                "got: {source_variant:?}"
            );
            assert!(path.ends_with(NON_EXISTENT_NAME), "{}", path);
        } else {
            panic!("unexpected error type: {err:?}");
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

    async fn check_list(
        integration: &LocalFileSystem,
        prefix: Option<&Path>,
        expected: &[&str],
    ) {
        let result: Vec<_> = integration
            .list(prefix)
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

        check_list(&integration, None, &["a/file.parquet"]).await;
        integration
            .head(&Path::from("a/file.parquet"))
            .await
            .unwrap();

        // Follow out of tree symlink
        let other = NamedTempFile::new().unwrap();
        std::os::unix::fs::symlink(other.path(), root.path().join("test.parquet"))
            .unwrap();

        // Should return test.parquet even though out of tree
        check_list(&integration, None, &["a/file.parquet", "test.parquet"]).await;

        // Can fetch test.parquet
        integration.head(&Path::from("test.parquet")).await.unwrap();

        // Follow in tree symlink
        std::os::unix::fs::symlink(&subdir, root.path().join("b")).unwrap();
        check_list(
            &integration,
            None,
            &["a/file.parquet", "b/file.parquet", "test.parquet"],
        )
        .await;
        check_list(&integration, Some(&Path::from("b")), &["b/file.parquet"]).await;

        // Can fetch through symlink
        integration
            .head(&Path::from("b/file.parquet"))
            .await
            .unwrap();

        // Ignore broken symlink
        std::os::unix::fs::symlink(
            root.path().join("foo.parquet"),
            root.path().join("c"),
        )
        .unwrap();

        check_list(
            &integration,
            None,
            &["a/file.parquet", "b/file.parquet", "test.parquet"],
        )
        .await;

        let mut r = integration.list_with_delimiter(None).await.unwrap();
        r.common_prefixes.sort_unstable();
        assert_eq!(r.common_prefixes.len(), 2);
        assert_eq!(r.common_prefixes[0].as_ref(), "a");
        assert_eq!(r.common_prefixes[1].as_ref(), "b");
        assert_eq!(r.objects.len(), 1);
        assert_eq!(r.objects[0].location.as_ref(), "test.parquet");

        let r = integration
            .list_with_delimiter(Some(&Path::from("a")))
            .await
            .unwrap();
        assert_eq!(r.common_prefixes.len(), 0);
        assert_eq!(r.objects.len(), 1);
        assert_eq!(r.objects[0].location.as_ref(), "a/file.parquet");

        // Deleting a symlink doesn't delete the source file
        integration
            .delete(&Path::from("test.parquet"))
            .await
            .unwrap();
        assert!(other.path().exists());

        check_list(&integration, None, &["a/file.parquet", "b/file.parquet"]).await;

        // Deleting through a symlink deletes both files
        integration
            .delete(&Path::from("b/file.parquet"))
            .await
            .unwrap();

        check_list(&integration, None, &[]).await;

        // Adding a file through a symlink creates in both paths
        integration
            .put(&Path::from("b/file.parquet"), Bytes::from(vec![0, 1, 2]))
            .await
            .unwrap();

        check_list(&integration, None, &["a/file.parquet", "b/file.parquet"]).await;
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
            err.contains("Encountered illegal character sequence \"💀\" whilst parsing path segment \"💀\""),
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

    #[tokio::test]
    async fn filesystem_filename_with_percent() {
        let temp_dir = TempDir::new().unwrap();
        let integration = LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap();
        let filename = "L%3ABC.parquet";

        std::fs::write(temp_dir.path().join(filename), "foo").unwrap();

        let list_stream = integration.list(None).await.unwrap();
        let res: Vec<_> = list_stream.try_collect().await.unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].location.as_ref(), filename);

        let res = integration.list_with_delimiter(None).await.unwrap();
        assert_eq!(res.objects.len(), 1);
        assert_eq!(res.objects[0].location.as_ref(), filename);
    }

    #[tokio::test]
    async fn relative_paths() {
        LocalFileSystem::new_with_prefix(".").unwrap();
        LocalFileSystem::new_with_prefix("..").unwrap();
        LocalFileSystem::new_with_prefix("../..").unwrap();

        let integration = LocalFileSystem::new();
        let path = Path::from_filesystem_path(".").unwrap();
        integration.list_with_delimiter(Some(&path)).await.unwrap();
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[cfg(test)]
mod not_wasm_tests {
    use crate::local::LocalFileSystem;
    use crate::{ObjectStore, Path};
    use bytes::Bytes;
    use tempfile::TempDir;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn creates_dir_if_not_present_append() {
        let root = TempDir::new().unwrap();
        let integration = LocalFileSystem::new_with_prefix(root.path()).unwrap();

        let location = Path::from("nested/file/test_file");

        let data = Bytes::from("arbitrary data");
        let expected_data = data.clone();

        let mut writer = integration.append(&location).await.unwrap();

        writer.write_all(data.as_ref()).await.unwrap();

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
    async fn unknown_length_append() {
        let root = TempDir::new().unwrap();
        let integration = LocalFileSystem::new_with_prefix(root.path()).unwrap();

        let location = Path::from("some_file");

        let data = Bytes::from("arbitrary data");
        let expected_data = data.clone();
        let mut writer = integration.append(&location).await.unwrap();

        writer.write_all(data.as_ref()).await.unwrap();

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
    async fn multiple_append() {
        let root = TempDir::new().unwrap();
        let integration = LocalFileSystem::new_with_prefix(root.path()).unwrap();

        let location = Path::from("some_file");

        let data = vec![
            Bytes::from("arbitrary"),
            Bytes::from("data"),
            Bytes::from("gnz"),
        ];

        let mut writer = integration.append(&location).await.unwrap();
        for d in &data {
            writer.write_all(d).await.unwrap();
        }

        let mut writer = integration.append(&location).await.unwrap();
        for d in &data {
            writer.write_all(d).await.unwrap();
        }

        let read_data = integration
            .get(&location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let expected_data = Bytes::from("arbitrarydatagnzarbitrarydatagnz");
        assert_eq!(&*read_data, expected_data);
    }
}

#[cfg(target_family = "unix")]
#[cfg(test)]
mod unix_test {
    use crate::local::LocalFileSystem;
    use crate::{ObjectStore, Path};
    use nix::sys::stat;
    use nix::unistd;
    use std::time::Duration;
    use tempfile::TempDir;
    use tokio::time::timeout;

    #[tokio::test]
    async fn test_head_fifo() {
        let filename = "some_file";
        let root = TempDir::new().unwrap();
        let integration = LocalFileSystem::new_with_prefix(root.path()).unwrap();
        unistd::mkfifo(&root.path().join(filename), stat::Mode::S_IRWXU).unwrap();
        let location = Path::from(filename);
        if (timeout(Duration::from_millis(10), integration.head(&location)).await)
            .is_err()
        {
            panic!("Did not receive value within 10 ms");
        }
    }
}
