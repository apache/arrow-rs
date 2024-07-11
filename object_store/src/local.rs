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
use std::fs::{metadata, symlink_metadata, File, Metadata, OpenOptions};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::sync::Arc;
use std::time::SystemTime;
use std::{collections::BTreeSet, convert::TryFrom, io};
use std::{collections::VecDeque, path::PathBuf};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{stream::BoxStream, StreamExt};
use futures::{FutureExt, TryStreamExt};
use parking_lot::Mutex;
use snafu::{ensure, OptionExt, ResultExt, Snafu};
use url::Url;
use walkdir::{DirEntry, WalkDir};

use crate::{
    maybe_spawn_blocking,
    path::{absolute_path_to_url, Path},
    util::InvalidGetRange,
    Attributes, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMode, PutMultipartOpts, PutOptions, PutPayload, PutResult, Result, UploadPart,
};

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
    Metadata {
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

    #[snafu(display("Unable to create file {}: {}", path.display(), source))]
    UnableToCreateFile {
        source: io::Error,
        path: PathBuf,
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

    #[snafu(display("Requested range was invalid"))]
    InvalidRange {
        source: InvalidGetRange,
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

    #[snafu(display("Filenames containing trailing '/#\\d+/' are not supported: {}", path))]
    InvalidPath {
        path: String,
    },

    #[snafu(display("Upload aborted"))]
    Aborted,
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
/// # Path Semantics
///
/// [`LocalFileSystem`] will expose the path semantics of the underlying filesystem, which may
/// have additional restrictions beyond those enforced by [`Path`].
///
/// For example:
///
/// * Windows forbids certain filenames, e.g. `COM0`,
/// * Windows forbids folders with trailing `.`
/// * Windows forbids certain ASCII characters, e.g. `<` or `|`
/// * OS X forbids filenames containing `:`
/// * Leading `-` are discouraged on Unix systems where they may be interpreted as CLI flags
/// * Filesystems may have restrictions on the maximum path or path segment length
/// * Filesystem support for non-ASCII characters is inconsistent
///
/// Additionally some filesystems, such as NTFS, are case-insensitive, whilst others like
/// FAT don't preserve case at all. Further some filesystems support non-unicode character
/// sequences, such as unpaired UTF-16 surrogates, and [`LocalFileSystem`] will error on
/// encountering such sequences.
///
/// Finally, filenames matching the regex `/.*#\d+/`, e.g. `foo.parquet#123`, are not supported
/// by [`LocalFileSystem`] as they are used to provide atomic writes. Such files will be ignored
/// for listing operations, and attempting to address such a file will error.
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
/// # Cross-Filesystem Copy
///
/// [`LocalFileSystem::copy`] is implemented using [`std::fs::hard_link`], and therefore
/// does not support copying across filesystem boundaries.
///
#[derive(Debug)]
pub struct LocalFileSystem {
    config: Arc<Config>,
    // if you want to delete empty directories when deleting files
    automatic_cleanup: bool,
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
            automatic_cleanup: false,
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
            automatic_cleanup: false,
        })
    }

    /// Return an absolute filesystem path of the given file location
    pub fn path_to_filesystem(&self, location: &Path) -> Result<PathBuf> {
        ensure!(
            is_valid_file_path(location),
            InvalidPathSnafu {
                path: location.as_ref()
            }
        );
        self.config.prefix_to_filesystem(location)
    }

    /// Enable automatic cleanup of empty directories when deleting files
    pub fn with_automatic_cleanup(mut self, automatic_cleanup: bool) -> Self {
        self.automatic_cleanup = automatic_cleanup;
        self
    }
}

impl Config {
    /// Return an absolute filesystem path of the given location
    fn prefix_to_filesystem(&self, location: &Path) -> Result<PathBuf> {
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

fn is_valid_file_path(path: &Path) -> bool {
    match path.filename() {
        Some(p) => match p.split_once('#') {
            Some((_, suffix)) if !suffix.is_empty() => {
                // Valid if contains non-digits
                !suffix.as_bytes().iter().all(|x| x.is_ascii_digit())
            }
            _ => true,
        },
        None => false,
    }
}

#[async_trait]
impl ObjectStore for LocalFileSystem {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        if matches!(opts.mode, PutMode::Update(_)) {
            return Err(crate::Error::NotImplemented);
        }

        if !opts.attributes.is_empty() {
            return Err(crate::Error::NotImplemented);
        }

        let path = self.path_to_filesystem(location)?;
        maybe_spawn_blocking(move || {
            let (mut file, staging_path) = new_staged_upload(&path)?;
            let mut e_tag = None;

            let err = match payload.iter().try_for_each(|x| file.write_all(x)) {
                Ok(_) => {
                    let metadata = file.metadata().map_err(|e| Error::Metadata {
                        source: e.into(),
                        path: path.to_string_lossy().to_string(),
                    })?;
                    e_tag = Some(get_etag(&metadata));
                    match opts.mode {
                        PutMode::Overwrite => {
                            // For some fuse types of file systems, the file must be closed first
                            // to trigger the upload operation, and then renamed, such as Blobfuse
                            std::mem::drop(file);
                            match std::fs::rename(&staging_path, &path) {
                                Ok(_) => None,
                                Err(source) => Some(Error::UnableToRenameFile { source }),
                            }
                        }
                        PutMode::Create => match std::fs::hard_link(&staging_path, &path) {
                            Ok(_) => {
                                let _ = std::fs::remove_file(&staging_path); // Attempt to cleanup
                                None
                            }
                            Err(source) => match source.kind() {
                                ErrorKind::AlreadyExists => Some(Error::AlreadyExists {
                                    path: path.to_str().unwrap().to_string(),
                                    source,
                                }),
                                _ => Some(Error::UnableToRenameFile { source }),
                            },
                        },
                        PutMode::Update(_) => unreachable!(),
                    }
                }
                Err(source) => Some(Error::UnableToCopyDataToFile { source }),
            };

            if let Some(err) = err {
                let _ = std::fs::remove_file(&staging_path); // Attempt to cleanup
                return Err(err.into());
            }

            Ok(PutResult {
                e_tag,
                version: None,
            })
        })
        .await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> Result<Box<dyn MultipartUpload>> {
        if !opts.attributes.is_empty() {
            return Err(crate::Error::NotImplemented);
        }

        let dest = self.path_to_filesystem(location)?;
        let (file, src) = new_staged_upload(&dest)?;
        Ok(Box::new(LocalUpload::new(src, dest, file)))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let location = location.clone();
        let path = self.path_to_filesystem(&location)?;
        maybe_spawn_blocking(move || {
            let (file, metadata) = open_file(&path)?;
            let meta = convert_metadata(metadata, location)?;
            options.check_preconditions(&meta)?;

            let range = match options.range {
                Some(r) => r.as_range(meta.size).context(InvalidRangeSnafu)?,
                None => 0..meta.size,
            };

            Ok(GetResult {
                payload: GetResultPayload::File(file, path),
                attributes: Attributes::default(),
                range,
                meta,
            })
        })
        .await
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
        let path = self.path_to_filesystem(location)?;
        maybe_spawn_blocking(move || {
            let (mut file, _) = open_file(&path)?;
            read_range(&mut file, &path, range)
        })
        .await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<usize>]) -> Result<Vec<Bytes>> {
        let path = self.path_to_filesystem(location)?;
        let ranges = ranges.to_vec();
        maybe_spawn_blocking(move || {
            // Vectored IO might be faster
            let (mut file, _) = open_file(&path)?;
            ranges
                .into_iter()
                .map(|r| read_range(&mut file, &path, r))
                .collect()
        })
        .await
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        let config = Arc::clone(&self.config);
        let path = self.path_to_filesystem(location)?;
        let automactic_cleanup = self.automatic_cleanup;
        maybe_spawn_blocking(move || {
            if let Err(e) = std::fs::remove_file(&path) {
                Err(match e.kind() {
                    ErrorKind::NotFound => Error::NotFound { path, source: e }.into(),
                    _ => Error::UnableToDeleteFile { path, source: e }.into(),
                })
            } else if automactic_cleanup {
                let root = &config.root;
                let root = root
                    .to_file_path()
                    .map_err(|_| Error::InvalidUrl { url: root.clone() })?;

                // here we will try to traverse up and delete an empty dir if possible until we reach the root or get an error
                let mut parent = path.parent();

                while let Some(loc) = parent {
                    if loc != root && std::fs::remove_dir(loc).is_ok() {
                        parent = loc.parent();
                    } else {
                        break;
                    }
                }

                Ok(())
            } else {
                Ok(())
            }
        })
        .await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, Result<ObjectMeta>> {
        let config = Arc::clone(&self.config);

        let root_path = match prefix {
            Some(prefix) => match config.prefix_to_filesystem(prefix) {
                Ok(path) => path,
                Err(e) => return futures::future::ready(Err(e)).into_stream().boxed(),
            },
            None => self.config.root.to_file_path().unwrap(),
        };

        let walkdir = WalkDir::new(root_path)
            // Don't include the root directory itself
            .min_depth(1)
            .follow_links(true);

        let s = walkdir.into_iter().flat_map(move |result_dir_entry| {
            let entry = match convert_walkdir_result(result_dir_entry).transpose()? {
                Ok(entry) => entry,
                Err(e) => return Some(Err(e)),
            };

            if !entry.path().is_file() {
                return None;
            }

            match config.filesystem_to_path(entry.path()) {
                Ok(path) => match is_valid_file_path(&path) {
                    true => convert_entry(entry, path).transpose(),
                    false => None,
                },
                Err(e) => Some(Err(e)),
            }
        });

        // If no tokio context, return iterator directly as no
        // need to perform chunked spawn_blocking reads
        if tokio::runtime::Handle::try_current().is_err() {
            return futures::stream::iter(s).boxed();
        }

        // Otherwise list in batches of CHUNK_SIZE
        const CHUNK_SIZE: usize = 1024;

        let buffer = VecDeque::with_capacity(CHUNK_SIZE);
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
        })
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let config = Arc::clone(&self.config);

        let prefix = prefix.cloned().unwrap_or_default();
        let resolved_prefix = config.prefix_to_filesystem(&prefix)?;

        maybe_spawn_blocking(move || {
            let walkdir = WalkDir::new(&resolved_prefix)
                .min_depth(1)
                .max_depth(1)
                .follow_links(true);

            let mut common_prefixes = BTreeSet::new();
            let mut objects = Vec::new();

            for entry_res in walkdir.into_iter().map(convert_walkdir_result) {
                if let Some(entry) = entry_res? {
                    let is_directory = entry.file_type().is_dir();
                    let entry_location = config.filesystem_to_path(entry.path())?;
                    if !is_directory && !is_valid_file_path(&entry_location) {
                        continue;
                    }

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
                    } else if let Some(metadata) = convert_entry(entry, entry_location)? {
                        objects.push(metadata);
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
        let from = self.path_to_filesystem(from)?;
        let to = self.path_to_filesystem(to)?;
        let mut id = 0;
        // In order to make this atomic we:
        //
        // - hard link to a hidden temporary file
        // - atomically rename this temporary file into place
        //
        // This is necessary because hard_link returns an error if the destination already exists
        maybe_spawn_blocking(move || loop {
            let staged = staged_upload_path(&to, &id.to_string());
            match std::fs::hard_link(&from, &staged) {
                Ok(_) => {
                    return std::fs::rename(&staged, &to).map_err(|source| {
                        let _ = std::fs::remove_file(&staged); // Attempt to clean up
                        Error::UnableToCopyFile { from, to, source }.into()
                    });
                }
                Err(source) => match source.kind() {
                    ErrorKind::AlreadyExists => id += 1,
                    ErrorKind::NotFound => match from.exists() {
                        true => create_parent_dirs(&to, source)?,
                        false => return Err(Error::NotFound { path: from, source }.into()),
                    },
                    _ => return Err(Error::UnableToCopyFile { from, to, source }.into()),
                },
            }
        })
        .await
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        let from = self.path_to_filesystem(from)?;
        let to = self.path_to_filesystem(to)?;
        maybe_spawn_blocking(move || loop {
            match std::fs::rename(&from, &to) {
                Ok(_) => return Ok(()),
                Err(source) => match source.kind() {
                    ErrorKind::NotFound => match from.exists() {
                        true => create_parent_dirs(&to, source)?,
                        false => return Err(Error::NotFound { path: from, source }.into()),
                    },
                    _ => return Err(Error::UnableToCopyFile { from, to, source }.into()),
                },
            }
        })
        .await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        let from = self.path_to_filesystem(from)?;
        let to = self.path_to_filesystem(to)?;

        maybe_spawn_blocking(move || loop {
            match std::fs::hard_link(&from, &to) {
                Ok(_) => return Ok(()),
                Err(source) => match source.kind() {
                    ErrorKind::AlreadyExists => {
                        return Err(Error::AlreadyExists {
                            path: to.to_str().unwrap().to_string(),
                            source,
                        }
                        .into())
                    }
                    ErrorKind::NotFound => match from.exists() {
                        true => create_parent_dirs(&to, source)?,
                        false => return Err(Error::NotFound { path: from, source }.into()),
                    },
                    _ => return Err(Error::UnableToCopyFile { from, to, source }.into()),
                },
            }
        })
        .await
    }
}

/// Creates the parent directories of `path` or returns an error based on `source` if no parent
fn create_parent_dirs(path: &std::path::Path, source: io::Error) -> Result<()> {
    let parent = path.parent().ok_or_else(|| Error::UnableToCreateFile {
        path: path.to_path_buf(),
        source,
    })?;

    std::fs::create_dir_all(parent).context(UnableToCreateDirSnafu { path: parent })?;
    Ok(())
}

/// Generates a unique file path `{base}#{suffix}`, returning the opened `File` and `path`
///
/// Creates any directories if necessary
fn new_staged_upload(base: &std::path::Path) -> Result<(File, PathBuf)> {
    let mut multipart_id = 1;
    loop {
        let suffix = multipart_id.to_string();
        let path = staged_upload_path(base, &suffix);
        let mut options = OpenOptions::new();
        match options.read(true).write(true).create_new(true).open(&path) {
            Ok(f) => return Ok((f, path)),
            Err(source) => match source.kind() {
                ErrorKind::AlreadyExists => multipart_id += 1,
                ErrorKind::NotFound => create_parent_dirs(&path, source)?,
                _ => return Err(Error::UnableToOpenFile { source, path }.into()),
            },
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

#[derive(Debug)]
struct LocalUpload {
    /// The upload state
    state: Arc<UploadState>,
    /// The location of the temporary file
    src: Option<PathBuf>,
    /// The next offset to write into the file
    offset: u64,
}

#[derive(Debug)]
struct UploadState {
    dest: PathBuf,
    file: Mutex<File>,
}

impl LocalUpload {
    pub fn new(src: PathBuf, dest: PathBuf, file: File) -> Self {
        Self {
            state: Arc::new(UploadState {
                dest,
                file: Mutex::new(file),
            }),
            src: Some(src),
            offset: 0,
        }
    }
}

#[async_trait]
impl MultipartUpload for LocalUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let offset = self.offset;
        self.offset += data.content_length() as u64;

        let s = Arc::clone(&self.state);
        maybe_spawn_blocking(move || {
            let mut file = s.file.lock();
            file.seek(SeekFrom::Start(offset))
                .context(SeekSnafu { path: &s.dest })?;

            data.iter()
                .try_for_each(|x| file.write_all(x))
                .context(UnableToCopyDataToFileSnafu)?;

            Ok(())
        })
        .boxed()
    }

    async fn complete(&mut self) -> Result<PutResult> {
        let src = self.src.take().context(AbortedSnafu)?;
        let s = Arc::clone(&self.state);
        maybe_spawn_blocking(move || {
            // Ensure no inflight writes
            let file = s.file.lock();
            std::fs::rename(&src, &s.dest).context(UnableToRenameFileSnafu)?;
            let metadata = file.metadata().map_err(|e| Error::Metadata {
                source: e.into(),
                path: src.to_string_lossy().to_string(),
            })?;

            Ok(PutResult {
                e_tag: Some(get_etag(&metadata)),
                version: None,
            })
        })
        .await
    }

    async fn abort(&mut self) -> Result<()> {
        let src = self.src.take().context(AbortedSnafu)?;
        maybe_spawn_blocking(move || {
            std::fs::remove_file(&src).context(UnableToDeleteFileSnafu { path: &src })?;
            Ok(())
        })
        .await
    }
}

impl Drop for LocalUpload {
    fn drop(&mut self) {
        if let Some(src) = self.src.take() {
            // Try to clean up intermediate file ignoring any error
            match tokio::runtime::Handle::try_current() {
                Ok(r) => drop(r.spawn_blocking(move || std::fs::remove_file(src))),
                Err(_) => drop(std::fs::remove_file(src)),
            };
        }
    }
}

pub(crate) fn chunked_stream(
    mut file: File,
    path: PathBuf,
    range: Range<usize>,
    chunk_size: usize,
) -> BoxStream<'static, Result<Bytes, super::Error>> {
    futures::stream::once(async move {
        let (file, path) = maybe_spawn_blocking(move || {
            file.seek(SeekFrom::Start(range.start as _))
                .map_err(|source| Error::Seek {
                    source,
                    path: path.clone(),
                })?;
            Ok((file, path))
        })
        .await?;

        let stream = futures::stream::try_unfold(
            (file, path, range.end - range.start),
            move |(mut file, path, remaining)| {
                maybe_spawn_blocking(move || {
                    if remaining == 0 {
                        return Ok(None);
                    }

                    let to_read = remaining.min(chunk_size);
                    let mut buffer = Vec::with_capacity(to_read);
                    let read = (&mut file)
                        .take(to_read as u64)
                        .read_to_end(&mut buffer)
                        .map_err(|e| Error::UnableToReadBytes {
                            source: e,
                            path: path.clone(),
                        })?;

                    Ok(Some((buffer.into(), (file, path, remaining - read))))
                })
            },
        );
        Ok::<_, super::Error>(stream)
    })
    .try_flatten()
    .boxed()
}

pub(crate) fn read_range(file: &mut File, path: &PathBuf, range: Range<usize>) -> Result<Bytes> {
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

fn open_file(path: &PathBuf) -> Result<(File, Metadata)> {
    let ret = match File::open(path).and_then(|f| Ok((f.metadata()?, f))) {
        Err(e) => Err(match e.kind() {
            ErrorKind::NotFound => Error::NotFound {
                path: path.clone(),
                source: e,
            },
            _ => Error::UnableToOpenFile {
                path: path.clone(),
                source: e,
            },
        }),
        Ok((metadata, file)) => match !metadata.is_dir() {
            true => Ok((file, metadata)),
            false => Err(Error::NotFound {
                path: path.clone(),
                source: io::Error::new(ErrorKind::NotFound, "is directory"),
            }),
        },
    }?;
    Ok(ret)
}

fn convert_entry(entry: DirEntry, location: Path) -> Result<Option<ObjectMeta>> {
    match entry.metadata() {
        Ok(metadata) => convert_metadata(metadata, location).map(Some),
        Err(e) => {
            if let Some(io_err) = e.io_error() {
                if io_err.kind() == ErrorKind::NotFound {
                    return Ok(None);
                }
            }
            Err(Error::Metadata {
                source: e.into(),
                path: location.to_string(),
            })?
        }
    }
}

fn last_modified(metadata: &Metadata) -> DateTime<Utc> {
    metadata
        .modified()
        .expect("Modified file time should be supported on this platform")
        .into()
}

fn get_etag(metadata: &Metadata) -> String {
    let inode = get_inode(metadata);
    let size = metadata.len();
    let mtime = metadata
        .modified()
        .ok()
        .and_then(|mtime| mtime.duration_since(SystemTime::UNIX_EPOCH).ok())
        .unwrap_or_default()
        .as_micros();

    // Use an ETag scheme based on that used by many popular HTTP servers
    // <https://httpd.apache.org/docs/2.2/mod/core.html#fileetag>
    // <https://stackoverflow.com/questions/47512043/how-etags-are-generated-and-configured>
    format!("{inode:x}-{mtime:x}-{size:x}")
}

fn convert_metadata(metadata: Metadata, location: Path) -> Result<ObjectMeta> {
    let last_modified = last_modified(&metadata);
    let size = usize::try_from(metadata.len()).context(FileSizeOverflowedUsizeSnafu {
        path: location.as_ref(),
    })?;

    Ok(ObjectMeta {
        location,
        last_modified,
        size,
        e_tag: Some(get_etag(&metadata)),
        version: None,
    })
}

#[cfg(unix)]
/// We include the inode when available to yield an ETag more resistant to collisions
/// and as used by popular web servers such as [Apache](https://httpd.apache.org/docs/2.2/mod/core.html#fileetag)
fn get_inode(metadata: &Metadata) -> u64 {
    std::os::unix::fs::MetadataExt::ino(metadata)
}

#[cfg(not(unix))]
/// On platforms where an inode isn't available, fallback to just relying on size and mtime
fn get_inode(metadata: &Metadata) -> u64 {
    0
}

/// Convert walkdir results and converts not-found errors into `None`.
/// Convert broken symlinks to `None`.
fn convert_walkdir_result(
    res: std::result::Result<DirEntry, walkdir::Error>,
) -> Result<Option<DirEntry>> {
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
                ErrorKind::NotFound => Ok(None),
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
    use std::fs;

    use futures::TryStreamExt;
    use tempfile::{NamedTempFile, TempDir};

    use crate::integration::*;

    use super::*;

    #[tokio::test]
    async fn file_test() {
        let root = TempDir::new().unwrap();
        let integration = LocalFileSystem::new_with_prefix(root.path()).unwrap();

        put_get_delete_list(&integration).await;
        get_opts(&integration).await;
        list_uses_directories_correctly(&integration).await;
        list_with_delimiter(&integration).await;
        rename_and_copy(&integration).await;
        copy_if_not_exists(&integration).await;
        copy_rename_nonexistent_object(&integration).await;
        stream_get(&integration).await;
        put_opts(&integration, false).await;
    }

    #[test]
    fn test_non_tokio() {
        let root = TempDir::new().unwrap();
        let integration = LocalFileSystem::new_with_prefix(root.path()).unwrap();
        futures::executor::block_on(async move {
            put_get_delete_list(&integration).await;
            list_uses_directories_correctly(&integration).await;
            list_with_delimiter(&integration).await;

            // Can't use stream_get test as WriteMultipart uses a tokio JoinSet
            let p = Path::from("manual_upload");
            let mut upload = integration.put_multipart(&p).await.unwrap();
            upload.put_part("123".into()).await.unwrap();
            upload.put_part("45678".into()).await.unwrap();
            let r = upload.complete().await.unwrap();

            let get = integration.get(&p).await.unwrap();
            assert_eq!(get.meta.e_tag.as_ref().unwrap(), r.e_tag.as_ref().unwrap());
            let actual = get.bytes().await.unwrap();
            assert_eq!(actual.as_ref(), b"12345678");
        });
    }

    #[tokio::test]
    async fn creates_dir_if_not_present() {
        let root = TempDir::new().unwrap();
        let integration = LocalFileSystem::new_with_prefix(root.path()).unwrap();

        let location = Path::from("nested/file/test_file");

        let data = Bytes::from("arbitrary data");

        integration
            .put(&location, data.clone().into())
            .await
            .unwrap();

        let read_data = integration
            .get(&location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(&*read_data, data);
    }

    #[tokio::test]
    async fn unknown_length() {
        let root = TempDir::new().unwrap();
        let integration = LocalFileSystem::new_with_prefix(root.path()).unwrap();

        let location = Path::from("some_file");

        let data = Bytes::from("arbitrary data");

        integration
            .put(&location, data.clone().into())
            .await
            .unwrap();

        let read_data = integration
            .get(&location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(&*read_data, data);
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

        let mut stream = store.list(None);
        let mut any_err = false;
        while let Some(res) = stream.next().await {
            if res.is_err() {
                any_err = true;
            }
        }
        assert!(any_err);

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
        if let crate::Error::NotFound { path, source } = err {
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

        let roundtrip = integration.path_to_filesystem(&path).unwrap();

        // Needed as on Windows canonicalize returns extended length path syntax
        // C:\Users\circleci -> \\?\C:\Users\circleci
        let roundtrip = roundtrip.canonicalize().unwrap();

        assert_eq!(roundtrip, canonical);

        integration.head(&path).await.unwrap();
    }

    #[tokio::test]
    #[cfg(target_family = "windows")]
    async fn test_list_root() {
        let fs = LocalFileSystem::new();
        let r = fs.list_with_delimiter(None).await.unwrap_err().to_string();

        assert!(
            r.contains("Unable to convert URL \"file:///\" to filesystem path"),
            "{}",
            r
        );
    }

    #[tokio::test]
    #[cfg(target_os = "linux")]
    async fn test_list_root() {
        let fs = LocalFileSystem::new();
        fs.list_with_delimiter(None).await.unwrap();
    }

    async fn check_list(integration: &LocalFileSystem, prefix: Option<&Path>, expected: &[&str]) {
        let result: Vec<_> = integration.list(prefix).try_collect().await.unwrap();

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
        std::os::unix::fs::symlink(other.path(), root.path().join("test.parquet")).unwrap();

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
        std::os::unix::fs::symlink(root.path().join("foo.parquet"), root.path().join("c")).unwrap();

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
            .put(&Path::from("b/file.parquet"), vec![0, 1, 2].into())
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
        integration.put(&object, data.clone().into()).await.unwrap();
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

        let emoji = root.join("💀");
        std::fs::write(emoji, "foo").unwrap();

        // Can list illegal file
        let mut paths = flatten_list_stream(&integration, None).await.unwrap();
        paths.sort_unstable();

        assert_eq!(
            paths,
            vec![
                Path::parse("directory/child.txt").unwrap(),
                Path::parse("💀").unwrap()
            ]
        );
    }

    #[tokio::test]
    async fn list_hides_incomplete_uploads() {
        let root = TempDir::new().unwrap();
        let integration = LocalFileSystem::new_with_prefix(root.path()).unwrap();
        let location = Path::from("some_file");

        let data = PutPayload::from("arbitrary data");
        let mut u1 = integration.put_multipart(&location).await.unwrap();
        u1.put_part(data.clone()).await.unwrap();

        let mut u2 = integration.put_multipart(&location).await.unwrap();
        u2.put_part(data).await.unwrap();

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

        let res: Vec<_> = integration.list(None).try_collect().await.unwrap();
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

    #[test]
    fn test_valid_path() {
        let cases = [
            ("foo#123/test.txt", true),
            ("foo#123/test#23.txt", true),
            ("foo#123/test#34", false),
            ("foo😁/test#34", false),
            ("foo/test#😁34", true),
        ];

        for (case, expected) in cases {
            let path = Path::parse(case).unwrap();
            assert_eq!(is_valid_file_path(&path), expected);
        }
    }

    #[tokio::test]
    async fn test_intermediate_files() {
        let root = TempDir::new().unwrap();
        let integration = LocalFileSystem::new_with_prefix(root.path()).unwrap();

        let a = Path::parse("foo#123/test.txt").unwrap();
        integration.put(&a, "test".into()).await.unwrap();

        let list = flatten_list_stream(&integration, None).await.unwrap();
        assert_eq!(list, vec![a.clone()]);

        std::fs::write(root.path().join("bar#123"), "test").unwrap();

        // Should ignore file
        let list = flatten_list_stream(&integration, None).await.unwrap();
        assert_eq!(list, vec![a.clone()]);

        let b = Path::parse("bar#123").unwrap();
        let err = integration.get(&b).await.unwrap_err().to_string();
        assert_eq!(err, "Generic LocalFileSystem error: Filenames containing trailing '/#\\d+/' are not supported: bar#123");

        let c = Path::parse("foo#123.txt").unwrap();
        integration.put(&c, "test".into()).await.unwrap();

        let mut list = flatten_list_stream(&integration, None).await.unwrap();
        list.sort_unstable();
        assert_eq!(list, vec![c, a]);
    }

    #[tokio::test]
    async fn delete_dirs_automatically() {
        let root = TempDir::new().unwrap();
        let integration = LocalFileSystem::new_with_prefix(root.path())
            .unwrap()
            .with_automatic_cleanup(true);
        let location = Path::from("nested/file/test_file");
        let data = Bytes::from("arbitrary data");

        integration
            .put(&location, data.clone().into())
            .await
            .unwrap();

        let read_data = integration
            .get(&location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();

        assert_eq!(&*read_data, data);
        assert!(fs::read_dir(root.path()).unwrap().count() > 0);
        integration.delete(&location).await.unwrap();
        assert!(fs::read_dir(root.path()).unwrap().count() == 0);
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[cfg(test)]
mod not_wasm_tests {
    use std::time::Duration;
    use tempfile::TempDir;

    use crate::local::LocalFileSystem;
    use crate::{ObjectStore, Path, PutPayload};

    #[tokio::test]
    async fn test_cleanup_intermediate_files() {
        let root = TempDir::new().unwrap();
        let integration = LocalFileSystem::new_with_prefix(root.path()).unwrap();

        let location = Path::from("some_file");
        let data = PutPayload::from_static(b"hello");
        let mut upload = integration.put_multipart(&location).await.unwrap();
        upload.put_part(data).await.unwrap();

        let file_count = std::fs::read_dir(root.path()).unwrap().count();
        assert_eq!(file_count, 1);
        drop(upload);

        for _ in 0..100 {
            tokio::time::sleep(Duration::from_millis(1)).await;
            let file_count = std::fs::read_dir(root.path()).unwrap().count();
            if file_count == 0 {
                return;
            }
        }
        panic!("Failed to cleanup file in 100ms")
    }
}

#[cfg(target_family = "unix")]
#[cfg(test)]
mod unix_test {
    use std::fs::OpenOptions;

    use nix::sys::stat;
    use nix::unistd;
    use tempfile::TempDir;

    use crate::local::LocalFileSystem;
    use crate::{ObjectStore, Path};

    #[tokio::test]
    async fn test_fifo() {
        let filename = "some_file";
        let root = TempDir::new().unwrap();
        let integration = LocalFileSystem::new_with_prefix(root.path()).unwrap();
        let path = root.path().join(filename);
        unistd::mkfifo(&path, stat::Mode::S_IRWXU).unwrap();

        // Need to open read and write side in parallel
        let spawned =
            tokio::task::spawn_blocking(|| OpenOptions::new().write(true).open(path).unwrap());

        let location = Path::from(filename);
        integration.head(&location).await.unwrap();
        integration.get(&location).await.unwrap();

        spawned.await.unwrap();
    }
}
