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

//! [`PageStoreReader`] — reads Arrow data from a content-addressed page store.

use std::collections::BTreeMap;
use std::fs;
use std::io::{self, Cursor};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;

use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, SchemaRef};

use super::{PageStoreManifest, MANIFEST_KEY};
use crate::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};
use crate::errors::Result;
use crate::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};
use crate::file::reader::{ChunkReader, Length};

/// Reads Parquet data from a content-addressed page store.
///
/// Takes a metadata-only Parquet file (written by [`super::PageStoreWriter`])
/// and the `store_dir` that holds the `{hash}.page` blobs. The metadata file
/// can live anywhere — it does not need to be inside `store_dir`.
///
/// Pages are read on-demand from the store directory — only the pages
/// needed for the requested row groups are loaded into memory.
///
/// # Example
/// ```no_run
/// # use parquet::arrow::page_store::PageStoreReader;
/// let reader = PageStoreReader::try_new(
///     "/data/tables/my_table.parquet",
///     "/data/pages",
/// ).unwrap();
/// let batches = reader.read_batches().unwrap();
/// ```
pub struct PageStoreReader {
    store_dir: PathBuf,
    metadata: Arc<ParquetMetaData>,
    manifest: PageStoreManifest,
}

impl std::fmt::Debug for PageStoreReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PageStoreReader")
            .field("store_dir", &self.store_dir)
            .field("num_pages", &self.manifest.pages.len())
            .finish()
    }
}

impl PageStoreReader {
    /// Open a page-store-backed Parquet file.
    ///
    /// * `metadata_path` — path to the metadata-only `.parquet` file.
    /// * `store_dir` — directory containing `{hash}.page` blobs.
    pub fn try_new(
        metadata_path: impl AsRef<Path>,
        store_dir: impl Into<PathBuf>,
    ) -> Result<Self> {
        let store_dir = store_dir.into();
        let file = fs::File::open(metadata_path.as_ref())?;

        let metadata = ParquetMetaDataReader::new()
            .with_page_index_policy(PageIndexPolicy::Required)
            .parse_and_finish(&file)?;

        let manifest = Self::parse_manifest(&metadata)?;

        Ok(Self {
            store_dir,
            metadata: Arc::new(metadata),
            manifest,
        })
    }

    /// Returns a reference to the Parquet metadata.
    pub fn metadata(&self) -> &ParquetMetaData {
        &self.metadata
    }

    /// Returns the manifest with all page references.
    pub fn manifest(&self) -> &PageStoreManifest {
        &self.manifest
    }

    /// Returns the Arrow schema.
    pub fn schema(&self) -> std::result::Result<SchemaRef, ArrowError> {
        let parquet_schema = self.metadata.file_metadata().schema_descr();
        Ok(Arc::new(crate::arrow::parquet_to_arrow_schema(
            parquet_schema,
            self.metadata.file_metadata().key_value_metadata(),
        )?))
    }

    /// Build a streaming [`ParquetRecordBatchReader`] over the page store.
    ///
    /// Prefer this over [`Self::read_batches`] for large files — batches are
    /// decoded on-demand and only one batch is held in memory at a time.
    pub fn reader(&self) -> Result<crate::arrow::arrow_reader::ParquetRecordBatchReader> {
        let chunk_reader = PageStoreChunkReader::new(self.store_dir.clone(), &self.manifest);
        let options =
            ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required);
        let arrow_metadata = ArrowReaderMetadata::try_new(Arc::clone(&self.metadata), options)?;
        ParquetRecordBatchReaderBuilder::new_with_metadata(chunk_reader, arrow_metadata).build()
    }

    /// Read all data from the page store and return as [`RecordBatch`]es.
    ///
    /// Convenient for small datasets and tests. For large files use
    /// [`Self::reader`] to stream batches one at a time.
    pub fn read_batches(&self) -> Result<Vec<RecordBatch>> {
        self.reader()?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| crate::errors::ParquetError::General(e.to_string()))
    }

    fn parse_manifest(metadata: &ParquetMetaData) -> Result<PageStoreManifest> {
        let kv = metadata
            .file_metadata()
            .key_value_metadata()
            .and_then(|kvs| kvs.iter().find(|kv| kv.key == MANIFEST_KEY))
            .ok_or_else(|| {
                crate::errors::ParquetError::General(format!(
                    "Missing '{MANIFEST_KEY}' in parquet key-value metadata"
                ))
            })?;

        let value = kv.value.as_ref().ok_or_else(|| {
            crate::errors::ParquetError::General(format!("'{MANIFEST_KEY}' has no value"))
        })?;

        serde_json::from_str(value)
            .map_err(|e| crate::errors::ParquetError::General(e.to_string()))
    }
}

// ---------------------------------------------------------------------------
// PageStoreChunkReader — on-demand ChunkReader backed by page blobs
// ---------------------------------------------------------------------------

/// A [`ChunkReader`] that serves byte ranges from page store blobs.
///
/// Builds a sorted interval map from the metadata offsets to page file hashes,
/// so that any byte-range request from the Parquet decoder is resolved by
/// reading only the appropriate `.page` file(s) from disk.
pub struct PageStoreChunkReader {
    store_dir: PathBuf,
    /// Sorted map: virtual file offset -> (size, hash).
    pages: BTreeMap<i64, (i32, String)>,
    /// Virtual file length (max offset + size across all pages).
    total_len: u64,
}

impl PageStoreChunkReader {
    fn new(store_dir: PathBuf, manifest: &PageStoreManifest) -> Self {
        let mut pages = BTreeMap::new();
        let mut total_len: u64 = 0;
        for pr in &manifest.pages {
            pages.insert(pr.offset, (pr.size, pr.hash.clone()));
            let end = pr.offset as u64 + pr.size as u64;
            if end > total_len {
                total_len = end;
            }
        }
        Self {
            store_dir,
            pages,
            total_len,
        }
    }

    fn read_page_file(&self, hash: &str) -> io::Result<Bytes> {
        let path = self.store_dir.join(format!("{hash}.page"));
        let data = fs::read(&path)?;
        Ok(Bytes::from(data))
    }
}

impl Length for PageStoreChunkReader {
    fn len(&self) -> u64 {
        self.total_len
    }
}

impl ChunkReader for PageStoreChunkReader {
    type T = Cursor<Bytes>;

    fn get_read(&self, start: u64) -> Result<Self::T> {
        let bytes = self.get_bytes(start, (self.total_len - start) as usize)?;
        Ok(Cursor::new(bytes))
    }

    fn get_bytes(&self, start: u64, length: usize) -> Result<Bytes> {
        let end = start as i64 + length as i64;
        let mut result = Vec::with_capacity(length);

        let scan_start = self
            .pages
            .range(..=start as i64)
            .next_back()
            .map(|(&o, _)| o)
            .unwrap_or(0);

        for (&offset, (size, hash)) in self.pages.range(scan_start..) {
            if offset >= end {
                break;
            }

            let page_data = self.read_page_file(hash)?;

            let copy_start = (start as i64 - offset).max(0) as usize;
            let copy_end = (end - offset).min(*size as i64) as usize;

            if copy_start < copy_end && copy_start < page_data.len() {
                let actual_end = copy_end.min(page_data.len());
                result.extend_from_slice(&page_data[copy_start..actual_end]);
            }

            if result.len() >= length {
                break;
            }
        }

        result.truncate(length);
        Ok(Bytes::from(result))
    }
}
