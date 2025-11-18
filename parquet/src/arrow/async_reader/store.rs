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

use std::{ops::Range, sync::Arc};

use crate::arrow::arrow_reader::ArrowReaderOptions;
use crate::arrow::async_reader::{AsyncFileReader, MetadataSuffixFetch};
use crate::errors::{ParquetError, Result};
use crate::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};
use bytes::Bytes;
use futures::{FutureExt, TryFutureExt, future::BoxFuture};
use object_store::{GetOptions, GetRange};
use object_store::{ObjectStore, path::Path};
use tokio::runtime::Handle;

/// Reads Parquet files in object storage using [`ObjectStore`].
///
/// ```no_run
/// # use std::io::stdout;
/// # use std::sync::Arc;
/// # use object_store::azure::MicrosoftAzureBuilder;
/// # use object_store::ObjectStore;
/// # use object_store::path::Path;
/// # use parquet::arrow::async_reader::ParquetObjectReader;
/// # use parquet::arrow::ParquetRecordBatchStreamBuilder;
/// # use parquet::schema::printer::print_parquet_metadata;
/// # async fn run() {
/// // Populate configuration from environment
/// let storage_container = Arc::new(MicrosoftAzureBuilder::from_env().build().unwrap());
/// let location = Path::from("path/to/blob.parquet");
/// let meta = storage_container.head(&location).await.unwrap();
/// println!("Found Blob with {}B at {}", meta.size, meta.location);
///
/// // Show Parquet metadata
/// let reader = ParquetObjectReader::new(storage_container, meta.location).with_file_size(meta.size);
/// let builder = ParquetRecordBatchStreamBuilder::new(reader).await.unwrap();
/// print_parquet_metadata(&mut stdout(), builder.metadata());
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct ParquetObjectReader {
    store: Arc<dyn ObjectStore>,
    path: Path,
    file_size: Option<u64>,
    metadata_size_hint: Option<usize>,
    preload_column_index: bool,
    preload_offset_index: bool,
    runtime: Option<Handle>,
}

impl ParquetObjectReader {
    /// Creates a new [`ParquetObjectReader`] for the provided [`ObjectStore`] and [`Path`].
    pub fn new(store: Arc<dyn ObjectStore>, path: Path) -> Self {
        Self {
            store,
            path,
            file_size: None,
            metadata_size_hint: None,
            preload_column_index: false,
            preload_offset_index: false,
            runtime: None,
        }
    }

    /// Provide a hint as to the size of the parquet file's footer,
    /// see [`ParquetMetaDataReader::with_prefetch_hint`]
    pub fn with_footer_size_hint(self, hint: usize) -> Self {
        Self {
            metadata_size_hint: Some(hint),
            ..self
        }
    }

    /// Provide the byte size of this file.
    ///
    /// If provided, the file size will ensure that only bounded range requests are used. If file
    /// size is not provided, the reader will use suffix range requests to fetch the metadata.
    ///
    /// Providing this size up front is an important optimization to avoid extra calls when the
    /// underlying store does not support suffix range requests.
    ///
    /// The file size can be obtained using [`ObjectStore::list`] or [`ObjectStore::head`].
    pub fn with_file_size(self, file_size: u64) -> Self {
        Self {
            file_size: Some(file_size),
            ..self
        }
    }

    /// Whether to load the Column Index as part of [`Self::get_metadata`]
    ///
    /// Note: This setting may be overridden by [`ArrowReaderOptions`] `page_index_policy`.
    /// If `page_index_policy` is `Optional` or `Required`, it will take precedence
    /// over this preload flag. When it is `Skip` (default), this flag is used.
    pub fn with_preload_column_index(self, preload_column_index: bool) -> Self {
        Self {
            preload_column_index,
            ..self
        }
    }

    /// Whether to load the Offset Index as part of [`Self::get_metadata`]
    ///
    /// Note: This setting may be overridden by [`ArrowReaderOptions`] `page_index_policy`.
    /// If `page_index_policy` is `Optional` or `Required`, it will take precedence
    /// over this preload flag. When it is `Skip` (default), this flag is used.
    pub fn with_preload_offset_index(self, preload_offset_index: bool) -> Self {
        Self {
            preload_offset_index,
            ..self
        }
    }

    /// Perform IO on the provided tokio runtime
    ///
    /// Tokio is a cooperative scheduler, and relies on tasks yielding in a timely manner
    /// to service IO. Therefore, running IO and CPU-bound tasks, such as parquet decoding,
    /// on the same tokio runtime can lead to degraded throughput, dropped connections and
    /// other issues. For more information see [here].
    ///
    /// [here]: https://www.influxdata.com/blog/using-rustlangs-async-tokio-runtime-for-cpu-bound-tasks/
    pub fn with_runtime(self, handle: Handle) -> Self {
        Self {
            runtime: Some(handle),
            ..self
        }
    }

    fn spawn<F, O, E>(&self, f: F) -> BoxFuture<'_, Result<O>>
    where
        F: for<'a> FnOnce(&'a Arc<dyn ObjectStore>, &'a Path) -> BoxFuture<'a, Result<O, E>>
            + Send
            + 'static,
        O: Send + 'static,
        E: Into<ParquetError> + Send + 'static,
    {
        match &self.runtime {
            Some(handle) => {
                let path = self.path.clone();
                let store = Arc::clone(&self.store);
                handle
                    .spawn(async move { f(&store, &path).await })
                    .map_ok_or_else(
                        |e| match e.try_into_panic() {
                            Err(e) => Err(ParquetError::External(Box::new(e))),
                            Ok(p) => std::panic::resume_unwind(p),
                        },
                        |res| res.map_err(|e| e.into()),
                    )
                    .boxed()
            }
            None => f(&self.store, &self.path).map_err(|e| e.into()).boxed(),
        }
    }
}

impl MetadataSuffixFetch for &mut ParquetObjectReader {
    fn fetch_suffix(&mut self, suffix: usize) -> BoxFuture<'_, Result<Bytes>> {
        let options = GetOptions {
            range: Some(GetRange::Suffix(suffix as u64)),
            ..Default::default()
        };
        self.spawn(|store, path| {
            async move {
                let resp = store.get_opts(path, options).await?;
                Ok::<_, ParquetError>(resp.bytes().await?)
            }
            .boxed()
        })
    }
}

impl AsyncFileReader for ParquetObjectReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes>> {
        self.spawn(|store, path| store.get_range(path, range))
    }

    fn get_byte_ranges(&mut self, ranges: Vec<Range<u64>>) -> BoxFuture<'_, Result<Vec<Bytes>>>
    where
        Self: Send,
    {
        self.spawn(|store, path| async move { store.get_ranges(path, &ranges).await }.boxed())
    }

    // This method doesn't directly call `self.spawn` because all of the IO that is done down the
    // line due to this method call is done through `self.get_bytes` and/or `self.get_byte_ranges`.
    // When `self` is passed into `ParquetMetaDataReader::load_and_finish`, it treats it as
    // an `impl MetadataFetch` and calls those methods to get data from it. Due to `Self`'s impl of
    // `AsyncFileReader`, the calls to `MetadataFetch::fetch` are just delegated to
    // `Self::get_bytes`.
    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, Result<Arc<ParquetMetaData>>> {
        Box::pin(async move {
            let metadata_opts = options.map(|o| o.metadata_options().clone());
            let mut metadata = ParquetMetaDataReader::new()
                .with_metadata_options(metadata_opts)
                .with_column_index_policy(PageIndexPolicy::from(self.preload_column_index))
                .with_offset_index_policy(PageIndexPolicy::from(self.preload_offset_index))
                .with_prefetch_hint(self.metadata_size_hint);

            #[cfg(feature = "encryption")]
            if let Some(options) = options {
                metadata = metadata.with_decryption_properties(
                    options.file_decryption_properties.as_ref().map(Arc::clone),
                );
            }

            // Override page index policies from ArrowReaderOptions if specified and not Skip.
            // When page_index_policy is Skip (default), use the reader's preload flags.
            // When page_index_policy is Optional or Required, override the preload flags
            // to ensure the specified policy takes precedence.
            if let Some(options) = options {
                if options.page_index_policy != PageIndexPolicy::Skip {
                    metadata = metadata.with_page_index_policy(options.page_index_policy);
                }
            }

            let metadata = if let Some(file_size) = self.file_size {
                metadata.load_and_finish(self, file_size).await?
            } else {
                metadata.load_via_suffix_and_finish(self).await?
            };

            Ok(Arc::new(metadata))
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::arrow::async_reader::ArrowReaderOptions;
    use crate::file::metadata::PageIndexPolicy;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use futures::TryStreamExt;

    use crate::arrow::ParquetRecordBatchStreamBuilder;
    use crate::arrow::async_reader::{AsyncFileReader, ParquetObjectReader};
    use crate::errors::ParquetError;
    use arrow::util::test_util::parquet_test_data;
    use futures::FutureExt;
    use object_store::local::LocalFileSystem;
    use object_store::path::Path;
    use object_store::{ObjectMeta, ObjectStore};

    async fn get_meta_store() -> (ObjectMeta, Arc<dyn ObjectStore>) {
        let res = parquet_test_data();
        let store = LocalFileSystem::new_with_prefix(res).unwrap();

        let meta = store
            .head(&Path::from("alltypes_plain.parquet"))
            .await
            .unwrap();

        (meta, Arc::new(store) as Arc<dyn ObjectStore>)
    }

    async fn get_meta_store_with_page_index() -> (ObjectMeta, Arc<dyn ObjectStore>) {
        let res = parquet_test_data();
        let store = LocalFileSystem::new_with_prefix(res).unwrap();

        let meta = store
            .head(&Path::from("alltypes_tiny_pages_plain.parquet"))
            .await
            .unwrap();

        (meta, Arc::new(store) as Arc<dyn ObjectStore>)
    }

    #[tokio::test]
    async fn test_simple() {
        let (meta, store) = get_meta_store().await;
        let object_reader =
            ParquetObjectReader::new(store, meta.location).with_file_size(meta.size);

        let builder = ParquetRecordBatchStreamBuilder::new(object_reader)
            .await
            .unwrap();
        let batches: Vec<_> = builder.build().unwrap().try_collect().await.unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 8);
    }

    #[tokio::test]
    async fn test_simple_without_file_length() {
        let (meta, store) = get_meta_store().await;
        let object_reader = ParquetObjectReader::new(store, meta.location);

        let builder = ParquetRecordBatchStreamBuilder::new(object_reader)
            .await
            .unwrap();
        let batches: Vec<_> = builder.build().unwrap().try_collect().await.unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 8);
    }

    #[tokio::test]
    async fn test_not_found() {
        let (mut meta, store) = get_meta_store().await;
        meta.location = Path::from("I don't exist.parquet");

        let object_reader =
            ParquetObjectReader::new(store, meta.location).with_file_size(meta.size);
        // Cannot use unwrap_err as ParquetRecordBatchStreamBuilder: !Debug
        match ParquetRecordBatchStreamBuilder::new(object_reader).await {
            Ok(_) => panic!("expected failure"),
            Err(e) => {
                let err = e.to_string();
                assert!(err.contains("I don't exist.parquet not found:"), "{err}",);
            }
        }
    }

    #[tokio::test]
    async fn test_runtime_is_used() {
        let num_actions = Arc::new(AtomicUsize::new(0));

        let (a1, a2) = (num_actions.clone(), num_actions.clone());
        let rt = tokio::runtime::Builder::new_multi_thread()
            .on_thread_park(move || {
                a1.fetch_add(1, Ordering::Relaxed);
            })
            .on_thread_unpark(move || {
                a2.fetch_add(1, Ordering::Relaxed);
            })
            .build()
            .unwrap();

        let (meta, store) = get_meta_store().await;

        let initial_actions = num_actions.load(Ordering::Relaxed);

        let reader = ParquetObjectReader::new(store, meta.location)
            .with_file_size(meta.size)
            .with_runtime(rt.handle().clone());

        let builder = ParquetRecordBatchStreamBuilder::new(reader).await.unwrap();
        let batches: Vec<_> = builder.build().unwrap().try_collect().await.unwrap();

        // Just copied these assert_eqs from the `test_simple` above
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 8);

        assert!(num_actions.load(Ordering::Relaxed) - initial_actions > 0);

        // Runtimes have to be dropped in blocking contexts, so we need to move this one to a new
        // blocking thread to drop it.
        tokio::runtime::Handle::current().spawn_blocking(move || drop(rt));
    }

    /// Unit test that `ParquetObjectReader::spawn`spawns on the provided runtime
    #[tokio::test]
    async fn test_runtime_thread_id_different() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .build()
            .unwrap();

        let (meta, store) = get_meta_store().await;

        let reader = ParquetObjectReader::new(store, meta.location)
            .with_file_size(meta.size)
            .with_runtime(rt.handle().clone());

        let current_id = std::thread::current().id();

        let other_id = reader
            .spawn(|_, _| async move { Ok::<_, ParquetError>(std::thread::current().id()) }.boxed())
            .await
            .unwrap();

        assert_ne!(current_id, other_id);

        tokio::runtime::Handle::current().spawn_blocking(move || drop(rt));
    }

    #[tokio::test]
    async fn io_fails_on_shutdown_runtime() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .build()
            .unwrap();

        let (meta, store) = get_meta_store().await;

        let mut reader = ParquetObjectReader::new(store, meta.location)
            .with_file_size(meta.size)
            .with_runtime(rt.handle().clone());

        rt.shutdown_background();

        let err = reader.get_bytes(0..1).await.unwrap_err().to_string();

        assert!(err.to_string().contains("was cancelled"));
    }

    #[tokio::test]
    async fn test_page_index_policy_skip_uses_preload_true() {
        let (meta, store) = get_meta_store_with_page_index().await;

        // Create reader with preload flags set to true
        let mut reader = ParquetObjectReader::new(store.clone(), meta.location.clone())
            .with_file_size(meta.size)
            .with_preload_column_index(true)
            .with_preload_offset_index(true);

        // Create options with page_index_policy set to Skip (default)
        let mut options = ArrowReaderOptions::new();
        options.page_index_policy = PageIndexPolicy::Skip;

        // Get metadata - Skip means use reader's preload flags (true)
        let metadata = reader.get_metadata(Some(&options)).await.unwrap();

        // With preload=true, indexes should be loaded since the test file has them
        assert!(metadata.column_index().is_some());
    }

    #[tokio::test]
    async fn test_page_index_policy_optional_overrides_preload_false() {
        let (meta, store) = get_meta_store_with_page_index().await;

        // Create reader with preload flags set to false
        let mut reader = ParquetObjectReader::new(store.clone(), meta.location.clone())
            .with_file_size(meta.size)
            .with_preload_column_index(false)
            .with_preload_offset_index(false);

        // Create options with page_index_policy set to Optional
        let mut options = ArrowReaderOptions::new();
        options.page_index_policy = PageIndexPolicy::Optional;

        // Get metadata - Optional overrides preload flags and attempts to load indexes
        let metadata = reader.get_metadata(Some(&options)).await.unwrap();

        // With Optional policy, it will TRY to load indexes but won't fail if they don't exist
        // The test file has page indexes, so they will be some
        assert!(metadata.column_index().is_some());
    }

    #[tokio::test]
    async fn test_page_index_policy_optional_vs_skip() {
        let (meta, store) = get_meta_store_with_page_index().await;

        // Test 1: preload=false + Skip policy -> uses preload flags (false)
        let mut reader1 = ParquetObjectReader::new(store.clone(), meta.location.clone())
            .with_file_size(meta.size)
            .with_preload_column_index(false)
            .with_preload_offset_index(false);

        let mut options1 = ArrowReaderOptions::new();
        options1.page_index_policy = PageIndexPolicy::Skip;
        let metadata1 = reader1.get_metadata(Some(&options1)).await.unwrap();

        // Test 2: preload=false + Optional policy -> overrides to try loading
        let mut reader2 = ParquetObjectReader::new(store.clone(), meta.location.clone())
            .with_file_size(meta.size)
            .with_preload_column_index(false)
            .with_preload_offset_index(false);

        let mut options2 = ArrowReaderOptions::new();
        options2.page_index_policy = PageIndexPolicy::Optional;
        let metadata2 = reader2.get_metadata(Some(&options2)).await.unwrap();

        // Both should succeed (no panic/error)
        // metadata1 (Skip) uses preload=false -> Skip policy
        // metadata2 (Optional) overrides preload=false -> Optional policy
        assert!(metadata1.column_index().is_none());
        assert!(metadata2.column_index().is_some());
    }

    #[tokio::test]
    async fn test_page_index_policy_no_options_uses_preload() {
        let (meta, store) = get_meta_store_with_page_index().await;

        // Create reader with preload flags set to true
        let mut reader = ParquetObjectReader::new(store, meta.location)
            .with_file_size(meta.size)
            .with_preload_column_index(true)
            .with_preload_offset_index(true);

        // Get metadata without options - should use reader's preload flags
        let metadata = reader.get_metadata(None).await.unwrap();

        // With no options provided, preload flags (true) should be respected
        // and converted to Optional policy internally (preload=true -> Optional)
        // The test file has page indexes, so they will be some
        assert!(metadata.column_index().is_some() && metadata.column_index().is_some());
    }
}
