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

use crate::errors::AvroError;
use bytes::Bytes;
use futures::FutureExt;
use futures::future::BoxFuture;
use std::io::SeekFrom;
use std::ops::Range;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt};

/// The asynchronous interface used by [`super::AsyncAvroFileReader`] to read avro files
///
/// Notes:
///
/// 1. There is a default implementation for types that implement [`AsyncRead`]
///    and [`AsyncSeek`], for example [`tokio::fs::File`].
///
/// 2. Implementations for remote storage, such as the [`object_store`] crate,
///    can implement this interface directly, typically by pairing a store
///    handle with an object path and delegating [`Self::get_bytes`] and
///    [`Self::get_byte_ranges`] to ranged reads. [`super::SpawnedReader`] can
///    wrap such a reader to perform its I/O on a dedicated tokio runtime.
///
/// [`object_store`]: https://crates.io/crates/object_store
///
/// # Example: implementing `AsyncFileReader` for the `object_store` crate
///
/// ```no_run
/// # use std::ops::Range;
/// # use std::sync::Arc;
/// use arrow_avro::errors::AvroError;
/// use arrow_avro::reader::AsyncFileReader;
/// use bytes::Bytes;
/// use futures::FutureExt;
/// use futures::future::BoxFuture;
/// use object_store::path::Path;
/// use object_store::{ObjectStore, ObjectStoreExt};
///
/// #[derive(Clone, Debug)]
/// struct ObjectStoreReader {
///     store: Arc<dyn ObjectStore>,
///     path: Path,
/// }
///
/// impl AsyncFileReader for ObjectStoreReader {
///     fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes, AvroError>> {
///         async move {
///             self.store
///                 .get_range(&self.path, range)
///                 .await
///                 .map_err(|e| AvroError::General(e.to_string()))
///         }
///         .boxed()
///     }
///
///     fn get_byte_ranges(
///         &mut self,
///         ranges: Vec<Range<u64>>,
///     ) -> BoxFuture<'_, Result<Vec<Bytes>, AvroError>> {
///         async move {
///             self.store
///                 .get_ranges(&self.path, &ranges)
///                 .await
///                 .map_err(|e| AvroError::General(e.to_string()))
///         }
///         .boxed()
///     }
/// }
/// ```
///
/// [`tokio::fs::File`]: https://docs.rs/tokio/latest/tokio/fs/struct.File.html
pub trait AsyncFileReader: Send {
    /// Retrieve the bytes in `range`
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes, AvroError>>;

    /// Retrieve multiple byte ranges. The default implementation will call `get_bytes` sequentially
    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, Result<Vec<Bytes>, AvroError>> {
        async move {
            let mut result = Vec::with_capacity(ranges.len());

            for range in ranges {
                let data = self.get_bytes(range).await?;
                result.push(data);
            }

            Ok(result)
        }
        .boxed()
    }
}

/// This allows Box<dyn AsyncFileReader + '_> to be used as an AsyncFileReader,
impl AsyncFileReader for Box<dyn AsyncFileReader + '_> {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes, AvroError>> {
        self.as_mut().get_bytes(range)
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, Result<Vec<Bytes>, AvroError>> {
        self.as_mut().get_byte_ranges(ranges)
    }
}

impl<T: AsyncRead + AsyncSeek + Unpin + Send> AsyncFileReader for T {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes, AvroError>> {
        async move {
            self.seek(SeekFrom::Start(range.start)).await?;

            let to_read = range.end - range.start;
            let mut buffer = Vec::with_capacity(to_read as usize);
            let read = self.take(to_read).read_to_end(&mut buffer).await?;
            if read as u64 != to_read {
                return Err(AvroError::EOF(format!(
                    "expected to read {} bytes, got {}",
                    to_read, read
                )));
            }

            Ok(buffer.into())
        }
        .boxed()
    }
}
