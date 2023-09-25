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

//! Utilities for performing tokio-style buffered IO

use crate::path::Path;
use crate::{ObjectMeta, ObjectStore};
use bytes::Bytes;
use futures::future::{BoxFuture, FutureExt};
use futures::ready;
use std::cmp::Ordering;
use std::io::{Error, ErrorKind, SeekFrom};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncBufRead, AsyncRead, AsyncSeek, ReadBuf};

/// The default buffer size used by [`BufReader`]
pub const DEFAULT_BUFFER_SIZE: usize = 1024 * 1024;

/// An async-buffered reader compatible with the tokio IO traits
///
/// Internally this maintains a buffer of the requested size, and uses [`ObjectStore::get_range`]
/// to populate its internal buffer once depleted. This buffer is cleared on seek.
///
/// Whilst simple, this interface will typically be outperformed by the native [`ObjectStore`]
/// methods that better map to the network APIs. This is because most object stores have
/// very [high first-byte latencies], on the order of 100-200ms, and so avoiding unnecessary
/// round-trips is critical to throughput.
///
/// Systems looking to sequentially scan a file should instead consider using [`ObjectStore::get`],
/// or [`ObjectStore::get_opts`], or [`ObjectStore::get_range`] to read a particular range.
///
/// Systems looking to read multiple ranges of a file should instead consider using
/// [`ObjectStore::get_ranges`], which will optimise the vectored IO.
///
/// [high first-byte latencies]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html
pub struct BufReader {
    /// The object store to fetch data from
    store: Arc<dyn ObjectStore>,
    /// The size of the object
    size: u64,
    /// The path to the object
    path: Path,
    /// The current position in the object
    cursor: u64,
    /// The number of bytes to read in a single request
    capacity: usize,
    /// The buffered data if any
    buffer: Buffer,
}

impl std::fmt::Debug for BufReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BufReader")
            .field("path", &self.path)
            .field("size", &self.size)
            .field("capacity", &self.capacity)
            .finish()
    }
}

enum Buffer {
    Empty,
    Pending(BoxFuture<'static, std::io::Result<Bytes>>),
    Ready(Bytes),
}

impl BufReader {
    /// Create a new [`BufReader`] from the provided [`ObjectMeta`] and [`ObjectStore`]
    pub fn new(store: Arc<dyn ObjectStore>, meta: &ObjectMeta) -> Self {
        Self::with_capacity(store, meta, DEFAULT_BUFFER_SIZE)
    }

    /// Create a new [`BufReader`] from the provided [`ObjectMeta`], [`ObjectStore`], and `capacity`
    pub fn with_capacity(
        store: Arc<dyn ObjectStore>,
        meta: &ObjectMeta,
        capacity: usize,
    ) -> Self {
        Self {
            path: meta.location.clone(),
            size: meta.size as _,
            store,
            capacity,
            cursor: 0,
            buffer: Buffer::Empty,
        }
    }

    fn poll_fill_buf_impl(
        &mut self,
        cx: &mut Context<'_>,
        amnt: usize,
    ) -> Poll<std::io::Result<&[u8]>> {
        let buf = &mut self.buffer;
        loop {
            match buf {
                Buffer::Empty => {
                    let store = Arc::clone(&self.store);
                    let path = self.path.clone();
                    let start = self.cursor.min(self.size) as _;
                    let end = self.cursor.saturating_add(amnt as u64).min(self.size) as _;

                    if start == end {
                        return Poll::Ready(Ok(&[]));
                    }

                    *buf = Buffer::Pending(Box::pin(async move {
                        Ok(store.get_range(&path, start..end).await?)
                    }))
                }
                Buffer::Pending(fut) => match ready!(fut.poll_unpin(cx)) {
                    Ok(b) => *buf = Buffer::Ready(b),
                    Err(e) => return Poll::Ready(Err(e)),
                },
                Buffer::Ready(r) => return Poll::Ready(Ok(r)),
            }
        }
    }
}

impl AsyncSeek for BufReader {
    fn start_seek(mut self: Pin<&mut Self>, position: SeekFrom) -> std::io::Result<()> {
        self.cursor = match position {
            SeekFrom::Start(offset) => offset,
            SeekFrom::End(offset) => {
                checked_add_signed(self.size,offset).ok_or_else(|| Error::new(ErrorKind::InvalidInput, format!("Seeking {offset} from end of {} byte file would result in overflow", self.size)))?
            }
            SeekFrom::Current(offset) => {
                checked_add_signed(self.cursor, offset).ok_or_else(|| Error::new(ErrorKind::InvalidInput, format!("Seeking {offset} from current offset of {} would result in overflow", self.cursor)))?
            }
        };
        self.buffer = Buffer::Empty;
        Ok(())
    }

    fn poll_complete(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<u64>> {
        Poll::Ready(Ok(self.cursor))
    }
}

impl AsyncRead for BufReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        // Read the maximum of the internal buffer and `out`
        let to_read = out.remaining().max(self.capacity);
        let r = match ready!(self.poll_fill_buf_impl(cx, to_read)) {
            Ok(buf) => {
                let to_consume = out.remaining().min(buf.len());
                out.put_slice(&buf[..to_consume]);
                self.consume(to_consume);
                Ok(())
            }
            Err(e) => Err(e),
        };
        Poll::Ready(r)
    }
}

impl AsyncBufRead for BufReader {
    fn poll_fill_buf(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<&[u8]>> {
        let capacity = self.capacity;
        self.get_mut().poll_fill_buf_impl(cx, capacity)
    }

    fn consume(mut self: Pin<&mut Self>, amt: usize) {
        match &mut self.buffer {
            Buffer::Empty => assert_eq!(amt, 0, "cannot consume from empty buffer"),
            Buffer::Ready(b) => match b.len().cmp(&amt) {
                Ordering::Less => panic!("{amt} exceeds buffer sized of {}", b.len()),
                Ordering::Greater => *b = b.slice(amt..),
                Ordering::Equal => self.buffer = Buffer::Empty,
            },
            Buffer::Pending(_) => panic!("cannot consume from pending buffer"),
        }
        self.cursor += amt as u64;
    }
}

/// Port of standardised function as requires Rust 1.66
///
/// <https://github.com/rust-lang/rust/pull/87601/files#diff-b9390ee807a1dae3c3128dce36df56748ad8d23c6e361c0ebba4d744bf6efdb9R1533>
#[inline]
fn checked_add_signed(a: u64, rhs: i64) -> Option<u64> {
    let (res, overflowed) = a.overflowing_add(rhs as _);
    let overflow = overflowed ^ (rhs < 0);
    (!overflow).then_some(res)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::InMemory;
    use crate::path::Path;
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt};

    #[tokio::test]
    async fn test_buf_reader() {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;

        let existent = Path::from("exists.txt");
        const BYTES: usize = 4096;

        let data: Bytes = b"12345678".iter().cycle().copied().take(BYTES).collect();
        store.put(&existent, data.clone()).await.unwrap();

        let meta = store.head(&existent).await.unwrap();

        let mut reader = BufReader::new(Arc::clone(&store), &meta);
        let mut out = Vec::with_capacity(BYTES);
        let read = reader.read_to_end(&mut out).await.unwrap();

        assert_eq!(read, BYTES);
        assert_eq!(&out, &data);

        let err = reader.seek(SeekFrom::Current(i64::MIN)).await.unwrap_err();
        assert_eq!(err.to_string(), "Seeking -9223372036854775808 from current offset of 4096 would result in overflow");

        reader.rewind().await.unwrap();

        let err = reader.seek(SeekFrom::Current(-1)).await.unwrap_err();
        assert_eq!(
            err.to_string(),
            "Seeking -1 from current offset of 0 would result in overflow"
        );

        // Seeking beyond the bounds of the file is permitted but should return no data
        reader.seek(SeekFrom::Start(u64::MAX)).await.unwrap();
        let buf = reader.fill_buf().await.unwrap();
        assert!(buf.is_empty());

        let err = reader.seek(SeekFrom::Current(1)).await.unwrap_err();
        assert_eq!(err.to_string(), "Seeking 1 from current offset of 18446744073709551615 would result in overflow");

        for capacity in [200, 1024, 4096, DEFAULT_BUFFER_SIZE] {
            let store = Arc::clone(&store);
            let mut reader = BufReader::with_capacity(store, &meta, capacity);

            let mut bytes_read = 0;
            loop {
                let buf = reader.fill_buf().await.unwrap();
                if buf.is_empty() {
                    assert_eq!(bytes_read, BYTES);
                    break;
                }
                assert!(buf.starts_with(b"12345678"));
                bytes_read += 8;
                reader.consume(8);
            }

            let mut buf = Vec::with_capacity(76);
            reader.seek(SeekFrom::Current(-76)).await.unwrap();
            reader.read_to_end(&mut buf).await.unwrap();
            assert_eq!(&buf, &data[BYTES - 76..]);

            reader.rewind().await.unwrap();
            let buffer = reader.fill_buf().await.unwrap();
            assert_eq!(buffer, &data[..capacity.min(BYTES)]);

            reader.seek(SeekFrom::Start(325)).await.unwrap();
            let buffer = reader.fill_buf().await.unwrap();
            assert_eq!(buffer, &data[325..(325 + capacity).min(BYTES)]);

            reader.seek(SeekFrom::End(0)).await.unwrap();
            let buffer = reader.fill_buf().await.unwrap();
            assert!(buffer.is_empty());
        }
    }
}
