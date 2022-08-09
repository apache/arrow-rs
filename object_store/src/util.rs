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

//! Common logic for interacting with remote object stores
use super::Result;
use bytes::Bytes;
use futures::{stream::StreamExt, Stream};

/// Returns the prefix to be passed to an object store
#[cfg(any(feature = "aws", feature = "gcp", feature = "azure"))]
pub fn format_prefix(prefix: Option<&crate::path::Path>) -> Option<String> {
    prefix
        .filter(|x| !x.as_ref().is_empty())
        .map(|p| format!("{}{}", p.as_ref(), crate::path::DELIMITER))
}

/// Returns a formatted HTTP range header as per
/// <https://httpwg.org/specs/rfc7233.html#header.range>
#[cfg(any(feature = "aws", feature = "gcp"))]
pub fn format_http_range(range: std::ops::Range<usize>) -> String {
    format!("bytes={}-{}", range.start, range.end.saturating_sub(1))
}

/// Collect a stream into [`Bytes`] avoiding copying in the event of a single chunk
pub async fn collect_bytes<S>(mut stream: S, size_hint: Option<usize>) -> Result<Bytes>
where
    S: Stream<Item = Result<Bytes>> + Send + Unpin,
{
    let first = stream.next().await.transpose()?.unwrap_or_default();

    // Avoid copying if single response
    match stream.next().await.transpose()? {
        None => Ok(first),
        Some(second) => {
            let size_hint = size_hint.unwrap_or_else(|| first.len() + second.len());

            let mut buf = Vec::with_capacity(size_hint);
            buf.extend_from_slice(&first);
            buf.extend_from_slice(&second);
            while let Some(maybe_bytes) = stream.next().await {
                buf.extend_from_slice(&maybe_bytes?);
            }

            Ok(buf.into())
        }
    }
}

/// Takes a function and spawns it to a tokio blocking pool if available
pub async fn maybe_spawn_blocking<F, T>(f: F) -> Result<T>
where
    F: FnOnce() -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    match tokio::runtime::Handle::try_current() {
        Ok(runtime) => runtime.spawn_blocking(f).await?,
        Err(_) => f(),
    }
}

/// Range requests with a gap less than or equal to this,
/// will be coalesced into a single request by [`coalesce_ranges`]
pub const OBJECT_STORE_COALESCE_DEFAULT: usize = 1024 * 1024;

/// Takes a function to fetch ranges and coalesces adjacent ranges if they are
/// less than `coalesce` bytes apart. Out of order `ranges` are not coalesced
pub async fn coalesce_ranges<F, Fut>(
    ranges: &[std::ops::Range<usize>],
    mut fetch: F,
    coalesce: usize,
) -> Result<Vec<Bytes>>
where
    F: Send + FnMut(std::ops::Range<usize>) -> Fut,
    Fut: std::future::Future<Output = Result<Bytes>> + Send,
{
    let mut ret = Vec::with_capacity(ranges.len());
    let mut start_idx = 0;
    let mut end_idx = 1;

    while start_idx != ranges.len() {
        while end_idx != ranges.len()
            && ranges[end_idx]
                .start
                .checked_sub(ranges[start_idx].end)
                .map(|delta| delta <= coalesce)
                .unwrap_or(false)
        {
            end_idx += 1;
        }

        let start = ranges[start_idx].start;
        let end = ranges[end_idx - 1].end;
        let bytes = fetch(start..end).await?;
        for range in ranges.iter().take(end_idx).skip(start_idx) {
            ret.push(bytes.slice(range.start - start..range.end - start))
        }
        start_idx = end_idx;
        end_idx += 1;
    }
    Ok(ret)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ops::Range;

    #[tokio::test]
    async fn test_coalesce_ranges() {
        let do_fetch = |ranges: Vec<Range<usize>>, coalesce: usize| async move {
            let max = ranges.iter().map(|x| x.end).max().unwrap_or(0);
            let src: Vec<_> = (0..max).map(|x| x as u8).collect();

            let mut fetches = vec![];
            let coalesced = coalesce_ranges(
                &ranges,
                |range| {
                    fetches.push(range.clone());
                    futures::future::ready(Ok(Bytes::from(src[range].to_vec())))
                },
                coalesce,
            )
            .await
            .unwrap();

            assert_eq!(ranges.len(), coalesced.len());
            for (range, bytes) in ranges.iter().zip(coalesced) {
                assert_eq!(bytes.as_ref(), &src[range.clone()]);
            }
            fetches
        };

        let fetches = do_fetch(vec![], 0).await;
        assert_eq!(fetches, vec![]);

        let fetches = do_fetch(vec![0..3], 0).await;
        assert_eq!(fetches, vec![0..3]);

        let fetches = do_fetch(vec![0..2, 3..5], 0).await;
        assert_eq!(fetches, vec![0..2, 3..5]);

        let fetches = do_fetch(vec![0..1, 1..2], 0).await;
        assert_eq!(fetches, vec![0..2]);

        let fetches = do_fetch(vec![0..1, 2..72], 1).await;
        assert_eq!(fetches, vec![0..72]);

        let fetches = do_fetch(vec![0..1, 56..72, 73..75], 1).await;
        assert_eq!(fetches, vec![0..1, 56..75]);

        let fetches = do_fetch(vec![0..1, 5..6, 7..9, 2..3, 4..6], 1).await;
        assert_eq!(fetches, vec![0..1, 5..9, 2..6]);
    }
}
