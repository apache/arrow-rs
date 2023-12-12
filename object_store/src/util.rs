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
use std::{
    fmt::Display,
    ops::{Range, RangeBounds},
};

use super::Result;
use bytes::Bytes;
use futures::{stream::StreamExt, Stream, TryStreamExt};

#[cfg(any(feature = "azure", feature = "http"))]
pub static RFC1123_FMT: &str = "%a, %d %h %Y %T GMT";

// deserialize dates according to rfc1123
#[cfg(any(feature = "azure", feature = "http"))]
pub fn deserialize_rfc1123<'de, D>(
    deserializer: D,
) -> Result<chrono::DateTime<chrono::Utc>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s: String = serde::Deserialize::deserialize(deserializer)?;
    let naive =
        chrono::NaiveDateTime::parse_from_str(&s, RFC1123_FMT).map_err(serde::de::Error::custom)?;
    Ok(chrono::TimeZone::from_utc_datetime(&chrono::Utc, &naive))
}

#[cfg(any(feature = "aws", feature = "azure"))]
pub(crate) fn hmac_sha256(secret: impl AsRef<[u8]>, bytes: impl AsRef<[u8]>) -> ring::hmac::Tag {
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, secret.as_ref());
    ring::hmac::sign(&key, bytes.as_ref())
}

/// Collect a stream into [`Bytes`] avoiding copying in the event of a single chunk
pub async fn collect_bytes<S, E>(mut stream: S, size_hint: Option<usize>) -> Result<Bytes, E>
where
    E: Send,
    S: Stream<Item = Result<Bytes, E>> + Send + Unpin,
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

#[cfg(not(target_arch = "wasm32"))]
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

/// Up to this number of range requests will be performed in parallel by [`coalesce_ranges`]
pub const OBJECT_STORE_COALESCE_PARALLEL: usize = 10;

/// Takes a function `fetch` that can fetch a range of bytes and uses this to
/// fetch the provided byte `ranges`
///
/// To improve performance it will:
///
/// * Combine ranges less than `coalesce` bytes apart into a single call to `fetch`
/// * Make multiple `fetch` requests in parallel (up to maximum of 10)
///
pub async fn coalesce_ranges<F, E, Fut>(
    ranges: &[std::ops::Range<usize>],
    fetch: F,
    coalesce: usize,
) -> Result<Vec<Bytes>, E>
where
    F: Send + FnMut(std::ops::Range<usize>) -> Fut,
    E: Send,
    Fut: std::future::Future<Output = Result<Bytes, E>> + Send,
{
    let fetch_ranges = merge_ranges(ranges, coalesce);

    let fetched: Vec<_> = futures::stream::iter(fetch_ranges.iter().cloned())
        .map(fetch)
        .buffered(OBJECT_STORE_COALESCE_PARALLEL)
        .try_collect()
        .await?;

    Ok(ranges
        .iter()
        .map(|range| {
            let idx = fetch_ranges.partition_point(|v| v.start <= range.start) - 1;
            let fetch_range = &fetch_ranges[idx];
            let fetch_bytes = &fetched[idx];

            let start = range.start - fetch_range.start;
            let end = range.end - fetch_range.start;
            fetch_bytes.slice(start..end.min(fetch_bytes.len()))
        })
        .collect())
}

/// Returns a sorted list of ranges that cover `ranges`
fn merge_ranges(ranges: &[std::ops::Range<usize>], coalesce: usize) -> Vec<std::ops::Range<usize>> {
    if ranges.is_empty() {
        return vec![];
    }

    let mut ranges = ranges.to_vec();
    ranges.sort_unstable_by_key(|range| range.start);

    let mut ret = Vec::with_capacity(ranges.len());
    let mut start_idx = 0;
    let mut end_idx = 1;

    while start_idx != ranges.len() {
        let mut range_end = ranges[start_idx].end;

        while end_idx != ranges.len()
            && ranges[end_idx]
                .start
                .checked_sub(range_end)
                .map(|delta| delta <= coalesce)
                .unwrap_or(true)
        {
            range_end = range_end.max(ranges[end_idx].end);
            end_idx += 1;
        }

        let start = ranges[start_idx].start;
        let end = range_end;
        ret.push(start..end);

        start_idx = end_idx;
        end_idx += 1;
    }

    ret
}

// Fully-featured HttpRange etc. implementation here https://github.com/clbarnes/arrow-rs/blob/httpsuff/object_store/src/util.rs

/// A single range in a `Range` request.
///
/// These can be created from [usize] ranges, like
///
/// ```rust
/// # use byteranges::request::HttpRange;
/// let range1: HttpRange = (50..150).into();
/// let range2: HttpRange = (50..=150).into();
/// let range3: HttpRange = (50..).into();
/// let range4: HttpRange = (..150).into();
/// ```
///
/// At present, this is only used internally.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(crate) enum HttpRange {
    /// A range with a given first and last bytes.
    /// If `last > first`, the response should be empty.
    Range {
        /// Offset of the first requested byte (0-based).
        first: usize,
        /// Offset of the last requested byte; e.g. the range `0-0` will request 1 byte.
        /// If [None], read to end of resource.
        last: usize,
    },
    /// A range defined only by the first byte requested (requests all remaining bytes).
    Offset {
        /// Offset of the first byte requested.
        first: usize,
    },
    /// A range defined as the number of bytes at the end of the resource.
    Suffix {
        /// Number of bytes requested.
        nbytes: usize,
    },
}

impl HttpRange {
    /// Create a new range which only has an offset.
    pub fn new_offset(first: usize) -> Self {
        Self::Offset { first }
    }

    /// Create a new range with a start and end point.
    pub fn new_range(first: usize, last: usize) -> Self {
        Self::Range { first, last }
    }

    /// Create a new suffix, requesting the last `nbytes` bytes of the resource.
    pub fn new_suffix(nbytes: usize) -> Self {
        Self::Suffix { nbytes }
    }

    /// The index of the first byte requested ([None] for suffix).
    pub fn first_byte(&self) -> Option<usize> {
        match self {
            HttpRange::Range { first, .. } => Some(*first),
            HttpRange::Offset { first } => Some(*first),
            HttpRange::Suffix { .. } => None,
        }
    }

    /// The index of the last byte requested ([None] for offset or suffix).
    pub fn last_byte(&self) -> Option<usize> {
        match self {
            HttpRange::Range { first: _, last } => Some(*last),
            HttpRange::Offset { .. } => None,
            HttpRange::Suffix { .. } => None,
        }
    }
}

impl<T: RangeBounds<usize>> From<T> for HttpRange {
    fn from(value: T) -> Self {
        use std::ops::Bound::*;
        let first = match value.start_bound() {
            Included(i) => *i,
            Excluded(i) => i.saturating_add(1),
            Unbounded => 0,
        };
        match value.end_bound() {
            Included(i) => HttpRange::new_range(first, *i),
            Excluded(i) => HttpRange::new_range(first, i.saturating_sub(1)),
            Unbounded => HttpRange::new_offset(first),
        }
    }
}

impl Display for HttpRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HttpRange::Range { first, last } => f.write_fmt(format_args!("{first}-{last}")),
            HttpRange::Offset { first } => f.write_fmt(format_args!("{first}-")),
            HttpRange::Suffix { nbytes } => f.write_fmt(format_args!("-{nbytes}")),
        }
    }
}

pub(crate) fn concrete_range(range: Option<HttpRange>, size: usize) -> Range<usize> {
    let Some(r) = range else {
        return 0..size;
    };
    let start = r.first_byte().unwrap_or(0);
    let end = r
        .last_byte()
        .map_or(size, |b| b.saturating_add(1).min(size));
    start..end
}

#[cfg(test)]
mod tests {
    use crate::Error;

    use super::*;
    use rand::{thread_rng, Rng};
    use std::ops::Range;

    /// Calls coalesce_ranges and validates the returned data is correct
    ///
    /// Returns the fetched ranges
    async fn do_fetch(ranges: Vec<Range<usize>>, coalesce: usize) -> Vec<Range<usize>> {
        let max = ranges.iter().map(|x| x.end).max().unwrap_or(0);
        let src: Vec<_> = (0..max).map(|x| x as u8).collect();

        let mut fetches = vec![];
        let coalesced = coalesce_ranges::<_, Error, _>(
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
    }

    #[tokio::test]
    async fn test_coalesce_ranges() {
        let fetches = do_fetch(vec![], 0).await;
        assert!(fetches.is_empty());

        let fetches = do_fetch(vec![0..3; 1], 0).await;
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
        assert_eq!(fetches, vec![0..9]);

        let fetches = do_fetch(vec![0..1, 5..6, 7..9, 2..3, 4..6], 1).await;
        assert_eq!(fetches, vec![0..9]);

        let fetches = do_fetch(vec![0..1, 6..7, 8..9, 10..14, 9..10], 4).await;
        assert_eq!(fetches, vec![0..1, 6..14]);
    }

    #[tokio::test]
    async fn test_coalesce_fuzz() {
        let mut rand = thread_rng();
        for _ in 0..100 {
            let object_len = rand.gen_range(10..250);
            let range_count = rand.gen_range(0..10);
            let ranges: Vec<_> = (0..range_count)
                .map(|_| {
                    let start = rand.gen_range(0..object_len);
                    let max_len = 20.min(object_len - start);
                    let len = rand.gen_range(0..max_len);
                    start..start + len
                })
                .collect();

            let coalesce = rand.gen_range(1..5);
            let fetches = do_fetch(ranges.clone(), coalesce).await;

            for fetch in fetches.windows(2) {
                assert!(
                    fetch[0].start <= fetch[1].start,
                    "fetches should be sorted, {:?} vs {:?}",
                    fetch[0],
                    fetch[1]
                );

                let delta = fetch[1].end - fetch[0].end;
                assert!(
                    delta > coalesce,
                    "fetches should not overlap by {}, {:?} vs {:?} for {:?}",
                    coalesce,
                    fetch[0],
                    fetch[1],
                    ranges
                );
            }
        }
    }
    #[test]

    fn http_range_str() {
        assert_eq!(HttpRange::new_offset(0).to_string(), "0-");
        assert_eq!(HttpRange::new_range(10, 20).to_string(), "10-20");
        assert_eq!(HttpRange::new_suffix(10).to_string(), "-10");
    }

    #[test]
    fn http_range_from() {
        assert_eq!(
            Into::<HttpRange>::into(10..15),
            HttpRange::new_range(10, 14),
        );
        assert_eq!(
            Into::<HttpRange>::into(10..=15),
            HttpRange::new_range(10, 15),
        );
        assert_eq!(Into::<HttpRange>::into(10..), HttpRange::new_offset(10),);
        assert_eq!(Into::<HttpRange>::into(..=15), HttpRange::new_range(0, 15));
    }
}
