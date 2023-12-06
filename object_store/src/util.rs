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

use crate::Error;

use super::Result;
use bytes::Bytes;
use futures::{stream::StreamExt, Stream, TryStreamExt};
use http_content_range::ContentRange;
use hyper::{header::CONTENT_RANGE, StatusCode};
use reqwest::Response;
use snafu::Snafu;

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
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum GetRange {
    /// A range with a given first and last bytes.
    Bounded(Range<usize>),
    /// A range defined only by the first byte requested (requests all remaining bytes).
    Offset(usize),
    /// A range defined as the number of bytes at the end of the resource.
    Suffix(usize),
}

impl PartialOrd for GetRange {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        use std::cmp::Ordering::*;
        use GetRange::*;
        // `Suffix`es cannot be compared with `Range`s or `Offset`s.
        // `Range`s and `Offset`s are compared by their first byte,
        // using the last byte as a tiebreaker (`Offset`s always greater than `Range`s).
        match self {
            Bounded(r1) => match other {
                Bounded(r2) => match r1.start.cmp(&r2.start) {
                    Equal => Some(r1.end.cmp(&r2.end)),
                    o => Some(o),
                },
                Offset(f2) => match r1.start.cmp(f2) {
                    Equal => Some(Less),
                    o => Some(o),
                },
                Suffix { .. } => None,
            },
            Offset(f1) => match other {
                Bounded(r2) => match f1.cmp(&r2.start) {
                    Equal => Some(Greater),
                    o => Some(o),
                },
                Offset(f2) => Some(f1.cmp(f2)),
                Suffix { .. } => None,
            },
            Suffix(b1) => match other {
                Suffix(b2) => Some(b2.cmp(b1)),
                _ => None,
            },
        }
    }
}

#[derive(Debug, Snafu)]
pub enum RangeMergeError {
    #[snafu(display("Ranges could not be merged because exactly one was a suffix"))]
    DifferentTypes,
    #[snafu(display("Ranges could not be merged because they were too far apart"))]
    Spread,
}

#[derive(Debug, Snafu)]
pub enum InvalidGetRange {
    #[snafu(display("Wanted suffix with {expected}B, resource was {actual}B long"))]
    SuffixTooLarge { expected: usize, actual: usize },

    #[snafu(display("Wanted range starting at {expected}, resource was {actual}B long"))]
    StartTooLarge { expected: usize, actual: usize },

    #[snafu(display("Wanted range ending at {expected}, resource was {actual}B long"))]
    EndTooLarge { expected: usize, actual: usize },

    #[snafu(display("Range started at {start} and ended at {end}"))]
    Inconsistent { start: usize, end: usize },
}

impl GetRange {
    /// Create a range representing the whole resource.
    pub fn new_whole() -> Self {
        Self::Offset(0)
    }

    /// Whether this is an offset.
    pub fn is_offset(&self) -> bool {
        match self {
            GetRange::Offset(_) => true,
            _ => false,
        }
    }

    /// Whether this is a range.
    pub fn is_range(&self) -> bool {
        match self {
            GetRange::Bounded(_) => true,
            _ => false,
        }
    }

    /// Whether this is an offset.
    pub fn is_suffix(&self) -> bool {
        match self {
            GetRange::Suffix(_) => true,
            _ => false,
        }
    }

    /// Whether the range has no bytes in it (i.e. the last byte is before the first).
    ///
    /// [None] if the range is an `Offset` or `Suffix`.
    /// The response may still be empty.
    pub fn is_empty(&self) -> Option<bool> {
        match self {
            GetRange::Bounded(r) => Some(r.is_empty()),
            _ => None,
        }
    }

    /// Whether the range represents the entire resource (i.e. it is an `Offset` of 0).
    ///
    /// [None] if the range is a `Range` or `Suffix`.
    /// The response may still be the full resource.
    pub fn is_whole(&self) -> Option<bool> {
        match self {
            GetRange::Offset(first) => Some(first == &0),
            _ => None,
        }
    }

    /// How many bytes the range is requesting.
    ///
    /// Note that the server may respond with a different number of bytes,
    /// depending on the length of the resource and other behaviour.
    pub fn nbytes(&self) -> Option<usize> {
        match self {
            GetRange::Bounded(r) => Some(r.end.saturating_sub(r.start)),
            GetRange::Offset(_) => None,
            GetRange::Suffix(n) => Some(*n),
        }
    }

    /// The index of the first byte requested ([None] for suffix).
    pub fn first_byte(&self) -> Option<usize> {
        match self {
            GetRange::Bounded(r) => Some(r.start),
            GetRange::Offset(o) => Some(*o),
            GetRange::Suffix(_) => None,
        }
    }

    /// The index of the last byte requested ([None] for offset or suffix).
    pub fn last_byte(&self) -> Option<usize> {
        match self {
            GetRange::Bounded(r) => match r.end {
                0 => None,
                n => Some(n),
            },
            GetRange::Offset { .. } => None,
            GetRange::Suffix { .. } => None,
        }
    }

    pub(crate) fn as_range(&self, len: usize) -> Result<Range<usize>, InvalidGetRange> {
        match self {
            Self::Bounded(r) => {
                if r.start >= len {
                    Err(InvalidGetRange::StartTooLarge {
                        expected: r.start,
                        actual: len,
                    })
                } else if r.end <= r.start {
                    Err(InvalidGetRange::Inconsistent {
                        start: r.start,
                        end: r.end,
                    })
                } else if r.end >= len {
                    Err(InvalidGetRange::EndTooLarge {
                        expected: r.end,
                        actual: len,
                    })
                } else {
                    Ok(r.clone())
                }
            }
            Self::Offset(o) => {
                if o >= &len {
                    Err(InvalidGetRange::StartTooLarge {
                        expected: *o,
                        actual: len,
                    })
                } else {
                    Ok(*o..len)
                }
            }
            Self::Suffix(n) => {
                len.checked_sub(*n)
                    .map(|start| start..len)
                    .ok_or(InvalidGetRange::SuffixTooLarge {
                        expected: *n,
                        actual: len,
                    })
            }
        }
    }

    /// Merge two ranges which fall within a certain distance `coalesce` of each other.
    ///
    /// Error if exactly one is a suffix or if the ranges are too far apart.
    pub(crate) fn try_merge(
        &self,
        other: &GetRange,
        coalesce: usize,
    ) -> Result<Self, RangeMergeError> {
        use GetRange::*;

        let (g1, g2) = match self.partial_cmp(other) {
            None => {
                // One is a suffix, the other isn't.
                // This is escapable if one represents the whole resource.
                if let Some(whole) = self.is_whole() {
                    if whole {
                        return Ok(GetRange::new_whole());
                    }
                }
                if let Some(whole) = other.is_whole() {
                    if whole {
                        return Ok(GetRange::new_whole());
                    }
                }
                return Err(RangeMergeError::DifferentTypes);
            }
            Some(o) => match o {
                std::cmp::Ordering::Greater => (other, self),
                _ => (self, other),
            },
        };

        match g1 {
            Bounded(r1) => match g2 {
                Bounded(r2) => {
                    if r2.start <= r1.end.saturating_add(coalesce) {
                        Ok(GetRange::Bounded(r1.start..r1.end.max(r2.end)))
                    } else {
                        Err(RangeMergeError::Spread)
                    }
                }
                Offset(first) => {
                    if first < &(r1.end.saturating_add(coalesce)) {
                        Ok(GetRange::Offset(r1.start))
                    } else {
                        Err(RangeMergeError::Spread)
                    }
                }
                Suffix { .. } => unreachable!(),
            },
            // Either an offset or suffix, both of which would contain the second range.
            _ => Ok(g1.clone()),
        }
    }

    pub fn matches_range(&self, range: Range<usize>, len: usize) -> bool {
        match self {
            Self::Bounded(r) => r == &range,
            Self::Offset(o) => o == &range.start && len == range.end,
            Self::Suffix(n) => range.end == len && range.start == len - n,
        }
    }
}

/// Returns a sorted [Vec] of [HttpRange::Offset] and [HttpRange::Range] that cover `ranges`,
/// and a single [HttpRange::Suffix] if one or more are given.
/// The suffix is also omitted if any of the ranges is the whole resource (`0-`).
///
/// The suffix is returned separately as it may still overlap with the other ranges,
/// so the caller may want to handle it differently.
pub fn merge_get_ranges<T: Into<GetRange> + Clone>(
    ranges: &[T],
    coalesce: usize,
) -> (Vec<GetRange>, Option<GetRange>) {
    if ranges.is_empty() {
        return (vec![], None);
    }
    let mut v = Vec::with_capacity(ranges.len());
    let mut o = None::<usize>;

    for rng in ranges.iter().cloned().map(|r| r.into()) {
        match rng {
            GetRange::Suffix(n) => {
                if let Some(suff) = o {
                    o = Some(suff.max(n));
                } else {
                    o = Some(n);
                }
            }
            _ => v.push(rng),
        }
    }

    let mut ret = Vec::with_capacity(v.len() + 1);
    let suff = o.map(|s| GetRange::Suffix(s));

    if v.is_empty() {
        if let Some(s) = suff.as_ref() {
            ret.push(s.clone());
        }
        return (ret, suff);
    }

    // unwrap is fine because we've already filtered out the suffixes
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let mut it = v.into_iter();
    let mut prev = it.next().unwrap();

    for curr in it {
        match prev {
            GetRange::Offset { .. } => {
                match prev.try_merge(&curr, coalesce) {
                    Ok(r) => ret.push(r),
                    Err(_) => {
                        ret.push(prev);
                        ret.push(curr);
                    }
                }

                let Some(s) = suff else {
                    return (ret, None);
                };

                if ret.last().unwrap().is_whole().unwrap() {
                    return (ret, None);
                } else {
                    return (ret, Some(s));
                }
            }
            GetRange::Bounded { .. } => match prev.try_merge(&curr, coalesce) {
                Ok(r) => ret.push(r),
                Err(_) => {
                    ret.push(prev);
                    ret.push(curr.clone());
                }
            },
            GetRange::Suffix { .. } => unreachable!(),
        }
        prev = curr;
    }

    (ret, suff)
}

impl Display for GetRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GetRange::Bounded(r) => f.write_fmt(format_args!("{}-{}", r.start, r.end - 1)),
            GetRange::Offset(o) => f.write_fmt(format_args!("{o}-")),
            GetRange::Suffix(n) => f.write_fmt(format_args!("-{n}")),
        }
    }
}

impl<T: RangeBounds<usize>> From<T> for GetRange {
    fn from(value: T) -> Self {
        use std::ops::Bound::*;
        let first = match value.start_bound() {
            Included(i) => *i,
            Excluded(i) => i + 1,
            Unbounded => 0,
        };
        match value.end_bound() {
            Included(i) => GetRange::Bounded(first..(i + 1)),
            Excluded(i) => GetRange::Bounded(first..*i),
            Unbounded => GetRange::Offset(first),
        }
    }
}

/// Takes a function `fetch` that can fetch a range of bytes and uses this to
/// fetch the provided byte `ranges`
///
/// To improve performance it will:
///
/// * Combine ranges less than `coalesce` bytes apart into a single call to `fetch`
/// * Make multiple `fetch` requests in parallel (up to maximum of 10)
pub async fn coalesce_get_ranges<F, E, Fut>(
    ranges: &[GetRange],
    fetch: F,
    coalesce: usize,
) -> Result<Vec<Bytes>, E>
where
    F: Send + FnMut(GetRange) -> Fut,
    E: Send,
    Fut: std::future::Future<Output = Result<Bytes, E>> + Send,
{
    let (mut fetch_ranges, suff_opt) = merge_get_ranges(ranges, coalesce);
    if let Some(suff) = suff_opt.as_ref() {
        fetch_ranges.push(suff.clone());
    }
    if fetch_ranges.is_empty() {
        return Ok(vec![]);
    }

    let mut fetched: Vec<_> = futures::stream::iter(fetch_ranges.iter().cloned())
        .map(fetch)
        .buffered(OBJECT_STORE_COALESCE_PARALLEL)
        .try_collect()
        .await?;

    let suff = suff_opt.map(|r| {
        let nbytes = match r {
            GetRange::Suffix(n) => n,
            _ => unreachable!(),
        };
        fetch_ranges.pop().unwrap();
        let b = fetched.pop().unwrap();
        if nbytes >= b.len() {
            b
        } else {
            b.slice((b.len() - nbytes)..)
        }
    });

    Ok(ranges
        .iter()
        .map(|range| {
            match range {
                GetRange::Suffix(n) => {
                    let b = suff.as_ref().unwrap();
                    let start = b.len().saturating_sub(*n);
                    return b.slice(start..b.len());
                }
                _ => (),
            }
            // unwrapping range.first_byte() is fine because we've early-returned suffixes
            let idx = fetch_ranges
                .partition_point(|v| v.first_byte().unwrap() <= range.first_byte().unwrap())
                - 1;
            let fetch_range = &fetch_ranges[idx];
            let fetch_bytes = &fetched[idx];

            let start = range.first_byte().unwrap() - fetch_range.first_byte().unwrap();
            let end = range.last_byte().map_or(fetch_bytes.len(), |range_last| {
                fetch_bytes
                    .len()
                    .max(range_last - fetch_range.first_byte().unwrap() + 1)
            });
            fetch_bytes.slice(start..end)
        })
        .collect())
}

#[derive(Debug, Snafu)]
pub enum InvalidRangeResponse {
    #[snafu(display("Response was not PARTIAL_CONTENT; length {length:?}"))]
    NotPartial { length: Option<usize> },
    #[snafu(display("Content-Range header not present"))]
    NoContentRange,
    #[snafu(display("Content-Range header could not be parsed: {value:?}"))]
    InvalidContentRange { value: Vec<u8> },
}

pub(crate) fn response_range(r: &Response) -> Result<Range<usize>, InvalidRangeResponse> {
    use InvalidRangeResponse::*;

    if r.status() != StatusCode::PARTIAL_CONTENT {
        return Err(NotPartial {
            length: r.content_length().map(|s| s as usize),
        });
    }

    let val_bytes = r
        .headers()
        .get(CONTENT_RANGE)
        .ok_or(NoContentRange)?
        .as_bytes();

    match ContentRange::parse_bytes(val_bytes) {
        ContentRange::Bytes(c) => Ok(c.first_byte as usize..(c.last_byte as usize + 1)),
        ContentRange::UnboundBytes(c) => Ok(c.first_byte as usize..(c.last_byte as usize + 1)),
        _ => Err(InvalidContentRange {
            value: val_bytes.to_vec(),
        }),
    }
}

pub(crate) fn as_generic_err<E: std::error::Error + Send + Sync + 'static>(
    store: &'static str,
    source: E,
) -> Error {
    Error::Generic {
        store,
        source: Box::new(source),
    }
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
        assert_eq!(GetRange::Offset(0).to_string(), "0-");
        assert_eq!(GetRange::Bounded(10..19).to_string(), "10-20");
        assert_eq!(GetRange::Suffix(10).to_string(), "-10");
    }

    #[test]
    fn http_range_from() {
        assert_eq!(Into::<GetRange>::into(10..15), GetRange::Bounded(10..15),);
        assert_eq!(Into::<GetRange>::into(10..=15), GetRange::Bounded(10..16),);
        assert_eq!(Into::<GetRange>::into(10..), GetRange::Offset(10),);
        assert_eq!(Into::<GetRange>::into(..=15), GetRange::Bounded(0..16));
    }

    #[test]
    fn merge_suffixes() {
        assert_eq!(
            GetRange::Suffix(10)
                .try_merge(&GetRange::Suffix(15), 0)
                .unwrap(),
            GetRange::Suffix(15)
        );
    }

    #[test]
    fn merge_range_suffix() {
        assert!(GetRange::Bounded(0..11)
            .try_merge(&GetRange::Suffix(15), 0)
            .is_err());
    }

    #[test]
    fn merge_range_range_spread() {
        assert_eq!(
            GetRange::Bounded(0..11)
                .try_merge(&GetRange::Bounded(11..21), 1)
                .unwrap(),
            GetRange::Bounded(0..21)
        );
    }

    #[test]
    fn merge_offset() {
        assert_eq!(
            GetRange::Offset(10)
                .try_merge(&GetRange::Offset(15), 0)
                .unwrap(),
            GetRange::Offset(10)
        )
    }

    #[test]
    fn merge_offset_range() {
        assert_eq!(
            GetRange::Offset(10)
                .try_merge(&GetRange::Bounded(5..16), 0)
                .unwrap(),
            GetRange::Offset(5)
        )
    }

    #[test]
    fn no_merge_offset_range() {
        assert!(GetRange::Offset(20)
            .try_merge(&GetRange::Bounded(5..16), 0)
            .is_err())
    }
}
