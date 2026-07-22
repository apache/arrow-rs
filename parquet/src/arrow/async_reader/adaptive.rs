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

//! Adaptive fetching for [`ParquetRecordBatchStream`]: small ranges are
//! coalesced and materialized exactly as before, large column chunks are
//! fetched with a single ranged request each whose body feeds the decoder
//! incrementally through a bounded buffer.
//!
//! See [`BoundedStreamingOptions`] for the user-facing description.
//!
//! [`ParquetRecordBatchStream`]: super::ParquetRecordBatchStream

use crate::arrow::arrow_reader::BoundedStreamingOptions;
use crate::arrow::async_reader::AsyncFileReader;
use crate::arrow::push_decoder::UpcomingFetchPlan;
use crate::errors::{ParquetError, Result};
use bytes::Bytes;
use futures::FutureExt;
use futures::future::BoxFuture;
use futures::stream::{BoxStream, StreamExt};
use std::collections::{HashSet, VecDeque};
use std::ops::Range;
use std::task::{Context, Poll};
use tokio::runtime::Handle;
use tokio::sync::mpsc;

/// Target size of a single message from a body-draining task to the fetcher.
/// Small network chunks are aggregated up to roughly this size so the
/// channel's message-count capacity corresponds to a byte bound.
const TARGET_MESSAGE_BYTES: usize = 256 * 1024;

/// Returns a sorted list of ranges that cover `ranges`, merging ranges whose
/// gap is at most `gap`.
///
/// This replicates `object_store::coalesce_ranges` (`merge_ranges`) so the
/// fetch plan matches what a `get_byte_ranges` implementation backed by
/// `ObjectStore::get_ranges` would issue.
fn merge_ranges(ranges: &[Range<u64>], gap: u64) -> Vec<Range<u64>> {
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
                .map(|delta| delta <= gap)
                .unwrap_or(true)
        {
            range_end = range_end.max(ranges[end_idx].end);
            end_idx += 1;
        }

        ret.push(ranges[start_idx].start..range_end);

        start_idx = end_idx;
        end_idx += 1;
    }

    ret
}

fn key(range: &Range<u64>) -> (u64, u64) {
    (range.start, range.end)
}

/// Drains `body` into `tx`, aggregating small chunks up to
/// [`TARGET_MESSAGE_BYTES`]. Exits when the body ends, errors, or the
/// receiver is dropped (checked even while awaiting the body, so dropping the
/// fetcher promptly drops the body and releases its connection). The bounded
/// channel provides backpressure: when the fetcher stops consuming, this task
/// parks on `send` and, transitively, the server stalls via flow control.
async fn pump_body(mut body: BoxStream<'static, Result<Bytes>>, tx: mpsc::Sender<Result<Bytes>>) {
    let mut pending: Vec<Bytes> = Vec::new();
    let mut pending_bytes = 0usize;
    loop {
        let item = tokio::select! {
            biased;
            _ = tx.closed() => return,
            item = body.next() => item,
        };
        match item {
            Some(Ok(bytes)) => {
                pending_bytes += bytes.len();
                pending.push(bytes);
                if pending_bytes >= TARGET_MESSAGE_BYTES {
                    let message = concat_bytes(std::mem::take(&mut pending), pending_bytes);
                    pending_bytes = 0;
                    if tx.send(Ok(message)).await.is_err() {
                        return;
                    }
                }
            }
            Some(Err(e)) => {
                let _ = tx.send(Err(e)).await;
                return;
            }
            None => break,
        }
    }
    if !pending.is_empty() {
        let message = concat_bytes(pending, pending_bytes);
        let _ = tx.send(Ok(message)).await;
    }
}

fn concat_bytes(mut parts: Vec<Bytes>, total: usize) -> Bytes {
    if parts.len() == 1 {
        return parts.pop().expect("checked len");
    }
    let mut out = Vec::with_capacity(total);
    for part in parts {
        out.extend_from_slice(&part);
    }
    out.into()
}

/// A single in-flight streamed request
#[derive(Debug)]
struct ActiveStream {
    /// Requested ranges this stream will satisfy, ascending, disjoint.
    /// Members are removed once delivered to the decoder.
    members: VecDeque<Range<u64>>,
    /// Members ending at or below this offset (small ranges preceding the
    /// streamed chunk) are delivered as soon as their bytes arrive; members
    /// above it are delivered only once the decoder asks for them.
    eager_end: u64,
    /// Absolute offset of the next byte the body will produce
    watermark: u64,
    /// Received, not-yet-delivered bytes: (absolute start offset, data)
    segments: VecDeque<(u64, Bytes)>,
    /// Dropping this receiver makes the pump task exit promptly (it selects
    /// on the channel being closed), dropping the body mid-transfer
    rx: mpsc::Receiver<Result<Bytes>>,
    /// The body has ended
    finished: bool,
    /// The body errored; surfaced when this stream's data is next needed
    error: Option<ParquetError>,
}

impl ActiveStream {
    /// True if an undelivered, currently-deliverable member still needs bytes
    /// beyond the watermark
    fn needs_progress(&self, wanted: &HashSet<(u64, u64)>) -> bool {
        !self.finished
            && self.error.is_none()
            && self.members.iter().any(|m| {
                m.end > self.watermark && (m.end <= self.eager_end || wanted.contains(&key(m)))
            })
    }

    /// True if the decoder is currently waiting on data from this stream
    fn is_needed_now(&self, wanted: &HashSet<(u64, u64)>) -> bool {
        self.members.iter().any(|m| wanted.contains(&key(m)))
    }

    /// Drop buffered bytes below `pos` (gap bytes between requested ranges)
    fn drop_below(&mut self, pos: u64) {
        while let Some((start, bytes)) = self.segments.front_mut() {
            let end = *start + bytes.len() as u64;
            if end <= pos {
                self.segments.pop_front();
            } else if *start < pos {
                let cut = (pos - *start) as usize;
                *bytes = bytes.slice(cut..);
                *start = pos;
                break;
            } else {
                break;
            }
        }
    }

    /// Assemble the bytes for `member`, which must be fully covered by the
    /// buffered segments
    fn extract(&mut self, member: &Range<u64>) -> Result<Bytes> {
        self.drop_below(member.start);
        let len = (member.end - member.start) as usize;

        // single segment covering the whole member: zero copy
        if let Some((start, bytes)) = self.segments.front() {
            if *start <= member.start && *start + bytes.len() as u64 >= member.end {
                let offset = (member.start - *start) as usize;
                let out = bytes.slice(offset..offset + len);
                self.drop_below(member.end);
                return Ok(out);
            }
        }

        let mut out = Vec::with_capacity(len);
        for (start, bytes) in self.segments.iter() {
            if *start >= member.end {
                break;
            }
            let seg_end = *start + bytes.len() as u64;
            let from = member.start.max(*start);
            let to = member.end.min(seg_end);
            if from < to {
                out.extend_from_slice(&bytes[(from - *start) as usize..(to - *start) as usize]);
            }
        }
        if out.len() != len {
            return Err(ParquetError::General(format!(
                "streamed request ended before covering range {}..{} ({} of {} bytes available)",
                member.start,
                member.end,
                out.len(),
                len
            )));
        }
        self.drop_below(member.end);
        Ok(out.into())
    }
}

/// Specification of a streamed request to open
struct StreamSpec {
    span: Range<u64>,
    tx: mpsc::Sender<Result<Bytes>>,
}

/// Resolves to (input, materialized ranges, their bytes)
type IssueFuture<T> = BoxFuture<'static, Result<(T, Vec<Range<u64>>, Vec<Bytes>)>>;

/// Ownership of the underlying [`AsyncFileReader`]. It is moved into a future
/// while requests are being issued (`get_bytes_stream` / `get_byte_ranges`
/// borrow the reader), and returned when the future resolves.
enum InputState<T> {
    Idle(T),
    Busy(IssueFuture<T>),
}

impl<T> std::fmt::Debug for InputState<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Idle(_) => write!(f, "InputState::Idle"),
            Self::Busy(_) => write!(f, "InputState::Busy"),
        }
    }
}

/// Adaptive fetcher for the bounded streaming mode: fetches small coalesced
/// ranges exactly like [`AsyncFileReader::get_byte_ranges`], and consumes
/// large spans incrementally via [`AsyncFileReader::get_bytes_stream`] with
/// bounded buffering.
///
/// Used internally by [`ParquetRecordBatchStream`] when
/// [`BoundedStreamingOptions`] are set. External drivers of
/// [`ParquetPushDecoder`] (systems that implement their own fetch loops) can
/// use it directly via [`Self::try_new`] and [`Self::fetch_more`]:
///
/// ```ignore
/// let mut fetcher = AdaptiveFetcher::try_new(options, reader)?;
/// loop {
///     match decoder.try_decode()? {
///         DecodeResult::NeedsData(ranges) => {
///             let plan = decoder.upcoming_fetch_plan();
///             let (ranges, data) = fetcher.fetch_more(ranges, plan).await?;
///             decoder.push_ranges(ranges, data)?;
///         }
///         DecodeResult::Data(batch) => { /* ... */ }
///         DecodeResult::Finished => break,
///     }
/// }
/// ```
///
/// [`ParquetRecordBatchStream`]: super::ParquetRecordBatchStream
/// [`ParquetPushDecoder`]: crate::arrow::push_decoder::ParquetPushDecoder
#[derive(Debug)]
pub struct AdaptiveFetcher<T> {
    options: BoundedStreamingOptions,
    runtime: Handle,
    input: InputState<T>,
    streams: Vec<ActiveStream>,
    /// Ranges assigned to an in-flight fetch (stream member or materialize
    /// batch); removed on delivery
    planned: HashSet<(u64, u64)>,
    /// Ranges the decoder needs to make progress now
    wanted: HashSet<(u64, u64)>,
    /// Assembled data not yet pushed to the decoder
    ready_ranges: Vec<Range<u64>>,
    ready_data: Vec<Bytes>,
}

impl<T> AdaptiveFetcher<T>
where
    T: AsyncFileReader + Send + 'static,
{
    /// Create a new fetcher over `input`, spawning body-draining tasks on the
    /// current tokio runtime.
    ///
    /// # Errors
    ///
    /// Returns an error if called outside a tokio runtime context.
    pub fn try_new(options: BoundedStreamingOptions, input: T) -> Result<Self> {
        let runtime = Handle::try_current().map_err(|e| {
            ParquetError::General(format!(
                "AdaptiveFetcher requires a tokio runtime to drive streamed request bodies: {e}"
            ))
        })?;
        Ok(Self::new(options, runtime, input))
    }

    /// Fetch (at least) the data the decoder needs to make progress now.
    ///
    /// `needs` are the ranges from [`DecodeResult::NeedsData`] and `plan` the
    /// corresponding [`ParquetPushDecoder::upcoming_fetch_plan`]. Returns
    /// (ranges, data) pairs ready to be passed to
    /// [`ParquetPushDecoder::push_ranges`]; call again with the decoder's next
    /// request if it still needs more.
    ///
    /// [`DecodeResult::NeedsData`]: crate::DecodeResult::NeedsData
    /// [`ParquetPushDecoder`]: crate::arrow::push_decoder::ParquetPushDecoder
    /// [`ParquetPushDecoder::push_ranges`]: crate::arrow::push_decoder::ParquetPushDecoder::push_ranges
    /// [`ParquetPushDecoder::upcoming_fetch_plan`]: crate::arrow::push_decoder::ParquetPushDecoder::upcoming_fetch_plan
    pub async fn fetch_more(
        &mut self,
        needs: Vec<Range<u64>>,
        plan: UpcomingFetchPlan,
    ) -> Result<(Vec<Range<u64>>, Vec<Bytes>)> {
        self.want(&needs);
        std::future::poll_fn(|cx| {
            loop {
                if let Err(e) = self.plan_and_issue(&plan) {
                    return Poll::Ready(Err(e));
                }
                let progressed = match self.poll_sources(cx) {
                    Ok(progressed) => progressed,
                    Err(e) => return Poll::Ready(Err(e)),
                };
                if self.has_ready() {
                    return Poll::Ready(Ok(self.take_ready()));
                }
                if !progressed {
                    return Poll::Pending;
                }
            }
        })
        .await
    }

    /// `runtime` is used to spawn body-draining tasks
    pub(crate) fn new(options: BoundedStreamingOptions, runtime: Handle, input: T) -> Self {
        Self {
            options,
            runtime,
            input: InputState::Idle(input),
            streams: Vec::new(),
            planned: HashSet::new(),
            wanted: HashSet::new(),
            ready_ranges: Vec::new(),
            ready_data: Vec::new(),
        }
    }

    pub(crate) fn has_ready(&self) -> bool {
        !self.ready_ranges.is_empty()
    }

    /// Take the assembled (range, bytes) pairs, to push into the decoder
    pub(crate) fn take_ready(&mut self) -> (Vec<Range<u64>>, Vec<Bytes>) {
        (
            std::mem::take(&mut self.ready_ranges),
            std::mem::take(&mut self.ready_data),
        )
    }

    /// Record the ranges the decoder needs to make progress now
    pub(crate) fn want(&mut self, ranges: &[Range<u64>]) {
        for range in ranges {
            self.wanted.insert(key(range));
        }
    }

    /// Plan and issue requests for any not-yet-planned upcoming ranges.
    ///
    /// The plan first reproduces the coalesced request set that
    /// `get_byte_ranges` would issue (see [`merge_ranges`]). Coalesced groups
    /// that contain no streamable span are materialized through a single
    /// `get_byte_ranges` call, exactly as the non-adaptive path. Groups
    /// containing streamable spans (advertised by the decoder via
    /// [`UpcomingFetchPlan::streamable`]) are split into one streamed request
    /// per streamable span; each stream also carries the small ranges
    /// preceding its span, and any ranges after the last span go to the
    /// materialize batch.
    pub(crate) fn plan_and_issue(&mut self, plan: &UpcomingFetchPlan) -> Result<()> {
        // Drop streams whose remaining members are no longer needed (e.g. the
        // decoder hit its row limit and skipped the rest of a row group).
        // Aborts their body tasks.
        let upcoming: HashSet<(u64, u64)> = plan.ranges.iter().map(key).collect();
        for stream in &mut self.streams {
            let wanted = &self.wanted;
            let planned = &mut self.planned;
            stream.members.retain(|m| {
                let keep = upcoming.contains(&key(m)) || wanted.contains(&key(m));
                if !keep {
                    planned.remove(&key(m));
                }
                keep
            });
        }
        self.streams.retain(|s| !s.members.is_empty());

        let unplanned: Vec<Range<u64>> = plan
            .ranges
            .iter()
            .filter(|r| !self.planned.contains(&key(r)))
            .cloned()
            .collect();
        if unplanned.is_empty() {
            return Ok(());
        }

        // Can only issue requests while the input is idle. If a request is
        // already in flight, plan again once it resolves (`poll_sources`
        // returns progress when it does).
        if matches!(self.input, InputState::Busy(_)) {
            return Ok(());
        }

        let mut materialize: Vec<Range<u64>> = Vec::new();
        let mut specs: Vec<StreamSpec> = Vec::new();

        let capacity = usize::try_from(self.options.window_bytes)
            .unwrap_or(usize::MAX)
            .div_ceil(TARGET_MESSAGE_BYTES)
            .clamp(2, 64);

        for group in merge_ranges(&unplanned, self.options.coalesce_gap) {
            // members of this coalesced group, ascending
            let mut members: VecDeque<Range<u64>> = unplanned
                .iter()
                .filter(|r| r.start >= group.start && r.end <= group.end)
                .cloned()
                .collect();
            members.make_contiguous().sort_unstable_by_key(|r| r.start);

            // streamable spans intersecting this group, large enough to be
            // worth a dedicated request
            let mut cuts: Vec<Range<u64>> = plan
                .streamable
                .iter()
                .filter_map(|s| {
                    let start = s.start.max(group.start);
                    let end = s.end.min(group.end);
                    (end > start && end - start >= self.options.stream_threshold)
                        .then_some(start..end)
                })
                .collect();
            cuts.sort_unstable_by_key(|r| r.start);

            if cuts.is_empty() {
                materialize.extend(members);
                continue;
            }

            for cut in cuts {
                let mut stream_members = VecDeque::new();
                while let Some(front) = members.front() {
                    if front.start < cut.end {
                        stream_members.push_back(members.pop_front().expect("front exists"));
                    } else {
                        break;
                    }
                }
                if stream_members.is_empty() {
                    continue;
                }
                let span = stream_members.front().expect("non-empty").start
                    ..stream_members.back().expect("non-empty").end;
                let (tx, rx) = mpsc::channel(capacity);
                let watermark = span.start;
                specs.push(StreamSpec {
                    span: span.clone(),
                    tx,
                });
                self.streams.push(ActiveStream {
                    members: stream_members,
                    eager_end: cut.start,
                    watermark,
                    segments: VecDeque::new(),
                    rx,
                    finished: false,
                    error: None,
                });
            }
            // anything after the last streamable span
            materialize.extend(members);
        }

        let InputState::Idle(mut input) = std::mem::replace(
            &mut self.input,
            InputState::Busy(futures::future::pending().boxed()),
        ) else {
            unreachable!("checked idle above");
        };

        for range in &materialize {
            self.planned.insert(key(range));
        }
        for stream in &self.streams {
            for member in &stream.members {
                self.planned.insert(key(member));
            }
        }

        let runtime = self.runtime.clone();
        let future = async move {
            // Open the streamed requests first so their bodies start flowing
            // while the materialized ranges download
            for spec in specs {
                let StreamSpec { span, tx } = spec;
                match input.get_bytes_stream(span).await {
                    Ok(body) => {
                        runtime.spawn(pump_body(body, tx));
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                    }
                }
            }
            let data = if materialize.is_empty() {
                vec![]
            } else {
                input.get_byte_ranges(materialize.clone()).await?
            };
            Ok((input, materialize, data))
        }
        .boxed();
        self.input = InputState::Busy(future);
        Ok(())
    }

    /// Poll all in-flight requests, assembling any data the decoder is
    /// waiting for. Returns `true` if any progress was made; `false` means
    /// everything is pending (wakers registered).
    pub(crate) fn poll_sources(&mut self, cx: &mut Context<'_>) -> Result<bool> {
        let mut progressed = false;

        // The materialize batch (and stream opening)
        if let InputState::Busy(future) = &mut self.input {
            match future.poll_unpin(cx) {
                Poll::Ready(Ok((input, ranges, data))) => {
                    for range in &ranges {
                        self.planned.remove(&key(range));
                        self.wanted.remove(&key(range));
                    }
                    self.ready_ranges.extend(ranges);
                    self.ready_data.extend(data);
                    self.input = InputState::Idle(input);
                    progressed = true;
                }
                Poll::Ready(Err(e)) => return Err(e),
                Poll::Pending => {}
            }
        }

        // Streamed bodies
        let mut failed: Option<ParquetError> = None;
        for stream in &mut self.streams {
            // Surface a stream failure once the decoder actually needs this
            // stream's data
            if stream.error.is_some() && stream.is_needed_now(&self.wanted) {
                failed = Some(stream.error.take().expect("checked above"));
                break;
            }

            while stream.needs_progress(&self.wanted) {
                match stream.rx.poll_recv(cx) {
                    Poll::Ready(Some(Ok(bytes))) => {
                        let start = stream.watermark;
                        stream.watermark += bytes.len() as u64;
                        stream.segments.push_back((start, bytes));
                        progressed = true;
                    }
                    Poll::Ready(Some(Err(e))) => {
                        stream.error = Some(e);
                        break;
                    }
                    Poll::Ready(None) => {
                        stream.finished = true;
                        break;
                    }
                    Poll::Pending => break,
                }
            }

            // Deliver members in offset order as they become deliverable
            while let Some(member) = stream.members.front().cloned() {
                let deliverable = member.end <= stream.watermark
                    && (member.end <= stream.eager_end || self.wanted.contains(&key(&member)));
                if !deliverable {
                    break;
                }
                let bytes = stream.extract(&member)?;
                stream.members.pop_front();
                self.planned.remove(&key(&member));
                self.wanted.remove(&key(&member));
                self.ready_ranges.push(member);
                self.ready_data.push(bytes);
                progressed = true;
            }

            // A finished body that cannot cover a still-needed member is an
            // error surfaced on demand
            if stream.finished
                && stream.error.is_none()
                && stream
                    .members
                    .front()
                    .is_some_and(|m| m.end > stream.watermark)
            {
                stream.error = Some(ParquetError::EOF(
                    "streamed request body ended before all requested bytes arrived".to_string(),
                ));
            }
        }
        if let Some(e) = failed {
            return Err(e);
        }
        self.streams
            .retain(|s| !s.members.is_empty() || s.error.is_some());

        Ok(progressed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merge_ranges_matches_object_store_semantics() {
        // gap of 1: 0..10 and 11..20 merge, 22..30 does not
        let merged = merge_ranges(&[22..30, 0..10, 11..20], 1);
        assert_eq!(merged, vec![0..20, 22..30]);
        // overlapping ranges merge
        let merged = merge_ranges(&[0..10, 5..15], 0);
        assert_eq!(merged, vec![0..15]);
        assert_eq!(merge_ranges(&[], 10), Vec::<Range<u64>>::new());
    }
}
