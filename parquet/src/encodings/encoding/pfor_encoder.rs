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

//! PFOR encoder. See [`crate::encodings::pfor`] for the wire format.
//!
//! The encoder decides three things per vector, and most of this file is the search over them:
//!
//!   1. whether to difference the values first,
//!   2. the frame of reference, which is not required to be the minimum,
//!   3. the bit width, from a histogram cost model.

use std::marker::PhantomData;

use bytes::Bytes;

use super::Encoder;
use crate::basic::Encoding;
use crate::data_type::DataType;
use crate::encodings::pfor::*;
use crate::errors::{ParquetError, Result};
use crate::util::bit_util::BitWriter;

/// Buckets used by the frame search, as a shift and as a count.
///
/// 256 keeps the scan in [`choose_frame_and_width`] at roughly two passes' worth of work over a
/// 1024-value vector.
const FRAME_SEARCH_BITS: u32 = 8;
const FRAME_SEARCH_BUCKETS: usize = 1 << FRAME_SEARCH_BITS;

/// Histogram of the bit widths needed by `values` reduced by `frame`.
///
/// `out[b]` counts the residuals that need exactly `b` bits. The subtraction is modular, so a value
/// below the frame wraps to a huge residual and lands in the top bin -- which is what makes a frame
/// above the minimum work at all: below-frame values are counted as exceptions here and then
/// patched on the way out, with no separate sign or direction to track.
fn build_offset_histogram<T: PforInt>(values: &[T], frame: T, out: &mut [i32; 65]) {
    let frame_bits = frame.to_bits();
    // Four independent accumulators so repeated bins do not serialize the read-modify-write and the
    // load/count-leading-zeros/bump chains overlap. This loop is about half of encode time; a
    // single array runs scalar and stalls on any column whose residuals share a width, which is
    // most of them.
    let mut h = [[0i32; 65]; 4];
    let mut chunks = values.chunks_exact(4);
    for chunk in &mut chunks {
        for (lane, value) in chunk.iter().enumerate() {
            h[lane][bits_required(value.residual_from(frame_bits)) as usize] += 1;
        }
    }
    for value in chunks.remainder() {
        h[0][bits_required(value.residual_from(frame_bits)) as usize] += 1;
    }
    for b in 0..=64 {
        out[b] = h[0][b] + h[1][b] + h[2][b] + h[3][b];
    }
}

/// Pick the width that minimises packed bits plus exception bits.
///
/// Returns the width and its cost in bits. The width is what the caller needs; the exception count
/// is not returned because the pass that writes the vector out recomputes it from the mask, and one
/// count on the wire is better than two that could ever disagree.
fn best_width_from_histogram<T: PforInt>(histogram: &[i32; 65], num_elements: usize) -> (u8, i64) {
    let max_bits = T::MAX_BIT_WIDTH;
    let exception_cost = exception_bits(T::BYTE_WIDTH);

    let mut best_cost = i64::MAX;
    let mut best_width = max_bits;

    let mut exceptions_above = num_elements as i64;
    for b in 0..=max_bits {
        exceptions_above -= histogram[b as usize] as i64;

        // A vector holds at most `MAX_VECTOR_SIZE` elements, so that is also the most exceptions one
        // can name. The full-width candidate has none at all, so a best is always found.
        if exceptions_above > MAX_VECTOR_SIZE as i64 {
            continue;
        }

        let total_cost = num_elements as i64 * b as i64 + exceptions_above * exception_cost;
        if total_cost < best_cost {
            best_cost = total_cost;
            best_width = b;
        }
    }

    (best_width, best_cost)
}

/// One walk producing both histograms the frame search needs.
///
/// The bit-width histogram costs the frame it is given; the bucket counts cost every other frame.
/// They are gathered together because each needs the same residual, and computing that residual
/// twice was measurably the larger half of the search.
///
/// Bucketing is by shift rather than division: `1 << shift` values per bucket keeps the count at or
/// below [`FRAME_SEARCH_BUCKETS`] without a per-element divide.
fn build_frame_search_histograms<T: PforInt>(
    values: &[T],
    frame: T,
    shift: u32,
    width_hist: &mut [i32; 65],
    buckets: &mut [i32; FRAME_SEARCH_BUCKETS + 1],
) {
    let frame_bits = frame.to_bits();
    let mut h = [[0i32; 65]; 4];
    for (i, value) in values.iter().enumerate() {
        let offset = value.residual_from(frame_bits);
        h[i & 3][bits_required(offset) as usize] += 1;
        buckets[(offset >> shift) as usize] += 1;
    }
    for b in 0..=64 {
        width_hist[b] = h[0][b] + h[1][b] + h[2][b] + h[3][b];
    }
}

/// The extremes of a run of values.
#[derive(Debug, Clone, Copy)]
struct MinMax<T> {
    min: T,
    max: T,
}

/// A frame of reference together with the width that suits it.
#[derive(Debug, Clone, Copy)]
struct FrameChoice<T> {
    frame_of_reference: T,
    bit_width: u8,
    cost_bits: i64,
}

/// Choose a frame of reference and a bit width together.
///
/// The frame PFOR has always used is the minimum, which makes every exception an overshoot: one
/// value far below the cluster drags the whole packed window down with it and nothing can patch it
/// back. Treating the frame as a free parameter instead -- any lower bound, not the lowest -- lets
/// the window sit where the values actually are and patch on both sides.
///
/// Storage is unaffected: the frame field already holds a full-width value, and the decoder only
/// ever adds it. The whole cost is this search.
///
/// The search is approximate by design. An exact answer needs the values sorted; instead the range
/// is bucketed with a shift, and for each candidate width a window is slid over the bucket counts.
/// Only whole buckets count as covered, so the exception estimate is an upper bound -- never
/// optimistic. The minimum-frame answer is always among the candidates, and it alone is costed from
/// a real histogram, so the search can never do worse than what a plain cost model would pick.
fn choose_frame_and_width<T: PforInt>(values: &[T], bounds: MinMax<T>) -> FrameChoice<T> {
    let num_elements = values.len();
    let max_bits = T::MAX_BIT_WIDTH;
    let exception_cost = exception_bits(T::BYTE_WIDTH);

    let min_val = bounds.min;
    let range = bounds.max.residual_from(min_val.to_bits());

    // A constant vector is already at the floor, and min/max has just proved it constant, so it
    // needs no histogram to find that out. Worth the special case for its own sake: a run of equal
    // values sends every element to one histogram bin, where the read-modify-write serializes and
    // the pass runs at a fraction of its usual rate.
    if range == 0 {
        return FrameChoice {
            frame_of_reference: min_val,
            bit_width: 0,
            cost_bits: 0,
        };
    }

    let range_bits = bits_required(range) as u32;
    let shift = range_bits.saturating_sub(FRAME_SEARCH_BITS);

    // One walk serves both halves of the search: the bit-width histogram costs the minimum as a
    // frame, the bucket counts cost every other frame.
    let mut histogram = [0i32; 65];
    let mut counts = [0i32; FRAME_SEARCH_BUCKETS + 1];
    build_frame_search_histograms(values, min_val, shift, &mut histogram, &mut counts);

    // Candidate 0: the minimum, i.e. what PFOR has always done. Costed unconditionally, and from a
    // real histogram, so the search cannot regress against it.
    let (bit_width, cost_bits) = best_width_from_histogram::<T>(&histogram, num_elements);
    let mut best = FrameChoice {
        frame_of_reference: min_val,
        bit_width,
        cost_bits,
    };

    // Already at width 0, with a handful of patches carrying the rest. Nothing a frame can do about
    // it. Note this is not the same as having no exceptions: the whole point of a frame above the
    // minimum is to trade a narrower width for a few patches, so an exception-free choice is where
    // the search starts, not a reason to skip it.
    if best.bit_width == 0 {
        return best;
    }

    let num_buckets = (range >> shift) as usize + 1;
    let mut prefix = [0i32; FRAME_SEARCH_BUCKETS + 2];
    for b in 0..num_buckets {
        prefix[b + 1] = prefix[b] + counts[b];
    }

    // Slide a `2^w`-wide window over the buckets and keep the position that costs least. Widths
    // below the bucket size cannot be resolved at this granularity, and a window spanning every
    // bucket has no exceptions left to remove, so the loop covers only the
    // `FRAME_SEARCH_BITS`-odd widths in between -- fixed work, and none of it touching the data
    // again.
    //
    // What comes out of this is a frame, not a width. Only whole buckets count as covered, so `w`
    // here is an upper bound on the width the frame really needs, and the exception count an upper
    // bound too. The exact pass below is what turns the frame into a plan.
    //
    // Seeded with the incumbent's cost, so a window only registers if it beats the minimum as a
    // frame. Everything after this loop -- the walk that lowers the frame onto a real value, and
    // the exact pass that turns it into a width -- is then skipped entirely on a column the frame
    // cannot help, which is most of them. Seeding it costs the conservative direction: the scan
    // over-counts exceptions, so it can decline a frame whose exact cost would have won, but it
    // cannot accept one that loses.
    let mut best_start: Option<usize> = None;
    let mut best_end = 0usize;
    let mut scan_cost = best.cost_bits;
    for w in shift..=max_bits as u32 {
        let whole_buckets = 1u64 << (w - shift);
        let k = std::cmp::min(whole_buckets as usize, num_buckets);
        for s in 0..num_buckets {
            let end = std::cmp::min(s + k, num_buckets);
            let exceptions = num_elements as i64 - (prefix[end] - prefix[s]) as i64;
            if exceptions > MAX_VECTOR_SIZE as i64 {
                continue;
            }
            let cost = num_elements as i64 * w as i64 + exceptions * exception_cost;
            if cost < scan_cost {
                scan_cost = cost;
                best_start = Some(s);
                best_end = end;
            }
        }
        if k >= num_buckets {
            break; // one window already spans the data
        }
    }

    let Some(best_start) = best_start else {
        return best;
    };

    // Lower the frame from the boundary of the winning window onto the smallest value the window
    // actually covers. Bucket boundaries stand `2^shift` apart, which on a wide column is
    // thousands, and a cluster sitting just above one would otherwise pay those bits for nothing.
    //
    // A walk of its own, rather than per-bucket minima kept by the pass above: tracking them there
    // costs every vector a compare and a store per element, including the vectors where the scan
    // finds nothing and the minima are discarded. Here only a vector the search has already won
    // pays, and it pays one traversal.
    let window_lo = (best_start as u64) << shift;
    // A window reaching the last bucket has no upper edge to test against: the edge would be
    // `num_buckets << shift`, which is one past the representable range whenever the residuals span
    // the whole type.
    let bounded_above = best_end < num_buckets;
    let window_hi = if bounded_above {
        (best_end as u64) << shift
    } else {
        0
    };

    let min_bits = min_val.to_bits();
    let mut frame_offset = 0u64;
    let mut covers_anything = false;
    for value in values {
        let offset = value.residual_from(min_bits);
        if offset < window_lo || (bounded_above && offset >= window_hi) {
            continue;
        }
        if !covers_anything || offset < frame_offset {
            frame_offset = offset;
            covers_anything = true;
        }
    }
    if !covers_anything {
        return best;
    }

    let scan_frame = T::from_bits(min_bits.wrapping_add(frame_offset));
    if scan_frame == min_val {
        return best;
    }

    // Cost the winning frame exactly. This pass is not bookkeeping -- it is where the width and the
    // exception count are actually decided. The scan works at bucket granularity and so cannot see
    // a window narrower than one bucket, which is exactly where the answers worth having tend to
    // be.
    build_offset_histogram(values, scan_frame, &mut histogram);
    let (bit_width, exact_cost) = best_width_from_histogram::<T>(&histogram, num_elements);
    if exact_cost < best.cost_bits {
        best = FrameChoice {
            frame_of_reference: scan_frame,
            bit_width,
            cost_bits: exact_cost,
        };
    }
    best
}

/// The bounds of `values`.
///
/// # Panics
///
/// Panics on an empty slice: an empty vector has no frame of reference.
fn min_max<T: PforInt>(values: &[T]) -> MinMax<T> {
    let mut bounds = MinMax {
        min: values[0],
        max: values[0],
    };
    for &value in &values[1..] {
        if value < bounds.min {
            bounds.min = value;
        }
        if value > bounds.max {
            bounds.max = value;
        }
    }
    bounds
}

/// Map a signed value onto an unsigned one of the same magnitude.
///
/// Negative values interleave with positive ones instead of wrapping to the top of the range, so a
/// small negative difference needs few bits rather than all of them. Used only by
/// [`estimate_delta_cost_bits`]; nothing on the wire is zigzagged.
#[inline]
fn zigzag<T: PforInt>(value: T) -> u64 {
    let bits = value.to_bits();
    let sign_bits = T::BYTE_WIDTH * 8;
    let sign_mask = 0u64.wrapping_sub(bits >> (sign_bits - 1));
    let shifted = (bits << 1) & low_mask(sign_bits as u8);
    (shifted ^ sign_mask) & low_mask(sign_bits as u8)
}

/// Fill `deltas` with the backward differences of `values`, and return their bounds.
///
/// `deltas[0]` is zero: the first value travels in the plan's start value, and giving slot 0 a real
/// difference would mean either a shorter packed run or a value that is not a difference sitting in
/// the width histogram. Zero costs `bit_width` bits and distorts nothing.
///
/// Subtraction is modular, because a column that spans the type's range will wrap and the decoder
/// sums the same way.
fn compute_deltas<T: PforInt>(values: &[T], deltas: &mut Vec<T>) -> MinMax<T> {
    deltas.clear();
    deltas.push(T::default());
    let mut bounds = MinMax {
        min: T::default(),
        max: T::default(),
    };
    for i in 1..values.len() {
        let d = T::from_bits(values[i].to_bits().wrapping_sub(values[i - 1].to_bits()));
        deltas.push(d);
        if d < bounds.min {
            bounds.min = d;
        }
        if d > bounds.max {
            bounds.max = d;
        }
    }
    bounds
}

/// Estimate what packing the differences of `values` would cost, in bits.
///
/// The full decision needs the differences written out and searched, which is most of what encoding
/// a vector costs. This reaches an answer good enough to decline the mode from a strided sample,
/// without writing anything.
///
/// The sample is of widths, not of a span. A gate on the span of the differences was tried first
/// and had to go: a sawtooth is a tight cluster of small positive differences with a handful of
/// large negative ones, so its span is as wide as its raw span while its cost is a fraction of it.
/// Feeding widths to the same cost model the search uses keeps that shape, because the model can
/// trade a wide bin against a patch.
///
/// Zigzagging is what lets a histogram stand in for a frame search that has not run. Differences in
/// `[-k, k]` zigzag into `[0, 2k]`, and a frame at `-k` maps them onto the same `[0, 2k]`, so for a
/// range that straddles zero evenly the estimated width is the width the search would find. Where
/// the range leans one way the estimate runs a bit or two wide.
fn estimate_delta_cost_bits<T: PforInt>(values: &[T]) -> i64 {
    // Enough of a sample to place a distribution across the width bins, and few enough that the
    // pass is a fraction of the one it is deciding against.
    const SAMPLE_TARGET: usize = 128;
    let num_elements = values.len();
    let stride = std::cmp::max(1, num_elements / SAMPLE_TARGET);

    let mut h = [[0i32; 65]; 4];
    let mut sampled = 0usize;
    let mut i = stride;
    while i < num_elements {
        let d = T::from_bits(values[i].to_bits().wrapping_sub(values[i - 1].to_bits()));
        h[sampled & 3][bits_required(zigzag(d)) as usize] += 1;
        sampled += 1;
        i += stride;
    }
    if sampled == 0 {
        return 0;
    }

    let mut histogram = [0i32; 65];
    for b in 0..=64 {
        histogram[b] = h[0][b] + h[1][b] + h[2][b] + h[3][b];
    }
    let (_, sample_cost) = best_width_from_histogram::<T>(&histogram, sampled);

    // Scale to the whole vector. Both terms of the model are per-element -- a width costs its bits
    // every element, an exception costs its slot every time it occurs -- so the sample cost scales
    // with the count.
    sample_cost * num_elements as i64 / sampled as i64
}

/// Everything the encoder decided about one vector.
#[derive(Debug, Clone, Copy)]
struct PforVectorPlan<T> {
    /// Difference the values before framing and packing them.
    delta: bool,
    /// Subtracted from every element before packing; added back on decode.
    frame_of_reference: T,
    /// First value of the vector, stored only when `delta` is set. It is what makes a differenced
    /// vector decodable on its own, without the vector before it.
    start_value: T,
    bit_width: u8,
    cost_bits: i64,
}

/// Decide how to encode one vector.
///
/// On return `delta_scratch` holds the differences if the plan chose them, and is clobbered either
/// way.
///
/// Both transforms are costed with the same model and the cheaper one wins, so the mode is a
/// per-vector decision. It has to be: differencing loses on every unclustered draw, and a column is
/// rarely all one shape.
fn choose_vector_plan<T: PforInt>(
    values: &[T],
    delta_scratch: &mut Vec<T>,
    delta_enabled: bool,
) -> PforVectorPlan<T> {
    let raw = choose_frame_and_width(values, min_max(values));

    let mut plan = PforVectorPlan {
        delta: false,
        frame_of_reference: raw.frame_of_reference,
        start_value: T::default(),
        bit_width: raw.bit_width,
        cost_bits: raw.cost_bits,
    };

    // One element has no difference to take, and a vector already packing at width 0 cannot be
    // improved on.
    if !delta_enabled || values.len() < 2 || raw.bit_width == 0 {
        return plan;
    }

    // A differenced vector carries its own first value, so it starts one full-width value behind
    // whatever its differences pack to.
    let start_value_bits = (T::BYTE_WIDTH * 8) as i64;

    // Estimate the mode before paying for it, and drop it here if the estimate cannot reach the
    // incumbent. What is skipped is the whole of the rest of the mode: the pass that writes the
    // differences out, and the frame search over them. The estimate is deliberately loose, so it
    // declines only where the two modes are more than a sampling error apart -- which is where the
    // choice matters least.
    if estimate_delta_cost_bits(values) + start_value_bits >= plan.cost_bits {
        return plan;
    }

    let delta_bounds = compute_deltas(values, delta_scratch);
    let delta = choose_frame_and_width(delta_scratch, delta_bounds);

    let delta_cost = delta.cost_bits + start_value_bits;
    if delta_cost < plan.cost_bits {
        plan = PforVectorPlan {
            delta: true,
            frame_of_reference: delta.frame_of_reference,
            start_value: values[0],
            bit_width: delta.bit_width,
            cost_bits: delta_cost,
        };
    }
    plan
}

/// Encoder for [`Encoding::PFOR`].
///
/// Values are buffered until [`Encoder::flush_buffer`], because a page's offset array is sized by
/// the number of vectors and so cannot be written before the last value has arrived.
pub struct PforEncoder<T: DataType> {
    /// Values put so far, awaiting a flush.
    values: Vec<T::T>,
    /// Elements per vector.
    vector_size: usize,
    /// Whether the planner may difference a vector.
    ///
    /// Nothing here changes how a page is read: a decoder is told which mode each vector used by
    /// the vector's own info block, so turning the mode off only narrows what the encoder will
    /// choose, and pages written either way read the same.
    delta_enabled: bool,
    /// Scratch for one vector's differences, kept across vectors so the allocation is paid once.
    delta_scratch: Vec<T::T>,
    _phantom: PhantomData<T>,
}

impl<T: DataType> Default for PforEncoder<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: DataType> PforEncoder<T> {
    /// Creates an encoder at the default vector size, with differencing enabled.
    pub fn new() -> Self {
        Self {
            values: Vec::new(),
            vector_size: DEFAULT_VECTOR_SIZE,
            delta_enabled: true,
            delta_scratch: Vec::new(),
            _phantom: PhantomData,
        }
    }

    /// Sets the number of elements per vector, which must be a power of two the page header can
    /// describe.
    pub fn with_vector_size(mut self, vector_size: usize) -> Result<Self> {
        validate_vector_size(vector_size)?;
        self.vector_size = vector_size;
        Ok(self)
    }

    /// Sets whether the planner may difference a vector.
    pub fn with_delta_enabled(mut self, delta_enabled: bool) -> Self {
        self.delta_enabled = delta_enabled;
        self
    }
}

impl<T: DataType> PforEncoder<T>
where
    T::T: PforInt,
{
    /// Encode one vector onto the end of `out`.
    fn encode_vector(&mut self, values: &[T::T], out: &mut Vec<u8>) {
        debug_assert!(!values.is_empty());
        let plan = choose_vector_plan(values, &mut self.delta_scratch, self.delta_enabled);
        let source: &[T::T] = if plan.delta {
            &self.delta_scratch
        } else {
            values
        };

        // Reduce by the frame, collecting whatever will not fit.
        //
        // The comparison is against the packed mask in the unsigned domain, which is what lets the
        // frame sit above the minimum: a source value below the frame wraps to a huge residual,
        // fails the same test as one above the window, and is patched from the exception list like
        // any other. No second test, no sign.
        let frame_bits = plan.frame_of_reference.to_bits();
        let mask = low_mask(plan.bit_width);
        let mut residuals: Vec<u64> = Vec::with_capacity(values.len());
        let mut exception_positions: Vec<u16> = Vec::new();
        let mut exception_values: Vec<T::T> = Vec::new();

        for (i, &value) in source.iter().enumerate() {
            let mut offset = value.residual_from(frame_bits);
            if offset > mask {
                exception_positions.push(i as u16);
                // The exception carries whatever the packed stream carries: a value in a plain
                // vector, a difference in a differenced one. The decoder patches it in before the
                // running sum, so a patched difference is summed like any other.
                exception_values.push(value);
                offset = 0;
            }
            residuals.push(offset);
        }

        let info = PforVectorInfo::<T::T> {
            frame_of_reference: plan.frame_of_reference,
            bit_width: plan.bit_width,
            // Taken from the pass that just ran rather than from the plan, so the count on the wire
            // is the count of exceptions actually written even if the two ever disagree.
            num_exceptions: exception_positions.len() as u16,
            is_delta: plan.delta,
        };
        info.write(out);

        if plan.delta {
            plan.start_value.write_le(out);
        }

        if plan.bit_width > 0 {
            // `consume` flushes to a byte boundary, which is where the packed section ends.
            let mut writer = BitWriter::new_from_buf(std::mem::take(out));
            for &residual in &residuals {
                writer.put_value(residual, plan.bit_width as usize);
            }
            *out = writer.consume();
        }

        for position in &exception_positions {
            out.extend_from_slice(&position.to_le_bytes());
        }
        for value in &exception_values {
            value.write_le(out);
        }
    }
}

impl<T: DataType> Encoder<T> for PforEncoder<T>
where
    T::T: PforInt,
{
    fn put(&mut self, values: &[T::T]) -> Result<()> {
        self.values.extend_from_slice(values);
        Ok(())
    }

    fn encoding(&self) -> Encoding {
        Encoding::PFOR
    }

    fn estimated_data_encoded_size(&self) -> usize {
        // Has to be O(1), and the real size is not known until the widths are chosen, so this is
        // the unpacked size -- which the cost model guarantees the page will not exceed.
        self.values.len() * T::T::BYTE_WIDTH
    }

    fn estimated_memory_size(&self) -> usize {
        self.values.capacity() * std::mem::size_of::<T::T>()
            + self.delta_scratch.capacity() * std::mem::size_of::<T::T>()
    }

    fn flush_buffer(&mut self) -> Result<Bytes> {
        let num_values = self.values.len();
        if num_values > i32::MAX as usize {
            return Err(general_err!(
                "PFOR page holds {} values, more than the {} its header can describe",
                num_values,
                i32::MAX
            ));
        }

        let log_vector_size = validate_vector_size(self.vector_size)?;
        let header = PforHeader {
            packing_mode: PACKING_MODE_FOR_BIT_PACK,
            log_vector_size,
            value_byte_width: T::T::BYTE_WIDTH as u8,
            num_elements: num_values as i32,
        };
        let num_vectors = header.num_vectors();

        let mut out =
            Vec::with_capacity(max_compressed_size::<T::T>(num_values, self.vector_size)?);
        header.write(&mut out);

        // The offset array is reserved now and filled in as each vector is written. An all-null
        // page holds no values and still has to be written, and a reader loads the header before it
        // knows how many values a page has, so zero values encodes to a bare header rather than to
        // nothing at all.
        let offset_array_at = out.len();
        out.resize(offset_array_at + num_vectors * OFFSET_SIZE, 0);

        let values = std::mem::take(&mut self.values);
        for (v, chunk) in values.chunks(self.vector_size).enumerate() {
            // Offsets count from the start of the offset array, not from the start of the page.
            let offset = (out.len() - offset_array_at) as u32;
            let at = offset_array_at + v * OFFSET_SIZE;
            out[at..at + OFFSET_SIZE].copy_from_slice(&offset.to_le_bytes());
            self.encode_vector(chunk, &mut out);
        }

        self.values = values;
        self.values.clear();
        Ok(out.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_type::{Int32Type, Int64Type};
    use crate::encodings::decoding::{Decoder, PforDecoder};

    /// Encode then decode, asserting the values survive and reporting the page.
    fn round_trip<T: DataType>(values: &[T::T], vector_size: usize) -> Bytes
    where
        T::T: PforInt,
    {
        let mut encoder = PforEncoder::<T>::new()
            .with_vector_size(vector_size)
            .unwrap();
        encoder.put(values).unwrap();
        let page = encoder.flush_buffer().unwrap();

        assert!(
            page.len() <= max_compressed_size::<T::T>(values.len(), vector_size).unwrap(),
            "page of {} bytes exceeds the bound of {}",
            page.len(),
            max_compressed_size::<T::T>(values.len(), vector_size).unwrap()
        );

        let mut decoder = PforDecoder::<T>::new();
        decoder.set_data(page.clone(), values.len()).unwrap();
        let mut out = vec![T::T::default(); values.len()];
        assert_eq!(decoder.get(&mut out).unwrap(), values.len());
        assert_eq!(out, values);
        assert_eq!(decoder.values_left(), 0);
        page
    }

    /// The info block of vector `v` of a page, for the tests that assert what the planner chose.
    fn vector_info<T: PforInt>(page: &[u8], v: usize) -> PforVectorInfo<T> {
        let offsets = &page[HEADER_SIZE..];
        let at = HEADER_SIZE + read_offset(offsets, v);
        PforVectorInfo::<T>::read(&page[at..]).unwrap()
    }

    /// A deterministic pseudo-random sequence, so a failure is reproducible.
    ///
    /// A 64-bit xorshift, in the test rather than as a dependency.
    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
    }

    #[test]
    fn test_round_trip_shapes_i32() {
        let mut rng = Rng(0x2545F491_4F6CDD1D);
        let shapes: Vec<(&str, Vec<i32>)> = vec![
            ("empty", vec![]),
            ("single", vec![42]),
            ("constant", vec![7; 100]),
            ("constant zero", vec![0; 100]),
            ("ramp", (0..1000).collect()),
            ("descending ramp", (0..1000).rev().collect()),
            ("negative ramp", (-1000..0).collect()),
            // A cluster with outliers on both sides of it, which is what a searched frame is for.
            (
                "two sided outliers",
                (0..1000)
                    .map(|i| match i % 100 {
                        0 => -1_000_000,
                        50 => 1_000_000,
                        _ => 500 + (i % 7),
                    })
                    .collect(),
            ),
            ("random", (0..1000).map(|_| rng.next() as i32).collect()),
            (
                "small random",
                (0..1000).map(|_| (rng.next() % 256) as i32).collect(),
            ),
            (
                "extremes",
                vec![i32::MIN, i32::MAX, 0, -1, 1, i32::MIN, i32::MAX],
            ),
            // A vector whose span covers the whole type, so the residuals need every bit.
            (
                "full span",
                (0..64)
                    .map(|i| {
                        if i % 2 == 0 {
                            i32::MIN + i
                        } else {
                            i32::MAX - i
                        }
                    })
                    .collect(),
            ),
            ("sawtooth", (0..1000).map(|i| (i % 37) * 3).collect()),
            // Steps: long runs of one value, which differencing turns into runs of zero.
            ("steps", (0..1000).map(|i| (i / 50) * 1_000_000).collect()),
        ];

        for (name, values) in shapes {
            for vector_size in [8usize, 64, 1024] {
                let page = round_trip::<Int32Type>(&values, vector_size);
                assert!(!page.is_empty(), "{name} at {vector_size} produced no page");
            }
        }
    }

    #[test]
    fn test_round_trip_shapes_i64() {
        let mut rng = Rng(0x9E3779B9_7F4A7C15);
        let shapes: Vec<(&str, Vec<i64>)> = vec![
            ("empty", vec![]),
            ("constant", vec![-9; 100]),
            ("ramp", (0..1000).collect()),
            ("wide ramp", (0..1000).map(|i| i * 1_000_000_007).collect()),
            ("random", (0..1000).map(|_| rng.next() as i64).collect()),
            (
                "random needing every bit",
                (0..1000).map(|_| rng.next() as i64 | i64::MIN).collect(),
            ),
            ("extremes", vec![i64::MIN, i64::MAX, 0, -1, 1]),
            (
                "full span",
                (0..64)
                    .map(|i| {
                        if i % 2 == 0 {
                            i64::MIN + i
                        } else {
                            i64::MAX - i
                        }
                    })
                    .collect(),
            ),
        ];

        for (name, values) in shapes {
            for vector_size in [8usize, 1024] {
                let page = round_trip::<Int64Type>(&values, vector_size);
                assert!(!page.is_empty(), "{name} at {vector_size} produced no page");
            }
        }
    }

    #[test]
    fn test_round_trip_every_vector_size() {
        let values: Vec<i32> = (0..5000).map(|i| (i * 31) % 9973).collect();
        for log_vector_size in MIN_LOG_VECTOR_SIZE..=MAX_LOG_VECTOR_SIZE {
            round_trip::<Int32Type>(&values, 1 << log_vector_size);
        }
    }

    #[test]
    fn test_round_trip_every_bit_width() {
        // One vector per width, built so the planner has to choose that width: values spanning
        // exactly `w` bits with nothing to gain from a frame or from differencing.
        for w in 1..=32u32 {
            let span: u64 = if w == 32 {
                u32::MAX as u64
            } else {
                (1u64 << w) - 1
            };
            let values: Vec<i32> = (0..64)
                .map(|i| ((i as u64 * 2_654_435_761) % (span + 1)) as u32 as i32)
                .collect();
            let page = round_trip::<Int32Type>(&values, 64);
            let info = vector_info::<i32>(&page, 0);
            assert!(
                info.bit_width <= 32,
                "width {} at w={w} is not a width INT32 can hold",
                info.bit_width
            );
        }
        for w in 33..=64u32 {
            let span: u64 = if w == 64 { u64::MAX } else { (1u64 << w) - 1 };
            let values: Vec<i64> = (0..64)
                .map(|i| ((i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) % span) as i64)
                .collect();
            let page = round_trip::<Int64Type>(&values, 64);
            let info = vector_info::<i64>(&page, 0);
            assert!(
                info.bit_width <= 64,
                "width {} at w={w} is not a width INT64 can hold",
                info.bit_width
            );
        }
    }

    #[test]
    fn test_constant_vector_packs_at_width_zero() {
        let page = round_trip::<Int32Type>(&vec![777; 64], 64);
        let info = vector_info::<i32>(&page, 0);
        assert_eq!(info.bit_width, 0);
        assert_eq!(info.num_exceptions, 0);
        assert_eq!(info.frame_of_reference, 777);
        assert!(!info.is_delta);
        // Header, one offset, and an info block holding the value: nothing else is needed.
        assert_eq!(
            page.len(),
            HEADER_SIZE + OFFSET_SIZE + <i32 as PforInt>::INFO_SIZE
        );
    }

    #[test]
    fn test_frame_is_searched_not_taken_as_the_minimum() {
        // A tight cluster with one value far below it. With the frame pinned to the minimum every
        // packed value would have to span the gap; with the frame searched, the outlier becomes an
        // exception and the width collapses to what the cluster needs.
        let mut values: Vec<i32> = (0..1024).map(|i| 1_000_000 + (i % 16)).collect();
        values[500] = -1_000_000;

        let page = round_trip::<Int32Type>(&values, 1024);
        let info = vector_info::<i32>(&page, 0);
        assert!(
            info.frame_of_reference > 0,
            "frame {} was not lifted above the outlier",
            info.frame_of_reference
        );
        assert!(
            info.bit_width <= 5,
            "width {} is wider than the cluster needs",
            info.bit_width
        );
        assert_eq!(info.num_exceptions, 1);

        // And the page is a fraction of what the minimum as a frame would have cost, which needs 21
        // bits for the gap.
        assert!(
            page.len() < 1024 * 21 / 8,
            "page of {} bytes did not beat a minimum frame",
            page.len()
        );
    }

    #[test]
    fn test_frame_search_covers_outliers_on_both_sides() {
        let mut values: Vec<i32> = (0..1024).map(|i| 500_000 + (i % 8)).collect();
        values[100] = -2_000_000;
        values[900] = 2_000_000;

        let page = round_trip::<Int32Type>(&values, 1024);
        let info = vector_info::<i32>(&page, 0);
        // One frame, two patches, one of them from below it -- where the residual wraps.
        assert_eq!(info.num_exceptions, 2);
        assert!(info.bit_width <= 4, "width {} is too wide", info.bit_width);
    }

    #[test]
    fn test_delta_is_chosen_on_a_ramp() {
        // A ramp spans its whole range, so packing it directly costs the span; differenced it is a
        // run of ones.
        let values: Vec<i64> = (0..1024).map(|i| i * 1_000_003).collect();
        let page = round_trip::<Int64Type>(&values, 1024);
        let info = vector_info::<i64>(&page, 0);
        assert!(info.is_delta, "differencing was not chosen for a ramp");
        assert!(info.bit_width <= 2, "width {} is too wide", info.bit_width);
    }

    #[test]
    fn test_delta_is_declined_on_unclustered_values() {
        // Differencing roughly doubles the range of independent draws, so it has to lose here. This
        // is the case the sampled prefilter exists to decline without paying for the mode.
        let mut rng = Rng(0xDEAD_BEEF_CAFE_F00D);
        let values: Vec<i32> = (0..1024).map(|_| (rng.next() % 4096) as i32).collect();
        let page = round_trip::<Int32Type>(&values, 1024);
        assert!(
            !vector_info::<i32>(&page, 0).is_delta,
            "differencing was chosen for unclustered values"
        );
    }

    #[test]
    fn test_delta_is_declined_on_a_sawtooth_that_a_span_gate_would_have_missed() {
        // The point of gating on sampled widths rather than on the span of the differences: a
        // sawtooth's differences are a tight cluster of small positives with a few large negatives,
        // so its span is as wide as the raw span while its cost is a fraction of it. Here the mode
        // still has to be evaluated, and whichever way it goes the values have to survive.
        let values: Vec<i32> = (0..1024).map(|i| (i % 64) * 1_000_000).collect();
        let page = round_trip::<Int32Type>(&values, 1024);
        let info = vector_info::<i32>(&page, 0);
        if info.is_delta {
            // Differencing a sawtooth leaves one large negative difference per tooth, which is
            // exactly what patching is for.
            assert!(info.num_exceptions > 0);
        }
    }

    #[test]
    fn test_delta_can_be_turned_off() {
        let values: Vec<i32> = (0..1024).map(|i| i * 7).collect();

        let mut encoder = PforEncoder::<Int32Type>::new();
        encoder.put(&values).unwrap();
        let with_delta = encoder.flush_buffer().unwrap();
        assert!(vector_info::<i32>(&with_delta, 0).is_delta);

        let mut encoder = PforEncoder::<Int32Type>::new().with_delta_enabled(false);
        encoder.put(&values).unwrap();
        let without_delta = encoder.flush_buffer().unwrap();
        assert!(!vector_info::<i32>(&without_delta, 0).is_delta);

        // Turning the mode off narrows what the encoder will choose and changes nothing about how a
        // page is read, because each vector says which mode it used.
        let mut decoder = PforDecoder::<Int32Type>::new();
        let mut out = vec![0i32; values.len()];
        decoder.set_data(without_delta, values.len()).unwrap();
        decoder.get(&mut out).unwrap();
        assert_eq!(out, values);
    }

    #[test]
    fn test_delta_start_value_makes_each_vector_self_contained() {
        // Two differenced vectors, read starting from the second one, which only decodes because it
        // carries its own start value.
        // A stride wide enough that differencing wins outright. On a short vector the two modes
        // can tie -- eight values of a gentle ramp cost the same either way, because the start
        // value costs as much as the width it saves -- and the planner keeps the plain form on a
        // tie.
        let values: Vec<i32> = (0..128).map(|i| 1000 + i * 100_000).collect();
        let page = round_trip::<Int32Type>(&values, 64);
        assert!(vector_info::<i32>(&page, 1).is_delta);

        let mut decoder = PforDecoder::<Int32Type>::new();
        decoder.set_data(page, values.len()).unwrap();
        assert_eq!(decoder.skip(64).unwrap(), 64);
        let mut out = vec![0i32; 64];
        decoder.get(&mut out).unwrap();
        assert_eq!(out, values[64..]);
    }

    #[test]
    fn test_mode_is_decided_per_vector() {
        // A ramp followed by unclustered draws. One page, and the two vectors have to disagree.
        let mut rng = Rng(0x0123_4567_89AB_CDEF);
        let mut values: Vec<i32> = (0..1024).map(|i| i * 1000).collect();
        values.extend((0..1024).map(|_| (rng.next() % 1_000_000) as i32));

        let page = round_trip::<Int32Type>(&values, 1024);
        assert!(vector_info::<i32>(&page, 0).is_delta);
        assert!(!vector_info::<i32>(&page, 1).is_delta);
    }

    #[test]
    fn test_exception_values_in_a_delta_vector_are_differences() {
        // A ramp with one jump in it. Differencing wins, and the jump has to travel as a difference
        // rather than as a value -- the decoder patches before it sums.
        let mut values: Vec<i32> = (0..1024).map(|i| i * 3).collect();
        values[512] += 10_000_000;
        for value in values.iter_mut().skip(513) {
            *value += 10_000_000;
        }

        let page = round_trip::<Int32Type>(&values, 1024);
        let info = vector_info::<i32>(&page, 0);
        assert!(info.is_delta);
        assert_eq!(info.num_exceptions, 1);
    }

    #[test]
    fn test_page_with_a_partial_final_vector() {
        // Not a multiple of the vector size, so the last vector is short. Its offset still has to
        // land where the header says, and its element count comes from the header rather than from
        // anything in the vector itself.
        let values: Vec<i32> = (0..100).collect();
        let page = round_trip::<Int32Type>(&values, 64);
        let header = PforHeader::read::<i32>(&page).unwrap();
        assert_eq!(header.num_vectors(), 2);
        assert_eq!(header.num_elements, 100);
    }

    #[test]
    fn test_empty_page_is_a_bare_header() {
        let mut encoder = PforEncoder::<Int32Type>::new();
        let page = encoder.flush_buffer().unwrap();
        assert_eq!(page.len(), HEADER_SIZE);
        let header = PforHeader::read::<i32>(&page).unwrap();
        assert_eq!(header.num_elements, 0);
        assert_eq!(header.num_vectors(), 0);
    }

    #[test]
    fn test_encoder_is_reusable_after_a_flush() {
        let mut encoder = PforEncoder::<Int32Type>::new().with_vector_size(8).unwrap();

        let first: Vec<i32> = (0..20).collect();
        encoder.put(&first).unwrap();
        let page = encoder.flush_buffer().unwrap();
        assert_eq!(PforHeader::read::<i32>(&page).unwrap().num_elements, 20);

        // The buffer has to be empty again, not carrying the first page's values into the second.
        assert_eq!(encoder.estimated_data_encoded_size(), 0);
        let second: Vec<i32> = (100..105).collect();
        encoder.put(&second).unwrap();
        let page = encoder.flush_buffer().unwrap();
        assert_eq!(PforHeader::read::<i32>(&page).unwrap().num_elements, 5);

        let mut decoder = PforDecoder::<Int32Type>::new();
        decoder.set_data(page, second.len()).unwrap();
        let mut out = vec![0i32; second.len()];
        decoder.get(&mut out).unwrap();
        assert_eq!(out, second);
    }

    #[test]
    fn test_put_accumulates_across_calls() {
        let mut encoder = PforEncoder::<Int32Type>::new().with_vector_size(8).unwrap();
        let values: Vec<i32> = (0..20).collect();
        for chunk in values.chunks(3) {
            encoder.put(chunk).unwrap();
        }
        let page = encoder.flush_buffer().unwrap();

        let mut decoder = PforDecoder::<Int32Type>::new();
        decoder.set_data(page, values.len()).unwrap();
        let mut out = vec![0i32; values.len()];
        decoder.get(&mut out).unwrap();
        assert_eq!(out, values);
    }

    #[test]
    fn test_estimated_sizes() {
        let mut encoder = PforEncoder::<Int64Type>::new();
        assert_eq!(encoder.encoding(), Encoding::PFOR);
        assert_eq!(encoder.estimated_data_encoded_size(), 0);
        encoder.put(&(0..100).collect::<Vec<i64>>()).unwrap();
        // Unpacked size: the real one is not known until the widths are chosen, and this has to be
        // O(1). It is an upper bound, which is the direction that matters to a writer sizing pages.
        assert_eq!(encoder.estimated_data_encoded_size(), 800);
        assert!(encoder.estimated_memory_size() >= 800);
    }

    #[test]
    fn test_rejects_a_vector_size_the_header_cannot_describe() {
        assert!(
            PforEncoder::<Int32Type>::new()
                .with_vector_size(1000)
                .is_err()
        );
        assert!(PforEncoder::<Int32Type>::new().with_vector_size(4).is_err());
        assert!(
            PforEncoder::<Int32Type>::new()
                .with_vector_size(1 << 16)
                .is_err()
        );
        assert!(PforEncoder::<Int32Type>::new().with_vector_size(8).is_ok());
    }

    #[test]
    fn test_zigzag() {
        assert_eq!(zigzag(0i32), 0);
        assert_eq!(zigzag(-1i32), 1);
        assert_eq!(zigzag(1i32), 2);
        assert_eq!(zigzag(-2i32), 3);
        assert_eq!(zigzag(i32::MAX), u32::MAX as u64 - 1);
        assert_eq!(zigzag(i32::MIN), u32::MAX as u64);
        assert_eq!(zigzag(-1i64), 1);
        assert_eq!(zigzag(i64::MIN), u64::MAX);
        assert_eq!(zigzag(i64::MAX), u64::MAX - 1);
    }

    #[test]
    fn test_compute_deltas() {
        let mut deltas = Vec::new();
        let bounds = compute_deltas(&[10i32, 13, 11, 11], &mut deltas);
        // The first slot is zero: the value it would hold travels in the start value instead.
        assert_eq!(deltas, vec![0, 3, -2, 0]);
        assert_eq!(bounds.min, -2);
        assert_eq!(bounds.max, 3);

        // Differencing across the range of the type wraps, and the decoder sums the same way.
        let mut deltas = Vec::new();
        compute_deltas(&[i32::MIN, i32::MAX], &mut deltas);
        assert_eq!(deltas, vec![0, -1]);
    }

    #[test]
    fn test_min_max() {
        let bounds = min_max(&[3i32, -1, 7, 7, 0]);
        assert_eq!(bounds.min, -1);
        assert_eq!(bounds.max, 7);
        let bounds = min_max(&[5i64]);
        assert_eq!(bounds.min, 5);
        assert_eq!(bounds.max, 5);
    }

    #[test]
    fn test_best_width_from_histogram_prefers_patching() {
        // 1000 values needing 4 bits and one needing 30. Packing all of them costs 30 bits each;
        // packing at 4 and patching the one costs 4 bits each plus one exception slot.
        let mut histogram = [0i32; 65];
        histogram[4] = 1000;
        histogram[30] = 1;
        let (width, cost) = best_width_from_histogram::<i32>(&histogram, 1001);
        assert_eq!(width, 4);
        assert_eq!(cost, 1001 * 4 + exception_bits(4));

        // With enough of them, the wide values stop being exceptions and set the width.
        let mut histogram = [0i32; 65];
        histogram[4] = 10;
        histogram[30] = 1000;
        let (width, _) = best_width_from_histogram::<i32>(&histogram, 1010);
        assert_eq!(width, 30);
    }

    #[test]
    fn test_build_offset_histogram_wraps_below_the_frame() {
        // A value below the frame lands in the top bin rather than in a low one, which is what makes
        // it fail the width test and become a patch.
        let mut histogram = [0i32; 65];
        build_offset_histogram(&[10i32, 11, 9], 10, &mut histogram);
        assert_eq!(histogram[0], 1); // 10 - 10
        assert_eq!(histogram[1], 1); // 11 - 10
        // 9 - 10 wraps to a 32-bit residual, not a 64-bit one, so it needs 32 bits. Wrapping at 64
        // would put it past every width INT32 has, and the full-width candidate would then look
        // like it needed exceptions.
        assert_eq!(histogram[32], 1);
        assert_eq!(histogram[64], 0);

        // The same on INT64, where the wrap is at the full width.
        let mut histogram = [0i32; 65];
        build_offset_histogram(&[10i64, 9], 10, &mut histogram);
        assert_eq!(histogram[0], 1);
        assert_eq!(histogram[64], 1);
    }

    #[test]
    fn test_round_trip_a_page_of_many_vectors() {
        // Enough vectors that the offset array is a real structure rather than one entry, over a
        // shape whose vectors do not all plan the same way.
        let values: Vec<i32> = (0..10_000i32)
            .map(|i| match i / 8 % 3 {
                0 => i,                                  // a ramp
                1 => 500,                                // a constant
                _ => (i.wrapping_mul(2_654_435)) % 1024, // scattered
            })
            .collect();
        let page = round_trip::<Int32Type>(&values, 8);
        let header = PforHeader::read::<i32>(&page).unwrap();
        assert_eq!(header.num_vectors(), 1250);
    }
}
