// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Comparing ALP-encoded values with a scalar without decoding whole pages.

use std::ops::Range;

use arrow_array::ArrowNativeTypeOp;
use arrow_buffer::BooleanBufferBuilder;

use super::{AlpEncodedVectorView, AlpPage, CurrentVector, decode_range};
use crate::arrow::arrow_reader::ComparisonOp;
use crate::encodings::alp::{AlpExact, AlpFloat};
use crate::errors::{ParquetError, Result};

impl ComparisonOp {
    pub(crate) fn eval<Value: ArrowNativeTypeOp>(self, value: Value, scalar: Value) -> bool {
        match self {
            Self::Eq => value.is_eq(scalar),
            Self::NotEq => value.is_ne(scalar),
            Self::Lt => value.is_lt(scalar),
            Self::LtEq => value.is_le(scalar),
            Self::Gt => value.is_gt(scalar),
            Self::GtEq => value.is_ge(scalar),
        }
    }

    /// Append `op(value, scalar)` for each value to `out`, with the operator
    /// resolved outside the loop as Arrow's comparison kernels do. `packed` is
    /// reusable scratch.
    pub(crate) fn append_all<Value: ArrowNativeTypeOp>(
        self,
        values: &[Value],
        scalar: Value,
        out: &mut BooleanBufferBuilder,
        packed: &mut Vec<u8>,
    ) {
        match self {
            Self::Eq => append_bits(values, out, packed, |v| v.is_eq(scalar)),
            Self::NotEq => append_bits(values, out, packed, |v| v.is_ne(scalar)),
            Self::Lt => append_bits(values, out, packed, |v| v.is_lt(scalar)),
            Self::LtEq => append_bits(values, out, packed, |v| v.is_le(scalar)),
            Self::Gt => append_bits(values, out, packed, |v| v.is_gt(scalar)),
            Self::GtEq => append_bits(values, out, packed, |v| v.is_ge(scalar)),
        }
    }

    /// The result shared by every value in `[min, max]`, if there is one.
    fn eval_bounds<Value: ArrowNativeTypeOp>(
        self,
        min: Value,
        max: Value,
        scalar: Value,
    ) -> Option<bool> {
        match self {
            Self::Eq | Self::NotEq => {
                let equal = if min.is_eq(scalar) && max.is_eq(scalar) {
                    Some(true)
                } else if scalar.is_lt(min) || scalar.is_gt(max) {
                    Some(false)
                } else {
                    None
                };
                equal.map(|equal| equal == (self == Self::Eq))
            }
            // The ordering comparisons are monotonic in the value, so agreement at
            // both ends holds for everything between.
            _ => {
                let at_min = self.eval(min, scalar);
                (at_min == self.eval(max, scalar)).then_some(at_min)
            }
        }
    }
}

/// Pack `f` of each value 64 to a little-endian word in `packed`, then append
/// the bits to `out`.
fn append_bits<Value: Copy>(
    values: &[Value],
    out: &mut BooleanBufferBuilder,
    packed: &mut Vec<u8>,
    f: impl Fn(Value) -> bool,
) {
    let n = values.len();
    // Unchecked reads let the loop vectorize, as in arrow-ord's kernels.
    let at = |i: usize| {
        debug_assert!(i < n);
        // SAFETY: both loops below index only `0..n`, and `n == values.len()`.
        unsafe { *values.get_unchecked(i) }
    };
    packed.clear();
    let whole = n / 64;
    for word in 0..whole {
        let mut bits = 0u64;
        for bit in 0..64 {
            bits |= u64::from(f(at(word * 64 + bit))) << bit;
        }
        packed.extend_from_slice(&bits.to_le_bytes());
    }
    if whole * 64 < n {
        let mut bits = 0u64;
        for bit in 0..n - whole * 64 {
            bits |= u64::from(f(at(whole * 64 + bit))) << bit;
        }
        packed.extend_from_slice(&bits.to_le_bytes());
    }
    out.append_packed_range(0..n, packed);
}

/// Bounds of the values decoded from a vector's packed slots, exceptions aside,
/// or `None` when `frame_of_reference + 2^bit_width - 1` overflows the signed
/// integer type and the slots are not ordered.
///
/// Exception slots hold placeholders that decode inside these bounds, so the
/// bounds cover every slot the decoder reads before patching exceptions.
fn packed_bounds<Value: AlpFloat>(
    view: &AlpEncodedVectorView<Value::Exact>,
) -> Option<(Value, Value)> {
    let for_info = &view.for_info;
    let max_delta = u64::MAX
        .checked_shr(64 - u32::from(for_info.bit_width))
        .unwrap_or(0);
    let max_delta = Value::Exact::from_le_slice(&max_delta.to_le_bytes()[..Value::Exact::WIDTH]);
    let min = for_info.frame_of_reference.reinterpret_as_signed();
    let max = for_info
        .frame_of_reference
        .wrapping_add(max_delta)
        .reinterpret_as_signed();
    // Decoding is monotonic in the signed integer, so the bounds hold unless
    // adding the largest delta wrapped around.
    (max >= min).then(|| {
        let scale = Value::decode_scale(view.alp_info.exponent, view.alp_info.factor);
        (
            Value::decode_value(min, scale),
            Value::decode_value(max, scale),
        )
    })
}

/// Buffers reused across [`AlpPage::compare_range_with`] calls over one page,
/// and the vector the last call stopped in.
pub(crate) struct CompareState<Value: AlpFloat> {
    scratch: Vec<Value::Exact>,
    decoded: Vec<Value>,
    packed: Vec<u8>,
    open: Option<OpenVector<Value>>,
}

impl<Value: AlpFloat> CompareState<Value> {
    pub(crate) fn new(page: &AlpPage<Value>) -> Self {
        Self {
            scratch: vec![Value::Exact::default(); page.decode_tile_len()],
            decoded: Vec::new(),
            packed: Vec::new(),
            open: None,
        }
    }
}

/// A vector compared up to vector-local value `next`.
struct OpenVector<Value: AlpFloat> {
    idx: usize,
    next: usize,
    view: AlpEncodedVectorView<Value::Exact>,
    decision: Decision<Value>,
}

enum Decision<Value: AlpFloat> {
    /// The packed bounds give every non-exception value this result.
    Shared(bool),
    /// Decode and compare each value, continuing from the cursor.
    Decode(CurrentVector<Value>),
}

impl<Value: AlpFloat + ArrowNativeTypeOp> AlpPage<Value> {
    /// Append `op(value, scalar)` for the values in `range` to `out`.
    ///
    /// `range` counts encoded values from the start of the page, which skips
    /// nulls. A vector whose packed bounds decide the comparison is not unpacked:
    /// only its exceptions are compared. Other vectors are decoded.
    pub fn compare_range(
        &self,
        op: ComparisonOp,
        scalar: Value,
        range: Range<usize>,
        out: &mut BooleanBufferBuilder,
    ) -> Result<()> {
        self.compare_range_with(&mut CompareState::new(self), op, scalar, range, out)
    }

    /// [`Self::compare_range`] with buffers, and the vector the previous call
    /// stopped in, kept in `state` for the next call.
    ///
    /// `state` must come from this page and be used with one `op` and `scalar`.
    pub(crate) fn compare_range_with(
        &self,
        state: &mut CompareState<Value>,
        op: ComparisonOp,
        scalar: Value,
        range: Range<usize>,
        out: &mut BooleanBufferBuilder,
    ) -> Result<()> {
        if range.start > range.end || range.end > self.num_values() {
            return Err(general_err!(
                "ALP compare range {:?} out of bounds for {} values",
                range,
                self.num_values()
            ));
        }
        if range.is_empty() {
            return Ok(());
        }

        let vector_size = self.vector_size();
        for idx in range.start / vector_size..=(range.end - 1) / vector_size {
            let base = idx * vector_size;
            let lo = range.start.saturating_sub(base);
            let mut open = match state.open.take() {
                Some(open) if open.idx == idx && open.next == lo => open,
                _ => self.open_vector(idx, lo, op, scalar)?,
            };
            let len = open.view.num_elements as usize;
            let hi = (range.end - base).min(len);

            match &mut open.decision {
                Decision::Shared(result) => {
                    let start = out.len();
                    out.append_n(hi - lo, *result);
                    open.view
                        .for_each_exception_in(&self.body, lo..hi, |offset, bits| {
                            let value = Value::from_exact_bits(bits);
                            out.set_bit(start + offset, op.eval(value, scalar));
                        });
                }
                Decision::Decode(cur) => {
                    state.decoded.resize(hi - lo, Value::default());
                    decode_range(cur, &self.body, &mut state.scratch, &mut state.decoded)?;
                    op.append_all(&state.decoded, scalar, out, &mut state.packed);
                }
            }

            open.next = hi;
            if hi < len {
                state.open = Some(open);
            }
        }
        Ok(())
    }

    /// Parse vector `idx` and decide how to compare it from value `lo` on.
    fn open_vector(
        &self,
        idx: usize,
        lo: usize,
        op: ComparisonOp,
        scalar: Value,
    ) -> Result<OpenVector<Value>> {
        let view = self.vector(idx)?;
        let shared =
            packed_bounds::<Value>(&view).and_then(|(min, max)| op.eval_bounds(min, max, scalar));
        let decision = match shared {
            Some(result) => Decision::Shared(result),
            None => {
                let mut cur = CurrentVector::new(self, view);
                cur.advance(lo);
                Decision::Decode(cur)
            }
        };
        Ok(OpenVector {
            idx,
            next: lo,
            view,
            decision,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::basic::Type as PhysicalType;
    use crate::column::page::Page;
    use crate::data_type::{DataType, DoubleType, FloatType};
    use crate::encodings::encoding::{AlpEncoder, Encoder};
    use crate::file::reader::{FileReader, SerializedFileReader};
    use crate::util::test_common::file_util::get_test_file;
    use arrow::compute::kernels::cmp;
    use arrow_array::types::{Float32Type, Float64Type};
    use arrow_array::{ArrowPrimitiveType, BooleanArray, PrimitiveArray, Scalar};
    use bytes::Bytes;

    const OPS: [ComparisonOp; 6] = [
        ComparisonOp::Eq,
        ComparisonOp::NotEq,
        ComparisonOp::Lt,
        ComparisonOp::LtEq,
        ComparisonOp::Gt,
        ComparisonOp::GtEq,
    ];

    fn arrow_compare<P: ArrowPrimitiveType>(
        values: &PrimitiveArray<P>,
        op: ComparisonOp,
        scalar: P::Native,
    ) -> BooleanArray {
        let scalar = Scalar::new(PrimitiveArray::<P>::from_iter_values([scalar]));
        match op {
            ComparisonOp::Eq => cmp::eq(values, &scalar),
            ComparisonOp::NotEq => cmp::neq(values, &scalar),
            ComparisonOp::Lt => cmp::lt(values, &scalar),
            ComparisonOp::LtEq => cmp::lt_eq(values, &scalar),
            ComparisonOp::Gt => cmp::gt(values, &scalar),
            ComparisonOp::GtEq => cmp::gt_eq(values, &scalar),
        }
        .unwrap()
    }

    /// Windows for fresh `compare_range` calls that the walk in
    /// [`assert_matches_arrow`] does not make: the whole page, an empty range,
    /// and one that starts mid-vector and crosses a vector boundary.
    fn ranges(num_values: usize, vector_size: usize) -> Vec<Range<usize>> {
        let n = num_values;
        let v = vector_size;
        [0..n, 0..0, v - 1..v + 1]
            .into_iter()
            .filter(|r| r.end <= n)
            .collect()
    }

    /// Check `compare_range` against decoding the page and comparing with
    /// Arrow's kernels, for every operator and each scalar `scalars` picks from
    /// the decoded values.
    fn assert_matches_arrow<T, P>(page: Bytes, scalars: fn(&[T::T]) -> Vec<T::T>)
    where
        T: DataType,
        T::T: AlpFloat + ArrowNativeTypeOp,
        <T::T as AlpFloat>::Exact: Send,
        P: ArrowPrimitiveType<Native = T::T>,
    {
        let alp = AlpPage::<T::T>::try_new(page.clone()).unwrap();
        let n = alp.num_values();
        let values = super::super::tests::decode_page::<T>(page.to_vec(), n);

        let ranges = ranges(n, alp.vector_size());
        let scalars = scalars(&values);
        let decoded = PrimitiveArray::<P>::from_iter_values(values);
        for scalar in scalars {
            for op in OPS {
                let expected = arrow_compare(&decoded, op, scalar);
                for range in &ranges {
                    let mut out = BooleanBufferBuilder::new(range.len());
                    alp.compare_range(op, scalar, range.clone(), &mut out)
                        .unwrap();
                    assert_eq!(
                        out.finish(),
                        expected.values().slice(range.start, range.len()),
                        "{op:?} {scalar:?} {range:?}"
                    );
                }
                // Walk the page in uneven steps with one state, sometimes leaving
                // a gap, so calls stop inside vectors and resume there or later.
                let mut state = CompareState::new(&alp);
                let mut start = 0;
                for (step, gap) in [(1, 0), (333, 5), (700, 0), (64, 17), (1500, 1)]
                    .into_iter()
                    .cycle()
                {
                    if start >= n {
                        break;
                    }
                    let end = (start + step).min(n);
                    let mut out = BooleanBufferBuilder::new(end - start);
                    alp.compare_range_with(&mut state, op, scalar, start..end, &mut out)
                        .unwrap();
                    assert_eq!(
                        out.finish(),
                        expected.values().slice(start, end - start),
                        "{op:?} {scalar:?} walk at {start}..{end}"
                    );
                    start = end + gap;
                }
            }
        }
    }

    // Scalars for a sample of `values`: each sampled value, both of its float
    // neighbours, the midpoint between consecutive sampled values, and the
    // special values. Midpoints put a threshold in every gap, such as between a
    // vector's packed values and a far outlier.
    macro_rules! scalars {
        ($name:ident, $t:ty) => {
            fn $name(values: &[$t]) -> Vec<$t> {
                let step = values.len().div_ceil(40).max(1);
                let mut sample: Vec<$t> = values.iter().copied().step_by(step).collect();
                sample.extend(values.iter().copied().filter(|v| v.abs() > 1e5).take(16));
                sample.sort_by(<$t>::total_cmp);
                sample.dedup_by(|a, b| a.total_cmp(b).is_eq());
                let mut scalars = vec![<$t>::NAN, <$t>::INFINITY, <$t>::NEG_INFINITY, 0.0, -0.0];
                for pair in sample.windows(2) {
                    scalars.push(pair[0] / 2.0 + pair[1] / 2.0);
                }
                for value in sample {
                    scalars.extend([value, value.next_up(), value.next_down()]);
                }
                scalars
            }
        };
    }
    scalars!(f64_scalars, f64);
    scalars!(f32_scalars, f32);

    fn page<T: DataType>(values: &[T::T]) -> Bytes
    where
        T::T: AlpFloat,
    {
        let mut encoder = AlpEncoder::<T>::new();
        encoder.put(values).unwrap();
        encoder.flush_buffer().unwrap()
    }

    fn check_f64(values: &[f64]) {
        assert_matches_arrow::<DoubleType, Float64Type>(page::<DoubleType>(values), f64_scalars);
    }

    fn check_f32(values: &[f32]) {
        assert_matches_arrow::<FloatType, Float32Type>(page::<FloatType>(values), f32_scalars);
    }

    /// Two-decimal values in ascending order, with exceptions that land inside
    /// and far outside each vector's packed range.
    fn sorted_with_exceptions(n: usize) -> Vec<f64> {
        (0..n)
            .map(|i| {
                let decimal = (i as f64 - 2000.0) / 100.0;
                match i % 97 {
                    5 => decimal + 1.0 / 3e6,
                    11 => 1e6 + 1.0 / 3.0,
                    13 => -1e6 - 1.0 / 3.0,
                    _ => decimal,
                }
            })
            .collect()
    }

    #[test]
    fn sorted_page() {
        check_f64(&sorted_with_exceptions(5000));
    }

    #[test]
    fn shuffled_page() {
        let sorted = sorted_with_exceptions(5000);
        // Deterministic shuffle: a stride coprime with the length.
        let shuffled: Vec<f64> = (0..5000).map(|i| sorted[i * 1931 % 5000]).collect();
        check_f64(&shuffled);
    }

    #[test]
    fn all_exception_vectors() {
        // None of these can be ALP encoded, so every packed slot is a placeholder.
        let specials = [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -0.0];
        let values: Vec<f64> = (0..2500).map(|i| specials[i % 4]).collect();
        check_f64(&values);

        let specials = [f32::NAN, f32::INFINITY, f32::NEG_INFINITY, -0.0];
        let values: Vec<f32> = (0..2500).map(|i| specials[i % 4]).collect();
        check_f32(&values);
    }

    #[test]
    fn constant_vectors() {
        check_f64(&vec![2.5; 3000]);
    }

    #[test]
    fn packed_bounds_overflow_decodes() {
        // One vector with frame of reference 1 and bit width 64. The second
        // delta wraps around to -1, below the frame of reference, so the vector
        // has no packed bounds and must be decoded.
        let mut page = vec![0, 0, 3];
        page.extend_from_slice(&2i32.to_le_bytes());
        page.extend_from_slice(&4u32.to_le_bytes());
        page.extend_from_slice(&[0, 0, 0, 0]);
        page.extend_from_slice(&1u64.to_le_bytes());
        page.push(64);
        page.extend_from_slice(&0u64.to_le_bytes());
        page.extend_from_slice(&(u64::MAX - 1).to_le_bytes());
        let page = Bytes::from(page);

        let alp = AlpPage::<f64>::try_new(page.clone()).unwrap();
        assert!(packed_bounds::<f64>(&alp.vector(0).unwrap()).is_none());
        assert_matches_arrow::<DoubleType, Float64Type>(page, f64_scalars);
    }

    #[test]
    fn f32_pages() {
        let values: Vec<f32> = (0..3000)
            .map(|i| match i % 101 {
                7 => 1e6 + 1.0 / 3.0,
                9 => i as f32 / 3.0,
                _ => (i as f32 - 400.0) / 4.0,
            })
            .collect();
        check_f32(&values);
    }

    #[test]
    fn alp_extended_file() {
        // Pages written by another implementation, with vector sizes 32, 1024
        // and 4096, nulls and non-finite values.
        let reader = SerializedFileReader::new(get_test_file("alp_extended.zstd.parquet")).unwrap();
        let schema = reader.metadata().file_metadata().schema_descr_ptr();
        let mut checked = 0;
        for col in 0..schema.num_columns() {
            let descr = schema.column(col);
            let mut pages = reader
                .get_row_group(0)
                .unwrap()
                .get_column_page_reader(col)
                .unwrap();
            while let Some(page) = pages.get_next_page().unwrap() {
                let values = match page {
                    Page::DataPage {
                        buf,
                        encoding: crate::basic::Encoding::ALP,
                        ..
                    } if descr.max_def_level() > 0 => {
                        let len = u32::from_le_bytes(buf[..4].try_into().unwrap()) as usize;
                        buf.slice(4 + len..)
                    }
                    Page::DataPage {
                        buf,
                        encoding: crate::basic::Encoding::ALP,
                        ..
                    } => buf,
                    Page::DataPageV2 {
                        buf,
                        encoding: crate::basic::Encoding::ALP,
                        def_levels_byte_len,
                        rep_levels_byte_len,
                        ..
                    } => buf.slice((def_levels_byte_len + rep_levels_byte_len) as usize..),
                    _ => continue,
                };
                match descr.physical_type() {
                    PhysicalType::DOUBLE => {
                        assert_matches_arrow::<DoubleType, Float64Type>(values, f64_scalars)
                    }
                    PhysicalType::FLOAT => {
                        assert_matches_arrow::<FloatType, Float32Type>(values, f32_scalars)
                    }
                    other => panic!("unexpected ALP physical type {other}"),
                }
                checked += 1;
            }
        }
        assert!(checked > 0, "no ALP pages found");
    }

    #[test]
    fn append_all_matches_eval() {
        // Small enough for Miri, which checks the unchecked reads in
        // `append_bits`. The output starts mid-byte, as a reader's often does.
        let values: Vec<f64> = (0..130).map(|i| (i % 7) as f64 - 3.0).collect();
        let mut packed = Vec::new();
        for len in [0, 1, 63, 64, 65, 130] {
            let values = &values[..len];
            for op in OPS {
                let mut out = BooleanBufferBuilder::new(3 + len);
                out.append_n(3, true);
                op.append_all(values, 0.0, &mut out, &mut packed);
                let bits = out.finish();
                assert_eq!(bits.len(), 3 + len);
                assert!((0..3).all(|i| bits.value(i)));
                for (i, &value) in values.iter().enumerate() {
                    assert_eq!(bits.value(3 + i), op.eval(value, 0.0), "{op:?} {len} {i}");
                }
            }
        }
    }

    #[test]
    fn range_out_of_bounds() {
        let alp = AlpPage::<f64>::try_new(page::<DoubleType>(&[1.5, 2.5, 3.5])).unwrap();
        let mut out = BooleanBufferBuilder::new(0);
        for range in [0..4, Range { start: 3, end: 2 }] {
            let err = alp
                .compare_range(ComparisonOp::Eq, 1.5, range, &mut out)
                .unwrap_err();
            assert!(
                err.to_string().contains("out of bounds for 3 values"),
                "{err}"
            );
        }
        assert_eq!(out.len(), 0);
    }
}
