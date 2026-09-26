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

//! Reads the page statistics stored in a Parquet `ColumnIndex` and puts them
//! straight into Arrow arrays.
//!
//! The usual route first builds a [`ColumnIndexMetaData`] and then copies its
//! values into Arrow arrays one by one. This module skips the middle step: it
//! reads the stored bytes once and writes each value directly into the memory
//! that will become the final Arrow array.
//!
//! [`ColumnIndexMetaData`]: crate::file::page_index::column_index::ColumnIndexMetaData

use super::from_bytes_to_i256;
use super::{from_bytes_to_f16, from_bytes_to_i32, from_bytes_to_i64, from_bytes_to_i128};
use crate::basic::{BoundaryOrder, Type as PhysicalType};
use crate::errors::{ParquetError, Result};
use crate::parquet_thrift::{
    ElementType, FieldType, ReadThrift, ThriftCompactInputProtocol, ThriftSliceInputProtocol,
    validate_list_type,
};
use arrow_array::types::{
    Date32Type, Date64Type, Decimal32Type, Decimal64Type, Decimal128Type, Decimal256Type, Int8Type,
    Int16Type, Time32MillisecondType, Time32SecondType, Time64MicrosecondType,
    Time64NanosecondType, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, BooleanArray, FixedSizeBinaryArray,
    Float16Array, Float32Array, Float64Array, Int32Array, Int64Array, LargeBinaryArray,
    LargeStringArray, PrimitiveArray, StringArray, StringViewArray, UInt64Array, new_null_array,
};
use arrow_buffer::{
    BooleanBuffer, BooleanBufferBuilder, NullBuffer, OffsetBuffer, ScalarBuffer, i256,
};
use arrow_schema::{DataType, TimeUnit};
use std::sync::Arc;

/// The min or max values of every page, kept in the form Parquet stores them in
/// (its "physical type"), before they are turned into the Arrow type the
/// caller asked for.
#[derive(Debug)]
enum PhysicalValues {
    Boolean(BooleanBufferBuilder),
    Int32(Vec<i32>),
    Int64(Vec<i64>),
    Float(Vec<f32>),
    Double(Vec<f64>),
    /// Used for both `BYTE_ARRAY` and `FIXED_LEN_BYTE_ARRAY`. Fixed length
    /// values are kept here too, because a stored min or max may have been
    /// shortened and so may not have the expected length.
    Bytes {
        offsets: Vec<i32>,
        values: Vec<u8>,
    },
    /// `INT96` values are only counted, not kept. No Arrow type is read
    /// from them, so their mins and maxes always come out as nulls. They are
    /// still checked for length, as the older decoder does.
    Int96(usize),
    /// Decimals stored as bytes (in either kind of byte column), turned
    /// into numbers while reading. This skips keeping a copy of the bytes
    /// and converting them afterwards, which is much faster.
    Decimal32(Vec<i32>),
    Decimal64(Vec<i64>),
    Decimal128(Vec<i128>),
    Decimal256(Vec<i256>),
}

impl PhysicalValues {
    /// `data_type` is the Arrow type the caller wants. It only matters for
    /// decimals stored as bytes, which are turned into numbers right away.
    fn new(physical_type: PhysicalType, data_type: &DataType, capacity: usize) -> Self {
        let data_type = match data_type {
            DataType::Dictionary(_, value_type) => value_type.as_ref(),
            data_type => data_type,
        };
        if matches!(
            physical_type,
            PhysicalType::BYTE_ARRAY | PhysicalType::FIXED_LEN_BYTE_ARRAY
        ) {
            match data_type {
                DataType::Decimal32(..) => return Self::Decimal32(Vec::with_capacity(capacity)),
                DataType::Decimal64(..) => return Self::Decimal64(Vec::with_capacity(capacity)),
                DataType::Decimal128(..) => return Self::Decimal128(Vec::with_capacity(capacity)),
                DataType::Decimal256(..) => return Self::Decimal256(Vec::with_capacity(capacity)),
                _ => {}
            }
        }
        match physical_type {
            PhysicalType::BOOLEAN => Self::Boolean(BooleanBufferBuilder::new(capacity)),
            PhysicalType::INT32 => Self::Int32(Vec::with_capacity(capacity)),
            PhysicalType::INT64 => Self::Int64(Vec::with_capacity(capacity)),
            PhysicalType::FLOAT => Self::Float(Vec::with_capacity(capacity)),
            PhysicalType::DOUBLE => Self::Double(Vec::with_capacity(capacity)),
            PhysicalType::BYTE_ARRAY | PhysicalType::FIXED_LEN_BYTE_ARRAY => {
                let mut offsets = Vec::with_capacity(capacity + 1);
                offsets.push(0);
                Self::Bytes {
                    offsets,
                    values: Vec::new(),
                }
            }
            PhysicalType::INT96 => Self::Int96(0),
        }
    }

    /// Adds a filler value for a page that has no min or max.
    fn append_null(&mut self) {
        self.append_nulls(1)
    }

    fn append_nulls(&mut self, n: usize) {
        match self {
            Self::Boolean(b) => b.append_n(n, false),
            Self::Int32(v) => v.resize(v.len() + n, 0),
            Self::Int64(v) => v.resize(v.len() + n, 0),
            Self::Float(v) => v.resize(v.len() + n, 0.0),
            Self::Double(v) => v.resize(v.len() + n, 0.0),
            Self::Bytes { offsets, values } => {
                // an empty entry: it starts and ends where the data ends now
                offsets.resize(offsets.len() + n, values.len() as i32)
            }
            Self::Int96(count) => *count += n,
            Self::Decimal32(v) => v.resize(v.len() + n, 0),
            Self::Decimal64(v) => v.resize(v.len() + n, 0),
            Self::Decimal128(v) => v.resize(v.len() + n, 0),
            Self::Decimal256(v) => v.resize(v.len() + n, i256::ZERO),
        }
    }

    /// Adds one stored min or max value.
    fn append_value(&mut self, bytes: &[u8]) -> Result<()> {
        match self {
            Self::Boolean(b) => b.append(first_bytes::<1>(bytes)?[0] != 0),
            Self::Int32(v) => v.push(i32::from_le_bytes(first_bytes(bytes)?)),
            Self::Int64(v) => v.push(i64::from_le_bytes(first_bytes(bytes)?)),
            Self::Float(v) => v.push(f32::from_le_bytes(first_bytes(bytes)?)),
            Self::Double(v) => v.push(f64::from_le_bytes(first_bytes(bytes)?)),
            Self::Bytes { offsets, values } => {
                values.extend_from_slice(bytes);
                offsets.push(bytes_end(values)?);
            }
            Self::Int96(count) => {
                first_bytes::<12>(bytes)?;
                *count += 1;
            }
            Self::Decimal32(v) => v.push(from_bytes_to_i32(bytes)),
            Self::Decimal64(v) => v.push(from_bytes_to_i64(bytes)),
            Self::Decimal128(v) => v.push(from_bytes_to_i128(bytes)),
            Self::Decimal256(v) => v.push(from_bytes_to_i256(bytes)),
        }
        Ok(())
    }

    /// Reads one value per page from a list, straight into the buffer.
    ///
    /// `has_min_max` holds one entry for each page in the list. Pages marked
    /// `false` get a filler value.
    ///
    /// This does the same as calling [`Self::append_value`] or
    /// [`Self::append_null`] for each value, but picks the value kind once
    /// for the whole list rather than once per value, which is much faster.
    fn read_list(
        &mut self,
        prot: &mut ThriftSliceInputProtocol,
        has_min_max: &[bool],
    ) -> Result<()> {
        // Work on a local copy of the unread bytes, so the compiler can keep
        // the read position in a register rather than in memory. This makes
        // these loops much faster.
        let mut buf = prot.as_slice();
        match self {
            Self::Boolean(b) => {
                for &has_value in has_min_max {
                    let (bytes, rest) = split_binary(buf)?;
                    buf = rest;
                    b.append(has_value && first_bytes::<1>(bytes)?[0] != 0);
                }
            }
            Self::Int32(v) => read_fixed(&mut buf, v, has_min_max, i32::from_le_bytes)?,
            Self::Int64(v) => read_fixed(&mut buf, v, has_min_max, i64::from_le_bytes)?,
            Self::Float(v) => read_fixed(&mut buf, v, has_min_max, f32::from_le_bytes)?,
            Self::Double(v) => read_fixed(&mut buf, v, has_min_max, f64::from_le_bytes)?,
            Self::Bytes { offsets, values } => {
                offsets.reserve(has_min_max.len());
                for &has_value in has_min_max {
                    let (bytes, rest) = split_binary(buf)?;
                    buf = rest;
                    if has_value {
                        values.extend_from_slice(bytes);
                    }
                    offsets.push(bytes_end(values)?);
                }
            }
            Self::Int96(count) => {
                for &has_value in has_min_max {
                    let (bytes, rest) = split_binary(buf)?;
                    buf = rest;
                    if has_value {
                        first_bytes::<12>(bytes)?;
                    }
                }
                *count += has_min_max.len();
            }
            Self::Decimal32(v) => read_each(&mut buf, v, has_min_max, from_bytes_to_i32)?,
            Self::Decimal64(v) => read_each(&mut buf, v, has_min_max, from_bytes_to_i64)?,
            Self::Decimal128(v) => read_each(&mut buf, v, has_min_max, from_bytes_to_i128)?,
            Self::Decimal256(v) => read_each(&mut buf, v, has_min_max, from_bytes_to_i256)?,
        }
        let used = prot.as_slice().len() - buf.len();
        Ok(prot.skip_bytes(used)?)
    }

    /// Turns the values into an Arrow array of the matching physical type.
    fn finish(self, nulls: Option<NullBuffer>) -> PhysicalArray {
        match self {
            Self::Boolean(mut b) => PhysicalArray::Boolean(BooleanArray::new(b.finish(), nulls)),
            Self::Int32(v) => PhysicalArray::Int32(Int32Array::new(ScalarBuffer::from(v), nulls)),
            Self::Int64(v) => PhysicalArray::Int64(Int64Array::new(ScalarBuffer::from(v), nulls)),
            Self::Float(v) => PhysicalArray::Float(Float32Array::new(ScalarBuffer::from(v), nulls)),
            Self::Double(v) => {
                PhysicalArray::Double(Float64Array::new(ScalarBuffer::from(v), nulls))
            }
            Self::Bytes { offsets, values } => {
                // the offsets never go down, because values are only ever added
                let offsets = OffsetBuffer::new(ScalarBuffer::from(offsets));
                PhysicalArray::Bytes(BinaryArray::new(offsets, values.into(), nulls))
            }
            Self::Int96(count) => PhysicalArray::Int96(count),
            Self::Decimal32(v) => PhysicalArray::Decimal32(PrimitiveArray::new(v.into(), nulls)),
            Self::Decimal64(v) => PhysicalArray::Decimal64(PrimitiveArray::new(v.into(), nulls)),
            Self::Decimal128(v) => PhysicalArray::Decimal128(PrimitiveArray::new(v.into(), nulls)),
            Self::Decimal256(v) => PhysicalArray::Decimal256(PrimitiveArray::new(v.into(), nulls)),
        }
    }
}

/// Reads fixed width values, one per page. See [`PhysicalValues::read_list`].
#[inline(never)]
fn read_fixed<const N: usize, T: Default>(
    buf: &mut &[u8],
    out: &mut Vec<T>,
    has_min_max: &[bool],
    from_le_bytes: impl Fn([u8; N]) -> T,
) -> Result<()> {
    out.reserve(has_min_max.len());
    for &has_value in has_min_max {
        let (bytes, rest) = split_binary(buf)?;
        *buf = rest;
        out.push(if has_value {
            from_le_bytes(first_bytes(bytes)?)
        } else {
            T::default()
        });
    }
    Ok(())
}

/// Reads byte values and turns each into a number with `convert`, one per
/// page. Pages without a min or max get a filler value, and `convert` is not
/// called for them. See [`PhysicalValues::read_list`].
#[inline(never)]
fn read_each<T: Default>(
    buf: &mut &[u8],
    out: &mut Vec<T>,
    has_min_max: &[bool],
    convert: impl Fn(&[u8]) -> T,
) -> Result<()> {
    out.reserve(has_min_max.len());
    for &has_value in has_min_max {
        let (bytes, rest) = split_binary(buf)?;
        *buf = rest;
        out.push(if has_value {
            convert(bytes)
        } else {
            T::default()
        });
    }
    Ok(())
}

/// Splits one Thrift binary value off the front of `buf`, returning the
/// value and the bytes after it. A binary value is its length followed by
/// its bytes.
#[inline(always)]
fn split_binary(buf: &[u8]) -> Result<(&[u8], &[u8])> {
    let (len, rest) = split_varint(buf)?;
    let len = len as usize;
    if rest.len() < len {
        return Err(eof_err!("Unexpected EOF"));
    }
    Ok(rest.split_at(len))
}

/// Splits one stored whole number off the front of `buf`, returning the
/// number and the bytes after it.
///
/// Numbers below 128 take a single byte, which covers nearly every length
/// and count in a page index, so that case is handled right here. Longer
/// numbers use the normal reader.
#[inline(always)]
fn split_varint(buf: &[u8]) -> Result<(u64, &[u8])> {
    match buf.split_first() {
        Some((&n, rest)) if n < 0x80 => Ok((n as u64, rest)),
        Some(_) => {
            let mut prot = ThriftSliceInputProtocol::new(buf);
            Ok((prot.read_vlq()?, prot.as_slice()))
        }
        None => Err(eof_err!("Unexpected EOF")),
    }
}

/// The end position of the byte data, as stored in the offsets list.
fn bytes_end(values: &[u8]) -> Result<i32> {
    i32::try_from(values.len()).map_err(|_| {
        general_err!(
            "ColumnIndex min/max values are larger than {} bytes",
            i32::MAX
        )
    })
}

/// Takes the first `N` bytes of `bytes`.
///
/// This behaves like the older decoder: extra bytes are ignored and too few
/// bytes is an error, with the same message.
fn first_bytes<const N: usize>(bytes: &[u8]) -> Result<[u8; N]> {
    match bytes.get(..N) {
        Some(b) => Ok(b.try_into().unwrap()),
        None => Err(general_err!(
            "error converting value, expected {} bytes got {}",
            N,
            bytes.len()
        )),
    }
}

/// An Arrow array holding values in their Parquet physical type.
#[derive(Debug)]
enum PhysicalArray {
    Boolean(BooleanArray),
    Int32(Int32Array),
    Int64(Int64Array),
    Float(Float32Array),
    Double(Float64Array),
    Bytes(BinaryArray),
    /// Only the number of values; see [`PhysicalValues::Int96`]
    Int96(usize),
    /// Decimals stored as bytes, already turned into numbers; see
    /// [`PhysicalValues::Decimal32`]. Precision and scale are added later.
    Decimal32(PrimitiveArray<Decimal32Type>),
    Decimal64(PrimitiveArray<Decimal64Type>),
    Decimal128(PrimitiveArray<Decimal128Type>),
    Decimal256(PrimitiveArray<Decimal256Type>),
}

impl PhysicalArray {
    fn len(&self) -> usize {
        match self {
            Self::Boolean(a) => a.len(),
            Self::Int32(a) => a.len(),
            Self::Int64(a) => a.len(),
            Self::Float(a) => a.len(),
            Self::Double(a) => a.len(),
            Self::Bytes(a) => a.len(),
            Self::Int96(len) => *len,
            Self::Decimal32(a) => a.len(),
            Self::Decimal64(a) => a.len(),
            Self::Decimal128(a) => a.len(),
            Self::Decimal256(a) => a.len(),
        }
    }
}

/// Collects the page statistics of one column, across any number of row
/// groups, while reading the stored `ColumnIndex` bytes.
#[derive(Debug)]
pub(super) struct ColumnIndexDecoder {
    physical_type: PhysicalType,
    /// One entry per page: `true` if the page has a min and max, `false` if
    /// every value in the page is null (or the row group has no index).
    has_min_max: Vec<bool>,
    mins: PhysicalValues,
    maxes: PhysicalValues,
    null_counts: Vec<u64>,
    null_counts_known: BooleanBufferBuilder,
    nan_counts: Vec<u64>,
    nan_counts_known: BooleanBufferBuilder,
}

/// What [`ColumnIndexDecoder`] produced, before the min and max values are
/// turned into the Arrow type the caller asked for.
#[derive(Debug)]
pub(super) struct DecodedPageStatistics {
    physical_type: PhysicalType,
    mins: PhysicalArray,
    maxes: PhysicalArray,
    null_counts: UInt64Array,
    nan_counts: UInt64Array,
}

impl ColumnIndexDecoder {
    /// `data_type` is the Arrow type the statistics will be turned into.
    pub(super) fn new(physical_type: PhysicalType, data_type: &DataType, capacity: usize) -> Self {
        Self {
            physical_type,
            has_min_max: Vec::with_capacity(capacity),
            mins: PhysicalValues::new(physical_type, data_type, capacity),
            maxes: PhysicalValues::new(physical_type, data_type, capacity),
            null_counts: Vec::with_capacity(capacity),
            null_counts_known: BooleanBufferBuilder::new(capacity),
            nan_counts: Vec::with_capacity(capacity),
            nan_counts_known: BooleanBufferBuilder::new(capacity),
        }
    }

    /// Adds `n` pages with no statistics, for a row group that has no index.
    pub(super) fn append_nulls(&mut self, n: usize) {
        self.has_min_max.resize(self.has_min_max.len() + n, false);
        self.mins.append_nulls(n);
        self.maxes.append_nulls(n);
        self.null_counts.resize(self.null_counts.len() + n, 0);
        self.null_counts_known.append_n(n, false);
        self.nan_counts.resize(self.nan_counts.len() + n, 0);
        self.nan_counts_known.append_n(n, false);
    }

    /// Reads one stored `ColumnIndex` (the statistics for one column in one
    /// row group) and adds its pages.
    ///
    /// If this returns an error, the decoder is left half filled and must not
    /// be used again.
    pub(super) fn append(&mut self, data: &[u8]) -> Result<()> {
        let mut prot = ThriftSliceInputProtocol::new(data);

        // Where this row group's pages start in the shared buffers.
        let first_page = self.has_min_max.len();
        let first_null_count = self.null_counts.len();
        let first_nan_count = self.nan_counts.len();

        let mut num_pages: Option<usize> = None;
        let mut num_mins: Option<usize> = None;
        let mut num_maxes: Option<usize> = None;
        let mut has_boundary_order = false;
        let mut has_null_counts = false;
        let mut has_nan_counts = false;
        // Writers put the list of null pages first, so we normally know which
        // pages to skip while reading the mins and maxes. If a file stores the
        // mins or maxes before that list, we hold on to them here and deal
        // with them at the end.
        let mut held_back_mins: Option<Vec<&[u8]>> = None;
        let mut held_back_maxes: Option<Vec<&[u8]>> = None;

        let mut last_field_id = 0i16;
        loop {
            let field = prot.read_field_begin(last_field_id)?;
            if field.field_type == FieldType::Stop {
                break;
            }
            match (field.id, field.field_type) {
                // null_pages
                (1, FieldType::List) => {
                    if num_pages.is_some() {
                        return Err(general_err!("ColumnIndex has more than one null_pages"));
                    }
                    let size = read_list_size(&mut prot, ElementType::Bool)?;
                    // Each item is one byte. `read_list_size` checked they are there.
                    let flags = &prot.as_slice()[..size];
                    self.has_min_max.reserve(size);
                    for &flag in flags {
                        // stored as "is this page all null", kept as the opposite
                        self.has_min_max.push(match flag {
                            0x01 => false,
                            0x00 | 0x02 => true,
                            _ => return Err(general_err!("cannot convert {} into bool", flag)),
                        });
                    }
                    prot.skip_bytes(size)?;
                    num_pages = Some(size);
                }
                // min_values and max_values
                (2 | 3, FieldType::List) => {
                    let is_min = field.id == 2;
                    let seen = if is_min { num_mins } else { num_maxes };
                    if seen.is_some() {
                        return Err(general_err!(
                            "ColumnIndex has more than one {}",
                            if is_min { "min_values" } else { "max_values" }
                        ));
                    }
                    let size = read_list_size(&mut prot, ElementType::Binary)?;
                    match num_pages {
                        Some(pages) if pages == size => {
                            let values = if is_min {
                                &mut self.mins
                            } else {
                                &mut self.maxes
                            };
                            values.read_list(&mut prot, &self.has_min_max[first_page..])?;
                        }
                        // The counts do not match. Step over the values; the
                        // check after the loop reports the error.
                        Some(_) => {
                            for _ in 0..size {
                                prot.skip(FieldType::Binary)?;
                            }
                        }
                        None => {
                            let mut held = Vec::with_capacity(size);
                            for _ in 0..size {
                                held.push(prot.read_bytes()?);
                            }
                            if is_min {
                                held_back_mins = Some(held);
                            } else {
                                held_back_maxes = Some(held);
                            }
                        }
                    }
                    if is_min {
                        num_mins = Some(size);
                    } else {
                        num_maxes = Some(size);
                    }
                }
                // boundary_order: not needed, but read so that a bad value is caught
                (4, FieldType::I32) => {
                    BoundaryOrder::read_thrift(&mut prot)?;
                    has_boundary_order = true;
                }
                // null_counts
                (5, FieldType::List) => {
                    if has_null_counts {
                        return Err(general_err!("ColumnIndex has more than one null_counts"));
                    }
                    read_counts(&mut prot, &mut self.null_counts, "null")?;
                    has_null_counts = true;
                }
                // nan_counts
                (8, FieldType::List) => {
                    if has_nan_counts {
                        return Err(general_err!("ColumnIndex has more than one nan_counts"));
                    }
                    read_counts(&mut prot, &mut self.nan_counts, "NaN")?;
                    has_nan_counts = true;
                }
                // Anything else, including the level histograms (fields 6 and
                // 7) and fields added to the format later, is not needed.
                (_, FieldType::List) => skip_list(&mut prot)?,
                _ => prot.skip(field.field_type)?,
            }
            last_field_id = field.id;
        }

        let Some(len) = num_pages else {
            return Err(general_err!("Required field null_pages is missing"));
        };
        let Some(num_mins) = num_mins else {
            return Err(general_err!("Required field min_values is missing"));
        };
        let Some(num_maxes) = num_maxes else {
            return Err(general_err!("Required field max_values is missing"));
        };
        if !has_boundary_order {
            return Err(general_err!("Required field boundary_order is missing"));
        }
        if num_mins != len || num_maxes != len {
            return Err(general_err!(
                "ColumnIndex min/max length mismatch: expected {len}, got min={num_mins} max={num_maxes}"
            ));
        }
        for (held, values) in [
            (held_back_mins, &mut self.mins),
            (held_back_maxes, &mut self.maxes),
        ] {
            for (i, bytes) in held.into_iter().flatten().enumerate() {
                if self.has_min_max[first_page + i] {
                    values.append_value(bytes)?;
                } else {
                    values.append_null();
                }
            }
        }

        finish_counts(
            &mut self.null_counts,
            &mut self.null_counts_known,
            first_null_count,
            has_null_counts,
            len,
            "null_counts",
        )?;
        finish_counts(
            &mut self.nan_counts,
            &mut self.nan_counts_known,
            first_nan_count,
            has_nan_counts,
            len,
            "nan_counts",
        )?;
        Ok(())
    }

    pub(super) fn finish(self) -> DecodedPageStatistics {
        // `collect_bool` packs 64 pages at a time, which is much faster than
        // converting the list one page at a time
        let has_min_max = &self.has_min_max;
        let nulls = NullBuffer::new(BooleanBuffer::collect_bool(has_min_max.len(), |i| {
            has_min_max[i]
        }));
        let nulls = (nulls.null_count() > 0).then_some(nulls);
        DecodedPageStatistics {
            physical_type: self.physical_type,
            mins: self.mins.finish(nulls.clone()),
            maxes: self.maxes.finish(nulls),
            null_counts: counts_array(self.null_counts, self.null_counts_known),
            nan_counts: counts_array(self.nan_counts, self.nan_counts_known),
        }
    }
}

/// Reads the start of a list and checks that it holds the expected kind of item.
fn read_list_size(prot: &mut ThriftSliceInputProtocol, expected: ElementType) -> Result<usize> {
    let list = prot.read_list_begin()?;
    let size = list.size as usize;
    // Some writers mark an empty list with a made up item kind, so only
    // check the kind when there are items.
    if size > 0 {
        validate_list_type(expected, &list)?;
    }
    // Every item takes at least one byte, so a list claiming more items than
    // there are bytes left is broken. Checking this first avoids reserving a
    // huge amount of memory for a bad file.
    let remaining = prot.as_slice().len();
    if size > remaining {
        return Err(general_err!(
            "Thrift list size {size} exceeds remaining input length {remaining}"
        ));
    }
    Ok(size)
}

/// Steps over a list we do not need.
///
/// The level histograms are long lists of whole numbers, one or more per
/// page. The general purpose skip looks at each number on its own, which
/// made skipping them take a quarter of the total time. Whole numbers are
/// stored so that every byte except the last has its top bit set, so here we
/// just count the bytes whose top bit is clear.
fn skip_list(prot: &mut ThriftSliceInputProtocol) -> Result<()> {
    let list = prot.read_list_begin()?;
    match list.element_type {
        ElementType::I16 | ElementType::I32 | ElementType::I64 => {
            let count = list.size as usize;
            let buf = prot.as_slice();
            let mut seen = 0;
            let mut len = 0;
            while seen < count {
                let byte = *buf.get(len).ok_or_else(|| eof_err!("Unexpected EOF"))?;
                len += 1;
                seen += usize::from(byte & 0x80 == 0);
            }
            prot.skip_bytes(len)?;
        }
        element_type => {
            let element_type = FieldType::from(element_type);
            for _ in 0..list.size {
                prot.skip(element_type)?;
            }
        }
    }
    Ok(())
}

/// Reads a list of null or NaN counts, refusing negative numbers.
fn read_counts(
    prot: &mut ThriftSliceInputProtocol,
    counts: &mut Vec<u64>,
    what: &str,
) -> Result<()> {
    let size = read_list_size(prot, ElementType::I64)?;
    let mut buf = prot.as_slice();
    counts.reserve(size);
    for _ in 0..size {
        // Numbers are stored so that small values of either sign are short:
        // 0, -1, 1, -2, 2, ... are stored as 0, 1, 2, 3, 4, ...
        let (stored, rest) = split_varint(buf)?;
        buf = rest;
        let count = (stored >> 1) as i64 ^ -((stored & 1) as i64);
        let count = u64::try_from(count)
            .map_err(|_| general_err!("ColumnIndex {what} count is negative {count}"))?;
        counts.push(count);
    }
    let used = prot.as_slice().len() - buf.len();
    Ok(prot.skip_bytes(used)?)
}

/// Checks that a row group gave one count per page, or fills in "unknown"
/// for every page if it gave none.
fn finish_counts(
    counts: &mut Vec<u64>,
    known: &mut BooleanBufferBuilder,
    first: usize,
    present: bool,
    len: usize,
    name: &str,
) -> Result<()> {
    if present {
        let got = counts.len() - first;
        if got != len {
            return Err(general_err!(
                "ColumnIndex {name} length mismatch: expected {len}, got {got}"
            ));
        }
        known.append_n(len, true);
    } else {
        counts.resize(first + len, 0);
        known.append_n(len, false);
    }
    Ok(())
}

fn counts_array(counts: Vec<u64>, mut known: BooleanBufferBuilder) -> UInt64Array {
    let nulls = NullBuffer::new(known.finish());
    let nulls = (nulls.null_count() > 0).then_some(nulls);
    UInt64Array::new(ScalarBuffer::from(counts), nulls)
}

impl DecodedPageStatistics {
    /// Turns the mins and maxes into `data_type`, and returns them with the
    /// null counts and NaN counts.
    pub(super) fn into_arrow(
        self,
        data_type: &DataType,
    ) -> Result<(ArrayRef, ArrayRef, UInt64Array, UInt64Array)> {
        let mins = physical_to_logical(self.mins, data_type, self.physical_type)?;
        let maxes = physical_to_logical(self.maxes, data_type, self.physical_type)?;
        Ok((mins, maxes, self.null_counts, self.nan_counts))
    }
}

/// Turns values in their Parquet form into the Arrow type the caller wants.
///
/// This must give exactly the same results as `get_data_page_statistics!` in
/// the parent module. In particular, a value that does not fit the wanted
/// type becomes null rather than an error, and a type pair that the old code
/// does not know how to handle gives an array of nulls.
///
/// Where the Parquet form and the Arrow type are the same, the data is reused
/// without copying.
fn physical_to_logical(
    array: PhysicalArray,
    data_type: &DataType,
    physical_type: PhysicalType,
) -> Result<ArrayRef> {
    // Dictionary columns report statistics as their value type.
    if let DataType::Dictionary(_, value_type) = data_type {
        return physical_to_logical(array, value_type, physical_type);
    }

    let len = array.len();
    let array: ArrayRef = match (array, data_type) {
        (PhysicalArray::Boolean(a), DataType::Boolean) => Arc::new(a),

        (PhysicalArray::Int32(a), DataType::Int32) => Arc::new(a),
        (PhysicalArray::Int32(a), DataType::Int8) => {
            Arc::new(a.unary_opt::<_, Int8Type>(|x| i8::try_from(x).ok()))
        }
        (PhysicalArray::Int32(a), DataType::Int16) => {
            Arc::new(a.unary_opt::<_, Int16Type>(|x| i16::try_from(x).ok()))
        }
        (PhysicalArray::Int32(a), DataType::UInt8) => {
            Arc::new(a.unary_opt::<_, UInt8Type>(|x| u8::try_from(x).ok()))
        }
        (PhysicalArray::Int32(a), DataType::UInt16) => {
            Arc::new(a.unary_opt::<_, UInt16Type>(|x| u16::try_from(x).ok()))
        }
        // unsigned values are stored as signed numbers with the same bits
        (PhysicalArray::Int32(a), DataType::UInt32) => {
            Arc::new(a.unary::<_, UInt32Type>(|x| x as u32))
        }
        (PhysicalArray::Int32(a), DataType::Date32) => Arc::new(a.reinterpret_cast::<Date32Type>()),
        // stored as days, wanted as milliseconds
        (PhysicalArray::Int32(a), DataType::Date64) => {
            Arc::new(a.unary::<_, Date64Type>(|x| (x as i64) * 24 * 60 * 60 * 1000))
        }
        (PhysicalArray::Int32(a), DataType::Time32(TimeUnit::Second)) => {
            Arc::new(a.reinterpret_cast::<Time32SecondType>())
        }
        (PhysicalArray::Int32(a), DataType::Time32(TimeUnit::Millisecond)) => {
            Arc::new(a.reinterpret_cast::<Time32MillisecondType>())
        }
        (PhysicalArray::Int32(a), DataType::Decimal32(p, s)) => Arc::new(
            a.reinterpret_cast::<Decimal32Type>()
                .with_precision_and_scale(*p, *s)?,
        ),
        (PhysicalArray::Int32(a), DataType::Decimal64(p, s)) => Arc::new(
            a.unary::<_, Decimal64Type>(|x| x as i64)
                .with_precision_and_scale(*p, *s)?,
        ),
        (PhysicalArray::Int32(a), DataType::Decimal128(p, s)) => Arc::new(
            a.unary::<_, Decimal128Type>(|x| x as i128)
                .with_precision_and_scale(*p, *s)?,
        ),
        (PhysicalArray::Int32(a), DataType::Decimal256(p, s)) => Arc::new(
            a.unary::<_, Decimal256Type>(|x| arrow_buffer::i256::from_i128(x as i128))
                .with_precision_and_scale(*p, *s)?,
        ),

        (PhysicalArray::Int64(a), DataType::Int64) => Arc::new(a),
        (PhysicalArray::Int64(a), DataType::UInt64) => {
            Arc::new(a.unary::<_, UInt64Type>(|x| x as u64))
        }
        (PhysicalArray::Int64(a), DataType::Timestamp(unit, tz)) => match unit {
            TimeUnit::Second => Arc::new(
                a.reinterpret_cast::<TimestampSecondType>()
                    .with_timezone_opt(tz.clone()),
            ),
            TimeUnit::Millisecond => Arc::new(
                a.reinterpret_cast::<TimestampMillisecondType>()
                    .with_timezone_opt(tz.clone()),
            ),
            TimeUnit::Microsecond => Arc::new(
                a.reinterpret_cast::<TimestampMicrosecondType>()
                    .with_timezone_opt(tz.clone()),
            ),
            TimeUnit::Nanosecond => Arc::new(
                a.reinterpret_cast::<TimestampNanosecondType>()
                    .with_timezone_opt(tz.clone()),
            ),
        },
        (PhysicalArray::Int64(a), DataType::Date64) => Arc::new(a.reinterpret_cast::<Date64Type>()),
        (PhysicalArray::Int64(a), DataType::Time64(TimeUnit::Microsecond)) => {
            Arc::new(a.reinterpret_cast::<Time64MicrosecondType>())
        }
        (PhysicalArray::Int64(a), DataType::Time64(TimeUnit::Nanosecond)) => {
            Arc::new(a.reinterpret_cast::<Time64NanosecondType>())
        }
        (PhysicalArray::Int64(a), DataType::Decimal32(p, s)) => Arc::new(
            a.unary_opt::<_, Decimal32Type>(|x| i32::try_from(x).ok())
                .with_precision_and_scale(*p, *s)?,
        ),
        (PhysicalArray::Int64(a), DataType::Decimal64(p, s)) => Arc::new(
            a.reinterpret_cast::<Decimal64Type>()
                .with_precision_and_scale(*p, *s)?,
        ),
        (PhysicalArray::Int64(a), DataType::Decimal128(p, s)) => Arc::new(
            a.unary::<_, Decimal128Type>(|x| x as i128)
                .with_precision_and_scale(*p, *s)?,
        ),
        (PhysicalArray::Int64(a), DataType::Decimal256(p, s)) => Arc::new(
            a.unary::<_, Decimal256Type>(|x| arrow_buffer::i256::from_i128(x as i128))
                .with_precision_and_scale(*p, *s)?,
        ),

        (PhysicalArray::Float(a), DataType::Float32) => Arc::new(a),
        (PhysicalArray::Double(a), DataType::Float64) => Arc::new(a),

        // Decimals stored as bytes, already turned into numbers while reading.
        // `PhysicalValues::new` only picks these when the wanted type matches.
        (PhysicalArray::Decimal32(a), DataType::Decimal32(p, s)) => {
            Arc::new(a.with_precision_and_scale(*p, *s)?)
        }
        (PhysicalArray::Decimal64(a), DataType::Decimal64(p, s)) => {
            Arc::new(a.with_precision_and_scale(*p, *s)?)
        }
        (PhysicalArray::Decimal128(a), DataType::Decimal128(p, s)) => {
            Arc::new(a.with_precision_and_scale(*p, *s)?)
        }
        (PhysicalArray::Decimal256(a), DataType::Decimal256(p, s)) => {
            Arc::new(a.with_precision_and_scale(*p, *s)?)
        }

        // Text and binary types only come from variable length byte columns.
        (PhysicalArray::Bytes(a), DataType::Binary)
            if physical_type == PhysicalType::BYTE_ARRAY =>
        {
            Arc::new(a)
        }
        (PhysicalArray::Bytes(a), DataType::LargeBinary)
            if physical_type == PhysicalType::BYTE_ARRAY =>
        {
            Arc::new(a.iter().collect::<LargeBinaryArray>())
        }
        (PhysicalArray::Bytes(a), DataType::BinaryView)
            if physical_type == PhysicalType::BYTE_ARRAY =>
        {
            Arc::new(a.iter().collect::<BinaryViewArray>())
        }
        (PhysicalArray::Bytes(a), DataType::Utf8) if physical_type == PhysicalType::BYTE_ARRAY => {
            Arc::new(binary_to_utf8(a))
        }
        (PhysicalArray::Bytes(a), DataType::LargeUtf8)
            if physical_type == PhysicalType::BYTE_ARRAY =>
        {
            Arc::new(utf8_values(&a).collect::<LargeStringArray>())
        }
        (PhysicalArray::Bytes(a), DataType::Utf8View)
            if physical_type == PhysicalType::BYTE_ARRAY =>
        {
            Arc::new(utf8_values(&a).collect::<StringViewArray>())
        }

        // Fixed length types only come from fixed length byte columns. A
        // value of the wrong length becomes null.
        (PhysicalArray::Bytes(a), DataType::Float16)
            if physical_type == PhysicalType::FIXED_LEN_BYTE_ARRAY =>
        {
            Arc::new(
                a.iter()
                    .map(|v| v.and_then(from_bytes_to_f16))
                    .collect::<Float16Array>(),
            )
        }
        (PhysicalArray::Bytes(a), DataType::FixedSizeBinary(size))
            if physical_type == PhysicalType::FIXED_LEN_BYTE_ARRAY =>
        {
            let values = a.iter().map(|v| v.filter(|v| v.len() == *size as usize));
            Arc::new(FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                values, *size,
            )?)
        }

        // Every other pair gives nulls, as the old code does.
        _ => new_null_array(data_type, len),
    };
    Ok(array)
}

/// Each value as text, or null if it is not valid UTF-8.
fn utf8_values(a: &BinaryArray) -> impl Iterator<Item = Option<&str>> {
    a.iter().map(|v| std::str::from_utf8(v?).ok())
}

/// Turns bytes into text. When every value is valid UTF-8 (the usual case)
/// the data is reused without copying; otherwise bad values become null.
fn binary_to_utf8(a: BinaryArray) -> StringArray {
    match StringArray::try_from_binary(a.clone()) {
        Ok(strings) => strings,
        Err(_) => utf8_values(&a).collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::super::{
        DataPageStatistics, StatisticsConverter, max_page_statistics, min_page_statistics,
        nan_counts_page_statistics, null_counts_page_statistics,
    };
    use crate::basic::Type as PhysicalType;
    use crate::errors::Result;
    use crate::file::page_index::index_reader::decode_column_index;
    use crate::parquet_thrift::{ElementType, FieldType, ThriftCompactOutputProtocol};
    use crate::schema::types::{SchemaDescriptor, Type as SchemaType};
    use arrow_array::{Array, BinaryArray, Int32Array, StringArray, UInt64Array, new_null_array};
    use arrow_schema::{DataType, Field, TimeUnit};
    use std::sync::Arc;

    /// The parts of a `ColumnIndex` to write in a test. A field set to `None`
    /// is left out of the bytes.
    #[derive(Debug, Clone, Default)]
    struct TestIndex {
        null_pages: Option<Vec<bool>>,
        mins: Option<Vec<Vec<u8>>>,
        maxes: Option<Vec<Vec<u8>>>,
        boundary_order: Option<i32>,
        null_counts: Option<Vec<i64>>,
        nan_counts: Option<Vec<i64>>,
        /// Also write the level histograms (fields 6 and 7)
        histograms: bool,
        /// Also write a field that does not exist (yet) in the format
        unknown_field: bool,
        /// Write the mins and maxes before the null pages
        mins_first: bool,
    }

    impl TestIndex {
        /// A full index. A page whose min is `None` is marked as all null.
        fn new(mins: &[Option<&[u8]>], maxes: &[Option<&[u8]>]) -> Self {
            Self {
                null_pages: Some(mins.iter().map(|m| m.is_none()).collect()),
                mins: Some(mins.iter().map(|m| m.unwrap_or(&[]).to_vec()).collect()),
                maxes: Some(maxes.iter().map(|m| m.unwrap_or(&[]).to_vec()).collect()),
                boundary_order: Some(0),
                ..Default::default()
            }
        }

        fn to_bytes(&self) -> Vec<u8> {
            let mut buf = Vec::new();
            let mut w = ThriftCompactOutputProtocol::new(&mut buf);
            let mut last = 0i16;
            let write_bytes_list =
                |w: &mut ThriftCompactOutputProtocol<_>, id, last, values: &[Vec<u8>]| {
                    w.write_field_begin(FieldType::List, id, last).unwrap();
                    w.write_list_begin(ElementType::Binary, values.len())
                        .unwrap();
                    for v in values {
                        w.write_bytes(v).unwrap();
                    }
                };
            let write_i64_list =
                |w: &mut ThriftCompactOutputProtocol<_>, id, last, values: &[i64]| {
                    w.write_field_begin(FieldType::List, id, last).unwrap();
                    w.write_list_begin(ElementType::I64, values.len()).unwrap();
                    for v in values {
                        w.write_i64(*v).unwrap();
                    }
                };

            if self.mins_first {
                if let Some(mins) = &self.mins {
                    write_bytes_list(&mut w, 2, last, mins);
                    last = 2;
                }
                if let Some(maxes) = &self.maxes {
                    write_bytes_list(&mut w, 3, last, maxes);
                    last = 3;
                }
            }
            if let Some(null_pages) = &self.null_pages {
                w.write_field_begin(FieldType::List, 1, last).unwrap();
                w.write_list_begin(ElementType::Bool, null_pages.len())
                    .unwrap();
                for v in null_pages {
                    w.write_bool(*v).unwrap();
                }
                last = 1;
            }
            if !self.mins_first {
                if let Some(mins) = &self.mins {
                    write_bytes_list(&mut w, 2, last, mins);
                    last = 2;
                }
                if let Some(maxes) = &self.maxes {
                    write_bytes_list(&mut w, 3, last, maxes);
                    last = 3;
                }
            }
            if let Some(order) = self.boundary_order {
                w.write_field_begin(FieldType::I32, 4, last).unwrap();
                w.write_i32(order).unwrap();
                last = 4;
            }
            if let Some(counts) = &self.null_counts {
                write_i64_list(&mut w, 5, last, counts);
                last = 5;
            }
            if self.histograms {
                let pages = self.null_pages.as_ref().map_or(0, |p| p.len());
                write_i64_list(&mut w, 6, last, &vec![1; pages * 2]);
                write_i64_list(&mut w, 7, 6, &vec![2; pages * 2]);
                last = 7;
            }
            if let Some(counts) = &self.nan_counts {
                write_i64_list(&mut w, 8, last, counts);
                last = 8;
            }
            if self.unknown_field {
                w.write_field_begin(FieldType::I32, 20, last).unwrap();
                w.write_i32(42).unwrap();
            }
            w.write_struct_end().unwrap();
            buf
        }
    }

    /// A one column Parquet schema, plus a matching Arrow field.
    fn schema(physical_type: PhysicalType, type_length: i32) -> SchemaDescriptor {
        let column = SchemaType::primitive_type_builder("col", physical_type)
            .with_length(type_length)
            .build()
            .unwrap();
        let root = SchemaType::group_type_builder("schema")
            .with_fields(vec![Arc::new(column)])
            .build()
            .unwrap();
        SchemaDescriptor::new(Arc::new(root))
    }

    /// Reads the statistics with the new direct route.
    fn from_bytes(
        physical_type: PhysicalType,
        data_type: &DataType,
        chunks: &[(usize, Option<&[u8]>)],
    ) -> Result<DataPageStatistics> {
        let schema = schema(physical_type, 2);
        let field = Field::new("col", data_type.clone(), true);
        let converter = StatisticsConverter::from_column_index(0, &field, &schema)?;
        converter.data_page_statistics_from_bytes(chunks.iter().copied())
    }

    /// Reads the statistics with the older route, which builds a
    /// `ColumnIndexMetaData` first.
    fn via_metadata(
        physical_type: PhysicalType,
        data_type: &DataType,
        chunks: &[(usize, Option<&[u8]>)],
    ) -> Result<DataPageStatistics> {
        let decoded = chunks
            .iter()
            .map(|(num_pages, bytes)| {
                let index = bytes
                    .map(|bytes| decode_column_index(bytes, physical_type))
                    .transpose()?;
                Ok((*num_pages, index))
            })
            .collect::<Result<Vec<_>>>()?;
        let iter = || decoded.iter().map(|(n, index)| (*n, index.as_ref()));
        let physical_type = Some(physical_type);

        Ok(DataPageStatistics {
            mins: min_page_statistics(data_type, iter(), physical_type)?,
            maxes: max_page_statistics(data_type, iter(), physical_type)?,
            null_counts: null_counts_page_statistics(iter())?,
            nan_counts: nan_counts_page_statistics(iter())?,
        })
    }

    /// Checks that both routes give the same answer.
    fn assert_same(
        physical_type: PhysicalType,
        data_type: &DataType,
        chunks: &[(usize, Option<&[u8]>)],
    ) -> DataPageStatistics {
        let new = from_bytes(physical_type, data_type, chunks).unwrap();
        let old = via_metadata(physical_type, data_type, chunks).unwrap();
        let context = format!("{physical_type:?} as {data_type}");
        assert_eq!(new.mins.data_type(), old.mins.data_type(), "{context}");
        assert_eq!(&new.mins, &old.mins, "mins of {context}");
        assert_eq!(&new.maxes, &old.maxes, "maxes of {context}");
        assert_eq!(new.null_counts, old.null_counts, "null counts of {context}");
        assert_eq!(new.nan_counts, old.nan_counts, "NaN counts of {context}");
        new
    }

    fn le_i32(values: &[Option<i32>]) -> Vec<Option<Vec<u8>>> {
        values
            .iter()
            .map(|v| v.map(|v| v.to_le_bytes().to_vec()))
            .collect()
    }

    fn le_i64(values: &[Option<i64>]) -> Vec<Option<Vec<u8>>> {
        values
            .iter()
            .map(|v| v.map(|v| v.to_le_bytes().to_vec()))
            .collect()
    }

    /// Builds an index whose min and max are the same for each page.
    fn index_of(values: &[Option<Vec<u8>>]) -> TestIndex {
        let values: Vec<Option<&[u8]>> = values.iter().map(|v| v.as_deref()).collect();
        let mut index = TestIndex::new(&values, &values);
        index.null_counts = Some((0..values.len() as i64).collect());
        index
    }

    fn all_data_types() -> Vec<DataType> {
        vec![
            DataType::Boolean,
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
            DataType::Float16,
            DataType::Float32,
            DataType::Float64,
            DataType::Date32,
            DataType::Date64,
            DataType::Time32(TimeUnit::Second),
            DataType::Time32(TimeUnit::Millisecond),
            DataType::Time64(TimeUnit::Microsecond),
            DataType::Time64(TimeUnit::Nanosecond),
            DataType::Timestamp(TimeUnit::Second, None),
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Timestamp(TimeUnit::Nanosecond, Some("+01:00".into())),
            DataType::Decimal32(9, 2),
            DataType::Decimal64(18, 2),
            DataType::Decimal128(38, 2),
            DataType::Decimal256(76, 2),
            DataType::Binary,
            DataType::LargeBinary,
            DataType::BinaryView,
            DataType::Utf8,
            DataType::LargeUtf8,
            DataType::Utf8View,
            DataType::FixedSizeBinary(2),
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Int16)),
            DataType::Dictionary(
                Box::new(DataType::Int32),
                Box::new(DataType::Decimal128(38, 2)),
            ),
            DataType::Duration(TimeUnit::Second),
            DataType::Null,
        ]
    }

    /// Every pair of Parquet type and Arrow type must give the same result
    /// as the older route, including values that do not fit and pairs that
    /// do not go together.
    #[test]
    fn same_as_metadata_route_for_all_types() {
        let int32 = le_i32(&[
            Some(i32::MIN),
            Some(-1),
            None,
            Some(0),
            Some(200),
            Some(i32::MAX),
        ]);
        let int64 = le_i64(&[
            Some(i64::MIN),
            Some(-1),
            None,
            Some(i32::MAX as i64 + 1),
            Some(70_000),
            Some(i64::MAX),
        ]);
        let floats: Vec<_> = [Some(-1.5f32), None, Some(f32::NAN), Some(3.0)]
            .iter()
            .map(|v| v.map(|v| v.to_le_bytes().to_vec()))
            .collect();
        let doubles: Vec<_> = [Some(-1.5f64), None, Some(f64::INFINITY)]
            .iter()
            .map(|v| v.map(|v| v.to_le_bytes().to_vec()))
            .collect();
        let bools = vec![Some(vec![0u8]), None, Some(vec![1u8])];
        // Every value is 1 to 4 bytes long: empty or longer values make the
        // shared decimal helper panic for Decimal32, on both routes.
        let bytes = vec![
            Some(b"abc".to_vec()),
            None,
            Some(vec![0xff, 0xfe]),    // not valid UTF-8
            Some(vec![0x80, 0, 0, 1]), // a negative decimal
        ];
        let fixed = vec![
            Some(vec![0x3c, 0x00]),
            None,
            Some(vec![0xff]), // shorter than the column's length
            Some(vec![0x80, 0x01]),
        ];

        let cases = [
            (PhysicalType::BOOLEAN, bools),
            (PhysicalType::INT32, int32),
            (PhysicalType::INT64, int64),
            (PhysicalType::FLOAT, floats),
            (PhysicalType::DOUBLE, doubles),
            (PhysicalType::BYTE_ARRAY, bytes),
            (PhysicalType::FIXED_LEN_BYTE_ARRAY, fixed),
        ];
        for (physical_type, values) in cases {
            let index = index_of(&values);
            let mut nan_index = index.clone();
            nan_index.nan_counts = Some(vec![1; values.len()]);
            let bytes = index.to_bytes();
            let nan_bytes = nan_index.to_bytes();
            // three row groups: a normal one, one with no index, one with NaN counts
            let chunks = [
                (values.len(), Some(bytes.as_slice())),
                (3, None),
                (values.len(), Some(nan_bytes.as_slice())),
            ];
            for data_type in all_data_types() {
                assert_same(physical_type, &data_type, &chunks);
            }
        }
    }

    #[test]
    fn int96_mins_and_maxes_are_null() {
        let int96 = vec![Some(vec![1u8; 12]), None];
        let bytes = index_of(&int96).to_bytes();
        let data_type = DataType::Timestamp(TimeUnit::Nanosecond, None);
        let chunks = [(2, Some(bytes.as_slice())), (1, None)];
        let stats = assert_same(PhysicalType::INT96, &data_type, &chunks);
        assert_eq!(
            stats.null_counts,
            UInt64Array::from(vec![Some(0), Some(1), None])
        );
    }

    #[test]
    fn int96_value_too_short() {
        let short: &[u8] = &[1, 2, 3];
        let bytes = TestIndex::new(&[Some(short)], &[Some(short)]).to_bytes();
        let data_type = DataType::Timestamp(TimeUnit::Nanosecond, None);
        let chunks = [(1, Some(bytes.as_slice()))];
        let new = from_bytes(PhysicalType::INT96, &data_type, &chunks).unwrap_err();
        let old = via_metadata(PhysicalType::INT96, &data_type, &chunks).unwrap_err();
        assert_eq!(new.to_string(), old.to_string());
    }

    #[test]
    fn values_and_nulls() {
        let values = le_i32(&[Some(5), None, Some(-3)]);
        let values: Vec<_> = values.iter().map(|v| v.as_deref()).collect();
        let maxes = le_i32(&[Some(9), None, Some(0)]);
        let maxes: Vec<_> = maxes.iter().map(|v| v.as_deref()).collect();
        let mut index = TestIndex::new(&values, &maxes);
        index.null_counts = Some(vec![0, 7, 1]);
        let bytes = index.to_bytes();

        let stats = from_bytes(
            PhysicalType::INT32,
            &DataType::Int32,
            &[(3, Some(&bytes)), (2, None)],
        )
        .unwrap();
        let expected_mins = Int32Array::from(vec![Some(5), None, Some(-3), None, None]);
        let expected_maxes = Int32Array::from(vec![Some(9), None, Some(0), None, None]);
        assert_eq!(stats.mins.as_ref(), &expected_mins as &dyn Array);
        assert_eq!(stats.maxes.as_ref(), &expected_maxes as &dyn Array);
        assert_eq!(
            stats.null_counts,
            UInt64Array::from(vec![Some(0), Some(7), Some(1), None, None])
        );
        assert_eq!(stats.nan_counts, UInt64Array::new_null(5));
    }

    #[test]
    fn all_pages_null() {
        let mut index = TestIndex::new(&[None, None], &[None, None]);
        index.null_counts = Some(vec![10, 20]);
        let bytes = index.to_bytes();
        let stats = assert_same(
            PhysicalType::BYTE_ARRAY,
            &DataType::Utf8,
            &[(2, Some(&bytes))],
        );
        assert_eq!(stats.mins.null_count(), 2);
        assert_eq!(stats.null_counts, UInt64Array::from(vec![10, 20]));
    }

    #[test]
    fn zero_pages() {
        let bytes = TestIndex::new(&[], &[]).to_bytes();
        let stats = assert_same(PhysicalType::INT64, &DataType::Int64, &[(0, Some(&bytes))]);
        assert_eq!(stats.mins.len(), 0);
        assert_eq!(stats.null_counts.len(), 0);
    }

    #[test]
    fn no_row_groups() {
        let stats = from_bytes(PhysicalType::INT32, &DataType::Int32, &[]).unwrap();
        assert_eq!(stats.mins.len(), 0);
        assert_eq!(stats.maxes.len(), 0);
    }

    #[test]
    fn column_not_in_file() {
        // "other" is in the Arrow schema but not in the Parquet file
        let arrow_schema = arrow_schema::Schema::new(vec![
            Field::new("col", DataType::Int32, true),
            Field::new("other", DataType::Utf8, true),
        ]);
        let parquet_schema = schema(PhysicalType::INT32, 0);
        let converter =
            StatisticsConverter::try_new("other", &arrow_schema, &parquet_schema).unwrap();
        assert_eq!(converter.parquet_column_index(), None);

        let bytes = TestIndex::new(&[None], &[None]).to_bytes();
        let stats = converter
            .data_page_statistics_from_bytes([(2, None), (1, Some(bytes.as_slice()))])
            .unwrap();
        assert_eq!(&stats.mins, &new_null_array(&DataType::Utf8, 3));
        assert_eq!(&stats.maxes, &new_null_array(&DataType::Utf8, 3));
        assert_eq!(stats.null_counts, UInt64Array::new_null(3));
        assert_eq!(stats.nan_counts, UInt64Array::new_null(3));
    }

    #[test]
    fn invalid_utf8_becomes_null() {
        let good: &[u8] = b"ok";
        let bad: &[u8] = &[0xc3, 0x28];
        let bytes = TestIndex::new(&[Some(good), Some(bad)], &[Some(bad), Some(good)]).to_bytes();
        let chunks = [(2, Some(bytes.as_slice()))];

        let stats = assert_same(PhysicalType::BYTE_ARRAY, &DataType::Utf8, &chunks);
        let expected = StringArray::from(vec![Some("ok"), None]);
        assert_eq!(stats.mins.as_ref(), &expected as &dyn Array);

        // as plain bytes nothing is lost
        let stats = assert_same(PhysicalType::BYTE_ARRAY, &DataType::Binary, &chunks);
        let expected = BinaryArray::from(vec![good, bad]);
        assert_eq!(stats.mins.as_ref(), &expected as &dyn Array);
    }

    #[test]
    fn unknown_fields_are_skipped() {
        let values = le_i32(&[Some(1), Some(2)]);
        let values: Vec<_> = values.iter().map(|v| v.as_deref()).collect();
        let mut index = TestIndex::new(&values, &values);
        index.histograms = true;
        index.unknown_field = true;
        index.nan_counts = Some(vec![0, 0]);
        let bytes = index.to_bytes();
        let stats = assert_same(PhysicalType::INT32, &DataType::Int32, &[(2, Some(&bytes))]);
        assert_eq!(stats.nan_counts, UInt64Array::from(vec![0, 0]));
    }

    #[test]
    fn mins_and_maxes_before_null_pages() {
        let values = le_i64(&[Some(1), None, Some(3)]);
        let values: Vec<_> = values.iter().map(|v| v.as_deref()).collect();
        let mut index = TestIndex::new(&values, &values);
        // a null page's stored value can be anything, including too short
        index.mins.as_mut().unwrap()[1] = vec![];
        index.mins_first = true;
        let bytes = index.to_bytes();
        let stats = assert_same(PhysicalType::INT64, &DataType::Int64, &[(3, Some(&bytes))]);
        assert_eq!(stats.mins.null_count(), 1);
    }

    fn error_message(physical_type: PhysicalType, index: &TestIndex) -> String {
        let bytes = index.to_bytes();
        from_bytes(physical_type, &DataType::Int32, &[(1, Some(&bytes))])
            .unwrap_err()
            .to_string()
    }

    #[test]
    fn min_max_length_mismatch() {
        let one = 1i32.to_le_bytes();
        let mut index = TestIndex::new(&[Some(&one), Some(&one)], &[Some(&one), Some(&one)]);
        index.mins.as_mut().unwrap().pop();
        assert_eq!(
            error_message(PhysicalType::INT32, &index),
            "Parquet error: ColumnIndex min/max length mismatch: expected 2, got min=1 max=2"
        );
        index.mins_first = true;
        assert_eq!(
            error_message(PhysicalType::INT32, &index),
            "Parquet error: ColumnIndex min/max length mismatch: expected 2, got min=1 max=2"
        );
    }

    #[test]
    fn count_length_mismatch() {
        let one = 1i32.to_le_bytes();
        let mut index = TestIndex::new(&[Some(&one)], &[Some(&one)]);
        index.null_counts = Some(vec![1, 2]);
        assert_eq!(
            error_message(PhysicalType::INT32, &index),
            "Parquet error: ColumnIndex null_counts length mismatch: expected 1, got 2"
        );
        index.null_counts = None;
        index.nan_counts = Some(vec![]);
        assert_eq!(
            error_message(PhysicalType::INT32, &index),
            "Parquet error: ColumnIndex nan_counts length mismatch: expected 1, got 0"
        );
    }

    #[test]
    fn negative_counts() {
        let one = 1i32.to_le_bytes();
        let mut index = TestIndex::new(&[Some(&one)], &[Some(&one)]);
        index.null_counts = Some(vec![-10]);
        assert_eq!(
            error_message(PhysicalType::INT32, &index),
            "Parquet error: ColumnIndex null count is negative -10"
        );
        index.null_counts = None;
        index.nan_counts = Some(vec![-1]);
        assert_eq!(
            error_message(PhysicalType::INT32, &index),
            "Parquet error: ColumnIndex NaN count is negative -1"
        );
    }

    #[test]
    fn missing_required_fields() {
        let one = 1i32.to_le_bytes();
        let full = TestIndex::new(&[Some(&one)], &[Some(&one)]);
        let cases = [
            (
                TestIndex {
                    null_pages: None,
                    ..full.clone()
                },
                "null_pages",
            ),
            (
                TestIndex {
                    mins: None,
                    ..full.clone()
                },
                "min_values",
            ),
            (
                TestIndex {
                    maxes: None,
                    ..full.clone()
                },
                "max_values",
            ),
            (
                TestIndex {
                    boundary_order: None,
                    ..full.clone()
                },
                "boundary_order",
            ),
        ];
        for (index, name) in cases {
            assert_eq!(
                error_message(PhysicalType::INT32, &index),
                format!("Parquet error: Required field {name} is missing")
            );
        }
    }

    #[test]
    fn value_too_short() {
        let short: &[u8] = &[1, 2];
        let index = TestIndex::new(&[Some(short)], &[Some(short)]);
        assert_eq!(
            error_message(PhysicalType::INT32, &index),
            "Parquet error: error converting value, expected 4 bytes got 2"
        );
    }

    #[test]
    fn bad_boundary_order() {
        let one = 1i32.to_le_bytes();
        let mut index = TestIndex::new(&[Some(&one)], &[Some(&one)]);
        index.boundary_order = Some(7);
        assert_eq!(
            error_message(PhysicalType::INT32, &index),
            "Parquet error: Unexpected BoundaryOrder 7"
        );
    }

    #[test]
    fn truncated_bytes() {
        let one = 1i32.to_le_bytes();
        let bytes = TestIndex::new(&[Some(&one)], &[Some(&one)]).to_bytes();
        for end in 0..bytes.len() {
            let result = from_bytes(
                PhysicalType::INT32,
                &DataType::Int32,
                &[(1, Some(&bytes[..end]))],
            );
            assert!(
                result.is_err(),
                "reading {end} of {} bytes should fail",
                bytes.len()
            );
        }
    }
}
