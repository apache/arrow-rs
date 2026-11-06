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
//! Only `INT32` columns are supported.
//!
//! [`ColumnIndexMetaData`]: crate::file::page_index::column_index::ColumnIndexMetaData

use super::DataPageStatistics;
use crate::basic::BoundaryOrder;
use crate::errors::{ParquetError, Result};
use crate::parquet_thrift::{
    ElementType, FieldType, ReadThrift, ThriftCompactInputProtocol, ThriftSliceInputProtocol,
    validate_list_type,
};
use arrow_array::types::{
    Date32Type, Date64Type, Decimal32Type, Decimal64Type, Decimal128Type, Decimal256Type, Int8Type,
    Int16Type, Time32MillisecondType, Time32SecondType, UInt8Type, UInt16Type, UInt32Type,
};
use arrow_array::{ArrayRef, Int32Array, UInt64Array, new_null_array};
use arrow_buffer::{BooleanBuffer, BooleanBufferBuilder, NullBuffer, ScalarBuffer, i256};
use arrow_schema::{DataType, TimeUnit};
use std::sync::Arc;

/// Adds `n` default values to `v`.
fn pad<T: Default + Clone>(v: &mut Vec<T>, n: usize) {
    v.resize(v.len() + n, T::default());
}

/// Reads one `INT32` value per page from the list of binary values at the
/// start of `buf`, and returns how many bytes the list took.
///
/// `has_min_max` holds one entry for each page in the list. Pages marked
/// `false` get a filler value, and their stored bytes are not looked at.
#[inline(never)]
fn read_list(mut buf: &[u8], out: &mut Vec<i32>, has_min_max: &[bool]) -> Result<usize> {
    // `buf` is a local copy of the unread bytes, so the compiler can keep
    // the read position in a register rather than in memory. This makes
    // this loop much faster.
    let start = buf.len();
    // Fill in filler values first and then overwrite them, which saves
    // checking the capacity for every value.
    let first = out.len();
    out.resize(first + has_min_max.len(), 0);
    for (slot, &has_value) in out[first..].iter_mut().zip(has_min_max) {
        // Writers store each value as a one byte length of 4 followed by
        // the 4 bytes, so handle that directly. Anything else, such as the
        // empty value of a null page, takes the general route below.
        if has_value
            && let Some((&4, rest)) = buf.split_first()
            && let Some((value, rest)) = rest.split_first_chunk::<4>()
        {
            *slot = i32::from_le_bytes(*value);
            buf = rest;
            continue;
        }
        let (bytes, rest) = split_binary(buf)?;
        buf = rest;
        if has_value {
            *slot = i32::from_le_bytes(first_bytes(bytes)?);
        }
    }
    Ok(start - buf.len())
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

/// The null or NaN counts of every page.
#[derive(Debug)]
struct Counts {
    values: Vec<u64>,
    /// One entry per page: `true` if its row group stored these counts.
    known: BooleanBufferBuilder,
}

impl Counts {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            values: Vec::with_capacity(capacity),
            known: BooleanBufferBuilder::new(capacity),
        }
    }

    /// Adds `n` pages whose count is not known.
    fn append_nulls(&mut self, n: usize) {
        pad(&mut self.values, n);
        self.known.append_n(n, false);
    }

    /// Reads a list of counts, refusing negative numbers. `what` names the
    /// counts in errors.
    fn read(&mut self, prot: &mut ThriftSliceInputProtocol, what: &str) -> Result<()> {
        let size = read_list_size(prot, ElementType::I64)?;
        let mut buf = prot.as_slice();
        self.values.reserve(size);
        let mut remaining = size;
        while remaining > 0 {
            // Counts from 0 to 63 take one byte each, with the top bit and
            // the sign bit (the lowest) clear. When the next 8 counts are all
            // like that, which is usual, convert them together.
            if remaining >= 8
                && let Some((chunk, rest)) = buf.split_first_chunk::<8>()
                && u64::from_le_bytes(*chunk) & 0x8181_8181_8181_8181 == 0
            {
                self.values.extend(chunk.map(|b| u64::from(b >> 1)));
                buf = rest;
                remaining -= 8;
                continue;
            }
            remaining -= 1;
            // Numbers are stored so that small values of either sign are short:
            // 0, -1, 1, -2, 2, ... are stored as 0, 1, 2, 3, 4, ...
            let (stored, rest) = split_varint(buf)?;
            buf = rest;
            let count = (stored >> 1) as i64 ^ -((stored & 1) as i64);
            let count = u64::try_from(count)
                .map_err(|_| general_err!("ColumnIndex {what} count is negative {count}"))?;
            self.values.push(count);
        }
        let used = prot.as_slice().len() - buf.len();
        Ok(prot.skip_bytes(used)?)
    }

    /// Ends a row group of `len` pages whose counts start at `first`: checks
    /// that it gave one count per page, or marks every page as unknown if it
    /// gave none.
    fn finish_row_group(
        &mut self,
        first: usize,
        present: bool,
        len: usize,
        name: &str,
    ) -> Result<()> {
        if !present {
            self.append_nulls(len);
            return Ok(());
        }
        let got = self.values.len() - first;
        if got != len {
            return Err(general_err!(
                "ColumnIndex {name} length mismatch: expected {len}, got {got}"
            ));
        }
        self.known.append_n(len, true);
        Ok(())
    }

    fn finish(mut self) -> UInt64Array {
        let nulls = NullBuffer::new(self.known.finish());
        let nulls = (nulls.null_count() > 0).then_some(nulls);
        UInt64Array::new(ScalarBuffer::from(self.values), nulls)
    }
}

/// The names of the min and max fields, in the order of
/// [`ColumnIndexDecoder::min_max`].
const MIN_MAX_NAMES: [&str; 2] = ["min_values", "max_values"];

/// Collects the page statistics of one column, across any number of row
/// groups, while reading the stored `ColumnIndex` bytes.
#[derive(Debug)]
pub(super) struct ColumnIndexDecoder {
    /// The Arrow type the mins and maxes are turned into. A dictionary type
    /// is replaced by its value type, as statistics come out as the values.
    data_type: DataType,
    /// One entry per page: `true` if the page has a min and max, `false` if
    /// every value in the page is null (or the row group has no index).
    has_min_max: Vec<bool>,
    /// The mins, then the maxes.
    min_max: [Vec<i32>; 2],
    null_counts: Counts,
    nan_counts: Counts,
}

impl ColumnIndexDecoder {
    /// `data_type` is the Arrow type the statistics will be turned into.
    pub(super) fn new(data_type: &DataType, capacity: usize) -> Self {
        let data_type = match data_type {
            DataType::Dictionary(_, value_type) => value_type.as_ref(),
            data_type => data_type,
        };
        Self {
            data_type: data_type.clone(),
            has_min_max: Vec::with_capacity(capacity),
            min_max: std::array::from_fn(|_| Vec::with_capacity(capacity)),
            null_counts: Counts::with_capacity(capacity),
            nan_counts: Counts::with_capacity(capacity),
        }
    }

    /// Adds `n` pages with no statistics, for a row group that has no index.
    pub(super) fn append_nulls(&mut self, n: usize) {
        self.has_min_max.resize(self.has_min_max.len() + n, false);
        for values in &mut self.min_max {
            pad(values, n);
        }
        self.null_counts.append_nulls(n);
        self.nan_counts.append_nulls(n);
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
        let first_null_count = self.null_counts.values.len();
        let first_nan_count = self.nan_counts.values.len();

        let mut num_pages: Option<usize> = None;
        // The number of mins and maxes, in the order of `self.min_max`.
        let mut num_min_max: [Option<usize>; 2] = [None; 2];
        let mut has_boundary_order = false;
        let mut has_null_counts = false;
        let mut has_nan_counts = false;
        // Writers put the list of null pages first, so we normally know which
        // pages to skip while reading the mins and maxes. If a file stores the
        // mins or maxes before that list, we note where they are and read them
        // at the end.
        let mut deferred: [Option<&[u8]>; 2] = [None; 2];

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
                    // `true` is stored as 1, and `false` as 0 or 2. Checking
                    // all of them first, then converting, lets the compiler
                    // handle many at a time.
                    if flags.iter().fold(0, |max, &flag| max.max(flag)) > 2 {
                        let flag = flags.iter().find(|&&flag| flag > 2).unwrap();
                        return Err(general_err!("cannot convert {} into bool", flag));
                    }
                    // stored as "is this page all null", kept as the opposite
                    self.has_min_max.extend(flags.iter().map(|&flag| flag != 1));
                    prot.skip_bytes(size)?;
                    num_pages = Some(size);
                }
                // min_values and max_values
                (2 | 3, FieldType::List) => {
                    let i = (field.id - 2) as usize;
                    if num_min_max[i].is_some() {
                        return Err(general_err!(
                            "ColumnIndex has more than one {}",
                            MIN_MAX_NAMES[i]
                        ));
                    }
                    let size = read_list_size(&mut prot, ElementType::Binary)?;
                    if num_pages == Some(size) {
                        let has_min_max = &self.has_min_max[first_page..];
                        let used = read_list(prot.as_slice(), &mut self.min_max[i], has_min_max)?;
                        prot.skip_bytes(used)?;
                    } else {
                        // The null pages are not known yet, or the counts do
                        // not match and the check after the loop reports the
                        // error. Step over the values for now.
                        deferred[i] = Some(prot.as_slice());
                        for _ in 0..size {
                            prot.skip(FieldType::Binary)?;
                        }
                    }
                    num_min_max[i] = Some(size);
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
                    self.null_counts.read(&mut prot, "null")?;
                    has_null_counts = true;
                }
                // nan_counts
                (8, FieldType::List) => {
                    if has_nan_counts {
                        return Err(general_err!("ColumnIndex has more than one nan_counts"));
                    }
                    self.nan_counts.read(&mut prot, "NaN")?;
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
        let [Some(num_mins), Some(num_maxes)] = num_min_max else {
            let name = MIN_MAX_NAMES[usize::from(num_min_max[0].is_some())];
            return Err(general_err!("Required field {name} is missing"));
        };
        if !has_boundary_order {
            return Err(general_err!("Required field boundary_order is missing"));
        }
        if num_mins != len || num_maxes != len {
            return Err(general_err!(
                "ColumnIndex min/max length mismatch: expected {len}, got min={num_mins} max={num_maxes}"
            ));
        }
        for (values, list) in self.min_max.iter_mut().zip(deferred) {
            if let Some(list) = list {
                read_list(list, values, &self.has_min_max[first_page..])?;
            }
        }

        self.null_counts
            .finish_row_group(first_null_count, has_null_counts, len, "null_counts")?;
        self.nan_counts
            .finish_row_group(first_nan_count, has_nan_counts, len, "nan_counts")?;
        Ok(())
    }

    /// Returns the statistics, with the mins and maxes turned into the Arrow
    /// type given to [`Self::new`].
    pub(super) fn finish(self) -> Result<DataPageStatistics> {
        // `collect_bool` packs 64 pages at a time, which is much faster than
        // converting the list one page at a time
        let len = self.has_min_max.len();
        let has_min_max = &self.has_min_max;
        let nulls = NullBuffer::new(BooleanBuffer::collect_bool(len, |i| has_min_max[i]));
        let nulls = (nulls.null_count() > 0).then_some(nulls);
        let [mins, maxes] = self.min_max;
        let to_array = |values: Vec<i32>, nulls| {
            let array = Int32Array::new(values.into(), nulls);
            // Every other type gives nulls, as the old code does.
            Ok::<_, ParquetError>(
                int32_to_logical(&array, &self.data_type)?
                    .unwrap_or_else(|| new_null_array(&self.data_type, len)),
            )
        };
        Ok(DataPageStatistics {
            mins: to_array(mins, nulls.clone())?,
            maxes: to_array(maxes, nulls)?,
            null_counts: self.null_counts.finish(),
            nan_counts: self.nan_counts.finish(),
        })
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
            // While at least 8 numbers are left, the next 8 bytes all belong
            // to the list, so count them together.
            while count - seen >= 8
                && let Some(chunk) = buf.get(len..len + 8)
            {
                let word = u64::from_le_bytes(chunk.try_into().unwrap());
                seen += (!word & 0x8080_8080_8080_8080).count_ones() as usize;
                len += 8;
            }
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

fn int32_to_logical(a: &Int32Array, data_type: &DataType) -> Result<Option<ArrayRef>> {
    let array: ArrayRef = match data_type {
        DataType::Int32 => Arc::new(a.clone()),
        DataType::Int8 => Arc::new(a.unary_opt::<_, Int8Type>(|x| i8::try_from(x).ok())),
        DataType::Int16 => Arc::new(a.unary_opt::<_, Int16Type>(|x| i16::try_from(x).ok())),
        DataType::UInt8 => Arc::new(a.unary_opt::<_, UInt8Type>(|x| u8::try_from(x).ok())),
        DataType::UInt16 => Arc::new(a.unary_opt::<_, UInt16Type>(|x| u16::try_from(x).ok())),
        // unsigned values are stored as signed numbers with the same bits
        DataType::UInt32 => Arc::new(a.unary::<_, UInt32Type>(|x| x as u32)),
        DataType::Date32 => Arc::new(a.reinterpret_cast::<Date32Type>()),
        // stored as days, wanted as milliseconds
        DataType::Date64 => {
            Arc::new(a.unary::<_, Date64Type>(|x| (x as i64) * 24 * 60 * 60 * 1000))
        }
        DataType::Time32(TimeUnit::Second) => Arc::new(a.reinterpret_cast::<Time32SecondType>()),
        DataType::Time32(TimeUnit::Millisecond) => {
            Arc::new(a.reinterpret_cast::<Time32MillisecondType>())
        }
        DataType::Decimal32(p, s) => Arc::new(
            a.reinterpret_cast::<Decimal32Type>()
                .with_precision_and_scale(*p, *s)?,
        ),
        DataType::Decimal64(p, s) => Arc::new(
            a.unary::<_, Decimal64Type>(|x| x as i64)
                .with_precision_and_scale(*p, *s)?,
        ),
        DataType::Decimal128(p, s) => Arc::new(
            a.unary::<_, Decimal128Type>(|x| x as i128)
                .with_precision_and_scale(*p, *s)?,
        ),
        DataType::Decimal256(p, s) => Arc::new(
            a.unary::<_, Decimal256Type>(|x| i256::from_i128(x as i128))
                .with_precision_and_scale(*p, *s)?,
        ),
        _ => return Ok(None),
    };
    Ok(Some(array))
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
    use arrow_array::{Array, Int32Array, UInt64Array, new_null_array};
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

            let write_mins_maxes = |w: &mut ThriftCompactOutputProtocol<_>, last: &mut i16| {
                if let Some(mins) = &self.mins {
                    write_bytes_list(w, 2, *last, mins);
                    *last = 2;
                }
                if let Some(maxes) = &self.maxes {
                    write_bytes_list(w, 3, *last, maxes);
                    *last = 3;
                }
            };

            if self.mins_first {
                write_mins_maxes(&mut w, &mut last);
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
                write_mins_maxes(&mut w, &mut last);
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
                // numbers of 1 to 5 bytes, so that skipping them has to
                // find where each one ends
                let pages = self.null_pages.as_ref().map_or(0, |p| p.len());
                let levels: Vec<i64> = (0..pages as i64 * 2).map(|i| i << (i % 5 * 7)).collect();
                write_i64_list(&mut w, 6, last, &levels);
                write_i64_list(&mut w, 7, 6, &levels);
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

    /// Every Arrow type must give the same result as the older route,
    /// including values that do not fit and types that do not go together.
    #[test]
    fn same_as_metadata_route_for_all_types() {
        let values = le_i32(&[
            Some(i32::MIN),
            Some(-1),
            None,
            Some(0),
            Some(200),
            Some(i32::MAX),
        ]);
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
            assert_same(PhysicalType::INT32, &data_type, &chunks);
        }
    }

    #[test]
    fn other_physical_types_are_not_supported() {
        let bytes = TestIndex::new(&[None], &[None]).to_bytes();
        for physical_type in [
            PhysicalType::BOOLEAN,
            PhysicalType::INT64,
            PhysicalType::INT96,
            PhysicalType::FLOAT,
            PhysicalType::DOUBLE,
            PhysicalType::BYTE_ARRAY,
            PhysicalType::FIXED_LEN_BYTE_ARRAY,
        ] {
            let err =
                from_bytes(physical_type, &DataType::Int64, &[(1, Some(&bytes))]).unwrap_err();
            assert_eq!(
                err.to_string(),
                format!(
                    "NYI: Reading page statistics from bytes is only supported for INT32 columns, got {physical_type}"
                )
            );
        }
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
        let stats = assert_same(PhysicalType::INT32, &DataType::Int32, &[(2, Some(&bytes))]);
        assert_eq!(stats.mins.null_count(), 2);
        assert_eq!(stats.null_counts, UInt64Array::from(vec![10, 20]));
    }

    #[test]
    fn zero_pages() {
        let bytes = TestIndex::new(&[], &[]).to_bytes();
        let stats = assert_same(PhysicalType::INT32, &DataType::Int32, &[(0, Some(&bytes))]);
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
    fn long_histograms_are_skipped() {
        // enough numbers that most are skipped 8 bytes at a time
        for pages in [3, 4, 5, 20, 21] {
            let values = le_i32(&(0..pages).map(Some).collect::<Vec<_>>());
            let values: Vec<_> = values.iter().map(|v| v.as_deref()).collect();
            let mut index = TestIndex::new(&values, &values);
            index.histograms = true;
            let nan_counts: Vec<i64> = (0..pages as i64).collect();
            index.nan_counts = Some(nan_counts.clone());
            let bytes = index.to_bytes();
            let chunks = [(pages as usize, Some(bytes.as_slice()))];
            let stats = assert_same(PhysicalType::INT32, &DataType::Int32, &chunks);
            let expected = UInt64Array::from_iter_values(nan_counts.iter().map(|&c| c as u64));
            assert_eq!(stats.nan_counts, expected);
        }
    }

    #[test]
    fn mins_and_maxes_before_null_pages() {
        let values = le_i32(&[Some(1), None, Some(3)]);
        let values: Vec<_> = values.iter().map(|v| v.as_deref()).collect();
        let mut index = TestIndex::new(&values, &values);
        // a null page's stored value can be anything, including too short
        index.mins.as_mut().unwrap()[1] = vec![];
        index.mins_first = true;
        let bytes = index.to_bytes();
        let stats = assert_same(PhysicalType::INT32, &DataType::Int32, &[(3, Some(&bytes))]);
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

        // among other counts that are read 8 at a time
        let values = le_i32(&[Some(1); 12]);
        let values: Vec<_> = values.iter().map(|v| v.as_deref()).collect();
        let mut index = TestIndex::new(&values, &values);
        let mut counts = vec![0; 12];
        counts[5] = -2;
        index.null_counts = Some(counts);
        assert_eq!(
            error_message(PhysicalType::INT32, &index),
            "Parquet error: ColumnIndex null count is negative -2"
        );
    }

    #[test]
    fn long_count_lists() {
        // one byte counts, which are read 8 at a time, mixed with longer ones
        let counts: Vec<i64> = (0..30)
            .map(|i| match i % 11 {
                3 => 63,
                4 => 64,
                7 => 1 << 40,
                _ => i,
            })
            .collect();
        let values = le_i32(&vec![Some(1); counts.len()]);
        let values: Vec<_> = values.iter().map(|v| v.as_deref()).collect();
        let mut index = TestIndex::new(&values, &values);
        index.null_counts = Some(counts.clone());
        index.nan_counts = Some(counts.iter().rev().copied().collect());
        let bytes = index.to_bytes();
        let chunks = [(counts.len(), Some(bytes.as_slice()))];
        let stats = assert_same(PhysicalType::INT32, &DataType::Int32, &chunks);
        let expected = UInt64Array::from_iter_values(counts.iter().map(|&c| c as u64));
        assert_eq!(stats.null_counts, expected);
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
        truncations_fail(&bytes, 1);

        // long histograms, which are skipped several bytes at a time
        let values = le_i32(&(0..10).map(Some).collect::<Vec<_>>());
        let values: Vec<_> = values.iter().map(|v| v.as_deref()).collect();
        let mut index = TestIndex::new(&values, &values);
        index.histograms = true;
        truncations_fail(&index.to_bytes(), 10);
    }

    /// Checks that decoding fails on every shortened copy of `bytes`
    fn truncations_fail(bytes: &[u8], pages: usize) {
        for end in 0..bytes.len() {
            let result = from_bytes(
                PhysicalType::INT32,
                &DataType::Int32,
                &[(pages, Some(&bytes[..end]))],
            );
            assert!(
                result.is_err(),
                "reading {end} of {} bytes should fail",
                bytes.len()
            );
        }
    }
}
