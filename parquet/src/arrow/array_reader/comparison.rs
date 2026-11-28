// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Evaluating a [`ColumnComparison`] while decoding a FLOAT or DOUBLE column.
//!
//! [`ColumnComparison`]: crate::arrow::arrow_reader::ColumnComparison

use std::any::Any;
use std::sync::Arc;

use arrow_array::{ArrayRef, ArrowNativeTypeOp, BooleanArray};
use arrow_buffer::{BooleanBufferBuilder, NullBuffer};
use arrow_schema::DataType as ArrowType;
use bytes::Bytes;

use crate::arrow::array_reader::{ArrayReader, read_records, skip_records};
use crate::arrow::arrow_reader::ComparisonOp;
use crate::arrow::buffer::bit_util::iter_set_bits_rev;
use crate::arrow::record_reader::GenericRecordReader;
use crate::arrow::record_reader::buffer::ValuesBuffer;
use crate::basic::Encoding;
use crate::column::page::PageIterator;
use crate::column::reader::decoder::{ColumnValueDecoder, ColumnValueDecoderImpl};
use crate::data_type::DataType;
use crate::encodings::alp::AlpFloat;
use crate::encodings::decoding::alp_decoder::AlpPage;
use crate::encodings::decoding::alp_decoder::compare::CompareState;
use crate::errors::{ParquetError, Result};
use crate::schema::types::ColumnDescPtr;

/// Comparison results, one bit per value or, after null padding, per level.
pub(crate) struct ComparisonBits(BooleanBufferBuilder);

impl ValuesBuffer for ComparisonBits {
    fn with_capacity(capacity: usize) -> Self {
        Self(BooleanBufferBuilder::new(capacity))
    }

    fn reserve_exact(&mut self, additional: usize) {
        self.0.reserve(additional)
    }

    /// Bit-level copy of the `Vec<T>` implementation.
    fn pad_nulls(
        &mut self,
        read_offset: usize,
        values_read: usize,
        levels_read: usize,
        valid_mask: &[u8],
    ) -> Result<()> {
        self.0.resize(read_offset + levels_read);
        let values_range = read_offset..read_offset + values_read;
        for (value_pos, level_pos) in values_range.rev().zip(iter_set_bits_rev(valid_mask)) {
            if level_pos <= value_pos {
                break;
            }
            self.0.set_bit(level_pos, self.0.get_bit(value_pos));
        }
        Ok(())
    }
}

/// Decodes `op(value, scalar)` for each non-null value instead of the value.
///
/// ALP pages are compared without decoding most vectors. Other encodings are
/// decoded and then compared.
pub(crate) struct ComparisonValueDecoder<T: DataType>
where
    T::T: AlpFloat,
{
    comparison: Option<(ComparisonOp, T::T)>,
    values: ColumnValueDecoderImpl<T>,
    alp: Option<AlpCursor<T::T>>,
    decoded: Vec<T::T>,
    packed: Vec<u8>,
}

struct AlpCursor<Value: AlpFloat> {
    page: AlpPage<Value>,
    position: usize,
    state: CompareState<Value>,
}

impl<Value: AlpFloat> AlpCursor<Value> {
    /// Advance by up to `n` values, returning the range passed over.
    fn advance(&mut self, n: usize) -> std::ops::Range<usize> {
        let start = self.position;
        self.position = (start + n).min(self.page.num_values());
        start..self.position
    }
}

impl<T> ComparisonValueDecoder<T>
where
    T: DataType,
    T::T: AlpFloat + ArrowNativeTypeOp,
{
    fn set_comparison(&mut self, op: ComparisonOp, scalar: T::T) {
        self.comparison = Some((op, scalar));
    }
}

impl<T> ColumnValueDecoder for ComparisonValueDecoder<T>
where
    T: DataType,
    T::T: AlpFloat + ArrowNativeTypeOp,
{
    type Buffer = ComparisonBits;

    fn new(col: &ColumnDescPtr) -> Self {
        Self {
            comparison: None,
            values: ColumnValueDecoderImpl::new(col),
            alp: None,
            decoded: Vec::new(),
            packed: Vec::new(),
        }
    }

    fn set_dict(
        &mut self,
        buf: Bytes,
        num_values: u32,
        encoding: Encoding,
        is_sorted: bool,
    ) -> Result<()> {
        self.values.set_dict(buf, num_values, encoding, is_sorted)
    }

    fn set_data(
        &mut self,
        encoding: Encoding,
        data: Bytes,
        num_levels: usize,
        num_values: Option<usize>,
    ) -> Result<()> {
        if encoding != Encoding::ALP {
            self.alp = None;
            return self.values.set_data(encoding, data, num_levels, num_values);
        }
        let page = AlpPage::try_new_bounded(data, num_values.unwrap_or(num_levels))?;
        let state = CompareState::new(&page);
        self.alp = Some(AlpCursor {
            page,
            position: 0,
            state,
        });
        Ok(())
    }

    fn read(&mut self, out: &mut ComparisonBits, num_values: usize) -> Result<usize> {
        let (op, scalar) = self
            .comparison
            .ok_or_else(|| general_err!("comparison decoder used without a comparison"))?;
        let start = out.0.len();
        match &mut self.alp {
            Some(cursor) => {
                let range = cursor.advance(num_values);
                cursor
                    .page
                    .compare_range_with(&mut cursor.state, op, scalar, range, &mut out.0)?;
            }
            None => {
                self.decoded.clear();
                self.values.read(&mut self.decoded, num_values)?;
                op.append_all(&self.decoded, scalar, &mut out.0, &mut self.packed);
            }
        }
        Ok(out.0.len() - start)
    }

    fn skip_values(&mut self, num_values: usize) -> Result<usize> {
        match &mut self.alp {
            Some(cursor) => Ok(cursor.advance(num_values).len()),
            None => self.values.skip_values(num_values),
        }
    }
}

/// Reads a FLOAT or DOUBLE column as the [`BooleanArray`] of a comparison with
/// a scalar, with the column's nulls.
pub(crate) struct ComparisonArrayReader<T>
where
    T: DataType,
    T::T: AlpFloat + ArrowNativeTypeOp,
{
    pages: Box<dyn PageIterator>,
    def_levels_buffer: Option<Vec<i16>>,
    rep_levels_buffer: Option<Vec<i16>>,
    record_reader: GenericRecordReader<ComparisonBits, ComparisonValueDecoder<T>>,
}

impl<T> ComparisonArrayReader<T>
where
    T: DataType,
    T::T: AlpFloat + ArrowNativeTypeOp,
{
    pub(crate) fn new(
        pages: Box<dyn PageIterator>,
        column_desc: ColumnDescPtr,
        batch_size: usize,
        op: ComparisonOp,
        scalar: T::T,
    ) -> Self {
        let mut record_reader = GenericRecordReader::new(column_desc, batch_size);
        record_reader.set_configure_values_decoder(
            move |decoder: &mut ComparisonValueDecoder<T>| decoder.set_comparison(op, scalar),
        );
        Self {
            pages,
            def_levels_buffer: None,
            rep_levels_buffer: None,
            record_reader,
        }
    }
}

impl<T> ArrayReader for ComparisonArrayReader<T>
where
    T: DataType,
    T::T: AlpFloat + ArrowNativeTypeOp,
    <T::T as AlpFloat>::Exact: Send,
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_data_type(&self) -> &ArrowType {
        &ArrowType::Boolean
    }

    fn read_records(&mut self, batch_size: usize) -> Result<usize> {
        read_records(&mut self.record_reader, self.pages.as_mut(), batch_size)
    }

    fn consume_batch(&mut self) -> Result<ArrayRef> {
        let len = self.record_reader.values_written();
        let values = self.record_reader.consume_record_data().0.finish();
        // Null slots hold arbitrary values, so the nulls must stay attached.
        let nulls = self
            .record_reader
            .consume_compact_bitmap()
            .and_then(|bitmap| NullBuffer::from_unsliced_buffer(bitmap, len));
        let array = BooleanArray::new(values.slice(0, len), nulls);

        self.def_levels_buffer = self.record_reader.consume_def_levels();
        self.rep_levels_buffer = self.record_reader.consume_rep_levels();
        self.record_reader.reset();
        Ok(Arc::new(array))
    }

    fn skip_records(&mut self, num_records: usize) -> Result<usize> {
        skip_records(&mut self.record_reader, self.pages.as_mut(), num_records)
    }

    fn get_def_levels(&self) -> Option<&[i16]> {
        self.def_levels_buffer.as_deref()
    }

    fn get_rep_levels(&self) -> Option<&[i16]> {
        self.rep_levels_buffer.as_deref()
    }

    fn max_def_level(&self) -> i16 {
        self.record_reader.max_def_level()
    }
}
