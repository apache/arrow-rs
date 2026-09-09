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

use std::mem::MaybeUninit;

use super::ArrowPhysicalBridge;
use crate::column::value_batch::{BatchSink, ValueProducer};
use crate::column::value_selection::{PhysicalValueSelection, PhysicalValueSpan};
use crate::column::writer::ByteBudgetTarget;
use crate::column::writer::encoder::TypedColumnChunkEncoder;
use crate::column::writer::encoder::{ByteArrayBatch, ByteArraySink, ByteArraySource};
use crate::data_type::ByteArrayType as ParquetByteArrayType;
use crate::errors::{ParquetError, Result};

use arrow_array::cast::AsArray;
use arrow_array::types::{BinaryType, ByteArrayType, LargeBinaryType, LargeUtf8Type, Utf8Type};
use arrow_array::{Array, BinaryViewArray, OffsetSizeTrait, StringViewArray};
use arrow_buffer::Buffer;
use arrow_schema::DataType;

/// Type-specific access to the final physical byte values. Index composition is
/// deliberately absent here: [`PhysicalValueSelection`] owns run and dictionary
/// mapping, leaving one Arrow-layout dispatch for both writing and page
/// budgeting.
trait ByteArrayValueAccess<'a>: Copy {
    /// Whether every contiguous logical range can be lent as a byte batch.
    const SUPPORTS_CONTIGUOUS_BATCHES: bool = false;

    fn value(self, index: usize) -> &'a [u8];

    #[inline]
    fn value_len(self, index: usize) -> usize {
        self.value(index).len()
    }

    /// Exact encoded size of a contiguous range, when the layout has prefix
    /// offsets. Includes Parquet's four-byte PLAIN length prefix per value.
    #[inline]
    fn exact_range_encoded_size(self, _start: usize, _len: usize) -> Option<usize> {
        None
    }

    /// Borrow a contiguous logical range when the layout supports it.
    /// Implementations setting [`Self::SUPPORTS_CONTIGUOUS_BATCHES`] must
    /// return a batch for every in-bounds range.
    #[inline]
    fn contiguous_batch(self, _start: usize, _len: usize) -> Option<ByteArrayBatch<'a, 'a>> {
        None
    }

    /// Conservative encoded-size upper bound for the requested contiguous
    /// range, when one is cheaply available.
    #[inline]
    fn range_encoded_upper_bound(self, _start: usize, _len: usize) -> Option<usize> {
        None
    }

    /// Raw Arrow view descriptors when every value is inline. Other byte
    /// layouts, and views containing indirect values, return `None`.
    #[inline]
    fn inline_views(self) -> Option<&'a [u128]> {
        None
    }
}

/// Access to offset-based byte arrays. Contiguous ranges iterate adjacent
/// offset windows and compute their total encoded size with one subtraction.
#[derive(Clone, Copy)]
struct OffsetByteArrayAccess<'a, O: OffsetSizeTrait> {
    offsets: &'a [O],
    data: &'a [u8],
}

impl<'a, O: OffsetSizeTrait> OffsetByteArrayAccess<'a, O> {
    #[inline]
    fn bind<T: ByteArrayType<Offset = O>>(values: &'a dyn Array) -> Self {
        let values = values.as_bytes::<T>();
        Self {
            offsets: values.value_offsets(),
            data: values.value_data(),
        }
    }
}

trait ByteArrayOffset: OffsetSizeTrait {
    fn batch<'a>(offsets: &'a [Self], data: &'a [u8]) -> ByteArrayBatch<'a, 'a>;
}

impl ByteArrayOffset for i32 {
    #[inline]
    fn batch<'a>(offsets: &'a [Self], data: &'a [u8]) -> ByteArrayBatch<'a, 'a> {
        ByteArrayBatch::Offset32 { offsets, data }
    }
}

impl ByteArrayOffset for i64 {
    #[inline]
    fn batch<'a>(offsets: &'a [Self], data: &'a [u8]) -> ByteArrayBatch<'a, 'a> {
        ByteArrayBatch::Offset64 { offsets, data }
    }
}

impl<'a, O: ByteArrayOffset> ByteArrayValueAccess<'a> for OffsetByteArrayAccess<'a, O> {
    const SUPPORTS_CONTIGUOUS_BATCHES: bool = true;

    #[inline]
    fn value(self, index: usize) -> &'a [u8] {
        let start = self.offsets[index].as_usize();
        &self.data[start..self.offsets[index + 1].as_usize()]
    }

    #[inline]
    fn value_len(self, index: usize) -> usize {
        (self.offsets[index + 1] - self.offsets[index]).as_usize()
    }

    #[inline]
    fn exact_range_encoded_size(self, start: usize, len: usize) -> Option<usize> {
        let payload = (self.offsets[start + len] - self.offsets[start]).as_usize();
        Some(payload.saturating_add(len.saturating_mul(std::mem::size_of::<u32>())))
    }

    #[inline]
    fn contiguous_batch(self, start: usize, len: usize) -> Option<ByteArrayBatch<'a, 'a>> {
        Some(O::batch(&self.offsets[start..start + len + 1], self.data))
    }
}

/// Access to view arrays through an already downcast accessor.
#[derive(Clone, Copy)]
struct ViewByteArrayAccess<'a, F> {
    get: F,
    views: &'a [u128],
    encoded_value_size_upper_bound: usize,
    _marker: std::marker::PhantomData<&'a [u8]>,
}

impl<'a, F> ViewByteArrayAccess<'a, F>
where
    F: Fn(usize) -> &'a [u8] + Copy,
{
    #[inline]
    fn variable(get: F, views: &'a [u128], value_len_upper_bound: usize) -> Self {
        Self {
            get,
            views,
            encoded_value_size_upper_bound: value_len_upper_bound
                .saturating_add(std::mem::size_of::<u32>()),
            _marker: std::marker::PhantomData,
        }
    }
}

impl<'a, F> ByteArrayValueAccess<'a> for ViewByteArrayAccess<'a, F>
where
    F: Fn(usize) -> &'a [u8] + Copy + 'a,
{
    #[inline]
    fn value(self, index: usize) -> &'a [u8] {
        (self.get)(index)
    }

    #[inline]
    fn value_len(self, index: usize) -> usize {
        // View arrays store each value's length in the low 32 bits of the u128
        // view word, so budgeting need not touch a data buffer.
        self.views[index] as u32 as usize
    }

    #[inline]
    fn range_encoded_upper_bound(self, _start: usize, len: usize) -> Option<usize> {
        Some(self.encoded_value_size_upper_bound.saturating_mul(len))
    }

    #[inline]
    fn inline_views(self) -> Option<&'a [u128]> {
        (self.encoded_value_size_upper_bound == 12 + size_of::<u32>()).then_some(self.views)
    }
}

/// Byte-value producer driven by a physical selection.
#[derive(Clone, Copy)]
struct PhysicalByteArraySource<'a, A> {
    selection: PhysicalValueSelection<'a>,
    values: A,
}

impl<'a, A> ValueProducer<&'a [u8]> for PhysicalByteArraySource<'a, A>
where
    A: ByteArrayValueAccess<'a> + 'a,
{
    #[inline]
    fn len(self) -> usize {
        self.selection.len()
    }

    #[inline]
    fn fill(&mut self, out: &mut [MaybeUninit<&'a [u8]>]) -> usize {
        let values = self.values;
        self.selection
            .fill_mapped(out, move |index| values.value(index))
    }

    #[inline]
    fn try_for_each<E>(self, mut f: impl FnMut(&'a [u8]) -> Result<(), E>) -> Result<(), E> {
        self.selection
            .try_for_each_index(|index| f(self.values.value(index)))
    }
}

impl<'a, A> ByteArraySource<'a> for PhysicalByteArraySource<'a, A>
where
    A: ByteArrayValueAccess<'a> + 'a,
{
    #[inline]
    fn write_flat_to(self, sink: &mut ByteArraySink<'a, '_>) -> Result<()> {
        // Offset layouts can lend every selected range directly. Other
        // layouts retain one gathered stream so short ranges fill full tiles.
        if A::SUPPORTS_CONTIGUOUS_BATCHES
            && self.selection.try_for_each_borrowable_range(|range| {
                sink.push_batch(
                    self.values
                        .contiguous_batch(range.start, range.len())
                        .expect("layout must lend every contiguous range"),
                )
            })?
        {
            return Ok(());
        }

        if let Some(views) = self.values.inline_views()
            && sink.try_push_inline_view_source(self.selection, views, move |index| {
                self.values.value(index)
            })?
        {
            return Ok(());
        }

        sink.push_source(self)
    }
}

/// The final, already-downcast Arrow byte layout. Wrapper lowering and layout
/// dispatch happen once when a leaf source is bound; page slicing, budgeting,
/// and encoding retain this small borrowed descriptor.
#[derive(Clone, Copy)]
pub(crate) struct ByteArrayStorage<'a> {
    kind: ByteArrayStorageKind<'a>,
}

#[derive(Clone, Copy)]
enum ByteArrayStorageKind<'a> {
    Offset32(OffsetByteArrayAccess<'a, i32>),
    Offset64(OffsetByteArrayAccess<'a, i64>),
    Utf8View {
        values: &'a StringViewArray,
        value_len_upper_bound: usize,
    },
    BinaryView {
        values: &'a BinaryViewArray,
        value_len_upper_bound: usize,
    },
}

/// Exhaustively lower the stored Arrow layout to its statically typed byte
/// accessor. The body is expanded in each match arm, retaining monomorphized
/// encoding without continuation structs or a parallel visitor hierarchy.
macro_rules! with_byte_array_access {
    ($kind:expr, |$access:ident| $body:expr) => {
        match $kind {
            ByteArrayStorageKind::Offset32($access) => $body,
            ByteArrayStorageKind::Offset64($access) => $body,
            ByteArrayStorageKind::Utf8View {
                values,
                value_len_upper_bound,
            } => {
                let $access = ViewByteArrayAccess::variable(
                    move |index| values.value(index).as_bytes(),
                    values.views(),
                    value_len_upper_bound,
                );
                $body
            }
            ByteArrayStorageKind::BinaryView {
                values,
                value_len_upper_bound,
            } => {
                let $access = ViewByteArrayAccess::variable(
                    move |index| values.value(index),
                    values.views(),
                    value_len_upper_bound,
                );
                $body
            }
        }
    };
}

impl<'a> ArrowPhysicalBridge<'a> for ByteArrayStorage<'a> {
    type ColumnEncoder = TypedColumnChunkEncoder<ParquetByteArrayType>;

    fn bind(values: &'a dyn Array) -> Result<Self> {
        let kind = match values.data_type() {
            DataType::Utf8 => {
                ByteArrayStorageKind::Offset32(OffsetByteArrayAccess::bind::<Utf8Type>(values))
            }
            DataType::LargeUtf8 => {
                ByteArrayStorageKind::Offset64(OffsetByteArrayAccess::bind::<LargeUtf8Type>(values))
            }
            DataType::Binary => {
                ByteArrayStorageKind::Offset32(OffsetByteArrayAccess::bind::<BinaryType>(values))
            }
            DataType::LargeBinary => ByteArrayStorageKind::Offset64(OffsetByteArrayAccess::bind::<
                LargeBinaryType,
            >(values)),
            DataType::Utf8View => {
                let values = values.as_any().downcast_ref::<StringViewArray>().unwrap();
                ByteArrayStorageKind::Utf8View {
                    values,
                    value_len_upper_bound: max_view_value_len(values.data_buffers()),
                }
            }
            DataType::BinaryView => {
                let values = values.as_any().downcast_ref::<BinaryViewArray>().unwrap();
                ByteArrayStorageKind::BinaryView {
                    values,
                    value_len_upper_bound: max_view_value_len(values.data_buffers()),
                }
            }
            data_type => {
                return Err(ParquetError::General(format!(
                    "Cannot coerce final physical {data_type} to BYTE_ARRAY"
                )));
            }
        };
        Ok(Self { kind })
    }

    fn write_values(
        self,
        encoder: &mut Self::ColumnEncoder,
        selection: PhysicalValueSelection<'a>,
    ) -> Result<()> {
        with_byte_array_access!(self.kind, |values| write_physical_byte_array_source(
            encoder, selection, values,
        ))
    }

    fn count_variable_width_within_byte_budget(
        self,
        _encoder: &Self::ColumnEncoder,
        selection: PhysicalValueSelection<'a>,
        budget: usize,
        _target: ByteBudgetTarget,
    ) -> Option<usize> {
        if selection.len() == 0 {
            return None;
        }

        with_byte_array_access!(self.kind, |values| Some(
            count_selection_within_byte_budget(selection, values, budget)
        ))
    }
}

#[inline]
fn write_physical_byte_array_source<'a, A: ByteArrayValueAccess<'a> + 'a>(
    encoder: &mut TypedColumnChunkEncoder<ParquetByteArrayType>,
    selection: PhysicalValueSelection<'a>,
    values: A,
) -> Result<()> {
    let values = PhysicalByteArraySource { selection, values };
    encoder.write_byte_array_source(values)
}

/// Count leading physical-selection values within a PLAIN byte budget. Range
/// spans over offset arrays cost O(1). The first value crossing the budget is
/// included so the post-write check flushes at this mini-batch boundary.
fn count_selection_within_byte_budget<'a, A: ByteArrayValueAccess<'a>>(
    selection: PhysicalValueSelection<'a>,
    values: A,
    budget: usize,
) -> usize {
    let prefix = std::mem::size_of::<u32>();

    // A conservative view bound is valid only when the selection is one direct range.
    if let Some(range) = selection.direct_physical_range()
        && values
            .exact_range_encoded_size(range.start, range.len())
            .or_else(|| values.range_encoded_upper_bound(range.start, range.len()))
            .is_some_and(|encoded_size_upper_bound| encoded_size_upper_bound <= budget)
    {
        return range.len();
    }

    let mut remaining = budget;
    let mut count = 0usize;
    let _: std::result::Result<(), ()> = selection.try_for_each_span(|span| match span {
        PhysicalValueSpan::Range { start, len } => {
            if let Some(encoded) = values.exact_range_encoded_size(start, len)
                && encoded <= remaining
            {
                remaining -= encoded;
                count += len;
                return Ok(());
            }

            for index in start..start + len {
                let encoded = values.value_len(index).saturating_add(prefix);
                count += 1;
                if encoded > remaining {
                    return Err(());
                }
                remaining -= encoded;
            }
            Ok(())
        }
        PhysicalValueSpan::Gather(indices) => {
            for &index in indices {
                let encoded = values.value_len(index).saturating_add(prefix);
                count += 1;
                if encoded > remaining {
                    return Err(());
                }
                remaining -= encoded;
            }
            Ok(())
        }
    });
    count
}

/// Upper bound on any single value's byte length in a view array.
fn max_view_value_len(buffers: &[Buffer]) -> usize {
    /// Bytes that fit inline in a u128 view word (the rest is len + prefix).
    const MAX_INLINE_VIEW_LEN: usize = 12;
    // An out-of-line view's data is a contiguous slice of exactly one data
    // buffer, so it cannot exceed the largest buffer; inline views hold at
    // most `MAX_INLINE_VIEW_LEN`. Loose (a value is usually far smaller than
    // a whole buffer) but O(number of buffers) and always sound.
    buffers
        .iter()
        .map(|b| b.len())
        .max()
        .unwrap_or(0)
        .max(MAX_INLINE_VIEW_LEN)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column::value_selection::ValueSelectionRef;
    use arrow_array::{LargeStringArray, StringArray};

    fn direct_batch<'a>(
        values: &'a dyn Array,
        selection: ValueSelectionRef<'a>,
    ) -> Option<ByteArrayBatch<'a, 'a>> {
        let storage = ByteArrayStorage::bind(values).unwrap();
        let selection = PhysicalValueSelection::identity(selection);
        let range = selection.direct_physical_range()?;
        with_byte_array_access!(storage.kind, |values| values
            .contiguous_batch(range.start, range.len()))
    }

    #[test]
    fn direct_offset_batches_preserve_selected_subrange() {
        let values = StringArray::from(vec!["a", "bb", "ccc", "dddd"]);
        let selection = ValueSelectionRef::Dense { offset: 1, len: 2 };
        let batch = direct_batch(&values, selection).unwrap();

        // Five payload bytes plus two four-byte PLAIN length prefixes.
        assert_eq!(batch.exact_plain_size(), Some((5, 13)));
        let ByteArrayBatch::Offset32 { offsets, data } = batch else {
            panic!("small-offset input did not produce an Offset32 batch");
        };
        assert_eq!(offsets, &[1, 3, 6]);
        assert_eq!(data, b"abbcccdddd");
        let selected: Vec<_> = offsets
            .windows(2)
            .map(|offset| &data[offset[0] as usize..offset[1] as usize])
            .collect();
        assert_eq!(selected, [b"bb".as_slice(), b"ccc".as_slice()]);

        // Large offsets can represent a single value longer than a Parquet
        // byte-array prefix, so they retain the validating sizing pass.
        let values = LargeStringArray::from(vec!["a", "bb", "ccc", "dddd"]);
        let batch = direct_batch(&values, selection).unwrap();
        assert_eq!(batch.exact_plain_size(), None);
        let ByteArrayBatch::Offset64 { offsets, data } = batch else {
            panic!("large-offset input did not produce an Offset64 batch");
        };
        assert_eq!(offsets, &[1, 3, 6]);
        let selected: Vec<_> = offsets
            .windows(2)
            .map(|offset| &data[offset[0] as usize..offset[1] as usize])
            .collect();
        assert_eq!(selected, [b"bb".as_slice(), b"ccc".as_slice()]);
    }
}
