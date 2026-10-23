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

//! Capacity of narrow offset buffers produced by coalescing array slices.

use arrow_array::{Array, RecordBatch, cast::AsArray};
use arrow_schema::{DataType, Schema};

#[cfg(test)]
mod tests;

/// Tracks logical offset growth, independently of retained buffer memory.
///
/// Each narrow offset buffer has its own counter. Nested arrays are visited
/// using offset endpoints, without scanning their payloads or dictionary keys.
/// Dictionary values are counted in full for every slice: concat's fallback
/// can append them all, including values that the selected keys never use.
#[derive(Debug)]
pub(super) struct OffsetCapacity {
    columns: Vec<(usize, Layout)>,
    totals: Vec<usize>,
    scratch: Vec<usize>,
    supported: bool,
    buffered: bool,
    limit: usize,
}

impl OffsetCapacity {
    pub(super) fn new(schema: &Schema) -> Self {
        let mut counters = 0;
        let mut columns = Vec::new();
        let mut supported = true;
        for (index, field) in schema.fields().iter().enumerate() {
            match Layout::new(field.data_type(), &mut counters) {
                Ok(Some(layout)) => columns.push((index, layout)),
                Ok(None) => {}
                Err(()) => {
                    supported = false;
                    break;
                }
            }
        }
        Self {
            columns,
            totals: vec![0; counters],
            scratch: vec![0; counters],
            supported,
            buffered: false,
            limit: i32::MAX as usize,
        }
    }

    /// Unsupported layouts must bypass coalescing, preserving their arrays.
    pub(super) fn is_supported(&self) -> bool {
        self.supported
    }

    /// Heap allocations retained by the compiled traversal and counters.
    pub(super) fn size(&self) -> usize {
        self.columns.capacity() * std::mem::size_of::<(usize, Layout)>()
            + (self.totals.capacity() + self.scratch.capacity()) * std::mem::size_of::<usize>()
            + self
                .columns
                .iter()
                .map(|(_, layout)| layout.size())
                .sum::<usize>()
    }

    /// Admit a source slice before any column builder is modified.
    ///
    /// Failure leaves the accumulated counters unchanged. After flushing and
    /// resetting, the same valid slice can always be admitted: concat of one
    /// slice returns it without rebuilding any offset buffers. This also avoids
    /// rejecting inputs because of conservative dictionary accounting.
    pub(super) fn try_append(&mut self, batch: &RecordBatch, start: usize, len: usize) -> bool {
        if !self.supported {
            return false;
        }
        if len == 0 || self.columns.is_empty() {
            return true;
        }
        for (column, layout) in &self.columns {
            layout.measure(
                batch.column(*column).as_ref(),
                start,
                len,
                &mut self.scratch,
            );
        }
        if self.buffered
            && self
                .totals
                .iter()
                .zip(&self.scratch)
                .any(|(total, added)| total.saturating_add(*added) > self.limit)
        {
            return false;
        }
        for (total, added) in self.totals.iter_mut().zip(&self.scratch) {
            *total = total.saturating_add(*added);
        }
        self.buffered = true;
        true
    }

    pub(super) fn reset(&mut self) {
        self.totals.fill(0);
        self.buffered = false;
    }

    #[cfg(test)]
    pub(super) fn with_limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }
}

/// A schema-compiled traversal containing only paths with narrow offsets.
#[derive(Debug)]
enum Layout {
    Utf8(usize),
    Binary(usize),
    List(usize, Option<Box<Self>>),
    Map(usize, Option<Box<Self>>),
    LargeList(Box<Self>),
    Struct(Vec<(usize, Self)>),
    FixedSizeList(usize, Box<Self>),
    Dictionary(Box<Self>),
}

impl Layout {
    fn new(data_type: &DataType, counters: &mut usize) -> Result<Option<Self>, ()> {
        fn next(counters: &mut usize) -> usize {
            let index = *counters;
            *counters += 1;
            index
        }
        let layout = match data_type {
            DataType::Utf8 => Self::Utf8(next(counters)),
            DataType::Binary => Self::Binary(next(counters)),
            DataType::List(field) => Self::List(
                next(counters),
                Self::new(field.data_type(), counters)?.map(Box::new),
            ),
            DataType::Map(field, _) => Self::Map(
                next(counters),
                Self::new(field.data_type(), counters)?.map(Box::new),
            ),
            DataType::LargeList(field) => {
                return Ok(Self::new(field.data_type(), counters)?
                    .map(|child| Self::LargeList(Box::new(child))));
            }
            DataType::FixedSizeList(field, size) => {
                return Ok(Self::new(field.data_type(), counters)?
                    .map(|child| Self::FixedSizeList(*size as usize, Box::new(child))));
            }
            DataType::Struct(fields) => {
                let mut children = Vec::new();
                for (index, field) in fields.iter().enumerate() {
                    if let Some(child) = Self::new(field.data_type(), counters)? {
                        children.push((index, child));
                    }
                }
                return Ok((!children.is_empty()).then_some(Self::Struct(children)));
            }
            DataType::Dictionary(_, values) => {
                return Ok(
                    Self::new(values, counters)?.map(|child| Self::Dictionary(Box::new(child)))
                );
            }
            // These layouts have different physical selection semantics.
            // Preserve them instead of guessing their concatenated offsets.
            DataType::ListView(_)
            | DataType::LargeListView(_)
            | DataType::Union(_, _)
            | DataType::RunEndEncoded(_, _) => {
                return if has_narrow_offsets(data_type) {
                    Err(())
                } else {
                    Ok(None)
                };
            }
            _ => return Ok(None),
        };
        Ok(Some(layout))
    }

    fn size(&self) -> usize {
        match self {
            Self::List(_, child) | Self::Map(_, child) => child
                .as_ref()
                .map_or(0, |child| std::mem::size_of::<Self>() + child.size()),
            Self::LargeList(child) | Self::FixedSizeList(_, child) | Self::Dictionary(child) => {
                std::mem::size_of::<Self>() + child.size()
            }
            Self::Struct(children) => {
                children.capacity() * std::mem::size_of::<(usize, Self)>()
                    + children
                        .iter()
                        .map(|(_, child)| child.size())
                        .sum::<usize>()
            }
            Self::Utf8(_) | Self::Binary(_) => 0,
        }
    }

    fn measure(&self, array: &dyn Array, start: usize, len: usize, costs: &mut [usize]) {
        match self {
            Self::Utf8(counter) => {
                let offsets = array.as_string::<i32>().value_offsets();
                costs[*counter] = (offsets[start + len] - offsets[start]) as usize;
            }
            Self::Binary(counter) => {
                let offsets = array.as_binary::<i32>().value_offsets();
                costs[*counter] = (offsets[start + len] - offsets[start]) as usize;
            }
            Self::List(counter, child) => {
                let array = array.as_list::<i32>();
                let offsets = array.value_offsets();
                let begin = offsets[start] as usize;
                let length = offsets[start + len] as usize - begin;
                costs[*counter] = length;
                if let Some(child) = child {
                    child.measure(array.values().as_ref(), begin, length, costs);
                }
            }
            Self::Map(counter, child) => {
                let array = array.as_map();
                let offsets = array.value_offsets();
                let begin = offsets[start] as usize;
                let length = offsets[start + len] as usize - begin;
                costs[*counter] = length;
                if let Some(child) = child {
                    child.measure(array.entries(), begin, length, costs);
                }
            }
            Self::LargeList(child) => {
                let array = array.as_list::<i64>();
                let offsets = array.value_offsets();
                let begin = offsets[start] as usize;
                let length = offsets[start + len] as usize - begin;
                child.measure(array.values().as_ref(), begin, length, costs);
            }
            Self::Struct(children) => {
                let array = array.as_struct();
                for (index, child) in children {
                    child.measure(array.column(*index).as_ref(), start, len, costs);
                }
            }
            Self::FixedSizeList(size, child) => {
                let array = array.as_fixed_size_list();
                child.measure(array.values().as_ref(), start * size, len * size, costs);
            }
            Self::Dictionary(child) => {
                let values = array.as_any_dictionary().values();
                child.measure(values.as_ref(), 0, values.len(), costs);
            }
        }
    }
}

fn has_narrow_offsets(data_type: &DataType) -> bool {
    match data_type {
        DataType::Utf8
        | DataType::Binary
        | DataType::List(_)
        | DataType::ListView(_)
        | DataType::Map(_, _) => true,
        DataType::LargeList(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::RunEndEncoded(_, field) => has_narrow_offsets(field.data_type()),
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| has_narrow_offsets(field.data_type())),
        DataType::Union(fields, _) => fields
            .iter()
            .any(|(_, field)| has_narrow_offsets(field.data_type())),
        DataType::Dictionary(_, values) => has_narrow_offsets(values),
        _ => false,
    }
}
