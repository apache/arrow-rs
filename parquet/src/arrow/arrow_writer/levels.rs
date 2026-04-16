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

//! Parquet definition and repetition levels
//!
//! Contains the algorithm for computing definition and repetition levels.
//! The algorithm works by tracking the slots of an array that should
//! ultimately be populated when writing to Parquet.
//! Parquet achieves nesting through definition levels and repetition levels \[1\].
//! Definition levels specify how many optional fields in the part for the column
//! are defined.
//! Repetition levels specify at what repeated field (list) in the path a column
//! is defined.
//!
//! In a nested data structure such as `a.b.c`, one can see levels as defining
//! whether a record is defined at `a`, `a.b`, or `a.b.c`.
//! Optional fields are nullable fields, thus if all 3 fields
//! are nullable, the maximum definition could be = 3 if there are no lists.
//!
//! The algorithm in this module computes the necessary information to enable
//! the writer to keep track of which columns are at which levels, and to extract
//! the correct values at the correct slots from Arrow arrays.
//!
//! It works by walking a record batch's arrays, keeping track of what values
//! are non-null, their positions and computing what their levels are.
//!
//! \[1\] [parquet-format#nested-encoding](https://github.com/apache/parquet-format#nested-encoding)

use crate::column::chunker::CdcChunk;
use crate::column::writer::{LevelDataRef, ValueSelectionRef};
use crate::errors::{ParquetError, Result};
use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, OffsetSizeTrait};
use arrow_buffer::bit_iterator::BitIndexIterator;
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field};
use std::ops::Range;
use std::sync::Arc;

/// Performs a depth-first scan of the children of `array`, constructing [`ArrayLevels`]
/// for each leaf column encountered
pub(crate) fn calculate_array_levels(array: &ArrayRef, field: &Field) -> Result<Vec<ArrayLevels>> {
    let mut builder = LevelInfoBuilder::try_new(field, Default::default(), array)?;
    builder.write(0..array.len());
    Ok(builder.finish())
}

/// Returns true if the DataType can be represented as a primitive parquet column,
/// i.e. a leaf array with no children
fn is_leaf(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Null
            | DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
            | DataType::Utf8View
            | DataType::LargeUtf8
            | DataType::Timestamp(_, _)
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Duration(_)
            | DataType::Interval(_)
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::Decimal32(_, _)
            | DataType::Decimal64(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
            | DataType::FixedSizeBinary(_)
    )
}

/// The definition and repetition level of an array within a potentially nested hierarchy
#[derive(Debug, Default, Clone, Copy)]
struct LevelContext {
    /// The current repetition level
    rep_level: i16,
    /// The current definition level
    def_level: i16,
}

/// A helper to construct [`ArrayLevels`] from a potentially nested [`Field`]
#[derive(Debug)]
enum LevelInfoBuilder {
    /// A primitive, leaf array
    Primitive(ArrayLevels),
    /// A list array
    List(
        Box<LevelInfoBuilder>, // Child Values
        LevelContext,          // Context
        OffsetBuffer<i32>,     // Offsets
        Option<NullBuffer>,    // Nulls
    ),
    /// A large list array
    LargeList(
        Box<LevelInfoBuilder>, // Child Values
        LevelContext,          // Context
        OffsetBuffer<i64>,     // Offsets
        Option<NullBuffer>,    // Nulls
    ),
    /// A fixed size list array
    FixedSizeList(
        Box<LevelInfoBuilder>, // Values
        LevelContext,          // Context
        usize,                 // List Size
        Option<NullBuffer>,    // Nulls
    ),
    /// A list view array
    ListView(
        Box<LevelInfoBuilder>, // Child Values
        LevelContext,          // Context
        ScalarBuffer<i32>,     // Offsets
        ScalarBuffer<i32>,     // Sizes
        Option<NullBuffer>,    // Nulls
    ),
    /// A large list view array
    LargeListView(
        Box<LevelInfoBuilder>, // Child Values
        LevelContext,          // Context
        ScalarBuffer<i64>,     // Offsets
        ScalarBuffer<i64>,     // Sizes
        Option<NullBuffer>,    // Nulls
    ),
    /// A struct array
    Struct(Vec<LevelInfoBuilder>, LevelContext, Option<NullBuffer>),
}

impl LevelInfoBuilder {
    /// Create a new [`LevelInfoBuilder`] for the given [`Field`] and parent [`LevelContext`]
    fn try_new(field: &Field, parent_ctx: LevelContext, array: &ArrayRef) -> Result<Self> {
        if !Self::types_compatible(field.data_type(), array.data_type()) {
            return Err(arrow_err!(format!(
                "Incompatible type. Field '{}' has type {}, array has type {}",
                field.name(),
                field.data_type(),
                array.data_type(),
            )));
        }

        let is_nullable = field.is_nullable();

        match array.data_type() {
            d if is_leaf(d) => {
                let levels = ArrayLevels::new(parent_ctx, is_nullable, array.clone());
                Ok(Self::Primitive(levels))
            }
            DataType::Dictionary(_, v) if is_leaf(v.as_ref()) => {
                let levels = ArrayLevels::new(parent_ctx, is_nullable, array.clone());
                Ok(Self::Primitive(levels))
            }
            DataType::Struct(children) => {
                let array = array.as_struct();
                let def_level = match is_nullable {
                    true => parent_ctx.def_level + 1,
                    false => parent_ctx.def_level,
                };

                let ctx = LevelContext {
                    rep_level: parent_ctx.rep_level,
                    def_level,
                };

                let children = children
                    .iter()
                    .zip(array.columns())
                    .map(|(f, a)| Self::try_new(f, ctx, a))
                    .collect::<Result<_>>()?;

                Ok(Self::Struct(children, ctx, array.nulls().cloned()))
            }
            DataType::List(child)
            | DataType::LargeList(child)
            | DataType::Map(child, _)
            | DataType::FixedSizeList(child, _)
            | DataType::ListView(child)
            | DataType::LargeListView(child) => {
                let def_level = match is_nullable {
                    true => parent_ctx.def_level + 2,
                    false => parent_ctx.def_level + 1,
                };

                let ctx = LevelContext {
                    rep_level: parent_ctx.rep_level + 1,
                    def_level,
                };

                Ok(match field.data_type() {
                    DataType::List(_) => {
                        let list = array.as_list();
                        let child = Self::try_new(child.as_ref(), ctx, list.values())?;
                        let offsets = list.offsets().clone();
                        Self::List(Box::new(child), ctx, offsets, list.nulls().cloned())
                    }
                    DataType::LargeList(_) => {
                        let list = array.as_list();
                        let child = Self::try_new(child.as_ref(), ctx, list.values())?;
                        let offsets = list.offsets().clone();
                        let nulls = list.nulls().cloned();
                        Self::LargeList(Box::new(child), ctx, offsets, nulls)
                    }
                    DataType::Map(_, _) => {
                        let map = array.as_map();
                        let entries = Arc::new(map.entries().clone()) as ArrayRef;
                        let child = Self::try_new(child.as_ref(), ctx, &entries)?;
                        let offsets = map.offsets().clone();
                        Self::List(Box::new(child), ctx, offsets, map.nulls().cloned())
                    }
                    DataType::FixedSizeList(_, size) => {
                        let list = array.as_fixed_size_list();
                        let child = Self::try_new(child.as_ref(), ctx, list.values())?;
                        let nulls = list.nulls().cloned();
                        Self::FixedSizeList(Box::new(child), ctx, *size as _, nulls)
                    }
                    DataType::ListView(_) => {
                        let list = array.as_list_view();
                        let child = Self::try_new(child.as_ref(), ctx, list.values())?;
                        let offsets = list.offsets().clone();
                        let sizes = list.sizes().clone();
                        let nulls = list.nulls().cloned();
                        Self::ListView(Box::new(child), ctx, offsets, sizes, nulls)
                    }
                    DataType::LargeListView(_) => {
                        let list = array.as_list_view();
                        let child = Self::try_new(child.as_ref(), ctx, list.values())?;
                        let offsets = list.offsets().clone();
                        let sizes = list.sizes().clone();
                        let nulls = list.nulls().cloned();
                        Self::LargeListView(Box::new(child), ctx, offsets, sizes, nulls)
                    }
                    _ => unreachable!(),
                })
            }
            d => Err(nyi_err!("Datatype {} is not yet supported", d)),
        }
    }

    /// Finish this [`LevelInfoBuilder`] returning the [`ArrayLevels`] for the leaf columns
    /// as enumerated by a depth-first search
    fn finish(self) -> Vec<ArrayLevels> {
        match self {
            LevelInfoBuilder::Primitive(v) => vec![v],
            LevelInfoBuilder::List(v, _, _, _)
            | LevelInfoBuilder::LargeList(v, _, _, _)
            | LevelInfoBuilder::FixedSizeList(v, _, _, _)
            | LevelInfoBuilder::ListView(v, _, _, _, _)
            | LevelInfoBuilder::LargeListView(v, _, _, _, _) => v.finish(),
            LevelInfoBuilder::Struct(v, _, _) => v.into_iter().flat_map(|l| l.finish()).collect(),
        }
    }

    /// Given an `array`, write the level data for the elements in `range`
    fn write(&mut self, range: Range<usize>) {
        match self {
            LevelInfoBuilder::Primitive(info) => Self::write_leaf(info, range),
            LevelInfoBuilder::List(child, ctx, offsets, nulls) => {
                Self::write_list(child, ctx, offsets, nulls.as_ref(), range)
            }
            LevelInfoBuilder::LargeList(child, ctx, offsets, nulls) => {
                Self::write_list(child, ctx, offsets, nulls.as_ref(), range)
            }
            LevelInfoBuilder::FixedSizeList(child, ctx, size, nulls) => {
                Self::write_fixed_size_list(child, ctx, *size, nulls.as_ref(), range)
            }
            LevelInfoBuilder::ListView(child, ctx, offsets, sizes, nulls) => {
                Self::write_list_view(child, ctx, offsets, sizes, nulls.as_ref(), range)
            }
            LevelInfoBuilder::LargeListView(child, ctx, offsets, sizes, nulls) => {
                Self::write_list_view(child, ctx, offsets, sizes, nulls.as_ref(), range)
            }
            LevelInfoBuilder::Struct(children, ctx, nulls) => {
                Self::write_struct(children, ctx, nulls.as_ref(), range)
            }
        }
    }

    /// Write `range` elements from ListArray `array`
    ///
    /// Note: MapArrays are `ListArray<i32>` under the hood and so are dispatched to this method
    fn write_list<O: OffsetSizeTrait>(
        child: &mut LevelInfoBuilder,
        ctx: &LevelContext,
        offsets: &[O],
        nulls: Option<&NullBuffer>,
        range: Range<usize>,
    ) {
        // Fast path: entire list array is null — emit bulk null rep/def levels
        if let Some(nulls) = nulls {
            if nulls.null_count() == nulls.len() {
                let count = range.end - range.start;
                child.visit_leaves(|leaf| {
                    leaf.extend_uniform_levels(ctx.def_level - 2, ctx.rep_level - 1, count);
                });
                return;
            }
        }

        let offsets = &offsets[range.start..range.end + 1];

        let write_non_null_slice =
            |child: &mut LevelInfoBuilder, start_idx: usize, end_idx: usize| {
                child.write(start_idx..end_idx);
                child.visit_leaves(|leaf| {
                    let rep_levels = leaf.rep_levels.materialize_mut().unwrap();
                    let mut rev = rep_levels.iter_mut().rev();
                    let mut remaining = end_idx - start_idx;

                    loop {
                        let next = rev.next().unwrap();
                        if *next > ctx.rep_level {
                            // Nested element - ignore
                            continue;
                        }

                        remaining -= 1;
                        if remaining == 0 {
                            *next = ctx.rep_level - 1;
                            break;
                        }
                    }
                })
            };

        // In a list column, each row falls into one of three categories:
        // - "null": the list slot is absent (!is_valid), encoded at def_level - 2
        // - "empty": the list slot is present but has zero elements
        //   (offsets[i] == offsets[i+1]), encoded at def_level - 1
        // - non-empty: the list slot has child values, which are recursed into
        //
        // Consecutive runs of null or empty rows are batched and written together.
        let write_null_run = |child: &mut LevelInfoBuilder, count: usize| {
            if count > 0 {
                child.visit_leaves(|leaf| {
                    leaf.append_rep_level_run(ctx.rep_level - 1, count);
                    leaf.append_def_level_run(ctx.def_level - 2, count);
                });
            }
        };

        let write_empty_run = |child: &mut LevelInfoBuilder, count: usize| {
            if count > 0 {
                child.visit_leaves(|leaf| {
                    leaf.append_rep_level_run(ctx.rep_level - 1, count);
                    leaf.append_def_level_run(ctx.def_level - 1, count);
                });
            }
        };

        match nulls {
            Some(nulls) => {
                let null_offset = range.start;
                let mut pending_nulls: usize = 0;
                let mut pending_empties: usize = 0;

                // TODO: Faster bitmask iteration (#1757)
                for (idx, w) in offsets.windows(2).enumerate() {
                    let is_valid = nulls.is_valid(idx + null_offset);
                    let start_idx = w[0].as_usize();
                    let end_idx = w[1].as_usize();

                    if !is_valid {
                        write_empty_run(child, pending_empties);
                        pending_empties = 0;
                        pending_nulls += 1;
                    } else if start_idx == end_idx {
                        write_null_run(child, pending_nulls);
                        pending_nulls = 0;
                        pending_empties += 1;
                    } else {
                        write_null_run(child, pending_nulls);
                        pending_nulls = 0;
                        write_empty_run(child, pending_empties);
                        pending_empties = 0;
                        write_non_null_slice(child, start_idx, end_idx);
                    }
                }
                write_null_run(child, pending_nulls);
                write_empty_run(child, pending_empties);
            }
            None => {
                let mut pending_empties: usize = 0;
                for w in offsets.windows(2) {
                    let start_idx = w[0].as_usize();
                    let end_idx = w[1].as_usize();
                    if start_idx == end_idx {
                        pending_empties += 1;
                    } else {
                        write_empty_run(child, pending_empties);
                        pending_empties = 0;
                        write_non_null_slice(child, start_idx, end_idx);
                    }
                }
                write_empty_run(child, pending_empties);
            }
        }
    }

    /// Write `range` elements from ListViewArray `array`
    fn write_list_view<O: OffsetSizeTrait>(
        child: &mut LevelInfoBuilder,
        ctx: &LevelContext,
        offsets: &[O],
        sizes: &[O],
        nulls: Option<&NullBuffer>,
        range: Range<usize>,
    ) {
        let offsets = &offsets[range.start..range.end];
        let sizes = &sizes[range.start..range.end];

        let write_non_null_slice =
            |child: &mut LevelInfoBuilder, start_idx: usize, end_idx: usize| {
                child.write(start_idx..end_idx);
                child.visit_leaves(|leaf| {
                    let rep_levels = leaf.rep_levels.materialize_mut().unwrap();
                    let mut rev = rep_levels.iter_mut().rev();
                    let mut remaining = end_idx - start_idx;

                    loop {
                        let next = rev.next().unwrap();
                        if *next > ctx.rep_level {
                            // Nested element - ignore
                            continue;
                        }

                        remaining -= 1;
                        if remaining == 0 {
                            *next = ctx.rep_level - 1;
                            break;
                        }
                    }
                })
            };

        let write_empty_slice = |child: &mut LevelInfoBuilder| {
            child.visit_leaves(|leaf| {
                leaf.append_rep_level_run(ctx.rep_level - 1, 1);
                leaf.append_def_level_run(ctx.def_level - 1, 1);
            })
        };

        let write_null_slice = |child: &mut LevelInfoBuilder| {
            child.visit_leaves(|leaf| {
                leaf.append_rep_level_run(ctx.rep_level - 1, 1);
                leaf.append_def_level_run(ctx.def_level - 2, 1);
            })
        };

        match nulls {
            Some(nulls) => {
                let null_offset = range.start;
                // TODO: Faster bitmask iteration (#1757)
                for (idx, (offset, size)) in offsets.iter().zip(sizes.iter()).enumerate() {
                    let is_valid = nulls.is_valid(idx + null_offset);
                    let start_idx = offset.as_usize();
                    let size = size.as_usize();
                    let end_idx = start_idx + size;
                    if !is_valid {
                        write_null_slice(child)
                    } else if size == 0 {
                        write_empty_slice(child)
                    } else {
                        write_non_null_slice(child, start_idx, end_idx)
                    }
                }
            }
            None => {
                for (offset, size) in offsets.iter().zip(sizes.iter()) {
                    let start_idx = offset.as_usize();
                    let size = size.as_usize();
                    let end_idx = start_idx + size;
                    if size == 0 {
                        write_empty_slice(child)
                    } else {
                        write_non_null_slice(child, start_idx, end_idx)
                    }
                }
            }
        }
    }

    /// Write `range` elements from StructArray `array`
    fn write_struct(
        children: &mut [LevelInfoBuilder],
        ctx: &LevelContext,
        nulls: Option<&NullBuffer>,
        range: Range<usize>,
    ) {
        // Fast path: entire struct array is null — emit bulk null def/rep levels
        if let Some(nulls) = nulls {
            if nulls.null_count() == nulls.len() {
                let len = range.end - range.start;
                for child in children.iter_mut() {
                    child.visit_leaves(|info| {
                        info.extend_uniform_levels(ctx.def_level - 1, ctx.rep_level, len);
                    });
                }
                return;
            }
        }

        let write_null = |children: &mut [LevelInfoBuilder], range: Range<usize>| {
            for child in children {
                child.visit_leaves(|info| {
                    let len = range.end - range.start;
                    info.append_def_level_run(ctx.def_level - 1, len);
                    info.append_rep_level_run(ctx.rep_level, len);
                })
            }
        };

        let write_non_null = |children: &mut [LevelInfoBuilder], range: Range<usize>| {
            for child in children {
                child.write(range.clone())
            }
        };

        match nulls {
            Some(validity) => {
                let mut last_non_null_idx = None;
                let mut last_null_idx = None;

                // TODO: Faster bitmask iteration (#1757)
                for i in range.clone() {
                    match validity.is_valid(i) {
                        true => {
                            if let Some(last_idx) = last_null_idx.take() {
                                write_null(children, last_idx..i)
                            }
                            last_non_null_idx.get_or_insert(i);
                        }
                        false => {
                            if let Some(last_idx) = last_non_null_idx.take() {
                                write_non_null(children, last_idx..i)
                            }
                            last_null_idx.get_or_insert(i);
                        }
                    }
                }

                if let Some(last_idx) = last_null_idx.take() {
                    write_null(children, last_idx..range.end)
                }

                if let Some(last_idx) = last_non_null_idx.take() {
                    write_non_null(children, last_idx..range.end)
                }
            }
            None => write_non_null(children, range),
        }
    }

    /// Write `range` elements from FixedSizeListArray with child data `values` and null bitmap `nulls`.
    fn write_fixed_size_list(
        child: &mut LevelInfoBuilder,
        ctx: &LevelContext,
        fixed_size: usize,
        nulls: Option<&NullBuffer>,
        range: Range<usize>,
    ) {
        // Fast path: entire fixed-size list array is null
        if let Some(nulls) = nulls {
            if nulls.null_count() == nulls.len() {
                let count = range.end - range.start;
                child.visit_leaves(|leaf| {
                    leaf.extend_uniform_levels(ctx.def_level - 2, ctx.rep_level - 1, count);
                });
                return;
            }
        }

        let write_non_null = |child: &mut LevelInfoBuilder, start_idx: usize, end_idx: usize| {
            let values_start = start_idx * fixed_size;
            let values_end = end_idx * fixed_size;
            child.write(values_start..values_end);

            child.visit_leaves(|leaf| {
                let rep_levels = leaf.rep_levels.materialize_mut().unwrap();

                let row_indices = (0..fixed_size)
                    .rev()
                    .cycle()
                    .take(values_end - values_start);

                // Step backward over the child rep levels and mark the start of each list
                rep_levels
                    .iter_mut()
                    .rev()
                    // Filter out reps from nested children
                    .filter(|&&mut r| r == ctx.rep_level)
                    .zip(row_indices)
                    .for_each(|(r, idx)| {
                        if idx == 0 {
                            *r = ctx.rep_level - 1;
                        }
                    });
            })
        };

        // If list size is 0, ignore values and just write rep/def levels.
        let write_empty = |child: &mut LevelInfoBuilder, start_idx: usize, end_idx: usize| {
            let len = end_idx - start_idx;
            child.visit_leaves(|leaf| {
                leaf.append_rep_level_run(ctx.rep_level - 1, len);
                leaf.append_def_level_run(ctx.def_level - 1, len);
            })
        };

        let write_rows = |child: &mut LevelInfoBuilder, start_idx: usize, end_idx: usize| {
            if fixed_size > 0 {
                write_non_null(child, start_idx, end_idx)
            } else {
                write_empty(child, start_idx, end_idx)
            }
        };

        match nulls {
            Some(nulls) => {
                let mut start_idx = None;
                for idx in range.clone() {
                    if nulls.is_valid(idx) {
                        // Start a run of valid rows if not already inside of one
                        start_idx.get_or_insert(idx);
                    } else {
                        // Write out any pending valid rows
                        if let Some(start) = start_idx.take() {
                            write_rows(child, start, idx);
                        }
                        // Add null row
                        child.visit_leaves(|leaf| {
                            leaf.append_rep_level_run(ctx.rep_level - 1, 1);
                            leaf.append_def_level_run(ctx.def_level - 2, 1);
                        })
                    }
                }
                // Write out any remaining valid rows
                if let Some(start) = start_idx.take() {
                    write_rows(child, start, range.end);
                }
            }
            // If all rows are valid then write the whole array
            None => write_rows(child, range.start, range.end),
        }
    }

    /// Write a primitive array, as defined by [`is_leaf`]
    fn write_leaf(info: &mut ArrayLevels, range: Range<usize>) {
        let len = range.end - range.start;

        // Fast path: entire leaf array is null
        if let Some(nulls) = &info.logical_nulls {
            if !matches!(info.def_levels, LevelData::Absent) && nulls.null_count() == nulls.len() {
                info.extend_uniform_levels(info.max_def_level - 1, info.max_rep_level, len);
                return;
            }
        }

        // Cheap Arc clone: releases the shared borrow on `info` so the arms can call &mut self methods.
        match info.logical_nulls.clone() {
            Some(nulls) if matches!(info.def_levels, LevelData::Absent) => {
                info.extend_value_indices(
                    BitIndexIterator::new(
                        nulls.inner().values(),
                        nulls.offset() + range.start,
                        len,
                    )
                    .map(|i| i + range.start),
                );
            }
            Some(nulls) => {
                assert!(range.end <= nulls.len());
                let nulls = nulls.inner();
                let max_def_level = info.max_def_level;
                info.extend_def_levels(range.clone().map(|i| {
                    // Safety: range.end was asserted to be in bounds earlier
                    let valid = unsafe { nulls.value_unchecked(i) };
                    max_def_level - (!valid as i16)
                }));
                info.extend_value_indices(
                    BitIndexIterator::new(
                        nulls.inner().as_slice(),
                        nulls.offset() + range.start,
                        len,
                    )
                    .map(|i| i + range.start),
                );
            }
            None if matches!(info.def_levels, LevelData::Absent) => {
                info.append_value_range(range);
            }
            None => {
                info.append_def_level_run(info.max_def_level, len);
                info.append_value_range(range);
            }
        }

        if !matches!(info.rep_levels, LevelData::Absent) {
            info.append_rep_level_run(info.max_rep_level, len)
        }
    }

    /// Visits all children of this node in depth first order
    fn visit_leaves(&mut self, visit: impl Fn(&mut ArrayLevels) + Copy) {
        match self {
            LevelInfoBuilder::Primitive(info) => visit(info),
            LevelInfoBuilder::List(c, _, _, _)
            | LevelInfoBuilder::LargeList(c, _, _, _)
            | LevelInfoBuilder::FixedSizeList(c, _, _, _)
            | LevelInfoBuilder::ListView(c, _, _, _, _)
            | LevelInfoBuilder::LargeListView(c, _, _, _, _) => c.visit_leaves(visit),
            LevelInfoBuilder::Struct(children, _, _) => {
                for c in children {
                    c.visit_leaves(visit)
                }
            }
        }
    }

    /// Determine if the fields are compatible for purposes of constructing `LevelBuilderInfo`.
    ///
    /// Fields are compatible if they're the same type. Otherwise if one of them is a dictionary
    /// and the other is a native array, the dictionary values must have the same type as the
    /// native array
    fn types_compatible(a: &DataType, b: &DataType) -> bool {
        // if the Arrow data types are equal, the types are deemed compatible
        if a.equals_datatype(b) {
            return true;
        }

        // get the values out of the dictionaries
        let (a, b) = match (a, b) {
            (DataType::Dictionary(_, va), DataType::Dictionary(_, vb)) => {
                (va.as_ref(), vb.as_ref())
            }
            (DataType::Dictionary(_, v), b) => (v.as_ref(), b),
            (a, DataType::Dictionary(_, v)) => (a, v.as_ref()),
            _ => (a, b),
        };

        // now that we've got the values from one/both dictionaries, if the values
        // have the same Arrow data type, they're compatible
        if a == b {
            return true;
        }

        // here we have different Arrow data types, but if the array contains the same type of data
        // then we consider the type compatible
        match a {
            // String, StringView and LargeString are compatible
            DataType::Utf8 => matches!(b, DataType::LargeUtf8 | DataType::Utf8View),
            DataType::Utf8View => matches!(b, DataType::LargeUtf8 | DataType::Utf8),
            DataType::LargeUtf8 => matches!(b, DataType::Utf8 | DataType::Utf8View),

            // Binary, BinaryView and LargeBinary are compatible
            DataType::Binary => matches!(b, DataType::LargeBinary | DataType::BinaryView),
            DataType::BinaryView => matches!(b, DataType::LargeBinary | DataType::Binary),
            DataType::LargeBinary => matches!(b, DataType::Binary | DataType::BinaryView),

            // otherwise we have incompatible types
            _ => false,
        }
    }
}

/// The data necessary to write a primitive Arrow array to parquet, taking into account
/// any non-primitive parents it may have in the arrow representation
#[derive(Debug, Clone)]
pub(crate) enum LevelData {
    Absent,
    Materialized(Vec<i16>),
    Uniform { value: i16, count: usize },
}

impl PartialEq for LevelData {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Absent, Self::Absent) => true,
            (Self::Materialized(a), Self::Materialized(b)) => a == b,
            (Self::Uniform { value: v, count: n }, Self::Materialized(b))
            | (Self::Materialized(b), Self::Uniform { value: v, count: n }) => {
                b.len() == *n && b.iter().all(|x| x == v)
            }
            (
                Self::Uniform {
                    value: v1,
                    count: n1,
                },
                Self::Uniform {
                    value: v2,
                    count: n2,
                },
            ) => v1 == v2 && n1 == n2,
            _ => false,
        }
    }
}

impl Eq for LevelData {}

impl LevelData {
    fn new(present: bool) -> Self {
        match present {
            true => Self::Materialized(Vec::new()),
            false => Self::Absent,
        }
    }

    pub(crate) fn as_ref(&self) -> LevelDataRef<'_> {
        match self {
            Self::Absent => LevelDataRef::Absent,
            Self::Materialized(values) => LevelDataRef::Materialized(values),
            Self::Uniform { value, count } => LevelDataRef::Uniform {
                value: *value,
                count: *count,
            },
        }
    }

    pub(crate) fn slice(&self, offset: usize, len: usize) -> Self {
        match self {
            Self::Absent => Self::Absent,
            Self::Materialized(values) => Self::Materialized(values[offset..offset + len].to_vec()),
            Self::Uniform { value, .. } => Self::Uniform {
                value: *value,
                count: len,
            },
        }
    }

    fn append_run(&mut self, value: i16, count: usize) {
        if count == 0 {
            return;
        }

        match self {
            Self::Absent => {}
            Self::Materialized(values) if values.is_empty() => {
                *self = Self::Uniform { value, count };
            }
            Self::Materialized(values) => values.extend(std::iter::repeat_n(value, count)),
            Self::Uniform {
                value: uniform_value,
                count: uniform_count,
            } if *uniform_value == value => {
                *uniform_count += count;
            }
            Self::Uniform { .. } => {
                let values = self.materialize_mut().unwrap();
                values.extend(std::iter::repeat_n(value, count));
            }
        }
    }

    fn extend_from_iter<I>(&mut self, iter: I)
    where
        I: IntoIterator<Item = i16>,
    {
        if let Some(values) = self.materialize_mut() {
            values.extend(iter);
        }
    }

    fn materialize_mut(&mut self) -> Option<&mut Vec<i16>> {
        match self {
            Self::Absent => None,
            Self::Materialized(values) => Some(values),
            Self::Uniform { value, count } => {
                let values = vec![*value; *count];
                *self = Self::Materialized(values);
                match self {
                    Self::Materialized(values) => Some(values),
                    _ => unreachable!(),
                }
            }
        }
    }
}

/// Zero-allocation iterator over the indices in a [`ValueSelection`].
pub(crate) enum ValueIndicesIter<'a> {
    Empty,
    Range(std::ops::Range<usize>),
    Slice(std::slice::Iter<'a, usize>),
}

impl Iterator for ValueIndicesIter<'_> {
    type Item = usize;

    #[inline]
    fn next(&mut self) -> Option<usize> {
        match self {
            Self::Empty => None,
            Self::Range(r) => r.next(),
            Self::Slice(s) => s.next().copied(),
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::Empty => (0, Some(0)),
            Self::Range(r) => r.size_hint(),
            Self::Slice(s) => s.size_hint(),
        }
    }
}

impl ExactSizeIterator for ValueIndicesIter<'_> {}

#[derive(Debug, Clone)]
pub(crate) enum ValueSelection {
    Empty,
    Dense { offset: usize, len: usize },
    Sparse(Vec<usize>),
}

impl PartialEq for ValueSelection {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Empty, Self::Empty) => true,
            (
                Self::Dense {
                    offset: o1,
                    len: l1,
                },
                Self::Dense {
                    offset: o2,
                    len: l2,
                },
            ) => o1 == o2 && l1 == l2,
            (Self::Sparse(a), Self::Sparse(b)) => a == b,
            // Dense and Sparse are equal when the sparse indices form a contiguous range.
            (Self::Dense { offset, len }, Self::Sparse(indices))
            | (Self::Sparse(indices), Self::Dense { offset, len }) => {
                indices.len() == *len
                    && indices.first().copied() == Some(*offset)
                    && indices.windows(2).all(|w| w[1] == w[0] + 1)
            }
            _ => false,
        }
    }
}

impl Eq for ValueSelection {}

impl ValueSelection {
    /// Returns an iterator over the selected indices without allocating.
    pub(crate) fn iter(&self) -> ValueIndicesIter<'_> {
        match self {
            Self::Empty => ValueIndicesIter::Empty,
            Self::Dense { offset, len } => ValueIndicesIter::Range(*offset..*offset + *len),
            Self::Sparse(indices) => ValueIndicesIter::Slice(indices.iter()),
        }
    }

    pub(crate) fn as_ref(&self) -> ValueSelectionRef<'_> {
        match self {
            Self::Empty => ValueSelectionRef::Empty,
            Self::Dense { offset, len } => ValueSelectionRef::Dense {
                offset: *offset,
                len: *len,
            },
            Self::Sparse(indices) => ValueSelectionRef::Sparse(indices),
        }
    }

    fn from_indices(indices: Vec<usize>) -> Self {
        match (indices.first().copied(), indices.last().copied()) {
            (None, _) => Self::Empty,
            (Some(start), Some(end)) if end + 1 - start == indices.len() => {
                debug_assert!(
                    indices.windows(2).all(|w| w[0] < w[1]),
                    "unsorted dense indices"
                );
                Self::Dense {
                    offset: start,
                    len: indices.len(),
                }
            }
            _ => Self::Sparse(indices),
        }
    }

    fn append_range(&mut self, range: Range<usize>) {
        if range.is_empty() {
            return;
        }

        match self {
            Self::Empty => {
                *self = Self::Dense {
                    offset: range.start,
                    len: range.end - range.start,
                };
            }
            Self::Dense { offset, len } if *offset + *len == range.start => {
                *len += range.end - range.start;
            }
            Self::Dense { .. } => {
                let indices = self.materialize_mut();
                indices.extend(range);
            }
            Self::Sparse(indices) => indices.extend(range),
        }
    }

    fn extend_indices<I>(&mut self, iter: I)
    where
        I: IntoIterator<Item = usize>,
    {
        self.materialize_mut().extend(iter);
    }

    fn materialize_mut(&mut self) -> &mut Vec<usize> {
        match self {
            Self::Sparse(indices) => indices,
            Self::Empty => {
                *self = Self::Sparse(Vec::new());
                match self {
                    Self::Sparse(indices) => indices,
                    _ => unreachable!(),
                }
            }
            Self::Dense { offset, len } => {
                let indices: Vec<_> = (*offset..*offset + *len).collect();
                *self = Self::Sparse(indices);
                match self {
                    Self::Sparse(indices) => indices,
                    _ => unreachable!(),
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ArrayLevels {
    /// Array's definition levels
    ///
    /// Present if `max_def_level != 0`
    def_levels: LevelData,

    /// Array's optional repetition levels
    ///
    /// Present if `max_rep_level != 0`
    rep_levels: LevelData,

    /// The corresponding array identifying non-null slices of data
    /// from the primitive array
    values: ValueSelection,

    /// The maximum definition level for this leaf column
    max_def_level: i16,

    /// The maximum repetition for this leaf column
    max_rep_level: i16,

    /// The arrow array
    array: ArrayRef,

    /// cached logical nulls of the array.
    logical_nulls: Option<NullBuffer>,
}

impl PartialEq for ArrayLevels {
    fn eq(&self, other: &Self) -> bool {
        self.def_levels == other.def_levels
            && self.rep_levels == other.rep_levels
            && self.values == other.values
            && self.max_def_level == other.max_def_level
            && self.max_rep_level == other.max_rep_level
            && self.array.as_ref() == other.array.as_ref()
            && self.logical_nulls.as_ref() == other.logical_nulls.as_ref()
    }
}
impl Eq for ArrayLevels {}

impl ArrayLevels {
    fn new(ctx: LevelContext, is_nullable: bool, array: ArrayRef) -> Self {
        let max_rep_level = ctx.rep_level;
        let max_def_level = match is_nullable {
            true => ctx.def_level + 1,
            false => ctx.def_level,
        };

        let logical_nulls = array.logical_nulls();

        Self {
            def_levels: LevelData::new(max_def_level != 0),
            rep_levels: LevelData::new(max_rep_level != 0),
            values: ValueSelection::Empty,
            max_def_level,
            max_rep_level,
            array,
            logical_nulls,
        }
    }

    pub fn array(&self) -> &ArrayRef {
        &self.array
    }

    pub(crate) fn def_level_data(&self) -> &LevelData {
        &self.def_levels
    }

    pub(crate) fn rep_level_data(&self) -> &LevelData {
        &self.rep_levels
    }

    pub(crate) fn value_selection(&self) -> &ValueSelection {
        &self.values
    }

    /// Create a sliced view of this `ArrayLevels` for a CDC chunk.
    ///
    /// Slices `def_levels` and `rep_levels` by the level range, then computes
    /// the minimal array range spanned by the chunk's non-null values and
    /// rebases the value selection to be relative to that slice.
    /// When `num_values == 0` (all-null chunk), the array slice has length 0.
    pub(crate) fn slice_for_chunk(&self, chunk: &CdcChunk) -> Self {
        let def_levels = self.def_levels.slice(chunk.level_offset, chunk.num_levels);
        let rep_levels = self.rep_levels.slice(chunk.level_offset, chunk.num_levels);

        // Compute the minimal array range spanned by the selected non-null values
        // and rebase the value selection relative to that range.
        let (array_start, array_len, values) = match &self.values {
            ValueSelection::Empty => (0, 0, ValueSelection::Empty),
            ValueSelection::Dense { offset, .. } => {
                let start = offset + chunk.value_offset;
                (
                    start,
                    chunk.num_values,
                    ValueSelection::Dense {
                        offset: 0,
                        len: chunk.num_values,
                    },
                )
            }
            ValueSelection::Sparse(indices) => {
                if chunk.num_values == 0 {
                    (0, 0, ValueSelection::Empty)
                } else {
                    let sel = &indices[chunk.value_offset..chunk.value_offset + chunk.num_values];
                    let start = sel[0];
                    let end = *sel.last().unwrap() + 1;
                    if end - start == sel.len() {
                        // Contiguous indices — represent as Dense without allocating.
                        (
                            start,
                            chunk.num_values,
                            ValueSelection::Dense {
                                offset: 0,
                                len: chunk.num_values,
                            },
                        )
                    } else {
                        let shifted =
                            ValueSelection::from_indices(sel.iter().map(|&i| i - start).collect());
                        (start, end - start, shifted)
                    }
                }
            }
        };

        let array = self.array.slice(array_start, array_len);
        let logical_nulls = array.logical_nulls();

        Self {
            def_levels,
            rep_levels,
            values,
            max_def_level: self.max_def_level,
            max_rep_level: self.max_rep_level,
            array,
            logical_nulls,
        }
    }

    /// Bulk-emit `count` uniform def/rep levels.
    fn extend_uniform_levels(&mut self, def_val: i16, rep_val: i16, count: usize) {
        self.def_levels.append_run(def_val, count);
        self.rep_levels.append_run(rep_val, count);
    }

    fn append_value_range(&mut self, range: Range<usize>) {
        self.values.append_range(range);
    }

    fn extend_value_indices<I>(&mut self, iter: I)
    where
        I: IntoIterator<Item = usize>,
    {
        self.values.extend_indices(iter);
    }

    fn append_def_level_run(&mut self, value: i16, count: usize) {
        self.def_levels.append_run(value, count);
    }

    fn append_rep_level_run(&mut self, value: i16, count: usize) {
        self.rep_levels.append_run(value, count);
    }

    fn extend_def_levels<I>(&mut self, iter: I)
    where
        I: IntoIterator<Item = i16>,
    {
        self.def_levels.extend_from_iter(iter);
    }

    #[cfg(test)]
    fn materialized_indices(&self) -> Vec<usize> {
        match &self.values {
            ValueSelection::Empty => Vec::new(),
            ValueSelection::Dense { offset, len } => (*offset..*offset + *len).collect(),
            ValueSelection::Sparse(indices) => indices.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column::chunker::CdcChunk;

    use arrow_array::builder::*;
    use arrow_array::types::Int32Type;
    use arrow_array::*;
    use arrow_buffer::{Buffer, ToByteSlice};
    use arrow_cast::display::array_value_to_string;
    use arrow_data::{ArrayData, ArrayDataBuilder};
    use arrow_schema::{Fields, Schema};

    #[test]
    fn test_calculate_array_levels_twitter_example() {
        // based on the example at https://blog.twitter.com/engineering/en_us/a/2013/dremel-made-simple-with-parquet.html
        // [[a, b, c], [d, e, f, g]], [[h], [i,j]]

        let leaf_type = Field::new_list_field(DataType::Int32, false);
        let inner_type = DataType::List(Arc::new(leaf_type));
        let inner_field = Field::new("l2", inner_type.clone(), false);
        let outer_type = DataType::List(Arc::new(inner_field));
        let outer_field = Field::new("l1", outer_type.clone(), false);

        let primitives = Int32Array::from_iter(0..10);

        // Cannot use from_iter_primitive as always infers nullable
        let offsets = Buffer::from_iter([0_i32, 3, 7, 8, 10]);
        let inner_list = ArrayDataBuilder::new(inner_type)
            .len(4)
            .add_buffer(offsets)
            .add_child_data(primitives.to_data())
            .build()
            .unwrap();

        let offsets = Buffer::from_iter([0_i32, 2, 4]);
        let outer_list = ArrayDataBuilder::new(outer_type)
            .len(2)
            .add_buffer(offsets)
            .add_child_data(inner_list)
            .build()
            .unwrap();
        let outer_list = make_array(outer_list);

        let levels = calculate_array_levels(&outer_list, &outer_field).unwrap();
        assert_eq!(levels.len(), 1);

        let expected = ArrayLevels {
            def_levels: LevelData::Materialized(vec![2; 10]),
            rep_levels: LevelData::Materialized(vec![0, 2, 2, 1, 2, 2, 2, 0, 1, 2]),
            values: ValueSelection::from_indices(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
            max_def_level: 2,
            max_rep_level: 2,
            array: Arc::new(primitives),
            logical_nulls: None,
        };
        assert_eq!(&levels[0], &expected);
    }

    #[test]
    fn test_calculate_one_level_1() {
        // This test calculates the levels for a non-null primitive array
        let array = Arc::new(Int32Array::from_iter(0..10)) as ArrayRef;
        let field = Field::new_list_field(DataType::Int32, false);

        let levels = calculate_array_levels(&array, &field).unwrap();
        assert_eq!(levels.len(), 1);

        let expected_levels = ArrayLevels {
            def_levels: LevelData::Absent,
            rep_levels: LevelData::Absent,
            values: ValueSelection::from_indices((0..10).collect()),
            max_def_level: 0,
            max_rep_level: 0,
            array,
            logical_nulls: None,
        };
        assert_eq!(&levels[0], &expected_levels);
    }

    #[test]
    fn test_calculate_one_level_2() {
        // This test calculates the levels for a nullable primitive array
        let array = Arc::new(Int32Array::from_iter([
            Some(0),
            None,
            Some(0),
            Some(0),
            None,
        ])) as ArrayRef;
        let field = Field::new_list_field(DataType::Int32, true);

        let levels = calculate_array_levels(&array, &field).unwrap();
        assert_eq!(levels.len(), 1);

        let logical_nulls = array.logical_nulls();
        let expected_levels = ArrayLevels {
            def_levels: LevelData::Materialized(vec![1, 0, 1, 1, 0]),
            rep_levels: LevelData::Absent,
            values: ValueSelection::from_indices(vec![0, 2, 3]),
            max_def_level: 1,
            max_rep_level: 0,
            array,
            logical_nulls,
        };
        assert_eq!(&levels[0], &expected_levels);
    }

    #[test]
    fn test_calculate_one_level_nullable_no_nulls_uses_uniform_dense() {
        let array = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let field = Field::new_list_field(DataType::Int32, true);

        let levels = calculate_array_levels(&array, &field).unwrap();
        assert_eq!(levels.len(), 1);

        let expected_levels = ArrayLevels {
            def_levels: LevelData::Uniform { value: 1, count: 3 },
            rep_levels: LevelData::Absent,
            values: ValueSelection::Dense { offset: 0, len: 3 },
            max_def_level: 1,
            max_rep_level: 0,
            array,
            logical_nulls: None,
        };
        assert_eq!(&levels[0], &expected_levels);
    }

    #[test]
    fn test_calculate_array_levels_1() {
        let leaf_field = Field::new_list_field(DataType::Int32, false);
        let list_type = DataType::List(Arc::new(leaf_field));

        // if all array values are defined (e.g. batch<list<_>>)
        // [[0], [1], [2], [3], [4]]

        let leaf_array = Int32Array::from_iter(0..5);
        // Cannot use from_iter_primitive as always infers nullable
        let offsets = Buffer::from_iter(0_i32..6);
        let list = ArrayDataBuilder::new(list_type.clone())
            .len(5)
            .add_buffer(offsets)
            .add_child_data(leaf_array.to_data())
            .build()
            .unwrap();
        let list = make_array(list);

        let list_field = Field::new("list", list_type.clone(), false);
        let levels = calculate_array_levels(&list, &list_field).unwrap();
        assert_eq!(levels.len(), 1);

        let expected_levels = ArrayLevels {
            def_levels: LevelData::Materialized(vec![1; 5]),
            rep_levels: LevelData::Materialized(vec![0; 5]),
            values: ValueSelection::from_indices((0..5).collect()),
            max_def_level: 1,
            max_rep_level: 1,
            array: Arc::new(leaf_array),
            logical_nulls: None,
        };
        assert_eq!(&levels[0], &expected_levels);

        // array: [[0, 0], NULL, [2, 2], [3, 3, 3, 3], [4, 4, 4]]
        // all values are defined as we do not have nulls on the root (batch)
        // repetition:
        //   0: 0, 1
        //   1: 0
        //   2: 0, 1
        //   3: 0, 1, 1, 1
        //   4: 0, 1, 1
        let leaf_array = Int32Array::from_iter([0, 0, 2, 2, 3, 3, 3, 3, 4, 4, 4]);
        let offsets = Buffer::from_iter([0_i32, 2, 2, 4, 8, 11]);
        let list = ArrayDataBuilder::new(list_type.clone())
            .len(5)
            .add_buffer(offsets)
            .add_child_data(leaf_array.to_data())
            .null_bit_buffer(Some(Buffer::from([0b00011101])))
            .build()
            .unwrap();
        let list = make_array(list);

        let list_field = Field::new("list", list_type, true);
        let levels = calculate_array_levels(&list, &list_field).unwrap();
        assert_eq!(levels.len(), 1);

        let expected_levels = ArrayLevels {
            def_levels: LevelData::Materialized(vec![2, 2, 0, 2, 2, 2, 2, 2, 2, 2, 2, 2]),
            rep_levels: LevelData::Materialized(vec![0, 1, 0, 0, 1, 0, 1, 1, 1, 0, 1, 1]),
            values: ValueSelection::from_indices((0..11).collect()),
            max_def_level: 2,
            max_rep_level: 1,
            array: Arc::new(leaf_array),
            logical_nulls: None,
        };
        assert_eq!(&levels[0], &expected_levels);
    }

    #[test]
    fn test_write_list_interleaved_null_empty() {
        let leaf_field = Field::new_list_field(DataType::Int32, false);
        let list_type = DataType::List(Arc::new(leaf_field));

        let leaf_array = Int32Array::from(vec![1, 2, 3]);
        let offsets = Buffer::from_iter([0_i32, 0, 0, 2, 2, 2, 2, 3, 3]);
        let null_bitmap = Buffer::from([0b11100110_u8]);
        let list = ArrayDataBuilder::new(list_type.clone())
            .len(8)
            .add_buffer(offsets)
            .add_child_data(leaf_array.to_data())
            .null_bit_buffer(Some(null_bitmap))
            .build()
            .unwrap();
        let list = make_array(list);

        let list_field = Field::new("list", list_type, true);
        let levels = calculate_array_levels(&list, &list_field).unwrap();
        assert_eq!(levels.len(), 1);
        let levels = &levels[0];

        assert_eq!(
            levels.def_level_data(),
            &LevelData::Materialized(vec![0, 1, 2, 2, 0, 0, 1, 2, 1]),
        );
        assert_eq!(
            levels.rep_level_data(),
            &LevelData::Materialized(vec![0, 0, 0, 1, 0, 0, 0, 0, 0]),
        );
        assert_eq!(levels.materialized_indices(), vec![0, 1, 2]);
    }

    #[test]
    fn test_calculate_array_levels_2() {
        // If some values are null
        //
        // This emulates an array in the form: <struct<list<?>>
        // with values:
        // - 0: [0, 1], but is null because of the struct
        // - 1: []
        // - 2: [2, 3], but is null because of the struct
        // - 3: [4, 5, 6, 7]
        // - 4: [8, 9, 10]
        //
        // If the first values of a list are null due to a parent, we have to still account for them
        // while indexing, because they would affect the way the child is indexed
        // i.e. in the above example, we have to know that [0, 1] has to be skipped
        let leaf = Int32Array::from_iter(0..11);
        let leaf_field = Field::new("leaf", DataType::Int32, false);

        let list_type = DataType::List(Arc::new(leaf_field));
        let list = ArrayData::builder(list_type.clone())
            .len(5)
            .add_child_data(leaf.to_data())
            .add_buffer(Buffer::from_iter([0_i32, 2, 2, 4, 8, 11]))
            .build()
            .unwrap();

        let list = make_array(list);
        let list_field = Arc::new(Field::new("list", list_type, true));

        let struct_array =
            StructArray::from((vec![(list_field, list)], Buffer::from([0b00011010])));
        let array = Arc::new(struct_array) as ArrayRef;

        let struct_field = Field::new("struct", array.data_type().clone(), true);

        let levels = calculate_array_levels(&array, &struct_field).unwrap();
        assert_eq!(levels.len(), 1);

        let expected_levels = ArrayLevels {
            def_levels: LevelData::Materialized(vec![0, 2, 0, 3, 3, 3, 3, 3, 3, 3]),
            rep_levels: LevelData::Materialized(vec![0, 0, 0, 0, 1, 1, 1, 0, 1, 1]),
            values: ValueSelection::from_indices((4..11).collect()),
            max_def_level: 3,
            max_rep_level: 1,
            array: Arc::new(leaf),
            logical_nulls: None,
        };

        assert_eq!(&levels[0], &expected_levels);

        // nested lists

        // 0: [[100, 101], [102, 103]]
        // 1: []
        // 2: [[104, 105], [106, 107]]
        // 3: [[108, 109], [110, 111], [112, 113], [114, 115]]
        // 4: [[116, 117], [118, 119], [120, 121]]

        let leaf = Int32Array::from_iter(100..122);
        let leaf_field = Field::new("leaf", DataType::Int32, true);

        let l1_type = DataType::List(Arc::new(leaf_field));
        let offsets = Buffer::from_iter([0_i32, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22]);
        let l1 = ArrayData::builder(l1_type.clone())
            .len(11)
            .add_child_data(leaf.to_data())
            .add_buffer(offsets)
            .build()
            .unwrap();

        let l1_field = Field::new("l1", l1_type, true);
        let l2_type = DataType::List(Arc::new(l1_field));
        let l2 = ArrayData::builder(l2_type)
            .len(5)
            .add_child_data(l1)
            .add_buffer(Buffer::from_iter([0, 2, 2, 4, 8, 11]))
            .build()
            .unwrap();

        let l2 = make_array(l2);
        let l2_field = Field::new("l2", l2.data_type().clone(), true);

        let levels = calculate_array_levels(&l2, &l2_field).unwrap();
        assert_eq!(levels.len(), 1);

        let expected_levels = ArrayLevels {
            def_levels: LevelData::Materialized(vec![
                5, 5, 5, 5, 1, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5,
            ]),
            rep_levels: LevelData::Materialized(vec![
                0, 2, 1, 2, 0, 0, 2, 1, 2, 0, 2, 1, 2, 1, 2, 1, 2, 0, 2, 1, 2, 1, 2,
            ]),
            values: ValueSelection::from_indices((0..22).collect()),
            max_def_level: 5,
            max_rep_level: 2,
            array: Arc::new(leaf),
            logical_nulls: None,
        };

        assert_eq!(&levels[0], &expected_levels);
    }

    #[test]
    fn test_calculate_array_levels_nested_list() {
        let leaf_field = Field::new("leaf", DataType::Int32, false);
        let list_type = DataType::List(Arc::new(leaf_field));

        // if all array values are defined (e.g. batch<list<_>>)
        // The array at this level looks like:
        // 0: [a]
        // 1: [a]
        // 2: [a]
        // 3: [a]

        let leaf = Int32Array::from_iter([0; 4]);
        let list = ArrayData::builder(list_type.clone())
            .len(4)
            .add_buffer(Buffer::from_iter(0_i32..5))
            .add_child_data(leaf.to_data())
            .build()
            .unwrap();
        let list = make_array(list);

        let list_field = Field::new("list", list_type.clone(), false);
        let levels = calculate_array_levels(&list, &list_field).unwrap();
        assert_eq!(levels.len(), 1);

        let expected_levels = ArrayLevels {
            def_levels: LevelData::Materialized(vec![1; 4]),
            rep_levels: LevelData::Materialized(vec![0; 4]),
            values: ValueSelection::from_indices((0..4).collect()),
            max_def_level: 1,
            max_rep_level: 1,
            array: Arc::new(leaf),
            logical_nulls: None,
        };
        assert_eq!(&levels[0], &expected_levels);

        // 0: null
        // 1: [1, 2, 3]
        // 2: [4, 5]
        // 3: [6, 7]
        let leaf = Int32Array::from_iter(0..8);
        let list = ArrayData::builder(list_type.clone())
            .len(4)
            .add_buffer(Buffer::from_iter([0_i32, 0, 3, 5, 7]))
            .null_bit_buffer(Some(Buffer::from([0b00001110])))
            .add_child_data(leaf.to_data())
            .build()
            .unwrap();
        let list = make_array(list);
        let list_field = Arc::new(Field::new("list", list_type, true));

        let struct_array = StructArray::from(vec![(list_field, list)]);
        let array = Arc::new(struct_array) as ArrayRef;

        let struct_field = Field::new("struct", array.data_type().clone(), true);
        let levels = calculate_array_levels(&array, &struct_field).unwrap();
        assert_eq!(levels.len(), 1);

        let expected_levels = ArrayLevels {
            def_levels: LevelData::Materialized(vec![1, 3, 3, 3, 3, 3, 3, 3]),
            rep_levels: LevelData::Materialized(vec![0, 0, 1, 1, 0, 1, 0, 1]),
            values: ValueSelection::from_indices((0..7).collect()),
            max_def_level: 3,
            max_rep_level: 1,
            array: Arc::new(leaf),
            logical_nulls: None,
        };
        assert_eq!(&levels[0], &expected_levels);

        // nested lists
        // In a JSON syntax with the schema: <struct<list<list<primitive>>>>, this translates into:
        // 0: {"struct": null }
        // 1: {"struct": [ [201], [202, 203], [] ]}
        // 2: {"struct": [ [204, 205, 206], [207, 208, 209, 210] ]}
        // 3: {"struct": [ [], [211, 212, 213, 214, 215] ]}

        let leaf = Int32Array::from_iter(201..216);
        let leaf_field = Field::new("leaf", DataType::Int32, false);
        let list_1_type = DataType::List(Arc::new(leaf_field));
        let list_1 = ArrayData::builder(list_1_type.clone())
            .len(7)
            .add_buffer(Buffer::from_iter([0_i32, 1, 3, 3, 6, 10, 10, 15]))
            .add_child_data(leaf.to_data())
            .build()
            .unwrap();

        let list_1_field = Field::new("l1", list_1_type, true);
        let list_2_type = DataType::List(Arc::new(list_1_field));
        let list_2 = ArrayData::builder(list_2_type.clone())
            .len(4)
            .add_buffer(Buffer::from_iter([0_i32, 0, 3, 5, 7]))
            .null_bit_buffer(Some(Buffer::from([0b00001110])))
            .add_child_data(list_1)
            .build()
            .unwrap();

        let list_2 = make_array(list_2);
        let list_2_field = Arc::new(Field::new("list_2", list_2_type, true));

        let struct_array =
            StructArray::from((vec![(list_2_field, list_2)], Buffer::from([0b00001111])));
        let struct_field = Field::new("struct", struct_array.data_type().clone(), true);

        let array = Arc::new(struct_array) as ArrayRef;
        let levels = calculate_array_levels(&array, &struct_field).unwrap();
        assert_eq!(levels.len(), 1);

        let expected_levels = ArrayLevels {
            def_levels: LevelData::Materialized(vec![
                1, 5, 5, 5, 4, 5, 5, 5, 5, 5, 5, 5, 4, 5, 5, 5, 5, 5,
            ]),
            rep_levels: LevelData::Materialized(vec![
                0, 0, 1, 2, 1, 0, 2, 2, 1, 2, 2, 2, 0, 1, 2, 2, 2, 2,
            ]),
            values: ValueSelection::from_indices((0..15).collect()),
            max_def_level: 5,
            max_rep_level: 2,
            array: Arc::new(leaf),
            logical_nulls: None,
        };
        assert_eq!(&levels[0], &expected_levels);
    }

    #[test]
    fn test_calculate_nested_struct_levels() {
        // tests a <struct[a]<struct[b]<int[c]>>
        // array:
        //  - {a: {b: {c: 1}}}
        //  - {a: {b: {c: null}}}
        //  - {a: {b: {c: 3}}}
        //  - {a: {b: null}}
        //  - {a: null}}
        //  - {a: {b: {c: 6}}}

        let c = Int32Array::from_iter([Some(1), None, Some(3), None, Some(5), Some(6)]);
        let leaf = Arc::new(c) as ArrayRef;
        let c_field = Arc::new(Field::new("c", DataType::Int32, true));
        let b = StructArray::from(((vec![(c_field, leaf.clone())]), Buffer::from([0b00110111])));

        let b_field = Arc::new(Field::new("b", b.data_type().clone(), true));
        let a = StructArray::from((
            (vec![(b_field, Arc::new(b) as ArrayRef)]),
            Buffer::from([0b00101111]),
        ));

        let a_field = Field::new("a", a.data_type().clone(), true);
        let a_array = Arc::new(a) as ArrayRef;

        let levels = calculate_array_levels(&a_array, &a_field).unwrap();
        assert_eq!(levels.len(), 1);

        let logical_nulls = leaf.logical_nulls();
        let expected_levels = ArrayLevels {
            def_levels: LevelData::Materialized(vec![3, 2, 3, 1, 0, 3]),
            rep_levels: LevelData::Absent,
            values: ValueSelection::from_indices(vec![0, 2, 5]),
            max_def_level: 3,
            max_rep_level: 0,
            array: leaf,
            logical_nulls,
        };
        assert_eq!(&levels[0], &expected_levels);
    }

    #[test]
    fn list_single_column() {
        // this tests the level generation from the arrow_writer equivalent test

        let a_values = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let a_value_offsets = arrow::buffer::Buffer::from_iter([0_i32, 1, 3, 3, 6, 10]);
        let a_list_type = DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true)));
        let a_list_data = ArrayData::builder(a_list_type.clone())
            .len(5)
            .add_buffer(a_value_offsets)
            .null_bit_buffer(Some(Buffer::from([0b00011011])))
            .add_child_data(a_values.to_data())
            .build()
            .unwrap();

        assert_eq!(a_list_data.null_count(), 1);

        let a = ListArray::from(a_list_data);

        let item_field = Field::new_list_field(a_list_type, true);
        let mut builder = levels(&item_field, a);
        builder.write(2..4);
        let levels = builder.finish();

        assert_eq!(levels.len(), 1);

        let list_level = &levels[0];

        let expected_level = ArrayLevels {
            def_levels: LevelData::Materialized(vec![0, 3, 3, 3]),
            rep_levels: LevelData::Materialized(vec![0, 0, 1, 1]),
            values: ValueSelection::from_indices(vec![3, 4, 5]),
            max_def_level: 3,
            max_rep_level: 1,
            array: Arc::new(a_values),
            logical_nulls: None,
        };
        assert_eq!(list_level, &expected_level);
    }

    #[test]
    fn mixed_struct_list() {
        // this tests the level generation from the equivalent arrow_writer_complex test

        // define schema
        let struct_field_d = Arc::new(Field::new("d", DataType::Float64, true));
        let struct_field_f = Arc::new(Field::new("f", DataType::Float32, true));
        let struct_field_g = Arc::new(Field::new(
            "g",
            DataType::List(Arc::new(Field::new("items", DataType::Int16, false))),
            false,
        ));
        let struct_field_e = Arc::new(Field::new(
            "e",
            DataType::Struct(vec![struct_field_f.clone(), struct_field_g.clone()].into()),
            true,
        ));
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, true),
            Field::new(
                "c",
                DataType::Struct(vec![struct_field_d.clone(), struct_field_e.clone()].into()),
                true, // https://github.com/apache/arrow-rs/issues/245
            ),
        ]);

        // create some data
        let a = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let b = Int32Array::from(vec![Some(1), None, None, Some(4), Some(5)]);
        let d = Float64Array::from(vec![None, None, None, Some(1.0), None]);
        let f = Float32Array::from(vec![Some(0.0), None, Some(333.3), None, Some(5.25)]);

        let g_value = Int16Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

        // Construct a buffer for value offsets, for the nested array:
        //  [[1], [2, 3], null, [4, 5, 6], [7, 8, 9, 10]]
        let g_value_offsets = arrow::buffer::Buffer::from([0, 1, 3, 3, 6, 10].to_byte_slice());

        // Construct a list array from the above two
        let g_list_data = ArrayData::builder(struct_field_g.data_type().clone())
            .len(5)
            .add_buffer(g_value_offsets)
            .add_child_data(g_value.into_data())
            .build()
            .unwrap();
        let g = ListArray::from(g_list_data);

        let e = StructArray::from(vec![
            (struct_field_f, Arc::new(f.clone()) as ArrayRef),
            (struct_field_g, Arc::new(g) as ArrayRef),
        ]);

        let c = StructArray::from(vec![
            (struct_field_d, Arc::new(d.clone()) as ArrayRef),
            (struct_field_e, Arc::new(e) as ArrayRef),
        ]);

        // build a record batch
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(a.clone()), Arc::new(b.clone()), Arc::new(c)],
        )
        .unwrap();

        //////////////////////////////////////////////
        // calculate the list's level
        let mut levels = vec![];
        batch
            .columns()
            .iter()
            .zip(batch.schema().fields())
            .for_each(|(array, field)| {
                let mut array_levels = calculate_array_levels(array, field).unwrap();
                levels.append(&mut array_levels);
            });
        assert_eq!(levels.len(), 5);

        // test "a" levels
        let list_level = &levels[0];

        let expected_level = ArrayLevels {
            def_levels: LevelData::Absent,
            rep_levels: LevelData::Absent,
            values: ValueSelection::from_indices(vec![0, 1, 2, 3, 4]),
            max_def_level: 0,
            max_rep_level: 0,
            array: Arc::new(a),
            logical_nulls: None,
        };
        assert_eq!(list_level, &expected_level);

        // test "b" levels
        let list_level = levels.get(1).unwrap();

        let b_logical_nulls = b.logical_nulls();
        let expected_level = ArrayLevels {
            def_levels: LevelData::Materialized(vec![1, 0, 0, 1, 1]),
            rep_levels: LevelData::Absent,
            values: ValueSelection::from_indices(vec![0, 3, 4]),
            max_def_level: 1,
            max_rep_level: 0,
            array: Arc::new(b),
            logical_nulls: b_logical_nulls,
        };
        assert_eq!(list_level, &expected_level);

        // test "d" levels
        let list_level = levels.get(2).unwrap();

        let d_logical_nulls = d.logical_nulls();
        let expected_level = ArrayLevels {
            def_levels: LevelData::Materialized(vec![1, 1, 1, 2, 1]),
            rep_levels: LevelData::Absent,
            values: ValueSelection::from_indices(vec![3]),
            max_def_level: 2,
            max_rep_level: 0,
            array: Arc::new(d),
            logical_nulls: d_logical_nulls,
        };
        assert_eq!(list_level, &expected_level);

        // test "f" levels
        let list_level = levels.get(3).unwrap();

        let f_logical_nulls = f.logical_nulls();
        let expected_level = ArrayLevels {
            def_levels: LevelData::Materialized(vec![3, 2, 3, 2, 3]),
            rep_levels: LevelData::Absent,
            values: ValueSelection::from_indices(vec![0, 2, 4]),
            max_def_level: 3,
            max_rep_level: 0,
            array: Arc::new(f),
            logical_nulls: f_logical_nulls,
        };
        assert_eq!(list_level, &expected_level);
    }

    #[test]
    fn test_null_vs_nonnull_struct() {
        // define schema
        let offset_field = Arc::new(Field::new("offset", DataType::Int32, true));
        let schema = Schema::new(vec![Field::new(
            "some_nested_object",
            DataType::Struct(vec![offset_field.clone()].into()),
            false,
        )]);

        // create some data
        let offset = Int32Array::from(vec![1, 2, 3, 4, 5]);

        let some_nested_object =
            StructArray::from(vec![(offset_field, Arc::new(offset) as ArrayRef)]);

        // build a record batch
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(some_nested_object)]).unwrap();

        let struct_null_level =
            calculate_array_levels(batch.column(0), batch.schema().field(0)).unwrap();

        // create second batch
        // define schema
        let offset_field = Arc::new(Field::new("offset", DataType::Int32, true));
        let schema = Schema::new(vec![Field::new(
            "some_nested_object",
            DataType::Struct(vec![offset_field.clone()].into()),
            true,
        )]);

        // create some data
        let offset = Int32Array::from(vec![1, 2, 3, 4, 5]);

        let some_nested_object =
            StructArray::from(vec![(offset_field, Arc::new(offset) as ArrayRef)]);

        // build a record batch
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(some_nested_object)]).unwrap();

        let struct_non_null_level =
            calculate_array_levels(batch.column(0), batch.schema().field(0)).unwrap();

        // The 2 levels should not be the same
        if struct_non_null_level == struct_null_level {
            panic!("Levels should not be equal, to reflect the difference in struct nullness");
        }
    }

    #[test]
    fn test_map_array() {
        // Note: we are using the JSON Arrow reader for brevity
        let json_content = r#"
        {"stocks":{"long": "$AAA", "short": "$BBB"}}
        {"stocks":{"long": "$CCC", "short": null}}
        {"stocks":{"hedged": "$YYY", "long": null, "short": "$D"}}
        "#;
        let entries_struct_type = DataType::Struct(Fields::from(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
        ]));
        let stocks_field = Field::new(
            "stocks",
            DataType::Map(
                Arc::new(Field::new("entries", entries_struct_type, false)),
                false,
            ),
            // not nullable, so the keys have max level = 1
            false,
        );
        let schema = Arc::new(Schema::new(vec![stocks_field]));
        let builder = arrow::json::ReaderBuilder::new(schema).with_batch_size(64);
        let mut reader = builder.build(std::io::Cursor::new(json_content)).unwrap();

        let batch = reader.next().unwrap().unwrap();

        // calculate the map's level
        let mut levels = vec![];
        batch
            .columns()
            .iter()
            .zip(batch.schema().fields())
            .for_each(|(array, field)| {
                let mut array_levels = calculate_array_levels(array, field).unwrap();
                levels.append(&mut array_levels);
            });
        assert_eq!(levels.len(), 2);

        let map = batch.column(0).as_map();
        let map_keys_logical_nulls = map.keys().logical_nulls();

        // test key levels
        let list_level = &levels[0];

        let expected_level = ArrayLevels {
            def_levels: LevelData::Materialized(vec![1; 7]),
            rep_levels: LevelData::Materialized(vec![0, 1, 0, 1, 0, 1, 1]),
            values: ValueSelection::from_indices(vec![0, 1, 2, 3, 4, 5, 6]),
            max_def_level: 1,
            max_rep_level: 1,
            array: map.keys().clone(),
            logical_nulls: map_keys_logical_nulls,
        };
        assert_eq!(list_level, &expected_level);

        // test values levels
        let list_level = levels.get(1).unwrap();
        let map_values_logical_nulls = map.values().logical_nulls();

        let expected_level = ArrayLevels {
            def_levels: LevelData::Materialized(vec![2, 2, 2, 1, 2, 1, 2]),
            rep_levels: LevelData::Materialized(vec![0, 1, 0, 1, 0, 1, 1]),
            values: ValueSelection::from_indices(vec![0, 1, 2, 4, 6]),
            max_def_level: 2,
            max_rep_level: 1,
            array: map.values().clone(),
            logical_nulls: map_values_logical_nulls,
        };
        assert_eq!(list_level, &expected_level);
    }

    #[test]
    fn test_list_of_struct() {
        // define schema
        let int_field = Field::new("a", DataType::Int32, true);
        let fields = Fields::from([Arc::new(int_field)]);
        let item_field = Field::new_list_field(DataType::Struct(fields.clone()), true);
        let list_field = Field::new("list", DataType::List(Arc::new(item_field)), true);

        let int_builder = Int32Builder::with_capacity(10);
        let struct_builder = StructBuilder::new(fields, vec![Box::new(int_builder)]);
        let mut list_builder = ListBuilder::new(struct_builder);

        // [{a: 1}], [], null, [null, null], [{a: null}], [{a: 2}]
        //
        // [{a: 1}]
        let values = list_builder.values();
        values
            .field_builder::<Int32Builder>(0)
            .unwrap()
            .append_value(1);
        values.append(true);
        list_builder.append(true);

        // []
        list_builder.append(true);

        // null
        list_builder.append(false);

        // [null, null]
        let values = list_builder.values();
        values
            .field_builder::<Int32Builder>(0)
            .unwrap()
            .append_null();
        values.append(false);
        values
            .field_builder::<Int32Builder>(0)
            .unwrap()
            .append_null();
        values.append(false);
        list_builder.append(true);

        // [{a: null}]
        let values = list_builder.values();
        values
            .field_builder::<Int32Builder>(0)
            .unwrap()
            .append_null();
        values.append(true);
        list_builder.append(true);

        // [{a: 2}]
        let values = list_builder.values();
        values
            .field_builder::<Int32Builder>(0)
            .unwrap()
            .append_value(2);
        values.append(true);
        list_builder.append(true);

        let array = Arc::new(list_builder.finish());

        let values = array.values().as_struct().column(0).clone();
        let values_len = values.len();
        assert_eq!(values_len, 5);

        let schema = Arc::new(Schema::new(vec![list_field]));

        let rb = RecordBatch::try_new(schema, vec![array]).unwrap();

        let levels = calculate_array_levels(rb.column(0), rb.schema().field(0)).unwrap();
        let list_level = &levels[0];

        let logical_nulls = values.logical_nulls();
        let expected_level = ArrayLevels {
            def_levels: LevelData::Materialized(vec![4, 1, 0, 2, 2, 3, 4]),
            rep_levels: LevelData::Materialized(vec![0, 0, 0, 0, 1, 0, 0]),
            values: ValueSelection::from_indices(vec![0, 4]),
            max_def_level: 4,
            max_rep_level: 1,
            array: values,
            logical_nulls,
        };

        assert_eq!(list_level, &expected_level);
    }

    #[test]
    fn test_struct_mask_list() {
        // Test the null mask of a struct array masking out non-empty slices of a child ListArray
        let inner = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(2)]),
            Some(vec![None]),
            Some(vec![]),
            Some(vec![Some(3), None]), // Masked by struct array
            Some(vec![Some(4), Some(5)]),
            None, // Masked by struct array
            None,
        ]);
        let values = inner.values().clone();

        // This test assumes that nulls don't take up space
        assert_eq!(inner.values().len(), 7);

        let field = Arc::new(Field::new("list", inner.data_type().clone(), true));
        let array = Arc::new(inner) as ArrayRef;
        let nulls = Buffer::from([0b01010111]);
        let struct_a = StructArray::from((vec![(field, array)], nulls));

        let field = Field::new("struct", struct_a.data_type().clone(), true);
        let array = Arc::new(struct_a) as ArrayRef;
        let levels = calculate_array_levels(&array, &field).unwrap();

        assert_eq!(levels.len(), 1);

        let logical_nulls = values.logical_nulls();
        let expected_level = ArrayLevels {
            def_levels: LevelData::Materialized(vec![4, 4, 3, 2, 0, 4, 4, 0, 1]),
            rep_levels: LevelData::Materialized(vec![0, 1, 0, 0, 0, 0, 1, 0, 0]),
            values: ValueSelection::from_indices(vec![0, 1, 5, 6]),
            max_def_level: 4,
            max_rep_level: 1,
            array: values,
            logical_nulls,
        };

        assert_eq!(&levels[0], &expected_level);
    }

    #[test]
    fn test_list_mask_struct() {
        // Test the null mask of a struct array and the null mask of a list array
        // masking out non-null elements of their children

        let a1 = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![None]), // Masked by list array
            Some(vec![]),     // Masked by list array
            Some(vec![Some(3), None]),
            Some(vec![Some(4), Some(5), None, Some(6)]), // Masked by struct array
            None,
            None,
        ]);
        let a1_values = a1.values().clone();
        let a1 = Arc::new(a1) as ArrayRef;

        let a2 = Arc::new(Int32Array::from_iter(vec![
            Some(1), // Masked by list array
            Some(2), // Masked by list array
            None,
            Some(4), // Masked by struct array
            Some(5),
            None,
        ])) as ArrayRef;
        let a2_values = a2.clone();

        let field_a1 = Arc::new(Field::new("list", a1.data_type().clone(), true));
        let field_a2 = Arc::new(Field::new("integers", a2.data_type().clone(), true));

        let nulls = Buffer::from([0b00110111]);
        let struct_a = Arc::new(StructArray::from((
            vec![(field_a1, a1), (field_a2, a2)],
            nulls,
        ))) as ArrayRef;

        let offsets = Buffer::from_iter([0_i32, 0, 2, 2, 3, 5, 5]);
        let nulls = Buffer::from([0b00111100]);

        let list_type = DataType::List(Arc::new(Field::new(
            "struct",
            struct_a.data_type().clone(),
            true,
        )));

        let data = ArrayDataBuilder::new(list_type.clone())
            .len(6)
            .null_bit_buffer(Some(nulls))
            .add_buffer(offsets)
            .add_child_data(struct_a.into_data())
            .build()
            .unwrap();

        let list = make_array(data);
        let list_field = Field::new("col", list_type, true);

        let expected = vec![
            r#""#.to_string(),
            r#""#.to_string(),
            r#"[]"#.to_string(),
            r#"[{list: [3, ], integers: }]"#.to_string(),
            r#"[, {list: , integers: 5}]"#.to_string(),
            r#"[]"#.to_string(),
        ];

        let actual: Vec<_> = (0..6)
            .map(|x| array_value_to_string(&list, x).unwrap())
            .collect();
        assert_eq!(actual, expected);

        let levels = calculate_array_levels(&list, &list_field).unwrap();

        assert_eq!(levels.len(), 2);

        let a1_logical_nulls = a1_values.logical_nulls();
        let expected_level = ArrayLevels {
            def_levels: LevelData::Materialized(vec![0, 0, 1, 6, 5, 2, 3, 1]),
            rep_levels: LevelData::Materialized(vec![0, 0, 0, 0, 2, 0, 1, 0]),
            values: ValueSelection::from_indices(vec![1]),
            max_def_level: 6,
            max_rep_level: 2,
            array: a1_values,
            logical_nulls: a1_logical_nulls,
        };

        assert_eq!(&levels[0], &expected_level);

        let a2_logical_nulls = a2_values.logical_nulls();
        let expected_level = ArrayLevels {
            def_levels: LevelData::Materialized(vec![0, 0, 1, 3, 2, 4, 1]),
            rep_levels: LevelData::Materialized(vec![0, 0, 0, 0, 0, 1, 0]),
            values: ValueSelection::from_indices(vec![4]),
            max_def_level: 4,
            max_rep_level: 1,
            array: a2_values,
            logical_nulls: a2_logical_nulls,
        };

        assert_eq!(&levels[1], &expected_level);
    }

    #[test]
    fn test_fixed_size_list() {
        // [[1, 2], null, null, [7, 8], null]
        let mut builder = FixedSizeListBuilder::new(Int32Builder::new(), 2);
        builder.values().append_slice(&[1, 2]);
        builder.append(true);
        builder.values().append_slice(&[3, 4]);
        builder.append(false);
        builder.values().append_slice(&[5, 6]);
        builder.append(false);
        builder.values().append_slice(&[7, 8]);
        builder.append(true);
        builder.values().append_slice(&[9, 10]);
        builder.append(false);
        let a = builder.finish();
        let values = a.values().clone();

        let item_field = Field::new_list_field(a.data_type().clone(), true);
        let mut builder = levels(&item_field, a);
        builder.write(1..4);
        let levels = builder.finish();

        assert_eq!(levels.len(), 1);

        let list_level = &levels[0];

        let logical_nulls = values.logical_nulls();
        let expected_level = ArrayLevels {
            def_levels: LevelData::Materialized(vec![0, 0, 3, 3]),
            rep_levels: LevelData::Materialized(vec![0, 0, 0, 1]),
            values: ValueSelection::from_indices(vec![6, 7]),
            max_def_level: 3,
            max_rep_level: 1,
            array: values,
            logical_nulls,
        };
        assert_eq!(list_level, &expected_level);
    }

    #[test]
    fn test_fixed_size_list_of_struct() {
        // define schema
        let field_a = Field::new("a", DataType::Int32, true);
        let field_b = Field::new("b", DataType::Int64, false);
        let fields = Fields::from([Arc::new(field_a), Arc::new(field_b)]);
        let item_field = Field::new_list_field(DataType::Struct(fields.clone()), true);
        let list_field = Field::new(
            "list",
            DataType::FixedSizeList(Arc::new(item_field), 2),
            true,
        );

        let builder_a = Int32Builder::with_capacity(10);
        let builder_b = Int64Builder::with_capacity(10);
        let struct_builder =
            StructBuilder::new(fields, vec![Box::new(builder_a), Box::new(builder_b)]);
        let mut list_builder = FixedSizeListBuilder::new(struct_builder, 2);

        // [
        //   [{a: 1, b: 2}, null],
        //   null,
        //   [null, null],
        //   [{a: null, b: 3}, {a: 2, b: 4}]
        // ]

        // [{a: 1, b: 2}, null]
        let values = list_builder.values();
        // {a: 1, b: 2}
        values
            .field_builder::<Int32Builder>(0)
            .unwrap()
            .append_value(1);
        values
            .field_builder::<Int64Builder>(1)
            .unwrap()
            .append_value(2);
        values.append(true);
        // null
        values
            .field_builder::<Int32Builder>(0)
            .unwrap()
            .append_null();
        values
            .field_builder::<Int64Builder>(1)
            .unwrap()
            .append_value(0);
        values.append(false);
        list_builder.append(true);

        // null
        let values = list_builder.values();
        // null
        values
            .field_builder::<Int32Builder>(0)
            .unwrap()
            .append_null();
        values
            .field_builder::<Int64Builder>(1)
            .unwrap()
            .append_value(0);
        values.append(false);
        // null
        values
            .field_builder::<Int32Builder>(0)
            .unwrap()
            .append_null();
        values
            .field_builder::<Int64Builder>(1)
            .unwrap()
            .append_value(0);
        values.append(false);
        list_builder.append(false);

        // [null, null]
        let values = list_builder.values();
        // null
        values
            .field_builder::<Int32Builder>(0)
            .unwrap()
            .append_null();
        values
            .field_builder::<Int64Builder>(1)
            .unwrap()
            .append_value(0);
        values.append(false);
        // null
        values
            .field_builder::<Int32Builder>(0)
            .unwrap()
            .append_null();
        values
            .field_builder::<Int64Builder>(1)
            .unwrap()
            .append_value(0);
        values.append(false);
        list_builder.append(true);

        // [{a: null, b: 3}, {a: 2, b: 4}]
        let values = list_builder.values();
        // {a: null, b: 3}
        values
            .field_builder::<Int32Builder>(0)
            .unwrap()
            .append_null();
        values
            .field_builder::<Int64Builder>(1)
            .unwrap()
            .append_value(3);
        values.append(true);
        // {a: 2, b: 4}
        values
            .field_builder::<Int32Builder>(0)
            .unwrap()
            .append_value(2);
        values
            .field_builder::<Int64Builder>(1)
            .unwrap()
            .append_value(4);
        values.append(true);
        list_builder.append(true);

        let array = Arc::new(list_builder.finish());

        assert_eq!(array.values().len(), 8);
        assert_eq!(array.len(), 4);

        let struct_values = array.values().as_struct();
        let values_a = struct_values.column(0).clone();
        let values_b = struct_values.column(1).clone();

        let schema = Arc::new(Schema::new(vec![list_field]));
        let rb = RecordBatch::try_new(schema, vec![array]).unwrap();

        let levels = calculate_array_levels(rb.column(0), rb.schema().field(0)).unwrap();
        let a_levels = &levels[0];
        let b_levels = &levels[1];

        // [[{a: 1}, null], null, [null, null], [{a: null}, {a: 2}]]
        let values_a_logical_nulls = values_a.logical_nulls();
        let expected_a = ArrayLevels {
            def_levels: LevelData::Materialized(vec![4, 2, 0, 2, 2, 3, 4]),
            rep_levels: LevelData::Materialized(vec![0, 1, 0, 0, 1, 0, 1]),
            values: ValueSelection::from_indices(vec![0, 7]),
            max_def_level: 4,
            max_rep_level: 1,
            array: values_a,
            logical_nulls: values_a_logical_nulls,
        };
        // [[{b: 2}, null], null, [null, null], [{b: 3}, {b: 4}]]
        let values_b_logical_nulls = values_b.logical_nulls();
        let expected_b = ArrayLevels {
            def_levels: LevelData::Materialized(vec![3, 2, 0, 2, 2, 3, 3]),
            rep_levels: LevelData::Materialized(vec![0, 1, 0, 0, 1, 0, 1]),
            values: ValueSelection::from_indices(vec![0, 6, 7]),
            max_def_level: 3,
            max_rep_level: 1,
            array: values_b,
            logical_nulls: values_b_logical_nulls,
        };

        assert_eq!(a_levels, &expected_a);
        assert_eq!(b_levels, &expected_b);
    }

    #[test]
    fn test_fixed_size_list_empty() {
        let mut builder = FixedSizeListBuilder::new(Int32Builder::new(), 0);
        builder.append(true);
        builder.append(false);
        builder.append(true);
        let array = builder.finish();
        let values = array.values().clone();

        let item_field = Field::new_list_field(array.data_type().clone(), true);
        let mut builder = levels(&item_field, array);
        builder.write(0..3);
        let levels = builder.finish();

        assert_eq!(levels.len(), 1);

        let list_level = &levels[0];

        let logical_nulls = values.logical_nulls();
        let expected_level = ArrayLevels {
            def_levels: LevelData::Materialized(vec![1, 0, 1]),
            rep_levels: LevelData::Materialized(vec![0, 0, 0]),
            values: ValueSelection::from_indices(vec![]),
            max_def_level: 3,
            max_rep_level: 1,
            array: values,
            logical_nulls,
        };
        assert_eq!(list_level, &expected_level);
    }

    #[test]
    fn test_fixed_size_list_of_var_lists() {
        // [[[1, null, 3], null], [[4], []], [[5, 6], [null, null]], null]
        let mut builder = FixedSizeListBuilder::new(ListBuilder::new(Int32Builder::new()), 2);
        builder.values().append_value([Some(1), None, Some(3)]);
        builder.values().append_null();
        builder.append(true);
        builder.values().append_value([Some(4)]);
        builder.values().append_value([]);
        builder.append(true);
        builder.values().append_value([Some(5), Some(6)]);
        builder.values().append_value([None, None]);
        builder.append(true);
        builder.values().append_null();
        builder.values().append_null();
        builder.append(false);
        let a = builder.finish();
        let values = a.values().as_list::<i32>().values().clone();

        let item_field = Field::new_list_field(a.data_type().clone(), true);
        let mut builder = levels(&item_field, a);
        builder.write(0..4);
        let levels = builder.finish();

        let logical_nulls = values.logical_nulls();
        let expected_level = ArrayLevels {
            def_levels: LevelData::Materialized(vec![5, 4, 5, 2, 5, 3, 5, 5, 4, 4, 0]),
            rep_levels: LevelData::Materialized(vec![0, 2, 2, 1, 0, 1, 0, 2, 1, 2, 0]),
            values: ValueSelection::from_indices(vec![0, 2, 3, 4, 5]),
            max_def_level: 5,
            max_rep_level: 2,
            array: values,
            logical_nulls,
        };

        assert_eq!(levels[0], expected_level);
    }

    #[test]
    fn test_null_dictionary_values() {
        let values = Int32Array::new(
            vec![1, 2, 3, 4].into(),
            Some(NullBuffer::from(vec![true, false, true, true])),
        );
        let keys = Int32Array::new(
            vec![1, 54, 2, 0].into(),
            Some(NullBuffer::from(vec![true, false, true, true])),
        );
        // [NULL, NULL, 3, 0]
        let dict = DictionaryArray::new(keys, Arc::new(values));

        let item_field = Field::new_list_field(dict.data_type().clone(), true);

        let mut builder = levels(&item_field, dict.clone());
        builder.write(0..4);
        let levels = builder.finish();

        let logical_nulls = dict.logical_nulls();
        let expected_level = ArrayLevels {
            def_levels: LevelData::Materialized(vec![0, 0, 1, 1]),
            rep_levels: LevelData::Absent,
            values: ValueSelection::from_indices(vec![2, 3]),
            max_def_level: 1,
            max_rep_level: 0,
            array: Arc::new(dict),
            logical_nulls,
        };
        assert_eq!(levels[0], expected_level);
    }

    #[test]
    fn mismatched_types() {
        let array = Arc::new(Int32Array::from_iter(0..10)) as ArrayRef;
        let field = Field::new_list_field(DataType::Float64, false);

        let err = LevelInfoBuilder::try_new(&field, Default::default(), &array)
            .unwrap_err()
            .to_string();

        assert_eq!(
            err,
            "Arrow: Incompatible type. Field 'item' has type Float64, array has type Int32",
        );
    }

    fn levels<T: Array + 'static>(field: &Field, array: T) -> LevelInfoBuilder {
        let v = Arc::new(array) as ArrayRef;
        LevelInfoBuilder::try_new(field, Default::default(), &v).unwrap()
    }

    #[test]
    fn test_slice_for_chunk_flat() {
        // Case 1: required field (max_def_level=0, no def/rep levels stored).
        // Array has 6 values; all are non-null so Dense{0,6} covers every position.
        // value_offset=2, num_values=3 → Dense start=0+2=2, compact array [3,4,5],
        // values rebased to Dense{0,3} → materialized_indices = [0,1,2].
        let array: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6]));
        let logical_nulls = array.logical_nulls();
        let levels = ArrayLevels {
            def_levels: LevelData::Absent,
            rep_levels: LevelData::Absent,
            values: ValueSelection::from_indices(vec![0, 1, 2, 3, 4, 5]),
            max_def_level: 0,
            max_rep_level: 0,
            array,
            logical_nulls,
        };
        let sliced = levels.slice_for_chunk(&CdcChunk {
            level_offset: 0,
            num_levels: 0,
            value_offset: 2,
            num_values: 3,
        });
        assert!(matches!(sliced.def_levels, LevelData::Absent));
        assert!(matches!(sliced.rep_levels, LevelData::Absent));
        assert_eq!(sliced.materialized_indices(), vec![0, 1, 2]);
        assert_eq!(sliced.array.len(), 3);

        // Case 2: optional field (max_def_level=1, def levels present, no rep levels).
        // Array: [Some(1), None, Some(3), None, Some(5), Some(6)]
        // values: Sparse([0, 2, 4, 5])  (array positions of the four non-null values)
        // def_levels: [1, 0, 1, 0, 1, 1]
        //
        // Chunk: level_offset=1, num_levels=3, value_offset=1, num_values=1.
        //   - sel = values[1..2] = [2]  → non-null value at array position 2
        //   - compact array: slice(2, 1) = [Some(3)], materialized index 0
        let array: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            None,
            Some(3),
            None,
            Some(5),
            Some(6),
        ]));
        let logical_nulls = array.logical_nulls();
        let levels = ArrayLevels {
            def_levels: LevelData::Materialized(vec![1, 0, 1, 0, 1, 1]),
            rep_levels: LevelData::Absent,
            values: ValueSelection::from_indices(vec![0, 2, 4, 5]),
            max_def_level: 1,
            max_rep_level: 0,
            array,
            logical_nulls,
        };
        let sliced = levels.slice_for_chunk(&CdcChunk {
            level_offset: 1,
            num_levels: 3,
            value_offset: 1,
            num_values: 1,
        });
        assert_eq!(sliced.def_levels, LevelData::Materialized(vec![0, 1, 0]));
        assert!(matches!(sliced.rep_levels, LevelData::Absent));
        assert_eq!(sliced.materialized_indices(), vec![0]); // [2] shifted by -2 (sel[0])
        assert_eq!(sliced.array.len(), 1);
    }

    #[test]
    fn test_slice_for_chunk_nested_with_nulls() {
        // Regression test for https://github.com/apache/arrow-rs/issues/9637
        //
        // Simulates a List<Int32?> where null list entries have non-zero child
        // ranges (valid per Arrow spec: "a null value may correspond to a
        // non-empty segment in the child array"). This creates gaps in the
        // leaf array that don't correspond to any levels.
        //
        // 5 rows with 2 null list entries owning non-empty child ranges:
        //   row 0: [1]       → leaf[0]
        //   row 1: null list → owns leaf[1..3] (gap of 2)
        //   row 2: [2, null] → leaf[3], leaf[4]=null element
        //   row 3: null list → owns leaf[5..8] (gap of 3)
        //   row 4: [4, 5]   → leaf[8], leaf[9]
        //
        // def_levels: [3,  0,  3, 2,  0,  3, 3]
        // rep_levels: [0,  0,  0, 1,  0,  0, 1]
        // non_null_indices: [0, 3, 8, 9]
        //   gaps in array: 0→3 (skip 1,2), 3→8 (skip 5,6,7)
        let array: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1), // 0: row 0
            None,    // 1: gap (null list row 1)
            None,    // 2: gap (null list row 1)
            Some(2), // 3: row 2
            None,    // 4: row 2, null element
            None,    // 5: gap (null list row 3)
            None,    // 6: gap (null list row 3)
            None,    // 7: gap (null list row 3)
            Some(4), // 8: row 4
            Some(5), // 9: row 4
        ]));
        let logical_nulls = array.logical_nulls();
        let levels = ArrayLevels {
            def_levels: LevelData::Materialized(vec![3, 0, 3, 2, 0, 3, 3]),
            rep_levels: LevelData::Materialized(vec![0, 0, 0, 1, 0, 0, 1]),
            values: ValueSelection::from_indices(vec![0, 3, 8, 9]),
            max_def_level: 3,
            max_rep_level: 1,
            array,
            logical_nulls,
        };

        // Chunk 0: rows 0-1, value[0]=1 → sel[0]=[0], compact array [0..1]
        let chunk0 = levels.slice_for_chunk(&CdcChunk {
            level_offset: 0,
            num_levels: 2,
            value_offset: 0,
            num_values: 1,
        });
        assert_eq!(chunk0.materialized_indices(), vec![0]);
        assert_eq!(chunk0.array.len(), 1);

        // Chunk 1: rows 2-3, value[1]=2 → sel[1]=[3], compact array [3..4]
        let chunk1 = levels.slice_for_chunk(&CdcChunk {
            level_offset: 2,
            num_levels: 3,
            value_offset: 1,
            num_values: 1,
        });
        assert_eq!(chunk1.materialized_indices(), vec![0]);
        assert_eq!(chunk1.array.len(), 1);

        // Chunk 2: row 4, values[2..4]=[8,9] → compact array [8..10]
        let chunk2 = levels.slice_for_chunk(&CdcChunk {
            level_offset: 5,
            num_levels: 2,
            value_offset: 2,
            num_values: 2,
        });
        assert_eq!(chunk2.materialized_indices(), vec![0, 1]);
        assert_eq!(chunk2.array.len(), 2);
    }

    #[test]
    fn test_slice_for_chunk_all_null() {
        // All-null chunk: num_values=0 → zero-length array.
        let array: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None, None, Some(4)]));
        let logical_nulls = array.logical_nulls();
        let levels = ArrayLevels {
            def_levels: LevelData::Materialized(vec![1, 0, 0, 1]),
            rep_levels: LevelData::Absent,
            values: ValueSelection::from_indices(vec![0, 3]),
            max_def_level: 1,
            max_rep_level: 0,
            array,
            logical_nulls,
        };
        // Chunk covering only the two null rows (levels 1..3), zero non-null values.
        let sliced = levels.slice_for_chunk(&CdcChunk {
            level_offset: 1,
            num_levels: 2,
            value_offset: 1,
            num_values: 0,
        });
        assert_eq!(sliced.def_levels, LevelData::Materialized(vec![0, 0]));
        assert_eq!(sliced.materialized_indices(), Vec::<usize>::new());
        assert_eq!(sliced.array.len(), 0);
    }

    #[test]
    fn test_slice_for_chunk_uniform_levels_and_dense_values() {
        let array: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6]));
        let logical_nulls = array.logical_nulls();
        let levels = ArrayLevels {
            def_levels: LevelData::Uniform { value: 1, count: 6 },
            rep_levels: LevelData::Absent,
            values: ValueSelection::Dense { offset: 0, len: 6 },
            max_def_level: 1,
            max_rep_level: 0,
            array,
            logical_nulls,
        };

        let sliced = levels.slice_for_chunk(&CdcChunk {
            level_offset: 2,
            num_levels: 3,
            value_offset: 2,
            num_values: 3,
        });

        assert_eq!(sliced.def_levels, LevelData::Uniform { value: 1, count: 3 });
        assert!(matches!(sliced.rep_levels, LevelData::Absent));
        assert_eq!(sliced.values, ValueSelection::Dense { offset: 0, len: 3 });
        assert_eq!(sliced.array.len(), 3);
    }

    #[test]
    fn test_all_null_list() {
        // A list where every slot is null — hits the all-null fast path in write_list.
        let leaf_field = Field::new_list_field(DataType::Int32, false);
        let list_type = DataType::List(Arc::new(leaf_field));

        let leaf_array = Int32Array::from(Vec::<i32>::new());
        let offsets = Buffer::from_iter([0_i32, 0, 0, 0]);
        let null_bitmap = Buffer::from([0b00000000_u8]); // all null
        let list = ArrayDataBuilder::new(list_type.clone())
            .len(3)
            .add_buffer(offsets)
            .add_child_data(leaf_array.to_data())
            .null_bit_buffer(Some(null_bitmap))
            .build()
            .unwrap();
        let list = make_array(list);

        let list_field = Field::new("list", list_type, true);
        let levels = calculate_array_levels(&list, &list_field).unwrap();
        assert_eq!(levels.len(), 1);

        let expected = ArrayLevels {
            def_levels: LevelData::Uniform { value: 0, count: 3 },
            rep_levels: LevelData::Uniform { value: 0, count: 3 },
            values: ValueSelection::Empty,
            max_def_level: 2,
            max_rep_level: 1,
            array: Arc::new(leaf_array),
            logical_nulls: None,
        };
        assert_eq!(&levels[0], &expected);
    }

    #[test]
    fn test_all_null_struct() {
        // Struct<Int32> where every struct slot is null.
        // Schema: a (struct, nullable) -> c (int32, nullable)
        // Data: [null, null, null, null]
        //
        // Expected: max_def=2, def_levels all 0 (struct is null → child never reached),
        // leaf values are empty.
        let c = Int32Array::from(vec![None::<i32>; 4]);
        let leaf = Arc::new(c) as ArrayRef;
        let c_field = Arc::new(Field::new("c", DataType::Int32, true));
        let a = StructArray::from((vec![(c_field, leaf.clone())], Buffer::from([0b00000000])));
        let a_field = Field::new("a", a.data_type().clone(), true);
        let a_array = Arc::new(a) as ArrayRef;

        let levels = calculate_array_levels(&a_array, &a_field).unwrap();
        assert_eq!(levels.len(), 1);

        let expected = ArrayLevels {
            def_levels: LevelData::Uniform { value: 0, count: 4 },
            rep_levels: LevelData::Absent,
            values: ValueSelection::Empty,
            max_def_level: 2,
            max_rep_level: 0,
            array: leaf,
            logical_nulls: Some(NullBuffer::new_null(4)),
        };
        assert_eq!(&levels[0], &expected);
    }

    #[test]
    fn test_all_null_fixed_size_list() {
        // A fixed-size list where every slot is null. Hits the all-null fast path
        // in write_fixed_size_list.
        let mut builder = FixedSizeListBuilder::new(Int32Builder::new(), 2);
        builder.values().append_slice(&[0, 0]);
        builder.append(false);
        builder.values().append_slice(&[0, 0]);
        builder.append(false);
        builder.values().append_slice(&[0, 0]);
        builder.append(false);
        let a = builder.finish();
        let values = a.values().clone();

        let item_field = Field::new_list_field(a.data_type().clone(), true);
        let levels = calculate_array_levels(&(Arc::new(a) as ArrayRef), &item_field).unwrap();
        assert_eq!(levels.len(), 1);

        let logical_nulls = values.logical_nulls();
        let expected = ArrayLevels {
            def_levels: LevelData::Uniform { value: 0, count: 3 },
            rep_levels: LevelData::Uniform { value: 0, count: 3 },
            values: ValueSelection::Empty,
            max_def_level: 3,
            max_rep_level: 1,
            array: values,
            logical_nulls,
        };
        assert_eq!(&levels[0], &expected);
    }

    #[test]
    fn test_non_nullable_field_with_nulls_in_array() {
        // A field declared non-nullable but the Arrow array physically has nulls.
        // This produces def_levels: Absent (max_def_level == 0) with logical_nulls: Some.
        let array = Arc::new(Int32Array::from_iter([Some(1), None, Some(3)])) as ArrayRef;
        let field = Field::new("item", DataType::Int32, false);

        let logical_nulls = array.logical_nulls();
        let levels = calculate_array_levels(&array, &field).unwrap();
        assert_eq!(levels.len(), 1);

        let expected = ArrayLevels {
            def_levels: LevelData::Absent,
            rep_levels: LevelData::Absent,
            values: ValueSelection::from_indices(vec![0, 2]),
            max_def_level: 0,
            max_rep_level: 0,
            array,
            logical_nulls,
        };
        assert_eq!(&levels[0], &expected);
    }

    #[test]
    fn test_list_view_nullable() {
        // [[1, 2], null, [], [3]]
        let leaf_field = Arc::new(Field::new_list_field(DataType::Int32, false));
        let values = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let a = ListViewArray::new(
            leaf_field.clone(),
            vec![0, 0, 0, 2].into(),
            vec![2, 0, 0, 1].into(),
            values.clone(),
            Some(vec![true, false, true, true].into()),
        );

        let list_field = Field::new("list", DataType::ListView(leaf_field), true);
        let levels = calculate_array_levels(&(Arc::new(a) as ArrayRef), &list_field).unwrap();
        assert_eq!(levels.len(), 1);

        let expected = ArrayLevels {
            def_levels: LevelData::Materialized(vec![2, 2, 0, 1, 2]),
            rep_levels: LevelData::Materialized(vec![0, 1, 0, 0, 0]),
            values: ValueSelection::from_indices(vec![0, 1, 2]),
            max_def_level: 2,
            max_rep_level: 1,
            array: values as ArrayRef,
            logical_nulls: None,
        };
        assert_eq!(&levels[0], &expected);
    }

    #[test]
    fn test_list_view_non_null() {
        // [[1, 2], [], [3]]
        let leaf_field = Arc::new(Field::new_list_field(DataType::Int32, false));
        let values = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let a = ListViewArray::new(
            leaf_field.clone(),
            vec![0, 0, 2].into(),
            vec![2, 0, 1].into(),
            values.clone(),
            None,
        );

        let list_field = Field::new("list", DataType::ListView(leaf_field), false);
        let levels = calculate_array_levels(&(Arc::new(a) as ArrayRef), &list_field).unwrap();
        assert_eq!(levels.len(), 1);

        let expected = ArrayLevels {
            def_levels: LevelData::Materialized(vec![1, 1, 0, 1]),
            rep_levels: LevelData::Materialized(vec![0, 1, 0, 0]),
            values: ValueSelection::from_indices(vec![0, 1, 2]),
            max_def_level: 1,
            max_rep_level: 1,
            array: values as ArrayRef,
            logical_nulls: None,
        };
        assert_eq!(&levels[0], &expected);
    }

    #[test]
    fn test_all_null_nested_struct() {
        // Struct<Struct<Int32>> where the outer struct is entirely null.
        // Schema: a (struct, nullable) -> b (struct, nullable) -> c (int32, nullable)
        // Data: [null, null, null]
        //
        // Expected: max_def=3, def_levels all 0.
        let c = Int32Array::from(vec![None::<i32>; 3]);
        let leaf = Arc::new(c) as ArrayRef;
        let c_field = Arc::new(Field::new("c", DataType::Int32, true));
        let b = StructArray::from((vec![(c_field, leaf.clone())], Buffer::from([0b00000000])));
        let b_field = Arc::new(Field::new("b", b.data_type().clone(), true));
        let a = StructArray::from((
            vec![(b_field, Arc::new(b) as ArrayRef)],
            Buffer::from([0b00000000]),
        ));
        let a_field = Field::new("a", a.data_type().clone(), true);
        let a_array = Arc::new(a) as ArrayRef;

        let levels = calculate_array_levels(&a_array, &a_field).unwrap();
        assert_eq!(levels.len(), 1);

        let expected = ArrayLevels {
            def_levels: LevelData::Uniform { value: 0, count: 3 },
            rep_levels: LevelData::Absent,
            values: ValueSelection::Empty,
            max_def_level: 3,
            max_rep_level: 0,
            array: leaf,
            logical_nulls: Some(NullBuffer::new_null(3)),
        };
        assert_eq!(&levels[0], &expected);
    }

    #[test]
    fn test_level_data_uniform_materialized_eq() {
        let uniform = LevelData::Uniform { value: 1, count: 3 };
        let materialized = LevelData::Materialized(vec![1, 1, 1]);
        assert_eq!(uniform, materialized);
        assert_eq!(materialized, uniform);

        // Mismatch
        let different = LevelData::Materialized(vec![1, 2, 1]);
        assert_ne!(uniform, different);
    }

    #[test]
    fn test_value_selection_dense_sparse_eq() {
        let dense = ValueSelection::Dense { offset: 2, len: 3 };
        let sparse = ValueSelection::Sparse(vec![2, 3, 4]);
        assert_eq!(dense, sparse);
        assert_eq!(sparse, dense);

        // Mismatch
        let non_contiguous = ValueSelection::Sparse(vec![2, 4, 5]);
        assert_ne!(dense, non_contiguous);
    }

    #[test]
    fn test_level_data_append_run_zero_count() {
        let mut data = LevelData::Uniform { value: 1, count: 3 };
        data.append_run(1, 0);
        assert_eq!(data, LevelData::Uniform { value: 1, count: 3 });

        let mut materialized = LevelData::Materialized(vec![1, 2]);
        materialized.append_run(3, 0);
        assert_eq!(materialized, LevelData::Materialized(vec![1, 2]));
    }

    #[test]
    fn test_level_data_absent_materialize_is_none() {
        let mut absent = LevelData::Absent;
        assert!(absent.materialize_mut().is_none());
    }

    #[test]
    fn test_value_selection_append_range_empty() {
        let mut sel = ValueSelection::Dense { offset: 0, len: 3 };
        sel.append_range(0..0);
        assert_eq!(sel, ValueSelection::Dense { offset: 0, len: 3 });
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "unsorted dense indices")]
    fn test_value_selection_from_indices_unsorted_panics() {
        // [0, 2, 1, 3]: first=0, last=3, end+1-start == len (4), so it enters the
        // Dense arm, but the middle elements are out of order triggering the debug_assert.
        ValueSelection::from_indices(vec![0, 2, 1, 3]);
    }

    #[test]
    fn test_all_null_struct_multiple_children() {
        // Struct with two leaf children, entirely null.
        // Schema: a (struct, nullable) -> { c1 (int32, nullable), c2 (int32, nullable) }
        // Data: [null, null]
        //
        // Both leaf columns should get uniform def_levels=0.
        let c1 = Arc::new(Int32Array::from(vec![None::<i32>; 2])) as ArrayRef;
        let c2 = Arc::new(Int32Array::from(vec![None::<i32>; 2])) as ArrayRef;
        let c1_field = Arc::new(Field::new("c1", DataType::Int32, true));
        let c2_field = Arc::new(Field::new("c2", DataType::Int32, true));
        let a = StructArray::from((
            vec![(c1_field, c1.clone()), (c2_field, c2.clone())],
            Buffer::from([0b00000000]),
        ));
        let a_field = Field::new("a", a.data_type().clone(), true);
        let a_array = Arc::new(a) as ArrayRef;

        let levels = calculate_array_levels(&a_array, &a_field).unwrap();
        assert_eq!(levels.len(), 2);

        for (i, leaf) in [c1, c2].into_iter().enumerate() {
            let expected = ArrayLevels {
                def_levels: LevelData::Uniform { value: 0, count: 2 },
                rep_levels: LevelData::Absent,
                values: ValueSelection::Empty,
                max_def_level: 2,
                max_rep_level: 0,
                array: leaf,
                logical_nulls: Some(NullBuffer::new_null(2)),
            };
            assert_eq!(&levels[i], &expected, "leaf {i} mismatch");
        }
    }
}
