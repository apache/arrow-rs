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

use crate::errors::{ParquetError, Result};
use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, OffsetSizeTrait};
use arrow_buffer::{NullBuffer, OffsetBuffer};
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
    /// A struct array
    Struct(Vec<LevelInfoBuilder>, LevelContext, Option<NullBuffer>),
}

impl LevelInfoBuilder {
    /// Create a new [`LevelInfoBuilder`] for the given [`Field`] and parent [`LevelContext`]
    fn try_new(field: &Field, parent_ctx: LevelContext, array: &ArrayRef) -> Result<Self> {
        if field.data_type() != array.data_type() {
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
            | DataType::FixedSizeList(child, _) => {
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
            | LevelInfoBuilder::FixedSizeList(v, _, _, _) => v.finish(),
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
        let offsets = &offsets[range.start..range.end + 1];

        let write_non_null_slice =
            |child: &mut LevelInfoBuilder, start_idx: usize, end_idx: usize| {
                child.write(start_idx..end_idx);
                child.visit_leaves(|leaf| {
                    let rep_levels = leaf.rep_levels.as_mut().unwrap();
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
                let rep_levels = leaf.rep_levels.as_mut().unwrap();
                rep_levels.push(ctx.rep_level - 1);
                let def_levels = leaf.def_levels.as_mut().unwrap();
                def_levels.push(ctx.def_level - 1);
            })
        };

        let write_null_slice = |child: &mut LevelInfoBuilder| {
            child.visit_leaves(|leaf| {
                let rep_levels = leaf.rep_levels.as_mut().unwrap();
                rep_levels.push(ctx.rep_level - 1);
                let def_levels = leaf.def_levels.as_mut().unwrap();
                def_levels.push(ctx.def_level - 2);
            })
        };

        match nulls {
            Some(nulls) => {
                let null_offset = range.start;
                // TODO: Faster bitmask iteration (#1757)
                for (idx, w) in offsets.windows(2).enumerate() {
                    let is_valid = nulls.is_valid(idx + null_offset);
                    let start_idx = w[0].as_usize();
                    let end_idx = w[1].as_usize();
                    if !is_valid {
                        write_null_slice(child)
                    } else if start_idx == end_idx {
                        write_empty_slice(child)
                    } else {
                        write_non_null_slice(child, start_idx, end_idx)
                    }
                }
            }
            None => {
                for w in offsets.windows(2) {
                    let start_idx = w[0].as_usize();
                    let end_idx = w[1].as_usize();
                    if start_idx == end_idx {
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
        let write_null = |children: &mut [LevelInfoBuilder], range: Range<usize>| {
            for child in children {
                child.visit_leaves(|info| {
                    let len = range.end - range.start;

                    let def_levels = info.def_levels.as_mut().unwrap();
                    def_levels.extend(std::iter::repeat(ctx.def_level - 1).take(len));

                    if let Some(rep_levels) = info.rep_levels.as_mut() {
                        rep_levels.extend(std::iter::repeat(ctx.rep_level).take(len));
                    }
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
        let write_non_null = |child: &mut LevelInfoBuilder, start_idx: usize, end_idx: usize| {
            let values_start = start_idx * fixed_size;
            let values_end = end_idx * fixed_size;
            child.write(values_start..values_end);

            child.visit_leaves(|leaf| {
                let rep_levels = leaf.rep_levels.as_mut().unwrap();

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
                let rep_levels = leaf.rep_levels.as_mut().unwrap();
                rep_levels.extend(std::iter::repeat(ctx.rep_level - 1).take(len));
                let def_levels = leaf.def_levels.as_mut().unwrap();
                def_levels.extend(std::iter::repeat(ctx.def_level - 1).take(len));
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
                            let rep_levels = leaf.rep_levels.as_mut().unwrap();
                            rep_levels.push(ctx.rep_level - 1);
                            let def_levels = leaf.def_levels.as_mut().unwrap();
                            def_levels.push(ctx.def_level - 2);
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

        match &mut info.def_levels {
            Some(def_levels) => {
                def_levels.reserve(len);
                info.non_null_indices.reserve(len);

                match info.array.logical_nulls() {
                    Some(nulls) => {
                        // TODO: Faster bitmask iteration (#1757)
                        for i in range {
                            match nulls.is_valid(i) {
                                true => {
                                    def_levels.push(info.max_def_level);
                                    info.non_null_indices.push(i)
                                }
                                false => def_levels.push(info.max_def_level - 1),
                            }
                        }
                    }
                    None => {
                        let iter = std::iter::repeat(info.max_def_level).take(len);
                        def_levels.extend(iter);
                        info.non_null_indices.extend(range);
                    }
                }
            }
            None => info.non_null_indices.extend(range),
        }

        if let Some(rep_levels) = &mut info.rep_levels {
            rep_levels.extend(std::iter::repeat(info.max_rep_level).take(len))
        }
    }

    /// Visits all children of this node in depth first order
    fn visit_leaves(&mut self, visit: impl Fn(&mut ArrayLevels) + Copy) {
        match self {
            LevelInfoBuilder::Primitive(info) => visit(info),
            LevelInfoBuilder::List(c, _, _, _)
            | LevelInfoBuilder::LargeList(c, _, _, _)
            | LevelInfoBuilder::FixedSizeList(c, _, _, _) => c.visit_leaves(visit),
            LevelInfoBuilder::Struct(children, _, _) => {
                for c in children {
                    c.visit_leaves(visit)
                }
            }
        }
    }
}
/// The data necessary to write a primitive Arrow array to parquet, taking into account
/// any non-primitive parents it may have in the arrow representation
#[derive(Debug, Clone)]
pub(crate) struct ArrayLevels {
    /// Array's definition levels
    ///
    /// Present if `max_def_level != 0`
    def_levels: Option<Vec<i16>>,

    /// Array's optional repetition levels
    ///
    /// Present if `max_rep_level != 0`
    rep_levels: Option<Vec<i16>>,

    /// The corresponding array identifying non-null slices of data
    /// from the primitive array
    non_null_indices: Vec<usize>,

    /// The maximum definition level for this leaf column
    max_def_level: i16,

    /// The maximum repetition for this leaf column
    max_rep_level: i16,

    /// The arrow array
    array: ArrayRef,
}

impl PartialEq for ArrayLevels {
    fn eq(&self, other: &Self) -> bool {
        self.def_levels == other.def_levels
            && self.rep_levels == other.rep_levels
            && self.non_null_indices == other.non_null_indices
            && self.max_def_level == other.max_def_level
            && self.max_rep_level == other.max_rep_level
            && self.array.as_ref() == other.array.as_ref()
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

        Self {
            def_levels: (max_def_level != 0).then(Vec::new),
            rep_levels: (max_rep_level != 0).then(Vec::new),
            non_null_indices: vec![],
            max_def_level,
            max_rep_level,
            array,
        }
    }

    pub fn array(&self) -> &ArrayRef {
        &self.array
    }

    pub fn def_levels(&self) -> Option<&[i16]> {
        self.def_levels.as_deref()
    }

    pub fn rep_levels(&self) -> Option<&[i16]> {
        self.rep_levels.as_deref()
    }

    pub fn non_null_indices(&self) -> &[usize] {
        &self.non_null_indices
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

        let leaf_type = Field::new("item", DataType::Int32, false);
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
            def_levels: Some(vec![2; 10]),
            rep_levels: Some(vec![0, 2, 2, 1, 2, 2, 2, 0, 1, 2]),
            non_null_indices: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            max_def_level: 2,
            max_rep_level: 2,
            array: Arc::new(primitives),
        };
        assert_eq!(&levels[0], &expected);
    }

    #[test]
    fn test_calculate_one_level_1() {
        // This test calculates the levels for a non-null primitive array
        let array = Arc::new(Int32Array::from_iter(0..10)) as ArrayRef;
        let field = Field::new("item", DataType::Int32, false);

        let levels = calculate_array_levels(&array, &field).unwrap();
        assert_eq!(levels.len(), 1);

        let expected_levels = ArrayLevels {
            def_levels: None,
            rep_levels: None,
            non_null_indices: (0..10).collect(),
            max_def_level: 0,
            max_rep_level: 0,
            array,
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
        let field = Field::new("item", DataType::Int32, true);

        let levels = calculate_array_levels(&array, &field).unwrap();
        assert_eq!(levels.len(), 1);

        let expected_levels = ArrayLevels {
            def_levels: Some(vec![1, 0, 1, 1, 0]),
            rep_levels: None,
            non_null_indices: vec![0, 2, 3],
            max_def_level: 1,
            max_rep_level: 0,
            array,
        };
        assert_eq!(&levels[0], &expected_levels);
    }

    #[test]
    fn test_calculate_array_levels_1() {
        let leaf_field = Field::new("item", DataType::Int32, false);
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
            def_levels: Some(vec![1; 5]),
            rep_levels: Some(vec![0; 5]),
            non_null_indices: (0..5).collect(),
            max_def_level: 1,
            max_rep_level: 1,
            array: Arc::new(leaf_array),
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
            def_levels: Some(vec![2, 2, 0, 2, 2, 2, 2, 2, 2, 2, 2, 2]),
            rep_levels: Some(vec![0, 1, 0, 0, 1, 0, 1, 1, 1, 0, 1, 1]),
            non_null_indices: (0..11).collect(),
            max_def_level: 2,
            max_rep_level: 1,
            array: Arc::new(leaf_array),
        };
        assert_eq!(&levels[0], &expected_levels);
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
            def_levels: Some(vec![0, 2, 0, 3, 3, 3, 3, 3, 3, 3]),
            rep_levels: Some(vec![0, 0, 0, 0, 1, 1, 1, 0, 1, 1]),
            non_null_indices: (4..11).collect(),
            max_def_level: 3,
            max_rep_level: 1,
            array: Arc::new(leaf),
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
            def_levels: Some(vec![
                5, 5, 5, 5, 1, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5,
            ]),
            rep_levels: Some(vec![
                0, 2, 1, 2, 0, 0, 2, 1, 2, 0, 2, 1, 2, 1, 2, 1, 2, 0, 2, 1, 2, 1, 2,
            ]),
            non_null_indices: (0..22).collect(),
            max_def_level: 5,
            max_rep_level: 2,
            array: Arc::new(leaf),
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
            def_levels: Some(vec![1; 4]),
            rep_levels: Some(vec![0; 4]),
            non_null_indices: (0..4).collect(),
            max_def_level: 1,
            max_rep_level: 1,
            array: Arc::new(leaf),
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
            def_levels: Some(vec![1, 3, 3, 3, 3, 3, 3, 3]),
            rep_levels: Some(vec![0, 0, 1, 1, 0, 1, 0, 1]),
            non_null_indices: (0..7).collect(),
            max_def_level: 3,
            max_rep_level: 1,
            array: Arc::new(leaf),
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
            def_levels: Some(vec![1, 5, 5, 5, 4, 5, 5, 5, 5, 5, 5, 5, 4, 5, 5, 5, 5, 5]),
            rep_levels: Some(vec![0, 0, 1, 2, 1, 0, 2, 2, 1, 2, 2, 2, 0, 1, 2, 2, 2, 2]),
            non_null_indices: (0..15).collect(),
            max_def_level: 5,
            max_rep_level: 2,
            array: Arc::new(leaf),
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

        let expected_levels = ArrayLevels {
            def_levels: Some(vec![3, 2, 3, 1, 0, 3]),
            rep_levels: None,
            non_null_indices: vec![0, 2, 5],
            max_def_level: 3,
            max_rep_level: 0,
            array: leaf,
        };
        assert_eq!(&levels[0], &expected_levels);
    }

    #[test]
    fn list_single_column() {
        // this tests the level generation from the arrow_writer equivalent test

        let a_values = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let a_value_offsets = arrow::buffer::Buffer::from_iter([0_i32, 1, 3, 3, 6, 10]);
        let a_list_type = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        let a_list_data = ArrayData::builder(a_list_type.clone())
            .len(5)
            .add_buffer(a_value_offsets)
            .null_bit_buffer(Some(Buffer::from([0b00011011])))
            .add_child_data(a_values.to_data())
            .build()
            .unwrap();

        assert_eq!(a_list_data.null_count(), 1);

        let a = ListArray::from(a_list_data);

        let item_field = Field::new("item", a_list_type, true);
        let mut builder = levels(&item_field, a);
        builder.write(2..4);
        let levels = builder.finish();

        assert_eq!(levels.len(), 1);

        let list_level = &levels[0];

        let expected_level = ArrayLevels {
            def_levels: Some(vec![0, 3, 3, 3]),
            rep_levels: Some(vec![0, 0, 1, 1]),
            non_null_indices: vec![3, 4, 5],
            max_def_level: 3,
            max_rep_level: 1,
            array: Arc::new(a_values),
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
            def_levels: None,
            rep_levels: None,
            non_null_indices: vec![0, 1, 2, 3, 4],
            max_def_level: 0,
            max_rep_level: 0,
            array: Arc::new(a),
        };
        assert_eq!(list_level, &expected_level);

        // test "b" levels
        let list_level = levels.get(1).unwrap();

        let expected_level = ArrayLevels {
            def_levels: Some(vec![1, 0, 0, 1, 1]),
            rep_levels: None,
            non_null_indices: vec![0, 3, 4],
            max_def_level: 1,
            max_rep_level: 0,
            array: Arc::new(b),
        };
        assert_eq!(list_level, &expected_level);

        // test "d" levels
        let list_level = levels.get(2).unwrap();

        let expected_level = ArrayLevels {
            def_levels: Some(vec![1, 1, 1, 2, 1]),
            rep_levels: None,
            non_null_indices: vec![3],
            max_def_level: 2,
            max_rep_level: 0,
            array: Arc::new(d),
        };
        assert_eq!(list_level, &expected_level);

        // test "f" levels
        let list_level = levels.get(3).unwrap();

        let expected_level = ArrayLevels {
            def_levels: Some(vec![3, 2, 3, 2, 3]),
            rep_levels: None,
            non_null_indices: vec![0, 2, 4],
            max_def_level: 3,
            max_rep_level: 0,
            array: Arc::new(f),
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

        // test key levels
        let list_level = &levels[0];

        let expected_level = ArrayLevels {
            def_levels: Some(vec![1; 7]),
            rep_levels: Some(vec![0, 1, 0, 1, 0, 1, 1]),
            non_null_indices: vec![0, 1, 2, 3, 4, 5, 6],
            max_def_level: 1,
            max_rep_level: 1,
            array: map.keys().clone(),
        };
        assert_eq!(list_level, &expected_level);

        // test values levels
        let list_level = levels.get(1).unwrap();

        let expected_level = ArrayLevels {
            def_levels: Some(vec![2, 2, 2, 1, 2, 1, 2]),
            rep_levels: Some(vec![0, 1, 0, 1, 0, 1, 1]),
            non_null_indices: vec![0, 1, 2, 4, 6],
            max_def_level: 2,
            max_rep_level: 1,
            array: map.values().clone(),
        };
        assert_eq!(list_level, &expected_level);
    }

    #[test]
    fn test_list_of_struct() {
        // define schema
        let int_field = Field::new("a", DataType::Int32, true);
        let fields = Fields::from([Arc::new(int_field)]);
        let item_field = Field::new("item", DataType::Struct(fields.clone()), true);
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

        let expected_level = ArrayLevels {
            def_levels: Some(vec![4, 1, 0, 2, 2, 3, 4]),
            rep_levels: Some(vec![0, 0, 0, 0, 1, 0, 0]),
            non_null_indices: vec![0, 4],
            max_def_level: 4,
            max_rep_level: 1,
            array: values,
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

        let expected_level = ArrayLevels {
            def_levels: Some(vec![4, 4, 3, 2, 0, 4, 4, 0, 1]),
            rep_levels: Some(vec![0, 1, 0, 0, 0, 0, 1, 0, 0]),
            non_null_indices: vec![0, 1, 5, 6],
            max_def_level: 4,
            max_rep_level: 1,
            array: values,
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

        let expected_level = ArrayLevels {
            def_levels: Some(vec![0, 0, 1, 6, 5, 2, 3, 1]),
            rep_levels: Some(vec![0, 0, 0, 0, 2, 0, 1, 0]),
            non_null_indices: vec![1],
            max_def_level: 6,
            max_rep_level: 2,
            array: a1_values,
        };

        assert_eq!(&levels[0], &expected_level);

        let expected_level = ArrayLevels {
            def_levels: Some(vec![0, 0, 1, 3, 2, 4, 1]),
            rep_levels: Some(vec![0, 0, 0, 0, 0, 1, 0]),
            non_null_indices: vec![4],
            max_def_level: 4,
            max_rep_level: 1,
            array: a2_values,
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

        let item_field = Field::new("item", a.data_type().clone(), true);
        let mut builder = levels(&item_field, a);
        builder.write(1..4);
        let levels = builder.finish();

        assert_eq!(levels.len(), 1);

        let list_level = &levels[0];

        let expected_level = ArrayLevels {
            def_levels: Some(vec![0, 0, 3, 3]),
            rep_levels: Some(vec![0, 0, 0, 1]),
            non_null_indices: vec![6, 7],
            max_def_level: 3,
            max_rep_level: 1,
            array: values,
        };
        assert_eq!(list_level, &expected_level);
    }

    #[test]
    fn test_fixed_size_list_of_struct() {
        // define schema
        let field_a = Field::new("a", DataType::Int32, true);
        let field_b = Field::new("b", DataType::Int64, false);
        let fields = Fields::from([Arc::new(field_a), Arc::new(field_b)]);
        let item_field = Field::new("item", DataType::Struct(fields.clone()), true);
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
        let expected_a = ArrayLevels {
            def_levels: Some(vec![4, 2, 0, 2, 2, 3, 4]),
            rep_levels: Some(vec![0, 1, 0, 0, 1, 0, 1]),
            non_null_indices: vec![0, 7],
            max_def_level: 4,
            max_rep_level: 1,
            array: values_a,
        };
        // [[{b: 2}, null], null, [null, null], [{b: 3}, {b: 4}]]
        let expected_b = ArrayLevels {
            def_levels: Some(vec![3, 2, 0, 2, 2, 3, 3]),
            rep_levels: Some(vec![0, 1, 0, 0, 1, 0, 1]),
            non_null_indices: vec![0, 6, 7],
            max_def_level: 3,
            max_rep_level: 1,
            array: values_b,
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

        let item_field = Field::new("item", array.data_type().clone(), true);
        let mut builder = levels(&item_field, array);
        builder.write(0..3);
        let levels = builder.finish();

        assert_eq!(levels.len(), 1);

        let list_level = &levels[0];

        let expected_level = ArrayLevels {
            def_levels: Some(vec![1, 0, 1]),
            rep_levels: Some(vec![0, 0, 0]),
            non_null_indices: vec![],
            max_def_level: 3,
            max_rep_level: 1,
            array: values,
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

        let item_field = Field::new("item", a.data_type().clone(), true);
        let mut builder = levels(&item_field, a);
        builder.write(0..4);
        let levels = builder.finish();

        let expected_level = ArrayLevels {
            def_levels: Some(vec![5, 4, 5, 2, 5, 3, 5, 5, 4, 4, 0]),
            rep_levels: Some(vec![0, 2, 2, 1, 0, 1, 0, 2, 1, 2, 0]),
            non_null_indices: vec![0, 2, 3, 4, 5],
            max_def_level: 5,
            max_rep_level: 2,
            array: values,
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

        let item_field = Field::new("item", dict.data_type().clone(), true);

        let mut builder = levels(&item_field, dict.clone());
        builder.write(0..4);
        let levels = builder.finish();
        let expected_level = ArrayLevels {
            def_levels: Some(vec![0, 0, 1, 1]),
            rep_levels: None,
            non_null_indices: vec![2, 3],
            max_def_level: 1,
            max_rep_level: 0,
            array: Arc::new(dict),
        };
        assert_eq!(levels[0], expected_level);
    }

    #[test]
    fn mismatched_types() {
        let array = Arc::new(Int32Array::from_iter(0..10)) as ArrayRef;
        let field = Field::new("item", DataType::Float64, false);

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
}
