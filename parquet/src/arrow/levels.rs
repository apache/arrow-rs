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

use arrow::array::{make_array, ArrayRef, MapArray, StructArray};
use arrow::datatypes::{DataType, Field};

/// Keeps track of the level information per array that is needed to write an Arrow array to Parquet.
///
/// When a nested schema is traversed, intermediate [LevelInfo] structs are created to track
/// the state of parent arrays. When a primitive Arrow array is encountered, a final [LevelInfo]
/// is created, and this is what is used to index into the array when writing data to Parquet.
#[derive(Debug, Eq, PartialEq, Clone)]
pub(crate) struct LevelInfo {
    /// Array's definition levels
    pub definition: Vec<i16>,
    /// Array's optional repetition levels
    pub repetition: Option<Vec<i16>>,
    /// Array's offsets, 64-bit is used to accommodate large offset arrays
    pub array_offsets: Vec<i64>,
    // TODO: Convert to an Arrow Buffer after ARROW-10766 is merged.
    /// Array's logical validity mask, whcih gets unpacked for list children.
    /// If the parent of an array is null, all children are logically treated as
    /// null. This mask keeps track of that.
    ///
    pub array_mask: Vec<bool>,
    /// The maximum definition at this level, 0 at the record batch
    pub max_definition: i16,
    /// The type of array represented by this level info
    pub level_type: LevelType,
    /// The offset of the current level's array
    pub offset: usize,
    /// The length of the current level's array
    pub length: usize,
}

/// LevelType defines the type of level, and whether it is nullable or not
#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub(crate) enum LevelType {
    Root,
    List(bool),
    Struct(bool),
    Primitive(bool),
}

impl LevelType {
    #[inline]
    const fn level_increment(&self) -> i16 {
        match self {
            LevelType::Root => 0,
            // List repetition adds a constant 1
            LevelType::List(is_nullable) => 1 + *is_nullable as i16,
            LevelType::Struct(is_nullable) | LevelType::Primitive(is_nullable) => {
                *is_nullable as i16
            }
        }
    }
}

impl LevelInfo {
    /// Create a new [LevelInfo] by filling `length` slots, and setting an initial offset.
    ///
    /// This is a convenience function to populate the starting point of the traversal.
    pub(crate) fn new(offset: usize, length: usize) -> Self {
        Self {
            // a batch has no definition level yet
            definition: vec![0; length],
            // a batch has no repetition as it is not a list
            repetition: None,
            // a batch has sequential offsets, should be num_rows + 1
            array_offsets: (0..=(length as i64)).collect(),
            // all values at a batch-level are non-null
            array_mask: vec![true; length],
            max_definition: 0,
            level_type: LevelType::Root,
            offset,
            length,
        }
    }

    /// Compute nested levels of the Arrow array, recursing into lists and structs.
    ///
    /// Returns a list of `LevelInfo`, where each level is for nested primitive arrays.
    ///
    /// The parent struct's nullness is tracked, as it determines whether the child
    /// max_definition should be incremented.
    /// The 'is_parent_struct' variable asks "is this field's parent a struct?".
    /// * If we are starting at a [RecordBatch], this is `false`.
    /// * If we are calculating a list's child, this is `false`.
    /// * If we are calculating a struct (i.e. `field.data_type90 == Struct`),
    /// this depends on whether the struct is a child of a struct.
    /// * If we are calculating a field inside a [StructArray], this is 'true'.
    pub(crate) fn calculate_array_levels(
        &self,
        array: &ArrayRef,
        field: &Field,
    ) -> Vec<Self> {
        let (array_offsets, array_mask) =
            Self::get_array_offsets_and_masks(array, self.offset, self.length);
        match array.data_type() {
            DataType::Null => vec![Self {
                definition: self.definition.clone(),
                repetition: self.repetition.clone(),
                array_offsets,
                array_mask,
                max_definition: self.max_definition.max(1),
                // Null type is always nullable
                level_type: LevelType::Primitive(true),
                offset: self.offset,
                length: self.length,
            }],
            DataType::Boolean
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
            | DataType::Decimal(_, _)
            | DataType::FixedSizeBinary(_) => {
                // we return a vector of 1 value to represent the primitive
                vec![self.calculate_child_levels(
                    array_offsets,
                    array_mask,
                    LevelType::Primitive(field.is_nullable()),
                )]
            }
            DataType::List(list_field) | DataType::LargeList(list_field) => {
                let child_offset = array_offsets[0] as usize;
                let child_len = *array_offsets.last().unwrap() as usize;
                // Calculate the list level
                let list_level = self.calculate_child_levels(
                    array_offsets,
                    array_mask,
                    LevelType::List(field.is_nullable()),
                );

                // Construct the child array of the list, and get its offset + mask
                let array_data = array.data();
                let child_data = array_data.child_data().get(0).unwrap();
                let child_array = make_array(child_data.clone());
                let (child_offsets, child_mask) = Self::get_array_offsets_and_masks(
                    &child_array,
                    child_offset,
                    child_len - child_offset,
                );

                match child_array.data_type() {
                    // TODO: The behaviour of a <list<null>> is untested
                    DataType::Null => vec![list_level],
                    DataType::Boolean
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
                    | DataType::Timestamp(_, _)
                    | DataType::Date32
                    | DataType::Date64
                    | DataType::Time32(_)
                    | DataType::Time64(_)
                    | DataType::Duration(_)
                    | DataType::Interval(_)
                    | DataType::Binary
                    | DataType::LargeBinary
                    | DataType::Utf8
                    | DataType::LargeUtf8
                    | DataType::Dictionary(_, _)
                    | DataType::Decimal(_, _)
                    | DataType::FixedSizeBinary(_) => {
                        vec![list_level.calculate_child_levels(
                            child_offsets,
                            child_mask,
                            LevelType::Primitive(list_field.is_nullable()),
                        )]
                    }
                    DataType::List(_)
                    | DataType::LargeList(_)
                    | DataType::Struct(_)
                    | DataType::Map(_, _) => {
                        list_level.calculate_array_levels(&child_array, list_field)
                    }
                    DataType::FixedSizeList(_, _) => unimplemented!(),
                    DataType::Union(_, _) => unimplemented!(),
                }
            }
            DataType::Map(map_field, _) => {
                // Calculate the map level
                let map_level = self.calculate_child_levels(
                    array_offsets,
                    array_mask,
                    // A map is treated like a list as it has repetition
                    LevelType::List(field.is_nullable()),
                );

                let map_array = array.as_any().downcast_ref::<MapArray>().unwrap();

                let key_array = map_array.keys();
                let value_array = map_array.values();

                if let DataType::Struct(fields) = map_field.data_type() {
                    let key_field = &fields[0];
                    let value_field = &fields[1];

                    let mut map_levels = vec![];

                    // Get key levels
                    let mut key_levels =
                        map_level.calculate_array_levels(&key_array, key_field);
                    map_levels.append(&mut key_levels);

                    let mut value_levels =
                        map_level.calculate_array_levels(&value_array, value_field);
                    map_levels.append(&mut value_levels);

                    map_levels
                } else {
                    panic!(
                        "Map field should be a struct, found {:?}",
                        map_field.data_type()
                    );
                }
            }
            DataType::FixedSizeList(_, _) => unimplemented!(),
            DataType::Struct(struct_fields) => {
                let struct_array: &StructArray = array
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .expect("Unable to get struct array");
                let mut struct_level = self.calculate_child_levels(
                    array_offsets,
                    array_mask,
                    LevelType::Struct(field.is_nullable()),
                );

                // If the parent field is a list, calculate the children of the struct as if it
                // were a list as well.
                if matches!(self.level_type, LevelType::List(_)) {
                    struct_level.level_type = LevelType::List(false);
                }

                let mut struct_levels = vec![];
                struct_array
                    .columns()
                    .into_iter()
                    .zip(struct_fields)
                    .for_each(|(child_array, child_field)| {
                        let mut levels =
                            struct_level.calculate_array_levels(child_array, child_field);
                        struct_levels.append(&mut levels);
                    });
                struct_levels
            }
            DataType::Union(_, _) => unimplemented!(),
            DataType::Dictionary(_, _) => {
                // Need to check for these cases not implemented in C++:
                // - "Writing DictionaryArray with nested dictionary type not yet supported"
                // - "Writing DictionaryArray with null encoded in dictionary type not yet supported"
                // vec![self.get_primitive_def_levels(array, field, array_mask)]
                vec![self.calculate_child_levels(
                    array_offsets,
                    array_mask,
                    LevelType::Primitive(field.is_nullable()),
                )]
            }
        }
    }

    /// Calculate child/leaf array levels.
    ///
    /// The algorithm works by incrementing definitions of array values based on whether:
    /// - a value is optional or required (is_nullable)
    /// - a list value is repeated + optional or required (is_list)
    ///
    /// A record batch always starts at a populated definition = level 0.
    /// When a batch only has a primitive, i.e. `<batch<primitive[a]>>, column `a`
    /// can only have a maximum level of 1 if it is not null.
    /// If it is not null, we increment by 1, such that the null slots will = level 1.
    /// The above applies to types that have no repetition (anything not a list or map).
    ///
    /// If a batch has lists, then we increment by up to 2 levels:
    /// - 1 level for the list (repeated)
    /// - 1 level if the list itself is nullable (optional)
    ///
    /// A list's child then gets incremented using the above rules.
    ///
    /// *Exceptions*
    ///
    /// There are 2 exceptions from the above rules:
    ///
    /// 1. When at the root of the schema: We always increment the
    /// level regardless of whether the child is nullable or not. If we do not do
    /// this, we could have a non-nullable array having a definition of 0.
    ///
    /// 2. List parent, non-list child: We always increment the level in this case,
    /// regardless of whether the child is nullable or not.
    ///
    /// *Examples*
    ///
    /// A batch with only a primitive that's non-nullable. `<primitive[required]>`:
    /// * We don't increment the definition level as the array is not optional.
    /// * This would leave us with a definition of 0, so the first exception applies.
    /// * The definition level becomes 1.
    ///
    /// A batch with only a primitive that's nullable. `<primitive[optional]>`:
    /// * The definition level becomes 1, as we increment it once.
    ///
    /// A batch with a single non-nullable list (both list and child not null):
    /// * We calculate the level twice, for the list, and for the child.
    /// * At the list, the level becomes 1, where 0 indicates that the list is
    ///  empty, and 1 says it's not (determined through offsets).
    /// * At the primitive level, the second exception applies. The level becomes 2.
    fn calculate_child_levels(
        &self,
        // we use 64-bit offsets to also accommodate large arrays
        array_offsets: Vec<i64>,
        array_mask: Vec<bool>,
        level_type: LevelType,
    ) -> Self {
        let min_len = *(array_offsets.last().unwrap()) as usize;
        let mut definition = Vec::with_capacity(min_len);
        let mut repetition = Vec::with_capacity(min_len);
        let mut merged_array_mask = Vec::with_capacity(min_len);

        let max_definition = match (self.level_type, level_type) {
            // Handle the illegal cases
            (_, LevelType::Root) => {
                unreachable!("Cannot have a root as a child")
            }
            (LevelType::Primitive(_), _) => {
                unreachable!("Cannot have a primitive parent for any type")
            }
            // The general case
            (_, _) => self.max_definition + level_type.level_increment(),
        };

        match (self.level_type, level_type) {
            (LevelType::List(_), LevelType::List(is_nullable)) => {
                // Parent is a list or descendant of a list, and child is a list
                let reps = self.repetition.clone().unwrap();

                // List is null, and not empty
                let l1 = max_definition - is_nullable as i16;
                // List is not null, but is empty
                let l2 = max_definition - 1;
                // List is not null, and not empty
                let l3 = max_definition;

                let mut nulls_seen = 0;

                self.array_offsets.windows(2).for_each(|w| {
                    let start = w[0] as usize;
                    let end = w[1] as usize;
                    let parent_len = end - start;

                    if parent_len == 0 {
                        // If the parent length is 0, there won't be a slot for the child
                        let index = start + nulls_seen - self.offset;
                        definition.push(self.definition[index]);
                        repetition.push(0);
                        merged_array_mask.push(self.array_mask[index]);
                        nulls_seen += 1;
                    } else {
                        (start..end).for_each(|parent_index| {
                            let index = parent_index + nulls_seen - self.offset;
                            let parent_index = parent_index - self.offset;

                            // parent is either defined at this level, or earlier
                            let parent_def = self.definition[index];
                            let parent_rep = reps[index];
                            let parent_mask = self.array_mask[index];

                            // valid parent, index into children
                            let child_start = array_offsets[parent_index] as usize;
                            let child_end = array_offsets[parent_index + 1] as usize;
                            let child_len = child_end - child_start;
                            let child_mask = array_mask[parent_index];
                            let merged_mask = parent_mask && child_mask;

                            if child_len == 0 {
                                // Empty slot, i.e. {"parent": {"child": [] } }
                                // Nullness takes priority over emptiness
                                definition.push(if child_mask { l2 } else { l1 });
                                repetition.push(parent_rep);
                                merged_array_mask.push(merged_mask);
                            } else {
                                (child_start..child_end).for_each(|child_index| {
                                    let rep = match (
                                        parent_index == start,
                                        child_index == child_start,
                                    ) {
                                        (true, true) => parent_rep,
                                        (true, false) => parent_rep + 2,
                                        (false, true) => parent_rep,
                                        (false, false) => parent_rep + 1,
                                    };

                                    definition.push(if !parent_mask {
                                        parent_def
                                    } else if child_mask {
                                        l3
                                    } else {
                                        l1
                                    });
                                    repetition.push(rep);
                                    merged_array_mask.push(merged_mask);
                                });
                            }
                        });
                    }
                });

                debug_assert_eq!(definition.len(), merged_array_mask.len());

                let offset = *array_offsets.first().unwrap() as usize;
                let length = *array_offsets.last().unwrap() as usize - offset;

                Self {
                    definition,
                    repetition: Some(repetition),
                    array_offsets,
                    array_mask: merged_array_mask,
                    max_definition,
                    level_type,
                    offset: offset + self.offset,
                    length,
                }
            }
            (LevelType::List(_), _) => {
                // List and primitive (or struct).
                // The list can have more values than the primitive, indicating that there
                // are slots where the list is empty. We use a counter to track this behaviour.
                let mut nulls_seen = 0;

                // let child_max_definition = list_max_definition + is_nullable as i16;
                // child values are a function of parent list offsets
                let reps = self.repetition.as_deref().unwrap();
                self.array_offsets.windows(2).for_each(|w| {
                    let start = w[0] as usize;
                    let end = w[1] as usize;
                    let parent_len = end - start;

                    if parent_len == 0 {
                        let index = start + nulls_seen - self.offset;
                        definition.push(self.definition[index]);
                        repetition.push(reps[index]);
                        merged_array_mask.push(self.array_mask[index]);
                        nulls_seen += 1;
                    } else {
                        // iterate through the array, adjusting child definitions for nulls
                        (start..end).for_each(|child_index| {
                            let index = child_index + nulls_seen - self.offset;
                            let child_mask = array_mask[child_index - self.offset];
                            let parent_mask = self.array_mask[index];
                            let parent_def = self.definition[index];

                            if !parent_mask || parent_def < self.max_definition {
                                definition.push(parent_def);
                                repetition.push(reps[index]);
                                merged_array_mask.push(parent_mask);
                            } else {
                                definition.push(max_definition - !child_mask as i16);
                                repetition.push(reps[index]);
                                merged_array_mask.push(child_mask);
                            }
                        });
                    }
                });

                debug_assert_eq!(definition.len(), merged_array_mask.len());

                let offset = *array_offsets.first().unwrap() as usize;
                let length = *array_offsets.last().unwrap() as usize - offset;

                Self {
                    definition,
                    repetition: Some(repetition),
                    array_offsets: self.array_offsets.clone(),
                    array_mask: merged_array_mask,
                    max_definition,
                    level_type,
                    offset: offset + self.offset,
                    length,
                }
            }
            (_, LevelType::List(is_nullable)) => {
                // Encountering a list for the first time.
                // Calculate the 2 list hierarchy definitions in advance

                // List is null, and not empty
                let l1 = max_definition - 1 - is_nullable as i16;
                // List is not null, but is empty
                let l2 = max_definition - 1;
                // List is not null, and not empty
                let l3 = max_definition;

                self.definition
                    .iter()
                    .enumerate()
                    .for_each(|(parent_index, def)| {
                        let child_from = array_offsets[parent_index];
                        let child_to = array_offsets[parent_index + 1];
                        let child_len = child_to - child_from;
                        let child_mask = array_mask[parent_index];
                        let parent_mask = self.array_mask[parent_index];

                        match (parent_mask, child_len) {
                            (true, 0) => {
                                // Empty slot, i.e. {"parent": {"child": [] } }
                                // Nullness takes priority over emptiness
                                definition.push(if child_mask { l2 } else { l1 });
                                repetition.push(0);
                                merged_array_mask.push(child_mask);
                            }
                            (false, 0) => {
                                // Inherit the parent definition as parent was null
                                definition.push(*def);
                                repetition.push(0);
                                merged_array_mask.push(child_mask);
                            }
                            (true, _) => {
                                (child_from..child_to).for_each(|child_index| {
                                    // l1 and l3 make sense as list is not empty,
                                    // but we reflect that it's either null or not
                                    definition.push(if child_mask { l3 } else { l1 });
                                    // Mark the first child slot as 0, and the next as 1
                                    repetition.push(if child_index == child_from {
                                        0
                                    } else {
                                        1
                                    });
                                    merged_array_mask.push(child_mask);
                                });
                            }
                            (false, _) => {
                                (child_from..child_to).for_each(|child_index| {
                                    // Inherit the parent definition as parent was null
                                    definition.push(*def);
                                    // mark the first child slot as 0, and the next as 1
                                    repetition.push(if child_index == child_from {
                                        0
                                    } else {
                                        1
                                    });
                                    merged_array_mask.push(false);
                                });
                            }
                        }
                    });

                debug_assert_eq!(definition.len(), merged_array_mask.len());

                let offset = *array_offsets.first().unwrap() as usize;
                let length = *array_offsets.last().unwrap() as usize - offset;

                Self {
                    definition,
                    repetition: Some(repetition),
                    array_offsets,
                    array_mask: merged_array_mask,
                    max_definition,
                    level_type,
                    offset,
                    length,
                }
            }
            (_, _) => {
                self.definition
                    .iter()
                    .zip(array_mask.into_iter().zip(&self.array_mask))
                    .for_each(|(current_def, (child_mask, parent_mask))| {
                        merged_array_mask.push(*parent_mask && child_mask);
                        match (parent_mask, child_mask) {
                            (true, true) => {
                                definition.push(max_definition);
                            }
                            (true, false) => {
                                // The child is only legally null if its array is nullable.
                                // Thus parent's max_definition is lower
                                definition.push(if *current_def <= self.max_definition {
                                    *current_def
                                } else {
                                    self.max_definition
                                });
                            }
                            // if the parent was false, retain its definitions
                            (false, _) => {
                                definition.push(*current_def);
                            }
                        }
                    });

                debug_assert_eq!(definition.len(), merged_array_mask.len());

                Self {
                    definition,
                    repetition: self.repetition.clone(), // it's None
                    array_offsets,
                    array_mask: merged_array_mask,
                    max_definition,
                    level_type,
                    // Inherit parent offset and length
                    offset: self.offset,
                    length: self.length,
                }
            }
        }
    }

    /// Get the offsets of an array as 64-bit values, and validity masks as booleans
    /// - Primitive, binary and struct arrays' offsets will be a sequence, masks obtained
    ///   from validity bitmap
    /// - List array offsets will be the value offsets, masks are computed from offsets
    fn get_array_offsets_and_masks(
        array: &ArrayRef,
        offset: usize,
        len: usize,
    ) -> (Vec<i64>, Vec<bool>) {
        match array.data_type() {
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
            | DataType::Timestamp(_, _)
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Duration(_)
            | DataType::Interval(_)
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Struct(_)
            | DataType::Dictionary(_, _)
            | DataType::Decimal(_, _) => {
                let array_mask = match array.data().null_buffer() {
                    Some(buf) => get_bool_array_slice(buf, array.offset() + offset, len),
                    None => vec![true; len],
                };
                ((0..=(len as i64)).collect(), array_mask)
            }
            DataType::List(_) | DataType::Map(_, _) => {
                let data = array.data();
                let offsets = unsafe { data.buffers()[0].typed_data::<i32>() };
                let offsets = offsets
                    .to_vec()
                    .into_iter()
                    .skip(offset)
                    .take(len + 1)
                    .map(|v| v as i64)
                    .collect::<Vec<i64>>();
                let array_mask = match array.data().null_buffer() {
                    Some(buf) => get_bool_array_slice(buf, array.offset() + offset, len),
                    None => vec![true; len],
                };
                (offsets, array_mask)
            }
            DataType::LargeList(_) => {
                let offsets = unsafe { array.data().buffers()[0].typed_data::<i64>() }
                    .iter()
                    .skip(offset)
                    .take(len + 1)
                    .copied()
                    .collect();
                let array_mask = match array.data().null_buffer() {
                    Some(buf) => get_bool_array_slice(buf, array.offset() + offset, len),
                    None => vec![true; len],
                };
                (offsets, array_mask)
            }
            DataType::FixedSizeBinary(value_len) => {
                let array_mask = match array.data().null_buffer() {
                    Some(buf) => get_bool_array_slice(buf, array.offset() + offset, len),
                    None => vec![true; len],
                };
                let value_len = *value_len as i64;
                (
                    (0..=(len as i64)).map(|v| v * value_len).collect(),
                    array_mask,
                )
            }
            DataType::FixedSizeList(_, _) | DataType::Union(_, _) => {
                unimplemented!("Getting offsets not yet implemented")
            }
        }
    }

    /// Given a level's information, calculate the offsets required to index an array correctly.
    pub(crate) fn filter_array_indices(&self) -> Vec<usize> {
        // happy path if not dealing with lists
        if self.repetition.is_none() {
            return self
                .definition
                .iter()
                .enumerate()
                .filter_map(|(i, def)| {
                    if *def == self.max_definition {
                        Some(i)
                    } else {
                        None
                    }
                })
                .collect();
        }

        let mut filtered = vec![];
        let mut definition_levels = self.definition.iter();
        let mut index = 0;

        for len in self.array_offsets.windows(2).map(|s| s[1] - s[0]) {
            if len == 0 {
                // Skip this definition level (it should be less than max_definition)
                definition_levels.next();
            } else {
                for (_, def) in (0..len).zip(&mut definition_levels) {
                    if *def == self.max_definition {
                        filtered.push(index);
                    }
                    index += 1;
                }
            }
        }

        filtered
    }
}

/// Convert an Arrow buffer to a boolean array slice
/// TODO: this was created for buffers, so might not work for bool array, might be slow too
#[inline]
fn get_bool_array_slice(
    buffer: &arrow::buffer::Buffer,
    offset: usize,
    len: usize,
) -> Vec<bool> {
    let data = buffer.as_slice();
    (offset..(len + offset))
        .map(|i| arrow::util::bit_util::get_bit(data, i))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use arrow::array::*;
    use arrow::buffer::Buffer;
    use arrow::datatypes::{Schema, ToByteSlice};
    use arrow::record_batch::RecordBatch;

    #[test]
    fn test_calculate_array_levels_twitter_example() {
        // based on the example at https://blog.twitter.com/engineering/en_us/a/2013/dremel-made-simple-with-parquet.html
        // [[a, b, c], [d, e, f, g]], [[h], [i,j]]
        let parent_levels = LevelInfo {
            definition: vec![0, 0],
            repetition: None,
            array_offsets: vec![0, 1, 2], // 2 records, root offsets always sequential
            array_mask: vec![true, true], // both lists defined
            max_definition: 0,
            level_type: LevelType::Root,
            offset: 0,
            length: 2,
        };
        // offset into array, each level1 has 2 values
        let array_offsets = vec![0, 2, 4];
        let array_mask = vec![true, true];

        // calculate level1 levels
        let levels = parent_levels.calculate_child_levels(
            array_offsets.clone(),
            array_mask,
            LevelType::List(false),
        );
        //
        let expected_levels = LevelInfo {
            definition: vec![1, 1, 1, 1],
            repetition: Some(vec![0, 1, 0, 1]),
            array_offsets,
            array_mask: vec![true, true, true, true],
            max_definition: 1,
            level_type: LevelType::List(false),
            offset: 0,
            length: 4,
        };
        // the separate asserts make it easier to see what's failing
        assert_eq!(&levels.definition, &expected_levels.definition);
        assert_eq!(&levels.repetition, &expected_levels.repetition);
        assert_eq!(&levels.array_mask, &expected_levels.array_mask);
        assert_eq!(&levels.array_offsets, &expected_levels.array_offsets);
        assert_eq!(&levels.max_definition, &expected_levels.max_definition);
        assert_eq!(&levels.level_type, &expected_levels.level_type);
        // this assert is to help if there are more variables added to the struct
        assert_eq!(&levels, &expected_levels);

        // level2
        let parent_levels = levels;
        let array_offsets = vec![0, 3, 7, 8, 10];
        let array_mask = vec![true, true, true, true];
        let levels = parent_levels.calculate_child_levels(
            array_offsets.clone(),
            array_mask,
            LevelType::List(false),
        );
        let expected_levels = LevelInfo {
            definition: vec![2, 2, 2, 2, 2, 2, 2, 2, 2, 2],
            repetition: Some(vec![0, 2, 2, 1, 2, 2, 2, 0, 1, 2]),
            array_offsets,
            array_mask: vec![true; 10],
            max_definition: 2,
            level_type: LevelType::List(false),
            offset: 0,
            length: 10,
        };
        assert_eq!(&levels.definition, &expected_levels.definition);
        assert_eq!(&levels.repetition, &expected_levels.repetition);
        assert_eq!(&levels.array_mask, &expected_levels.array_mask);
        assert_eq!(&levels.max_definition, &expected_levels.max_definition);
        assert_eq!(&levels.array_offsets, &expected_levels.array_offsets);
        assert_eq!(&levels.level_type, &expected_levels.level_type);
        assert_eq!(&levels, &expected_levels);
    }

    #[test]
    fn test_calculate_one_level_1() {
        // This test calculates the levels for a non-null primitive array
        let parent_levels = LevelInfo {
            definition: vec![0; 10],
            repetition: None,
            array_offsets: (0..=10).collect(),
            array_mask: vec![true; 10],
            max_definition: 0,
            level_type: LevelType::Root,
            offset: 0,
            length: 10,
        };
        let array_offsets: Vec<i64> = (0..=10).collect();
        let array_mask = vec![true; 10];

        let levels = parent_levels.calculate_child_levels(
            array_offsets.clone(),
            array_mask.clone(),
            LevelType::Primitive(false),
        );
        let expected_levels = LevelInfo {
            // As it is non-null, definitions can be omitted
            definition: vec![0; 10],
            repetition: None,
            array_offsets,
            array_mask,
            max_definition: 0,
            level_type: LevelType::Primitive(false),
            offset: 0,
            length: 10,
        };
        assert_eq!(&levels, &expected_levels);
    }

    #[test]
    fn test_calculate_one_level_2() {
        // This test calculates the levels for a non-null primitive array
        let parent_levels = LevelInfo {
            definition: vec![0; 5],
            repetition: None,
            array_offsets: (0..=5).collect(),
            array_mask: vec![true, true, true, true, true],
            max_definition: 0,
            level_type: LevelType::Root,
            offset: 0,
            length: 5,
        };
        let array_offsets: Vec<i64> = (0..=5).collect();
        let array_mask = vec![true, false, true, true, false];

        let levels = parent_levels.calculate_child_levels(
            array_offsets.clone(),
            array_mask.clone(),
            LevelType::Primitive(true),
        );
        let expected_levels = LevelInfo {
            definition: vec![1, 0, 1, 1, 0],
            repetition: None,
            array_offsets,
            array_mask,
            max_definition: 1,
            level_type: LevelType::Primitive(true),
            offset: 0,
            length: 5,
        };
        assert_eq!(&levels, &expected_levels);
    }

    #[test]
    fn test_calculate_array_levels_1() {
        // if all array values are defined (e.g. batch<list<_>>)
        // [[0], [1], [2], [3], [4]]
        let parent_levels = LevelInfo {
            definition: vec![0; 5],
            repetition: None,
            array_offsets: vec![0, 1, 2, 3, 4, 5],
            array_mask: vec![true, true, true, true, true],
            max_definition: 0,
            level_type: LevelType::Root,
            offset: 0,
            length: 5,
        };
        let array_offsets = vec![0, 2, 2, 4, 8, 11];
        let array_mask = vec![true, false, true, true, true];

        let levels = parent_levels.calculate_child_levels(
            array_offsets.clone(),
            array_mask,
            LevelType::List(true),
        );
        // array: [[0, 0], _1_, [2, 2], [3, 3, 3, 3], [4, 4, 4]]
        // all values are defined as we do not have nulls on the root (batch)
        // repetition:
        //   0: 0, 1
        //   1:
        //   2: 0, 1
        //   3: 0, 1, 1, 1
        //   4: 0, 1, 1
        let expected_levels = LevelInfo {
            // The levels are normally 2 because we:
            // - Calculate the level at the list
            // - Calculate the level at the list's child
            // We do not do this in these tests, thus the levels are 1 less.
            definition: vec![2, 2, 0, 2, 2, 2, 2, 2, 2, 2, 2, 2],
            repetition: Some(vec![0, 1, 0, 0, 1, 0, 1, 1, 1, 0, 1, 1]),
            array_offsets,
            array_mask: vec![
                true, true, false, true, true, true, true, true, true, true, true, true,
            ],
            max_definition: 2,
            level_type: LevelType::List(true),
            offset: 0,
            length: 11, // the child has 11 slots
        };
        assert_eq!(&levels.definition, &expected_levels.definition);
        assert_eq!(&levels.repetition, &expected_levels.repetition);
        assert_eq!(&levels.array_offsets, &expected_levels.array_offsets);
        assert_eq!(&levels.max_definition, &expected_levels.max_definition);
        assert_eq!(&levels.level_type, &expected_levels.level_type);
        assert_eq!(&levels, &expected_levels);
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
        let parent_levels = LevelInfo {
            definition: vec![0, 1, 0, 1, 1],
            repetition: None,
            array_offsets: vec![0, 1, 2, 3, 4, 5],
            array_mask: vec![false, true, false, true, true],
            max_definition: 1,
            level_type: LevelType::Struct(true),
            offset: 0,
            length: 5,
        };
        let array_offsets = vec![0, 2, 2, 4, 8, 11];
        let array_mask = vec![true, false, true, true, true];

        let levels = parent_levels.calculate_child_levels(
            array_offsets.clone(),
            array_mask,
            LevelType::List(true),
        );
        let expected_levels = LevelInfo {
            // 0 1 [2] are 0 (not defined at level 1)
            // [2] is 1, but has 0 slots so is not populated (defined at level 1 only)
            // 2 3 [4] are 0
            // 4 5 6 7 [8] are 1 (defined at level 1 only)
            // 8 9 10 [11] are 2 (defined at both levels)
            definition: vec![0, 0, 1, 0, 0, 3, 3, 3, 3, 3, 3, 3],
            repetition: Some(vec![0, 1, 0, 0, 1, 0, 1, 1, 1, 0, 1, 1]),
            array_offsets,
            array_mask: vec![
                false, false, false, false, false, true, true, true, true, true, true,
                true,
            ],
            max_definition: 3,
            level_type: LevelType::List(true),
            offset: 0,
            length: 11,
        };
        assert_eq!(&levels.definition, &expected_levels.definition);
        assert_eq!(&levels.repetition, &expected_levels.repetition);
        assert_eq!(&levels.array_offsets, &expected_levels.array_offsets);
        assert_eq!(&levels.max_definition, &expected_levels.max_definition);
        assert_eq!(&levels.level_type, &expected_levels.level_type);
        assert_eq!(&levels, &expected_levels);

        // nested lists (using previous test)
        let nested_parent_levels = levels;
        let array_offsets = vec![0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22];
        let array_mask = vec![
            true, true, true, true, true, true, true, true, true, true, true,
        ];
        let levels = nested_parent_levels.calculate_child_levels(
            array_offsets.clone(),
            array_mask,
            LevelType::List(true),
        );
        let expected_levels = LevelInfo {
            // (def: 0) 0 1 [2] are 0 (take parent)
            // (def: 0) 2 3 [4] are 0 (take parent)
            // (def: 0) 4 5 [6] are 0 (take parent)
            // (def: 0) 6 7 [8] are 0 (take parent)
            // (def: 1) 8 9 [10] are 1 (take parent)
            // (def: 1) 10 11 [12] are 1 (take parent)
            // (def: 1) 12 23 [14] are 1 (take parent)
            // (def: 1) 14 15 [16] are 1 (take parent)
            // (def: 2) 16 17 [18] are 2 (defined at all levels)
            // (def: 2) 18 19 [20] are 2 (defined at all levels)
            // (def: 2) 20 21 [22] are 2 (defined at all levels)
            //
            // 0 1 [2] are 0 (not defined at level 1)
            // [2] is 1, but has 0 slots so is not populated (defined at level 1 only)
            // 2 3 [4] are 0
            // 4 5 6 7 [8] are 1 (defined at level 1 only)
            // 8 9 10 [11] are 2 (defined at both levels)
            //
            // 0: [[100, 101], [102, 103]]
            // 1: []
            // 2: [[104, 105], [106, 107]]
            // 3: [[108, 109], [110, 111], [112, 113], [114, 115]]
            // 4: [[116, 117], [118, 119], [120, 121]]
            definition: vec![
                0, 0, 0, 0, 1, 0, 0, 0, 0, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5,
            ],
            repetition: Some(vec![
                0, 2, 1, 2, 0, 0, 2, 1, 2, 0, 2, 1, 2, 1, 2, 1, 2, 0, 2, 1, 2, 1, 2,
            ]),
            array_offsets,
            array_mask: vec![
                false, false, false, false, false, false, false, false, false, true,
                true, true, true, true, true, true, true, true, true, true, true, true,
                true,
            ],
            max_definition: 5,
            level_type: LevelType::List(true),
            offset: 0,
            length: 22,
        };
        assert_eq!(&levels.definition, &expected_levels.definition);
        assert_eq!(&levels.repetition, &expected_levels.repetition);
        assert_eq!(&levels.array_offsets, &expected_levels.array_offsets);
        assert_eq!(&levels.array_mask, &expected_levels.array_mask);
        assert_eq!(&levels.max_definition, &expected_levels.max_definition);
        assert_eq!(&levels.level_type, &expected_levels.level_type);
        assert_eq!(&levels, &expected_levels);
    }

    #[test]
    fn test_calculate_array_levels_nested_list() {
        // if all array values are defined (e.g. batch<list<_>>)
        // The array at this level looks like:
        // 0: [a]
        // 1: [a]
        // 2: [a]
        // 3: [a]
        let parent_levels = LevelInfo {
            definition: vec![1, 1, 1, 1],
            repetition: None,
            array_offsets: vec![0, 1, 2, 3, 4],
            array_mask: vec![true, true, true, true],
            max_definition: 1,
            level_type: LevelType::Struct(true),
            offset: 0,
            length: 4,
        };
        // 0: null ([], but mask is false, so it's not just an empty list)
        // 1: [1, 2, 3]
        // 2: [4, 5]
        // 3: [6, 7]
        let array_offsets = vec![0, 1, 4, 6, 8];
        let array_mask = vec![false, true, true, true];

        let levels = parent_levels.calculate_child_levels(
            array_offsets.clone(),
            array_mask,
            LevelType::List(true),
        );
        // 0: [null], level 1 is defined, but not 2
        // 1: [1, 2, 3]
        // 2: [4, 5]
        // 3: [6, 7]
        let expected_levels = LevelInfo {
            definition: vec![1, 3, 3, 3, 3, 3, 3, 3],
            repetition: Some(vec![0, 0, 1, 1, 0, 1, 0, 1]),
            array_offsets,
            array_mask: vec![false, true, true, true, true, true, true, true],
            max_definition: 3,
            level_type: LevelType::List(true),
            offset: 0,
            length: 8,
        };
        assert_eq!(&levels.definition, &expected_levels.definition);
        assert_eq!(&levels.repetition, &expected_levels.repetition);
        assert_eq!(&levels.array_offsets, &expected_levels.array_offsets);
        assert_eq!(&levels.max_definition, &expected_levels.max_definition);
        assert_eq!(&levels.level_type, &expected_levels.level_type);
        assert_eq!(&levels, &expected_levels);

        // nested lists (using previous test)
        let nested_parent_levels = levels;
        // 0: [null] (was a populated null slot at the parent)
        // 1: [201]
        // 2: [202, 203]
        // 3: null ([])
        // 4: [204, 205, 206]
        // 5: [207, 208, 209, 210]
        // 6: [] (tests a non-null empty list slot)
        // 7: [211, 212, 213, 214, 215]
        let array_offsets = vec![0, 1, 2, 4, 4, 7, 11, 11, 16];
        // logically, the fist slot of the mask is false
        let array_mask = vec![true, true, true, false, true, true, true, true];
        let levels = nested_parent_levels.calculate_child_levels(
            array_offsets.clone(),
            array_mask,
            LevelType::List(true),
        );
        // We have 7 array values, and at least 15 primitives (from array_offsets)
        // 0: (-)[null], parent was null, no value populated here
        // 1: (0)[201], (1)[202, 203], (2)[[null]]
        // 2: (3)[204, 205, 206], (4)[207, 208, 209, 210]
        // 3: (5)[[]], (6)[211, 212, 213, 214, 215]
        //
        // In a JSON syntax with the schema: <struct<list<list<primitive>>>>, this translates into:
        // 0: {"struct": [ null ]}
        // 1: {"struct": [ [201], [202, 203], [] ]}
        // 2: {"struct": [ [204, 205, 206], [207, 208, 209, 210] ]}
        // 3: {"struct": [ [], [211, 212, 213, 214, 215] ]}
        let expected_levels = LevelInfo {
            definition: vec![1, 5, 5, 5, 4, 5, 5, 5, 5, 5, 5, 5, 4, 5, 5, 5, 5, 5],
            repetition: Some(vec![0, 0, 1, 2, 1, 0, 2, 2, 1, 2, 2, 2, 0, 1, 2, 2, 2, 2]),
            array_mask: vec![
                false, true, true, true, false, true, true, true, true, true, true, true,
                true, true, true, true, true, true,
            ],
            array_offsets,
            max_definition: 5,
            level_type: LevelType::List(true),
            offset: 0,
            length: 16,
        };
        assert_eq!(&levels.definition, &expected_levels.definition);
        assert_eq!(&levels.repetition, &expected_levels.repetition);
        assert_eq!(&levels.array_offsets, &expected_levels.array_offsets);
        assert_eq!(&levels.array_mask, &expected_levels.array_mask);
        assert_eq!(&levels.max_definition, &expected_levels.max_definition);
        assert_eq!(&levels.level_type, &expected_levels.level_type);
        assert_eq!(&levels, &expected_levels);
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
        let a_levels = LevelInfo {
            definition: vec![1, 1, 1, 1, 0, 1],
            repetition: None,
            array_offsets: (0..=6).collect(),
            array_mask: vec![true, true, true, true, false, true],
            max_definition: 1,
            level_type: LevelType::Struct(true),
            offset: 0,
            length: 6,
        };
        // b's offset and mask
        let b_offsets: Vec<i64> = (0..=6).collect();
        let b_mask = vec![true, true, true, false, false, true];
        // b's expected levels
        let b_expected_levels = LevelInfo {
            definition: vec![2, 2, 2, 1, 0, 2],
            repetition: None,
            array_offsets: (0..=6).collect(),
            array_mask: vec![true, true, true, false, false, true],
            max_definition: 2,
            level_type: LevelType::Struct(true),
            offset: 0,
            length: 6,
        };
        let b_levels = a_levels.calculate_child_levels(
            b_offsets.clone(),
            b_mask,
            LevelType::Struct(true),
        );
        assert_eq!(&b_expected_levels, &b_levels);

        // c's offset and mask
        let c_offsets = b_offsets;
        let c_mask = vec![true, false, true, false, false, true];
        // c's expected levels
        let c_expected_levels = LevelInfo {
            definition: vec![3, 2, 3, 1, 0, 3],
            repetition: None,
            array_offsets: c_offsets.clone(),
            array_mask: vec![true, false, true, false, false, true],
            max_definition: 3,
            level_type: LevelType::Struct(true),
            offset: 0,
            length: 6,
        };
        let c_levels =
            b_levels.calculate_child_levels(c_offsets, c_mask, LevelType::Struct(true));
        assert_eq!(&c_expected_levels, &c_levels);
    }

    #[test]
    fn list_single_column() {
        // this tests the level generation from the arrow_writer equivalent test

        let a_values = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let a_value_offsets =
            arrow::buffer::Buffer::from(&[0, 1, 3, 3, 6, 10].to_byte_slice());
        let a_list_type =
            DataType::List(Box::new(Field::new("item", DataType::Int32, true)));
        let a_list_data = ArrayData::builder(a_list_type.clone())
            .len(5)
            .add_buffer(a_value_offsets)
            .null_bit_buffer(Buffer::from(vec![0b00011011]))
            .add_child_data(a_values.data().clone())
            .build()
            .unwrap();

        assert_eq!(a_list_data.null_count(), 1);

        let a = ListArray::from(a_list_data);
        let values = Arc::new(a);

        let schema = Schema::new(vec![Field::new("item", a_list_type, true)]);

        let batch = RecordBatch::try_new(Arc::new(schema), vec![values]).unwrap();

        let expected_batch_level = LevelInfo {
            definition: vec![0; 2],
            repetition: None,
            array_offsets: (0..=2).collect(),
            array_mask: vec![true, true],
            max_definition: 0,
            level_type: LevelType::Root,
            offset: 2,
            length: 2,
        };

        let batch_level = LevelInfo::new(2, 2);
        assert_eq!(&batch_level, &expected_batch_level);

        // calculate the list's level
        let mut levels = vec![];
        batch
            .columns()
            .iter()
            .zip(batch.schema().fields())
            .for_each(|(array, field)| {
                let mut array_levels = batch_level.calculate_array_levels(array, field);
                levels.append(&mut array_levels);
            });
        assert_eq!(levels.len(), 1);

        let list_level = levels.get(0).unwrap();

        let expected_level = LevelInfo {
            definition: vec![0, 3, 3, 3],
            repetition: Some(vec![0, 0, 1, 1]),
            array_offsets: vec![3, 3, 6],
            array_mask: vec![false, true, true, true],
            max_definition: 3,
            level_type: LevelType::Primitive(true),
            offset: 3,
            length: 3,
        };
        assert_eq!(&list_level.definition, &expected_level.definition);
        assert_eq!(&list_level.repetition, &expected_level.repetition);
        assert_eq!(&list_level.array_offsets, &expected_level.array_offsets);
        assert_eq!(&list_level.array_mask, &expected_level.array_mask);
        assert_eq!(&list_level.max_definition, &expected_level.max_definition);
        assert_eq!(&list_level.level_type, &expected_level.level_type);
        assert_eq!(list_level, &expected_level);
    }

    #[test]
    fn mixed_struct_list() {
        // this tests the level generation from the equivalent arrow_writer_complex test

        // define schema
        let struct_field_d = Field::new("d", DataType::Float64, true);
        let struct_field_f = Field::new("f", DataType::Float32, true);
        let struct_field_g = Field::new(
            "g",
            DataType::List(Box::new(Field::new("items", DataType::Int16, false))),
            false,
        );
        let struct_field_e = Field::new(
            "e",
            DataType::Struct(vec![struct_field_f.clone(), struct_field_g.clone()]),
            true,
        );
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, true),
            Field::new(
                "c",
                DataType::Struct(vec![struct_field_d.clone(), struct_field_e.clone()]),
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
        let g_value_offsets =
            arrow::buffer::Buffer::from(&[0, 1, 3, 3, 6, 10].to_byte_slice());

        // Construct a list array from the above two
        let g_list_data = ArrayData::builder(struct_field_g.data_type().clone())
            .len(5)
            .add_buffer(g_value_offsets)
            .add_child_data(g_value.data().clone())
            .build()
            .unwrap();
        let g = ListArray::from(g_list_data);

        let e = StructArray::from(vec![
            (struct_field_f, Arc::new(f) as ArrayRef),
            (struct_field_g, Arc::new(g) as ArrayRef),
        ]);

        let c = StructArray::from(vec![
            (struct_field_d, Arc::new(d) as ArrayRef),
            (struct_field_e, Arc::new(e) as ArrayRef),
        ]);

        // build a record batch
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(a), Arc::new(b), Arc::new(c)],
        )
        .unwrap();

        //////////////////////////////////////////////
        let expected_batch_level = LevelInfo {
            definition: vec![0; 5],
            repetition: None,
            array_offsets: (0..=5).collect(),
            array_mask: vec![true, true, true, true, true],
            max_definition: 0,
            level_type: LevelType::Root,
            offset: 0,
            length: 5,
        };

        let batch_level = LevelInfo::new(0, 5);
        assert_eq!(&batch_level, &expected_batch_level);

        // calculate the list's level
        let mut levels = vec![];
        batch
            .columns()
            .iter()
            .zip(batch.schema().fields())
            .for_each(|(array, field)| {
                let mut array_levels = batch_level.calculate_array_levels(array, field);
                levels.append(&mut array_levels);
            });
        assert_eq!(levels.len(), 5);

        // test "a" levels
        let list_level = levels.get(0).unwrap();

        let expected_level = LevelInfo {
            definition: vec![0, 0, 0, 0, 0],
            repetition: None,
            array_offsets: vec![0, 1, 2, 3, 4, 5],
            array_mask: vec![true, true, true, true, true],
            max_definition: 0,
            level_type: LevelType::Primitive(false),
            offset: 0,
            length: 5,
        };
        assert_eq!(list_level, &expected_level);

        // test "b" levels
        let list_level = levels.get(1).unwrap();

        let expected_level = LevelInfo {
            definition: vec![1, 0, 0, 1, 1],
            repetition: None,
            array_offsets: vec![0, 1, 2, 3, 4, 5],
            array_mask: vec![true, false, false, true, true],
            max_definition: 1,
            level_type: LevelType::Primitive(true),
            offset: 0,
            length: 5,
        };
        assert_eq!(list_level, &expected_level);

        // test "d" levels
        let list_level = levels.get(2).unwrap();

        let expected_level = LevelInfo {
            definition: vec![1, 1, 1, 2, 1],
            repetition: None,
            array_offsets: vec![0, 1, 2, 3, 4, 5],
            array_mask: vec![false, false, false, true, false],
            max_definition: 2,
            level_type: LevelType::Primitive(true),
            offset: 0,
            length: 5,
        };
        assert_eq!(list_level, &expected_level);

        // test "f" levels
        let list_level = levels.get(3).unwrap();

        let expected_level = LevelInfo {
            definition: vec![3, 2, 3, 2, 3],
            repetition: None,
            array_offsets: vec![0, 1, 2, 3, 4, 5],
            array_mask: vec![true, false, true, false, true],
            max_definition: 3,
            level_type: LevelType::Primitive(true),
            offset: 0,
            length: 5,
        };
        assert_eq!(list_level, &expected_level);
    }

    #[test]
    fn test_filter_array_indices() {
        let level = LevelInfo {
            definition: vec![3, 3, 3, 1, 3, 3, 3],
            repetition: Some(vec![0, 1, 1, 0, 0, 1, 1]),
            array_offsets: vec![0, 3, 3, 6],
            array_mask: vec![true, true, true, false, true, true, true],
            max_definition: 3,
            level_type: LevelType::Primitive(true),
            offset: 0,
            length: 6,
        };

        let expected = vec![0, 1, 2, 3, 4, 5];
        let filter = level.filter_array_indices();
        assert_eq!(expected, filter);
    }

    #[test]
    fn test_null_vs_nonnull_struct() {
        // define schema
        let offset_field = Field::new("offset", DataType::Int32, true);
        let schema = Schema::new(vec![Field::new(
            "some_nested_object",
            DataType::Struct(vec![offset_field.clone()]),
            false,
        )]);

        // create some data
        let offset = Int32Array::from(vec![1, 2, 3, 4, 5]);

        let some_nested_object =
            StructArray::from(vec![(offset_field, Arc::new(offset) as ArrayRef)]);

        // build a record batch
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(some_nested_object)])
                .unwrap();

        let batch_level = LevelInfo::new(0, batch.num_rows());
        let struct_null_level =
            batch_level.calculate_array_levels(batch.column(0), batch.schema().field(0));

        // create second batch
        // define schema
        let offset_field = Field::new("offset", DataType::Int32, true);
        let schema = Schema::new(vec![Field::new(
            "some_nested_object",
            DataType::Struct(vec![offset_field.clone()]),
            true,
        )]);

        // create some data
        let offset = Int32Array::from(vec![1, 2, 3, 4, 5]);

        let some_nested_object =
            StructArray::from(vec![(offset_field, Arc::new(offset) as ArrayRef)]);

        // build a record batch
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(some_nested_object)])
                .unwrap();

        let batch_level = LevelInfo::new(0, batch.num_rows());
        let struct_non_null_level =
            batch_level.calculate_array_levels(batch.column(0), batch.schema().field(0));

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
        {"stocks":{"long": null, "long": "$CCC", "short": null}}
        {"stocks":{"hedged": "$YYY", "long": null, "short": "$D"}}
        "#;
        let entries_struct_type = DataType::Struct(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
        ]);
        let stocks_field = Field::new(
            "stocks",
            DataType::Map(
                Box::new(Field::new("entries", entries_struct_type, false)),
                false,
            ),
            // not nullable, so the keys have max level = 1
            false,
        );
        let schema = Arc::new(Schema::new(vec![stocks_field]));
        let builder = arrow::json::ReaderBuilder::new()
            .with_schema(schema)
            .with_batch_size(64);
        let mut reader = builder.build(std::io::Cursor::new(json_content)).unwrap();

        let batch = reader.next().unwrap().unwrap();

        let expected_batch_level = LevelInfo {
            definition: vec![0; 3],
            repetition: None,
            array_offsets: (0..=3).collect(),
            array_mask: vec![true, true, true],
            max_definition: 0,
            level_type: LevelType::Root,
            offset: 0,
            length: 3,
        };

        let batch_level = LevelInfo::new(0, 3);
        assert_eq!(&batch_level, &expected_batch_level);

        // calculate the map's level
        let mut levels = vec![];
        batch
            .columns()
            .iter()
            .zip(batch.schema().fields())
            .for_each(|(array, field)| {
                let mut array_levels = batch_level.calculate_array_levels(array, field);
                levels.append(&mut array_levels);
            });
        assert_eq!(levels.len(), 2);

        // test key levels
        let list_level = levels.get(0).unwrap();

        let expected_level = LevelInfo {
            definition: vec![1; 7],
            repetition: Some(vec![0, 1, 0, 1, 0, 1, 1]),
            array_offsets: vec![0, 2, 4, 7],
            array_mask: vec![true; 7],
            max_definition: 1,
            level_type: LevelType::Primitive(false),
            offset: 0,
            length: 7,
        };
        assert_eq!(list_level, &expected_level);

        // test values levels
        let list_level = levels.get(1).unwrap();

        let expected_level = LevelInfo {
            definition: vec![2, 2, 2, 1, 2, 1, 2],
            repetition: Some(vec![0, 1, 0, 1, 0, 1, 1]),
            array_offsets: vec![0, 2, 4, 7],
            array_mask: vec![true, true, true, false, true, false, true],
            max_definition: 2,
            level_type: LevelType::Primitive(true),
            offset: 0,
            length: 7,
        };
        assert_eq!(list_level, &expected_level);
    }

    #[test]
    fn test_list_of_struct() {
        // define schema
        let int_field = Field::new("a", DataType::Int32, true);
        let item_field =
            Field::new("item", DataType::Struct(vec![int_field.clone()]), true);
        let list_field = Field::new("list", DataType::List(Box::new(item_field)), true);

        let int_builder = Int32Builder::new(10);
        let struct_builder =
            StructBuilder::new(vec![int_field], vec![Box::new(int_builder)]);
        let mut list_builder = ListBuilder::new(struct_builder);

        // [{a: 1}], [], null, [null, null], [{a: null}], [{a: 2}]
        //
        // [{a: 1}]
        let values = list_builder.values();
        values
            .field_builder::<Int32Builder>(0)
            .unwrap()
            .append_value(1)
            .unwrap();
        values.append(true).unwrap();
        list_builder.append(true).unwrap();

        // []
        list_builder.append(true).unwrap();

        // null
        list_builder.append(false).unwrap();

        // [null, null]
        let values = list_builder.values();
        values
            .field_builder::<Int32Builder>(0)
            .unwrap()
            .append_null()
            .unwrap();
        values.append(false).unwrap();
        values
            .field_builder::<Int32Builder>(0)
            .unwrap()
            .append_null()
            .unwrap();
        values.append(false).unwrap();
        list_builder.append(true).unwrap();

        // [{a: null}]
        let values = list_builder.values();
        values
            .field_builder::<Int32Builder>(0)
            .unwrap()
            .append_null()
            .unwrap();
        values.append(true).unwrap();
        list_builder.append(true).unwrap();

        // [{a: 2}]
        let values = list_builder.values();
        values
            .field_builder::<Int32Builder>(0)
            .unwrap()
            .append_value(2)
            .unwrap();
        values.append(true).unwrap();
        list_builder.append(true).unwrap();

        let array = Arc::new(list_builder.finish());

        let schema = Arc::new(Schema::new(vec![list_field]));

        let rb = RecordBatch::try_new(schema, vec![array]).unwrap();

        let batch_level = LevelInfo::new(0, rb.num_rows());
        let list_level =
            &batch_level.calculate_array_levels(rb.column(0), rb.schema().field(0))[0];

        let expected_level = LevelInfo {
            definition: vec![4, 1, 0, 2, 2, 3, 4],
            repetition: Some(vec![0, 0, 0, 0, 1, 0, 0]),
            array_offsets: vec![0, 1, 1, 1, 3, 4, 5],
            array_mask: vec![true, true, false, false, false, false, true],
            max_definition: 4,
            level_type: LevelType::Primitive(true),
            offset: 0,
            length: 5,
        };

        assert_eq!(list_level, &expected_level);
    }

    #[test]
    fn test_nested_indices() {
        // Given a buffer like
        // [0, null, null, 1, 2]
        //
        // The two level infos below might represent the two structures
        // 1: [{a: 0}], [], null, [null, null], [{a: 1}], [{a: 2}]
        // 2: [0], [], null, [null, null], [1], [2]
        //
        // (That is, their only difference is that the leaf values are nested one level deeper in a
        // struct).

        let level1 = LevelInfo {
            definition: vec![4, 1, 0, 2, 2, 4, 4],
            repetition: Some(vec![0, 0, 0, 0, 1, 0, 0]),
            array_offsets: vec![0, 1, 1, 1, 3, 4, 5],
            array_mask: vec![true, true, false, false, false, false, true],
            max_definition: 4,
            level_type: LevelType::Primitive(true),
            offset: 0,
            length: 5,
        };

        let level2 = LevelInfo {
            definition: vec![3, 1, 0, 2, 2, 3, 3],
            repetition: Some(vec![0, 0, 0, 0, 1, 0, 0]),
            array_offsets: vec![0, 1, 1, 1, 3, 4, 5],
            array_mask: vec![true, true, false, false, false, false, true],
            max_definition: 3,
            level_type: LevelType::Primitive(true),
            offset: 0,
            length: 5,
        };

        // filter_array_indices should return the same indices in this case.
        assert_eq!(level1.filter_array_indices(), level2.filter_array_indices());
    }
}
