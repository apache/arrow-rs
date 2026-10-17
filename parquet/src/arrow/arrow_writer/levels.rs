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

use crate::column::writer::{LevelDataRef, RunLevelsRef};
use crate::errors::ParquetError;
use arrow_schema::{DataType, Field};

pub(crate) mod cursor;
mod plan;

pub(crate) use plan::LeafBatch;
use plan::{LEVEL_RUN_PROBE_SIZE, MIN_AVERAGE_LEVEL_RUN_LENGTH};

/// Minimum sub-range length eligible for bitmap span filling.
const BULK_FILL_MIN_LEN: usize = 64;

/// Returns true if the DataType can be represented as a primitive parquet column.
pub(super) fn is_leaf(data_type: &DataType) -> bool {
    data_type.is_primitive()
        || matches!(
            data_type,
            DataType::Null
                | DataType::Boolean
                | DataType::Utf8
                | DataType::Utf8View
                | DataType::LargeUtf8
                | DataType::Binary
                | DataType::LargeBinary
                | DataType::BinaryView
                | DataType::FixedSizeBinary(_)
        )
}

#[derive(Clone, Copy)]
struct FieldContract<'a> {
    data_type: &'a DataType,
    nullable: bool,
    name: &'a str,
}

/// Erase schema-only dictionary and REE wrappers. REE value-field
/// nullability belongs to the logical node it exposes.
fn normalized(field: &Field) -> FieldContract<'_> {
    let (data_type, nullable) = logical_type(field.data_type());
    FieldContract {
        data_type,
        nullable: field.is_nullable() || nullable,
        name: field.name(),
    }
}

fn logical_type(mut data_type: &DataType) -> (&DataType, bool) {
    let mut nullable = false;
    loop {
        match data_type {
            DataType::Dictionary(_, value) => data_type = value,
            DataType::RunEndEncoded(_, value) => {
                nullable |= value.is_nullable();
                data_type = value.data_type();
            }
            _ => return (data_type, nullable),
        }
    }
}

fn leaf_types_compatible(expected: &DataType, actual: &DataType) -> bool {
    is_leaf(expected)
        && is_leaf(actual)
        && (expected.equals_datatype(actual)
            || matches!(
                (expected, actual),
                (
                    DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8,
                    DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8
                ) | (
                    DataType::Binary | DataType::BinaryView | DataType::LargeBinary,
                    DataType::Binary | DataType::BinaryView | DataType::LargeBinary
                )
            ))
}

fn required_null(field: &str, index: usize) -> ParquetError {
    ParquetError::ArrowError(format!(
        "Found null at index {index} for required field '{field}'"
    ))
}

#[derive(Debug, Default, Clone, Copy)]
struct LevelContext {
    /// The current repetition level
    rep_level: i16,
    /// The current definition level
    def_level: i16,
}

/// One owned definition- or repetition-level stream for a leaf batch.
#[derive(Debug, Clone)]
pub(crate) enum LevelData {
    Absent,
    Materialized(Vec<i16>),
    Uniform { value: i16, count: usize },
    Runs(LevelRuns),
}

impl LevelData {
    pub(super) fn new(present: bool) -> Self {
        match present {
            true => Self::Materialized(Vec::new()),
            false => Self::Absent,
        }
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Absent => 0,
            Self::Materialized(values) => values.len(),
            Self::Uniform { count, .. } => *count,
            Self::Runs(runs) => runs.len(),
        }
    }

    /// Append `count` repetitions of `value` straight into a materialized
    /// buffer, skipping the compact representations.
    ///
    /// For a stream that a later pass has to index into — repetition levels,
    /// which `patch_list_starts` rewrites per list row — the compact forms are
    /// pure overhead: they would be built, re-checked on every append, and then
    /// converted back. `Absent` stays absent, so this is a no-op for a column
    /// that has no such stream.
    #[inline]
    pub(super) fn append_dense_run(&mut self, value: i16, count: usize) {
        if count == 0 {
            return;
        }
        if let Some(values) = self.materialize_mut() {
            values.extend(std::iter::repeat_n(value, count));
        }
    }

    #[inline]
    pub(super) fn clear(&mut self) {
        match self {
            Self::Absent => {}
            Self::Materialized(values) => values.clear(),
            Self::Uniform { .. } => *self = Self::Materialized(Vec::new()),
            Self::Runs(runs) => {
                runs.ends.clear();
                runs.values.clear();
            }
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
            Self::Runs(runs) => LevelDataRef::Runs(RunLevelsRef::from_level_runs(
                &runs.ends,
                &runs.values,
                0,
                runs.len(),
            )),
        }
    }

    /// Append `count` repetitions of `value`, keeping the most compact
    /// representation that still describes the stream exactly: a single value,
    /// then cumulative runs, and finally a materialized buffer once the runs
    /// stop paying for themselves.
    #[inline]
    pub(super) fn append_run(&mut self, value: i16, count: usize) {
        if count == 0 {
            return;
        }
        match self {
            // No physical level stream exists for this schema. Higher-level
            // traversal may still append implicit levels, so this remains a no-op.
            Self::Absent => {}
            // Start compact: the first appended run can be represented without
            // allocating a level buffer.
            Self::Materialized(values) if values.is_empty() => {
                *self = Self::Uniform { value, count };
            }
            Self::Materialized(values) if count == 1 => values.push(value),
            // Already materialized, so preserve the buffer representation and append.
            Self::Materialized(values) => values.extend(std::iter::repeat_n(value, count)),
            // Preserve the compact representation while the appended run has
            // the same value.
            Self::Uniform {
                value: uniform,
                count: len,
            } if *uniform == value => *len += count,
            // A different value breaks the uniform representation. Switch to
            // cumulative runs, unless the runs are already too short to pay for
            // themselves.
            Self::Uniform {
                value: uniform,
                count: len,
            } => {
                let runs = LevelRuns::from_two_runs(*uniform, *len, value, count);
                *self = if runs.should_materialize() {
                    Self::Materialized(runs.into_materialized())
                } else {
                    Self::Runs(runs)
                };
            }
            Self::Runs(runs) => {
                runs.append_run(value, count);
                if runs.should_materialize() {
                    let Self::Runs(runs) = std::mem::replace(self, Self::Absent) else {
                        unreachable!()
                    };
                    *self = Self::Materialized(runs.into_materialized());
                }
            }
        }
    }

    #[inline(never)]
    pub(super) fn extend_from_iter<I>(&mut self, iter: I)
    where
        I: IntoIterator<Item = i16>,
    {
        match self {
            Self::Absent => {}
            Self::Materialized(values) => values.extend(iter),
            _ => self.materialize_mut().unwrap().extend(iter),
        }
    }

    /// Every dense append goes through here, and the stream is already
    /// materialized for all but the first of them. Keep that check inline and
    /// leave the conversion itself out of line.
    #[inline]
    pub(super) fn materialize_mut(&mut self) -> Option<&mut Vec<i16>> {
        match self {
            Self::Absent => return None,
            Self::Materialized(_) => {}
            _ => self.materialize_compact(),
        }
        let Self::Materialized(values) = self else {
            unreachable!()
        };
        Some(values)
    }

    #[cold]
    #[inline(never)]
    fn materialize_compact(&mut self) {
        let values = match self {
            Self::Uniform { value, count } => vec![*value; *count],
            Self::Runs(_) => {
                let Self::Runs(runs) = std::mem::replace(self, Self::Absent) else {
                    unreachable!()
                };
                runs.into_materialized()
            }
            _ => unreachable!("only compact representations are converted"),
        };
        *self = Self::Materialized(values);
    }
}

/// Cumulative-end run representation for definition and repetition levels.
#[derive(Debug, Clone)]
pub(crate) struct LevelRuns {
    ends: Vec<usize>,
    values: Vec<i16>,
}

impl LevelRuns {
    fn from_two_runs(first: i16, first_count: usize, second: i16, second_count: usize) -> Self {
        let mut runs = Self {
            ends: vec![first_count],
            values: vec![first],
        };
        runs.append_run(second, second_count);
        runs
    }

    fn len(&self) -> usize {
        self.ends.last().copied().unwrap_or(0)
    }

    #[inline]
    fn append_run(&mut self, value: i16, count: usize) {
        debug_assert_ne!(count, 0);
        let end = self
            .len()
            .checked_add(count)
            .expect("level stream length overflow");
        if self.values.last().copied() == Some(value) {
            *self.ends.last_mut().unwrap() = end;
        } else {
            self.ends.push(end);
            self.values.push(value);
        }
    }

    /// Runs stop paying for themselves once the stream is long enough to
    /// measure and the average run falls below the configured threshold.
    fn should_materialize(&self) -> bool {
        let len = self.len();
        len >= LEVEL_RUN_PROBE_SIZE
            && self
                .values
                .len()
                .saturating_mul(MIN_AVERAGE_LEVEL_RUN_LENGTH)
                > len
    }

    fn into_materialized(self) -> Vec<i16> {
        let mut materialized = Vec::with_capacity(self.len());
        let mut start = 0;
        for (end, value) in self.ends.into_iter().zip(self.values) {
            materialized.extend(std::iter::repeat_n(value, end - start));
            start = end;
        }
        materialized
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::Result;
    use arrow_array::builder::*;
    use arrow_array::cast::AsArray;
    use arrow_array::types::Int32Type;
    use arrow_array::*;
    use arrow_buffer::{Buffer, NullBuffer, ToByteSlice};
    use arrow_cast::display::array_value_to_string;
    use arrow_data::{ArrayData, ArrayDataBuilder};
    use arrow_schema::{Fields, Schema};

    use crate::column::value_selection::ValueSelectionRef;
    use crate::column::writer::LevelValueWindow;
    use std::sync::Arc;

    /// Logical observations collected from production leaf cursors for assertions.
    #[derive(Debug, Clone)]
    struct CollectedLeafOutput {
        def_levels: LevelData,
        rep_levels: LevelData,
        non_null_indices: Vec<usize>,
        max_def_level: i16,
        max_rep_level: i16,
        array: ArrayRef,
        logical_nulls: Option<NullBuffer>,
    }

    fn level_data_eq(left: &LevelData, right: &LevelData) -> bool {
        if matches!(left, LevelData::Absent) || matches!(right, LevelData::Absent) {
            return matches!((left, right), (LevelData::Absent, LevelData::Absent));
        }

        let left = left.as_ref();
        let right = right.as_ref();
        left.len() == right.len()
            && (0..left.len()).all(|index| left.value_at(index) == right.value_at(index))
    }

    impl PartialEq for CollectedLeafOutput {
        fn eq(&self, other: &Self) -> bool {
            level_data_eq(&self.def_levels, &other.def_levels)
                && level_data_eq(&self.rep_levels, &other.rep_levels)
                && self.non_null_indices == other.non_null_indices
                && self.max_def_level == other.max_def_level
                && self.max_rep_level == other.max_rep_level
                && self.array.as_ref() == other.array.as_ref()
                && self.logical_nulls == other.logical_nulls
        }
    }

    impl Eq for CollectedLeafOutput {}

    fn append_observed_levels(output: &mut Option<Vec<i16>>, levels: LevelDataRef<'_>) {
        assert_eq!(
            output.is_none(),
            matches!(levels, LevelDataRef::Absent),
            "cursor level stream disagrees with its planned maximum"
        );
        if let Some(output) = output {
            output.extend(levels.cursor());
        }
    }

    /// Collect logical cursor output for the explicit expected-level fixtures.
    /// Each configuration must produce the same observations; representation
    /// sharing and batch boundaries are asserted directly in cursor tests.
    fn collect_leaf_output(array: &ArrayRef, field: &Field) -> Result<Vec<CollectedLeafOutput>> {
        let tree = cursor::LevelTree::build(field, array)?;
        let mut reference = None;
        for (target_slots, target_rows) in [(1, 1), (1024, 2), (1024, 1024)] {
            let mut output = Vec::new();
            for leaf in 0..tree.leaf_count() as u32 {
                let (max_def_level, max_rep_level) = tree.leaf_max_levels_for_test(leaf);
                let terminal = tree.terminal(leaf);
                let mut cursor = tree.cursor(leaf..leaf + 1, target_slots, target_rows)?;
                let mut def_levels = (max_def_level != 0).then(Vec::new);
                let mut rep_levels = (max_rep_level != 0).then(Vec::new);
                let mut non_null_indices = Vec::new();
                while let Some(tiles) = cursor.next_tiles()? {
                    let batch = tiles.leaf(0, terminal);
                    append_observed_levels(&mut def_levels, batch.def_level_data());
                    append_observed_levels(&mut rep_levels, batch.rep_level_data());
                    let values = batch.value_selection();
                    non_null_indices.extend((0..values.len()).map(|index| values.index_at(index)));
                }
                output.push(CollectedLeafOutput {
                    def_levels: def_levels.map_or(LevelData::Absent, LevelData::Materialized),
                    rep_levels: rep_levels.map_or(LevelData::Absent, LevelData::Materialized),
                    non_null_indices,
                    max_def_level,
                    max_rep_level,
                    logical_nulls: terminal.logical_nulls(),
                    array: make_array(terminal.to_data()),
                });
            }
            if let Some(expected) = &reference {
                assert_eq!(
                    &output, expected,
                    "slot target {target_slots}, row target {target_rows}"
                );
            } else {
                reference = Some(output);
            }
        }
        Ok(reference.unwrap())
    }

    #[test]
    fn cursor_collector_handles_empty_leaves() {
        for nullable in [false, true] {
            let array: ArrayRef = Arc::new(Int32Array::from(Vec::<i32>::new()));
            let field = Field::new("value", DataType::Int32, nullable);
            let observed = collect_leaf_output(&array, &field).unwrap();
            assert_eq!(
                observed,
                vec![CollectedLeafOutput {
                    def_levels: if nullable {
                        LevelData::Materialized(vec![])
                    } else {
                        LevelData::Absent
                    },
                    rep_levels: LevelData::Absent,
                    non_null_indices: vec![],
                    max_def_level: i16::from(nullable),
                    max_rep_level: 0,
                    array,
                    logical_nulls: None,
                }]
            );
        }
    }

    #[test]
    fn cursor_collector_traverses_the_sliced_input() {
        let array: ArrayRef = Arc::new(Int32Array::from(vec![None, Some(2), Some(3), None]));
        let field = Field::new("value", DataType::Int32, false);
        assert!(collect_leaf_output(&array, &field).is_err());

        // Nulls outside the slice must not be visited or validated. Selected
        // indices are relative to the sliced primitive array, not its parent.
        let sliced = array.slice(1, 2);
        let observed = collect_leaf_output(&sliced, &field).unwrap();
        assert_eq!(
            observed,
            vec![CollectedLeafOutput {
                def_levels: LevelData::Absent,
                rep_levels: LevelData::Absent,
                non_null_indices: vec![0, 1],
                max_def_level: 0,
                max_rep_level: 0,
                logical_nulls: sliced.logical_nulls(),
                array: sliced,
            }]
        );
    }

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

        let levels = collect_leaf_output(&outer_list, &outer_field).unwrap();
        assert_eq!(levels.len(), 1);

        let expected = CollectedLeafOutput {
            def_levels: LevelData::Materialized(vec![2; 10]),
            rep_levels: LevelData::Materialized(vec![0, 2, 2, 1, 2, 2, 2, 0, 1, 2]),
            non_null_indices: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
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

        let levels = collect_leaf_output(&array, &field).unwrap();
        assert_eq!(levels.len(), 1);

        let expected_levels = CollectedLeafOutput {
            def_levels: LevelData::Absent,
            rep_levels: LevelData::Absent,
            non_null_indices: (0..10).collect(),
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

        let levels = collect_leaf_output(&array, &field).unwrap();
        assert_eq!(levels.len(), 1);

        let logical_nulls = array.logical_nulls();
        let expected_levels = CollectedLeafOutput {
            def_levels: LevelData::Materialized(vec![1, 0, 1, 1, 0]),
            rep_levels: LevelData::Absent,
            non_null_indices: vec![0, 2, 3],
            max_def_level: 1,
            max_rep_level: 0,
            array,
            logical_nulls,
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
        let levels = collect_leaf_output(&list, &list_field).unwrap();
        assert_eq!(levels.len(), 1);

        let expected_levels = CollectedLeafOutput {
            def_levels: LevelData::Materialized(vec![1; 5]),
            rep_levels: LevelData::Materialized(vec![0; 5]),
            non_null_indices: (0..5).collect(),
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
        let levels = collect_leaf_output(&list, &list_field).unwrap();
        assert_eq!(levels.len(), 1);

        let expected_levels = CollectedLeafOutput {
            def_levels: LevelData::Materialized(vec![2, 2, 0, 2, 2, 2, 2, 2, 2, 2, 2, 2]),
            rep_levels: LevelData::Materialized(vec![0, 1, 0, 0, 1, 0, 1, 1, 1, 0, 1, 1]),
            non_null_indices: (0..11).collect(),
            max_def_level: 2,
            max_rep_level: 1,
            array: Arc::new(leaf_array),
            logical_nulls: None,
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

        let levels = collect_leaf_output(&array, &struct_field).unwrap();
        assert_eq!(levels.len(), 1);

        let expected_levels = CollectedLeafOutput {
            def_levels: LevelData::Materialized(vec![0, 2, 0, 3, 3, 3, 3, 3, 3, 3]),
            rep_levels: LevelData::Materialized(vec![0, 0, 0, 0, 1, 1, 1, 0, 1, 1]),
            non_null_indices: (4..11).collect(),
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

        let levels = collect_leaf_output(&l2, &l2_field).unwrap();
        assert_eq!(levels.len(), 1);

        let expected_levels = CollectedLeafOutput {
            def_levels: LevelData::Materialized(vec![
                5, 5, 5, 5, 1, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5,
            ]),
            rep_levels: LevelData::Materialized(vec![
                0, 2, 1, 2, 0, 0, 2, 1, 2, 0, 2, 1, 2, 1, 2, 1, 2, 0, 2, 1, 2, 1, 2,
            ]),
            non_null_indices: (0..22).collect(),
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
        let levels = collect_leaf_output(&list, &list_field).unwrap();
        assert_eq!(levels.len(), 1);

        let expected_levels = CollectedLeafOutput {
            def_levels: LevelData::Materialized(vec![1; 4]),
            rep_levels: LevelData::Materialized(vec![0; 4]),
            non_null_indices: (0..4).collect(),
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
        let levels = collect_leaf_output(&array, &struct_field).unwrap();
        assert_eq!(levels.len(), 1);

        let expected_levels = CollectedLeafOutput {
            def_levels: LevelData::Materialized(vec![1, 3, 3, 3, 3, 3, 3, 3]),
            rep_levels: LevelData::Materialized(vec![0, 0, 1, 1, 0, 1, 0, 1]),
            non_null_indices: (0..7).collect(),
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
        let levels = collect_leaf_output(&array, &struct_field).unwrap();
        assert_eq!(levels.len(), 1);

        let expected_levels = CollectedLeafOutput {
            def_levels: LevelData::Materialized(vec![
                1, 5, 5, 5, 4, 5, 5, 5, 5, 5, 5, 5, 4, 5, 5, 5, 5, 5,
            ]),
            rep_levels: LevelData::Materialized(vec![
                0, 0, 1, 2, 1, 0, 2, 2, 1, 2, 2, 2, 0, 1, 2, 2, 2, 2,
            ]),
            non_null_indices: (0..15).collect(),
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

        let levels = collect_leaf_output(&a_array, &a_field).unwrap();
        assert_eq!(levels.len(), 1);

        let logical_nulls = leaf.logical_nulls();
        let expected_levels = CollectedLeafOutput {
            def_levels: LevelData::Materialized(vec![3, 2, 3, 1, 0, 3]),
            rep_levels: LevelData::Absent,
            non_null_indices: vec![0, 2, 5],
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
        let array: ArrayRef = Arc::new(a);
        // Traverse the sliced input itself, retaining absolute child indices.
        let levels = collect_leaf_output(&array.slice(2, 2), &item_field).unwrap();

        assert_eq!(levels.len(), 1);

        let list_level = &levels[0];

        let expected_level = CollectedLeafOutput {
            def_levels: LevelData::Materialized(vec![0, 3, 3, 3]),
            rep_levels: LevelData::Materialized(vec![0, 0, 1, 1]),
            non_null_indices: vec![3, 4, 5],
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
                let mut array_levels = collect_leaf_output(array, field).unwrap();
                levels.append(&mut array_levels);
            });
        assert_eq!(levels.len(), 5);

        // test "a" levels
        let list_level = &levels[0];

        let expected_level = CollectedLeafOutput {
            def_levels: LevelData::Absent,
            rep_levels: LevelData::Absent,
            non_null_indices: vec![0, 1, 2, 3, 4],
            max_def_level: 0,
            max_rep_level: 0,
            array: Arc::new(a),
            logical_nulls: None,
        };
        assert_eq!(list_level, &expected_level);

        // test "b" levels
        let list_level = levels.get(1).unwrap();

        let b_logical_nulls = b.logical_nulls();
        let expected_level = CollectedLeafOutput {
            def_levels: LevelData::Materialized(vec![1, 0, 0, 1, 1]),
            rep_levels: LevelData::Absent,
            non_null_indices: vec![0, 3, 4],
            max_def_level: 1,
            max_rep_level: 0,
            array: Arc::new(b),
            logical_nulls: b_logical_nulls,
        };
        assert_eq!(list_level, &expected_level);

        // test "d" levels
        let list_level = levels.get(2).unwrap();

        let d_logical_nulls = d.logical_nulls();
        let expected_level = CollectedLeafOutput {
            def_levels: LevelData::Materialized(vec![1, 1, 1, 2, 1]),
            rep_levels: LevelData::Absent,
            non_null_indices: vec![3],
            max_def_level: 2,
            max_rep_level: 0,
            array: Arc::new(d),
            logical_nulls: d_logical_nulls,
        };
        assert_eq!(list_level, &expected_level);

        // test "f" levels
        let list_level = levels.get(3).unwrap();

        let f_logical_nulls = f.logical_nulls();
        let expected_level = CollectedLeafOutput {
            def_levels: LevelData::Materialized(vec![3, 2, 3, 2, 3]),
            rep_levels: LevelData::Absent,
            non_null_indices: vec![0, 2, 4],
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
            collect_leaf_output(batch.column(0), batch.schema().field(0)).unwrap();

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
            collect_leaf_output(batch.column(0), batch.schema().field(0)).unwrap();

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
            Field::new(Field::MAP_KEY_FIELD_DEFAULT_NAME, DataType::Utf8, false),
            Field::new(Field::MAP_VALUE_FIELD_DEFAULT_NAME, DataType::Utf8, true),
        ]));
        let stocks_field = Field::new(
            "stocks",
            DataType::Map(
                Arc::new(Field::new(
                    Field::MAP_ENTRIES_FIELD_DEFAULT_NAME,
                    entries_struct_type,
                    false,
                )),
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
                let mut array_levels = collect_leaf_output(array, field).unwrap();
                levels.append(&mut array_levels);
            });
        assert_eq!(levels.len(), 2);

        let map = batch.column(0).as_map();
        let map_keys_logical_nulls = map.keys().logical_nulls();

        // test key levels
        let list_level = &levels[0];

        let expected_level = CollectedLeafOutput {
            def_levels: LevelData::Materialized(vec![1; 7]),
            rep_levels: LevelData::Materialized(vec![0, 1, 0, 1, 0, 1, 1]),
            non_null_indices: vec![0, 1, 2, 3, 4, 5, 6],
            max_def_level: 1,
            max_rep_level: 1,
            array: map.keys().clone(),
            logical_nulls: map_keys_logical_nulls,
        };
        assert_eq!(list_level, &expected_level);

        // test values levels
        let list_level = levels.get(1).unwrap();
        let map_values_logical_nulls = map.values().logical_nulls();

        let expected_level = CollectedLeafOutput {
            def_levels: LevelData::Materialized(vec![2, 2, 2, 1, 2, 1, 2]),
            rep_levels: LevelData::Materialized(vec![0, 1, 0, 1, 0, 1, 1]),
            non_null_indices: vec![0, 1, 2, 4, 6],
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

        let levels = collect_leaf_output(rb.column(0), rb.schema().field(0)).unwrap();
        let list_level = &levels[0];

        let logical_nulls = values.logical_nulls();
        let expected_level = CollectedLeafOutput {
            def_levels: LevelData::Materialized(vec![4, 1, 0, 2, 2, 3, 4]),
            rep_levels: LevelData::Materialized(vec![0, 0, 0, 0, 1, 0, 0]),
            non_null_indices: vec![0, 4],
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
        let levels = collect_leaf_output(&array, &field).unwrap();

        assert_eq!(levels.len(), 1);

        let logical_nulls = values.logical_nulls();
        let expected_level = CollectedLeafOutput {
            def_levels: LevelData::Materialized(vec![4, 4, 3, 2, 0, 4, 4, 0, 1]),
            rep_levels: LevelData::Materialized(vec![0, 1, 0, 0, 0, 0, 1, 0, 0]),
            non_null_indices: vec![0, 1, 5, 6],
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
            String::new(),
            String::new(),
            "[]".to_string(),
            "[{list: [3, ], integers: }]".to_string(),
            "[, {list: , integers: 5}]".to_string(),
            "[]".to_string(),
        ];

        let actual: Vec<_> = (0..6)
            .map(|x| array_value_to_string(&list, x).unwrap())
            .collect();
        assert_eq!(actual, expected);

        let levels = collect_leaf_output(&list, &list_field).unwrap();

        assert_eq!(levels.len(), 2);

        let a1_logical_nulls = a1_values.logical_nulls();
        let expected_level = CollectedLeafOutput {
            def_levels: LevelData::Materialized(vec![0, 0, 1, 6, 5, 2, 3, 1]),
            rep_levels: LevelData::Materialized(vec![0, 0, 0, 0, 2, 0, 1, 0]),
            non_null_indices: vec![1],
            max_def_level: 6,
            max_rep_level: 2,
            array: a1_values,
            logical_nulls: a1_logical_nulls,
        };

        assert_eq!(&levels[0], &expected_level);

        let a2_logical_nulls = a2_values.logical_nulls();
        let expected_level = CollectedLeafOutput {
            def_levels: LevelData::Materialized(vec![0, 0, 1, 3, 2, 4, 1]),
            rep_levels: LevelData::Materialized(vec![0, 0, 0, 0, 0, 1, 0]),
            non_null_indices: vec![4],
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
        let a: ArrayRef = Arc::new(builder.finish());
        let item_field = Field::new_list_field(a.data_type().clone(), true);
        let sliced = a.slice(1, 3);
        let values = sliced.as_fixed_size_list().values().clone();
        let levels = collect_leaf_output(&sliced, &item_field).unwrap();

        assert_eq!(levels.len(), 1);

        let list_level = &levels[0];

        let logical_nulls = values.logical_nulls();
        let expected_level = CollectedLeafOutput {
            def_levels: LevelData::Materialized(vec![0, 0, 3, 3]),
            rep_levels: LevelData::Materialized(vec![0, 0, 0, 1]),
            non_null_indices: vec![4, 5],
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

        let levels = collect_leaf_output(rb.column(0), rb.schema().field(0)).unwrap();
        let a_levels = &levels[0];
        let b_levels = &levels[1];

        // [[{a: 1}, null], null, [null, null], [{a: null}, {a: 2}]]
        let values_a_logical_nulls = values_a.logical_nulls();
        let expected_a = CollectedLeafOutput {
            def_levels: LevelData::Materialized(vec![4, 2, 0, 2, 2, 3, 4]),
            rep_levels: LevelData::Materialized(vec![0, 1, 0, 0, 1, 0, 1]),
            non_null_indices: vec![0, 7],
            max_def_level: 4,
            max_rep_level: 1,
            array: values_a,
            logical_nulls: values_a_logical_nulls,
        };
        // [[{b: 2}, null], null, [null, null], [{b: 3}, {b: 4}]]
        let values_b_logical_nulls = values_b.logical_nulls();
        let expected_b = CollectedLeafOutput {
            def_levels: LevelData::Materialized(vec![3, 2, 0, 2, 2, 3, 3]),
            rep_levels: LevelData::Materialized(vec![0, 1, 0, 0, 1, 0, 1]),
            non_null_indices: vec![0, 6, 7],
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
        let array: ArrayRef = Arc::new(array);
        let levels = collect_leaf_output(&array, &item_field).unwrap();

        assert_eq!(levels.len(), 1);

        let list_level = &levels[0];

        let logical_nulls = values.logical_nulls();
        let expected_level = CollectedLeafOutput {
            def_levels: LevelData::Materialized(vec![1, 0, 1]),
            rep_levels: LevelData::Materialized(vec![0, 0, 0]),
            non_null_indices: vec![],
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
        let array: ArrayRef = Arc::new(a);
        let levels = collect_leaf_output(&array, &item_field).unwrap();

        let logical_nulls = values.logical_nulls();
        let expected_level = CollectedLeafOutput {
            def_levels: LevelData::Materialized(vec![5, 4, 5, 2, 5, 3, 5, 5, 4, 4, 0]),
            rep_levels: LevelData::Materialized(vec![0, 2, 2, 1, 0, 1, 0, 2, 1, 2, 0]),
            non_null_indices: vec![0, 2, 3, 4, 5],
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

        let array: ArrayRef = Arc::new(dict.clone());
        let levels = collect_leaf_output(&array, &item_field).unwrap();

        let logical_nulls = dict.logical_nulls();
        let expected_level = CollectedLeafOutput {
            def_levels: LevelData::Materialized(vec![0, 0, 1, 1]),
            rep_levels: LevelData::Absent,
            non_null_indices: vec![2, 3],
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

        let err = cursor::LevelTree::build(&field, &array)
            .unwrap_err()
            .to_string();

        assert_eq!(
            err,
            "Arrow: Incompatible type. Field 'item' has type Float64, array has type Int32",
        );
    }

    fn materialized_levels(levels: LevelDataRef<'_>) -> Vec<i16> {
        (0..levels.len())
            .map(|index| levels.value_at(index).unwrap())
            .collect()
    }

    fn selected_indices(values: ValueSelectionRef<'_>) -> Vec<usize> {
        (0..values.len())
            .map(|index| values.index_at(index))
            .collect()
    }

    #[test]
    fn test_slice_for_chunk_flat() {
        // Required field: values 2..5 select source indices [2, 3, 4]. The
        // zero-copy slice retains the complete source array and keeps indices
        // absolute rather than rebasing them.
        let array: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6]));
        let indices = [0, 1, 2, 3, 4, 5];
        let batch = LeafBatch::new(
            array.as_ref(),
            LevelDataRef::Absent,
            LevelDataRef::Absent,
            ValueSelectionRef::Sparse(&indices),
        );
        let sliced = batch.slice(LevelValueWindow {
            levels: 0..0,
            values: 2..5,
        });
        assert!(matches!(sliced.def_level_data(), LevelDataRef::Absent));
        assert!(matches!(sliced.rep_level_data(), LevelDataRef::Absent));
        assert_eq!(selected_indices(sliced.value_selection()), vec![2, 3, 4]);
        assert_eq!(sliced.array().len(), 6);

        // Optional field: the level window covers [null, 3, null], for which
        // the selected-value window contains absolute source index 2.
        let array: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            None,
            Some(3),
            None,
            Some(5),
            Some(6),
        ]));
        let def_levels = [1, 0, 1, 0, 1, 1];
        let indices = [0, 2, 4, 5];
        let batch = LeafBatch::new(
            array.as_ref(),
            LevelDataRef::Materialized(&def_levels),
            LevelDataRef::Absent,
            ValueSelectionRef::Sparse(&indices),
        );
        let sliced = batch.slice(LevelValueWindow {
            levels: 1..4,
            values: 1..2,
        });
        assert_eq!(materialized_levels(sliced.def_level_data()), vec![0, 1, 0]);
        assert!(matches!(sliced.rep_level_data(), LevelDataRef::Absent));
        assert_eq!(selected_indices(sliced.value_selection()), vec![2]);
        assert_eq!(sliced.array().len(), 6);
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
        // selected indices: [0, 3, 8, 9]
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
        let def_levels = [3, 0, 3, 2, 0, 3, 3];
        let rep_levels = [0, 0, 0, 1, 0, 0, 1];
        let indices = [0, 3, 8, 9];
        let batch = LeafBatch::new(
            array.as_ref(),
            LevelDataRef::Materialized(&def_levels),
            LevelDataRef::Materialized(&rep_levels),
            ValueSelectionRef::Sparse(&indices),
        );

        for (window, expected_def, expected_rep, expected_indices) in [
            (
                LevelValueWindow {
                    levels: 0..2,
                    values: 0..1,
                },
                vec![3, 0],
                vec![0, 0],
                vec![0],
            ),
            (
                LevelValueWindow {
                    levels: 2..5,
                    values: 1..2,
                },
                vec![3, 2, 0],
                vec![0, 1, 0],
                vec![3],
            ),
            (
                LevelValueWindow {
                    levels: 5..7,
                    values: 2..4,
                },
                vec![3, 3],
                vec![0, 1],
                vec![8, 9],
            ),
        ] {
            let sliced = batch.slice(window);
            assert_eq!(materialized_levels(sliced.def_level_data()), expected_def);
            assert_eq!(materialized_levels(sliced.rep_level_data()), expected_rep);
            assert_eq!(selected_indices(sliced.value_selection()), expected_indices);
            assert_eq!(sliced.array().len(), 10);
        }
    }

    #[test]
    fn test_slice_for_chunk_all_null() {
        // The level window contains only null rows, so its selected-value
        // window is empty. The zero-copy slice still retains the source array.
        let array: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None, None, Some(4)]));
        let def_levels = [1, 0, 0, 1];
        let indices = [0, 3];
        let batch = LeafBatch::new(
            array.as_ref(),
            LevelDataRef::Materialized(&def_levels),
            LevelDataRef::Absent,
            ValueSelectionRef::Sparse(&indices),
        );
        let sliced = batch.slice(LevelValueWindow {
            levels: 1..3,
            values: 1..1,
        });
        assert_eq!(materialized_levels(sliced.def_level_data()), vec![0, 0]);
        assert!(selected_indices(sliced.value_selection()).is_empty());
        assert_eq!(sliced.array().len(), 4);
    }

    #[test]
    fn test_all_null_list() {
        // List<Int32> where every list slot is null.
        // Schema: list (nullable) -> item (int32, nullable)
        // Data: [null, null, null, null]
        //
        // Expected: max_def=3, max_rep=1, def/rep levels all 0.
        let item_field = Arc::new(Field::new_list_field(DataType::Int32, true));
        let list = ListArray::new_null(item_field, 4);
        let values = list.values().clone();
        let field = Field::new("list", list.data_type().clone(), true);
        let array = Arc::new(list) as ArrayRef;

        let levels = collect_leaf_output(&array, &field).unwrap();
        assert_eq!(levels.len(), 1);

        let logical_nulls = values.logical_nulls();
        let expected = CollectedLeafOutput {
            def_levels: LevelData::Uniform { value: 0, count: 4 },
            rep_levels: LevelData::Uniform { value: 0, count: 4 },
            non_null_indices: vec![],
            max_def_level: 3,
            max_rep_level: 1,
            array: values,
            logical_nulls,
        };
        assert_eq!(&levels[0], &expected);

        let required = Field::new("list", array.data_type().clone(), false);
        let error = collect_leaf_output(&array, &required).unwrap_err();
        assert_eq!(
            error.to_string(),
            "Arrow: Found null at index 0 for required field 'list'"
        );
    }

    #[test]
    fn test_all_null_fixed_size_list() {
        // FixedSizeList<Int32; 2> where every list slot is null.
        // Schema: list (nullable) -> item (int32, nullable)
        // Data: [null, null, null]
        //
        // Expected: max_def=3, max_rep=1, def/rep levels all 0.
        let item_field = Arc::new(Field::new_list_field(DataType::Int32, true));
        let list = FixedSizeListArray::new_null(item_field, 2, 3);
        let values = list.values().clone();
        let field = Field::new("list", list.data_type().clone(), true);
        let array = Arc::new(list) as ArrayRef;

        let levels = collect_leaf_output(&array, &field).unwrap();
        assert_eq!(levels.len(), 1);

        let logical_nulls = values.logical_nulls();
        let expected = CollectedLeafOutput {
            def_levels: LevelData::Uniform { value: 0, count: 3 },
            rep_levels: LevelData::Uniform { value: 0, count: 3 },
            non_null_indices: vec![],
            max_def_level: 3,
            max_rep_level: 1,
            array: values,
            logical_nulls,
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

        let levels = collect_leaf_output(&a_array, &a_field).unwrap();
        assert_eq!(levels.len(), 1);

        let expected = CollectedLeafOutput {
            def_levels: LevelData::Uniform { value: 0, count: 4 },
            rep_levels: LevelData::Absent,
            non_null_indices: vec![],
            max_def_level: 2,
            max_rep_level: 0,
            array: leaf,
            logical_nulls: Some(NullBuffer::new_null(4)),
        };
        assert_eq!(&levels[0], &expected);

        let required = Field::new("a", a_array.data_type().clone(), false);
        let error = collect_leaf_output(&a_array, &required).unwrap_err();
        assert_eq!(
            error.to_string(),
            "Arrow: Found null at index 0 for required field 'a'"
        );
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

        let levels = collect_leaf_output(&a_array, &a_field).unwrap();
        assert_eq!(levels.len(), 1);

        let expected = CollectedLeafOutput {
            def_levels: LevelData::Uniform { value: 0, count: 3 },
            rep_levels: LevelData::Absent,
            non_null_indices: vec![],
            max_def_level: 3,
            max_rep_level: 0,
            array: leaf,
            logical_nulls: Some(NullBuffer::new_null(3)),
        };
        assert_eq!(&levels[0], &expected);
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

        let levels = collect_leaf_output(&a_array, &a_field).unwrap();
        assert_eq!(levels.len(), 2);

        for (i, leaf) in [c1, c2].into_iter().enumerate() {
            let expected = CollectedLeafOutput {
                def_levels: LevelData::Uniform { value: 0, count: 2 },
                rep_levels: LevelData::Absent,
                non_null_indices: vec![],
                max_def_level: 2,
                max_rep_level: 0,
                array: leaf,
                logical_nulls: Some(NullBuffer::new_null(2)),
            };
            assert_eq!(&levels[i], &expected, "leaf {i} mismatch");
        }
    }

    #[test]
    fn compact_level_data_transitions_and_overflow() {
        let mut data = LevelData::new(true);
        data.append_run(2, 64);
        data.append_run(0, 64);
        assert!(matches!(data, LevelData::Runs(_)));
        assert_eq!(
            data.as_ref().cursor().collect::<Vec<_>>(),
            [vec![2; 64], vec![0; 64]].concat()
        );
        data.append_run(0, 16);
        assert_eq!(data.as_ref().value_count(144, 2), 64);
        for i in 0..128 {
            data.append_run(i % 2, 1);
        }
        assert!(matches!(data, LevelData::Materialized(_)));
        let mut absent = LevelData::new(false);
        absent.append_run(1, 99);
        assert!(matches!(absent, LevelData::Absent));
        let overflow = std::panic::catch_unwind(|| {
            let mut runs = LevelRuns::from_two_runs(0, usize::MAX - 1, 1, 1);
            runs.append_run(2, 1);
        });
        assert!(overflow.is_err());
    }
}
