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

//! Defines partition kernel for `ArrayRef`

use std::ops::Range;

use arrow_array::{Array, ArrayRef};
use arrow_buffer::BooleanBuffer;
use arrow_schema::{ArrowError, SortOptions};

use crate::cmp::{distinct, supports_distinct};
use crate::ord::make_comparator;

/// A computed set of partitions, see [`partition`]
#[derive(Debug, Clone)]
pub struct Partitions(Option<BooleanBuffer>);

impl Partitions {
    /// Returns the range of each partition
    ///
    /// Consecutive ranges will be contiguous: i.e [`(a, b)` and `(b, c)`], and
    /// `start = 0` and `end = self.len()` for the first and last range respectively
    pub fn ranges(&self) -> Vec<Range<usize>> {
        let boundaries = match &self.0 {
            Some(boundaries) => boundaries,
            None => return vec![],
        };

        let mut out = vec![];
        let mut current = 0;
        for idx in boundaries.set_indices() {
            let t = current;
            current = idx + 1;
            out.push(t..current)
        }
        let last = boundaries.len() + 1;
        if current != last {
            out.push(current..last)
        }
        out
    }

    /// Returns the number of partitions
    pub fn len(&self) -> usize {
        match &self.0 {
            Some(b) => b.count_set_bits() + 1,
            None => 0,
        }
    }

    /// Returns true if this contains no partitions
    pub fn is_empty(&self) -> bool {
        self.0.is_none()
    }
}

/// Given a list of lexicographically sorted columns, computes the [`Partitions`],
/// where a partition consists of the set of consecutive rows with equal values
///
/// Returns an error if no columns are specified or all columns do not
/// have the same number of rows.
///
/// # Example:
///
/// For example, given columns `x`, `y` and `z`, calling
/// [`partition`]`(values, (x, y))` will divide the
/// rows into ranges where the values of `(x, y)` are equal:
///
/// ```text
/// ┌ ─ ┬───┬ ─ ─┌───┐─ ─ ┬───┬ ─ ─ ┐
///     │ 1 │    │ 1 │    │ A │        Range: 0..1 (x=1, y=1)
/// ├ ─ ┼───┼ ─ ─├───┤─ ─ ┼───┼ ─ ─ ┤
///     │ 1 │    │ 2 │    │ B │
/// │   ├───┤    ├───┤    ├───┤     │
///     │ 1 │    │ 2 │    │ C │        Range: 1..4 (x=1, y=2)
/// │   ├───┤    ├───┤    ├───┤     │
///     │ 1 │    │ 2 │    │ D │
/// ├ ─ ┼───┼ ─ ─├───┤─ ─ ┼───┼ ─ ─ ┤
///     │ 2 │    │ 1 │    │ E │        Range: 4..5 (x=2, y=1)
/// ├ ─ ┼───┼ ─ ─├───┤─ ─ ┼───┼ ─ ─ ┤
///     │ 3 │    │ 1 │    │ F │        Range: 5..6 (x=3, y=1)
/// └ ─ ┴───┴ ─ ─└───┘─ ─ ┴───┴ ─ ─ ┘
///
///       x        y        z     partition(&[x, y])
/// ```
///
/// # Example Code
///
/// ```
/// # use std::{sync::Arc, ops::Range};
/// # use arrow_array::{RecordBatch, Int64Array, StringArray, ArrayRef};
/// # use arrow_ord::sort::{SortColumn, SortOptions};
/// # use arrow_ord::partition::partition;
/// let batch = RecordBatch::try_from_iter(vec![
///     ("x", Arc::new(Int64Array::from(vec![1, 1, 1, 1, 2, 3])) as ArrayRef),
///     ("y", Arc::new(Int64Array::from(vec![1, 2, 2, 2, 1, 1])) as ArrayRef),
///     ("z", Arc::new(StringArray::from(vec!["A", "B", "C", "D", "E", "F"])) as ArrayRef),
/// ]).unwrap();
///
/// // Partition on first two columns
/// let ranges = partition(&batch.columns()[..2]).unwrap().ranges();
///
/// let expected = vec![
///     (0..1),
///     (1..4),
///     (4..5),
///     (5..6),
/// ];
///
/// assert_eq!(ranges, expected);
/// ```
pub fn partition(columns: &[ArrayRef]) -> Result<Partitions, ArrowError> {
    if columns.is_empty() {
        return Err(ArrowError::InvalidArgumentError(
            "Partition requires at least one column".to_string(),
        ));
    }
    let num_rows = columns[0].len();
    if columns.iter().any(|item| item.len() != num_rows) {
        return Err(ArrowError::InvalidArgumentError(
            "Partition columns have different row counts".to_string(),
        ));
    };

    match num_rows {
        0 => return Ok(Partitions(None)),
        1 => return Ok(Partitions(Some(BooleanBuffer::new_unset(0)))),
        _ => {}
    }

    let acc = find_boundaries(&columns[0])?;
    let acc = columns
        .iter()
        .skip(1)
        .try_fold(acc, |acc, c| find_boundaries(c.as_ref()).map(|b| &acc | &b))?;

    Ok(Partitions(Some(acc)))
}

/// Returns a mask with bits set whenever the value or nullability changes
fn find_boundaries(v: &dyn Array) -> Result<BooleanBuffer, ArrowError> {
    let slice_len = v.len() - 1;
    let v1 = v.slice(0, slice_len);
    let v2 = v.slice(1, slice_len);

    if supports_distinct(v.data_type()) {
        return Ok(distinct(&v1, &v2)?.values().clone());
    }
    // Given that we're only comparing values, null ordering in the input or
    // sort options do not matter.
    let cmp = make_comparator(&v1, &v2, SortOptions::default())?;
    Ok((0..slice_len).map(|i| !cmp(i, i).is_eq()).collect())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use arrow_array::*;
    use arrow_schema::DataType;

    #[test]
    fn test_partition_empty() {
        let err = partition(&[]).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Invalid argument error: Partition requires at least one column"
        );
    }

    #[test]
    fn test_partition_unaligned_rows() {
        let input = vec![
            Arc::new(Int64Array::from(vec![None, Some(-1)])) as _,
            Arc::new(StringArray::from(vec![Some("foo")])) as _,
        ];
        let err = partition(&input).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Invalid argument error: Partition columns have different row counts"
        )
    }

    #[test]
    fn test_partition_small() {
        let results = partition(&[
            Arc::new(Int32Array::new(vec![].into(), None)) as _,
            Arc::new(Int32Array::new(vec![].into(), None)) as _,
            Arc::new(Int32Array::new(vec![].into(), None)) as _,
        ])
        .unwrap();
        assert_eq!(results.len(), 0);
        assert!(results.is_empty());

        let results = partition(&[
            Arc::new(Int32Array::from(vec![1])) as _,
            Arc::new(Int32Array::from(vec![1])) as _,
        ])
        .unwrap()
        .ranges();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], 0..1);
    }

    #[test]
    fn test_partition_single_column() {
        let a = Int64Array::from(vec![1, 2, 2, 2, 2, 2, 2, 2, 9]);
        let input = vec![Arc::new(a) as _];
        assert_eq!(
            partition(&input).unwrap().ranges(),
            vec![(0..1), (1..8), (8..9)],
        );
    }

    #[test]
    fn test_partition_all_equal_values() {
        let a = Int64Array::from_value(1, 1000);
        let input = vec![Arc::new(a) as _];
        assert_eq!(partition(&input).unwrap().ranges(), vec![(0..1000)]);
    }

    #[test]
    fn test_partition_all_null_values() {
        let input = vec![
            new_null_array(&DataType::Int8, 1000),
            new_null_array(&DataType::UInt16, 1000),
        ];
        assert_eq!(partition(&input).unwrap().ranges(), vec![(0..1000)]);
    }

    #[test]
    fn test_partition_unique_column_1() {
        let input = vec![
            Arc::new(Int64Array::from(vec![None, Some(-1)])) as _,
            Arc::new(StringArray::from(vec![Some("foo"), Some("bar")])) as _,
        ];
        assert_eq!(partition(&input).unwrap().ranges(), vec![(0..1), (1..2)],);
    }

    #[test]
    fn test_partition_unique_column_2() {
        let input = vec![
            Arc::new(Int64Array::from(vec![None, Some(-1), Some(-1)])) as _,
            Arc::new(StringArray::from(vec![
                Some("foo"),
                Some("bar"),
                Some("apple"),
            ])) as _,
        ];
        assert_eq!(
            partition(&input).unwrap().ranges(),
            vec![(0..1), (1..2), (2..3),],
        );
    }

    #[test]
    fn test_partition_non_unique_column_1() {
        let input = vec![
            Arc::new(Int64Array::from(vec![None, Some(-1), Some(-1), Some(1)])) as _,
            Arc::new(StringArray::from(vec![
                Some("foo"),
                Some("bar"),
                Some("bar"),
                Some("bar"),
            ])) as _,
        ];
        assert_eq!(
            partition(&input).unwrap().ranges(),
            vec![(0..1), (1..3), (3..4),],
        );
    }

    #[test]
    fn test_partition_masked_nulls() {
        let input = vec![
            Arc::new(Int64Array::new(vec![1; 9].into(), None)) as _,
            Arc::new(Int64Array::new(
                vec![1, 1, 2, 2, 2, 3, 3, 3, 3].into(),
                Some(vec![false, true, true, true, true, false, false, true, false].into()),
            )) as _,
            Arc::new(Int64Array::new(
                vec![1, 1, 2, 2, 2, 2, 2, 3, 7].into(),
                Some(vec![true, true, true, true, false, true, true, true, false].into()),
            )) as _,
        ];

        assert_eq!(
            partition(&input).unwrap().ranges(),
            vec![(0..1), (1..2), (2..4), (4..5), (5..7), (7..8), (8..9)],
        );
    }

    #[test]
    fn test_partition_run_end_encoded() {
        let run_ends = Int32Array::from(vec![2, 3, 5]);
        let values = StringArray::from(vec!["x", "y", "x"]);
        let ree = RunArray::try_new(&run_ends, &values).unwrap();
        // logical: ["x", "x", "y", "x", "x"]
        let input = vec![Arc::new(ree) as _];
        assert_eq!(partition(&input).unwrap().ranges(), vec![0..2, 2..3, 3..5],);
    }

    #[test]
    fn test_partition_nested_run_end_encoded() {
        // Inner REE (values of the outer): run_ends [1, 2, 3], values ["x", "y", "x"]
        // logical length 3: ["x", "y", "x"]
        let inner_run_ends = Int32Array::from(vec![1, 2, 3]);
        let inner_values = StringArray::from(vec!["x", "y", "x"]);
        let inner_ree = RunArray::try_new(&inner_run_ends, &inner_values).unwrap();

        // Outer REE: run_ends [2, 3, 5], values = inner_ree (length 3)
        // logical: rows 0,1 → inner[0]="x", row 2 → inner[1]="y", rows 3,4 → inner[2]="x"
        // = ["x", "x", "y", "x", "x"]
        let outer_run_ends = Int32Array::from(vec![2, 3, 5]);
        let outer_ree = RunArray::try_new(&outer_run_ends, &inner_ree).unwrap();

        let input = vec![Arc::new(outer_ree) as ArrayRef];
        assert_eq!(partition(&input).unwrap().ranges(), vec![0..2, 2..3, 3..5]);
    }

    #[test]
    fn test_partition_ree_with_dictionary_values() {
        // Dictionary values: keys [0, 1, 0], dict ["x", "y"] → logical ["x", "y", "x"]
        let dict_values = StringArray::from(vec!["x", "y"]);
        let keys = Int32Array::from(vec![0, 1, 0]);
        let dict = DictionaryArray::try_new(keys, Arc::new(dict_values)).unwrap();

        // REE wrapping dict: run_ends [2, 3, 5] → logical [dict[0], dict[0], dict[1], dict[2], dict[2]]
        // = ["x", "x", "y", "x", "x"]
        let run_ends = Int32Array::from(vec![2, 3, 5]);
        let ree = RunArray::try_new(&run_ends, &dict).unwrap();
        let input = vec![Arc::new(ree) as ArrayRef];
        assert_eq!(partition(&input).unwrap().ranges(), vec![0..2, 2..3, 3..5],);
    }

    #[test]
    fn test_partition_dictionary() {
        let values = StringArray::from(vec!["x", "y"]);
        let keys = Int32Array::from(vec![0, 0, 1, 0, 0]);
        let dict = DictionaryArray::try_new(keys, Arc::new(values)).unwrap();
        // logical: ["x", "x", "y", "x", "x"]
        let input = vec![Arc::new(dict) as _];
        assert_eq!(partition(&input).unwrap().ranges(), vec![0..2, 2..3, 3..5],);
    }

    #[test]
    fn test_partition_nested_dictionary() {
        let inner_values = StringArray::from(vec!["x", "y"]);
        let inner_keys = Int32Array::from(vec![0, 1, 0]);
        let inner_dict = DictionaryArray::try_new(inner_keys, Arc::new(inner_values)).unwrap();

        // Outer dict keys index into inner dict's logical values: ["x", "y", "x"]
        // keys [0, 0, 1, 2, 2] → logical ["x", "x", "y", "x", "x"]
        let outer_keys = Int32Array::from(vec![0, 0, 1, 2, 2]);
        let outer_dict = DictionaryArray::try_new(outer_keys, Arc::new(inner_dict)).unwrap();
        let input = vec![Arc::new(outer_dict) as ArrayRef];
        assert_eq!(partition(&input).unwrap().ranges(), vec![0..2, 2..3, 3..5],);
    }

    #[test]
    fn test_partition_dictionary_with_ree_values() {
        // REE values: run_ends [2, 3], values ["x", "y"] → logical ["x", "x", "y"]
        let run_ends = Int32Array::from(vec![2, 3]);
        let str_values = StringArray::from(vec!["x", "y"]);
        let ree = RunArray::try_new(&run_ends, &str_values).unwrap();

        // Dictionary keys index into the REE's logical values
        // keys [0, 0, 2, 0, 0] → logical ["x", "x", "y", "x", "x"]
        let keys = Int32Array::from(vec![0, 0, 2, 0, 0]);
        let dict = DictionaryArray::try_new(keys, Arc::new(ree)).unwrap();
        let input = vec![Arc::new(dict) as ArrayRef];
        assert_eq!(partition(&input).unwrap().ranges(), vec![0..2, 2..3, 3..5],);
    }

    #[test]
    fn test_partition_nested() {
        let input = vec![
            Arc::new(
                StructArray::try_from(vec![(
                    "f1",
                    Arc::new(Int64Array::from(vec![
                        None,
                        None,
                        Some(1),
                        Some(2),
                        Some(2),
                        Some(2),
                        Some(3),
                        Some(4),
                    ])) as _,
                )])
                .unwrap(),
            ) as _,
            Arc::new(Int64Array::from(vec![1, 1, 1, 2, 3, 3, 3, 4])) as _,
        ];
        assert_eq!(
            partition(&input).unwrap().ranges(),
            vec![0..2, 2..3, 3..4, 4..6, 6..7, 7..8]
        )
    }
}
