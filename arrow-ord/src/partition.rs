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

use crate::comparison::neq_dyn;
use crate::sort::SortColumn;
use arrow_array::Array;
use arrow_buffer::BooleanBuffer;
use arrow_schema::ArrowError;
use std::ops::Range;

/// Given a list of already sorted columns, returns [`Range`]es that
/// partition the input such that each partition has equal values
/// across sort columns.
///
/// Returns an error if no columns are specified or all columns do not
/// have the same number of rows.
///
/// Returns an iterator with `k` items where `k` is cardinality of the
/// sort values: Consecutive values will be connected: `(a, b)` and `(b,
/// c)`, where `start = 0` and `end = n` for the first and last range.
///
/// # Example:
///
/// For example, given columns `x`, `y` and `z`, calling
/// `lexicographical_partition_ranges(values, (x, y))` will divide the
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
///       x        y        z     lexicographical_partition_ranges
///                               by (x,y)
/// ```
///
/// # Example Code
///
/// ```
/// # use std::{sync::Arc, ops::Range};
/// # use arrow_array::{RecordBatch, Int64Array, StringArray, ArrayRef};
/// # use arrow_ord::sort::{SortColumn, SortOptions};
/// # use arrow_ord::partition::lexicographical_partition_ranges;
/// let batch = RecordBatch::try_from_iter(vec![
///     ("x", Arc::new(Int64Array::from(vec![1, 1, 1, 1, 2, 3])) as ArrayRef),
///     ("y", Arc::new(Int64Array::from(vec![1, 2, 2, 2, 1, 1])) as ArrayRef),
///     ("z", Arc::new(StringArray::from(vec!["A", "B", "C", "D", "E", "F"])) as ArrayRef),
/// ]).unwrap();
///
/// // Lexographically partition on (x, y)
/// let sort_columns = vec![
///     SortColumn {
///         values: batch.column(0).clone(),
///         options: Some(SortOptions::default()),
///     },
///     SortColumn {
///         values: batch.column(1).clone(),
///         options: Some(SortOptions::default()),
///  },
/// ];
/// let ranges:Vec<Range<usize>> = lexicographical_partition_ranges(&sort_columns)
///   .unwrap()
///   .collect();
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
pub fn lexicographical_partition_ranges(
    columns: &[SortColumn],
) -> Result<impl Iterator<Item = Range<usize>> + '_, ArrowError> {
    if columns.is_empty() {
        return Err(ArrowError::InvalidArgumentError(
            "Sort requires at least one column".to_string(),
        ));
    }
    let num_rows = columns[0].values.len();
    if columns.iter().any(|item| item.values.len() != num_rows) {
        return Err(ArrowError::ComputeError(
            "Lexical sort columns have different row counts".to_string(),
        ));
    };

    let acc = find_boundaries(&columns[0])?;
    let acc = columns
        .iter()
        .skip(1)
        .try_fold(acc, |acc, c| find_boundaries(c).map(|b| &acc | &b))?;

    let mut out = vec![];
    let mut current = 0;
    for idx in acc.set_indices() {
        let t = current;
        current = idx + 1;
        out.push(t..current)
    }
    if current != num_rows {
        out.push(current..num_rows)
    }
    Ok(out.into_iter())
}

/// Returns a mask with bits set whenever the value or nullability changes
fn find_boundaries(col: &SortColumn) -> Result<BooleanBuffer, ArrowError> {
    let v = &col.values;
    let slice_len = v.len().saturating_sub(1);
    let v1 = v.slice(0, slice_len);
    let v2 = v.slice(1, slice_len);

    let array_ne = neq_dyn(v1.as_ref(), v2.as_ref())?;
    // Set if values have different non-NULL values
    let values_ne = match array_ne.nulls().filter(|n| n.null_count() > 0) {
        Some(n) => n.inner() & array_ne.values(),
        None => array_ne.values().clone(),
    };

    Ok(match v.nulls().filter(|x| x.null_count() > 0) {
        Some(n) => {
            let n1 = n.inner().slice(0, slice_len);
            let n2 = n.inner().slice(1, slice_len);
            // Set if values_ne or the nullability has changed
            &(&n1 ^ &n2) | &values_ne
        }
        None => values_ne,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sort::SortOptions;
    use arrow_array::*;
    use arrow_schema::DataType;
    use std::sync::Arc;

    #[test]
    fn test_lexicographical_partition_ranges_empty() {
        let input = vec![];
        assert!(
            lexicographical_partition_ranges(&input).is_err(),
            "lexicographical_partition_ranges should reject columns with empty rows"
        );
    }

    #[test]
    fn test_lexicographical_partition_ranges_unaligned_rows() {
        let input = vec![
            SortColumn {
                values: Arc::new(Int64Array::from(vec![None, Some(-1)])) as ArrayRef,
                options: None,
            },
            SortColumn {
                values: Arc::new(StringArray::from(vec![Some("foo")])) as ArrayRef,
                options: None,
            },
        ];
        assert!(
            lexicographical_partition_ranges(&input).is_err(),
            "lexicographical_partition_ranges should reject columns with different row counts"
        );
    }

    #[test]
    fn test_lexicographical_partition_single_column() {
        let input = vec![SortColumn {
            values: Arc::new(Int64Array::from(vec![1, 2, 2, 2, 2, 2, 2, 2, 9]))
                as ArrayRef,
            options: Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
        }];
        let results = lexicographical_partition_ranges(&input).unwrap();
        assert_eq!(
            vec![(0_usize..1_usize), (1_usize..8_usize), (8_usize..9_usize)],
            results.collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_lexicographical_partition_all_equal_values() {
        let input = vec![SortColumn {
            values: Arc::new(Int64Array::from_value(1, 1000)) as ArrayRef,
            options: Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
        }];

        let results = lexicographical_partition_ranges(&input).unwrap();
        assert_eq!(vec![(0_usize..1000_usize)], results.collect::<Vec<_>>());
    }

    #[test]
    fn test_lexicographical_partition_all_null_values() {
        let input = vec![
            SortColumn {
                values: new_null_array(&DataType::Int8, 1000),
                options: Some(SortOptions {
                    descending: false,
                    nulls_first: true,
                }),
            },
            SortColumn {
                values: new_null_array(&DataType::UInt16, 1000),
                options: Some(SortOptions {
                    descending: false,
                    nulls_first: false,
                }),
            },
        ];
        let results = lexicographical_partition_ranges(&input).unwrap();
        assert_eq!(vec![(0_usize..1000_usize)], results.collect::<Vec<_>>());
    }

    #[test]
    fn test_lexicographical_partition_unique_column_1() {
        let input = vec![
            SortColumn {
                values: Arc::new(Int64Array::from(vec![None, Some(-1)])) as ArrayRef,
                options: Some(SortOptions {
                    descending: false,
                    nulls_first: true,
                }),
            },
            SortColumn {
                values: Arc::new(StringArray::from(vec![Some("foo"), Some("bar")]))
                    as ArrayRef,
                options: Some(SortOptions {
                    descending: true,
                    nulls_first: true,
                }),
            },
        ];
        let results = lexicographical_partition_ranges(&input).unwrap();
        assert_eq!(
            vec![(0_usize..1_usize), (1_usize..2_usize)],
            results.collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_lexicographical_partition_unique_column_2() {
        let input = vec![
            SortColumn {
                values: Arc::new(Int64Array::from(vec![None, Some(-1), Some(-1)]))
                    as ArrayRef,
                options: Some(SortOptions {
                    descending: false,
                    nulls_first: true,
                }),
            },
            SortColumn {
                values: Arc::new(StringArray::from(vec![
                    Some("foo"),
                    Some("bar"),
                    Some("apple"),
                ])) as ArrayRef,
                options: Some(SortOptions {
                    descending: true,
                    nulls_first: true,
                }),
            },
        ];
        let results = lexicographical_partition_ranges(&input).unwrap();
        assert_eq!(
            vec![(0_usize..1_usize), (1_usize..2_usize), (2_usize..3_usize),],
            results.collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_lexicographical_partition_non_unique_column_1() {
        let input = vec![
            SortColumn {
                values: Arc::new(Int64Array::from(vec![
                    None,
                    Some(-1),
                    Some(-1),
                    Some(1),
                ])) as ArrayRef,
                options: Some(SortOptions {
                    descending: false,
                    nulls_first: true,
                }),
            },
            SortColumn {
                values: Arc::new(StringArray::from(vec![
                    Some("foo"),
                    Some("bar"),
                    Some("bar"),
                    Some("bar"),
                ])) as ArrayRef,
                options: Some(SortOptions {
                    descending: true,
                    nulls_first: true,
                }),
            },
        ];
        let results = lexicographical_partition_ranges(&input).unwrap();
        assert_eq!(
            vec![(0_usize..1_usize), (1_usize..3_usize), (3_usize..4_usize),],
            results.collect::<Vec<_>>()
        );
    }
}
