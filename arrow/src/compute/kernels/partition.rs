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

use crate::compute::kernels::sort::LexicographicalComparator;
use crate::compute::SortColumn;
use crate::error::{ArrowError, Result};
use std::cmp::Ordering;
use std::ops::Range;

/// Given a list of already sorted columns, find partition ranges that would partition
/// lexicographically equal values across columns.
///
/// Here LexicographicalComparator is used in conjunction with binary
/// search so the columns *MUST* be pre-sorted already.
///
/// The returned vec would be of size k where k is cardinality of the sorted values; Consecutive
/// values will be connected: (a, b) and (b, c), where start = 0 and end = n for the first and last
/// range.
pub fn lexicographical_partition_ranges(
    columns: &[SortColumn],
) -> Result<Vec<Range<usize>>> {
    let partition_points = lexicographical_partition_points(columns)?;
    Ok(partition_points
        .iter()
        .zip(partition_points[1..].iter())
        .map(|(&start, &end)| Range { start, end })
        .collect())
}

/// Given a list of already sorted columns, find partition ranges that would partition
/// lexicographically equal values across columns.
///
/// Here LexicographicalComparator is used in conjunction with binary
/// search so the columns *MUST* be pre-sorted already.
///
/// The returned vec would be of size k+1 where k is cardinality of the sorted values; the first and
/// last value would be 0 and n.
fn lexicographical_partition_points(columns: &[SortColumn]) -> Result<Vec<usize>> {
    if columns.is_empty() {
        return Err(ArrowError::InvalidArgumentError(
            "Sort requires at least one column".to_string(),
        ));
    }
    let row_count = columns[0].values.len();
    if columns.iter().any(|item| item.values.len() != row_count) {
        return Err(ArrowError::ComputeError(
            "Lexical sort columns have different row counts".to_string(),
        ));
    };

    let mut result = vec![];
    if row_count == 0 {
        return Ok(result);
    }

    let lexicographical_comparator = LexicographicalComparator::try_new(columns)?;
    let value_indices = (0..row_count).collect::<Vec<usize>>();

    let mut previous_partition_point = 0;
    result.push(previous_partition_point);
    while previous_partition_point < row_count {
        // invariant:
        // value_indices[0..previous_partition_point] all are values <= value_indices[previous_partition_point]
        // so in order to save time we can do binary search on the value_indices[previous_partition_point..]
        // and find when any value is greater than value_indices[previous_partition_point]; because we are using
        // new indices, the new offset is _added_ to the previous_partition_point.
        //
        // be careful that idx is of type &usize which points to the actual value within value_indices, which itself
        // contains usize (0..row_count), providing access to lexicographical_comparator as pointers into the
        // original columnar data.
        previous_partition_point += value_indices[previous_partition_point..]
            .partition_point(|idx| {
                lexicographical_comparator.compare(idx, &previous_partition_point)
                    != Ordering::Greater
            });
        result.push(previous_partition_point);
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::*;
    use crate::compute::SortOptions;
    use crate::datatypes::DataType;
    use std::sync::Arc;

    #[test]
    fn test_lexicographical_partition_points_empty() {
        let input = vec![];
        assert!(
            lexicographical_partition_points(&input).is_err(),
            "lexicographical_partition_points should reject columns with empty rows"
        );
    }

    #[test]
    fn test_lexicographical_partition_points_unaligned_rows() {
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
            lexicographical_partition_points(&input).is_err(),
            "lexicographical_partition_points should reject columns with different row counts"
        );
    }

    #[test]
    fn test_lexicographical_partition_single_column() -> Result<()> {
        let input = vec![SortColumn {
            values: Arc::new(Int64Array::from(vec![1, 2, 2, 2, 2, 2, 2, 2, 9]))
                as ArrayRef,
            options: Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
        }];
        {
            let results = lexicographical_partition_points(&input)?;
            assert_eq!(vec![0, 1, 8, 9], results);
        }
        {
            let results = lexicographical_partition_ranges(&input)?;
            assert_eq!(
                vec![(0_usize..1_usize), (1_usize..8_usize), (8_usize..9_usize)],
                results
            );
        }
        Ok(())
    }

    #[test]
    fn test_lexicographical_partition_all_equal_values() -> Result<()> {
        let input = vec![SortColumn {
            values: Arc::new(Int64Array::from_value(1, 1000)) as ArrayRef,
            options: Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
        }];
        {
            let results = lexicographical_partition_points(&input)?;
            assert_eq!(vec![0, 1000], results);
        }
        {
            let results = lexicographical_partition_ranges(&input)?;
            assert_eq!(vec![(0_usize..1000_usize)], results);
        }
        Ok(())
    }

    #[test]
    fn test_lexicographical_partition_all_null_values() -> Result<()> {
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
        {
            let results = lexicographical_partition_points(&input)?;
            assert_eq!(vec![0, 1000], results);
        }
        {
            let results = lexicographical_partition_ranges(&input)?;
            assert_eq!(vec![(0_usize..1000_usize)], results);
        }
        Ok(())
    }

    #[test]
    fn test_lexicographical_partition_unique_column_1() -> Result<()> {
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
        {
            let results = lexicographical_partition_points(&input)?;
            assert_eq!(vec![0, 1, 2], results);
        }
        {
            let results = lexicographical_partition_ranges(&input)?;
            assert_eq!(vec![(0_usize..1_usize), (1_usize..2_usize)], results);
        }
        Ok(())
    }

    #[test]
    fn test_lexicographical_partition_unique_column_2() -> Result<()> {
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
        {
            let results = lexicographical_partition_points(&input)?;
            assert_eq!(vec![0, 1, 2, 3], results);
        }
        {
            let results = lexicographical_partition_ranges(&input)?;
            assert_eq!(
                vec![(0_usize..1_usize), (1_usize..2_usize), (2_usize..3_usize),],
                results
            );
        }
        Ok(())
    }

    #[test]
    fn test_lexicographical_partition_non_unique_column_1() -> Result<()> {
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
        {
            let results = lexicographical_partition_points(&input)?;
            assert_eq!(vec![0, 1, 3, 4], results);
        }
        {
            let results = lexicographical_partition_ranges(&input)?;
            assert_eq!(
                vec![(0_usize..1_usize), (1_usize..3_usize), (3_usize..4_usize),],
                results
            );
        }
        Ok(())
    }
}
