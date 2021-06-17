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
use std::iter::Iterator;
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
) -> Result<impl Iterator<Item = Range<usize>> + '_> {
    LexicographicalPartitionIterator::try_new(columns)
}

struct LexicographicalPartitionIterator<'a> {
    comparator: LexicographicalComparator<'a>,
    num_rows: usize,
    previous_partition_point: usize,
    partition_point: usize,
    value_indices: Vec<usize>,
}

impl<'a> LexicographicalPartitionIterator<'a> {
    fn try_new(columns: &'a [SortColumn]) -> Result<LexicographicalPartitionIterator> {
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

        let comparator = LexicographicalComparator::try_new(columns)?;
        let value_indices = (0..num_rows).collect::<Vec<usize>>();
        Ok(LexicographicalPartitionIterator {
            comparator,
            num_rows,
            previous_partition_point: 0,
            partition_point: 0,
            value_indices,
        })
    }
}

impl<'a> Iterator for LexicographicalPartitionIterator<'a> {
    type Item = Range<usize>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.partition_point < self.num_rows {
            // invariant:
            // value_indices[0..previous_partition_point] all are values <= value_indices[previous_partition_point]
            // so in order to save time we can do binary search on the value_indices[previous_partition_point..]
            // and find when any value is greater than value_indices[previous_partition_point]; because we are using
            // new indices, the new offset is _added_ to the previous_partition_point.
            //
            // be careful that idx is of type &usize which points to the actual value within value_indices, which itself
            // contains usize (0..row_count), providing access to lexicographical_comparator as pointers into the
            // original columnar data.
            self.partition_point += self.value_indices[self.partition_point..]
                .partition_point(|idx| {
                    self.comparator.compare(idx, &self.partition_point)
                        != Ordering::Greater
                });
            let start = self.previous_partition_point;
            let end = self.partition_point;
            self.previous_partition_point = self.partition_point;
            Some(Range { start, end })
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::*;
    use crate::compute::SortOptions;
    use crate::datatypes::DataType;
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
            let results = lexicographical_partition_ranges(&input)?;
            assert_eq!(
                vec![(0_usize..1_usize), (1_usize..8_usize), (8_usize..9_usize)],
                results.collect::<Vec<_>>()
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
            let results = lexicographical_partition_ranges(&input)?;
            assert_eq!(vec![(0_usize..1000_usize)], results.collect::<Vec<_>>());
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
            let results = lexicographical_partition_ranges(&input)?;
            assert_eq!(vec![(0_usize..1000_usize)], results.collect::<Vec<_>>());
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
            let results = lexicographical_partition_ranges(&input)?;
            assert_eq!(
                vec![(0_usize..1_usize), (1_usize..2_usize)],
                results.collect::<Vec<_>>()
            );
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
            let results = lexicographical_partition_ranges(&input)?;
            assert_eq!(
                vec![(0_usize..1_usize), (1_usize..2_usize), (2_usize..3_usize),],
                results.collect::<Vec<_>>()
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
            let results = lexicographical_partition_ranges(&input)?;
            assert_eq!(
                vec![(0_usize..1_usize), (1_usize..3_usize), (3_usize..4_usize),],
                results.collect::<Vec<_>>()
            );
        }
        Ok(())
    }
}
