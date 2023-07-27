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

/// Given a list of already sorted columns, find partition ranges that would partition
/// lexicographically equal values across columns.
///
/// The returned vec would be of size k where k is cardinality of the sorted values; Consecutive
/// values will be connected: (a, b) and (b, c), where start = 0 and end = n for the first and last
/// range.
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
    let values_ne = match array_ne.nulls().filter(|n| n.null_count() > 0) {
        Some(n) => n.inner() & array_ne.values(),
        None => array_ne.values().clone(),
    };

    Ok(match v.nulls().filter(|x| x.null_count() > 0) {
        Some(n) => {
            let n1 = n.inner().slice(0, slice_len);
            let n2 = n.inner().slice(1, slice_len);
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
