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

use super::*;
use rand::{Rng, rng};

#[test]
fn test_loaded_row_ranges_detects_sparse_ranges() {
    assert!(!LoadedRowRanges::new(std::iter::once(0..6).collect(), 6).is_sparse());
    assert!(!LoadedRowRanges::new(vec![], 0).is_sparse());
    assert!(LoadedRowRanges::new(vec![0..2, 4..6], 6).is_sparse());
    assert!(LoadedRowRanges::new(std::iter::once(1..6).collect(), 6).is_sparse());
}

#[test]
fn test_sparse_mask_cursor_skips_unloaded_ranges() {
    let selection = RowSelection::from(vec![
        RowSelector::select(1),
        RowSelector::skip(4),
        RowSelector::select(1),
    ]);

    let loaded = LoadedRowRanges::new(vec![0..2, 4..6], 6);
    let selectors: Vec<RowSelector> = selection.into();
    let mut cursor = SparseMaskCursor::new(selectors, loaded);

    let chunk = cursor.next_sparse_mask_chunk(1024).unwrap().unwrap();
    assert_eq!(chunk.selected_rows, 2);
    assert_eq!(
        chunk.segments,
        vec![
            MaskSegment {
                row_range: 0..1,
                mask_start: 0,
                mask_len: 1,
            },
            MaskSegment {
                row_range: 5..6,
                mask_start: 5,
                mask_len: 1,
            },
        ]
    );
    assert!(cursor.is_empty());
}

#[test]
fn test_sparse_mask_cursor_errors_selected_rows_after_loaded_ranges() {
    let selection = RowSelection::from(vec![RowSelector::skip(5), RowSelector::select(1)]);

    let loaded = LoadedRowRanges::new(std::iter::once(0..2).collect(), 6);
    let selectors: Vec<RowSelector> = selection.into();
    let mut cursor = SparseMaskCursor::new(selectors, loaded);

    let err = cursor.next_sparse_mask_chunk(1024).unwrap_err();
    assert!(
        err.to_string()
            .contains("sparse mask selected row 5 outside loaded row ranges"),
        "{err}"
    );
}

#[test]
fn test_sparse_mask_cursor_exhausts_empty_loaded_ranges() {
    let selection = RowSelection::from(vec![RowSelector::select(6)]);

    let loaded = LoadedRowRanges::new(vec![], 6);
    let selectors: Vec<RowSelector> = selection.into();
    let mut cursor = SparseMaskCursor::new(selectors, loaded);

    let err = cursor.next_sparse_mask_chunk(1024).unwrap_err();
    assert!(
        err.to_string()
            .contains("sparse mask selected row 0 outside loaded row ranges"),
        "{err}"
    );
}

#[test]
fn test_from_filters() {
    let filters = vec![
        BooleanArray::from(vec![false, false, false, true, true, true, true]),
        BooleanArray::from(vec![true, true, false, false, true, true, true]),
        BooleanArray::from(vec![false, false, false, false]),
        BooleanArray::from(Vec::<bool>::new()),
    ];

    let selection = RowSelection::from_filters(&filters[..1]);
    assert!(selection.selects_any());
    assert_eq!(
        selection.selectors,
        vec![RowSelector::skip(3), RowSelector::select(4)]
    );

    let selection = RowSelection::from_filters(&filters[..2]);
    assert!(selection.selects_any());
    assert_eq!(
        selection.selectors,
        vec![
            RowSelector::skip(3),
            RowSelector::select(6),
            RowSelector::skip(2),
            RowSelector::select(3)
        ]
    );

    let selection = RowSelection::from_filters(&filters);
    assert!(selection.selects_any());
    assert_eq!(
        selection.selectors,
        vec![
            RowSelector::skip(3),
            RowSelector::select(6),
            RowSelector::skip(2),
            RowSelector::select(3),
            RowSelector::skip(4)
        ]
    );

    let selection = RowSelection::from_filters(&filters[2..3]);
    assert!(!selection.selects_any());
    assert_eq!(selection.selectors, vec![RowSelector::skip(4)]);
}

#[test]
fn test_split_off() {
    let mut selection = RowSelection::from(vec![
        RowSelector::skip(34),
        RowSelector::select(12),
        RowSelector::skip(3),
        RowSelector::select(35),
    ]);

    let split = selection.split_off(34);
    assert_eq!(split.selectors, vec![RowSelector::skip(34)]);
    assert_eq!(
        selection.selectors,
        vec![
            RowSelector::select(12),
            RowSelector::skip(3),
            RowSelector::select(35)
        ]
    );

    let split = selection.split_off(5);
    assert_eq!(split.selectors, vec![RowSelector::select(5)]);
    assert_eq!(
        selection.selectors,
        vec![
            RowSelector::select(7),
            RowSelector::skip(3),
            RowSelector::select(35)
        ]
    );

    let split = selection.split_off(8);
    assert_eq!(
        split.selectors,
        vec![RowSelector::select(7), RowSelector::skip(1)]
    );
    assert_eq!(
        selection.selectors,
        vec![RowSelector::skip(2), RowSelector::select(35)]
    );

    let split = selection.split_off(200);
    assert_eq!(
        split.selectors,
        vec![RowSelector::skip(2), RowSelector::select(35)]
    );
    assert!(selection.selectors.is_empty());
}

#[test]
fn test_offset() {
    let selection = RowSelection::from(vec![
        RowSelector::select(5),
        RowSelector::skip(23),
        RowSelector::select(7),
        RowSelector::skip(33),
        RowSelector::select(6),
    ]);

    let selection = selection.offset(2);
    assert_eq!(
        selection.selectors,
        vec![
            RowSelector::skip(2),
            RowSelector::select(3),
            RowSelector::skip(23),
            RowSelector::select(7),
            RowSelector::skip(33),
            RowSelector::select(6),
        ]
    );

    let selection = selection.offset(5);
    assert_eq!(
        selection.selectors,
        vec![
            RowSelector::skip(30),
            RowSelector::select(5),
            RowSelector::skip(33),
            RowSelector::select(6),
        ]
    );

    let selection = selection.offset(3);
    assert_eq!(
        selection.selectors,
        vec![
            RowSelector::skip(33),
            RowSelector::select(2),
            RowSelector::skip(33),
            RowSelector::select(6),
        ]
    );

    let selection = selection.offset(2);
    assert_eq!(
        selection.selectors,
        vec![RowSelector::skip(68), RowSelector::select(6),]
    );

    let selection = selection.offset(3);
    assert_eq!(
        selection.selectors,
        vec![RowSelector::skip(71), RowSelector::select(3),]
    );
}

#[test]
fn test_and() {
    let mut a = RowSelection::from(vec![
        RowSelector::skip(12),
        RowSelector::select(23),
        RowSelector::skip(3),
        RowSelector::select(5),
    ]);

    let b = RowSelection::from(vec![
        RowSelector::select(5),
        RowSelector::skip(4),
        RowSelector::select(15),
        RowSelector::skip(4),
    ]);

    let mut expected = RowSelection::from(vec![
        RowSelector::skip(12),
        RowSelector::select(5),
        RowSelector::skip(4),
        RowSelector::select(14),
        RowSelector::skip(3),
        RowSelector::select(1),
        RowSelector::skip(4),
    ]);

    assert_eq!(a.and_then(&b), expected);

    a.split_off(7);
    expected.split_off(7);
    assert_eq!(a.and_then(&b), expected);

    let a = RowSelection::from(vec![RowSelector::select(5), RowSelector::skip(3)]);

    let b = RowSelection::from(vec![
        RowSelector::select(2),
        RowSelector::skip(1),
        RowSelector::select(1),
        RowSelector::skip(1),
    ]);

    assert_eq!(
        a.and_then(&b).selectors,
        vec![
            RowSelector::select(2),
            RowSelector::skip(1),
            RowSelector::select(1),
            RowSelector::skip(4)
        ]
    );
}

#[test]
fn test_combine() {
    let a = vec![
        RowSelector::skip(3),
        RowSelector::skip(3),
        RowSelector::select(10),
        RowSelector::skip(4),
    ];

    let b = vec![
        RowSelector::skip(3),
        RowSelector::skip(3),
        RowSelector::select(10),
        RowSelector::skip(4),
        RowSelector::skip(0),
    ];

    let c = vec![
        RowSelector::skip(2),
        RowSelector::skip(4),
        RowSelector::select(3),
        RowSelector::select(3),
        RowSelector::select(4),
        RowSelector::skip(3),
        RowSelector::skip(1),
        RowSelector::skip(0),
    ];

    let expected = RowSelection::from(vec![
        RowSelector::skip(6),
        RowSelector::select(10),
        RowSelector::skip(4),
    ]);

    assert_eq!(RowSelection::from_iter(a), expected);
    assert_eq!(RowSelection::from_iter(b), expected);
    assert_eq!(RowSelection::from_iter(c), expected);
}

#[test]
fn test_combine_2elements() {
    let a = vec![RowSelector::select(10), RowSelector::select(5)];
    let a_expect = vec![RowSelector::select(15)];
    assert_eq!(RowSelection::from_iter(a).selectors, a_expect);

    let b = vec![RowSelector::select(10), RowSelector::skip(5)];
    let b_expect = vec![RowSelector::select(10), RowSelector::skip(5)];
    assert_eq!(RowSelection::from_iter(b).selectors, b_expect);

    let c = vec![RowSelector::skip(10), RowSelector::select(5)];
    let c_expect = vec![RowSelector::skip(10), RowSelector::select(5)];
    assert_eq!(RowSelection::from_iter(c).selectors, c_expect);

    let d = vec![RowSelector::skip(10), RowSelector::skip(5)];
    let d_expect = vec![RowSelector::skip(15)];
    assert_eq!(RowSelection::from_iter(d).selectors, d_expect);
}

#[test]
fn test_from_one_and_empty() {
    let a = vec![RowSelector::select(10)];
    let selection1 = RowSelection::from(a.clone());
    assert_eq!(selection1.selectors, a);

    let b = vec![];
    let selection1 = RowSelection::from(b.clone());
    assert_eq!(selection1.selectors, b)
}

#[test]
#[should_panic(expected = "selection exceeds the number of selected rows")]
fn test_and_longer() {
    let a = RowSelection::from(vec![
        RowSelector::select(3),
        RowSelector::skip(33),
        RowSelector::select(3),
        RowSelector::skip(33),
    ]);
    let b = RowSelection::from(vec![RowSelector::select(36)]);
    a.and_then(&b);
}

#[test]
#[should_panic(expected = "selection contains less than the number of selected rows")]
fn test_and_shorter() {
    let a = RowSelection::from(vec![
        RowSelector::select(3),
        RowSelector::skip(33),
        RowSelector::select(3),
        RowSelector::skip(33),
    ]);
    let b = RowSelection::from(vec![RowSelector::select(3)]);
    a.and_then(&b);
}

#[test]
fn test_intersect_row_selection_and_combine() {
    // a size equal b size
    let a = vec![
        RowSelector::select(5),
        RowSelector::skip(4),
        RowSelector::select(1),
    ];
    let b = vec![
        RowSelector::select(8),
        RowSelector::skip(1),
        RowSelector::select(1),
    ];

    let res = intersect_row_selections(&a, &b);
    assert_eq!(
        res.selectors,
        vec![
            RowSelector::select(5),
            RowSelector::skip(4),
            RowSelector::select(1),
        ],
    );

    // a size larger than b size
    let a = vec![
        RowSelector::select(3),
        RowSelector::skip(33),
        RowSelector::select(3),
        RowSelector::skip(33),
    ];
    let b = vec![RowSelector::select(36), RowSelector::skip(36)];
    let res = intersect_row_selections(&a, &b);
    assert_eq!(
        res.selectors,
        vec![RowSelector::select(3), RowSelector::skip(69)]
    );

    // a size less than b size
    let a = vec![RowSelector::select(3), RowSelector::skip(7)];
    let b = vec![
        RowSelector::select(2),
        RowSelector::skip(2),
        RowSelector::select(2),
        RowSelector::skip(2),
        RowSelector::select(2),
    ];
    let res = intersect_row_selections(&a, &b);
    assert_eq!(
        res.selectors,
        vec![RowSelector::select(2), RowSelector::skip(8)]
    );

    let a = vec![RowSelector::select(3), RowSelector::skip(7)];
    let b = vec![
        RowSelector::select(2),
        RowSelector::skip(2),
        RowSelector::select(2),
        RowSelector::skip(2),
        RowSelector::select(2),
    ];
    let res = intersect_row_selections(&a, &b);
    assert_eq!(
        res.selectors,
        vec![RowSelector::select(2), RowSelector::skip(8)]
    );
}

#[test]
fn test_and_fuzz() {
    let mut rand = rng();
    for _ in 0..100 {
        let a_len = rand.random_range(10..100);
        let a_bools: Vec<_> = (0..a_len).map(|_| rand.random_bool(0.2)).collect();
        let a = RowSelection::from_filters(&[BooleanArray::from(a_bools.clone())]);

        let b_len: usize = a_bools.iter().map(|x| *x as usize).sum();
        let b_bools: Vec<_> = (0..b_len).map(|_| rand.random_bool(0.8)).collect();
        let b = RowSelection::from_filters(&[BooleanArray::from(b_bools.clone())]);

        let mut expected_bools = vec![false; a_len];

        let mut iter_b = b_bools.iter();
        for (idx, b) in a_bools.iter().enumerate() {
            if *b && *iter_b.next().unwrap() {
                expected_bools[idx] = true;
            }
        }

        let expected = RowSelection::from_filters(&[BooleanArray::from(expected_bools)]);

        let total_rows: usize = expected.selectors.iter().map(|s| s.row_count).sum();
        assert_eq!(a_len, total_rows);

        assert_eq!(a.and_then(&b), expected);
    }
}

#[test]
fn test_iter() {
    // use the iter() API to show it does what is expected and
    // avoid accidental deletion
    let selectors = vec![
        RowSelector::select(3),
        RowSelector::skip(33),
        RowSelector::select(4),
    ];

    let round_tripped = RowSelection::from(selectors.clone())
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(selectors, round_tripped);
}

#[test]
fn test_limit() {
    // Limit to existing limit should no-op
    let selection = RowSelection::from(vec![RowSelector::select(10), RowSelector::skip(90)]);
    let limited = selection.limit(10);
    assert_eq!(RowSelection::from(vec![RowSelector::select(10)]), limited);

    let selection = RowSelection::from(vec![
        RowSelector::select(10),
        RowSelector::skip(10),
        RowSelector::select(10),
        RowSelector::skip(10),
        RowSelector::select(10),
    ]);

    let limited = selection.clone().limit(5);
    let expected = vec![RowSelector::select(5)];
    assert_eq!(limited.selectors, expected);

    let limited = selection.clone().limit(15);
    let expected = vec![
        RowSelector::select(10),
        RowSelector::skip(10),
        RowSelector::select(5),
    ];
    assert_eq!(limited.selectors, expected);

    let limited = selection.clone().limit(0);
    let expected = vec![];
    assert_eq!(limited.selectors, expected);

    let limited = selection.clone().limit(30);
    let expected = vec![
        RowSelector::select(10),
        RowSelector::skip(10),
        RowSelector::select(10),
        RowSelector::skip(10),
        RowSelector::select(10),
    ];
    assert_eq!(limited.selectors, expected);

    let limited = selection.limit(100);
    let expected = vec![
        RowSelector::select(10),
        RowSelector::skip(10),
        RowSelector::select(10),
        RowSelector::skip(10),
        RowSelector::select(10),
    ];
    assert_eq!(limited.selectors, expected);
}

#[test]
fn test_scan_ranges() {
    let index = vec![
        PageLocation {
            offset: 0,
            compressed_page_size: 10,
            first_row_index: 0,
        },
        PageLocation {
            offset: 10,
            compressed_page_size: 10,
            first_row_index: 10,
        },
        PageLocation {
            offset: 20,
            compressed_page_size: 10,
            first_row_index: 20,
        },
        PageLocation {
            offset: 30,
            compressed_page_size: 10,
            first_row_index: 30,
        },
        PageLocation {
            offset: 40,
            compressed_page_size: 10,
            first_row_index: 40,
        },
        PageLocation {
            offset: 50,
            compressed_page_size: 10,
            first_row_index: 50,
        },
        PageLocation {
            offset: 60,
            compressed_page_size: 10,
            first_row_index: 60,
        },
    ];

    let selection = RowSelection::from(vec![
        // Skip first page
        RowSelector::skip(10),
        // Multiple selects in same page
        RowSelector::select(3),
        RowSelector::skip(3),
        RowSelector::select(4),
        // Select to page boundary
        RowSelector::skip(5),
        RowSelector::select(5),
        // Skip full page past page boundary
        RowSelector::skip(12),
        // Select across page boundaries
        RowSelector::select(12),
        // Skip final page
        RowSelector::skip(12),
    ]);

    let ranges = selection.scan_ranges(&index);

    // assert_eq!(mask, vec![false, true, true, false, true, true, false]);
    assert_eq!(ranges, vec![10..20, 20..30, 40..50, 50..60]);

    let selection = RowSelection::from(vec![
        // Skip first page
        RowSelector::skip(10),
        // Multiple selects in same page
        RowSelector::select(3),
        RowSelector::skip(3),
        RowSelector::select(4),
        // Select to page boundary
        RowSelector::skip(5),
        RowSelector::select(5),
        // Skip full page past page boundary
        RowSelector::skip(12),
        // Select across page boundaries
        RowSelector::select(12),
        RowSelector::skip(1),
        // Select across page boundaries including final page
        RowSelector::select(8),
    ]);

    let ranges = selection.scan_ranges(&index);

    // assert_eq!(mask, vec![false, true, true, false, true, true, true]);
    assert_eq!(ranges, vec![10..20, 20..30, 40..50, 50..60, 60..70]);

    let selection = RowSelection::from(vec![
        // Skip first page
        RowSelector::skip(10),
        // Multiple selects in same page
        RowSelector::select(3),
        RowSelector::skip(3),
        RowSelector::select(4),
        // Select to page boundary
        RowSelector::skip(5),
        RowSelector::select(5),
        // Skip full page past page boundary
        RowSelector::skip(12),
        // Select to final page boundary
        RowSelector::select(12),
        RowSelector::skip(1),
        // Skip across final page boundary
        RowSelector::skip(8),
        // Select from final page
        RowSelector::select(4),
    ]);

    let ranges = selection.scan_ranges(&index);

    // assert_eq!(mask, vec![false, true, true, false, true, true, true]);
    assert_eq!(ranges, vec![10..20, 20..30, 40..50, 50..60, 60..70]);

    let selection = RowSelection::from(vec![
        // Skip first page
        RowSelector::skip(10),
        // Multiple selects in same page
        RowSelector::select(3),
        RowSelector::skip(3),
        RowSelector::select(4),
        // Select to remaining in page and first row of next page
        RowSelector::skip(5),
        RowSelector::select(6),
        // Skip remaining
        RowSelector::skip(50),
    ]);

    let ranges = selection.scan_ranges(&index);

    // assert_eq!(mask, vec![false, true, true, false, true, true, true]);
    assert_eq!(ranges, vec![10..20, 20..30, 30..40]);
}

#[test]
fn test_selected_page_row_ranges() {
    let selection = RowSelection::from(vec![
        RowSelector::select(1),
        RowSelector::skip(4),
        RowSelector::select(1),
    ]);
    let pages = vec![
        PageLocation {
            offset: 0,
            compressed_page_size: 10,
            first_row_index: 0,
        },
        PageLocation {
            offset: 10,
            compressed_page_size: 10,
            first_row_index: 2,
        },
        PageLocation {
            offset: 20,
            compressed_page_size: 10,
            first_row_index: 4,
        },
    ];

    assert_eq!(
        selection.selected_page_row_ranges(&pages, 6),
        vec![0..2, 4..6]
    );
}

#[test]
fn test_from_ranges() {
    let ranges = [1..3, 4..6, 6..6, 8..8, 9..10];
    let selection = RowSelection::from_consecutive_ranges(ranges.into_iter(), 10);
    assert_eq!(
        selection.selectors,
        vec![
            RowSelector::skip(1),
            RowSelector::select(2),
            RowSelector::skip(1),
            RowSelector::select(2),
            RowSelector::skip(3),
            RowSelector::select(1)
        ]
    );

    let out_of_order_ranges = [1..3, 8..10, 4..7];
    let result = std::panic::catch_unwind(|| {
        RowSelection::from_consecutive_ranges(out_of_order_ranges.into_iter(), 10)
    });
    assert!(result.is_err());
}

#[test]
fn test_empty_selector() {
    let selection = RowSelection::from(vec![
        RowSelector::skip(0),
        RowSelector::select(2),
        RowSelector::skip(0),
        RowSelector::select(2),
    ]);
    assert_eq!(selection.selectors, vec![RowSelector::select(4)]);

    let selection = RowSelection::from(vec![
        RowSelector::select(0),
        RowSelector::skip(2),
        RowSelector::select(0),
        RowSelector::skip(2),
    ]);
    assert_eq!(selection.selectors, vec![RowSelector::skip(4)]);
}

#[test]
fn test_intersection() {
    let selection = RowSelection::from(vec![RowSelector::select(1048576)]);
    let result = selection.intersection(&selection);
    assert_eq!(result, selection);

    let a = RowSelection::from(vec![
        RowSelector::skip(10),
        RowSelector::select(10),
        RowSelector::skip(10),
        RowSelector::select(20),
    ]);

    let b = RowSelection::from(vec![
        RowSelector::skip(20),
        RowSelector::select(20),
        RowSelector::skip(10),
    ]);

    let result = a.intersection(&b);
    assert_eq!(
        result.selectors,
        vec![
            RowSelector::skip(30),
            RowSelector::select(10),
            RowSelector::skip(10)
        ]
    );
}

#[test]
fn test_union() {
    let selection = RowSelection::from(vec![RowSelector::select(1048576)]);
    let result = selection.union(&selection);
    assert_eq!(result, selection);

    // NYNYY
    let a = RowSelection::from(vec![
        RowSelector::skip(10),
        RowSelector::select(10),
        RowSelector::skip(10),
        RowSelector::select(20),
    ]);

    // NNYYNYN
    let b = RowSelection::from(vec![
        RowSelector::skip(20),
        RowSelector::select(20),
        RowSelector::skip(10),
        RowSelector::select(10),
        RowSelector::skip(10),
    ]);

    let result = a.union(&b);

    // NYYYYYN
    assert_eq!(
        result.iter().collect::<Vec<_>>(),
        vec![
            &RowSelector::skip(10),
            &RowSelector::select(50),
            &RowSelector::skip(10),
        ]
    );
}

#[test]
fn test_row_count() {
    let selection = RowSelection::from(vec![
        RowSelector::skip(34),
        RowSelector::select(12),
        RowSelector::skip(3),
        RowSelector::select(35),
    ]);

    assert_eq!(selection.row_count(), 12 + 35);
    assert_eq!(selection.skipped_row_count(), 34 + 3);

    let selection = RowSelection::from(vec![RowSelector::select(12), RowSelector::select(35)]);

    assert_eq!(selection.row_count(), 12 + 35);
    assert_eq!(selection.skipped_row_count(), 0);

    let selection = RowSelection::from(vec![RowSelector::skip(34), RowSelector::skip(3)]);

    assert_eq!(selection.row_count(), 0);
    assert_eq!(selection.skipped_row_count(), 34 + 3);

    let selection = RowSelection::from(vec![]);

    assert_eq!(selection.row_count(), 0);
    assert_eq!(selection.skipped_row_count(), 0);
}

#[test]
fn test_trim() {
    let selection = RowSelection::from(vec![
        RowSelector::skip(34),
        RowSelector::select(12),
        RowSelector::skip(3),
        RowSelector::select(35),
    ]);

    let expected = vec![
        RowSelector::skip(34),
        RowSelector::select(12),
        RowSelector::skip(3),
        RowSelector::select(35),
    ];

    assert_eq!(selection.trim().selectors, expected);

    let selection = RowSelection::from(vec![
        RowSelector::skip(34),
        RowSelector::select(12),
        RowSelector::skip(3),
    ]);

    let expected = vec![RowSelector::skip(34), RowSelector::select(12)];

    assert_eq!(selection.trim().selectors, expected);
}
