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

use arrow_array::builder::{BinaryViewBuilder, StringViewBuilder};
use arrow_array::cast::AsArray;
use arrow_array::types::StringViewType;
use arrow_array::{Array, StringViewArray};
use arrow_buffer::NullBuffer;
use arrow_schema::ArrowError;
use arrow_select::interleave::{interleave, interleave_byte_view_compact};

#[test]
fn sliced_multibuffer_string_sources() {
    let mut first = StringViewBuilder::new().with_fixed_block_size(32);
    first.append_value("discard before slice");
    first.append_value("first selected long string");
    first.append_null();
    first.append_value("second selected long string");
    first.append_value("discard after slice");
    let first = first.finish().slice(1, 3);

    let mut second = StringViewBuilder::new().with_fixed_block_size(32);
    second.append_value("discard before slice");
    second.append_value("third selected long string");
    second.append_value("short");
    second.append_value("fourth selected long string");
    let second = second.finish().slice(1, 3);
    assert!(first.data_buffers().len() > 1);
    assert!(second.data_buffers().len() > 1);

    let indices = [(1, 2), (0, 1), (0, 2), (1, 1), (0, 0), (1, 2)];
    let compact = interleave_byte_view_compact(&[&first, &second], &indices).unwrap();
    let ordinary = interleave(&[&first, &second], &indices).unwrap();
    assert_eq!(
        compact.iter().collect::<Vec<_>>(),
        ordinary.as_string_view().iter().collect::<Vec<_>>()
    );
    assert_eq!(
        compact.iter().collect::<Vec<_>>(),
        vec![
            Some("fourth selected long string"),
            None,
            Some("second selected long string"),
            Some("short"),
            Some("first selected long string"),
            Some("fourth selected long string"),
        ]
    );
}

#[test]
fn binary_values_across_inline_boundary() {
    let inline = [0xff; 12];
    let external = [0xfe; 13];
    let mut input = BinaryViewBuilder::new();
    input.append_value(inline);
    input.append_value(external);
    input.append_null();
    input.append_value([]);
    let input = input.finish();

    let indices = [(0, 1), (0, 0), (0, 2), (0, 3), (0, 1)];
    let compact = interleave_byte_view_compact(&[&input], &indices).unwrap();
    let ordinary = interleave(&[&input], &indices).unwrap();
    assert_eq!(
        compact.iter().collect::<Vec<_>>(),
        ordinary.as_binary_view().iter().collect::<Vec<_>>()
    );
    assert_eq!(compact.value(0), &external);
    assert_eq!(compact.value(1), &inline);
    assert!(compact.is_null(2));
    assert_eq!(compact.value(3), b"");
    assert_eq!(
        compact
            .data_buffers()
            .iter()
            .map(|b| b.len())
            .sum::<usize>(),
        2 * external.len()
    );
}

#[test]
fn copies_only_selected_nonnull_payload() {
    let unselected = "u".repeat(32 * 1024);
    let hidden = "n".repeat(32 * 1024);
    let selected = "selected long string";
    let input = StringViewArray::from(vec![unselected.as_str(), selected, hidden.as_str()]);
    // Keep a valid, nonempty view underneath the null bit.
    let input = StringViewArray::try_new(
        input.views().clone(),
        input.data_buffers().clone(),
        Some(NullBuffer::from(vec![true, true, false])),
    )
    .unwrap();
    let indices = [(0, 1), (0, 2), (0, 1)];
    let compact = interleave_byte_view_compact(&[&input], &indices).unwrap();
    let ordinary = interleave(&[&input], &indices).unwrap();
    assert_eq!(
        compact.iter().collect::<Vec<_>>(),
        ordinary.as_string_view().iter().collect::<Vec<_>>()
    );
    assert_eq!(
        compact
            .data_buffers()
            .iter()
            .map(|b| b.len())
            .sum::<usize>(),
        2 * selected.len()
    );
    assert!(
        compact
            .data_buffers()
            .iter()
            .map(|b| b.capacity())
            .sum::<usize>()
            < 1024
    );
    for output in compact.data_buffers().iter() {
        for source in input.data_buffers().iter() {
            // Compare allocation starts, including when a buffer is a slice.
            assert_ne!(
                output.as_ptr() as usize - output.ptr_offset(),
                source.as_ptr() as usize - source.ptr_offset()
            );
        }
    }
    assert_ne!(compact.views().as_ptr(), input.views().as_ptr());
    assert_ne!(
        compact.nulls().unwrap().buffer().as_ptr(),
        input.nulls().unwrap().buffer().as_ptr()
    );
    drop(ordinary);
    drop(input);
    assert_eq!(
        compact.iter().collect::<Vec<_>>(),
        vec![Some(selected), None, Some(selected)]
    );
}

#[test]
fn empty_null_and_inline_selections_need_no_data_buffers() {
    let input = StringViewArray::from(vec![Some("unselected long string"), None, Some("inline")]);
    for indices in [vec![], vec![(0, 1), (0, 1)], vec![(0, 2), (0, 1)]] {
        let compact = interleave_byte_view_compact(&[&input], &indices).unwrap();
        let ordinary = interleave(&[&input], &indices).unwrap();
        assert_eq!(
            compact.iter().collect::<Vec<_>>(),
            ordinary.as_string_view().iter().collect::<Vec<_>>()
        );
        assert!(compact.data_buffers().is_empty());
    }
    let empty = StringViewArray::from(Vec::<&str>::new());
    assert!(
        interleave_byte_view_compact(&[&empty], &[])
            .unwrap()
            .is_empty()
    );
}

#[test]
fn missing_input_is_an_error_even_for_empty_selection() {
    let error = interleave_byte_view_compact::<StringViewType>(&[], &[]).unwrap_err();
    assert!(matches!(error, ArrowError::InvalidArgumentError(_)));
}
