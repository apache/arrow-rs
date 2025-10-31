use crate::filter::SlicesIterator;
use arrow_array::{make_array, new_empty_array, Array, ArrayRef, BooleanArray, Datum};
use arrow_data::transform::MutableArrayData;
use arrow_data::ArrayData;
use arrow_schema::ArrowError;

/// An index for the [merge] function.
///
/// This trait allows the indices argument for [merge] to be stored using a more
/// compact representation than `usize` when the input arrays are small.
/// If the number of input arrays is less than 256 for instance, the indices can be stored as `u8`.
///
/// Implementation must ensure that all values which return `None` from [MergeIndex::index] are
/// considered equal by the [PartialEq] and [Eq] implementations.
pub trait MergeIndex : PartialEq + Eq + Copy {
    /// Returns the index value as an `Option<usize>`.
    ///
    /// `None` values returned by this function indicate holes in the index array and will result
    /// in null values in the array created by [merge].
    fn index(&self) -> Option<usize>;
}

impl MergeIndex for usize {
    fn index(&self) -> Option<usize> {
        Some(*self)
    }
}

impl MergeIndex for Option<usize> {
    fn index(&self) -> Option<usize> {
        *self
    }
}

/// Merges elements by index from a list of [`Array`], creating a new [`Array`] from
/// those values.
///
/// Each element in `indices` is the index of an array in `values`. The `indices` array is processed
/// sequentially. The first occurrence of index value `n` will be mapped to the first
/// value of the array at index `n`. The second occurrence to the second value, and so on.
/// An index value where `MergeIndex::index` returns `None` is interpreted as a null value.
///
/// # Implementation notes
///
/// This algorithm is similar in nature to both `zip` and `interleave`, but there are some important
/// differences.
///
/// In contrast to `zip`, this function supports multiple input arrays. Instead of a boolean
/// selection vector, an index array is to take values from the input arrays, and a special marker
/// value is used to indicate null values.
///
/// In contrast to `interleave`, this function does not use pairs of indices. The values in
/// `indices` serve the same purpose as the first value in the pairs passed to `interleave`.
/// The index in the array is implicit and is derived from the number of times a particular array
/// index occurs.
/// The more constrained indexing mechanism used by this algorithm makes it easier to copy values
/// in contiguous slices. In the example below, the two subsequent elements from array `2` can be
/// copied in a single operation from the source array instead of copying them one by one.
/// Long spans of null values are also especially cheap because they do not need to be represented
/// in an input array.
///
/// # Safety
///
/// This function does not check that the number of occurrences of any particular array index matches
/// the length of the corresponding input array. If an array contains more values than required, the
/// spurious values will be ignored. If an array contains fewer values than necessary, this function
/// will panic.
///
/// # Example
///
/// ```text
/// ┌───────────┐  ┌─────────┐                             ┌─────────┐
/// │┌─────────┐│  │   None  │                             │   NULL  │
/// ││    A    ││  ├─────────┤                             ├─────────┤
/// │└─────────┘│  │    1    │                             │    B    │
/// │┌─────────┐│  ├─────────┤                             ├─────────┤
/// ││    B    ││  │    0    │    merge(values, indices)   │    A    │
/// │└─────────┘│  ├─────────┤  ─────────────────────────▶ ├─────────┤
/// │┌─────────┐│  │   None  │                             │   NULL  │
/// ││    C    ││  ├─────────┤                             ├─────────┤
/// │├─────────┤│  │    2    │                             │    C    │
/// ││    D    ││  ├─────────┤                             ├─────────┤
/// │└─────────┘│  │    2    │                             │    D    │
/// └───────────┘  └─────────┘                             └─────────┘
///    values        indices                                  result
///
/// ```
pub fn merge_n(values: &[&dyn Array], indices: &[impl MergeIndex]) -> Result<ArrayRef, ArrowError> {
    let data_type = values[0].data_type();

    for array in values.iter().skip(1) {
        if array.data_type() != data_type {
            return Err(ArrowError::InvalidArgumentError(format!(
                "It is not possible to merge arrays of different data types ({} and {})",
                data_type,
                array.data_type()
            )));
        }
    }

    if indices.is_empty() {
        return Ok(new_empty_array(data_type));
    }

    #[cfg(debug_assertions)]
    for ix in indices {
        if let Some(index) = ix.index() {
            assert!(
                index < values.len(),
                "Index out of bounds: {} >= {}",
                index,
                values.len()
            );
        }
    }

    let data: Vec<ArrayData> = values.iter().map(|a| a.to_data()).collect();
    let data_refs = data.iter().collect();

    let mut mutable = MutableArrayData::new(data_refs, true, indices.len());

    // This loop extends the mutable array by taking slices from the partial results.
    //
    // take_offsets keeps track of how many values have been taken from each array.
    let mut take_offsets = vec![0; values.len() + 1];
    let mut start_row_ix = 0;
    loop {
        let array_ix = indices[start_row_ix];

        // Determine the length of the slice to take.
        let mut end_row_ix = start_row_ix + 1;
        while end_row_ix < indices.len() && indices[end_row_ix] == array_ix {
            end_row_ix += 1;
        }
        let slice_length = end_row_ix - start_row_ix;

        // Extend mutable with either nulls or with values from the array.
        match array_ix.index() {
            None => mutable.extend_nulls(slice_length),
            Some(index) => {
                let start_offset = take_offsets[index];
                let end_offset = start_offset + slice_length;
                mutable.extend(index, start_offset, end_offset);
                take_offsets[index] = end_offset;
            }
        }

        if end_row_ix == indices.len() {
            break;
        } else {
            // Set the start_row_ix for the next slice.
            start_row_ix = end_row_ix;
        }
    }

    Ok(make_array(mutable.freeze()))
}

/// Merges two arrays in the order specified by a boolean mask.
///
/// This algorithm is a variant of [zip] that does not require the truthy and
/// falsy arrays to have the same length.
///
/// # Example
///
/// ```text
///  truthy
/// ┌─────────┐   mask
/// │    A    │  ┌─────────┐                             ┌─────────┐
/// ├─────────┤  │  true   │                             │    A    │
/// │    B    │  ├─────────┤                             ├─────────┤
/// └─────────┘  │  false  │  merge(mask, truthy, falsy) │    C    │
///  falsy       ├─────────┤  ─────────────────────────▶ ├─────────┤
/// ┌─────────┐  │  true   │                             │    B    │
/// │    C    │  ├─────────┤                             ├─────────┤
/// ├─────────┤  │  false  │                             │   NULL  │
/// │   NULL  │  └─────────┘                             └─────────┘
/// └─────────┘
/// ```
pub fn merge(
    mask: &BooleanArray,
    truthy: &dyn Datum,
    falsy: &dyn Datum,
) -> Result<ArrayRef, ArrowError> {
    let (truthy, truthy_is_scalar) = truthy.get();
    let (falsy, falsy_is_scalar) = falsy.get();

    if truthy.data_type() != falsy.data_type() {
        return Err(ArrowError::InvalidArgumentError(
            "arguments need to have the same data type".into(),
        ));
    }

    if truthy_is_scalar && truthy.len() != 1 {
        return Err(ArrowError::InvalidArgumentError(
            "scalar arrays must have 1 element".into(),
        ));
    }
    if falsy_is_scalar && falsy.len() != 1 {
        return Err(ArrowError::InvalidArgumentError(
            "scalar arrays must have 1 element".into(),
        ));
    }

    let falsy = falsy.to_data();
    let truthy = truthy.to_data();

    let mut mutable = MutableArrayData::new(vec![&truthy, &falsy], false, truthy.len());

    // the SlicesIterator slices only the true values. So the gaps left by this iterator we need to
    // fill with falsy values

    // keep track of how much is filled
    let mut filled = 0;
    let mut falsy_offset = 0;
    let mut truthy_offset = 0;

    SlicesIterator::new(mask).for_each(|(start, end)| {
        // the gap needs to be filled with falsy values
        if start > filled {
            if falsy_is_scalar {
                for _ in filled..start {
                    // Copy the first item from the 'falsy' array into the output buffer.
                    mutable.extend(1, 0, 1);
                }
            } else {
                let falsy_length = start - filled;
                let falsy_end = falsy_offset + falsy_length;
                mutable.extend(1, falsy_offset, falsy_end);
                falsy_offset = falsy_end;
            }
        }
        // fill with truthy values
        if truthy_is_scalar {
            for _ in start..end {
                // Copy the first item from the 'truthy' array into the output buffer.
                mutable.extend(0, 0, 1);
            }
        } else {
            let truthy_length = end - start;
            let truthy_end = truthy_offset + truthy_length;
            mutable.extend(0, truthy_offset, truthy_end);
            truthy_offset = truthy_end;
        }
        filled = end;
    });
    // the remaining part is falsy
    if filled < mask.len() {
        if falsy_is_scalar {
            for _ in filled..mask.len() {
                // Copy the first item from the 'falsy' array into the output buffer.
                mutable.extend(1, 0, 1);
            }
        } else {
            let falsy_length = mask.len() - filled;
            let falsy_end = falsy_offset + falsy_length;
            mutable.extend(1, falsy_offset, falsy_end);
        }
    }

    let data = mutable.freeze();
    Ok(make_array(data))
}

#[cfg(test)]
mod tests {
    use crate::merge::{merge, merge_n, MergeIndex};
    use arrow_array::cast::AsArray;
    use arrow_array::{Array, BooleanArray, StringArray};

    #[derive(PartialEq, Eq, Copy, Clone)]
    struct CompactMergeIndex {
        index: u8,
    }

    impl MergeIndex for CompactMergeIndex {
        fn index(&self) -> Option<usize> {
            if self.index == u8::MAX {
                None
            } else {
                Some(self.index as usize)
            }
        }
    }

    #[test]
    fn test_merge() {
        let a1 = StringArray::from(vec![Some("A"), Some("B"), Some("E"), None]);
        let a2 = StringArray::from(vec![Some("C"), Some("D")]);

        let indices = BooleanArray::from(vec![
            true,
            false,
            true,
            false,
            true,
            true
        ]);

        let merged = merge(&indices, &a1, &a2).unwrap();
        let merged = merged.as_string::<i32>();

        assert_eq!(merged.len(), indices.len());
        assert!(merged.is_valid(0));
        assert_eq!(merged.value(0), "A");
        assert!(merged.is_valid(1));
        assert_eq!(merged.value(1), "C");
        assert!(merged.is_valid(2));
        assert_eq!(merged.value(2), "B");
        assert!(merged.is_valid(3));
        assert_eq!(merged.value(3), "D");
        assert!(merged.is_valid(4));
        assert_eq!(merged.value(4), "E");
        assert!(!merged.is_valid(5));
    }

    #[test]
    fn test_merge_n() {
        let a1 = StringArray::from(vec![Some("A")]);
        let a2 = StringArray::from(vec![Some("B"), None, None]);
        let a3 = StringArray::from(vec![Some("C"), Some("D")]);

        let indices = vec![
            CompactMergeIndex { index: u8::MAX },
            CompactMergeIndex { index: 1 },
            CompactMergeIndex { index: 0 },
            CompactMergeIndex { index: u8::MAX },
            CompactMergeIndex { index: 2 },
            CompactMergeIndex { index: 2 },
            CompactMergeIndex { index: 1 },
            CompactMergeIndex { index: 1 },
        ];

        let arrays = vec![a1, a2, a3];
        let array_refs = arrays.iter().map(|a| a as &dyn Array).collect::<Vec<_>>();
        let merged = merge_n(&array_refs, &indices).unwrap();
        let merged = merged.as_string::<i32>();

        assert_eq!(merged.len(), indices.len());
        assert!(!merged.is_valid(0));
        assert!(merged.is_valid(1));
        assert_eq!(merged.value(1), "B");
        assert!(merged.is_valid(2));
        assert_eq!(merged.value(2), "A");
        assert!(!merged.is_valid(3));
        assert!(merged.is_valid(4));
        assert_eq!(merged.value(4), "C");
        assert!(merged.is_valid(5));
        assert_eq!(merged.value(5), "D");
        assert!(!merged.is_valid(6));
        assert!(!merged.is_valid(7));
    }
}