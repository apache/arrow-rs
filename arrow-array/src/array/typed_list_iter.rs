use arrow_buffer::{NullBuffer, OffsetBuffer};
use crate::{Array, ArrowPrimitiveType, DictionaryArray, GenericByteArray, GenericByteViewArray, GenericListArray, GenericListViewArray, OffsetSizeTrait, PrimitiveArray};
use crate::types::{ArrowDictionaryKeyType, ByteArrayType, ByteViewType};

/// Arrays that can be sliced in a zero copy, zero allocation way.
pub trait SliceableArray {
    fn slice(&self, offset: usize, length: usize) -> Self;
}

impl<T: ArrowPrimitiveType> SliceableArray for PrimitiveArray<T> {
    fn slice(&self, offset: usize, length: usize) -> Self {
        PrimitiveArray::slice(self, offset, length)
    }
}
impl<T: ByteArrayType> SliceableArray for GenericByteArray<T> {
    fn slice(&self, offset: usize, length: usize) -> Self {
        GenericByteArray::slice(self, offset, length)
    }
}

impl<T: ByteViewType + ?Sized> SliceableArray for GenericByteViewArray<T> {
    fn slice(&self, offset: usize, length: usize) -> Self {
        GenericByteViewArray::slice(self, offset, length)
    }
}

impl<K: ArrowDictionaryKeyType> SliceableArray for DictionaryArray<K> {
    fn slice(&self, offset: usize, length: usize) -> Self {
        DictionaryArray::slice(self, offset, length)
    }
}


impl<T: OffsetSizeTrait> SliceableArray for GenericListArray<T> {
    fn slice(&self, offset: usize, length: usize) -> Self {
        GenericListArray::slice(self, offset, length)
    }
}


impl<T: OffsetSizeTrait> SliceableArray for GenericListViewArray<T> {
    fn slice(&self, offset: usize, length: usize) -> Self {
        GenericListViewArray::slice(self, offset, length)
    }
}

/// A typed iterator on a GenericListArray.
///
/// Downcasting at iterator creation time allows to avoid allocations during the iteration, since
/// we can now directly return the target type, instead of having to create
/// Arc<Array> that require allocations. This version should be both more ergonomic and more
/// efficient than the standard ArrayIter for GenericListArrays for supported types.
pub struct GenericListTypedIter<OffsetSize: OffsetSizeTrait, ValueArray: SliceableArray> {
    nulls: Option<NullBuffer>,
    values: ValueArray,
    value_offsets: OffsetBuffer<OffsetSize>,
    current: usize,
}

impl<OffsetSize: OffsetSizeTrait, ValueArray: SliceableArray + Clone + 'static> GenericListTypedIter<OffsetSize, ValueArray> {
    pub fn new(list: GenericListArray<OffsetSize>) -> Option<Self> {
        let nulls = list.nulls().cloned();
        let values = list.values().as_any().downcast_ref::<ValueArray>()?.clone();
        let value_offsets = list.offsets().clone();
        Some(Self {
            nulls,
            values,
            value_offsets,
            current: 0,
        })
    }
}

impl<OffsetSize: OffsetSizeTrait, ValueArray: SliceableArray> Iterator for GenericListTypedIter<OffsetSize, ValueArray> {
    type Item = Option<ValueArray>;

    fn next(&mut self) -> Option<Self::Item> {
        // Check if we've reached the end
        if self.current >= self.value_offsets.len() - 1 {
            return None;
        }

        // Check if current row is null
        let is_null = self.nulls.as_ref().map_or(false, |n| n.is_null(self.current));

        let result = if is_null {
            Some(None)
        } else {
            // Get start and end offsets for this list element
            let start = self.value_offsets[self.current].as_usize();
            let end = self.value_offsets[self.current + 1].as_usize();

            // Slice the values array - this is zero-copy
            Some(Some(self.values.slice(start, end - start)))
        };

        self.current += 1;
        result
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.value_offsets.len() - 1 - self.current;
        (remaining, Some(remaining))
    }
}

impl<OffsetSize: OffsetSizeTrait, ValueArray: SliceableArray> ExactSizeIterator for GenericListTypedIter<OffsetSize, ValueArray> {
    fn len(&self) -> usize {
        self.value_offsets.len() - 1 - self.current
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Int32Array, Int64Array, ListArray, StringArray, types::{Int32Type, Int64Type}};

    #[test]
    fn test_primitive_array_no_nulls() {
        let list_array = ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
            Some(vec![Some(1), Some(2), Some(3)]),
            Some(vec![Some(4)]),
            Some(vec![]),
        ]);

        let typed_iter: Option<GenericListTypedIter<i32, Int64Array>> = list_array.typed_iter();
        let mut iter = typed_iter.unwrap();

        // First element
        let arr = iter.next().unwrap().unwrap();
        assert_eq!(arr.len(), 3);
        assert_eq!(arr.value(0), 1);
        assert_eq!(arr.value(1), 2);
        assert_eq!(arr.value(2), 3);

        // Second element
        let arr = iter.next().unwrap().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr.value(0), 4);

        // Third element (empty list)
        let arr = iter.next().unwrap().unwrap();
        assert_eq!(arr.len(), 0);

        // No more elements
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_primitive_array_with_nulls() {
        let list_array = ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
            Some(vec![Some(1), Some(2)]),
            None,
            Some(vec![Some(3)]),
            None,
        ]);

        let typed_iter: Option<GenericListTypedIter<i32, Int64Array>> = list_array.typed_iter();
        let mut iter = typed_iter.unwrap();

        // First element
        assert!(iter.next().unwrap().is_some());

        // Null element
        assert!(iter.next().unwrap().is_none());

        // Third element
        assert!(iter.next().unwrap().is_some());

        // Another null element
        assert!(iter.next().unwrap().is_none());

        // No more elements
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_string_array() {
        let list_array = ListArray::new(
            arrow_schema::Field::new("item", arrow_schema::DataType::Utf8, true).into(),
            arrow_buffer::OffsetBuffer::from_lengths([2, 1, 3]),
            std::sync::Arc::new(StringArray::from(vec![
                Some("a"), Some("b"), // First list
                Some("c"),             // Second list
                Some("d"), Some("e"), Some("f"), // Third list
            ])),
            None,
        );

        let typed_iter: Option<GenericListTypedIter<i32, StringArray>> = list_array.typed_iter();
        let mut iter = typed_iter.unwrap();

        // First element
        let arr = iter.next().unwrap().unwrap();
        assert_eq!(arr.len(), 2);
        assert_eq!(arr.value(0), "a");
        assert_eq!(arr.value(1), "b");

        // Second element
        let arr = iter.next().unwrap().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr.value(0), "c");

        // Third element
        let arr = iter.next().unwrap().unwrap();
        assert_eq!(arr.len(), 3);
        assert_eq!(arr.value(0), "d");
        assert_eq!(arr.value(1), "e");
        assert_eq!(arr.value(2), "f");

        // No more elements
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_wrong_type_returns_none() {
        let list_array = ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
            Some(vec![Some(1), Some(2)]),
        ]);

        // Try to create iterator with wrong type - should return None
        let typed_iter: Option<GenericListTypedIter<i32, Int32Array>> = list_array.typed_iter();
        assert!(typed_iter.is_none());
    }

    #[test]
    fn test_iterator_size_hint() {
        let list_array = ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
            Some(vec![Some(1)]),
            Some(vec![Some(2)]),
            Some(vec![Some(3)]),
        ]);

        let typed_iter: Option<GenericListTypedIter<i32, Int64Array>> = list_array.typed_iter();
        let iter = typed_iter.unwrap();

        assert_eq!(iter.size_hint(), (3, Some(3)));
        assert_eq!(iter.len(), 3);
    }

    #[test]
    fn test_iterator_with_enumerate() {
        let list_array = ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
            Some(vec![Some(10)]),
            None,
            Some(vec![Some(20), Some(30)]),
        ]);

        let typed_iter: Option<GenericListTypedIter<i32, Int64Array>> = list_array.typed_iter();
        let iter = typed_iter.unwrap();

        for (idx, arr) in iter.enumerate() {
            match idx {
                0 => {
                    let a = arr.unwrap();
                    assert_eq!(a.len(), 1);
                    assert_eq!(a.value(0), 10);
                }
                1 => assert!(arr.is_none()),
                2 => {
                    let a = arr.unwrap();
                    assert_eq!(a.len(), 2);
                    assert_eq!(a.value(0), 20);
                    assert_eq!(a.value(1), 30);
                }
                _ => panic!("Unexpected index"),
            }
        }
    }

    #[test]
    fn test_iterator_with_zip() {
        let list1 = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(2)]),
            Some(vec![Some(3)]),
        ]);
        let list2 = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(10)]),
            Some(vec![Some(20), Some(30)]),
        ]);

        let iter1: GenericListTypedIter<i32, Int32Array> = list1.typed_iter().unwrap();
        let iter2: GenericListTypedIter<i32, Int32Array> = list2.typed_iter().unwrap();

        for (arr1, arr2) in iter1.zip(iter2) {
            let a1 = arr1.unwrap();
            let a2 = arr2.unwrap();
            // Just verify they're not empty
            assert!(a1.len() > 0);
            assert!(a2.len() > 0);
        }
    }
}