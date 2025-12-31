use arrow_buffer::{NullBuffer, OffsetBuffer};
use crate::{Array, ArrayRef, ArrowPrimitiveType, DictionaryArray, GenericByteArray, GenericListArray, OffsetSizeTrait, PrimitiveArray};
use crate::types::{ArrowDictionaryKeyType, ByteArrayType};

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

impl<K: ArrowDictionaryKeyType> SliceableArray for DictionaryArray<K> {
    fn slice(&self, offset: usize, length: usize) -> Self {
        DictionaryArray::slice(self, offset, length)
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
}

impl<OffsetSize: OffsetSizeTrait, ValueArray: SliceableArray + Clone + 'static> GenericListTypedIter<OffsetSize, ValueArray> {
    pub fn new(list: GenericListArray<OffsetSize>) -> Option<Self> {
        let nulls = list.nulls().cloned();
        let values = list.values().as_any().downcast_ref::<ValueArray>()?.clone();
        let value_offsets = list.offsets().clone();
        Some(Self {
            nulls,
            values,
            value_offsets
        })
    }
}

impl<OffsetSize: OffsetSizeTrait, ValueArray: SliceableArray> Iterator for GenericListTypedIter<OffsetSize, ValueArray> {
    type Item = ValueArray;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}