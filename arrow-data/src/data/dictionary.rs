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

use crate::data::types::DictionaryKeyType;
use crate::data::ArrayDataLayout;
use crate::{ArrayData, ArrayDataBuilder, Buffers};
use arrow_buffer::buffer::{NullBuffer, ScalarBuffer};
use arrow_buffer::ArrowNativeType;
use arrow_schema::DataType;

mod private {
    use super::*;

    pub trait DictionaryKeySealed {
        /// Downcast [`ArrayDataDictionary`] to `[DictionaryArrayData`]
        fn downcast_ref(data: &ArrayDataDictionary) -> Option<&DictionaryArrayData<Self>>
        where
            Self: DictionaryKey;

        /// Downcast [`ArrayDataDictionary`] to `[DictionaryArrayData`]
        fn downcast(data: ArrayDataDictionary) -> Option<DictionaryArrayData<Self>>
        where
            Self: DictionaryKey;

        /// Cast [`DictionaryArrayData`] to [`ArrayDataDictionary`]
        fn upcast(v: DictionaryArrayData<Self>) -> ArrayDataDictionary
        where
            Self: DictionaryKey;
    }
}

/// Types of dictionary key used by dictionary arrays
pub trait DictionaryKey: private::DictionaryKeySealed + ArrowNativeType {
    const TYPE: DictionaryKeyType;
}

macro_rules! dictionary {
    ($t:ty,$v:ident) => {
        impl DictionaryKey for $t {
            const TYPE: DictionaryKeyType = DictionaryKeyType::$v;
        }
        impl private::DictionaryKeySealed for $t {
            fn downcast_ref(
                data: &ArrayDataDictionary,
            ) -> Option<&DictionaryArrayData<Self>> {
                match data {
                    ArrayDataDictionary::$v(v) => Some(v),
                    _ => None,
                }
            }

            fn downcast(data: ArrayDataDictionary) -> Option<DictionaryArrayData<Self>> {
                match data {
                    ArrayDataDictionary::$v(v) => Some(v),
                    _ => None,
                }
            }

            fn upcast(v: DictionaryArrayData<Self>) -> ArrayDataDictionary {
                ArrayDataDictionary::$v(v)
            }
        }
    };
}

dictionary!(i8, Int8);
dictionary!(i16, Int16);
dictionary!(i32, Int32);
dictionary!(i64, Int64);
dictionary!(u8, UInt8);
dictionary!(u16, UInt16);
dictionary!(u32, UInt32);
dictionary!(u64, UInt64);

/// Applies op to each variant of [`ArrayDataDictionary`]
macro_rules! dictionary_op {
    ($array:ident, $op:block) => {
        match $array {
            ArrayDataDictionary::Int8($array) => $op
            ArrayDataDictionary::Int16($array) => $op
            ArrayDataDictionary::Int32($array) => $op
            ArrayDataDictionary::Int64($array) => $op
            ArrayDataDictionary::UInt8($array) => $op
            ArrayDataDictionary::UInt16($array) => $op
            ArrayDataDictionary::UInt32($array) => $op
            ArrayDataDictionary::UInt64($array) => $op
        }
    };
}

/// An enumeration of the types of [`DictionaryArrayData`]
#[derive(Debug, Clone)]
pub enum ArrayDataDictionary {
    Int8(DictionaryArrayData<i8>),
    Int16(DictionaryArrayData<i16>),
    Int32(DictionaryArrayData<i32>),
    Int64(DictionaryArrayData<i64>),
    UInt8(DictionaryArrayData<u8>),
    UInt16(DictionaryArrayData<u16>),
    UInt32(DictionaryArrayData<u32>),
    UInt64(DictionaryArrayData<u64>),
}

impl ArrayDataDictionary {
    /// Downcast this [`ArrayDataDictionary`] to the corresponding [`DictionaryArrayData`]
    pub fn downcast_ref<K: DictionaryKey>(&self) -> Option<&DictionaryArrayData<K>> {
        K::downcast_ref(self)
    }

    /// Downcast this [`ArrayDataDictionary`] to the corresponding [`DictionaryArrayData`]
    pub fn downcast<K: DictionaryKey>(self) -> Option<DictionaryArrayData<K>> {
        K::downcast(self)
    }

    /// Returns the values of this dictionary
    pub fn values(&self) -> &ArrayData {
        let s = self;
        dictionary_op!(s, { s.values() })
    }

    /// Returns a zero-copy slice of this array
    pub fn slice(&self, offset: usize, len: usize) -> Self {
        let s = self;
        dictionary_op!(s, { s.slice(offset, len).into() })
    }

    /// Returns an [`ArrayDataLayout`] representation of this
    pub(crate) fn layout(&self) -> ArrayDataLayout<'_> {
        let s = self;
        dictionary_op!(s, { s.layout() })
    }

    /// Creates a new [`ArrayDataDictionary`] from raw buffers
    ///
    /// # Safety
    ///
    /// See [`DictionaryArrayData::new_unchecked`]
    pub(crate) unsafe fn from_raw(
        builder: ArrayDataBuilder,
        key: DictionaryKeyType,
    ) -> Self {
        use DictionaryKeyType::*;
        match key {
            Int8 => Self::Int8(DictionaryArrayData::from_raw(builder)),
            Int16 => Self::Int16(DictionaryArrayData::from_raw(builder)),
            Int32 => Self::Int32(DictionaryArrayData::from_raw(builder)),
            Int64 => Self::Int64(DictionaryArrayData::from_raw(builder)),
            UInt8 => Self::UInt8(DictionaryArrayData::from_raw(builder)),
            UInt16 => Self::UInt16(DictionaryArrayData::from_raw(builder)),
            UInt32 => Self::UInt32(DictionaryArrayData::from_raw(builder)),
            UInt64 => Self::UInt64(DictionaryArrayData::from_raw(builder)),
        }
    }
}

impl<K: DictionaryKey> From<DictionaryArrayData<K>> for ArrayDataDictionary {
    fn from(value: DictionaryArrayData<K>) -> Self {
        K::upcast(value)
    }
}

/// ArrayData for [dictionary arrays](https://arrow.apache.org/docs/format/Columnar.html#dictionary-encoded-layout)
#[derive(Debug, Clone)]
pub struct DictionaryArrayData<K: DictionaryKey> {
    data_type: DataType,
    nulls: Option<NullBuffer>,
    keys: ScalarBuffer<K>,
    values: Box<ArrayData>,
}

impl<K: DictionaryKey> DictionaryArrayData<K> {
    /// Create a new [`DictionaryArrayData`]
    ///
    /// # Safety
    ///
    /// - `data_type` must be valid for this layout
    /// - child must have a type matching `data_type`
    /// - all values in `keys` must be `0 < v < child.len()` or be a null according to `nulls`
    /// - `nulls` must have the same length as `child`
    pub unsafe fn new_unchecked(
        data_type: DataType,
        keys: ScalarBuffer<K>,
        nulls: Option<NullBuffer>,
        child: ArrayData,
    ) -> Self {
        Self {
            data_type,
            nulls,
            keys,
            values: Box::new(child),
        }
    }

    /// Creates a new [`DictionaryArrayData`] from raw buffers
    ///
    /// # Safety
    ///
    /// See [`Self::new_unchecked`]
    pub(crate) unsafe fn from_raw(builder: ArrayDataBuilder) -> Self {
        let keys = builder.buffers.into_iter().next().unwrap();
        let keys = ScalarBuffer::new(keys, builder.offset, builder.len);
        let values = builder.child_data.into_iter().next().unwrap();
        Self {
            keys,
            data_type: builder.data_type,
            nulls: builder.nulls,
            values: Box::new(values),
        }
    }

    /// Returns the length
    #[inline]
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Returns true if this array is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Returns the null buffer if any
    #[inline]
    pub fn nulls(&self) -> Option<&NullBuffer> {
        self.nulls.as_ref()
    }

    /// Returns the keys
    #[inline]
    pub fn keys(&self) -> &[K] {
        &self.keys
    }

    /// Returns the values data
    #[inline]
    pub fn values(&self) -> &ArrayData {
        self.values.as_ref()
    }

    /// Returns the data type of this array
    #[inline]
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    /// Returns the underlying parts of this [`DictionaryArrayData`]
    pub fn into_parts(
        self,
    ) -> (DataType, ScalarBuffer<K>, Option<NullBuffer>, ArrayData) {
        (self.data_type, self.keys, self.nulls, *self.values)
    }

    /// Returns a zero-copy slice of this array
    pub fn slice(&self, offset: usize, len: usize) -> Self {
        Self {
            keys: self.keys.slice(offset, len),
            data_type: self.data_type.clone(),
            nulls: self.nulls.as_ref().map(|x| x.slice(offset, len)),
            values: self.values.clone(),
        }
    }

    /// Returns an [`ArrayDataLayout`] representation of this
    pub(crate) fn layout(&self) -> ArrayDataLayout<'_> {
        ArrayDataLayout {
            data_type: &self.data_type,
            len: self.keys.len(),
            offset: 0,
            nulls: self.nulls.as_ref(),
            buffers: Buffers::one(self.keys.inner()),
            child_data: std::slice::from_ref(self.values.as_ref()),
        }
    }
}
