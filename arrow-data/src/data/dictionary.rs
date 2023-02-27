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
use crate::ArrayData;
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

/// Types of offset used by variable length byte arrays
pub trait DictionaryKey: private::DictionaryKeySealed + ArrowNativeType {
    const TYPE: DictionaryKeyType;
}

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

impl ArrayDataDictionary {
    /// Downcast this [`ArrayDataDictionary`] to the corresponding [`DictionaryArrayData`]
    pub fn downcast_ref<K: DictionaryKey>(&self) -> Option<&DictionaryArrayData<K>> {
        K::downcast_ref(self)
    }

    /// Downcast this [`ArrayDataDictionary`] to the corresponding [`DictionaryArrayData`]
    pub fn downcast<K: DictionaryKey>(self) -> Option<DictionaryArrayData<K>> {
        K::downcast(self)
    }
}

impl<K: DictionaryKey> From<DictionaryArrayData<K>> for ArrayDataDictionary {
    fn from(value: DictionaryArrayData<K>) -> Self {
        K::upcast(value)
    }
}

/// ArrayData for [dictionary arrays](https://arrow.apache.org/docs/format/Columnar.html#dictionary-encoded-layout)
pub struct DictionaryArrayData<K: DictionaryKey> {
    data_type: DataType,
    nulls: Option<NullBuffer>,
    keys: ScalarBuffer<K>,
    child: Box<ArrayData>,
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
            child: Box::new(child),
        }
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

    /// Returns the child data
    #[inline]
    pub fn child(&self) -> &ArrayData {
        self.child.as_ref()
    }

    /// Returns the data type of this array
    #[inline]
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }
}
