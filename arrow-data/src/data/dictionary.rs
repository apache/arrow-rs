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
use crate::{ArrayData, ArrayDataBuilder, Buffers};
use arrow_buffer::buffer::{NullBuffer, ScalarBuffer};
use arrow_buffer::ArrowNativeType;
use arrow_schema::DataType;

mod private {
    pub trait DictionaryKeySealed {}
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
        impl private::DictionaryKeySealed for $t {}
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

/// ArrayData for [dictionary arrays](https://arrow.apache.org/docs/format/Columnar.html#dictionary-encoded-layout)
#[derive(Debug, Clone)]
pub struct DictionaryArrayData<K: DictionaryKey> {
    data_type: DataType,
    nulls: Option<NullBuffer>,
    keys: ScalarBuffer<K>,
    values: ArrayData,
}

impl<K: DictionaryKey> DictionaryArrayData<K> {
    /// Create a new [`DictionaryArrayData`]
    ///
    /// # Safety
    ///
    /// - `PhysicalType::from(&data_type) == PhysicalType::Dictionary(K::TYPE)`
    /// - child must have a type matching `data_type`
    /// - all values in `keys` must be `0 < v < values.len()` or be a null according to `nulls`
    /// - `nulls` must have the same length as `values`
    pub unsafe fn new_unchecked(
        data_type: DataType,
        keys: ScalarBuffer<K>,
        nulls: Option<NullBuffer>,
        values: ArrayData,
    ) -> Self {
        Self {
            data_type,
            nulls,
            keys,
            values,
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
    pub fn keys(&self) -> &ScalarBuffer<K> {
        &self.keys
    }

    /// Returns the values data
    #[inline]
    pub fn values(&self) -> &ArrayData {
        &self.values
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
        (self.data_type, self.keys, self.nulls, self.values)
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
}

impl<K: DictionaryKey> From<ArrayData> for DictionaryArrayData<K> {
    fn from(data: ArrayData) -> Self {
        let keys = data.buffers.into_iter().next().unwrap();
        let keys = ScalarBuffer::new(keys, data.offset, data.len);
        let values = data.child_data.into_iter().next().unwrap();
        Self {
            keys,
            data_type: data.data_type,
            nulls: data.nulls,
            values,
        }
    }
}

impl<K: DictionaryKey> From<DictionaryArrayData<K>> for ArrayData {
    fn from(value: DictionaryArrayData<K>) -> Self {
        Self {
            data_type: value.data_type,
            len: value.keys.len(),
            offset: 0,
            buffers: vec![],
            child_data: vec![value.values],
            nulls: value.nulls,
        }
    }
}
