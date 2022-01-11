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

//! Defines helper functions for force Array type downcast

use crate::array::*;
use crate::datatypes::*;

/// Force downcast ArrayRef to PrimitiveArray<T>
pub fn as_primitive_array<T>(arr: &dyn Array) -> &PrimitiveArray<T>
where
    T: ArrowPrimitiveType,
{
    arr.as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .expect("Unable to downcast to primitive array")
}

/// Force downcast ArrayRef to DictionaryArray<T>
pub fn as_dictionary_array<T>(arr: &dyn Array) -> &DictionaryArray<T>
where
    T: ArrowDictionaryKeyType,
{
    arr.as_any()
        .downcast_ref::<DictionaryArray<T>>()
        .expect("Unable to downcast to dictionary array")
}

#[doc = "Force downcast ArrayRef to GenericListArray"]
pub fn as_generic_list_array<S: OffsetSizeTrait>(
    arr: &dyn Array,
) -> &GenericListArray<S> {
    arr.as_any()
        .downcast_ref::<GenericListArray<S>>()
        .expect("Unable to downcast to list array")
}

#[doc = "Force downcast ArrayRef to ListArray"]
#[inline]
pub fn as_list_array(arr: &dyn Array) -> &ListArray {
    as_generic_list_array::<i32>(arr)
}

#[doc = "Force downcast ArrayRef to LargeListArray"]
#[inline]
pub fn as_large_list_array(arr: &dyn Array) -> &LargeListArray {
    as_generic_list_array::<i64>(arr)
}

#[doc = "Force downcast ArrayRef to GenericBinaryArray"]
#[inline]
pub fn as_generic_binary_array<S: BinaryOffsetSizeTrait>(
    arr: &dyn Array,
) -> &GenericBinaryArray<S> {
    arr.as_any()
        .downcast_ref::<GenericBinaryArray<S>>()
        .expect("Unable to downcast to binary array")
}

macro_rules! array_downcast_fn {
    ($name: ident, $arrty: ty, $arrty_str:expr) => {
        #[doc = "Force downcast ArrayRef to "]
        #[doc = $arrty_str]
        pub fn $name(arr: &dyn Array) -> &$arrty {
            arr.as_any().downcast_ref::<$arrty>().expect(concat!(
                "Unable to downcast to typed array through ",
                stringify!($name)
            ))
        }
    };

    // use recursive macro to generate dynamic doc string for a given array type
    ($name: ident, $arrty: ty) => {
        array_downcast_fn!($name, $arrty, stringify!($arrty));
    };
}

array_downcast_fn!(as_string_array, StringArray);
array_downcast_fn!(as_largestring_array, LargeStringArray);
array_downcast_fn!(as_boolean_array, BooleanArray);
array_downcast_fn!(as_null_array, NullArray);
array_downcast_fn!(as_struct_array, StructArray);
array_downcast_fn!(as_union_array, UnionArray);
array_downcast_fn!(as_map_array, MapArray);

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn test_as_primitive_array_ref() {
        let array: Int32Array = vec![1, 2, 3].into_iter().map(Some).collect();
        assert!(!as_primitive_array::<Int32Type>(&array).is_empty());

        // should also work when wrapped in an Arc
        let array: ArrayRef = Arc::new(array);
        assert!(!as_primitive_array::<Int32Type>(&array).is_empty());
    }

    #[test]
    fn test_as_string_array_ref() {
        let array: StringArray = vec!["foo", "bar"].into_iter().map(Some).collect();
        assert!(!as_string_array(&array).is_empty());

        // should also work when wrapped in an Arc
        let array: ArrayRef = Arc::new(array);
        assert!(!as_string_array(&array).is_empty())
    }
}
