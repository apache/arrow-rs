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

//! Contains functionality to load an ArrayData from the C Data Interface

use std::convert::TryFrom;

use crate::{
    error::{ArrowError, Result},
    ffi,
    ffi::ArrowArrayRef,
};

use super::ArrayData;

impl TryFrom<ffi::ArrowArray> for ArrayData {
    type Error = ArrowError;

    fn try_from(value: ffi::ArrowArray) -> Result<Self> {
        value.to_data()
    }
}

impl TryFrom<ArrayData> for ffi::ArrowArray {
    type Error = ArrowError;

    fn try_from(value: ArrayData) -> Result<Self> {
        unsafe { ffi::ArrowArray::try_new(value) }
    }
}

#[cfg(test)]
mod tests {
    use crate::array::{DictionaryArray, Int32Array, StringArray};
    use crate::error::Result;
    use crate::{
        array::{
            Array, ArrayData, BooleanArray, Int64Array, StructArray, UInt32Array,
            UInt64Array,
        },
        datatypes::{DataType, Field},
        ffi::ArrowArray,
    };
    use std::convert::TryFrom;
    use std::sync::Arc;

    fn test_round_trip(expected: &ArrayData) -> Result<()> {
        // create a `ArrowArray` from the data.
        let d1 = ArrowArray::try_from(expected.clone())?;

        // here we export the array as 2 pointers. We would have no control over ownership if it was not for
        // the release mechanism.
        let (array, schema) = ArrowArray::into_raw(d1);

        // simulate an external consumer by being the consumer
        let d1 = unsafe { ArrowArray::try_from_raw(array, schema) }?;

        let result = &ArrayData::try_from(d1)?;

        assert_eq!(result, expected);
        Ok(())
    }

    #[test]
    fn test_u32() -> Result<()> {
        let array = UInt32Array::from(vec![Some(2), None, Some(1), None]);
        let data = array.data();
        test_round_trip(data)
    }

    #[test]
    fn test_u64() -> Result<()> {
        let array = UInt64Array::from(vec![Some(2), None, Some(1), None]);
        let data = array.data();
        test_round_trip(data)
    }

    #[test]
    fn test_i64() -> Result<()> {
        let array = Int64Array::from(vec![Some(2), None, Some(1), None]);
        let data = array.data();
        test_round_trip(data)
    }

    #[test]
    fn test_struct() -> Result<()> {
        let inner = StructArray::from(vec![
            (
                Field::new("a1", DataType::Boolean, false),
                Arc::new(BooleanArray::from(vec![true, true, false, false]))
                    as Arc<dyn Array>,
            ),
            (
                Field::new("a2", DataType::UInt32, false),
                Arc::new(UInt32Array::from(vec![1, 2, 3, 4])),
            ),
        ]);

        let array = StructArray::from(vec![
            (
                Field::new("a", inner.data_type().clone(), false),
                Arc::new(inner) as Arc<dyn Array>,
            ),
            (
                Field::new("b", DataType::Boolean, false),
                Arc::new(BooleanArray::from(vec![false, false, true, true]))
                    as Arc<dyn Array>,
            ),
            (
                Field::new("c", DataType::UInt32, false),
                Arc::new(UInt32Array::from(vec![42, 28, 19, 31])),
            ),
        ]);
        let data = array.data();
        test_round_trip(data)
    }

    #[test]
    fn test_dictionary() -> Result<()> {
        let values = StringArray::from(vec![Some("foo"), Some("bar"), None]);
        let keys = Int32Array::from(vec![
            Some(0),
            Some(1),
            None,
            Some(1),
            Some(1),
            None,
            Some(1),
            Some(2),
            Some(1),
            None,
        ]);
        let array = DictionaryArray::try_new(&keys, &values)?;

        let data = array.data();
        test_round_trip(data)
    }
}
