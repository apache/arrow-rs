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

//! Contains functions and function factories to compare arrays.

use std::cmp::Ordering;

use crate::array::*;
use crate::datatypes::*;
use crate::error::{ArrowError, Result};

use arrow_array::{downcast_integer, downcast_primitive};

macro_rules! primitive_helper {
    ($t:ty, $left:ident, $right:ident) => {{
        let left = PrimitiveArray::<$t>::from($left.data().clone());
        let right = PrimitiveArray::<$t>::from($right.data().clone());
        Box::new(move |i, j| left.value(i).compare(right.value(j)))
    }};
}

macro_rules! array_helper {
    ($t:ty, $left:ident, $right:ident) => {{
        let left = <$t>::from($left.data().clone());
        let right = <$t>::from($right.data().clone());
        Box::new(move |i, j| left.value(i).cmp(&right.value(j)))
    }};
}

macro_rules! dictionary_helper {
    ($t:ty, $build_compare:ident, $left:ident, $right:ident) => {{
        let left = DictionaryArray::<$t>::from($left.data().clone());
        let right = DictionaryArray::<$t>::from($right.data().clone());
        let compare = $build_compare(left.values().as_ref(), right.values().as_ref())?;

        Box::new(move |i, j| {
            let left_v = left.keys().value(i);
            let right_v = right.keys().value(j);
            compare(left_v as _, right_v as _)
        })
    }};
}

/// Compare the values at two arbitrary indices in two arrays.
pub type DynComparator = Box<dyn Fn(usize, usize) -> Ordering + Send + Sync>;

/// returns a comparison function that compares two values at two different positions
/// between the two arrays.
/// The arrays' types must be equal.
/// # Example
/// ```
/// use arrow::array::{build_compare, Int32Array};
///
/// # fn main() -> arrow::error::Result<()> {
/// let array1 = Int32Array::from(vec![1, 2]);
/// let array2 = Int32Array::from(vec![3, 4]);
///
/// let cmp = build_compare(&array1, &array2)?;
///
/// // 1 (index 0 of array1) is smaller than 4 (index 1 of array2)
/// assert_eq!(std::cmp::Ordering::Less, (cmp)(0, 1));
/// # Ok(())
/// # }
/// ```
// This is a factory of comparisons.
// The lifetime 'a enforces that we cannot use the closure beyond any of the array's lifetime.
pub fn build_compare(left: &dyn Array, right: &dyn Array) -> Result<DynComparator> {
    let left_d = left.data_type();
    let right_d = right.data_type();

    if left_d != right_d {
        return Err(ArrowError::InvalidArgumentError(format!(
            "Cannot compare arrays with different types ({}, {})",
            left_d, right_d
        )));
    }

    Ok(downcast_primitive! {
        left_d => (primitive_helper, left, right),
        DataType::Boolean => array_helper!(BooleanArray, left, right),
        DataType::Utf8 => array_helper!(StringArray, left, right),
        DataType::LargeUtf8 => array_helper!(LargeStringArray, left, right),
        DataType::Binary => array_helper!(BinaryArray, left, right),
        DataType::LargeBinary => array_helper!(LargeBinaryArray, left, right),
        DataType::FixedSizeBinary(_) => array_helper!(FixedSizeBinaryArray, left, right),
        DataType::Dictionary(k, _) => downcast_integer! {
            k.as_ref() => (dictionary_helper, build_compare, left, right),
            _ => unreachable!()
        },
        lhs => {
            return Err(ArrowError::InvalidArgumentError(format!(
                "The data type type {:?} has no natural order",
                lhs
            )))
        }
    })
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::array::{FixedSizeBinaryArray, Float64Array, Int32Array};
    use crate::error::Result;
    use std::cmp::Ordering;

    #[test]
    fn test_fixed_size_binary() -> Result<()> {
        let items = vec![vec![1u8], vec![2u8]];
        let array = FixedSizeBinaryArray::try_from_iter(items.into_iter()).unwrap();

        let cmp = build_compare(&array, &array)?;

        assert_eq!(Ordering::Less, (cmp)(0, 1));
        Ok(())
    }

    #[test]
    fn test_fixed_size_binary_fixed_size_binary() -> Result<()> {
        let items = vec![vec![1u8]];
        let array1 = FixedSizeBinaryArray::try_from_iter(items.into_iter()).unwrap();
        let items = vec![vec![2u8]];
        let array2 = FixedSizeBinaryArray::try_from_iter(items.into_iter()).unwrap();

        let cmp = build_compare(&array1, &array2)?;

        assert_eq!(Ordering::Less, (cmp)(0, 0));
        Ok(())
    }

    #[test]
    fn test_i32() -> Result<()> {
        let array = Int32Array::from(vec![1, 2]);

        let cmp = build_compare(&array, &array)?;

        assert_eq!(Ordering::Less, (cmp)(0, 1));
        Ok(())
    }

    #[test]
    fn test_i32_i32() -> Result<()> {
        let array1 = Int32Array::from(vec![1]);
        let array2 = Int32Array::from(vec![2]);

        let cmp = build_compare(&array1, &array2)?;

        assert_eq!(Ordering::Less, (cmp)(0, 0));
        Ok(())
    }

    #[test]
    fn test_f64() -> Result<()> {
        let array = Float64Array::from(vec![1.0, 2.0]);

        let cmp = build_compare(&array, &array)?;

        assert_eq!(Ordering::Less, (cmp)(0, 1));
        Ok(())
    }

    #[test]
    fn test_f64_nan() -> Result<()> {
        let array = Float64Array::from(vec![1.0, f64::NAN]);

        let cmp = build_compare(&array, &array)?;

        assert_eq!(Ordering::Less, (cmp)(0, 1));
        Ok(())
    }

    #[test]
    fn test_f64_zeros() -> Result<()> {
        let array = Float64Array::from(vec![-0.0, 0.0]);

        let cmp = build_compare(&array, &array)?;

        assert_eq!((-0.0_f32).total_cmp(&0.0), (cmp)(0, 1));
        assert_eq!(0.0_f32.total_cmp(&-0.0), (cmp)(1, 0));
        Ok(())
    }

    #[test]
    fn test_decimal() -> Result<()> {
        let array = vec![Some(5_i128), Some(2_i128), Some(3_i128)]
            .into_iter()
            .collect::<Decimal128Array>()
            .with_precision_and_scale(23, 6)
            .unwrap();

        let cmp = build_compare(&array, &array)?;
        assert_eq!(Ordering::Less, (cmp)(1, 0));
        assert_eq!(Ordering::Greater, (cmp)(0, 2));
        Ok(())
    }

    #[test]
    fn test_dict() -> Result<()> {
        let data = vec!["a", "b", "c", "a", "a", "c", "c"];
        let array = data.into_iter().collect::<DictionaryArray<Int16Type>>();

        let cmp = build_compare(&array, &array)?;

        assert_eq!(Ordering::Less, (cmp)(0, 1));
        assert_eq!(Ordering::Equal, (cmp)(3, 4));
        assert_eq!(Ordering::Greater, (cmp)(2, 3));
        Ok(())
    }

    #[test]
    fn test_multiple_dict() -> Result<()> {
        let d1 = vec!["a", "b", "c", "d"];
        let a1 = d1.into_iter().collect::<DictionaryArray<Int16Type>>();
        let d2 = vec!["e", "f", "g", "a"];
        let a2 = d2.into_iter().collect::<DictionaryArray<Int16Type>>();

        let cmp = build_compare(&a1, &a2)?;

        assert_eq!(Ordering::Less, (cmp)(0, 0));
        assert_eq!(Ordering::Equal, (cmp)(0, 3));
        assert_eq!(Ordering::Greater, (cmp)(1, 3));
        Ok(())
    }

    #[test]
    fn test_primitive_dict() -> Result<()> {
        let values = Int32Array::from(vec![1_i32, 0, 2, 5]);
        let keys = Int8Array::from_iter_values([0, 0, 1, 3]);
        let array1 = DictionaryArray::<Int8Type>::try_new(&keys, &values).unwrap();

        let values = Int32Array::from(vec![2_i32, 3, 4, 5]);
        let keys = Int8Array::from_iter_values([0, 1, 1, 3]);
        let array2 = DictionaryArray::<Int8Type>::try_new(&keys, &values).unwrap();

        let cmp = build_compare(&array1, &array2)?;

        assert_eq!(Ordering::Less, (cmp)(0, 0));
        assert_eq!(Ordering::Less, (cmp)(0, 3));
        assert_eq!(Ordering::Equal, (cmp)(3, 3));
        assert_eq!(Ordering::Greater, (cmp)(3, 1));
        assert_eq!(Ordering::Greater, (cmp)(3, 2));
        Ok(())
    }
}
