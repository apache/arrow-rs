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

//! Defines helper functions for downcasting [`dyn Array`](Array) to concrete types

use crate::array::*;
use crate::types::*;

/// Repeats the provided pattern based on the number of comma separated identifiers
#[doc(hidden)]
#[macro_export]
macro_rules! repeat_pat {
    ($e:pat, $v_:ident) => {
        $e
    };
    ($e:pat, $v_:ident $(, $tail:ident)+) => {
        ($e, $crate::repeat_pat!($e $(, $tail)+))
    }
}

/// Downcast an [`Array`] to a [`PrimitiveArray`] based on its [`DataType`]
/// accepts a number of subsequent patterns to match the data type
///
/// ```
/// # use arrow_array::{Array, downcast_primitive_array, cast::as_string_array};
/// # use arrow_schema::DataType;
///
/// fn print_primitive(array: &dyn Array) {
///     downcast_primitive_array!(
///         array => {
///             for v in array {
///                 println!("{:?}", v);
///             }
///         }
///         DataType::Utf8 => {
///             for v in as_string_array(array) {
///                 println!("{:?}", v);
///             }
///         }
///         t => println!("Unsupported datatype {}", t)
///     )
/// }
/// ```
///
/// [`DataType`]: arrow_schema::DataType
#[macro_export]
macro_rules! downcast_primitive_array {
    ($values:ident => $e:expr, $($p:pat => $fallback:expr $(,)*)*) => {
        downcast_primitive_array!($values => {$e} $($p => $fallback)*)
    };
    (($($values:ident),+) => $e:block $($($p:pat),+ => $fallback:expr $(,)*)*) => {
        $crate::downcast_primitive_array!($($values),+ => $e $($($p),+ => $fallback)*)
    };
    (($($values:ident),+) => $e:block $(($($p:pat),+) => $fallback:expr $(,)*)*) => {
        $crate::downcast_primitive_array!($($values),+ => $e $($($p),+ => $fallback)*)
    };
    ($($values:ident),+ => $e:block $($($p:pat),+ => $fallback:expr $(,)*)*) => {
        match ($($values.data_type()),+) {
            $crate::repeat_pat!(arrow_schema::DataType::Int8, $($values),+) => {
                $(let $values = $crate::cast::as_primitive_array::<
                    $crate::types::Int8Type,
                >($values);)+
                $e
            }
            $crate::repeat_pat!(arrow_schema::DataType::Int16, $($values),+) => {
                $(let $values = $crate::cast::as_primitive_array::<
                    $crate::types::Int16Type,
                >($values);)+
                $e
            }
            $crate::repeat_pat!(arrow_schema::DataType::Int32, $($values),+) => {
                $(let $values = $crate::cast::as_primitive_array::<
                    $crate::types::Int32Type,
                >($values);)+
                $e
            }
            $crate::repeat_pat!(arrow_schema::DataType::Int64, $($values),+) => {
                $(let $values = $crate::cast::as_primitive_array::<
                    $crate::types::Int64Type,
                >($values);)+
                $e
            }
            $crate::repeat_pat!(arrow_schema::DataType::UInt8, $($values),+) => {
                $(let $values = $crate::cast::as_primitive_array::<
                    $crate::types::UInt8Type,
                >($values);)+
                $e
            }
            $crate::repeat_pat!(arrow_schema::DataType::UInt16, $($values),+) => {
                $(let $values = $crate::cast::as_primitive_array::<
                    $crate::types::UInt16Type,
                >($values);)+
                $e
            }
            $crate::repeat_pat!(arrow_schema::DataType::UInt32, $($values),+) => {
                $(let $values = $crate::cast::as_primitive_array::<
                    $crate::types::UInt32Type,
                >($values);)+
                $e
            }
            $crate::repeat_pat!(arrow_schema::DataType::UInt64, $($values),+) => {
                $(let $values = $crate::cast::as_primitive_array::<
                    $crate::types::UInt64Type,
                >($values);)+
                $e
            }
            $crate::repeat_pat!(arrow_schema::DataType::Float16, $($values),+) => {
                $(let $values = $crate::cast::as_primitive_array::<
                    $crate::types::Float16Type,
                >($values);)+
                $e
            }
            $crate::repeat_pat!(arrow_schema::DataType::Float32, $($values),+) => {
                $(let $values = $crate::cast::as_primitive_array::<
                    $crate::types::Float32Type,
                >($values);)+
                $e
            }
            $crate::repeat_pat!(arrow_schema::DataType::Float64, $($values),+) => {
                $(let $values = $crate::cast::as_primitive_array::<
                    $crate::types::Float64Type,
                >($values);)+
                $e
            }
            $crate::repeat_pat!(arrow_schema::DataType::Date32, $($values),+) => {
                $(let $values = $crate::cast::as_primitive_array::<
                    $crate::types::Date32Type,
                >($values);)+
                $e
            }
            $crate::repeat_pat!(arrow_schema::DataType::Date64, $($values),+) => {
                $(let $values = $crate::cast::as_primitive_array::<
                    $crate::types::Date64Type,
                >($values);)+
                $e
            }
            $crate::repeat_pat!(arrow_schema::DataType::Time32(arrow_schema::TimeUnit::Second), $($values),+) => {
                $(let $values = $crate::cast::as_primitive_array::<
                    $crate::types::Time32SecondType,
                >($values);)+
                $e
            }
            $crate::repeat_pat!(arrow_schema::DataType::Time32(arrow_schema::TimeUnit::Millisecond), $($values),+) => {
                $(let $values = $crate::cast::as_primitive_array::<
                    $crate::types::Time32MillisecondType,
                >($values);)+
                $e
            }
            $crate::repeat_pat!(arrow_schema::DataType::Time64(arrow_schema::TimeUnit::Microsecond), $($values),+) => {
                $(let $values = $crate::cast::as_primitive_array::<
                    $crate::types::Time64MicrosecondType,
                >($values);)+
                $e
            }
            $crate::repeat_pat!(arrow_schema::DataType::Time64(arrow_schema::TimeUnit::Nanosecond), $($values),+) => {
                $(let $values = $crate::cast::as_primitive_array::<
                    $crate::types::Time64NanosecondType,
                >($values);)+
                $e
            }
            $crate::repeat_pat!(arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Second, _), $($values),+) => {
                $(let $values = $crate::cast::as_primitive_array::<
                    $crate::types::TimestampSecondType,
                >($values);)+
                $e
            }
            $crate::repeat_pat!(arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, _), $($values),+) => {
                $(let $values = $crate::cast::as_primitive_array::<
                    $crate::types::TimestampMillisecondType,
                >($values);)+
                $e
            }
            $crate::repeat_pat!(arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, _), $($values),+) => {
                $(let $values = $crate::cast::as_primitive_array::<
                    $crate::types::TimestampMicrosecondType,
                >($values);)+
                $e
            }
            $crate::repeat_pat!(arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, _), $($values),+) => {
                $(let $values = $crate::cast::as_primitive_array::<
                    $crate::types::TimestampNanosecondType,
                >($values);)+
                $e
            }
            $crate::repeat_pat!(arrow_schema::DataType::Interval(arrow_schema::IntervalUnit::YearMonth), $($values),+) => {
                $(let $values = $crate::cast::as_primitive_array::<
                    $crate::types::IntervalYearMonthType,
                >($values);)+
                $e
            }
            $crate::repeat_pat!(arrow_schema::DataType::Interval(arrow_schema::IntervalUnit::DayTime), $($values),+) => {
                $(let $values = $crate::cast::as_primitive_array::<
                    $crate::types::IntervalDayTimeType,
                >($values);)+
                $e
            }
            $crate::repeat_pat!(arrow_schema::DataType::Interval(arrow_schema::IntervalUnit::MonthDayNano), $($values),+) => {
                $(let $values = $crate::cast::as_primitive_array::<
                    $crate::types::IntervalMonthDayNanoType,
                >($values);)+
                $e
            }
            $crate::repeat_pat!(arrow_schema::DataType::Duration(arrow_schema::TimeUnit::Second), $($values),+) => {
                $(let $values = $crate::cast::as_primitive_array::<
                    $crate::types::DurationSecondType,
                >($values);)+
                $e
            }
            $crate::repeat_pat!(arrow_schema::DataType::Duration(arrow_schema::TimeUnit::Millisecond), $($values),+) => {
                $(let $values = $crate::cast::as_primitive_array::<
                    $crate::types::DurationMillisecondType,
                >($values);)+
                $e
            }
            $crate::repeat_pat!(arrow_schema::DataType::Duration(arrow_schema::TimeUnit::Microsecond), $($values),+) => {
                $(let $values = $crate::cast::as_primitive_array::<
                    $crate::types::DurationMicrosecondType,
                >($values);)+
                $e
            }
            $crate::repeat_pat!(arrow_schema::DataType::Duration(arrow_schema::TimeUnit::Nanosecond), $($values),+) => {
                $(let $values = $crate::cast::as_primitive_array::<
                    $crate::types::DurationNanosecondType,
                >($values);)+
                $e
            }
            $(($($p),+) => $fallback,)*
        }
    };
}

/// Force downcast of an [`Array`], such as an [`ArrayRef`], to
/// [`PrimitiveArray<T>`], panic'ing on failure.
///
/// # Example
///
/// ```
/// # use std::sync::Arc;
/// # use arrow_array::{ArrayRef, Int32Array};
/// # use arrow_array::cast::as_primitive_array;
/// # use arrow_array::types::Int32Type;
///
/// let arr: ArrayRef = Arc::new(Int32Array::from(vec![Some(1)]));
///
/// // Downcast an `ArrayRef` to Int32Array / PrimiveArray<Int32>:
/// let primitive_array: &Int32Array = as_primitive_array(&arr);
///
/// // Equivalently:
/// let primitive_array = as_primitive_array::<Int32Type>(&arr);
///
/// // This is the equivalent of:
/// let primitive_array = arr
///     .as_any()
///     .downcast_ref::<Int32Array>()
///     .unwrap();
/// ```

pub fn as_primitive_array<T>(arr: &dyn Array) -> &PrimitiveArray<T>
where
    T: ArrowPrimitiveType,
{
    arr.as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .expect("Unable to downcast to primitive array")
}

/// Downcast an [`Array`] to a [`DictionaryArray`] based on its [`DataType`], accepts
/// a number of subsequent patterns to match the data type
///
/// ```
/// # use arrow_array::{Array, StringArray, downcast_dictionary_array, cast::as_string_array};
/// # use arrow_schema::DataType;
///
/// fn print_strings(array: &dyn Array) {
///     downcast_dictionary_array!(
///         array => match array.values().data_type() {
///             DataType::Utf8 => {
///                 for v in array.downcast_dict::<StringArray>().unwrap() {
///                     println!("{:?}", v);
///                 }
///             }
///             t => println!("Unsupported dictionary value type {}", t),
///         },
///         DataType::Utf8 => {
///             for v in as_string_array(array) {
///                 println!("{:?}", v);
///             }
///         }
///         t => println!("Unsupported datatype {}", t)
///     )
/// }
/// ```
///
/// [`DataType`]: arrow_schema::DataType
#[macro_export]
macro_rules! downcast_dictionary_array {
    ($values:ident => $e:expr, $($p:pat => $fallback:expr $(,)*)*) => {
        downcast_dictionary_array!($values => {$e} $($p => $fallback)*)
    };

    ($values:ident => $e:block $($p:pat => $fallback:expr $(,)*)*) => {
        match $values.data_type() {
            arrow_schema::DataType::Dictionary(k, _) => match k.as_ref() {
                arrow_schema::DataType::Int8 => {
                    let $values = $crate::cast::as_dictionary_array::<
                        $crate::types::Int8Type,
                    >($values);
                    $e
                },
                arrow_schema::DataType::Int16 => {
                    let $values = $crate::cast::as_dictionary_array::<
                        $crate::types::Int16Type,
                    >($values);
                    $e
                },
                arrow_schema::DataType::Int32 => {
                    let $values = $crate::cast::as_dictionary_array::<
                        $crate::types::Int32Type,
                    >($values);
                    $e
                },
                arrow_schema::DataType::Int64 => {
                    let $values = $crate::cast::as_dictionary_array::<
                        $crate::types::Int64Type,
                    >($values);
                    $e
                },
                arrow_schema::DataType::UInt8 => {
                    let $values = $crate::cast::as_dictionary_array::<
                        $crate::types::UInt8Type,
                    >($values);
                    $e
                },
                arrow_schema::DataType::UInt16 => {
                    let $values = $crate::cast::as_dictionary_array::<
                        $crate::types::UInt16Type,
                    >($values);
                    $e
                },
                arrow_schema::DataType::UInt32 => {
                    let $values = $crate::cast::as_dictionary_array::<
                        $crate::types::UInt32Type,
                    >($values);
                    $e
                },
                arrow_schema::DataType::UInt64 => {
                    let $values = $crate::cast::as_dictionary_array::<
                        $crate::types::UInt64Type,
                    >($values);
                    $e
                },
                k => unreachable!("unsupported dictionary key type: {}", k)
            }
            $($p => $fallback,)*
        }
    }
}

/// Force downcast of an [`Array`], such as an [`ArrayRef`] to
/// [`DictionaryArray<T>`], panic'ing on failure.
///
/// # Example
///
/// ```
/// # use arrow_array::{ArrayRef, DictionaryArray};
/// # use arrow_array::cast::as_dictionary_array;
/// # use arrow_array::types::Int32Type;
///
/// let arr: DictionaryArray<Int32Type> = vec![Some("foo")].into_iter().collect();
/// let arr: ArrayRef = std::sync::Arc::new(arr);
/// let dict_array: &DictionaryArray<Int32Type> = as_dictionary_array::<Int32Type>(&arr);
/// ```
pub fn as_dictionary_array<T>(arr: &dyn Array) -> &DictionaryArray<T>
where
    T: ArrowDictionaryKeyType,
{
    arr.as_any()
        .downcast_ref::<DictionaryArray<T>>()
        .expect("Unable to downcast to dictionary array")
}

/// Force downcast of an [`Array`], such as an [`ArrayRef`] to
/// [`GenericListArray<T>`], panic'ing on failure.
pub fn as_generic_list_array<S: OffsetSizeTrait>(
    arr: &dyn Array,
) -> &GenericListArray<S> {
    arr.as_any()
        .downcast_ref::<GenericListArray<S>>()
        .expect("Unable to downcast to list array")
}

/// Force downcast of an [`Array`], such as an [`ArrayRef`] to
/// [`ListArray`], panic'ing on failure.
#[inline]
pub fn as_list_array(arr: &dyn Array) -> &ListArray {
    as_generic_list_array::<i32>(arr)
}

/// Force downcast of an [`Array`], such as an [`ArrayRef`] to
/// [`LargeListArray`], panic'ing on failure.
#[inline]
pub fn as_large_list_array(arr: &dyn Array) -> &LargeListArray {
    as_generic_list_array::<i64>(arr)
}

/// Force downcast of an [`Array`], such as an [`ArrayRef`] to
/// [`GenericBinaryArray<S>`], panic'ing on failure.
#[inline]
pub fn as_generic_binary_array<S: OffsetSizeTrait>(
    arr: &dyn Array,
) -> &GenericBinaryArray<S> {
    arr.as_any()
        .downcast_ref::<GenericBinaryArray<S>>()
        .expect("Unable to downcast to binary array")
}

/// Force downcast of an [`Array`], such as an [`ArrayRef`] to
/// [`StringArray`], panic'ing on failure.
///
/// # Example
///
/// ```
/// # use std::sync::Arc;
/// # use arrow_array::cast::as_string_array;
/// # use arrow_array::{ArrayRef, StringArray};
///
/// let arr: ArrayRef = Arc::new(StringArray::from_iter(vec![Some("foo")]));
/// let string_array = as_string_array(&arr);
/// ```
pub fn as_string_array(arr: &dyn Array) -> &StringArray {
    arr.as_any()
        .downcast_ref::<StringArray>()
        .expect("Unable to downcast to StringArray")
}

/// Force downcast of an [`Array`], such as an [`ArrayRef`] to
/// [`BooleanArray`], panic'ing on failure.
///
/// # Example
///
/// ```
/// # use std::sync::Arc;
/// # use arrow_array::{ArrayRef, BooleanArray};
/// # use arrow_array::cast::as_boolean_array;
///
/// let arr: ArrayRef = Arc::new(BooleanArray::from_iter(vec![Some(true)]));
/// let boolean_array = as_boolean_array(&arr);
/// ```
pub fn as_boolean_array(arr: &dyn Array) -> &BooleanArray {
    arr.as_any()
        .downcast_ref::<BooleanArray>()
        .expect("Unable to downcast to BooleanArray")
}

macro_rules! array_downcast_fn {
    ($name: ident, $arrty: ty, $arrty_str:expr) => {
        #[doc = "Force downcast of an [`Array`], such as an [`ArrayRef`] to "]
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
        array_downcast_fn!(
            $name,
            $arrty,
            concat!("[`", stringify!($arrty), "`], panic'ing on failure.")
        );
    };
}

array_downcast_fn!(as_largestring_array, LargeStringArray);
array_downcast_fn!(as_null_array, NullArray);
array_downcast_fn!(as_struct_array, StructArray);
array_downcast_fn!(as_union_array, UnionArray);
array_downcast_fn!(as_map_array, MapArray);
array_downcast_fn!(as_decimal_array, Decimal128Array);

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn test_as_decimal_array_ref() {
        let array: Decimal128Array = vec![Some(123), None, Some(1111)]
            .into_iter()
            .collect::<Decimal128Array>()
            .with_precision_and_scale(10, 2)
            .unwrap();
        assert!(!as_decimal_array(&array).is_empty());
        let result_decimal = as_decimal_array(&array);
        assert_eq!(result_decimal, &array);
    }

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
