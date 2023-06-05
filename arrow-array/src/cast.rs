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
use arrow_data::ArrayData;

/// Repeats the provided pattern based on the number of comma separated identifiers
#[doc(hidden)]
#[macro_export]
macro_rules! repeat_pat {
    ($e:pat, $v_:expr) => {
        $e
    };
    ($e:pat, $v_:expr $(, $tail:expr)+) => {
        ($e, $crate::repeat_pat!($e $(, $tail)+))
    }
}

/// Given one or more expressions evaluating to an integer [`DataType`] invokes the provided macro
/// `m` with the corresponding integer [`ArrowPrimitiveType`], followed by any additional arguments
///
/// ```
/// # use arrow_array::{downcast_primitive, ArrowPrimitiveType, downcast_integer};
/// # use arrow_schema::DataType;
///
/// macro_rules! dictionary_key_size_helper {
///   ($t:ty, $o:ty) => {
///       std::mem::size_of::<<$t as ArrowPrimitiveType>::Native>() as $o
///   };
/// }
///
/// fn dictionary_key_size(t: &DataType) -> u8 {
///     match t {
///         DataType::Dictionary(k, _) => downcast_integer! {
///             k.as_ref() => (dictionary_key_size_helper, u8),
///             _ => unreachable!(),
///         },
///         _ => u8::MAX,
///     }
/// }
///
/// assert_eq!(dictionary_key_size(&DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))), 4);
/// assert_eq!(dictionary_key_size(&DataType::Dictionary(Box::new(DataType::Int64), Box::new(DataType::Utf8))), 8);
/// assert_eq!(dictionary_key_size(&DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8))), 2);
/// ```
///
/// [`DataType`]: arrow_schema::DataType
#[macro_export]
macro_rules! downcast_integer {
    ($($data_type:expr),+ => ($m:path $(, $args:tt)*), $($($p:pat),+ => $fallback:expr $(,)*)*) => {
        match ($($data_type),+) {
            $crate::repeat_pat!(arrow_schema::DataType::Int8, $($data_type),+) => {
                $m!($crate::types::Int8Type $(, $args)*)
            }
            $crate::repeat_pat!(arrow_schema::DataType::Int16, $($data_type),+) => {
                $m!($crate::types::Int16Type $(, $args)*)
            }
            $crate::repeat_pat!(arrow_schema::DataType::Int32, $($data_type),+) => {
                $m!($crate::types::Int32Type $(, $args)*)
            }
            $crate::repeat_pat!(arrow_schema::DataType::Int64, $($data_type),+) => {
                $m!($crate::types::Int64Type $(, $args)*)
            }
            $crate::repeat_pat!(arrow_schema::DataType::UInt8, $($data_type),+) => {
                $m!($crate::types::UInt8Type $(, $args)*)
            }
            $crate::repeat_pat!(arrow_schema::DataType::UInt16, $($data_type),+) => {
                $m!($crate::types::UInt16Type $(, $args)*)
            }
            $crate::repeat_pat!(arrow_schema::DataType::UInt32, $($data_type),+) => {
                $m!($crate::types::UInt32Type $(, $args)*)
            }
            $crate::repeat_pat!(arrow_schema::DataType::UInt64, $($data_type),+) => {
                $m!($crate::types::UInt64Type $(, $args)*)
            }
            $(($($p),+) => $fallback,)*
        }
    };
}

/// Given one or more expressions evaluating to an integer [`DataType`] invokes the provided macro
/// `m` with the corresponding integer [`RunEndIndexType`], followed by any additional arguments
///
/// ```
/// # use std::sync::Arc;
/// # use arrow_array::{downcast_primitive, ArrowPrimitiveType, downcast_run_end_index};
/// # use arrow_schema::{DataType, Field};
///
/// macro_rules! run_end_size_helper {
///   ($t:ty, $o:ty) => {
///       std::mem::size_of::<<$t as ArrowPrimitiveType>::Native>() as $o
///   };
/// }
///
/// fn run_end_index_size(t: &DataType) -> u8 {
///     match t {
///         DataType::RunEndEncoded(k, _) => downcast_run_end_index! {
///             k.data_type() => (run_end_size_helper, u8),
///             _ => unreachable!(),
///         },
///         _ => u8::MAX,
///     }
/// }
///
/// assert_eq!(run_end_index_size(&DataType::RunEndEncoded(Arc::new(Field::new("a", DataType::Int32, false)), Arc::new(Field::new("b", DataType::Utf8, true)))), 4);
/// assert_eq!(run_end_index_size(&DataType::RunEndEncoded(Arc::new(Field::new("a", DataType::Int64, false)), Arc::new(Field::new("b", DataType::Utf8, true)))), 8);
/// assert_eq!(run_end_index_size(&DataType::RunEndEncoded(Arc::new(Field::new("a", DataType::Int16, false)), Arc::new(Field::new("b", DataType::Utf8, true)))), 2);
/// ```
///
/// [`DataType`]: arrow_schema::DataType
#[macro_export]
macro_rules! downcast_run_end_index {
    ($($data_type:expr),+ => ($m:path $(, $args:tt)*), $($($p:pat),+ => $fallback:expr $(,)*)*) => {
        match ($($data_type),+) {
            $crate::repeat_pat!(arrow_schema::DataType::Int16, $($data_type),+) => {
                $m!($crate::types::Int16Type $(, $args)*)
            }
            $crate::repeat_pat!(arrow_schema::DataType::Int32, $($data_type),+) => {
                $m!($crate::types::Int32Type $(, $args)*)
            }
            $crate::repeat_pat!(arrow_schema::DataType::Int64, $($data_type),+) => {
                $m!($crate::types::Int64Type $(, $args)*)
            }
            $(($($p),+) => $fallback,)*
        }
    };
}

/// Given one or more expressions evaluating to primitive [`DataType`] invokes the provided macro
/// `m` with the corresponding [`ArrowPrimitiveType`], followed by any additional arguments
///
/// ```
/// # use arrow_array::{downcast_temporal, ArrowPrimitiveType};
/// # use arrow_schema::DataType;
///
/// macro_rules! temporal_size_helper {
///   ($t:ty, $o:ty) => {
///       std::mem::size_of::<<$t as ArrowPrimitiveType>::Native>() as $o
///   };
/// }
///
/// fn temporal_size(t: &DataType) -> u8 {
///     downcast_temporal! {
///         t => (temporal_size_helper, u8),
///         _ => u8::MAX
///     }
/// }
///
/// assert_eq!(temporal_size(&DataType::Date32), 4);
/// assert_eq!(temporal_size(&DataType::Date64), 8);
/// ```
///
/// [`DataType`]: arrow_schema::DataType
#[macro_export]
macro_rules! downcast_temporal {
    ($($data_type:expr),+ => ($m:path $(, $args:tt)*), $($($p:pat),+ => $fallback:expr $(,)*)*) => {
        match ($($data_type),+) {
            $crate::repeat_pat!(arrow_schema::DataType::Time32(arrow_schema::TimeUnit::Second), $($data_type),+) => {
                $m!($crate::types::Time32SecondType $(, $args)*)
            }
            $crate::repeat_pat!(arrow_schema::DataType::Time32(arrow_schema::TimeUnit::Millisecond), $($data_type),+) => {
                $m!($crate::types::Time32MillisecondType $(, $args)*)
            }
            $crate::repeat_pat!(arrow_schema::DataType::Time64(arrow_schema::TimeUnit::Microsecond), $($data_type),+) => {
                $m!($crate::types::Time64MicrosecondType $(, $args)*)
            }
            $crate::repeat_pat!(arrow_schema::DataType::Time64(arrow_schema::TimeUnit::Nanosecond), $($data_type),+) => {
                $m!($crate::types::Time64NanosecondType $(, $args)*)
            }
            $crate::repeat_pat!(arrow_schema::DataType::Date32, $($data_type),+) => {
                $m!($crate::types::Date32Type $(, $args)*)
            }
            $crate::repeat_pat!(arrow_schema::DataType::Date64, $($data_type),+) => {
                $m!($crate::types::Date64Type $(, $args)*)
            }
            $crate::repeat_pat!(arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Second, _), $($data_type),+) => {
                $m!($crate::types::TimestampSecondType $(, $args)*)
            }
            $crate::repeat_pat!(arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, _), $($data_type),+) => {
                $m!($crate::types::TimestampMillisecondType $(, $args)*)
            }
            $crate::repeat_pat!(arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, _), $($data_type),+) => {
                $m!($crate::types::TimestampMicrosecondType $(, $args)*)
            }
            $crate::repeat_pat!(arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, _), $($data_type),+) => {
                $m!($crate::types::TimestampNanosecondType $(, $args)*)
            }
            $(($($p),+) => $fallback,)*
        }
    };
}

/// Downcast an [`Array`] to a temporal [`PrimitiveArray`] based on its [`DataType`]
/// accepts a number of subsequent patterns to match the data type
///
/// ```
/// # use arrow_array::{Array, downcast_temporal_array, cast::as_string_array};
/// # use arrow_schema::DataType;
///
/// fn print_temporal(array: &dyn Array) {
///     downcast_temporal_array!(
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
macro_rules! downcast_temporal_array {
    ($values:ident => $e:expr, $($p:pat => $fallback:expr $(,)*)*) => {
        $crate::downcast_temporal_array!($values => {$e} $($p => $fallback)*)
    };
    (($($values:ident),+) => $e:block $($($p:pat),+ => $fallback:expr $(,)*)*) => {
        $crate::downcast_temporal_array!($($values),+ => $e $($($p),+ => $fallback)*)
    };
    (($($values:ident),+) => $e:block $(($($p:pat),+) => $fallback:expr $(,)*)*) => {
        $crate::downcast_temporal_array!($($values),+ => $e $($($p),+ => $fallback)*)
    };
    ($($values:ident),+ => $e:block $($($p:pat),+ => $fallback:expr $(,)*)*) => {
        $crate::downcast_temporal!{
            $($values.data_type()),+ => ($crate::downcast_primitive_array_helper, $($values),+, $e),
            $($($p),+ => $fallback,)*
        }
    };
}

/// Given one or more expressions evaluating to primitive [`DataType`] invokes the provided macro
/// `m` with the corresponding [`ArrowPrimitiveType`], followed by any additional arguments
///
/// ```
/// # use arrow_array::{downcast_primitive, ArrowPrimitiveType};
/// # use arrow_schema::DataType;
///
/// macro_rules! primitive_size_helper {
///   ($t:ty, $o:ty) => {
///       std::mem::size_of::<<$t as ArrowPrimitiveType>::Native>() as $o
///   };
/// }
///
/// fn primitive_size(t: &DataType) -> u8 {
///     downcast_primitive! {
///         t => (primitive_size_helper, u8),
///         _ => u8::MAX
///     }
/// }
///
/// assert_eq!(primitive_size(&DataType::Int32), 4);
/// assert_eq!(primitive_size(&DataType::Int64), 8);
/// assert_eq!(primitive_size(&DataType::Float16), 2);
/// assert_eq!(primitive_size(&DataType::Decimal128(38, 10)), 16);
/// assert_eq!(primitive_size(&DataType::Decimal256(76, 20)), 32);
/// ```
///
/// [`DataType`]: arrow_schema::DataType
#[macro_export]
macro_rules! downcast_primitive {
    ($($data_type:expr),+ => ($m:path $(, $args:tt)*), $($($p:pat),+ => $fallback:expr $(,)*)*) => {
        $crate::downcast_integer! {
            $($data_type),+ => ($m $(, $args)*),
            $crate::repeat_pat!(arrow_schema::DataType::Float16, $($data_type),+) => {
                $m!($crate::types::Float16Type $(, $args)*)
            }
            $crate::repeat_pat!(arrow_schema::DataType::Float32, $($data_type),+) => {
                $m!($crate::types::Float32Type $(, $args)*)
            }
            $crate::repeat_pat!(arrow_schema::DataType::Float64, $($data_type),+) => {
                $m!($crate::types::Float64Type $(, $args)*)
            }
            $crate::repeat_pat!(arrow_schema::DataType::Decimal128(_, _), $($data_type),+) => {
                $m!($crate::types::Decimal128Type $(, $args)*)
            }
            $crate::repeat_pat!(arrow_schema::DataType::Decimal256(_, _), $($data_type),+) => {
                $m!($crate::types::Decimal256Type $(, $args)*)
            }
            $crate::repeat_pat!(arrow_schema::DataType::Interval(arrow_schema::IntervalUnit::YearMonth), $($data_type),+) => {
                $m!($crate::types::IntervalYearMonthType $(, $args)*)
            }
            $crate::repeat_pat!(arrow_schema::DataType::Interval(arrow_schema::IntervalUnit::DayTime), $($data_type),+) => {
                $m!($crate::types::IntervalDayTimeType $(, $args)*)
            }
            $crate::repeat_pat!(arrow_schema::DataType::Interval(arrow_schema::IntervalUnit::MonthDayNano), $($data_type),+) => {
                $m!($crate::types::IntervalMonthDayNanoType $(, $args)*)
            }
            $crate::repeat_pat!(arrow_schema::DataType::Duration(arrow_schema::TimeUnit::Second), $($data_type),+) => {
                $m!($crate::types::DurationSecondType $(, $args)*)
            }
            $crate::repeat_pat!(arrow_schema::DataType::Duration(arrow_schema::TimeUnit::Millisecond), $($data_type),+) => {
                $m!($crate::types::DurationMillisecondType $(, $args)*)
            }
            $crate::repeat_pat!(arrow_schema::DataType::Duration(arrow_schema::TimeUnit::Microsecond), $($data_type),+) => {
                $m!($crate::types::DurationMicrosecondType $(, $args)*)
            }
            $crate::repeat_pat!(arrow_schema::DataType::Duration(arrow_schema::TimeUnit::Nanosecond), $($data_type),+) => {
                $m!($crate::types::DurationNanosecondType $(, $args)*)
            }
            _ => {
                $crate::downcast_temporal! {
                    $($data_type),+ => ($m $(, $args)*),
                    $($($p),+ => $fallback,)*
                }
            }
        }
    };
}

#[macro_export]
#[doc(hidden)]
macro_rules! downcast_primitive_array_helper {
    ($t:ty, $($values:ident),+, $e:block) => {{
        $(let $values = $crate::cast::as_primitive_array::<$t>($values);)+
        $e
    }};
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
        $crate::downcast_primitive_array!($values => {$e} $($p => $fallback)*)
    };
    (($($values:ident),+) => $e:block $($($p:pat),+ => $fallback:expr $(,)*)*) => {
        $crate::downcast_primitive_array!($($values),+ => $e $($($p),+ => $fallback)*)
    };
    (($($values:ident),+) => $e:block $(($($p:pat),+) => $fallback:expr $(,)*)*) => {
        $crate::downcast_primitive_array!($($values),+ => $e $($($p),+ => $fallback)*)
    };
    ($($values:ident),+ => $e:block $($($p:pat),+ => $fallback:expr $(,)*)*) => {
        $crate::downcast_primitive!{
            $($values.data_type()),+ => ($crate::downcast_primitive_array_helper, $($values),+, $e),
            $($($p),+ => $fallback,)*
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
/// // Downcast an `ArrayRef` to Int32Array / PrimitiveArray<Int32>:
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

#[macro_export]
#[doc(hidden)]
macro_rules! downcast_dictionary_array_helper {
    ($t:ty, $($values:ident),+, $e:block) => {{
        $(let $values = $crate::cast::as_dictionary_array::<$t>($values);)+
        $e
    }};
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
            arrow_schema::DataType::Dictionary(k, _) => {
                $crate::downcast_integer! {
                    k.as_ref() => ($crate::downcast_dictionary_array_helper, $values, $e),
                    k => unreachable!("unsupported dictionary key type: {}", k)
                }
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
/// [`RunArray<T>`], panic'ing on failure.
///
/// # Example
///
/// ```
/// # use arrow_array::{ArrayRef, RunArray};
/// # use arrow_array::cast::as_run_array;
/// # use arrow_array::types::Int32Type;
///
/// let arr: RunArray<Int32Type> = vec![Some("foo")].into_iter().collect();
/// let arr: ArrayRef = std::sync::Arc::new(arr);
/// let run_array: &RunArray<Int32Type> = as_run_array::<Int32Type>(&arr);
/// ```
pub fn as_run_array<T>(arr: &dyn Array) -> &RunArray<T>
where
    T: RunEndIndexType,
{
    arr.as_any()
        .downcast_ref::<RunArray<T>>()
        .expect("Unable to downcast to run array")
}

#[macro_export]
#[doc(hidden)]
macro_rules! downcast_run_array_helper {
    ($t:ty, $($values:ident),+, $e:block) => {{
        $(let $values = $crate::cast::as_run_array::<$t>($values);)+
        $e
    }};
}

/// Downcast an [`Array`] to a [`RunArray`] based on its [`DataType`], accepts
/// a number of subsequent patterns to match the data type
///
/// ```
/// # use arrow_array::{Array, StringArray, downcast_run_array, cast::as_string_array};
/// # use arrow_schema::DataType;
///
/// fn print_strings(array: &dyn Array) {
///     downcast_run_array!(
///         array => match array.values().data_type() {
///             DataType::Utf8 => {
///                 for v in array.downcast::<StringArray>().unwrap() {
///                     println!("{:?}", v);
///                 }
///             }
///             t => println!("Unsupported run array value type {}", t),
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
macro_rules! downcast_run_array {
    ($values:ident => $e:expr, $($p:pat => $fallback:expr $(,)*)*) => {
        downcast_run_array!($values => {$e} $($p => $fallback)*)
    };

    ($values:ident => $e:block $($p:pat => $fallback:expr $(,)*)*) => {
        match $values.data_type() {
            arrow_schema::DataType::RunEndEncoded(k, _) => {
                $crate::downcast_run_end_index! {
                    k.data_type() => ($crate::downcast_run_array_helper, $values, $e),
                    k => unreachable!("unsupported run end index type: {}", k)
                }
            }
            $($p => $fallback,)*
        }
    }
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
/// [`FixedSizeListArray`], panic'ing on failure.
#[inline]
pub fn as_fixed_size_list_array(arr: &dyn Array) -> &FixedSizeListArray {
    arr.as_any()
        .downcast_ref::<FixedSizeListArray>()
        .expect("Unable to downcast to fixed size list array")
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

/// Force downcast of an Array, such as an ArrayRef to Decimal128Array, panic’ing on failure.
#[deprecated(note = "please use `as_primitive_array::<Decimal128Type>` instead")]
pub fn as_decimal_array(arr: &dyn Array) -> &PrimitiveArray<Decimal128Type> {
    as_primitive_array::<Decimal128Type>(arr)
}

/// Downcasts a `dyn Array` to a concrete type
///
/// ```
/// # use arrow_array::{BooleanArray, Int32Array, RecordBatch, StringArray};
/// # use arrow_array::cast::downcast_array;
/// struct ConcreteBatch {
///     col1: Int32Array,
///     col2: BooleanArray,
///     col3: StringArray,
/// }
///
/// impl ConcreteBatch {
///     fn new(batch: &RecordBatch) -> Self {
///         Self {
///             col1: downcast_array(batch.column(0).as_ref()),
///             col2: downcast_array(batch.column(1).as_ref()),
///             col3: downcast_array(batch.column(2).as_ref()),
///         }
///     }
/// }
/// ```
///
/// # Panics
///
/// Panics if array is not of the correct data type
pub fn downcast_array<T>(array: &dyn Array) -> T
where
    T: From<ArrayData>,
{
    T::from(array.to_data())
}

mod private {
    pub trait Sealed {}
}

/// An extension trait for `dyn Array` that provides ergonomic downcasting
///
/// ```
/// # use std::sync::Arc;
/// # use arrow_array::{ArrayRef, Int32Array};
/// # use arrow_array::cast::AsArray;
/// # use arrow_array::types::Int32Type;
/// let col = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
/// assert_eq!(col.as_primitive::<Int32Type>().values(), &[1, 2, 3]);
/// ```
pub trait AsArray: private::Sealed {
    /// Downcast this to a [`BooleanArray`] returning `None` if not possible
    fn as_boolean_opt(&self) -> Option<&BooleanArray>;

    /// Downcast this to a [`BooleanArray`] panicking if not possible
    fn as_boolean(&self) -> &BooleanArray {
        self.as_boolean_opt().expect("boolean array")
    }

    /// Downcast this to a [`PrimitiveArray`] returning `None` if not possible
    fn as_primitive_opt<T: ArrowPrimitiveType>(&self) -> Option<&PrimitiveArray<T>>;

    /// Downcast this to a [`PrimitiveArray`] panicking if not possible
    fn as_primitive<T: ArrowPrimitiveType>(&self) -> &PrimitiveArray<T> {
        self.as_primitive_opt().expect("primitive array")
    }

    /// Downcast this to a [`GenericByteArray`] returning `None` if not possible
    fn as_bytes_opt<T: ByteArrayType>(&self) -> Option<&GenericByteArray<T>>;

    /// Downcast this to a [`GenericByteArray`] panicking if not possible
    fn as_bytes<T: ByteArrayType>(&self) -> &GenericByteArray<T> {
        self.as_bytes_opt().expect("byte array")
    }

    /// Downcast this to a [`GenericStringArray`] returning `None` if not possible
    fn as_string_opt<O: OffsetSizeTrait>(&self) -> Option<&GenericStringArray<O>> {
        self.as_bytes_opt()
    }

    /// Downcast this to a [`GenericStringArray`] panicking if not possible
    fn as_string<O: OffsetSizeTrait>(&self) -> &GenericStringArray<O> {
        self.as_bytes_opt().expect("string array")
    }

    /// Downcast this to a [`GenericBinaryArray`] returning `None` if not possible
    fn as_binary_opt<O: OffsetSizeTrait>(&self) -> Option<&GenericBinaryArray<O>> {
        self.as_bytes_opt()
    }

    /// Downcast this to a [`GenericBinaryArray`] panicking if not possible
    fn as_binary<O: OffsetSizeTrait>(&self) -> &GenericBinaryArray<O> {
        self.as_bytes_opt().expect("binary array")
    }

    /// Downcast this to a [`StructArray`] returning `None` if not possible
    fn as_struct_opt(&self) -> Option<&StructArray>;

    /// Downcast this to a [`StructArray`] panicking if not possible
    fn as_struct(&self) -> &StructArray {
        self.as_struct_opt().expect("struct array")
    }

    /// Downcast this to a [`GenericListArray`] returning `None` if not possible
    fn as_list_opt<O: OffsetSizeTrait>(&self) -> Option<&GenericListArray<O>>;

    /// Downcast this to a [`GenericListArray`] panicking if not possible
    fn as_list<O: OffsetSizeTrait>(&self) -> &GenericListArray<O> {
        self.as_list_opt().expect("list array")
    }

    /// Downcast this to a [`MapArray`] returning `None` if not possible
    fn as_map_opt(&self) -> Option<&MapArray>;

    /// Downcast this to a [`MapArray`] panicking if not possible
    fn as_map(&self) -> &MapArray {
        self.as_map_opt().expect("map array")
    }

    /// Downcast this to a [`DictionaryArray`] returning `None` if not possible
    fn as_dictionary_opt<K: ArrowDictionaryKeyType>(&self)
        -> Option<&DictionaryArray<K>>;

    /// Downcast this to a [`DictionaryArray`] panicking if not possible
    fn as_dictionary<K: ArrowDictionaryKeyType>(&self) -> &DictionaryArray<K> {
        self.as_dictionary_opt().expect("dictionary array")
    }
}

impl private::Sealed for dyn Array + '_ {}
impl AsArray for dyn Array + '_ {
    fn as_boolean_opt(&self) -> Option<&BooleanArray> {
        self.as_any().downcast_ref()
    }

    fn as_primitive_opt<T: ArrowPrimitiveType>(&self) -> Option<&PrimitiveArray<T>> {
        self.as_any().downcast_ref()
    }

    fn as_bytes_opt<T: ByteArrayType>(&self) -> Option<&GenericByteArray<T>> {
        self.as_any().downcast_ref()
    }

    fn as_struct_opt(&self) -> Option<&StructArray> {
        self.as_any().downcast_ref()
    }

    fn as_list_opt<O: OffsetSizeTrait>(&self) -> Option<&GenericListArray<O>> {
        self.as_any().downcast_ref()
    }

    fn as_map_opt(&self) -> Option<&MapArray> {
        self.as_any().downcast_ref()
    }

    fn as_dictionary_opt<K: ArrowDictionaryKeyType>(
        &self,
    ) -> Option<&DictionaryArray<K>> {
        self.as_any().downcast_ref()
    }
}

impl private::Sealed for ArrayRef {}
impl AsArray for ArrayRef {
    fn as_boolean_opt(&self) -> Option<&BooleanArray> {
        self.as_ref().as_boolean_opt()
    }

    fn as_primitive_opt<T: ArrowPrimitiveType>(&self) -> Option<&PrimitiveArray<T>> {
        self.as_ref().as_primitive_opt()
    }

    fn as_bytes_opt<T: ByteArrayType>(&self) -> Option<&GenericByteArray<T>> {
        self.as_ref().as_bytes_opt()
    }

    fn as_struct_opt(&self) -> Option<&StructArray> {
        self.as_ref().as_struct_opt()
    }

    fn as_list_opt<O: OffsetSizeTrait>(&self) -> Option<&GenericListArray<O>> {
        self.as_ref().as_list_opt()
    }

    fn as_map_opt(&self) -> Option<&MapArray> {
        self.as_any().downcast_ref()
    }

    fn as_dictionary_opt<K: ArrowDictionaryKeyType>(
        &self,
    ) -> Option<&DictionaryArray<K>> {
        self.as_ref().as_dictionary_opt()
    }
}

#[cfg(test)]
mod tests {
    use arrow_buffer::i256;
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

    #[test]
    fn test_decimal128array() {
        let a = Decimal128Array::from_iter_values([1, 2, 4, 5]);
        assert!(!as_primitive_array::<Decimal128Type>(&a).is_empty());
    }

    #[test]
    fn test_decimal256array() {
        let a = Decimal256Array::from_iter_values(
            [1, 2, 4, 5].into_iter().map(i256::from_i128),
        );
        assert!(!as_primitive_array::<Decimal256Type>(&a).is_empty());
    }
}
