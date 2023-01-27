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

use std::any::Any;

use arrow_buffer::ArrowNativeType;
use arrow_data::{ArrayData, ArrayDataBuilder};
use arrow_schema::{ArrowError, DataType, Field};

use crate::{
    builder::StringRunBuilder,
    make_array,
    types::{Int16Type, Int32Type, Int64Type, RunEndIndexType},
    Array, ArrayRef, PrimitiveArray,
};

///
/// A run-end encoding (REE) is a variation of [run-length encoding (RLE)](https://en.wikipedia.org/wiki/Run-length_encoding).
///
/// This encoding is good for representing data containing same values repeated consecutively.
///
/// [`RunArray`] contains `run_ends` array and `values` array of same length.
/// The `run_ends` array stores the indexes at which the run ends. The `values` array
/// stores the value of each run. Below example illustrates how a logical array is represented in
/// [`RunArray`]
///
///
/// ```text
/// ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─┐
///   ┌─────────────────┐  ┌─────────┐       ┌─────────────────┐
/// │ │        A        │  │    2    │ │     │        A        │     
///   ├─────────────────┤  ├─────────┤       ├─────────────────┤
/// │ │        D        │  │    3    │ │     │        A        │    run length of 'A' = runs_ends[0] - 0 = 2
///   ├─────────────────┤  ├─────────┤       ├─────────────────┤
/// │ │        B        │  │    6    │ │     │        D        │    run length of 'D' = run_ends[1] - run_ends[0] = 1
///   └─────────────────┘  └─────────┘       ├─────────────────┤
/// │        values          run_ends  │     │        B        │     
///                                          ├─────────────────┤
/// └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─┘     │        B        │     
///                                          ├─────────────────┤
///                RunArray                  │        B        │    run length of 'B' = run_ends[2] - run_ends[1] = 3
///               length = 3                 └─────────────────┘
///  
///                                             Logical array
///                                                Contents
/// ```

pub struct RunArray<R: RunEndIndexType> {
    data: ArrayData,
    run_ends: PrimitiveArray<R>,
    values: ArrayRef,
}

impl<R: RunEndIndexType> RunArray<R> {
    // calculates the logical length of the array encoded
    // by the given run_ends array.
    fn logical_len(run_ends: &PrimitiveArray<R>) -> usize {
        let len = run_ends.len();
        if len == 0 {
            return 0;
        }
        run_ends.value(len - 1).as_usize()
    }

    /// Attempts to create RunArray using given run_ends (index where a run ends)
    /// and the values (value of the run). Returns an error if the given data is not compatible
    /// with RunEndEncoded specification.
    pub fn try_new(
        run_ends: &PrimitiveArray<R>,
        values: &dyn Array,
    ) -> Result<Self, ArrowError> {
        let run_ends_type = run_ends.data_type().clone();
        let values_type = values.data_type().clone();
        let ree_array_type = DataType::RunEndEncoded(
            Box::new(Field::new("run_ends", run_ends_type, false)),
            Box::new(Field::new("values", values_type, true)),
        );
        let len = RunArray::logical_len(run_ends);
        let builder = ArrayDataBuilder::new(ree_array_type)
            .len(len)
            .add_child_data(run_ends.data().clone())
            .add_child_data(values.data().clone());

        // `build_unchecked` is used to avoid recursive validation of child arrays.
        let array_data = unsafe { builder.build_unchecked() };

        // Safety: `validate_data` checks below
        //    1. The given array data has exactly two child arrays.
        //    2. The first child array (run_ends) has valid data type.
        //    3. run_ends array does not have null values
        //    4. run_ends array has non-zero and strictly increasing values.
        //    5. The length of run_ends array and values array are the same.
        array_data.validate_data()?;

        Ok(array_data.into())
    }

    /// Returns a reference to run_ends array
    ///
    /// Note: any slicing of this array is not applied to the returned array
    /// and must be handled separately
    pub fn run_ends(&self) -> &PrimitiveArray<R> {
        &self.run_ends
    }

    /// Returns a reference to values array
    pub fn values(&self) -> &ArrayRef {
        &self.values
    }
}

impl<R: RunEndIndexType> From<ArrayData> for RunArray<R> {
    // The method assumes the caller already validated the data using `ArrayData::validate_data()`
    fn from(data: ArrayData) -> Self {
        match data.data_type() {
            DataType::RunEndEncoded(_, _) => {}
            _ => {
                panic!("Invalid data type for RunArray. The data type should be DataType::RunEndEncoded");
            }
        }

        let run_ends = PrimitiveArray::<R>::from(data.child_data()[0].clone());
        let values = make_array(data.child_data()[1].clone());
        Self {
            data,
            run_ends,
            values,
        }
    }
}

impl<R: RunEndIndexType> From<RunArray<R>> for ArrayData {
    fn from(array: RunArray<R>) -> Self {
        array.data
    }
}

impl<T: RunEndIndexType> Array for RunArray<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data(&self) -> &ArrayData {
        &self.data
    }

    fn into_data(self) -> ArrayData {
        self.into()
    }
}

impl<R: RunEndIndexType> std::fmt::Debug for RunArray<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        writeln!(
            f,
            "RunArray {{run_ends: {:?}, values: {:?}}}",
            self.run_ends, self.values
        )
    }
}

/// Constructs a `RunArray` from an iterator of optional strings.
///
/// # Example:
/// ```
/// use arrow_array::{RunArray, PrimitiveArray, StringArray, types::Int16Type};
///
/// let test = vec!["a", "a", "b", "c", "c"];
/// let array: RunArray<Int16Type> = test
///     .iter()
///     .map(|&x| if x == "b" { None } else { Some(x) })
///     .collect();
/// assert_eq!(
///     "RunArray {run_ends: PrimitiveArray<Int16>\n[\n  2,\n  3,\n  5,\n], values: StringArray\n[\n  \"a\",\n  null,\n  \"c\",\n]}\n",
///     format!("{:?}", array)
/// );
/// ```
impl<'a, T: RunEndIndexType> FromIterator<Option<&'a str>> for RunArray<T> {
    fn from_iter<I: IntoIterator<Item = Option<&'a str>>>(iter: I) -> Self {
        let it = iter.into_iter();
        let (lower, _) = it.size_hint();
        let mut builder = StringRunBuilder::with_capacity(lower, 256);
        it.for_each(|i| {
            builder.append_option(i);
        });

        builder.finish()
    }
}

/// Constructs a `RunArray` from an iterator of strings.
///
/// # Example:
///
/// ```
/// use arrow_array::{RunArray, PrimitiveArray, StringArray, types::Int16Type};
///
/// let test = vec!["a", "a", "b", "c"];
/// let array: RunArray<Int16Type> = test.into_iter().collect();
/// assert_eq!(
///     "RunArray {run_ends: PrimitiveArray<Int16>\n[\n  2,\n  3,\n  4,\n], values: StringArray\n[\n  \"a\",\n  \"b\",\n  \"c\",\n]}\n",
///     format!("{:?}", array)
/// );
/// ```
impl<'a, T: RunEndIndexType> FromIterator<&'a str> for RunArray<T> {
    fn from_iter<I: IntoIterator<Item = &'a str>>(iter: I) -> Self {
        let it = iter.into_iter();
        let (lower, _) = it.size_hint();
        let mut builder = StringRunBuilder::with_capacity(lower, 256);
        it.for_each(|i| {
            builder.append_value(i);
        });

        builder.finish()
    }
}

///
/// A [`RunArray`] array where run ends are stored using `i16` data type.
///
/// # Example: Using `collect`
/// ```
/// # use arrow_array::{Array, Int16RunArray, Int16Array, StringArray};
/// # use std::sync::Arc;
///
/// let array: Int16RunArray = vec!["a", "a", "b", "c", "c"].into_iter().collect();
/// let values: Arc<dyn Array> = Arc::new(StringArray::from(vec!["a", "b", "c"]));
/// assert_eq!(array.run_ends(), &Int16Array::from(vec![2, 3, 5]));
/// assert_eq!(array.values(), &values);
/// ```
pub type Int16RunArray = RunArray<Int16Type>;

///
/// A [`RunArray`] array where run ends are stored using `i32` data type.
///
/// # Example: Using `collect`
/// ```
/// # use arrow_array::{Array, Int32RunArray, Int32Array, StringArray};
/// # use std::sync::Arc;
///
/// let array: Int32RunArray = vec!["a", "a", "b", "c", "c"].into_iter().collect();
/// let values: Arc<dyn Array> = Arc::new(StringArray::from(vec!["a", "b", "c"]));
/// assert_eq!(array.run_ends(), &Int32Array::from(vec![2, 3, 5]));
/// assert_eq!(array.values(), &values);
/// ```
pub type Int32RunArray = RunArray<Int32Type>;

///
/// A [`RunArray`] array where run ends are stored using `i64` data type.
///
/// # Example: Using `collect`
/// ```
/// # use arrow_array::{Array, Int64RunArray, Int64Array, StringArray};
/// # use std::sync::Arc;
///
/// let array: Int64RunArray = vec!["a", "a", "b", "c", "c"].into_iter().collect();
/// let values: Arc<dyn Array> = Arc::new(StringArray::from(vec!["a", "b", "c"]));
/// assert_eq!(array.run_ends(), &Int64Array::from(vec![2, 3, 5]));
/// assert_eq!(array.values(), &values);
/// ```
pub type Int64RunArray = RunArray<Int64Type>;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::builder::PrimitiveRunBuilder;
    use crate::types::{Int16Type, Int32Type, Int8Type, UInt32Type};
    use crate::{Array, Int16Array, Int32Array, StringArray};

    #[test]
    fn test_run_array() {
        // Construct a value array
        let value_data = PrimitiveArray::<Int8Type>::from_iter_values([
            10_i8, 11, 12, 13, 14, 15, 16, 17,
        ]);

        // Construct a run_ends array:
        let run_ends_data = PrimitiveArray::<Int16Type>::from_iter_values([
            4_i16, 6, 7, 9, 13, 18, 20, 22,
        ]);

        // Construct a run ends encoded array from the above two
        let ree_array =
            RunArray::<Int16Type>::try_new(&run_ends_data, &value_data).unwrap();

        assert_eq!(ree_array.len(), 22);
        assert_eq!(ree_array.null_count(), 0);

        let values = ree_array.values();
        assert_eq!(&value_data.into_data(), values.data());
        assert_eq!(&DataType::Int8, values.data_type());

        let run_ends = ree_array.run_ends();
        assert_eq!(&run_ends_data.into_data(), run_ends.data());
        assert_eq!(&DataType::Int16, run_ends.data_type());
    }

    #[test]
    fn test_run_array_fmt_debug() {
        let mut builder = PrimitiveRunBuilder::<Int16Type, UInt32Type>::with_capacity(3);
        builder.append_value(12345678);
        builder.append_null();
        builder.append_value(22345678);
        let array = builder.finish();
        assert_eq!(
            "RunArray {run_ends: PrimitiveArray<Int16>\n[\n  1,\n  2,\n  3,\n], values: PrimitiveArray<UInt32>\n[\n  12345678,\n  null,\n  22345678,\n]}\n",
            format!("{array:?}")
        );

        let mut builder = PrimitiveRunBuilder::<Int16Type, UInt32Type>::with_capacity(20);
        for _ in 0..20 {
            builder.append_value(1);
        }
        let array = builder.finish();

        assert_eq!(array.len(), 20);
        assert_eq!(array.null_count(), 0);

        assert_eq!(
            "RunArray {run_ends: PrimitiveArray<Int16>\n[\n  20,\n], values: PrimitiveArray<UInt32>\n[\n  1,\n]}\n",
            format!("{array:?}")
        );
    }

    #[test]
    fn test_run_array_from_iter() {
        let test = vec!["a", "a", "b", "c"];
        let array: RunArray<Int16Type> = test
            .iter()
            .map(|&x| if x == "b" { None } else { Some(x) })
            .collect();
        assert_eq!(
            "RunArray {run_ends: PrimitiveArray<Int16>\n[\n  2,\n  3,\n  4,\n], values: StringArray\n[\n  \"a\",\n  null,\n  \"c\",\n]}\n",
            format!("{array:?}")
        );

        assert_eq!(array.len(), 4);
        assert_eq!(array.null_count(), 0);

        let array: RunArray<Int16Type> = test.into_iter().collect();
        assert_eq!(
            "RunArray {run_ends: PrimitiveArray<Int16>\n[\n  2,\n  3,\n  4,\n], values: StringArray\n[\n  \"a\",\n  \"b\",\n  \"c\",\n]}\n",
            format!("{array:?}")
        );
    }

    #[test]
    fn test_run_array_run_ends_as_primitive_array() {
        let test = vec!["a", "b", "c", "a"];
        let array: RunArray<Int16Type> = test.into_iter().collect();

        assert_eq!(array.len(), 4);
        assert_eq!(array.null_count(), 0);

        let run_ends = array.run_ends();
        assert_eq!(&DataType::Int16, run_ends.data_type());
        assert_eq!(0, run_ends.null_count());
        assert_eq!(&[1, 2, 3, 4], run_ends.values());
    }

    #[test]
    fn test_run_array_as_primitive_array_with_null() {
        let test = vec![Some("a"), None, Some("b"), None, None, Some("a")];
        let array: RunArray<Int32Type> = test.into_iter().collect();

        assert_eq!(array.len(), 6);
        assert_eq!(array.null_count(), 0);

        let run_ends = array.run_ends();
        assert_eq!(&DataType::Int32, run_ends.data_type());
        assert_eq!(0, run_ends.null_count());
        assert_eq!(5, run_ends.len());
        assert_eq!(&[1, 2, 3, 5, 6], run_ends.values());

        let values_data = array.values();
        assert_eq!(2, values_data.null_count());
        assert_eq!(5, values_data.len());
    }

    #[test]
    fn test_run_array_all_nulls() {
        let test = vec![None, None, None];
        let array: RunArray<Int32Type> = test.into_iter().collect();

        assert_eq!(array.len(), 3);
        assert_eq!(array.null_count(), 0);

        let run_ends = array.run_ends();
        assert_eq!(1, run_ends.len());
        assert_eq!(&[3], run_ends.values());

        let values_data = array.values();
        assert_eq!(1, values_data.null_count());
    }

    #[test]
    fn test_run_array_try_new() {
        let values: StringArray = [Some("foo"), Some("bar"), None, Some("baz")]
            .into_iter()
            .collect();
        let run_ends: Int32Array =
            [Some(1), Some(2), Some(3), Some(4)].into_iter().collect();

        let array = RunArray::<Int32Type>::try_new(&run_ends, &values).unwrap();
        assert_eq!(array.run_ends().data_type(), &DataType::Int32);
        assert_eq!(array.values().data_type(), &DataType::Utf8);

        assert_eq!(array.null_count(), 0);
        assert_eq!(array.len(), 4);
        assert_eq!(array.run_ends.null_count(), 0);
        assert_eq!(array.values().null_count(), 1);

        assert_eq!(
            "RunArray {run_ends: PrimitiveArray<Int32>\n[\n  1,\n  2,\n  3,\n  4,\n], values: StringArray\n[\n  \"foo\",\n  \"bar\",\n  null,\n  \"baz\",\n]}\n",
            format!("{array:?}")
        );
    }

    #[test]
    fn test_run_array_int16_type_definition() {
        let array: Int16RunArray = vec!["a", "a", "b", "c", "c"].into_iter().collect();
        let values: Arc<dyn Array> = Arc::new(StringArray::from(vec!["a", "b", "c"]));
        assert_eq!(array.run_ends(), &Int16Array::from(vec![2, 3, 5]));
        assert_eq!(array.values(), &values);
    }

    #[test]
    fn test_run_array_empty_string() {
        let array: Int16RunArray = vec!["a", "a", "", "", "c"].into_iter().collect();
        let values: Arc<dyn Array> = Arc::new(StringArray::from(vec!["a", "", "c"]));
        assert_eq!(array.run_ends(), &Int16Array::from(vec![2, 4, 5]));
        assert_eq!(array.values(), &values);
    }

    #[test]
    fn test_run_array_length_mismatch() {
        let values: StringArray = [Some("foo"), Some("bar"), None, Some("baz")]
            .into_iter()
            .collect();
        let run_ends: Int32Array = [Some(1), Some(2), Some(3)].into_iter().collect();

        let actual = RunArray::<Int32Type>::try_new(&run_ends, &values);
        let expected = ArrowError::InvalidArgumentError("The run_ends array length should be the same as values array length. Run_ends array length is 3, values array length is 4".to_string());
        assert_eq!(expected.to_string(), actual.err().unwrap().to_string());
    }

    #[test]
    fn test_run_array_run_ends_with_null() {
        let values: StringArray = [Some("foo"), Some("bar"), Some("baz")]
            .into_iter()
            .collect();
        let run_ends: Int32Array = [Some(1), None, Some(3)].into_iter().collect();

        let actual = RunArray::<Int32Type>::try_new(&run_ends, &values);
        let expected = ArrowError::InvalidArgumentError("Found null values in run_ends array. The run_ends array should not have null values.".to_string());
        assert_eq!(expected.to_string(), actual.err().unwrap().to_string());
    }

    #[test]
    fn test_run_array_run_ends_with_zeroes() {
        let values: StringArray = [Some("foo"), Some("bar"), Some("baz")]
            .into_iter()
            .collect();
        let run_ends: Int32Array = [Some(0), Some(1), Some(3)].into_iter().collect();

        let actual = RunArray::<Int32Type>::try_new(&run_ends, &values);
        let expected = ArrowError::InvalidArgumentError("The values in run_ends array should be strictly positive. Found value 0 at index 0 that does not match the criteria.".to_string());
        assert_eq!(expected.to_string(), actual.err().unwrap().to_string());
    }

    #[test]
    fn test_run_array_run_ends_non_increasing() {
        let values: StringArray = [Some("foo"), Some("bar"), Some("baz")]
            .into_iter()
            .collect();
        let run_ends: Int32Array = [Some(1), Some(4), Some(4)].into_iter().collect();

        let actual = RunArray::<Int32Type>::try_new(&run_ends, &values);
        let expected = ArrowError::InvalidArgumentError("The values in run_ends array should be strictly increasing. Found value 4 at index 2 with previous value 4 that does not match the criteria.".to_string());
        assert_eq!(expected.to_string(), actual.err().unwrap().to_string());
    }

    #[test]
    #[should_panic(
        expected = "PrimitiveArray expected ArrayData with type Int64 got Int32"
    )]
    fn test_run_array_run_ends_data_type_mismatch() {
        let a = RunArray::<Int32Type>::from_iter(["32"]);
        let _ = RunArray::<Int64Type>::from(a.into_data());
    }
}
