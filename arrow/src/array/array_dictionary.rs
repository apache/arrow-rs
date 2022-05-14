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

use crate::compute::{sort_to_indices, take, TakeOptions};
use std::any::Any;
use std::fmt;
use std::iter::IntoIterator;
use std::{convert::From, iter::FromIterator};

use super::{
    make_array, Array, ArrayData, ArrayRef, PrimitiveArray, PrimitiveBuilder,
    StringArray, StringBuilder, StringDictionaryBuilder,
};
use crate::datatypes::{
    ArrowDictionaryKeyType, ArrowNativeType, ArrowPrimitiveType, DataType,
};
use crate::error::ArrowError;
use crate::error::Result;

/// A dictionary array where each element is a single value indexed by an integer key.
/// This is mostly used to represent strings or a limited set of primitive types as integers,
/// for example when doing NLP analysis or representing chromosomes by name.
///
/// Example **with nullable** data:
///
/// ```
/// use arrow::array::{DictionaryArray, Int8Array};
/// use arrow::datatypes::Int8Type;
/// let test = vec!["a", "a", "b", "c"];
/// let array : DictionaryArray<Int8Type> = test.iter().map(|&x| if x == "b" {None} else {Some(x)}).collect();
/// assert_eq!(array.keys(), &Int8Array::from(vec![Some(0), Some(0), None, Some(1)]));
/// ```
///
/// Example **without nullable** data:
///
/// ```
/// use arrow::array::{DictionaryArray, Int8Array};
/// use arrow::datatypes::Int8Type;
/// let test = vec!["a", "a", "b", "c"];
/// let array : DictionaryArray<Int8Type> = test.into_iter().collect();
/// assert_eq!(array.keys(), &Int8Array::from(vec![0, 0, 1, 2]));
/// ```
///
/// Example from existing arrays:
///
/// ```
/// use arrow::array::{DictionaryArray, Int8Array, StringArray};
/// use arrow::datatypes::Int8Type;
/// // You can form your own DictionaryArray by providing the
/// // values (dictionary) and keys (indexes into the dictionary):
/// let values = StringArray::from_iter_values(["a", "b", "c"]);
/// let keys = Int8Array::from_iter_values([0, 0, 1, 2]);
/// let array = DictionaryArray::<Int8Type>::try_new(&keys, &values).unwrap();
/// let expected: DictionaryArray::<Int8Type> = vec!["a", "a", "b", "c"]
///    .into_iter()
///    .collect();
/// assert_eq!(&array, &expected);
/// ```
pub struct DictionaryArray<K: ArrowPrimitiveType> {
    /// Data of this dictionary. Note that this is _not_ compatible with the C Data interface,
    /// as, in the current implementation, `values` below are the first child of this struct.
    data: ArrayData,

    /// The keys of this dictionary. These are constructed from the
    /// buffer and null bitmap of `data`.  Also, note that these do
    /// not correspond to the true values of this array. Rather, they
    /// map to the real values.
    keys: PrimitiveArray<K>,

    /// Array of dictionary values (can by any DataType).
    values: ArrayRef,

    /// Values are ordered.
    is_ordered: bool,
}

impl<'a, K: ArrowPrimitiveType> DictionaryArray<K> {
    /// Attempt to create a new DictionaryArray with a specified keys
    /// (indexes into the dictionary) and values (dictionary)
    /// array. Returns an error if there are any keys that are outside
    /// of the dictionary array.
    pub fn try_new(keys: &PrimitiveArray<K>, values: &dyn Array) -> Result<Self> {
        let dict_data_type = DataType::Dictionary(
            Box::new(keys.data_type().clone()),
            Box::new(values.data_type().clone()),
        );

        // Note: This use the ArrayDataBuilder::build_unchecked and afterwards
        // call the new function which only validates that the keys are in bounds.
        let mut data = ArrayData::builder(dict_data_type)
            .len(keys.len())
            .add_buffer(keys.data().buffers()[0].clone())
            .add_child_data(values.data().clone());

        match keys.data().null_buffer() {
            Some(buffer) if keys.data().null_count() > 0 => {
                data = data
                    .null_bit_buffer(buffer.clone())
                    .null_count(keys.data().null_count());
            }
            _ => data = data.null_count(0),
        }

        // Safety: `validate` ensures key type is correct, and
        //  `validate_dictionary_offset` ensures all offsets are within range
        let array = unsafe { data.build_unchecked() };

        array.validate()?;
        array.validate_dictionary_offset()?;

        Ok(array.into())
    }

    /// Return an array view of the keys of this dictionary as a PrimitiveArray.
    pub fn keys(&self) -> &PrimitiveArray<K> {
        &self.keys
    }

    /// Returns the lookup key by doing reverse dictionary lookup
    pub fn lookup_key(&self, value: &str) -> Option<K::Native> {
        let rd_buf: &StringArray =
            self.values.as_any().downcast_ref::<StringArray>().unwrap();

        (0..rd_buf.len())
            .position(|i| rd_buf.value(i) == value)
            .and_then(K::Native::from_usize)
    }

    /// Returns a reference to the dictionary values array
    pub fn values(&self) -> &ArrayRef {
        &self.values
    }

    /// Returns a clone of the value type of this list.
    pub fn value_type(&self) -> DataType {
        self.values.data_ref().data_type().clone()
    }

    /// The length of the dictionary is the length of the keys array.
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Whether this dictionary is empty
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Currently exists for compatibility purposes with Arrow IPC.
    pub fn is_ordered(&self) -> bool {
        self.is_ordered
    }

    /// Returns a DictionaryArray referencing the same data
    /// with the [DictionaryArray::is_ordered] flag set to `true`.
    /// Note that this does not actually reorder the values in the dictionary.
    pub fn as_ordered(&self) -> Self {
        Self {
            data: self.data.clone(),
            values: self.values.clone(),
            keys: PrimitiveArray::<K>::from(self.keys.data().clone()),
            is_ordered: true,
        }
    }

    pub fn make_ordered(&self) -> Result<Self> {
        if self.is_ordered {
            Ok(self.as_ordered())
        } else {
            // validate up front that we can do all of the conversions needed below
            u32::try_from(self.values.len())
                .and_then(usize::try_from)
                .ok()
                .and_then(K::Native::from_usize)
                .ok_or(ArrowError::DictionaryKeyOverflowError)?;

            let sort_indices = sort_to_indices(self.values(), None, None)?;
            let sorted_dictionary = take(
                self.values().as_ref(),
                &sort_indices,
                Some(TakeOptions {
                    check_bounds: false,
                }),
            )?;
            let sort_indices = sort_indices.values();

            let mut lookup = vec![0; sort_indices.len()];
            sort_indices.into_iter().enumerate().for_each(|(i, idx)| {
                lookup[*idx as usize] = i;
            });

            let new_indices = &self
                .keys
                .iter()
                .map(|opt_key| {
                    if let Some(key) = opt_key {
                        let key_usize = key.to_usize().unwrap();
                        // Safety:
                        // lookup has the same length as the dictionary values
                        // so if the keys were valid for values they will be valid indices into lookup
                        let new_key = unsafe { *lookup.get_unchecked(key_usize) };
                        Some(K::Native::from_usize(new_key).unwrap())
                    } else {
                        None
                    }
                })
                .collect::<PrimitiveArray<K>>();

            // Safety:
            // after remapping the keys will be in the same range as before
            let new_data = unsafe {
                ArrayData::new_unchecked(
                    self.data_type().clone(),
                    new_indices.len(),
                    Some(new_indices.null_count()),
                    new_indices.data().null_buffer().cloned(),
                    0,
                    new_indices.data().buffers().to_vec(),
                    vec![sorted_dictionary.data().clone()],
                )
            };

            Ok(DictionaryArray::from(new_data).as_ordered())
        }
    }

    /// Return an iterator over the keys (indexes into the dictionary)
    pub fn keys_iter(&self) -> impl Iterator<Item = Option<usize>> + '_ {
        self.keys
            .iter()
            .map(|key| key.map(|k| k.to_usize().expect("Dictionary index not usize")))
    }
}

/// Constructs a `DictionaryArray` from an array data reference.
impl<T: ArrowPrimitiveType> From<ArrayData> for DictionaryArray<T> {
    fn from(data: ArrayData) -> Self {
        assert_eq!(
            data.buffers().len(),
            1,
            "DictionaryArray data should contain a single buffer only (keys)."
        );
        assert_eq!(
            data.child_data().len(),
            1,
            "DictionaryArray should contain a single child array (values)."
        );

        if let DataType::Dictionary(key_data_type, _) = data.data_type() {
            if key_data_type.as_ref() != &T::DATA_TYPE {
                panic!("DictionaryArray's data type must match.")
            };
            // create a zero-copy of the keys' data
            let keys = PrimitiveArray::<T>::from(unsafe {
                ArrayData::new_unchecked(
                    T::DATA_TYPE,
                    data.len(),
                    Some(data.null_count()),
                    data.null_buffer().cloned(),
                    data.offset(),
                    data.buffers().to_vec(),
                    vec![],
                )
            });
            let values = make_array(data.child_data()[0].clone());
            Self {
                data,
                keys,
                values,
                is_ordered: false,
            }
        } else {
            panic!("DictionaryArray must have Dictionary data type.")
        }
    }
}

/// Constructs a `DictionaryArray` from an iterator of optional strings.
///
/// # Example:
/// ```
/// use arrow::array::{DictionaryArray, PrimitiveArray, StringArray};
/// use arrow::datatypes::Int8Type;
///
/// let test = vec!["a", "a", "b", "c"];
/// let array: DictionaryArray<Int8Type> = test
///     .iter()
///     .map(|&x| if x == "b" { None } else { Some(x) })
///     .collect();
/// assert_eq!(
///     "DictionaryArray {keys: PrimitiveArray<Int8>\n[\n  0,\n  0,\n  null,\n  1,\n] values: StringArray\n[\n  \"a\",\n  \"c\",\n]}\n",
///     format!("{:?}", array)
/// );
/// ```
impl<'a, T: ArrowPrimitiveType + ArrowDictionaryKeyType> FromIterator<Option<&'a str>>
    for DictionaryArray<T>
{
    fn from_iter<I: IntoIterator<Item = Option<&'a str>>>(iter: I) -> Self {
        let it = iter.into_iter();
        let (lower, _) = it.size_hint();
        let key_builder = PrimitiveBuilder::<T>::new(lower);
        let value_builder = StringBuilder::new(256);
        let mut builder = StringDictionaryBuilder::new(key_builder, value_builder);
        it.for_each(|i| {
            if let Some(i) = i {
                // Note: impl ... for Result<DictionaryArray<T>> fails with
                // error[E0117]: only traits defined in the current crate can be implemented for arbitrary types
                builder
                    .append(i)
                    .expect("Unable to append a value to a dictionary array.");
            } else {
                builder
                    .append_null()
                    .expect("Unable to append a null value to a dictionary array.");
            }
        });

        builder.finish()
    }
}

/// Constructs a `DictionaryArray` from an iterator of strings.
///
/// # Example:
///
/// ```
/// use arrow::array::{DictionaryArray, PrimitiveArray, StringArray};
/// use arrow::datatypes::Int8Type;
///
/// let test = vec!["a", "a", "b", "c"];
/// let array: DictionaryArray<Int8Type> = test.into_iter().collect();
/// assert_eq!(
///     "DictionaryArray {keys: PrimitiveArray<Int8>\n[\n  0,\n  0,\n  1,\n  2,\n] values: StringArray\n[\n  \"a\",\n  \"b\",\n  \"c\",\n]}\n",
///     format!("{:?}", array)
/// );
/// ```
impl<'a, T: ArrowPrimitiveType + ArrowDictionaryKeyType> FromIterator<&'a str>
    for DictionaryArray<T>
{
    fn from_iter<I: IntoIterator<Item = &'a str>>(iter: I) -> Self {
        let it = iter.into_iter();
        let (lower, _) = it.size_hint();
        let key_builder = PrimitiveBuilder::<T>::new(lower);
        let value_builder = StringBuilder::new(256);
        let mut builder = StringDictionaryBuilder::new(key_builder, value_builder);
        it.for_each(|i| {
            builder
                .append(i)
                .expect("Unable to append a value to a dictionary array.");
        });

        builder.finish()
    }
}

impl<T: ArrowPrimitiveType> Array for DictionaryArray<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data(&self) -> &ArrayData {
        &self.data
    }
}

impl<T: ArrowPrimitiveType> fmt::Debug for DictionaryArray<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "DictionaryArray {{keys: {:?} values: {:?}}}",
            self.keys, self.values
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::array::{Float32Array, Int8Array};
    use crate::datatypes::{Float32Type, Int16Type};
    use crate::{
        array::Int16DictionaryArray, array::PrimitiveDictionaryBuilder,
        datatypes::DataType,
    };
    use crate::{
        array::{Int16Array, Int32Array},
        datatypes::{Int32Type, Int8Type, UInt32Type, UInt8Type},
    };
    use crate::{buffer::Buffer, datatypes::ToByteSlice};

    #[test]
    fn test_dictionary_array() {
        // Construct a value array
        let value_data = ArrayData::builder(DataType::Int8)
            .len(8)
            .add_buffer(Buffer::from(
                &[10_i8, 11, 12, 13, 14, 15, 16, 17].to_byte_slice(),
            ))
            .build()
            .unwrap();

        // Construct a buffer for value offsets, for the nested array:
        let keys = Buffer::from(&[2_i16, 3, 4].to_byte_slice());

        // Construct a dictionary array from the above two
        let key_type = DataType::Int16;
        let value_type = DataType::Int8;
        let dict_data_type =
            DataType::Dictionary(Box::new(key_type), Box::new(value_type));
        let dict_data = ArrayData::builder(dict_data_type.clone())
            .len(3)
            .add_buffer(keys.clone())
            .add_child_data(value_data.clone())
            .build()
            .unwrap();
        let dict_array = Int16DictionaryArray::from(dict_data);

        let values = dict_array.values();
        assert_eq!(&value_data, values.data());
        assert_eq!(DataType::Int8, dict_array.value_type());
        assert_eq!(3, dict_array.len());

        // Null count only makes sense in terms of the component arrays.
        assert_eq!(0, dict_array.null_count());
        assert_eq!(0, dict_array.values().null_count());
        assert_eq!(dict_array.keys(), &Int16Array::from(vec![2_i16, 3, 4]));

        // Now test with a non-zero offset
        let dict_data = ArrayData::builder(dict_data_type)
            .len(2)
            .offset(1)
            .add_buffer(keys)
            .add_child_data(value_data.clone())
            .build()
            .unwrap();
        let dict_array = Int16DictionaryArray::from(dict_data);

        let values = dict_array.values();
        assert_eq!(&value_data, values.data());
        assert_eq!(DataType::Int8, dict_array.value_type());
        assert_eq!(2, dict_array.len());
        assert_eq!(dict_array.keys(), &Int16Array::from(vec![3_i16, 4]));
    }

    #[test]
    fn test_dictionary_array_fmt_debug() {
        let key_builder = PrimitiveBuilder::<UInt8Type>::new(3);
        let value_builder = PrimitiveBuilder::<UInt32Type>::new(2);
        let mut builder = PrimitiveDictionaryBuilder::new(key_builder, value_builder);
        builder.append(12345678).unwrap();
        builder.append_null().unwrap();
        builder.append(22345678).unwrap();
        let array = builder.finish();
        assert_eq!(
            "DictionaryArray {keys: PrimitiveArray<UInt8>\n[\n  0,\n  null,\n  1,\n] values: PrimitiveArray<UInt32>\n[\n  12345678,\n  22345678,\n]}\n",
            format!("{:?}", array)
        );

        let key_builder = PrimitiveBuilder::<UInt8Type>::new(20);
        let value_builder = PrimitiveBuilder::<UInt32Type>::new(2);
        let mut builder = PrimitiveDictionaryBuilder::new(key_builder, value_builder);
        for _ in 0..20 {
            builder.append(1).unwrap();
        }
        let array = builder.finish();
        assert_eq!(
            "DictionaryArray {keys: PrimitiveArray<UInt8>\n[\n  0,\n  0,\n  0,\n  0,\n  0,\n  0,\n  0,\n  0,\n  0,\n  0,\n  0,\n  0,\n  0,\n  0,\n  0,\n  0,\n  0,\n  0,\n  0,\n  0,\n] values: PrimitiveArray<UInt32>\n[\n  1,\n]}\n",
            format!("{:?}", array)
        );
    }

    #[test]
    fn test_dictionary_array_from_iter() {
        let test = vec!["a", "a", "b", "c"];
        let array: DictionaryArray<Int8Type> = test
            .iter()
            .map(|&x| if x == "b" { None } else { Some(x) })
            .collect();
        assert_eq!(
            "DictionaryArray {keys: PrimitiveArray<Int8>\n[\n  0,\n  0,\n  null,\n  1,\n] values: StringArray\n[\n  \"a\",\n  \"c\",\n]}\n",
            format!("{:?}", array)
        );

        let array: DictionaryArray<Int8Type> = test.into_iter().collect();
        assert_eq!(
            "DictionaryArray {keys: PrimitiveArray<Int8>\n[\n  0,\n  0,\n  1,\n  2,\n] values: StringArray\n[\n  \"a\",\n  \"b\",\n  \"c\",\n]}\n",
            format!("{:?}", array)
        );
    }

    #[test]
    fn test_dictionary_array_reverse_lookup_key() {
        let test = vec!["a", "a", "b", "c"];
        let array: DictionaryArray<Int8Type> = test.into_iter().collect();

        assert_eq!(array.lookup_key("c"), Some(2));

        // Direction of building a dictionary is the iterator direction
        let test = vec!["t3", "t3", "t2", "t2", "t1", "t3", "t4", "t1", "t0"];
        let array: DictionaryArray<Int8Type> = test.into_iter().collect();

        assert_eq!(array.lookup_key("t1"), Some(2));
        assert_eq!(array.lookup_key("non-existent"), None);
    }

    #[test]
    fn test_dictionary_keys_as_primitive_array() {
        let test = vec!["a", "b", "c", "a"];
        let array: DictionaryArray<Int8Type> = test.into_iter().collect();

        let keys = array.keys();
        assert_eq!(&DataType::Int8, keys.data_type());
        assert_eq!(0, keys.null_count());
        assert_eq!(&[0, 1, 2, 0], keys.values());
    }

    #[test]
    fn test_dictionary_keys_as_primitive_array_with_null() {
        let test = vec![Some("a"), None, Some("b"), None, None, Some("a")];
        let array: DictionaryArray<Int32Type> = test.into_iter().collect();

        let keys = array.keys();
        assert_eq!(&DataType::Int32, keys.data_type());
        assert_eq!(3, keys.null_count());

        assert!(keys.is_valid(0));
        assert!(!keys.is_valid(1));
        assert!(keys.is_valid(2));
        assert!(!keys.is_valid(3));
        assert!(!keys.is_valid(4));
        assert!(keys.is_valid(5));

        assert_eq!(0, keys.value(0));
        assert_eq!(1, keys.value(2));
        assert_eq!(0, keys.value(5));
    }

    #[test]
    fn test_dictionary_all_nulls() {
        let test = vec![None, None, None];
        let array: DictionaryArray<Int32Type> = test.into_iter().collect();
        array
            .data()
            .validate_full()
            .expect("All null array has valid array data");
    }

    #[test]
    fn test_dictionary_make_ordered() {
        let test = vec![
            Some("b"),
            Some("b"),
            None,
            Some("d"),
            Some("d"),
            Some("c"),
            Some("a"),
        ];
        let array: DictionaryArray<Int32Type> = test.into_iter().collect();

        let ordered = array.make_ordered().unwrap();
        let actual_keys = ordered.keys.iter().collect::<Vec<_>>();

        let expected_keys =
            vec![Some(1), Some(1), None, Some(3), Some(3), Some(2), Some(0)];
        assert_eq!(&expected_keys, &actual_keys);

        let expected_values = StringArray::from(vec!["a", "b", "c", "d"]);
        let actual_values = ordered
            .values
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(&expected_values, actual_values);
    }

    #[test]
    fn test_dictionary_iter() {
        // Construct a value array
        let values = Int8Array::from_iter_values([10_i8, 11, 12, 13, 14, 15, 16, 17]);
        let keys = Int16Array::from_iter_values([2_i16, 3, 4]);

        // Construct a dictionary array from the above two
        let dict_array = DictionaryArray::<Int16Type>::try_new(&keys, &values).unwrap();

        let mut key_iter = dict_array.keys_iter();
        assert_eq!(2, key_iter.next().unwrap().unwrap());
        assert_eq!(3, key_iter.next().unwrap().unwrap());
        assert_eq!(4, key_iter.next().unwrap().unwrap());
        assert!(key_iter.next().is_none());

        let mut iter = dict_array
            .values()
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap()
            .take_iter(dict_array.keys_iter());

        assert_eq!(12, iter.next().unwrap().unwrap());
        assert_eq!(13, iter.next().unwrap().unwrap());
        assert_eq!(14, iter.next().unwrap().unwrap());
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_dictionary_iter_with_null() {
        let test = vec![Some("a"), None, Some("b"), None, None, Some("a")];
        let array: DictionaryArray<Int32Type> = test.into_iter().collect();

        let mut iter = array
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .take_iter(array.keys_iter());

        assert_eq!("a", iter.next().unwrap().unwrap());
        assert!(iter.next().unwrap().is_none());
        assert_eq!("b", iter.next().unwrap().unwrap());
        assert!(iter.next().unwrap().is_none());
        assert!(iter.next().unwrap().is_none());
        assert_eq!("a", iter.next().unwrap().unwrap());
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_try_new() {
        let values: StringArray = [Some("foo"), Some("bar"), Some("baz")]
            .into_iter()
            .collect();
        let keys: Int32Array = [Some(0), Some(2), None, Some(1)].into_iter().collect();

        let array = DictionaryArray::<Int32Type>::try_new(&keys, &values).unwrap();
        assert_eq!(array.keys().data_type(), &DataType::Int32);
        assert_eq!(array.values().data_type(), &DataType::Utf8);

        assert_eq!(array.data().null_count(), 1);

        assert!(array.keys().is_valid(0));
        assert!(array.keys().is_valid(1));
        assert!(array.keys().is_null(2));
        assert!(array.keys().is_valid(3));

        assert_eq!(array.keys().value(0), 0);
        assert_eq!(array.keys().value(1), 2);
        assert_eq!(array.keys().value(3), 1);

        assert_eq!(
            "DictionaryArray {keys: PrimitiveArray<Int32>\n[\n  0,\n  2,\n  null,\n  1,\n] values: StringArray\n[\n  \"foo\",\n  \"bar\",\n  \"baz\",\n]}\n",
            format!("{:?}", array)
        );
    }

    #[test]
    #[should_panic(
        expected = "Value at position 1 out of bounds: 3 (should be in [0, 1])"
    )]
    fn test_try_new_index_too_large() {
        let values: StringArray = [Some("foo"), Some("bar")].into_iter().collect();
        // dictionary only has 2 values, so offset 3 is out of bounds
        let keys: Int32Array = [Some(0), Some(3)].into_iter().collect();
        DictionaryArray::<Int32Type>::try_new(&keys, &values).unwrap();
    }

    #[test]
    #[should_panic(
        expected = "Value at position 0 out of bounds: -100 (should be in [0, 1])"
    )]
    fn test_try_new_index_too_small() {
        let values: StringArray = [Some("foo"), Some("bar")].into_iter().collect();
        let keys: Int32Array = [Some(-100)].into_iter().collect();
        DictionaryArray::<Int32Type>::try_new(&keys, &values).unwrap();
    }

    #[test]
    #[should_panic(expected = "Dictionary key type must be integer, but was Float32")]
    fn test_try_wrong_dictionary_key_type() {
        let values: StringArray = [Some("foo"), Some("bar")].into_iter().collect();
        let keys: Float32Array = [Some(0_f32), None, Some(3_f32)].into_iter().collect();
        DictionaryArray::<Float32Type>::try_new(&keys, &values).unwrap();
    }
}
