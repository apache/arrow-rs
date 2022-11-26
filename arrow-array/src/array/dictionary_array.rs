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

use crate::builder::StringDictionaryBuilder;
use crate::iterator::ArrayIter;
use crate::types::*;
use crate::{
    make_array, Array, ArrayAccessor, ArrayRef, ArrowPrimitiveType, PrimitiveArray,
    StringArray,
};
use arrow_buffer::ArrowNativeType;
use arrow_data::ArrayData;
use arrow_schema::{ArrowError, DataType};
use std::any::Any;

///
/// A dictionary array where each element is a single value indexed by an integer key.
///
/// # Example: Using `collect`
/// ```
/// # use arrow_array::{Array, Int8DictionaryArray, Int8Array, StringArray};
/// # use std::sync::Arc;
///
/// let array: Int8DictionaryArray = vec!["a", "a", "b", "c"].into_iter().collect();
/// let values: Arc<dyn Array> = Arc::new(StringArray::from(vec!["a", "b", "c"]));
/// assert_eq!(array.keys(), &Int8Array::from(vec![0, 0, 1, 2]));
/// assert_eq!(array.values(), &values);
/// ```
pub type Int8DictionaryArray = DictionaryArray<Int8Type>;
///
/// A dictionary array where each element is a single value indexed by an integer key.
///
/// # Example: Using `collect`
/// ```
/// # use arrow_array::{Array, Int16DictionaryArray, Int16Array, StringArray};
/// # use std::sync::Arc;
///
/// let array: Int16DictionaryArray = vec!["a", "a", "b", "c"].into_iter().collect();
/// let values: Arc<dyn Array> = Arc::new(StringArray::from(vec!["a", "b", "c"]));
/// assert_eq!(array.keys(), &Int16Array::from(vec![0, 0, 1, 2]));
/// assert_eq!(array.values(), &values);
/// ```
pub type Int16DictionaryArray = DictionaryArray<Int16Type>;
///
/// A dictionary array where each element is a single value indexed by an integer key.
///
/// # Example: Using `collect`
/// ```
/// # use arrow_array::{Array, Int32DictionaryArray, Int32Array, StringArray};
/// # use std::sync::Arc;
///
/// let array: Int32DictionaryArray = vec!["a", "a", "b", "c"].into_iter().collect();
/// let values: Arc<dyn Array> = Arc::new(StringArray::from(vec!["a", "b", "c"]));
/// assert_eq!(array.keys(), &Int32Array::from(vec![0, 0, 1, 2]));
/// assert_eq!(array.values(), &values);
/// ```
pub type Int32DictionaryArray = DictionaryArray<Int32Type>;
///
/// A dictionary array where each element is a single value indexed by an integer key.
///
/// # Example: Using `collect`
/// ```
/// # use arrow_array::{Array, Int64DictionaryArray, Int64Array, StringArray};
/// # use std::sync::Arc;
///
/// let array: Int64DictionaryArray = vec!["a", "a", "b", "c"].into_iter().collect();
/// let values: Arc<dyn Array> = Arc::new(StringArray::from(vec!["a", "b", "c"]));
/// assert_eq!(array.keys(), &Int64Array::from(vec![0, 0, 1, 2]));
/// assert_eq!(array.values(), &values);
/// ```
pub type Int64DictionaryArray = DictionaryArray<Int64Type>;
///
/// A dictionary array where each element is a single value indexed by an integer key.
///
/// # Example: Using `collect`
/// ```
/// # use arrow_array::{Array, UInt8DictionaryArray, UInt8Array, StringArray};
/// # use std::sync::Arc;
///
/// let array: UInt8DictionaryArray = vec!["a", "a", "b", "c"].into_iter().collect();
/// let values: Arc<dyn Array> = Arc::new(StringArray::from(vec!["a", "b", "c"]));
/// assert_eq!(array.keys(), &UInt8Array::from(vec![0, 0, 1, 2]));
/// assert_eq!(array.values(), &values);
/// ```
pub type UInt8DictionaryArray = DictionaryArray<UInt8Type>;
///
/// A dictionary array where each element is a single value indexed by an integer key.
///
/// # Example: Using `collect`
/// ```
/// # use arrow_array::{Array, UInt16DictionaryArray, UInt16Array, StringArray};
/// # use std::sync::Arc;
///
/// let array: UInt16DictionaryArray = vec!["a", "a", "b", "c"].into_iter().collect();
/// let values: Arc<dyn Array> = Arc::new(StringArray::from(vec!["a", "b", "c"]));
/// assert_eq!(array.keys(), &UInt16Array::from(vec![0, 0, 1, 2]));
/// assert_eq!(array.values(), &values);
/// ```
pub type UInt16DictionaryArray = DictionaryArray<UInt16Type>;
///
/// A dictionary array where each element is a single value indexed by an integer key.
///
/// # Example: Using `collect`
/// ```
/// # use arrow_array::{Array, UInt32DictionaryArray, UInt32Array, StringArray};
/// # use std::sync::Arc;
///
/// let array: UInt32DictionaryArray = vec!["a", "a", "b", "c"].into_iter().collect();
/// let values: Arc<dyn Array> = Arc::new(StringArray::from(vec!["a", "b", "c"]));
/// assert_eq!(array.keys(), &UInt32Array::from(vec![0, 0, 1, 2]));
/// assert_eq!(array.values(), &values);
/// ```
pub type UInt32DictionaryArray = DictionaryArray<UInt32Type>;
///
/// A dictionary array where each element is a single value indexed by an integer key.
///
/// # Example: Using `collect`
/// ```
/// # use arrow_array::{Array, UInt64DictionaryArray, UInt64Array, StringArray};
/// # use std::sync::Arc;
///
/// let array: UInt64DictionaryArray = vec!["a", "a", "b", "c"].into_iter().collect();
/// let values: Arc<dyn Array> = Arc::new(StringArray::from(vec!["a", "b", "c"]));
/// assert_eq!(array.keys(), &UInt64Array::from(vec![0, 0, 1, 2]));
/// assert_eq!(array.values(), &values);
/// ```
pub type UInt64DictionaryArray = DictionaryArray<UInt64Type>;

/// A dictionary array where each element is a single value indexed by an integer key.
/// This is mostly used to represent strings or a limited set of primitive types as integers,
/// for example when doing NLP analysis or representing chromosomes by name.
///
/// [`DictionaryArray`] are represented using a `keys` array and a
/// `values` array, which may be different lengths. The `keys` array
/// stores indexes in the `values` array which holds
/// the corresponding logical value, as shown here:
///
/// ```text
/// ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
///   ┌─────────────────┐  ┌─────────┐ │     ┌─────────────────┐
/// │ │        A        │  │    0    │       │        A        │     values[keys[0]]
///   ├─────────────────┤  ├─────────┤ │     ├─────────────────┤
/// │ │        D        │  │    2    │       │        B        │     values[keys[1]]
///   ├─────────────────┤  ├─────────┤ │     ├─────────────────┤
/// │ │        B        │  │    2    │       │        B        │     values[keys[2]]
///   └─────────────────┘  ├─────────┤ │     ├─────────────────┤
/// │                      │    1    │       │        D        │     values[keys[3]]
///                        ├─────────┤ │     ├─────────────────┤
/// │                      │    1    │       │        D        │     values[keys[4]]
///                        ├─────────┤ │     ├─────────────────┤
/// │                      │    0    │       │        A        │     values[keys[5]]
///                        └─────────┘ │     └─────────────────┘
/// │       values            keys
///  ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
///                                             Logical array
///                                                Contents
///           DictionaryArray
///              length = 6
/// ```
///
/// Example **with nullable** data:
///
/// ```
/// use arrow_array::{DictionaryArray, Int8Array, types::Int8Type};
/// let test = vec!["a", "a", "b", "c"];
/// let array : DictionaryArray<Int8Type> = test.iter().map(|&x| if x == "b" {None} else {Some(x)}).collect();
/// assert_eq!(array.keys(), &Int8Array::from(vec![Some(0), Some(0), None, Some(1)]));
/// ```
///
/// Example **without nullable** data:
///
/// ```
/// use arrow_array::{DictionaryArray, Int8Array, types::Int8Type};
/// let test = vec!["a", "a", "b", "c"];
/// let array : DictionaryArray<Int8Type> = test.into_iter().collect();
/// assert_eq!(array.keys(), &Int8Array::from(vec![0, 0, 1, 2]));
/// ```
///
/// Example from existing arrays:
///
/// ```
/// use arrow_array::{DictionaryArray, Int8Array, StringArray, types::Int8Type};
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

impl<K: ArrowPrimitiveType> Clone for DictionaryArray<K> {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            keys: self.keys.clone(),
            values: self.values.clone(),
            is_ordered: self.is_ordered,
        }
    }
}

impl<K: ArrowPrimitiveType> DictionaryArray<K> {
    /// Attempt to create a new DictionaryArray with a specified keys
    /// (indexes into the dictionary) and values (dictionary)
    /// array. Returns an error if there are any keys that are outside
    /// of the dictionary array.
    pub fn try_new(
        keys: &PrimitiveArray<K>,
        values: &dyn Array,
    ) -> Result<Self, ArrowError> {
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
                    .null_bit_buffer(Some(buffer.clone()))
                    .null_count(keys.data().null_count());
            }
            _ => data = data.null_count(0),
        }

        // Safety: `validate` ensures key type is correct, and
        //  `validate_values` ensures all offsets are within range
        let array = unsafe { data.build_unchecked() };

        array.validate()?;
        array.validate_values()?;

        Ok(array.into())
    }

    /// Return an array view of the keys of this dictionary as a PrimitiveArray.
    pub fn keys(&self) -> &PrimitiveArray<K> {
        &self.keys
    }

    /// If `value` is present in `values` (aka the dictionary),
    /// returns the corresponding key (index into the `values`
    /// array). Otherwise returns `None`.
    ///
    /// Panics if `values` is not a [`StringArray`].
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

    /// Return an iterator over the keys (indexes into the dictionary)
    pub fn keys_iter(&self) -> impl Iterator<Item = Option<usize>> + '_ {
        self.keys.iter().map(|key| key.map(|k| k.as_usize()))
    }

    /// Return the value of `keys` (the dictionary key) at index `i`,
    /// cast to `usize`, `None` if the value at `i` is `NULL`.
    pub fn key(&self, i: usize) -> Option<usize> {
        self.keys.is_valid(i).then(|| self.keys.value(i).as_usize())
    }

    /// Downcast this dictionary to a [`TypedDictionaryArray`]
    ///
    /// ```
    /// use arrow_array::{Array, ArrayAccessor, DictionaryArray, StringArray, types::Int32Type};
    ///
    /// let orig = [Some("a"), Some("b"), None];
    /// let dictionary = DictionaryArray::<Int32Type>::from_iter(orig);
    /// let typed = dictionary.downcast_dict::<StringArray>().unwrap();
    /// assert_eq!(typed.value(0), "a");
    /// assert_eq!(typed.value(1), "b");
    /// assert!(typed.is_null(2));
    /// ```
    ///
    pub fn downcast_dict<V: 'static>(&self) -> Option<TypedDictionaryArray<'_, K, V>> {
        let values = self.values.as_any().downcast_ref()?;
        Some(TypedDictionaryArray {
            dictionary: self,
            values,
        })
    }

    /// Returns a new dictionary with the same keys as the current instance
    /// but with a different set of dictionary values
    ///
    /// This can be used to perform an operation on the values of a dictionary
    ///
    /// # Panics
    ///
    /// Panics if `values` has a length less than the current values
    ///
    /// ```
    /// use arrow_array::builder::PrimitiveDictionaryBuilder;
    /// use arrow_array::{Int8Array, Int64Array, ArrayAccessor};
    /// use arrow_array::types::{Int32Type, Int8Type};
    ///
    /// // Construct a Dict(Int32, Int8)
    /// let mut builder = PrimitiveDictionaryBuilder::<Int32Type, Int8Type>::with_capacity(2, 200);
    /// for i in 0..100 {
    ///     builder.append(i % 2).unwrap();
    /// }
    ///
    /// let dictionary = builder.finish();
    ///
    /// // Perform a widening cast of dictionary values
    /// let typed_dictionary = dictionary.downcast_dict::<Int8Array>().unwrap();
    /// let values: Int64Array = typed_dictionary.values().unary(|x| x as i64);
    ///
    /// // Create a Dict(Int32,
    /// let new = dictionary.with_values(&values);
    ///
    /// // Verify values are as expected
    /// let new_typed = new.downcast_dict::<Int64Array>().unwrap();
    /// for i in 0..100 {
    ///     assert_eq!(new_typed.value(i), (i % 2) as i64)
    /// }
    /// ```
    ///
    pub fn with_values(&self, values: &dyn Array) -> Self {
        assert!(values.len() >= self.values.len());

        let builder = self
            .data
            .clone()
            .into_builder()
            .data_type(DataType::Dictionary(
                Box::new(K::DATA_TYPE),
                Box::new(values.data_type().clone()),
            ))
            .child_data(vec![values.data().clone()]);

        // SAFETY:
        // Offsets were valid before and verified length is greater than or equal
        Self::from(unsafe { builder.build_unchecked() })
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
            assert_eq!(
                &T::DATA_TYPE,
                key_data_type.as_ref(),
                "DictionaryArray's data type must match, expected {} got {}",
                T::DATA_TYPE,
                key_data_type
            );

            // create a zero-copy of the keys' data
            // SAFETY:
            // ArrayData is valid and verified type above
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

impl<T: ArrowPrimitiveType> From<DictionaryArray<T>> for ArrayData {
    fn from(array: DictionaryArray<T>) -> Self {
        array.data
    }
}

/// Constructs a `DictionaryArray` from an iterator of optional strings.
///
/// # Example:
/// ```
/// use arrow_array::{DictionaryArray, PrimitiveArray, StringArray, types::Int8Type};
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
impl<'a, T: ArrowDictionaryKeyType> FromIterator<Option<&'a str>> for DictionaryArray<T> {
    fn from_iter<I: IntoIterator<Item = Option<&'a str>>>(iter: I) -> Self {
        let it = iter.into_iter();
        let (lower, _) = it.size_hint();
        let mut builder = StringDictionaryBuilder::with_capacity(lower, 256, 1024);
        it.for_each(|i| {
            if let Some(i) = i {
                // Note: impl ... for Result<DictionaryArray<T>> fails with
                // error[E0117]: only traits defined in the current crate can be implemented for arbitrary types
                builder
                    .append(i)
                    .expect("Unable to append a value to a dictionary array.");
            } else {
                builder.append_null();
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
/// use arrow_array::{DictionaryArray, PrimitiveArray, StringArray, types::Int8Type};
///
/// let test = vec!["a", "a", "b", "c"];
/// let array: DictionaryArray<Int8Type> = test.into_iter().collect();
/// assert_eq!(
///     "DictionaryArray {keys: PrimitiveArray<Int8>\n[\n  0,\n  0,\n  1,\n  2,\n] values: StringArray\n[\n  \"a\",\n  \"b\",\n  \"c\",\n]}\n",
///     format!("{:?}", array)
/// );
/// ```
impl<'a, T: ArrowDictionaryKeyType> FromIterator<&'a str> for DictionaryArray<T> {
    fn from_iter<I: IntoIterator<Item = &'a str>>(iter: I) -> Self {
        let it = iter.into_iter();
        let (lower, _) = it.size_hint();
        let mut builder = StringDictionaryBuilder::with_capacity(lower, 256, 1024);
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

    fn into_data(self) -> ArrayData {
        self.into()
    }
}

impl<T: ArrowPrimitiveType> std::fmt::Debug for DictionaryArray<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        writeln!(
            f,
            "DictionaryArray {{keys: {:?} values: {:?}}}",
            self.keys, self.values
        )
    }
}

/// A strongly-typed wrapper around a [`DictionaryArray`] that implements [`ArrayAccessor`]
/// allowing fast access to its elements
///
/// ```
/// use arrow_array::{DictionaryArray, StringArray, types::Int32Type};
///
/// let orig = ["a", "b", "a", "b"];
/// let dictionary = DictionaryArray::<Int32Type>::from_iter(orig);
///
/// // `TypedDictionaryArray` allows you to access the values directly
/// let typed = dictionary.downcast_dict::<StringArray>().unwrap();
///
/// for (maybe_val, orig) in typed.into_iter().zip(orig) {
///     assert_eq!(maybe_val.unwrap(), orig)
/// }
/// ```
pub struct TypedDictionaryArray<'a, K: ArrowPrimitiveType, V> {
    /// The dictionary array
    dictionary: &'a DictionaryArray<K>,
    /// The values of the dictionary
    values: &'a V,
}

// Manually implement `Clone` to avoid `V: Clone` type constraint
impl<'a, K: ArrowPrimitiveType, V> Clone for TypedDictionaryArray<'a, K, V> {
    fn clone(&self) -> Self {
        Self {
            dictionary: self.dictionary,
            values: self.values,
        }
    }
}

impl<'a, K: ArrowPrimitiveType, V> Copy for TypedDictionaryArray<'a, K, V> {}

impl<'a, K: ArrowPrimitiveType, V> std::fmt::Debug for TypedDictionaryArray<'a, K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        writeln!(f, "TypedDictionaryArray({:?})", self.dictionary)
    }
}

impl<'a, K: ArrowPrimitiveType, V> TypedDictionaryArray<'a, K, V> {
    /// Returns the keys of this [`TypedDictionaryArray`]
    pub fn keys(&self) -> &'a PrimitiveArray<K> {
        self.dictionary.keys()
    }

    /// Returns the values of this [`TypedDictionaryArray`]
    pub fn values(&self) -> &'a V {
        self.values
    }
}

impl<'a, K: ArrowPrimitiveType, V: Sync> Array for TypedDictionaryArray<'a, K, V> {
    fn as_any(&self) -> &dyn Any {
        self.dictionary
    }

    fn data(&self) -> &ArrayData {
        &self.dictionary.data
    }

    fn into_data(self) -> ArrayData {
        self.dictionary.into_data()
    }
}

impl<'a, K, V> IntoIterator for TypedDictionaryArray<'a, K, V>
where
    K: ArrowPrimitiveType,
    Self: ArrayAccessor,
{
    type Item = Option<<Self as ArrayAccessor>::Item>;
    type IntoIter = ArrayIter<Self>;

    fn into_iter(self) -> Self::IntoIter {
        ArrayIter::new(self)
    }
}

impl<'a, K, V> ArrayAccessor for TypedDictionaryArray<'a, K, V>
where
    K: ArrowPrimitiveType,
    V: Sync + Send,
    &'a V: ArrayAccessor,
    <&'a V as ArrayAccessor>::Item: Default,
{
    type Item = <&'a V as ArrayAccessor>::Item;

    fn value(&self, index: usize) -> Self::Item {
        assert!(
            index < self.len(),
            "Trying to access an element at index {} from a TypedDictionaryArray of length {}",
            index,
            self.len()
        );
        unsafe { self.value_unchecked(index) }
    }

    unsafe fn value_unchecked(&self, index: usize) -> Self::Item {
        let val = self.dictionary.keys.value_unchecked(index);
        let value_idx = val.as_usize();

        // As dictionary keys are only verified for non-null indexes
        // we must check the value is within bounds
        match value_idx < self.values.len() {
            true => self.values.value_unchecked(value_idx),
            false => Default::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::PrimitiveDictionaryBuilder;
    use crate::types::{
        Float32Type, Int16Type, Int32Type, Int8Type, UInt32Type, UInt8Type,
    };
    use crate::{Float32Array, Int16Array, Int32Array, Int8Array};
    use arrow_buffer::{Buffer, ToByteSlice};

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
        let mut builder =
            PrimitiveDictionaryBuilder::<UInt8Type, UInt32Type>::with_capacity(3, 2);
        builder.append(12345678).unwrap();
        builder.append_null();
        builder.append(22345678).unwrap();
        let array = builder.finish();
        assert_eq!(
            "DictionaryArray {keys: PrimitiveArray<UInt8>\n[\n  0,\n  null,\n  1,\n] values: PrimitiveArray<UInt32>\n[\n  12345678,\n  22345678,\n]}\n",
            format!("{:?}", array)
        );

        let mut builder =
            PrimitiveDictionaryBuilder::<UInt8Type, UInt32Type>::with_capacity(20, 2);
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
    fn test_dictionary_key() {
        let keys = Int8Array::from(vec![Some(2), None, Some(1)]);
        let values = StringArray::from(vec!["foo", "bar", "baz", "blarg"]);

        let array = DictionaryArray::try_new(&keys, &values).unwrap();
        assert_eq!(array.key(0), Some(2));
        assert_eq!(array.key(1), None);
        assert_eq!(array.key(2), Some(1));
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

    #[test]
    #[should_panic(
        expected = "DictionaryArray's data type must match, expected Int64 got Int32"
    )]
    fn test_from_array_data_validation() {
        let a = DictionaryArray::<Int32Type>::from_iter(["32"]);
        let _ = DictionaryArray::<Int64Type>::from(a.into_data());
    }
}
