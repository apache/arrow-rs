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

use crate::builder::{PrimitiveDictionaryBuilder, StringDictionaryBuilder};
use crate::cast::AsArray;
use crate::iterator::ArrayIter;
use crate::types::*;
use crate::{
    make_array, Array, ArrayAccessor, ArrayRef, ArrowNativeTypeOp, PrimitiveArray, Scalar,
    StringArray,
};
use arrow_buffer::bit_util::set_bit;
use arrow_buffer::buffer::NullBuffer;
use arrow_buffer::{ArrowNativeType, BooleanBuffer, BooleanBufferBuilder};
use arrow_data::ArrayData;
use arrow_schema::{ArrowError, DataType};
use std::any::Any;
use std::sync::Arc;

/// A [`DictionaryArray`] indexed by `i8`
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
///
/// See [`DictionaryArray`] for more information and examples
pub type Int8DictionaryArray = DictionaryArray<Int8Type>;

/// A [`DictionaryArray`] indexed by `i16`
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
///
/// See [`DictionaryArray`] for more information and examples
pub type Int16DictionaryArray = DictionaryArray<Int16Type>;

/// A [`DictionaryArray`] indexed by `i32`
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
///
/// See [`DictionaryArray`] for more information and examples
pub type Int32DictionaryArray = DictionaryArray<Int32Type>;

/// A [`DictionaryArray`] indexed by `i64`
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
///
/// See [`DictionaryArray`] for more information and examples
pub type Int64DictionaryArray = DictionaryArray<Int64Type>;

/// A [`DictionaryArray`] indexed by `u8`
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
///
/// See [`DictionaryArray`] for more information and examples
pub type UInt8DictionaryArray = DictionaryArray<UInt8Type>;

/// A [`DictionaryArray`] indexed by `u16`
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
///
/// See [`DictionaryArray`] for more information and examples
pub type UInt16DictionaryArray = DictionaryArray<UInt16Type>;

/// A [`DictionaryArray`] indexed by `u32`
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
///
/// See [`DictionaryArray`] for more information and examples
pub type UInt32DictionaryArray = DictionaryArray<UInt32Type>;

/// A [`DictionaryArray`] indexed by `u64`
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
///
/// See [`DictionaryArray`] for more information and examples
pub type UInt64DictionaryArray = DictionaryArray<UInt64Type>;

/// An array of [dictionary encoded values](https://arrow.apache.org/docs/format/Columnar.html#dictionary-encoded-layout)
///
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
/// # Example: From Nullable Data
///
/// ```
/// # use arrow_array::{DictionaryArray, Int8Array, types::Int8Type};
/// let test = vec!["a", "a", "b", "c"];
/// let array : DictionaryArray<Int8Type> = test.iter().map(|&x| if x == "b" {None} else {Some(x)}).collect();
/// assert_eq!(array.keys(), &Int8Array::from(vec![Some(0), Some(0), None, Some(1)]));
/// ```
///
/// # Example: From Non-Nullable Data
///
/// ```
/// # use arrow_array::{DictionaryArray, Int8Array, types::Int8Type};
/// let test = vec!["a", "a", "b", "c"];
/// let array : DictionaryArray<Int8Type> = test.into_iter().collect();
/// assert_eq!(array.keys(), &Int8Array::from(vec![0, 0, 1, 2]));
/// ```
///
/// # Example: From Existing Arrays
///
/// ```
/// # use std::sync::Arc;
/// # use arrow_array::{DictionaryArray, Int8Array, StringArray, types::Int8Type};
/// // You can form your own DictionaryArray by providing the
/// // values (dictionary) and keys (indexes into the dictionary):
/// let values = StringArray::from_iter_values(["a", "b", "c"]);
/// let keys = Int8Array::from_iter_values([0, 0, 1, 2]);
/// let array = DictionaryArray::<Int8Type>::try_new(keys, Arc::new(values)).unwrap();
/// let expected: DictionaryArray::<Int8Type> = vec!["a", "a", "b", "c"].into_iter().collect();
/// assert_eq!(&array, &expected);
/// ```
///
/// # Example: Using Builder
///
/// ```
/// # use arrow_array::{Array, StringArray};
/// # use arrow_array::builder::StringDictionaryBuilder;
/// # use arrow_array::types::Int32Type;
/// let mut builder = StringDictionaryBuilder::<Int32Type>::new();
/// builder.append_value("a");
/// builder.append_null();
/// builder.append_value("a");
/// builder.append_value("b");
/// let array = builder.finish();
///
/// let values: Vec<_> = array.downcast_dict::<StringArray>().unwrap().into_iter().collect();
/// assert_eq!(&values, &[Some("a"), None, Some("a"), Some("b")]);
/// ```
pub struct DictionaryArray<K: ArrowDictionaryKeyType> {
    data_type: DataType,

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

impl<K: ArrowDictionaryKeyType> Clone for DictionaryArray<K> {
    fn clone(&self) -> Self {
        Self {
            data_type: self.data_type.clone(),
            keys: self.keys.clone(),
            values: self.values.clone(),
            is_ordered: self.is_ordered,
        }
    }
}

impl<K: ArrowDictionaryKeyType> DictionaryArray<K> {
    /// Attempt to create a new DictionaryArray with a specified keys
    /// (indexes into the dictionary) and values (dictionary)
    /// array.
    ///
    /// # Panics
    ///
    /// Panics if [`Self::try_new`] returns an error
    pub fn new(keys: PrimitiveArray<K>, values: ArrayRef) -> Self {
        Self::try_new(keys, values).unwrap()
    }

    /// Attempt to create a new DictionaryArray with a specified keys
    /// (indexes into the dictionary) and values (dictionary)
    /// array.
    ///
    /// # Errors
    ///
    /// Returns an error if any `keys[i] >= values.len() || keys[i] < 0`
    pub fn try_new(keys: PrimitiveArray<K>, values: ArrayRef) -> Result<Self, ArrowError> {
        let data_type = DataType::Dictionary(
            Box::new(keys.data_type().clone()),
            Box::new(values.data_type().clone()),
        );

        let zero = K::Native::usize_as(0);
        let values_len = values.len();

        if let Some((idx, v)) =
            keys.values().iter().enumerate().find(|(idx, v)| {
                (v.is_lt(zero) || v.as_usize() >= values_len) && keys.is_valid(*idx)
            })
        {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Invalid dictionary key {v:?} at index {idx}, expected 0 <= key < {values_len}",
            )));
        }

        Ok(Self {
            data_type,
            keys,
            values,
            is_ordered: false,
        })
    }

    /// Create a new [`Scalar`] from `value`
    pub fn new_scalar<T: Array + 'static>(value: Scalar<T>) -> Scalar<Self> {
        Scalar::new(Self::new(
            PrimitiveArray::new(vec![K::Native::usize_as(0)].into(), None),
            Arc::new(value.into_inner()),
        ))
    }

    /// Create a new [`DictionaryArray`] without performing validation
    ///
    /// # Safety
    ///
    /// Safe provided [`Self::try_new`] would not return an error
    pub unsafe fn new_unchecked(keys: PrimitiveArray<K>, values: ArrayRef) -> Self {
        let data_type = DataType::Dictionary(
            Box::new(keys.data_type().clone()),
            Box::new(values.data_type().clone()),
        );

        Self {
            data_type,
            keys,
            values,
            is_ordered: false,
        }
    }

    /// Deconstruct this array into its constituent parts
    pub fn into_parts(self) -> (PrimitiveArray<K>, ArrayRef) {
        (self.keys, self.values)
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
        let rd_buf: &StringArray = self.values.as_any().downcast_ref::<StringArray>().unwrap();

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
        self.values.data_type().clone()
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

    /// Returns a zero-copy slice of this array with the indicated offset and length.
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        Self {
            data_type: self.data_type.clone(),
            keys: self.keys.slice(offset, length),
            values: self.values.clone(),
            is_ordered: self.is_ordered,
        }
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
    /// # use std::sync::Arc;
    /// # use arrow_array::builder::PrimitiveDictionaryBuilder;
    /// # use arrow_array::{Int8Array, Int64Array, ArrayAccessor};
    /// # use arrow_array::types::{Int32Type, Int8Type};
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
    /// let new = dictionary.with_values(Arc::new(values));
    ///
    /// // Verify values are as expected
    /// let new_typed = new.downcast_dict::<Int64Array>().unwrap();
    /// for i in 0..100 {
    ///     assert_eq!(new_typed.value(i), (i % 2) as i64)
    /// }
    /// ```
    ///
    pub fn with_values(&self, values: ArrayRef) -> Self {
        assert!(values.len() >= self.values.len());
        let data_type =
            DataType::Dictionary(Box::new(K::DATA_TYPE), Box::new(values.data_type().clone()));
        Self {
            data_type,
            keys: self.keys.clone(),
            values,
            is_ordered: false,
        }
    }

    /// Returns `PrimitiveDictionaryBuilder` of this dictionary array for mutating
    /// its keys and values if the underlying data buffer is not shared by others.
    pub fn into_primitive_dict_builder<V>(self) -> Result<PrimitiveDictionaryBuilder<K, V>, Self>
    where
        V: ArrowPrimitiveType,
    {
        if !self.value_type().is_primitive() {
            return Err(self);
        }

        let key_array = self.keys().clone();
        let value_array = self.values().as_primitive::<V>().clone();

        drop(self.keys);
        drop(self.values);

        let key_builder = key_array.into_builder();
        let value_builder = value_array.into_builder();

        match (key_builder, value_builder) {
            (Ok(key_builder), Ok(value_builder)) => Ok(unsafe {
                PrimitiveDictionaryBuilder::new_from_builders(key_builder, value_builder)
            }),
            (Err(key_array), Ok(mut value_builder)) => {
                Err(Self::try_new(key_array, Arc::new(value_builder.finish())).unwrap())
            }
            (Ok(mut key_builder), Err(value_array)) => {
                Err(Self::try_new(key_builder.finish(), Arc::new(value_array)).unwrap())
            }
            (Err(key_array), Err(value_array)) => {
                Err(Self::try_new(key_array, Arc::new(value_array)).unwrap())
            }
        }
    }

    /// Applies an unary and infallible function to a mutable dictionary array.
    /// Mutable dictionary array means that the buffers are not shared with other arrays.
    /// As a result, this mutates the buffers directly without allocating new buffers.
    ///
    /// # Implementation
    ///
    /// This will apply the function for all dictionary values, including those on null slots.
    /// This implies that the operation must be infallible for any value of the corresponding type
    /// or this function may panic.
    /// # Example
    /// ```
    /// # use std::sync::Arc;
    /// # use arrow_array::{Array, ArrayAccessor, DictionaryArray, StringArray, types::{Int8Type, Int32Type}};
    /// # use arrow_array::{Int8Array, Int32Array};
    /// let values = Int32Array::from(vec![Some(10), Some(20), None]);
    /// let keys = Int8Array::from_iter_values([0, 0, 1, 2]);
    /// let dictionary = DictionaryArray::<Int8Type>::try_new(keys, Arc::new(values)).unwrap();
    /// let c = dictionary.unary_mut::<_, Int32Type>(|x| x + 1).unwrap();
    /// let typed = c.downcast_dict::<Int32Array>().unwrap();
    /// assert_eq!(typed.value(0), 11);
    /// assert_eq!(typed.value(1), 11);
    /// assert_eq!(typed.value(2), 21);
    /// ```
    pub fn unary_mut<F, V>(self, op: F) -> Result<DictionaryArray<K>, DictionaryArray<K>>
    where
        V: ArrowPrimitiveType,
        F: Fn(V::Native) -> V::Native,
    {
        let mut builder: PrimitiveDictionaryBuilder<K, V> = self.into_primitive_dict_builder()?;
        builder
            .values_slice_mut()
            .iter_mut()
            .for_each(|v| *v = op(*v));
        Ok(builder.finish())
    }

    /// Computes an occupancy mask for this dictionary's values
    ///
    /// For each value in [`Self::values`] the corresponding bit will be set in the
    /// returned mask if it is referenced by a key in this [`DictionaryArray`]
    pub fn occupancy(&self) -> BooleanBuffer {
        let len = self.values.len();
        let mut builder = BooleanBufferBuilder::new(len);
        builder.resize(len);
        let slice = builder.as_slice_mut();
        match self.keys.nulls().filter(|n| n.null_count() > 0) {
            Some(n) => {
                let v = self.keys.values();
                n.valid_indices()
                    .for_each(|idx| set_bit(slice, v[idx].as_usize()))
            }
            None => {
                let v = self.keys.values();
                v.iter().for_each(|v| set_bit(slice, v.as_usize()))
            }
        }
        builder.finish()
    }
}

/// Constructs a `DictionaryArray` from an array data reference.
impl<T: ArrowDictionaryKeyType> From<ArrayData> for DictionaryArray<T> {
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

            let values = make_array(data.child_data()[0].clone());
            let data_type = data.data_type().clone();

            // create a zero-copy of the keys' data
            // SAFETY:
            // ArrayData is valid and verified type above

            let keys = PrimitiveArray::<T>::from(unsafe {
                data.into_builder()
                    .data_type(T::DATA_TYPE)
                    .child_data(vec![])
                    .build_unchecked()
            });

            Self {
                data_type,
                keys,
                values,
                is_ordered: false,
            }
        } else {
            panic!("DictionaryArray must have Dictionary data type.")
        }
    }
}

impl<T: ArrowDictionaryKeyType> From<DictionaryArray<T>> for ArrayData {
    fn from(array: DictionaryArray<T>) -> Self {
        let builder = array
            .keys
            .into_data()
            .into_builder()
            .data_type(array.data_type)
            .child_data(vec![array.values.to_data()]);

        unsafe { builder.build_unchecked() }
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
        builder.extend(it);
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

impl<T: ArrowDictionaryKeyType> Array for DictionaryArray<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn to_data(&self) -> ArrayData {
        self.clone().into()
    }

    fn into_data(self) -> ArrayData {
        self.into()
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn slice(&self, offset: usize, length: usize) -> ArrayRef {
        Arc::new(self.slice(offset, length))
    }

    fn len(&self) -> usize {
        self.keys.len()
    }

    fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    fn offset(&self) -> usize {
        self.keys.offset()
    }

    fn nulls(&self) -> Option<&NullBuffer> {
        self.keys.nulls()
    }

    fn logical_nulls(&self) -> Option<NullBuffer> {
        match self.values.nulls() {
            None => self.nulls().cloned(),
            Some(value_nulls) => {
                let mut builder = BooleanBufferBuilder::new(self.len());
                match self.keys.nulls() {
                    Some(n) => builder.append_buffer(n.inner()),
                    None => builder.append_n(self.len(), true),
                }
                for (idx, k) in self.keys.values().iter().enumerate() {
                    let k = k.as_usize();
                    // Check range to allow for nulls
                    if k < value_nulls.len() && value_nulls.is_null(k) {
                        builder.set_bit(idx, false);
                    }
                }
                Some(builder.finish().into())
            }
        }
    }

    fn is_nullable(&self) -> bool {
        !self.is_empty() && (self.nulls().is_some() || self.values.is_nullable())
    }

    fn get_buffer_memory_size(&self) -> usize {
        self.keys.get_buffer_memory_size() + self.values.get_buffer_memory_size()
    }

    fn get_array_memory_size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.keys.get_buffer_memory_size()
            + self.values.get_array_memory_size()
    }
}

impl<T: ArrowDictionaryKeyType> std::fmt::Debug for DictionaryArray<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        writeln!(
            f,
            "DictionaryArray {{keys: {:?} values: {:?}}}",
            self.keys, self.values
        )
    }
}

/// A [`DictionaryArray`] typed on its child values array
///
/// Implements [`ArrayAccessor`] allowing fast access to its elements
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
pub struct TypedDictionaryArray<'a, K: ArrowDictionaryKeyType, V> {
    /// The dictionary array
    dictionary: &'a DictionaryArray<K>,
    /// The values of the dictionary
    values: &'a V,
}

// Manually implement `Clone` to avoid `V: Clone` type constraint
impl<K: ArrowDictionaryKeyType, V> Clone for TypedDictionaryArray<'_, K, V> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<K: ArrowDictionaryKeyType, V> Copy for TypedDictionaryArray<'_, K, V> {}

impl<K: ArrowDictionaryKeyType, V> std::fmt::Debug for TypedDictionaryArray<'_, K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        writeln!(f, "TypedDictionaryArray({:?})", self.dictionary)
    }
}

impl<'a, K: ArrowDictionaryKeyType, V> TypedDictionaryArray<'a, K, V> {
    /// Returns the keys of this [`TypedDictionaryArray`]
    pub fn keys(&self) -> &'a PrimitiveArray<K> {
        self.dictionary.keys()
    }

    /// Returns the values of this [`TypedDictionaryArray`]
    pub fn values(&self) -> &'a V {
        self.values
    }
}

impl<K: ArrowDictionaryKeyType, V: Sync> Array for TypedDictionaryArray<'_, K, V> {
    fn as_any(&self) -> &dyn Any {
        self.dictionary
    }

    fn to_data(&self) -> ArrayData {
        self.dictionary.to_data()
    }

    fn into_data(self) -> ArrayData {
        self.dictionary.into_data()
    }

    fn data_type(&self) -> &DataType {
        self.dictionary.data_type()
    }

    fn slice(&self, offset: usize, length: usize) -> ArrayRef {
        Arc::new(self.dictionary.slice(offset, length))
    }

    fn len(&self) -> usize {
        self.dictionary.len()
    }

    fn is_empty(&self) -> bool {
        self.dictionary.is_empty()
    }

    fn offset(&self) -> usize {
        self.dictionary.offset()
    }

    fn nulls(&self) -> Option<&NullBuffer> {
        self.dictionary.nulls()
    }

    fn logical_nulls(&self) -> Option<NullBuffer> {
        self.dictionary.logical_nulls()
    }

    fn is_nullable(&self) -> bool {
        self.dictionary.is_nullable()
    }

    fn get_buffer_memory_size(&self) -> usize {
        self.dictionary.get_buffer_memory_size()
    }

    fn get_array_memory_size(&self) -> usize {
        self.dictionary.get_array_memory_size()
    }
}

impl<K, V> IntoIterator for TypedDictionaryArray<'_, K, V>
where
    K: ArrowDictionaryKeyType,
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
    K: ArrowDictionaryKeyType,
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

/// A [`DictionaryArray`] with the key type erased
///
/// This can be used to efficiently implement kernels for all possible dictionary
/// keys without needing to create specialized implementations for each key type
///
/// For example
///
/// ```
/// # use arrow_array::*;
/// # use arrow_array::cast::AsArray;
/// # use arrow_array::builder::PrimitiveDictionaryBuilder;
/// # use arrow_array::types::*;
/// # use arrow_schema::ArrowError;
/// # use std::sync::Arc;
///
/// fn to_string(a: &dyn Array) -> Result<ArrayRef, ArrowError> {
///     if let Some(d) = a.as_any_dictionary_opt() {
///         // Recursively handle dictionary input
///         let r = to_string(d.values().as_ref())?;
///         return Ok(d.with_values(r));
///     }
///     downcast_primitive_array! {
///         a => Ok(Arc::new(a.iter().map(|x| x.map(|x| format!("{x:?}"))).collect::<StringArray>())),
///         d => Err(ArrowError::InvalidArgumentError(format!("{d:?} not supported")))
///     }
/// }
///
/// let result = to_string(&Int32Array::from(vec![1, 2, 3])).unwrap();
/// let actual = result.as_string::<i32>().iter().map(Option::unwrap).collect::<Vec<_>>();
/// assert_eq!(actual, &["1", "2", "3"]);
///
/// let mut dict = PrimitiveDictionaryBuilder::<Int32Type, UInt16Type>::new();
/// dict.extend([Some(1), Some(1), Some(2), Some(3), Some(2)]);
/// let dict = dict.finish();
///
/// let r = to_string(&dict).unwrap();
/// let r = r.as_dictionary::<Int32Type>().downcast_dict::<StringArray>().unwrap();
/// assert_eq!(r.keys(), dict.keys()); // Keys are the same
///
/// let actual = r.into_iter().map(Option::unwrap).collect::<Vec<_>>();
/// assert_eq!(actual, &["1", "1", "2", "3", "2"]);
/// ```
///
/// See [`AsArray::as_any_dictionary_opt`] and [`AsArray::as_any_dictionary`]
pub trait AnyDictionaryArray: Array {
    /// Returns the primitive keys of this dictionary as an [`Array`]
    fn keys(&self) -> &dyn Array;

    /// Returns the values of this dictionary
    fn values(&self) -> &ArrayRef;

    /// Returns the keys of this dictionary as usize
    ///
    /// The values for nulls will be arbitrary, but are guaranteed
    /// to be in the range `0..self.values.len()`
    ///
    /// # Panic
    ///
    /// Panics if `values.len() == 0`
    fn normalized_keys(&self) -> Vec<usize>;

    /// Create a new [`DictionaryArray`] replacing `values` with the new values
    ///
    /// See [`DictionaryArray::with_values`]
    fn with_values(&self, values: ArrayRef) -> ArrayRef;
}

impl<K: ArrowDictionaryKeyType> AnyDictionaryArray for DictionaryArray<K> {
    fn keys(&self) -> &dyn Array {
        &self.keys
    }

    fn values(&self) -> &ArrayRef {
        self.values()
    }

    fn normalized_keys(&self) -> Vec<usize> {
        let v_len = self.values().len();
        assert_ne!(v_len, 0);
        let iter = self.keys().values().iter();
        iter.map(|x| x.as_usize().min(v_len - 1)).collect()
    }

    fn with_values(&self, values: ArrayRef) -> ArrayRef {
        Arc::new(self.with_values(values))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cast::as_dictionary_array;
    use crate::{Int16Array, Int32Array, Int8Array};
    use arrow_buffer::{Buffer, ToByteSlice};

    #[test]
    fn test_dictionary_array() {
        // Construct a value array
        let value_data = ArrayData::builder(DataType::Int8)
            .len(8)
            .add_buffer(Buffer::from(
                [10_i8, 11, 12, 13, 14, 15, 16, 17].to_byte_slice(),
            ))
            .build()
            .unwrap();

        // Construct a buffer for value offsets, for the nested array:
        let keys = Buffer::from([2_i16, 3, 4].to_byte_slice());

        // Construct a dictionary array from the above two
        let key_type = DataType::Int16;
        let value_type = DataType::Int8;
        let dict_data_type = DataType::Dictionary(Box::new(key_type), Box::new(value_type));
        let dict_data = ArrayData::builder(dict_data_type.clone())
            .len(3)
            .add_buffer(keys.clone())
            .add_child_data(value_data.clone())
            .build()
            .unwrap();
        let dict_array = Int16DictionaryArray::from(dict_data);

        let values = dict_array.values();
        assert_eq!(value_data, values.to_data());
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
        assert_eq!(value_data, values.to_data());
        assert_eq!(DataType::Int8, dict_array.value_type());
        assert_eq!(2, dict_array.len());
        assert_eq!(dict_array.keys(), &Int16Array::from(vec![3_i16, 4]));
    }

    #[test]
    fn test_dictionary_builder_append_many() {
        let mut builder = PrimitiveDictionaryBuilder::<UInt8Type, UInt32Type>::new();

        builder.append(1).unwrap();
        builder.append_n(2, 2).unwrap();
        builder.append_options(None, 2);
        builder.append_options(Some(3), 3);

        let array = builder.finish();

        let values = array
            .values()
            .as_primitive::<UInt32Type>()
            .iter()
            .map(Option::unwrap)
            .collect::<Vec<_>>();
        assert_eq!(values, &[1, 2, 3]);
        let keys = array.keys().iter().collect::<Vec<_>>();
        assert_eq!(
            keys,
            &[
                Some(0),
                Some(1),
                Some(1),
                None,
                None,
                Some(2),
                Some(2),
                Some(2)
            ]
        );
    }

    #[test]
    fn test_string_dictionary_builder_append_many() {
        let mut builder = StringDictionaryBuilder::<Int8Type>::new();

        builder.append("a").unwrap();
        builder.append_n("b", 2).unwrap();
        builder.append_options(None::<&str>, 2);
        builder.append_options(Some("c"), 3);

        let array = builder.finish();

        let values = array
            .values()
            .as_string::<i32>()
            .iter()
            .map(Option::unwrap)
            .collect::<Vec<_>>();
        assert_eq!(values, &["a", "b", "c"]);
        let keys = array.keys().iter().collect::<Vec<_>>();
        assert_eq!(
            keys,
            &[
                Some(0),
                Some(1),
                Some(1),
                None,
                None,
                Some(2),
                Some(2),
                Some(2)
            ]
        );
    }

    #[test]
    fn test_dictionary_array_fmt_debug() {
        let mut builder = PrimitiveDictionaryBuilder::<UInt8Type, UInt32Type>::with_capacity(3, 2);
        builder.append(12345678).unwrap();
        builder.append_null();
        builder.append(22345678).unwrap();
        let array = builder.finish();
        assert_eq!(
            "DictionaryArray {keys: PrimitiveArray<UInt8>\n[\n  0,\n  null,\n  1,\n] values: PrimitiveArray<UInt32>\n[\n  12345678,\n  22345678,\n]}\n",
            format!("{array:?}")
        );

        let mut builder = PrimitiveDictionaryBuilder::<UInt8Type, UInt32Type>::with_capacity(20, 2);
        for _ in 0..20 {
            builder.append(1).unwrap();
        }
        let array = builder.finish();
        assert_eq!(
            "DictionaryArray {keys: PrimitiveArray<UInt8>\n[\n  0,\n  0,\n  0,\n  0,\n  0,\n  0,\n  0,\n  0,\n  0,\n  0,\n  0,\n  0,\n  0,\n  0,\n  0,\n  0,\n  0,\n  0,\n  0,\n  0,\n] values: PrimitiveArray<UInt32>\n[\n  1,\n]}\n",
            format!("{array:?}")
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
            format!("{array:?}")
        );

        let array: DictionaryArray<Int8Type> = test.into_iter().collect();
        assert_eq!(
            "DictionaryArray {keys: PrimitiveArray<Int8>\n[\n  0,\n  0,\n  1,\n  2,\n] values: StringArray\n[\n  \"a\",\n  \"b\",\n  \"c\",\n]}\n",
            format!("{array:?}")
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
            .into_data()
            .validate_full()
            .expect("All null array has valid array data");
    }

    #[test]
    fn test_dictionary_iter() {
        // Construct a value array
        let values = Int8Array::from_iter_values([10_i8, 11, 12, 13, 14, 15, 16, 17]);
        let keys = Int16Array::from_iter_values([2_i16, 3, 4]);

        // Construct a dictionary array from the above two
        let dict_array = DictionaryArray::new(keys, Arc::new(values));

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

        let array = DictionaryArray::new(keys, Arc::new(values));
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

        let array = DictionaryArray::new(keys, Arc::new(values));
        assert_eq!(array.keys().data_type(), &DataType::Int32);
        assert_eq!(array.values().data_type(), &DataType::Utf8);

        assert_eq!(array.null_count(), 1);
        assert_eq!(array.logical_null_count(), 1);

        assert!(array.keys().is_valid(0));
        assert!(array.keys().is_valid(1));
        assert!(array.keys().is_null(2));
        assert!(array.keys().is_valid(3));

        assert_eq!(array.keys().value(0), 0);
        assert_eq!(array.keys().value(1), 2);
        assert_eq!(array.keys().value(3), 1);

        assert_eq!(
            "DictionaryArray {keys: PrimitiveArray<Int32>\n[\n  0,\n  2,\n  null,\n  1,\n] values: StringArray\n[\n  \"foo\",\n  \"bar\",\n  \"baz\",\n]}\n",
            format!("{array:?}")
        );
    }

    #[test]
    #[should_panic(expected = "Invalid dictionary key 3 at index 1, expected 0 <= key < 2")]
    fn test_try_new_index_too_large() {
        let values: StringArray = [Some("foo"), Some("bar")].into_iter().collect();
        // dictionary only has 2 values, so offset 3 is out of bounds
        let keys: Int32Array = [Some(0), Some(3)].into_iter().collect();
        DictionaryArray::new(keys, Arc::new(values));
    }

    #[test]
    #[should_panic(expected = "Invalid dictionary key -100 at index 0, expected 0 <= key < 2")]
    fn test_try_new_index_too_small() {
        let values: StringArray = [Some("foo"), Some("bar")].into_iter().collect();
        let keys: Int32Array = [Some(-100)].into_iter().collect();
        DictionaryArray::new(keys, Arc::new(values));
    }

    #[test]
    #[should_panic(expected = "DictionaryArray's data type must match, expected Int64 got Int32")]
    fn test_from_array_data_validation() {
        let a = DictionaryArray::<Int32Type>::from_iter(["32"]);
        let _ = DictionaryArray::<Int64Type>::from(a.into_data());
    }

    #[test]
    fn test_into_primitive_dict_builder() {
        let values = Int32Array::from_iter_values([10_i32, 12, 15]);
        let keys = Int8Array::from_iter_values([1_i8, 0, 2, 0]);

        let dict_array = DictionaryArray::new(keys, Arc::new(values));

        let boxed: ArrayRef = Arc::new(dict_array);
        let col: DictionaryArray<Int8Type> = as_dictionary_array(&boxed).clone();

        drop(boxed);

        let mut builder = col.into_primitive_dict_builder::<Int32Type>().unwrap();

        let slice = builder.values_slice_mut();
        assert_eq!(slice, &[10, 12, 15]);

        slice[0] = 4;
        slice[1] = 2;
        slice[2] = 1;

        let values = Int32Array::from_iter_values([4_i32, 2, 1]);
        let keys = Int8Array::from_iter_values([1_i8, 0, 2, 0]);

        let expected = DictionaryArray::new(keys, Arc::new(values));

        let new_array = builder.finish();
        assert_eq!(expected, new_array);
    }

    #[test]
    fn test_into_primitive_dict_builder_cloned_array() {
        let values = Int32Array::from_iter_values([10_i32, 12, 15]);
        let keys = Int8Array::from_iter_values([1_i8, 0, 2, 0]);

        let dict_array = DictionaryArray::new(keys, Arc::new(values));

        let boxed: ArrayRef = Arc::new(dict_array);

        let col: DictionaryArray<Int8Type> = DictionaryArray::<Int8Type>::from(boxed.to_data());
        let err = col.into_primitive_dict_builder::<Int32Type>();

        let returned = err.unwrap_err();

        let values = Int32Array::from_iter_values([10_i32, 12, 15]);
        let keys = Int8Array::from_iter_values([1_i8, 0, 2, 0]);

        let expected = DictionaryArray::new(keys, Arc::new(values));
        assert_eq!(expected, returned);
    }

    #[test]
    fn test_occupancy() {
        let keys = Int32Array::new((100..200).collect(), None);
        let values = Int32Array::from(vec![0; 1024]);
        let dict = DictionaryArray::new(keys, Arc::new(values));
        for (idx, v) in dict.occupancy().iter().enumerate() {
            let expected = (100..200).contains(&idx);
            assert_eq!(v, expected, "{idx}");
        }

        let keys = Int32Array::new(
            (0..100).collect(),
            Some((0..100).map(|x| x % 4 == 0).collect()),
        );
        let values = Int32Array::from(vec![0; 1024]);
        let dict = DictionaryArray::new(keys, Arc::new(values));
        for (idx, v) in dict.occupancy().iter().enumerate() {
            let expected = idx % 4 == 0 && idx < 100;
            assert_eq!(v, expected, "{idx}");
        }
    }

    #[test]
    fn test_iterator_nulls() {
        let keys = Int32Array::new(
            vec![0, 700, 1, 2].into(),
            Some(NullBuffer::from(vec![true, false, true, true])),
        );
        let values = Int32Array::from(vec![Some(50), None, Some(2)]);
        let dict = DictionaryArray::new(keys, Arc::new(values));
        let values: Vec<_> = dict
            .downcast_dict::<Int32Array>()
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(values, &[Some(50), None, None, Some(2)])
    }

    #[test]
    fn test_normalized_keys() {
        let values = vec![132, 0, 1].into();
        let nulls = NullBuffer::from(vec![false, true, true]);
        let keys = Int32Array::new(values, Some(nulls));
        let dictionary = DictionaryArray::new(keys, Arc::new(Int32Array::new_null(2)));
        assert_eq!(&dictionary.normalized_keys(), &[1, 0, 1])
    }
}
