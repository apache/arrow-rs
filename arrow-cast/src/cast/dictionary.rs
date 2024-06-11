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

use crate::cast::*;

/// Attempts to cast an `ArrayDictionary` with index type K into
/// `to_type` for supported types.
///
/// K is the key type
pub(crate) fn dictionary_cast<K: ArrowDictionaryKeyType>(
    array: &dyn Array,
    to_type: &DataType,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    use DataType::*;

    match to_type {
        Dictionary(to_index_type, to_value_type) => {
            let dict_array = array
                .as_any()
                .downcast_ref::<DictionaryArray<K>>()
                .ok_or_else(|| {
                    ArrowError::ComputeError(
                        "Internal Error: Cannot cast dictionary to DictionaryArray of expected type".to_string(),
                    )
                })?;

            let keys_array: ArrayRef =
                Arc::new(PrimitiveArray::<K>::from(dict_array.keys().to_data()));
            let values_array = dict_array.values();
            let cast_keys = cast_with_options(&keys_array, to_index_type, cast_options)?;
            let cast_values = cast_with_options(values_array, to_value_type, cast_options)?;

            // Failure to cast keys (because they don't fit in the
            // target type) results in NULL values;
            if cast_keys.null_count() > keys_array.null_count() {
                return Err(ArrowError::ComputeError(format!(
                    "Could not convert {} dictionary indexes from {:?} to {:?}",
                    cast_keys.null_count() - keys_array.null_count(),
                    keys_array.data_type(),
                    to_index_type
                )));
            }

            let data = cast_keys.into_data();
            let builder = data
                .into_builder()
                .data_type(to_type.clone())
                .child_data(vec![cast_values.into_data()]);

            // Safety
            // Cast keys are still valid
            let data = unsafe { builder.build_unchecked() };

            // create the appropriate array type
            let new_array: ArrayRef = match **to_index_type {
                Int8 => Arc::new(DictionaryArray::<Int8Type>::from(data)),
                Int16 => Arc::new(DictionaryArray::<Int16Type>::from(data)),
                Int32 => Arc::new(DictionaryArray::<Int32Type>::from(data)),
                Int64 => Arc::new(DictionaryArray::<Int64Type>::from(data)),
                UInt8 => Arc::new(DictionaryArray::<UInt8Type>::from(data)),
                UInt16 => Arc::new(DictionaryArray::<UInt16Type>::from(data)),
                UInt32 => Arc::new(DictionaryArray::<UInt32Type>::from(data)),
                UInt64 => Arc::new(DictionaryArray::<UInt64Type>::from(data)),
                _ => {
                    return Err(ArrowError::CastError(format!(
                        "Unsupported type {to_index_type:?} for dictionary index"
                    )));
                }
            };

            Ok(new_array)
        }
        Utf8View => {
            // `unpack_dictionary` can handle Utf8View/BinaryView types, but incurs unnecessary data copy of the value buffer.
            // we handle it here to avoid the copy.
            let dict_array = array
                .as_dictionary::<K>()
                .downcast_dict::<StringArray>()
                .unwrap();

            let string_values = dict_array.values();
            let value_offsets = string_values.value_offsets();
            let value_buffer = string_values.values().clone();

            let view_buffer =
                view_from_dict_values(value_offsets, &value_buffer, dict_array.keys());

            // Safety:
            // the buffer is from StringArray which is utf8.
            let string_view = unsafe {
                StringViewArray::new_unchecked(
                    view_buffer,
                    vec![value_buffer],
                    dict_array.nulls().cloned(),
                )
            };
            Ok(Arc::new(string_view))
        }
        BinaryView => {
            // `unpack_dictionary` can handle Utf8View/BinaryView types, but incurs unnecessary data copy of the value buffer.
            // we handle it here to avoid the copy.
            let dict_array = array
                .as_dictionary::<K>()
                .downcast_dict::<BinaryArray>()
                .unwrap();

            let binary_values = dict_array.values();
            let value_offsets = binary_values.value_offsets();
            let value_buffer = binary_values.values().clone();

            let view_buffer =
                view_from_dict_values(value_offsets, &value_buffer, dict_array.keys());
            let binary_view = unsafe {
                BinaryViewArray::new_unchecked(
                    view_buffer,
                    vec![value_buffer],
                    dict_array.nulls().cloned(),
                )
            };
            Ok(Arc::new(binary_view))
        }
        _ => unpack_dictionary::<K>(array, to_type, cast_options),
    }
}

fn view_from_dict_values<K: ArrowDictionaryKeyType>(
    value_offsets: &[i32],
    value_buffer: &arrow_buffer::Buffer,
    keys: &PrimitiveArray<K>,
) -> ScalarBuffer<u128> {
    let mut view_builder = BufferBuilder::<u128>::new(keys.len());
    for i in keys.iter() {
        match i {
            Some(v) => {
                let idx = v.to_usize().unwrap();
                let offset = value_offsets[idx];
                let end = value_offsets[idx + 1];
                let length = end - offset;
                let value_buf = &value_buffer[offset as usize..end as usize];

                if length <= 12 {
                    let mut view_buffer = [0; 16];
                    view_buffer[0..4].copy_from_slice(&length.to_le_bytes());
                    view_buffer[4..4 + value_buf.len()].copy_from_slice(value_buf);
                    view_builder.append(u128::from_le_bytes(view_buffer));
                } else {
                    let view = ByteView {
                        length: length as u32,
                        prefix: u32::from_le_bytes(value_buf[0..4].try_into().unwrap()),
                        buffer_index: 0,
                        offset: offset as u32,
                    };
                    view_builder.append(view.into());
                }
            }
            None => {
                view_builder.append_n_zeroed(1);
            }
        }
    }
    ScalarBuffer::new(view_builder.finish(), 0, keys.len())
}

// Unpack a dictionary where the keys are of type <K> into a flattened array of type to_type
pub(crate) fn unpack_dictionary<K>(
    array: &dyn Array,
    to_type: &DataType,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError>
where
    K: ArrowDictionaryKeyType,
{
    let dict_array = array.as_dictionary::<K>();
    let cast_dict_values = cast_with_options(dict_array.values(), to_type, cast_options)?;
    take(cast_dict_values.as_ref(), dict_array.keys(), None)
}

/// Attempts to encode an array into an `ArrayDictionary` with index
/// type K and value (dictionary) type value_type
///
/// K is the key type
pub(crate) fn cast_to_dictionary<K: ArrowDictionaryKeyType>(
    array: &dyn Array,
    dict_value_type: &DataType,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    use DataType::*;

    match *dict_value_type {
        Int8 => pack_numeric_to_dictionary::<K, Int8Type>(array, dict_value_type, cast_options),
        Int16 => pack_numeric_to_dictionary::<K, Int16Type>(array, dict_value_type, cast_options),
        Int32 => pack_numeric_to_dictionary::<K, Int32Type>(array, dict_value_type, cast_options),
        Int64 => pack_numeric_to_dictionary::<K, Int64Type>(array, dict_value_type, cast_options),
        UInt8 => pack_numeric_to_dictionary::<K, UInt8Type>(array, dict_value_type, cast_options),
        UInt16 => pack_numeric_to_dictionary::<K, UInt16Type>(array, dict_value_type, cast_options),
        UInt32 => pack_numeric_to_dictionary::<K, UInt32Type>(array, dict_value_type, cast_options),
        UInt64 => pack_numeric_to_dictionary::<K, UInt64Type>(array, dict_value_type, cast_options),
        Decimal128(_, _) => {
            pack_numeric_to_dictionary::<K, Decimal128Type>(array, dict_value_type, cast_options)
        }
        Decimal256(_, _) => {
            pack_numeric_to_dictionary::<K, Decimal256Type>(array, dict_value_type, cast_options)
        }
        Utf8 => {
            if array.data_type() == &DataType::Utf8View {
                return string_view_to_dictionary::<K, i32>(array);
            }
            pack_byte_to_dictionary::<K, GenericStringType<i32>>(array, cast_options)
        }
        LargeUtf8 => {
            if array.data_type() == &DataType::Utf8View {
                return string_view_to_dictionary::<K, i64>(array);
            }
            pack_byte_to_dictionary::<K, GenericStringType<i64>>(array, cast_options)
        }
        Binary => {
            if array.data_type() == &DataType::BinaryView {
                return binary_view_to_dictionary::<K, i32>(array);
            }
            pack_byte_to_dictionary::<K, GenericBinaryType<i32>>(array, cast_options)
        }
        LargeBinary => {
            if array.data_type() == &DataType::BinaryView {
                return binary_view_to_dictionary::<K, i64>(array);
            }
            pack_byte_to_dictionary::<K, GenericBinaryType<i64>>(array, cast_options)
        }
        _ => Err(ArrowError::CastError(format!(
            "Unsupported output type for dictionary packing: {dict_value_type:?}"
        ))),
    }
}

// Packs the data from the primitive array of type <V> to a
// DictionaryArray with keys of type K and values of value_type V
pub(crate) fn pack_numeric_to_dictionary<K, V>(
    array: &dyn Array,
    dict_value_type: &DataType,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError>
where
    K: ArrowDictionaryKeyType,
    V: ArrowPrimitiveType,
{
    // attempt to cast the source array values to the target value type (the dictionary values type)
    let cast_values = cast_with_options(array, dict_value_type, cast_options)?;
    let values = cast_values.as_primitive::<V>();

    let mut b = PrimitiveDictionaryBuilder::<K, V>::with_capacity(values.len(), values.len());

    // copy each element one at a time
    for i in 0..values.len() {
        if values.is_null(i) {
            b.append_null();
        } else {
            b.append(values.value(i))?;
        }
    }
    Ok(Arc::new(b.finish()))
}

pub(crate) fn string_view_to_dictionary<K, O: OffsetSizeTrait>(
    array: &dyn Array,
) -> Result<ArrayRef, ArrowError>
where
    K: ArrowDictionaryKeyType,
{
    let mut b = GenericByteDictionaryBuilder::<K, GenericStringType<O>>::with_capacity(
        array.len(),
        1024,
        1024,
    );
    let string_view = array.as_any().downcast_ref::<StringViewArray>().unwrap();
    for v in string_view.iter() {
        match v {
            Some(v) => {
                b.append(v)?;
            }
            None => {
                b.append_null();
            }
        }
    }

    return Ok(Arc::new(b.finish()));
}

pub(crate) fn binary_view_to_dictionary<K, O: OffsetSizeTrait>(
    array: &dyn Array,
) -> Result<ArrayRef, ArrowError>
where
    K: ArrowDictionaryKeyType,
{
    let mut b = GenericByteDictionaryBuilder::<K, GenericBinaryType<O>>::with_capacity(
        array.len(),
        1024,
        1024,
    );
    let binary_view = array.as_any().downcast_ref::<BinaryViewArray>().unwrap();
    for v in binary_view.iter() {
        match v {
            Some(v) => {
                b.append(v)?;
            }
            None => {
                b.append_null();
            }
        }
    }

    return Ok(Arc::new(b.finish()));
}

// Packs the data as a GenericByteDictionaryBuilder, if possible, with the
// key types of K
pub(crate) fn pack_byte_to_dictionary<K, T>(
    array: &dyn Array,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError>
where
    K: ArrowDictionaryKeyType,
    T: ByteArrayType,
{
    let cast_values = cast_with_options(array, &T::DATA_TYPE, cast_options)?;
    let values = cast_values
        .as_any()
        .downcast_ref::<GenericByteArray<T>>()
        .unwrap();
    let mut b = GenericByteDictionaryBuilder::<K, T>::with_capacity(values.len(), 1024, 1024);

    // copy each element one at a time
    for i in 0..values.len() {
        if values.is_null(i) {
            b.append_null();
        } else {
            b.append(values.value(i))?;
        }
    }
    Ok(Arc::new(b.finish()))
}
