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
                .ok_or_else(|| {
                    ArrowError::ComputeError(
                        "Internal Error: Cannot cast Utf8View to StringArray of expected type"
                            .to_string(),
                    )
                })?;

            let string_view = view_from_dict_values::<K, StringViewType, GenericStringType<i32>>(
                dict_array.values(),
                dict_array.keys(),
            )?;
            Ok(Arc::new(string_view))
        }
        BinaryView => {
            // `unpack_dictionary` can handle Utf8View/BinaryView types, but incurs unnecessary data copy of the value buffer.
            // we handle it here to avoid the copy.
            let dict_array = array
                .as_dictionary::<K>()
                .downcast_dict::<BinaryArray>()
                .ok_or_else(|| {
                    ArrowError::ComputeError(
                        "Internal Error: Cannot cast BinaryView to BinaryArray of expected type"
                            .to_string(),
                    )
                })?;

            let binary_view = view_from_dict_values::<K, BinaryViewType, BinaryType>(
                dict_array.values(),
                dict_array.keys(),
            )?;
            Ok(Arc::new(binary_view))
        }
        _ => unpack_dictionary::<K>(array, to_type, cast_options),
    }
}

fn view_from_dict_values<K: ArrowDictionaryKeyType, T: ByteViewType, V: ByteArrayType>(
    array: &GenericByteArray<V>,
    keys: &PrimitiveArray<K>,
) -> Result<GenericByteViewArray<T>, ArrowError> {
    let value_buffer = array.values();
    let value_offsets = array.value_offsets();
    let mut builder = GenericByteViewBuilder::<T>::with_capacity(keys.len());
    builder.append_block(value_buffer.clone());
    for i in keys.iter() {
        match i {
            Some(v) => {
                let idx = v.to_usize().ok_or_else(|| {
                    ArrowError::ComputeError("Invalid dictionary index".to_string())
                })?;

                // Safety
                // (1) The index is within bounds as they are offsets
                // (2) The append_view is safe
                unsafe {
                    let offset = value_offsets.get_unchecked(idx).as_usize();
                    let end = value_offsets.get_unchecked(idx + 1).as_usize();
                    let length = end - offset;
                    builder.append_view_unchecked(0, offset as u32, length as u32)
                }
            }
            None => {
                builder.append_null();
            }
        }
    }
    Ok(builder.finish())
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

/// Pack a data type into a dictionary array passing the values through a primitive array
pub(crate) fn pack_array_to_dictionary_via_primitive<K: ArrowDictionaryKeyType>(
    array: &dyn Array,
    primitive_type: DataType,
    dict_value_type: &DataType,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    let primitive = cast_with_options(array, &primitive_type, cast_options)?;
    let dict = cast_with_options(
        primitive.as_ref(),
        &DataType::Dictionary(Box::new(K::DATA_TYPE), Box::new(primitive_type)),
        cast_options,
    )?;
    cast_with_options(
        dict.as_ref(),
        &DataType::Dictionary(Box::new(K::DATA_TYPE), Box::new(dict_value_type.clone())),
        cast_options,
    )
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
        Decimal128(p, s) => {
            let dict = pack_numeric_to_dictionary::<K, Decimal128Type>(
                array,
                dict_value_type,
                cast_options,
            )?;
            let dict = dict
                .as_dictionary::<K>()
                .downcast_dict::<Decimal128Array>()
                .ok_or_else(|| {
                    ArrowError::ComputeError(
                        "Internal Error: Cannot cast dict to Decimal128Array".to_string(),
                    )
                })?;
            let value = dict.values().clone();
            // Set correct precision/scale
            let value = value.with_precision_and_scale(p, s)?;
            Ok(Arc::new(DictionaryArray::<K>::try_new(
                dict.keys().clone(),
                Arc::new(value),
            )?))
        }
        Decimal256(p, s) => {
            let dict = pack_numeric_to_dictionary::<K, Decimal256Type>(
                array,
                dict_value_type,
                cast_options,
            )?;
            let dict = dict
                .as_dictionary::<K>()
                .downcast_dict::<Decimal256Array>()
                .ok_or_else(|| {
                    ArrowError::ComputeError(
                        "Internal Error: Cannot cast dict to Decimal256Array".to_string(),
                    )
                })?;
            let value = dict.values().clone();
            // Set correct precision/scale
            let value = value.with_precision_and_scale(p, s)?;
            Ok(Arc::new(DictionaryArray::<K>::try_new(
                dict.keys().clone(),
                Arc::new(value),
            )?))
        }
        Float16 => {
            pack_numeric_to_dictionary::<K, Float16Type>(array, dict_value_type, cast_options)
        }
        Float32 => {
            pack_numeric_to_dictionary::<K, Float32Type>(array, dict_value_type, cast_options)
        }
        Float64 => {
            pack_numeric_to_dictionary::<K, Float64Type>(array, dict_value_type, cast_options)
        }
        Date32 => pack_array_to_dictionary_via_primitive::<K>(
            array,
            DataType::Int32,
            dict_value_type,
            cast_options,
        ),
        Date64 => pack_array_to_dictionary_via_primitive::<K>(
            array,
            DataType::Int64,
            dict_value_type,
            cast_options,
        ),
        Time32(_) => pack_array_to_dictionary_via_primitive::<K>(
            array,
            DataType::Int32,
            dict_value_type,
            cast_options,
        ),
        Time64(_) => pack_array_to_dictionary_via_primitive::<K>(
            array,
            DataType::Int64,
            dict_value_type,
            cast_options,
        ),
        Timestamp(_, _) => pack_array_to_dictionary_via_primitive::<K>(
            array,
            DataType::Int64,
            dict_value_type,
            cast_options,
        ),
        Utf8 => {
            // If the input is a view type, we can avoid casting (thus copying) the data
            if array.data_type() == &DataType::Utf8View {
                return string_view_to_dictionary::<K, i32>(array);
            }
            pack_byte_to_dictionary::<K, GenericStringType<i32>>(array, cast_options)
        }
        LargeUtf8 => {
            // If the input is a view type, we can avoid casting (thus copying) the data
            if array.data_type() == &DataType::Utf8View {
                return string_view_to_dictionary::<K, i64>(array);
            }
            pack_byte_to_dictionary::<K, GenericStringType<i64>>(array, cast_options)
        }
        Binary => {
            // If the input is a view type, we can avoid casting (thus copying) the data
            if array.data_type() == &DataType::BinaryView {
                return binary_view_to_dictionary::<K, i32>(array);
            }
            pack_byte_to_dictionary::<K, GenericBinaryType<i32>>(array, cast_options)
        }
        LargeBinary => {
            // If the input is a view type, we can avoid casting (thus copying) the data
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
    let string_view = array
        .as_any()
        .downcast_ref::<StringViewArray>()
        .ok_or_else(|| {
            ArrowError::ComputeError("Internal Error: Cannot cast to StringViewArray".to_string())
        })?;
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

    Ok(Arc::new(b.finish()))
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
    let binary_view = array
        .as_any()
        .downcast_ref::<BinaryViewArray>()
        .ok_or_else(|| {
            ArrowError::ComputeError("Internal Error: Cannot cast to BinaryViewArray".to_string())
        })?;
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

    Ok(Arc::new(b.finish()))
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
        .ok_or_else(|| {
            ArrowError::ComputeError("Internal Error: Cannot cast to GenericByteArray".to_string())
        })?;
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
