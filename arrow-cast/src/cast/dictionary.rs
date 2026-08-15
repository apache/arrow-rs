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

    /// Whether the dictionary is sparse; gates short of the measured 0.6x crossover for margin.
    #[inline]
    fn is_sparse<K: ArrowDictionaryKeyType>(array: &DictionaryArray<K>) -> bool {
        array.keys().len() < array.values().len() / 2
    }

    #[inline]
    fn values_buffer_fits_in_view<T: ByteArrayType>(values: &GenericByteArray<T>) -> bool {
        values.values().len() < i32::MAX as usize
    }

    let array = array.as_dictionary::<K>();
    let from_child_type = array.values().data_type();
    match (from_child_type, to_type) {
        (_, Dictionary(to_index_type, to_value_type)) => {
            dictionary_to_dictionary_cast(array, to_index_type, to_value_type, cast_options)
        }
        // `unpack_dictionary` operates per dictionary value before using take kernel to form
        // final view. `view_from_dict_values` builds output view directly per row (key index).
        // Based on benchmarking, `view_from_dict_values` is more efficient for sparse dictionaries
        // (more dictionary values than there are rows/keys), whilst `unpack_dictionary` is
        // more efficient for dense dictionaries, where a sparse dictionary is when rows reach
        // roughly 0.6x the dictionary size.
        //
        // Therefore delegate to the faster method based on the density of the input dictionary.
        (Utf8, Utf8View) if is_sparse(array) => {
            view_from_dict_values::<K, Utf8Type, StringViewType>(
                array.keys(),
                array.values().as_string::<i32>(),
            )
        }
        (Binary, BinaryView) if is_sparse(array) => {
            view_from_dict_values::<K, BinaryType, BinaryViewType>(
                array.keys(),
                array.values().as_binary::<i32>(),
            )
        }
        // `view_from_dict_values` directly appends the values buffer as a block using
        // `GenericByteViewBuilder::append_block` which asserts length of the buffer; we must
        // ensure this assertion holds to use it for large variants which may exceed the max allowable length.
        // If we exceed the length we can simply fallback to `unpack_dictionary` which still builds it
        // correctly.
        (LargeUtf8, Utf8View)
            if is_sparse(array)
                && values_buffer_fits_in_view(array.values().as_string::<i64>()) =>
        {
            view_from_dict_values::<K, LargeUtf8Type, StringViewType>(
                array.keys(),
                array.values().as_string::<i64>(),
            )
        }
        (LargeBinary, BinaryView)
            if is_sparse(array)
                && values_buffer_fits_in_view(array.values().as_binary::<i64>()) =>
        {
            view_from_dict_values::<K, LargeBinaryType, BinaryViewType>(
                array.keys(),
                array.values().as_binary::<i64>(),
            )
        }
        // Cross casts to a binary view need no validation: valid UTF-8 is valid binary.
        (Utf8, BinaryView) if is_sparse(array) => {
            view_from_dict_values::<K, Utf8Type, BinaryViewType>(
                array.keys(),
                array.values().as_string::<i32>(),
            )
        }
        (LargeUtf8, BinaryView)
            if is_sparse(array)
                && values_buffer_fits_in_view(array.values().as_string::<i64>()) =>
        {
            view_from_dict_values::<K, LargeUtf8Type, BinaryViewType>(
                array.keys(),
                array.values().as_string::<i64>(),
            )
        }
        // Cross casts to a string view require UTF-8 validation of the dictionary values.
        (Binary, Utf8View) if is_sparse(array) => binary_dict_to_string_view::<K, i32>(
            array.keys(),
            array.values().as_binary::<i32>(),
            cast_options,
        ),
        (LargeBinary, Utf8View)
            if is_sparse(array)
                && values_buffer_fits_in_view(array.values().as_binary::<i64>()) =>
        {
            binary_dict_to_string_view::<K, i64>(
                array.keys(),
                array.values().as_binary::<i64>(),
                cast_options,
            )
        }
        _ => unpack_dictionary(array, to_type, cast_options),
    }
}

fn dictionary_to_dictionary_cast<K: ArrowDictionaryKeyType>(
    array: &DictionaryArray<K>,
    to_index_type: &DataType,
    to_value_type: &DataType,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    use DataType::*;

    // Fast path for a nested dictionary source (`Dictionary<K, Dictionary<K2, V>>`).
    // Both layers index into the same inner values, so the two index layers can
    // be composed into one rather than materializing the values: `take` gathers
    // the inner keys through the outer keys and reuses the inner values buffer
    // untouched, so no value data is rewritten. The flattened single-level
    // dictionary is then cast to the requested index/value types.
    if matches!(array.values().data_type(), Dictionary(_, _)) {
        let flattened = take(array.values().as_ref(), array.keys(), None)?;
        return cast_with_options(
            &flattened,
            &Dictionary(
                Box::new(to_index_type.clone()),
                Box::new(to_value_type.clone()),
            ),
            cast_options,
        );
    }

    let keys_array: ArrayRef = Arc::new(PrimitiveArray::<K>::from(array.keys().to_data()));
    let values_array = array.values();
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
        .data_type(Dictionary(
            Box::new(to_index_type.clone()),
            Box::new(to_value_type.clone()),
        ))
        .child_data(vec![cast_values.into_data()]);

    // Safety
    // Cast keys are still valid
    let data = unsafe { builder.build_unchecked() };

    // create the appropriate array type
    let new_array: ArrayRef = match to_index_type {
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
                "Unsupported type {to_index_type} for dictionary index"
            )));
        }
    };

    Ok(new_array)
}

/// Cast `Dict<K, Binary>` or `Dict<K, LargeBinary>` to `Utf8View`, validating UTF-8 for each
/// dictionary value.
///
/// Fast path when all values are valid UTF-8: reuses the values buffer without copying.
/// When some values are invalid and `cast_options.safe` is true, rows pointing to those
/// values become null. When `cast_options.safe` is false, returns an error immediately.
fn binary_dict_to_string_view<K: ArrowDictionaryKeyType, O: OffsetSizeTrait>(
    keys: &PrimitiveArray<K>,
    values: &GenericByteArray<GenericBinaryType<O>>,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    match GenericStringArray::<O>::try_from_binary(values.clone()) {
        Ok(_) => {
            // All dictionary values are valid UTF-8: reuse the buffer zero-copy.
            view_from_dict_values::<K, GenericBinaryType<O>, StringViewType>(keys, values)
        }
        Err(e) => {
            if !cast_options.safe {
                return Err(e);
            }
            // safe=true: validate each dictionary value individually so we can nullify
            // only the rows whose key points to a null or invalid UTF-8 value.
            let valid: Vec<bool> = (0..values.len())
                .map(|i| !values.is_null(i) && std::str::from_utf8(values.value(i)).is_ok())
                .collect();

            let value_buffer = values.values();
            let value_offsets = values.value_offsets();
            let mut builder = StringViewBuilder::with_capacity(keys.len());
            builder.append_block(value_buffer.clone());

            for key in keys.iter() {
                match key {
                    Some(v) => {
                        let idx = v.to_usize().ok_or_else(|| {
                            ArrowError::ComputeError("Invalid dictionary index".to_string())
                        })?;
                        let is_valid = *valid.get(idx).ok_or_else(|| {
                            ArrowError::InvalidArgumentError(format!(
                                "Dictionary key {idx} out of bounds for dictionary values of length {}",
                                valid.len()
                            ))
                        })?;
                        if is_valid {
                            // Safety:
                            // (1) `idx` and `idx + 1` are in bounds, checked above
                            // (2) offsets are monotonically increasing, so end >= offset
                            // (3) the slice [offset..end] is within the buffer
                            // (4) the bytes are valid UTF-8, checked above
                            unsafe {
                                let offset = value_offsets.get_unchecked(idx).as_usize();
                                let end = value_offsets.get_unchecked(idx + 1).as_usize();
                                let length = end - offset;
                                builder.append_view_unchecked(0, offset as u32, length as u32);
                            }
                        } else {
                            builder.append_null();
                        }
                    }
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
    }
}

fn view_from_dict_values<K: ArrowDictionaryKeyType, V: ByteArrayType, T: ByteViewType>(
    keys: &PrimitiveArray<K>,
    values: &GenericByteArray<V>,
) -> Result<ArrayRef, ArrowError> {
    let value_buffer = values.values();
    let value_offsets = values.value_offsets();
    // A null *value* must produce a null row, not the empty slice its offsets happen to span.
    let values_have_nulls = values.null_count() != 0;
    let mut builder = GenericByteViewBuilder::<T>::with_capacity(keys.len());
    builder.append_block(value_buffer.clone());
    for i in keys.iter() {
        match i {
            Some(v) => {
                let idx = v.to_usize().ok_or_else(|| {
                    ArrowError::ComputeError("Invalid dictionary index".to_string())
                })?;

                if values_have_nulls && values.is_null(idx) {
                    builder.append_null();
                    continue;
                }

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
    Ok(Arc::new(builder.finish()))
}

// Unpack a dictionary into a flattened array of type to_type
pub(crate) fn unpack_dictionary<K: ArrowDictionaryKeyType>(
    array: &DictionaryArray<K>,
    to_type: &DataType,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    let cast_dict_values = cast_with_options(array.values(), to_type, cast_options)?;
    take(cast_dict_values.as_ref(), array.keys(), None)
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
        Decimal32(p, s) => pack_decimal_to_dictionary::<K, Decimal32Type>(
            array,
            dict_value_type,
            p,
            s,
            cast_options,
        ),
        Decimal64(p, s) => pack_decimal_to_dictionary::<K, Decimal64Type>(
            array,
            dict_value_type,
            p,
            s,
            cast_options,
        ),
        Decimal128(p, s) => pack_decimal_to_dictionary::<K, Decimal128Type>(
            array,
            dict_value_type,
            p,
            s,
            cast_options,
        ),
        Decimal256(p, s) => pack_decimal_to_dictionary::<K, Decimal256Type>(
            array,
            dict_value_type,
            p,
            s,
            cast_options,
        ),
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
        Utf8View => {
            let base_value_type = match array.data_type() {
                DataType::LargeUtf8 | DataType::Utf8View => DataType::LargeUtf8,
                _ => DataType::Utf8,
            };

            let dict_base = cast_to_dictionary::<K>(array, &base_value_type, cast_options)?;
            dictionary_cast::<K>(
                dict_base.as_ref(),
                &DataType::Dictionary(Box::new(K::DATA_TYPE), Box::new(DataType::Utf8View)),
                cast_options,
            )
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
        BinaryView => {
            let base_value_type = match array.data_type() {
                DataType::LargeBinary | DataType::BinaryView => DataType::LargeBinary,
                _ => DataType::Binary,
            };

            let dict_base = cast_to_dictionary::<K>(array, &base_value_type, cast_options)?;
            dictionary_cast::<K>(
                dict_base.as_ref(),
                &DataType::Dictionary(Box::new(K::DATA_TYPE), Box::new(DataType::BinaryView)),
                cast_options,
            )
        }
        FixedSizeBinary(byte_size) => {
            pack_byte_to_fixed_size_dictionary::<K>(array, cast_options, byte_size)
        }
        Struct(_) => pack_struct_to_dictionary::<K>(array, dict_value_type, cast_options),
        _ => Err(ArrowError::CastError(format!(
            "Unsupported output type for dictionary packing: {dict_value_type}"
        ))),
    }
}

/// Wrap a struct-valued array as a `DictionaryArray<K, Struct>` with identity
/// keys `[0, 1, ..., len-1]`. Unlike the primitive / byte packers above, no
/// deduplication is performed, since struct values have no general hash/equality
/// builder in arrow-rs.
///
/// Each child field of the source is recursively cast to the matching field of
/// `dict_value_type` via `cast_with_options` before keys are emitted. If any
/// child cast fails, the whole pack fails, the same contract as the primitive
/// packers above.
fn pack_struct_to_dictionary<K: ArrowDictionaryKeyType>(
    array: &dyn Array,
    dict_value_type: &DataType,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    let cast_values = cast_with_options(array, dict_value_type, cast_options)?;
    let len = cast_values.len();

    // Identity keys `[0, 1, ..., len-1]`, with null entries wherever the
    // source row is null so the dictionary's logical null mask matches.
    let mut builder = PrimitiveBuilder::<K>::with_capacity(len);
    for i in 0..len {
        if cast_values.is_null(i) {
            builder.append_null();
        } else {
            let key = K::Native::from_usize(i).ok_or_else(|| {
                ArrowError::CastError(format!(
                    "Cannot fit {len} dictionary keys in {:?}",
                    K::DATA_TYPE,
                ))
            })?;
            builder.append_value(key);
        }
    }
    let keys = builder.finish();

    Ok(Arc::new(DictionaryArray::<K>::try_new(keys, cast_values)?))
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

pub(crate) fn pack_decimal_to_dictionary<K, D>(
    array: &dyn Array,
    dict_value_type: &DataType,
    precision: u8,
    scale: i8,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError>
where
    K: ArrowDictionaryKeyType,
    D: DecimalType + ArrowPrimitiveType,
{
    let dict = pack_numeric_to_dictionary::<K, D>(array, dict_value_type, cast_options)?;
    let dict = dict.as_dictionary::<K>();
    let typed = dict.downcast_dict::<PrimitiveArray<D>>().ok_or_else(|| {
        ArrowError::ComputeError(format!(
            "Internal Error: Cannot cast dict to {}Array",
            D::PREFIX
        ))
    })?;
    let value = typed
        .values()
        .clone()
        .with_precision_and_scale(precision, scale)?;
    Ok(Arc::new(dict.with_values(Arc::new(value))))
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

// Packs the data as a GenericByteDictionaryBuilder, if possible, with the
// key types of K
pub(crate) fn pack_byte_to_fixed_size_dictionary<K>(
    array: &dyn Array,
    cast_options: &CastOptions,
    byte_width: i32,
) -> Result<ArrayRef, ArrowError>
where
    K: ArrowDictionaryKeyType,
{
    let cast_values =
        cast_with_options(array, &DataType::FixedSizeBinary(byte_width), cast_options)?;
    let values = cast_values
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| {
            ArrowError::ComputeError("Internal Error: Cannot cast to GenericByteArray".to_string())
        })?;
    let mut b = FixedSizeBinaryDictionaryBuilder::<K>::with_capacity(1024, 1024, byte_width);

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

#[cfg(test)]
mod tests {
    use super::*;

    /// Casting a dictionary to a view type has two implementations: building one view per row
    /// directly against the values buffer, and `unpack_dictionary`. Which one runs depends on
    /// how the row count compares to the dictionary size, so these helpers pin both branches of
    /// that choice for each arm.
    ///
    /// `values` must have 6 entries; the returned key sets sit either side of the threshold.
    fn keys_taking_direct_path() -> Int32Array {
        // 2 keys < 6/2 values -> views are built directly per row
        Int32Array::from_iter([Some(0), Some(3)])
    }

    fn keys_taking_unpack_path() -> Int32Array {
        // 6 keys >= 6/2 values -> unpack_dictionary
        Int32Array::from_iter([Some(0), Some(3), None, Some(1), Some(2), Some(0)])
    }

    fn cast_dict(values: ArrayRef, keys: Int32Array, to_type: &DataType) -> ArrayRef {
        let dict = DictionaryArray::<Int32Type>::try_new(keys, values).unwrap();
        assert!(can_cast_types(dict.data_type(), to_type));
        let casted = cast(&dict, to_type).unwrap();
        assert_eq!(casted.data_type(), to_type);
        casted
    }

    #[test]
    fn test_dict_to_view_both_paths_agree() {
        // Every arm, exercised through both implementations.
        let long = "a value over twelve bytes";
        let expect_direct = vec![Some("aa"), Some("dd")];
        let expect_unpack = vec![
            Some("aa"),
            Some("dd"),
            None,
            Some("bb"),
            Some(long),
            Some("aa"),
        ];
        fn as_bytes<'a>(v: &[Option<&'a str>]) -> Vec<Option<&'a [u8]>> {
            v.iter().map(|s| s.map(|s| s.as_bytes())).collect()
        }

        let utf8: ArrayRef = Arc::new(StringArray::from(vec!["aa", "bb", long, "dd", "ee", "ff"]));
        let large_utf8: ArrayRef = Arc::new(LargeStringArray::from(vec![
            "aa", "bb", long, "dd", "ee", "ff",
        ]));
        let binary: ArrayRef = Arc::new(BinaryArray::from_iter_values([
            b"aa".as_slice(),
            b"bb",
            long.as_bytes(),
            b"dd",
            b"ee",
            b"ff",
        ]));
        let large_binary: ArrayRef = Arc::new(LargeBinaryArray::from_iter_values([
            b"aa".as_slice(),
            b"bb",
            long.as_bytes(),
            b"dd",
            b"ee",
            b"ff",
        ]));

        // every source type that can reach Utf8View
        for (label, values, to_type) in [
            ("Utf8->Utf8View", utf8.clone(), DataType::Utf8View),
            (
                "LargeUtf8->Utf8View",
                large_utf8.clone(),
                DataType::Utf8View,
            ),
            ("Binary->Utf8View", binary.clone(), DataType::Utf8View),
            (
                "LargeBinary->Utf8View",
                large_binary.clone(),
                DataType::Utf8View,
            ),
        ] {
            let direct = cast_dict(values.clone(), keys_taking_direct_path(), &to_type);
            assert_eq!(
                direct.as_string_view().iter().collect::<Vec<_>>(),
                expect_direct,
                "{label} (direct path)"
            );
            let unpacked = cast_dict(values, keys_taking_unpack_path(), &to_type);
            assert_eq!(
                unpacked.as_string_view().iter().collect::<Vec<_>>(),
                expect_unpack,
                "{label} (unpack path)"
            );
        }

        // every source type that can reach BinaryView
        for (label, values, to_type) in [
            ("Utf8->BinaryView", utf8, DataType::BinaryView),
            ("LargeUtf8->BinaryView", large_utf8, DataType::BinaryView),
            ("Binary->BinaryView", binary, DataType::BinaryView),
            (
                "LargeBinary->BinaryView",
                large_binary,
                DataType::BinaryView,
            ),
        ] {
            let direct = cast_dict(values.clone(), keys_taking_direct_path(), &to_type);
            assert_eq!(
                direct.as_binary_view().iter().collect::<Vec<_>>(),
                as_bytes(&expect_direct),
                "{label} (direct path)"
            );
            let unpacked = cast_dict(values, keys_taking_unpack_path(), &to_type);
            assert_eq!(
                unpacked.as_binary_view().iter().collect::<Vec<_>>(),
                as_bytes(&expect_unpack),
                "{label} (unpack path)"
            );
        }
    }

    #[test]
    fn test_dict_binary_to_utf8view_invalid_utf8_both_paths() {
        // Invalid UTF-8 must behave identically whichever implementation runs, for both
        // Binary and LargeBinary sources.
        let mut b32 = BinaryBuilder::new();
        let mut b64 = GenericBinaryBuilder::<i64>::new();
        for v in [b"aa".as_slice(), b"bb", &[0xFF, 0xFE], b"dd", b"ee", b"ff"] {
            b32.append_value(v);
            b64.append_value(v);
        }
        let binary: ArrayRef = Arc::new(b32.finish());
        let large_binary: ArrayRef = Arc::new(b64.finish());

        let strict = CastOptions {
            safe: false,
            ..Default::default()
        };
        let safe = CastOptions {
            safe: true,
            ..Default::default()
        };

        for values in [binary, large_binary] {
            for keys in [keys_taking_direct_path(), keys_taking_unpack_path()] {
                let dict = DictionaryArray::<Int32Type>::try_new(keys, values.clone()).unwrap();

                let err = cast_with_options(&dict, &DataType::Utf8View, &strict).unwrap_err();
                assert!(
                    matches!(err, ArrowError::InvalidArgumentError(_)),
                    "expected InvalidArgumentError, got {err:?}"
                );

                let casted = cast_with_options(&dict, &DataType::Utf8View, &safe).unwrap();
                let got: Vec<_> = casted.as_string_view().iter().collect();
                // only rows whose key points at the invalid value are nullified
                assert!(got.iter().all(|v| *v != Some("\u{FFFD}")));
                assert_eq!(got[0], Some("aa"));
            }
        }
    }

    #[test]
    fn test_dict_large_utf8_to_utf8view() {
        // Dict<Int8, LargeUtf8> -> Utf8View, exercising the offset-fit check
        let values = LargeStringArray::from(vec![
            Some("hello"),
            Some("large payload over 12 bytes"),
            Some("hello"),
        ]);
        let keys = Int8Array::from_iter([Some(0), Some(1), None, Some(0), Some(1)]);
        let dict_array = DictionaryArray::<Int8Type>::try_new(keys, Arc::new(values)).unwrap();

        assert!(can_cast_types(dict_array.data_type(), &DataType::Utf8View));
        let casted = cast(&dict_array, &DataType::Utf8View).unwrap();
        assert_eq!(casted.data_type(), &DataType::Utf8View);

        let expected = StringViewArray::from(vec![
            Some("hello"),
            Some("large payload over 12 bytes"),
            None,
            Some("hello"),
            Some("large payload over 12 bytes"),
        ]);
        assert_eq!(casted.as_ref(), &expected);
    }

    #[test]
    fn test_dict_large_binary_to_binary_view() {
        // Dict<Int8, LargeBinary> -> BinaryView, exercising the offset-fit check
        let mut builder = GenericBinaryBuilder::<i64>::new();
        builder.append_value(b"hello");
        builder.append_value(b"world");
        let values = builder.finish();

        let keys = Int8Array::from_iter([Some(0), Some(1), None, Some(0)]);
        let dict_array = DictionaryArray::<Int8Type>::try_new(keys, Arc::new(values)).unwrap();

        assert!(can_cast_types(
            dict_array.data_type(),
            &DataType::BinaryView
        ));
        let casted = cast(&dict_array, &DataType::BinaryView).unwrap();
        assert_eq!(casted.data_type(), &DataType::BinaryView);

        let expected = BinaryViewArray::from_iter(vec![
            Some(b"hello".as_slice()),
            Some(b"world".as_slice()),
            None,
            Some(b"hello".as_slice()),
        ]);
        assert_eq!(casted.as_ref(), &expected);
    }

    #[test]
    fn test_dict_utf8_to_binary_view() {
        // Dict<Int8, Utf8> -> BinaryView cross cast: UTF-8 strings are always valid binary
        let data = [
            Some("hello"),
            Some("repeated"),
            None,
            Some("large payload over 12 bytes"),
            Some("repeated"),
        ];
        let values = StringArray::from(data.to_vec());
        let keys = Int8Array::from_iter([Some(1), Some(0), None, Some(3), None, Some(1), Some(4)]);
        let dict_array = DictionaryArray::<Int8Type>::try_new(keys, Arc::new(values)).unwrap();

        assert!(can_cast_types(
            dict_array.data_type(),
            &DataType::BinaryView
        ));
        let casted = cast(&dict_array, &DataType::BinaryView).unwrap();
        assert_eq!(casted.data_type(), &DataType::BinaryView);

        let expected = BinaryViewArray::from_iter(vec![
            data[1], data[0], None, data[3], None, data[1], data[4],
        ]);
        assert_eq!(casted.as_ref(), &expected);
    }

    #[test]
    fn test_dict_binary_to_utf8view_valid() {
        // Dict<Int8, Binary> -> Utf8View cross cast: all values are valid UTF-8
        let values = BinaryArray::from_iter_values([b"hello".as_slice(), b"world", b"foo"]);
        let keys = Int8Array::from_iter([Some(0), Some(1), None, Some(0), Some(2)]);
        let dict_array = DictionaryArray::<Int8Type>::try_new(keys, Arc::new(values)).unwrap();

        assert!(can_cast_types(dict_array.data_type(), &DataType::Utf8View));
        let casted = cast(&dict_array, &DataType::Utf8View).unwrap();
        assert_eq!(casted.data_type(), &DataType::Utf8View);

        let result: Vec<_> = casted.as_string_view().iter().collect();
        assert_eq!(
            result,
            vec![
                Some("hello"),
                Some("world"),
                None,
                Some("hello"),
                Some("foo")
            ]
        );
    }

    #[test]
    fn test_dict_binary_to_utf8view_invalid_utf8_strict() {
        // Dict<Int8, Binary> -> Utf8View with invalid UTF-8: safe=false returns an error
        let mut builder = BinaryBuilder::new();
        builder.append_value(b"valid");
        builder.append_value([0xFF]); // invalid UTF-8
        builder.append_value(b"also valid");
        let values = builder.finish();

        let keys = Int8Array::from_iter([Some(0), Some(1), Some(2)]);
        let dict_array = DictionaryArray::<Int8Type>::try_new(keys, Arc::new(values)).unwrap();

        let strict = CastOptions {
            safe: false,
            ..Default::default()
        };
        let err = cast_with_options(&dict_array, &DataType::Utf8View, &strict).unwrap_err();
        assert!(
            matches!(err, ArrowError::InvalidArgumentError(_)),
            "expected InvalidArgumentError, got {err:?}"
        );
    }

    #[test]
    fn test_dict_binary_to_utf8view_invalid_utf8_safe() {
        // Dict<Int8, Binary> -> Utf8View with invalid UTF-8: safe=true nullifies affected rows
        let mut builder = BinaryBuilder::new();
        builder.append_value(b"valid");
        builder.append_value([0xFF]); // invalid UTF-8 - dict index 1
        builder.append_value(b"also valid");
        let values = builder.finish();

        // keys: 0, 1, 2, 1, 0  -> "valid", INVALID, "also valid", INVALID, "valid"
        let keys = Int8Array::from_iter([Some(0), Some(1), Some(2), Some(1), Some(0)]);
        let dict_array = DictionaryArray::<Int8Type>::try_new(keys, Arc::new(values)).unwrap();

        let safe = CastOptions {
            safe: true,
            ..Default::default()
        };
        let casted = cast_with_options(&dict_array, &DataType::Utf8View, &safe).unwrap();
        assert_eq!(casted.data_type(), &DataType::Utf8View);

        let result: Vec<_> = casted.as_string_view().iter().collect();
        assert_eq!(
            result,
            vec![Some("valid"), None, Some("also valid"), None, Some("valid")]
        );
    }
}
