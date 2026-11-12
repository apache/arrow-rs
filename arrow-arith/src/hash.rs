// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Hashing kernels for Arrow arrays.

use std::hash::{BuildHasher, Hash};
use std::mem::size_of;

use arrow_array::cast::AsArray;
use arrow_array::types::{
    ArrowDictionaryKeyType, ArrowPrimitiveType, IntervalDayTime, IntervalMonthDayNano,
};
use arrow_array::{
    Array, ArrayAccessor, BooleanArray, DictionaryArray, FixedSizeBinaryArray, GenericBinaryArray,
    GenericByteViewArray, GenericStringArray, PrimitiveArray, downcast_dictionary_array,
};
use arrow_buffer::{ArrowNativeType, MutableBuffer, ScalarBuffer, i256};
use arrow_schema::{ArrowError, DataType};

/// Combines a hash for a new column with the hash accumulated for prior columns.
#[inline]
pub fn combine_hashes(new_hash: u64, previous_hash: u64) -> u64 {
    let hash = (17 * 37u64).wrapping_add(new_hash);
    hash.wrapping_mul(37).wrapping_add(previous_hash)
}

/// Creates one hash per row from `arrays`.
///
/// The caller supplies the hash builder and output storage. Supplying the hash
/// builder avoids defining a hash algorithm or stability guarantee as part of
/// this API. All input arrays and `hashes` must have the same length.
/// Hashes should not be persisted or exchanged unless the caller controls the
/// hash builder and its compatibility across versions and platforms.
///
/// Nulls have a fixed hash derived from `1_i32`. Floating-point values are
/// hashed by bit pattern after normalizing `-0.0` to `+0.0`; NaN payloads are
/// preserved. Dictionary values are hashed once and scattered by key. Nested
/// arrays other than dictionaries are not yet supported.
///
/// # Example
///
/// ```
/// use std::collections::hash_map::RandomState;
/// use arrow_array::{Array, Int32Array, StringArray};
/// use arrow_arith::hash::create_hashes;
///
/// let ids = Int32Array::from(vec![1, 2, 3]);
/// let labels = StringArray::from(vec!["a", "b", "c"]);
/// let mut hashes = vec![0; ids.len()];
/// create_hashes(
///     [&ids as &dyn Array, &labels as &dyn Array],
///     &RandomState::new(),
///     &mut hashes,
/// )?;
/// # Ok::<(), arrow_schema::ArrowError>(())
/// ```
pub fn create_hashes<'a, 'b, I, S>(
    arrays: I,
    hash_builder: &S,
    hashes: &'a mut [u64],
) -> Result<&'a mut [u64], ArrowError>
where
    I: IntoIterator<Item = &'b dyn Array>,
    S: BuildHasher,
{
    let mut arrays = arrays.into_iter().peekable();
    if arrays.peek().is_none() {
        return Err(ArrowError::InvalidArgumentError(
            "create_hashes requires at least one array".to_string(),
        ));
    }

    for (column, array) in arrays.enumerate() {
        if array.len() != hashes.len() {
            return Err(ArrowError::InvalidArgumentError(format!(
                "hash output has length {}, but column {column} has length {}",
                hashes.len(),
                array.len()
            )));
        }
        hash_array(array, hash_builder, hashes, column != 0)?;
    }
    Ok(hashes)
}

/// Creates one hash per row in cache-aligned Arrow storage.
///
/// The returned [`ScalarBuffer`] uses [`MutableBuffer`] allocation, allowing
/// downstream architecture-specific kernels to rely on Arrow's allocation
/// alignment.
pub fn create_hashes_buffer<'a, I, S>(
    arrays: I,
    hash_builder: &S,
) -> Result<ScalarBuffer<u64>, ArrowError>
where
    I: IntoIterator<Item = &'a dyn Array>,
    S: BuildHasher,
{
    let mut arrays = arrays.into_iter().peekable();
    let len = arrays.peek().map(|array| array.len()).ok_or_else(|| {
        ArrowError::InvalidArgumentError(
            "create_hashes_buffer requires at least one array".to_string(),
        )
    })?;
    let byte_len = len
        .checked_mul(size_of::<u64>())
        .ok_or_else(|| ArrowError::InvalidArgumentError("hash output size overflow".to_string()))?;
    let mut buffer = MutableBuffer::try_from_len_zeroed(byte_len)
        .map_err(|error| ArrowError::MemoryError(error.to_string()))?;
    create_hashes(arrays, hash_builder, buffer.typed_data_mut::<u64>())?;
    Ok(buffer.into())
}

trait HashValue {
    fn hash_one<S: BuildHasher>(self, hash_builder: &S) -> u64;
}

macro_rules! impl_hash_value {
    ($($t:ty),+ $(,)?) => {
        $(impl HashValue for $t {
            #[inline]
            fn hash_one<S: BuildHasher>(self, hash_builder: &S) -> u64 {
                hash_builder.hash_one(self)
            }
        })+
    };
}

impl_hash_value!(
    i8,
    i16,
    i32,
    i64,
    i128,
    i256,
    u8,
    u16,
    u32,
    u64,
    IntervalDayTime,
    IntervalMonthDayNano,
);

macro_rules! impl_float_hash_value {
    ($(($t:ty, $zero:expr)),+ $(,)?) => {
        $(impl HashValue for $t {
            #[inline]
            fn hash_one<S: BuildHasher>(self, hash_builder: &S) -> u64 {
                let normalized = if self == $zero { $zero } else { self };
                hash_builder.hash_one(normalized.to_bits())
            }
        })+
    };
}

impl_float_hash_value!((half::f16, half::f16::ZERO), (f32, 0.0), (f64, 0.0));

#[inline]
fn write_hash(output: &mut u64, value_hash: u64, rehash: bool) {
    if rehash {
        *output = combine_hashes(value_hash, *output);
    } else {
        *output = value_hash;
    }
}

fn hash_nulls<S: BuildHasher>(hash_builder: &S, hashes: &mut [u64], rehash: bool) {
    let null_hash = hash_builder.hash_one(1_i32);
    for output in hashes {
        write_hash(output, null_hash, rehash);
    }
}

fn hash_primitive<T, S>(
    array: &PrimitiveArray<T>,
    hash_builder: &S,
    hashes: &mut [u64],
    rehash: bool,
) where
    T: ArrowPrimitiveType,
    T::Native: HashValue,
    S: BuildHasher,
{
    const FRESH_LANES: usize = 16;
    const REHASH_LANES: usize = 8;

    if array.null_count() == 0 {
        if rehash {
            let (value_chunks, value_tail) = array.values().as_ref().as_chunks::<REHASH_LANES>();
            let (hash_chunks, hash_tail) = hashes.as_chunks_mut::<REHASH_LANES>();
            for (output, values) in hash_chunks.iter_mut().zip(value_chunks) {
                *output = std::array::from_fn(|lane| {
                    combine_hashes(values[lane].hash_one(hash_builder), output[lane])
                });
            }
            for (output, &value) in hash_tail.iter_mut().zip(value_tail) {
                *output = combine_hashes(value.hash_one(hash_builder), *output);
            }
        } else {
            let (value_chunks, value_tail) = array.values().as_ref().as_chunks::<FRESH_LANES>();
            let (hash_chunks, hash_tail) = hashes.as_chunks_mut::<FRESH_LANES>();
            for (output, values) in hash_chunks.iter_mut().zip(value_chunks) {
                *output = values.map(|value| value.hash_one(hash_builder));
            }
            for (output, &value) in hash_tail.iter_mut().zip(value_tail) {
                *output = value.hash_one(hash_builder);
            }
        }
        return;
    }

    let null_hash = hash_builder.hash_one(1_i32);
    for (row, output) in hashes.iter_mut().enumerate() {
        let value_hash = if array.is_valid(row) {
            // SAFETY: row is bounded by hashes, whose length was checked against array.
            unsafe { array.value_unchecked(row) }.hash_one(hash_builder)
        } else {
            null_hash
        };
        write_hash(output, value_hash, rehash);
    }
}

fn hash_boolean<S: BuildHasher>(
    array: &BooleanArray,
    hash_builder: &S,
    hashes: &mut [u64],
    rehash: bool,
) {
    let true_hash = hash_builder.hash_one(true);
    let false_hash = hash_builder.hash_one(false);
    let null_hash = hash_builder.hash_one(1_i32);
    for (row, output) in hashes.iter_mut().enumerate() {
        let value_hash = if array.is_null(row) {
            null_hash
        } else if array.value(row) {
            true_hash
        } else {
            false_hash
        };
        write_hash(output, value_hash, rehash);
    }
}

fn hash_accessor<'a, A, S>(array: &'a A, hash_builder: &S, hashes: &mut [u64], rehash: bool)
where
    A: Array,
    &'a A: ArrayAccessor,
    <&'a A as ArrayAccessor>::Item: Hash,
    S: BuildHasher,
{
    let null_hash = hash_builder.hash_one(1_i32);
    if array.null_count() == 0 {
        for (row, output) in hashes.iter_mut().enumerate() {
            // SAFETY: row is bounded by hashes, whose length was checked against array.
            let value_hash = hash_builder.hash_one(unsafe { array.value_unchecked(row) });
            write_hash(output, value_hash, rehash);
        }
        return;
    }
    for (row, output) in hashes.iter_mut().enumerate() {
        let value_hash = if array.is_null(row) {
            null_hash
        } else {
            // SAFETY: row is bounded by hashes, whose length was checked against array.
            hash_builder.hash_one(unsafe { array.value_unchecked(row) })
        };
        write_hash(output, value_hash, rehash);
    }
}

fn hash_dictionary<K, S>(
    array: &DictionaryArray<K>,
    hash_builder: &S,
    hashes: &mut [u64],
    rehash: bool,
) -> Result<(), ArrowError>
where
    K: ArrowDictionaryKeyType,
    S: BuildHasher,
{
    let mut value_hashes = vec![0; array.values().len()];
    hash_array(
        array.values().as_ref(),
        hash_builder,
        &mut value_hashes,
        false,
    )?;
    let null_hash = hash_builder.hash_one(1_i32);
    for (row, output) in hashes.iter_mut().enumerate() {
        let value_hash = if array.is_null(row) {
            null_hash
        } else {
            let key = array.keys().value(row).as_usize();
            value_hashes[key]
        };
        write_hash(output, value_hash, rehash);
    }
    Ok(())
}

fn hash_array<S: BuildHasher>(
    array: &dyn Array,
    hash_builder: &S,
    hashes: &mut [u64],
    rehash: bool,
) -> Result<(), ArrowError> {
    match array.data_type() {
        DataType::Null => {
            hash_nulls(hash_builder, hashes, rehash);
            Ok(())
        }
        DataType::Boolean => {
            hash_boolean(array.as_boolean(), hash_builder, hashes, rehash);
            Ok(())
        }
        data_type if data_type.is_primitive() => arrow_array::downcast_primitive_array!(
            array => {
                hash_primitive(array, hash_builder, hashes, rehash);
                Ok(())
            }
            _ => unreachable!("primitive dispatch called for non-primitive type")
        ),
        DataType::Utf8 => {
            hash_accessor::<GenericStringArray<i32>, _>(
                array.as_string::<i32>(),
                hash_builder,
                hashes,
                rehash,
            );
            Ok(())
        }
        DataType::LargeUtf8 => {
            hash_accessor::<GenericStringArray<i64>, _>(
                array.as_string::<i64>(),
                hash_builder,
                hashes,
                rehash,
            );
            Ok(())
        }
        DataType::Binary => {
            hash_accessor::<GenericBinaryArray<i32>, _>(
                array.as_binary::<i32>(),
                hash_builder,
                hashes,
                rehash,
            );
            Ok(())
        }
        DataType::LargeBinary => {
            hash_accessor::<GenericBinaryArray<i64>, _>(
                array.as_binary::<i64>(),
                hash_builder,
                hashes,
                rehash,
            );
            Ok(())
        }
        DataType::Utf8View => {
            hash_accessor::<GenericByteViewArray<arrow_array::types::StringViewType>, _>(
                array.as_string_view(),
                hash_builder,
                hashes,
                rehash,
            );
            Ok(())
        }
        DataType::BinaryView => {
            hash_accessor::<GenericByteViewArray<arrow_array::types::BinaryViewType>, _>(
                array.as_binary_view(),
                hash_builder,
                hashes,
                rehash,
            );
            Ok(())
        }
        DataType::FixedSizeBinary(_) => {
            hash_accessor::<FixedSizeBinaryArray, _>(
                array.as_fixed_size_binary(),
                hash_builder,
                hashes,
                rehash,
            );
            Ok(())
        }
        DataType::Dictionary(_, _) => arrow_array::downcast_dictionary_array!(
            array => hash_dictionary(array, hash_builder, hashes, rehash),
            _ => unreachable!("dictionary dispatch called for non-dictionary type")
        ),
        data_type => Err(ArrowError::NotYetImplemented(format!(
            "hashing {data_type} arrays"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::hash_map::RandomState;
    use std::sync::Arc;

    use arrow_array::types::Int8Type;
    use arrow_array::{ArrayRef, Float64Array, Int32Array, StringArray, StringViewArray};
    use arrow_buffer::alloc::ALIGNMENT;

    use super::*;

    fn expected<S: BuildHasher, T: Hash>(
        state: &S,
        values: impl IntoIterator<Item = T>,
    ) -> Vec<u64> {
        values
            .into_iter()
            .map(|value| state.hash_one(value))
            .collect()
    }

    #[test]
    fn primitive_hashes_cover_chunks_tail_nulls_and_slices() {
        let state = RandomState::new();
        let values = (0..19).map(|value| (value != 7).then_some(value));
        let array = Int32Array::from_iter(values).slice(1, 17);
        let mut actual = vec![0; array.len()];
        create_hashes([&array as &dyn Array], &state, &mut actual).unwrap();

        let expected = array
            .iter()
            .map(|value| value.map_or_else(|| state.hash_one(1_i32), |value| state.hash_one(value)))
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }

    #[test]
    fn hashes_multiple_columns_in_order() {
        let state = RandomState::new();
        let left = Int32Array::from(vec![1, 2, 3]);
        let right = StringArray::from(vec![Some("a"), None, Some("c")]);
        let mut actual = vec![0; 3];
        create_hashes(
            [&left as &dyn Array, &right as &dyn Array],
            &state,
            &mut actual,
        )
        .unwrap();

        let left = expected(&state, [1, 2, 3]);
        let right = [
            state.hash_one("a"),
            state.hash_one(1_i32),
            state.hash_one("c"),
        ];
        let expected = left
            .into_iter()
            .zip(right)
            .map(|(previous, new)| combine_hashes(new, previous))
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }

    #[test]
    fn dictionary_hashes_values_once_and_preserves_null_semantics() {
        let state = RandomState::new();
        let keys = Int32Array::from(vec![Some(2), None, Some(0), Some(1)]);
        let values: ArrayRef = Arc::new(StringArray::from(vec![
            Some("a"),
            None,
            Some("c"),
            Some("unused"),
        ]));
        let dictionary = DictionaryArray::<Int8Type>::try_new(
            keys.unary::<_, Int8Type>(|key| key as i8),
            values,
        )
        .unwrap();
        let mut actual = vec![0; dictionary.len()];
        create_hashes([&dictionary as &dyn Array], &state, &mut actual).unwrap();

        assert_eq!(
            actual,
            vec![
                state.hash_one("c"),
                state.hash_one(1_i32),
                state.hash_one("a"),
                state.hash_one(1_i32),
            ]
        );
    }

    #[test]
    fn byte_views_hash_logical_values() {
        let state = RandomState::new();
        let array = StringViewArray::from(vec![
            Some("short"),
            Some("a value longer than twelve bytes"),
            None,
        ]);
        let mut actual = vec![0; array.len()];
        create_hashes([&array as &dyn Array], &state, &mut actual).unwrap();
        assert_eq!(
            actual,
            vec![
                state.hash_one("short"),
                state.hash_one("a value longer than twelve bytes"),
                state.hash_one(1_i32),
            ]
        );
    }

    #[test]
    fn floats_normalize_negative_zero_and_preserve_nan_payloads() {
        let state = RandomState::new();
        let array = Float64Array::from(vec![0.0, -0.0, f64::from_bits(0x7ff8_0000_0000_0001)]);
        let mut actual = vec![0; array.len()];
        create_hashes([&array as &dyn Array], &state, &mut actual).unwrap();
        assert_eq!(actual[0], actual[1]);
        assert_eq!(actual[2], state.hash_one(0x7ff8_0000_0000_0001_u64));
    }

    #[test]
    fn allocated_hash_buffer_is_arrow_aligned() {
        let state = RandomState::new();
        let array = Int32Array::from(vec![1, 2, 3]);
        let hashes = create_hashes_buffer([&array as &dyn Array], &state).unwrap();
        assert_eq!(hashes.as_ptr().addr() % ALIGNMENT, 0);
        assert_eq!(hashes.len(), array.len());
    }

    #[test]
    fn rejects_empty_mismatched_and_unsupported_inputs() {
        let state = RandomState::new();
        let mut empty = [];
        assert!(create_hashes(std::iter::empty(), &state, &mut empty).is_err());

        let array = Int32Array::from(vec![1, 2]);
        let mut wrong_len = [0];
        assert!(create_hashes([&array as &dyn Array], &state, &mut wrong_len).is_err());

        let list =
            arrow_array::ListArray::from_iter_primitive::<arrow_array::types::Int32Type, _, _>([
                Some(vec![Some(1)]),
            ]);
        let mut output = [0];
        assert!(matches!(
            create_hashes([&list as &dyn Array], &state, &mut output),
            Err(ArrowError::NotYetImplemented(_))
        ));
    }
}
