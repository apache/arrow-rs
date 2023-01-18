use crate::interleave::interleave;
use arrow_array::builder::BooleanBufferBuilder;
use arrow_array::cast::{as_generic_binary_array, as_largestring_array, as_string_array};
use arrow_array::types::{ArrowDictionaryKeyType, ByteArrayType};
use arrow_array::{Array, ArrayRef, DictionaryArray, GenericByteArray};
use arrow_buffer::{ArrowNativeType, Buffer, MutableBuffer};
use arrow_data::bit_iterator::BitIndexIterator;
use arrow_schema::{ArrowError, DataType};
use std::collections::hash_map::Entry;
use std::collections::HashMap;

/// Given an array of dictionaries and an optional row mask compute a values array
/// containing all unique, reference values, along with mappings from the [`DictionaryArray`]
/// keys to the new keys within this values array
pub fn merge_dictionaries<K: ArrowDictionaryKeyType>(
    dictionaries: &[(&DictionaryArray<K>, Option<&[u8]>)],
) -> Result<(Vec<Vec<K::Native>>, ArrayRef), ArrowError> {
    let mut num_values = 0;

    let mut values = Vec::with_capacity(dictionaries.len());
    let mut value_slices = Vec::with_capacity(dictionaries.len());

    for (dictionary, key_mask) in dictionaries {
        let values_mask = match key_mask {
            Some(key_mask) => {
                let iter = BitIndexIterator::new(key_mask, 0, dictionary.len());
                compute_values_mask(dictionary, iter)
            }
            None => compute_values_mask(dictionary, 0..dictionary.len()),
        };
        let v = dictionary.values().as_ref();
        num_values += v.len();
        value_slices.push(get_masked_values(v, &values_mask));
        values.push(v)
    }

    // Map from value to new index
    let mut interner = HashMap::with_capacity(num_values);
    // Interleave indices for new values array
    let mut indices = Vec::with_capacity(num_values);

    // Compute the mapping for each dictionary
    let mappings = dictionaries
        .iter()
        .enumerate()
        .zip(value_slices)
        .map(|((dictionary_idx, (dictionary, _)), values)| {
            let zero = K::Native::from_usize(0).unwrap();
            let mut mapping = vec![zero; dictionary.values().len()];
            for (value_idx, value) in values {
                mapping[value_idx] = match interner.entry(value) {
                    Entry::Vacant(v) => {
                        let idx = K::Native::from_usize(indices.len())
                            .ok_or_else(|| ArrowError::DictionaryKeyOverflowError)?;
                        indices.push((dictionary_idx, value_idx));
                        v.insert(idx);
                        idx
                    }
                    Entry::Occupied(o) => *o.get(),
                }
            }
            Ok(mapping)
        })
        .collect::<Result<Vec<_>, ArrowError>>()?;

    let array = interleave(&values, &indices)?;
    Ok((mappings, array))
}

/// Return a mask identifying the values that are referenced by keys in `dictionary`
/// at the positions indicated by `selection`
fn compute_values_mask<K, I>(dictionary: &DictionaryArray<K>, selection: I) -> Buffer
where
    K: ArrowDictionaryKeyType,
    I: IntoIterator<Item = usize>,
{
    let len = dictionary.values().len();
    let mut builder =
        BooleanBufferBuilder::new_from_buffer(MutableBuffer::new_null(len), len);

    let keys = dictionary.keys();

    for i in selection {
        if keys.is_valid(i) {
            let key = keys.values()[i];
            builder.set_bit(key.as_usize(), true)
        }
    }
    builder.finish()
}

/// Return a Vec containing for each set index in `mask`, the index and byte value of that index
fn get_masked_values<'a>(array: &'a dyn Array, mask: &Buffer) -> Vec<(usize, &'a [u8])> {
    match array.data_type() {
        DataType::Utf8 => masked_bytes(as_string_array(array), mask),
        DataType::LargeUtf8 => masked_bytes(as_largestring_array(array), mask),
        DataType::Binary => masked_bytes(as_generic_binary_array::<i32>(array), mask),
        DataType::LargeBinary => {
            masked_bytes(as_generic_binary_array::<i64>(array), mask)
        }
        _ => unimplemented!(),
    }
}

/// Compute [`get_masked_values`] for a [`GenericByteArray`]
///
/// Note: this does not check the null mask and will return values contained in null slots
fn masked_bytes<'a, T: ByteArrayType>(
    array: &'a GenericByteArray<T>,
    mask: &Buffer,
) -> Vec<(usize, &'a [u8])> {
    let cap = mask.count_set_bits_offset(0, array.len());
    let mut out = Vec::with_capacity(cap);
    for idx in BitIndexIterator::new(mask.as_slice(), 0, array.len()) {
        out.push((idx, array.value(idx).as_ref()))
    }
    out
}

#[cfg(test)]
mod tests {
    use crate::dictionary::merge_dictionaries;
    use arrow_array::cast::{as_dictionary_array, as_string_array};
    use arrow_array::types::Int32Type;
    use arrow_array::{Array, DictionaryArray};
    use arrow_buffer::Buffer;

    #[test]
    fn test_merge_strings() {
        let a =
            DictionaryArray::<Int32Type>::from_iter(["a", "b", "a", "b", "d", "c", "e"]);
        let b = DictionaryArray::<Int32Type>::from_iter(["c", "f", "c", "d", "a", "d"]);
        let (mappings, combined) = merge_dictionaries(&[(&a, None), (&b, None)]).unwrap();

        let values = as_string_array(combined.as_ref());
        let actual: Vec<_> = values.iter().map(Option::unwrap).collect();
        assert_eq!(&actual, &["a", "b", "d", "c", "e", "f"]);

        assert_eq!(mappings.len(), 2);
        assert_eq!(&mappings[0], &[0, 1, 2, 3, 4]);
        assert_eq!(&mappings[1], &[3, 5, 2, 0]);

        let a_slice = a.slice(1, 4);
        let (mappings, combined) = merge_dictionaries(&[
            (as_dictionary_array::<Int32Type>(a_slice.as_ref()), None),
            (&b, None),
        ])
        .unwrap();

        let values = as_string_array(combined.as_ref());
        let actual: Vec<_> = values.iter().map(Option::unwrap).collect();
        assert_eq!(&actual, &["a", "b", "d", "c", "f"]);

        assert_eq!(mappings.len(), 2);
        assert_eq!(&mappings[0], &[0, 1, 2, 0, 0]);
        assert_eq!(&mappings[1], &[3, 4, 2, 0]);

        // Mask out only ["b", "b", "d"] from a
        let mask = Buffer::from_iter([false, true, false, true, true, false, false]);
        let (mappings, combined) =
            merge_dictionaries(&[(&a, Some(&mask)), (&b, None)]).unwrap();

        let values = as_string_array(combined.as_ref());
        let actual: Vec<_> = values.iter().map(Option::unwrap).collect();
        assert_eq!(&actual, &["b", "d", "c", "f", "a"]);

        assert_eq!(mappings.len(), 2);
        assert_eq!(&mappings[0], &[0, 0, 1, 0, 0]);
        assert_eq!(&mappings[1], &[2, 3, 1, 4]);
    }
}
