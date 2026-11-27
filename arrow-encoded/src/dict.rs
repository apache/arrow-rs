// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::any::Any;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, DictionaryArray, downcast_integer_array, make_array};
use arrow_buffer::{ArrowNativeType, BooleanBuffer, NullBuffer};
use arrow_schema::{ArrowError, DataType, Field};
use arrow_select::take::take;

use crate::{
    EncodedArray, EncodedArrayRef, EncodingId, PlainArray, cast_to_target, from_arrow,
    validate_target,
};

/// Dictionary encoding: integer keys into an array of values, which may itself
/// be encoded.
///
/// Its [`data_type`](EncodedArray::data_type) is the type of the values. Arrow's
/// `Dictionary<K, V>` is one of the layouts it decodes to.
#[derive(Debug, Clone)]
pub struct DictArray {
    keys: ArrayRef,
    values: EncodedArrayRef,
}

impl DictArray {
    /// Id of the dictionary encoding.
    pub const ENCODING: EncodingId = EncodingId::new("arrow.dictionary");

    /// Create a dictionary from `keys` into `values`.
    ///
    /// Returns an error if `keys` is not a dictionary key type or a non-null
    /// key is out of bounds for `values`.
    pub fn try_new(keys: ArrayRef, values: EncodedArrayRef) -> Result<Self, ArrowError> {
        let num_values = values.len();
        let key_array = keys.as_ref();
        downcast_integer_array!(
            key_array => {
                for key in key_array.iter().flatten() {
                    if key.to_usize().is_none_or(|k| k >= num_values) {
                        return Err(ArrowError::InvalidArgumentError(format!(
                            "dictionary key {key:?} out of bounds for {num_values} values"
                        )));
                    }
                }
            }
            t => {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "dictionary keys must be an integer type, got {t}"
                )));
            }
        );
        Ok(Self { keys, values })
    }

    /// Wrap an Arrow dictionary array, encoding its values with [`from_arrow`].
    ///
    /// Returns an error if `array` is not a dictionary array.
    pub fn from_arrow(array: &dyn Array) -> Result<Self, ArrowError> {
        let dict = array.as_any_dictionary_opt().ok_or_else(|| {
            ArrowError::InvalidArgumentError(format!(
                "expected a dictionary array, got {}",
                array.data_type()
            ))
        })?;
        let keys = make_array(dict.keys().to_data());
        let values = from_arrow(Arc::clone(dict.values()))?;
        Ok(Self { keys, values })
    }

    /// The keys, one per element.
    pub fn keys(&self) -> &ArrayRef {
        &self.keys
    }

    /// The values the keys index into.
    pub fn values(&self) -> &EncodedArrayRef {
        &self.values
    }

    /// An Arrow dictionary array of these keys over `values`.
    fn to_dictionary(&self, values: ArrayRef) -> Result<ArrayRef, ArrowError> {
        let keys = self.keys.as_ref();
        downcast_integer_array!(
            keys => Ok(Arc::new(DictionaryArray::try_new(keys.clone(), values)?)),
            t => unreachable!("keys of type {t} were rejected on construction"),
        )
    }
}

impl EncodedArray for DictArray {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn encoding(&self) -> EncodingId {
        Self::ENCODING
    }

    fn data_type(&self) -> &DataType {
        self.values.data_type()
    }

    fn len(&self) -> usize {
        self.keys.len()
    }

    fn logical_nulls(&self) -> Option<NullBuffer> {
        let Some(value_nulls) = self.values.logical_nulls() else {
            return self.keys.logical_nulls();
        };
        let keys = self.keys.as_ref();
        let valid = downcast_integer_array!(
            keys => BooleanBuffer::collect_bool(keys.len(), |i| {
                keys.is_valid(i) && value_nulls.is_valid(keys.value(i).as_usize())
            }),
            t => unreachable!("keys of type {t} were rejected on construction"),
        );
        Some(NullBuffer::new(valid))
    }

    fn children(&self) -> Vec<EncodedArrayRef> {
        let keys = PlainArray::try_new(Arc::clone(&self.keys))
            .expect("integer keys are never a dictionary");
        vec![Arc::new(keys), Arc::clone(&self.values)]
    }

    fn slice(&self, offset: usize, length: usize) -> EncodedArrayRef {
        Arc::new(Self {
            keys: self.keys.slice(offset, length),
            values: Arc::clone(&self.values),
        })
    }

    fn decode(&self, target: Option<&Field>) -> Result<ArrayRef, ArrowError> {
        if let Some(target) = target {
            let null_count = self.logical_nulls().map_or(0, |n| n.null_count());
            validate_target(self.data_type(), null_count, target)?;
        }
        // Decode the values straight into the layout the target asks for, so
        // the result needs no further cast in the common cases.
        let value_type = match target.map(Field::data_type) {
            Some(DataType::Dictionary(_, value_type)) => Some(value_type.as_ref()),
            Some(DataType::RunEndEncoded(_, _)) | None => None,
            Some(value_type) => Some(value_type),
        };
        let value_field = value_type.map(|t| Field::new("values", t.clone(), true));
        let values = self.values.decode(value_field.as_ref())?;

        let decoded = match target.map(Field::data_type) {
            Some(DataType::Dictionary(_, _)) => self.to_dictionary(values)?,
            _ => take(values.as_ref(), self.keys.as_ref(), None)?,
        };
        cast_to_target(decoded, target)
    }

    fn get_array_memory_size(&self) -> usize {
        self.keys.get_array_memory_size() + self.values.get_array_memory_size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::types::{Int32Type, UInt16Type};
    use arrow_array::{Int8Array, Int32Array, StringArray};

    /// Keys `[0, null, 1, 2, 0]` into `["a", "bc", null]`.
    fn strings() -> DictionaryArray<Int32Type> {
        let keys = Int32Array::from(vec![Some(0), None, Some(1), Some(2), Some(0)]);
        let values = StringArray::from(vec![Some("a"), Some("bc"), None]);
        DictionaryArray::try_new(keys, Arc::new(values)).unwrap()
    }

    fn expected() -> Vec<Option<&'static str>> {
        vec![Some("a"), None, Some("bc"), None, Some("a")]
    }

    fn dict_type(key: DataType, value: DataType) -> DataType {
        DataType::Dictionary(Box::new(key), Box::new(value))
    }

    #[test]
    fn from_arrow_dictionary() {
        let encoded = from_arrow(Arc::new(strings())).unwrap();

        assert_eq!(encoded.encoding(), DictArray::ENCODING);
        assert_eq!(encoded.data_type(), &DataType::Utf8);
        assert_eq!(encoded.len(), 5);
        let nulls = encoded.logical_nulls().unwrap();
        assert_eq!(
            nulls.iter().collect::<Vec<_>>(),
            [true, false, true, false, true]
        );
        let children = encoded.children();
        assert_eq!(children.len(), 2);
        assert_eq!(children[0].data_type(), &DataType::Int32);
        assert_eq!(children[1].encoding(), PlainArray::ENCODING);
    }

    #[test]
    fn decode_without_target_gathers_values() {
        let encoded = DictArray::from_arrow(&strings()).unwrap();
        let decoded = encoded.decode(None).unwrap();
        assert_eq!(decoded.data_type(), &DataType::Utf8);
        assert_eq!(
            decoded.as_string::<i32>().iter().collect::<Vec<_>>(),
            expected()
        );
    }

    #[test]
    fn decode_to_same_dictionary_reuses_buffers() {
        let arrow = strings();
        let encoded = DictArray::from_arrow(&arrow).unwrap();
        let target = Field::new("d", arrow.data_type().clone(), true);

        let decoded = encoded.decode(Some(&target)).unwrap();
        let decoded = decoded.as_dictionary::<Int32Type>();
        assert_eq!(
            decoded.keys().values().as_ptr(),
            arrow.keys().values().as_ptr()
        );
        assert!(Arc::ptr_eq(decoded.values(), arrow.values()));
    }

    #[test]
    fn decode_to_other_layouts() {
        let encoded = DictArray::from_arrow(&strings()).unwrap();

        let view = encoded
            .decode(Some(&Field::new("d", DataType::Utf8View, true)))
            .unwrap();
        assert_eq!(view.as_string_view().iter().collect::<Vec<_>>(), expected());

        let target_type = dict_type(DataType::UInt16, DataType::Utf8View);
        let target = Field::new("d", target_type.clone(), true);
        let dict = encoded.decode(Some(&target)).unwrap();
        assert_eq!(dict.data_type(), &target_type);
        assert_eq!(
            dict.as_dictionary::<UInt16Type>().values().data_type(),
            &DataType::Utf8View
        );
        let strings = arrow_cast::cast(&dict, &DataType::Utf8).unwrap();
        assert_eq!(
            strings.as_string::<i32>().iter().collect::<Vec<_>>(),
            expected()
        );
    }

    #[test]
    fn rejects_incompatible_target() {
        let encoded = DictArray::from_arrow(&strings()).unwrap();
        let int_dict = Field::new("d", dict_type(DataType::Int32, DataType::Int64), true);
        assert!(encoded.decode(Some(&int_dict)).is_err());
        let required = Field::new("d", DataType::Utf8, false);
        assert!(encoded.decode(Some(&required)).is_err());
    }

    #[test]
    fn slice_shares_values() {
        let encoded = DictArray::from_arrow(&strings()).unwrap();
        let sliced = encoded.slice(1, 3);
        assert_eq!(sliced.len(), 3);
        let decoded = sliced.decode(None).unwrap();
        assert_eq!(
            decoded.as_string::<i32>().iter().collect::<Vec<_>>(),
            &expected()[1..4]
        );
        let sliced = sliced.as_any().downcast_ref::<DictArray>().unwrap();
        assert!(Arc::ptr_eq(sliced.values(), encoded.values()));
    }

    #[test]
    fn rejects_invalid_keys() {
        let values = from_arrow(Arc::new(StringArray::from(vec!["a", "b"]))).unwrap();
        let out_of_bounds = Arc::new(Int32Array::from(vec![0, 2]));
        assert!(DictArray::try_new(out_of_bounds, Arc::clone(&values)).is_err());
        let negative = Arc::new(Int8Array::from(vec![-1]));
        assert!(DictArray::try_new(negative, Arc::clone(&values)).is_err());
        let not_integer = Arc::new(StringArray::from(vec!["0"]));
        assert!(DictArray::try_new(not_integer, values).is_err());
    }

    #[test]
    fn smaller_than_decoded() {
        let keys = Int32Array::from_iter_values((0..10_000).map(|i| i % 4));
        let values = StringArray::from(vec!["alpha", "beta", "gamma", "delta"]);
        let arrow = DictionaryArray::try_new(keys, Arc::new(values)).unwrap();
        let encoded = DictArray::from_arrow(&arrow).unwrap();
        let decoded = encoded.decode(None).unwrap();
        assert!(encoded.get_array_memory_size() < decoded.get_array_memory_size());
    }
}
