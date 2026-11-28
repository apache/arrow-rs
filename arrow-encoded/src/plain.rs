// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::any::Any;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef};
use arrow_buffer::NullBuffer;
use arrow_schema::{ArrowError, DataType, Field};

use crate::{EncodedArray, EncodedArrayRef, EncodingId, cast_to_target};

/// The identity encoding: an ordinary Arrow array, which decodes to itself.
#[derive(Debug, Clone)]
pub struct PlainArray {
    array: ArrayRef,
}

impl PlainArray {
    /// Id of the plain encoding.
    pub const ENCODING: EncodingId = EncodingId::new("arrow.plain");

    /// Wrap `array`, or return an error if its type is or contains a dictionary.
    pub fn try_new(array: ArrayRef) -> Result<Self, ArrowError> {
        if contains_dictionary(array.data_type()) {
            return Err(ArrowError::InvalidArgumentError(format!(
                "PlainArray cannot hold {}: dictionary encoding is an encoding, not a logical type",
                array.data_type()
            )));
        }
        Ok(Self { array })
    }

    /// The wrapped Arrow array.
    pub fn array(&self) -> &ArrayRef {
        &self.array
    }

    /// Unwrap into the Arrow array.
    pub fn into_inner(self) -> ArrayRef {
        self.array
    }
}

impl EncodedArray for PlainArray {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn encoding(&self) -> EncodingId {
        Self::ENCODING
    }

    fn data_type(&self) -> &DataType {
        self.array.data_type()
    }

    fn len(&self) -> usize {
        self.array.len()
    }

    fn logical_nulls(&self) -> Option<NullBuffer> {
        self.array.logical_nulls()
    }

    fn children(&self) -> Vec<EncodedArrayRef> {
        vec![]
    }

    fn slice(&self, offset: usize, length: usize) -> EncodedArrayRef {
        Arc::new(Self {
            array: self.array.slice(offset, length),
        })
    }

    fn decode(&self, target: Option<&Field>) -> Result<ArrayRef, ArrowError> {
        cast_to_target(Arc::clone(&self.array), target)
    }

    fn get_array_memory_size(&self) -> usize {
        self.array.get_array_memory_size()
    }
}

fn contains_dictionary(data_type: &DataType) -> bool {
    match data_type {
        DataType::Dictionary(_, _) => true,
        DataType::List(field)
        | DataType::ListView(field)
        | DataType::LargeList(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => contains_dictionary(field.data_type()),
        DataType::Struct(fields) => fields.iter().any(|f| contains_dictionary(f.data_type())),
        DataType::Union(fields, _) => fields
            .iter()
            .any(|(_, f)| contains_dictionary(f.data_type())),
        DataType::RunEndEncoded(_, values) => contains_dictionary(values.data_type()),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::cast::AsArray;
    use arrow_array::types::Int32Type;
    use arrow_array::{DictionaryArray, Float64Array, ListArray, StringArray, StructArray};
    use arrow_buffer::OffsetBuffer;

    fn plain(array: impl Array + 'static) -> PlainArray {
        PlainArray::try_new(Arc::new(array)).unwrap()
    }

    #[test]
    fn decode_returns_the_wrapped_array() {
        let array: ArrayRef = Arc::new(Float64Array::from(vec![Some(1.5), None, Some(-2.0)]));
        let encoded = PlainArray::try_new(Arc::clone(&array)).unwrap();

        assert_eq!(encoded.encoding(), PlainArray::ENCODING);
        assert_eq!(encoded.data_type(), &DataType::Float64);
        assert_eq!(encoded.len(), 3);
        assert_eq!(encoded.logical_nulls(), array.logical_nulls());
        assert!(encoded.children().is_empty());
        assert_eq!(
            encoded.get_array_memory_size(),
            array.get_array_memory_size()
        );
        assert!(Arc::ptr_eq(&encoded.decode(None).unwrap(), &array));
        let same = Field::new("x", DataType::Float64, true);
        assert!(Arc::ptr_eq(&encoded.decode(Some(&same)).unwrap(), &array));
    }

    #[test]
    fn decode_to_target_layout() {
        let encoded = plain(StringArray::from(vec![Some("a"), None, Some("bc")]));

        let view = Field::new("s", DataType::Utf8View, true);
        let decoded = encoded.decode(Some(&view)).unwrap();
        assert_eq!(decoded.data_type(), &DataType::Utf8View);
        assert_eq!(
            decoded.as_string_view().iter().collect::<Vec<_>>(),
            vec![Some("a"), None, Some("bc")]
        );

        let dict_type =
            DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::LargeUtf8));
        let dict = Field::new("s", dict_type.clone(), true);
        assert_eq!(encoded.decode(Some(&dict)).unwrap().data_type(), &dict_type);
    }

    #[test]
    fn rejects_incompatible_target() {
        let encoded = plain(StringArray::from(vec![Some("a"), None]));

        let binary = Field::new("s", DataType::Binary, true);
        let err = encoded.decode(Some(&binary)).unwrap_err();
        assert!(err.to_string().contains("different logical types"), "{err}");

        let required = Field::new("s", DataType::Utf8, false);
        let err = encoded.decode(Some(&required)).unwrap_err();
        assert!(err.to_string().contains("non-nullable"), "{err}");
    }

    #[test]
    fn slice_matches_arrow_slice() {
        let encoded = plain(Float64Array::from(vec![
            Some(1.0),
            None,
            Some(3.0),
            Some(4.0),
        ]));
        let sliced = encoded.slice(1, 2);

        assert_eq!(sliced.len(), 2);
        let expected: ArrayRef = Arc::new(Float64Array::from(vec![None, Some(3.0)]));
        assert_eq!(&sliced.decode(None).unwrap(), &expected);
        assert!(sliced.as_any().downcast_ref::<PlainArray>().is_some());
    }

    #[test]
    fn rejects_dictionary() {
        let dict: DictionaryArray<Int32Type> = vec!["a", "b", "a"].into_iter().collect();
        let err = PlainArray::try_new(Arc::new(dict)).unwrap_err();
        assert!(
            err.to_string()
                .contains("dictionary encoding is an encoding")
        );
    }

    #[test]
    fn rejects_nested_dictionary() {
        let dict: DictionaryArray<Int32Type> = vec!["a", "b"].into_iter().collect();
        let dict: ArrayRef = Arc::new(dict);

        let strukt = StructArray::from(vec![(
            Arc::new(Field::new("d", dict.data_type().clone(), false)),
            Arc::clone(&dict),
        )]);
        assert!(PlainArray::try_new(Arc::new(strukt)).is_err());

        let field = Arc::new(Field::new_list_field(dict.data_type().clone(), false));
        let offsets = OffsetBuffer::from_lengths([2]);
        let list = ListArray::try_new(field, offsets, dict, None).unwrap();
        assert!(PlainArray::try_new(Arc::new(list)).is_err());
    }
}
