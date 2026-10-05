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

//! Borrowed traversal of shredded Variant rows.

use crate::VariantArray;
use crate::variant_array::{binary_array_value, validate_binary_array};
use crate::variant_scalar::{DecodePrimitive, decode_decimal, decode_timestamp};
use arrow::array::{
    Array, ArrayRef, AsArray, BinaryArray, BinaryViewArray, BooleanArray, FixedSizeBinaryArray,
    GenericListArray, GenericListViewArray, LargeBinaryArray, LargeStringArray, ListLikeArray,
    PrimitiveArray, StringArray, StringViewArray, StructArray,
};
use arrow::datatypes::{
    DataType, Date32Type, Decimal32Type, Decimal64Type, Decimal128Type, Float32Type, Float64Type,
    Int8Type, Int16Type, Int32Type, Int64Type, Time64MicrosecondType, TimeUnit,
    TimestampMicrosecondType, TimestampNanosecondType,
};
use arrow::error::{ArrowError, Result};
use indexmap::IndexMap;
use parquet_variant::{
    Variant, VariantDecimal4, VariantDecimal8, VariantDecimal16, VariantDecimalType, VariantList,
    VariantMetadata, VariantObject,
};
use std::ops::Range;

/// Prepares a decoder for reading rows without building an output array.
///
/// Rows borrow the input and this decoder. Use [`VariantRow::state`] to check validity
/// before decoding. Callers choose the output encoding, metadata dictionary, and how
/// to handle missing values.
///
/// ```
/// use parquet_variant_compute::{DecodedVariant, VariantArray, VariantRowDecoder};
/// let input = VariantArray::from_iter([Some(42i64), None]);
/// let decoder = VariantRowDecoder::try_new(&input)?;
/// assert!(matches!(decoder.row(0)?.decode()?, DecodedVariant::Residual { .. }));
/// assert!(matches!(decoder.row(1)?.decode()?, DecodedVariant::Null));
/// # Ok::<(), arrow::error::ArrowError>(())
/// ```
pub struct VariantRowDecoder<'a> {
    array: &'a VariantArray,
    root: FieldDecoder<'a>,
}

impl<'a> VariantRowDecoder<'a> {
    /// Checks the shredding schema and prepares nested decoders.
    /// Value bytes are validated on access; null parents mask their children.
    pub fn try_new(array: &'a VariantArray) -> Result<Self> {
        Ok(Self {
            array,
            root: FieldDecoder::try_new(array.inner())?,
        })
    }

    /// Borrows a row without parsing its metadata or residual bytes.
    /// Returns an error for an out-of-bounds index.
    pub fn row(&self, index: usize) -> Result<VariantRow<'_>> {
        if index >= self.array.len() {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Index {index} out of bounds for VariantArray of length {}",
                self.array.len()
            )));
        }
        // Parent nulls mask metadata as well as both value columns.
        let metadata = if self.array.is_null(index) {
            &[][..]
        } else {
            binary_array_value(self.array.metadata_column().as_ref(), index).ok_or_else(|| {
                ArrowError::InvalidArgumentError(
                    "metadata field must be a binary-like array".into(),
                )
            })?
        };
        Ok(VariantRow {
            source: RowSource::Shredded(&self.root, index),
            metadata: Metadata::Bytes(metadata),
        })
    }
}

/// Validity of a row before any residual bytes are decoded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VariantRowState {
    /// The enclosing Arrow struct is null; its children are masked.
    Null,
    /// Both value and typed_value are absent or null.
    Missing,
    /// Only value is valid. Its bytes may encode explicit Variant null.
    Residual,
    /// Only typed_value is valid.
    Typed,
    /// Both columns are valid. Decoding requires an object in each column.
    /// Other combinations return an error from [`VariantRow::decode`].
    PartiallyShredded,
}

#[derive(Clone)]
enum Metadata<'a> {
    Bytes(&'a [u8]),
    Validated(VariantMetadata<'a>),
}

impl<'a> Metadata<'a> {
    fn get(&self) -> Result<VariantMetadata<'a>> {
        match self {
            Self::Bytes(bytes) => VariantMetadata::try_new(bytes),
            Self::Validated(metadata) => Ok(metadata.clone()),
        }
    }
}

enum RowSource<'a> {
    Shredded(&'a FieldDecoder<'a>, usize),
    // The parent container has already been validated recursively.
    Residual(&'a [u8], Variant<'a, 'a>),
}

/// A borrowed value at the root, an object field, or a list element.
/// Use [`Self::state`] to inspect validity before decoding residual bytes.
pub struct VariantRow<'a> {
    source: RowSource<'a>,
    metadata: Metadata<'a>,
}

impl<'a> VariantRow<'a> {
    /// Returns physical validity without parsing metadata or residual bytes.
    pub fn state(&self) -> VariantRowState {
        match &self.source {
            RowSource::Residual(_, _) => VariantRowState::Residual,
            RowSource::Shredded(decoder, index) => decoder.state(*index),
        }
    }

    /// Decodes this value, borrowing strings, binary data and residual bytes.
    /// Containers expose their children as rows.
    ///
    /// Residual values are validated recursively. Shredded children are validated when
    /// visited, so visit every child to validate a whole row. Invalid encodings and
    /// conflicting fields return errors.
    pub fn decode(&self) -> Result<DecodedVariant<'a>> {
        match self.state() {
            VariantRowState::Null => return Ok(DecodedVariant::Null),
            VariantRowState::Missing => return Ok(DecodedVariant::Missing),
            _ => {}
        }
        match &self.source {
            RowSource::Residual(bytes, value) => {
                Ok(decoded_residual(self.metadata.get()?, bytes, value.clone()))
            }
            RowSource::Shredded(decoder, index) => {
                let index = *index;
                let value = decoder.value.filter(|v| v.is_valid(index));
                if !decoder.typed.is_some_and(|v| v.is_valid(index)) {
                    let bytes = binary_array_value(value.unwrap().as_ref(), index).unwrap();
                    let metadata = self.metadata.get()?;
                    let value = Variant::try_new_with_metadata(metadata.clone(), bytes)?;
                    return Ok(decoded_residual(metadata, bytes, value));
                }
                if value.is_some() && !matches!(decoder.kind, TypedDecoder::Object(_)) {
                    return Err(ArrowError::InvalidArgumentError(
                        "Invalid shredded variant: both value and typed_value are non-null".into(),
                    ));
                }
                match &decoder.kind {
                    TypedDecoder::Scalar(decode) => Ok(DecodedVariant::Typed(decode(
                        decoder.typed.unwrap().as_ref(),
                        index,
                    )?)),
                    TypedDecoder::Object(fields) => {
                        let metadata = self.metadata.get()?;
                        let residual = value.map(|value| {
                            let bytes = binary_array_value(value.as_ref(), index).unwrap();
                            match Variant::try_new_with_metadata(metadata.clone(), bytes)? {
                                Variant::Object(object) => Ok(object),
                                _ => Err(ArrowError::InvalidArgumentError(
                                    "Expected object in value field for partially shredded struct".into()
                                )),
                            }
                        }).transpose()?;
                        if let Some(object) = &residual {
                            for (name, _) in object.iter() {
                                if fields.contains_key(name) {
                                    return Err(ArrowError::InvalidArgumentError(format!(
                                        "Field '{name}' appears in both typed_value and value"
                                    )));
                                }
                            }
                        }
                        Ok(DecodedVariant::Object(DecodedObject {
                            typed: Some((fields, index)),
                            residual,
                            metadata: Metadata::Validated(metadata),
                        }))
                    }
                    TypedDecoder::List { elements, range } => {
                        Ok(DecodedVariant::List(DecodedList {
                            source: ListSource::Typed(
                                elements,
                                range(decoder.typed.unwrap().as_ref(), index),
                            ),
                            metadata: Metadata::Validated(self.metadata.get()?),
                        }))
                    }
                    TypedDecoder::None => unreachable!("typed value has a decoder"),
                }
            }
        }
    }
}

/// A scalar or borrowed container. Null parents, missing values, and explicit Variant null
/// are distinct; explicit Variant null is returned as [`Self::Residual`].
pub enum DecodedVariant<'a> {
    /// A null Arrow parent.
    Null,
    /// Neither shredding column contains a value.
    Missing,
    /// A scalar decoded from typed_value, retaining its Arrow width and floating-point bits.
    Typed(Variant<'a, 'a>),
    /// A residual scalar, including its original encoding and source dictionary.
    /// Containers are exposed as [`Self::Object`] and [`Self::List`] so writers can remap IDs.
    Residual {
        /// Decoded scalar.
        value: Variant<'a, 'a>,
        /// Original scalar encoding.
        bytes: &'a [u8],
        /// Input dictionary for these bytes.
        metadata: VariantMetadata<'a>,
    },
    /// A shredded, partially shredded, or residual object.
    Object(DecodedObject<'a>),
    /// A shredded or residual list.
    List(DecodedList<'a>),
}

fn decoded_residual<'a>(
    metadata: VariantMetadata<'a>,
    bytes: &'a [u8],
    value: Variant<'a, 'a>,
) -> DecodedVariant<'a> {
    match value {
        Variant::Object(object) => DecodedVariant::Object(DecodedObject {
            typed: None,
            residual: Some(object),
            metadata: Metadata::Validated(metadata),
        }),
        Variant::List(list) => DecodedVariant::List(DecodedList {
            source: ListSource::Residual(list),
            metadata: Metadata::Validated(metadata),
        }),
        value => DecodedVariant::Residual {
            value,
            bytes,
            metadata,
        },
    }
}

/// Borrowed fields of an object.
pub struct DecodedObject<'a> {
    typed: Option<(&'a IndexMap<&'a str, FieldDecoder<'a>>, usize)>,
    residual: Option<VariantObject<'a, 'a>>,
    metadata: Metadata<'a>,
}

impl<'a> DecodedObject<'a> {
    /// Returns typed fields in schema order, then residual fields in encoded field order.
    /// Includes missing fields and resolves residual names using the input dictionary.
    pub fn fields(&self) -> impl Iterator<Item = Result<(&'a str, VariantRow<'a>)>> + '_ {
        let typed = self.typed.into_iter().flat_map(move |(fields, index)| {
            fields.iter().map(move |(&name, decoder)| {
                Ok((
                    name,
                    VariantRow {
                        source: RowSource::Shredded(decoder, index),
                        metadata: self.metadata.clone(),
                    },
                ))
            })
        });
        let residual = self.residual.iter().flat_map(move |object| {
            (0..object.len()).map(move |index| {
                Ok((
                    object.field_name(index).unwrap(),
                    VariantRow {
                        source: RowSource::Residual(
                            object.field_value_bytes(index)?,
                            object.field(index).unwrap(),
                        ),
                        metadata: self.metadata.clone(),
                    },
                ))
            })
        });
        typed.chain(residual)
    }
}

enum ListSource<'a> {
    Typed(&'a FieldDecoder<'a>, Range<usize>),
    Residual(VariantList<'a, 'a>),
}

/// Borrowed list elements, respecting sliced offsets and list-view ranges.
pub struct DecodedList<'a> {
    source: ListSource<'a>,
    metadata: Metadata<'a>,
}

impl<'a> DecodedList<'a> {
    /// Iterates elements in list order, including missing and parent-null elements.
    pub fn elements(&self) -> impl Iterator<Item = Result<VariantRow<'a>>> + '_ {
        let len = match &self.source {
            ListSource::Typed(_, range) => range.len(),
            ListSource::Residual(list) => list.len(),
        };
        (0..len).map(move |index| {
            Ok(VariantRow {
                source: match &self.source {
                    ListSource::Typed(decoder, range) => {
                        RowSource::Shredded(decoder, range.start + index)
                    }
                    ListSource::Residual(list) => RowSource::Residual(
                        list.element_value_bytes(index)?,
                        list.get(index).unwrap(),
                    ),
                },
                metadata: self.metadata.clone(),
            })
        })
    }
}

type ScalarDecoder = for<'a> fn(&'a dyn Array, usize) -> Result<Variant<'a, 'a>>;

enum TypedDecoder<'a> {
    None,
    Scalar(ScalarDecoder),
    Object(IndexMap<&'a str, FieldDecoder<'a>>),
    List {
        elements: Box<FieldDecoder<'a>>,
        range: fn(&dyn Array, usize) -> Range<usize>,
    },
}

struct FieldDecoder<'a> {
    inner: &'a StructArray,
    value: Option<&'a ArrayRef>,
    typed: Option<&'a ArrayRef>,
    kind: TypedDecoder<'a>,
}

impl<'a> FieldDecoder<'a> {
    fn state(&self, index: usize) -> VariantRowState {
        if self.inner.is_null(index) {
            return VariantRowState::Null;
        }
        match (
            self.value.is_some_and(|v| v.is_valid(index)),
            self.typed.is_some_and(|v| v.is_valid(index)),
        ) {
            (false, false) => VariantRowState::Missing,
            (true, false) => VariantRowState::Residual,
            (false, true) => VariantRowState::Typed,
            (true, true) => VariantRowState::PartiallyShredded,
        }
    }

    fn try_new(inner: &'a StructArray) -> Result<Self> {
        let value = inner.column_by_name("value");
        if let Some(value) = value {
            validate_binary_array(value.as_ref(), "value")?;
        }
        let typed = inner.column_by_name("typed_value");
        let kind = match typed {
            Some(typed) => TypedDecoder::try_new(typed.as_ref())?,
            None => TypedDecoder::None,
        };
        Ok(Self {
            inner,
            value,
            typed,
            kind,
        })
    }
}

impl<'a> TypedDecoder<'a> {
    fn try_new(array: &'a dyn Array) -> Result<Self> {
        macro_rules! primitive {
            ($ty:ty) => {
                Self::Scalar(|a, i| {
                    a.as_any()
                        .downcast_ref::<$ty>()
                        .unwrap()
                        .decode_primitive(i)
                })
            };
        }
        Ok(match array.data_type() {
            DataType::Int8 => primitive!(PrimitiveArray<Int8Type>),
            DataType::Int16 => primitive!(PrimitiveArray<Int16Type>),
            DataType::Int32 => primitive!(PrimitiveArray<Int32Type>),
            DataType::Int64 => primitive!(PrimitiveArray<Int64Type>),
            DataType::Float32 => primitive!(PrimitiveArray<Float32Type>),
            DataType::Float64 => primitive!(PrimitiveArray<Float64Type>),
            DataType::Boolean => primitive!(BooleanArray),
            DataType::Date32 => primitive!(PrimitiveArray<Date32Type>),
            DataType::Time64(TimeUnit::Microsecond) => {
                primitive!(PrimitiveArray<Time64MicrosecondType>)
            }
            DataType::Utf8 => primitive!(StringArray),
            DataType::Utf8View => primitive!(StringViewArray),
            DataType::LargeUtf8 => primitive!(LargeStringArray),
            DataType::Binary => primitive!(BinaryArray),
            DataType::BinaryView => primitive!(BinaryViewArray),
            DataType::LargeBinary => primitive!(LargeBinaryArray),
            DataType::FixedSizeBinary(16) => primitive!(FixedSizeBinaryArray),
            DataType::Decimal32(p, s) if VariantDecimal4::is_valid_precision_and_scale(p, s) => {
                Self::Scalar(decode_decimal::<Decimal32Type, VariantDecimal4>)
            }
            DataType::Decimal64(p, s) if VariantDecimal8::is_valid_precision_and_scale(p, s) => {
                Self::Scalar(decode_decimal::<Decimal64Type, VariantDecimal8>)
            }
            DataType::Decimal128(p, s) if VariantDecimal16::is_valid_precision_and_scale(p, s) => {
                Self::Scalar(decode_decimal::<Decimal128Type, VariantDecimal16>)
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                Self::Scalar(decode_timestamp::<TimestampMicrosecondType>)
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                Self::Scalar(decode_timestamp::<TimestampNanosecondType>)
            }
            DataType::Struct(_) => {
                let array = array.as_struct();
                let mut fields = IndexMap::new();
                for (field, child) in array.fields().iter().zip(array.columns()) {
                    let child = child.as_struct_opt().ok_or_else(|| {
                        ArrowError::InvalidArgumentError(format!(
                            "Invalid shredded variant object field: expected Struct, got {}",
                            child.data_type()
                        ))
                    })?;
                    fields.insert(field.name().as_str(), FieldDecoder::try_new(child)?);
                }
                Self::Object(fields)
            }
            DataType::List(_) => Self::list::<GenericListArray<i32>>(array)?,
            DataType::LargeList(_) => Self::list::<GenericListArray<i64>>(array)?,
            DataType::ListView(_) => Self::list::<GenericListViewArray<i32>>(array)?,
            DataType::LargeListView(_) => Self::list::<GenericListViewArray<i64>>(array)?,
            data_type @ (DataType::Decimal32(_, _)
            | DataType::Decimal64(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
            | DataType::Time64(_)
            | DataType::Timestamp(_, _)
            | DataType::FixedSizeBinary(_)) => {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "{data_type} is not a valid variant shredding type"
                )));
            }
            data_type => {
                return Err(ArrowError::NotYetImplemented(format!(
                    "Unshredding not yet supported for type: {data_type}"
                )));
            }
        })
    }

    fn list<L: ListLikeArray + 'static>(array: &'a dyn Array) -> Result<Self> {
        let array = array.as_any().downcast_ref::<L>().unwrap();
        let elements = array.values().as_struct_opt().ok_or_else(|| {
            ArrowError::InvalidArgumentError(format!(
                "Invalid shredded variant array element: expected Struct, got {}",
                array.values().data_type()
            ))
        })?;
        Ok(Self::List {
            elements: Box::new(FieldDecoder::try_new(elements)?),
            range: |a, i| a.as_any().downcast_ref::<L>().unwrap().element_range(i),
        })
    }
}
