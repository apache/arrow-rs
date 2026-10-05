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

//! Borrowed decoding of scalar Variant rows.

use crate::VariantArray;
use crate::variant_array::{binary_array_value, validate_binary_array};
use crate::variant_scalar::{DecodePrimitive, decode_decimal, decode_timestamp};
use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, BooleanArray, FixedSizeBinaryArray,
    LargeBinaryArray, LargeStringArray, PrimitiveArray, StringArray, StringViewArray, StructArray,
};
use arrow::datatypes::{
    DataType, Date32Type, Decimal32Type, Decimal64Type, Decimal128Type, Float32Type, Float64Type,
    Int8Type, Int16Type, Int32Type, Int64Type, Time64MicrosecondType, TimeUnit,
    TimestampMicrosecondType, TimestampNanosecondType,
};
use arrow::error::{ArrowError, Result};
use parquet_variant::{
    Variant, VariantDecimal4, VariantDecimal8, VariantDecimal16, VariantDecimalType,
    VariantMetadata,
};

/// Prepares a typed scalar decoder once for an input array.
///
/// Rows borrow the input and this decoder. No encoded output or per-row tree is allocated.
/// Use [`Self::row`] to inspect validity before decoding, then [`VariantRow::decode`] to
/// read the value. Nested values are not supported yet. Output encoding and metadata
/// dictionaries belong to the caller.
/// Unlike [`crate::unshred_variant`], this API exposes missing values instead of deciding
/// whether to omit an object field or emit Variant null.
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
    /// Checks the scalar shredding schema. Value bytes are
    /// validated when decoded, so invalid bytes masked by a null parent are never read.
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
            decoder: &self.root,
            index,
            metadata,
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
    /// Both columns are valid. Scalar decoding rejects this state before reading residual bytes.
    PartiallyShredded,
}

/// A borrowed scalar row. Inspect [`Self::state`] before decoding residual bytes.
pub struct VariantRow<'a> {
    decoder: &'a FieldDecoder<'a>,
    index: usize,
    metadata: &'a [u8],
}

impl<'a> VariantRow<'a> {
    /// Returns physical validity without parsing metadata or residual bytes.
    pub fn state(&self) -> VariantRowState {
        self.decoder.state(self.index)
    }

    /// Decodes a scalar, preserving typed/residual provenance and borrowing input bytes.
    /// Invalid encodings and conflicting shredding states return errors. Nested values
    /// are not supported yet. Output encoding is chosen by the caller.
    pub fn decode(&self) -> Result<DecodedVariant<'a>> {
        match self.state() {
            VariantRowState::Null => Ok(DecodedVariant::Null),
            VariantRowState::Missing => Ok(DecodedVariant::Missing),
            VariantRowState::PartiallyShredded => Err(ArrowError::InvalidArgumentError(
                "Invalid shredded variant: both value and typed_value are non-null".into(),
            )),
            VariantRowState::Typed => {
                let TypedDecoder::Scalar(decode) = self.decoder.kind else {
                    unreachable!("typed value has a decoder")
                };
                Ok(DecodedVariant::Typed(decode(
                    self.decoder.typed.unwrap().as_ref(),
                    self.index,
                )?))
            }
            VariantRowState::Residual => {
                let bytes =
                    binary_array_value(self.decoder.value.unwrap().as_ref(), self.index).unwrap();
                let metadata = VariantMetadata::try_new(self.metadata)?;
                let value = Variant::try_new_with_metadata(metadata.clone(), bytes)?;
                if matches!(value, Variant::Object(_) | Variant::List(_)) {
                    return Err(ArrowError::NotYetImplemented(
                        "Nested row decoding is not yet supported".into(),
                    ));
                }
                Ok(DecodedVariant::Residual {
                    value,
                    bytes,
                    metadata,
                })
            }
        }
    }
}

/// A decoded scalar. Physical null, missing and explicit
/// Variant null are distinct: the latter is a [`Self::Residual`] scalar.
pub enum DecodedVariant<'a> {
    /// A null Arrow parent.
    Null,
    /// Neither shredding column contains a value.
    Missing,
    /// A scalar decoded from typed_value, retaining its Arrow width and floating-point bits.
    Typed(Variant<'a, 'a>),
    /// A residual scalar, including its original encoding and source dictionary.
    Residual {
        /// Decoded scalar.
        value: Variant<'a, 'a>,
        /// Original scalar bytes. These can be copied without canonicalizing the scalar.
        bytes: &'a [u8],
        /// Dictionary associated with the input bytes, independent of any output dictionary.
        metadata: VariantMetadata<'a>,
    },
}

type ScalarDecoder = for<'a> fn(&'a dyn Array, usize) -> Result<Variant<'a, 'a>>;

enum TypedDecoder {
    None,
    Scalar(ScalarDecoder),
}

struct FieldDecoder<'a> {
    inner: &'a StructArray,
    value: Option<&'a ArrayRef>,
    typed: Option<&'a ArrayRef>,
    kind: TypedDecoder,
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

impl TypedDecoder {
    fn try_new(array: &dyn Array) -> Result<Self> {
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
}
