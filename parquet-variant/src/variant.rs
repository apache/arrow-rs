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

pub use self::decimal::{VariantDecimal16, VariantDecimal4, VariantDecimal8};
pub use self::list::VariantList;
pub use self::metadata::VariantMetadata;
pub use self::object::VariantObject;
use crate::decoder::{
    self, get_basic_type, get_primitive_type, VariantBasicType, VariantPrimitiveType,
};
use crate::utils::{first_byte_from_slice, slice_from_slice};
use std::ops::Deref;

use arrow_schema::ArrowError;
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};

mod decimal;
mod list;
mod metadata;
mod object;

const MAX_SHORT_STRING_BYTES: usize = 0x3F;

/// A Variant [`ShortString`]
///
/// This implementation is a zero cost wrapper over `&str` that ensures
/// the length of the underlying string is a valid Variant short string (63 bytes or less)
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ShortString<'a>(pub(crate) &'a str);

impl<'a> ShortString<'a> {
    /// Attempts to interpret `value` as a variant short string value.
    ///
    /// # Errors
    ///
    /// Returns an error if  `value` is longer than the maximum allowed length
    /// of a Variant short string (63 bytes).
    pub fn try_new(value: &'a str) -> Result<Self, ArrowError> {
        if value.len() > MAX_SHORT_STRING_BYTES {
            return Err(ArrowError::InvalidArgumentError(format!(
                "value is larger than {MAX_SHORT_STRING_BYTES} bytes"
            )));
        }

        Ok(Self(value))
    }

    /// Returns the underlying Variant short string as a &str
    pub fn as_str(&self) -> &'a str {
        self.0
    }
}

impl<'a> From<ShortString<'a>> for &'a str {
    fn from(value: ShortString<'a>) -> Self {
        value.0
    }
}

impl<'a> TryFrom<&'a str> for ShortString<'a> {
    type Error = ArrowError;

    fn try_from(value: &'a str) -> Result<Self, Self::Error> {
        Self::try_new(value)
    }
}

impl AsRef<str> for ShortString<'_> {
    fn as_ref(&self) -> &str {
        self.0
    }
}

impl Deref for ShortString<'_> {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

/// Represents a [Parquet Variant]
///
/// The lifetimes `'m` and `'v` are for metadata and value buffers, respectively.
///
/// # Background
///
/// The [specification] says:
///
/// The Variant Binary Encoding allows representation of semi-structured data
/// (e.g. JSON) in a form that can be efficiently queried by path. The design is
/// intended to allow efficient access to nested data even in the presence of
/// very wide or deep structures.
///
/// Another motivation for the representation is that (aside from metadata) each
/// nested Variant value is contiguous and self-contained. For example, in a
/// Variant containing an Array of Variant values, the representation of an
/// inner Variant value, when paired with the metadata of the full variant, is
/// itself a valid Variant.
///
/// When stored in Parquet files, Variant fields can also be *shredded*. Shredding
/// refers to extracting some elements of the variant into separate columns for
/// more efficient extraction/filter pushdown. The [Variant Shredding
/// specification] describes the details of shredding Variant values as typed
/// Parquet columns.
///
/// A Variant represents a type that contains one of:
///
/// * Primitive: A type and corresponding value (e.g. INT, STRING)
///
/// * Array: An ordered list of Variant values
///
/// * Object: An unordered collection of string/Variant pairs (i.e. key/value
///   pairs). An object may not contain duplicate keys.
///
/// # Encoding
///
/// A Variant is encoded with 2 binary values, the value and the metadata. The
/// metadata stores a header and an optional dictionary of field names which are
/// referred to by offset in the value. The value is a binary representation of
/// the actual data, and varies depending on the type.
///
/// # Design Goals
///
/// The design goals of the Rust API are as follows:
/// 1. Speed / Zero copy access (no `clone`ing is required)
/// 2. Safety
/// 3. Follow standard Rust conventions
///
/// [Parquet Variant]: https://github.com/apache/parquet-format/blob/master/VariantEncoding.md
/// [specification]: https://github.com/apache/parquet-format/blob/master/VariantEncoding.md
/// [Variant Shredding specification]: https://github.com/apache/parquet-format/blob/master/VariantShredding.md
///
/// # Examples:
///
/// ## Creating `Variant` from Rust Types
/// ```
/// use parquet_variant::Variant;
/// // variants can be directly constructed
/// let variant = Variant::Int32(123);
/// // or constructed via `From` impls
/// assert_eq!(variant, Variant::from(123i32));
/// ```
/// ## Creating `Variant` from metadata and value
/// ```
/// # use parquet_variant::{Variant, VariantMetadata};
/// let metadata = [0x01, 0x00, 0x00];
/// let value = [0x09, 0x48, 0x49];
/// // parse the header metadata
/// assert_eq!(
///   Variant::from("HI"),
///   Variant::new(&metadata, &value)
/// );
/// ```
///
/// ## Using `Variant` values
/// ```
/// # use parquet_variant::Variant;
/// # let variant = Variant::Int32(123);
/// // variants can be used in match statements like normal enums
/// match variant {
///   Variant::Int32(i) => println!("Integer: {}", i),
///   Variant::String(s) => println!("String: {}", s),
///   _ => println!("Other variant"),
/// }
/// ```
///
/// # Validation
///
/// Every instance of variant is either _valid_ or _invalid_. depending on whether the
/// underlying bytes are a valid encoding of a variant value (see below).
///
/// Instances produced by [`Self::try_new`], [`Self::try_new_with_metadata`], or [`Self::with_full_validation`]
/// are fully _validated_. They always contain _valid_ data, and infallible accesses such as
/// iteration and indexing are panic-free. The validation cost is `O(m + v)` where `m` and
/// `v` are the number of bytes in the metadata and value buffers, respectively.
///
/// Instances produced by [`Self::new`] and [`Self::new_with_metadata`] are _unvalidated_ and so
/// they may contain either _valid_ or _invalid_ data. Infallible accesses to variant objects and
/// arrays, such as iteration and indexing will panic if the underlying bytes are _invalid_, and
/// fallible alternatives are provided as panic-free alternatives. [`Self::with_full_validation`] can also be
/// used to _validate_ an _unvalidated_ instance, if desired.
///
/// _Unvalidated_ instances can be constructed in constant time. This can be useful if the caller
/// knows the underlying bytes were already validated previously, or if the caller intends to
/// perform a small number of (fallible) accesses to a large variant value.
///
/// A _validated_ variant value guarantees that the associated [metadata] and all nested [object]
/// and [array] values are _valid_. Primitive variant subtypes are always _valid_ by construction.
///
/// # Safety
///
/// Even an _invalid_ variant value is still _safe_ to use in the Rust sense. Accessing it with
/// infallible methods may cause panics but will never lead to undefined behavior.
///
/// [metadata]: VariantMetadata#Validation
/// [object]: VariantObject#Validation
/// [array]: VariantList#Validation
#[derive(Debug, Clone, PartialEq)]
pub enum Variant<'m, 'v> {
    /// Primitive type: Null
    Null,
    /// Primitive (type_id=1): INT(8, SIGNED)
    Int8(i8),
    /// Primitive (type_id=1): INT(16, SIGNED)
    Int16(i16),
    /// Primitive (type_id=1): INT(32, SIGNED)
    Int32(i32),
    /// Primitive (type_id=1): INT(64, SIGNED)
    Int64(i64),
    /// Primitive (type_id=1): DATE
    Date(NaiveDate),
    /// Primitive (type_id=1): TIMESTAMP(isAdjustedToUTC=true, MICROS)
    TimestampMicros(DateTime<Utc>),
    /// Primitive (type_id=1): TIMESTAMP(isAdjustedToUTC=false, MICROS)
    TimestampNtzMicros(NaiveDateTime),
    /// Primitive (type_id=1): DECIMAL(precision, scale) 32-bits
    Decimal4(VariantDecimal4),
    /// Primitive (type_id=1): DECIMAL(precision, scale) 64-bits
    Decimal8(VariantDecimal8),
    /// Primitive (type_id=1): DECIMAL(precision, scale) 128-bits
    Decimal16(VariantDecimal16),
    /// Primitive (type_id=1): FLOAT
    Float(f32),
    /// Primitive (type_id=1): DOUBLE
    Double(f64),
    /// Primitive (type_id=1): BOOLEAN (true)
    BooleanTrue,
    /// Primitive (type_id=1): BOOLEAN (false)
    BooleanFalse,
    // Note: only need the *value* buffer for these types
    /// Primitive (type_id=1): BINARY
    Binary(&'v [u8]),
    /// Primitive (type_id=1): STRING
    String(&'v str),
    /// Short String (type_id=2): STRING
    ShortString(ShortString<'v>),
    // need both metadata & value
    /// Object (type_id=3): N/A
    Object(VariantObject<'m, 'v>),
    /// Array (type_id=4): N/A
    List(VariantList<'m, 'v>),
}

impl<'m, 'v> Variant<'m, 'v> {
    /// Attempts to interpret a metadata and value buffer pair as a new `Variant`.
    ///
    /// The instance is fully [validated].
    ///
    /// # Example
    /// ```
    /// use parquet_variant::{Variant, VariantMetadata};
    /// let metadata = [0x01, 0x00, 0x00];
    /// let value = [0x09, 0x48, 0x49];
    /// // parse the header metadata
    /// assert_eq!(
    ///   Variant::from("HI"),
    ///   Variant::try_new(&metadata, &value).unwrap()
    /// );
    /// ```
    ///
    /// [validated]: Self#Validation
    pub fn try_new(metadata: &'m [u8], value: &'v [u8]) -> Result<Self, ArrowError> {
        let metadata = VariantMetadata::try_new(metadata)?;
        Self::try_new_with_metadata(metadata, value)
    }

    /// Attempts to interpret a metadata and value buffer pair as a new `Variant`.
    ///
    /// The instance is [unvalidated].
    ///
    /// # Example
    /// ```
    /// use parquet_variant::{Variant, VariantMetadata};
    /// let metadata = [0x01, 0x00, 0x00];
    /// let value = [0x09, 0x48, 0x49];
    /// // parse the header metadata
    /// assert_eq!(
    ///   Variant::from("HI"),
    ///   Variant::new(&metadata, &value)
    /// );
    /// ```
    ///
    /// [unvalidated]: Self#Validation
    pub fn new(metadata: &'m [u8], value: &'v [u8]) -> Self {
        let metadata = VariantMetadata::try_new_with_shallow_validation(metadata)
            .expect("Invalid variant metadata");
        Self::try_new_with_metadata_and_shallow_validation(metadata, value)
            .expect("Invalid variant data")
    }

    /// Create a new variant with existing metadata.
    ///
    /// The instance is fully [validated].
    ///
    /// # Example
    /// ```
    /// # use parquet_variant::{Variant, VariantMetadata};
    /// let metadata = [0x01, 0x00, 0x00];
    /// let value = [0x09, 0x48, 0x49];
    /// // parse the header metadata first
    /// let metadata = VariantMetadata::new(&metadata);
    /// assert_eq!(
    ///   Variant::from("HI"),
    ///   Variant::try_new_with_metadata(metadata, &value).unwrap()
    /// );
    /// ```
    ///
    /// [validated]: Self#Validation
    pub fn try_new_with_metadata(
        metadata: VariantMetadata<'m>,
        value: &'v [u8],
    ) -> Result<Self, ArrowError> {
        Self::try_new_with_metadata_and_shallow_validation(metadata, value)?.with_full_validation()
    }

    /// Similar to [`Self::try_new_with_metadata`], but [unvalidated].
    ///
    /// [unvalidated]: Self#Validation
    pub fn new_with_metadata(metadata: VariantMetadata<'m>, value: &'v [u8]) -> Self {
        Self::try_new_with_metadata_and_shallow_validation(metadata, value)
            .expect("Invalid variant")
    }

    // The actual constructor, which only performs shallow (constant-time) validation.
    fn try_new_with_metadata_and_shallow_validation(
        metadata: VariantMetadata<'m>,
        value: &'v [u8],
    ) -> Result<Self, ArrowError> {
        let value_metadata = first_byte_from_slice(value)?;
        let value_data = slice_from_slice(value, 1..)?;
        let new_self = match get_basic_type(value_metadata) {
            VariantBasicType::Primitive => match get_primitive_type(value_metadata)? {
                VariantPrimitiveType::Null => Variant::Null,
                VariantPrimitiveType::Int8 => Variant::Int8(decoder::decode_int8(value_data)?),
                VariantPrimitiveType::Int16 => Variant::Int16(decoder::decode_int16(value_data)?),
                VariantPrimitiveType::Int32 => Variant::Int32(decoder::decode_int32(value_data)?),
                VariantPrimitiveType::Int64 => Variant::Int64(decoder::decode_int64(value_data)?),
                VariantPrimitiveType::Decimal4 => {
                    let (integer, scale) = decoder::decode_decimal4(value_data)?;
                    Variant::Decimal4(VariantDecimal4::try_new(integer, scale)?)
                }
                VariantPrimitiveType::Decimal8 => {
                    let (integer, scale) = decoder::decode_decimal8(value_data)?;
                    Variant::Decimal8(VariantDecimal8::try_new(integer, scale)?)
                }
                VariantPrimitiveType::Decimal16 => {
                    let (integer, scale) = decoder::decode_decimal16(value_data)?;
                    Variant::Decimal16(VariantDecimal16::try_new(integer, scale)?)
                }
                VariantPrimitiveType::Float => Variant::Float(decoder::decode_float(value_data)?),
                VariantPrimitiveType::Double => {
                    Variant::Double(decoder::decode_double(value_data)?)
                }
                VariantPrimitiveType::BooleanTrue => Variant::BooleanTrue,
                VariantPrimitiveType::BooleanFalse => Variant::BooleanFalse,
                VariantPrimitiveType::Date => Variant::Date(decoder::decode_date(value_data)?),
                VariantPrimitiveType::TimestampMicros => {
                    Variant::TimestampMicros(decoder::decode_timestamp_micros(value_data)?)
                }
                VariantPrimitiveType::TimestampNtzMicros => {
                    Variant::TimestampNtzMicros(decoder::decode_timestampntz_micros(value_data)?)
                }
                VariantPrimitiveType::Binary => {
                    Variant::Binary(decoder::decode_binary(value_data)?)
                }
                VariantPrimitiveType::String => {
                    Variant::String(decoder::decode_long_string(value_data)?)
                }
            },
            VariantBasicType::ShortString => {
                Variant::ShortString(decoder::decode_short_string(value_metadata, value_data)?)
            }
            VariantBasicType::Object => Variant::Object(
                VariantObject::try_new_with_shallow_validation(metadata, value)?,
            ),
            VariantBasicType::Array => Variant::List(VariantList::try_new_with_shallow_validation(
                metadata, value,
            )?),
        };
        Ok(new_self)
    }

    /// True if this variant instance has already been [validated].
    ///
    /// [validated]: Self#Validation
    pub fn is_fully_validated(&self) -> bool {
        match self {
            Variant::List(list) => list.is_fully_validated(),
            Variant::Object(obj) => obj.is_fully_validated(),
            _ => true,
        }
    }

    /// Recursively validates this variant value, ensuring that infallible access will not panic due
    /// to invalid bytes.
    ///
    /// Variant leaf values are always valid by construction, but [objects] and [arrays] can be
    /// constructed in unvalidated (and potentially invalid) state.
    ///
    /// If [`Self::is_fully_validated`] is true, validation is a no-op. Otherwise, the cost is `O(m + v)`
    /// where `m` and `v` are the sizes of metadata and value buffers, respectively.
    ///
    /// [objects]: VariantObject#Validation
    /// [arrays]: VariantList#Validation
    pub fn with_full_validation(self) -> Result<Self, ArrowError> {
        use Variant::*;
        match self {
            List(list) => list.with_full_validation().map(List),
            Object(obj) => obj.with_full_validation().map(Object),
            _ => Ok(self),
        }
    }

    /// Converts this variant to `()` if it is null.
    ///
    /// Returns `Some(())` for null variants,
    /// `None` for non-null variants.
    ///
    /// # Examples
    ///
    /// ```
    /// use parquet_variant::Variant;
    ///
    /// // you can extract `()` from a null variant
    /// let v1 = Variant::from(());
    /// assert_eq!(v1.as_null(), Some(()));
    ///
    /// // but not from other variants
    /// let v2 = Variant::from("hello!");
    /// assert_eq!(v2.as_null(), None);
    /// ```
    pub fn as_null(&self) -> Option<()> {
        matches!(self, Variant::Null).then_some(())
    }

    /// Converts this variant to a `bool` if possible.
    ///
    /// Returns `Some(bool)` for boolean variants,
    /// `None` for non-boolean variants.
    ///
    /// # Examples
    ///
    /// ```
    /// use parquet_variant::Variant;
    ///
    /// // you can extract a bool from the true variant
    /// let v1 = Variant::from(true);
    /// assert_eq!(v1.as_boolean(), Some(true));
    ///
    /// // and the false variant
    /// let v2 = Variant::from(false);
    /// assert_eq!(v2.as_boolean(), Some(false));
    ///
    /// // but not from other variants
    /// let v3 = Variant::from("hello!");
    /// assert_eq!(v3.as_boolean(), None);
    /// ```
    pub fn as_boolean(&self) -> Option<bool> {
        match self {
            Variant::BooleanTrue => Some(true),
            Variant::BooleanFalse => Some(false),
            _ => None,
        }
    }

    /// Converts this variant to a `NaiveDate` if possible.
    ///
    /// Returns `Some(NaiveDate)` for date variants,
    /// `None` for non-date variants.
    ///
    /// # Examples
    ///
    /// ```
    /// use parquet_variant::Variant;
    /// use chrono::NaiveDate;
    ///
    /// // you can extract a NaiveDate from a date variant
    /// let date = NaiveDate::from_ymd_opt(2025, 4, 12).unwrap();
    /// let v1 = Variant::from(date);
    /// assert_eq!(v1.as_naive_date(), Some(date));
    ///
    /// // but not from other variants
    /// let v2 = Variant::from("hello!");
    /// assert_eq!(v2.as_naive_date(), None);
    /// ```
    pub fn as_naive_date(&self) -> Option<NaiveDate> {
        if let Variant::Date(d) = self {
            Some(*d)
        } else {
            None
        }
    }

    /// Converts this variant to a `DateTime<Utc>` if possible.
    ///
    /// Returns `Some(DateTime<Utc>)` for timestamp variants,
    /// `None` for non-timestamp variants.
    ///
    /// # Examples
    ///
    /// ```
    /// use parquet_variant::Variant;
    /// use chrono::NaiveDate;
    ///
    /// // you can extract a DateTime<Utc> from a UTC-adjusted variant
    /// let datetime = NaiveDate::from_ymd_opt(2025, 4, 16).unwrap().and_hms_milli_opt(12, 34, 56, 780).unwrap().and_utc();
    /// let v1 = Variant::from(datetime);
    /// assert_eq!(v1.as_datetime_utc(), Some(datetime));
    ///
    /// // or a non-UTC-adjusted variant
    /// let datetime = NaiveDate::from_ymd_opt(2025, 4, 16).unwrap().and_hms_milli_opt(12, 34, 56, 780).unwrap();
    /// let v2 = Variant::from(datetime);
    /// assert_eq!(v2.as_datetime_utc(), Some(datetime.and_utc()));
    ///
    /// // but not from other variants
    /// let v3 = Variant::from("hello!");
    /// assert_eq!(v3.as_datetime_utc(), None);
    /// ```
    pub fn as_datetime_utc(&self) -> Option<DateTime<Utc>> {
        match *self {
            Variant::TimestampMicros(d) => Some(d),
            Variant::TimestampNtzMicros(d) => Some(d.and_utc()),
            _ => None,
        }
    }

    /// Converts this variant to a `NaiveDateTime` if possible.
    ///
    /// Returns `Some(NaiveDateTime)` for timestamp variants,
    /// `None` for non-timestamp variants.
    ///
    /// # Examples
    ///
    /// ```
    /// use parquet_variant::Variant;
    /// use chrono::NaiveDate;
    ///
    /// // you can extract a NaiveDateTime from a non-UTC-adjusted variant
    /// let datetime = NaiveDate::from_ymd_opt(2025, 4, 16).unwrap().and_hms_milli_opt(12, 34, 56, 780).unwrap();
    /// let v1 = Variant::from(datetime);
    /// assert_eq!(v1.as_naive_datetime(), Some(datetime));
    ///
    /// // or a UTC-adjusted variant
    /// let datetime = NaiveDate::from_ymd_opt(2025, 4, 16).unwrap().and_hms_milli_opt(12, 34, 56, 780).unwrap().and_utc();
    /// let v2 = Variant::from(datetime);
    /// assert_eq!(v2.as_naive_datetime(), Some(datetime.naive_utc()));
    ///
    /// // but not from other variants
    /// let v3 = Variant::from("hello!");
    /// assert_eq!(v3.as_naive_datetime(), None);
    /// ```
    pub fn as_naive_datetime(&self) -> Option<NaiveDateTime> {
        match *self {
            Variant::TimestampNtzMicros(d) => Some(d),
            Variant::TimestampMicros(d) => Some(d.naive_utc()),
            _ => None,
        }
    }

    /// Converts this variant to a `&[u8]` if possible.
    ///
    /// Returns `Some(&[u8])` for binary variants,
    /// `None` for non-binary variants.
    ///
    /// # Examples
    ///
    /// ```
    /// use parquet_variant::Variant;
    ///
    /// // you can extract a byte slice from a binary variant
    /// let data = b"hello!";
    /// let v1 = Variant::Binary(data);
    /// assert_eq!(v1.as_u8_slice(), Some(data.as_slice()));
    ///
    /// // but not from other variant types
    /// let v2 = Variant::from(123i64);
    /// assert_eq!(v2.as_u8_slice(), None);
    /// ```
    pub fn as_u8_slice(&'v self) -> Option<&'v [u8]> {
        if let Variant::Binary(d) = self {
            Some(d)
        } else {
            None
        }
    }

    /// Converts this variant to a `&str` if possible.
    ///
    /// Returns `Some(&str)` for string variants (both regular and short strings),
    /// `None` for non-string variants.
    ///
    /// # Examples
    ///
    /// ```
    /// use parquet_variant::Variant;
    ///
    /// // you can extract a string from string variants
    /// let s = "hello!";
    /// let v1 = Variant::from(s);
    /// assert_eq!(v1.as_string(), Some(s));
    ///
    /// // but not from other variants
    /// let v2 = Variant::from(123i64);
    /// assert_eq!(v2.as_string(), None);
    /// ```
    pub fn as_string(&'v self) -> Option<&'v str> {
        match self {
            Variant::String(s) | Variant::ShortString(ShortString(s)) => Some(s),
            _ => None,
        }
    }

    /// Converts this variant to an `i8` if possible.
    ///
    /// Returns `Some(i8)` for integer variants that fit in `i8` range,
    /// `None` for non-integer variants or values that would overflow.
    ///
    /// # Examples
    ///
    /// ```
    /// use parquet_variant::Variant;
    ///
    /// // you can read an int64 variant into an i8 if it fits
    /// let v1 = Variant::from(123i64);
    /// assert_eq!(v1.as_int8(), Some(123i8));
    ///
    /// // but not if it would overflow
    /// let v2 = Variant::from(1234i64);
    /// assert_eq!(v2.as_int8(), None);
    ///
    /// // or if the variant cannot be cast into an integer
    /// let v3 = Variant::from("hello!");
    /// assert_eq!(v3.as_int8(), None);
    /// ```
    pub fn as_int8(&self) -> Option<i8> {
        match *self {
            Variant::Int8(i) => Some(i),
            Variant::Int16(i) => i.try_into().ok(),
            Variant::Int32(i) => i.try_into().ok(),
            Variant::Int64(i) => i.try_into().ok(),
            Variant::Decimal4(d) if d.scale() == 0 => d.integer().try_into().ok(),
            Variant::Decimal8(d) if d.scale() == 0 => d.integer().try_into().ok(),
            Variant::Decimal16(d) if d.scale() == 0 => d.integer().try_into().ok(),
            _ => None,
        }
    }

    /// Converts this variant to an `i16` if possible.
    ///
    /// Returns `Some(i16)` for integer variants that fit in `i16` range,
    /// `None` for non-integer variants or values that would overflow.
    ///
    /// # Examples
    ///
    /// ```
    /// use parquet_variant::Variant;
    ///
    /// // you can read an int64 variant into an i16 if it fits
    /// let v1 = Variant::from(123i64);
    /// assert_eq!(v1.as_int16(), Some(123i16));
    ///
    /// // but not if it would overflow
    /// let v2 = Variant::from(123456i64);
    /// assert_eq!(v2.as_int16(), None);
    ///
    /// // or if the variant cannot be cast into an integer
    /// let v3 = Variant::from("hello!");
    /// assert_eq!(v3.as_int16(), None);
    /// ```
    pub fn as_int16(&self) -> Option<i16> {
        match *self {
            Variant::Int8(i) => Some(i.into()),
            Variant::Int16(i) => Some(i),
            Variant::Int32(i) => i.try_into().ok(),
            Variant::Int64(i) => i.try_into().ok(),
            Variant::Decimal4(d) if d.scale() == 0 => d.integer().try_into().ok(),
            Variant::Decimal8(d) if d.scale() == 0 => d.integer().try_into().ok(),
            Variant::Decimal16(d) if d.scale() == 0 => d.integer().try_into().ok(),
            _ => None,
        }
    }

    /// Converts this variant to an `i32` if possible.
    ///
    /// Returns `Some(i32)` for integer variants that fit in `i32` range,
    /// `None` for non-integer variants or values that would overflow.
    ///
    /// # Examples
    ///
    /// ```
    /// use parquet_variant::Variant;
    ///
    /// // you can read an int64 variant into an i32 if it fits
    /// let v1 = Variant::from(123i64);
    /// assert_eq!(v1.as_int32(), Some(123i32));
    ///
    /// // but not if it would overflow
    /// let v2 = Variant::from(12345678901i64);
    /// assert_eq!(v2.as_int32(), None);
    ///
    /// // or if the variant cannot be cast into an integer
    /// let v3 = Variant::from("hello!");
    /// assert_eq!(v3.as_int32(), None);
    /// ```
    pub fn as_int32(&self) -> Option<i32> {
        match *self {
            Variant::Int8(i) => Some(i.into()),
            Variant::Int16(i) => Some(i.into()),
            Variant::Int32(i) => Some(i),
            Variant::Int64(i) => i.try_into().ok(),
            Variant::Decimal4(d) if d.scale() == 0 => Some(d.integer()),
            Variant::Decimal8(d) if d.scale() == 0 => d.integer().try_into().ok(),
            Variant::Decimal16(d) if d.scale() == 0 => d.integer().try_into().ok(),
            _ => None,
        }
    }

    /// Converts this variant to an `i64` if possible.
    ///
    /// Returns `Some(i64)` for integer variants that fit in `i64` range,
    /// `None` for non-integer variants or values that would overflow.
    ///
    /// # Examples
    ///
    /// ```
    /// use parquet_variant::Variant;
    ///
    /// // you can read an int64 variant into an i64
    /// let v1 = Variant::from(123i64);
    /// assert_eq!(v1.as_int64(), Some(123i64));
    ///
    /// // but not a variant that cannot be cast into an integer
    /// let v2 = Variant::from("hello!");
    /// assert_eq!(v2.as_int64(), None);
    /// ```
    pub fn as_int64(&self) -> Option<i64> {
        match *self {
            Variant::Int8(i) => Some(i.into()),
            Variant::Int16(i) => Some(i.into()),
            Variant::Int32(i) => Some(i.into()),
            Variant::Int64(i) => Some(i),
            Variant::Decimal4(d) if d.scale() == 0 => Some(d.integer().into()),
            Variant::Decimal8(d) if d.scale() == 0 => Some(d.integer()),
            Variant::Decimal16(d) if d.scale() == 0 => d.integer().try_into().ok(),
            _ => None,
        }
    }

    /// Converts this variant to tuple with a 4-byte unscaled value if possible.
    ///
    /// Returns `Some((i32, u8))` for decimal variants where the unscaled value
    /// fits in `i32` range,
    /// `None` for non-decimal variants or decimal values that would overflow.
    ///
    /// # Examples
    ///
    /// ```
    /// use parquet_variant::{Variant, VariantDecimal4, VariantDecimal8};
    ///
    /// // you can extract decimal parts from smaller or equally-sized decimal variants
    /// let v1 = Variant::from(VariantDecimal4::try_new(1234_i32, 2).unwrap());
    /// assert_eq!(v1.as_decimal4(), VariantDecimal4::try_new(1234_i32, 2).ok());
    ///
    /// // and from larger decimal variants if they fit
    /// let v2 = Variant::from(VariantDecimal8::try_new(1234_i64, 2).unwrap());
    /// assert_eq!(v2.as_decimal4(), VariantDecimal4::try_new(1234_i32, 2).ok());
    ///
    /// // but not if the value would overflow i32
    /// let v3 = Variant::from(VariantDecimal8::try_new(12345678901i64, 2).unwrap());
    /// assert_eq!(v3.as_decimal4(), None);
    ///
    /// // or if the variant is not a decimal
    /// let v4 = Variant::from("hello!");
    /// assert_eq!(v4.as_decimal4(), None);
    /// ```
    pub fn as_decimal4(&self) -> Option<VariantDecimal4> {
        match *self {
            Variant::Int8(i) => i32::from(i).try_into().ok(),
            Variant::Int16(i) => i32::from(i).try_into().ok(),
            Variant::Int32(i) => i.try_into().ok(),
            Variant::Int64(i) => i32::try_from(i).ok()?.try_into().ok(),
            Variant::Decimal4(decimal4) => Some(decimal4),
            Variant::Decimal8(decimal8) => decimal8.try_into().ok(),
            Variant::Decimal16(decimal16) => decimal16.try_into().ok(),
            _ => None,
        }
    }

    /// Converts this variant to tuple with an 8-byte unscaled value if possible.
    ///
    /// Returns `Some((i64, u8))` for decimal variants where the unscaled value
    /// fits in `i64` range,
    /// `None` for non-decimal variants or decimal values that would overflow.
    ///
    /// # Examples
    ///
    /// ```
    /// use parquet_variant::{Variant, VariantDecimal4, VariantDecimal8, VariantDecimal16};
    ///
    /// // you can extract decimal parts from smaller or equally-sized decimal variants
    /// let v1 = Variant::from(VariantDecimal4::try_new(1234_i32, 2).unwrap());
    /// assert_eq!(v1.as_decimal8(), VariantDecimal8::try_new(1234_i64, 2).ok());
    ///
    /// // and from larger decimal variants if they fit
    /// let v2 = Variant::from(VariantDecimal16::try_new(1234_i128, 2).unwrap());
    /// assert_eq!(v2.as_decimal8(), VariantDecimal8::try_new(1234_i64, 2).ok());
    ///
    /// // but not if the value would overflow i64
    /// let v3 = Variant::from(VariantDecimal16::try_new(2e19 as i128, 2).unwrap());
    /// assert_eq!(v3.as_decimal8(), None);
    ///
    /// // or if the variant is not a decimal
    /// let v4 = Variant::from("hello!");
    /// assert_eq!(v4.as_decimal8(), None);
    /// ```
    pub fn as_decimal8(&self) -> Option<VariantDecimal8> {
        match *self {
            Variant::Int8(i) => i64::from(i).try_into().ok(),
            Variant::Int16(i) => i64::from(i).try_into().ok(),
            Variant::Int32(i) => i64::from(i).try_into().ok(),
            Variant::Int64(i) => i.try_into().ok(),
            Variant::Decimal4(decimal4) => Some(decimal4.into()),
            Variant::Decimal8(decimal8) => Some(decimal8),
            Variant::Decimal16(decimal16) => decimal16.try_into().ok(),
            _ => None,
        }
    }

    /// Converts this variant to tuple with a 16-byte unscaled value if possible.
    ///
    /// Returns `Some((i128, u8))` for decimal variants where the unscaled value
    /// fits in `i128` range,
    /// `None` for non-decimal variants or decimal values that would overflow.
    ///
    /// # Examples
    ///
    /// ```
    /// use parquet_variant::{Variant, VariantDecimal16, VariantDecimal4};
    ///
    /// // you can extract decimal parts from smaller or equally-sized decimal variants
    /// let v1 = Variant::from(VariantDecimal4::try_new(1234_i32, 2).unwrap());
    /// assert_eq!(v1.as_decimal16(), VariantDecimal16::try_new(1234_i128, 2).ok());
    ///
    /// // but not if the variant is not a decimal
    /// let v2 = Variant::from("hello!");
    /// assert_eq!(v2.as_decimal16(), None);
    /// ```
    pub fn as_decimal16(&self) -> Option<VariantDecimal16> {
        match *self {
            Variant::Int8(i) => i128::from(i).try_into().ok(),
            Variant::Int16(i) => i128::from(i).try_into().ok(),
            Variant::Int32(i) => i128::from(i).try_into().ok(),
            Variant::Int64(i) => i128::from(i).try_into().ok(),
            Variant::Decimal4(decimal4) => Some(decimal4.into()),
            Variant::Decimal8(decimal8) => Some(decimal8.into()),
            Variant::Decimal16(decimal16) => Some(decimal16),
            _ => None,
        }
    }
    /// Converts this variant to an `f32` if possible.
    ///
    /// Returns `Some(f32)` for float and double variants,
    /// `None` for non-floating-point variants.
    ///
    /// # Examples
    ///
    /// ```
    /// use parquet_variant::Variant;
    ///
    /// // you can extract an f32 from a float variant
    /// let v1 = Variant::from(std::f32::consts::PI);
    /// assert_eq!(v1.as_f32(), Some(std::f32::consts::PI));
    ///
    /// // and from a double variant (with loss of precision to nearest f32)
    /// let v2 = Variant::from(std::f64::consts::PI);
    /// assert_eq!(v2.as_f32(), Some(std::f32::consts::PI));
    ///
    /// // but not from other variants
    /// let v3 = Variant::from("hello!");
    /// assert_eq!(v3.as_f32(), None);
    /// ```
    #[allow(clippy::cast_possible_truncation)]
    pub fn as_f32(&self) -> Option<f32> {
        match *self {
            Variant::Float(i) => Some(i),
            Variant::Double(i) => Some(i as f32),
            _ => None,
        }
    }

    /// Converts this variant to an `f64` if possible.
    ///
    /// Returns `Some(f64)` for float and double variants,
    /// `None` for non-floating-point variants.
    ///
    /// # Examples
    ///
    /// ```
    /// use parquet_variant::Variant;
    ///
    /// // you can extract an f64 from a float variant
    /// let v1 = Variant::from(std::f32::consts::PI);
    /// assert_eq!(v1.as_f64(), Some(std::f32::consts::PI as f64));
    ///
    /// // and from a double variant
    /// let v2 = Variant::from(std::f64::consts::PI);
    /// assert_eq!(v2.as_f64(), Some(std::f64::consts::PI));
    ///
    /// // but not from other variants
    /// let v3 = Variant::from("hello!");
    /// assert_eq!(v3.as_f64(), None);
    /// ```
    pub fn as_f64(&self) -> Option<f64> {
        match *self {
            Variant::Float(i) => Some(i.into()),
            Variant::Double(i) => Some(i),
            _ => None,
        }
    }

    /// Converts this variant to an `Object` if it is an [`VariantObject`].
    ///
    /// Returns `Some(&VariantObject)` for object variants,
    /// `None` for non-object variants.
    ///
    /// # Examples
    /// ```
    /// # use parquet_variant::{Variant, VariantBuilder, VariantObject};
    /// # let (metadata, value) = {
    /// # let mut builder = VariantBuilder::new();
    /// #   let mut obj = builder.new_object();
    /// #   obj.insert("name", "John");
    /// #   obj.finish();
    /// #   builder.finish()
    /// # };
    /// // object that is {"name": "John"}
    ///  let variant = Variant::new(&metadata, &value);
    /// // use the `as_object` method to access the object
    /// let obj = variant.as_object().expect("variant should be an object");
    /// assert_eq!(obj.get("name"), Some(Variant::from("John")));
    /// ```
    pub fn as_object(&'m self) -> Option<&'m VariantObject<'m, 'v>> {
        if let Variant::Object(obj) = self {
            Some(obj)
        } else {
            None
        }
    }

    /// Converts this variant to a `List` if it is a [`VariantList`].
    ///
    /// Returns `Some(&VariantList)` for list variants,
    /// `None` for non-list variants.
    ///
    /// # Examples
    /// ```
    /// # use parquet_variant::{Variant, VariantBuilder, VariantList};
    /// # let (metadata, value) = {
    /// # let mut builder = VariantBuilder::new();
    /// #   let mut list = builder.new_list();
    /// #   list.append_value("John");
    /// #   list.append_value("Doe");
    /// #   list.finish();
    /// #   builder.finish()
    /// # };
    /// // list that is ["John", "Doe"]
    /// let variant = Variant::new(&metadata, &value);
    /// // use the `as_list` method to access the list
    /// let list = variant.as_list().expect("variant should be a list");
    /// assert_eq!(list.len(), 2);
    /// assert_eq!(list.get(0).unwrap(), Variant::from("John"));
    /// assert_eq!(list.get(1).unwrap(), Variant::from("Doe"));
    /// ```
    pub fn as_list(&'m self) -> Option<&'m VariantList<'m, 'v>> {
        if let Variant::List(list) = self {
            Some(list)
        } else {
            None
        }
    }

    /// Return the metadata associated with this variant, if any.
    ///
    /// Returns `Some(&VariantMetadata)` for object and list variants,
    pub fn metadata(&self) -> Option<&'m VariantMetadata> {
        match self {
            Variant::Object(VariantObject { metadata, .. })
            | Variant::List(VariantList { metadata, .. }) => Some(metadata),
            _ => None,
        }
    }
}

impl From<()> for Variant<'_, '_> {
    fn from((): ()) -> Self {
        Variant::Null
    }
}

impl From<bool> for Variant<'_, '_> {
    fn from(value: bool) -> Self {
        match value {
            true => Variant::BooleanTrue,
            false => Variant::BooleanFalse,
        }
    }
}

impl From<i8> for Variant<'_, '_> {
    fn from(value: i8) -> Self {
        Variant::Int8(value)
    }
}

impl From<i16> for Variant<'_, '_> {
    fn from(value: i16) -> Self {
        Variant::Int16(value)
    }
}

impl From<i32> for Variant<'_, '_> {
    fn from(value: i32) -> Self {
        Variant::Int32(value)
    }
}

impl From<i64> for Variant<'_, '_> {
    fn from(value: i64) -> Self {
        Variant::Int64(value)
    }
}

impl From<VariantDecimal4> for Variant<'_, '_> {
    fn from(value: VariantDecimal4) -> Self {
        Variant::Decimal4(value)
    }
}

impl From<VariantDecimal8> for Variant<'_, '_> {
    fn from(value: VariantDecimal8) -> Self {
        Variant::Decimal8(value)
    }
}

impl From<VariantDecimal16> for Variant<'_, '_> {
    fn from(value: VariantDecimal16) -> Self {
        Variant::Decimal16(value)
    }
}

impl From<f32> for Variant<'_, '_> {
    fn from(value: f32) -> Self {
        Variant::Float(value)
    }
}

impl From<f64> for Variant<'_, '_> {
    fn from(value: f64) -> Self {
        Variant::Double(value)
    }
}

impl From<NaiveDate> for Variant<'_, '_> {
    fn from(value: NaiveDate) -> Self {
        Variant::Date(value)
    }
}

impl From<DateTime<Utc>> for Variant<'_, '_> {
    fn from(value: DateTime<Utc>) -> Self {
        Variant::TimestampMicros(value)
    }
}
impl From<NaiveDateTime> for Variant<'_, '_> {
    fn from(value: NaiveDateTime) -> Self {
        Variant::TimestampNtzMicros(value)
    }
}

impl<'v> From<&'v [u8]> for Variant<'_, 'v> {
    fn from(value: &'v [u8]) -> Self {
        Variant::Binary(value)
    }
}

impl<'v> From<&'v str> for Variant<'_, 'v> {
    fn from(value: &'v str) -> Self {
        if value.len() > MAX_SHORT_STRING_BYTES {
            Variant::String(value)
        } else {
            Variant::ShortString(ShortString(value))
        }
    }
}

impl TryFrom<(i32, u8)> for Variant<'_, '_> {
    type Error = ArrowError;

    fn try_from(value: (i32, u8)) -> Result<Self, Self::Error> {
        Ok(Variant::Decimal4(VariantDecimal4::try_new(
            value.0, value.1,
        )?))
    }
}

impl TryFrom<(i64, u8)> for Variant<'_, '_> {
    type Error = ArrowError;

    fn try_from(value: (i64, u8)) -> Result<Self, Self::Error> {
        Ok(Variant::Decimal8(VariantDecimal8::try_new(
            value.0, value.1,
        )?))
    }
}

impl TryFrom<(i128, u8)> for Variant<'_, '_> {
    type Error = ArrowError;

    fn try_from(value: (i128, u8)) -> Result<Self, Self::Error> {
        Ok(Variant::Decimal16(VariantDecimal16::try_new(
            value.0, value.1,
        )?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_construct_short_string() {
        let short_string = ShortString::try_new("norm").expect("should fit in short string");
        assert_eq!(short_string.as_str(), "norm");

        let long_string = "a".repeat(MAX_SHORT_STRING_BYTES + 1);
        let res = ShortString::try_new(&long_string);
        assert!(res.is_err());
    }

    #[test]
    fn test_variant_decimal_conversion() {
        let decimal4 = VariantDecimal4::try_new(1234_i32, 2).unwrap();
        let variant = Variant::from(decimal4);
        assert_eq!(variant.as_decimal4(), Some(decimal4));

        let decimal8 = VariantDecimal8::try_new(12345678901_i64, 2).unwrap();
        let variant = Variant::from(decimal8);
        assert_eq!(variant.as_decimal8(), Some(decimal8));

        let decimal16 = VariantDecimal16::try_new(123456789012345678901234567890_i128, 2).unwrap();
        let variant = Variant::from(decimal16);
        assert_eq!(variant.as_decimal16(), Some(decimal16));
    }
}
