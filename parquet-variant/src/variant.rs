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

pub use self::decimal::{VariantDecimal4, VariantDecimal8, VariantDecimal16, VariantDecimalType};
pub use self::list::VariantList;
pub use self::metadata::{EMPTY_VARIANT_METADATA, EMPTY_VARIANT_METADATA_BYTES, VariantMetadata};
pub use self::object::VariantObject;

// Publically export types used in the API
pub use half::f16;
pub use uuid::Uuid;

use crate::decoder::{
    self, VariantBasicType, VariantPrimitiveType, get_basic_type, get_primitive_type,
};
use crate::path::{VariantPath, VariantPathElement};
use crate::utils::{first_byte_from_slice, fits_precision, slice_from_slice};
use std::ops::Deref;

use arrow_schema::ArrowError;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};

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
#[derive(Clone, PartialEq)]
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
    /// Primitive (type_id=1): TIMESTAMP(isAdjustedToUTC=true, NANOS)
    TimestampNanos(DateTime<Utc>),
    /// Primitive (type_id=1): TIMESTAMP(isAdjustedToUTC=false, NANOS)
    TimestampNtzNanos(NaiveDateTime),
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
    /// Primitive (type_id=1): TIME(isAdjustedToUTC=false, MICROS)
    Time(NaiveTime),
    /// Primitive (type_id=1): UUID
    Uuid(Uuid),
    /// Short String (type_id=2): STRING
    ShortString(ShortString<'v>),
    // need both metadata & value
    /// Object (type_id=3): N/A
    Object(VariantObject<'m, 'v>),
    /// Array (type_id=4): N/A
    List(VariantList<'m, 'v>),
}

// We don't want this to grow because it could hurt performance of a frequently-created type.
const _: () = crate::utils::expect_size_of::<Variant>(80);

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
                VariantPrimitiveType::TimestampNanos => {
                    Variant::TimestampNanos(decoder::decode_timestamp_nanos(value_data)?)
                }
                VariantPrimitiveType::TimestampNtzNanos => {
                    Variant::TimestampNtzNanos(decoder::decode_timestampntz_nanos(value_data)?)
                }
                VariantPrimitiveType::Uuid => Variant::Uuid(decoder::decode_uuid(value_data)?),
                VariantPrimitiveType::Binary => {
                    Variant::Binary(decoder::decode_binary(value_data)?)
                }
                VariantPrimitiveType::String => {
                    Variant::String(decoder::decode_long_string(value_data)?)
                }
                VariantPrimitiveType::Time => Variant::Time(decoder::decode_time_ntz(value_data)?),
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
    /// Returns `Some(DateTime<Utc>)` for [`Variant::TimestampMicros`] variants,
    /// `None` for other variants.
    ///
    /// # Examples
    ///
    /// ```
    /// use parquet_variant::Variant;
    /// use chrono::NaiveDate;
    ///
    /// // you can extract a DateTime<Utc> from a UTC-adjusted variant
    /// let datetime = NaiveDate::from_ymd_opt(2025, 4, 16)
    ///     .unwrap()
    ///     .and_hms_milli_opt(12, 34, 56, 780)
    ///     .unwrap()
    ///     .and_utc();
    /// let v1 = Variant::from(datetime);
    /// assert_eq!(v1.as_timestamp_micros(), Some(datetime));
    ///
    /// // but not for other variants.
    /// let datetime_nanos = NaiveDate::from_ymd_opt(2025, 8, 14)
    ///     .unwrap()
    ///     .and_hms_nano_opt(12, 33, 54, 123456789)
    ///     .unwrap()
    ///     .and_utc();
    /// let v2 = Variant::from(datetime_nanos);
    /// assert_eq!(v2.as_timestamp_micros(), None);
    /// ```
    pub fn as_timestamp_micros(&self) -> Option<DateTime<Utc>> {
        match *self {
            Variant::TimestampMicros(d) => Some(d),
            _ => None,
        }
    }

    /// Converts this variant to a `NaiveDateTime` if possible.
    ///
    /// Returns `Some(NaiveDateTime)` for [`Variant::TimestampNtzMicros`] variants,
    /// `None` for other variants.
    ///
    /// # Examples
    ///
    /// ```
    /// use parquet_variant::Variant;
    /// use chrono::NaiveDate;
    ///
    /// // you can extract a NaiveDateTime from a non-UTC-adjusted variant
    /// let datetime = NaiveDate::from_ymd_opt(2025, 4, 16)
    ///     .unwrap()
    ///     .and_hms_milli_opt(12, 34, 56, 780)
    ///     .unwrap();
    /// let v1 = Variant::from(datetime);
    /// assert_eq!(v1.as_timestamp_ntz_micros(), Some(datetime));
    ///
    /// // but not for other variants.
    /// let datetime_nanos = NaiveDate::from_ymd_opt(2025, 8, 14)
    ///     .unwrap()
    ///     .and_hms_nano_opt(12, 33, 54, 123456789)
    ///     .unwrap();
    /// let v2 = Variant::from(datetime_nanos);
    /// assert_eq!(v2.as_timestamp_micros(), None);
    /// ```
    pub fn as_timestamp_ntz_micros(&self) -> Option<NaiveDateTime> {
        match *self {
            Variant::TimestampNtzMicros(d) => Some(d),
            _ => None,
        }
    }

    /// Converts this variant to a `DateTime<Utc>` if possible.
    ///
    /// Returns `Some(DateTime<Utc>)` for timestamp variants,
    /// `None` for other variants.
    ///
    /// # Examples
    ///
    /// ```
    /// use parquet_variant::Variant;
    /// use chrono::NaiveDate;
    ///
    /// // you can extract a DateTime<Utc> from a UTC-adjusted nanosecond-precision variant
    /// let datetime = NaiveDate::from_ymd_opt(2025, 4, 16)
    ///     .unwrap()
    ///     .and_hms_nano_opt(12, 34, 56, 789123456)
    ///     .unwrap()
    ///     .and_utc();
    /// let v1 = Variant::from(datetime);
    /// assert_eq!(v1.as_timestamp_nanos(), Some(datetime));
    ///
    /// // or from UTC-adjusted microsecond-precision variant
    /// let datetime_micros = NaiveDate::from_ymd_opt(2025, 8, 14)
    ///     .unwrap()
    ///     .and_hms_milli_opt(12, 33, 54, 123)
    ///     .unwrap()
    ///     .and_utc();
    /// // this will convert to `Variant::TimestampMicros`.
    /// let v2 = Variant::from(datetime_micros);
    /// assert_eq!(v2.as_timestamp_nanos(), Some(datetime_micros));
    ///
    /// // but not for other variants.
    /// let v3 = Variant::from("hello!");
    /// assert_eq!(v3.as_timestamp_nanos(), None);
    /// ```
    pub fn as_timestamp_nanos(&self) -> Option<DateTime<Utc>> {
        match *self {
            Variant::TimestampNanos(d) | Variant::TimestampMicros(d) => Some(d),
            _ => None,
        }
    }

    /// Converts this variant to a `NaiveDateTime` if possible.
    ///
    /// Returns `Some(NaiveDateTime)` for timestamp variants,
    /// `None` for other variants.
    ///
    /// # Examples
    ///
    /// ```
    /// use parquet_variant::Variant;
    /// use chrono::NaiveDate;
    ///
    /// // you can extract a NaiveDateTime from a non-UTC-adjusted variant
    /// let datetime = NaiveDate::from_ymd_opt(2025, 4, 16)
    ///     .unwrap()
    ///     .and_hms_nano_opt(12, 34, 56, 789123456)
    ///     .unwrap();
    /// let v1 = Variant::from(datetime);
    /// assert_eq!(v1.as_timestamp_ntz_nanos(), Some(datetime));
    ///
    /// // or from a microsecond-precision non-UTC-adjusted variant
    /// let datetime_micros = NaiveDate::from_ymd_opt(2025, 8, 14)
    ///     .unwrap()
    ///     .and_hms_milli_opt(12, 33, 54, 123)
    ///     .unwrap();
    /// // this will convert to `Variant::TimestampMicros`.
    /// let v2 = Variant::from(datetime_micros);
    /// assert_eq!(v2.as_timestamp_ntz_nanos(), Some(datetime_micros));
    ///
    /// // but not for other variants.
    /// let v3 = Variant::from("hello!");
    /// assert_eq!(v3.as_timestamp_ntz_nanos(), None);
    /// ```
    pub fn as_timestamp_ntz_nanos(&self) -> Option<NaiveDateTime> {
        match *self {
            Variant::TimestampNtzNanos(d) | Variant::TimestampNtzMicros(d) => Some(d),
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

    /// Converts this variant to a `uuid hyphenated string` if possible.
    ///
    /// Returns `Some(String)` for UUID variants, `None` for non-UUID variants.
    ///
    /// # Examples
    ///
    /// ```
    /// use parquet_variant::Variant;
    ///
    /// // You can extract a UUID from a UUID variant
    /// let s = uuid::Uuid::parse_str("67e55044-10b1-426f-9247-bb680e5fe0c8").unwrap();
    /// let v1 = Variant::Uuid(s);
    /// assert_eq!(s, v1.as_uuid().unwrap());
    /// assert_eq!("67e55044-10b1-426f-9247-bb680e5fe0c8", v1.as_uuid().unwrap().to_string());
    ///
    /// //but not from other variants
    /// let v2 = Variant::from(1234);
    /// assert_eq!(None, v2.as_uuid())
    /// ```
    pub fn as_uuid(&self) -> Option<Uuid> {
        match self {
            Variant::Uuid(u) => Some(*u),
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

    fn generic_convert_unsigned_primitive<T>(&self) -> Option<T>
    where
        T: TryFrom<i8> + TryFrom<i16> + TryFrom<i32> + TryFrom<i64> + TryFrom<i128>,
    {
        match *self {
            Variant::Int8(i) => i.try_into().ok(),
            Variant::Int16(i) => i.try_into().ok(),
            Variant::Int32(i) => i.try_into().ok(),
            Variant::Int64(i) => i.try_into().ok(),
            Variant::Decimal4(d) if d.scale() == 0 => d.integer().try_into().ok(),
            Variant::Decimal8(d) if d.scale() == 0 => d.integer().try_into().ok(),
            Variant::Decimal16(d) if d.scale() == 0 => d.integer().try_into().ok(),
            _ => None,
        }
    }

    /// Converts this variant to a `u8` if possible.
    ///
    /// Returns `Some(u8)` for integer variants that fit in `u8`
    /// `None` for non-integer variants or values that would overflow.
    ///
    /// # Examples
    ///
    /// ```
    ///  use parquet_variant::{Variant, VariantDecimal4};
    ///
    ///  // you can read an int64 variant into an u8
    ///  let v1 = Variant::from(123i64);
    ///  assert_eq!(v1.as_u8(), Some(123u8));
    ///
    ///  // or a Decimal4 with scale 0 into u8
    ///  let d = VariantDecimal4::try_new(26, 0).unwrap();
    ///  let v2 = Variant::from(d);
    ///  assert_eq!(v2.as_u8(), Some(26u8));
    ///
    ///  // but not a variant that can't fit into the range
    ///  let v3 = Variant::from(-1);
    ///  assert_eq!(v3.as_u8(), None);
    ///
    ///  // not a variant that decimal with scale not equal to zero
    ///  let d = VariantDecimal4::try_new(1, 2).unwrap();
    ///  let v4 = Variant::from(d);
    ///  assert_eq!(v4.as_u8(), None);
    ///
    ///  // or not a variant that cannot be cast into an integer
    ///  let v5 = Variant::from("hello!");
    ///  assert_eq!(v5.as_u8(), None);
    /// ```
    pub fn as_u8(&self) -> Option<u8> {
        self.generic_convert_unsigned_primitive::<u8>()
    }

    /// Converts this variant to an `u16` if possible.
    ///
    /// Returns `Some(u16)` for integer variants that fit in `u16`
    /// `None` for non-integer variants or values that would overflow.
    ///
    /// # Examples
    ///
    /// ```
    ///  use parquet_variant::{Variant, VariantDecimal4};
    ///
    ///  // you can read an int64 variant into an u16
    ///  let v1 = Variant::from(123i64);
    ///  assert_eq!(v1.as_u16(), Some(123u16));
    ///
    ///  // or a Decimal4 with scale 0 into u8
    ///  let d = VariantDecimal4::try_new(u16::MAX as i32, 0).unwrap();
    ///  let v2 = Variant::from(d);
    ///  assert_eq!(v2.as_u16(), Some(u16::MAX));
    ///
    ///  // but not a variant that can't fit into the range
    ///  let v3 = Variant::from(-1);
    ///  assert_eq!(v3.as_u16(), None);
    ///
    ///  // not a variant that decimal with scale not equal to zero
    ///  let d = VariantDecimal4::try_new(1, 2).unwrap();
    ///  let v4 = Variant::from(d);
    ///  assert_eq!(v4.as_u16(), None);
    ///
    ///  // or not a variant that cannot be cast into an integer
    ///  let v5 = Variant::from("hello!");
    ///  assert_eq!(v5.as_u16(), None);
    /// ```
    pub fn as_u16(&self) -> Option<u16> {
        self.generic_convert_unsigned_primitive::<u16>()
    }

    /// Converts this variant to an `u32` if possible.
    ///
    /// Returns `Some(u32)` for integer variants that fit in `u32`
    /// `None` for non-integer variants or values that would overflow.
    ///
    /// # Examples
    ///
    /// ```
    ///  use parquet_variant::{Variant, VariantDecimal8};
    ///
    ///  // you can read an int64 variant into an u32
    ///  let v1 = Variant::from(123i64);
    ///  assert_eq!(v1.as_u32(), Some(123u32));
    ///
    ///  // or a Decimal4 with scale 0 into u8
    ///  let d = VariantDecimal8::try_new(u32::MAX as i64, 0).unwrap();
    ///  let v2 = Variant::from(d);
    ///  assert_eq!(v2.as_u32(), Some(u32::MAX));
    ///
    ///  // but not a variant that can't fit into the range
    ///  let v3 = Variant::from(-1);
    ///  assert_eq!(v3.as_u32(), None);
    ///
    ///  // not a variant that decimal with scale not equal to zero
    ///  let d = VariantDecimal8::try_new(1, 2).unwrap();
    ///  let v4 = Variant::from(d);
    ///  assert_eq!(v4.as_u32(), None);
    ///
    ///  // or not a variant that cannot be cast into an integer
    ///  let v5 = Variant::from("hello!");
    ///  assert_eq!(v5.as_u32(), None);
    /// ```
    pub fn as_u32(&self) -> Option<u32> {
        self.generic_convert_unsigned_primitive::<u32>()
    }

    /// Converts this variant to an `u64` if possible.
    ///
    /// Returns `Some(u64)` for integer variants that fit in `u64`
    /// `None` for non-integer variants or values that would overflow.
    ///
    /// # Examples
    ///
    /// ```
    ///  use parquet_variant::{Variant, VariantDecimal16};
    ///
    ///  // you can read an int64 variant into an u64
    ///  let v1 = Variant::from(123i64);
    ///  assert_eq!(v1.as_u64(), Some(123u64));
    ///
    ///  // or a Decimal16 with scale 0 into u8
    ///  let d = VariantDecimal16::try_new(u64::MAX as i128, 0).unwrap();
    ///  let v2 = Variant::from(d);
    ///  assert_eq!(v2.as_u64(), Some(u64::MAX));
    ///
    ///  // but not a variant that can't fit into the range
    ///  let v3 = Variant::from(-1);
    ///  assert_eq!(v3.as_u64(), None);
    ///
    ///  // not a variant that decimal with scale not equal to zero
    /// let d = VariantDecimal16::try_new(1, 2).unwrap();
    ///  let v4 = Variant::from(d);
    ///  assert_eq!(v4.as_u64(), None);
    ///
    ///  // or not a variant that cannot be cast into an integer
    ///  let v5 = Variant::from("hello!");
    ///  assert_eq!(v5.as_u64(), None);
    /// ```
    pub fn as_u64(&self) -> Option<u64> {
        self.generic_convert_unsigned_primitive::<u64>()
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

    /// Converts this variant to an `f16` if possible.
    ///
    /// Returns `Some(f16)` for floating point values, and integers with up to 11 bits of
    /// precision. `None` otherwise.
    ///
    /// # Example
    ///
    /// ```
    /// use parquet_variant::Variant;
    /// use half::f16;
    ///
    /// // you can extract an f16 from a float variant
    /// let v1 = Variant::from(std::f32::consts::PI);
    /// assert_eq!(v1.as_f16(), Some(f16::from_f32(std::f32::consts::PI)));
    ///
    /// // and from a double variant (with loss of precision to nearest f16)
    /// let v2 = Variant::from(std::f64::consts::PI);
    /// assert_eq!(v2.as_f16(), Some(f16::from_f64(std::f64::consts::PI)));
    ///
    /// // and from integers with no more than 11 bits of precision
    /// let v3 = Variant::from(2047);
    /// assert_eq!(v3.as_f16(), Some(f16::from_f32(2047.0)));
    ///
    /// // but not from other variants
    /// let v4 = Variant::from("hello!");
    /// assert_eq!(v4.as_f16(), None);
    pub fn as_f16(&self) -> Option<f16> {
        match *self {
            Variant::Float(i) => Some(f16::from_f32(i)),
            Variant::Double(i) => Some(f16::from_f64(i)),
            Variant::Int8(i) => Some(i.into()),
            Variant::Int16(i) if fits_precision::<11>(i) => Some(f16::from_f32(i as _)),
            Variant::Int32(i) if fits_precision::<11>(i) => Some(f16::from_f32(i as _)),
            Variant::Int64(i) if fits_precision::<11>(i) => Some(f16::from_f32(i as _)),
            _ => None,
        }
    }

    /// Converts this variant to an `f32` if possible.
    ///
    /// Returns `Some(f32)` for floating point values, and integer values with up to 24 bits of
    /// precision.  `None` otherwise.
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
    /// // and from integers with no more than 24 bits of precision
    /// let v3 = Variant::from(16777215i64);
    /// assert_eq!(v3.as_f32(), Some(16777215.0));
    ///
    /// // but not from other variants
    /// let v4 = Variant::from("hello!");
    /// assert_eq!(v4.as_f32(), None);
    /// ```
    #[allow(clippy::cast_possible_truncation)]
    pub fn as_f32(&self) -> Option<f32> {
        match *self {
            Variant::Float(i) => Some(i),
            Variant::Double(i) => Some(i as f32),
            Variant::Int8(i) => Some(i.into()),
            Variant::Int16(i) => Some(i.into()),
            Variant::Int32(i) if fits_precision::<24>(i) => Some(i as _),
            Variant::Int64(i) if fits_precision::<24>(i) => Some(i as _),
            _ => None,
        }
    }

    /// Converts this variant to an `f64` if possible.
    ///
    /// Returns `Some(f64)` for floating point values, and integer values with up to 53 bits of
    /// precision.  `None` otherwise.
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
    /// // and from integers with no more than 53 bits of precision
    /// let v3 = Variant::from(9007199254740991i64);
    /// assert_eq!(v3.as_f64(), Some(9007199254740991.0));
    ///
    /// // but not from other variants
    /// let v4 = Variant::from("hello!");
    /// assert_eq!(v4.as_f64(), None);
    /// ```
    pub fn as_f64(&self) -> Option<f64> {
        match *self {
            Variant::Float(i) => Some(i.into()),
            Variant::Double(i) => Some(i),
            Variant::Int8(i) => Some(i.into()),
            Variant::Int16(i) => Some(i.into()),
            Variant::Int32(i) => Some(i.into()),
            Variant::Int64(i) if fits_precision::<53>(i) => Some(i as _),
            _ => None,
        }
    }

    /// Converts this variant to an `Object` if it is an [`VariantObject`].
    ///
    /// Returns `Some(&VariantObject)` for object variants,
    /// `None` for non-object variants.
    ///
    /// See [`Self::get_path`] to dynamically traverse objects
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

    /// If this is an object and the requested field name exists, retrieves the corresponding field
    /// value. Otherwise, returns None.
    ///
    /// This is shorthand for [`Self::as_object`] followed by [`VariantObject::get`].
    ///
    /// # Examples
    /// ```
    /// # use parquet_variant::{Variant, VariantBuilder, VariantObject};
    /// # let mut builder = VariantBuilder::new();
    /// # let mut obj = builder.new_object();
    /// # obj.insert("name", "John");
    /// # obj.finish();
    /// # let (metadata, value) = builder.finish();
    /// // object that is {"name": "John"}
    ///  let variant = Variant::new(&metadata, &value);
    /// // use the `get_object_field` method to access the object
    /// let obj = variant.get_object_field("name");
    /// assert_eq!(obj, Some(Variant::from("John")));
    /// let obj = variant.get_object_field("foo");
    /// assert!(obj.is_none());
    /// ```
    pub fn get_object_field(&self, field_name: &str) -> Option<Self> {
        match self {
            Variant::Object(object) => object.get(field_name),
            _ => None,
        }
    }

    /// Converts this variant to a `List` if it is a [`VariantList`].
    ///
    /// Returns `Some(&VariantList)` for list variants,
    /// `None` for non-list variants.
    ///
    /// See [`Self::get_path`] to dynamically traverse lists
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

    /// Converts this variant to a `NaiveTime` if possible.
    ///
    /// Returns `Some(NaiveTime)` for `Variant::Time`,
    /// `None` for non-Time variants.
    ///
    /// # Example
    ///
    /// ```
    /// use chrono::NaiveTime;
    /// use parquet_variant::Variant;
    ///
    /// // you can extract a `NaiveTime` from a `Variant::Time`
    /// let time = NaiveTime::from_hms_micro_opt(1, 2, 3, 4).unwrap();
    /// let v1 = Variant::from(time);
    /// assert_eq!(Some(time), v1.as_time_utc());
    ///
    /// // but not from other variants.
    /// let v2 = Variant::from("Hello");
    /// assert_eq!(None, v2.as_time_utc());
    /// ```
    pub fn as_time_utc(&'m self) -> Option<NaiveTime> {
        if let Variant::Time(time) = self {
            Some(*time)
        } else {
            None
        }
    }

    /// If this is a list and the requested index is in bounds, retrieves the corresponding
    /// element. Otherwise, returns None.
    ///
    /// This is shorthand for [`Self::as_list`] followed by [`VariantList::get`].
    ///
    /// # Examples
    /// ```
    /// # use parquet_variant::{Variant, VariantBuilder, VariantList};
    /// # let mut builder = VariantBuilder::new();
    /// # let mut list = builder.new_list();
    /// # list.append_value("John");
    /// # list.append_value("Doe");
    /// # list.finish();
    /// # let (metadata, value) = builder.finish();
    /// // list that is ["John", "Doe"]
    /// let variant = Variant::new(&metadata, &value);
    /// // use the `get_list_element` method to access the list
    /// assert_eq!(variant.get_list_element(0), Some(Variant::from("John")));
    /// assert_eq!(variant.get_list_element(1), Some(Variant::from("Doe")));
    /// assert!(variant.get_list_element(2).is_none());
    /// ```
    pub fn get_list_element(&self, index: usize) -> Option<Self> {
        match self {
            Variant::List(list) => list.get(index),
            _ => None,
        }
    }

    /// Return the metadata dictionary associated with this variant value.
    pub fn metadata(&self) -> &VariantMetadata<'m> {
        match self {
            Variant::Object(VariantObject { metadata, .. })
            | Variant::List(VariantList { metadata, .. }) => metadata,
            _ => &EMPTY_VARIANT_METADATA,
        }
    }

    /// Return a new Variant with the path followed.
    ///
    /// If the path is not found, `None` is returned.
    ///
    /// # Example
    /// ```
    /// # use parquet_variant::{Variant, VariantBuilder, VariantObject, VariantPath};
    /// # let mut builder = VariantBuilder::new();
    /// # let mut obj = builder.new_object();
    /// # let mut list = obj.new_list("foo");
    /// # list.append_value("bar");
    /// # list.append_value("baz");
    /// # list.finish();
    /// # obj.finish();
    /// # let (metadata, value) = builder.finish();
    /// // given a variant like `{"foo": ["bar", "baz"]}`
    /// let variant = Variant::new(&metadata, &value);
    /// // Accessing a non existent path returns None
    /// assert_eq!(variant.get_path(&VariantPath::from("non_existent")), None);
    /// // Access obj["foo"]
    /// let path = VariantPath::from("foo");
    /// let foo = variant.get_path(&path).expect("field `foo` should exist");
    /// assert!(foo.as_list().is_some(), "field `foo` should be a list");
    /// // Access foo[0]
    /// let path = VariantPath::from(0);
    /// let bar = foo.get_path(&path).expect("element 0 should exist");
    /// // bar is a string
    /// assert_eq!(bar.as_string(), Some("bar"));
    /// // You can also access nested paths
    /// let path = VariantPath::from("foo").join(0);
    /// assert_eq!(variant.get_path(&path).unwrap(), bar);
    /// ```
    pub fn get_path(&self, path: &VariantPath) -> Option<Variant<'_, '_>> {
        path.iter()
            .try_fold(self.clone(), |output, element| match element {
                VariantPathElement::Field { name } => output.get_object_field(name),
                VariantPathElement::Index { index } => output.get_list_element(*index),
            })
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

impl From<u8> for Variant<'_, '_> {
    fn from(value: u8) -> Self {
        // if it fits in i8, use that, otherwise use i16
        if let Ok(value) = i8::try_from(value) {
            Variant::Int8(value)
        } else {
            Variant::Int16(i16::from(value))
        }
    }
}

impl From<u16> for Variant<'_, '_> {
    fn from(value: u16) -> Self {
        // if it fits in i16, use that, otherwise use i32
        if let Ok(value) = i16::try_from(value) {
            Variant::Int16(value)
        } else {
            Variant::Int32(i32::from(value))
        }
    }
}
impl From<u32> for Variant<'_, '_> {
    fn from(value: u32) -> Self {
        // if it fits in i32, use that, otherwise use i64
        if let Ok(value) = i32::try_from(value) {
            Variant::Int32(value)
        } else {
            Variant::Int64(i64::from(value))
        }
    }
}

impl From<u64> for Variant<'_, '_> {
    fn from(value: u64) -> Self {
        // if it fits in i64, use that, otherwise use Decimal16
        if let Ok(value) = i64::try_from(value) {
            Variant::Int64(value)
        } else {
            // u64 max is 18446744073709551615, which fits in i128
            Variant::Decimal16(VariantDecimal16::try_new(i128::from(value), 0).unwrap())
        }
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

impl From<half::f16> for Variant<'_, '_> {
    fn from(value: half::f16) -> Self {
        Variant::Float(value.into())
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
        if value.nanosecond() % 1000 > 0 {
            Variant::TimestampNanos(value)
        } else {
            Variant::TimestampMicros(value)
        }
    }
}

impl From<NaiveDateTime> for Variant<'_, '_> {
    fn from(value: NaiveDateTime) -> Self {
        if value.nanosecond() % 1000 > 0 {
            Variant::TimestampNtzNanos(value)
        } else {
            Variant::TimestampNtzMicros(value)
        }
    }
}

impl<'v> From<&'v [u8]> for Variant<'_, 'v> {
    fn from(value: &'v [u8]) -> Self {
        Variant::Binary(value)
    }
}

impl From<NaiveTime> for Variant<'_, '_> {
    fn from(value: NaiveTime) -> Self {
        Variant::Time(value)
    }
}

impl From<Uuid> for Variant<'_, '_> {
    fn from(value: Uuid) -> Self {
        Variant::Uuid(value)
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

// helper to print <invalid> instead of "<invalid>" in debug mode when a VariantObject or VariantList contains invalid values.
struct InvalidVariant;

impl std::fmt::Debug for InvalidVariant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<invalid>")
    }
}

// helper to print binary data in hex format in debug mode, as space-separated hex byte values.
struct HexString<'a>(&'a [u8]);

impl<'a> std::fmt::Debug for HexString<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some((first, rest)) = self.0.split_first() {
            write!(f, "{:02x}", first)?;
            for b in rest {
                write!(f, " {:02x}", b)?;
            }
        }
        Ok(())
    }
}

impl std::fmt::Debug for Variant<'_, '_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Variant::Null => write!(f, "Null"),
            Variant::BooleanTrue => write!(f, "BooleanTrue"),
            Variant::BooleanFalse => write!(f, "BooleanFalse"),
            Variant::Int8(v) => f.debug_tuple("Int8").field(v).finish(),
            Variant::Int16(v) => f.debug_tuple("Int16").field(v).finish(),
            Variant::Int32(v) => f.debug_tuple("Int32").field(v).finish(),
            Variant::Int64(v) => f.debug_tuple("Int64").field(v).finish(),
            Variant::Float(v) => f.debug_tuple("Float").field(v).finish(),
            Variant::Double(v) => f.debug_tuple("Double").field(v).finish(),
            Variant::Decimal4(d) => f.debug_tuple("Decimal4").field(d).finish(),
            Variant::Decimal8(d) => f.debug_tuple("Decimal8").field(d).finish(),
            Variant::Decimal16(d) => f.debug_tuple("Decimal16").field(d).finish(),
            Variant::Date(d) => f.debug_tuple("Date").field(d).finish(),
            Variant::TimestampMicros(ts) => f.debug_tuple("TimestampMicros").field(ts).finish(),
            Variant::TimestampNtzMicros(ts) => {
                f.debug_tuple("TimestampNtzMicros").field(ts).finish()
            }
            Variant::TimestampNanos(ts) => f.debug_tuple("TimestampNanos").field(ts).finish(),
            Variant::TimestampNtzNanos(ts) => f.debug_tuple("TimestampNtzNanos").field(ts).finish(),
            Variant::Binary(bytes) => write!(f, "Binary({:?})", HexString(bytes)),
            Variant::String(s) => f.debug_tuple("String").field(s).finish(),
            Variant::Time(s) => f.debug_tuple("Time").field(s).finish(),
            Variant::ShortString(s) => f.debug_tuple("ShortString").field(s).finish(),
            Variant::Uuid(uuid) => f.debug_tuple("Uuid").field(&uuid).finish(),
            Variant::Object(obj) => {
                let mut map = f.debug_map();
                for res in obj.iter_try() {
                    match res {
                        Ok((k, v)) => map.entry(&k, &v),
                        Err(_) => map.entry(&InvalidVariant, &InvalidVariant),
                    };
                }
                map.finish()
            }
            Variant::List(arr) => {
                let mut list = f.debug_list();
                for res in arr.iter_try() {
                    match res {
                        Ok(v) => list.entry(&v),
                        Err(_) => list.entry(&InvalidVariant),
                    };
                }
                list.finish()
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_empty_variant_will_fail() {
        let metadata = VariantMetadata::try_new(&[1, 0, 0]).unwrap();

        let err = Variant::try_new_with_metadata(metadata, &[]).unwrap_err();

        assert!(matches!(
            err,
            ArrowError::InvalidArgumentError(ref msg) if msg == "Received empty bytes"));
    }

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

    #[test]
    fn test_variant_all_subtypes_debug() {
        use crate::VariantBuilder;

        let mut builder = VariantBuilder::new();

        // Create a root object that contains one of every variant subtype
        let mut root_obj = builder.new_object();

        // Add primitive types
        root_obj.insert("null", ());
        root_obj.insert("boolean_true", true);
        root_obj.insert("boolean_false", false);
        root_obj.insert("int8", 42i8);
        root_obj.insert("int16", 1234i16);
        root_obj.insert("int32", 123456i32);
        root_obj.insert("int64", 1234567890123456789i64);
        root_obj.insert("float", 1.234f32);
        root_obj.insert("double", 1.23456789f64);

        // Add date and timestamp types
        let date = chrono::NaiveDate::from_ymd_opt(2024, 12, 25).unwrap();
        root_obj.insert("date", date);

        let timestamp_utc = chrono::NaiveDate::from_ymd_opt(2024, 12, 25)
            .unwrap()
            .and_hms_milli_opt(15, 30, 45, 123)
            .unwrap()
            .and_utc();
        root_obj.insert("timestamp_micros", Variant::TimestampMicros(timestamp_utc));

        let timestamp_ntz = chrono::NaiveDate::from_ymd_opt(2024, 12, 25)
            .unwrap()
            .and_hms_milli_opt(15, 30, 45, 123)
            .unwrap();
        root_obj.insert(
            "timestamp_ntz_micros",
            Variant::TimestampNtzMicros(timestamp_ntz),
        );

        let timestamp_nanos_utc = chrono::NaiveDate::from_ymd_opt(2025, 8, 15)
            .unwrap()
            .and_hms_nano_opt(12, 3, 4, 123456789)
            .unwrap()
            .and_utc();
        root_obj.insert(
            "timestamp_nanos",
            Variant::TimestampNanos(timestamp_nanos_utc),
        );

        let timestamp_ntz_nanos = chrono::NaiveDate::from_ymd_opt(2025, 8, 15)
            .unwrap()
            .and_hms_nano_opt(12, 3, 4, 123456789)
            .unwrap();
        root_obj.insert(
            "timestamp_ntz_nanos",
            Variant::TimestampNtzNanos(timestamp_ntz_nanos),
        );

        // Add decimal types
        let decimal4 = VariantDecimal4::try_new(1234i32, 2).unwrap();
        root_obj.insert("decimal4", decimal4);

        let decimal8 = VariantDecimal8::try_new(123456789i64, 3).unwrap();
        root_obj.insert("decimal8", decimal8);

        let decimal16 = VariantDecimal16::try_new(123456789012345678901234567890i128, 4).unwrap();
        root_obj.insert("decimal16", decimal16);

        // Add binary and string types
        let binary_data = b"\x01\x02\x03\x04\xde\xad\xbe\xef";
        root_obj.insert("binary", binary_data.as_slice());

        let long_string =
            "This is a long string that exceeds the short string limit and contains emoji 🦀";
        root_obj.insert("string", long_string);
        root_obj.insert("short_string", "Short string with emoji 🎉");
        let time = NaiveTime::from_hms_micro_opt(1, 2, 3, 4).unwrap();
        root_obj.insert("time", time);

        // Add uuid
        let uuid = Uuid::parse_str("67e55044-10b1-426f-9247-bb680e5fe0c8").unwrap();
        root_obj.insert("uuid", Variant::Uuid(uuid));

        // Add nested object
        let mut nested_obj = root_obj.new_object("nested_object");
        nested_obj.insert("inner_key1", "inner_value1");
        nested_obj.insert("inner_key2", 999i32);
        nested_obj.finish();

        // Add list with mixed types
        let mut mixed_list = root_obj.new_list("mixed_list");
        mixed_list.append_value(1i32);
        mixed_list.append_value("two");
        mixed_list.append_value(true);
        mixed_list.append_value(4.0f32);
        mixed_list.append_value(());

        // Add nested list inside the mixed list
        let mut nested_list = mixed_list.new_list();
        nested_list.append_value("nested");
        nested_list.append_value(10i8);
        nested_list.finish();

        mixed_list.finish();

        root_obj.finish();

        let (metadata, value) = builder.finish();
        let variant = Variant::try_new(&metadata, &value).unwrap();

        // Test Debug formatter (?)
        let debug_output = format!("{:?}", variant);

        // Verify that the debug output contains all the expected types
        assert!(debug_output.contains("\"null\": Null"));
        assert!(debug_output.contains("\"boolean_true\": BooleanTrue"));
        assert!(debug_output.contains("\"boolean_false\": BooleanFalse"));
        assert!(debug_output.contains("\"int8\": Int8(42)"));
        assert!(debug_output.contains("\"int16\": Int16(1234)"));
        assert!(debug_output.contains("\"int32\": Int32(123456)"));
        assert!(debug_output.contains("\"int64\": Int64(1234567890123456789)"));
        assert!(debug_output.contains("\"float\": Float(1.234)"));
        assert!(debug_output.contains("\"double\": Double(1.23456789"));
        assert!(debug_output.contains("\"date\": Date(2024-12-25)"));
        assert!(debug_output.contains("\"timestamp_micros\": TimestampMicros("));
        assert!(debug_output.contains("\"timestamp_ntz_micros\": TimestampNtzMicros("));
        assert!(debug_output.contains("\"timestamp_nanos\": TimestampNanos("));
        assert!(debug_output.contains("\"timestamp_ntz_nanos\": TimestampNtzNanos("));
        assert!(debug_output.contains("\"decimal4\": Decimal4("));
        assert!(debug_output.contains("\"decimal8\": Decimal8("));
        assert!(debug_output.contains("\"decimal16\": Decimal16("));
        assert!(debug_output.contains("\"binary\": Binary(01 02 03 04 de ad be ef)"));
        assert!(debug_output.contains("\"string\": String("));
        assert!(debug_output.contains("\"short_string\": ShortString("));
        assert!(debug_output.contains("\"uuid\": Uuid(67e55044-10b1-426f-9247-bb680e5fe0c8)"));
        assert!(debug_output.contains("\"time\": Time(01:02:03.000004)"));
        assert!(debug_output.contains("\"nested_object\":"));
        assert!(debug_output.contains("\"mixed_list\":"));

        let expected = r#"{"binary": Binary(01 02 03 04 de ad be ef), "boolean_false": BooleanFalse, "boolean_true": BooleanTrue, "date": Date(2024-12-25), "decimal16": Decimal16(VariantDecimal16 { integer: 123456789012345678901234567890, scale: 4 }), "decimal4": Decimal4(VariantDecimal4 { integer: 1234, scale: 2 }), "decimal8": Decimal8(VariantDecimal8 { integer: 123456789, scale: 3 }), "double": Double(1.23456789), "float": Float(1.234), "int16": Int16(1234), "int32": Int32(123456), "int64": Int64(1234567890123456789), "int8": Int8(42), "mixed_list": [Int32(1), ShortString(ShortString("two")), BooleanTrue, Float(4.0), Null, [ShortString(ShortString("nested")), Int8(10)]], "nested_object": {"inner_key1": ShortString(ShortString("inner_value1")), "inner_key2": Int32(999)}, "null": Null, "short_string": ShortString(ShortString("Short string with emoji 🎉")), "string": String("This is a long string that exceeds the short string limit and contains emoji 🦀"), "time": Time(01:02:03.000004), "timestamp_micros": TimestampMicros(2024-12-25T15:30:45.123Z), "timestamp_nanos": TimestampNanos(2025-08-15T12:03:04.123456789Z), "timestamp_ntz_micros": TimestampNtzMicros(2024-12-25T15:30:45.123), "timestamp_ntz_nanos": TimestampNtzNanos(2025-08-15T12:03:04.123456789), "uuid": Uuid(67e55044-10b1-426f-9247-bb680e5fe0c8)}"#;
        assert_eq!(debug_output, expected);

        // Test alternate Debug formatter (#?)
        let alt_debug_output = format!("{:#?}", variant);
        let expected = r#"{
    "binary": Binary(01 02 03 04 de ad be ef),
    "boolean_false": BooleanFalse,
    "boolean_true": BooleanTrue,
    "date": Date(
        2024-12-25,
    ),
    "decimal16": Decimal16(
        VariantDecimal16 {
            integer: 123456789012345678901234567890,
            scale: 4,
        },
    ),
    "decimal4": Decimal4(
        VariantDecimal4 {
            integer: 1234,
            scale: 2,
        },
    ),
    "decimal8": Decimal8(
        VariantDecimal8 {
            integer: 123456789,
            scale: 3,
        },
    ),
    "double": Double(
        1.23456789,
    ),
    "float": Float(
        1.234,
    ),
    "int16": Int16(
        1234,
    ),
    "int32": Int32(
        123456,
    ),
    "int64": Int64(
        1234567890123456789,
    ),
    "int8": Int8(
        42,
    ),
    "mixed_list": [
        Int32(
            1,
        ),
        ShortString(
            ShortString(
                "two",
            ),
        ),
        BooleanTrue,
        Float(
            4.0,
        ),
        Null,
        [
            ShortString(
                ShortString(
                    "nested",
                ),
            ),
            Int8(
                10,
            ),
        ],
    ],
    "nested_object": {
        "inner_key1": ShortString(
            ShortString(
                "inner_value1",
            ),
        ),
        "inner_key2": Int32(
            999,
        ),
    },
    "null": Null,
    "short_string": ShortString(
        ShortString(
            "Short string with emoji 🎉",
        ),
    ),
    "string": String(
        "This is a long string that exceeds the short string limit and contains emoji 🦀",
    ),
    "time": Time(
        01:02:03.000004,
    ),
    "timestamp_micros": TimestampMicros(
        2024-12-25T15:30:45.123Z,
    ),
    "timestamp_nanos": TimestampNanos(
        2025-08-15T12:03:04.123456789Z,
    ),
    "timestamp_ntz_micros": TimestampNtzMicros(
        2024-12-25T15:30:45.123,
    ),
    "timestamp_ntz_nanos": TimestampNtzNanos(
        2025-08-15T12:03:04.123456789,
    ),
    "uuid": Uuid(
        67e55044-10b1-426f-9247-bb680e5fe0c8,
    ),
}"#;
        assert_eq!(alt_debug_output, expected);
    }
}
