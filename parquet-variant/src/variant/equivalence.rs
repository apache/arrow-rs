// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Equivalence classes of [`Variant`] primitive types.
//!
//! The [Variant spec] groups physical types into equivalence classes: `int8` through `int64` and
//! the three decimal widths are all *Exact Numeric*, short and long strings are both *String*,
//! micro- and nanosecond timestamps are both *Timestamp*, and so on. Two values in the same class
//! that denote the same thing are logically equal, even though their bytes differ.
//!
//! [`Variant::canonical_primitive`] picks one representative encoding per value in each class, so
//! that logically equal primitives produce identical bytes. The rules match the canonical form
//! used by Apache Spark ([apache/spark#59669]).
//!
//! [Variant spec]: https://github.com/apache/parquet-format/blob/master/VariantEncoding.md#encoding-types
//! [apache/spark#59669]: https://github.com/apache/spark/pull/59669

use crate::{Variant, VariantDecimal4, VariantDecimal8, VariantDecimal16};

impl<'m, 'v> Variant<'m, 'v> {
    /// Returns the representative encoding of this value within its equivalence class.
    ///
    /// * Exact Numeric: the smallest of `INT8`, `INT16`, `INT32`, `INT64` that holds the value.
    ///   Decimals drop trailing zeros; an integral decimal that fits an `i64` becomes an integer,
    ///   otherwise the smallest of `DECIMAL4`, `DECIMAL8`, `DECIMAL16` that holds it is used.
    /// * Float and Double keep their type; `-0.0` becomes `0.0` and every NaN becomes the same
    ///   bit pattern ([`f32::NAN`] / [`f64::NAN`]).
    /// * String: the short-string encoding when the value fits (at most 63 bytes).
    /// * Timestamp and TimestampNTZ: microseconds unless the value has sub-microsecond digits.
    /// * Null, Boolean, Date, Time, Binary and UUID have one encoding and are returned unchanged.
    /// * Objects and lists are returned unchanged; their contents are not visited.
    ///
    /// The match is exhaustive on purpose: a type added to the spec must be classified here.
    pub(crate) fn canonical_primitive(&self) -> Variant<'m, 'v> {
        match self {
            Variant::Int8(v) => canonical_int(*v as i64),
            Variant::Int16(v) => canonical_int(*v as i64),
            Variant::Int32(v) => canonical_int(*v as i64),
            Variant::Int64(v) => canonical_int(*v),
            Variant::Decimal4(d) => canonical_decimal(d.integer() as i128, d.scale()),
            Variant::Decimal8(d) => canonical_decimal(d.integer() as i128, d.scale()),
            Variant::Decimal16(d) => canonical_decimal(d.integer(), d.scale()),
            Variant::Float(v) => Variant::Float(canonical_f32(*v)),
            Variant::Double(v) => Variant::Double(canonical_f64(*v)),
            // `From<&str>` picks the short-string encoding whenever the string fits
            Variant::String(s) => Variant::from(*s),
            Variant::ShortString(s) => Variant::from(s.as_str()),
            // `From<DateTime<Utc>>` / `From<NaiveDateTime>` pick micros unless there are
            // sub-microsecond digits
            Variant::TimestampMicros(v) | Variant::TimestampNanos(v) => Variant::from(*v),
            Variant::TimestampNtzMicros(v) | Variant::TimestampNtzNanos(v) => Variant::from(*v),
            Variant::Null
            | Variant::BooleanTrue
            | Variant::BooleanFalse
            | Variant::Date(_)
            | Variant::Time(_)
            | Variant::Binary(_)
            | Variant::Uuid(_)
            | Variant::Object(_)
            | Variant::List(_) => self.clone(),
        }
    }
}

/// The smallest integer type that holds `value`.
fn canonical_int(value: i64) -> Variant<'static, 'static> {
    if let Ok(v) = i8::try_from(value) {
        Variant::Int8(v)
    } else if let Ok(v) = i16::try_from(value) {
        Variant::Int16(v)
    } else if let Ok(v) = i32::try_from(value) {
        Variant::Int32(v)
    } else {
        Variant::Int64(value)
    }
}

/// Drops trailing zeros, promotes integral values that fit an `i64` to an integer, and otherwise
/// picks the smallest decimal type that holds the value.
fn canonical_decimal(mut integer: i128, mut scale: u8) -> Variant<'static, 'static> {
    while scale > 0 && integer % 10 == 0 {
        integer /= 10;
        scale -= 1;
    }
    if scale == 0
        && let Ok(v) = i64::try_from(integer)
    {
        return canonical_int(v);
    }
    if let Ok(v) = i32::try_from(integer)
        && let Ok(d) = VariantDecimal4::try_new(v, scale)
    {
        return Variant::Decimal4(d);
    }
    if let Ok(v) = i64::try_from(integer)
        && let Ok(d) = VariantDecimal8::try_new(v, scale)
    {
        return Variant::Decimal8(d);
    }
    // Stripping zeros only shrinks the unscaled value and the scale, so a value that was a valid
    // decimal still fits the widest type.
    Variant::Decimal16(
        VariantDecimal16::try_new(integer, scale).expect("stripped decimal fits DECIMAL16"),
    )
}

/// Maps `-0.0` to `0.0` and every NaN to [`f32::NAN`].
fn canonical_f32(value: f32) -> f32 {
    if value == 0.0 {
        0.0
    } else if value.is_nan() {
        f32::NAN
    } else {
        value
    }
}

/// Maps `-0.0` to `0.0` and every NaN to [`f64::NAN`].
fn canonical_f64(value: f64) -> f64 {
    if value == 0.0 {
        0.0
    } else if value.is_nan() {
        f64::NAN
    } else {
        value
    }
}
