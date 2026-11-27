// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Canonical encoding of [`Variant`] values.
//!
//! The Variant binary encoding allows one logical value to be written in many ways: the metadata
//! dictionary may hold unused or unsorted field names, an integer may use a wider width than it
//! needs, a decimal may carry trailing zeros, and so on. Byte-oriented operations such as hashing
//! for `GROUP BY` or a join therefore treat equal values as different.
//!
//! [`Variant::canonicalize`] rebuilds a value so that equal values share one encoding.

use std::collections::BTreeSet;

use arrow_schema::ArrowError;

use crate::{ObjectFieldBuilder, Variant, VariantBuilder, VariantBuilderExt};

impl Variant<'_, '_> {
    /// Returns the canonical `(metadata, value)` encoding of this variant.
    ///
    /// Two variants that are logically equal canonicalize to identical bytes, so the result can be
    /// hashed or compared byte-wise. The rules follow the canonical form used by Apache Spark
    /// ([apache/spark#59669]):
    ///
    /// * The metadata dictionary holds only the field names the value references, sorted by their
    ///   unsigned UTF-8 bytes, with the smallest offset width that fits.
    /// * Object fields are written in dictionary (key) order; list elements keep their order.
    /// * Each primitive uses the representative encoding of its equivalence class in the Variant
    ///   spec, see [`Variant::canonical_primitive`]: the narrowest integer, decimals with trailing
    ///   zeros dropped (promoted to an integer when integral and within `i64`), `-0.0` as `0.0`
    ///   and one NaN pattern per float type, short strings when they fit, microsecond timestamps
    ///   unless sub-microsecond digits are present. Null, booleans, dates, times, binary and UUID
    ///   have one encoding already.
    ///
    /// Canonicalizing an already canonical value returns the same bytes.
    ///
    /// Note that the canonical form may change the Rust-level type of a value: `Variant::Int64(1)`
    /// canonicalizes to the bytes of `Variant::Int8(1)`, and `Variant::Decimal4(1.0)` to those of
    /// `Variant::Int8(1)`.
    ///
    /// # Errors
    ///
    /// Returns an error if the variant does not pass [full validation], for example when an object
    /// holds unsorted or repeated field names. Validation is cached, so this is free for values
    /// created with [`Variant::try_new`].
    ///
    /// # Example
    /// ```
    /// # use parquet_variant::{Variant, VariantBuilder};
    /// // Two builders write the same object with a different dictionary order
    /// let mut a = VariantBuilder::new().with_field_names(["b", "a", "unused"]);
    /// a.new_object().with_field("a", 1i64).with_field("b", 2i8).finish();
    /// let (am, av) = a.finish();
    ///
    /// let mut b = VariantBuilder::new();
    /// b.new_object().with_field("a", 1i8).with_field("b", 2i32).finish();
    /// let (bm, bv) = b.finish();
    ///
    /// assert_ne!((&am, &av), (&bm, &bv));
    /// let a_canonical = Variant::try_new(&am, &av).unwrap().canonicalize().unwrap();
    /// let b_canonical = Variant::try_new(&bm, &bv).unwrap().canonicalize().unwrap();
    /// assert_eq!(a_canonical, b_canonical);
    /// ```
    ///
    /// [apache/spark#59669]: https://github.com/apache/spark/pull/59669
    /// [full validation]: Variant#Validation
    pub fn canonicalize(&self) -> Result<(Vec<u8>, Vec<u8>), ArrowError> {
        // Full validation guarantees that object fields are unique and iterate in key order.
        let value = self.clone().with_full_validation()?;
        let mut field_names = BTreeSet::new();
        collect_field_names(&value, &mut field_names);
        // `str` orders by bytes, so the set is already in unsigned UTF-8 order.
        let mut builder = VariantBuilder::new().with_field_names(field_names.iter().copied());
        append_canonical(&mut builder, &value)?;
        builder.try_finish()
    }
}

/// Collects every field name referenced anywhere inside the validated `value`.
fn collect_field_names<'m>(value: &Variant<'m, '_>, names: &mut BTreeSet<&'m str>) {
    match value {
        Variant::Object(object) => {
            for (name, value) in object.iter() {
                names.insert(name);
                collect_field_names(&value, names);
            }
        }
        Variant::List(list) => {
            for item in list.iter() {
                collect_field_names(&item, names);
            }
        }
        _ => {}
    }
}

/// Appends the validated `value` to `builder` in canonical form. The builder's metadata dictionary
/// must already hold every field name `value` references, in sorted order.
fn append_canonical<B: VariantBuilderExt>(
    builder: &mut B,
    value: &Variant<'_, '_>,
) -> Result<(), ArrowError> {
    match value {
        Variant::Object(object) => {
            let mut object_builder = builder.try_new_object()?;
            for (name, value) in object.iter() {
                let mut field_builder = ObjectFieldBuilder::new(name, &mut object_builder);
                append_canonical(&mut field_builder, &value)?;
            }
            object_builder.finish();
        }
        Variant::List(list) => {
            let mut list_builder = builder.try_new_list()?;
            for item in list.iter() {
                append_canonical(&mut list_builder, &item)?;
            }
            list_builder.finish();
        }
        primitive => builder.append_value(primitive.canonical_primitive()),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{VariantDecimal4, VariantDecimal8, VariantDecimal16, VariantMetadata};
    use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
    use uuid::Uuid;

    fn canonical_of<'m, 'v>(value: impl Into<Variant<'m, 'v>>) -> (Vec<u8>, Vec<u8>) {
        let mut builder = VariantBuilder::new();
        builder.append_value(value);
        let (metadata, value) = builder.finish();
        Variant::try_new(&metadata, &value)
            .unwrap()
            .canonicalize()
            .unwrap()
    }

    fn encode<'m, 'v>(value: impl Into<Variant<'m, 'v>>) -> (Vec<u8>, Vec<u8>) {
        let mut builder = VariantBuilder::new();
        builder.append_value(value);
        builder.finish()
    }

    fn assert_canonical_eq<'m, 'v>(
        input: impl Into<Variant<'m, 'v>>,
        expected: impl Into<Variant<'m, 'v>>,
    ) {
        let (metadata, value) = canonical_of(input);
        assert_eq!(
            Variant::try_new(&metadata, &value).unwrap(),
            expected.into()
        );
    }

    #[test]
    fn narrows_integers() {
        assert_canonical_eq(Variant::Int64(1), Variant::Int8(1));
        assert_canonical_eq(Variant::Int32(-128), Variant::Int8(-128));
        assert_canonical_eq(Variant::Int64(-129), Variant::Int16(-129));
        assert_canonical_eq(Variant::Int64(32_768), Variant::Int32(32_768));
        assert_canonical_eq(Variant::Int64(1 << 40), Variant::Int64(1 << 40));
        assert_canonical_eq(Variant::Int64(i64::MIN), Variant::Int64(i64::MIN));
    }

    #[test]
    fn decimals_drop_trailing_zeros_and_promote() {
        // 1.0 -> 1
        assert_canonical_eq(VariantDecimal4::try_new(10, 1).unwrap(), Variant::Int8(1));
        // 0.0000 -> 0
        assert_canonical_eq(VariantDecimal8::try_new(0, 4).unwrap(), Variant::Int8(0));
        // 12.50 -> 12.5
        assert_canonical_eq(
            VariantDecimal16::try_new(1250, 2).unwrap(),
            VariantDecimal4::try_new(125, 1).unwrap(),
        );
        // -1.25 stays, but in the narrowest decimal
        assert_canonical_eq(
            VariantDecimal16::try_new(-125, 2).unwrap(),
            VariantDecimal4::try_new(-125, 2).unwrap(),
        );
        // 10 digits need DECIMAL8
        assert_canonical_eq(
            VariantDecimal16::try_new(1_234_567_891, 1).unwrap(),
            VariantDecimal8::try_new(1_234_567_891, 1).unwrap(),
        );
        // integral but wider than i64 stays a decimal
        let wide = i64::MAX as i128 + 1;
        assert_canonical_eq(
            VariantDecimal16::try_new(wide * 10, 1).unwrap(),
            VariantDecimal16::try_new(wide, 0).unwrap(),
        );
        // scale wider than DECIMAL4 precision forces a wider type even for a small integer
        assert_canonical_eq(
            VariantDecimal16::try_new(1, 12).unwrap(),
            VariantDecimal8::try_new(1, 12).unwrap(),
        );
    }

    #[test]
    fn floats_keep_type_and_normalize_zero_and_nan() {
        let (_, pos) = canonical_of(0.0f32);
        let (_, neg) = canonical_of(-0.0f32);
        assert_eq!(pos, neg);
        let (_, pos) = canonical_of(0.0f64);
        let (_, neg) = canonical_of(-0.0f64);
        assert_eq!(pos, neg);

        let (_, a) = canonical_of(f32::NAN);
        let (_, b) = canonical_of(f32::from_bits(0xffc0_0001));
        assert_eq!(a, b);
        let (_, a) = canonical_of(f64::NAN);
        let (_, b) = canonical_of(-f64::NAN);
        assert_eq!(a, b);

        // 1.0f32 and 1.0f64 are different types and stay different
        assert_ne!(canonical_of(1.0f32), canonical_of(1.0f64));
        assert_canonical_eq(1.5f32, Variant::Float(1.5));
        assert_canonical_eq(1.5f64, Variant::Double(1.5));
    }

    #[test]
    fn strings_use_short_encoding_when_they_fit() {
        let short = "hello";
        let (_, value) = Variant::String(short).canonicalize().unwrap();
        let (_, expected) = encode(short);
        assert_eq!(value, expected);
        assert!(matches!(
            Variant::try_new(&[1, 0, 0], &value).unwrap(),
            Variant::ShortString(_)
        ));

        let long = "x".repeat(64);
        assert_canonical_eq(long.as_str(), Variant::String(long.as_str()));
    }

    #[test]
    fn copies_single_encoding_types_unchanged() {
        for value in [
            Variant::Null,
            Variant::BooleanTrue,
            Variant::BooleanFalse,
            Variant::Date(NaiveDate::from_ymd_opt(2026, 11, 17).unwrap()),
            Variant::Time(NaiveTime::from_hms_micro_opt(1, 2, 3, 4).unwrap()),
            Variant::Binary(b"\x00\x01\x02"),
            Variant::Uuid(Uuid::from_u128(0x1234_5678_9abc_def0_1234_5678_9abc_def0)),
        ] {
            let (metadata, encoded) = encode(value.clone());
            let canonical = Variant::try_new(&metadata, &encoded)
                .unwrap()
                .canonicalize()
                .unwrap();
            assert_eq!(canonical, (metadata, encoded));
        }
    }

    #[test]
    fn timestamps_use_micros_unless_sub_microsecond() {
        let whole_micros = DateTime::<Utc>::from_timestamp(1_700_000_000, 123_000).unwrap();
        let sub_micro = DateTime::<Utc>::from_timestamp(1_700_000_000, 123_456).unwrap();

        // The same instant stored as nanos canonicalizes to the micros encoding
        assert_canonical_eq(
            Variant::TimestampNanos(whole_micros),
            Variant::TimestampMicros(whole_micros),
        );
        assert_eq!(
            canonical_of(Variant::TimestampNanos(whole_micros)),
            canonical_of(Variant::TimestampMicros(whole_micros))
        );
        // Sub-microsecond digits keep the nanos encoding
        assert_canonical_eq(
            Variant::TimestampNanos(sub_micro),
            Variant::TimestampNanos(sub_micro),
        );

        let ntz = whole_micros.naive_utc();
        assert_canonical_eq(
            Variant::TimestampNtzNanos(ntz),
            Variant::TimestampNtzMicros(ntz),
        );
        let ntz_sub = sub_micro.naive_utc();
        assert_canonical_eq(
            Variant::TimestampNtzNanos(ntz_sub),
            Variant::TimestampNtzNanos(ntz_sub),
        );
    }

    #[test]
    fn dictionary_holds_only_used_names_sorted() {
        // Dictionary seeded with an unused name and out of order
        let mut builder = VariantBuilder::new().with_field_names(["zeta", "unused", "alpha"]);
        builder
            .new_object()
            .with_field("zeta", 1i8)
            .with_field("alpha", 2i8)
            .finish();
        let (metadata, value) = builder.finish();
        let variant = Variant::try_new(&metadata, &value).unwrap();

        let (c_metadata, c_value) = variant.canonicalize().unwrap();
        let c_meta = VariantMetadata::try_new(&c_metadata).unwrap();
        assert_eq!(c_meta.iter().collect::<Vec<_>>(), ["alpha", "zeta"]);
        assert!(c_meta.is_sorted());

        let canonical = Variant::try_new(&c_metadata, &c_value).unwrap();
        assert_eq!(canonical, variant);
    }

    #[test]
    fn nested_names_are_collected_and_sorted_by_bytes() {
        // "B" (0x42) sorts before "a" (0x61) by bytes; "é" (0xC3 0xA9) sorts after both
        let mut builder = VariantBuilder::new().with_field_names(["é", "a", "B"]);
        {
            let mut object = builder.new_object();
            object.insert("a", 1i8);
            let mut list = object.new_list("é");
            {
                let mut inner = list.new_object();
                inner.insert("B", 2i8);
                inner.finish();
            }
            list.finish();
            object.finish();
        }
        let (metadata, value) = builder.finish();
        let variant = Variant::try_new(&metadata, &value).unwrap();

        let (c_metadata, c_value) = variant.canonicalize().unwrap();
        let c_meta = VariantMetadata::try_new(&c_metadata).unwrap();
        assert_eq!(c_meta.iter().collect::<Vec<_>>(), ["B", "a", "é"]);
        assert_eq!(Variant::try_new(&c_metadata, &c_value).unwrap(), variant);
    }

    #[test]
    fn equal_values_from_different_dictionaries_share_bytes() {
        // The datafusion-variant#183 case: "api" extracted from rows with different fields
        let mut a = VariantBuilder::new().with_field_names(["service", "status", "tags"]);
        a.append_value("api");
        let (am, av) = a.finish();
        let mut b = VariantBuilder::new().with_field_names(["error", "service"]);
        b.append_value("api");
        let (bm, bv) = b.finish();
        assert_ne!(am, bm);

        let ca = Variant::try_new(&am, &av).unwrap().canonicalize().unwrap();
        let cb = Variant::try_new(&bm, &bv).unwrap().canonicalize().unwrap();
        assert_eq!(ca, cb);
        // and the dictionary is empty
        assert_eq!(VariantMetadata::try_new(&ca.0).unwrap().len(), 0);
    }

    #[test]
    fn objects_with_same_fields_in_different_order_share_bytes() {
        let mut a = VariantBuilder::new();
        a.new_object()
            .with_field("b", 2i64)
            .with_field("a", 1i16)
            .finish();
        let (am, av) = a.finish();
        let mut b = VariantBuilder::new();
        b.new_object()
            .with_field("a", 1i8)
            .with_field("b", 2i8)
            .finish();
        let (bm, bv) = b.finish();

        let ca = Variant::try_new(&am, &av).unwrap().canonicalize().unwrap();
        let cb = Variant::try_new(&bm, &bv).unwrap().canonicalize().unwrap();
        assert_eq!(ca, cb);
    }

    #[test]
    fn null_object_fields_are_kept() {
        let mut builder = VariantBuilder::new();
        builder
            .new_object()
            .with_field("error", Variant::Null)
            .with_field("service", "api")
            .finish();
        let (metadata, value) = builder.finish();
        let variant = Variant::try_new(&metadata, &value).unwrap();

        let (c_metadata, c_value) = variant.canonicalize().unwrap();
        let canonical = Variant::try_new(&c_metadata, &c_value).unwrap();
        let object = canonical.as_object().unwrap();
        assert_eq!(object.len(), 2);
        assert_eq!(object.get("error"), Some(Variant::Null));
    }

    #[test]
    fn list_order_is_preserved() {
        let mut builder = VariantBuilder::new();
        builder
            .new_list()
            .with_value(3i64)
            .with_value(1i64)
            .with_value(2i64)
            .finish();
        let (metadata, value) = builder.finish();

        let (c_metadata, c_value) = Variant::try_new(&metadata, &value)
            .unwrap()
            .canonicalize()
            .unwrap();
        let list = Variant::try_new(&c_metadata, &c_value).unwrap();
        let items: Vec<_> = list.as_list().unwrap().iter().collect();
        assert_eq!(
            items,
            [Variant::Int8(3), Variant::Int8(1), Variant::Int8(2)]
        );
    }

    #[test]
    fn canonicalize_is_idempotent() {
        let mut builder = VariantBuilder::new().with_field_names(["unused", "z", "a"]);
        {
            let mut object = builder.new_object();
            object.insert("z", VariantDecimal8::try_new(1_500, 2).unwrap());
            object.insert("a", -0.0f64);
            let mut list = object.new_list("list");
            list.append_value(i64::from(i32::MAX) + 1);
            list.append_value("s");
            list.finish();
            object.finish();
        }
        let (metadata, value) = builder.finish();

        let once = Variant::try_new(&metadata, &value)
            .unwrap()
            .canonicalize()
            .unwrap();
        let twice = Variant::try_new(&once.0, &once.1)
            .unwrap()
            .canonicalize()
            .unwrap();
        assert_eq!(once, twice);
    }

    #[test]
    fn malformed_objects_are_rejected_by_validation() {
        // Hand-built object {"a": 1, "a": 2}: both fields point at dictionary id 0. Full
        // validation rejects the repeated name before any rebuilding happens.
        let metadata = [
            0x01, // header: version 1, unsorted, offset_size 1
            1,    // one dictionary entry
            0, 1, // offsets
            b'a',
        ];
        let value = [
            0x02, // header: basic_type=object, 1-byte ids and offsets, small
            2,    // two fields
            0, 0, // field ids: both "a"
            0, 2, 4, // field offsets
            0x0C, 1, // int8 1
            0x0C, 2, // int8 2
        ];
        let variant = Variant::new(&metadata, &value);
        let err = variant.canonicalize().unwrap_err();
        assert!(
            err.to_string().contains("field names not sorted"),
            "unexpected error: {err}"
        );
    }
}
