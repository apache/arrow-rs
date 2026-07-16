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

//! Property tests for variant decoding and validation.
//!
//! Two families of invariant are checked here:
//!
//! 1. **Untrusted input.** [`Variant::try_new`] and [`VariantMetadata::try_new`] are fallible APIs
//!    over bytes read from a file. They must return `Err` on malformed input rather than panic, and
//!    anything they accept must then be panic-free to access, as [`VariantMetadata`] documents.
//! 2. **Writer/reader agreement.** Anything [`VariantBuilder`] emits must validate and read back
//!    unchanged, whatever order the dictionary happens to be in.
//!
//! The generators deliberately produce degenerate shapes: empty field names, multi-byte UTF-8, and
//! interior NULs. This matters more than the properties themselves -- narrowing `arb_key` to
//! `[a-z]+` makes these tests pass against code with known bugs.
//!
//! Objects are generated structurally rather than as random bytes: random bytes are rejected by
//! header and offset validation long before reaching the object logic, so they never exercise it.

use parquet_variant::{Variant, VariantBuilder, VariantMetadata};
use proptest::prelude::*;
use std::collections::HashSet;

/// The empty metadata dictionary.
const EMPTY_METADATA: &[u8] = &[1, 0, 0];

/// Field names covering the shapes that distinguish correct validation from incorrect: an empty
/// name (encoded as two equal dictionary offsets), and multi-byte characters (whose interior bytes
/// are not valid split points).
fn arb_key() -> impl Strategy<Value = String> {
    prop_oneof![
        Just(String::new()),
        "[a-c]{1,3}",
        Just("é".to_string()),
        Just("€".to_string()),
        Just("𝄞".to_string()),
        Just("a\u{0}b".to_string()),
    ]
}

/// Distinct field names, in generated order. `Vec::dedup` only drops *consecutive* duplicates.
fn arb_field_names(len: std::ops::Range<usize>) -> impl Strategy<Value = Vec<String>> {
    prop::collection::vec(arb_key(), len).prop_map(|mut names| {
        let mut seen = HashSet::new();
        names.retain(|n| seen.insert(n.clone()));
        names
    })
}

/// Builds an object from `names`, seeding the dictionary in `dictionary_order` so that both the
/// sorted and unsorted validation paths can be reached for the same logical value.
fn build_object(names: &[String], dictionary_order: &[&str]) -> (Vec<u8>, Vec<u8>) {
    let mut builder = VariantBuilder::new().with_field_names(dictionary_order.iter().copied());
    let mut obj = builder.new_object();
    for (i, name) in names.iter().enumerate() {
        obj.insert(name.as_str(), i as i32);
    }
    obj.finish();
    builder.finish()
}

/// Forces every infallible accessor that a validated instance promises is panic-free.
fn traverse(variant: &Variant) {
    match variant {
        Variant::Object(o) => o.iter().for_each(|(_, child)| traverse(&child)),
        Variant::List(l) => l.iter().for_each(|child| traverse(&child)),
        other => {
            let _ = format!("{other:?}");
        }
    }
}

proptest! {
    // The byte-level properties below explore a large space in which the interesting shapes are
    // rare, so they need far more than proptest's default 256 cases: at the default, neither the
    // Date overflow nor the UTF-8 boundary bug this file was written to catch is found.
    #![proptest_config(ProptestConfig::with_cases(20_000))]

    /// Whatever the builder writes, validation must accept.
    #[test]
    fn builder_output_validates(names in arb_field_names(0..6)) {
        let order: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
        let (metadata, value) = build_object(&names, &order);
        prop_assert!(
            VariantMetadata::try_new(&metadata).is_ok(),
            "builder emitted metadata that fails validation: {metadata:?}"
        );
        prop_assert!(Variant::try_new(&metadata, &value).is_ok());
    }

    /// Dictionary ordering is an encoding detail. The same logical object must validate, and read
    /// back, identically whether its dictionary happens to be sorted or not.
    #[test]
    fn dictionary_order_does_not_change_behavior(names in arb_field_names(2..6)) {
        prop_assume!(names.len() > 1);

        let mut ascending: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
        ascending.sort_unstable();
        let descending: Vec<&str> = ascending.iter().rev().copied().collect();

        let (m1, v1) = build_object(&names, &ascending);
        let (m2, v2) = build_object(&names, &descending);

        let sorted = Variant::try_new(&m1, &v1);
        let unsorted = Variant::try_new(&m2, &v2);
        prop_assert_eq!(
            sorted.is_ok(),
            unsorted.is_ok(),
            "dictionary order changed validity for {:?}: {:?}",
            names,
            unsorted.err()
        );

        if let (Ok(a), Ok(b)) = (sorted, unsorted) {
            let (a, b) = (a.as_object().unwrap(), b.as_object().unwrap());
            for name in &names {
                prop_assert_eq!(
                    format!("{:?}", a.get(name.as_str())),
                    format!("{:?}", b.get(name.as_str()))
                );
            }
        }
    }

    /// Every inserted field reads back with the value it was given.
    #[test]
    fn object_fields_round_trip(names in arb_field_names(0..6)) {
        let order: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
        let (metadata, value) = build_object(&names, &order);
        let variant = Variant::try_new(&metadata, &value).unwrap();
        let obj = variant.as_object().unwrap();
        prop_assert_eq!(obj.len(), names.len());
        for (i, name) in names.iter().enumerate() {
            prop_assert_eq!(
                format!("{:?}", obj.get(name.as_str())),
                format!("{:?}", Some(Variant::from(i as i32)))
            );
        }
    }

    /// Every primitive type id, against arbitrary payloads of arbitrary length.
    #[test]
    fn primitive_decoding_never_panics(
        type_id in 0u8..=20,
        payload in prop::collection::vec(any::<u8>(), 0..24),
    ) {
        let mut value = vec![type_id << 2];
        value.extend_from_slice(&payload);
        let _ = Variant::try_new(EMPTY_METADATA, &value).as_ref().map(traverse);
    }

    /// Arbitrary header byte and payload, covering short strings, objects and lists.
    #[test]
    fn arbitrary_value_never_panics(value in prop::collection::vec(any::<u8>(), 1..32)) {
        let _ = Variant::try_new(EMPTY_METADATA, &value).as_ref().map(traverse);
    }

    /// Arbitrary metadata paired with arbitrary value, as both arrive from a file.
    #[test]
    fn arbitrary_metadata_and_value_never_panic(
        metadata in prop::collection::vec(any::<u8>(), 1..24),
        value in prop::collection::vec(any::<u8>(), 1..24),
    ) {
        let _ = Variant::try_new(&metadata, &value).as_ref().map(traverse);
    }

    /// Validated metadata must be panic-free to index and iterate, and `get` must not fail.
    #[test]
    fn validated_metadata_is_accessible(metadata in arb_metadata_bytes()) {
        if let Ok(md) = VariantMetadata::try_new(&metadata) {
            for i in 0..md.len() {
                prop_assert!(md.get(i).is_ok(), "validated, but get({i}) failed");
                let _ = &md[i];
            }
            let _: Vec<_> = md.iter().collect();
        }
    }

    /// A well-formed object, then one byte perturbed. Reaches near-miss states that random bytes
    /// do not.
    #[test]
    fn perturbed_object_never_panics(
        names in arb_field_names(1..5),
        index in any::<prop::sample::Index>(),
        byte in any::<u8>(),
    ) {
        let order: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
        let (metadata, mut value) = build_object(&names, &order);
        let i = index.index(value.len());
        value[i] = byte;
        let _ = Variant::try_new(&metadata, &value).as_ref().map(traverse);
    }
}

/// Metadata buffers shaped like the real thing -- a plausible header, dictionary size, offsets and
/// values -- since fully random bytes are rejected by the header check and never reach the offset
/// and UTF-8 validation this is meant to exercise.
fn arb_metadata_bytes() -> impl Strategy<Value = Vec<u8>> {
    (
        prop::bool::ANY,
        0usize..6,
        prop::collection::vec(0u8..8, 0..8),
        prop::collection::vec(
            prop_oneof![
                Just(b"a".to_vec()),
                Just("é".as_bytes().to_vec()),
                Just("€".as_bytes().to_vec()),
                Just(Vec::new()),
            ],
            0..5,
        ),
    )
        .prop_map(|(sorted, size, offsets, values)| {
            let mut bytes = vec![0x01 | u8::from(sorted) << 4];
            bytes.push(size as u8);
            let mut offsets = offsets;
            offsets.resize(size + 1, 0);
            bytes.extend_from_slice(&offsets);
            values.iter().for_each(|v| bytes.extend_from_slice(v));
            bytes
        })
}
