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

//! Integration tests for custom decoder functionality
//!
//! This test suite demonstrates various patterns for customizing JSON decoding:
//! 1. Type-based routing - customize all fields of a given type
//! 2. Annotation-based routing - customize specific fields marked with metadata
//! 3. Type-specific behavior - custom parsing logic for specific types
//! 4. Composition - combining multiple custom factories
//! 5. Delegation with interleaving - wrapping standard decoders when direct access not possible
//! 6. Path-based routing - customize specific paths in the schema tree

use arrow_array::Array as _;
use arrow_array::builder::StringBuilder;
use arrow_array::cast::AsArray;
use arrow_data::ArrayData;
use arrow_json::reader::{ArrayDecoder, DecoderFactory};
use arrow_json::{ReaderBuilder, StructMode, Tape, TapeElement};
use arrow_schema::{ArrowError, DataType, Field, FieldRef, Fields, Schema};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::io::Cursor;
use std::sync::Arc;

// ============================================================================
// Test 1: Type-based lenient string decoder
// ============================================================================

/// A string decoder that converts type mismatches to NULL instead of erroring
struct LenientStringDecoder;

impl ArrayDecoder for LenientStringDecoder {
    fn decode(&mut self, tape: &Tape<'_>, pos: &[u32]) -> Result<ArrayData, ArrowError> {
        let mut builder = StringBuilder::new();
        for p in pos {
            match tape.get(*p) {
                TapeElement::String(idx) => builder.append_value(tape.get_string(idx)),
                _ => builder.append_null(),
            }
        }
        Ok(builder.finish().into_data())
    }
}

/// A factory that applies the LenientStringDecoder to ALL Utf8 fields (type-based routing)
#[derive(Debug)]
struct TypeBasedLenientStringFactory;

impl DecoderFactory for TypeBasedLenientStringFactory {
    fn make_custom_decoder(
        &self,
        data_type: &DataType,
        _is_nullable: bool,
        _field_metadata: &HashMap<String, String>,
        _coerce_primitive: bool,
        _strict_mode: bool,
        _struct_mode: StructMode,
        _factory_override: Option<&dyn DecoderFactory>,
    ) -> Result<Option<Box<dyn ArrayDecoder>>, ArrowError> {
        match data_type {
            DataType::Utf8 => Ok(Some(Box::new(LenientStringDecoder))),
            _ => Ok(None),
        }
    }
}

#[test]
fn test_type_based_lenient_strings() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("email", DataType::Utf8, true),
    ]));

    // JSON with type mismatches in BOTH string fields
    let json = r#"
    {"id": 1, "name": "Alice", "email": "alice@example.com"}
    {"id": 2, "name": 42, "email": "bob@example.com"}
    {"id": 3, "name": "Charlie", "email": true}
    "#;

    let reader = ReaderBuilder::new(schema)
        .with_decoder_factory(Arc::new(TypeBasedLenientStringFactory))
        .build(Cursor::new(json.as_bytes()))
        .unwrap();

    let batch = reader.into_iter().next().unwrap().unwrap();

    let names = batch.column(1).as_string::<i32>();
    assert_eq!(names.value(0), "Alice");
    assert!(names.is_null(1)); // 42 -> NULL
    assert_eq!(names.value(2), "Charlie");

    let emails = batch.column(2).as_string::<i32>();
    assert_eq!(emails.value(0), "alice@example.com");
    assert_eq!(emails.value(1), "bob@example.com");
    assert!(emails.is_null(2)); // true -> NULL
}

// ============================================================================
// Test 2: Annotation-based lenient string decoder
// ============================================================================

/// A factory that applies LenientStringDecoder only to annotated Utf8 fields
#[derive(Debug)]
struct AnnotatedLenientStringFactory;

impl DecoderFactory for AnnotatedLenientStringFactory {
    fn make_custom_decoder(
        &self,
        data_type: &DataType,
        _is_nullable: bool,
        field_metadata: &HashMap<String, String>,
        _coerce_primitive: bool,
        _strict_mode: bool,
        _struct_mode: StructMode,
        _factory_override: Option<&dyn DecoderFactory>,
    ) -> Result<Option<Box<dyn ArrayDecoder>>, ArrowError> {
        let config = field_metadata
            .get("test:decoder:config")
            .map(|s| s.as_str());
        match data_type {
            DataType::Utf8 if config == Some("lenient") => Ok(Some(Box::new(LenientStringDecoder))),
            _ => Ok(None),
        }
    }
}

#[test]
fn test_annotation_based_lenient_strings() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        // ONLY this field is marked lenient
        Field::new("name", DataType::Utf8, true).with_metadata(HashMap::from([(
            "test:decoder:config".to_string(),
            "lenient".to_string(),
        )])),
        // This field is NOT marked, should use standard decoder
        Field::new("email", DataType::Utf8, false),
    ]));

    // JSON with type mismatches in BOTH string fields
    let json_valid = r#"
    {"id": 1, "name": "Alice", "email": "alice@example.com"}
    {"id": 2, "name": 42, "email": "bob@example.com"}
    "#;

    let reader = ReaderBuilder::new(schema.clone())
        .with_decoder_factory(Arc::new(AnnotatedLenientStringFactory))
        .build(Cursor::new(json_valid.as_bytes()))
        .unwrap();

    let batch = reader.into_iter().next().unwrap().unwrap();

    let names = batch.column(1).as_string::<i32>();
    assert_eq!(names.value(0), "Alice");
    assert!(names.is_null(1)); // 42 -> NULL (lenient behavior)

    let emails = batch.column(2).as_string::<i32>();
    assert_eq!(emails.value(0), "alice@example.com");
    assert_eq!(emails.value(1), "bob@example.com");

    // Negative test: email field without annotation should error on type mismatch
    let json_invalid = r#"
    {"id": 1, "name": "Alice", "email": true}
    "#;

    let reader = ReaderBuilder::new(schema)
        .with_decoder_factory(Arc::new(AnnotatedLenientStringFactory))
        .build(Cursor::new(json_invalid.as_bytes()))
        .unwrap();

    let result = reader.into_iter().next().unwrap();
    assert!(result.is_err()); // email field errors on type mismatch
}

// ============================================================================
// Shared helpers for interleaved decoding pattern
// ============================================================================

/// A general-purpose interleaved decoder that routes positions to different decoders
/// based on a filter predicate, then interleaves the results back to original order.
///
/// This pattern is useful when you want to customize behavior but need to delegate
/// to standard decoders (which you can't directly access the builders of).
///
/// Note: This example uses `Fn(TapeElement) -> bool` for simplicity. A production
/// implementation might use `Fn(&Tape, u32) -> bool` to support filters that need
/// to examine list/object contents before routing (e.g., routing based on list length
/// or presence of discriminator fields). Downside is more complexity in simple cases.
struct InterleavedDecoder<F> {
    primary: Box<dyn ArrayDecoder>,
    fallback: Box<dyn ArrayDecoder>,
    filter: F,
}

impl<F: Fn(TapeElement) -> bool + Send> ArrayDecoder for InterleavedDecoder<F> {
    fn decode(&mut self, tape: &Tape<'_>, pos: &[u32]) -> Result<ArrayData, ArrowError> {
        use arrow_select::interleave::interleave;

        // Partition positions based on filter
        let mut primary_pos = Vec::new();
        let mut fallback_pos = Vec::new();
        let mut indices = Vec::with_capacity(pos.len());

        for &p in pos {
            if (self.filter)(tape.get(p)) {
                indices.push((0, primary_pos.len()));
                primary_pos.push(p);
            } else {
                indices.push((1, fallback_pos.len()));
                fallback_pos.push(p);
            }
        }

        // Decode both parts
        let primary = self.primary.decode(tape, &primary_pos)?;
        let fallback = self.fallback.decode(tape, &fallback_pos)?;

        // Convert to arrays for interleaving
        let primary = arrow_array::make_array(primary);
        let fallback = arrow_array::make_array(fallback);

        // Interleave back to original order
        let result = interleave(&[primary.as_ref(), fallback.as_ref()], &indices)?;
        Ok(result.into_data())
    }
}

/// A decoder that always produces NULL values (useful as fallback in InterleavedDecoder)
struct NullDecoder {
    data_type: DataType,
}

impl ArrayDecoder for NullDecoder {
    fn decode(&mut self, _tape: &Tape<'_>, pos: &[u32]) -> Result<ArrayData, ArrowError> {
        Ok(arrow_array::new_null_array(&self.data_type, pos.len()).into_data())
    }
}

// ============================================================================
// Test 3: Lenient struct decoder (introduces InterleavedDecoder pattern)
// ============================================================================

/// A factory that makes ALL Struct fields lenient (type-based routing)
///
/// This demonstrates the InterleavedDecoder pattern: we want to customize struct
/// handling but can't directly access the StructArrayDecoder's internal builder
/// and -- unlike string decoding -- the logic is too complex to replicate easily.
///
/// Solution: partition positions and delegate to standard decoder + null decoder.
#[derive(Debug)]
struct TypeBasedLenientStructFactory;

impl DecoderFactory for TypeBasedLenientStructFactory {
    fn make_custom_decoder(
        &self,
        data_type: &DataType,
        is_nullable: bool,
        _field_metadata: &HashMap<String, String>,
        coerce_primitive: bool,
        strict_mode: bool,
        struct_mode: StructMode,
        factory_override: Option<&dyn DecoderFactory>,
    ) -> Result<Option<Box<dyn ArrayDecoder>>, ArrowError> {
        if !matches!(data_type, DataType::Struct(_)) {
            return Ok(None);
        }

        // Create standard struct decoder (using make_delegate_decoder to avoid infinite recursion)
        let primary = Self::make_delegate_decoder(
            factory_override.unwrap_or(self),
            data_type,
            is_nullable,
            coerce_primitive,
            strict_mode,
            struct_mode,
        )?;

        // Create null decoder as fallback
        let fallback = Box::new(NullDecoder {
            data_type: data_type.clone(),
        });

        Ok(Some(Box::new(InterleavedDecoder {
            primary,
            fallback,
            filter: |elem| matches!(elem, TapeElement::StartObject(_)),
        })))
    }
}

#[test]
fn test_type_based_lenient_structs() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new(
            "person",
            DataType::Struct(vec![Field::new("name", DataType::Utf8, false)].into()),
            true,
        ),
    ]));

    let json = r#"
    {"id": 1, "person": {"name": "Alice"}}
    {"id": 2, "person": "not a struct"}
    {"id": 3, "person": 42}
    {"id": 4, "person": null}
    "#;

    let reader = ReaderBuilder::new(schema)
        .with_decoder_factory(Arc::new(TypeBasedLenientStructFactory))
        .build(Cursor::new(json.as_bytes()))
        .unwrap();

    let batch = reader.into_iter().next().unwrap().unwrap();
    let person = batch.column(1).as_struct();
    assert!(!person.is_null(0)); // Valid struct
    assert!(person.is_null(1)); // "not a struct" -> NULL
    assert!(person.is_null(2)); // 42 -> NULL
    assert!(person.is_null(3)); // null -> NULL

    // Verify first row decoded correctly
    let names = person.column(0).as_string::<i32>();
    assert_eq!(names.value(0), "Alice");
}

// ============================================================================
// Test 4: Quirky string list decoder (reinforces InterleavedDecoder pattern)
// ============================================================================

/// Parse a string like "[a, b, c]" into an iterator of strings, e.g. "a", "b", "c"
fn parse_list_string(s: &str) -> Result<impl Iterator<Item = &str> + '_, ArrowError> {
    let trimmed = s.trim();
    if !trimmed.starts_with('[') || !trimmed.ends_with(']') {
        return Err(ArrowError::JsonError(format!(
            "Failed to parse list string: {}",
            s
        )));
    }
    let inner = &trimmed[1..trimmed.len() - 1];
    Ok(inner.split(',').map(|s| s.trim()))
}

/// A decoder that ONLY handles string representations like "[a, b, c]" and parses them as lists.
/// Designed to be used with InterleavedDecoder (standard list decoder handles normal lists).
struct StringToListDecoder {
    field: FieldRef,
}

impl ArrayDecoder for StringToListDecoder {
    fn decode(&mut self, tape: &Tape<'_>, pos: &[u32]) -> Result<ArrayData, ArrowError> {
        // Use with_field() to ensure the builder respects the schema's field (including nullability)
        let mut builder = arrow_array::builder::ListBuilder::new(StringBuilder::new())
            .with_field(self.field.clone());

        for p in pos {
            let TapeElement::String(s) = tape.get(*p) else {
                unreachable!("InterleavedDecoder filter should only route String elements here");
            };

            // Parse string representation like "[x, y, z]"
            for item in parse_list_string(tape.get_string(s))? {
                builder.values().append_value(item);
            }
            builder.append(true);
        }

        Ok(builder.finish().into_data())
    }
}

/// A factory that makes ALL List<Utf8> fields quirky (type-based routing)
///
/// Uses InterleavedDecoder to combine string parsing with standard list decoding.
#[derive(Debug)]
struct TypeBasedQuirkyListFactory;

impl DecoderFactory for TypeBasedQuirkyListFactory {
    fn make_custom_decoder(
        &self,
        data_type: &DataType,
        is_nullable: bool,
        _field_metadata: &HashMap<String, String>,
        coerce_primitive: bool,
        strict_mode: bool,
        struct_mode: StructMode,
        factory_override: Option<&dyn DecoderFactory>,
    ) -> Result<Option<Box<dyn ArrayDecoder>>, ArrowError> {
        let field = match data_type {
            DataType::List(f) if *f.data_type() == DataType::Utf8 => f.clone(),
            _ => return Ok(None),
        };

        // Primary: parse string representations
        // Fallback: standard list decoder for normal JSON lists
        Ok(Some(Box::new(InterleavedDecoder {
            primary: Box::new(StringToListDecoder { field }),
            fallback: Self::make_delegate_decoder(
                factory_override.unwrap_or(self),
                data_type,
                is_nullable,
                coerce_primitive,
                strict_mode,
                struct_mode,
            )?,
            filter: |elem| matches!(elem, TapeElement::String(_)),
        })))
    }
}

#[test]
fn test_type_based_quirky_lists() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
            true,
        ),
    ]));

    let json = r#"
{"id": 1, "tags": ["a", "b", "c"]}
{"id": 2, "tags": "[x, y, z]"}
{"id": 3, "tags": null}
"#;

    let reader = ReaderBuilder::new(schema)
        .with_decoder_factory(Arc::new(TypeBasedQuirkyListFactory))
        .build(Cursor::new(json.as_bytes()))
        .unwrap();

    let batch = reader.into_iter().next().unwrap().unwrap();

    let tags = batch.column(1).as_list::<i32>();

    // First row: normal JSON list
    let row0 = tags.value(0);
    let row0 = row0.as_string::<i32>();
    assert_eq!(row0.len(), 3);
    assert_eq!(row0.value(0), "a");
    assert_eq!(row0.value(1), "b");
    assert_eq!(row0.value(2), "c");

    // Second row: parsed from string representation
    let row1 = tags.value(1);
    let row1 = row1.as_string::<i32>();
    assert_eq!(row1.len(), 3);
    assert_eq!(row1.value(0), "x");
    assert_eq!(row1.value(1), "y");
    assert_eq!(row1.value(2), "z");

    // Third row: null
    assert!(tags.is_null(2));
}

// ============================================================================
// Test 5: Composition - combining multiple type-based factories
// ============================================================================

/// A factory that tries multiple child factories in sequence
#[derive(Debug)]
struct ComposedDecoderFactory {
    factories: Vec<Arc<dyn DecoderFactory>>,
}

impl DecoderFactory for ComposedDecoderFactory {
    fn make_custom_decoder(
        &self,
        data_type: &DataType,
        is_nullable: bool,
        field_metadata: &HashMap<String, String>,
        coerce_primitive: bool,
        strict_mode: bool,
        struct_mode: StructMode,
        factory_override: Option<&dyn DecoderFactory>,
    ) -> Result<Option<Box<dyn ArrayDecoder>>, ArrowError> {
        // Try each child factory in order until one returns Some or Err
        for factory in &self.factories {
            if let Some(decoder) = factory.make_custom_decoder(
                data_type,
                is_nullable,
                field_metadata,
                coerce_primitive,
                strict_mode,
                struct_mode,
                Some(factory_override.unwrap_or(self)),
            )? {
                return Ok(Some(decoder));
            }
        }
        Ok(None)
    }
}

#[test]
fn test_composed_type_based_factories() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
            true,
        ),
    ]));

    let json = r#"
{"id": 1, "name": "Alice", "tags": ["a", "b"]}
{"id": 2, "name": 42, "tags": "[x, y]"}
{"id": 3, "name": "Bob", "tags": null}
"#;

    // Compose two type-based factories
    let factories = vec![
        Arc::new(TypeBasedLenientStringFactory) as _,
        Arc::new(TypeBasedQuirkyListFactory) as _,
    ];
    let factory = Arc::new(ComposedDecoderFactory { factories });

    let reader = ReaderBuilder::new(schema)
        .with_decoder_factory(factory)
        .build(Cursor::new(json.as_bytes()))
        .unwrap();

    let batch = reader.into_iter().next().unwrap().unwrap();

    // Verify lenient string behavior
    let names = batch.column(1).as_string::<i32>();
    assert_eq!(names.value(0), "Alice");
    assert!(names.is_null(1)); // 42 -> NULL
    assert_eq!(names.value(2), "Bob");

    // Verify quirky list behavior
    let tags = batch.column(2).as_list::<i32>();

    // Row 0: normal JSON list
    let row0 = tags.value(0);
    let row0 = row0.as_string::<i32>();
    assert_eq!(row0.len(), 2);
    assert_eq!(row0.value(0), "a");
    assert_eq!(row0.value(1), "b");

    // Row 1: parsed from string representation
    let row1 = tags.value(1);
    let row1 = row1.as_string::<i32>();
    assert_eq!(row1.len(), 2);
    assert_eq!(row1.value(0), "x");
    assert_eq!(row1.value(1), "y");

    // Row 2: null
    assert!(tags.is_null(2));
}

// ============================================================================
// Test 6: Path-based routing (pointer-identity-based)
// ============================================================================

/// Identity wrapper for DataType that uses pointer equality for HashMap lookup.
///
/// Supports two modes:
/// - `FieldRef` variant: Stores ownership of a Field (for HashMap storage)
/// - `DataType` variant: Borrows a DataType temporarily (for HashMap lookup)
///
/// Both variants compare by pointer identity of the DataType they reference.
///
/// Safety: Pointer-identity comparison relies on DataType stability guarantees:
/// - We store the owning FieldRef (Arc<Field>) which keeps the Field alive
/// - We never call any potentially-mutating methods such as `Arc::get_mut` or `Arc::make_mut`
/// - We never share a reference to the FieldRef that could allow others to mutate it
/// - Our FieldRef ensures that anyone else who might attempt potentially-mutating operations
///   of the same Field through their own FieldRef will fail because `!Arc::is_unique()`
/// - The &DataType returned by `Field::data_type` is stable -- no interior mutability
///   such as `Mutex<Box<DataType>>` that could move it to a new memory location
///
/// NOTE: We never dereference the raw pointer values used for comparison. A violation
/// of the above would only produce incorrect HashMap lookups (false positives/negatives).
#[derive(Debug)]
enum DataTypeIdentity<'a> {
    FieldRef(FieldRef),
    DataType(&'a DataType),
}

impl<'a> DataTypeIdentity<'a> {
    /// Extract the raw DataType pointer for identity comparison.
    fn as_ptr(&self) -> *const DataType {
        match self {
            DataTypeIdentity::FieldRef(f) => f.data_type(),
            DataTypeIdentity::DataType(dt) => *dt,
        }
    }
}

impl<'a> Hash for DataTypeIdentity<'a> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_ptr().hash(state);
    }
}

impl<'a> PartialEq for DataTypeIdentity<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.as_ptr() == other.as_ptr()
    }
}

impl<'a> Eq for DataTypeIdentity<'a> {}

/// A factory that routes to custom decoders based on specific field paths.
///
/// This allows fine-grained control: customize specific fields by name without
/// polluting the schema with metadata or affecting all fields of a given type.
#[derive(Debug)]
struct PathBasedDecoderFactory {
    // Maps DataTypeIdentity::FieldRef to factory
    routes: HashMap<DataTypeIdentity<'static>, Arc<dyn DecoderFactory>>,
}

impl PathBasedDecoderFactory {
    /// Create a new path-based factory by mapping field paths to factories.
    ///
    /// Takes a reference to the schema fields and a map of paths to factories.
    /// Paths can be nested using dot notation: "field", "struct.nested", "struct.deep.nested"
    fn new(fields: &Fields, path_routes: HashMap<&str, Arc<dyn DecoderFactory>>) -> Self {
        // Walk the fields and associate DataTypeIdentity::FieldRef with factory for O(1) lookup
        let mut routes = HashMap::new();
        for (path, factory) in path_routes {
            let parts: Vec<&str> = path.split('.').collect();
            if let Some(field) = Self::find_field_by_path(fields, &parts) {
                routes.insert(DataTypeIdentity::FieldRef(field), factory);
            }
        }

        Self { routes }
    }

    /// Recursively find a Field by following a path of field names.
    fn find_field_by_path(fields: &Fields, path: &[&str]) -> Option<FieldRef> {
        let (first, rest) = path.split_first()?;
        let field = fields.iter().find(|f| f.name() == *first)?;

        if rest.is_empty() {
            // End of path - return this field
            return Some(field.clone());
        }

        // Path continues - attempt to recurse into a nested struct
        let DataType::Struct(children) = field.data_type() else {
            return None;
        };

        Self::find_field_by_path(children, rest)
    }
}

impl DecoderFactory for PathBasedDecoderFactory {
    fn make_custom_decoder(
        &self,
        data_type: &DataType,
        is_nullable: bool,
        field_metadata: &HashMap<String, String>,
        coerce_primitive: bool,
        strict_mode: bool,
        struct_mode: StructMode,
        factory_override: Option<&dyn DecoderFactory>,
    ) -> Result<Option<Box<dyn ArrayDecoder>>, ArrowError> {
        // O(1) lookup using temporary DataType variant for identity comparison
        let key = DataTypeIdentity::DataType(data_type);
        let Some(factory) = self.routes.get(&key) else {
            return Ok(None);
        };

        // Delegate to the route-specific factory
        factory.make_custom_decoder(
            data_type,
            is_nullable,
            field_metadata,
            coerce_primitive,
            strict_mode,
            struct_mode,
            Some(factory_override.unwrap_or(self)),
        )
    }
}

#[test]
fn test_path_based_routing() {
    // Create schema with both flat and nested String fields
    let metadata_fields = Fields::from(vec![
        Field::new("source", DataType::Utf8, true),
        Field::new("comment", DataType::Utf8, true), // Nested path: metadata.comment
    ]);

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("email", DataType::Utf8, true),
        Field::new("metadata", DataType::Struct(metadata_fields.clone()), true),
    ]));

    // Create a lenient string factory (will be applied to specific paths only)
    let lenient_factory = Arc::new(TypeBasedLenientStringFactory);

    // Build routes: ONLY the nested "metadata.comment" field gets lenient handling
    // Demonstrates nested path routing with dot notation
    let mut path_routes = HashMap::new();
    path_routes.insert(
        "metadata.comment",
        lenient_factory.clone() as Arc<dyn DecoderFactory>,
    );

    let factory = Arc::new(PathBasedDecoderFactory::new(schema.fields(), path_routes));

    // JSON with type mismatches in multiple string fields
    let json = r#"
{"id": 1, "name": "Alice", "email": "alice@example.com", "metadata": {"source": "web", "comment": "Good"}}
{"id": 2, "name": 42, "email": "bob@example.com", "metadata": {"source": "api", "comment": 100}}
{"id": 3, "name": "Charlie", "email": 999, "metadata": {"source": "mobile", "comment": "Excellent"}}
"#;

    let mut reader = ReaderBuilder::new(schema.clone())
        .with_decoder_factory(factory)
        .build(Cursor::new(json))
        .unwrap();

    // The decode should FAIL because "name" and "email" don't have lenient handling
    // Only "metadata.comment" is lenient, but "name" has a type mismatch in row 2
    let result = reader.next().unwrap();
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("name") || err.contains("expected string"));

    // Now let's test with data that only has issues in the lenient nested field
    let json_lenient_only = r#"
{"id": 1, "name": "Alice", "email": "alice@example.com", "metadata": {"source": "web", "comment": "Good"}}
{"id": 2, "name": "Bob", "email": "bob@example.com", "metadata": {"source": "api", "comment": 100}}
{"id": 3, "name": "Charlie", "email": "charlie@example.com", "metadata": {"source": "mobile", "comment": true}}
"#;

    // Rebuild routes for the second test
    let mut path_routes2 = HashMap::new();
    path_routes2.insert(
        "metadata.comment",
        lenient_factory as Arc<dyn DecoderFactory>,
    );

    let mut reader = ReaderBuilder::new(schema.clone())
        .with_decoder_factory(Arc::new(PathBasedDecoderFactory::new(
            schema.fields(),
            path_routes2,
        )))
        .build(Cursor::new(json_lenient_only))
        .unwrap();

    let batch = reader.next().unwrap().unwrap();

    // Verify all fields
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(batch.num_columns(), 4);

    // ID column: all valid
    let ids = batch
        .column(0)
        .as_primitive::<arrow_array::types::Int32Type>();
    assert_eq!(ids.value(0), 1);
    assert_eq!(ids.value(1), 2);
    assert_eq!(ids.value(2), 3);

    // Name column: all valid (no type mismatches)
    let names = batch.column(1).as_string::<i32>();
    assert_eq!(names.value(0), "Alice");
    assert_eq!(names.value(1), "Bob");
    assert_eq!(names.value(2), "Charlie");

    // Email column: all valid (no type mismatches)
    let emails = batch.column(2).as_string::<i32>();
    assert_eq!(emails.value(0), "alice@example.com");
    assert_eq!(emails.value(1), "bob@example.com");
    assert_eq!(emails.value(2), "charlie@example.com");

    // Metadata struct column
    let metadata = batch.column(3).as_struct();

    // metadata.source: all valid (no lenient handling)
    let sources = metadata.column(0).as_string::<i32>();
    assert_eq!(sources.value(0), "web");
    assert_eq!(sources.value(1), "api");
    assert_eq!(sources.value(2), "mobile");

    // metadata.comment: LENIENT - non-strings become NULL
    let comments = metadata.column(1).as_string::<i32>();
    assert_eq!(comments.value(0), "Good");
    assert!(comments.is_null(1)); // 100 -> NULL
    assert!(comments.is_null(2)); // true -> NULL
}

// ============================================================================
// Test 7: Recursive factory propagation
// ============================================================================

#[test]
fn test_recursive_factory_propagation() {
    // Schema with nested struct containing string fields
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new(
            "person",
            DataType::Struct(
                vec![
                    Field::new("name", DataType::Utf8, true), // Make nullable
                    Field::new("email", DataType::Utf8, true),
                ]
                .into(),
            ),
            true,
        ),
    ]));

    let json = r#"
{"id": 1, "person": {"name": "Alice", "email": "alice@example.com"}}
{"id": 2, "person": 42}
{"id": 3, "person": {"name": 123, "email": "charlie@example.com"}}
{"id": 4, "person": {"name": "Dave", "email": true}}
"#;

    // Compose lenient struct + lenient string factories
    // This tests that the factory propagates through struct decoder creation
    let factories = vec![
        Arc::new(TypeBasedLenientStructFactory) as Arc<dyn DecoderFactory>,
        Arc::new(TypeBasedLenientStringFactory) as Arc<dyn DecoderFactory>,
    ];
    let factory = Arc::new(ComposedDecoderFactory { factories });

    let mut reader = ReaderBuilder::new(schema)
        .with_decoder_factory(factory)
        .build(Cursor::new(json))
        .unwrap();

    let batch = reader.next().unwrap().unwrap();

    // ID column: all valid
    let ids = batch
        .column(0)
        .as_primitive::<arrow_array::types::Int32Type>();
    assert_eq!(ids.value(0), 1);
    assert_eq!(ids.value(1), 2);
    assert_eq!(ids.value(2), 3);
    assert_eq!(ids.value(3), 4);

    // Person struct column
    let person = batch.column(1).as_struct();

    // Row 0: Valid struct with valid strings
    assert!(!person.is_null(0));
    let names = person.column(0).as_string::<i32>();
    let emails = person.column(1).as_string::<i32>();
    assert_eq!(names.value(0), "Alice");
    assert_eq!(emails.value(0), "alice@example.com");

    // Row 1: Not a struct (42) -> NULL from struct factory
    assert!(person.is_null(1));

    // Row 2: Valid struct but name has type mismatch (123)
    // This tests that the string factory was invoked for the nested field!
    assert!(!person.is_null(2));
    assert!(names.is_null(2)); // 123 -> NULL from string factory
    assert_eq!(emails.value(2), "charlie@example.com");

    // Row 3: Valid struct but email has type mismatch (true)
    assert!(!person.is_null(3));
    assert_eq!(names.value(3), "Dave");
    assert!(emails.is_null(3)); // true -> NULL from string factory
}
