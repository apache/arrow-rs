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

//! Contains structs and methods to build Parquet schema and schema descriptors.

use std::{collections::HashMap, convert::From, fmt, sync::Arc};

use parquet_format::SchemaElement;

use crate::basic::{
    ConvertedType, LogicalType, Repetition, TimeType, TimeUnit, Type as PhysicalType,
};
use crate::errors::{ParquetError, Result};

// ----------------------------------------------------------------------
// Parquet Type definitions

/// Type alias for `Arc<Type>`.
pub type TypePtr = Arc<Type>;
/// Type alias for `Arc<SchemaDescriptor>`.
pub type SchemaDescPtr = Arc<SchemaDescriptor>;
/// Type alias for `Arc<ColumnDescriptor>`.
pub type ColumnDescPtr = Arc<ColumnDescriptor>;

/// Representation of a Parquet type.
/// Used to describe primitive leaf fields and structs, including top-level schema.
/// Note that the top-level schema type is represented using `GroupType` whose
/// repetition is `None`.
#[derive(Clone, Debug, PartialEq)]
pub enum Type {
    PrimitiveType {
        basic_info: BasicTypeInfo,
        physical_type: PhysicalType,
        type_length: i32,
        scale: i32,
        precision: i32,
    },
    GroupType {
        basic_info: BasicTypeInfo,
        fields: Vec<TypePtr>,
    },
}

impl Type {
    /// Creates primitive type builder with provided field name and physical type.
    pub fn primitive_type_builder(
        name: &str,
        physical_type: PhysicalType,
    ) -> PrimitiveTypeBuilder {
        PrimitiveTypeBuilder::new(name, physical_type)
    }

    /// Creates group type builder with provided column name.
    pub fn group_type_builder(name: &str) -> GroupTypeBuilder {
        GroupTypeBuilder::new(name)
    }

    /// Returns [`BasicTypeInfo`] information about the type.
    pub fn get_basic_info(&self) -> &BasicTypeInfo {
        match *self {
            Type::PrimitiveType { ref basic_info, .. } => &basic_info,
            Type::GroupType { ref basic_info, .. } => &basic_info,
        }
    }

    /// Returns this type's field name.
    pub fn name(&self) -> &str {
        self.get_basic_info().name()
    }

    /// Gets the fields from this group type.
    /// Note that this will panic if called on a non-group type.
    // TODO: should we return `&[&Type]` here?
    pub fn get_fields(&self) -> &[TypePtr] {
        match *self {
            Type::GroupType { ref fields, .. } => &fields[..],
            _ => panic!("Cannot call get_fields() on a non-group type"),
        }
    }

    /// Gets physical type of this primitive type.
    /// Note that this will panic if called on a non-primitive type.
    pub fn get_physical_type(&self) -> PhysicalType {
        match *self {
            Type::PrimitiveType {
                basic_info: _,
                physical_type,
                ..
            } => physical_type,
            _ => panic!("Cannot call get_physical_type() on a non-primitive type"),
        }
    }

    /// Gets precision of this primitive type.
    /// Note that this will panic if called on a non-primitive type.
    pub fn get_precision(&self) -> i32 {
        match *self {
            Type::PrimitiveType { precision, .. } => precision,
            _ => panic!("Cannot call get_precision() on non-primitive type"),
        }
    }

    /// Gets scale of this primitive type.
    /// Note that this will panic if called on a non-primitive type.
    pub fn get_scale(&self) -> i32 {
        match *self {
            Type::PrimitiveType { scale, .. } => scale,
            _ => panic!("Cannot call get_scale() on non-primitive type"),
        }
    }

    /// Checks if `sub_type` schema is part of current schema.
    /// This method can be used to check if projected columns are part of the root schema.
    pub fn check_contains(&self, sub_type: &Type) -> bool {
        // Names match, and repetitions match or not set for both
        let basic_match = self.get_basic_info().name()
            == sub_type.get_basic_info().name()
            && (self.is_schema() && sub_type.is_schema()
                || !self.is_schema()
                    && !sub_type.is_schema()
                    && self.get_basic_info().repetition()
                        == sub_type.get_basic_info().repetition());

        match *self {
            Type::PrimitiveType { .. } if basic_match && sub_type.is_primitive() => {
                self.get_physical_type() == sub_type.get_physical_type()
            }
            Type::GroupType { .. } if basic_match && sub_type.is_group() => {
                // build hashmap of name -> TypePtr
                let mut field_map = HashMap::new();
                for field in self.get_fields() {
                    field_map.insert(field.name(), field);
                }

                for field in sub_type.get_fields() {
                    if !field_map
                        .get(field.name())
                        .map(|tpe| tpe.check_contains(field))
                        .unwrap_or(false)
                    {
                        return false;
                    }
                }
                true
            }
            _ => false,
        }
    }

    /// Returns `true` if this type is a primitive type, `false` otherwise.
    pub fn is_primitive(&self) -> bool {
        matches!(*self, Type::PrimitiveType { .. })
    }

    /// Returns `true` if this type is a group type, `false` otherwise.
    pub fn is_group(&self) -> bool {
        matches!(*self, Type::GroupType { .. })
    }

    /// Returns `true` if this type is the top-level schema type (message type).
    pub fn is_schema(&self) -> bool {
        match *self {
            Type::GroupType { ref basic_info, .. } => !basic_info.has_repetition(),
            _ => false,
        }
    }

    /// Returns `true` if this type is repeated or optional.
    /// If this type doesn't have repetition defined, we still treat it as optional.
    pub fn is_optional(&self) -> bool {
        self.get_basic_info().has_repetition()
            && self.get_basic_info().repetition() != Repetition::REQUIRED
    }
}

/// A builder for primitive types. All attributes are optional
/// except the name and physical type.
/// Note that if not specified explicitly, `Repetition::OPTIONAL` is used.
pub struct PrimitiveTypeBuilder<'a> {
    name: &'a str,
    repetition: Repetition,
    physical_type: PhysicalType,
    converted_type: ConvertedType,
    logical_type: Option<LogicalType>,
    length: i32,
    precision: i32,
    scale: i32,
    id: Option<i32>,
}

impl<'a> PrimitiveTypeBuilder<'a> {
    /// Creates new primitive type builder with provided field name and physical type.
    pub fn new(name: &'a str, physical_type: PhysicalType) -> Self {
        Self {
            name,
            repetition: Repetition::OPTIONAL,
            physical_type,
            converted_type: ConvertedType::NONE,
            logical_type: None,
            length: -1,
            precision: -1,
            scale: -1,
            id: None,
        }
    }

    /// Sets [`Repetition`](crate::basic::Repetition) for this field and returns itself.
    pub fn with_repetition(mut self, repetition: Repetition) -> Self {
        self.repetition = repetition;
        self
    }

    /// Sets [`ConvertedType`](crate::basic::ConvertedType) for this field and returns itself.
    pub fn with_converted_type(mut self, converted_type: ConvertedType) -> Self {
        self.converted_type = converted_type;
        self
    }

    /// Sets [`LogicalType`](crate::basic::LogicalType) for this field and returns itself.
    /// If only the logical type is populated for a primitive type, the converted type
    /// will be automatically populated, and can thus be omitted.
    pub fn with_logical_type(mut self, logical_type: Option<LogicalType>) -> Self {
        self.logical_type = logical_type;
        self
    }

    /// Sets type length and returns itself.
    /// This is only applied to FIXED_LEN_BYTE_ARRAY and INT96 (INTERVAL) types, because
    /// they maintain fixed size underlying byte array.
    /// By default, value is `0`.
    pub fn with_length(mut self, length: i32) -> Self {
        self.length = length;
        self
    }

    /// Sets precision for Parquet DECIMAL physical type and returns itself.
    /// By default, it equals to `0` and used only for decimal context.
    pub fn with_precision(mut self, precision: i32) -> Self {
        self.precision = precision;
        self
    }

    /// Sets scale for Parquet DECIMAL physical type and returns itself.
    /// By default, it equals to `0` and used only for decimal context.
    pub fn with_scale(mut self, scale: i32) -> Self {
        self.scale = scale;
        self
    }

    /// Sets optional field id and returns itself.
    pub fn with_id(mut self, id: i32) -> Self {
        self.id = Some(id);
        self
    }

    /// Creates a new `PrimitiveType` instance from the collected attributes.
    /// Returns `Err` in case of any building conditions are not met.
    pub fn build(self) -> Result<Type> {
        let mut basic_info = BasicTypeInfo {
            name: String::from(self.name),
            repetition: Some(self.repetition),
            converted_type: self.converted_type,
            logical_type: self.logical_type.clone(),
            id: self.id,
        };

        // Check length before logical type, since it is used for logical type validation.
        if self.physical_type == PhysicalType::FIXED_LEN_BYTE_ARRAY && self.length < 0 {
            return Err(general_err!(
                "Invalid FIXED_LEN_BYTE_ARRAY length: {}",
                self.length
            ));
        }

        match &self.logical_type {
            Some(logical_type) => {
                // If a converted type is populated, check that it is consistent with
                // its logical type
                if self.converted_type != ConvertedType::NONE {
                    if ConvertedType::from(self.logical_type.clone())
                        != self.converted_type
                    {
                        return Err(general_err!(
                            "Logical type {:?} is imcompatible with converted type {}",
                            logical_type,
                            self.converted_type
                        ));
                    }
                } else {
                    // Populate the converted type for backwards compatibility
                    basic_info.converted_type = self.logical_type.clone().into();
                }
                // Check that logical type and physical type are compatible
                match (logical_type, self.physical_type) {
                    (LogicalType::MAP(_), _) | (LogicalType::LIST(_), _) => {
                        return Err(general_err!(
                            "{:?} cannot be applied to a primitive type",
                            logical_type
                        ));
                    }
                    (LogicalType::ENUM(_), PhysicalType::BYTE_ARRAY) => {}
                    (LogicalType::DECIMAL(t), _) => {
                        // Check that scale and precision are consistent with legacy values
                        if t.scale != self.scale {
                            return Err(general_err!(
                                "DECIMAL logical type scale {} must match self.scale {}",
                                t.scale,
                                self.scale
                            ));
                        }
                        if t.precision != self.precision {
                            return Err(general_err!(
                                "DECIMAL logical type precision {} must match self.precision {}",
                                t.precision,
                                self.precision
                            ));
                        }
                        self.check_decimal_precision_scale()?;
                    }
                    (LogicalType::DATE(_), PhysicalType::INT32) => {}
                    (
                        LogicalType::TIME(TimeType {
                            unit: TimeUnit::MILLIS(_),
                            ..
                        }),
                        PhysicalType::INT32,
                    ) => {}
                    (LogicalType::TIME(t), PhysicalType::INT64) => {
                        if t.unit == TimeUnit::MILLIS(Default::default()) {
                            return Err(general_err!(
                                "Cannot use millisecond unit on INT64 type"
                            ));
                        }
                    }
                    (LogicalType::TIMESTAMP(_), PhysicalType::INT64) => {}
                    (LogicalType::INTEGER(t), PhysicalType::INT32)
                        if t.bit_width <= 32 => {}
                    (LogicalType::INTEGER(t), PhysicalType::INT64)
                        if t.bit_width == 64 => {}
                    // Null type
                    (LogicalType::UNKNOWN(_), PhysicalType::INT32) => {}
                    (LogicalType::STRING(_), PhysicalType::BYTE_ARRAY) => {}
                    (LogicalType::JSON(_), PhysicalType::BYTE_ARRAY) => {}
                    (LogicalType::BSON(_), PhysicalType::BYTE_ARRAY) => {}
                    (LogicalType::UUID(_), PhysicalType::FIXED_LEN_BYTE_ARRAY) => {}
                    (a, b) => {
                        return Err(general_err!(
                            "Cannot annotate {:?} from {} fields",
                            a,
                            b
                        ))
                    }
                }
            }
            None => {}
        }

        match self.converted_type {
            ConvertedType::NONE => {}
            ConvertedType::UTF8 | ConvertedType::BSON | ConvertedType::JSON => {
                if self.physical_type != PhysicalType::BYTE_ARRAY {
                    return Err(general_err!(
                        "{} can only annotate BYTE_ARRAY fields",
                        self.converted_type
                    ));
                }
            }
            ConvertedType::DECIMAL => {
                self.check_decimal_precision_scale()?;
            }
            ConvertedType::DATE
            | ConvertedType::TIME_MILLIS
            | ConvertedType::UINT_8
            | ConvertedType::UINT_16
            | ConvertedType::UINT_32
            | ConvertedType::INT_8
            | ConvertedType::INT_16
            | ConvertedType::INT_32 => {
                if self.physical_type != PhysicalType::INT32 {
                    return Err(general_err!(
                        "{} can only annotate INT32",
                        self.converted_type
                    ));
                }
            }
            ConvertedType::TIME_MICROS
            | ConvertedType::TIMESTAMP_MILLIS
            | ConvertedType::TIMESTAMP_MICROS
            | ConvertedType::UINT_64
            | ConvertedType::INT_64 => {
                if self.physical_type != PhysicalType::INT64 {
                    return Err(general_err!(
                        "{} can only annotate INT64",
                        self.converted_type
                    ));
                }
            }
            ConvertedType::INTERVAL => {
                if self.physical_type != PhysicalType::FIXED_LEN_BYTE_ARRAY
                    || self.length != 12
                {
                    return Err(general_err!(
                        "INTERVAL can only annotate FIXED_LEN_BYTE_ARRAY(12)"
                    ));
                }
            }
            ConvertedType::ENUM => {
                if self.physical_type != PhysicalType::BYTE_ARRAY {
                    return Err(general_err!("ENUM can only annotate BYTE_ARRAY fields"));
                }
            }
            _ => {
                return Err(general_err!(
                    "{} cannot be applied to a primitive type",
                    self.converted_type
                ));
            }
        }

        Ok(Type::PrimitiveType {
            basic_info,
            physical_type: self.physical_type,
            type_length: self.length,
            scale: self.scale,
            precision: self.precision,
        })
    }

    #[inline]
    fn check_decimal_precision_scale(&self) -> Result<()> {
        match self.physical_type {
            PhysicalType::INT32
            | PhysicalType::INT64
            | PhysicalType::BYTE_ARRAY
            | PhysicalType::FIXED_LEN_BYTE_ARRAY => (),
            _ => {
                return Err(general_err!(
                    "DECIMAL can only annotate INT32, INT64, BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY"
                ));
            }
        }

        // Precision is required and must be a non-zero positive integer.
        if self.precision < 1 {
            return Err(general_err!(
                "Invalid DECIMAL precision: {}",
                self.precision
            ));
        }

        // Scale must be zero or a positive integer less than the precision.
        if self.scale < 0 {
            return Err(general_err!("Invalid DECIMAL scale: {}", self.scale));
        }

        if self.scale >= self.precision {
            return Err(general_err!(
            "Invalid DECIMAL: scale ({}) cannot be greater than or equal to precision \
             ({})",
            self.scale,
            self.precision
        ));
        }

        // Check precision and scale based on physical type limitations.
        match self.physical_type {
            PhysicalType::INT32 => {
                if self.precision > 9 {
                    return Err(general_err!(
                        "Cannot represent INT32 as DECIMAL with precision {}",
                        self.precision
                    ));
                }
            }
            PhysicalType::INT64 => {
                if self.precision > 18 {
                    return Err(general_err!(
                        "Cannot represent INT64 as DECIMAL with precision {}",
                        self.precision
                    ));
                }
            }
            PhysicalType::FIXED_LEN_BYTE_ARRAY => {
                let max_precision =
                    (2f64.powi(8 * self.length - 1) - 1f64).log10().floor() as i32;

                if self.precision > max_precision {
                    return Err(general_err!(
                        "Cannot represent FIXED_LEN_BYTE_ARRAY as DECIMAL with length {} and \
                        precision {}. The max precision can only be {}",
                        self.length,
                        self.precision,
                        max_precision
                    ));
                }
            }
            _ => (), // For BYTE_ARRAY precision is not limited
        }

        Ok(())
    }
}

/// A builder for group types. All attributes are optional except the name.
/// Note that if not specified explicitly, `None` is used as the repetition of the group,
/// which means it is a root (message) type.
pub struct GroupTypeBuilder<'a> {
    name: &'a str,
    repetition: Option<Repetition>,
    converted_type: ConvertedType,
    logical_type: Option<LogicalType>,
    fields: Vec<TypePtr>,
    id: Option<i32>,
}

impl<'a> GroupTypeBuilder<'a> {
    /// Creates new group type builder with provided field name.
    pub fn new(name: &'a str) -> Self {
        Self {
            name,
            repetition: None,
            converted_type: ConvertedType::NONE,
            logical_type: None,
            fields: Vec::new(),
            id: None,
        }
    }

    /// Sets [`Repetition`](crate::basic::Repetition) for this field and returns itself.
    pub fn with_repetition(mut self, repetition: Repetition) -> Self {
        self.repetition = Some(repetition);
        self
    }

    /// Sets [`ConvertedType`](crate::basic::ConvertedType) for this field and returns itself.
    pub fn with_converted_type(mut self, converted_type: ConvertedType) -> Self {
        self.converted_type = converted_type;
        self
    }

    /// Sets [`LogicalType`](crate::basic::LogicalType) for this field and returns itself.
    pub fn with_logical_type(mut self, logical_type: Option<LogicalType>) -> Self {
        self.logical_type = logical_type;
        self
    }

    /// Sets a list of fields that should be child nodes of this field.
    /// Returns updated self.
    pub fn with_fields(mut self, fields: &mut Vec<TypePtr>) -> Self {
        self.fields.append(fields);
        self
    }

    /// Sets optional field id and returns itself.
    pub fn with_id(mut self, id: i32) -> Self {
        self.id = Some(id);
        self
    }

    /// Creates a new `GroupType` instance from the gathered attributes.
    pub fn build(self) -> Result<Type> {
        let mut basic_info = BasicTypeInfo {
            name: String::from(self.name),
            repetition: self.repetition,
            converted_type: self.converted_type,
            logical_type: self.logical_type.clone(),
            id: self.id,
        };
        // Populate the converted type if only the logical type is populated
        if self.logical_type.is_some() && self.converted_type == ConvertedType::NONE {
            basic_info.converted_type = self.logical_type.into();
        }
        Ok(Type::GroupType {
            basic_info,
            fields: self.fields,
        })
    }
}

/// Basic type info. This contains information such as the name of the type,
/// the repetition level, the logical type and the kind of the type (group, primitive).
#[derive(Clone, Debug, PartialEq)]
pub struct BasicTypeInfo {
    name: String,
    repetition: Option<Repetition>,
    converted_type: ConvertedType,
    logical_type: Option<LogicalType>,
    id: Option<i32>,
}

impl BasicTypeInfo {
    /// Returns field name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns `true` if type has repetition field set, `false` otherwise.
    /// This is mostly applied to group type, because primitive type always has
    /// repetition set.
    pub fn has_repetition(&self) -> bool {
        self.repetition.is_some()
    }

    /// Returns [`Repetition`](crate::basic::Repetition) value for the type.
    pub fn repetition(&self) -> Repetition {
        assert!(self.repetition.is_some());
        self.repetition.unwrap()
    }

    /// Returns [`ConvertedType`](crate::basic::ConvertedType) value for the type.
    pub fn converted_type(&self) -> ConvertedType {
        self.converted_type
    }

    /// Returns [`LogicalType`](crate::basic::LogicalType) value for the type.
    pub fn logical_type(&self) -> Option<LogicalType> {
        // Unlike ConvertedType, LogicalType cannot implement Copy, thus we clone it
        self.logical_type.clone()
    }

    /// Returns `true` if id is set, `false` otherwise.
    pub fn has_id(&self) -> bool {
        self.id.is_some()
    }

    /// Returns id value for the type.
    pub fn id(&self) -> i32 {
        assert!(self.id.is_some());
        self.id.unwrap()
    }
}

// ----------------------------------------------------------------------
// Parquet descriptor definitions

/// Represents a path in a nested schema
#[derive(Clone, PartialEq, Debug, Eq, Hash)]
pub struct ColumnPath {
    parts: Vec<String>,
}

impl ColumnPath {
    /// Creates new column path from vector of field names.
    pub fn new(parts: Vec<String>) -> Self {
        ColumnPath { parts }
    }

    /// Returns string representation of this column path.
    /// ```rust
    /// use parquet::schema::types::ColumnPath;
    ///
    /// let path = ColumnPath::new(vec!["a".to_string(), "b".to_string(), "c".to_string()]);
    /// assert_eq!(&path.string(), "a.b.c");
    /// ```
    pub fn string(&self) -> String {
        self.parts.join(".")
    }

    /// Appends more components to end of column path.
    /// ```rust
    /// use parquet::schema::types::ColumnPath;
    ///
    /// let mut path = ColumnPath::new(vec!["a".to_string(), "b".to_string(), "c"
    /// .to_string()]);
    /// assert_eq!(&path.string(), "a.b.c");
    ///
    /// path.append(vec!["d".to_string(), "e".to_string()]);
    /// assert_eq!(&path.string(), "a.b.c.d.e");
    /// ```
    pub fn append(&mut self, mut tail: Vec<String>) {
        self.parts.append(&mut tail);
    }

    pub fn parts(&self) -> &[String] {
        &self.parts
    }
}

impl fmt::Display for ColumnPath {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self.string())
    }
}

impl From<Vec<String>> for ColumnPath {
    fn from(parts: Vec<String>) -> Self {
        ColumnPath { parts }
    }
}

impl<'a> From<&'a str> for ColumnPath {
    fn from(single_path: &str) -> Self {
        let s = String::from(single_path);
        ColumnPath::from(s)
    }
}

impl From<String> for ColumnPath {
    fn from(single_path: String) -> Self {
        let v = vec![single_path];
        ColumnPath { parts: v }
    }
}

impl AsRef<[String]> for ColumnPath {
    fn as_ref(&self) -> &[String] {
        &self.parts
    }
}

/// A descriptor for leaf-level primitive columns.
/// This encapsulates information such as definition and repetition levels and is used to
/// re-assemble nested data.
#[derive(Debug, PartialEq)]
pub struct ColumnDescriptor {
    // The "leaf" primitive type of this column
    primitive_type: TypePtr,

    // The maximum definition level for this column
    max_def_level: i16,

    // The maximum repetition level for this column
    max_rep_level: i16,

    // The path of this column. For instance, "a.b.c.d".
    path: ColumnPath,
}

impl ColumnDescriptor {
    /// Creates new descriptor for leaf-level column.
    pub fn new(
        primitive_type: TypePtr,
        max_def_level: i16,
        max_rep_level: i16,
        path: ColumnPath,
    ) -> Self {
        Self {
            primitive_type,
            max_def_level,
            max_rep_level,
            path,
        }
    }

    /// Returns maximum definition level for this column.
    #[inline]
    pub fn max_def_level(&self) -> i16 {
        self.max_def_level
    }

    /// Returns maximum repetition level for this column.
    #[inline]
    pub fn max_rep_level(&self) -> i16 {
        self.max_rep_level
    }

    /// Returns [`ColumnPath`] for this column.
    pub fn path(&self) -> &ColumnPath {
        &self.path
    }

    /// Returns self type [`Type`](crate::schema::types::Type) for this leaf column.
    pub fn self_type(&self) -> &Type {
        self.primitive_type.as_ref()
    }

    /// Returns self type [`TypePtr`](crate::schema::types::TypePtr)  for this leaf
    /// column.
    pub fn self_type_ptr(&self) -> TypePtr {
        self.primitive_type.clone()
    }

    /// Returns column name.
    pub fn name(&self) -> &str {
        self.primitive_type.name()
    }

    /// Returns [`ConvertedType`](crate::basic::ConvertedType) for this column.
    pub fn converted_type(&self) -> ConvertedType {
        self.primitive_type.get_basic_info().converted_type()
    }

    /// Returns [`LogicalType`](crate::basic::LogicalType) for this column.
    pub fn logical_type(&self) -> Option<LogicalType> {
        self.primitive_type.get_basic_info().logical_type()
    }

    /// Returns physical type for this column.
    /// Note that it will panic if called on a non-primitive type.
    pub fn physical_type(&self) -> PhysicalType {
        match self.primitive_type.as_ref() {
            Type::PrimitiveType { physical_type, .. } => *physical_type,
            _ => panic!("Expected primitive type!"),
        }
    }

    /// Returns type length for this column.
    /// Note that it will panic if called on a non-primitive type.
    pub fn type_length(&self) -> i32 {
        match self.primitive_type.as_ref() {
            Type::PrimitiveType { type_length, .. } => *type_length,
            _ => panic!("Expected primitive type!"),
        }
    }

    /// Returns type precision for this column.
    /// Note that it will panic if called on a non-primitive type.
    pub fn type_precision(&self) -> i32 {
        match self.primitive_type.as_ref() {
            Type::PrimitiveType { precision, .. } => *precision,
            _ => panic!("Expected primitive type!"),
        }
    }

    /// Returns type scale for this column.
    /// Note that it will panic if called on a non-primitive type.
    pub fn type_scale(&self) -> i32 {
        match self.primitive_type.as_ref() {
            Type::PrimitiveType { scale, .. } => *scale,
            _ => panic!("Expected primitive type!"),
        }
    }
}

/// A schema descriptor. This encapsulates the top-level schemas for all the columns,
/// as well as all descriptors for all the primitive columns.
pub struct SchemaDescriptor {
    // The top-level schema (the "message" type).
    // This must be a `GroupType` where each field is a root column type in the schema.
    schema: TypePtr,

    // All the descriptors for primitive columns in this schema, constructed from
    // `schema` in DFS order.
    leaves: Vec<ColumnDescPtr>,

    // Mapping from a leaf column's index to the root column type that it
    // comes from. For instance: the leaf `a.b.c.d` would have a link back to `a`:
    // -- a  <-----+
    // -- -- b     |
    // -- -- -- c  |
    // -- -- -- -- d
    leaf_to_base: Vec<TypePtr>,
}

impl fmt::Debug for SchemaDescriptor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Skip leaves and leaf_to_base as they only a cache information already found in `schema`
        f.debug_struct("SchemaDescriptor")
            .field("schema", &self.schema)
            .finish()
    }
}

impl SchemaDescriptor {
    /// Creates new schema descriptor from Parquet schema.
    pub fn new(tp: TypePtr) -> Self {
        assert!(tp.is_group(), "SchemaDescriptor should take a GroupType");
        let mut leaves = vec![];
        let mut leaf_to_base = Vec::new();
        for f in tp.get_fields() {
            let mut path = vec![];
            build_tree(f, f, 0, 0, &mut leaves, &mut leaf_to_base, &mut path);
        }

        Self {
            schema: tp,
            leaves,
            leaf_to_base,
        }
    }

    /// Returns [`ColumnDescriptor`] for a field position.
    pub fn column(&self, i: usize) -> ColumnDescPtr {
        assert!(
            i < self.leaves.len(),
            "Index out of bound: {} not in [0, {})",
            i,
            self.leaves.len()
        );
        self.leaves[i].clone()
    }

    /// Returns slice of [`ColumnDescriptor`].
    pub fn columns(&self) -> &[ColumnDescPtr] {
        &self.leaves
    }

    /// Returns number of leaf-level columns.
    pub fn num_columns(&self) -> usize {
        self.leaves.len()
    }

    /// Returns column root [`Type`](crate::schema::types::Type) for a field position.
    pub fn get_column_root(&self, i: usize) -> &Type {
        let result = self.column_root_of(i);
        result.as_ref()
    }

    /// Returns column root [`Type`](crate::schema::types::Type) pointer for a field
    /// position.
    pub fn get_column_root_ptr(&self, i: usize) -> TypePtr {
        let result = self.column_root_of(i);
        result.clone()
    }

    fn column_root_of(&self, i: usize) -> &Arc<Type> {
        assert!(
            i < self.leaves.len(),
            "Index out of bound: {} not in [0, {})",
            i,
            self.leaves.len()
        );

        self.leaf_to_base
            .get(i)
            .unwrap_or_else(|| panic!("Expected a value for index {} but found None", i))
    }

    /// Returns schema as [`Type`](crate::schema::types::Type).
    pub fn root_schema(&self) -> &Type {
        self.schema.as_ref()
    }

    pub fn root_schema_ptr(&self) -> TypePtr {
        self.schema.clone()
    }

    /// Returns schema name.
    pub fn name(&self) -> &str {
        self.schema.name()
    }
}

fn build_tree<'a>(
    tp: &'a TypePtr,
    base_tp: &TypePtr,
    mut max_rep_level: i16,
    mut max_def_level: i16,
    leaves: &mut Vec<ColumnDescPtr>,
    leaf_to_base: &mut Vec<TypePtr>,
    path_so_far: &mut Vec<&'a str>,
) {
    assert!(tp.get_basic_info().has_repetition());

    path_so_far.push(tp.name());
    match tp.get_basic_info().repetition() {
        Repetition::OPTIONAL => {
            max_def_level += 1;
        }
        Repetition::REPEATED => {
            max_def_level += 1;
            max_rep_level += 1;
        }
        _ => {}
    }

    match tp.as_ref() {
        Type::PrimitiveType { .. } => {
            let mut path: Vec<String> = vec![];
            path.extend(path_so_far.iter().copied().map(String::from));
            leaves.push(Arc::new(ColumnDescriptor::new(
                tp.clone(),
                max_def_level,
                max_rep_level,
                ColumnPath::new(path),
            )));
            leaf_to_base.push(base_tp.clone());
        }
        Type::GroupType { ref fields, .. } => {
            for f in fields {
                build_tree(
                    f,
                    base_tp,
                    max_rep_level,
                    max_def_level,
                    leaves,
                    leaf_to_base,
                    path_so_far,
                );
                path_so_far.pop();
            }
        }
    }
}

/// Method to convert from Thrift.
pub fn from_thrift(elements: &[SchemaElement]) -> Result<TypePtr> {
    let mut index = 0;
    let mut schema_nodes = Vec::new();
    while index < elements.len() {
        let t = from_thrift_helper(elements, index)?;
        index = t.0;
        schema_nodes.push(t.1);
    }
    if schema_nodes.len() != 1 {
        return Err(general_err!(
            "Expected exactly one root node, but found {}",
            schema_nodes.len()
        ));
    }

    Ok(schema_nodes.remove(0))
}

/// Constructs a new Type from the `elements`, starting at index `index`.
/// The first result is the starting index for the next Type after this one. If it is
/// equal to `elements.len()`, then this Type is the last one.
/// The second result is the result Type.
fn from_thrift_helper(
    elements: &[SchemaElement],
    index: usize,
) -> Result<(usize, TypePtr)> {
    // Whether or not the current node is root (message type).
    // There is only one message type node in the schema tree.
    let is_root_node = index == 0;

    if index > elements.len() {
        return Err(general_err!(
            "Index out of bound, index = {}, len = {}",
            index,
            elements.len()
        ));
    }
    let element = &elements[index];
    let converted_type = ConvertedType::from(element.converted_type);
    // LogicalType is only present in v2 Parquet files. ConvertedType is always
    // populated, regardless of the version of the file (v1 or v2).
    let logical_type = element
        .logical_type
        .as_ref()
        .map(|value| LogicalType::from(value.clone()));
    let field_id = elements[index].field_id;
    match elements[index].num_children {
        // From parquet-format:
        //   The children count is used to construct the nested relationship.
        //   This field is not set when the element is a primitive type
        // Sometimes parquet-cpp sets num_children field to 0 for primitive types, so we
        // have to handle this case too.
        None | Some(0) => {
            // primitive type
            if elements[index].repetition_type.is_none() {
                return Err(general_err!(
                    "Repetition level must be defined for a primitive type"
                ));
            }
            let repetition = Repetition::from(elements[index].repetition_type.unwrap());
            let physical_type = PhysicalType::from(elements[index].type_.unwrap());
            let length = elements[index].type_length.unwrap_or(-1);
            let scale = elements[index].scale.unwrap_or(-1);
            let precision = elements[index].precision.unwrap_or(-1);
            let name = &elements[index].name;
            let mut builder = Type::primitive_type_builder(name, physical_type)
                .with_repetition(repetition)
                .with_converted_type(converted_type)
                .with_logical_type(logical_type)
                .with_length(length)
                .with_precision(precision)
                .with_scale(scale);
            if let Some(id) = field_id {
                builder = builder.with_id(id);
            }
            Ok((index + 1, Arc::new(builder.build()?)))
        }
        Some(n) => {
            let repetition = elements[index].repetition_type.map(Repetition::from);
            let mut fields = vec![];
            let mut next_index = index + 1;
            for _ in 0..n {
                let child_result = from_thrift_helper(elements, next_index as usize)?;
                next_index = child_result.0;
                fields.push(child_result.1);
            }

            let mut builder = Type::group_type_builder(&elements[index].name)
                .with_converted_type(converted_type)
                .with_logical_type(logical_type)
                .with_fields(&mut fields);
            if let Some(rep) = repetition {
                // Sometimes parquet-cpp and parquet-mr set repetition level REQUIRED or
                // REPEATED for root node.
                //
                // We only set repetition for group types that are not top-level message
                // type. According to parquet-format:
                //   Root of the schema does not have a repetition_type.
                //   All other types must have one.
                if !is_root_node {
                    builder = builder.with_repetition(rep);
                }
            }
            if let Some(id) = field_id {
                builder = builder.with_id(id);
            }
            Ok((next_index, Arc::new(builder.build().unwrap())))
        }
    }
}

/// Method to convert to Thrift.
pub fn to_thrift(schema: &Type) -> Result<Vec<SchemaElement>> {
    if !schema.is_group() {
        return Err(general_err!("Root schema must be Group type"));
    }
    let mut elements: Vec<SchemaElement> = Vec::new();
    to_thrift_helper(schema, &mut elements);
    Ok(elements)
}

/// Constructs list of `SchemaElement` from the schema using depth-first traversal.
/// Here we assume that schema is always valid and starts with group type.
fn to_thrift_helper(schema: &Type, elements: &mut Vec<SchemaElement>) {
    match *schema {
        Type::PrimitiveType {
            ref basic_info,
            physical_type,
            type_length,
            scale,
            precision,
        } => {
            let element = SchemaElement {
                type_: Some(physical_type.into()),
                type_length: if type_length >= 0 {
                    Some(type_length)
                } else {
                    None
                },
                repetition_type: Some(basic_info.repetition().into()),
                name: basic_info.name().to_owned(),
                num_children: None,
                converted_type: basic_info.converted_type().into(),
                scale: if scale >= 0 { Some(scale) } else { None },
                precision: if precision >= 0 {
                    Some(precision)
                } else {
                    None
                },
                field_id: if basic_info.has_id() {
                    Some(basic_info.id())
                } else {
                    None
                },
                logical_type: basic_info.logical_type().map(|value| value.into()),
            };

            elements.push(element);
        }
        Type::GroupType {
            ref basic_info,
            ref fields,
        } => {
            let repetition = if basic_info.has_repetition() {
                Some(basic_info.repetition().into())
            } else {
                None
            };

            let element = SchemaElement {
                type_: None,
                type_length: None,
                repetition_type: repetition,
                name: basic_info.name().to_owned(),
                num_children: Some(fields.len() as i32),
                converted_type: basic_info.converted_type().into(),
                scale: None,
                precision: None,
                field_id: if basic_info.has_id() {
                    Some(basic_info.id())
                } else {
                    None
                },
                logical_type: basic_info.logical_type().map(|value| value.into()),
            };

            elements.push(element);

            // Add child elements for a group
            for field in fields {
                to_thrift_helper(field, elements);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::basic::{DecimalType, IntType};
    use crate::schema::parser::parse_message_type;

    // TODO: add tests for v2 types

    #[test]
    fn test_primitive_type() {
        let mut result = Type::primitive_type_builder("foo", PhysicalType::INT32)
            .with_logical_type(Some(LogicalType::INTEGER(IntType {
                bit_width: 32,
                is_signed: true,
            })))
            .with_id(0)
            .build();
        assert!(result.is_ok());

        if let Ok(tp) = result {
            assert!(tp.is_primitive());
            assert!(!tp.is_group());
            let basic_info = tp.get_basic_info();
            assert_eq!(basic_info.repetition(), Repetition::OPTIONAL);
            assert_eq!(
                basic_info.logical_type(),
                Some(LogicalType::INTEGER(IntType {
                    bit_width: 32,
                    is_signed: true
                }))
            );
            assert_eq!(basic_info.converted_type(), ConvertedType::INT_32);
            assert_eq!(basic_info.id(), 0);
            match tp {
                Type::PrimitiveType { physical_type, .. } => {
                    assert_eq!(physical_type, PhysicalType::INT32);
                }
                _ => panic!(),
            }
        }

        // Test illegal inputs with logical type
        result = Type::primitive_type_builder("foo", PhysicalType::INT64)
            .with_repetition(Repetition::REPEATED)
            .with_logical_type(Some(LogicalType::INTEGER(IntType {
                is_signed: true,
                bit_width: 8,
            })))
            .build();
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(
                format!("{}", e),
                "Parquet error: Cannot annotate INTEGER(IntType { bit_width: 8, is_signed: true }) from INT64 fields"
            );
        }

        // Test illegal inputs with converted type
        result = Type::primitive_type_builder("foo", PhysicalType::INT64)
            .with_repetition(Repetition::REPEATED)
            .with_converted_type(ConvertedType::BSON)
            .build();
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(
                format!("{}", e),
                "Parquet error: BSON can only annotate BYTE_ARRAY fields"
            );
        }

        result = Type::primitive_type_builder("foo", PhysicalType::INT96)
            .with_repetition(Repetition::REQUIRED)
            .with_converted_type(ConvertedType::DECIMAL)
            .with_precision(-1)
            .with_scale(-1)
            .build();
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(
                format!("{}", e),
                "Parquet error: DECIMAL can only annotate INT32, INT64, BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY"
            );
        }

        result = Type::primitive_type_builder("foo", PhysicalType::BYTE_ARRAY)
            .with_repetition(Repetition::REQUIRED)
            .with_logical_type(Some(LogicalType::DECIMAL(DecimalType {
                scale: 32,
                precision: 12,
            })))
            .with_precision(-1)
            .with_scale(-1)
            .build();
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(
                format!("{}", e),
                "Parquet error: DECIMAL logical type scale 32 must match self.scale -1"
            );
        }

        result = Type::primitive_type_builder("foo", PhysicalType::BYTE_ARRAY)
            .with_repetition(Repetition::REQUIRED)
            .with_converted_type(ConvertedType::DECIMAL)
            .with_precision(-1)
            .with_scale(-1)
            .build();
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(
                format!("{}", e),
                "Parquet error: Invalid DECIMAL precision: -1"
            );
        }

        result = Type::primitive_type_builder("foo", PhysicalType::BYTE_ARRAY)
            .with_repetition(Repetition::REQUIRED)
            .with_converted_type(ConvertedType::DECIMAL)
            .with_precision(0)
            .with_scale(-1)
            .build();
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(
                format!("{}", e),
                "Parquet error: Invalid DECIMAL precision: 0"
            );
        }

        result = Type::primitive_type_builder("foo", PhysicalType::BYTE_ARRAY)
            .with_repetition(Repetition::REQUIRED)
            .with_converted_type(ConvertedType::DECIMAL)
            .with_precision(1)
            .with_scale(-1)
            .build();
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(format!("{}", e), "Parquet error: Invalid DECIMAL scale: -1");
        }

        result = Type::primitive_type_builder("foo", PhysicalType::BYTE_ARRAY)
            .with_repetition(Repetition::REQUIRED)
            .with_converted_type(ConvertedType::DECIMAL)
            .with_precision(1)
            .with_scale(2)
            .build();
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(
                format!("{}", e),
                "Parquet error: Invalid DECIMAL: scale (2) cannot be greater than or equal to precision (1)"
            );
        }

        result = Type::primitive_type_builder("foo", PhysicalType::INT32)
            .with_repetition(Repetition::REQUIRED)
            .with_converted_type(ConvertedType::DECIMAL)
            .with_precision(18)
            .with_scale(2)
            .build();
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(
                format!("{}", e),
                "Parquet error: Cannot represent INT32 as DECIMAL with precision 18"
            );
        }

        result = Type::primitive_type_builder("foo", PhysicalType::INT64)
            .with_repetition(Repetition::REQUIRED)
            .with_converted_type(ConvertedType::DECIMAL)
            .with_precision(32)
            .with_scale(2)
            .build();
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(
                format!("{}", e),
                "Parquet error: Cannot represent INT64 as DECIMAL with precision 32"
            );
        }

        result = Type::primitive_type_builder("foo", PhysicalType::FIXED_LEN_BYTE_ARRAY)
            .with_repetition(Repetition::REQUIRED)
            .with_converted_type(ConvertedType::DECIMAL)
            .with_length(5)
            .with_precision(12)
            .with_scale(2)
            .build();
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(
                format!("{}", e),
                "Parquet error: Cannot represent FIXED_LEN_BYTE_ARRAY as DECIMAL with length 5 and precision 12. The max precision can only be 11"
            );
        }

        result = Type::primitive_type_builder("foo", PhysicalType::INT64)
            .with_repetition(Repetition::REQUIRED)
            .with_converted_type(ConvertedType::UINT_8)
            .build();
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(
                format!("{}", e),
                "Parquet error: UINT_8 can only annotate INT32"
            );
        }

        result = Type::primitive_type_builder("foo", PhysicalType::INT32)
            .with_repetition(Repetition::REQUIRED)
            .with_converted_type(ConvertedType::TIME_MICROS)
            .build();
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(
                format!("{}", e),
                "Parquet error: TIME_MICROS can only annotate INT64"
            );
        }

        result = Type::primitive_type_builder("foo", PhysicalType::BYTE_ARRAY)
            .with_repetition(Repetition::REQUIRED)
            .with_converted_type(ConvertedType::INTERVAL)
            .build();
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(
                format!("{}", e),
                "Parquet error: INTERVAL can only annotate FIXED_LEN_BYTE_ARRAY(12)"
            );
        }

        result = Type::primitive_type_builder("foo", PhysicalType::FIXED_LEN_BYTE_ARRAY)
            .with_repetition(Repetition::REQUIRED)
            .with_converted_type(ConvertedType::INTERVAL)
            .with_length(1)
            .build();
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(
                format!("{}", e),
                "Parquet error: INTERVAL can only annotate FIXED_LEN_BYTE_ARRAY(12)"
            );
        }

        result = Type::primitive_type_builder("foo", PhysicalType::INT32)
            .with_repetition(Repetition::REQUIRED)
            .with_converted_type(ConvertedType::ENUM)
            .build();
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(
                format!("{}", e),
                "Parquet error: ENUM can only annotate BYTE_ARRAY fields"
            );
        }

        result = Type::primitive_type_builder("foo", PhysicalType::INT32)
            .with_repetition(Repetition::REQUIRED)
            .with_converted_type(ConvertedType::MAP)
            .build();
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(
                format!("{}", e),
                "Parquet error: MAP cannot be applied to a primitive type"
            );
        }

        result = Type::primitive_type_builder("foo", PhysicalType::FIXED_LEN_BYTE_ARRAY)
            .with_repetition(Repetition::REQUIRED)
            .with_converted_type(ConvertedType::DECIMAL)
            .with_length(-1)
            .build();
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(
                format!("{}", e),
                "Parquet error: Invalid FIXED_LEN_BYTE_ARRAY length: -1"
            );
        }
    }

    #[test]
    fn test_group_type() {
        let f1 = Type::primitive_type_builder("f1", PhysicalType::INT32)
            .with_converted_type(ConvertedType::INT_32)
            .with_id(0)
            .build();
        assert!(f1.is_ok());
        let f2 = Type::primitive_type_builder("f2", PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8)
            .with_id(1)
            .build();
        assert!(f2.is_ok());

        let mut fields = vec![];
        fields.push(Arc::new(f1.unwrap()));
        fields.push(Arc::new(f2.unwrap()));

        let result = Type::group_type_builder("foo")
            .with_repetition(Repetition::REPEATED)
            .with_logical_type(Some(LogicalType::LIST(Default::default())))
            .with_fields(&mut fields)
            .with_id(1)
            .build();
        assert!(result.is_ok());

        let tp = result.unwrap();
        let basic_info = tp.get_basic_info();
        assert!(tp.is_group());
        assert!(!tp.is_primitive());
        assert_eq!(basic_info.repetition(), Repetition::REPEATED);
        assert_eq!(
            basic_info.logical_type(),
            Some(LogicalType::LIST(Default::default()))
        );
        assert_eq!(basic_info.converted_type(), ConvertedType::LIST);
        assert_eq!(basic_info.id(), 1);
        assert_eq!(tp.get_fields().len(), 2);
        assert_eq!(tp.get_fields()[0].name(), "f1");
        assert_eq!(tp.get_fields()[1].name(), "f2");
    }

    #[test]
    fn test_column_descriptor() {
        let result = test_column_descriptor_helper();
        assert!(
            result.is_ok(),
            "Expected result to be OK but got err:\n {}",
            result.unwrap_err()
        );
    }

    fn test_column_descriptor_helper() -> Result<()> {
        let tp = Type::primitive_type_builder("name", PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8)
            .build()?;

        let descr = ColumnDescriptor::new(Arc::new(tp), 4, 1, ColumnPath::from("name"));

        assert_eq!(descr.path(), &ColumnPath::from("name"));
        assert_eq!(descr.converted_type(), ConvertedType::UTF8);
        assert_eq!(descr.physical_type(), PhysicalType::BYTE_ARRAY);
        assert_eq!(descr.max_def_level(), 4);
        assert_eq!(descr.max_rep_level(), 1);
        assert_eq!(descr.name(), "name");
        assert_eq!(descr.type_length(), -1);
        assert_eq!(descr.type_precision(), -1);
        assert_eq!(descr.type_scale(), -1);

        Ok(())
    }

    #[test]
    fn test_schema_descriptor() {
        let result = test_schema_descriptor_helper();
        assert!(
            result.is_ok(),
            "Expected result to be OK but got err:\n {}",
            result.unwrap_err()
        );
    }

    // A helper fn to avoid handling the results from type creation
    fn test_schema_descriptor_helper() -> Result<()> {
        let mut fields = vec![];

        let inta = Type::primitive_type_builder("a", PhysicalType::INT32)
            .with_repetition(Repetition::REQUIRED)
            .with_converted_type(ConvertedType::INT_32)
            .build()?;
        fields.push(Arc::new(inta));
        let intb = Type::primitive_type_builder("b", PhysicalType::INT64)
            .with_converted_type(ConvertedType::INT_64)
            .build()?;
        fields.push(Arc::new(intb));
        let intc = Type::primitive_type_builder("c", PhysicalType::BYTE_ARRAY)
            .with_repetition(Repetition::REPEATED)
            .with_converted_type(ConvertedType::UTF8)
            .build()?;
        fields.push(Arc::new(intc));

        // 3-level list encoding
        let item1 = Type::primitive_type_builder("item1", PhysicalType::INT64)
            .with_repetition(Repetition::REQUIRED)
            .with_converted_type(ConvertedType::INT_64)
            .build()?;
        let item2 =
            Type::primitive_type_builder("item2", PhysicalType::BOOLEAN).build()?;
        let item3 = Type::primitive_type_builder("item3", PhysicalType::INT32)
            .with_repetition(Repetition::REPEATED)
            .with_converted_type(ConvertedType::INT_32)
            .build()?;
        let list = Type::group_type_builder("records")
            .with_repetition(Repetition::REPEATED)
            .with_converted_type(ConvertedType::LIST)
            .with_fields(&mut vec![Arc::new(item1), Arc::new(item2), Arc::new(item3)])
            .build()?;
        let bag = Type::group_type_builder("bag")
            .with_repetition(Repetition::OPTIONAL)
            .with_fields(&mut vec![Arc::new(list)])
            .build()?;
        fields.push(Arc::new(bag));

        let schema = Type::group_type_builder("schema")
            .with_repetition(Repetition::REPEATED)
            .with_fields(&mut fields)
            .build()?;
        let descr = SchemaDescriptor::new(Arc::new(schema));

        let nleaves = 6;
        assert_eq!(descr.num_columns(), nleaves);

        //                             mdef mrep
        // required int32 a            0    0
        // optional int64 b            1    0
        // repeated byte_array c       1    1
        // optional group bag          1    0
        //   repeated group records    2    1
        //     required int64 item1    2    1
        //     optional boolean item2  3    1
        //     repeated int32 item3    3    2
        let ex_max_def_levels = vec![0, 1, 1, 2, 3, 3];
        let ex_max_rep_levels = vec![0, 0, 1, 1, 1, 2];

        for i in 0..nleaves {
            let col = descr.column(i);
            assert_eq!(col.max_def_level(), ex_max_def_levels[i], "{}", i);
            assert_eq!(col.max_rep_level(), ex_max_rep_levels[i], "{}", i);
        }

        assert_eq!(descr.column(0).path().string(), "a");
        assert_eq!(descr.column(1).path().string(), "b");
        assert_eq!(descr.column(2).path().string(), "c");
        assert_eq!(descr.column(3).path().string(), "bag.records.item1");
        assert_eq!(descr.column(4).path().string(), "bag.records.item2");
        assert_eq!(descr.column(5).path().string(), "bag.records.item3");

        assert_eq!(descr.get_column_root(0).name(), "a");
        assert_eq!(descr.get_column_root(3).name(), "bag");
        assert_eq!(descr.get_column_root(4).name(), "bag");
        assert_eq!(descr.get_column_root(5).name(), "bag");

        Ok(())
    }

    #[test]
    fn test_schema_build_tree_def_rep_levels() {
        let message_type = "
    message spark_schema {
      REQUIRED INT32 a;
      OPTIONAL group b {
        OPTIONAL INT32 _1;
        OPTIONAL INT32 _2;
      }
      OPTIONAL group c (LIST) {
        REPEATED group list {
          OPTIONAL INT32 element;
        }
      }
    }
    ";
        let schema = parse_message_type(message_type).expect("should parse schema");
        let descr = SchemaDescriptor::new(Arc::new(schema));
        // required int32 a
        assert_eq!(descr.column(0).max_def_level(), 0);
        assert_eq!(descr.column(0).max_rep_level(), 0);
        // optional int32 b._1
        assert_eq!(descr.column(1).max_def_level(), 2);
        assert_eq!(descr.column(1).max_rep_level(), 0);
        // optional int32 b._2
        assert_eq!(descr.column(2).max_def_level(), 2);
        assert_eq!(descr.column(2).max_rep_level(), 0);
        // repeated optional int32 c.list.element
        assert_eq!(descr.column(3).max_def_level(), 3);
        assert_eq!(descr.column(3).max_rep_level(), 1);
    }

    #[test]
    #[should_panic(expected = "Cannot call get_physical_type() on a non-primitive type")]
    fn test_get_physical_type_panic() {
        let list = Type::group_type_builder("records")
            .with_repetition(Repetition::REPEATED)
            .build()
            .unwrap();
        list.get_physical_type();
    }

    #[test]
    fn test_get_physical_type_primitive() {
        let f = Type::primitive_type_builder("f", PhysicalType::INT64)
            .build()
            .unwrap();
        assert_eq!(f.get_physical_type(), PhysicalType::INT64);

        let f = Type::primitive_type_builder("f", PhysicalType::BYTE_ARRAY)
            .build()
            .unwrap();
        assert_eq!(f.get_physical_type(), PhysicalType::BYTE_ARRAY);
    }

    #[test]
    fn test_check_contains_primitive_primitive() {
        // OK
        let f1 = Type::primitive_type_builder("f", PhysicalType::INT32)
            .build()
            .unwrap();
        let f2 = Type::primitive_type_builder("f", PhysicalType::INT32)
            .build()
            .unwrap();
        assert!(f1.check_contains(&f2));

        // OK: different logical type does not affect check_contains
        let f1 = Type::primitive_type_builder("f", PhysicalType::INT32)
            .with_converted_type(ConvertedType::UINT_8)
            .build()
            .unwrap();
        let f2 = Type::primitive_type_builder("f", PhysicalType::INT32)
            .with_converted_type(ConvertedType::UINT_16)
            .build()
            .unwrap();
        assert!(f1.check_contains(&f2));

        // KO: different name
        let f1 = Type::primitive_type_builder("f1", PhysicalType::INT32)
            .build()
            .unwrap();
        let f2 = Type::primitive_type_builder("f2", PhysicalType::INT32)
            .build()
            .unwrap();
        assert!(!f1.check_contains(&f2));

        // KO: different type
        let f1 = Type::primitive_type_builder("f", PhysicalType::INT32)
            .build()
            .unwrap();
        let f2 = Type::primitive_type_builder("f", PhysicalType::INT64)
            .build()
            .unwrap();
        assert!(!f1.check_contains(&f2));

        // KO: different repetition
        let f1 = Type::primitive_type_builder("f", PhysicalType::INT32)
            .with_repetition(Repetition::REQUIRED)
            .build()
            .unwrap();
        let f2 = Type::primitive_type_builder("f", PhysicalType::INT32)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();
        assert!(!f1.check_contains(&f2));
    }

    // function to create a new group type for testing
    fn test_new_group_type(name: &str, repetition: Repetition, types: Vec<Type>) -> Type {
        let mut fields = Vec::new();
        for tpe in types {
            fields.push(Arc::new(tpe))
        }
        Type::group_type_builder(name)
            .with_repetition(repetition)
            .with_fields(&mut fields)
            .build()
            .unwrap()
    }

    #[test]
    fn test_check_contains_group_group() {
        // OK: should match okay with empty fields
        let f1 = Type::group_type_builder("f").build().unwrap();
        let f2 = Type::group_type_builder("f").build().unwrap();
        assert!(f1.check_contains(&f2));

        // OK: fields match
        let f1 = test_new_group_type(
            "f",
            Repetition::REPEATED,
            vec![
                Type::primitive_type_builder("f1", PhysicalType::INT32)
                    .build()
                    .unwrap(),
                Type::primitive_type_builder("f2", PhysicalType::INT64)
                    .build()
                    .unwrap(),
            ],
        );
        let f2 = test_new_group_type(
            "f",
            Repetition::REPEATED,
            vec![
                Type::primitive_type_builder("f1", PhysicalType::INT32)
                    .build()
                    .unwrap(),
                Type::primitive_type_builder("f2", PhysicalType::INT64)
                    .build()
                    .unwrap(),
            ],
        );
        assert!(f1.check_contains(&f2));

        // OK: subset of fields
        let f1 = test_new_group_type(
            "f",
            Repetition::REPEATED,
            vec![
                Type::primitive_type_builder("f1", PhysicalType::INT32)
                    .build()
                    .unwrap(),
                Type::primitive_type_builder("f2", PhysicalType::INT64)
                    .build()
                    .unwrap(),
            ],
        );
        let f2 = test_new_group_type(
            "f",
            Repetition::REPEATED,
            vec![Type::primitive_type_builder("f2", PhysicalType::INT64)
                .build()
                .unwrap()],
        );
        assert!(f1.check_contains(&f2));

        // KO: different name
        let f1 = Type::group_type_builder("f1").build().unwrap();
        let f2 = Type::group_type_builder("f2").build().unwrap();
        assert!(!f1.check_contains(&f2));

        // KO: different repetition
        let f1 = Type::group_type_builder("f")
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap();
        let f2 = Type::group_type_builder("f")
            .with_repetition(Repetition::REPEATED)
            .build()
            .unwrap();
        assert!(!f1.check_contains(&f2));

        // KO: different fields
        let f1 = test_new_group_type(
            "f",
            Repetition::REPEATED,
            vec![
                Type::primitive_type_builder("f1", PhysicalType::INT32)
                    .build()
                    .unwrap(),
                Type::primitive_type_builder("f2", PhysicalType::INT64)
                    .build()
                    .unwrap(),
            ],
        );
        let f2 = test_new_group_type(
            "f",
            Repetition::REPEATED,
            vec![
                Type::primitive_type_builder("f1", PhysicalType::INT32)
                    .build()
                    .unwrap(),
                Type::primitive_type_builder("f2", PhysicalType::BOOLEAN)
                    .build()
                    .unwrap(),
            ],
        );
        assert!(!f1.check_contains(&f2));

        // KO: different fields
        let f1 = test_new_group_type(
            "f",
            Repetition::REPEATED,
            vec![
                Type::primitive_type_builder("f1", PhysicalType::INT32)
                    .build()
                    .unwrap(),
                Type::primitive_type_builder("f2", PhysicalType::INT64)
                    .build()
                    .unwrap(),
            ],
        );
        let f2 = test_new_group_type(
            "f",
            Repetition::REPEATED,
            vec![Type::primitive_type_builder("f3", PhysicalType::INT32)
                .build()
                .unwrap()],
        );
        assert!(!f1.check_contains(&f2));
    }

    #[test]
    fn test_check_contains_group_primitive() {
        // KO: should not match
        let f1 = Type::group_type_builder("f").build().unwrap();
        let f2 = Type::primitive_type_builder("f", PhysicalType::INT64)
            .build()
            .unwrap();
        assert!(!f1.check_contains(&f2));
        assert!(!f2.check_contains(&f1));

        // KO: should not match when primitive field is part of group type
        let f1 = test_new_group_type(
            "f",
            Repetition::REPEATED,
            vec![Type::primitive_type_builder("f1", PhysicalType::INT32)
                .build()
                .unwrap()],
        );
        let f2 = Type::primitive_type_builder("f1", PhysicalType::INT32)
            .build()
            .unwrap();
        assert!(!f1.check_contains(&f2));
        assert!(!f2.check_contains(&f1));

        // OK: match nested types
        let f1 = test_new_group_type(
            "a",
            Repetition::REPEATED,
            vec![
                test_new_group_type(
                    "b",
                    Repetition::REPEATED,
                    vec![Type::primitive_type_builder("c", PhysicalType::INT32)
                        .build()
                        .unwrap()],
                ),
                Type::primitive_type_builder("d", PhysicalType::INT64)
                    .build()
                    .unwrap(),
                Type::primitive_type_builder("e", PhysicalType::BOOLEAN)
                    .build()
                    .unwrap(),
            ],
        );
        let f2 = test_new_group_type(
            "a",
            Repetition::REPEATED,
            vec![test_new_group_type(
                "b",
                Repetition::REPEATED,
                vec![Type::primitive_type_builder("c", PhysicalType::INT32)
                    .build()
                    .unwrap()],
            )],
        );
        assert!(f1.check_contains(&f2)); // should match
        assert!(!f2.check_contains(&f1)); // should fail
    }

    #[test]
    fn test_schema_type_thrift_conversion_err() {
        let schema = Type::primitive_type_builder("col", PhysicalType::INT32)
            .build()
            .unwrap();
        let thrift_schema = to_thrift(&schema);
        assert!(thrift_schema.is_err());
        if let Err(e) = thrift_schema {
            assert_eq!(
                format!("{}", e),
                "Parquet error: Root schema must be Group type"
            );
        }
    }

    #[test]
    fn test_schema_type_thrift_conversion() {
        let message_type = "
    message conversions {
      REQUIRED INT64 id;
      OPTIONAL group int_array_Array (LIST) {
        REPEATED group list {
          OPTIONAL group element (LIST) {
            REPEATED group list {
              OPTIONAL INT32 element;
            }
          }
        }
      }
      OPTIONAL group int_map (MAP) {
        REPEATED group map (MAP_KEY_VALUE) {
          REQUIRED BYTE_ARRAY key (UTF8);
          OPTIONAL INT32 value;
        }
      }
      OPTIONAL group int_Map_Array (LIST) {
        REPEATED group list {
          OPTIONAL group g (MAP) {
            REPEATED group map (MAP_KEY_VALUE) {
              REQUIRED BYTE_ARRAY key (UTF8);
              OPTIONAL group value {
                OPTIONAL group H {
                  OPTIONAL group i (LIST) {
                    REPEATED group list {
                      OPTIONAL DOUBLE element;
                    }
                  }
                }
              }
            }
          }
        }
      }
      OPTIONAL group nested_struct {
        OPTIONAL INT32 A;
        OPTIONAL group b (LIST) {
          REPEATED group list {
            REQUIRED FIXED_LEN_BYTE_ARRAY (16) element;
          }
        }
      }
    }
    ";
        let expected_schema = parse_message_type(message_type).unwrap();
        let thrift_schema = to_thrift(&expected_schema).unwrap();
        let result_schema = from_thrift(&thrift_schema).unwrap();
        assert_eq!(result_schema, Arc::new(expected_schema));
    }

    #[test]
    fn test_schema_type_thrift_conversion_decimal() {
        let message_type = "
    message decimals {
      OPTIONAL INT32 field0;
      OPTIONAL INT64 field1 (DECIMAL (18, 2));
      OPTIONAL FIXED_LEN_BYTE_ARRAY (16) field2 (DECIMAL (38, 18));
      OPTIONAL BYTE_ARRAY field3 (DECIMAL (9));
    }
    ";
        let expected_schema = parse_message_type(message_type).unwrap();
        let thrift_schema = to_thrift(&expected_schema).unwrap();
        let result_schema = from_thrift(&thrift_schema).unwrap();
        assert_eq!(result_schema, Arc::new(expected_schema));
    }

    // Tests schema conversion from thrift, when num_children is set to Some(0) for a
    // primitive type.
    #[test]
    fn test_schema_from_thrift_with_num_children_set() {
        // schema definition written by parquet-cpp version 1.3.2-SNAPSHOT
        let message_type = "
    message schema {
      OPTIONAL BYTE_ARRAY id (UTF8);
      OPTIONAL BYTE_ARRAY name (UTF8);
      OPTIONAL BYTE_ARRAY message (UTF8);
      OPTIONAL INT32 type (UINT_8);
      OPTIONAL INT64 author_time (TIMESTAMP_MILLIS);
      OPTIONAL INT64 __index_level_0__;
    }
    ";

        let expected_schema = parse_message_type(message_type).unwrap();
        let mut thrift_schema = to_thrift(&expected_schema).unwrap();
        // Change all of None to Some(0)
        for mut elem in &mut thrift_schema[..] {
            if elem.num_children == None {
                elem.num_children = Some(0);
            }
        }

        let result_schema = from_thrift(&thrift_schema).unwrap();
        assert_eq!(result_schema, Arc::new(expected_schema));
    }

    // Sometimes parquet-cpp sets repetition level for the root node, which is against
    // the format definition, but we need to handle it by setting it back to None.
    #[test]
    fn test_schema_from_thrift_root_has_repetition() {
        // schema definition written by parquet-cpp version 1.3.2-SNAPSHOT
        let message_type = "
    message schema {
      OPTIONAL BYTE_ARRAY a (UTF8);
      OPTIONAL INT32 b (UINT_8);
    }
    ";

        let expected_schema = parse_message_type(message_type).unwrap();
        let mut thrift_schema = to_thrift(&expected_schema).unwrap();
        thrift_schema[0].repetition_type = Some(Repetition::REQUIRED.into());

        let result_schema = from_thrift(&thrift_schema).unwrap();
        assert_eq!(result_schema, Arc::new(expected_schema));
    }
}
