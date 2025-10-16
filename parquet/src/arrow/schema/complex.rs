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

use std::collections::HashMap;
use std::sync::Arc;

use crate::arrow::schema::extension::try_add_extension_type;
use crate::arrow::schema::primitive::convert_primitive;
use crate::arrow::{PARQUET_FIELD_ID_META_KEY, ProjectionMask};
use crate::basic::{ConvertedType, Repetition};
use crate::errors::ParquetError;
use crate::errors::Result;
use crate::schema::types::{SchemaDescriptor, Type, TypePtr};
use arrow_schema::{DataType, Field, Fields, SchemaBuilder};

fn get_repetition(t: &Type) -> Repetition {
    let info = t.get_basic_info();
    match info.has_repetition() {
        true => info.repetition(),
        false => Repetition::REQUIRED,
    }
}

/// Representation of a parquet schema element, in terms of arrow schema elements
#[derive(Debug, Clone)]
pub struct ParquetField {
    /// The level which represents an insertion into the current list
    /// i.e. guaranteed to be > 0 for an element of list type
    pub rep_level: i16,
    /// The level at which this field is fully defined,
    /// i.e. guaranteed to be > 0 for a nullable type or child of a
    /// nullable type
    pub def_level: i16,
    /// Whether this field is nullable
    pub nullable: bool,
    /// The arrow type of the column data
    ///
    /// Note: In certain cases the data stored in parquet may have been coerced
    /// to a different type and will require conversion on read (e.g. Date64 and Interval)
    pub arrow_type: DataType,
    /// The type of this field
    pub field_type: ParquetFieldType,
}

impl ParquetField {
    /// Converts `self` into an arrow list, with its current type as the field type
    ///
    /// This is used to convert repeated columns, into their arrow representation
    fn into_list(self, name: &str) -> Self {
        ParquetField {
            rep_level: self.rep_level,
            def_level: self.def_level,
            nullable: false,
            arrow_type: DataType::List(Arc::new(Field::new(name, self.arrow_type.clone(), false))),
            field_type: ParquetFieldType::Group {
                children: vec![self],
            },
        }
    }

    /// Converts `self` into an arrow list, with its current type as the field type
    /// accept an optional `list_data_type` to specify the type of list to create
    ///
    /// This is used to convert [deprecated repeated columns] (not in a list), into their arrow representation
    ///
    /// [deprecated repeated columns]: https://github.com/apache/parquet-format/blob/9fd57b59e0ce1a82a69237dcf8977d3e72a2965d/LogicalTypes.md?plain=1#L649-L650
    fn into_list_with_arrow_list_hint(
        self,
        parquet_field_type: &Type,
        list_data_type: Option<DataType>,
    ) -> Result<Self, ParquetError> {
        let arrow_field = match &list_data_type {
            Some(DataType::List(field_hint))
            | Some(DataType::LargeList(field_hint))
            | Some(DataType::FixedSizeList(field_hint, _)) => Some(field_hint.as_ref()),
            Some(_) => return Err(general_err!(
                "Internal error: should be validated earlier that list_data_type is only a type of list"
            )),
            None => None,
        };

        let arrow_field = convert_field(
            parquet_field_type,
            &self,
            arrow_field,
            // Only add the field id to the list and not to the element
            false,
        )?
        .with_nullable(false);

        Ok(ParquetField {
            rep_level: self.rep_level,
            def_level: self.def_level,
            nullable: false,
            arrow_type: match list_data_type {
                Some(DataType::List(_)) => DataType::List(Arc::new(arrow_field)),
                Some(DataType::LargeList(_)) => DataType::LargeList(Arc::new(arrow_field)),
                Some(DataType::FixedSizeList(_, len)) => {
                    DataType::FixedSizeList(Arc::new(arrow_field), len)
                }
                _ => DataType::List(Arc::new(arrow_field)),
            },
            field_type: ParquetFieldType::Group {
                children: vec![self],
            },
        })
    }

    /// Returns a list of [`ParquetField`] children if this is a group type
    pub fn children(&self) -> Option<&[Self]> {
        match &self.field_type {
            ParquetFieldType::Primitive { .. } => None,
            ParquetFieldType::Group { children } => Some(children),
        }
    }
}

#[derive(Debug, Clone)]
pub enum ParquetFieldType {
    Primitive {
        /// The index of the column in parquet
        col_idx: usize,
        /// The type of the column in parquet
        primitive_type: TypePtr,
    },
    Group {
        children: Vec<ParquetField>,
    },
}

/// Encodes the context of the parent of the field currently under consideration
struct VisitorContext {
    rep_level: i16,
    def_level: i16,
    /// An optional [`DataType`] sourced from the embedded arrow schema
    data_type: Option<DataType>,

    /// Whether to treat repeated types as list from arrow types
    /// when true, if data_type provided it should be DataType::List() (or other list type)
    /// and the list field data type would be treated as the hint for the parquet type
    ///
    /// when false, if data_type provided it will be treated as the hint without unwrapping
    treat_repeated_as_list_arrow_hint: bool,
}

impl VisitorContext {
    /// Compute the resulting definition level, repetition level and nullability
    /// for a child field with the given [`Repetition`]
    fn levels(&self, repetition: Repetition) -> (i16, i16, bool) {
        match repetition {
            Repetition::OPTIONAL => (self.def_level + 1, self.rep_level, true),
            Repetition::REQUIRED => (self.def_level, self.rep_level, false),
            Repetition::REPEATED => (self.def_level + 1, self.rep_level + 1, false),
        }
    }
}

/// Walks the parquet schema in a depth-first fashion in order to map it to arrow data structures
///
/// See [Logical Types] for more information on the conversion algorithm
///
/// [Logical Types]: https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
struct Visitor {
    /// The column index of the next leaf column
    next_col_idx: usize,

    /// Mask of columns to include
    mask: ProjectionMask,
}

impl Visitor {
    fn visit_primitive(
        &mut self,
        primitive_type: &TypePtr,
        context: VisitorContext,
    ) -> Result<Option<ParquetField>> {
        let col_idx = self.next_col_idx;
        self.next_col_idx += 1;

        if !self.mask.leaf_included(col_idx) {
            return Ok(None);
        }

        let repetition = get_repetition(primitive_type);
        let (def_level, rep_level, nullable) = context.levels(repetition);

        let primitive_arrow_data_type = match repetition {
            Repetition::REPEATED if context.treat_repeated_as_list_arrow_hint => {
                let arrow_field = match &context.data_type {
                    Some(DataType::List(f)) => Some(f.as_ref()),
                    Some(DataType::LargeList(f)) => Some(f.as_ref()),
                    Some(DataType::FixedSizeList(f, _)) => Some(f.as_ref()),
                    Some(d) => return Err(arrow_err!(
                        "incompatible arrow schema, expected list got {} for repeated primitive field",
                        d
                    )),
                    None => None,
                };

                arrow_field.map(|f| f.data_type().clone())
            }
            _ => context.data_type.clone(),
        };

        let arrow_type = convert_primitive(primitive_type, primitive_arrow_data_type)?;

        let primitive_field = ParquetField {
            rep_level,
            def_level,
            nullable,
            arrow_type,
            field_type: ParquetFieldType::Primitive {
                primitive_type: primitive_type.clone(),
                col_idx,
            },
        };

        Ok(Some(match repetition {
            Repetition::REPEATED if context.treat_repeated_as_list_arrow_hint => {
                primitive_field.into_list_with_arrow_list_hint(primitive_type, context.data_type)?
            }
            Repetition::REPEATED => primitive_field.into_list(primitive_type.name()),
            _ => primitive_field,
        }))
    }

    fn visit_struct(
        &mut self,
        struct_type: &TypePtr,
        context: VisitorContext,
    ) -> Result<Option<ParquetField>> {
        // The root type will not have a repetition level
        let repetition = get_repetition(struct_type);
        let (def_level, rep_level, nullable) = context.levels(repetition);

        let parquet_fields = struct_type.get_fields();

        // Extract any arrow fields from the hints
        let arrow_struct = match repetition {
            Repetition::REPEATED if context.treat_repeated_as_list_arrow_hint => {
                let arrow_field = match &context.data_type {
                    Some(DataType::List(f)) => Some(f.as_ref()),
                    Some(DataType::LargeList(f)) => Some(f.as_ref()),
                    Some(DataType::FixedSizeList(f, _)) => Some(f.as_ref()),
                    Some(d) => {
                        return Err(arrow_err!(
                        "incompatible arrow schema, expected list got {} for repeated struct field",
                        d
                    ))
                    }
                    None => None,
                };

                arrow_field.map(|f| f.data_type())
            }
            _ => context.data_type.as_ref(),
        };

        let arrow_fields = match &arrow_struct {
            Some(DataType::Struct(fields)) => {
                if fields.len() != parquet_fields.len() {
                    return Err(arrow_err!(
                        "incompatible arrow schema, expected {} struct fields got {}",
                        parquet_fields.len(),
                        fields.len()
                    ));
                }
                Some(fields)
            }
            Some(d) => {
                return Err(arrow_err!(
                    "incompatible arrow schema, expected struct got {}",
                    d
                ));
            }
            None => None,
        };

        let mut child_fields = SchemaBuilder::with_capacity(parquet_fields.len());
        let mut children = Vec::with_capacity(parquet_fields.len());

        // Perform a DFS of children
        for (idx, parquet_field) in parquet_fields.iter().enumerate() {
            let data_type = match arrow_fields {
                Some(fields) => {
                    let field = &fields[idx];
                    if field.name() != parquet_field.name() {
                        return Err(arrow_err!(
                            "incompatible arrow schema, expected field named {} got {}",
                            parquet_field.name(),
                            field.name()
                        ));
                    }
                    Some(field.data_type().clone())
                }
                None => None,
            };

            let arrow_field = arrow_fields.map(|x| &*x[idx]);
            let child_ctx = VisitorContext {
                rep_level,
                def_level,
                data_type,
                treat_repeated_as_list_arrow_hint: true,
            };

            if let Some(child) = self.dispatch(parquet_field, child_ctx)? {
                // The child type returned may be different from what is encoded in the arrow
                // schema in the event of a mismatch or a projection
                child_fields.push(convert_field(parquet_field, &child, arrow_field, true)?);
                children.push(child);
            }
        }

        if children.is_empty() {
            return Ok(None);
        }

        let struct_field = ParquetField {
            rep_level,
            def_level,
            nullable,
            arrow_type: DataType::Struct(child_fields.finish().fields),
            field_type: ParquetFieldType::Group { children },
        };

        Ok(Some(match repetition {
            Repetition::REPEATED if context.treat_repeated_as_list_arrow_hint => {
                struct_field.into_list_with_arrow_list_hint(struct_type, context.data_type)?
            }
            Repetition::REPEATED => struct_field.into_list(struct_type.name()),
            _ => struct_field,
        }))
    }

    fn visit_map(
        &mut self,
        map_type: &TypePtr,
        context: VisitorContext,
    ) -> Result<Option<ParquetField>> {
        let rep_level = context.rep_level + 1;
        let (def_level, nullable) = match get_repetition(map_type) {
            Repetition::REQUIRED => (context.def_level + 1, false),
            Repetition::OPTIONAL => (context.def_level + 2, true),
            Repetition::REPEATED => return Err(arrow_err!("Map cannot be repeated")),
        };

        if map_type.get_fields().len() != 1 {
            return Err(arrow_err!(
                "Map field must have exactly one key_value child, found {}",
                map_type.get_fields().len()
            ));
        }

        // Add map entry (key_value) to context
        let map_key_value = &map_type.get_fields()[0];
        if map_key_value.get_basic_info().repetition() != Repetition::REPEATED {
            return Err(arrow_err!("Child of map field must be repeated"));
        }

        // According to the specification the values are optional (#1642).
        // In this case, return the keys as a list.
        if map_key_value.get_fields().len() == 1 {
            return self.visit_list(map_type, context);
        }

        if map_key_value.get_fields().len() != 2 {
            return Err(arrow_err!(
                "Child of map field must have two children, found {}",
                map_key_value.get_fields().len()
            ));
        }

        // Get key and value, and create context for each
        let map_key = &map_key_value.get_fields()[0];
        let map_value = &map_key_value.get_fields()[1];

        match map_key.get_basic_info().repetition() {
            Repetition::REPEATED => {
                return Err(arrow_err!("Map keys cannot be repeated"));
            }
            Repetition::REQUIRED | Repetition::OPTIONAL => {
                // Relaxed check for having repetition REQUIRED as there exists
                // parquet writers and files that do not conform to this standard.
                // This allows us to consume a broader range of existing files even
                // if they are out of spec.
            }
        }

        if map_value.get_basic_info().repetition() == Repetition::REPEATED {
            return Err(arrow_err!("Map values cannot be repeated"));
        }

        // Extract the arrow fields
        let (arrow_map, arrow_key, arrow_value, sorted) = match &context.data_type {
            Some(DataType::Map(field, sorted)) => match field.data_type() {
                DataType::Struct(fields) => {
                    if fields.len() != 2 {
                        return Err(arrow_err!(
                            "Map data type should contain struct with two children, got {}",
                            fields.len()
                        ));
                    }

                    (Some(field), Some(&*fields[0]), Some(&*fields[1]), *sorted)
                }
                d => {
                    return Err(arrow_err!("Map data type should contain struct got {}", d));
                }
            },
            Some(d) => {
                return Err(arrow_err!(
                    "incompatible arrow schema, expected map got {}",
                    d
                ));
            }
            None => (None, None, None, false),
        };

        let maybe_key = {
            let context = VisitorContext {
                rep_level,
                def_level,
                data_type: arrow_key.map(|x| x.data_type().clone()),
                // Key is not repeated
                treat_repeated_as_list_arrow_hint: false,
            };

            self.dispatch(map_key, context)?
        };

        let maybe_value = {
            let context = VisitorContext {
                rep_level,
                def_level,
                data_type: arrow_value.map(|x| x.data_type().clone()),
                // Value type can be repeated
                treat_repeated_as_list_arrow_hint: true,
            };

            self.dispatch(map_value, context)?
        };

        // Need both columns to be projected
        match (maybe_key, maybe_value) {
            (Some(key), Some(value)) => {
                let key_field = Arc::new(
                    convert_field(map_key, &key, arrow_key, true)?
                        // The key is always non-nullable (#5630)
                        .with_nullable(false),
                );
                let value_field = Arc::new(convert_field(map_value, &value, arrow_value, true)?);
                let field_metadata = match arrow_map {
                    Some(field) => field.metadata().clone(),
                    _ => HashMap::default(),
                };

                let map_field = Field::new_struct(
                    map_key_value.name(),
                    [key_field, value_field],
                    false, // The inner map field is always non-nullable (#1697)
                )
                .with_metadata(field_metadata);

                Ok(Some(ParquetField {
                    rep_level,
                    def_level,
                    nullable,
                    arrow_type: DataType::Map(Arc::new(map_field), sorted),
                    field_type: ParquetFieldType::Group {
                        children: vec![key, value],
                    },
                }))
            }
            _ => Ok(None),
        }
    }

    fn visit_list(
        &mut self,
        list_type: &TypePtr,
        context: VisitorContext,
    ) -> Result<Option<ParquetField>> {
        if list_type.is_primitive() {
            return Err(arrow_err!(
                "{:?} is a list type and can't be processed as primitive.",
                list_type
            ));
        }

        let fields = list_type.get_fields();
        if fields.len() != 1 {
            return Err(arrow_err!(
                "list type must have a single child, found {}",
                fields.len()
            ));
        }

        let repeated_field = &fields[0];
        if get_repetition(repeated_field) != Repetition::REPEATED {
            return Err(arrow_err!("List child must be repeated"));
        }

        // If the list is nullable
        let (def_level, nullable) = match list_type.get_basic_info().repetition() {
            Repetition::REQUIRED => (context.def_level, false),
            Repetition::OPTIONAL => (context.def_level + 1, true),
            Repetition::REPEATED => return Err(arrow_err!("List type cannot be repeated")),
        };

        let arrow_field = match &context.data_type {
            Some(DataType::List(f)) => Some(f.as_ref()),
            Some(DataType::LargeList(f)) => Some(f.as_ref()),
            Some(DataType::FixedSizeList(f, _)) => Some(f.as_ref()),
            Some(d) => {
                return Err(arrow_err!(
                    "incompatible arrow schema, expected list got {}",
                    d
                ));
            }
            None => None,
        };

        if repeated_field.is_primitive() {
            // If the repeated field is not a group, then its type is the element type and elements are required.
            //
            // required/optional group my_list (LIST) {
            //   repeated int32 element;
            // }
            //
            let context = VisitorContext {
                rep_level: context.rep_level,
                def_level,
                data_type: arrow_field.map(|f| f.data_type().clone()),
                treat_repeated_as_list_arrow_hint: false,
            };

            return match self.visit_primitive(repeated_field, context) {
                Ok(Some(mut field)) => {
                    // visit_primitive will infer a non-nullable list, update if necessary
                    field.nullable = nullable;
                    Ok(Some(field))
                }
                r => r,
            };
        }

        // test to see if the repeated field is a struct or one-tuple
        let items = repeated_field.get_fields();
        if items.len() != 1
            || (!repeated_field.is_list()
                && !repeated_field.has_single_repeated_child()
                && (repeated_field.name() == "array"
                    || repeated_field.name() == format!("{}_tuple", list_type.name())))
        {
            // If the repeated field is a group with multiple fields, then its type is the element
            // type and elements are required.
            //
            // If the repeated field is a group with one field and is named either array or uses
            // the LIST-annotated group's name with _tuple appended then the repeated type is the
            // element type and elements are required. But this rule only applies if the
            // repeated field is not annotated, and the single child field is not `repeated`.
            let context = VisitorContext {
                rep_level: context.rep_level,
                def_level,
                data_type: arrow_field.map(|f| f.data_type().clone()),
                treat_repeated_as_list_arrow_hint: false,
            };

            return match self.visit_struct(repeated_field, context) {
                Ok(Some(mut field)) => {
                    field.nullable = nullable;
                    Ok(Some(field))
                }
                r => r,
            };
        }

        // Regular list handling logic
        let item_type = &items[0];
        let rep_level = context.rep_level + 1;
        let def_level = def_level + 1;

        let new_context = VisitorContext {
            def_level,
            rep_level,
            data_type: arrow_field.map(|f| f.data_type().clone()),
            treat_repeated_as_list_arrow_hint: true,
        };

        match self.dispatch(item_type, new_context) {
            Ok(Some(item)) => {
                let item_field = Arc::new(convert_field(item_type, &item, arrow_field, true)?);

                // Use arrow type as hint for index size
                let arrow_type = match context.data_type {
                    Some(DataType::LargeList(_)) => DataType::LargeList(item_field),
                    Some(DataType::FixedSizeList(_, len)) => {
                        DataType::FixedSizeList(item_field, len)
                    }
                    _ => DataType::List(item_field),
                };

                Ok(Some(ParquetField {
                    rep_level,
                    def_level,
                    nullable,
                    arrow_type,
                    field_type: ParquetFieldType::Group {
                        children: vec![item],
                    },
                }))
            }
            r => r,
        }
    }

    fn dispatch(
        &mut self,
        cur_type: &TypePtr,
        context: VisitorContext,
    ) -> Result<Option<ParquetField>> {
        if cur_type.is_primitive() {
            self.visit_primitive(cur_type, context)
        } else {
            match cur_type.get_basic_info().converted_type() {
                ConvertedType::LIST => self.visit_list(cur_type, context),
                ConvertedType::MAP | ConvertedType::MAP_KEY_VALUE => {
                    self.visit_map(cur_type, context)
                }
                _ => self.visit_struct(cur_type, context),
            }
        }
    }
}

/// Computes the Arrow [`Field`] for a child column
///
/// The resulting Arrow [`Field`] will have the type dictated by the Parquet `field`, a name
/// dictated by the `parquet_type`, and any metadata from `arrow_hint`
fn convert_field(
    parquet_type: &Type,
    field: &ParquetField,
    arrow_hint: Option<&Field>,
    add_field_id: bool,
) -> Result<Field, ParquetError> {
    let name = parquet_type.name();
    let data_type = field.arrow_type.clone();
    let nullable = field.nullable;

    match arrow_hint {
        Some(hint) => {
            // If the inferred type is a dictionary, preserve dictionary metadata
            #[allow(deprecated)]
            let field = match (&data_type, hint.dict_id(), hint.dict_is_ordered()) {
                (DataType::Dictionary(_, _), Some(id), Some(ordered)) =>
                {
                    #[allow(deprecated)]
                    Field::new_dict(name, data_type, nullable, id, ordered)
                }
                _ => Field::new(name, data_type, nullable),
            };

            Ok(field.with_metadata(hint.metadata().clone()))
        }
        None => {
            let mut ret = Field::new(name, data_type, nullable);
            let basic_info = parquet_type.get_basic_info();
            if add_field_id && basic_info.has_id() {
                let mut meta = HashMap::with_capacity(1);
                meta.insert(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    basic_info.id().to_string(),
                );
                ret.set_metadata(meta);
            }
            try_add_extension_type(ret, parquet_type)
        }
    }
}

/// Computes the [`ParquetField`] for the provided [`SchemaDescriptor`] with `leaf_columns` listing
/// the indexes of leaf columns to project, and `embedded_arrow_schema` the optional
/// [`Fields`] embedded in the parquet metadata
///
/// Note: This does not support out of order column projection
pub fn convert_schema(
    schema: &SchemaDescriptor,
    mask: ProjectionMask,
    embedded_arrow_schema: Option<&Fields>,
) -> Result<Option<ParquetField>> {
    let mut visitor = Visitor {
        next_col_idx: 0,
        mask,
    };

    let context = VisitorContext {
        rep_level: 0,
        def_level: 0,
        data_type: embedded_arrow_schema.map(|fields| DataType::Struct(fields.clone())),
        treat_repeated_as_list_arrow_hint: true,
    };

    visitor.dispatch(&schema.root_schema_ptr(), context)
}

/// Computes the [`ParquetField`] for the provided `parquet_type`
pub fn convert_type(parquet_type: &TypePtr) -> Result<ParquetField> {
    let mut visitor = Visitor {
        next_col_idx: 0,
        mask: ProjectionMask::all(),
    };

    let context = VisitorContext {
        rep_level: 0,
        def_level: 0,
        data_type: None,
        // We might be inside list
        treat_repeated_as_list_arrow_hint: false,
    };

    Ok(visitor.dispatch(parquet_type, context)?.unwrap())
}

#[cfg(test)]
mod tests {
    use crate::arrow::schema::complex::convert_schema;
    use crate::arrow::{ProjectionMask, PARQUET_FIELD_ID_META_KEY};
    use crate::schema::parser::parse_message_type;
    use crate::schema::types::SchemaDescriptor;
    use arrow_schema::{DataType, Field, Fields};
    use std::sync::Arc;

    trait WithFieldId {
        fn with_field_id(self, id: i32) -> Self;
    }
    impl WithFieldId for arrow_schema::Field {
        fn with_field_id(self, id: i32) -> Self {
            let mut metadata = self.metadata().clone();
            metadata.insert(PARQUET_FIELD_ID_META_KEY.to_string(), id.to_string());
            self.with_metadata(metadata)
        }
    }

    fn test_roundtrip(message_type: &str) -> crate::errors::Result<()> {
        let parsed_input_schema = Arc::new(parse_message_type(message_type)?);
        let schema = SchemaDescriptor::new(parsed_input_schema);

        let converted = convert_schema(&schema, ProjectionMask::all(), None)?.unwrap();

        let DataType::Struct(schema_fields) = &converted.arrow_type else {
            panic!("Expected struct from convert_schema");
        };

        // Should be able to convert the same thing
        let converted_again =
            convert_schema(&schema, ProjectionMask::all(), Some(schema_fields))?.unwrap();

        // Assert that we changed to Utf8
        assert_eq!(converted_again.arrow_type, converted.arrow_type);

        Ok(())
    }

    fn test_expected_type(
        message_type: &str,
        expected_fields: Fields,
    ) -> crate::errors::Result<()> {
        test_roundtrip(message_type)?;

        let parsed_input_schema = Arc::new(parse_message_type(message_type)?);
        let schema = SchemaDescriptor::new(parsed_input_schema);

        let converted = convert_schema(&schema, ProjectionMask::all(), None)?.unwrap();

        let DataType::Struct(schema_fields) = &converted.arrow_type else {
            panic!("Expected struct from convert_schema");
        };

        assert_eq!(schema_fields, &expected_fields);

        Ok(())
    }

    /// Taken from the example in [Parquet Format - Nested Types - Lists - Backward-compatibility rules](https://github.com/apache/parquet-format/blob/9fd57b59e0ce1a82a69237dcf8977d3e72a2965d/LogicalTypes.md?plain=1#L766-L769)
    #[test]
    fn basic_backward_compatible_list_1() -> crate::errors::Result<()> {
        test_expected_type(
            "
            message schema {
                optional group my_list (LIST) {
                  repeated int32 element;
                }
            }
        ",
            Fields::from(vec![
                // Rule 1: List<Integer> (nullable list, non-null elements)
                Field::new(
                    "my_list",
                    DataType::List(Arc::new(Field::new("element", DataType::Int32, false))),
                    true,
                ),
            ]),
        )
    }

    /// Taken from the example in [Parquet Format - Nested Types - Lists - Backward-compatibility rules](https://github.com/apache/parquet-format/blob/9fd57b59e0ce1a82a69237dcf8977d3e72a2965d/LogicalTypes.md?plain=1#L771-L777)
    #[test]
    fn basic_backward_compatible_list_2() -> crate::errors::Result<()> {
        test_expected_type(
            "
            message schema {
              optional group my_list (LIST) {
                  repeated group element {
                    required binary str (STRING);
                    required int32 num;
                  }
              }
            }
        ",
            Fields::from(vec![
                // Rule 2: List<Tuple<String, Integer>> (nullable list, non-null elements)
                Field::new(
                    "my_list",
                    DataType::List(Arc::new(Field::new(
                        "element",
                        DataType::Struct(Fields::from(vec![
                            Field::new("str", DataType::Utf8, false),
                            Field::new("num", DataType::Int32, false),
                        ])),
                        false,
                    ))),
                    true,
                ),
            ]),
        )
    }

    /// Taken from the example in [Parquet Format - Nested Types - Lists - Backward-compatibility rules](https://github.com/apache/parquet-format/blob/9fd57b59e0ce1a82a69237dcf8977d3e72a2965d/LogicalTypes.md?plain=1#L779-L784)
    #[test]
    fn basic_backward_compatible_list_3() -> crate::errors::Result<()> {
        test_expected_type(
            "
            message schema {
              optional group my_list (LIST) {
                  repeated group array (LIST) {
                    repeated int32 array;
                  }
              }
            }
        ",
            Fields::from(vec![
                // Rule 3: List<List<Integer>> (nullable outer list, non-null elements)
                Field::new(
                    "my_list",
                    DataType::List(Arc::new(Field::new(
                        "array",
                        DataType::List(Arc::new(Field::new("array", DataType::Int32, false))),
                        false,
                    ))),
                    true,
                ),
            ]),
        )
    }

    /// Taken from the example in [Parquet Format - Nested Types - Lists - Backward-compatibility rules](https://github.com/apache/parquet-format/blob/9fd57b59e0ce1a82a69237dcf8977d3e72a2965d/LogicalTypes.md?plain=1#L786-L791)
    #[test]
    fn basic_backward_compatible_list_4_1() -> crate::errors::Result<()> {
        test_expected_type(
            "
            message schema {
              optional group my_list (LIST) {
                  repeated group array {
                    required binary str (STRING);
                  }
              }
            }
        ",
            Fields::from(vec![
                // Rule 4: List<OneTuple<String>> (nullable list, non-null elements)
                Field::new(
                    "my_list",
                    DataType::List(Arc::new(Field::new(
                        "array",
                        DataType::Struct(Fields::from(vec![Field::new(
                            "str",
                            DataType::Utf8,
                            false,
                        )])),
                        false,
                    ))),
                    true,
                ),
            ]),
        )
    }

    /// Taken from the example in [Parquet Format - Nested Types - Lists - Backward-compatibility rules](https://github.com/apache/parquet-format/blob/9fd57b59e0ce1a82a69237dcf8977d3e72a2965d/LogicalTypes.md?plain=1#L793-L798)
    #[test]
    fn basic_backward_compatible_list_4_2() -> crate::errors::Result<()> {
        test_expected_type(
            "
            message schema {
                optional group my_list (LIST) {
                    repeated group my_list_tuple {
                        required binary str (STRING);
                    }
                }
            }
        ",
            Fields::from(vec![
                // Rule 4: List<OneTuple<String>> (nullable list, non-null elements)
                Field::new(
                    "my_list",
                    DataType::List(Arc::new(Field::new(
                        "my_list_tuple",
                        DataType::Struct(Fields::from(vec![Field::new(
                            "str",
                            DataType::Utf8,
                            false,
                        )])),
                        false,
                    ))),
                    true,
                ),
            ]),
        )
    }

    /// Taken from the example in [Parquet Format - Nested Types - Lists - Backward-compatibility rules](https://github.com/apache/parquet-format/blob/9fd57b59e0ce1a82a69237dcf8977d3e72a2965d/LogicalTypes.md?plain=1#L800-L805)
    #[test]
    fn basic_backward_compatible_list_5() -> crate::errors::Result<()> {
        test_expected_type(
            "
            message schema {
                optional group my_list (LIST) {
                    repeated group element {
                        optional binary str (STRING);
                    }
                }
            }
        ",
            Fields::from(vec![
                // Rule 5: List<String>  (nullable list, nullable elements)
                Field::new(
                    "my_list",
                    DataType::List(Arc::new(Field::new("str", DataType::Utf8, true))),
                    true,
                ),
            ]),
        )
    }

    #[test]
    fn basic_backward_compatible_map_1() -> crate::errors::Result<()> {
        test_expected_type(
            "
            message schema {
                optional group my_map (MAP) {
                  repeated group map {
                    required binary str (STRING);
                    required int32 num;
                  }
                }
            }
        ",
            Fields::from(vec![
                // Map<String, Integer> (nullable map, non-null values)
                Field::new(
                    "my_map",
                    DataType::Map(
                        Arc::new(Field::new(
                            "map",
                            DataType::Struct(Fields::from(vec![
                                Field::new("str", DataType::Utf8, false),
                                Field::new("num", DataType::Int32, false),
                            ])),
                            false,
                        )),
                        false,
                    ),
                    true,
                ),
            ]),
        )
    }

    #[test]
    fn basic_backward_compatible_map_2() -> crate::errors::Result<()> {
        test_expected_type(
            "
            message schema {
                optional group my_map (MAP_KEY_VALUE) {
                  repeated group map {
                    required binary key (STRING);
                    optional int32 value;
                  }
                }
            }
        ",
            Fields::from(vec![
                // Map<String, Integer> (nullable map, nullable values)
                Field::new(
                    "my_map",
                    DataType::Map(
                        Arc::new(Field::new(
                            "map",
                            DataType::Struct(Fields::from(vec![
                                Field::new("key", DataType::Utf8, false),
                                Field::new("value", DataType::Int32, true),
                            ])),
                            false,
                        )),
                        false,
                    ),
                    true,
                ),
            ]),
        )
    }

    #[test]
    fn convert_schema_with_nested_list_repeated_primitive() -> crate::errors::Result<()> {
        test_roundtrip(
            "
            message schema {
                optional group f1 (LIST) {
                    repeated group element {
                        repeated int32 element;
                    }
                }
            }
        ",
        )
    }

    #[test]
    fn convert_schema_with_repeated_primitive_keep_field_id() -> crate::errors::Result<()> {
        let message_type = "
    message schema {
      repeated BYTE_ARRAY col_1 = 1;
    }
    ";

        let parsed_input_schema = Arc::new(parse_message_type(message_type)?);
        let schema = SchemaDescriptor::new(parsed_input_schema);

        let converted = convert_schema(&schema, ProjectionMask::all(), None)?.unwrap();

        let DataType::Struct(schema_fields) = &converted.arrow_type else {
            panic!("Expected struct from convert_schema");
        };

        assert_eq!(schema_fields.len(), 1);

        let expected_schema = DataType::Struct(Fields::from(vec![Arc::new(
            arrow_schema::Field::new(
                "col_1",
                DataType::List(Arc::new(
                    // No metadata on inner field
                    arrow_schema::Field::new("col_1", DataType::Binary, false),
                )),
                false,
            )
            // add the field id to the outer list
            .with_field_id(1),
        )]));

        assert_eq!(converted.arrow_type, expected_schema);

        Ok(())
    }

    #[test]
    fn convert_schema_with_repeated_primitive_should_use_inferred_schema(
    ) -> crate::errors::Result<()> {
        let message_type = "
    message schema {
      repeated BYTE_ARRAY col_1 = 1;
    }
    ";

        let parsed_input_schema = Arc::new(parse_message_type(message_type)?);
        let schema = SchemaDescriptor::new(parsed_input_schema);

        let converted = convert_schema(&schema, ProjectionMask::all(), None)?.unwrap();

        let DataType::Struct(schema_fields) = &converted.arrow_type else {
            panic!("Expected struct from convert_schema");
        };

        assert_eq!(schema_fields.len(), 1);

        let expected_schema = DataType::Struct(Fields::from(vec![Arc::new(
            arrow_schema::Field::new(
                "col_1",
                DataType::List(Arc::new(arrow_schema::Field::new(
                    "col_1",
                    DataType::Binary,
                    false,
                ))),
                false,
            )
            .with_metadata(schema_fields[0].metadata().clone()),
        )]));

        assert_eq!(converted.arrow_type, expected_schema);

        let utf8_instead_of_binary = Fields::from(vec![Arc::new(
            arrow_schema::Field::new(
                "col_1",
                DataType::List(Arc::new(arrow_schema::Field::new(
                    "col_1",
                    DataType::Utf8,
                    false,
                ))),
                false,
            )
            .with_metadata(schema_fields[0].metadata().clone()),
        )]);

        // Should be able to convert the same thing
        let converted_again = convert_schema(
            &schema,
            ProjectionMask::all(),
            Some(&utf8_instead_of_binary),
        )?
        .unwrap();

        // Assert that we changed to Utf8
        assert_eq!(
            converted_again.arrow_type,
            DataType::Struct(utf8_instead_of_binary)
        );

        Ok(())
    }

    #[test]
    fn convert_schema_with_repeated_primitive_should_use_inferred_schema_for_list_as_well(
    ) -> crate::errors::Result<()> {
        let message_type = "
    message schema {
      repeated BYTE_ARRAY col_1 = 1;
    }
    ";

        let parsed_input_schema = Arc::new(parse_message_type(message_type)?);
        let schema = SchemaDescriptor::new(parsed_input_schema);

        let converted = convert_schema(&schema, ProjectionMask::all(), None)?.unwrap();

        let DataType::Struct(schema_fields) = &converted.arrow_type else {
            panic!("Expected struct from convert_schema");
        };

        assert_eq!(schema_fields.len(), 1);

        let expected_schema = DataType::Struct(Fields::from(vec![Arc::new(
            arrow_schema::Field::new(
                "col_1",
                DataType::List(Arc::new(arrow_schema::Field::new(
                    "col_1",
                    DataType::Binary,
                    false,
                ))),
                false,
            )
            .with_metadata(schema_fields[0].metadata().clone()),
        )]));

        assert_eq!(converted.arrow_type, expected_schema);

        let utf8_instead_of_binary = Fields::from(vec![Arc::new(
            arrow_schema::Field::new(
                "col_1",
                // Inferring as LargeList instead of List
                DataType::LargeList(Arc::new(arrow_schema::Field::new(
                    "col_1",
                    DataType::Utf8,
                    false,
                ))),
                false,
            )
            .with_metadata(schema_fields[0].metadata().clone()),
        )]);

        // Should be able to convert the same thing
        let converted_again = convert_schema(
            &schema,
            ProjectionMask::all(),
            Some(&utf8_instead_of_binary),
        )?
        .unwrap();

        // Assert that we changed to Utf8
        assert_eq!(
            converted_again.arrow_type,
            DataType::Struct(utf8_instead_of_binary)
        );

        Ok(())
    }

    #[test]
    fn convert_schema_with_repeated_struct_and_inferred_schema() -> crate::errors::Result<()> {
        test_roundtrip(
            "
    message schema {
        repeated group my_col_1 = 1 {
          optional binary my_col_2 = 2;
          optional binary my_col_3 = 3;
          optional group my_col_4 = 4 {
            optional int64 my_col_5 = 5;
            optional int32 my_col_6 = 6;
          }
        }
    }
    ",
        )
    }

    #[test]
    fn convert_schema_with_repeated_struct_and_inferred_schema_and_field_id(
    ) -> crate::errors::Result<()> {
        let message_type = "
    message schema {
        repeated group my_col_1 = 1 {
          optional binary my_col_2 = 2;
          optional binary my_col_3 = 3;
          optional group my_col_4 = 4 {
            optional int64 my_col_5 = 5;
            optional int32 my_col_6 = 6;
          }
        }
    }
    ";

        let parsed_input_schema = Arc::new(parse_message_type(message_type)?);
        let schema = SchemaDescriptor::new(parsed_input_schema);

        let converted = convert_schema(&schema, ProjectionMask::all(), None)?.unwrap();

        let DataType::Struct(schema_fields) = &converted.arrow_type else {
            panic!("Expected struct from convert_schema");
        };

        assert_eq!(schema_fields.len(), 1);

        // Should be able to convert the same thing
        let converted_again =
            convert_schema(&schema, ProjectionMask::all(), Some(schema_fields))?.unwrap();

        // Assert that we changed to Utf8
        assert_eq!(converted_again.arrow_type, converted.arrow_type);

        Ok(())
    }

    #[test]
    fn convert_schema_with_nested_repeated_struct_and_primitives() -> crate::errors::Result<()> {
        let message_type = "
message schema {
    repeated group my_col_1 = 1 {
        optional binary my_col_2 = 2;
        repeated BYTE_ARRAY my_col_3 = 3;
        repeated group my_col_4 = 4 {
            optional int64 my_col_5 = 5;
            repeated binary my_col_6 = 6;
        }
    }
}
";

        let parsed_input_schema = Arc::new(parse_message_type(message_type)?);
        let schema = SchemaDescriptor::new(parsed_input_schema);

        let converted = convert_schema(&schema, ProjectionMask::all(), None)?.unwrap();

        let DataType::Struct(schema_fields) = &converted.arrow_type else {
            panic!("Expected struct from convert_schema");
        };

        assert_eq!(schema_fields.len(), 1);

        // Build expected schema
        let expected_schema = DataType::Struct(Fields::from(vec![Arc::new(
            arrow_schema::Field::new(
                "my_col_1",
                DataType::List(Arc::new(arrow_schema::Field::new(
                    "my_col_1",
                    DataType::Struct(Fields::from(vec![
                        Arc::new(
                            arrow_schema::Field::new("my_col_2", DataType::Binary, true)
                                .with_field_id(2),
                        ),
                        Arc::new(
                            arrow_schema::Field::new(
                                "my_col_3",
                                DataType::List(Arc::new(arrow_schema::Field::new(
                                    "my_col_3",
                                    DataType::Binary,
                                    false,
                                ))),
                                false,
                            )
                            // add the field id to the outer list
                            .with_field_id(3),
                        ),
                        Arc::new(
                            arrow_schema::Field::new(
                                "my_col_4",
                                DataType::List(Arc::new(arrow_schema::Field::new(
                                    "my_col_4",
                                    DataType::Struct(Fields::from(vec![
                                        Arc::new(
                                            arrow_schema::Field::new(
                                                "my_col_5",
                                                DataType::Int64,
                                                true,
                                            )
                                            // add the field id to the outer list
                                            .with_field_id(5),
                                        ),
                                        Arc::new(
                                            arrow_schema::Field::new(
                                                "my_col_6",
                                                DataType::List(Arc::new(arrow_schema::Field::new(
                                                    "my_col_6",
                                                    DataType::Binary,
                                                    false,
                                                ))),
                                                false,
                                            )
                                            // add the field id to the outer list
                                            .with_field_id(6),
                                        ),
                                    ])),
                                    false,
                                ))),
                                false,
                            )
                            // add the field id to the outer list
                            .with_field_id(4),
                        ),
                    ])),
                    false,
                ))),
                false,
            )
            // add the field id to the outer list
            .with_field_id(1),
        )]));

        assert_eq!(converted.arrow_type, expected_schema);

        // Test conversion with inferred schema
        let converted_again =
            convert_schema(&schema, ProjectionMask::all(), Some(schema_fields))?.unwrap();

        assert_eq!(converted_again.arrow_type, converted.arrow_type);

        // Test conversion with modified schema (change lists to either LargeList or FixedSizeList)
        // as well as changing Binary to Utf8 or BinaryView
        let modified_schema_fields = Fields::from(vec![Arc::new(
            arrow_schema::Field::new(
                "my_col_1",
                DataType::LargeList(Arc::new(arrow_schema::Field::new(
                    "my_col_1",
                    DataType::Struct(Fields::from(vec![
                        Arc::new(
                            arrow_schema::Field::new("my_col_2", DataType::LargeBinary, true)
                                .with_field_id(2),
                        ),
                        Arc::new(
                            arrow_schema::Field::new(
                                "my_col_3",
                                DataType::LargeList(Arc::new(arrow_schema::Field::new(
                                    "my_col_3",
                                    DataType::Utf8,
                                    false,
                                ))),
                                false,
                            )
                            // add the field id to the outer list
                            .with_field_id(3),
                        ),
                        Arc::new(
                            arrow_schema::Field::new(
                                "my_col_4",
                                DataType::FixedSizeList(
                                    Arc::new(arrow_schema::Field::new(
                                        "my_col_4",
                                        DataType::Struct(Fields::from(vec![
                                            Arc::new(
                                                arrow_schema::Field::new(
                                                    "my_col_5",
                                                    DataType::Int64,
                                                    true,
                                                )
                                                .with_field_id(5),
                                            ),
                                            Arc::new(
                                                arrow_schema::Field::new(
                                                    "my_col_6",
                                                    DataType::LargeList(Arc::new(
                                                        arrow_schema::Field::new(
                                                            "my_col_6",
                                                            DataType::BinaryView,
                                                            false,
                                                        ),
                                                    )),
                                                    false,
                                                )
                                                // add the field id to the outer list
                                                .with_field_id(6),
                                            ),
                                        ])),
                                        false,
                                    )),
                                    3,
                                ),
                                false,
                            )
                            // add the field id to the outer list
                            .with_field_id(4),
                        ),
                    ])),
                    false,
                ))),
                false,
            )
            // add the field id to the outer list
            .with_field_id(1),
        )]);

        let converted_with_modified = convert_schema(
            &schema,
            ProjectionMask::all(),
            Some(&modified_schema_fields),
        )?
        .unwrap();

        assert_eq!(
            converted_with_modified.arrow_type,
            DataType::Struct(modified_schema_fields)
        );

        Ok(())
    }

    /// Backwards-compatibility: LIST with nullable element type - 1 - standard
    /// Taken from [Spark](https://github.com/apache/spark/blob/8ab50765cd793169091d983b50d87a391f6ac1f4/sql/core/src/test/scala/org/apache/spark/sql/parquet/ParquetSchemaSuite.scala#L452-L466)
    #[test]
    fn list_nullable_element_standard() -> crate::errors::Result<()> {
        test_expected_type(
            "
            message root {
              optional group f1 (LIST) {
                repeated group list {
                  optional int32 element;
                }
              }
            }",
            Fields::from(vec![Field::new(
                "f1",
                DataType::List(Arc::new(Field::new("element", DataType::Int32, true))),
                true,
            )]),
        )
    }

    /// Backwards-compatibility: LIST with nullable element type - 2
    /// Taken from [Spark](https://github.com/apache/spark/blob/8ab50765cd793169091d983b50d87a391f6ac1f4/sql/core/src/test/scala/org/apache/spark/sql/parquet/ParquetSchemaSuite.scala#L468-L482)
    #[test]
    fn list_nullable_element_nested() -> crate::errors::Result<()> {
        test_expected_type(
            "
            message root {
              optional group f1 (LIST) {
                repeated group element {
                  optional int32 num;
                }
              }
            }",
            Fields::from(vec![Field::new(
                "f1",
                DataType::List(Arc::new(Field::new("num", DataType::Int32, true))),
                true,
            )]),
        )
    }

    /// Backwards-compatibility: LIST with non-nullable element type - 1 - standard
    /// Taken from [Spark](https://github.com/apache/spark/blob/8ab50765cd793169091d983b50d87a391f6ac1f4/sql/core/src/test/scala/org/apache/spark/sql/parquet/ParquetSchemaSuite.scala#L484-L495)
    #[test]
    fn list_required_element_standard() -> crate::errors::Result<()> {
        test_expected_type(
            "
            message root {
              optional group f1 (LIST) {
                repeated group list {
                  required int32 element;
                }
              }
            }",
            Fields::from(vec![Field::new(
                "f1",
                DataType::List(Arc::new(Field::new("element", DataType::Int32, false))),
                true,
            )]),
        )
    }

    /// Backwards-compatibility: LIST with non-nullable element type - 2
    /// Taken from [Spark](https://github.com/apache/spark/blob/8ab50765cd793169091d983b50d87a391f6ac1f4/sql/core/src/test/scala/org/apache/spark/sql/parquet/ParquetSchemaSuite.scala#L497-L508)
    #[test]
    fn list_required_element_nested() -> crate::errors::Result<()> {
        test_expected_type(
            "
            message root {
              optional group f1 (LIST) {
                repeated group element {
                  required int32 num;
                }
              }
            }",
            Fields::from(vec![Field::new(
                "f1",
                DataType::List(Arc::new(Field::new("num", DataType::Int32, false))),
                true,
            )]),
        )
    }

    /// Backwards-compatibility: LIST with non-nullable element type - 3
    /// Taken from [Spark](https://github.com/apache/spark/blob/8ab50765cd793169091d983b50d87a391f6ac1f4/sql/core/src/test/scala/org/apache/spark/sql/parquet/ParquetSchemaSuite.scala#L510-L519)
    #[test]
    fn list_required_element_primitive() -> crate::errors::Result<()> {
        test_expected_type(
            "
            message root {
              optional group f1 (LIST) {
                repeated int32 element;
              }
            }",
            Fields::from(vec![Field::new(
                "f1",
                DataType::List(Arc::new(Field::new("element", DataType::Int32, false))),
                true,
            )]),
        )
    }

    /// Backwards-compatibility: LIST with non-nullable element type - 4
    /// Taken from [Spark](https://github.com/apache/spark/blob/8ab50765cd793169091d983b50d87a391f6ac1f4/sql/core/src/test/scala/org/apache/spark/sql/parquet/ParquetSchemaSuite.scala#L521-L540)
    #[test]
    fn list_required_element_struct() -> crate::errors::Result<()> {
        test_expected_type(
            "
            message root {
              optional group f1 (LIST) {
                repeated group element {
                  required binary str (UTF8);
                  required int32 num;
                }
              }
            }",
            Fields::from(vec![Field::new(
                "f1",
                DataType::List(Arc::new(Field::new(
                    "element",
                    DataType::Struct(Fields::from(vec![
                        Field::new("str", DataType::Utf8, false),
                        Field::new("num", DataType::Int32, false),
                    ])),
                    false,
                ))),
                true,
            )]),
        )
    }

    /// Backwards-compatibility: LIST with non-nullable element type - 5 - parquet-avro style
    /// Taken from [Spark](https://github.com/apache/spark/blob/8ab50765cd793169091d983b50d87a391f6ac1f4/sql/core/src/test/scala/org/apache/spark/sql/parquet/ParquetSchemaSuite.scala#L542-L559)
    #[test]
    fn list_required_element_avro_style() -> crate::errors::Result<()> {
        test_expected_type(
            "
            message root {
              optional group f1 (LIST) {
                repeated group array {
                  required binary str (UTF8);
                }
              }
            }",
            Fields::from(vec![Field::new(
                "f1",
                DataType::List(Arc::new(Field::new(
                    "array",
                    DataType::Struct(Fields::from(vec![Field::new("str", DataType::Utf8, false)])),
                    false,
                ))),
                true,
            )]),
        )
    }

    /// Backwards-compatibility: LIST with non-nullable element type - 6 - parquet-thrift style
    /// Taken from [Spark](https://github.com/apache/spark/blob/8ab50765cd793169091d983b50d87a391f6ac1f4/sql/core/src/test/scala/org/apache/spark/sql/parquet/ParquetSchemaSuite.scala#L561-L578)
    #[test]
    fn list_required_element_thrift_style() -> crate::errors::Result<()> {
        test_expected_type(
            "
            message root {
              optional group f1 (LIST) {
                repeated group f1_tuple {
                  required binary str (UTF8);
                }
              }
            }",
            Fields::from(vec![Field::new(
                "f1",
                DataType::List(Arc::new(Field::new(
                    "f1_tuple",
                    DataType::Struct(Fields::from(vec![Field::new("str", DataType::Utf8, false)])),
                    false,
                ))),
                true,
            )]),
        )
    }

    /// Backwards-compatibility: MAP with non-nullable value type - 1 - standard
    /// Taken from [Spark](https://github.com/apache/spark/blob/8ab50765cd793169091d983b50d87a391f6ac1f4/sql/core/src/test/scala/org/apache/spark/sql/parquet/ParquetSchemaSuite.scala#L652-L667)
    #[test]
    fn map_required_value_standard() -> crate::errors::Result<()> {
        test_expected_type(
            "
            message root {
              optional group f1 (MAP) {
                repeated group key_value {
                  required int32 key;
                  required binary value (UTF8);
                }
              }
            }",
            Fields::from(vec![Field::new_map(
                "f1",
                "key_value",
                Field::new("key", DataType::Int32, false),
                Field::new("value", DataType::Utf8, false),
                false,
                true,
            )]),
        )
    }

    /// Backwards-compatibility: MAP with non-nullable value type - 2
    /// Taken from [Spark](https://github.com/apache/spark/blob/8ab50765cd793169091d983b50d87a391f6ac1f4/sql/core/src/test/scala/org/apache/spark/sql/parquet/ParquetSchemaSuite.scala#L669-L684)
    #[test]
    fn map_required_value_map_key_value() -> crate::errors::Result<()> {
        test_expected_type(
            "
            message root {
              optional group f1 (MAP_KEY_VALUE) {
                repeated group map {
                  required int32 num;
                  required binary str (UTF8);
                }
              }
            }",
            Fields::from(vec![Field::new_map(
                "f1",
                "map",
                Field::new("num", DataType::Int32, false),
                Field::new("str", DataType::Utf8, false),
                false,
                true,
            )]),
        )
    }

    /// Backwards-compatibility: MAP with non-nullable value type - 3 - prior to 1.4.x
    /// Taken from [Spark](https://github.com/apache/spark/blob/8ab50765cd793169091d983b50d87a391f6ac1f4/sql/core/src/test/scala/org/apache/spark/sql/parquet/ParquetSchemaSuite.scala#L686-L701)
    #[test]
    fn map_required_value_legacy() -> crate::errors::Result<()> {
        test_expected_type(
            "
            message root {
              optional group f1 (MAP) {
                repeated group map (MAP_KEY_VALUE) {
                  required int32 key;
                  required binary value (UTF8);
                }
              }
            }",
            Fields::from(vec![Field::new_map(
                "f1",
                "map",
                Field::new("key", DataType::Int32, false),
                Field::new("value", DataType::Utf8, false),
                false,
                true,
            )]),
        )
    }

    /// Backwards-compatibility: MAP with nullable value type - 1 - standard
    /// Taken from [Spark](https://github.com/apache/spark/blob/8ab50765cd793169091d983b50d87a391f6ac1f4/sql/core/src/test/scala/org/apache/spark/sql/parquet/ParquetSchemaSuite.scala#L703-L718)
    #[test]
    fn map_optional_value_standard() -> crate::errors::Result<()> {
        test_expected_type(
            "
            message root {
              optional group f1 (MAP) {
                repeated group key_value {
                  required int32 key;
                  optional binary value (UTF8);
                }
              }
            }",
            Fields::from(vec![Field::new_map(
                "f1",
                "key_value",
                Field::new("key", DataType::Int32, false),
                Field::new("value", DataType::Utf8, true),
                false,
                true,
            )]),
        )
    }

    /// Backwards-compatibility: MAP with nullable value type - 2
    /// Taken from [Spark](https://github.com/apache/spark/blob/8ab50765cd793169091d983b50d87a391f6ac1f4/sql/core/src/test/scala/org/apache/spark/sql/parquet/ParquetSchemaSuite.scala#L720-L735)
    #[test]
    fn map_optional_value_map_key_value() -> crate::errors::Result<()> {
        test_expected_type(
            "
            message root {
              optional group f1 (MAP_KEY_VALUE) {
                repeated group map {
                  required int32 num;
                  optional binary str (UTF8);
                }
              }
            }",
            Fields::from(vec![Field::new_map(
                "f1",
                "map",
                Field::new("num", DataType::Int32, false),
                Field::new("str", DataType::Utf8, true),
                false,
                true,
            )]),
        )
    }

    /// Backwards-compatibility: MAP with nullable value type - 3 - parquet-avro style
    /// Taken from [Spark](https://github.com/apache/spark/blob/8ab50765cd793169091d983b50d87a391f6ac1f4/sql/core/src/test/scala/org/apache/spark/sql/parquet/ParquetSchemaSuite.scala#L737-L752)
    #[test]
    fn map_optional_value_avro_style() -> crate::errors::Result<()> {
        test_expected_type(
            "
            message root {
              optional group f1 (MAP) {
                repeated group map (MAP_KEY_VALUE) {
                  required int32 key;
                  optional binary value (UTF8);
                }
              }
            }",
            Fields::from(vec![Field::new_map(
                "f1",
                "map",
                Field::new("key", DataType::Int32, false),
                Field::new("value", DataType::Utf8, true),
                false,
                true,
            )]),
        )
    }
}
