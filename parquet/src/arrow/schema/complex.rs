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

use crate::arrow::schema::primitive::convert_primitive;
use crate::arrow::{ProjectionMask, PARQUET_FIELD_ID_META_KEY};
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

        let arrow_type = convert_primitive(primitive_type, context.data_type)?;

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

        // Extract the arrow fields
        let arrow_fields = match &context.data_type {
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
                ))
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
            };

            if let Some(child) = self.dispatch(parquet_field, child_ctx)? {
                // The child type returned may be different from what is encoded in the arrow
                // schema in the event of a mismatch or a projection
                child_fields.push(convert_field(parquet_field, &child, arrow_field));
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

        if map_key_value.get_fields().len() != 2 {
            // According to the specification the values are optional (#1642)
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
                ))
            }
            None => (None, None, None, false),
        };

        let maybe_key = {
            let context = VisitorContext {
                rep_level,
                def_level,
                data_type: arrow_key.map(|x| x.data_type().clone()),
            };

            self.dispatch(map_key, context)?
        };

        let maybe_value = {
            let context = VisitorContext {
                rep_level,
                def_level,
                data_type: arrow_value.map(|x| x.data_type().clone()),
            };

            self.dispatch(map_value, context)?
        };

        // Need both columns to be projected
        match (maybe_key, maybe_value) {
            (Some(key), Some(value)) => {
                let key_field = Arc::new(
                    convert_field(map_key, &key, arrow_key)
                        // The key is always non-nullable (#5630)
                        .with_nullable(false),
                );
                let value_field = Arc::new(convert_field(map_value, &value, arrow_value));
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
                ))
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

        let items = repeated_field.get_fields();
        if items.len() != 1
            || repeated_field.name() == "array"
            || repeated_field.name() == format!("{}_tuple", list_type.name())
        {
            // If the repeated field is a group with multiple fields, then its type is the element type and elements are required.
            //
            // If the repeated field is a group with one field and is named either array or uses the LIST-annotated group's name
            // with _tuple appended then the repeated type is the element type and elements are required.
            let context = VisitorContext {
                rep_level: context.rep_level,
                def_level,
                data_type: arrow_field.map(|f| f.data_type().clone()),
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
        };

        match self.dispatch(item_type, new_context) {
            Ok(Some(item)) => {
                let item_field = Arc::new(convert_field(item_type, &item, arrow_field));

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

/// Computes the [`Field`] for a child column
///
/// The resulting [`Field`] will have the type dictated by `field`, a name
/// dictated by the `parquet_type`, and any metadata from `arrow_hint`
fn convert_field(parquet_type: &Type, field: &ParquetField, arrow_hint: Option<&Field>) -> Field {
    let name = parquet_type.name();
    let data_type = field.arrow_type.clone();
    let nullable = field.nullable;

    match arrow_hint {
        Some(hint) => {
            // If the inferred type is a dictionary, preserve dictionary metadata
            let field = match (&data_type, hint.dict_id(), hint.dict_is_ordered()) {
                (DataType::Dictionary(_, _), Some(id), Some(ordered)) => {
                    Field::new_dict(name, data_type, nullable, id, ordered)
                }
                _ => Field::new(name, data_type, nullable),
            };

            field.with_metadata(hint.metadata().clone())
        }
        None => {
            let mut ret = Field::new(name, data_type, nullable);
            let basic_info = parquet_type.get_basic_info();
            if basic_info.has_id() {
                let mut meta = HashMap::with_capacity(1);
                meta.insert(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    basic_info.id().to_string(),
                );
                ret.set_metadata(meta);
            }
            ret
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
    };

    Ok(visitor.dispatch(parquet_type, context)?.unwrap())
}
