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

#[derive(Debug, PartialEq)]
pub struct Field {
    ident: syn::Ident,
    ty: Type,
    is_a_byte_buf: bool,
    third_party_type: Option<ThirdPartyType>,
}

/// Use third party libraries, detected
/// at compile time. These libraries will
/// be written to parquet as their preferred
/// physical type.
///
///   ChronoNaiveDateTime is written as i64
///   ChronoNaiveDate is written as i32
#[derive(Debug, PartialEq)]
enum ThirdPartyType {
    ChronoNaiveDateTime,
    ChronoNaiveDate,
    Uuid,
}

impl Field {
    pub fn from(f: &syn::Field) -> Self {
        let ty = Type::from(f);
        let is_a_byte_buf = ty.physical_type() == parquet::basic::Type::BYTE_ARRAY;

        let third_party_type = match &ty.last_part()[..] {
            "NaiveDateTime" => Some(ThirdPartyType::ChronoNaiveDateTime),
            "NaiveDate" => Some(ThirdPartyType::ChronoNaiveDate),
            "Uuid" => Some(ThirdPartyType::Uuid),
            _ => None,
        };

        Field {
            ident: f
                .ident
                .clone()
                .expect("Only structs with named fields are currently supported"),
            ty,
            is_a_byte_buf,
            third_party_type,
        }
    }

    /// Takes the parsed field of the struct and emits a valid
    /// column writer snippet. Should match exactly what you
    /// would write by hand.
    ///
    /// Can only generate writers for basic structs, for example:
    ///
    /// struct Record {
    ///   a_bool: bool,
    ///   maybe_a_bool: `Option<bool>`
    /// }
    ///
    /// but not
    ///
    /// struct UnsupportedNestedRecord {
    ///   a_property: bool,
    ///   nested_record: Record
    /// }
    ///
    /// because this parsing logic is not sophisticated enough for definition
    /// levels beyond 2.
    pub fn writer_snippet(&self) -> proc_macro2::TokenStream {
        let ident = &self.ident;
        let column_writer = self.ty.column_writer();

        let vals_builder = match &self.ty {
            Type::TypePath(_) => self.copied_direct_vals(),
            Type::Option(ref first_type) => match **first_type {
                Type::TypePath(_) => self.option_into_vals(),
                Type::Reference(_, ref second_type) => match **second_type {
                    Type::TypePath(_) => self.option_into_vals(),
                    _ => unimplemented!("Unsupported type encountered"),
                },
                ref f => unimplemented!("Unsupported: {:#?}", f),
            },
            Type::Reference(_, ref first_type) => match **first_type {
                Type::TypePath(_) => self.copied_direct_vals(),
                Type::Option(ref second_type) => match **second_type {
                    Type::TypePath(_) => self.option_into_vals(),
                    Type::Reference(_, ref second_type) => match **second_type {
                        Type::TypePath(_) => self.option_into_vals(),
                        _ => unimplemented!("Unsupported type encountered"),
                    },
                    ref f => unimplemented!("Unsupported: {:#?}", f),
                },
                ref f => unimplemented!("Unsupported: {:#?}", f),
            },
            f => unimplemented!("Unsupported: {:#?}", f),
        };

        let definition_levels = match &self.ty {
            Type::TypePath(_) => None,
            Type::Option(ref first_type) => match **first_type {
                Type::TypePath(_) => Some(self.optional_definition_levels()),
                Type::Option(_) => unimplemented!("Unsupported nesting encountered"),
                Type::Reference(_, ref second_type)
                | Type::Vec(ref second_type)
                | Type::Array(ref second_type) => match **second_type {
                    Type::TypePath(_) => Some(self.optional_definition_levels()),
                    _ => unimplemented!("Unsupported nesting encountered"),
                },
            },
            Type::Reference(_, ref first_type)
            | Type::Vec(ref first_type)
            | Type::Array(ref first_type) => match **first_type {
                Type::TypePath(_) => None,
                Type::Reference(_, ref second_type)
                | Type::Vec(ref second_type)
                | Type::Array(ref second_type)
                | Type::Option(ref second_type) => match **second_type {
                    Type::TypePath(_) => Some(self.optional_definition_levels()),
                    Type::Reference(_, ref third_type) => match **third_type {
                        Type::TypePath(_) => Some(self.optional_definition_levels()),
                        _ => unimplemented!("Unsupported definition encountered"),
                    },
                    _ => unimplemented!("Unsupported definition encountered"),
                },
            },
        };

        // "vals" is the run of primitive data being written for the column
        // "definition_levels" is a vector of bools which controls whether a value is missing or present
        // this TokenStream is only one part of the code for writing a column and
        // it relies on values calculated in prior code snippets, namely "definition_levels" and "vals_builder".
        // All the context is put together in this functions final quote and
        // this expression just switches between non-nullable and nullable write statements
        let write_batch_expr = if definition_levels.is_some() {
            quote! {
                if let #column_writer(ref mut typed) = column_writer.untyped() {
                    typed.write_batch(&vals[..], Some(&definition_levels[..]), None)?;
                } else {
                    panic!("Schema and struct disagree on type for {}", stringify!{#ident})
                }
            }
        } else {
            quote! {
                if let #column_writer(ref mut typed) = column_writer.untyped() {
                    typed.write_batch(&vals[..], None, None)?;
                } else {
                    panic!("Schema and struct disagree on type for {}", stringify!{#ident})
                }
            }
        };

        quote! {
            {
                #definition_levels

                #vals_builder

                #write_batch_expr
            }
        }
    }

    pub fn parquet_type(&self) -> proc_macro2::TokenStream {
        // TODO: Support group types
        // TODO: Add length if dealing with fixedlenbinary

        let field_name = &self.ident.to_string();
        let physical_type = match self.ty.physical_type() {
            parquet::basic::Type::BOOLEAN => quote! {
                ::parquet::basic::Type::BOOLEAN
            },
            parquet::basic::Type::INT32 => quote! {
                ::parquet::basic::Type::INT32
            },
            parquet::basic::Type::INT64 => quote! {
                ::parquet::basic::Type::INT64
            },
            parquet::basic::Type::INT96 => quote! {
                ::parquet::basic::Type::INT96
            },
            parquet::basic::Type::FLOAT => quote! {
                ::parquet::basic::Type::FLOAT
            },
            parquet::basic::Type::DOUBLE => quote! {
                ::parquet::basic::Type::DOUBLE
            },
            parquet::basic::Type::BYTE_ARRAY => quote! {
                ::parquet::basic::Type::BYTE_ARRAY
            },
            parquet::basic::Type::FIXED_LEN_BYTE_ARRAY => quote! {
                ::parquet::basic::Type::FIXED_LEN_BYTE_ARRAY
            },
        };
        let logical_type = self.ty.logical_type();
        let repetition = self.ty.repetition();
        let converted_type = self.ty.converted_type();

        if let Some(converted_type) = converted_type {
            quote! {
                fields.push(ParquetType::primitive_type_builder(#field_name, #physical_type)
                    .with_logical_type(#logical_type)
                    .with_repetition(#repetition)
                    .with_converted_type(#converted_type)
                    .build().unwrap().into()
                )
            }
        } else {
            quote! {
                fields.push(ParquetType::primitive_type_builder(#field_name, #physical_type)
                    .with_logical_type(#logical_type)
                    .with_repetition(#repetition)
                    .build().unwrap().into()
                )
            }
        }
    }

    fn option_into_vals(&self) -> proc_macro2::TokenStream {
        let field_name = &self.ident;
        let is_a_byte_buf = self.is_a_byte_buf;
        let is_a_timestamp =
            self.third_party_type == Some(ThirdPartyType::ChronoNaiveDateTime);
        let is_a_date = self.third_party_type == Some(ThirdPartyType::ChronoNaiveDate);
        let is_a_uuid = self.third_party_type == Some(ThirdPartyType::Uuid);
        let copy_to_vec = !matches!(
            self.ty.physical_type(),
            parquet::basic::Type::BYTE_ARRAY | parquet::basic::Type::FIXED_LEN_BYTE_ARRAY
        );

        let binding = if copy_to_vec {
            quote! { let Some(inner) = rec.#field_name }
        } else {
            quote! { let Some(ref inner) = rec.#field_name }
        };

        let some = if is_a_timestamp {
            quote! { Some(inner.timestamp_millis()) }
        } else if is_a_date {
            quote! { Some(inner.signed_duration_since(::chrono::NaiveDate::from_ymd(1970, 1, 1)).num_days() as i32)  }
        } else if is_a_uuid {
            quote! { Some((&inner.to_string()[..]).into()) }
        } else if is_a_byte_buf {
            quote! { Some((&inner[..]).into())}
        } else {
            // Type might need converting to a physical type
            match self.ty.physical_type() {
                parquet::basic::Type::INT32 => quote! { Some(inner as i32) },
                parquet::basic::Type::INT64 => quote! { Some(inner as i64) },
                _ => quote! { Some(inner) },
            }
        };

        quote! {
            let vals: Vec<_> = records.iter().filter_map(|rec| {
                if #binding {
                    #some
                } else {
                    None
                }
            }).collect();
        }
    }

    fn copied_direct_vals(&self) -> proc_macro2::TokenStream {
        let field_name = &self.ident;
        let is_a_byte_buf = self.is_a_byte_buf;
        let is_a_timestamp =
            self.third_party_type == Some(ThirdPartyType::ChronoNaiveDateTime);
        let is_a_date = self.third_party_type == Some(ThirdPartyType::ChronoNaiveDate);
        let is_a_uuid = self.third_party_type == Some(ThirdPartyType::Uuid);

        let access = if is_a_timestamp {
            quote! { rec.#field_name.timestamp_millis() }
        } else if is_a_date {
            quote! { rec.#field_name.signed_duration_since(::chrono::NaiveDate::from_ymd(1970, 1, 1)).num_days() as i32 }
        } else if is_a_uuid {
            quote! { (&rec.#field_name.to_string()[..]).into() }
        } else if is_a_byte_buf {
            quote! { (&rec.#field_name[..]).into() }
        } else {
            // Type might need converting to a physical type
            match self.ty.physical_type() {
                parquet::basic::Type::INT32 => quote! { rec.#field_name as i32 },
                parquet::basic::Type::INT64 => quote! { rec.#field_name as i64 },
                _ => quote! { rec.#field_name },
            }
        };

        quote! {
            let vals: Vec<_> = records.iter().map(|rec| #access).collect();
        }
    }

    fn optional_definition_levels(&self) -> proc_macro2::TokenStream {
        let field_name = &self.ident;

        quote! {
            let definition_levels: Vec<i16> = self
              .iter()
              .map(|rec| if rec.#field_name.is_some() { 1 } else { 0 })
              .collect();
        }
    }
}

#[allow(clippy::enum_variant_names)]
#[allow(clippy::large_enum_variant)]
#[derive(Debug, PartialEq)]
enum Type {
    Array(Box<Type>),
    Option(Box<Type>),
    Vec(Box<Type>),
    TypePath(syn::Type),
    Reference(Option<syn::Lifetime>, Box<Type>),
}

impl Type {
    /// Takes a rust type and returns the appropriate
    /// parquet-rs column writer
    fn column_writer(&self) -> syn::TypePath {
        use parquet::basic::Type as BasicType;

        match self.physical_type() {
            BasicType::BOOLEAN => {
                syn::parse_quote!(ColumnWriter::BoolColumnWriter)
            }
            BasicType::INT32 => syn::parse_quote!(ColumnWriter::Int32ColumnWriter),
            BasicType::INT64 => syn::parse_quote!(ColumnWriter::Int64ColumnWriter),
            BasicType::INT96 => syn::parse_quote!(ColumnWriter::Int96ColumnWriter),
            BasicType::FLOAT => syn::parse_quote!(ColumnWriter::FloatColumnWriter),
            BasicType::DOUBLE => syn::parse_quote!(ColumnWriter::DoubleColumnWriter),
            BasicType::BYTE_ARRAY => {
                syn::parse_quote!(ColumnWriter::ByteArrayColumnWriter)
            }
            BasicType::FIXED_LEN_BYTE_ARRAY => {
                syn::parse_quote!(ColumnWriter::FixedLenByteArrayColumnWriter)
            }
        }
    }

    /// Helper to simplify a nested field definition to its leaf type
    ///
    /// Ex:
    ///   `Option<&String>` => Type::TypePath(String)
    ///   `&Option<i32>` => Type::TypePath(i32)
    ///   `Vec<Vec<u8>>` => Type::Vec(u8)
    ///
    /// Useful in determining the physical type of a field and the
    /// definition levels.
    fn leaf_type_recursive(&self) -> &Type {
        Type::leaf_type_recursive_helper(self, None)
    }

    fn leaf_type_recursive_helper<'a>(
        ty: &'a Type,
        parent_ty: Option<&'a Type>,
    ) -> &'a Type {
        match ty {
            Type::TypePath(_) => parent_ty.unwrap_or(ty),
            Type::Option(ref first_type)
            | Type::Vec(ref first_type)
            | Type::Array(ref first_type)
            | Type::Reference(_, ref first_type) => {
                Type::leaf_type_recursive_helper(first_type, Some(ty))
            }
        }
    }

    /// Helper method to further unwrap leaf_type() to get inner-most
    /// type information, useful for determining the physical type
    /// and normalizing the type paths.
    fn inner_type(&self) -> &syn::Type {
        let leaf_type = self.leaf_type_recursive();

        match leaf_type {
            Type::TypePath(ref type_) => type_,
            Type::Option(ref first_type)
            | Type::Vec(ref first_type)
            | Type::Array(ref first_type)
            | Type::Reference(_, ref first_type) => match **first_type {
                Type::TypePath(ref type_) => type_,
                _ => unimplemented!("leaf_type() should only return shallow types"),
            },
        }
    }

    /// Helper to normalize a type path by extracting the
    /// most identifiable part
    ///
    /// Ex:
    ///   std::string::String => String
    ///   `Vec<u8>` => `Vec<u8>`
    ///   chrono::NaiveDateTime => NaiveDateTime
    ///
    /// Does run the risk of mis-identifying a type if import
    /// rename is in play. Please note procedural macros always
    /// run before type resolution so this is a risk the user
    /// takes on when renaming imports.
    fn last_part(&self) -> String {
        let inner_type = self.inner_type();
        let inner_type_str = (quote! { #inner_type }).to_string();

        inner_type_str
            .split("::")
            .last()
            .unwrap()
            .trim()
            .to_string()
    }

    /// Converts rust types to parquet physical types.
    ///
    /// Ex:
    ///   [u8; 10] => FIXED_LEN_BYTE_ARRAY
    ///   `Vec<u8>`  => BYTE_ARRAY
    ///   String => BYTE_ARRAY
    ///   i32 => INT32
    fn physical_type(&self) -> parquet::basic::Type {
        use parquet::basic::Type as BasicType;

        let last_part = self.last_part();
        let leaf_type = self.leaf_type_recursive();

        match leaf_type {
            Type::Array(ref first_type) => {
                if let Type::TypePath(_) = **first_type {
                    if last_part == "u8" {
                        return BasicType::FIXED_LEN_BYTE_ARRAY;
                    }
                }
            }
            Type::Vec(ref first_type) => {
                if let Type::TypePath(_) = **first_type {
                    if last_part == "u8" {
                        return BasicType::BYTE_ARRAY;
                    }
                }
            }
            _ => (),
        }

        match last_part.trim() {
            "bool" => BasicType::BOOLEAN,
            "u8" | "u16" | "u32" => BasicType::INT32,
            "i8" | "i16" | "i32" | "NaiveDate" => BasicType::INT32,
            "u64" | "i64" | "NaiveDateTime" => BasicType::INT64,
            "usize" | "isize" => {
                if usize::BITS == 64 {
                    BasicType::INT64
                } else {
                    BasicType::INT32
                }
            }
            "f32" => BasicType::FLOAT,
            "f64" => BasicType::DOUBLE,
            "String" | "str" | "Uuid" => BasicType::BYTE_ARRAY,
            f => unimplemented!("{} currently is not supported", f),
        }
    }

    fn logical_type(&self) -> proc_macro2::TokenStream {
        let last_part = self.last_part();
        let leaf_type = self.leaf_type_recursive();

        match leaf_type {
            Type::Array(ref first_type) => {
                if let Type::TypePath(_) = **first_type {
                    if last_part == "u8" {
                        return quote! { None };
                    }
                }
            }
            Type::Vec(ref first_type) => {
                if let Type::TypePath(_) = **first_type {
                    if last_part == "u8" {
                        return quote! { None };
                    }
                }
            }
            _ => (),
        }

        match last_part.trim() {
            "bool" => quote! { None },
            "u8" => quote! { Some(LogicalType::Integer {
                bit_width: 8,
                is_signed: false,
            }) },
            "u16" => quote! { Some(LogicalType::Integer {
                bit_width: 16,
                is_signed: false,
            }) },
            "u32" => quote! { Some(LogicalType::Integer {
                bit_width: 32,
                is_signed: false,
            }) },
            "u64" => quote! { Some(LogicalType::Integer {
                bit_width: 64,
                is_signed: false,
            }) },
            "i8" => quote! { Some(LogicalType::Integer {
                bit_width: 8,
                is_signed: true,
            }) },
            "i16" => quote! { Some(LogicalType::Integer {
                bit_width: 16,
                is_signed: true,
            }) },
            "i32" | "i64" => quote! { None },
            "usize" => {
                quote! { Some(LogicalType::Integer {
                    bit_width: usize::BITS as i8,
                    is_signed: false
                }) }
            }
            "isize" => {
                quote! { Some(LogicalType::Integer {
                    bit_width: usize::BITS as i8,
                    is_signed: true
                }) }
            }
            "NaiveDate" => quote! { Some(LogicalType::Date) },
            "NaiveDateTime" => quote! { None },
            "f32" | "f64" => quote! { None },
            "String" | "str" => quote! { Some(LogicalType::String) },
            "Uuid" => quote! { Some(LogicalType::Uuid) },
            f => unimplemented!("{} currently is not supported", f),
        }
    }

    fn converted_type(&self) -> Option<proc_macro2::TokenStream> {
        let last_part = self.last_part();

        match last_part.trim() {
            "NaiveDateTime" => {
                Some(quote! { ::parquet::basic::ConvertedType::TIMESTAMP_MILLIS })
            }
            _ => None,
        }
    }

    fn repetition(&self) -> proc_macro2::TokenStream {
        match self {
            Type::Option(_) => quote! { ::parquet::basic::Repetition::OPTIONAL },
            Type::Reference(_, ty) => ty.repetition(),
            _ => quote! { ::parquet::basic::Repetition::REQUIRED },
        }
    }

    /// Convert a parsed rust field AST in to a more easy to manipulate
    /// parquet_derive::Field
    fn from(f: &syn::Field) -> Self {
        Type::from_type(f, &f.ty)
    }

    fn from_type(f: &syn::Field, ty: &syn::Type) -> Self {
        match ty {
            syn::Type::Path(ref p) => Type::from_type_path(f, p),
            syn::Type::Reference(ref tr) => Type::from_type_reference(f, tr),
            syn::Type::Array(ref ta) => Type::from_type_array(f, ta),
            other => unimplemented!(
                "Unable to derive {:?} - it is currently an unsupported type\n{:#?}",
                f.ident.as_ref().unwrap(),
                other
            ),
        }
    }

    fn from_type_path(f: &syn::Field, p: &syn::TypePath) -> Self {
        let last_segment = p.path.segments.last().unwrap();

        let is_vec =
            last_segment.ident == syn::Ident::new("Vec", proc_macro2::Span::call_site());
        let is_option = last_segment.ident
            == syn::Ident::new("Option", proc_macro2::Span::call_site());

        if is_vec || is_option {
            let generic_type = match &last_segment.arguments {
                syn::PathArguments::AngleBracketed(angle_args) => {
                    assert_eq!(angle_args.args.len(), 1);
                    let first_arg = &angle_args.args[0];

                    match first_arg {
                        syn::GenericArgument::Type(ref typath) => typath.clone(),
                        other => unimplemented!("Unsupported: {:#?}", other),
                    }
                }
                other => unimplemented!("Unsupported: {:#?}", other),
            };

            if is_vec {
                Type::Vec(Box::new(Type::from_type(f, &generic_type)))
            } else {
                Type::Option(Box::new(Type::from_type(f, &generic_type)))
            }
        } else {
            Type::TypePath(syn::Type::Path(p.clone()))
        }
    }

    fn from_type_reference(f: &syn::Field, tr: &syn::TypeReference) -> Self {
        let lifetime = tr.lifetime.clone();
        let inner_type = Type::from_type(f, tr.elem.as_ref());
        Type::Reference(lifetime, Box::new(inner_type))
    }

    fn from_type_array(f: &syn::Field, ta: &syn::TypeArray) -> Self {
        let inner_type = Type::from_type(f, ta.elem.as_ref());
        Type::Array(Box::new(inner_type))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use syn::{self, Data, DataStruct, DeriveInput};

    fn extract_fields(input: proc_macro2::TokenStream) -> Vec<syn::Field> {
        let input: DeriveInput = syn::parse2(input).unwrap();

        let fields = match input.data {
            Data::Struct(DataStruct { fields, .. }) => fields,
            _ => panic!("Input must be a struct"),
        };

        fields.iter().map(|field| field.to_owned()).collect()
    }

    #[test]
    fn test_generating_a_simple_writer_snippet() {
        let snippet: proc_macro2::TokenStream = quote! {
          struct ABoringStruct {
            counter: usize,
          }
        };

        let fields = extract_fields(snippet);
        let counter = Field::from(&fields[0]);

        let snippet = counter.writer_snippet().to_string();
        assert_eq!(snippet,
                   (quote!{
                        {
                            let vals : Vec < _ > = records . iter ( ) . map ( | rec | rec . counter as i64 ) . collect ( );

                            if let ColumnWriter::Int64ColumnWriter ( ref mut typed ) = column_writer.untyped() {
                                typed . write_batch ( & vals [ .. ] , None , None ) ?;
                            }  else {
                                panic!("Schema and struct disagree on type for {}" , stringify!{ counter } )
                            }
                        }
                   }).to_string()
        )
    }

    #[test]
    fn test_optional_to_writer_snippet() {
        let struct_def: proc_macro2::TokenStream = quote! {
          struct StringBorrower<'a> {
            optional_str: Option<&'a str>,
            optional_string: Option<&String>,
            optional_dumb_int: Option<&i32>,
          }
        };

        let fields = extract_fields(struct_def);

        let optional = Field::from(&fields[0]);
        let snippet = optional.writer_snippet();
        assert_eq!(snippet.to_string(),
          (quote! {
          {
                let definition_levels : Vec < i16 > = self . iter ( ) . map ( | rec | if rec . optional_str . is_some ( ) { 1 } else { 0 } ) . collect ( ) ;

                let vals: Vec <_> = records.iter().filter_map( |rec| {
                    if let Some ( ref inner ) = rec . optional_str {
                        Some ( (&inner[..]).into() )
                    } else {
                        None
                    }
                }).collect();

                if let ColumnWriter::ByteArrayColumnWriter ( ref mut typed ) = column_writer.untyped() {
                    typed . write_batch ( & vals [ .. ] , Some(&definition_levels[..]) , None ) ? ;
                } else {
                    panic!("Schema and struct disagree on type for {}" , stringify ! { optional_str } )
                }
           }
            }
          ).to_string());

        let optional = Field::from(&fields[1]);
        let snippet = optional.writer_snippet();
        assert_eq!(snippet.to_string(),
                   (quote!{
                   {
                        let definition_levels : Vec < i16 > = self . iter ( ) . map ( | rec | if rec . optional_string . is_some ( ) { 1 } else { 0 } ) . collect ( ) ;

                        let vals: Vec <_> = records.iter().filter_map( |rec| {
                            if let Some ( ref inner ) = rec . optional_string {
                                Some ( (&inner[..]).into() )
                            } else {
                                None
                            }
                        }).collect();

                        if let ColumnWriter::ByteArrayColumnWriter ( ref mut typed ) = column_writer.untyped() {
                            typed . write_batch ( & vals [ .. ] , Some(&definition_levels[..]) , None ) ? ;
                        } else {
                            panic!("Schema and struct disagree on type for {}" , stringify ! { optional_string } )
                        }
                    }
        }).to_string());

        let optional = Field::from(&fields[2]);
        let snippet = optional.writer_snippet();
        assert_eq!(snippet.to_string(),
                   (quote!{
                    {
                        let definition_levels : Vec < i16 > = self . iter ( ) . map ( | rec | if rec . optional_dumb_int . is_some ( ) { 1 } else { 0 } ) . collect ( ) ;

                        let vals: Vec <_> = records.iter().filter_map( |rec| {
                            if let Some ( inner ) = rec . optional_dumb_int {
                                Some ( inner as i32 )
                            } else {
                                None
                            }
                        }).collect();

                        if let ColumnWriter::Int32ColumnWriter ( ref mut typed ) = column_writer.untyped() {
                            typed . write_batch ( & vals [ .. ] , Some(&definition_levels[..]) , None ) ? ;
                        }  else {
                            panic!("Schema and struct disagree on type for {}" , stringify ! { optional_dumb_int } )
                        }
                    }
        }).to_string());
    }

    #[test]
    fn test_converting_to_column_writer_type() {
        let snippet: proc_macro2::TokenStream = quote! {
          struct ABasicStruct {
            yes_no: bool,
            name: String,
          }
        };

        let fields = extract_fields(snippet);
        let processed: Vec<_> = fields.iter().map(Field::from).collect();

        let column_writers: Vec<_> = processed
            .iter()
            .map(|field| field.ty.column_writer())
            .collect();

        assert_eq!(
            column_writers,
            vec![
                syn::parse_quote!(ColumnWriter::BoolColumnWriter),
                syn::parse_quote!(ColumnWriter::ByteArrayColumnWriter)
            ]
        );
    }

    #[test]
    fn convert_basic_struct() {
        let snippet: proc_macro2::TokenStream = quote! {
          struct ABasicStruct {
            yes_no: bool,
            name: String,
            length: usize
          }
        };

        let fields = extract_fields(snippet);
        let processed: Vec<_> = fields.iter().map(Field::from).collect();
        assert_eq!(processed.len(), 3);

        assert_eq!(
            processed,
            vec![
                Field {
                    ident: syn::Ident::new("yes_no", proc_macro2::Span::call_site()),
                    ty: Type::TypePath(syn::parse_quote!(bool)),
                    is_a_byte_buf: false,
                    third_party_type: None,
                },
                Field {
                    ident: syn::Ident::new("name", proc_macro2::Span::call_site()),
                    ty: Type::TypePath(syn::parse_quote!(String)),
                    is_a_byte_buf: true,
                    third_party_type: None,
                },
                Field {
                    ident: syn::Ident::new("length", proc_macro2::Span::call_site()),
                    ty: Type::TypePath(syn::parse_quote!(usize)),
                    is_a_byte_buf: false,
                    third_party_type: None,
                }
            ]
        )
    }

    #[test]
    fn test_get_inner_type() {
        let snippet: proc_macro2::TokenStream = quote! {
          struct LotsOfInnerTypes {
            a_vec: Vec<u8>,
            a_option: ::std::option::Option<bool>,
            a_silly_string: ::std::string::String,
            a_complicated_thing: ::std::option::Option<::std::result::Result<(),()>>,
          }
        };

        let fields = extract_fields(snippet);
        let converted_fields: Vec<_> = fields.iter().map(Type::from).collect();
        let inner_types: Vec<_> = converted_fields
            .iter()
            .map(|field| field.inner_type())
            .collect();
        let inner_types_strs: Vec<_> = inner_types
            .iter()
            .map(|ty| (quote! { #ty }).to_string())
            .collect();

        assert_eq!(
            inner_types_strs,
            vec![
                "u8",
                "bool",
                ":: std :: string :: String",
                ":: std :: result :: Result < () , () >"
            ]
        )
    }

    #[test]
    fn test_physical_type() {
        use parquet::basic::Type as BasicType;
        let snippet: proc_macro2::TokenStream = quote! {
          struct LotsOfInnerTypes {
            a_buf: ::std::vec::Vec<u8>,
            a_number: i32,
            a_verbose_option: ::std::option::Option<bool>,
            a_silly_string: String,
            a_fix_byte_buf: [u8; 10],
            a_complex_option: ::std::option::Option<&Vec<u8>>,
            a_complex_vec: &::std::vec::Vec<&Option<u8>>,
          }
        };

        let fields = extract_fields(snippet);
        let converted_fields: Vec<_> = fields.iter().map(Type::from).collect();
        let physical_types: Vec<_> = converted_fields
            .iter()
            .map(|ty| ty.physical_type())
            .collect();

        assert_eq!(
            physical_types,
            vec![
                BasicType::BYTE_ARRAY,
                BasicType::INT32,
                BasicType::BOOLEAN,
                BasicType::BYTE_ARRAY,
                BasicType::FIXED_LEN_BYTE_ARRAY,
                BasicType::BYTE_ARRAY,
                BasicType::INT32
            ]
        )
    }

    #[test]
    fn test_convert_comprehensive_owned_struct() {
        let snippet: proc_macro2::TokenStream = quote! {
          struct VecHolder {
            a_vec: ::std::vec::Vec<u8>,
            a_option: ::std::option::Option<bool>,
            a_silly_string: ::std::string::String,
            a_complicated_thing: ::std::option::Option<::std::result::Result<(),()>>,
          }
        };

        let fields = extract_fields(snippet);
        let converted_fields: Vec<_> = fields.iter().map(Type::from).collect();

        assert_eq!(
            converted_fields,
            vec![
                Type::Vec(Box::new(Type::TypePath(syn::parse_quote!(u8)))),
                Type::Option(Box::new(Type::TypePath(syn::parse_quote!(bool)))),
                Type::TypePath(syn::parse_quote!(::std::string::String)),
                Type::Option(Box::new(Type::TypePath(
                    syn::parse_quote!(::std::result::Result<(),()>)
                ))),
            ]
        );
    }

    #[test]
    fn test_convert_borrowed_struct() {
        let snippet: proc_macro2::TokenStream = quote! {
          struct Borrower<'a> {
            a_str: &'a str,
            a_borrowed_option: &'a Option<bool>,
            so_many_borrows: &'a Option<&'a str>,
          }
        };

        let fields = extract_fields(snippet);
        let types: Vec<_> = fields.iter().map(Type::from).collect();

        assert_eq!(
            types,
            vec![
                Type::Reference(
                    Some(syn::Lifetime::new("'a", proc_macro2::Span::call_site())),
                    Box::new(Type::TypePath(syn::parse_quote!(str)))
                ),
                Type::Reference(
                    Some(syn::Lifetime::new("'a", proc_macro2::Span::call_site())),
                    Box::new(Type::Option(Box::new(Type::TypePath(syn::parse_quote!(
                        bool
                    )))))
                ),
                Type::Reference(
                    Some(syn::Lifetime::new("'a", proc_macro2::Span::call_site())),
                    Box::new(Type::Option(Box::new(Type::Reference(
                        Some(syn::Lifetime::new("'a", proc_macro2::Span::call_site())),
                        Box::new(Type::TypePath(syn::parse_quote!(str)))
                    ))))
                ),
            ]
        );
    }

    #[test]
    fn test_chrono_timestamp_millis() {
        let snippet: proc_macro2::TokenStream = quote! {
          struct ATimestampStruct {
            henceforth: chrono::NaiveDateTime,
            maybe_happened: Option<&chrono::NaiveDateTime>,
          }
        };

        let fields = extract_fields(snippet);
        let when = Field::from(&fields[0]);
        assert_eq!(when.writer_snippet().to_string(),(quote!{
            {
                let vals : Vec<_> = records.iter().map(|rec| rec.henceforth.timestamp_millis() ).collect();
                if let ColumnWriter::Int64ColumnWriter(ref mut typed) = column_writer.untyped() {
                    typed.write_batch(&vals[..], None, None) ?;
                } else {
                    panic!("Schema and struct disagree on type for {}" , stringify!{ henceforth })
                }
            }
        }).to_string());

        let maybe_happened = Field::from(&fields[1]);
        assert_eq!(maybe_happened.writer_snippet().to_string(),(quote!{
            {
                let definition_levels : Vec<i16> = self.iter().map(|rec| if rec.maybe_happened.is_some() { 1 } else { 0 }).collect();
                let vals : Vec<_> = records.iter().filter_map(|rec| {
                    if let Some(inner) = rec.maybe_happened {
                        Some(inner.timestamp_millis())
                    } else {
                        None
                    }
                }).collect();

                if let ColumnWriter::Int64ColumnWriter(ref mut typed) = column_writer.untyped() {
                    typed.write_batch(&vals[..], Some(&definition_levels[..]), None) ?;
                } else {
                    panic!("Schema and struct disagree on type for {}" , stringify!{ maybe_happened })
                }
            }
        }).to_string());
    }

    #[test]
    fn test_chrono_date() {
        let snippet: proc_macro2::TokenStream = quote! {
          struct ATimestampStruct {
            henceforth: chrono::NaiveDate,
            maybe_happened: Option<&chrono::NaiveDate>,
          }
        };

        let fields = extract_fields(snippet);
        let when = Field::from(&fields[0]);
        assert_eq!(when.writer_snippet().to_string(),(quote!{
            {
                let vals : Vec<_> = records.iter().map(|rec| rec.henceforth.signed_duration_since(::chrono::NaiveDate::from_ymd(1970, 1, 1)).num_days() as i32).collect();
                if let ColumnWriter::Int32ColumnWriter(ref mut typed) = column_writer.untyped() {
                    typed.write_batch(&vals[..], None, None) ?;
                } else {
                    panic!("Schema and struct disagree on type for {}" , stringify!{ henceforth })
                }
            }
        }).to_string());

        let maybe_happened = Field::from(&fields[1]);
        assert_eq!(maybe_happened.writer_snippet().to_string(),(quote!{
            {
                let definition_levels : Vec<i16> = self.iter().map(|rec| if rec.maybe_happened.is_some() { 1 } else { 0 }).collect();
                let vals : Vec<_> = records.iter().filter_map(|rec| {
                    if let Some(inner) = rec.maybe_happened {
                        Some(inner.signed_duration_since(::chrono::NaiveDate::from_ymd(1970, 1, 1)).num_days() as i32)
                    } else {
                        None
                    }
                }).collect();

                if let ColumnWriter::Int32ColumnWriter(ref mut typed) = column_writer.untyped() {
                    typed.write_batch(&vals[..], Some(&definition_levels[..]), None) ?;
                } else {
                    panic!("Schema and struct disagree on type for {}" , stringify!{ maybe_happened })
                }
            }
        }).to_string());
    }

    #[test]
    fn test_uuid() {
        let snippet: proc_macro2::TokenStream = quote! {
          struct AUuidStruct {
            unique_id: uuid::Uuid,
            maybe_unique_id: Option<&uuid::Uuid>,
          }
        };

        let fields = extract_fields(snippet);
        let when = Field::from(&fields[0]);
        assert_eq!(when.writer_snippet().to_string(),(quote!{
            {
                let vals : Vec<_> = records.iter().map(|rec| (&rec.unique_id.to_string()[..]).into() ).collect();
                if let ColumnWriter::ByteArrayColumnWriter(ref mut typed) = column_writer.untyped() {
                    typed.write_batch(&vals[..], None, None) ?;
                } else {
                    panic!("Schema and struct disagree on type for {}" , stringify!{ unique_id })
                }
            }
        }).to_string());

        let maybe_happened = Field::from(&fields[1]);
        assert_eq!(maybe_happened.writer_snippet().to_string(),(quote!{
            {
                let definition_levels : Vec<i16> = self.iter().map(|rec| if rec.maybe_unique_id.is_some() { 1 } else { 0 }).collect();
                let vals : Vec<_> = records.iter().filter_map(|rec| {
                    if let Some(ref inner) = rec.maybe_unique_id {
                        Some((&inner.to_string()[..]).into())
                    } else {
                        None
                    }
                }).collect();

                if let ColumnWriter::ByteArrayColumnWriter(ref mut typed) = column_writer.untyped() {
                    typed.write_batch(&vals[..], Some(&definition_levels[..]), None) ?;
                } else {
                    panic!("Schema and struct disagree on type for {}" , stringify!{ maybe_unique_id })
                }
            }
        }).to_string());
    }

    #[test]
    fn test_converted_type() {
        let snippet: proc_macro2::TokenStream = quote! {
          struct ATimeStruct {
            time: chrono::NaiveDateTime,
          }
        };

        let fields = extract_fields(snippet);

        let time = Field::from(&fields[0]);

        let converted_type = time.ty.converted_type();
        assert_eq!(
            converted_type.unwrap().to_string(),
            quote! { ::parquet::basic::ConvertedType::TIMESTAMP_MILLIS }.to_string()
        );
    }
}
