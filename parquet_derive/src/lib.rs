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

//! This crate provides a procedural macro to derive
//! implementations of a RecordWriter and RecordReader

#![warn(missing_docs)]
#![recursion_limit = "128"]

extern crate proc_macro;
extern crate proc_macro2;
extern crate syn;
#[macro_use]
extern crate quote;

extern crate parquet;

use ::syn::{parse_macro_input, Data, DataStruct, DeriveInput};

mod parquet_field;

/// Derive flat, simple RecordWriter implementations.
///
/// Works by parsing a struct tagged with `#[derive(ParquetRecordWriter)]` and emitting
/// the correct writing code for each field of the struct. Column writers
/// are generated in the order they are defined.
///
/// It is up to the programmer to keep the order of the struct
/// fields lined up with the schema.
///
/// Example:
///
/// ```no_run
/// use parquet_derive::ParquetRecordWriter;
/// use std::io::{self, Write};
/// use parquet::file::properties::WriterProperties;
/// use parquet::file::writer::SerializedFileWriter;
/// use parquet::record::RecordWriter;
/// use std::fs::File;
///
/// use std::sync::Arc;
///
/// #[derive(ParquetRecordWriter)]
/// struct ACompleteRecord<'a> {
///   pub a_bool: bool,
///   pub a_str: &'a str,
/// }
///
/// pub fn write_some_records() {
///   let samples = vec![
///     ACompleteRecord {
///       a_bool: true,
///       a_str: "I'm true"
///     },
///     ACompleteRecord {
///       a_bool: false,
///       a_str: "I'm false"
///     }
///   ];
///  let file = File::open("some_file.parquet").unwrap();
///
///  let schema = samples.as_slice().schema().unwrap();
///
///  let mut writer = SerializedFileWriter::new(file, schema, Default::default()).unwrap();
///
///  let mut row_group = writer.next_row_group().unwrap();
///  samples.as_slice().write_to_row_group(&mut row_group).unwrap();
///  row_group.close().unwrap();
///  writer.close().unwrap();
/// }
/// ```
///
#[proc_macro_derive(ParquetRecordWriter)]
pub fn parquet_record_writer(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input: DeriveInput = parse_macro_input!(input as DeriveInput);
    let fields = match input.data {
        Data::Struct(DataStruct { fields, .. }) => fields,
        Data::Enum(_) => unimplemented!("Enum currently is not supported"),
        Data::Union(_) => unimplemented!("Union currently is not supported"),
    };

    let field_infos: Vec<_> = fields.iter().map(parquet_field::Field::from).collect();

    let writer_snippets: Vec<proc_macro2::TokenStream> =
        field_infos.iter().map(|x| x.writer_snippet()).collect();

    let derived_for = input.ident;
    let generics = input.generics;

    let field_types: Vec<proc_macro2::TokenStream> =
        field_infos.iter().map(|x| x.parquet_type()).collect();

    (quote! {
    impl #generics ::parquet::record::RecordWriter<#derived_for #generics> for &[#derived_for #generics] {
      fn write_to_row_group<W: ::std::io::Write + Send>(
        &self,
        row_group_writer: &mut ::parquet::file::writer::SerializedRowGroupWriter<'_, W>
      ) -> Result<(), ::parquet::errors::ParquetError> {
        use ::parquet::column::writer::ColumnWriter;

        let mut row_group_writer = row_group_writer;
        let records = &self; // Used by all the writer snippets to be more clear

        #(
          {
              let mut some_column_writer = row_group_writer.next_column().unwrap();
              if let Some(mut column_writer) = some_column_writer {
                  #writer_snippets
                  column_writer.close()?;
              } else {
                  return Err(::parquet::errors::ParquetError::General("Failed to get next column".into()))
              }
          }
        );*

        Ok(())
      }

      fn schema(&self) -> Result<::parquet::schema::types::TypePtr, ::parquet::errors::ParquetError> {
        use ::parquet::schema::types::Type as ParquetType;
        use ::parquet::schema::types::TypePtr;
        use ::parquet::basic::LogicalType;

        let mut fields: ::std::vec::Vec<TypePtr> = ::std::vec::Vec::new();
        #(
          #field_types
        );*;
        let group = ParquetType::group_type_builder("rust_schema")
          .with_fields(fields)
          .build()?;
        Ok(group.into())
      }
    }
  }).into()
}

/// Derive flat, simple RecordReader implementations.
///
/// Works by parsing a struct tagged with `#[derive(ParquetRecordReader)]` and emitting
/// the correct writing code for each field of the struct. Column readers
/// are generated by matching names in the schema to the names in the struct.
///
/// It is up to the programmer to ensure the names in the struct
/// fields line up with the schema.
///
/// Example:
///
/// ```no_run
/// use parquet::record::RecordReader;
/// use parquet::file::{serialized_reader::SerializedFileReader, reader::FileReader};
/// use parquet_derive::{ParquetRecordReader};
/// use std::fs::File;
///
/// #[derive(ParquetRecordReader)]
/// struct ACompleteRecord {
///     pub a_bool: bool,
///     pub a_string: String,
/// }
///
/// pub fn read_some_records() -> Vec<ACompleteRecord> {
///   let mut samples: Vec<ACompleteRecord> = Vec::new();
///   let file = File::open("some_file.parquet").unwrap();
///
///   let reader = SerializedFileReader::new(file).unwrap();
///   let mut row_group = reader.get_row_group(0).unwrap();
///   samples.read_from_row_group(&mut *row_group, 1).unwrap();
///   samples
/// }
/// ```
///
#[proc_macro_derive(ParquetRecordReader)]
pub fn parquet_record_reader(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input: DeriveInput = parse_macro_input!(input as DeriveInput);
    let fields = match input.data {
        Data::Struct(DataStruct { fields, .. }) => fields,
        Data::Enum(_) => unimplemented!("Enum currently is not supported"),
        Data::Union(_) => unimplemented!("Union currently is not supported"),
    };

    let field_infos: Vec<_> = fields.iter().map(parquet_field::Field::from).collect();
    let field_names: Vec<_> = fields.iter().map(|f| f.ident.clone()).collect();
    let reader_snippets: Vec<proc_macro2::TokenStream> =
        field_infos.iter().map(|x| x.reader_snippet()).collect();

    let derived_for = input.ident;
    let generics = input.generics;

    (quote! {

    impl #generics ::parquet::record::RecordReader<#derived_for #generics> for Vec<#derived_for #generics> {
      fn read_from_row_group(
        &mut self,
        row_group_reader: &mut dyn ::parquet::file::reader::RowGroupReader,
        num_records: usize,
      ) -> Result<(), ::parquet::errors::ParquetError> {
        use ::parquet::column::reader::ColumnReader;

        let mut row_group_reader = row_group_reader;

        // key: parquet file column name, value: column index
        let mut name_to_index = std::collections::HashMap::new();
        for (idx, col) in row_group_reader.metadata().schema_descr().columns().iter().enumerate() {
            name_to_index.insert(col.name().to_string(), idx);
        }

        for _ in 0..num_records {
          self.push(#derived_for {
            #(
              #field_names: Default::default()
            ),*
          })
        }

        let records = self; // Used by all the reader snippets to be more clear

        #(
          {
              let idx: usize = match name_to_index.get(stringify!(#field_names)) {
                Some(&col_idx) => col_idx,
                None => {
                  let error_msg = format!("column name '{}' is not found in parquet file!", stringify!(#field_names));
                  return Err(::parquet::errors::ParquetError::General(error_msg));
                }
              };
              if let Ok(mut column_reader) = row_group_reader.get_column_reader(idx) {
                  #reader_snippets
              } else {
                  return Err(::parquet::errors::ParquetError::General("Failed to get next column".into()))
              }
          }
        );*

        Ok(())
      }
    }
  }).into()
}
