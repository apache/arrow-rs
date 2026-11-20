// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Annotate a schema field with its measurement unit and check it before reading rows.
//!
//! A DOUBLE column named `speed` does not distinguish metres per second from
//! kilometres per hour. This example attaches the declared unit to that field's
//! `SchemaElement`, so it applies to the column across row groups. A listener
//! rejects an incompatible annotation while the standard reader loads the footer.
//!
//! Run with `cargo run -p parquet --example metadata_extensions --features experimental`.

use bytes::Bytes;
use parquet::column::reader::get_typed_column_reader;
use parquet::data_type::DoubleType;
use parquet::errors::{ParquetError, Result};
use parquet::file::metadata::extensions::{
    MetadataExtensionHook, MetadataExtensionListener, MetadataExtensionSelection, MetadataSite,
    ThriftPathSegment, ThriftStruct,
};
use parquet::file::metadata::thrift_fields::file_meta_data;
use parquet::file::properties::{ReaderProperties, WriterProperties};
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::{ReadOptionsBuilder, SerializedFileReader};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use std::sync::Arc;

// Fixed UUID for this unit-annotation format; choose your own for other formats.
const EXTENSION_ID: [u8; 16] = 0xecf0c105_c24a_4d05_9667_3e80d14c2cfdu128.to_be_bytes();
const VALUES: [f64; 3] = [1.0, 3.5, 10.0];

fn is_speed_schema_element(site: &mut MetadataSite<'_>) -> Result<bool> {
    use ThriftPathSegment::{Element, Field};
    // For this flat schema, entry 0 is the root and entry 1 is `speed`.
    // This is a flattened schema index, not a leaf-column ordinal.
    Ok(site.root() == ThriftStruct::FileMetaData
        && site
            .resolve_path()?
            .iter()
            .eq([Field(file_meta_data::SCHEMA), Element(1)]))
}

struct SpeedUnitHook {
    payload: Vec<u8>,
}

impl MetadataExtensionHook for SpeedUnitHook {
    fn on_site(&mut self, site: &mut MetadataSite<'_>) -> Result<Option<&[u8]>> {
        Ok(is_speed_schema_element(site)?.then_some(self.payload.as_slice()))
    }
}

struct SpeedUnitObserver {
    expected_unit: &'static str,
}

impl MetadataExtensionListener for SpeedUnitObserver {
    fn on_extension(&self, site: &mut MetadataSite<'_>, payload: &[u8]) -> Result<()> {
        let Some(unit) = payload.strip_prefix(&EXTENSION_ID) else {
            return Ok(());
        };
        if is_speed_schema_element(site)? {
            let unit = std::str::from_utf8(unit)
                .map_err(|error| ParquetError::External(Box::new(error)))?;
            if unit != self.expected_unit {
                return Err(ParquetError::General(format!(
                    "speed unit is {unit}, expected {}",
                    self.expected_unit
                )));
            }
        }
        Ok(())
    }
}

fn write_file() -> Result<Bytes> {
    let schema = Arc::new(parse_message_type(
        "message measurements { REQUIRED DOUBLE speed; }",
    )?);
    let properties = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .build();
    let mut payload = EXTENSION_ID.to_vec();
    payload.extend_from_slice(b"m/s");
    let mut writer = SerializedFileWriter::new(Vec::new(), schema, Arc::new(properties))?
        .with_metadata_extension_hook(
            [MetadataExtensionSelection::roots([
                ThriftStruct::FileMetaData,
            ])],
            SpeedUnitHook { payload },
        )?;
    let mut group = writer.next_row_group()?;
    let mut column = group.next_column()?.expect("speed column");
    column
        .typed::<DoubleType>()
        .write_batch(&VALUES, None, None)?;
    column.close()?;
    group.close()?;
    Ok(Bytes::from(writer.into_inner()?))
}

fn open_file(bytes: Bytes, expected_unit: &'static str) -> Result<SerializedFileReader<Bytes>> {
    let properties = ReaderProperties::builder()
        .set_metadata_extension_listener(
            [MetadataExtensionSelection::roots([
                ThriftStruct::FileMetaData,
            ])],
            SpeedUnitObserver { expected_unit },
        )?
        .build();
    SerializedFileReader::new_with_options(
        bytes,
        ReadOptionsBuilder::new()
            .with_reader_properties(properties)
            .build(),
    )
}

fn main() -> Result<()> {
    let bytes = write_file()?;
    let reader = open_file(bytes.clone(), "m/s")?;
    let group = reader.get_row_group(0)?;
    let mut column = get_typed_column_reader::<DoubleType>(group.get_column_reader(0)?);
    let mut values = Vec::new();
    let (rows, _, _) = column.read_records(VALUES.len() + 1, None, None, &mut values)?;
    assert_eq!(rows, VALUES.len());
    assert_eq!(values, VALUES);
    println!("Accepted speed values in m/s: {values:?}");

    let Err(error) = open_file(bytes, "km/h") else {
        panic!("Expected the incompatible unit to be rejected during footer loading");
    };
    assert_eq!(
        error.to_string(),
        "Parquet error: speed unit is m/s, expected km/h"
    );
    println!("Rejected before reading rows: {error}");
    Ok(())
}
