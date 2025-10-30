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

//! Avro Writer Formats for Arrow.

use crate::compression::{CODEC_METADATA_KEY, CompressionCodec};
use crate::schema::{AvroSchema, AvroSchemaOptions, SCHEMA_METADATA_KEY};
use crate::writer::encoder::write_long;
use arrow_schema::{ArrowError, Schema};
use rand::RngCore;
use std::fmt::Debug;
use std::io::Write;

/// Format abstraction implemented by each container‐level writer.
pub trait AvroFormat: Debug + Default {
    /// If `true`, the writer for this format will query `single_object_prefix()`
    /// and write the prefix before each record. If `false`, the writer can
    /// skip this step. This is a performance hint for the writer.
    const NEEDS_PREFIX: bool;

    /// Write any bytes required at the very beginning of the output stream
    /// (file header, etc.).
    /// Implementations **must not** write any record data.
    fn start_stream<W: Write>(
        &mut self,
        writer: &mut W,
        schema: &Schema,
        compression: Option<CompressionCodec>,
    ) -> Result<(), ArrowError>;

    /// Return the 16‑byte sync marker (OCF) or `None` (binary stream).
    fn sync_marker(&self) -> Option<&[u8; 16]>;
}

/// Avro Object Container File (OCF) format writer.
#[derive(Debug, Default)]
pub struct AvroOcfFormat {
    sync_marker: [u8; 16],
}

impl AvroFormat for AvroOcfFormat {
    const NEEDS_PREFIX: bool = false;
    fn start_stream<W: Write>(
        &mut self,
        writer: &mut W,
        schema: &Schema,
        compression: Option<CompressionCodec>,
    ) -> Result<(), ArrowError> {
        let mut rng = rand::rng();
        rng.fill_bytes(&mut self.sync_marker);
        // Choose the Avro schema JSON that the file will advertise.
        // If `schema.metadata[SCHEMA_METADATA_KEY]` exists, AvroSchema::try_from
        // uses it verbatim; otherwise it is generated from the Arrow schema.
        let avro_schema = AvroSchema::from_arrow_with_options(
            schema,
            Some(AvroSchemaOptions {
                null_order: None,
                strip_metadata: true,
            }),
        )?;
        // Magic
        writer
            .write_all(b"Obj\x01")
            .map_err(|e| ArrowError::IoError(format!("write OCF magic: {e}"), e))?;
        // File metadata map: { "avro.schema": <json>, "avro.codec": <codec> }
        let codec_str = match compression {
            Some(CompressionCodec::Deflate) => "deflate",
            Some(CompressionCodec::Snappy) => "snappy",
            Some(CompressionCodec::ZStandard) => "zstandard",
            Some(CompressionCodec::Bzip2) => "bzip2",
            Some(CompressionCodec::Xz) => "xz",
            None => "null",
        };
        // Map block: count=2, then key/value pairs, then terminating count=0
        write_long(writer, 2)?;
        write_string(writer, SCHEMA_METADATA_KEY)?;
        write_bytes(writer, avro_schema.json_string.as_bytes())?;
        write_string(writer, CODEC_METADATA_KEY)?;
        write_bytes(writer, codec_str.as_bytes())?;
        write_long(writer, 0)?;
        // Sync marker (16 bytes)
        writer
            .write_all(&self.sync_marker)
            .map_err(|e| ArrowError::IoError(format!("write OCF sync marker: {e}"), e))?;
        Ok(())
    }

    fn sync_marker(&self) -> Option<&[u8; 16]> {
        Some(&self.sync_marker)
    }
}

/// Raw Avro binary streaming format using **Single-Object Encoding** per record.
///
/// Each record written by the stream writer is framed with a prefix determined
/// by the schema fingerprinting algorithm.
///
/// See: <https://avro.apache.org/docs/1.11.1/specification/#single-object-encoding>
/// See: <https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format>
#[derive(Debug, Default)]
pub struct AvroSoeFormat {}

impl AvroFormat for AvroSoeFormat {
    const NEEDS_PREFIX: bool = true;
    fn start_stream<W: Write>(
        &mut self,
        _writer: &mut W,
        _schema: &Schema,
        compression: Option<CompressionCodec>,
    ) -> Result<(), ArrowError> {
        if compression.is_some() {
            return Err(ArrowError::InvalidArgumentError(
                "Compression not supported for Avro SOE streaming".to_string(),
            ));
        }
        Ok(())
    }

    fn sync_marker(&self) -> Option<&[u8; 16]> {
        None
    }
}

#[inline]
fn write_string<W: Write>(writer: &mut W, s: &str) -> Result<(), ArrowError> {
    write_bytes(writer, s.as_bytes())
}

#[inline]
fn write_bytes<W: Write>(writer: &mut W, bytes: &[u8]) -> Result<(), ArrowError> {
    write_long(writer, bytes.len() as i64)?;
    writer
        .write_all(bytes)
        .map_err(|e| ArrowError::IoError(format!("write bytes: {e}"), e))
}
