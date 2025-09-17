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

use crate::compression::{CompressionCodec, CODEC_METADATA_KEY};
use crate::schema::{AvroSchema, Fingerprint, SCHEMA_METADATA_KEY, SINGLE_OBJECT_MAGIC};
use crate::writer::encoder::write_long;
use arrow_schema::{ArrowError, Schema};
use rand::RngCore;
use std::fmt::Debug;
use std::io::Write;

/// Format abstraction implemented by each container‐level writer.
pub trait AvroFormat: Debug + Default {
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

    /// Return the 10‑byte **Avro single‑object** prefix (`C3 01` magic +
    /// little‑endian schema fingerprint) to be written **before each record**,
    /// or `None` if the format does not use single‑object encoding.
    ///
    /// The default implementation returns `None`. `AvroBinaryFormat` overrides
    /// this to return the appropriate single-object encoding prefix.
    #[inline]
    fn single_object_prefix(&self) -> Option<&[u8]> {
        None
    }
}

/// Avro Object Container File (OCF) format writer.
#[derive(Debug, Default)]
pub struct AvroOcfFormat {
    sync_marker: [u8; 16],
}

impl AvroFormat for AvroOcfFormat {
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
        let avro_schema = AvroSchema::try_from(schema)?;
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
pub struct AvroBinaryFormat {
    /// Pre-built, variable-length prefix written before each record.
    prefix: Vec<u8>,
}

impl AvroFormat for AvroBinaryFormat {
    fn start_stream<W: Write>(
        &mut self,
        _writer: &mut W,
        schema: &Schema,
        compression: Option<CompressionCodec>,
    ) -> Result<(), ArrowError> {
        if compression.is_some() {
            return Err(ArrowError::InvalidArgumentError(
                "Compression not supported for Avro binary streaming (single-object encoding)"
                    .to_string(),
            ));
        }

        if let Some(id_str) = schema.metadata().get("confluent.schema.id") {
            let id: u32 = id_str.parse().map_err(|_| {
                ArrowError::InvalidArgumentError(format!(
                    "Invalid Confluent schema ID in metadata: {id_str}"
                ))
            })?;
            self.prefix.clear();
            self.prefix.push(0x00);
            self.prefix.extend_from_slice(&id.to_be_bytes());
            return Ok(());
        }

        let avro_schema = AvroSchema::try_from(schema)?;
        self.prefix.clear();

        match avro_schema.fingerprint()? {
            // Case 1: Confluent Schema Registry ID format.
            // 1 magic byte (0x00) + 4-byte schema ID (Big Endian).
            Fingerprint::Id(id) => {
                self.prefix.push(0x00);
                self.prefix.extend_from_slice(&id.to_be_bytes());
            }

            // Case 2: Standard single-object encoding with hash-based fingerprints.
            // 2 magic bytes + N-byte fingerprint.
            fp => {
                self.prefix.extend_from_slice(&SINGLE_OBJECT_MAGIC);
                match fp {
                    Fingerprint::Rabin(val) => self.prefix.extend_from_slice(&val.to_le_bytes()),
                    #[cfg(feature = "md5")]
                    Fingerprint::MD5(val) => self.prefix.extend_from_slice(val.as_ref()),
                    #[cfg(feature = "sha256")]
                    Fingerprint::SHA256(val) => self.prefix.extend_from_slice(val.as_ref()),
                    Fingerprint::Id(_) => unreachable!(),
                }
            }
        }
        Ok(())
    }

    fn sync_marker(&self) -> Option<&[u8; 16]> {
        None
    }

    fn single_object_prefix(&self) -> Option<&[u8]> {
        if self.prefix.is_empty() {
            None
        } else {
            Some(&self.prefix)
        }
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
