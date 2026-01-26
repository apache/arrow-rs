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

use crate::codec::AvroFieldBuilder;
use crate::reader::async_reader::ReaderState;
use crate::reader::header::{Header, HeaderDecoder};
use crate::reader::record::RecordDecoder;
use crate::reader::{AsyncAvroFileReader, AsyncFileReader, Decoder};
use crate::schema::{AvroSchema, FingerprintAlgorithm, SCHEMA_METADATA_KEY};
use arrow_schema::ArrowError;
use indexmap::IndexMap;
use std::ops::Range;

const DEFAULT_HEADER_SIZE_HINT: u64 = 16 * 1024; // 16 KB

/// Builder for an asynchronous Avro file reader.
pub struct AsyncAvroFileReaderBuilder<R> {
    reader: R,
    file_size: u64,
    batch_size: usize,
    range: Option<Range<u64>>,
    reader_schema: Option<AvroSchema>,
    projection: Option<Vec<usize>>,
    header_size_hint: Option<u64>,
    utf8_view: bool,
    strict_mode: bool,
}

impl<R> AsyncAvroFileReaderBuilder<R> {
    pub(super) fn new(reader: R, file_size: u64, batch_size: usize) -> Self {
        Self {
            reader,
            file_size,
            batch_size,
            range: None,
            reader_schema: None,
            projection: None,
            header_size_hint: None,
            utf8_view: false,
            strict_mode: false,
        }
    }

    /// Specify a byte range to read from the Avro file.
    /// If this is provided, the reader will read all the blocks within the specified range,
    /// if the range ends mid-block, it will attempt to fetch the remaining bytes to complete the block,
    /// but no further blocks will be read.
    /// If this is omitted, the full file will be read.
    pub fn with_range(self, range: Range<u64>) -> Self {
        Self {
            range: Some(range),
            ..self
        }
    }

    /// Specify a reader schema to use when reading the Avro file.
    /// This can be useful to project specific columns or handle schema evolution.
    /// If this is not provided, the schema will be derived from the Arrow schema provided.
    pub fn with_reader_schema(self, reader_schema: AvroSchema) -> Self {
        Self {
            reader_schema: Some(reader_schema),
            ..self
        }
    }

    /// Specify a projection of column indices to read from the Avro file.
    /// This can help optimize reading by only fetching the necessary columns.
    pub fn with_projection(self, projection: Vec<usize>) -> Self {
        Self {
            projection: Some(projection),
            ..self
        }
    }

    /// Provide a hint for the expected size of the Avro header in bytes.
    /// This can help optimize the initial read operation when fetching the header.
    pub fn with_header_size_hint(self, hint: u64) -> Self {
        Self {
            header_size_hint: Some(hint),
            ..self
        }
    }

    /// Enable usage of Utf8View types when reading string data.
    pub fn with_utf8_view(self, utf8_view: bool) -> Self {
        Self { utf8_view, ..self }
    }

    /// Enable strict mode for schema validation and data reading.
    pub fn with_strict_mode(self, strict_mode: bool) -> Self {
        Self {
            strict_mode,
            ..self
        }
    }
}

impl<R: AsyncFileReader> AsyncAvroFileReaderBuilder<R> {
    async fn read_header(&mut self) -> Result<(Header, u64), ArrowError> {
        let mut decoder = HeaderDecoder::default();
        let mut position = 0;
        loop {
            let range_to_fetch = position
                ..(position + self.header_size_hint.unwrap_or(DEFAULT_HEADER_SIZE_HINT))
                    .min(self.file_size);
            let current_data = self.reader.get_bytes(range_to_fetch).await.map_err(|err| {
                ArrowError::AvroError(format!(
                    "Error fetching Avro header from object store: {err}"
                ))
            })?;
            if current_data.is_empty() {
                break;
            }
            let read = current_data.len();
            let decoded = decoder.decode(&current_data)?;
            if decoded != read {
                position += decoded as u64;
                break;
            }
            position += read as u64;
        }

        decoder
            .flush()
            .map(|header| (header, position))
            .ok_or_else(|| ArrowError::AvroError("Unexpected EOF while reading Avro header".into()))
    }

    /// Build the asynchronous Avro reader with the provided parameters.
    /// This reads the header first to initialize the reader state.
    pub async fn try_build(mut self) -> Result<AsyncAvroFileReader<R>, ArrowError> {
        if self.file_size == 0 {
            return Err(ArrowError::AvroError("File size cannot be 0".into()));
        }

        // Start by reading the header from the beginning of the avro file
        // take the writer schema from the header
        let (header, header_len) = self.read_header().await?;
        let writer_schema = {
            let raw = header.get(SCHEMA_METADATA_KEY).ok_or_else(|| {
                ArrowError::ParseError("No Avro schema present in file header".to_string())
            })?;
            let json_string = std::str::from_utf8(raw)
                .map_err(|e| {
                    ArrowError::ParseError(format!("Invalid UTF-8 in Avro schema header: {e}"))
                })?
                .to_string();
            AvroSchema::new(json_string)
        };

        // If projection exists, project the reader schema,
        // if no reader schema is provided, parse it from the header(get the raw writer schema), and project that
        // this projected schema will be the schema used for reading.
        let projected_reader_schema = self
            .projection
            .as_deref()
            .map(|projection| {
                let base_schema = if let Some(reader_schema) = &self.reader_schema {
                    reader_schema
                } else {
                    &writer_schema
                };
                base_schema.project(projection)
            })
            .transpose()?;

        // Use either the projected reader schema or the original reader schema(if no projection)
        // (both optional, at worst no reader schema is provided, in which case we read with the writer schema)
        let effective_reader_schema = projected_reader_schema
            .as_ref()
            .or(self.reader_schema.as_ref())
            .map(|s| s.schema())
            .transpose()?;

        let root = {
            let writer_schema = writer_schema.schema()?;
            let mut builder = AvroFieldBuilder::new(&writer_schema);
            if let Some(reader_schema) = &effective_reader_schema {
                builder = builder.with_reader_schema(reader_schema);
            }
            builder
                .with_utf8view(self.utf8_view)
                .with_strict_mode(self.strict_mode)
                .build()
        }?;

        let record_decoder = RecordDecoder::try_new_with_options(root.data_type())?;
        let decoder = Decoder::from_parts(
            self.batch_size,
            record_decoder,
            None,
            IndexMap::new(),
            FingerprintAlgorithm::Rabin,
        );
        let range = match self.range {
            Some(r) => {
                // If this PartitionedFile's range starts at 0, we need to skip the header bytes.
                // But then we need to seek back 16 bytes to include the sync marker for the first block,
                // as the logic in this reader searches the data for the first sync marker(after which a block starts),
                // then reads blocks from the count, size etc.
                let start = r.start.max(header_len.checked_sub(16).ok_or(ArrowError::ParseError("Avro header length overflow, header was not long enough to contain avro bytes".to_string()))?);
                let end = r.end.max(start).min(self.file_size); // Ensure end is not less than start, worst case range is empty
                start..end
            }
            None => 0..self.file_size,
        };

        let reader_state = if range.start == range.end {
            ReaderState::Finished
        } else {
            ReaderState::Idle {
                reader: self.reader,
            }
        };
        let codec = header.compression()?;
        let sync_marker = header.sync();

        Ok(AsyncAvroFileReader::new(
            range,
            self.file_size,
            decoder,
            codec,
            sync_marker,
            reader_state,
        ))
    }
}
