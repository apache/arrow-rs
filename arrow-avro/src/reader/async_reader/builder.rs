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
use crate::errors::AvroError;
use crate::reader::async_reader::ReaderState;
use crate::reader::header::{Header, HeaderDecoder};
use crate::reader::record::RecordDecoder;
use crate::reader::{AsyncAvroFileReader, AsyncFileReader, Decoder};
use crate::schema::{AvroSchema, FingerprintAlgorithm, SCHEMA_METADATA_KEY};
use indexmap::IndexMap;
use std::ops::Range;

const DEFAULT_HEADER_SIZE_HINT: u64 = 16 * 1024; // 16 KB

/// Builder for an asynchronous Avro file reader.
pub struct ReaderBuilder<R> {
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

impl<R> ReaderBuilder<R> {
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

impl<R> ReaderBuilder<R>
where
    R: AsyncFileReader,
{
    /// Reads the Avro file header asynchronously to extract metadata.
    ///
    /// The returned builder contains the parsed header information.
    pub async fn read_header(mut self) -> Result<ReaderBuilderWithHeaderInfo<R>, AvroError> {
        if self.file_size == 0 {
            return Err(AvroError::InvalidArgument("File size cannot be 0".into()));
        }

        let mut decoder = HeaderDecoder::default();
        let mut position = 0;
        loop {
            let range_to_fetch = position
                ..(position + self.header_size_hint.unwrap_or(DEFAULT_HEADER_SIZE_HINT))
                    .min(self.file_size);

            // Maybe EOF after the header, no actual data
            if range_to_fetch.is_empty() {
                break;
            }

            let current_data = self
                .reader
                .get_bytes(range_to_fetch.clone())
                .await
                .map_err(|err| {
                    AvroError::General(format!(
                        "Error fetching Avro header from file reader: {err}"
                    ))
                })?;
            if current_data.is_empty() {
                return Err(AvroError::EOF(
                    "Unexpected EOF while fetching header data".into(),
                ));
            }

            let read = current_data.len();
            let decoded = decoder.decode(&current_data)?;
            if decoded != read {
                position += decoded as u64;
                break;
            }
            position += read as u64;
        }

        let header = decoder.flush().ok_or_else(|| {
            AvroError::ParseError("Unexpected EOF while reading Avro header".into())
        })?;
        Ok(ReaderBuilderWithHeaderInfo::new(self, header, position))
    }

    /// Build the asynchronous Avro reader with the provided parameters.
    /// This reads the header first to initialize the reader state.
    pub async fn try_build(self) -> Result<AsyncAvroFileReader<R>, AvroError> {
        // Start by reading the header from the beginning of the avro file
        // take the writer schema from the header
        let builder_with_header = self.read_header().await?;
        builder_with_header.try_build().await
    }
}

/// Intermediate builder struct that holds the writer schema and header length
/// parsed from the file.
pub struct ReaderBuilderWithHeaderInfo<R> {
    inner: ReaderBuilder<R>,
    header: Header,
    header_len: u64,
}

impl<R> ReaderBuilderWithHeaderInfo<R> {
    fn new(inner: ReaderBuilder<R>, header: Header, header_len: u64) -> Self {
        Self {
            inner,
            header,
            header_len,
        }
    }

    /// Returns the writer schema parsed from the Avro file header.
    pub fn writer_schema(&self) -> Result<AvroSchema, AvroError> {
        let raw = self.header.get(SCHEMA_METADATA_KEY).ok_or_else(|| {
            AvroError::ParseError("No Avro schema present in file header".to_string())
        })?;
        let json_string = std::str::from_utf8(raw)
            .map_err(|e| {
                AvroError::ParseError(format!("Invalid UTF-8 in Avro schema header: {e}"))
            })?
            .to_string();
        Ok(AvroSchema::new(json_string))
    }

    /// Sets the reader schema used during decoding.
    ///
    /// If not provided, the writer schema from the OCF header is used directly.
    ///
    /// A reader schema can be used for schema evolution or projection.
    pub fn with_reader_schema(mut self, reader_schema: AvroSchema) -> Self {
        self.inner = self.inner.with_reader_schema(reader_schema);
        self
    }

    /// Specify a projection of column indices to read from the Avro file.
    /// This can help optimize reading by only fetching the necessary columns.
    pub fn with_projection(mut self, projection: Vec<usize>) -> Self {
        self.inner = self.inner.with_projection(projection);
        self
    }

    /// Build the asynchronous Avro reader with the provided parameters.
    pub async fn try_build(self) -> Result<AsyncAvroFileReader<R>, AvroError> {
        // Take the writer schema from the header
        let writer_schema = self.writer_schema()?;

        // If projection exists, project the reader schema,
        // if no reader schema is provided, project the writer schema.
        // this projected schema will be the schema used for reading.
        let projected_reader_schema = self
            .inner
            .projection
            .as_deref()
            .map(|projection| {
                let base_schema = if let Some(reader_schema) = &self.inner.reader_schema {
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
            .or(self.inner.reader_schema.as_ref())
            .map(|s| s.schema())
            .transpose()?;

        let root = {
            let writer_schema = writer_schema.schema()?;
            let mut builder = AvroFieldBuilder::new(&writer_schema);
            if let Some(reader_schema) = &effective_reader_schema {
                builder = builder.with_reader_schema(reader_schema);
            }
            builder
                .with_utf8view(self.inner.utf8_view)
                .with_strict_mode(self.inner.strict_mode)
                .build()
        }?;

        let record_decoder = RecordDecoder::try_new_with_options(root.data_type())?;
        let decoder = Decoder::from_parts(
            self.inner.batch_size,
            record_decoder,
            None,
            IndexMap::new(),
            FingerprintAlgorithm::Rabin,
        );
        let range = match self.inner.range {
            Some(r) => {
                // If this PartitionedFile's range starts at 0, we need to skip the header bytes.
                // But then we need to seek back 16 bytes to include the sync marker for the first block,
                // as the logic in this reader searches the data for the first sync marker(after which a block starts),
                // then reads blocks from the count, size etc.
                let start = r.start.max(self.header_len.checked_sub(16).ok_or(AvroError::ParseError("Avro header length overflow, header was not long enough to contain avro bytes".to_string()))?);
                let end = r.end.max(start).min(self.inner.file_size); // Ensure end is not less than start, worst case range is empty
                start..end
            }
            None => 0..self.inner.file_size,
        };

        // Determine if there is actually data to fetch, note that we subtract the header len from range.start,
        // so we need to check if range.end == header_len to see if there's no data after the header
        let reader_state = if range.start == range.end || self.header_len == range.end {
            ReaderState::Finished
        } else {
            ReaderState::Idle {
                reader: self.inner.reader,
            }
        };
        let codec = self.header.compression()?;
        let sync_marker = self.header.sync();

        Ok(AsyncAvroFileReader::new(
            range,
            self.inner.file_size,
            decoder,
            codec,
            sync_marker,
            reader_state,
        ))
    }
}
