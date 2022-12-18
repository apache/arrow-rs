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

use arrow_schema::ArrowError;
use csv_core::{ReadRecordResult, Reader};
use std::io::BufRead;

/// The estimated length of a field in bytes
const AVERAGE_FIELD_SIZE: usize = 8;

/// The minimum amount of data in a single read
const MIN_CAPACITY: usize = 1024;

pub struct RecordReader<R> {
    reader: R,
    delimiter: Reader,

    num_columns: usize,

    num_rows: usize,
    offsets: Vec<usize>,
    data: Vec<u8>,
}

impl<R: BufRead> RecordReader<R> {
    pub fn new(reader: R, delimiter: Reader, num_columns: usize) -> Self {
        Self {
            reader,
            delimiter,
            num_columns,
            num_rows: 0,
            offsets: vec![],
            data: vec![],
        }
    }

    fn fill_buf(&mut self, to_read: usize) -> Result<(), ArrowError> {
        // Reserve sufficient capacity in offsets
        self.offsets.resize(to_read * self.num_columns + 1, 0);
        self.num_rows = 0;

        if to_read == 0 {
            return Ok(());
        }

        // The current offset into `self.data`
        let mut output_offset = 0;
        // The current offset into `input`
        let mut input_offset = 0;
        // The current offset into `self.offsets`
        let mut field_offset = 1;
        // The number of fields read for the current row
        let mut field_count = 0;

        'outer: loop {
            let input = self.reader.fill_buf()?;

            'input: loop {
                // Reserve necessary space in output data based on best estimate
                let remaining_rows = to_read - self.num_rows;
                let capacity = remaining_rows * self.num_columns * AVERAGE_FIELD_SIZE;
                let estimated_data = capacity.max(MIN_CAPACITY);
                self.data.resize(output_offset + estimated_data, 0);

                loop {
                    let (result, bytes_read, bytes_written, end_positions) =
                        self.delimiter.read_record(
                            &input[input_offset..],
                            &mut self.data[output_offset..],
                            &mut self.offsets[field_offset..],
                        );

                    field_count += end_positions;
                    field_offset += end_positions;
                    input_offset += bytes_read;
                    output_offset += bytes_written;

                    match result {
                        ReadRecordResult::End => break 'outer, // Reached end of file
                        ReadRecordResult::InputEmpty => break 'input, // Input exhausted, need to read more
                        ReadRecordResult::OutputFull => break, // Need to allocate more capacity
                        ReadRecordResult::OutputEndsFull => {
                            return Err(ArrowError::CsvError(format!("incorrect number of fields, expected {} got more than {}", self.num_columns, field_count)))
                        }
                        ReadRecordResult::Record => {
                            if field_count != self.num_columns {
                                return Err(ArrowError::CsvError(format!("incorrect number of fields, expected {} got {}", self.num_columns, field_count)))
                            }
                            self.num_rows += 1;
                            field_count = 0;

                            if self.num_rows == to_read {
                                break 'outer // Read sufficient rows
                            }

                            if input.len() == input_offset {
                                // Input exhausted, need to read more
                                // Without this read_record will interpret the empty input
                                // byte array as indicating the end of the file
                                break 'input
                            }
                        }
                    }
                }
            }
            self.reader.consume(input_offset);
            input_offset = 0;
        }
        self.reader.consume(input_offset);

        // csv_core::Reader writes end offsets relative to the start of the row
        // Therefore scan through and offset these based on the cumulative row offsets
        let mut row_offset = 0;
        self.offsets[1..]
            .chunks_mut(self.num_columns)
            .for_each(|row| {
                let offset = row_offset;
                row.iter_mut().for_each(|x| {
                    *x += offset;
                    row_offset = *x;
                });
            });

        Ok(())
    }

    /// Skips forward `to_skip` rows
    pub fn skip(&mut self, mut to_skip: usize) -> Result<(), ArrowError> {
        // TODO: This could be done by scanning for unquoted newline delimiters
        while to_skip != 0 {
            self.fill_buf(to_skip.min(1024))?;
            to_skip -= self.num_rows;
        }
        Ok(())
    }

    /// Reads up to `to_read` rows from the reader
    pub fn read(&mut self, to_read: usize) -> Result<StringRecords<'_>, ArrowError> {
        self.fill_buf(to_read)?;

        // Need to slice fields to the actual number of rows read
        //
        // We intentionally avoid using `Vec::truncate` to avoid having
        // to re-initialize the data again
        let num_fields = self.num_rows * self.num_columns;
        let last_offset = self.offsets[num_fields];

        // Need to truncate data to the actual amount of data read
        let data = std::str::from_utf8(&self.data[..last_offset]).map_err(|e| {
            ArrowError::CsvError(format!("Encountered invalid UTF-8 data: {}", e))
        })?;

        Ok(StringRecords {
            num_columns: self.num_columns,
            num_rows: self.num_rows,
            offsets: &self.offsets[..num_fields + 1],
            data,
        })
    }
}

/// A collection of parsed, UTF-8 CSV records
#[derive(Debug)]
pub struct StringRecords<'a> {
    num_columns: usize,
    num_rows: usize,
    offsets: &'a [usize],
    data: &'a str,
}

impl<'a> StringRecords<'a> {
    fn get(&self, index: usize) -> StringRecord<'a> {
        let field_idx = index * self.num_columns;
        StringRecord {
            data: self.data,
            offsets: &self.offsets[field_idx..field_idx + self.num_columns + 1],
        }
    }

    pub fn len(&self) -> usize {
        self.num_rows
    }

    pub fn is_empty(&self) -> bool {
        self.num_rows == 0
    }

    pub fn iter(&self) -> impl Iterator<Item = StringRecord<'a>> + '_ {
        (0..self.num_rows).map(|x| self.get(x))
    }
}

/// A single parsed, UTF-8 CSV record
#[derive(Debug, Clone, Copy)]
pub struct StringRecord<'a> {
    data: &'a str,
    offsets: &'a [usize],
}

impl<'a> StringRecord<'a> {
    pub fn get(&self, index: usize) -> &'a str {
        let end = self.offsets[index + 1];
        let start = self.offsets[index];

        // SAFETY:
        // Parsing produces offsets at valid byte boundaries
        unsafe { self.data.get_unchecked(start..end) }
    }
}

#[cfg(test)]
mod tests {
    use crate::reader::records::RecordReader;
    use csv_core::Reader;
    use std::io::Cursor;

    #[test]
    fn test_basic() {
        let csv = [
            "foo,bar,baz",
            "a,b,c",
            "12,3,5",
            "\"asda\"\"asas\",\"sdffsnsd\", as",
        ]
        .join("\n");

        let mut expected = vec![
            vec!["foo", "bar", "baz"],
            vec!["a", "b", "c"],
            vec!["12", "3", "5"],
            vec!["asda\"asas", "sdffsnsd", " as"],
        ]
        .into_iter();

        let cursor = Cursor::new(csv.as_bytes());
        let mut reader = RecordReader::new(cursor, Reader::new(), 3);

        loop {
            let b = reader.read(3).unwrap();
            if b.is_empty() {
                break;
            }

            b.iter().zip(&mut expected).for_each(|(record, expected)| {
                let actual = (0..3)
                    .map(|field_idx| record.get(field_idx))
                    .collect::<Vec<_>>();
                assert_eq!(actual, expected)
            })
        }
    }
}
