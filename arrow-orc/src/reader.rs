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

//! Traits abstract reading bytes from a source

use std::{
    fs::File,
    io::{BufReader, Read, Seek, SeekFrom},
};

/// Primary source used for reading required bytes for operations.
pub trait Reader {
    type T: Read;

    /// Get total length of bytes. Useful for parsing the metadata located at
    /// the end of the file.
    fn len(&self) -> u64;

    /// Get a reader starting at a specific offset.
    fn get_read(&self, offset_from_start: u64) -> std::io::Result<Self::T>;

    /// Read bytes from an offset with specific length.
    fn get_bytes(&self, offset_from_start: u64, length: u64) -> std::io::Result<Vec<u8>> {
        let mut bytes = vec![0; length as usize];
        self.get_read(offset_from_start)?
            .take(length)
            .read_exact(&mut bytes)?;
        Ok(bytes)
    }
}

impl Reader for File {
    type T = BufReader<File>;

    fn len(&self) -> u64 {
        self.metadata().map(|m| m.len()).unwrap_or(0u64)
    }

    /// Care needs to be taken when using this simulatenously as underlying
    /// file descriptor is the same and will be affected by other invocations.
    ///
    /// See [`File::try_clone()`] for more details.
    fn get_read(&self, offset_from_start: u64) -> std::io::Result<Self::T> {
        let mut reader = self.try_clone()?;
        reader.seek(SeekFrom::Start(offset_from_start))?;
        Ok(BufReader::new(self.try_clone()?))
    }
}
