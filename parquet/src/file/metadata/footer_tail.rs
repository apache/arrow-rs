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

use crate::errors::{ParquetError, Result};
use crate::file::{FOOTER_SIZE, PARQUET_MAGIC, PARQUET_MAGIC_ENCR_FOOTER};

/// Parsed Parquet footer tail (last 8 bytes of a Parquet file)
///
/// There are 8 bytes at the end of the Parquet footer with the following layout:
/// * 4 bytes for the metadata length
/// * 4 bytes for the magic bytes 'PAR1' or 'PARE' (encrypted footer)
///
/// ```text
/// +-----+------------------+
/// | len | 'PAR1' or 'PARE' |
/// +-----+------------------+
/// ```
///
/// # Examples
/// ```
/// # use parquet::file::metadata::FooterTail;
/// // a non encrypted footer with 28 bytes of metadata
/// let last_8_bytes: [u8; 8] = [0x1C, 0x00, 0x00, 0x00, b'P', b'A', b'R', b'1'];
/// let footer_tail = FooterTail::try_from(last_8_bytes).unwrap();
/// assert_eq!(footer_tail.metadata_length(), 28);
/// assert_eq!(footer_tail.is_encrypted_footer(), false);
/// ```
///
/// ```
/// # use parquet::file::metadata::FooterTail;
/// // an encrypted footer with 512 bytes of metadata
/// let last_8_bytes = vec![0x00, 0x02, 0x00, 0x00, b'P', b'A', b'R', b'E'];
/// let footer_tail = FooterTail::try_from(&last_8_bytes[..]).unwrap();
/// assert_eq!(footer_tail.metadata_length(), 512);
/// assert_eq!(footer_tail.is_encrypted_footer(), true);
/// ```
///
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FooterTail {
    metadata_length: usize,
    encrypted_footer: bool,
}

impl FooterTail {
    /// Try to decode the footer tail from the given 8 bytes
    pub fn try_new(slice: &[u8; FOOTER_SIZE]) -> Result<FooterTail> {
        let magic = &slice[4..];
        let encrypted_footer = if magic == PARQUET_MAGIC_ENCR_FOOTER {
            true
        } else if magic == PARQUET_MAGIC {
            false
        } else {
            return Err(general_err!("Invalid Parquet file. Corrupt footer"));
        };
        // get the metadata length from the footer
        let metadata_len = u32::from_le_bytes(slice[..4].try_into().unwrap());

        Ok(FooterTail {
            // u32 won't be larger than usize in most cases
            metadata_length: metadata_len.try_into()?,
            encrypted_footer,
        })
    }

    /// The length of the footer metadata in bytes
    pub fn metadata_length(&self) -> usize {
        self.metadata_length
    }

    /// Whether the footer metadata is encrypted
    pub fn is_encrypted_footer(&self) -> bool {
        self.encrypted_footer
    }
}

impl TryFrom<[u8; FOOTER_SIZE]> for FooterTail {
    type Error = ParquetError;

    fn try_from(value: [u8; FOOTER_SIZE]) -> Result<Self> {
        Self::try_new(&value)
    }
}

impl TryFrom<&[u8]> for FooterTail {
    type Error = ParquetError;

    fn try_from(value: &[u8]) -> Result<Self> {
        if value.len() != FOOTER_SIZE {
            return Err(general_err!(
                "Invalid footer length {}, expected {FOOTER_SIZE}",
                value.len()
            ));
        }
        let slice: &[u8; FOOTER_SIZE] = value.try_into().unwrap();
        Self::try_new(slice)
    }
}
