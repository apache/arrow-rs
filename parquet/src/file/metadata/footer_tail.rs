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
use crate::file::{
    FOOTER_SIZE, PARQUET_MAGIC, PARQUET_MAGIC_ENCR_FOOTER, PARQUET_MAGIC_PARX, PARX_FOOTER_SIZE,
    PARX_KNOWN_FEATURE_FLAGS,
};

/// Additional metadata stored in the footer of PARX-format files.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParxInfo {
    feature_flags: u32,
    expected_crc: u32,
    /// The 8 bytes [length(4)][flags(4)] used in CRC computation.
    crc_suffix_bytes: [u8; 8],
}

impl ParxInfo {
    /// Returns the feature flags from the PARX footer.
    pub fn feature_flags(&self) -> u32 {
        self.feature_flags
    }

    /// Validates the CRC32 of the given metadata bytes against the expected CRC stored in the footer.
    ///
    /// CRC covers: `[metadata_bytes][length(4)][flags(4)]`
    pub fn validate_crc(&self, metadata_bytes: &[u8]) -> Result<()> {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(metadata_bytes);
        hasher.update(&self.crc_suffix_bytes);
        let actual_crc = hasher.finalize();
        if actual_crc != self.expected_crc {
            return Err(general_err!(
                "PARX footer CRC32 mismatch: expected {:#010x}, got {:#010x}",
                self.expected_crc,
                actual_crc
            ));
        }
        Ok(())
    }
}

/// Parsed Parquet footer tail.
///
/// PAR1/PARE: last 8 bytes of a Parquet file:
/// ```text
/// +-----+------------------+
/// | len | 'PAR1' or 'PARE' |
/// +-----+------------------+
/// ```
///
/// PARX: last 16 bytes of a PARX file:
/// ```text
/// +-----+-------+-----+--------+
/// | len | flags | crc | 'PARX' |
/// +-----+-------+-----+--------+
///   4     4       4     4
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FooterTail {
    metadata_length: usize,
    encrypted_footer: bool,
    parx_info: Option<ParxInfo>,
}

impl FooterTail {
    /// Try to decode the footer tail from the given 8 bytes (PAR1/PARE format only).
    pub fn try_new(slice: &[u8; FOOTER_SIZE]) -> Result<FooterTail> {
        let magic = &slice[4..];
        let encrypted_footer = if magic == PARQUET_MAGIC_ENCR_FOOTER {
            true
        } else if magic == PARQUET_MAGIC {
            false
        } else {
            return Err(general_err!("Invalid Parquet file. Corrupt footer"));
        };
        let metadata_len = u32::from_le_bytes(slice[..4].try_into().unwrap());

        Ok(FooterTail {
            metadata_length: metadata_len.try_into()?,
            encrypted_footer,
            parx_info: None,
        })
    }

    /// Try to decode the footer tail from a 16-byte PARX footer.
    ///
    /// Layout: `[len(4)][flags(4)][crc(4)]['PARX'(4)]`
    pub fn try_new_parx(slice: &[u8; PARX_FOOTER_SIZE]) -> Result<FooterTail> {
        let magic = &slice[12..16];
        if magic != PARQUET_MAGIC_PARX {
            return Err(general_err!(
                "Invalid PARX footer: expected 'PARX' magic, got {:?}",
                magic
            ));
        }

        let metadata_len = u32::from_le_bytes(slice[0..4].try_into().unwrap());
        let feature_flags = u32::from_le_bytes(slice[4..8].try_into().unwrap());
        let expected_crc = u32::from_le_bytes(slice[8..12].try_into().unwrap());

        // Flags are checked before CRC: future flag values may alter how the CRC is computed,
        // so we must know which flags are active before validating the checksum.
        let unknown_flags = feature_flags & !PARX_KNOWN_FEATURE_FLAGS;
        if unknown_flags != 0 {
            let bits: Vec<String> = (0..32)
                .filter(|&i| unknown_flags & (1u32 << i) != 0)
                .map(|i| format!("{:#010x}", 1u32 << i))
                .collect();
            return Err(general_err!(
                "PARX footer contains unknown feature flags: {}",
                bits.join(", ")
            ));
        }

        let encrypted_footer =
            feature_flags & crate::file::PARX_FEATURE_FLAG_ENCRYPTED_FOOTER != 0;

        let mut crc_suffix_bytes = [0u8; 8];
        crc_suffix_bytes.copy_from_slice(&slice[0..8]);

        Ok(FooterTail {
            metadata_length: metadata_len.try_into()?,
            encrypted_footer,
            parx_info: Some(ParxInfo {
                feature_flags,
                expected_crc,
                crc_suffix_bytes,
            }),
        })
    }

    /// The length of the footer metadata in bytes.
    pub fn metadata_length(&self) -> usize {
        self.metadata_length
    }

    /// Whether the footer metadata is encrypted.
    pub fn is_encrypted_footer(&self) -> bool {
        self.encrypted_footer
    }

    /// The size of the fixed footer tail in bytes (not including variable-length metadata).
    ///
    /// Returns [`PARX_FOOTER_SIZE`] for PARX files, [`FOOTER_SIZE`] otherwise.
    pub fn fixed_footer_size(&self) -> usize {
        if self.parx_info.is_some() {
            PARX_FOOTER_SIZE
        } else {
            FOOTER_SIZE
        }
    }

    /// Returns PARX-specific footer info, if this is a PARX file.
    pub fn parx_info(&self) -> Option<&ParxInfo> {
        self.parx_info.as_ref()
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
        let len = value.len();
        if len < FOOTER_SIZE {
            return Err(general_err!(
                "Invalid footer length {}, minimum {}",
                len,
                FOOTER_SIZE
            ));
        }

        let magic = &value[len - 4..len];

        if magic == PARQUET_MAGIC || magic == PARQUET_MAGIC_ENCR_FOOTER {
            let slice: &[u8; FOOTER_SIZE] = value[len - FOOTER_SIZE..len].try_into().unwrap();
            return Self::try_new(slice);
        }

        if magic == PARQUET_MAGIC_PARX {
            if len < PARX_FOOTER_SIZE {
                return Err(ParquetError::NeedMoreData(PARX_FOOTER_SIZE));
            }
            let slice: &[u8; PARX_FOOTER_SIZE] =
                value[len - PARX_FOOTER_SIZE..len].try_into().unwrap();
            return Self::try_new_parx(slice);
        }

        Err(general_err!("Invalid Parquet file. Corrupt footer"))
    }
}
