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

use ring::digest::digest as ring_digest;
use ring::digest::SHA256 as DigestSha256;

#[allow(non_camel_case_types)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Enum representing checksum algorithm supported by S3.
pub enum Checksum {
    /// SHA-256 algorithm.
    SHA256,

    #[cfg(feature = "sha1")]
    SHA1,

    #[cfg(feature = "crc32")]
    CRC32,

    #[cfg(feature = "crc32c")]
    CRC32C,
}

impl Checksum {
    pub(super) fn digest(&self, bytes: &[u8]) -> Vec<u8> {
        match self {
            Self::SHA256 => ring_digest(&DigestSha256, bytes).as_ref().to_owned(),

            #[cfg(feature = "sha1")]
            Checksum::SHA1 => todo!(),

            #[cfg(feature = "crc32")]
            Checksum::CRC32 => todo!(),

            #[cfg(feature = "crc32c")]
            Checksum::CRC32C => todo!(),
        }
    }

    pub(super) fn header_name(&self) -> &'static str {
        match self {
            Self::SHA256 => "x-amz-checksum-sha256",

            #[cfg(feature = "sha1")]
            Checksum::SHA1 => "x-amz-checksum-sha1",

            #[cfg(feature = "crc32")]
            Checksum::CRC32 => "x-amz-checksum-crc32",

            #[cfg(feature = "crc32c")]
            Checksum::CRC32C => "x-amz-checksum-crc32c",
        }
    }
}

impl TryFrom<&String> for Checksum {
    type Error = ();

    fn try_from(value: &String) -> Result<Self, Self::Error> {
        match value.as_str() {
            "sha256" => Ok(Self::SHA256),
            _ => unreachable!(),
        }
    }
}
