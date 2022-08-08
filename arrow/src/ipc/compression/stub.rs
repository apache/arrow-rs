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

//! Stubs that implement the same interface as ipc_compression
//! but always error.

use crate::buffer::Buffer;
use crate::error::{ArrowError, Result};
use crate::ipc::CompressionType;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CompressionCodec {}

impl TryFrom<CompressionCodec> for CompressionType {
    type Error = ArrowError;
    fn try_from(codec: CompressionCodec) -> Result<Self> {
        return Err(ArrowError::InvalidArgumentError(
            format!("codec type {:?} not supported because arrow was not compiled with the ipc_compression feature", codec)));
    }
}

impl TryFrom<CompressionType> for CompressionCodec {
    type Error = ArrowError;

    fn try_from(compression_type: CompressionType) -> Result<Self> {
        Err(ArrowError::InvalidArgumentError(
            format!("compression type {:?} not supported because arrow was not compiled with the ipc_compression feature", compression_type))
            )
    }
}

impl CompressionCodec {
    #[allow(clippy::ptr_arg)]
    pub(crate) fn compress_to_vec(
        &self,
        _input: &[u8],
        _output: &mut Vec<u8>,
    ) -> Result<usize> {
        Err(ArrowError::InvalidArgumentError(
            "compression not supported because arrow was not compiled with the ipc_compression feature".to_string()
        ))
    }

    pub(crate) fn decompress_to_buffer(&self, _input: &[u8]) -> Result<Buffer> {
        Err(ArrowError::InvalidArgumentError(
            "decompression not supported because arrow was not compiled with the ipc_compression feature".to_string()
        ))
    }
}
