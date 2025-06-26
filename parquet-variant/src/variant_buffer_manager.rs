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

//! `VariantBufferManager` allows callers to have full control over the outputs that the Variant
//! library writes to when constructing Variants.

use arrow_schema::ArrowError;

pub trait VariantBufferManager {
    /// Returns the slice where the variant metadata needs to be written to. This method may be
    /// called several times during the construction of a new `metadata` field in a variant. The
    /// implementation must make sure that on every call, all the data written to the metadata
    /// buffer so far are preserved.
    /// The implementation must also make sure that the length of the slice being returned is at
    /// least `size` bytes. The implementation may throw an error if it is unable to fulfill its
    /// requirements.
    fn ensure_size_and_borrow_metadata_buffer(
        &mut self,
        size: usize,
    ) -> Result<&mut [u8], ArrowError>;

    /// Returns the slice where value needs to be written to. This method may be called several
    /// times during the construction of a new `value` field in a variant. The implementation must
    /// make sure that on every call, all the data written to the value buffer so far are preserved.
    /// The implementation must also make sure that the length of the slice being returned is at
    /// least `size` bytes. The implementation may throw an error if it is unable to fulfill its
    /// requirements.
    fn ensure_size_and_borrow_value_buffer(&mut self, size: usize)
        -> Result<&mut [u8], ArrowError>;
}

pub struct SampleVecBasedVariantBufferManager {
    pub value_buffer: Vec<u8>,
    pub metadata_buffer: Vec<u8>,
}

impl VariantBufferManager for SampleVecBasedVariantBufferManager {
    fn ensure_size_and_borrow_metadata_buffer(
        &mut self,
        size: usize,
    ) -> Result<&mut [u8], ArrowError> {
        let cur_len = self.metadata_buffer.len();
        if size > cur_len {
            // Reallocate larger buffer
            let new_len = size.next_power_of_two();
            self.metadata_buffer.resize(new_len, 0);
        }
        Ok(&mut self.metadata_buffer)
    }

    fn ensure_size_and_borrow_value_buffer(
        &mut self,
        size: usize,
    ) -> Result<&mut [u8], ArrowError> {
        let cur_len = self.value_buffer.len();
        if size > cur_len {
            // Reallocate larger buffer
            let new_len = size.next_power_of_two();
            self.value_buffer.resize(new_len, 0);
        }
        Ok(&mut self.value_buffer)
    }
}
