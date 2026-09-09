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

//! Boolean physical storage and Arrow writer bridge.

use super::*;

#[derive(Clone, Copy)]
pub(super) struct BoolStorage<'a> {
    bytes: &'a [u8],
    bit_offset: usize,
}

impl<'a> ArrowPhysicalBridge<'a> for BoolStorage<'a> {
    type ColumnEncoder = TypedColumnChunkEncoder<BoolType>;

    fn bind(column: &'a dyn arrow_array::Array) -> Result<Self> {
        let array = column.as_boolean();
        let values = array.values();
        Ok(Self {
            bytes: values.values(),
            bit_offset: values.offset(),
        })
    }

    fn write_values(
        self,
        encoder: &mut Self::ColumnEncoder,
        selection: PhysicalValueSelection<'a>,
    ) -> Result<()> {
        encoder.write_bool_batch(BoolBatch::new_physical(
            self.bytes,
            self.bit_offset,
            selection,
        ))
    }
}
