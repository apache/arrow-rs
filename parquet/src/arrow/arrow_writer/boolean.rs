// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

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
