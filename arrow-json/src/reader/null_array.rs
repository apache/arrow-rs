// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::sync::Arc;

use arrow_array::{ArrayRef, NullArray};
use arrow_schema::ArrowError;

use crate::reader::tape::{Tape, TapeElement};
use crate::reader::{ArrayDecoder, DecoderContext};

#[derive(Default)]
pub struct NullArrayDecoder {
    ignore_type_conflicts: bool,
}
impl NullArrayDecoder {
    pub fn new(ctx: &DecoderContext) -> Self {
        Self {
            ignore_type_conflicts: ctx.ignore_type_conflicts(),
        }
    }
}

impl ArrayDecoder for NullArrayDecoder {
    fn decode(&mut self, tape: &Tape<'_>, pos: &[u32]) -> Result<ArrayRef, ArrowError> {
        if !self.ignore_type_conflicts {
            for p in pos {
                if !matches!(tape.get(*p), TapeElement::Null) {
                    return Err(tape.error(*p, "null"));
                }
            }
        }
        Ok(Arc::new(NullArray::new(pos.len())))
    }
}
