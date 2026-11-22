// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_array::builder::BooleanBuilder;
use arrow_schema::ArrowError;

use crate::reader::tape::{Tape, TapeElement};
use crate::reader::{ArrayDecoder, DecoderContext};

#[derive(Default)]
pub struct BooleanArrayDecoder {
    ignore_type_conflicts: bool,
}
impl BooleanArrayDecoder {
    pub fn new(ctx: &DecoderContext) -> Self {
        Self {
            ignore_type_conflicts: ctx.ignore_type_conflicts(),
        }
    }
}

impl ArrayDecoder for BooleanArrayDecoder {
    fn decode(&mut self, tape: &Tape<'_>, pos: &[u32]) -> Result<ArrayRef, ArrowError> {
        let mut builder = BooleanBuilder::with_capacity(pos.len());
        for p in pos {
            match tape.get(*p) {
                TapeElement::Null => builder.append_null(),
                TapeElement::True => builder.append_value(true),
                TapeElement::False => builder.append_value(false),
                _ if self.ignore_type_conflicts => builder.append_null(),
                _ => return Err(tape.error(*p, "boolean")),
            }
        }

        Ok(Arc::new(builder.finish()))
    }
}
