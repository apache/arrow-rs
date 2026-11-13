// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::marker::PhantomData;
use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_array::builder::PrimitiveBuilder;
use arrow_array::types::DecimalType;
use arrow_cast::parse::parse_decimal;
use arrow_schema::ArrowError;

use crate::reader::tape::{Tape, TapeElement};
use crate::reader::{ArrayDecoder, DecoderContext};

pub struct DecimalArrayDecoder<D: DecimalType> {
    precision: u8,
    scale: i8,
    ignore_type_conflicts: bool,
    // Invariant and Send
    phantom: PhantomData<fn(D) -> D>,
}

impl<D: DecimalType> DecimalArrayDecoder<D> {
    pub fn new(ctx: &DecoderContext, precision: u8, scale: i8) -> Self {
        Self {
            precision,
            scale,
            ignore_type_conflicts: ctx.ignore_type_conflicts(),
            phantom: PhantomData,
        }
    }
}

impl<D> ArrayDecoder for DecimalArrayDecoder<D>
where
    D: DecimalType,
{
    fn decode(&mut self, tape: &Tape<'_>, pos: &[u32]) -> Result<ArrayRef, ArrowError> {
        let mut builder = PrimitiveBuilder::<D>::with_capacity(pos.len());

        #[expect(unused_assignments)] // initial value overwritten without ever being read
        let mut anchor = String::default();
        for p in pos {
            let value = match tape.get(*p) {
                TapeElement::Null => {
                    builder.append_null();
                    continue;
                }
                TapeElement::String(idx) | TapeElement::Number(idx) => tape.get_string(idx),
                TapeElement::I64(high) => match tape.get(*p + 1) {
                    TapeElement::I32(low) => {
                        anchor = (((high as i64) << 32) | (low as u32) as i64).to_string();
                        anchor.as_str()
                    }
                    _ => unreachable!(),
                },
                TapeElement::I32(val) => {
                    anchor = val.to_string();
                    anchor.as_str()
                }
                TapeElement::F64(high) => match tape.get(*p + 1) {
                    TapeElement::F32(low) => {
                        anchor = f64::from_bits(((high as u64) << 32) | low as u64).to_string();
                        anchor.as_str()
                    }
                    _ => unreachable!(),
                },
                TapeElement::F32(val) => {
                    anchor = f32::from_bits(val).to_string();
                    anchor.as_str()
                }
                _ if self.ignore_type_conflicts => {
                    builder.append_null();
                    continue;
                }
                _ => return Err(tape.error(*p, "decimal")),
            };

            match parse_decimal::<D>(value, self.precision, self.scale) {
                Ok(value) => builder.append_value(value),
                Err(_) if self.ignore_type_conflicts => builder.append_null(),
                Err(e) => return Err(e),
            }
        }

        Ok(Arc::new(
            builder
                .finish()
                .with_precision_and_scale(self.precision, self.scale)?,
        ))
    }
}
