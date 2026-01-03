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

use crate::reader::tape::{Tape, TapeElement};
use crate::reader::{ArrayDecoder, StructMode, make_decoder};
use arrow_array::builder::BooleanBufferBuilder;
use arrow_buffer::buffer::NullBuffer;
use arrow_data::{ArrayData, ArrayDataBuilder};
use arrow_schema::{ArrowError, DataType, Fields};
use std::collections::HashMap;

pub struct StructArrayDecoder {
    data_type: DataType,
    decoders: Vec<Box<dyn ArrayDecoder>>,
    strict_mode: bool,
    is_nullable: bool,
    struct_mode: StructMode,
    field_name_to_index: Option<HashMap<String, usize>>,
    child_pos: Vec<u32>,
}

impl StructArrayDecoder {
    pub fn new(
        data_type: DataType,
        coerce_primitive: bool,
        strict_mode: bool,
        is_nullable: bool,
        struct_mode: StructMode,
    ) -> Result<Self, ArrowError> {
        let (decoders, field_name_to_index) = {
            let fields = struct_fields(&data_type);
            let decoders = fields
                .iter()
                .map(|f| {
                    // If this struct nullable, need to permit nullability in child array
                    // StructArrayDecoder::decode verifies that if the child is not nullable
                    // it doesn't contain any nulls not masked by its parent
                    let nullable = f.is_nullable() || is_nullable;
                    make_decoder(
                        f.data_type().clone(),
                        coerce_primitive,
                        strict_mode,
                        nullable,
                        struct_mode,
                    )
                })
                .collect::<Result<Vec<_>, ArrowError>>()?;
            let field_name_to_index = if struct_mode == StructMode::ObjectOnly {
                build_field_index(fields)
            } else {
                None
            };
            (decoders, field_name_to_index)
        };

        Ok(Self {
            data_type,
            decoders,
            strict_mode,
            is_nullable,
            struct_mode,
            field_name_to_index,
            child_pos: Vec::new(),
        })
    }
}

impl ArrayDecoder for StructArrayDecoder {
    fn decode(&mut self, tape: &Tape<'_>, pos: &[u32]) -> Result<ArrayData, ArrowError> {
        let fields = struct_fields(&self.data_type);
        let row_count = pos.len();
        let field_count = fields.len();
        let total_len = field_count.checked_mul(row_count).ok_or_else(|| {
            ArrowError::JsonError(format!(
                "StructArrayDecoder child position buffer size overflow for rows={row_count} fields={field_count}"
            ))
        })?;
        if total_len > self.child_pos.len() {
            self.child_pos
                .try_reserve(total_len - self.child_pos.len())
                .map_err(|_| {
                    ArrowError::JsonError(format!(
                        "StructArrayDecoder child position buffer allocation failed for rows={row_count} fields={field_count}"
                    ))
                })?;
        }
        self.child_pos.resize(total_len, 0);
        self.child_pos.fill(0);
        let mut nulls = self
            .is_nullable
            .then(|| BooleanBufferBuilder::new(pos.len()));

        {
            let child_pos = self.child_pos.as_mut_slice();
            // We avoid having the match on self.struct_mode inside the hot loop for performance
            // TODO: Investigate how to extract duplicated logic.
            match self.struct_mode {
                StructMode::ObjectOnly => {
                    for (row, p) in pos.iter().enumerate() {
                        let end_idx = match (tape.get(*p), nulls.as_mut()) {
                            (TapeElement::StartObject(end_idx), None) => end_idx,
                            (TapeElement::StartObject(end_idx), Some(nulls)) => {
                                nulls.append(true);
                                end_idx
                            }
                            (TapeElement::Null, Some(nulls)) => {
                                nulls.append(false);
                                continue;
                            }
                            (_, _) => return Err(tape.error(*p, "{")),
                        };

                        let mut cur_idx = *p + 1;
                        while cur_idx < end_idx {
                            // Read field name
                            let field_name = match tape.get(cur_idx) {
                                TapeElement::String(s) => tape.get_string(s),
                                _ => return Err(tape.error(cur_idx, "field name")),
                            };

                            // Update child pos if match found
                            let field_idx = match &self.field_name_to_index {
                                Some(map) => map.get(field_name).copied(),
                                None => fields.iter().position(|x| x.name() == field_name),
                            };
                            match field_idx {
                                Some(field_idx) => {
                                    child_pos[field_idx * row_count + row] = cur_idx + 1;
                                }
                                None => {
                                    if self.strict_mode {
                                        return Err(ArrowError::JsonError(format!(
                                            "column '{field_name}' missing from schema",
                                        )));
                                    }
                                }
                            }
                            // Advance to next field
                            cur_idx = tape.next(cur_idx + 1, "field value")?;
                        }
                    }
                }
                StructMode::ListOnly => {
                    for (row, p) in pos.iter().enumerate() {
                        let end_idx = match (tape.get(*p), nulls.as_mut()) {
                            (TapeElement::StartList(end_idx), None) => end_idx,
                            (TapeElement::StartList(end_idx), Some(nulls)) => {
                                nulls.append(true);
                                end_idx
                            }
                            (TapeElement::Null, Some(nulls)) => {
                                nulls.append(false);
                                continue;
                            }
                            (_, _) => return Err(tape.error(*p, "[")),
                        };

                        let mut cur_idx = *p + 1;
                        let mut entry_idx = 0;
                        while cur_idx < end_idx {
                            if entry_idx >= fields.len() {
                                return Err(ArrowError::JsonError(format!(
                                    "found extra columns for {} fields",
                                    fields.len()
                                )));
                            }
                            child_pos[entry_idx * row_count + row] = cur_idx;
                            entry_idx += 1;
                            // Advance to next field
                            cur_idx = tape.next(cur_idx, "field value")?;
                        }
                        if entry_idx != fields.len() {
                            return Err(ArrowError::JsonError(format!(
                                "found {} columns for {} fields",
                                entry_idx,
                                fields.len()
                            )));
                        }
                    }
                }
            }
        }

        let child_pos = self.child_pos.as_slice();
        let child_data = self
            .decoders
            .iter_mut()
            .enumerate()
            .zip(fields)
            .map(|((field_idx, d), f)| {
                let start = field_idx * row_count;
                let end = start + row_count;
                let pos = &child_pos[start..end];
                d.decode(tape, pos).map_err(|e| match e {
                    ArrowError::JsonError(s) => {
                        ArrowError::JsonError(format!("whilst decoding field '{}': {s}", f.name()))
                    }
                    e => e,
                })
            })
            .collect::<Result<Vec<_>, ArrowError>>()?;

        let nulls = nulls.as_mut().map(|x| NullBuffer::new(x.finish()));

        for (c, f) in child_data.iter().zip(fields) {
            // Sanity check
            assert_eq!(c.len(), pos.len());
            if let Some(a) = c.nulls() {
                let nulls_valid =
                    f.is_nullable() || nulls.as_ref().map(|n| n.contains(a)).unwrap_or_default();

                if !nulls_valid {
                    return Err(ArrowError::JsonError(format!(
                        "Encountered unmasked nulls in non-nullable StructArray child: {f}"
                    )));
                }
            }
        }

        let data = ArrayDataBuilder::new(self.data_type.clone())
            .len(pos.len())
            .nulls(nulls)
            .child_data(child_data);

        // Safety
        // Validated lengths above
        Ok(unsafe { data.build_unchecked() })
    }
}

fn struct_fields(data_type: &DataType) -> &Fields {
    match &data_type {
        DataType::Struct(f) => f,
        _ => unreachable!(),
    }
}

fn build_field_index(fields: &Fields) -> Option<HashMap<String, usize>> {
    const FIELD_INDEX_LINEAR_THRESHOLD: usize = 16;
    if fields.len() < FIELD_INDEX_LINEAR_THRESHOLD {
        return None;
    }

    let mut map = HashMap::with_capacity(fields.len());
    for (idx, field) in fields.iter().enumerate() {
        let name = field.name();
        if !map.contains_key(name) {
            map.insert(name.to_string(), idx);
        }
    }
    Some(map)
}
