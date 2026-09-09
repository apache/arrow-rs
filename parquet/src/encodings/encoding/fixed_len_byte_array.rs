// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Fixed-length byte-array native batches and encoder adapters.

use super::PlainEncoderImpl as PlainEncoder;
use super::*;

impl FixedLenByteArrayEncodingFamily {
    pub(super) fn from_encoding(
        encoding: Encoding,
        descr: &ColumnDescPtr,
        delta_options: Option<DeltaBinaryPackedEncoderOptions>,
    ) -> Result<Self> {
        match encoding {
            Encoding::PLAIN => Ok(Self::Plain(PlainEncoder::new())),
            Encoding::DELTA_BYTE_ARRAY => Ok(Self::DeltaByteArray(match delta_options {
                Some(options) => ByteArrayDeltaEncoder::new_with_options(options),
                None => ByteArrayDeltaEncoder::default(),
            })),
            Encoding::BYTE_STREAM_SPLIT => Ok(Self::ByteStreamSplit(
                VariableWidthByteStreamSplitEncoder::new(descr.type_length()),
            )),
            e => Err(unsupported_column_encoding(e, Type::FIXED_LEN_BYTE_ARRAY)),
        }
    }
}
