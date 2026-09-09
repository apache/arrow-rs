// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Boolean native batches and encoder adapters.

use super::PlainEncoderImpl as PlainEncoder;
use super::*;

impl<D: DataType<T = bool>> EncodingFamily<D> for BoolEncodingFamily {
    fn try_new(
        _dict_supported: bool,
        fallback_encoding: Encoding,
        descr: &ColumnDescPtr,
        _delta_options: Option<DeltaBinaryPackedEncoderOptions>,
    ) -> Result<Self> {
        Self::from_encoding(fallback_encoding, descr)
    }

    fn flush_data_page(&mut self) -> Result<(Bytes, Encoding)> {
        let buf = <Self as Encoder<BoolType>>::flush_buffer(self)?;
        let encoding = <Self as Encoder<BoolType>>::encoding(self);
        Ok((buf, encoding))
    }

    fn data_page_size(&self) -> usize {
        <Self as Encoder<BoolType>>::estimated_data_encoded_size(self)
    }

    fn memory_size(&self) -> usize {
        <Self as Encoder<BoolType>>::estimated_memory_size(self)
    }
}

impl BoolEncodingFamily {
    fn from_encoding(encoding: Encoding, _descr: &ColumnDescPtr) -> Result<Self> {
        match encoding {
            Encoding::PLAIN => Ok(Self::Plain(PlainEncoder::new())),
            Encoding::RLE => Ok(Self::Rle(RleValueEncoder::new())),
            e => Err(unsupported_column_encoding(e, Type::BOOLEAN)),
        }
    }
}
