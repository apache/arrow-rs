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

//! Boolean native batches and encoder adapters.

use super::PlainEncoderImpl as PlainEncoder;
use super::*;

impl<D: DataType<T = bool>> EncodingFamily<D> for BoolEncodingFamily {
    fn try_new(
        _dict_supported: bool,
        fallback_encoding: Encoding,
        descr: &ColumnDescPtr,
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
