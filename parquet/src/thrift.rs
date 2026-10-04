// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Custom thrift definitions

pub use thrift::protocol::TCompactOutputProtocol;
use thrift::protocol::{TInputProtocol, TOutputProtocol};

/// Reads and writes the struct to Thrift protocols.
///
/// Unlike [`thrift::protocol::TSerializable`] this uses generics instead of trait objects
pub trait TSerializable: Sized {
    /// Reads the struct from the input Thrift protocol
    fn read_from_in_protocol<T: TInputProtocol>(i_prot: &mut T) -> thrift::Result<Self>;
    /// Writes the struct to the output Thrift protocol
    fn write_to_out_protocol<T: TOutputProtocol>(&self, o_prot: &mut T) -> thrift::Result<()>;
}

#[cfg(test)]
mod tests {
    use crate::{
        basic::Type,
        file::page_index::{column_index::ColumnIndexMetaData, index_reader::decode_column_index},
    };

    #[test]
    fn legacy_statistics_reject_oversized_binary_before_allocation() {
        use crate::{format::Statistics, thrift::TSerializable};
        use thrift::{Error, ProtocolErrorKind, protocol::TCompactInputProtocol};

        // Field 1 (max), binary length 100 MiB + 1, and no payload. The length
        // must be rejected before allocating the claimed buffer or reading it.
        let bytes = [0x18, 0x81, 0x80, 0x80, 0x32];
        let mut input = TCompactInputProtocol::new(bytes.as_slice());
        let error = Statistics::read_from_in_protocol(&mut input).unwrap_err();
        assert!(
            matches!(error, Error::Protocol(error) if error.kind == ProtocolErrorKind::SizeLimit)
        );
    }

    #[test]
    fn legacy_statistics_compact_protocol_roundtrip() {
        use crate::{format::Statistics, thrift::TSerializable};
        use thrift::protocol::{TCompactInputProtocol, TCompactOutputProtocol};

        let expected = Statistics {
            min_value: Some(vec![0, 1, 255]),
            max_value: Some(vec![255, 0, 1]),
            null_count: Some(7),
            ..Default::default()
        };
        let mut bytes = Vec::new();
        expected
            .write_to_out_protocol(&mut TCompactOutputProtocol::new(&mut bytes))
            .unwrap();
        let actual =
            Statistics::read_from_in_protocol(&mut TCompactInputProtocol::new(bytes.as_slice()))
                .unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    pub fn read_boolean_list_field_type() {
        // Boolean collection type encoded as 0x01, as used by this crate when writing.
        // Values encoded as 1 (true) or 2 (false) as in the current version of the thrift
        // documentation.
        let bytes = vec![
            0x19, 0x21, 2, 1, 0x19, 0x28, 1, 0, 0, 0x19, 0x28, 1, 1, 0, 0x15, 0, 0,
        ];
        let index = decode_column_index(&bytes, Type::BOOLEAN).unwrap();

        let index = match index {
            ColumnIndexMetaData::BOOLEAN(index) => index,
            _ => panic!("expected boolean column index"),
        };

        // should be false, true
        assert!(!index.is_null_page(0));
        assert!(index.is_null_page(1));
        assert!(!index.min_value(0).unwrap()); // min is false
        assert!(index.max_value(0).unwrap()); // max is true
        assert!(index.min_value(1).is_none());
        assert!(index.max_value(1).is_none());
    }

    #[test]
    pub fn read_boolean_list_alternative_encoding() {
        // Boolean collection type encoded as 0x02, as allowed by the spec.
        // Values encoded as 1 (true) or 0 (false) as before the thrift documentation change on 2024-12-13.
        let bytes = vec![
            0x19, 0x22, 0, 1, 0x19, 0x28, 1, 0, 0, 0x19, 0x28, 1, 1, 0, 0x15, 0, 0,
        ];
        let index = decode_column_index(&bytes, Type::BOOLEAN).unwrap();

        let index = match index {
            ColumnIndexMetaData::BOOLEAN(index) => index,
            _ => panic!("expected boolean column index"),
        };

        // should be false, true
        assert!(!index.is_null_page(0));
        assert!(index.is_null_page(1));
        assert!(!index.min_value(0).unwrap()); // min is false
        assert!(index.max_value(0).unwrap()); // max is true
        assert!(index.min_value(1).is_none());
        assert!(index.max_value(1).is_none());
    }
}
