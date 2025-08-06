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

// These macros are adapted from Jörn Horstmann's thrift macros at
// https://github.com/jhorstmann/compact-thrift
// They allow for pasting sections of the Parquet thrift IDL file
// into a macro to generate rust structures and implementations.

#[macro_export]
/// macro to generate rust enums from a thrift enum definition
macro_rules! thrift_enum {
    ($(#[$($def_attrs:tt)*])* enum $identifier:ident { $($(#[$($field_attrs:tt)*])* $field_name:ident = $field_value:literal;)* }) => {
        $(#[$($def_attrs)*])*
        #[derive(Debug, Copy, Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
        #[allow(non_camel_case_types)]
        #[allow(missing_docs)]
        pub enum $identifier {
            $($field_name = $field_value,)*
        }

        impl<'a> TryFrom<&mut ThriftCompactInputProtocol<'a>> for $identifier {
            type Error = ParquetError;
            fn try_from(prot: &mut ThriftCompactInputProtocol<'a>) -> Result<Self> {
                let val = prot.read_i32()?;
                match val {
                    $($field_value => Ok(Self::$field_name),)*
                    _ => Err(general_err!("Unexpected {} {}", stringify!($identifier), val)),
                }
            }
        }

        impl fmt::Display for $identifier {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, "{self:?}")
            }
        }

        // TODO: remove when we finally get rid of the format module
        impl TryFrom<parquet::$identifier> for $identifier {
            type Error = ParquetError;

            fn try_from(value: parquet::$identifier) -> Result<Self> {
                Ok(match value {
                    $(parquet::$identifier::$field_name => Self::$field_name,)*
                    _ => return Err(general_err!("Unexpected parquet {}: {}", stringify!($identifier), value.0)),
                })
            }
        }

        impl From<$identifier> for parquet::$identifier {
            fn from(value: $identifier) -> Self {
                match value {
                    $($identifier::$field_name => Self::$field_name,)*
                }
            }
        }
    }
}
