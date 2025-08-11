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
#[allow(clippy::crate_in_macro_def)]
/// macro to generate rust enums from a thrift enum definition
macro_rules! thrift_enum {
    ($(#[$($def_attrs:tt)*])* enum $identifier:ident { $($(#[$($field_attrs:tt)*])* $field_name:ident = $field_value:literal;)* }) => {
        $(#[$($def_attrs)*])*
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
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
        impl TryFrom<crate::format::$identifier> for $identifier {
            type Error = ParquetError;

            fn try_from(value: crate::format::$identifier) -> Result<Self> {
                Ok(match value {
                    $(crate::format::$identifier::$field_name => Self::$field_name,)*
                    _ => return Err(general_err!("Unexpected parquet {}: {}", stringify!($identifier), value.0)),
                })
            }
        }

        impl From<$identifier> for crate::format::$identifier {
            fn from(value: $identifier) -> Self {
                match value {
                    $($identifier::$field_name => Self::$field_name,)*
                }
            }
        }
    }
}

#[macro_export]
#[allow(clippy::crate_in_macro_def)]
/// macro to generate rust enums for empty thrift structs used in unions
macro_rules! thrift_empty_struct {
    ($identifier: ident) => {
        #[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
        pub struct $identifier {}

        impl<'a> TryFrom<&mut ThriftCompactInputProtocol<'a>> for $identifier {
            type Error = ParquetError;
            fn try_from(prot: &mut ThriftCompactInputProtocol<'a>) -> Result<Self> {
                prot.skip_empty_struct()?;
                Ok(Self {})
            }
        }

        // TODO: remove when we finally get rid of the format module
        impl From<crate::format::$identifier> for $identifier {
            fn from(_: $crate::format::$identifier) -> Self {
                Self {}
            }
        }

        impl From<$identifier> for crate::format::$identifier {
            fn from(_: $identifier) -> Self {
                Self {}
            }
        }
    };
}

/// macro to generate rust enums for thrift unions where all fields are typed with empty structs
#[macro_export]
#[allow(clippy::crate_in_macro_def)]
macro_rules! thrift_union_all_empty {
    ($(#[$($def_attrs:tt)*])* union $identifier:ident { $($(#[$($field_attrs:tt)*])* $field_id:literal : $field_type:ident $(< $element_type:ident >)? $field_name:ident $(;)?)* }) => {
        $(#[cfg_attr(not(doctest), $($def_attrs)*)])*
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        #[allow(non_camel_case_types)]
        #[allow(non_snake_case)]
        #[allow(missing_docs)]
        pub enum $identifier {
            $($(#[cfg_attr(not(doctest), $($field_attrs)*)])* $field_name),*
        }

        impl<'a> TryFrom<&mut ThriftCompactInputProtocol<'a>> for $identifier {
            type Error = ParquetError;

            fn try_from(prot: &mut ThriftCompactInputProtocol<'a>) -> Result<Self> {
                prot.read_struct_begin()?;
                let field_ident = prot.read_field_begin()?;
                if field_ident.field_type == FieldType::Stop {
                    return Err(general_err!("Received empty union from remote {}", stringify!($identifier)));
                }
                let ret = match field_ident.id {
                    $($field_id => {
                        prot.skip_empty_struct()?;
                        Self::$field_name
                    }
                    )*
                    _ => {
                        return Err(general_err!("Unexpected {} {}", stringify!($identifier), field_ident.id));
                    }
                };
                let field_ident = prot.read_field_begin()?;
                if field_ident.field_type != FieldType::Stop {
                    return Err(general_err!(
                        "Received multiple fields for union from remote {}", stringify!($identifier)
                    ));
                }
                prot.read_struct_end()?;
                Ok(ret)
            }
        }

        // TODO: remove when we finally get rid of the format module
        impl From<crate::format::$identifier> for $identifier {
            fn from(value: crate::format::$identifier) -> Self {
                match value {
                    $(crate::format::$identifier::$field_name(_) => Self::$field_name,)*
                }
            }
        }

        impl From<$identifier> for crate::format::$identifier {
            fn from(value: $identifier) -> Self {
                match value {
                    $($identifier::$field_name => Self::$field_name(Default::default()),)*
                }
            }
        }
    }
}

/// macro to generate rust structs from a thrift struct definition
/// unlike enum and union, this macro will allow for visibility specifier
/// can also take optional lifetime for struct and elements within it (need e.g.)
#[macro_export]
macro_rules! thrift_struct {
    ($(#[$($def_attrs:tt)*])* $vis:vis struct $identifier:ident $(< $lt:lifetime >)? { $($(#[$($field_attrs:tt)*])* $field_id:literal : $required_or_optional:ident $field_type:ident $(< $field_lt:lifetime >)? $(< $element_type:ident >)? $field_name:ident $(= $default_value:literal)? $(;)?)* }) => {
        $(#[cfg_attr(not(doctest), $($def_attrs)*)])*
        #[derive(Clone, Debug, PartialEq)]
        #[allow(non_camel_case_types)]
        #[allow(non_snake_case)]
        #[allow(missing_docs)]
        $vis struct $identifier $(<$lt>)? {
            $($(#[cfg_attr(not(doctest), $($field_attrs)*)])* $vis $field_name: $crate::__thrift_required_or_optional!($required_or_optional $crate::__thrift_field_type!($field_type $($field_lt)? $($element_type)?))),*
        }

        impl<'a> TryFrom<&mut ThriftCompactInputProtocol<'a>> for $identifier $(<$lt>)? {
            type Error = ParquetError;
            fn try_from(prot: &mut ThriftCompactInputProtocol<'a>) -> Result<Self> {
                $(let mut $field_name: Option<$crate::__thrift_field_type!($field_type $($field_lt)? $($element_type)?)> = None;)*
                prot.read_struct_begin()?;
                loop {
                    let field_ident = prot.read_field_begin()?;
                    if field_ident.field_type == FieldType::Stop {
                        break;
                    }
                    match field_ident.id {
                        $($field_id => {
                            let val = $crate::__thrift_read_field!(prot, $field_type $($field_lt)? $($element_type)?);
                            $field_name = Some(val);
                        })*
                        _ => {
                            prot.skip(field_ident.field_type)?;
                        }
                    };
                }
                prot.read_struct_end()?;
                $($crate::__thrift_result_required_or_optional!($required_or_optional $field_name);)*
                Ok(Self {
                    $($field_name),*
                })
            }
        }
    }
}

/// macro to simplify reading lists from a thrift input stream
#[macro_export]
macro_rules! thrift_read_list {
    ($prot:expr, $identifier:ident) => {{
        let list_ident = $prot.read_list_begin()?;
        let mut val: Vec<$crate::__thrift_field_type!($identifier)> =
            Vec::with_capacity(list_ident.size as usize);
        for _ in 0..list_ident.size {
            let pes = $crate::__thrift_read_field!($prot, $identifier);
            val.push(pes);
        }
        val
    }};
}

/// macro to use when decoding struct fields
#[macro_export]
macro_rules! thrift_read_field {
    ($field_name:ident, $prot:tt, $field_type:ident) => {
        $field_name = Some($crate::__thrift_read_field!($prot, $field_type));
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __thrift_required_or_optional {
    (required $field_type:ty) => { $field_type };
    (optional $field_type:ty) => { Option<$field_type> };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __thrift_result_required_or_optional {
    (required $field_name:ident) => {
        let $field_name = $field_name.expect(concat!(
            "Required field ",
            stringify!($field_name),
            " is missing",
        ));
    };
    (optional $field_name:ident) => {};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __thrift_read_field {
    ($prot:tt, list $lt:lifetime $element_type:ident) => {
        $crate::thrift_read_list!($prot, $element_type)
    };
    ($prot:tt, list $element_type:ident) => {
        $crate::thrift_read_list!($prot, $element_type)
    };
    ($prot:tt, string $lt:lifetime) => {
        <&$lt str>::try_from(&mut *$prot)?
    };
    ($prot:tt, binary $lt:lifetime) => {
        <&$lt [u8]>::try_from(&mut *$prot)?
    };
    ($prot:tt, $field_type:ident $lt:lifetime) => {
        $field_type::try_from(&mut *$prot)?
    };
    ($prot:tt, string) => {
        String::try_from(&mut *$prot)?
    };
    ($prot:tt, binary) => {
        <Vec<u8>>::try_from(&mut *$prot)?
    };
    ($prot:tt, double) => {
        f64::try_from(&mut *$prot)?
    };
    ($prot:tt, $field_type:ident) => {
        $field_type::try_from(&mut *$prot)?
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __thrift_field_type {
    (binary $lt:lifetime) => { &$lt [u8] };
    (string $lt:lifetime) => { &$lt str };
    ($field_type:ident $lt:lifetime) => { $field_type<$lt> };
    (list $lt:lifetime $element_type:ident) => { Vec< $crate::__thrift_field_type!($element_type $lt) > };
    (list string) => { Vec<String> };
    (list $element_type:ident) => { Vec< $crate::__thrift_field_type!($element_type) > };
    (binary) => { Vec<u8> };
    (string) => { String };
    (double) => { f64 };
    ($field_type:ty) => { $field_type };
}
