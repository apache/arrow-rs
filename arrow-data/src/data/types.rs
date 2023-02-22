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

use arrow_schema::{DataType, IntervalUnit};

/// An enumeration of the primitive types implementing [`ArrowNativeType`](arrow_buffer::ArrowNativeType)
#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub enum PrimitiveType {
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    Int256,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float16,
    Float32,
    Float64,
}

/// An enumeration of the types of offsets for variable length encodings
#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub enum OffsetType {
    Small,
    Large,
}

/// An enumeration of the types of variable length byte arrays
#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub enum BytesType {
    Binary,
    Utf8,
}

/// An enumeration of the types of dictionary key
#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub enum DictionaryKeyType {
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
}

/// An enumeration of the types of run key
#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub enum RunEndType {
    Int16,
    Int32,
    Int64,
}

/// Describes the physical representation of a given [`DataType`]
#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub enum PhysicalType {
    Null,
    Boolean,
    Primitive(PrimitiveType),
    FixedSizeBinary,
    Bytes(OffsetType, BytesType),
    FixedSizeList,
    List(OffsetType),
    Map,
    Struct,
    Union,
    Dictionary(DictionaryKeyType),
    Run(RunEndType),
}

impl From<&DataType> for PhysicalType {
    fn from(value: &DataType) -> Self {
        match value {
            DataType::Null => Self::Null,
            DataType::Boolean => Self::Boolean,
            DataType::Int8 => Self::Primitive(PrimitiveType::Int8),
            DataType::Int16 => Self::Primitive(PrimitiveType::Int16),
            DataType::Int32 => Self::Primitive(PrimitiveType::Int32),
            DataType::Int64 => Self::Primitive(PrimitiveType::Int64),
            DataType::UInt8 => Self::Primitive(PrimitiveType::UInt8),
            DataType::UInt16 => Self::Primitive(PrimitiveType::UInt16),
            DataType::UInt32 => Self::Primitive(PrimitiveType::UInt32),
            DataType::UInt64 => Self::Primitive(PrimitiveType::UInt64),
            DataType::Float16 => Self::Primitive(PrimitiveType::Float16),
            DataType::Float32 => Self::Primitive(PrimitiveType::Float32),
            DataType::Float64 => Self::Primitive(PrimitiveType::Float64),
            DataType::Timestamp(_, _) => Self::Primitive(PrimitiveType::Int64),
            DataType::Date32 => Self::Primitive(PrimitiveType::Int32),
            DataType::Date64 => Self::Primitive(PrimitiveType::Int64),
            DataType::Time32(_) => Self::Primitive(PrimitiveType::Int32),
            DataType::Time64(_) => Self::Primitive(PrimitiveType::Int64),
            DataType::Duration(_) => Self::Primitive(PrimitiveType::Int64),
            DataType::Decimal128(_, _) => Self::Primitive(PrimitiveType::Int128),
            DataType::Decimal256(_, _) => Self::Primitive(PrimitiveType::Int256),
            DataType::Interval(IntervalUnit::YearMonth) => {
                Self::Primitive(PrimitiveType::Int32)
            }
            DataType::Interval(IntervalUnit::DayTime) => {
                Self::Primitive(PrimitiveType::Int64)
            }
            DataType::Interval(IntervalUnit::MonthDayNano) => {
                Self::Primitive(PrimitiveType::Int128)
            }
            DataType::Binary => Self::Bytes(OffsetType::Small, BytesType::Binary),
            DataType::FixedSizeBinary(_) => Self::FixedSizeBinary,
            DataType::LargeBinary => Self::Bytes(OffsetType::Large, BytesType::Binary),
            DataType::Utf8 => Self::Bytes(OffsetType::Small, BytesType::Utf8),
            DataType::LargeUtf8 => Self::Bytes(OffsetType::Large, BytesType::Utf8),
            DataType::List(_) => Self::List(OffsetType::Small),
            DataType::FixedSizeList(_, _) => Self::FixedSizeList,
            DataType::LargeList(_) => Self::List(OffsetType::Large),
            DataType::Struct(_) => Self::Struct,
            DataType::Union(_, _, _) => Self::Union,
            DataType::Dictionary(k, _) => match k.as_ref() {
                DataType::Int8 => Self::Dictionary(DictionaryKeyType::Int8),
                DataType::Int16 => Self::Dictionary(DictionaryKeyType::Int16),
                DataType::Int32 => Self::Dictionary(DictionaryKeyType::Int32),
                DataType::Int64 => Self::Dictionary(DictionaryKeyType::Int64),
                DataType::UInt8 => Self::Dictionary(DictionaryKeyType::UInt8),
                DataType::UInt16 => Self::Dictionary(DictionaryKeyType::UInt16),
                DataType::UInt32 => Self::Dictionary(DictionaryKeyType::UInt32),
                DataType::UInt64 => Self::Dictionary(DictionaryKeyType::UInt64),
                d => panic!("illegal dictionary key data type {d}"),
            },
            DataType::Map(_, _) => Self::Map,
            DataType::RunEndEncoded(f, _) => match f.data_type() {
                DataType::Int16 => Self::Run(RunEndType::Int16),
                DataType::Int32 => Self::Run(RunEndType::Int32),
                DataType::Int64 => Self::Run(RunEndType::Int64),
                d => panic!("illegal run end data type {d}"),
            },
        }
    }
}
