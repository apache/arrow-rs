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

/// An enumeration of the primitive types implementing [`ArrowNativeType`]
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

#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub enum PhysicalType {
    Primitive(PrimitiveType),
}

impl From<&DataType> for PhysicalType {
    fn from(value: &DataType) -> Self {
        match value {
            DataType::Null => todo!(),
            DataType::Boolean => todo!(),
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
            DataType::Binary => todo!(),
            DataType::FixedSizeBinary(_) => todo!(),
            DataType::LargeBinary => todo!(),
            DataType::Utf8 => todo!(),
            DataType::LargeUtf8 => todo!(),
            DataType::List(_) => todo!(),
            DataType::FixedSizeList(_, _) => todo!(),
            DataType::LargeList(_) => todo!(),
            DataType::Struct(_) => todo!(),
            DataType::Union(_, _, _) => todo!(),
            DataType::Dictionary(_, _) => todo!(),
            DataType::Map(_, _) => todo!(),
            DataType::RunEndEncoded(_, _) => todo!(),
        }
    }
}
