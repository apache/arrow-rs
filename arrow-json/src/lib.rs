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

//! Transfer data between the Arrow memory format and JSON
//! line-delimited records. See the module level documentation for the
//! [`reader`] and [`writer`] for usage examples.

#![deny(rustdoc::broken_intra_doc_links)]
#![warn(missing_docs)]

pub mod reader;
pub mod writer;

pub use self::reader::Reader;
pub use self::reader::ReaderBuilder;
pub use self::writer::{ArrayWriter, LineDelimitedWriter, Writer};
use half::f16;
use serde_json::{Number, Value};

/// Trait declaring any type that is serializable to JSON. This includes all primitive types (bool, i32, etc.).
pub trait JsonSerializable: 'static {
    /// Converts self into json value if its possible
    fn into_json_value(self) -> Option<Value>;
}

macro_rules! json_serializable {
    ($t:ty) => {
        impl JsonSerializable for $t {
            fn into_json_value(self) -> Option<Value> {
                Some(self.into())
            }
        }
    };
}

json_serializable!(bool);
json_serializable!(u8);
json_serializable!(u16);
json_serializable!(u32);
json_serializable!(u64);
json_serializable!(i8);
json_serializable!(i16);
json_serializable!(i32);
json_serializable!(i64);

impl JsonSerializable for i128 {
    fn into_json_value(self) -> Option<Value> {
        // Serialize as string to avoid issues with arbitrary_precision serde_json feature
        // - https://github.com/serde-rs/json/issues/559
        // - https://github.com/serde-rs/json/issues/845
        // - https://github.com/serde-rs/json/issues/846
        Some(self.to_string().into())
    }
}

impl JsonSerializable for f16 {
    fn into_json_value(self) -> Option<Value> {
        Number::from_f64(f64::round(f64::from(self) * 1000.0) / 1000.0).map(Value::Number)
    }
}

impl JsonSerializable for f32 {
    fn into_json_value(self) -> Option<Value> {
        Number::from_f64(f64::round(self as f64 * 1000.0) / 1000.0).map(Value::Number)
    }
}

impl JsonSerializable for f64 {
    fn into_json_value(self) -> Option<Value> {
        Number::from_f64(self).map(Value::Number)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::{
        Number,
        Value::{Bool, Number as VNumber, String as VString},
    };

    #[test]
    fn test_arrow_native_type_to_json() {
        assert_eq!(Some(Bool(true)), true.into_json_value());
        assert_eq!(Some(VNumber(Number::from(1))), 1i8.into_json_value());
        assert_eq!(Some(VNumber(Number::from(1))), 1i16.into_json_value());
        assert_eq!(Some(VNumber(Number::from(1))), 1i32.into_json_value());
        assert_eq!(Some(VNumber(Number::from(1))), 1i64.into_json_value());
        assert_eq!(Some(VString("1".to_string())), 1i128.into_json_value());
        assert_eq!(Some(VNumber(Number::from(1))), 1u8.into_json_value());
        assert_eq!(Some(VNumber(Number::from(1))), 1u16.into_json_value());
        assert_eq!(Some(VNumber(Number::from(1))), 1u32.into_json_value());
        assert_eq!(Some(VNumber(Number::from(1))), 1u64.into_json_value());
        assert_eq!(
            Some(VNumber(Number::from_f64(0.01f64).unwrap())),
            0.01.into_json_value()
        );
        assert_eq!(
            Some(VNumber(Number::from_f64(0.01f64).unwrap())),
            0.01f64.into_json_value()
        );
        assert_eq!(None, f32::NAN.into_json_value());
    }
}
