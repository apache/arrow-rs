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

//! Manually tests if parsing JSON strings to Variants returns the expected results.

use arrow_schema::ArrowError;
use parquet_variant::{
    json_to_variant, variant_to_json_string, ShortString, Variant, VariantBuilder,
    VariantDecimal16, VariantDecimal4, VariantDecimal8,
};

#[test]
fn test_json_to_variant() -> Result<(), ArrowError> {
    struct JsonToVariantTest<'a> {
        json: &'a str,
        expected: Variant<'a, 'a>,
    }

    impl<'a> JsonToVariantTest<'a> {
        fn run(self) -> Result<(), ArrowError> {
            let mut variant_builder = VariantBuilder::new();
            json_to_variant(self.json, &mut variant_builder)?;
            let (metadata, value) = variant_builder.finish();
            let variant = Variant::try_new(&metadata, &value)?;
            assert_eq!(variant, self.expected);
            Ok(())
        }
    }

    // Null
    JsonToVariantTest {
        json: "null",
        expected: Variant::Null,
    }
    .run()?;
    // Bool
    JsonToVariantTest {
        json: "true",
        expected: Variant::BooleanTrue,
    }
    .run()?;
    JsonToVariantTest {
        json: "false",
        expected: Variant::BooleanFalse,
    }
    .run()?;
    // Integers
    JsonToVariantTest {
        json: "  127 ",
        expected: Variant::Int8(127),
    }
    .run()?;
    JsonToVariantTest {
        json: "  -128 ",
        expected: Variant::Int8(-128),
    }
    .run()?;
    JsonToVariantTest {
        json: "  27134  ",
        expected: Variant::Int16(27134),
    }
    .run()?;
    JsonToVariantTest {
        json: " -32767431  ",
        expected: Variant::Int32(-32767431),
    }
    .run()?;
    JsonToVariantTest {
        json: "92842754201389",
        expected: Variant::Int64(92842754201389),
    }
    .run()?;
    // Decimals
    // Decimal 4
    JsonToVariantTest {
        json: "1.23",
        expected: Variant::from(VariantDecimal4::try_new(123, 2)?),
    }
    .run()?;
    JsonToVariantTest {
        json: "99999999.9",
        expected: Variant::from(VariantDecimal4::try_new(999999999, 1)?),
    }
    .run()?;
    JsonToVariantTest {
        json: "-99999999.9",
        expected: Variant::from(VariantDecimal4::try_new(-999999999, 1)?),
    }
    .run()?;
    JsonToVariantTest {
        json: "0.999999999",
        expected: Variant::from(VariantDecimal4::try_new(999999999, 9)?),
    }
    .run()?;
    JsonToVariantTest {
        json: "0.000000001",
        expected: Variant::from(VariantDecimal4::try_new(1, 9)?),
    }
    .run()?;
    JsonToVariantTest {
        json: "-0.999999999",
        expected: Variant::from(VariantDecimal4::try_new(-999999999, 9)?),
    }
    .run()?;
    // Decimal 8
    JsonToVariantTest {
        json: "999999999.0",
        expected: Variant::from(VariantDecimal8::try_new(9999999990, 1)?),
    }
    .run()?;
    JsonToVariantTest {
        json: "-999999999.0",
        expected: Variant::from(VariantDecimal8::try_new(-9999999990, 1)?),
    }
    .run()?;
    JsonToVariantTest {
        json: "0.999999999999999999",
        expected: Variant::from(VariantDecimal8::try_new(999999999999999999, 18)?),
    }
    .run()?;
    JsonToVariantTest {
        json: "9999999999999999.99",
        expected: Variant::from(VariantDecimal8::try_new(999999999999999999, 2)?),
    }
    .run()?;
    JsonToVariantTest {
        json: "-9999999999999999.99",
        expected: Variant::from(VariantDecimal8::try_new(-999999999999999999, 2)?),
    }
    .run()?;
    // Decimal 16
    JsonToVariantTest {
        json: "9999999999999999999", // integer larger than i64
        expected: Variant::from(VariantDecimal16::try_new(9999999999999999999, 0)?),
    }
    .run()?;
    JsonToVariantTest {
        json: "0.9999999999999999999",
        expected: Variant::from(VariantDecimal16::try_new(9999999999999999999, 19)?),
    }
    .run()?;
    JsonToVariantTest {
        json: "79228162514264337593543950335", // 2 ^ 96 - 1
        expected: Variant::from(VariantDecimal16::try_new(79228162514264337593543950335, 0)?),
    }
    .run()?;
    JsonToVariantTest {
        json: "7.9228162514264337593543950335", // using scale higher than this falls into double
        // since the max scale is 28.
        expected: Variant::from(VariantDecimal16::try_new(
            79228162514264337593543950335,
            28,
        )?),
    }
    .run()?;
    // Double
    JsonToVariantTest {
        json: "0.79228162514264337593543950335",
        expected: Variant::Double(0.79228162514264337593543950335f64),
    }
    .run()?;
    JsonToVariantTest {
        json: "15e-1",
        expected: Variant::Double(15e-1f64),
    }
    .run()?;
    JsonToVariantTest {
        json: "-15e-1",
        expected: Variant::Double(-15e-1f64),
    }
    .run()?;
    // short strings
    // random short string
    JsonToVariantTest {
        json: "\"harsh\"",
        expected: Variant::ShortString(ShortString::try_new("harsh")?),
    }
    .run()?;
    // longest short string
    JsonToVariantTest {
        json: &format!(
            "\"{}\"",
            std::iter::repeat('a').take(63).collect::<String>()
        ),
        expected: Variant::ShortString(ShortString::try_new(&format!(
            "{}",
            std::iter::repeat('a').take(63).collect::<String>()
        ))?),
    }
    .run()?;
    // long strings
    JsonToVariantTest {
        json: &format!(
            "\"{}\"",
            std::iter::repeat('a').take(64).collect::<String>()
        ),
        expected: Variant::String(&format!(
            "{}",
            std::iter::repeat('a').take(64).collect::<String>()
        )),
    }
    .run()?;
    JsonToVariantTest {
        json: &format!(
            "\"{}\"",
            std::iter::repeat('b').take(100000).collect::<String>()
        ),
        expected: Variant::String(&format!(
            "{}",
            std::iter::repeat('b').take(100000).collect::<String>()
        )),
    }
    .run()?;

    // arrays
    // u8 offset
    {
        let mut variant_builder = VariantBuilder::new();
        let mut list_builder = variant_builder.new_list();
        list_builder.append_value(Variant::Int8(127));
        list_builder.append_value(Variant::Int16(128));
        list_builder.append_value(Variant::Int32(-32767431));
        list_builder.finish();
        let (metadata, value) = variant_builder.finish();
        let variant = Variant::try_new(&metadata, &value)?;

        JsonToVariantTest {
            json: "[127, 128, -32767431]",
            expected: variant,
        }
        .run()?;
    }
    {
        let mut variant_builder = VariantBuilder::new();
        let mut list_builder = variant_builder.new_list();
        let mut object_builder_inner = list_builder.new_object();
        object_builder_inner.insert("age", Variant::Int8(32));
        object_builder_inner.finish();
        list_builder.append_value(Variant::Int16(128));
        list_builder.append_value(Variant::BooleanFalse);
        list_builder.finish();
        let (metadata, value) = variant_builder.finish();
        let variant = Variant::try_new(&metadata, &value)?;

        JsonToVariantTest {
            json: "[{\"age\": 32}, 128, false]",
            expected: variant,
        }
        .run()?;
    }
    // u16 offset - 128 i8's + 1 "true" = 257 bytes
    {
        let mut variant_builder = VariantBuilder::new();
        let mut list_builder = variant_builder.new_list();
        for _ in 0..128 {
            list_builder.append_value(Variant::Int8(1));
        }
        list_builder.append_value(Variant::BooleanTrue);
        list_builder.finish();
        let (metadata, value) = variant_builder.finish();
        let variant = Variant::try_new(&metadata, &value)?;

        JsonToVariantTest {
            json: &format!(
                "[{} true]",
                std::iter::repeat("1, ").take(128).collect::<String>()
            ),
            expected: variant,
        }
        .run()?;
    }
    // verify u24, and large_size
    {
        let mut variant_builder = VariantBuilder::new();
        let mut list_builder = variant_builder.new_list();
        for _ in 0..256 {
            let mut list_builder_inner = list_builder.new_list();
            for _ in 0..255 {
                list_builder_inner.append_value(Variant::Null);
            }
            list_builder_inner.finish();
        }
        list_builder.finish();
        let (metadata, value) = variant_builder.finish();
        let variant = Variant::try_new(&metadata, &value)?;
        let intermediate = format!("[{}]", vec!["null"; 255].join(", "));
        let json = format!("[{}]", vec![intermediate; 256].join(", "));
        JsonToVariantTest {
            json: json.as_str(),
            expected: variant,
        }
        .run()?;
    }

    // objects
    {
        let mut variant_builder = VariantBuilder::new();
        let mut object_builder = variant_builder.new_object();
        object_builder.insert("b", Variant::Int8(2));
        object_builder.insert("a", Variant::Int8(3));
        object_builder.finish();
        let (metadata, value) = variant_builder.finish();
        let variant = Variant::try_new(&metadata, &value)?;
        JsonToVariantTest {
            json: "{\"b\": 2, \"a\": 1, \"a\": 3}",
            expected: variant,
        }
        .run()?;
    }
    {
        let mut variant_builder = VariantBuilder::new();
        let mut object_builder = variant_builder.new_object();
        let mut inner_list_builder = object_builder.new_list("numbers");
        inner_list_builder.append_value(Variant::Int8(4));
        inner_list_builder.append_value(Variant::Double(-3e0));
        inner_list_builder.append_value(Variant::Decimal4(VariantDecimal4::try_new(1001, 3)?));
        inner_list_builder.finish();
        object_builder.insert("null", Variant::Null);
        let mut inner_list_builder = object_builder.new_list("booleans");
        inner_list_builder.append_value(Variant::BooleanTrue);
        inner_list_builder.append_value(Variant::BooleanFalse);
        inner_list_builder.finish();
        object_builder.finish();
        let (metadata, value) = variant_builder.finish();
        let variant = Variant::try_new(&metadata, &value)?;
        JsonToVariantTest {
            json: "{\"numbers\": [4, -3e0, 1.001], \"null\": null, \"booleans\": [true, false]}",
            expected: variant,
        }
        .run()?;
    }
    {
        let mut variant_builder = VariantBuilder::new();
        let mut object_builder = variant_builder.new_object();
        let mut inner_list_builder = object_builder.new_list("numbers");
        inner_list_builder.append_value(Variant::Int8(4));
        inner_list_builder.append_value(Variant::Double(-3e0));
        inner_list_builder.append_value(Variant::Decimal4(VariantDecimal4::try_new(1001, 3)?));
        inner_list_builder.finish();
        object_builder.insert("null", Variant::Null);
        let mut inner_list_builder = object_builder.new_list("booleans");
        inner_list_builder.append_value(Variant::BooleanTrue);
        inner_list_builder.append_value(Variant::BooleanFalse);
        inner_list_builder.finish();
        object_builder.finish();
        let (metadata, value) = variant_builder.finish();
        let variant = Variant::try_new(&metadata, &value)?;
        JsonToVariantTest {
            json: "{\"numbers\": [4, -3e0, 1.001], \"null\": null, \"booleans\": [true, false]}",
            expected: variant,
        }
        .run()?;
    }
    {
        // 256 elements (keys: 000-255) - each element is an object of 256 elements (240-495) - each
        // element a list of numbers from 0-127
        let keys: Vec<String> = (0..=255).map(|n| format!("{:03}", n)).collect();
        let innermost_list: String = format!(
            "[{}]",
            (0..=127)
                .map(|n| format!("{}", n))
                .collect::<Vec<_>>()
                .join(",")
        );
        let inner_keys: Vec<String> = (240..=495).map(|n| format!("{}", n)).collect();
        let inner_object = format!(
            "{{{}:{}}}",
            inner_keys
                .iter()
                .map(|k| format!("\"{}\"", k))
                .collect::<Vec<String>>()
                .join(format!(":{},", innermost_list).as_str()),
            innermost_list
        );
        let json = format!(
            "{{{}:{}}}",
            keys.iter()
                .map(|k| format!("\"{}\"", k))
                .collect::<Vec<String>>()
                .join(format!(":{},", inner_object).as_str()),
            inner_object
        );
        // Manually verify raw JSON value size
        let mut variant_builder = VariantBuilder::new();
        json_to_variant(&json, &mut variant_builder)?;
        let (metadata, value) = variant_builder.finish();
        let v = parquet_variant::Variant::try_new(&metadata, &value)?;
        let output_string = variant_to_json_string(&v)?;
        assert_eq!(output_string, json);
        // Verify metadata size = 1 + 2 + 2 * 497 + 3 * 496
        assert_eq!(metadata.len(), 2485);
        // Verify value size.
        // Size of innermost_list: 1 + 1 + 258 + 256 = 516
        // Size of inner object: 1 + 4 + 256 + 257 * 3 + 256 * 516 = 133128
        // Size of json: 1 + 4 + 512 + 1028 + 256 * 133128 = 34082313
        assert_eq!(value.len(), 34082313);

        let mut variant_builder = VariantBuilder::new();
        let mut object_builder = variant_builder.new_object();
        keys.iter().for_each(|key| {
            let mut inner_object_builder = object_builder.new_object(key);
            inner_keys.iter().for_each(|inner_key| {
                let mut list_builder = inner_object_builder.new_list(&inner_key);
                for i in 0..=127 {
                    list_builder.append_value(Variant::Int8(i));
                }
                list_builder.finish();
            });
            inner_object_builder.finish();
        });
        object_builder.finish();
        let (metadata, value) = variant_builder.finish();
        let variant = Variant::try_new(&metadata, &value)?;

        JsonToVariantTest {
            json: &json,
            expected: variant,
        }
        .run()?;
    }
    {
        let json = "{\"爱\":\"अ\",\"a\":1}";
        let mut variant_builder = VariantBuilder::new();
        json_to_variant(&json, &mut variant_builder)?;
        let (metadata, value) = variant_builder.finish();
        let v = parquet_variant::Variant::try_new(&metadata, &value)?;
        let output_string = variant_to_json_string(&v)?;
        assert_eq!(output_string, "{\"a\":1,\"爱\":\"अ\"}");
        let mut variant_builder = VariantBuilder::new();
        let mut object_builder = variant_builder.new_object();
        object_builder.insert("爱", Variant::ShortString(ShortString::try_new("अ")?));
        object_builder.insert("a", Variant::Int8(1));
        object_builder.finish();
        let (metadata, value) = variant_builder.finish();
        let variant = Variant::try_new(&metadata, &value)?;

        assert_eq!(
            value,
            &[2u8, 2u8, 1u8, 0u8, 4u8, 0u8, 6u8, 13u8, 0xe0u8, 0xa4u8, 0x85u8, 12u8, 1u8]
        );
        assert_eq!(
            metadata,
            &[1u8, 2u8, 0u8, 3u8, 4u8, 0xe7u8, 0x88u8, 0xb1u8, 97u8]
        );
        JsonToVariantTest {
            json: &json,
            expected: variant,
        }
        .run()?;
    }

    Ok(())
}
