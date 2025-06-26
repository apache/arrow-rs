use arrow_schema::ArrowError;
use parquet_variant::{json_to_variant, SampleVecBasedVariantBufferManager};

#[test]
fn test_json_to_variant() -> Result<(), ArrowError> {
    fn compare_results(
        json: &str,
        expected_value: &[u8],
        expected_metadata: &[u8],
    ) -> Result<(), ArrowError> {
        let json = json;

        let mut variant_buffer_manager = SampleVecBasedVariantBufferManager {
            value_buffer: vec![0u8; 1],
            metadata_buffer: vec![0u8; 1],
        };
        let (metadata_size, value_size) = json_to_variant(json, &mut variant_buffer_manager)?;
        let computed_metadata_slize: &[u8] = &*variant_buffer_manager.metadata_buffer;
        let computed_value_slize: &[u8] = &*variant_buffer_manager.value_buffer;
        assert_eq!(&computed_metadata_slize[..metadata_size], expected_metadata);
        assert_eq!(&computed_value_slize[..value_size], expected_value);
        Ok(())
    }

    let empty_metadata: &[u8] = &[1u8, 0u8, 0u8];
    // Null
    compare_results("null", &[0u8], empty_metadata)?;
    // Bool
    compare_results("true", &[4u8], empty_metadata)?;
    compare_results("false", &[8u8], empty_metadata)?;
    // Integers
    compare_results("  127 ", &[12u8, 127u8], empty_metadata)?;
    compare_results("  -128  ", &[12u8, 128u8], empty_metadata)?;
    compare_results(" 27134  ", &[16u8, 254u8, 105u8], empty_metadata)?;
    compare_results(
        " -32767431  ",
        &[20u8, 57u8, 2u8, 12u8, 254u8],
        empty_metadata,
    )?;
    compare_results(
        "92842754201389",
        &[24u8, 45u8, 87u8, 98u8, 163u8, 112u8, 84u8, 0u8, 0u8],
        empty_metadata,
    )?;
    // Decimals
    // Decimal 4
    compare_results("1.23", &[32u8, 2u8, 123u8, 0u8, 0u8, 0u8], empty_metadata)?;
    compare_results(
        "99999999.9",
        &[32u8, 1u8, 0xffu8, 0xc9u8, 0x9au8, 0x3bu8],
        empty_metadata,
    )?;
    compare_results(
        "-99999999.9",
        &[32u8, 1u8, 1u8, 0x36u8, 0x65u8, 0xc4u8],
        empty_metadata,
    )?;
    compare_results(
        "0.999999999",
        &[32u8, 9u8, 0xffu8, 0xc9u8, 0x9au8, 0x3bu8],
        empty_metadata,
    )?;
    compare_results("0.000000001", &[32u8, 9u8, 1u8, 0, 0, 0], empty_metadata)?;
    compare_results(
        "-0.999999999",
        &[32u8, 9u8, 1u8, 0x36u8, 0x65u8, 0xc4u8],
        empty_metadata,
    )?;
    compare_results(
        "-0.000000001",
        &[32u8, 9u8, 0xffu8, 0xffu8, 0xffu8, 0xffu8],
        empty_metadata,
    )?;
    // Decimal 8
    compare_results(
        "999999999.0",
        &[36u8, 1u8, 0xf6u8, 0xe3u8, 0x0bu8, 0x54u8, 0x02u8, 0, 0, 0],
        empty_metadata,
    )?;
    compare_results(
        "-999999999.0",
        &[
            36u8, 1u8, 0x0au8, 0x1cu8, 0xf4u8, 0xabu8, 0xfdu8, 0xffu8, 0xffu8, 0xffu8,
        ],
        empty_metadata,
    )?;
    compare_results(
        "0.999999999999999999",
        &[
            36u8, 18u8, 0xffu8, 0xffu8, 0x63u8, 0xa7u8, 0xb3u8, 0xb6u8, 0xe0u8, 0x0du8,
        ],
        empty_metadata,
    )?;
    compare_results(
        "-9999999999999999.99",
        &[
            36u8, 2u8, 0x01u8, 0x00u8, 0x9cu8, 0x58u8, 0x4cu8, 0x49u8, 0x1fu8, 0xf2u8,
        ],
        empty_metadata,
    )?;
    // Decimal 16
    compare_results(
        "9999999999999999999", // integer larger than i64
        &[
            40u8, 0u8, 0xffu8, 0xffu8, 0xe7u8, 0x89u8, 4u8, 0x23u8, 0xc7u8, 0x8au8, 0u8, 0u8, 0u8,
            0u8, 0u8, 0u8, 0u8, 0u8,
        ],
        empty_metadata,
    )?;
    compare_results(
        "0.9999999999999999999",
        &[
            40u8, 19u8, 0xffu8, 0xffu8, 0xe7u8, 0x89u8, 4u8, 0x23u8, 0xc7u8, 0x8au8, 0u8, 0u8, 0u8,
            0u8, 0u8, 0u8, 0u8, 0u8,
        ],
        empty_metadata,
    )?;
    compare_results(
        "79228162514264337593543950335", // 2 ^ 96 - 1
        &[
            40u8, 0u8, 0xffu8, 0xffu8, 0xffu8, 0xffu8, 0xffu8, 0xffu8, 0xffu8, 0xffu8, 0xffu8,
            0xffu8, 0xffu8, 0xffu8, 0u8, 0u8, 0u8, 0u8,
        ],
        empty_metadata,
    )?;
    compare_results(
        "7.9228162514264337593543950335", // using scale higher than this falls into double
        // since the max scale is 28.
        &[
            40u8, 28u8, 0xffu8, 0xffu8, 0xffu8, 0xffu8, 0xffu8, 0xffu8, 0xffu8, 0xffu8, 0xffu8,
            0xffu8, 0xffu8, 0xffu8, 0u8, 0u8, 0u8, 0u8,
        ],
        empty_metadata,
    )?;
    // Double
    {
        let mut arr = [28u8; 9];
        arr[1..].copy_from_slice(&0.79228162514264337593543950335f64.to_le_bytes());
        compare_results("0.79228162514264337593543950335", &arr, empty_metadata)?;
    }
    compare_results(
        "15e-1",
        &[28u8, 0, 0, 0, 0, 0, 0, 0xf8, 0x3fu8],
        empty_metadata,
    )?;
    compare_results(
        "-15e-1",
        &[28u8, 0, 0, 0, 0, 0, 0, 0xf8, 0xBfu8],
        empty_metadata,
    )?;

    // short strings
    // random short string
    compare_results(
        "\"harsh\"",
        &[21u8, 104u8, 97u8, 114u8, 115u8, 104u8],
        empty_metadata,
    )?;
    // longest short string
    let mut expected = [97u8; 64];
    expected[0] = 253u8;
    compare_results(
        &format!(
            "\"{}\"",
            std::iter::repeat('a').take(63).collect::<String>()
        ),
        &expected,
        empty_metadata,
    )?;
    // long strings
    let mut expected = [97u8; 69];
    expected[..5].copy_from_slice(&[64u8, 64u8, 0, 0, 0]);
    compare_results(
        &format!(
            "\"{}\"",
            std::iter::repeat('a').take(64).collect::<String>()
        ),
        &expected,
        empty_metadata,
    )?;
    let mut expected = [98u8; 100005];
    expected[0] = 64u8;
    expected[1..5].copy_from_slice(&(100000 as u32).to_le_bytes());
    compare_results(
        &format!(
            "\"{}\"",
            std::iter::repeat('b').take(100000).collect::<String>()
        ),
        &expected,
        empty_metadata,
    )?;

    // arrays
    // u8 offset
    compare_results(
        "[127, 128, -32767431]",
        &[
            3u8, 3u8, 0u8, 2u8, 5u8, 10u8, 12u8, 127u8, 16u8, 128u8, 0u8, 20u8, 57u8, 2u8, 12u8,
            254u8,
        ],
        empty_metadata,
    )?;
    compare_results(
        "[[\"a\", null, true, 4], 128, false]",
        &[
            3u8, 3u8, 0u8, 13u8, 16u8, 17u8, 3u8, 4u8, 0u8, 2u8, 3u8, 4u8, 6u8, 5u8, 97u8, 0u8,
            4u8, 12u8, 4u8, 16u8, 128u8, 0u8, 8u8,
        ],
        empty_metadata,
    )?;
    compare_results(
        "[{\"age\": 32}, 128, false]",
        &[
            3u8, 3u8, 0u8, 7u8, 10u8, 11u8, 2u8, 1u8, 0u8, 0u8, 2u8, 12u8, 32u8, 16u8, 128u8, 0u8,
            8u8,
        ],
        &[1u8, 1u8, 0u8, 3u8, 97u8, 103u8, 101u8],
    )?;
    // u16 offset - 128 i8's + 1 "true" = 257 bytes
    compare_results(
        &format!(
            "[{} true]",
            std::iter::repeat("1, ").take(128).collect::<String>()
        ),
        &[
            7u8, 129u8, 0, 0, 2, 0, 4, 0, 6, 0, 8, 0, 10, 0, 12, 0, 14, 0, 16, 0, 18, 0, 20, 0, 22,
            0, 24, 0, 26, 0, 28, 0, 30, 0, 32, 0, 34, 0, 36, 0, 38, 0, 40, 0, 42, 0, 44, 0, 46, 0,
            48, 0, 50, 0, 52, 0, 54, 0, 56, 0, 58, 0, 60, 0, 62, 0, 64, 0, 66, 0, 68, 0, 70, 0, 72,
            0, 74, 0, 76, 0, 78, 0, 80, 0, 82, 0, 84, 0, 86, 0, 88, 0, 90, 0, 92, 0, 94, 0, 96, 0,
            98, 0, 100, 0, 102, 0, 104, 0, 106, 0, 108, 0, 110, 0, 112, 0, 114, 0, 116, 0, 118, 0,
            120, 0, 122, 0, 124, 0, 126, 0, 128, 0, 130, 0, 132, 0, 134, 0, 136, 0, 138, 0, 140, 0,
            142, 0, 144, 0, 146, 0, 148, 0, 150, 0, 152, 0, 154, 0, 156, 0, 158, 0, 160, 0, 162, 0,
            164, 0, 166, 0, 168, 0, 170, 0, 172, 0, 174, 0, 176, 0, 178, 0, 180, 0, 182, 0, 184, 0,
            186, 0, 188, 0, 190, 0, 192, 0, 194, 0, 196, 0, 198, 0, 200, 0, 202, 0, 204, 0, 206, 0,
            208, 0, 210, 0, 212, 0, 214, 0, 216, 0, 218, 0, 220, 0, 222, 0, 224, 0, 226, 0, 228, 0,
            230, 0, 232, 0, 234, 0, 236, 0, 238, 0, 240, 0, 242, 0, 244, 0, 246, 0, 248, 0, 250, 0,
            252, 0, 254, 0, 0, 1, 1, 1, // Final offset is 257
            12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12,
            1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1,
            12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12,
            1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1,
            12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12,
            1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1,
            12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12,
            1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1,
            12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12,
            1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1,
            12, 1, 12, 1, 12, 1, 4u8,
        ],
        empty_metadata,
    )?;
    // verify u24, and large_size
    {
        let null_array: [u8; 513] = std::array::from_fn(|i| match i {
            0 => 3u8,
            1 => 255u8,
            j => {
                if j <= 257 {
                    (j - 2) as u8
                } else {
                    0u8
                }
            }
        });
        // 256 elements => large size
        // each element is an array of 256 nulls => u24 offset
        let mut whole_array: [u8; 5 + 3 * 257 + 256 * 513] = std::array::from_fn(|i| match i {
            0 => 0x1Bu8,
            1 => 0u8,
            2 => 1u8,
            3 => 0u8,
            4 => 0u8,
            _ => 0,
        });
        for i in 0..257 {
            let cur_idx = 5 + i * 3 as usize;
            let cur_offset: usize = i * 513;
            whole_array[cur_idx..cur_idx + 3].copy_from_slice(&cur_offset.to_le_bytes()[..3]);
            if i != 256 {
                let abs_offset = 5 + 3 * 257 + cur_offset;
                whole_array[abs_offset..abs_offset + 513].copy_from_slice(&null_array);
            }
        }
        let intermediate = format!("[{}]", vec!["null"; 255].join(", "));
        let json = format!("[{}]", vec![intermediate; 256].join(", "));
        compare_results(json.as_str(), &whole_array, empty_metadata)?;
    }

    // // objects
    compare_results(
        "{\"b\": 2, \"a\": 1, \"a\": 3}",
        &[2u8, 2u8, 1u8, 0u8, 2u8, 0u8, 4u8, 12u8, 2u8, 12u8, 3u8],
        &[1, 2, 0, 1, 2, 98u8, 97u8],
    )?;

    // TODO: verify different offset_size, id_size, is_large values, nesting and more object
    // tests in general
    // NOTE: objects and lists are treated as `pending` and are added to the dictionary after scalar
    // values. Therefore "null" has ID 0, "numbers" as ID 1 and "booleans" has ID 2.
    compare_results(
        "{\"numbers\": [4, -3e0, 1.001], \"null\": null, \"booleans\": [true, false]}",
        &[
            2u8, 3u8, 2u8, 1u8, 0u8, 24u8, 23u8, 0u8, 31u8, 3u8, 3u8, 0u8, 2u8, 11u8, 17u8, 12u8,
            4u8, 28u8, 0, 0, 0, 0, 0, 0, 0x08, 0xc0, 32u8, 3, 0xe9, 0x03, 0, 0, 0, 3u8, 2u8, 0u8,
            1u8, 2u8, 4u8, 8u8,
        ],
        &[
            1u8, 3u8, 0u8, 7u8, 11u8, 19u8, 0x6eu8, 0x75u8, 0x6du8, 0x62u8, 0x65u8, 0x72u8,
            0x73u8, 0x6eu8, 0x75u8, 0x6cu8, 0x6cu8, 0x62u8, 0x6fu8, 0x6fu8, 0x6cu8, 0x65u8,
            0x61u8, 0x6eu8, 0x73u8,
        ],
    )?;

    Ok(())
}
