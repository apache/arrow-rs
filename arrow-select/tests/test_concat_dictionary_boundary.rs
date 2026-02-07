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

#[cfg(test)]
mod test_concat_dictionary_boundary {
    use arrow_array::types::{UInt8Type, UInt16Type};
    use arrow_array::{Array, DictionaryArray, StringArray, UInt8Array, UInt16Array};
    use arrow_select::concat::concat;
    use std::sync::Arc;

    #[test]
    fn test_concat_u8_dictionary_256_values() {
        // Integration test: concat should work with exactly 256 unique values
        let values = StringArray::from((0..256).map(|i| format!("v{}", i)).collect::<Vec<_>>());
        let keys = UInt8Array::from((0..256).map(|i| i as u8).collect::<Vec<_>>());
        let dict = DictionaryArray::<UInt8Type>::try_new(keys, Arc::new(values)).unwrap();

        // Concatenate with itself - should succeed
        let result = concat(&[&dict as &dyn Array, &dict as &dyn Array]);
        assert!(
            result.is_ok(),
            "Concat should succeed with 256 unique values for u8"
        );

        let concatenated = result.unwrap();
        assert_eq!(
            concatenated.len(),
            512,
            "Should have 512 total elements (256 * 2)"
        );
    }

    #[test]
    fn test_concat_u8_dictionary_257_values_fails() {
        // Integration test: concat should fail with 257 distinct values
        let values1 = StringArray::from((0..128).map(|i| format!("a{}", i)).collect::<Vec<_>>());
        let keys1 = UInt8Array::from((0..128).map(|i| i as u8).collect::<Vec<_>>());
        let dict1 = DictionaryArray::<UInt8Type>::try_new(keys1, Arc::new(values1)).unwrap();

        let values2 = StringArray::from((0..129).map(|i| format!("b{}", i)).collect::<Vec<_>>());
        let keys2 = UInt8Array::from((0..129).map(|i| i as u8).collect::<Vec<_>>());
        let dict2 = DictionaryArray::<UInt8Type>::try_new(keys2, Arc::new(values2)).unwrap();

        // Should fail with 257 distinct values
        let result = concat(&[&dict1 as &dyn Array, &dict2 as &dyn Array]);
        assert!(
            result.is_err(),
            "Concat should fail with 257 distinct values for u8"
        );
    }

    #[test]
    fn test_concat_u16_dictionary_65536_values() {
        // Integration test: concat should work with exactly 65,536 unique values for u16
        // Note: This test creates a large array, so it may be slow
        let values = StringArray::from((0..65536).map(|i| format!("v{}", i)).collect::<Vec<_>>());
        let keys = UInt16Array::from((0..65536).map(|i| i as u16).collect::<Vec<_>>());
        let dict = DictionaryArray::<UInt16Type>::try_new(keys, Arc::new(values)).unwrap();

        // Concatenate with itself - should succeed
        let result = concat(&[&dict as &dyn Array, &dict as &dyn Array]);
        assert!(
            result.is_ok(),
            "Concat should succeed with 65,536 unique values for u16"
        );

        let concatenated = result.unwrap();
        assert_eq!(
            concatenated.len(),
            131072,
            "Should have 131,072 total elements (65,536 * 2)"
        );
    }

    #[test]
    fn test_concat_returns_error_not_panic() {
        // Verify that overflow returns an error instead of panicking
        let values1 = StringArray::from((0..200).map(|i| format!("a{}", i)).collect::<Vec<_>>());
        let keys1 = UInt8Array::from((0..200).map(|i| i as u8).collect::<Vec<_>>());
        let dict1 = DictionaryArray::<UInt8Type>::try_new(keys1, Arc::new(values1)).unwrap();

        let values2 = StringArray::from((0..200).map(|i| format!("b{}", i)).collect::<Vec<_>>());
        let keys2 = UInt8Array::from((0..200).map(|i| i as u8).collect::<Vec<_>>());
        let dict2 = DictionaryArray::<UInt8Type>::try_new(keys2, Arc::new(values2)).unwrap();

        // This should return an error, NOT panic
        let result = concat(&[&dict1 as &dyn Array, &dict2 as &dyn Array]);

        // The key test: we successfully got here without panicking!
        // If there was a panic, the test would have failed before reaching this assertion
        assert!(
            result.is_err(),
            "Should return error for overflow, not panic"
        );
    }
}
