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
mod test_main_branch_bug {
    use arrow_array::types::UInt8Type;
    use arrow_array::{Array, DictionaryArray, StringArray, UInt8Array};
    use arrow_select::concat::concat;
    use std::sync::Arc;

    #[test]
    fn test_u8_dictionary_256_values_on_main() {
        // This test should FAIL on main branch (proving the bug exists)
        // and PASS on the fix branch (proving the fix works)

        let values = StringArray::from((0..256).map(|i| format!("v{}", i)).collect::<Vec<_>>());
        let keys = UInt8Array::from((0..256).map(|i| i as u8).collect::<Vec<_>>());
        let dict = DictionaryArray::<UInt8Type>::try_new(keys, Arc::new(values)).unwrap();

        let result = concat(&[&dict as &dyn Array, &dict as &dyn Array]);

        // On main: this should FAIL
        // On fix branch: this should PASS
        assert!(result.is_ok(), "256 values should work with u8 keys");
    }
}
