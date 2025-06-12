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
mod tests {
    use crate::{RowConverter, SortField};
    use arrow_array::types::Int32Type;
    use arrow_array::RunArray;
    use arrow_schema::DataType;
    use std::sync::Arc;

    #[test]
    fn test_run_end_encoded_supports_datatype() {
        // Test that the RowConverter correctly supports run-end encoded arrays
        assert!(RowConverter::supports_datatype(&DataType::RunEndEncoded(
            Arc::new(arrow_schema::Field::new("run_ends", DataType::Int32, false)),
            Arc::new(arrow_schema::Field::new("values", DataType::Utf8, true)),
        )));
    }

    #[test]
    fn test_run_end_encoded_sorting() {
        // Create two run arrays with different values
        let run_array1: RunArray<Int32Type> = vec!["b", "b", "a"].into_iter().collect();
        let run_array2: RunArray<Int32Type> = vec!["a", "a", "b"].into_iter().collect();

        let converter = RowConverter::new(vec![SortField::new(DataType::RunEndEncoded(
            Arc::new(arrow_schema::Field::new("run_ends", DataType::Int32, false)),
            Arc::new(arrow_schema::Field::new("values", DataType::Utf8, true)),
        ))])
        .unwrap();

        let rows1 = converter.convert_columns(&[Arc::new(run_array1)]).unwrap();
        let rows2 = converter.convert_columns(&[Arc::new(run_array2)]).unwrap();

        // Compare rows - the second row should be less than the first
        assert!(rows2.row(0) < rows1.row(0));
        assert!(rows2.row(1) < rows1.row(0));
        assert!(rows1.row(2) < rows2.row(2));
    }
}
