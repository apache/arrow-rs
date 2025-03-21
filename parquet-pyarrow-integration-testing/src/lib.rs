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

//! This library tests interoperability of encrypted Parquet
//! files with PyArrow.

use std::fs::File;
use std::sync::Arc;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyString;
use pyo3::wrap_pyfunction;
use arrow::array::{Float32Builder, StructArray, UInt64Builder};
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::encryption::key_management::crypto_factory::{CryptoFactory, EncryptionConfigurationBuilder};
use parquet::encryption::key_management::kms::KmsConnectionConfig;
use parquet::encryption::key_management::test_kms::TestKmsClientFactory;
use parquet::errors::ParquetError;
use parquet::file::properties::WriterProperties;

fn to_py_err(err: ParquetError) -> PyErr {
    PyRuntimeError::new_err(err.to_string())
}

/// Writes an encrypted Parquet file to the specified path
#[pyfunction]
fn write_encrypted_parquet(file_path: &Bound<PyString>, py: Python) -> PyResult<()> {
    let file_path: String = file_path.extract()?;
    let file = File::create(file_path)?;

    let client_factory = TestKmsClientFactory::with_default_keys();
    let crypto_factory = CryptoFactory::new(client_factory);

    let encryption_config = EncryptionConfigurationBuilder::new("kf".into())
        .add_column_key("kc1".into(), vec!["x".into()])
        .build();
    let connection_config = Arc::new(KmsConnectionConfig::default());

    // Use the CryptoFactory to generate file encryption properties
    let encryption_properties =
        crypto_factory.file_encryption_properties(connection_config.clone(), &encryption_config).map_err(to_py_err)?;
    let properties = WriterProperties::builder()
        .with_file_encryption_properties(encryption_properties)
        .build();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("x", DataType::Float32, false),
        Field::new("y", DataType::Float32, false),
    ]));

    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(properties)).map_err(to_py_err)?;

    let mut id_builder = UInt64Builder::new();
    let mut x_builder = Float32Builder::new();
    let mut y_builder = Float32Builder::new();
    let num_rows = 10;
    for i in 0..num_rows {
        id_builder.append_value(i);
        x_builder.append_value(i as f32 / 10.0);
        y_builder.append_value(i as f32 / 100.0);
    }
    writer.write(
        &StructArray::new(
            schema.fields().clone(),
            vec![
                Arc::new(id_builder.finish()),
                Arc::new(x_builder.finish()),
                Arc::new(y_builder.finish()),
            ],
            None,
        )
            .into(),
    ).map_err(to_py_err)?;
    writer.flush().map_err(to_py_err)?;
    writer.close().map_err(to_py_err)?;

    Ok(())
}

#[pymodule]
fn parquet_pyarrow_integration_testing(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_wrapped(wrap_pyfunction!(write_encrypted_parquet))?;
    Ok(())
}
