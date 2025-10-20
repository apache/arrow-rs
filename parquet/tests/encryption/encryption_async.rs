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

//! This module contains tests for reading encrypted Parquet files with the async Arrow API

use crate::encryption_util::{
    TestKeyRetriever, read_encrypted_file, verify_column_indexes,
    verify_encryption_double_test_data, verify_encryption_test_data,
};
use arrow_array::RecordBatch;
use arrow_schema::Schema;
use futures::TryStreamExt;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::arrow_writer::{
    ArrowColumnChunk, ArrowColumnWriter, ArrowLeafColumn, ArrowRowGroupWriterFactory,
    ArrowWriterOptions, compute_leaves,
};
use parquet::arrow::{
    ArrowSchemaConverter, ArrowWriter, AsyncArrowWriter, ParquetRecordBatchStreamBuilder,
};
use parquet::encryption::decrypt::FileDecryptionProperties;
use parquet::encryption::encrypt::FileEncryptionProperties;
use parquet::errors::ParquetError;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::properties::{WriterProperties, WriterPropertiesBuilder};
use parquet::file::writer::SerializedFileWriter;
use std::io::Write;
use std::sync::Arc;
use tokio::fs::File;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinHandle;

#[tokio::test]
async fn test_non_uniform_encryption_plaintext_footer() {
    let test_data = arrow::util::test_util::parquet_test_data();
    let path = format!("{test_data}/encrypt_columns_plaintext_footer.parquet.encrypted");
    let mut file = File::open(&path).await.unwrap();

    // There is always a footer key even with a plaintext footer,
    // but this is used for signing the footer.
    let footer_key = "0123456789012345".as_bytes().to_vec(); // 128bit/16
    let column_1_key = "1234567890123450".as_bytes().to_vec();
    let column_2_key = "1234567890123451".as_bytes().to_vec();

    let decryption_properties = FileDecryptionProperties::builder(footer_key)
        .with_column_key("double_field", column_1_key)
        .with_column_key("float_field", column_2_key)
        .build()
        .unwrap();

    verify_encryption_test_file_read_async(&mut file, decryption_properties)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_misspecified_encryption_keys() {
    let test_data = arrow::util::test_util::parquet_test_data();
    let path = format!("{test_data}/encrypt_columns_and_footer.parquet.encrypted");

    // There is always a footer key even with a plaintext footer,
    // but this is used for signing the footer.
    let footer_key = "0123456789012345".as_bytes(); // 128bit/16
    let column_1_key = "1234567890123450".as_bytes();
    let column_2_key = "1234567890123451".as_bytes();

    // read file with keys and check for expected error message
    async fn check_for_error(
        expected_message: &str,
        path: &String,
        footer_key: &[u8],
        column_1_key: &[u8],
        column_2_key: &[u8],
    ) {
        let mut file = File::open(&path).await.unwrap();

        let mut decryption_properties = FileDecryptionProperties::builder(footer_key.to_vec());

        if !column_1_key.is_empty() {
            decryption_properties =
                decryption_properties.with_column_key("double_field", column_1_key.to_vec());
        }

        if !column_2_key.is_empty() {
            decryption_properties =
                decryption_properties.with_column_key("float_field", column_2_key.to_vec());
        }

        let decryption_properties = decryption_properties.build().unwrap();

        match verify_encryption_test_file_read_async(&mut file, decryption_properties).await {
            Ok(_) => {
                panic!("did not get expected error")
            }
            Err(e) => {
                assert_eq!(e.to_string(), expected_message);
            }
        }
    }

    // Too short footer key
    check_for_error(
        "Parquet error: Invalid footer key. Failed to create AES key",
        &path,
        "bad_pwd".as_bytes(),
        column_1_key,
        column_2_key,
    )
    .await;

    // Wrong footer key
    check_for_error(
        "Parquet error: Provided footer key and AAD were unable to decrypt parquet footer",
        &path,
        "1123456789012345".as_bytes(),
        column_1_key,
        column_2_key,
    )
    .await;

    // Missing column key
    check_for_error(
        "Parquet error: No column decryption key set for encrypted column 'double_field'",
        &path,
        footer_key,
        "".as_bytes(),
        column_2_key,
    )
    .await;

    // Too short column key
    check_for_error(
        "Parquet error: Failed to create AES key",
        &path,
        footer_key,
        "abc".as_bytes(),
        column_2_key,
    )
    .await;

    // Wrong column key
    check_for_error(
        "Parquet error: Unable to decrypt column 'double_field', perhaps the column key is wrong?",
        &path,
        footer_key,
        "1123456789012345".as_bytes(),
        column_2_key,
    )
    .await;

    // Mixed up keys
    check_for_error(
        "Parquet error: Unable to decrypt column 'float_field', perhaps the column key is wrong?",
        &path,
        footer_key,
        column_2_key,
        column_1_key,
    )
    .await;
}

#[tokio::test]
#[cfg(feature = "snap")]
async fn test_plaintext_footer_read_without_decryption() {
    crate::encryption_agnostic::read_plaintext_footer_file_without_decryption_properties_async()
        .await;
}

#[tokio::test]
async fn test_non_uniform_encryption() {
    let test_data = arrow::util::test_util::parquet_test_data();
    let path = format!("{test_data}/encrypt_columns_and_footer.parquet.encrypted");
    let mut file = File::open(&path).await.unwrap();

    let footer_key = "0123456789012345".as_bytes().to_vec(); // 128bit/16
    let column_1_key = "1234567890123450".as_bytes().to_vec();
    let column_2_key = "1234567890123451".as_bytes().to_vec();

    let decryption_properties = FileDecryptionProperties::builder(footer_key.to_vec())
        .with_column_key("double_field", column_1_key)
        .with_column_key("float_field", column_2_key)
        .build()
        .unwrap();

    verify_encryption_test_file_read_async(&mut file, decryption_properties)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_uniform_encryption() {
    let test_data = arrow::util::test_util::parquet_test_data();
    let path = format!("{test_data}/uniform_encryption.parquet.encrypted");
    let mut file = File::open(&path).await.unwrap();

    let key_code: &[u8] = "0123456789012345".as_bytes();
    let decryption_properties = FileDecryptionProperties::builder(key_code.to_vec())
        .build()
        .unwrap();

    verify_encryption_test_file_read_async(&mut file, decryption_properties)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_aes_ctr_encryption() {
    let test_data = arrow::util::test_util::parquet_test_data();
    let path = format!("{test_data}/encrypt_columns_and_footer_ctr.parquet.encrypted");
    let mut file = File::open(&path).await.unwrap();

    let footer_key = "0123456789012345".as_bytes().to_vec();
    let column_1_key = "1234567890123450".as_bytes().to_vec();
    //let column_2_key = "1234567890123451".as_bytes().to_vec();

    let decryption_properties = FileDecryptionProperties::builder(footer_key)
        .with_column_key("double_field", column_1_key.clone())
        .with_column_key("float_field", column_1_key)
        .build()
        .unwrap();

    let options = ArrowReaderOptions::new().with_file_decryption_properties(decryption_properties);
    let metadata = ArrowReaderMetadata::load_async(&mut file, options).await;

    match metadata {
        Err(ParquetError::NYI(s)) => {
            assert!(s.contains("AES_GCM_CTR_V1"));
        }
        _ => {
            panic!("Expected ParquetError::NYI");
        }
    };
}

#[tokio::test]
async fn test_decrypting_without_decryption_properties_fails() {
    let test_data = arrow::util::test_util::parquet_test_data();
    let path = format!("{test_data}/uniform_encryption.parquet.encrypted");
    let mut file = File::open(&path).await.unwrap();

    let options = ArrowReaderOptions::new();
    let result = ArrowReaderMetadata::load_async(&mut file, options).await;
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().to_string(),
        "Parquet error: Parquet file has an encrypted footer but decryption properties were not provided"
    );
}

#[tokio::test]
async fn test_write_non_uniform_encryption() {
    let testdata = arrow::util::test_util::parquet_test_data();
    let path = format!("{testdata}/encrypt_columns_and_footer.parquet.encrypted");

    let footer_key = b"0123456789012345".to_vec(); // 128bit/16
    let column_names = vec!["double_field", "float_field"];
    let column_keys = vec![b"1234567890123450".to_vec(), b"1234567890123451".to_vec()];

    let decryption_properties = FileDecryptionProperties::builder(footer_key.clone())
        .with_column_keys(column_names.clone(), column_keys.clone())
        .unwrap()
        .build()
        .unwrap();

    let file_encryption_properties = FileEncryptionProperties::builder(footer_key)
        .with_column_keys(column_names, column_keys)
        .unwrap()
        .build()
        .unwrap();

    read_and_roundtrip_to_encrypted_file_async(
        &path,
        decryption_properties,
        file_encryption_properties,
    )
    .await
    .unwrap();
}

#[cfg(feature = "object_store")]
async fn get_encrypted_meta_store() -> (
    object_store::ObjectMeta,
    std::sync::Arc<dyn object_store::ObjectStore>,
) {
    use object_store::ObjectStore;
    use object_store::local::LocalFileSystem;
    use object_store::path::Path;

    use std::sync::Arc;
    let test_data = arrow::util::test_util::parquet_test_data();
    let store = LocalFileSystem::new_with_prefix(test_data).unwrap();

    let meta = store
        .head(&Path::from("uniform_encryption.parquet.encrypted"))
        .await
        .unwrap();

    (meta, Arc::new(store) as Arc<dyn ObjectStore>)
}

#[tokio::test]
#[cfg(feature = "object_store")]
async fn test_read_encrypted_file_from_object_store() {
    use parquet::arrow::async_reader::{AsyncFileReader, ParquetObjectReader};
    let (meta, store) = get_encrypted_meta_store().await;

    let key_code: &[u8] = "0123456789012345".as_bytes();
    let decryption_properties = FileDecryptionProperties::builder(key_code.to_vec())
        .build()
        .unwrap();
    let options = ArrowReaderOptions::new().with_file_decryption_properties(decryption_properties);

    let mut reader = ParquetObjectReader::new(store, meta.location).with_file_size(meta.size);
    let metadata = reader.get_metadata(Some(&options)).await.unwrap();
    let builder = ParquetRecordBatchStreamBuilder::new_with_options(reader, options)
        .await
        .unwrap();
    let batch_stream = builder.build().unwrap();
    let record_batches: Vec<_> = batch_stream.try_collect().await.unwrap();

    verify_encryption_test_data(record_batches, &metadata);
}

#[tokio::test]
async fn test_non_uniform_encryption_plaintext_footer_with_key_retriever() {
    let testdata = arrow::util::test_util::parquet_test_data();
    let path = format!("{testdata}/encrypt_columns_plaintext_footer.parquet.encrypted");
    let mut file = File::open(&path).await.unwrap();

    let key_retriever = TestKeyRetriever::new()
        .with_key("kf".to_owned(), "0123456789012345".as_bytes().to_vec())
        .with_key("kc1".to_owned(), "1234567890123450".as_bytes().to_vec())
        .with_key("kc2".to_owned(), "1234567890123451".as_bytes().to_vec());

    let decryption_properties =
        FileDecryptionProperties::with_key_retriever(Arc::new(key_retriever))
            .build()
            .unwrap();

    verify_encryption_test_file_read_async(&mut file, decryption_properties)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_non_uniform_encryption_with_key_retriever() {
    let testdata = arrow::util::test_util::parquet_test_data();
    let path = format!("{testdata}/encrypt_columns_and_footer.parquet.encrypted");
    let mut file = File::open(&path).await.unwrap();

    let key_retriever = TestKeyRetriever::new()
        .with_key("kf".to_owned(), "0123456789012345".as_bytes().to_vec())
        .with_key("kc1".to_owned(), "1234567890123450".as_bytes().to_vec())
        .with_key("kc2".to_owned(), "1234567890123451".as_bytes().to_vec());

    let decryption_properties =
        FileDecryptionProperties::with_key_retriever(Arc::new(key_retriever))
            .build()
            .unwrap();

    verify_encryption_test_file_read_async(&mut file, decryption_properties)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_uniform_encryption_with_key_retriever() {
    let testdata = arrow::util::test_util::parquet_test_data();
    let path = format!("{testdata}/uniform_encryption.parquet.encrypted");
    let mut file = File::open(&path).await.unwrap();

    let key_retriever =
        TestKeyRetriever::new().with_key("kf".to_owned(), "0123456789012345".as_bytes().to_vec());

    let decryption_properties =
        FileDecryptionProperties::with_key_retriever(Arc::new(key_retriever))
            .build()
            .unwrap();

    verify_encryption_test_file_read_async(&mut file, decryption_properties)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_decrypt_page_index_uniform() {
    let test_data = arrow::util::test_util::parquet_test_data();
    let path = format!("{test_data}/uniform_encryption.parquet.encrypted");

    let key_code: &[u8] = "0123456789012345".as_bytes();
    let decryption_properties = FileDecryptionProperties::builder(key_code.to_vec())
        .build()
        .unwrap();

    test_decrypt_page_index(&path, decryption_properties)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_decrypt_page_index_non_uniform() {
    let test_data = arrow::util::test_util::parquet_test_data();
    let path = format!("{test_data}/encrypt_columns_and_footer.parquet.encrypted");

    let footer_key = "0123456789012345".as_bytes().to_vec();
    let column_1_key = "1234567890123450".as_bytes().to_vec();
    let column_2_key = "1234567890123451".as_bytes().to_vec();

    let decryption_properties = FileDecryptionProperties::builder(footer_key.to_vec())
        .with_column_key("double_field", column_1_key)
        .with_column_key("float_field", column_2_key)
        .build()
        .unwrap();

    test_decrypt_page_index(&path, decryption_properties)
        .await
        .unwrap();
}

async fn test_decrypt_page_index(
    path: &str,
    decryption_properties: Arc<FileDecryptionProperties>,
) -> Result<(), ParquetError> {
    let mut file = File::open(&path).await?;

    let options = ArrowReaderOptions::new()
        .with_file_decryption_properties(decryption_properties)
        .with_page_index(true);

    let arrow_metadata = ArrowReaderMetadata::load_async(&mut file, options).await?;

    verify_column_indexes(arrow_metadata.metadata());

    Ok(())
}

async fn verify_encryption_test_file_read_async(
    file: &mut tokio::fs::File,
    decryption_properties: Arc<FileDecryptionProperties>,
) -> Result<(), ParquetError> {
    let options = ArrowReaderOptions::new().with_file_decryption_properties(decryption_properties);

    let arrow_metadata = ArrowReaderMetadata::load_async(file, options).await?;
    let metadata = arrow_metadata.metadata();

    let record_reader = ParquetRecordBatchStreamBuilder::new_with_metadata(
        file.try_clone().await?,
        arrow_metadata.clone(),
    )
    .build()?;
    let record_batches = record_reader.try_collect::<Vec<_>>().await?;

    verify_encryption_test_data(record_batches, metadata);
    Ok(())
}

async fn read_and_roundtrip_to_encrypted_file_async(
    path: &str,
    decryption_properties: Arc<FileDecryptionProperties>,
    encryption_properties: Arc<FileEncryptionProperties>,
) -> Result<(), ParquetError> {
    let temp_file = tempfile::tempfile().unwrap();
    let mut file = File::open(&path).await.unwrap();

    let options = ArrowReaderOptions::new()
        .with_file_decryption_properties(Arc::clone(&decryption_properties));
    let arrow_metadata = ArrowReaderMetadata::load_async(&mut file, options).await?;
    let record_reader = ParquetRecordBatchStreamBuilder::new_with_metadata(
        file.try_clone().await?,
        arrow_metadata.clone(),
    )
    .build()?;
    let record_batches = record_reader.try_collect::<Vec<_>>().await?;

    let props = WriterProperties::builder()
        .with_file_encryption_properties(encryption_properties)
        .build();
    let options = ArrowWriterOptions::new().with_properties(props);

    let file = tokio::fs::File::from_std(temp_file.try_clone().unwrap());
    let mut writer =
        AsyncArrowWriter::try_new_with_options(file, arrow_metadata.schema().clone(), options)
            .unwrap();
    for batch in record_batches {
        writer.write(&batch).await.unwrap();
    }
    writer.close().await.unwrap();

    let mut file = tokio::fs::File::from_std(temp_file.try_clone().unwrap());
    verify_encryption_test_file_read_async(&mut file, decryption_properties).await
}

// Type aliases for multithreaded file writing tests
type ColSender = Sender<ArrowLeafColumn>;
type ColumnWriterTask = JoinHandle<Result<ArrowColumnWriter, ParquetError>>;
type RBStreamSerializeResult = Result<(Vec<ArrowColumnChunk>, usize), ParquetError>;

async fn send_arrays_to_column_writers(
    col_array_channels: &[ColSender],
    rb: &RecordBatch,
    schema: &Arc<Schema>,
) -> Result<(), ParquetError> {
    // Each leaf column has its own channel, increment next_channel for each leaf column sent.
    let mut next_channel = 0;
    for (array, field) in rb.columns().iter().zip(schema.fields()) {
        for c in compute_leaves(field, array)? {
            if col_array_channels[next_channel].send(c).await.is_err() {
                return Ok(());
            }
            next_channel += 1;
        }
    }
    Ok(())
}

/// Spawns a tokio task which joins the parallel column writer tasks,
/// and finalizes the row group
fn spawn_rg_join_and_finalize_task(
    column_writer_tasks: Vec<ColumnWriterTask>,
    rg_rows: usize,
) -> JoinHandle<RBStreamSerializeResult> {
    tokio::task::spawn(async move {
        let num_cols = column_writer_tasks.len();
        let mut finalized_rg = Vec::with_capacity(num_cols);
        for task in column_writer_tasks.into_iter() {
            let writer = task
                .await
                .map_err(|e| ParquetError::General(e.to_string()))??;
            finalized_rg.push(writer.close()?);
        }
        Ok((finalized_rg, rg_rows))
    })
}

fn spawn_parquet_parallel_serialization_task(
    writer_factory: ArrowRowGroupWriterFactory,
    mut data: Receiver<RecordBatch>,
    serialize_tx: Sender<JoinHandle<RBStreamSerializeResult>>,
    schema: Arc<Schema>,
) -> JoinHandle<Result<(), ParquetError>> {
    tokio::spawn(async move {
        let max_buffer_rb = 10;
        let max_row_group_rows = 10;
        let mut row_group_index = 0;

        let column_writers = writer_factory.create_column_writers(row_group_index)?;

        let (mut col_writer_tasks, mut col_array_channels) =
            spawn_column_parallel_row_group_writer(column_writers, max_buffer_rb)?;

        let mut current_rg_rows = 0;

        while let Some(mut rb) = data.recv().await {
            // This loop allows the "else" block to repeatedly split the RecordBatch to handle the case
            // when max_row_group_rows < execution.batch_size as an alternative to a recursive async
            // function.
            loop {
                if current_rg_rows + rb.num_rows() < max_row_group_rows {
                    send_arrays_to_column_writers(&col_array_channels, &rb, &schema).await?;
                    current_rg_rows += rb.num_rows();
                    break;
                } else {
                    let rows_left = max_row_group_rows - current_rg_rows;
                    let rb_split = rb.slice(0, rows_left);
                    send_arrays_to_column_writers(&col_array_channels, &rb_split, &schema).await?;

                    // Signal the parallel column writers that the RowGroup is done, join and finalize RowGroup
                    // on a separate task, so that we can immediately start on the next RG before waiting
                    // for the current one to finish.
                    drop(col_array_channels);

                    let finalize_rg_task =
                        spawn_rg_join_and_finalize_task(col_writer_tasks, max_row_group_rows);

                    // Do not surface error from closed channel (means something
                    // else hit an error, and the plan is shutting down).
                    if serialize_tx.send(finalize_rg_task).await.is_err() {
                        return Ok(());
                    }

                    current_rg_rows = 0;
                    rb = rb.slice(rows_left, rb.num_rows() - rows_left);

                    row_group_index += 1;
                    let column_writers = writer_factory.create_column_writers(row_group_index)?;
                    (col_writer_tasks, col_array_channels) =
                        spawn_column_parallel_row_group_writer(column_writers, 100)?;
                }
            }
        }

        drop(col_array_channels);
        // Handle leftover rows as final rowgroup, which may be smaller than max_row_group_rows
        if current_rg_rows > 0 {
            let finalize_rg_task =
                spawn_rg_join_and_finalize_task(col_writer_tasks, current_rg_rows);

            // Do not surface error from closed channel (means something
            // else hit an error, and the plan is shutting down).
            if serialize_tx.send(finalize_rg_task).await.is_err() {
                return Ok(());
            }
        }

        Ok(())
    })
}

fn spawn_column_parallel_row_group_writer(
    col_writers: Vec<ArrowColumnWriter>,
    max_buffer_size: usize,
) -> Result<(Vec<ColumnWriterTask>, Vec<ColSender>), ParquetError> {
    let num_columns = col_writers.len();

    let mut col_writer_tasks = Vec::with_capacity(num_columns);
    let mut col_array_channels = Vec::with_capacity(num_columns);
    for mut col_writer in col_writers.into_iter() {
        let (send_array, mut receive_array) =
            tokio::sync::mpsc::channel::<ArrowLeafColumn>(max_buffer_size);
        col_array_channels.push(send_array);
        let handle = tokio::spawn(async move {
            while let Some(col) = receive_array.recv().await {
                col_writer.write(&col)?;
            }
            Ok(col_writer)
        });
        col_writer_tasks.push(handle);
    }
    Ok((col_writer_tasks, col_array_channels))
}

/// Consume RowGroups serialized by other parallel tasks and concatenate them
/// to the final parquet file
async fn concatenate_parallel_row_groups<W: Write + Send>(
    mut parquet_writer: SerializedFileWriter<W>,
    mut serialize_rx: Receiver<JoinHandle<RBStreamSerializeResult>>,
) -> Result<ParquetMetaData, ParquetError> {
    while let Some(task) = serialize_rx.recv().await {
        let result = task.await;
        let mut rg_out = parquet_writer.next_row_group()?;
        let (serialized_columns, _cnt) =
            result.map_err(|e| ParquetError::General(e.to_string()))??;

        for column_chunk in serialized_columns {
            column_chunk.append_to_row_group(&mut rg_out)?;
        }
        rg_out.close()?;
    }

    let file_metadata = parquet_writer.close()?;
    Ok(file_metadata)
}

// This test is based on DataFusion's ParquetSink. Motivation is to test
// concurrent writing of encrypted data over multiple row groups using the low-level API.
#[tokio::test]
async fn test_concurrent_encrypted_writing_over_multiple_row_groups() {
    // Read example data and set up encryption/decryption properties
    let testdata = arrow::util::test_util::parquet_test_data();
    let path = format!("{testdata}/encrypt_columns_and_footer.parquet.encrypted");
    let file = std::fs::File::open(path).unwrap();

    let file_encryption_properties = FileEncryptionProperties::builder(b"0123456789012345".into())
        .with_column_key("double_field", b"1234567890123450".into())
        .with_column_key("float_field", b"1234567890123451".into())
        .build()
        .unwrap();
    let decryption_properties = FileDecryptionProperties::builder(b"0123456789012345".into())
        .with_column_key("double_field", b"1234567890123450".into())
        .with_column_key("float_field", b"1234567890123451".into())
        .build()
        .unwrap();

    let (record_batches, metadata) =
        read_encrypted_file(&file, decryption_properties.clone()).unwrap();
    let schema = metadata.schema();

    // Create a channel to send RecordBatches to the writer and send row groups
    let (record_batch_tx, data) = tokio::sync::mpsc::channel::<RecordBatch>(100);
    let data_generator = tokio::spawn(async move {
        for record_batch in record_batches {
            record_batch_tx.send(record_batch).await.unwrap();
        }
    });

    let props = Arc::new(
        WriterPropertiesBuilder::default()
            .with_file_encryption_properties(file_encryption_properties)
            .build(),
    );
    let parquet_schema = ArrowSchemaConverter::new()
        .with_coerce_types(props.coerce_types())
        .convert(schema)
        .unwrap();

    // Create a temporary file to write the encrypted data
    let temp_file = tempfile::tempfile().unwrap();

    let writer =
        SerializedFileWriter::new(&temp_file, parquet_schema.root_schema_ptr(), props).unwrap();
    let row_group_writer_factory = ArrowRowGroupWriterFactory::new(&writer, Arc::clone(schema));
    let max_row_groups = 1;

    let (serialize_tx, serialize_rx) =
        tokio::sync::mpsc::channel::<JoinHandle<RBStreamSerializeResult>>(max_row_groups);

    let launch_serialization_task = spawn_parquet_parallel_serialization_task(
        row_group_writer_factory,
        data,
        serialize_tx,
        schema.clone(),
    );

    let _file_metadata = concatenate_parallel_row_groups(writer, serialize_rx)
        .await
        .unwrap();

    data_generator.await.unwrap();
    launch_serialization_task.await.unwrap().unwrap();

    // Check that the file was written correctly
    let (read_record_batches, read_metadata) =
        read_encrypted_file(&temp_file, decryption_properties.clone()).unwrap();

    assert_eq!(read_metadata.metadata().file_metadata().num_rows(), 50);
    verify_encryption_test_data(read_record_batches, read_metadata.metadata());
}

#[tokio::test]
async fn test_multi_threaded_encrypted_writing() {
    // Read example data and set up encryption/decryption properties
    let testdata = arrow::util::test_util::parquet_test_data();
    let path = format!("{testdata}/encrypt_columns_and_footer.parquet.encrypted");
    let file = std::fs::File::open(path).unwrap();

    let file_encryption_properties = FileEncryptionProperties::builder(b"0123456789012345".into())
        .with_column_key("double_field", b"1234567890123450".into())
        .with_column_key("float_field", b"1234567890123451".into())
        .build()
        .unwrap();
    let decryption_properties = FileDecryptionProperties::builder(b"0123456789012345".into())
        .with_column_key("double_field", b"1234567890123450".into())
        .with_column_key("float_field", b"1234567890123451".into())
        .build()
        .unwrap();

    let (record_batches, metadata) =
        read_encrypted_file(&file, Arc::clone(&decryption_properties)).unwrap();
    let schema = metadata.schema().clone();

    let props = Arc::new(
        WriterPropertiesBuilder::default()
            .with_file_encryption_properties(file_encryption_properties)
            .build(),
    );

    let parquet_schema = ArrowSchemaConverter::new()
        .with_coerce_types(props.coerce_types())
        .convert(&schema)
        .unwrap();

    // Create a temporary file to write the encrypted data
    let temp_file = tempfile::tempfile().unwrap();
    let mut writer =
        SerializedFileWriter::new(&temp_file, parquet_schema.root_schema_ptr(), props).unwrap();
    let row_group_writer_factory = ArrowRowGroupWriterFactory::new(&writer, Arc::clone(&schema));

    let (serialize_tx, mut serialize_rx) =
        tokio::sync::mpsc::channel::<JoinHandle<RBStreamSerializeResult>>(1);

    // Create a channel to send RecordBatches to the writer and send row batches
    let (record_batch_tx, mut data) = tokio::sync::mpsc::channel::<RecordBatch>(100);
    let data_generator = tokio::spawn(async move {
        for record_batch in record_batches {
            record_batch_tx.send(record_batch).await.unwrap();
        }
    });

    // Get column writers
    let col_writers = row_group_writer_factory.create_column_writers(0).unwrap();

    let (col_writer_tasks, col_array_channels) =
        spawn_column_parallel_row_group_writer(col_writers, 10).unwrap();

    // Spawn serialization tasks for incoming RecordBatches
    let launch_serialization_task = tokio::spawn(async move {
        let Some(rb) = data.recv().await else {
            panic!()
        };
        send_arrays_to_column_writers(&col_array_channels, &rb, &schema)
            .await
            .unwrap();
        let finalize_rg_task = spawn_rg_join_and_finalize_task(col_writer_tasks, 10);

        serialize_tx.send(finalize_rg_task).await.unwrap();
        drop(col_array_channels);
    });

    // Append the finalized row groups to the SerializedFileWriter
    while let Some(task) = serialize_rx.recv().await {
        let (arrow_column_chunks, _) = task.await.unwrap().unwrap();
        let mut row_group_writer = writer.next_row_group().unwrap();
        for chunk in arrow_column_chunks {
            chunk.append_to_row_group(&mut row_group_writer).unwrap();
        }
        row_group_writer.close().unwrap();
    }

    // Wait for data generator and serialization task to finish
    data_generator.await.unwrap();
    launch_serialization_task.await.unwrap();
    let metadata = writer.close().unwrap();

    // Close the file writer which writes the footer
    assert_eq!(metadata.file_metadata().num_rows(), 50);

    // Check that the file was written correctly
    let (read_record_batches, read_metadata) =
        read_encrypted_file(&temp_file, decryption_properties).unwrap();
    verify_encryption_test_data(read_record_batches, read_metadata.metadata());

    // Check that file was encrypted
    let result = ArrowReaderMetadata::load(&temp_file, ArrowReaderOptions::default());
    assert_eq!(
        result.unwrap_err().to_string(),
        "Parquet error: Parquet file has an encrypted footer but decryption properties were not provided"
    );
}

#[tokio::test]
async fn test_multi_threaded_encrypted_writing_deprecated() {
    // Read example data and set up encryption/decryption properties
    let testdata = arrow::util::test_util::parquet_test_data();
    let path = format!("{testdata}/encrypt_columns_and_footer.parquet.encrypted");
    let file = std::fs::File::open(path).unwrap();

    let file_encryption_properties = FileEncryptionProperties::builder(b"0123456789012345".into())
        .with_column_key("double_field", b"1234567890123450".into())
        .with_column_key("float_field", b"1234567890123451".into())
        .build()
        .unwrap();
    let decryption_properties = FileDecryptionProperties::builder(b"0123456789012345".into())
        .with_column_key("double_field", b"1234567890123450".into())
        .with_column_key("float_field", b"1234567890123451".into())
        .build()
        .unwrap();

    let (record_batches, metadata) =
        read_encrypted_file(&file, Arc::clone(&decryption_properties)).unwrap();
    let to_write: Vec<_> = record_batches
        .iter()
        .flat_map(|rb| rb.columns().to_vec())
        .collect();
    let schema = metadata.schema().clone();

    let props = Some(
        WriterPropertiesBuilder::default()
            .with_file_encryption_properties(file_encryption_properties)
            .build(),
    );

    // Create a temporary file to write the encrypted data
    let temp_file = tempfile::tempfile().unwrap();
    let mut writer = ArrowWriter::try_new(&temp_file, schema.clone(), props).unwrap();

    // LOW-LEVEL API: Use low level API to write into a file using multiple threads

    // Get column writers
    #[allow(deprecated)]
    let col_writers = writer.get_column_writers().unwrap();
    let num_columns = col_writers.len();

    let (col_writer_tasks, mut col_array_channels) =
        spawn_column_parallel_row_group_writer(col_writers, 100).unwrap();

    // Send the ArrowLeafColumn data to the respective column writer channels
    let mut worker_iter = col_array_channels.iter_mut();
    for (array, field) in to_write.iter().zip(schema.fields()) {
        for leaves in compute_leaves(field, array).unwrap() {
            worker_iter.next().unwrap().send(leaves).await.unwrap();
        }
    }
    drop(col_array_channels);

    // Wait for all column writers to finish writing
    let mut finalized_rg = Vec::with_capacity(num_columns);
    for task in col_writer_tasks.into_iter() {
        finalized_rg.push(task.await.unwrap().unwrap().close().unwrap());
    }

    // Append the finalized row group to the SerializedFileWriter
    #[allow(deprecated)]
    writer.append_row_group(finalized_rg).unwrap();

    // HIGH-LEVEL API: Write RecordBatches into the file using ArrowWriter

    // Write individual RecordBatches into the file
    for rb in record_batches {
        writer.write(&rb).unwrap()
    }
    assert!(writer.flush().is_ok());

    // Close the file writer which writes the footer
    let metadata = writer.finish().unwrap();
    assert_eq!(metadata.file_metadata().num_rows(), 100);

    // Check that the file was written correctly
    let (read_record_batches, read_metadata) =
        read_encrypted_file(&temp_file, decryption_properties).unwrap();
    verify_encryption_double_test_data(read_record_batches, read_metadata.metadata());

    // Check that file was encrypted
    let result = ArrowReaderMetadata::load(&temp_file, ArrowReaderOptions::default());
    assert_eq!(
        result.unwrap_err().to_string(),
        "Parquet error: Parquet file has an encrypted footer but decryption properties were not provided"
    );
}
