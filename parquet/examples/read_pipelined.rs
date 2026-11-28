// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use arrow_array::{ArrayRef, Int32Array, RecordBatch, StringArray};
use bytes::Bytes;
use parquet::arrow::{ArrowWriter, ParquetRecordBatchStreamBuilder};
use parquet::errors::{ParquetError, Result};
use parquet::file::properties::WriterProperties;
use std::io::Cursor;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Fetches row groups in one task while another decodes them on a blocking thread.
#[tokio::main]
async fn main() -> Result<()> {
    let data = create_parquet_data(4, 1_000)?;

    let batches = read_pipelined(data).await?;
    let num_rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    println!("Read {} batches with {num_rows} rows", batches.len());

    Ok(())
}

async fn read_pipelined(data: Bytes) -> Result<Vec<RecordBatch>> {
    let builder = ParquetRecordBatchStreamBuilder::new(Cursor::new(data)).await?;
    let mut stream = builder.with_batch_size(512).build()?;

    // The channel holds up to 2 row groups that are fetched and waiting to be decoded
    let (tx, mut rx) = mpsc::channel(2);

    let io_task = tokio::spawn(async move {
        while let Some(reader) = stream.next_row_group().await? {
            // The receiver is dropped when decoding failed, so stop fetching
            if tx.send(reader).await.is_err() {
                break;
            }
        }
        Ok::<_, ParquetError>(())
    });

    let mut batches = vec![];
    while let Some(reader) = rx.recv().await {
        let decoded = tokio::task::spawn_blocking(move || reader.collect::<Result<Vec<_>, _>>())
            .await
            .map_err(|e| ParquetError::External(Box::new(e)))??;
        batches.extend(decoded);
    }

    io_task
        .await
        .map_err(|e| ParquetError::External(Box::new(e)))??;

    Ok(batches)
}

/// Writes `num_row_groups` row groups of `rows_per_group` rows each into memory
fn create_parquet_data(num_row_groups: usize, rows_per_group: usize) -> Result<Bytes> {
    let num_rows = num_row_groups * rows_per_group;
    let ids: ArrayRef = Arc::new(Int32Array::from_iter_values(0..num_rows as i32));
    let names: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..num_rows).map(|i| format!("name-{i}")),
    ));
    let batch = RecordBatch::try_from_iter([("id", ids), ("name", names)])?;

    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(rows_per_group))
        .build();
    let mut buffer = vec![];
    let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(buffer.into())
}
