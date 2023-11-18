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

use object_store::azure::MicrosoftAzureBuilder;
use object_store::path::Path;
use object_store::ObjectStore;
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::schema::printer::print_parquet_metadata;
use std::error::Error;
use std::io::stdout;
use std::sync::Arc;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    // Open Azure Storage Blob https://myaccount.blob.core.windows.net/mycontainer/path/to/blob.parquet
    // Requires running Azure CLI `az login` beforehand to setup authentication
    let storage_container = Arc::new(
        MicrosoftAzureBuilder::new()
            .with_account("myaccount")
            .with_container_name("mycontainer")
            .with_use_azure_cli(true)
            .build()?,
    );
    let blob = storage_container
        .get(&Path::from("path/to/blob.parquet"))
        .await?
        .meta;
    println!("Found Blob with {}B at {}", blob.size, blob.location);

    // Show Parquet metadata
    let reader = ParquetObjectReader::new(storage_container, blob);
    let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
    print_parquet_metadata(&mut stdout(), builder.metadata());

    Ok(())
}
