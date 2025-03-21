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

//! Example of reading arrow IPC files and streams using "zero copy" API
//!
//! Zero copy in this case means the Arrow arrays refer directly to a user
//! provided buffer or memory region.

use arrow::array::{record_batch, RecordBatch};
use arrow::error::Result;
use arrow_buffer::Buffer;
use arrow_cast::pretty::pretty_format_batches;
use arrow_ipc::convert::fb_to_schema;
use arrow_ipc::reader::{read_footer_length, FileDecoder};
use arrow_ipc::writer::FileWriter;
use arrow_ipc::{root_as_footer, Block};
use std::path::PathBuf;
use std::sync::Arc;

/// This example shows how to read data from an Arrow IPC file without copying
/// using `mmap` and the [`FileDecoder`] API
fn main() {
    // Create a temporary file with 3 record batches
    let ipc_path = ipc_file();
    // Open the file and memory map it using the mmap2 crate:
    let ipc_file = std::fs::File::open(&ipc_path.path).expect("failed to open file");
    let mmap = unsafe { memmap2::Mmap::map(&ipc_file).expect("failed to mmap file") };

    // Convert the mmap region to an Arrow `Buffer` to back the arrow arrays. We
    // do this by first creating a `bytes::Bytes` (which is zero copy) and then
    // creating a Buffer from the `Bytes` (which is also zero copy)
    let bytes = bytes::Bytes::from_owner(mmap);
    let buffer = Buffer::from(bytes);

    // Now, use the FileDecoder API (wrapped by `IPCBufferDecoder` for
    // convenience) to crate Arrays re-using the data in the underlying buffer
    let decoder = IPCBufferDecoder::new(buffer);
    assert_eq!(decoder.num_batches(), 3);

    // Create the Arrays and print them
    for i in 0..decoder.num_batches() {
        let batch = decoder.get_batch(i).unwrap().expect("failed to read batch");
        assert_eq!(3, batch.num_rows());
        println!("Batch {i}\n{}", pretty_format_batches(&[batch]).unwrap());
    }
}

/// Return 3 [`RecordBatch`]es with a single column of type `Int32`
fn example_data() -> Vec<RecordBatch> {
    vec![
        record_batch!(("my_column", Int32, [1, 2, 3])).unwrap(),
        record_batch!(("my_column", Int32, [4, 5, 6])).unwrap(),
        record_batch!(("my_column", Int32, [7, 8, 9])).unwrap(),
    ]
}

/// Return a temporary file that contains an IPC file with 3 [`RecordBatch`]es
fn ipc_file() -> TempFile {
    let path = PathBuf::from("example.arrow");
    let file = std::fs::File::create(&path).unwrap();
    let data = example_data();
    let mut writer = FileWriter::try_new(file, &data[0].schema()).unwrap();
    for batch in &data {
        writer.write(batch).unwrap();
    }
    writer.finish().unwrap();
    TempFile { path }
}

/// Incrementally decodes [`RecordBatch`]es from an IPC file stored in a Arrow
/// [`Buffer`] using the [`FileDecoder`] API.
///
/// This is a wrapper around the example in the `FileDecoder` which handles the
/// low level interaction with the Arrow IPC format.
struct IPCBufferDecoder {
    /// Memory (or memory mapped) Buffer with the data
    buffer: Buffer,
    /// Decoder that reads Arrays that refers to the underlying buffers
    decoder: FileDecoder,
    /// Location of the batches within the buffer
    batches: Vec<Block>,
}

impl IPCBufferDecoder {
    fn new(buffer: Buffer) -> Self {
        let trailer_start = buffer.len() - 10;
        let footer_len = read_footer_length(buffer[trailer_start..].try_into().unwrap()).unwrap();
        let footer = root_as_footer(&buffer[trailer_start - footer_len..trailer_start]).unwrap();

        let schema = fb_to_schema(footer.schema().unwrap());

        let mut decoder = FileDecoder::new(Arc::new(schema), footer.version());

        // Read dictionaries
        for block in footer.dictionaries().iter().flatten() {
            let block_len = block.bodyLength() as usize + block.metaDataLength() as usize;
            let data = buffer.slice_with_length(block.offset() as _, block_len);
            decoder.read_dictionary(block, &data).unwrap();
        }

        // convert to Vec from the flatbuffers Vector to avoid having a direct dependency on flatbuffers
        let batches = footer
            .recordBatches()
            .map(|b| b.iter().copied().collect())
            .unwrap_or_default();

        Self {
            buffer,
            decoder,
            batches,
        }
    }

    /// Return the number of [`RecordBatch`]es in this buffer
    fn num_batches(&self) -> usize {
        self.batches.len()
    }

    /// Return the [`RecordBatch`] at message index `i`.
    ///
    /// This may return `None` if the IPC message was None
    fn get_batch(&self, i: usize) -> Result<Option<RecordBatch>> {
        let block = &self.batches[i];
        let block_len = block.bodyLength() as usize + block.metaDataLength() as usize;
        let data = self
            .buffer
            .slice_with_length(block.offset() as _, block_len);
        self.decoder.read_record_batch(block, &data)
    }
}

/// This structure deletes the file when it is dropped
struct TempFile {
    path: PathBuf,
}

impl Drop for TempFile {
    fn drop(&mut self) {
        if let Err(e) = std::fs::remove_file(&self.path) {
            println!("Error deleting '{:?}': {:?}", self.path, e);
        }
    }
}
