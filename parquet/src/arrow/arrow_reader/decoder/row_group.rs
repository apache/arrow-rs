use crate::arrow::arrow_reader::decoder::buffers::Buffers;
use crate::arrow::arrow_reader::ParquetRecordBatchReader;
use crate::errors::ParquetError;
use crate::file::metadata::ParquetMetaData;
use std::ops::Range;
use std::sync::Arc;

/// TODO combine with `ParquetRecordBatchReaderResult`
pub enum RowGroupReaderResult {
    /// The next reader to use for reading the next batch of rows
    Ready {
        record_batch_reader: ParquetRecordBatchReader,
    },
    /// Need more data to proceed
    NeedsData {
        /// The ranges of data that are needed to proceed
        ranges: Vec<Range<u64>>,
    },
}

/// Builder that encapsulates the logic for figuring out what data is needed
/// to begin reading the next chunk of rows from a Parquet file.
///
/// This is typically a row group but the author has aspirations that
/// the pattern can be extended to data boundaries other than RowGroups in the future.
#[derive(Debug)]
pub struct RowGroupReaderBuilder {
    parquet_metadata: Option<Arc<ParquetMetaData>>,
    /// the source of data to read from
    buffers: Buffers,
}

impl RowGroupReaderBuilder {
    pub fn new(file_len: u64) -> Self {
        Self {
            parquet_metadata: None,
            buffers: Buffers::new(file_len),
        }
    }

    /// Set the Parquet metadata for this reader
    pub fn with_metadata(mut self, metadata: Arc<ParquetMetaData>) -> Self {
        self.parquet_metadata = Some(metadata);
        self
    }

    /// return the file length of the Parquet file being read
    pub fn file_len(&self) -> u64 {
        self.buffers.file_len()
    }

    /// return the inner Buffers
    pub fn buffers(&self) -> &Buffers {
        &self.buffers
    }

    /// return a mutable reference to the inner Buffers
    pub fn buffers_mut(&mut self) -> &mut Buffers {
        &mut self.buffers
    }

    /// Return what additional ranges, if any, are needed to create the next
    /// RowGroupDecoder

    /// returns a  ParquetRecordBatchReader suitable for reading the next
    /// group of rows from the Parquet, or the list of data ranges still
    /// needed to proceed
    pub fn try_next_reader(&mut self) -> Result<RowGroupReaderResult, ParquetError> {
        todo!()
    }
}

/// State required for decoding a RowGroup
///
/// The idea is eventually these could be decoded in parallel so keep all the
/// decoding logic in a single struct.
#[derive(Debug)]
struct RowGroupDecoder {
    file_len: u64,
    parquet_metadata: Arc<ParquetMetaData>,
    current_row_group: usize,
}

enum RowGroupDecoderState {
    //TODO add state for "evaluating predicates"
    /// Needs input data to proceed
    NeedsData {
        /// The ranges of data that are needed to proceed
        ranges: Vec<Range<u64>>,
    },
    Decoding {
        reader: ParquetRecordBatchReader,
    },
}
