//! Contains record-based API for reading Parquet files.

mod api;
pub mod reader;
mod record_reader;
mod record_writer;
mod triplet;

pub use self::{
    api::{
        Field, List, ListAccessor, Map, MapAccessor, Row, RowAccessor, RowColumnIter, RowFormatter,
    },
    record_reader::RecordReader,
    record_writer::RecordWriter,
};
