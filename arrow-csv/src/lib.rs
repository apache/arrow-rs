//! Transfer data between the [Apache Arrow] memory format and CSV (comma-separated values).
//!
//! [Apache Arrow]: https://arrow.apache.org/
//!
//! # Platform Support
//!
//! Only little-endian platforms are officially supported and tested in CI.
//! Big-endian platforms are not tested in CI and may not work correctly.
//! Fixes for big-endian platforms are welcome and handled on a best-effort basis,
//! but compatibility is not guaranteed.

#![doc(
    html_logo_url = "https://arrow.apache.org/img/arrow-logo_chevrons_black-txt_white-bg.svg",
    html_favicon_url = "https://arrow.apache.org/img/arrow-logo_chevrons_black-txt_transparent-bg.svg"
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_docs)]

pub mod reader;
pub mod writer;

pub use self::reader::Reader;
pub use self::reader::ReaderBuilder;
pub use self::reader::infer_schema_from_files;
pub use self::writer::QuoteStyle;
pub use self::writer::Writer;
pub use self::writer::WriterBuilder;
use arrow_schema::ArrowError;

fn map_csv_error(error: csv::Error) -> ArrowError {
    match error.kind() {
        csv::ErrorKind::Io(error) => ArrowError::CsvError(error.to_string()),
        csv::ErrorKind::Utf8 { pos, err } => ArrowError::CsvError(format!(
            "Encountered UTF-8 error while reading CSV file: {}{}",
            err,
            pos.as_ref()
                .map(|pos| format!(" at line {}", pos.line()))
                .unwrap_or_default(),
        )),
        csv::ErrorKind::UnequalLengths {
            pos,
            expected_len,
            len,
        } => ArrowError::CsvError(format!(
            "Encountered unequal lengths between records on CSV file. Expected {} \
                 records, found {} records{}",
            expected_len,
            len,
            pos.as_ref()
                .map(|pos| format!(" at line {}", pos.line()))
                .unwrap_or_default(),
        )),
        _ => ArrowError::CsvError("Error reading CSV file".to_string()),
    }
}
