use crate::reader::async_reader::DataFetchFutureBoxed;
use std::ops::Range;

/// A broad generic trait definition allowing fetching bytes from any source asynchronously.
/// This trait has very few limitations, mostly in regard to ownership and lifetime,
/// but it must return a boxed Future containing [`bytes::Bytes`] or an error.
pub trait AsyncFileReader: Send + Unpin {
    /// Fetch a range of bytes asynchronously using a custom reading method
    fn fetch_range(&mut self, range: Range<u64>) -> DataFetchFutureBoxed;

    /// Fetch a range that is beyond the originally provided file range,
    /// such as reading the header before reading the file,
    /// or fetching the remainder of the block in case the range ended before the block's end.
    /// By default, this will simply point to the fetch_range function.
    fn fetch_extra_range(&mut self, range: Range<u64>) -> DataFetchFutureBoxed {
        self.fetch_range(range)
    }
}
