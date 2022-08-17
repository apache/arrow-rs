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

use std::{cell::RefCell, cmp, fmt, io::*};

use crate::file::reader::Length;

const DEFAULT_BUF_SIZE: usize = 8 * 1024;

// ----------------------------------------------------------------------

/// TryClone tries to clone the type and should maintain the `Seek` position of the given
/// instance.
pub trait TryClone: Sized {
    /// Clones the type returning a new instance or an error if it's not possible
    /// to clone it.
    fn try_clone(&self) -> Result<Self>;
}

/// ParquetReader is the interface which needs to be fulfilled to be able to parse a
/// parquet source.
pub trait ParquetReader: Read + Seek + Length + TryClone {}
impl<T: Read + Seek + Length + TryClone> ParquetReader for T {}

// Read/Write wrappers for `File`.

/// Struct that represents a slice of a file data with independent start position and
/// length. Internally clones provided file handle, wraps with a custom implementation
/// of BufReader that resets position before any read.
///
/// This is workaround and alternative for `file.try_clone()` method. It clones `File`
/// while preserving independent position, which is not available with `try_clone()`.
///
/// Designed after `arrow::io::RandomAccessFile` and `std::io::BufReader`
pub struct FileSource<R: ParquetReader> {
    reader: RefCell<R>,
    start: u64,     // start position in a file
    end: u64,       // end position in a file
    buf: Vec<u8>,   // buffer where bytes read in advance are stored
    buf_pos: usize, // current position of the reader in the buffer
    buf_cap: usize, // current number of bytes read into the buffer
}

impl<R: ParquetReader> fmt::Debug for FileSource<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileSource")
            .field("reader", &"OPAQUE")
            .field("start", &self.start)
            .field("end", &self.end)
            .field("buf.len", &self.buf.len())
            .field("buf_pos", &self.buf_pos)
            .field("buf_cap", &self.buf_cap)
            .finish()
    }
}

impl<R: ParquetReader> FileSource<R> {
    /// Creates new file reader with start and length from a file handle
    pub fn new(fd: &R, start: u64, length: usize) -> Self {
        let reader = RefCell::new(fd.try_clone().unwrap());
        Self {
            reader,
            start,
            end: start + length as u64,
            buf: vec![0_u8; DEFAULT_BUF_SIZE],
            buf_pos: 0,
            buf_cap: 0,
        }
    }

    fn fill_inner_buf(&mut self) -> Result<&[u8]> {
        if self.buf_pos >= self.buf_cap {
            // If we've reached the end of our internal buffer then we need to fetch
            // some more data from the underlying reader.
            // Branch using `>=` instead of the more correct `==`
            // to tell the compiler that the pos..cap slice is always valid.
            debug_assert!(self.buf_pos == self.buf_cap);
            let mut reader = self.reader.borrow_mut();
            reader.seek(SeekFrom::Start(self.start))?; // always seek to start before reading
            self.buf_cap = reader.read(&mut self.buf)?;
            self.buf_pos = 0;
        }
        Ok(&self.buf[self.buf_pos..self.buf_cap])
    }

    fn skip_inner_buf(&mut self, buf: &mut [u8]) -> Result<usize> {
        // discard buffer
        self.buf_pos = 0;
        self.buf_cap = 0;
        // read directly into param buffer
        let mut reader = self.reader.borrow_mut();
        reader.seek(SeekFrom::Start(self.start))?; // always seek to start before reading
        let nread = reader.read(buf)?;
        self.start += nread as u64;
        Ok(nread)
    }
}

impl<R: ParquetReader> Read for FileSource<R> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let bytes_to_read = cmp::min(buf.len(), (self.end - self.start) as usize);
        let buf = &mut buf[0..bytes_to_read];

        // If we don't have any buffered data and we're doing a massive read
        // (larger than our internal buffer), bypass our internal buffer
        // entirely.
        if self.buf_pos == self.buf_cap && buf.len() >= self.buf.len() {
            return self.skip_inner_buf(buf);
        }
        let nread = {
            let mut rem = self.fill_inner_buf()?;
            // copy the data from the inner buffer to the param buffer
            rem.read(buf)?
        };
        // consume from buffer
        self.buf_pos = cmp::min(self.buf_pos + nread, self.buf_cap);

        self.start += nread as u64;
        Ok(nread)
    }
}

impl<R: ParquetReader> Length for FileSource<R> {
    fn len(&self) -> u64 {
        self.end - self.start
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::iter;

    use crate::util::test_common::file_util::get_test_file;

    #[test]
    fn test_io_read_fully() {
        let mut buf = vec![0; 8];
        let mut src = FileSource::new(&get_test_file("alltypes_plain.parquet"), 0, 4);

        let bytes_read = src.read(&mut buf[..]).unwrap();
        assert_eq!(bytes_read, 4);
        assert_eq!(buf, vec![b'P', b'A', b'R', b'1', 0, 0, 0, 0]);
    }

    #[test]
    fn test_io_read_in_chunks() {
        let mut buf = vec![0; 4];
        let mut src = FileSource::new(&get_test_file("alltypes_plain.parquet"), 0, 4);

        let bytes_read = src.read(&mut buf[0..2]).unwrap();
        assert_eq!(bytes_read, 2);
        let bytes_read = src.read(&mut buf[2..]).unwrap();
        assert_eq!(bytes_read, 2);
        assert_eq!(buf, vec![b'P', b'A', b'R', b'1']);
    }

    #[test]
    fn test_io_read_pos() {
        let mut src = FileSource::new(&get_test_file("alltypes_plain.parquet"), 0, 4);

        let _ = src.read(&mut [0; 1]).unwrap();
        assert_eq!(src.start, 1);

        let _ = src.read(&mut [0; 4]).unwrap();
        assert_eq!(src.start, 4);
    }

    #[test]
    fn test_io_read_over_limit() {
        let mut src = FileSource::new(&get_test_file("alltypes_plain.parquet"), 0, 4);

        // Read all bytes from source
        let _ = src.read(&mut [0; 128]).unwrap();
        assert_eq!(src.start, 4);

        // Try reading again, should return 0 bytes.
        let bytes_read = src.read(&mut [0; 128]).unwrap();
        assert_eq!(bytes_read, 0);
        assert_eq!(src.start, 4);
    }

    #[test]
    fn test_io_seek_switch() {
        let mut buf = vec![0; 4];
        let mut file = get_test_file("alltypes_plain.parquet");
        let mut src = FileSource::new(&file, 0, 4);

        file.seek(SeekFrom::Start(5_u64))
            .expect("File seek to a position");

        let bytes_read = src.read(&mut buf[..]).unwrap();
        assert_eq!(bytes_read, 4);
        assert_eq!(buf, vec![b'P', b'A', b'R', b'1']);
    }

    #[test]
    fn test_io_large_read() {
        // Generate repeated 'abcdef' pattern and write it into a file
        let patterned_data: Vec<u8> = iter::repeat(vec![0, 1, 2, 3, 4, 5])
            .flatten()
            .take(3 * DEFAULT_BUF_SIZE)
            .collect();

        let mut file = tempfile::tempfile().unwrap();
        file.write_all(&patterned_data).unwrap();

        // seek the underlying file to the first 'd'
        file.seek(SeekFrom::Start(3)).unwrap();

        // create the FileSource reader that starts at pos 1 ('b')
        let mut chunk = FileSource::new(&file, 1, patterned_data.len() - 1);

        // read the 'b' at pos 1
        let mut res = vec![0u8; 1];
        chunk.read_exact(&mut res).unwrap();
        assert_eq!(res, &[1]);

        // the underlying file is sought to 'e'
        file.seek(SeekFrom::Start(4)).unwrap();

        // now read large chunk that starts with 'c' (after 'b')
        let mut res = vec![0u8; 2 * DEFAULT_BUF_SIZE];
        chunk.read_exact(&mut res).unwrap();
        assert_eq!(
            res,
            &patterned_data[2..2 + 2 * DEFAULT_BUF_SIZE],
            "read buf and original data are not equal"
        );
    }
}
