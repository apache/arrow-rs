use arrow::array::BooleanBufferBuilder;
use arrow::bitmap::Bitmap;
use arrow::buffer::Buffer;

use crate::column::reader::private::ColumnLevelDecoderImpl;
use crate::schema::types::ColumnDescPtr;

use super::{
    buffer::{RecordBuffer, TypedBuffer},
    MIN_BATCH_SIZE,
};

pub struct DefinitionLevelBuffer {
    buffer: TypedBuffer<i16>,
    builder: BooleanBufferBuilder,
    max_level: i16,
}

impl RecordBuffer for DefinitionLevelBuffer {
    type Output = Buffer;
    type Writer = [i16];

    fn create(desc: &ColumnDescPtr) -> Self {
        Self {
            buffer: RecordBuffer::create(desc),
            builder: BooleanBufferBuilder::new(0),
            max_level: desc.max_def_level(),
        }
    }

    fn split(&mut self, len: usize) -> Self::Output {
        self.buffer.split(len)
    }

    fn writer(&mut self, batch_size: usize) -> &mut Self::Writer {
        assert_eq!(self.buffer.len(), self.builder.len());
        self.buffer.writer(batch_size)
    }

    fn commit(&mut self, len: usize) {
        self.buffer.commit(len);
        let buf = self.buffer.as_slice();

        let range = self.builder.len()..len;
        self.builder.reserve(range.end - range.start);
        for i in &buf[range] {
            self.builder.append(*i == self.max_level)
        }
    }
}

impl DefinitionLevelBuffer {
    /// Split `len` levels out of `self`
    pub fn split_bitmask(&mut self, len: usize) -> Bitmap {
        let old_len = self.builder.len();
        let num_left_values = old_len - len;
        let new_bitmap_builder =
            BooleanBufferBuilder::new(MIN_BATCH_SIZE.max(num_left_values));

        let old_bitmap =
            std::mem::replace(&mut self.builder, new_bitmap_builder).finish();
        let old_bitmap = Bitmap::from(old_bitmap);

        for i in len..old_len {
            self.builder.append(old_bitmap.is_set(i));
        }

        old_bitmap
    }
}

pub type DefinitionLevelDecoder = ColumnLevelDecoderImpl;
