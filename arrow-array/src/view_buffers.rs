use std::sync::Arc;

use arrow_buffer::Buffer;

/// A cheaply cloneable, owned slice of [`Buffer`]
///
/// Similar to `Arc<Vec<Buffer>>` or `Arc<[Buffer]>`
#[derive(Clone, Debug)]
pub struct ViewBuffers(pub(crate) Arc<[Buffer]>);

impl FromIterator<Buffer> for ViewBuffers {
    fn from_iter<T: IntoIterator<Item = Buffer>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

impl From<Vec<Buffer>> for ViewBuffers {
    fn from(value: Vec<Buffer>) -> Self {
        Self(value.into())
    }
}

impl From<&[Buffer]> for ViewBuffers {
    fn from(value: &[Buffer]) -> Self {
        Self(value.into())
    }
}
