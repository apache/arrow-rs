use std::{any::Any, sync::Arc};

use arrow_data::ArrayData;
use arrow_schema::{extension::DynExtensionType, ArrowError, DataType};

use super::{make_array, Array, ArrayRef};

/// Array type for DataType::Extension
#[derive(Debug)]
pub struct ExtensionArray {
    data_type: DataType,
    storage: ArrayRef,
}

impl ExtensionArray {
    /// Try to create a new ExtensionArray
    pub fn try_new(
        extension: Arc<dyn DynExtensionType + Send + Sync>,
        storage: ArrayRef,
    ) -> Result<Self, ArrowError> {
        Ok(Self {
            data_type: DataType::Extension(extension),
            storage,
        })
    }

    /// Return the underlying storage array
    pub fn storage(&self) -> &ArrayRef {
        &self.storage
    }
}

impl From<ArrayData> for ExtensionArray {
    fn from(data: ArrayData) -> Self {
        if let DataType::Extension(_) = data.data_type() {
            Self {
                data_type: data.data_type().clone(),
                storage: Arc::new(make_array(data)) as ArrayRef,
            }
        } else {
            panic!("{} is not Extension", data.data_type())
        }
    }
}

impl Array for ExtensionArray {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn to_data(&self) -> ArrayData {
        self.storage.to_data()
    }

    fn into_data(self) -> ArrayData {
        self.storage.to_data()
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn slice(&self, offset: usize, length: usize) -> ArrayRef {
        Arc::new(Self {
            data_type: self.data_type.clone(),
            storage: self.storage.slice(offset, length),
        })
    }

    fn len(&self) -> usize {
        self.storage.len()
    }

    fn is_empty(&self) -> bool {
        self.storage.is_empty()
    }

    fn offset(&self) -> usize {
        self.storage.offset()
    }

    fn nulls(&self) -> Option<&arrow_buffer::NullBuffer> {
        self.storage.nulls()
    }

    fn get_buffer_memory_size(&self) -> usize {
        self.storage.get_buffer_memory_size()
    }

    fn get_array_memory_size(&self) -> usize {
        self.storage.get_array_memory_size()
    }
}
