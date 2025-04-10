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

    /// Create a new ExtensionArray
    pub fn new(extension: Arc<dyn DynExtensionType + Send + Sync>, storage: ArrayRef) -> Self {
        Self::try_new(extension, storage).unwrap()
    }

    /// Return the underlying storage array
    pub fn storage(&self) -> &ArrayRef {
        &self.storage
    }

    /// Return a new array with new storage of the same type
    pub fn with_storage(&self, new_storage: ArrayRef) -> Self {
        assert_eq!(new_storage.data_type(), new_storage.data_type());
        Self {
            data_type: self.data_type.clone(),
            storage: new_storage,
        }
    }
}

impl From<ArrayData> for ExtensionArray {
    fn from(data: ArrayData) -> Self {
        if let DataType::Extension(extension) = data.data_type() {
            let storage_data = ArrayData::try_new(
                extension.storage_type().clone(),
                data.len(),
                data.nulls().map(|b| b.buffer()).cloned(),
                data.offset(),
                data.buffers().to_vec(),
                data.child_data().to_vec(),
            )
            .unwrap();

            Self {
                data_type: data.data_type().clone(),
                storage: Arc::new(make_array(storage_data)) as ArrayRef,
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
        let storage_data = self.storage.to_data();
        ArrayData::try_new(
            self.data_type.clone(),
            storage_data.len(),
            storage_data.nulls().map(|b| b.buffer()).cloned(),
            storage_data.offset(),
            storage_data.buffers().to_vec(),
            storage_data.child_data().to_vec(),
        )
        .unwrap()
    }

    fn into_data(self) -> ArrayData {
        self.to_data()
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
