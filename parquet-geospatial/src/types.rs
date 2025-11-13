use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BinaryArray, BinaryViewArray, LargeBinaryArray, make_array};
use arrow::buffer::NullBuffer;
use arrow::error::Result;
use arrow_schema::Field;
use arrow_schema::{ArrowError, DataType, extension::ExtensionType};
use serde::{Deserialize, Serialize};

#[derive(Default)]
pub struct WkbType(Option<Metadata>);

impl WkbType {
    pub fn new(metadata: Option<Metadata>) -> Self {
        Self(metadata)
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Metadata {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub crs: Option<String>, // TODO: explore when this is valid JSON to avoid double escaping
    #[serde(skip_serializing_if = "Option::is_none")]
    pub algorithm: Option<String>,
}

impl ExtensionType for WkbType {
    const NAME: &'static str = "geoarrow.wkb";

    type Metadata = Option<Metadata>;

    fn metadata(&self) -> &Self::Metadata {
        &self.0
    }

    fn serialize_metadata(&self) -> Option<String> {
        self.0.clone().map(|md| serde_json::to_string(&md).unwrap())
    }

    fn deserialize_metadata(metadata: Option<&str>) -> Result<Self::Metadata> {
        Ok(metadata.map(|md| serde_json::from_str(md).unwrap()))
    }

    fn supports_data_type(&self, data_type: &arrow_schema::DataType) -> Result<()> {
        match data_type {
            DataType::Binary | DataType::LargeBinary | DataType::BinaryView => Ok(()),
            dt => Err(ArrowError::InvalidArgumentError(format!(
                "Geometry data type mismatch, expected one of Binary, LargeBinary, BinaryView. Found {dt}"
            ))),
        }
    }

    fn try_new(data_type: &arrow_schema::DataType, metadata: Self::Metadata) -> Result<Self> {
        let wkb = Self(metadata);
        wkb.supports_data_type(data_type)?;
        Ok(wkb)
    }
}

pub struct WkbArray {
    inner: ArrayRef,
    metadata: Option<Metadata>,
}

impl WkbArray {
    pub fn try_new(inner: &dyn Array, metadata: Option<Metadata>) -> Result<Self> {
        // TODO: validate the input array is one of our expected binary types
        let inner = make_array(inner.to_data());
        Ok(Self { inner, metadata })
    }

    /// Returns a reference to the underlying [`ArrayRef`]
    pub fn inner(&self) -> &ArrayRef {
        &self.inner
    }

    /// Returns the inner [`ArrayRef`], consuming self
    pub fn into_inner(self) -> ArrayRef {
        self.inner
    }

    /// Returns a reference to the [`Metadata`] associated with this [`WkbArray`]
    pub fn metadata(&self) -> &Option<Metadata> {
        &self.metadata
    }

    /// Return the WKB (Well-Known-Binary) data stored at the given row
    pub fn value(&self, index: usize) -> &[u8] {
        if let Some(bv) = self.inner.as_any().downcast_ref::<BinaryViewArray>() {
            bv.value(index)
        } else if let Some(b) = self.inner.as_any().downcast_ref::<BinaryArray>() {
            b.value(index)
        } else if let Some(lb) = self.inner.as_any().downcast_ref::<LargeBinaryArray>() {
            lb.value(index)
        } else {
            // Panic safety: Our try_new method ensures our inner array is one of the expected
            // binary array types
            unreachable!()
        }
    }

    /// Return a [`Field`] to represent this [`WkbArray`] in an [`arrow_schema::Schema`] with a particular name
    pub fn field(&self, name: impl Into<String>) -> Field {
        Field::new(
            name.into(),
            self.data_type().clone(),
            self.inner.is_nullable(),
        )
        .with_extension_type(WkbType(self.metadata().clone()))
    }

    /// Returns a new [`DataType`] representing this [`WkbArray`]'s inner type
    pub fn data_type(&self) -> &DataType {
        self.inner.data_type()
    }

    pub fn slice(&self, offset: usize, length: usize) -> Self {
        let inner = self.inner.slice(offset, length);
        Self {
            inner,
            metadata: self.metadata().clone(),
        }
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn nulls(&self) -> Option<&NullBuffer> {
        self.inner.nulls()
    }

    /// Is the element at index null?
    pub fn is_null(&self, index: usize) -> bool {
        self.nulls().is_some_and(|n| n.is_null(index))
    }

    /// Is the element at index valid (not null)?
    pub fn is_valid(&self, index: usize) -> bool {
        !self.is_null(index)
    }

    // TODO: implement iter()
}

impl From<WkbArray> for ArrayRef {
    fn from(wkb_array: WkbArray) -> Self {
        Arc::new(wkb_array.into_inner())
    }
}
