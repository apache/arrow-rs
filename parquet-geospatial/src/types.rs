use arrow_schema::{ArrowError, DataType, extension::ExtensionType};
use serde::{Deserialize, Serialize};

#[derive(Default)]
pub struct WkbType(Option<Metadata>);

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Metadata {
    crs: Option<String>,
    algorithm: Option<String>,
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

    fn deserialize_metadata(
        metadata: Option<&str>,
    ) -> Result<Self::Metadata, arrow_schema::ArrowError> {
        Ok(metadata.map(|md| serde_json::from_str(md).unwrap()))
    }

    fn supports_data_type(
        &self,
        data_type: &arrow_schema::DataType,
    ) -> Result<(), arrow_schema::ArrowError> {
        match data_type {
            DataType::Binary | DataType::LargeBinary | DataType::BinaryView => Ok(()),
            dt => Err(ArrowError::InvalidArgumentError(format!(
                "Geometry data type mismatch, expected one of Binary, LargeBinary, BinaryView. Found {dt}"
            ))),
        }
    }

    fn try_new(
        data_type: &arrow_schema::DataType,
        metadata: Self::Metadata,
    ) -> Result<Self, arrow_schema::ArrowError> {
        let wkb = Self(metadata);
        wkb.supports_data_type(data_type)?;
        Ok(wkb)
    }
}
