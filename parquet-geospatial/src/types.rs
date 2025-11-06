use arrow_schema::{Schema, SchemaBuilder, extension::ExtensionType};

use crate::crs::Crs;

pub struct Geometry {
    crs: Crs,
}

impl Geometry {
    pub fn with_parse_crs(crs: &str) -> Self {
        Self {
            crs: Crs::try_from_arrow_str(crs),
        }
    }
}

impl ExtensionType for Geometry {
    const NAME: &'static str = "geoarrow.wkb";

    type Metadata = Crs;

    fn metadata(&self) -> &Self::Metadata {
        &self.crs
    }

    fn serialize_metadata(&self) -> Option<String> {
        Some(self.crs.to_arrow_string())
    }

    fn deserialize_metadata(
        metadata: Option<&str>,
    ) -> Result<Self::Metadata, arrow_schema::ArrowError> {
        Ok(Crs::try_from_arrow_str(metadata.unwrap()))
    }

    fn supports_data_type(
        &self,
        data_type: &arrow_schema::DataType,
    ) -> Result<(), arrow_schema::ArrowError> {
        Ok(())
    }

    fn try_new(
        data_type: &arrow_schema::DataType,
        metadata: Self::Metadata,
    ) -> Result<Self, arrow_schema::ArrowError> {
        let geom = Self { crs: metadata };
        geom.supports_data_type(data_type)?;
        Ok(geom)
    }
}

pub struct Geography;

impl ExtensionType for Geography {
    const NAME: &'static str = "geoarrow.wkb";

    type Metadata = ();

    fn metadata(&self) -> &Self::Metadata {
        todo!()
    }

    fn serialize_metadata(&self) -> Option<String> {
        todo!()
    }

    fn deserialize_metadata(
        metadata: Option<&str>,
    ) -> Result<Self::Metadata, arrow_schema::ArrowError> {
        todo!()
    }

    fn supports_data_type(
        &self,
        data_type: &arrow_schema::DataType,
    ) -> Result<(), arrow_schema::ArrowError> {
        todo!()
    }

    fn try_new(
        data_type: &arrow_schema::DataType,
        metadata: Self::Metadata,
    ) -> Result<Self, arrow_schema::ArrowError> {
        todo!()
    }
}
