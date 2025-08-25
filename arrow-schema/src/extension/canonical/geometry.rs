use crate::extension::ExtensionType;
use crate::ArrowError;

/// Geospatial features in the WKB format with linear/planar edges interpolation
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
pub struct Geometry {
    crs: Option<String>,
}

impl Geometry {
    /// Create a new Geometry extension type with an optional CRS.
    pub fn new(crs: Option<String>) -> Self {
        Self { crs }
    }

    /// Get the CRS of the Geometry type, if any.
    pub fn crs(&self) -> Option<&str> {
        self.crs.as_deref()
    }
}

impl ExtensionType for Geometry {
    const NAME: &'static str = "geoarrow.wkb";

    type Metadata = ();

    fn metadata(&self) -> &Self::Metadata {
        &()
    }

    fn serialize_metadata(&self) -> Option<String> {
        None
    }

    fn deserialize_metadata(_metadata: Option<&str>) -> Result<Self::Metadata, ArrowError> {
        Ok(())
    }

    fn supports_data_type(&self, data_type: &crate::DataType) -> Result<(), ArrowError> {
        match data_type {
            crate::DataType::Binary
            | crate::DataType::LargeBinary
            | crate::DataType::BinaryView => Ok(()),
            data_type => Err(ArrowError::InvalidArgumentError(format!(
                "Geometry data type mismatch, expected one of Binary, LargeBinary, BinaryView, found {data_type}"
            ))),
        }
    }

    fn try_new(data_type: &crate::DataType, _metadata: Self::Metadata) -> Result<Self, ArrowError> {
        // TODO: fix
        let geo = Self { crs: None };
        geo.supports_data_type(data_type)?;
        Ok(geo)
    }
}

/// Edge interpolation algorithm for Geography logical type
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum GeographyAlgorithm {
    /// Edges are interpolated as geodesics on a sphere.
    SPHERICAL,

    /// <https://en.wikipedia.org/wiki/Vincenty%27s_formulae>
    VINCENTY,

    /// Thomas, Paul D. Spheroidal geodesics, reference systems, & local geometry. US Naval Oceanographic Office, 1970
    THOMAS,

    /// Thomas, Paul D. Mathematical models for navigation systems. US Naval Oceanographic Office, 1965.
    ANDOYER,

    /// Karney, Charles FF. "Algorithms for geodesics." Journal of Geodesy 87 (2013): 43-55
    KARNEY,
}

/// Geospatial features in the [WKB format](https://libgeos.org/specifications/wkb/) with an
/// explicit (non-linear/non-planar) edges interpolation algorithm.
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
pub struct Geography {
    crs: Option<String>,
    algorithm: Option<GeographyAlgorithm>,
}

impl Geography {
    /// Create a new Geography extension type with an optional CRS and algorithm.
    pub fn new(crs: Option<String>, algorithm: Option<GeographyAlgorithm>) -> Self {
        Self { crs, algorithm }
    }

    /// Get the CRS of the Geography type, if any.
    pub fn crs(&self) -> Option<&str> {
        self.crs.as_deref()
    }

    /// Get the edge interpolation algorithm of the Geography type, if any.
    pub fn algorithm(&self) -> Option<&GeographyAlgorithm> {
        self.algorithm.as_ref()
    }
}

impl ExtensionType for Geography {
    const NAME: &'static str = "geoarrow.wkb";

    type Metadata = ();

    fn metadata(&self) -> &Self::Metadata {
        &()
    }

    fn serialize_metadata(&self) -> Option<String> {
        None
    }

    fn deserialize_metadata(_metadata: Option<&str>) -> Result<Self::Metadata, ArrowError> {
        Ok(())
    }

    fn supports_data_type(&self, data_type: &crate::DataType) -> Result<(), ArrowError> {
        match data_type {
            crate::DataType::Binary
            | crate::DataType::LargeBinary
            | crate::DataType::BinaryView => Ok(()),
            data_type => Err(ArrowError::InvalidArgumentError(format!(
                "Geography data type mismatch, expected one of Binary, LargeBinary, BinaryView, found {data_type}"
            ))),
        }
    }

    fn try_new(data_type: &crate::DataType, _metadata: Self::Metadata) -> Result<Self, ArrowError> {
        // TODO: fix
        let geo = Self::default();
        geo.supports_data_type(data_type)?;
        Ok(geo)
    }
}
