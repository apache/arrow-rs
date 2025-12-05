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

use arrow_schema::{ArrowError, DataType, extension::ExtensionType};
use serde::{Deserialize, Serialize};

/// Hints at the likely Parquet geospatial logical type represented by a [`Metadata`].
///
/// Based on the `algorithm` field:
/// - [`Hint::Geometry`]: WKB format with linear/planar edge interpolation
/// - [`Hint::Geography`]: WKB format with explicit non-linear/non-planar edge interpolation
///
/// See the [Parquet Geospatial specification](https://github.com/apache/parquet-format/blob/master/Geospatial.md)
/// for more details.
#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub enum Hint {
    /// Geospatial features in WKB format with linear/planar edge interpolation
    Geometry,
    /// Geospatial features in WKB format with explicit non-linear/non-planar edge interpolation
    Geography,
}

/// The edge interpolation algorithms used with `GEOMETRY` logical types.
#[derive(Default, Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Edges {
    /// Edges are interpolated as geodesics on a sphere.
    #[default]
    Spherical,
    /// <https://en.wikipedia.org/wiki/Vincenty%27s_formulae>
    Vincenty,
    /// Thomas, Paul D. Spheroidal geodesics, reference systems, & local geometry. US Naval Oceanographic Office, 1970
    Thomas,
    /// Thomas, Paul D. Mathematical models for navigation systems. US Naval Oceanographic Office, 1965.
    Andoyer,
    /// Karney, Charles FF. "Algorithms for geodesics." Journal of Geodesy 87 (2013): 43-55
    Karney,
}

/// The metadata associated with a [`WkbType`].
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Metadata {
    /// The Coordinate Reference System (CRS) of the [`WkbType`], if present.
    ///
    /// This may be a raw string value (e.g., "EPSG:3857") or a JSON object (e.g., PROJJSON).
    /// Note: Common lon/lat CRS representations (EPSG:4326, OGC:CRS84) are canonicalized
    /// to `None` during serialization to match Parquet conventions.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub crs: Option<serde_json::Value>,
    /// The edge interpolation algorithm of the [`WkbType`], if present.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub algorithm: Option<Edges>,
}

impl Metadata {
    /// Constructs a new [`Metadata`] with the given CRS and algorithm.
    ///
    /// If a CRS is provided, and can be parsed as JSON, it will be stored as a JSON object instead
    /// of its string representation.
    pub fn new(crs: Option<&str>, algorithm: Option<Edges>) -> Self {
        let crs = crs.map(|c| match serde_json::from_str(c) {
            Ok(crs) => crs,
            Err(_) => serde_json::Value::String(c.to_string()),
        });

        Self { crs, algorithm }
    }

    /// Returns a [`Hint`] to the likely underlying Logical Type that this [`Metadata`] represents.
    pub fn type_hint(&self) -> Hint {
        match &self.algorithm {
            Some(_) => Hint::Geography,
            None => Hint::Geometry,
        }
    }

    /// Detect if the CRS is a common representation of lon/lat on the standard WGS84 ellipsoid
    fn crs_is_lon_lat(&self) -> bool {
        use serde_json::Value;

        let Some(crs) = &self.crs else {
            return false;
        };

        match crs {
            Value::String(s) if s == "EPSG:4326" || s == "OGC:CRS84" => true,
            Value::Object(_) => match (&crs["id"]["authority"], &crs["id"]["code"]) {
                (Value::String(auth), Value::String(code)) if auth == "OGC" && code == "CRS84" => {
                    true
                }
                (Value::String(auth), Value::String(code)) if auth == "EPSG" && code == "4326" => {
                    true
                }
                (Value::String(auth), Value::Number(code))
                    if auth == "EPSG" && code.as_i64() == Some(4326) =>
                {
                    true
                }
                _ => false,
            },
            _ => false,
        }
    }
}

/// Well-Known Binary (WKB) [`ExtensionType`] for geospatial data.
///
/// Represents the canonical Arrow Extension Type for storing
/// [GeoArrow](https://github.com/geoarrow/geoarrow) data.
#[derive(Debug, Default)]
pub struct WkbType(Metadata);

impl WkbType {
    /// Constructs a new [`WkbType`] with the given [`Metadata`].
    ///
    /// If `None` is provided, default (empty) metadata is used.
    pub fn new(metadata: Option<Metadata>) -> Self {
        Self(metadata.unwrap_or_default())
    }
}

type ArrowResult<T> = Result<T, ArrowError>;
impl ExtensionType for WkbType {
    const NAME: &'static str = "geoarrow.wkb";

    type Metadata = Metadata;

    fn metadata(&self) -> &Self::Metadata {
        &self.0
    }

    fn serialize_metadata(&self) -> Option<String> {
        let md = if self.0.crs_is_lon_lat() {
            &Metadata {
                crs: None, // lon/lat CRS is canonicalized as omitted (None) for Parquet
                algorithm: self.0.algorithm,
            }
        } else {
            &self.0
        };

        serde_json::to_string(md).ok()
    }

    fn deserialize_metadata(metadata: Option<&str>) -> ArrowResult<Self::Metadata> {
        let Some(metadata) = metadata else {
            return Ok(Self::Metadata::default());
        };

        serde_json::from_str(metadata).map_err(|e| ArrowError::JsonError(e.to_string()))
    }

    fn supports_data_type(&self, data_type: &arrow_schema::DataType) -> ArrowResult<()> {
        match data_type {
            DataType::Binary | DataType::LargeBinary | DataType::BinaryView => Ok(()),
            dt => Err(ArrowError::InvalidArgumentError(format!(
                "Geometry data type mismatch, expected one of Binary, LargeBinary, BinaryView. Found {dt}"
            ))),
        }
    }

    fn try_new(data_type: &arrow_schema::DataType, metadata: Self::Metadata) -> ArrowResult<Self> {
        let wkb = Self(metadata);
        wkb.supports_data_type(data_type)?;
        Ok(wkb)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::Field;

    /// Test metadata serialization and deserialization with empty/default metadata
    #[test]
    fn test_metadata_empty_roundtrip() -> ArrowResult<()> {
        let metadata = Metadata::default();
        let wkb = WkbType::new(Some(metadata));

        let serialized = wkb.serialize_metadata().unwrap();
        assert_eq!(serialized, "{}");

        let deserialized = WkbType::deserialize_metadata(Some(&serialized))?;
        assert!(deserialized.crs.is_none());
        assert!(deserialized.algorithm.is_none());

        Ok(())
    }

    /// Test metadata serialization with CRS as a simple string
    #[test]
    fn test_metadata_crs_string_roundtrip() -> ArrowResult<()> {
        let metadata = Metadata::new(Some("srid:1234"), None);
        let wkb = WkbType::new(Some(metadata));

        let serialized = wkb.serialize_metadata().unwrap();
        assert_eq!(serialized, r#"{"crs":"srid:1234"}"#);

        let deserialized = WkbType::deserialize_metadata(Some(&serialized))?;
        assert_eq!(
            deserialized.crs.unwrap(),
            serde_json::Value::String(String::from("srid:1234"))
        );
        assert!(deserialized.algorithm.is_none());

        Ok(())
    }

    /// Test metadata serialization with CRS as a JSON object
    #[test]
    fn test_metadata_crs_json_object_roundtrip() -> ArrowResult<()> {
        let crs_json = r#"{"type":"custom_json","properties":{"name":"EPSG:4326"}}"#;
        let metadata = Metadata::new(Some(crs_json), None);
        let wkb = WkbType::new(Some(metadata));

        let serialized = wkb.serialize_metadata().unwrap();
        // Validate by parsing the JSON and checking structure (field order is not guaranteed)
        let parsed: serde_json::Value = serde_json::from_str(&serialized).unwrap();
        assert_eq!(parsed["crs"]["type"], "custom_json");
        assert_eq!(parsed["crs"]["properties"]["name"], "EPSG:4326");

        let deserialized = WkbType::deserialize_metadata(Some(&serialized))?;

        // Verify it's a JSON object with expected structure
        let crs = deserialized.crs.unwrap();
        assert!(crs.is_object());
        assert_eq!(crs["type"], "custom_json");
        assert_eq!(crs["properties"]["name"], "EPSG:4326");

        Ok(())
    }

    /// Test metadata serialization with algorithm field
    #[test]
    fn test_metadata_algorithm_roundtrip() -> ArrowResult<()> {
        let metadata = Metadata::new(None, Some(Edges::Spherical));
        let wkb = WkbType::new(Some(metadata));

        let serialized = wkb.serialize_metadata().unwrap();
        assert_eq!(serialized, r#"{"algorithm":"spherical"}"#);

        let deserialized = WkbType::deserialize_metadata(Some(&serialized))?;
        assert!(deserialized.crs.is_none());
        assert_eq!(deserialized.algorithm, Some(Edges::Spherical));

        Ok(())
    }

    /// Test metadata serialization with both CRS and algorithm
    #[test]
    fn test_metadata_full_roundtrip() -> ArrowResult<()> {
        let metadata = Metadata::new(Some("srid:1234"), Some(Edges::Spherical));
        let wkb = WkbType::new(Some(metadata));

        let serialized = wkb.serialize_metadata().unwrap();
        assert_eq!(serialized, r#"{"crs":"srid:1234","algorithm":"spherical"}"#);

        let deserialized = WkbType::deserialize_metadata(Some(&serialized))?;
        assert_eq!(
            deserialized.crs.unwrap(),
            serde_json::Value::String("srid:1234".to_string())
        );
        assert_eq!(deserialized.algorithm, Some(Edges::Spherical));

        Ok(())
    }

    /// Test deserialization of None metadata
    #[test]
    fn test_metadata_deserialize_none() -> ArrowResult<()> {
        let deserialized = WkbType::deserialize_metadata(None)?;
        assert!(deserialized.crs.is_none());
        assert!(deserialized.algorithm.is_none());
        Ok(())
    }

    /// Test deserialization of invalid JSON
    #[test]
    fn test_metadata_deserialize_invalid_json() {
        let result = WkbType::deserialize_metadata(Some("not valid json {"));
        assert!(matches!(result, Err(ArrowError::JsonError(_))));
    }

    /// Test metadata that results in a Geometry type hint
    #[test]
    fn test_type_hint_geometry() {
        let metadata = Metadata::new(None, None);
        assert!(matches!(metadata.type_hint(), Hint::Geometry));
    }

    /// Test metadata that results in a Geography type hint
    #[test]
    fn test_type_hint_edges_is_geography() {
        let algorithms = vec![
            Edges::Spherical,
            Edges::Vincenty,
            Edges::Thomas,
            Edges::Andoyer,
            Edges::Karney,
        ];
        for algo in algorithms {
            let metadata = Metadata::new(None, Some(algo));
            assert!(matches!(metadata.type_hint(), Hint::Geography));
        }
    }

    /// Test extension type integration using a Field
    #[test]
    fn test_extension_type_with_field() -> ArrowResult<()> {
        let metadata = Metadata::new(Some("srid:1234"), None);
        let wkb_type = WkbType::new(Some(metadata));

        let mut field = Field::new("geometry", DataType::Binary, false);
        field.try_with_extension_type(wkb_type)?;

        // Verify we can extract the extension type back
        let extracted = field.try_extension_type::<WkbType>()?;
        assert_eq!(
            extracted.metadata().crs.as_ref().unwrap(),
            &serde_json::Value::String(String::from("srid:1234"))
        );

        Ok(())
    }

    /// Test extension type DataType support
    #[test]
    fn test_extension_type_support() -> ArrowResult<()> {
        let wkb = WkbType::default();
        // supported types
        wkb.supports_data_type(&DataType::Binary)?;
        wkb.supports_data_type(&DataType::LargeBinary)?;
        wkb.supports_data_type(&DataType::BinaryView)?;

        // reject unsupported types with an error
        let result = wkb.supports_data_type(&DataType::Utf8);
        assert!(matches!(result, Err(ArrowError::InvalidArgumentError(_))));

        Ok(())
    }

    /// Test CRS canonicalization logic for common lon/lat representations
    #[test]
    fn test_crs_canonicalization() -> ArrowResult<()> {
        // EPSG:4326 as string should be omitted
        let metadata = Metadata::new(Some("EPSG:4326"), None);
        let wkb = WkbType::new(Some(metadata));
        let serialized = wkb.serialize_metadata().unwrap();
        assert_eq!(serialized, "{}");

        // OGC:CRS84 as string should be omitted
        let metadata = Metadata::new(Some("OGC:CRS84"), None);
        let wkb = WkbType::new(Some(metadata));
        let serialized = wkb.serialize_metadata().unwrap();
        assert_eq!(serialized, "{}");

        // A JSON object that reasonably looks like PROJJSON for EPSG:4326 should be omitted
        // detect "4326" as a string
        let crs_json = r#"{"id":{"authority":"EPSG","code":"4326"}}"#;
        let metadata = Metadata::new(Some(crs_json), None);
        let wkb = WkbType::new(Some(metadata));
        let serialized = wkb.serialize_metadata().unwrap();
        assert_eq!(serialized, "{}");

        // detect 4326 as a number
        let crs_json = r#"{"id":{"authority":"EPSG","code":4326}}"#;
        let metadata = Metadata::new(Some(crs_json), None);
        let wkb = WkbType::new(Some(metadata));
        let serialized = wkb.serialize_metadata().unwrap();
        assert_eq!(serialized, "{}");

        // A JSON object that reasonably looks like PROJJSON for OGC:CRS84 should be omitted
        let crs_json = r#"{"id":{"authority":"OGC","code":"CRS84"}}"#;
        let metadata = Metadata::new(Some(crs_json), None);
        let wkb = WkbType::new(Some(metadata));
        let serialized = wkb.serialize_metadata().unwrap();
        assert_eq!(serialized, "{}");

        // Other input types should be preserved
        let metadata = Metadata::new(Some("srid:1234"), None);
        let wkb = WkbType::new(Some(metadata));
        let serialized = wkb.serialize_metadata().unwrap();
        assert_eq!(serialized, r#"{"crs":"srid:1234"}"#);

        // Canonicalization should work with algorithm field
        let metadata = Metadata::new(Some("EPSG:4326"), Some(Edges::Spherical));
        let wkb = WkbType::new(Some(metadata));
        let serialized = wkb.serialize_metadata().unwrap();
        assert_eq!(serialized, r#"{"algorithm":"spherical"}"#);

        Ok(())
    }
}
