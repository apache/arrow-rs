use std::{collections::HashMap, sync::Arc};

use arrow_schema::{Schema, SchemaBuilder};
use serde_json::{Value, json};

#[derive(Debug)]
pub enum Crs {
    Projjson(serde_json::Value),
    Srid(u64),
    Other(String),
}

impl Crs {
    // TODO: make fallible
    fn try_from_parquet_str(crs: &str, metadata: &HashMap<String, String>) -> Self {
        let de: Value = serde_json::from_str(crs).unwrap();

        // A CRS that does not exist or is empty defaults to 4326
        // TODO: http link to parquet geospatial doc
        let Some(crs) = de["crs"].as_str() else {
            return Crs::Srid(4326);
        };

        if let Some(key) = crs.strip_prefix("projjson:") {
            let Some(proj_meta) = metadata.get(key) else {
                panic!("Failed to find key in meta: {:?}", metadata)
            };

            Self::Projjson(serde_json::from_str(proj_meta).unwrap())
        } else if let Some(srid) = crs.strip_prefix("srid:") {
            Self::Srid(srid.parse().unwrap())
        } else {
            if crs.is_empty() {
                return Self::Srid(4326);
            }

            Self::Other(crs.to_owned())
        }
    }

    // TODO: make fallible
    pub(super) fn try_from_arrow_str(crs: &str) -> Self {
        let de: Value = serde_json::from_str(crs).unwrap();

        let crs = match de["crs"] {
            Value::Null => panic!("CRS must be specified. Inputs crs {crs}"),
            _ => &de["crs"],
        };

        match de["crs_type"].as_str() {
            Some("projjson") => Crs::Projjson(crs.clone()),
            Some("srid") => Crs::Srid(crs.as_number().unwrap().as_u64().unwrap()),
            _ => Crs::Other(crs.to_string())
        }
    }

    pub(super) fn to_arrow_string(&self) -> String {
        match &self {
            Self::Projjson(pj) => json!({
                "crs": pj,
                "crs_type": "projjson",
            })
            .to_string(),
            Self::Srid(srid) => json!({
                "crs": srid,
                "crs_type": "srid",
            })
            .to_string(),
            Self::Other(s) => json!({
                "crs": s,
            })
            .to_string(),
        }
    }
}

// TODO: make fallible
pub fn parquet_to_arrow(schema: &mut Schema, metadata: &HashMap<String, String>) {
    let n_fields = schema.flattened_fields().len();
    let mut sb: SchemaBuilder = schema.as_ref().into();
    for i in 0..n_fields {
        let mut field = sb.field(i).as_ref().clone();
        // TODO: use consts instead of raw strings
        if let Some(ext_name) = field.extension_type_name()
            && ext_name == "geoarrow.wkb"
            && let Some(ext_meta) = field.metadata_mut().get_mut("ARROW:extension:metadata")
        {
            let crs = Crs::try_from_parquet_str(ext_meta, metadata);
            *ext_meta = crs.to_arrow_string();
        }

        let schema_field = sb.field_mut(i);
        *schema_field = Arc::new(field);
    }

    *schema = sb.finish()
}
