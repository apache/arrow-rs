use std::{collections::HashMap, sync::Arc};

use arrow_schema::{Schema, SchemaBuilder};
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Crs {
    crs: Option<String>,
    crs_type: Option<String>,
}

impl Crs {
    // TODO: make fallible
    fn try_from_parquet_str(crs: &str, metadata: &HashMap<String, String>) -> Self {
        let crs: Crs = serde_json::from_str(crs).unwrap();

        let Some(crs_value) = &crs.crs else {
            return crs;
        };

        let Some(key) = crs_value.strip_prefix("projjson:") else {
            return crs;
        };

        let Some(proj_meta) = metadata.get(key) else {
            return crs;
        };

        Self {
            crs: Some(proj_meta.clone()),
            crs_type: Some(String::from("projjson")),
        }
    }

    pub(super) fn to_arrow_string(&self) -> String {
        serde_json::to_string(&self).unwrap_or_default()
    }
}

// TODO: make fallible
pub fn replace_keyvalue(schema: &mut Schema, metadata: &HashMap<String, String>) {
    let n_fields = schema.flattened_fields().len();
    let mut sb: SchemaBuilder = schema.as_ref().into();
    for i in 0..n_fields {
        let mut field = sb.field(i).as_ref().clone();
        // TODO: use consts instead of raw strings
        if let Some("geoarrow.wkb") = field.extension_type_name() {
            if let Some(ext_meta) = field.metadata_mut().get_mut("ARROW:extension:metadata") {
                let crs = Crs::try_from_parquet_str(ext_meta, metadata);
                *ext_meta = crs.to_arrow_string();

                let schema_field = sb.field_mut(i);
                *schema_field = Arc::new(field);
            }
        }
    }

    *schema = sb.finish()
}
