use crate::raw::{make_decoder, ArrayDecoder};
use arrow_data::{ArrayData, ArrayDataBuilder};
use arrow_schema::{ArrowError, DataType, Field};
use serde::de::{MapAccess, Visitor};
use serde::Deserializer;
use serde_json::value::RawValue;

pub struct StructArrayDecoder {
    data_type: DataType,
    decoders: Vec<Box<dyn ArrayDecoder>>,
}

impl StructArrayDecoder {
    pub fn new(data_type: DataType) -> Result<Self, ArrowError> {
        let decoders = struct_fields(&data_type)
            .iter()
            .map(|f| make_decoder(f.data_type().clone()))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            data_type,
            decoders,
        })
    }
}

impl ArrayDecoder for StructArrayDecoder {
    fn decode(&mut self, values: &[Option<&RawValue>]) -> Result<ArrayData, ArrowError> {
        let fields = struct_fields(&self.data_type);
        let mut children: Vec<Vec<Option<&RawValue>>> = (0..fields.len())
            .map(|_| vec![None; values.len()])
            .collect();

        for (row, value) in values.iter().enumerate() {
            match value {
                Some(v) => {
                    let mut reader = serde_json::de::Deserializer::from_str(v.get());

                    let visitor = StructVisitor {
                        fields,
                        row,
                        output: &mut children,
                    };

                    reader.deserialize_map(visitor).map_err(|_| {
                        ArrowError::JsonError(format!(
                            "Failed to parse \"{}\" as struct",
                            v.get()
                        ))
                    })?;
                }
                None => {
                    return Err(ArrowError::NotYetImplemented(
                        "Struct containing nested nulls is not yet supported".to_string(),
                    ))
                }
            }
        }

        let child_data = self
            .decoders
            .iter_mut()
            .zip(&children)
            .map(|(decoder, values)| decoder.decode(values))
            .collect::<Result<Vec<_>, _>>()?;

        // Sanity check
        child_data
            .iter()
            .for_each(|x| assert_eq!(x.len(), values.len()));

        let data = ArrayDataBuilder::new(self.data_type.clone())
            .len(values.len())
            .child_data(child_data);

        // Safety
        // Validated lengths above
        Ok(unsafe { data.build_unchecked() })
    }
}

struct StructVisitor<'de, 'a> {
    fields: &'a [Field],
    output: &'a mut [Vec<Option<&'de RawValue>>],
    row: usize,
}

impl<'de, 'a> Visitor<'de> for StructVisitor<'de, 'a> {
    type Value = ();

    // Format a message stating what data this Visitor expects to receive.
    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("an object")
    }

    fn visit_map<M>(self, mut access: M) -> Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        while let Some((key, value)) = access.next_entry::<&'de str, &'de RawValue>()? {
            // Optimize for the common case of a few fields with short names
            if let Some(field_idx) = self.fields.iter().position(|x| x.name() == key) {
                self.output[field_idx][self.row] = Some(value);
            }
        }
        Ok(())
    }
}

fn struct_fields(data_type: &DataType) -> &[Field] {
    match &data_type {
        DataType::Struct(f) => f,
        _ => unreachable!(),
    }
}
