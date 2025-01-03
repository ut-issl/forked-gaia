use std::{
    collections::{HashMap, HashSet},
    fmt::{self, Display},
};

use anyhow::{anyhow, Result};
use gaia_ccsds_c2a::access::tlm::schema::{
    from_tlmcmddb, FieldSchema, FieldValueSchema, FloatingFieldSchema, IntegralFieldSchema,
};
use itertools::Itertools;

use crate::{
    proto::tmtc_generic_c2a::{self as proto, ConversionHex},
    satconfig,
};

#[derive(Debug, Clone)]
pub struct FatTelemetrySchema {
    component: String,
    telemetry: String,
    is_restricted: bool,
    pub schema: TelemetrySchema,
}

impl FatTelemetrySchema {
    pub fn build_tmiv_name<'a>(&'a self, channel: &'a str) -> TmivName<'a> {
        TmivName {
            channel,
            component: &self.component,
            telemetry: &self.telemetry,
        }
    }
}

pub struct TmivName<'a> {
    channel: &'a str,
    component: &'a str,
    telemetry: &'a str,
}

impl<'a> Display for TmivName<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.channel, self.component, self.telemetry)
    }
}

#[derive(Debug, Clone)]
pub struct TelemetrySchema {
    pub integral_fields: Vec<(FieldMetadata, IntegralFieldSchema)>,
    pub floating_fields: Vec<(FieldMetadata, FloatingFieldSchema)>,
}

#[derive(Debug, Clone)]
pub struct FieldMetadata {
    order: usize,
    original_name: String,
    pub converted_name: String,
    pub raw_name: String,
    pub description: String,
    pub data_type: DataType,
    pub conv_type: ConversionType,
}

#[derive(Debug, Clone)]
pub enum DataType {
    Int8,
    Int16,
    Int32,
    Int64,
    Uint8,
    Uint16,
    Uint32,
    Uint64,
    Float,
    Double,
}

#[derive(Debug, Clone)]
pub enum ConversionType {
    None,
    Hex,
    Polynomial {
        coefficients: Vec<f64>,
    },
    Status {
        variants: HashMap<i64, String>,
        default: Option<String>,
    },
}

#[derive(Debug, Clone)]
pub struct Registry {
    channel_map: satconfig::TelemetryChannelMap,
    schema_map: HashMap<(u16, u8), FatTelemetrySchema>,
}

impl Registry {
    pub fn build_telemetry_channel_schema_map(
        &self,
    ) -> HashMap<String, proto::TelemetryChannelSchema> {
        self.channel_map
            .iter()
            .map(|(channel_name, ch)| {
                let channel_name = channel_name.to_string();
                let telmetry_channel_schema = proto::TelemetryChannelSchema {
                    metadata: Some(proto::TelemetryChannelSchemaMetadata {
                        destination_flag_mask: ch.destination_flag_mask as u32,
                    }),
                };
                (channel_name, telmetry_channel_schema)
            })
            .collect()
    }

    pub fn build_telemetry_component_schema_map(
        &self,
    ) -> HashMap<String, proto::TelemetryComponentSchema> {
        self.schema_map
            .iter()
            .map(|((apid, tlm_id), fat_tlm_schema)| {
                let fields = fat_tlm_schema
                    .schema
                    .integral_fields
                    .iter()
                    .map(|(m, _)| m)
                    .chain(fat_tlm_schema.schema.floating_fields.iter().map(|(m, _)| m))
                    .sorted_by_key(|m| m.order)
                    .map(|m| proto::TelemetryFieldSchema {
                        metadata: Some(proto::TelemetryFieldSchemaMetadata {
                            description: m.description.clone(),
                            data_type: match m.data_type {
                                DataType::Int8 => {
                                    proto::TelemetryFieldDataType::TlmFieldInt8 as i32
                                }
                                DataType::Int16 => {
                                    proto::TelemetryFieldDataType::TlmFieldInt16 as i32
                                }
                                DataType::Int32 => {
                                    proto::TelemetryFieldDataType::TlmFieldInt32 as i32
                                }
                                DataType::Int64 => {
                                    proto::TelemetryFieldDataType::TlmFieldInt64 as i32
                                }
                                DataType::Uint8 => {
                                    proto::TelemetryFieldDataType::TlmFieldUint8 as i32
                                }
                                DataType::Uint16 => {
                                    proto::TelemetryFieldDataType::TlmFieldUint16 as i32
                                }
                                DataType::Uint32 => {
                                    proto::TelemetryFieldDataType::TlmFieldUint32 as i32
                                }
                                DataType::Uint64 => {
                                    proto::TelemetryFieldDataType::TlmFieldUint64 as i32
                                }
                                DataType::Float => {
                                    proto::TelemetryFieldDataType::TlmFieldFloat as i32
                                }
                                DataType::Double => {
                                    proto::TelemetryFieldDataType::TlmFieldDouble as i32
                                }
                            },
                            conv_type: match &m.conv_type {
                                ConversionType::None => None,
                                ConversionType::Hex => {
                                    Some(proto::telemetry_field_schema_metadata::ConvType::Hex(
                                        ConversionHex {},
                                    ))
                                }
                                ConversionType::Polynomial { coefficients } => Some(
                                    proto::telemetry_field_schema_metadata::ConvType::Polynomial(
                                        proto::ConversionPolynomial {
                                            coefficients: coefficients.clone(),
                                        },
                                    ),
                                ),
                                ConversionType::Status { variants, default } => {
                                    let variants =
                                        variants.iter().map(|(k, v)| (v.clone(), *k)).collect();
                                    Some(proto::telemetry_field_schema_metadata::ConvType::Status(
                                        proto::ConversionStatus {
                                            variants,
                                            default: default.clone(),
                                        },
                                    ))
                                }
                            },
                        }),
                        name: m.original_name.to_string(),
                    })
                    .collect();
                let telemetry_schema = proto::TelemetrySchema {
                    metadata: Some(proto::TelemetrySchemaMetadata {
                        id: *tlm_id as u32,
                        is_restricted: fat_tlm_schema.is_restricted,
                    }),
                    fields,
                };
                (
                    (fat_tlm_schema.component.as_str(), *apid),
                    fat_tlm_schema.telemetry.as_str(),
                    telemetry_schema,
                )
            })
            .sorted_by_key(|&((component_name, _), _, _)| component_name)
            .group_by(|&(key, _, _)| key)
            .into_iter()
            .map(|((component_name, apid), group)| {
                let metadata = proto::TelemetryComponentSchemaMetadata { apid: apid as u32 };
                let telemetries: HashMap<String, proto::TelemetrySchema> = group
                    .map(|(_, telemetry_name, telemetry_schema)| {
                        (telemetry_name.to_string(), telemetry_schema)
                    })
                    .collect();
                let component_name = component_name.to_string();
                let telemetry_component_schema = proto::TelemetryComponentSchema {
                    metadata: Some(metadata),
                    telemetries,
                };
                (component_name, telemetry_component_schema)
            })
            .collect()
    }

    pub fn all_tmiv_names(&self) -> HashSet<String> {
        self.channel_map
            .keys()
            .flat_map(|channel| {
                self.schema_map
                    .values()
                    .map(|schema| schema.build_tmiv_name(channel).to_string())
            })
            .collect()
    }

    pub fn find_channels(&self, destination_flags: u8) -> impl Iterator<Item = &str> {
        self.channel_map.iter().filter_map(move |(name, ch)| {
            if ch.destination_flag_mask & destination_flags != 0 {
                Some(name.as_str())
            } else {
                None
            }
        })
    }

    pub fn lookup(&self, apid: u16, tlm_id: u8) -> Option<&FatTelemetrySchema> {
        let fat_schema = self.schema_map.get(&(apid, tlm_id))?;
        Some(fat_schema)
    }

    pub fn from_tlmcmddb_with_apid_map(
        db: &tlmcmddb::Database,
        apid_map: &HashMap<u16, String>,
        channel_map: satconfig::TelemetryChannelMap,
    ) -> Result<Self> {
        let mut rev_apid_map: HashMap<&str, Vec<u16>> = HashMap::new();
        for (apid, component) in apid_map.iter() {
            let entry = rev_apid_map.entry(component.as_str());
            entry
                .and_modify(|e| e.push(*apid))
                .or_insert_with(|| vec![*apid]);
        }

        let mut schema_map = HashMap::new();
        for (metadata, fields) in from_tlmcmddb(db).flatten() {
            let apids = rev_apid_map
                .get(metadata.component_name.as_str())
                .ok_or_else(|| anyhow!("APID not defined for {}", metadata.component_name))?;
            let schema = build_telemetry_schema(fields)?;
            for apid in apids {
                let metadata = metadata.clone();
                let schema = schema.clone();
                schema_map.insert(
                    (*apid, metadata.tlm_id),
                    FatTelemetrySchema {
                        component: metadata.component_name,
                        telemetry: metadata.telemetry_name,
                        is_restricted: metadata.is_restricted,
                        schema,
                    },
                );
            }
        }
        Ok(Self {
            channel_map,
            schema_map,
        })
    }
}

fn build_telemetry_schema<'a>(
    iter: impl Iterator<Item = Result<(&'a str, FieldSchema)>>,
) -> Result<TelemetrySchema> {
    let mut schema = TelemetrySchema {
        integral_fields: vec![],
        floating_fields: vec![],
    };
    for (order, pair) in iter.enumerate() {
        let (field_name, field_schema) = pair?;
        let (data_type, conv_type) = match &field_schema.value {
            FieldValueSchema::Integral(schema) => (
                match schema.field {
                    structpack::GenericIntegralField::I8(_) => DataType::Int8,
                    structpack::GenericIntegralField::I16(_) => DataType::Int16,
                    structpack::GenericIntegralField::I32(_) => DataType::Int32,
                    structpack::GenericIntegralField::I64(_) => DataType::Int64,
                    structpack::GenericIntegralField::U8(_) => DataType::Uint8,
                    structpack::GenericIntegralField::U16(_) => DataType::Uint16,
                    structpack::GenericIntegralField::U32(_) => DataType::Uint32,
                    structpack::GenericIntegralField::U64(_) => DataType::Uint64,
                },
                match &schema.converter {
                    None => ConversionType::None,
                    Some(gaia_ccsds_c2a::access::tlm::converter::Integral::Hex) => {
                        ConversionType::Hex
                    }
                    Some(gaia_ccsds_c2a::access::tlm::converter::Integral::Polynomial(poly)) => {
                        ConversionType::Polynomial {
                            coefficients: poly.to_vec(),
                        }
                    }
                    Some(gaia_ccsds_c2a::access::tlm::converter::Integral::Status(status)) => {
                        let (variants, default) = status.to_map_and_default();
                        ConversionType::Status {
                            variants,
                            default: Some(default),
                        }
                    }
                },
            ),
            FieldValueSchema::Floating(schema) => (
                match schema.field {
                    structpack::GenericFloatingField::F32(_) => DataType::Float,
                    structpack::GenericFloatingField::F64(_) => DataType::Double,
                },
                match &schema.converter {
                    None => ConversionType::None,
                    Some(poly) => ConversionType::Polynomial {
                        coefficients: poly.to_vec(),
                    },
                },
            ),
        };
        let name_pair = build_field_metadata(
            order,
            field_name,
            &field_schema.metadata.description,
            data_type,
            conv_type,
        );
        match field_schema.value {
            FieldValueSchema::Integral(field_schema) => {
                schema.integral_fields.push((name_pair, field_schema));
            }
            FieldValueSchema::Floating(field_schema) => {
                schema.floating_fields.push((name_pair, field_schema));
            }
        }
    }
    Ok(schema)
}

fn build_field_metadata(
    order: usize,
    tlmdb_name: &str,
    description: &str,
    data_type: DataType,
    conv_type: ConversionType,
) -> FieldMetadata {
    FieldMetadata {
        order,
        original_name: tlmdb_name.to_string(),
        converted_name: tlmdb_name.to_string(),
        raw_name: format!("{tlmdb_name}@RAW"),
        description: description.to_string(),
        data_type,
        conv_type,
    }
}
