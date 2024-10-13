use std::{collections::HashMap, fmt::Display};

use gaia_tmtc::tco_tmiv::{tmiv_field, TmivField};

use crate::proto::tmtc_generic_c2a::{
    telemetry_field_schema_metadata::{self, ConvType},
    ConversionHex, ConversionStatus, TelemetryFieldDataType, TelemetryFieldSchema,
    TelemetryFieldSchemaMetadata,
};

pub enum ByteSize {
    Uint8,
    Uint16,
    Uint32,
    Uint64,
}

pub fn field_schema_bytes(
    name: &str,
    desctiprion: &str,
    byte_size: ByteSize,
) -> TelemetryFieldSchema {
    let data_type = match byte_size {
        ByteSize::Uint8 => TelemetryFieldDataType::TlmFieldUint8.into(),
        ByteSize::Uint16 => TelemetryFieldDataType::TlmFieldUint16.into(),
        ByteSize::Uint32 => TelemetryFieldDataType::TlmFieldUint32.into(),
        ByteSize::Uint64 => TelemetryFieldDataType::TlmFieldUint64.into(),
    };
    TelemetryFieldSchema {
        metadata: Some(TelemetryFieldSchemaMetadata {
            description: desctiprion.to_string(),
            data_type,
            conv_type: Some(ConvType::Hex(ConversionHex {})),
        }),
        name: name.to_string(),
    }
}

pub fn field_schema_number(
    name: &str,
    desctiprion: &str,
    data_type: TelemetryFieldDataType,
) -> TelemetryFieldSchema {
    TelemetryFieldSchema {
        metadata: Some(TelemetryFieldSchemaMetadata {
            description: desctiprion.to_string(),
            data_type: data_type as i32,
            conv_type: None,
        }),
        name: name.to_string(),
    }
}

pub fn field_schema_enum(
    name: &str,
    desctiprion: &str,
    variants: HashMap<String, i64>,
    default: Option<String>,
) -> TelemetryFieldSchema {
    TelemetryFieldSchema {
        metadata: Some(TelemetryFieldSchemaMetadata {
            description: desctiprion.to_string(),
            data_type: TelemetryFieldDataType::TlmFieldInt32 as i32,
            conv_type: Some(telemetry_field_schema_metadata::ConvType::Status(
                ConversionStatus { variants, default },
            )),
        }),
        name: name.to_string(),
    }
}

pub fn field_int<I: Into<i64>>(name: &str, v: I) -> TmivField {
    TmivField {
        name: name.to_string(),
        value: Some(tmiv_field::Value::Integer(v.into())),
    }
}

pub fn field_optint<I: Into<i64>>(name: &str, v: Option<I>) -> TmivField {
    TmivField {
        name: name.to_string(),
        value: v.map(|v| tmiv_field::Value::Integer(v.into())),
    }
}

pub fn field_double<I: Into<f64>>(name: &str, v: I) -> TmivField {
    TmivField {
        name: name.to_string(),
        value: Some(tmiv_field::Value::Double(v.into())),
    }
}

pub fn field_optdouble<I: Into<f64>>(name: &str, v: Option<I>) -> TmivField {
    TmivField {
        name: name.to_string(),
        value: v.map(|v| tmiv_field::Value::Double(v.into())),
    }
}

pub fn field_enum<S: Display>(name: &str, s: S) -> TmivField {
    TmivField {
        name: name.to_string(),
        value: Some(tmiv_field::Value::Enum(s.to_string())),
    }
}

pub fn field_optenum<S: Display>(name: &str, s: Option<S>) -> TmivField {
    TmivField {
        name: name.to_string(),
        value: s.map(|s| tmiv_field::Value::Enum(s.to_string())),
    }
}

struct OptBool(Option<bool>);
impl Display for OptBool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Some(true) => write!(f, "TRUE"),
            Some(false) => write!(f, "FALSE"),
            None => write!(f, "UNKNOWN"),
        }
    }
}

struct Bool(bool);
impl Display for Bool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            true => write!(f, "TRUE"),
            false => write!(f, "FALSE"),
        }
    }
}

fn field_bool(name: &str, v: bool) -> TmivField {
    field_enum(name, Bool(v))
}

fn field_optbool(name: &str, v: Option<bool>) -> TmivField {
    field_enum(name, OptBool(v))
}