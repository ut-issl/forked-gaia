use std::{collections::HashMap, fmt::Display};

use gaia_tmtc::tco_tmiv::{tmiv_field, TmivField};

use crate::proto::tmtc_generic_c2a::{telemetry_field_schema_metadata, ConversionStatus, TelemetryFieldDataType, TelemetryFieldSchema, TelemetryFieldSchemaMetadata};

pub fn field_schema_int(name: &str, desctiprion: &str) -> TelemetryFieldSchema {
    TelemetryFieldSchema {
        metadata: Some(TelemetryFieldSchemaMetadata {
            description: desctiprion.to_string(),
            data_type: TelemetryFieldDataType::TlmFieldInt32 as i32,
            conv_type: None,
        }),
        name: name.to_string(),
    }
}

pub fn field_schema_double(name: &str, desctiprion: &str) -> TelemetryFieldSchema {
    TelemetryFieldSchema {
        metadata: Some(TelemetryFieldSchemaMetadata {
            description: desctiprion.to_string(),
            data_type: TelemetryFieldDataType::TlmFieldDouble as i32,
            conv_type: None,
        }),
        name: name.to_string(),
    }
}

pub fn field_schema_enum(name: &str, desctiprion: &str, variants: HashMap<String, i64>, default: Option<String>) -> TelemetryFieldSchema {
    TelemetryFieldSchema {
        metadata: Some(TelemetryFieldSchemaMetadata {
            description: desctiprion.to_string(),
            data_type: TelemetryFieldDataType::TlmFieldInt32 as i32,
            conv_type: Some(telemetry_field_schema_metadata::ConvType::Status(ConversionStatus { 
                variants, default 
            })),
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
