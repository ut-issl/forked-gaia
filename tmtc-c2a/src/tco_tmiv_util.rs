use std::fmt::Display;

use gaia_tmtc::tco_tmiv::{tmiv, tmiv_field, TmivField};

pub fn field_schema_int(name: &str) -> tmiv::FieldSchema {
    tmiv::FieldSchema {
        name: name.to_string(),
        data_type: tmiv::DataType::INTEGER,
        variants: vec![],
    }
}

pub fn field_schema_double(name: &str) -> tmiv::FieldSchema {
    tmiv::FieldSchema {
        name: name.to_string(),
        data_type: tmiv::DataType::DOUBLE,
        variants: vec![],
    }
}

pub fn field_schema_enum(name: &str, variants: &[&str]) -> tmiv::FieldSchema {
    tmiv::FieldSchema {
        name: name.to_string(),
        data_type: tmiv::DataType::ENUM,
        variants: variants
            .iter()
            .map(|name| tmiv::Variant {
                name: name.to_string(),
            })
            .collect(),
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
