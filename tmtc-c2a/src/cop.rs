use std::{fmt::Display, time};

use gaia_ccsds_c2a::ccsds::tc::clcw::CLCW;
use gaia_tmtc::tco_tmiv::{Tmiv, TmivField};

use crate::tco_tmiv_util::{field_enum, field_int};

const TMIV_NAME: &str = "GAIA.COP";

#[derive(Debug, PartialEq, Eq, Clone, Copy, Hash)]
pub enum State {
    Initial,
    Active,
}

impl Default for State {
    fn default() -> Self {
        Self::Initial
    }
}

pub struct Fop1Variables {
    state: State,
    vs: u8,
    last_clcw: CLCW,
}

impl Display for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            State::Initial => write!(f, "INITIAL"),
            State::Active => write!(f, "ACTIVE"),
        }
    }
}

pub fn build_tmiv_fields_from_fop1(fields: &mut Vec<TmivField>, state: State, vs: u8) {
    fields.push(field_enum("FOP1_STATE", state));
    fields.push(field_int("FOP1_VS", vs));
}

pub fn build_tmiv_fields_from_clcw(fields: &mut Vec<TmivField>, clcw: &CLCW) {
    fields.push(field_int(
        "CLCW_CONTROL_WORD_TYPE",
        clcw.control_word_type(),
    ));
    fields.push(field_int("CLCW_VERSION_NUMBER", clcw.clcw_version_number()));
    fields.push(field_int("CLCW_STATUS_FIELD", clcw.status_field()));
    fields.push(field_int("CLCW_COP_IN_EFFECT", clcw.cop_in_effect()));
    fields.push(field_int(
        "CLCW_VCID",
        clcw.virtual_channel_identification(),
    ));
    fields.push(field_int("CLCW_NO_RF_AVAILABLE", clcw.no_rf_available()));
    fields.push(field_int("CLCW_NO_BIT_LOCK", clcw.no_bit_lock()));
    fields.push(field_int("CLCW_LOCKOUT", clcw.lockout()));
    fields.push(field_int("CLCW_WAIT", clcw.wait()));
    fields.push(field_int("CLCW_RETRANSMIT", clcw.retransmit()));
    fields.push(field_int("CLCW_FARM_B_COUNTER", clcw.farm_b_counter()));
    fields.push(field_int("CLCW_REPORT_VALUE", clcw.report_value()));
}

pub fn build_tmiv(time: time::SystemTime, state: State, vs: u8, clcw: &CLCW) -> Tmiv {
    let plugin_received_time = time
        .duration_since(time::UNIX_EPOCH)
        .expect("incorrect system clock")
        .as_secs();
    let mut fields = vec![];
    build_tmiv_fields_from_fop1(&mut fields, state, vs);
    build_tmiv_fields_from_clcw(&mut fields, clcw);
    Tmiv {
        name: TMIV_NAME.to_string(),
        plugin_received_time,
        timestamp: Some(time.into()),
        fields,
    }
}