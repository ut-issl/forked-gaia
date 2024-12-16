use std::{collections::HashMap, fmt::Display, time::SystemTime};

use gaia_ccsds_c2a::ccsds::tc::clcw::CLCW;
use gaia_tmtc::{cop::CopWorkerStatusPattern, tco_tmiv::{Tmiv, TmivField}};
use prost_types::Timestamp;

use crate::{proto::tmtc_generic_c2a::{TelemetryChannelSchema, TelemetryChannelSchemaMetadata, TelemetryComponentSchema, TelemetryComponentSchemaMetadata, TelemetrySchema, TelemetrySchemaMetadata}, tco_tmiv_util::{field_int, field_schema_bytes, ByteSize}};

pub struct FopStateString {
    state: CopWorkerStatusPattern,
}

impl From<CopWorkerStatusPattern> for FopStateString {
    fn from(state: CopWorkerStatusPattern) -> Self {
        Self { state }
    }
}

impl Display for FopStateString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.state {
            CopWorkerStatusPattern::WorkerClcwUnreceived => write!(f, "CLCW_UNRECEIVED"),
            CopWorkerStatusPattern::WorkerIdle => write!(f, "IDLE"),
            CopWorkerStatusPattern::WorkerInitialize => write!(f, "INITIALIZE"),
            CopWorkerStatusPattern::WorkerActive => write!(f, "ACTIVE"),
            CopWorkerStatusPattern::WorkerAutoRetransmitOff => write!(f, "AUTO_RETRANSMIT_OFF"),
            CopWorkerStatusPattern::WorkerLockout => write!(f, "LOCKOUT"),
            CopWorkerStatusPattern::WorkerUnlocking => write!(f, "UNLOCKING"),
            CopWorkerStatusPattern::WorkerTimeout => write!(f, "TIMEOUT"),
            CopWorkerStatusPattern::WorkerFailed => write!(f, "FAILED"),
            CopWorkerStatusPattern::WorkerCanceled => write!(f, "CANCELED"),
        }
    }
}

const TMIV_DESTINATION_TYPE: &str = "RT";
const TMIV_COMPONENT_NAME: &str = "GAIA";
const CLCW_TELEMETRY_NAME: &str = "TF_CLCW";

fn tmiv_name() -> String {
    format!("{}.{}.{}", TMIV_DESTINATION_TYPE, TMIV_COMPONENT_NAME, CLCW_TELEMETRY_NAME)
}

pub fn build_telemetry_channel_schema_map() -> HashMap<String, TelemetryChannelSchema> {
    vec![(
        TMIV_DESTINATION_TYPE.to_string(),
        TelemetryChannelSchema {
            metadata: Some(TelemetryChannelSchemaMetadata {
                destination_flag_mask: 0,
            }),
        },
    )]
    .into_iter()
    .collect()
}

pub fn build_telemetry_component_schema_map() -> HashMap<String, TelemetryComponentSchema> {
    vec![(
        TMIV_COMPONENT_NAME.to_string(),
        TelemetryComponentSchema {
            metadata: Some(TelemetryComponentSchemaMetadata { apid: 0 }),
            telemetries: vec![(
                CLCW_TELEMETRY_NAME.to_string(),
                TelemetrySchema {
                    metadata: Some(TelemetrySchemaMetadata {
                        id: 0,
                        is_restricted: false,
                    }),
                    fields: vec![
                        field_schema_bytes(
                            "CONTROL_WORD_TYPE",
                            "制御ワードタイプ",
                            ByteSize::Uint8,
                        ),
                        field_schema_bytes("VERSION_NUMBER", "バージョン番号", ByteSize::Uint8),
                        field_schema_bytes("STATUS_FIELD", "ステータスフィールド", ByteSize::Uint8),
                        field_schema_bytes("COP_IN_EFFECT", "COP有効フラグ", ByteSize::Uint8),
                        field_schema_bytes("VCID", "VCID", ByteSize::Uint8),
                        field_schema_bytes("NO_RF_AVAILABLE", "RF利用不可フラグ", ByteSize::Uint8),
                        field_schema_bytes(
                            "NO_BIT_LOCK",
                            "ビットロック不可フラグ",
                            ByteSize::Uint8,
                        ),
                        field_schema_bytes("LOCKOUT", "ロックアウトフラグ", ByteSize::Uint8),
                        field_schema_bytes("WAIT", "ウェイトフラグ", ByteSize::Uint8),
                        field_schema_bytes("RETRANSMIT", "再送信フラグ", ByteSize::Uint8),
                        field_schema_bytes("FARM_B_COUNTER", "FARM-Bカウンタ", ByteSize::Uint8),
                        field_schema_bytes("REPORT_VALUE", "VR値", ByteSize::Uint8),
                    ],
                },
            )]
            .into_iter()
            .collect(),
        },
    )]
    .into_iter()
    .collect()
}

pub fn build_tmiv_fields_from_clcw(fields: &mut Vec<TmivField>, clcw: &CLCW) {
    fields.push(field_int(
        "CONTROL_WORD_TYPE",
        clcw.control_word_type(),
    ));
    fields.push(field_int("VERSION_NUMBER", clcw.clcw_version_number()));
    fields.push(field_int("STATUS_FIELD", clcw.status_field()));
    fields.push(field_int("COP_IN_EFFECT", clcw.cop_in_effect()));
    fields.push(field_int(
        "VCID",
        clcw.virtual_channel_identification(),
    ));
    fields.push(field_int("NO_RF_AVAILABLE", clcw.no_rf_available()));
    fields.push(field_int("NO_BIT_LOCK", clcw.no_bit_lock()));
    fields.push(field_int("LOCKOUT", clcw.lockout()));
    fields.push(field_int("WAIT", clcw.wait()));
    fields.push(field_int("RETRANSMIT", clcw.retransmit()));
    fields.push(field_int("FARM_B_COUNTER", clcw.farm_b_counter()));
    fields.push(field_int("REPORT_VALUE", clcw.report_value()));
}

pub fn build_clcw_tmiv(time: SystemTime, clcw: &CLCW) -> Tmiv {
    let plugin_received_time = time
        .duration_since(std::time::UNIX_EPOCH)
        .expect("incorrect system clock")
        .as_secs();
    let mut fields = vec![];
    build_tmiv_fields_from_clcw(&mut fields, clcw);
    let now = chrono::Utc::now().naive_utc();
    let timestamp = Some(
        Timestamp {
            seconds: now.and_utc().timestamp(),
            nanos: now.and_utc().timestamp_subsec_nanos() as i32,
        }
    );
    Tmiv {
        name: tmiv_name().to_string(),
        plugin_received_time,
        timestamp,
        fields,
    }
}
