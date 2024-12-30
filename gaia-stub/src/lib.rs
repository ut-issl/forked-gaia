pub mod broker {
    tonic::include_proto!("broker");

    pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("broker_descriptor");
}

pub mod recorder {
    tonic::include_proto!("recorder");

    pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("recorder_descriptor");
}

pub mod tco_tmiv {
    tonic::include_proto!("tco_tmiv");

    pub mod tmiv {
        pub fn get_timestamp(tmiv: &super::Tmiv, pseudo_nanos: i32) -> prost_types::Timestamp {
            tmiv.timestamp.clone().unwrap_or(prost_types::Timestamp {
                seconds: tmiv.plugin_received_time as i64,
                nanos: pseudo_nanos,
            })
        }
    }
}

pub mod cop {
    tonic::include_proto!("cop");

    pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("cop_descriptor");
    pub mod status {
        pub fn task_status_to_string(task_status: super::CopTaskStatusPattern) -> String {
            match task_status {
                super::CopTaskStatusPattern::Pending => "PENDING".to_string(),
                super::CopTaskStatusPattern::Accepted => "ACCEPTED".to_string(),
                super::CopTaskStatusPattern::Canceled => "CANCELED".to_string(),
                super::CopTaskStatusPattern::Failed => "FAILED".to_string(),
                super::CopTaskStatusPattern::Executed => "EXECUTED".to_string(),
                super::CopTaskStatusPattern::Lockout => "LOCKOUT".to_string(),
                super::CopTaskStatusPattern::Timeout => "TIMEOUT".to_string(),
                super::CopTaskStatusPattern::Rejected => "REJECTED".to_string(),
            }
        }
        
        pub fn worker_state_to_string(worker_state: super::CopWorkerStatusPattern) -> String {
            match worker_state {
                super::CopWorkerStatusPattern::WorkerClcwUnreceived => "CLCW_UNRECEIVED".to_string(),
                super::CopWorkerStatusPattern::WorkerIdle => "IDLE".to_string(),
                super::CopWorkerStatusPattern::WorkerActive => "ACTIVE".to_string(),
                super::CopWorkerStatusPattern::WorkerAutoRetransmitOff => "AUTO_RETRANSMIT_OFF".to_string(),
                super::CopWorkerStatusPattern::WorkerInitialize => "INITIALIZE".to_string(),
                super::CopWorkerStatusPattern::WorkerCanceled => "CANCELED".to_string(),
                super::CopWorkerStatusPattern::WorkerFailed => "FAILED".to_string(),
                super::CopWorkerStatusPattern::WorkerLockout => "LOCKOUT".to_string(),
                super::CopWorkerStatusPattern::WorkerTimeout => "TIMEOUT".to_string(),
                super::CopWorkerStatusPattern::WorkerUnlocking => "UNLOCKING".to_string(),
            }
        }
    }
}
