use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use cop_recorder_client::CopRecorderClient;
use gaia_stub::{
    cop::{CopCommand, CopQueueStatusSet, CopTaskStatus, CopWorkerStatus},
    recorder::tmtc_recorder_client::TmtcRecorderClient,
    tco_tmiv::{Tco, Tmiv},
};
use prost_types::Timestamp;
use tonic::transport::Channel;
use tracing::error;

use super::Hook;

pub use gaia_stub::recorder::*;

#[derive(Clone)]
pub struct TmtcRecordHook {
    recorder_client: TmtcRecorderClient<Channel>,
}

impl TmtcRecordHook {
    pub fn new(recorder_client: TmtcRecorderClient<Channel>) -> Self {
        Self { recorder_client }
    }
}

#[async_trait]
impl Hook<Arc<Tco>> for TmtcRecordHook {
    type Output = Arc<Tco>;

    async fn hook(&mut self, tco: Arc<Tco>) -> Result<Self::Output> {
        let now = chrono::Utc::now().naive_utc();
        let timestamp = Timestamp {
            seconds: now.and_utc().timestamp(),
            nanos: now.and_utc().timestamp_subsec_nanos() as i32,
        };
        self.recorder_client
            .post_command(PostCommandRequest {
                tco: Some(tco.as_ref().clone()),
                timestamp: Some(timestamp),
            })
            .await?;
        Ok(tco)
    }
}

#[async_trait]
impl Hook<Arc<Tmiv>> for TmtcRecordHook {
    type Output = Arc<Tmiv>;

    async fn hook(&mut self, tmiv: Arc<Tmiv>) -> Result<Self::Output> {
        let ret = self
            .recorder_client
            .post_telemetry(PostTelemetryRequest {
                tmiv: Some(tmiv.as_ref().clone()),
            })
            .await;
        if let Err(e) = ret {
            error!("failed to record TMIV: {}", e);
        }
        Ok(tmiv)
    }
}

#[derive(Clone)]
pub struct CopRecordHook {
    recorder_client: CopRecorderClient<Channel>,
}

impl CopRecordHook {
    pub fn new(recorder_client: CopRecorderClient<Channel>) -> Self {
        Self { recorder_client }
    }
}


#[async_trait]
impl Hook<Arc<CopTaskStatus>> for CopRecordHook {
    type Output = Arc<CopTaskStatus>;

    async fn hook(&mut self, task_status: Arc<CopTaskStatus>) -> Result<Self::Output> {
        let ret = self
            .recorder_client
            .post_cop_task_status(PostCopTaskStatusRequest {
                task_status: Some(task_status.as_ref().clone()),
            })
            .await;
        if let Err(e) = ret {
            error!("failed to record COP status: {}", e);
        }
        Ok(task_status)
    }
}

#[async_trait]
impl Hook<Arc<CopWorkerStatus>> for CopRecordHook {
    type Output = Arc<CopWorkerStatus>;

    async fn hook(&mut self, worker_status: Arc<CopWorkerStatus>) -> Result<Self::Output> {
        let ret = self
            .recorder_client
            .post_cop_worker_status(PostCopWorkerStatusRequest {
                worker_status: Some(worker_status.as_ref().clone()),
            })
            .await;
        if let Err(e) = ret {
            error!("failed to record COP worker status: {}", e);
        }
        Ok(worker_status)
    }
}

#[async_trait]
impl Hook<Arc<CopQueueStatusSet>> for CopRecordHook {
    type Output = Arc<CopQueueStatusSet>;

    async fn hook(&mut self, queue_status: Arc<CopQueueStatusSet>) -> Result<Self::Output> {
        let ret = self
            .recorder_client
            .post_cop_queue_status(PostCopQueueStatusRequest {
                queue_status: Some(queue_status.as_ref().clone()),
            })
            .await;
        if let Err(e) = ret {
            error!("failed to record worker status: {}", e);
        }
        Ok(queue_status)
    }
}

#[async_trait]
impl Hook<Arc<CopCommand>> for CopRecordHook {
    type Output = Arc<CopCommand>;

    async fn hook(&mut self, cop_command: Arc<CopCommand>) -> Result<Self::Output> {
        let ret = self
            .recorder_client
            .post_cop_command(PostCopCommandRequest {
                command: Some(cop_command.as_ref().clone()),
            })
            .await;
        if let Err(e) = ret {
            error!("failed to record COP command: {}", e);
        }
        Ok(cop_command)
    }
}
