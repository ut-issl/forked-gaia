use std::{collections::VecDeque, fmt::Display, future::Future, pin::Pin};

use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use gaia_ccsds_c2a::ccsds::tc::{self, clcw::CLCW, sync_and_channel_coding::FrameType};
use gaia_tmtc::cop::{CopQueueStatus, CopQueueStatusSet, CopTaskStatus, CopTaskStatusPattern, CopWorkerStatus, CopWorkerStatusPattern, CopQueueStatusPattern};
use prost_types::Timestamp;
use tokio::sync::broadcast;
use tracing::error;

use crate::satellite::CopTaskId;

use super::{queue::{FopQueue, FopQueueContext}, worker::CommandContext};

fn create_set_vr_body(vr: u8) -> Vec<u8> {
    vec![0b10000010, 0b00000000, vr]
}

fn create_unlock_body() -> Vec<u8> {
    vec![0u8]
}

async fn send_type_bc<T: tc::SyncAndChannelCoding + ?Sized>(
    sync_and_channel_coding: &mut T,
    tc_scid: u16,
    data_field: &[u8],
) -> Result<()> {
    let vcid = 0;
    sync_and_channel_coding
        .transmit(tc_scid, vcid, FrameType::TypeBC, 0, data_field)
        .await?;
    Ok(())
}

pub struct FopStateNodeError {
    message: anyhow::Error,
    state: Box<dyn FopStateNode>,
}

impl FopStateNodeError {
    fn new(message: &str, state: Box<dyn FopStateNode>) -> Self {
        Self {
            message: anyhow::Error::msg(message.to_string()),
            state,
        }
    }
}

trait FopStateNode: Display {
    fn evaluate_timeout(self: Box<Self>, context: FopStateContext) -> Box<dyn FopStateNode>;

    fn clcw_received(self: Box<Self>, context: FopStateContext) -> Box<dyn FopStateNode>;
    fn lockout(self: Box<Self>, context: FopStateContext, flag: bool) -> Box<dyn FopStateNode>;
    fn vsvr_matched(self: Box<Self>, context: FopStateContext, vr: u8) -> Box<dyn FopStateNode>;
    fn accept (&mut self, context: FopStateContext, vr: u8);
    fn reject (&mut self, context: FopStateContext);

    fn terminate(self: Box<Self>, context: FopStateContext) -> Result<Box<dyn FopStateNode>, FopStateNodeError>;
    fn start_unlocking(self: Box<Self>, context: FopStateContext) -> Result<Box<dyn FopStateNode>, FopStateNodeError>;
    fn start_initializing(self: Box<Self>, context: FopStateContext, vsvr: u8) -> Result<Box<dyn FopStateNode>, FopStateNodeError>;
    fn auto_retransmit_enable(self: Box<Self>, context: FopStateContext) -> Result<Box<dyn FopStateNode>, FopStateNodeError>;
    fn auto_retransmit_disable(self: Box<Self>, context: FopStateContext) -> Result<Box<dyn FopStateNode>, FopStateNodeError>;
    fn send_set_vr_command(&mut self, context: FopStateContext, sync_and_channel_coding: Box<dyn tc::SyncAndChannelCoding + Send + Sync>, vr: u8) -> Pin<Box<dyn Future<Output = Result<()>>>>;
    fn send_unlock_command(&self, context: FopStateContext, sync_and_channel_coding: Box<dyn tc::SyncAndChannelCoding + Send + Sync + 'static>) -> Pin<Box<dyn Future<Output = Result<()>>>>;
    fn break_point_confirm(&mut self, context: FopStateContext, cmd_ctx: CommandContext) -> Result<()>;

    fn execute (self: Box<Self>, context: FopStateContext, sync_and_channel_coding: Box<dyn tc::SyncAndChannelCoding + Send + Sync>) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>>;

    fn append (&mut self, context: FopStateContext, cmd_ctx: CommandContext) -> Result<Option<CopTaskId>>;
    fn get_next_id(&self) -> Option<CopTaskId>;
}

#[derive(Clone)]
pub struct FopStateContext {
    worker_state_tx: broadcast::Sender<CopWorkerStatus>,
    queue_status_tx: broadcast::Sender<CopQueueStatusSet>,
    task_status_tx: broadcast::Sender<CopTaskStatus>,
    timeout_sec: u32,
    next_id: CopTaskId,
    tc_scid: u16,
}

impl FopStateContext {
    pub fn send_worker_status(&self, status: CopWorkerStatusPattern) {
        let now = chrono::Utc::now().naive_utc();
        let timestamp = Timestamp {
            seconds: now.and_utc().timestamp(),
            nanos: now.and_utc().timestamp_subsec_nanos() as i32,
        };
        if let Err(e) = self.worker_state_tx.send(CopWorkerStatus {
            state: status.into(),
            timeout_sec: self.timeout_sec.into(),
            timestamp: Some(timestamp),
        }) {
            error!("failed to send FOP state: {}", e);
        }
    }
    pub fn get_queue_context(&self) -> FopQueueContext {
        FopQueueContext {
            queue_status_tx: self.queue_status_tx.clone(),
            task_status_tx: self.task_status_tx.clone(),
        }
    }
}

struct FopStateIdle;

impl Display for FopStateIdle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Idle")
    }
}

impl FopStateNode for FopStateIdle {
    fn evaluate_timeout(self: Box<Self>, _: FopStateContext) -> Box<dyn FopStateNode> {
        self as Box<dyn FopStateNode>
    }  

    fn clcw_received(self: Box<Self>, _: FopStateContext) -> Box<dyn FopStateNode> {
        self as Box<dyn FopStateNode>
    }
    fn lockout(self: Box<Self>, context: FopStateContext, flag: bool) -> Box<dyn FopStateNode> {
        if flag {
            context.send_worker_status(CopWorkerStatusPattern::WorkerLockout);
            Box::new(FopStateLockout) as Box<dyn FopStateNode> 
        } else {
            self as Box<dyn FopStateNode> 
        }
    }
    fn vsvr_matched(self: Box<Self>, _: FopStateContext, _: u8) -> Box<dyn FopStateNode> {
        self as Box<dyn FopStateNode>
    }
    fn accept (&mut self, _: FopStateContext, _: u8) {}
    fn reject (&mut self, _: FopStateContext) {}

    fn terminate(self: Box<Self>, _: FopStateContext) -> Result<Box<dyn FopStateNode>, FopStateNodeError> {
        Err(FopStateNodeError::new(
            "cancel is not allowed in idle state",
            self as Box<dyn FopStateNode>,
        ))
    }
    fn start_unlocking(self: Box<Self>, _: FopStateContext) -> Result<Box<dyn FopStateNode>, FopStateNodeError> {
        Err(FopStateNodeError::new(
            "start_unlocking is not allowed in idle state",
            self as Box<dyn FopStateNode>,
        ))
    }
    fn start_initializing(self: Box<Self>, context: FopStateContext, vsvr: u8) -> Result<Box<dyn FopStateNode>, FopStateNodeError> {
        context.send_worker_status(CopWorkerStatusPattern::WorkerInitialize);
        Ok(Box::new(FopStateInitialize::new(vsvr)) as Box<dyn FopStateNode>)
    }
    fn auto_retransmit_disable(self: Box<Self>, context: FopStateContext) -> Result<Box<dyn FopStateNode>, FopStateNodeError> {
        context.send_worker_status(CopWorkerStatusPattern::WorkerAutoRetransmitOff);
        Ok(Box::new(FopStateAutoRetransmitOff::new()) as Box<dyn FopStateNode>)
    }
    fn auto_retransmit_enable(self: Box<Self>, _: FopStateContext) -> Result<Box<dyn FopStateNode>, FopStateNodeError> {
        Err(FopStateNodeError::new(
            "auto_retransmit_enable is not allowed in idle state",
            self as Box<dyn FopStateNode>,
        ))
    }
    fn send_set_vr_command(&mut self, _: FopStateContext, _: Box<dyn tc::SyncAndChannelCoding + Send + Sync >, _: u8) -> Pin<Box<dyn Future<Output = Result<()>>>> {
        Box::pin(async { Err(anyhow!("send_set_vr_command is not allowed in idle state")) })
    }
    fn send_unlock_command(&self, _: FopStateContext, _: Box<dyn tc::SyncAndChannelCoding + Send + Sync >) -> Pin<Box<dyn Future<Output = Result<()>>>> {
        Box::pin(async { Err(anyhow!("send_unlock_command is not allowed in idle state")) })
    }
    fn break_point_confirm(&mut self, _: FopStateContext, _: CommandContext) -> Result<()> {
        Err(anyhow!("break_point_confirm is not allowed in idle state"))
    }

    fn append (&mut self, _: FopStateContext, _: CommandContext) -> Result<Option<CopTaskId>> {
        Err(anyhow!("append is not allowed in idle state")) 
    }
    fn execute (self: Box<Self>, _: FopStateContext, _: Box<dyn tc::SyncAndChannelCoding + Send + Sync>) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async { self as Box<dyn FopStateNode> })
    }
    fn get_next_id(&self) -> Option<CopTaskId> {
        None
    }
}

struct FopStateLockout;

impl Display for FopStateLockout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Lockout")
    }
}

impl FopStateNode for FopStateLockout {
    fn evaluate_timeout(self: Box<Self>, _: FopStateContext) -> Box<dyn FopStateNode> {
        self as Box<dyn FopStateNode> 
    }

    fn clcw_received(self: Box<Self>, _: FopStateContext) -> Box<dyn FopStateNode> {
        self as Box<dyn FopStateNode>
    }
    fn lockout(self: Box<Self>, context: FopStateContext, flag: bool) -> Box<dyn FopStateNode> {
        if flag {
            self as Box<dyn FopStateNode>
        } else {
            context.send_worker_status(CopWorkerStatusPattern::WorkerIdle);
            Box::new(FopStateIdle) as Box<dyn FopStateNode> 
        }
    }
    fn vsvr_matched(self: Box<Self>, _: FopStateContext, _: u8) -> Box<dyn FopStateNode> {
        self as Box<dyn FopStateNode>
    }
    fn accept (&mut self, _: FopStateContext, _: u8) {}
    fn reject (&mut self, _: FopStateContext) {}
    
    fn terminate(self: Box<Self>, _: FopStateContext) -> Result<Box<dyn FopStateNode>, FopStateNodeError> {
        Err(FopStateNodeError::new(
            "cancel is not allowed in lockout state",
            self as Box<dyn FopStateNode>,
        ))
    }
    fn start_unlocking(self: Box<Self>, context: FopStateContext) -> Result<Box<dyn FopStateNode>, FopStateNodeError> {
        context.send_worker_status(CopWorkerStatusPattern::WorkerUnlocking);
        Ok(Box::new(FopStateUnlocking::new()) as Box<dyn FopStateNode>)
    }
    fn start_initializing(self: Box<Self>, _: FopStateContext, _: u8) -> Result<Box<dyn FopStateNode>, FopStateNodeError> {
        Err(FopStateNodeError::new(
            "start_initializing is not allowed in lockout state",
            self as Box<dyn FopStateNode>,
        ))
    }
    fn auto_retransmit_disable(self: Box<Self>, context: FopStateContext) -> Result<Box<dyn FopStateNode>, FopStateNodeError> {
        context.send_worker_status(CopWorkerStatusPattern::WorkerAutoRetransmitOff);
        Ok(Box::new(FopStateAutoRetransmitOff::new()) as Box<dyn FopStateNode>)
    }
    fn auto_retransmit_enable(self: Box<Self>, _: FopStateContext) -> Result<Box<dyn FopStateNode>, FopStateNodeError> {
        Err(FopStateNodeError::new(
            "auto_retransmit_enable is not allowed in lockout state",
            self as Box<dyn FopStateNode>,
        ))
    }
    fn send_set_vr_command(&mut self, _: FopStateContext, _: Box<dyn tc::SyncAndChannelCoding + Send + Sync >, _: u8) -> Pin<Box<dyn Future<Output = Result<()>>>> {
        Box::pin(async { Err(anyhow!("send_set_vr_command is not allowed in lockout state")) })
    }
    fn send_unlock_command(&self, _: FopStateContext, _: Box<dyn tc::SyncAndChannelCoding + Send + Sync >) -> Pin<Box<dyn Future<Output = Result<()>>>> {
        Box::pin(async { Err(anyhow!("send_unlock_command is not allowed in lockout state")) })
    }
    fn break_point_confirm(&mut self, _: FopStateContext, _: CommandContext) -> Result<()> {
        Err(anyhow!("break_point_confirm is not allowed in lockout state"))
    }

    fn append (&mut self, _: FopStateContext, _: CommandContext) -> Result<Option<CopTaskId>> {
        Err(anyhow!("append is not allowed in lockout state"))
    }
    fn execute (self: Box<Self>, _: FopStateContext, _: Box<dyn tc::SyncAndChannelCoding + Send + Sync>) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async { self as Box<dyn FopStateNode> })
    }
    fn get_next_id(&self) -> Option<CopTaskId> {
        None
    }
}

struct FopStateUnlocking{
    start_time: DateTime<Utc>,
}

impl FopStateUnlocking {
    fn new() -> Self {
        Self {
            start_time: chrono::Utc::now(),
        }
    }
}

impl Display for FopStateUnlocking {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Unlocking")
    }
}

impl FopStateNode for FopStateUnlocking {
    fn evaluate_timeout(self: Box<Self>, context: FopStateContext) -> Box<dyn FopStateNode> {
        let now = chrono::Utc::now();
        if now - self.start_time > chrono::TimeDelta::seconds(context.timeout_sec as i64) {
            tokio::spawn(async move { 
                context.send_worker_status(CopWorkerStatusPattern::WorkerTimeout);
                tokio::time::sleep(tokio::time::Duration::from_nanos(1)).await;
                context.send_worker_status(CopWorkerStatusPattern::WorkerLockout);
            });
            Box::new(FopStateLockout) as Box<dyn FopStateNode> 
        } else {
            self as Box<dyn FopStateNode>
        }
    }

    fn clcw_received(self: Box<Self>, _: FopStateContext) -> Box<dyn FopStateNode> {
        self as Box<dyn FopStateNode>
    }
    fn lockout(self: Box<Self>, context: FopStateContext, flag: bool) -> Box<dyn FopStateNode> {
        if flag {
            self as Box<dyn FopStateNode>
        } else {
            context.send_worker_status(CopWorkerStatusPattern::WorkerIdle);
            Box::new(FopStateIdle) as Box<dyn FopStateNode>
        }
    }
    fn vsvr_matched(self: Box<Self>, _: FopStateContext, _: u8) -> Box<dyn FopStateNode> {
        self as Box<dyn FopStateNode>
    }
    fn accept (&mut self, _: FopStateContext, _: u8) {}
    fn reject (&mut self, _: FopStateContext) {}

    fn terminate(self: Box<Self>, context: FopStateContext) -> Result<Box<dyn FopStateNode>, FopStateNodeError> {
        tokio::spawn(async move {
            context.send_worker_status(CopWorkerStatusPattern::WorkerCanceled);
            tokio::time::sleep(tokio::time::Duration::from_nanos(1)).await;
            context.send_worker_status(CopWorkerStatusPattern::WorkerLockout);
        });
        Ok(Box::new(FopStateLockout) as Box<dyn FopStateNode>) 
    }
    fn start_unlocking(self: Box<Self>, _: FopStateContext) -> Result<Box<dyn FopStateNode>, FopStateNodeError> {
        Err(FopStateNodeError::new(
            "start_unlocking is not allowed in unlocking state",
            self as Box<dyn FopStateNode>,
        ))
    }
    fn start_initializing(self: Box<Self>, _: FopStateContext, _: u8) -> Result<Box<dyn FopStateNode>, FopStateNodeError> {
        Err(FopStateNodeError::new(
            "start_initializing is not allowed in unlocking state",
            self as Box<dyn FopStateNode>,
        ))
    }
    fn auto_retransmit_disable(self: Box<Self>, context: FopStateContext) -> Result<Box<dyn FopStateNode>, FopStateNodeError> {
        context.send_worker_status(CopWorkerStatusPattern::WorkerAutoRetransmitOff);
        Ok(Box::new(FopStateAutoRetransmitOff::new()) as Box<dyn FopStateNode>)
    }
    fn auto_retransmit_enable(self: Box<Self>, _: FopStateContext) -> Result<Box<dyn FopStateNode>, FopStateNodeError> {
        Err(FopStateNodeError::new(
            "auto_retransmit_enable is not allowed in unlocking state",
            self as Box<dyn FopStateNode>,
        ))
    }
    fn send_set_vr_command(&mut self, _: FopStateContext, _: Box<dyn tc::SyncAndChannelCoding + Send + Sync >, _: u8) -> Pin<Box<dyn Future<Output = Result<()>>>> {
        Box::pin(async { Err(anyhow!("send_set_vr_command is not allowed in unlocking state")) })
    }
    fn send_unlock_command(&self, _: FopStateContext, _: Box<dyn tc::SyncAndChannelCoding + Send + Sync >) -> Pin<Box<dyn Future<Output = Result<()>>>> {
        Box::pin(async { Err(anyhow!("send_unlock_command is not allowed in unlocking state")) })
    }
    fn break_point_confirm(&mut self, _: FopStateContext, _: CommandContext) -> Result<()> {
        Err(anyhow!("break_point_confirm is not allowed in unlocking state"))
    }

    fn append (&mut self, _: FopStateContext, _: CommandContext) -> Result<Option<CopTaskId>> {
       Err(anyhow!("append is not allowed in unlocking state"))
    }

    fn execute(self: Box<Self>, context: FopStateContext, mut sync_and_channel_coding: Box<dyn tc::SyncAndChannelCoding + Send + Sync + 'static>) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async move {
            match send_type_bc(
                sync_and_channel_coding.as_mut(),
                context.tc_scid,
                &create_unlock_body(),
            ).await {
                Ok(_) => self as Box<dyn FopStateNode>,
                Err(e) => {
                    error!("failed to send unlock command: {}", e);
                    context.send_worker_status(CopWorkerStatusPattern::WorkerFailed);
                    tokio::time::sleep(tokio::time::Duration::from_nanos(1)).await;
                    context.send_worker_status(CopWorkerStatusPattern::WorkerLockout);
                    Box::new(FopStateLockout) as Box<dyn FopStateNode>
                }
            } 
        })
    }
    fn get_next_id(&self) -> Option<CopTaskId> {
        None
    }
}

struct FopStateClcwUnreceived;

impl Display for FopStateClcwUnreceived {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CLCW Unreceived")
    }
}

impl FopStateNode for FopStateClcwUnreceived {
    fn evaluate_timeout(self: Box<Self>, _: FopStateContext) -> Box<dyn FopStateNode> {
        self as Box<dyn FopStateNode>
    }

    fn clcw_received(self: Box<Self>, context: FopStateContext) -> Box<dyn FopStateNode> {
        context.send_worker_status(CopWorkerStatusPattern::WorkerIdle);
        Box::new(FopStateIdle) as Box<dyn FopStateNode>
    }
    fn lockout(self: Box<Self>, context: FopStateContext, flag: bool) -> Box<dyn FopStateNode> {
        if flag {
            context.send_worker_status(CopWorkerStatusPattern::WorkerLockout);
            Box::new(FopStateLockout) as Box<dyn FopStateNode>
        } else {
            context.send_worker_status(CopWorkerStatusPattern::WorkerIdle);
            Box::new(FopStateIdle) as Box<dyn FopStateNode>
        }
    }
    fn vsvr_matched(self: Box<Self>, context: FopStateContext, _: u8) -> Box<dyn FopStateNode> {
        context.send_worker_status(CopWorkerStatusPattern::WorkerIdle);
        Box::new(FopStateIdle) as Box<dyn FopStateNode>
    }
    fn accept (&mut self, _: FopStateContext, _: u8) {}
    fn reject (&mut self, _: FopStateContext) {}

    fn terminate(self: Box<Self>, _: FopStateContext) -> Result<Box<dyn FopStateNode>, FopStateNodeError> {
        Err(FopStateNodeError::new(
            "cancel is not allowed in clcw_unreceived state",
            self as Box<dyn FopStateNode>,
        ))
    }
    fn start_unlocking(self: Box<Self>, _: FopStateContext) -> Result<Box<dyn FopStateNode>, FopStateNodeError> {
        Err(FopStateNodeError::new(
            "start_unlocking is not allowed in clcw_unreceived state",
            self as Box<dyn FopStateNode>,
        ))
    }
    fn start_initializing(self: Box<Self>, _: FopStateContext, _: u8) -> Result<Box<dyn FopStateNode>, FopStateNodeError> {
        Err(FopStateNodeError::new(
            "start_initializing is not allowed in clcw_unreceived state",
            self as Box<dyn FopStateNode>,
        ))
    }
    fn auto_retransmit_disable(self: Box<Self>, context: FopStateContext) -> Result<Box<dyn FopStateNode>, FopStateNodeError> {
        context.send_worker_status(CopWorkerStatusPattern::WorkerAutoRetransmitOff);
        Ok(Box::new(FopStateAutoRetransmitOff::new()) as Box<dyn FopStateNode>)
    }
    fn auto_retransmit_enable(self: Box<Self>, _: FopStateContext) -> Result<Box<dyn FopStateNode>, FopStateNodeError> {
        Err(FopStateNodeError::new(
            "auto_retransmit_enable is not allowed in clcw_unreceived state",
            self as Box<dyn FopStateNode>,
        ))
    }
    fn send_set_vr_command(&mut self, _: FopStateContext, _: Box<dyn tc::SyncAndChannelCoding + Send + Sync >, _: u8) -> Pin<Box<dyn Future<Output = Result<()>>>> {
        Box::pin(async { Err(anyhow!("send_set_vr_command is not allowed in clcw_unreceived state")) })
    }
    fn send_unlock_command(&self, _: FopStateContext, _: Box<dyn tc::SyncAndChannelCoding + Send + Sync >) -> Pin<Box<dyn Future<Output = Result<()>>>> {
        Box::pin(async { Err(anyhow!("send_unlock_command is not allowed in clcw_unreceived state")) })
    }
    fn break_point_confirm(&mut self, _: FopStateContext, _: CommandContext) -> Result<()> {
        Err(anyhow!("break_point_confirm is not allowed in clcw_unreceived state"))
    }

    fn append (&mut self, _: FopStateContext, _: CommandContext) -> Result<Option<CopTaskId>> {
        Err(anyhow!("append is not allowed in clcw_unreceived state"))
    }

    fn execute (self: Box<Self>, _: FopStateContext, _: Box<dyn tc::SyncAndChannelCoding + Send + Sync>) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async { self as Box<dyn FopStateNode> })
    }
    fn get_next_id(&self) -> Option<CopTaskId> {
        None
    }
}

struct FopStateInitialize {
    start_time: DateTime<Utc>,
    vsvr: u8
}

impl FopStateInitialize {
    fn new(vsvr: u8) -> Self {
        Self {
            start_time: chrono::Utc::now(),
            vsvr
        }
    }
}

impl Display for FopStateInitialize {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Initialize")
    }
}

impl FopStateNode for FopStateInitialize {
    fn evaluate_timeout(self: Box<Self>, context: FopStateContext) -> Box<dyn FopStateNode> {
        let now = chrono::Utc::now();
        if now - self.start_time > chrono::TimeDelta::seconds(context.timeout_sec as i64) {
            context.send_worker_status(CopWorkerStatusPattern::WorkerTimeout);
            Box::new(FopStateIdle) as Box<dyn FopStateNode>
        } else {
            self as Box<dyn FopStateNode>
        }
    }

    fn clcw_received(self: Box<Self>, _: FopStateContext) -> Box<dyn FopStateNode> {
        self as Box<dyn FopStateNode>
    }
    fn lockout(self: Box<Self>, context: FopStateContext, flag: bool) -> Box<dyn FopStateNode> {
        if flag {
            context.send_worker_status(CopWorkerStatusPattern::WorkerLockout);
            Box::new(FopStateLockout) as Box<dyn FopStateNode>
        } else {
            self as Box<dyn FopStateNode>
        }
    }
    fn vsvr_matched(self: Box<Self>, context: FopStateContext, vr: u8) -> Box<dyn FopStateNode> {
        if vr == self.vsvr {
            context.send_worker_status(CopWorkerStatusPattern::WorkerActive);
            Box::new(FopStateActive::new(self.vsvr, context.next_id)) as Box<dyn FopStateNode>
        } else {
            self as Box<dyn FopStateNode>
        }
    }
    fn accept (&mut self, _: FopStateContext, _: u8) {}
    fn reject (&mut self, _: FopStateContext) {}

    fn terminate(self: Box<Self>, context: FopStateContext) -> Result<Box<dyn FopStateNode>, FopStateNodeError> {
        context.send_worker_status(CopWorkerStatusPattern::WorkerCanceled);
        Ok(Box::new(FopStateIdle) as Box<dyn FopStateNode>)
    }
    fn start_unlocking(self: Box<Self>, _: FopStateContext) -> Result<Box<dyn FopStateNode>, FopStateNodeError> {
        Err(FopStateNodeError::new(
            "start_unlocking is not allowed in initialize state",
            self as Box<dyn FopStateNode>,
        ))
    }
    fn start_initializing(self: Box<Self>, _: FopStateContext, _: u8) -> Result<Box<dyn FopStateNode>, FopStateNodeError> {
        Err(FopStateNodeError::new(
            "start_initializing is not allowed in initialize state",
            self as Box<dyn FopStateNode>,
        ))
    }
    fn auto_retransmit_enable(self: Box<Self>, _: FopStateContext) -> Result<Box<dyn FopStateNode>, FopStateNodeError> {
        Err(FopStateNodeError::new(
            "auto_retransmit_enable is not allowed in initialize state",
            self as Box<dyn FopStateNode>,
        ))
    }
    fn auto_retransmit_disable(self: Box<Self>, context: FopStateContext) -> Result<Box<dyn FopStateNode>, FopStateNodeError> {
        context.send_worker_status(CopWorkerStatusPattern::WorkerAutoRetransmitOff);
        Ok(Box::new(FopStateAutoRetransmitOff::new()) as Box<dyn FopStateNode>)
    }
    fn send_set_vr_command(&mut self, _: FopStateContext, _: Box<dyn tc::SyncAndChannelCoding + Send + Sync >,  _: u8) -> Pin<Box<dyn Future<Output = Result<()>>>> {
        Box::pin(async { Err(anyhow!("send_set_vr_command is not allowed in initialize state")) })
    }
    fn send_unlock_command(&self, _: FopStateContext, _: Box<dyn tc::SyncAndChannelCoding + Send + Sync >) -> Pin<Box<dyn Future<Output = Result<()>>>> {
        Box::pin(async { Err(anyhow!("send_unlock_command is not allowed in initialize state")) })
    }
    fn break_point_confirm(&mut self, _: FopStateContext, _: CommandContext) -> Result<()> {
        Err(anyhow!("break_point_confirm is not allowed in initialize state"))
    }

    fn append (&mut self, _: FopStateContext, _: CommandContext) -> Result<Option<CopTaskId>> {
        Err(anyhow!("append is not allowed in initialize state"))
    }

    fn execute (self: Box<Self>, context: FopStateContext, mut sync_and_channel_coding: Box<dyn tc::SyncAndChannelCoding + Send + Sync>) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async move {
            match send_type_bc(
                sync_and_channel_coding.as_mut(),
                context.tc_scid,
                &create_set_vr_body(self.vsvr),
            ).await {
                Ok(_) => {
                    self as Box<dyn FopStateNode>
                }
                Err(e) => {
                    error!("failed to send set_vr command: {}", e);
                    context.send_worker_status(CopWorkerStatusPattern::WorkerFailed);
                    Box::new(FopStateIdle) as Box<dyn FopStateNode>
                }
            }
        })
    }
    fn get_next_id(&self) -> Option<CopTaskId> {
        None
    }
}

struct FopStateActive{
    queue: FopQueue,
}

impl FopStateActive {
    fn new(vs: u8, next_id: CopTaskId) -> Self {
        Self {
            queue: FopQueue::new(vs, next_id),
        }
    }
}

impl Display for FopStateActive {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Active")
    }
}

impl FopStateNode for FopStateActive {
    fn evaluate_timeout(self: Box<Self>, context: FopStateContext) -> Box<dyn FopStateNode> {
        let now = chrono::Utc::now();
        let oldest_arrival_time = match self.queue.get_oldest_arrival_time() {
            Some(time) => time,
            None => return self as Box<dyn FopStateNode>,
        };
        if now - oldest_arrival_time > chrono::TimeDelta::seconds(context.timeout_sec as i64) { 
            context.send_worker_status(CopWorkerStatusPattern::WorkerTimeout);
            Box::new(FopStateIdle) as Box<dyn FopStateNode> 
        } else {
            self as Box<dyn FopStateNode>
        }
    }

    fn clcw_received(self: Box<Self>, _: FopStateContext) -> Box<dyn FopStateNode> {
        self as Box<dyn FopStateNode>
    }
    fn lockout(mut self: Box<Self>, context: FopStateContext, flag: bool) -> Box<dyn FopStateNode> {
        if flag {
            self.queue.clear(context.get_queue_context(), CopTaskStatusPattern::Lockout);
            context.send_worker_status(CopWorkerStatusPattern::WorkerLockout);
            Box::new(FopStateLockout) as Box<dyn FopStateNode>
        } else {
            self as Box<dyn FopStateNode>
        }
    }
    fn vsvr_matched(self: Box<Self>, _: FopStateContext, _: u8) -> Box<dyn FopStateNode> {
        self as Box<dyn FopStateNode>
    }
    fn accept (&mut self, context: FopStateContext, vr: u8) {
        self.queue.accept(context.get_queue_context(), vr);
    }
    fn reject (&mut self, context: FopStateContext) {
        self.queue.reject(context.get_queue_context());
    }

    fn terminate(mut self: Box<Self>, context: FopStateContext) -> Result<Box<dyn FopStateNode>, FopStateNodeError> {
        self.queue.clear(context.get_queue_context(), CopTaskStatusPattern::Canceled);
        context.send_worker_status(CopWorkerStatusPattern::WorkerCanceled);
        Ok(Box::new(FopStateIdle) as Box<dyn FopStateNode>)
    }
    fn start_unlocking(self: Box<Self>, _: FopStateContext) -> Result<Box<dyn FopStateNode>, FopStateNodeError> {
        Err(FopStateNodeError::new(
            "start_unlocking is not allowed in active state",
            self as Box<dyn FopStateNode>,
        ))
    }
    fn start_initializing(mut self: Box<Self>, context: FopStateContext, vsvr: u8) -> Result<Box<dyn FopStateNode>, FopStateNodeError> {
        self.queue.clear(context.get_queue_context(), CopTaskStatusPattern::Canceled);
        tokio::spawn(async move {
            context.send_worker_status(CopWorkerStatusPattern::WorkerCanceled);
            tokio::time::sleep(tokio::time::Duration::from_nanos(1)).await;
            context.send_worker_status(CopWorkerStatusPattern::WorkerInitialize);
        });
        Ok(Box::new(FopStateInitialize::new(vsvr)) as Box<dyn FopStateNode>)
    }
    fn auto_retransmit_enable(self: Box<Self>, _: FopStateContext) -> Result<Box<dyn FopStateNode>, FopStateNodeError> {
        Err(FopStateNodeError::new(
            "auto_retransmit_enable is not allowed in active state",
            self as Box<dyn FopStateNode>,
        ))
    }
    fn auto_retransmit_disable(mut self: Box<Self>, context: FopStateContext) -> Result<Box<dyn FopStateNode>, FopStateNodeError> {
        self.queue.clear(context.get_queue_context(), CopTaskStatusPattern::Canceled);
        context.send_worker_status(CopWorkerStatusPattern::WorkerAutoRetransmitOff);
        Ok(Box::new(FopStateAutoRetransmitOff::new()) as Box<dyn FopStateNode>)
    }
    fn send_set_vr_command(&mut self, _: FopStateContext, _: Box<dyn tc::SyncAndChannelCoding + Send + Sync>, _: u8) -> Pin<Box<dyn Future<Output = Result<()>>>> {
        Box::pin(async { Err(anyhow!("send_set_vr_command is not allowed in active state")) })
    }
    fn send_unlock_command(&self, _: FopStateContext, _: Box<dyn tc::SyncAndChannelCoding + Send + Sync>) -> Pin<Box<dyn Future<Output = Result<()>>>> {
        Box::pin(async { Err(anyhow!("send_unlock_command is not allowed in active state")) })
    }
    fn break_point_confirm(&mut self, context: FopStateContext, cmd_ctx: CommandContext) -> Result<()> {
        self.queue.confirm(context.get_queue_context(), cmd_ctx);
        Ok(())
    }

    fn append (&mut self, context: FopStateContext, cmd_ctx: CommandContext) -> Result<Option<CopTaskId>> {
        let queue_ctx = context.get_queue_context();
        Ok(Some(self.queue.push(queue_ctx, cmd_ctx)))
    }

    fn execute (mut self: Box<Self>, context: FopStateContext, mut sync_and_channel_coding: Box<dyn tc::SyncAndChannelCoding + Send + Sync>) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async move {
            let (vs, ctx) = match self.queue.execute(context.get_queue_context()) {
                Some((vs, ctx)) => (vs, ctx),
                None => return self as Box<dyn FopStateNode>,
            };
            match ctx.transmit_to(sync_and_channel_coding.as_mut(), Some(vs)).await {
                Ok(_) => self as Box<dyn FopStateNode>,
                Err(e) => {
                    error!("failed to transmit COP: {}", e);
                    context.send_worker_status(CopWorkerStatusPattern::WorkerFailed);
                    Box::new(FopStateIdle) as Box<dyn FopStateNode>
                }
            }
        })
    }
    fn get_next_id(&self) -> Option<CopTaskId> {
        Some(self.queue.next_id())
    }
}

pub struct FopStateAutoRetransmitOff {
    next_vs: u8,
    queue: VecDeque<(u8, CommandContext)>
}

impl FopStateAutoRetransmitOff {
    fn new() -> Self {
        Self {
            next_vs: 0,
            queue: VecDeque::new(),
        }
    }
}

impl Display for FopStateAutoRetransmitOff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Auto Retransmit Off")
    }
}

impl FopStateNode for FopStateAutoRetransmitOff {
    fn evaluate_timeout(self: Box<Self>, _: FopStateContext) -> Box<dyn FopStateNode> {
        self as Box<dyn FopStateNode>
    }

    fn clcw_received(self: Box<Self>, _: FopStateContext) -> Box<dyn FopStateNode> {
        self as Box<dyn FopStateNode>
    }
    fn lockout(self: Box<Self>, _: FopStateContext, _: bool) -> Box<dyn FopStateNode> {
        self as Box<dyn FopStateNode>
    }
    fn vsvr_matched(self: Box<Self>, _: FopStateContext, _: u8) -> Box<dyn FopStateNode> {
        self as Box<dyn FopStateNode>
    }
    fn accept (&mut self, _: FopStateContext, _: u8) {}
    fn reject (&mut self, _: FopStateContext) {}

    fn terminate(self: Box<Self>, _: FopStateContext) -> Result<Box<dyn FopStateNode>, FopStateNodeError> {
        Err(FopStateNodeError::new(
            "cancel is not allowed in auto_retransmit_off state",
            self as Box<dyn FopStateNode>,
        ))
    }
    fn start_unlocking(self: Box<Self>, _: FopStateContext) -> Result<Box<dyn FopStateNode>, FopStateNodeError> {
        Err(FopStateNodeError::new(
            "start_unlocking is not allowed in auto_retransmit_off state",
            self as Box<dyn FopStateNode>,
        ))
    }
    fn start_initializing(self: Box<Self>, _: FopStateContext, _: u8) -> Result<Box<dyn FopStateNode>, FopStateNodeError> {
        Err(FopStateNodeError::new(
            "start_initializing is not allowed in auto_retransmit_off state",
            self as Box<dyn FopStateNode>,
        ))
    }
    fn auto_retransmit_enable(self: Box<Self>, context: FopStateContext) -> Result<Box<dyn FopStateNode>, FopStateNodeError>{
        context.send_worker_status(CopWorkerStatusPattern::WorkerClcwUnreceived);
        Ok(Box::new(FopStateClcwUnreceived) as Box<dyn FopStateNode>)
    }
    fn auto_retransmit_disable(self: Box<Self>, _: FopStateContext) -> Result<Box<dyn FopStateNode>, FopStateNodeError> {
        Err(FopStateNodeError::new(
            "auto_retransmit_disable is not allowed in auto_retransmit_off state",
            self as Box<dyn FopStateNode>,
        ))
    }
    fn send_set_vr_command(&mut self, context: FopStateContext, mut sync_and_channel_coding: Box<dyn tc::SyncAndChannelCoding + Send + Sync>, vsvr: u8) -> Pin<Box<dyn Future<Output = Result<()>>>> {
        self.next_vs = vsvr;
        self.queue.drain(..);
        Box::pin(async move { 
            if let Err(e) = send_type_bc(
                sync_and_channel_coding.as_mut(),
                context.tc_scid,
                &create_set_vr_body(vsvr),
            ).await {
                error!("failed to send set_vr command: {}", e);
                Err(e)
            } else {
                Ok(())
            }
        })
    }
    fn send_unlock_command(&self, context: FopStateContext, mut sync_and_channel_coding: Box<dyn tc::SyncAndChannelCoding + Send + Sync>) -> Pin<Box<dyn Future<Output = Result<()>>>> {
        Box::pin(async move { 
            if let Err(e) = send_type_bc(
                sync_and_channel_coding.as_mut(),
                context.tc_scid,
                &create_unlock_body(),
            ).await {
                error!("failed to send unlock command: {}", e);
                Err(e)
            } else {
                Ok(())
            }
        })
    }
    fn break_point_confirm(&mut self, _: FopStateContext, _: CommandContext) -> Result<()> {
        Err(anyhow!("break_point_confirm is not allowed in auto_retransmit_off state"))
    }

    fn append (&mut self, context: FopStateContext, cmd_ctx: CommandContext) -> Result<Option<CopTaskId>> {
        self.queue.push_back((self.next_vs, cmd_ctx));
        let vs = self.next_vs;
        self.next_vs = self.next_vs.wrapping_add(1);
        let now = chrono::Utc::now().naive_utc();
        let timestamp = Some(Timestamp {
            seconds: now.and_utc().timestamp(),
            nanos: now.and_utc().timestamp_subsec_nanos() as i32,
        });
        context.queue_status_tx.send(
            CopQueueStatusSet {
                pending: Some(CopQueueStatus { head_id: None, head_tco_name: None, task_count: 0 }),
                executed: Some(CopQueueStatus { head_id: None, head_tco_name: None, task_count: 0 }),
                rejected: Some(CopQueueStatus { head_id: None, head_tco_name: None, task_count: 0 }),
                head_vs: vs as u32,
                vs_at_id0: 0,
                oldest_arrival_time: None,
                timestamp,
                status: CopQueueStatusPattern::Processing.into(),
            }
        )?;
        Ok(None)
    }
    fn execute (mut self: Box<Self>, context: FopStateContext, mut sync_and_channel_coding: Box<dyn tc::SyncAndChannelCoding + Send + Sync + 'static>) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async move {
            let (vs, ctx) = match self.queue.pop_front() {
                Some((vs, ctx)) => (vs, ctx),
                None => return self as Box<dyn FopStateNode>,
            };
            match ctx.transmit_to(sync_and_channel_coding.as_mut(), Some(vs)).await {
                Ok(_) => self as Box<dyn FopStateNode>,
                Err(e) => {
                    error!("failed to transmit COP: {}", e);
                    context.send_worker_status(CopWorkerStatusPattern::WorkerFailed);
                    Box::new(FopStateIdle) as Box<dyn FopStateNode>
                }
            }
        })
    }
    fn get_next_id(&self) -> Option<CopTaskId> {
        None
    }
}

pub struct FopStateMachine<T> 
where 
    T: tc::SyncAndChannelCoding + Clone + Send + Sync + 'static,
{
    inner: Option<Box<dyn FopStateNode>>,
    timeout_sec: u32,
    tc_scid: u16,
    next_id: CopTaskId,
    worker_state_tx: broadcast::Sender<CopWorkerStatus>,
    queue_status_tx: broadcast::Sender<CopQueueStatusSet>,
    task_status_tx: broadcast::Sender<CopTaskStatus>,
    sync_and_channel_coding: T,
}

impl<T> FopStateMachine<T> 
where 
    T: tc::SyncAndChannelCoding + Clone + Send + Sync + 'static,
{
    pub fn new(
        worker_state_tx: broadcast::Sender<CopWorkerStatus>,
        queue_status_tx: broadcast::Sender<CopQueueStatusSet>,
        task_status_tx: broadcast::Sender<CopTaskStatus>,
        sync_and_channel_coding: T,
        tc_scid: u16,
    ) -> Self {
        Self {
            inner: Some(Box::new(FopStateClcwUnreceived) as Box<dyn FopStateNode>),
            worker_state_tx,
            queue_status_tx,
            task_status_tx,
            timeout_sec: 20,
            sync_and_channel_coding,
            tc_scid,
            next_id: 0,
        }
    }
    fn get_context(&self) -> FopStateContext {
        FopStateContext {
            worker_state_tx: self.worker_state_tx.clone(),
            queue_status_tx: self.queue_status_tx.clone(),
            task_status_tx: self.task_status_tx.clone(),
            timeout_sec: self.timeout_sec,
            next_id: self.next_id,
            tc_scid: self.tc_scid,
        }
    }

    pub fn evaluate_timeout(&mut self) {
        let context = self.get_context();
        self.inner = match self.inner.take(){
            Some(state) => Some(state.evaluate_timeout(context)),
            None => unreachable!(),
        };
    }
    pub fn clcw_received(&mut self) {
        let context = self.get_context();
        self.inner = match self.inner.take(){
            Some(state) => Some(state.clcw_received(context)),
            None => unreachable!(),
        };
    }
    pub fn set_clcw(&mut self, clcw: &CLCW) {
        let context = self.get_context();
        self.inner = match self.inner.take(){
            Some(state) => Some(state.vsvr_matched(context.clone(), clcw.report_value())),
            None => unreachable!(),
        };
        if clcw.retransmit() == 1 {
            self.inner = match self.inner.take(){
                Some(mut state) => {
                    state.reject(context.clone());
                    Some(state)
                },
                None => unreachable!(),
            };
        }
        self.inner = match self.inner.take(){
            Some(mut state) => {
                state.accept(context.clone(), clcw.report_value());
                Some(state)
            },
            None => unreachable!(),
        };
        self.inner = match self.inner.take(){
            Some(state) => Some(state.lockout(context.clone(), clcw.lockout() == 1)),
            None => unreachable!(),
        };
    }
    pub fn cancel(&mut self) -> Result<()> {
        let context = self.get_context();
        let res = match self.inner.take(){
            Some(state) => state.terminate(context),
            None => unreachable!(),
        };
        match res {
            Ok(state) => {
                self.inner = Some(state);
                Ok(())
            },
            Err(e) => {
                self.inner = Some(e.state);
                Err(e.message)
            },
        }
    }
    pub fn start_unlocking(&mut self) -> Result<()> {
        let context = self.get_context();
        let res = match self.inner.take(){
            Some(state) => state.start_unlocking(context),
            None => unreachable!(),
        };
        match res {
            Ok(state) => {
                self.inner = Some(state);
                Ok(())
            },
            Err(e) => {
                self.inner = Some(e.state);
                Err(e.message)
            },
        }
    }
    pub fn start_initializing(&mut self, vsvr: u8) -> Result<()> {
        let context = self.get_context();
        let res = match self.inner.take(){
            Some(state) => state.start_initializing(context, vsvr),
            None => unreachable!(),
        };
        match res {
            Ok(state) => {
                self.inner = Some(state);
                Ok(())
            },
            Err(e) => {
                self.inner = Some(e.state);
                Err(e.message)
            },
        }
    }
    pub fn auto_retransmit_enable(&mut self) -> Result<()> {
        let context = self.get_context();
        let res = match self.inner.take(){
            Some(state) => state.auto_retransmit_enable(context),
            None => unreachable!(),
        };
        match res {
            Ok(state) => {
                self.inner = Some(state);
                Ok(())
            },
            Err(e) => {
                self.inner = Some(e.state);
                Err(e.message)
            },
        }
    }
    pub fn auto_retransmit_disable(&mut self) -> Result<()> {
        let context = self.get_context();
        let res = match self.inner.take(){
            Some(state) => state.auto_retransmit_disable(context),
            None => unreachable!(),
        };
        match res {
            Ok(state) => {
                self.inner = Some(state);
                Ok(())
            },
            Err(e) => {
                self.inner = Some(e.state);
                Err(e.message)
            },
        }
    }
    pub fn set_timeout_sec(&mut self, timeout_sec: u32) {
        self.timeout_sec = timeout_sec;
    }
    pub async fn send_set_vr_command(&mut self, vr: u8) -> Result<()> {
        let context = self.get_context();
        match self.inner.as_mut(){
            Some(state) => {
                state.send_set_vr_command(context, Box::new(self.sync_and_channel_coding.clone()), vr).await
            },
            None => unreachable!(),
        }
    }
    pub async fn send_unlock_command(&mut self) -> Result<()> {
        let context = self.get_context();
        match self.inner.as_mut(){
            Some(state) => {
                state.send_unlock_command(context, Box::new(self.sync_and_channel_coding.clone())).await
            },
            None => unreachable!(),
        }
    }

    pub fn break_point_confirm(&mut self, cmd_ctx: CommandContext) -> Result<()> {
        let context = self.get_context();
        let (ret, next_id) = match self.inner.as_mut(){
            Some(state) => {
                let ret = state.break_point_confirm(context, cmd_ctx);
                let next_id = state.get_next_id();
                (ret, next_id)
            },
            None => unreachable!(),
        };
        self.next_id = next_id.unwrap_or(self.next_id);
        ret
    }

    pub fn append(&mut self, cmd_ctx: CommandContext) -> Result<Option<CopTaskId>> {
        let context = self.get_context();
        let (ret, next_id) = match self.inner.as_mut(){
            Some(state) => {
                let ret = state.append(context, cmd_ctx);
                let next_id = state.get_next_id();
                (ret, next_id)
            },
            None => unreachable!(),
        };
        self.next_id = next_id.unwrap_or(self.next_id);
        ret
    }
    pub async fn execute(&mut self) {
        let context = self.get_context();
        self.inner = match self.inner.take(){
            Some(state) => Some(state.execute(context, Box::new(self.sync_and_channel_coding.clone())).await),
            None => unreachable!(),
        };
    }
}

#[cfg(test)]
mod tests {
    use crate::{fop::worker::CommandContext, registry::FatCommandSchema};

    use super::{FopStateActive, FopStateAutoRetransmitOff, FopStateIdle, FopStateInitialize, FopStateLockout, FopStateMachine, FopStateNode, FopStateUnlocking};
    use std::sync::Arc;

    use anyhow::Result;
    use gaia_ccsds_c2a::{access::cmd::schema::CommandSchema, ccsds::tc::{self, clcw::CLCW, sync_and_channel_coding::FrameType}};
    use gaia_tmtc::{cop::CopWorkerStatusPattern, tco_tmiv::Tco};
    use tokio::sync::{broadcast, Mutex};

    impl<T> FopStateMachine<T> 
    where 
        T: tc::SyncAndChannelCoding + Clone + Send + Sync + 'static,
    {
        fn set_inner(&mut self, state: Box<dyn FopStateNode>) {
            self.inner = Some(state);
        }
    }

    type Transmitted = (u16, u8, FrameType, u8, Vec<u8>);

    #[derive(Clone)]
    struct MockSyncAndChannelCoding {
        transmitted: Arc<Mutex<Vec<Transmitted>>>,
    }

    impl MockSyncAndChannelCoding {
        fn new() -> Self {
            Self {
                transmitted: Arc::new(Mutex::new(Vec::new()))
            }
        }

        async fn get_transmitted(&self) -> Vec<(u16, u8, FrameType, u8, Vec<u8>)> {
            self.transmitted.lock().await.clone()
        }
    }

    fn create_cmd_ctx() -> CommandContext {
        CommandContext {
            tco: Arc::new(Tco {
                name: "test".to_string(),
                params: Vec::new(),
                is_type_ad: true,
            }),
            tc_scid: 100,
            fat_schema: FatCommandSchema {
                apid: 0,
                command_id: 0,
                destination_type: 0,
                execution_type: 0,
                has_time_indicator: false,
                schema: CommandSchema {
                    sized_parameters: Vec::new(),
                    static_size: 0,
                    has_trailer_parameter: false,
                },
            }
        }
    }

    fn create_clcw(vr: u8, lockout: bool, retransmit: bool) -> CLCW {
        let mut ret = CLCW::new();
        ret.set_report_value(vr);
        ret.set_lockout(lockout as u8);
        ret.set_retransmit(retransmit as u8);
        ret
    }

    #[async_trait::async_trait]
    impl tc::SyncAndChannelCoding for MockSyncAndChannelCoding {
        async fn transmit(
            &mut self,
            scid: u16,
            vcid: u8,
            frame_type: FrameType,
            sequence_number: u8,
            data_field: &[u8],
        ) -> Result<()> {
            let mut transmitted = self.transmitted.lock().await;
            transmitted.push((scid, vcid, frame_type, sequence_number, data_field.to_vec()));
            Ok(())
        }
    }

    #[tokio::test]
    async fn clcw_unreceived_without_transition() {
        // ブロードキャストチャネルを作成
        let (worker_tx, _worker_rx) = broadcast::channel(16);
        let (queue_tx, _queue_rx) = broadcast::channel(16);
        let (task_tx, _task_rx) = broadcast::channel(16);

        // モックSyncAndChannelCodingを初期化
        let mock_sc = MockSyncAndChannelCoding::new();

        // 状態マシンを作成
        let mut fop_sm = FopStateMachine::new(
            worker_tx.clone(),
            queue_tx.clone(),
            task_tx.clone(),
            mock_sc.clone(),
            100, // tc_scid
        );

        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "CLCW Unreceived");

        fop_sm.evaluate_timeout();
        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "CLCW Unreceived");

        let res = fop_sm.cancel();
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "cancel is not allowed in clcw_unreceived state");
        assert!(fop_sm.inner.is_some());

        let res = fop_sm.start_unlocking();
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "start_unlocking is not allowed in clcw_unreceived state");
        assert!(fop_sm.inner.is_some());

        let res = fop_sm.start_initializing(0);
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "start_initializing is not allowed in clcw_unreceived state");
        assert!(fop_sm.inner.is_some());

        let res = fop_sm.auto_retransmit_enable();
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "auto_retransmit_enable is not allowed in clcw_unreceived state");

        let res = fop_sm.send_set_vr_command(0).await;
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "send_set_vr_command is not allowed in clcw_unreceived state");

        let res = fop_sm.send_unlock_command().await;
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "send_unlock_command is not allowed in clcw_unreceived state");

        let res = fop_sm.append(create_cmd_ctx());
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "append is not allowed in clcw_unreceived state");

        fop_sm.execute().await;
        assert!(fop_sm.inner.is_some());
        assert!(mock_sc.get_transmitted().await.is_empty());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "CLCW Unreceived");
    }

    #[tokio::test]
    async fn clcw_unreceived_clcw_received(){
        // ブロードキャストチャネルを作成
        let (worker_tx, mut worker_rx) = broadcast::channel(16);
        let (queue_tx, _queue_rx) = broadcast::channel(16);
        let (task_tx, _task_rx) = broadcast::channel(16);

        // モックSyncAndChannelCodingを初期化
        let mock_sc = MockSyncAndChannelCoding::new();

        // 状態マシンを作成
        let mut fop_sm = FopStateMachine::new(
            worker_tx.clone(),
            queue_tx.clone(),
            task_tx.clone(),
            mock_sc.clone(),
            100, // tc_scid
        );

        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "CLCW Unreceived");

        fop_sm.clcw_received();
        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Idle");

        assert_eq!(worker_rx.recv().await.unwrap().state, CopWorkerStatusPattern::WorkerIdle as i32);
        assert_eq!(worker_rx.try_recv().unwrap_err(), broadcast::error::TryRecvError::Empty);
    }

    #[tokio::test]
    async fn clcw_unreceived_lockout(){
        // ブロードキャストチャネルを作成
        let (worker_tx, mut worker_rx) = broadcast::channel(16);
        let (queue_tx, _queue_rx) = broadcast::channel(16);
        let (task_tx, _task_rx) = broadcast::channel(16);

        // モックSyncAndChannelCodingを初期化
        let mock_sc = MockSyncAndChannelCoding::new();

        // 状態マシンを作成
        let mut fop_sm = FopStateMachine::new(
            worker_tx.clone(),
            queue_tx.clone(),
            task_tx.clone(),
            mock_sc.clone(),
            100, // tc_scid
        );

        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "CLCW Unreceived");

        fop_sm.set_clcw(&create_clcw(0, true, false));
        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Lockout");
        assert_eq!(worker_rx.recv().await.unwrap().state, CopWorkerStatusPattern::WorkerIdle as i32);
        assert_eq!(worker_rx.recv().await.unwrap().state, CopWorkerStatusPattern::WorkerLockout as i32);
        assert_eq!(worker_rx.try_recv().unwrap_err(), broadcast::error::TryRecvError::Empty);
    }

    #[tokio::test]
    async fn clcw_unreceived_set_clcw(){
        // ブロードキャストチャネルを作成
        let (worker_tx, mut worker_rx) = broadcast::channel(16);
        let (queue_tx, _queue_rx) = broadcast::channel(16);
        let (task_tx, _task_rx) = broadcast::channel(16);

        // モックSyncAndChannelCodingを初期化
        let mock_sc = MockSyncAndChannelCoding::new();

        // 状態マシンを作成
        let mut fop_sm = FopStateMachine::new(
            worker_tx.clone(),
            queue_tx.clone(),
            task_tx.clone(),
            mock_sc.clone(),
            100, // tc_scid
        );

        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "CLCW Unreceived");

        fop_sm.set_clcw(&create_clcw(0, false, false));
        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Idle");

        assert_eq!(worker_rx.recv().await.unwrap().state, CopWorkerStatusPattern::WorkerIdle as i32);
        assert_eq!(worker_rx.try_recv().unwrap_err(), broadcast::error::TryRecvError::Empty);
    }

    #[tokio::test]
    async fn clcw_unreceived_auto_retransmit_disable() {
        // ブロードキャストチャネルを作成
        let (worker_tx, mut worker_rx) = broadcast::channel(16);
        let (queue_tx, _queue_rx) = broadcast::channel(16);
        let (task_tx, _task_rx) = broadcast::channel(16);

        // モックSyncAndChannelCodingを初期化
        let mock_sc = MockSyncAndChannelCoding::new();

        // 状態マシンを作成
        let mut fop_sm = FopStateMachine::new(
            worker_tx.clone(),
            queue_tx.clone(),
            task_tx.clone(),
            mock_sc.clone(),
            100, // tc_scid
        );

        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "CLCW Unreceived");

        let res = fop_sm.auto_retransmit_disable();
        assert!(res.is_ok());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Auto Retransmit Off");

        assert_eq!(worker_rx.recv().await.unwrap().state, CopWorkerStatusPattern::WorkerAutoRetransmitOff as i32);
        assert_eq!(worker_rx.try_recv().unwrap_err(), broadcast::error::TryRecvError::Empty);
    }

    #[tokio::test]
    async fn idle_without_transition() {
        // ブロードキャストチャネルを作成
        let (worker_tx, _worker_rx) = broadcast::channel(16);
        let (queue_tx, _queue_rx) = broadcast::channel(16);
        let (task_tx, _task_rx) = broadcast::channel(16);

        // モックSyncAndChannelCodingを初期化
        let mock_sc = MockSyncAndChannelCoding::new();

        // 状態マシンを作成
        let mut fop_sm = FopStateMachine::new(
            worker_tx.clone(),
            queue_tx.clone(),
            task_tx.clone(),
            mock_sc.clone(),
            100, // tc_scid
        );

        fop_sm.set_inner(Box::new(FopStateIdle));

        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Idle");

        fop_sm.evaluate_timeout();
        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Idle");

        fop_sm.clcw_received();
        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Idle");

        fop_sm.set_clcw(&create_clcw(0, false, false));
        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Idle");

        let res = fop_sm.cancel();
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "cancel is not allowed in idle state");
        assert!(fop_sm.inner.is_some());

        let res = fop_sm.start_unlocking();
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "start_unlocking is not allowed in idle state");
        assert!(fop_sm.inner.is_some());

        let res = fop_sm.auto_retransmit_enable();
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "auto_retransmit_enable is not allowed in idle state");

        let res = fop_sm.send_set_vr_command(0).await;
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "send_set_vr_command is not allowed in idle state");

        let res = fop_sm.send_unlock_command().await;
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "send_unlock_command is not allowed in idle state");

        let res = fop_sm.append(create_cmd_ctx());
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "append is not allowed in idle state");

        fop_sm.execute().await;
        assert!(fop_sm.inner.is_some());
        assert!(mock_sc.get_transmitted().await.is_empty());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Idle");
    }

    #[tokio::test]
    async fn idle_lockout(){
        // ブロードキャストチャネルを作成
        let (worker_tx, mut worker_rx) = broadcast::channel(16);
        let (queue_tx, _queue_rx) = broadcast::channel(16);
        let (task_tx, _task_rx) = broadcast::channel(16);

        // モックSyncAndChannelCodingを初期化
        let mock_sc = MockSyncAndChannelCoding::new();

        // 状態マシンを作成
        let mut fop_sm = FopStateMachine::new(
            worker_tx.clone(),
            queue_tx.clone(),
            task_tx.clone(),
            mock_sc.clone(),
            100, // tc_scid
        );

        fop_sm.set_inner(Box::new(FopStateIdle));

        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Idle");

        fop_sm.set_clcw(&create_clcw(0, true, false));
        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Lockout");

        assert_eq!(worker_rx.recv().await.unwrap().state, CopWorkerStatusPattern::WorkerLockout as i32);
        assert_eq!(worker_rx.try_recv().unwrap_err(), broadcast::error::TryRecvError::Empty);
    }

    #[tokio::test]
    async fn idle_start_initialize(){
        // ブロードキャストチャネルを作成
        let (worker_tx, mut worker_rx) = broadcast::channel(16);
        let (queue_tx, _queue_rx) = broadcast::channel(16);
        let (task_tx, _task_rx) = broadcast::channel(16);

        // モックSyncAndChannelCodingを初期化
        let mock_sc = MockSyncAndChannelCoding::new();

        // 状態マシンを作成
        let mut fop_sm = FopStateMachine::new(
            worker_tx.clone(),
            queue_tx.clone(),
            task_tx.clone(),
            mock_sc.clone(),
            100, // tc_scid
        );

        fop_sm.set_inner(Box::new(FopStateIdle));

        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Idle");

        let res = fop_sm.start_initializing(0);
        assert!(res.is_ok());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Initialize");

        assert_eq!(worker_rx.recv().await.unwrap().state, CopWorkerStatusPattern::WorkerInitialize as i32);
        assert_eq!(worker_rx.try_recv().unwrap_err(), broadcast::error::TryRecvError::Empty);
    }

    #[tokio::test]
    async fn idle_auto_retransmit_disable() {
        // ブロードキャストチャネルを作成
        let (worker_tx, mut worker_rx) = broadcast::channel(16);
        let (queue_tx, _queue_rx) = broadcast::channel(16);
        let (task_tx, _task_rx) = broadcast::channel(16);

        // モックSyncAndChannelCodingを初期化
        let mock_sc = MockSyncAndChannelCoding::new();

        // 状態マシンを作成
        let mut fop_sm = FopStateMachine::new(
            worker_tx.clone(),
            queue_tx.clone(),
            task_tx.clone(),
            mock_sc.clone(),
            100, // tc_scid
        );

        fop_sm.set_inner(Box::new(FopStateIdle));

        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Idle");

        let res = fop_sm.auto_retransmit_disable();
        assert!(res.is_ok());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Auto Retransmit Off");

        assert_eq!(worker_rx.recv().await.unwrap().state, CopWorkerStatusPattern::WorkerAutoRetransmitOff as i32);
        assert_eq!(worker_rx.try_recv().unwrap_err(), broadcast::error::TryRecvError::Empty);
    }

    #[tokio::test]
    async fn lockout_without_transition() {
        // ブロードキャストチャネルを作成
        let (worker_tx, _worker_rx) = broadcast::channel(16);
        let (queue_tx, _queue_rx) = broadcast::channel(16);
        let (task_tx, _task_rx) = broadcast::channel(16);

        // モックSyncAndChannelCodingを初期化
        let mock_sc = MockSyncAndChannelCoding::new();

        // 状態マシンを作成
        let mut fop_sm = FopStateMachine::new(
            worker_tx.clone(),
            queue_tx.clone(),
            task_tx.clone(),
            mock_sc.clone(),
            100, // tc_scid
        );

        fop_sm.set_inner(Box::new(FopStateLockout));

        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Lockout");

        fop_sm.clcw_received();
        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Lockout");

        fop_sm.set_clcw(&create_clcw(0, true, false));
        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Lockout");

        fop_sm.evaluate_timeout();
        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Lockout");

        let res = fop_sm.cancel();
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "cancel is not allowed in lockout state");
        assert!(fop_sm.inner.is_some());

        let res = fop_sm.start_initializing(0);
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "start_initializing is not allowed in lockout state");
        assert!(fop_sm.inner.is_some());

        let res = fop_sm.auto_retransmit_enable();
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "auto_retransmit_enable is not allowed in lockout state");

        let res = fop_sm.send_set_vr_command(0).await;
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "send_set_vr_command is not allowed in lockout state");

        let res = fop_sm.send_unlock_command().await;
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "send_unlock_command is not allowed in lockout state");

        let res = fop_sm.append(create_cmd_ctx());
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "append is not allowed in lockout state");

        fop_sm.execute().await;
        assert!(fop_sm.inner.is_some());
        assert!(mock_sc.get_transmitted().await.is_empty());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Lockout");
    }

    #[tokio::test]
    async fn lockout_unlocked(){
        // ブロードキャストチャネルを作成
        let (worker_tx, mut worker_rx) = broadcast::channel(16);
        let (queue_tx, _queue_rx) = broadcast::channel(16);
        let (task_tx, _task_rx) = broadcast::channel(16);

        // モックSyncAndChannelCodingを初期化
        let mock_sc = MockSyncAndChannelCoding::new();

        // 状態マシンを作成
        let mut fop_sm = FopStateMachine::new(
            worker_tx.clone(),
            queue_tx.clone(),
            task_tx.clone(),
            mock_sc.clone(),
            100, // tc_scid
        );

        fop_sm.set_inner(Box::new(FopStateLockout));

        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Lockout");

        fop_sm.set_clcw(&create_clcw(0, false, false));
        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Idle");

        assert_eq!(worker_rx.recv().await.unwrap().state, CopWorkerStatusPattern::WorkerIdle as i32);
    }

    #[tokio::test]
    async fn lockout_auto_retransmit_disable() {
        // ブロードキャストチャネルを作成
        let (worker_tx, mut worker_rx) = broadcast::channel(16);
        let (queue_tx, _queue_rx) = broadcast::channel(16);
        let (task_tx, _task_rx) = broadcast::channel(16);

        // モックSyncAndChannelCodingを初期化
        let mock_sc = MockSyncAndChannelCoding::new();

        // 状態マシンを作成
        let mut fop_sm = FopStateMachine::new(
            worker_tx.clone(),
            queue_tx.clone(),
            task_tx.clone(),
            mock_sc.clone(),
            100, // tc_scid
        );

        fop_sm.set_inner(Box::new(FopStateLockout));

        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Lockout");

        let res = fop_sm.auto_retransmit_disable();
        assert!(res.is_ok());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Auto Retransmit Off");

        assert_eq!(worker_rx.recv().await.unwrap().state, CopWorkerStatusPattern::WorkerAutoRetransmitOff as i32);
        assert_eq!(worker_rx.try_recv().unwrap_err(), broadcast::error::TryRecvError::Empty);
    }

    #[tokio::test]
    async fn lockout_start_unlocking(){
        // ブロードキャストチャネルを作成
        let (worker_tx, mut worker_rx) = broadcast::channel(16);
        let (queue_tx, _queue_rx) = broadcast::channel(16);
        let (task_tx, _task_rx) = broadcast::channel(16);

        // モックSyncAndChannelCodingを初期化
        let mock_sc = MockSyncAndChannelCoding::new();

        // 状態マシンを作成
        let mut fop_sm = FopStateMachine::new(
            worker_tx.clone(),
            queue_tx.clone(),
            task_tx.clone(),
            mock_sc.clone(),
            100, // tc_scid
        );

        fop_sm.set_inner(Box::new(FopStateLockout));

        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Lockout");

        let res = fop_sm.start_unlocking();
        assert!(res.is_ok());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Unlocking");

        assert_eq!(worker_rx.recv().await.unwrap().state, CopWorkerStatusPattern::WorkerUnlocking as i32);
        assert_eq!(worker_rx.try_recv().unwrap_err(), broadcast::error::TryRecvError::Empty);
    }

    #[tokio::test]
    async fn unlocking_without_transition() {
        // ブロードキャストチャネルを作成
        let (worker_tx, _worker_rx) = broadcast::channel(16);
        let (queue_tx, _queue_rx) = broadcast::channel(16);
        let (task_tx, _task_rx) = broadcast::channel(16);

        // モックSyncAndChannelCodingを初期化
        let mock_sc = MockSyncAndChannelCoding::new();

        // 状態マシンを作成
        let mut fop_sm = FopStateMachine::new(
            worker_tx.clone(),
            queue_tx.clone(),
            task_tx.clone(),
            mock_sc.clone(),
            100, // tc_scid
        );

        fop_sm.set_inner(Box::new(FopStateUnlocking::new()));

        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Unlocking");

        fop_sm.clcw_received();
        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Unlocking");

        fop_sm.set_clcw(&create_clcw(0, true, false));
        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Unlocking");

        let res = fop_sm.start_unlocking();
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "start_unlocking is not allowed in unlocking state");
        assert!(fop_sm.inner.is_some());

        let res = fop_sm.start_initializing(0);
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "start_initializing is not allowed in unlocking state");
        assert!(fop_sm.inner.is_some());

        let res = fop_sm.auto_retransmit_enable();
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "auto_retransmit_enable is not allowed in unlocking state");

        let res = fop_sm.send_set_vr_command(0).await;
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "send_set_vr_command is not allowed in unlocking state");

        let res = fop_sm.send_unlock_command().await;
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "send_unlock_command is not allowed in unlocking state");

        let res = fop_sm.append(create_cmd_ctx());
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "append is not allowed in unlocking state");

        fop_sm.execute().await;
        assert!(fop_sm.inner.is_some());
        assert_eq!(mock_sc.get_transmitted().await.len(), 1);
        assert_eq!(mock_sc.get_transmitted().await[0].0, 100);
        assert_eq!(mock_sc.get_transmitted().await[0].2, FrameType::TypeBC);
        assert_eq!(mock_sc.get_transmitted().await[0].4, vec![0x00]);
    }

    #[tokio::test]
    async fn unlocking_unlocked(){
        // ブロードキャストチャネルを作成
        let (worker_tx, mut worker_rx) = broadcast::channel(16);
        let (queue_tx, _queue_rx) = broadcast::channel(16);
        let (task_tx, _task_rx) = broadcast::channel(16);

        // モックSyncAndChannelCodingを初期化
        let mock_sc = MockSyncAndChannelCoding::new();

        // 状態マシンを作成
        let mut fop_sm = FopStateMachine::new(
            worker_tx.clone(),
            queue_tx.clone(),
            task_tx.clone(),
            mock_sc.clone(),
            100, // tc_scid
        );

        fop_sm.set_inner(Box::new(FopStateUnlocking::new()));

        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Unlocking");

        fop_sm.set_clcw(&create_clcw(0, false, false));
        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Idle");

        assert_eq!(worker_rx.recv().await.unwrap().state, CopWorkerStatusPattern::WorkerIdle as i32);
        assert_eq!(worker_rx.try_recv().unwrap_err(), broadcast::error::TryRecvError::Empty);
    }

    #[tokio::test]
    async fn unlocking_cancelled() {
        // ブロードキャストチャネルを作成
        let (worker_tx, mut worker_rx) = broadcast::channel(16);
        let (queue_tx, _queue_rx) = broadcast::channel(16);
        let (task_tx, _task_rx) = broadcast::channel(16);

        // モックSyncAndChannelCodingを初期化
        let mock_sc = MockSyncAndChannelCoding::new();

        // 状態マシンを作成
        let mut fop_sm = FopStateMachine::new(
            worker_tx.clone(),
            queue_tx.clone(),
            task_tx.clone(),
            mock_sc.clone(),
            100, // tc_scid
        );

        fop_sm.set_inner(Box::new(FopStateUnlocking::new()));

        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Unlocking");

        let res = fop_sm.cancel();
        assert!(res.is_ok());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Lockout");

        assert_eq!(worker_rx.recv().await.unwrap().state, CopWorkerStatusPattern::WorkerCanceled as i32);
        assert_eq!(worker_rx.recv().await.unwrap().state, CopWorkerStatusPattern::WorkerLockout as i32);
        assert_eq!(worker_rx.try_recv().unwrap_err(), broadcast::error::TryRecvError::Empty);
    }

    #[tokio::test]
    async fn unlocking_auto_retransmit_disable() {
        // ブロードキャストチャネルを作成
        let (worker_tx, mut worker_rx) = broadcast::channel(16);
        let (queue_tx, _queue_rx) = broadcast::channel(16);
        let (task_tx, _task_rx) = broadcast::channel(16);

        // モックSyncAndChannelCodingを初期化
        let mock_sc = MockSyncAndChannelCoding::new();

        // 状拋マシンを作成
        let mut fop_sm = FopStateMachine::new(
            worker_tx.clone(),
            queue_tx.clone(),
            task_tx.clone(),
            mock_sc.clone(),
            100, // tc_scid
        );

        fop_sm.set_inner(Box::new(FopStateUnlocking::new()));

        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Unlocking");

        let res = fop_sm.auto_retransmit_disable();
        assert!(res.is_ok());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Auto Retransmit Off");

        assert_eq!(worker_rx.recv().await.unwrap().state, CopWorkerStatusPattern::WorkerAutoRetransmitOff as i32);
        assert_eq!(worker_rx.try_recv().unwrap_err(), broadcast::error::TryRecvError::Empty);
    }

    #[tokio::test]
    async fn unlocking_timeout() {
        // ブロードキャストチャネルを作成
        let (worker_tx, mut worker_rx) = broadcast::channel(16);
        let (queue_tx, _queue_rx) = broadcast::channel(16);
        let (task_tx, _task_rx) = broadcast::channel(16);

        // モックSyncAndChannelCodingを初期化
        let mock_sc = MockSyncAndChannelCoding::new();

        let mut fop_sm = FopStateMachine::new(
            worker_tx.clone(),
            queue_tx.clone(),
            task_tx.clone(),
            mock_sc.clone(),
            100,
        );

        fop_sm.set_inner(Box::new(FopStateUnlocking::new()));

        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Unlocking");

        fop_sm.set_timeout_sec(1);

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        fop_sm.evaluate_timeout();
        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Lockout");

        assert_eq!(worker_rx.recv().await.unwrap().state, CopWorkerStatusPattern::WorkerTimeout as i32);
        assert_eq!(worker_rx.recv().await.unwrap().state, CopWorkerStatusPattern::WorkerLockout as i32);
        assert_eq!(worker_rx.try_recv().unwrap_err(), broadcast::error::TryRecvError::Empty);
    }

    #[tokio::test]
    async fn initialize_without_transition() {
        // ブロードキャストチャネルを作成
        let (worker_tx, _worker_rx) = broadcast::channel(16);
        let (queue_tx, _queue_rx) = broadcast::channel(16);
        let (task_tx, _task_rx) = broadcast::channel(16);

        // モックSyncAndChannelCodingを初期化
        let mock_sc = MockSyncAndChannelCoding::new();

        let mut fop_sm = FopStateMachine::new(
            worker_tx.clone(),
            queue_tx.clone(),
            task_tx.clone(),
            mock_sc.clone(),
            100,
        );

        let vr = 10;

        fop_sm.set_inner(Box::new(FopStateInitialize::new(vr)));

        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Initialize");

        fop_sm.clcw_received();
        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Initialize");

        fop_sm.set_clcw(&create_clcw(0, false, false));
        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Initialize");

        let res = fop_sm.start_initializing(0);
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "start_initializing is not allowed in initialize state");
        assert!(fop_sm.inner.is_some());

        let res = fop_sm.auto_retransmit_enable();
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "auto_retransmit_enable is not allowed in initialize state");

        let res = fop_sm.send_set_vr_command(0).await;
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "send_set_vr_command is not allowed in initialize state");

        let res = fop_sm.send_unlock_command().await;
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "send_unlock_command is not allowed in initialize state");

        let res = fop_sm.append(create_cmd_ctx());
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "append is not allowed in initialize state");

        fop_sm.execute().await;
        assert!(fop_sm.inner.is_some());
        assert!(mock_sc.get_transmitted().await.len() == 1);
        assert_eq!(mock_sc.get_transmitted().await[0].0, 100);
        assert_eq!(mock_sc.get_transmitted().await[0].2, FrameType::TypeBC);
        assert_eq!(mock_sc.get_transmitted().await[0].4, vec![0b10000010, 0b00000000, vr]);
    }

    #[tokio::test]
    async fn initialize_timeout() {
        // ブロードキャストチャネルを作成
        let (worker_tx, mut worker_rx) = broadcast::channel(16);
        let (queue_tx, _queue_rx) = broadcast::channel(16);
        let (task_tx, _task_rx) = broadcast::channel(16);

        // モックSyncAndChannelCodingを初期化
        let mock_sc = MockSyncAndChannelCoding::new();

        let mut fop_sm = FopStateMachine::new(
            worker_tx.clone(),
            queue_tx.clone(),
            task_tx.clone(),
            mock_sc.clone(),
            100,
        );

        let vr = 10;

        fop_sm.set_inner(Box::new(FopStateInitialize::new(vr)));

        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Initialize");

        fop_sm.set_timeout_sec(1);

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        fop_sm.evaluate_timeout();
        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Idle");

        assert_eq!(worker_rx.recv().await.unwrap().state, CopWorkerStatusPattern::WorkerTimeout as i32);
        assert_eq!(worker_rx.try_recv().unwrap_err(), broadcast::error::TryRecvError::Empty);
    }

    #[tokio::test]
    async fn initialize_cancelled() {
        // ブロードキャストチャネルを作成
        let (worker_tx, mut worker_rx) = broadcast::channel(16);
        let (queue_tx, _queue_rx) = broadcast::channel(16);
        let (task_tx, _task_rx) = broadcast::channel(16);

        // モックSyncAndChannelCodingを初期化
        let mock_sc = MockSyncAndChannelCoding::new();

        let mut fop_sm = FopStateMachine::new(
            worker_tx.clone(),
            queue_tx.clone(),
            task_tx.clone(),
            mock_sc.clone(),
            100,
        );

        let vr = 10;

        fop_sm.set_inner(Box::new(FopStateInitialize::new(vr)));

        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Initialize");

        let res = fop_sm.cancel();
        assert!(res.is_ok());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Idle");

        assert_eq!(worker_rx.recv().await.unwrap().state, CopWorkerStatusPattern::WorkerCanceled as i32);
        assert_eq!(worker_rx.try_recv().unwrap_err(), broadcast::error::TryRecvError::Empty);
    }

    #[tokio::test]
    async fn initialize_auto_retransmit_disable() {
        // ブロードキャストチャネルを作成
        let (worker_tx, mut worker_rx) = broadcast::channel(16);
        let (queue_tx, _queue_rx) = broadcast::channel(16);
        let (task_tx, _task_rx) = broadcast::channel(16);

        // モックSyncAndChannelCodingを初期化
        let mock_sc = MockSyncAndChannelCoding::new();

        let mut fop_sm = FopStateMachine::new(
            worker_tx.clone(),
            queue_tx.clone(),
            task_tx.clone(),
            mock_sc.clone(),
            100,
        );

        let vr = 10;

        fop_sm.set_inner(Box::new(FopStateInitialize::new(vr)));

        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Initialize");

        let res = fop_sm.auto_retransmit_disable();
        assert!(res.is_ok());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Auto Retransmit Off");

        assert_eq!(worker_rx.recv().await.unwrap().state, CopWorkerStatusPattern::WorkerAutoRetransmitOff as i32);
        assert_eq!(worker_rx.try_recv().unwrap_err(), broadcast::error::TryRecvError::Empty);
    }

    #[tokio::test]
    async fn initialize_vsvr_matched() {
        // ブロードキャストチャネルを作成
        let (worker_tx, mut worker_rx) = broadcast::channel(16);
        let (queue_tx, _queue_rx) = broadcast::channel(16);
        let (task_tx, _task_rx) = broadcast::channel(16);

        // モックSyncAndChannelCodingを初期化
        let mock_sc = MockSyncAndChannelCoding::new();

        let mut fop_sm = FopStateMachine::new(
            worker_tx.clone(),
            queue_tx.clone(),
            task_tx.clone(),
            mock_sc.clone(),
            100,
        );

        fop_sm.next_id = 50;

        let vr = 10;

        fop_sm.set_inner(Box::new(FopStateInitialize::new(vr)));

        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Initialize");

        let vsvr = 10;

        fop_sm.set_clcw(&create_clcw(vsvr, false, false));
        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Active");
        assert_eq!(fop_sm.next_id, 50);

        assert_eq!(worker_rx.recv().await.unwrap().state, CopWorkerStatusPattern::WorkerActive as i32);
        assert_eq!(worker_rx.try_recv().unwrap_err(), broadcast::error::TryRecvError::Empty);
    }

    #[tokio::test]
    async fn initialize_lockout() {
        // ブロードキャストチャネルを作成
        let (worker_tx, mut worker_rx) = broadcast::channel(16);
        let (queue_tx, _queue_rx) = broadcast::channel(16);
        let (task_tx, _task_rx) = broadcast::channel(16);

        // モックSyncAndChannelCodingを初期化
        let mock_sc = MockSyncAndChannelCoding::new();

        let mut fop_sm = FopStateMachine::new(
            worker_tx.clone(),
            queue_tx.clone(),
            task_tx.clone(),
            mock_sc.clone(),
            100,
        );

        fop_sm.next_id = 50;

        let vr = 10;

        fop_sm.set_inner(Box::new(FopStateInitialize::new(vr)));

        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Initialize");

        fop_sm.set_clcw(&create_clcw(0, true, false));
        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Lockout");

        assert_eq!(worker_rx.recv().await.unwrap().state, CopWorkerStatusPattern::WorkerLockout as i32);
        assert_eq!(worker_rx.try_recv().unwrap_err(), broadcast::error::TryRecvError::Empty);
    }

    #[tokio::test]
    async fn auto_retransmit_off_without_transition() {
        // ブロードキャストチャネルを作成
        let (worker_tx, _worker_rx) = broadcast::channel(16);
        let (queue_tx, _queue_rx) = broadcast::channel(16);
        let (task_tx, _task_rx) = broadcast::channel(16);

        // モックSyncAndChannelCodingを初期化
        let mock_sc = MockSyncAndChannelCoding::new();

        let mut fop_sm = FopStateMachine::new(
            worker_tx.clone(),
            queue_tx.clone(),
            task_tx.clone(),
            mock_sc.clone(),
            100,
        );

        fop_sm.set_inner(Box::new(FopStateAutoRetransmitOff::new()));

        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Auto Retransmit Off");

        fop_sm.clcw_received();
        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Auto Retransmit Off");

        fop_sm.set_clcw(&create_clcw(0, false, false));
        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Auto Retransmit Off");

        fop_sm.set_clcw(&create_clcw(0, true, false));
        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Auto Retransmit Off");

        let res = fop_sm.cancel();
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "cancel is not allowed in auto_retransmit_off state");
        assert!(fop_sm.inner.is_some());

        let res = fop_sm.start_initializing(0);
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "start_initializing is not allowed in auto_retransmit_off state");
        assert!(fop_sm.inner.is_some());

        let res = fop_sm.auto_retransmit_disable();
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "auto_retransmit_disable is not allowed in auto_retransmit_off state");

        let vr = 10;
        let res = fop_sm.send_set_vr_command(vr).await;
        assert!(res.is_ok());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Auto Retransmit Off");
        assert_eq!(mock_sc.get_transmitted().await.len(), 1);
        assert_eq!(mock_sc.get_transmitted().await[0].0, 100);
        assert_eq!(mock_sc.get_transmitted().await[0].2, FrameType::TypeBC);
        assert_eq!(mock_sc.get_transmitted().await[0].4, vec![0b10000010, 0b00000000, vr]);

        let res = fop_sm.send_unlock_command().await;
        assert!(res.is_ok());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Auto Retransmit Off");
        assert_eq!(mock_sc.get_transmitted().await.len(), 2);
        assert_eq!(mock_sc.get_transmitted().await[1].0, 100);
        assert_eq!(mock_sc.get_transmitted().await[1].2, FrameType::TypeBC);
        assert_eq!(mock_sc.get_transmitted().await[1].4, vec![0u8]);

        let res = fop_sm.append(create_cmd_ctx());
        assert!(res.is_ok());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Auto Retransmit Off");

        fop_sm.execute().await;
        assert_eq!(mock_sc.get_transmitted().await.len(), 3);
        assert_eq!(mock_sc.get_transmitted().await[2].0, 100);
        assert_eq!(mock_sc.get_transmitted().await[2].2, FrameType::TypeAD);        
        assert_eq!(mock_sc.get_transmitted().await[2].3, vr);
    }

    #[tokio::test]
    async fn active_do_nothing() {
        // ブロードキャストチャネルを作成
        let (worker_tx, _worker_rx) = broadcast::channel(16);
        let (queue_tx, _queue_rx) = broadcast::channel(16);
        let (task_tx, _task_rx) = broadcast::channel(16);

        // モックSyncAndChannelCodingを初期化
        let mock_sc = MockSyncAndChannelCoding::new();

        let mut fop_sm = FopStateMachine::new(
            worker_tx.clone(),
            queue_tx.clone(),
            task_tx.clone(),
            mock_sc.clone(),
            100,
        );

        let vr = 10;

        fop_sm.set_inner(Box::new(FopStateActive::new(vr, 50)));

        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Active");

        fop_sm.clcw_received();
        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Active");

        let res = fop_sm.start_unlocking();
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "start_unlocking is not allowed in active state");

        let res = fop_sm.auto_retransmit_enable();
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "auto_retransmit_enable is not allowed in active state");

        let res = fop_sm.send_set_vr_command(0).await;
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "send_set_vr_command is not allowed in active state");

        let res = fop_sm.send_unlock_command().await;
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "send_unlock_command is not allowed in active state");
    }
}
