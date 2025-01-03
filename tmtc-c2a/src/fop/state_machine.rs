use std::{collections::VecDeque, fmt::Display, future::Future, pin::Pin};

use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use gaia_ccsds_c2a::ccsds::tc::{self, clcw::CLCW, sync_and_channel_coding::FrameType};
use gaia_tmtc::cop::{CopQueueStatus, CopQueueStatusSet, CopTaskStatus, CopTaskStatusPattern, CopWorkerStatus, CopWorkerStatusPattern};
use prost_types::Timestamp;
use tokio::sync::mpsc;
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

type FopStateNodeResult = Result<Box<dyn FopStateNode>, FopStateNodeError>;
type FopTaskAppendResult = Result<(Box<dyn FopStateNode>, Option<CopTaskId>), FopStateNodeError>;

trait FopStateNode: Display {
    fn evaluate_timeout(self: Box<Self>, context: FopStateContext) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>>;

    fn clcw_received(self: Box<Self>, context: FopStateContext) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>>;
    fn lockout(self: Box<Self>, context: FopStateContext, flag: bool) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>>;
    fn vsvr_matched(self: Box<Self>, context: FopStateContext, vr: u8) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>>;
    fn accept (self: Box<Self>, context: FopStateContext, vr: u8) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>>;
    fn reject (self: Box<Self>, context: FopStateContext) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>>;

    fn terminate(self: Box<Self>, context: FopStateContext) -> Pin<Box<dyn Future<Output = FopStateNodeResult>>>;
    fn start_unlocking(self: Box<Self>, context: FopStateContext) -> Pin<Box<dyn Future<Output = FopStateNodeResult>>>;
    fn start_initializing(self: Box<Self>, context: FopStateContext, vsvr: u8) -> Pin<Box<dyn Future<Output = FopStateNodeResult>>>;
    fn auto_retransmit_enable(self: Box<Self>, context: FopStateContext) -> Pin<Box<dyn Future<Output = FopStateNodeResult>>>;
    fn auto_retransmit_disable(self: Box<Self>, context: FopStateContext) -> Pin<Box<dyn Future<Output = FopStateNodeResult>>>;
    fn send_set_vr_command(&mut self, context: FopStateContext, sync_and_channel_coding: Box<dyn tc::SyncAndChannelCoding + Send + Sync>, vr: u8) -> Pin<Box<dyn Future<Output = Result<()>>>>;
    fn send_unlock_command(&self, context: FopStateContext, sync_and_channel_coding: Box<dyn tc::SyncAndChannelCoding + Send + Sync + 'static>) -> Pin<Box<dyn Future<Output = Result<()>>>>;

    fn execute (self: Box<Self>, context: FopStateContext, sync_and_channel_coding: Box<dyn tc::SyncAndChannelCoding + Send + Sync>) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>>;

    fn append (self: Box<Self>, context: FopStateContext, cmd_ctx: CommandContext) -> Pin<Box<dyn Future<Output = FopTaskAppendResult>>>;
    fn get_next_id(&self) -> Option<CopTaskId>;

    fn send_status(self: Box<Self>, context: FopStateContext) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>>;
}

#[derive(Clone)]
pub struct FopStateContext {
    worker_state_tx: mpsc::Sender<CopWorkerStatus>,
    queue_status_tx: mpsc::Sender<CopQueueStatusSet>,
    task_status_tx: mpsc::Sender<CopTaskStatus>,
    timeout_sec: u32,
    max_executing: usize,
    next_id: CopTaskId,
    tc_scid: u16,
}

impl FopStateContext {
    pub async fn send_worker_status(&self, status: CopWorkerStatusPattern) {
        let now = chrono::Utc::now().naive_utc();
        let timestamp = Timestamp {
            seconds: now.and_utc().timestamp(),
            nanos: now.and_utc().timestamp_subsec_nanos() as i32,
        };
        if let Err(e) = self.worker_state_tx.send(CopWorkerStatus {
            state: status.into(),
            timeout_sec: self.timeout_sec.into(),
            max_executing: self.max_executing as u32,
            timestamp: Some(timestamp),
        }).await {
            error!("failed to send FOP state: {}", e);
        }
    }
    pub fn get_queue_context(&self) -> FopQueueContext {
        FopQueueContext {
            queue_status_tx: self.queue_status_tx.clone(),
            task_status_tx: self.task_status_tx.clone(),
            max_executing: self.max_executing,
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
    fn evaluate_timeout(self: Box<Self>, _: FopStateContext) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async move{ self as Box<dyn FopStateNode> })
    }  

    fn clcw_received(self: Box<Self>, _: FopStateContext) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async { self as Box<dyn FopStateNode> })
    }
    fn lockout(self: Box<Self>, context: FopStateContext, flag: bool) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async move {
            if flag {
                context.send_worker_status(CopWorkerStatusPattern::WorkerLockout).await;
                Box::new(FopStateLockout) as Box<dyn FopStateNode> 
            } else {
                self as Box<dyn FopStateNode> 
            }
        })
    }
    fn vsvr_matched(self: Box<Self>, _: FopStateContext, _: u8) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async { self as Box<dyn FopStateNode> })
    }
    fn accept (self: Box<Self>, _: FopStateContext, _: u8) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async { self as Box<dyn FopStateNode> })
    }
    fn reject (self: Box<Self>, _: FopStateContext) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async { self as Box<dyn FopStateNode> })
    }

    fn terminate(self: Box<Self>, _: FopStateContext) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>, FopStateNodeError>>>> {
        Box::pin(async {
            Err(FopStateNodeError::new(
                "cancel is not allowed in idle state",
                self as Box<dyn FopStateNode>,
            ))
        })
    }
    fn start_unlocking(self: Box<Self>, _: FopStateContext) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>, FopStateNodeError>>>> {
        Box::pin(async move {
            Err(FopStateNodeError::new(
                "start_unlocking is not allowed in idle state",
                self as Box<dyn FopStateNode>,
            ))
        })
    }
    fn start_initializing(self: Box<Self>, context: FopStateContext, vsvr: u8) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>, FopStateNodeError>>>> {
        Box::pin(async move {
            context.send_worker_status(CopWorkerStatusPattern::WorkerInitialize).await;
            Ok(Box::new(FopStateInitialize::new(vsvr)) as Box<dyn FopStateNode>)
        })
    }
    fn auto_retransmit_disable(self: Box<Self>, context: FopStateContext) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>, FopStateNodeError>>>> {
        Box::pin(async move {
            context.send_worker_status(CopWorkerStatusPattern::WorkerAutoRetransmitOff).await;
            Ok(Box::new(FopStateAutoRetransmitOff::new()) as Box<dyn FopStateNode>)
        })
    }
    fn auto_retransmit_enable(self: Box<Self>, _: FopStateContext) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>, FopStateNodeError>>>> {
        Box::pin(async {
            Err(FopStateNodeError::new(
                "auto_retransmit_enable is not allowed in idle state",
                self as Box<dyn FopStateNode>,
            ))
        })
    }
    fn send_set_vr_command(&mut self, _: FopStateContext, _: Box<dyn tc::SyncAndChannelCoding + Send + Sync >, _: u8) -> Pin<Box<dyn Future<Output = Result<()>>>> {
        Box::pin(async { Err(anyhow!("send_set_vr_command is not allowed in idle state")) })
    }
    fn send_unlock_command(&self, _: FopStateContext, _: Box<dyn tc::SyncAndChannelCoding + Send + Sync >) -> Pin<Box<dyn Future<Output = Result<()>>>> {
        Box::pin(async { Err(anyhow!("send_unlock_command is not allowed in idle state")) })
    }

    fn append (self: Box<Self>, _: FopStateContext, _: CommandContext) -> Pin<Box<dyn Future<Output = Result<(Box<dyn FopStateNode>,Option<CopTaskId>), FopStateNodeError>>>> {
        Box::pin(async {
            Err(FopStateNodeError::new(
                "append is not allowed in idle state",
                self as Box<dyn FopStateNode>,
            ))
        })
    }
    fn execute (self: Box<Self>, _: FopStateContext, _: Box<dyn tc::SyncAndChannelCoding + Send + Sync>) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async { self as Box<dyn FopStateNode> })
    }
    fn get_next_id(&self) -> Option<CopTaskId> {
        None
    }

    fn send_status(self: Box<Self>, context: FopStateContext) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async move {
            context.send_worker_status(CopWorkerStatusPattern::WorkerIdle).await;
            self as Box<dyn FopStateNode>
        })
    }
}

struct FopStateLockout;

impl Display for FopStateLockout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Lockout")
    }
}

impl FopStateNode for FopStateLockout {
    fn evaluate_timeout(self: Box<Self>, _: FopStateContext) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async move{ self as Box<dyn FopStateNode> })
    }  

    fn clcw_received(self: Box<Self>, _: FopStateContext) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async { self as Box<dyn FopStateNode> })
    }
    fn lockout(self: Box<Self>, context: FopStateContext, flag: bool) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async move {
            if flag {
                self as Box<dyn FopStateNode>
            } else {
                context.send_worker_status(CopWorkerStatusPattern::WorkerIdle).await;
                Box::new(FopStateIdle) as Box<dyn FopStateNode> 
            }
        })
    }
    fn vsvr_matched(self: Box<Self>, _: FopStateContext, _: u8) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async { self as Box<dyn FopStateNode> })
    }
    fn accept (self: Box<Self>, _: FopStateContext, _: u8) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async { self as Box<dyn FopStateNode> })
    }
    fn reject (self: Box<Self>, _: FopStateContext) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async { self as Box<dyn FopStateNode> })
    }
    
    fn terminate(self: Box<Self>, _: FopStateContext) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>, FopStateNodeError>>>> {
        Box::pin(async {
            Err(FopStateNodeError::new(
                "cancel is not allowed in lockout state",
                self as Box<dyn FopStateNode>,
            ))
        })
    }
    fn start_unlocking(self: Box<Self>, context: FopStateContext) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>, FopStateNodeError>>>> {
        Box::pin(async move {
            context.send_worker_status(CopWorkerStatusPattern::WorkerUnlocking).await;
            Ok(Box::new(FopStateUnlocking::new()) as Box<dyn FopStateNode>)
        })
    }
    fn start_initializing(self: Box<Self>, _: FopStateContext, _: u8) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>, FopStateNodeError>>>> {
        Box::pin(async {
            Err(FopStateNodeError::new(
                "start_initializing is not allowed in lockout state",
                self as Box<dyn FopStateNode>,
            ))
        })
    }
    fn auto_retransmit_disable(self: Box<Self>, context: FopStateContext) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>, FopStateNodeError>>>> {
        Box::pin(async move {
            context.send_worker_status(CopWorkerStatusPattern::WorkerAutoRetransmitOff).await;
            Ok(Box::new(FopStateAutoRetransmitOff::new()) as Box<dyn FopStateNode>)
        })
    }
    fn auto_retransmit_enable(self: Box<Self>, _: FopStateContext) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>, FopStateNodeError>>>> {
        Box::pin(async move {
            Err(FopStateNodeError::new(
                "auto_retransmit_enable is not allowed in lockout state",
                self as Box<dyn FopStateNode>,
            ))
        })
    }
    fn send_set_vr_command(&mut self, _: FopStateContext, _: Box<dyn tc::SyncAndChannelCoding + Send + Sync >, _: u8) -> Pin<Box<dyn Future<Output = Result<()>>>> {
        Box::pin(async { Err(anyhow!("send_set_vr_command is not allowed in lockout state")) })
    }
    fn send_unlock_command(&self, _: FopStateContext, _: Box<dyn tc::SyncAndChannelCoding + Send + Sync >) -> Pin<Box<dyn Future<Output = Result<()>>>> {
        Box::pin(async { Err(anyhow!("send_unlock_command is not allowed in lockout state")) })
    }

    fn append (self: Box<Self>, _: FopStateContext, _: CommandContext) -> Pin<Box<dyn Future<Output = Result<(Box<dyn FopStateNode>, Option <CopTaskId>), FopStateNodeError>>>> {
        Box::pin(async {
            Err(FopStateNodeError::new(
                "append is not allowed in lockout state",
                self as Box<dyn FopStateNode>,
            ))
        })
    }
    fn execute (self: Box<Self>, _: FopStateContext, _: Box<dyn tc::SyncAndChannelCoding + Send + Sync>) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async { self as Box<dyn FopStateNode> })
    }
    fn get_next_id(&self) -> Option<CopTaskId> {
        None
    }

    fn send_status(self: Box<Self>, context: FopStateContext) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async move {
            context.send_worker_status(CopWorkerStatusPattern::WorkerLockout).await;
            self as Box<dyn FopStateNode>
        })
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
    fn evaluate_timeout(self: Box<Self>, context: FopStateContext) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async move{
            let now = chrono::Utc::now();
            if now - self.start_time > chrono::TimeDelta::seconds(context.timeout_sec as i64) {
                tokio::spawn(async move { 
                    context.send_worker_status(CopWorkerStatusPattern::WorkerTimeout).await;
                    tokio::time::sleep(tokio::time::Duration::from_nanos(1)).await;
                    context.send_worker_status(CopWorkerStatusPattern::WorkerLockout).await;
                });
                Box::new(FopStateLockout) as Box<dyn FopStateNode> 
            } else {
                self as Box<dyn FopStateNode>
            }
        })
    }

    fn clcw_received(self: Box<Self>, _: FopStateContext) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async { self as Box<dyn FopStateNode> })
    }
    fn lockout(self: Box<Self>, context: FopStateContext, flag: bool) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async move {
            if flag {
                self as Box<dyn FopStateNode>
            } else {
                context.send_worker_status(CopWorkerStatusPattern::WorkerIdle).await;
                Box::new(FopStateIdle) as Box<dyn FopStateNode>
            }
        })
    }
    fn vsvr_matched(self: Box<Self>, _: FopStateContext, _: u8) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async { self as Box<dyn FopStateNode> })
    }
    fn accept (self: Box<Self>, _: FopStateContext, _: u8) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async { self as Box<dyn FopStateNode> })
    }
    fn reject (self: Box<Self>, _: FopStateContext) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async { self as Box<dyn FopStateNode> })
    }

    fn terminate(self: Box<Self>, context: FopStateContext) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>, FopStateNodeError>>>> {
        Box::pin(async move {
            context.send_worker_status(CopWorkerStatusPattern::WorkerCanceled).await;
            tokio::time::sleep(tokio::time::Duration::from_nanos(1)).await;
            context.send_worker_status(CopWorkerStatusPattern::WorkerLockout).await;
            Ok(Box::new(FopStateLockout) as Box<dyn FopStateNode>) 
        })
    }
    fn start_unlocking(self: Box<Self>, _: FopStateContext) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>, FopStateNodeError>>>> {
        Box::pin(async move {
            Err(FopStateNodeError::new(
                "start_unlocking is not allowed in unlocking state",
                self as Box<dyn FopStateNode>,
            ))
        })
    }
    fn start_initializing(self: Box<Self>, _: FopStateContext, _: u8) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>, FopStateNodeError>>>> {
        Box::pin(async move {
            Err(FopStateNodeError::new(
                "start_initializing is not allowed in unlocking state",
                self as Box<dyn FopStateNode>,
            ))
        })
    }
    fn auto_retransmit_disable(self: Box<Self>, context: FopStateContext) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>, FopStateNodeError>>>> {
        Box::pin(async move {
            context.send_worker_status(CopWorkerStatusPattern::WorkerAutoRetransmitOff).await;
            Ok(Box::new(FopStateAutoRetransmitOff::new()) as Box<dyn FopStateNode>)
        })
    }
    fn auto_retransmit_enable(self: Box<Self>, _: FopStateContext) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>, FopStateNodeError>>>> {
        Box::pin(async {
            Err(FopStateNodeError::new(
                "auto_retransmit_enable is not allowed in unlocking state",
                self as Box<dyn FopStateNode>,
            ))
        })
    }
    fn send_set_vr_command(&mut self, _: FopStateContext, _: Box<dyn tc::SyncAndChannelCoding + Send + Sync >, _: u8) -> Pin<Box<dyn Future<Output = Result<()>>>> {
        Box::pin(async { Err(anyhow!("send_set_vr_command is not allowed in unlocking state")) })
    }
    fn send_unlock_command(&self, _: FopStateContext, _: Box<dyn tc::SyncAndChannelCoding + Send + Sync >) -> Pin<Box<dyn Future<Output = Result<()>>>> {
        Box::pin(async { Err(anyhow!("send_unlock_command is not allowed in unlocking state")) })
    }

    fn append (self: Box<Self>, _: FopStateContext, _: CommandContext) -> Pin<Box<dyn Future<Output = Result<(Box<dyn FopStateNode>, Option<CopTaskId>), FopStateNodeError>>>> {
        Box::pin(async {
            Err(FopStateNodeError::new(
                "append is not allowed in unlocking state",
                self as Box<dyn FopStateNode>,
            ))
        })
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
                    context.send_worker_status(CopWorkerStatusPattern::WorkerFailed).await;
                    tokio::time::sleep(tokio::time::Duration::from_nanos(1)).await;
                    context.send_worker_status(CopWorkerStatusPattern::WorkerLockout).await;
                    Box::new(FopStateLockout) as Box<dyn FopStateNode>
                }
            } 
        })
    }
    fn get_next_id(&self) -> Option<CopTaskId> {
        None
    }

    fn send_status(self: Box<Self>, context: FopStateContext) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async move {
            context.send_worker_status(CopWorkerStatusPattern::WorkerUnlocking).await;
            self as Box<dyn FopStateNode>
        })
    }
}

struct FopStateClcwUnreceived;

impl Display for FopStateClcwUnreceived {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CLCW Unreceived")
    }
}

impl FopStateNode for FopStateClcwUnreceived {
    fn evaluate_timeout(self: Box<Self>, _: FopStateContext) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async move{ self as Box<dyn FopStateNode> })
    }  

    fn clcw_received(self: Box<Self>, context: FopStateContext) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async move {
            context.send_worker_status(CopWorkerStatusPattern::WorkerIdle).await;
            Box::new(FopStateIdle) as Box<dyn FopStateNode>
        })
    }
    fn lockout(self: Box<Self>, context: FopStateContext, flag: bool) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async move {
            if flag {
                context.send_worker_status(CopWorkerStatusPattern::WorkerLockout).await;
                Box::new(FopStateLockout) as Box<dyn FopStateNode>
            } else {
                context.send_worker_status(CopWorkerStatusPattern::WorkerIdle).await;
                Box::new(FopStateIdle) as Box<dyn FopStateNode>
            }
        })
    }
    fn vsvr_matched(self: Box<Self>, context: FopStateContext, _: u8) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async move {
            context.send_worker_status(CopWorkerStatusPattern::WorkerIdle).await;
            Box::new(FopStateIdle) as Box<dyn FopStateNode>
        })
    }
    fn accept (self: Box<Self>, _: FopStateContext, _: u8) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async { self as Box<dyn FopStateNode> })
    }
    fn reject (self: Box<Self>, _: FopStateContext) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async { self as Box<dyn FopStateNode> })
    }

    fn terminate(self: Box<Self>, _: FopStateContext) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>, FopStateNodeError>>>> {
        Box::pin(async {
            Err(FopStateNodeError::new(
                "cancel is not allowed in clcw_unreceived state",
                self as Box<dyn FopStateNode>,
            ))
        })
    }
    fn start_unlocking(self: Box<Self>, _: FopStateContext) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>, FopStateNodeError>>>> {
        Box::pin(async move {
            Err(FopStateNodeError::new(
                "start_unlocking is not allowed in clcw_unreceived state",
                self as Box<dyn FopStateNode>,
            ))
        })
    }
    fn start_initializing(self: Box<Self>, _: FopStateContext, _: u8) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>, FopStateNodeError>>>> {
        Box::pin(async {
            Err(FopStateNodeError::new(
                "start_initializing is not allowed in clcw_unreceived state",
                self as Box<dyn FopStateNode>,
            ))
        })
    }
    fn auto_retransmit_disable(self: Box<Self>, context: FopStateContext) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>, FopStateNodeError>>>> {
        Box::pin(async move {
            context.send_worker_status(CopWorkerStatusPattern::WorkerAutoRetransmitOff).await;
            Ok(Box::new(FopStateAutoRetransmitOff::new()) as Box<dyn FopStateNode>)
        })
    }
    fn auto_retransmit_enable(self: Box<Self>, _: FopStateContext) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>, FopStateNodeError>>>> {
        Box::pin(async {
            Err(FopStateNodeError::new(
                "auto_retransmit_enable is not allowed in clcw_unreceived state",
                self as Box<dyn FopStateNode>,
            ))
        })
    }
    fn send_set_vr_command(&mut self, _: FopStateContext, _: Box<dyn tc::SyncAndChannelCoding + Send + Sync >, _: u8) -> Pin<Box<dyn Future<Output = Result<()>>>> {
        Box::pin(async { Err(anyhow!("send_set_vr_command is not allowed in clcw_unreceived state")) })
    }
    fn send_unlock_command(&self, _: FopStateContext, _: Box<dyn tc::SyncAndChannelCoding + Send + Sync >) -> Pin<Box<dyn Future<Output = Result<()>>>> {
        Box::pin(async { Err(anyhow!("send_unlock_command is not allowed in clcw_unreceived state")) })
    }

    fn append (self: Box<Self>, _: FopStateContext, _: CommandContext) -> Pin<Box<dyn Future<Output = Result<(Box<dyn FopStateNode>, Option<CopTaskId>), FopStateNodeError>>>> {
        Box::pin(async {
            Err(FopStateNodeError::new(
                "append is not allowed in clcw_unreceived state",
                self as Box<dyn FopStateNode>,
            ))
        })
    }

    fn execute (self: Box<Self>, _: FopStateContext, _: Box<dyn tc::SyncAndChannelCoding + Send + Sync>) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async { self as Box<dyn FopStateNode> })
    }
    fn get_next_id(&self) -> Option<CopTaskId> {
        None
    }

    fn send_status(self: Box<Self>, context: FopStateContext) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async move {
            context.send_worker_status(CopWorkerStatusPattern::WorkerClcwUnreceived).await;
            self as Box<dyn FopStateNode>
        })
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
    fn evaluate_timeout(self: Box<Self>, context: FopStateContext) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async move{
            let now = chrono::Utc::now();
            if now - self.start_time > chrono::TimeDelta::seconds(context.timeout_sec as i64) {
                context.send_worker_status(CopWorkerStatusPattern::WorkerTimeout).await;
                Box::new(FopStateIdle) as Box<dyn FopStateNode>
            } else {
                self as Box<dyn FopStateNode>
            }
        })
    }

    fn clcw_received(self: Box<Self>, _: FopStateContext) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async { self as Box<dyn FopStateNode> })
    }
    fn lockout(self: Box<Self>, context: FopStateContext, flag: bool) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async move {
            if flag {
                context.send_worker_status(CopWorkerStatusPattern::WorkerLockout).await;
                Box::new(FopStateLockout) as Box<dyn FopStateNode>
            } else {
                self as Box<dyn FopStateNode>
            }
        })
    }
    fn vsvr_matched(self: Box<Self>, context: FopStateContext, vr: u8) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async move {
            if vr == self.vsvr {
                context.send_worker_status(CopWorkerStatusPattern::WorkerActive).await;
                Box::new(FopStateActive::new(self.vsvr, context.next_id, context.get_queue_context()).await) as Box<dyn FopStateNode>
            } else {
                self as Box<dyn FopStateNode>
            }
        })
    }
    fn accept (self: Box<Self>, _: FopStateContext, _: u8) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async { self as Box<dyn FopStateNode> })
    }
    fn reject (self: Box<Self>, _: FopStateContext) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async { self as Box<dyn FopStateNode> })
    }

    fn terminate(self: Box<Self>, context: FopStateContext) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>, FopStateNodeError>>>> {
        Box::pin(async move {
            context.send_worker_status(CopWorkerStatusPattern::WorkerCanceled).await;
            Ok(Box::new(FopStateIdle) as Box<dyn FopStateNode>)
        })
    }
    fn start_unlocking(self: Box<Self>, _: FopStateContext) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>, FopStateNodeError>>>> {
        Box::pin(async move {
            Err(FopStateNodeError::new(
                "start_unlocking is not allowed in initialize state",
                self as Box<dyn FopStateNode>,
            ))
        })
    }
    fn start_initializing(self: Box<Self>, _: FopStateContext, _: u8) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>, FopStateNodeError>>>> {
        Box::pin(async move {
            Err(FopStateNodeError::new(
                "start_initializing is not allowed in initialize state",
                self as Box<dyn FopStateNode>,
            ))
        })
    }
    fn auto_retransmit_enable(self: Box<Self>, _: FopStateContext) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>, FopStateNodeError>>>> {
        Box::pin(async {
            Err(FopStateNodeError::new(
                "auto_retransmit_enable is not allowed in initialize state",
                self as Box<dyn FopStateNode>,
            ))
        })
    }
    fn auto_retransmit_disable(self: Box<Self>, context: FopStateContext) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>, FopStateNodeError>>>>{
        Box::pin(async move {
            context.send_worker_status(CopWorkerStatusPattern::WorkerAutoRetransmitOff).await;
            Ok(Box::new(FopStateAutoRetransmitOff::new()) as Box<dyn FopStateNode>)
        })
    }
    fn send_set_vr_command(&mut self, _: FopStateContext, _: Box<dyn tc::SyncAndChannelCoding + Send + Sync >,  _: u8) -> Pin<Box<dyn Future<Output = Result<()>>>> {
        Box::pin(async { Err(anyhow!("send_set_vr_command is not allowed in initialize state")) })
    }
    fn send_unlock_command(&self, _: FopStateContext, _: Box<dyn tc::SyncAndChannelCoding + Send + Sync >) -> Pin<Box<dyn Future<Output = Result<()>>>> {
        Box::pin(async { Err(anyhow!("send_unlock_command is not allowed in initialize state")) })
    }

    fn append (self: Box<Self>, _: FopStateContext, _: CommandContext) -> Pin<Box<dyn Future<Output = Result<(Box<dyn FopStateNode>, Option<CopTaskId>), FopStateNodeError>>>> {
        Box::pin(async {
            Err(FopStateNodeError::new(
                "append is not allowed in initialize state",
                self as Box<dyn FopStateNode>,
            ))
        })
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
                    context.send_worker_status(CopWorkerStatusPattern::WorkerFailed).await;
                    Box::new(FopStateIdle) as Box<dyn FopStateNode>
                }
            }
        })
    }
    fn get_next_id(&self) -> Option<CopTaskId> {
        None
    }

    fn send_status(self: Box<Self>, context: FopStateContext) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async move {
            context.send_worker_status(CopWorkerStatusPattern::WorkerInitialize).await;
            self as Box<dyn FopStateNode>
        })
    }
}

struct FopStateActive{
    queue: FopQueue,
}

impl FopStateActive {
    async fn new(vs: u8, next_id: CopTaskId, ctx: FopQueueContext) -> Self {
        Self {
            queue: FopQueue::new(vs, next_id, ctx).await,
        }
    }
}

impl Display for FopStateActive {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Active")
    }
}

impl FopStateNode for FopStateActive {
    fn evaluate_timeout(mut self: Box<Self>, context: FopStateContext) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async move{ 
            let now = chrono::Utc::now();
            let oldest_arrival_time = match self.queue.get_last_time() {
                Some(time) => time,
                None => return self as Box<dyn FopStateNode>,
            };
            if now - oldest_arrival_time > chrono::TimeDelta::seconds(context.timeout_sec as i64) { 
                context.send_worker_status(CopWorkerStatusPattern::WorkerTimeout).await;
                self.queue.clear(context.get_queue_context(), CopTaskStatusPattern::Timeout).await;
                Box::new(FopStateIdle) as Box<dyn FopStateNode> 
            } else {
                self as Box<dyn FopStateNode>
            }
        })
    }

    fn clcw_received(self: Box<Self>, _: FopStateContext) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async { self as Box<dyn FopStateNode> })
    }
    fn lockout(mut self: Box<Self>, context: FopStateContext, flag: bool) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async move {
            if flag {
                self.queue.clear(context.get_queue_context(), CopTaskStatusPattern::Lockout).await;
                context.send_worker_status(CopWorkerStatusPattern::WorkerLockout).await;
                Box::new(FopStateLockout) as Box<dyn FopStateNode>
            } else {
                self as Box<dyn FopStateNode>
            }
        })
    }
    fn vsvr_matched(self: Box<Self>, _: FopStateContext, _: u8) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async { self as Box<dyn FopStateNode> })
    }
    fn accept (mut self: Box<Self>, context: FopStateContext, vr: u8) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async move {
            self.queue.accept(context.get_queue_context(), vr).await;
            self as Box<dyn FopStateNode>
        })
    }
    fn reject (mut self: Box<Self>, context: FopStateContext) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async move {
            self.queue.reject(context.get_queue_context()).await;
            self as Box<dyn FopStateNode>
        })
    }

    fn terminate(mut self: Box<Self>, context: FopStateContext) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>, FopStateNodeError>>>> {
        Box::pin(async move {
            self.queue.clear(context.get_queue_context(), CopTaskStatusPattern::Canceled).await;
            context.send_worker_status(CopWorkerStatusPattern::WorkerCanceled).await;
            Ok(Box::new(FopStateIdle) as Box<dyn FopStateNode>)
        })
    }
    fn start_unlocking(self: Box<Self>, _: FopStateContext) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>, FopStateNodeError>>>> {
        Box::pin(async {
            Err(FopStateNodeError::new(
                "start_unlocking is not allowed in active state",
                self as Box<dyn FopStateNode>,
            ))
        })
    }
    fn start_initializing(mut self: Box<Self>, context: FopStateContext, vsvr: u8) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>, FopStateNodeError>>>> {
        Box::pin(async move {
            self.queue.clear(context.get_queue_context(), CopTaskStatusPattern::Canceled).await;
            context.send_worker_status(CopWorkerStatusPattern::WorkerCanceled).await;
            tokio::time::sleep(tokio::time::Duration::from_nanos(1)).await;
            context.send_worker_status(CopWorkerStatusPattern::WorkerInitialize).await;
            Ok(Box::new(FopStateInitialize::new(vsvr)) as Box<dyn FopStateNode>)
        })
    }
    fn auto_retransmit_enable(self: Box<Self>, _: FopStateContext) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>, FopStateNodeError>>>> {
        Box::pin(async {
            Err(FopStateNodeError::new(
                "auto_retransmit_enable is not allowed in active state",
                self as Box<dyn FopStateNode>,
            ))
        })
    }
    fn auto_retransmit_disable(mut self: Box<Self>, context: FopStateContext) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>, FopStateNodeError>>>> {
        Box::pin(async move {
            self.queue.clear(context.get_queue_context(), CopTaskStatusPattern::Canceled).await;
            context.send_worker_status(CopWorkerStatusPattern::WorkerAutoRetransmitOff).await;
            Ok(Box::new(FopStateAutoRetransmitOff::new()) as Box<dyn FopStateNode>)
        })
    }
    fn send_set_vr_command(&mut self, _: FopStateContext, _: Box<dyn tc::SyncAndChannelCoding + Send + Sync>, _: u8) -> Pin<Box<dyn Future<Output = Result<()>>>> {
        Box::pin(async { Err(anyhow!("send_set_vr_command is not allowed in active state")) })
    }
    fn send_unlock_command(&self, _: FopStateContext, _: Box<dyn tc::SyncAndChannelCoding + Send + Sync>) -> Pin<Box<dyn Future<Output = Result<()>>>> {
        Box::pin(async { Err(anyhow!("send_unlock_command is not allowed in active state")) })
    }

    fn append (mut self: Box<Self>, context: FopStateContext, cmd_ctx: CommandContext) -> Pin<Box<dyn Future<Output = Result<(Box<dyn FopStateNode>, Option<CopTaskId>), FopStateNodeError>>>> {
        Box::pin(async move {
            let queue_ctx = context.get_queue_context();
            let ret = self.queue.push(queue_ctx, cmd_ctx).await;
            Ok((
                self as Box<dyn FopStateNode>,
                Some(ret)
            ))
        })
    }

    fn execute (mut self: Box<Self>, context: FopStateContext, mut sync_and_channel_coding: Box<dyn tc::SyncAndChannelCoding + Send + Sync>) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async move {
            let (vs, ctx) = match self.queue.execute(context.get_queue_context()).await {
                Some((vs, ctx)) => (vs, ctx),
                None => return self as Box<dyn FopStateNode>,
            };
            match ctx.transmit_to(sync_and_channel_coding.as_mut(), Some(vs)).await {
                Ok(_) => self as Box<dyn FopStateNode>,
                Err(e) => {
                    error!("failed to transmit COP: {}", e);
                    context.send_worker_status(CopWorkerStatusPattern::WorkerFailed).await;
                    Box::new(FopStateIdle) as Box<dyn FopStateNode>
                }
            }
        })
    }
    fn get_next_id(&self) -> Option<CopTaskId> {
        Some(self.queue.next_id())
    }

    fn send_status(self: Box<Self>, context: FopStateContext) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async move {
            self.queue.send_status(context.get_queue_context().queue_status_tx).await;
            context.send_worker_status(CopWorkerStatusPattern::WorkerActive).await;
            self as Box<dyn FopStateNode>
        })
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

fn create_queue_status(vs: u8) -> CopQueueStatusSet {
    let now = chrono::Utc::now().naive_utc();
    let timestamp = Some(Timestamp {
        seconds: now.and_utc().timestamp(),
        nanos: now.and_utc().timestamp_subsec_nanos() as i32,
    });
    CopQueueStatusSet {
        pending: Some(CopQueueStatus { head_id: None, head_tco_name: None, task_count: 0 }),
        executed: Some(CopQueueStatus { head_id: None, head_tco_name: None, task_count: 0 }),
        rejected: Some(CopQueueStatus { head_id: None, head_tco_name: None, task_count: 0 }),
        head_vs: vs as u32,
        vs_at_id0: 0,
        oldest_arrival_time: None,
        timestamp,
        confirm_inc: 0,
    }
}

impl FopStateNode for FopStateAutoRetransmitOff {
    fn evaluate_timeout(self: Box<Self>, _: FopStateContext) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async { self as Box<dyn FopStateNode> })
    }

    fn clcw_received(self: Box<Self>, _: FopStateContext) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async { self as Box<dyn FopStateNode> })
    }
    fn lockout(self: Box<Self>, _: FopStateContext, _: bool) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async { self as Box<dyn FopStateNode> })
    }
    fn vsvr_matched(self: Box<Self>, _: FopStateContext, _: u8) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async { self as Box<dyn FopStateNode> })
    }
    fn accept (self: Box<Self>, _: FopStateContext, _: u8) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async { self as Box<dyn FopStateNode> })
    }
    fn reject (self: Box<Self>, _: FopStateContext) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async { self as Box<dyn FopStateNode> })
    }

    fn terminate(self: Box<Self>, _: FopStateContext) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>, FopStateNodeError>>>> {
        Box::pin(async {
            Err(FopStateNodeError::new(
                "cancel is not allowed in auto_retransmit_off state",
                self as Box<dyn FopStateNode>,
            ))
        })
    }
    fn start_unlocking(self: Box<Self>, _: FopStateContext) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>, FopStateNodeError>>>> {
        Box::pin(async {
            Err(FopStateNodeError::new(
                "start_unlocking is not allowed in auto_retransmit_off state",
                self as Box<dyn FopStateNode>,
            ))
        })
    }
    fn start_initializing(self: Box<Self>, _: FopStateContext, _: u8) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>, FopStateNodeError>>>> {
        Box::pin(async {
            Err(FopStateNodeError::new(
                "start_initializing is not allowed in auto_retransmit_off state",
                self as Box<dyn FopStateNode>,
            ))
        })
    }
    fn auto_retransmit_enable(self: Box<Self>, context: FopStateContext) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>, FopStateNodeError>>>> {
        Box::pin(async move {
            context.send_worker_status(CopWorkerStatusPattern::WorkerClcwUnreceived).await;
            Ok(Box::new(FopStateClcwUnreceived) as Box<dyn FopStateNode>)
        })
        
    }
    fn auto_retransmit_disable(self: Box<Self>, _: FopStateContext) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>, FopStateNodeError>>>> {
        Box::pin(async {
            Err(FopStateNodeError::new(
                "auto_retransmit_disable is not allowed in auto_retransmit_off state",
                self as Box<dyn FopStateNode>,
            ))
        })
    }
    fn send_set_vr_command(&mut self, context: FopStateContext, mut sync_and_channel_coding: Box<dyn tc::SyncAndChannelCoding + Send + Sync>, vsvr: u8) -> Pin<Box<dyn Future<Output = Result<()>>>> {
        self.next_vs = vsvr;
        self.queue.drain(..);
        Box::pin(async move { 
            if let Err(e) = context.queue_status_tx.send(create_queue_status(vsvr)).await {
                error!("failed to send queue status: {}", e);
            }
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

    fn append (mut self: Box<Self>, _: FopStateContext, cmd_ctx: CommandContext) -> Pin<Box<dyn Future<Output = Result<(Box<dyn FopStateNode>,Option<CopTaskId>), FopStateNodeError>>>> {
        Box::pin(async move {
            self.queue.push_back((self.next_vs, cmd_ctx));
            self.next_vs = self.next_vs.wrapping_add(1);
            Ok((self as Box<dyn FopStateNode>, None))
        })
    }
    fn execute (mut self: Box<Self>, context: FopStateContext, mut sync_and_channel_coding: Box<dyn tc::SyncAndChannelCoding + Send + Sync + 'static>) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async move {
            let (vs, ctx) = match self.queue.pop_front() {
                Some((vs, ctx)) => (vs, ctx),
                None => return self as Box<dyn FopStateNode>,
            };
            if let Err(e) = context.queue_status_tx.send(create_queue_status(vs.wrapping_add(1))).await {
                error!("failed to send queue status: {}", e);
            }
            match ctx.transmit_to(sync_and_channel_coding.as_mut(), Some(vs)).await {
                Ok(_) => self as Box<dyn FopStateNode>,
                Err(e) => {
                    error!("failed to transmit COP: {}", e);
                    context.send_worker_status(CopWorkerStatusPattern::WorkerFailed).await;
                    Box::new(FopStateIdle) as Box<dyn FopStateNode>
                }
            }
        })
    }
    fn get_next_id(&self) -> Option<CopTaskId> {
        None
    }

    fn send_status(self: Box<Self>, context: FopStateContext) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async move {
            if let Err(e) = context.queue_status_tx.send(create_queue_status(self.next_vs)).await {
                error!("failed to send queue status: {}", e);
            }
            context.send_worker_status(CopWorkerStatusPattern::WorkerAutoRetransmitOff).await;
            self as Box<dyn FopStateNode>
        })
    }
}

pub struct FopStateMachine<T> 
where 
    T: tc::SyncAndChannelCoding + Clone + Send + Sync + 'static,
{
    inner: Option<Box<dyn FopStateNode>>,
    timeout_sec: u32,
    max_executing: usize,
    tc_scid: u16,
    next_id: CopTaskId,
    worker_state_tx: mpsc::Sender<CopWorkerStatus>,
    queue_status_tx: mpsc::Sender<CopQueueStatusSet>,
    task_status_tx: mpsc::Sender<CopTaskStatus>,
    sync_and_channel_coding: T,
}

impl<T> FopStateMachine<T> 
where 
    T: tc::SyncAndChannelCoding + Clone + Send + Sync + 'static,
{
    pub fn new(
        worker_state_tx: mpsc::Sender<CopWorkerStatus>,
        queue_status_tx: mpsc::Sender<CopQueueStatusSet>,
        task_status_tx: mpsc::Sender<CopTaskStatus>,
        sync_and_channel_coding: T,
        tc_scid: u16,
    ) -> Self {
        Self {
            inner: Some(Box::new(FopStateClcwUnreceived) as Box<dyn FopStateNode>),
            worker_state_tx,
            queue_status_tx,
            task_status_tx,
            timeout_sec: 20,
            max_executing: 10,
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
            max_executing: self.max_executing,
            next_id: self.next_id,
            tc_scid: self.tc_scid,
        }
    }

    pub async fn evaluate_timeout(&mut self) {
        let context = self.get_context();
        self.inner = match self.inner.take(){
            Some(state) => Some(state.evaluate_timeout(context).await),
            None => unreachable!(),
        };
    }
    pub async fn clcw_received(&mut self) {
        let context = self.get_context();
        self.inner = match self.inner.take(){
            Some(state) => Some(state.clcw_received(context).await),
            None => unreachable!(),
        };
    }
    pub async fn set_clcw(&mut self, clcw: &CLCW) {
        let context = self.get_context();
        self.inner = match self.inner.take(){
            Some(state) => Some(state.vsvr_matched(context.clone(), clcw.report_value()).await),
            None => unreachable!(),
        };
        self.inner = match self.inner.take(){
            Some(state) => Some(state.accept(context.clone(), clcw.report_value()).await),
            None => unreachable!(),
        };
        if clcw.retransmit() == 1 {
            self.inner = match self.inner.take(){
                Some(state) => Some(state.reject(context.clone()).await),
                None => unreachable!(),
            };
        }
        self.inner = match self.inner.take(){
            Some(state) => Some(state.lockout(context.clone(), clcw.lockout() == 1).await),
            None => unreachable!(),
        };
    }
    pub async fn cancel(&mut self) -> Result<()> {
        let context = self.get_context();
        let res = match self.inner.take(){
            Some(state) => state.terminate(context).await,
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
    pub async fn start_unlocking(&mut self) -> Result<()> {
        let context = self.get_context();
        let res = match self.inner.take(){
            Some(state) => state.start_unlocking(context).await,
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
    pub async fn start_initializing(&mut self, vsvr: u8) -> Result<()> {
        let context = self.get_context();
        let res = match self.inner.take(){
            Some(state) => state.start_initializing(context, vsvr).await,
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
    pub async fn auto_retransmit_enable(&mut self) -> Result<()> {
        let context = self.get_context();
        let res = match self.inner.take(){
            Some(state) => state.auto_retransmit_enable(context).await,
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
    pub async fn auto_retransmit_disable(&mut self) -> Result<()> {
        let context = self.get_context();
        let res = match self.inner.take(){
            Some(state) => state.auto_retransmit_disable(context).await,
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
    pub async fn set_timeout_sec(&mut self, timeout_sec: u32) {
        self.timeout_sec = timeout_sec;
        self.send_status().await;
    }
    pub async fn set_max_executing(&mut self, max_executing: usize) {
        self.max_executing = max_executing;
        self.send_status().await;
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

    pub async fn append(&mut self, cmd_ctx: CommandContext) -> Result<Option<CopTaskId>> {
        let context = self.get_context();
        match self.inner.take(){
            Some(state) => {
                let ret = state.append(context, cmd_ctx).await;
                match ret {
                    Ok((state, id)) => {
                        let next_id = state.get_next_id();
                        self.inner = Some(state);
                        self.next_id = next_id.unwrap_or(self.next_id);
                        Ok(id)
                    },
                    Err(e) => {
                        let next_id = e.state.get_next_id();
                        self.inner = Some(e.state);
                        self.next_id = next_id.unwrap_or(self.next_id);
                        Err(e.message)
                    },
                }
            },
            None => unreachable!(),
        }
    }
    pub async fn execute(&mut self) {
        let context = self.get_context();
        self.inner = match self.inner.take(){
            Some(state) => Some(state.execute(context, Box::new(self.sync_and_channel_coding.clone())).await),
            None => unreachable!(),
        };
    }

    pub async fn send_status(&mut self) {
        let context = self.get_context();
        self.inner = match self.inner.take(){
            Some(state) => Some(state.send_status(context).await),
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
    use tokio::sync::{mpsc, Mutex};

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
        let (worker_tx, _worker_rx) = mpsc::channel(16);
        let (queue_tx, _queue_rx) = mpsc::channel(16);
        let (task_tx, _task_rx) = mpsc::channel(16);

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

        fop_sm.evaluate_timeout().await;
        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "CLCW Unreceived");

        let res = fop_sm.cancel().await;
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "cancel is not allowed in clcw_unreceived state");
        assert!(fop_sm.inner.is_some());

        let res = fop_sm.start_unlocking().await;
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "start_unlocking is not allowed in clcw_unreceived state");
        assert!(fop_sm.inner.is_some());

        let res = fop_sm.start_initializing(0).await;
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "start_initializing is not allowed in clcw_unreceived state");
        assert!(fop_sm.inner.is_some());

        let res = fop_sm.auto_retransmit_enable().await;
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "auto_retransmit_enable is not allowed in clcw_unreceived state");

        let res = fop_sm.send_set_vr_command(0).await;
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "send_set_vr_command is not allowed in clcw_unreceived state");

        let res = fop_sm.send_unlock_command().await;
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "send_unlock_command is not allowed in clcw_unreceived state");

        let res = fop_sm.append(create_cmd_ctx()).await;
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
        let (worker_tx, mut worker_rx) = mpsc::channel(16);
        let (queue_tx, _queue_rx) = mpsc::channel(16);
        let (task_tx, _task_rx) = mpsc::channel(16);

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

        fop_sm.clcw_received().await;
        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Idle");

        assert_eq!(worker_rx.recv().await.unwrap().state, CopWorkerStatusPattern::WorkerIdle as i32);
        assert_eq!(worker_rx.try_recv().unwrap_err(), mpsc::error::TryRecvError::Empty);
    }

    #[tokio::test]
    async fn clcw_unreceived_lockout(){
        // ブロードキャストチャネルを作成
        let (worker_tx, mut worker_rx) = mpsc::channel(16);
        let (queue_tx, _queue_rx) = mpsc::channel(16);
        let (task_tx, _task_rx) = mpsc::channel(16);

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

        fop_sm.set_clcw(&create_clcw(0, true, false)).await;
        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Lockout");
        assert_eq!(worker_rx.recv().await.unwrap().state, CopWorkerStatusPattern::WorkerIdle as i32);
        assert_eq!(worker_rx.recv().await.unwrap().state, CopWorkerStatusPattern::WorkerLockout as i32);
        assert_eq!(worker_rx.try_recv().unwrap_err(), mpsc::error::TryRecvError::Empty);
    }

    #[tokio::test]
    async fn clcw_unreceived_set_clcw(){
        // ブロードキャストチャネルを作成
        let (worker_tx, mut worker_rx) = mpsc::channel(16);
        let (queue_tx, _queue_rx) = mpsc::channel(16);
        let (task_tx, _task_rx) = mpsc::channel(16);

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

        fop_sm.set_clcw(&create_clcw(0, false, false)).await;
        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Idle");

        assert_eq!(worker_rx.recv().await.unwrap().state, CopWorkerStatusPattern::WorkerIdle as i32);
        assert_eq!(worker_rx.try_recv().unwrap_err(), mpsc::error::TryRecvError::Empty);
    }

    #[tokio::test]
    async fn clcw_unreceived_auto_retransmit_disable() {
        // ブロードキャストチャネルを作成
        let (worker_tx, mut worker_rx) = mpsc::channel(16);
        let (queue_tx, _queue_rx) = mpsc::channel(16);
        let (task_tx, _task_rx) = mpsc::channel(16);

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

        let res = fop_sm.auto_retransmit_disable().await;
        assert!(res.is_ok());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Auto Retransmit Off");

        assert_eq!(worker_rx.recv().await.unwrap().state, CopWorkerStatusPattern::WorkerAutoRetransmitOff as i32);
        assert_eq!(worker_rx.try_recv().unwrap_err(), mpsc::error::TryRecvError::Empty);
    }

    #[tokio::test]
    async fn idle_without_transition() {
        // ブロードキャストチャネルを作成
        let (worker_tx, _worker_rx) = mpsc::channel(16);
        let (queue_tx, _queue_rx) = mpsc::channel(16);
        let (task_tx, _task_rx) = mpsc::channel(16);

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

        fop_sm.evaluate_timeout().await;
        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Idle");

        fop_sm.clcw_received().await;
        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Idle");

        fop_sm.set_clcw(&create_clcw(0, false, false)).await;
        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Idle");

        let res = fop_sm.cancel().await;
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "cancel is not allowed in idle state");
        assert!(fop_sm.inner.is_some());

        let res = fop_sm.start_unlocking().await;
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "start_unlocking is not allowed in idle state");
        assert!(fop_sm.inner.is_some());

        let res = fop_sm.auto_retransmit_enable().await;
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "auto_retransmit_enable is not allowed in idle state");

        let res = fop_sm.send_set_vr_command(0).await;
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "send_set_vr_command is not allowed in idle state");

        let res = fop_sm.send_unlock_command().await;
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "send_unlock_command is not allowed in idle state");

        let res = fop_sm.append(create_cmd_ctx()).await;
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
        let (worker_tx, mut worker_rx) = mpsc::channel(16);
        let (queue_tx, _queue_rx) = mpsc::channel(16);
        let (task_tx, _task_rx) = mpsc::channel(16);

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

        fop_sm.set_clcw(&create_clcw(0, true, false)).await;
        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Lockout");

        assert_eq!(worker_rx.recv().await.unwrap().state, CopWorkerStatusPattern::WorkerLockout as i32);
        assert_eq!(worker_rx.try_recv().unwrap_err(), mpsc::error::TryRecvError::Empty);
    }

    #[tokio::test]
    async fn idle_start_initialize(){
        // ブロードキャストチャネルを作成
        let (worker_tx, mut worker_rx) = mpsc::channel(16);
        let (queue_tx, _queue_rx) = mpsc::channel(16);
        let (task_tx, _task_rx) = mpsc::channel(16);

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

        let res = fop_sm.start_initializing(0).await;
        assert!(res.is_ok());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Initialize");

        assert_eq!(worker_rx.recv().await.unwrap().state, CopWorkerStatusPattern::WorkerInitialize as i32);
        assert_eq!(worker_rx.try_recv().unwrap_err(), mpsc::error::TryRecvError::Empty);
    }

    #[tokio::test]
    async fn idle_auto_retransmit_disable() {
        // ブロードキャストチャネルを作成
        let (worker_tx, mut worker_rx) = mpsc::channel(16);
        let (queue_tx, _queue_rx) = mpsc::channel(16);
        let (task_tx, _task_rx) = mpsc::channel(16);

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

        let res = fop_sm.auto_retransmit_disable().await;
        assert!(res.is_ok());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Auto Retransmit Off");

        assert_eq!(worker_rx.recv().await.unwrap().state, CopWorkerStatusPattern::WorkerAutoRetransmitOff as i32);
        assert_eq!(worker_rx.try_recv().unwrap_err(), mpsc::error::TryRecvError::Empty);
    }

    #[tokio::test]
    async fn lockout_without_transition() {
        // ブロードキャストチャネルを作成
        let (worker_tx, _worker_rx) = mpsc::channel(16);
        let (queue_tx, _queue_rx) = mpsc::channel(16);
        let (task_tx, _task_rx) = mpsc::channel(16);

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

        fop_sm.clcw_received().await;
        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Lockout");

        fop_sm.set_clcw(&create_clcw(0, true, false)).await;
        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Lockout");

        fop_sm.evaluate_timeout().await;
        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Lockout");

        let res = fop_sm.cancel().await;
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "cancel is not allowed in lockout state");
        assert!(fop_sm.inner.is_some());

        let res = fop_sm.start_initializing(0).await;
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "start_initializing is not allowed in lockout state");
        assert!(fop_sm.inner.is_some());

        let res = fop_sm.auto_retransmit_enable().await;
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "auto_retransmit_enable is not allowed in lockout state");

        let res = fop_sm.send_set_vr_command(0).await;
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "send_set_vr_command is not allowed in lockout state");

        let res = fop_sm.send_unlock_command().await;
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "send_unlock_command is not allowed in lockout state");

        let res = fop_sm.append(create_cmd_ctx()).await;
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
        let (worker_tx, mut worker_rx) = mpsc::channel(16);
        let (queue_tx, _queue_rx) = mpsc::channel(16);
        let (task_tx, _task_rx) = mpsc::channel(16);

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

        fop_sm.set_clcw(&create_clcw(0, false, false)).await;
        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Idle");

        assert_eq!(worker_rx.recv().await.unwrap().state, CopWorkerStatusPattern::WorkerIdle as i32);
    }

    #[tokio::test]
    async fn lockout_auto_retransmit_disable() {
        // ブロードキャストチャネルを作成
        let (worker_tx, mut worker_rx) = mpsc::channel(16);
        let (queue_tx, _queue_rx) = mpsc::channel(16);
        let (task_tx, _task_rx) = mpsc::channel(16);

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

        let res = fop_sm.auto_retransmit_disable().await;
        assert!(res.is_ok());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Auto Retransmit Off");

        assert_eq!(worker_rx.recv().await.unwrap().state, CopWorkerStatusPattern::WorkerAutoRetransmitOff as i32);
        assert_eq!(worker_rx.try_recv().unwrap_err(), mpsc::error::TryRecvError::Empty);
    }

    #[tokio::test]
    async fn lockout_start_unlocking(){
        // ブロードキャストチャネルを作成
        let (worker_tx, mut worker_rx) = mpsc::channel(16);
        let (queue_tx, _queue_rx) = mpsc::channel(16);
        let (task_tx, _task_rx) = mpsc::channel(16);

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

        let res = fop_sm.start_unlocking().await;
        assert!(res.is_ok());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Unlocking");

        assert_eq!(worker_rx.recv().await.unwrap().state, CopWorkerStatusPattern::WorkerUnlocking as i32);
        assert_eq!(worker_rx.try_recv().unwrap_err(), mpsc::error::TryRecvError::Empty);
    }

    #[tokio::test]
    async fn unlocking_without_transition() {
        // ブロードキャストチャネルを作成
        let (worker_tx, _worker_rx) = mpsc::channel(16);
        let (queue_tx, _queue_rx) = mpsc::channel(16);
        let (task_tx, _task_rx) = mpsc::channel(16);

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

        fop_sm.clcw_received().await;
        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Unlocking");

        fop_sm.set_clcw(&create_clcw(0, true, false)).await;
        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Unlocking");

        let res = fop_sm.start_unlocking().await;
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "start_unlocking is not allowed in unlocking state");
        assert!(fop_sm.inner.is_some());

        let res = fop_sm.start_initializing(0).await;
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "start_initializing is not allowed in unlocking state");
        assert!(fop_sm.inner.is_some());

        let res = fop_sm.auto_retransmit_enable().await;
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "auto_retransmit_enable is not allowed in unlocking state");

        let res = fop_sm.send_set_vr_command(0).await;
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "send_set_vr_command is not allowed in unlocking state");

        let res = fop_sm.send_unlock_command().await;
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "send_unlock_command is not allowed in unlocking state");

        let res = fop_sm.append(create_cmd_ctx()).await;
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
        let (worker_tx, mut worker_rx) = mpsc::channel(16);
        let (queue_tx, _queue_rx) = mpsc::channel(16);
        let (task_tx, _task_rx) = mpsc::channel(16);

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

        fop_sm.set_clcw(&create_clcw(0, false, false)).await;
        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Idle");

        assert_eq!(worker_rx.recv().await.unwrap().state, CopWorkerStatusPattern::WorkerIdle as i32);
        assert_eq!(worker_rx.try_recv().unwrap_err(), mpsc::error::TryRecvError::Empty);
    }

    #[tokio::test]
    async fn unlocking_cancelled() {
        // ブロードキャストチャネルを作成
        let (worker_tx, mut worker_rx) = mpsc::channel(16);
        let (queue_tx, _queue_rx) = mpsc::channel(16);
        let (task_tx, _task_rx) = mpsc::channel(16);

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

        let res = fop_sm.cancel().await;
        assert!(res.is_ok());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Lockout");

        assert_eq!(worker_rx.recv().await.unwrap().state, CopWorkerStatusPattern::WorkerCanceled as i32);
        assert_eq!(worker_rx.recv().await.unwrap().state, CopWorkerStatusPattern::WorkerLockout as i32);
        assert_eq!(worker_rx.try_recv().unwrap_err(), mpsc::error::TryRecvError::Empty);
    }

    #[tokio::test]
    async fn unlocking_auto_retransmit_disable() {
        // ブロードキャストチャネルを作成
        let (worker_tx, mut worker_rx) = mpsc::channel(16);
        let (queue_tx, _queue_rx) = mpsc::channel(16);
        let (task_tx, _task_rx) = mpsc::channel(16);

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

        let res = fop_sm.auto_retransmit_disable().await;
        assert!(res.is_ok());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Auto Retransmit Off");

        assert_eq!(worker_rx.recv().await.unwrap().state, CopWorkerStatusPattern::WorkerAutoRetransmitOff as i32);
        assert_eq!(worker_rx.try_recv().unwrap_err(), mpsc::error::TryRecvError::Empty);
    }

    #[tokio::test]
    async fn unlocking_timeout() {
        // ブロードキャストチャネルを作成
        let (worker_tx, mut worker_rx) = mpsc::channel(16);
        let (queue_tx, _queue_rx) = mpsc::channel(16);
        let (task_tx, _task_rx) = mpsc::channel(16);

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

        fop_sm.set_timeout_sec(1).await;

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        assert_eq!(worker_rx.recv().await.unwrap().state, CopWorkerStatusPattern::WorkerUnlocking as i32);

        fop_sm.evaluate_timeout().await;
        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Lockout");

        assert_eq!(worker_rx.recv().await.unwrap().state, CopWorkerStatusPattern::WorkerTimeout as i32);
        assert_eq!(worker_rx.recv().await.unwrap().state, CopWorkerStatusPattern::WorkerLockout as i32);
        assert_eq!(worker_rx.try_recv().unwrap_err(), mpsc::error::TryRecvError::Empty);
    }

    #[tokio::test]
    async fn initialize_without_transition() {
        // ブロードキャストチャネルを作成
        let (worker_tx, _worker_rx) = mpsc::channel(16);
        let (queue_tx, _queue_rx) = mpsc::channel(16);
        let (task_tx, _task_rx) = mpsc::channel(16);

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

        fop_sm.clcw_received().await;
        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Initialize");

        fop_sm.set_clcw(&create_clcw(0, false, false)).await;
        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Initialize");

        let res = fop_sm.start_initializing(0).await;
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "start_initializing is not allowed in initialize state");
        assert!(fop_sm.inner.is_some());

        let res = fop_sm.auto_retransmit_enable().await;
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "auto_retransmit_enable is not allowed in initialize state");

        let res = fop_sm.send_set_vr_command(0).await;
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "send_set_vr_command is not allowed in initialize state");

        let res = fop_sm.send_unlock_command().await;
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "send_unlock_command is not allowed in initialize state");

        let res = fop_sm.append(create_cmd_ctx()).await;
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
        let (worker_tx, mut worker_rx) = mpsc::channel(16);
        let (queue_tx, _queue_rx) = mpsc::channel(16);
        let (task_tx, _task_rx) = mpsc::channel(16);

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

        fop_sm.set_timeout_sec(1).await;

        assert_eq!(worker_rx.recv().await.unwrap().state, CopWorkerStatusPattern::WorkerInitialize as i32);
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        fop_sm.evaluate_timeout().await;
        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Idle");

        assert_eq!(worker_rx.recv().await.unwrap().state, CopWorkerStatusPattern::WorkerTimeout as i32);
        assert_eq!(worker_rx.try_recv().unwrap_err(), mpsc::error::TryRecvError::Empty);
    }

    #[tokio::test]
    async fn initialize_cancelled() {
        // ブロードキャストチャネルを作成
        let (worker_tx, mut worker_rx) = mpsc::channel(16);
        let (queue_tx, _queue_rx) = mpsc::channel(16);
        let (task_tx, _task_rx) = mpsc::channel(16);

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

        let res = fop_sm.cancel().await;
        assert!(res.is_ok());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Idle");

        assert_eq!(worker_rx.recv().await.unwrap().state, CopWorkerStatusPattern::WorkerCanceled as i32);
        assert_eq!(worker_rx.try_recv().unwrap_err(), mpsc::error::TryRecvError::Empty);
    }

    #[tokio::test]
    async fn initialize_auto_retransmit_disable() {
        // ブロードキャストチャネルを作成
        let (worker_tx, mut worker_rx) = mpsc::channel(16);
        let (queue_tx, _queue_rx) = mpsc::channel(16);
        let (task_tx, _task_rx) = mpsc::channel(16);

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

        let res = fop_sm.auto_retransmit_disable().await;
        assert!(res.is_ok());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Auto Retransmit Off");

        assert_eq!(worker_rx.recv().await.unwrap().state, CopWorkerStatusPattern::WorkerAutoRetransmitOff as i32);
        assert_eq!(worker_rx.try_recv().unwrap_err(), mpsc::error::TryRecvError::Empty);
    }

    #[tokio::test]
    async fn initialize_vsvr_matched() {
        // ブロードキャストチャネルを作成
        let (worker_tx, mut worker_rx) = mpsc::channel(16);
        let (queue_tx, _queue_rx) = mpsc::channel(16);
        let (task_tx, _task_rx) = mpsc::channel(16);

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

        fop_sm.set_clcw(&create_clcw(vsvr, false, false)).await;
        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Active");
        assert_eq!(fop_sm.next_id, 50);

        assert_eq!(worker_rx.recv().await.unwrap().state, CopWorkerStatusPattern::WorkerActive as i32);
        assert_eq!(worker_rx.try_recv().unwrap_err(), mpsc::error::TryRecvError::Empty);
    }

    #[tokio::test]
    async fn initialize_lockout() {
        // ブロードキャストチャネルを作成
        let (worker_tx, mut worker_rx) = mpsc::channel(16);
        let (queue_tx, _queue_rx) = mpsc::channel(16);
        let (task_tx, _task_rx) = mpsc::channel(16);

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

        fop_sm.set_clcw(&create_clcw(0, true, false)).await;
        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Lockout");

        assert_eq!(worker_rx.recv().await.unwrap().state, CopWorkerStatusPattern::WorkerLockout as i32);
        assert_eq!(worker_rx.try_recv().unwrap_err(), mpsc::error::TryRecvError::Empty);
    }

    #[tokio::test]
    async fn auto_retransmit_off_without_transition() {
        // ブロードキャストチャネルを作成
        let (worker_tx, _worker_rx) = mpsc::channel(16);
        let (queue_tx, mut queue_rx) = mpsc::channel(16);
        let (task_tx, _task_rx) = mpsc::channel(16);

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

        fop_sm.clcw_received().await;
        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Auto Retransmit Off");

        fop_sm.set_clcw(&create_clcw(0, false, false)).await;
        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Auto Retransmit Off");

        fop_sm.set_clcw(&create_clcw(0, true, false)).await;
        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Auto Retransmit Off");

        let res = fop_sm.cancel().await;
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "cancel is not allowed in auto_retransmit_off state");
        assert!(fop_sm.inner.is_some());

        let res = fop_sm.start_initializing(0).await;
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "start_initializing is not allowed in auto_retransmit_off state");
        assert!(fop_sm.inner.is_some());

        let res = fop_sm.auto_retransmit_disable().await;
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

        let res = queue_rx.try_recv();
        assert_eq!(res.unwrap().head_vs, vr as u32);

        let res = fop_sm.send_unlock_command().await;
        assert!(res.is_ok());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Auto Retransmit Off");
        assert_eq!(mock_sc.get_transmitted().await.len(), 2);
        assert_eq!(mock_sc.get_transmitted().await[1].0, 100);
        assert_eq!(mock_sc.get_transmitted().await[1].2, FrameType::TypeBC);
        assert_eq!(mock_sc.get_transmitted().await[1].4, vec![0u8]);

        let res = fop_sm.append(create_cmd_ctx()).await;
        assert!(res.is_ok());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Auto Retransmit Off");

        fop_sm.execute().await;
        assert_eq!(mock_sc.get_transmitted().await.len(), 3);
        assert_eq!(mock_sc.get_transmitted().await[2].0, 100);
        assert_eq!(mock_sc.get_transmitted().await[2].2, FrameType::TypeAD);        
        assert_eq!(mock_sc.get_transmitted().await[2].3, vr);

        let res = queue_rx.try_recv();
        assert_eq!(res.unwrap().head_vs, vr as u32 + 1);
    }

    #[tokio::test]
    async fn active_do_nothing() {
        // ブロードキャストチャネルを作成
        let (worker_tx, _worker_rx) = mpsc::channel(16);
        let (queue_tx, _queue_rx) = mpsc::channel(16);
        let (task_tx, _task_rx) = mpsc::channel(16);

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

        fop_sm.set_inner(Box::new(FopStateActive::new(vr, 50, fop_sm.get_context().get_queue_context()).await));

        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Active");

        fop_sm.clcw_received().await;
        assert!(fop_sm.inner.is_some());
        assert_eq!(fop_sm.inner.as_ref().unwrap().to_string(), "Active");

        let res = fop_sm.start_unlocking().await;
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "start_unlocking is not allowed in active state");

        let res = fop_sm.auto_retransmit_enable().await;
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
