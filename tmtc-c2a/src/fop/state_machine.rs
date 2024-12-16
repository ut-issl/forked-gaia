use std::{collections::VecDeque, future::Future, pin::Pin};

use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use gaia_ccsds_c2a::ccsds::tc::{self, clcw::CLCW, sync_and_channel_coding::FrameType};
use gaia_tmtc::cop::{CopQueueStatus, CopQueueStatusSet, CopTaskStatus, CopTaskStatusPattern, CopWorkerStatus, CopWorkerStatusPattern};
use prost_types::Timestamp;
use tokio::sync::broadcast;
use tracing::error;

use crate::satellite::CopTaskId;

use super::{queue::FopQueue, worker::CommandContext};

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

trait FopStateNode {
    fn evaluate_timeout(self: Box<Self>, context: FopStateContext) -> Box<dyn FopStateNode>;

    fn clcw_received(self: Box<Self>, context: FopStateContext) -> Box<dyn FopStateNode>;
    fn lockout(self: Box<Self>, context: FopStateContext, flag: bool) -> Box<dyn FopStateNode>;
    fn vsvr_matched(self: Box<Self>, context: FopStateContext, vr: u8) -> Box<dyn FopStateNode>;
    fn accept (&mut self, context: FopStateContext, vr: u8);
    fn reject (&mut self, context: FopStateContext);

    fn terminate(self: Box<Self>, context: FopStateContext) -> Result<Box<dyn FopStateNode>>;
    fn start_unlocking(self: Box<Self>, context: FopStateContext) -> Result<Box<dyn FopStateNode>>;
    fn start_initializing(self: Box<Self>, context: FopStateContext, vsvr: u8, confirmation_cmd: CommandContext) -> Result<Box<dyn FopStateNode>>;
    fn auto_retransmit_enable(self: Box<Self>, context: FopStateContext) -> Result<Box<dyn FopStateNode>>;
    fn auto_retransmit_disable(self: Box<Self>, context: FopStateContext) -> Result<Box<dyn FopStateNode>>;
    fn send_set_vr_command(&mut self, context: FopStateContext, sync_and_channel_coding: Box<dyn tc::SyncAndChannelCoding + Send + Sync>, vr: u8) -> Pin<Box<dyn Future<Output = Result<()>>>>;
    fn send_unlock_command(&self, context: FopStateContext, sync_and_channel_coding: Box<dyn tc::SyncAndChannelCoding + Send + Sync + 'static>) -> Pin<Box<dyn Future<Output = Result<()>>>>;

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
}

struct FopStateIdle;

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

    fn terminate(self: Box<Self>, _: FopStateContext) -> Result<Box<dyn FopStateNode>> {
        Err(anyhow!("cancel is not allowed in idle state"))
    }
    fn start_unlocking(self: Box<Self>, _: FopStateContext) -> Result<Box<dyn FopStateNode>> {
        Err(anyhow!("start_unlocking is not allowed in idle state"))
    }
    fn start_initializing(self: Box<Self>, context: FopStateContext, vsvr: u8, confirmation_cmd: CommandContext) -> Result<Box<dyn FopStateNode>> {
        context.send_worker_status(CopWorkerStatusPattern::WorkerInitialize);
        Ok(Box::new(FopStateInitialize::new(vsvr, confirmation_cmd)) as Box<dyn FopStateNode>)
    }
    fn auto_retransmit_disable(self: Box<Self>, context: FopStateContext) -> Result<Box<dyn FopStateNode>> {
        context.send_worker_status(CopWorkerStatusPattern::WorkerAutoRetransmitOff);
        Ok(Box::new(FopStateAutoRetransmitOff::new()) as Box<dyn FopStateNode>)
    }
    fn auto_retransmit_enable(self: Box<Self>, _: FopStateContext) -> Result<Box<dyn FopStateNode>> {
        Err(anyhow!("auto_retransmit_enable is not allowed in idle state"))
    }
    fn send_set_vr_command(&mut self, _: FopStateContext, _: Box<dyn tc::SyncAndChannelCoding + Send + Sync >, _: u8) -> Pin<Box<dyn Future<Output = Result<()>>>> {
        Box::pin(async { Err(anyhow!("send_set_vr_command is not allowed in idle state")) })
    }
    fn send_unlock_command(&self, _: FopStateContext, _: Box<dyn tc::SyncAndChannelCoding + Send + Sync >) -> Pin<Box<dyn Future<Output = Result<()>>>> {
        Box::pin(async { Err(anyhow!("send_unlock_command is not allowed in idle state")) })
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
    
    fn terminate(self: Box<Self>, _: FopStateContext) -> Result<Box<dyn FopStateNode>> {
        Err(anyhow!("cancel is not allowed in lockout state"))
    }
    fn start_unlocking(self: Box<Self>, context: FopStateContext) -> Result<Box<dyn FopStateNode>> {
        context.send_worker_status(CopWorkerStatusPattern::WorkerUnlocking);
        Ok(Box::new(FopStateUnlocking::new()) as Box<dyn FopStateNode>)
    }
    fn start_initializing(self: Box<Self>, _: FopStateContext, _: u8, _: CommandContext) -> Result<Box<dyn FopStateNode>> {
        Err(anyhow!("start_initializing is not allowed in lockout state"))
    }
    fn auto_retransmit_disable(self: Box<Self>, context: FopStateContext) -> Result<Box<dyn FopStateNode>> {
        context.send_worker_status(CopWorkerStatusPattern::WorkerAutoRetransmitOff);
        Ok(Box::new(FopStateAutoRetransmitOff::new()) as Box<dyn FopStateNode>)
    }
    fn auto_retransmit_enable(self: Box<Self>, _: FopStateContext) -> Result<Box<dyn FopStateNode>> {
        Err(anyhow!("auto_retransmit_enable is not allowed in lockout state"))
    }
    fn send_set_vr_command(&mut self, _: FopStateContext, _: Box<dyn tc::SyncAndChannelCoding + Send + Sync >, _: u8) -> Pin<Box<dyn Future<Output = Result<()>>>> {
        Box::pin(async { Err(anyhow!("send_set_vr_command is not allowed in lockout state")) })
    }
    fn send_unlock_command(&self, _: FopStateContext, _: Box<dyn tc::SyncAndChannelCoding + Send + Sync >) -> Pin<Box<dyn Future<Output = Result<()>>>> {
        Box::pin(async { Err(anyhow!("send_unlock_command is not allowed in lockout state")) })
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

impl FopStateNode for FopStateUnlocking {
    fn evaluate_timeout(self: Box<Self>, context: FopStateContext) -> Box<dyn FopStateNode> {
        let now = chrono::Utc::now();
        if now - self.start_time > chrono::TimeDelta::seconds(context.timeout_sec as i64) {
            tokio::spawn(async move { 
                context.send_worker_status(CopWorkerStatusPattern::WorkerTimeout);
                tokio::time::sleep(tokio::time::Duration::from_nanos(1)).await;
                context.send_worker_status(CopWorkerStatusPattern::WorkerLockout);
            });
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

    fn terminate(self: Box<Self>, context: FopStateContext) -> Result<Box<dyn FopStateNode>> {
        tokio::spawn(async move {
            context.send_worker_status(CopWorkerStatusPattern::WorkerCanceled);
            tokio::time::sleep(tokio::time::Duration::from_nanos(1)).await;
            context.send_worker_status(CopWorkerStatusPattern::WorkerLockout);
        });
        Ok(Box::new(FopStateLockout) as Box<dyn FopStateNode>) 
    }
    fn start_unlocking(self: Box<Self>, _: FopStateContext) -> Result<Box<dyn FopStateNode>> {
        Err(anyhow!("start_unlocking is not allowed in unlocking state"))
    }
    fn start_initializing(self: Box<Self>, _: FopStateContext, _: u8, _: CommandContext) -> Result<Box<dyn FopStateNode>> {
        Err(anyhow!("start_initializing is not allowed in unlocking state"))
    }
    fn auto_retransmit_disable(self: Box<Self>, context: FopStateContext) -> Result<Box<dyn FopStateNode>> {
        context.send_worker_status(CopWorkerStatusPattern::WorkerAutoRetransmitOff);
        Ok(Box::new(FopStateAutoRetransmitOff::new()) as Box<dyn FopStateNode>)
    }
    fn auto_retransmit_enable(self: Box<Self>, _: FopStateContext) -> Result<Box<dyn FopStateNode>> {
        Err(anyhow!("auto_retransmit_enable is not allowed in unlocking state"))
    }
    fn send_set_vr_command(&mut self, _: FopStateContext, _: Box<dyn tc::SyncAndChannelCoding + Send + Sync >, _: u8) -> Pin<Box<dyn Future<Output = Result<()>>>> {
        Box::pin(async { Err(anyhow!("send_set_vr_command is not allowed in unlocking state")) })
    }
    fn send_unlock_command(&self, _: FopStateContext, _: Box<dyn tc::SyncAndChannelCoding + Send + Sync >) -> Pin<Box<dyn Future<Output = Result<()>>>> {
        Box::pin(async { Err(anyhow!("send_unlock_command is not allowed in unlocking state")) })
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

    fn terminate(self: Box<Self>, _: FopStateContext) -> Result<Box<dyn FopStateNode>> {
        Err(anyhow!("cancel is not allowed in clcw_unreceived state"))
    }
    fn start_unlocking(self: Box<Self>, _: FopStateContext) -> Result<Box<dyn FopStateNode>> {
        Err(anyhow!("start_unlocking is not allowed in clcw_unreceived state"))
    }
    fn start_initializing(self: Box<Self>, _: FopStateContext, _: u8, _: CommandContext) -> Result<Box<dyn FopStateNode>> {
        Err(anyhow!("start_initializing is not allowed in clcw_unreceived state"))
    }
    fn auto_retransmit_disable(self: Box<Self>, context: FopStateContext) -> Result<Box<dyn FopStateNode>> {
        context.send_worker_status(CopWorkerStatusPattern::WorkerAutoRetransmitOff);
        Ok(Box::new(FopStateAutoRetransmitOff::new()) as Box<dyn FopStateNode>)
    }
    fn auto_retransmit_enable(self: Box<Self>, _: FopStateContext) -> Result<Box<dyn FopStateNode>> {
        Err(anyhow!("auto_retransmit_enable is not allowed in clcw_unreceived state"))
    }
    fn send_set_vr_command(&mut self, _: FopStateContext, _: Box<dyn tc::SyncAndChannelCoding + Send + Sync >, _: u8) -> Pin<Box<dyn Future<Output = Result<()>>>> {
        Box::pin(async { Err(anyhow!("send_set_vr_command is not allowed in clcw_unreceived state")) })
    }
    fn send_unlock_command(&self, _: FopStateContext, _: Box<dyn tc::SyncAndChannelCoding + Send + Sync >) -> Pin<Box<dyn Future<Output = Result<()>>>> {
        Box::pin(async { Err(anyhow!("send_unlock_command is not allowed in clcw_unreceived state")) })
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
    vsvr: u8,
    confirmation_cmd: CommandContext
}

impl FopStateInitialize {
    fn new(vsvr: u8, confirmation_cmd: CommandContext) -> Self {
        Self {
            start_time: chrono::Utc::now(),
            vsvr,
            confirmation_cmd
        }
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
            Box::new(FopStateActive::new(self.vsvr, context.next_id, self.confirmation_cmd)) as Box<dyn FopStateNode>
        } else {
            self as Box<dyn FopStateNode>
        }
    }
    fn accept (&mut self, _: FopStateContext, _: u8) {}
    fn reject (&mut self, _: FopStateContext) {}

    fn terminate(self: Box<Self>, context: FopStateContext) -> Result<Box<dyn FopStateNode>> {
        context.send_worker_status(CopWorkerStatusPattern::WorkerCanceled);
        Ok(Box::new(FopStateIdle) as Box<dyn FopStateNode>)
    }
    fn start_unlocking(self: Box<Self>, _: FopStateContext) -> Result<Box<dyn FopStateNode>> {
        Err(anyhow!("start_unlocking is not allowed in initialize state"))
    }
    fn start_initializing(self: Box<Self>, _: FopStateContext, _: u8, _: CommandContext) -> Result<Box<dyn FopStateNode>> {
        Err(anyhow!("start_initializing is not allowed in initialize state"))
    }
    fn auto_retransmit_enable(self: Box<Self>, _: FopStateContext) -> Result<Box<dyn FopStateNode>> {
        Err(anyhow!("auto_retransmit_enable is not allowed in active state"))
    }
    fn auto_retransmit_disable(self: Box<Self>, context: FopStateContext) -> Result<Box<dyn FopStateNode>> {
        context.send_worker_status(CopWorkerStatusPattern::WorkerAutoRetransmitOff);
        Ok(Box::new(FopStateAutoRetransmitOff::new()) as Box<dyn FopStateNode>)
    }
    fn send_set_vr_command(&mut self, _: FopStateContext, _: Box<dyn tc::SyncAndChannelCoding + Send + Sync >,  _: u8) -> Pin<Box<dyn Future<Output = Result<()>>>> {
        Box::pin(async { Err(anyhow!("send_set_vr_command is not allowed in active state")) })
    }
    fn send_unlock_command(&self, _: FopStateContext, _: Box<dyn tc::SyncAndChannelCoding + Send + Sync >) -> Pin<Box<dyn Future<Output = Result<()>>>> {
        Box::pin(async { Err(anyhow!("send_unlock_command is not allowed in active state")) })
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
    fn new(vs: u8, next_id: CopTaskId, confirmation_cmd: CommandContext) -> Self {
        Self {
            queue: FopQueue::new(vs, next_id, confirmation_cmd),
        }
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
            self.queue.clear(CopTaskStatusPattern::Lockout, context.task_status_tx.clone(), context.queue_status_tx.clone());
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
        self.queue.accept(vr, context.task_status_tx.clone(), context.queue_status_tx.clone());
    }
    fn reject (&mut self, context: FopStateContext) {
        self.queue.reject(context.task_status_tx.clone(), context.queue_status_tx.clone());
    }

    fn terminate(mut self: Box<Self>, context: FopStateContext) -> Result<Box<dyn FopStateNode>> {
        self.queue.clear(CopTaskStatusPattern::Canceled, context.task_status_tx.clone(), context.queue_status_tx.clone());
        context.send_worker_status(CopWorkerStatusPattern::WorkerCanceled);
        Ok(Box::new(FopStateIdle) as Box<dyn FopStateNode>)
    }
    fn start_unlocking(self: Box<Self>, _: FopStateContext) -> Result<Box<dyn FopStateNode>> {
        Err(anyhow!("start_unlocking is not allowed in active state"))
    }
    fn start_initializing(mut self: Box<Self>, context: FopStateContext, vsvr: u8, confirmation_cmd: CommandContext) -> Result<Box<dyn FopStateNode>> {
        self.queue.clear(CopTaskStatusPattern::Canceled, context.task_status_tx.clone(), context.queue_status_tx.clone());
        tokio::spawn(async move {
            context.send_worker_status(CopWorkerStatusPattern::WorkerCanceled);
            tokio::time::sleep(tokio::time::Duration::from_nanos(1)).await;
            context.send_worker_status(CopWorkerStatusPattern::WorkerInitialize);
        });
        Ok(Box::new(FopStateInitialize::new(vsvr, confirmation_cmd)) as Box<dyn FopStateNode>)
    }
    fn auto_retransmit_enable(self: Box<Self>, _: FopStateContext) -> Result<Box<dyn FopStateNode>> {
        Err(anyhow!("auto_retransmit_enable is not allowed in active state"))
    }
    fn auto_retransmit_disable(mut self: Box<Self>, context: FopStateContext) -> Result<Box<dyn FopStateNode>> {
        self.queue.clear(CopTaskStatusPattern::Canceled, context.task_status_tx.clone(), context.queue_status_tx.clone());
        context.send_worker_status(CopWorkerStatusPattern::WorkerAutoRetransmitOff);
        Ok(Box::new(FopStateAutoRetransmitOff::new()) as Box<dyn FopStateNode>)
    }
    fn send_set_vr_command(&mut self, _: FopStateContext, _: Box<dyn tc::SyncAndChannelCoding + Send + Sync>, _: u8) -> Pin<Box<dyn Future<Output = Result<()>>>> {
        Box::pin(async { Err(anyhow!("send_set_vr_command is not allowed in active state")) })
    }
    fn send_unlock_command(&self, _: FopStateContext, _: Box<dyn tc::SyncAndChannelCoding + Send + Sync>) -> Pin<Box<dyn Future<Output = Result<()>>>> {
        Box::pin(async { Err(anyhow!("send_unlock_command is not allowed in active state")) })
    }

    fn append (&mut self, context: FopStateContext, cmd_ctx: CommandContext) -> Result<Option<CopTaskId>> {
        Ok(Some(self.queue.push(cmd_ctx, context.task_status_tx, context.queue_status_tx)))
    }

    fn execute (mut self: Box<Self>, context: FopStateContext, mut sync_and_channel_coding: Box<dyn tc::SyncAndChannelCoding + Send + Sync>) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async move {
            let (vs, ctx) = match self.queue.execute(context.task_status_tx.clone(), context.queue_status_tx.clone()) {
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

    fn terminate(self: Box<Self>, _: FopStateContext) -> Result<Box<dyn FopStateNode>> {
        Err(anyhow!("cancel is not allowed in auto_retransmit_off state"))
    }
    fn start_unlocking(self: Box<Self>, _: FopStateContext) -> Result<Box<dyn FopStateNode>> {
        Err(anyhow!("start_unlocking is not allowed in auto_retransmit_off state"))
    }
    fn start_initializing(self: Box<Self>, _: FopStateContext, _: u8, _: CommandContext) -> Result<Box<dyn FopStateNode>> {
        Err(anyhow!("start_initializing is not allowed in auto_retransmit_off state"))
    }
    fn auto_retransmit_enable(self: Box<Self>, context: FopStateContext) -> Result<Box<dyn FopStateNode>>{
        context.send_worker_status(CopWorkerStatusPattern::WorkerClcwUnreceived);
        Ok(Box::new(FopStateClcwUnreceived) as Box<dyn FopStateNode>)
    }
    fn auto_retransmit_disable(self: Box<Self>, _: FopStateContext) -> Result<Box<dyn FopStateNode>> {
        Err(anyhow!("auto_retransmit_disable is not allowed in auto_retransmit_off state"))
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
                timestamp
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
        self.inner = match self.inner.take(){
            Some(state) => Some(state.terminate(context)?),
            None => unreachable!(),
        };
        Ok(())
    }
    pub fn start_unlocking(&mut self) -> Result<()> {
        let context = self.get_context();
        self.inner = match self.inner.take(){
            Some(state) => Some(state.start_unlocking(context)?),
            None => unreachable!(),
        };
        Ok(())
    }
    pub fn start_initializing(&mut self, vsvr: u8, confirmation_cmd: CommandContext) -> Result<()> {
        let context = self.get_context();
        self.inner = match self.inner.take(){
            Some(state) => Some(state.start_initializing(context, vsvr, confirmation_cmd)?),
            None => unreachable!(),
        };
        Ok(())
    }
    pub fn auto_retransmit_enable(&mut self) -> Result<()> {
        let context = self.get_context();
        self.inner = match self.inner.take(){
            Some(state) => Some(state.auto_retransmit_enable(context)?),
            None => unreachable!(),
        };
        Ok(())
    }
    pub fn auto_retransmit_disable(&mut self) -> Result<()> {
        let context = self.get_context();
        self.inner = match self.inner.take(){
            Some(state) => Some(state.auto_retransmit_disable(context)?),
            None => unreachable!(),
        };
        Ok(())
    }
    pub fn set_timeout_sec(&mut self, timeout_sec: u32) {
        self.timeout_sec = timeout_sec;
    }
    pub async fn send_set_vr_command(&mut self, vr: u8) -> Result<()> {
        let context = self.get_context();
        self.inner = match self.inner.take(){
            Some(mut state) => {
                state.send_set_vr_command(context, Box::new(self.sync_and_channel_coding.clone()), vr).await?;
                Some(state)
            },
            None => unreachable!(),
        };
        Ok(())
    }
    pub async fn send_unlock_command(&mut self) -> Result<()> {
        let context = self.get_context();
        self.inner = match self.inner.take(){
            Some(state) => {
                state.send_unlock_command(context, Box::new(self.sync_and_channel_coding.clone())).await?;
                Some(state)
            },
            None => unreachable!(),
        };
        Ok(())
    }

    pub fn append(&mut self, cmd_ctx: CommandContext) -> Result<Option<CopTaskId>> {
        let context = self.get_context();
        let (state, ret, next_id) = match self.inner.take(){
            Some(mut state) => {
                let ret = state.append(context, cmd_ctx)?;
                let next_id = state.get_next_id();
                (Some(state), ret, next_id)
            },
            None => unreachable!(),
        };
        self.next_id = next_id.unwrap_or(self.next_id);
        self.inner = state;
        Ok(ret)
    }
    pub async fn execute(&mut self) {
        let context = self.get_context();
        self.inner = match self.inner.take(){
            Some(state) => Some(state.execute(context, Box::new(self.sync_and_channel_coding.clone())).await),
            None => unreachable!(),
        };
    }
}
