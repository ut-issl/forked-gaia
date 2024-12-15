use std::collections::HashMap;

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::Arc;
use std::time::SystemTime;
use std::{collections::VecDeque, fmt::Display, time};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use gaia_ccsds_c2a::ccsds::tc::sync_and_channel_coding::FrameType;
use gaia_ccsds_c2a::ccsds::{
    aos,
    tc::SyncAndChannelCoding,
    tc::{self, clcw::CLCW},
};
use gaia_tmtc::cop::{
    cop_command, CopCommand, CopQueueStatus, CopQueueStatusSet, CopTaskStatus, CopTaskStatusPattern, CopVsvr, CopWorkerStatus, CopWorkerStatusPattern
};
use gaia_tmtc::tco_tmiv::{Tco, Tmiv, TmivField};
use gaia_tmtc::Handle;
use prost_types::Timestamp;
use tokio::sync::{broadcast, RwLock};
use tracing::error;

use crate::proto::tmtc_generic_c2a::{
    TelemetryChannelSchema, TelemetryChannelSchemaMetadata, TelemetryComponentSchema,
    TelemetryComponentSchemaMetadata, TelemetrySchema, TelemetrySchemaMetadata,
};
use crate::satellite::{
    self, create_clcw_channel, create_cop_command_channel, create_cop_task_channel, CLCWReceiver,
    CommandContext, CopCommandReceiver, CopCommandSender, CopTaskId, TelemetryReporter,
    TimeOutResponse, TmivBuilder,
};
use crate::tco_tmiv_util::{field_schema_bytes, ByteSize};
use crate::{satellite::CopTaskReceiver, tco_tmiv_util::field_int};

use crate::registry::{CommandRegistry, TelemetryRegistry};

#[allow(clippy::too_many_arguments)]
pub fn new<T, R>(
    aos_scid: u8,
    tc_scid: u16,
    tlm_registry: TelemetryRegistry,
    cmd_registry: impl Into<Arc<CommandRegistry>>,
    receiver: R,
    transmitter: T,
) -> (
    satellite::Service,
    TelemetryReporter<R>,
    FopWorker<T>,
    Service,
    Reporter,
)
where
    T: tc::SyncAndChannelCoding + Clone + Send + Sync + 'static,
    R: aos::SyncAndChannelCoding,
{
    let (task_tx, task_rx) = create_cop_task_channel();
    let (clcw_tx, _) = create_clcw_channel();
    let (command_tx, command_rx) = create_cop_command_channel();
    let (queue_status_tx, queue_status_rx) = broadcast::channel(10);
    let (worker_state_tx, worker_state_rx) = broadcast::channel(10);
    let (task_status_tx, task_status_rx) = broadcast::channel(10);
    (
        satellite::Service::new(cmd_registry.into(), tc_scid, task_tx),
        TelemetryReporter::new(
            aos_scid,
            receiver,
            TmivBuilder::new(tlm_registry),
            clcw_tx.clone(),
        ),
        FopWorker::new(
            tc_scid,
            transmitter,
            task_rx,
            clcw_tx.subscribe(),
            command_rx,
            queue_status_tx,
            worker_state_tx,
            task_status_tx,
        ),
        Service { command_tx },
        Reporter::new(
            worker_state_rx,
            queue_status_rx,
            task_status_rx,
            clcw_tx.subscribe(),
        ),
    )
}

pub struct Reporter {
    worker_state_rx: broadcast::Receiver<CopWorkerStatus>,
    queue_status_rx: broadcast::Receiver<CopQueueStatusSet>,
    task_status_rx: broadcast::Receiver<CopTaskStatus>,
    clcw_rx: CLCWReceiver,
}

impl Reporter {
    pub fn new(
        worker_state_rx: broadcast::Receiver<CopWorkerStatus>,
        queue_status_rx: broadcast::Receiver<CopQueueStatusSet>,
        task_status_rx: broadcast::Receiver<CopTaskStatus>,
        clcw_rx: CLCWReceiver,
    ) -> Self {
        Self {
            worker_state_rx,
            queue_status_rx,
            task_status_rx,
            clcw_rx,
        }
    }

    pub async fn run<TLM, TH, WH, QH, VH>(
        mut self, 
        mut tlm_handler: TLM, 
        mut task_handler: TH, 
        mut worker_handler: WH,
        mut queue_handler: QH,
        vsvr_handler: VH,
    ) -> Result<()>
    where
        TH: Handle<Arc<CopTaskStatus>, Response = ()> + Clone,
        WH: Handle<Arc<CopWorkerStatus>, Response = ()> + Clone,
        QH: Handle<Arc<CopQueueStatusSet>, Response = ()> + Clone,
        VH: Handle<Arc<CopVsvr>, Response = ()> + Clone,
        TLM: Handle<Arc<Tmiv>, Response = ()> + Clone,
    {
        let vsvr = Arc::new(RwLock::new(CopVsvr::default()));
        let task_status_rx_task = async {
            loop {
                let status = self.task_status_rx.recv().await?;
                if let Err(e) = task_handler.handle(Arc::new(status)).await {
                    error!("failed to send COP status: {}", e);
                }
            }
        };
        let worker_state_rx_task = async {
            loop {
                let state = self.worker_state_rx.recv().await?;
                if let Err(e) = worker_handler.handle(Arc::new(state)).await
                {
                    error!("failed to send COP status: {}", e);
                }
            }
        };
        let mut vsvr_handler_clone = vsvr_handler.clone();
        let queue_status_rx_task = async {
            loop {
                let status = self.queue_status_rx.recv().await?;
                {
                    let now = chrono::Utc::now().naive_utc();
                    let timestamp = Some(Timestamp {
                        seconds: now.and_utc().timestamp(),
                        nanos: now.and_utc().timestamp_subsec_nanos() as i32,
                    });
                    let mut vsvr = vsvr.write().await;
                    vsvr.vs = status.head_vs;
                    vsvr.timestamp = timestamp;
                    if let Err(e) = vsvr_handler_clone.handle(Arc::new(vsvr.clone())).await
                    {
                        error!("failed to send VSVR status: {}", e);
                    }
                }
                if let Err(e) = queue_handler.handle(Arc::new(status)).await
                {
                    error!("failed to send COP status: {}", e);
                }
            }
        };
        let mut vsvr_handler_clone = vsvr_handler.clone();
        let clcw_rx_task = async {
            loop {
                let clcw = self
                    .clcw_rx
                    .recv()
                    .await
                    .ok_or(anyhow!("CLCW connection has gone"))?;
                {
                    let now = chrono::Utc::now().naive_utc();
                    let timestamp = Some(Timestamp {
                        seconds: now.and_utc().timestamp(),
                        nanos: now.and_utc().timestamp_subsec_nanos() as i32,
                    });
                    let mut vsvr = vsvr.write().await;
                    vsvr.vr = clcw.report_value() as u32;
                    vsvr.timestamp = timestamp;
                    if let Err(e) = vsvr_handler_clone.handle(Arc::new(vsvr.clone())).await
                    {
                        error!("failed to send VSVR status: {}", e);
                    }
                }
                let now = SystemTime::now();
                let tmiv = build_clcw_tmiv(now, &clcw);
                if let Err(e) = tlm_handler.handle(Arc::new(tmiv)).await {
                    error!("failed to send TMIV: {}", e);
                }
            }
        };
        tokio::select! {
            ret = worker_state_rx_task => ret,
            ret = queue_status_rx_task => ret,
            ret = clcw_rx_task => ret,
            ret = task_status_rx_task => ret,
        }
    }
}

pub type IdOffset = CopTaskId;

enum FopQueueState {
    Processing,
    Confirming,
}

pub struct FopQueue {
    next_id: CopTaskId,
    vs_at_id0: u32,
    pending: VecDeque<(CopTaskId, CommandContext)>,
    executed: VecDeque<(CopTaskId, CommandContext, DateTime<Utc>)>,
    rejected: VecDeque<(CopTaskId, CommandContext, DateTime<Utc>)>,
}

impl FopQueue {
    pub fn new(
        vs: u8,
        next_id: CopTaskId,
    ) -> Self {
        Self {
            next_id,
            vs_at_id0: vs.wrapping_sub(next_id as u8) as u32,
            pending: VecDeque::new(),
            executed: VecDeque::new(),
            rejected: VecDeque::new(),
        }
    }

    fn update_status(&mut self, queue_status_tx: broadcast::Sender<CopQueueStatusSet>) {
        let (pending_vs, pending) = {
            if let Some((head_id, ctx)) = self.pending.front() {
                (
                    Some((head_id + self.vs_at_id0) as u8),
                    Some(CopQueueStatus {
                        head_id: Some(*head_id),
                        head_tco_name: Some(ctx.tco.name.clone()),
                        task_count: self.pending.len() as u32,
                    }),
                )
            } else {
                (None, Some(CopQueueStatus::default()))
            }
        };
        let (executed_vs, executed) = {
            if let Some((head_id, ctx, _)) = self.executed.front() {
                (
                    Some((head_id + self.vs_at_id0) as u8),
                    Some(CopQueueStatus {
                        head_id: Some(*head_id),
                        head_tco_name: Some(ctx.tco.name.clone()),
                        task_count: self.executed.len() as u32,
                    }),
                )
            } else {
                (None, Some(CopQueueStatus::default()))
            }
        };
        let (rejected_vs, rejected) = {
            if let Some((head_id, ctx, _)) = self.rejected.front() {
                (
                    Some((head_id + self.vs_at_id0) as u8),
                    Some(CopQueueStatus {
                        head_id: Some(*head_id),
                        head_tco_name: Some(ctx.tco.name.clone()),
                        task_count: self.rejected.len() as u32,
                    }),
                )
            } else {
                (None, Some(CopQueueStatus::default()))
            }
        };
        let oldest_arrival_time = self
            .executed
            .front()
            .map(|(_, _, time)| *time)
            .or_else(|| self.rejected.front().map(|(_, _, time)| *time));
        let vs_list = vec![pending_vs, executed_vs, rejected_vs];
        let head_vs = vs_list.into_iter().flatten().min().map(|vs| vs as u32).unwrap_or(((self.next_id + self.vs_at_id0) as u8) as u32);
        let oldest_arrival_time = oldest_arrival_time.map(|time| Timestamp {
            seconds: time.timestamp(),
            nanos: time.timestamp_subsec_nanos() as i32,
        });
        println!("oldest_arrival_time: {:?}", oldest_arrival_time);
        let now = chrono::Utc::now().naive_utc();
        let timestamp = Some(Timestamp {
            seconds: now.and_utc().timestamp(),
            nanos: now.and_utc().timestamp_subsec_nanos() as i32,
        });
        let status = CopQueueStatusSet {
            pending,
            executed,
            rejected,
            head_vs,
            oldest_arrival_time,
            vs_at_id0: self.vs_at_id0,
            timestamp,
        };
        if let Err(e) = queue_status_tx.send(status) {
            error!("failed to send FOP queue status: {}", e);
        }
    }

    pub fn push(
        &mut self, 
        ctx: CommandContext, 
        task_status_tx: broadcast::Sender<CopTaskStatus>,
        queue_status_tx: broadcast::Sender<CopQueueStatusSet>
    ) -> CopTaskId {
        let id = self.next_id;
        self.next_id += 1;
        let ret = (id, ctx.tco.as_ref().clone());
        self.pending.push_back((id, ctx));
        let id = ret.0;
        let status = CopTaskStatus::from_id_tco(ret, CopTaskStatusPattern::Pending);
        if let Err(e) = task_status_tx.send(status) {
            error!("failed to send COP status: {}", e);
        }
        self.update_status(queue_status_tx);
        id
    }

    pub fn execute(
        &mut self,
        task_status_tx: broadcast::Sender<CopTaskStatus>,
        queue_status_tx: broadcast::Sender<CopQueueStatusSet>,
    ) -> Option<(u8, CommandContext)> {
        let (id, ctx, time) = match self.rejected.pop_front() {
            Some(id_ctx_time) => id_ctx_time,
            None => match self.pending.pop_front() {
                Some((id, ctx)) => (id, ctx, chrono::Utc::now().naive_utc().and_utc()),
                None => return None,
            },
        };
        let ret = ((id + self.vs_at_id0) as u8, ctx.clone());
        self.executed.push_back((id, ctx.clone(), time));
        let status = CopTaskStatus::from_id_tco(
            (id, ctx.tco.as_ref().clone()),
            CopTaskStatusPattern::Executed,
        );
        if let Err(e) = task_status_tx.send(status) {
            error!("failed to send COP status: {}", e);
        }
        self.update_status(queue_status_tx);
        Some(ret)
    }

    pub fn accept(
        &mut self, 
        vr: u8,
        task_status_tx: broadcast::Sender<CopTaskStatus>,
        queue_status_tx: broadcast::Sender<CopQueueStatusSet>
    ) {
        let accepted_num = if let Some((head_id, _, _)) = self.executed.front() {
            if vr.wrapping_sub((head_id + self.vs_at_id0) as u8) > self.executed.len() as u8 {
                0
            } else {
                vr.wrapping_sub((head_id + self.vs_at_id0) as u8)
            }
        } else {
            0
        };
        let accepted = self
            .executed
            .drain(0..(accepted_num as usize))
            .map(|(id, ctx, _)| (id, ctx.tco.as_ref().clone()));
        for id_tco in accepted {
            if let Err(e) = task_status_tx.send(CopTaskStatus::from_id_tco(
                id_tco,
                CopTaskStatusPattern::Accepted,
            )) {
                error!("failed to send COP status: {}", e);
            }
        }
        self.update_status(queue_status_tx);
    }

    pub fn reject(
        &mut self,
        task_status_tx: broadcast::Sender<CopTaskStatus>,
        queue_status_tx: broadcast::Sender<CopQueueStatusSet>
    ) {
        let stash = self.rejected.drain(..);
        let (ret, mut moved): (Vec<_>, Vec<_>) = self
            .executed
            .drain(..)
            .map(|(id, ctx, time)| ((id, ctx.tco.as_ref().clone()), (id, ctx, time)))
            .unzip();
        self.rejected = moved.drain(..).chain(stash).collect();
        for id_tco in ret.into_iter() {
            if let Err(e) = task_status_tx.send(CopTaskStatus::from_id_tco(
                id_tco,
                CopTaskStatusPattern::Rejected,
            )) {
                error!("failed to send COP status: {}", e);
            }
        }
        self.update_status(queue_status_tx);
    }

    pub fn clear(
        &mut self, status_pattern: CopTaskStatusPattern, 
        task_status_tx: broadcast::Sender<CopTaskStatus>,
        queue_status_tx: broadcast::Sender<CopQueueStatusSet>
    ) {
        let canceled = self
            .pending
            .drain(..)
            .chain(self.executed.drain(..).map(|(id, ctx, _)| (id, ctx)))
            .chain(self.rejected.drain(..).map(|(id, ctx, _)| (id, ctx)))
            .map(|(id, ctx)| (id, ctx.tco.as_ref().clone()));
        for id_tco in canceled {
            if let Err(e) = task_status_tx
                .send(CopTaskStatus::from_id_tco(id_tco, status_pattern))
            {
                error!("failed to send COP status: {}", e);
            }
        }
        self.update_status(queue_status_tx);
    }
}

pub struct Service {
    command_tx: CopCommandSender,
}

impl Service {
    async fn try_handle_command(&mut self, command: CopCommand) -> Result<TimeOutResponse> {
        if command.command.is_none() {
            return Err(anyhow!("command is required"));
        }
        let response = self.command_tx.send(command).await??;
        Ok(response)
    }
}

#[async_trait]
impl Handle<Arc<CopCommand>> for Service {
    type Response = TimeOutResponse;

    async fn handle(&mut self, command: Arc<CopCommand>) -> Result<Self::Response> {
        self.try_handle_command(command.as_ref().clone()).await
    }
}

async fn send_type_bc<T: SyncAndChannelCoding + ?Sized>(
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

fn create_set_vr_body(vr: u8) -> Vec<u8> {
    vec![0b10000010, 0b00000000, vr]
}

fn create_unlock_body() -> Vec<u8> {
    vec![0u8]
}

struct FopStateIdle;

impl FopStateNode for FopStateIdle {
    fn evaluate_timeout(self: Box<Self>, _: FopStateContext) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async { self as Box<dyn FopStateNode> })
    }  

    fn clcw_received(self: Box<Self>, _: FopStateContext) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async { self as Box<dyn FopStateNode> })
    }
    fn lockout(self: Box<Self>, context: FopStateContext, flag: bool) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        if flag {
            context.send_worker_status(CopWorkerStatusPattern::WorkerLockout);
            Box::pin(async { Box::new(FopStateLockout) as Box<dyn FopStateNode> })
        } else {
            Box::pin(async { self as Box<dyn FopStateNode> })
        }
    }
    fn vsvr_matched(self: Box<Self>, _: FopStateContext, _: u8) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async { self as Box<dyn FopStateNode> })
    }
    fn accept (&mut self, _: FopStateContext, _: u8) -> Pin<Box<dyn Future<Output = ()>>> {
        Box::pin(async {})
    }
    fn reject (&mut self, _: FopStateContext) -> Pin<Box<dyn Future<Output = ()>>> {
        Box::pin(async {})
    }

    fn terminate(self: Box<Self>, _: FopStateContext) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>>>>> {
        Box::pin(async { Err(anyhow!("cancel is not allowed in idle state")) })
    }
    fn start_unlocking(self: Box<Self>, _: FopStateContext) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>>>>> {
        Box::pin(async { Err(anyhow!("start_unlocking is not allowed in idle state")) })
    }
    fn start_initializing(self: Box<Self>, context: FopStateContext, vsvr: u8) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>>>>> {
        context.send_worker_status(CopWorkerStatusPattern::WorkerInitialize);
        Box::pin(async move { Ok(Box::new(FopStateInitialize::new(vsvr)) as Box<dyn FopStateNode>) })
    }

    fn append (&mut self, _: FopStateContext) -> Pin<Box<dyn Future<Output = Result<()>>>> {
        Box::pin(async { Err(anyhow!("append is not allowed in idle state")) })
    }
    fn execute (self: Box<Self>, _: FopStateContext, _: &mut (dyn tc::SyncAndChannelCoding + Send + Sync + 'static), _: u16) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async { self as Box<dyn FopStateNode> })
    }
}

struct FopStateLockout;

impl FopStateNode for FopStateLockout {
    fn evaluate_timeout(self: Box<Self>, _: FopStateContext) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async { self as Box<dyn FopStateNode> })
    }

    fn clcw_received(self: Box<Self>, _: FopStateContext) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async { self as Box<dyn FopStateNode> })
    }
    fn lockout(self: Box<Self>, context: FopStateContext, flag: bool) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        if flag {
            Box::pin(async { self as Box<dyn FopStateNode> })
        } else {
            context.send_worker_status(CopWorkerStatusPattern::WorkerIdle);
            Box::pin(async { Box::new(FopStateIdle) as Box<dyn FopStateNode> })
        }
    }
    fn vsvr_matched(self: Box<Self>, _: FopStateContext, _: u8) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async { self as Box<dyn FopStateNode> })
    }
    fn accept (&mut self, _: FopStateContext, _: u8) -> Pin<Box<dyn Future<Output = ()>>> {
        Box::pin(async {})
    }
    fn reject (&mut self, _: FopStateContext) -> Pin<Box<dyn Future<Output = ()>>> {
        Box::pin(async {})
    }
    
    fn terminate(self: Box<Self>, _: FopStateContext) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>>>>> {
        Box::pin(async { Err(anyhow!("cancel is not allowed in lockout state")) })
    }
    fn start_unlocking(self: Box<Self>, context: FopStateContext) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>>>>> {
        context.send_worker_status(CopWorkerStatusPattern::WorkerUnlocking);
        Box::pin(async { Ok(Box::new(FopStateUnlocking::new()) as Box<dyn FopStateNode>) })
    }
    fn start_initializing(self: Box<Self>, _: FopStateContext, _: u8) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>>>>> {
        Box::pin(async { Err(anyhow!("start_initializing is not allowed in lockout state")) })
    }

    fn append (&mut self, _: FopStateContext) -> Pin<Box<dyn Future<Output = Result<()>>>> {
        Box::pin(async { Err(anyhow!("append is not allowed in lockout state")) })
    }
    fn execute (self: Box<Self>, _: FopStateContext, _: &mut (dyn tc::SyncAndChannelCoding + Send + Sync + 'static), _: u16) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async { self as Box<dyn FopStateNode> })
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
    fn evaluate_timeout(self: Box<Self>, context: FopStateContext) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        let now = chrono::Utc::now();
        if now - self.start_time > chrono::TimeDelta::seconds(context.timeout_sec as i64) {
            Box::pin(async move { 
                context.send_worker_status(CopWorkerStatusPattern::WorkerTimeout);
                tokio::time::sleep(time::Duration::from_nanos(1)).await;
                context.send_worker_status(CopWorkerStatusPattern::WorkerLockout);
                Box::new(FopStateIdle) as Box<dyn FopStateNode> 
            })
        } else {
            Box::pin(async { self as Box<dyn FopStateNode> })
        }
    }

    fn clcw_received(self: Box<Self>, _: FopStateContext) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async { self as Box<dyn FopStateNode> })
    }
    fn lockout(self: Box<Self>, context: FopStateContext, flag: bool) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        if flag {
            Box::pin(async { self as Box<dyn FopStateNode> })
        } else {
            context.send_worker_status(CopWorkerStatusPattern::WorkerIdle);
            Box::pin(async { Box::new(FopStateIdle) as Box<dyn FopStateNode> })
        }
    }
    fn vsvr_matched(self: Box<Self>, _: FopStateContext, _: u8) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async { self as Box<dyn FopStateNode> })
    }
    fn accept (&mut self, _: FopStateContext, _: u8) -> Pin<Box<dyn Future<Output = ()>>> {
        Box::pin(async {})
    }
    fn reject (&mut self, _: FopStateContext) -> Pin<Box<dyn Future<Output = ()>>> {
        Box::pin(async {})
    }

    fn terminate(self: Box<Self>, context: FopStateContext) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>>>>> {
        Box::pin(async move { 
            context.send_worker_status(CopWorkerStatusPattern::WorkerCanceled);
            tokio::time::sleep(time::Duration::from_nanos(1)).await;
            context.send_worker_status(CopWorkerStatusPattern::WorkerLockout);
            Ok(Box::new(FopStateLockout) as Box<dyn FopStateNode>) 
        })
    }
    fn start_unlocking(self: Box<Self>, _: FopStateContext) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>>>>> {
        Box::pin(async { Err(anyhow!("start_unlocking is not allowed in unlocking state")) })
    }
    fn start_initializing(self: Box<Self>, _: FopStateContext, _: u8) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>>>>> {
        Box::pin(async { Err(anyhow!("start_initializing is not allowed in unlocking state")) })
    }

    fn append (&mut self, _: FopStateContext) -> Pin<Box<dyn Future<Output = Result<()>>>> {
        Box::pin(async { Err(anyhow!("append is not allowed in unlocking state")) })
    }

    fn execute(self: Box<Self>, context: FopStateContext, sync_and_channel_coding: &'static mut (dyn tc::SyncAndChannelCoding + Send + Sync + 'static), tc_scid: u16) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async move {
            match send_type_bc(
                sync_and_channel_coding,
                tc_scid,
                &create_unlock_body(),
            ).await {
                Ok(_) => self as Box<dyn FopStateNode>,
                Err(e) => {
                    error!("failed to send unlock command: {}", e);
                    context.send_worker_status(CopWorkerStatusPattern::WorkerFailed);
                    tokio::time::sleep(time::Duration::from_nanos(1)).await;
                    context.send_worker_status(CopWorkerStatusPattern::WorkerLockout);
                    Box::new(FopStateLockout) as Box<dyn FopStateNode>
                }
            } 
        })
    }
}

struct FopStateClcwUnreceived;

impl FopStateNode for FopStateClcwUnreceived {
    fn evaluate_timeout(self: Box<Self>, _: FopStateContext) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async { self as Box<dyn FopStateNode> })
    }

    fn clcw_received(self: Box<Self>, context: FopStateContext) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        context.send_worker_status(CopWorkerStatusPattern::WorkerIdle);
        Box::pin(async { Box::new(FopStateIdle) as Box<dyn FopStateNode> })
    }
    fn lockout(self: Box<Self>, context: FopStateContext, flag: bool) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        if flag {
            context.send_worker_status(CopWorkerStatusPattern::WorkerLockout);
            Box::pin(async { Box::new(FopStateLockout) as Box<dyn FopStateNode> })
        } else {
            context.send_worker_status(CopWorkerStatusPattern::WorkerIdle);
            Box::pin(async { Box::new(FopStateIdle) as Box<dyn FopStateNode> })
        }
    }
    fn vsvr_matched(self: Box<Self>, context: FopStateContext, _: u8) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        context.send_worker_status(CopWorkerStatusPattern::WorkerIdle);
        Box::pin(async { Box::new(FopStateIdle) as Box<dyn FopStateNode> })
    }
    fn accept (&mut self, _: FopStateContext, _: u8) -> Pin<Box<dyn Future<Output = ()>>> {
        Box::pin(async {})
    }
    fn reject (&mut self, _: FopStateContext) -> Pin<Box<dyn Future<Output = ()>>> {
        Box::pin(async {})
    }

    fn terminate(self: Box<Self>, _: FopStateContext) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>>>>> {
        Box::pin(async { Err(anyhow!("cancel is not allowed in clcw_unreceived state")) })
    }
    fn start_unlocking(self: Box<Self>, _: FopStateContext) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>>>>> {
        Box::pin(async { Err(anyhow!("start_unlocking is not allowed in clcw_unreceived state")) })
    }
    fn start_initializing(self: Box<Self>, _: FopStateContext, _: u8) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>>>>> {
        Box::pin(async { Err(anyhow!("start_initializing is not allowed in clcw_unreceived state")) })
    }

    fn append (&mut self, _: FopStateContext) -> Pin<Box<dyn Future<Output = Result<()>>>> {
        Box::pin(async { Err(anyhow!("append is not allowed in clcw_unreceived state")) })
    }

    fn execute (self: Box<Self>, _: FopStateContext, _: &mut (dyn tc::SyncAndChannelCoding + Send + Sync + 'static), _: u16) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async { self as Box<dyn FopStateNode> })
    }
}

struct FopStateInitialize {
    start_time: DateTime<Utc>,
    vsvr: u8,
    is_auto_retransmit_enabled: bool,
}

impl FopStateInitialize {
    fn new(vsvr: u8, is_auto_retransmit_enabled: bool) -> Self {
        Self {
            start_time: chrono::Utc::now(),
            vsvr,
            is_auto_retransmit_enabled
        }
    }
}

impl FopStateNode for FopStateInitialize {
    fn evaluate_timeout(self: Box<Self>, context: FopStateContext) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        let now = chrono::Utc::now();
        if now - self.start_time > chrono::TimeDelta::seconds(context.timeout_sec as i64) {
            Box::pin(async move { 
                context.send_worker_status(CopWorkerStatusPattern::WorkerTimeout);
                tokio::time::sleep(time::Duration::from_nanos(1)).await;
                Box::new(FopStateIdle) as Box<dyn FopStateNode> 
            })
        } else {
            Box::pin(async { self as Box<dyn FopStateNode> })
        }
    }

    fn clcw_received(self: Box<Self>, _: FopStateContext) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async { self as Box<dyn FopStateNode> })
    }
    fn lockout(self: Box<Self>, context: FopStateContext, flag: bool) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        if flag {
            context.send_worker_status(CopWorkerStatusPattern::WorkerLockout);
            Box::pin(async { Box::new(FopStateLockout) as Box<dyn FopStateNode> })
        } else {
            Box::pin(async { self as Box<dyn FopStateNode> })
        }
    }
    fn vsvr_matched(self: Box<Self>, context: FopStateContext, vr: u8) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        if vr == self.vsvr {
            context.send_worker_status(CopWorkerStatusPattern::WorkerActive);
            Box::pin(async { Box::new(FopStateActive::new(self.vsvr, context.next_id)) as Box<dyn FopStateNode> })
        } else {
            Box::pin(async { self as Box<dyn FopStateNode> })
        }
    }
    fn accept (&mut self, _: FopStateContext, _: u8) -> Pin<Box<dyn Future<Output = ()>>> {
        Box::pin(async {})
    }
    fn reject (&mut self, _: FopStateContext) -> Pin<Box<dyn Future<Output = ()>>> {
        Box::pin(async {})
    }

    fn terminate(self: Box<Self>, context: FopStateContext) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>>>>> {
        context.send_worker_status(CopWorkerStatusPattern::WorkerCanceled);
        Box::pin(async { Ok(Box::new(FopStateIdle) as Box<dyn FopStateNode>) })
    }
    fn start_unlocking(self: Box<Self>, _: FopStateContext) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>>>>> {
        Box::pin(async { Err(anyhow!("start_unlocking is not allowed in initialize state")) })
    }
    fn start_initializing(self: Box<Self>, _: FopStateContext, _: u8) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>>>>> {
        Box::pin(async { Err(anyhow!("start_initializing is not allowed in initialize state")) })
    }
    fn auto_retransmit_enable(self: Box<Self>, _: FopStateContext) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>>>>>{
        Box::pin(async { Err(anyhow!("auto_retransmit_enable is not allowed in active state")) })
    }
    fn auto_retransmit_disable(self: Box<Self>, context: FopStateContext) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>>>>> {
        context.send_worker_status(CopWorkerStatusPattern::WorkerAutoRetransmitOff);
        Box::pin(async { Ok(Box::new(FopStateAutoRetransmitOff) as Box<dyn FopStateNode>) })
    }
    fn send_set_vr_command(self: Box<Self>, _: FopStateContext, _: &'static mut (dyn tc::SyncAndChannelCoding + Send + Sync + 'static), _: u16, _: u8) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>>>>> {
        Box::pin(async { Err(anyhow!("send_set_vr_command is not allowed in active state")) })
    }
    fn send_unlock_command(self: Box<Self>, _: FopStateContext, _: &'static mut (dyn tc::SyncAndChannelCoding + Send + Sync + 'static), _: u16) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>>>>> {
        Box::pin(async { Err(anyhow!("send_unlock_command is not allowed in active state")) })
    }

    fn append (&mut self, _: FopStateContext) -> Pin<Box<dyn Future<Output = Result<()>>>> {
        Box::pin(async { Err(anyhow!("append is not allowed in initialize state")) })
    }

    fn execute (self: Box<Self>, context: FopStateContext, sync_and_channel_coding: &'static mut (dyn tc::SyncAndChannelCoding + Send + Sync + 'static), tc_scid: u16) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async move {
            match send_type_bc(
                sync_and_channel_coding,
                tc_scid,
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

impl FopStateNode for FopStateActive {
    fn evaluate_timeout(self: Box<Self>, context: FopStateContext) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        unimplemented!()
    }

    fn clcw_received(self: Box<Self>, context: FopStateContext) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async { self as Box<dyn FopStateNode> })
    }
    fn lockout(self: Box<Self>, context: FopStateContext, flag: bool) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        unimplemented!()
    }
    fn vsvr_matched(self: Box<Self>, context: FopStateContext, _: u8) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        Box::pin(async { self as Box<dyn FopStateNode> })
    }
    fn accept (&mut self, context: FopStateContext, vr: u8) -> Pin<Box<dyn Future<Output = ()>>> {
        unimplemented!()
    }
    fn reject (&mut self, context: FopStateContext) -> Pin<Box<dyn Future<Output = ()>>> {
        unimplemented!()
    }

    fn terminate(self: Box<Self>, context: FopStateContext) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>>>>> {
        unimplemented!()
    }
    fn start_unlocking(self: Box<Self>, context: FopStateContext) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>>>>> {
        Box::pin(async { Err(anyhow!("start_unlocking is not allowed in active state")) })
    }
    fn start_initializing(self: Box<Self>, context: FopStateContext, vsvr: u8) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>>>>> {
        unimplemented!()
    }
    fn auto_retransmit_enable(self: Box<Self>, _: FopStateContext) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>>>>>{
        Box::pin(async { Err(anyhow!("auto_retransmit_enable is not allowed in active state")) })
    }
    fn auto_retransmit_disable(self: Box<Self>, context: FopStateContext) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>>>>> {
        context.send_worker_status(CopWorkerStatusPattern::WorkerAutoRetransmitOff);
        Box::pin(async { Ok(Box::new(FopStateAutoRetransmitOff) as Box<dyn FopStateNode>) })
    }
    fn send_set_vr_command(self: Box<Self>, _: FopStateContext, _: &'static mut (dyn tc::SyncAndChannelCoding + Send + Sync + 'static), _: u16, _: u8) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>>>>> {
        Box::pin(async { Err(anyhow!("send_set_vr_command is not allowed in active state")) })
    }
    fn send_unlock_command(self: Box<Self>, _: FopStateContext, _: &'static mut (dyn tc::SyncAndChannelCoding + Send + Sync + 'static), _: u16) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>>>>> {
        Box::pin(async { Err(anyhow!("send_unlock_command is not allowed in active state")) })
    }

    fn append (&mut self, context: FopStateContext) -> Pin<Box<dyn Future<Output = Result<()>>>> {
        unimplemented!()
    }

    fn execute (self: Box<Self>, context: FopStateContext, sync_and_channel_coding: &'static mut (dyn tc::SyncAndChannelCoding + Send + Sync + 'static), tc_scid: u16) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>> {
        unimplemented!()
    }
}

pub struct FopStateAutoRetransmitOff {
    next_vs: u8,
    queue: VecDeque<(u8, CommandContext)>
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
    fn accept (&mut self, _: FopStateContext, _: u8) -> Pin<Box<dyn Future<Output = ()>>> {
        Box::pin(async {})
    }
    fn reject (&mut self, _: FopStateContext) -> Pin<Box<dyn Future<Output = ()>>> {
        Box::pin(async {})
    }

    fn terminate(self: Box<Self>, _: FopStateContext) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>>>>> {
        Box::pin(async { Err(anyhow!("cancel is not allowed in auto_retransmit_off state")) })
    }
    fn start_unlocking(self: Box<Self>, _: FopStateContext) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>>>>> {
        Box::pin(async { Err(anyhow!("start_unlocking is not allowed in auto_retransmit_off state")) })
    }
    fn start_initializing(self: Box<Self>, _: FopStateContext, vsvr: _) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>>>>> {
        Box::pin(async { Err(anyhow!("start_initializing is not allowed in auto_retransmit_off state")) })
    }
    fn auto_retransmit_enable(self: Box<Self>, context: FopStateContext) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>>>>>{
        context.send_worker_status(CopWorkerStatusPattern::WorkerClcwUnreceived);
        Box::pin(async { Ok(Box::new(FopStateClcwUnreceived) as Box<dyn FopStateNode>) })
    }
    fn auto_retransmit_disable(self: Box<Self>, _: FopStateContext) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>>>>> {
        Box::pin(async { Err(anyhow!("auto_retransmit_disable is not allowed in auto_retransmit_off state")) })
    }
    fn send_set_vr_command(&mut self, sync_and_channel_coding: &'static mut (dyn tc::SyncAndChannelCoding + Send + Sync + 'static), tc_scid: u16, vsvr: u8) -> Pin<Box<dyn Future<Output = Result<()>>>> {
        self.next_vs = vsvr;
        self.queue.drain(..);
        Box::pin(async move { 
            if let Err(e) = send_type_bc(
                sync_and_channel_coding,
                tc_scid,
                &create_set_vr_body(vsvr),
            ).await {
                error!("failed to send set_vr command: {}", e);
                Err(e)
            } else {
                Ok(())
            }
        })
    }
    fn send_unlock_command(&self, sync_and_channel_coding: &'static mut (dyn tc::SyncAndChannelCoding + Send + Sync + 'static), tc_scid: u16) -> Pin<Box<dyn Future<Output = Result<()>>>> {
        Box::pin(async move { 
            if let Err(e) = send_type_bc(
                sync_and_channel_coding,
                tc_scid,
                &create_unlock_body(),
            ).await {
                error!("failed to send unlock command: {}", e);
                Err(e)
            } else {
                Ok(())
            }
        })
    }
    fn append (&mut self, context: FopStateContext) -> Pin<Box<dyn Future<Output = Result<Option<CopTaskId>>>>> {
        self.queue.push_back((self.next_vs, CommandContext::new(id)));

        Box::pin(async { Ok(Some(id)) })
    }
    
}

trait FopStateNode {
    fn evaluate_timeout(self: Box<Self>, context: FopStateContext) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>>;

    fn clcw_received(self: Box<Self>, context: FopStateContext) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>>;
    fn lockout(self: Box<Self>, context: FopStateContext, flag: bool) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>>;
    fn vsvr_matched(self: Box<Self>, context: FopStateContext, vr: u8) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>>;
    fn accept (&mut self, context: FopStateContext, vr: u8) -> Pin<Box<dyn Future<Output = ()>>>;
    fn reject (&mut self, context: FopStateContext) -> Pin<Box<dyn Future<Output = ()>>>;

    fn terminate(self: Box<Self>, context: FopStateContext) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>>>>>;
    fn start_unlocking(self: Box<Self>, context: FopStateContext) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>>>>>;
    fn start_initializing(self: Box<Self>, context: FopStateContext, vsvr: u8) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>>>>>;
    fn auto_retransmit_enable(self: Box<Self>, context: FopStateContext) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>>>>>;
    fn auto_retransmit_disable(self: Box<Self>, context: FopStateContext) -> Pin<Box<dyn Future<Output = Result<Box<dyn FopStateNode>>>>>;
    fn send_set_vr_command(&mut self, sync_and_channel_coding: &'static mut (dyn tc::SyncAndChannelCoding + Send + Sync + 'static), tc_scid: u16, vr: u8) -> Pin<Box<dyn Future<Output = Result<()>>>>;
    fn send_unlock_command(&self, sync_and_channel_coding: &'static mut (dyn tc::SyncAndChannelCoding + Send + Sync + 'static), tc_scid: u16) -> Pin<Box<dyn Future<Output = Result<()>>>>;

    fn execute (self: Box<Self>, context: FopStateContext, sync_and_channel_coding: &'static mut (dyn tc::SyncAndChannelCoding + Send + Sync + 'static), tc_scid: u16) -> Pin<Box<dyn Future<Output = Box<dyn FopStateNode>>>>;

    fn append (&mut self, context: FopStateContext) -> Pin<Box<dyn Future<Output = Result<Option<CopTaskId>>>>>;
}

#[derive(Clone)]
pub struct FopStateContext {
    worker_state_tx: broadcast::Sender<CopWorkerStatus>,
    queue_status_tx: broadcast::Sender<CopQueueStatusSet>,
    task_status_tx: broadcast::Sender<CopTaskStatus>,
    timeout_sec: u64,
    next_id: CopTaskId,
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
            timeout_sec: self.timeout_sec,
            timestamp: Some(timestamp),
        }) {
            error!("failed to send FOP state: {}", e);
        }
    }
}

pub struct FopStateMachine<T> 
where 
    T: tc::SyncAndChannelCoding + Clone + Send + Sync + 'static,
{
    inner: Option<Box<dyn FopStateNode>>,
    timeout_sec: u64,
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
        }
    }

    async fn evaluate_timeout(&mut self) {
        let context = self.get_context();
        self.inner = match self.inner.take(){
            Some(state) => Some(state.evaluate_timeout(context).await),
            None => unreachable!(),
        };
    }
    async fn clcw_received(&mut self) {
        let context = self.get_context();
        self.inner = match self.inner.take(){
            Some(state) => Some(state.clcw_received(context).await),
            None => unreachable!(),
        };
    }
    async fn set_clcw(&mut self, clcw: &CLCW) -> Result<()> {
        let context = self.get_context();
        self.inner = match self.inner.take(){
            Some(state) => Some(state.vsvr_matched(context.clone(), clcw.report_value()).await),
            None => unreachable!(),
        };
        if clcw.retransmit() == 1 {
            self.inner = match self.inner.take(){
                Some(mut state) => {
                    state.reject(context.clone()).await;
                    Some(state)
                },
                None => unreachable!(),
            };
        }
        self.inner = match self.inner.take(){
            Some(mut state) => {
                state.accept(context.clone(), clcw.report_value()).await;
                Some(state)
            },
            None => unreachable!(),
        };
        self.inner = match self.inner.take(){
            Some(state) => Some(state.lockout(context.clone(), clcw.lockout() == 1).await),
            None => unreachable!(),
        };
        Ok(())
    }
    async fn cancel(&mut self) -> Result<()> {
        let context = self.get_context();
        self.inner = match self.inner.take(){
            Some(state) => Some(state.terminate(context).await?),
            None => unreachable!(),
        };
        Ok(())
    }
    async fn start_unlocking(&mut self) -> Result<()> {
        let context = self.get_context();
        self.inner = match self.inner.take(){
            Some(state) => Some(state.start_unlocking(context).await?),
            None => unreachable!(),
        };
        Ok(())
    }
    async fn start_initializing(&mut self, vsvr: u8) -> Result<()> {
        let context = self.get_context();
        self.inner = match self.inner.take(){
            Some(state) => Some(state.start_initializing(context, vsvr).await?),
            None => unreachable!(),
        };
        Ok(())
    }
    async fn append(&mut self) -> Result<()> {
        let context = self.get_context();
        self.inner = match self.inner.take(){
            Some(mut state) => {
                state.append(context).await?;
                Some(state)
            },
            None => unreachable!(),
        };
        Ok(())
    }
    async fn execute(&'static mut self) {
        let context = self.get_context();
        self.inner = match self.inner.take(){
            Some(state) => Some(state.execute(context, &mut self.sync_and_channel_coding, self.tc_scid).await),
            None => unreachable!(),
        };
    }
}

pub struct FopWorker<T> {
    tc_scid: u16,
    variables: FopVariables,
    queue: FopQueue,
    sync_and_channel_coding: T,
    task_rx: CopTaskReceiver,
    clcw_rx: CLCWReceiver,
    command_rx: CopCommandReceiver,
    queue_status_rx: broadcast::Receiver<CopQueueStatusSet>,
}

impl<T> FopWorker<T>
where
    T: tc::SyncAndChannelCoding + Clone + Send + Sync + 'static,
{
    pub fn new(
        tc_scid: u16,
        sync_and_channel_coding: T,
        task_rx: CopTaskReceiver,
        clcw_rx: CLCWReceiver,
        command_rx: CopCommandReceiver,
        queue_status_tx: broadcast::Sender<CopQueueStatusSet>,
        worker_state_tx: broadcast::Sender<CopWorkerStatus>,
        task_status_tx: broadcast::Sender<CopTaskStatus>,
    ) -> Self {
        let queue_status_rx = queue_status_tx.subscribe();
        Self {
            tc_scid,
            variables: FopVariables {
                is_active: Arc::new(AtomicBool::new(false)),
                is_auto_retransmit_enabled: Arc::new(AtomicBool::new(true)),
                worker_state: CopWorkerStatusPattern::default(),
                timeout_sec: Arc::new(AtomicU64::new(20)),
                last_clcw: CLCW::default(),
                worker_state_tx,
            },
            queue: FopQueue::new(
                queue_status_tx,
                 task_status_tx),
            sync_and_channel_coding,
            task_rx,
            clcw_rx,
            command_rx,
            queue_status_rx,
        }
    }

    fn split_self(
        self,
    ) -> (
        u16,
        Arc<RwLock<FopVariables>>,
        Arc<RwLock<FopQueue>>,
        T,
        CopTaskReceiver,
        CLCWReceiver,
        CopCommandReceiver,
        broadcast::Receiver<CopQueueStatusSet>,
    ) {
        (
            self.tc_scid,
            Arc::new(RwLock::new(self.variables)),
            Arc::new(RwLock::new(self.queue)),
            self.sync_and_channel_coding,
            self.task_rx,
            self.clcw_rx,
            self.command_rx,
            self.queue_status_rx,
        )
    }

    pub async fn run(self) -> Result<()> {
        let (
            tc_scid,
            variables,
            queue,
            sync_and_channel_coding,
            mut command_rx,
            mut clcw_rx,
            mut cop_command_rx,
            mut queue_rx,
        ) = self.split_self();
        let timeout_task = async {
            let mut instant = tokio::time::interval(time::Duration::from_secs(1));
            instant.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut oldest_arrival_time = None;
            let timeout_sec = {
                let timeout_sec = variables.read().await.timeout_sec.clone();
                timeout_sec
            };
            loop {
                instant.tick().await;
                loop {
                    match queue_rx.try_recv() {
                        Ok(status) => oldest_arrival_time = status.oldest_arrival_time,
                        Err(broadcast::error::TryRecvError::Lagged(_)) => {
                            error!("lagged FOP queue status");
                            continue;
                        },
                        Err(broadcast::error::TryRecvError::Empty) => break Ok(()),
                        Err(_) => break Err(anyhow!("FOP queue status channel has gone")),
                    }
                }?;
                if let Some(oldest_time) = &oldest_arrival_time {
                    let duration = chrono::Utc::now().timestamp() - oldest_time.seconds;
                    if duration > timeout_sec.load(std::sync::atomic::Ordering::Relaxed) as i64 {
                        error!("oldest_time: {:?}, duration: {}", oldest_time, duration);
                        {
                            let mut variables = variables.write().await;
                            variables.set_state(CopWorkerStatusPattern::WorkerTimeout);
                        }
                        {
                            let mut queue = queue.write().await;
                            queue.clear(CopTaskStatusPattern::Timeout);
                        }
                        oldest_arrival_time = None;
                    }
                }
            }
        };
        let update_variable_task = async {
            while let Some(clcw) = clcw_rx.recv().await {
                {
                    let mut variables = variables.write().await;
                    if variables.last_clcw.clone().into_bytes() == clcw.clone().into_bytes() {
                        continue;
                    } else {
                        variables.update_clcw(clcw.clone()).await;
                    }
                }
                {
                    let mut queue = queue.write().await;
                    queue.accept(clcw.report_value())
                }
                if clcw.retransmit() == 1 {
                    let mut queue = queue.write().await;
                    queue.reject();
                }
                if clcw.lockout() == 1 {
                    {
                        let mut queue = queue.write().await;
                        queue.clear(CopTaskStatusPattern::Lockout);
                    }
                    {
                        let mut variables = variables.write().await;
                        variables.set_state(CopWorkerStatusPattern::WorkerLockout);
                    }
                } else {
                    let mut variables = variables.write().await;
                    if variables.worker_state == CopWorkerStatusPattern::WorkerUnlocking {
                        variables.set_state(CopWorkerStatusPattern::WorkerIdle);
                    }
                }
            }
            Err(anyhow!("CLCW connection has gone"))
        };
        let mut sync_and_channel_coding_clone = sync_and_channel_coding.clone();
        let append_command_task = async {
            let is_active = {
                let variable = variables.read().await;
                variable.is_active.clone()
            };
            let is_auto_retransmit_enable = {
                let variable = variables.read().await;
                variable.is_auto_retransmit_enabled.clone()
            };
            while let Some((ctx, tx)) = command_rx.recv().await {
                if ctx.tco.is_type_ad {
                    if !is_active.load(std::sync::atomic::Ordering::Relaxed) {
                        if tx.send(Err(anyhow!("COP is not active"))).is_err() {
                            error!("response receiver has gone");
                        }
                        continue;
                    }
                    let mut queue = queue.write().await;
                    if is_auto_retransmit_enable.load(std::sync::atomic::Ordering::Relaxed) {
                        let id = queue.push(ctx);
                        if tx.send(Ok(Some(id))).is_err() {
                            error!("response receiver has gone");
                        }
                    } else {
                        let vs = queue.get_vs_for_bypass();
                        let ret = if let Err(e) = ctx
                            .transmit_to(&mut sync_and_channel_coding_clone, Some(vs))
                            .await
                        {
                            error!("failed to send command: {}", e);
                            Err(anyhow!("failed to transmit COP command"))
                        } else {
                            Ok(None)
                        };
                        if tx.send(ret).is_err() {
                        error!("response receiver has gone");
                    }
                    }
                } else {
                    let ret = if let Err(e) = ctx
                        .transmit_to(&mut sync_and_channel_coding_clone, None)
                        .await
                    {
                        error!("failed to send command: {}", e);
                        Err(anyhow!("failed to transmit COP command"))
                    } else {
                        Ok(None)
                    };
                    if tx.send(ret).is_err() {
                        error!("response receiver has gone");
                    }
                }
            }
            Err(anyhow!("FOP command receiver has gone"))
        };
        let mut sync_and_channel_coding_clone = sync_and_channel_coding.clone();
        let execute_command_task = async {
            let executable = {
                let queue = queue.read().await;
                queue.executable.clone()
            };
            let mut instant = tokio::time::interval(time::Duration::from_millis(10));
            instant.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                if !executable.load(std::sync::atomic::Ordering::Relaxed) {
                    instant.tick().await;
                    continue;
                }
                let (vs, ctx) = {
                    let mut queue = queue.write().await;
                    let Some(vs_ctx) = queue.execute() else {
                        error!("failed to execute command");
                        continue;
                    };
                    vs_ctx
                };
                if let Err(e) = ctx
                    .transmit_to(&mut sync_and_channel_coding_clone, Some(vs))
                    .await
                {
                    error!("failed to send command: {}", e);
                    {
                        let mut queue = queue.write().await;
                        queue.clear(CopTaskStatusPattern::Failed);
                    }
                    {
                        let mut variables = variables.write().await;
                        variables.set_state(CopWorkerStatusPattern::WorkerFailed);
                    }
                }
            }
        };
        let mut sync_and_channel_coding_clone = sync_and_channel_coding.clone();
        let cop_command_task = async {
            let timeout_sec = {
                let timeout_sec = variables.read().await.timeout_sec.clone();
                timeout_sec
            };
            while let Some((command, tx)) = cop_command_rx.recv().await {
                let command_inner = match command.command {
                    Some(command) => command,
                    None => {
                        if tx.send(Err(anyhow!("command is required"))).is_err() {
                            error!("response receiver has gone");
                        }
                        continue;
                    }
                };
                match command_inner {
                    cop_command::Command::Initialize(inner) => {
                        {
                            let mut variables = variables.write().await;
                            if variables.worker_state == CopWorkerStatusPattern::WorkerLockout {
                                if tx.send(Err(anyhow!("COP is locked out"))).is_err() {
                                    error!("response receiver has gone");
                                }
                                continue;
                            } else if variables.worker_state == CopWorkerStatusPattern::WorkerClcwUnreceived {
                                if tx.send(Err(anyhow!("CLCW is not received"))).is_err() {
                                    error!("response receiver has gone");
                                }
                                continue;
                            }
                            variables.set_state(CopWorkerStatusPattern::WorkerInitialize);
                        }

                        {
                            let mut queue = queue.write().await;
                            queue.set_vs(inner.vsvr as u8);
                        }
                        let mut instant = tokio::time::interval(time::Duration::from_millis(500));
                        instant.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                        let duration = time::Duration::from_secs(timeout_sec.load(std::sync::atomic::Ordering::Relaxed));
                        let res = tokio::time::timeout(duration, async {
                            loop {
                                let vr = {
                                    let variable = variables.read().await;
                                    variable.last_clcw.report_value() as u32
                                };
                                if vr == inner.vsvr {
                                    let mut variables = variables.write().await;
                                    variables.set_state(CopWorkerStatusPattern::WorkerActive);
                                    break Ok(());
                                } else {
                                    if let Err(e) = send_type_bc(
                                        &mut sync_and_channel_coding_clone,
                                        tc_scid,
                                        create_set_vr_body(inner.vsvr as u8).as_ref(),
                                    )
                                    .await
                                    {
                                        error!("failed to transmit BC command: {}", e);
                                        break Err(anyhow!("failed to transmit BC command"));
                                    }
                                    instant.tick().await;
                                }
                            }
                        })
                        .await;
                        match res {
                            Ok(ret) => {
                                if tx
                                    .send(ret.map(|_| TimeOutResponse { is_timeout: false }))
                                    .is_err()
                                {
                                    error!("response receiver has gone");
                                }
                            }
                            Err(e) => {
                                error!("timeout: {}", e);
                                {
                                    let mut variables = variables.write().await;
                                    variables.set_state(CopWorkerStatusPattern::WorkerTimeout);
                                }
                                if tx.send(Ok(TimeOutResponse { is_timeout: true })).is_err() {
                                    error!("response receiver has gone");
                                }
                            }
                        }
                    }
                    cop_command::Command::Unlock(_) => {
                        {
                            let mut variables = variables.write().await;
                            if variables.worker_state == CopWorkerStatusPattern::WorkerClcwUnreceived {
                                if tx.send(Err(anyhow!("CLCW is not received"))).is_err() {
                                    error!("response receiver has gone");
                                }
                                continue;
                            } else if variables.worker_state != CopWorkerStatusPattern::WorkerLockout {
                                if tx.send(Ok(TimeOutResponse { is_timeout: false })).is_err() {
                                    error!("response receiver has gone");
                                }
                                continue;
                            }
                            variables.set_state(CopWorkerStatusPattern::WorkerUnlocking);
                        }
                        let mut instant = tokio::time::interval(time::Duration::from_millis(500));
                        instant.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                        let duration = time::Duration::from_secs(timeout_sec.load(std::sync::atomic::Ordering::Relaxed));
                        let res = tokio::time::timeout(duration, async {
                            loop {
                                if let Err(e) = send_type_bc(
                                    &mut sync_and_channel_coding_clone,
                                    tc_scid,
                                    create_unlock_body().as_ref(),
                                )
                                .await
                                {
                                    error!("failed to transmit BC command: {}", e);
                                    break Err(anyhow!("failed to transmit BC command: {}", e));
                                }
                                {
                                    let variable = variables.read().await;
                                    if variable.worker_state != CopWorkerStatusPattern::WorkerUnlocking
                                    {
                                        break Ok(());
                                    }
                                }
                                instant.tick().await;
                            }
                        })
                        .await;
                        match res {
                            Ok(ret) => {
                                if tx
                                    .send(ret.map(|_| TimeOutResponse { is_timeout: false }))
                                    .is_err()
                                {
                                    error!("response receiver has gone");
                                }
                            }
                            Err(e) => {
                                error!("timeout: {}", e);
                                {
                                    let mut variables = variables.write().await;
                                    variables.set_state(CopWorkerStatusPattern::WorkerTimeout);
                                }
                                if tx.send(Ok(TimeOutResponse { is_timeout: true })).is_err() {
                                    error!("response receiver has gone");
                                }
                            }
                        }
                    }
                    cop_command::Command::Terminate(_) => {
                        {
                            let mut variables = variables.write().await;
                            variables.set_state(CopWorkerStatusPattern::WorkerCanceled);
                        }
                        {
                            let mut queue = queue.write().await;
                            queue.clear(CopTaskStatusPattern::Canceled);
                        }
                        if tx.send(Ok(TimeOutResponse { is_timeout: false })).is_err() {
                            error!("response receiver has gone");
                        }
                    }
                    cop_command::Command::SetTimeout(inner) => {
                        {
                            let mut variables = variables.write().await;
                            variables.set_timeout_sec(inner.timeout_sec.into());
                        }
                        if tx.send(Ok(TimeOutResponse { is_timeout: false })).is_err() {
                            error!("response receiver has gone");
                        }
                    }
                    cop_command::Command::SetAutoRetransmitEnable(_) => {
                        {
                            let mut variables = variables.write().await;
                            variables.set_auto_retransmit_enable(true);
                        }
                        if tx.send(Ok(TimeOutResponse { is_timeout: false })).is_err() {
                            error!("response receiver has gone");
                        }
                    }
                    cop_command::Command::SetAutoRetransmitDisable(_) => {
                        {
                            let mut variables = variables.write().await;
                            variables.set_auto_retransmit_enable(false);
                        }
                        if tx.send(Ok(TimeOutResponse { is_timeout: false })).is_err() {
                            error!("response receiver has gone");
                        }
                    }
                }
            }
            Err(anyhow!("COP command receiver has gone"))
        };
        tokio::select! {
            ret = timeout_task => ret,
            ret = update_variable_task => ret,
            ret = append_command_task => ret,
            _ = execute_command_task => Ok(()),
            ret = cop_command_task => ret,
        }
    }
}

trait FromIdTco {
    fn from_id_tco(id_tco: (CopTaskId, Tco), status: CopTaskStatusPattern) -> Self;
}

impl FromIdTco for CopTaskStatus {
    fn from_id_tco((id, tco): (CopTaskId, Tco), status: CopTaskStatusPattern) -> Self {
        let now = chrono::Utc::now().naive_utc();
        let timestamp = Timestamp {
            seconds: now.and_utc().timestamp(),
            nanos: now.and_utc().timestamp_subsec_nanos() as i32,
        };
        CopTaskStatus {
            task_id: id,
            tco: Some(tco),
            status: status as i32,
            timestamp: Some(timestamp),
        }
    }
}

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
        .duration_since(time::UNIX_EPOCH)
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
