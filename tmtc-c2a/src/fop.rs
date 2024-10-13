use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::SystemTime;
use std::{collections::VecDeque, fmt::Display, time};

use anyhow::{anyhow, Result};
use gaia_ccsds_c2a::ccsds::tc::sync_and_channel_coding::FrameType;
use gaia_ccsds_c2a::ccsds::{tc::{self, clcw::CLCW}, aos, tc::SyncAndChannelCoding};
use gaia_tmtc::cop::cop_status::Inner;
use gaia_tmtc::tco_tmiv::{Tco, Tmiv, TmivField};
use gaia_tmtc::cop::{cop_command, CopCommand, CopTaskStatus, CopTaskStatusPattern, CopStatus, TaskQueueStatus, TaskQueueStatusSet, WorkerState, WorkerStatePattern};
use gaia_tmtc::Handle;
use tokio::sync::{broadcast, Mutex, RwLock};
use async_trait::async_trait;
use tracing::error;

use crate::proto::tmtc_generic_c2a::{TelemetryChannelSchema, TelemetryChannelSchemaMetadata, TelemetryComponentSchema, TelemetryComponentSchemaMetadata, TelemetrySchema, TelemetrySchemaMetadata};
use crate::satellite::{self, create_clcw_channel, create_cop_command_channel, create_cop_task_channel, CLCWReceiver, CommandContext, CopCommandReceiver, CopCommandSender, CopTaskId, TelemetryReporter, TimeOutResponse, TmivBuilder};
use crate::tco_tmiv_util::field_schema_int;
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
) -> (satellite::Service, TelemetryReporter<R>, FopWorker<T>, Service, Reporter)
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
        satellite::Service::new(
            cmd_registry.into(),
            tc_scid,
            task_tx,
        ),
        TelemetryReporter::new(
            aos_scid,
            receiver,
            TmivBuilder::new(tlm_registry),
            clcw_tx.clone(),
        ),
        FopWorker::new(
            tc_scid, 
            transmitter, task_rx, 
            clcw_tx.subscribe(), 
            command_rx,
            queue_status_tx,
            worker_state_tx,
            task_status_tx,
        ),
        Service {
            command_tx,
        },
        Reporter::new(
            worker_state_rx,
            queue_status_rx,
            task_status_rx,
            clcw_tx.subscribe()
        )
    )
}

pub struct Reporter {
    worker_state_rx: broadcast::Receiver<WorkerStatePattern>,
    queue_status_rx: broadcast::Receiver<TaskQueueStatusSet>,
    task_status_rx: broadcast::Receiver<CopStatus>,
    clcw_rx: CLCWReceiver,
}

impl Reporter {
    pub fn new(
        worker_state_rx: broadcast::Receiver<WorkerStatePattern>, 
        queue_status_rx: broadcast::Receiver<TaskQueueStatusSet>, 
        task_status_rx: broadcast::Receiver<CopStatus>, 
        clcw_rx: CLCWReceiver
    ) -> Self {
        Self {
            worker_state_rx,
            queue_status_rx,
            task_status_rx,
            clcw_rx,
        }
    }

    pub async fn run<TLM, ST>(mut self, mut tlm_handler: TLM, status_handler: ST) -> Result<()>
    where 
        ST: Handle<Arc<CopStatus>, Response = ()> + Clone,
        TLM: Handle<Arc<Tmiv>, Response = ()> + Clone,
    {
        let mut status_handler_clone = status_handler.clone();
        let worker_state_rx_task = async {
            loop {
                let state = self.worker_state_rx.recv().await?;
                let now = time::SystemTime::now();
                if let Err(e) = status_handler_clone.handle(Arc::new(CopStatus {
                    inner: Some(Inner::WorkerState(WorkerState {
                        state: state.into(),
                        timestamp: Some(now.into()),
                    }))
                })).await {
                    error!("failed to send COP status: {}", e);
                }
            }
        };
        let mut status_handler_clone = status_handler.clone();
        let queue_status_rx_task = async {
            loop {
                let status = self.queue_status_rx.recv().await?;
                if let Err(e) = status_handler_clone.handle(Arc::new(CopStatus { inner: Some(Inner::TaskQueueStatus(status)) })).await {
                    error!("failed to send COP status: {}", e);
                }
            }
        };
        let clcw_rx_task = async {
            loop {
                let clcw = self.clcw_rx.recv().await.ok_or(anyhow!("CLCW connection has gone"))?;
                let now = time::SystemTime::now();
                let tmiv = build_clcw_tmiv(now, &clcw);
                if let Err(e) = tlm_handler.handle(Arc::new(tmiv)).await {
                    error!("failed to send TMIV: {}", e);
                }
            }
        };
        let mut status_handler_clone = status_handler.clone();
        let task_status_rx_task = async {
            loop {
                let status = self.task_status_rx.recv().await?;
                if let Err(e) = status_handler_clone.handle(Arc::new(status)).await {
                    error!("failed to send COP status: {}", e);
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

#[derive(Debug, Clone)]
pub struct FopVariables {
    is_active: Arc<AtomicBool>,
    worker_state: WorkerStatePattern,
    last_clcw: CLCW,
    worker_state_tx: broadcast::Sender<WorkerStatePattern>,
}

impl FopVariables {
    pub fn new(worker_state_tx: broadcast::Sender<WorkerStatePattern>) -> Self {
        Self {
            is_active: Arc::new(AtomicBool::new(false)),
            worker_state: WorkerStatePattern::WorkerClcwUnreceived,
            last_clcw: CLCW::default(),
            worker_state_tx,
        }
    }

    pub fn set_state(&mut self, state: WorkerStatePattern) {
        self.worker_state = state;
        if state == WorkerStatePattern::WorkerActive {
            self.is_active.store(true, std::sync::atomic::Ordering::Relaxed);
        } else {
            self.is_active.store(false, std::sync::atomic::Ordering::Relaxed);
        }
        if let Err(e) = self.worker_state_tx.send(state) {
            error!("failed to send FOP state: {}", e);
        }
    }

    pub async fn update_clcw(&mut self, clcw: CLCW) 
    {
        if self.worker_state == WorkerStatePattern::WorkerClcwUnreceived {
            self.set_state(WorkerStatePattern::WorkerIdle);
        }
        self.last_clcw = clcw.clone();
    }
}

pub type IdOffset = CopTaskId;

pub struct FopQueue {
    next_id: CopTaskId,
    vs_at_id0: u32,
    executable: Arc<AtomicBool>,
    pending: VecDeque<(CopTaskId, CommandContext)>,
    executed: VecDeque<(CopTaskId, CommandContext, SystemTime)>,
    rejected: VecDeque<(CopTaskId, CommandContext, SystemTime)>,
    queue_status_tx: broadcast::Sender<TaskQueueStatusSet>,
    task_status_tx: broadcast::Sender<CopStatus>,
}

impl FopQueue {
    pub fn new(queue_status_tx: broadcast::Sender<TaskQueueStatusSet>, task_status_tx: broadcast::Sender<CopStatus>) -> Self {
        Self {
            next_id: 0,
            vs_at_id0: 0,
            executable: Arc::new(AtomicBool::new(false)),
            pending: VecDeque::new(),
            executed: VecDeque::new(),
            rejected: VecDeque::new(),
            queue_status_tx,
            task_status_tx,
        }
    }

    fn update_status(&mut self) {
        let (pending_vs, pending) = {
            if let Some((front_id, ctx)) = self.pending.front() {
                (
                    Some((front_id + self.vs_at_id0) as u8),
                    Some(TaskQueueStatus {
                        front_id: Some(*front_id),
                        front_tco_name: Some(ctx.tco.name.clone()),
                        command_number: self.pending.len() as u32,
                    })
                )
            } else {
                (None, Some(TaskQueueStatus::default()))
            }
        };
        let (executed_vs, executed) = {
            if let Some((front_id, ctx, _)) = self.executed.front() {
                (
                    Some((front_id + self.vs_at_id0) as u8),
                    Some(TaskQueueStatus {
                        front_id: Some(*front_id),
                        front_tco_name: Some(ctx.tco.name.clone()),
                        command_number: self.executed.len() as u32,
                    })
                )
            } else {
                (None, Some(TaskQueueStatus::default()))
            }
        };
        let (rejected_vs, rejected) = {
            if let Some((front_id, ctx, _)) = self.rejected.front() {
                (
                    Some((front_id + self.vs_at_id0) as u8),
                    Some(TaskQueueStatus {
                        front_id: Some(*front_id),
                        front_tco_name: Some(ctx.tco.name.clone()),
                        command_number: self.rejected.len() as u32,
                    })
                )
            } else {
                (None, Some(TaskQueueStatus::default()))
            }
        };
        let oldest_time = self.executed.front().map(|(_, _, time)| *time).or_else(|| self.rejected.front().map(|(_, _, time)| *time));
        let vs_list = vec![pending_vs, executed_vs, rejected_vs];
        let front_vs = vs_list.into_iter().flatten().min().map(|vs| vs as u32);
        let oldest_time = oldest_time.map(|time| time.into());
        let timestamp = Some(SystemTime::now().into());
        let status = TaskQueueStatusSet {
            pending,
            executed,
            rejected,
            front_vs,
            oldest_time,
            timestamp,
        };
        if let Err(e) = self.queue_status_tx.send(status) {
            error!("failed to send FOP queue status: {}", e);
        }
    }

    pub fn push(&mut self, ctx: CommandContext) -> CopTaskId {
        let id = self.next_id;
        self.executable.store(true, std::sync::atomic::Ordering::Relaxed);
        self.next_id += 1;
        let ret = (id, ctx.tco.as_ref().clone());
        self.pending.push_back((id, ctx));
        self.update_status();
        let id = ret.0;
        let status = CopStatus::from_id_tco(ret, CopTaskStatusPattern::Pending);
        if let Err(e) = self.task_status_tx.send(status) {
            error!("failed to send COP status: {}", e);
        }
        id
    }

    pub fn execute(&mut self) -> Option<(u8, CommandContext)> {
        let (id, ctx, time) = match self.rejected.pop_front() {
            Some(id_ctx_time) => id_ctx_time,
            None => match self.pending.pop_front() {
                Some((id, ctx)) => (id, ctx, SystemTime::now()),
                None => return None,
            },
        };
        let ret = ((id + self.vs_at_id0) as u8, ctx.clone());
        self.executed.push_back((id, ctx.clone(), time));

        if !self.rejected.is_empty() || !self.pending.is_empty() {
            self.executable.store(true, std::sync::atomic::Ordering::Relaxed);
        } else {
            self.executable.store(false, std::sync::atomic::Ordering::Relaxed);
        }
        let status = CopStatus::from_id_tco((id, ctx.tco.as_ref().clone()), CopTaskStatusPattern::Executed);
        if let Err(e) = self.task_status_tx.send(status) {
            error!("failed to send COP status: {}", e);
        }
        self.update_status();
        Some(ret)
    }

    pub fn accept(&mut self, vr: u8) {
        let accepted_num = if let Some((head_id, _, _)) = self.executed.front() {
            (vr + 1).wrapping_sub((head_id + self.vs_at_id0) as u8)
        } else {
            0
        };
        let accepted = self.executed.drain(0..(accepted_num as usize)).map(|(id, ctx, _)| (id, ctx.tco.as_ref().clone()));
        for id_tco in accepted {
            if let Err(e) = self.task_status_tx.send(CopStatus::from_id_tco(id_tco, CopTaskStatusPattern::Accepted)) {
                error!("failed to send COP status: {}", e);
            }
        }
        self.update_status();
    }

    pub fn reject(&mut self) {
        let stash = self.rejected.drain(..);
        let (ret, mut moved): (Vec<_>, Vec<_>) = self.executed.drain(..).map(|(id, ctx, time)| ((id, ctx.tco.as_ref().clone()), (id, ctx, time))
        ).unzip();
        self.rejected = moved.drain(..).chain(stash).collect();
        for id_tco in ret.into_iter() {
            if let Err(e) = self.task_status_tx.send(CopStatus::from_id_tco(id_tco, CopTaskStatusPattern::Rejected)) {
                error!("failed to send COP status: {}", e);
            }
        }
        self.update_status();
    }

    pub fn clear(&mut self, status_pattern: CopTaskStatusPattern) {
        self.executable.store(false, std::sync::atomic::Ordering::Relaxed);
        self.update_status();
        let canceled = self.pending.drain(..)
            .chain(self.executed.drain(..).map(|(id, ctx, _)| (id, ctx)))
            .chain(self.rejected.drain(..).map(|(id, ctx, _)| (id, ctx)))
            .map(|(id, ctx)| (id, ctx.tco.as_ref().clone()));
        for id_tco in canceled {
            if let Err(e) = self.task_status_tx.send(CopStatus::from_id_tco(id_tco, status_pattern)) {
                error!("failed to send COP status: {}", e);
            }
        }
    }

    pub fn set_vs(&mut self, vs: u8) {
        self.clear(CopTaskStatusPattern::Canceled);
        self.vs_at_id0 = vs.wrapping_sub(self.next_id as u8) as u32;
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

async fn send_type_bc<T: SyncAndChannelCoding>(sync_and_channel_coding: &mut T, tc_scid: u16, data_field: &[u8]) -> Result<()> {
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

pub struct FopWorker<T> {
    tc_scid: u16,
    timeout_sec: tokio::time::Duration,
    variables: FopVariables,
    queue: FopQueue,
    sync_and_channel_coding: T,
    task_rx: CopTaskReceiver,
    clcw_rx: CLCWReceiver,
    command_rx: CopCommandReceiver,
    queue_status_rx: broadcast::Receiver<TaskQueueStatusSet>,
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
        queue_status_tx: broadcast::Sender<TaskQueueStatusSet>, 
        worker_state_tx: broadcast::Sender<WorkerStatePattern>,
        task_status_tx: broadcast::Sender<CopStatus>,
    ) -> Self {
        let queue_status_rx = queue_status_tx.subscribe();
        Self {
            tc_scid,
            timeout_sec: tokio::time::Duration::from_secs(20),
            variables: FopVariables {
                is_active: Arc::new(AtomicBool::new(false)),
                worker_state: WorkerStatePattern::default(),
                last_clcw: CLCW::default(),
                worker_state_tx,
            },
            queue: FopQueue::new(queue_status_tx, task_status_tx),
            sync_and_channel_coding,
            task_rx,
            clcw_rx,
            command_rx,
            queue_status_rx,
        }
    }

    fn split_self(self) -> 
    (
        u16,
        Arc<Mutex<tokio::time::Duration>>,
        Arc<RwLock<FopVariables>>, 
        Arc<RwLock<FopQueue>>, 
        T, 
        CopTaskReceiver, 
        CLCWReceiver, 
        CopCommandReceiver,
        broadcast::Receiver<TaskQueueStatusSet>,
    ) {
        (
            self.tc_scid,
            Arc::new(Mutex::new(self.timeout_sec)),
            Arc::new(RwLock::new(self.variables)), 
            Arc::new(RwLock::new(self.queue)), 
            self.sync_and_channel_coding, 
            self.task_rx, 
            self.clcw_rx, 
            self.command_rx,
            self.queue_status_rx,
        )
    }

    pub async fn run(self) -> Result<()> 
    {
        let (
            tc_scid,
            timeout_sec,
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
            let mut oldest_time = None;
            loop {
                oldest_time = match queue_rx.try_recv() {
                    Ok(status) => status.oldest_time,
                    Err(broadcast::error::TryRecvError::Empty) => oldest_time,
                    Err(_) => break Err(anyhow!("FOP queue status channel has gone")),
                };
                let timeout_sec = {
                    let timeout_sec = timeout_sec.lock().await;
                    *timeout_sec
                };
                if let Some(oldest_time) = &oldest_time {
                    if tokio::time::Duration::from_secs(oldest_time.seconds as u64) > timeout_sec {
                        {
                            let mut variables = variables.write().await;
                            variables.set_state(WorkerStatePattern::WorkerTimeout);
                        }
                        {
                            let mut queue = queue.write().await;
                            queue.clear(CopTaskStatusPattern::Timeout);
                        }
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
                        variables.set_state(WorkerStatePattern::WorkerLockout);
                    }
                } else {
                    let mut variables = variables.write().await;
                    if variables.worker_state == WorkerStatePattern::WorkerLockout {
                        variables.set_state(WorkerStatePattern::WorkerIdle);
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
            while let Some((ctx, tx)) = command_rx.recv().await {
                if ctx.tco.is_type_ad {
                    if !is_active.load(std::sync::atomic::Ordering::Relaxed) {
                        if tx.send(Err(anyhow!("COP is not active"))).is_err() {
                            error!("response receiver has gone");
                        }
                        continue;
                    }
                    let mut queue = queue.write().await;
                    let id = queue.push(ctx);                    
                    if tx.send(Ok(Some(id))).is_err() {
                        error!("response receiver has gone");
                    }
                } else {
                    let ret = if let Err(e) = ctx.transmit_to(&mut sync_and_channel_coding_clone, None).await {
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
            let executable ={
                let queue = queue.read().await;
                queue.executable.clone()
            };
            let mut instant = tokio::time::interval(time::Duration::from_millis(500));
            instant.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                if !executable.load(std::sync::atomic::Ordering::Relaxed) {
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
                if let Err(e) = ctx.transmit_to(&mut sync_and_channel_coding_clone, Some(vs)).await {
                    error!("failed to send command: {}", e);
                    {
                        let mut queue = queue.write().await;
                        queue.clear(CopTaskStatusPattern::Failed);
                    }
                    {
                        let mut variables = variables.write().await;
                        variables.set_state(WorkerStatePattern::WorkerFailed);
                    }
                }
            }
        };
        let mut sync_and_channel_coding_clone = sync_and_channel_coding.clone();
        let cop_command_task = async {
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
                            if variables.worker_state == WorkerStatePattern::WorkerLockout {
                                if tx.send(Err(anyhow!("COP is locked out"))).is_err() {
                                    error!("response receiver has gone");
                                }
                                continue;
                            }
                            variables.set_state(WorkerStatePattern::WorkerInitialize);
                        }
            
                        {
                            let mut queue = queue.write().await;
                            queue.set_vs(inner.vsvr as u8);
                        }
                        let mut instant = tokio::time::interval(time::Duration::from_millis(500));
                        instant.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                        let timeout_sec = {
                            let timeout_sec = timeout_sec.lock().await;
                            *timeout_sec
                        };
                        let res =  tokio::time::timeout(timeout_sec,  async { loop {
                            let vr = {
                                let variable = variables.read().await;
                                variable.last_clcw.report_value() as u32
                            };
                            if vr == inner.vsvr {
                                let mut variables = variables.write().await;
                                variables.set_state(WorkerStatePattern::WorkerActive);
                                break Ok(())
                            } else {
                                if let Err(e) = send_type_bc(&mut sync_and_channel_coding_clone, tc_scid, create_set_vr_body(inner.vsvr as u8).as_ref()).await {
                                    error!("failed to transmit BC command: {}", e);
                                    break Err(anyhow!("failed to transmit BC command"))
                                }
                                instant.tick().await;
                            }
                        }}).await;
                        match res {
                            Ok(ret) => {
                                if tx.send(ret.map(|_| TimeOutResponse { is_timeout: false })).is_err() {
                                    error!("response receiver has gone");
                                }
                            }
                            Err(e) => {
                                error!("timeout: {}", e);
                                {
                                    let mut variables = variables.write().await;
                                    variables.set_state(WorkerStatePattern::WorkerTimeout);
                                }
                                if tx.send(Ok(TimeOutResponse { is_timeout: true })).is_err() {
                                    error!("response receiver has gone");
                                }
                            }
                        }
                    },
                    cop_command::Command::Unlock(_) => {
                        {
                            let mut variables = variables.write().await;
                            if variables.worker_state != WorkerStatePattern::WorkerLockout {
                                if tx.send(Ok(TimeOutResponse { is_timeout: false })).is_err() {
                                    error!("response receiver has gone");
                                }
                                continue;
                            }
                            variables.set_state(WorkerStatePattern::WorkerUnlocking);
                        }
                        let timeout_sec = {
                            let timeout_sec = timeout_sec.lock().await;
                            *timeout_sec
                        };
                        let res = tokio::time::timeout(timeout_sec,  async { loop {
                            if let Err(e) = send_type_bc(&mut sync_and_channel_coding_clone, tc_scid, create_unlock_body().as_ref()).await {
                                error!("failed to transmit BC command: {}", e);
                                break Err(anyhow!("failed to transmit BC command: {}", e))
                            }
                            {
                                let variable = variables.read().await;
                                if variable.worker_state != WorkerStatePattern::WorkerUnlocking {
                                    break Ok(())
                                }
                            }
                        }}).await;
                        match res {
                            Ok(ret) => {
                                if tx.send(ret.map(|_| TimeOutResponse { is_timeout: false })).is_err() {
                                    error!("response receiver has gone");
                                }
                            }
                            Err(e) => {
                                error!("timeout: {}", e);
                                {
                                    let mut variables = variables.write().await;
                                    variables.set_state(WorkerStatePattern::WorkerTimeout);
                                }
                                if tx.send(Ok(TimeOutResponse { is_timeout: true })).is_err() {
                                    error!("response receiver has gone");
                                }
                            }
                        }
                    },
                    cop_command::Command::Terminate(_) => {
                        {
                            let mut variables = variables.write().await;
                            variables.set_state(WorkerStatePattern::WorkerCanceled);
                        }
                        {
                            let mut queue = queue.write().await;
                            queue.clear(CopTaskStatusPattern::Canceled);
                        }
                        if tx.send(Ok(TimeOutResponse { is_timeout: false })).is_err() {
                            error!("response receiver has gone");
                        }
                    },
                    cop_command::Command::SetTimeout(inner) => {
                        {
                            let mut timeout_sec = timeout_sec.lock().await;
                            *timeout_sec = tokio::time::Duration::from_secs(inner.timeout_sec as u64);
                        }
                        if tx.send(Ok(TimeOutResponse { is_timeout: false })).is_err() {
                            error!("response receiver has gone");
                        }
                    },
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

impl FromIdTco for CopStatus {
    fn from_id_tco((id, tco): (CopTaskId, Tco), status: CopTaskStatusPattern) -> Self {
        CopStatus {
            inner: Some(Inner::TaskStatus(
                CopTaskStatus {
                    task_id: id,
                    tco: Some(tco),
                    status: status as i32,
                    timestamp: Some(time::SystemTime::now().into()),
                }
            ))
        }
    }
}

struct FopStateString {
    state: WorkerStatePattern,
}

impl From<WorkerStatePattern> for FopStateString {
    fn from(state: WorkerStatePattern) -> Self {
        Self { state }
    }
}

impl Display for FopStateString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.state {
            WorkerStatePattern::WorkerClcwUnreceived => write!(f, "CLCW_UNRECEIVED"),
            WorkerStatePattern::WorkerIdle => write!(f, "IDLE"),
            WorkerStatePattern::WorkerInitialize => write!(f, "INITIALIZE"),
            WorkerStatePattern::WorkerActive => write!(f, "ACTIVE"),
            WorkerStatePattern::WorkerLockout => write!(f, "LOCKOUT"),
            WorkerStatePattern::WorkerUnlocking => write!(f, "UNLOCKING"),
            WorkerStatePattern::WorkerTimeout => write!(f, "TIMEOUT"),
            WorkerStatePattern::WorkerFailed => write!(f, "FAILED"),
            WorkerStatePattern::WorkerCanceled => write!(f, "CANCELED"),
        }
    }
}

const TMIV_DESTINATION_TYPE: &str = "RT";
const TMIV_COMPONENT_NAME: &str = "GAIA";
const CLCW_TMIV_NAME: &str = "TF_CLCW";

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
    vec![
        (
            TMIV_COMPONENT_NAME.to_string(),
            TelemetryComponentSchema {
                metadata: Some(TelemetryComponentSchemaMetadata { apid: 0 }),
                telemetries: vec![
                    (
                        CLCW_TMIV_NAME.to_string(),
                        TelemetrySchema {
                            metadata: Some(TelemetrySchemaMetadata {
                                id: 0,
                                is_restricted: false,
                            }),
                            fields: vec![
                                field_schema_int(
                                    "CONTROL_WORD_TYPE",
                                    "制御ワードタイプ",
                                ),
                                field_schema_int(
                                    "VERSION_NUMBER",
                                    "バージョン番号",
                                ),
                                field_schema_int(
                                    "STATUS_FIELD",
                                    "ステータスフィールド",
                                ),
                                field_schema_int(
                                    "COP_IN_EFFECT",
                                    "COP有効フラグ",
                                ),
                                field_schema_int(
                                    "VCID",
                                    "VCID",
                                ),
                                field_schema_int(
                                    "NO_RF_AVAILABLE",
                                    "RF利用不可フラグ",
                                ),
                                field_schema_int(
                                    "NO_BIT_LOCK",
                                    "ビットロック不可フラグ",
                                ),
                                field_schema_int(
                                    "LOCKOUT",
                                    "ロックアウトフラグ",
                                ),
                                field_schema_int(
                                    "WAIT",
                                    "ウェイトフラグ",
                                ),
                                field_schema_int(
                                    "RETRANSMIT",
                                    "再送信フラグ",
                                ),
                                field_schema_int(
                                    "FARM_B_COUNTER",
                                    "FARM-Bカウンタ",
                                ),
                                field_schema_int(
                                    "REPORT_VALUE",
                                    "VR値",
                                ),
                            ],
                        }
                    )
                ].into_iter().collect(),
            }
        ),
    ].into_iter().collect()
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

pub fn build_clcw_tmiv(time: time::SystemTime, clcw: &CLCW) -> Tmiv {
    let plugin_received_time = time
        .duration_since(time::UNIX_EPOCH)
        .expect("incorrect system clock")
        .as_secs();
    let mut fields = vec![];
    build_tmiv_fields_from_clcw(&mut fields, clcw);
    Tmiv {
        name: CLCW_TMIV_NAME.to_string(),
        plugin_received_time,
        timestamp: Some(time.into()),
        fields,
    }
}