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
use gaia_tmtc::cop::{cop_command, CopCommand, CopQueueStatus, CopQueueStatusPattern, CopStatus, CopWorkerStatus, CopWorkerStatusPattern};
use gaia_tmtc::Handle;
use tokio::sync::{broadcast, Mutex, RwLock};
use async_trait::async_trait;
use tracing::error;

use crate::proto::tmtc_generic_c2a::{TelemetryChannelSchema, TelemetryChannelSchemaMetadata, TelemetryComponentSchema, TelemetryComponentSchemaMetadata, TelemetrySchema, TelemetrySchemaMetadata};
use crate::satellite::{self, create_clcw_channel, create_cop_command_channel, create_fop_channel, CLCWReceiver, CommandContext, CopCommandReceiver, CopCommandSender, FopCommandId, TelemetryReporter, TimeOutResponse, TmivBuilder};
use crate::tco_tmiv_util::{field_optenum, field_optint, field_schema_enum, field_schema_int};
use crate::{satellite::FopReceiver, tco_tmiv_util::{field_enum, field_int}};

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
    let (cop_tx, cop_rx) = create_fop_channel();
    let (clcw_tx, _) = create_clcw_channel();
    let (cop_command_tx, cop_command_rx) = create_cop_command_channel();
    let (queue_tx, queue_rx) = broadcast::channel(10);
    let (state_tx, state_rx) = broadcast::channel(10);
    let (queue_command_tx, queue_command_rx) = broadcast::channel(10);
    (
        satellite::Service::new(
            cmd_registry.into(),
            tc_scid,
            cop_tx,
        ),
        TelemetryReporter::new(
            aos_scid,
            receiver,
            TmivBuilder::new(tlm_registry),
            clcw_tx.clone(),
        ),
        FopWorker::new(
            tc_scid, 
            transmitter, cop_rx, 
            clcw_tx.subscribe(), 
            cop_command_rx,
            queue_tx,
            state_tx,
            queue_command_tx,
        ),
        Service {
            cop_command_tx,
        },
        Reporter::new(
            state_rx, 
            queue_rx,
            queue_command_rx,
            clcw_tx.subscribe()
        )
    )
}

#[derive(Debug, PartialEq, Eq, Clone, Copy, Hash)]
pub enum State {
    Idle,
    Initialize,
    Active,
    LockOut,
    Unlocking,
    TimeOut,
    Failed,
    Canceled,
}

impl Default for State {
    fn default() -> Self {
        Self::Idle
    }
}

pub struct Reporter {
    state_rx: broadcast::Receiver<State>,
    queue_rx: broadcast::Receiver<FopQueueStatus>,
    queue_command_rx: broadcast::Receiver<CopStatus>,
    clcw_rx: CLCWReceiver,
    last_state: Arc<Mutex<State>>,
    last_queue: Arc<Mutex<FopQueueStatus>>,
}

impl Reporter {
    pub fn new(state_rx: broadcast::Receiver<State>, queue_rx: broadcast::Receiver<FopQueueStatus>, queue_command_rx: broadcast::Receiver<CopStatus>, clcw_rx: CLCWReceiver) -> Self {
        Self {
            state_rx,
            queue_rx,
            queue_command_rx,
            clcw_rx,
            last_state: Arc::new(Mutex::new(State::default())),
            last_queue: Arc::new(Mutex::new(FopQueueStatus::default())),
        }
    }

    pub async fn run<TLM, ST>(mut self, tlm_handler: TLM, status_handler: ST) -> Result<()>
    where 
        ST: Handle<Arc<CopStatus>, Response = ()> + Clone,
        TLM: Handle<Arc<Tmiv>, Response = ()> + Clone,
    {
        let mut tlm_handler_clone = tlm_handler.clone();
        let mut status_handler_clone = status_handler.clone();
        let state_rx_task = async {
            loop {
                let state = self.state_rx.recv().await?;
                {
                    let mut last_state = self.last_state.lock().await;
                    *last_state = state;
                }
                let now = time::SystemTime::now();
                let tmiv = {
                    let queue = self.last_queue.lock().await;
                    build_fop_tmiv(now, state, &queue)   
                };
                if let Err(e) = tlm_handler_clone.handle(Arc::new(tmiv)).await {
                    error!("failed to send TMIV: {}", e);
                }
                if let Err(e) = status_handler_clone.handle(Arc::new(CopStatus {
                    inner: Some(Inner::WorkerStatus(CopWorkerStatus {
                        status: {
                            match state {
                                State::Idle => CopWorkerStatusPattern::WorkerIdle as i32,
                                State::Initialize => CopWorkerStatusPattern::WorkerInitialize as i32,
                                State::Active => CopWorkerStatusPattern::WorkerActive as i32,
                                State::LockOut => CopWorkerStatusPattern::WorkerLockout as i32,
                                State::Unlocking => CopWorkerStatusPattern::WorkerUnlocking as i32,
                                State::TimeOut => CopWorkerStatusPattern::WorkerTimeout as i32,
                                State::Failed => CopWorkerStatusPattern::WorkerFailed as i32,
                                State::Canceled => CopWorkerStatusPattern::WorkerCanceled as i32,
                            }
                        },
                        timestamp: Some(now.into()),
                    }))
                })).await {
                    error!("failed to send COP status: {}", e);
                }
            }
        };
        let mut tlm_handler_clone = tlm_handler.clone();
        let queue_rx_task = async {
            loop {
                let queue = self.queue_rx.recv().await?;
                {
                    let mut last_queue = self.last_queue.lock().await;
                    *last_queue = queue.clone();
                }
                let now = time::SystemTime::now();
                let tmiv = {
                    let state = self.last_state.lock().await;
                    build_fop_tmiv(now, *state, &queue)
                };
                if let Err(e) = tlm_handler_clone.handle(Arc::new(tmiv)).await {
                    error!("failed to send TMIV: {}", e);
                }
            }
        };
        let mut tlm_handler_clone = tlm_handler.clone();
        let clcw_rx_task = async {
            loop {
                let clcw = self.clcw_rx.recv().await.ok_or(anyhow!("CLCW connection has gone"))?;
                let now = time::SystemTime::now();
                let tmiv = build_clcw_tmiv(now, &clcw);
                if let Err(e) = tlm_handler_clone.handle(Arc::new(tmiv)).await {
                    error!("failed to send TMIV: {}", e);
                }
            }
        };
        let mut status_handler_clone = status_handler.clone();
        let queue_command_rx_task = async {
            loop {
                let status = self.queue_command_rx.recv().await?;
                if let Err(e) = status_handler_clone.handle(Arc::new(status)).await {
                    error!("failed to send COP status: {}", e);
                }
            }
        };
        tokio::select! {
            ret = state_rx_task => ret,
            ret = queue_rx_task => ret,
            ret = clcw_rx_task => ret,
            ret = queue_command_rx_task => ret,
        }
    }
}

#[derive(Debug, Clone)]
pub struct FopVariables {
    is_active: Arc<AtomicBool>,
    state: State,
    last_clcw: CLCW,
    state_tx: broadcast::Sender<State>,
}

impl FopVariables {
    pub fn new(state_tx: broadcast::Sender<State>) -> Self {
        Self {
            is_active: Arc::new(AtomicBool::new(false)),
            state: State::default(),
            last_clcw: CLCW::default(),
            state_tx,
        }
    }

    pub fn set_state(&mut self, state: State) {
        self.state = state;
        if state == State::Active {
            self.is_active.store(true, std::sync::atomic::Ordering::Relaxed);
        } else {
            self.is_active.store(false, std::sync::atomic::Ordering::Relaxed);
        }
        if let Err(e) = self.state_tx.send(state) {
            error!("failed to send FOP state: {}", e);
        }
    }

    pub async fn update_clcw(&mut self, clcw: CLCW) 
    {
        self.last_clcw = clcw.clone();
    }
}

pub type IdOffset = FopCommandId;

#[derive(Debug, Clone, Default)]
pub struct QueueTmivValue {
    front_id: Option<FopCommandId>,
    front_vs: Option<u8>,
    front_tco_name: Option<String>,
    command_num: u32,
}

#[derive(Debug, Clone, Default)]
pub struct FopQueueStatus {
    pending: QueueTmivValue,
    executed: QueueTmivValue,
    rejected: QueueTmivValue,
    oldest_time: Option<SystemTime>,
}

pub struct FopQueue {
    next_id: FopCommandId,
    vs_at_id0: u32,
    executable: Arc<AtomicBool>,
    pending: VecDeque<(FopCommandId, CommandContext)>,
    executed: VecDeque<(FopCommandId, CommandContext, SystemTime)>,
    rejected: VecDeque<(FopCommandId, CommandContext, SystemTime)>,
    status_tx: broadcast::Sender<FopQueueStatus>,
    queue_command_tx: broadcast::Sender<CopStatus>,
}

impl FopQueue {
    pub fn new(status_tx: broadcast::Sender<FopQueueStatus>, queue_command_tx: broadcast::Sender<CopStatus>) -> Self {
        Self {
            next_id: 0,
            vs_at_id0: 0,
            executable: Arc::new(AtomicBool::new(false)),
            pending: VecDeque::new(),
            executed: VecDeque::new(),
            rejected: VecDeque::new(),
            status_tx,
            queue_command_tx,
        }
    }

    fn update_status(&mut self) {
        let pending = {
            if let Some((front_id, ctx)) = self.pending.front() {
                QueueTmivValue {
                    front_id: Some(*front_id),
                    front_vs: Some((front_id + self.vs_at_id0) as u8),
                    front_tco_name: Some(ctx.tco.name.clone()),
                    command_num: self.pending.len() as u32,
                }
            } else {
                QueueTmivValue::default()
            }
        };
        let executed = {
            if let Some((front_id, ctx, _)) = self.executed.front() {
                QueueTmivValue {
                    front_id: Some(*front_id),
                    front_vs: Some((front_id + self.vs_at_id0) as u8),
                    front_tco_name: Some(ctx.tco.name.clone()),
                    command_num: self.executed.len() as u32,
                }
            } else {
                QueueTmivValue::default()
            }
        };
        let rejected = {
            if let Some((front_id, ctx, _)) = self.rejected.front() {
                QueueTmivValue {
                    front_id: Some(*front_id),
                    front_vs: Some((front_id + self.vs_at_id0) as u8),
                    front_tco_name: Some(ctx.tco.name.clone()),
                    command_num: self.rejected.len() as u32,
                }
            } else {
                QueueTmivValue::default()
            }
        };
        let oldest_time = self.executed.front().map(|(_, _, time)| *time).or_else(|| self.rejected.front().map(|(_, _, time)| *time));
        let status = FopQueueStatus {
            pending,
            executed,
            rejected,
            oldest_time,
        };
        if let Err(e) = self.status_tx.send(status) {
            error!("failed to send FOP queue status: {}", e);
        }
    }

    pub fn push(&mut self, ctx: CommandContext) -> FopCommandId {
        let id = self.next_id;
        self.executable.store(true, std::sync::atomic::Ordering::Relaxed);
        self.next_id += 1;
        let ret = (id, ctx.tco.as_ref().clone());
        self.pending.push_back((id, ctx));
        self.update_status();
        let id = ret.0;
        let status = CopStatus::from_id_tco(ret, CopQueueStatusPattern::Pending);
        if let Err(e) = self.queue_command_tx.send(status) {
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

        if !self.rejected.is_empty() {
            self.executable.store(true, std::sync::atomic::Ordering::Relaxed);
        } else if !self.pending.is_empty() {
            self.executable.store(true, std::sync::atomic::Ordering::Relaxed);
        } else {
            self.executable.store(false, std::sync::atomic::Ordering::Relaxed);
        }
        let status = CopStatus::from_id_tco((id, ctx.tco.as_ref().clone()), CopQueueStatusPattern::Executed);
        if let Err(e) = self.queue_command_tx.send(status) {
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
            if let Err(e) = self.queue_command_tx.send(CopStatus::from_id_tco(id_tco, CopQueueStatusPattern::Accepted)) {
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
            if let Err(e) = self.queue_command_tx.send(CopStatus::from_id_tco(id_tco, CopQueueStatusPattern::Rejected)) {
                error!("failed to send COP status: {}", e);
            }
        }
        self.update_status();
    }

    pub fn clear(&mut self, status_pattern: CopQueueStatusPattern) {
        self.executable.store(false, std::sync::atomic::Ordering::Relaxed);
        self.update_status();
        let canceled = self.pending.drain(..)
            .chain(self.executed.drain(..).map(|(id, ctx, _)| (id, ctx)))
            .chain(self.rejected.drain(..).map(|(id, ctx, _)| (id, ctx)))
            .map(|(id, ctx)| (id, ctx.tco.as_ref().clone()));
        for id_tco in canceled {
            if let Err(e) = self.queue_command_tx.send(CopStatus::from_id_tco(id_tco, status_pattern)) {
                error!("failed to send COP status: {}", e);
            }
        }
    }

    pub fn set_vs(&mut self, vs: u8) {
        self.clear(CopQueueStatusPattern::Canceled);
        self.vs_at_id0 = vs.wrapping_sub(self.next_id as u8) as u32;
    }
}

pub struct Service {
    cop_command_tx: CopCommandSender,
}

impl Service {
    async fn try_handle_command(&mut self, command: CopCommand) -> Result<TimeOutResponse> {
        if command.command.is_none() {
            return Err(anyhow!("command is required"));
        }
        let response = self.cop_command_tx.send(command).await??;
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
    command_rx: FopReceiver,
    clcw_rx: CLCWReceiver,
    cop_command_rx: CopCommandReceiver,
    queue_rx: broadcast::Receiver<FopQueueStatus>,
}

impl<T> FopWorker<T> 
where
    T: tc::SyncAndChannelCoding + Clone + Send + Sync + 'static,
{
    pub fn new(
        tc_scid: u16, 
        sync_and_channel_coding: T, 
        command_rx: FopReceiver, 
        clcw_rx: CLCWReceiver, 
        cop_command_rx: CopCommandReceiver, 
        queue_tx: broadcast::Sender<FopQueueStatus>, 
        state_tx: broadcast::Sender<State>,
        queue_command_tx: broadcast::Sender<CopStatus>,
    ) -> Self {
        let queue_rx = queue_tx.subscribe();
        Self {
            tc_scid,
            timeout_sec: tokio::time::Duration::from_secs(20),
            variables: FopVariables {
                is_active: Arc::new(AtomicBool::new(false)),
                state: State::default(),
                last_clcw: CLCW::default(),
                state_tx,
            },
            queue: FopQueue::new(queue_tx, queue_command_tx),
            sync_and_channel_coding,
            command_rx,
            clcw_rx,
            cop_command_rx,
            queue_rx,
        }
    }

    fn split_self(self) -> 
    (
        u16,
        Arc<Mutex<tokio::time::Duration>>,
        Arc<RwLock<FopVariables>>, 
        Arc<RwLock<FopQueue>>, 
        T, 
        FopReceiver, 
        CLCWReceiver, 
        CopCommandReceiver,
        broadcast::Receiver<FopQueueStatus>,
    ) {
        (
            self.tc_scid,
            Arc::new(Mutex::new(self.timeout_sec)),
            Arc::new(RwLock::new(self.variables)), 
            Arc::new(RwLock::new(self.queue)), 
            self.sync_and_channel_coding, 
            self.command_rx, 
            self.clcw_rx, 
            self.cop_command_rx,
            self.queue_rx,
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
                if let Some(oldest_time) = oldest_time {
                    if oldest_time.elapsed().unwrap_or_default() > timeout_sec {
                        {
                            let mut variables = variables.write().await;
                            variables.set_state(State::TimeOut);
                        }
                        {
                            let mut queue = queue.write().await;
                            queue.clear(CopQueueStatusPattern::Timeout);
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
                        queue.clear(CopQueueStatusPattern::Lockout);
                    }
                    {
                        let mut variables = variables.write().await;
                        variables.set_state(State::LockOut);
                    }
                } else {
                    let mut variables = variables.write().await;
                    if variables.state == State::LockOut {
                        variables.set_state(State::Idle);
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
                        if let Err(_) = tx.send(Err(anyhow!("COP is not active"))) {
                            error!("response receiver has gone");
                        }
                        continue;
                    }
                    let mut queue = queue.write().await;
                    let id = queue.push(ctx);                    
                    if let Err(_) = tx.send(Ok(Some(id))) {
                        error!("response receiver has gone");
                    }
                } else {
                    let ret = if let Err(e) = ctx.transmit_to(&mut sync_and_channel_coding_clone, None).await {
                        error!("failed to send command: {}", e);
                        Err(anyhow!("failed to transmit COP command"))
                    } else {
                        Ok(None)
                    };
                    if let Err(_) = tx.send(ret) {
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
                        queue.clear(CopQueueStatusPattern::Failed);
                    }
                    {
                        let mut variables = variables.write().await;
                        variables.set_state(State::Failed);
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
                        if let Err(_) = tx.send(Err(anyhow!("command is required"))) {
                            error!("response receiver has gone");
                        }
                        continue;
                    }
                };
                match command_inner {
                    cop_command::Command::Initialize(inner) => {
                        {
                            let mut variables = variables.write().await;
                            if variables.state == State::LockOut {
                                if let Err(_) = tx.send(Err(anyhow!("COP is locked out"))) {
                                    error!("response receiver has gone");
                                }
                                continue;
                            }
                            variables.set_state(State::Initialize);
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
                                variables.set_state(State::Active);
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
                                if let Err(_) = tx.send(ret.map(|_| TimeOutResponse { is_timeout: false })) {
                                    error!("response receiver has gone");
                                }
                            }
                            Err(e) => {
                                error!("timeout: {}", e);
                                {
                                    let mut variables = variables.write().await;
                                    variables.set_state(State::TimeOut);
                                }
                                if let Err(_) = tx.send(Ok(TimeOutResponse { is_timeout: true })) {
                                    error!("response receiver has gone");
                                }
                            }
                        }
                    },
                    cop_command::Command::Unlock(_) => {
                        {
                            let mut variables = variables.write().await;
                            if variables.state != State::LockOut {
                                if let Err(_) = tx.send(Ok(TimeOutResponse { is_timeout: false })) {
                                    error!("response receiver has gone");
                                }
                                continue;
                            }
                            variables.set_state(State::Unlocking);
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
                                if variable.state != State::Unlocking {
                                    break Ok(())
                                }
                            }
                        }}).await;
                        match res {
                            Ok(ret) => {
                                if let Err(_) = tx.send(ret.map(|_| TimeOutResponse { is_timeout: false })) {
                                    error!("response receiver has gone");
                                }
                            }
                            Err(e) => {
                                error!("timeout: {}", e);
                                {
                                    let mut variables = variables.write().await;
                                    variables.set_state(State::TimeOut);
                                }
                                if let Err(_) = tx.send(Ok(TimeOutResponse { is_timeout: true })) {
                                    error!("response receiver has gone");
                                }
                            }
                        }
                    },
                    cop_command::Command::Terminate(_) => {
                        {
                            let mut variables = variables.write().await;
                            variables.set_state(State::Canceled);
                        }
                        {
                            let mut queue = queue.write().await;
                            queue.clear(CopQueueStatusPattern::Canceled);
                        }
                        if let Err(_) = tx.send(Ok(TimeOutResponse { is_timeout: false })) {
                            error!("response receiver has gone");
                        }
                    },
                    cop_command::Command::SetTimeout(inner) => {
                        {
                            let mut timeout_sec = timeout_sec.lock().await;
                            *timeout_sec = tokio::time::Duration::from_secs(inner.timeout_sec as u64);
                        }
                        if let Err(_) = tx.send(Ok(TimeOutResponse { is_timeout: false })) {
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
    fn from_id_tco(id_tco: (FopCommandId, Tco), status: CopQueueStatusPattern) -> Self;
}

impl FromIdTco for CopStatus {
    fn from_id_tco((id, tco): (FopCommandId, Tco), status: CopQueueStatusPattern) -> Self {
        CopStatus {
            inner: Some(Inner::QueueStatus(
                CopQueueStatus {
                    cop_id: id,
                    tco: Some(tco),
                    status: status as i32,
                    timestamp: Some(time::SystemTime::now().into()),
                }
            ))
        }
    }
}

impl Display for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            State::Idle => write!(f, "IDLE"),
            State::Initialize => write!(f, "INITIALIZE"),
            State::Active => write!(f, "ACTIVE"),
            State::LockOut => write!(f, "LOCKOUT"),
            State::Unlocking => write!(f, "UNLOCKING"),
            State::TimeOut => write!(f, "TIMEOUT"),
            State::Failed => write!(f, "FAILED"),
            State::Canceled => write!(f, "CANCELED"),
        }
    }
}

const TMIV_DESTINATION_TYPE: &str = "RT";
const TMIV_COMPONENT_NAME: &str = "GAIA";
const WORKER_TMIV_NAME: &str = "GAIA.FOP";
const CLCW_TMIV_NAME: &str = "GAIA.CLCW";

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
                        WORKER_TMIV_NAME.to_string(),
                        TelemetrySchema {
                            metadata: Some(TelemetrySchemaMetadata {
                                id: 0,
                                is_restricted: false,
                            }),
                            fields: vec!
                            [
                                field_schema_enum(
                                    "FOP1_STATE", 
                                    "FOPの状態",
                                    vec![
                                        ("IDLE".to_string(), 0),
                                        ("INITIALIZE".to_string(), 1), 
                                        ("ACTIVE".to_string(), 2), 
                                        ("LOCKOUT".to_string(), 3), 
                                        ("UNLOCKING".to_string(), 4), 
                                        ("TIMEOUT".to_string(), 5), 
                                        ("FAILED".to_string(), 6), 
                                        ("CANCELED".to_string(), 7)
                                    ].into_iter().collect(),
                                    None,
                                ),
                                field_schema_int(
                                    "FOP1_QUEUE.PENDING.NUMBER",
                                    "待機状態のコマンド数",
                                ),
                                field_schema_int(
                                    "FOP1_QUEUE.PENDING.FRONT_ID",
                                    "待機状態の先頭コマンドID",
                                ),
                                field_schema_int(
                                    "FOP1_QUEUE.PENDING.FRONT_VS",
                                    "待機状態の先頭コマンドVS",
                                ),
                                field_schema_enum(
                                    "FOP1_QUEUE.PENDING.FRONT_TCO_NAME", 
                                    "待機状態の先頭コマンドTCO名",
                                    HashMap::new(),
                                    None,
                                ),
                                field_schema_int(
                                    "FOP1_QUEUE.EXECUTED.NUMBER",
                                    "実行済のコマンド数",
                                ),
                                field_schema_int(
                                    "FOP1_QUEUE.EXECUTED.FRONT_ID",
                                    "実行済の先頭コマンドID",
                                ),
                                field_schema_int(
                                    "FOP1_QUEUE.EXECUTED.FRONT_VS",
                                    "実行済の先頭コマンドVS",
                                ),
                                field_schema_enum(
                                    "FOP1_QUEUE.EXECUTED.FRONT_TCO_NAME",
                                    "実行済の先頭コマンドTCO名",
                                    HashMap::new(),
                                    None,
                                ),
                                field_schema_int(
                                    "FOP1_QUEUE.REJECTED.NUMBER",
                                    "再送待ちのコマンド数",
                                ),
                                field_schema_int(
                                    "FOP1_QUEUE.REJECTED.FRONT_ID",
                                    "再送待ちの先頭コマンドID",
                                ),
                                field_schema_int(
                                    "FOP1_QUEUE.REJECTED.FRONT_VS",
                                    "再送待ちの先頭コマンドVS",
                                ),
                                field_schema_enum(
                                    "FOP1_QUEUE.REJECTED.FRONT_TCO_NAME",
                                    "再送待ちの先頭コマンドTCO名",
                                    HashMap::new(),
                                    None,
                                ),
                                field_schema_enum(
                                    "FOP1_QUEUE.OLDEST_TIME",
                                    "先頭コマンドの受信時刻",
                                    HashMap::new(),
                                    None,
                                ),
                            ],
                        },
                    ),
                    (
                        CLCW_TMIV_NAME.to_string(),
                        TelemetrySchema {
                            metadata: Some(TelemetrySchemaMetadata {
                                id: 0,
                                is_restricted: false,
                            }),
                            fields: vec![
                                field_schema_int(
                                    "CLCW_CONTROL_WORD_TYPE",
                                    "CLCWの制御ワードタイプ",
                                ),
                                field_schema_int(
                                    "CLCW_VERSION_NUMBER",
                                    "CLCWのバージョン番号",
                                ),
                                field_schema_int(
                                    "CLCW_STATUS_FIELD",
                                    "CLCWのステータスフィールド",
                                ),
                                field_schema_int(
                                    "CLCW_COP_IN_EFFECT",
                                    "CLCWのCOP有効フラグ",
                                ),
                                field_schema_int(
                                    "CLCW_VCID",
                                    "CLCWのVCID",
                                ),
                                field_schema_int(
                                    "CLCW_NO_RF_AVAILABLE",
                                    "CLCWのRF利用不可フラグ",
                                ),
                                field_schema_int(
                                    "CLCW_NO_BIT_LOCK",
                                    "CLCWのビットロック不可フラグ",
                                ),
                                field_schema_int(
                                    "CLCW_LOCKOUT",
                                    "CLCWのロックアウトフラグ",
                                ),
                                field_schema_int(
                                    "CLCW_WAIT",
                                    "CLCWのウェイトフラグ",
                                ),
                                field_schema_int(
                                    "CLCW_RETRANSMIT",
                                    "CLCWの再送信フラグ",
                                ),
                                field_schema_int(
                                    "CLCW_FARM_B_COUNTER",
                                    "CLCWのFARM-Bカウンタ",
                                ),
                                field_schema_int(
                                    "CLCW_REPORT_VALUE",
                                    "CLCWのVR値",
                                ),
                            ],
                        }
                    )
                ].into_iter().collect(),
            }
        ),
    ].into_iter().collect()
}

pub fn build_tmiv_fields_from_fop(fields: &mut Vec<TmivField>, state: State, queue: &FopQueueStatus) {
    fields.push(field_enum("FOP1_STATE", state));
    fields.push(field_int("FOP1_QUEUE.PENDING.NUMBER", queue.pending.command_num));
    fields.push(field_optint("FOP1_QUEUE.PENDING.FRONT_ID", queue.pending.front_id));
    fields.push(field_optint("FOP1_QUEUE.PENDING.FRONT_VS", queue.pending.front_vs));
    fields.push(field_optenum("FOP1_QUEUE.PENDING.FRONT_TCO_NAME", queue.pending.front_tco_name.clone()));
    fields.push(field_int("FOP1_QUEUE.EXECUTED.NUMBER", queue.executed.command_num));
    fields.push(field_optint("FOP1_QUEUE.EXECUTED.FRONT_ID", queue.executed.front_id));
    fields.push(field_optint("FOP1_QUEUE.EXECUTED.FRONT_VS", queue.executed.front_vs));
    fields.push(field_optenum("FOP1_QUEUE.EXECUTED.FRONT_TCO_NAME", queue.executed.front_tco_name.clone()));
    fields.push(field_int("FOP1_QUEUE.REJECTED.NUMBER", queue.rejected.command_num));
    fields.push(field_optint("FOP1_QUEUE.REJECTED.FRONT_ID", queue.rejected.front_id));
    fields.push(field_optint("FOP1_QUEUE.REJECTED.FRONT_VS", queue.rejected.front_vs));
    fields.push(field_optenum("FOP1_QUEUE.REJECTED.FRONT_TCO_NAME", queue.rejected.front_tco_name.clone()));
    let oldest_time: Option<chrono::DateTime<chrono::Local>>  = queue.oldest_time.map(|time| time.into());
    fields.push(field_optenum("FOP1_QUEUE.OLDEST_TIME", oldest_time.map(|t| t.format("%Y-%m-%d %H:%M:%S").to_string())));
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

pub fn build_fop_tmiv(time: time::SystemTime, state: State, queue: &FopQueueStatus) -> Tmiv {
    let plugin_received_time = time
        .duration_since(time::UNIX_EPOCH)
        .expect("incorrect system clock")
        .as_secs();
    let mut fields = vec![];
    build_tmiv_fields_from_fop(&mut fields, state, queue);
    Tmiv {
        name: WORKER_TMIV_NAME.to_string(),
        plugin_received_time,
        timestamp: Some(time.into()),
        fields,
    }
}