use std::sync::atomic::{AtomicBool, AtomicU32};
use std::sync::Arc;
use std::{collections::VecDeque, fmt::Display, time};

use anyhow::{anyhow, Result};
use gaia_ccsds_c2a::ccsds::{tc::{self, clcw::CLCW}, aos};
use gaia_tmtc::cop::cop_status::Inner;
use gaia_tmtc::tco_tmiv::{tmiv, Tco, Tmiv, TmivField};
use gaia_tmtc::cop::{CopCommand, CopQueueStatus, CopQueueStatusPattern, CopStatus};
use gaia_tmtc::Handle;
use tokio::sync::RwLock;
use async_trait::async_trait;
use tracing::error;

use crate::satellite::{self, create_clcw_channel, create_cop_command_channel, create_fop_channel, CLCWReceiver, CommandContext, CopCommandReceiver, CopCommandSender, FopCommandId, TelemetryReporter, TmivBuilder};
use crate::tco_tmiv_util::{field_schema_enum, field_schema_int};
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
) -> (satellite::Service, TelemetryReporter<R>, FopWorker<T>, Service)
where
    T: tc::SyncAndChannelCoding + Clone + Send + Sync + 'static,
    R: aos::SyncAndChannelCoding,
{
    let (cop_tx, cop_rx) = create_fop_channel();
    let (clcw_tx, clcw_rx) = create_clcw_channel();
    let (cop_command_tx, cop_command_rx) = create_cop_command_channel();
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
            clcw_tx,
        ),
        FopWorker::new(transmitter, cop_rx, clcw_rx, cop_command_rx),
        Service {
            cop_command_tx,
        },
    )
}

#[derive(Debug, PartialEq, Eq, Clone, Copy, Hash)]
pub enum State {
    Initial,
    Active,
    LockOut,
    TimeOut,
    Failed
}

impl Default for State {
    fn default() -> Self {
        Self::Initial
    }
}

pub struct FopVariables {
    is_active: Arc<AtomicBool>,
    state: State,
    vs: u8,
    last_clcw: CLCW,
}

pub type IdOffset = FopCommandId;

pub struct FopQueue {
    next_id: FopCommandId,
    head_id: Option<FopCommandId>,
    executable: Arc<AtomicBool>,
    pending: VecDeque<(FopCommandId, CommandContext)>,
    executed: VecDeque<(FopCommandId, CommandContext)>,
    rejected: VecDeque<(FopCommandId, CommandContext)>,
}

impl FopQueue {
    pub fn new() -> Self {
        Self {
            next_id: 0,
            head_id: None,
            executable: Arc::new(AtomicBool::new(false)),
            pending: VecDeque::new(),
            executed: VecDeque::new(),
            rejected: VecDeque::new(),
        }
    }

    pub fn push(&mut self, ctx: CommandContext) -> (FopCommandId, Tco) {
        let id = self.next_id;
        if self.head_id.is_none() {
            self.head_id = Some(id);
        }
        self.executable.store(true, std::sync::atomic::Ordering::Relaxed);
        self.next_id += 1;
        let ret = (id, ctx.tco.as_ref().clone());
        self.pending.push_back((id, ctx));
        ret
    }

    pub fn execute(&mut self) -> Option<(FopCommandId, CommandContext)> {
        let (id, ctx) = match self.rejected.pop_front() {
            Some(id_ctx) => id_ctx,
            None => match self.pending.pop_front() {
                Some(id_ctx) => id_ctx,
                None => return None,
            },
        };
        let ret = (id, ctx);
        self.executed.push_back(ret.clone());

        if !self.rejected.is_empty() {
            self.executable.store(true, std::sync::atomic::Ordering::Relaxed);
            self.head_id = Some(self.rejected.front().unwrap().0);
        } else if !self.pending.is_empty() {
            self.executable.store(true, std::sync::atomic::Ordering::Relaxed);
            self.head_id = Some(self.pending.front().unwrap().0);
        } else {
            self.head_id = None;
            self.executable.store(false, std::sync::atomic::Ordering::Relaxed);
        }
        Some(ret)
    }

    pub fn accept(&mut self, id: FopCommandId) -> Vec<(FopCommandId, Tco)> {
        let accepted_num = match self.head_id {
            Some(head_id) => id - head_id + 1,
            None => return vec![],
        };
        self.executed.drain(0..(accepted_num as usize)).map(|(id, ctx)| (id, ctx.tco.as_ref().clone())).collect()
    }

    pub fn reject(&mut self) -> Vec<(FopCommandId, Tco)> {
        let stash = self.rejected.drain(..);
        let (ret, mut moved): (Vec<_>, Vec<_>) = self.executed.drain(..).map(|(id, ctx)| ((id, ctx.tco.as_ref().clone()), (id, ctx))
        ).unzip();
        self.rejected = moved.drain(..).chain(stash).collect();
        ret
    }

    pub fn clear(&mut self) -> Vec<(FopCommandId, Tco)> {
        self.head_id = None;
        self.executable.store(false, std::sync::atomic::Ordering::Relaxed);
        self.pending.drain(..)
            .chain(self.executed.drain(..))
            .chain(self.rejected.drain(..))
            .map(|(id, ctx)| (id, ctx.tco.as_ref().clone())).collect()
    }
}

pub struct Service {
    cop_command_tx: CopCommandSender,
}

impl Service {
    async fn try_handle_command(&mut self, command: CopCommand) -> Result<bool> {
        if command.command.is_none() {
            return Err(anyhow!("command is required"));
        }
        let response = self.cop_command_tx.send(command).await??;
        Ok(response)
    }
}

#[async_trait]
impl Handle<Arc<CopCommand>> for Service {
    type Response = bool;

    async fn handle(&mut self, command: Arc<CopCommand>) -> Result<Self::Response> {
        self.try_handle_command(command.as_ref().clone()).await
    }
}

pub struct FopWorker<T> {
    variables: Arc<RwLock<FopVariables>>,
    queue: Arc<RwLock<FopQueue>>,
    id_offset_from_vs: Arc<AtomicU32>,
    sync_and_channel_coding: T,
    command_rx: FopReceiver,
    clcw_rx: CLCWReceiver,
    cop_command_rx: CopCommandReceiver,
}

impl<T> FopWorker<T> 
where
    T: tc::SyncAndChannelCoding + Clone + Send + Sync + 'static,
{
    pub fn new(sync_and_channel_coding: T, command_rx: FopReceiver, clcw_rx: CLCWReceiver, cop_command_rx: CopCommandReceiver) -> Self {
        Self {
            variables: Arc::new(RwLock::new(FopVariables {
                is_active: Arc::new(AtomicBool::new(false)),
                state: State::Initial,
                vs: 0,
                last_clcw: CLCW::default(),
            })),
            queue: Arc::new(RwLock::new(FopQueue::new())),
            id_offset_from_vs: Arc::new(AtomicU32::new(0)),
            sync_and_channel_coding,
            command_rx,
            clcw_rx,
            cop_command_rx
        }
    }

    fn split_self(self) -> (Arc<RwLock<FopVariables>>, Arc<RwLock<FopQueue>>, T, FopReceiver, CLCWReceiver, Arc<AtomicU32>) {
        (self.variables, self.queue, self.sync_and_channel_coding, self.command_rx, self.clcw_rx, self.id_offset_from_vs)
    }

    pub async fn run<ST, TLM>(self, status_handler: ST, tlm_handler: TLM) -> Result<()> 
    where 
        ST: Handle<Arc<CopStatus>, Response = ()> + Clone,
        TLM: Handle<Arc<Tmiv>, Response = ()> + Clone,
    {
        let (variables, queue, sync_and_channel_coding, mut command_rx, mut clcw_rx, id_offset_from_vs) = self.split_self();
        let mut status_handler_clone = status_handler.clone();
        let mut tlm_handler_clone = tlm_handler.clone();
        let update_variable_task = async {
            let is_active = {
                let variable = variables.read().await;
                variable.is_active.clone()
            };
            while let Some(clcw) = clcw_rx.recv().await {
                {
                    let mut queue = queue.write().await;
                    for id_tco in queue.accept(clcw.report_value() as FopCommandId + id_offset_from_vs.load(std::sync::atomic::Ordering::Relaxed)) {
                        if let Err(e) = status_handler_clone.handle(Arc::new(CopStatus::from_id_tco(id_tco, CopQueueStatusPattern::Accepted))).await {
                            error!("failed to send COP status: {}", e);
                        }
                    }
                }
                {
                    let mut variables = variables.write().await;
                    if clcw.lockout() == 1 {
                        variables.state = State::LockOut;
                        is_active.store(false, std::sync::atomic::Ordering::Relaxed);
                    } else if clcw.lockout() == 0 && variables.state == State::LockOut {
                        variables.state = State::Initial;
                        is_active.store(false, std::sync::atomic::Ordering::Relaxed);
                    }
                }
                if clcw.retransmit() == 1 {
                    let mut queue = queue.write().await;
                    for id_tco in queue.reject() {
                        if let Err(e) = status_handler_clone.handle(Arc::new(CopStatus::from_id_tco(id_tco, CopQueueStatusPattern::Rejected))).await {
                            error!("failed to send COP status: {}", e);
                        }
                    }
                }
                {
                    let now = time::SystemTime::now();
                    let variable = variables.read().await;
                    let tmiv = build_clcw_tmiv(now, &variable.last_clcw);
                    if let Err(e) = tlm_handler_clone.handle(Arc::new(tmiv)).await {
                        error!("failed to send TMIV: {}", e);
                    }
                }
            }
            Ok::<_,anyhow::Error>(())
        };
        let mut status_handler_clone = status_handler.clone();
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
                    let id_ctx = queue.push(ctx);
                    let id = id_ctx.0;
                    let status = CopStatus::from_id_tco(id_ctx, CopQueueStatusPattern::Pending);
                    if let Err(e) = status_handler_clone.handle(Arc::new(status)).await {
                        error!("failed to send COP status: {}", e);
                    }
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
            Ok::<_,anyhow::Error>(())
        };
        let mut status_handler_clone = status_handler.clone();
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
                let ns = {
                    let variable = variables.read().await;
                    if variable.state != State::Active {
                        instant.tick().await;
                        continue;
                    } else {
                        let (ns, overflowed) = variable.vs.overflowing_add(1);
                        if overflowed {
                            id_offset_from_vs.fetch_add(256, std::sync::atomic::Ordering::Relaxed);
                        }
                        ns
                    }
                };
                let (id, ctx) = {
                    let mut queue = queue.write().await;
                    let Some(id_ctx) = queue.execute() else {
                        error!("failed to execute command");
                        continue;
                    };
                    id_ctx
                };
                let status = CopStatus::from_id_tco((id, ctx.tco.as_ref().clone()), CopQueueStatusPattern::Executed);
                if let Err(e) = status_handler_clone.handle(Arc::new(status)).await {
                    error!("failed to send COP status: {}", e);
                }
                if let Err(e) = ctx.transmit_to(&mut sync_and_channel_coding_clone, Some(ns)).await {
                    error!("failed to send command: {}", e);
                    {
                        let mut queue = queue.write().await;
                        let id_tco = queue.clear();
                        for id_tco in id_tco {
                            if let Err(e) = status_handler_clone.handle(Arc::new(CopStatus::from_id_tco(id_tco, CopQueueStatusPattern::Failed))).await {
                                error!("failed to send COP status: {}", e);
                            }
                        }
                    }
                    {
                        let mut variables = variables.write().await;
                        variables.state = State::Failed;
                        variables.is_active.store(false, std::sync::atomic::Ordering::Relaxed);
                    }
                } else {

                }
            }
        };

        tokio::select! {
            ret = update_variable_task => Ok(ret?),
            ret = append_command_task => Ok(ret?),
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
            State::Initial => write!(f, "INITIAL"),
            State::Active => write!(f, "ACTIVE"),
            State::LockOut => write!(f, "LOCKOUT"),
            State::TimeOut => write!(f, "TIMEOUT"),
            State::Failed => write!(f, "FAILED"),
        }
    }
}

const WORKER_TMIV_NAME: &str = "GAIA.FOP";
const CLCW_TMIV_NAME: &str = "GAIA.CLCW";

pub fn tmiv_schema_set() -> Vec<tmiv::Schema> {
    vec![
        tmiv::Schema {
            name: WORKER_TMIV_NAME.to_string(),
            fields: vec![
                field_schema_enum("FOP1_STATE", &["INITIAL", "ACTIVE", "LOCKOUT", "TIMEOUT", "FAILED"]),
                field_schema_int("FOP1_VS"),
                field_schema_int("ID_OFFSET_FROM_VS"),
            ]
        },
        tmiv::Schema {
            name: CLCW_TMIV_NAME.to_string(),
            fields: vec![
                field_schema_int("CLCW_CONTROL_WORD_TYPE"),
                field_schema_int("CLCW_VERSION_NUMBER"),
                field_schema_int("CLCW_STATUS_FIELD"),
                field_schema_int("CLCW_COP_IN_EFFECT"),
                field_schema_int("CLCW_VCID"),
                field_schema_int("CLCW_NO_RF_AVAILABLE"),
                field_schema_int("CLCW_NO_BIT_LOCK"),
                field_schema_int("CLCW_LOCKOUT"),
                field_schema_int("CLCW_WAIT"),
                field_schema_int("CLCW_RETRANSMIT"),
                field_schema_int("CLCW_FARM_B_COUNTER"),
                field_schema_int("CLCW_REPORT_VALUE"),
            ],
        }
    ]
}

pub fn build_tmiv_fields_from_fop(fields: &mut Vec<TmivField>, state: State, vs: u8, offset: IdOffset) {
    fields.push(field_enum("FOP1_STATE", state));
    fields.push(field_int("FOP1_VS", vs));
    fields.push(field_int("ID_OFFSET_FROM_VS", offset));
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

pub fn build_fop_tmiv(time: time::SystemTime, state: State, vs: u8, offset: IdOffset) -> Tmiv {
    let plugin_received_time = time
        .duration_since(time::UNIX_EPOCH)
        .expect("incorrect system clock")
        .as_secs();
    let mut fields = vec![];
    build_tmiv_fields_from_fop(&mut fields, state, vs, offset);
    Tmiv {
        name: WORKER_TMIV_NAME.to_string(),
        plugin_received_time,
        timestamp: Some(time.into()),
        fields,
    }
}