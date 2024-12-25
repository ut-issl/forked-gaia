use std::{sync::Arc, time::SystemTime};

use anyhow::{anyhow, Result};
use axum::async_trait;
use gaia_ccsds_c2a::{ccsds::tc::{self, clcw::CLCW}, ccsds_c2a::tc::{segment, space_packet}};
use gaia_tmtc::{cop::{cop_command, CopCommand, CopQueueStatusSet, CopTaskStatus, CopVsvr, CopWorkerStatus}, tco_tmiv::{Tco, Tmiv}, Handle};
use prost_types::Timestamp;
use tokio::sync::{broadcast, Mutex, RwLock};
use tracing::{error, warn};

use crate::{fop::tlmcmd::build_clcw_tmiv, registry::{CommandRegistry, FatCommandSchema}, satellite::{CLCWReceiver, CopCommandReceiver, CopCommandSender, CopTaskReceiver}, tco::{self, ParameterListWriter}};

use super::state_machine::FopStateMachine;

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

pub struct Service {
    pub command_tx: CopCommandSender,
}

impl Service {
    async fn try_handle_command(&mut self, command: CopCommand) -> Result<()> {
        if command.command.is_none() {
            return Err(anyhow!("command is required"));
        }
        self.command_tx.send(command).await??;
        Ok(())
    }
}

#[async_trait]
impl Handle<Arc<CopCommand>> for Service {
    type Response = ();

    async fn handle(&mut self, command: Arc<CopCommand>) -> Result<Self::Response> {
        self.try_handle_command(command.as_ref().clone()).await
    }
}

#[derive(Clone)]
pub struct CommandContext {
    pub tc_scid: u16,
    pub fat_schema: FatCommandSchema,
    pub tco: Arc<Tco>,
}

impl CommandContext {
    fn build_tc_segment(&self, data_field_buf: &mut [u8]) -> Result<usize> {
        let mut segment = segment::Builder::new(data_field_buf).unwrap();
        segment.use_default();

        let space_packet_bytes = segment.body_mut();
        let mut space_packet = space_packet::Builder::new(&mut space_packet_bytes[..]).unwrap();
        let tco_reader = tco::Reader::new(&self.tco);
        let params_writer = ParameterListWriter::new(&self.fat_schema.schema);
        space_packet.use_default();
        let ph = space_packet.ph_mut();
        ph.set_version_number(0); // always zero
        ph.set_apid(self.fat_schema.apid);
        let sh = space_packet.sh_mut();
        sh.set_command_id(self.fat_schema.command_id);
        sh.set_destination_type(self.fat_schema.destination_type);
        sh.set_execution_type(self.fat_schema.execution_type);
        if self.fat_schema.has_time_indicator {
            sh.set_time_indicator(tco_reader.time_indicator()?);
        } else {
            sh.set_time_indicator(0);
        }
        let user_data_len = params_writer.write_all(
            space_packet.user_data_mut(),
            tco_reader.parameters().into_iter(),
        )?;
        let space_packet_len = space_packet.finish(user_data_len);
        let segment_len = segment.finish(space_packet_len);
        Ok(segment_len)
    }

    pub async fn transmit_to<T>(
        &self,
        sync_and_channel_coding: &mut T,
        vs: Option<u8>,
    ) -> Result<()>
    where
        T: tc::SyncAndChannelCoding + ?Sized,
    {
        let vcid = 0; // FIXME: make this configurable

        let (frame_type, sequence_number) = match (self.tco.is_type_ad, vs) {
            (true, Some(vs)) => (tc::sync_and_channel_coding::FrameType::TypeAD, vs),
            (false, None) => (tc::sync_and_channel_coding::FrameType::TypeBD, 0),
            (true, None) => {
                return Err(anyhow!("VS is required for Type-AD"));
            }
            (false, Some(_)) => {
                warn!("VS is not allowed for Type-BD. Ignoring VS.");
                (tc::sync_and_channel_coding::FrameType::TypeBD, 0)
            }
        };

        let mut data_field = vec![0u8; 1017]; // FIXME: hard-coded max size
        let segment_len = self.build_tc_segment(&mut data_field)?;
        data_field.truncate(segment_len);
        sync_and_channel_coding
            .transmit(self.tc_scid, vcid, frame_type, sequence_number, &data_field)
            .await?;
        Ok(())
    }
}

type SplitedFopWorker<T> = (
    CopTaskReceiver,
    CLCWReceiver,
    CopCommandReceiver,
    Arc<Mutex<FopStateMachine<T>>>,
    T,
    Arc<CommandRegistry>,
    u16,
);

pub struct FopWorker<T> 
where
    T: tc::SyncAndChannelCoding + Clone + Send + Sync + 'static,
{
    task_rx: CopTaskReceiver,
    clcw_rx: CLCWReceiver,
    command_rx: CopCommandReceiver,
    state_machine: Arc<Mutex<FopStateMachine<T>>>,
    sync_and_channel_coding: T,
    registry: Arc<CommandRegistry>,
    tc_scid: u16,
}

impl<T> FopWorker<T>
where
    T: tc::SyncAndChannelCoding + Clone + Send + Sync + 'static,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        task_rx: CopTaskReceiver,
        clcw_rx: CLCWReceiver,
        command_rx: CopCommandReceiver,
        queue_status_tx: broadcast::Sender<CopQueueStatusSet>,
        worker_state_tx: broadcast::Sender<CopWorkerStatus>,
        task_status_tx: broadcast::Sender<CopTaskStatus>,
        sync_and_channel_coding: T,
        tc_scid: u16,
        registry: Arc<CommandRegistry>
    ) -> Self {
        let state_machine = Arc::new(Mutex::new(FopStateMachine::new(
            worker_state_tx,
            queue_status_tx,
            task_status_tx,
            sync_and_channel_coding.clone(),
            tc_scid,
        )));
        Self {
            task_rx,
            clcw_rx,
            command_rx,
            state_machine,
            sync_and_channel_coding,
            registry,
            tc_scid,
        }
    }

    fn split_self(
        self,
    ) -> SplitedFopWorker<T> {
        (
            self.task_rx,
            self.clcw_rx,
            self.command_rx,
            self.state_machine.clone(),
            self.sync_and_channel_coding.clone(),
            self.registry.clone(),
            self.tc_scid,
        )
    }

    pub async fn run(self) -> Result<()> {
        let (
            mut command_rx,
            mut clcw_rx,
            mut cop_command_rx,
            state_machine,
            mut sync_and_channel_coding,
            registry,
            tc_scid
        ) = self.split_self();
        let state_machine_clone = state_machine.clone();
        let timeout_task = async move {
            let mut instant = tokio::time::interval(tokio::time::Duration::from_secs(1));
            instant.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                instant.tick().await;
                {
                    let mut sm_locked = state_machine_clone.lock().await;
                    sm_locked.evaluate_timeout();
                }
            }
        };
        let state_machine_clone = state_machine.clone();
        let update_variable_task = async move {
            let mut last_clcw_opt: Option<CLCW> = None;
            while let Some(clcw) = clcw_rx.recv().await {
                let mut sm_locked = state_machine_clone.lock().await;
                sm_locked.clcw_received();
                if let Some(last_clcw) = &last_clcw_opt {
                    if last_clcw.clone().into_bytes() == clcw.clone().into_bytes() {
                        continue;
                    } 
                }
                sm_locked.set_clcw(&clcw);
                last_clcw_opt = Some(clcw);
            }
            Err(anyhow!("CLCW connection has gone"))
        };
        let state_machine_clone = state_machine.clone();
        let registry_clone = registry.clone();
        let append_command_task = async move {
            while let Some((tco, tx)) = command_rx.recv().await {
                let Some(fat_schema) = registry_clone.lookup(&tco.name) else {
                    return Err(anyhow!("unknown command: {}", tco.name));
                };
                let ctx = CommandContext {
                    tc_scid,
                    fat_schema,
                    tco,
                };
                let ret = if ctx.tco.is_type_ad {
                    let mut sm_locked = state_machine_clone.lock().await;
                    sm_locked.append(ctx)
                } else if let Err(e) = ctx
                    .transmit_to(&mut sync_and_channel_coding, None)
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
            Err(anyhow!("FOP command receiver has gone"))
        };
        let state_machine_clone = state_machine.clone();
        let execute_command_task = async {
            let mut instant = tokio::time::interval(tokio::time::Duration::from_millis(10));
            instant.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                instant.tick().await;
                {
                    let mut sm_locked = state_machine_clone.lock().await;
                    sm_locked.execute().await;
                }
            }
        };
        let state_machine_clone = state_machine.clone();
        let registry_clone = registry.clone();
        let cop_command_task = async move {
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
                        let mut sm_locked = state_machine_clone.lock().await;
                        let ret = sm_locked.start_initializing(inner.vsvr as u8);
                        if tx.send(ret).is_err() {
                            error!("response receiver has gone");
                        }
                    }
                    cop_command::Command::Unlock(_) => {
                        let mut sm_locked = state_machine_clone.lock().await;
                        let ret = sm_locked.start_unlocking();
                        if tx.send(ret).is_err() {
                            error!("response receiver has gone");
                        }
                    }
                    cop_command::Command::Terminate(_) => {
                        let mut sm_locked = state_machine_clone.lock().await;
                        let ret = sm_locked.cancel();
                        if tx.send(ret).is_err() {
                            error!("response receiver has gone");
                        }
                    }
                    cop_command::Command::SetTimeout(inner) => {
                        let mut sm_locked = state_machine_clone.lock().await;
                        sm_locked.set_timeout_sec(inner.timeout_sec);
                        if tx.send(Ok(())).is_err() {
                            error!("response receiver has gone");
                        }
                    }
                    cop_command::Command::SetAutoRetransmitEnable(_) => {
                        let mut sm_locked = state_machine_clone.lock().await;
                        let ret = sm_locked.auto_retransmit_enable();
                        if tx.send(ret).is_err() {
                            error!("response receiver has gone");
                        }
                    }
                    cop_command::Command::SetAutoRetransmitDisable(_) => {
                        let mut sm_locked = state_machine_clone.lock().await;
                        let ret = sm_locked.auto_retransmit_disable();
                        if tx.send(ret).is_err() {
                            error!("response receiver has gone");
                        }
                    }
                    cop_command::Command::SendSetVr(inner) => {
                        let mut sm_locked = state_machine_clone.lock().await;
                        let ret = sm_locked.send_set_vr_command(inner.vr as u8).await;
                        if tx.send(ret).is_err() {
                            error!("response receiver has gone");
                        }
                    }
                    cop_command::Command::SendUnlock(_) => {
                        let mut sm_locked = state_machine_clone.lock().await;
                        let ret =  sm_locked.send_unlock_command().await;
                        if tx.send(ret).is_err() {
                            error!("response receiver has gone");
                        }
                    }
                    cop_command::Command::BreakPointConfirm(inner) => {
                        let mut sm_locked = state_machine_clone.lock().await;
                        let tco = match inner.confirmation_tco {
                            None => {
                                if tx.send(Err(anyhow!("confirmation TCO is required"))).is_err() {
                                    error!("response receiver has gone");
                                }
                                continue;
                            }
                            Some(tco) => Arc::new(tco),
                        };
                        let Some(fat_schema) = registry_clone.lookup(&tco.name) else {
                            return Err(anyhow!("unknown command: {}", tco.name));
                        };
                        let ctx = CommandContext {
                            tc_scid,
                            fat_schema,
                            tco,
                        };
                        let ret = sm_locked.break_point_confirm(ctx);
                        if tx.send(ret).is_err() {
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