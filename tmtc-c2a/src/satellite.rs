use std::{sync::Arc, time};

use crate::{
    registry::TelemetryRegistry,
    tmiv,
};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use gaia_ccsds_c2a::{
    ccsds::{
        self, aos,
        tc::clcw::CLCW,
    },
    ccsds_c2a::{
        self,
        aos::{virtual_channel::Demuxer, SpacePacket},
    },
};
use gaia_tmtc::{
    cop::CopCommand,
    tco_tmiv::{Tco, Tmiv},
    Handle,
};
use tokio::sync::{broadcast, mpsc, oneshot};
use tracing::{debug, error, warn};

pub struct TmivBuilder {
    tlm_registry: TelemetryRegistry,
}

impl TmivBuilder {
    pub fn new(tlm_registry: TelemetryRegistry) -> Self {
        Self { tlm_registry }
    }

    fn build(
        &self,
        plugin_received_time: time::SystemTime,
        space_packet_bytes: &[u8],
        space_packet: ccsds::SpacePacket<&[u8]>,
    ) -> Result<Vec<Tmiv>> {
        let plugin_received_time_secs = plugin_received_time
            .duration_since(time::UNIX_EPOCH)
            .expect("incorrect system clock")
            .as_secs();

        let space_packet = SpacePacket::from_generic(space_packet)
            .ok_or_else(|| anyhow!("space packet is too short"))?;
        let apid = space_packet.primary_header.apid();
        let tlm_id = space_packet.secondary_header.telemetry_id();
        let Some(telemetry) = self.tlm_registry.lookup(apid, tlm_id) else {
            return Err(anyhow!("unknown tlm_id: {tlm_id} from apid: {apid}"));
        };
        let channels = self
            .tlm_registry
            .find_channels(space_packet.secondary_header.destination_flags());
        let mut fields = vec![];
        tmiv::FieldsBuilder::new(&telemetry.schema).build(&mut fields, space_packet_bytes)?;
        let tmivs = channels
            .map(|channel| {
                let name = telemetry.build_tmiv_name(channel);
                Tmiv {
                    name: name.to_string(),
                    plugin_received_time: plugin_received_time_secs,
                    timestamp: Some(plugin_received_time.into()),
                    fields: fields.clone(),
                }
            })
            .collect();
        Ok(tmivs)
    }
}

pub type CopTaskId = u32;

pub fn create_cop_task_channel() -> (CopTaskSender, CopTaskReceiver) {
    let (tx, rx) = mpsc::channel(256);
    (CopTaskSender { tx }, CopTaskReceiver { rx })
}

#[derive(Clone)]
pub struct CopTaskSender {
    tx: mpsc::Sender<(Arc<Tco>, oneshot::Sender<Result<Option<CopTaskId>>>)>,
}

impl CopTaskSender {
    pub async fn send(&self, tco: Arc<Tco>) -> Result<Result<Option<CopTaskId>>> {
        let (tx, rx) = oneshot::channel();
        self.tx.send((tco, tx)).await?;
        Ok(rx.await?)
    }
}

pub struct CopTaskReceiver {
    rx: mpsc::Receiver<(Arc<Tco>, oneshot::Sender<Result<Option<CopTaskId>>>)>,
}

impl CopTaskReceiver {
    pub async fn recv(
        &mut self,
    ) -> Option<(Arc<Tco>, oneshot::Sender<Result<Option<CopTaskId>>>)> {
        self.rx.recv().await
    }
}

pub fn create_clcw_channel() -> (CLCWSender, CLCWReceiver) {
    let (tx, rx) = broadcast::channel(16);
    (CLCWSender { tx }, CLCWReceiver { rx })
}

#[derive(Clone)]
pub struct CLCWSender {
    tx: broadcast::Sender<CLCW>,
}

impl CLCWSender {
    pub async fn send(&self, clcw: CLCW) -> Result<()> {
        self.tx.send(clcw)?;
        Ok(())
    }

    pub fn subscribe(&self) -> CLCWReceiver {
        CLCWReceiver {
            rx: self.tx.subscribe(),
        }
    }
}

pub struct CLCWReceiver {
    rx: broadcast::Receiver<CLCW>,
}

impl CLCWReceiver {
    pub async fn recv(&mut self) -> Option<CLCW> {
        self.rx.recv().await.ok()
    }
}

pub fn create_cop_command_channel() -> (CopCommandSender, CopCommandReceiver) {
    let (tx, rx) = mpsc::channel(16);
    (CopCommandSender { tx }, CopCommandReceiver { rx })
}

pub struct CopCommandSender {
    tx: mpsc::Sender<(CopCommand, oneshot::Sender<Result<()>>)>,
}

impl CopCommandSender {
    pub async fn send(&self, command: CopCommand) -> Result<Result<()>> {
        let (tx, rx) = oneshot::channel();
        self.tx.send((command, tx)).await?;
        Ok(rx.await?)
    }
}

pub struct CopCommandReceiver {
    rx: mpsc::Receiver<(CopCommand, oneshot::Sender<Result<()>>)>,
}

impl CopCommandReceiver {
    pub async fn recv(&mut self) -> Option<(CopCommand, oneshot::Sender<Result<()>>)> {
        self.rx.recv().await
    }
}

#[derive(Clone)]
pub struct Service {
    task_tx: CopTaskSender,
}

impl Service {
    pub fn new(task_tx: CopTaskSender) -> Self {
        Self {
            task_tx,
        }
    }

    async fn try_handle_command(&mut self, tco: Arc<Tco>) -> Result<Option<CopTaskId>> {
        let response = self.task_tx.send(tco).await??;
        Ok(response)
    }
}

#[async_trait]
impl Handle<Arc<Tco>> for Service {
    type Response = (Option<CopTaskId>, bool);

    async fn handle(&mut self, tco: Arc<Tco>) -> Result<Self::Response> {
        Ok((self.try_handle_command(tco).await?, true))
    }
}

pub struct TelemetryReporter<R> {
    #[allow(unused)]
    aos_scid: u8,
    tmiv_builder: TmivBuilder,
    receiver: R,
    clcw_tx: CLCWSender,
}

impl<R> TelemetryReporter<R>
where
    R: aos::SyncAndChannelCoding,
{
    pub fn new(aos_scid: u8, receiver: R, tmiv_builder: TmivBuilder, clcw_tx: CLCWSender) -> Self {
        Self {
            aos_scid,
            receiver,
            tmiv_builder,
            clcw_tx,
        }
    }

    pub async fn run<H>(mut self, mut tlm_handler: H) -> Result<()>
    where
        H: Handle<Arc<Tmiv>, Response = ()>,
    {
        let mut demuxer = Demuxer::default();
        loop {
            let tf_buf = self.receiver.receive().await?;
            let mut plugin_received_time = time::SystemTime::now();
            let tf: Option<ccsds_c2a::aos::TransferFrame<_>> = tf_buf.transfer_frame();
            let Some(tf) = tf else {
                let bytes = tf_buf.into_inner();
                warn!(
                    "transfer frame is too short ({} bytes): {:02x?}",
                    bytes.len(),
                    bytes
                );
                continue;
            };
            let incoming_scid = tf.primary_header.scid();
            if incoming_scid != self.aos_scid {
                warn!("unknown SCID: {incoming_scid}");
                continue;
            }
            let vcid = tf.primary_header.vcid();
            self.clcw_tx.send(tf.trailer.clone()).await?;
            let channel = demuxer.demux(vcid);
            let frame_count = tf.primary_header.frame_count();
            if let Err(expected) = channel.synchronizer.next(frame_count) {
                warn!(
                    %vcid,
                    "some transfer frames has been dropped: expected frame count: {} but got {}",
                    expected, frame_count,
                );
                channel.defragmenter.reset();
            }
            if let Err(e) = channel.defragmenter.push(tf.data_unit_zone) {
                warn!(%vcid, "malformed M_PDU: {}", e);
                channel.synchronizer.reset();
                channel.defragmenter.reset();
                continue;
            }
            while let Some((space_packet_bytes, space_packet)) =
                channel.defragmenter.read_as_bytes_and_packet()
            {
                if space_packet.primary_header.is_idle_packet() {
                    debug!("skipping idle packet");
                } else {
                    match self.tmiv_builder.build(
                        plugin_received_time,
                        space_packet_bytes,
                        space_packet,
                    ) {
                        Ok(tmivs) => {
                            for tmiv in tmivs {
                                if let Err(e) = tlm_handler.handle(Arc::new(tmiv)).await {
                                    error!("failed to handle telemetry: {:?}", e);
                                }
                            }
                        }
                        Err(e) => {
                            warn!(%vcid, "failed to build TMIV from space packet: {}", e);
                            channel.defragmenter.reset();
                            break;
                        }
                    };
                    // NOTE: workaround to avoid timestamp collision
                    plugin_received_time += time::Duration::from_nanos(1);
                }
                channel.defragmenter.advance();
            }
        }
    }
}
