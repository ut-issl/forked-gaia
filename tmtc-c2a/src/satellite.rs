use std::{sync::Arc, time};

use crate::{
    registry::{CommandRegistry, FatCommandSchema, TelemetryRegistry}, tco::{self, ParameterListWriter}, tmiv
};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use gaia_ccsds_c2a::{
    ccsds::{self, aos, tc::{self, clcw::CLCW}},
    ccsds_c2a::{
        self,
        aos::{virtual_channel::Demuxer, SpacePacket},
        tc::{segment, space_packet},
    },
};
use gaia_tmtc::{
    cop::CopCommand, tco_tmiv::{Tco, Tmiv}, Handle
};
use tokio::sync::{mpsc, oneshot};
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

#[derive(Clone)]
pub struct CommandContext {
    tc_scid: u16,
    fat_schema: FatCommandSchema,
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

    pub async fn transmit_to<T>(&self, sync_and_channel_coding: &mut T, vs: Option<u8>) -> Result<()>
    where
        T: tc::SyncAndChannelCoding,
    {
        let vcid = 0; // FIXME: make this configurable

        let (frame_type, sequence_number) = match (self.tco.is_type_ad, vs) {
            (true, Some(vs)) => (tc::sync_and_channel_coding::FrameType::TypeAD, vs),
            (false, None) => (tc::sync_and_channel_coding::FrameType::TypeBD, 0),
            (true, None) => {
                return Err(anyhow!("VS is required for Type-AD"));
            },
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

pub type FopCommandId = u32;

pub fn create_fop_channel() -> (FopSender, FopReceiver) {
    let (tx, rx) = mpsc::channel(256);
    (FopSender { tx }, FopReceiver { rx })
}

#[derive(Clone)]
pub struct FopSender {
    tx: mpsc::Sender<(CommandContext,oneshot::Sender<Result<Option<FopCommandId>>>)>,
}

impl FopSender {
    pub async fn send(&self, ctx: CommandContext) -> Result<Result<Option<FopCommandId>>> {
        let (tx, rx) = oneshot::channel();
        self.tx.send((ctx, tx)).await?;
        Ok(rx.await?)
    }
}

pub struct FopReceiver {
    rx: mpsc::Receiver<(CommandContext,oneshot::Sender<Result<Option<FopCommandId>>>)>,
}

impl FopReceiver {
    pub async fn recv(&mut self) -> Option<(CommandContext,oneshot::Sender<Result<Option<FopCommandId>>>)> {
        self.rx.recv().await
    }
}

pub fn create_clcw_channel() -> (CLCWSender, CLCWReceiver) {
    let (tx, rx) = mpsc::channel(16);
    (CLCWSender { tx }, CLCWReceiver { rx })
}

#[derive(Clone)]
pub struct CLCWSender {
    tx: mpsc::Sender<CLCW>,
}

impl CLCWSender {
    pub async fn send(&self, clcw: CLCW) -> Result<()> {
        self.tx.send(clcw).await?;
        Ok(())
    }
}

pub struct CLCWReceiver {
    rx: mpsc::Receiver<CLCW>,
}

impl CLCWReceiver {
    pub async fn recv(&mut self) -> Option<CLCW> {
        self.rx.recv().await
    }
}

pub fn create_cop_command_channel() -> (CopCommandSender, CopCommandReceiver) {
    let (tx, rx) = mpsc::channel(16);
    (CopCommandSender { tx }, CopCommandReceiver { rx })
}

pub struct CopCommandSender {
    tx: mpsc::Sender<(CopCommand, oneshot::Sender<Result<bool>>)>,
}

impl CopCommandSender {
    pub async fn send(&self, command: CopCommand) -> Result<Result<bool>> {
        let (tx, rx) = oneshot::channel();
        self.tx.send((command, tx)).await?;
        Ok(rx.await?)
    }
}

pub struct CopCommandReceiver {
    rx: mpsc::Receiver<(CopCommand, oneshot::Sender<Result<bool>>)>,
}

impl CopCommandReceiver {
    pub async fn recv(&mut self) -> Option<(CopCommand, oneshot::Sender<Result<bool>>)> {
        self.rx.recv().await
    }
}

#[derive(Clone)]
pub struct Service {
    registry: Arc<CommandRegistry>,
    tc_scid: u16,
    cop_tx: FopSender,
}

impl Service {
    pub fn new(registry: Arc<CommandRegistry>, tc_scid: u16, cop_tx: FopSender) -> Self {
        Self {
            registry,
            tc_scid,
            cop_tx,
        }
    }

    async fn try_handle_command(&mut self, tco: Arc<Tco>) -> Result<Option<FopCommandId>> {
        let Some(fat_schema) = self.registry.lookup(&tco.name) else {
            return Err(anyhow!("unknown command: {}" ,tco.name));
        };
        let ctx = CommandContext {
            tc_scid: self.tc_scid,
            fat_schema,
            tco,
        };
        let response = self.cop_tx.send(ctx).await??;
        Ok(response)
    }
}

#[async_trait]
impl Handle<Arc<Tco>> for Service {
    type Response = Option<FopCommandId>;

    async fn handle(&mut self, tco: Arc<Tco>) -> Result<Self::Response> {
        Ok(self.try_handle_command(tco).await?)
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
