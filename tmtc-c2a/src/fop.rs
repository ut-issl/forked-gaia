use std::sync::Arc;

use gaia_ccsds_c2a::ccsds::{tc, aos};
use tokio::sync::mpsc;
use worker::{FopWorker, Reporter, Service};

use crate::{registry::{CommandRegistry, TelemetryRegistry}, satellite::{self, create_clcw_channel, create_cop_command_channel, create_cop_task_channel, TelemetryReporter, TmivBuilder}};

pub mod state_machine;
pub mod queue;
pub mod tlmcmd;
pub mod worker;

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
    let (queue_status_tx, queue_status_rx) = mpsc::channel(10);
    let (worker_state_tx, worker_state_rx) = mpsc::channel(10);
    let (task_status_tx, task_status_rx) = mpsc::channel(10);
    (
        satellite::Service::new(task_tx),
        TelemetryReporter::new(
            aos_scid,
            receiver,
            TmivBuilder::new(tlm_registry),
            clcw_tx.clone(),
        ),
        FopWorker::new(
            task_rx,
            clcw_tx.subscribe(),
            command_rx,
            queue_status_tx,
            worker_state_tx,
            task_status_tx,
            transmitter,
            tc_scid,
            cmd_registry.into()
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