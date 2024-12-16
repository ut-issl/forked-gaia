use std::collections::VecDeque;

use chrono::{DateTime, Utc};
use gaia_tmtc::{cop::{CopQueueStatus, CopQueueStatusSet, CopTaskStatus, CopTaskStatusPattern}, tco_tmiv::Tco};
use prost_types::Timestamp;
use tokio::sync::broadcast;
use tracing::error;

use crate::satellite::CopTaskId;

use super::worker::CommandContext;


pub type IdOffset = CopTaskId;

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

trait FopQueueStateNode {
    fn get_oldest_arrival_time(&self) -> Option<DateTime<Utc>>;
    fn next_id(&self) -> CopTaskId;
    fn push(&mut self, context: FopQueueContext, cmd_ctx: CommandContext) -> CopTaskId;
    fn execute(&mut self, context: FopQueueContext) -> Option<(u8, CommandContext)>;
    fn accept(&mut self, context: FopQueueContext, vr: u8);
    fn reject(&mut self, context: FopQueueContext);
    fn clear(&mut self, context: FopQueueContext, status_pattern: CopTaskStatusPattern);
}

struct FopQueueContext {
    task_status_tx: broadcast::Sender<CopTaskStatus>,
    queue_status_tx: broadcast::Sender<CopQueueStatusSet>
}

struct ProcessingQueue {
    pending: VecDeque<(CopTaskId, CommandContext)>,
    executed: VecDeque<(CopTaskId, CommandContext, DateTime<Utc>)>,
    oldest_arrival_time: Option<DateTime<Utc>>,
}

pub struct FopQueue {
    next_id: CopTaskId,
    vs_at_id0: u32,
    oldest_arrival_time: Option<DateTime<Utc>>,
    pending: VecDeque<(CopTaskId, CommandContext)>,
    executed: VecDeque<(CopTaskId, CommandContext, DateTime<Utc>)>,
    rejected: VecDeque<(CopTaskId, CommandContext, DateTime<Utc>)>,
    confirmation_cmd: CommandContext,
}

impl FopQueue {
    pub fn new(
        vs: u8,
        next_id: CopTaskId,
        confirmation_cmd: CommandContext,
    ) -> Self {
        Self {
            next_id,
            vs_at_id0: vs.wrapping_sub(next_id as u8) as u32,
            oldest_arrival_time: None,
            pending: VecDeque::new(),
            executed: VecDeque::new(),
            rejected: VecDeque::new(),
            confirmation_cmd,
        }
    }
    pub fn get_oldest_arrival_time(&self) -> Option<DateTime<Utc>> {
        self.oldest_arrival_time
    }
    pub fn next_id(&self) -> CopTaskId {
        self.next_id
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
        self.oldest_arrival_time = oldest_arrival_time;
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
        ctx: CommandContext
    ) -> CopTaskId {
        let id = self.next_id;
        self.next_id += 1;
        let id_tco = (id, ctx.tco.as_ref().clone());
        self.pending.push_back((id, ctx));
        let status = CopTaskStatus::from_id_tco(id_tco, CopTaskStatusPattern::Pending);
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
            Some((id,ctx,time)) => (id,ctx, time),
            None => match self.pending.pop_front() {
                Some((id, qctx)) => (id, qctx, chrono::Utc::now().naive_utc().and_utc()),
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