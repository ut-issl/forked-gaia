use std::collections::VecDeque;

use chrono::{DateTime, Utc};
use gaia_tmtc::{cop::{CopQueueStatus, CopQueueStatusSet, CopTaskStatus, CopTaskStatusPattern, CopQueueStatusPattern}, tco_tmiv::Tco};
use prost_types::Timestamp;
use tokio::sync::broadcast;
use tracing::error;

use crate::satellite::CopTaskId;

use super::worker::CommandContext;


pub type IdOffset = CopTaskId;

trait FromIdTco {
    fn from_id_tco(id_tco: (CopTaskId, Tco), status: CopTaskStatusPattern, is_confirm_command: bool) -> Self;
}

impl FromIdTco for CopTaskStatus {
    fn from_id_tco((id, tco): (CopTaskId, Tco), status: CopTaskStatusPattern, is_confirm_command: bool) -> Self {
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
            is_confirm_command,
        }
    }
}

trait FopQueueStateNode {
    fn get_oldest_arrival_time(&self) -> Option<DateTime<Utc>>;
    fn next_id(&self) -> CopTaskId;
    fn push(&mut self, context: FopQueueContext, cmd_ctx: CommandContext) -> CopTaskId;
    fn confirm(&mut self, context: FopQueueContext, cmd_ctx: CommandContext);
    fn execute(self: Box<Self>, context: FopQueueContext) -> (Box<dyn FopQueueStateNode>, Option<(u8, CommandContext)>);
    fn accept(self: Box<Self>, context: FopQueueContext, vr: u8) -> Box<dyn FopQueueStateNode>;
    fn reject(self: Box<Self>, context: FopQueueContext) -> Box<dyn FopQueueStateNode>;
    fn clear(&mut self, context: FopQueueContext, status_pattern: CopTaskStatusPattern);
}

pub struct FopQueueContext {
    pub task_status_tx: broadcast::Sender<CopTaskStatus>,
    pub queue_status_tx: broadcast::Sender<CopQueueStatusSet>,
}

enum FopQueueTask {
    Process(CommandContext),
    Confirm(CommandContext),
}

impl FopQueueTask {
    fn context(&self) -> CommandContext {
        match self {
            FopQueueTask::Process(ctx) 
            | FopQueueTask::Confirm(ctx) => ctx.clone(),
        }
    }
}

struct ProcessingQueue {
    pending: VecDeque<(CopTaskId, FopQueueTask)>,
    executed: VecDeque<(CopTaskId, CommandContext, DateTime<Utc>)>,
    rejected: VecDeque<(CopTaskId, CommandContext, DateTime<Utc>)>,
    oldest_arrival_time: Option<DateTime<Utc>>,
    next_id: CopTaskId,
    vs_at_id0: u32,
}

impl ProcessingQueue {
    pub fn new(
        vs: u8,
        next_id: CopTaskId,
    ) -> Self {
        Self {
            pending: VecDeque::new(),
            executed: VecDeque::new(),
            rejected: VecDeque::new(),
            oldest_arrival_time: None,
            next_id,
            vs_at_id0: vs.wrapping_sub(next_id as u8) as u32,
        }
    }
    fn update_status(&mut self, queue_status_tx: broadcast::Sender<CopQueueStatusSet>) {
        let (pending_vs, pending) = {
            if let Some((head_id, qctx)) = self.pending.front() {
                let ctx = qctx.context();
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
            status: CopQueueStatusPattern::Processing.into(),
        };
        if let Err(e) = queue_status_tx.send(status) {
            error!("failed to send FOP queue status: {}", e);
        }
    }
}

impl FopQueueStateNode for ProcessingQueue {
    fn get_oldest_arrival_time(&self) -> Option<DateTime<Utc>> {
        self.oldest_arrival_time
    }
    fn next_id(&self) -> CopTaskId {
        self.next_id
    }
    fn push(&mut self, context: FopQueueContext, cmd_ctx: CommandContext) -> CopTaskId {
        let id = self.next_id;
        self.next_id += 1;
        let id_tco = (id, cmd_ctx.tco.as_ref().clone());
        self.pending.push_back((id, FopQueueTask::Process(cmd_ctx.clone())));
        let status = CopTaskStatus::from_id_tco(id_tco, CopTaskStatusPattern::Pending, false);
        if let Err(e) = context.task_status_tx.send(status) {
            error!("failed to send COP status: {}", e);
        }
        self.update_status(context.queue_status_tx);
        id
    }
    fn confirm(&mut self, context: FopQueueContext, cmd_ctx: CommandContext) {
        self.pending.push_back((self.next_id, FopQueueTask::Confirm(cmd_ctx.clone())));
        let status = CopTaskStatus::from_id_tco(
            (self.next_id, cmd_ctx.tco.as_ref().clone()),
            CopTaskStatusPattern::Pending,
            true,
        );
        if let Err(e) = context.task_status_tx.send(status) {
            error!("failed to send COP status: {}", e);
        }
        self.next_id += 1;
        self.update_status(context.queue_status_tx);
    }
    fn execute(
        mut self: Box<Self>,
        context: FopQueueContext,
    ) -> (Box<dyn FopQueueStateNode>, Option<(u8, CommandContext)>) {
        let (id, ctx, time) = match self.rejected.pop_front() {
            Some((id,ctx,time)) => (id,ctx, time),
            None => match self.pending.pop_front() {
                Some((id, FopQueueTask::Process(ctx))) => (id, ctx, chrono::Utc::now().naive_utc().and_utc()),
                Some((id, FopQueueTask::Confirm(ctx))) => {
                    let mut pending = self.pending;
                    pending.push_front((id, FopQueueTask::Confirm(ctx.clone())));
                    let status = CopTaskStatus::from_id_tco(
                        (id, ctx.tco.as_ref().clone()),
                        CopTaskStatusPattern::Executed,
                        true,
                    );
                    if let Err(e) = context.task_status_tx.send(status) {
                        error!("failed to send COP status: {}", e);
                    }
                    return (
                        Box::new(ConfirmingQueue{
                            pending,
                            executed: self.executed,
                            oldest_arrival_time: self.oldest_arrival_time,
                            next_id: self.next_id,
                            vs_at_id0: self.vs_at_id0,
                        }),
                        Some(((id + self.vs_at_id0) as u8, ctx))
                    )
                },
                None => return (self ,None),
            },
        };
        let ret = ((id + self.vs_at_id0) as u8, ctx.clone());
        self.executed.push_back((id, ctx.clone(), time));
        let status = CopTaskStatus::from_id_tco(
            (id, ctx.tco.as_ref().clone()),
            CopTaskStatusPattern::Executed,
            false,
        );
        if let Err(e) = context.task_status_tx.send(status) {
            error!("failed to send COP status: {}", e);
        }
        self.update_status(context.queue_status_tx);
        (self, Some(ret))
    }

    fn accept(
        mut self: Box<Self>, 
        context: FopQueueContext,
        vr: u8,
    ) -> Box<dyn FopQueueStateNode> {
        let accepted_num = if let Some((head_id, _, _)) = self.executed.front() {
            if vr.wrapping_sub((head_id + self.vs_at_id0) as u8) > self.executed.len() as u8 {
                0
            } else {
                vr.wrapping_sub((head_id + self.vs_at_id0) as u8)
            }
        } else {
            return self
        }; 
        let accepted = self
            .executed
            .drain(0..(accepted_num as usize))
            .map(|(id, ctx, _)| (id, ctx.tco.as_ref().clone()));
        for id_tco in accepted {
            if let Err(e) = context.task_status_tx.send(CopTaskStatus::from_id_tco(
                id_tco,
                CopTaskStatusPattern::Accepted,
                false,
            )) {
                error!("failed to send COP status: {}", e);
            }
        }
        self.update_status(context.queue_status_tx);
        self
    }

    fn reject(
        mut self: Box<Self>,
        context: FopQueueContext,
    ) -> Box<dyn FopQueueStateNode> {
        let stash = self.rejected.drain(..);
        let (ret, mut moved): (Vec<_>, Vec<_>) = self
            .executed
            .drain(..)
            .map(|(id, ctx, time)| ((id, ctx.tco.as_ref().clone()), (id, ctx, time)))
            .unzip();
        self.rejected = moved.drain(..).chain(stash).collect();
        for id_tco in ret.into_iter() {
            if let Err(e) = context.task_status_tx.send(CopTaskStatus::from_id_tco(
                id_tco,
                CopTaskStatusPattern::Rejected,
                false,
            )) {
                error!("failed to send COP status: {}", e);
            }
        }
        self.update_status(context.queue_status_tx);
        self
    }

    fn clear(
        &mut self, 
        context: FopQueueContext,
        status_pattern: CopTaskStatusPattern
    ) {
        let canceled = self
            .pending
            .drain(..)
            .map(|(id, task)| (id, task.context()))
            .chain(self.executed.drain(..).map(|(id, ctx, _)| (id, ctx)))
            .chain(self.rejected.drain(..).map(|(id, ctx, _)| (id, ctx)))
            .map(|(id, ctx)| (id, ctx.tco.as_ref().clone()));
        for id_tco in canceled {
            if let Err(e) = context.task_status_tx
                .send(CopTaskStatus::from_id_tco(id_tco, status_pattern, false))
            {
                error!("failed to send COP status: {}", e);
            }
        }
        self.update_status(context.queue_status_tx);
    }
}

struct ConfirmingQueue {
    pending: VecDeque<(CopTaskId, FopQueueTask)>,
    executed: VecDeque<(CopTaskId, CommandContext, DateTime<Utc>)>,
    oldest_arrival_time: Option<DateTime<Utc>>,
    next_id: CopTaskId,
    vs_at_id0: u32,
}

impl ConfirmingQueue {
    fn update_status(&mut self, queue_status_tx: broadcast::Sender<CopQueueStatusSet>) {
        let (pending_vs, pending) = {
            if let Some((head_id, qctx)) = self.pending.front() {
                let ctx = qctx.context();
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
        let oldest_arrival_time = self
            .executed
            .front()
            .map(|(_, _, time)| *time);
        self.oldest_arrival_time = oldest_arrival_time;
        let vs_list = vec![pending_vs, executed_vs];
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
            rejected: Some(CopQueueStatus::default()),
            head_vs,
            oldest_arrival_time,
            vs_at_id0: self.vs_at_id0,
            timestamp,
            status: CopQueueStatusPattern::Confirming.into(),
        };
        if let Err(e) = queue_status_tx.send(status) {
            error!("failed to send FOP queue status: {}", e);
        }
    }
}

impl FopQueueStateNode for ConfirmingQueue {
    fn get_oldest_arrival_time(&self) -> Option<DateTime<Utc>> {
        self.oldest_arrival_time
    }
    fn next_id(&self) -> CopTaskId {
        self.next_id
    }
    fn push(&mut self, context: FopQueueContext, cmd_ctx: CommandContext) -> CopTaskId {
        let id = self.next_id;
        self.next_id += 1;
        let id_tco = (id, cmd_ctx.tco.as_ref().clone());
        self.pending.push_back((id, FopQueueTask::Process(cmd_ctx.clone())));
        let status = CopTaskStatus::from_id_tco(id_tco, CopTaskStatusPattern::Pending, false);
        if let Err(e) = context.task_status_tx.send(status) {
            error!("failed to send COP status: {}", e);
        }
        self.update_status(context.queue_status_tx);
        id
    }
    fn confirm(&mut self, context: FopQueueContext, cmd_ctx: CommandContext) {
        self.pending.push_back((self.next_id, FopQueueTask::Confirm(cmd_ctx.clone())));
        let status = CopTaskStatus::from_id_tco(
            (self.next_id, cmd_ctx.tco.as_ref().clone()),
            CopTaskStatusPattern::Pending,
            true,
        );
        if let Err(e) = context.task_status_tx.send(status) {
            error!("failed to send COP status: {}", e);
        }
        self.next_id += 1;
        self.update_status(context.queue_status_tx);
    }
    fn execute(self: Box<Self>, context: FopQueueContext) -> (Box<dyn FopQueueStateNode>, Option<(u8, CommandContext)>) {
        let (id, ctx) = match self.pending.front() {
            Some((id, FopQueueTask::Confirm(ctx))) => {
                let status = CopTaskStatus::from_id_tco(
                    (*id, ctx.tco.as_ref().clone()),
                    CopTaskStatusPattern::Executed,
                    true,
                );
                if let Err(e) = context.task_status_tx.send(status) {
                    error!("failed to send COP status: {}", e);
                }
                ((*id + self.vs_at_id0) as u8, ctx.clone())
            },
            _ => unreachable!("No confirmation command in the queue"),
        };
        (self, Some((id, ctx)))
    }
    fn accept(mut self: Box<Self>, context: FopQueueContext, vr: u8) -> Box<dyn FopQueueStateNode> {
        if let Some((head_id, _, _)) = self.executed.front() {
            if vr.wrapping_sub((head_id + self.vs_at_id0) as u8) > (self.executed.len() + 1) as u8 {
                self
            } else if vr.wrapping_sub((head_id + self.vs_at_id0) as u8) > self.executed.len() as u8 {
                let accepted = self
                    .executed
                    .drain(..)
                    .map(|(id, ctx, _)| (id, ctx.tco.as_ref().clone()));
                for id_tco in accepted {
                    if let Err(e) = context.task_status_tx.send(CopTaskStatus::from_id_tco(
                        id_tco,
                        CopTaskStatusPattern::Accepted,
                        false
                    )) {
                        error!("failed to send COP status: {}", e);
                    }
                }
                let (id, ctx) = match self.pending.pop_front() {
                    Some((id, FopQueueTask::Confirm(ctx))) => (id, ctx),
                    _ => unreachable!("No confirmation command in the queue"),
                };
                let status = CopTaskStatus::from_id_tco(
                    (id, ctx.tco.as_ref().clone()),
                    CopTaskStatusPattern::Accepted,
                    true,
                );
                if let Err(e) = context.task_status_tx.send(status) {
                    error!("failed to send COP status: {}", e);
                }
                self.update_status(context.queue_status_tx);
                Box::new(ProcessingQueue {
                    pending: self.pending,
                    executed: self.executed,
                    rejected: VecDeque::new(),
                    oldest_arrival_time: self.oldest_arrival_time,
                    next_id: self.next_id,
                    vs_at_id0: self.vs_at_id0,
                })
            } else {
                let accepted_num = vr.wrapping_sub((head_id + self.vs_at_id0) as u8);
                let accepted = self
                    .executed
                    .drain(0..(accepted_num as usize))
                    .map(|(id, ctx, _)| (id, ctx.tco.as_ref().clone()));
                for id_tco in accepted {
                    if let Err(e) = context.task_status_tx.send(CopTaskStatus::from_id_tco(
                        id_tco,
                        CopTaskStatusPattern::Accepted,
                        false
                    )) {
                        error!("failed to send COP status: {}", e);
                    }
                }
                self.update_status(context.queue_status_tx);
                self
            }
        } else if let Some((head_id, _)) = self.pending.front() {
            if vr.wrapping_sub((head_id + self.vs_at_id0) as u8) > 1 
            || vr.wrapping_sub((head_id + self.vs_at_id0) as u8) == 0 { 
                self
            } else {
                let (id, ctx) = match self.pending.pop_front() {
                    Some((id, FopQueueTask::Confirm(ctx))) => (id, ctx),
                    _ => unreachable!("No confirmation command in the queue"),
                };
                let status = CopTaskStatus::from_id_tco(
                    (id, ctx.tco.as_ref().clone()),
                    CopTaskStatusPattern::Executed,
                    true,
                );
                if let Err(e) = context.task_status_tx.send(status) {
                    error!("failed to send COP status: {}", e);
                }
                self.update_status(context.queue_status_tx);
                Box::new(ProcessingQueue {
                    pending: self.pending,
                    executed: self.executed,
                    rejected: VecDeque::new(),
                    oldest_arrival_time: self.oldest_arrival_time,
                    next_id: self.next_id,
                    vs_at_id0: self.vs_at_id0,
                })
            }
        } else {
            unreachable!("No confirm command in the pending queue")
        }
    }

    fn reject(
        mut self: Box<Self>,
        context: FopQueueContext,
    ) -> Box<dyn FopQueueStateNode> {
        let (ret, mut moved): (Vec<_>, Vec<_>) = self
            .executed
            .drain(..)
            .map(|(id, ctx, time)| ((id, ctx.tco.as_ref().clone()), (id, ctx, time)))
            .unzip();
        let rejected = moved.drain(..).collect();
        for id_tco in ret.into_iter() {
            if let Err(e) = context.task_status_tx.send(CopTaskStatus::from_id_tco(
                id_tco,
                CopTaskStatusPattern::Rejected,
                false,
            )) {
                error!("failed to send COP status: {}", e);
            }
        }
        self.update_status(context.queue_status_tx);
        Box::new(ProcessingQueue {
            pending: self.pending,
            executed: VecDeque::new(),
            rejected,
            oldest_arrival_time: self.oldest_arrival_time,
            next_id: self.next_id,
            vs_at_id0: self.vs_at_id0,
        })
    }

    fn clear(
        &mut self, 
        context: FopQueueContext,
        status_pattern: CopTaskStatusPattern
    ) {
        let canceled = self.executed.drain(..).map(|(id, ctx, _)| (id, ctx))
            .map(|(id, ctx)| (id, ctx.tco.as_ref().clone()));
        for id_tco in canceled {
            if let Err(e) = context.task_status_tx
                .send(CopTaskStatus::from_id_tco(id_tco, status_pattern, false))
            {
                error!("failed to send COP status: {}", e);
            }
        }
        self.update_status(context.queue_status_tx);
    }
}

pub struct FopQueue {
    inner: Option<Box<dyn FopQueueStateNode>>,
}

impl FopQueue {
    pub fn new(
        vs: u8,
        next_id: CopTaskId,
    ) -> Self {
        Self {
            inner: Some(Box::new(ProcessingQueue::new(vs, next_id))),
        }
    }
    pub fn get_oldest_arrival_time(&self) -> Option<DateTime<Utc>> {
        match &self.inner {
            Some(inner) => inner.get_oldest_arrival_time(),
            None => unreachable!("FopQueue is empty"),
        }
    }
    pub fn next_id(&self) -> CopTaskId {
        match &self.inner {
            Some(inner) => inner.next_id(),
            None => unreachable!("FopQueue is empty"),
        }
    }

    pub fn push(
        &mut self, 
        queue_context: FopQueueContext,
        ctx: CommandContext,
    ) -> CopTaskId {
        match &mut self.inner {
            Some(inner) => inner.push(queue_context, ctx),
            None => unreachable!("FopQueue is empty"),
        }
    }

    pub fn confirm(
        &mut self, 
        queue_context: FopQueueContext,
        ctx: CommandContext,
    ) {
        match &mut self.inner {
            Some(inner) => inner.confirm(queue_context, ctx),
            None => unreachable!("FopQueue is empty"),
        }
    }

    pub fn execute(
        &mut self,
        queue_context: FopQueueContext,
    ) -> Option<(u8, CommandContext)> {
        let (new_state, ret) = match self.inner.take(){
            Some(inner) => inner.execute(queue_context),
            None => unreachable!("FopQueue is empty"),
        };
        self.inner = Some(new_state);
        ret
    }

    pub fn accept(
        &mut self, 
        queue_context: FopQueueContext,
        vr: u8,
    ) {
        let new_state = match self.inner.take() {
            Some(inner) => inner.accept(queue_context, vr),
            None => unreachable!("FopQueue is empty"),
        };
        self.inner = Some(new_state);
    }

    pub fn reject(
        &mut self,
        queue_context: FopQueueContext
    ) {
        let new_state = match self.inner.take() {
            Some(inner) => inner.reject(queue_context),
            None => unreachable!("FopQueue is empty"),
        };
        self.inner = Some(new_state);
    }

    pub fn clear(
        &mut self, 
        queue_context: FopQueueContext,
        status_pattern: CopTaskStatusPattern, 
    ) {
        match &mut self.inner {
            Some(inner) => inner.clear(queue_context, status_pattern),
            None => unreachable!("FopQueue is empty"),
        }
    }
}