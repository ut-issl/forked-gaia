use std::{collections::VecDeque, future::Future, pin::Pin};

use chrono::{DateTime, Utc};
use gaia_tmtc::{cop::{CopQueueStatus, CopQueueStatusSet, CopTaskStatus, CopTaskStatusPattern, CopQueueStatusPattern}, tco_tmiv::Tco};
use prost_types::Timestamp;
use tokio::sync::mpsc;
use tracing::{error, info};

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

type FopQueuePushOutput = (Box<dyn FopQueueStateNode>, CopTaskId);
type FopQueueExecuteOutput = (Box<dyn FopQueueStateNode>, Option<(u8, CommandContext)>);

trait FopQueueStateNode {
    fn get_last_time(&self) -> Option<DateTime<Utc>>;
    fn next_id(&self) -> CopTaskId;
    fn push(self: Box<Self>, context: FopQueueContext, cmd_ctx: CommandContext) -> Pin<Box<dyn Future<Output = FopQueuePushOutput>>>;
    fn confirm(self: Box<Self>, context: FopQueueContext, cmd_ctx: CommandContext) -> Pin<Box<dyn Future<Output = Box<dyn FopQueueStateNode>>>>;
    fn execute(self: Box<Self>, context: FopQueueContext) -> Pin<Box<dyn Future<Output = FopQueueExecuteOutput>>>;
    fn accept(self: Box<Self>, context: FopQueueContext, vr: u8) -> Pin<Box<dyn Future<Output = Box<dyn FopQueueStateNode>>>>;
    fn reject(self: Box<Self>, context: FopQueueContext) -> Pin<Box<dyn Future<Output = Box<dyn FopQueueStateNode>>>>;
    fn clear(self: Box<Self>, context: FopQueueContext, status_pattern: CopTaskStatusPattern) -> Pin<Box<dyn Future<Output = Box<dyn FopQueueStateNode>>>>;
    fn send_status(&self, queue_status_tx: mpsc::Sender<CopQueueStatusSet>) -> Pin<Box<dyn Future<Output = ()>>>;
}

#[derive(Clone)]
pub struct FopQueueContext {
    pub task_status_tx: mpsc::Sender<CopTaskStatus>,
    pub queue_status_tx: mpsc::Sender<CopQueueStatusSet>,
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
    executed: VecDeque<(CopTaskId, CommandContext)>,
    rejected: VecDeque<(CopTaskId, CommandContext)>,
    last_time: Option<DateTime<Utc>>,
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
            last_time: None,
            next_id,
            vs_at_id0: vs.wrapping_sub(next_id as u8) as u32,
        }
    }

    fn create_status(&self) -> CopQueueStatusSet {
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
            if let Some((head_id, ctx)) = self.executed.front() {
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
            if let Some((head_id, ctx)) = self.rejected.front() {
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
        let vs_list = vec![pending_vs, executed_vs, rejected_vs];
        let head_vs = vs_list.into_iter().flatten().min().map(|vs| vs as u32).unwrap_or(((self.next_id + self.vs_at_id0) as u8) as u32);
        let oldest_arrival_time = self.last_time.map(|time| Timestamp {
            seconds: time.timestamp(),
            nanos: time.timestamp_subsec_nanos() as i32,
        });
        let now = chrono::Utc::now().naive_utc();
        let timestamp = Some(Timestamp {
            seconds: now.and_utc().timestamp(),
            nanos: now.and_utc().timestamp_subsec_nanos() as i32,
        });
        CopQueueStatusSet {
            pending,
            executed,
            rejected,
            head_vs,
            oldest_arrival_time,
            vs_at_id0: self.vs_at_id0,
            timestamp,
            status: CopQueueStatusPattern::Processing.into(),
        }
    }

    async fn update_status(&mut self, queue_status_tx: mpsc::Sender<CopQueueStatusSet>) {
        if self.pending.is_empty() && self.executed.is_empty() && self.rejected.is_empty() {
            self.last_time = None;
        }
        let status = self.create_status();
        if let Err(e) = queue_status_tx.send(status).await {
            error!("failed to send FOP queue status: {}", e);
        }
    }
}

impl FopQueueStateNode for ProcessingQueue {
    fn get_last_time(&self) -> Option<DateTime<Utc>> {
        self.last_time
    }
    fn next_id(&self) -> CopTaskId {
        self.next_id
    }
    fn push(mut self: Box<Self>, context: FopQueueContext, cmd_ctx: CommandContext) -> Pin<Box<dyn Future<Output = (Box<dyn FopQueueStateNode>, CopTaskId)>>> {
        if self.last_time.is_none() {
            self.last_time = Some(chrono::Utc::now());
        }
        let id = self.next_id;
        self.next_id += 1;
        let id_tco = (id, cmd_ctx.tco.as_ref().clone());
        self.pending.push_back((id, FopQueueTask::Process(cmd_ctx.clone())));
        let status = CopTaskStatus::from_id_tco(id_tco, CopTaskStatusPattern::Pending, false);
        Box::pin(async move {
            self.update_status(context.queue_status_tx).await;
            if let Err(e) = context.task_status_tx.send(status).await {
                error!("failed to send COP status: {}", e);
            }
            (self as Box<dyn FopQueueStateNode>, id)
        })
    }
    fn confirm(mut self: Box<Self>, context: FopQueueContext, cmd_ctx: CommandContext) -> Pin<Box<dyn Future<Output = Box<dyn FopQueueStateNode>>>> {
        if let Some((_, FopQueueTask::Confirm(_))) = self.pending.back() {
            return Box::pin(async move { self as Box<dyn FopQueueStateNode> });
        } else if self.pending.is_empty() && self.executed.is_empty() && self.rejected.is_empty() {
            return Box::pin(async move { self as Box<dyn FopQueueStateNode> });
        }
        self.pending.push_back((self.next_id, FopQueueTask::Confirm(cmd_ctx.clone())));
        let status = CopTaskStatus::from_id_tco(
            (self.next_id, cmd_ctx.tco.as_ref().clone()),
            CopTaskStatusPattern::Pending,
            true,
        );
        self.next_id += 1;
        Box::pin(async move {
            self.update_status(context.queue_status_tx).await;
            if let Err(e) = context.task_status_tx.send(status).await {
                error!("failed to send COP status: {}", e);
            }
            self as Box<dyn FopQueueStateNode>
        })
    }
    fn execute(
        mut self: Box<Self>,
        context: FopQueueContext,
    ) -> Pin<Box<dyn Future<Output = (Box<dyn FopQueueStateNode>, Option<(u8, CommandContext)>)>>> {
        Box::pin(async move {
            let (id, ctx) = match self.rejected.pop_front() {
                Some((id,ctx)) => (id,ctx),
                None => match self.pending.pop_front() {
                    Some((id, FopQueueTask::Process(ctx))) => (id, ctx),
                    Some((id, FopQueueTask::Confirm(ctx))) => {
                        let pending = self.pending;
                        let confirm = (id, ctx.clone());
                        let status = CopTaskStatus::from_id_tco(
                            (id, ctx.tco.as_ref().clone()),
                            CopTaskStatusPattern::Executed,
                            true,
                        );
                        if let Err(e) = context.task_status_tx.send(status).await {
                            error!("failed to send COP status: {}", e);
                        }
                        let ret = Box::new(ConfirmingQueue{
                            pending,
                            confirm,
                            executed: self.executed,
                            last_time: self.last_time,
                            next_id: self.next_id,
                            vs_at_id0: self.vs_at_id0,
                        });
                        ret.update_status(context.queue_status_tx).await;
                        return (
                            ret as Box<dyn FopQueueStateNode>,
                            Some(((id + self.vs_at_id0) as u8, ctx))
                        )
                    },
                    None =>  return (self as Box<dyn FopQueueStateNode> ,None),
                },
            };
            let ret = ((id + self.vs_at_id0) as u8, ctx.clone());
            self.executed.push_back((id, ctx.clone()));
            let status = CopTaskStatus::from_id_tco(
                (id, ctx.tco.as_ref().clone()),
                CopTaskStatusPattern::Executed,
                false,
            );
            if let Err(e) = context.task_status_tx.send(status).await {
                error!("failed to send COP status: {}", e);
            }
            self.update_status(context.queue_status_tx).await;
            (self as Box<dyn FopQueueStateNode>, Some(ret))
        })
    }

    fn accept(
        mut self: Box<Self>, 
        context: FopQueueContext,
        vr: u8,
    ) -> Pin<Box<dyn Future<Output = Box<dyn FopQueueStateNode>>>> {
        Box::pin(async move {
            let accepted_num = if let Some((head_id, _)) = self.executed.front() {
                if vr.wrapping_sub((head_id + self.vs_at_id0) as u8) > self.executed.len() as u8 {
                    info!("length of executed: {} vs {}", self.executed.len(), vr.wrapping_sub((head_id + self.vs_at_id0) as u8));
                    0
                } else {
                    vr.wrapping_sub((head_id + self.vs_at_id0) as u8)
                }
            } else {
                return self as Box<dyn FopQueueStateNode>;
            }; 
            let accepted = self
                .executed
                .drain(0..(accepted_num as usize))
                .map(|(id, ctx)| (id, ctx.tco.as_ref().clone()));
            if accepted.len() != 0 {
                self.last_time = Some(chrono::Utc::now());
            }
            for id_tco in accepted {
                if let Err(e) = context.task_status_tx.send(CopTaskStatus::from_id_tco(
                    id_tco,
                    CopTaskStatusPattern::Accepted,
                    false,
                )).await {
                    error!("failed to send COP status: {}", e);
                }
            }
            self.update_status(context.queue_status_tx).await;
            self as Box<dyn FopQueueStateNode>
        })
    }

    fn reject(
        mut self: Box<Self>,
        context: FopQueueContext,
    ) -> Pin<Box<dyn Future<Output = Box<dyn FopQueueStateNode>>>> {
        Box::pin(async move {
            let stash = self.rejected.drain(..);
            let (ret, mut moved): (Vec<_>, Vec<_>) = self
                .executed
                .drain(..)
                .map(|(id, ctx)| ((id, ctx.tco.as_ref().clone()), (id, ctx)))
                .unzip();
            self.rejected = moved.drain(..).chain(stash).collect();
            for id_tco in ret.into_iter() {
                if let Err(e) = context.task_status_tx.send(CopTaskStatus::from_id_tco(
                    id_tco,
                    CopTaskStatusPattern::Rejected,
                    false,
                )).await {
                    error!("failed to send COP status: {}", e);
                }
            }
            self.update_status(context.queue_status_tx).await;
            self as Box<dyn FopQueueStateNode>
        })
    }

    fn clear(
        mut self: Box<Self>, 
        context: FopQueueContext,
        status_pattern: CopTaskStatusPattern
    ) -> Pin<Box<dyn Future<Output = Box<dyn FopQueueStateNode>>>> {
        Box::pin(async move {
            let canceled = self
                .pending
                .drain(..)
                .map(|(id, task)| (id, task.context()))
                .chain(self.executed.drain(..))
                .chain(self.rejected.drain(..))
                .map(|(id, ctx)| (id, ctx.tco.as_ref().clone()));
            for id_tco in canceled {
                if let Err(e) = context.task_status_tx
                    .send(CopTaskStatus::from_id_tco(id_tco, status_pattern, false)).await
                {
                    error!("failed to send COP status: {}", e);
                }
            }
            self.update_status(context.queue_status_tx).await;
            self as Box<dyn FopQueueStateNode>
        })
    }

    fn send_status(&self, queue_status_tx: mpsc::Sender<CopQueueStatusSet>) -> Pin<Box<dyn Future<Output = ()>>> {
        let status = self.create_status();
        Box::pin(async move {
            if let Err(e) = queue_status_tx.send(status).await {
                error!("failed to send FOP queue status: {}", e);
            }
        })
    }
}

struct ConfirmingQueue {
    pending: VecDeque<(CopTaskId, FopQueueTask)>,
    confirm: (CopTaskId, CommandContext),
    executed: VecDeque<(CopTaskId, CommandContext)>,
    last_time: Option<DateTime<Utc>>,
    next_id: CopTaskId,
    vs_at_id0: u32,
}

impl ConfirmingQueue {
    fn create_status(&self) -> CopQueueStatusSet {
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
            if let Some((head_id, ctx)) = self.executed.front() {
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
        let vs_list = vec![pending_vs, executed_vs];
        let head_vs = vs_list.into_iter().flatten().min().map(|vs| vs as u32).unwrap_or(((self.next_id + self.vs_at_id0) as u8) as u32);
        let oldest_arrival_time = self.last_time.map(|time| Timestamp {
            seconds: time.timestamp(),
            nanos: time.timestamp_subsec_nanos() as i32,
        });
        let now = chrono::Utc::now().naive_utc();
        let timestamp = Some(Timestamp {
            seconds: now.and_utc().timestamp(),
            nanos: now.and_utc().timestamp_subsec_nanos() as i32,
        });
        CopQueueStatusSet {
            pending,
            executed,
            rejected: Some(CopQueueStatus::default()),
            head_vs,
            oldest_arrival_time,
            vs_at_id0: self.vs_at_id0,
            timestamp,
            status: CopQueueStatusPattern::Confirming.into(),
        }
    }
    async fn update_status(&self, queue_status_tx: mpsc::Sender<CopQueueStatusSet>) {
        let status = self.create_status();
        if let Err(e) = queue_status_tx.send(status).await {
            error!("failed to send FOP queue status: {}", e);
        }
    }
}

impl FopQueueStateNode for ConfirmingQueue {
    fn get_last_time(&self) -> Option<DateTime<Utc>> {
        self.last_time
    }
    fn next_id(&self) -> CopTaskId {
        self.next_id
    }
    fn push(mut self: Box<Self>, context: FopQueueContext, cmd_ctx: CommandContext) -> Pin<Box<dyn Future<Output = (Box<dyn FopQueueStateNode>, CopTaskId)>>> {
        Box::pin(async move {
            let id = self.next_id;
            self.next_id += 1;
            let id_tco = (id, cmd_ctx.tco.as_ref().clone());
            self.pending.push_back((id, FopQueueTask::Process(cmd_ctx.clone())));
            let status = CopTaskStatus::from_id_tco(id_tco, CopTaskStatusPattern::Pending, false);
            if let Err(e) = context.task_status_tx.send(status).await {
                error!("failed to send COP status: {}", e);
            }
            self.update_status(context.queue_status_tx).await;
            (self as Box<dyn FopQueueStateNode>, id)
        })
    }
    fn confirm(mut self: Box<Self>, context: FopQueueContext, cmd_ctx: CommandContext) -> Pin<Box<dyn Future<Output = Box<dyn FopQueueStateNode>>>> {
        Box::pin(async move {
            if let Some((_, FopQueueTask::Confirm(_))) = self.pending.back() {
                return self as Box<dyn FopQueueStateNode>;
            } else if self.pending.is_empty() {
                return self as Box<dyn FopQueueStateNode>;
            }
            self.pending.push_back((self.next_id, FopQueueTask::Confirm(cmd_ctx.clone())));
            let status = CopTaskStatus::from_id_tco(
                (self.next_id, cmd_ctx.tco.as_ref().clone()),
                CopTaskStatusPattern::Pending,
                true,
            );
            if let Err(e) = context.task_status_tx.send(status).await {
                error!("failed to send COP status: {}", e);
            }
            self.next_id += 1;
            self.update_status(context.queue_status_tx).await;
            self as Box<dyn FopQueueStateNode>
        })
    }
    fn execute(self: Box<Self>, context: FopQueueContext) -> Pin<Box<dyn Future<Output = (Box<dyn FopQueueStateNode>, Option<(u8, CommandContext)>)>>> {
        Box::pin(async move {
            let (id, ctx) = {
                let (id, ctx) = self.confirm.clone();
                let status = CopTaskStatus::from_id_tco(
                    (id, ctx.tco.as_ref().clone()),
                    CopTaskStatusPattern::Executed,
                    true,
                );
                if let Err(e) = context.task_status_tx.send(status).await {
                    error!("failed to send COP status: {}", e);
                }
                ((id + self.vs_at_id0) as u8, ctx)
            };
            self.update_status(context.queue_status_tx).await;
            (self as Box<dyn FopQueueStateNode>, Some((id, ctx)))
        })
    }
    fn accept(mut self: Box<Self>, context: FopQueueContext, vr: u8) -> Pin<Box<dyn Future<Output = Box<dyn FopQueueStateNode>>>> {
        Box::pin(async move{
            if let Some((head_id, _)) = self.executed.front() {
                if vr.wrapping_sub((head_id + self.vs_at_id0) as u8) > (self.executed.len() + 1) as u8 {
                    info!("length of executed: {} vs {}", self.executed.len(), vr.wrapping_sub((head_id + self.vs_at_id0) as u8));
                    self
                } else if vr.wrapping_sub((head_id + self.vs_at_id0) as u8) > self.executed.len() as u8 {
                    let accepted = self
                        .executed
                        .drain(..)
                        .map(|(id, ctx)| (id, ctx.tco.as_ref().clone()));
                    if accepted.len() != 0 {
                        self.last_time = Some(chrono::Utc::now());
                    }
                    self.last_time = Some(chrono::Utc::now());
                    for id_tco in accepted {
                        if let Err(e) = context.task_status_tx.send(CopTaskStatus::from_id_tco(
                            id_tco,
                            CopTaskStatusPattern::Accepted,
                            false
                        )).await {
                            error!("failed to send COP status: {}", e);
                        }
                    }
                    let (id, ctx) = self.confirm.clone();
                    let status = CopTaskStatus::from_id_tco(
                        (id, ctx.tco.as_ref().clone()),
                        CopTaskStatusPattern::Accepted,
                        true,
                    );
                    if let Err(e) = context.task_status_tx.send(status).await {
                        error!("failed to send COP status: {}", e);
                    }
                    let mut ret = Box::new(ProcessingQueue {
                        pending: self.pending,
                        executed: self.executed,
                        rejected: VecDeque::new(),
                        last_time: self.last_time,
                        next_id: self.next_id,
                        vs_at_id0: self.vs_at_id0,
                    });
                    ret.update_status(context.queue_status_tx).await;
                    return ret as Box<dyn FopQueueStateNode>
                } else {
                    let accepted_num = vr.wrapping_sub((head_id + self.vs_at_id0) as u8);
                    let accepted = self
                        .executed
                        .drain(0..(accepted_num as usize))
                        .map(|(id, ctx)| (id, ctx.tco.as_ref().clone()));
                    if accepted.len() != 0 {
                        self.last_time = Some(chrono::Utc::now());
                    }
                    for id_tco in accepted {
                        if let Err(e) = context.task_status_tx.send(CopTaskStatus::from_id_tco(
                            id_tco,
                            CopTaskStatusPattern::Accepted,
                            false
                        )).await {
                            error!("failed to send COP status: {}", e);
                        }
                    }
                    self.update_status(context.queue_status_tx).await;
                    self as Box<dyn FopQueueStateNode>
                }
            } else {
                let (head_id, ctx) = self.confirm.clone();
                if vr.wrapping_sub((head_id + self.vs_at_id0) as u8) > 1 
                || vr.wrapping_sub((head_id + self.vs_at_id0) as u8) == 0 { 
                    self
                } else {
                    let status = CopTaskStatus::from_id_tco(
                        (head_id, ctx.tco.as_ref().clone()),
                        CopTaskStatusPattern::Executed,
                        true,
                    );
                    self.last_time = Some(chrono::Utc::now());
                    if let Err(e) = context.task_status_tx.send(status).await {
                        error!("failed to send COP status: {}", e);
                    }
                    let mut ret = Box::new(ProcessingQueue {
                        pending: self.pending,
                        executed: self.executed,
                        rejected: VecDeque::new(),
                        last_time: self.last_time,
                        next_id: self.next_id,
                        vs_at_id0: self.vs_at_id0,
                    });
                    ret.update_status(context.queue_status_tx).await;
                    ret as Box<dyn FopQueueStateNode>
                }
            } 
        })
    }

    fn reject(
        mut self: Box<Self>,
        context: FopQueueContext,
    ) -> Pin<Box<dyn Future<Output = Box<dyn FopQueueStateNode>>>> {
        Box::pin(async move {
            let (ret, mut moved): (Vec<_>, Vec<_>) = self
                .executed
                .drain(..)
                .map(|(id, ctx)| ((id, ctx.tco.as_ref().clone()), (id, ctx)))
                .unzip();
            let rejected = moved.drain(..).collect();
            for id_tco in ret.into_iter() {
                if let Err(e) = context.task_status_tx.send(CopTaskStatus::from_id_tco(
                    id_tco,
                    CopTaskStatusPattern::Rejected,
                    false,
                )).await {
                    error!("failed to send COP status: {}", e);
                }
            }
            let (confirm_id, confirm_ctx) = self.confirm.clone();
            self.pending.push_front((confirm_id, FopQueueTask::Confirm(confirm_ctx)));
            let mut ret = Box::new(ProcessingQueue {
                pending: self.pending,
                executed: VecDeque::new(),
                rejected,
                last_time: self.last_time,
                next_id: self.next_id,
                vs_at_id0: self.vs_at_id0,
            });
            ret.update_status(context.queue_status_tx).await;
            ret as Box<dyn FopQueueStateNode>
        })
    }

    fn clear(
        mut self: Box<Self>, 
        context: FopQueueContext,
        status_pattern: CopTaskStatusPattern
    ) -> Pin<Box<dyn Future<Output = Box<dyn FopQueueStateNode>>>> {
        Box::pin(async move {
            let canceled = self.executed.drain(..)
                .map(|(id, ctx)| (id, ctx.tco.as_ref().clone()));
            for id_tco in canceled {
                if let Err(e) = context.task_status_tx
                    .send(CopTaskStatus::from_id_tco(id_tco, status_pattern, false)).await
                {
                    error!("failed to send COP status: {}", e);
                }
            }
            self.update_status(context.queue_status_tx).await;
            self as Box<dyn FopQueueStateNode>
        })
    }

    fn send_status(&self, queue_status_tx: mpsc::Sender<CopQueueStatusSet>) -> Pin<Box<dyn Future<Output = ()>>> {
        let status = self.create_status();
        Box::pin(async move {
            if let Err(e) = queue_status_tx.send(status).await {
                error!("failed to send FOP queue status: {}", e);
            }
        })
    }
}

pub struct FopQueue {
    inner: Option<Box<dyn FopQueueStateNode>>,
}

impl FopQueue {
    pub async fn new(
        vs: u8,
        next_id: CopTaskId,
        context: FopQueueContext,
    ) -> Self {
        let mut inner = ProcessingQueue::new(vs, next_id);
        inner.update_status(context.queue_status_tx).await;
        Self {
            inner: Some(Box::new(ProcessingQueue::new(vs, next_id))),
        }
    }
    pub fn get_last_time(&self) -> Option<DateTime<Utc>> {
        match &self.inner {
            Some(inner) => inner.get_last_time(),
            None => unreachable!("FopQueue is empty"),
        }
    }
    pub fn next_id(&self) -> CopTaskId {
        match &self.inner {
            Some(inner) => inner.next_id(),
            None => unreachable!("FopQueue is empty"),
        }
    }

    pub async fn push(
        &mut self, 
        queue_context: FopQueueContext,
        ctx: CommandContext,
    ) -> CopTaskId {
        let (queue, id) = match self.inner.take() {
            Some(inner) => inner.push(queue_context, ctx).await,
            None => unreachable!("FopQueue is empty"),
        };
        self.inner = Some(queue);
        id
    }

    pub async fn confirm(
        &mut self, 
        queue_context: FopQueueContext,
        ctx: CommandContext,
    ) {
        let queue = match self.inner.take() {
            Some(inner) => inner.confirm(queue_context, ctx).await,
            None => unreachable!("FopQueue is empty"),
        };
        self.inner = Some(queue);
    }

    pub async fn execute(
        &mut self,
        queue_context: FopQueueContext,
    ) -> Option<(u8, CommandContext)> {
        let (new_state, ret) = match self.inner.take(){
            Some(inner) => inner.execute(queue_context).await,
            None => unreachable!("FopQueue is empty"),
        };
        self.inner = Some(new_state);
        ret
    }

    pub async fn accept(
        &mut self, 
        queue_context: FopQueueContext,
        vr: u8,
    ) {
        let new_state = match self.inner.take() {
            Some(inner) => inner.accept(queue_context, vr).await,
            None => unreachable!("FopQueue is empty"),
        };
        self.inner = Some(new_state);
    }

    pub async fn reject(
        &mut self,
        queue_context: FopQueueContext
    ) {
        let new_state = match self.inner.take() {
            Some(inner) => inner.reject(queue_context).await,
            None => unreachable!("FopQueue is empty"),
        };
        self.inner = Some(new_state);
    }

    pub async fn clear(
        &mut self, 
        queue_context: FopQueueContext,
        status_pattern: CopTaskStatusPattern, 
    ) {
        let queue = match self.inner.take() {
            Some(inner) => inner.clear(queue_context, status_pattern).await,
            None => unreachable!("FopQueue is empty"),
        };
        self.inner = Some(queue);
    }

    pub async fn send_status(&self, queue_status_tx: mpsc::Sender<CopQueueStatusSet>) {
        if let Some(inner) = &self.inner {
            inner.send_status(queue_status_tx).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use gaia_ccsds_c2a::access::cmd::schema::CommandSchema;
    use gaia_tmtc::{cop::{CopQueueStatusPattern, CopQueueStatusSet, CopTaskStatus, CopTaskStatusPattern}, tco_tmiv::Tco};
    use tokio::sync::mpsc;

    use crate::{fop::{queue::{FopQueue, FopQueueContext}, worker::CommandContext}, registry::FatCommandSchema};

    fn create_cmd_ctx() -> CommandContext {
        CommandContext {
            tco: Arc::new(Tco {
                name: "test".to_string(),
                params: Vec::new(),
                is_type_ad: true,
            }),
            tc_scid: 100,
            fat_schema: FatCommandSchema {
                apid: 0,
                command_id: 0,
                destination_type: 0,
                execution_type: 0,
                has_time_indicator: false,
                schema: CommandSchema {
                    sized_parameters: Vec::new(),
                    static_size: 0,
                    has_trailer_parameter: false,
                },
            }
        }
    }

    fn create_test_context() -> (FopQueueContext, mpsc::Receiver<CopTaskStatus>, mpsc::Receiver<CopQueueStatusSet>) {
        let (task_status_tx, task_status_rx) = mpsc::channel(100);
        let (queue_status_tx, queue_status_rx) = mpsc::channel(100);
        let context = FopQueueContext {
            task_status_tx,
            queue_status_tx,
        };
        (context, task_status_rx, queue_status_rx)
    }

    #[tokio::test]
    async fn test_fop_queue_initialization() {
        let initial_id = 1;
        let vs = 10;
        let (ctx, _, mut queue_rx) = create_test_context();
        let queue = FopQueue::new(vs, initial_id, ctx.clone()).await;
        
        assert_eq!(queue.next_id(), initial_id);
        assert_eq!(queue.get_last_time(), None);
        let status = match queue_rx.try_recv() {
            Ok(status) => status,
            Err(e) => panic!("failed to receive COP queue status: {}", e),
        };
        assert_eq!(status.head_vs, vs as u32);
    }

    #[tokio::test]
    async fn queue_test() {
        let (context, mut task_rx, mut queue_rx) = create_test_context();
        let initial_id = 1;
        let initial_vs = 10;
        let mut queue = FopQueue::new(initial_vs, initial_id, context.clone()).await;
        assert_eq!(queue.next_id(), initial_id);
        assert_eq!(queue.get_last_time(), None);
        let status = match queue_rx.try_recv() {
            Ok(status) => status,
            Err(e) => panic!("failed to receive COP queue status: {}", e),
        };
        assert_eq!(status.head_vs, initial_vs as u32);
        assert_eq!(status.status, CopQueueStatusPattern::Processing as i32);

        let cmd_ctx = create_cmd_ctx();

        queue.confirm(context.clone(), cmd_ctx.clone()).await;

        let task_num = 10;

        for i in 0..task_num {
            let task_id = queue.push(context.clone(), cmd_ctx.clone()).await;

            assert_eq!(task_id, initial_id + i);
            assert_eq!(queue.next_id(), initial_id + i + 1);

            // ステータスが送信されているか確認
            let status = match task_rx.try_recv() {
                Ok(status) => status,
                Err(e) => panic!("failed to receive COP status: {}", e),
            };
            assert_eq!(status.task_id, task_id);
            assert_eq!(status.status, CopTaskStatusPattern::Pending as i32);
            assert!(!status.is_confirm_command);

            // キューのステータスが更新されているか確認
            let queue_status = match queue_rx.try_recv() {
                Ok(status) => status,
                Err(e) => panic!("failed to receive COP queue status: {}", e),
            };
            assert_eq!(queue_status.status, CopQueueStatusPattern::Processing as i32);
            let pending = queue_status.pending.unwrap();
            assert_eq!(pending.head_id, Some(initial_id));
            assert_eq!(pending.task_count, 1 + i);
        }

        queue.confirm(context.clone(), cmd_ctx.clone()).await;

        // ステータスが送信されているか確認
        let status = match task_rx.try_recv() {
            Ok(status) => status,
            Err(e) => panic!("failed to receive COP status: {}", e),
        };
        assert_eq!(status.task_id, initial_id + task_num);
        assert_eq!(status.status, CopTaskStatusPattern::Pending as i32);
        assert!(status.is_confirm_command);

        // キューのステータスが更新されているか確認
        let queue_status = match queue_rx.try_recv() {
            Ok(status) => status,
            Err(e) => panic!("failed to receive COP queue status: {}", e),
        };
        let pending = queue_status.pending.unwrap();
        assert_eq!(pending.task_count, task_num + 1);

        let task_num = 10;

        for i in 0..task_num {
            let (vs, _) = queue.execute(context.clone()).await.unwrap();
            assert_eq!(vs, initial_vs + i as u8);
            // ステータスが送信されているか確認
            let status = match task_rx.try_recv() {
                Ok(status) => status,
                Err(e) => panic!("failed to receive COP status: {}", e),
            };

            assert_eq!(status.task_id, initial_id + i);
            assert_eq!(status.status, CopTaskStatusPattern::Executed as i32);
            assert!(!status.is_confirm_command);

            // キューのステータスが更新されているか確認
            let queue_status = match queue_rx.try_recv() {
                Ok(status) => status,
                Err(e) => panic!("failed to receive COP queue status: {}", e),
            };
            assert_eq!(queue_status.status, CopQueueStatusPattern::Processing as i32);
            let pending = queue_status.pending.unwrap();
            assert_eq!(pending.head_id, Some(initial_id + 1 + i));
            assert_eq!(pending.task_count, task_num - i);
            let executed = queue_status.executed.unwrap();
            assert_eq!(executed.head_id, Some(initial_id));
            assert_eq!(executed.task_count, i + 1);
        }

        let accept_num = 3;
        queue.accept(context.clone(), initial_vs + accept_num).await;
        // キューのステータスが更新されているか確認
        let queue_status = match queue_rx.try_recv() {
            Ok(status) => status,
            Err(e) => panic!("failed to receive COP queue status: {}", e),
        };
        assert_eq!(queue_status.status, CopQueueStatusPattern::Processing as i32);
        let pending = queue_status.pending.unwrap();
        assert_eq!(pending.task_count, task_num - task_num + 1);
        let executed = queue_status.executed.unwrap();
        assert_eq!(executed.task_count, task_num - accept_num as u32);
        let rejected = queue_status.rejected.unwrap();
        assert_eq!(rejected.task_count, 0);

        for i in 0..accept_num {
            // ステータスが送信されているか確認
            let status = match task_rx.try_recv() {
                Ok(status) => status,
                Err(e) => panic!("failed to receive COP status: {}", e),
            };
            assert_eq!(status.task_id, initial_id + i as u32);
            assert_eq!(status.status, CopTaskStatusPattern::Accepted as i32);
            assert!(!status.is_confirm_command);
        }

        queue.reject(context.clone()).await;
        // キューのステータスが更新されているか確認
        let queue_status = match queue_rx.try_recv() {
            Ok(status) => status,
            Err(e) => panic!("failed to receive COP queue status: {}", e),
        };
        assert_eq!(queue_status.status, CopQueueStatusPattern::Processing as i32);
        assert_eq!(queue_status.pending.unwrap().task_count, task_num - task_num + 1);
        assert_eq!(queue_status.executed.unwrap().task_count, 0);
        assert_eq!(queue_status.rejected.unwrap().task_count, task_num - accept_num as u32);

        for i in 0..(task_num - accept_num as u32) {
            // ステータスが送信されているか確認
            let status = match task_rx.try_recv() {
                Ok(status) => status,
                Err(e) => panic!("failed to receive COP status: {}", e),
            };
            assert_eq!(status.task_id, initial_id + accept_num as u32 + i);
            assert_eq!(status.status, CopTaskStatusPattern::Rejected as i32);
            assert!(!status.is_confirm_command);
        }

        for i in 0..(task_num - accept_num as u32) {
            let (vs, _) = queue.execute(context.clone()).await.unwrap();
            assert_eq!(vs, initial_vs + accept_num + i as u8);
            // ステータスが送信されているか確認
            let status = match task_rx.try_recv() {
                Ok(status) => status,
                Err(e) => panic!("failed to receive COP status: {}", e),
            };
            assert_eq!(status.task_id, initial_id + accept_num as u32 + i);
            assert_eq!(status.status, CopTaskStatusPattern::Executed as i32);
            assert!(!status.is_confirm_command);

            // キューのステータスが更新されているか確認
            let queue_status = match queue_rx.try_recv() {
                Ok(status) => status,
                Err(e) => panic!("failed to receive COP queue status: {}", e),
            };
            assert_eq!(queue_status.status, CopQueueStatusPattern::Processing as i32);
            assert_eq!(queue_status.pending.unwrap().task_count, task_num - task_num + 1);
            assert_eq!(queue_status.executed.unwrap().task_count, i + 1);
            assert_eq!(queue_status.rejected.unwrap().task_count, task_num - accept_num as u32 - i - 1);
        }

        for _  in 0..3 {
            let (vs, _) = queue.execute(context.clone()).await.unwrap();
            assert_eq!(vs, initial_vs + task_num as u8);
            // ステータスが送信されているか確認
            let status = match task_rx.try_recv() {
                Ok(status) => status,
                Err(e) => panic!("failed to receive COP status: {}", e),
            };
            assert_eq!(status.task_id, initial_id + task_num);
            assert_eq!(status.status, CopTaskStatusPattern::Executed as i32);
            assert!(status.is_confirm_command);

            // キューのステータスが更新されているか確認
            let queue_status = match queue_rx.try_recv() {
                Ok(status) => status,
                Err(e) => panic!("failed to receive COP queue status: {}", e),
            };
            assert_eq!(queue_status.status, CopQueueStatusPattern::Confirming as i32);
            assert_eq!(queue_status.pending.unwrap().task_count, 0);
            assert_eq!(queue_status.executed.unwrap().task_count, task_num - accept_num as u32);
            assert_eq!(queue_status.rejected.unwrap().task_count, 0);
        }

        queue.reject(context.clone()).await;
        // キューのステータスが更新されているか確認
        let queue_status = match queue_rx.try_recv() {
            Ok(status) => status,
            Err(e) => panic!("failed to receive COP queue status: {}", e),
        };
        assert_eq!(queue_status.status, CopQueueStatusPattern::Processing as i32);
        assert_eq!(queue_status.pending.unwrap().task_count, task_num - task_num + 1);
        assert_eq!(queue_status.executed.unwrap().task_count, 0);
        assert_eq!(queue_status.rejected.unwrap().task_count, task_num - accept_num as u32);

        for i in 0..(task_num - accept_num as u32) {
            // ステータスが送信されているか確認
            let status = match task_rx.try_recv() {
                Ok(status) => status,
                Err(e) => panic!("failed to receive COP status: {}", e),
            };
            assert_eq!(status.task_id, initial_id + accept_num as u32 + i);
            assert_eq!(status.status, CopTaskStatusPattern::Rejected as i32);
            assert!(!status.is_confirm_command);
        }

    }
}