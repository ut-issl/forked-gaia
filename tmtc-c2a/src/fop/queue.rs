use std::collections::VecDeque;

use chrono::{DateTime, Utc};
use gaia_tmtc::{cop::{CopQueueStatus, CopQueueStatusSet, CopTaskStatus, CopTaskStatusPattern}, tco_tmiv::Tco};
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

#[derive(Clone)]
pub struct FopQueueContext {
    pub task_status_tx: mpsc::Sender<CopTaskStatus>,
    pub queue_status_tx: mpsc::Sender<CopQueueStatusSet>,
}

pub struct FopQueue {
    pending: VecDeque<(CopTaskId, CommandContext)>,
    executed: VecDeque<(CopTaskId, CommandContext)>,
    rejected: VecDeque<(CopTaskId, CommandContext)>,
    last_time: Option<DateTime<Utc>>,
    next_id: CopTaskId,
    confirm_inc: usize,
    vs_at_id0: u32,
}

impl FopQueue {
    pub async fn new(
        vs: u8,
        next_id: CopTaskId,
        context: FopQueueContext
    ) -> Self {
        let mut ret = Self {
            pending: VecDeque::new(),
            executed: VecDeque::new(),
            rejected: VecDeque::new(),
            last_time: None,
            next_id,
            confirm_inc: 0,
            vs_at_id0: vs.wrapping_sub(next_id as u8) as u32,
        };
        ret.update_status(context.queue_status_tx).await;
        ret
    }

    fn create_status(&self) -> CopQueueStatusSet {
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
            confirm_inc: self.confirm_inc as u32,
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

    pub fn get_last_time(&self) -> Option<DateTime<Utc>> {
        self.last_time
    }
    pub fn next_id(&self) -> CopTaskId {
        self.next_id
    }
    pub async fn push(&mut self, context: FopQueueContext, cmd_ctx: CommandContext) -> CopTaskId {
        if self.last_time.is_none() {
            self.last_time = Some(chrono::Utc::now());
        }
        let id = self.next_id;
        self.next_id += 1;
        let id_tco = (id, cmd_ctx.tco.as_ref().clone());
        self.pending.push_back((id, cmd_ctx.clone()));
        let status = CopTaskStatus::from_id_tco(id_tco, CopTaskStatusPattern::Pending, false);
        self.update_status(context.queue_status_tx).await;
        if let Err(e) = context.task_status_tx.send(status).await {
            error!("failed to send COP status: {}", e);
        }
        id
    }
    pub async fn execute(
        &mut self,
        context: FopQueueContext,
    ) -> Option<(u8, CommandContext)> {
        let (id, ctx) = {
            let id_ctx = match self.rejected.pop_front() {
                Some((id,ctx)) => Some((id,ctx)),
                None => self.pending.pop_front(),
            };
            if let Some((id, ctx)) = id_ctx {
                self.confirm_inc = 0;
                self.executed.push_back((id, ctx.clone()));
                (id, ctx)
            } else {
                let index = match self.confirm_inc.checked_rem_euclid(self.executed.len()) {
                    Some(index) => index,
                    None => {
                        self.confirm_inc = 0;
                        0
                    }
                };
                match self.executed.get(index) {
                    Some((id, ctx)) => {
                        self.confirm_inc += 1;
                        (*id, ctx.clone())
                    },
                    None => return None,
                }
            }
        };
        let ret = ((id + self.vs_at_id0) as u8, ctx.clone());
        let status = CopTaskStatus::from_id_tco(
            (id, ctx.tco.as_ref().clone()),
            CopTaskStatusPattern::Executed,
            false,
        );
        if let Err(e) = context.task_status_tx.send(status).await {
            error!("failed to send COP status: {}", e);
        }
        self.update_status(context.queue_status_tx).await;
        Some(ret)
    }

    pub async fn accept(
        &mut self, 
        context: FopQueueContext,
        vr: u8,
    ) {
        let accepted_num = if let Some((head_id, _)) = self.executed.front() {
            if vr.wrapping_sub((head_id + self.vs_at_id0) as u8) > self.executed.len() as u8 {
                info!("length of executed: {} vs {}", self.executed.len(), vr.wrapping_sub((head_id + self.vs_at_id0) as u8));
                0
            } else {
                vr.wrapping_sub((head_id + self.vs_at_id0) as u8)
            }
        } else {
            return
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
    }

    pub async fn reject(
        &mut self,
        context: FopQueueContext,
    ) {
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
    }

    pub async fn clear(
        &mut self, 
        context: FopQueueContext,
        status_pattern: CopTaskStatusPattern
    ) {
        let canceled = self
            .pending
            .drain(..)
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
    }

    pub async fn send_status(&self, queue_status_tx: mpsc::Sender<CopQueueStatusSet>) {
        let status = self.create_status();
        if let Err(e) = queue_status_tx.send(status).await {
            error!("failed to send FOP queue status: {}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use gaia_ccsds_c2a::access::cmd::schema::CommandSchema;
    use gaia_tmtc::{cop::{CopQueueStatusSet, CopTaskStatus, CopTaskStatusPattern}, tco_tmiv::Tco};
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
        let queue = FopQueue::new(vs, initial_id, ctx).await;
        
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

        let cmd_ctx = create_cmd_ctx();

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
            let pending = queue_status.pending.unwrap();
            assert_eq!(pending.head_id, Some(initial_id));
            assert_eq!(pending.task_count, 1 + i);
        }

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
            let pending = queue_status.pending.unwrap();
            if i == task_num - 1 {
                assert_eq!(pending.head_id, None);
                assert_eq!(pending.task_count, 0);
            } else {
                assert_eq!(pending.head_id, Some(initial_id + 1 + i));
            }
            assert_eq!(pending.task_count, task_num - 1 - i);
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
        let pending = queue_status.pending.unwrap();
        assert_eq!(pending.task_count, 0);
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
        assert_eq!(queue_status.pending.unwrap().task_count, 0);
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
            assert_eq!(queue_status.pending.unwrap().task_count, 0);
            assert_eq!(queue_status.executed.unwrap().task_count, i + 1);
            assert_eq!(queue_status.rejected.unwrap().task_count, task_num - accept_num as u32 - i - 1);
        }

        queue.reject(context.clone()).await;
        // キューのステータスが更新されているか確認
        let queue_status = match queue_rx.try_recv() {
            Ok(status) => status,
            Err(e) => panic!("failed to receive COP queue status: {}", e),
        };
        assert_eq!(queue_status.pending.unwrap().task_count, 0);
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