use std::{collections::{HashMap, VecDeque}, fmt::Debug, sync::Arc};

use anyhow::{anyhow, Result};
use cop_server::Cop;
use futures::{stream, TryStreamExt};
use tokio::sync::{broadcast, Mutex, RwLock};
use tokio_stream::wrappers::BroadcastStream;
use tonic::{async_trait, Request, Response, Status};

use crate::{Handle, Hook};

pub use gaia_stub::cop::*;

#[derive(Clone)]
pub struct Bus {
    cop_status_tx: broadcast::Sender<Arc<CopStatus>>,
}

impl Bus {
    pub fn new(capacity: usize) -> Self {
        let (cop_status_tx, _) = broadcast::channel(capacity);
        Self { cop_status_tx }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Arc<CopStatus>> {
        self.cop_status_tx.subscribe()
    }
}

#[async_trait]
impl Handle<Arc<CopStatus>> for Bus {
    type Response = ();

    async fn handle(&mut self, tmiv: Arc<CopStatus>) -> Result<Self::Response> {
        // it's ok if there are no receivers
        // so just fire and forget
        self.cop_status_tx.send(tmiv).ok();
        Ok(())
    }
}

pub struct CopStatusStore {
    status: RwLock<HashMap<u32, CopTaskStatus>>,
    completed: RwLock<VecDeque<u32>>,
    worker: RwLock<WorkerStatusSet>,
    capacity: usize,
}

impl CopStatusStore {
    pub fn new(capacity: usize) -> Self {
        Self {
            status: RwLock::new(HashMap::new()),
            completed: RwLock::new(VecDeque::with_capacity(capacity)),
            worker: RwLock::new(WorkerStatusSet::default()),
            capacity,
        }
    }

    pub async fn get(&self, id: &Option<u32>) -> Option<CopTaskStatus> {
        match id {
            Some(id) => 
                self.status.read().await.get(id).cloned(),
            None => {
                None
            }
        }
    }

    pub async fn get_worker(&self) -> WorkerStatusSet {
        self.worker.read().await.clone()
    }

    pub async fn get_all(&self) -> GetAllStatusResponse {
        GetAllStatusResponse {
            task_statuses: self.status.read().await.values().cloned().collect(),
            worker_status: Some(self.worker.read().await.clone()),
        }
    }

    pub async fn set(&self, status: Arc<CopStatus>) -> Result<()> {
        let Some(inner) = status.as_ref().clone().inner else {
            return Err(anyhow!("inner status is required"));
        }; 
        match inner {
            cop_status::Inner::TaskStatus(status) => {
                let id = status.task_id;
                let pattern = status.status();
                match pattern {
                    CopTaskStatusPattern::Accepted | CopTaskStatusPattern::Canceled => {
                        let pop_item = {
                            let mut completed = self.completed.write().await;
                            if completed.len() == self.capacity {
                                completed.pop_front()
                            } else {
                                None
                            }
                        };
                        if let Some(item) = pop_item {
                            self.status.write().await.remove(&item);
                        }
                        {
                            let mut completed = self.completed.write().await;
                            completed.push_back(status.task_id);
                        }
                    }
                    _ => ()
                }
                self.status.write().await.insert(id, status.clone());
            }
            cop_status::Inner::WorkerState(state) => {
                self.worker.write().await.worker_state.clone_from(&Some(state));
            }
            cop_status::Inner::TaskQueueStatus(status_set) => {
                self.worker.write().await.task_queue_status.clone_from(&Some(status_set));
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct StoreCopStatusHook {
    store: Arc<CopStatusStore>,
}

impl StoreCopStatusHook {
    pub fn new(store: Arc<CopStatusStore>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl Hook<Arc<CopStatus>> for StoreCopStatusHook {
    type Output = Arc<CopStatus>;

    async fn hook(&mut self, cop_status: Arc<CopStatus>) -> Result<Self::Output> {    
        self.store.set(cop_status.clone()).await?;    
        Ok(cop_status)
    }
}

pub struct CopService<C> {
    cop_handler: Mutex<C>,
    cop_status_bus: Bus,
    cop_status_store: Arc<CopStatusStore>,
}

impl<C> CopService<C> {
    pub fn new(cop_service: C, cop_status_bus: Bus, cop_status_store: Arc<CopStatusStore>) -> Self {
        Self {
            cop_handler: Mutex::new(cop_service),
            cop_status_bus,
            cop_status_store,
        }
    }
}

pub trait IsTimeout {
    fn is_timeout(&self) -> bool;
}

#[tonic::async_trait]
impl<C> Cop for CopService<C> 
where
    C: super::Handle<Arc<CopCommand>> + Send + Sync + 'static,
    C::Response: Send + IsTimeout + 'static,
{
    type OpenStatusStreamStream = stream::BoxStream<'static, Result<StatusStreamResponse, Status>>;
    
    #[tracing::instrument(skip(self))]
    async fn open_status_stream(
        &self,
        _request: Request<StatusStreamRequest>,
    ) -> Result<Response<Self::OpenStatusStreamStream>, Status> {
        let rx = self.cop_status_bus.subscribe();
        let stream = BroadcastStream::new(rx)
            .map_ok(move |status| StatusStreamResponse {
                cop_status: Some(status.as_ref().clone()),
            })
            .map_err(|_| Status::data_loss("stream was lagged"));
        Ok(Response::new(Box::pin(stream)))
    }

    #[tracing::instrument(skip(self))]
    async fn get_task_status(
        &self,
        request: Request<GetTaskStatusRequest>,
    ) -> Result<Response<GetTaskStatusResponse>, Status> {
        let message = request.get_ref();
        let status = self
            .cop_status_store
            .get(&message.task_id)
            .await;
        Ok(Response::new(GetTaskStatusResponse {
            task_status: status,
        }))
    }
    #[tracing::instrument(skip(self))]
    async fn get_worker_status(
        &self,
        _request: Request<GetWorkerStatusRequest>,
    ) -> Result<Response<GetWorkerStatusResponse>, Status> {
        let worker_status = self.cop_status_store.get_worker().await;
        Ok(Response::new(GetWorkerStatusResponse { worker_status: Some(worker_status) }))
    }

    #[tracing::instrument(skip(self))]
    async fn get_all_status(
        &self,
        _request: Request<GetAllStatusRequest>,
    ) -> Result<Response<GetAllStatusResponse>, Status> {
        let statuses = self.cop_status_store.get_all().await;
        Ok(Response::new(statuses))
    }

    #[tracing::instrument(skip(self))]
    async fn post_command(
        &self,
        request: Request<PostCommandRequest>,
    ) -> Result<Response<PostCommandResponse>, Status> {
        let message = request.into_inner();

        let cop_command = message
            .command
            .ok_or_else(|| Status::invalid_argument("tco is required"))?;

        fn internal_error<E: Debug>(e: E) -> Status {
            Status::internal(format!("{:?}", e))
        }

        let time_out: bool = self.cop_handler
            .lock()
            .await
            .handle(Arc::new(cop_command))
            .await
            .map_err(internal_error)?.is_timeout();

        if time_out {
            return Err(Status::deadline_exceeded("command was not completed on time"));
        }

        Ok(Response::new(PostCommandResponse {}))
    }

}