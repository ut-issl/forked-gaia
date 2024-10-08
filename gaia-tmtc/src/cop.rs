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
    status: RwLock<HashMap<u32, CopQueueStatus>>,
    completed: RwLock<VecDeque<CopQueueStatus>>,
    worker: RwLock<CopWorkerStatus>,
    capacity: usize,
}

impl CopStatusStore {
    pub fn new(capacity: usize) -> Self {
        Self {
            status: RwLock::new(HashMap::new()),
            completed: RwLock::new(VecDeque::with_capacity(capacity)),
            worker: RwLock::new(CopWorkerStatus::default()),
            capacity,
        }
    }

    pub async fn get(&self, id: &Option<u32>) -> Option<CopStatus> {
        match id {
            Some(id) => 
                self.status.read().await.get(id).cloned().map(|status| {
                    CopStatus {
                        inner: Some(cop_status::Inner::QueueStatus(status)),
                    }
                }),
            None => {
                let worker = self.worker.read().await.clone();
                Some(CopStatus {
                    inner: Some(cop_status::Inner::WorkerStatus(worker)),
                })
            }
        }
    }

    pub async fn get_all(&self) -> GetAllStatusResponse {
        GetAllStatusResponse {
            cop_statuses: self.status.read().await.values().cloned().collect(),
            worker_status: Some(self.worker.read().await.clone()),
        }
    }

    pub async fn set(&self, status: Arc<CopStatus>) -> Result<()> {
        let Some(inner) = status.as_ref().clone().inner else {
            return Err(anyhow!("inner status is required"));
        }; 
        match inner {
            cop_status::Inner::QueueStatus(status) => {
                let id = status.cop_id.clone();
                let pattern = status.status();
                match pattern {
                    CopQueueStatusPattern::Accepted | CopQueueStatusPattern::Canceled => {
                        let pop_item = {
                            let mut completed = self.completed.write().await;
                            if completed.len() == self.capacity {
                                completed.pop_front()
                            } else {
                                None
                            }
                        };
                        if let Some(item) = pop_item {
                            self.status.write().await.remove(&item.cop_id);
                        }
                        {
                            let mut completed = self.completed.write().await;
                            completed.push_back(status.clone());
                        }
                    }
                    _ => ()
                }
                self.status.write().await.insert(id, status.clone());
            }
            cop_status::Inner::WorkerStatus(status) => {
                self.worker.write().await.clone_from(&status);
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

#[tonic::async_trait]
impl<C> Cop for CopService<C> 
where
    C: super::Handle<Arc<CopCommand>> + Send + Sync + 'static,
    C::Response: Send + Into<bool> + 'static,
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
    async fn get_status(
        &self,
        request: Request<GetStatusRequest>,
    ) -> Result<Response<GetStatusResponse>, Status> {
        let message = request.get_ref();
        let status = self
            .cop_status_store
            .get(&message.cop_id)
            .await;
        if let Some(status) = status {
            Ok(Response::new(GetStatusResponse {
                cop_status: Some(status.clone()),
            }))
        } else {
            Ok(Response::new(GetStatusResponse {
                cop_status: None,
            }))
        }
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

        let completed_on_time: bool = self.cop_handler
            .lock()
            .await
            .handle(Arc::new(cop_command))
            .await
            .map_err(internal_error)?.into();

        if !completed_on_time {
            return Err(Status::deadline_exceeded("command was not completed on time"));
        }

        Ok(Response::new(PostCommandResponse {}))
    }

}