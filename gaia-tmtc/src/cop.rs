use std::{
    collections::{HashMap, VecDeque},
    fmt::Debug,
    sync::Arc,
};

use anyhow::Result;
use cop_server::Cop;
use futures::{stream, TryStreamExt};
use tokio::sync::{broadcast, Mutex, RwLock};
use tokio_stream::wrappers::BroadcastStream;
use tonic::{async_trait, Request, Response, Status};

use crate::{Handle, Hook};

pub use gaia_stub::cop::*;

#[derive(Clone)]
pub struct Bus<T> {
    cop_status_tx: broadcast::Sender<Arc<T>>,
}

impl<T> Bus<T> {
    pub fn new(capacity: usize) -> Self {
        let (cop_status_tx, _) = broadcast::channel(capacity);
        Self { cop_status_tx }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Arc<T>> {
        self.cop_status_tx.subscribe()
    }
}

#[async_trait]
impl<T: Send + Sync> Handle<Arc<T>> for Bus<T> {
    type Response = ();

    async fn handle(&mut self, status: Arc<T>) -> Result<Self::Response> {
        // it's ok if there are no receivers
        // so just fire and forget
        self.cop_status_tx.send(status).ok();
        Ok(())
    }
}


pub struct CopStatusStore {
    task_status: RwLock<HashMap<u32, CopTaskStatus>>,
    completed: RwLock<VecDeque<u32>>,
    worker_status: RwLock<CopWorkerStatus>,
    queue_status: RwLock<CopQueueStatusSet>,
    vsvr: RwLock<CopVsvr>,
    capacity: usize,
}

impl CopStatusStore {
    pub fn new(capacity: usize) -> Self {
        Self {
            task_status: RwLock::new(HashMap::new()),
            completed: RwLock::new(VecDeque::with_capacity(capacity)),
            worker_status: RwLock::new(CopWorkerStatus::default()),
            queue_status: RwLock::new(CopQueueStatusSet::default()),
            vsvr: RwLock::new(CopVsvr::default()),
            capacity,
        }
    }

    pub async fn get_task(&self, id: &u32) -> Option<CopTaskStatus> {
        self.task_status.read().await.get(id).cloned()
    }

    pub async fn get_worker(&self) -> CopWorkerStatus {
        self.worker_status.read().await.clone()
    }

    pub async fn get_queue(&self) -> CopQueueStatusSet {
        self.queue_status.read().await.clone()
    }

    pub async fn get_vsvr(&self) -> CopVsvr {
        self.vsvr.read().await.clone()
    }

    pub async fn set_task(&self, status: &CopTaskStatus) {
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
                    self.task_status.write().await.remove(&item);
                }
                {
                    let mut completed = self.completed.write().await;
                    completed.push_back(status.task_id);
                }
            }
            _ => (),
        }
        self.task_status.write().await.insert(id, status.clone());
    }

    pub async fn set_worker(&self, status: &CopWorkerStatus) {
        self.worker_status.write().await.clone_from(status);
    }
    
    pub async fn set_queue(&self, status: &CopQueueStatusSet) {
        self.queue_status.write().await.clone_from(status);
    }

    pub async fn set_vsvr(&self, status: &CopVsvr) {
        self.vsvr.write().await.clone_from(status);
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
impl Hook<Arc<CopTaskStatus>> for StoreCopStatusHook {
    type Output = Arc<CopTaskStatus>;

    async fn hook(&mut self, cop_status: Arc<CopTaskStatus>) -> Result<Self::Output> {
        self.store.set_task(cop_status.as_ref()).await;
        Ok(cop_status)
    }
}

#[async_trait]
impl Hook<Arc<CopWorkerStatus>> for StoreCopStatusHook {
    type Output = Arc<CopWorkerStatus>;

    async fn hook(&mut self, worker_status: Arc<CopWorkerStatus>) -> Result<Self::Output> {
        self.store.set_worker(worker_status.as_ref()).await;
        Ok(worker_status)
    }
}

#[async_trait]
impl Hook<Arc<CopQueueStatusSet>> for StoreCopStatusHook {
    type Output = Arc<CopQueueStatusSet>;

    async fn hook(&mut self, queue_status: Arc<CopQueueStatusSet>) -> Result<Self::Output> {
        self.store.set_queue(queue_status.as_ref()).await;
        Ok(queue_status)
    }
}

#[async_trait]
impl Hook<Arc<CopVsvr>> for StoreCopStatusHook {
    type Output = Arc<CopVsvr>;

    async fn hook(&mut self, vsvr: Arc<CopVsvr>) -> Result<Self::Output> {
        self.store.set_vsvr(vsvr.as_ref()).await;
        Ok(vsvr)
    }
}

pub struct CopService<C> {
    cop_handler: Mutex<C>,
    task_status_bus: Bus<CopTaskStatus>,
    worker_status_bus: Bus<CopWorkerStatus>,
    queue_status_bus: Bus<CopQueueStatusSet>,
    vsvr_bus: Bus<CopVsvr>,
    cop_status_store: Arc<CopStatusStore>,
}

impl<C> CopService<C> {
    pub fn new(
        cop_service: C, 
        task_status_bus: Bus<CopTaskStatus>,  
        worker_status_bus: Bus<CopWorkerStatus>,
        queue_status_bus: Bus<CopQueueStatusSet>,
        vsvr_bus: Bus<CopVsvr>,
        cop_status_store: Arc<CopStatusStore>
    ) -> Self {
        Self {
            cop_handler: Mutex::new(cop_service),
            task_status_bus,
            worker_status_bus,
            queue_status_bus,
            vsvr_bus,
            cop_status_store,
        }
    }
}

#[tonic::async_trait]
impl<C> Cop for CopService<C>
where
    C: super::Handle<Arc<CopCommand>> + Send + Sync + 'static,
    C::Response: Send + 'static,
{
    type OpenTaskStatusStreamStream = stream::BoxStream<'static, Result<CopTaskStatusStreamResponse, Status>>;
    type OpenWorkerStatusStreamStream = stream::BoxStream<'static, Result<CopWorkerStatusStreamResponse, Status>>;
    type OpenQueueStatusStreamStream = stream::BoxStream<'static, Result<CopQueueStatusStreamResponse, Status>>;
    type OpenVsvrStreamStream = stream::BoxStream<'static, Result<CopVsvrStreamResponse, Status>>;

    #[tracing::instrument(skip(self))]
    async fn open_task_status_stream(
        &self,
        _request: Request<CopTaskStatusStreamRequest>,
    ) -> Result<Response<Self::OpenTaskStatusStreamStream>, Status> {
        let rx = self.task_status_bus.subscribe();
        let stream = BroadcastStream::new(rx)
            .map_ok(move |status| CopTaskStatusStreamResponse {
                task_status: Some(status.as_ref().clone()),
            })
            .map_err(|_| Status::data_loss("stream was lagged"));
        Ok(Response::new(Box::pin(stream)))
    }

    #[tracing::instrument(skip(self))]
    async fn open_worker_status_stream(
        &self,
        _request: Request<CopWorkerStatusStreamRequest>,
    ) -> Result<Response<Self::OpenWorkerStatusStreamStream>, Status> {
        let rx = self.worker_status_bus.subscribe();
        let stream = BroadcastStream::new(rx)
            .map_ok(move |status| CopWorkerStatusStreamResponse {
                worker_status: Some(status.as_ref().clone()),
            })
            .map_err(|_| Status::data_loss("stream was lagged"));
        Ok(Response::new(Box::pin(stream)))
    }

    #[tracing::instrument(skip(self))]
    async fn open_queue_status_stream(
        &self,
        _request: Request<CopQueueStatusStreamRequest>,
    ) -> Result<Response<Self::OpenQueueStatusStreamStream>, Status> {
        let rx = self.queue_status_bus.subscribe();
        let stream = BroadcastStream::new(rx)
            .map_ok(move |status| CopQueueStatusStreamResponse {
                queue_status: Some(status.as_ref().clone()),
            })
            .map_err(|_| Status::data_loss("stream was lagged"));
        Ok(Response::new(Box::pin(stream)))
    }

    #[tracing::instrument(skip(self))]
    async fn open_vsvr_stream(
        &self,
        _request: Request<CopVsvrStreamRequest>,
    ) -> Result<Response<Self::OpenVsvrStreamStream>, Status> {
        let rx = self.vsvr_bus.subscribe();
        let stream = BroadcastStream::new(rx)
            .map_ok(move |status| CopVsvrStreamResponse {
                vsvr: Some(status.as_ref().clone()),
            })
            .map_err(|_| Status::data_loss("stream was lagged"));
        Ok(Response::new(Box::pin(stream)))
    }

    #[tracing::instrument(skip(self))]
    async fn get_task_status(
        &self,
        request: Request<GetCopTaskStatusRequest>,
    ) -> Result<Response<GetCopTaskStatusResponse>, Status> {
        let message = request.get_ref();
        let status = self.cop_status_store.get_task(&message.task_id).await;
        Ok(Response::new(GetCopTaskStatusResponse {
            task_status: status,
        }))
    }

    #[tracing::instrument(skip(self))]
    async fn get_all_task_status(
        &self,
        _request: Request<GetAllCopTaskStatusRequest>,
    ) -> Result<Response<GetAllCopTaskStatusResponse>, Status> {
        let task_status = self.cop_status_store.task_status.read().await.clone();
        Ok(Response::new(GetAllCopTaskStatusResponse {
            task_status,
        }))
    }

    #[tracing::instrument(skip(self))]
    async fn get_worker_status(
        &self,
        _request: Request<GetCopWorkerStatusRequest>,
    ) -> Result<Response<GetCopWorkerStatusResponse>, Status> {
        let worker_status = self.cop_status_store.get_worker().await;
        Ok(Response::new(GetCopWorkerStatusResponse {
            worker_status: Some(worker_status),
        }))
    }

    #[tracing::instrument(skip(self))]
    async fn get_queue_status(
        &self,
        _request: Request<GetCopQueueStatusRequest>,
    ) -> Result<Response<GetCopQueueStatusResponse>, Status> {
        let queue_status = self.cop_status_store.get_queue().await;
        Ok(Response::new(GetCopQueueStatusResponse {
            queue_status: Some(queue_status),
        }))
    }

    #[tracing::instrument(skip(self))]
    async fn get_vsvr(
        &self,
        _request: Request<GetCopVsvrRequest>,
    ) -> Result<Response<GetCopVsvrResponse>, Status> {
        let vsvr = self.cop_status_store.get_vsvr().await;
        Ok(Response::new(GetCopVsvrResponse {
            vsvr: Some(vsvr),
        }))
    }

    #[tracing::instrument(skip(self))]
    async fn post_command(
        &self,
        request: Request<PostCopCommandRequest>,
    ) -> Result<Response<PostCopCommandResponse>, Status> {
        let message = request.into_inner();

        let cop_command = message
            .command
            .ok_or_else(|| Status::invalid_argument("cop command is required"))?;

        fn internal_error<E: Debug>(e: E) -> Status {
            Status::internal(format!("{:?}", e))
        }

        self.cop_handler
            .lock()
            .await
            .handle(Arc::new(cop_command))
            .await
            .map_err(internal_error)?;

        Ok(Response::new(PostCopCommandResponse {}))
    }
}
