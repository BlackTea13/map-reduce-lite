use std::net::SocketAddr;

use tokio::select;
use tonic::Status;
use tonic::transport::Channel;

use crate::core::{WorkerClient, WorkType};

pub type WorkerID = i32;
pub type WorkerVersion = u16;
pub type WorkerIndex = u16;

pub struct Worker {}

impl Worker {
    pub fn get_worker_version(worker_id: WorkerID) -> WorkerVersion {
        (worker_id >> 16) as WorkerVersion
    }

    pub fn get_worker_index(worker_id: WorkerID) -> WorkerIndex {
        worker_id as WorkerIndex
    }

    pub fn construct_worker_id(
        worker_index: WorkerIndex,
        worker_version: WorkerVersion,
    ) -> WorkerID {
        ((worker_version as WorkerID) << 16) | (worker_index as WorkerID)
    }

    pub fn increment_worker_version(worker_id: WorkerID) -> WorkerID {
        let index = Worker::get_worker_index(worker_id);
        let new_version = Worker::get_worker_version(worker_id) + 1;
        Worker::construct_worker_id(index, new_version)
    }
}

#[derive(Debug)]
pub struct WorkerIDVendor {
    next_worker_id: WorkerID,
    valid_workers: Vec<bool>,
    reusable_workers: Vec<WorkerID>,
    active_worker_version: Vec<WorkerVersion>,
}

impl Default for WorkerIDVendor {
    fn default() -> Self {
        Self {
            next_worker_id: 0,
            valid_workers: Vec::with_capacity(100),
            reusable_workers: Vec::with_capacity(100),
            active_worker_version: Vec::with_capacity(100),
        }
    }
}

impl WorkerIDVendor {
    pub fn new() -> Self {
        WorkerIDVendor::default()
    }

    /// Returns worker ID.
    pub fn create_worker(&mut self) -> WorkerID {
        // Reuse worker ID, if available.
        if let Some(reusable_worker_id) = self.reusable_workers.pop() {
            let new_worker_id = Worker::increment_worker_version(reusable_worker_id);
            let new_worker_version = Worker::get_worker_version(new_worker_id);
            let new_worker_index = Worker::get_worker_index(new_worker_id) as usize;

            self.valid_workers[new_worker_index] = true;
            self.active_worker_version[new_worker_index] = new_worker_version;

            new_worker_id
        } else {
            self.valid_workers.push(true);
            self.active_worker_version.push(0);

            let res = self.next_worker_id;
            self.next_worker_id += 1;

            res
        }
    }

    /// Check whether the worker ID is valid.
    ///
    /// Worker ID is valid when the index is valid, and it
    /// is the most up-to-date version.
    pub fn worker_valid(&self, worker_id: WorkerID) -> bool {
        let index = Worker::get_worker_index(worker_id) as usize;
        let version = Worker::get_worker_version(worker_id);

        self.valid_workers[index] && self.active_worker_version[index] == version
    }

    /// Invalid the worker_id, allowing it to be reused with a new version.
    pub fn delete_worker(&mut self, worker_id: WorkerID) {
        let index = Worker::get_worker_index(worker_id) as usize;
        self.valid_workers[index] = false;
        self.reusable_workers.push(worker_id);
    }
}

#[derive(Debug, Clone, PartialEq, Copy)]
pub enum WorkerState {
    Free,
    Mapping,
    Reducing,
}

impl WorkerState {
    pub fn from_work_type(work_type: WorkType) -> WorkerState {
        match work_type {
            WorkType::Map => WorkerState::Mapping,
            WorkType::Reduce => WorkerState::Reducing,
        }
    }
}

#[derive(Debug)]
pub struct WorkerInfo {
    /// Unique ID for each worker.
    pub id: WorkerID,

    /// The state of the worker.
    pub state: WorkerState,

    // Worker Client for gRPC Communication
    pub client: WorkerClient<Channel>,
}

impl WorkerInfo {
    pub async fn new(id: WorkerID, addr: SocketAddr) -> Result<Self, Status> {
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);

        let sender_clone = sender.clone();
        tokio::task::spawn(async move {
            let mut client = WorkerClient::connect(format!("http://{}", addr).to_string()).await;

            while client.is_err() {
                client = WorkerClient::connect(format!("http://{}", addr).to_string()).await;
            }

            let _ = sender.send(Some(client)).await;
        });

        select! {
            Some(client) = receiver.recv() => {
                Ok(Self {
            client: client.unwrap().unwrap(),
            id,
            state: WorkerState::Free, // By default, workers start off free.
        })
            }
            // TODO: Store timeout config somewhere.
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(5)) => {
                Err(Status::unavailable("I forgot the message"))
            }
        }
    }

    /// Set worker state.
    pub fn set_state(&mut self, new_state: WorkerState) {
        self.state = new_state;
    }
}
