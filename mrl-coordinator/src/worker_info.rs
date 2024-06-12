use std::{fmt::Pointer, net::SocketAddr};

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

#[derive(Debug)]
pub struct WorkerInfo {
    pub id: WorkerID,
    pub addr: SocketAddr,
}

impl WorkerInfo {
    pub fn new(id: WorkerID, addr: SocketAddr) -> Self {
        Self { addr, id }
    }
}
