use std::net::SocketAddr;
use tonic::Status;
use tracing::info;

use crate::worker_info::*;

/// Registry for workers.
#[derive(Debug, Default)]
pub struct WorkerRegistry {
    /// Internal vendor for managing worker ID.
    worker_vendor: WorkerIDVendor,

    /// List of workers, each entry contains ID, address of their gRPC server and their state.
    worker_list: Vec<WorkerInfo>,
}

impl WorkerRegistry {
    /// Generate worker information.
    async fn generate_worker_info(
        &mut self,
        worker_address: SocketAddr,
    ) -> Result<WorkerInfo, Status> {
        let vendor = &mut self.worker_vendor;

        // Generate ID for the worker.
        let worker_id = vendor.create_worker();
    
        info!("Appy: A");
        let worker_info = WorkerInfo::new(worker_id, worker_address).await;
        info!("Appy: B");
        
        if worker_info.is_err() {
            vendor.delete_worker(worker_id)
        }

        worker_info
    }

    /// Add worker information into the worker list.
    ///
    /// Note: If the index indicated by the ID is within range of the
    /// existing worker list then we are reusing a slot, otherwise we
    /// are allocating a new spot for the worker.
    fn add_worker_info(&mut self, worker_info: WorkerInfo) {
        let workers_list = &mut self.worker_list;

        let worker_id = worker_info.id;
        let index = Worker::get_worker_index(worker_id) as usize;

        if index < workers_list.len() {
            workers_list[index] = worker_info;
        } else {
            workers_list.push(worker_info);
        }
    }

    /// Add worker to the registry and return the worker handle ID.
    pub async fn register_worker(
        &mut self,
        worker_address: SocketAddr,
    ) -> Result<WorkerID, Status> {
        // Generate worker information (id + address + state).
        let worker_info = self.generate_worker_info(worker_address).await?;
        let worker_id = worker_info.id;

        // Add the worker's information into the worker list.
        self.add_worker_info(worker_info);

        Ok(worker_id)
    }

    /// Remove worker from the registry
    ///
    /// Note: The entry still remains in the list, only
    /// the worker ID is invalidated, and won't be valid until
    /// another worker reuses the ID's index.
    pub fn delete_worker(&mut self, worker_id: WorkerID) {
        let vendor = &mut self.worker_vendor;

        vendor.delete_worker(worker_id);
    }

    /// Set worker state.
    pub fn set_worker_state(&mut self, worker_id: WorkerID, new_state: WorkerState) {
        let index = Worker::get_worker_index(worker_id) as usize;
        let worker = &mut self.worker_list[index];

        worker.state = new_state;
    }

    /// Retrieve free workers.
    pub fn get_free_workers(&self) -> Vec<WorkerID> {
        let vendor = &self.worker_vendor;

        let worker_free = |worker: &WorkerInfo| {
            vendor.worker_valid(worker.id) && matches!(worker.state, WorkerState::Free)
        };

        self.worker_list
            .iter()
            .filter(|worker| worker_free(worker))
            .map(|worker| worker.id)
            .collect()
    }

    /// Size of the worker registry, only counting valid workers.
    pub fn len(&self) -> usize {
        self.get_workers().len()
    }

    pub fn get_workers(&self) -> Vec<&WorkerInfo> {
        self.worker_list
            .iter()
            .filter(|w| self.worker_vendor.worker_valid(w.id))
            .collect()
    }

    pub fn get_worker(&self, worker_id: WorkerID) -> Option<&WorkerInfo> {
        let index = Worker::get_worker_index(worker_id) as usize;
        self.worker_list.get(index)
    }

    pub fn get_worker_mut(&mut self, worker_id: WorkerID) -> Option<&mut WorkerInfo> {
        let index = Worker::get_worker_index(worker_id) as usize;
        self.worker_list.get_mut(index)
    }
}
