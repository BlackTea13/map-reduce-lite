use std::collections::VecDeque;

use crate::{
    core::coordinator::AddJobRequest,
    worker_info::{WorkerID},
};
/// State of the job.
#[derive(Debug, Clone, Copy)]
pub enum JobState {
    /// Job not started.
    Pending,

    /// Mapping phase.
    Mapping,

    /// Shuffling phase.
    Shuffling,

    /// Reducing phase.
    Reducing,

    /// Job completed.
    Completed,
}

/// A job context.
#[derive(Debug, Clone)]
pub struct Job {
    /// The current state of the job.
    state: JobState,

    /// The input file path in s3. e.g `foo/bar/input/*`.
    input_files_path: String,

    /// The output file path in s3. eg `foo/bar/out`.
    output_files_path: String,

    /// The intended workload to be run for the job.
    workload: String,

    /// Auxiliary arguments to pass to the MapReduce application.
    args: Vec<String>,

    /// List of workers who are working on the job.
    workers: Vec<WorkerID>,

    /// Timeout allowed before marked as straggler for worker
    timeout: u32
}

impl Job {
    /// Generate job from request.
    /// NOTE: Be careful, this function is TIGHTLY coupled with the stubs generated from `coordinator.proto`.
    ///       If you make changes to `coordinator.proto`, make sure to propagate changes accordingly.
    pub fn from_request(request: AddJobRequest) -> Self {
        Self {
            state: JobState::Pending,
            input_files_path: request.input_files,
            output_files_path: request.output_files,
            workload: request.workload,
            args: request.aux,
            workers: vec![],
            timeout: request.timeout
        }
    }

    pub fn get_job_state(&self) -> JobState {
        self.state
    }

    pub fn get_input_path(&self) -> &String {
        &self.input_files_path
    }

    pub fn get_output_path(&self) -> &String {
        &self.output_files_path
    }

    pub fn get_workload(&self) -> &String {
        &self.workload
    }

    pub fn get_args(&self) -> &Vec<String> {
        &self.args
    }

    /// Get the state of the job.
    pub fn get_state(&self) -> JobState {
        self.state
    }

    /// Get workers working on the job.
    pub fn get_workers(&self) -> &Vec<WorkerID> {
        &self.workers
    }

    /// Add worker to job.
    pub fn add_worker(&mut self, worker_id: WorkerID) {
        self.workers.push(worker_id);
    }

    /// Add workers to job.
    pub fn add_workers(&mut self, worker_ids: Vec<WorkerID>) {
        worker_ids
            .into_iter()
            .for_each(|worker_id| self.add_worker(worker_id));
    }
}

#[derive(Debug)]
/// Job queue.
///
/// Jobs are kept in order to maintain history.
pub struct JobQueue {
    /// Job contexts.
    jobs: VecDeque<Job>,

    /// Index of the current job.
    current_index: usize,
}

impl JobQueue {
    /// Default Ctor.
    pub fn new() -> Self {
        Self {
            jobs: VecDeque::new(),
            current_index: 0,
        }
    }

    /// The number of task which has been queued (including completed tasks)
    pub fn len(&self) -> usize {
        self.jobs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return the number of jobs which has been processed. (Pointer has passed it.)
    pub fn number_of_jobs_pending(&self) -> usize {
        self.len() - self.current_index
    }

    /// Return the number of jobs which has been processed. (Pointer has passed it.)
    pub fn number_of_jobs_processed(&self) -> usize {
        self.current_index
    }

    pub fn advance(&mut self) {
        self.current_index += 1;
    }

    /// Return the current job and increment to next.
    pub fn pop_job(&mut self) -> Option<Job> {
        let index = self.current_index;
        self.advance();

        // NOTE: We can't return a reference.
        self.jobs.get(index).cloned()
    }

    /// Decrement pointer.
    pub fn revert(&mut self) {
        self.current_index -= 1;
    }

    /// Return the current job.
    pub fn peek_job(&self) -> Option<&Job> {
        self.jobs.get(self.current_index)
    }

    /// Push new job.
    pub fn push_job(&mut self, job: Job) {
        self.jobs.push_back(job);
    }

    /// Flush all jobs. Get rid of all history.
    pub fn flush(&mut self) {
        self.jobs.clear();
        self.current_index = 0;
    }

    /// Get the entries of all jobs.
    pub fn get_all_jobs(&self) -> &VecDeque<Job> {
        &self.jobs
    }
}
