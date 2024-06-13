#[derive(Debug)]
pub enum JobState {
    Pending,
    Mapping,
    Shuffling,
    Reducing,
    Completed,
}

#[derive(Debug)]
pub struct Job {
    state: JobState,
}
