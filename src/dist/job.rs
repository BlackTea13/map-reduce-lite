use std::collections::VecDeque;

pub enum JobState {
  Pending,
  Mapping,
  Shuffling,
  Reducing,
  Completed,
}

pub struct Job {
  state: JobState,
}
