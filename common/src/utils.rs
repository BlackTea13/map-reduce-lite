//! Utility functions that may be helpful for implementing
//! and testing MapReduce.
//!

// use crate::coordinator::args::Args as CoordinatorArgs; // l8tr
// use crate::worker; // l8tr
// use crate::worker::args::Args as WorkerArgs;
// use crate::{coordinator, COORDINATOR_STARTUP_MS};
use anyhow::Result;
use bytes::Bytes;
// use std::time::Duration;

/// Read an entire [`Bytes`] slice into a [`String`].
///
/// Note that the entire slice will be read into the string.
/// It is the caller's responsibility to ensure the slice is
/// of the correct length. Failure to do so may result in
/// an error being returned, or the string data being incorrect.
///
/// Returns an error if the slice contains invalid UTF-8.
pub fn string_from_bytes(buf: Bytes) -> Result<String> {
    Ok(String::from_utf8(buf.as_ref().into())?)
}

/// Convert a [`String`] to [`Bytes`].
#[inline]
pub fn string_to_bytes(s: String) -> Bytes {
    Bytes::from(s)
}
