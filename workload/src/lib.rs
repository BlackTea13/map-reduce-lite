//! Converts MapReduce application names to actual application code.
//!
//! # Example
//!
//! To get the word count application:
//!
//!
//! # use anyhow::Result;
//! // This is the correct import to use if you are outside the crate:
//! use mrlite::workload;
//! // Since you will be working within the `mrlite` crate,
//! // you should write `use crate::workload;` instead.
//! # fn main() -> Result<()> {
//! let wc = workload::named("wc")?;
//! # Ok(())
//! #
//!

use anyhow::{bail, Result};

use common::Workload;

pub mod grep;
pub mod vertex_degree;
pub mod wc;

/// Gets the [`Workload`] named `name`.
///
/// Returns [`None`] if no application with the given name was found.
pub fn try_named(name: &str) -> Option<Workload> {
    match name {
        "wc" => Some(Workload {
            map_fn: wc::map,
            reduce_fn: wc::reduce,
        }),
        "grep" => Some(Workload {
            map_fn: grep::map,
            reduce_fn: grep::reduce,
        }),
        "vertex-degree" => Some(Workload {
            map_fn: vertex_degree::map,
            reduce_fn: vertex_degree::reduce,
        }),
        _ => None,
    }
}

/// Gets the [`Workload`] named `name`.
///
/// Returns an [`anyhow::Error`] if no application with the given name was found.
pub fn named(name: &str) -> Result<Workload> {
    match try_named(name) {
        Some(app) => Ok(app),
        None => bail!("No app named `{}` found.", name),
    }
}
