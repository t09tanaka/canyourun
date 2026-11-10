//! Process supervision, leases and cancellation.

pub mod error;
pub mod escalation;
pub mod identity;
pub mod lease;
pub mod lock;
pub mod outcome;
pub mod process;
pub mod recover;
pub mod run;
pub mod spec;
pub mod spool;
pub mod spool_plan;
pub mod stop;
pub mod termination;
pub mod timeouts;

#[cfg(test)]
mod tests;
