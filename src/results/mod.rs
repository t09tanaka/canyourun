//! Logs, outputs and result collection.
//!
//! [`plan`] holds the decisions, [`store`] the files of the parent (results,
//! logs and artifacts, published atomically), [`collect`] fetches the outputs
//! of an ended attempt from its worker and [`retention`] removes what is past
//! its retention.

pub mod collect;
pub mod plan;
pub mod retention;
pub mod store;

pub use collect::{CollectConfig, CollectError, CollectReport, Collector};
pub use store::ResultStore;
