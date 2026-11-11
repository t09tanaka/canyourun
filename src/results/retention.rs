//! Removing collected results that are past their retention (spec chapter
//! 13: inputs, logs and results are kept for 7 days after the end by
//! default; what runs, waits for collection or is referenced is never
//! removed).
//!
//! Which references go is decided by the parent database
//! ([`CoordinatorStore::gc`]); this deletes the files of the result store
//! whose last reference went, and only then forgets their locators.

use super::collect::CollectError;
use super::plan::retention_ms;
use super::store::ResultStore;
use crate::storage::CoordinatorStore;

/// What one sweep did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SweepReport {
    /// The files and directories of the result store that were deleted.
    pub deleted: Vec<String>,
    /// Queued locators of other stores: left for their owners.
    pub foreign: u32,
}

/// Drop the references of attempts that ended `retention_days` ago or
/// earlier and delete their files. Safe to run again after a crash at any
/// point: a file that was deleted and not yet forgotten is forgotten by the
/// next sweep.
pub fn sweep(
    store: &CoordinatorStore,
    files: &ResultStore,
    now_ms: u64,
    retention_days: u32,
) -> Result<SweepReport, CollectError> {
    store.gc(now_ms, retention_ms(retention_days))?;
    let mut report = SweepReport::default();
    for locator in store.pending_deletes()? {
        if !ResultStore::owns(&locator) {
            report.foreign += 1;
            continue;
        }
        files.remove(&locator)?;
        store.complete_delete(&locator)?;
        report.deleted.push(locator);
    }
    Ok(report)
}
