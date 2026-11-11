//! Evidence that a process group still holds a process of a managed command.
//!
//! A command is started through task-traffic-controller, which leaves two
//! traces that do not depend on a PID alone: every process the command
//! starts inherits its marker variables, and the descendants seen while it
//! ran are recorded with their start times. A process found by either, that
//! is in the group now and is still the process that was found, proves that
//! the group number is still the command's group.
//!
//! What cannot be found this way proves nothing (a descendant that cleared
//! its environment and was never recorded, or, on macOS, a system binary
//! whose environment the system hides and that the tracker did not see):
//! its group is then left alone.

use super::identity::member_is_ours;
use super::stop::Members;
use crate::platform;
use std::cell::{Cell, RefCell};
use task_traffic_controller::lineage::Tracked;
use task_traffic_controller::managed::{self, Loaded};
use task_traffic_controller::marker::ProcRef;
use task_traffic_controller::{process, Context};

/// How long a scan of the system's processes is good for: a stop asks at
/// every look, and a scan reads every process of the user.
const SCAN_EVERY_MS: u64 = 500;

/// The members of the managed command `id`, found by its markers and its
/// recorded descendants.
pub(crate) struct ManagedMembers<'a> {
    ctx: &'a Context,
    id: &'a str,
    /// The command's processes found by the last scan.
    found: RefCell<Vec<ProcRef>>,
    scanned_at: Cell<Option<u64>>,
}

impl<'a> ManagedMembers<'a> {
    pub(crate) fn new(ctx: &'a Context, id: &'a str) -> Self {
        ManagedMembers {
            ctx,
            id,
            found: RefCell::default(),
            scanned_at: Cell::new(None),
        }
    }

    fn scan(&self) {
        let tracked = match managed::load_sidecar(self.ctx, self.id) {
            Loaded::Ok(sidecar) => sidecar.tracked(),
            _ => Tracked::default(),
        };
        *self.found.borrow_mut() = self.ctx.scan_tracked(self.id, &tracked).alive;
    }

    /// Whether one of the processes found is in `pgid` now. Each is read
    /// again here, so a process found by an earlier scan counts only while
    /// its PID is still that process.
    fn any_in(&self, pgid: i32) -> bool {
        self.found.borrow().iter().any(|p| in_group(p, pgid))
    }
}

/// Whether the process `p` (found as the command's) is in `pgid`, and is
/// still the same process after its group was read.
fn in_group(p: &ProcRef, pgid: i32) -> bool {
    let group = platform::process_group_of(p.pid);
    let after = process::start_time(p.pid);
    member_is_ours(p.start_time.as_ref(), group, after.as_ref(), pgid)
}

/// Whether a scan is due at `now`, when the last one was at `last`.
fn scan_due(last: Option<u64>, now: u64) -> bool {
    last.is_none_or(|at| now.saturating_sub(at) >= SCAN_EVERY_MS)
}

impl Members for ManagedMembers<'_> {
    fn proven(&self, pgid: i32) -> bool {
        if self.any_in(pgid) {
            return true;
        }
        let now = platform::monotonic_ms();
        if !scan_due(self.scanned_at.get(), now) {
            return false;
        }
        self.scan();
        self.scanned_at.set(Some(now));
        self.any_in(pgid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_scan_is_due_at_first_and_then_only_after_the_interval() {
        assert!(scan_due(None, 0));
        assert!(!scan_due(Some(1_000), 1_000));
        assert!(!scan_due(Some(1_000), 1_499));
        assert!(scan_due(Some(1_000), 1_500));
        // A clock that went back never makes a scan due.
        assert!(!scan_due(Some(1_000), 0));
    }

    #[test]
    fn a_process_proves_only_the_group_it_is_in() {
        let me = std::process::id();
        // SAFETY: plain syscall without arguments.
        let mine = unsafe { libc::getpgrp() };
        let this = ProcRef {
            pid: me,
            start_time: process::start_time(me),
        };
        assert!(this.start_time.is_some());
        assert!(in_group(&this, mine));
        assert!(!in_group(&this, mine + 1));
        // The same PID seen with another start time is another process.
        let other = ProcRef {
            pid: me,
            start_time: this.start_time.map(|s| s + 1),
        };
        assert!(!in_group(&other, mine));
        // One whose start time was never learned proves nothing.
        let unread = ProcRef {
            pid: me,
            start_time: None,
        };
        assert!(!in_group(&unread, mine));
    }
}
