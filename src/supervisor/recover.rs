//! Settling an attempt whose supervisor is gone (spec chapter 12: supervisor
//! crash, worker reboot, agent restart that finds a dead supervisor).
//!
//! The command is never started again. Each recorded launch is compared with
//! the system by boot ID and process start identity; a process that is still
//! the same one is stopped (TERM, then KILL, confirmed), the task-traffic-
//! controller entry is settled by its own checks, and only then is the attempt
//! fixed as `interrupted` (or `cancelled` when a cancel had been requested).
//! A launch that might have started a process but left no identity is
//! investigated through task-traffic-controller and, when that cannot rule a
//! process out, the attempt stays unresolved.

use super::error::Result;
use super::identity::{verdict, Verdict};
use super::run::{finalize, stop_times, Report, Traffic};
use super::spec::{command_id, SuperviseSpec};
use super::stop::{stop_group, StopResult, StopTarget};
use super::termination::{termination, StopReason};
use crate::platform;
use crate::state::AttemptState;
use crate::storage::{Finalized, WorkerStore};
use serde_json::json;
use task_traffic_controller::managed;

pub fn recover(spec: &SuperviseSpec, store: &WorkerStore) -> Result<Report> {
    let attempt = &spec.attempt_id;
    let Some(rec) = store.get(attempt)? else {
        return Err(super::error::SupervisorError::State(format!(
            "attempt {attempt} is unknown"
        )));
    };
    let traffic = Traffic::open(spec)?;
    let times = stop_times(spec);
    let boot = platform::boot_id();
    let mut unresolved: Vec<String> = Vec::new();
    for (node, _intent_at, ident) in store.launches(attempt)? {
        let start_now = ident
            .as_ref()
            .and_then(|i| platform::process_start_identity(i.pid));
        let v = verdict(ident.as_ref(), boot.as_deref(), start_now.as_deref());
        match (v, &ident) {
            (Verdict::Same, Some(i)) => {
                let target = StopTarget {
                    pid: i.pid,
                    pgid: i.pid as i32,
                    start_identity: Some(i.start_identity.clone()),
                };
                if stop_group(&target, times) == StopResult::Unconfirmed {
                    unresolved.push(format!("node {node}: the process could not be stopped"));
                }
            }
            (Verdict::CannotTell, _) => {
                unresolved.push(format!("node {node}: the boot id is not readable"));
            }
            // Gone, rebooted, reused (never signalled) and not recorded are
            // settled by task-traffic-controller's own checks below.
            _ => {}
        }
        store.append_event(
            Some(&rec.run_id),
            Some(attempt),
            "supervisor_recover_node",
            &json!({ "node": node, "verdict": format!("{v:?}") }),
            platform::wall_ms(),
        )?;
        if let Some(index) = spec.nodes.iter().position(|n| n.node_id == node) {
            let id = command_id(attempt, index);
            match managed::resolve_checked(&traffic.ctx, &id) {
                Ok(r) if r.found && !r.released => {
                    unresolved.push(format!("node {node}: {}", r.checked));
                }
                Ok(_) => {}
                Err(e) => unresolved.push(format!("node {node}: {e}")),
            }
        }
    }
    if !unresolved.is_empty() {
        return Ok(Report {
            termination: None,
            finalized_here: false,
            unresolved: Some(unresolved.join("; ")),
            sleep_inhibit: "not held".into(),
        });
    }
    let reason =
        (rec.cancel_requested_at_ms.is_some() || rec.retired).then_some(StopReason::Cancel);
    if reason.is_some() && rec.state != AttemptState::Cancelling {
        let _ = store.transition(attempt, AttemptState::Cancelling, platform::wall_ms());
    }
    let (t, here) = match finalize(store, attempt, termination(reason, None))? {
        Finalized::Won(t) => (t, true),
        Finalized::AlreadyTerminal(t) => (t, false),
    };
    Ok(Report {
        termination: Some(t),
        finalized_here: here,
        unresolved: None,
        sleep_inhibit: "not held".into(),
    })
}
