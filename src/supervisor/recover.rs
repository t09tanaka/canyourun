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
//!
//! The attempt is settled through its log spool like a run that ended
//! normally: the spool is resumed, closed and described in the result. What
//! the command wrote while no supervisor was alive is not in it. A spool the
//! lost supervisor had closed already is settled from its final positions,
//! and an attempt it had made terminal without storing the result gets the
//! result ([`super::run::complete_terminal`]). The exit the lost supervisor
//! saw is not known here: unless the termination was fixed, the attempt is
//! `interrupted` (or `cancelled`), whatever the command did.
//!
//! A leader that is gone does not mean its group is: descendants can outlive
//! it. So the group (from the pgid task-traffic-controller recorded, else the
//! leader's pid) is stopped when it still has members and the leader is the
//! same process or is gone. After a reboot or when the PID was reused,
//! nothing is signalled. When a stop cannot be confirmed the attempt stays
//! unresolved and the parent starts `supervise` again later, which lands here
//! and redoes the stop.

use super::error::Result;
use super::identity::{verdict, Verdict};
use super::pump::Pipe;
use super::run::{
    complete_terminal, open_spool, settle, settle_without_spool, sidecar_child, stop_confirmed,
    unresolved_report, Report, Traffic,
};
use super::spec::{command_id, SuperviseSpec};
use super::stop::{group_alive, StopResult, StopTarget};
use super::termination::{termination, StopReason};
use crate::platform;
use crate::state::AttemptState;
use crate::storage::WorkerStore;
use serde_json::json;
use task_traffic_controller::managed;

pub fn recover(spec: &SuperviseSpec, store: &WorkerStore) -> Result<Report> {
    let attempt = &spec.attempt_id;
    let Some(rec) = store.get(attempt)? else {
        return Err(super::error::SupervisorError::State(format!(
            "attempt {attempt} is unknown"
        )));
    };
    if let Some(t) = rec.termination {
        // Nothing is left to stop: only a result the crash kept back.
        complete_terminal(spec, store, t)?;
        return Ok(Report {
            termination: Some(t),
            finalized_here: false,
            unresolved: None,
            sleep_inhibit: "not held".into(),
        });
    }
    let traffic = Traffic::open(spec)?;
    let boot = platform::boot_id();
    let mut unresolved: Vec<String> = Vec::new();
    let mut unresolved_nodes: Vec<String> = Vec::new();
    for (node, _intent_at, ident) in store.launches(attempt)? {
        let index = spec.nodes.iter().position(|n| n.node_id == node);
        let id = index.map(|i| command_id(attempt, i));
        let start_now = ident
            .as_ref()
            .and_then(|i| platform::process_start_identity(i.pid));
        let v = verdict(ident.as_ref(), boot.as_deref(), start_now.as_deref());
        // The group number is the one task-traffic-controller recorded, which
        // is what the running supervisor stops too.
        let pgid = |pid: u32| {
            id.as_ref()
                .and_then(|id| sidecar_child(&traffic.ctx, id))
                .filter(|(p, _)| *p == pid)
                .map_or(pid as i32, |(_, g)| g)
        };
        let mut group_stopped = false;
        match (v, &ident) {
            (Verdict::Same | Verdict::Gone, Some(i)) => {
                let target = StopTarget {
                    pid: i.pid,
                    pgid: pgid(i.pid),
                    // The same process: its start identity guards the signal.
                    // Gone: only the group's remaining members are looked at.
                    start_identity: (v == Verdict::Same).then(|| i.start_identity.clone()),
                };
                if group_alive(&target) {
                    group_stopped = true;
                    if stop_confirmed(&target, spec) == StopResult::Unconfirmed {
                        unresolved.push(format!("node {node}: the process could not be stopped"));
                        unresolved_nodes.push(node.to_string());
                    }
                }
            }
            (Verdict::CannotTell, _) => {
                unresolved.push(format!("node {node}: the boot id is not readable"));
                unresolved_nodes.push(node.to_string());
            }
            // Rebooted, reused (never signalled) and not recorded are settled
            // by task-traffic-controller's own checks below.
            _ => {}
        }
        store.append_event(
            Some(&rec.run_id),
            Some(attempt),
            "supervisor_recover_node",
            &json!({
                "node": node,
                "verdict": format!("{v:?}"),
                "group_stopped": group_stopped,
            }),
            platform::wall_ms(),
        )?;
        if let Some(id) = &id {
            let problem = match managed::resolve_checked(&traffic.ctx, id) {
                Ok(r) if r.found && !r.released => Some(r.checked.to_string()),
                Ok(_) => None,
                Err(e) => Some(e.to_string()),
            };
            if let Some(why) = problem {
                unresolved.push(format!("node {node}: {why}"));
                unresolved_nodes.push(node.to_string());
            }
        }
    }
    if !unresolved.is_empty() {
        return unresolved_report(
            store,
            attempt,
            &unresolved_nodes,
            unresolved.join("; "),
            "not held".into(),
        );
    }
    let reason =
        (rec.cancel_requested_at_ms.is_some() || rec.retired).then_some(StopReason::Cancel);
    if reason.is_some() && rec.state != AttemptState::Cancelling {
        let _ = store.transition(attempt, AttemptState::Cancelling, platform::wall_ms());
    }
    // Whatever the spool holds of the lost supervisor's output is closed
    // with the attempt; a spool that cannot be resumed is a spool failure.
    let term = termination(reason, None);
    let root = spec.spool_dir();
    // No command of the attempt is alive any more: the FIFOs the lost
    // supervisor made for them have no writer and no reader.
    for index in 0..spec.nodes.len() {
        remove_fifo(&Pipe::path_for(&root, attempt, index));
    }
    let (t, here) = match open_spool(spec, store)? {
        Ok(spool) => settle(store, &root, attempt, spool.end(), term)?,
        Err(e) => settle_without_spool(store, &root, attempt, "open", &e.to_string(), term)?,
    };
    Ok(Report {
        termination: Some(t),
        finalized_here: here,
        unresolved: None,
        sleep_inhibit: "not held".into(),
    })
}

/// Remove `path` when it is a FIFO (never anything else: the name is ours,
/// but a file found there is not removed on the name alone). Best effort: a
/// FIFO that stays is removed with the attempt's directory by GC.
fn remove_fifo(path: &std::path::Path) {
    use std::os::unix::fs::FileTypeExt;
    if std::fs::symlink_metadata(path).is_ok_and(|m| m.file_type().is_fifo()) {
        let _ = std::fs::remove_file(path);
    }
}
