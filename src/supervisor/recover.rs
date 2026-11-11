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
//! same process or is definitely gone. After a reboot or when the PID was
//! reused, nothing is signalled. When what has the PID now cannot be read
//! (any failure other than a definite "no such process"), nothing is
//! signalled either and nothing is concluded: the attempt stays unresolved.
//! When a stop cannot be confirmed the attempt stays unresolved too, and the
//! parent starts `supervise` again later, which lands here and looks again.
//!
//! Every signal goes through [`super::stop`], which judges the group from the
//! recorded identity again immediately before sending it.

use super::error::Result;
use super::identity::{only_identity_unknown, verdict, Leader, Verdict};
use super::members::ManagedMembers;
use super::run::{
    complete_terminal, hidden_suspects_json, open_spool, remove_fifos, settle,
    settle_without_spool, sidecar_child, stop_confirmed_on, unresolved_report, Report, Traffic,
    MAX_HIDDEN_SUSPECTS,
};
use super::spec::{command_id, SuperviseSpec};
use super::stop::{group_state_on, GroupState, NoEvidence, Os, StopResult, StopTarget, System};
use super::termination::{termination, StopReason};
use crate::platform::{self, StartIdentity};
use crate::state::AttemptState;
use crate::storage::WorkerStore;
use serde_json::json;
use task_traffic_controller::managed;

/// Why a node stays unresolved when what has its PID now cannot be read.
const IDENTITY_UNKNOWN: &str = "whose the recorded process or its group is could not be told \
                                (its identity could not be read, or nothing left in the group \
                                is known to be the command's), so nothing was signalled; it is \
                                looked at again later";

pub fn recover(spec: &SuperviseSpec, store: &WorkerStore) -> Result<Report> {
    recover_on(spec, store, None)
}

/// [`recover`]; with `fake`, the system calls that read identities and send
/// signals are the caller's (a test records them instead of sending).
/// Without it they are the real ones, with the evidence about each
/// command's members taken from task-traffic-controller.
pub(crate) fn recover_on(
    spec: &SuperviseSpec,
    store: &WorkerStore,
    fake: Option<&dyn Os>,
) -> Result<Report> {
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
            identity_unknown: false,
            sleep_inhibit: "not held".into(),
        });
    }
    let traffic = Traffic::open(spec)?;
    let boot = platform::boot_id();
    // Each reason, and whether it is only that an identity could not be told.
    let mut unresolved: Vec<(String, bool)> = Vec::new();
    let mut unresolved_nodes: Vec<String> = Vec::new();
    let mut hidden: Vec<serde_json::Value> = Vec::new();
    for (node, _intent_at, ident) in store.launches(attempt)? {
        let index = spec.nodes.iter().position(|n| n.node_id == node);
        let id = index.map(|i| command_id(attempt, i));
        // What a gone leader left under its group number is signalled only
        // with evidence that a member is the command's own.
        let members = id.as_ref().map(|id| ManagedMembers::new(&traffic.ctx, id));
        let real = System(match &members {
            Some(m) => m,
            None => &NoEvidence,
        });
        let os: &dyn Os = fake.unwrap_or(&real);
        let start_now = ident
            .as_ref()
            .map_or(StartIdentity::Unknown, |i| os.read(i.pid).start);
        let v = verdict(ident.as_ref(), boot.as_deref(), &start_now);
        // The group number is the one task-traffic-controller recorded, which
        // is what the running supervisor stops too.
        let pgid = |pid: u32| {
            id.as_ref()
                .and_then(|id| sidecar_child(&traffic.ctx, id))
                .filter(|(p, _)| *p == pid)
                .map_or(pid as i32, |(_, g)| g)
        };
        let mut group_stopped = false;
        let mut identity_unknown = false;
        match (v, &ident) {
            (Verdict::Same | Verdict::Gone, Some(i)) => {
                // The recorded identity goes with the target in both cases:
                // the PID is read and compared again right before every
                // signal, so a PID reused since the verdict is not signalled
                // and a read that fails then sends nothing.
                let target = StopTarget {
                    pid: i.pid,
                    pgid: pgid(i.pid),
                    leader: Leader::Known(i.start_identity.clone()),
                };
                group_stopped = group_state_on(os, &target) == GroupState::Alive;
                let problem = match stop_confirmed_on(os, &target, spec) {
                    StopResult::Confirmed => None,
                    StopResult::Unconfirmed => Some("the process could not be stopped"),
                    StopResult::IdentityUnknown => {
                        identity_unknown = true;
                        Some(IDENTITY_UNKNOWN)
                    }
                };
                if let Some(why) = problem {
                    unresolved.push((format!("node {node}: {why}"), identity_unknown));
                    unresolved_nodes.push(node.to_string());
                }
            }
            (Verdict::CannotTell, _) => {
                unresolved.push((format!("node {node}: the boot id is not readable"), false));
                unresolved_nodes.push(node.to_string());
            }
            (Verdict::Unknown, _) => {
                identity_unknown = true;
                unresolved.push((format!("node {node}: {IDENTITY_UNKNOWN}"), true));
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
                // An entry kept for a node whose processes could not be told
                // is that same reason again, not another one.
                Ok(r) if r.found && !r.released && !identity_unknown => Some(r.checked.to_string()),
                Ok(_) => None,
                Err(e) => Some(e.to_string()),
            };
            if let Some(why) = problem {
                unresolved.push((format!("node {node}: {why}"), false));
                unresolved_nodes.push(node.to_string());
                let room = MAX_HIDDEN_SUSPECTS.saturating_sub(hidden.len());
                hidden.extend(
                    hidden_suspects_json(&traffic.ctx, id)
                        .into_iter()
                        .take(room),
                );
            }
        }
    }
    if !unresolved.is_empty() {
        let by_identity = only_identity_unknown(unresolved.iter().map(|(_, by)| *by));
        let reasons: Vec<&str> = unresolved.iter().map(|(why, _)| why.as_str()).collect();
        return unresolved_report(
            store,
            attempt,
            &unresolved_nodes,
            reasons.join("; "),
            by_identity,
            "not held".into(),
            hidden,
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
    remove_fifos(spec);
    let (t, here) = match open_spool(spec, store)? {
        Ok(spool) => settle(store, &root, attempt, spool.end(), term)?,
        Err(e) => settle_without_spool(store, &root, attempt, "open", &e.to_string(), term)?,
    };
    Ok(Report {
        termination: Some(t),
        finalized_here: here,
        unresolved: None,
        identity_unknown: false,
        sleep_inhibit: "not held".into(),
    })
}

#[cfg(test)]
mod tests {
    //! Recovery against a system that only records: the recorded process is
    //! made up, no managed entry exists for it, and every identity read and
    //! signal goes to the fake. Nothing here can reach a real `kill`.

    use super::super::stop::fake::{id, FakeOs, NO_SUCH_PID};
    use super::*;
    use crate::ids::{
        AttemptId, CoordinatorEpoch, Digest, Generation, NodeId, RunId, TaskId, WorkerBootId,
        WorkerId,
    };
    use crate::state::Outcome;
    use crate::storage::{NewAcceptance, ProcessIdentity};
    use crate::supervisor::spec::NodeSpec;

    const START: &str = "linux:1000";

    struct Fx {
        _dir: tempfile::TempDir,
        store: WorkerStore,
        spec: SuperviseSpec,
    }

    /// An attempt whose lost supervisor recorded one launch: a process of
    /// this boot with a PID that cannot exist and the start identity `START`.
    fn fx() -> Fx {
        let dir = tempfile::tempdir().unwrap();
        let state = dir.path().join("state");
        let traffic = dir.path().join("traffic");
        std::fs::create_dir_all(&traffic).unwrap();
        let worker = WorkerId::parse("w-test").unwrap();
        let store = WorkerStore::open(&state, &worker).unwrap();
        let attempt = AttemptId::generate();
        store
            .submit(
                NewAcceptance {
                    attempt_id: attempt.clone(),
                    run_id: RunId::generate(),
                    task_id: TaskId::generate(),
                    generation: Generation(1),
                    payload_hash: Digest::of_bytes(b"p"),
                    coordinator_epoch: CoordinatorEpoch(1),
                    spec: json!({}),
                },
                1,
            )
            .unwrap();
        for (to, at) in [(AttemptState::Preparing, 2), (AttemptState::Ready, 3)] {
            store.transition(&attempt, to, at).unwrap();
        }
        let node = NodeId::parse("n").unwrap();
        store.claim_launch(&attempt, &node, 10).unwrap();
        let boot = platform::boot_id().expect("a boot id");
        store
            .record_process_identity(
                &attempt,
                &node,
                &ProcessIdentity {
                    boot_id: WorkerBootId::parse(&boot).unwrap(),
                    pid: NO_SUCH_PID,
                    start_identity: START.into(),
                },
                11,
            )
            .unwrap();
        let spec = SuperviseSpec {
            state_dir: state,
            worker_id: worker,
            attempt_id: attempt,
            traffic_state_dir: traffic,
            traffic_config: None,
            nodes: vec![NodeSpec {
                node_id: node,
                argv: vec!["/bin/true".into()],
                cwd: dir.path().to_path_buf(),
                env: Default::default(),
                weight: Some(1),
                timeout_ms: None,
            }],
            lease_ms: 60_000,
            term_grace_ms: 50,
            kill_wait_ms: 50,
            poll_ms: 5,
            command_timeout_ms: None,
            resource_wait_timeout_ms: None,
            deadline_ms: None,
            lease_seed: None,
            inhibit_sleep: false,
            spool_root: Some(dir.path().join("spool")),
            log_limit_bytes: 1 << 20,
            workspace_warnings: vec![],
            mask: Default::default(),
            artifacts: None,
        };
        Fx {
            _dir: dir,
            store,
            spec,
        }
    }

    fn node_event(f: &Fx) -> serde_json::Value {
        let events = f
            .store
            .events_of(&f.spec.attempt_id, "supervisor_recover_node")
            .unwrap();
        events.last().expect("a node event").payload.clone()
    }

    fn assert_unresolved_for_identity(f: &Fx, report: &Report) {
        let why = report.unresolved.as_deref().expect("unresolved");
        assert!(
            why.contains("node n: whose the recorded process or its group is"),
            "{why}"
        );
        assert!(why.contains("nothing was signalled"), "{why}");
        assert!(report.identity_unknown, "no other reason");
        assert_eq!(report.termination, None);
        let rec = f.store.get(&f.spec.attempt_id).unwrap().unwrap();
        assert_eq!(rec.termination, None, "nothing was settled");
        let unresolved = f
            .store
            .events_of(&f.spec.attempt_id, "supervisor_unresolved")
            .unwrap();
        assert_eq!(unresolved.len(), 1);
        assert_eq!(unresolved[0].payload["nodes"], json!(["n"]));
        assert_eq!(unresolved[0].payload["identity_unknown"], true);
        assert_eq!(
            unresolved[0].payload["hidden_suspects"],
            json!([]),
            "no hidden process holds an entry that was never kept"
        );
    }

    #[test]
    fn what_a_gone_leader_left_is_not_signalled_without_evidence_of_a_member() {
        let f = fx();
        // No process has the recorded PID and its number has members, but
        // none is known to be the command's: the PID may have been taken
        // and left by a process that started a group of its own.
        let os = FakeOs::new([StartIdentity::NoProcess]);
        os.proven.set(false);
        let report = recover_on(&f.spec, &f.store, Some(&os)).unwrap();
        assert!(os.sent().is_empty(), "only probes: {:?}", os.signals);
        assert_unresolved_for_identity(&f, &report);
        let event = node_event(&f);
        assert_eq!(event["verdict"], "Gone");
        assert_eq!(event["group_stopped"], false);
    }

    #[test]
    fn a_zombie_with_the_pid_is_judged_by_its_identity() {
        // Another process's zombie has the PID: nothing is probed or sent.
        let f = fx();
        let os = FakeOs::new([StartIdentity::Zombie("linux:2000".into())]);
        let report = recover_on(&f.spec, &f.store, Some(&os)).unwrap();
        assert!(os.signals.borrow().is_empty());
        assert_eq!(report.termination.unwrap().outcome(), Outcome::Interrupted);
        assert_eq!(node_event(&f)["verdict"], "PidReused");
        // The recorded process itself, ended and not reaped: what it left
        // is stopped when a member is known to be the command's.
        let f = fx();
        let os = FakeOs::new([StartIdentity::Zombie(START.into())]);
        let report = recover_on(&f.spec, &f.store, Some(&os)).unwrap();
        assert_eq!(os.sent(), vec![(NO_SUCH_PID as i32, libc::SIGTERM)]);
        assert_eq!(report.termination.unwrap().outcome(), Outcome::Interrupted);
        assert_eq!(node_event(&f)["verdict"], "Gone");
    }

    #[test]
    fn an_unreadable_identity_signals_nothing_and_leaves_the_attempt_unresolved() {
        let f = fx();
        let os = FakeOs::new([StartIdentity::Unknown]);
        let report = recover_on(&f.spec, &f.store, Some(&os)).unwrap();
        assert!(os.signals.borrow().is_empty(), "not even a probe");
        assert_unresolved_for_identity(&f, &report);
        let event = node_event(&f);
        assert_eq!(event["verdict"], "Unknown");
        assert_eq!(event["group_stopped"], false);
    }

    #[test]
    fn an_identity_that_becomes_unreadable_before_the_signal_sends_nothing() {
        let f = fx();
        // The verdict sees the same process; every later read fails.
        let os = FakeOs::new([id(START), StartIdentity::Unknown]);
        let report = recover_on(&f.spec, &f.store, Some(&os)).unwrap();
        assert!(os.signals.borrow().is_empty());
        assert_unresolved_for_identity(&f, &report);
        assert_eq!(node_event(&f)["verdict"], "Same");
    }

    #[test]
    fn a_later_recovery_that_can_read_the_identity_settles_the_attempt() {
        let f = fx();
        let os = FakeOs::new([StartIdentity::Unknown]);
        assert!(recover_on(&f.spec, &f.store, Some(&os))
            .unwrap()
            .unresolved
            .is_some());
        // Looked at again: now the system says there is no such process and
        // its group is empty.
        let os = FakeOs::new([StartIdentity::NoProcess]);
        os.members.set(false);
        let report = recover_on(&f.spec, &f.store, Some(&os)).unwrap();
        assert_eq!(report.unresolved, None);
        assert_eq!(report.termination.unwrap().outcome(), Outcome::Interrupted);
        assert!(os.sent().is_empty(), "only probes: {:?}", os.signals);
        assert_eq!(node_event(&f)["verdict"], "Gone");
    }

    #[test]
    fn a_reused_pid_is_never_signalled_or_probed() {
        let f = fx();
        let os = FakeOs::new([id("linux:2000")]);
        let report = recover_on(&f.spec, &f.store, Some(&os)).unwrap();
        assert!(os.signals.borrow().is_empty());
        assert_eq!(report.termination.unwrap().outcome(), Outcome::Interrupted);
        assert_eq!(node_event(&f)["verdict"], "PidReused");
    }

    #[test]
    fn the_same_process_is_stopped_through_its_group() {
        let f = fx();
        // The same process until TERM; then it is gone and its group empty.
        let os = FakeOs::new([
            id(START),
            id(START),
            id(START),
            id(START),
            StartIdentity::NoProcess,
        ]);
        let report = recover_on(&f.spec, &f.store, Some(&os)).unwrap();
        assert_eq!(os.sent(), vec![(NO_SUCH_PID as i32, libc::SIGTERM)]);
        assert_eq!(report.termination.unwrap().outcome(), Outcome::Interrupted);
        let event = node_event(&f);
        assert_eq!(event["verdict"], "Same");
        assert_eq!(event["group_stopped"], true);
    }

    #[test]
    fn a_pid_reused_between_the_verdict_and_the_signal_is_not_signalled() {
        let f = fx();
        // Gone at the verdict; by the stop another process has the PID.
        let os = FakeOs::new([StartIdentity::NoProcess, id("linux:2000")]);
        let report = recover_on(&f.spec, &f.store, Some(&os)).unwrap();
        assert!(os.signals.borrow().is_empty());
        assert_eq!(report.termination.unwrap().outcome(), Outcome::Interrupted);
    }
}
