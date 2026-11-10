//! The worker agent's decisions, free of I/O (spec chapters 11 and 12).
//!
//! What a submit may carry, what a restarted agent does with each attempt it
//! finds, how a retire and a cancel are answered, which journal entries are
//! wire events, and the order of the startup steps.

use crate::state::{AttemptState, Receipt};
use crate::wire::{AttemptPayload, CancelPhase, EventKind, RetireStatus};
use serde_json::Value;
use std::collections::BTreeSet;

/// Commands in one attempt.
pub const MAX_COMMANDS: usize = 256;
/// Bytes of all argv, cwd and env strings of one attempt.
pub const MAX_COMMAND_BYTES: usize = 1024 * 1024;
/// Attempts in one `list_attempts` answer.
pub const MAX_LIST_ATTEMPTS: u32 = 500;
/// Events in one `watch_events` answer.
pub const MAX_EVENTS: u32 = 500;
/// Journal entries of one attempt a `watch_events` reads.
pub const MAX_EVENT_SCAN: u32 = 100_000;
/// Digests in one `blob_verify`.
pub const MAX_VERIFY_DIGESTS: usize = 4096;

/// Why a payload is refused before anything is stored. The sender's numbers
/// are not trusted: the checks run on what actually arrived.
pub fn check_payload(p: &AttemptPayload) -> Result<(), &'static str> {
    if p.commands.is_empty() {
        return Err("the attempt has no command");
    }
    if p.commands.len() > MAX_COMMANDS {
        return Err("the attempt has too many commands");
    }
    if p.lease_ms == 0 {
        return Err("lease_ms must be positive");
    }
    if p.command_timeout_ms == Some(0)
        || p.deadline_ms == Some(0)
        || p.resource_wait_timeout_ms == Some(0)
    {
        return Err("a timeout of zero is not a timeout");
    }
    let mut seen = BTreeSet::new();
    let mut bytes = 0usize;
    for c in &p.commands {
        if c.argv.first().is_none_or(|a| a.is_empty()) {
            return Err("a command is empty");
        }
        if !seen.insert(c.node_id.as_str()) {
            return Err("a node_id is used twice");
        }
        if c.timeout_ms == Some(0) {
            return Err("a timeout of zero is not a timeout");
        }
        bytes += c.cwd.len();
        bytes += c.argv.iter().map(String::len).sum::<usize>();
        bytes += c.env.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>();
    }
    if bytes > MAX_COMMAND_BYTES {
        return Err("the commands are too large");
    }
    Ok(())
}

/// Whether the machine was restarted since the agent last ran. A first start
/// (nothing recorded) is not a restart.
pub fn boot_changed(recorded: Option<&str>, now: &str) -> bool {
    recorded.is_some_and(|r| r != now)
}

/// What a starting agent knows about one attempt that is not terminal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AttemptFacts {
    pub state: AttemptState,
    /// A launch intent exists: a command may have been spawned.
    pub has_launches: bool,
    /// A cancel or a retire was persisted.
    pub stop_requested: bool,
    /// The supervisor's spec file is on disk.
    pub has_spec: bool,
}

/// How an attempt nobody may have started is fixed without a supervisor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Settle {
    Cancelled,
    Interrupted,
}

/// What the agent does with an attempt it finds at startup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Recovery {
    /// Terminal already.
    Nothing,
    /// Nothing was started and nothing forbids it: prepare and launch.
    Resume,
    /// Start `supervise` for it. A supervisor that still lives answers
    /// "alive" (it is adopted); otherwise the new one recovers: it stops what
    /// is left and fixes the result. It never starts the command again.
    Supervise,
    /// No command was ever started: fix the result here, under the
    /// supervisor lock.
    Settle(Settle),
    /// A command may have run and there is no spec to recover it with: the
    /// attempt stays as it is and is reported.
    Unresolved,
}

/// Spec chapter 12: an agent restart reconnects to living supervisors and
/// never restarts a command because the database says "active"; a changed
/// boot ID ends everything that was not finished as `interrupted`.
pub fn decide_recovery(boot_changed: bool, f: AttemptFacts) -> Recovery {
    use AttemptState::*;
    if f.state == Terminal {
        return Recovery::Nothing;
    }
    let may_have_started = f.has_launches || matches!(f.state, Starting | Active);
    if may_have_started {
        return if f.has_spec {
            Recovery::Supervise
        } else {
            Recovery::Unresolved
        };
    }
    if f.stop_requested {
        return Recovery::Settle(Settle::Cancelled);
    }
    if boot_changed || matches!(f.state, Cancelling | Finalizing) {
        return Recovery::Settle(Settle::Interrupted);
    }
    Recovery::Resume
}

/// The answer to `retire_attempt` (the tombstone is persisted in every case).
pub fn retire_status(
    known: bool,
    was_terminal: bool,
    now_terminal: bool,
    had_launches: bool,
) -> RetireStatus {
    if !known {
        RetireStatus::NotStarted
    } else if was_terminal {
        RetireStatus::AlreadyTerminal
    } else if !now_terminal {
        RetireStatus::Unconfirmed
    } else if had_launches {
        RetireStatus::Stopped
    } else {
        RetireStatus::NotStarted
    }
}

/// Cancel accepted and cancel completed are different answers.
pub fn cancel_phase(state: Option<AttemptState>) -> CancelPhase {
    if state == Some(AttemptState::Terminal) {
        CancelPhase::Completed
    } else {
        CancelPhase::Accepted
    }
}

/// A journal entry as a wire event, or `None` for the entries that are
/// diagnostics only.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WireEvent {
    pub kind: EventKind,
    pub state: Option<AttemptState>,
    pub receipt: Option<Receipt>,
}

pub fn wire_event(kind: &str, payload: &Value) -> Option<WireEvent> {
    let ev = |kind, state, receipt| {
        Some(WireEvent {
            kind,
            state,
            receipt,
        })
    };
    match kind {
        "attempt_accepted" => ev(
            EventKind::ReceiptReached,
            None,
            Some(Receipt::WorkerAccepted),
        ),
        "attempt_state" => {
            let to: AttemptState = serde_json::from_value(payload.get("to")?.clone()).ok()?;
            ev(EventKind::StateChanged, Some(to), None)
        }
        "attempt_terminal" => ev(
            EventKind::StateChanged,
            Some(AttemptState::Terminal),
            Some(Receipt::Terminal),
        ),
        "log_truncated" => ev(EventKind::LogTruncated, None, None),
        "supervisor_stop" if payload.get("reason")?.as_str()? == "LeaseExpired" => {
            ev(EventKind::LeaseExpired, None, None)
        }
        _ => None,
    }
}

/// The wire `seq` of the events of one attempt: 1, 2, 3, ... over the journal
/// entries that are wire events, so a reader sees no holes where a
/// diagnostic entry was. Returns the `(seq, index into kinds)` to send for
/// `after_seq`, at most `limit`.
pub fn select_events(is_wire: &[bool], after_seq: u64, limit: u32) -> Vec<(u64, usize)> {
    let limit = limit.clamp(1, MAX_EVENTS) as usize;
    is_wire
        .iter()
        .enumerate()
        .filter(|(_, w)| **w)
        .zip(1u64..)
        .filter(|(_, seq)| *seq > after_seq)
        .take(limit)
        .map(|((i, _), seq)| (seq, i))
        .collect()
}

/// `inspect`: attempts whose command may be running, and attempts accepted
/// but not started.
pub fn load_counts(states: impl IntoIterator<Item = AttemptState>) -> (u32, u32) {
    let (mut active, mut pending) = (0, 0);
    for s in states {
        match s {
            AttemptState::Accepted | AttemptState::Preparing | AttemptState::Ready => pending += 1,
            AttemptState::Terminal => {}
            _ => active += 1,
        }
    }
    (active, pending)
}

/// One step of the agent's startup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    /// `<state dir>` exists with mode 0700.
    StateDir,
    /// No core file can hold a secret.
    CoreDumps,
    /// One agent per state directory.
    Lock,
    /// The database is open and the agent epoch advanced.
    Store,
    /// Plaintext secret bundles a crashed capture left in the store.
    StoreSecretCopies,
    /// Stale files of the bundle vault.
    Vault,
    /// Secret files of attempts that ended or are unknown.
    PlacedSecrets,
    /// Workspaces and scratch directories no attempt owns.
    Workspaces,
    /// Adopt, recover, interrupt or resume each attempt.
    Attempts,
    /// Remember this boot.
    BootRecord,
}

/// The order is part of the contract: core dumps are off before any secret
/// is read; every sweep runs before an attempt is resumed (a new workspace's
/// scratch directory would look like a leftover); the boot ID is recorded
/// last, so a start that died half-way sees the change again.
pub const STARTUP_ORDER: [Step; 10] = [
    Step::StateDir,
    Step::CoreDumps,
    Step::Lock,
    Step::Store,
    Step::StoreSecretCopies,
    Step::Vault,
    Step::PlacedSecrets,
    Step::Workspaces,
    Step::Attempts,
    Step::BootRecord,
];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::*;
    use crate::wire::CommandSpec;
    use serde_json::json;
    use std::collections::BTreeMap;

    fn cmd(node: &str) -> CommandSpec {
        CommandSpec {
            node_id: NodeId::parse(node).unwrap(),
            argv: vec!["true".into()],
            cwd: String::new(),
            env: BTreeMap::new(),
            depends_on: vec![],
            timeout_ms: None,
        }
    }

    fn payload() -> AttemptPayload {
        let d = Digest::of_bytes(b"x");
        AttemptPayload {
            run_id: RunId::generate(),
            task_id: TaskId::generate(),
            generation: Generation(1),
            source_id: SourceId::generate(),
            plan_digest: d.clone(),
            manifest_digest: d,
            permit_id: None,
            commands: vec![cmd("a")],
            lease_ms: 60_000,
            command_timeout_ms: None,
            deadline_ms: None,
            resource_wait_timeout_ms: None,
            has_secret_bundle: false,
        }
    }

    #[test]
    fn a_plain_payload_passes_and_each_fault_is_named() {
        assert_eq!(check_payload(&payload()), Ok(()));
        let bad = |f: &dyn Fn(&mut AttemptPayload)| {
            let mut p = payload();
            f(&mut p);
            check_payload(&p).unwrap_err()
        };
        assert_eq!(bad(&|p| p.commands.clear()), "the attempt has no command");
        assert_eq!(
            bad(&|p| p.commands = (0..=MAX_COMMANDS).map(|i| cmd(&format!("n{i}"))).collect()),
            "the attempt has too many commands"
        );
        assert_eq!(bad(&|p| p.lease_ms = 0), "lease_ms must be positive");
        for f in [
            (&|p: &mut AttemptPayload| p.command_timeout_ms = Some(0)) as &dyn Fn(&mut _),
            &|p: &mut AttemptPayload| p.deadline_ms = Some(0),
            &|p: &mut AttemptPayload| p.resource_wait_timeout_ms = Some(0),
            &|p: &mut AttemptPayload| p.commands[0].timeout_ms = Some(0),
        ] {
            assert_eq!(bad(f), "a timeout of zero is not a timeout");
        }
        assert_eq!(bad(&|p| p.commands[0].argv.clear()), "a command is empty");
        assert_eq!(
            bad(&|p| p.commands[0].argv[0].clear()),
            "a command is empty"
        );
        assert_eq!(
            bad(&|p| p.commands.push(cmd("a"))),
            "a node_id is used twice"
        );
    }

    #[test]
    fn the_size_limit_counts_argv_cwd_and_env_and_is_inclusive() {
        let sized = |argv: usize, cwd: usize, key: usize, val: usize| {
            let mut p = payload();
            p.commands[0].argv = vec!["x".repeat(argv)];
            p.commands[0].cwd = "c".repeat(cwd);
            p.commands[0].env.insert("k".repeat(key), "v".repeat(val));
            check_payload(&p)
        };
        assert_eq!(sized(MAX_COMMAND_BYTES - 3, 1, 1, 1), Ok(()));
        for (a, c, k, v) in [(2, 1, 1, 1), (1, 2, 1, 1), (1, 1, 2, 1), (1, 1, 1, 2)] {
            assert_eq!(
                sized(MAX_COMMAND_BYTES - 4 + a, c, k, v),
                Err("the commands are too large")
            );
        }
        // Every command counts, not only the last one.
        let mut p = payload();
        p.commands.push(cmd("b"));
        p.commands[0].argv = vec!["x".repeat(MAX_COMMAND_BYTES)];
        assert_eq!(check_payload(&p), Err("the commands are too large"));
    }

    #[test]
    fn only_a_recorded_and_different_boot_id_is_a_reboot() {
        assert!(!boot_changed(None, "a"));
        assert!(!boot_changed(Some("a"), "a"));
        assert!(boot_changed(Some("a"), "b"));
    }

    fn facts(state: AttemptState) -> AttemptFacts {
        AttemptFacts {
            state,
            has_launches: false,
            stop_requested: false,
            has_spec: true,
        }
    }

    #[test]
    fn an_attempt_that_may_have_started_is_never_resumed() {
        use AttemptState::*;
        for boot in [false, true] {
            for state in AttemptState::ALL {
                if state == Terminal {
                    continue;
                }
                let launched = AttemptFacts {
                    has_launches: true,
                    ..facts(state)
                };
                assert_eq!(decide_recovery(boot, launched), Recovery::Supervise);
                let no_spec = AttemptFacts {
                    has_spec: false,
                    ..launched
                };
                assert_eq!(decide_recovery(boot, no_spec), Recovery::Unresolved);
            }
            for state in [Starting, Active] {
                assert_eq!(decide_recovery(boot, facts(state)), Recovery::Supervise);
                let no_spec = AttemptFacts {
                    has_spec: false,
                    ..facts(state)
                };
                assert_eq!(decide_recovery(boot, no_spec), Recovery::Unresolved);
            }
        }
    }

    #[test]
    fn an_unstarted_attempt_resumes_unless_stopped_or_rebooted() {
        use AttemptState::*;
        for state in [Accepted, Preparing, Ready] {
            assert_eq!(decide_recovery(false, facts(state)), Recovery::Resume);
            assert_eq!(
                decide_recovery(true, facts(state)),
                Recovery::Settle(Settle::Interrupted)
            );
            let stopped = AttemptFacts {
                stop_requested: true,
                ..facts(state)
            };
            for boot in [false, true] {
                assert_eq!(
                    decide_recovery(boot, stopped),
                    Recovery::Settle(Settle::Cancelled)
                );
            }
            // No spec file is needed for something that never started.
            let no_spec = AttemptFacts {
                has_spec: false,
                ..facts(state)
            };
            assert_eq!(decide_recovery(false, no_spec), Recovery::Resume);
        }
        for state in [Cancelling, Finalizing] {
            assert_eq!(
                decide_recovery(false, facts(state)),
                Recovery::Settle(Settle::Interrupted)
            );
        }
        for boot in [false, true] {
            let t = AttemptFacts {
                has_launches: true,
                stop_requested: true,
                ..facts(Terminal)
            };
            assert_eq!(decide_recovery(boot, t), Recovery::Nothing);
        }
    }

    #[test]
    fn retire_answers_what_is_known_about_the_stop() {
        use RetireStatus::*;
        assert_eq!(retire_status(false, true, true, true), NotStarted);
        assert_eq!(retire_status(true, true, true, true), AlreadyTerminal);
        assert_eq!(retire_status(true, false, false, false), Unconfirmed);
        assert_eq!(retire_status(true, false, false, true), Unconfirmed);
        assert_eq!(retire_status(true, false, true, true), Stopped);
        assert_eq!(retire_status(true, false, true, false), NotStarted);
    }

    #[test]
    fn cancel_is_completed_only_at_terminal() {
        assert_eq!(cancel_phase(None), CancelPhase::Accepted);
        for s in AttemptState::ALL {
            let want = if s == AttemptState::Terminal {
                CancelPhase::Completed
            } else {
                CancelPhase::Accepted
            };
            assert_eq!(cancel_phase(Some(s)), want);
        }
    }

    #[test]
    fn journal_entries_map_to_wire_events() {
        let e = wire_event("attempt_accepted", &Value::Null).unwrap();
        assert_eq!(
            (e.kind, e.state, e.receipt),
            (
                EventKind::ReceiptReached,
                None,
                Some(Receipt::WorkerAccepted)
            )
        );
        let e = wire_event("attempt_state", &json!({"from": "ready", "to": "starting"})).unwrap();
        assert_eq!(
            (e.kind, e.state, e.receipt),
            (EventKind::StateChanged, Some(AttemptState::Starting), None)
        );
        assert_eq!(wire_event("attempt_state", &json!({})), None);
        assert_eq!(wire_event("attempt_state", &json!({"to": "nowhere"})), None);
        let e = wire_event("attempt_terminal", &Value::Null).unwrap();
        assert_eq!(
            (e.kind, e.state, e.receipt),
            (
                EventKind::StateChanged,
                Some(AttemptState::Terminal),
                Some(Receipt::Terminal)
            )
        );
        let e = wire_event("log_truncated", &Value::Null).unwrap();
        assert_eq!(
            (e.kind, e.state, e.receipt),
            (EventKind::LogTruncated, None, None)
        );
        let e = wire_event("supervisor_stop", &json!({"reason": "LeaseExpired"})).unwrap();
        assert_eq!(
            (e.kind, e.state, e.receipt),
            (EventKind::LeaseExpired, None, None)
        );
        assert_eq!(
            wire_event("supervisor_stop", &json!({"reason": "Cancel"})),
            None
        );
        assert_eq!(wire_event("supervisor_stop", &json!({"reason": 1})), None);
        assert_eq!(wire_event("supervisor_stop", &json!({})), None);
        assert_eq!(wire_event("sleep_inhibit", &Value::Null), None);
    }

    #[test]
    fn wire_seq_has_no_holes_and_pages_by_after_seq() {
        let w = [true, false, true, true, false, true];
        assert_eq!(
            select_events(&w, 0, 10),
            vec![(1, 0), (2, 2), (3, 3), (4, 5)]
        );
        assert_eq!(select_events(&w, 2, 10), vec![(3, 3), (4, 5)]);
        assert_eq!(select_events(&w, 1, 2), vec![(2, 2), (3, 3)]);
        assert_eq!(select_events(&w, 4, 10), vec![]);
        // A limit of zero still makes progress; a huge one is capped.
        assert_eq!(select_events(&w, 0, 0), vec![(1, 0)]);
        let many = vec![true; MAX_EVENTS as usize + 5];
        assert_eq!(select_events(&many, 0, u32::MAX).len(), MAX_EVENTS as usize);
        assert_eq!(
            select_events(&many, 0, MAX_EVENTS).len(),
            MAX_EVENTS as usize
        );
    }

    #[test]
    fn load_counts_split_started_from_waiting() {
        use AttemptState::*;
        assert_eq!(load_counts(AttemptState::ALL), (4, 3));
        assert_eq!(load_counts([Terminal, Terminal]), (0, 0));
        assert_eq!(load_counts([Active, Ready, Ready]), (1, 2));
    }

    #[test]
    fn startup_order_keeps_its_invariants() {
        let at = |s: Step| STARTUP_ORDER.iter().position(|x| *x == s).unwrap();
        for sweep in [
            Step::StoreSecretCopies,
            Step::Vault,
            Step::PlacedSecrets,
            Step::Workspaces,
        ] {
            assert!(at(Step::CoreDumps) < at(sweep));
            assert!(at(Step::Store) < at(sweep));
            assert!(at(sweep) < at(Step::Attempts));
        }
        assert_eq!(at(Step::StateDir), 0);
        assert!(at(Step::Lock) < at(Step::Store));
        assert_eq!(at(Step::BootRecord), STARTUP_ORDER.len() - 1);
    }
}
