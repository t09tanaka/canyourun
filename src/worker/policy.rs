//! The worker agent's decisions, free of I/O (spec chapters 11 and 12).
//!
//! What a submit may carry, what a restarted agent does with each attempt it
//! finds, how a retire and a cancel are answered, which journal entries are
//! wire events, and the order of the startup steps.

use crate::source::safety::fold_path;
use crate::state::{AttemptState, Observation, Receipt};
use crate::supervisor::spool_plan::{
    check_artifact_names, relative_path, ArtifactSetError, MAX_ARTIFACTS,
};
use crate::wire::{
    ArtifactDecl, AttemptPayload, CancelPhase, CommandSpec, EventKind, RetireStatus,
};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

/// Commands in one attempt.
pub const MAX_COMMANDS: usize = 256;
/// Bytes of all argv, cwd and env strings of one attempt.
pub const MAX_COMMAND_BYTES: usize = 1_048_576;
/// Bytes of one declared artifact's name. The count of declarations is
/// the supervisor's [`MAX_ARTIFACTS`] (1024).
pub const MAX_ARTIFACT_NAME_BYTES: usize = 255;
/// Bytes of one declared artifact's path in the workspace.
pub const MAX_ARTIFACT_PATH_BYTES: usize = 1024;
/// Attempts in one `list_attempts` answer.
pub const MAX_LIST_ATTEMPTS: u32 = 500;
/// Events in one `watch_events` answer.
pub const MAX_EVENTS: u32 = 500;
/// Journal entries of one attempt a `watch_events` reads.
pub const MAX_EVENT_SCAN: u32 = 100_000;
/// Digests in one `blob_verify`.
pub const MAX_VERIFY_DIGESTS: usize = 4096;
/// The longest execution lease a submit may ask for: 24 hours.
pub const MAX_LEASE_MS: u64 = 86_400_000;
/// The longest deadline or timeout a submit may ask for: 7 days.
pub const MAX_TIMEOUT_MS: u64 = 604_800_000;
/// Attempts that are not terminal on one worker, unless configured.
pub const DEFAULT_MAX_LIVE_ATTEMPTS: u32 = 32;
/// Workspaces prepared at once, unless configured.
pub const DEFAULT_MAX_PREPARES: u32 = 4;
/// Supervisor starts that may fail in a row before the agent gives up.
pub const DEFAULT_MAX_SPAWN_FAILURES: u32 = 5;

/// A string that becomes part of a process's argv, environment or path.
fn has_nul(s: &str) -> bool {
    s.contains('\0')
}

/// Why a payload is refused before anything is stored. The sender's numbers
/// are not trusted: the checks run on what actually arrived. `poll_ms` is how
/// often this worker's supervisors look at the lease; a lease that is not
/// longer could never be seen valid.
pub fn check_payload(p: &AttemptPayload, poll_ms: u64) -> Result<(), &'static str> {
    if p.commands.is_empty() {
        return Err("the attempt has no command");
    }
    if p.commands.len() > MAX_COMMANDS {
        return Err("the attempt has too many commands");
    }
    if p.lease_ms == 0 {
        return Err("lease_ms must be positive");
    }
    if p.lease_ms <= poll_ms {
        return Err("lease_ms must be longer than the worker's poll interval");
    }
    if p.lease_ms > MAX_LEASE_MS {
        return Err("lease_ms is longer than 24 hours");
    }
    for t in [
        p.command_timeout_ms,
        p.deadline_ms,
        p.resource_wait_timeout_ms,
    ] {
        if t == Some(0) {
            return Err("a timeout of zero is not a timeout");
        }
        if t.is_some_and(|t| t > MAX_TIMEOUT_MS) {
            return Err("a timeout is longer than 7 days");
        }
    }
    let mut seen = BTreeSet::new();
    let mut bytes = 0usize;
    for c in &p.commands {
        // Before anything is done per name. No command has more commands to
        // wait for than the attempt may have; more names are repeats.
        if c.depends_on.len() > MAX_COMMANDS {
            return Err("a command names too many dependencies");
        }
        if c.argv.first().is_none_or(|a| a.is_empty()) {
            return Err("a command is empty");
        }
        // A command's own limit replaces the default while it runs.
        if c.timeout_ms == Some(0) {
            return Err("a timeout of zero is not a timeout");
        }
        if c.timeout_ms.is_some_and(|t| t > MAX_TIMEOUT_MS) {
            return Err("a timeout is longer than 7 days");
        }
        if !seen.insert(c.node_id.as_str()) {
            return Err("a node_id is used twice");
        }
        if has_nul(&c.cwd) || c.argv.iter().any(|a| has_nul(a)) {
            return Err("a command or its directory contains a NUL byte");
        }
        for (k, v) in &c.env {
            if k.is_empty() || k.contains('=') {
                return Err("an environment name is empty or contains '='");
            }
            if has_nul(k) || has_nul(v) {
                return Err("an environment entry contains a NUL byte");
            }
        }
        bytes += c.cwd.len();
        bytes += c.argv.iter().map(String::len).sum::<usize>();
        bytes += c.env.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>();
        bytes += c.depends_on.iter().map(|d| d.as_str().len()).sum::<usize>();
    }
    if bytes > MAX_COMMAND_BYTES {
        return Err("the commands are too large");
    }
    check_artifacts(&p.artifacts)?;
    execution_order(&p.commands).map(|_| ())
}

/// Whether every component of `path` is a name: none is empty (so the path
/// is not empty and has no leading, trailing or doubled `/`), none is `.`,
/// and there is no control character (NUL included). `..` is the caller's
/// to refuse, with its own message.
fn plain_components(path: &str) -> bool {
    path.split('/').all(|c| !c.is_empty() && c != ".") && !path.chars().any(char::is_control)
}

/// Why the artifacts a payload declares are refused, from the declarations
/// alone: how many, each name and each path, and that no two share a name
/// or a path. Names are held to what the supervisor's store accepts
/// ([`relative_path`], [`check_artifact_names`]), so what passes here is not
/// refused later when the supervisor's spec is written. Names and paths are
/// compared folded (NFC, lowercase): two that a case-folding file system
/// would store as one are one.
pub fn check_artifacts(decls: &[ArtifactDecl]) -> Result<(), &'static str> {
    if decls.len() > MAX_ARTIFACTS {
        return Err("the attempt declares too many artifacts");
    }
    let mut paths = BTreeSet::new();
    for d in decls {
        if d.name.is_empty() {
            return Err("an artifact has no name");
        }
        if d.name.len() > MAX_ARTIFACT_NAME_BYTES {
            return Err("an artifact name is too long");
        }
        if relative_path(&d.name).is_none() {
            return Err("an artifact name is not a plain relative path");
        }
        if d.path.len() > MAX_ARTIFACT_PATH_BYTES {
            return Err("an artifact path is too long");
        }
        if d.path.starts_with('/') {
            return Err("an artifact path is absolute");
        }
        if d.path.split('/').any(|c| c == "..") {
            return Err("an artifact path has a '..' component");
        }
        if !plain_components(&d.path) {
            return Err("an artifact path is not a plain relative path");
        }
        if !paths.insert(fold_path(d.path.as_bytes())) {
            return Err("an artifact path is declared twice");
        }
    }
    let names: Vec<&str> = decls.iter().map(|d| d.name.as_str()).collect();
    match check_artifact_names(&names) {
        Ok(()) => Ok(()),
        Err(ArtifactSetError::Duplicate(_)) => Err("an artifact name is declared twice"),
        Err(ArtifactSetError::Nested(_)) => Err("an artifact name lies below another artifact"),
    }
}

/// Whether a declared artifact is one of the attempt's secret files
/// (`secret_paths`: the secret files of its source manifest, which hold the
/// declared ones and what `.worktreeinclude` matched; relative to the
/// workspace root like the declarations). Compared folded, like the
/// declarations among themselves. Such a declaration is refused.
///
/// This is the early answer to the plain mistake, and not the guarantee: the
/// folding (NFC and lower case) is not the file system's, so a name the file
/// system opens as the secret file can pass here (`ſ` for `s`, `ß` for `ss`),
/// and a command can link or copy the file to any name. What is guaranteed
/// is decided when the file is stored, by the supervisor
/// ([`crate::supervisor::outcome::store_artifact`]): the secret file itself
/// under any name (device and inode of the opened file), and a file holding
/// one of the masked values, are not kept. A transformed copy, or a copy of
/// a secret file without masked values, is not recognised.
pub fn artifact_is_secret<S: AsRef<str>>(decls: &[ArtifactDecl], secret_paths: &[S]) -> bool {
    let secrets: BTreeSet<Vec<u8>> = secret_paths
        .iter()
        .map(|s| fold_path(s.as_ref().as_bytes()))
        .collect();
    decls
        .iter()
        .any(|d| secrets.contains(&fold_path(d.path.as_bytes())))
}

/// The order the commands of an attempt run in, as positions in `commands`:
/// every command after all the commands its `depends_on` names, wherever
/// they stand in the list (a later one may be named). Among the commands
/// that may run next, the one given first runs first, so a list that is
/// already in order runs as given and the same list always gives the same
/// order. The supervisor runs them one after another and stops at the first
/// that does not succeed, so a command never runs after a dependency that
/// failed.
///
/// Refused: a name that is no command of the attempt, and a cycle (a command
/// naming itself included).
pub fn execution_order(commands: &[CommandSpec]) -> Result<Vec<usize>, &'static str> {
    let index: BTreeMap<&str, usize> = commands
        .iter()
        .enumerate()
        .map(|(i, c)| (c.node_id.as_str(), i))
        .collect();
    // Per command: how many commands it still waits for, and who waits for
    // it. A name given more than once is one dependency (the set), so the
    // work is bounded by the names read once, not by commands times names.
    let mut waits = vec![0usize; commands.len()];
    let mut unblocks: Vec<Vec<usize>> = vec![Vec::new(); commands.len()];
    for (i, c) in commands.iter().enumerate() {
        let on: BTreeSet<usize> = c
            .depends_on
            .iter()
            .map(|d| index.get(d.as_str()).copied())
            .collect::<Option<_>>()
            .ok_or("depends_on names no command of the attempt")?;
        waits[i] = on.len();
        for d in on {
            unblocks[d].push(i);
        }
    }
    // Kahn's algorithm; the set gives the first of the list among the ready.
    let mut ready: BTreeSet<usize> = (0..commands.len()).filter(|&i| waits[i] == 0).collect();
    let mut order = Vec::with_capacity(commands.len());
    while let Some(i) = ready.pop_first() {
        order.push(i);
        for &next in &unblocks[i] {
            waits[next] -= 1;
            if waits[next] == 0 {
                ready.insert(next);
            }
        }
    }
    if order.len() < commands.len() {
        return Err("depends_on forms a cycle");
    }
    Ok(order)
}

/// The secret values as the strings the supervisor's spec carries. A value
/// that is not UTF-8 text cannot be written there, and a changed copy of it
/// would match nothing in the output: the attempt is not started rather than
/// run with a secret that is not masked.
pub fn mask_strings(values: Vec<Vec<u8>>) -> Result<Vec<String>, &'static str> {
    values
        .into_iter()
        .map(|v| String::from_utf8(v).map_err(|_| "a secret value is not UTF-8 text"))
        .collect()
}

/// Whether the machine was restarted since the agent last ran. A first start
/// (nothing recorded, nothing in the database) is not a restart; a missing
/// record next to attempts that are not terminal or to launch intents is: the
/// record was lost, and "same boot" cannot be shown.
pub fn boot_changed(recorded: Option<&str>, now: &str, has_history: bool) -> bool {
    match recorded {
        Some(r) => r != now,
        None => has_history,
    }
}

/// What is left of a period of `total_ms` that began `elapsed_ms` ago;
/// `None` when it ran out.
pub fn remaining_ms(total_ms: u64, elapsed_ms: u64) -> Option<u64> {
    total_ms.checked_sub(elapsed_ms).filter(|left| *left > 0)
}

/// Whether an answer of `found` entries fits a cap. One that does not is an
/// error, never a part of the list.
pub fn fits(found: usize, cap: u32) -> bool {
    found <= cap as usize
}

/// Whether a new attempt is refused because `live` attempts are not terminal.
pub fn at_live_cap(live: u64, cap: u32) -> bool {
    live >= u64::from(cap)
}

/// Whether the agent stops starting a supervisor after `failures` in a row.
pub fn gives_up(failures: u32, cap: u32) -> bool {
    failures >= cap
}

/// The pause before the next try after `failures` failed ones: `base_ms`,
/// doubled each time, at most 32 times `base_ms`.
pub fn backoff_ms(base_ms: u64, failures: u32) -> u64 {
    base_ms.saturating_mul(1 << failures.min(5))
}

/// What the exit of a supervisor means to the agent that started it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SupervisorExit {
    /// The attempt is settled, or another supervisor holds it.
    Done,
    /// The attempt is unresolved only because whose a recorded process is
    /// could not be told. Not a failure: it is looked at again, for as long
    /// as it takes, and never counted towards giving up.
    IdentityUnknown,
    /// The supervisor failed.
    Failed,
}

/// What a supervisor's exit `code` means (`None`: it was killed by a signal).
pub fn supervisor_exit(code: Option<i32>) -> SupervisorExit {
    match code {
        Some(0 | 4) => SupervisorExit::Done,
        Some(5) => SupervisorExit::IdentityUnknown,
        _ => SupervisorExit::Failed,
    }
}

/// Whether the pause after `tries` earlier ones is the first of the longest
/// ([`backoff_ms`] no longer grows from here): said once in the event log.
pub fn backoff_capped(tries: u32) -> bool {
    tries == 5
}

/// Whether a wait before the next look at an attempt ends early: a cancel
/// or a retire that was not there when the wait began is acted on at once.
pub fn wakes(cancel_then: bool, cancel_now: bool, retired_then: bool, retired_now: bool) -> bool {
    (cancel_now && !cancel_then) || (retired_now && !retired_then)
}

/// Whether every launch of an attempt was recorded by another boot of the
/// machine: nothing of it can still run. A launch without a recorded process
/// proves nothing.
pub fn launched_on_another_boot(boots: &[Option<&str>], now: &str) -> bool {
    !boots.is_empty() && boots.iter().all(|b| b.is_some_and(|b| b != now))
}

/// What the worker knows about an attempt: `Unknown` when a command may have
/// run and the agent can neither watch nor recover it.
pub fn observation(state: AttemptState, gave_up: bool, recovery: Recovery) -> Observation {
    if state != AttemptState::Terminal && (gave_up || recovery == Recovery::Unresolved) {
        Observation::Unknown
    } else {
        Observation::Known
    }
}

/// The answer to a `secret_bundle_put`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecretPut {
    /// The files go into the workspace now.
    Place,
    /// A cancel or a retire was persisted (`RETIRED`).
    Stopping,
    /// The commands may have started (`CONFLICT`): too late for good.
    TooLate,
    /// The workspace does not exist yet (`NOT_FOUND`): send it again.
    NotReady,
}

pub fn secret_put(state: AttemptState, stop_requested: bool, has_workspace: bool) -> SecretPut {
    if stop_requested {
        SecretPut::Stopping
    } else if !matches!(state, AttemptState::Accepted | AttemptState::Preparing) {
        SecretPut::TooLate
    } else if !has_workspace {
        SecretPut::NotReady
    } else {
        SecretPut::Place
    }
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
        "attempt_receipt" => {
            let r: Receipt = serde_json::from_value(payload.get("receipt")?.clone()).ok()?;
            // Only the two the journal records this way; the others have
            // their own entries.
            if !matches!(r, Receipt::CommandRegistered | Receipt::Running) {
                return None;
            }
            ev(EventKind::ReceiptReached, None, Some(r))
        }
        "lease_expired" => ev(EventKind::LeaseExpired, None, None),
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
            artifacts: Vec::new(),
        }
    }

    fn decl(name: &str, path: &str) -> ArtifactDecl {
        ArtifactDecl {
            name: name.into(),
            path: path.into(),
            if_missing: crate::wire::MissingPolicy::Required,
        }
    }

    #[test]
    fn the_caps_on_declared_artifacts_have_their_documented_values() {
        assert_eq!(MAX_ARTIFACTS, 1024);
        assert_eq!(MAX_ARTIFACT_NAME_BYTES, 255);
        assert_eq!(MAX_ARTIFACT_PATH_BYTES, 1024);
    }

    #[test]
    fn declared_artifacts_are_counted_and_each_name_and_path_is_checked() {
        assert_eq!(check_artifacts(&[]), Ok(()));
        let ok = [
            decl("report.xml", "out/report.xml"),
            decl("logs/a.txt", "target/.tmp/deep/a.txt"),
            decl("c", ".hidden"),
        ];
        assert_eq!(check_artifacts(&ok), Ok(()));
        let one = |name: &str, path: &str| check_artifacts(&[decl(name, path)]);

        // Count: exactly the cap passes, one more does not.
        let many = |n: usize| -> Vec<ArtifactDecl> {
            (0..n)
                .map(|i| decl(&format!("n{i}"), &format!("p{i}")))
                .collect()
        };
        assert_eq!(check_artifacts(&many(1024)), Ok(()));
        assert_eq!(
            check_artifacts(&many(1025)),
            Err("the attempt declares too many artifacts")
        );

        // Names.
        assert_eq!(one("", "a"), Err("an artifact has no name"));
        assert_eq!(one(&"n".repeat(255), "a"), Ok(()));
        assert_eq!(
            one(&"n".repeat(256), "a"),
            Err("an artifact name is too long")
        );
        for name in [
            "/abs", "../up", "a/../b", "a//b", "a/", ".", "a\0b", ".tmpx", "a\\b",
        ] {
            assert_eq!(
                one(name, "a"),
                Err("an artifact name is not a plain relative path"),
                "{name:?}"
            );
        }
        // Paths: the length first, at exactly the cap and one over.
        let long = |n: usize| format!("{}/{}", "d".repeat(n - 2), "f");
        assert_eq!(long(1024).len(), 1024);
        assert_eq!(one("a", &long(1024)), Ok(()));
        assert_eq!(one("a", &long(1025)), Err("an artifact path is too long"));
        assert_eq!(one("a", "/etc/passwd"), Err("an artifact path is absolute"));
        assert_eq!(one("a", "/"), Err("an artifact path is absolute"));
        for path in ["..", "../a", "a/..", "a/../b"] {
            assert_eq!(
                one("a", path),
                Err("an artifact path has a '..' component"),
                "{path:?}"
            );
        }
        // Not a component: a name that only holds two dots.
        assert_eq!(one("a", "a..b/..c"), Ok(()));
        for path in ["", ".", "a/./b", "a//b", "a/", "a\0b", "a\nb"] {
            assert_eq!(
                one("a", path),
                Err("an artifact path is not a plain relative path"),
                "{path:?}"
            );
        }
    }

    #[test]
    fn two_declared_artifacts_share_neither_a_name_nor_a_path() {
        let two =
            |a: (&str, &str), b: (&str, &str)| check_artifacts(&[decl(a.0, a.1), decl(b.0, b.1)]);
        assert_eq!(two(("a", "x"), ("b", "y")), Ok(()));
        assert_eq!(
            two(("a", "x"), ("a", "y")),
            Err("an artifact name is declared twice")
        );
        // What a case-folding file system stores as one file is one name.
        assert_eq!(
            two(("Report", "x"), ("report", "y")),
            Err("an artifact name is declared twice")
        );
        assert_eq!(
            two(("a", "x"), ("a/b", "y")),
            Err("an artifact name lies below another artifact")
        );
        assert_eq!(
            two(("a", "out/x"), ("b", "out/x")),
            Err("an artifact path is declared twice")
        );
        assert_eq!(
            two(("a", "out/x"), ("b", "OUT/X")),
            Err("an artifact path is declared twice")
        );
        // A name may equal another artifact's path: they are different things.
        assert_eq!(two(("a", "b"), ("b", "a")), Ok(()));
    }

    #[test]
    fn a_payload_with_a_refused_declaration_is_refused() {
        let mut p = payload();
        p.artifacts = vec![decl("a", "out/a")];
        assert_eq!(check_payload(&p, 0), Ok(()));
        p.artifacts = vec![decl("a", "../a")];
        assert_eq!(
            check_payload(&p, 0),
            Err("an artifact path has a '..' component")
        );
    }

    #[test]
    fn a_declared_artifact_that_is_a_secret_file_is_found() {
        let decls = [decl("a", "out/a"), decl("env", "config/.env")];
        let none: [&str; 0] = [];
        assert!(!artifact_is_secret(&decls, &none));
        assert!(!artifact_is_secret(&decls, &[".env", "out/b"]));
        assert!(artifact_is_secret(&decls, &["config/.env"]));
        assert!(artifact_is_secret(&decls, &["other", "out/a"]));
        // Folded: the same file on a case-folding file system.
        assert!(artifact_is_secret(&decls, &["Config/.ENV".to_string()]));
        assert!(!artifact_is_secret(&[], &["config/.env"]));
        // The name is not a path in the workspace.
        assert!(!artifact_is_secret(&[decl(".env", "out/a")], &[".env"]));
    }

    #[test]
    fn a_plain_payload_passes_and_each_fault_is_named() {
        assert_eq!(check_payload(&payload(), 0), Ok(()));
        let bad = |f: &dyn Fn(&mut AttemptPayload)| {
            let mut p = payload();
            f(&mut p);
            check_payload(&p, 0).unwrap_err()
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
        ] {
            assert_eq!(bad(f), "a timeout of zero is not a timeout");
        }
        // Exactly the limit is allowed.
        let mut full = payload();
        full.commands = (0..MAX_COMMANDS).map(|i| cmd(&format!("n{i}"))).collect();
        assert_eq!(check_payload(&full, 0), Ok(()));
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
    fn a_command_may_have_its_own_timeout_within_the_same_bounds() {
        let mut p = payload();
        // Its own limit, with or without a default, shorter or longer.
        p.commands[0].timeout_ms = Some(5);
        assert_eq!(check_payload(&p, 0), Ok(()));
        p.command_timeout_ms = Some(6);
        assert_eq!(check_payload(&p, 0), Ok(()));
        p.command_timeout_ms = Some(4);
        assert_eq!(check_payload(&p, 0), Ok(()));
        p.commands[0].timeout_ms = Some(0);
        assert_eq!(
            check_payload(&p, 0),
            Err("a timeout of zero is not a timeout")
        );
        p.commands[0].timeout_ms = Some(MAX_TIMEOUT_MS);
        assert_eq!(check_payload(&p, 0), Ok(()));
        p.commands[0].timeout_ms = Some(MAX_TIMEOUT_MS + 1);
        assert_eq!(check_payload(&p, 0), Err("a timeout is longer than 7 days"));
    }

    fn deps(spec: &[(&str, &[&str])]) -> Vec<CommandSpec> {
        spec.iter()
            .map(|(id, on)| {
                let mut c = cmd(id);
                c.depends_on = on.iter().map(|d| NodeId::parse(d).unwrap()).collect();
                c
            })
            .collect()
    }

    #[test]
    fn commands_run_after_what_they_depend_on_wherever_it_stands() {
        let order = |spec: &[(&str, &[&str])]| execution_order(&deps(spec));
        // Nothing named, or only earlier ones: as given.
        assert_eq!(
            order(&[("a", &[]), ("b", &[]), ("c", &[])]),
            Ok(vec![0, 1, 2])
        );
        assert_eq!(
            order(&[("a", &[]), ("b", &["a"]), ("c", &["a", "b"])]),
            Ok(vec![0, 1, 2])
        );
        // A later one named: it runs first, the rest keeps its order.
        assert_eq!(
            order(&[("a", &["c"]), ("b", &[]), ("c", &[])]),
            Ok(vec![1, 2, 0])
        );
        assert_eq!(
            order(&[("a", &["b"]), ("b", &["c"]), ("c", &[])]),
            Ok(vec![2, 1, 0])
        );
        // Every one of several: `a` waits for both.
        assert_eq!(
            order(&[("a", &["b", "d"]), ("b", &[]), ("c", &[]), ("d", &[])]),
            Ok(vec![1, 2, 3, 0])
        );
        // The same name twice is one dependency.
        assert_eq!(order(&[("a", &["b", "b"]), ("b", &[])]), Ok(vec![1, 0]));
        assert_eq!(order(&[]), Ok(vec![]));
        // Of the commands that may run next, the first of the list: `b` is
        // freed by `a` and runs before `c`, which was free all along.
        assert_eq!(
            order(&[("a", &[]), ("b", &["a"]), ("c", &[])]),
            Ok(vec![0, 1, 2])
        );
        assert_eq!(
            order(&[("a", &["d"]), ("b", &["d"]), ("c", &["a"]), ("d", &[])]),
            Ok(vec![3, 0, 1, 2])
        );
    }

    /// The definition, word for word: again and again the first command of
    /// the list whose dependencies have all run.
    fn order_by_definition(commands: &[CommandSpec]) -> Result<Vec<usize>, &'static str> {
        let mut done: Vec<&str> = Vec::new();
        let mut order: Vec<usize> = Vec::new();
        while order.len() < commands.len() {
            let next = commands.iter().enumerate().position(|(i, c)| {
                !order.contains(&i) && c.depends_on.iter().all(|d| done.contains(&d.as_str()))
            });
            let i = next.ok_or("depends_on forms a cycle")?;
            done.push(commands[i].node_id.as_str());
            order.push(i);
        }
        Ok(order)
    }

    #[test]
    fn every_small_graph_is_ordered_as_the_definition_says() {
        // Four commands, each naming any subset of the four (itself too):
        // all 65536 graphs, cycles included.
        let names = ["a", "b", "c", "d"];
        let (mut orders, mut cycles) = (0, 0);
        for graph in 0u32..65_536 {
            let commands: Vec<CommandSpec> = (0..4)
                .map(|i| {
                    let mut c = cmd(names[i]);
                    c.depends_on = (0..4)
                        .filter(|d| graph >> (i * 4 + d) & 1 == 1)
                        .map(|d| NodeId::parse(names[d]).unwrap())
                        .collect();
                    c
                })
                .collect();
            let got = execution_order(&commands);
            assert_eq!(got, order_by_definition(&commands), "graph {graph:#x}");
            match got {
                Ok(_) => orders += 1,
                Err(_) => cycles += 1,
            }
        }
        // 543 of them have no cycle (the labelled acyclic graphs on four).
        assert_eq!((orders, cycles), (543, 64_993));
    }

    #[test]
    fn repeated_names_cost_one_look_each_and_too_many_are_refused() {
        // At the limit, all of them the same name: accepted, one dependency.
        let mut p = payload();
        p.commands = deps(&[("a", &[]), ("b", &[])]);
        p.commands[1].depends_on = vec![NodeId::parse("a").unwrap(); MAX_COMMANDS];
        assert_eq!(check_payload(&p, 0), Ok(()));
        assert_eq!(execution_order(&p.commands), Ok(vec![0, 1]));
        p.commands[1].depends_on.push(NodeId::parse("a").unwrap());
        assert_eq!(
            check_payload(&p, 0),
            Err("a command names too many dependencies")
        );
        // The count is refused before the names are looked at.
        p.commands[1].depends_on[0] = NodeId::parse("nowhere").unwrap();
        assert_eq!(
            check_payload(&p, 0),
            Err("a command names too many dependencies")
        );
        // Every command of a full attempt naming the first one as often as
        // allowed: 65 280 names, each read once.
        let mut full = payload();
        full.commands = (0..MAX_COMMANDS).map(|i| cmd(&format!("n{i}"))).collect();
        for c in &mut full.commands[1..] {
            c.depends_on = vec![NodeId::parse("n0").unwrap(); MAX_COMMANDS];
        }
        assert_eq!(check_payload(&full, 0), Ok(()));
        assert_eq!(
            execution_order(&full.commands),
            Ok((0..MAX_COMMANDS).collect())
        );
    }

    #[test]
    fn unknown_names_and_cycles_are_refused() {
        let order = |spec: &[(&str, &[&str])]| execution_order(&deps(spec));
        let unknown = Err("depends_on names no command of the attempt");
        let cycle = Err("depends_on forms a cycle");
        assert_eq!(order(&[("a", &["nowhere"])]), unknown);
        assert_eq!(order(&[("a", &[]), ("b", &["a", "x"])]), unknown);
        assert_eq!(order(&[("a", &["a"])]), cycle);
        assert_eq!(order(&[("a", &["b"]), ("b", &["a"])]), cycle);
        assert_eq!(
            order(&[("a", &[]), ("b", &["d"]), ("c", &["b"]), ("d", &["c"])]),
            cycle
        );
        // The payload check refuses the same, and takes a forward reference.
        let with = |spec: &[(&str, &[&str])]| {
            let mut p = payload();
            p.commands = deps(spec);
            check_payload(&p, 0)
        };
        assert_eq!(with(&[("a", &["b"]), ("b", &[])]), Ok(()));
        assert_eq!(with(&[("a", &["nowhere"])]), unknown.clone().map(|_| ()));
        assert_eq!(
            with(&[("a", &["b"]), ("b", &["a"])]),
            cycle.clone().map(|_| ())
        );
    }

    #[test]
    fn the_names_in_depends_on_count_toward_the_size_of_the_commands() {
        let mut p = payload();
        p.commands.push(cmd("b"));
        p.commands[0].argv = vec!["x".repeat(MAX_COMMAND_BYTES - 1)];
        p.commands[1].argv = vec![String::new()];
        p.commands[1].argv[0].push('y');
        // One byte over with the name `a`.
        p.commands[1].depends_on = vec![NodeId::parse("a").unwrap()];
        assert_eq!(check_payload(&p, 0), Err("the commands are too large"));
        p.commands[1].depends_on.clear();
        assert_eq!(check_payload(&p, 0), Ok(()));
    }

    #[test]
    fn secret_values_become_strings_or_the_start_is_refused() {
        assert_eq!(mask_strings(vec![]), Ok(vec![]));
        assert_eq!(
            mask_strings(vec![
                b"hunter2-hunter2".to_vec(),
                "p\u{e4}ssw\u{f6}rd".as_bytes().to_vec()
            ]),
            Ok(vec![
                "hunter2-hunter2".to_string(),
                "p\u{e4}ssw\u{f6}rd".to_string()
            ])
        );
        assert_eq!(
            mask_strings(vec![b"fine-value".to_vec(), vec![0xff, 0xfe, 0x41]]),
            Err("a secret value is not UTF-8 text")
        );
    }

    #[test]
    fn the_size_limit_counts_argv_cwd_and_env_and_is_inclusive() {
        let sized = |argv: usize, cwd: usize, key: usize, val: usize| {
            let mut p = payload();
            p.commands[0].argv = vec!["x".repeat(argv)];
            p.commands[0].cwd = "c".repeat(cwd);
            p.commands[0].env.insert("k".repeat(key), "v".repeat(val));
            check_payload(&p, 0)
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
        assert_eq!(check_payload(&p, 0), Err("the commands are too large"));
    }

    #[test]
    fn a_different_or_a_lost_boot_record_is_a_reboot() {
        assert!(!boot_changed(None, "a", false));
        assert!(!boot_changed(Some("a"), "a", false));
        assert!(!boot_changed(Some("a"), "a", true));
        assert!(boot_changed(Some("a"), "b", false));
        assert!(boot_changed(Some("a"), "b", true));
        // No record, but attempts or launches: the record was lost.
        assert!(boot_changed(None, "a", true));
    }

    #[test]
    fn the_lease_and_the_timeouts_are_bounded() {
        let short = "lease_ms must be longer than the worker's poll interval";
        let mut p = payload();
        p.lease_ms = 200;
        assert_eq!(check_payload(&p, 200), Err(short));
        assert_eq!(check_payload(&p, 201), Err(short));
        assert_eq!(check_payload(&p, 199), Ok(()));
        p.lease_ms = MAX_LEASE_MS;
        assert_eq!(check_payload(&p, 0), Ok(()));
        p.lease_ms = MAX_LEASE_MS + 1;
        assert_eq!(
            check_payload(&p, 0),
            Err("lease_ms is longer than 24 hours")
        );
        assert_eq!(MAX_LEASE_MS, 86_400_000);
        assert_eq!(MAX_TIMEOUT_MS, 604_800_000);
        for which in 0..3 {
            let set = |v: Option<u64>| {
                let mut p = payload();
                match which {
                    0 => p.command_timeout_ms = v,
                    1 => p.deadline_ms = v,
                    _ => p.resource_wait_timeout_ms = v,
                }
                check_payload(&p, 0)
            };
            assert_eq!(set(Some(MAX_TIMEOUT_MS)), Ok(()));
            assert_eq!(set(Some(1)), Ok(()));
            assert_eq!(
                set(Some(MAX_TIMEOUT_MS + 1)),
                Err("a timeout is longer than 7 days")
            );
            assert_eq!(set(Some(0)), Err("a timeout of zero is not a timeout"));
        }
    }

    #[test]
    fn text_that_cannot_reach_a_process_is_refused() {
        let nul = Err("a command or its directory contains a NUL byte");
        let mut p = payload();
        p.commands[0].argv.push("a\0b".into());
        assert_eq!(check_payload(&p, 0), nul);
        let mut p = payload();
        p.commands[0].cwd = "sub\0".into();
        assert_eq!(check_payload(&p, 0), nul);
        let env = |k: &str, v: &str| {
            let mut p = payload();
            p.commands[0].env.insert(k.into(), v.into());
            check_payload(&p, 0)
        };
        let name = Err("an environment name is empty or contains '='");
        assert_eq!(env("", "v"), name);
        assert_eq!(env("A=B", "v"), name);
        let entry = Err("an environment entry contains a NUL byte");
        assert_eq!(env("A\0", "v"), entry);
        assert_eq!(env("A", "v\0"), entry);
        assert_eq!(env("A", ""), Ok(()));
        assert_eq!(env("A", "b=c"), Ok(()));
    }

    #[test]
    fn what_is_left_of_a_period_is_never_zero() {
        assert_eq!(remaining_ms(10, 0), Some(10));
        assert_eq!(remaining_ms(10, 3), Some(7));
        assert_eq!(remaining_ms(10, 9), Some(1));
        assert_eq!(remaining_ms(10, 10), None);
        assert_eq!(remaining_ms(10, 11), None);
    }

    #[test]
    fn caps_are_inclusive_where_they_say_so() {
        assert!(fits(0, 5) && fits(5, 5));
        assert!(!fits(6, 5));
        assert!(!at_live_cap(31, 32));
        assert!(at_live_cap(32, 32) && at_live_cap(33, 32));
        assert!(!gives_up(4, 5));
        assert!(gives_up(5, 5) && gives_up(6, 5));
        assert_eq!(DEFAULT_MAX_LIVE_ATTEMPTS, 32);
        assert_eq!(DEFAULT_MAX_PREPARES, 4);
        assert_eq!(DEFAULT_MAX_SPAWN_FAILURES, 5);
    }

    #[test]
    fn a_supervisor_exit_is_a_failure_unless_it_says_otherwise() {
        use crate::supervisor::process::code;
        assert_eq!(supervisor_exit(Some(code::SETTLED)), SupervisorExit::Done);
        assert_eq!(supervisor_exit(Some(code::ALIVE)), SupervisorExit::Done);
        assert_eq!(
            supervisor_exit(Some(code::IDENTITY_UNKNOWN)),
            SupervisorExit::IdentityUnknown
        );
        for failed in [
            Some(code::FAILED),
            Some(code::BAD_SPEC),
            Some(code::UNRESOLVED),
            Some(6),
            Some(-1),
            Some(69),
            Some(75),
            None,
        ] {
            assert_eq!(supervisor_exit(failed), SupervisorExit::Failed);
        }
        assert_eq!(
            (code::SETTLED, code::ALIVE, code::IDENTITY_UNKNOWN),
            (0, 4, 5)
        );
    }

    #[test]
    fn the_cap_of_the_backoff_is_reached_once() {
        let capped: Vec<u32> = (0..40).filter(|t| backoff_capped(*t)).collect();
        assert_eq!(capped, vec![5]);
        assert!(!backoff_capped(u32::MAX));
        // It is the first try whose pause is the longest.
        assert!(backoff_ms(100, 4) < backoff_ms(100, 5));
        assert_eq!(backoff_ms(100, 5), backoff_ms(100, 6));
    }

    #[test]
    fn a_wait_ends_early_only_for_a_cancel_or_a_retire_that_is_new() {
        assert!(wakes(false, true, false, false));
        assert!(wakes(false, false, false, true));
        assert!(wakes(false, true, false, true));
        assert!(wakes(true, true, false, true));
        assert!(wakes(false, true, true, true));
        assert!(!wakes(false, false, false, false));
        assert!(!wakes(true, true, false, false));
        assert!(!wakes(false, false, true, true));
        assert!(!wakes(true, true, true, true));
        // What was there and is not any more is not news.
        assert!(!wakes(true, false, true, false));
    }

    #[test]
    fn the_backoff_doubles_up_to_32_times_the_base() {
        assert_eq!(backoff_ms(100, 0), 100);
        assert_eq!(backoff_ms(100, 1), 200);
        assert_eq!(backoff_ms(100, 4), 1_600);
        assert_eq!(backoff_ms(100, 5), 3_200);
        assert_eq!(backoff_ms(100, 6), 3_200);
        assert_eq!(backoff_ms(100, u32::MAX), 3_200);
        assert_eq!(backoff_ms(u64::MAX, 1), u64::MAX);
    }

    #[test]
    fn only_launches_all_recorded_by_another_boot_prove_a_reboot() {
        assert!(!launched_on_another_boot(&[], "a"));
        assert!(launched_on_another_boot(&[Some("b")], "a"));
        assert!(launched_on_another_boot(&[Some("b"), Some("c")], "a"));
        assert!(!launched_on_another_boot(&[Some("a")], "a"));
        assert!(!launched_on_another_boot(&[Some("b"), Some("a")], "a"));
        assert!(!launched_on_another_boot(&[Some("b"), None], "a"));
        assert!(!launched_on_another_boot(&[None], "a"));
    }

    #[test]
    fn an_attempt_nobody_watches_is_unknown_until_it_ends() {
        use AttemptState::*;
        let seen = |s, gave_up, r| observation(s, gave_up, r);
        assert_eq!(seen(Active, false, Recovery::Supervise), Observation::Known);
        assert_eq!(
            seen(Active, true, Recovery::Supervise),
            Observation::Unknown
        );
        assert_eq!(
            seen(Active, false, Recovery::Unresolved),
            Observation::Unknown
        );
        assert_eq!(seen(Terminal, true, Recovery::Nothing), Observation::Known);
        assert_eq!(
            seen(Terminal, false, Recovery::Unresolved),
            Observation::Known
        );
    }

    #[test]
    fn a_secret_bundle_is_placed_only_before_the_start() {
        use AttemptState::*;
        assert_eq!(secret_put(Preparing, false, true), SecretPut::Place);
        assert_eq!(secret_put(Accepted, false, true), SecretPut::Place);
        assert_eq!(secret_put(Accepted, false, false), SecretPut::NotReady);
        assert_eq!(secret_put(Preparing, false, false), SecretPut::NotReady);
        for s in [Ready, Starting, Active, Cancelling, Finalizing, Terminal] {
            assert_eq!(secret_put(s, false, true), SecretPut::TooLate);
            assert_eq!(secret_put(s, false, false), SecretPut::TooLate);
        }
        // A stop wins over everything: the answer does not depend on timing.
        assert_eq!(secret_put(Preparing, true, true), SecretPut::Stopping);
        assert_eq!(secret_put(Active, true, false), SecretPut::Stopping);
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
        let e = wire_event("lease_expired", &Value::Null).unwrap();
        assert_eq!(
            (e.kind, e.state, e.receipt),
            (EventKind::LeaseExpired, None, None)
        );
        // The stop the supervisor notes is a diagnostic, whatever it says.
        assert_eq!(
            wire_event("supervisor_stop", &json!({"reason": "LeaseExpired"})),
            None
        );
        for (name, r) in [
            ("command_registered", Receipt::CommandRegistered),
            ("running", Receipt::Running),
        ] {
            let e = wire_event("attempt_receipt", &json!({ "receipt": name })).unwrap();
            assert_eq!(
                (e.kind, e.state, e.receipt),
                (EventKind::ReceiptReached, None, Some(r))
            );
        }
        // The other receipts have their own journal entries; an entry that
        // names one of them, or nothing readable, is not a wire event.
        for other in [
            "coordinator_accepted",
            "worker_accepted",
            "terminal",
            "collected",
        ] {
            assert_eq!(
                wire_event("attempt_receipt", &json!({ "receipt": other })),
                None
            );
        }
        assert_eq!(
            wire_event("attempt_receipt", &json!({ "receipt": 1 })),
            None
        );
        assert_eq!(wire_event("attempt_receipt", &json!({})), None);
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
