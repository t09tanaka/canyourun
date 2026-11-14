//! Running an attempt's nodes under supervision (spec chapter 12, 15).
//!
//! One supervisor process serves one attempt. It does not depend on the
//! agent: everything it needs is in the spec and the worker database, and
//! everything it decides is written there. For each node it registers the
//! command with task-traffic-controller, waits for admission, persists the
//! launch intent, and runs the command; while the command runs it watches the
//! cancel request, the lease and the timeouts, and stops the command's whole
//! process group when one of them says so. The result is fixed once, after
//! the stop is confirmed.
//!
//! The commands' output goes to the attempt's log spool
//! ([`super::spool`], through [`super::pump`]). While a command runs, every
//! look (one per `poll_ms`: the supervisor's heartbeat) also makes the spool
//! durable and stores its positions in the worker database; the result is
//! fixed through [`finish_attempt`], which closes the spool first. A spool
//! that cannot store the output (it cannot be opened, a write or the final
//! sync fails) stops the command, is recorded as a `spool_failed` event and
//! makes the attempt `interrupted`: its output is incomplete, so it is never
//! reported as a success ([`after_spool_failure`]). The secret values of the
//! spec (`mask`) are replaced before any output is stored.
//!
//! When the command's processes cannot be confirmed stopped (even after a few
//! bounded retries), the attempt is left non-terminal and the process exits
//! with `UNRESOLVED` after recording a `supervisor_unresolved` event (reason
//! and node). Trying again is the parent's job, not this process's: the
//! parent's reconcile sees the supervisor lock free and the attempt not
//! terminal, starts `supervise` again, and [`super::recover::recover`] redoes
//! the stop of the group (S10/S12 implement that loop). When the only reason
//! is that whose a process or group is could not be told, the exit code is
//! `IDENTITY_UNKNOWN` instead, which the parent does not count as a failure.

use super::error::{Result, SupervisorError};
use super::identity::Leader;
use super::members::ManagedMembers;
use super::outcome::{
    add_ref, finish_attempt_with, remove_unlisted, store_artifact, sweep_tmp, sync_log_position,
    ArtifactInput, Forbidden, OutcomeError, SpoolEnd,
};
use super::pump::{pump, Pipe};
use super::spec::{command_id, spec_digest, NodeSpec, SuperviseSpec};
use super::spool::{closed_status, AttemptSpool, SpoolError, SpoolReader, StreamStatus};
use super::spool_plan::{
    artifacts_event, attempt_limit, journaled_artifacts, journaled_not_stored, logs_locator,
    warning_texts, ARTIFACTS_EVENT_KEY, MAX_WARNINGS, MAX_WARNING_BYTES,
};
use super::stop::{
    retries_for, stop_group_retrying_on, Os, StopResult, StopTarget, StopTimes, System,
};
use super::termination::{
    after_output, after_spool_failure, may_continue, pick_reason, termination, StopReason,
};
use super::{lease::LeaseClock, lease::LeaseStatus, timeouts};
use crate::ids::{AttemptId, NodeId, WorkerBootId};
use crate::platform::{self, Inhibit, StartIdentity};
use crate::secrets::mask::Masker;
use crate::state::{AttemptState, Exit, Receipt, Termination};
use crate::storage::{
    BlobKind, Finalized, LaunchClaim, LogPosition, ProcessIdentity, StorageError, WorkerStore,
};
use crate::wire::{ArtifactNotStored, ArtifactRecord, LogStream, NotStoredReason};
use serde_json::json;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use task_traffic_controller::config::Config;
use task_traffic_controller::context::Context;
use task_traffic_controller::exec::Opts;
use task_traffic_controller::managed::{
    self, CommandSpec, Loaded, Registration, TerminalKind, TryAdmit,
};
use task_traffic_controller::paths::Paths;
use task_traffic_controller::runner::Exit as TtcExit;

/// How a supervisor run ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Report {
    /// The attempt's result, or `None` when it could not be settled (see
    /// `unresolved`): the attempt then stays non-terminal and the parent keeps
    /// it as unknown.
    pub termination: Option<Termination>,
    /// This run fixed the termination (otherwise somebody had before).
    pub finalized_here: bool,
    /// Why the attempt was left unresolved, when it was. The parent retries
    /// by starting the supervisor again (it recovers and redoes the stop);
    /// this process does not wait for that.
    pub unresolved: Option<String>,
    /// The attempt is unresolved for no other reason than that whose a
    /// process or group is could not be told (nothing of it was signalled).
    /// That can clear up by itself, so the parent keeps looking again
    /// instead of counting it as a failure.
    pub identity_unknown: bool,
    /// What came of the idle-sleep inhibition ("held via ..." or
    /// "unavailable: ...", "disabled").
    pub sleep_inhibit: String,
}

/// How one node ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct NodeEnd {
    pub reason: Option<StopReason>,
    pub exit: Option<Exit>,
    /// The command's group could not be confirmed stopped.
    pub unconfirmed: bool,
    /// It could not be confirmed because the identity of the command's
    /// process could not be read: nothing was signalled.
    pub identity_unknown: bool,
}

pub(crate) fn exit_of(e: TtcExit) -> Option<Exit> {
    match e {
        TtcExit::Code(c) => Some(Exit::Code(c)),
        TtcExit::Signal(s) => Some(Exit::Signal(s)),
    }
}

pub(crate) struct Traffic {
    pub ctx: Context,
    pub cfg: Config,
}

impl Traffic {
    pub fn open(spec: &SuperviseSpec) -> Result<Self> {
        let paths = Paths::new(&spec.traffic_state_dir, spec.traffic_config_path());
        let cfg = Config::load(paths.config_file()).map_err(SupervisorError::traffic)?;
        Ok(Traffic {
            ctx: Context::new(paths),
            cfg,
        })
    }
}

pub(crate) fn stop_times(spec: &SuperviseSpec) -> StopTimes {
    StopTimes {
        grace_ms: spec.term_grace_ms,
        kill_wait_ms: spec.kill_wait_ms,
        poll_ms: spec.poll_ms.clamp(5, 100),
    }
}

/// Stop the group, with the bounded retries when it cannot be confirmed.
pub(crate) fn stop_confirmed_on(os: &dyn Os, t: &StopTarget, spec: &SuperviseSpec) -> StopResult {
    stop_group_retrying_on(os, t, stop_times(spec), retries_for(spec.kill_wait_ms))
}

/// A retryable storage error is not a fault: the same look may work next time.
pub(crate) fn absorb_retryable(r: Result<Option<StopReason>>) -> Result<Option<StopReason>> {
    match r {
        Err(SupervisorError::Storage(e)) if e.is_retryable() => Ok(None),
        other => other,
    }
}

/// At most this many hidden suspects are named in an `unresolved` event.
pub(crate) const MAX_HIDDEN_SUSPECTS: usize = 8;

/// The hidden processes that keep entry `id` from being resolved, as event
/// detail: who holds a stuck recovery (pid, start time, parent, session and
/// the reason it cannot be ruled out). At most [`MAX_HIDDEN_SUSPECTS`].
pub(crate) fn hidden_suspects_json(ctx: &Context, id: &str) -> Vec<serde_json::Value> {
    managed::hidden_suspects(ctx, id)
        .iter()
        .take(MAX_HIDDEN_SUSPECTS)
        .map(|h| {
            json!({
                "pid": h.proc.pid,
                "start_time": h.proc.start_time,
                "parent": h.parent,
                "session": h.sid,
                "reason": h.reason(),
            })
        })
        .collect()
}

/// The report for an attempt that cannot be settled, after recording why
/// (and for which nodes) in the worker database.
pub(crate) fn unresolved_report(
    store: &WorkerStore,
    attempt: &crate::ids::AttemptId,
    nodes: &[String],
    reason: String,
    identity_unknown: bool,
    sleep_inhibit: String,
    hidden_suspects: Vec<serde_json::Value>,
) -> Result<Report> {
    if let Some(a) = store.get(attempt)? {
        store.append_event(
            Some(&a.run_id),
            Some(attempt),
            "supervisor_unresolved",
            &json!({
                "reason": reason,
                "nodes": nodes,
                "identity_unknown": identity_unknown,
                "hidden_suspects": hidden_suspects,
            }),
            platform::wall_ms(),
        )?;
    }
    Ok(Report {
        termination: None,
        finalized_here: false,
        unresolved: Some(reason),
        identity_unknown,
        sleep_inhibit,
    })
}

/// Settle the attempt: move it to `Finalizing` (through `Cancelling` first
/// for a cancel, which the caller did earlier) and fix the termination once.
pub(crate) fn finalize(
    store: &WorkerStore,
    attempt: &crate::ids::AttemptId,
    term: Termination,
) -> Result<Finalized> {
    to_finalizing(store, attempt)?;
    Ok(store.finalize(attempt, term, platform::wall_ms())?)
}

/// Move the attempt to `Finalizing` unless it is there or terminal already.
fn to_finalizing(store: &WorkerStore, attempt: &AttemptId) -> Result<()> {
    for _ in 0..3 {
        let state = store.get(attempt)?.map(|a| a.state);
        match state {
            Some(AttemptState::Finalizing) | Some(AttemptState::Terminal) => break,
            Some(_) => {
                match store.transition(attempt, AttemptState::Finalizing, platform::wall_ms()) {
                    Ok(_) => break,
                    // The state moved under us: look again.
                    Err(StorageError::Transition(_)) | Err(StorageError::IllegalState { .. }) => {}
                    Err(e) => return Err(e.into()),
                }
            }
            None => {
                return Err(SupervisorError::State(format!(
                    "attempt {attempt} vanished"
                )));
            }
        }
    }
    Ok(())
}

/// The attempt's spool as the supervisor finds it.
#[derive(Debug)]
pub(crate) enum Spool {
    Open(AttemptSpool),
    /// Closed by a supervisor that did not live to settle the attempt: the
    /// final positions `(stdout, stderr)`.
    Closed(StreamStatus, StreamStatus),
}

impl Spool {
    pub fn end(&self) -> SpoolEnd<'_> {
        match self {
            Spool::Open(s) => SpoolEnd::Open(s),
            Spool::Closed(out, err) => SpoolEnd::Closed(*out, *err),
        }
    }
}

/// Open (or, after a crash, resume) the attempt's spool with what the task's
/// limit leaves it: the limit less what the task's other attempts stored.
/// A spool that is closed already (the supervisor crashed between closing it
/// and settling the attempt) is [`Spool::Closed`], which settles like an open
/// one. The inner error is the spool's own: the caller settles the attempt
/// without a spool then ([`settle_without_spool`]).
///
/// The limit is a known approximation when two attempts of one task run at
/// the same time: each sees only what the other had stored when it opened
/// its spool, so together they may exceed the task's limit (by at most the
/// limit itself for each attempt running beside another). Attempts of a task normally run one after another.
pub(crate) fn open_spool(
    spec: &SuperviseSpec,
    store: &WorkerStore,
) -> Result<std::result::Result<Spool, SpoolError>> {
    let others = store.log_bytes_of_other_attempts(&spec.attempt_id)?;
    let limit = attempt_limit(spec.log_limit_bytes, others);
    let root = spec.spool_dir();
    Ok(match AttemptSpool::open(&root, &spec.attempt_id, limit) {
        Ok(s) => Ok(Spool::Open(s)),
        Err(SpoolError::Finished) => match closed_status(&root, &spec.attempt_id) {
            Ok(Some((out, err))) => Ok(Spool::Closed(out, err)),
            Ok(None) => Err(SpoolError::Finished),
            Err(e) => Err(e),
        },
        Err(e) => Err(e),
    })
}

/// Record that the spool failed at `stage`, once per stage.
fn note_spool_failure(
    store: &WorkerStore,
    attempt: &AttemptId,
    stage: &str,
    why: &str,
) -> Result<()> {
    let Some(a) = store.get(attempt)? else {
        return Ok(());
    };
    store.append_events_unseen(
        Some(&a.run_id),
        attempt,
        "spool_failed",
        "stage",
        &[(stage.to_string(), json!({ "stage": stage, "error": why }))],
        platform::wall_ms(),
    )?;
    Ok(())
}

/// Settle the attempt as `term`: close its spool, fix the termination and
/// store the result ([`finish_attempt`]). When the spool cannot be completed
/// the attempt is settled without it. Returns the termination that holds and
/// whether this call fixed it.
pub(crate) fn settle<'a>(
    store: &WorkerStore,
    root: &Path,
    attempt: &AttemptId,
    spool: impl Into<SpoolEnd<'a>>,
    term: Termination,
) -> Result<(Termination, bool)> {
    to_finalizing(store, attempt)?;
    // The artifacts are the ones the journal fixed ([`keep_artifacts`]),
    // whoever settles: a supervisor that comes after a crash, when the
    // workspace may be gone, stores the same list. What was copied without
    // getting into that list is in no result, and is removed.
    let (artifacts, not_stored) = fixed_artifacts(store, attempt)?;
    remove_unlisted(root, attempt, &artifacts).map_err(outcome_error)?;
    match finish_attempt_with(
        store,
        root,
        attempt,
        spool,
        term,
        &artifacts,
        &not_stored,
        platform::wall_ms(),
    ) {
        Ok(recorded) => {
            let fixed = store
                .get(attempt)?
                .and_then(|a| a.termination)
                .ok_or_else(|| {
                    SupervisorError::State(format!(
                        "attempt {attempt} was finished but is not terminal"
                    ))
                })?;
            Ok((fixed, recorded.finalized))
        }
        Err(OutcomeError::SpoolFailed(e)) => {
            settle_without_spool(store, root, attempt, "finish", &e.to_string(), term)
        }
        Err(e) => Err(outcome_error(e)),
    }
}

fn outcome_error(e: OutcomeError) -> SupervisorError {
    match e {
        OutcomeError::Storage(e) => e.into(),
        OutcomeError::Io(e) => e.into(),
        e => SupervisorError::State(e.to_string()),
    }
}

/// The artifacts the attempt's journal fixed, and the declared ones it fixed
/// as not stored; none when it fixed none.
pub(crate) fn fixed_artifacts(
    store: &WorkerStore,
    attempt: &AttemptId,
) -> Result<(Vec<ArtifactRecord>, Vec<ArtifactNotStored>)> {
    let events = store.events_of(attempt, ARTIFACTS_EVENT)?;
    let payloads: Vec<&serde_json::Value> = events.iter().map(|e| &e.payload).collect();
    journaled_artifacts(&payloads)
        .zip(journaled_not_stored(&payloads))
        .ok_or_else(|| {
            SupervisorError::State(format!(
                "the artifacts entry in the journal of attempt {attempt} cannot be read"
            ))
        })
}

const ARTIFACTS_EVENT: &str = "artifacts_stored";

/// Why a declared artifact was not stored, for the result: `missing` only
/// when nothing is at the declared path (the file, or a directory on the way
/// to it, does not exist). A store that is not there is not a missing
/// artifact.
pub(super) fn not_stored_reason(e: &OutcomeError) -> NotStoredReason {
    match e {
        OutcomeError::SourceMissing(_) => NotStoredReason::Missing,
        _ => NotStoredReason::Refused,
    }
}

/// Copy the declared artifacts out of the workspace and fix the list in the
/// journal, before the attempt is settled. Call it once the commands have
/// ended and are confirmed stopped (nothing writes the files any more), and
/// only then: the result is immutable and the workspace goes when the attempt
/// is terminal.
///
/// A file that cannot be kept (missing, not a regular file, reached through
/// a symlink, too large, one of the attempt's secret files, holding one of
/// its secret values, ...) is left out and said in the journal
/// (`artifact_not_stored`, once per name, with the masked reason); the
/// attempt's termination is not changed by it. Whether a missing artifact
/// fails the run is the coordinator's decision, which knows what was
/// required. The list is fixed once: a second call changes nothing.
pub(crate) fn keep_artifacts(
    spec: &SuperviseSpec,
    store: &WorkerStore,
    masker: &Masker,
) -> Result<()> {
    let Some(plan) = &spec.artifacts else {
        return Ok(());
    };
    let attempt = &spec.attempt_id;
    if !store.events_of(attempt, ARTIFACTS_EVENT)?.is_empty() {
        return Ok(());
    }
    let Some(rec) = store.get(attempt)? else {
        return Err(SupervisorError::State(format!(
            "attempt {attempt} vanished"
        )));
    };
    let root = spec.spool_dir();
    sweep_tmp(&root, attempt).map_err(outcome_error)?;
    let mut kept = Vec::new();
    let mut not_stored = Vec::new();
    let mut refused = Vec::new();
    let forbidden = Forbidden {
        files: plan.secret_files.clone(),
        values: masker.clone(),
    };
    for f in &plan.files {
        let input = ArtifactInput {
            name: &f.name,
            source: &f.source,
            source_id: plan.source_id.clone(),
            plan_digest: plan.plan_digest.clone(),
            profile_fingerprint: plan.profile_fingerprint.clone(),
            forbidden: &forbidden,
        };
        match store_artifact(&root, attempt, &plan.workspace, &input, plan.max_bytes) {
            Ok(record) => kept.push(record),
            Err(OutcomeError::Storage(e)) => return Err(e.into()),
            Err(e) => {
                not_stored.push(ArtifactNotStored {
                    name: f.name.clone(),
                    path: f.source.to_string_lossy().into_owned(),
                    if_missing: f.if_missing,
                    reason: not_stored_reason(&e),
                });
                let why =
                    String::from_utf8_lossy(&masker.mask(e.to_string().as_bytes())).into_owned();
                refused.push((f.name.clone(), json!({ "name": f.name, "why": why })));
            }
        }
    }
    let at = platform::wall_ms();
    store.append_events_unseen(
        Some(&rec.run_id),
        attempt,
        "artifact_not_stored",
        "name",
        &refused,
        at,
    )?;
    store.append_events_unseen(
        Some(&rec.run_id),
        attempt,
        ARTIFACTS_EVENT,
        "set",
        &[(
            ARTIFACTS_EVENT_KEY.to_string(),
            artifacts_event(&kept, &not_stored),
        )],
        at,
    )?;
    Ok(())
}

/// Settle an attempt whose spool failed at `stage`: say so in the journal and
/// fix what [`after_spool_failure`] makes of `term` (never a success). No
/// result is stored: there is no complete output to describe, and the
/// `spool_failed` event (written first) is what tells this terminal attempt
/// from one whose supervisor crashed before storing its result
/// ([`complete_terminal`]). The spool is not marked closed either, so a
/// reader gets no `eof` from it and goes by the attempt's termination.
///
/// What the spool holds stays accounted for: the positions its index
/// publishes are stored (as far as they can be read), and its directory is
/// referenced before the termination is fixed, so GC collects it like any
/// other attempt's.
pub(crate) fn settle_without_spool(
    store: &WorkerStore,
    root: &Path,
    attempt: &AttemptId,
    stage: &str,
    why: &str,
    term: Termination,
) -> Result<(Termination, bool)> {
    note_spool_failure(store, attempt, stage, why)?;
    if let Ok(reader) = SpoolReader::open(root, attempt) {
        for (stream, name) in [(LogStream::Stdout, "stdout"), (LogStream::Stderr, "stderr")] {
            if let Ok(st) = reader.status(stream) {
                store.update_log_position(attempt, name, st.into())?;
            }
        }
    }
    let at = platform::wall_ms();
    match add_ref(
        store,
        attempt,
        format!("{attempt}:logs"),
        BlobKind::Log,
        logs_locator(attempt),
        at,
    ) {
        Ok(()) => {}
        Err(OutcomeError::Storage(e)) => return Err(e.into()),
        Err(e) => return Err(SupervisorError::State(e.to_string())),
    }
    // No result will name an artifact, so none is kept: a file that stayed
    // would be in no reference and never collected.
    remove_unlisted(root, attempt, &[]).map_err(outcome_error)?;
    let fin = finalize(store, attempt, after_spool_failure(term))?;
    Ok((fin.termination(), fin.won()))
}

/// Finish what a supervisor that crashed while settling left of a terminal
/// attempt: one whose termination is fixed but which has no result (the crash
/// came between the two). The spool is closed or found closed, its files
/// referenced and the result stored for the termination that holds. An
/// attempt that has its result, or that was settled without its spool (it
/// has a `spool_failed` event of the stage `open` or `finish`, the two
/// [`settle_without_spool`] is called with, and no result by design), is
/// left as it is. A `spool_failed` event of another stage (`write`, `pipe`)
/// does not say that: such an attempt is settled through its spool, and a
/// crash on the way is completed like any other.
///
/// A terminal attempt has no command left, so the FIFOs of its nodes are
/// removed here as well.
pub(crate) fn complete_terminal(
    spec: &SuperviseSpec,
    store: &WorkerStore,
    fixed: Termination,
) -> Result<()> {
    let attempt = &spec.attempt_id;
    let root = spec.spool_dir();
    remove_fifos(spec);
    let without_spool = store
        .events_of(attempt, "spool_failed")?
        .iter()
        .any(|e| settled_without_spool_at(e.payload["stage"].as_str()));
    if store.get_result(attempt)?.is_some() || without_spool {
        return Ok(());
    }
    match open_spool(spec, store)? {
        Ok(spool) => settle(store, &root, attempt, spool.end(), fixed)?,
        Err(e) => settle_without_spool(store, &root, attempt, "open", &e.to_string(), fixed)?,
    };
    Ok(())
}

/// True for the stages of a `spool_failed` event that mean the attempt was
/// settled without its spool.
fn settled_without_spool_at(stage: Option<&str>) -> bool {
    matches!(stage, Some("open" | "finish"))
}

/// Remove the FIFOs the attempt's nodes got their output through. Only when
/// no command of the attempt is alive: they have no writer and no reader
/// then. Best effort: one that stays goes with the attempt's directory.
pub(crate) fn remove_fifos(spec: &SuperviseSpec) {
    use std::os::unix::fs::FileTypeExt;
    let root = spec.spool_dir();
    for index in 0..spec.nodes.len() {
        let path = Pipe::path_for(&root, &spec.attempt_id, index);
        // Never anything but a FIFO: the name is ours, but a file found
        // there is not removed on the name alone.
        if std::fs::symlink_metadata(&path).is_ok_and(|m| m.file_type().is_fifo()) {
            let _ = std::fs::remove_file(&path);
        }
    }
}

/// What the reader thread reported (`pump`) when the heartbeat did not get
/// to record it (a busy database made it skip): the `spool_failed` event is
/// written now, before the attempt is settled. Returns whether output was
/// lost, by either account.
pub(crate) fn account_for_reader(
    store: &WorkerStore,
    attempt: &AttemptId,
    beat: &SpoolBeat,
    pump: Option<String>,
) -> Result<bool> {
    match (&beat.failure, pump) {
        (Some(_), _) => Ok(true),
        (None, Some(why)) => {
            note_spool_failure(store, attempt, "write", &why)?;
            Ok(true)
        }
        (None, None) => Ok(false),
    }
}

/// The spool side of the supervisor's heartbeat.
#[derive(Debug, Default)]
pub(crate) struct SpoolBeat {
    /// Why the spool can no longer store the output, once it cannot.
    pub failure: Option<String>,
    /// The positions the worker database holds.
    synced: Option<(LogPosition, LogPosition)>,
}

impl SpoolBeat {
    /// One heartbeat: make what the spool holds durable and store its
    /// positions ([`sync_log_position`]); nothing is done when nothing was
    /// appended since the last one. `pump_error` is what the thread feeding the
    /// spool reported. A spool failure is recorded here (the event, then
    /// `failure`) and is not an error of this call; a storage error is.
    pub fn beat(
        &mut self,
        store: &WorkerStore,
        attempt: &AttemptId,
        spool: &AttemptSpool,
        pump_error: Option<String>,
    ) -> Result<()> {
        if self.failure.is_some() {
            return Ok(());
        }
        let failed = match pump_error {
            Some(why) => Some(why),
            None => {
                let now: (LogPosition, LogPosition) = (
                    spool.writer(LogStream::Stdout).status().into(),
                    spool.writer(LogStream::Stderr).status().into(),
                );
                if self.synced == Some(now) {
                    return Ok(());
                }
                match sync_log_position(store, attempt, spool) {
                    Ok(stored) => {
                        self.synced = Some(stored);
                        None
                    }
                    Err(OutcomeError::Storage(e)) => return Err(e.into()),
                    Err(e) => Some(e.to_string()),
                }
            }
        };
        if let Some(why) = failed {
            note_spool_failure(store, attempt, "write", &why)?;
            self.failure = Some(why);
        }
        Ok(())
    }
}

/// Move to `to` when the state machine allows it from where the attempt is;
/// leave it alone otherwise (already there, or past it).
fn move_to(store: &WorkerStore, attempt: &crate::ids::AttemptId, to: AttemptState) -> Result<()> {
    let Some(a) = store.get(attempt)? else {
        return Ok(());
    };
    if a.state == to || !a.state.can_transition(to) {
        return Ok(());
    }
    match store.transition(attempt, to, platform::wall_ms()) {
        Ok(_) | Err(StorageError::Transition(_)) | Err(StorageError::IllegalState { .. }) => Ok(()),
        Err(e) => Err(e.into()),
    }
}

/// Sets the flag when dropped: by the normal end of its scope or by a panic.
struct SetOnDrop<'a>(&'a AtomicBool);

impl Drop for SetOnDrop<'_> {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

struct Control<'a> {
    spec: &'a SuperviseSpec,
    store: &'a WorkerStore,
    traffic: Traffic,
    lease: LeaseClock,
    started_ms: u64,
    spool: &'a AttemptSpool,
    masker: Masker,
    /// The run-time limit of the node being run, when it has its own.
    node_timeout_ms: Option<u64>,
    beat: SpoolBeat,
    /// The first failure of a thread feeding the spool.
    pump_error: Arc<Mutex<Option<String>>>,
}

/// Notify only after a new grant was accepted by the clock. Diagnostics are
/// best effort: their storage failure cannot change the lease decision.
fn observe_lease(
    clock: &mut LeaseClock,
    now: u64,
    key: Option<crate::wire::LeaseKey>,
    db_expired: bool,
    note: impl FnOnce(crate::wire::LeaseKey) -> bool,
) -> LeaseStatus {
    let before = clock.applied_key();
    let status = clock.observe(now, key, db_expired);
    if matches!(status, LeaseStatus::Valid { .. }) && clock.applied_key() != before {
        if let Some(applied) = clock.applied_key() {
            let _ = note(applied);
        }
    }
    status
}

impl Control<'_> {
    /// One look at the cancel request, the lease and the timeouts.
    fn look(&mut self, stage: timeouts::Stage) -> Result<Option<StopReason>> {
        let attempt = &self.spec.attempt_id;
        let now = platform::monotonic_ms();
        let Some(rec) = self.store.get(attempt)? else {
            return Err(SupervisorError::State(format!(
                "attempt {attempt} vanished"
            )));
        };
        let (key, db_expired) = match self.store.lease(attempt)? {
            Some((k, e)) => (Some(k), e),
            None => (None, false),
        };
        let status = observe_lease(&mut self.lease, now, key, db_expired, |applied| {
            // Like the identity diagnostic, this is not an authoritative
            // receipt or a condition for keeping the command alive.
            self.store
                .append_event(
                    Some(&rec.run_id),
                    Some(attempt),
                    "lease_renewal_observed",
                    &json!({ "key": applied }),
                    platform::wall_ms(),
                )
                .is_ok()
        });
        let expired = status == LeaseStatus::Expired;
        if expired && !db_expired {
            // Persist it: from now on a renewal is refused, and no new node
            // can be claimed.
            self.store.expire_lease(attempt, platform::wall_ms())?;
        }
        let policy = self.spec.timeout_policy().for_command(self.node_timeout_ms);
        let timeout = timeouts::fired(&policy, self.started_ms, stage, now);
        Ok(pick_reason(
            rec.cancel_requested_at_ms.is_some() || rec.retired,
            expired,
            timeout,
        ))
    }

    fn begin_stop(&self, reason: StopReason) -> Result<()> {
        let attempt = &self.spec.attempt_id;
        if reason == StopReason::Cancel {
            // The cancel was persisted by whoever asked; now the attempt shows
            // that it is being stopped.
            move_to(self.store, attempt, AttemptState::Cancelling)?;
        }
        if let Some(a) = self.store.get(attempt)? {
            self.store.append_event(
                Some(&a.run_id),
                Some(attempt),
                "supervisor_stop",
                &json!({ "reason": format!("{reason:?}") }),
                platform::wall_ms(),
            )?;
        }
        Ok(())
    }

    fn run_node(&mut self, index: usize, node: &NodeSpec) -> Result<NodeEnd> {
        let spec = self.spec;
        self.node_timeout_ms = node.timeout_ms;
        let attempt = spec.attempt_id.clone();
        let id = command_id(&attempt, index);
        // The pipe the command's output comes through. Without it nothing is
        // registered or started.
        let pipe = match Pipe::open(&Pipe::path_for(&spec.spool_dir(), &attempt, index)) {
            Ok(p) => p,
            Err(e) => {
                let why = format!("the output pipe of node {}: {e}", node.node_id);
                note_spool_failure(self.store, &attempt, "pipe", &why)?;
                self.beat.failure = Some(why);
                return Ok(NodeEnd {
                    reason: Some(StopReason::SpoolFailed),
                    exit: None,
                    unconfirmed: false,
                    identity_unknown: false,
                });
            }
        };
        let opts = Opts {
            label: None,
            weight: node.weight,
            front: false,
            key: None,
            argv: node.argv.clone(),
            cwd: node.cwd.clone(),
            env: Some(node.env.clone()),
            stdout_log: Some(pipe.path().to_path_buf()),
        };
        let limits = self.traffic.cfg.limits();
        let registration = managed::register(
            &self.traffic.ctx,
            &self.traffic.cfg,
            &CommandSpec {
                command_id: id.clone(),
                spec_digest: spec_digest(node),
                opts: opts.clone(),
                attempt: Some(json!({
                    "attempt_id": attempt,
                    "node_id": node.node_id,
                })),
                services: vec![],
            },
        )
        .map_err(SupervisorError::traffic)?;
        let guard = match registration {
            Registration::Registered(_, guard) => guard,
            // The id already ended: nothing may run for it again.
            Registration::AlreadyTerminal(_) => {
                return Ok(NodeEnd {
                    reason: None,
                    exit: None,
                    unconfirmed: false,
                    identity_unknown: false,
                })
            }
        };
        self.store
            .mark_receipt(&attempt, Receipt::CommandRegistered, platform::wall_ms())?;

        // Admission. Nothing has been started yet, so any stop reason just
        // withdraws the registration.
        let waiting_since = platform::monotonic_ms();
        loop {
            let stage = timeouts::Stage::Waiting {
                since_ms: waiting_since,
            };
            if let Some(reason) = absorb_retryable(self.look(stage))? {
                return self.withdraw(guard, reason);
            }
            let waited = platform::monotonic_ms().saturating_sub(waiting_since) as f64 / 1000.0;
            match managed::try_admit(&self.traffic.ctx, limits, &id, waited)
                .map_err(SupervisorError::traffic)?
            {
                TryAdmit::Admitted => break,
                TryAdmit::Wait(_) | TryAdmit::LockBusy => {
                    std::thread::sleep(Duration::from_millis(spec.poll_ms))
                }
            }
        }

        // The launch intent. The last look comes first so that an expired
        // lease or a cancel seen now never starts anything; the claim itself
        // is refused by the database in the same cases.
        let stage = timeouts::Stage::Waiting {
            since_ms: waiting_since,
        };
        if let Some(reason) = self.look(stage)? {
            return self.withdraw(guard, reason);
        }
        match self
            .store
            .claim_launch(&attempt, &node.node_id, platform::wall_ms())
        {
            Ok(LaunchClaim::Granted) => {}
            Ok(LaunchClaim::AlreadyClaimed { .. }) => {
                return Err(SupervisorError::State(format!(
                    "node {} was already claimed: it is never started twice",
                    node.node_id
                )))
            }
            Err(StorageError::IllegalState { .. }) | Err(StorageError::Retired(_)) => {
                // Refused: a cancel, a retire or a lease expiry got in first.
                return match self.look(stage)? {
                    Some(reason) => self.withdraw(guard, reason),
                    None => Err(SupervisorError::State(
                        "the launch was refused for an unknown reason".into(),
                    )),
                };
            }
            Err(e) => return Err(e.into()),
        }
        self.run_piped(guard, &opts, node, &id, limits, &pipe)
    }

    /// Run the admitted command with its output pumped into the spool, and
    /// leave the spool's positions stored when it is over.
    fn run_piped(
        &mut self,
        guard: managed::OwnerGuard,
        opts: &Opts,
        node: &NodeSpec,
        id: &str,
        limits: task_traffic_controller::admit::Limits,
        pipe: &Pipe,
    ) -> Result<NodeEnd> {
        let out = self.spool.writer(LogStream::Stdout);
        let error = Arc::clone(&self.pump_error);
        let done = AtomicBool::new(false);
        let end = std::thread::scope(|s| {
            let mask = self.masker.stream();
            let feeder = s.spawn(|| pump(pipe, out, mask, &done, &error));
            // The command is over (or given up on) when this is dropped, also
            // by a panic on the way: what it wrote is read to the end, within
            // a bound, and the reader never waits for a supervisor that is gone.
            let over = SetOnDrop(&done);
            let end = self.run_admitted(guard, opts, node, id, limits);
            drop(over);
            if feeder.join().is_err() {
                error
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .get_or_insert_with(|| "the output reader panicked".into());
            }
            end
        });
        let mut end = end?;
        match self.heartbeat() {
            // The final positions are stored again when the attempt is settled.
            Err(SupervisorError::Storage(e)) if e.is_retryable() => {}
            other => other?,
        }
        // Output was lost: whatever the command did, nothing follows it and
        // it is not a success. The reader's own report is looked at here, not
        // only what the heartbeat made of it: that one may have been skipped
        // (a busy database), and its event is then written before settling.
        let lost = self.beat.failure.is_some() || self.pump_failure().is_some();
        end.reason = after_output(end.reason, lost);
        Ok(end)
    }

    /// What the thread feeding the spool reported, if anything.
    fn pump_failure(&self) -> Option<String> {
        self.pump_error
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    fn heartbeat(&mut self) -> Result<()> {
        let pump_error = self.pump_failure();
        self.beat
            .beat(self.store, &self.spec.attempt_id, self.spool, pump_error)
    }

    /// Stop before anything was started: release the registration.
    fn withdraw(&self, guard: managed::OwnerGuard, reason: StopReason) -> Result<NodeEnd> {
        self.begin_stop(reason)?;
        managed::finish(&self.traffic.ctx, guard, TerminalKind::Cancelled)
            .map_err(SupervisorError::traffic)?;
        Ok(NodeEnd {
            reason: Some(reason),
            exit: None,
            unconfirmed: false,
            identity_unknown: false,
        })
    }

    fn run_admitted(
        &mut self,
        guard: managed::OwnerGuard,
        opts: &Opts,
        node: &NodeSpec,
        id: &str,
        limits: task_traffic_controller::admit::Limits,
    ) -> Result<NodeEnd> {
        let run_since = platform::monotonic_ms();
        let ctx = self.traffic.ctx.clone();
        let mut watch = Watch::default();
        // What the command leaves under its group number once its leader has
        // ended is signalled only with evidence that a member is its own.
        let members = ManagedMembers::new(&ctx, id);
        let os = System(&members);
        let ran = std::thread::scope(|s| {
            let handle = s.spawn(|| managed::run_command(&ctx, &guard, opts, limits));
            while !handle.is_finished() {
                if let Err(e) = self.tick(&ctx, id, node, run_since, &mut watch) {
                    if !matches!(&e, SupervisorError::Storage(se) if se.is_retryable()) {
                        // Supervision is lost: stop the command rather than
                        // leave it running unwatched.
                        watch.reason.get_or_insert(StopReason::LeaseExpired);
                        watch.fatal.get_or_insert(e);
                    }
                }
                if let (Some(_), Some(t), false) = (watch.reason, &watch.target, watch.stopped) {
                    let (pid, result) = (t.pid, stop_confirmed_on(&os, t, self.spec));
                    if watch.after_stop(result) {
                        // Nothing was signalled; the stop is tried again on
                        // the next look. Said once in the event log.
                        watch.deferred_noted = self.note_identity_unknown(&node.node_id, pid);
                    }
                }
                std::thread::sleep(Duration::from_millis(self.spec.poll_ms));
            }
            handle.join()
        });
        let ran = ran.map_err(|_| SupervisorError::State("the command runner panicked".into()))?;
        let exit = ran.as_ref().ok().and_then(|r| exit_of(r.outcome.exit));
        // The leader has been reaped (even when it ended before its identity
        // could be read); whatever it left in its group is stopped too before
        // anything is recorded. The group is found from the sidecar's pid and
        // pgid. The leader is judged by what was read of it while it ran,
        // never by a new read: its PID may be another process's by now.
        let seen = watch.target.take();
        let leftover = sidecar_child(&ctx, id)
            .or_else(|| seen.as_ref().map(|t| (t.pid, t.pgid)))
            .map(|(pid, pgid)| StopTarget {
                pid,
                pgid,
                leader: reaped_leader(seen.as_ref(), pid),
            });
        if let (Some(t), false, false) = (&leftover, watch.recorded, watch.noted) {
            // The command ended before the watch saw it at all.
            self.note_not_recorded(&node.node_id, t.pid)?;
        }
        if let Some(t) = &leftover {
            let result = stop_confirmed_on(&os, t, self.spec);
            if watch.after_leftover_stop(result) && !watch.deferred_noted {
                self.note_identity_unknown(&node.node_id, t.pid);
            }
        }
        let (reason, unconfirmed) = (watch.reason, watch.unconfirmed);
        if let Some(e) = watch.fatal {
            // The entry is left for the reaper; the supervisor reports the fault.
            return Err(e);
        }
        if unconfirmed {
            // Do not release the budget or record a stop: the entry stays for
            // the reaper and the attempt stays unresolved.
            return Ok(NodeEnd {
                reason,
                exit,
                unconfirmed: true,
                identity_unknown: watch.identity_unknown,
            });
        }
        let kind = match (reason, exit) {
            (Some(StopReason::Cancel), _) => TerminalKind::Cancelled,
            (_, Some(Exit::Code(code))) => TerminalKind::Exited { code },
            (_, Some(Exit::Signal(signal))) => TerminalKind::Signaled { signal },
            (_, None) => TerminalKind::SpawnFailed,
        };
        let confirmed = managed::finish(&self.traffic.ctx, guard, kind).is_ok();
        Ok(NodeEnd {
            reason,
            exit,
            unconfirmed: !confirmed,
            identity_unknown: false,
        })
    }

    /// One look while the command runs: record the child once it exists,
    /// then check whether to stop it.
    fn tick(
        &mut self,
        ctx: &Context,
        id: &str,
        node: &NodeSpec,
        run_since: u64,
        w: &mut Watch,
    ) -> Result<()> {
        if !w.recorded {
            // Looked up again until the identity is recorded: it may become
            // readable on a later look.
            w.target = refreshed(w.target.take(), child_target(ctx, id));
        }
        if let (Some(t), false) = (&w.target, w.recorded) {
            if self.record_child(&node.node_id, t)? {
                w.recorded = true;
            } else if !w.noted {
                self.note_not_recorded(&node.node_id, t.pid)?;
                w.noted = true;
            }
        }
        if w.reason.is_none() {
            let stage = timeouts::Stage::Running {
                since_ms: run_since,
            };
            w.reason = self.look(stage)?;
            if let Some(r) = w.reason {
                self.begin_stop(r)?;
            }
        }
        // After the look: a database too busy for the positions must not
        // keep a cancel or an expired lease from being seen.
        self.heartbeat()?;
        if w.reason.is_none() && self.beat.failure.is_some() {
            // The output can no longer be stored: the command is stopped
            // rather than left to run with its output lost.
            w.reason = Some(StopReason::SpoolFailed);
            self.begin_stop(StopReason::SpoolFailed)?;
        }
        Ok(())
    }

    /// Persist the child's identity (boot ID, PID, start identity) and show
    /// the attempt as running. False when there is no identity to persist
    /// (see [`Self::note_not_recorded`]).
    fn record_child(&self, node: &NodeId, t: &StopTarget) -> Result<bool> {
        let attempt = &self.spec.attempt_id;
        let boot = platform::boot_id()
            .and_then(|b| WorkerBootId::parse(&b).ok())
            .ok_or_else(|| SupervisorError::State("the boot id is not readable".into()))?;
        let Leader::Known(start) = t.leader.clone() else {
            return Ok(false);
        };
        self.store.record_process_identity(
            attempt,
            node,
            &ProcessIdentity {
                boot_id: boot,
                pid: t.pid,
                start_identity: start,
            },
            platform::wall_ms(),
        )?;
        move_to(self.store, attempt, AttemptState::Active)?;
        self.store
            .mark_receipt(attempt, Receipt::Running, platform::wall_ms())?;
        Ok(true)
    }

    /// Say in the event log why no identity was recorded, so the launch is
    /// not a mystery later: the command ended before it could be read, or it
    /// was running but its start identity could not be read.
    fn note_not_recorded(&self, node: &NodeId, pid: u32) -> Result<()> {
        let attempt = &self.spec.attempt_id;
        let why = match platform::process_start_identity(pid) {
            StartIdentity::NoProcess | StartIdentity::Zombie(_) => {
                "the command ended before its identity could be read"
            }
            StartIdentity::Identity(_) | StartIdentity::Unknown => {
                "the start identity of the command could not be read"
            }
        };
        if let Some(a) = self.store.get(attempt)? {
            self.store.append_event(
                Some(&a.run_id),
                Some(attempt),
                "supervisor_identity_not_recorded",
                &json!({ "node": node, "pid": pid, "reason": why }),
                platform::wall_ms(),
            )?;
        }
        Ok(())
    }

    /// Say in the event log that a stop sent no signal because the identity
    /// of the command's process could not be read. True when it was written.
    fn note_identity_unknown(&self, node: &NodeId, pid: u32) -> bool {
        let attempt = &self.spec.attempt_id;
        let Ok(Some(a)) = self.store.get(attempt) else {
            return false;
        };
        self.store
            .append_event(
                Some(&a.run_id),
                Some(attempt),
                "supervisor_stop_identity_unknown",
                &json!({
                    "node": node,
                    "pid": pid,
                    "reason": "the identity of the command's process could not be read; \
                               nothing was signalled, the stop is tried again later",
                }),
                platform::wall_ms(),
            )
            .is_ok()
    }
}

/// What the watch loop has learned about a running command.
#[derive(Default)]
struct Watch {
    target: Option<StopTarget>,
    recorded: bool,
    /// The event for a missing identity was written.
    noted: bool,
    reason: Option<StopReason>,
    stopped: bool,
    unconfirmed: bool,
    /// The last stop sent nothing because the identity could not be read.
    identity_unknown: bool,
    /// The event for that was written.
    deferred_noted: bool,
    fatal: Option<SupervisorError>,
}

impl Watch {
    /// Take in the result of a stop tried while the command runs. A stop
    /// that could not tell whose the group is has done nothing: it is not
    /// marked as done, so the next look tries it again. True when that
    /// is to be said in the event log (once).
    fn after_stop(&mut self, result: StopResult) -> bool {
        match result {
            StopResult::Confirmed => self.stopped = true,
            StopResult::Unconfirmed => {
                self.stopped = true;
                self.unconfirmed = true;
            }
            StopResult::IdentityUnknown => return !self.deferred_noted,
        }
        false
    }

    /// Take in the result of the stop of what the reaped command left. There
    /// is no later look here: an unknown identity leaves the attempt
    /// unresolved, and the next `supervise` (recovery) tries again. True
    /// when the identity was the reason.
    fn after_leftover_stop(&mut self, result: StopResult) -> bool {
        match result {
            StopResult::Confirmed => false,
            StopResult::Unconfirmed => {
                self.unconfirmed = true;
                false
            }
            StopResult::IdentityUnknown => {
                self.unconfirmed = true;
                self.identity_unknown = true;
                true
            }
        }
    }
}

/// The watch's target after another look. A leader already seen to have
/// ended, or whose identity is known, is kept: whatever has its PID at a
/// later look may be another process, and must not become the target.
fn refreshed(old: Option<StopTarget>, new: Option<StopTarget>) -> Option<StopTarget> {
    match (old, new) {
        (Some(old), Some(new)) if old.pid == new.pid && old.leader != Leader::Unread => Some(old),
        (old, new) => new.or(old),
    }
}

/// What is known of a command's leader once it has been reaped: the identity
/// read while it ran (`seen`, when it is of the same PID), else only that it
/// ended. Never a new read of the PID.
fn reaped_leader(seen: Option<&StopTarget>, pid: u32) -> Leader {
    match seen {
        Some(StopTarget {
            pid: p,
            leader: Leader::Known(start),
            ..
        }) if *p == pid => Leader::Known(start.clone()),
        _ => Leader::Ended,
    }
}

/// The pid and process group of the registered command `id`'s child, once
/// task-traffic-controller has recorded it (the group defaults to the pid:
/// the child leads its own).
pub(crate) fn sidecar_child(ctx: &Context, id: &str) -> Option<(u32, i32)> {
    let Loaded::Ok(sidecar) = managed::load_sidecar(ctx, id) else {
        return None;
    };
    let child = sidecar.child?;
    Some((child.pid, child.pgid.unwrap_or(child.pid as i32)))
}

/// The child of the registered command `id` as a stop target, with what a
/// read of its PID says about it now: its identity, that it has ended, or
/// that it could not be read (and then nothing of it is signalled until a
/// later look can read it).
///
/// The command is a child of this process, and is reaped by the thread that
/// runs it. A read after that may find another process with the PID, so an
/// identity is taken only from a process whose parent (from the same read)
/// is this process ([`Leader::from_read`]).
fn child_target(ctx: &Context, id: &str) -> Option<StopTarget> {
    let (pid, pgid) = sidecar_child(ctx, id)?;
    Some(StopTarget {
        pid,
        pgid,
        leader: Leader::from_read(&platform::read_process(pid), std::process::id()),
    })
}

/// Supervise the attempt in `spec` from its start. An attempt that already
/// has a launch intent is never started again: it goes to
/// [`super::recover::recover`].
pub fn run(spec: &SuperviseSpec) -> Result<Report> {
    spec.validate()?;
    let store = WorkerStore::open(&spec.state_dir, &spec.worker_id)?;
    let attempt = &spec.attempt_id;
    let Some(rec) = store.get(attempt)? else {
        return Err(SupervisorError::State(format!(
            "attempt {attempt} is unknown"
        )));
    };
    if let Some(t) = rec.termination {
        // Fixed already; a crash may have kept its result from being stored.
        complete_terminal(spec, &store, t)?;
        return Ok(Report {
            termination: Some(t),
            finalized_here: false,
            unresolved: None,
            identity_unknown: false,
            sleep_inhibit: "not needed".into(),
        });
    }
    if rec.state != AttemptState::Ready || !store.launches(attempt)?.is_empty() {
        return super::recover::recover(spec, &store);
    }

    let (_guard, sleep_inhibit) = if spec.inhibit_sleep {
        match platform::inhibit::acquire("remote-task-runner", "running a command") {
            Inhibit::Held(g) => {
                let how = format!("held via {}", g.via());
                (Some(g), how)
            }
            Inhibit::Unavailable(why) => (None, format!("unavailable: {why}")),
        }
    } else {
        (None, "disabled".to_string())
    };
    store.append_event(
        Some(&rec.run_id),
        Some(attempt),
        "sleep_inhibit",
        &json!({ "status": sleep_inhibit }),
        platform::wall_ms(),
    )?;

    let masker = spec.mask.masker();
    // A warning may quote what a tool printed: it is masked whole and cut
    // after, so no part of a secret is left at the cut. The position is the
    // key: two warnings that read the same once masked are both kept, and a
    // second start of this supervisor adds none.
    let warnings: Vec<_> = warning_texts(
        &spec.workspace_warnings,
        MAX_WARNINGS,
        MAX_WARNING_BYTES,
        |w| String::from_utf8_lossy(&masker.mask(w.as_bytes())).into_owned(),
    )
    .into_iter()
    .enumerate()
    .map(|(i, m)| {
        (
            i.to_string(),
            json!({ "index": i.to_string(), "message": m }),
        )
    })
    .collect();
    store.append_events_unseen(
        Some(&rec.run_id),
        attempt,
        "workspace_warning",
        "index",
        &warnings,
        platform::wall_ms(),
    )?;

    let root = spec.spool_dir();
    let spool = match open_spool(spec, &store)? {
        Ok(Spool::Open(s)) => s,
        other => {
            // Nothing is started without a place for its output. A spool
            // that is closed already was closed by a supervisor that died
            // while settling this attempt before any command (a cancel
            // before the start): the attempt is settled through it, which
            // is no spool failure. Any other spool is settled without.
            let reason =
                (rec.cancel_requested_at_ms.is_some() || rec.retired).then_some(StopReason::Cancel);
            if reason.is_some() {
                move_to(&store, attempt, AttemptState::Cancelling)?;
            }
            let term = termination(reason, None);
            let (termination, finalized_here) = match other {
                Ok(closed) => settle(&store, &root, attempt, closed.end(), term)?,
                Err(e) => {
                    settle_without_spool(&store, &root, attempt, "open", &e.to_string(), term)?
                }
            };
            return Ok(Report {
                termination: Some(termination),
                finalized_here,
                unresolved: None,
                identity_unknown: false,
                sleep_inhibit,
            });
        }
    };

    let now = platform::monotonic_ms();
    let applied = store.lease(attempt)?.map(|(k, _)| k);
    let mut ctl = Control {
        spec,
        store: &store,
        traffic: Traffic::open(spec)?,
        lease: LeaseClock::start_seeded(now, spec.lease_ms, spec.poll_ms, applied, spec.lease_seed),
        started_ms: now,
        spool: &spool,
        masker: masker.clone(),
        node_timeout_ms: None,
        beat: SpoolBeat::default(),
        pump_error: Arc::default(),
    };
    let mut end = NodeEnd {
        reason: None,
        exit: Some(Exit::Code(0)),
        unconfirmed: false,
        identity_unknown: false,
    };
    let mut last_node = String::new();
    for (index, node) in spec.nodes.iter().enumerate() {
        last_node = node.node_id.to_string();
        end = ctl.run_node(index, node)?;
        if end.unconfirmed || !may_continue(end.reason, end.exit) {
            break;
        }
    }
    if end.unconfirmed {
        let why = if end.identity_unknown {
            "whose the command's processes are could not be told (an identity could not be \
             read, or nothing left in its group is known to be its own), so nothing was \
             signalled and they could not be confirmed stopped"
        } else {
            "the command's processes could not be confirmed stopped"
        };
        return unresolved_report(
            &store,
            attempt,
            &[last_node.clone()],
            format!("node {last_node}: {why}; the attempt stays unresolved"),
            end.identity_unknown,
            sleep_inhibit,
            Vec::new(),
        );
    }
    // The heartbeat that would have recorded a reader error may not have got
    // through; `run_piped` made the stop reason of it already.
    account_for_reader(&store, attempt, &ctl.beat, ctl.pump_failure())?;
    // Every command that was started has ended and is confirmed stopped.
    // With none started (a cancel got in first) there is nothing to keep.
    let term = if spec.nodes.is_empty() {
        // `prepared_only`: no command, so nothing above looked for a cancel.
        // One that is recorded wins. Otherwise the attempt succeeded by what
        // the template put into its workspace: it has no exit status (no
        // command ended), an empty log, and the artifacts it declared, which
        // are taken from the workspace as after any command.
        let cancelled = store
            .get(attempt)?
            .is_none_or(|r| r.cancel_requested_at_ms.is_some() || r.retired);
        if cancelled {
            move_to(&store, attempt, AttemptState::Cancelling)?;
            termination(Some(StopReason::Cancel), None)
        } else {
            store.append_events_unseen(
                Some(&rec.run_id),
                attempt,
                "prepared_only",
                "what",
                &[(
                    "setup".to_string(),
                    json!({
                        "what": "setup",
                        "message": "every command was a setup step a prepared template \
                                    holds; none was started",
                    }),
                )],
                platform::wall_ms(),
            )?;
            keep_artifacts(spec, &store, &masker)?;
            Termination::new(crate::state::Outcome::Succeeded, None, None)
                .expect("a success without an exit is valid")
        }
    } else {
        if !store.launches(attempt)?.is_empty() {
            keep_artifacts(spec, &store, &masker)?;
        }
        termination(end.reason, end.exit)
    };
    let (termination, finalized_here) = settle(&store, &root, attempt, &spool, term)?;
    Ok(Report {
        termination: Some(termination),
        finalized_here,
        unresolved: None,
        identity_unknown: false,
        sleep_inhibit,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_new_valid_grant_is_noted_even_when_the_diagnostic_cannot_be_written() {
        use crate::ids::CoordinatorEpoch;
        use crate::wire::LeaseKey;
        let key = LeaseKey {
            coordinator_epoch: CoordinatorEpoch(1),
            lease_seq: 1,
        };
        let mut clock = LeaseClock::start(0, 100, 0, None);
        let mut noted = Vec::new();
        let status = observe_lease(&mut clock, 10, Some(key), false, |applied| {
            noted.push(applied);
            // A failed best-effort write has no result to feed back into
            // the clock or the caller's stop decision.
            false
        });
        assert_eq!(status, LeaseStatus::Valid { remaining_ms: 100 });
        assert_eq!(noted, vec![key]);
        assert_eq!(
            observe_lease(&mut clock, 20, Some(key), false, |_| panic!("replay")),
            LeaseStatus::Valid { remaining_ms: 90 }
        );
        let newer = LeaseKey {
            lease_seq: 2,
            ..key
        };
        assert_eq!(
            observe_lease(&mut clock, 110, Some(newer), false, |_| panic!("late")),
            LeaseStatus::Expired
        );
        let mut clock = LeaseClock::start(0, 100, 0, None);
        assert_eq!(
            observe_lease(&mut clock, 10, Some(key), true, |_| panic!(
                "expired in database"
            )),
            LeaseStatus::Expired
        );
    }

    #[test]
    fn the_flag_is_set_when_its_guard_goes_also_by_a_panic() {
        let done = AtomicBool::new(false);
        {
            let _over = SetOnDrop(&done);
            assert!(!done.load(Ordering::SeqCst));
        }
        assert!(done.load(Ordering::SeqCst));
        // A thread that waits for the flag (the output reader does) is not
        // left waiting when the code between panics: the scope returns.
        let done = AtomicBool::new(false);
        let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            std::thread::scope(|s| {
                s.spawn(|| {
                    while !done.load(Ordering::SeqCst) {
                        std::thread::sleep(Duration::from_millis(1));
                    }
                });
                let _over = SetOnDrop(&done);
                panic!("the supervisor's own fault");
            })
        }));
        assert!(panicked.is_err());
        assert!(done.load(Ordering::SeqCst));
    }

    #[test]
    fn a_busy_database_is_absorbed_and_other_errors_are_not() {
        let busy = Err(SupervisorError::Storage(StorageError::Busy));
        assert!(matches!(absorb_retryable(busy), Ok(None)));
        let seen = Ok(Some(StopReason::Cancel));
        assert!(matches!(
            absorb_retryable(seen),
            Ok(Some(StopReason::Cancel))
        ));
        let fault = Err(SupervisorError::State("x".into()));
        assert!(matches!(
            absorb_retryable(fault),
            Err(SupervisorError::State(_))
        ));
        let other = Err(SupervisorError::Storage(StorageError::Retired(
            crate::ids::AttemptId::generate(),
        )));
        assert!(matches!(
            absorb_retryable(other),
            Err(SupervisorError::Storage(_))
        ));
    }
}

#[cfg(test)]
mod identity_tests {
    use super::*;

    fn target(pid: u32, leader: Leader) -> StopTarget {
        StopTarget {
            pid,
            pgid: pid as i32,
            leader,
        }
    }

    #[test]
    fn a_stop_that_could_not_tell_the_identity_is_left_to_be_tried_again() {
        let mut w = Watch::default();
        assert!(w.after_stop(StopResult::IdentityUnknown), "said once");
        assert!(!w.stopped && !w.unconfirmed && !w.identity_unknown);
        w.deferred_noted = true;
        assert!(!w.after_stop(StopResult::IdentityUnknown), "not said twice");
        assert!(!w.stopped && !w.unconfirmed);
        // A later look that can read the identity finishes the stop.
        assert!(!w.after_stop(StopResult::Confirmed));
        assert!(w.stopped && !w.unconfirmed);
    }

    #[test]
    fn a_stop_that_could_not_be_confirmed_is_not_tried_again_by_the_watch() {
        let mut w = Watch::default();
        assert!(!w.after_stop(StopResult::Unconfirmed));
        assert!(w.stopped && w.unconfirmed && !w.identity_unknown);
    }

    #[test]
    fn leftovers_with_an_unknown_identity_leave_the_node_unconfirmed() {
        let mut w = Watch::default();
        assert!(!w.after_leftover_stop(StopResult::Confirmed));
        assert!(!w.unconfirmed && !w.identity_unknown);
        assert!(!w.after_leftover_stop(StopResult::Unconfirmed));
        assert!(w.unconfirmed && !w.identity_unknown);
        let mut w = Watch::default();
        assert!(w.after_leftover_stop(StopResult::IdentityUnknown));
        assert!(w.unconfirmed && w.identity_unknown);
    }

    #[test]
    fn a_later_look_never_replaces_a_leader_that_ended_or_is_known() {
        let ended = target(0x7fff_fff0, Leader::Ended);
        let known = target(0x7fff_fff0, Leader::Known("linux:5".into()));
        let unread = target(0x7fff_fff0, Leader::Unread);
        let other = target(0x7fff_fff0, Leader::Known("linux:9".into()));
        // The PID was seen free (or with the command): what has it later is
        // not the command.
        for old in [&ended, &known] {
            assert_eq!(
                refreshed(Some(old.clone()), Some(other.clone())),
                Some(old.clone())
            );
            assert_eq!(refreshed(Some(old.clone()), None), Some(old.clone()));
        }
        // A read that failed is tried again, and whatever it says is taken.
        for new in [&ended, &known, &unread] {
            assert_eq!(
                refreshed(Some(unread.clone()), Some(new.clone())),
                Some(new.clone())
            );
        }
        assert_eq!(refreshed(Some(unread.clone()), None), Some(unread.clone()));
        assert_eq!(refreshed(None, Some(known.clone())), Some(known.clone()));
        assert_eq!(refreshed(None, None), None);
        // Another child (another PID) is another target.
        let next = target(0x7fff_fff1, Leader::Known("linux:9".into()));
        assert_eq!(
            refreshed(Some(ended.clone()), Some(next.clone())),
            Some(next)
        );
    }

    #[test]
    fn a_reaped_leader_keeps_only_the_identity_read_while_it_ran() {
        let known = target(0x7fff_fff0, Leader::Known("linux:5".into()));
        assert_eq!(
            reaped_leader(Some(&known), 0x7fff_fff0),
            Leader::Known("linux:5".into())
        );
        // Another PID, a leader that was never read, or nothing seen: all
        // that is known is that the command ended.
        assert_eq!(reaped_leader(Some(&known), 0x7fff_fff1), Leader::Ended);
        assert_eq!(
            reaped_leader(Some(&target(0x7fff_fff0, Leader::Unread)), 0x7fff_fff0),
            Leader::Ended
        );
        assert_eq!(
            reaped_leader(Some(&target(0x7fff_fff0, Leader::Ended)), 0x7fff_fff0),
            Leader::Ended
        );
        assert_eq!(reaped_leader(None, 0x7fff_fff0), Leader::Ended);
    }
}
