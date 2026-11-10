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
//! reported as a success ([`after_spool_failure`]).
//!
//! When the command's processes cannot be confirmed stopped (even after a few
//! bounded retries), the attempt is left non-terminal and the process exits
//! with `UNRESOLVED` after recording a `supervisor_unresolved` event (reason
//! and node). Trying again is the parent's job, not this process's: the
//! parent's reconcile sees the supervisor lock free and the attempt not
//! terminal, starts `supervise` again, and [`super::recover::recover`] redoes
//! the stop of the group (S10/S12 implement that loop).

use super::error::{Result, SupervisorError};
use super::outcome::{finish_attempt, sync_log_position, OutcomeError};
use super::pump::{pump, Pipe};
use super::spec::{command_id, spec_digest, NodeSpec, SuperviseSpec};
use super::spool::{AttemptSpool, SpoolError};
use super::spool_plan::{attempt_limit, warning_texts, MAX_WARNINGS, MAX_WARNING_BYTES};
use super::stop::{
    group_alive, retries_for, stop_group_retrying, StopResult, StopTarget, StopTimes,
};
use super::termination::{after_spool_failure, may_continue, pick_reason, termination, StopReason};
use super::{lease::LeaseClock, lease::LeaseStatus, timeouts};
use crate::ids::{AttemptId, NodeId, WorkerBootId};
use crate::platform::{self, Inhibit};
use crate::state::{AttemptState, Exit, Receipt, Termination};
use crate::storage::{
    Finalized, LaunchClaim, LogPosition, ProcessIdentity, StorageError, WorkerStore,
};
use crate::wire::LogStream;
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
pub(crate) fn stop_confirmed(t: &StopTarget, spec: &SuperviseSpec) -> StopResult {
    stop_group_retrying(t, stop_times(spec), retries_for(spec.kill_wait_ms))
}

/// A retryable storage error is not a fault: the same look may work next time.
pub(crate) fn absorb_retryable(r: Result<Option<StopReason>>) -> Result<Option<StopReason>> {
    match r {
        Err(SupervisorError::Storage(e)) if e.is_retryable() => Ok(None),
        other => other,
    }
}

/// The report for an attempt that cannot be settled, after recording why
/// (and for which nodes) in the worker database.
pub(crate) fn unresolved_report(
    store: &WorkerStore,
    attempt: &crate::ids::AttemptId,
    nodes: &[String],
    reason: String,
    sleep_inhibit: String,
) -> Result<Report> {
    if let Some(a) = store.get(attempt)? {
        store.append_event(
            Some(&a.run_id),
            Some(attempt),
            "supervisor_unresolved",
            &json!({ "reason": reason, "nodes": nodes }),
            platform::wall_ms(),
        )?;
    }
    Ok(Report {
        termination: None,
        finalized_here: false,
        unresolved: Some(reason),
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

/// Open (or, after a crash, resume) the attempt's spool with what the task's
/// limit leaves it: the limit less what the task's other attempts stored.
/// The inner error is the spool's own: the caller settles the attempt
/// without a spool then ([`settle_without_spool`]).
pub(crate) fn open_spool(
    spec: &SuperviseSpec,
    store: &WorkerStore,
) -> Result<std::result::Result<AttemptSpool, SpoolError>> {
    let others = store.log_bytes_of_other_attempts(&spec.attempt_id)?;
    let limit = attempt_limit(spec.log_limit_bytes, others);
    Ok(AttemptSpool::open(
        &spec.spool_root,
        &spec.attempt_id,
        limit,
    ))
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
pub(crate) fn settle(
    store: &WorkerStore,
    root: &Path,
    attempt: &AttemptId,
    spool: &AttemptSpool,
    term: Termination,
) -> Result<(Termination, bool)> {
    to_finalizing(store, attempt)?;
    match finish_attempt(store, root, attempt, spool, term, &[], platform::wall_ms()) {
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
            settle_without_spool(store, attempt, "finish", &e.to_string(), term)
        }
        Err(OutcomeError::Storage(e)) => Err(e.into()),
        Err(OutcomeError::Io(e)) => Err(e.into()),
        Err(e) => Err(SupervisorError::State(e.to_string())),
    }
}

/// Settle an attempt whose spool failed at `stage`: say so in the journal and
/// fix what [`after_spool_failure`] makes of `term` (never a success). No
/// result is stored: there is no complete output to describe.
pub(crate) fn settle_without_spool(
    store: &WorkerStore,
    attempt: &AttemptId,
    stage: &str,
    why: &str,
    term: Termination,
) -> Result<(Termination, bool)> {
    note_spool_failure(store, attempt, stage, why)?;
    let fin = finalize(store, attempt, after_spool_failure(term))?;
    Ok((fin.termination(), fin.won()))
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

struct Control<'a> {
    spec: &'a SuperviseSpec,
    store: &'a WorkerStore,
    traffic: Traffic,
    lease: LeaseClock,
    started_ms: u64,
    spool: &'a AttemptSpool,
    beat: SpoolBeat,
    /// The first failure of a thread feeding the spool.
    pump_error: Arc<Mutex<Option<String>>>,
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
        let status = self.lease.observe(now, key, db_expired);
        let expired = status == LeaseStatus::Expired;
        if expired && !db_expired {
            // Persist it: from now on a renewal is refused, and no new node
            // can be claimed.
            self.store.expire_lease(attempt, platform::wall_ms())?;
        }
        let timeout = timeouts::fired(&self.spec.timeout_policy(), self.started_ms, stage, now);
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
        let attempt = spec.attempt_id.clone();
        let id = command_id(&attempt, index);
        // The pipe the command's output comes through. Without it nothing is
        // registered or started.
        let pipe = match Pipe::open(&Pipe::path_for(&spec.spool_root, &attempt, index)) {
            Ok(p) => p,
            Err(e) => {
                let why = format!("the output pipe of node {}: {e}", node.node_id);
                note_spool_failure(self.store, &attempt, "pipe", &why)?;
                self.beat.failure = Some(why);
                return Ok(NodeEnd {
                    reason: Some(StopReason::SpoolFailed),
                    exit: None,
                    unconfirmed: false,
                });
            }
        };
        let opts = Opts {
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
            let feeder = s.spawn(|| pump(pipe, out, &done, &error));
            let end = self.run_admitted(guard, opts, node, id, limits);
            // The command is over (or given up on): what it wrote is read to
            // the end, within a bound.
            done.store(true, Ordering::SeqCst);
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
        if self.beat.failure.is_some() {
            // Output was lost: whatever the command did, nothing follows it.
            end.reason.get_or_insert(StopReason::SpoolFailed);
        }
        Ok(end)
    }

    fn heartbeat(&mut self) -> Result<()> {
        let pump_error = self
            .pump_error
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
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
                    watch.stopped = true;
                    if stop_confirmed(t, self.spec) == StopResult::Unconfirmed {
                        watch.unconfirmed = true;
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
        // pgid, and judged by the group alone.
        let leftover = child_target(&ctx, id)
            .or_else(|| watch.target.take())
            .map(|t| StopTarget {
                start_identity: None,
                ..t
            });
        if let (Some(t), false, false) = (&leftover, watch.recorded, watch.noted) {
            // The command ended before the watch saw it at all.
            self.note_not_recorded(&node.node_id, t.pid)?;
        }
        if let Some(t) = &leftover {
            if group_alive(t) && stop_confirmed(t, self.spec) == StopResult::Unconfirmed {
                watch.unconfirmed = true;
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
            w.target = child_target(ctx, id).or(w.target.take());
        }
        if let (Some(t), false) = (&w.target, w.recorded) {
            if self.record_child(&node.node_id, t)? {
                w.recorded = true;
            } else if !w.noted {
                self.note_not_recorded(&node.node_id, t.pid)?;
                w.noted = true;
            }
        }
        self.heartbeat()?;
        if w.reason.is_none() && self.beat.failure.is_some() {
            // The output can no longer be stored: the command is stopped
            // rather than left to run with its output lost.
            w.reason = Some(StopReason::SpoolFailed);
            self.begin_stop(StopReason::SpoolFailed)?;
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
        let Some(start) = t.start_identity.clone() else {
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
        let why = if platform::process_start_identity(pid).is_none() {
            "the command ended before its identity could be read"
        } else {
            "the start identity of the command could not be read"
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
    fatal: Option<SupervisorError>,
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

/// The child of the registered command `id` as a stop target. A child that
/// has already ended (or whose identity cannot be read) has no start
/// identity, but its group is still a target.
fn child_target(ctx: &Context, id: &str) -> Option<StopTarget> {
    let (pid, pgid) = sidecar_child(ctx, id)?;
    Some(StopTarget {
        pid,
        pgid,
        start_identity: platform::process_start_identity(pid),
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
        return Ok(Report {
            termination: Some(t),
            finalized_here: false,
            unresolved: None,
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

    let warnings: Vec<_> = warning_texts(&spec.workspace_warnings, MAX_WARNINGS, MAX_WARNING_BYTES)
        .into_iter()
        .map(|m| (m.clone(), json!({ "message": m })))
        .collect();
    store.append_events_unseen(
        Some(&rec.run_id),
        attempt,
        "workspace_warning",
        "message",
        &warnings,
        platform::wall_ms(),
    )?;

    let spool = match open_spool(spec, &store)? {
        Ok(s) => s,
        Err(e) => {
            // Nothing is started without a place for its output.
            let reason =
                (rec.cancel_requested_at_ms.is_some() || rec.retired).then_some(StopReason::Cancel);
            if reason.is_some() {
                move_to(&store, attempt, AttemptState::Cancelling)?;
            }
            let (termination, finalized_here) = settle_without_spool(
                &store,
                attempt,
                "open",
                &e.to_string(),
                termination(reason, None),
            )?;
            return Ok(Report {
                termination: Some(termination),
                finalized_here,
                unresolved: None,
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
        lease: LeaseClock::start(now, spec.lease_ms, spec.poll_ms, applied),
        started_ms: now,
        spool: &spool,
        beat: SpoolBeat::default(),
        pump_error: Arc::default(),
    };
    let mut end = NodeEnd {
        reason: None,
        exit: Some(Exit::Code(0)),
        unconfirmed: false,
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
        return unresolved_report(
            &store,
            attempt,
            &[last_node.clone()],
            format!(
                "node {last_node}: the command's processes could not be confirmed \
                 stopped; the attempt stays unresolved"
            ),
            sleep_inhibit,
        );
    }
    let term = termination(end.reason, end.exit);
    let (termination, finalized_here) = settle(&store, &spec.spool_root, attempt, &spool, term)?;
    Ok(Report {
        termination: Some(termination),
        finalized_here,
        unresolved: None,
        sleep_inhibit,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

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
