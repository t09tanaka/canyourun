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

use super::error::{Result, SupervisorError};
use super::spec::{command_id, spec_digest, NodeSpec, SuperviseSpec};
use super::stop::{group_alive, stop_group, StopResult, StopTarget, StopTimes};
use super::termination::{may_continue, pick_reason, termination, StopReason};
use super::{lease::LeaseClock, lease::LeaseStatus, timeouts};
use crate::ids::{NodeId, WorkerBootId};
use crate::platform::{self, Inhibit};
use crate::state::{AttemptState, Exit, Receipt, Termination};
use crate::storage::{Finalized, LaunchClaim, ProcessIdentity, StorageError, WorkerStore};
use serde_json::json;
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
    /// Why the attempt was left unresolved, when it was.
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

/// Settle the attempt: move it to `Finalizing` (through `Cancelling` first
/// for a cancel, which the caller did earlier) and fix the termination once.
pub(crate) fn finalize(
    store: &WorkerStore,
    attempt: &crate::ids::AttemptId,
    term: Termination,
) -> Result<Finalized> {
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
    Ok(store.finalize(attempt, term, platform::wall_ms())?)
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
        let opts = Opts {
            weight: node.weight,
            front: false,
            key: None,
            argv: node.argv.clone(),
            cwd: node.cwd.clone(),
            env: Some(node.env.clone()),
            stdout_log: node.stdout_log.clone(),
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
            if let Some(reason) = self.look(stage)? {
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
        self.run_admitted(guard, &opts, node, &id, limits)
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
        let times = stop_times(self.spec);
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
                    if stop_group(t, times) == StopResult::Unconfirmed {
                        watch.unconfirmed = true;
                    }
                }
                std::thread::sleep(Duration::from_millis(self.spec.poll_ms));
            }
            handle.join()
        });
        let ran = ran.map_err(|_| SupervisorError::State("the command runner panicked".into()))?;
        let exit = ran.as_ref().ok().and_then(|r| exit_of(r.outcome.exit));
        // The leader is gone; whatever it left in its group is stopped too
        // before anything is recorded.
        if watch.target.is_none() {
            watch.target = child_target(&ctx, id);
        }
        if let Some(t) = &watch.target {
            if group_alive(t) && stop_group(t, times) == StopResult::Unconfirmed {
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
        if w.target.is_none() {
            w.target = child_target(ctx, id);
        }
        if let (Some(t), false) = (&w.target, w.recorded) {
            self.record_child(&node.node_id, t)?;
            w.recorded = true;
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
    /// the attempt as running.
    fn record_child(&self, node: &NodeId, t: &StopTarget) -> Result<()> {
        let attempt = &self.spec.attempt_id;
        let boot = platform::boot_id()
            .and_then(|b| WorkerBootId::parse(&b).ok())
            .ok_or_else(|| SupervisorError::State("the boot id is not readable".into()))?;
        let Some(start) = t.start_identity.clone() else {
            return Ok(());
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
        Ok(())
    }
}

/// What the watch loop has learned about a running command.
#[derive(Default)]
struct Watch {
    target: Option<StopTarget>,
    recorded: bool,
    reason: Option<StopReason>,
    stopped: bool,
    unconfirmed: bool,
    fatal: Option<SupervisorError>,
}

/// The running child of the registered command `id`, once
/// task-traffic-controller has recorded it. A child that has already exited
/// has no start identity and is not a target.
fn child_target(ctx: &Context, id: &str) -> Option<StopTarget> {
    let Loaded::Ok(sidecar) = managed::load_sidecar(ctx, id) else {
        return None;
    };
    let child = sidecar.child?;
    let start = platform::process_start_identity(child.pid)?;
    Some(StopTarget {
        pid: child.pid,
        pgid: child.pgid.unwrap_or(child.pid as i32),
        start_identity: Some(start),
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

    let now = platform::monotonic_ms();
    let applied = store.lease(attempt)?.map(|(k, _)| k);
    let mut ctl = Control {
        spec,
        store: &store,
        traffic: Traffic::open(spec)?,
        lease: LeaseClock::start(now, spec.lease_ms, applied),
        started_ms: now,
    };
    let mut end = NodeEnd {
        reason: None,
        exit: Some(Exit::Code(0)),
        unconfirmed: false,
    };
    for (index, node) in spec.nodes.iter().enumerate() {
        end = ctl.run_node(index, node)?;
        if end.unconfirmed || !may_continue(end.reason, end.exit) {
            break;
        }
    }
    if end.unconfirmed {
        return Ok(Report {
            termination: None,
            finalized_here: false,
            unresolved: Some(
                "the command's processes could not be confirmed stopped; \
                 the attempt stays unresolved"
                    .into(),
            ),
            sleep_inhibit,
        });
    }
    let term = termination(end.reason, end.exit);
    let fin = finalize(&store, attempt, term)?;
    let (termination, finalized_here) = match fin {
        Finalized::Won(t) => (t, true),
        Finalized::AlreadyTerminal(t) => (t, false),
    };
    Ok(Report {
        termination: Some(termination),
        finalized_here,
        unresolved: None,
        sleep_inhibit,
    })
}
