//! The worker's `forecast` operation (spec chapters 9 and 11): when would a
//! workload start and finish here, if it were queued now?
//!
//! The simulation is task-traffic-controller's. This module lays the overlay
//! on top of the ledger (what the worker accepted but has not registered,
//! what the coordinator is still sending), labels every estimate, and answers
//! in the wire types. It is read-only: it reads the worker's accepted
//! attempts, the ledger without its lock and the sidecars, and writes
//! nothing, so asking a hundred times adds no queue entry, lease or job.
//!
//! - [`plan`]: the decisions, free of I/O (coverage and mutation gates)
//! - this file: the gathering, in the order that cannot lose a demand

pub mod plan;

use crate::error::{ApiError, ErrorCode};
use crate::ids::CoordinatorEpoch;
use crate::storage::AcceptedAttempt;
use crate::wire::{AttemptPayload, ForecastRequest, ForecastResponse};
use plan::{AcceptedNode, Defaults, Ident, Wiring};
use std::path::Path;
use task_traffic_controller::config::Config;
use task_traffic_controller::context::Context;
use task_traffic_controller::forecast as ttc;
use task_traffic_controller::inspect::{inspect_with, Inspection};
use task_traffic_controller::managed::{self, Loaded};
use task_traffic_controller::paths::Paths;

fn internal(what: &str, e: impl std::fmt::Display) -> ApiError {
    ApiError::new(ErrorCode::Internal, format!("{what}: {e}"))
}

/// Simulate the request on a given snapshot. No I/O besides the clock.
///
/// Returns the simulation's report of what became of each overlay demand
/// (applied, replaced by the ledger, removed as finished, dropped as a
/// duplicate) next to the answer.
pub fn compute(
    ctx: &Context,
    insp: &Inspection,
    default_weight: u32,
    ident: &Ident,
    req: &ForecastRequest,
    accepted: &[AcceptedNode],
) -> Result<(ForecastResponse, ttc::OverlayReport), ApiError> {
    plan::check_request(req).map_err(plan::RequestError::into_api)?;
    let defaults = Defaults {
        weight: default_weight,
    };
    let (target, mut notes) = plan::target_plan(&req.workload, defaults);
    if accepted.iter().any(|a| !a.terminal) {
        // Not `soft`: it only makes the answer later, never earlier.
        notes.note(
            "dependencies inside accepted attempts are ignored; all their unfinished nodes count as demand ahead (over-counts)"
                .into(),
        );
    }
    let overlay = plan::merge_overlay(&req.workload, &req.pending, accepted, defaults);
    let fc = ttc::forecast(ctx, insp, &target, &overlay).map_err(|e| match e {
        task_traffic_controller::error::Error::Plan(m) => ApiError::new(ErrorCode::InvalidInput, m),
        other => internal("forecast", other),
    })?;
    let answer = plan::to_wire(
        &fc,
        &Wiring {
            ident,
            plan: &target,
            target_digest: &req.workload.plan_digest,
            insp,
            overlay: &overlay,
            notes: &notes,
        },
    )
    .map_err(|e| internal("forecast answer", e.0))?;
    Ok((answer, fc.overlay))
}

/// The nodes of the attempts the worker has not finished. An attempt whose
/// record cannot be read is an error: leaving its demand out would show a
/// busy worker as free.
fn accepted_of(attempts: &[AcceptedAttempt]) -> Result<Vec<AcceptedNode>, ApiError> {
    let mut out = Vec::new();
    for rec in attempts {
        let payload: AttemptPayload = serde_json::from_value(rec.spec.clone())
            .map_err(|e| internal(&format!("attempt {} payload", rec.attempt_id), e))?;
        let nodes = plan::accepted_nodes(&rec.attempt_id, &payload)
            .map_err(|e| internal(&format!("attempt {}", rec.attempt_id), e))?;
        out.extend(nodes);
    }
    Ok(out)
}

/// The three reads, in the one order that cannot lose a demand: the accepted
/// attempts, then the ledger snapshot, then the sidecar of each accepted
/// node. They are arguments so a test can record the order they are called
/// in; swapping any two reintroduces a window in which a node is in neither
/// the ledger nor the overlay, or counted from a finished attempt.
fn gather(
    accepted: impl FnOnce() -> Result<Vec<AcceptedNode>, ApiError>,
    ledger: impl FnOnce() -> Result<Inspection, ApiError>,
    mut sidecar: impl FnMut(&mut AcceptedNode),
) -> Result<(Vec<AcceptedNode>, Inspection), ApiError> {
    let mut nodes = accepted()?;
    let insp = ledger()?;
    nodes.iter_mut().for_each(&mut sidecar);
    Ok((nodes, insp))
}

/// Answer one `forecast` request from the worker's state.
///
/// Reading order: the accepted attempts first, then the ledger snapshot,
/// then which accepted nodes have finished. A node registered between the
/// first two reads is in the ledger (replaces its overlay demand); a node
/// that finished between the last two is dropped (no phantom demand). No
/// order of reads leaves a node in neither, and a node is counted once.
///
/// `attempts` are the attempts that are not terminal, `max_epoch` the highest
/// coordinator epoch the worker has seen (a request below it is stale).
pub fn answer_with(
    ctx: &Context,
    cfg: &Config,
    ident: &Ident,
    attempts: &[AcceptedAttempt],
    max_epoch: CoordinatorEpoch,
    req: &ForecastRequest,
) -> Result<ForecastResponse, ApiError> {
    if req.coordinator_epoch < max_epoch {
        return Err(ApiError::new(
            ErrorCode::StaleEpoch,
            format!(
                "coordinator epoch {} is behind the worker's {}",
                req.coordinator_epoch, max_epoch
            ),
        ));
    }
    // Refuse an oversized request before any file is read.
    plan::check_request(req).map_err(plan::RequestError::into_api)?;
    let (accepted, insp) = gather(
        || accepted_of(attempts),
        || inspect_with(ctx, cfg).map_err(|e| internal("inspect", e)),
        |node| {
            // A sidecar that cannot be read keeps the demand.
            if let Loaded::Ok(sc) = managed::load_sidecar(ctx, &node.command_id) {
                node.terminal = sc.terminal.is_some();
            }
        },
    )?;
    compute(ctx, &insp, cfg.default_weight, ident, req, &accepted).map(|(a, _)| a)
}

/// [`answer_with`] on the task-traffic-controller state the worker's
/// supervisors use.
pub fn answer(
    traffic_state_dir: &Path,
    traffic_config: Option<&Path>,
    ident: &Ident,
    attempts: &[AcceptedAttempt],
    max_epoch: CoordinatorEpoch,
    req: &ForecastRequest,
) -> Result<ForecastResponse, ApiError> {
    let config_path = traffic_config
        .map(Path::to_path_buf)
        .unwrap_or_else(|| traffic_state_dir.join("config.toml"));
    let paths = Paths::new(traffic_state_dir, config_path);
    let cfg = Config::load(paths.config_file()).map_err(|e| internal("traffic config", e))?;
    answer_with(&Context::new(paths), &cfg, ident, attempts, max_epoch, req)
}

#[cfg(test)]
mod tests {
    //! A07 to A10 on hand-built snapshots (no process, no file): the same
    //! simulation task-traffic-controller runs, fed with exact numbers.
    use super::*;
    use crate::ids::{AgentEpoch, AttemptId, BuilderId, Digest, NodeId, WorkerBootId, WorkerId};
    use crate::wire::{
        ConcurrencyLimits, Correlation, ForecastField, RuntimeSource, SchemaVersion, Uncertainty,
        WaitReason, WorkloadNode, WorkloadPlan,
    };
    use chrono::{DateTime, Duration as Cd, Utc};
    use std::sync::Arc;
    use task_traffic_controller::admit::{Limits, LiveSnapshot, State};
    use task_traffic_controller::context::{FakeClock, FixedLive};
    use task_traffic_controller::inspect::{InspectedEntry, SCHEMA_VERSION};
    use task_traffic_controller::ledger::Entry;

    fn t0() -> DateTime<Utc> {
        "2030-01-01T00:00:00Z".parse().unwrap()
    }

    fn ctx() -> Context {
        Context::new(Paths::new("/nonexistent/state", "/nonexistent/config.toml"))
            .with_clock(Arc::new(FakeClock::new(t0())))
            .with_live(Arc::new(FixedLive(None)))
    }

    fn ident() -> Ident {
        Ident {
            worker_id: WorkerId::parse("w-test").unwrap(),
            boot_id: WorkerBootId::parse("boot-1").unwrap(),
            agent_epoch: AgentEpoch(3),
            forecast_ttl_ms: 2000,
        }
    }

    fn calm() -> Option<LiveSnapshot> {
        Some(LiveSnapshot {
            available_memory: 1 << 40,
            cpu_busy: 0.1,
            load_avg: 0.0,
        })
    }

    fn insp(budget: u32, live: Option<LiveSnapshot>, entries: Vec<InspectedEntry>) -> Inspection {
        Inspection {
            schema_version: SCHEMA_VERSION,
            taken_at: t0(),
            snapshot_digest: Digest::of_bytes(b"ledger").to_string(),
            policy_digest: Digest::of_bytes(b"policy").to_string(),
            limits: Limits {
                budget,
                memory_reserve: 1 << 30,
                cpu_threshold: 0.9,
                cpu_guard_max_wait_secs: 30.0,
                front_overload_load: 0.0,
                front_overload_max_wait_secs: 10.0,
                backfill_max_job_secs: 0.0,
                backfill_max_wait_secs: 60.0,
            },
            live,
            ledger_locked: true,
            entries,
            history: Vec::new(),
            history_quality: Vec::new(),
        }
    }

    /// A running ledger entry of `weight` that started `elapsed` seconds ago
    /// with a learned wall time of `expected`.
    fn running(id: &str, weight: u32, expected: Option<f64>, elapsed: i64) -> InspectedEntry {
        InspectedEntry {
            entry: Entry {
                label: None,
                id: id.into(),
                pid: 1,
                state: State::Running,
                front: false,
                weight,
                predicted_rss: 1,
                expected_secs: expected,
                repo: String::new(),
                subdir: String::new(),
                argv: id.into(),
                key: id.into(),
                seq: 1,
                enqueued_at: t0() - Cd::seconds(elapsed),
                started_at: Some(t0() - Cd::seconds(elapsed)),
                using: None,
                backfilled: false,
            },
            suspect_dead: false,
        }
    }

    fn node(attempt: &AttemptId, id: &str, runtime_ms: Option<u64>) -> WorkloadNode {
        WorkloadNode {
            node_id: NodeId::parse(id).unwrap(),
            depends_on: vec![],
            priority: 0,
            weight: Some(1),
            predicted_rss_bytes: Some(1),
            predicted_runtime_ms: runtime_ms,
            external_delay_ms: None,
            ready_in_ms: Some(0),
            concurrency: ConcurrencyLimits::default(),
            correlation: Correlation {
                attempt_id: attempt.clone(),
                node_id: NodeId::parse(id).unwrap(),
                ledger_entry: None,
            },
            shared_builder: None,
        }
    }

    fn plan_of(attempt: &AttemptId, nodes: Vec<WorkloadNode>) -> WorkloadPlan {
        WorkloadPlan {
            attempt_id: attempt.clone(),
            plan_digest: Digest::of_bytes(attempt.as_str().as_bytes()),
            expansion_seq: 0,
            nodes,
        }
    }

    fn request(workload: WorkloadPlan, pending: Vec<WorkloadPlan>) -> ForecastRequest {
        ForecastRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: crate::ids::RequestId::generate(),
            coordinator_epoch: CoordinatorEpoch(1),
            workload,
            pending,
        }
    }

    fn run(
        insp: &Inspection,
        req: &ForecastRequest,
        accepted: &[AcceptedNode],
    ) -> (ForecastResponse, ttc::OverlayReport) {
        compute(&ctx(), insp, 2, &ident(), req, accepted).unwrap()
    }

    fn accepted_node(attempt: &AttemptId, id: &str, index: usize, terminal: bool) -> AcceptedNode {
        AcceptedNode {
            attempt_id: attempt.clone(),
            plan_digest: Digest::of_bytes(attempt.as_str().as_bytes()),
            node_id: NodeId::parse(id).unwrap(),
            command_id: crate::supervisor::spec::command_id(attempt, index),
            terminal,
        }
    }

    /// Every missing number has a reason and every present one has none.
    fn assert_reasons_match(a: &ForecastResponse) {
        let t = &a.target;
        for (field, value) in [
            (ForecastField::ReadyInMs, t.ready_in_ms),
            (
                ForecastField::QueueWaitAfterReadyMs,
                t.queue_wait_after_ready_ms,
            ),
            (ForecastField::ExecutionMs, t.execution_ms),
            (ForecastField::FinishInMs, t.finish_in_ms),
        ] {
            assert_eq!(
                value.is_none(),
                t.null_reasons.contains_key(&field),
                "{field:?}: {t:?}"
            );
        }
    }

    #[test]
    fn a07_ready_delay_queue_and_runtime_make_the_finish() {
        let a = AttemptId::generate();
        // The one slot is taken for another 30 s; the node is ready at 10 s
        // and runs 20 s: it waits 20 s after it is ready, and ends at 50 s.
        let i = insp(1, calm(), vec![running("busy", 1, Some(40.0), 10)]);
        let mut n = node(&a, "main", Some(20_000));
        n.ready_in_ms = Some(10_000);
        let (ans, _) = run(&i, &request(plan_of(&a, vec![n]), vec![]), &[]);
        let t = &ans.target;
        assert_eq!(t.ready_in_ms, Some(10_000));
        assert_eq!(t.queue_wait_after_ready_ms, Some(20_000));
        assert_eq!(t.execution_ms, Some(20_000));
        assert_eq!(t.finish_in_ms, Some(50_000));
        assert_eq!(t.runtime_source, RuntimeSource::Measured);
        assert!(t.null_reasons.is_empty());
        assert!(!t.overrun);
        assert_eq!(ans.ttl_ms, 2000);
        assert_eq!(ans.agent_epoch, AgentEpoch(3));
        assert_eq!(ans.worker_boot_id, WorkerBootId::parse("boot-1").unwrap());
    }

    #[test]
    fn a08_an_accepted_node_is_counted_once_in_every_state() {
        let a = AttemptId::generate();
        let b = AttemptId::generate();
        // `b/work` takes the only slot for 20 s; the target runs 10 s after it.
        let mut work = node(&b, "work", Some(20_000));
        let target = plan_of(&a, vec![node(&a, "main", Some(10_000))]);
        let req_of = |pending: Vec<WorkloadPlan>| request(target.clone(), pending);
        let finish = |ans: &ForecastResponse| ans.target.finish_in_ms;

        // Accepted, not registered: the demand is the overlay's.
        let acc = accepted_node(&b, "work", 0, false);
        let (ans, report) = run(
            &insp(1, calm(), vec![]),
            &req_of(vec![plan_of(&b, vec![work.clone()])]),
            std::slice::from_ref(&acc),
        );
        assert_eq!(finish(&ans), Some(30_000));
        assert_eq!(report.applied, vec![format!("{b}/work")]);

        // The same node, sent by the coordinator only (the worker has not
        // accepted it yet): the same answer.
        let (ans, report) = run(
            &insp(1, calm(), vec![]),
            &req_of(vec![plan_of(&b, vec![work.clone()])]),
            &[],
        );
        assert_eq!(finish(&ans), Some(30_000));
        assert_eq!(report.applied.len(), 1);

        // Registered: the ledger entry replaces the overlay demand; the
        // number does not change and nothing is counted twice.
        let entry = running(&acc.command_id, 1, Some(20.0), 0);
        let (ans, report) = run(
            &insp(1, calm(), vec![entry.clone()]),
            &req_of(vec![plan_of(&b, vec![work.clone()])]),
            std::slice::from_ref(&acc),
        );
        assert_eq!(finish(&ans), Some(30_000));
        assert!(report.applied.is_empty());
        assert_eq!(report.replaced_by_ledger, vec![format!("{b}/work")]);

        // The worker alone knows it (no pending copy): same again, with the
        // worker's default numbers for a node it knows nothing about.
        let (ans, report) = run(
            &insp(1, calm(), vec![entry]),
            &req_of(vec![]),
            std::slice::from_ref(&acc),
        );
        assert_eq!(report.replaced_by_ledger.len(), 1);
        assert_eq!(finish(&ans), Some(10_000 + 20_000));

        // Finished: no demand at all.
        let done = accepted_node(&b, "work", 0, true);
        let (ans, report) = run(
            &insp(1, calm(), vec![]),
            &req_of(vec![plan_of(&b, vec![work.clone()])]),
            &[done],
        );
        assert_eq!(finish(&ans), Some(10_000));
        assert_eq!(report.terminal_removed, vec![format!("{b}/work")]);

        // The requested workload is never also demand: a node of it that the
        // worker has accepted, or that is pending, is the plan's.
        work.correlation.attempt_id = a.clone();
        let mut same = node(&a, "main", Some(10_000));
        same.correlation.ledger_entry = None;
        let dup = accepted_node(&a, "main", 0, false);
        let (ans, report) = run(
            &insp(1, calm(), vec![]),
            &req_of(vec![plan_of(&a, vec![same])]),
            &[dup],
        );
        assert_eq!(finish(&ans), Some(10_000));
        assert!(report.applied.is_empty());
    }

    #[test]
    fn a08_coverage_names_the_attempts_and_nodes_counted() {
        let a = AttemptId::generate();
        let b = AttemptId::generate();
        let pending = plan_of(&b, vec![node(&b, "work", Some(5_000))]);
        let req = request(
            plan_of(&a, vec![node(&a, "main", Some(1_000))]),
            vec![pending.clone()],
        );
        let (ans, _) = run(&insp(4, calm(), vec![]), &req, &[]);
        let covered: Vec<(String, String)> = ans
            .coverage
            .iter()
            .flat_map(|c| {
                c.node_ids
                    .iter()
                    .map(move |n| (c.attempt_id.to_string(), n.to_string()))
            })
            .collect();
        assert!(covered.contains(&(a.to_string(), "main".into())));
        assert!(covered.contains(&(b.to_string(), "work".into())));
        let c = ans.coverage.iter().find(|c| c.attempt_id == b).unwrap();
        assert_eq!(c.plan_digest, pending.plan_digest);
        assert_eq!(c.expansion_seq, 0);
    }

    #[test]
    fn a09_no_history_is_fallback_and_never_an_eta() {
        let a = AttemptId::generate();
        let (ans, _) = run(
            &insp(1, calm(), vec![]),
            &request(plan_of(&a, vec![node(&a, "main", None)]), vec![]),
            &[],
        );
        let t = &ans.target;
        assert_eq!(t.runtime_source, RuntimeSource::Fallback);
        assert_eq!(t.execution_ms, None);
        assert_eq!(t.finish_in_ms, None);
        assert_eq!(
            t.null_reasons[&ForecastField::ExecutionMs],
            Uncertainty::NoHistory
        );
        assert_eq!(
            t.null_reasons[&ForecastField::FinishInMs],
            Uncertainty::NoHistory
        );
        assert_eq!(t.ready_in_ms, Some(0));
        assert_reasons_match(&ans);
    }

    #[test]
    fn a09_an_overrun_entry_is_unknown_and_never_zero() {
        let a = AttemptId::generate();
        // It ran 50 s of a learned 10 s, and holds the only slot.
        let i = insp(1, calm(), vec![running("late", 1, Some(10.0), 50)]);
        let (ans, _) = run(
            &i,
            &request(plan_of(&a, vec![node(&a, "main", Some(5_000))]), vec![]),
            &[],
        );
        let t = &ans.target;
        assert!(t.overrun);
        assert_eq!(t.runtime_source, RuntimeSource::Measured);
        assert_eq!(t.finish_in_ms, None);
        assert_eq!(
            t.null_reasons[&ForecastField::FinishInMs],
            Uncertainty::Overrun
        );
        assert!(t.conditional);
        assert_reasons_match(&ans);
        // With room to spare the same node is not held by it.
        let i = insp(2, calm(), vec![running("late", 1, Some(10.0), 50)]);
        let (ans, _) = run(
            &i,
            &request(plan_of(&a, vec![node(&a, "main", Some(5_000))]), vec![]),
            &[],
        );
        assert_eq!(ans.target.finish_in_ms, Some(5_000));
        assert!(!ans.target.overrun);
    }

    #[test]
    fn a09_a_memory_block_is_conditional_and_says_so() {
        let a = AttemptId::generate();
        let low = Some(LiveSnapshot {
            available_memory: 1,
            cpu_busy: 0.1,
            load_avg: 0.0,
        });
        let i = insp(2, low, vec![running("other", 1, Some(30.0), 0)]);
        let (ans, _) = run(
            &i,
            &request(plan_of(&a, vec![node(&a, "main", Some(5_000))]), vec![]),
            &[],
        );
        let t = &ans.target;
        assert_eq!(t.wait_reason, Some(WaitReason::MemoryBlocked));
        assert!(t.conditional);
        assert!(t.assumptions.iter().any(|x| x.contains("live guard")));
        assert_reasons_match(&ans);
    }

    #[test]
    fn a09_the_three_cases_stay_distinct() {
        let a = AttemptId::generate();
        let fallback = run(
            &insp(1, calm(), vec![]),
            &request(plan_of(&a, vec![node(&a, "m", None)]), vec![]),
            &[],
        )
        .0;
        let overrun = run(
            &insp(1, calm(), vec![running("late", 1, Some(1.0), 9)]),
            &request(plan_of(&a, vec![node(&a, "m", Some(1_000))]), vec![]),
            &[],
        )
        .0;
        let measured = run(
            &insp(1, calm(), vec![]),
            &request(plan_of(&a, vec![node(&a, "m", Some(1_000))]), vec![]),
            &[],
        )
        .0;
        assert_eq!(fallback.target.runtime_source, RuntimeSource::Fallback);
        assert!(!fallback.target.overrun);
        assert!(overrun.target.overrun);
        assert_eq!(overrun.target.runtime_source, RuntimeSource::Measured);
        assert_eq!(measured.target.finish_in_ms, Some(1_000));
        assert!(measured.target.null_reasons.is_empty());
    }

    #[test]
    fn a10_a_chain_reports_the_end_of_the_last_step() {
        let a = AttemptId::generate();
        let mut setup = node(&a, "setup", Some(10_000));
        let mut test = node(&a, "test", Some(20_000));
        let mut lint = node(&a, "lint", Some(5_000));
        test.depends_on = vec![setup.node_id.clone()];
        lint.depends_on = vec![test.node_id.clone()];
        setup.priority = 0;
        let (ans, _) = run(
            &insp(4, calm(), vec![]),
            &request(plan_of(&a, vec![setup, test, lint]), vec![]),
            &[],
        );
        let t = &ans.target;
        // Not the first step's 10 s: the whole chain.
        assert_eq!(t.finish_in_ms, Some(35_000));
        assert_eq!(t.ready_in_ms, Some(0));
        // A sum of nodes is no time on the clock; the critical path is.
        assert_eq!(t.execution_ms, Some(35_000));
        assert_eq!(t.queue_wait_after_ready_ms, Some(0));
        assert_reasons_match(&ans);
    }

    #[test]
    fn a10_parallel_nodes_end_with_the_longest_not_the_sum() {
        let a = AttemptId::generate();
        let nodes = (0..4)
            .map(|i| node(&a, &format!("shard-{i}"), Some(20_000)))
            .collect();
        let (ans, _) = run(
            &insp(4, calm(), vec![]),
            &request(plan_of(&a, nodes), vec![]),
            &[],
        );
        assert_eq!(ans.target.finish_in_ms, Some(20_000));
        // One slot: they queue, and the last one ends at the sum.
        let nodes = (0..4)
            .map(|i| node(&a, &format!("shard-{i}"), Some(20_000)))
            .collect();
        let (ans, _) = run(
            &insp(1, calm(), vec![]),
            &request(plan_of(&a, nodes), vec![]),
            &[],
        );
        assert_eq!(ans.target.finish_in_ms, Some(80_000));
    }

    #[test]
    fn a10_a_shared_setup_builder_is_counted_once() {
        let a = AttemptId::generate();
        let builder = BuilderId::generate();
        let mut x = node(&a, "unit-a", Some(10_000));
        let mut y = node(&a, "unit-b", Some(10_000));
        x.shared_builder = Some(builder.clone());
        y.shared_builder = Some(builder);
        let (ans, _) = run(
            &insp(1, calm(), vec![]),
            &request(plan_of(&a, vec![x, y]), vec![]),
            &[],
        );
        assert_eq!(ans.target.finish_in_ms, Some(10_000));
        // Both nodes are covered.
        assert_eq!(ans.coverage[0].node_ids.len(), 2);
    }

    #[test]
    fn a10_an_unmeasured_step_makes_the_plan_unmeasured() {
        let a = AttemptId::generate();
        let setup = node(&a, "setup", Some(10_000));
        let mut test = node(&a, "test", None);
        test.depends_on = vec![setup.node_id.clone()];
        let (ans, _) = run(
            &insp(4, calm(), vec![]),
            &request(plan_of(&a, vec![setup, test]), vec![]),
            &[],
        );
        assert_eq!(ans.target.runtime_source, RuntimeSource::Fallback);
        assert_eq!(ans.target.finish_in_ms, None);
        assert_reasons_match(&ans);
    }

    #[test]
    fn a_plan_the_simulation_refuses_is_invalid_input() {
        let a = AttemptId::generate();
        let mut n = node(&a, "main", Some(1));
        n.depends_on = vec![NodeId::parse("nowhere").unwrap()];
        let err = compute(
            &ctx(),
            &insp(1, calm(), vec![]),
            2,
            &ident(),
            &request(plan_of(&a, vec![n]), vec![]),
            &[],
        )
        .unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidInput);
    }

    #[test]
    fn the_snapshot_digest_follows_the_overlay() {
        let a = AttemptId::generate();
        let b = AttemptId::generate();
        let req = request(plan_of(&a, vec![node(&a, "main", Some(1_000))]), vec![]);
        let i = insp(1, calm(), vec![]);
        let alone = run(&i, &req, &[]).0.snapshot_digest;
        let again = run(&i, &req, &[]).0.snapshot_digest;
        let with = run(&i, &req, &[accepted_node(&b, "work", 0, false)])
            .0
            .snapshot_digest;
        assert_eq!(alone, again);
        assert_ne!(alone, with);
        // The policy digest is the worker's policy, as `inspect` reports it.
        assert_eq!(
            run(&i, &req, &[]).0.policy_digest.to_string(),
            i.policy_digest
        );
    }

    #[test]
    fn a_measured_target_behind_fallback_demand_is_no_measured_answer() {
        let a = AttemptId::generate();
        let b = AttemptId::generate();
        let i = insp(1, calm(), vec![]);
        let req = request(plan_of(&a, vec![node(&a, "main", Some(10_000))]), vec![]);
        // Alone: measured, a finish.
        let (alone, _) = run(&i, &req, &[]);
        assert_eq!(alone.target.runtime_source, RuntimeSource::Measured);
        assert_eq!(alone.target.finish_in_ms, Some(10_000));
        // An accepted node that is not registered runs on the fallback
        // runtime, and the one slot is its first: the target's start is no
        // measured number, whatever the target's own runtime.
        let (ans, report) = run(&i, &req, &[accepted_node(&b, "work", 0, false)]);
        assert_eq!(report.applied.len(), 1);
        let t = &ans.target;
        assert_eq!(t.finish_in_ms, None);
        assert_eq!(t.runtime_source, RuntimeSource::Fallback);
        assert_eq!(
            t.null_reasons.get(&ForecastField::FinishInMs),
            Some(&Uncertainty::NoHistory)
        );
        assert!(t
            .assumptions
            .iter()
            .any(|x| x.contains("ahead of the target has no recorded runtime")));
        assert!(t
            .assumptions
            .iter()
            .any(|x| x.contains("dependencies inside accepted attempts are ignored")));
        assert_reasons_match(&ans);
        // The coordinator's recorded 20 s for the same node replaces the
        // fallback: measured again, and the finish is a number.
        let pending = plan_of(&b, vec![node(&b, "work", Some(20_000))]);
        let req = request(
            plan_of(&a, vec![node(&a, "main", Some(10_000))]),
            vec![pending],
        );
        let (ans, _) = run(&i, &req, &[accepted_node(&b, "work", 0, false)]);
        assert_eq!(ans.target.runtime_source, RuntimeSource::Measured);
        assert_eq!(ans.target.finish_in_ms, Some(30_000));
        // A target that is a fallback itself is not "behind" anything: the
        // note about work ahead is not made.
        let (own, _) = run(
            &i,
            &request(plan_of(&a, vec![node(&a, "main", None)]), vec![]),
            &[],
        );
        assert_eq!(own.target.runtime_source, RuntimeSource::Fallback);
        assert!(!own
            .target
            .assumptions
            .iter()
            .any(|x| x.contains("ahead of the target")));
        // A finished node is no demand: no note about it either.
        let (done, _) = run(&i, &req_of_main(&a), &[accepted_node(&b, "work", 0, true)]);
        assert_eq!(done.target.runtime_source, RuntimeSource::Measured);
        assert!(!done
            .target
            .assumptions
            .iter()
            .any(|x| x.contains("accepted attempts")));
    }

    fn req_of_main(a: &AttemptId) -> ForecastRequest {
        request(plan_of(a, vec![node(a, "main", Some(10_000))]), vec![])
    }

    #[test]
    fn the_answer_carries_the_configured_ttl() {
        let a = AttemptId::generate();
        let i = insp(1, calm(), vec![]);
        let mut id = ident();
        id.forecast_ttl_ms = 750;
        let (ans, _) = compute(&ctx(), &i, 2, &id, &req_of_main(&a), &[]).unwrap();
        // Not the simulation's own 2000.
        assert_eq!(ans.ttl_ms, 750);
    }

    #[test]
    fn the_reads_run_accepted_then_ledger_then_sidecars() {
        use std::cell::RefCell;
        let log = RefCell::new(Vec::new());
        let a = AttemptId::generate();
        let (nodes, _) = gather(
            || {
                log.borrow_mut().push("accepted");
                Ok(vec![
                    accepted_node(&a, "x", 0, false),
                    accepted_node(&a, "y", 1, false),
                ])
            },
            || {
                log.borrow_mut().push("ledger");
                Ok(insp(1, calm(), vec![]))
            },
            |n| {
                log.borrow_mut().push("sidecar");
                n.terminal = n.node_id.as_str() == "y";
            },
        )
        .unwrap();
        assert_eq!(
            *log.borrow(),
            ["accepted", "ledger", "sidecar", "sidecar"],
            "a node registered between two reads must be in the ledger, a node finished \
             between them must be dropped"
        );
        assert_eq!(
            nodes.iter().map(|n| n.terminal).collect::<Vec<_>>(),
            [false, true]
        );
        // A failing read stops the later ones.
        let log = RefCell::new(Vec::new());
        let err = gather(
            || Err(ApiError::new(ErrorCode::Internal, "x")),
            || {
                log.borrow_mut().push("ledger");
                Ok(insp(1, calm(), vec![]))
            },
            |_| {},
        );
        assert!(err.is_err() && log.borrow().is_empty());
        let err = gather(
            || Ok(vec![accepted_node(&a, "x", 0, false)]),
            || Err(ApiError::new(ErrorCode::Internal, "y")),
            |_| log.borrow_mut().push("sidecar"),
        );
        assert!(err.is_err() && log.borrow().is_empty());
    }
}
