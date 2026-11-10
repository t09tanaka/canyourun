//! Repository-level tests: the invariants storage enforces.

use super::*;
use crate::error::ErrorCode;
use crate::ids::*;
use crate::state::{Exit, Observation, Outcome, Receipt};
use crate::wire::LeaseKey;
use serde_json::json;
use tempfile::TempDir;

const DAY: u64 = 86_400_000;

fn hash(c: char) -> PayloadHash {
    PayloadHash::parse(&format!("sha256:{}", c.to_string().repeat(64))).unwrap()
}

fn done(outcome: Outcome) -> Termination {
    match outcome {
        Outcome::Succeeded => Termination::new(outcome, Some(Exit::Code(0)), None).unwrap(),
        Outcome::Failed => Termination::new(outcome, Some(Exit::Code(1)), None).unwrap(),
        _ => Termination::new(outcome, None, None).unwrap(),
    }
}

fn key(epoch: u64, seq: u64) -> LeaseKey {
    LeaseKey {
        coordinator_epoch: CoordinatorEpoch(epoch),
        lease_seq: seq,
    }
}

fn path_state(dir: &TempDir) -> &std::path::Path {
    dir.path()
}

// ============================================================ coordinator

struct Fx {
    _dir: TempDir,
    store: CoordinatorStore,
    run: RunId,
    hw: HomeworkId,
    worker: WorkerId,
}

fn fx() -> Fx {
    let dir = tempfile::tempdir().unwrap();
    let store = CoordinatorStore::open(path_state(&dir), 1).unwrap();
    let run = RunId::generate();
    let hw = HomeworkId::generate();
    store.create_run(&run, None, None, &json!({}), 1).unwrap();
    store.create_homework(&hw, &run, &json!({}), 1).unwrap();
    Fx {
        _dir: dir,
        store,
        run,
        hw,
        worker: WorkerId::parse("mini-a").unwrap(),
    }
}

impl Fx {
    fn attempt(&self, h: char) -> Result<AttemptRecord> {
        self.store.create_attempt(
            NewAttempt {
                attempt_id: AttemptId::generate(),
                homework_id: self.hw.clone(),
                worker_id: self.worker.clone(),
                payload_hash: hash(h),
                permit_id: None,
            },
            10,
        )
    }

    fn to_finalizing(&self, id: &AttemptId) {
        for s in [
            AttemptState::Preparing,
            AttemptState::Ready,
            AttemptState::Starting,
            AttemptState::Active,
            AttemptState::Finalizing,
        ] {
            self.store.transition_attempt(id, s, 20).unwrap();
        }
    }
}

#[test]
fn each_open_starts_a_new_epoch_and_a_second_open_is_locked_out() {
    let dir = tempfile::tempdir().unwrap();
    let first = CoordinatorStore::open(dir.path(), 1).unwrap();
    assert_eq!(first.epoch(), CoordinatorEpoch(1));
    let err = CoordinatorStore::open(dir.path(), 2).err().unwrap();
    assert!(matches!(err, StorageError::Locked(_)), "{err:?}");
    // Reading needs no lock and does not bump the epoch.
    let ro = CoordinatorStore::open_read_only(dir.path()).unwrap();
    assert_eq!(ro.epoch(), CoordinatorEpoch(1));
    assert!(ro
        .create_run(&RunId::generate(), None, None, &json!({}), 1)
        .is_err());
    let (cluster, coordinator) = (first.cluster_id().clone(), first.coordinator_id().clone());
    drop(first);
    drop(ro);
    let second = CoordinatorStore::open(dir.path(), 3).unwrap();
    assert_eq!(second.epoch(), CoordinatorEpoch(2));
    assert_eq!(second.cluster_id(), &cluster);
    assert_eq!(second.coordinator_id(), &coordinator);
    drop(second);
    assert_eq!(
        CoordinatorStore::open(dir.path(), 4).unwrap().epoch(),
        CoordinatorEpoch(3)
    );
}

#[test]
fn a_homework_never_has_two_live_attempts() {
    let f = fx();
    let a1 = f.attempt('a').unwrap();
    assert_eq!(a1.generation, Generation(1));
    assert_eq!(a1.state, AttemptState::Accepted);
    assert!(a1.receipts.reached(Receipt::CoordinatorAccepted));
    assert_eq!(a1.coordinator_epoch, f.store.epoch());
    let err = f.attempt('a').err().unwrap();
    assert!(matches!(err, StorageError::LiveAttemptExists(_)), "{err:?}");
    assert_eq!(err.code(), ErrorCode::Conflict);
    // Even the unique index alone refuses it, bypassing the repository check.
    let raw = f.store.db.call(|c| {
        Ok(c.execute(
            "INSERT INTO attempt (attempt_id, homework_id, worker_id, generation, payload_hash,
                coordinator_epoch, state, receipts_json, created_at_ms)
             SELECT 'x', homework_id, 'w', 99, 'h', 1, 'accepted', '{}', 1 FROM attempt LIMIT 1",
            [],
        )?)
    });
    assert!(raw.is_err());

    // A terminal attempt frees the homework; the next generation counts up.
    f.to_finalizing(&a1.attempt_id);
    f.store
        .finalize_attempt(&a1.attempt_id, done(Outcome::Failed), 30)
        .unwrap();
    let a2 = f.attempt('a').unwrap();
    assert_eq!(a2.generation, Generation(2));
}

#[test]
fn a_new_attempt_waits_for_a_confirmed_retire_with_a_persisted_tombstone() {
    let f = fx();
    let a1 = f.attempt('a').unwrap();
    // Confirming without the tombstone is refused.
    assert!(matches!(
        f.store.confirm_retire(&a1.attempt_id, 5).err().unwrap(),
        StorageError::NotFound { .. }
    ));
    f.store.begin_retire(&a1.attempt_id, "ack lost", 5).unwrap();
    f.store.begin_retire(&a1.attempt_id, "again", 6).unwrap();
    assert_eq!(
        f.store.retire_tombstone(&a1.attempt_id).unwrap(),
        Some((5, None))
    );
    // The tombstone alone does not free the homework (the old one may still run).
    assert!(f.attempt('a').is_err());
    f.store.confirm_retire(&a1.attempt_id, 7).unwrap();
    assert_eq!(
        f.store.retire_tombstone(&a1.attempt_id).unwrap(),
        Some((5, Some(7)))
    );
    assert!(
        f.store
            .get_attempt(&a1.attempt_id)
            .unwrap()
            .unwrap()
            .retired
    );
    f.attempt('a').unwrap();
    // The tombstone cannot be deleted, not even by SQL.
    let del = f
        .store
        .db
        .call(|c| Ok(c.execute("DELETE FROM retire_tombstone", [])?));
    assert!(del.is_err());
    assert!(f.store.retire_tombstone(&a1.attempt_id).unwrap().is_some());
}

#[test]
fn terminal_is_fixed_once_when_natural_end_and_cancel_race() {
    let f = fx();
    let a = f.attempt('a').unwrap();
    f.to_finalizing(&a.attempt_id);
    let results: Vec<Finalized> = std::thread::scope(|s| {
        let hs: Vec<_> = [
            Outcome::Succeeded,
            Outcome::Cancelled,
            Outcome::Failed,
            Outcome::Interrupted,
        ]
        .into_iter()
        .map(|o| {
            let (st, id) = (&f.store, &a.attempt_id);
            s.spawn(move || st.finalize_attempt(id, done(o), 40).unwrap())
        })
        .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    assert_eq!(results.iter().filter(|r| r.won()).count(), 1);
    let winner = results.iter().find(|r| r.won()).unwrap().termination();
    assert!(results.iter().all(|r| r.termination() == winner));
    let rec = f.store.get_attempt(&a.attempt_id).unwrap().unwrap();
    assert_eq!(rec.termination, Some(winner));
    assert_eq!(rec.state, AttemptState::Terminal);
    assert!(rec.receipts.reached(Receipt::Terminal));
    // A later call with a different result still gets the first one.
    let again = f
        .store
        .finalize_attempt(&a.attempt_id, done(Outcome::Succeeded), 50)
        .unwrap();
    assert_eq!(again, Finalized::AlreadyTerminal(winner));
    // Exactly one terminal event.
    let events = f.store.events_after(0, 1000).unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|e| e.kind == "attempt_terminal")
            .count(),
        1
    );
}

#[test]
fn only_the_state_machine_moves_attempts() {
    let f = fx();
    let a = f.attempt('a').unwrap();
    let id = &a.attempt_id;
    // Accepted -> Active skips stages.
    let e = f
        .store
        .transition_attempt(id, AttemptState::Active, 1)
        .err()
        .unwrap();
    assert!(matches!(e, StorageError::Transition(_)), "{e:?}");
    // Terminal is never reached by a plain transition, nor finalize from Accepted.
    assert!(f
        .store
        .transition_attempt(id, AttemptState::Terminal, 1)
        .is_err());
    assert!(f
        .store
        .finalize_attempt(id, done(Outcome::Succeeded), 1)
        .is_err());
    f.to_finalizing(id);
    assert!(f
        .store
        .transition_attempt(id, AttemptState::Terminal, 1)
        .is_err());
    let rec = f.store.get_attempt(id).unwrap().unwrap();
    assert_eq!(rec.state, AttemptState::Finalizing);
    assert_eq!(rec.termination, None);
    // The database itself refuses a terminal row without a termination.
    let bad = f
        .store
        .db
        .call(|c| Ok(c.execute("UPDATE attempt SET state = 'terminal'", [])?));
    assert!(bad.is_err());
}

#[test]
fn cancel_is_persisted_and_receipts_are_first_write_wins() {
    let f = fx();
    let a = f.attempt('a').unwrap();
    assert!(f.store.request_cancel(&a.attempt_id, 5).unwrap());
    assert!(!f.store.request_cancel(&a.attempt_id, 9).unwrap());
    let rec = f.store.get_attempt(&a.attempt_id).unwrap().unwrap();
    assert_eq!(rec.cancel_requested_at_ms, Some(5));
    assert!(f.store.request_cancel(&AttemptId::generate(), 1).is_err());
    assert!(f
        .store
        .mark_receipt(&a.attempt_id, Receipt::WorkerAccepted, 6)
        .unwrap());
    assert!(!f
        .store
        .mark_receipt(&a.attempt_id, Receipt::WorkerAccepted, 7)
        .unwrap());
    for r in [Receipt::Terminal, Receipt::Collected] {
        assert!(f.store.mark_receipt(&a.attempt_id, r, 8).is_err());
    }
    f.store
        .set_observation(&a.attempt_id, Observation::Unknown)
        .unwrap();
    let rec = f.store.get_attempt(&a.attempt_id).unwrap().unwrap();
    assert_eq!(rec.observation, Observation::Unknown);
    assert_eq!(rec.receipts.at_ms(Receipt::WorkerAccepted), Some(6));
}

#[test]
fn the_event_journal_is_append_only_with_increasing_sequence() {
    let f = fx();
    let s1 = f
        .store
        .append_event(Some(&f.run), None, "a", &json!(1), 1)
        .unwrap();
    let s2 = f
        .store
        .append_event(Some(&f.run), None, "b", &json!(2), 2)
        .unwrap();
    assert!(s2 > s1);
    let all = f.store.events_after(0, 1000).unwrap();
    assert!(all.windows(2).all(|w| w[0].seq < w[1].seq));
    let tail = f.store.events_after(s1, 1000).unwrap();
    assert_eq!(tail.len(), 1);
    assert_eq!(tail[0].payload, json!(2));
    assert!(f.store.events_after(0, 1).unwrap().len() == 1);
    assert!(f
        .store
        .db
        .call(|c| Ok(c.execute("UPDATE event SET kind='x'", [])?))
        .is_err());
    assert!(f
        .store
        .db
        .call(|c| Ok(c.execute("DELETE FROM event", [])?))
        .is_err());
    // A sequence number is never reused, even after the newest row would go.
    let s3 = f.store.append_event(None, None, "c", &json!(3), 3).unwrap();
    assert!(s3 > s2);
}

#[test]
fn matrix_permits_hold_until_terminal_or_confirmed_retire() {
    let f = fx();
    f.store.create_matrix_group("g", &f.run, 2, 1).unwrap();
    let (p1, p2, p3) = (
        PermitId::generate(),
        PermitId::generate(),
        PermitId::generate(),
    );
    assert!(f.store.acquire_permit("g", &p1, 1).unwrap().is_some());
    assert!(f.store.acquire_permit("g", &p2, 1).unwrap().is_some());
    assert!(f.store.acquire_permit("g", &p3, 1).unwrap().is_none());
    // Asking again with an existing id returns it without taking another slot.
    assert!(f.store.acquire_permit("g", &p1, 2).unwrap().is_some());
    assert_eq!(f.store.held_permits("g").unwrap(), 2);

    // Bind p1 to an attempt in the same commit as the attempt.
    let aid = AttemptId::generate();
    let a = f
        .store
        .create_attempt(
            NewAttempt {
                attempt_id: aid.clone(),
                homework_id: f.hw.clone(),
                worker_id: f.worker.clone(),
                payload_hash: hash('a'),
                permit_id: Some(p1.clone()),
            },
            3,
        )
        .unwrap();
    assert_eq!(
        f.store.get_permit(&p1).unwrap().unwrap().attempt_id,
        Some(aid.clone())
    );
    assert_eq!(a.permit_id, Some(p1.clone()));
    // Unknown, running, even finalizing: the slot stays.
    let e = f.store.release_permit(&p1, 4).err().unwrap();
    assert!(matches!(e, StorageError::IllegalState { .. }), "{e:?}");
    f.to_finalizing(&aid);
    assert!(f.store.release_permit(&p1, 5).is_err());
    f.store
        .finalize_attempt(&aid, done(Outcome::Succeeded), 6)
        .unwrap();
    f.store.release_permit(&p1, 7).unwrap();
    f.store.release_permit(&p1, 8).unwrap();
    assert_eq!(f.store.held_permits("g").unwrap(), 1);
    assert!(f.store.acquire_permit("g", &p3, 9).unwrap().is_some());
    // An unbound permit can be given back, and a bound one frees on retire.
    f.store.release_permit(&p2, 9).unwrap();
    assert!(f.store.get_permit(&PermitId::generate()).unwrap().is_none());
    assert!(f.store.release_permit(&PermitId::generate(), 1).is_err());
    assert!(f
        .store
        .acquire_permit("nope", &PermitId::generate(), 1)
        .is_err());
}

#[test]
fn creating_an_attempt_with_a_bad_permit_leaves_no_attempt() {
    let f = fx();
    let r = f.store.create_attempt(
        NewAttempt {
            attempt_id: AttemptId::generate(),
            homework_id: f.hw.clone(),
            worker_id: f.worker.clone(),
            payload_hash: hash('a'),
            permit_id: Some(PermitId::generate()),
        },
        1,
    );
    assert!(r.is_err());
    assert!(f.store.list_nonterminal_attempts().unwrap().is_empty());
    // The failed commit did not use up the homework.
    f.attempt('a').unwrap();
}

#[test]
fn placements_stay_in_the_unacked_demand_until_acknowledged() {
    let f = fx();
    let p1 = f
        .store
        .record_placement(&f.hw, &f.worker, None, &json!({"cpu": 2}), 1)
        .unwrap();
    let p2 = f
        .store
        .record_placement(&f.hw, &f.worker, None, &json!({"cpu": 1}), 2)
        .unwrap();
    assert!(p2 > p1);
    let open = f.store.unacked_demand().unwrap();
    assert_eq!(open.len(), 2);
    assert_eq!(open[0].demand, json!({"cpu": 2}));
    f.store.ack_placement(p1, 3).unwrap();
    f.store.ack_placement(p1, 4).unwrap();
    let open = f.store.unacked_demand().unwrap();
    assert_eq!(open.len(), 1);
    assert_eq!(open[0].placement_id, p2);
    assert!(f.store.ack_placement(999, 1).is_err());
}

#[test]
fn source_and_secret_refs_are_idempotent_and_immutable() {
    let f = fx();
    let src = SourceId::generate();
    let d1 = Digest::of_bytes(b"one");
    let d2 = Digest::of_bytes(b"two");
    f.store.put_source_ref(&src, &d1, 1).unwrap();
    f.store.put_source_ref(&src, &d1, 2).unwrap();
    let e = f.store.put_source_ref(&src, &d2, 3).err().unwrap();
    assert!(matches!(e, StorageError::Immutable(_)));
    assert_eq!(e.code(), ErrorCode::Conflict);
    f.store.put_secret_bundle_ref("b1", &d1, 1).unwrap();
    f.store.put_secret_bundle_ref("b1", &d1, 1).unwrap();
    assert!(f.store.put_secret_bundle_ref("b1", &d2, 1).is_err());
    let run = RunId::generate();
    f.store
        .create_run(&run, Some(&src), Some("b1"), &json!({"k": 1}), 4)
        .unwrap();
    f.store.close_run(&run, 5).unwrap();
    assert!(f
        .store
        .create_run(
            &RunId::generate(),
            Some(&SourceId::generate()),
            None,
            &json!({}),
            1
        )
        .is_err());
}

fn receipt(a: &AttemptId, name: &str) -> ArtifactReceipt {
    ArtifactReceipt {
        attempt_id: a.clone(),
        name: name.into(),
        rel_path: format!("out/{name}"),
        size: 3,
        digest: Digest::of_bytes(name.as_bytes()),
        source_id: SourceId::generate(),
        plan_digest: Digest::of_bytes(b"plan"),
        profile_fingerprint: "fp".into(),
    }
}

#[test]
fn collected_needs_the_terminal_and_every_required_artifact() {
    let f = fx();
    let a = f.attempt('a').unwrap();
    let id = &a.attempt_id;
    f.store.declare_artifact(id, "bin", true).unwrap();
    f.store.declare_artifact(id, "extra", false).unwrap();
    f.store.declare_artifact(id, "bin", true).unwrap();
    // Not terminal yet.
    assert!(f.store.mark_collected(id, 1).is_err());
    f.to_finalizing(id);
    f.store
        .finalize_attempt(id, done(Outcome::Succeeded), 2)
        .unwrap();
    // Terminal, but the required artifact has no receipt.
    let e = f.store.mark_collected(id, 3).err().unwrap();
    assert!(matches!(e, StorageError::IllegalState { .. }), "{e:?}");
    let r = receipt(id, "bin");
    f.store.record_artifact(&r, 4).unwrap();
    f.store.record_artifact(&r, 5).unwrap();
    let mut changed = r.clone();
    changed.size = 4;
    assert!(matches!(
        f.store.record_artifact(&changed, 6).err().unwrap(),
        StorageError::Immutable(_)
    ));
    assert!(f.store.mark_collected(id, 7).unwrap());
    assert!(!f.store.mark_collected(id, 8).unwrap());
    let rec = f.store.get_attempt(id).unwrap().unwrap();
    assert_eq!(rec.receipts.at_ms(Receipt::Collected), Some(7));
    // An undeclared artifact is recorded as optional.
    f.store.record_artifact(&receipt(id, "late"), 9).unwrap();
}

#[test]
fn an_attempt_without_required_artifacts_is_collected_at_terminal() {
    let f = fx();
    let a = f.attempt('a').unwrap();
    f.to_finalizing(&a.attempt_id);
    f.store
        .finalize_attempt(&a.attempt_id, done(Outcome::Failed), 2)
        .unwrap();
    assert!(f.store.mark_collected(&a.attempt_id, 3).unwrap());
}

fn blob(r: &str, a: &AttemptId, kind: BlobKind, loc: &str) -> NewBlobRef {
    NewBlobRef {
        ref_id: r.into(),
        attempt_id: a.to_string(),
        kind,
        locator: loc.into(),
    }
}

#[test]
fn coordinator_gc_keeps_everything_that_is_not_safely_finished() {
    let f = fx();
    let run = &f.run;
    let mk = |hw: &HomeworkId| -> AttemptRecord {
        f.store.create_homework(hw, run, &json!({}), 1).unwrap();
        f.store
            .create_attempt(
                NewAttempt {
                    attempt_id: AttemptId::generate(),
                    homework_id: hw.clone(),
                    worker_id: f.worker.clone(),
                    payload_hash: hash('a'),
                    permit_id: None,
                },
                1,
            )
            .unwrap()
    };
    let end = |a: &AttemptRecord, at: u64| {
        for s in [
            AttemptState::Preparing,
            AttemptState::Ready,
            AttemptState::Starting,
            AttemptState::Active,
            AttemptState::Finalizing,
        ] {
            f.store.transition_attempt(&a.attempt_id, s, 1).unwrap();
        }
        f.store
            .finalize_attempt(&a.attempt_id, done(Outcome::Succeeded), at)
            .unwrap();
    };
    let old = mk(&HomeworkId::generate()); // terminal, collected, old: collectable
    let running = mk(&HomeworkId::generate()); // not terminal
    let uncollected = mk(&HomeworkId::generate()); // terminal, required artifact missing
    let fresh = mk(&HomeworkId::generate()); // terminal, collected, recent
    let pinned = mk(&HomeworkId::generate()); // terminal, collected, old, pinned
    end(&old, 1_000);
    end(&uncollected, 1_000);
    end(&fresh, 1_000 + 6 * DAY);
    end(&pinned, 1_000);
    f.store
        .declare_artifact(&uncollected.attempt_id, "bin", true)
        .unwrap();
    for a in [&old, &fresh, &pinned] {
        assert!(f.store.mark_collected(&a.attempt_id, 2_000).unwrap());
    }
    for (name, a) in [
        ("old", &old),
        ("run", &running),
        ("unc", &uncollected),
        ("fresh", &fresh),
        ("pin", &pinned),
    ] {
        f.store
            .add_blob_ref(
                &blob(
                    &format!("{name}-in"),
                    &a.attempt_id,
                    BlobKind::Input,
                    &format!("blob/{name}"),
                ),
                1,
            )
            .unwrap();
        f.store
            .add_blob_ref(
                &blob(
                    &format!("{name}-log"),
                    &a.attempt_id,
                    BlobKind::Log,
                    &format!("log/{name}"),
                ),
                1,
            )
            .unwrap();
    }
    f.store.pin_blob("pin-in").unwrap();
    assert!(
        f.store.unpin_blob("run-in").is_err(),
        "pins never go negative"
    );
    // Several attempts may share one file: the locator stays in use.
    f.store
        .add_blob_ref(
            &blob("shared", &old.attempt_id, BlobKind::Result, "blob/fresh"),
            1,
        )
        .unwrap();

    let now = 1_000 + 7 * DAY;
    let report = f.store.gc(now, 7 * DAY).unwrap();
    let removed: Vec<&str> = report.removed.iter().map(|r| r.ref_id.as_str()).collect();
    assert_eq!(removed, vec!["old-in", "old-log", "pin-log", "shared"]);
    let kept = |id: &str| report.kept.iter().find(|(r, _)| r == id).map(|(_, k)| *k);
    use policy::KeepReason::*;
    assert_eq!(kept("run-in"), Some(NotTerminal));
    assert_eq!(kept("unc-in"), Some(AwaitingCollection));
    assert_eq!(kept("fresh-in"), Some(WithinRetention));
    assert_eq!(kept("pin-in"), Some(Pinned));
    // `blob/fresh` is still named by fresh-in, so its file must stay.
    assert!(f.store.locator_in_use("blob/fresh").unwrap());
    assert!(!f.store.locator_in_use("blob/old").unwrap());
    // After the pin is released the rest follows once old enough.
    assert_eq!(f.store.unpin_blob("pin-in").unwrap(), 0);
    let report = f.store.gc(now, 7 * DAY).unwrap();
    assert_eq!(report.removed.len(), 1);
    assert_eq!(report.removed[0].ref_id, "pin-in");
    // Dedup records, tombstones and the journal are never GC'd.
    assert!(!f.store.events_after(0, 1000).unwrap().is_empty());
    assert!(f.store.get_attempt(&old.attempt_id).unwrap().is_some());
}

#[test]
fn a_restart_keeps_what_was_committed_and_drops_what_was_not() {
    let dir = tempfile::tempdir().unwrap();
    let (run, hw, aid);
    {
        let s = CoordinatorStore::open(dir.path(), 1).unwrap();
        run = RunId::generate();
        hw = HomeworkId::generate();
        aid = AttemptId::generate();
        s.create_run(&run, None, None, &json!({}), 1).unwrap();
        s.create_homework(&hw, &run, &json!({}), 1).unwrap();
        s.create_attempt(
            NewAttempt {
                attempt_id: aid.clone(),
                homework_id: hw.clone(),
                worker_id: WorkerId::parse("w").unwrap(),
                payload_hash: hash('a'),
                permit_id: None,
            },
            1,
        )
        .unwrap();
        s.transition_attempt(&aid, AttemptState::Preparing, 2)
            .unwrap();
        // A write that "crashes" before commit: it is gone after the restart.
        s.db.call(|c| {
            let tx = c.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            tx.execute("UPDATE attempt SET state = 'ready'", [])?;
            Ok(())
        })
        .unwrap();
        assert_eq!(
            s.get_attempt(&aid).unwrap().unwrap().state,
            AttemptState::Preparing
        );
    }
    let s = CoordinatorStore::open(dir.path(), 9).unwrap();
    assert_eq!(s.epoch(), CoordinatorEpoch(2));
    let live = s.list_nonterminal_attempts().unwrap();
    assert_eq!(live.len(), 1);
    assert_eq!(live[0].attempt_id, aid);
    assert_eq!(live[0].state, AttemptState::Preparing);
    assert!(!live[0].state.recovery_must_not_respawn());
}

// ================================================================== worker

struct Wx {
    dir: TempDir,
    store: WorkerStore,
    id: WorkerId,
}

fn wx() -> Wx {
    let dir = tempfile::tempdir().unwrap();
    let id = WorkerId::parse("mini-a").unwrap();
    let store = WorkerStore::open(dir.path(), &id).unwrap();
    store.start_agent(1).unwrap();
    Wx { dir, store, id }
}

fn acceptance(a: &AttemptId, hw: &HomeworkId, h: char, epoch: u64) -> NewAcceptance {
    NewAcceptance {
        attempt_id: a.clone(),
        run_id: RunId::generate(),
        homework_id: hw.clone(),
        generation: Generation(1),
        payload_hash: hash(h),
        coordinator_epoch: CoordinatorEpoch(epoch),
        spec: json!({"cmd": "true"}),
    }
}

fn accepted(w: &Wx) -> AcceptedAttempt {
    match w
        .store
        .submit(
            acceptance(&AttemptId::generate(), &HomeworkId::generate(), 'a', 1),
            10,
        )
        .unwrap()
    {
        SubmitOutcome::Accepted(a) => a,
        other => panic!("{other:?}"),
    }
}

fn drive(w: &Wx, id: &AttemptId, to: &[AttemptState]) {
    for s in to {
        w.store.transition(id, *s, 20).unwrap();
    }
}

const TO_READY: [AttemptState; 2] = [AttemptState::Preparing, AttemptState::Ready];

fn node(n: &str) -> NodeId {
    NodeId::parse(n).unwrap()
}

#[test]
fn submit_is_idempotent_for_the_same_payload_and_conflicts_otherwise() {
    let w = wx();
    let (a, hw) = (AttemptId::generate(), HomeworkId::generate());
    let first = match w.store.submit(acceptance(&a, &hw, 'a', 1), 10).unwrap() {
        SubmitOutcome::Accepted(r) => r,
        other => panic!("{other:?}"),
    };
    assert_eq!(first.state, AttemptState::Accepted);
    assert!(first.receipts.reached(Receipt::WorkerAccepted));
    // 100 resends return the same record and change nothing.
    for i in 0..100 {
        let again = w
            .store
            .submit(acceptance(&a, &hw, 'a', 1), 1_000 + i)
            .unwrap();
        assert_eq!(again, SubmitOutcome::Existing(first.clone()));
    }
    let e = w
        .store
        .submit(acceptance(&a, &hw, 'b', 1), 11)
        .err()
        .unwrap();
    assert!(matches!(e, StorageError::PayloadConflict(_)), "{e:?}");
    assert_eq!(e.code(), ErrorCode::Conflict);
    assert_eq!(w.store.get(&a).unwrap().unwrap(), first);
    let accepted_events = w.store.events_after(0, 1000).unwrap();
    assert_eq!(
        accepted_events
            .iter()
            .filter(|e| e.kind == "attempt_accepted")
            .count(),
        1
    );
}

#[test]
fn a_second_live_attempt_for_a_homework_is_refused() {
    let w = wx();
    let hw = HomeworkId::generate();
    let a1 = AttemptId::generate();
    w.store.submit(acceptance(&a1, &hw, 'a', 1), 1).unwrap();
    let e = w
        .store
        .submit(acceptance(&AttemptId::generate(), &hw, 'a', 1), 2)
        .err()
        .unwrap();
    assert!(matches!(e, StorageError::LiveAttemptExists(_)), "{e:?}");
    // Once the first is retired the next generation may come.
    w.store.retire(&a1, CoordinatorEpoch(1), 3).unwrap();
    w.store
        .submit(acceptance(&AttemptId::generate(), &hw, 'a', 1), 4)
        .unwrap();
}

#[test]
fn a_cancel_that_arrives_first_leaves_a_tombstone_and_the_late_submit_is_retired() {
    let w = wx();
    let (a, hw) = (AttemptId::generate(), HomeworkId::generate());
    assert_eq!(
        w.store.cancel(&a, CoordinatorEpoch(1), 5).unwrap(),
        CancelOutcome::Tombstoned
    );
    assert_eq!(w.store.tombstone(&a).unwrap().as_deref(), Some("cancel"));
    let e = w
        .store
        .submit(acceptance(&a, &hw, 'a', 1), 6)
        .err()
        .unwrap();
    assert!(matches!(e, StorageError::Retired(_)), "{e:?}");
    assert_eq!(e.code(), ErrorCode::Retired);
    assert!(w.store.get(&a).unwrap().is_none(), "nothing was accepted");
    // The tombstone survives a restart and cannot be deleted.
    let Wx { dir, store, id } = w;
    drop(store);
    let store = WorkerStore::open(dir.path(), &id).unwrap();
    assert!(store.submit(acceptance(&a, &hw, 'a', 1), 7).is_err());
    assert!(store
        .db
        .call(|c| Ok(c.execute("DELETE FROM tombstone", [])?))
        .is_err());
    // Retire upgrades the same tombstone.
    assert_eq!(store.retire(&a, CoordinatorEpoch(1), 8).unwrap(), None);
    assert_eq!(store.tombstone(&a).unwrap().as_deref(), Some("retire"));
}

#[test]
fn cancelling_a_known_attempt_is_recorded_once_and_stops_new_spawns() {
    let w = wx();
    let a = accepted(&w);
    drive(&w, &a.attempt_id, &TO_READY);
    let CancelOutcome::Requested(rec) = w
        .store
        .cancel(&a.attempt_id, CoordinatorEpoch(1), 30)
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(rec.cancel_requested_at_ms, Some(30));
    assert!(matches!(
        w.store.cancel(&a.attempt_id, CoordinatorEpoch(1), 31).unwrap(),
        CancelOutcome::AlreadyRequested(r) if r.cancel_requested_at_ms == Some(30)
    ));
    let e = w
        .store
        .claim_launch(&a.attempt_id, &node("build"), 32)
        .err()
        .unwrap();
    assert!(matches!(e, StorageError::IllegalState { .. }), "{e:?}");
    assert_eq!(
        w.store.get(&a.attempt_id).unwrap().unwrap().state,
        AttemptState::Ready
    );
    // The same submit still answers with the existing state (not Retired).
    let again = w
        .store
        .submit(
            NewAcceptance {
                attempt_id: a.attempt_id.clone(),
                run_id: a.run_id.clone(),
                homework_id: a.homework_id.clone(),
                generation: a.generation,
                payload_hash: a.payload_hash.clone(),
                coordinator_epoch: CoordinatorEpoch(1),
                spec: a.spec.clone(),
            },
            40,
        )
        .unwrap();
    assert!(matches!(again, SubmitOutcome::Existing(_)));
}

#[test]
fn a_retired_attempt_refuses_submit_launch_and_lease() {
    let w = wx();
    let a = accepted(&w);
    drive(&w, &a.attempt_id, &TO_READY);
    let rec = w
        .store
        .retire(&a.attempt_id, CoordinatorEpoch(1), 30)
        .unwrap()
        .unwrap();
    assert!(rec.retired);
    let mut resend = acceptance(&a.attempt_id, &a.homework_id, 'a', 1);
    resend.payload_hash = a.payload_hash.clone();
    assert!(matches!(
        w.store.submit(resend, 31).err().unwrap(),
        StorageError::Retired(_)
    ));
    assert!(matches!(
        w.store
            .claim_launch(&a.attempt_id, &node("n"), 32)
            .err()
            .unwrap(),
        StorageError::Retired(_)
    ));
    let e = w
        .store
        .renew_lease(&a.attempt_id, key(1, 1), 33)
        .err()
        .unwrap();
    assert!(matches!(e, StorageError::LeaseNotRenewable(_)));
    assert_eq!(e.code(), ErrorCode::LeaseNotRenewable);
    // Retire twice is fine and keeps one event.
    w.store
        .retire(&a.attempt_id, CoordinatorEpoch(1), 34)
        .unwrap();
    let ev = w.store.events_after(0, 1000).unwrap();
    assert_eq!(ev.iter().filter(|e| e.kind == "retired").count(), 1);
}

#[test]
fn the_first_claim_grants_the_spawn_and_a_replay_never_does() {
    let w = wx();
    let a = accepted(&w);
    let id = &a.attempt_id;
    // Too early.
    assert!(w.store.claim_launch(id, &node("build"), 1).is_err());
    drive(&w, id, &TO_READY);
    assert_eq!(
        w.store.claim_launch(id, &node("build"), 30).unwrap(),
        LaunchClaim::Granted
    );
    assert_eq!(
        w.store.get(id).unwrap().unwrap().state,
        AttemptState::Starting
    );
    // Crash before the identity was recorded: recovery sees the claim, and
    // the state forbids a respawn.
    assert_eq!(
        w.store.claim_launch(id, &node("build"), 31).unwrap(),
        LaunchClaim::AlreadyClaimed { identity: None }
    );
    assert!(w
        .store
        .get(id)
        .unwrap()
        .unwrap()
        .state
        .recovery_must_not_respawn());
    let ident = ProcessIdentity {
        boot_id: WorkerBootId::parse("boot-1").unwrap(),
        pid: 4242,
        start_identity: "Mon Nov 10 10:00:00 2026".into(),
    };
    w.store
        .record_process_identity(id, &node("build"), &ident, 32)
        .unwrap();
    w.store
        .record_process_identity(id, &node("build"), &ident, 33)
        .unwrap();
    let other = ProcessIdentity {
        pid: 1,
        ..ident.clone()
    };
    assert!(matches!(
        w.store
            .record_process_identity(id, &node("build"), &other, 34)
            .err()
            .unwrap(),
        StorageError::Immutable(_)
    ));
    assert!(w
        .store
        .record_process_identity(id, &node("nope"), &ident, 34)
        .is_err());
    assert_eq!(
        w.store.claim_launch(id, &node("build"), 35).unwrap(),
        LaunchClaim::AlreadyClaimed {
            identity: Some(ident.clone())
        }
    );
    // A further node of the same attempt is a new claim, in Starting or Active.
    assert_eq!(
        w.store.claim_launch(id, &node("test"), 36).unwrap(),
        LaunchClaim::Granted
    );
    w.store.transition(id, AttemptState::Active, 37).unwrap();
    assert_eq!(
        w.store.claim_launch(id, &node("lint"), 38).unwrap(),
        LaunchClaim::Granted
    );
    let launches = w.store.launches(id).unwrap();
    assert_eq!(launches.len(), 3);
    assert_eq!(launches[0].0, node("build"));
    assert_eq!(launches[0].2, Some(ident));
    // Finalizing / Terminal: no more commands.
    w.store
        .transition(id, AttemptState::Finalizing, 40)
        .unwrap();
    assert!(w.store.claim_launch(id, &node("late"), 41).is_err());
}

#[test]
fn lease_renewal_is_ordered_idempotent_and_never_revives() {
    let w = wx();
    let a = accepted(&w);
    let id = &a.attempt_id;
    drive(&w, id, &TO_READY);
    let renew = |k| w.store.renew_lease(id, k, 50);
    assert_eq!(renew(key(1, 1)).unwrap(), LeaseOutcome::Extended);
    assert_eq!(renew(key(1, 1)).unwrap(), LeaseOutcome::Stale);
    assert_eq!(renew(key(1, 3)).unwrap(), LeaseOutcome::Extended);
    assert_eq!(renew(key(1, 2)).unwrap(), LeaseOutcome::Stale);
    // A new coordinator epoch restarts the sequence and still wins.
    assert_eq!(renew(key(2, 1)).unwrap(), LeaseOutcome::Extended);
    assert_eq!(w.store.lease(id).unwrap(), Some((key(2, 1), false)));
    // The old coordinator is now rejected outright.
    let e = renew(key(1, 99)).err().unwrap();
    assert!(matches!(e, StorageError::StaleEpoch { .. }), "{e:?}");
    assert_eq!(e.code(), ErrorCode::StaleEpoch);
    assert_eq!(w.store.lease(id).unwrap(), Some((key(2, 1), false)));
    // An expiry is remembered and a late renewal does not undo it.
    w.store.expire_lease(id, 60).unwrap();
    w.store.expire_lease(id, 61).unwrap();
    assert_eq!(w.store.lease(id).unwrap(), Some((key(2, 1), true)));
    assert!(renew(key(2, 5)).is_err());
    assert!(w
        .store
        .renew_lease(&AttemptId::generate(), key(2, 1), 1)
        .is_err());
}

#[test]
fn a_lease_is_not_renewed_while_cancelling_or_after_the_end() {
    let w = wx();
    let a = accepted(&w);
    let id = &a.attempt_id;
    drive(&w, id, &TO_READY);
    w.store.renew_lease(id, key(1, 1), 1).unwrap();
    drive(&w, id, &[AttemptState::Cancelling]);
    assert!(w.store.renew_lease(id, key(1, 2), 2).is_err());
    drive(&w, id, &[AttemptState::Finalizing]);
    assert!(w.store.renew_lease(id, key(1, 3), 3).is_err());
    w.store.finalize(id, done(Outcome::Cancelled), 4).unwrap();
    assert!(w.store.renew_lease(id, key(1, 4), 5).is_err());
    // A never-started cancel request alone also blocks renewal.
    let b = accepted(&w);
    w.store
        .cancel(&b.attempt_id, CoordinatorEpoch(1), 6)
        .unwrap();
    assert!(w.store.renew_lease(&b.attempt_id, key(1, 1), 7).is_err());
}

#[test]
fn worker_terminal_is_fixed_once() {
    let w = wx();
    let a = accepted(&w);
    let id = &a.attempt_id;
    drive(
        &w,
        id,
        &[
            AttemptState::Preparing,
            AttemptState::Ready,
            AttemptState::Starting,
            AttemptState::Active,
            AttemptState::Finalizing,
        ],
    );
    let wins: Vec<bool> = std::thread::scope(|s| {
        let hs: Vec<_> = [Outcome::Succeeded, Outcome::Cancelled, Outcome::Failed]
            .into_iter()
            .map(|o| {
                let st = &w.store;
                s.spawn(move || st.finalize(id, done(o), 50).unwrap().won())
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    assert_eq!(wins.iter().filter(|w| **w).count(), 1);
    let rec = w.store.get(id).unwrap().unwrap();
    assert!(rec.state.is_terminal() && rec.termination.is_some());
    assert_eq!(rec.terminal_at_ms, Some(50));
    // A receipt cannot be set around finalize.
    for r in [
        Receipt::Terminal,
        Receipt::Collected,
        Receipt::WorkerAccepted,
        Receipt::CoordinatorAccepted,
    ] {
        assert!(w.store.mark_receipt(id, r, 1).is_err());
    }
}

#[test]
fn receipts_log_positions_and_results() {
    let w = wx();
    let a = accepted(&w);
    let id = &a.attempt_id;
    assert!(w
        .store
        .mark_receipt(id, Receipt::CommandRegistered, 5)
        .unwrap());
    assert!(!w
        .store
        .mark_receipt(id, Receipt::CommandRegistered, 6)
        .unwrap());
    assert!(w.store.mark_receipt(id, Receipt::Running, 7).unwrap());
    let p = |w, d, s| LogPosition {
        written_bytes: w,
        dropped_bytes: d,
        last_seq: s,
    };
    assert_eq!(
        w.store.log_position(id, "stdout").unwrap(),
        LogPosition::default()
    );
    assert_eq!(
        w.store
            .update_log_position(id, "stdout", p(100, 0, 3))
            .unwrap(),
        p(100, 0, 3)
    );
    // Never backwards, per column.
    assert_eq!(
        w.store
            .update_log_position(id, "stdout", p(50, 0, 2))
            .unwrap(),
        p(100, 0, 3)
    );
    let pos = w
        .store
        .update_log_position(id, "stdout", p(120, 900, 4))
        .unwrap();
    assert_eq!(pos, p(120, 900, 4));
    assert!(pos.truncated() && !LogPosition::default().truncated());
    assert_eq!(
        w.store.log_position(id, "stderr").unwrap(),
        LogPosition::default()
    );
    assert!(w
        .store
        .update_log_position(&AttemptId::generate(), "stdout", p(1, 0, 1))
        .is_err());
    // Results only at terminal, once.
    assert!(w.store.record_result(id, &json!({"ok": 1})).is_err());
    drive(
        &w,
        id,
        &[
            AttemptState::Preparing,
            AttemptState::Ready,
            AttemptState::Starting,
            AttemptState::Active,
            AttemptState::Finalizing,
        ],
    );
    w.store.finalize(id, done(Outcome::Succeeded), 9).unwrap();
    assert_eq!(w.store.get_result(id).unwrap(), None);
    w.store.record_result(id, &json!({"ok": 1})).unwrap();
    w.store.record_result(id, &json!({"ok": 1})).unwrap();
    assert!(matches!(
        w.store.record_result(id, &json!({"ok": 2})).err().unwrap(),
        StorageError::Immutable(_)
    ));
    assert_eq!(w.store.get_result(id).unwrap(), Some(json!({"ok": 1})));
    assert_eq!(w.store.get_result(&AttemptId::generate()).unwrap(), None);
}

#[test]
fn worker_gc_waits_for_the_ack_and_the_retention_and_skips_pins() {
    let w = wx();
    let mut ids = Vec::new();
    for _ in 0..4 {
        let a = accepted(&w);
        drive(
            &w,
            &a.attempt_id,
            &[
                AttemptState::Preparing,
                AttemptState::Ready,
                AttemptState::Starting,
                AttemptState::Active,
                AttemptState::Finalizing,
            ],
        );
        w.store
            .finalize(&a.attempt_id, done(Outcome::Succeeded), 1_000)
            .unwrap();
        ids.push(a.attempt_id);
    }
    let running = accepted(&w).attempt_id;
    for (i, id) in ids.iter().chain([&running]).enumerate() {
        w.store
            .add_blob_ref(
                &blob(&format!("r{i}"), id, BlobKind::Result, &format!("res/{i}")),
                1,
            )
            .unwrap();
    }
    // r0: acked and old. r1: not acked. r2: acked but pinned. r3: acked, recent.
    for i in [0, 2, 3] {
        assert!(w
            .store
            .ack_collected(&ids[i], CoordinatorEpoch(1), 2_000)
            .unwrap());
    }
    assert!(!w
        .store
        .ack_collected(&ids[0], CoordinatorEpoch(1), 2_001)
        .unwrap());
    assert!(w
        .store
        .ack_collected(&running, CoordinatorEpoch(1), 1)
        .is_err());
    w.store.pin_blob("r2").unwrap();
    let now = 1_000 + 7 * DAY;
    // r3 would be old enough too; make it recent by giving it a later terminal.
    let report = w.store.gc(now - 1, 7 * DAY).unwrap();
    assert!(
        report.removed.is_empty(),
        "one ms short of retention: {report:?}"
    );
    let report = w.store.gc(now, 7 * DAY).unwrap();
    let removed: Vec<_> = report.removed.iter().map(|r| r.ref_id.as_str()).collect();
    assert_eq!(removed, vec!["r0", "r3"]);
    assert!(report
        .kept
        .contains(&("r1".to_string(), policy::KeepReason::AwaitingCollection)));
    assert!(report
        .kept
        .contains(&("r2".to_string(), policy::KeepReason::Pinned)));
    assert!(report
        .kept
        .contains(&("r4".to_string(), policy::KeepReason::NotTerminal)));
    // The dedup record of a GC'd attempt stays: an old submit is not new work.
    let old = w.store.get(&ids[0]).unwrap().unwrap();
    let mut again = acceptance(&old.attempt_id, &old.homework_id, 'a', 1);
    again.payload_hash = old.payload_hash.clone();
    assert!(matches!(
        w.store.submit(again, now).unwrap(),
        SubmitOutcome::Existing(_)
    ));
}

#[test]
fn stale_coordinator_epochs_are_rejected_for_every_control_request() {
    let w = wx();
    let (a, hw) = (AttemptId::generate(), HomeworkId::generate());
    w.store.submit(acceptance(&a, &hw, 'a', 5), 1).unwrap();
    let old = CoordinatorEpoch(4);
    let e = w.store.cancel(&a, old, 2).err().unwrap();
    assert!(matches!(e, StorageError::StaleEpoch { .. }));
    assert!(w.store.retire(&a, old, 2).is_err());
    assert!(w
        .store
        .submit(
            acceptance(&AttemptId::generate(), &HomeworkId::generate(), 'a', 4),
            2
        )
        .is_err());
    assert!(w
        .store
        .get(&a)
        .unwrap()
        .unwrap()
        .cancel_requested_at_ms
        .is_none());
    assert!(w.store.tombstone(&a).unwrap().is_none());
}

#[test]
fn a_worker_restart_keeps_the_launch_claim_and_the_agent_epoch_grows() {
    let w = wx();
    let a = accepted(&w);
    drive(&w, &a.attempt_id, &TO_READY);
    w.store
        .claim_launch(&a.attempt_id, &node("build"), 30)
        .unwrap();
    // A claim made in an uncommitted transaction is lost, the committed one is not.
    w.store
        .db
        .call(|c| {
            let tx = c.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            tx.execute("DELETE FROM command_launch", [])?;
            Ok(())
        })
        .unwrap();
    let Wx { dir, store, id } = w;
    drop(store);
    let store = WorkerStore::open(dir.path(), &id).unwrap();
    assert_eq!(store.start_agent(100).unwrap(), AgentEpoch(2));
    assert_eq!(store.start_agent(101).unwrap(), AgentEpoch(3));
    let live = store.list_nonterminal().unwrap();
    assert_eq!(live.len(), 1);
    assert_eq!(live[0].state, AttemptState::Starting);
    assert!(live[0].state.recovery_must_not_respawn());
    assert_eq!(
        store
            .claim_launch(&a.attempt_id, &node("build"), 102)
            .unwrap(),
        LaunchClaim::AlreadyClaimed { identity: None }
    );
    // Another worker's database is not ours.
    let other = WorkerId::parse("mini-b").unwrap();
    assert!(WorkerStore::open(dir.path(), &other).is_err());
    let ro = WorkerStore::open_read_only(dir.path()).unwrap();
    assert_eq!(ro.worker_id(), &id);
    assert!(ro.start_agent(1).is_err());
}

#[test]
fn database_files_of_the_wrong_kind_are_not_opened() {
    let dir = tempfile::tempdir().unwrap();
    drop(WorkerStore::open(dir.path(), &WorkerId::parse("w").unwrap()).unwrap());
    std::fs::rename(
        dir.path().join("worker.db"),
        dir.path().join("coordinator.db"),
    )
    .unwrap();
    let e = CoordinatorStore::open(dir.path(), 1).err().unwrap();
    assert!(matches!(e, StorageError::KindMismatch { .. }), "{e:?}");
    assert_eq!(e.code(), ErrorCode::ProtocolIncompatible);
}

#[test]
fn error_codes_map_to_the_wire_codes() {
    use StorageError::*;
    let a = AttemptId::generate();
    let cases: Vec<(StorageError, ErrorCode)> = vec![
        (
            Incompatible(policy::Incompat::Foreign),
            ErrorCode::ProtocolIncompatible,
        ),
        (Locked("x".into()), ErrorCode::Conflict),
        (PayloadConflict(a.clone()), ErrorCode::Conflict),
        (NotFound { what: "x".into() }, ErrorCode::NotFound),
        (Retired(a.clone()), ErrorCode::Retired),
        (LeaseNotRenewable(a), ErrorCode::LeaseNotRenewable),
        (Invalid("x".into()), ErrorCode::InvalidInput),
        (Corrupt("x".into()), ErrorCode::Internal),
        (Closed, ErrorCode::Internal),
        (Panicked, ErrorCode::Internal),
    ];
    for (e, c) in cases {
        assert_eq!(e.code(), c, "{e}");
    }
}
