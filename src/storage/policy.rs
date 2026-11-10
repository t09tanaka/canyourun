//! Pure decisions of the storage layer: schema compatibility, idempotent
//! submit, launch claims, lease renewal and GC selection.
//!
//! Nothing here touches SQLite or the clock. The repositories gather the facts
//! (inside the transaction that acts on them) and these functions decide. They
//! are under the coverage gate and the mutation gate.

use crate::ids::{Generation, HomeworkId, PayloadHash, RunId};
use crate::state::AttemptState;
use crate::wire::LeaseKey;

// ---------------------------------------------------------------- schema

/// Which schema versions a binary understands (spec chapter 18: the
/// compatibility table of readable and writable versions).
///
/// * Read-only access reads `min_readable..=current` as they are.
/// * Read-write access needs the current version, or migrates up from
///   `min_migratable..current` first.
/// * A newer version is never touched.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompatTable {
    pub current: u32,
    pub min_readable: u32,
    pub min_migratable: u32,
}

/// How the database is opened.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Access {
    ReadOnly,
    ReadWrite,
}

/// What a probe of the database file found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Found {
    /// No file, or no tables at all.
    Empty,
    /// Tables exist but there is no usable `schema_meta`: not ours.
    Foreign,
    Version(u32),
}

/// What to do to the database before using it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaPlan {
    /// Create the schema from scratch (read-write only).
    Create,
    /// Use the file as it is.
    AsIs,
    /// Run the migration steps `from -> from + 1 -> ... -> current`.
    Migrate { from: u32 },
}

/// Why the file cannot be used. None of these modify the file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Incompat {
    /// Not a remote-task-runner database.
    Foreign,
    /// Read-only access to a database that has no schema yet.
    Empty,
    /// Written by a newer binary.
    Newer { found: u32, current: u32 },
    /// Older than this binary can read or migrate.
    TooOld { found: u32, oldest: u32 },
}

pub fn plan_schema(
    found: Found,
    access: Access,
    table: CompatTable,
) -> Result<SchemaPlan, Incompat> {
    let version = match found {
        Found::Foreign => return Err(Incompat::Foreign),
        Found::Empty => {
            return match access {
                Access::ReadWrite => Ok(SchemaPlan::Create),
                Access::ReadOnly => Err(Incompat::Empty),
            };
        }
        Found::Version(v) => v,
    };
    if version > table.current {
        return Err(Incompat::Newer {
            found: version,
            current: table.current,
        });
    }
    match access {
        Access::ReadOnly if version < table.min_readable => Err(Incompat::TooOld {
            found: version,
            oldest: table.min_readable,
        }),
        Access::ReadOnly => Ok(SchemaPlan::AsIs),
        Access::ReadWrite if version == table.current => Ok(SchemaPlan::AsIs),
        Access::ReadWrite if version >= table.min_migratable => {
            Ok(SchemaPlan::Migrate { from: version })
        }
        Access::ReadWrite => Err(Incompat::TooOld {
            found: version,
            oldest: table.min_migratable,
        }),
    }
}

// ---------------------------------------------------------------- submit

/// What makes a submit "the same": the payload and where it belongs. A resend
/// that differs in any of these is a different request, not a replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubmitKey<'a> {
    pub run_id: &'a RunId,
    pub homework_id: &'a HomeworkId,
    pub generation: Generation,
    pub payload_hash: &'a PayloadHash,
}

/// An attempt the worker already holds.
#[derive(Debug, Clone, Copy)]
pub struct ExistingAttempt<'a> {
    pub key: SubmitKey<'a>,
    pub retired: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubmitDecision {
    /// Persist it (then, and only then, answer).
    Insert,
    /// Same attempt, same request: answer with the existing state.
    Existing,
    /// Same attempt id, but the payload, run, homework or generation differs.
    Conflict,
    /// A tombstone exists: the attempt must not start.
    Retired,
}

/// `attempt_id` plus the [`SubmitKey`] makes a submit idempotent. `tombstoned`
/// is true when a cancel or retire tombstone exists for the attempt id.
pub fn decide_submit(
    existing: Option<ExistingAttempt<'_>>,
    tombstoned: bool,
    incoming: SubmitKey<'_>,
) -> SubmitDecision {
    match existing {
        Some(e) if e.key != incoming => SubmitDecision::Conflict,
        Some(e) if e.retired => SubmitDecision::Retired,
        Some(_) => SubmitDecision::Existing,
        None if tombstoned => SubmitDecision::Retired,
        None => SubmitDecision::Insert,
    }
}

// ---------------------------------------------------------------- launch

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimDecision {
    /// First node: `Ready -> Starting` and the spawn authority, together.
    StartAndGrant,
    /// A further node of an attempt that is already starting or active.
    Grant,
    /// The node was claimed before. Recovery must check the recorded process
    /// identity; it must not spawn again.
    AlreadyClaimed,
    /// A cancel or retire, or a state that cannot start commands.
    Refused,
}

/// Whether the supervisor may spawn the node's command. The launch intent is
/// persisted in the same commit that grants this. An expired lease means the
/// right to run is gone: nothing new starts.
pub fn decide_claim(
    state: AttemptState,
    cancel_requested: bool,
    retired: bool,
    expired: bool,
    already_claimed: bool,
) -> ClaimDecision {
    if already_claimed {
        ClaimDecision::AlreadyClaimed
    } else if cancel_requested || retired || expired {
        ClaimDecision::Refused
    } else if AttemptState::grants_spawn(state, AttemptState::Starting) {
        ClaimDecision::StartAndGrant
    } else if matches!(state, AttemptState::Starting | AttemptState::Active) {
        ClaimDecision::Grant
    } else {
        ClaimDecision::Refused
    }
}

// ---------------------------------------------------------------- lease

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseDecision {
    /// Store the key and extend.
    Extend,
    /// A replay or an older key: not an error, `extended: false`.
    Stale,
    /// Expired, cancelling, finalizing, terminal or retired.
    Refuse,
}

/// A late renewal never revives an attempt that expired, is being cancelled
/// or retired, or has ended.
pub fn decide_lease(
    state: AttemptState,
    retired: bool,
    expired: bool,
    cancel_requested: bool,
    applied: Option<LeaseKey>,
    incoming: LeaseKey,
) -> LeaseDecision {
    let ended = matches!(
        state,
        AttemptState::Cancelling | AttemptState::Finalizing | AttemptState::Terminal
    );
    if retired || expired || cancel_requested || ended {
        LeaseDecision::Refuse
    } else if incoming.extends(applied) {
        LeaseDecision::Extend
    } else {
        LeaseDecision::Stale
    }
}

// ---------------------------------------------------------------- GC

/// Facts about the owner of a reference (an attempt).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GcCandidate {
    /// When the attempt became terminal; `None` while it is not.
    pub terminal_at_ms: Option<u64>,
    /// When a retire was confirmed (the attempt is known to have stopped), for
    /// an attempt that never became terminal.
    pub retired_at_ms: Option<u64>,
    /// The coordinator holds the verified required outputs (on a worker: it
    /// received `ack_collected`).
    pub collected: bool,
    /// The owner gave up collecting (the coordinator recorded a failed
    /// collection): nothing is awaited any more.
    pub collection_failed_at_ms: Option<u64>,
    /// Required artifacts without a verified, persisted receipt.
    pub missing_required_artifacts: u32,
    /// Cache or other references still in use.
    pub pins: u32,
}

impl GcCandidate {
    /// The moment retention is measured from: the latest of the end events
    /// that happened. `None` while the attempt may still be running.
    pub fn ended_at_ms(&self) -> Option<u64> {
        let ended = self.terminal_at_ms.max(self.retired_at_ms)?;
        Some(ended.max(self.collection_failed_at_ms.unwrap_or(0)))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeepReason {
    NotTerminal,
    Pinned,
    AwaitingCollection,
    MissingRequiredArtifact,
    WithinRetention,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GcVerdict {
    Collect,
    Keep(KeepReason),
}

/// Inputs, logs and results are kept for `retention_ms` after the attempt
/// ended (terminal, confirmed retired, or the collection was given up). A
/// running, referenced or awaiting-collection item is never collected, and an
/// unrecovered required artifact stops deletion, until the wait is over:
///
/// * the owner recorded a failed collection, or
/// * the retire was confirmed (the outputs of a retired attempt are not
///   wanted), or
/// * `ack_wait_ms` (a worker whose `ack_collected` never came) has passed
///   since the end.
///
/// So every way an attempt can end reaches a deadline.
pub fn decide_gc(
    c: &GcCandidate,
    now_ms: u64,
    retention_ms: u64,
    ack_wait_ms: Option<u64>,
) -> GcVerdict {
    let Some(ended) = c.ended_at_ms() else {
        return GcVerdict::Keep(KeepReason::NotTerminal);
    };
    let waited = now_ms.saturating_sub(ended);
    let abandoned = c.collection_failed_at_ms.is_some()
        || c.retired_at_ms.is_some()
        || ack_wait_ms.is_some_and(|w| waited >= w);
    if c.pins > 0 {
        GcVerdict::Keep(KeepReason::Pinned)
    } else if !abandoned && !c.collected {
        GcVerdict::Keep(KeepReason::AwaitingCollection)
    } else if !abandoned && c.missing_required_artifacts > 0 {
        GcVerdict::Keep(KeepReason::MissingRequiredArtifact)
    } else if waited < retention_ms {
        GcVerdict::Keep(KeepReason::WithinRetention)
    } else {
        GcVerdict::Collect
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::CoordinatorEpoch;

    const T: CompatTable = CompatTable {
        current: 5,
        min_readable: 2,
        min_migratable: 3,
    };

    fn plan(f: Found, a: Access) -> Result<SchemaPlan, Incompat> {
        plan_schema(f, a, T)
    }

    #[test]
    fn empty_is_created_only_for_writers() {
        assert_eq!(
            plan(Found::Empty, Access::ReadWrite),
            Ok(SchemaPlan::Create)
        );
        assert_eq!(plan(Found::Empty, Access::ReadOnly), Err(Incompat::Empty));
    }

    #[test]
    fn foreign_is_refused_for_both() {
        for a in [Access::ReadOnly, Access::ReadWrite] {
            assert_eq!(plan(Found::Foreign, a), Err(Incompat::Foreign));
        }
    }

    #[test]
    fn newer_is_refused_for_both() {
        for a in [Access::ReadOnly, Access::ReadWrite] {
            assert_eq!(
                plan(Found::Version(6), a),
                Err(Incompat::Newer {
                    found: 6,
                    current: 5
                })
            );
            assert_eq!(
                plan(Found::Version(u32::MAX), a),
                Err(Incompat::Newer {
                    found: u32::MAX,
                    current: 5
                })
            );
            assert_ne!(plan(Found::Version(5), a), plan(Found::Version(6), a));
        }
    }

    #[test]
    fn read_only_reads_the_readable_range_as_is() {
        for v in 2..=5 {
            assert_eq!(
                plan(Found::Version(v), Access::ReadOnly),
                Ok(SchemaPlan::AsIs),
                "v{v}"
            );
        }
        assert_eq!(
            plan(Found::Version(1), Access::ReadOnly),
            Err(Incompat::TooOld {
                found: 1,
                oldest: 2
            })
        );
    }

    #[test]
    fn read_write_uses_current_and_migrates_the_migratable_range() {
        assert_eq!(
            plan(Found::Version(5), Access::ReadWrite),
            Ok(SchemaPlan::AsIs)
        );
        assert_eq!(
            plan(Found::Version(4), Access::ReadWrite),
            Ok(SchemaPlan::Migrate { from: 4 })
        );
        assert_eq!(
            plan(Found::Version(3), Access::ReadWrite),
            Ok(SchemaPlan::Migrate { from: 3 })
        );
        // Readable but not migratable: a writer refuses it.
        assert_eq!(
            plan(Found::Version(2), Access::ReadWrite),
            Err(Incompat::TooOld {
                found: 2,
                oldest: 3
            })
        );
        assert_eq!(
            plan(Found::Version(0), Access::ReadWrite),
            Err(Incompat::TooOld {
                found: 0,
                oldest: 3
            })
        );
    }

    fn h(c: char) -> PayloadHash {
        PayloadHash::parse(&format!("sha256:{}", c.to_string().repeat(64))).unwrap()
    }

    struct Ids {
        run: RunId,
        hw: HomeworkId,
    }

    fn ids() -> Ids {
        Ids {
            run: RunId::generate(),
            hw: HomeworkId::generate(),
        }
    }

    fn sk<'a>(i: &'a Ids, g: u64, hash: &'a PayloadHash) -> SubmitKey<'a> {
        SubmitKey {
            run_id: &i.run,
            homework_id: &i.hw,
            generation: Generation(g),
            payload_hash: hash,
        }
    }

    #[test]
    fn submit_decisions() {
        let (a, b) = (h('a'), h('b'));
        let i = ids();
        let ex = |g, hash, retired| {
            Some(ExistingAttempt {
                key: sk(&i, g, hash),
                retired,
            })
        };
        assert_eq!(
            decide_submit(None, false, sk(&i, 1, &a)),
            SubmitDecision::Insert
        );
        assert_eq!(
            decide_submit(None, true, sk(&i, 1, &a)),
            SubmitDecision::Retired
        );
        assert_eq!(
            decide_submit(ex(1, &a, false), false, sk(&i, 1, &a)),
            SubmitDecision::Existing
        );
        assert_eq!(
            decide_submit(ex(1, &a, false), true, sk(&i, 1, &a)),
            SubmitDecision::Existing
        );
        assert_eq!(
            decide_submit(ex(1, &a, false), false, sk(&i, 1, &b)),
            SubmitDecision::Conflict
        );
        assert_eq!(
            decide_submit(ex(1, &a, true), false, sk(&i, 1, &a)),
            SubmitDecision::Retired
        );
        assert_eq!(
            decide_submit(ex(1, &a, true), true, sk(&i, 1, &b)),
            SubmitDecision::Conflict
        );
    }

    #[test]
    fn a_resend_with_another_run_homework_or_generation_is_a_conflict() {
        let a = h('a');
        let i = ids();
        let existing = || {
            Some(ExistingAttempt {
                key: sk(&i, 1, &a),
                retired: false,
            })
        };
        assert_eq!(
            decide_submit(existing(), false, sk(&i, 2, &a)),
            SubmitDecision::Conflict
        );
        let other_run = Ids {
            run: RunId::generate(),
            hw: i.hw.clone(),
        };
        assert_eq!(
            decide_submit(existing(), false, sk(&other_run, 1, &a)),
            SubmitDecision::Conflict
        );
        let other_hw = Ids {
            run: i.run.clone(),
            hw: HomeworkId::generate(),
        };
        assert_eq!(
            decide_submit(existing(), false, sk(&other_hw, 1, &a)),
            SubmitDecision::Conflict
        );
        // A different key beats a tombstone and a retired record.
        assert_eq!(
            decide_submit(
                Some(ExistingAttempt {
                    key: sk(&i, 1, &a),
                    retired: true
                }),
                true,
                sk(&i, 2, &a)
            ),
            SubmitDecision::Conflict
        );
    }

    #[test]
    fn claim_decisions() {
        use AttemptState::*;
        let d = |s, cancel, retired, claimed| decide_claim(s, cancel, retired, false, claimed);
        assert_eq!(d(Ready, false, false, false), ClaimDecision::StartAndGrant);
        assert_eq!(d(Starting, false, false, false), ClaimDecision::Grant);
        assert_eq!(d(Active, false, false, false), ClaimDecision::Grant);
        for s in [Accepted, Preparing, Cancelling, Finalizing, Terminal] {
            assert_eq!(d(s, false, false, false), ClaimDecision::Refused, "{s:?}");
        }
        assert_eq!(d(Ready, true, false, false), ClaimDecision::Refused);
        assert_eq!(d(Active, true, false, false), ClaimDecision::Refused);
        assert_eq!(d(Ready, false, true, false), ClaimDecision::Refused);
        assert_eq!(d(Active, false, true, false), ClaimDecision::Refused);
        // Claimed before: never a second spawn, whatever else is true.
        for s in AttemptState::ALL {
            assert_eq!(d(s, false, false, true), ClaimDecision::AlreadyClaimed);
        }
        assert_eq!(d(Ready, true, true, true), ClaimDecision::AlreadyClaimed);
    }

    #[test]
    fn an_expired_lease_refuses_new_claims_but_not_a_replay() {
        use AttemptState::*;
        for s in [Ready, Starting, Active] {
            assert_eq!(
                decide_claim(s, false, false, true, false),
                ClaimDecision::Refused,
                "{s:?}"
            );
        }
        assert_eq!(
            decide_claim(Active, false, false, true, true),
            ClaimDecision::AlreadyClaimed
        );
        assert_eq!(
            decide_claim(Ready, false, false, false, false),
            ClaimDecision::StartAndGrant
        );
    }

    fn key(epoch: u64, seq: u64) -> LeaseKey {
        LeaseKey {
            coordinator_epoch: CoordinatorEpoch(epoch),
            lease_seq: seq,
        }
    }

    #[test]
    fn lease_decisions() {
        use AttemptState::*;
        let live = |s, retired, expired, cancel, applied, k| {
            decide_lease(s, retired, expired, cancel, applied, k)
        };
        let ok = |applied, k| live(Active, false, false, false, applied, k);
        assert_eq!(ok(None, key(1, 1)), LeaseDecision::Extend);
        assert_eq!(ok(Some(key(1, 1)), key(1, 2)), LeaseDecision::Extend);
        assert_eq!(ok(Some(key(1, 9)), key(2, 1)), LeaseDecision::Extend);
        assert_eq!(ok(Some(key(1, 2)), key(1, 2)), LeaseDecision::Stale);
        assert_eq!(ok(Some(key(1, 2)), key(1, 1)), LeaseDecision::Stale);
        assert_eq!(ok(Some(key(2, 1)), key(1, 99)), LeaseDecision::Stale);
        for s in [Accepted, Preparing, Ready, Starting, Active] {
            assert_eq!(
                live(s, false, false, false, None, key(1, 1)),
                LeaseDecision::Extend,
                "{s:?}"
            );
        }
        for s in [Cancelling, Finalizing, Terminal] {
            assert_eq!(
                live(s, false, false, false, None, key(1, 1)),
                LeaseDecision::Refuse,
                "{s:?}"
            );
        }
        assert_eq!(
            live(Active, true, false, false, None, key(1, 1)),
            LeaseDecision::Refuse
        );
        assert_eq!(
            live(Active, false, true, false, None, key(1, 1)),
            LeaseDecision::Refuse
        );
        assert_eq!(
            live(Active, false, false, true, None, key(1, 1)),
            LeaseDecision::Refuse
        );
        // Refusal wins over staleness.
        assert_eq!(
            live(Terminal, false, false, false, Some(key(9, 9)), key(1, 1)),
            LeaseDecision::Refuse
        );
    }

    const DAY: u64 = 86_400_000;

    fn cand() -> GcCandidate {
        GcCandidate {
            terminal_at_ms: Some(1_000),
            retired_at_ms: None,
            collected: true,
            collection_failed_at_ms: None,
            missing_required_artifacts: 0,
            pins: 0,
        }
    }

    fn gc(c: &GcCandidate, now: u64, retention: u64) -> GcVerdict {
        decide_gc(c, now, retention, None)
    }

    #[test]
    fn gc_decisions() {
        let r = 7 * DAY;
        let at = 1_000 + r;
        assert_eq!(gc(&cand(), at, r), GcVerdict::Collect);
        assert_eq!(gc(&cand(), at + 1, r), GcVerdict::Collect);
        assert_eq!(
            gc(&cand(), at - 1, r),
            GcVerdict::Keep(KeepReason::WithinRetention)
        );
        // A clock that went backwards never collects early.
        assert_eq!(
            gc(&cand(), 0, r),
            GcVerdict::Keep(KeepReason::WithinRetention)
        );
        let not_terminal = GcCandidate {
            terminal_at_ms: None,
            ..cand()
        };
        assert_eq!(
            gc(&not_terminal, u64::MAX, 0),
            GcVerdict::Keep(KeepReason::NotTerminal)
        );
        let pinned = GcCandidate { pins: 1, ..cand() };
        assert_eq!(gc(&pinned, at, r), GcVerdict::Keep(KeepReason::Pinned));
        let waiting = GcCandidate {
            collected: false,
            ..cand()
        };
        assert_eq!(
            gc(&waiting, at, r),
            GcVerdict::Keep(KeepReason::AwaitingCollection)
        );
        let missing = GcCandidate {
            missing_required_artifacts: 1,
            ..cand()
        };
        assert_eq!(
            gc(&missing, at, r),
            GcVerdict::Keep(KeepReason::MissingRequiredArtifact)
        );
        // Precedence: pinned over awaiting over missing over retention.
        let all = GcCandidate {
            collected: false,
            missing_required_artifacts: 1,
            pins: 1,
            ..cand()
        };
        assert_eq!(gc(&all, 0, r), GcVerdict::Keep(KeepReason::Pinned));
        let aw = GcCandidate { pins: 0, ..all };
        assert_eq!(
            gc(&aw, 0, r),
            GcVerdict::Keep(KeepReason::AwaitingCollection)
        );
        let mi = GcCandidate {
            collected: true,
            ..aw
        };
        assert_eq!(
            gc(&mi, 0, r),
            GcVerdict::Keep(KeepReason::MissingRequiredArtifact)
        );
    }

    #[test]
    fn every_way_an_attempt_ends_reaches_a_deadline() {
        let r = 7 * DAY;
        let late = 100 * DAY;
        // Terminal, collection given up: no longer awaited, even with missing
        // artifacts; the clock starts at the later of terminal and the failure.
        let failed = GcCandidate {
            collected: false,
            missing_required_artifacts: 2,
            collection_failed_at_ms: Some(5_000),
            ..cand()
        };
        assert_eq!(gc(&failed, late, r), GcVerdict::Collect);
        assert_eq!(
            gc(&failed, 5_000 + r - 1, r),
            GcVerdict::Keep(KeepReason::WithinRetention)
        );
        assert_eq!(gc(&failed, 5_000 + r, r), GcVerdict::Collect);
        // A failure recorded earlier than the terminal time cannot move the
        // clock back.
        let early = GcCandidate {
            collection_failed_at_ms: Some(10),
            ..failed
        };
        assert_eq!(
            gc(&early, 1_000 + r - 1, r),
            GcVerdict::Keep(KeepReason::WithinRetention)
        );
        // Retire confirmed without ever becoming terminal.
        let retired = GcCandidate {
            terminal_at_ms: None,
            retired_at_ms: Some(2_000),
            collected: false,
            missing_required_artifacts: 1,
            ..cand()
        };
        assert_eq!(gc(&retired, late, r), GcVerdict::Collect);
        assert_eq!(
            gc(&retired, 2_000 + r - 1, r),
            GcVerdict::Keep(KeepReason::WithinRetention)
        );
        // Pins protect even a retired or failed attempt.
        assert_eq!(
            gc(&GcCandidate { pins: 1, ..retired }, late, r),
            GcVerdict::Keep(KeepReason::Pinned)
        );
        // A failure record without an end is not an end.
        let no_end = GcCandidate {
            terminal_at_ms: None,
            collection_failed_at_ms: Some(1),
            ..cand()
        };
        assert_eq!(
            gc(&no_end, late, r),
            GcVerdict::Keep(KeepReason::NotTerminal)
        );
        // A worker that never got its ack gives up after the wait, and only then.
        let unacked = GcCandidate {
            collected: false,
            ..cand()
        };
        let wait = Some(r);
        assert_eq!(
            decide_gc(&unacked, 1_000 + r - 1, r, wait),
            GcVerdict::Keep(KeepReason::AwaitingCollection)
        );
        assert_eq!(decide_gc(&unacked, 1_000 + r, r, wait), GcVerdict::Collect);
        // The wait never shortens the retention.
        assert_eq!(
            decide_gc(&unacked, 1_000 + 2 * DAY, 7 * DAY, Some(DAY)),
            GcVerdict::Keep(KeepReason::WithinRetention)
        );
        assert_eq!(
            decide_gc(&unacked, 1_000 + 7 * DAY, 7 * DAY, Some(DAY)),
            GcVerdict::Collect
        );
        // A running attempt waits forever.
        assert_eq!(
            decide_gc(&not_running(), u64::MAX, 0, Some(0)),
            GcVerdict::Keep(KeepReason::NotTerminal)
        );
    }

    fn not_running() -> GcCandidate {
        GcCandidate {
            terminal_at_ms: None,
            collected: false,
            ..cand()
        }
    }
}
