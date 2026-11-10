//! Pure decisions of the storage layer: schema compatibility, idempotent
//! submit, launch claims, lease renewal and GC selection.
//!
//! Nothing here touches SQLite or the clock. The repositories gather the facts
//! (inside the transaction that acts on them) and these functions decide. They
//! are under the coverage gate and the mutation gate.

use crate::ids::PayloadHash;
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

/// An attempt the worker already holds.
#[derive(Debug, Clone, Copy)]
pub struct ExistingAttempt<'a> {
    pub payload_hash: &'a PayloadHash,
    pub retired: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubmitDecision {
    /// Persist it (then, and only then, answer).
    Insert,
    /// Same attempt, same payload: answer with the existing state.
    Existing,
    /// Same attempt, different payload.
    Conflict,
    /// A tombstone exists: the attempt must not start.
    Retired,
}

/// `attempt_id + payload_hash` makes a submit idempotent. `tombstoned` is true
/// when a cancel or retire tombstone exists for the attempt id.
pub fn decide_submit(
    existing: Option<ExistingAttempt<'_>>,
    tombstoned: bool,
    incoming: &PayloadHash,
) -> SubmitDecision {
    match existing {
        Some(e) if e.payload_hash != incoming => SubmitDecision::Conflict,
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
/// persisted in the same commit that grants this.
pub fn decide_claim(
    state: AttemptState,
    cancel_requested: bool,
    retired: bool,
    already_claimed: bool,
) -> ClaimDecision {
    if already_claimed {
        ClaimDecision::AlreadyClaimed
    } else if cancel_requested || retired {
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
    /// The coordinator holds the verified required outputs (on a worker: it
    /// received `ack_collected`).
    pub collected: bool,
    /// Required artifacts without a verified, persisted receipt.
    pub missing_required_artifacts: u32,
    /// Cache or other references still in use.
    pub pins: u32,
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

/// Inputs, logs and results are kept for `retention_ms` after terminal. A
/// running, awaiting-collection or referenced item is never collected, and an
/// unrecovered required artifact stops deletion.
pub fn decide_gc(c: &GcCandidate, now_ms: u64, retention_ms: u64) -> GcVerdict {
    let Some(terminal_at) = c.terminal_at_ms else {
        return GcVerdict::Keep(KeepReason::NotTerminal);
    };
    if c.pins > 0 {
        GcVerdict::Keep(KeepReason::Pinned)
    } else if !c.collected {
        GcVerdict::Keep(KeepReason::AwaitingCollection)
    } else if c.missing_required_artifacts > 0 {
        GcVerdict::Keep(KeepReason::MissingRequiredArtifact)
    } else if now_ms.saturating_sub(terminal_at) < retention_ms {
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

    #[test]
    fn submit_decisions() {
        let (a, b) = (h('a'), h('b'));
        let ex = |hash, retired| {
            Some(ExistingAttempt {
                payload_hash: hash,
                retired,
            })
        };
        assert_eq!(decide_submit(None, false, &a), SubmitDecision::Insert);
        assert_eq!(decide_submit(None, true, &a), SubmitDecision::Retired);
        assert_eq!(
            decide_submit(ex(&a, false), false, &a),
            SubmitDecision::Existing
        );
        assert_eq!(
            decide_submit(ex(&a, false), true, &a),
            SubmitDecision::Existing
        );
        assert_eq!(
            decide_submit(ex(&a, false), false, &b),
            SubmitDecision::Conflict
        );
        assert_eq!(
            decide_submit(ex(&a, true), false, &a),
            SubmitDecision::Retired
        );
        assert_eq!(
            decide_submit(ex(&a, true), true, &b),
            SubmitDecision::Conflict
        );
    }

    #[test]
    fn claim_decisions() {
        use AttemptState::*;
        let d = decide_claim;
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
            collected: true,
            missing_required_artifacts: 0,
            pins: 0,
        }
    }

    #[test]
    fn gc_decisions() {
        let r = 7 * DAY;
        let at = 1_000 + r;
        assert_eq!(decide_gc(&cand(), at, r), GcVerdict::Collect);
        assert_eq!(decide_gc(&cand(), at + 1, r), GcVerdict::Collect);
        assert_eq!(
            decide_gc(&cand(), at - 1, r),
            GcVerdict::Keep(KeepReason::WithinRetention)
        );
        // A clock that went backwards never collects early.
        assert_eq!(
            decide_gc(&cand(), 0, r),
            GcVerdict::Keep(KeepReason::WithinRetention)
        );
        let not_terminal = GcCandidate {
            terminal_at_ms: None,
            ..cand()
        };
        assert_eq!(
            decide_gc(&not_terminal, u64::MAX, 0),
            GcVerdict::Keep(KeepReason::NotTerminal)
        );
        let pinned = GcCandidate { pins: 1, ..cand() };
        assert_eq!(
            decide_gc(&pinned, at, r),
            GcVerdict::Keep(KeepReason::Pinned)
        );
        let waiting = GcCandidate {
            collected: false,
            ..cand()
        };
        assert_eq!(
            decide_gc(&waiting, at, r),
            GcVerdict::Keep(KeepReason::AwaitingCollection)
        );
        let missing = GcCandidate {
            missing_required_artifacts: 1,
            ..cand()
        };
        assert_eq!(
            decide_gc(&missing, at, r),
            GcVerdict::Keep(KeepReason::MissingRequiredArtifact)
        );
        // Precedence: pinned over awaiting over missing over retention.
        let all = GcCandidate {
            collected: false,
            missing_required_artifacts: 1,
            pins: 1,
            ..cand()
        };
        assert_eq!(decide_gc(&all, 0, r), GcVerdict::Keep(KeepReason::Pinned));
        let aw = GcCandidate { pins: 0, ..all };
        assert_eq!(
            decide_gc(&aw, 0, r),
            GcVerdict::Keep(KeepReason::AwaitingCollection)
        );
        let mi = GcCandidate {
            collected: true,
            ..aw
        };
        assert_eq!(
            decide_gc(&mi, 0, r),
            GcVerdict::Keep(KeepReason::MissingRequiredArtifact)
        );
    }
}
