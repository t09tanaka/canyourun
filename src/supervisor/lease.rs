//! The supervisor's view of the execution lease (spec chapter 12).
//!
//! The lease is counted by the supervisor on the monotonic clock that
//! includes suspend; it never compares wall-clock times and never trusts a
//! timestamp from another host. The agent applies renewals to the worker
//! database; the supervisor observes the highest applied key and, whenever it
//! grows, restarts its own countdown from the moment it noticed.
//!
//! The supervisor only looks every `poll_ms`, so a renewal (or the grant at
//! the start) can have happened up to one poll before it is noticed. The
//! countdown is shortened by that delay (`duration - poll_ms`): the lease
//! never outlives what the coordinator granted.
//!
//! All functions take the current monotonic time as an argument, so the
//! decisions are pure.

use crate::wire::LeaseKey;
use serde::{Deserialize, Serialize};

/// What the agent tells a supervisor it starts about the lease that is
/// already running: the grant (the acceptance or the last renewal) was made
/// before the supervisor existed, so the supervisor counts what is left of it
/// and not a full period.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseSeed {
    /// When the grant runs out, on the worker's monotonic clock (suspend
    /// included). The agent and the supervisor read the same clock of the
    /// same boot.
    pub ends_mono_ms: u64,
    /// The highest key applied when the agent measured that. A different key
    /// in the database at the supervisor's start means a renewal arrived in
    /// between, and the seed says nothing about that one.
    #[serde(default)]
    pub key: Option<LeaseKey>,
}

/// Where the lease stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseStatus {
    Valid {
        remaining_ms: u64,
    },
    /// Final: a lease that ran out is never revived by a late renewal.
    Expired,
}

#[derive(Debug, Clone)]
pub struct LeaseClock {
    /// What one grant is worth after the noticing delay is taken off.
    window_ms: u64,
    deadline_ms: u64,
    applied: Option<LeaseKey>,
    expired: bool,
}

impl LeaseClock {
    /// The lease the agent granted by starting the supervisor: `duration_ms`
    /// from `now_ms`, less `poll_ms` for the delay in noticing it. `applied`
    /// is the highest key already in the database.
    pub fn start(now_ms: u64, duration_ms: u64, poll_ms: u64, applied: Option<LeaseKey>) -> Self {
        let window_ms = duration_ms.saturating_sub(poll_ms);
        LeaseClock {
            window_ms,
            deadline_ms: now_ms.saturating_add(window_ms),
            applied,
            expired: false,
        }
    }

    /// [`LeaseClock::start`] for a lease that began before the supervisor
    /// did. With a `seed` measured against the key that is still the applied
    /// one, the countdown ends where the seed says, never later than a full
    /// period from now (a seed from another boot would otherwise be a lease
    /// without end) and less `poll_ms` like every grant. A seed whose key is
    /// not the applied one is void: a renewal arrived after it was measured,
    /// and that grant is counted in full from now.
    pub fn start_seeded(
        now_ms: u64,
        duration_ms: u64,
        poll_ms: u64,
        applied: Option<LeaseKey>,
        seed: Option<LeaseSeed>,
    ) -> Self {
        let mut clock = Self::start(now_ms, duration_ms, poll_ms, applied);
        if let Some(seed) = seed.filter(|s| s.key == applied) {
            let left = seed.ends_mono_ms.saturating_sub(now_ms).min(duration_ms);
            clock.deadline_ms = now_ms.saturating_add(left.saturating_sub(poll_ms));
        }
        clock
    }

    /// Look at the database again at `now_ms`. `seen` is the highest key
    /// applied there and `db_expired` whether the row is marked expired.
    ///
    /// The deadline is judged first, so a renewal that is only noticed after
    /// the deadline cannot bring the lease back. A key that is not strictly
    /// greater than the applied one (a replay, an older renewal) changes
    /// nothing: renewal is idempotent.
    pub fn observe(
        &mut self,
        now_ms: u64,
        seen: Option<LeaseKey>,
        db_expired: bool,
    ) -> LeaseStatus {
        if self.expired || db_expired || now_ms >= self.deadline_ms {
            self.expired = true;
            return LeaseStatus::Expired;
        }
        if let Some(key) = seen {
            if key.extends(self.applied) {
                self.applied = Some(key);
                self.deadline_ms = now_ms.saturating_add(self.window_ms);
            }
        }
        LeaseStatus::Valid {
            remaining_ms: self.deadline_ms - now_ms,
        }
    }

    /// The last key whose grant this clock accepted, including its seed.
    pub fn applied_key(&self) -> Option<LeaseKey> {
        self.applied
    }

    pub fn is_expired(&self) -> bool {
        self.expired
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::CoordinatorEpoch;

    fn key(epoch: u64, seq: u64) -> LeaseKey {
        LeaseKey {
            coordinator_epoch: CoordinatorEpoch(epoch),
            lease_seq: seq,
        }
    }

    fn valid(ms: u64) -> LeaseStatus {
        LeaseStatus::Valid { remaining_ms: ms }
    }

    #[test]
    fn the_start_grants_one_full_lease() {
        let mut c = LeaseClock::start(1_000, 60_000, 0, None);
        assert_eq!(c.observe(1_000, None, false), valid(60_000));
        assert_eq!(c.observe(31_000, None, false), valid(30_000));
        assert_eq!(c.observe(60_999, None, false), valid(1));
        assert!(!c.is_expired());
        assert_eq!(c.observe(61_000, None, false), LeaseStatus::Expired);
        assert!(c.is_expired());
    }

    #[test]
    fn a_greater_key_restarts_the_countdown_from_when_it_was_seen() {
        let mut c = LeaseClock::start(0, 10_000, 0, Some(key(1, 1)));
        assert_eq!(c.applied_key(), Some(key(1, 1)));
        assert_eq!(c.observe(6_000, Some(key(1, 2)), false), valid(10_000));
        assert_eq!(c.applied_key(), Some(key(1, 2)));
        assert_eq!(c.observe(15_999, Some(key(1, 2)), false), valid(1));
        assert_eq!(
            c.observe(16_000, Some(key(1, 2)), false),
            LeaseStatus::Expired
        );
    }

    #[test]
    fn renewal_is_idempotent_a_replay_never_extends_again() {
        let mut c = LeaseClock::start(0, 10_000, 0, None);
        assert_eq!(c.observe(5_000, Some(key(1, 1)), false), valid(10_000));
        // The same key, seen again later, does not extend.
        assert_eq!(c.observe(9_000, Some(key(1, 1)), false), valid(6_000));
        // An older key does not either.
        assert_eq!(c.observe(9_500, Some(key(1, 0)), false), valid(5_500));
        assert_eq!(c.observe(9_500, Some(key(0, 99)), false), valid(5_500));
    }

    #[test]
    fn the_key_already_applied_at_start_is_not_a_renewal() {
        let mut c = LeaseClock::start(0, 10_000, 0, Some(key(1, 4)));
        assert_eq!(c.observe(8_000, Some(key(1, 4)), false), valid(2_000));
    }

    #[test]
    fn a_newer_epoch_wins_over_a_higher_sequence() {
        let mut c = LeaseClock::start(0, 10_000, 0, Some(key(1, 50)));
        assert_eq!(c.observe(4_000, Some(key(2, 1)), false), valid(10_000));
    }

    #[test]
    fn a_late_renewal_does_not_revive_an_expired_lease() {
        let mut c = LeaseClock::start(0, 10_000, 0, None);
        assert_eq!(
            c.observe(10_500, Some(key(1, 1)), false),
            LeaseStatus::Expired
        );
        assert_eq!(c.applied_key(), None);
        // Still expired afterwards, whatever arrives, and even if the clock
        // were to be read as earlier.
        assert_eq!(
            c.observe(10_600, Some(key(1, 2)), false),
            LeaseStatus::Expired
        );
        assert_eq!(c.observe(0, Some(key(9, 9)), false), LeaseStatus::Expired);
    }

    #[test]
    fn the_database_marking_the_lease_expired_is_final() {
        let mut c = LeaseClock::start(0, 10_000, 0, None);
        assert_eq!(c.observe(1, Some(key(1, 1)), true), LeaseStatus::Expired);
        assert!(c.is_expired());
        assert_eq!(c.observe(2, None, false), LeaseStatus::Expired);
    }

    #[test]
    fn a_long_suspend_expires_the_lease() {
        // The clock counts suspend: a jump of an hour is an hour.
        let mut c = LeaseClock::start(1_000, 60_000, 0, None);
        assert_eq!(
            c.observe(1_000 + 3_600_000, None, false),
            LeaseStatus::Expired
        );
    }

    #[test]
    fn the_noticing_delay_is_taken_off_every_grant() {
        // Polling every 100 ms: a grant is worth 100 ms less than it says.
        let mut c = LeaseClock::start(1_000, 10_000, 100, None);
        assert_eq!(c.observe(1_000, None, false), valid(9_900));
        assert_eq!(c.observe(10_899, None, false), valid(1));
        assert_eq!(c.observe(10_900, None, false), LeaseStatus::Expired);
        // A renewal noticed at 6_000 counts from there, also less 100 ms.
        let mut c = LeaseClock::start(0, 10_000, 100, Some(key(1, 1)));
        assert_eq!(c.observe(6_000, Some(key(1, 2)), false), valid(9_900));
        assert_eq!(
            c.observe(15_900, Some(key(1, 2)), false),
            LeaseStatus::Expired
        );
    }

    #[test]
    fn a_poll_longer_than_the_lease_expires_it_at_once() {
        let mut c = LeaseClock::start(500, 50, 100, None);
        assert_eq!(c.observe(500, None, false), LeaseStatus::Expired);
    }

    fn seed(ends: u64, key: Option<LeaseKey>) -> Option<LeaseSeed> {
        Some(LeaseSeed {
            ends_mono_ms: ends,
            key,
        })
    }

    #[test]
    fn a_seed_counts_what_is_left_of_the_grant_not_a_full_period() {
        // Granted at 0 for 10 s; the supervisor starts at 7 s.
        let mut c = LeaseClock::start_seeded(7_000, 10_000, 0, None, seed(10_000, None));
        assert_eq!(c.observe(7_000, None, false), valid(3_000));
        assert_eq!(c.observe(9_999, None, false), valid(1));
        assert_eq!(c.observe(10_000, None, false), LeaseStatus::Expired);
        // The same after a renewal the seed was measured against.
        let k = Some(key(1, 3));
        let mut c = LeaseClock::start_seeded(7_000, 10_000, 0, k, seed(10_000, k));
        assert_eq!(c.observe(7_000, k, false), valid(3_000));
        // A renewal noticed later is a full period again.
        assert_eq!(c.observe(8_000, Some(key(1, 4)), false), valid(10_000));
        // Without a seed, the start is a full period as before.
        let mut c = LeaseClock::start_seeded(7_000, 10_000, 0, k, None);
        assert_eq!(c.observe(7_000, k, false), valid(10_000));
    }

    #[test]
    fn the_noticing_delay_is_taken_off_a_seed_too() {
        let mut c = LeaseClock::start_seeded(7_000, 10_000, 100, None, seed(10_000, None));
        assert_eq!(c.observe(7_000, None, false), valid(2_900));
        assert_eq!(c.observe(9_900, None, false), LeaseStatus::Expired);
        // Less left than one poll: nothing is left.
        let mut c = LeaseClock::start_seeded(9_950, 10_000, 100, None, seed(10_000, None));
        assert_eq!(c.observe(9_950, None, false), LeaseStatus::Expired);
    }

    #[test]
    fn a_seed_that_ran_out_expires_the_lease_at_once() {
        let mut c = LeaseClock::start_seeded(10_000, 10_000, 0, None, seed(10_000, None));
        assert_eq!(c.observe(10_000, None, false), LeaseStatus::Expired);
        let mut c = LeaseClock::start_seeded(50_000, 10_000, 0, None, seed(10_000, None));
        assert_eq!(
            c.observe(50_000, Some(key(1, 1)), false),
            LeaseStatus::Expired
        );
    }

    #[test]
    fn a_seed_is_never_worth_more_than_one_period() {
        // A value of another boot's clock, or a wrong one.
        let mut c = LeaseClock::start_seeded(1_000, 10_000, 100, None, seed(u64::MAX, None));
        assert_eq!(c.observe(1_000, None, false), valid(9_900));
        let mut c = LeaseClock::start_seeded(1_000, 10_000, 0, None, seed(11_001, None));
        assert_eq!(c.observe(1_000, None, false), valid(10_000));
        let mut c = LeaseClock::start_seeded(1_000, 10_000, 0, None, seed(10_999, None));
        assert_eq!(c.observe(1_000, None, false), valid(9_999));
    }

    #[test]
    fn a_seed_measured_before_a_renewal_is_void() {
        // The agent measured against (1, 3); (1, 4) was applied before the
        // supervisor started: that grant is counted in full.
        let mut c = LeaseClock::start_seeded(
            7_000,
            10_000,
            0,
            Some(key(1, 4)),
            seed(7_500, Some(key(1, 3))),
        );
        assert_eq!(c.observe(7_000, Some(key(1, 4)), false), valid(10_000));
        // Also the first renewal after a grant without one, and a seed that
        // names a key the database does not have.
        let mut c = LeaseClock::start_seeded(7_000, 10_000, 0, Some(key(1, 1)), seed(7_500, None));
        assert_eq!(c.observe(7_000, Some(key(1, 1)), false), valid(10_000));
        let mut c = LeaseClock::start_seeded(7_000, 10_000, 0, None, seed(7_500, Some(key(1, 1))));
        assert_eq!(c.observe(7_000, None, false), valid(10_000));
    }

    #[test]
    fn extreme_values_do_not_overflow() {
        let mut c = LeaseClock::start(u64::MAX - 5, u64::MAX, 0, None);
        assert_eq!(c.observe(u64::MAX - 5, Some(key(1, 1)), false), valid(5));
        assert_eq!(c.observe(u64::MAX, None, false), LeaseStatus::Expired);
    }
}
