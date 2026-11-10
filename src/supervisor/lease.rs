//! The supervisor's view of the execution lease (spec chapter 12).
//!
//! The lease is counted by the supervisor on the monotonic clock that
//! includes suspend; it never compares wall-clock times and never trusts a
//! timestamp from another host. The agent applies renewals to the worker
//! database; the supervisor observes the highest applied key and, whenever it
//! grows, restarts its own countdown from the moment it noticed.
//!
//! All functions take the current monotonic time as an argument, so the
//! decisions are pure.

use crate::wire::LeaseKey;

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
    duration_ms: u64,
    deadline_ms: u64,
    applied: Option<LeaseKey>,
    expired: bool,
}

impl LeaseClock {
    /// The lease the agent granted by starting the supervisor: `duration_ms`
    /// from `now_ms`. `applied` is the highest key already in the database.
    pub fn start(now_ms: u64, duration_ms: u64, applied: Option<LeaseKey>) -> Self {
        LeaseClock {
            duration_ms,
            deadline_ms: now_ms.saturating_add(duration_ms),
            applied,
            expired: false,
        }
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
                self.deadline_ms = now_ms.saturating_add(self.duration_ms);
            }
        }
        LeaseStatus::Valid {
            remaining_ms: self.deadline_ms - now_ms,
        }
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
        let mut c = LeaseClock::start(1_000, 60_000, None);
        assert_eq!(c.observe(1_000, None, false), valid(60_000));
        assert_eq!(c.observe(31_000, None, false), valid(30_000));
        assert_eq!(c.observe(60_999, None, false), valid(1));
        assert!(!c.is_expired());
        assert_eq!(c.observe(61_000, None, false), LeaseStatus::Expired);
        assert!(c.is_expired());
    }

    #[test]
    fn a_greater_key_restarts_the_countdown_from_when_it_was_seen() {
        let mut c = LeaseClock::start(0, 10_000, Some(key(1, 1)));
        assert_eq!(c.observe(6_000, Some(key(1, 2)), false), valid(10_000));
        assert_eq!(c.observe(15_999, Some(key(1, 2)), false), valid(1));
        assert_eq!(
            c.observe(16_000, Some(key(1, 2)), false),
            LeaseStatus::Expired
        );
    }

    #[test]
    fn renewal_is_idempotent_a_replay_never_extends_again() {
        let mut c = LeaseClock::start(0, 10_000, None);
        assert_eq!(c.observe(5_000, Some(key(1, 1)), false), valid(10_000));
        // The same key, seen again later, does not extend.
        assert_eq!(c.observe(9_000, Some(key(1, 1)), false), valid(6_000));
        // An older key does not either.
        assert_eq!(c.observe(9_500, Some(key(1, 0)), false), valid(5_500));
        assert_eq!(c.observe(9_500, Some(key(0, 99)), false), valid(5_500));
    }

    #[test]
    fn the_key_already_applied_at_start_is_not_a_renewal() {
        let mut c = LeaseClock::start(0, 10_000, Some(key(1, 4)));
        assert_eq!(c.observe(8_000, Some(key(1, 4)), false), valid(2_000));
    }

    #[test]
    fn a_newer_epoch_wins_over_a_higher_sequence() {
        let mut c = LeaseClock::start(0, 10_000, Some(key(1, 50)));
        assert_eq!(c.observe(4_000, Some(key(2, 1)), false), valid(10_000));
    }

    #[test]
    fn a_late_renewal_does_not_revive_an_expired_lease() {
        let mut c = LeaseClock::start(0, 10_000, None);
        assert_eq!(
            c.observe(10_500, Some(key(1, 1)), false),
            LeaseStatus::Expired
        );
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
        let mut c = LeaseClock::start(0, 10_000, None);
        assert_eq!(c.observe(1, Some(key(1, 1)), true), LeaseStatus::Expired);
        assert!(c.is_expired());
        assert_eq!(c.observe(2, None, false), LeaseStatus::Expired);
    }

    #[test]
    fn a_long_suspend_expires_the_lease() {
        // The clock counts suspend: a jump of an hour is an hour.
        let mut c = LeaseClock::start(1_000, 60_000, None);
        assert_eq!(
            c.observe(1_000 + 3_600_000, None, false),
            LeaseStatus::Expired
        );
    }

    #[test]
    fn extreme_values_do_not_overflow() {
        let mut c = LeaseClock::start(u64::MAX - 5, u64::MAX, None);
        assert_eq!(c.observe(u64::MAX - 5, Some(key(1, 1)), false), valid(5));
        assert_eq!(c.observe(u64::MAX, None, false), LeaseStatus::Expired);
    }
}
