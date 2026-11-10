//! The stop sequence for a command's process group: TERM, a grace period,
//! then KILL, until the group is confirmed gone (spec chapter 12).
//!
//! This is the pure part: given the time and whether the group still has
//! members, it says what to do next. The caller sends the signals and probes
//! the group.

/// What to do now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopStep {
    /// Send SIGTERM to the group.
    Term,
    /// The grace period is running; check again.
    Wait,
    /// Send SIGKILL to the group (also repeated while confirming).
    Kill,
    /// The group is gone. Only now may the stop be recorded.
    Confirmed,
    /// Still alive after KILL and the confirmation window: do not record a
    /// stop; the attempt stays unresolved.
    Unconfirmed,
}

#[derive(Debug, Clone)]
pub struct Escalation {
    grace_ms: u64,
    kill_wait_ms: u64,
    termed_at: Option<u64>,
    killed_at: Option<u64>,
}

impl Escalation {
    /// `grace_ms`: from TERM to KILL. `kill_wait_ms`: how long after KILL to
    /// keep checking before giving up.
    pub fn new(grace_ms: u64, kill_wait_ms: u64) -> Self {
        Escalation {
            grace_ms,
            kill_wait_ms,
            termed_at: None,
            killed_at: None,
        }
    }

    pub fn step(&mut self, now_ms: u64, group_alive: bool) -> StopStep {
        if !group_alive {
            return StopStep::Confirmed;
        }
        let Some(termed) = self.termed_at else {
            self.termed_at = Some(now_ms);
            return StopStep::Term;
        };
        if let Some(killed) = self.killed_at {
            return if now_ms >= killed.saturating_add(self.kill_wait_ms) {
                StopStep::Unconfirmed
            } else {
                StopStep::Kill
            };
        }
        if now_ms >= termed.saturating_add(self.grace_ms) {
            self.killed_at = Some(now_ms);
            StopStep::Kill
        } else {
            StopStep::Wait
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_group_already_gone_is_confirmed_without_any_signal() {
        let mut e = Escalation::new(10_000, 5_000);
        assert_eq!(e.step(0, false), StopStep::Confirmed);
    }

    #[test]
    fn term_then_wait_then_kill_after_the_grace() {
        let mut e = Escalation::new(10_000, 5_000);
        assert_eq!(e.step(100, true), StopStep::Term);
        assert_eq!(e.step(100, true), StopStep::Wait);
        assert_eq!(e.step(10_099, true), StopStep::Wait);
        assert_eq!(e.step(10_100, true), StopStep::Kill);
    }

    #[test]
    fn a_group_that_exits_in_the_grace_is_confirmed_without_kill() {
        let mut e = Escalation::new(10_000, 5_000);
        assert_eq!(e.step(0, true), StopStep::Term);
        assert_eq!(e.step(3_000, true), StopStep::Wait);
        assert_eq!(e.step(3_500, false), StopStep::Confirmed);
    }

    #[test]
    fn kill_is_repeated_until_confirmed_or_the_window_ends() {
        let mut e = Escalation::new(1_000, 2_000);
        assert_eq!(e.step(0, true), StopStep::Term);
        assert_eq!(e.step(1_000, true), StopStep::Kill);
        assert_eq!(e.step(1_500, true), StopStep::Kill);
        assert_eq!(e.step(2_999, true), StopStep::Kill);
        assert_eq!(e.step(3_000, true), StopStep::Unconfirmed);
        // Confirmation still wins if the group disappears at last.
        let mut e = Escalation::new(1_000, 2_000);
        e.step(0, true);
        e.step(1_000, true);
        assert_eq!(e.step(2_000, false), StopStep::Confirmed);
    }

    #[test]
    fn zero_grace_kills_on_the_next_look() {
        let mut e = Escalation::new(0, 100);
        assert_eq!(e.step(5, true), StopStep::Term);
        assert_eq!(e.step(5, true), StopStep::Kill);
        assert_eq!(e.step(105, true), StopStep::Unconfirmed);
    }

    #[test]
    fn extreme_times_do_not_overflow() {
        let mut e = Escalation::new(u64::MAX, u64::MAX);
        assert_eq!(e.step(u64::MAX - 1, true), StopStep::Term);
        assert_eq!(e.step(u64::MAX - 1, true), StopStep::Wait);
        assert_eq!(e.step(u64::MAX, true), StopStep::Kill);
    }
}
