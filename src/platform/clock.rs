//! Clocks.
//!
//! Leases, timeouts and queue ages are measured on [`monotonic_ms`], which
//! keeps counting while the machine sleeps (spec chapter 12). The wall clock
//! is for display and for stamping database rows only; nothing decides on it.

/// Milliseconds on a monotonic clock that includes time spent suspended.
///
/// Linux: `CLOCK_BOOTTIME`. macOS: `CLOCK_MONOTONIC`, which is
/// `mach_continuous_time` and keeps running through sleep (unlike
/// `CLOCK_UPTIME_RAW`). The origin is arbitrary and differs between boots, so
/// values are only comparable within one boot.
pub fn monotonic_ms() -> u64 {
    #[cfg(target_os = "linux")]
    const CLOCK: libc::clockid_t = libc::CLOCK_BOOTTIME;
    #[cfg(not(target_os = "linux"))]
    const CLOCK: libc::clockid_t = libc::CLOCK_MONOTONIC;
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `ts` is a valid out pointer for the duration of the call.
    let rc = unsafe { libc::clock_gettime(CLOCK, &mut ts) };
    if rc != 0 {
        // The clock ids above exist on every supported OS; a failure is a
        // broken system. Report the largest value, which makes every deadline
        // look reached rather than never reached (fail closed).
        return u64::MAX;
    }
    (ts.tv_sec as u64)
        .saturating_mul(1000)
        .saturating_add(ts.tv_nsec as u64 / 1_000_000)
}

/// Unix time in milliseconds, for diagnostics and database stamps.
pub fn wall_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn monotonic_never_goes_backwards() {
        let mut last = monotonic_ms();
        for _ in 0..1000 {
            let now = monotonic_ms();
            assert!(now >= last);
            last = now;
        }
    }

    #[test]
    fn monotonic_advances_with_real_time() {
        let a = monotonic_ms();
        std::thread::sleep(std::time::Duration::from_millis(30));
        let b = monotonic_ms();
        assert!(b - a >= 25, "{a} -> {b}");
    }

    #[test]
    fn wall_clock_is_after_2020() {
        assert!(wall_ms() > 1_577_836_800_000);
    }
}
