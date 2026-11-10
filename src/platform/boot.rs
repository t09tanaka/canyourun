//! The boot ID: changes when the OS restarts (spec chapter 12, 15).

/// This boot's identifier, or `None` when the OS does not say. Callers treat
/// `None` as "cannot tell", never as "same boot".
///
/// Linux: `/proc/sys/kernel/random/boot_id`. macOS: `kern.bootsessionuuid`
/// (unlike `kern.boottime`, it does not move when the wall clock is set).
/// The format is the same one task-traffic-controller records, so the two
/// agree on what "the same boot" means.
pub fn boot_id() -> Option<String> {
    #[cfg(target_os = "linux")]
    {
        let s = std::fs::read_to_string("/proc/sys/kernel/random/boot_id").ok()?;
        non_empty(s.trim())
    }
    #[cfg(target_os = "macos")]
    {
        let name = std::ffi::CString::new("kern.bootsessionuuid").ok()?;
        let mut buf = [0u8; 128];
        let mut len: libc::size_t = buf.len();
        // SAFETY: `buf` and `len` are valid for the call; `name` is NUL-terminated.
        let rc = unsafe {
            libc::sysctlbyname(
                name.as_ptr(),
                buf.as_mut_ptr().cast(),
                &mut len,
                std::ptr::null_mut(),
                0,
            )
        };
        if rc != 0 || len == 0 {
            return None;
        }
        let s = std::str::from_utf8(&buf[..len]).ok()?;
        non_empty(s.trim_end_matches('\0').trim())
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        None
    }
}

#[allow(dead_code)]
fn non_empty(s: &str) -> Option<String> {
    (!s.is_empty()).then(|| s.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn boot_id_is_available_and_stable() {
        let a = boot_id().expect("macOS and Linux report a boot id");
        assert_eq!(boot_id().unwrap(), a);
        assert!(crate::ids::WorkerBootId::parse(&a).is_ok(), "{a}");
    }

    #[test]
    fn non_empty_rejects_blank() {
        assert_eq!(non_empty(""), None);
        assert_eq!(non_empty("x").as_deref(), Some("x"));
    }
}
