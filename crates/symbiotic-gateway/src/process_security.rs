//! Process-wide crash protection, established before any secret reads.
/// Disable core dumps and Linux dumpability before reading configuration or secrets.
/// Any OS refusal must abort startup; this protection applies to the whole process.
pub fn protect_process() -> std::io::Result<()> {
    // Linux piped core collectors ignore RLIMIT_CORE. Disable dumpability as well,
    // before reading configuration or secrets; failure refuses startup.
    #[cfg(target_os = "linux")]
    // SAFETY: PR_SET_DUMPABLE accepts an integer and retains no pointers.
    if unsafe {
        libc::prctl(
            libc::PR_SET_DUMPABLE,
            0 as libc::c_ulong,
            0 as libc::c_ulong,
            0 as libc::c_ulong,
            0 as libc::c_ulong,
        )
    } != 0
    {
        return Err(std::io::Error::last_os_error());
    }
    // Retain the portable resource-limit protection on every supported Unix host.
    let limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: setrlimit reads the initialized rlimit and retains no pointer.
    if unsafe { libc::setrlimit(libc::RLIMIT_CORE, &limit) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

/// Check the OS state rather than retaining a second protection flag.
pub(crate) fn require_protection() -> std::io::Result<()> {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: getrlimit writes the initialized struct and retains no pointer.
    if unsafe { libc::getrlimit(libc::RLIMIT_CORE, &mut limit) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    let protected = limit.rlim_cur == 0 && limit.rlim_max == 0;
    #[cfg(target_os = "linux")]
    {
        // SAFETY: PR_GET_DUMPABLE accepts only integer arguments.
        if unsafe { libc::prctl(libc::PR_GET_DUMPABLE, 0, 0, 0, 0) } != 0 {
            return Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied));
        }
    }
    if protected {
        Ok(())
    } else {
        Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied))
    }
}

#[cfg(test)]
mod tests {
    use std::process::Command;

    #[test]
    fn guard_child() {
        let Ok(_) = std::env::var("PROTECTION_TEST_MODE") else {
            return;
        };
        let result = super::protect_process();
        result.unwrap();
        let mut limit = libc::rlimit {
            rlim_cur: 1,
            rlim_max: 1,
        };
        // SAFETY: getrlimit writes the initialized struct and retains no pointer.
        assert_eq!(unsafe { libc::getrlimit(libc::RLIMIT_CORE, &mut limit) }, 0);
        assert_eq!((limit.rlim_cur, limit.rlim_max), (0, 0));
        #[cfg(target_os = "linux")]
        // SAFETY: PR_GET_DUMPABLE accepts only integer arguments.
        assert_eq!(
            unsafe {
                libc::prctl(
                    libc::PR_GET_DUMPABLE,
                    0 as libc::c_ulong,
                    0 as libc::c_ulong,
                    0 as libc::c_ulong,
                    0 as libc::c_ulong,
                )
            },
            0
        );
    }

    fn child() -> Command {
        let mut child = Command::new(std::env::current_exe().unwrap());
        child.args([
            "--exact",
            "process_security::tests::guard_child",
            "--nocapture",
        ]);
        child
    }

    #[test]
    fn protection_is_applied_in_a_real_process() {
        assert!(
            child()
                .env("PROTECTION_TEST_MODE", "protected")
                .status()
                .unwrap()
                .success()
        );
    }
}
