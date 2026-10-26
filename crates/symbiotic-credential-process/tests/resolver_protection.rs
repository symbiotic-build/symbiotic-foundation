//! A fresh test executable keeps process-wide protection independent of other tests.
#![cfg(unix)]
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use symbiotic_ai_runtime::model::SecretValue;
use symbiotic_credential_process::{
    protect_process,
    secrets::{SecretSource, initialize_resolver_panic_hook},
};
use symbiotic_egress::EgressError;

fn core_limits() -> (libc::rlim_t, libc::rlim_t) {
    let mut limits = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: getrlimit writes this initialized struct and retains no pointer.
    assert_eq!(
        unsafe { libc::getrlimit(libc::RLIMIT_CORE, &mut limits) },
        0
    );
    (limits.rlim_cur, limits.rlim_max)
}
#[test]
fn resolver_protection_child() {
    let Some(mode) = std::env::var_os("RESOLVER_PROTECTION_TEST") else {
        return;
    };
    assert_ne!(
        core_limits(),
        (0, 0),
        "fixture must start before irreversible process protection"
    );
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let source = SecretSource::Resolver {
        name: "provider-key".into(),
        resolve: Arc::new(move |name| {
            count.fetch_add(1, Ordering::SeqCst);
            assert_eq!(name, "provider-key");
            assert_eq!(core_limits(), (0, 0));
            #[cfg(target_os = "linux")]
            // SAFETY: PR_GET_DUMPABLE accepts only integer arguments.
            assert_eq!(unsafe { libc::prctl(libc::PR_GET_DUMPABLE, 0, 0, 0, 0) }, 0);
            Ok(SecretValue::new(b"synthetic-key".to_vec()))
        }),
    };
    if mode == "hook_first" {
        initialize_resolver_panic_hook(std::panic::take_hook());
    }
    assert!(matches!(
        source.load(100),
        Err(EgressError::CredentialUnavailable)
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    protect_process().unwrap();
    if mode != "hook_first" {
        assert!(matches!(
            source.load(100),
            Err(EgressError::CredentialUnavailable)
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        initialize_resolver_panic_hook(std::panic::take_hook());
    }
    assert_eq!(&*source.load(100).unwrap(), b"synthetic-key");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(matches!(
        source.load(2),
        Err(EgressError::CredentialUnavailable)
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}
#[test]
fn resolver_refuses_calls_before_process_protection_or_hook_initialization() {
    for mode in ["hook_first", "protection_first"] {
        assert!(
            std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "resolver_protection_child", "--nocapture"])
                .env("RESOLVER_PROTECTION_TEST", mode)
                .status()
                .unwrap()
                .success()
        );
    }
}
