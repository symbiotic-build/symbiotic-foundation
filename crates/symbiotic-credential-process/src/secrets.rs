//! Local secret resolution. Secret values never implement Debug or Serialize.
use serde::{Deserialize, Serialize};
use std::{
    fmt,
    fs::OpenOptions,
    io::{Read, Write},
    panic::{AssertUnwindSafe, catch_unwind},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};
use symbiotic_ai_runtime::model::SecretValue;
use symbiotic_egress::EgressError;

/// App-owned failure; its text and source are discarded at the credential boundary.
pub type ResolverError = Box<dyn std::error::Error + Send + Sync>;

/// Resolve a named key into the same zeroizing bytes used by file sources.
/// Initialize [`initialize_resolver_panic_hook`] before loading a resolver source.
///
/// The app must return promptly with a value or error and support concurrent calls.
/// Admission-key resolution runs synchronously during startup (including reopening).
/// Provider resolution runs in an awaited blocking task during dispatch. Direct
/// dispatch resolves before request-budget admission and provider execution, so
/// provider `timeout_seconds` and `max_in_flight` do not cover that resolution.
/// Queued jobs acquire their account slot before resolving, but the provider
/// timeout still does not bound the callback. The app owns any lookup deadline
/// and concurrency control it needs; Foundation imposes no resolver deadline.
///
/// Slow resolution delays startup or dispatch. Dropping or cancelling an awaiting
/// future cannot stop a running callback. During shutdown, Tokio runtime drop waits
/// for outstanding blocking callbacks to return; a runtime shutdown timeout only
/// stops waiting, not the callbacks. After a crash, recovery preserves an accepted
/// unfinished direct dispatch as uncertain and does not rerun its provider lookup
/// or resend it. Opening the credential process still resolves its admission key.
pub type SecretResolver = dyn Fn(&str) -> Result<SecretValue<Vec<u8>>, ResolverError> + Send + Sync;

/// Fixed subscription protocol and token destination.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LoginFamily {
    /// ChatGPT subscription used by Codex.
    ChatgptCodex,
    /// First-party xAI subscription used by Grok.
    XaiGrok,
    /// Kimi Code subscription used by the Kimi CLI.
    KimiCode,
}

/// Configured backend; references carry locations or callbacks, never secret values.
#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "backend", rename_all = "snake_case", deny_unknown_fields)]
pub enum SecretSource {
    /// Keyless provider. Must never be used for an admission MAC key.
    None,
    /// Exact owner-only file, opened without following its final symlink.
    OwnerOnlyFile { path: PathBuf },
    /// Renewable subscription, available only to passthrough routes.
    LoginFile {
        /// Owner-only versioned store.
        path: PathBuf,
        /// Fixed provider login protocol.
        family: LoginFamily,
    },
    /// App callback, available only in thread mode and never serialized.
    #[serde(skip)]
    Resolver {
        /// App-defined key name, independent of the tenant-scoped admission reference.
        name: String,
        /// Called lazily inside the protected credential boundary.
        resolve: Arc<SecretResolver>,
    },
}

impl fmt::Debug for SecretSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => f.write_str("None"),
            Self::OwnerOnlyFile { .. } => f.write_str("OwnerOnlyFile { .. }"),
            Self::LoginFile { .. } => f.write_str("LoginFile { .. }"),
            Self::Resolver { .. } => f.write_str("Resolver { .. }"),
        }
    }
}

static PANIC_HOOK_INITIALIZED: AtomicBool = AtomicBool::new(false);
static ACTIVE_RESOLUTIONS: AtomicUsize = AtomicUsize::new(0);

/// Install credential redaction with the app's ordinary panic reporter beneath it.
///
/// Call before the first resolver lookup. Initialize or replace app reporting through
/// this function, including after a startup admission-key lookup; calling
/// `std::panic::set_hook` directly afterward replaces credential protection.
/// While any resolution is active, all threads report only a static diagnostic,
/// covering joined lookup workers. Ordinary reporting resumes after the last
/// resolution exits. As with `std::panic::set_hook`, do not call while panicking.
pub fn initialize_resolver_panic_hook(
    reporter: impl Fn(&std::panic::PanicHookInfo<'_>) + Send + Sync + 'static,
) {
    std::panic::set_hook(Box::new(move |info| {
        if ACTIVE_RESOLUTIONS.load(Ordering::SeqCst) != 0 {
            // A closed stderr must not turn the reporter into another panic.
            // The caller still receives CredentialUnavailable.
            let _ = writeln!(std::io::stderr().lock(), "credential resolver panicked");
        } else {
            reporter(info);
        }
    }));
    PANIC_HOOK_INITIALIZED.store(true, Ordering::Release);
}

struct ResolverScope;

impl ResolverScope {
    fn enter() -> Result<Self, EgressError> {
        if !PANIC_HOOK_INITIALIZED.load(Ordering::Acquire) {
            return Err(EgressError::CredentialUnavailable);
        }
        ACTIVE_RESOLUTIONS.fetch_add(1, Ordering::SeqCst);
        Ok(Self)
    }
}

impl Drop for ResolverScope {
    fn drop(&mut self) {
        // Nested and concurrent lookups remain protected until every scope exits.
        ACTIVE_RESOLUTIONS.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Owned credential; the shared model HTTP boundary rejects output echoes.
pub(crate) struct Secret {
    value: SecretValue<String>,
}

impl Secret {
    pub(crate) fn keyless() -> Self {
        Self {
            value: SecretValue::new(String::new()),
        }
    }
    pub(crate) fn value(&self) -> &str {
        &self.value
    }

    pub(crate) fn from_bytes(bytes: SecretValue<Vec<u8>>) -> Result<Self, EgressError> {
        let value = SecretValue::new(
            std::str::from_utf8(&bytes)
                .map_err(|_| EgressError::CredentialUnavailable)?
                .to_owned(),
        );
        if value.is_empty() {
            return Err(EgressError::CredentialUnavailable);
        }
        Ok(Self { value })
    }
}

impl SecretSource {
    /// Load bounded bytes only in a protected process; all failures are redacted.
    pub fn load(&self, max_bytes: usize) -> Result<SecretValue<Vec<u8>>, EgressError> {
        #[cfg(unix)]
        let protected = crate::process_security::require_protection().is_ok();
        #[cfg(not(unix))]
        let protected = false;
        if !protected || max_bytes == 0 {
            return Err(EgressError::CredentialUnavailable);
        }
        let bytes = match self {
            Self::None | Self::LoginFile { .. } => return Err(EgressError::CredentialUnavailable),
            Self::OwnerOnlyFile { path } => read_private_file(path, max_bytes)?,
            Self::Resolver { name, resolve } => {
                let _scope = ResolverScope::enter()?;
                catch_unwind(AssertUnwindSafe(|| {
                    resolve(name).map_err(|_| EgressError::CredentialUnavailable)
                }))
                .unwrap_or(Err(EgressError::CredentialUnavailable))?
            }
        };
        if bytes.is_empty() || bytes.len() > max_bytes {
            return Err(EgressError::CredentialUnavailable);
        }
        Ok(bytes)
    }
}

pub(crate) fn read_private_file(
    path: &std::path::Path,
    max_bytes: usize,
) -> Result<SecretValue<Vec<u8>>, EgressError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
        let mut file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(path)
            .map_err(|_| EgressError::CredentialUnavailable)?;
        let metadata = file
            .metadata()
            .map_err(|_| EgressError::CredentialUnavailable)?;
        // SAFETY: geteuid has no preconditions and does not access memory.
        if !metadata.is_file()
            || metadata.uid() != unsafe { libc::geteuid() }
            || metadata.mode() & 0o077 != 0
            || metadata.len() > max_bytes as u64
        {
            return Err(EgressError::CredentialUnavailable);
        }
        let mut bytes = SecretValue::new(Vec::new());
        (&mut file)
            .take(max_bytes as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| EgressError::CredentialUnavailable)?;
        if bytes.len() > max_bytes {
            return Err(EgressError::CredentialUnavailable);
        }
        Ok(bytes)
    }
    #[cfg(not(unix))]
    {
        let _ = (path, max_bytes);
        Err(EgressError::CredentialUnavailable)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn configuration_types_do_not_implement_debug() {
        // Inference becomes ambiguous if a configuration gains Debug, refusing
        // diagnostic formatting even before endpoint validation can run.
        trait AmbiguousIfDebug<A> {
            fn check() {}
        }
        impl<T> AmbiguousIfDebug<()> for T {}
        impl<T: fmt::Debug> AmbiguousIfDebug<u8> for T {}
        let _ = <crate::ProcessConfig as AmbiguousIfDebug<_>>::check;
        let _ = <crate::RouteConfig as AmbiguousIfDebug<_>>::check;
        let _ = <crate::RouteProvider as AmbiguousIfDebug<_>>::check;
    }

    #[cfg(unix)]
    #[test]
    fn file_backend_refuses_public_files_symlinks_and_oversize() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("key");
        std::fs::write(&path, "synthetic-secret").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(read_private_file(&path, 100).is_err());
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert_eq!(
            &*read_private_file(&path, 100).unwrap(),
            b"synthetic-secret"
        );
        assert!(read_private_file(&path, 2).is_err());
        let link = dir.path().join("link");
        std::os::unix::fs::symlink(&path, &link).unwrap();
        assert!(read_private_file(&link, 100).is_err());
    }
}
