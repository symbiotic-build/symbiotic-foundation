//! Same-user HTTP passthrough with runtime account admission and renewable logins.
//! Process protection, the state lock, the socket bind and the owner-only file reader are
//! temporary copies of the credential process's helpers, because neither binary may depend on
//! the other; issue #207 tracks their removal with the credential process's typed path.
#![cfg(unix)]

mod error;
pub use error::GatewayError;
mod process_security;
pub use process_security::protect_process;
pub mod login;
pub mod passthrough;
pub use passthrough::{PassthroughConfig, PassthroughMethod, PassthroughRoute};
pub mod secrets;
mod server;
pub mod stream;

use serde::{Deserialize, Serialize};
use std::{path::PathBuf, sync::Arc};
use symbiotic_ai_runtime::{Runtime, RuntimeConfig};

/// Deployment locations and request limits; no protocol version or credential values.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GatewayConfig {
    /// EnvFilter directives; `SYMBIOTIC_LOG` overrides this setting.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log: Option<String>,
    /// Owner-only runtime directory with the existing account admission database.
    pub state_dir: PathBuf,
    /// Finite upper bound on each secret.
    pub max_secret_bytes: usize,
    /// Maximum HTTP request header bytes.
    pub max_header_bytes: u32,
    /// Time allowed for reading/writing caller socket data.
    pub io_timeout_seconds: u64,
    /// Required same-user HTTP server and routes.
    pub passthrough: PassthroughConfig,
}
impl GatewayConfig {
    /// Validate route policies without opening state or credentials.
    pub fn validate_routes(&self) -> Result<(), GatewayError> {
        passthrough::validate(self)
    }
}

struct Inner {
    config: GatewayConfig,
    #[cfg(test)]
    runtime: Runtime,
    passthrough: passthrough::Passthrough,
    _process_lock: ProcessLock,
}

/// Cloneable gateway handle; the server owns active requests and shutdown.
#[derive(Clone)]
pub struct Gateway {
    inner: Arc<Inner>,
}
impl Gateway {
    /// Open protected state and configured credentials.
    /// Embedded callers must also call [`protect_process`] before reading configuration.
    pub fn open(config: GatewayConfig) -> Result<Self, GatewayError> {
        Self::open_with_clock(config, Arc::new(chrono::Utc::now))
    }

    /// Open with a trusted host clock for runtime admission and request deadlines.
    pub fn open_with_clock(
        config: GatewayConfig,
        clock: symbiotic_queue::jobs::JobClock,
    ) -> Result<Self, GatewayError> {
        protect_process().map_err(|_| GatewayError::StateUnavailable)?;
        // Keep the credential process's minimum for the field this one replaces.
        if config.max_header_bytes < 4096 + 4 * 6 + 4 {
            return Err(GatewayError::InvalidHeaderLimit);
        }
        if config.max_secret_bytes < 32 || config.io_timeout_seconds == 0 {
            return Err(GatewayError::InvalidRequest);
        }
        config.validate_routes()?;
        symbiotic_ai_runtime::model::private_fs::ensure_private_dir(&config.state_dir)
            .map_err(|_| GatewayError::StateUnavailable)?;
        let process_lock = lock_process(&config.state_dir)?;
        let runtime = Runtime::open(RuntimeConfig {
            clock: clock.clone(),
            registry: None,
            state_dir: Some(config.state_dir.clone()),
            ..RuntimeConfig::default()
        })
        .map_err(|error| match error {
            symbiotic_ai_runtime::model::ModelError::Queue(
                symbiotic_core::DiagnosticCode::UnsupportedQueueSchema,
            ) => GatewayError::UnsupportedQueueSchema,
            _ => GatewayError::StateUnavailable,
        })?;
        let passthrough = passthrough::Passthrough::open(&config, &runtime, clock)?;
        Ok(Self {
            inner: Arc::new(Inner {
                config,
                #[cfg(test)]
                runtime,
                passthrough,
                _process_lock: process_lock,
            }),
        })
    }

    /// Install an embedded-only retry decision hook. JSON cannot configure the hook.
    /// Call it on a freshly opened gateway, before cloning or serving it.
    pub fn with_retry_hook(mut self, hook: passthrough::RetryHook) -> Result<Self, GatewayError> {
        let inner = Arc::get_mut(&mut self.inner).ok_or(GatewayError::StateUnavailable)?;
        inner.passthrough.retry_hook = Some(hook);
        Ok(self)
    }

    /// Deployment settings without credential values.
    pub fn config(&self) -> &GatewayConfig {
        &self.inner.config
    }
}

struct ProcessLock(std::fs::File);

impl Drop for ProcessLock {
    fn drop(&mut self) {
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            // Closing alone leaves the flock held while a concurrently spawned
            // child retains an inherited descriptor, even with O_CLOEXEC.
            // SAFETY: this guard owns the live descriptor; flock retains no pointer.
            let _ = unsafe { libc::flock(self.0.as_raw_fd(), libc::LOCK_UN) };
        }
    }
}

fn lock_process(dir: &std::path::Path) -> Result<ProcessLock, GatewayError> {
    // Keep this name so a credential process and gateway cannot share one state directory.
    lock_file(&dir.join("credential-process.lock"), false)
}

fn lock_file(path: &std::path::Path, wait: bool) -> Result<ProcessLock, GatewayError> {
    symbiotic_ai_runtime::model::private_fs::ensure_private_file(path)
        .map_err(|_| GatewayError::StateUnavailable)?;
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .map_err(|_| GatewayError::StateUnavailable)?;
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;
        // SAFETY: the descriptor belongs to this live File; flock retains no pointer.
        let operation = libc::LOCK_EX | if wait { 0 } else { libc::LOCK_NB };
        if unsafe { libc::flock(file.as_raw_fd(), operation) } != 0 {
            return Err(GatewayError::StateUnavailable);
        }
    }
    #[cfg(not(unix))]
    return Err(GatewayError::StateUnavailable);
    Ok(ProcessLock(file))
}
