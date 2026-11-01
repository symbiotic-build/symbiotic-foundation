//! Owner-only static credentials and renewable login locations.
use crate::GatewayError;
use serde::{Deserialize, Serialize};
use std::{fmt, fs::OpenOptions, io::Read, path::PathBuf};
use symbiotic_ai_runtime::model::SecretValue;

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

/// Configured credential location; never a credential value.
#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "backend", rename_all = "snake_case", deny_unknown_fields)]
pub enum SecretSource {
    /// Exact owner-only file, opened without following its final symlink.
    OwnerOnlyFile { path: PathBuf },
    /// Renewable subscription store.
    LoginFile {
        /// Owner-only versioned store.
        path: PathBuf,
        /// Fixed provider login protocol.
        family: LoginFamily,
    },
}
impl fmt::Debug for SecretSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OwnerOnlyFile { .. } => f.write_str("OwnerOnlyFile { .. }"),
            Self::LoginFile { .. } => f.write_str("LoginFile { .. }"),
        }
    }
}
impl SecretSource {
    /// Load bounded bytes only in a protected process; all failures are redacted.
    pub fn load(&self, max_bytes: usize) -> Result<SecretValue<Vec<u8>>, GatewayError> {
        if crate::process_security::require_protection().is_err() || max_bytes == 0 {
            return Err(GatewayError::CredentialUnavailable);
        }
        let bytes = match self {
            Self::OwnerOnlyFile { path } => read_private_file(path, max_bytes)?,
            Self::LoginFile { .. } => return Err(GatewayError::CredentialUnavailable),
        };
        if bytes.is_empty() || bytes.len() > max_bytes {
            return Err(GatewayError::CredentialUnavailable);
        }
        Ok(bytes)
    }
}

pub(crate) fn read_private_file(
    path: &std::path::Path,
    max_bytes: usize,
) -> Result<SecretValue<Vec<u8>>, GatewayError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
        let mut file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(path)
            .map_err(|_| GatewayError::CredentialUnavailable)?;
        let metadata = file
            .metadata()
            .map_err(|_| GatewayError::CredentialUnavailable)?;
        // SAFETY: geteuid has no preconditions and does not access memory.
        if !metadata.is_file()
            || metadata.uid() != unsafe { libc::geteuid() }
            || metadata.mode() & 0o077 != 0
            || metadata.len() > max_bytes as u64
        {
            return Err(GatewayError::CredentialUnavailable);
        }
        let mut bytes = SecretValue::new(Vec::new());
        (&mut file)
            .take(max_bytes as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| GatewayError::CredentialUnavailable)?;
        if bytes.len() > max_bytes {
            return Err(GatewayError::CredentialUnavailable);
        }
        Ok(bytes)
    }
    #[cfg(not(unix))]
    {
        let _ = (path, max_bytes);
        Err(GatewayError::CredentialUnavailable)
    }
}
