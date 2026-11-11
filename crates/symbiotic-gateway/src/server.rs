//! Owner-only socket binding copied from the credential process.
use crate::GatewayError;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use tokio::net::UnixListener;

pub(crate) fn bind_path(path: &std::path::Path) -> Result<UnixListener, GatewayError> {
    bind_socket(path, false)
}

pub(crate) fn bind_forwarder_path(path: &std::path::Path) -> Result<UnixListener, GatewayError> {
    bind_socket(path, true)
}

fn bind_socket(path: &std::path::Path, forwarder: bool) -> Result<UnixListener, GatewayError> {
    let parent = path.parent().ok_or(GatewayError::InvalidRequest)?;
    let group = if forwarder {
        let meta = std::fs::symlink_metadata(parent).map_err(|_| GatewayError::StateUnavailable)?;
        // SAFETY: geteuid takes no arguments and retains no memory.
        if !meta.is_dir()
            || meta.uid() != unsafe { libc::geteuid() }
            || !matches!(meta.mode() & 0o7777, 0o750 | 0o2750)
        {
            return Err(GatewayError::StateUnavailable);
        }
        Some(meta.gid())
    } else {
        symbiotic_ai_runtime::model::private_fs::check_private_dir(parent)
            .map_err(|_| GatewayError::StateUnavailable)?;
        None
    };
    let mut lock_path = path.as_os_str().to_owned();
    lock_path.push(".bind.lock");
    // Open a separate descriptor on every bind, even for cloned state owners.
    // Keep the lock file: unlinking it would let binders lock different inodes.
    let _socket_lock = crate::lock_file(std::path::Path::new(&lock_path), true)?;
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => {
            use std::os::unix::fs::FileTypeExt;
            if !metadata.file_type().is_socket() {
                return Err(GatewayError::StateUnavailable);
            }
            // Do not block on a full listener backlog. Only ECONNREFUSED proves
            // there is no listener at this existing socket; other failures refuse.
            let socket = socket2::Socket::new(socket2::Domain::UNIX, socket2::Type::STREAM, None)
                .map_err(|_| GatewayError::StateUnavailable)?;
            socket
                .set_nonblocking(true)
                .map_err(|_| GatewayError::StateUnavailable)?;
            let address =
                socket2::SockAddr::unix(path).map_err(|_| GatewayError::StateUnavailable)?;
            match socket.connect(&address) {
                Err(error) if error.raw_os_error() == Some(libc::ECONNREFUSED) => {}
                _ => return Err(GatewayError::StateUnavailable),
            }
            std::fs::remove_file(path).map_err(|_| GatewayError::StateUnavailable)?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => return Err(GatewayError::StateUnavailable),
    }
    let listener = UnixListener::bind(path).map_err(|_| GatewayError::StateUnavailable)?;
    std::fs::set_permissions(
        path,
        std::fs::Permissions::from_mode(if forwarder { 0o660 } else { 0o600 }),
    )
    .map_err(|_| GatewayError::StateUnavailable)?;
    if let Some(group) = group
        && std::fs::symlink_metadata(path)
            .map_err(|_| GatewayError::StateUnavailable)?
            .gid()
            != group
    {
        return Err(GatewayError::StateUnavailable);
    }
    Ok(listener)
}
