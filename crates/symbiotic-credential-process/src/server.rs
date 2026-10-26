//! Owner-only Unix-socket transport. Request handling never prints a request/body.
use crate::CredentialProcess;
use std::{os::unix::fs::PermissionsExt, sync::Arc, time::Duration};
use symbiotic_egress::{
    EgressError, PROTOCOL_VERSION, Request, Response,
    socket::{read_frame, write_frame},
};
use tokio::{net::UnixListener, sync::Semaphore};

/// Bind under the state lock and an exclusive `<socket_path>.bind.lock` flock.
/// The socket lock serializes probe, stale unlink and bind across state owners.
/// An active listener or non-socket path is always refused.
pub fn bind(process: &CredentialProcess) -> Result<UnixListener, EgressError> {
    bind_path(&process.config().socket_path)
}

pub(crate) fn bind_path(path: &std::path::Path) -> Result<UnixListener, EgressError> {
    let parent = path.parent().ok_or(EgressError::InvalidRequest)?;
    symbiotic_ai_runtime::model::private_fs::check_private_dir(parent)
        .map_err(|_| EgressError::StateUnavailable)?;
    let mut lock_path = path.as_os_str().to_owned();
    lock_path.push(".bind.lock");
    // Open a separate descriptor on every bind, even for cloned state owners.
    // Keep the lock file: unlinking it would let binders lock different inodes.
    let _socket_lock = crate::lock_file(std::path::Path::new(&lock_path), true)?;
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => {
            use std::os::unix::fs::FileTypeExt;
            if !metadata.file_type().is_socket() {
                return Err(EgressError::StateUnavailable);
            }
            // Do not block on a full listener backlog. Only ECONNREFUSED proves
            // there is no listener at this existing socket; other failures refuse.
            let socket = socket2::Socket::new(socket2::Domain::UNIX, socket2::Type::STREAM, None)
                .map_err(|_| EgressError::StateUnavailable)?;
            socket
                .set_nonblocking(true)
                .map_err(|_| EgressError::StateUnavailable)?;
            let address =
                socket2::SockAddr::unix(path).map_err(|_| EgressError::StateUnavailable)?;
            match socket.connect(&address) {
                Err(error) if error.raw_os_error() == Some(libc::ECONNREFUSED) => {}
                _ => return Err(EgressError::StateUnavailable),
            }
            std::fs::remove_file(path).map_err(|_| EgressError::StateUnavailable)?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => return Err(EgressError::StateUnavailable),
    }
    let listener = UnixListener::bind(path).map_err(|_| EgressError::StateUnavailable)?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
        .map_err(|_| EgressError::StateUnavailable)?;
    Ok(listener)
}

/// Serve one framed exchange per same-user connection. Dropping the client does not
/// cancel a dispatched provider call; the process owns completion and accounting.
pub async fn serve(process: CredentialProcess, listener: UnixListener) -> Result<(), EgressError> {
    let slots = Arc::new(Semaphore::new(process.config().max_connections));
    let serve = async {
        loop {
            let slot = slots
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| EgressError::StateUnavailable)?;
            let (mut stream, _) = listener
                .accept()
                .await
                .map_err(|_| EgressError::Transport)?;
            // A disconnected peer can make credential lookup fail (notably on
            // macOS). Refuse that connection without terminating recovery for all
            // other clients. No request is read before successful authentication.
            let Ok(credentials) = stream.peer_cred() else {
                continue;
            };
            // SAFETY: geteuid has no preconditions and does not access memory.
            if credentials.uid() != unsafe { libc::geteuid() } {
                continue;
            }
            let process = process.clone();
            tokio::spawn(async move {
                let _slot = slot;
                let limit = process.config().max_frame_bytes;
                let timeout = Duration::from_secs(process.config().io_timeout_seconds);
                let response =
                    match tokio::time::timeout(timeout, read_frame::<Request>(&mut stream, limit))
                        .await
                    {
                        Ok(Ok(request)) => process.handle(request).await,
                        Ok(Err(error)) => Response {
                            version: PROTOCOL_VERSION,
                            result: Err(error),
                        },
                        Err(_) => Response {
                            version: PROTOCOL_VERSION,
                            result: Err(EgressError::Transport),
                        },
                    };
                let _ =
                    tokio::time::timeout(timeout, write_response(&mut stream, &response, limit))
                        .await;
            });
        }
    };
    let cleanup = async {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            interval.tick().await;
            if let Err(error) = process.purge_expired_results() {
                break Err(error);
            }
        }
    };
    tokio::select! {
        result = serve => result,
        result = cleanup => result,
    }
}

async fn write_response(
    stream: &mut (impl tokio::io::AsyncWrite + Unpin),
    response: &Response,
    limit: u32,
) -> Result<(), EgressError> {
    match write_frame(stream, response, limit).await {
        // Serialization finishes before write_frame sends its length prefix,
        // so this refusal is still the connection's first and only frame.
        Err(EgressError::LimitExceeded) => {
            write_frame(
                stream,
                &Response {
                    version: PROTOCOL_VERSION,
                    result: Err(EgressError::LimitExceeded),
                },
                limit,
            )
            .await
        }
        result => result,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn oversized_response_returns_a_typed_refusal_before_writing_any_bytes() {
        let (mut writer, mut reader) = tokio::io::duplex(4096);
        let response = Response {
            version: PROTOCOL_VERSION,
            result: Ok(symbiotic_egress::Reply::Dispatched(
                symbiotic_egress::DispatchResult {
                    error: None,
                    diagnostics: Vec::new(),
                    receipt_persisted: true,
                    receipt: symbiotic_egress::DispatchReceipt {
                        attempt_digest: "a".repeat(64),
                        attempt_id: symbiotic_egress::AttemptId {
                            tenant: "tenant".into(),
                            incarnation: "incarnation".into(),
                            invocation_id: "i".repeat(4096),
                            job_queue: None,
                            attempt_ordinal: 1,
                        },
                        reference: symbiotic_egress::SpendReceiptRef::new("reference").unwrap(),
                        status: symbiotic_egress::DispatchStatus::Succeeded,
                        usage: symbiotic_trace::UsageTrace::default(),
                        spend_state: symbiotic_egress::SpendState::Settled,
                    },
                    output: Some(symbiotic_egress::ProviderOutput::Chat {
                        text: "paid answer".repeat(1024),
                        finish_reason: None,
                    }),
                },
            )),
        };
        let sent = write_response(&mut writer, &response, 1024).await;
        drop(writer);
        assert!(sent.is_ok());
        let received: Response = read_frame(&mut reader, 1024).await.unwrap();
        assert!(matches!(received.result, Err(EgressError::LimitExceeded)));
    }
}
