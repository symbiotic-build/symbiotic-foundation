//! Owning-user Unix socket transport and hourly SQLite backups.
use crate::*;
use std::{
    os::unix::fs::FileTypeExt,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{UnixListener, UnixStream},
};

fn backup(state: &Path) -> Result<()> {
    let folder = state.join("backups");
    fs::DirBuilder::new()
        .mode(0o700)
        .create(&folder)
        .or_else(|e| {
            if e.kind() == std::io::ErrorKind::AlreadyExists {
                Ok(())
            } else {
                Err(e)
            }
        })?;
    let filename = format!("core-{}.sqlite", Utc::now().format("%Y%m%dT%H%M%SZ"));
    let path = folder.join(filename);
    let conn = Connection::open(state.join("core.sqlite"))?;
    conn.busy_timeout(Duration::from_secs(60))?;
    conn.execute(
        "VACUUM INTO ?1",
        [path.to_str().ok_or_else(|| invalid("backup path"))?],
    )?;
    let mut files = vec![];
    for entry in fs::read_dir(folder)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(stamp) = name
            .strip_prefix("core-")
            .and_then(|n| n.strip_suffix(".sqlite"))
            && stamp.len() == 16
            && chrono::NaiveDateTime::parse_from_str(stamp, "%Y%m%dT%H%M%SZ").is_ok()
            && entry.file_type()?.is_file()
        {
            files.push(entry.path());
        }
    }
    files.sort();
    for path in files.iter().take(files.len().saturating_sub(48)) {
        fs::remove_file(path)?;
    }
    Ok(())
}
async fn backup_logged(state: PathBuf) {
    match tokio::task::spawn_blocking(move || backup(&state)).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => eprintln!("core backup failed: {e}"),
        Err(e) => eprintln!("core backup task failed: {e}"),
    }
}
async fn connection(stream: UnixStream, store: Arc<Mutex<Store>>) -> Result<()> {
    let (read, mut write) = stream.into_split();
    let mut lines = BufReader::new(read).lines();
    while let Some(line) = lines.next_line().await? {
        let store = store.clone();
        let response = tokio::task::spawn_blocking(move || {
            let mut guard = store
                .lock()
                .map_err(|_| Error::new("store_error", "writer mutex is poisoned"))?;
            Ok::<_, Error>(dispatch(&mut guard, &line))
        })
        .await
        .map_err(|e| Error::new("store_error", e.to_string()))??;
        write.write_all(response.as_bytes()).await?;
        write.write_all(b"\n").await?;
    }
    Ok(())
}
/// Serve until the process stops, authenticating each peer before reading requests.
pub async fn serve(state: &Path) -> Result<()> {
    // Owner-only files from creation on: the socket is reachable as soon as it is bound,
    // and the store and its backups hold every record.
    // SAFETY: umask has no preconditions and does not access memory.
    unsafe { libc::umask(0o077) };
    let store = Store::open(state)?;
    let path = state.join("core.sock");
    if let Ok(metadata) = fs::symlink_metadata(&path) {
        if !metadata.file_type().is_socket() {
            return Err(Error::new("socket_in_use", "core.sock is not a socket"));
        }
        match UnixStream::connect(&path).await {
            Ok(_) => {
                return Err(Error::new(
                    "socket_in_use",
                    "a core process already answers on core.sock",
                ));
            }
            Err(e) if e.raw_os_error() == Some(libc::ECONNREFUSED) => fs::remove_file(&path)?,
            Err(e) => return Err(e.into()),
        }
    }
    let listener = UnixListener::bind(&path)?;
    backup_logged(state.to_owned()).await;
    let backup_state = state.to_owned();
    let backup_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(3600));
        interval.tick().await;
        loop {
            interval.tick().await;
            backup_logged(backup_state.clone()).await;
        }
    });
    let store = Arc::new(Mutex::new(store));
    let result: Result<()> = async {
        loop {
            let (stream, _) = listener.accept().await?;
            let credentials = match stream.peer_cred() {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("core peer authentication failed: {e}");
                    continue;
                }
            };
            // SAFETY: geteuid has no preconditions and does not access memory.
            if credentials.uid() != unsafe { libc::geteuid() } {
                eprintln!("core refused a peer with a different uid");
                continue;
            }
            let store = store.clone();
            tokio::spawn(async move {
                if let Err(e) = connection(stream, store).await {
                    eprintln!("core connection failed: {e}");
                }
            });
        }
    }
    .await;
    backup_task.abort();
    result
}
/// Send one call to a running core service and return its complete wire response.
pub async fn request(state: &Path, call: &str, args: Value) -> Result<Value> {
    let mut stream = UnixStream::connect(state.join("core.sock"))
        .await
        .map_err(|e| {
            Error::new(
                "socket_unreachable",
                format!("core socket is not reachable: {e}"),
            )
        })?;
    stream
        .write_all(format!("{}\n", json!({"call":call,"args":args})).as_bytes())
        .await?;
    let mut line = String::new();
    if BufReader::new(stream).read_line(&mut line).await? == 0 {
        return Err(Error::new(
            "socket_unreachable",
            "core socket closed without a response",
        ));
    }
    Ok(serde_json::from_str(&line)?)
}
