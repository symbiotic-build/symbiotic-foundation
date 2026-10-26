//! Launch with one protected JSON configuration path; diagnostics contain static codes only.
#[cfg(unix)]
#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

#[cfg(unix)]
async fn run() -> Result<(), Box<dyn std::error::Error>> {
    use symbiotic_credential_process::{
        CredentialProcess, ProcessConfig, secrets::SecretSource, server,
    };
    use symbiotic_egress::EgressError;
    symbiotic_credential_process::protect_process().map_err(|_| EgressError::StateUnavailable)?;
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_writer(std::io::stderr)
        .with_ansi(false)
        // The default IO-error fallback uses eprintln!, which panics on closed stderr.
        .log_internal_errors(false)
        .try_init()
        .map_err(|_| EgressError::StateUnavailable)?;
    let mut args = std::env::args_os().skip(1);
    let first = args.next().ok_or(EgressError::InvalidRequest)?;
    if first == "login" {
        return import_login(args).map_err(Into::into);
    }
    let path = if first == "--child" {
        symbiotic_supervise::watch_parent(|| Ok(())).map_err(|_| EgressError::StateUnavailable)?;
        args.next().ok_or(EgressError::InvalidRequest)?
    } else {
        first
    };
    if args.next().is_some() {
        return Err(EgressError::InvalidRequest.into());
    }
    let bytes = SecretSource::OwnerOnlyFile { path: path.into() }.load(1024 * 1024)?;
    let config: ProcessConfig = serde_json::from_slice(&bytes).map_err(
        |_| "invalid configuration: supported secret backends are `none`, `owner_only_file`, and `login_file`",
    )?;
    let process = CredentialProcess::open(config)?;
    let listener = server::bind(&process)?;
    if process.config().passthrough.is_some() {
        let passthrough = symbiotic_credential_process::passthrough::bind(&process)?;
        tokio::select! {
            result = server::serve(process.clone(), listener) => Ok(result?),
            result = symbiotic_credential_process::passthrough::serve(process, passthrough) => Ok(result?),
        }
    } else {
        Ok(server::serve(process, listener).await?)
    }
}

#[cfg(not(unix))]
fn main() {
    eprintln!("credential process requires Unix sockets and owner-only filesystem protection");
    std::process::exit(1);
}

#[cfg(unix)]
fn import_login(
    mut args: impl Iterator<Item = std::ffi::OsString>,
) -> Result<(), symbiotic_egress::EgressError> {
    use symbiotic_credential_process::{login::import_native, secrets::LoginFamily};
    use symbiotic_egress::EgressError::InvalidRequest;
    if args.next().as_deref() != Some(std::ffi::OsStr::new("import")) {
        return Err(InvalidRequest);
    }
    let (mut family, mut native, mut store, mut replace) = (None, None, None, false);
    while let Some(flag) = args.next() {
        match flag.to_str() {
            Some("--family") if family.is_none() => {
                family = Some(
                    match args.next().as_deref().and_then(|value| value.to_str()) {
                        Some("chatgpt_codex") => LoginFamily::ChatgptCodex,
                        Some("xai_grok") => LoginFamily::XaiGrok,
                        _ => return Err(InvalidRequest),
                    },
                );
            }
            Some("--native") if native.is_none() => {
                native = Some(std::path::PathBuf::from(args.next().ok_or(InvalidRequest)?))
            }
            Some("--store") if store.is_none() => {
                store = Some(std::path::PathBuf::from(args.next().ok_or(InvalidRequest)?))
            }
            Some("--replace") if !replace => replace = true,
            _ => return Err(InvalidRequest),
        }
    }
    import_native(
        family.ok_or(InvalidRequest)?,
        &native.ok_or(InvalidRequest)?,
        &store.ok_or(InvalidRequest)?,
        replace,
        1024 * 1024,
    )
}
