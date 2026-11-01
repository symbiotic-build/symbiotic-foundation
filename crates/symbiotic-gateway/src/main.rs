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
    use symbiotic_gateway::GatewayError;
    use symbiotic_gateway::{Gateway, GatewayConfig, passthrough, secrets::SecretSource};
    symbiotic_gateway::protect_process().map_err(|_| GatewayError::StateUnavailable)?;
    let mut args = std::env::args_os().skip(1);
    let first = args.next().ok_or(GatewayError::InvalidRequest)?;
    if first == "login" {
        symbiotic_core::logging::init(None)?;
        return import_login(args).map_err(Into::into);
    }
    let path = if first == "--child" {
        symbiotic_supervise::watch_parent(|| Ok(())).map_err(|_| GatewayError::StateUnavailable)?;
        args.next().ok_or(GatewayError::InvalidRequest)?
    } else {
        first
    };
    if args.next().is_some() {
        return Err(GatewayError::InvalidRequest.into());
    }
    let bytes = SecretSource::OwnerOnlyFile { path: path.into() }.load(1024 * 1024)?;
    let config: GatewayConfig = serde_json::from_slice(&bytes).map_err(
        |_| "invalid configuration: supported secret backends are `owner_only_file` and `login_file`",
    )?;
    symbiotic_core::logging::init(config.log.as_deref())?;
    let process = Gateway::open(config)?;
    let listener = passthrough::bind(&process)?;
    Ok(passthrough::serve(process, listener).await?)
}

#[cfg(not(unix))]
fn main() {
    eprintln!("gateway requires Unix sockets and owner-only filesystem protection");
    std::process::exit(1);
}

#[cfg(unix)]
fn import_login(
    mut args: impl Iterator<Item = std::ffi::OsString>,
) -> Result<(), symbiotic_gateway::GatewayError> {
    use symbiotic_gateway::GatewayError::InvalidRequest;
    use symbiotic_gateway::{login::import_native, secrets::LoginFamily};
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
                        Some("kimi_code") => LoginFamily::KimiCode,
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
