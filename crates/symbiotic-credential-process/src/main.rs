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
    let mut args = std::env::args_os().skip(1);
    let first = args.next().ok_or(EgressError::InvalidRequest)?;
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
        |_| "invalid configuration: supported secret backends are `none` and `owner_only_file`",
    )?;
    symbiotic_core::logging::init(config.log.as_deref())?;
    let process = CredentialProcess::open(config)?;
    let listener = server::bind(&process)?;
    Ok(server::serve(process, listener).await?)
}

#[cfg(not(unix))]
fn main() {
    eprintln!("credential process requires Unix sockets and owner-only filesystem protection");
    std::process::exit(1);
}
