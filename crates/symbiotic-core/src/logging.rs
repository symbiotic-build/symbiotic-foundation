//! Shared JSON diagnostics for hosts that opt into the `logging` feature.
use tracing::Subscriber;
use tracing_subscriber::{EnvFilter, fmt::MakeWriter};

/// Local correlation header; validated by the receiving host before logging.
pub const REQUEST_ID_HEADER: &str = "x-symbiotic-request-id";
/// Environment override for tracing-subscriber EnvFilter directives.
pub const LOG_ENV: &str = "SYMBIOTIC_LOG";

/// Failure to configure or install the shared subscriber.
#[derive(Debug, thiserror::Error)]
pub enum LoggingError {
    /// The directives are not valid EnvFilter syntax.
    #[error("invalid logging directives: {0}")]
    Directives(#[from] tracing_subscriber::filter::ParseError),
    /// The environment override is not Unicode.
    #[error("logging environment override is not Unicode")]
    Environment,
    /// Another global subscriber is already installed.
    #[error("logging subscriber already installed")]
    Installed(#[from] tracing::subscriber::SetGlobalDefaultError),
}

fn directives<'a>(environment: Option<&'a str>, configured: Option<&'a str>) -> &'a str {
    environment
        .filter(|value| !value.is_empty())
        .or(configured)
        .unwrap_or("info")
}

/// Install JSON lines on stderr, using environment, configuration, then `info`.
pub fn init(configured: Option<&str>) -> Result<(), LoggingError> {
    let environment = match std::env::var(LOG_ENV) {
        Ok(value) => Some(value),
        Err(std::env::VarError::NotPresent) => None,
        Err(std::env::VarError::NotUnicode(_)) => return Err(LoggingError::Environment),
    };
    let subscriber = subscriber(
        directives(environment.as_deref(), configured),
        std::io::stderr,
    )?;
    tracing::subscriber::set_global_default(subscriber)?;
    Ok(())
}

/// Build the production subscriber with a caller-supplied output destination.
pub fn subscriber<W>(
    directives: &str,
    writer: W,
) -> Result<impl Subscriber + Send + Sync, LoggingError>
where
    W: for<'w> MakeWriter<'w> + Send + Sync + 'static,
{
    Ok(tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_new(directives)?)
        .json()
        .with_writer(writer)
        .with_ansi(false)
        // The default IO-error fallback uses eprintln!, which panics on closed stderr.
        .log_internal_errors(false)
        .flatten_event(true)
        .with_current_span(false)
        .with_span_list(true)
        .with_target(true)
        .finish())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn environment_directives_override_configuration_and_invalid_directives_fail() {
        for (environment, configured, expected) in [
            (Some("debug"), Some("warn"), "debug"),
            (Some(""), Some("warn"), "warn"),
            (None, Some("warn"), "warn"),
            (None, None, "info"),
            (Some(""), None, "info"),
        ] {
            assert_eq!(directives(environment, configured), expected);
            assert!(subscriber(directives(environment, configured), std::io::sink).is_ok());
        }
        assert!(matches!(
            subscriber(directives(Some("invalid["), Some("info")), std::io::sink),
            Err(LoggingError::Directives(_))
        ));
    }
}
