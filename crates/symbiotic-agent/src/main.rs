//! Command-line entry point for the bounded-step agent runtime.

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::process::ExitCode {
    symbiotic_agent::run().await
}
