use serde_json::{Value, json};
use std::{io::Read, path::PathBuf};
use symbiotic_core_service::server;
const USAGE: &str = "Usage: symbiotic-core [--state DIR] serve\n       symbiotic-core [--state DIR] <job.create|job.transition|request.record|budget.get|decision.answer|events.since> <JSON|->\n       symbiotic-core [--state DIR] reconcile --day YYYY-MM-DD --warden-state DIR\nState defaults to SYMBIOTIC_CORE_STATE.\n";

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::process::ExitCode {
    match run().await {
        Ok(code) => std::process::ExitCode::from(code),
        Err(e) => {
            eprintln!("{e}");
            std::process::ExitCode::from(2)
        }
    }
}
async fn run() -> Result<u8, Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1).collect::<Vec<_>>();
    if args == ["--help"] {
        print!("{USAGE}");
        return Ok(0);
    }
    let state = if args.first().is_some_and(|s| s == "--state") {
        if args.len() < 2 {
            return Err(USAGE.into());
        }
        let path = PathBuf::from(&args[1]);
        args.drain(..2);
        Some(path)
    } else {
        std::env::var_os("SYMBIOTIC_CORE_STATE").map(PathBuf::from)
    };
    let state = state.ok_or(USAGE)?;
    let call = args.first().ok_or(USAGE)?.as_str();
    if call == "serve" {
        if args.len() != 1 {
            return Err(USAGE.into());
        }
        server::serve(&state).await?;
        return Ok(0);
    }
    let value = if call == "reconcile" {
        let mut day = None;
        let mut warden = None;
        let (pairs, remainder) = args[1..].as_chunks::<2>();
        for pair in pairs {
            match pair[0].as_str() {
                "--day" if day.is_none() => day = Some(pair[1].clone()),
                "--warden-state" if warden.is_none() => warden = Some(pair[1].clone()),
                _ => return Err(USAGE.into()),
            }
        }
        if !remainder.is_empty() {
            return Err(USAGE.into());
        }
        // The server resolves paths from its own working directory, so send an absolute one.
        let warden = std::path::absolute(warden.ok_or(USAGE)?)?;
        json!({"day":day.ok_or(USAGE)?,"warden_state":warden})
    } else {
        if ![
            "job.create",
            "job.transition",
            "request.record",
            "budget.get",
            "decision.answer",
            "events.since",
        ]
        .contains(&call)
            || args.len() != 2
        {
            return Err(USAGE.into());
        }
        let mut text = args[1].clone();
        if text == "-" {
            text.clear();
            std::io::stdin().read_to_string(&mut text)?;
        }
        serde_json::from_str::<Value>(&text)?
    };
    let response = server::request(&state, call, value).await?;
    if let Some(error) = response.get("error") {
        eprintln!("{error}");
        return Ok(if call == "reconcile" { 2 } else { 1 });
    }
    let ok = response
        .get("ok")
        .ok_or("core response has neither ok nor error")?;
    if call == "reconcile" {
        println!("{}", serde_json::to_string_pretty(ok)?);
        Ok(if ok["passed"] == true { 0 } else { 1 })
    } else {
        println!("{ok}");
        Ok(0)
    }
}
