//! Bounded-step agent runtime. Interface: crates/symbiotic-agent/README.md.

fn main() {
    if std::env::args().nth(1).as_deref() == Some("--version") {
        println!("symbiotic-agent {}", env!("CARGO_PKG_VERSION"));
        return;
    }
    eprintln!("symbiotic-agent: run is not implemented yet");
    std::process::exit(1);
}
