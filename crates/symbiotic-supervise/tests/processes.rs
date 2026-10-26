#![cfg(unix)]
use std::{
    fs,
    path::Path,
    process::{Child, Command, Output, Stdio},
    time::{Duration, Instant},
};
mod common;
use common::{alive, policy, started, until, until_termination};

use symbiotic_supervise::{Error, Event, Supervisor};

// Paths are read when the tests run, not compiled in: a compiled-in path makes this test build
// specific to one checkout, so no other worktree can reuse it from the build cache.
fn fixture_binary() -> std::path::PathBuf {
    std::env::var_os("CARGO_BIN_EXE_symbiotic-supervise-test-fixture")
        .expect("cargo test sets CARGO_BIN_EXE_symbiotic-supervise-test-fixture")
        .into()
}

fn fixture(role: &str, dir: &Path) -> Command {
    common::fixture(&fixture_binary(), role, dir)
}

struct Reap(Child);
impl Drop for Reap {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn c_compiler(configured: Option<&str>) -> Command {
    let mut compiler = configured.unwrap_or("cc").split_whitespace();
    let mut command = Command::new(compiler.next().expect("C compiler executable"));
    command.args(compiler);
    command
}

fn compiler_output(mut command: Command, dir: &Path, timeout: Duration) -> Output {
    // Files retain both streams without risking a full capture pipe during the wait.
    let stdout = dir.join("compiler.stdout");
    let stderr = dir.join("compiler.stderr");
    let mut compiler = Reap(
        command
            .stdin(Stdio::null())
            .stdout(fs::File::create(&stdout).unwrap())
            .stderr(fs::File::create(&stderr).unwrap())
            .spawn()
            .unwrap(),
    );
    common::until_termination_with_timeout(timeout, || compiler.0.try_wait().unwrap().is_some());
    Output {
        status: compiler.0.wait().unwrap(),
        stdout: fs::read(stdout).unwrap(),
        stderr: fs::read(stderr).unwrap(),
    }
}

fn read_pid(path: &Path) -> u32 {
    until(|| fs::read_to_string(path).is_ok_and(|text| text.trim().parse::<u32>().is_ok()));
    fs::read_to_string(path).unwrap().trim().parse().unwrap()
}

#[test]
fn restarts_after_reaping_and_reports_crash_limit_with_backoff() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_owned();
    let supervisor = Supervisor::start(move || fixture("crash", &path), policy()).unwrap();
    let begin = Instant::now();
    for _ in 0..3 {
        let pid = started(&supervisor);
        assert!(
            matches!(supervisor.next_event().unwrap(), Event::Exited(status) if status.code() == Some(7))
        );
        assert!(!alive(pid), "exit must be reaped before restart");
    }
    assert!(begin.elapsed() >= Duration::from_millis(80));
    assert!(matches!(
        supervisor.next_event(),
        Err(Error::CrashLimit {
            restarts: 2,
            window_ms: 10_000,
            ..
        })
    ));
    supervisor.stop().unwrap();
}

#[test]
fn graceful_stop_and_forced_stop_both_reap() {
    for role in ["graceful", "stubborn"] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_owned();
        let supervisor = Supervisor::start(move || fixture(role, &path), policy()).unwrap();
        let pid = started(&supervisor);
        until(|| dir.path().join(format!("{role}.ready")).exists());
        let start = Instant::now();
        supervisor.stop().unwrap();
        assert!(!alive(pid));
        if role == "graceful" {
            assert!(dir.path().join("stopped").exists());
        } else {
            assert!(start.elapsed() >= Duration::from_millis(100));
        }
    }
}

#[test]
fn parent_sigkill_closes_pipe_while_children_are_busy_and_grandchild_lives() {
    let dir = tempfile::tempdir().unwrap();
    let mut parent = Reap(fixture("parent", dir.path()).spawn().unwrap());
    until(|| dir.path().join("parent.ready").exists());
    let child = read_pid(&dir.path().join("child.pid"));
    let sibling = read_pid(&dir.path().join("sibling.pid"));
    let grandchild = read_pid(&dir.path().join("grandchild.pid"));
    parent.0.kill().unwrap();
    parent.0.wait().unwrap();
    until(|| !alive(child) && !alive(sibling));
    assert!(alive(grandchild), "grandchild must not delay watcher EOF");
    // This unmodified test tool has no wrapper: clean it up explicitly.
    unsafe {
        libc::kill(grandchild as i32, libc::SIGKILL);
    }
}

#[test]
fn temporary_calling_thread_does_not_own_parent_death_signal() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_owned();
    let supervisor = std::thread::spawn(move || {
        Supervisor::start(move || fixture("stubborn", &path), policy()).unwrap()
    })
    .join()
    .unwrap();
    let pid = started(&supervisor);
    until(|| dir.path().join("stubborn.ready").exists());
    assert!(alive(pid));
    supervisor.stop().unwrap();
}

#[test]
fn failed_spawn_is_visible_and_invalid_policy_is_refused() {
    let supervisor =
        Supervisor::start(|| Command::new("/nonexistent-foundation-child"), policy()).unwrap();
    assert!(matches!(supervisor.next_event(), Err(Error::Io(_))));
    supervisor.stop().unwrap();
    let mut invalid = policy();
    invalid.version = 2;
    assert!(matches!(
        Supervisor::start(|| Command::new("unused"), invalid),
        Err(Error::InvalidPolicy)
    ));
}

#[cfg(target_os = "macos")]
#[test]
fn app_owned_wrapper_kills_and_reaps_tool_group_on_parent_eof() {
    let dir = tempfile::tempdir().unwrap();
    let mut command = fixture("parent", dir.path());
    command.env("SUPERVISE_TEST_WRAPPER", "1");
    let mut parent = Reap(command.spawn().unwrap());
    until(|| dir.path().join("parent.ready").exists());
    let wrapper = read_pid(&dir.path().join("child.pid"));
    let tool = read_pid(&dir.path().join("grandchild.pid"));
    let descendant = read_pid(&dir.path().join("tool-descendant.pid"));
    parent.0.kill().unwrap();
    parent.0.wait().unwrap();
    until(|| !alive(wrapper) && !alive(tool) && !alive(descendant));
}

#[cfg(target_os = "linux")]
#[test]
fn linux_parent_death_signal_kills_an_unwatched_busy_child() {
    let dir = tempfile::tempdir().unwrap();
    let mut parent = Reap(fixture("linux-parent", dir.path()).spawn().unwrap());
    until(|| dir.path().join("parent.ready").exists());
    let child = read_pid(&dir.path().join("child.pid"));
    assert!(alive(child));
    parent.0.kill().unwrap();
    parent.0.wait().unwrap();
    until(|| !alive(child));
}

#[test]
fn closed_standard_streams_do_not_replace_the_parent_pipe() {
    let dir = tempfile::tempdir().unwrap();
    let mut parent = Reap(fixture("closed-stdin-parent", dir.path()).spawn().unwrap());
    until(|| parent.0.try_wait().unwrap().is_some());
    assert!(parent.0.wait().unwrap().success());
}

#[test]
fn cleanup_failure_exits_even_with_a_broken_stderr_pipe() {
    use std::os::{fd::AsRawFd, unix::process::CommandExt};
    let dir = tempfile::tempdir().unwrap();
    let (reader, writer) = std::io::pipe().unwrap();
    let read_fd = reader.as_raw_fd();
    let mut command = fixture("broken-stderr", dir.path());
    command.env("SYMBIOTIC_PARENT_FD", read_fd.to_string());
    // The pipe is close-on-exec, so unrelated fixtures cannot retain its writer.
    // SAFETY: pre_exec explicitly inherits only this child's parent reader.
    unsafe {
        command.pre_exec(move || {
            if libc::fcntl(read_fd, libc::F_SETFD, 0) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = Reap(command.spawn().unwrap());
    drop(reader);
    until(|| dir.path().join("broken-stderr.ready").exists());
    drop(writer);
    until(|| child.0.try_wait().unwrap().is_some());
    assert_eq!(child.0.wait().unwrap().code(), Some(1));
}

#[test]
fn cleanup_failure_writes_its_diagnostic_before_exit() {
    use std::os::{fd::AsRawFd, unix::process::CommandExt};
    let dir = tempfile::tempdir().unwrap();
    let stderr = dir.path().join("watcher.stderr");
    let (reader, writer) = std::io::pipe().unwrap();
    let read_fd = reader.as_raw_fd();
    let mut command = fixture("full-stderr", dir.path());
    command
        .env("SYMBIOTIC_PARENT_FD", read_fd.to_string())
        .stderr(fs::File::create(&stderr).unwrap());
    // The pipe is close-on-exec, so unrelated fixtures cannot retain its writer.
    // SAFETY: pre_exec explicitly inherits only this child's parent reader.
    unsafe {
        command.pre_exec(move || {
            if libc::fcntl(read_fd, libc::F_SETFD, 0) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = Reap(command.spawn().unwrap());
    drop(reader);
    until_termination(|| dir.path().join("full-stderr.ready").exists());
    drop(writer);
    until_termination(|| child.0.try_wait().unwrap().is_some());
    assert_eq!(child.0.wait().unwrap().code(), Some(1));
    let written = fs::read_to_string(&stderr).unwrap();
    assert!(
        written.contains("parent-death cleanup failed"),
        "writable stderr must receive the diagnostic, got {written:?}"
    );
}

#[test]
fn full_stderr_does_not_block_parent_death_exit() {
    use std::os::{fd::AsRawFd, unix::process::CommandExt};
    let dir = tempfile::tempdir().unwrap();
    let (reader, writer) = std::io::pipe().unwrap();
    let read_fd = reader.as_raw_fd();
    let mut command = fixture("full-stderr", dir.path());
    command
        .env("SYMBIOTIC_PARENT_FD", read_fd.to_string())
        .env("SUPERVISE_TEST_FULL_STDERR", "1");
    // The pipe is close-on-exec, so unrelated fixtures cannot retain its writer.
    // SAFETY: pre_exec explicitly inherits only this child's parent reader.
    unsafe {
        command.pre_exec(move || {
            if libc::fcntl(read_fd, libc::F_SETFD, 0) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = Reap(command.spawn().unwrap());
    drop(reader);
    until_termination(|| dir.path().join("full-stderr.ready").exists());
    drop(writer);
    until_termination(|| child.0.try_wait().unwrap().is_some());
    assert_eq!(child.0.wait().unwrap().code(), Some(1));
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn shutdown_error_returns_after_emergency_cleanup() {
    shutdown_error(false);
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn full_stderr_does_not_block_emergency_reaping() {
    shutdown_error(true);
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn shutdown_error(refuse_kill: bool) {
    let dir = tempfile::tempdir().unwrap();
    let mut command = fixture("shutdown-error-parent", dir.path());
    if refuse_kill {
        command
            .env("SUPERVISE_TEST_REFUSE_KILL", "1")
            .env("SUPERVISE_TEST_FULL_STDERR", "1")
            .env(
                "SUPERVISE_TEST_EXIT_TRIGGER",
                dir.path().join("exit-trigger"),
            );
    }
    if cfg!(target_os = "macos") || refuse_kill {
        let library = dir.path().join(if cfg!(target_os = "macos") {
            "refuse-term.dylib"
        } else {
            "refuse-term.so"
        });
        let configured = std::env::var_os("CC");
        let mut compiler = c_compiler(
            configured
                .as_deref()
                .map(|value| value.to_str().expect("CC must be valid UTF-8")),
        );
        compiler
            .args(if cfg!(target_os = "macos") {
                vec!["-O2", "-dynamiclib"]
            } else {
                vec!["-O2", "-shared", "-fPIC", "-ldl"]
            })
            .arg(
                std::path::Path::new(
                    &std::env::var_os("CARGO_MANIFEST_DIR")
                        .expect("cargo test sets CARGO_MANIFEST_DIR"),
                )
                .join("tests/refuse_term.c"),
            )
            .arg("-o")
            .arg(&library);
        let output = compiler_output(compiler, dir.path(), Duration::from_secs(60));
        assert!(
            output.status.success(),
            "compiler stdout:\n{}\ncompiler stderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        command.env(
            if cfg!(target_os = "macos") {
                "DYLD_INSERT_LIBRARIES"
            } else {
                "LD_PRELOAD"
            },
            library,
        );
    }
    #[cfg(target_os = "linux")]
    if !refuse_kill {
        use std::os::unix::process::CommandExt;
        // SAFETY: pre_exec uses only prctl and preallocated seccomp data.
        unsafe {
            command.pre_exec(|| {
                let filter = [
                    libc::sock_filter {
                        code: 0x20,
                        jt: 0,
                        jf: 0,
                        k: 0,
                    }, // syscall
                    libc::sock_filter {
                        code: 0x15,
                        jt: 0,
                        jf: 3,
                        k: libc::SYS_kill as u32,
                    },
                    libc::sock_filter {
                        code: 0x20,
                        jt: 0,
                        jf: 0,
                        k: 24,
                    }, // signal argument
                    libc::sock_filter {
                        code: 0x15,
                        jt: 0,
                        jf: 1,
                        k: libc::SIGTERM as u32,
                    },
                    libc::sock_filter {
                        code: 0x06,
                        jt: 0,
                        jf: 0,
                        k: libc::SECCOMP_RET_ERRNO | libc::EPERM as u32,
                    },
                    libc::sock_filter {
                        code: 0x06,
                        jt: 0,
                        jf: 0,
                        k: libc::SECCOMP_RET_ALLOW,
                    },
                ];
                let program = libc::sock_fprog {
                    len: filter.len() as u16,
                    filter: filter.as_ptr() as *mut _,
                };
                if libc::prctl(
                    libc::PR_SET_NO_NEW_PRIVS,
                    1 as libc::c_ulong,
                    0 as libc::c_ulong,
                    0 as libc::c_ulong,
                    0 as libc::c_ulong,
                ) != 0
                    || libc::prctl(
                        libc::PR_SET_SECCOMP,
                        libc::SECCOMP_MODE_FILTER as libc::c_ulong,
                        &program,
                    ) != 0
                {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
    }
    let mut parent = Reap(command.spawn().unwrap());
    until_termination(|| parent.0.try_wait().unwrap().is_some());
    assert!(parent.0.wait().unwrap().success());
}
