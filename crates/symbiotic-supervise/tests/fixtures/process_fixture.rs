#![cfg(unix)]
#[path = "../common/mod.rs"]
mod common;

use common::{alive, policy, started, until};
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::Duration,
};
use symbiotic_supervise::{Error, Supervisor, watch_parent};

fn fixture(role: &str, dir: &Path) -> Command {
    common::fixture(&std::env::current_exe().unwrap(), role, dir)
}

fn full_stderr() -> std::io::PipeReader {
    use std::os::fd::AsRawFd;
    let (reader, writer) = std::io::pipe().unwrap();
    let write_fd = writer.as_raw_fd();
    // SAFETY: this isolated fixture owns both new descriptors and fd 2.
    unsafe {
        assert_eq!(libc::fcntl(write_fd, libc::F_SETFL, libc::O_NONBLOCK), 0);
        let chunk = [b'x'; 4096];
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        // Fill native pipe capacity in blocks, then fill any tail byte by byte.
        for bytes in [&chunk[..], &chunk[..1]] {
            loop {
                assert!(std::time::Instant::now() < deadline, "pipe fill hang guard");
                if libc::write(write_fd, bytes.as_ptr().cast(), bytes.len()) == -1 {
                    assert_eq!(
                        std::io::Error::last_os_error().kind(),
                        std::io::ErrorKind::WouldBlock
                    );
                    break;
                }
            }
        }
        assert_eq!(libc::fcntl(write_fd, libc::F_SETFL, 0), 0);
        assert_eq!(
            libc::dup2(write_fd, libc::STDERR_FILENO),
            libc::STDERR_FILENO
        );
        drop(writer);
        reader // Keep the undrained reader open: writes must face EAGAIN, not EPIPE.
    }
}

fn main() {
    let Ok(role) = std::env::var("SUPERVISE_TEST_ROLE") else {
        return;
    };
    let dir = PathBuf::from(std::env::var_os("SUPERVISE_TEST_DIR").unwrap());
    let _stderr_reader = std::env::var_os("SUPERVISE_TEST_FULL_STDERR").map(|_| full_stderr());
    if role == "closed-stdin-parent" {
        // Close all standard streams so both pipe ends need relocation.
        // SAFETY: this isolated fixture owns its standard descriptors.
        unsafe {
            for fd in 0..3 {
                libc::close(fd);
            }
        }
        let child_dir = dir.clone();
        let child = Supervisor::start(
            move || {
                let mut command = fixture("busy", &child_dir);
                command.stdin(Stdio::null()).stderr(Stdio::null());
                command
            },
            policy(),
        )
        .unwrap();
        until(|| dir.join("busy.ready").exists());
        child.stop().unwrap();
        std::process::exit(0);
    }
    if role == "shutdown-error-parent" {
        let child_dir = dir.clone();
        let reap_error = std::env::var_os("SUPERVISE_TEST_REFUSE_KILL").is_some();
        let child = Supervisor::start(
            move || {
                fixture(
                    if reap_error {
                        "exit-on-trigger"
                    } else {
                        "busy"
                    },
                    &child_dir,
                )
            },
            policy(),
        )
        .unwrap();
        let pid = started(&child);
        until(|| {
            dir.join(if reap_error {
                "exit-on-trigger.ready"
            } else {
                "busy.ready"
            })
            .exists()
        });
        assert!(
            matches!(child.stop(), Err(Error::Io(error)) if error.raw_os_error() == Some(libc::EPERM))
        );
        assert!(!alive(pid), "shutdown failure must still kill and reap");
        if reap_error {
            let mut status = 0;
            // SAFETY: probe only this managed child; WNOHANG cannot block.
            assert_eq!(
                unsafe { libc::waitpid(pid as i32, &mut status, libc::WNOHANG) },
                -1
            );
            assert_eq!(
                std::io::Error::last_os_error().raw_os_error(),
                Some(libc::ECHILD),
                "emergency cleanup must reap, not leave a zombie"
            );
        }
        std::process::exit(0);
    }
    if role == "full-stderr" {
        symbiotic_supervise::watch_parent_with_diagnostic_wait(
            || Err(std::io::Error::from_raw_os_error(libc::EPERM)),
            Duration::from_millis(10),
        )
        .unwrap();
        fs::write(dir.join("full-stderr.ready"), "ready").unwrap();
        loop {
            std::thread::park();
        }
    }
    if role == "broken-stderr" {
        use std::os::fd::{FromRawFd, OwnedFd};
        let mut fds = [-1; 2];
        // SAFETY: this fixture owns both pipe descriptors; disconnect the reader
        // and replace stderr with the writer (closing stderr alone is insufficient).
        unsafe {
            assert_eq!(libc::pipe(fds.as_mut_ptr()), 0);
            let reader = OwnedFd::from_raw_fd(fds[0]);
            let writer = OwnedFd::from_raw_fd(fds[1]);
            assert_eq!(libc::dup2(fds[1], libc::STDERR_FILENO), libc::STDERR_FILENO);
            drop(reader);
            drop(writer);
        }
        watch_parent(|| Err(std::io::Error::from_raw_os_error(libc::EPERM))).unwrap();
        fs::write(dir.join("broken-stderr.ready"), "ready").unwrap();
        loop {
            std::hint::spin_loop();
        }
    }
    #[cfg(target_os = "linux")]
    if role == "linux-parent" {
        let child_dir = dir.clone();
        let child = Supervisor::start(move || fixture("unwatched", &child_dir), policy()).unwrap();
        fs::write(dir.join("child.pid"), started(&child).to_string()).unwrap();
        until(|| dir.join("unwatched.ready").exists());
        fs::write(dir.join("parent.ready"), "ready").unwrap();
        loop {
            std::thread::sleep(Duration::from_secs(1));
        }
    }
    #[cfg(target_os = "linux")]
    {
        // PDEATHSIG belongs to the exec thread. A libtest worker would see zero
        // and could not disable the exec thread's signal for the pipe-only roles.
        if role != "parent" && role != "crash" {
            let mut signal = 0;
            // SAFETY: PR_GET_PDEATHSIG writes the signal to the supplied integer.
            assert_eq!(
                unsafe {
                    libc::prctl(
                        libc::PR_GET_PDEATHSIG,
                        &mut signal,
                        0 as libc::c_ulong,
                        0 as libc::c_ulong,
                        0 as libc::c_ulong,
                    )
                },
                0
            );
            assert_eq!(signal, libc::SIGKILL);
        }
        if role == "unwatched" {
            fs::write(dir.join("unwatched.ready"), "ready").unwrap();
            loop {
                std::hint::spin_loop();
            }
        }
    }
    if role == "parent" {
        let wrapped = std::env::var_os("SUPERVISE_TEST_WRAPPER").is_some();
        let first_dir = dir.clone();
        let first = Supervisor::start(
            move || fixture(if wrapped { "wrapper" } else { "grandchild" }, &first_dir),
            policy(),
        )
        .unwrap();
        fs::write(dir.join("child.pid"), started(&first).to_string()).unwrap();
        let second_dir = dir.clone();
        let second = Supervisor::start(move || fixture("busy", &second_dir), policy()).unwrap();
        fs::write(dir.join("sibling.pid"), started(&second).to_string()).unwrap();
        until(|| dir.join("grandchild.pid").exists() && dir.join("busy.ready").exists());
        fs::write(dir.join("parent.ready"), "ready").unwrap();
        loop {
            std::thread::sleep(Duration::from_secs(1));
        }
    }
    if role == "crash" {
        std::process::exit(7);
    }
    if role == "wrapper" {
        use std::os::unix::process::CommandExt;
        let mut tool = Command::new("/bin/sh")
            .args(["-c", "sleep 20 & echo $! > \"$1\"; wait", "fixture"])
            .arg(dir.join("tool-descendant.pid"))
            .process_group(0)
            .spawn()
            .unwrap();
        let pid = tool.id();
        watch_parent(move || {
            // SAFETY: the wrapper owns this still-unreaped tool's process group.
            if unsafe { libc::kill(-(pid as i32), libc::SIGKILL) } != 0 {
                return Err(std::io::Error::last_os_error());
            }
            tool.wait()?;
            Ok(())
        })
        .unwrap();
        fs::write(dir.join("grandchild.pid"), pid.to_string()).unwrap();
    } else {
        watch_parent(|| Ok(())).unwrap();
    }
    assert!(
        watch_parent(|| Ok(())).is_err(),
        "a second watcher must not own the same FD"
    );
    // Exercise the pipe even on Linux: PDEATHSIG must not mask inherited writers.
    #[cfg(target_os = "linux")]
    if role == "grandchild" || role == "busy" {
        // Disable only in the pipe-specific fixture, to distinguish EOF from SIGKILL.
        unsafe {
            assert_eq!(libc::prctl(libc::PR_SET_PDEATHSIG, 0 as libc::c_ulong), 0);
        }
    }
    if role == "grandchild" {
        let mut child = Command::new("/bin/sleep").arg("20").spawn().unwrap();
        fs::write(dir.join("grandchild.pid"), child.id().to_string()).unwrap();
        std::thread::spawn(move || child.wait().unwrap());
    }
    if role == "graceful" {
        // The shell's trap tests that SIGTERM arrives before forced stop.
        use std::os::unix::process::CommandExt;
        let error = Command::new("/bin/sh").args(["-c", "trap 'echo stopped > \"$1\"; exit 0' TERM; echo ready > \"$2\"; while :; do sleep 0.01; done", "fixture"])
            .arg(dir.join("stopped")).arg(dir.join("graceful.ready")).exec();
        panic!("exec failed: {error}");
    }
    if role == "stubborn" {
        // SAFETY: install the OS's signal-ignore disposition, with no callback.
        unsafe {
            libc::signal(libc::SIGTERM, libc::SIG_IGN);
        }
    }
    fs::write(dir.join(format!("{role}.ready")), "ready").unwrap();
    if role == "exit-on-trigger" {
        common::until_termination(|| dir.join("exit-trigger").exists());
        return;
    }
    // Parent watcher runs independently while the main thread is occupied.
    loop {
        std::hint::spin_loop();
    }
}
