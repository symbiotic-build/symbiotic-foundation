use crate::{Error, Policy};
use std::{
    collections::HashSet,
    io::{self, Read},
    os::{
        fd::{AsRawFd, FromRawFd, OwnedFd},
        unix::process::CommandExt,
    },
    process::{Child, Command},
    sync::{
        Mutex, OnceLock,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    time::{Duration, Instant},
};

// Queued diagnostics beyond this are dropped while stderr does not drain.
const DIAGNOSTIC_QUEUE: usize = 16;
// Default wait for the parent-death diagnostic before exit.
const EXIT_DIAGNOSTIC_WAIT: Duration = Duration::from_secs(1);

type Diagnostic = (Vec<u8>, Option<mpsc::SyncSender<()>>);
// A failed writer start is not remembered: the next diagnostic tries again.
static DIAGNOSTICS: Mutex<Option<mpsc::SyncSender<Diagnostic>>> = Mutex::new(None);

/// Drop and termination diagnostics are best effort and never make the caller
/// wait on stderr. One writer thread performs ordinary blocking writes, so fd 2's
/// file status flags, which the app and every process that inherited its stderr
/// share, are never changed. A message is dropped only when the queue is full.
pub(crate) fn diagnostic(message: std::fmt::Arguments<'_>) {
    queue_diagnostic(message, None);
}

/// Like `diagnostic`, but waits for the supplied duration before process exit.
fn diagnostic_before_exit(message: std::fmt::Arguments<'_>, wait: Duration) {
    let (written, done) = mpsc::sync_channel(1);
    if queue_diagnostic(message, Some(written)) {
        let _ = done.recv_timeout(wait);
    }
}

fn queue_diagnostic(
    message: std::fmt::Arguments<'_>,
    written: Option<mpsc::SyncSender<()>>,
) -> bool {
    let Ok(mut diagnostics) = DIAGNOSTICS.lock() else {
        return false;
    };
    if diagnostics.is_none() {
        let (tx, rx) = mpsc::sync_channel::<Diagnostic>(DIAGNOSTIC_QUEUE);
        let started = std::thread::Builder::new()
            .name("foundation-diagnostics".into())
            .spawn(move || {
                for (bytes, written) in rx {
                    write_diagnostic(&bytes, |remaining| {
                        // SAFETY: write borrows the live message buffer.
                        let count = unsafe {
                            libc::write(
                                libc::STDERR_FILENO,
                                remaining.as_ptr().cast(),
                                remaining.len(),
                            )
                        };
                        if count == -1 {
                            Err(io::Error::last_os_error())
                        } else {
                            Ok(count as usize)
                        }
                    });
                    if let Some(written) = written {
                        let _ = written.send(());
                    }
                }
            });
        if started.is_err() {
            return false;
        }
        *diagnostics = Some(tx);
    }
    diagnostics.as_ref().is_some_and(|sender| {
        sender
            .try_send((format!("{message}\n").into_bytes(), written))
            .is_ok()
    })
}

fn write_diagnostic(mut bytes: &[u8], mut write: impl FnMut(&[u8]) -> io::Result<usize>) {
    while !bytes.is_empty() {
        match write(bytes) {
            Ok(0) => break,
            Ok(written) => bytes = &bytes[written..],
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            // Any other error ends this message, including WouldBlock when the
            // app itself made stderr nonblocking.
            Err(_) => break,
        }
    }
}

const PARENT_FD: &str = "SYMBIOTIC_PARENT_FD";
static WATCHING: AtomicBool = AtomicBool::new(false);
static WRITERS: Mutex<Option<HashSet<i32>>> = Mutex::new(None);

type SpawnRequest = (Command, mpsc::SyncSender<Result<ManagedChild, Error>>);
static SPAWNER: OnceLock<Result<mpsc::Sender<SpawnRequest>, io::Error>> = OnceLock::new();

pub(crate) fn spawn(command: Command) -> Result<ManagedChild, Error> {
    // This thread never exits when a supervisor or a child stops: Linux ties
    // PDEATHSIG to the creating thread, not to the thread-group leader.
    let spawner = SPAWNER
        .get_or_init(|| {
            let (tx, rx) = mpsc::channel::<SpawnRequest>();
            std::thread::Builder::new()
                .name("foundation-spawner".into())
                .spawn(move || {
                    while let Ok((command, reply)) = rx.recv() {
                        // A failed delivery drops ManagedChild, which kills and reaps it.
                        let _ = reply.send(spawn_here(command));
                    }
                })?;
            Ok(tx)
        })
        .as_ref()
        .map_err(|error| io::Error::new(error.kind(), "spawning thread failed"))?;
    let (tx, rx) = mpsc::sync_channel(1);
    spawner
        .send((command, tx))
        .map_err(|_| Error::Unavailable)?;
    rx.recv().map_err(|_| Error::Unavailable)?
}

struct Writer(Option<OwnedFd>);
impl Drop for Writer {
    fn drop(&mut self) {
        // Closing under the registry lock prevents a spawn snapshot from including
        // a recycled descriptor. The writer is closed before releasing the lock.
        match WRITERS.lock() {
            Ok(mut registry) => {
                if let Some(fd) = self.0.take() {
                    if let Some(writers) = registry.as_mut() {
                        writers.remove(&fd.as_raw_fd());
                    }
                    drop(fd);
                }
            }
            Err(_) => {
                drop(self.0.take());
                diagnostic(format_args!("parent pipe registry unavailable"));
            }
        }
    }
}

pub(crate) struct ManagedChild {
    child: Child,
    _writer: Writer,
    reaped: bool,
}
impl ManagedChild {
    pub(crate) fn id(&self) -> u32 {
        self.child.id()
    }
    pub(crate) fn try_wait(&mut self) -> Result<Option<std::process::ExitStatus>, Error> {
        let status = self.child.try_wait()?;
        self.reaped |= status.is_some();
        Ok(status)
    }
    pub(crate) fn stop(&mut self, policy: &Policy) -> Result<(), Error> {
        if self.try_wait()?.is_some() {
            return Ok(());
        }
        // SAFETY: a non-reaped child PID cannot be recycled. Signal the child only;
        // tool process groups are the app wrapper's responsibility.
        if unsafe { libc::kill(self.id() as i32, libc::SIGTERM) } != 0 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() != Some(libc::ESRCH) {
                return Err(error.into());
            }
        }
        let start = Instant::now();
        while start.elapsed() < Duration::from_millis(policy.stop_grace_ms) {
            if self.try_wait()?.is_some() {
                return Ok(());
            }
            std::thread::sleep(
                Duration::from_millis(policy.poll_ms).min(
                    Duration::from_millis(policy.stop_grace_ms).saturating_sub(start.elapsed()),
                ),
            );
        }
        if self.try_wait()?.is_none() {
            self.child.kill()?;
            self.child.wait()?;
            self.reaped = true;
        }
        Ok(())
    }
}
impl Drop for ManagedChild {
    fn drop(&mut self) {
        if !self.reaped {
            // Emergency cleanup on an error path. Explicit shutdown reports errors.
            let killed = self.child.kill();
            let reaped = self.child.wait();
            // Complete both cleanup operations before reporting either failure.
            if let Err(error) = killed {
                diagnostic(format_args!("child kill failed: {error}"));
            }
            if let Err(error) = reaped {
                diagnostic(format_args!("child reap failed: {error}"));
            }
        }
    }
}

fn spawn_here(mut command: Command) -> Result<ManagedChild, Error> {
    let mut writers = WRITERS.lock().map_err(|_| Error::Unavailable)?;
    let writers = writers.get_or_insert_with(HashSet::new);
    let mut fds = [-1; 2];
    // SAFETY: pipe writes exactly two descriptors to the supplied array.
    #[cfg(target_os = "linux")]
    let result = unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) };
    #[cfg(not(target_os = "linux"))]
    let result = unsafe { libc::pipe(fds.as_mut_ptr()) };
    if result != 0 {
        return Err(io::Error::last_os_error().into());
    }
    // SAFETY: pipe succeeded; these descriptors have unique ownership.
    let (reader, writer) = unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) };
    for fd in &fds {
        // SAFETY: both live pipe descriptors accept FD_CLOEXEC.
        if unsafe { libc::fcntl(*fd, libc::F_SETFD, libc::FD_CLOEXEC) } != 0 {
            return Err(io::Error::last_os_error().into());
        }
    }
    let reader = dedicated_pipe_fd(reader)?;
    let writer = dedicated_pipe_fd(writer)?;
    let mut inherited_writers: Vec<_> = writers.iter().copied().collect();
    inherited_writers.push(writer.as_raw_fd());
    let read_fd = reader.as_raw_fd();
    // SAFETY: getpid has no preconditions.
    #[cfg(target_os = "linux")]
    let parent = unsafe { libc::getpid() };
    command.env(PARENT_FD, read_fd.to_string());
    // SAFETY: only async-signal-safe syscalls execute in the forked child, with
    // captured numbers/arrays allocated before fork. No locks or allocation here.
    unsafe {
        command.pre_exec(move || {
            for fd in &inherited_writers {
                if libc::close(*fd) != 0 {
                    return Err(io::Error::last_os_error());
                }
            }
            if libc::fcntl(read_fd, libc::F_SETFD, 0) != 0 {
                return Err(io::Error::last_os_error());
            }
            #[cfg(target_os = "linux")]
            arm_parent_death(parent)?;
            Ok(())
        });
    }
    let child = command.spawn()?;
    writers.insert(writer.as_raw_fd());
    Ok(ManagedChild {
        child,
        _writer: Writer(Some(writer)),
        reaped: false,
    })
}

fn dedicated_pipe_fd(fd: OwnedFd) -> io::Result<OwnedFd> {
    if fd.as_raw_fd() >= 3 {
        return Ok(fd);
    }
    // SAFETY: duplicate the live descriptor above the standard streams, retaining
    // close-on-exec. Dropping the original releases its standard-stream slot.
    let duplicate = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 3) };
    if duplicate == -1 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: fcntl returned a new descriptor with unique ownership.
    Ok(unsafe { OwnedFd::from_raw_fd(duplicate) })
}

#[cfg(target_os = "linux")]
unsafe fn arm_parent_death(parent: libc::pid_t) -> io::Result<()> {
    // SAFETY: these syscalls accept integers and retain no pointers.
    unsafe {
        if libc::prctl(
            libc::PR_SET_PDEATHSIG,
            libc::SIGKILL as libc::c_ulong,
            0 as libc::c_ulong,
            0 as libc::c_ulong,
            0 as libc::c_ulong,
        ) != 0
        {
            return Err(io::Error::last_os_error());
        }
        if libc::getppid() != parent {
            libc::_exit(1);
        }
    }
    Ok(())
}

/// Watch the parent pipe on an independent thread, exiting on EOF even while busy.
/// Call before child configuration/secret reads. The helper sets the reserved
/// `SYMBIOTIC_PARENT_FD` environment variable; absence or a non-pipe FD is an error.
/// On pipe termination, `cleanup` runs once before the watcher exits.
/// SIGKILL (including Linux PDEATHSIG) cannot run callbacks.
/// EOF exits with 0; read or cleanup failure exits with 1. Readers become
/// close-on-exec again so grandchildren do not retain the entrypoint's descriptor.
pub fn watch_parent(
    cleanup: impl FnOnce() -> io::Result<()> + Send + 'static,
) -> Result<(), Error> {
    watch_parent_with_diagnostic_wait(cleanup, EXIT_DIAGNOSTIC_WAIT)
}

/// Watch the parent pipe with a host-supplied wait for the final failure diagnostic.
/// Cleanup runs before this wait. A blocked stderr cannot delay exit beyond it.
/// The pipe validation, cleanup and exit codes follow [`watch_parent`].
pub fn watch_parent_with_diagnostic_wait(
    cleanup: impl FnOnce() -> io::Result<()> + Send + 'static,
    diagnostic_wait: Duration,
) -> Result<(), Error> {
    let fd: i32 = std::env::var(PARENT_FD)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|fd| *fd >= 3)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "missing parent pipe"))?;
    // SAFETY: fstat writes the initialized struct; fcntl operates on the supplied FD.
    let mut stat: libc::stat = unsafe { std::mem::zeroed() };
    if unsafe { libc::fstat(fd, &mut stat) } != 0 || stat.st_mode & libc::S_IFMT != libc::S_IFIFO {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid parent pipe").into());
    }
    if unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) } != 0 {
        return Err(io::Error::last_os_error().into());
    }
    if WATCHING
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "parent watcher already started",
        )
        .into());
    }
    // SAFETY: the reserved descriptor is transferred once to this entrypoint.
    let mut pipe = unsafe { std::fs::File::from_raw_fd(fd) };
    std::thread::Builder::new()
        .name("foundation-parent-watch".into())
        .spawn(move || {
            let code = loop {
                match pipe.read(&mut [0]) {
                    Ok(0) => break 0,
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                    _ => break 1,
                }
            };
            if let Err(error) = cleanup() {
                diagnostic_before_exit(
                    format_args!("parent-death cleanup failed: {error}"),
                    diagnostic_wait,
                );
                std::process::exit(1);
            }
            std::process::exit(code);
        })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::write_diagnostic;
    use std::io;

    #[test]
    fn diagnostic_continues_after_partial_writes() {
        let message = b"cleanup failed\n";
        let mut output = Vec::new();
        let mut calls = 0;
        write_diagnostic(message, |remaining| {
            calls += 1;
            assert!(calls <= message.len(), "partial writes must make progress");
            let count = remaining.len().min(3);
            output.extend_from_slice(&remaining[..count]);
            Ok(count)
        });
        assert_eq!(output, message);
    }

    #[test]
    fn diagnostic_continues_after_interruption() {
        let message = b"cleanup failed\n";
        let mut calls = 0;
        let mut output = Vec::new();
        write_diagnostic(message, |remaining| {
            calls += 1;
            assert!(calls <= 2, "interrupted write must finish on the next call");
            if calls == 1 {
                Err(io::ErrorKind::Interrupted.into())
            } else {
                output.extend_from_slice(remaining);
                Ok(remaining.len())
            }
        });
        assert_eq!(output, message);
        assert_eq!(calls, 2);
    }

    #[test]
    fn diagnostic_stops_when_output_cannot_progress() {
        for outcome in [
            Ok(0),
            Err(io::ErrorKind::WouldBlock),
            Err(io::ErrorKind::BrokenPipe),
        ] {
            let mut calls = 0;
            write_diagnostic(b"cleanup failed\n", |_| {
                calls += 1;
                assert_eq!(calls, 1, "must not retry output that cannot progress");
                outcome.map_err(io::Error::from)
            });
            assert_eq!(calls, 1);
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn parent_mismatch_exits_after_arming_death_signal() {
        // SAFETY: the fork child performs only the async-signal-safe setup and _exit;
        // it deliberately models a parent that vanished before PR_SET_PDEATHSIG.
        let pid = unsafe { libc::fork() };
        assert!(pid >= 0);
        if pid == 0 {
            unsafe {
                let _ = super::arm_parent_death(libc::getpid());
                libc::_exit(42);
            }
        }
        let mut status = 0;
        // SAFETY: wait for this exact child, writing to a valid status pointer.
        assert_eq!(unsafe { libc::waitpid(pid, &mut status, 0) }, pid);
        assert_eq!(libc::WEXITSTATUS(status), 1);
    }
}
