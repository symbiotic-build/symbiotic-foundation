//! Async-signal-safe wakeup without adding a second Tokio signal implementation.
use super::*;
use std::{
    os::fd::AsRawFd,
    sync::atomic::{AtomicI32, AtomicUsize, Ordering},
};
static SIGNAL_FD: AtomicI32 = AtomicI32::new(-1);
static ACTIVE_HANDLERS: AtomicUsize = AtomicUsize::new(0);
extern "C" fn wake(_: libc::c_int) {
    // A signal may interrupt a failed syscall before its caller reads errno.
    // Preserve it around the nonblocking wakeup on our Linux/macOS deployments.
    #[cfg(target_os = "linux")]
    let errno = unsafe { libc::__errno_location() };
    #[cfg(target_vendor = "apple")]
    let errno = unsafe { libc::__error() };
    #[cfg(any(target_os = "linux", target_vendor = "apple"))]
    let saved_errno = unsafe { *errno };
    ACTIVE_HANDLERS.fetch_add(1, Ordering::SeqCst);
    let fd = SIGNAL_FD.load(Ordering::SeqCst);
    if fd >= 0 {
        // SAFETY: write is async-signal-safe; the nonblocking descriptor remains
        // owned until both handlers have been restored. A full socket is already readable.
        unsafe {
            libc::write(fd, [1u8].as_ptr().cast(), 1);
        }
    }
    ACTIVE_HANDLERS.fetch_sub(1, Ordering::SeqCst);
    #[cfg(any(target_os = "linux", target_vendor = "apple"))]
    unsafe {
        *errno = saved_errno;
    }
}
// Disable new writes and wait for any callback that already read the descriptor.
// The -2 sentinel also prevents a new installation during teardown. Callbacks do
// only lock-free atomics and a nonblocking write, so they never await this guard.
fn quiesce() {
    SIGNAL_FD.store(-2, Ordering::SeqCst);
    while ACTIVE_HANDLERS.load(Ordering::SeqCst) != 0 {
        std::hint::spin_loop();
    }
}

pub(super) struct Shutdown {
    read: UnixStream,
    _write: std::os::unix::net::UnixStream,
    old_term: libc::sigaction,
    old_int: libc::sigaction,
}
impl Shutdown {
    pub(super) fn install() -> Result<Self> {
        let (read, write) =
            std::os::unix::net::UnixStream::pair().map_err(|_| EgressError::StateUnavailable)?;
        read.set_nonblocking(true)
            .map_err(|_| EgressError::StateUnavailable)?;
        write
            .set_nonblocking(true)
            .map_err(|_| EgressError::StateUnavailable)?;
        let read = UnixStream::from_std(read).map_err(|_| EgressError::StateUnavailable)?;
        if SIGNAL_FD
            .compare_exchange(-1, write.as_raw_fd(), Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return Err(EgressError::StateUnavailable);
        }
        // SAFETY: sigaction structures are C POD; sigemptyset initializes the mask.
        let mut action: libc::sigaction = unsafe { std::mem::zeroed() };
        let mut old_term: libc::sigaction = unsafe { std::mem::zeroed() };
        let mut old_int: libc::sigaction = unsafe { std::mem::zeroed() };
        action.sa_sigaction = wake as *const () as usize;
        action.sa_flags = libc::SA_RESTART;
        // SAFETY: all pointers refer to live initialized structures.
        unsafe {
            libc::sigemptyset(&mut action.sa_mask);
            if libc::sigaction(libc::SIGTERM, &action, &mut old_term) != 0 {
                quiesce();
                SIGNAL_FD.store(-1, Ordering::Release);
                return Err(EgressError::StateUnavailable);
            }
            if libc::sigaction(libc::SIGINT, &action, &mut old_int) != 0 {
                quiesce();
                libc::sigaction(libc::SIGTERM, &old_term, std::ptr::null_mut());
                SIGNAL_FD.store(-1, Ordering::Release);
                return Err(EgressError::StateUnavailable);
            }
        }
        Ok(Self {
            read,
            _write: write,
            old_term,
            old_int,
        })
    }
    pub(super) async fn wait(&mut self) -> Result<()> {
        let mut byte = [0];
        self.read
            .read_exact(&mut byte)
            .await
            .map(|_| ())
            .map_err(|_| EgressError::StateUnavailable)
    }
}
impl Drop for Shutdown {
    fn drop(&mut self) {
        quiesce();
        // SAFETY: restore the handlers before closing their wakeup descriptor.
        unsafe {
            libc::sigaction(libc::SIGTERM, &self.old_term, std::ptr::null_mut());
            libc::sigaction(libc::SIGINT, &self.old_int, std::ptr::null_mut());
        }
        SIGNAL_FD.store(-1, Ordering::Release);
    }
}
