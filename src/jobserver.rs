use std::ffi::{OsStr, OsString};
use std::io;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;

use futures::StreamExt;
use futures::channel::mpsc;
use futures::channel::oneshot;

use crate::errors::*;

// The execution model of sccache is that on the first run it spawns a server
// in the background and detaches it.
// When normally executing the rust compiler from either cargo or make, it
// will use cargo/make's jobserver and limit its resource usage accordingly.
// When executing the rust compiler through the sccache server, that jobserver
// is not available, and spawning as many rustc as there are CPUs can lead to
// a quadratic use of the CPU resources (each rustc spawning as many threads
// as there are CPUs).
// One way around this issue is to inherit the jobserver from cargo or make
// when the sccache server is spawned, but that means that in some cases, the
// cargo or make process can't terminate until the sccache server terminates
// after its idle timeout (which also never happens if SCCACHE_IDLE_TIMEOUT=0).
// Also, if the sccache server ends up shared between multiple runs of
// cargo/make, then which jobserver is used doesn't make sense anymore.
// Ideally, the sccache client would give a handle to the jobserver it has
// access to, so that the rust compiler would "just" use the jobserver it
// would have used if it had run without sccache, but that adds some extra
// complexity, and requires to use Unix domain sockets.
// What we do instead is to arbitrary use our own jobserver.
// Unfortunately, that doesn't absolve us from having to deal with the original
// jobserver, because make may give us file descriptors to its pipes, and the
// simple fact of keeping them open can block it.
// So if it does give us those file descriptors, close the preemptively.
//
// unsafe because it can use the wrong fds.
#[cfg(not(windows))]
pub unsafe fn discard_inherited_jobserver() {
    if let Some(value) = ["CARGO_MAKEFLAGS", "MAKEFLAGS", "MFLAGS"]
        .into_iter()
        .find_map(|env| std::env::var(env).ok())
    {
        if let Some(auth) = value.rsplit(' ').find_map(|arg| {
            arg.strip_prefix("--jobserver-auth=")
                .or_else(|| arg.strip_prefix("--jobserver-fds="))
        }) {
            if !auth.starts_with("fifo:") {
                let mut parts = auth.splitn(2, ',');
                let read = parts.next().unwrap();
                let write = match parts.next() {
                    Some(w) => w,
                    None => return,
                };
                let read = read.parse().unwrap();
                let write = write.parse().unwrap();
                if read < 0 || write < 0 {
                    return;
                }
                unsafe {
                    if libc::fcntl(read, libc::F_GETFD) == -1 {
                        return;
                    }
                    if libc::fcntl(write, libc::F_GETFD) == -1 {
                        return;
                    }
                    libc::close(read);
                    libc::close(write);
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct Client {
    helper: Option<Arc<jobserver::HelperThread>>,
    tx: Option<mpsc::UnboundedSender<oneshot::Sender<io::Result<jobserver::Acquired>>>>,
    inner: jobserver::Client,
}

pub struct Acquired {
    _token: Option<jobserver::Acquired>,
}

impl Client {
    pub fn new() -> Client {
        Client::new_num(crate::util::num_cpus())
    }

    pub fn new_num(num: usize) -> Client {
        let inner = jobserver::Client::new(num).expect("failed to create jobserver");
        Client::_new(inner, false)
    }

    fn _new(inner: jobserver::Client, inherited: bool) -> Client {
        let (helper, tx) = if inherited {
            (None, None)
        } else {
            let (tx, mut rx) = mpsc::unbounded::<oneshot::Sender<_>>();
            let helper = inner
                .clone()
                .into_helper_thread(move |token| {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .build()
                        .unwrap();
                    rt.block_on(async {
                        if let Some(sender) = rx.next().await {
                            drop(sender.send(token));
                        }
                    });
                })
                .expect("failed to spawn helper thread");
            (Some(Arc::new(helper)), Some(tx))
        };

        Client { inner, helper, tx }
    }

    /// Configures this jobserver to be inherited by the specified command
    pub fn configure(&self, cmd: &mut Command) {
        self.inner.configure(cmd);
    }

    /// Returns a future that represents an acquired jobserver token.
    ///
    /// This should be invoked before any "work" is spawned (for whatever the
    /// definition of "work" is) to ensure that the system is properly
    /// rate-limiting itself.
    pub async fn acquire(&self) -> Result<Acquired> {
        let (helper, tx) = match (self.helper.as_ref(), self.tx.as_ref()) {
            (Some(a), Some(b)) => (a, b),
            _ => return Ok(Acquired { _token: None }),
        };
        let (mytx, myrx) = oneshot::channel();
        helper.request_token();
        tx.unbounded_send(mytx).unwrap();

        let acquired = myrx
            .await
            .context("jobserver helper panicked")?
            .context("failed to acquire jobserver token")?;

        Ok(Acquired {
            _token: Some(acquired),
        })
    }
}

// =====================================================================
// Per-request jobserver donation.
//
// Each Compile RPC carries the wrapper's environment in `env_vars`. The
// wrapper was launched by make and inherited make's `MAKEFLAGS`, which
// contains the jobserver auth string. The wrapper's own jobserver token
// is what gates make from spawning more recipes; if we hold it for the
// whole coordination wait, make's `-jN` parallelism collapses.
//
// To free that slot during a wait, the daemon writes one byte to the
// jobserver pipe (donate). When the wait ends, it reads one byte back
// (reacquire), restoring the at-rest token count exactly. The wrapper's
// normal exit-write balances the wrapper's start-read, so the donate +
// reacquire pair is self-contained from the daemon's perspective.
//
// We support the fifo:PATH form (make >= 4.4 / `--jobserver-style=fifo`).
// The pipe-fd form (R,W) only works for processes that inherited those
// fds; sccache deliberately discards them at daemonize time, so per-RPC
// donation against pipe-fd jobservers is not supported here.
// =====================================================================

/// A reference to a make-style jobserver pipe extracted from a Compile
/// request's environment variables.
pub struct JobserverPipe {
    fifo_path: PathBuf,
}

impl JobserverPipe {
    /// Try to extract the wrapper's jobserver auth from the env_vars
    /// captured at the call site. Returns `None` if MAKEFLAGS is absent
    /// or uses the legacy pipe-fd form.
    pub fn from_env_vars(env_vars: &[(OsString, OsString)]) -> Option<Self> {
        for var_name in &["CARGO_MAKEFLAGS", "MAKEFLAGS", "MFLAGS"] {
            for (k, v) in env_vars {
                if k.as_os_str() == OsStr::new(var_name) {
                    if let Some(s) = v.to_str() {
                        if let Some(path) = parse_fifo_auth(s) {
                            return Some(JobserverPipe {
                                fifo_path: PathBuf::from(path),
                            });
                        }
                    }
                }
            }
        }
        None
    }

    /// Donate one slot to the jobserver and return a guard. Reacquire
    /// happens in the guard's `Drop` -- when the guard goes out of
    /// scope, one byte is read back from the fifo. The read is the
    /// back-pressure: it blocks until make has rotated a token back
    /// into the pool, which is the right time to resume real work.
    pub fn donate(&self) -> io::Result<JobserverDonation> {
        use std::fs::OpenOptions;
        use std::io::Write;
        // Open RDWR so a transient empty pipe (no writer attached) does
        // not yield EOF on the matching reacquire path.
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.fifo_path)?;
        f.write_all(b"+")?;
        Ok(JobserverDonation {
            fifo_path: self.fifo_path.clone(),
        })
    }
}

/// Outstanding donation. Drop reads one byte back from the jobserver
/// fifo, blocking until a token is available -- this is the
/// back-pressure that bounds total concurrent donations and matches
/// make's semantics. Once Drop returns the caller again "holds" the
/// wrapper's slot and may proceed with real work.
pub struct JobserverDonation {
    fifo_path: PathBuf,
}

impl Drop for JobserverDonation {
    fn drop(&mut self) {
        let path = std::mem::take(&mut self.fifo_path);
        let read_byte = move || -> io::Result<()> {
            use std::fs::OpenOptions;
            use std::io::Read;
            let mut f = OpenOptions::new().read(true).write(true).open(&path)?;
            let mut buf = [0u8; 1];
            f.read_exact(&mut buf)?;
            Ok(())
        };
        // Inside a multi-thread tokio runtime (sccache's server runtime
        // always is), `block_in_place` lets the runtime migrate other
        // tasks off this worker so the blocking read doesn't freeze
        // unrelated work. Outside any runtime (tests that don't enter
        // tokio, shutdown paths) we just block this thread directly.
        let result = match tokio::runtime::Handle::try_current() {
            Ok(_) => tokio::task::block_in_place(read_byte),
            Err(_) => read_byte(),
        };
        if let Err(e) = result {
            log::warn!(
                "jobserver reacquire on drop failed ({}): make's -jN bookkeeping \
                 may be off by one for this build",
                e
            );
        }
    }
}

fn parse_fifo_auth(makeflags: &str) -> Option<&str> {
    makeflags.split_whitespace().find_map(|arg| {
        let auth = arg
            .strip_prefix("--jobserver-auth=")
            .or_else(|| arg.strip_prefix("--jobserver-fds="))?;
        auth.strip_prefix("fifo:")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_fifo() {
        assert_eq!(
            parse_fifo_auth("-j8 --jobserver-auth=fifo:/tmp/GMfifo123"),
            Some("/tmp/GMfifo123")
        );
        assert_eq!(
            parse_fifo_auth("--jobserver-auth=fifo:/tmp/x  -j4"),
            Some("/tmp/x")
        );
        assert_eq!(parse_fifo_auth("-j4 --jobserver-auth=3,4"), None);
        assert_eq!(parse_fifo_auth("-j4"), None);
    }

    #[test]
    fn from_env_vars_picks_fifo() {
        let envs = vec![
            (OsString::from("FOO"), OsString::from("bar")),
            (
                OsString::from("MAKEFLAGS"),
                OsString::from("--jobserver-auth=fifo:/tmp/jobs"),
            ),
        ];
        let p = JobserverPipe::from_env_vars(&envs).unwrap();
        assert_eq!(p.fifo_path, PathBuf::from("/tmp/jobs"));
    }

    #[test]
    fn from_env_vars_skips_pipe_fds() {
        let envs = vec![(
            OsString::from("MAKEFLAGS"),
            OsString::from("-j4 --jobserver-auth=3,4"),
        )];
        assert!(JobserverPipe::from_env_vars(&envs).is_none());
    }

    /// End-to-end donate -> drop round-trip against a real fifo.
    /// Verifies the byte accounting: donate writes one byte, the
    /// guard's Drop reads one byte back via `block_in_place`, net
    /// change is zero. This is the contract make relies on -- if
    /// donate+drop ever ends up unbalanced, make either deadlocks
    /// (under-count) or over-spawns (over-count).
    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn donate_then_drop_is_balanced() {
        use std::os::unix::fs::FileTypeExt;

        let dir = tempfile::tempdir().unwrap();
        let fifo = dir.path().join("fifo");
        // mkfifo via the libc binding so we don't pull in another dep.
        let c_path = std::ffi::CString::new(fifo.as_os_str().as_encoded_bytes()).unwrap();
        let rc = unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) };
        assert_eq!(rc, 0, "mkfifo failed: {}", io::Error::last_os_error());
        assert!(std::fs::metadata(&fifo).unwrap().file_type().is_fifo());

        // Pre-seed one byte into the pipe to simulate a token already
        // sitting in make's pool. We hold the writer end open for the
        // duration of the test so the pipe never reports EOF on reads.
        let pre_seeded_byte = b'x';
        let writer_holder = {
            use std::fs::OpenOptions;
            use std::io::Write;
            let mut f = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&fifo)
                .unwrap();
            f.write_all(&[pre_seeded_byte]).unwrap();
            f
        };

        let pipe = JobserverPipe {
            fifo_path: fifo.clone(),
        };

        // Donate: pipe should now contain 2 bytes (pre-seed + donated).
        let donation = pipe.donate().expect("donate failed");
        // Dropping reads exactly one byte back via block_in_place.
        // Whichever byte we get, exactly one should remain afterward.
        drop(donation);

        // Drain the remaining byte to confirm count == 1.
        use std::io::Read;
        use std::os::unix::io::AsRawFd;
        unsafe {
            let fd = writer_holder.as_raw_fd();
            let flags = libc::fcntl(fd, libc::F_GETFL);
            libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK);
        }
        let mut leftover = [0u8; 8];
        let mut reader = writer_holder;
        let n = reader.read(&mut leftover).unwrap_or(0);
        assert_eq!(
            n, 1,
            "expected exactly one byte left in pipe after balanced donate+drop, got {n}"
        );
    }
}
