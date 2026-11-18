pub(crate) mod files;
pub(crate) mod journal;
pub(crate) mod waits;

use crate::fixtures::{client_bin, fail, tempdir, Server};
use crate::mirror::waits::wait_for;
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};
use zonepair_client::{
    daemon::{self, owner},
    sync::persist::state,
};

// ---- mirror sync (zonepaird) helpers — T-701–T-716 ---------------------------
//
// Three helper families per the §5 E-layer spec:
//   1. daemon spawn/shutdown: `spawn_daemon` (stub-client zonepaird child),
//      `Daemon`, `stop_writer`, `shutdown_writer` — every test-owned writer
//      dies with its scope so no zonepaird outlives the suite.
//   2. journal manipulation: `write_intent` (journal::append), `journal_empty`
//      (pending_count), `journal_intents` (read_pending), `read_state`
//      (strict read-only state.json decode — never quarantines).
//   3. convergence waiting: `wait_for` predicate polling at the RFC's ≤50 ms
//      cadence, `wait_state`/`wait_lock_free`/`wait_journal_drained` on top.
//
// Every A-side process (`zonepaird`, `sync`, `sess`, transport stubs) runs as
// a real `stub-client` subprocess — the argv-adaptation and cross-process
// wait paths under test only exist there.

/// A scenario's mirror root + scratch dir. Drop stops whatever writer holds
/// `.sync/` — our spawned `Daemon`s AND daemons detached-spawned by
/// stub-client subprocesses (`ensure_owner`/waiter revive) — then removes
/// the tempdir unless ZONEPAIR_KEEP is set.
pub(crate) struct MirrorHome {
    dir: PathBuf,
    pub(crate) mirror: PathBuf,
}

impl MirrorHome {
    pub(crate) fn new() -> Self {
        let dir = tempdir();
        let mirror = dir.join("mirror");
        std::fs::create_dir_all(&mirror).unwrap();
        MirrorHome { dir, mirror }
    }
    /// `<mirror>/.sync/`
    pub(crate) fn sync_dir(&self) -> PathBuf {
        state::sync_dir(&self.mirror)
    }
    /// Daemon stderr log — outside `.sync` and outside `projects/**`, so it
    /// enters neither the state namespace nor the reconcile scan set.
    pub(crate) fn log_path(&self) -> PathBuf {
        self.dir.join("daemon.log")
    }
    pub(crate) fn log_text(&self) -> String {
        std::fs::read_to_string(self.log_path()).unwrap_or_default()
    }
}

impl Drop for MirrorHome {
    fn drop(&mut self) {
        shutdown_writer(&self.sync_dir());
        if std::env::var("ZONEPAIR_KEEP").is_err() {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }
}

/// One spawned `stub-client zonepaird` child. `kill()` is the crash
/// injection — SIGKILL on unix, no flush, no commit, lock dies with the
/// process (RFC-0003-zonepaird §9.3's full-coverage row). Drop kills if still running.
pub(crate) struct Daemon {
    child: Child,
}

impl Daemon {
    pub(crate) fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
    pub(crate) fn exited(&mut self) -> Option<std::process::ExitStatus> {
        self.child.try_wait().unwrap()
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        self.kill();
    }
}

/// `stub-client zonepaird --mirror M --url U [--once]`, stderr appended to
/// the scenario's daemon log (the suspension/reconcile evidence surface).
/// The child takes the lock a few ms AFTER spawn — callers that write
/// intents must wait for ownership (`spawn_daemon_up` or `wait_writer_up`)
/// before `journal::append`, or a concurrent `ensure_owner` can spawn a
/// competitor whose boot cleanup sweeps the in-flight `*.tmp` mid-append.
pub(crate) fn spawn_daemon(home: &MirrorHome, server: &Server, extra_args: &[&str]) -> Daemon {
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(home.log_path())
        .unwrap();
    let mut cmd = Command::new(client_bin());
    cmd.arg("zonepaird")
        .arg("--mirror")
        .arg(&home.mirror)
        .arg("--url")
        .arg(server.base.as_str())
        .args(extra_args)
        .env("CLAUDE_STUB_URL", server.base.as_str())
        .env_remove("CLAUDE_STUB_TOKEN")
        .env("ZONEPAIR_CLIENT_CONFIG", home.dir.join("no-such-client.toml"))
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(log);
    let child = cmd.spawn().unwrap_or_else(|e| fail(&format!("spawn zonepaird: {e}")));
    Daemon { child }
}

/// `spawn_daemon` + wait until the child actually owns the lock (or
/// exits). After this, `ensure_owner` in any other process answers
/// AlreadyOwned — no competitor boots, no cleanup sweep races appends.
pub(crate) fn spawn_daemon_up(home: &MirrorHome, server: &Server, extra_args: &[&str]) -> Daemon {
    let mut daemon = spawn_daemon(home, server, extra_args);
    wait_writer_up(home, &mut daemon);
    daemon
}

pub(crate) fn wait_writer_up(home: &MirrorHome, daemon: &mut Daemon) {
    let sync_dir = home.sync_dir();
    wait_for("writer to own owner.lock", Duration::from_secs(20), || {
        daemon.exited().is_some() || !owner::lock_free(&sync_dir)
    });
    if let Some(status) = daemon.exited() {
        fail(&format!("zonepaird exited during boot: {status:?}"));
    }
}

/// Env for a stub-client CLI subprocess (`sync`, `sess`, the transport
/// adapter): everything resolves from the environment, never from the
/// host's real client config.
pub(crate) fn stub_env(cmd: &mut Command, home: &MirrorHome, server: &Server) {
    cmd.env("CLAUDE_STUB_URL", server.base.as_str())
        .env_remove("CLAUDE_STUB_TOKEN")
        .env("CLAUDE_CONFIG_DIR", &home.mirror)
        .env("CLAUDE_STUB_MIRROR", "1")
        .env("ZONEPAIR_CLIENT_CONFIG", home.dir.join("no-such-client.toml"));
}

/// Run a stub-client CLI to completion within `timeout` — returns None on
/// timeout (child killed). Assertions live on the returned Output.
pub(crate) fn run_stub(home: &MirrorHome, server: &Server, args: &[&str], timeout: Duration) -> Option<std::process::Output> {
    let mut cmd = Command::new(client_bin());
    cmd.args(args);
    stub_env(&mut cmd, home, server);
    let mut child = cmd
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|e| fail(&format!("spawn stub-client {args:?}: {e}")));
    let deadline = Instant::now() + timeout;
    loop {
        match child.try_wait().unwrap() {
            Some(_) => return Some(child.wait_with_output().unwrap()),
            None => {
                if Instant::now() >= deadline {
                    let _ = child.kill();
                    let _ = child.wait_with_output();
                    return None;
                }
                std::thread::sleep(Duration::from_millis(25));
            }
        }
    }
}

/// A live transport stub (`stub-client` with claude argv — the relay). The
/// test drives stdin lines and reads stdout frames through a reader thread,
/// like the SDK would.
pub(crate) struct StubProc {
    child: Child,
    rx: std::sync::mpsc::Receiver<String>,
    stderr: std::sync::mpsc::Receiver<String>,
}

impl StubProc {
    pub(crate) fn send(&mut self, line: &str) {
        use std::io::Write;
        let stdin = self.child.stdin.as_mut().unwrap_or_else(|| fail("stub stdin closed"));
        writeln!(stdin, "{line}").unwrap_or_else(|e| fail(&format!("stub stdin write: {e}")));
        stdin.flush().unwrap();
    }
    pub(crate) fn close_stdin(&mut self) {
        drop(self.child.stdin.take());
    }
    /// Next relayed stdout line within `timeout` (None on timeout/eof).
    pub(crate) fn next_line(&self, timeout: Duration) -> Option<String> {
        self.rx.recv_timeout(timeout).ok()
    }
    /// Wait for one JSON frame of `typ` (e.g. "result"), skipping others.
    pub(crate) fn wait_frame(&self, typ: &str, timeout: Duration) -> Value {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            let left = deadline.saturating_duration_since(Instant::now());
            let Some(line) = self.next_line(left.min(Duration::from_secs(2))) else {
                continue;
            };
            if let Ok(v) = serde_json::from_str::<Value>(&line) {
                if v.get("type").and_then(Value::as_str) == Some(typ) {
                    return v;
                }
            }
        }
        fail(&format!("no {typ} frame within {timeout:?}"));
    }
    /// Assert the relay produces nothing for `dur` (WS-not-established
    /// observable: stdout stays empty until open_stdio returns).
    pub(crate) fn assert_silent(&self, dur: Duration) {
        if let Some(line) = self.rx.recv_timeout(dur).ok() {
            fail(&format!("stub relayed output before publish: {}", &line[..line.len().min(160)]));
        }
    }
    pub(crate) fn wait_exit(&mut self, timeout: Duration) -> Option<std::process::ExitStatus> {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if let Some(status) = self.child.try_wait().unwrap() {
                return Some(status);
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        None
    }
    pub(crate) fn stderr_text(&self) -> String {
        let mut out = String::new();
        while let Ok(line) = self.stderr.try_recv() {
            out.push_str(&line);
            out.push('\n');
        }
        out
    }
}

impl Drop for StubProc {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

pub(crate) fn spawn_stub(home: &MirrorHome, server: &Server, args: &[&str]) -> StubProc {
    let mut cmd = Command::new(client_bin());
    cmd.args(args);
    stub_env(&mut cmd, home, server);
    let mut child = cmd
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|e| fail(&format!("spawn stub {args:?}: {e}")));
    let stdout = child.stdout.take().unwrap();
    let stderr = child.stderr.take().unwrap();
    let (tx, rx) = std::sync::mpsc::channel::<String>();
    let (etx, erx) = std::sync::mpsc::channel::<String>();
    std::thread::spawn(move || {
        use std::io::BufRead;
        for line in std::io::BufReader::new(stdout).lines() {
            match line {
                Ok(l) => {
                    if tx.send(l).is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });
    std::thread::spawn(move || {
        use std::io::BufRead;
        for line in std::io::BufReader::new(stderr).lines() {
            match line {
                Ok(l) => {
                    if etx.send(l).is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });
    StubProc { child, rx, stderr: erx }
}

/// Dead-or-alive guarantee: the lock must end free and the pid dead,
/// whether the writer exits gracefully or has to be forced.
pub(crate) fn shutdown_writer(sync_dir: &Path) {
    if !sync_dir.exists() {
        return;
    }
    let _ = daemon::stop_daemon(sync_dir, &daemon::DaemonConfig::default());
    let deadline = Instant::now() + Duration::from_secs(5);
    while !owner::lock_free(sync_dir) && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(50));
    }
    if !owner::lock_free(sync_dir) {
        if let Ok(pid) = std::fs::read_to_string(sync_dir.join(owner::PID_FILE)) {
            if let Ok(pid) = pid.trim().parse::<u32>() {
                sigkill(pid);
            }
        }
        let deadline = Instant::now() + Duration::from_secs(3);
        while !owner::lock_free(sync_dir) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(50));
        }
    }
    // Sweep a marker/pid a crashed `stop` left so the next boot is clean.
    let _ = std::fs::remove_file(sync_dir.join(daemon::STOP_FILE));
}

#[cfg(unix)]
pub(crate) fn sigkill(pid: u32) {
    let _ = Command::new("kill").arg("-9").arg(pid.to_string()).status();
}
#[cfg(not(unix))]
pub(crate) fn sigkill(_pid: u32) {}
