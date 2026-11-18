use crate::fixtures::Server;
use std::collections::HashSet;

// ---- no-dial (ADR-0001) ------------------------------------------------------

pub(crate) fn socket_inodes(pid: u32) -> HashSet<String> {
    let fd_dir = format!("/proc/{pid}/fd");
    let mut inodes = HashSet::new();
    let Ok(entries) = std::fs::read_dir(fd_dir) else {
        return inodes;
    };
    for entry in entries.flatten() {
        if let Ok(link) = std::fs::read_link(entry.path()) {
            let s = link.to_string_lossy().to_string();
            if let Some(inode) = s.strip_prefix("socket:[") {
                inodes.insert(inode.trim_end_matches(']').to_string());
            }
        }
    }
    inodes
}

pub(crate) fn established_rows() -> Vec<(u16, String)> {
    let mut rows = Vec::new();
    for file in ["/proc/net/tcp", "/proc/net/tcp6"] {
        let Ok(text) = std::fs::read_to_string(file) else { continue };
        for line in text.lines().skip(1) {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() < 10 || parts[3] != "01" {
                continue;
            }
            let local_port = u16::from_str_radix(parts[1].rsplit(':').next().unwrap_or("0"), 16).unwrap_or(0);
            rows.push((local_port, parts[9].to_string()));
        }
    }
    rows
}

/// ADR-0001, enforced: the server process must own no established socket that
/// it did not accept on its listen port.
///
/// procfs-only: `/proc/{pid}/fd` and `/proc/net/tcp` do not exist on macOS.
/// There the attribution is skipped with a note (same policy as an external
/// server) — the container e2e still enforces it where it matters (ADR-0001's
/// network namespace lives on machine B, linux).
pub(crate) fn assert_no_dial_for(child_id: u32, port: u16) {
    let inodes = socket_inodes(child_id);
    if !cfg!(target_os = "linux") && inodes.is_empty() {
        eprintln!("  note: no procfs on this platform — no-dial not attributable, skipped");
        return;
    }
    assert!(!inodes.is_empty(), "server has no open fds");
    let offenders: Vec<u16> = established_rows()
        .into_iter()
        .filter(|(_, inode)| inodes.contains(inode))
        .map(|(port, _)| port)
        .filter(|own| *own != port)
        .collect();
    assert!(
        offenders.is_empty(),
        "server holds established connection(s) on {offenders:?} — an outbound dial"
    );
}

pub(crate) fn assert_no_dial(server: &Server) {
    let Some(child) = &server.child else {
        eprintln!("  note: external server — no-dial not attributable, skipped");
        return;
    };
    assert_no_dial_for(child.id(), server.port);
}
