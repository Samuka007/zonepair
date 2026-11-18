//! Clock utilities for the sync engine: a unix-ms stamp (journal filename
//! leg, quarantine suffix) and the `*At` fields' RFC3339 UTC spelling.

pub(crate) fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Wall clock as RFC3339 UTC — every `*At` field is observational only
/// (§3.3: clocks never carry correctness), so a 25-line civil-from-days
/// implementation beats a chrono dependency for this one format.
///
/// `pub` so the e2e harness (`zonepair-e2e`) mints `enqueuedAt` stamps for
/// journal records through the same implementation the writers use —
/// duplicating the format in test code would be a second definition of a
/// wire-contract field (T-10 bypass, minimal surface). Re-exported through
/// `sync::state` for its callers.
pub fn now_rfc3339() -> String {
    let secs = unix_ms() / 1000;
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    let (h, m, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    let (y, mo, d) = civil_from_days(days);
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{m:02}:{s:02}Z")
}

/// Howard Hinnant's `civil_from_days`: days since 1970-01-01 → (year, month,
/// day). Gregorian, proleptic.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097); // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    (y + i64::from(m <= 2), m, d)
}
