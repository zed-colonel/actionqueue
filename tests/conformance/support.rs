//! Shared helpers for the `AQ-CONT-1` conformance checks.
//!
//! Everything here is test-only. It reads the repository from the root package's
//! manifest directory and never writes outside a caller-provided scratch path.

#![allow(dead_code)]

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;

/// Returns the repository root (the acceptance-harness manifest directory).
pub fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// Reads a UTF-8 file relative to the repository root.
pub fn read_repo_text(rel: &str) -> String {
    let path = repo_root().join(rel);
    fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

/// Returns the path relative to the repository root using forward slashes.
pub fn rel_path(path: &Path) -> String {
    path.strip_prefix(repo_root())
        .unwrap_or(path)
        .components()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .collect::<Vec<_>>()
        .join("/")
}

/// Repo-relative paths (forward slashes) of every file git tracks that still exists in
/// the working tree. This is the hermetic file universe for the conformance checks:
/// ignored build output, editor swap files, local notes, and other untracked content
/// never influence a result, so a check passes or fails on the committed tree alone.
pub fn tracked_files() -> &'static [String] {
    static FILES: OnceLock<Vec<String>> = OnceLock::new();
    FILES.get_or_init(|| {
        let output = Command::new("git")
            .args(["ls-files", "-z", "--cached", "--full-name"])
            .current_dir(repo_root())
            .output()
            .expect("git must be available to enumerate tracked files");
        assert!(
            output.status.success(),
            "git ls-files failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let root = repo_root();
        let mut files: Vec<String> = output
            .stdout
            .split(|b| *b == 0)
            .filter(|entry| !entry.is_empty())
            .map(|entry| String::from_utf8(entry.to_vec()).expect("tracked path is UTF-8"))
            .filter(|rel| root.join(rel).is_file())
            .collect();
        files.sort();
        files.dedup();
        files
    })
}

/// Tracked files whose repo-relative path equals `prefix` or lies underneath it.
pub fn tracked_files_under(prefix: &str) -> Vec<String> {
    tracked_files().iter().filter(|rel| under(rel, prefix)).cloned().collect()
}

/// Returns true if `rel` equals `prefix` or lies underneath it.
pub fn under(rel: &str, prefix: &str) -> bool {
    rel == prefix || rel.starts_with(&format!("{prefix}/"))
}

/// Recursively lists files under `root`, skipping any path whose repo-relative
/// form starts with one of `excluded`.
pub fn walk_files(root: &Path, excluded: &[String]) -> Vec<PathBuf> {
    let mut out = Vec::new();
    walk_into(root, excluded, &mut out);
    out.sort();
    out
}

fn walk_into(dir: &Path, excluded: &[String], out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let rel = rel_path(&path);
        if excluded.iter().any(|ex| under(&rel, ex)) {
            continue;
        }
        if path.is_dir() {
            walk_into(&path, excluded, out);
        } else if path.is_file() {
            out.push(path);
        }
    }
}

/// Returns the file content if it is UTF-8 text without NUL bytes.
pub fn read_text_if_text(path: &Path) -> Option<String> {
    let bytes = fs::read(path).ok()?;
    if bytes.iter().take(8192).any(|b| *b == 0) {
        return None;
    }
    String::from_utf8(bytes).ok()
}

/// Reads a scalar `key: value` line from a simple YAML document.
pub fn yaml_scalar(text: &str, key: &str) -> Option<String> {
    let prefix = format!("{key}:");
    text.lines()
        .map(str::trim_end)
        .find(|line| line.starts_with(&prefix))
        .map(|line| line[prefix.len()..].trim().trim_matches('\'').trim_matches('"').to_string())
}

/// Parses `sha256sum` output (`<hex>  <path>` per line).
pub fn parse_sha256sums(text: &str) -> Vec<(String, String)> {
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            let (hash, rest) = line.split_once(' ').expect("sha256sums line has a hash");
            (hash.to_string(), rest.trim_start_matches(['*', ' ']).to_string())
        })
        .collect()
}

/// Hex-encodes bytes.
pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// SHA-256 of a file's bytes as lowercase hex.
pub fn sha256_file(path: &Path) -> String {
    let bytes = fs::read(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    hex(&sha256(&bytes))
}

/// Pure-Rust SHA-256 (FIPS 180-4). Test-only; target crates select their own
/// digest dependency under `AQ-ADR-003`.
pub fn sha256(data: &[u8]) -> [u8; 32] {
    let mut state: [u32; 8] = [
        0x6a09_e667,
        0xbb67_ae85,
        0x3c6e_f372,
        0xa54f_f53a,
        0x510e_527f,
        0x9b05_688c,
        0x1f83_d9ab,
        0x5be0_cd19,
    ];
    let mut message = data.to_vec();
    let bit_len = (data.len() as u64).wrapping_mul(8);
    message.push(0x80);
    while message.len() % 64 != 56 {
        message.push(0);
    }
    message.extend_from_slice(&bit_len.to_be_bytes());
    for block in message.chunks_exact(64) {
        compress(&mut state, block);
    }
    let mut out = [0u8; 32];
    for (chunk, word) in out.chunks_exact_mut(4).zip(state.iter()) {
        chunk.copy_from_slice(&word.to_be_bytes());
    }
    out
}

const K: [u32; 64] = [
    0x428a_2f98,
    0x7137_4491,
    0xb5c0_fbcf,
    0xe9b5_dba5,
    0x3956_c25b,
    0x59f1_11f1,
    0x923f_82a4,
    0xab1c_5ed5,
    0xd807_aa98,
    0x1283_5b01,
    0x2431_85be,
    0x550c_7dc3,
    0x72be_5d74,
    0x80de_b1fe,
    0x9bdc_06a7,
    0xc19b_f174,
    0xe49b_69c1,
    0xefbe_4786,
    0x0fc1_9dc6,
    0x240c_a1cc,
    0x2de9_2c6f,
    0x4a74_84aa,
    0x5cb0_a9dc,
    0x76f9_88da,
    0x983e_5152,
    0xa831_c66d,
    0xb003_27c8,
    0xbf59_7fc7,
    0xc6e0_0bf3,
    0xd5a7_9147,
    0x06ca_6351,
    0x1429_2967,
    0x27b7_0a85,
    0x2e1b_2138,
    0x4d2c_6dfc,
    0x5338_0d13,
    0x650a_7354,
    0x766a_0abb,
    0x81c2_c92e,
    0x9272_2c85,
    0xa2bf_e8a1,
    0xa81a_664b,
    0xc24b_8b70,
    0xc76c_51a3,
    0xd192_e819,
    0xd699_0624,
    0xf40e_3585,
    0x106a_a070,
    0x19a4_c116,
    0x1e37_6c08,
    0x2748_774c,
    0x34b0_bcb5,
    0x391c_0cb3,
    0x4ed8_aa4a,
    0x5b9c_ca4f,
    0x682e_6ff3,
    0x748f_82ee,
    0x78a5_636f,
    0x84c8_7814,
    0x8cc7_0208,
    0x90be_fffa,
    0xa450_6ceb,
    0xbef9_a3f7,
    0xc671_78f2,
];

fn compress(state: &mut [u32; 8], block: &[u8]) {
    let mut w = [0u32; 64];
    for (i, chunk) in block.chunks_exact(4).enumerate() {
        w[i] = u32::from_be_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
    }
    for i in 16..64 {
        let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
        let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
        w[i] = w[i - 16].wrapping_add(s0).wrapping_add(w[i - 7]).wrapping_add(s1);
    }
    let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut h] = *state;
    for i in 0..64 {
        let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
        let ch = (e & f) ^ (!e & g);
        let t1 = h.wrapping_add(s1).wrapping_add(ch).wrapping_add(K[i]).wrapping_add(w[i]);
        let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
        let maj = (a & b) ^ (a & c) ^ (b & c);
        let t2 = s0.wrapping_add(maj);
        h = g;
        g = f;
        f = e;
        e = d.wrapping_add(t1);
        d = c;
        c = b;
        b = a;
        a = t1.wrapping_add(t2);
    }
    for (slot, value) in state.iter_mut().zip([a, b, c, d, e, f, g, h]) {
        *slot = slot.wrapping_add(value);
    }
}

/// Creates a unique scratch directory under the system temp dir.
pub fn scratch_dir(label: &str) -> PathBuf {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after epoch")
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("aq-conformance-{label}-{unique}"));
    fs::create_dir_all(&dir).expect("scratch dir");
    dir
}
