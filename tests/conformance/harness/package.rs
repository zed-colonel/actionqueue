//! Structured, read-only validation of the versioned conformance inventory.
#![allow(dead_code)]
use std::{
    collections::BTreeSet,
    fs,
    path::{Component, Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Asset {
    pub path: String,
    pub sha256: String,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Fixture {
    pub id: String,
    pub driver: String,
    pub path: String,
    pub sha256: String,
    pub variants: Vec<String>,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Manifest {
    pub schema_version: u32,
    pub package_revision: u32,
    pub status: String,
    pub contract_revision: String,
    pub developmental_profile_revision: String,
    pub minimum_feature_set: Vec<String>,
    pub full_feature_set: Vec<String>,
    pub drivers: Vec<String>,
    pub normative_documents: Vec<Asset>,
    pub acceptance_matrices: Vec<Asset>,
    pub fixtures: Vec<Fixture>,
    pub assets: Vec<Asset>,
    pub coverage: String,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Coverage {
    pub schema_version: u32,
    pub cases: Vec<Case>,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Case {
    pub id: String,
    pub fixtures: Vec<String>,
    pub drivers: Vec<String>,
    pub required_features: Vec<String>,
    pub variants: Vec<String>,
    pub assertions: Vec<String>,
    pub supplemental_tests: Vec<String>,
}
pub fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
pub fn safe_path(root: &Path, relative: &str) -> Result<PathBuf, String> {
    if relative.is_empty()
        || relative.contains('\\')
        || Path::new(relative).components().any(|c| !matches!(c, Component::Normal(_)))
    {
        return Err(format!("unsafe package path: {relative}"));
    }
    let p = root.join(relative);
    let canonical = p.canonicalize().map_err(|e| format!("{relative}: {e}"))?;
    if !canonical.starts_with(root.canonicalize().map_err(|e| e.to_string())?)
        || !canonical.is_file()
    {
        return Err(format!("escaping or non-file asset: {relative}"));
    }
    Ok(p)
}
fn check_asset(root: &Path, a: &Asset) -> Result<(), String> {
    if a.sha256.len() != 64
        || !a.sha256.bytes().all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    {
        return Err(format!("invalid SHA-256: {}", a.path));
    }
    let actual = hash(&fs::read(safe_path(root, &a.path)?).map_err(|e| e.to_string())?);
    if actual != a.sha256 {
        return Err(format!("immutable fixture changed: {}", a.path));
    }
    Ok(())
}
pub fn validate(root: &Path) -> Result<(Manifest, Coverage), String> {
    // JSON is a strict subset of YAML. No line-oriented YAML interpretation.
    let m: Manifest =
        serde_json::from_slice(&fs::read(root.join("manifest.yaml")).map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())?;
    if m.schema_version != 1 || m.package_revision < 9 || !m.minimum_feature_set.is_empty() {
        return Err("unsupported package schema/revision/base features".into());
    }
    let known: BTreeSet<_> = ["embedded", "daemon", "cli", "adapter"].into_iter().collect();
    let drivers: BTreeSet<_> = m.drivers.iter().map(String::as_str).collect();
    if drivers.len() != m.drivers.len() || !drivers.is_subset(&known) {
        return Err("duplicate or unknown driver".into());
    }
    let mut ids = BTreeSet::new();
    let mut paths = BTreeSet::new();
    for f in &m.fixtures {
        if !ids.insert(f.id.clone()) || !f.id.starts_with("AQ-CF-") {
            return Err(format!("duplicate or invalid fixture ID: {}", f.id));
        }
        if !drivers.contains(f.driver.as_str()) {
            return Err(format!("unregistered driver: {}", f.driver));
        }
        check_asset(root, &Asset { path: f.path.clone(), sha256: f.sha256.clone() })?;
        if !paths.insert(f.path.clone()) {
            return Err(format!("duplicate asset: {}", f.path));
        }
    }
    for a in m.assets.iter().chain(m.acceptance_matrices.iter()) {
        check_asset(root, a)?;
        if !paths.insert(a.path.clone()) {
            return Err(format!("duplicate asset: {}", a.path));
        }
    }
    for f in &m.fixtures {
        let variants: BTreeSet<_> = f.variants.iter().map(String::as_str).collect();
        if variants.len() != f.variants.len()
            || variants.iter().any(|v| !["ordinary", "replay", "crash", "race"].contains(v))
        {
            return Err(format!("invalid fixture variants: {}", f.id));
        }
        if f.path.starts_with("fixtures/") {
            let scenario: serde_json::Value =
                serde_json::from_slice(&fs::read(root.join(&f.path)).map_err(|e| e.to_string())?)
                    .map_err(|e| e.to_string())?;
            if scenario["schema_version"] != 1 || scenario["id"] != f.id {
                return Err(format!("scenario identity/schema mismatch: {}", f.id));
            }
            let expected = scenario["expected_observations"]
                .as_str()
                .ok_or("missing expected observations")?;
            if !paths.contains(expected) {
                return Err(format!("unpinned expectations: {expected}"));
            }
            let steps = scenario["steps"].as_array().ok_or("missing steps")?;
            if steps.is_empty() {
                return Err("empty scenario".into());
            }
            let cuts = scenario["recovery_cuts"].as_array().ok_or("missing cuts")?;
            let mut seen = BTreeSet::new();
            for cut in cuts {
                let n = cut.as_u64().ok_or("invalid cut")?;
                if n == 0 || n > steps.len() as u64 || !seen.insert(n) {
                    return Err("invalid/duplicate cut".into());
                }
            }
        }
    }
    let repo = root.parent().and_then(Path::parent).ok_or("package has no repository parent")?;
    for a in &m.normative_documents {
        check_asset(repo, a)?;
    }
    if !paths.contains(&m.coverage) {
        return Err("coverage must be hash-pinned".into());
    }
    let coverage: Coverage = serde_json::from_slice(
        &fs::read(safe_path(root, &m.coverage)?).map_err(|e| e.to_string())?,
    )
    .map_err(|e| e.to_string())?;
    if coverage.schema_version != 1 {
        return Err("unsupported coverage schema".into());
    }
    let expected: BTreeSet<_> = (1..=20)
        .map(|i| format!("AQ-H{i}"))
        .chain((1..=18).map(|i| format!("AQ-DD-{i:03}")))
        .collect();
    let mut cases = BTreeSet::new();
    for c in &coverage.cases {
        if !cases.insert(c.id.clone()) {
            return Err(format!("duplicate coverage ID: {}", c.id));
        }
        if c.assertions.is_empty() || c.variants.is_empty() {
            return Err(format!("empty requirement: {}", c.id));
        }
        if c.fixtures.iter().any(|f| !ids.contains(f))
            || c.drivers.iter().any(|d| !drivers.contains(d.as_str()))
        {
            return Err(format!("unresolved coverage: {}", c.id));
        }
        if c.variants.iter().any(|v| !["ordinary", "replay", "crash", "race"].contains(&v.as_str()))
        {
            return Err("unknown variant".into());
        }
        if c.required_features.iter().any(|f| !m.full_feature_set.contains(f)) {
            return Err("unknown feature".into());
        }
        for test in &c.supplemental_tests {
            safe_path(repo, test)?;
        }
        if m.status == "executable" && (c.fixtures.is_empty() || c.drivers.is_empty()) {
            return Err(format!("unimplemented executable case: {}", c.id));
        }
    }
    if cases != expected {
        return Err("coverage must enumerate AQ-H1..20 and AQ-DD-001..018 exactly".into());
    }
    fn inventory(root: &Path, dir: &Path, paths: &BTreeSet<String>) -> Result<(), String> {
        for entry in fs::read_dir(dir).map_err(|e| e.to_string())? {
            let p = entry.map_err(|e| e.to_string())?.path();
            if p.is_dir() {
                inventory(root, &p, paths)?;
            } else {
                let rel = p.strip_prefix(root).unwrap().to_str().ok_or("non-UTF8 asset")?;
                let asset = ["json", "yaml", "bin", "wal"]
                    .contains(&p.extension().and_then(|e| e.to_str()).unwrap_or(""));
                if asset
                    && !["manifest.yaml", "contract-boundaries.json"].contains(&rel)
                    && !paths.contains(rel)
                {
                    return Err(format!("unregistered fixture asset: {rel}"));
                }
            }
        }
        Ok(())
    }
    inventory(root, root, &paths)?;
    Ok((m, coverage))
}
pub fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("conformance/aq-cont-1")
}
