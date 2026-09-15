//! `AQ-CONT-1` frozen-evidence integrity checks.
//!
//! Verifies that the normative contract documents, the planning package, the
//! pre-`AQ-CONT-1` archive, the conformance manifest, the developmental acceptance
//! matrix, and the ADR queue are present and match their pinned hashes. Prints the
//! pinned revisions so CI logs state exactly which contract revision the tree implements.

#[path = "harness/package.rs"]
mod package;
mod support;

use std::collections::BTreeSet;

use support::{
    parse_sha256sums, read_repo_text, repo_root, sha256, sha256_file, tracked_files,
    tracked_files_under, yaml_scalar,
};

const MANIFEST_PATH: &str = "conformance/aq-cont-1/manifest.yaml";
const MATRIX_PATH: &str =
    "conformance/aq-cont-1/aq-cont-1-developmental-campaign-acceptance-matrix.yaml";
const POLICY_PATH: &str = "conformance/aq-cont-1/contract-boundaries.json";
const BASELINE_COMMIT: &str = "97c9dc26c19c697dbfb204ed503e82c5f053394f";

fn verify_sha256sums(dir: &str) {
    let base = repo_root().join(dir);
    let listing = read_repo_text(&format!("{dir}/SHA256SUMS"));
    let entries = parse_sha256sums(&listing);
    assert!(!entries.is_empty(), "{dir}/SHA256SUMS is empty");
    for (expected, rel) in entries {
        let path = base.join(&rel);
        assert!(path.is_file(), "{dir}/SHA256SUMS lists missing file {rel}");
        let actual = sha256_file(&path);
        assert_eq!(actual, expected, "{dir}/{rel} does not match its pinned hash");
    }
}

#[test]
fn sha256_known_answers() {
    assert_eq!(
        support::hex(&sha256(b"")),
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    );
    assert_eq!(
        support::hex(&sha256(b"abc")),
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
    assert_eq!(
        support::hex(&sha256(b"The quick brown fox jumps over the lazy dog")),
        "d7a8fbb307d7809469ca9abcb0082e4f8d5651e46d3cdb762d02d0bf37c9e592"
    );
    let long = vec![b'a'; 1_000_000];
    assert_eq!(
        support::hex(&sha256(&long)),
        "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0"
    );
}

#[test]
fn contract_documents_match_pinned_hashes() {
    verify_sha256sums("docs/contracts");
}

#[test]
fn planning_package_matches_pinned_hashes() {
    verify_sha256sums("docs/planning/aq-cont-1");
}

/// The plan bundle ships its own validation manifest for the two plan documents. It must
/// agree with the files as committed and with the package's SHA256SUMS, so the committed
/// plan is provably the one the work breakdown was cut from.
#[test]
fn planning_validation_manifest_matches_committed_plan() {
    const DIR: &str = "docs/planning/aq-cont-1";
    let validation =
        parse_sha256sums(&read_repo_text(&format!("{DIR}/aq-cont-1-implementation-plan.sha256")));
    let pinned: std::collections::BTreeMap<String, String> =
        parse_sha256sums(&read_repo_text(&format!("{DIR}/SHA256SUMS")))
            .into_iter()
            .map(|(hash, rel)| (rel, hash))
            .collect();
    let names: BTreeSet<&str> = validation.iter().map(|(_, rel)| rel.as_str()).collect();
    assert_eq!(
        names,
        ["aq-cont-1-implementation-plan.md", "aq-cont-1-work-breakdown.yaml"].into_iter().collect()
    );
    for (expected, rel) in &validation {
        assert_eq!(
            &sha256_file(&repo_root().join(DIR).join(rel)),
            expected,
            "{rel} differs from the plan bundle's validation manifest"
        );
        assert_eq!(pinned.get(rel), Some(expected), "{rel}: SHA256SUMS disagrees with .sha256");
    }
}

/// Every tracked file in the archive except the checksum list itself is hash-pinned, so a
/// catalogue or README cannot drift without an explicit re-pin.
#[test]
fn archive_matches_pinned_hashes_and_pins_every_tracked_file() {
    const DIR: &str = "archive/pre-aq-cont-1";
    verify_sha256sums(DIR);
    let listed: BTreeSet<String> = parse_sha256sums(&read_repo_text(&format!("{DIR}/SHA256SUMS")))
        .into_iter()
        .map(|(_, rel)| rel)
        .collect();
    let tracked: BTreeSet<String> = tracked_files_under(DIR)
        .into_iter()
        .map(|rel| rel[DIR.len() + 1..].to_string())
        .filter(|rel| rel != "SHA256SUMS")
        .collect();
    assert!(!tracked.is_empty(), "archive has no tracked files");
    let unpinned: Vec<&String> = tracked.difference(&listed).collect();
    assert!(unpinned.is_empty(), "archive files not pinned in SHA256SUMS: {unpinned:?}");
    for catalogue in [
        "known-failure-cases/README.md",
        "crash-scenarios/catalogue.json",
        "crash-scenarios/README.md",
        "performance-baseline/README.md",
        "README.md",
        "tools/capture_fixtures.rs",
    ] {
        assert!(listed.contains(catalogue), "{catalogue} must be pinned");
    }
}

#[test]
fn manifest_pins_contract_baseline_and_normative_documents() {
    let manifest = read_repo_text(MANIFEST_PATH);
    assert_eq!(yaml_scalar(&manifest, "contract").as_deref(), Some("AQ-CONT-1"));
    assert_eq!(
        yaml_scalar(&manifest, "contract_revision").as_deref(),
        Some("STACK-2026-07-20-CLEAN-1")
    );
    assert_eq!(
        yaml_scalar(&manifest, "developmental_profile_revision").as_deref(),
        Some("STACK-DEVELOPMENTAL-DIAGNOSTICS-1")
    );
    assert_eq!(yaml_scalar(&manifest, "baseline_commit").as_deref(), Some(BASELINE_COMMIT));
    assert_eq!(
        yaml_scalar(&manifest, "baseline_tag").as_deref(),
        Some("actionqueue/pre-aq-cont-1")
    );

    package::validate(&repo_root().join("conformance/aq-cont-1")).unwrap();
}

#[test]
fn developmental_matrix_has_eighteen_cases_in_five_groups() {
    let matrix = read_repo_text(MATRIX_PATH);
    assert_eq!(yaml_scalar(&matrix, "case_count").as_deref(), Some("18"));
    assert_eq!(yaml_scalar(&matrix, "base_contract").as_deref(), Some("AQ-CONT-1"));
    assert_eq!(
        yaml_scalar(&matrix, "profile_revision").as_deref(),
        Some("STACK-DEVELOPMENTAL-DIAGNOSTICS-1")
    );
    let ids: Vec<&str> = matrix
        .lines()
        .map(str::trim)
        .filter_map(|line| line.strip_prefix("- id:"))
        .map(str::trim)
        .collect();
    let expected: Vec<String> = (1..=18).map(|n| format!("AQ-DD-{n:03}")).collect();
    assert_eq!(ids, expected, "matrix case IDs must be AQ-DD-001..018 in order");
    let groups: BTreeSet<&str> = matrix
        .lines()
        .map(str::trim)
        .filter_map(|line| line.strip_prefix("group:"))
        .map(str::trim)
        .collect();
    let expected_groups: BTreeSet<&str> = [
        "attribution_neutrality",
        "durable_execution",
        "recovery_and_idempotency",
        "semantic_non_ownership",
        "privacy_and_observability",
    ]
    .into_iter()
    .collect();
    assert_eq!(groups, expected_groups);
    for id in &expected {
        let case = matrix.split("- id: ").find(|c| c.starts_with(id.as_str())).expect("case block");
        assert!(case.contains("setup:") && case.contains("expected:"), "{id} lacks setup/expected");
    }
}

/// The ADR queue is read from the tracked tree, like every other check, so an untracked
/// draft cannot satisfy or break it.
#[test]
fn adr_queue_is_complete() {
    let tracked: Vec<String> = tracked_files_under("docs/adrs")
        .into_iter()
        .map(|rel| rel["docs/adrs/".len()..].to_string())
        .collect();
    let index = read_repo_text("docs/adrs/README.md");
    for n in 1..=18 {
        let prefix = format!("AQ-ADR-{n:03}-");
        let found =
            tracked.iter().find(|name| name.starts_with(&prefix) && name.ends_with(".md")).cloned();
        let name = found.unwrap_or_else(|| panic!("missing ADR file for {prefix}*"));
        assert!(index.contains(&name), "docs/adrs/README.md does not link {name}");
        let body = read_repo_text(&format!("docs/adrs/{name}"));
        for heading in
            ["## Context", "## Recommended decision", "## Consequences", "## Verification required"]
        {
            assert!(body.contains(heading), "{name} lacks `{heading}`");
        }
        assert!(body.contains("**Decide before:**"), "{name} lacks a decide-before gate");
    }
}

#[test]
fn contract_index_and_profile_are_committed() {
    for rel in [
        "docs/contracts/AQ-CONT-1.md",
        "docs/contracts/actionqueue-hardening-implementation-ready.md",
        "docs/contracts/aq-cont-1-developmental-campaign-execution-profile.md",
        "archive/pre-aq-cont-1/README.md",
        "conformance/aq-cont-1/README.md",
        POLICY_PATH,
    ] {
        assert!(tracked_files().contains(&rel.to_string()), "{rel} must be tracked");
    }
    let architecture =
        read_repo_text("docs/contracts/actionqueue-hardening-implementation-ready.md");
    assert!(architecture.contains("### AQ-H19 — Developmental correlation is neutral attribution"));
    assert!(architecture.contains("### AQ-H20 — Execution outcome is not performance judgment"));
    let profile =
        read_repo_text("docs/contracts/aq-cont-1-developmental-campaign-execution-profile.md");
    assert!(profile.contains("No developmental primitive is added"));
    assert!(profile.contains("Retries are not samples"));
}

#[test]
fn revision_report() {
    let manifest = read_repo_text(MANIFEST_PATH);
    let policy: serde_json::Value =
        serde_json::from_str(&read_repo_text(POLICY_PATH)).expect("policy parses");
    let get = |key: &str| yaml_scalar(&manifest, key).unwrap_or_else(|| panic!("manifest `{key}`"));
    assert_eq!(policy["contract_revision"].as_str(), Some(get("contract_revision").as_str()));
    assert_eq!(
        policy["developmental_profile_revision"].as_str(),
        Some(get("developmental_profile_revision").as_str())
    );
    println!(
        "AQ-CONT-1 revision report: contract={} contract_revision={} planning_profile={} \
         developmental_profile_revision={} package_revision={} baseline_commit={} baseline_tag={}",
        get("contract"),
        get("contract_revision"),
        get("planning_profile"),
        get("developmental_profile_revision"),
        get("package_revision"),
        get("baseline_commit"),
        get("baseline_tag"),
    );
}
