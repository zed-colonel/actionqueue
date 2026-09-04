//! `AQ-CONT-1` repository boundary checks.
//!
//! Driven by `conformance/aq-cont-1/contract-boundaries.json`. The checks enforce:
//!
//! - no downstream domain ownership in target crate code (`AQ-H1`, `AQ-H17`, `AQ-H18`);
//! - no queue-owned developmental ontology (`AQ-H19`, `AQ-H20`);
//! - no free-form metadata backchannel (`AQ-H10`);
//! - no scheduler, budget, or authority branching on attribution (`AQ-H11`, `AQ-H19`);
//! - no developmental or high-cardinality identifiers as metric labels;
//! - no target-crate reference to the frozen archive;
//! - staged removal of forbidden legacy symbols.
//!
//! Rust sources are scanned with comments stripped so explanatory prose is exempt while
//! identifiers and string literals are not.

mod support;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde_json::Value;
use support::{read_text_if_text, rel_path, repo_root, under, walk_files};

const POLICY_PATH: &str = "conformance/aq-cont-1/contract-boundaries.json";
const MANIFEST_PATH: &str = "conformance/aq-cont-1/manifest.yaml";

/// One rule violation with enough context to locate it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Violation {
    rule: &'static str,
    path: String,
    line: usize,
    detail: String,
}

impl std::fmt::Display for Violation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}:{}: {}", self.rule, self.path, self.line, self.detail)
    }
}

fn load_policy() -> Value {
    let text = support::read_repo_text(POLICY_PATH);
    serde_json::from_str(&text).expect("contract-boundaries.json parses")
}

fn strings(value: &Value, key: &str) -> Vec<String> {
    value[key]
        .as_array()
        .unwrap_or_else(|| panic!("policy key `{key}` is an array"))
        .iter()
        .map(|v| v.as_str().expect("policy strings").to_string())
        .collect()
}

fn allowed(rule: &Value, rel: &str) -> bool {
    rule["allow"]
        .as_array()
        .map(|entries| {
            entries.iter().any(|entry| {
                let prefix = entry["path"].as_str().unwrap_or("");
                entry["reason"].as_str().is_some_and(|r| !r.is_empty()) && under(rel, prefix)
            })
        })
        .unwrap_or(false)
}

fn is_ident_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

/// True when `needle` occurs in `haystack` bounded by non-identifier characters.
fn contains_identifier(haystack: &str, needle: &str) -> bool {
    let mut start = 0;
    while let Some(pos) = haystack[start..].find(needle) {
        let begin = start + pos;
        let end = begin + needle.len();
        let before_ok = begin == 0 || !haystack[..begin].ends_with(is_ident_char);
        let after_ok = end == haystack.len() || !haystack[end..].starts_with(is_ident_char);
        if before_ok && after_ok {
            return true;
        }
        start = begin + 1;
    }
    false
}

/// Splits a line into identifier tokens.
fn identifier_tokens(line: &str) -> Vec<&str> {
    line.split(|c: char| !is_ident_char(c)).filter(|t| !t.is_empty()).collect()
}

/// Extracts the contents of ordinary `"..."` string literals on a line.
fn string_literals(line: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current: Option<String> = None;
    let mut escaped = false;
    for c in line.chars() {
        match (&mut current, c) {
            (Some(buf), '\\') if !escaped => {
                escaped = true;
                buf.push(c);
            }
            (Some(buf), '"') if !escaped => {
                out.push(std::mem::take(buf));
                current = None;
            }
            (Some(buf), _) => {
                escaped = false;
                buf.push(c);
            }
            (None, '"') => current = Some(String::new()),
            (None, _) => {}
        }
    }
    out
}

/// Removes `//` line comments and `/* */` block comments while preserving line
/// numbers and string literal contents.
fn strip_rust_comments(source: &str) -> String {
    let chars: Vec<char> = source.chars().collect();
    let mut out = String::with_capacity(source.len());
    let mut i = 0;
    let mut in_block = false;
    let mut in_string = false;
    while i < chars.len() {
        let c = chars[i];
        let next = chars.get(i + 1).copied();
        if in_block {
            if c == '*' && next == Some('/') {
                in_block = false;
                i += 2;
                continue;
            }
            if c == '\n' {
                out.push('\n');
            }
        } else if in_string {
            out.push(c);
            if c == '\\' {
                if let Some(n) = next {
                    out.push(n);
                    i += 1;
                }
            } else if c == '"' {
                in_string = false;
            }
        } else if c == '/' && next == Some('/') {
            while i < chars.len() && chars[i] != '\n' {
                i += 1;
            }
            continue;
        } else if c == '/' && next == Some('*') {
            in_block = true;
            i += 2;
            continue;
        } else if c == '\'' && chars.get(i + 2) == Some(&'\'') {
            out.extend(&chars[i..i + 3]);
            i += 3;
            continue;
        } else {
            if c == '"' {
                in_string = true;
            }
            out.push(c);
        }
        i += 1;
    }
    out
}

/// A Rust source file with comments stripped.
struct RustSource {
    rel: String,
    lines: Vec<String>,
}

fn rust_sources(policy: &Value, roots: &[String]) -> Vec<RustSource> {
    let excluded = strings(policy, "always_excluded");
    let mut out = Vec::new();
    for root in roots {
        for path in walk_files(&repo_root().join(root), &excluded) {
            if path.extension().and_then(|e| e.to_str()) != Some("rs") {
                continue;
            }
            let Some(text) = read_text_if_text(&path) else {
                continue;
            };
            let stripped = strip_rust_comments(&text);
            out.push(RustSource {
                rel: rel_path(&path),
                lines: stripped.lines().map(str::to_string).collect(),
            });
        }
    }
    out
}

fn target_sources(policy: &Value) -> Vec<RustSource> {
    rust_sources(policy, &strings(policy, "target_code_roots"))
}

fn check_domain_ownership(policy: &Value) -> Vec<Violation> {
    let rule = &policy["domain_ownership"];
    let identifiers = strings(rule, "identifiers");
    let mut violations = Vec::new();
    for source in target_sources(policy) {
        if allowed(rule, &source.rel) {
            continue;
        }
        for (index, line) in source.lines.iter().enumerate() {
            for token in identifier_tokens(line) {
                if identifiers.iter().any(|id| id == token) {
                    violations.push(Violation {
                        rule: "domain_ownership",
                        path: source.rel.clone(),
                        line: index + 1,
                        detail: format!("downstream domain identifier `{token}`"),
                    });
                }
            }
        }
    }
    violations
}

fn check_developmental_ontology(policy: &Value) -> Vec<Violation> {
    let rule = &policy["developmental_ontology"];
    let fragments = strings(rule, "identifier_fragments");
    let mut violations = Vec::new();
    for source in target_sources(policy) {
        if allowed(rule, &source.rel) {
            continue;
        }
        for (index, line) in source.lines.iter().enumerate() {
            for token in identifier_tokens(line) {
                let lower = token.to_ascii_lowercase();
                if let Some(fragment) = fragments.iter().find(|f| lower.contains(f.as_str())) {
                    violations.push(Violation {
                        rule: "developmental_ontology",
                        path: source.rel.clone(),
                        line: index + 1,
                        detail: format!(
                            "token `{token}` contains developmental fragment `{fragment}`"
                        ),
                    });
                }
            }
        }
    }
    violations
}

fn normalize_type_spacing(line: &str) -> String {
    line.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn is_public_declaration(line: &str) -> bool {
    let trimmed = line.trim_start();
    (trimmed.starts_with("pub ") || trimmed.starts_with("pub(")) && trimmed.contains(':')
}

fn check_free_form_metadata(policy: &Value) -> Vec<Violation> {
    let rule = &policy["free_form_metadata"];
    let type_fragments: Vec<String> = strings(rule, "pub_field_type_fragments")
        .iter()
        .map(|f| normalize_type_spacing(f))
        .collect();
    let identifiers = strings(rule, "identifiers");
    let mut violations = Vec::new();
    for source in target_sources(policy) {
        if allowed(rule, &source.rel) {
            continue;
        }
        for (index, line) in source.lines.iter().enumerate() {
            if is_public_declaration(line) {
                let normalized = normalize_type_spacing(line);
                if let Some(fragment) =
                    type_fragments.iter().find(|f| normalized.contains(f.as_str()))
                {
                    violations.push(Violation {
                        rule: "free_form_metadata",
                        path: source.rel.clone(),
                        line: index + 1,
                        detail: format!("public declaration carries free-form type `{fragment}`"),
                    });
                }
            }
            for token in identifier_tokens(line) {
                if identifiers.iter().any(|id| id == token) {
                    violations.push(Violation {
                        rule: "free_form_metadata",
                        path: source.rel.clone(),
                        line: index + 1,
                        detail: format!("metadata channel identifier `{token}`"),
                    });
                }
            }
        }
    }
    violations
}

fn check_scheduler_backchannel(policy: &Value) -> Vec<Violation> {
    let rule = &policy["scheduler_backchannel"];
    let identifiers = strings(rule, "identifiers");
    let mut violations = Vec::new();
    for source in rust_sources(policy, &strings(rule, "scope_paths")) {
        if allowed(rule, &source.rel) {
            continue;
        }
        for (index, line) in source.lines.iter().enumerate() {
            for token in identifier_tokens(line) {
                if identifiers.iter().any(|id| id == token) {
                    violations.push(Violation {
                        rule: "scheduler_backchannel",
                        path: source.rel.clone(),
                        line: index + 1,
                        detail: format!("scheduling/authority code reads attribution `{token}`"),
                    });
                }
            }
        }
    }
    violations
}

fn check_metric_labels(policy: &Value) -> Vec<Violation> {
    let rule = &policy["metric_labels"];
    let forbidden = strings(rule, "forbidden_label_tokens");
    let mut violations = Vec::new();
    for source in rust_sources(policy, &strings(rule, "scope_paths")) {
        if allowed(rule, &source.rel) {
            continue;
        }
        for (index, line) in source.lines.iter().enumerate() {
            for literal in string_literals(line) {
                for token in identifier_tokens(&literal) {
                    if forbidden.iter().any(|f| f == token) {
                        violations.push(Violation {
                            rule: "metric_labels",
                            path: source.rel.clone(),
                            line: index + 1,
                            detail: format!(
                                "string literal `{literal}` names forbidden label `{token}`"
                            ),
                        });
                    }
                }
            }
        }
    }
    violations
}

fn check_archive_isolation(policy: &Value) -> Vec<Violation> {
    let rule = &policy["archive_isolation"];
    let substrings = strings(rule, "substrings");
    let excluded = strings(policy, "always_excluded");
    let mut violations = Vec::new();
    for root in strings(policy, "target_code_roots") {
        for path in walk_files(&repo_root().join(root), &excluded) {
            let rel = rel_path(&path);
            let Some(text) = read_text_if_text(&path) else {
                continue;
            };
            if allowed(rule, &rel) {
                continue;
            }
            for (index, line) in text.lines().enumerate() {
                if let Some(needle) = substrings.iter().find(|s| line.contains(s.as_str())) {
                    violations.push(Violation {
                        rule: "archive_isolation",
                        path: rel.clone(),
                        line: index + 1,
                        detail: format!(
                            "target crate references the frozen archive via `{needle}`"
                        ),
                    });
                }
            }
        }
    }
    violations
}

/// Files outside the frozen roots that are subject to the legacy-symbol scan.
fn legacy_scan_files(policy: &Value) -> Vec<PathBuf> {
    let mut excluded = strings(policy, "always_excluded");
    excluded.extend(strings(policy, "frozen_roots"));
    walk_files(&repo_root(), &excluded)
}

/// Counts, per legacy symbol, the files that still contain it.
fn legacy_symbol_files(policy: &Value) -> BTreeMap<String, Vec<String>> {
    let symbols: Vec<String> = policy["legacy_symbols"]["symbols"]
        .as_array()
        .expect("legacy symbols array")
        .iter()
        .map(|s| s["symbol"].as_str().expect("symbol").to_string())
        .collect();
    let mut found: BTreeMap<String, Vec<String>> =
        symbols.iter().map(|s| (s.clone(), Vec::new())).collect();
    for path in legacy_scan_files(policy) {
        let Some(text) = read_text_if_text(&path) else {
            continue;
        };
        for symbol in &symbols {
            if contains_identifier(&text, symbol) {
                found.get_mut(symbol).expect("symbol entry").push(rel_path(&path));
            }
        }
    }
    found
}

/// Applies the staged policy to observed legacy-symbol file lists.
fn evaluate_legacy_stages(policy: &Value, found: &BTreeMap<String, Vec<String>>) -> Vec<String> {
    let mut failures = Vec::new();
    for entry in policy["legacy_symbols"]["symbols"].as_array().expect("symbols") {
        let symbol = entry["symbol"].as_str().expect("symbol");
        let stage = entry["stage"].as_str().expect("stage");
        let files = &found[symbol];
        match stage {
            "forbid" if !files.is_empty() => failures.push(format!(
                "`{symbol}` was removed by {} but still appears in {} file(s): {}",
                entry["removal_pr"].as_str().unwrap_or("?"),
                files.len(),
                files.join(", ")
            )),
            "report" => {
                let max = entry["max_files"].as_u64().expect("max_files") as usize;
                if files.len() > max {
                    failures.push(format!(
                        "`{symbol}` footprint grew to {} file(s) (policy max {max}); remove the \
                         new use or bump max_files consciously: {}",
                        files.len(),
                        files.join(", ")
                    ));
                }
            }
            "forbid" => {}
            other => failures.push(format!("`{symbol}` has unknown stage `{other}`")),
        }
    }
    failures
}

fn assert_no_violations(rule: &str, violations: &[Violation]) {
    if !violations.is_empty() {
        let listing = violations.iter().map(ToString::to_string).collect::<Vec<_>>().join("\n");
        panic!("{rule}: {} violation(s)\n{listing}", violations.len());
    }
}

#[test]
fn policy_matches_conformance_manifest_revisions() {
    let policy = load_policy();
    let manifest = support::read_repo_text(MANIFEST_PATH);
    for key in
        ["contract", "contract_revision", "planning_profile", "developmental_profile_revision"]
    {
        let in_policy = policy[key].as_str().unwrap_or_else(|| panic!("policy `{key}`"));
        let in_manifest =
            support::yaml_scalar(&manifest, key).unwrap_or_else(|| panic!("manifest `{key}`"));
        assert_eq!(in_policy, in_manifest, "policy and manifest disagree on `{key}`");
    }
    println!(
        "AQ-CONT-1 boundary policy: contract_revision={} developmental_profile_revision={}",
        policy["contract_revision"].as_str().unwrap_or("?"),
        policy["developmental_profile_revision"].as_str().unwrap_or("?")
    );
}

#[test]
fn target_code_contains_no_downstream_domain_ownership() {
    let policy = load_policy();
    assert_no_violations("domain_ownership", &check_domain_ownership(&policy));
}

#[test]
fn target_code_contains_no_developmental_ontology() {
    let policy = load_policy();
    assert_no_violations("developmental_ontology", &check_developmental_ontology(&policy));
}

#[test]
fn target_code_has_no_free_form_metadata_channel() {
    let policy = load_policy();
    assert_no_violations("free_form_metadata", &check_free_form_metadata(&policy));
}

#[test]
fn scheduling_budget_and_authority_code_do_not_read_attribution() {
    let policy = load_policy();
    assert_no_violations("scheduler_backchannel", &check_scheduler_backchannel(&policy));
}

#[test]
fn metric_labels_exclude_developmental_and_high_cardinality_identifiers() {
    let policy = load_policy();
    assert_no_violations("metric_labels", &check_metric_labels(&policy));
}

#[test]
fn target_crates_do_not_reference_the_archive() {
    let policy = load_policy();
    assert_no_violations("archive_isolation", &check_archive_isolation(&policy));
}

#[test]
fn legacy_symbols_respect_their_removal_stage() {
    let policy = load_policy();
    let found = legacy_symbol_files(&policy);
    for entry in policy["legacy_symbols"]["symbols"].as_array().expect("symbols") {
        let symbol = entry["symbol"].as_str().expect("symbol");
        println!(
            "legacy symbol `{symbol}`: stage={} removal_pr={} files={}",
            entry["stage"].as_str().unwrap_or("?"),
            entry["removal_pr"].as_str().unwrap_or("?"),
            found[symbol].len()
        );
    }
    let failures = evaluate_legacy_stages(&policy, &found);
    assert!(failures.is_empty(), "legacy symbol policy failed:\n{}", failures.join("\n"));
}

#[test]
fn scanned_target_roots_are_nonempty() {
    let policy = load_policy();
    let sources = target_sources(&policy);
    assert!(
        sources.len() > 100,
        "expected the eleven crates to yield many sources, got {}",
        sources.len()
    );
    assert!(!legacy_scan_files(&policy).is_empty());
    assert!(!Path::new(&repo_root().join(POLICY_PATH)).as_os_str().is_empty());
}

#[cfg(test)]
mod unit {
    use super::*;

    #[test]
    fn identifier_matching_respects_boundaries() {
        assert!(contains_identifier("let x = FooBar::new();", "FooBar"));
        assert!(contains_identifier("use a::FooBar;", "FooBar"));
        assert!(!contains_identifier("let x = FooBarBaz::new();", "FooBar"));
        assert!(!contains_identifier("let x = MyFooBar;", "FooBar"));
        assert!(contains_identifier("Filter::Custom(x)", "Filter::Custom"));
        assert!(!contains_identifier("Filter::Customary(x)", "Filter::Custom"));
    }

    #[test]
    fn comment_stripping_keeps_code_and_strings() {
        let src = "//! Vesselish doc\nfn a() { /* Vesselish */ let s = \"keep // this\"; } // \
                   Vesselish\n";
        let stripped = strip_rust_comments(src);
        assert_eq!(stripped.lines().count(), 2, "line numbers must be preserved");
        assert!(!stripped.contains("Vesselish"));
        assert!(stripped.contains("\"keep // this\""));
    }

    #[test]
    fn comment_stripping_handles_char_literals() {
        let src = "let q = '\"'; let s = \"x\"; // tail\n";
        let stripped = strip_rust_comments(src);
        assert_eq!(stripped.trim_end(), "let q = '\"'; let s = \"x\";");
    }

    #[test]
    fn string_literal_extraction() {
        assert_eq!(string_literals("a(\"one\", \"two\\\"x\")"), vec!["one", "two\\\"x"]);
        assert!(string_literals("no strings here").is_empty());
    }

    #[test]
    fn public_free_form_map_declarations_are_detected() {
        let fragment = normalize_type_spacing("HashMap<String, String>");
        let bad = normalize_type_spacing("    pub extra: HashMap<String,   String>,");
        assert!(is_public_declaration(&bad) && bad.contains(&fragment));
        let private = "    extra: HashMap<String, String>,";
        assert!(!is_public_declaration(private));
        let typed = "    pub tags: Vec<String>,";
        assert!(is_public_declaration(typed) && !normalize_type_spacing(typed).contains(&fragment));
    }

    #[test]
    fn developmental_fragments_match_case_insensitively() {
        let lower = "SomeCampaignRef".to_ascii_lowercase();
        assert!(lower.contains("campaign"));
        let tokens = identifier_tokens("let arm_id = 3; let warm = 4;");
        assert!(tokens.contains(&"arm_id"));
        assert!(!tokens.contains(&"arm"));
    }

    #[test]
    fn legacy_stage_evaluation() {
        let policy: Value = serde_json::json!({
            "legacy_symbols": { "symbols": [
                { "symbol": "OldA", "removal_pr": "AQ-02", "stage": "report", "max_files": 2 },
                { "symbol": "OldB", "removal_pr": "AQ-08", "stage": "forbid", "max_files": 0 },
                { "symbol": "OldC", "removal_pr": "AQ-09", "stage": "report", "max_files": 1 }
            ]}
        });
        let mut found = BTreeMap::new();
        found.insert("OldA".to_string(), vec!["a.rs".to_string(), "b.rs".to_string()]);
        found.insert("OldB".to_string(), vec!["c.rs".to_string()]);
        found.insert("OldC".to_string(), vec!["d.rs".to_string(), "e.rs".to_string()]);
        let failures = evaluate_legacy_stages(&policy, &found);
        assert_eq!(failures.len(), 2, "{failures:?}");
        assert!(failures[0].contains("OldB"));
        assert!(failures[1].contains("OldC"));
    }

    #[test]
    fn allow_entries_require_a_reason() {
        let rule: Value = serde_json::json!({ "allow": [
            { "path": "crates/x", "reason": "" },
            { "path": "crates/y", "reason": "bench fixture" }
        ]});
        assert!(!allowed(&rule, "crates/x/src/lib.rs"));
        assert!(allowed(&rule, "crates/y/benches/b.rs"));
        assert!(!allowed(&rule, "crates/yy/src/lib.rs"));
    }
}
