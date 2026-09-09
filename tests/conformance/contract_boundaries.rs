//! `AQ-CONT-1` repository boundary checks.
//!
//! Driven by `conformance/aq-cont-1/contract-boundaries.json`. The checks enforce:
//!
//! - no downstream domain ownership in target crate code (`AQ-H1`, `AQ-H17`, `AQ-H18`);
//! - no queue-owned developmental ontology (`AQ-H19`, `AQ-H20`);
//! - no free-form metadata backchannel in any public position (`AQ-H10`);
//! - no scheduler, budget, or authority branching on attribution (`AQ-H11`, `AQ-H19`);
//! - no developmental or high-cardinality identifiers as metric labels;
//! - no target-crate reference to the frozen archive;
//! - staged removal of forbidden legacy symbols.
//!
//! The file universe is the git-tracked tree (see `support::tracked_files`), so ignored
//! build output and local notes can neither cause nor hide a failure. Rust sources are
//! parsed with `syn`; identifiers and string literals come from the token stream with
//! documentation attributes dropped, so explanatory prose is exempt while code is not,
//! and public positions (fields, variants, signatures, aliases, constants) are checked
//! from the AST rather than from line prefixes.

mod support;

use std::collections::BTreeMap;

use proc_macro2::{Delimiter, TokenStream, TokenTree};
use quote::ToTokens;
use serde_json::Value;
use support::{read_repo_text, repo_root, tracked_files, under};
use syn::spanned::Spanned;
use syn::visit::Visit;

const POLICY_PATH: &str = "conformance/aq-cont-1/contract-boundaries.json";
const MANIFEST_PATH: &str = "conformance/aq-cont-1/manifest.yaml";

/// One rule violation with enough context to locate and, if policy permits, allow it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Violation {
    rule: &'static str,
    path: String,
    line: usize,
    /// Declaration the violation belongs to (`Type::member`, `fn_name`), when known.
    item: Option<String>,
    detail: String,
}

impl std::fmt::Display for Violation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}:{}: ", self.rule, self.path, self.line)?;
        if let Some(item) = &self.item {
            write!(f, "{item}: ")?;
        }
        write!(f, "{}", self.detail)
    }
}

fn load_policy() -> Value {
    serde_json::from_str(&read_repo_text(POLICY_PATH)).expect("contract-boundaries.json parses")
}

fn strings(value: &Value, key: &str) -> Vec<String> {
    value[key]
        .as_array()
        .unwrap_or_else(|| panic!("policy key `{key}` is an array"))
        .iter()
        .map(|v| v.as_str().expect("policy strings").to_string())
        .collect()
}

/// True when an allow entry with a non-empty reason covers the violation. An entry
/// matches by path prefix and, when it names an `item`, only that declaration.
fn allowed(rule: &Value, violation: &Violation) -> bool {
    rule["allow"].as_array().is_some_and(|entries| {
        entries.iter().any(|entry| {
            let prefix = entry["path"].as_str().unwrap_or("");
            let reasoned = entry["reason"].as_str().is_some_and(|r| !r.is_empty());
            let item_matches = match entry["item"].as_str() {
                None => true,
                Some(item) => violation.item.as_deref() == Some(item),
            };
            reasoned && under(&violation.path, prefix) && item_matches
        })
    })
}

fn disallowed(rule: &Value, violations: Vec<Violation>) -> Vec<Violation> {
    violations.into_iter().filter(|v| !allowed(rule, v)).collect()
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

/// Splits text into identifier tokens.
fn identifier_tokens(text: &str) -> Vec<&str> {
    text.split(|c: char| !is_ident_char(c)).filter(|t| !t.is_empty()).collect()
}

// ---------------------------------------------------------------------------
// Token-level scanning
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TokenKind {
    Ident,
    Str,
}

/// An identifier or string literal from a source file, with its line.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Token {
    kind: TokenKind,
    text: String,
    line: usize,
}

fn is_doc_attribute(group: &proc_macro2::Group) -> bool {
    group.delimiter() == Delimiter::Bracket
        && matches!(group.stream().into_iter().next(), Some(TokenTree::Ident(id)) if id == "doc")
}

/// Collects identifiers and string literals from a token stream, descending into every
/// group (including macro invocations) and skipping `#[doc]` / `#![doc]` attributes, which
/// is how the lexer represents `///` and `//!` comments.
fn collect_tokens(stream: TokenStream, out: &mut Vec<Token>) {
    let trees: Vec<TokenTree> = stream.into_iter().collect();
    let mut i = 0;
    while i < trees.len() {
        match &trees[i] {
            TokenTree::Punct(p) if p.as_char() == '#' => {
                let mut j = i + 1;
                if matches!(trees.get(j), Some(TokenTree::Punct(bang)) if bang.as_char() == '!') {
                    j += 1;
                }
                if let Some(TokenTree::Group(group)) = trees.get(j) {
                    if is_doc_attribute(group) {
                        i = j + 1;
                        continue;
                    }
                }
            }
            TokenTree::Ident(ident) => out.push(Token {
                kind: TokenKind::Ident,
                text: ident.to_string(),
                line: ident.span().start().line,
            }),
            TokenTree::Literal(literal) => {
                if let syn::Lit::Str(s) = syn::Lit::new(literal.clone()) {
                    out.push(Token {
                        kind: TokenKind::Str,
                        text: s.value(),
                        line: literal.span().start().line,
                    });
                }
            }
            TokenTree::Group(group) => collect_tokens(group.stream(), out),
            TokenTree::Punct(_) => {}
        }
        i += 1;
    }
}

/// A parsed Rust source file.
struct RustSource {
    rel: String,
    tokens: Vec<Token>,
    file: syn::File,
}

impl RustSource {
    fn parse(rel: &str, text: &str) -> Self {
        let stream: TokenStream =
            text.parse().unwrap_or_else(|e| panic!("{rel}: cannot tokenize as Rust: {e}"));
        let file: syn::File =
            syn::parse2(stream.clone()).unwrap_or_else(|e| panic!("{rel}: cannot parse: {e}"));
        let mut tokens = Vec::new();
        collect_tokens(stream, &mut tokens);
        Self { rel: rel.to_string(), tokens, file }
    }

    /// Identifier tokens, including identifiers embedded in string literals.
    fn identifiers(&self) -> impl Iterator<Item = (usize, &str)> {
        self.tokens.iter().flat_map(|token| match token.kind {
            TokenKind::Ident => vec![(token.line, token.text.as_str())],
            TokenKind::Str => {
                identifier_tokens(&token.text).into_iter().map(|t| (token.line, t)).collect()
            }
        })
    }

    fn string_literals(&self) -> impl Iterator<Item = &Token> {
        self.tokens.iter().filter(|token| token.kind == TokenKind::Str)
    }
}

/// Parses every tracked `.rs` file under the given roots.
fn rust_sources(policy: &Value, roots: &[String]) -> Vec<RustSource> {
    let excluded = strings(policy, "always_excluded");
    tracked_files()
        .iter()
        .filter(|rel| rel.ends_with(".rs"))
        .filter(|rel| roots.iter().any(|root| under(rel, root)))
        .filter(|rel| !excluded.iter().any(|ex| under(rel, ex)))
        .map(|rel| RustSource::parse(rel, &read_repo_text(rel)))
        .collect()
}

fn target_sources(policy: &Value) -> Vec<RustSource> {
    rust_sources(policy, &strings(policy, "target_code_roots"))
}

/// Reports every identifier (including those inside string literals) that equals one of
/// `identifiers`.
fn scan_identifiers(
    rule: &'static str,
    sources: &[RustSource],
    identifiers: &[String],
    describe: impl Fn(&str) -> String,
) -> Vec<Violation> {
    let mut violations = Vec::new();
    for source in sources {
        for (line, token) in source.identifiers() {
            if identifiers.iter().any(|id| id == token) {
                violations.push(Violation {
                    rule,
                    path: source.rel.clone(),
                    line,
                    item: None,
                    detail: describe(token),
                });
            }
        }
    }
    violations
}

fn check_domain_ownership(policy: &Value) -> Vec<Violation> {
    let rule = &policy["domain_ownership"];
    let violations = scan_identifiers(
        "domain_ownership",
        &target_sources(policy),
        &strings(rule, "identifiers"),
        |token| format!("downstream domain identifier `{token}`"),
    );
    disallowed(rule, violations)
}

fn check_developmental_ontology(policy: &Value) -> Vec<Violation> {
    let rule = &policy["developmental_ontology"];
    let fragments = strings(rule, "identifier_fragments");
    let mut violations = Vec::new();
    for source in target_sources(policy) {
        for (line, token) in source.identifiers() {
            let lower = token.to_ascii_lowercase();
            if let Some(fragment) = fragments.iter().find(|f| lower.contains(f.as_str())) {
                violations.push(Violation {
                    rule: "developmental_ontology",
                    path: source.rel.clone(),
                    line,
                    item: None,
                    detail: format!("token `{token}` contains developmental fragment `{fragment}`"),
                });
            }
        }
    }
    disallowed(rule, violations)
}

// ---------------------------------------------------------------------------
// Public-position type checks (free-form metadata)
// ---------------------------------------------------------------------------

/// Renders a type with whitespace removed except where it separates two identifiers.
fn compact_type(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut pending_space = false;
    for c in text.chars() {
        if c.is_whitespace() {
            pending_space = true;
            continue;
        }
        if pending_space && is_ident_char(c) && out.ends_with(is_ident_char) {
            out.push(' ');
        }
        pending_space = false;
        out.push(c);
    }
    out
}

/// Rewrites leading path segments that are `use` aliases to their full paths, so
/// `Value` imported from `serde_json` renders as `serde_json::Value`.
fn canonicalize_type(compact: &str, aliases: &BTreeMap<String, String>) -> String {
    let mut out = String::with_capacity(compact.len());
    let mut rest = compact;
    while !rest.is_empty() {
        let ident_len = rest.chars().take_while(|c| is_ident_char(*c)).map(char::len_utf8).sum();
        if ident_len == 0 {
            let c = rest.chars().next().expect("non-empty");
            out.push(c);
            rest = &rest[c.len_utf8()..];
            continue;
        }
        let ident = &rest[..ident_len];
        let leading_segment = !out.ends_with("::") && !out.ends_with('\'');
        match aliases.get(ident) {
            Some(full) if leading_segment => out.push_str(full),
            _ => out.push_str(ident),
        }
        rest = &rest[ident_len..];
    }
    out
}

/// Flattens a `use` tree into `(local name, full path)` pairs.
fn collect_use_aliases(prefix: &str, tree: &syn::UseTree, out: &mut BTreeMap<String, String>) {
    match tree {
        syn::UseTree::Path(path) => {
            let next = if prefix.is_empty() {
                path.ident.to_string()
            } else {
                format!("{prefix}::{}", path.ident)
            };
            collect_use_aliases(&next, &path.tree, out);
        }
        syn::UseTree::Name(name) if name.ident != "self" => {
            out.insert(name.ident.to_string(), format!("{prefix}::{}", name.ident));
        }
        syn::UseTree::Rename(rename) => {
            out.insert(rename.rename.to_string(), format!("{prefix}::{}", rename.ident));
        }
        syn::UseTree::Group(group) => {
            for item in &group.items {
                collect_use_aliases(prefix, item, out);
            }
        }
        syn::UseTree::Name(_) | syn::UseTree::Glob(_) => {}
    }
}

/// Collects the names a file binds to other types: `use` imports and non-generic `type`
/// aliases of any visibility. Both are expanded before matching, so a private
/// `type Meta = HashMap<String, String>` cannot launder a public field.
#[derive(Default)]
struct AliasCollector {
    uses: BTreeMap<String, String>,
    type_aliases: BTreeMap<String, String>,
}

impl AliasCollector {
    /// Merges both maps, rendering alias targets through the `use` map (and once more
    /// through the merged map so an alias of an alias expands one level further).
    fn into_map(self) -> BTreeMap<String, String> {
        let mut map = self.uses;
        let mut expanded: BTreeMap<String, String> = self
            .type_aliases
            .iter()
            .map(|(name, target)| (name.clone(), canonicalize_type(target, &map)))
            .collect();
        let first_pass = expanded.clone();
        for target in expanded.values_mut() {
            *target = canonicalize_type(target, &first_pass);
        }
        map.extend(expanded);
        map
    }
}

impl<'ast> Visit<'ast> for AliasCollector {
    fn visit_item_use(&mut self, item: &'ast syn::ItemUse) {
        collect_use_aliases("", &item.tree, &mut self.uses);
    }

    fn visit_item_type(&mut self, item: &'ast syn::ItemType) {
        if item.generics.params.is_empty() {
            let target = compact_type(&item.ty.to_token_stream().to_string());
            self.type_aliases.insert(item.ident.to_string(), target);
        }
    }
}

/// Any non-inherited visibility counts as public: `pub(crate)` and `pub(super)` positions
/// are still channels between modules, so the metadata rule is deliberately stricter than
/// the crate's external API (see `public_type_fragments_note` in the policy).
fn is_pub(vis: &syn::Visibility) -> bool {
    !matches!(vis, syn::Visibility::Inherited)
}

fn type_name(ty: &syn::Type) -> String {
    match ty {
        syn::Type::Path(path) => {
            path.path.segments.last().map(|s| s.ident.to_string()).unwrap_or_default()
        }
        other => compact_type(&other.to_token_stream().to_string()),
    }
}

/// Visits every public position that carries a type and reports forbidden fragments.
struct PublicTypeVisitor<'p> {
    rel: &'p str,
    fragments: &'p [String],
    aliases: BTreeMap<String, String>,
    impl_stack: Vec<String>,
    violations: Vec<Violation>,
}

impl PublicTypeVisitor<'_> {
    fn check(&mut self, ty: &syn::Type, item: String, position: &str) {
        let rendered =
            canonicalize_type(&compact_type(&ty.to_token_stream().to_string()), &self.aliases);
        if let Some(fragment) = self.fragments.iter().find(|f| rendered.contains(f.as_str())) {
            self.violations.push(Violation {
                rule: "free_form_metadata",
                path: self.rel.to_string(),
                line: ty.span().start().line,
                item: Some(item),
                detail: format!(
                    "public {position} type `{rendered}` carries free-form `{fragment}`"
                ),
            });
        }
    }

    /// Checks struct fields (`Type::field`, public ones only) or the fields of an enum
    /// variant (`Enum::Variant`, all of them, since variant fields are as public as the enum).
    fn check_fields(&mut self, owner: &str, fields: &syn::Fields, struct_fields: bool) {
        for (index, field) in fields.iter().enumerate() {
            if struct_fields && !is_pub(&field.vis) {
                continue;
            }
            let item = if struct_fields {
                let name =
                    field.ident.as_ref().map_or_else(|| index.to_string(), |i| i.to_string());
                format!("{owner}::{name}")
            } else {
                owner.to_string()
            };
            self.check(&field.ty, item, "field");
        }
    }

    fn check_signature(&mut self, item: &str, sig: &syn::Signature) {
        for input in &sig.inputs {
            if let syn::FnArg::Typed(typed) = input {
                self.check(&typed.ty, item.to_string(), "parameter");
            }
        }
        if let syn::ReturnType::Type(_, ty) = &sig.output {
            self.check(ty, item.to_string(), "return");
        }
    }
}

impl<'ast> Visit<'ast> for PublicTypeVisitor<'_> {
    fn visit_item_struct(&mut self, item: &'ast syn::ItemStruct) {
        self.check_fields(&item.ident.to_string(), &item.fields, true);
    }

    fn visit_item_union(&mut self, item: &'ast syn::ItemUnion) {
        let fields = syn::Fields::Named(item.fields.clone());
        self.check_fields(&item.ident.to_string(), &fields, true);
    }

    fn visit_item_enum(&mut self, item: &'ast syn::ItemEnum) {
        if is_pub(&item.vis) {
            for variant in &item.variants {
                let owner = format!("{}::{}", item.ident, variant.ident);
                self.check_fields(&owner, &variant.fields, false);
            }
        }
    }

    fn visit_item_fn(&mut self, item: &'ast syn::ItemFn) {
        if is_pub(&item.vis) {
            self.check_signature(&item.sig.ident.to_string(), &item.sig);
        }
        syn::visit::visit_item_fn(self, item);
    }

    fn visit_item_impl(&mut self, item: &'ast syn::ItemImpl) {
        self.impl_stack.push(type_name(&item.self_ty));
        syn::visit::visit_item_impl(self, item);
        self.impl_stack.pop();
    }

    fn visit_impl_item_fn(&mut self, item: &'ast syn::ImplItemFn) {
        if is_pub(&item.vis) {
            let owner = self.impl_stack.last().cloned().unwrap_or_default();
            self.check_signature(&format!("{owner}::{}", item.sig.ident), &item.sig);
        }
        syn::visit::visit_impl_item_fn(self, item);
    }

    fn visit_item_trait(&mut self, item: &'ast syn::ItemTrait) {
        if is_pub(&item.vis) {
            for trait_item in &item.items {
                match trait_item {
                    syn::TraitItem::Fn(f) => {
                        self.check_signature(&format!("{}::{}", item.ident, f.sig.ident), &f.sig);
                    }
                    syn::TraitItem::Const(c) => {
                        self.check(&c.ty, format!("{}::{}", item.ident, c.ident), "const");
                    }
                    _ => {}
                }
            }
        }
        syn::visit::visit_item_trait(self, item);
    }

    fn visit_item_type(&mut self, item: &'ast syn::ItemType) {
        if is_pub(&item.vis) {
            self.check(&item.ty, item.ident.to_string(), "alias");
        }
    }

    fn visit_item_const(&mut self, item: &'ast syn::ItemConst) {
        if is_pub(&item.vis) {
            self.check(&item.ty, item.ident.to_string(), "const");
        }
    }

    fn visit_item_static(&mut self, item: &'ast syn::ItemStatic) {
        if is_pub(&item.vis) {
            self.check(&item.ty, item.ident.to_string(), "static");
        }
    }
}

fn public_type_violations(source: &RustSource, fragments: &[String]) -> Vec<Violation> {
    let mut aliases = AliasCollector::default();
    aliases.visit_file(&source.file);
    let mut visitor = PublicTypeVisitor {
        rel: &source.rel,
        fragments,
        aliases: aliases.into_map(),
        impl_stack: Vec::new(),
        violations: Vec::new(),
    };
    visitor.visit_file(&source.file);
    visitor.violations
}

fn check_free_form_metadata(policy: &Value) -> Vec<Violation> {
    let rule = &policy["free_form_metadata"];
    let fragments: Vec<String> =
        strings(rule, "public_type_fragments").iter().map(|f| compact_type(f)).collect();
    let sources = target_sources(policy);
    let mut violations = Vec::new();
    for source in &sources {
        violations.extend(public_type_violations(source, &fragments));
    }
    violations.extend(scan_identifiers(
        "free_form_metadata",
        &sources,
        &strings(rule, "identifiers"),
        |token| format!("metadata channel identifier `{token}`"),
    ));
    disallowed(rule, violations)
}

fn check_scheduler_backchannel(policy: &Value) -> Vec<Violation> {
    let rule = &policy["scheduler_backchannel"];
    let violations = scan_identifiers(
        "scheduler_backchannel",
        &rust_sources(policy, &strings(rule, "scope_paths")),
        &strings(rule, "identifiers"),
        |token| format!("scheduling/authority code reads attribution `{token}`"),
    );
    disallowed(rule, violations)
}

fn check_metric_labels(policy: &Value) -> Vec<Violation> {
    let rule = &policy["metric_labels"];
    let forbidden = strings(rule, "forbidden_label_tokens");
    let mut violations = Vec::new();
    for source in rust_sources(policy, &strings(rule, "scope_paths")) {
        for literal in source.string_literals() {
            for token in identifier_tokens(&literal.text) {
                if forbidden.iter().any(|f| f == token) {
                    violations.push(Violation {
                        rule: "metric_labels",
                        path: source.rel.clone(),
                        line: literal.line,
                        item: None,
                        detail: format!(
                            "string literal `{}` names forbidden label `{token}`",
                            literal.text
                        ),
                    });
                }
            }
        }
    }
    disallowed(rule, violations)
}

fn check_archive_isolation(policy: &Value) -> Vec<Violation> {
    let rule = &policy["archive_isolation"];
    let substrings = strings(rule, "substrings");
    let excluded = strings(policy, "always_excluded");
    let roots = strings(policy, "target_code_roots");
    let mut violations = Vec::new();
    for rel in tracked_files() {
        if !roots.iter().any(|root| under(rel, root)) || excluded.iter().any(|ex| under(rel, ex)) {
            continue;
        }
        let Some(text) = support::read_text_if_text(&repo_root().join(rel)) else {
            continue;
        };
        for (index, line) in text.lines().enumerate() {
            if let Some(needle) = substrings.iter().find(|s| line.contains(s.as_str())) {
                violations.push(Violation {
                    rule: "archive_isolation",
                    path: rel.clone(),
                    line: index + 1,
                    item: None,
                    detail: format!("target crate references the frozen archive via `{needle}`"),
                });
            }
        }
    }
    disallowed(rule, violations)
}

// ---------------------------------------------------------------------------
// Legacy symbols
// ---------------------------------------------------------------------------

/// Tracked files outside the exempt roots that are subject to the legacy-symbol scan.
fn legacy_scan_files(policy: &Value) -> Vec<String> {
    let mut excluded = strings(policy, "always_excluded");
    excluded.extend(strings(policy, "legacy_scan_exempt_roots"));
    tracked_files()
        .iter()
        .filter(|rel| !excluded.iter().any(|ex| under(rel, ex)))
        .cloned()
        .collect()
}

/// Lists, per legacy symbol, the tracked text files that still contain it.
fn legacy_symbol_files(policy: &Value) -> BTreeMap<String, Vec<String>> {
    let symbols: Vec<String> = policy["legacy_symbols"]["symbols"]
        .as_array()
        .expect("legacy symbols array")
        .iter()
        .map(|s| s["symbol"].as_str().expect("symbol").to_string())
        .collect();
    let mut found: BTreeMap<String, Vec<String>> =
        symbols.iter().map(|s| (s.clone(), Vec::new())).collect();
    for rel in legacy_scan_files(policy) {
        let Some(text) = support::read_text_if_text(&repo_root().join(&rel)) else {
            continue;
        };
        for symbol in &symbols {
            if contains_identifier(&text, symbol) {
                found.get_mut(symbol).expect("symbol entry").push(rel.clone());
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn policy_matches_conformance_manifest_revisions() {
    let policy = load_policy();
    let manifest = read_repo_text(MANIFEST_PATH);
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
fn every_allow_entry_names_a_path_and_a_reason() {
    let policy = load_policy();
    for (rule_name, rule) in policy.as_object().expect("policy object") {
        let Some(entries) = rule.get("allow").and_then(Value::as_array) else {
            continue;
        };
        for entry in entries {
            let path = entry["path"].as_str().unwrap_or("");
            assert!(!path.is_empty(), "{rule_name}: allow entry without a path: {entry}");
            assert!(
                !support::tracked_files_under(path).is_empty(),
                "{rule_name}: allow entry path `{path}` matches no tracked file"
            );
            assert!(
                entry["reason"].as_str().is_some_and(|r| !r.trim().is_empty()),
                "{rule_name}: allow entry for `{path}` lacks a reason"
            );
        }
    }
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

/// The allow entry for the CLI's JSON output variant must stay necessary: if the variant
/// disappears, the entry must be removed rather than silently outliving its purpose.
#[test]
fn free_form_metadata_allow_entries_are_still_exercised() {
    let policy = load_policy();
    let rule = &policy["free_form_metadata"];
    let fragments: Vec<String> =
        strings(rule, "public_type_fragments").iter().map(|f| compact_type(f)).collect();
    let raw: Vec<Violation> = target_sources(&policy)
        .iter()
        .flat_map(|source| public_type_violations(source, &fragments))
        .collect();
    for entry in rule["allow"].as_array().expect("allow array") {
        let path = entry["path"].as_str().expect("path");
        let item = entry["item"].as_str();
        let exercised = raw
            .iter()
            .any(|v| under(&v.path, path) && item.is_none_or(|i| v.item.as_deref() == Some(i)));
        assert!(exercised, "allow entry {entry} no longer matches any public declaration");
    }
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
fn scans_cover_only_tracked_files_and_every_target_crate() {
    let policy = load_policy();
    let sources = target_sources(&policy);
    assert!(
        sources.len() > 100,
        "expected the eleven crates to yield many sources, got {}",
        sources.len()
    );
    for member in [
        "core",
        "storage",
        "engine",
        "executor-local",
        "runtime",
        "daemon",
        "cli",
        "workflow",
        "budget",
        "actor",
        "platform",
    ] {
        let prefix = format!("crates/actionqueue-{member}/src/");
        assert!(sources.iter().any(|s| s.rel.starts_with(&prefix)), "no sources under {prefix}");
    }
    let scanned = legacy_scan_files(&policy);
    assert!(!scanned.is_empty());
    for rel in &scanned {
        assert!(repo_root().join(rel).is_file(), "{rel} listed but missing");
        assert!(!rel.starts_with("target/") && !rel.starts_with("archive/"), "{rel} scanned");
    }
}

/// Removes a scratch file when dropped, even if an assertion fails first.
struct Untracked(std::path::PathBuf);

impl Drop for Untracked {
    fn drop(&mut self) {
        std::fs::remove_file(&self.0).ok();
    }
}

/// An untracked file inside the repository (local notes, editor backups, ignored build
/// output) must never enter any scan, so the ratchet counts depend only on the tree.
#[test]
fn untracked_files_inside_the_repository_are_invisible_to_every_scan() {
    let policy = load_policy();
    let legacy = policy["legacy_symbols"]["symbols"][0]["symbol"].as_str().expect("symbol");
    let nonce = std::process::id();
    let name = format!("aq-conformance-scratch-{nonce}.rs");
    let guard = Untracked(repo_root().join("crates").join(&name));
    std::fs::write(
        &guard.0,
        format!("pub struct Vessel {{ pub campaign: HashMap<String, String> }} // {legacy}\n"),
    )
    .expect("write scratch file");
    let rel = format!("crates/{name}");
    assert!(!tracked_files().contains(&rel));
    assert!(!legacy_scan_files(&policy).contains(&rel));
    assert!(!target_sources(&policy).iter().any(|s| s.rel == rel));
    let found = legacy_symbol_files(&policy);
    assert!(!found[legacy].contains(&rel));
    assert!(check_domain_ownership(&policy).iter().all(|v| v.path != rel));
    drop(guard);
}

#[cfg(test)]
mod unit {
    use super::*;

    fn source(text: &str) -> RustSource {
        RustSource::parse("crates/x/src/lib.rs", text)
    }

    fn idents(text: &str) -> Vec<String> {
        source(text).identifiers().map(|(_, t)| t.to_string()).collect()
    }

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
    fn comments_and_doc_attributes_are_exempt_but_code_and_strings_are_not() {
        let src = "//! Vesselish crate doc\n/// Vesselish item doc\n#[doc = \"Vesselish explicit \
                   doc\"]\nfn a() { /* Vesselish */ let s = \"keep // Receiptish\"; } // \
                   Vesselish\n";
        let found = idents(src);
        assert!(!found.iter().any(|t| t == "Vesselish"), "{found:?}");
        assert!(found.iter().any(|t| t == "Receiptish"), "{found:?}");
        assert!(found.iter().any(|t| t == "keep"));
        let parsed = source(src);
        let receipt = parsed.identifiers().find(|(_, t)| *t == "Receiptish").expect("found");
        assert_eq!(receipt.0, 4, "line numbers come from spans");
    }

    #[test]
    fn raw_strings_with_odd_quote_counts_do_not_desynchronize_the_scan() {
        let src = "const A: &str = r#\"one \" quote\"#;\n/// Vesselish doc after an odd raw \
                   string\nfn later() { let campaign_id = 1; }\nconst B: &str = r##\"three \" \" \
                   \" quotes\"##;\nfn end() { let arm_id = 2; }\n";
        let found = idents(src);
        assert!(!found.iter().any(|t| t == "Vesselish"), "{found:?}");
        assert!(found.iter().any(|t| t == "campaign_id"), "{found:?}");
        assert!(found.iter().any(|t| t == "arm_id"), "{found:?}");
        let parsed = source(src);
        let literals: Vec<&str> = parsed.string_literals().map(|t| t.text.as_str()).collect();
        assert_eq!(literals, vec!["one \" quote", "three \" \" \" quotes"]);
    }

    #[test]
    fn char_literals_and_macro_bodies_are_scanned_correctly() {
        let src = "fn a() { let q = '\"'; let s = \"x\"; println!(\"campaign {}\", q); }\n";
        let found = idents(src);
        assert!(found.iter().any(|t| t == "campaign"), "{found:?}");
        let parsed = source(src);
        let literals: Vec<&str> = parsed.string_literals().map(|t| t.text.as_str()).collect();
        assert_eq!(literals, vec!["x", "campaign {}"]);
    }

    fn fragments() -> Vec<String> {
        [
            "HashMap<String, String>",
            "BTreeMap<String, String>",
            "serde_json::Value",
            "serde_json::Map",
        ]
        .iter()
        .map(|f| compact_type(f))
        .collect()
    }

    fn public_items(text: &str) -> Vec<String> {
        public_type_violations(&source(text), &fragments())
            .into_iter()
            .map(|v| v.item.expect("item"))
            .collect()
    }

    #[test]
    fn public_positions_of_every_kind_are_checked() {
        let src = "use serde_json::Value;\nuse std::collections::{BTreeMap, HashMap};\npub struct \
                   S { pub extra: HashMap<String,   String>, hidden: HashMap<String, String> \
                   }\npub struct T(pub BTreeMap<String, String>);\npub enum Out { Text(String), \
                   Json(Value), Named { v: serde_json::Value } }\nenum Private { Json(Value) \
                   }\npub fn f(v: Value) -> Option<Value> { None }\nfn private(v: Value) -> Value \
                   { v }\npub type Alias = Vec<Value>;\npub const C: Option<Value> = None;\npub \
                   trait Tr { fn m(&self) -> Value; }\nimpl S { pub fn get(&self) -> &Value { \
                   unreachable!() } fn p(&self) -> Value { unreachable!() } }\n";
        let items = public_items(src);
        assert_eq!(
            items,
            vec![
                "S::extra",
                "T::0",
                "Out::Json",
                "Out::Named",
                "f",
                "f",
                "Alias",
                "C",
                "Tr::m",
                "S::get",
            ]
        );
    }

    #[test]
    fn use_aliases_are_canonicalized_before_matching() {
        let src = "use serde_json::{Map, Value as Json};\npub struct A { pub a: Json, pub b: \
                   Map<String, Json> }\nmod other { pub struct Value; }\npub struct B { pub \
                   not_json: other::Value }\n";
        let violations = public_type_violations(&source(src), &fragments());
        let rendered: Vec<String> = violations.iter().map(|v| v.detail.clone()).collect();
        assert_eq!(violations.len(), 2, "{rendered:?}");
        assert!(rendered[0].contains("`serde_json::Value`"), "{rendered:?}");
        assert!(
            rendered[1].contains("`serde_json::Map<String,serde_json::Value>`"),
            "{rendered:?}"
        );
    }

    #[test]
    fn private_type_aliases_do_not_launder_public_positions() {
        let src = "use std::collections::HashMap;\ntype Meta = HashMap<String, String>;\ntype \
                   Meta2 = Option<Meta>;\ntype Generic<T> = HashMap<String, T>;\npub struct A { \
                   pub m: Meta, pub n: Meta2, pub g: Generic<u8> }\npub fn f() -> Meta { \
                   unreachable!() }\n";
        let violations = public_type_violations(&source(src), &fragments());
        let items: Vec<&str> = violations.iter().map(|v| v.item.as_deref().unwrap()).collect();
        assert_eq!(items, vec!["A::m", "A::n", "f"], "{violations:?}");
        assert!(violations[1].detail.contains("Option<std::collections::HashMap<String,String>>"));
    }

    #[test]
    fn pub_crate_positions_count_as_public() {
        let src =
            "use serde_json::Value;\npub(crate) struct S { pub(crate) v: Value }\npub(super) \
                   fn f(v: Value) {}\nstruct P { v: Value }\n";
        assert_eq!(public_items(src), vec!["S::v", "f"]);
    }

    #[test]
    fn compact_type_keeps_identifier_separation() {
        assert_eq!(compact_type("HashMap<String,   String>"), "HashMap<String,String>");
        assert_eq!(compact_type("impl Fn() -> Value + Send"), "impl Fn()->Value+Send");
        assert_eq!(compact_type("& 'a str"), "&'a str");
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
    fn allow_entries_require_a_reason_and_honour_item_scope() {
        let rule: Value = serde_json::json!({ "allow": [
            { "path": "crates/x", "reason": "" },
            { "path": "crates/y", "reason": "bench fixture" },
            { "path": "crates/z/src/lib.rs", "item": "Out::Json", "reason": "cli rendering" }
        ]});
        let v = |path: &str, item: Option<&str>| Violation {
            rule: "free_form_metadata",
            path: path.to_string(),
            line: 1,
            item: item.map(str::to_string),
            detail: String::new(),
        };
        assert!(!allowed(&rule, &v("crates/x/src/lib.rs", None)));
        assert!(allowed(&rule, &v("crates/y/benches/b.rs", None)));
        assert!(!allowed(&rule, &v("crates/yy/src/lib.rs", None)));
        assert!(allowed(&rule, &v("crates/z/src/lib.rs", Some("Out::Json"))));
        assert!(!allowed(&rule, &v("crates/z/src/lib.rs", Some("Out::Other"))));
        assert!(!allowed(&rule, &v("crates/z/src/lib.rs", None)));
    }
}
