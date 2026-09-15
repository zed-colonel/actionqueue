#!/usr/bin/env bash
# Run from repository root. Each executable is built independently, without
# workspace test-feature unification. Scratch and build output honor TMPDIR and CARGO_TARGET_DIR.
set -euo pipefail
probe_dir=$(mktemp -d "${TMPDIR:-/tmp}/cross-feature.XXXXXX")
probe_target_dir="$(cargo metadata --offline --no-deps --format-version 1 | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')"
export CARGO_TARGET_DIR="$probe_target_dir"
trap 'rm -rf "$probe_dir"' EXIT
cargo build --offline -p actionqueue-storage --example lineage_probe --no-default-features --features serde
cp "$CARGO_TARGET_DIR/debug/examples/lineage_probe" "$probe_dir/base"
cargo build --offline -p actionqueue-storage --example lineage_probe --no-default-features --features serde,workflow,budget,actor,platform
cp "$CARGO_TARGET_DIR/debug/examples/lineage_probe" "$probe_dir/rich"
"$probe_dir/base" wire > "$probe_dir/base-wire"
"$probe_dir/rich" wire > "$probe_dir/rich-wire"
cmp "$probe_dir/base-wire" "$probe_dir/rich-wire"
"$probe_dir/base" create "$probe_dir/base-store"
"$probe_dir/rich" read "$probe_dir/base-store" > "$probe_dir/compatible.json"
"$probe_dir/rich" create "$probe_dir/rich-store"
# The continuation workload is single-tenant; separately verify the complete
# production feature manifest (including platform) without forging tenant grants.
"$probe_dir/rich" create-profile "$probe_dir/full-profile-store"
"$probe_dir/rich" read "$probe_dir/full-profile-store" > "$probe_dir/full-profile.json"
cp -a "$probe_dir/full-profile-store" "$probe_dir/full-profile-before"
if "$probe_dir/base" read "$probe_dir/full-profile-store" 2> "$probe_dir/full-profile-refusal"; then
    echo 'ERROR: base binary accepted complete rich profile' >&2; exit 1
fi
grep -q UnsupportedFeatures "$probe_dir/full-profile-refusal"
diff -r "$probe_dir/full-profile-before" "$probe_dir/full-profile-store"
cp -a "$probe_dir/base-store" "$probe_dir/base-before"
cp -a "$probe_dir/rich-store" "$probe_dir/rich-before"
if "$probe_dir/base" read "$probe_dir/rich-store" 2> "$probe_dir/refusal"; then
    echo 'ERROR: base binary accepted unsupported profile' >&2; exit 1
fi
grep -q UnsupportedFeatures "$probe_dir/refusal"
if "$probe_dir/rich" add-budget "$probe_dir/base-store" 2> "$probe_dir/write-refusal"; then
    echo 'ERROR: rich binary expanded base profile' >&2; exit 1
fi
grep -q UnsupportedFeatures "$probe_dir/write-refusal"
diff -r "$probe_dir/base-before" "$probe_dir/base-store"
diff -r "$probe_dir/rich-before" "$probe_dir/rich-store"
echo 'Cross-build persistence: compatible read, stable bytes, unsupported profile, and inactive-feature write refusal passed.'
