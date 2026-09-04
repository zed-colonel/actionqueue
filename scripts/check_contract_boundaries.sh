#!/usr/bin/env bash
# AQ-CONT-1 contract boundary and frozen-evidence checks.
#
# Runs the conformance test binaries that enforce the boundary policy in
# conformance/aq-cont-1/contract-boundaries.json and verify the pinned evidence,
# then prints the contract and developmental-profile revisions the tree implements.
# Exits non-zero if any check fails.
#
# Usage: scripts/check_contract_boundaries.sh [extra cargo test args]
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

cargo test \
  --test conformance_contract_boundaries \
  --test conformance_frozen_evidence \
  --test conformance_pre_contract_store \
  "$@" -- --test-threads=1 --nocapture

manifest=conformance/aq-cont-1/manifest.yaml
field() { awk -v k="$1:" '$1 == k { print $2; exit }' "$manifest"; }
echo
echo "AQ-CONT-1 pinned revisions"
echo "  contract:                        $(field contract)"
echo "  contract_revision:               $(field contract_revision)"
echo "  planning_profile:                $(field planning_profile)"
echo "  developmental_profile_revision:  $(field developmental_profile_revision)"
echo "  package_revision:                $(field package_revision)"
echo "  baseline_commit:                 $(field baseline_commit)"
