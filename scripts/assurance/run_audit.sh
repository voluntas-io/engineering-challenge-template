#!/usr/bin/env bash
# Non-blocking English and terminology audit.
#
# Usage:
#   scripts/assurance/run_audit.sh [--history] [OUTPUT]
#
# The command writes a JSON report and exits zero when findings are present.
# Findings are review candidates, not automatic failures. Operational errors
# (for example, a missing Git repository) still return non-zero.
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
history=()
if [[ "${1:-}" == "--history" ]]; then
  history=(--include-history)
  shift
fi

output="${1:-${repo_root}/build/assurance/english-audit.json}"
mkdir -p "$(dirname "${output}")"

set +e
if [[ "${#history[@]}" -gt 0 ]]; then
  python3 "${repo_root}/scripts/assurance/english_audit.py" \
    --repo "${repo_root}" \
    "${history[@]}" \
    --json-out "${output}"
else
  python3 "${repo_root}/scripts/assurance/english_audit.py" \
    --repo "${repo_root}" \
    --json-out "${output}"
fi
status=$?
set -e

if [[ "${status}" -eq 2 ]]; then
  exit 2
fi

printf 'English audit report: %s\n' "${output}"
exit 0
