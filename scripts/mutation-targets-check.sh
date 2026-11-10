#!/usr/bin/env bash
# Fails unless every given file produces at least one mutant. `cargo schemata
# --file <glob>` exits 0 when the glob matches nothing, so a renamed file would
# otherwise leave a mutation job green while checking nothing.
# Usage: scripts/mutation-targets-check.sh src/matrix.rs src/filter.rs ...
set -euo pipefail
cd "$(dirname "$0")/.."
[ "$#" -gt 0 ] || { echo "usage: $0 <file>..." >&2; exit 2; }
status=0
for f in "$@"; do
  n=$(cargo schemata --file "$f" --list | grep -c "^$f:" || true)
  if [ "$n" -eq 0 ]; then
    echo "no mutants for $f (renamed, or the glob matches nothing)" >&2
    status=1
  else
    echo "$f: $n mutants"
  fi
done
exit "$status"
