#!/usr/bin/env bash
# 100% line coverage for the pure modules, measured over the whole test suite.
# Add a file to `want` when a module becomes pure and tested.
set -euo pipefail
cd "$(dirname "$0")/.."
cargo llvm-cov --json --output-path target/llvm-cov.json >/dev/null
python3 - <<'PY'
import json, os, sys
root = os.getcwd()
data = json.load(open("target/llvm-cov.json"))
want = {
    "src/ids.rs",
    "src/state.rs",
    "src/error.rs",
    "src/wire.rs",
    "src/config/legacy.rs",
    "src/config/parse.rs",
    "src/config/paths.rs",
    "src/config/resolve.rs",
}
seen = set()
bad = []
for f in data["data"][0]["files"]:
    rel = os.path.relpath(f["filename"], root)
    if rel not in want:
        continue
    seen.add(rel)
    lines = f["summary"]["lines"]
    print(f"{rel}: {lines['covered']}/{lines['count']} lines ({lines['percent']:.1f}%)")
    if lines["covered"] != lines["count"]:
        bad.append(rel)
missing = want - seen
if missing:
    print("not measured:", ", ".join(sorted(missing)))
    sys.exit(1)
if bad:
    print("below 100%:", ", ".join(bad))
    sys.exit(1)
print(f"{len(want)} pure modules at 100%")
PY
