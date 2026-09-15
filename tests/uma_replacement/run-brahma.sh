#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
run_id="$(date -u +%Y%m%dT%H%M%SZ)-$$"
remote_dir="/home/anchen/analysis/uma-cutover-20260915/replacement-tests/$run_id"
local_dir="${TMPDIR:-/tmp}/uma-replacement-$run_id"
mkdir -p "$local_dir"
git rev-parse HEAD > "$local_dir/source-head.txt"
git diff --binary HEAD -- Cargo.toml Cargo.lock src tests > "$local_dir/source-diff.patch"
# Explicit allowlist: no deploy configuration, keys, runtime journal or environment files.
COPYFILE_DISABLE=1 tar --no-xattrs -czf "$local_dir/source.tar.gz" Cargo.toml Cargo.lock src tests
ssh brahma "mkdir -p '$remote_dir/source' '$remote_dir/results' && chmod 0777 '$remote_dir/results'"
scp -q "$local_dir/source.tar.gz" "$local_dir/source-head.txt" "$local_dir/source-diff.patch" "brahma:$remote_dir/"
ssh brahma "tar -xzf '$remote_dir/source.tar.gz' -C '$remote_dir/source' && cd '$remote_dir/source' && sudo -n docker build -f tests/uma_replacement/Dockerfile -t 'uma-replacement-tests:$run_id' ." 2>&1 | tee "$local_dir/build.log"
set +e
ssh brahma "sudo -n docker run --rm --init --network none --read-only --cap-drop ALL --security-opt no-new-privileges --cpus 2 --memory 1g --pids-limit 128 --tmpfs /tmp:rw,nosuid,size=128m -v '$remote_dir/results:/results' 'uma-replacement-tests:$run_id'" 2>&1 | tee "$local_dir/run.log"
code=${PIPESTATUS[0]}
set -e
scp -qr "brahma:$remote_dir/results" "$local_dir/"
scp -q "$local_dir/build.log" "$local_dir/run.log" "brahma:$remote_dir/"
printf 'Results: %s/results\nBrahma: %s\nExit: %s (0 pass, 1 acceptance/regression failure, 2 infrastructure failure)\n' "$local_dir" "$remote_dir" "$code"
exit "$code"
