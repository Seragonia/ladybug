#!/usr/bin/env bash
# Benchmark this fork against upstream Ladybug from PyPI on the same synthetic data.
# usage: run.sh <data_dir> <work_dir> [buffer_pool_gb]
#   Needs a Python with duckdb, and: UPSTREAM_PYTHON = a Python with `pip install ladybug` (the upstream wheel),
#   FORK_PYTHON (default: python3) run with PYTHONPATH=<this repo>/tools/python_api/build (`make python-bundled`).
# Phases run one after another on the same database: load, query, extend (+10%), delete (~1%), query.
# Results are appended to <work_dir>/results.jsonl; chart.py turns them into results.svg.
set -u
data=$1; work=$2; bp=${3:-8}
here=$(cd "$(dirname "$0")" && pwd); repo=$(cd "$here/../.." && pwd)
mkdir -p "$work"
for engine in upstream fork; do
  if [ $engine = upstream ]; then py=${UPSTREAM_PYTHON:?set UPSTREAM_PYTHON}; pp=; else py=${FORK_PYTHON:-python3}; pp=$repo/tools/python_api/build; fi
  db=$work/$engine.$(basename "$data").lbug
  for phase in load query extend delete query-after-edits; do
    PYTHONPATH=$pp timeout ${PHASE_TIMEOUT:-7200} "$py" "$here/bench.py" $engine $phase "$data" "$db" "$work/results.jsonl" "$bp" \
      || echo "{\"engine\": \"$engine\", \"phase\": \"$phase\", \"data\": \"$(basename "$data")\", \"ok\": false, \"error\": \"exit $? (killed or timed out)\"}" >> "$work/results.jsonl"
  done
  rm -rf "$db" "$db.wal" "$db.work"
done
