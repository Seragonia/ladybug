<div align="center">
  <picture>
    <!-- <source srcset="https://ladybugdb.com/img/lbug-logo-dark.png" media="(prefers-color-scheme: dark)"> -->
    <img src="https://ladybugdb.com/logo.png" height="100" alt="Ladybug Logo">
  </picture>
</div>

<br>

<p align="center">
  <a href="https://github.com/LadybugDB/ladybug/actions">
    <img src="https://github.com/LadybugDB/ladybug/actions/workflows/ci-workflow.yml/badge.svg?branch=master" alt="Github Actions Badge"></a>
  <a href="https://deepwiki.com/LadybugDB/ladybug"><img src="https://deepwiki.com/badge.svg" alt="Ask DeepWiki"></a>
  <a href="https://discord.com/invite/hXyHmvW3Vy">
    <img src="https://img.shields.io/discord/1162999022819225631?logo=discord" alt="discord" /></a>
  <a href="https://twitter.com/lbugdb">
    <img src="https://img.shields.io/badge/follow-@lbugdb-1DA1F2?logo=twitter" alt="twitter"></a>
</p>

# Ladybug, fast-load fork

[Ladybug](https://github.com/LadybugDB/ladybug) is an embedded graph database: a library that runs inside your process,
like SQLite, with Cypher queries. It is the successor of [Kuzu](https://github.com/kuzudb/kuzu).

**This fork lets it handle graphs that upstream cannot**: build, grow and edit hundreds of millions to billions of
relationships on one machine, with a fixed amount of memory. Queries and Cypher are unchanged.

![Fork vs upstream Ladybug](scripts/benchmark/results.svg)

- **330M relationships, 8 GB memory budget:** upstream runs out of memory building the graph; the fork builds it, then
  extends and edits it.
- **33M relationships** (both work): the fork builds with half the memory, and adds or deletes data about 3x faster.
- **Queries:** same speed, same engine.

## How

- **Load in pieces.** Relationships are copied a slice of the graph at a time, so memory holds one slice, never the
  whole table.
- **Look keys up once.** All node keys are turned into internal ids in one fast pass before loading.
- **Only rewrite what changed.** Adding data touches only the parts of the graph that receive new relationships, and
  those are rewritten in parallel.
- **Delete in batches.** A bulk delete scans each adjacency list once, instead of searching once per relationship.
- **Batteries included.** Full-text search and graph algorithms are built in (no download), and it can run in the same
  process as Kuzu.

## Use it

```bash
git clone --recurse-submodules https://github.com/Seragonia/ladybug
cd ladybug && make python-bundled
export PYTHONPATH=$PWD/tools/python_api/build
```

Load large relationship tables with [`scripts/bucketed_load`](scripts/bucketed_load); everything else is plain Ladybug.

Experimental. Tested by the suites in [`scripts/bucketed_load`](scripts/bucketed_load) (results checked against a
stock load and against DuckDB), not by upstream's full test suite.

<details><summary>Benchmark details</summary>

Synthetic money-transfer graph with a few very busy accounts (`scripts/benchmark/gen.py`), 16 vCPU / 24 GB RAM,
8 GB buffer pool, upstream = `pip install ladybug==0.22.0`. Reproduce with `scripts/benchmark/run.sh`; raw results in
`scripts/benchmark/results.jsonl`. The fork's memory includes the loader's DuckDB, in the same process.

| Data | Measure | Upstream | Fork | Speed-up |
|---|---|---|---|---|
| 33M edges | build graph | 14.0 s | 27.1 s | 0.5x |
| 33M edges | build graph: peak RAM | 6.17 GB | 2.89 GB | 2.1x |
| 33M edges | extend +10% edges | 28.2 s | 8.54 s | 3.3x |
| 33M edges | extend +10% edges: peak RAM | 2.75 GB | 3.71 GB | 0.7x |
| 33M edges | delete ~1% edges | 42.4 s | 12.9 s | 3.3x |
| 33M edges | delete ~1% edges: peak RAM | 2.93 GB | 3.84 GB | 0.8x |
| 33M edges | point lookup | 0.10 ms | 0.07 ms | 1.4x |
| 33M edges | 1-hop out | 0.21 ms | 0.20 ms | 1.0x |
| 33M edges | 1-hop in | 0.17 ms | 0.16 ms | 1.1x |
| 33M edges | 2-hop count | 0.63 ms | 0.62 ms | 1.0x |
| 33M edges | super-node in | 4.10 ms | 3.90 ms | 1.1x |
| 33M edges | filtered scan | 142 ms | 140 ms | 1.0x |
| 33M edges | group by | 156 ms | 164 ms | 1.0x |
| 33M edges | super-node 2-hop | 127 ms | 113 ms | 1.1x |
| 330M edges | build graph | failed | 1,070 s | upstream fails |
| 330M edges | build graph: peak RAM | 13.5 GB | 7.44 GB | 1.8x |
| 330M edges | extend +10% edges | failed | 220 s | upstream fails |
| 330M edges | extend +10% edges: peak RAM | failed | 11.4 GB | upstream fails |
| 330M edges | delete ~1% edges | failed | 327 s | upstream fails |
| 330M edges | delete ~1% edges: peak RAM | failed | 16.0 GB | upstream fails |
| 330M edges | point lookup | failed | 2.61 ms | upstream fails |
| 330M edges | 1-hop out | failed | 31.6 ms | upstream fails |
| 330M edges | 1-hop in | failed | 7.10 ms | upstream fails |
| 330M edges | 2-hop count | failed | 5.28 ms | upstream fails |
| 330M edges | super-node in | failed | 32.8 ms | upstream fails |
| 330M edges | filtered scan | failed | 2,004 ms | upstream fails |
| 330M edges | group by | failed | 3,335 ms | upstream fails |
| 330M edges | super-node 2-hop | failed | 2,155 ms | upstream fails |

</details>

For everything else, see the [upstream README](https://github.com/LadybugDB/ladybug#readme).
