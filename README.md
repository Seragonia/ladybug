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

A fork of [Ladybug](https://github.com/LadybugDB/ladybug) (formerly [Kuzu](https://github.com/kuzudb/kuzu)) optimized for
**building, extending and editing very large graphs inside one process with bounded memory**: hundreds of millions to
billions of relationships on a single machine, without running a server. Query behaviour and the Cypher dialect are
unchanged; everything upstream offers still works.

| | Upstream Ladybug | This fork |
|---|---|---|
| Load 330M relationships, 8 GB buffer pool | fails after 28 min ("Buffer pool is full", 16.7 GB RSS) | ~7 min, 8 GB RSS |
| Extend that table by 33M relationships | 321 s | 287 s |
| Delete ~3.3M relationships by predicate (skewed in-degree) | 1,559 s | 585 s |
| Full-text search and graph algorithms offline | need `INSTALL` from the network; `LOAD algo` fails on Linux | linked in, no download |
| `import kuzu` and `import ladybug` in one process | import error, then crash at exit | works |

Measured on a synthetic transaction graph (11.8M nodes, 330M relationships with super-nodes) on 16 vCPU / 24 GB.
The upstream column is the same source tree without this fork's commits; since upstream cannot load this table, its
extend and delete numbers are measured with only the bucketed load applied.

## What changed

- **Bucketed relationship COPY.** Two COPY options, `DIRECTION='fwd'|'bwd'` (build one direction's CSR) and
  `RAW_OFFSETS=true` (take source, destination and relationship offsets from the input instead of looking up keys),
  plus a COPY that leaves untouched node groups alone. A loader can then copy one node-group-aligned bucket at a time,
  so memory is bounded by the bucket, not the table. See [`scripts/bucketed_load`](scripts/bucketed_load).
- **Faster extend.** A COPY into populated node groups is merged at checkpoint in contiguous runs instead of row by row.
- **Faster bulk delete and update.** Long relationship lists are scanned once per transaction instead of once per
  deleted or updated relationship.
- **Bundled extensions.** `make python-bundled` links `fts` and `algo` (PAGE_RANK, WCC, SCC, LOUVAIN, K_CORE) into the
  Python module.
- **Coexists with Kuzu.** Module-local bindings, `-Bsymbolic`, and a local load of the native module.

## Build

```bash
git clone --recurse-submodules https://github.com/Seragonia/ladybug
cd ladybug && make python-bundled
export PYTHONPATH=$PWD/tools/python_api/build
```

Experimental: correctness is checked by the tests in [`scripts/bucketed_load`](scripts/bucketed_load) (bucketed load,
extend, deletes and updates compared against a stock COPY and against DuckDB), not by a full run of upstream's suite.

---

## Upstream README

## Ladybug
Ladybug is an embedded graph database built for query speed and scalability. Ladybug is optimized for handling complex analytical workloads
on very large databases and provides a set of retrieval features, such as a full text search and vector indices. Our core feature set includes:

- Flexible Property Graph Data Model and Cypher query language
- Embeddable, serverless integration into applications
- Native full text search and vector index
- Columnar disk-based storage
- Columnar sparse row-based (CSR) adjacency list/join indices
- Vectorized and factorized query processor
- Novel and very fast join algorithms
- Multi-core query parallelism
- Serializable ACID transactions
- Wasm (WebAssembly) bindings for fast, secure execution in the browser

Ladybug is being developed by [LadybugDB Developers](https://github.com/LadybugDB) and
is available under a permissive license. So try it out and help us make it better! We welcome your feedback and feature requests.

The database was formerly known as [Kuzu](https://github.com/kuzudb/kuzu).

## Installation

| Language | Installation                                                           |
| -------- |------------------------------------------------------------------------|
| Python   | `pip install ladybug`                                             |
| NodeJS   | `npm install @ladybugdb/core`                                            |
| Rust     | `cargo add lbug`                                                       |
| Go       | `go get github.com/LadybugDB/go-ladybug`                                     |
| Swift    | [lbug-swift](https://github.com/LadybugDB/swift-ladybug)                     |
| Java     | [Maven Central](https://central.sonatype.com/artifact/com.ladybugdb/lbug) |
| C/C++    | [precompiled binaries](https://github.com/LadybugDB/ladybug/releases/latest) |
| CLI      | [precompiled binaries](https://github.com/LadybugDB/ladybug/releases/latest) |

To learn more about installation, see our [Installation](https://docs.ladybugdb.com/installation) page.

## Getting Started

Refer to our [Getting Started](https://docs.ladybugdb.com/get-started/) page for your first example.

## Build from Source

You can build from source using the instructions provided in the [developer guide](https://docs.ladybugdb.com/developer-guide/).

## Contributing
We welcome contributions to Ladybug. If you are interested in contributing to Ladybug, please read our [Contributing Guide](CONTRIBUTING.md).

## License
By contributing to Ladybug, you agree that your contributions will be licensed under the [MIT License](LICENSE).

## Contact
You can contact us at [social@ladybugdb.com](mailto:social@ladybugdb.com) or [join our Discord community](https://discord.com/invite/hXyHmvW3Vy).
