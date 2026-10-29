# Bucketed rel load

`bucketed_load.bucketed_copy` loads a large rel table with bounded memory using two COPY options of
this fork:

- `DIRECTION='fwd'|'bwd'` builds the CSR of one direction only.
- `RAW_OFFSETS=true` reads the src node offset, dst node offset and rel offset from the first three
  input columns instead of looking up primary keys.

DuckDB resolves keys to node offsets, assigns each rel its offset once, and splits the rels into
node-group-aligned buckets: by src for FWD, by dst for BWD. Each bucket is one COPY that writes only
the node groups it covers, so memory is bounded by the bucket, not the table. Run it again on more
rels to extend a table: rel offsets continue after the largest one in the table.

```python
import ladybug
from bucketed_load import bucketed_copy

conn = ladybug.Connection(ladybug.Database("graph.lbug"))
bucketed_copy(conn, "TX", "Account", "id", "Account", "id", "'tx.parquet'", "src", "dst",
              ["amount", "ts", "kind"], "graph.lbug.work")
```

## Without DuckDB

`bucketed_load.engine_bucketed_copy` does the same with the engine only, using two more options:

- `REL_OFFSETS=true` keeps the primary-key lookups but reads the rel offset from the third input
  column (input: src key, dst key, rel offset, properties).
- `NODE_GROUP_BEGIN=b, NODE_GROUP_END=e` (with `DIRECTION` and `REL_OFFSETS`) keeps only the rels
  whose bound node (src for FWD, dst for BWD) is in node groups `[b, e)`. The bound key is looked
  up first and the other rels are dropped before the other lookup and before the partitioner
  buffers them.

Each bucket re-scans the input and looks its bound keys up again: K buckets cost K scans and K + 1
lookups per direction. The rel offset must be the same in both directions, so it comes from the
input: pass `rel_col`, a 0-based row sequence (free to add when the caller stages its rows), or the
loader first copies the input with a row number (pyarrow, one batch at a time). The engine cannot
number rows itself: its scans expose no file row number and `nextval()` returns one value per
vector. Rows dropped by `where` leave holes in the rel offsets, which is fine.

```python
engine_bucketed_copy(conn, "TX", "Account", "Account", "tx.parquet", "src", "dst",
                     ["amount", "ts", "kind"], "graph.lbug.work", rel_col="rn")
```

Tests (build with `make python-bundled`, then put `tools/python_api/build` on `PYTHONPATH`):

- `python test_bucketed_load.py <work_dir> [duckdb|engine]` compares a bucketed load, extend, deletes and updates with a stock COPY.
- `python test_bundled_extensions.py` checks FTS, PAGE_RANK and WCC without INSTALL, with kuzu imported in the same process.
