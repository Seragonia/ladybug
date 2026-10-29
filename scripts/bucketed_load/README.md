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

Needs `duckdb`. Tests (build with `make python-bundled`, then put `tools/python_api/build` on `PYTHONPATH`):

- `python test_bucketed_load.py <work_dir>` compares a bucketed load, extend, deletes and updates with a stock COPY.
- `python test_bundled_extensions.py` checks FTS, PAGE_RANK and WCC without INSTALL, with kuzu imported in the same process.
