"""Bucketed rel COPY for the Ladybug fork (COPY options DIRECTION + RAW_OFFSETS).

DuckDB resolves keys to node offsets and assigns rel offsets once, then splits the edges into
node-group-aligned buckets: by src for the FWD CSR, by dst for the BWD CSR. Each bucket is one COPY
that writes only the node groups it covers, so RAM is bounded by the bucket, not the table, and an
extend writes only the node groups it touches. engine_bucketed_copy does the same without DuckDB."""
import os, shutil, time

NODE_GROUP_SIZE = 1 << 17


def node_offsets(conn, table, key, out_path):
    conn.execute(f"COPY (MATCH (n:{table}) RETURN n.{key} AS k, offset(id(n)) AS off) TO '{out_path}'")


def next_rel_offset(conn, rel):
    r = conn.execute(f"MATCH ()-[r:{rel}]->() RETURN max(offset(id(r)))").get_next()[0]
    return 0 if r is None else r + 1


def scalar(conn, query):
    return conn.execute(query).get_next()[0]


def add_row_numbers(path, out_path, column="_rn"):
    """Copies the Parquet input with a 0-based row number column, one record batch at a time."""
    import numpy as np, pyarrow as pa, pyarrow.dataset as ds, pyarrow.parquet as pq
    dataset = ds.dataset(path, format="parquet")
    schema = dataset.schema.append(pa.field(column, pa.int64()))
    n, pending = 0, []
    with pq.ParquetWriter(out_path, schema) as writer:
        # Row groups of ~1M rows: the engine scans a file one row group per thread.
        for batch in dataset.to_batches():
            rn = pa.array(np.arange(n, n + batch.num_rows, dtype=np.int64))
            pending.append(pa.RecordBatch.from_arrays(batch.columns + [rn], schema=schema))
            n += batch.num_rows
            if sum(b.num_rows for b in pending) >= 1 << 20:
                writer.write_table(pa.Table.from_batches(pending), row_group_size=1 << 20)
                pending = []
        if pending:
            writer.write_table(pa.Table.from_batches(pending), row_group_size=1 << 20)
    return out_path


def engine_bucketed_copy(conn, rel, src_table, dst_table, edges, src_col, dst_col, props, work_dir,
                         rel_col=None, where=None, edges_per_bucket=25_000_000, log=print):
    """bucketed_copy with the engine only (COPY options REL_OFFSETS, NODE_GROUP_BEGIN/END).

    edges is a Parquet file or directory, where an optional LOAD FROM filter on it. Every bucket
    re-scans the input and looks its bound keys up again, keeping only the rels of its node groups.
    A rel's offset must be the same in both directions, so it comes from the input: rel_col, a
    0-based sequence of the input rows (free to write when the caller stages its rows), or else
    the input is copied once with a row number added (pyarrow, streaming). The engine cannot number
    rows itself: its scans expose no file row number and nextval() gives one value per vector.
    Rel offsets continue after the largest in the table; rows the filter drops leave holes."""
    shutil.rmtree(work_dir, ignore_errors=True)
    os.makedirs(work_dir)
    t0 = time.time()
    base = next_rel_offset(conn, rel)
    if rel_col is None:
        edges, rel_col = add_row_numbers(edges, f"{work_dir}/rn.parquet"), "_rn"
    flt = f" WHERE {where}" if where else ""
    cols = "".join(f", {p}" for p in props)
    source = f"(LOAD FROM '{edges}'{flt} RETURN {src_col}, {dst_col}, {base} + {rel_col}{cols})"
    log("stage", round(time.time() - t0, 1))
    t = time.time()
    n_edges = scalar(conn, f"LOAD FROM '{edges}'{flt} RETURN count(*)")
    k = max(1, -(-n_edges // edges_per_bucket))
    log("count", round(time.time() - t, 1), "k", k)
    timings = {}
    for name, table in (("fwd", src_table), ("bwd", dst_table)):
        n_nodes = (scalar(conn, f"MATCH (n:{table}) RETURN max(offset(id(n)))") or 0) + 1
        n_groups = -(-n_nodes // NODE_GROUP_SIZE)
        span = max(1, -(-n_groups // k))
        for begin in range(0, n_groups, span):
            # The last bucket is open-ended so that no rel is ever left out.
            end = f", NODE_GROUP_END={begin + span}" if begin + span < n_groups else ""
            t = time.time()
            conn.execute(f"COPY {rel} FROM {source} (DIRECTION='{name}', REL_OFFSETS=true, "
                         f"NODE_GROUP_BEGIN={begin}{end})")
            timings[f"{name}_{begin}"] = round(time.time() - t, 2)
            log(name, begin, timings[f"{name}_{begin}"])
    shutil.rmtree(work_dir, ignore_errors=True)
    return {"edges": n_edges, "buckets": k, "copies": timings, "total_s": round(time.time() - t0, 1)}


def bucketed_copy(conn, rel, src_table, src_key, dst_table, dst_key, edges, src_col, dst_col,
                  props, work_dir, edges_per_bucket=25_000_000, duck_mem="6GB", log=print):
    import duckdb
    shutil.rmtree(work_dir, ignore_errors=True)
    os.makedirs(work_dir)
    t0 = time.time()
    src_map, dst_map = f"{work_dir}/src_map.parquet", f"{work_dir}/dst_map.parquet"
    node_offsets(conn, src_table, src_key, src_map)
    if (dst_table, dst_key) != (src_table, src_key):
        node_offsets(conn, dst_table, dst_key, dst_map)
    else:
        dst_map = src_map
    base = next_rel_offset(conn, rel)
    log("maps", round(time.time() - t0, 1))
    d = duckdb.connect()
    d.execute(f"SET memory_limit='{duck_mem}'; SET temp_directory='{work_dir}/duck_tmp'; "
              "SET preserve_insertion_order=false")
    n_src = d.execute(f"SELECT max(off) + 1 FROM '{src_map}'").fetchone()[0] or 0
    n_dst = d.execute(f"SELECT max(off) + 1 FROM '{dst_map}'").fetchone()[0] or 0
    n_edges = d.execute(f"SELECT count(*) FROM {edges}").fetchone()[0]
    k = max(1, -(-n_edges // edges_per_bucket))
    ceil = lambda a, b: -(-a // b)
    # A bucket spans whole node groups: its COPY then writes each of them once.
    src_span = NODE_GROUP_SIZE * max(1, ceil(ceil(n_src, NODE_GROUP_SIZE), k))
    dst_span = NODE_GROUP_SIZE * max(1, ceil(ceil(n_dst, NODE_GROUP_SIZE), k))
    cols = ", ".join(f"e.{p}" for p in props)
    t = time.time()
    # Resolve keys once; edges whose endpoints are missing are dropped, like COPY's lookups fail.
    d.execute(f"""COPY (SELECT s.off AS src, t.off AS dst,
        {base} + row_number() OVER () - 1 AS rel{', ' + cols if cols else ''}
        FROM {edges} e JOIN '{src_map}' s ON s.k = e.{src_col} JOIN '{dst_map}' t ON t.k = e.{dst_col})
        TO '{work_dir}/r.parquet' (FORMAT parquet)""")
    log("resolve", round(time.time() - t, 1))
    t = time.time()
    for name, col, span in (("fwd", "src", src_span), ("bwd", "dst", dst_span)):
        d.execute(f"COPY (SELECT *, {col} // {span} AS b FROM '{work_dir}/r.parquet') TO '{work_dir}/{name}' "
                  "(FORMAT parquet, PARTITION_BY (b))")
    os.remove(f"{work_dir}/r.parquet")
    d.close()
    log("bucket", round(time.time() - t, 1), "k", k)
    timings = {}
    for name in ("fwd", "bwd"):
        for b in sorted(os.listdir(f"{work_dir}/{name}"), key=lambda s: int(s.split("=")[1])):
            t = time.time()
            conn.execute(f"COPY {rel} FROM '{work_dir}/{name}/{b}/*.parquet' "
                         f"(DIRECTION='{name}', RAW_OFFSETS=true)")
            timings[f"{name}_{b}"] = round(time.time() - t, 2)
            log(name, b, timings[f"{name}_{b}"])
    shutil.rmtree(work_dir, ignore_errors=True)
    return {"edges": n_edges, "buckets": k, "copies": timings, "total_s": round(time.time() - t0, 1)}
