"""Bucketed rel COPY for the Ladybug fork (COPY options DIRECTION + RAW_OFFSETS).

DuckDB resolves keys to node offsets and assigns rel offsets once, then splits the edges into
node-group-aligned buckets: by src for the FWD CSR, by dst for the BWD CSR. Each bucket is one COPY
that writes only the node groups it covers, so RAM is bounded by the bucket, not the table, and an
extend writes only the node groups it touches."""
import os, shutil, time

NODE_GROUP_SIZE = 1 << 17


def node_offsets(conn, table, key, out_path):
    conn.execute(f"COPY (MATCH (n:{table}) RETURN n.{key} AS k, offset(id(n)) AS off) TO '{out_path}'")


def next_rel_offset(conn, rel):
    r = conn.execute(f"MATCH ()-[r:{rel}]->() RETURN max(offset(id(r)))").get_next()[0]
    return 0 if r is None else r + 1


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
