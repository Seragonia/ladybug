"""Bucketed rel COPY for the Ladybug fork (COPY options DIRECTION + RAW_OFFSETS).

DuckDB resolves keys to node offsets and assigns rel offsets once, then splits the edges into
node-group-aligned buckets: by src for the FWD CSR, by dst for the BWD CSR. Each bucket is one COPY
that writes only the node groups it covers, so RAM is bounded by the bucket, not the table, and an
extend writes only the node groups it touches. engine_bucketed_copy does the same without DuckDB."""
import contextlib, os, shutil, threading, time

NODE_GROUP_SIZE = 1 << 17


def node_offsets(conn, table, key, out_path):
    conn.execute(f"COPY (MATCH (n:{table}) RETURN n.{key} AS k, offset(id(n)) AS off) TO '{out_path}'")


def next_rel_offset(conn, rel):
    r = conn.execute(f"MATCH ()-[r:{rel}]->() RETURN max(offset(id(r)))").get_next()[0]
    return 0 if r is None else r + 1


def scalar(conn, query):
    return conn.execute(query).get_next()[0]


@contextlib.contextmanager
def one_checkpoint(conn, rel, base):
    """Checkpoints once after the bucket COPYs of a load into an empty rel table, not after each.

    A COPY checkpoints when it commits, and each checkpoint syncs the data file: ~0.3-1 s a bucket,
    and much more while another process writes (the BWD split), since ext4 then syncs its writes
    too. Into an empty table every COPY writes its node groups straight to disk, so skipping the
    checkpoints between them keeps nothing more in memory. An extend keeps them: the rels it adds
    to existing node groups stay in memory until the next checkpoint."""
    fresh = base == 0 and scalar(conn, f"CALL storage_info('{rel}') RETURN count(*)") == 0
    defer = fresh and str(scalar(conn, "CALL current_setting('auto_checkpoint') RETURN *")) == "True"
    if defer:
        conn.execute("CALL auto_checkpoint=false")
    try:
        yield
    finally:
        if defer:
            conn.execute("CALL auto_checkpoint=true")
    if defer:
        conn.execute("CHECKPOINT")


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
    with one_checkpoint(conn, rel, base):
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
                  props, work_dir, edges_per_bucket=25_000_000, duck_mem="6GB", where=None, log=print):
    """edges: a quoted Parquet path or glob ('...'), filtered by `where` if given, or any DuckDB relation.

    For Parquet input a rel's offset is its row position in the files, which DuckDB reads in parallel;
    rows that `where` or a missing endpoint drops leave holes. Any other relation is numbered with a
    window, which DuckDB has to materialize whole, so it is much slower on large inputs."""
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
    # More threads than this split DuckDB's memory into buffers too small for the joins, which then spill.
    d.execute(f"SET memory_limit='{duck_mem}'; SET threads={min(os.cpu_count() or 1, 8)}; "
              f"SET temp_directory='{work_dir}/duck_tmp'; SET preserve_insertion_order=false")
    # Join on 64-bit key hashes: hash tables a quarter the size of string keys', so far fewer spills. If two
    # node keys share a hash, join on the keys. An edge key with no node can only match a node whose key has
    # the same hash (odds ~1e-11 per such edge at 100M nodes) instead of being dropped.
    maps = {}
    for name, path in (("src_map", src_map), ("dst_map", dst_map)):
        if path in maps.values():
            maps[name] = maps[next(n for n, p in maps.items() if p == path)]
            continue
        unique = d.execute(f"SELECT count(DISTINCT hash(k)) = count(*) FROM '{path}'").fetchone()[0]
        if unique:
            d.execute(f"COPY (SELECT hash(k) AS k, off FROM '{path}') TO '{path}.h.parquet'")
        maps[name] = f"{path}.h.parquet" if unique else path
    key = lambda name, col: f"hash(e.{col})" if maps[name].endswith(".h.parquet") else f"e.{col}"
    n_src = d.execute(f"SELECT max(off) + 1 FROM '{src_map}'").fetchone()[0] or 0
    n_dst = d.execute(f"SELECT max(off) + 1 FROM '{dst_map}'").fetchone()[0] or 0
    flt = f" WHERE {where}" if where else ""
    files = edges[1:-1] if edges.startswith("'") and edges.endswith("'") else None
    n_edges = d.execute(f"SELECT count(*) FROM {edges} e{flt}").fetchone()[0]
    k = max(1, -(-n_edges // edges_per_bucket))
    ceil = lambda a, b: -(-a // b)
    # A bucket spans whole node groups: its COPY then writes each of them once.
    src_span = NODE_GROUP_SIZE * max(1, ceil(ceil(n_src, NODE_GROUP_SIZE), k))
    dst_span = NODE_GROUP_SIZE * max(1, ceil(ceil(n_dst, NODE_GROUP_SIZE), k))
    cols = "".join(f", e.{p}" for p in props)
    t = time.time()
    if files:
        # A rel's offset is its row position in the files: the first row of each file, plus its row number.
        d.execute(f"""CREATE TEMP TABLE file_start AS SELECT file_name,
            sum(num_rows) OVER (ORDER BY file_name) - num_rows AS start FROM parquet_file_metadata('{files}')""")
        source = f"""read_parquet('{files}', filename=true, file_row_number=true) e
            JOIN file_start f ON f.file_name = e.filename"""
        rel_offset = f"{base} + f.start + e.file_row_number"
    else:
        source, rel_offset = f"{edges} e", f"{base} + row_number() OVER () - 1"
    # Resolve keys once, straight into the FWD buckets; edges whose endpoints are missing are dropped, as
    # COPY's key lookups would. The BWD buckets are then split from the FWD ones.
    d.execute(f"""COPY (SELECT s.off AS src, t.off AS dst, ({rel_offset})::BIGINT AS rel{cols}, s.off // {src_span} AS b
        FROM {source} JOIN '{maps["src_map"]}' s ON s.k = {key("src_map", src_col)}
        JOIN '{maps["dst_map"]}' t ON t.k = {key("dst_map", dst_col)}{flt})
        TO '{work_dir}/fwd' (FORMAT parquet, PARTITION_BY (b))""")
    log("resolve", round(time.time() - t, 1))
    # DuckDB splits the BWD buckets while the engine copies the FWD ones: neither uses every core.
    split_error = []

    def split_bwd():
        try:
            t = time.time()
            d.execute(f"""COPY (SELECT *, dst // {dst_span} AS b
                FROM read_parquet('{work_dir}/fwd/*/*.parquet', hive_partitioning=false))
                TO '{work_dir}/bwd' (FORMAT parquet, PARTITION_BY (b))""")
            log("bucket", round(time.time() - t, 1), "k", k)
        except Exception as error:  # raised after the FWD copies
            split_error.append(error)
        finally:
            d.close()

    splitter = threading.Thread(target=split_bwd)
    timings = {}

    def copy_buckets(name):
        for b in sorted(os.listdir(f"{work_dir}/{name}"), key=lambda s: int(s.split("=")[1])):
            t = time.time()
            conn.execute(f"COPY {rel} FROM '{work_dir}/{name}/{b}/*.parquet' "
                         f"(DIRECTION='{name}', RAW_OFFSETS=true)")
            timings[f"{name}_{b}"] = round(time.time() - t, 2)
            log(name, b, timings[f"{name}_{b}"])

    with one_checkpoint(conn, rel, base):
        splitter.start()
        try:
            copy_buckets("fwd")
        finally:
            splitter.join()
        if split_error:
            raise split_error[0]
        copy_buckets("bwd")
    shutil.rmtree(work_dir, ignore_errors=True)
    return {"edges": n_edges, "buckets": k, "copies": timings, "total_s": round(time.time() - t0, 1)}
