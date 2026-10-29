"""One benchmark phase on one engine build; appends a JSON line to <results>.

usage: bench.py <engine:upstream|fork> <phase:load|query|extend|delete|query-after-edits> <data_dir> <db_path> <results> [buffer_pool_gb]

Both engines import `ladybug`; run.sh points PYTHONPATH at the build under test. The only difference in what
is run: above 25M rels the fork loads and extends them with scripts/bucketed_load, upstream always with one COPY.
"""
import json
import os
import random
import resource
import shutil
import sys
import time

import ladybug

engine, phase, data, db_path, results = sys.argv[1:6]
buffer_pool = int(float(sys.argv[6]) * 2**30) if len(sys.argv) > 6 else 8 * 2**30
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "bucketed_load"))

EDGES_PER_BUCKET = 25_000_000
# The bucketed loader runs DuckDB in this process: its memory counts in the fork's peak RSS.
DUCKDB_MEMORY = os.environ.get("BENCH_DUCKDB_MEMORY", "3GB")
out = {"engine": engine, "phase": phase, "data": os.path.basename(data.rstrip("/"))}


def open_db(read_only=False):
    db = ladybug.Database(db_path, buffer_pool_size=buffer_pool, read_only=read_only)
    return db, ladybug.Connection(db)


def timed(name, fn):
    t = time.time()
    fn()
    out[name] = round(time.time() - t, 2)


def copy_rels(conn, rel, src, dst, file, props, where=None):
    edges = f"'{data}/{file}'"
    flt = "" if where is None else f" WHERE {where}"
    rows = conn.execute(f"LOAD FROM '{data}/{file}'{flt} RETURN count(*)").get_next()[0]
    # Buckets bound memory but cost a key-resolution pass: like an application would, the fork buckets only above
    # one bucket's worth of rels and uses a single COPY below.
    if engine == "fork" and rows > EDGES_PER_BUCKET:
        from bucketed_load import bucketed_copy

        bucketed_copy(conn, rel, src, "id", dst, "id", edges, "src", "dst", props, db_path + ".work",
                      edges_per_bucket=EDGES_PER_BUCKET, duck_mem=DUCKDB_MEMORY, where=where, log=lambda *a: None)
    elif where is None:
        conn.execute(f"COPY {rel} FROM '{data}/{file}'")
    else:
        conn.execute(f"COPY {rel} FROM (LOAD FROM '{data}/{file}' WHERE {where} RETURN *)")


try:
    if phase == "load":
        for p in (db_path, db_path + ".wal"):
            shutil.rmtree(p, ignore_errors=True) if os.path.isdir(p) else os.path.exists(p) and os.remove(p)
        db, conn = open_db()
        conn.execute("CREATE NODE TABLE Customer(id STRING, name STRING, age INT32, segment STRING, PRIMARY KEY(id))")
        conn.execute("CREATE NODE TABLE Account(id STRING, kind STRING, balance DOUBLE, opened DATE, PRIMARY KEY(id))")
        conn.execute("CREATE REL TABLE OWNS(FROM Customer TO Account, role STRING)")
        conn.execute("CREATE REL TABLE TRANSFER(FROM Account TO Account, amount DOUBLE, ts TIMESTAMP, kind STRING)")
        t = time.time()
        conn.execute(f"COPY Customer FROM '{data}/customer.parquet'")
        conn.execute(f"COPY Account FROM '{data}/account.parquet'")
        copy_rels(conn, "OWNS", "Customer", "Account", "owns.parquet", ["role"])
        copy_rels(conn, "TRANSFER", "Account", "Account", "transfer.parquet", ["amount", "ts", "kind"])
        out["seconds"] = round(time.time() - t, 2)
        out["edges"] = conn.execute("MATCH ()-[t:TRANSFER]->() RETURN count(*)").get_next()[0]
    elif phase == "extend":
        db, conn = open_db()
        timed("seconds", lambda: copy_rels(conn, "TRANSFER", "Account", "Account", "transfer.parquet",
                                           ["amount", "ts", "kind"], where="amount < 100"))
        out["edges"] = conn.execute("MATCH ()-[t:TRANSFER]->() RETURN count(*)").get_next()[0]
    elif phase == "delete":
        db, conn = open_db()
        timed("seconds", lambda: conn.execute("MATCH ()-[t:TRANSFER]->() WHERE t.amount < 10 DELETE t"))
        out["edges"] = conn.execute("MATCH ()-[t:TRANSFER]->() RETURN count(*)").get_next()[0]
    elif phase.startswith("query"):
        db, conn = open_db(read_only=True)
        random.seed(1)
        n_accounts = conn.execute("MATCH (a:Account) RETURN count(*)").get_next()[0]
        n_customers = conn.execute("MATCH (c:Customer) RETURN count(*)").get_next()[0]
        accounts = ["A" + str(random.randrange(n_accounts)).zfill(12) for _ in range(200)]
        customers = ["C" + str(random.randrange(n_customers)).zfill(10) for _ in range(50)]

        def p50_ms(query, keys):
            latencies = []
            for key in keys:
                t = time.time()
                result = conn.execute(query, {"id": key})
                while result.has_next():
                    result.get_next()
                latencies.append(time.time() - t)
            return round(sorted(latencies)[len(latencies) // 2] * 1000, 2)

        def once_ms(query):
            t = time.time()
            result = conn.execute(query)
            while result.has_next():
                result.get_next()
            return round((time.time() - t) * 1000, 1)

        hub = "A" + "0".zfill(12)
        out["query_ms"] = {
            "point lookup": p50_ms("MATCH (a:Account {id: $id}) RETURN a.*", accounts),
            "1-hop out": p50_ms("MATCH (a:Account {id: $id})-[t:TRANSFER]->(b) RETURN b.id, t.amount LIMIT 100", accounts),
            "1-hop in": p50_ms("MATCH (a:Account {id: $id})<-[t:TRANSFER]-(b) RETURN b.id, t.amount LIMIT 100", accounts),
            "2-hop count": p50_ms("MATCH (c:Customer {id: $id})-[:OWNS]->(:Account)-[:TRANSFER]->(b) RETURN count(*)",
                                  customers),
            "super-node in": once_ms(f"MATCH (b:Account {{id: '{hub}'}})<-[t:TRANSFER]-() RETURN count(*), sum(t.amount)"),
            "filtered scan": once_ms("MATCH ()-[t:TRANSFER]->() WHERE t.amount > 999 AND t.kind = 'K1' RETURN count(*)"),
            "group by": once_ms("MATCH ()-[t:TRANSFER]->() RETURN t.kind, count(*), avg(t.amount)"),
            "super-node 2-hop": once_ms(
                f"MATCH (b:Account {{id: '{hub}'}})<-[:TRANSFER]-(:Account)<-[:OWNS]-(c) RETURN count(DISTINCT c)"),
        }
    conn.close()
    db.close()
    out["ok"] = True
except Exception as error:  # a failed phase is a result too (e.g. "Buffer pool is full")
    out["ok"] = False
    out["error"] = str(error)[:200]
out["peak_rss_gb"] = round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 2**20, 2)
if os.path.exists(db_path):
    out["disk_gb"] = round(os.path.getsize(db_path) / 2**30, 2)
with open(results, "a") as f:
    f.write(json.dumps(out) + "\n")
print(json.dumps(out))
