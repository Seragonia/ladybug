"""Extend correctness: bucketed COPYs into populated CSR node groups, checked against DuckDB.
usage: test_extend.py <work_dir> [threads] [failgroup|-] [bp_mb]
  failgroup: with a build that has the test hook, the first CHECKPOINT after the first extend is
  made to fail in that node group (env LBUG_TEST_FAIL_CSR_CKPT_GROUP); it must leave the data
  readable and correct, and a retried CHECKPOINT must succeed."""
import sys, os, shutil, threading, duckdb
import ladybug as lb
sys.path.insert(0, os.path.dirname(__file__))
from bucketed_load import bucketed_copy

w = sys.argv[1]
threads = int(sys.argv[2]) if len(sys.argv) > 2 else 8
failgroup = sys.argv[3] if len(sys.argv) > 3 and sys.argv[3] != "-" else None
bp = {"buffer_pool_size": int(sys.argv[4]) << 20} if len(sys.argv) > 4 else {}  # MB
shutil.rmtree(w, ignore_errors=True); os.makedirs(w)
N, E = 1_100_000, 3_000_000  # 9 node groups, the last one partial; nodes past 90% get no edge
d = duckdb.connect()
d.execute(f"COPY (SELECT 'A' || lpad(i::VARCHAR, 9, '0') AS id, i % 7 AS grp FROM range({N}) t(i)) TO '{w}/nodes.parquet'")
d.execute(f"""COPY (SELECT 'A' || lpad((hash(i) % {int(N * .9)})::VARCHAR, 9, '0') AS s,
                           'A' || lpad((hash(i * 31 + 7) % {int(N * .9)})::VARCHAR, 9, '0') AS t,
                           (i % 1000)::DOUBLE AS amount, 'k' || (i % 5) AS kind, i AS seq
              FROM range({E}) r(i)
              UNION ALL  -- hubs: long lists that force region growth and redistribution
              SELECT 'A' || lpad((i % {int(N * .9)})::VARCHAR, 9, '0'), 'A00000000' || (i % 3)::VARCHAR,
                     (i % 1000)::DOUBLE, 'hub', {E} + i
              FROM range(60000) r(i)) TO '{w}/edges.parquet'""")
# Base: 70% of the edges. Extends: three disjoint hash slices of the remaining 30%.
base = f"(SELECT * FROM '{w}/edges.parquet' WHERE hash(seq) % 10 < 7)"
ext = [f"(SELECT * FROM '{w}/edges.parquet' WHERE hash(seq) % 10 = {k})" for k in (7, 8, 9)]
dbp = f"{w}/t.lbug"
db = lb.Database(dbp, max_num_threads=threads, **bp); c = lb.Connection(db)
c.execute("CREATE NODE TABLE A(id STRING, grp INT64, PRIMARY KEY(id))")
c.execute("CREATE REL TABLE T(FROM A TO A, amount DOUBLE, kind STRING)")
c.execute(f"COPY A FROM '{w}/nodes.parquet'")
load = lambda src, epb: bucketed_copy(c, "T", "A", "id", "A", "id", src, "s", "t", ["amount", "kind"],
                                      f"{w}/work", edges_per_bucket=epb, log=lambda *a: None)
load(base, 400_000)
c.execute("CHECKPOINT")

def rows(conn, q):
    r = conn.execute(q); out = []
    while r.has_next(): out.append(tuple(r.get_next()))
    return sorted(out)

live = []  # DuckDB-side predicate of the edges that should be present
def expect(q):
    return sorted(tuple(x) for x in d.execute(q.format(src=f"(SELECT * FROM '{w}/edges.parquet' WHERE {' OR '.join(live)})")).fetchall())

def compare(tag, conn=None):
    conn = conn or c
    checks = [
        ("MATCH ()-[r:T]->() RETURN count(*), sum(r.amount)",
         "SELECT count(*), sum(amount) FROM {src}"),
        ("MATCH (a:A)-[r:T]->(b:A) RETURN a.grp, b.grp, r.kind, count(*), sum(r.amount)",
         "SELECT a.grp, b.grp, e.kind, count(*), sum(e.amount) FROM {src} e JOIN '" + w +
         "/nodes.parquet' a ON a.id = e.s JOIN '" + w + "/nodes.parquet' b ON b.id = e.t GROUP BY ALL"),
        # Out- and in-degree distributions: catch rels attached to the wrong node.
        ("MATCH (a:A)-[r:T]->() WITH a, count(r) AS k RETURN k, count(*)",
         "SELECT k, count(*) FROM (SELECT s, count(*) k FROM {src} GROUP BY s) GROUP BY k"),
        ("MATCH (a:A)<-[r:T]-() WITH a, count(r) AS k RETURN k, count(*)",
         "SELECT k, count(*) FROM (SELECT t, count(*) k FROM {src} GROUP BY t) GROUP BY k"),
        ("MATCH (a:A)-[r:T]->(b:A) WHERE a.grp = 2 RETURN a.id, b.id, r.amount, r.kind",
         "SELECT s, t, amount, kind FROM {src} e JOIN '" + w + "/nodes.parquet' a ON a.id = e.s WHERE a.grp = 2"),
        ("MATCH (a:A)<-[r:T]-(b:A) WHERE a.grp = 5 RETURN b.id, a.id, r.amount, r.kind",
         "SELECT s, t, amount, kind FROM {src} e JOIN '" + w + "/nodes.parquet' a ON a.id = e.t WHERE a.grp = 5"),
    ]
    for q, dq in checks:
        got, exp = rows(conn, q), expect(dq)
        assert got == exp, (tag, q, len(got), len(exp), [x for x in got if x not in set(exp)][:3],
                            [x for x in exp if x not in set(got)][:3])
    # FWD and BWD agree on every rel id, and rel ids are unique.
    fw = rows(conn, "MATCH (a:A)-[r:T]->(b:A) RETURN a.id, b.id, offset(id(r))")
    bw = rows(conn, "MATCH (b:A)<-[r:T]-(a:A) RETURN a.id, b.id, offset(id(r))")
    assert fw == bw, (tag, len(fw), len(bw))
    assert len({x[2] for x in fw}) == len(fw), (tag, "duplicate rel ids")
    print(tag, "ok", len(fw), flush=True)

live.append("hash(seq) % 10 < 7")
compare("base")
c.execute("MATCH (a:A)-[r:T]->(b:A) WHERE r.amount = 3 DELETE r")  # persistent deletions in old groups
live[0] += " AND amount <> 3"
compare("delete")
if failgroup is not None:
    c.execute("CALL auto_checkpoint=false")
    os.environ["LBUG_TEST_FAIL_CSR_CKPT_GROUP"] = failgroup
load(ext[0], 300_000)
live.append("hash(seq) % 10 = 7")
compare("extend1")
if failgroup is not None:
    try:
        c.execute("CHECKPOINT"); raise SystemExit("CHECKPOINT was expected to fail")
    except Exception as e:
        print("checkpoint failed as injected:", str(e)[:120])
    del os.environ["LBUG_TEST_FAIL_CSR_CKPT_GROUP"]
    compare("after failed checkpoint")
    c.execute("CHECKPOINT")
    compare("after retried checkpoint")
    c.execute("CALL auto_checkpoint=true")

# A reader running through the next extend and its checkpoint sees either all or none of it.
before = rows(c, "MATCH ()-[r:T]->() RETURN count(*), sum(r.amount)")[0]
stop = threading.Event(); seen = set(); errors = []
def reader():
    rc = lb.Connection(db)
    while not stop.is_set():
        try: seen.add(rows(rc, "MATCH ()-[r:T]->() RETURN count(*), sum(r.amount)")[0])
        except Exception as e: errors.append(e); return
states = {before}  # each COPY of the loader commits on its own: FWD buckets land before BWD ones
class Recorder:
    def execute(self, q, *a):
        r = c.execute(q, *a)
        if q.startswith("COPY T "):
            states.add(rows(c, "MATCH ()-[r:T]->() RETURN count(*), sum(r.amount)")[0])
        return r
th = threading.Thread(target=reader); th.start()
bucketed_copy(Recorder(), "T", "A", "id", "A", "id", ext[1], "s", "t", ["amount", "kind"],
              f"{w}/work", edges_per_bucket=300_000, log=lambda *a: None)
stop.set(); th.join()
live.append("hash(seq) % 10 = 8")
assert not errors, errors
assert seen <= states and len(seen) > 1, (seen, states)
compare("extend2 with reader")
c.execute("CHECKPOINT"); c.close(); db.close()
db = lb.Database(dbp, max_num_threads=threads, **bp); c = lb.Connection(db)
compare("reopen")
load(ext[2], 10_000_000)  # one bucket for the whole table
live.append("hash(seq) % 10 = 9")
compare("extend3")
c.execute("CHECKPOINT"); c.close(); db.close()
db = lb.Database(dbp, max_num_threads=threads, **bp); c = lb.Connection(db)
compare("reopen2")
print("ALL OK")
