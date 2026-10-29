"""MVCC visibility of rel and node deletions against DuckDB expectations: a read transaction
started before a delete keeps seeing the rows, rollback restores them, deletions by several
transactions in the same vectors, checkpoint with readers open, checkpoint + reopen, and deletes
on reopened (deserialized) deletion info.  usage: sparse_mvcc.py <work_dir>"""
import sys, os, shutil, duckdb
import ladybug as lb

w = sys.argv[1]
shutil.rmtree(w, ignore_errors=True); os.makedirs(w)
N, E = 300_000, 900_000
d = duckdb.connect()
d.execute(f"CREATE TABLE n AS SELECT 'A' || lpad(i::VARCHAR, 9, '0') AS id, i % 7 AS grp FROM range({N}) t(i)")
d.execute(f"""CREATE TABLE e AS
  SELECT 'A' || lpad((hash(i) % {N})::VARCHAR, 9, '0') AS s,
         'A' || lpad((hash(i * 31 + 7) % {N})::VARCHAR, 9, '0') AS t,
         i AS eid, (i * 7919 % 1000)::DOUBLE AS amount FROM range({E}) r(i)""")
d.execute(f"COPY n TO '{w}/nodes.parquet'")
d.execute(f"COPY e TO '{w}/edges.parquet'")

dbp = f"{w}/db.lbug"
def open_db():
    db = lb.Database(dbp, buffer_pool_size=1 << 30)
    c = lb.Connection(db)
    c.execute("CALL auto_checkpoint=false")
    return db, c
db, c = open_db()
c.execute("CREATE NODE TABLE A(id STRING, grp INT64, PRIMARY KEY(id))")
c.execute("CREATE REL TABLE T(FROM A TO A, eid INT64, amount DOUBLE)")
c.execute(f"COPY A FROM '{w}/nodes.parquet'")
c.execute(f"COPY T FROM '{w}/edges.parquet'")
c.execute("CHECKPOINT")

def rows(conn, q):
    r = conn.execute(q); out = []
    while r.has_next(): out.append(tuple(r.get_next()))
    return out

def snap():
    """Current DuckDB expectation: (sorted rel tuples, sorted node ids)."""
    return (sorted(d.execute("SELECT eid, s, t, amount FROM e").fetchall()),
            sorted(x[0] for x in d.execute("SELECT id FROM n").fetchall()))

def check(tag, conn, exp):
    erels, enodes = exp
    fwd = sorted(rows(conn, "MATCH (a:A)-[r:T]->(b:A) RETURN r.eid, a.id, b.id, r.amount"))
    bwd = sorted(rows(conn, "MATCH (b:A)<-[r:T]-(a:A) RETURN r.eid, a.id, b.id, r.amount"))
    assert fwd == erels, (tag, "fwd", len(fwd), len(erels))
    assert bwd == erels, (tag, "bwd", len(bwd), len(erels))
    cnt = rows(conn, "MATCH ()-[r:T]->() RETURN count(*)")[0][0]
    assert cnt == len(erels), (tag, "count", cnt, len(erels))
    nodes = sorted(x[0] for x in rows(conn, "MATCH (a:A) RETURN a.id"))
    assert nodes == enodes, (tag, "nodes", len(nodes), len(enodes))
    ncnt = rows(conn, "MATCH (a:A) RETURN count(*)")[0][0]
    assert ncnt == len(enodes), (tag, "node count", ncnt, len(enodes))
    # point lookups by primary key (index + node version info)
    for k in enodes[:3] + enodes[-3:]:
        assert rows(conn, f"MATCH (a:A {{id:'{k}'}}) RETURN count(*)")[0][0] == 1, (tag, k)
    print(tag, "ok", len(erels), len(enodes), flush=True)

def reader():
    r = lb.Connection(db)
    r.execute("BEGIN TRANSACTION READ ONLY")
    return r

def both(cypher, sql):
    c.execute(cypher); d.execute(sql)

S0 = snap()
check("load", c, S0)

# 1. a reader started before a committed delete still sees the rows
r1 = reader(); check("r1 before", r1, S0)
both("MATCH ()-[r:T]->() WHERE r.amount < 10 DELETE r", "DELETE FROM e WHERE amount < 10")
S1 = snap()
check("writer after d1", c, S1); check("r1 after d1", r1, S0)

# 2. a second deleting transaction in the same vectors (rows deleted at two versions), with
#    readers at both snapshots open
r2 = reader(); check("r2", r2, S1)
both("MATCH (b:A)<-[r:T]-(a:A) WHERE r.amount >= 10 AND r.amount < 20 DELETE r",
     "DELETE FROM e WHERE amount >= 10 AND amount < 20")
S2 = snap()
check("writer after d2", c, S2); check("r1 after d2", r1, S0); check("r2 after d2", r2, S1)

# 3. rollback of a delete spanning vectors with committed deletions of two versions
c.execute("BEGIN TRANSACTION")
c.execute("MATCH ()-[r:T]->() WHERE r.amount >= 20 AND r.amount < 120 DELETE r")
n_in = rows(c, "MATCH ()-[r:T]->() RETURN count(*)")[0][0]
assert n_in == d.execute("SELECT count(*) FROM e WHERE NOT (amount >= 20 AND amount < 120)").fetchone()[0]
check("r1 during uncommitted", r1, S0)
c.execute("ROLLBACK")
check("writer after rollback", c, S2); check("r2 after rollback", r2, S1)

# 4. checkpoint with readers open (retained deletions keep their commit timestamps)
try:
    c.execute("CHECKPOINT")
    print("checkpoint with open readers ran", flush=True)
except Exception as ex:
    print("checkpoint with open readers refused:", str(ex)[:120], flush=True)
check("writer after ckpt", c, S2); check("r1 after ckpt", r1, S0); check("r2 after ckpt", r2, S1)
r1.execute("COMMIT"); r2.execute("COMMIT"); r1.close(); r2.close()
c.execute("CHECKPOINT")
check("after ckpt", c, S2)
c.close(); db.close(); db, c = open_db()
check("reopen 1", c, S2)

# 5. node deletes: DETACH DELETE with a reader open, rollback, checkpoint + reopen
r3 = reader()
both("MATCH (a:A) WHERE a.grp = 3 AND a.id < 'A000150000' DETACH DELETE a",
     "DELETE FROM e WHERE (s < 'A000150000' AND CAST(substr(s, 2) AS INT) % 7 = 3) OR (t < 'A000150000' AND CAST(substr(t, 2) AS INT) % 7 = 3); "
     "DELETE FROM n WHERE id < 'A000150000' AND grp = 3")
S3 = snap()
check("writer after detach", c, S3); check("r3 after detach", r3, S2)
c.execute("BEGIN TRANSACTION")
c.execute("MATCH (a:A) WHERE a.grp = 5 DETACH DELETE a")
c.execute("ROLLBACK")
check("after node rollback", c, S3); check("r3 after node rollback", r3, S2)
r3.execute("COMMIT"); r3.close()
c.execute("CHECKPOINT"); c.close(); db.close(); db, c = open_db()
check("reopen 2", c, S3)

# 6. deletes on reopened deletion info (stored deletions + a new transaction in the same vectors),
#    whole vectors of nodes, a reader, rollback, WAL replay, checkpoint + reopen
r4 = reader()
both("MATCH (a:A) WHERE a.grp = 4 AND a.id < 'A000150000' DETACH DELETE a",
     "DELETE FROM e WHERE (s < 'A000150000' AND CAST(substr(s, 2) AS INT) % 7 = 4) OR (t < 'A000150000' AND CAST(substr(t, 2) AS INT) % 7 = 4); "
     "DELETE FROM n WHERE id < 'A000150000' AND grp = 4")
both("MATCH (a:A) WHERE a.id >= 'A000200000' AND a.id < 'A000210000' DETACH DELETE a",
     "DELETE FROM e WHERE (s >= 'A000200000' AND s < 'A000210000') OR (t >= 'A000200000' AND t < 'A000210000'); "
     "DELETE FROM n WHERE id >= 'A000200000' AND id < 'A000210000'")
both("MATCH ()-[r:T]->() WHERE r.amount >= 500 AND r.amount < 510 DELETE r",
     "DELETE FROM e WHERE amount >= 500 AND amount < 510")
S4 = snap()
check("writer after round 2", c, S4); check("r4 after round 2", r4, S3)
c.execute("BEGIN TRANSACTION")
c.execute("MATCH (a:A) WHERE a.grp = 1 AND a.id < 'A000250000' DETACH DELETE a")
c.execute("MATCH ()-[r:T]->() WHERE r.amount >= 600 DELETE r")
c.execute("ROLLBACK")
check("after rollback 2", c, S4); check("r4 after rollback 2", r4, S3)
r4.execute("COMMIT"); r4.close()
c.close(); db.close(); db, c = open_db()
check("wal replay", c, S4)
c.execute("CHECKPOINT"); c.close(); db.close(); db, c = open_db()
check("reopen 3", c, S4)
print("ALL OK")
