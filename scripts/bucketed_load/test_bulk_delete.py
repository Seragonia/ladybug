"""Bulk rel DELETE / DETACH DELETE / SET correctness against DuckDB, both directions, WAL replay,
checkpoint + reopen, rollback.  usage: del_test.py <work_dir>"""
import sys, os, shutil, duckdb
import ladybug as lb

w = sys.argv[1]
shutil.rmtree(w, ignore_errors=True); os.makedirs(w)
N, E, HUB = 300_000, 1_500_000, "A000000005"
d = duckdb.connect()
d.execute(f"COPY (SELECT 'A' || lpad(i::VARCHAR, 9, '0') AS id, i % 7 AS grp FROM range({N}) t(i)) TO '{w}/nodes.parquet'")
d.execute(f"""CREATE TABLE src AS
  SELECT 'A' || lpad((hash(i) % {int(N * .8)})::VARCHAR, 9, '0') AS s,
         'A' || lpad((hash(i * 31 + 7) % {int(N * .8)})::VARCHAR, 9, '0') AS t,
         i AS eid, (i % 1000)::DOUBLE AS amount, 'k' || (i % 5) AS kind
  FROM range({E}) r(i)
  UNION ALL  -- a hub with a long in-list and a node with a long out-list
  SELECT 'A' || lpad((i % {int(N * .8)})::VARCHAR, 9, '0'), '{HUB}', {E} + i, (i % 1000)::DOUBLE, 'hub'
  FROM range(30000) r(i)
  UNION ALL
  SELECT 'A000000009', 'A' || lpad((i * 7 % {int(N * .8)})::VARCHAR, 9, '0'), {E} + 30000 + i, (i % 1000)::DOUBLE, 'out'
  FROM range(10000) r(i)""")
# loaded (checkpointed) part and an extension left in memory
d.execute(f"COPY (SELECT * FROM src WHERE eid % 10 <> 3) TO '{w}/base.parquet'")
d.execute(f"COPY (SELECT * FROM src WHERE eid % 10 = 3) TO '{w}/ext.parquet'")
d.execute("CREATE TABLE e AS SELECT * FROM src WHERE eid % 10 <> 3")

db = lb.Database(f"{w}/db.lbug", buffer_pool_size=2 << 30); c = lb.Connection(db)
def setup_conn():
    c.execute("CALL auto_checkpoint=false")
    c.execute("CALL force_checkpoint_on_close=false")
setup_conn()
c.execute("CREATE NODE TABLE A(id STRING, grp INT64, PRIMARY KEY(id))")
c.execute("CREATE REL TABLE T(FROM A TO A, eid INT64, amount DOUBLE, kind STRING)")
c.execute(f"COPY A FROM '{w}/nodes.parquet'")
c.execute(f"COPY T FROM '{w}/base.parquet'")
c.execute("CHECKPOINT")

def rows(q, **p):
    r = c.execute(q, p); out = []
    while r.has_next(): out.append(tuple(r.get_next()))
    return out

def check(tag):
    exp_fwd = sorted(d.execute("SELECT eid, s, t, amount, kind FROM e").fetchall())
    fwd = sorted(rows("MATCH (a:A)-[r:T]->(b:A) RETURN r.eid, a.id, b.id, r.amount, r.kind"))
    bwd = sorted(rows("MATCH (b:A)<-[r:T]-(a:A) RETURN r.eid, a.id, b.id, r.amount, r.kind"))
    assert fwd == exp_fwd, (tag, "fwd", len(fwd), len(exp_fwd), [x for x in fwd if x not in set(exp_fwd)][:3],
                            [x for x in exp_fwd if x not in set(fwd)][:3])
    assert bwd == exp_fwd, (tag, "bwd", len(bwd), len(exp_fwd))
    # rel ids agree across directions
    fi = sorted(rows("MATCH (a:A)-[r:T]->(b:A) RETURN r.eid, offset(id(r))"))
    bi = sorted(rows("MATCH (b:A)<-[r:T]-(a:A) RETURN r.eid, offset(id(r))"))
    assert fi == bi, (tag, "ids")
    got = rows("MATCH ()-[r:T]->() RETURN count(*), sum(r.amount), count(DISTINCT r.eid)")[0]
    exp = d.execute("SELECT count(*), sum(amount), count(DISTINCT eid) FROM e").fetchone()
    assert (got[0], got[2]) == (exp[0], exp[2]) and abs(got[1] - exp[1]) < 1e-6 * max(1, exp[1]), (tag, got, exp)
    # per-node degrees, both directions, the hub and the long out-list
    for k in (HUB, "A000000009", "A000000001", "A000123456"):
        o = rows("MATCH (a:A {id:$k})-[r:T]->() RETURN count(*)", k=k)[0][0]
        i = rows("MATCH (a:A {id:$k})<-[r:T]-() RETURN count(*)", k=k)[0][0]
        assert (o, i) == d.execute("SELECT (SELECT count(*) FROM e WHERE s=$k), (SELECT count(*) FROM e WHERE t=$k)",
                                   {"k": k}).fetchone(), (tag, k, o, i)
    print(tag, "ok", got[0], flush=True)

def both(cypher, sql):
    c.execute(cypher); d.execute(sql)

check("load")
# A COPY into a non-empty table is only durable once checkpointed (it writes no WAL records; with
# auto_checkpoint off it is lost on reopen), so checkpoint it; committed CREATEs then leave
# in-memory CSR rows behind the persistent ones, the hub's included, that the WAL does replay.
c.execute(f"COPY T FROM '{w}/ext.parquet'")
c.execute("CHECKPOINT")
d.execute("INSERT INTO e SELECT * FROM src WHERE eid % 10 = 3")
created = d.execute(f"""SELECT s, t, 8000000 + i AS eid, (i * 13 % 1000)::DOUBLE AS amount, 'new' AS kind FROM (
    SELECT 'A' || lpad((hash(i + 77) % {int(N * .8)})::VARCHAR, 9, '0') AS s,
           CASE WHEN i % 4 = 0 THEN '{HUB}' ELSE 'A' || lpad((hash(i + 99) % {int(N * .8)})::VARCHAR, 9, '0') END AS t, i
    FROM range(20000) r(i))""").fetchall()
ins = c.prepare("MATCH (a:A {id: $s}), (b:A {id: $t}) CREATE (a)-[:T {eid: $eid, amount: $amount, kind: $kind}]->(b)")
for k in range(0, len(created), 5000):
    c.execute("BEGIN TRANSACTION")
    for r in created[k:k + 5000]:
        c.execute(ins, dict(s=r[0], t=r[1], eid=r[2], amount=r[3], kind=r[4]))
    c.execute("COMMIT")
d.execute("INSERT INTO e SELECT * FROM (VALUES " + ",".join(f"('{r[0]}','{r[1]}',{r[2]},{r[3]},'{r[4]}')" for r in created) + ")")
check("extend")

# 1. one transaction: local inserts + a bulk delete covering persistent, in-memory and local rels
#    (> RelTable::DELETE_BATCH_SIZE = 524288 rels, so a batch is flushed mid-statement)
c.execute("BEGIN TRANSACTION")
c.execute("MATCH (a:A {id:'A000000001'}), (b:A {id:'A000000002'}) CREATE (a)-[:T {eid: 9000001, amount: 5, kind:'loc'}]->(b)")
c.execute(f"MATCH (a:A {{id:'A000000003'}}), (b:A {{id:'{HUB}'}}) CREATE (a)-[:T {{eid: 9000002, amount: 650, kind:'loc'}}]->(b)")
c.execute("MATCH (a:A)-[r:T]->(b:A) WHERE r.amount < 400 DELETE r")
n_in_txn = rows("MATCH ()-[r:T]->() RETURN count(*)")[0][0]
c.execute("COMMIT")
d.execute(f"INSERT INTO e VALUES ('A000000003', '{HUB}', 9000002, 650, 'loc')")
d.execute("DELETE FROM e WHERE amount < 400")
assert n_in_txn == d.execute("SELECT count(*) FROM e").fetchone()[0]
check("bulk delete")

# 2. rollback of a deleting transaction
c.execute("BEGIN TRANSACTION")
c.execute("MATCH ()-[r:T]->() WHERE r.amount >= 400 AND r.amount < 500 DELETE r")
inside = rows("MATCH ()-[r:T]->() RETURN count(*)")[0][0]
assert inside == d.execute("SELECT count(*) FROM e WHERE NOT (amount >= 400 AND amount < 500)").fetchone()[0]
c.execute("ROLLBACK")
check("rollback")

# 3. concentrated on the hub, from either side, and on a long out-list
both(f"MATCH (a:A)-[r:T]->(b:A {{id:'{HUB}'}}) WHERE r.amount < 600 DELETE r", f"DELETE FROM e WHERE t='{HUB}' AND amount < 600")
both(f"MATCH (b:A {{id:'{HUB}'}})<-[r:T]-(a:A) WHERE r.amount >= 900 DELETE r", f"DELETE FROM e WHERE t='{HUB}' AND amount >= 900")
both("MATCH (a:A {id:'A000000009'})-[r:T]->(b:A) WHERE r.amount < 700 DELETE r", "DELETE FROM e WHERE s='A000000009' AND amount < 700")
check("hub delete")

# 4. each rel matched three times, two rels per tuple, and a DELETE followed by RETURN (not batched)
both("MATCH (a:A)-[r:T]->(b:A) WHERE r.amount = 777 UNWIND [1, 2, 3] AS k DELETE r", "DELETE FROM e WHERE amount = 777")
both("MATCH (a:A)-[r:T]->(b:A)-[r2:T]->(x:A) WHERE r.amount = 778 AND r2.amount = 779 DELETE r, r2",
     """DELETE FROM e WHERE eid IN (SELECT e1.eid FROM e e1 JOIN e e2 ON e1.t = e2.s WHERE e1.amount = 778 AND e2.amount = 779
                                  UNION SELECT e2.eid FROM e e1 JOIN e e2 ON e1.t = e2.s WHERE e1.amount = 778 AND e2.amount = 779)""")
n = rows("MATCH ()-[r:T]->() WHERE r.amount = 780 DELETE r RETURN count(*)")[0][0]
assert n == d.execute("SELECT count(*) FROM e WHERE amount = 780").fetchone()[0]
d.execute("DELETE FROM e WHERE amount = 780")
check("dup / multi / return")

# 5. SET on rels (shares the row lookup) and DETACH DELETE of nodes, the hub among them
both("MATCH ()-[r:T]->() WHERE r.amount = 800 SET r.kind = 'set800'", "UPDATE e SET kind = 'set800' WHERE amount = 800")
both(f"MATCH (b:A {{id:'{HUB}'}})<-[r:T]-() WHERE r.amount = 650 SET r.amount = 651", f"UPDATE e SET amount = 651 WHERE t='{HUB}' AND amount = 650")
both("MATCH (a:A) WHERE a.grp = 3 AND a.id < 'A000060000' DETACH DELETE a",
     "DELETE FROM e WHERE (s < 'A000060000' AND CAST(substr(s, 2) AS INT) % 7 = 3) OR (t < 'A000060000' AND CAST(substr(t, 2) AS INT) % 7 = 3)")
both(f"MATCH (a:A {{id:'{HUB}'}}) DETACH DELETE a", f"DELETE FROM e WHERE s='{HUB}' OR t='{HUB}'")
check("set + detach")

# 6. WAL replay: close without checkpoint, reopen
c.close(); db.close()
db = lb.Database(f"{w}/db.lbug", buffer_pool_size=2 << 30); c = lb.Connection(db); setup_conn()
check("wal replay")
c.execute("CHECKPOINT"); c.close(); db.close()
db = lb.Database(f"{w}/db.lbug", buffer_pool_size=2 << 30); c = lb.Connection(db); setup_conn()
check("checkpoint + reopen")

# 7. delete on checkpointed data with deletions already merged, rollback, checkpoint, reopen
both("MATCH ()-[r:T]->() WHERE r.amount >= 950 DELETE r", "DELETE FROM e WHERE amount >= 950")
c.execute("BEGIN TRANSACTION")
c.execute("MATCH ()-[r:T]->() WHERE r.amount < 940 DELETE r")
c.execute("ROLLBACK")
c.execute("CHECKPOINT"); c.close(); db.close()
db = lb.Database(f"{w}/db.lbug", buffer_pool_size=2 << 30); c = lb.Connection(db); setup_conn()
check("second round")
print("ALL OK")
