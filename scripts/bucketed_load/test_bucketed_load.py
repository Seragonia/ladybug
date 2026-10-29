"""Correctness of the fork's bucketed rel COPY against a stock COPY of the same edges.
usage: test_fork.py <work_dir>"""
import sys, os, shutil, random, duckdb
import ladybug as lb
sys.path.insert(0, os.path.dirname(__file__))
from bucketed_load import bucketed_copy

w = sys.argv[1]
shutil.rmtree(w, ignore_errors=True); os.makedirs(w)
N, E = 400_000, 2_000_000  # 4 node groups; nodes past 80% get no edge at all
d = duckdb.connect()
d.execute(f"COPY (SELECT 'A' || lpad(i::VARCHAR, 9, '0') AS id, i % 7 AS grp FROM range({N}) t(i)) TO '{w}/nodes.parquet'")
d.execute(f"""COPY (SELECT 'A' || lpad((hash(i) % {int(N * .8)})::VARCHAR, 9, '0') AS s,
                           'A' || lpad((hash(i * 31 + 7) % {int(N * .8)})::VARCHAR, 9, '0') AS t,
                           (i % 1000)::DOUBLE AS amount, 'k' || (i % 5) AS kind
              FROM range({E}) r(i)
              UNION ALL  -- a hub with a list long enough for the row lookup cache
              SELECT 'A' || lpad((i % {int(N * .8)})::VARCHAR, 9, '0'), 'A000000005', (i % 1000)::DOUBLE, 'hub'
              FROM range(20000) r(i)) TO '{w}/edges.parquet'""")

def fresh(name):
    db = lb.Database(f"{w}/{name}.lbug"); c = lb.Connection(db)
    c.execute("CREATE NODE TABLE A(id STRING, grp INT64, PRIMARY KEY(id))")
    c.execute("CREATE REL TABLE T(FROM A TO A, amount DOUBLE, kind STRING)")
    c.execute(f"COPY A FROM '{w}/nodes.parquet'")
    return db, c

db1, c1 = fresh("stock"); c1.execute(f"COPY T FROM '{w}/edges.parquet'")
db2, c2 = fresh("fork")
print(bucketed_copy(c2, "T", "A", "id", "A", "id", f"'{w}/edges.parquet'", "s", "t", ["amount", "kind"],
                    f"{w}/work", edges_per_bucket=300_000, log=lambda *a: None))

def rows(c, q, **p):
    r = c.execute(q, p); out = []
    while r.has_next(): out.append(tuple(r.get_next()))
    return sorted(out)

checks = [
    "MATCH ()-[r:T]->() RETURN count(*), sum(r.amount)",
    "MATCH (a:A)-[r:T]->(b:A) RETURN a.grp, b.grp, r.kind, count(*), sum(r.amount)",
    "MATCH (a:A) RETURN count(*)",
]
random.seed(3)
keys = ["A" + str(random.randrange(N)).zfill(9) for _ in range(150)] + ["A" + str(N - 1).zfill(9)]
def compare(tag):
    for q in checks:
        assert rows(c1, q) == rows(c2, q), (tag, q, rows(c1, q)[:3], rows(c2, q)[:3])
    for k in keys:
        for q in ("MATCH (a:A {id:$k})-[r:T]->(b:A) RETURN b.id, r.amount, r.kind",
                  "MATCH (a:A {id:$k})<-[r:T]-(b:A) RETURN b.id, r.amount, r.kind",
                  "MATCH (a:A {id:$k})-[r:T]->(b:A)-[r2:T]->(c:A) RETURN count(*)"):
            assert rows(c1, q, k=k) == rows(c2, q, k=k), (tag, q, k)
    # FWD and BWD must agree on rel ids: every edge seen forward is found backward with the same id.
    fw = rows(c2, "MATCH (a:A)-[r:T]->(b:A) WHERE a.grp = 3 RETURN a.id, b.id, offset(id(r))")
    bw = rows(c2, "MATCH (b:A)<-[r:T]-(a:A) WHERE a.grp = 3 RETURN a.id, b.id, offset(id(r))")
    assert fw == bw and len(fw) > 0, (tag, len(fw), len(bw))
    print(tag, "ok", len(fw))

compare("load")
# Many deletes and updates through one long list, checked against DuckDB rather than another DB of this build.
hub_in = "MATCH (b:A {id:'A000000005'})<-[r:T]-(a:A) RETURN count(*), sum(r.amount)"
expected = d.execute(f"SELECT count(*), sum(amount) FROM '{w}/edges.parquet' WHERE t = 'A000000005' AND amount >= 300").fetchone()
for c in (c1, c2):
    c.execute("MATCH (a:A)-[r:T]->(b:A {id:'A000000005'}) WHERE r.amount < 300 DELETE r")
    c.execute("MATCH (a:A)-[r:T]->(b:A {id:'A000000005'}) WHERE r.amount = 400 SET r.kind = 'four hundred'")
    got = rows(c, hub_in)[0]
    assert (got[0], got[1]) == (expected[0], expected[1]), (got, expected)
    assert rows(c, "MATCH (a:A)-[r:T]->(b:A {id:'A000000005'}) WHERE r.kind = 'four hundred' RETURN count(*)")[0][0] == \
        d.execute(f"SELECT count(*) FROM '{w}/edges.parquet' WHERE t = 'A000000005' AND amount = 400").fetchone()[0]
compare("hub delete+update")
for c in (c1, c2):  # extend with the stock path, delete through both directions, update
    c.execute(f"COPY T FROM (LOAD FROM '{w}/edges.parquet' WHERE amount < 20 RETURN *)")
    c.execute("CREATE (:A {id:'Z1', grp: 3})")
    c.execute("MATCH (a:A {id:'Z1'}), (b:A {id:'A000000001'}) CREATE (a)-[:T {amount: 5, kind:'z'}]->(b)")
    c.execute("MATCH (a:A)-[r:T]->(b:A) WHERE r.amount = 7 DELETE r")
    c.execute("MATCH (a:A)<-[r:T]-(b:A) WHERE r.amount = 8 AND a.grp = 2 DELETE r")
    c.execute("MATCH ()-[r:T]->() WHERE r.amount = 9 SET r.kind = 'nine'")
keys.append("Z1")
compare("extend+delete+update")
for c, db in ((c1, db1), (c2, db2)):
    c.execute("CHECKPOINT"); c.close(); db.close()
db1 = lb.Database(f"{w}/stock.lbug"); c1 = lb.Connection(db1)
db2 = lb.Database(f"{w}/fork.lbug"); c2 = lb.Connection(db2)
compare("reopen")
# A second bucketed load on top of existing data (EXTEND) must append, not collide.
extra = f"(SELECT * FROM '{w}/edges.parquet' LIMIT 100000)"
c1.execute(f"COPY T FROM (LOAD FROM '{w}/edges.parquet' WHERE amount >= 990 RETURN *)")
bucketed_copy(c2, "T", "A", "id", "A", "id", f"(SELECT * FROM '{w}/edges.parquet' WHERE amount >= 990)", "s", "t",
              ["amount", "kind"], f"{w}/work", edges_per_bucket=300_000, log=lambda *a: None)
compare("bucketed extend")
print("ALL OK")
