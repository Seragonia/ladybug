"""Fork: FTS and algo work without INSTALL/LOAD, and kuzu + ladybug share one process."""
import os, shutil, tempfile
import kuzu, ladybug  # both, in this order: the pybind clash broke the second import
d = tempfile.mkdtemp()
db = ladybug.Database(os.path.join(d, "x.lb")); c = ladybug.Connection(db)
c.execute("CREATE NODE TABLE P(id INT64, name STRING, PRIMARY KEY(id))")
c.execute("CREATE REL TABLE K(FROM P TO P)")
c.execute("UNWIND range(0, 9) AS i CREATE (:P {id: i, name: 'person ' + CAST(i AS STRING) + (CASE WHEN i % 2 = 0 THEN ' alpha' ELSE '' END)})")
c.execute("UNWIND range(0, 8) AS i MATCH (a:P {id: i}), (b:P {id: i + 1}) CREATE (a)-[:K]->(b)")
c.execute("CALL CREATE_FTS_INDEX('P', 'idx', ['name'], stemmer := 'none')")
r = c.execute("CALL QUERY_FTS_INDEX('P', 'idx', 'alpha') RETURN node.id ORDER BY node.id").get_as_df()
assert r.iloc[:, 0].tolist() == [0, 2, 4, 6, 8], r
c.execute("CALL project_graph('G', ['P'], ['K'])")
pr = c.execute("CALL page_rank('G') RETURN node.id, rank").get_as_df()
wcc = c.execute("CALL weakly_connected_components('G') RETURN node.id, group_id").get_as_df()
assert len(pr) == 10 and wcc["group_id"].nunique() == 1, (pr, wcc)
k = kuzu.Connection(kuzu.Database(os.path.join(d, "x.kz")))
assert k.execute("RETURN 41 + 1").get_next() == [42]
print("ext ok: fts", len(r), "page_rank", len(pr), "wcc groups", wcc["group_id"].nunique(), "| kuzu", kuzu.__version__, "+ ladybug ok")
shutil.rmtree(d)
