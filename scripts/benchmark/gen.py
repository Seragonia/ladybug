"""Synthetic transaction graph: customers, accounts, OWNS (customer -> account), TRANSFER (account -> account).

30% of transfers go to a few heavily skewed destinations (super-nodes); rows are unsorted, like real exports.
usage: gen.py <scale> <out_dir>   (scale 1.0 = 21.6M customers, 118M accounts, 3.3B transfers; 0.01 is a laptop run)
"""
import os
import sys
import time

import duckdb

scale, out = float(sys.argv[1]), sys.argv[2]
os.makedirs(out, exist_ok=True)
C, A, T = int(21.6e6 * scale), int(118e6 * scale), int(3.3e9 * scale)
con = duckdb.connect()
con.execute("SET preserve_insertion_order=false")
t = time.time()
con.execute(f"""COPY (SELECT 'C' || lpad(i::VARCHAR, 10, '0') AS id, 'name_' || i AS name,
  (i % 90 + 18)::INT AS age, ['A','B','C'][i % 3 + 1] AS segment FROM range({C}) t(i))
  TO '{out}/customer.parquet' (FORMAT parquet, ROW_GROUP_SIZE 122880)""")
con.execute(f"""COPY (SELECT 'A' || lpad(i::VARCHAR, 12, '0') AS id, ['K1','K2','K3','K4'][i % 4 + 1] AS kind,
  round(random() * 100000, 2) AS balance, DATE '2000-01-01' + (i % 9000)::INT AS opened FROM range({A}) t(i))
  TO '{out}/account.parquet' (FORMAT parquet, ROW_GROUP_SIZE 122880)""")
con.execute(f"""COPY (SELECT 'C' || lpad((hash(i) % {C})::VARCHAR, 10, '0') AS src,
  'A' || lpad(i::VARCHAR, 12, '0') AS dst, ['OWNER','CO_OWNER'][(i % 7 = 0)::INT + 1] AS role FROM range({A}) t(i))
  TO '{out}/owns.parquet' (FORMAT parquet, ROW_GROUP_SIZE 122880)""")
con.execute(f"""COPY (SELECT 'A' || lpad((hash(i * 2) % {A})::VARCHAR, 12, '0') AS src,
  'A' || lpad((CASE WHEN hash(i * 3) % 10 < 7 THEN hash(i * 5) % {A}
                    ELSE floor({A} * pow((hash(i * 7) % 1000000007) / 1000000007.0, 4))::BIGINT END)::VARCHAR, 12, '0') AS dst,
  round(((hash(i * 11) % 100000) / 100.0), 2) AS amount,
  TIMESTAMP '2023-01-01' + to_seconds((hash(i * 13) % 94608000)::BIGINT) AS ts,
  ['K1','K2','K3','K4','K5'][(hash(i * 17) % 5)::INT + 1] AS kind
  FROM range({T}) t(i)) TO '{out}/transfer.parquet' (FORMAT parquet, ROW_GROUP_SIZE 122880)""")
print(f"customers={C} accounts={A} transfers={T} in {time.time() - t:.1f}s")
