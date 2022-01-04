#!/usr/bin/env python
import neo4j
from time import time

query = """
        UNWIND range(1, $rows) AS row
        RETURN row, [_ IN range(1, $dimension) | rand()] as fauxEmbedding
    """
params = {"rows": 1_000_000, "dimension": 128}

auth = ('neo4j', 'password')
with neo4j.GraphDatabase.driver('neo4j://localhost:7687', auth=auth) as d:
    with d.session(fetch_size=10_000) as s:
        print(f"Starting query {query}")
        result = s.run(query, params)
        cnt = 0
        start = time()
        for row in result:
            cnt = cnt + 1
            if cnt % 50_000 == 0:
                print(f"Current Row @ {cnt:,}:\t[fields: {row.keys()}]")
        finish = time()
        print(f"Done! Time Delta: {round(finish - start, 2):,}s")
        print(f"Count: {cnt:,}, Rate: {round(cnt / (finish - start)):,} rows/s")
