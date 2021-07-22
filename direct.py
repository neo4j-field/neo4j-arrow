#!/usr/bin/env python
import neo4j
from time import time

query = "UNWIND range(1, toInteger(1e7)) as n RETURN n"
auth = ('neo4j', 'password')
with neo4j.GraphDatabase.driver('neo4j://localhost:7687', auth=auth) as d:
    with d.session() as s:
        print(f"Starting query {query}")
        result = s.run(query)
        cnt = 0
        start = time()
        for row in result:
            cnt = cnt + 1
        finish = time()
        print(f"Done! Time Delta: {round(finish - start, 2):,}s")
        print(f"Count: {cnt:,}, Rate: {round(cnt / (finish - start)):,} rows/s")

