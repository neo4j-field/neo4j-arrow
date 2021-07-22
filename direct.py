#!/usr/bin/env python
import neo4j
from time import time

query = "UNWIND range(1, 1000000) as n RETURN n"
auth = ('neo4j', 'password')
with neo4j.GraphDatabase.driver('neo4j://192.168.1.42:7687', auth=auth) as d:
    with d.session() as s:
        print(f"starting query {query}")
        result = s.run(query)
        start = time()
        for row in result:
            pass
        finish = time()
        print(f"done! summary: {result.consume()}")
        print(f"time delta: {finish - start}")

