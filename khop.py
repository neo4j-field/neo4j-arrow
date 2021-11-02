#!/usr/bin/env python
import neo4j_arrow as na
from time import time
import os, sys

graph = sys.argv[-1]
db = sys.argv[-2]
HOST = os.environ.get('HOST', 'localhost')
print(f"using graph {graph} in db {db} on host {HOST}")

client = na.Neo4jArrow('neo4j', 'password', (HOST, 9999))

t = client.khop(graph, database=db)
rows, nbytes, log_cnt, log_nbytes = 0, 0, 0, 0

start = time()
original_start = start
for (chunk, _) in client.stream(t):
    rows = rows + chunk.num_rows
    log_cnt = log_cnt + chunk.num_rows
    nbytes = nbytes + chunk.nbytes
    log_nbytes = log_nbytes + chunk.nbytes

    if log_cnt >= 1_000_000:
        delta = time() - start
        rate = int(log_nbytes / delta)
        print(f"@ {rows:,} rows, {(nbytes >> 20):,} MiB, rate: {(rate >> 20):,} MiB/s, {int(log_cnt / delta):,} row/s")
        log_cnt, log_nbytes = 0, 0
        start = time()

delta = time() - original_start
rate = int(nbytes / delta)
print(f"consumed {rows:,}, {(nbytes >> 20):,} MiB")
print(f"rate: {(rate >> 20):,} MiB/s, {int(rows / delta):,}")


