# `neo4j-arrow` OR _how to go >10x faster than with Neo4j and Python_

This is some tire kicking of the [Apache Arrow](https://arrow.apache.org/) project to see if Arrow can help solve a few rough spots for us:


## Why? My Problem Statements

1. Python driver performance in Data Science use cases that require reading a LOT of data from a Neo4j database (think dumping node embeddings at scale)...it's hellishly slow compared to our Java driver.
2. Needing to handle "job-like" processing where the query could take an unknown amount of time to execute before results are returned...can we decouple the client from the transaction?
3. Make it a more "native" Python data science experience, integrating better with NumPy and Pandas...this is just a gap in our current out-of-box experience today.

### 1. Python Driver Perf

Trying to operationalize your GDS deployment? You need to get data out...often at scale...and often into PySpark or other Python related environments. The Python driver, being written in pure Python, seems to have a bottleneck in how fast it can process bolt messages off the wire and transform them into Python data types.

PyArrow uses, I believe, a C++ Arrow backend...like how Numpy, etc. work...so all the data wranging is done outside Python and outside the GIL.

This native backend should make things faster.

### 2. Jobs

While there's an active PRD around the concept of "jobs," it doesn't solve the data transmission issues.

While not my core concern here, Arrow Flight offers an extensible RPC framework that in theory could satisfy some of the API around this (but not the persistence & Job control). The API, in my opinion, is important enough that it is worthwhile looking into how friendly we can make it.

The concept of Arrow Flight RPC "actions" along with the basic "get"/"set" stream features feel like really good building blocks.

### 3. Native Python Data Science DX

People love Py2neo because it's less hoops and hurdles to weave Python driver code into their data engineering or analytics code in platforms like Databricks, Dataiku, etc.

PyArrow natively integrates with both NumPy and Pandas...allowing for simple conversions to/from their respective data formats like DataFrames.

The built-in integration in PyArrow should reduce the steps required to go from "cypher query" to "data frame", sort of like how the Neo4j Spark Connector expedites that as well.

Plus, Apache Arrow seems to be incorporated into Databricks (via Apache Spark) and other platforms these days either internally or potentially externally. Amazon is even using it for some of their data services.

## My Experiments

Given the silly query that generates fake embeddings with row "numbers":

```
UNWIND range(1, $rows) AS row
RETURN row, [_ IN range(1, $dimension) | rand()] as fauxEmbedding
```

How long does it take to get ALL the results to a Neo4j Python driver? (Or any driver for that matter?)

### Neo4j Python Performance
Given my measurements, in pure Python it can take **>200s** on my laptop to stream the results back using something trivial like:

```python
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

```
> See [direct.py](./direct.py) for the code

### Using PyArrow

PyArrow can't talk Bolt...**but**, it can talk to an Apache Arrow Flight service. IF one existed that has it's own way to talk to Neo4j, THEN it could proxy the Cypher transaction and facilitate rendering the Arrow stream back to the PyArrow client.

IF the Neo4j Java Driver is _measurably faster than_ the Neo4j Python driver, THEN it's possible the overhead of using it as a proxy is negligible.

What's the performance with PyArrow and a Java-Driver-Based Arrow Flight service? On my laptop with local db...it's **28s** to stream all the data back to the client.

> That's **~7x faster**, btw...and in other workloads with smaller row sizes I've alreayd seen **>10x** improvements.

What's the client code look like? Take a look at [client.py](./client.py).

## Sooooo...

The above PyArrow client stuff can be cleaned up and made friendlier...and the server side code can use some work. But this _feels_ promising. A **10x speedup** on my laptop gives me some hope!

If we can remove the Bolt proxy from the equation, it _might get even faster!_