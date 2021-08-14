# neo4j-arrow -- Data Science workflows 10-100x faster

> "“When you want to do something, do it right away. Do it when you can. 
> It’s the only way to live a life without regrets.” 
>   --Sonic The Hedgehog

![gotta-go-fast](./fast.gif)

This is some tire kicking of the [Apache Arrow](https://arrow.apache.org/)
project to see if Arrow can help solve a few rough spots for us.

## Why? My Problem Statements

1. **Data Science involves moving lots of "big data" around**, both into and 
   out of the graph. Some customers need to move millions of scalars (e.g. 
   community assignments) or even millions of vectors (e.g. 256-degree node 
   embeddings). This is traditionally a challenge for Cypher-based approaches.
2. **Data Science relies heavily on Python as the _lingua franca_**, but 
   Neo4j's Python driver has traditionally been ill-fitted for integration 
   with Neo4j. There's a general lack of integration with common libraries 
   like Pandas, NumPy, etc. As a result, PySpark + the Neo4j Spark Connector 
   are often recommended as an improvement.
3. **Data Science often involves batch processing** where algorithms run for 
   long periods of time. The Neo4j drivers, nor Neo4j itself, provide async 
   transaction/job capabilities.

### Problem 1: Moving "Big Data"

> “Only love that continues to flow in the face of anger, blame, and 
> indifference can be called love. All else is simply a **transaction.**” 
>   -- Vironika Tugaleva

Getting large amounts of data into Neo4j has been a challenge...and, honestly, 
this project is yet to address this matter. However, _getting data out en 
masse_, **can** be immediately addressed with Apache Arrow.

How?

1. Sidestepping the **transaction layer** of Neo4j, avoiding "PageCache thrash"
2. Using Arrow's more efficient (for fixed-width data types) **vector format 
   wire and in-memory format**
3. Exploiting support for things like `float[]` primitives over `double[]`

#### Dumping Data Today
Neo4j users needing to exfiltrate large quantities of node or relationship 
properties, such as node embeddings, have a few choices today:

1. Use a driver and Cypher to retrieve Node properties
2. Use a driver and Cypher to call procs like `gds.graph.streamNodeProperties`
3. Use APOC routines that write to files on the Neo4j file system
4. Use alternative integrations like the Neo4j Streams (Apache Kafka) 
   integration

In all the above cases, the user needs to either write orchestration code 
around our drivers (which subjectively is quite cumbersome and full of 
footguns) OR needs access to the filesystem along with additional Neo4j 
plugins like APOC, etc. In this latter case, users of Aura are effectively SOL.

We can do better!

#### Streaming data with Apache Arrow
Apache Arrow solves multiple problems directly and indirectly related to 
moving large quantities of data. Replacing Bolt as the wire-format and
leveraging a common API across languages (from Python to R to Java, etc.) it 
foremost provides a way to elegantly work with "big data" in a columnar 
fashion.

In many data export use cases I've found, the users need only a few 
properties exported, making columnar representations a good fit.

Arrow also includes the Flight framework, a combination of RPC (based on 
gRPC) and basic remote stream operations (get, put, list, etc.).

### Problem 2: Python as a Platform
Modern Data Science platforms expose Python as the de facto language the 
scientists learn for working with data. However, Python is used mostly as a 
friendly veil over the complex "computer science-y" things like memory 
allocation, zero-copy slices, etc. Underneath Pandas, NumPy, SciPy, etc. is 
a sea of C code (and sometimes C++, Fortran, etc.) specializing in efficiency.

How popular is Python? **VERY POPULAR.**

In a [2019 Kaggle survey](http://businessoverbroadway.com/2020/06/29/usage-of-programming-languages-by-data-scientists-python-grows-while-r-weakens/
), **87% of respondents** said they use Python on a regular basis.

![What programming languages do you use on a regular basis?](http://businessoverbroadway.com/wp-content/uploads/2020/06/Kaggle_Programming_2019.png)

#### Python and Neo4j
The Neo4j Python driver is implemented in pure Python. Python is known for 
being _effectively_ single threaded due to the 
[Global Interpreter Lock](https://en.wikipedia.org/wiki/Global_interpreter_lock)
(GIL) that impedes CPU-bound workloads from being spread across multiple 
cores. (One can navigate the GIL for IO-bound things like networking, disk, 
etc., but anything creating/destroying Python objects needs to hold the GIL 
to do so.)

Given the following program, [direct.py](./direct.py):

```python
#!/usr/bin/env python
import neo4j
from os import environ as env
from time import time

query = """
        UNWIND range(1, $rows) AS row
        RETURN row, [_ IN range(1, $dimension) | rand()] as fauxEmbedding
    """
params = {"rows": 1_000_000, "dimension": 128}

username = env.get('NEO4J_USERNAME', 'neo4j')
password = env.get('NEO4J_PASSWORD', 'password')
database = env.get('NEO4J_DATABASE', 'neo4j')
neo4j_url = env.get('NEO4J_URL', 'neo4j://localhost:7687')
fetch_size = int(env.get('BOLT_FETCH_SIZE', '10000'))

with neo4j.GraphDatabase.driver(neo4j_url, auth=(username, password)) as d:
    with d.session(fetch_size=fetch_size, database=database) as s:
        print(f"Starting query {query}")
        result = s.run(query, params)
        cnt = 0
        start = time()
        for row in result:
            cnt = cnt + 1
            if cnt % 25_000 == 0:
                print(f"Current Row @ {cnt:,}:\t[fields: {row.keys()}]")
        finish = time()
        print(f"Done! Time Delta: {round(finish - start, 2):,}s")
        print(f"Count: {cnt:,}, Rate: {round(cnt / (finish - start)):,} rows/s")
```

It simulates dumping 1 million, 128-degree node embedding feature vectors from 
the database using Cypher. Running it with the Python driver (on a GCP 
`e2-standard-4` 4 vCPU, 16 GB vm) results in:

* A CPU core pegged at ~98% utilization...
* Almost entirely in user-land (see below)...
* And performance 1/10th that using the equivalant code with the Java Driver.

```
...
Done! Time Delta: 238.21s
Count: 1,000,000, Rate: 4,198 rows/s

real    3m58.392s
user    3m53.301s
sys     0m4.964s
```

##### But what about py2neo?
[py2neo](https://github.com/technige/py2neo) is popular among the Neo4j 
Python community. (It technically has more "stars" on GitHub than the 
official driver. Whatever that means.)

The problem is: _it simply cannot handle bulk data!_

Given the following query:

```cypher
UNWIND range(1, 500000) AS row
RETURN row, [_ IN range(1, 256) | rand()] as fauxEmbedding
```

The following `py2neo` code *fails to complete* without raising an 
`IndexError` while processing the bolt stream:

```python
from py2neo import Graph
g = Graph('bolt://voutila-arrow-test:7687', auth=('neo4j', 'password'))
cnt = 0
query = """
  UNWIND range(1, 500000) AS row
  RETURN row, [_ IN range(1, 256) | rand()] as fauxEmbedding
"""
for result in g.query(query):
    cnt = cnt + 1
    if (cnt % 25000 == 0):
        print(f"row {cnt}")
```

#### So what about Apache Arrow?
PyArrow, the Python implementation of the Arrow API, avoids these 
GIL-related pitfalls by simply not using Python to deal with the networking 
and (de)serialization. The core implementation is predominantly C++ and 
outside the realm of the CPython interpreter.

For comparison, the same Cypher that generates fake embeddings takes the 
`neo4j-arrow` implementation, accessed via PyArrow, **only 11.5s** to return 
all the data and only using about 30% of a single CPU.

> **That's 20x faster than using the official Python driver.**

The best part about this: _it's a suboptimal implementation!_

In short, as will (hopefully) be explained later in this write-up, this 
implementation is using variable-width list vectors which add measurable 
overhead to Arrow's batching. If implemented with fixed-width, i.e. assuming 
a schema of arrays of floats or doubles for the embedding vectors, it can 
easily be _over 2x as fast_.

> _"But what about the Java Driver?"_ you may ask.
> 
> PyArrow is _as fast_, even with the suboptimal variable-width list vectors,
> as the Java Driver running the same Cypher. The Java driver consumes 
> multiple CPU cores and twice as much memory as the PyArrow. As you'll see 
> shortly, it's possible to _beat the Java Driver_ if we use known width 
> data structures.

### Problem 3: Batch Jobs
While not my core concern here, Arrow Flight offers an extensible RPC 
framework that in theory could satisfy some of the API around this (but not 
the persistence & Job control). The API, in my opinion, is important enough 
that it is worthwhile looking into how friendly we can make it.

While there's an active PRD around the concept of "jobs," it doesn't solve 
the data transmission issues.

The concept of Arrow Flight RPC "actions" along with the basic "get"/"set" 
stream features feel like really good building blocks.


...TBC...