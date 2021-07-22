# `neo4j-arrow` OR _how to go 100x faster with Neo4j and Python_

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

PyArrow natively integrates wiht both NumPy and Pandas...allowing for simple conversions to/from their respective data formats like DataFrames.

The built in integration in PyArrow should reduce the steps required to go from "cypher query" to "data frame", sort of like how the Neo4j Spark Connector expedites that as well.

## My Experiments

Given the silly query:

```
// Return the numbers from 1 to 1M
UNWIND range(1, 1000000) as n RETURN n
```

How long does it take to get ALL the results to a Neo4j Python driver? (Or any driver for that matter?)

### Neo4j Python Performance
Given my measurements, in pure Python it can take **20s** on my laptop to stream the results back using something trivial like:

```python
#!/usr/bin/env python
import neo4j
from time import time

query = "UNWIND range(1, 1000000) as n RETURN n"
auth = ('neo4j', 'password')
with neo4j.GraphDatabase.driver('neo4j://localhost:7687', auth=auth) as d:
    with d.session() as s:
        print(f"starting query {query}")
        result = s.run(query)
        start = time()
        for row in result:
            pass
        finish = time()
        print(f"done! summary: {result.consume()}")
        print(f"time delta: {finish - start}")

```

> See `direct.py`

### Using PyArrow

PyArrow can't talk Bolt...but it can talk to an Apache Arrow Flight service. IF one existed that has it's own way to talk to Neo4j, THEN it could proxy the Cypher transaction and facilitate rendering the Arrow stream back to the PyArrow client.

IF the Neo4j Java Driver is _measurably faster than_ the Neo4j Python driver, THEN it's possible the overhead of using it as a proxy is negligible.

What's the performance with PyArrow and a Java Arrow Flight service? On my laptop with local db...it's **0.17s** to stream all the data back to the client.

> That's **~100x faster**, btw

What's the client code look like? Well...this is a bit verbose as it handles some client/server auth, has to submit the query, then it needs to take the Ticket and get the stream...but it's not that bad:

```python
#!/usr/bin/env python
import pyarrow as pa
import pyarrow.flight as flight
import base64
import sys
from time import time

pa.enable_signal_handlers(True)

location = ("192.168.1.42", 9999)
client = flight.FlightClient(location)
print(f"Trying to connect to location {location}")

try:
    client.wait_for_available(5)
    print(f"Connected")
except Exception as e:
    if type(e) is not flight.FlightUnauthenticatedError:
        print(f"⁉ Failed to connect to {location}: {e.args}")
        sys.exit(1)
    else:
        print("Server requires auth, but connection possible")

options = flight.FlightCallOptions(headers=[
    (b'authorization', b'Basic ' + base64.b64encode(b'neo4j:password'))
])

actions = list(client.list_actions(options=options))
if len(actions) == 0:
    print("Found zero actions 😕")
else:
    print(f"💥 Found {len(actions)} actions!")
    for action in actions:
        print(f"action {action}")

action = ("cypherRead", "UNWIND range(1, 1000000) AS n RETURN n".encode('utf8'))
try:
    for row in client.do_action(action, options=options):
        print(f"row: {row.body.to_pybytes()}")
except Exception as e:
    print(f"⚠ {e}")
    sys.exit(1)

flights = list(client.list_flights(options=options))
if len(flights) == 0:
    print("Found zero flights 😕")
else:
    print(f"Found {len(flights)} flights")
    for flight in flights:
        ticket = flight.endpoints[0].ticket
        print(f"flight: [cmd={flight.descriptor.command}, ticket={ticket}")
        result = client.do_get(ticket, options=options)
        start = time()
        for chunk, metadata in result:
            pass
        finish = time()
        print(f"done! time delta: {finish - start}")
```

## Sooooo...

The above PyArrow client stuff can be cleaned up and made friendlier...and the server side code can use some work. But this _feels_ promising. A **100x speedup** on my laptop gives me some hope!
