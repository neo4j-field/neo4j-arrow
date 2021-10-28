# neo4j-arrow -- Data Science workflows 10-100x faster

> “When you want to do something, do it right away. Do it when you can.
> It’s the only way to live a life without regrets.”
>   -- Sonic The Hedgehog

![gotta-go-fast](./fast.gif)

## tl;dr: neo4j-arrow in a nutshell

This project aims to step outside of traditional ways of integrating with Neo4j and expose things like the in-memory GDS graph projection via high-performance Arrow Flight APIs for streaming nodes, relationships, and their properties.

`neo4j-arrow` has been shown to:
* Move data out of Neo4j **20x faster** than the Neo4j Java Driver
* Move data out of Neo4j **450x faster** than the Neo4j Python Driver
* Facilitate live-migration of GDS graph projections, moving _50m
  nodes, 200m relationship, and 50m 256-degree feature vectors_
  between live Neo4j instances in different GCP regions in **under 15
  minutes**.
* Provide a faster integration with *BigQuery*, extracting and loading
  _50m nodes with 256-degree feature vector properties_ in under **2
  minutes**.
* Make you happier and more at peace.

## Building and Deploying

Most users will want to use the `plugin` form of `neo4j-arrow`. The
most recent tagged version is available on the [./releases](Releases)
page.

To build your own from source, you'll need:

* A **JDK 11** distribution
* Access to the Neo4j _private_ maven repo (contact Neo4j Support or
  your Customer Success Manager)
  - This restriction may be lifted with a bit more dev work...it's on
    the backlog.

Simply run: `$ ./gradlew plugin:shadowJar`

You should end up with a Neo4j plugin jar in `./plugin/build/libs/`
that you can drop into a Neo4j system.

### Runtime Requirements

`neo4j-arrow` should work out of the box with **Neo4j 4.3** and **GDS
v1.7**. If you have earlier versions of either, you'll need to compile
your own jars from source.

The `neo4j_arrow.py` PyArrow client requires **Python 3** and
**PyArrow 5.0.0**.

## The `neo4j-arrow` Lifecycle

`neo4j-arrow` uses the *Arrow Flight* RPC framework to expose
read/write operations to Neo4j. While you can implement the protocol
yourself using an Arrow/Arrow Flight client implementation in any
language, a helpful wrapper using *PyArrow* is provided in
[./neo4j_arrow.py](neo4j_arrow.py).

The general lifecycle looks like:

1. Create an Arrow client with authentication details for the Neo4j
   system, optionally using TLS.
2. Submit a valid `neo4j-arrow` *Job* to the service. Current jobs
   include:
   - Cypher Read jobs (`cypherRead`)
   - GDS Read jobs (`gds.read`) with ability to specify nodes or
     relationships
   - GDS Write Jobs (`gds.write.nodes` & `gds.write.relationships`)
     for creating GDS graphs from Arrow vectors/tables
3. Take the given *ticket* returned by the server for the *Job* and
   either request a readable stream or submit a stream via a "put"
   operation.

In practice, the nuances of jobs, tickets, etc. are handled for you if
you use the provided Python wrapper.

### Using Python

Assuming you've using `neo4j_arrow.py`, a simple GDS read operation to
request and stream computed node embeddings (for example) looks like:

```python
import neo4j_arrow as na

# using the neo4j user and 'password' as the password
client = na.Neo4jArrow('neo4j', 'password', ('neo4-host', 9999))

# stream the 'mygraph' projection and include the 'fastRp' node properties
ticket = client.gds_nodes('mygraph', properties=['fastRp'])

# get a PyArrow Table from the stream, reading it all into the client
table = client.stream(ticket).read_all()

# convert to a Pandas dataframe and go have fun!
df = table.to_pandas()
```

## Some Examples

A few *IPython notebooks* are provided that demonstrate more complex
usage of `neo4j-arrow` via PyArrow:

1. [Basic usage](./PyArrow%20Demo.ipynb) and lifecycle
2. [Live migration](./live_migration_demo.ipynb) of a graph between
  disparate Neo4j instances

Some code examples:

* [Integration with BigQuery](arrow_to_bq.py) showing how to relay a
  `neo4j-arrow` stream to a target BigQuery table.
* [Trivial example](./example.py) of reading some nodes from GDS
