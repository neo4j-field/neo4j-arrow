# neo4j-arrow 🏹 -- Graph Data Science Workflows _Accelerated_

> “When you want to do something, do it right away. Do it when you can.
> It’s the only way to live a life without regrets.”
>   -- Sonic The Hedgehog

![gotta-go-fast](./fast.gif)

## tl;dr: neo4j-arrow in a nutshell 🥜

This project aims to step outside of traditional ways of integrating with Neo4j and expose things like the in-memory GDS graph projection via high-performance Arrow Flight APIs for streaming nodes, relationships, and their properties.

`neo4j-arrow` has been shown to:
* ⏩ Move data out of Neo4j **20x faster** than the Neo4j Java Driver
* ⏩ Move data out of Neo4j **450x faster** than the Neo4j Python Driver
* 🏎️ Facilitate live-migration of GDS graph projections, moving _50m
  nodes, 200m relationship, and 50m 256-degree feature vectors_
  between live Neo4j instances in different GCP regions in **under 15
  minutes**.
* Provide a faster integration with *BigQuery*, extracting and loading
  _50m nodes with 256-degree feature vector properties_ in under **2
  minutes**.
* 😃 Make you happier and more at peace.

## Building and Deploying 🛠️

Most users will want to use the `plugin` form of `neo4j-arrow`. The
most recent tagged version is available on the 
[releases](https://github.com/neo4j-field/neo4j-arrow/releases) page.

To build your own from source, you'll need:

* A **JDK 11** distribution
* Internet access to the public Maven repo

Simply run: `$ ./gradlew plugin:shadowJar`

You should end up with a Neo4j plugin jar in `./plugin/build/libs/`
that you can drop into a Neo4j system.


## Plugin Operation 🔌

`neo4j-arrow` should work out of the box with **Neo4j 4.3** and **GDS
v1.7**. If you have earlier versions of either, you'll need to compile
your own jars from source.

The `neo4j_arrow.py` PyArrow client requires **Python 3** and
**PyArrow 6.0.0**. (It may still work with v5.0.0, but I'm developing on
v6.0.0.)

Any other Arrow Flight clients should use `v6.0.0` of Arrow/Arrow Flight if
possible.

### Configuration ✔️

All configuration for `neo4j-arrow` is performed via environment
variables. (Currently there is no support for property-based config in
`neo4j.conf`.) This holds true for all subprojects of `neo4j-arrow`.

Available configuration options specific to the `neo4j-arrow` plugin:

| Option                  | Description                                                                                                 | Default             |
|-------------------------|-------------------------------------------------------------------------------------------------------------|---------------------|
| `HOST`                  | Hostname or IP for `neo4j-arrow` to listen on                                                               | `"localhost"`       |
| `PORT`                  | TCP Port for `neo4j-arrow` to listen on                                                                     | `9999`              |
| `MAX_MEM_GLOBAL`        | Global memory limit for Arrow memory allocator                                                              | `Long.MAX_VALUE`    |
| `MAX_MEM_STREAM`        | Per-stream memory limit                                                                                     | `Long.MAX_VALUE`    |
| `ARROW_BATCH_SIZE`      | Max number of rows to include when sending a vector batch to a client                                       | `1000`              |
| `ARROW_MAX_PARTITIONS`  | Max number of partitions to create when generating streams, roughly translates to # of CPU cores to utilize | `max(vcpus - 2, 1)` |
| `ARROW_FLUSH_TIMEOUT`   | Max time (in seconds) to wait before flushing a stream to the client                                        | `1800`              |
| `ARROW_TLS_CERTIFICATE` | Path to x.509 full-chain certifcate in PEM format                                                           | `""`                |
| `ARROW_TLS_PRIVATE_KEY` | Path to PEM private key file                                                                                | `""`                |

> See also: the `org.neo4j.arrow.Config` class.

### Performance Tuning 🔧

The primary knobs available for tuning for read or write performance
is the `ARROW_BATCH_SIZE` and `ARROW_MAX_PARTITIONS` value. Given the
partition number defaults to a value based on the host cpu core count,
in practice this means tuning the batch size.

Some general advice:

- **Reads** tend to perform better with smaller batch sizes (such as the default)
- **Writes** benefit from much larger (150-200x than the default) values.

In practice, the true performance will depend heavily on the _type_ of
vectors being created. Scalars generally outperform List-based vectors
for multiple reasons: total buffer size and cpu instructions required
to read/write a value.


## The `neo4j-arrow` Protocol/Lifecycle 🗣️

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

### Using Python 🐍

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


## Some Examples 🧑‍🏫

A few *IPython notebooks* are provided that demonstrate more complex
usage of `neo4j-arrow` via PyArrow:

1. [Basic usage](./PyArrow%20Demo.ipynb) and lifecycle
2. [Live migration](./live_migration_demo.ipynb) of a graph between
  disparate Neo4j instances

Some code examples:

* [Integration with BigQuery](arrow_to_bq.py) showing how to relay a
  `neo4j-arrow` stream to a target BigQuery table.
* [Trivial example](./example.py) of reading some nodes from GDS


## Known Issues or Incomplete Things ⚠️

Keep in mind `neo4j-arrow` is a work in progress. The following are
areas still requiring some development cycles or help.

- Cannot currently _write_ relationship properties to a GDS graph.
  * On the todo list.
- Cannot _read_ string properties (not counting Labels or Relationship
  Types) from a GDS graph.
  * GDS doesn't natively support strings and reads them from the db.
  * This could be implemented, but may slow down streaming.
- Only basic Cypher types are supported for _cypher jobs_ (mostly
  scalars, Strings, and simple Lists of scalars).
  * The cardinality of the set of types Cypher supports is _too damn
    high_.
  * What, you don't like numbers? :-)
- Error handling and cleanup for failed/incomplete jobs needs work.
- Batch sizes are not dynamically configurable or chosen. Ideally they
  would be determined based on schema and host hardware.
  * As we develop and utilize `neo4j-arrow` a best practice should be
    baked in, but we're not there yet.
- Cannot currently _write_ to the database via Cypher jobs.
  * Requires more complex Cypher transaction jobs.
  * Not out of the realm of possibility, but not a priority.
- GDS _write_ jobs report completion to the client before completion
  of server-side processing. Client's may start writing
  _relationships_ while _nodes_ are still being post-processed,
  resulting in a "graph not found" error.
  * On the todo list.


# Licensing & Copyright
Like other `neo4j-labs` and `neo4j-contrib` projects, this project is
provided under the terms of the [Apache 2.0](./LICENSE) license.

All files and code are copyright 2021, Neo4j, Inc.
