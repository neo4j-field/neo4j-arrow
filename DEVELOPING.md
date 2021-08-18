# So you wanna hack on neo4j-arrow?

You'll need:
* A JDK11 distro
* Familiarity with Gradle
* Comfort with Java's async `CompletableFuture` framework
* An iron will?

## Project Structure
The code is broken up into multiple projects.

* The _root project_ contains the core Arrow Flight implementation
  * This foundational code should be effectively _"neo4j free"_, i.e. not 
    require anything to do with Neo4j dependencies
  * The core interfaces you should study are contained here!
* The subprojects each provide something specific:
  * [client](./client) -- an example Java-based Neo4j Arrow client
  * [common](./common) -- code shared between multiple subprojects
  * [plugin](./plugin) -- a server-side Neo4j plugin exposing GDS capabilities
  * [server](./server) -- a standalone Neo4j Arrow server supporting only Cypher
  * [strawman](./strawman) -- A Neo4j Java Driver client for testing

## Design
This implementation builds atop the 
[Apache Arrow Flight](https://arrow.apache.org/blog/2019/10/13/introducing-arrow-flight/) 
framework to provide high performance, remote, stream-based access to Neo4j 
graphs. Knowledge of Flight is beneficial.

On top of the core Flight concepts of `FlightServer`s and `FlightProducer`s, 
the `neo4j-arrow` project adds the concept of `Job`s and `ActionHandler`s 
along with `RowBasedRecord`s.

### Jobs
Jobs encapsulate the lifecycle of getting data in/out of the backend, which 
in this case is Neo4j. Jobs have a very simplified state machine at their core:

* `PENDING`: the Job is submitted and pending results
* `PRODUCING`: the Job is producing results, i.e. the first record has been 
  received
* `COMPLETE`: the Job is no longer producing results
* `ERROR`: something bad happened `:-(`

Jobs provide an asynchronous way for clients to request the creation of a 
stream, for example via a Cypher-based transaction, and a way to reference 
the stream in a future client request.

Each Job that produces a stream gets "ticketed." Using an Arrow Flight 
Ticket provides a way to uniquely reference the job (and stream). Currently, 
this is handled via just a UUID that gets serialized and passed back to the 
client.

Clients built with the open-source Apache Arrow and Arrow Flight projects 
utilize the Arrow Flight RPC framework, specifically the `do_action` or 
similar methods. This means any Arrow client (well, at least v5.0.0) in any 
language can interact with `neo4j-arrow`!

Clients configure Jobs via Arrow Flight Actions, which contain an Action 
Type (just a string) and an arbitrary "body" payload. Different Jobs expect 
different messages/payloads to parameterize inputs for the Job.

#### Cypher Jobs
A Cypher Job encapsulates running a "traditional" Neo4j Cypher-based 
transaction against the database. Since it's Cypher-based, the actual logic 
is pretty similar between Java Driver based versions (see `AsyncDriverJob` 
in the [server](./server) project) and the Transaction API based version 
(see `Neo4jTransactionApiJob` in the [plugin](./plugin) project).

Cypher jobs use the `CypherMessage` (defined in the [common](./common) 
project) to communicate:

* the Cypher to execute
* the database (by name) to execute against
* any parameters for the Cypher

It currently uses the following serialized message format (which should 
probably be simplified to just JSON):

```
 [start - end byte]      [description]
 ----------------------------------------------------
 [0     - 3        ]     length of cypher UTF-8 string (C)
 [4     - C        ]     cypher UTF-8 string
 [C     - C+4      ]     length of UTF-8 database name (D)
 [C+4   - C+D+8    ]     database name UTF-8 string
 [C+D+8 - C+D+12   ]     length of params (P) as UTF-8 JSON
 [C+D+12 - C+D+P+12]     params serialized as UTF-8 JSON
```

#### GDS Jobs
GDS Jobs provide direct read (& soon write) access to in-memory Graph 
projections.

It currently uses the following message format in a simple serialized JSON 
format (in utf-8):

```json
{
  "db": "<the database name>",
  "graph": "<name of the in-memory graph>",
  "filters": ["<list of label or relationship filters>"],
  "parameters": ["<list of node/relationship parameters>"]
}
```

### Action Handlers
TBD

### Row-based Records
This is part of the "secret sauce" to adapting Arrow to Neo4j. For now, the 
short description is this is where any mapping of native "types" (from Neo4j 
Driver Records, GDS values, etc.) to a generalized type occurs.

TBD

## TODO!
Some high level TODOs not in code comments:

* General Stuff
- [ ] Figure out how to properly interrupt/kill streams on exceptions
- [ ] Write support
- [ ] Dockerize the standalone server app

* GDS Native Support
- [ ] Multiple node property support for GDS Jobs
- [ ] Relationship properties!
- [ ] Property filters (label-based, rel-type based)

## TODO's pulled from the code
> last updated 18 Aug 2021

