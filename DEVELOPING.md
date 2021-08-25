# So you wanna hack on neo4j-arrow?

You'll need:
* A JDK11 [distro](https://adoptopenjdk.net/)
* Familiarity with [Gradle](https://gradle.org/)
* Comfort with Java's async `CompletableFuture` framework

## Project Core Values
The following drive decision-making with respect to scope of `neo4j-arrow`:

1. **Performance** -- consumers or producers of data should be able to read or 
   update the graph as fast as the network can transmit bytes
2. **Simplicity** -- how easily can a Data Scientist/Engineer get the data they 
   need from the graph?
3. **Interoperability** -- can the data be easily used/stored/retransmitted by 
   the tools Data Scientists/Engineers already use

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

A quick cheat-sheet:
* The `Producer` brokers `Actions` to `ActionHandlers` and requests to 
  consume streams of data
  * _If you want to customize the core streaming logic, look here._
* `ActionHandlers` interpret RPC call, turning their `Action` bodies/payloads 
  into `Messages` and use them to create `Jobs`.
  * _If you want to create new `Actions`, build an `ActionHandler` and a new 
    type of `Job`. Up to you how you deal with messages/payloads._
* `Jobs` unofficially 

> There's no formal interface or abstract class for `Messages` at the moment,
> ...consider them POJOs?

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
Action Handlers define new Arrow Flight RPC action types available, provide 
descriptions of the action offered, and perform any servicing of the actions 
when called by clients.

The way `neo4j-arrow` uses actions is primarily around Job control: 
submitting new stream-producing jobs and checking on their status.

Any new action handler gets registered with the App (or Producer).

### Row-based Records
This is part of the "secret sauce" to adapting Arrow to Neo4j. For now, the 
short description is this is where any mapping of native "types" (from Neo4j 
Driver Records, GDS values, etc.) to a generalized type occurs.

Other than the core `Producer` logic dealing with putting bytes on the wire, 
this is one of the hotter code paths ripe for optimization. The `Value` 
interface is designed to help translate the raw underlying scalar or array 
value (no map support yet) into the appropriate Arrow `FieldVector`.

## TODO!
Some high level TODOs not in code comments:

* General Stuff
- [ ] Figure out how to properly interrupt/kill streams on exceptions
- [ ] Write support
- [ ] Dockerize the standalone server app

* GDS Native Support
- [X] Multiple node property support for GDS Jobs
- [ ] Relationship properties!
- [ ] Property filters (label-based, rel-type based)
  - [X] node labels
  - [ ] rel types
- [ ] Pivot away from RPC actions and just expose Graphs as discoverable 
  flights?

## TODO's pulled from the code
> last updated 18 Aug 2021 via [todo.sh](./todo.sh)

- [ ] TODO: get Cypher username/password from Context? [CypherActionHandler.java](./common/src/main/java/org/neo4j/arrow/action/CypherActionHandler.java)
- [ ] TODO: better mapping support for generic Cypher values? [CypherActionHandler.java](./common/src/main/java/org/neo4j/arrow/action/CypherActionHandler.java)
- [ ] TODO: fallback to raw bytes? [CypherActionHandler.java](./common/src/main/java/org/neo4j/arrow/action/CypherActionHandler.java)
- [ ] TODO: fallback to raw bytes? [GdsActionHandler.java](./plugin/src/main/java/org/neo4j/arrow/action/GdsActionHandler.java)
- [ ] TODO: validation / constraints of values? [GdsMessage.java](./plugin/src/main/java/org/neo4j/arrow/action/GdsMessage.java)
- [ ] TODO: assert our minimum schema? [GdsMessage.java](./plugin/src/main/java/org/neo4j/arrow/action/GdsMessage.java)
- [ ] TODO: INT? Does it exist? [GdsRecord.java](./plugin/src/main/java/org/neo4j/arrow/GdsRecord.java)
- [ ] TODO: INT_ARRAY? [GdsRecord.java](./plugin/src/main/java/org/neo4j/arrow/GdsRecord.java)
- [ ] TODO: String? Object? What should we do? [GdsRecord.java](./plugin/src/main/java/org/neo4j/arrow/GdsRecord.java)
- [ ] TODO: apply "filters" to labels or types...for now just get all [GdsJob.java](./plugin/src/main/java/org/neo4j/arrow/job/GdsJob.java)
- [ ] TODO: inspect the schema via the Graph instance...need to change the Job message type [GdsJob.java](./plugin/src/main/java/org/neo4j/arrow/job/GdsJob.java)
- [ ] TODO: support more than 1 property in the request. Use first filter for now as label filter [GdsJob.java](./plugin/src/main/java/org/neo4j/arrow/job/GdsJob.java)
- [ ] TODO: GDS lets us batch access to lists of nodes...future opportunity? [GdsJob.java](./plugin/src/main/java/org/neo4j/arrow/job/GdsJob.java)
- [ ] TODO: pull in reference to LoginContext and use it in the Transaction [Neo4jTransactionApiJob.java](./plugin/src/main/java/org/neo4j/arrow/job/Neo4jTransactionApiJob.java)
- [ ] TODO: standardize on matching logic? case sensitive/insensitive? [StatusHandler.java](./src/main/java/org/neo4j/arrow/action/StatusHandler.java)
- [ ] TODO: make an auth handler that isn't this silly [HorribleBasicAuthValidator.java](./src/main/java/org/neo4j/arrow/auth/HorribleBasicAuthValidator.java)
- [ ] TODO: do we need to allocate explicitly? [Producer.java](./src/main/java/org/neo4j/arrow/Producer.java)
- [ ] TODO: handle batches of records to decrease frequency of calls [Producer.java](./src/main/java/org/neo4j/arrow/Producer.java)
- [ ] TODO: refactor to using fixed arrays for speed [Producer.java](./src/main/java/org/neo4j/arrow/Producer.java)
