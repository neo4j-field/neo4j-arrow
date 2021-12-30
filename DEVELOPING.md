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

* `INITIALIZING`: the Job is starting, state is in flux
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
  "type": "<type of job: 'node' or 'relationships' or 'khop'>",
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
- [X] Write support
  - Basic GDS writing except GDS rel properties
- [ ] ~~Dockerize the standalone server app~~

* GDS Native Support
- [X] Multiple node property support for GDS Jobs
- [X] Relationship properties!
- [X] Property filters (label-based, rel-type based)
  - [X] node labels
  - [?] rel types
- [ ] Pivot away from RPC actions and just expose Graphs as discoverable 
  flights?

## TODO's pulled from the code
> last updated 11 Nov 2021 via [todo.sh](./todo.sh)
> 
- [ ] TODO: make an auth handler that isn't this silly [HorribleBasicAuthValidator.java](./src/main/java/org/neo4j/arrow/auth/HorribleBasicAuthValidator.java)
- [ ] TODO: abort [WorkBuffer.java](./src/main/java/org/neo4j/arrow/WorkBuffer.java)
- [ ] TODO: should we allocate a single byte array and not have to reallocate? [WorkBuffer.java](./src/main/java/org/neo4j/arrow/WorkBuffer.java)
- [ ] TODO: should we allocate a single byte array and not have to reallocate? [WorkBuffer.java](./src/main/java/org/neo4j/arrow/WorkBuffer.java)
- [ ] TODO: check isReady(), yield if not [Producer.javalistener.putNext();](./src/main/java/org/neo4j/arrow/Producer.javalistener.putNext();)
- [ ] TODO: validate root.Schema [Producer.java](./src/main/java/org/neo4j/arrow/Producer.java)
- [ ] TODO!!! [Producer.javajob.onComplete(arrowBatches);](./src/main/java/org/neo4j/arrow/Producer.javajob.onComplete(arrowBatches);)
- [ ] TODO: we need to wait until the post-processing completes, need a callback here [Producer.java](./src/main/java/org/neo4j/arrow/Producer.java)
- [ ] TODO: validate schema option? [ArrowBatch.java](./src/main/java/org/neo4j/arrow/ArrowBatch.java)
- [ ] XXX  TODO [ArrowBatch.javareturn(int)rowCount;](./src/main/java/org/neo4j/arrow/ArrowBatch.javareturn(int)rowCount;)
- [ ] TODO handle writejob close??? [WriteJob.java](./src/main/java/org/neo4j/arrow/job/WriteJob.java)
- [ ] TODO: standardize on matching logic? case sensitive/insensitive? [StatusHandler.java](./src/main/java/org/neo4j/arrow/action/StatusHandler.java)
- [ ] TODO: use all 4 bits [Edge.java](./plugin/src/main/java/org/neo4j/arrow/gds/Edge.java)
- [ ] TODO: new Exception class [CypherRecord.java](./plugin/src/main/java/org/neo4j/arrow/CypherRecord.java)
- [ ] TODO: clean this Double to Float mess :-( [CypherRecord.java](./plugin/src/main/java/org/neo4j/arrow/CypherRecord.java)
- [ ] TODO: object copy might be slow, check on this [CypherRecord.java](./plugin/src/main/java/org/neo4j/arrow/CypherRecord.java)
- [ ] TODO: ShortArray [CypherRecord.java}](./plugin/src/main/java/org/neo4j/arrow/CypherRecord.java})
- [ ] TODO: error handling for graph store retrieval [GdsReadJob.java](./plugin/src/main/java/org/neo4j/arrow/job/GdsReadJob.java)
- [ ] TODO: support both rel type and node label filtering [GdsReadJob.java](./plugin/src/main/java/org/neo4j/arrow/job/GdsReadJob.java)
- [ ] TODO: nested for-loop is ugly [GdsReadJob.java](./plugin/src/main/java/org/neo4j/arrow/job/GdsReadJob.java)
- [ ] TODO: GDS lets us batch access to lists of nodes...future opportunity? [GdsReadJob.java](./plugin/src/main/java/org/neo4j/arrow/job/GdsReadJob.java)
- [ ] TODO: should it be nodeCount - 1? We advanced the iterator...maybe? [GdsReadJob.java](./plugin/src/main/java/org/neo4j/arrow/job/GdsReadJob.java)
- [ ] TODO: clean up [GdsWriteJob.java](./plugin/src/main/java/org/neo4j/arrow/job/GdsWriteJob.java)
- [ ] TODO: add in filtered label support [GdsWriteJob.java](./plugin/src/main/java/org/neo4j/arrow/job/GdsWriteJob.java)
- [ ] TODO: implement cloning for id maps [GdsWriteJob.java](./plugin/src/main/java/org/neo4j/arrow/job/GdsWriteJob.java)
- [ ] TODO: what the heck is this Cases<R> stuff?! [GdsWriteJob.java](./plugin/src/main/java/org/neo4j/arrow/job/GdsWriteJob.java)
- [ ] TODO: relationship properties! [GdsWriteJob.java](./plugin/src/main/java/org/neo4j/arrow/job/GdsWriteJob.java)
- [ ] TODO: XXX when we implement rel props, do NOT close this here! [GdsWriteJob.java](./plugin/src/main/java/org/neo4j/arrow/job/GdsWriteJob.java)
- [ ] TODO: wire in maps [GdsWriteJob.java](./plugin/src/main/java/org/neo4j/arrow/job/GdsWriteJob.java)
- [ ] TODO: pull in reference to LoginContext and use it in the Transaction [TransactionApiJob.java](./plugin/src/main/java/org/neo4j/arrow/job/TransactionApiJob.java)
- [ ] TODO: for now wait up to a minute for the first result...needs future work [TransactionApiJob.java](./plugin/src/main/java/org/neo4j/arrow/job/TransactionApiJob.java)
- [ ] TODO: wrap function should take our assumed schema to optimize [TransactionApiJob.java](./plugin/src/main/java/org/neo4j/arrow/job/TransactionApiJob.java)
- [ ] TODO: INT? Does it exist? [GdsNodeRecord.java](./plugin/src/main/java/org/neo4j/arrow/GdsNodeRecord.java)
- [ ] TODO: INT_ARRAY? [GdsNodeRecord.java](./plugin/src/main/java/org/neo4j/arrow/GdsNodeRecord.java)
- [ ] TODO: String? Object? What should we do? [GdsNodeRecord.java](./plugin/src/main/java/org/neo4j/arrow/GdsNodeRecord.java)
- [ ] TODO: rename property keys to read/write forms [GdsActionHandler.java](./plugin/src/main/java/org/neo4j/arrow/action/GdsActionHandler.java)
- [ ] TODO: fallback to raw bytes? [GdsActionHandler.java](./plugin/src/main/java/org/neo4j/arrow/action/GdsActionHandler.java)
- [ ] TODO: "type" is pretty vague...needs a better name [GdsMessage.java](./plugin/src/main/java/org/neo4j/arrow/action/GdsMessage.java)
- [ ] TODO: validation / constraints of values? [GdsMessage.java](./plugin/src/main/java/org/neo4j/arrow/action/GdsMessage.java)
- [ ] TODO: assert our minimum schema? [GdsMessage.java](./plugin/src/main/java/org/neo4j/arrow/action/GdsMessage.java)
- [ ] TODO: assert our minimum schema? [GdsWriteRelsMessage.java](./plugin/src/main/java/org/neo4j/arrow/action/GdsWriteRelsMessage.java)
- [ ] TODO: "type" is pretty vague...needs a better name [GdsWriteNodeMessage.java](./plugin/src/main/java/org/neo4j/arrow/action/GdsWriteNodeMessage.java)
- [ ] TODO: assert our minimum schema? [GdsWriteNodeMessage.java](./plugin/src/main/java/org/neo4j/arrow/action/GdsWriteNodeMessage.java)
- [ ] TODO: better mapping support for generic Cypher values? [CypherActionHandler.java](./common/src/main/java/org/neo4j/arrow/action/CypherActionHandler.java)
- [ ] TODO: fallback to raw bytes? [CypherActionHandler.java](./common/src/main/java/org/neo4j/arrow/action/CypherActionHandler.java)