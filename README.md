# neo4j-arrow -- Data Science workflows 10-100x faster

> "“When you want to do something, do it right away. Do it when you can. 
> It’s the only way to live a life without regrets.” 
> 
>                                    --Sonic The Hedgehog

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
> 
>                                    -- Vironika Tugaleva

Getting large amounts of data into Neo4j has been a challenge...and, honestly, 
this project is yet to address this matter. However, _getting data out en 
masse_, **can** be immediately addressed with Apache Arrow.

How?

Sidestepping the **transaction layer** of Neo4j and using Arrow's more 
efficient (for fixed-width data types) vector format.

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
The secret sauce of modern Data Science platforms appears to be exposing 
Python as the thing the scientists learn and interact with and use it to 
abstract out the complex "computer science-y" things like memory allocation, 
zero-copy slices, etc. Underneath Pandas, NumPy, SciPy, etc. is a sea of C 
code (and sometimes C++, Fortran, etc.) specializing in efficiency. The 

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