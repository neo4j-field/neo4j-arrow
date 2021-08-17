# neo4j-arrow server plugin

A Neo4j server-side extension exposing Arrow Flight services. This is the 
fun project you should build and play with :-)

## Building
In the parent project, run:

```
$ ./gradlew :plugin:shadowJar
```

That's it! You'll get a `-all` plugin uberjar in: `plugins/build/libs`

## Installing
Drop the uberjar into a 4.3 version of Neo4j Enterprise (hasn't been tested 
with Community Edition). You also need a recent 1.6.x version of GDS.

> I recommend using `v1.6.4` of GDS as that's what the project builds with!

## Running
The plugin exposes a new service on the Neo4j server. By default, this is on 
TCP port `9999`, though this is configurable via environment variables. (See 
the [server](../server) docs for now on what environment variables exist.)

## Ok, now what?
Grab an Arrow client, either:

* [neo4j-arrow.py](../neo4j_arrow.py) for Python
* [org.neo4j.arrow.demo.Client](../client) for Java

Docs are still coming together, but see the recent 
[demo walk-through](../PyArrow%20Demo.ipynb) using the PyArrow-based client 
for an example.
