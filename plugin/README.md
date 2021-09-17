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

## An Example using Docker
Assuming you've got a local directory called `./plugins` that has GDS 1.7 
and the `neo4j-arrow` jar file (and is readable by uid:gid 7474:7474):

> Tune heap/pagecache as needed

```shell
#!/bin/sh
HOST=0.0.0.0
PORT=9999
docker run --rm -it --name neo4j-test \
        -e HOST="${HOST}" -e PORT=${PORT} \
        -v "$(pwd)/plugins":/plugins \
        -e NEO4J_AUTH=neo4j/password \
        -e NEO4J_ACCEPT_LICENSE_AGREEMENT=yes \
        -p 7687:7687 -p 7474:7474 \
        -v "$(pwd)/data:/data" \
        -p "${PORT}:${PORT}" \
        -e NEO4J_dbms_memory_heap_initial__size=80g \
        -e NEO4J_dbms_memory_heap_max__size=80g \
        -e NEO4J_dbms_memory_pagecache_size=16g \
        -e NEO4J_dbms_security_procedures_unrestricted="gds.*" \
        -e NEO4J_dbms_jvm_additional=-Dio.netty.tryReflectionSetAccessible=true \
        -e NEO4J_dbms_allow__upgrade=true \
        -e NEO4J_gds_enterprise_license__file=/plugins/gds.txt \
        neo4j:4.3.3-enterprise
```