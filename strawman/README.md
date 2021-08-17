# A Strawman

This subproject simply uses the Neo4j Java Driver to facilitate testing 
performance against an "out of the box" approach. That is to say, it uses 
the Java driver to run either:

* A Cypher-only "export" of embeddings
* Using GDS procedures to export from the in-memory Graph

## Building
Run the following in the parent project:

```
$ ./gradlew :strawman:shadowJar
```

## Running
Most of the pertinent config options described in the [server](../server) 
project apply. However, there's one special environment variable:

* `GDS`: set any value (e.g. `'ok'` or `1`) to use GDS procedures

```
$ java -jar strawman/build/libs/strawman-1.0-SNAPSHOT.jar
```