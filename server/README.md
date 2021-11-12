> **NOTE**: I'm no longer hacking on this server sub-project!

# neo4j-arrow "proxy" server
Can't install the `neo4j-arrow` [plugin](../plugin)? Try the proxy.

> This approach support Cypher jobs, but does *not* support GDS jobs!

## Building
Simplest way is to run the `shadowJar` task (from the project root):

```
$ ./gradlew :server:shadowJar
```

## Running
I'm lazy, so the config is all via environment variables:

* `NEO4J_URL`: bolt url (default: `neo4j://localhost:7687`)
* `NEO4J_USERNAME`: (default: `neo4j`)
* `NEO4J_PASSWORD`: (default: `password`)
* `NEO4J_DATABASE`: (default: `neo4j`)
* `HOST`: ip/hostname to listen on (default: `localhost`)
* `PORT`: tpc port to listen on (default: `9999`)

Some tuning config available:

* `MAX_MEM_GLOBAL`: maximum memory to allow allocating by the Arrow global 
  allocator (default: java's `Long.MAX_VALUE`)
* `MAX_MEM_STREAM`: maximum memory allowed to be allocated by an individual 
  stream (default: java's `Integer.MAX_VALUE`)
* `ARROW_BATCH_SIZE`: size of the transmitted vector batches (i.e. number of 
  "rows") (default: `25,000`)
* `BOLT_FETCH_SIZE`: (default: `1,000`)

Set any of the above and run:

```
$ java -jar <path to jar>/server-1.0-SNAPSHOT.jar
```