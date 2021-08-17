# neo4j-arrow java client
A quick and dirty Java client for talking to a `neo4j-arrow` service.

## Building
In the parent project:

```
$ ./gradlew :client:shadowJar
```

## Running
You can just run the uberjar created by Gradle. See the [server](../server) 
docs for details on environment variables for configuration.

```
$ java -jar <path to jar>/client-1.0-SNAPSHOT.jar
```

## Extending or Embedding
The `org.neo4j.arrow.demo.Client` class has both a `static main` method as 
well as a Class structure with a `run` method. Either use from the command 
line or feel free to instantiate, configure, and run from other JVM based code!