package org.neo4j.arrow;

import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.auth2.BasicCallHeaderAuthenticator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class Neo4jFlightApp implements AutoCloseable {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Neo4jFlightApp.class);

    private final FlightServer server;
    private final Location location;
    private final Neo4jProducer producer;
    private final BufferAllocator allocator;
    private final String name;

    public Neo4jFlightApp(BufferAllocator rootAllocator, Location location, JobCreator jobCreator) {
        this(rootAllocator, location, jobCreator, "unnamed-app");
    }

    public Neo4jFlightApp(BufferAllocator rootAllocator, Location location, JobCreator jobCreator, String name) {
        allocator = rootAllocator.newChildAllocator("neo4j-flight-server", 0, Long.MAX_VALUE);
        this.location = location;
        this.producer = new Neo4jProducer(allocator, location, jobCreator);
        this.server = FlightServer.builder(rootAllocator, location, this.producer)
                // XXX header auth expects basic HTTP headers in the GRPC calls
                .headerAuthenticator(new BasicCallHeaderAuthenticator(new Neo4jBasicAuthValidator()))
                // XXX this approach for some reason didn't work for me in python :-(
                //.authHandler(new BasicServerAuthHandler(new Neo4jBasicAuthValidator()))
                .build();
        this.name = name;
    }

    public void start() throws IOException {
        server.start();
        logger.info("server listening @ {}", location.getUri().toString());
    }

    public void awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        server.awaitTermination(timeout, unit);
    }

    public Location getLocation() {
        return location;
    }

    @Override
    public String toString() {
        return "NeojFlightApp { name: " + name + ", location: " + location.toString() + " }";
    }

    @Override
    public void close() throws Exception {
        logger.debug("closing");
        AutoCloseables.close(producer, server, allocator);
    }
}
