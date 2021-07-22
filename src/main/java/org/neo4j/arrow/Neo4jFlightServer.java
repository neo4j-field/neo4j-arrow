package org.neo4j.arrow;

import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.auth2.BasicCallHeaderAuthenticator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class Neo4jFlightServer implements AutoCloseable {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Neo4jFlightServer.class);

    private final FlightServer server;
    private final Location location;
    private final Neo4jProducer producer;
    private final BufferAllocator allocator;

    public Neo4jFlightServer(BufferAllocator allocator, Location location) {
        this.allocator = allocator.newChildAllocator("neo4j-flight-server", 0, Long.MAX_VALUE);
        this.location = location;
        this.producer = new Neo4jProducer(allocator, location);
        this.server = FlightServer.builder(allocator, location, this.producer)
                // XXX header auth expects basic HTTP headers in the GRPC calls
                .headerAuthenticator(new BasicCallHeaderAuthenticator(new Neo4jBasicAuthValidator()))
                // XXX this approach for some reason didn't work for me in python :-(
                //.authHandler(new BasicServerAuthHandler(new Neo4jBasicAuthValidator()))
                .build();
    }

    public void start() throws IOException {
        server.start();
        logger.info("server listening @ {}", location.getUri().toString());
    }

    public void awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        server.awaitTermination(timeout, unit);
    }

    @Override
    public void close() throws Exception {
        AutoCloseables.close(producer, server, allocator);
    }
}
