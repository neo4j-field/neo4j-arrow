package org.neo4j.arrow;

import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.auth2.BasicCallHeaderAuthenticator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.neo4j.arrow.action.ActionHandler;
import org.neo4j.arrow.auth.HorribleBasicAuthValidator;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class App implements AutoCloseable {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(App.class);

    private final FlightServer server;
    private final Location location;
    private final Producer producer;
    private final BufferAllocator allocator;
    private final String name;

    public App(BufferAllocator rootAllocator, Location location) {
        this(rootAllocator, location, "unnamed-app");
    }

    public App(BufferAllocator rootAllocator, Location location, String name) {
        allocator = rootAllocator.newChildAllocator("neo4j-flight-server", 0, Long.MAX_VALUE);
        this.location = location;
        this.producer = new Producer(allocator, location);
        this.server = FlightServer.builder(rootAllocator, location, this.producer)
                // XXX header auth expects basic HTTP headers in the GRPC calls
                .headerAuthenticator(new BasicCallHeaderAuthenticator(new HorribleBasicAuthValidator()))
                // XXX this approach for some reason didn't work for me in python :-(
                //.authHandler(new BasicServerAuthHandler(new Neo4jBasicAuthValidator()))
                .build();
        this.name = name;
    }

    public void registerHandler(ActionHandler handler) {
        producer.registerHandler(handler);
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
