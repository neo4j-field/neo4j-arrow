package org.neo4j.arrow;

import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;

import java.util.concurrent.TimeUnit;

public class Neo4jFlightServerTest {
    private static final org.slf4j.Logger logger;

    static {
        // Set up nicer logging output.
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "[yyyy-MM-dd'T'HH:mm:ss:SSS]");
        System.setProperty("org.slf4j.simpleLogger.logFile", "System.out");
        logger = org.slf4j.LoggerFactory.getLogger(Neo4jFlightServerTest.class);
    }

    public static void main(String[] args) throws Exception {
        long timeout = 30;
        TimeUnit unit = TimeUnit.SECONDS;

        final BufferAllocator bufferAllocator = new RootAllocator(Long.MAX_VALUE);
        final Neo4jFlightApp neo4jFlightServer = new Neo4jFlightApp(
                bufferAllocator,
                Location.forGrpcInsecure("0.0.0.0", 9999));
        neo4jFlightServer.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                logger.info("Shutting down...");
                AutoCloseables.close(neo4jFlightServer, bufferAllocator);
                logger.info("Stopped.");
            } catch (Exception e) {
                logger.error("Failure during shutdown", e);
            }
        }));

        neo4jFlightServer.awaitTermination(timeout, unit);
    }
}
