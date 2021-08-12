package org.neo4j.arrow.demo;

import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.neo4j.arrow.Config;
import org.neo4j.arrow.App;
import org.neo4j.arrow.action.CypherActionHandler;
import org.neo4j.arrow.job.AsyncDriverJob;
import org.neo4j.driver.AuthTokens;

import java.util.concurrent.TimeUnit;

/**
 * A simple implementation of a Neo4j Arrow Service.
 */
public class Neo4jProxyServer {
    private static final org.slf4j.Logger logger;

    static {
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "[yyyy-MM-dd'T'HH:mm:ss:SSS]");
        System.setProperty("org.slf4j.simpleLogger.logFile", "System.out");
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        logger = org.slf4j.LoggerFactory.getLogger(Neo4jProxyServer.class);
    }

    public static void main(String[] args) throws Exception {
        long timeout = 5;
        TimeUnit unit = TimeUnit.MINUTES;

        final BufferAllocator bufferAllocator = new RootAllocator(Config.maxGlobalMemory);
        final App app = new App(
                bufferAllocator,
                Location.forGrpcInsecure(Config.host, Config.port));

        final CypherActionHandler cypherHandler = new CypherActionHandler(
                (cypherMsg, mode, username, password) ->
                        new AsyncDriverJob(cypherMsg, mode, AuthTokens.basic(username.get(), password.get())));
        app.registerHandler(cypherHandler);

        app.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                logger.info("Shutting down...");
                AutoCloseables.close(app, bufferAllocator);
                logger.info("Stopped.");
            } catch (Exception e) {
                logger.error("Failure during shutdown!", e);
            }
        }));

        logger.info("Will terminate after timeout of {} {}", timeout, unit);
        app.awaitTermination(timeout, unit);
    }
}
