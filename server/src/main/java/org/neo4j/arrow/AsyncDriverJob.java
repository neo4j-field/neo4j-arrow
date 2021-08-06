package org.neo4j.arrow;

import org.neo4j.driver.*;
import org.neo4j.driver.async.AsyncSession;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

/**
 * Implementation of a Neo4jJob that uses an AsyncSession via the Java Driver.
 */
public class AsyncDriverJob extends Neo4jJob {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(AsyncDriverJob.class);

    /* Drivers per identity */
    private static ConcurrentMap<AuthToken, Driver> driverMap = new ConcurrentHashMap<>();

    private final AsyncSession session;
    private final CompletableFuture future;

    protected AsyncDriverJob(CypherMessage msg, Mode mode, AuthToken authToken) {
        super(msg, mode);

        Driver driver;
        if (!driverMap.containsKey(authToken)) {
            org.neo4j.driver.Config.ConfigBuilder builder = org.neo4j.driver.Config.builder();
            driver = GraphDatabase.driver(Config.neo4jUrl, authToken,
                    builder.withUserAgent("Neo4j-Arrow/alpha")
                            .withMaxConnectionPoolSize(8)
                            .withFetchSize(Config.boltFetchSize)
                            .build());
            driverMap.put(authToken, driver);
        } else {
            driver = driverMap.get(authToken);
        }

        this.session = driver.asyncSession(SessionConfig.builder()
                .withDatabase(Config.database)
                .withDefaultAccessMode(AccessMode.valueOf(mode.name()))
                .build());

        future = session.runAsync(msg.getCypher(), msg.getParams())
                .thenComposeAsync(resultCursor -> {
                    logger.info("Job {} producing", session);
                    setStatus(Status.PRODUCING);

                    Record firstRecord = resultCursor.peekAsync().toCompletableFuture().join();
                    onFirstRecord(Neo4jDriverRecord.wrap(firstRecord));

                    Consumer<Neo4jRecord> consumer = futureConsumer.join();
                    return resultCursor.forEachAsync(record -> {
                        consumer.accept(Neo4jDriverRecord.wrap(record));
                    });
                }).whenCompleteAsync((resultSummary, throwable) -> {
                    if (throwable != null) {
                        setStatus(Status.ERROR);
                        logger.error("job failure", throwable);
                    } else {
                        logger.info("job {} complete", session);
                        setStatus(Status.COMPLETE);
                    }
                    onCompletion(Neo4jDriverJobSummary.wrap(resultSummary));
                    session.closeAsync().toCompletableFuture().join();
                }).toCompletableFuture();
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return future.cancel(mayInterruptIfRunning);
    }

    @Override
    public void close() throws Exception {

    }
}
