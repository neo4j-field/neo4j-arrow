package org.neo4j.arrow.job;

import org.neo4j.arrow.DriverRecord;
import org.neo4j.arrow.RowBasedRecord;
import org.neo4j.arrow.action.CypherMessage;
import org.neo4j.driver.*;
import org.neo4j.driver.async.AsyncSession;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

/**
 * Implementation of a Neo4jJob that uses an AsyncSession via the Neo4j Java Driver.
 */
public class AsyncDriverJob extends Job {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(AsyncDriverJob.class);

    /* 1 Driver per identity */
    private static ConcurrentMap<AuthToken, Driver> driverMap = new ConcurrentHashMap<>();

    private final AsyncSession session;
    private final CompletableFuture future;

    public AsyncDriverJob(CypherMessage msg, Mode mode, AuthToken authToken) {
        super();

        Driver driver;
        if (!driverMap.containsKey(authToken)) {
            org.neo4j.driver.Config.ConfigBuilder builder = org.neo4j.driver.Config.builder();
            driver = GraphDatabase.driver(org.neo4j.arrow.Config.neo4jUrl, authToken,
                    builder.withUserAgent("Neo4j-Arrow-Proxy/alpha")
                            .withMaxConnectionPoolSize(8)
                            .withFetchSize(org.neo4j.arrow.Config.boltFetchSize)
                            .build());
            driverMap.put(authToken, driver);
        } else {
            driver = driverMap.get(authToken);
        }

        this.session = driver.asyncSession(SessionConfig.builder()
                .withDatabase(org.neo4j.arrow.Config.database)
                .withDefaultAccessMode(AccessMode.valueOf(mode.name()))
                .build());

        future = session.runAsync(msg.getCypher(), msg.getParams())
                .thenComposeAsync(resultCursor -> {
                    logger.info("Job {} producing", session);
                    setStatus(Status.PRODUCING);

                    /* We need to inspect the first record and guess at a schema :-( */
                    final Record firstRecord = resultCursor.peekAsync().toCompletableFuture().join();
                    onFirstRecord(DriverRecord.wrap(firstRecord));

                    final Consumer<RowBasedRecord> consumer = futureConsumer.join();
                    return resultCursor.forEachAsync(record ->
                            consumer.accept(DriverRecord.wrap(record)));
                }).whenCompleteAsync((resultSummary, throwable) -> {
                    if (throwable != null) {
                        setStatus(Status.ERROR);
                        logger.error("Job failure", throwable);
                    } else {
                        logger.info("Job {} complete", session);
                        setStatus(Status.COMPLETE);
                    }
                    onCompletion(DriverJobSummary.wrap(resultSummary));
                    session.closeAsync().toCompletableFuture().join();
                }).toCompletableFuture();
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return future.cancel(mayInterruptIfRunning);
    }

    @Override
    public void close() throws Exception {
        future.cancel(true);
        session.closeAsync();
    }
}
