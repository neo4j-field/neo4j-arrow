package org.neo4j.arrow;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.summary.ResultSummary;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class Neo4jJob implements AutoCloseable, Future<ResultSummary> {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Neo4jJob.class);

    public enum Status {
        PENDING,
        PRODUCING,
        COMPLETE,
        ERROR;

        @Override
        public String toString() {
            switch (this) {
                case PENDING:
                    return "PENDING";
                case COMPLETE:
                    return "COMPLETE";
                case ERROR:
                    return "ERROR";
                case PRODUCING:
                    return "PRODUCING";
            }
            return "UNKNOWN";
        }
    }

    private final AsyncSession session;
    private AtomicReference<Status> status = new AtomicReference<>(Status.PENDING);

    private final Future<ResultSummary> future;
    private CompletableFuture<Consumer<Record>> futureConsumer = new CompletableFuture<>();
    private CompletableFuture<Void> done = new CompletableFuture<>();

    public Neo4jJob(Driver driver, CypherMessage msg, AccessMode mode) {
        session = driver.asyncSession(SessionConfig.builder()
                .withDatabase("neo4j")
                .withDefaultAccessMode(mode)
                //.withFetchSize(5000)
                .build());

        future = session.runAsync(msg.getCypher(), msg.getParams())
                .thenComposeAsync(resultCursor -> {
                    logger.info("Job {} producing", session);
                    status.set(Status.PRODUCING);

                    Record firstRecord = resultCursor.peekAsync().toCompletableFuture().join();
                    logger.info("First record received {}", firstRecord);

                    Consumer<Record> consumer = futureConsumer.join();
                    logger.info("Consuming!");
                    return resultCursor.forEachAsync(consumer);
                }).whenCompleteAsync((resultSummary, throwable) -> {
                    if (throwable != null) {
                        status.set(Status.ERROR);
                        logger.error("job failure", throwable);
                    } else {
                        logger.info("job {} complete", session);
                        status.set(Status.COMPLETE);
                    }
                    done.complete(null);

                    session.closeAsync().toCompletableFuture().join();
                }).toCompletableFuture();
    }

    public Status getStatus() {
        return status.get();
    }

    public void consume(Consumer<Record> consumer) {
        if (!futureConsumer.isDone())
            futureConsumer.complete(consumer);
        else
            logger.error("consumer already supplied for job {}", this);
    }


    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return future.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
        return future.isCancelled();
    }

    @Override
    public boolean isDone() {
        return future.isDone();
    }

    @Override
    public ResultSummary get() throws InterruptedException, ExecutionException {
        return future.get();
    }

    @Override
    public ResultSummary get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return future.get(timeout, unit);
    }


    @Override
    public void close() throws Exception {
        future.cancel(true);
    }
}
