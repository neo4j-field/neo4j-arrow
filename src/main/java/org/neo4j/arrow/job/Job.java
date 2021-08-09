package org.neo4j.arrow.job;

import org.neo4j.arrow.Neo4jRecord;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public abstract class Job implements AutoCloseable, Future<JobSummary> {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Job.class);
    private static final AtomicLong jobCounter = new AtomicLong(0);

    public enum Mode {
        READ,
        WRITE
    }

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

    private final AtomicReference<Status> jobStatus = new AtomicReference<>(Status.PENDING);
    private final CompletableFuture<JobSummary> jobSummary = new CompletableFuture<>();
    private final CompletableFuture<Neo4jRecord> firstRecord = new CompletableFuture<>();
    protected final CompletableFuture<Consumer<Neo4jRecord>> futureConsumer = new CompletableFuture<>();
    private final String jobId;

    protected Job(CypherMessage msg, Mode mode) {
        jobId = String.format("job-%d", jobCounter.getAndIncrement());
    }

    public Status getStatus() {
        return jobStatus.get();
    }

    protected void setStatus(Status status) {
        jobStatus.set(status);
    }

    protected void onFirstRecord(Neo4jRecord record) {
        logger.info("First record received {}", firstRecord);
        setStatus(Status.PRODUCING);
        firstRecord.complete(record);
    }

    protected void onCompletion(JobSummary summary) {
        logger.info("Completed job {}", firstRecord);
        setStatus(Status.COMPLETE);
        jobSummary.complete(summary);
    }

    public void consume(Consumer<Neo4jRecord> consumer) {
        if (!futureConsumer.isDone())
            futureConsumer.complete(consumer);
        else
            logger.error("consumer already supplied for job {}", this);
    }


    @Override
    public abstract boolean cancel(boolean mayInterruptIfRunning);

    @Override
    public boolean isCancelled() {
        return jobSummary.isCancelled();
    }

    @Override
    public boolean isDone() {
        return jobSummary.isDone();
    }

    @Override
    public JobSummary get() throws InterruptedException, ExecutionException {
        return jobSummary.get();
    }

    @Override
    public JobSummary get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return jobSummary.get(timeout, unit);
    }

    public Future<Neo4jRecord> getFirstRecord() {
        return firstRecord;
    }

    @Override
    public abstract void close() throws Exception;

    @Override
    public String toString() {
        return "Neo4jJob{" +
                "status=" + jobStatus +
                ", id='" + jobId + '\'' +
                '}';
    }
}
