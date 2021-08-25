package org.neo4j.arrow.job;

import org.neo4j.arrow.RowBasedRecord;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

/**
 * The abstract base class for neo4j-arrow Jobs.
 */
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
    private final CompletableFuture<RowBasedRecord> firstRecord = new CompletableFuture<>();
    /** Provides a {@link BiConsumer} taking a {@link RowBasedRecord} with data and a partition id {@link Integer} */
    protected final CompletableFuture<BiConsumer<RowBasedRecord, Integer>> futureConsumer = new CompletableFuture<>();
    private final String jobId;

    protected Job() {
        jobId = String.format("job-%d", jobCounter.getAndIncrement());
    }

    public Status getStatus() {
        return jobStatus.get();
    }

    protected void setStatus(Status status) {
        jobStatus.set(status);
    }

    protected void onFirstRecord(RowBasedRecord record) {
        logger.debug("First record received {}", firstRecord);
        setStatus(Status.PRODUCING);
        firstRecord.complete(record);
    }

    protected void onCompletion(JobSummary summary) {
        setStatus(Status.COMPLETE);
        logger.info("Job {} completed", jobId);
        jobSummary.complete(summary);
    }

    public void consume(BiConsumer<RowBasedRecord, Integer> consumer) {
        if (!futureConsumer.isDone())
            futureConsumer.complete(consumer);
        else
            logger.error("Consumer already supplied for job {}", this);
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

    public String getJobId() {
        return jobId;
    }

    @Override
    public JobSummary get() throws InterruptedException, ExecutionException {
        return jobSummary.get();
    }

    @Override
    public JobSummary get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return jobSummary.get(timeout, unit);
    }

    public Future<RowBasedRecord> getFirstRecord() {
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
