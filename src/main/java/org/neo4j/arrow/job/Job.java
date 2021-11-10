package org.neo4j.arrow.job;

import javax.annotation.Nonnull;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The abstract base class for neo4j-arrow Jobs.
 */
public abstract class Job implements AutoCloseable, Future<JobSummary> {
    protected static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Job.class);
    private static final AtomicLong jobCounter = new AtomicLong(0);

    protected final String jobId;

    public enum Mode {
        READ,
        WRITE
    }

    public enum Status {
        INITIALIZING,
        PENDING,
        PRODUCING,
        COMPLETE,
        ERROR;

        @Override
        public String toString() {
            switch (this) {
                case INITIALIZING:
                    return "INITIALIZING";
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

    private Status jobStatus = Status.INITIALIZING;

    protected final CompletableFuture<JobSummary> jobSummary = new CompletableFuture<>();

    protected Job() {
        jobId = String.format("job-%d", jobCounter.getAndIncrement());
    }

    public synchronized Status getStatus() {
        return jobStatus;
    }

    // XXX making public for now...API needs some future rework
    public synchronized void setStatus(Status status) {
        logger.info("status {} -> {}", jobStatus, status);
        jobStatus = status;
    }

    protected void onCompletion(JobSummary summary) {
        logger.info("Job {} completed", jobId);
        jobSummary.complete(summary);
        setStatus(Status.COMPLETE);
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
    public JobSummary get(long timeout, @Nonnull TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return jobSummary.get(timeout, unit);
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
