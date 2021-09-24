package org.neo4j.arrow.job;

import org.neo4j.arrow.RowBasedRecord;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;

public abstract class ReadJob extends Job {

    public static final Mode mode = Mode.READ;

    private final CompletableFuture<RowBasedRecord> firstRecord = new CompletableFuture<>();
    /** Provides a {@link BiConsumer} taking a {@link RowBasedRecord} with data and a partition id {@link Integer} */

    protected final CompletableFuture<BiConsumer<RowBasedRecord, Integer>> futureConsumer = new CompletableFuture<>();

    protected void onFirstRecord(RowBasedRecord record) {
        logger.info("First record received {}", record);
        firstRecord.complete(record);
        setStatus(Status.PENDING);
    }

    public Future<RowBasedRecord> getFirstRecord() {
        return firstRecord;
    }

    public void consume(BiConsumer<RowBasedRecord, Integer> consumer) {
        if (!futureConsumer.isDone())
            futureConsumer.complete(consumer);
        else
            logger.error("Consumer already supplied for job {}", this);
    }


}
