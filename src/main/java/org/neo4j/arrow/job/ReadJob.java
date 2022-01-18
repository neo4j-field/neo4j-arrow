package org.neo4j.arrow.job;

import org.neo4j.arrow.Config;
import org.neo4j.arrow.RowBasedRecord;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;

public abstract class ReadJob extends Job {

    public static final Mode mode = Mode.READ;

    /** Completes once the first record is received and ready from the Neo4j system. */
    private final CompletableFuture<RowBasedRecord> firstRecord = new CompletableFuture<>();

    /** Provides a {@link BiConsumer} taking a {@link RowBasedRecord} with data and a partition id {@link Integer} */
    protected final CompletableFuture<BiConsumer<RowBasedRecord, Integer>> futureConsumer = new CompletableFuture<>();

    /** Maximum number of simultaneous buffers to populate */
    public final int maxPartitionCnt;

    /** Maximum number of "rows" in a batch, i.e. the max vector dimension. */
    public final int maxRowCount;

    /** Maximum entries in a variable size list. Selectively used by certain jobs. */
    public final int maxVariableListSize;

    protected ReadJob() {
        this(Config.arrowMaxPartitions, Config.arrowBatchSize, Config.arrowMaxListSize);
    }

    protected ReadJob(int maxPartitionCnt, int maxRowCount, int maxVariableListSize) {
        this.maxPartitionCnt = maxPartitionCnt;
        this.maxRowCount = maxRowCount;
        this.maxVariableListSize = maxVariableListSize;
    }

    protected void onFirstRecord(RowBasedRecord record) {
        logger.debug("First record received {}", record);
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
