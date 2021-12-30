package org.neo4j.arrow.job;


import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.neo4j.arrow.Config;
import org.neo4j.arrow.batch.ArrowBatch;
import org.neo4j.arrow.batch.ArrowBatches;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

public abstract class WriteJob extends Job {

    public static final Mode mode = Mode.WRITE;

    private final BufferAllocator allocator;
    private CompletableFuture<Schema> schema = new CompletableFuture<>();

    private final CompletableFuture<Void> streamComplete = new CompletableFuture<>();

    public WriteJob(BufferAllocator parentAllocator) {
        super();
        allocator = parentAllocator.newChildAllocator(this.getJobId(), 0L, Config.maxStreamMemory);
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public void close() throws Exception {
        allocator.close();
    }

    public Future<Void> getStreamCompletion() {
        return streamComplete;
    }

    public void onComplete() {
        streamComplete.complete(null);
    }

    public void onSchema(Schema schema) {
        this.schema.complete(schema);
    }

    public CompletableFuture<Schema> getSchema() {
        return schema;
    }

    public abstract Consumer<ArrowBatch> getConsumer();

    public BufferAllocator getAllocator() {
        return allocator;
    }

}
