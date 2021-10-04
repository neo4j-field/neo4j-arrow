package org.neo4j.arrow.job;


import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public abstract class WriteJob extends Job {

    public static final Mode mode = Mode.WRITE;

    private final CompletableFuture<VectorSchemaRoot> streamComplete = new CompletableFuture<>();

    public WriteJob() {
        super();
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public void close() throws Exception {
        // TODO handle writejob close???
    }

    public Future<VectorSchemaRoot> getStreamCompletion() {
        return streamComplete;
    }

    public void onComplete(VectorSchemaRoot root) {
        streamComplete.complete(root);
    }

    public abstract void onError(Exception e);
}
