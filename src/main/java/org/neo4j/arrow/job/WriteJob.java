package org.neo4j.arrow.job;


import org.neo4j.arrow.ArrowBatch;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public abstract class WriteJob extends Job {

    public static final Mode mode = Mode.WRITE;

    private final CompletableFuture<ArrowBatch> streamComplete = new CompletableFuture<>();

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

    public Future<ArrowBatch> getStreamCompletion() {
        return streamComplete;
    }

    public void onComplete(ArrowBatch root) {
        streamComplete.complete(root);
    }

    public abstract void onError(Exception e);
}
