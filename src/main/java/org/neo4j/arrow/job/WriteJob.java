package org.neo4j.arrow.job;


import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;

public abstract class WriteJob extends Job {

    public static final Mode mode = Mode.WRITE;

    private BiConsumer<Long, String[]> consumer;
    private CompletableFuture<Void> streamComplete = new CompletableFuture<>();

    public WriteJob() {
        super();
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public void close() throws Exception {

    }

    public Future<Void> getStreamCompletion() {
        return streamComplete;
    }

    public boolean onComplete() {
        return streamComplete.complete(null);
    }

    public void setConsumer(BiConsumer<Long, String[]> consumer) {
        this.consumer = consumer;
    }

    public BiConsumer<Long, String[]> getConsumer() {
        return consumer;
    }
}
