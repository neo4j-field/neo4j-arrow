package org.neo4j.arrow.job;


import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public abstract class WriteJob extends Job {

    public static final Mode mode = Mode.WRITE;

    private BiConsumer<List<String>, List<Object>> consumer;
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
        // TODO handle writejob close???
    }

    public Future<Void> getStreamCompletion() {
        return streamComplete;
    }

    public boolean onComplete() {
        return streamComplete.complete(null);
    }

    public void setConsumer(BiConsumer<List<String>, List<Object>> consumer) {
        this.consumer = consumer;
    }

    public BiConsumer<List<String>, List<Object>> getConsumer() {
        return consumer;
    }
}
