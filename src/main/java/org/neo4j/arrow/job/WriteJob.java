package org.neo4j.arrow.job;


import org.neo4j.arrow.RowBasedRecord;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public abstract class WriteJob extends Job {

    public static final Mode mode = Mode.WRITE;

    private Consumer<Node> nodeConsumer;
    private CompletableFuture<Void> streamComplete = new CompletableFuture<>();

    public WriteJob() {
        super();
    }

    public static class Node {
        public final long nodeId;
        public final Map<String, RowBasedRecord.Value> properties;
        public final String[] labels;

        public Node(long nodeId, Map<String, RowBasedRecord.Value> properties, String... labels) {
            this.nodeId = nodeId;
            this.properties = properties;
            this.labels = labels;
        }

        public static Node of(long nodeId, Map<String, RowBasedRecord.Value> properties, String... labels) {
            return new Node(nodeId, properties, labels);
        }
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

    public void setConsumer(Consumer<Node> consumer) {
        this.nodeConsumer = consumer;
    }

    public Consumer<Node> getConsumer() {
        return nodeConsumer;
    }
}
