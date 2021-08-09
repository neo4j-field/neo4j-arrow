package org.neo4j.arrow.job;

import org.neo4j.arrow.Neo4jRecord;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.core.loading.ImmutableCatalogRequest;
import org.neo4j.graphalgo.core.utils.collection.primitive.PrimitiveLongIterator;
import org.neo4j.logging.Log;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public class GdsJob extends Job {


    private final CompletableFuture<Boolean> future;

    public GdsJob(CypherMessage msg, Mode mode, Log log) {
        super(msg, mode);
        log.info("GdsJob called");

        final GraphStore store = GraphStoreCatalog.get(
                ImmutableCatalogRequest.of("neo4j", "neo4j"), "mygraph")
                .graphStore();
        log.info("got graphstore " + store.toString());

        // just get all stuff for now
        final Graph graph = store
                .getGraph(store.nodeLabels(), store.relationshipTypes(), Optional.empty());
        log.info("got graph " + graph.toString());

        future = CompletableFuture.supplyAsync(() -> {
            log.info("...starting streaming future...");

            // TODO: inspect the schema via the Graph instance...need to change the Job message type
            final PrimitiveLongIterator iterator = graph.nodeIterator();
            final NodeProperties properties = graph.nodeProperties("n");

            // get first node
            long nodeId = iterator.next();
            onFirstRecord(GdsRecord.wrap(nodeId, properties.getObject(nodeId)));
            log.info("got first record");

            final Consumer<Neo4jRecord> consumer = futureConsumer.join();
            log.info("consuming...");
            consumer.accept(GdsRecord.wrap(nodeId, properties.getObject(nodeId)));

            while (iterator.hasNext()) {
                nodeId = iterator.next();
                consumer.accept(GdsRecord.wrap(nodeId, properties.getObject(nodeId)));
            }
            log.info("finishing stream");
            onCompletion(new JobSummary() {
                @Override
                public String toString() {
                    return "done";
                }
            });
            return true;
        }).handleAsync((aBoolean, throwable) -> {
            log.info("job completed. result: " + (aBoolean == null ? "failed" : "ok!"));
            if (throwable != null)
                log.error(throwable.getMessage(), throwable);
            return false;
        }).toCompletableFuture();
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return future.cancel(mayInterruptIfRunning);
    }

    @Override
    public void close() throws Exception {
        future.cancel(true);
    }
}
