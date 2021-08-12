package org.neo4j.arrow.job;

import org.neo4j.arrow.RowBasedRecord;
import org.neo4j.arrow.action.CypherMessage;
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
        super();
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

            // TODO: support property other than 'n'
            final String propertykey = "n";
            final NodeProperties properties = graph.nodeProperties(propertykey);

            // XXX: hacky get first node...assume it exists
            long nodeId = iterator.next();
            onFirstRecord(GdsRecord.wrap(properties, nodeId));
            log.info("got first record");
            log.info(String.format("  %s -> %s", propertykey, properties.valueType()));

            final Consumer<RowBasedRecord> consumer = futureConsumer.join();

            // Blast off!
            consumer.accept(GdsRecord.wrap(properties, nodeId));
            while (iterator.hasNext()) {
                consumer.accept(GdsRecord.wrap(properties, iterator.next()));
            }

            log.info("finishing stream");
            onCompletion(() -> "done");
            return true;
        }).exceptionally(throwable -> {
            log.error(throwable.getMessage(), throwable);
            return false;
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
