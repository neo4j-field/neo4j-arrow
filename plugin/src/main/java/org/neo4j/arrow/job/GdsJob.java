package org.neo4j.arrow.job;

import org.neo4j.arrow.GdsRecord;
import org.neo4j.arrow.RowBasedRecord;
import org.neo4j.arrow.action.GdsMessage;
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

    public GdsJob(GdsMessage msg, Mode mode, String username, Log log) {
        super();
        log.info("GdsJob called");

        final GraphStore store = GraphStoreCatalog.get(
                ImmutableCatalogRequest.of(username, msg.getDbName()), msg.getGraphName())
                .graphStore();
        log.info("got graphstore for graph named %s", msg.getGraphName());

        // just get all stuff for now
        final Graph graph = store
                .getGraph(store.nodeLabels(), store.relationshipTypes(), Optional.empty());
        log.info("got graph for labels %s, relationship types %s", store.nodeLabels(), store.relationshipTypes());

        future = CompletableFuture.supplyAsync(() -> {
            log.info("...starting streaming future...");

            // TODO: inspect the schema via the Graph instance...need to change the Job message type
            final PrimitiveLongIterator iterator = graph.nodeIterator();

            // TODO: support more than 1 property in the request
            final String propertyName = msg.getProperties().get(0);
            final NodeProperties properties = graph.nodeProperties(propertyName);

            if (properties == null) {
                log.error("no node property found for %s", propertyName);
                return false;
            }

            // XXX: hacky get first node...assume it exists
            long nodeId = iterator.next();
            onFirstRecord(GdsRecord.wrap(nodeId, propertyName, properties));
            log.info("got first record");
            log.info(String.format("  %s -> %s", propertyName, properties.valueType()));

            final Consumer<RowBasedRecord> consumer = futureConsumer.join();

            // Blast off!
            consumer.accept(GdsRecord.wrap(nodeId, propertyName, properties));
            while (iterator.hasNext()) {
                consumer.accept(GdsRecord.wrap(nodeId, propertyName, properties));
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
