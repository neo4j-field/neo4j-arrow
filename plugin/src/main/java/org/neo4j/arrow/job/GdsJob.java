package org.neo4j.arrow.job;

import org.apache.arrow.flight.CallStatus;
import org.neo4j.arrow.GdsNodeRecord;
import org.neo4j.arrow.RowBasedRecord;
import org.neo4j.arrow.action.GdsMessage;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.core.loading.ImmutableCatalogRequest;
import org.neo4j.graphalgo.core.utils.collection.primitive.PrimitiveLongIterator;
import org.neo4j.logging.Log;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Interact directly with the GDS in-memory Graph, allowing for reads of node properties.
 */
public class GdsJob extends Job {
    private final CompletableFuture<Boolean> future;

    /**
     * Create a new GdsJob for processing the given {@link GdsMessage}.
     * <p>
     * The supplied username is assumed to allow GDS to enforce authorization and is assumed to be
     * previously authenticated.
     *
     * @param msg the {@link GdsMessage} to process in the job
     * @param username an already authenticated username
     * @param log the Neo4j log instance
     */
    public GdsJob(GdsMessage msg, String username, Log log) throws RuntimeException {
        super();
        log.info("GdsJob called for msg: %s", msg);

        final GraphStore store = GraphStoreCatalog.get(
                ImmutableCatalogRequest.of(username, msg.getDbName()), msg.getGraphName())
                .graphStore();
        log.info("got graphstore for graph named %s", msg.getGraphName());

        Graph graph;
        if (msg.getRequestType() == GdsMessage.RequestType.node) {
            final Collection<NodeLabel> labels = msg.getFilters()
                    .stream().map(NodeLabel::of).collect(Collectors.toUnmodifiableList());
            graph = labels.size() > 0 ? store.getGraph(labels, store.relationshipTypes(), Optional.empty())
                    : store.getGraph(store.nodeLabels(), store.relationshipTypes(), Optional.empty());
        } else {
            throw CallStatus.UNIMPLEMENTED.withDescription("can't do rels yet :'(").toRuntimeException();
        }
        log.info("got graph for labels %s, relationship types %s", store.nodeLabels(), store.relationshipTypes());

        // TODO: logic for rels
        final PrimitiveLongIterator iterator = graph.nodeIterator();

        // Make sure we have the requested node properties
        for (String key : msg.getProperties()) {
            if (!store.hasNodeProperty(store.nodeLabels(), key)) {
                log.error("no node property found for %s", key);
                throw CallStatus.NOT_FOUND
                        .withDescription(String.format("no node property found for %s", key))
                        .toRuntimeException();
            }
        }

        // Setup some arrays
        final String[] keys = msg.getProperties().toArray(new String[0]);
        final NodeProperties[] propertiesArray = new NodeProperties[keys.length];
        for (int i=0; i<keys.length; i++)
            propertiesArray[i] = store.nodePropertyValues(keys[i]);

        future = CompletableFuture.supplyAsync(() -> {
            // XXX: hacky get first node...assume it exists
            long nodeId = iterator.next();
            GdsNodeRecord record = GdsNodeRecord.wrap(nodeId, keys, propertiesArray, graph::toOriginalNodeId);
            onFirstRecord(record);
            log.debug("got first record");
            for (int i=0; i<keys.length; i++)
                log.info("  %s -> %s", keys[i], propertiesArray[i].valueType());

            final Consumer<RowBasedRecord> consumer = futureConsumer.join();

            // Blast off!
            // TODO: GDS lets us batch access to lists of nodes...future opportunity?
            final long start = System.currentTimeMillis();
            consumer.accept(record);
            while (iterator.hasNext()) {
                consumer.accept(GdsNodeRecord.wrap(iterator.next(), keys, propertiesArray, graph::toOriginalNodeId));
            }
            final long delta = System.currentTimeMillis() - start;

            onCompletion(() -> String.format("finished GDS stream, duration %,d ms", delta));
            return true;
        }).exceptionally(throwable -> {
            log.error(throwable.getMessage(), throwable);
            return false;
        }).handleAsync((aBoolean, throwable) -> {
            log.info("job completed. result: " + (aBoolean == null ? "failed" : "ok!"));
            if (throwable != null)
                log.error(throwable.getMessage(), throwable);
            return false;
        });
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return future.cancel(mayInterruptIfRunning);
    }

    @Override
    public void close() {
        future.cancel(true);
    }
}
