package org.neo4j.arrow.job;

import org.apache.arrow.flight.CallStatus;
import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.arrow.GdsNodeRecord;
import org.neo4j.arrow.GdsRelRecord;
import org.neo4j.arrow.RowBasedRecord;
import org.neo4j.arrow.action.GdsMessage;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.loading.ImmutableCatalogRequest;
import org.neo4j.gds.core.utils.collection.primitive.PrimitiveLongIterator;
import org.neo4j.logging.Log;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Interact directly with the GDS in-memory Graph, allowing for reads of node properties.
 */
public class GdsJob extends Job {
    private final CompletableFuture<Boolean> future;
    private final Log log;

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
        final CompletableFuture<Boolean> job;
        this.log = log;
        log.info("GdsJob called for msg: %s", msg);

        final GraphStore store = GraphStoreCatalog.get(
                ImmutableCatalogRequest.of(username, msg.getDbName()), msg.getGraphName())
                .graphStore();
        log.info("got GraphStore for graph named %s", msg.getGraphName());

        switch (msg.getRequestType()) {
            case node:
                job = handleNodeJob(msg, store);
                break;
            case relationship:
                job = handleRelsJob(msg, store);
                break;
            default:
                throw CallStatus.UNIMPLEMENTED.withDescription("unhandled request type").toRuntimeException();
        }

        future = job.exceptionally(throwable -> {
            log.error(throwable.getMessage(), throwable);
            return false;
        }).handleAsync((aBoolean, throwable) -> {
            log.info("job completed. result: " + (aBoolean == null ? "failed" : "ok!"));
            if (throwable != null)
                log.error(throwable.getMessage(), throwable);
            return false;
        });
    }

    private Spliterator.OfLong spliterate(PrimitiveLongIterator iterator, long nodeCount) {
        return Spliterators.spliterator(new PrimitiveIterator.OfLong() {
            @Override
            public long nextLong() {
                return iterator.next();
            }

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }
        }, nodeCount, 0);
    }

    protected CompletableFuture<Boolean> handleRelsJob(GdsMessage msg, GraphStore store) {
        // TODO: support both rel type and node label filtering
        final Collection<RelationshipType> relTypes = (msg.getFilters().size() > 0) ?
                msg.getFilters().stream()
                    .map(RelationshipType::of)
                    .filter(store::hasRelationshipType)
                    .collect(Collectors.toUnmodifiableList())
                : store.relationshipTypes();

        // Make sure we have the requested rel properties.
        // TODO: nested for-loop is ugly
        for (String key : msg.getProperties()) {
            boolean found = false;
            for (RelationshipType type : relTypes) {
                if (store.hasRelationshipProperty(type, key))
                    found = true;
                    break;
            }
            if (!found) {
                log.error("no relationship property found for %s", key);
                throw CallStatus.NOT_FOUND
                        .withDescription(String.format("no relationship property found for %s", key))
                        .toRuntimeException();
            }
        }

        final Graph baseGraph = (relTypes.size() > 0) ?
                store.getGraph(store.nodeLabels(), relTypes, Optional.empty())
                : store.getGraph(store.nodeLabels(), store.relationshipTypes(), Optional.empty());
        log.info("got graph for labels %s, relationship types %s", store.nodeLabels(),
                store.relationshipTypes());

        // Setup some arrays
        // XXX for now (v1.6.4) all rel properties are doubles, but this could change
        final String[] keys = msg.getProperties().toArray(new String[0]);
        final var graphPairs = relTypes.stream()
                //.map(relType -> Pair.of(relType,
                //        store.getGraph(relType, Optional.of())))
                .toArray(Pair[]::new);

        // TODO: optimize?
        final PrimitiveLongIterator iterator = baseGraph.nodeIterator();
        return CompletableFuture.supplyAsync(() -> {
            long nodeId = iterator.next();

            // We cheat and make a fake record just to communicate schema
            GdsRelRecord firstRecord;
            //baseGraph.

            return true;
        });
    }

    protected CompletableFuture<Boolean> handleNodeJob(GdsMessage msg, GraphStore store) {
        final Collection<NodeLabel> labels = msg.getFilters()
                .stream().map(NodeLabel::of).collect(Collectors.toUnmodifiableList());

        final Graph graph = (labels.size() > 0) ?
                store.getGraph(labels, store.relationshipTypes(), Optional.empty())
                : store.getGraph(store.nodeLabels(), store.relationshipTypes(), Optional.empty());
        log.info("got graph for labels %s, relationship types %s", store.nodeLabels(),
                store.relationshipTypes());

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

        final PrimitiveLongIterator iterator = graph.nodeIterator();

        return CompletableFuture.supplyAsync(() -> {
            // XXX: hacky get first node...assume it exists
            long nodeId = iterator.next();
            GdsNodeRecord record = GdsNodeRecord.wrap(nodeId, keys, propertiesArray, graph::toOriginalNodeId);
            onFirstRecord(record);
            log.debug("got first record");
            for (int i=0; i<keys.length; i++)
                log.info("  %s -> %s", keys[i], propertiesArray[i].valueType());

            final BiConsumer<RowBasedRecord, Integer> consumer = futureConsumer.join();

            // Blast off!
            // TODO: GDS lets us batch access to lists of nodes...future opportunity?
            final long start = System.currentTimeMillis();
            consumer.accept(record, 0);

            // TODO: should it be nodeCount - 1? We advanced the iterator...maybe?
            var s = spliterate(iterator, graph.nodeCount() - 1);
            StreamSupport.stream(s, true).parallel().forEach(i ->
                    consumer.accept(GdsNodeRecord.wrap(i, keys, propertiesArray, graph::toOriginalNodeId),
                            i.intValue()));

            final long delta = System.currentTimeMillis() - start;

            onCompletion(() -> String.format("finished generating GDS stream, duration %,d ms", delta));
            return true;
        }).exceptionally(throwable -> {
            log.error(throwable.getMessage(), throwable);
            return false;
        }).handleAsync((aBoolean, throwable) -> {
            log.info("gds job completed. result: " + (aBoolean == null ? "failed" : "ok!"));
            if (throwable != null)
                log.error(throwable.getMessage(), throwable);
            return true;
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
