package org.neo4j.arrow.job;

import org.apache.arrow.flight.CallStatus;
import org.apache.commons.lang3.tuple.Triple;
import org.neo4j.arrow.GdsNodeRecord;
import org.neo4j.arrow.GdsRecord;
import org.neo4j.arrow.GdsRelationshipRecord;
import org.neo4j.arrow.RowBasedRecord;
import org.neo4j.arrow.action.GdsMessage;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.loading.ImmutableCatalogRequest;
import org.neo4j.gds.core.utils.collection.primitive.PrimitiveLongIterator;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.StreamSupport;

/**
 * Interact directly with the GDS in-memory Graph, allowing for reads of node properties.
 */
public class GdsReadJob extends ReadJob {
    private final CompletableFuture<Boolean> future;

    /**
     * Create a new GdsReadJob for processing the given {@link GdsMessage} that reads Node or
     * Relationship properties from the GDS in-memory graph(s).
     * <p>
     * The supplied username is assumed to allow GDS to enforce authorization and is assumed to be
     * previously authenticated.
     *
     * @param msg the {@link GdsMessage} to process in the job
     * @param username an already authenticated username
     */
    public GdsReadJob(GdsMessage msg, String username) throws RuntimeException {
        super();
        final CompletableFuture<Boolean> job;
        logger.info("GdsReadJob called with msg: {}", msg);

        // TODO: error handling for graph store retrieval
        final GraphStore store = GraphStoreCatalog.get(
                ImmutableCatalogRequest.of(username, msg.getDbName()), msg.getGraphName())
                .graphStore();
        logger.info("got GraphStore for graph named {}", msg.getGraphName());

        switch (msg.getRequestType()) {
            case node:
                job = handleNodeJob(msg, store);
                break;
            case relationship:
                job = handleRelationshipsJob(msg, store);
                break;
            default:
                throw CallStatus.UNIMPLEMENTED.withDescription("unhandled request type").toRuntimeException();
        }

        future = job.exceptionally(throwable -> {
            logger.error(throwable.getMessage(), throwable);
            return false;
        }).handleAsync((aBoolean, throwable) -> {
            logger.info("GdsReadJob completed! result: {}", (aBoolean == null ? "failed" : "ok!"));
            if (throwable != null)
                logger.error(throwable.getMessage(), throwable);
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

    protected CompletableFuture<Boolean> handleRelationshipsJob(GdsMessage msg, GraphStore store) {
        // TODO: support both rel type and node label filtering
        final Collection<RelationshipType> relTypes = (msg.getFilters().size() > 0) ?
                msg.getFilters().stream()
                    .map(RelationshipType::of)
                    .filter(store::hasRelationshipType)
                    .collect(Collectors.toUnmodifiableList())
                : store.relationshipTypes();
        logger.info("proceeding with relTypes: {}", relTypes);

        // Make sure we have the requested rel properties.
        // TODO: nested for-loop is ugly
        for (String key : msg.getProperties()) {
            boolean found = false;
            for (RelationshipType type : relTypes) {
                logger.info("type {} has key {}", type.name(), key);
                if (store.hasRelationshipProperty(type, key)) {
                    logger.info("  yes!");
                    found = true;
                    break;
                } else {
                    logger.info("  nope.");
                }
            }
            if (!found) {
                logger.error("no relationship property found for {}", key);
                throw CallStatus.NOT_FOUND
                        .withDescription(String.format("no relationship property found for %s", key))
                        .toRuntimeException();
            }
        }

        final Graph baseGraph = (relTypes.size() > 0) ?
                store.getGraph(store.nodeLabels(), relTypes, Optional.empty())
                : store.getGraph(store.nodeLabels(), store.relationshipTypes(), Optional.empty());
        logger.info("got graph for labels {}, relationship types {}", baseGraph.schema().nodeSchema().availableLabels(),
                baseGraph.schema().relationshipSchema().availableTypes());

        // Borrow the approach used by gds.graph.streamRelationshipProperties()...i.e. build triples
        // of relationship types, property keys, and references to filtered graph views.
        // XXX for now (as of v1.7.x) all rel properties are doubles, but this could change
        final String[] keys = msg.getProperties().toArray(new String[0]);
        var triples = relTypes.stream()
                .flatMap(relType -> Arrays.stream(keys)
                        .filter(key -> store.hasRelationshipProperty(relType, key))
                        .map(key -> Triple.of(relType, key, store.getGraph(relType, Optional.of(key)))))
                .toArray(Triple[]::new);

        if (baseGraph.nodeCount() == 0)
            throw CallStatus.NOT_FOUND.withDescription("no matching node ids for GDS job").toRuntimeException();

        AtomicInteger rowCnt = new AtomicInteger(0);

        return CompletableFuture.supplyAsync(() -> {
            // XXX We cheat and make a fake record just to communicate schema :-(
            GdsRelationshipRecord fauxRecord = new GdsRelationshipRecord(0, 1, "type", "key",
                    GdsRecord.wrapScalar(0.0d, ValueType.DOUBLE));
            onFirstRecord(fauxRecord);

            // Make rocket go now
            final BiConsumer<RowBasedRecord, Integer> consumer = futureConsumer.join();

            LongStream.range(0, baseGraph.nodeCount())
                    .parallel()
                    .boxed()
                    .forEach(nodeId -> {
                        final long originalNodeId = baseGraph.toOriginalNodeId(nodeId);
                        Arrays.stream(triples)
                                .flatMap(triple -> {
                                    final RelationshipType type = (RelationshipType) triple.getLeft();
                                    final String key = (String) triple.getMiddle();
                                    final Graph graph = ((Graph) triple.getRight()).concurrentCopy();

                                    return graph
                                            .streamRelationships(nodeId, Double.NaN)
                                            .map(cursor -> new GdsRelationshipRecord(
                                                    originalNodeId,
                                                    graph.toOriginalNodeId(cursor.targetId()),
                                                    type.name(),
                                                    key,
                                                    GdsRecord.wrapScalar(cursor.property(), ValueType.DOUBLE)));
                                }).forEach(record -> consumer.accept(record, rowCnt.getAndIncrement()));
                    });

            final String summary = String.format("finished generating GDS rel stream, fed %,d rows", rowCnt.get());
            logger.info(summary);
            onCompletion(() -> summary);
            return true;
        }).exceptionally(throwable -> {
            logger.error(throwable.getMessage(), throwable);
            return false;
        }).handleAsync((aBoolean, throwable) -> {
            logger.info("gds job completed. result: {}", (aBoolean == null ? "failed" : "ok!"));
            if (throwable != null)
                logger.error(throwable.getMessage(), throwable);
            return true;
        });
    }

    protected CompletableFuture<Boolean> handleNodeJob(GdsMessage msg, GraphStore store) {
        final Collection<NodeLabel> labels = msg.getFilters()
                .stream().map(NodeLabel::of).collect(Collectors.toUnmodifiableList());

        final Graph graph = (labels.size() > 0) ?
                store.getGraph(labels, store.relationshipTypes(), Optional.empty())
                : store.getGraph(store.nodeLabels(), store.relationshipTypes(), Optional.empty());
        logger.info("got graph for labels {}, relationship types {}", store.nodeLabels(),
                store.relationshipTypes());

        // Make sure we have the requested node properties
        for (String key : msg.getProperties()) {
            if (!store.hasNodeProperty(store.nodeLabels(), key)) {
                logger.error("no node property found for {}", key);
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
            logger.debug("got first record");
            for (int i=0; i<keys.length; i++)
                logger.info(" {} -> {}", keys[i], propertiesArray[i].valueType());

            final BiConsumer<RowBasedRecord, Integer> consumer = futureConsumer.join();

            // Blast off!
            // TODO: GDS lets us batch access to lists of nodes...future opportunity?
            final long start = System.currentTimeMillis();
            consumer.accept(record, 0);

            // TODO: should it be nodeCount - 1? We advanced the iterator...maybe?
            var s = spliterate(iterator, graph.nodeCount() - 1);
            StreamSupport.stream(s, true).forEach(i ->
                    consumer.accept(GdsNodeRecord.wrap(i, keys, propertiesArray, graph::toOriginalNodeId),
                            i.intValue()));

            final long delta = System.currentTimeMillis() - start;

            onCompletion(() -> String.format("finished generating GDS stream, duration %,d ms", delta));
            return true;
        }).exceptionally(throwable -> {
            logger.error(throwable.getMessage(), throwable);
            return false;
        }).handleAsync((aBoolean, throwable) -> {
            logger.info("gds job completed. result: {}", (aBoolean == null ? "failed" : "ok!"));
            if (throwable != null)
                logger.error(throwable.getMessage(), throwable);
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
