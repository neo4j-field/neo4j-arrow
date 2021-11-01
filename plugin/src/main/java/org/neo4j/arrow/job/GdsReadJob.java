package org.neo4j.arrow.job;

import org.apache.arrow.flight.CallStatus;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.neo4j.arrow.*;
import org.neo4j.arrow.action.GdsMessage;
import org.neo4j.arrow.action.KHopMessage;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.loading.ImmutableCatalogRequest;
import org.neo4j.gds.core.utils.collection.primitive.PrimitiveLongIterator;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;
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
     * @param msg      the {@link GdsMessage} to process in the job
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
            case khop:
                final KHopMessage kmsg = new KHopMessage(msg.getDbName(), msg.getGraphName(), 2);
                job = handleKHopJob(kmsg, store);
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


    private static long index(Triple<Long, Long, Boolean> edge) {
        // XXX dragons
        final long left = (edge.getLeft() & 0x00000000FFFFFFFF) << 32;
        final long right = edge.getMiddle();
        return left | right;
    }

    private CompletableFuture<Boolean> handleKHopJob(KHopMessage msg, GraphStore store) {
        final Graph graph = store.getGraph(store.nodeLabels(), store.relationshipTypes(), Optional.of("cnt"));
        final long nodeCount = graph.nodeCount();
        final long relCount = graph.relationshipCount();
        final AtomicLong cacheHits = new AtomicLong(0);

        final int k = msg.getK();

        // TODO: pull out as static methods once we move into a dedicated class
        final Function<Triple<Long, Pair<Long, Long>, Long>, SubGraphRecord> convert =
                (triple)-> {
                    final long origin = triple.getLeft();
                    final Pair<Long, Long> edge = triple.getMiddle();

                    final long source = edge.getLeft();
                    final long target = edge.getRight();
                    // discard the Right...it's the terminus

                    return SubGraphRecord.of(graph.toOriginalNodeId(origin),
                            graph.toOriginalNodeId(source), graph.nodeLabels(source),
                            "UNKNOWN", // XXX lame
                            graph.toOriginalNodeId(target), graph.nodeLabels(target));
                };

        final Function<BiConsumer<RowBasedRecord, Integer>, Consumer<RowBasedRecord>> wrapConsumer =
                (consumer) ->
                        (row) -> consumer.accept(row, (int) row.get(1).asLong());

        // XXX analyze degrees
        final Map<Integer, Long> histogram = new ConcurrentHashMap<>();
        final Queue<Long> superNodes = new ConcurrentLinkedQueue<>();

        LongStream.range(0, nodeCount)
                .parallel()
                .mapToObj(id -> Pair.of(id, 0))
                .map(pair -> Pair.of(pair.getLeft(), graph.degree(pair.getLeft())))
                .map(pair -> (pair.getRight() == 0) ? Pair.of(pair.getLeft(), Double.NaN)
                        : Pair.of(pair.getLeft(), Math.floor(Math.log10(pair.getRight()))))
                .map(pair -> (pair.getRight().isNaN()) ? Pair.of(pair.getLeft(), 0)
                        : Pair.of(pair.getLeft(), pair.getRight().intValue() + 1))
                .forEach(pair -> {
                    int magnitude = pair.getRight();
                    histogram.compute(magnitude, (key, val) -> (val == null) ? 1L : val + 1L);
                    if (magnitude > 2) // XXX cutoff?
                        superNodes.add(pair.getLeft());
                });

        histogram.keySet().stream()
                .sorted()
                .forEach(key ->
                        logger.info(String.format("\t[ 10 * %d ]\t- %,d nodes", key, histogram.get(key))));
        histogram.clear();
        logger.info(String.format("%,d potential supernodes!", superNodes.size()));

        // XXX faux record
        onFirstRecord(SubGraphRecord.of(0L, 0L, store.nodeLabels(), "TYPE", 1L, store.nodeLabels()));

        return CompletableFuture.supplyAsync(() -> {
            logger.info(String.format("starting node stream for gds khop job %s (%,d nodes, %,d rels)",
                    jobId, nodeCount, relCount));
            final Map<Long, List<Triple<Long, Long, Boolean>>> supernodeCache = new ConcurrentHashMap<>();

            // Pre-cache supernodes adjacency lists
            logger.info("optimizing supernodes...");
            superNodes.parallelStream().forEach(superNodeId -> {
                final List<Triple<Long, Long, Boolean>> targets = new ArrayList<>(graph.degree(superNodeId));
                graph.concurrentCopy()
                        .streamRelationships(superNodeId, Double.NaN)
                        .forEach(cursor -> {
                            final long source = cursor.sourceId();
                            final long target = cursor.targetId();
                            final boolean isNatural = Double.isNaN(cursor.property());
                            final Triple<Long, Long, Boolean> edge = isNatural
                                    ? Triple.of(source, target, true) : Triple.of(target, source, false);
                            targets.add(edge);
                        });
                supernodeCache.put(superNodeId, targets);
            });
            logger.info(String.format("preprocessed %,d supernodes", superNodes.size()));

            var consume = wrapConsumer.apply(futureConsumer.join()); // XXX join()

            // Only randomize if supernodes
            final Stream<Long> nodeStream;
            if (superNodes.size() > 0) {
                final List<Long> nodeList = LongStream.range(0, nodeCount).boxed().collect(Collectors.toList());
                Collections.shuffle(nodeList); // XXX
                nodeStream = nodeList.stream();
            } else {
                nodeStream = LongStream.range(0, nodeCount).boxed();
            }

            final Pair<Long, Long> result = nodeStream
                    .parallel()
                    .map(startNodeId -> {
                        final List<Triple<Long, Long, Boolean>> cachedList = supernodeCache.get(startNodeId); // XXX unchecked
                        final Set<Long> relHistory = new ConcurrentSkipListSet<>();
                        Stream<Pair<Triple<Long, Long, Boolean>, Long>> stream; // pair of (edge, isNatural?)

                        // "Triples makes it safe. Triples is best."
                        // (result, edge, next node id)

                        if (cachedList == null) {
                            stream = graph.concurrentCopy()
                                    .streamRelationships(startNodeId, Double.NaN)
                                    .map(cursor -> {
                                        final long source = cursor.sourceId();
                                        final long target = cursor.targetId();
                                        final boolean isNatural = Double.isNaN(cursor.property());
                                        logger.trace("xxx origin={}, source={}, target={}, isNat?={}", startNodeId, source, target, isNatural);

                                        final Triple<Long, Long, Boolean> edge =
                                                isNatural ? Triple.of(source, target, true) : Triple.of(target, source, false);
                                        return Pair.of(edge, isNatural ? target : source);
                                    });
                        } else {
                            cacheHits.incrementAndGet();
                            stream = cachedList.parallelStream()
                                    .map(edge -> {
                                        final long source = edge.getLeft();
                                        final long target = edge.getMiddle();

                                        final long next = edge.getRight() ? target : source;
                                        return Pair.of(edge, next);
                                    });
                        }

                        // k-hop
                        for (int i = 1; i < k; i++) {
                            stream = stream.flatMap(triple -> {
                                final long next = triple.getRight();
                                final List<Triple<Long, Long, Boolean>> cachedList2 = supernodeCache.get(next); // XXX unchecked
                                Stream<Pair<Triple<Long, Long, Boolean>, Long>> newStream; // pair of (edge, isNatural?)

                                if (cachedList2 == null) {
                                    newStream = graph.concurrentCopy() // XXX do we need another copy?
                                            .streamRelationships(next, Double.NaN)
                                            .map(cursor -> {
                                                final long source = cursor.sourceId();
                                                final long target = cursor.targetId();
                                                final boolean isNatural = Double.isNaN(cursor.property());

                                                final long nextNext = (source == next) ? target : source;
                                                logger.trace("xxx origin={}, source={}, target={}, isNat?={}", startNodeId, source, target, isNatural);

                                                final Triple<Long, Long, Boolean> edge =
                                                        isNatural ? Triple.of(source, target, true) : Triple.of(target, source, false);
                                                return Pair.of(edge, nextNext);
                                            });
                                } else {
                                    cacheHits.incrementAndGet();
                                    newStream = cachedList2.parallelStream()
                                            .map(edge ->{
                                                final long source = edge.getLeft();
                                                final long target = edge.getMiddle();
                                                final boolean isNatural = edge.getRight();

                                                final long nextNext = (source == next) ? target : source;
                                                logger.trace("xxx origin={}, source={}, target={}, isNat?={}", startNodeId, source, target, isNatural);
                                                return Pair.of(edge, nextNext);
                                            });
                                }
                                return Stream.concat(Stream.of(triple), newStream);
                            });
                        }

                        // Consume the stream >>>
                        long cnt = stream
                                .map(Pair::getLeft)
                                .filter(edge -> !relHistory.add(index(edge)))
                                .map(edge -> {
                                    final SubGraphRecord row = SubGraphRecord.of(startNodeId,
                                            edge.getLeft(), graph.nodeLabels(edge.getLeft()),
                                            "UNKNOWN_TYPE",
                                            edge.getMiddle(), graph.nodeLabels(edge.getMiddle())
                                    );
                                    consume.accept(row);
                                    return row;
                                })
                                .peek(row -> logger.trace("{}", row))
                                .count();

                        logger.trace("finished k-hop for {} ({} rows)", startNodeId, cnt);
                        return Pair.of(1L, cnt);
                    })
                    .reduce(Pair.of(0L, 0L),
                            (p1, p2) -> Pair.of(p1.getLeft() + p2.getLeft(), p1.getRight() + p2.getRight()));

            logger.info(String.format("completed gds k-hop job: %,d nodes, %,d total rows",
                    result.getLeft(), result.getRight()));
            logger.info(String.format("cache hits: %,d", cacheHits.get()));
            return true;
        }).handleAsync((isOk, err) -> {
            if (isOk != null && isOk) {
                onCompletion(() -> String.format("job %s finished OK!", jobId));
                return true;
            }
            onCompletion(() -> String.format("job %s FAILED!", jobId));
            logger.error(String.format("job %s FAILED!", jobId), err);
            return false;
        });
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
        if (msg.getProperties() != GdsMessage.ANY_PROPERTIES) {
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
                .flatMap(relType -> {
                    logger.info("assembling triples for type {}", relType);
                    // Filtering for certain properties
                    if (msg.getProperties() != GdsMessage.ANY_PROPERTIES) {
                        return Arrays.stream(keys)
                                .filter(key -> store.hasRelationshipProperty(relType, key))
                                .map(key -> Triple.of(relType, key, store.getGraph(relType, Optional.of(key))));
                    } else {
                        logger.info("using all property keys for {}", relType);
                        return Stream.concat(
                                store.relationshipPropertyKeys(relType).stream()
                                        .map(key -> Triple.of(relType, key, store.getGraph(relType, Optional.of(key)))),
                                Stream.of(Triple.of(relType, null, store.getGraph(relType, Optional.empty())))
                        );
                    }
                })
                .peek(triple -> logger.debug("constructed triple {}", triple))
                .toArray(Triple[]::new);
        logger.info(String.format("assembled %,d triples", triples.length));

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

            logger.info(String.format("finding rels for %,d nodes", baseGraph.nodeCount()));
            LongStream.range(0, baseGraph.nodeCount())
                    .parallel()
                    .boxed()
                    .forEach(nodeId -> {
                        final long originalNodeId = baseGraph.toOriginalNodeId(nodeId);
                        // logger.info("processing node {} (originalId: {})", nodeId, originalNodeId);
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
        for (int i = 0; i < keys.length; i++)
            propertiesArray[i] = store.nodePropertyValues(keys[i]);

        final PrimitiveLongIterator iterator = graph.nodeIterator();

        return CompletableFuture.supplyAsync(() -> {
            // XXX: hacky get first node...assume it exists
            long nodeId = iterator.next();
            GdsNodeRecord record = GdsNodeRecord.wrap(nodeId, graph.nodeLabels(nodeId), keys, propertiesArray, graph::toOriginalNodeId);
            onFirstRecord(record);
            logger.debug("offered first record to producer");
            for (int i = 0; i < keys.length; i++)
                logger.info(" key {}/{}: {} -> {}", i, keys.length - 1, keys[i], propertiesArray[i].valueType());

            final BiConsumer<RowBasedRecord, Integer> consumer = futureConsumer.join();
            logger.info("acquired consumer");

            // Blast off!
            // TODO: GDS lets us batch access to lists of nodes...future opportunity?
            final long start = System.currentTimeMillis();
            consumer.accept(record, 0);

            // TODO: should it be nodeCount - 1? We advanced the iterator...maybe?
            var s = spliterate(iterator, graph.nodeCount() - 1);
            StreamSupport.stream(s, true).forEach(i ->
                    consumer.accept(GdsNodeRecord.wrap(i, graph.nodeLabels(i), keys, propertiesArray, graph::toOriginalNodeId),
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
