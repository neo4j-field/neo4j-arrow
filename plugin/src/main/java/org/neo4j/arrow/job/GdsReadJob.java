package org.neo4j.arrow.job;

import com.google.common.collect.Iterators;
import org.apache.arrow.flight.CallStatus;
import org.apache.commons.lang3.tuple.Triple;
import org.neo4j.arrow.*;
import org.neo4j.arrow.action.GdsMessage;
import org.neo4j.arrow.action.KHopMessage;
import org.neo4j.arrow.gds.KHop;
import org.neo4j.arrow.gds.SuperNodeCache;
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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
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
    private final CompletableFuture<Boolean> aborted = new CompletableFuture<>();

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
        super(msg.getPartitionCnt(), msg.getBatchSize(), msg.getListSize());
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
                final KHopMessage kmsg = new KHopMessage(msg.getDbName(), msg.getGraphName(), 2, msg.getProperties().get(0));
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
        }, nodeCount, Spliterator.SIZED | Spliterator.DISTINCT | Spliterator.ORDERED | Spliterator.NONNULL);
    }

    // For now until I refactor the API, just move this stuff out into a static area.
    private static final AtomicInteger partitionCounter = new AtomicInteger(0);
    private static Consumer<RowBasedRecord> wrapConsumer(BiConsumer<RowBasedRecord, Integer> consumer) {
        return (row) -> consumer.accept(row, Math.abs(partitionCounter.getAndIncrement()));
    }

    private CompletableFuture<Boolean> handleKHopJob(KHopMessage msg, GraphStore store) {
        final Graph graph = store.getGraph(store.nodeLabels(), store.relationshipTypes(),
                Optional.of(msg.getRelProperty()));
        final long nodeCount = graph.nodeCount();
        final int k = msg.getK();
        assert(k == 2); // XXX optimizations for this condition exist for now

        // XXX faux record...needed to progress the job status
        onFirstRecord(SubGraphRecord.of(0, new int[]{1}, new int[]{2}));

        return CompletableFuture.supplyAsync(() -> {
            logger.info(String.format("starting node stream for gds khop job %s (%,d nodes, %,d rels)",
                    jobId, nodeCount, graph.relationshipCount()));

            /*
             * "Triples makes it safe. Triples is best."
             *   -- Your dad's old friend (https://www.youtube.com/watch?v=8Inf1Yz_fgk)
             *
             * We'll encode a "triple" effectively in a 64-bit Java long, leaning on the
             * assumption that we have < 1B nodes...
             */
            final Collection<Long> superNodes = KHop.findSupernodes(graph);
            final SuperNodeCache supernodeCache = KHop.populateCache(graph, superNodes);
            final Consumer<RowBasedRecord> consumer = wrapConsumer(futureConsumer.join()); // XXX join()

            /*
             * Core k-hop work logic. Given a start node (origin), perform the k-hop.
             * Return the total number of records created.
             */
            final Function<Integer, Long> processNode = (origin) -> {
                final LongStream stream = KHop.stream(origin, k, graph, supernodeCache);
                final AtomicLong cnt = new AtomicLong(0);
                Iterators.partition(stream.iterator(), this.maxVariableListSize)
                        .forEachRemaining(batch -> {
                            consumer.accept(SubGraphRecord.of(origin, batch, batch.size()));
                            cnt.incrementAndGet();
                        });

                return cnt.get();
            };

            /*
             * Hacky way to constrain how many streams we process simultaneously. We'll use a thread pool
             * and semaphores. This also lets us name the threads.
             */
            final Executor khopExecutor = KHop.buildKhopExecutor();
            final int tickets = Math.max(1, Runtime.getRuntime().availableProcessors() - 4);
            final Semaphore semaphore = new Semaphore(tickets);
            final AtomicLong rowCnt = new AtomicLong(0);

            /*
             * Utilize a semaphore with a finite amount of permits/tickets to choke the
             * number of concurrent threads without creating hundreds of thousands of
             * Runnables on the heap. A countdown latch is used to wait for the final
             * batch of workers to complete.
             */
            final CountDownLatch countDownLatch = new CountDownLatch((int) nodeCount); // XXX cast
            for (long id = 0; id < nodeCount && !aborted.isDone(); id++) {
                    final int nodeId = (int) id; // XXX cast
                    try {
                        semaphore.acquire();
                        khopExecutor.execute(() -> {
                            try {
                                final long cnt = rowCnt.addAndGet(processNode.apply(nodeId));
                                if (nodeId > 0 && nodeId % 10_000 == 0) {
                                    logger.info(String.format("at node %,d, rowCnt: %,d", nodeId, cnt));
                                }
                            } finally {
                                semaphore.release();
                                countDownLatch.countDown();
                            }
                        });
                    } catch (InterruptedException e) {
                        logger.error("interrupted in khop", e);
                        break;
                    }
            }

            /*
             * Wait for our last streams to finish or a cancellation to be received.
             */
            final AtomicBoolean result = new AtomicBoolean(false);
            try {
                CompletableFuture.anyOf(
                        aborted,
                        CompletableFuture.supplyAsync(() -> {
                            try {
                                return countDownLatch.await(24, TimeUnit.HOURS);
                            }
                            catch (Exception e) {
                                logger.error("error acquiring all semaphore tickets", e);
                                return false;
                            }
                        })
                ).whenComplete((obj, throwable) -> {
                    if (obj instanceof Boolean && throwable == null) {
                        if (((Boolean)obj) ) { // things are ok if true
                            logger.info(String.format("completed gds k-hop job: %,d nodes, %,d total rows", nodeCount, rowCnt.get()));
                            result.set(true);
                        } else {
                            logger.error("something failed in k-hop job");
                        }
                    } else {
                        logger.error("error in k-hop: " + throwable.getMessage(), throwable);
                    }
                }).join();
            } catch (Exception e) {
                logger.error("unhandled exception waiting for k-hop to complete");
            }
            return result.get();
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

        // Make sure we have the requested node properties and any request node id property
        for (String key : Stream.concat(Stream.of(msg.getNodeIdProperty()), msg.getProperties().stream())
                .filter(s -> !s.isBlank()).collect(Collectors.toUnmodifiableList())) {
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
        final NodeProperties idProperties = msg.getNodeIdProperty().isBlank()
                ? null : store.nodePropertyValues(msg.getNodeIdProperty());

        final PrimitiveLongIterator iterator = graph.nodeIterator();

        // TODO: support node id properties other than longs
        final Function<Long, Long> nodeIdResolver = idProperties != null ? idProperties::longValue : graph::toOriginalNodeId;

        return CompletableFuture.supplyAsync(() -> {
            // XXX: hacky get first node...assume it exists
            long nodeId = iterator.next();
            GdsNodeRecord record = GdsNodeRecord.wrap(nodeId, graph.nodeLabels(nodeId), keys, propertiesArray, nodeIdResolver);
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
                    consumer.accept(GdsNodeRecord.wrap(i, graph.nodeLabels(i), keys, propertiesArray, nodeIdResolver),
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
        aborted.complete(true);
        return future.cancel(mayInterruptIfRunning);
    }

    @Override
    public void close() {
        aborted.complete(true);
        future.cancel(true);
    }
}
