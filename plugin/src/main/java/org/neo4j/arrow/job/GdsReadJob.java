package org.neo4j.arrow.job;

import com.google.common.base.Predicates;
import com.google.common.collect.Iterators;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.arrow.flight.CallStatus;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.neo4j.arrow.*;
import org.neo4j.arrow.action.GdsMessage;
import org.neo4j.arrow.action.KHopMessage;
import org.neo4j.arrow.gds.Edge;
import org.neo4j.arrow.gds.NodeHistory;
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
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.*;

import static org.neo4j.arrow.gds.Edge.target;

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

    private LongStream hop(int k, long start, Graph graph, SuperNodeCache cache, AtomicLong cacheHits) {
        final long[] cachedList = cache.get((int) start); // XXX cast
        final Graph copy = graph.concurrentCopy();

        final Set<Integer> dropSet = new HashSet<>();
        final NodeHistory history = NodeHistory.given((int) graph.nodeCount()); // XXX cast

        final LongStream stream;
        if (cachedList == null) {
            if (k == 0) {
                /*
                 * If we're at the origin, we need to collect a set of the initial neighbor ids
                 * AND find a list of targets to drop if there's a bi-directional relationship.
                 * This prevents duplicates without having to keep a huge list of edges.
                 */
                copy.streamRelationships(start, Double.NaN)
                        .sequential()
                        .mapToInt(cursor -> (int) cursor.targetId())
                        .forEach(id -> {
                            if (history.getAndSet(id))
                                dropSet.add(id);
                        });
            }
            stream = copy
                    .streamRelationships(start, Double.NaN)
                    .mapToLong(cursor -> Edge.edge(cursor.sourceId(), cursor.targetId(), (Double.isNaN(cursor.property()))));
        } else {
            cacheHits.incrementAndGet();
            if (k == 0) {
                Arrays.stream(cachedList)
                        .sequential()
                        .mapToInt(Edge::targetAsInt)
                        .forEach(id -> {
                            if (history.getAndSet(id))
                                dropSet.add(id);
                        });
            }
            stream = Arrays.stream(cachedList);
        }
        if (k == 0) {
            return stream
                    .filter(edge -> Edge.isNatural(edge) || !dropSet.contains(Edge.targetAsInt(edge)));
        }
        return stream;
    }

    private CompletableFuture<Boolean> handleKHopJob(KHopMessage msg, GraphStore store) {
        final Graph graph = store.getGraph(store.nodeLabels(), store.relationshipTypes(),
                Optional.of(msg.getRelProperty()));
        final long nodeCount = graph.nodeCount();
        final long relCount = graph.relationshipCount();
        final AtomicLong cacheHits = new AtomicLong(0);

        final int k = msg.getK();

        final AtomicInteger x = new AtomicInteger(0);
        final Function<BiConsumer<RowBasedRecord, Integer>, Consumer<RowBasedRecord>> wrapConsumer =
                (consumer) ->
                        (row) -> consumer.accept(row, Math.abs(x.getAndIncrement()));

        // XXX analyze degrees
        final Queue<Long> superNodes = new ConcurrentLinkedQueue<>();

        logger.info(String.format("checking for super nodes in graph with %,d nodes, %,d edges...", nodeCount, relCount));
        final AtomicInteger[] histogram = new AtomicInteger[10];
        for (int i = 0; i < histogram.length; i++)
            histogram[i] = new AtomicInteger(0);

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
                    if (magnitude > 4) { // 100k's of edges
                        superNodes.add(pair.getLeft());
                    }
                    histogram[pair.getRight()].incrementAndGet();
                });

        logger.info(String.format("\t[ zero ]\t- %,d nodes", histogram[0].get()));
        for (int i = 1; i < histogram.length; i++)
            logger.info(String.format("  [ 1e%d - 1e%d )\t- %,d nodes", i - 1, i, histogram[i].get()));

        logger.info(String.format("%,d potential supernodes!", superNodes.size()));

        // XXX faux record
        onFirstRecord(SubGraphRecord.of(0, new int[]{1}, new int[]{2}));

        return CompletableFuture.supplyAsync(() -> {
            logger.info(String.format("starting node stream for gds khop job %s (%,d nodes, %,d rels)",
                    jobId, nodeCount, relCount));

            /*
             * "Triples makes it safe. Triples is best."
             *   -- Your dad's old friend (https://www.youtube.com/watch?v=8Inf1Yz_fgk)
             *
             * We'll encode a "triple" effectively in a 64-bit Java long, leaning on the
             * assumption that we have < 1B nodes...
             */

            final SuperNodeCache supernodeCache;
            // Pre-cache supernodes adjacency lists
            if (superNodes.size() > 0) {
                logger.info("caching supernodes...");
                supernodeCache = SuperNodeCache.ofSize(superNodes.size());
                superNodes.parallelStream()
                        .forEach(superNodeId ->
                                supernodeCache.set(superNodeId.intValue(),
                                        graph.concurrentCopy()
                                                .streamRelationships(superNodeId, Double.NaN)
                                                .mapToLong(cursor -> {
                                                    final boolean isNatural = Double.isNaN(cursor.property());
                                                    return Edge.edge(cursor.sourceId(), cursor.targetId(), isNatural);
                                                }).toArray()));

                logger.info(String.format("cached %,d supernodes (%,d edges)",
                        superNodes.size(), supernodeCache.stream()
                                .mapToLong(array -> array == null ? 0 : array.length).sum()));
            } else {
                supernodeCache = SuperNodeCache.empty();
            }

            var consume = wrapConsumer.apply(futureConsumer.join()); // XXX join()

            final Function<Long, Long> processNode = (origin) -> {
                final NodeHistory history = NodeHistory.given((int) nodeCount); // XXX cast

                LongStream stream = hop(0, origin, graph, supernodeCache, cacheHits);

                // k-hop...where k == 2 for now ;)
                for (int i = 1; i < k; i++) {
                    final int n = i;
                    stream = stream
                            .flatMap(edge -> Streams.concat(
                                    LongStream.of(edge),
                                    hop(n, target(edge), graph, supernodeCache, cacheHits)));
                }

                stream = stream
                        .sequential()
                        .filter(edge -> Edge.isNatural(edge) || !history.getAndSet(Edge.targetAsInt(edge)))
                        .map(edge -> (Edge.isNatural(edge)) ? edge : Edge.uniquify(edge));

                final AtomicLong cnt = new AtomicLong(0);
                Iterators.partition(stream.iterator(), Config.arrowMaxListSize)
                        .forEachRemaining(batch -> {
                            consume.accept(SubGraphRecord.of(origin, batch, batch.size()));
                            cnt.incrementAndGet();
                        });

                return cnt.get();
            };

            /*
             * Hacky way to constrain how many streams we process simultaneously
             */
            final ThreadFactory factory = (new ThreadFactoryBuilder())
                    .setDaemon(true)
                    .setNameFormat("khop-%d")
                    .build();
            final Executor khopExecutor = Executors.newCachedThreadPool(factory);
            // Not sure if this is needed...what happens if you blast 300m Runnables into an Executor :D
            final int tickets = Math.max(1, Runtime.getRuntime().availableProcessors() - 2);
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
                    final long nodeId = id;
                    try {
                        semaphore.acquire();
                        khopExecutor.execute(() -> {
                            try {
                                rowCnt.addAndGet(processNode.apply(nodeId));
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
                            logger.info(String.format("cache hits: %,d", cacheHits.get()));
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
        aborted.complete(true);
        return future.cancel(mayInterruptIfRunning);
    }

    @Override
    public void close() {
        aborted.complete(true);
        future.cancel(true);
    }
}
