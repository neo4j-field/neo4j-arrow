package org.neo4j.arrow.gds;

import com.google.common.collect.Streams;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.gds.api.Graph;
import org.roaringbitmap.BitmapDataProvider;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.RoaringBitmapWriter;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.LongStream;

/**
 * Utilities for our k-hop implementation.
 */
public class KHop {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(KHop.class);

    public static Collection<Long> findSupernodes(Graph graph) {
        final long nodeCount = graph.nodeCount();
        final long relCount = graph.relationshipCount();

        final Collection<Long> superNodes = new ConcurrentLinkedQueue<>();

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

        return superNodes;
    }

    public static SuperNodeCache populateCache(Graph graph, Collection<Long> nodes) {
        // Pre-cache supernodes adjacency lists
        if (nodes.size() > 0) {
            logger.info("caching supernodes...");
            final SuperNodeCache supernodeCache = SuperNodeCache.ofSize(nodes.size());
            nodes.parallelStream()
                    .forEach(superNodeId ->
                            supernodeCache.set(superNodeId.intValue(),
                                    graph.concurrentCopy()
                                            .streamRelationships(superNodeId, Double.NaN)
                                            .mapToLong(cursor -> {
                                                final boolean isNatural = Double.isNaN(cursor.property());
                                                return Edge.edge(cursor.sourceId(), cursor.targetId(), isNatural);
                                            }).toArray()));

            logger.info(String.format("cached %,d supernodes (%,d edges)",
                    nodes.size(), supernodeCache.stream()
                            .mapToLong(array -> array == null ? 0 : array.length).sum()));
            return supernodeCache;
        }

        return SuperNodeCache.empty();
    }

    /**
     * Perform a k'th hop from the given origin.
     *
     * @param k depth of current hop/traversal
     * @param origin starting node id for the hop
     * @param graph reference to a GDS {@link Graph}
     * @param history a stateful {@link NodeHistory} to update
     * @param cache an optional {@link SuperNodeCache} to leverage
     * @return new {@link LongStream} of {@link Edge}s as longs
     */
    protected static LongStream hop(int k, int origin, Graph graph, NodeHistory history, SuperNodeCache cache) {
        final long[] cachedList = cache.get(origin);
        final Graph copy = graph.concurrentCopy();

        final Set<Integer> dropSet = new HashSet<>();

        final LongStream stream;
        if (cachedList == null) {
            if (k == 0) {
                /*
                 * If we're at the origin, we need to collect a set of the initial neighbor ids
                 * AND find a list of targets to drop if there's a bi-directional relationship.
                 * This prevents duplicates without having to keep a huge list of edges.
                 */
                copy.streamRelationships(origin, Double.NaN)
                        .mapToInt(cursor -> (int) cursor.targetId())
                        .forEach(id -> {
                            if (history.getAndSet(id))
                                dropSet.add(id);
                        });
            }
            stream = copy
                    .streamRelationships(origin, Double.NaN)
                    .mapToLong(cursor -> Edge.edge(cursor.sourceId(), cursor.targetId(), (Double.isNaN(cursor.property()))));
        } else {
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

    public static LongStream stream(int origin, int k, Graph graph, SuperNodeCache cache) {
        final NodeHistory history = NodeHistory.given((int) graph.nodeCount()); // XXX cast
        history.getAndSet(origin);

        LongStream stream = hop(0, origin, graph, history, cache);

        // k-hop...where k == 2 for now ;) There are specific optimizations for the k=2 condition.
        for (int i = 1; i < k; i++) {
            final int n = i;
            stream = stream
                    .flatMap(edge -> Streams.concat(
                            LongStream.of(edge),
                            hop(n, Edge.targetAsInt(edge), graph, history, cache)));
        }

        return stream
                .sequential() // Be safe since we use a stateful filter
                .filter(edge -> Edge.isNatural(edge) || !history.getAndSet(Edge.targetAsInt(edge)))
                .map(edge -> (Edge.isNatural(edge)) ? edge : Edge.uniquify(edge));
    }

    public static Executor buildKhopExecutor() {
        return Executors.newCachedThreadPool((new ThreadFactoryBuilder())
                .setDaemon(true)
                .setNameFormat("khop-%d")
                .build());
    }
}
