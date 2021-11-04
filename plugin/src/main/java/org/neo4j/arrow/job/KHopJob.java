package org.neo4j.arrow.job;

import org.apache.arrow.flight.CallStatus;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.neo4j.arrow.RowBasedRecord;
import org.neo4j.arrow.SubGraphRecord;
import org.neo4j.arrow.action.KHopMessage;
import org.neo4j.arrow.auth.NativeAuthValidator;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class KHopJob extends ReadJob {

    private final CompletableFuture<Boolean> future;
    private final AtomicBoolean alreadyFailed = new AtomicBoolean(false);

    /**
     * Generate a {@link Stream} of {@link Triple}s of {@link Relationship}s and
     * {@link Node}s reachable by a single hop from a given source {@link Node}.
     */
    private Stream<Pair<Relationship, Node>> hop(Node source, InternalTransaction tx, HugeObjectArray<List> relCache) {
        final Stream<Relationship> stream;

        final List<Long> cachedRelIds = (List<Long>) relCache.get(source.getId()); // XXX cast
        if (cachedRelIds == null) {
            final List<Long> list = new ArrayList<>(source.getDegree());
            final var spliterator = Spliterators.spliterator(
                    source.getRelationships().iterator(),
                    source.getDegree(), Spliterator.SIZED | Spliterator.IMMUTABLE | Spliterator.NONNULL | Spliterator.DISTINCT);
            stream = StreamSupport.stream(spliterator, false)
                    .peek(rel -> list.add(rel.getId()))
                    .onClose(() -> relCache.set(source.getId(), list));
        } else {
            stream = cachedRelIds.parallelStream()
                    .map(tx::getRelationshipById);
        }
        logger.trace("hopping out of ({}) (degree={})", source.getId(), source.getDegree());

        return stream.map(rel -> Pair.of(rel, rel.getOtherNode(source)));
    }

    /**
     * The core k-hop logic. Will open an {@InternalTransaction} and close it upon completion of the generated stream.
     */
    private Pair<Long, Long> khop(long startId, long maxNodeId, int k, GraphDatabaseAPI api, LoginContext context,
                                  HugeObjectArray<List> relCache) {
        assert(k > 0 && k < 4); // XXX arbitrary limit for now
        final HugeAtomicBitSet relationshipBitSet =
                HugeAtomicBitSet.create(maxNodeId, AllocationTracker.empty());
        long cnt = 0;

        // XXX bound total open tx's?
        try (InternalTransaction tx = api.beginTransaction(KernelTransaction.Type.EXPLICIT, context)) {
            final BiConsumer<RowBasedRecord, Integer> consumer = futureConsumer.join(); // XXX join()
            final Node start = tx.getNodeById(startId);

            logger.trace("beginning khop transaction for nodeId {} (k={})", start.getId(), k);
            var stream = hop(start, tx, relCache);
            for (int i = 1; i < k; i++) {
                stream = stream.flatMap(pair -> {
                    final Node next = pair.getRight();
                    return hop(next, tx, relCache);
                });
            }

            cnt = stream
                    .filter(pair -> !relationshipBitSet.getAndSet(pair.getLeft().getId()))
                    .map(Pair::getLeft)
                    .map(rel -> {
                        final Node source = rel.getStartNode();
                        final Node target = rel.getEndNode();

                        final Set<NodeLabel> sourceLabels = new HashSet<>();
                        Iterator<Label> iterator = source.getLabels().iterator();
                        while (iterator.hasNext())
                            sourceLabels.add(NodeLabel.of(iterator.next().name()));

                        final Set<NodeLabel> targetLabels = new HashSet<>();
                        iterator = target.getLabels().iterator();
                        while (iterator.hasNext())
                            targetLabels.add(NodeLabel.of(iterator.next().name()));

                        return null; // XXX TODO
                    })
                    .map(row -> {
                        consumer.accept(null, (int) 0); // XXX cast
                        return 1;
                    })
                    .count();

        } catch (NotFoundException notFound) {
            logger.warn("no node found for id {}", startId);
        } catch (Exception e) {
            if (!alreadyFailed.getAndSet(true)) {
                logger.error(String.format("failure to perform khop for id %d", startId), e);
            } // squelch errors at scale.
        }

        return Pair.of(1L, cnt);
    }

    public KHopJob(KHopMessage msg, String username, DatabaseManagementService dbms) {
        super();

        final LoginContext context = NativeAuthValidator.contextMap.get(username);
        if (context == null)
            throw CallStatus.UNAUTHENTICATED.withDescription("no existing login context for user")
                    .toRuntimeException();
        final GraphDatabaseAPI api = (GraphDatabaseAPI) dbms.database(msg.getDbName());

        // XXX validate k is in range
        final int k = msg.getK();

        // XXX Fake result just to get things moving
        onFirstRecord(SubGraphRecord.of(0L, List.of(0), List.of(0)));

        this.future = CompletableFuture.supplyAsync(() -> {
            long maxNodeId = 1_000;
            long maxRelId = 1_000;
            try {
                maxNodeId = (Long) dbms.database(msg.getDbName())
                        .executeTransactionally("MATCH (n) RETURN max(id(n)) AS x", Map.of(),
                                (results) -> {
                                    final Map<String, Object> row = results.next();
                                    return row.getOrDefault("x", 1_000_000L);
                                });
                maxRelId = (Long) dbms.database(msg.getDbName())
                        .executeTransactionally("MATCH ()-[r]->() RETURN max(id(r)) AS x", Map.of(),
                                (results) -> {
                                    final Map<String, Object> row = results.next();
                                    return row.getOrDefault("x", 1_000_000L);
                                });
            } finally {
                logger.info("using max node id {}, max rel id {}", maxNodeId, maxRelId);
            }

            // TODO: randomize
            final long[] nodeIds = LongStream.rangeClosed(0, maxNodeId).toArray();
            final long relCount = maxRelId;

            final HugeObjectArray<List> relCache = HugeObjectArray.newArray(List.class, maxNodeId, AllocationTracker.empty());

            try {
                final Pair<Long, Long> result = Arrays.stream(nodeIds)
                        .parallel()
                        .mapToObj(nodeId -> khop(nodeId, relCount, k, api, context, relCache))
                        .reduce(Pair.of(0L, 0L), (p1, p2) -> Pair.of(p1.getLeft() + p2.getLeft(), p1.getRight() + p2.getRight()));
                logger.info(String.format("performed k-hop for %,d nodes producing %,d rows", result.getLeft(), result.getRight()));

            } catch (Exception e) {
                logger.error("something not working in khop stream :-(", e);
                onCompletion(() -> "khop failed!");
                return false;
            }

            onCompletion(() -> "khop completed successfully");
            return true;
        });
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return future.cancel(mayInterruptIfRunning);
    }

    @Override
    public void close() throws Exception {
        cancel(true);
    }
}
