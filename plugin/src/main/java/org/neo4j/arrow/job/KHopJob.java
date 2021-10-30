package org.neo4j.arrow.job;

import com.google.common.hash.BloomFilter;
import org.apache.arrow.flight.CallStatus;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.neo4j.arrow.RowBasedRecord;
import org.neo4j.arrow.SubGraphRecord;
import org.neo4j.arrow.action.CypherMessage;
import org.neo4j.arrow.auth.NativeAuthValidator;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Spliterators;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class KHopJob extends ReadJob {

    private final CompletableFuture<Boolean> future;
    private final AtomicBoolean alreadyFailed = new AtomicBoolean(false);
    private final BloomFilter<Long> relIdBloomFilter = BloomFilter.create(
            (from, into) -> into.putLong(from), 1_000_000_000);

    /**
     * Generate a {@link Stream} of {@link Triple}s of {@link Relationship}s and
     * {@link Node}s reachable by a single hop from a given source {@link Node}.
     */
    private Stream<Triple<Node, Relationship, Node>> hop(Node source, HugeAtomicBitSet relationshipBitSet) {
        final var spliterator = Spliterators.spliterator(
                source.getRelationships().iterator(),
                source.getDegree(), 0);

        logger.trace("hop(id={}, degree={})", source.getId(), source.getDegree());

        // XXX not sure if we should decompose our Node/Relationship instances to
        // allow dropping any references and/or locks
        return StreamSupport.stream(spliterator, false)
                .filter(rel -> {
                    final boolean seen = relationshipBitSet.getAndSet(rel.getId());
                    if (seen) {
                        logger.trace("  skipping already seen rel ({})-[{}]-({})", rel.getStartNode(), rel.getId(), rel.getEndNode());
                        return false;
                    }
                    return true;
                })
                .map(rel -> Triple.of(source, rel, rel.getOtherNode(source)));
    }

    /**
     * The core k-hop logic. Will open an {@InternalTransaction} and close it upon completion of the generated stream.
     */
    private Pair<Long, Integer> khop(long startId, int k, GraphDatabaseAPI api, LoginContext context) {
        assert(k > 0 && k < 4); // XXX arbitrary limit for now
        final HugeAtomicBitSet relationshipBitSet =
                HugeAtomicBitSet.create(Integer.MAX_VALUE, // XXX can we find the max id?
                        AllocationTracker.empty());

        logger.debug("khop(startId={}, k={})", startId, k);
        final AtomicInteger cnt = new AtomicInteger(0);

        try (InternalTransaction tx = api.beginTransaction(KernelTransaction.Type.IMPLICIT, context)) {
            final BiConsumer<RowBasedRecord, Integer> consumer = futureConsumer.join(); // XXX join()
            final Node start = tx.getNodeById(startId);

            logger.debug("beginning khop transaction for nodeId {} (k={})", start.getId(), k);
            var stream = hop(start, relationshipBitSet);
            for (int i = 1; i < k; i++) {
                stream = stream.flatMap(triple -> hop(triple.getRight(), relationshipBitSet));
            }

            stream.map(triple -> SubGraphRecord.of(triple.getLeft(), triple.getMiddle(), triple.getRight()))
                    .map(row -> Pair.of(row, cnt.getAndIncrement()))
                    .forEach(pair -> consumer.accept(pair.getLeft(), pair.getRight()));
        } catch (NotFoundException notFound) {
            logger.warn("no node found for id {}", startId);
        } catch (Exception e) {
            if (!alreadyFailed.getAndSet(true)) {
                logger.error(String.format("failure to perform khop for id %d", startId), e);
            } // squelch errors at scale.
        }

        return Pair.of(startId, cnt.get());
    }

    public KHopJob(CypherMessage msg, String username, DatabaseManagementService dbms) {
        super();

        final LoginContext context = NativeAuthValidator.contextMap.get(username);
        if (context == null)
            throw CallStatus.UNAUTHENTICATED.withDescription("no existing login context for user")
                    .toRuntimeException();
        final GraphDatabaseAPI api = (GraphDatabaseAPI) dbms.database("neo4j"); // TODO get from message

        // TODO parameterize!!!
        final int k = 2;
        long maxId = 1_000_000;
        try {
            maxId = (Long) dbms.database("neo4j")
                    .executeTransactionally("MATCH (n) RETURN max(id(n)) AS x", Map.of(), (results) -> {
                        final Map<String, Object> row =  results.next();
                        return row.getOrDefault("x", 1_000_000L);
                    });
        } finally {
            logger.info("using max node id {}", maxId);
        }

        final long[] nodeIds = LongStream.range(0, maxId).toArray();

        // XXX Fake result just to get things moving
        //onFirstRecord(new SubGraphRecord(0, List.of(NodeLabel.ALL_NODES), 1, "TYPE", 2, List.of(NodeLabel.ALL_NODES)));

        this.future = CompletableFuture.supplyAsync(() -> {
            try {
                Arrays.stream(nodeIds)
                        .parallel()
                        .mapToObj(nodeId -> khop(nodeId, k, api, context))
                        .forEach(pair -> logger.debug("finished khop for {} ({} rows)", pair.getLeft(), pair.getRight()));
            } catch (Exception e) {
                logger.error("something not working in khop stream :-(", e);
                onCompletion(() -> "khop failed!");
                return false;
            }

            onCompletion(() -> "finished khop");
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
