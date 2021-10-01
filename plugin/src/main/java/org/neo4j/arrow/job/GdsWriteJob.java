package org.neo4j.arrow.job;

import org.checkerframework.checker.units.qual.A;
import org.neo4j.arrow.Config;
import org.neo4j.arrow.GdsNodeRecord;
import org.neo4j.arrow.GdsRecord;
import org.neo4j.arrow.RowBasedRecord;
import org.neo4j.arrow.action.GdsMessage;
import org.neo4j.arrow.action.GdsWriteNodeMessage;
import org.neo4j.arrow.action.Message;
import org.neo4j.cypher.internal.util.symbols.StorableType;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.*;
import org.neo4j.gds.config.GraphCreateConfig;
import org.neo4j.gds.core.huge.HugeGraph;
import org.neo4j.gds.core.loading.CSRGraphStoreUtil;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.loading.construction.NodesBuilder;
import org.neo4j.gds.core.loading.construction.NodesBuilderBuilder;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;


public class GdsWriteJob extends WriteJob {
    private final CompletableFuture<Boolean> future;
    private final DatabaseManagementService dbms;

    /**
     * Create a new GdsWriteJob for processing the given {@link GdsMessage}, creating or writing to
     * an in-memory GDS graph.
     * <p>
     * The supplied username is assumed to allow GDS to enforce authorization and is assumed to be
     * previously authenticated.
     *
     * @param msg      the {@link GdsMessage} to process in the job
     * @param username an already authenticated username
     * @param dbms reference to a {@link DatabaseManagementService}
     */
    public GdsWriteJob(GdsWriteNodeMessage msg, // XXX need to abstract here?
                       String username, DatabaseManagementService dbms) throws RuntimeException {
        super();
        this.dbms = dbms;

        final CompletableFuture<Boolean> job;
        logger.info("GdsWriteJob called with msg: {}", msg);

        job = handleNodeJob(msg, username);

        /* XXX later
        switch (msg.getRequestType()) {
            case node:
                job = handleNodeJob(msg, username);
                break;
            case relationship:
                job = handleRelationshipsJob(msg);
                break;
            default:
                throw CallStatus.UNIMPLEMENTED.withDescription("unhandled request type").toRuntimeException();
         }
         */

        future = job.exceptionally(throwable -> {
            logger.error(throwable.getMessage(), throwable);
            return false;
        }).handleAsync((aBoolean, throwable) -> {
            logger.info("GdsWriteJob completed! result: {}", (aBoolean == null ? "failed" : "ok!"));
            if (throwable != null)
                logger.error(throwable.getMessage(), throwable);
            return false;
        });
    }

    protected CompletableFuture<Boolean> handleNodeJob(GdsWriteNodeMessage msg, String username) {
        final NodesBuilder builder = (new NodesBuilderBuilder())
                .concurrency(Config.arrowMaxPartitions)
                .hasLabelInformation(true)
                .hasProperties(true)
                .maxOriginalId(100)
                .allocationTracker(AllocationTracker.create())
                .nodeCount(100)
                .build();

        final GraphDatabaseAPI api = (GraphDatabaseAPI) dbms.database(msg.getDbName());
        final NamedDatabaseId dbId = api.databaseId();

        logger.info("configuring job for {}", msg);

        final AtomicInteger nodeCount = new AtomicInteger(0);

        // XXX consumer
        setConsumer((ks, vs) -> {
            final List<Value> values = vs.stream()
                    .map(Values::of).collect(Collectors.toList());

            // dumb
            final HashMap<String, Value> map = new HashMap<>();
            long nodeId = -1;
            for (int i=0; i<ks.size(); i++) {
                map.put(ks.get(i), values.get(i));
                if (Objects.equals(ks.get(i), msg.getIdField()))
                    nodeId = (Long)vs.get(i);
            }

            logger.info("consuming nodeId {}, map: {}", nodeId, map);
            builder.addNode(nodeId, map, NodeLabel.ALL_NODES);
            nodeCount.incrementAndGet();
        });

        return CompletableFuture.supplyAsync(() -> {
            // XXX we assume we're creating a graph (for now), not updating
            try {
                getStreamCompletion().get(1, TimeUnit.HOURS);    // XXX
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                e.printStackTrace();
                return false;
            }

            final HugeGraph hugeGraph = GraphFactory.create(builder.build().nodeMapping(),
                    Relationships.of(0, Orientation.NATURAL, false, new AdjacencyList() {
                        // See GraphStoreFilterTest.java for inspiration of how to stub out
                        @Override
                        public int degree(long node) {
                            return 0;
                        }

                        @Override
                        public AdjacencyCursor adjacencyCursor(long node, double fallbackValue) {
                            return AdjacencyCursor.EmptyAdjacencyCursor.INSTANCE;
                        }

                        @Override
                        public AdjacencyCursor rawAdjacencyCursor() {
                            return AdjacencyCursor.EmptyAdjacencyCursor.INSTANCE;
                        }

                        @Override
                        public void close() {
                        }
                    }), AllocationTracker.empty());

            GraphStore store = CSRGraphStoreUtil.createFromGraph(
                    dbId, hugeGraph, "REL", Optional.empty(),
                    Config.arrowMaxPartitions, AllocationTracker.create());
            // XXX
            GraphCreateConfig config = new GraphCreateConfig() {
                @Override
                public String graphName() {
                    return msg.getGraphName();
                }

                @Override
                public GraphStoreFactory.Supplier graphStoreFactory() {
                    throw new RuntimeException("crap: graphStoreFactory() called");
                }

                @Override
                public <R> R accept(Cases<R> visitor) {
                    // TODO: what the heck is this Cases<R> stuff?!
                    return null;
                }

                @Override
                public int readConcurrency() {
                    return GraphCreateConfig.super.readConcurrency();
                }

                @Override
                public long nodeCount() {
                    return nodeCount.get(); // XXX
                }

                @Override
                public long relationshipCount() {
                    return 0;
                }

                @Override
                public boolean validateRelationships() {
                    return false;
                }

                @Override
                public String username() {
                    return username;
                }
            };

            GraphStoreCatalog.set(config, store);

            return true;
        });
    }

    protected CompletableFuture<Boolean> handleRelationshipsJob(Message unused) {
        return null;
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
