package org.neo4j.arrow.job;

import org.apache.arrow.flight.CallStatus;
import org.neo4j.arrow.Config;
import org.neo4j.arrow.action.GdsMessage;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.*;
import org.neo4j.gds.config.GraphCreateConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.huge.HugeGraph;
import org.neo4j.gds.core.loading.CSRGraphStoreUtil;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.loading.construction.NodesBuilder;
import org.neo4j.gds.core.loading.construction.NodesBuilderBuilder;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


public class GdsWriteJob extends WriteJob {
    private final CompletableFuture<Boolean> future;
    private final DatabaseManagementService dbms; //

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
    public GdsWriteJob(GdsMessage msg, String username, DatabaseManagementService dbms) throws RuntimeException {
        super();
        this.dbms = dbms;

        final CompletableFuture<Boolean> job;
        logger.info("GdsWriteJob called with msg: {}", msg);

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

    protected CompletableFuture<Boolean> handleNodeJob(GdsMessage msg, String username) {
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

        // XXX consumer
        setConsumer((nodeId, strings) -> {
            // nop
            logger.info("  {}: {}", nodeId, strings);
            builder.addNode(nodeId, NodeLabel.listOf(strings).toArray(new NodeLabel[0]));
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
                    return 100; // XXX
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

    protected CompletableFuture<Boolean> handleRelationshipsJob(GdsMessage unused) {
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
