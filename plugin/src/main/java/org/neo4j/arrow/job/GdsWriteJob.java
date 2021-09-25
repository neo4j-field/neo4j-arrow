package org.neo4j.arrow.job;

import org.apache.arrow.flight.CallStatus;
import org.neo4j.arrow.Config;
import org.neo4j.arrow.action.GdsMessage;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.AdjacencyCursor;
import org.neo4j.gds.api.AdjacencyList;
import org.neo4j.gds.api.Relationships;
import org.neo4j.gds.core.huge.HugeGraph;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.loading.construction.NodesBuilder;
import org.neo4j.gds.core.loading.construction.NodesBuilderBuilder;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.logging.Log;

import java.util.concurrent.CompletableFuture;

public class GdsWriteJob extends WriteJob {
    private final CompletableFuture<Boolean> future;

    /**
     * Create a new GdsWriteJob for processing the given {@link GdsMessage}, creating or writing to
     * an in-memory GDS graph.
     * <p>
     * The supplied username is assumed to allow GDS to enforce authorization and is assumed to be
     * previously authenticated.
     *
     * @param msg the {@link GdsMessage} to process in the job
     * @param username an already authenticated username
     */
    public GdsWriteJob(GdsMessage msg, String username) throws RuntimeException {
        super();
        final CompletableFuture<Boolean> job;
        logger.info("GdsWriteJob called with msg: {}", msg);

        switch (msg.getRequestType()) {
            case node:
                job = handleNodeJob(msg);
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

    protected CompletableFuture<Boolean> handleNodeJob(GdsMessage msg) {
        final NodesBuilder builder = (new NodesBuilderBuilder())
                .concurrency(Config.arrowMaxPartitions)
                .hasLabelInformation(true)
                .hasProperties(true)
                .build();

        setConsumer((batch, schema) -> {
            // yolo
            logger.info("pretending to process batch...");
            //batch.getNodes().get(0).
        });

        return CompletableFuture.supplyAsync(() -> {



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

            return true;
        });
    }

    protected CompletableFuture<Boolean> handleRelationshipsJob(GdsMessage msg) {
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
