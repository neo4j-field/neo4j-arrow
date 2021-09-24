package org.neo4j.arrow.job;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.neo4j.arrow.Config;
import org.neo4j.arrow.action.GdsMessage;
import org.neo4j.gds.NodeLabel;
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

import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@SuppressWarnings("unused") // XXX remove me
public class GdsWriteJob extends Job {
    private final CompletableFuture<Boolean> future;
    private final CompletableFuture<Iterator<ArrowRecordBatch>> streamFuture = new CompletableFuture<>();
    private final Log log;

    /**
     * Create a new GdsWriteJob for processing the given {@link GdsMessage}, creating or writing to
     * an in-memory GDS graph.
     * <p>
     * The supplied username is assumed to allow GDS to enforce authorization and is assumed to be
     * previously authenticated.
     *
     * @param msg the {@link GdsMessage} to process in the job
     * @param username an already authenticated username
     * @param log the Neo4j log instance
     */
    public GdsWriteJob(GdsMessage msg, String username, Log log) throws RuntimeException {
        super();
        final CompletableFuture<Boolean> job;
        this.log = log;
        log.info("GdsWriteJob called with msg: %s", msg);

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
            log.error(throwable.getMessage(), throwable);
            return false;
        }).handleAsync((aBoolean, throwable) -> {
            log.info("GdsWriteJob completed! result: " + (aBoolean == null ? "failed" : "ok!"));
            if (throwable != null)
                log.error(throwable.getMessage(), throwable);
            return false;
        });
    }

    protected CompletableFuture<Boolean> handleNodeJob(GdsMessage msg) {
        final NodesBuilder builder = (new NodesBuilderBuilder())
                .concurrency(Config.arrowMaxPartitions)
                .hasLabelInformation(true)
                .hasProperties(true)
                .build();

        return CompletableFuture.supplyAsync(() -> {
            // We need to wait for the client to feed us data
            try {
                return Optional.of(streamFuture.get(5, TimeUnit.MINUTES));
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                log.error("error waiting for client stream", e);
            }
            return Optional.empty();
        }).thenApplyAsync(maybeBatches -> {
            if (maybeBatches.isEmpty())
                throw CallStatus.TIMED_OUT
                        .withDescription("timed out waiting for client to send stream").toRuntimeException();

            builder.addNode(0, NodeLabel.of("crap"));
            builder.build();

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
