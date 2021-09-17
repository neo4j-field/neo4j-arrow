package org.neo4j.arrow.job;

import org.apache.arrow.flight.CallStatus;
import org.neo4j.arrow.Config;
import org.neo4j.arrow.CypherRecord;
import org.neo4j.arrow.RowBasedRecord;
import org.neo4j.arrow.action.CypherMessage;
import org.neo4j.arrow.auth.NativeAuthValidator;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.KernelTransactionFactory;
import org.neo4j.kernel.impl.query.*;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.VirtualValues;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

/**
 * Interact with the Database directly via the Transaction API and Cypher.
 */
public class TransactionApiJob extends Job {

    private final CompletableFuture<JobSummary> future;

    public TransactionApiJob(CypherMessage msg, String username, DatabaseManagementService dbms, Log log) {
        super();

        final LoginContext context = NativeAuthValidator.contextMap.get(username);

        if (context == null)
            throw CallStatus.UNAUTHENTICATED.withDescription("no existing login context for user")
                    .toRuntimeException();

        // TODO: pull in reference to LoginContext and use it in the Transaction
        future = CompletableFuture.supplyAsync(() -> {
            final GraphDatabaseAPI api = (GraphDatabaseAPI) dbms.database(msg.getDatabase());
            log.info("Starting neo4j Tx job for cypher:\n%s", msg.getCypher().trim());

            // !!! XXX THERE BE DRAGONS HERE XXX !!!
            final QueryExecutionEngine queryExecutionEngine = api.getDependencyResolver().resolveDependency(QueryExecutionEngine.class);
            assert(queryExecutionEngine != null);
            final Supplier<GraphDatabaseQueryService> queryServiceSupplier = () -> {
                final GraphDatabaseQueryService queryService = api.getDependencyResolver().resolveDependency(GraphDatabaseQueryService.class);
                assert(queryService != null);
                return queryService;
            };
            KernelTransactionFactory kernelTransactionFactory = api.getDependencyResolver().resolveDependency(KernelTransactionFactory.class);
            assert(kernelTransactionFactory != null);
            final TransactionalContextFactory contextFactory = Neo4jTransactionalContextFactory.create(queryServiceSupplier, kernelTransactionFactory);

            try (InternalTransaction tx = api.beginTransaction(KernelTransaction.Type.EXPLICIT, context)) {

                final List<AnyValue> values = new ArrayList<>();
                final String[] keys = msg.getParams().keySet().toArray(new String[0]);
                for (String key : keys)
                    values.add(Values.of(msg.getParams().get(key), false));
                final MapValue mv = keys.length > 0 ? VirtualValues.map(keys, values.toArray(new AnyValue[0])) : MapValue.EMPTY;

                final TransactionalContext transactionalContext = contextFactory.newContext(tx, msg.getCypher(), mv);
                // le sigh...TransactionalContext isn't AutoClosable
                try {
                    final BlockingDeque<AnyValue[]> queue = new LinkedBlockingDeque<>();
                    final CompletableFuture<Void> feeding = new CompletableFuture<>();
                    final CompletableFuture<AnyValue[]> firstRecordReady = new CompletableFuture<>();

                    final QueryExecution execution = queryExecutionEngine.executeQuery(
                            // prePopulate isn't documented, but it seems to mean "should we lookup properties for Nodes/Rels"
                            // even if they weren't requested? (e.g. if you MATCH (n) RETURN n, do you get n's props?
                            msg.getCypher(), mv, transactionalContext, false, new QuerySubscriber() {

                                AnyValue[] values = new AnyValue[0];
                                int rowNum = 0;

                                @Override
                                public void onResult(int numberOfFields) {
                                    log.info("started fetching results for job %s with %d fields", getJobId(), numberOfFields);
                                    values = new AnyValue[numberOfFields];
                                }

                                @Override
                                public void onRecord() {
                                    // For now, we'll "zero" all entries every record
                                    Arrays.fill(values, Values.NO_VALUE);
                                }

                                @Override
                                public void onField(int offset, AnyValue value) {
                                    values[offset] = value;
                                }

                                @Override
                                public void onRecordCompleted() {
                                    try {
                                        int spunout = 10;
                                        final AnyValue[] copy = Arrays.copyOf(values, values.length);
                                        while (!queue.offerLast(copy, 10, TimeUnit.SECONDS)) {
                                            log.info("...timed out adding to queue. Trying again.");
                                            spunout--;
                                            if (spunout < 0) {
                                                log.error("spunout adding to queue :-(");
                                                break;
                                            }
                                        }

                                        if (rowNum == 0) {
                                            log.info("producer completing the firstRecordReady future");
                                            firstRecordReady.complete(copy);
                                        }
                                        if (rowNum % 10_000 == 0)
                                            log.info("added row %d (queue size = %d)", rowNum, queue.size());
                                        rowNum++;
                                    } catch (Exception e) {
                                        log.error("failed to add work to the queue", e);
                                    }
                                }

                                @Override
                                public void onError(Throwable throwable) {
                                    log.error("error consuming results", throwable);
                                }

                                @Override
                                public void onResultCompleted(QueryStatistics statistics) {
                                    log.info("finished fetching results for job %s, still %d items in queue", getJobId(), queue.size());
                                    feeding.complete(null);
                                }
                            });

                    // Blast off! Grab 1 record for now.
                    execution.request(1);
                    final String[] fieldNames = execution.fieldNames();

                    // XXX wtf seriously? this has no way to request all and not block?!
                    CompletableFuture.runAsync(() -> {
                        try {
                            execution.consumeAll();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        log.info("finished requesting all rows");
                    });

                    // onFirstRecord
                    log.info("...waiting for first item before blasting off 🚀");
                    // TODO: for now wait up to a minute for the first result...needs future work
                    AnyValue[] firstValues;
                    try {
                        firstValues = firstRecordReady.get(5, TimeUnit.SECONDS);
                        log.info("first record should be ready!");
                        for (AnyValue av : firstValues)
                            log.info("av: " + av.getTypeName());
                    } catch (Exception e) {
                        log.error("bad news bears with the first record!");
                        throw CallStatus.INTERNAL.withCause(e).toRuntimeException();
                    }

                    final CypherRecord firstRecord = CypherRecord.wrap(fieldNames, firstValues);
                    log.info("processing first record...");
                    onFirstRecord(firstRecord);

                    // Feed the first record, then go hog wild
                    final BiConsumer<RowBasedRecord, Integer> consumer = futureConsumer.join();

                    final CompletableFuture<?>[] futures = new CompletableFuture[Config.arrowMaxPartitions];
                    for (int i = 0; i < futures.length; i++) {
                        final int partitionId = i;
                        futures[i] = CompletableFuture.runAsync(() -> {
                            try {
                                while (!queue.isEmpty() || !feeding.isDone()) {
                                    final AnyValue[] work = queue.pollFirst(10, TimeUnit.MILLISECONDS);
                                    // TODO: wrap function should take our assumed schema to optimize
                                    if (work != null)
                                        consumer.accept(CypherRecord.wrap(fieldNames, work), partitionId);
                                }
                            } catch (InterruptedException e) {
                                log.debug("queue consumer interrupted");
                            } catch (Exception e) {
                                log.error("queue consumer errored: %s", e.getMessage());
                                e.printStackTrace();
                            }
                        }).thenRunAsync(() -> log.debug("completed queue worker task"));
                    }

                    // And we wait...
                    log.info("waiting for %d workers to complete...", futures.length);
                    CompletableFuture.allOf(futures)
                            .exceptionally(throwable -> {
                                log.error("oh no...%s", throwable.getMessage());
                                return null;
                            })
                            .join();
                } finally { // transactional context
                    transactionalContext.close();
                }
            } catch (Exception e) {
                throw CallStatus.INTERNAL.withDescription("failure during TransactionAPIJob")
                        .withCause(e).toRuntimeException();
            }
            return summarize();
        }).thenApplyAsync(summary -> {
            onCompletion(summary);
            return summary;
        });
    }

    private JobSummary summarize() {
        return () -> "{ TBD }";
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
