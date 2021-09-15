package org.neo4j.arrow.job;

import org.apache.arrow.flight.CallStatus;
import org.neo4j.arrow.CypherRecord;
import org.neo4j.arrow.RowBasedRecord;
import org.neo4j.arrow.action.CypherMessage;
import org.neo4j.arrow.auth.NativeAuthValidator;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import java.util.Spliterators;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.stream.StreamSupport;

/**
 * Interact with the Database directly via the Transaction API and Cypher.
 */
public class Neo4jTransactionApiJob extends Job {

    private final CompletableFuture<JobSummary> future;

    public Neo4jTransactionApiJob(CypherMessage msg, String username, DatabaseManagementService dbms, Log log) {
        super();

        final LoginContext context = NativeAuthValidator.contextMap.get(username);

        if (context == null)
            throw CallStatus.UNAUTHENTICATED.withDescription("no existing login context for user")
                    .toRuntimeException();

        // TODO: pull in reference to LoginContext and use it in the Transaction
        future = CompletableFuture.supplyAsync(() -> {
            final GraphDatabaseAPI api = (GraphDatabaseAPI) dbms.database(msg.getDatabase());
            log.info("Starting neo4j Tx job for cypher:\n%s", msg.getCypher().trim());

            try (Transaction tx = api.beginTransaction(KernelTransaction.Type.EXPLICIT, context)) {
                try (Result result = tx.execute(msg.getCypher(), msg.getParams())) {

                    // Get the first record.
                    if (!result.hasNext()) {
                        // XXX we currently assume we get data. no data is boring!
                        throw CallStatus.NOT_FOUND.withDescription("no data returned for job").toRuntimeException();
                    }
                    final CypherRecord record = CypherRecord.wrap(result.next());
                    onFirstRecord(record);

                    // Start consuming the initial result, then iterate through the rest
                    final BiConsumer<RowBasedRecord, Integer> consumer = futureConsumer.join();
                    consumer.accept(record, 0);

                    // yolo this for now...could make lock free
                    final AtomicInteger p = new AtomicInteger(0);
                    final var s = Spliterators.spliteratorUnknownSize(result, 0);
                    StreamSupport.stream(s, true).parallel()
                                    .forEach(i -> consumer.accept(CypherRecord.wrap(i), p.incrementAndGet()));
                    log.info("completed processing cypher results");
                } catch (Exception e) {
                    log.error("crap", e);
                    throw CallStatus.INTERNAL.withCause(e).toRuntimeException();
                } finally {
                    tx.commit();
                }
            } catch (Exception e) {
                log.error("crap", e);
                throw CallStatus.INTERNAL.withCause(e).toRuntimeException();
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
