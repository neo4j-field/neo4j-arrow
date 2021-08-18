package org.neo4j.arrow.job;

import org.neo4j.arrow.CypherRecord;
import org.neo4j.arrow.RowBasedRecord;
import org.neo4j.arrow.action.CypherMessage;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Interact with the Database directly via the Transaction API and Cypher.
 */
public class Neo4jTransactionApiJob extends Job {

    private final CompletableFuture<JobSummary> future;
    private final GraphDatabaseAPI api;
    private final Log log;

    public Neo4jTransactionApiJob(CypherMessage msg, Mode mode, DatabaseManagementService dbms, Log log) {
        super();
        this.log = log;
        this.api = (GraphDatabaseAPI) dbms.database(msg.getDatabase());

        // TODO: pull in reference to LoginContext and use it in the Transaction
        future = CompletableFuture.supplyAsync(() -> {
            try (Transaction tx = api.beginTx()) {

                log.info("Starting neo4j Tx job for cypher:\n%s", msg.getCypher());
                try (Result result = tx.execute(msg.getCypher(), msg.getParams())) {
                    AtomicLong cnt = new AtomicLong(1);
                    ArrayList<String> fields = new ArrayList(result.columns());

                    result.accept(row -> {
                        long i = cnt.getAndIncrement();
                        RowBasedRecord record = CypherRecord.wrap(row, fields);
                        if (i == 1) {
                            log.debug("(arrow) first record seen for stream with fields " + record.keys());
                            for (String field : record.keys()) {
                                log.info(String.format("(arrow)  %s -> %s", field, record.get(field).type()));
                            }
                            onFirstRecord(record);
                        }

                        futureConsumer.join().accept(record);
                        return true;
                    });
                } finally {
                    tx.commit();
                }
            }
            return summarize();
        }).thenApplyAsync(summary -> {
            onCompletion(summary);
            return summary;
        }).toCompletableFuture();
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
