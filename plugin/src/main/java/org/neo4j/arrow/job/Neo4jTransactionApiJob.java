package org.neo4j.arrow.job;

import org.neo4j.arrow.CypherRecord;
import org.neo4j.arrow.RowBasedRecord;
import org.neo4j.arrow.action.CypherMessage;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

public class Neo4jTransactionApiJob extends Job {

    private final CompletableFuture<JobSummary> future;
    private final Log log;

    public Neo4jTransactionApiJob(CypherMessage msg, Mode mode, GraphDatabaseService db, Log log) {
        super();
        this.log = log;

        future = CompletableFuture.supplyAsync(() -> {
            try (Transaction tx = db.beginTx()) {
                log.info("(arrow) starting tx...");

                try (Result result = tx.execute(msg.getCypher(), msg.getParams())) {
                    AtomicLong cnt = new AtomicLong(1);
                    ArrayList<String> fields = new ArrayList(result.columns());

                    result.accept(row -> {
                        long i = cnt.getAndIncrement();
                        RowBasedRecord record = CypherRecord.wrap(row, fields);
                        if (i == 1) {
                            log.info("(arrow) first record seen for stream with fields " + record.keys());
                            for (String field : record.keys()) {
                                log.info(String.format("(arrow)  %s -> %s", field, record.get(field).type()));
                            }
                            onFirstRecord(record);
                        }
                        if (i % 25_000 == 0)
                            log.info("(arrow) feed " + i + " rows");

                        futureConsumer.join().accept(record);
                        return true;
                    });
                    log.info("(arrow) reached end of stream at row " + cnt.decrementAndGet());
                } finally {
                    tx.commit();
                }
            }
            return summarize();
        }).thenApplyAsync(summary -> {
            onCompletion(summary);
            return summary;
        }).toCompletableFuture();
        log.info("(arrow) job kicking off...");
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
