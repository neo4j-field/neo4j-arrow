package org.neo4j.arrow;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class CypherJob extends Neo4jJob {

    private final CompletableFuture<JobSummary> future;
    private final Log log;

    public CypherJob(CypherMessage msg, Mode mode, GraphDatabaseService db, Log log) {
        super(msg, mode);
        this.log = log;

        future = CompletableFuture.supplyAsync(() -> {
            try (Transaction tx = db.beginTx()) {
                log.info("(arrow) starting tx...");
                try (Result result = tx.execute(msg.getCypher(), msg.getParams())) {
                    AtomicLong cnt = new AtomicLong(1);
                    ArrayList<String> fields = new ArrayList(result.columns());

                    result.accept(row -> {
                        long i = cnt.getAndIncrement();
                        Neo4jRecord record = CypherRecord.wrap(row, fields);
                        if (i == 1) {
                            log.info("(arrow) first record seen for stream with fields " + fields);
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

    class Visitor implements Result.ResultVisitor {
        final ArrayList<String> fields;
        Consumer<Neo4jRecord> consumer;
        long rows = 0;

        public Visitor(Collection<String> fields) {
            this.fields = new ArrayList(fields);
        }

        @Override
        public boolean visit(Result.ResultRow row) throws Exception {
            final Neo4jRecord record = CypherRecord.wrap(row, fields);

            if (rows == 0) {
                log.info("(arrow) got first row");
                onFirstRecord(record);
            } else if (rows % 25_000 == 0) {
                log.info(String.format("(arrow) feed %d, rows"), rows);
            }
            rows++;

            if (consumer == null) {
                log.info("(arrow) trying to get consumer");
                consumer = futureConsumer.get(15, TimeUnit.SECONDS);
                log.info("(arrow) got consumer");
            }

            consumer.accept(CypherRecord.wrap(row, fields));
            return true;
        }
    }

    private JobSummary summarize() {
        return new JobSummary() {
            @Override
            public String toString() {
                return "{ TBD }";
            }
        };
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
