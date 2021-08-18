package org.neo4j.arrow;

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.neo4j.arrow.action.GdsActionHandler;
import org.neo4j.arrow.action.GdsMessage;
import org.neo4j.arrow.demo.Client;
import org.neo4j.arrow.job.Job;
import org.neo4j.logging.Log;
import org.neo4j.logging.log4j.Log4jLogProvider;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * A simple local perf test, nothing fancy. Just make sure data can flow and flow effectively. Used
 * mostly for doing flame graphs of the call stack to find hot spots.
 */
public class GdsRecordBenchmarkTest {
    private final static Log log;

    static {
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "[yyyy-MM-dd'T'HH:mm:ss:SSS]");
        System.setProperty("org.slf4j.simpleLogger.logFile", "System.out");
        log = new Log4jLogProvider(System.out).getLog(GdsRecordBenchmarkTest.class);
    }

    // 256 floats!
    private static final float[] PAYLOAD = {
            1f, 2f, 3f, 4f, 5f, 6f, 7f, 8f, 9f, 10f, // 10
            1f, 2f, 3f, 4f, 5f, 6f, 7f, 8f, 9f, 10f, // 20
            1f, 2f, 3f, 4f, 5f, 6f, 7f, 8f, 9f, 10f, // 30
            1f, 2f, 3f, 4f, 5f, 6f, 7f, 8f, 9f, 10f, // 40
            1f, 2f, 3f, 4f, 5f, 6f, 7f, 8f, 9f, 10f, // 50
            1f, 2f, 3f, 4f, 5f, 6f, 7f, 8f, 9f, 10f, // 60
            1f, 2f, 3f, 4f, 5f, 6f, 7f, 8f, 9f, 10f, // 70
            1f, 2f, 3f, 4f, 5f, 6f, 7f, 8f, 9f, 10f, // 80
            1f, 2f, 3f, 4f, 5f, 6f, 7f, 8f, 9f, 10f, // 90
            1f, 2f, 3f, 4f, 5f, 6f, 7f, 8f, 9f, 10f, // 100
            1f, 2f, 3f, 4f, 5f, 6f, 7f, 8f, 9f, 10f, // 110
            1f, 2f, 3f, 4f, 5f, 6f, 7f, 8f, 9f, 10f, // 120
            1f, 2f, 3f, 4f, 5f, 6f, 7f, 8f, 9f, 10f, // 130
            1f, 2f, 3f, 4f, 5f, 6f, 7f, 8f, 9f, 10f, // 140
            1f, 2f, 3f, 4f, 5f, 6f, 7f, 8f, 9f, 10f, // 150
            1f, 2f, 3f, 4f, 5f, 6f, 7f, 8f, 9f, 10f, // 160
            1f, 2f, 3f, 4f, 5f, 6f, 7f, 8f, 9f, 10f, // 170
            1f, 2f, 3f, 4f, 5f, 6f, 7f, 8f, 9f, 10f, // 180
            1f, 2f, 3f, 4f, 5f, 6f, 7f, 8f, 9f, 10f, // 190
            1f, 2f, 3f, 4f, 5f, 6f, 7f, 8f, 9f, 10f, // 200
            1f, 2f, 3f, 4f, 5f, 6f, 7f, 8f, 9f, 10f, // 210
            1f, 2f, 3f, 4f, 5f, 6f, 7f, 8f, 9f, 10f, // 220
            1f, 2f, 3f, 4f, 5f, 6f, 7f, 8f, 9f, 10f, // 230
            1f, 2f, 3f, 4f, 5f, 6f, 7f, 8f, 9f, 10f, // 240
            1f, 2f, 3f, 4f, 5f, 6f, 7f, 8f, 9f, 10f, // 250
            1f, 2f, 3f, 4f, 5f, 6f }; // 256

    private static RowBasedRecord getRecord() {
        final RowBasedRecord record = new GdsRecord(1, List.of("embedding"),
                List.of(GdsRecord.wrapFloatArray(PAYLOAD)));
        return record;
    }

    private class NoOpJob extends Job {

        final CompletableFuture<Integer> future;
        final int numResults;

        public NoOpJob(int numResults, CompletableFuture<Long> signal) {
            super();
            this.numResults = numResults;

            future = CompletableFuture.supplyAsync(() -> {
                try {
                    log.info("Job starting");
                    final RowBasedRecord record = getRecord();
                    onFirstRecord(record);
                    log.info("Job feeding");
                    Consumer<RowBasedRecord> consumer = super.futureConsumer.join();
                    for (int i = 0; i < numResults; i++)
                        consumer.accept(record);
                    signal.complete(System.currentTimeMillis());
                    log.info("Job finished");
                    onCompletion(() -> "done");
                    return numResults;
                } catch (Exception e) {
                    Assertions.fail(e);
                }
                return 0;
            });
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public void close() {}
    }

    @Test
    @Timeout(value = 1, unit = TimeUnit.MINUTES)
    public void testSpeed() throws Exception {
        final BufferAllocator serverAllocator = new RootAllocator(Integer.MAX_VALUE);
        final BufferAllocator clientAllocator = new RootAllocator(Integer.MAX_VALUE);

        final Location location = Location.forGrpcInsecure("localhost", 12345);
        final CompletableFuture<Long> signal = new CompletableFuture<>();

        try (App app = new App(serverAllocator, location);
             Client client = new Client(clientAllocator, location)) {

            app.registerHandler(new GdsActionHandler(
                    (msg, mode, username, password) -> new NoOpJob(1_000_000, signal),
                    log));
            app.start();

            final long start = System.currentTimeMillis();
            final GdsMessage msg = new GdsMessage("neo4j", "mygraph", List.of("fastRp"), List.of());
            final Action action = new Action(GdsActionHandler.NODE_PROPS_ACTION, msg.serialize());
            client.run(action);
            final long stop = signal.join();
            log.info(String.format("Client Lifecycle Time: %,d ms", stop - start));

            app.awaitTermination(1, TimeUnit.SECONDS);
        }
    }
}
