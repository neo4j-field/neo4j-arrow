package org.neo4j.arrow;

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.neo4j.arrow.action.GdsActionhandler;
import org.neo4j.arrow.action.GdsMessage;
import org.neo4j.arrow.demo.Client;
import org.neo4j.arrow.job.Job;
import org.neo4j.logging.Log;
import org.neo4j.logging.log4j.Log4jLogProvider;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class GdsRecordBenchmarkTest {
    private final static Log log;

    static {
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "[yyyy-MM-dd'T'HH:mm:ss:SSS]");
        System.setProperty("org.slf4j.simpleLogger.logFile", "System.out");
        log = new Log4jLogProvider(System.out).getLog(GdsRecordBenchmarkTest.class);
    }

    private static final double[] PAYLOAD = {
            1d, 2d, 3d, 4d, 5d, 6d, 7d, 8d, 9d, 10d, // 10
            1d, 2d, 3d, 4d, 5d, 6d, 7d, 8d, 9d, 10d, // 20
            1d, 2d, 3d, 4d, 5d, 6d, 7d, 8d, 9d, 10d, // 30
            1d, 2d, 3d, 4d, 5d, 6d, 7d, 8d, 9d, 10d, // 40
            1d, 2d, 3d, 4d, 5d, 6d, 7d, 8d, 9d, 10d, // 50
            1d, 2d, 3d, 4d, 5d, 6d, 7d, 8d, 9d, 10d, // 60
            1d, 2d, 3d, 4d, 5d, 6d, 7d, 8d, 9d, 10d, // 70
            1d, 2d, 3d, 4d, 5d, 6d, 7d, 8d, 9d, 10d, // 80
            1d, 2d, 3d, 4d, 5d, 6d, 7d, 8d, 9d, 10d, // 90
            1d, 2d, 3d, 4d, 5d, 6d, 7d, 8d, 9d, 10d, // 100
            1d, 2d, 3d, 4d, 5d, 6d, 7d, 8d, 9d, 10d, // 110
            1d, 2d, 3d, 4d, 5d, 6d, 7d, 8d, 9d, 10d, // 120
            1d, 2d, 3d, 4d, 5d, 6d, 7d, 8d, 9d, 10d, // 130
            1d, 2d, 3d, 4d, 5d, 6d, 7d, 8d, 9d, 10d, // 140
            1d, 2d, 3d, 4d, 5d, 6d, 7d, 8d, 9d, 10d, // 150
            1d, 2d, 3d, 4d, 5d, 6d, 7d, 8d, 9d, 10d, // 160
            1d, 2d, 3d, 4d, 5d, 6d, 7d, 8d, 9d, 10d, // 170
            1d, 2d, 3d, 4d, 5d, 6d, 7d, 8d, 9d, 10d, // 180
            1d, 2d, 3d, 4d, 5d, 6d, 7d, 8d, 9d, 10d, // 190
            1d, 2d, 3d, 4d, 5d, 6d, 7d, 8d, 9d, 10d, // 200
            1d, 2d, 3d, 4d, 5d, 6d, 7d, 8d, 9d, 10d, // 210
            1d, 2d, 3d, 4d, 5d, 6d, 7d, 8d, 9d, 10d, // 220
            1d, 2d, 3d, 4d, 5d, 6d, 7d, 8d, 9d, 10d, // 230
            1d, 2d, 3d, 4d, 5d, 6d, 7d, 8d, 9d, 10d, // 240
            1d, 2d, 3d, 4d, 5d, 6d, 7d, 8d, 9d, 10d, // 250
            1d, 2d, 3d, 4d, 5d, 6d }; // 256

    private static RowBasedRecord getRecord() {
        final RowBasedRecord record = new GdsRecord(1, Map.of("nodeProp", GdsRecord.wrapDoubleArray(PAYLOAD)));
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

            app.registerHandler(new GdsActionhandler(
                    (msg, mode, username, password) -> new NoOpJob(1_000_000, signal),
                    log));
            app.start();

            final long start = System.currentTimeMillis();
            final GdsMessage msg = new GdsMessage("neo4j", "mygraph", List.of("fastRp"), List.of());
            final Action action = new Action(GdsActionhandler.NODE_PROPS_ACTION, msg.serialize());
            client.run(action);
            final long stop = signal.join();
            log.info(String.format("Client Lifecycle Time: %,d ms", stop - start));

            app.awaitTermination(1, TimeUnit.SECONDS);
        }
    }
}
