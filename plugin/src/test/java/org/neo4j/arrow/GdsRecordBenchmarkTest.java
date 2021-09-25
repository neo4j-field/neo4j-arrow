package org.neo4j.arrow;

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.arrow.action.GdsActionHandler;
import org.neo4j.arrow.action.GdsMessage;
import org.neo4j.arrow.demo.Client;
import org.neo4j.arrow.job.ReadJob;
import org.neo4j.logging.Log;
import org.neo4j.logging.log4j.Log4jLogProvider;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

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

    private static RowBasedRecord getRecord(String type) {
        final int size = 256;
        switch (type) {
            case "float":
                float[] data = new float[size];
                for (int i=0; i<size; i++)
                    data[i] = (float)i;
                return new GdsNodeRecord(1, new String[] {"embedding"},
                        new RowBasedRecord.Value[] { GdsRecord.wrapFloatArray(data) },
                        Function.identity());
            case "double":
                return new GdsNodeRecord(1, new String[] {"embedding"},
                        new RowBasedRecord.Value[] {
                                GdsRecord.wrapDoubleArray(
                                        IntStream.range(1, size).boxed().mapToDouble(Integer::doubleValue).toArray())
                        }, Function.identity());
            case "long":
                return new GdsNodeRecord(1, new String[] {"embedding"},
                        new RowBasedRecord.Value[] {
                                GdsRecord.wrapLongArray(LongStream.range(1, size).toArray())
                        }, Function.identity());
        }

        throw new RuntimeException("bad type");
    }

    private static class NoOpJob extends ReadJob {

        final CompletableFuture<Integer> future;
        final int numResults;

        public NoOpJob(int numResults, CompletableFuture<Long> signal, String type) {
            super();
            this.numResults = numResults;

            future = CompletableFuture.supplyAsync(() -> {
                try {
                    log.info("Job starting");
                    final RowBasedRecord record = getRecord(type);
                    onFirstRecord(record);
                    log.info("Job feeding");
                    BiConsumer<RowBasedRecord, Integer> consumer = super.futureConsumer.join();

                    IntStream.range(1, numResults).parallel().forEach(i -> consumer.accept(record, i));

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

    private static Stream<Arguments> provideDifferentNativeArrays() {
        return Stream.of(
                Arguments.of("long"),
                Arguments.of("float"),
                Arguments.of("double")
        );
    }

    @ParameterizedTest
    @MethodSource("provideDifferentNativeArrays")
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    public void testSpeed(String type) throws Exception {
        final Location location = Location.forGrpcInsecure("localhost", 12345);
        final CompletableFuture<Long> signal = new CompletableFuture<>();

        try (App app = new App(new RootAllocator(Long.MAX_VALUE), location);
             Client client = new Client(new RootAllocator(Long.MAX_VALUE), location)) {

            app.registerHandler(new GdsActionHandler(
                    (msg, mode, username) -> new NoOpJob(1_000_000, signal, type), log));
            app.start();

            final long start = System.currentTimeMillis();
            final GdsMessage msg = new GdsMessage("neo4j", "mygraph",
                    GdsMessage.RequestType.node, List.of("fastRp"), List.of());
            final Action action = new Action(GdsActionHandler.NODE_READ_ACTION, msg.serialize());
            client.run(action);
            final long stop = signal.join();
            log.info(String.format("Client Lifecycle Time: %,d ms", stop - start));

            app.awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}
