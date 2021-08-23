package org.neo4j.arrow;

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
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
import java.util.function.Function;
import java.util.stream.DoubleStream;
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
        switch (type) {
            case "float":
                float[] data = new float[256];
                for (int i=0; i<256; i++)
                    data[i] = (float)i;
                return new GdsNodeRecord(1, new String[] {"embedding"},
                        new RowBasedRecord.Value[] { GdsRecord.wrapFloatArray(data) },
                        Function.identity());
            case "double":
                return new GdsNodeRecord(1, new String[] {"embedding"},
                        new RowBasedRecord.Value[] {
                                GdsRecord.wrapDoubleArray(
                                        IntStream.range(1, 256).boxed().mapToDouble(Integer::doubleValue).toArray())
                        }, Function.identity());
            case "long":
                return new GdsNodeRecord(1, new String[] {"embedding"},
                        new RowBasedRecord.Value[] {
                                GdsRecord.wrapLongArray(LongStream.range(1, 256).toArray())
                        }, Function.identity());
        }

        throw new RuntimeException("bad type");
    }

    private class NoOpJob extends Job {

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

    private static Stream<Arguments> provideDifferentNativeArrays() {
        return Stream.of(
                Arguments.of("long"),
                Arguments.of("float"),
                Arguments.of("double")
        );
    }

    @ParameterizedTest
    @MethodSource("provideDifferentNativeArrays")
    //@Timeout(value = 5, unit = TimeUnit.MINUTES)
    public void testSpeed(String type) throws Exception {
        final BufferAllocator serverAllocator = new RootAllocator(Integer.MAX_VALUE);
        final BufferAllocator clientAllocator = new RootAllocator(Integer.MAX_VALUE);

        final Location location = Location.forGrpcInsecure("localhost", 12345);
        final CompletableFuture<Long> signal = new CompletableFuture<>();

        try (App app = new App(serverAllocator, location);
             Client client = new Client(clientAllocator, location)) {

            app.registerHandler(new GdsActionHandler(
                    (msg, mode, username, password) ->
                            new NoOpJob(2_000_000, signal, type),
                    log));
            app.start();

            final long start = System.currentTimeMillis();
            final GdsMessage msg = new GdsMessage("neo4j", "mygraph", GdsMessage.RequestType.NODE, List.of("fastRp"), List.of());
            final Action action = new Action(GdsActionHandler.NODE_PROPS_ACTION, msg.serialize());
            client.run(action);
            final long stop = signal.join();
            log.info(String.format("Client Lifecycle Time: %,d ms", stop - start));

            app.awaitTermination(1, TimeUnit.SECONDS);
        }
    }
}
