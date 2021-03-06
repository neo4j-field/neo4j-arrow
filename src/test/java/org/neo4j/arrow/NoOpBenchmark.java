package org.neo4j.arrow;

import org.apache.arrow.flight.*;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.neo4j.arrow.action.ActionHandler;
import org.neo4j.arrow.action.Outcome;
import org.neo4j.arrow.demo.Client;
import org.neo4j.arrow.job.ReadJob;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A relatively simple NoOp test mostly for producing flamegraphs and doing some light integration testing.
 */
public class NoOpBenchmark {
    private static final org.slf4j.Logger logger;
    private final static String ACTION_NAME = "NoOp";

    static {
        // Set up nicer logging output.
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "[yyyy-MM-dd'T'HH:mm:ss:SSS]");
        System.setProperty("org.slf4j.simpleLogger.logFile", "System.out");
        logger = org.slf4j.LoggerFactory.getLogger(NoOpBenchmark.class);
    }

    private static final double[] PAYLOAD = { 1d, 2d, 3d, 4d, 5d, 6d, 7d, 8d, 9d, 10d,
            1d, 2d, 3d, 4d, 5d, 6d, 7d, 8d, 9d, 10d,
            1d, 2d, 3d, 4d, 5d, 6d, 7d, 8d, 9d, 10d,
            1d, 2d, 3d, 4d, 5d, 6d, 7d, 8d, 9d, 10d,
            1d, 2d, 3d, 4d, 5d, 6d, 7d, 8d, 9d, 10d,
            1d, 2d, 3d, 4d, 5d, 6d, 7d, 8d, 9d, 10d,
            1d, 2d, 3d, 4d, 5d, 6d, 7d, 8d, 9d, 10d,
            1d, 2d, 3d, 4d, 5d, 6d, 7d, 8d, 9d, 10d,
            1d, 2d, 3d, 4d, 5d, 6d, 7d, 8d, 9d, 10d,
            1d, 2d, 3d, 4d, 5d, 6d, 7d, 8d, 9d, 10d,
            1d, 2d, 3d, 4d, 5d, 6d, 7d, 8d, 9d, 10d,
            1d, 2d, 3d, 4d, 5d, 6d, 7d, 8d, 9d, 10d,
            1d, 2d, 3d, 4d, 5d, 6d, 7d, 8d };

    private static class NoOpRecord implements RowBasedRecord {

        @Override
        public Value get(int index) {
            return new Value() {
                private final List<Double> doubleList = Arrays.stream(PAYLOAD).boxed().collect(Collectors.toList());

                @Override
                public int size() {
                    return doubleList.size();
                }

                @Override
                public List<Double> asDoubleList() {
                    return doubleList;
                }

                @Override
                public double[] asDoubleArray() {
                    return PAYLOAD;
                }

                @Override
                public Type type() {
                    return Type.DOUBLE_ARRAY;
                }
            };
        }

        @Override
        public Value get(String field) {
            return get(1);
        }

        @Override
        public List<String> keys() {
            return List.of("n");
        }
    }

    private static class NoOpJob extends ReadJob {

        final CompletableFuture<Integer> future;
        final int numResults;

        public NoOpJob(int numResults, CompletableFuture<Long> signal) {
            super();
            this.numResults = numResults;

            future = CompletableFuture.supplyAsync(() -> {
                logger.info("Job starting");
                final RowBasedRecord record = new NoOpRecord();
                onFirstRecord(record);
                logger.info("Job feeding");

                BiConsumer<RowBasedRecord, Integer> consumer = super.futureConsumer.join();
                IntStream.range(1, numResults + 1)
                        .parallel()
                        .forEach(i -> consumer.accept(record, i));

                signal.complete(System.currentTimeMillis());
                logger.info("Job finished");
                onCompletion(() -> "done");
                return numResults;
            });
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public void close() {
        }
    }

    private static class NoOpHandler implements ActionHandler {

        final CompletableFuture<Long> signal;
        NoOpHandler(CompletableFuture<Long> signal) {
            this.signal = signal;
        }

        @Override
        public List<String> actionTypes() {
            return List.of(ACTION_NAME);
        }

        @Override
        public List<ActionType> actionDescriptions() {
            return List.of(new ActionType(ACTION_NAME, "Nothing"));
        }

        @Override
        public Outcome handle(FlightProducer.CallContext context, Action action, Producer producer) {
            Assertions.assertEquals(ACTION_NAME, action.getType());
            final Ticket ticket = producer.ticketJob(new NoOpJob(1_000_000, signal));
            producer.setFlightInfo(ticket, new Schema(
                    List.of(new Field("embedding",
                            FieldType.nullable(new ArrowType.FixedSizeList(PAYLOAD.length)),
                            List.of(new Field("embedding",
                                    FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null))))));
            return Outcome.success(new Result(ticket.serialize().array()));
        }
    }

    @Test
    public void testSpeed() throws Exception {
        final Location location = Location.forGrpcInsecure("localhost", 12345);
        final CompletableFuture<Long> signal = new CompletableFuture<>();

        App app = new App(new RootAllocator(Long.MAX_VALUE), location);
        Client client = new Client(new RootAllocator(Long.MAX_VALUE), location);

        try {
            app.registerHandler(new NoOpHandler(signal));
            app.start();

            long start = System.currentTimeMillis();
            Action action = new Action(ACTION_NAME);
            client.run(action);
            long stop = signal.join();
            logger.info(String.format("Client Lifecycle Time: %,d ms", stop - start));

            app.awaitTermination(1, TimeUnit.SECONDS);
        } catch (Exception e) {
            e.printStackTrace();
        }
        app.close();
        client.close();
    }
}
