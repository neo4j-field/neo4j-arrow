package org.neo4j.arrow;

import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Test;
import org.neo4j.arrow.job.CypherMessage;
import org.neo4j.arrow.job.Job;
import org.neo4j.arrow.job.JobSummary;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class NoOpBenchmark {
    private static final org.slf4j.Logger logger;

    static {
        // Set up nicer logging output.
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "[yyyy-MM-dd'T'HH:mm:ss:SSS]");
        System.setProperty("org.slf4j.simpleLogger.logFile", "System.out");
        logger = org.slf4j.LoggerFactory.getLogger(NoOpBenchmark.class);
    }

    private class NoOpRecord implements Neo4jRecord {

        @Override
        public Value get(int index) {
            return new Value() {
                @Override
                public int asInt() {
                    return 1;
                }

                @Override
                public long asLong() {
                    return 1L;
                }

                @Override
                public float asFloat() {
                    return 1.0f;
                }

                @Override
                public double asDouble() {
                    return 1.0d;
                }

                @Override
                public String asString() {
                    return "1";
                }

                @Override
                public List<Value> asList() {
                    return List.of();
                }

                @Override
                public Type type() {
                    return Type.INT;
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

    private class NoOpJob extends Job {

        final CompletableFuture<Integer> future;
        final int numResults;

        public NoOpJob(int numResults, CompletableFuture<Long> signal) {
            super(new CypherMessage("RETURN 1", Map.of()), Mode.READ);
            this.numResults = numResults;

            future = CompletableFuture.supplyAsync(() -> {
                logger.info("Job starting");
                final Neo4jRecord record = new NoOpRecord();
                onFirstRecord(record);
                logger.info("Job feeding");
                Consumer<Neo4jRecord> consumer = super.futureConsumer.join();
                for (int i=0; i<numResults; i++)
                    consumer.accept(record);
                signal.complete(System.currentTimeMillis());
                logger.info("Job finished");
                onCompletion(new JobSummary() {
                    @Override
                    public String toString() {
                        return "Done";
                    }
                });
                return numResults;
            }).toCompletableFuture();
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public void close() throws Exception {

        }
    }

    @Test
    public void testSpeed() throws Exception {
        final BufferAllocator serverAllocator = new RootAllocator();
        final BufferAllocator clientAllocator = new RootAllocator();

        final Location location = Location.forGrpcInsecure("localhost", 12345);
        final CompletableFuture<Long> signal = new CompletableFuture<>();

        try (Neo4jFlightApp app = new Neo4jFlightApp(serverAllocator, location,
                (message, mode, username, password) -> new NoOpJob(10_000_000, signal));
             Neo4jArrowClient client = new Neo4jArrowClient(clientAllocator, location)) {

            app.start();

            long start = System.currentTimeMillis();
            client.run();
            long stop = signal.join();
            logger.info(String.format("Client Lifecycle Time: %,d ms", stop - start));

            app.awaitTermination(1, TimeUnit.SECONDS);
        }
    }
}
