package org.neo4j.arrow.demo;

import org.apache.arrow.flight.*;
import org.apache.arrow.flight.auth2.BasicAuthCredentialWriter;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.neo4j.arrow.Config;
import org.neo4j.arrow.action.CypherActionHandler;
import org.neo4j.arrow.action.CypherMessage;
import org.neo4j.arrow.action.StatusHandler;
import org.neo4j.arrow.job.Job;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class Client implements AutoCloseable {

    private static final org.slf4j.Logger logger;

    static {
        // Set up nicer logging output.
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "[yyyy-MM-dd'T'HH:mm:ss:SSS]");
        System.setProperty("org.slf4j.simpleLogger.logFile", "System.out");
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        logger = org.slf4j.LoggerFactory.getLogger(Client.class);
    }

    final private BufferAllocator allocator;
    final private Location location;
    final private FlightClient client;
    final private CredentialCallOption option;

    public Client(BufferAllocator allocator, Location location) {
        this.allocator = allocator;
        this.location = location;

        client = FlightClient.builder()
                .allocator(allocator)
                .location(location)
                .build();
        option = new CredentialCallOption(new BasicAuthCredentialWriter(Config.username, Config.password));
    }

    private void getStream(Ticket ticket) throws Exception {
        logger.info("Fetching stream for ticket: {}",
                StandardCharsets.UTF_8.decode(ByteBuffer.wrap(ticket.getBytes())));

        long start = System.currentTimeMillis();
        long cnt = 0;
        long byteCnt = 0;

        try (FlightStream stream = client.getStream(ticket, option);
                VectorSchemaRoot root = stream.getRoot();
                VectorSchemaRoot downloadedRoot = VectorSchemaRoot.create(root.getSchema(), allocator)) {
            final VectorLoader loader = new VectorLoader(downloadedRoot);
            final VectorUnloader unloader = new VectorUnloader(root);

            logger.info("got schema: {}", root.getSchema().toJson());

            while (stream.next()) {
                try (ArrowRecordBatch batch = unloader.getRecordBatch()) {
                    logger.info("got batch, sized: {}", batch.getLength());
                    loader.load(batch);
                    cnt += batch.getLength();
                    for (FieldVector vector : root.getFieldVectors())
                        byteCnt += vector.getBufferSize();
                    if (cnt % 25_000 == 0) {
                        logger.info(String.format("Current Row @ %,d: [fields:%s, batchLen: %,d]",
                                cnt, root.getSchema().getFields(), batch.getLength()));
                        for (FieldVector vector : root.getFieldVectors()) {
                            logger.info(String.format("vector %s: %,d bytes", vector.getName(), vector.getBufferSize()));
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
        long delta = System.currentTimeMillis() - start;
        logger.info(String.format("Finished. Count=%,d rows, Time Delta=%,d ms, Rate=%,d rows/s, Bytes=%,d",
                cnt, delta, 1000 * (cnt / delta), byteCnt ));
        logger.info(String.format("Throughput: %,d MiB/s", ((byteCnt / delta) * 1000) / (1024 * 1024)));
    }

    public void run(Action action) throws Exception {
        Result result = client.doAction(action, option).next();
        Ticket ticket = Ticket.deserialize(ByteBuffer.wrap(result.getBody()));
        logger.info("ticketId: {}", StandardCharsets.UTF_8.decode(ticket.serialize()));

        // Use a silly retry strategy for now, waiting until we have a PRODUCING status
        int retries = 100;
        boolean ready = false;
        while (!ready && retries > 0) {
            Action check = new Action(StatusHandler.STATUS_ACTION, ticket.serialize().array());
            try {
                result = client.doAction(check, option).next();
                String status = StandardCharsets.UTF_8.decode(ByteBuffer.wrap(result.getBody())).toString();
                logger.info("status: {}", status);
                if (status.equalsIgnoreCase(Job.Status.PRODUCING.toString()))
                    ready = true;
                Thread.sleep(100);
            } catch (FlightRuntimeException runtimeException) {
                if (runtimeException.status().code() != FlightStatusCode.NOT_FOUND)
                    throw runtimeException;
                Thread.sleep(100);
            }
            retries--;
        }

        // Use a silly retry strategy for now because there could be a time lag between PRODUCING
        // and the stream actually being available to read
        retries = 100;
        while (retries > 0) {
            try {
                // XXX Might throw other exceptions
                getStream(ticket);
                break;
            } catch (FlightRuntimeException runtimeException) {
                if (runtimeException.status().code() != FlightStatusCode.NOT_FOUND)
                    throw runtimeException;
                logger.info("waiting before trying again...");
                Thread.sleep(100);
            }
            retries--;
        }
    }

    public static void main(String[] args) throws Exception {
        final Location location = Location.forGrpcInsecure(Config.host, Config.port);
        BufferAllocator allocator = null;
        Client client = null;

        try {
            logger.info("starting client connection to {}", location.getUri());
            allocator = new RootAllocator(Integer.MAX_VALUE);
            client = new Client(allocator, location);

            CypherMessage msg = new CypherMessage("neo4j", "UNWIND range(1, $rows) AS row\n" +
                    "RETURN row, [_ IN range(1, $dimension) | rand()] as fauxEmbedding",
                    Map.of("rows", 1_000_000, "dimension", 128));
            Action action = new Action(CypherActionHandler.CYPHER_READ_ACTION, msg.serialize());
            client.run(action);
            logger.info("client finished!");
        } finally {
            try {
                AutoCloseables.close(client, allocator);
            } catch (Exception e) {
                logger.error("error on cleanup", e);
            }
        }
    }

    @Override
    public void close() throws Exception {
        AutoCloseables.close(client, allocator);
    }
}
