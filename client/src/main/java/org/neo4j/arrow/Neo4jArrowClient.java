package org.neo4j.arrow;

import org.apache.arrow.flight.*;
import org.apache.arrow.flight.auth2.BasicAuthCredentialWriter;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

public class Neo4jArrowClient implements AutoCloseable {

    private static final org.slf4j.Logger logger;

    static {
        // Set up nicer logging output.
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "[yyyy-MM-dd'T'HH:mm:ss:SSS]");
        System.setProperty("org.slf4j.simpleLogger.logFile", "System.out");
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        logger = org.slf4j.LoggerFactory.getLogger(Neo4jArrowClient.class);
    }

    final private BufferAllocator allocator;
    final private Location location;
    final private FlightClient client;
    final private CredentialCallOption option;

    public Neo4jArrowClient(BufferAllocator allocator, Location location) {
        this.allocator = allocator;
        this.location = location;

        client = FlightClient.builder()
                .allocator(allocator)
                .location(location)
                .build();

        final FlightCallHeaders headers = new FlightCallHeaders();
        headers.insert("authorization",
                String.format("Basic %s", Base64.getEncoder()
                        .encodeToString("neo4j:password".getBytes(StandardCharsets.UTF_8))));
        option = new CredentialCallOption(new BasicAuthCredentialWriter("neo4j", "password"));
    }

    private void getStream(Ticket ticket) {
        logger.info("Fetching stream for ticket: {}",
                StandardCharsets.UTF_8.decode(ByteBuffer.wrap(ticket.getBytes())));

        long start = System.currentTimeMillis();
        long cnt = 0;

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
                    if (cnt % 25_000 == 0)
                        logger.info("Current Row @ {}: [fields:{}, batchLen: {}]", cnt, root.getSchema().getFields(), batch.getLength());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        long delta = System.currentTimeMillis() - start;
        logger.info(String.format("Finished. Count=%,d rows, Time Delta=%,d ms, Rate=%,d rows/s",
                cnt, delta, 1000 * (cnt / delta) ));
    }

    public void run() throws InterruptedException, IOException {
        client.listActions(option)
                .forEach(action -> logger.info("found action: {}", action.getType()));

        CypherMessage msg = new CypherMessage("UNWIND range(1, $rows) AS row\n" +
                "RETURN row, [_ IN range(1, $dimension) | rand()] as fauxEmbedding",
                Map.of("rows", 1_000_000, "dimension", 128));

        Action action = new Action("cypherRead", msg.serialize());
        Result result = client.doAction(action, option).next();
        Ticket ticket = Ticket.deserialize(ByteBuffer.wrap(result.getBody()));
        logger.info("ticketId: {}", StandardCharsets.UTF_8.decode(ticket.serialize()));

        boolean ready = false;
        while (!ready) {
            Action check = new Action("cypherStatus", ticket.serialize().array());
            result = client.doAction(check, option).next();
            String status = StandardCharsets.UTF_8.decode(ByteBuffer.wrap(result.getBody())).toString();
            logger.info("status: {}", status);
            if (status.equalsIgnoreCase(Neo4jJob.Status.PRODUCING.toString()))
                ready = true;
            else
                Thread.sleep(2000);
        }

        getStream(ticket);
    }

    public static void main(String[] args) throws Exception {
        final Location location = Location.forGrpcInsecure("localhost", 9999);
        BufferAllocator allocator = null;
        Neo4jArrowClient client = null;

        try {
            logger.info("starting client connection to {}", location.getUri());
            allocator = new RootAllocator(Integer.MAX_VALUE);
            client = new Neo4jArrowClient(allocator, location);
            client.run();
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
        AutoCloseables.close(allocator, client);
    }
}
