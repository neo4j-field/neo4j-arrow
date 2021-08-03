package org.neo4j.arrow;

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
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

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

    private void getStream(Ticket ticket, Schema schema) {
        logger.info("Fetching stream for ticket: {}",
                StandardCharsets.UTF_8.decode(ByteBuffer.wrap(ticket.getBytes())));

        long start = System.currentTimeMillis();
        long cnt = 0;
        try (
                FlightStream stream = client.getStream(ticket, option);
                VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {

            final VectorLoader loader = new VectorLoader(root);
            final VectorUnloader unloader = new VectorUnloader(stream.getRoot());

            while (stream.next()) {
                try (ArrowRecordBatch batch = unloader.getRecordBatch()) {
                    // logger.info("got batch, sized: {}", batch.getLength());
                    loader.load(batch);
                    FieldVector fv = root.getVector("n");
                    logger.info(String.format("vector: len=%,d", fv.getValueCount()));
                    logger.info("fieldVector: {}", fv);
                    cnt += fv.getValueCount();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        long delta = System.currentTimeMillis() - start;
        logger.info(String.format("Finished. Count=%,d rows, Time Delta=%,d ms, Rate=%,d rows/s",
                cnt, delta, 1000 * (cnt / delta) ));
    }

    public void run() {
        client.listActions(option)
                .forEach(action -> logger.info("found action: {}", action.getType()));

        Schema schema = new Schema(Arrays.asList(
                new Field("n", FieldType.nullable(new ArrowType.Int(32, true)), null)),
                null);
        CypherMessage msg = new CypherMessage("UNWIND range(1, toInteger(1e7)) AS n RETURN n;",
                new HashMap<>());

        Action action = new Action("cypherRead", msg.serialize());
        Result result = client.doAction(action, option).next();
        String ticketId = StandardCharsets.UTF_8.decode(ByteBuffer.wrap(result.getBody())).toString();
        logger.info("ticketId: {}", ticketId);

        Action check = new Action("cypherStatus", ticketId.getBytes(StandardCharsets.UTF_8));
        result = client.doAction(check, option).next();

        logger.info("status: {}", StandardCharsets.UTF_8.decode(ByteBuffer.wrap(result.getBody())));

        List<FlightInfo> flights = new ArrayList<>();
        client.listFlights(Criteria.ALL, option).forEach(flights::add);

        flights.forEach(info -> {
            final FlightDescriptor descriptor = info.getDescriptor();
            logger.info("processing FlightInfo for flight '{}'",
                    StandardCharsets.UTF_8.decode(ByteBuffer.wrap(descriptor.getCommand())));
            getStream(info.getEndpoints().get(0).getTicket(), info.getSchema());
        });
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
