package org.neo4j.arrow;

import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.neo4j.driver.*;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class Neo4jProducer implements FlightProducer, AutoCloseable {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Neo4jProducer.class);

    final static String CYPHER_READ_ACTION = "cypherRead";
    final static String CYPHER_WRITE_ACTION = "cypherWrite";
    final ActionType cypherRead = new ActionType(CYPHER_READ_ACTION, "Submit a READ transaction");
    final ActionType cypherWrite = new ActionType(CYPHER_WRITE_ACTION, "Submit a WRITE transaction");

    final Location location;
    final BufferAllocator allocator;
    final Map<Ticket, FlightInfo> flightMap;

    final Map<Ticket, Neo4jJob> jobMap;
    final Driver driver;

    public Neo4jProducer(BufferAllocator allocator, Location location) {
        this.location = location;
        this.allocator = allocator;
        this.flightMap = new ConcurrentHashMap<>();

        this.driver = GraphDatabase.driver("neo4j://localhost:7687",
                AuthTokens.basic("neo4j", "password"));
        this.jobMap = new ConcurrentHashMap<>();
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
        logger.info("getStream called: context={}, ticket={}", context, ticket.getBytes());

        final Neo4jJob job = jobMap.get(ticket);
        if (job == null) {
            listener.error(CallStatus.NOT_FOUND.withDescription("can't find job").toRuntimeException());
            return;
        }
        final FlightInfo info = flightMap.get(ticket);
        if (info == null) {
            listener.error(CallStatus.NOT_FOUND.withDescription("can't find info").toRuntimeException());
            return;
        }

        logger.info("producing stream for ticket {}", ticket);

        try (VectorSchemaRoot root = VectorSchemaRoot.create(info.getSchema(), allocator)) {
            listener.start(root);
            final VectorLoader loader = new VectorLoader(root);

            final int maxBatchSize = 5;
            int currentSize = 0;

            org.neo4j.driver.Result result = job.getResult();
            UInt8Vector vector = null;

            while (result.hasNext()) {
                final Record record = result.next();
                final Value value = record.get("n");

                final int idx = currentSize % maxBatchSize;

                if (idx == 0) {
                    // New batch
                    if (vector != null) {
                        // flush old
                        vector.setValueCount(maxBatchSize);
                        logger.info("flushing vector {}...", vector);
                        final ArrowFieldNode node = new ArrowFieldNode(maxBatchSize, 0);
                        final ArrowRecordBatch batch = new ArrowRecordBatch(maxBatchSize, Arrays.asList(node),
                                Arrays.asList(vector.getBuffers(false)));
                        logger.info("loading batch {}", batch);
                        loader.load(batch);
                        listener.putNext();
                        safeSleep(2000);
                    }

                    vector = new UInt8Vector("n", allocator);
                    vector.allocateNew(maxBatchSize);
                    logger.info("allocated new vector {}", vector);
                }
                vector.set(idx, value.asLong());
                logger.info("added {} to vector @ idx {} (currentSize = {})", value.asLong(), idx, currentSize);

                currentSize++;
            }

            // flush remainder
            if (vector != null) {
                // flush old
                vector.setValueCount(currentSize % maxBatchSize);
                logger.info("flushing vector {}...", vector);
                final ArrowFieldNode node = new ArrowFieldNode(currentSize % maxBatchSize, 0);
                final ArrowRecordBatch batch = new ArrowRecordBatch(currentSize % maxBatchSize, Arrays.asList(node),
                        Arrays.asList(vector.getBuffers(false)));
                logger.info("loading batch {}", batch);
                loader.load(batch);
                listener.putNext();
            }

            listener.completed();
        } catch (Exception e) {
            logger.error("error producing stream", e);
            listener.error(e);
        } finally {
            job.close();
            jobMap.remove(ticket);
            flightMap.remove(ticket);
        }
    }

    @Override
    public void listFlights(CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {
        logger.info("listFlights called: context={}, criteria={}", context, criteria);
        flightMap.values().stream().forEach(listener::onNext);
        listener.onCompleted();
    }

    @Override
    public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
        logger.info("getFlightInfo called: context={}, descriptor={}", context, descriptor);
        FlightInfo info = flightMap.values().stream().findFirst().get();
        return info;
    }

    @Override
    public Runnable acceptPut(CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
        logger.info("acceptPut called");
        return ackStream::onCompleted;
    }

    private static void safeSleep(long millis) {
        try {
            logger.info("sleeping {}ms", millis);
            Thread.sleep(millis);
        } catch (Exception e) {
            return;
        }
    }

    @Override
    public void doAction(CallContext context, Action action, StreamListener<Result> listener) {
        logger.info("doAction called: action.type={}, peer={}", action.getType(), context.peerIdentity());

        if (action.getBody().length < 1) {
            logger.warn("missing body in action {}", action.getType());
            listener.onError(CallStatus.INVALID_ARGUMENT.withDescription("missing body in action!").toRuntimeException());
            return;
        }

        switch (action.getType()) {
            case CYPHER_READ_ACTION:
                Session session = driver.session(SessionConfig.builder()
                        .withDatabase("neo4j")
                        .withDefaultAccessMode(AccessMode.READ)
                        .withFetchSize(2500)
                        .build());
                String query = StandardCharsets.UTF_8.decode(ByteBuffer.wrap(action.getBody())).toString();

                org.neo4j.driver.Result result = session.run(query);
                // XXX does this block? does this consume the full stream?
                List<String> keys = result.keys();
                Ticket ticket = new Ticket(UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));
                jobMap.put(ticket, new Neo4jJob(session, result));

                FlightInfo info = new FlightInfo(new Schema(keys.stream()
                            .map(key -> new Field(key,
                                    FieldType.nullable(new ArrowType.Int(Long.SIZE, true)),
                                    null))
                            .collect(Collectors.toList()), null),
                        FlightDescriptor.command(action.getBody()),
                        Arrays.asList(new FlightEndpoint(ticket, location)), -1, -1);
                flightMap.put(ticket, info);

                listener.onCompleted();
                break;
            case CYPHER_WRITE_ACTION:
                listener.onError(CallStatus.UNIMPLEMENTED.withDescription("can't do writes yet!").toRuntimeException());
                break;
            default:
                logger.warn("unknown action {}", action.getType());
                listener.onError(CallStatus.INVALID_ARGUMENT.withDescription("unknown action!").toRuntimeException());
                break;
        }

    }

    @Override
    public void listActions(CallContext context, StreamListener<ActionType> listener) {
        logger.info("listActions called: context={}", context);
        listener.onNext(cypherRead);
        listener.onNext(cypherWrite);
        listener.onCompleted();
    }

    @Override
    public void close() throws Exception {

    }
}
