package org.neo4j.arrow;

import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.*;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.neo4j.driver.*;
import org.neo4j.driver.util.Pair;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Neo4jProducer implements FlightProducer, AutoCloseable {
    final static int MAX_BATCH_SIZE = 1_000;

    final static String CYPHER_READ_ACTION = "cypherRead";
    final static String CYPHER_WRITE_ACTION = "cypherWrite";

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Neo4jProducer.class);

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
        logger.debug("getStream called: context={}, ticket={}", context, ticket.getBytes());

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

            org.neo4j.driver.Result result = job.getResult();
            Map<String, FieldVector> vectorMap = new HashMap<>();

            /*
                Do things synchronously for now while figuring out the API. This should either be
                adapted to use the AsyncSession or RsSession
             */
            int batchCnt = 0, currentPos = -1;
            while (result.hasNext()) {
                currentPos++;

                final Record record = result.next();
                final int idx = currentPos % MAX_BATCH_SIZE;

                // Are we starting a new batch?
                if (currentPos % MAX_BATCH_SIZE == 0) {
                    vectorMap.values().forEach(ValueVector::close);
                    vectorMap.clear();
                    for (String key : record.keys()) {
                        UInt4Vector vector = new UInt4Vector(key, allocator);
                        vector.allocateNew(MAX_BATCH_SIZE);
                        vectorMap.put(key, vector);
                    }
                }

                // Process Fields in the Record
                // XXX: for now use 4-byte uints for all fields
                for (Pair<String, Value> field : record.fields()) {
                    UInt4Vector vector = (UInt4Vector) vectorMap.get(field.key());
                    vector.setSafe(idx, field.value().asInt());
                }

                // Flush a full buffer!
                final int batchSize = (currentPos % MAX_BATCH_SIZE) + 1;
                if (batchSize == MAX_BATCH_SIZE) {
                    final List<ArrowFieldNode> nodes = new ArrayList<>();
                    logger.info("flushing batch of size = {}", batchSize);

                    // We need to tell the vectors how big they are...they don't know this!
                    vectorMap.values().stream().forEach(vector -> {
                        vector.setValueCount(batchSize);
                        nodes.add(new ArrowFieldNode(batchSize, 0));
                    });

                    // Build and put our Batch. Throw in some metadata about which batch # this is
                    try (ArrowRecordBatch batch = new ArrowRecordBatch(batchSize, nodes,
                            vectorMap.values().stream()
                                    .map(vector -> vector.getBuffers(true))
                                    .flatMap(Stream::of).collect(Collectors.toList()))) {
                        loader.load(batch);

                        final byte[] metaMsg = String.format("batch %d", ++batchCnt).getBytes(StandardCharsets.UTF_8);
                        final ArrowBuf metaBuf = allocator.buffer(metaMsg.length);
                        metaBuf.writeBytes(metaMsg);
                        listener.putNext(metaBuf);
                        logger.debug("put batch (location={}, batchSize={})", currentPos + 1, batchSize);
                    }
                }
            }

            // If our current batch isn't "full" at this point, it wasn't flushed.
            final int batchSize = (currentPos % MAX_BATCH_SIZE) + 1;
            if (batchSize < MAX_BATCH_SIZE) {
                logger.info("flushing final batch, location={}, batchSize={}...", currentPos + 1, batchSize);
                final List<ArrowFieldNode> nodes = new ArrayList<>();

                // We need to tell the vectors how big they are...they don't know this!
                vectorMap.values().stream().forEach(vector -> {
                    vector.setValueCount(batchSize);
                    nodes.add(new ArrowFieldNode(batchSize, 0));
                });

                try (ArrowRecordBatch batch = new ArrowRecordBatch(batchSize, nodes,
                        vectorMap.values().stream()
                                .map(vector -> vector.getBuffers(true))
                                .flatMap(Stream::of).collect(Collectors.toList()))) {
                    loader.load(batch);

                    final byte[] metaMsg = String.format("batch %d", ++batchCnt).getBytes(StandardCharsets.UTF_8);
                    final ArrowBuf metaBuf = allocator.buffer(metaMsg.length);
                    metaBuf.writeBytes(metaMsg);
                    listener.putNext(metaBuf);
                    logger.debug("put batch (location={}, batchSize={})", currentPos + 1, batchSize);
                }
            }

            listener.completed();
            logger.info("completed sending {} batches for ticket {}", batchCnt, ticket);
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
        logger.debug("listFlights called: context={}, criteria={}", context, criteria);
        flightMap.values().stream().forEach(listener::onNext);
        listener.onCompleted();
    }

    @Override
    public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
        logger.debug("getFlightInfo called: context={}, descriptor={}", context, descriptor);
        FlightInfo info = flightMap.values().stream().findFirst().get();
        return info;
    }

    @Override
    public Runnable acceptPut(CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
        logger.debug("acceptPut called");
        return ackStream::onCompleted;
    }

    @Override
    public void doAction(CallContext context, Action action, StreamListener<Result> listener) {
        logger.debug("doAction called: action.type={}, peer={}", action.getType(), context.peerIdentity());

        if (action.getBody().length < 1) {
            logger.warn("missing body in action {}", action.getType());
            listener.onError(CallStatus.INVALID_ARGUMENT.withDescription("missing body in action!").toRuntimeException());
            return;
        }

        switch (action.getType()) {
            case CYPHER_READ_ACTION:
                // XXX use a simple for now, adapt to async or rx session later to by stream friendly
                Session session = driver.session(SessionConfig.builder()
                        .withDatabase("neo4j")
                        .withDefaultAccessMode(AccessMode.READ)
                        .withFetchSize(2500)
                        .build());
                String query = StandardCharsets.UTF_8.decode(ByteBuffer.wrap(action.getBody())).toString();

                org.neo4j.driver.Result result = session.run(query);

                // TODO: we need to build a Schema instance from inspecting the result's keys
                // XXX does this block? does this consume the full stream?
                List<String> keys = result.keys();
                Ticket ticket = new Ticket(UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));
                jobMap.put(ticket, new Neo4jJob(session, result));

                final List<Field> fields = keys.stream()
                        .map(key -> new Field(key,
                                FieldType.nullable(new ArrowType.Int(32, true)),
                                null))
                        .collect(Collectors.toList());
                final FlightInfo info = new FlightInfo(new Schema(fields, null),
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
        logger.debug("listActions called: context={}", context);
        listener.onNext(cypherRead);
        listener.onNext(cypherWrite);
        listener.onCompleted();
    }

    @Override
    public void close() throws Exception {
        driver.close();

        for (Neo4jJob job : jobMap.values()) {
            job.close();
        }
    }
}
