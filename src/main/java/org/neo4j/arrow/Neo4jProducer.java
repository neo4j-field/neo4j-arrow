package org.neo4j.arrow;

import org.apache.arrow.flatbuf.Utf8;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.*;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
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
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Neo4jProducer.class);

    final static int MAX_BATCH_SIZE = 10_000;

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

            int currentSize = 0;
            org.neo4j.driver.Result result = job.getResult();
            Map<String, FieldVector> vectorMap = null;

            /*
                Do things synchronously for now while figuring out the API. This should either be
                adapted to use the AsyncSession or Reac
             */
            int cnt = 0;
            while (result.hasNext()) {
                final Record record = result.next();
                final int idx = currentSize % MAX_BATCH_SIZE;

                if (idx == 0) {
                    // New batch
                    if (vectorMap != null) {
                        // flush old
                        vectorMap.values().stream().forEach(vector -> vector.setValueCount(MAX_BATCH_SIZE));
                        final List<ArrowFieldNode> nodes = new ArrayList<>();
                        vectorMap.values().stream().forEach(vector -> {
                            vector.setValueCount(MAX_BATCH_SIZE);
                            nodes.add(new ArrowFieldNode(MAX_BATCH_SIZE, 0));
                        });

                        try (ArrowRecordBatch batch = new ArrowRecordBatch(MAX_BATCH_SIZE, nodes,
                                vectorMap.values().stream()
                                        .map(vector -> vector.getBuffers(true))
                                        .flatMap(Stream::of).collect(Collectors.toList()))) {
                            loader.load(batch);
                            final byte[] metaMsg = String.format("batch %d", ++cnt).getBytes(StandardCharsets.UTF_8);
                            final ArrowBuf metaBuf = allocator.buffer(metaMsg.length);
                            metaBuf.writeBytes(metaMsg);
                            listener.putNext(metaBuf);
                            vectorMap.values().forEach(vector -> vector.close());
                            logger.info("put batch (pos={}, sz={})", currentSize, currentSize % MAX_BATCH_SIZE);
                        }
                    }

                    vectorMap = new HashMap<>();
                    for (String key : record.keys()) {
                        UInt4Vector vector = new UInt4Vector(key, allocator);
                        vector.allocateNew();
                        vectorMap.put(key, vector);
                    }
                }

                for (Pair<String, Value> field : record.fields()) {
                    UInt4Vector vector = (UInt4Vector) vectorMap.get(field.key());
                    vector.set(idx, field.value().asInt());
                }
                currentSize++;
            }

            // flush remainder
            // logger.info("flushing vector {}...", vector);
            if (currentSize % MAX_BATCH_SIZE > 0) {
                final List<ArrowFieldNode> nodes = new ArrayList<>();
                for (FieldVector vector : vectorMap.values()) {
                    vector.setValueCount(currentSize % MAX_BATCH_SIZE);
                    nodes.add(new ArrowFieldNode(currentSize % MAX_BATCH_SIZE, 0));
                }
                try (ArrowRecordBatch batch = new ArrowRecordBatch(currentSize % MAX_BATCH_SIZE, nodes,
                        vectorMap.values().stream()
                                .map(vector -> vector.getBuffers(true))
                                .flatMap(Stream::of).collect(Collectors.toList()))) {
                    loader.load(batch);
                    final byte[] metaMsg = String.format("batch %d", ++cnt).getBytes(StandardCharsets.UTF_8);
                    final ArrowBuf metaBuf = allocator.buffer(metaMsg.length);
                    metaBuf.writeBytes(metaMsg);
                    listener.putNext(metaBuf);
                    vectorMap.values().forEach(vector -> vector.close());
                    logger.info("put batch (pos={}, sz={})", currentSize, currentSize % MAX_BATCH_SIZE);
                }
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
                // XXX use a simple for now, adapt to async or rx session later to by stream friendly
                Session session = driver.session(SessionConfig.builder()
                        .withDatabase("neo4j")
                        .withDefaultAccessMode(AccessMode.READ)
                        .withFetchSize(2500)
                        .build());
                String query = StandardCharsets.UTF_8.decode(ByteBuffer.wrap(action.getBody())).toString();

                org.neo4j.driver.Result result = session.run(query);
                logger.info("XXXXX asking for keys");
                // XXX does this block? does this consume the full stream?
                List<String> keys = result.keys();
                logger.info("XXXXXX got keys {}", keys);
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
        logger.info("listActions called: context={}", context);
        listener.onNext(cypherRead);
        listener.onNext(cypherWrite);
        listener.onCompleted();
    }

    @Override
    public void close() throws Exception {

    }
}
