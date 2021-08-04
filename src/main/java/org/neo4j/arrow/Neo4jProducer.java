package org.neo4j.arrow;

import org.apache.arrow.flight.*;
import org.apache.arrow.flight.Result;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.neo4j.driver.*;
import org.neo4j.driver.summary.ResultSummary;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Neo4jProducer implements FlightProducer, AutoCloseable {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Neo4jProducer.class);

    final static int MAX_BATCH_SIZE = 100_000;

    // TODO: enum?
    final static String CYPHER_READ_ACTION = "cypherRead";
    final static String CYPHER_WRITE_ACTION = "cypherWrite";
    final static String CYPHER_STATUS_ACTION = "cypherStatus";
    final ActionType cypherRead = new ActionType(CYPHER_READ_ACTION, "Submit a READ transaction");
    final ActionType cypherWrite = new ActionType(CYPHER_WRITE_ACTION, "Submit a WRITE transaction");
    final ActionType cypherStatus = new ActionType(CYPHER_STATUS_ACTION, "Check status of a Cypher transaction");

    final Location location;
    final BufferAllocator allocator;

    /* Holds all known current streams based on their tickets */
    final Map<Ticket, FlightInfo> flightMap;
    /* Holds all existing jobs based on their tickets */
    final Map<Ticket, Neo4jJob> jobMap;

    /* Global Neo4j Driver instance */
    final Driver driver;

    public Neo4jProducer(BufferAllocator allocator, Location location) {
        this.location = location;
        this.allocator = allocator;
        this.flightMap = new ConcurrentHashMap<>();

        this.driver = GraphDatabase.driver(Config.neo4jUrl, AuthTokens.basic(Config.username, Config.password));
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
        BufferAllocator streamAllocator = allocator.newChildAllocator(
                String.format("stream-%s", UUID.nameUUIDFromBytes(ticket.getBytes())),
                1024*1024, Config.maxStreamMemory);
        if (streamAllocator == null) {
            logger.error("oh no", new Exception("couldn't create child allocator!"));
        }
        try {
            final VectorSchemaRoot root = VectorSchemaRoot.create(info.getSchema(), streamAllocator);
            final VectorLoader loader = new VectorLoader(root);
            final AtomicInteger cnt = new AtomicInteger(0);
            final IntVector vector = (IntVector) info.getSchema()
                    .findField("n")
                    .createVector(streamAllocator);
            vector.allocateNew(MAX_BATCH_SIZE);
            listener.start(root);

            // A bit hacky, but provide the core processing logic as a Consumer
            job.consume(record -> {
                int idx = cnt.getAndIncrement();
                // logger.info("record: {}", record);
                vector.setSafe(idx % MAX_BATCH_SIZE, record.get(0).asInt());

                if ((idx + 1) % MAX_BATCH_SIZE == 0) {
                    vector.setValueCount(MAX_BATCH_SIZE);
                    try (ArrowRecordBatch batch = new ArrowRecordBatch(MAX_BATCH_SIZE,
                            Arrays.asList(new ArrowFieldNode(MAX_BATCH_SIZE, 0)),
                            Arrays.asList(vector.getBuffers(false)))) {
                        loader.load(batch);
                        listener.putNext();
                        logger.debug("{}", streamAllocator);
                    }
                }
            });

            // Add a job cancellation hook
            listener.setOnCancelHandler(() -> job.cancel(true));

            // For now, we just block until the job finishes and then tell the client we're complete
            ResultSummary summary = job.get(5, TimeUnit.MINUTES);
            logger.info("finished get_stream, summary: {}", summary);

            listener.completed();
            flightMap.remove(ticket);
            AutoCloseables.close(root, vector);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                AutoCloseables.close(job, streamAllocator);
            } catch (Exception e) {
                logger.error("problem when auto-closing after get_stream", e);
            }
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

        // We assume for now that our "commands" are just a serialized ticket
        try {
            Ticket ticket = Ticket.deserialize(ByteBuffer.wrap(descriptor.getCommand()));
            FlightInfo info = flightMap.get(ticket);

            if (info == null) {
                logger.info("no flight found for ticket {}", ticket);
                throw CallStatus.NOT_FOUND.withDescription("no flight found").toRuntimeException();
            }
            return info;
        } catch (IOException e) {
            logger.error("failed to get flight info", e);
            throw CallStatus.INVALID_ARGUMENT.withDescription("failed to interpret ticket").toRuntimeException();
        }
    }

    @Override
    public Runnable acceptPut(CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
        logger.debug("acceptPut called");
        return ackStream::onCompleted;
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
            case CYPHER_STATUS_ACTION:
                try {
                    final Ticket ticket = Ticket.deserialize(ByteBuffer.wrap(action.getBody()));
                    Neo4jJob job = jobMap.get(ticket);
                    if (job != null) {
                        listener.onNext(new Result(job.getStatus().toString().getBytes(StandardCharsets.UTF_8)));
                        listener.onCompleted();
                    } else {
                        listener.onError(CallStatus.NOT_FOUND.withDescription("no job for ticket").toRuntimeException());
                    }
                } catch (IOException e) {
                    logger.error("problem servicing cypher status action", e);
                    listener.onError(CallStatus.INTERNAL.withDescription(e.getMessage()).toRuntimeException());
                }
                break;
            case CYPHER_READ_ACTION:
                CypherMessage msg;
                try {
                    msg = CypherMessage.deserialize(action.getBody());
                } catch (IOException e) {
                    listener.onError(CallStatus.INVALID_ARGUMENT.withDescription("invalid CypherMessage").toRuntimeException());
                    return;
                }

                /* Ticket this job */
                final Ticket ticket = new Ticket(UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));
                final Neo4jJob job = new Neo4jJob(driver, msg, AccessMode.READ);
                jobMap.put(ticket, job);

                /* We need to wait for the first record to discern our final schema */
                final Future<Record> futureRecord = job.getFirstRecord();
                CompletableFuture.runAsync(() -> {
                    Record record;
                    try {
                        record = futureRecord.get();
                    } catch (InterruptedException e) {
                        logger.error(String.format("flight info task for job %s interrupted", job), e);
                    } catch (ExecutionException e) {
                        logger.error(String.format("flight info task for job %s failed", job), e);
                    }
                    final FlightInfo info = new FlightInfo(new Schema(Arrays.asList(new Field("n", FieldType.nullable(new ArrowType.Int(32, true)), null))),
                            FlightDescriptor.command(ticket.getBytes()),
                            Arrays.asList(new FlightEndpoint(ticket, location)), -1, -1);
                    flightMap.put(ticket, info);
                    logger.info("finished ticketing flight {}", info);
                });

                /* We're taking off, so hand the ticket back to our client. */
                listener.onNext(new Result(ticket.serialize().array()));
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
        listener.onNext(cypherStatus);
        listener.onCompleted();
    }

    @Override
    public void close() throws Exception {
        logger.debug("closing");
        for (Neo4jJob job : jobMap.values()) {
            job.close();
        }
        AutoCloseables.close(allocator, driver);
    }
}
