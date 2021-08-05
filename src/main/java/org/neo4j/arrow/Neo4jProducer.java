package org.neo4j.arrow;

import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.*;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.writer.FieldWriter;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.neo4j.driver.*;
import org.neo4j.driver.summary.ResultSummary;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Neo4jProducer implements FlightProducer, AutoCloseable {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Neo4jProducer.class);

    final static int MAX_BATCH_SIZE = 10;

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

            // We need to dynamically build out our vectors based on the identified schema
            final HashMap<String, FieldVector> vectorMap = new HashMap<>();
            // Complex things like Lists need special writers to fill them :-(
            final HashMap<String, FieldWriter> writerMap = new HashMap<>();

            final List<Field> fieldList = info.getSchema().getFields();

            logger.info("using schema: {}", info.getSchema().toJson());
            for (Field field : fieldList) {
                FieldVector fieldVector = root.getVector(field);
                fieldVector.allocateNew();
                vectorMap.put(field.getName(), fieldVector);
                logger.info("created fieldvector {} for field {}", fieldVector.getName(), field);
            }

            // Signal the client that we're about to start the stream
            listener.start(root);

            // A bit hacky, but provide the core processing logic as a Consumer
            job.consume(record -> {
                int idx = cnt.getAndIncrement();

                for (Field field : fieldList) {
                    final Value value = record.get(field.getName());
                    final FieldVector vector = vectorMap.get(field.getName());
                    if (vector instanceof IntVector) {
                        ((IntVector) vector).set(idx, value.asInt());
                        logger.info("set {} @ idx {} in {}", ((IntVector) vector).get(idx), idx, vector.getName());
                    } else if (vector instanceof BigIntVector) {
                        ((BigIntVector) vector).set(idx, value.asLong());
                    } else if (vector instanceof Float4Vector) {
                        ((Float4Vector)vector).set(idx, (float)value.asDouble());
                        logger.info("set {} @ idx {} in {}", ((Float4Vector) vector).get(idx), idx, vector.getName());
                    } else if (vector instanceof Float8Vector) {
                        ((Float8Vector)vector).set(idx, value.asDouble());
                    } else if (vector instanceof VarCharVector) {
                        ((VarCharVector)vector).set(idx, value.asString().getBytes(StandardCharsets.UTF_8));
                    } else if (vector instanceof ListVector) {
                        UnionListWriter writer = (UnionListWriter) writerMap.get(field.getName());
                        if (writer == null) {
                            writer = ((ListVector)vector).getWriter();
                            writer.allocate();
                            writerMap.put(field.getName(), writer);
                        }
                        writer.startList();
                        // TODO: support type mapping here...for now we assume numbers
                        for (Double d : value.asList(item -> item.asDouble(0.0))) {
                            logger.info("writing to list {} @ idx {} element {}", vector.getName(), idx, d);
                            writer.float8().writeFloat8(d);
                        }
                        writer.endList();
                    }
                }

                if ((idx + 1) == MAX_BATCH_SIZE) {
                    flush(listener, streamAllocator, loader, vectorMap, fieldList, idx);
                    cnt.set(0);
                }
            });

            // Add a job cancellation hook
            listener.setOnCancelHandler(() -> job.cancel(true));

            // For now, we just block until the job finishes and then tell the client we're complete
            ResultSummary summary = job.get(5, TimeUnit.MINUTES);

            // Final flush
            if (cnt.get() > 0)
                flush(listener, streamAllocator, loader, vectorMap, fieldList, cnt.decrementAndGet());

            logger.info("finished get_stream, summary: {}", summary);

            listener.completed();
            flightMap.remove(ticket);
            AutoCloseables.close(writerMap.values());
            AutoCloseables.close(vectorMap.values());
            AutoCloseables.close(root);

        } catch (Exception e) {
            logger.error("ruh row", e);
            throw CallStatus.INTERNAL.withCause(e).withDescription(e.getMessage()).toRuntimeException();
        } finally {
            try {
                AutoCloseables.close(job, streamAllocator);
            } catch (Exception e) {
                logger.error("problem when auto-closing after get_stream", e);
            }
        }
    }

    private void flush(ServerStreamListener listener, BufferAllocator streamAllocator, VectorLoader loader,
                       Map<String, FieldVector> vectorMap, List<Field> fieldList, int idx) {
        final List<ArrowFieldNode> nodes = new ArrayList<>();
        final List<ArrowBuf> buffers = new ArrayList<>();

        for (Field field : fieldList) {
            final FieldVector vector = vectorMap.get(field.getName());
            logger.info("batching vector {}", field.getName());
            vector.setValueCount(idx + 1);

            nodes.add(new ArrowFieldNode(idx + 1, 0));
            for (ArrowBuf buf : vector.getBuffers(false)) {
                logger.info("adding buf {} for vector {}", buf, vector.getName());
                buffers.add(buf);
            }

            if (vector instanceof ListVector) {
                ((ListVector) vector).setLastSet(idx + 1);
                for (FieldVector child : vector.getChildrenFromFields()) {
                    logger.info("batching child vector {} ({}, {})", child.getName(), child.getValueCount(), child.getNullCount());
                    nodes.add(new ArrowFieldNode(child.getValueCount(), child.getNullCount()));
                }
            }
        }

        logger.info("ready to batch {} nodes with {} buffers", nodes.size(), buffers.size());

        // The actual data transmission...
        try (ArrowRecordBatch batch = new ArrowRecordBatch(idx + 1, nodes, buffers)) {
            loader.load(batch);
            listener.putNext();
            logger.info("put batch of {} rows", batch.getLength());
            logger.debug("{}", streamAllocator);
        }

        vectorMap.values().forEach(ValueVector::reset);
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
                        return;
                    } catch (ExecutionException e) {
                        logger.error(String.format("flight info task for job %s failed", job), e);
                        return;
                    }
                    final List<Field> fields = new ArrayList<>();
                    record.fields().stream().forEach(pair -> {
                        final String fieldName = pair.key();
                        final Value value = pair.value();
                        // TODO: better mapping support?
                        logger.info("Translating Neo4j value {}", value.type().name());
                        switch (value.type().name()) {
                            case "INTEGER":
                                fields.add(new Field(fieldName,
                                        FieldType.nullable(new ArrowType.Int(32, true)), null));
                                break;
                            case "LONG":
                                fields.add(new Field(fieldName,
                                        FieldType.nullable(new ArrowType.Int(64, true)), null));
                                break;
                            case "FLOAT":
                                fields.add(new Field(fieldName,
                                        FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null));
                                break;
                            case "DOUBLE":
                                fields.add(new Field(fieldName,
                                        FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null));
                                break;
                            case "STRING":
                                fields.add(new Field(fieldName,
                                        FieldType.nullable(new ArrowType.Utf8()), null));
                                break;
                            case "LIST OF ANY?":
                                fields.add(new Field(fieldName,
                                        FieldType.nullable(new ArrowType.List()),
                                        // TODO: inspect inner type...assume doubles for now.
                                        Arrays.asList(new Field(fieldName,
                                                FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null))));
                                break;
                            default:
                                logger.info("not sure how to translate field {} with type {}",
                                        fieldName, value.type().name());
                                break;
                        }
                    });
                    final FlightInfo info = new FlightInfo(new Schema(fields),
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
