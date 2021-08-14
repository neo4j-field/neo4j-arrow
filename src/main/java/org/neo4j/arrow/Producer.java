package org.neo4j.arrow;

import org.apache.arrow.flight.*;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.BaseListVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionFixedSizeListWriter;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.neo4j.arrow.action.ActionHandler;
import org.neo4j.arrow.action.Outcome;
import org.neo4j.arrow.action.StatusHandler;
import org.neo4j.arrow.job.Job;
import org.neo4j.arrow.job.JobSummary;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class Producer implements FlightProducer, AutoCloseable {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Producer.class);

    final Location location;
    final BufferAllocator allocator;

    /* Holds all known current streams based on their tickets */
    final protected Map<Ticket, FlightInfo> flightMap = new ConcurrentHashMap<>();
    /* Holds all existing jobs based on their tickets */
    final protected Map<Ticket, Job> jobMap = new ConcurrentHashMap<>();
    /* All registered Action handlers */
    final protected Map<String, ActionHandler> handlerMap = new ConcurrentHashMap<>();

    public Producer(BufferAllocator allocator, Location location) {
        this.location = location;
        this.allocator = allocator;

        // Default event handlers
        handlerMap.put(StatusHandler.STATUS_ACTION, new StatusHandler());
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
        logger.debug("getStream called: context={}, ticket={}", context, ticket.getBytes());

        final Job job = jobMap.get(ticket);
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
                1024L, Config.maxStreamMemory);
        if (streamAllocator == null) {
            logger.error("oh no", new Exception("couldn't create child allocator!"));
            listener.error(CallStatus.INTERNAL.withDescription("failed to create child allocator").toRuntimeException());
        }
        try {
            final VectorSchemaRoot root = VectorSchemaRoot.create(info.getSchema(), streamAllocator);
            final VectorLoader loader = new VectorLoader(root);
            final AtomicInteger cnt = new AtomicInteger(0);

            // We need to dynamically build out our vectors based on the identified schema
            final HashMap<String, FieldVector> vectorMap = new HashMap<>();
            // Complex things like Lists need special writers to fill them :-(
            final Map<String, BaseWriter.ListWriter> writerMap = new ConcurrentHashMap<>();

            final List<Field> fieldList = info.getSchema().getFields();

            logger.info("using schema: {}", info.getSchema().toJson());
            for (Field field : fieldList) {
                FieldVector fieldVector = root.getVector(field);
                fieldVector.setInitialCapacity(Config.arrowBatchSize);
                fieldVector.allocateNew();
                vectorMap.put(field.getName(), fieldVector);
            }

            // Signal the client that we're about to start the stream
            listener.start(root);

            // A bit hacky, but provide the core processing logic as a Consumer
            final AtomicBoolean errored = new AtomicBoolean(false);
            job.consume(record -> {
                try {
                    int idx = cnt.getAndIncrement();

                    for (Field field : fieldList) {
                        final RowBasedRecord.Value value = record.get(field.getName());
                        final FieldVector vector = vectorMap.get(field.getName());
                        if (vector instanceof IntVector) {
                            ((IntVector) vector).set(idx, value.asInt());
                        } else if (vector instanceof BigIntVector) {
                            ((BigIntVector) vector).set(idx, value.asLong());
                        } else if (vector instanceof Float4Vector) {
                            ((Float4Vector) vector).set(idx, value.asFloat());
                        } else if (vector instanceof Float8Vector) {
                            ((Float8Vector) vector).set(idx, value.asDouble());
                        } else if (vector instanceof VarCharVector) {
                            ((VarCharVector) vector).set(idx, value.asString().getBytes(StandardCharsets.UTF_8));
                        } else if (vector instanceof FixedSizeListVector) {
                            // Used for GDS
                            final UnionFixedSizeListWriter writer =
                                    (UnionFixedSizeListWriter) writerMap.computeIfAbsent(field.getName(),
                                    s -> ((FixedSizeListVector) vector).getWriter());
                            writer.startList();
                            // XXX: Assumes all values share the same type and first value is non-null
                            switch (value.type()) {
                                case INT_ARRAY:
                                    for (int i : value.asIntArray())
                                        writer.writeInt(i);
                                    break;
                                case LONG_ARRAY:
                                    for (long l : value.asLongArray())
                                        writer.writeBigInt(l);
                                    break;
                                case FLOAT_ARRAY:
                                    for (float f : value.asFloatArray())
                                        writer.writeFloat4(f);
                                    break;
                                case DOUBLE_ARRAY:
                                    for (double d : value.asDoubleArray())
                                        writer.writeFloat8(d);
                                    break;
                                default:
                                    if (errored.compareAndSet(false, true)) {
                                        Exception e = CallStatus.INVALID_ARGUMENT.withDescription("invalid array type")
                                                .toRuntimeException();
                                        listener.error(e);
                                        logger.error("invalid array type: " + value.type(), e);
                                        job.cancel(true);
                                    }
                                    return;
                            }
                            writer.setValueCount(value.size());
                            writer.endList();
                        } else if (vector instanceof ListVector) {
                            // Used for Cypher
                            final UnionListWriter writer =
                                    (UnionListWriter) writerMap.computeIfAbsent(field.getName(),
                                            s -> ((ListVector) vector).getWriter());
                            writer.startList();
                            // XXX: Assumes all values are doubles for now :-(
                            for (Double d : value.asDoubleList())
                                writer.writeFloat8(d);
                            writer.setValueCount(value.asList().size());
                            writer.endList();
                        }
                    }

                    if ((idx + 1) == Config.arrowBatchSize) {
                        flush(listener, streamAllocator, loader, vectorMap, fieldList, idx);
                        cnt.set(0);
                        writerMap.clear();
                        vectorMap.values().clear();

                        for (Field field : fieldList) {
                            FieldVector fieldVector = root.getVector(field);
                            fieldVector.setInitialCapacity(Config.arrowBatchSize);
                            fieldVector.allocateNew();
                            vectorMap.put(field.getName(), fieldVector);
                        }
                    }
                } catch (Exception e) {
                    if (errored.compareAndSet(false, true)) {
                        logger.error(e.getMessage(), e);
                        job.cancel(true);
                        listener.error(CallStatus.UNKNOWN.withDescription(e.getMessage()).toRuntimeException());
                    }
                }
            });

            // Add a job cancellation hook
            listener.setOnCancelHandler(() -> job.cancel(true));

            // For now, we just block until the job finishes and then tell the client we're complete
            JobSummary summary = job.get(15, TimeUnit.MINUTES);

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
            e.printStackTrace();
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

    private void handleRecord(RowBasedRecord record ){

    }

    /**
     * Flush out our vectors into the stream. At this point, all data is Arrow-based.
     *
     * @param listener
     * @param streamAllocator
     * @param loader
     * @param vectorMap
     * @param fieldList
     * @param idx
     */
    private void flush(ServerStreamListener listener, BufferAllocator streamAllocator, VectorLoader loader,
                       Map<String, FieldVector> vectorMap, List<Field> fieldList, int idx) {
        final List<ArrowFieldNode> nodes = new ArrayList<>();
        final List<ArrowBuf> buffers = new ArrayList<>();

        for (Field field : fieldList) {
            final FieldVector vector = vectorMap.get(field.getName());
            logger.debug(String.format("batching vector %s: %s", field.getName(), vector.toString()));
            vector.setValueCount(idx + 1);

            nodes.add(new ArrowFieldNode(idx + 1, 0));

            if (vector instanceof BaseListVector) {
                // Variable-width ListVectors have some special crap we need to deal with
                if (vector instanceof ListVector) {
                    ((ListVector) vector).setLastSet(idx + 1);
                    buffers.add(vector.getValidityBuffer());
                    buffers.add(vector.getOffsetBuffer());
                } else {
                    buffers.add(vector.getValidityBuffer());
                }

                for (FieldVector child : vector.getChildrenFromFields()) {
                    logger.debug("batching child vector {} ({}, {})", child.getName(), child.getValueCount(), child.getNullCount());
                    nodes.add(new ArrowFieldNode(child.getValueCount(), child.getNullCount()));
                    buffers.addAll(List.of(child.getBuffers(false)));
                }

            } else {
                for (ArrowBuf buf : vector.getBuffers(false)) {
                    logger.debug("adding buf {} for vector {}", buf, vector.getName());
                    buffers.add(buf);
                }
            }
        }

        logger.debug("ready to batch {} nodes with {} buffers", nodes.size(), buffers.size());

        // The actual data transmission...
        try (ArrowRecordBatch batch = new ArrowRecordBatch(idx + 1, nodes, buffers)) {
            loader.load(batch);
            listener.putNext();
            logger.info(String.format("put batch of %,d rows", batch.getLength()));
            logger.debug("{}", streamAllocator);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage(), e);
            listener.error(CallStatus.UNKNOWN.withDescription("unknown error during batching").toRuntimeException());
        }
    }

    /**
     * Ticket a Job and add it to the jobMap.
     * @param job
     * @return new Ticket
     */
    public Ticket ticketJob(Job job) {
        final Ticket ticket = new Ticket(UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));
        jobMap.put(ticket, job);
        return ticket;
    }

    public Job getJob(Ticket ticket) {
        return jobMap.get(ticket);
    }

    public void setFlightInfo(Ticket ticket, Schema schema) {
        final FlightInfo info = new FlightInfo(schema, FlightDescriptor.command(ticket.getBytes()),
                Arrays.asList(new FlightEndpoint(ticket, location)), -1, -1);
        flightMap.put(ticket, info);
        logger.info("set flight info {}", info);
    }

    public void deleteFlight(Ticket ticket) {
        // XXX just nuke map values for now
        logger.info("deleting flight for ticket {}", ticket);
        flightMap.remove(ticket);
        jobMap.remove(ticket);
    }

    public void registerHandler(ActionHandler handler) {
        handler.actionTypes().forEach(action -> handlerMap.put(action, handler));
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

        ActionHandler handler = handlerMap.get(action.getType());
        if (handler == null) {
            final Exception e = CallStatus.NOT_FOUND.withDescription("unsupported action").toRuntimeException();
            logger.error(String.format("no handler for action type %s", action.getType()), e);
            listener.onError(e);
            return;
        }

        try {
            final Outcome outcome = handler.handle(context, action, this);
            if (outcome.isSuccessful()) {
                listener.onNext(outcome.result.get());
                listener.onCompleted();
            } else {
                final CallStatus callStatus = outcome.callStatus.get();
                logger.error(callStatus.description(), callStatus.toRuntimeException());
                listener.onError(callStatus.toRuntimeException());
            }
        } catch (Exception e) {
            logger.error(String.format("unexpected exception: %s", e.getMessage()), e);
            listener.onError(CallStatus.INTERNAL.withDescription("internal error").toRuntimeException());
        }
    }

    @Override
    public void listActions(CallContext context, StreamListener<ActionType> listener) {
        logger.debug("listActions called: context={}", context);
        handlerMap.values().stream()
                .distinct()
                .flatMap(handler -> handler.actionDescriptions().stream())
                .forEach(listener::onNext);
        listener.onCompleted();
    }

    @Override
    public void close() throws Exception {
        logger.debug("closing");
        for (Job job : jobMap.values()) {
            job.close();
        }
        AutoCloseables.close(allocator);
    }
}
