package org.neo4j.arrow;

import org.apache.arrow.flight.*;
import org.apache.arrow.memory.AllocationListener;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.BaseListVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.complex.impl.UnionFixedSizeListWriter;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.TransferPair;
import org.neo4j.arrow.action.ActionHandler;
import org.neo4j.arrow.action.Outcome;
import org.neo4j.arrow.action.StatusHandler;
import org.neo4j.arrow.job.Job;
import org.neo4j.arrow.job.ReadJob;
import org.neo4j.arrow.job.WriteJob;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The Producer encapsulates the core Arrow Flight orchestration logic including both the RPC
 * framework and stream wrangling.
 *
 * <p>
 * RPC {@link Action}s are made available via registration into the {@link #handlerMap} via
 * {@link #registerHandler(ActionHandler)}.
 * </p>
 * <p>
 * Streams, aka "Flights", are indexed by {@link Ticket}s and kept in {@link #flightMap}. As a
 * consequence, there's currently no multi-process support. The {@link Job} backing the stream
 * is kept in {@link #jobMap}.
 * </p>
 */
public class Producer implements FlightProducer, AutoCloseable {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Producer.class);

    final Location location;
    final BufferAllocator allocator;

    final Queue<AutoCloseable> closeable = new ConcurrentLinkedQueue<>();

    /* Holds all known current streams based on their tickets */
    final protected Map<Ticket, FlightInfo> flightMap = new ConcurrentHashMap<>();
    /* Holds all existing jobs based on their tickets */
    final protected Map<Ticket, Job> jobMap = new ConcurrentHashMap<>();
    /* All registered Action handlers */
    final protected Map<String, ActionHandler> handlerMap = new ConcurrentHashMap<>();

    public Producer(BufferAllocator parentAllocator, Location location) {
        this.location = location;
        this.allocator = parentAllocator.newChildAllocator("neo4j-flight-producer", 0, Config.maxArrowMemory);

        // Default event handlers
        handlerMap.put(StatusHandler.STATUS_ACTION, new StatusHandler());
    }

    private static class FlushWork {
        public final List<ValueVector> vectors;
        public final int vectorDimension;

        private FlushWork(List<ValueVector> vectors, int vectorDimension) {
            this.vectors = vectors;
            this.vectorDimension = vectorDimension;
        }

        public static FlushWork from(List<ValueVector> vectors, int vectorDimension) {
            return new FlushWork(vectors, vectorDimension);
        }

        public void release() {
            // Pull the rip cord
            vectors.forEach(ValueVector::close);
        }
    }

    /**
     * Attempt to get an Arrow stream for the given {@link Ticket}.
     *
     * @param context  the {@link org.apache.arrow.flight.FlightProducer.CallContext} that contains
     *                 details on the client (the peer) attempting to access the stream.
     * @param ticket   the {@link Ticket} for the Flight
     * @param listener the {@link org.apache.arrow.flight.FlightProducer.ServerStreamListener} for
     *                 returning results in the stream back to the caller.
     */
    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
        logger.info("getStream called: context={}, ticket={}", context, ticket.getBytes());

        final Job rawJob = jobMap.get(ticket);
        if (rawJob == null) {
            listener.error(CallStatus.NOT_FOUND.withDescription("No job for ticket").toRuntimeException());
            return;
        }

        if (!(rawJob instanceof ReadJob)) {
            listener.error(CallStatus.INTERNAL.withDescription("invalid job base type").toRuntimeException());
            return;
        }

        final ReadJob job = (ReadJob) rawJob;

        final FlightInfo info = flightMap.get(ticket);
        if (info == null) {
            listener.error(CallStatus.NOT_FOUND.withDescription("No flight for ticket").toRuntimeException());
            return;
        }

        // Our work queue for multiple producers, but a single consumer
        final BlockingDeque<FlushWork> workQueue = new LinkedBlockingDeque<>();
        final AtomicBoolean isFeeding = new AtomicBoolean(true);
        final AtomicBoolean fatality = new AtomicBoolean(false);

        try (BufferAllocator baseAllocator = allocator.newChildAllocator(
                String.format("convert-%s", UUID.nameUUIDFromBytes(ticket.getBytes())),
                0, Config.maxStreamMemory / 2);
             BufferAllocator transmitAllocator = allocator.newChildAllocator(
                     String.format("transmit-%s", UUID.nameUUIDFromBytes(ticket.getBytes())),
                     0, Config.maxStreamMemory / 2);
             VectorSchemaRoot root = VectorSchemaRoot.create(info.getSchema(), baseAllocator)) {

            final VectorLoader loader = new VectorLoader(root);
            final Schema schema = info.getSchema();
            logger.debug("using schema: {}", schema);

            // TODO: do we need to allocate explicitly? Or can we just not?
            final List<Field> fieldList = info.getSchema().getFields();

            // listener.setUseZeroCopy(true);
            // Add a job cancellation hook
            listener.setOnCancelHandler(() -> {
                logger.info("client disconnected or cancelled stream");
                job.cancel(true);
            });

            // Signal the client that we're about to start the stream
            listener.setUseZeroCopy(false);
            listener.start(root);

            // Tunable partition size...
            // TODO: figure out ideal way to set a good default based on host
            final int maxPartitions = Config.arrowMaxPartitions;

            // Map<String, BaseWriter.ListWriter> writerMap
            @SuppressWarnings("unchecked") final Map<String, BaseWriter.ListWriter>[] partitionedWriters = new Map[maxPartitions];
            @SuppressWarnings("unchecked") final List<FieldVector>[] partitionedVectorList = new List[maxPartitions];
            final BufferAllocator[] bufferAllocators = new BufferAllocator[maxPartitions];
            final AtomicInteger[] partitionedCounts = new AtomicInteger[maxPartitions];
            final Semaphore[] partitionedSemaphores = new Semaphore[maxPartitions];
            final Semaphore transferMutex = new Semaphore(1);

            // Our work queue consumer
            final CompletableFuture<Void> flushJob = CompletableFuture.runAsync(() -> {
                while (isFeeding.get() || !workQueue.isEmpty()) {
                    try {
                        final FlushWork work = workQueue.pollFirst(100, TimeUnit.MILLISECONDS);
                        if (work != null) {
                            transferMutex.acquire();
                            flush(listener, loader, schema, work.vectors, work.vectorDimension);
                            transferMutex.release();
                        }
                    } catch (InterruptedException e) {
                        logger.error("flush job interrupted!", e);
                        return;
                    } catch (Exception e) {
                        logger.error("problem flushing :(", e);
                        job.cancel(true);
                        fatality.set(true);
                        workQueue.forEach(FlushWork::release);
                    }
                }
                logger.info("flusher for job {} finished", job);
            }, Executors.newSingleThreadExecutor());

            // Wasteful, but pre-init for now
            for (int i = 0; i < maxPartitions; i++) {
                bufferAllocators[i] = baseAllocator.newChildAllocator(String.format("partition-%d", i), 0,
                        baseAllocator.getLimit() / maxPartitions);
                partitionedSemaphores[i] = new Semaphore(1);
                partitionedWriters[i] = new HashMap<>();
                partitionedCounts[i] = new AtomicInteger(0);
                partitionedVectorList[i] = new ArrayList<>(0);
            }

            logger.info("starting consumer");

            // Core job logic
            job.consume((record, partitionKey) -> {
                if (fatality.get()) {
                    // drop record
                    return;
                }
                // Trivial partitioning scheme...
                final int partition = Math.abs(partitionKey % maxPartitions);
                try {
                    partitionedSemaphores[partition].acquire();
                    final BufferAllocator streamAllocator = bufferAllocators[partition];
                    final AtomicInteger cnt = partitionedCounts[partition];
                    final int idx = cnt.getAndIncrement();
                    final List<FieldVector> vectorList = partitionedVectorList[partition];
                    final Map<String, BaseWriter.ListWriter> writerMap = partitionedWriters[partition];

                    if (idx == 0) {
                        logger.trace("starting new batch for partition {}", partition);
                        // (re)init field vectors
                        if (vectorList.size() == 0) {
                            for (Field field : fieldList) {
                                FieldVector fieldVector = field.createVector(streamAllocator);
                                vectorList.add(fieldVector);
                            }
                        }
                        for (FieldVector fieldVector : vectorList) {
                            int retries = 1000;
                            fieldVector.setInitialCapacity(Config.arrowBatchSize);
                            while (!fieldVector.allocateNewSafe() && --retries > 0) {
                                // logger.error("failed to allocate memory for field {}", fieldVector.getName());
                                try {
                                    Thread.sleep(100);
                                } catch (Exception ignored) {
                                }
                            }
                            if (retries == 0) {
                                fatality.set(true);
                                throw new Exception("failed to allocate memory for vector" + fieldVector.getName());
                            }
                        }
                    }

                    // TODO: refactor to using fixed arrays for speed
                    // Our translation guts...CPU intensive
                    for (int n = 0; n < fieldList.size(); n++) {
                        final Field field = fieldList.get(n);
                        final RowBasedRecord.Value value = record.get(n);
                        final FieldVector vector = vectorList.get(n);

                        if (vector instanceof IntVector) {
                            ((IntVector) vector).set(idx, value.asInt());
                        } else if (vector instanceof BigIntVector) {
                            ((BigIntVector) vector).set(idx, value.asLong());
                        } else if (vector instanceof Float4Vector) {
                            ((Float4Vector) vector).set(idx, value.asFloat());
                        } else if (vector instanceof Float8Vector) {
                            ((Float8Vector) vector).set(idx, value.asDouble());
                        } else if (vector instanceof VarCharVector && value.asString() != null) {
                            ((VarCharVector) vector).setSafe(idx, value.asString().getBytes(StandardCharsets.UTF_8));
                        } else if (vector instanceof FixedSizeListVector) {
                            // Used for GdsJobs
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
                                    if (fatality.compareAndSet(false, true)) {
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
                                            s -> {
                                                UnionListWriter w = ((ListVector) vector).getWriter();
                                                w.start();
                                                return w;
                                            });
                            writer.startList();
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
                                case INT_LIST:
                                    try {
                                        for (int i : value.asIntList()) {
                                            writer.writeInt(i);
                                        }
                                    } catch (OutOfMemoryException oom) {
                                        logger.error(String.format("OOM writing LONG_LIST %s (value size: %,d): %s",
                                                vector.getName(), value.size(), oom.getMessage()));
                                        fatality.set(true);
                                    }
                                    break;
                                case LONG_LIST:
                                    try {
                                        for (long l : value.asLongList()) {
                                            writer.writeBigInt(l);
                                        }
                                    } catch (OutOfMemoryException oom) {
                                        logger.error(String.format("OOM writing LONG_LIST %s (value size: %,d): %s",
                                                vector.getName(), value.size(), oom.getMessage()));
                                        fatality.set(true);
                                    }
                                    break;
                                case STRING_LIST:
                                    for (final String s : value.asStringList()) {
                                        // TODO: should we allocate a single byte array and not have to reallocate?
                                        final byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
                                        try (final ArrowBuf buf = streamAllocator.buffer(bytes.length)) {
                                            buf.setBytes(0, bytes);
                                            writer.writeVarChar(0, bytes.length, buf);
                                            logger.trace("wrote string {}", s);
                                        }
                                    }
                                    break;
                                default:
                                    for (final Object o : value.asList()) {
                                        // TODO: should we allocate a single byte array and not have to reallocate?
                                        final byte[] bytes = o.toString().getBytes(StandardCharsets.UTF_8);
                                        try (final ArrowBuf buf = streamAllocator.buffer(bytes.length)) {
                                            buf.setBytes(0, bytes);
                                            writer.writeVarChar(0, bytes.length, buf);
                                        }
                                    }
                                    break;
                            }
                            writer.endList();
                        }
                    }

                    // Flush at our batch size limit and reset our batch states.
                    if ((idx + 1) == Config.arrowBatchSize) {
                        writerMap.values().forEach(writer -> {
                            if (writer instanceof UnionFixedSizeListWriter) {
                                logger.trace("calling writer.end() on {}", writer);
                                //((UnionFixedSizeListWriter) writer).end();
                            } else if (writer instanceof UnionListWriter) {
                                logger.trace("calling writer.end() on {}", writer);
                                ((UnionListWriter) writer).end();
                            } else {
                                logger.warn("writer isn't what we thought! {}", writer.getClass().getCanonicalName());
                            }
                        });
                        // Yolo?
                        final ArrayList<ValueVector> copy = new ArrayList<>();
                        final int vectorSize = idx + 1;
                        try {
                            transferMutex.acquire();
                            for (final FieldVector vector : vectorList) {
                                final TransferPair tp = vector.getTransferPair(transmitAllocator);
                                tp.transfer();
                                copy.add(tp.getTo());
                            }
                        } finally {
                            transferMutex.release();
                        }

                        // Queue the flush work
                        logger.trace("flushing partition {}", partition);
                        workQueue.add(FlushWork.from(copy, vectorSize));

                        // Reset our partition state
                        cnt.set(0);
                        vectorList.forEach(FieldVector::clear);
                        writerMap.values().forEach(writer -> {
                            try {
                                writer.close();
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        });
                        writerMap.clear();
                    }
                } catch (Exception e) {
                    if (fatality.compareAndSet(false, true)) {
                        logger.error(e.getMessage(), e);
                        job.cancel(true);
                        listener.error(CallStatus.UNKNOWN.withDescription(e.getMessage()).toRuntimeException());
                    }
                } finally {
                    partitionedSemaphores[partition].release();
                }
            });

            // This should block until all data from the Job is prepared for the stream
            job.get();

            // Final conversion of stragglers...
            for (int partition = 0; partition < maxPartitions; partition++) {
                if (fatality.get())
                    break;

                final List<FieldVector> vectorList = partitionedVectorList[partition];
                final Map<String, BaseWriter.ListWriter> writerMap = partitionedWriters[partition];
                final int vectorSize = partitionedCounts[partition].get();

                writerMap.values().forEach(writer -> {
                    if (writer instanceof UnionFixedSizeListWriter) {
                        logger.trace("calling writer.end() on {}", writer);
                        // ((UnionFixedSizeListWriter) writer).end();
                    } else if (writer instanceof UnionListWriter) {
                        logger.trace("calling writer.end() on {}", writer);
                        ((UnionListWriter) writer).end();
                    } else {
                        logger.warn("writer isn't what we thought! {}", writer.getClass().getCanonicalName());
                    }
                });

                if (vectorSize > 0) {
                    final ArrayList<ValueVector> copy = new ArrayList<>();
                    try {
                        transferMutex.acquire();
                        for (FieldVector vector : partitionedVectorList[partition]) {
                            final TransferPair tp = vector.getTransferPair(transmitAllocator);
                            tp.transfer();
                            copy.add(tp.getTo());
                        }
                    } finally {
                        transferMutex.release();
                    }
                    workQueue.add(FlushWork.from(copy, vectorSize));
                }
                vectorList.forEach(FieldVector::close);
                writerMap.values().forEach(writer -> {
                    try {
                        writer.close();
                    } catch (Exception e) {
                        // XXX unhandled exception
                        e.printStackTrace();
                    }
                });
            }

            if (!isFeeding.compareAndSet(true, false)) {
                logger.error("invalid state: expected isFeeding == true");
                listener.error(CallStatus.INTERNAL.withDescription("unexpected state").toRuntimeException());
                return;
            }
            logger.info("waiting up to {} seconds for flushing to finish...", Config.arrowFlushTimeout);
            flushJob.get(Config.arrowFlushTimeout, TimeUnit.SECONDS);
            logger.info("flushing complete");

            // Close the allocators for each partition
            for (BufferAllocator allocator : bufferAllocators)
                allocator.close();

        } catch (Exception e) {
            isFeeding.set(false);
            fatality.set(true);

            workQueue.forEach(FlushWork::release);
            workQueue.clear();
            logger.error("ruh row", e);
            listener.error(CallStatus.INTERNAL.withCause(e).withDescription(e.getMessage()).toRuntimeException());
        } finally {
            logger.info("finishing getStream for ticket {}", ticket);
            flightMap.remove(ticket);
            listener.completed();
        }
    }

    /**
     * Flush out our vectors into the stream. At this point, all data is Arrow-based.
     * <p>
     * This part is tricky and requires turning Arrow vectors into Arrow Flight messages based
     * on the concept of {@link ArrowFieldNode}s and {@link ArrowBuf}s. Not as simple as "here's
     * my vectors!"
     * </p>
     *
     * @param listener  reference to the {@link ServerStreamListener}
     * @param loader    reference to the {@link VectorLoader}
     * @param vectors   list of {@link ValueVector}s corresponding to the columns of data to flush
     * @param dimension dimension of the vectors
     */
    private static void flush(ServerStreamListener listener, VectorLoader loader, Schema schema,
                              List<ValueVector> vectors, int dimension) throws Exception {
        final List<ArrowFieldNode> nodes = new ArrayList<>();
        final List<ArrowBuf> buffers = new ArrayList<>();

        try {
            for (ValueVector vector : vectors) {
                vector.setValueCount(dimension);
                nodes.add(new ArrowFieldNode(dimension, vector.getNullCount()));

                if (vector instanceof BaseListVector) {
                    // Variable-width ListVectors have some special crap we need to deal with
                    if (vector instanceof ListVector) {
                        logger.trace("working on ListVector {}", vector.getName());
                        ((ListVector) vector).setLastSet(dimension - 1);
                        buffers.add(vector.getValidityBuffer());
                        buffers.add(vector.getOffsetBuffer());
                    } else {
                        buffers.add(vector.getValidityBuffer());
                    }

                    for (FieldVector child : ((BaseListVector) vector).getChildrenFromFields()) {
                        logger.trace("batching child vector {} ({}, {})", child.getName(), child.getValueCount(), child.getNullCount());

                        nodes.add(new ArrowFieldNode(child.getValueCount(), child.getNullCount()));
                        if (vector instanceof ListVector && child instanceof UnionVector) {
                            final UnionVector uv = (UnionVector) child;
                            final String innerName = schema.findField(vector.getName())
                                    .getChildren().get(0).getName();

                            final FieldVector innerVector;
                            switch (innerName.toLowerCase(Locale.getDefault())) {
                                // XXX assume homogeneity
                                case "uint32":
                                    innerVector = uv.getIntVector();
                                    break;
                                case "int64":
                                    innerVector = uv.getBigIntVector();
                                    break;
                                case "utf8":
                                    innerVector = uv.getVarCharVector();
                                    break;
                                default:
                                    logger.warn("unhandled vector type: {}",
                                            schema.findField(vector.getName()).getChildren().get(0).getType());
                                    innerVector = uv.getStruct();
                                    break;
                            }
                            buffers.addAll(List.of(innerVector.getBuffers(false)));
                        } else {
                            buffers.addAll(List.of(child.getBuffers(false)));
                        }
                    }

                } else {
                    for (ArrowBuf buf : vector.getBuffers(false)) {
                        logger.trace("adding buf {} for vector {}", buf, vector.getName());
                        buffers.add(buf);
                    }
                }
            }
        } catch (Exception e) {
            vectors.forEach(ValueVector::close);
            logger.error("error preparing batch", e);
            throw e;
        }

        // The actual data transmission...
        try (ArrowRecordBatch batch = new ArrowRecordBatch(dimension, nodes, buffers)) {
            loader.load(batch);
            listener.putNext();
        } catch (Exception e) {
            listener.error(CallStatus.UNKNOWN.withDescription("Unknown error during batching").toRuntimeException());
            throw e;
        } finally {
            // We need to close our reference to the ValueVector to decrement the ref count in the underlying buffers.
            vectors.forEach(ValueVector::close);
        }
    }

    /**
     * Ticket a Job and add it to the jobMap.
     *
     * @param job instance of a Job
     * @return new Ticket
     */
    public Ticket ticketJob(Job job) {
        final Ticket ticket = new Ticket(UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));
        jobMap.put(ticket, job);
        logger.info("ticketed job {}", ticket);
        return ticket;
    }

    public Job getJob(Ticket ticket) {
        return jobMap.get(ticket);
    }

    public void setFlightInfo(Ticket ticket, Schema schema) {
        final Job job = getJob(ticket);
        if (job == null)
            throw CallStatus.INTERNAL.withDescription("no job for flight???").toRuntimeException();
        assert (job.getStatus() == Job.Status.PENDING || job.getStatus() == Job.Status.INITIALIZING);

        final FlightInfo info = new FlightInfo(schema, FlightDescriptor.command(ticket.getBytes()),
                List.of(new FlightEndpoint(ticket, location)), -1, -1);
        flightMap.put(ticket, info);

        // We need to flip the job status only after the flight map is updated otherwise we could race
        job.setStatus(Job.Status.PRODUCING);
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
        flightMap.values().forEach(listener::onNext);
        listener.onCompleted();
    }

    @Override
    public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
        logger.debug("getFlightInfo called: context={}, descriptor={}", context, descriptor);
        // XXX needs authorization

        // We assume for now that our "commands" are just a serialized ticket
        try {
            final Ticket ticket = Ticket.deserialize(ByteBuffer.wrap(descriptor.getCommand()));
            final FlightInfo info = flightMap.get(ticket);

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
        return () -> {
            Job rawJob;
            try {
                final Ticket ticket = Ticket.deserialize(ByteBuffer.wrap(flightStream.getDescriptor().getCommand()));
                rawJob = jobMap.get(ticket);

                // Do we even have a job for this ticket? Provide guidance if we don't.
                if (rawJob == null) {
                    RuntimeException e = CallStatus.NOT_FOUND.withDescription("job not found for ticket").toRuntimeException();
                    logger.error(String.format("can't find job for ticket %s", ticket), e);
                    ackStream.onError(e);
                    return;
                }
            } catch (IOException e) {
                // Could be a malformed ticket. Log something helpful on the server, but don't divulge too much to client.
                logger.error("failed to deserialize ticket", e);
                ackStream.onError(CallStatus.INVALID_ARGUMENT
                        .withDescription("could not deserialize ticket").toRuntimeException());
                return;
            }

            // Double-check our Job type
            if (!(rawJob instanceof WriteJob)) {
                logger.error("job isn't a write job");
                ackStream.onError(CallStatus.INVALID_ARGUMENT
                        .withDescription("provided ticket isn't for a write job").toRuntimeException());
                return;
            }

            // TODO: validate root.Schema

            final WriteJob job = (WriteJob) rawJob;
            final ArrowBatch arrowBatch = new ArrowBatch(flightStream.getSchema(), allocator, job.getJobId());

            // Process our stream. Everything in here needs to be checked for memory leaks!
            try (final VectorSchemaRoot streamRoot = flightStream.getRoot()) {

                logger.info("client putting stream with schema: {}", streamRoot.getSchema());
                // Consume the entire stream into memory
                long cnt = 0;
                while (flightStream.next()) {
                    if (cnt % 1_000 == 0) {
                        logger.info(String.format("consuming @ batch %,d each with %,d rows", cnt, streamRoot.getRowCount()));
                    }
                    arrowBatch.appendRoot(streamRoot);
                    ackStream.onNext(PutResult.metadata(flightStream.getLatestMetadata()));
                    cnt++;
                }
                logger.info(String.format("consumed %,d batches (%,d rows)", cnt, arrowBatch.rowCount));

                // XXX need a way to guarantee we call this at the end (I think?)
                flightStream.takeDictionaryOwnership();

                logger.info(String.format("produced ArrowBatch of est. size %,d MiB, actual size %,d MiB",
                        (arrowBatch.estimateSize() >> 20), (arrowBatch.actualSize() >> 20)));
                job.onComplete(arrowBatch); // TODO!!!
                closeable.add(arrowBatch);
            } catch (Exception e) {
                logger.error("error during batch unloading", e);
                ackStream.onError(CallStatus.INTERNAL.withDescription(e.getMessage()).toRuntimeException());
                try {
                    arrowBatch.close();
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
                return;
            }
            // TODO: we need to wait until the post-processing completes, need a callback here
            ackStream.onCompleted();

            // At this point we should have the full stream in memory.
            logger.debug("batch rowCount={}, schema={}", arrowBatch.getRowCount(), arrowBatch.getSchema());
        };
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
                assert (outcome.getResult().isPresent());
                listener.onNext(outcome.getResult().get());
                listener.onCompleted();
            } else {
                assert (outcome.getCallStatus().isPresent());
                final CallStatus callStatus = outcome.getCallStatus().get();
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
        logger.info("closing");
        for (Job job : jobMap.values()) {
            job.close();
        }
        AutoCloseables.close(closeable);
        AutoCloseables.close(allocator);
    }
}
