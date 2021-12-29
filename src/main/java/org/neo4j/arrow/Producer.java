package org.neo4j.arrow;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.arrow.flight.*;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.BaseListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.neo4j.arrow.action.ActionHandler;
import org.neo4j.arrow.action.Outcome;
import org.neo4j.arrow.action.ServerInfoHandler;
import org.neo4j.arrow.action.StatusHandler;
import org.neo4j.arrow.job.Job;
import org.neo4j.arrow.job.ReadJob;
import org.neo4j.arrow.job.WriteJob;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
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
        handlerMap.put(ServerInfoHandler.SERVER_INFO, new ServerInfoHandler());
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

        // Track certain stream states. This is flaky...
        final AtomicBoolean isFeeding = new AtomicBoolean(true);
        final AtomicBoolean flusherDone = new AtomicBoolean(false);
        final AtomicBoolean fatality = new AtomicBoolean(false);

        // We utilize 2 queues to allow reuse of BufferAllocators and ValueVectors
        final BlockingQueue<WorkBuffer> workQueue = new LinkedBlockingQueue<>();
        final BlockingQueue<WorkBuffer> availableBuffers = new LinkedBlockingQueue<>();

        // 75% of stream memory is available for converting records to vectors, the other
        // 25% is utilized by the flusher to take ownership of the buffers before transmission
        final BufferAllocator converterAllocator = allocator.newChildAllocator(
                String.format("convert-%s", UUID.nameUUIDFromBytes(ticket.getBytes())),
                0, 3 * Math.floorDiv(Config.maxStreamMemory, 4));
        final BufferAllocator transmitAllocator = allocator.newChildAllocator(
                String.format("transmit-%s", UUID.nameUUIDFromBytes(ticket.getBytes())),
                0, Math.floorDiv(Config.maxStreamMemory, 4));

        // The heart of the stream producer...
        try (VectorSchemaRoot root = VectorSchemaRoot.create(info.getSchema(), transmitAllocator)) {
            final VectorLoader loader = new VectorLoader(root);
            final Schema schema = info.getSchema();
            final List<Field> fieldList = info.getSchema().getFields();
            logger.debug("using schema: {}", schema);

            // Add a job cancellation hook
            listener.setOnCancelHandler(() -> {
                logger.info("client disconnected or cancelled stream");
                job.cancel(true);
            });

            // Our buffers
            for (int i = 0; i < job.maxPartitionCnt; i++) {
                final long maxAllocation = Math.floorDiv(converterAllocator.getLimit(), job.maxPartitionCnt);
                final WorkBuffer buffer = new WorkBuffer(fieldList, converterAllocator, maxAllocation, job.maxRowCount);
                buffer.init();
                availableBuffers.add(buffer);
            }
            logger.info(String.format("prepared %,d buffers", availableBuffers.size()));

            // Our work queue consumer
            final CompletableFuture<Void> flushJob = CompletableFuture.runAsync(() -> {
                while (isFeeding.get() || !workQueue.isEmpty()) {
                    try {
                        final WorkBuffer workBuffer = workQueue.poll(100, TimeUnit.MILLISECONDS);
                        if (workBuffer != null) {
                            if (workBuffer.getVectorDimension() > 0) {
                                workBuffer.prepareForFlush(); // TODO: move prepareForFlush into transfer method?

                                // Take ownership of the backing Arrow buffers
                                final int vectorDimension = workBuffer.getVectorDimension();
                                final List<ValueVector> vectors = workBuffer.transfer(transmitAllocator);

                                // Before we perform a flush, which could block, re-queue this buffer
                                workBuffer.init();
                                availableBuffers.add(workBuffer);

                                flush(listener, loader, schema, vectors, vectorDimension);
                            } else {
                                logger.warn("empty work buffer, re-initializing and making available");
                                workBuffer.init();
                                availableBuffers.add(workBuffer);
                            }
                        }
                    } catch (InterruptedException e) {
                        logger.error("flush job interrupted!", e);
                        return;
                    } catch (Exception e) {
                        logger.error("problem flushing :(", e);
                        job.cancel(true);
                        fatality.set(true);
                        workQueue.forEach(WorkBuffer::release);
                    }
                }
                flusherDone.set(true);
                logger.info("flusher for job {} finished", job);
                assert(workQueue.size() == 0);
            }, Executors.newSingleThreadExecutor((new ThreadFactoryBuilder())
                    .setNameFormat("arrow-flusher").setDaemon(true).build()));

            // Signal the client that we're about to start the stream
            listener.start(root);

            logger.info("starting consumer");
            // Core job logic
            final AtomicInteger drops = new AtomicInteger(0);
            final AtomicLong pauses = new AtomicLong(0);
            job.consume((record, partitionKey) -> {
                if (fatality.get()) {
                    // drop record
                    return;
                }

                try {
                    WorkBuffer buffer;
                    int retries = 100_000;
                    do {
                        buffer = availableBuffers.poll(100, TimeUnit.MILLISECONDS);
                        if (buffer == null) pauses.incrementAndGet();
                    } while (--retries > 0 && buffer == null);
                    if (buffer == null) {
                        logger.warn("failed to acquire an available buffer :(");
                        drops.incrementAndGet();
                        return;
                    }

                    if (buffer.convert(record) == job.maxRowCount) {
                        // move the buffer to the flushable queue
                        retries = 10_000;
                        boolean accepted = false;
                        do {
                            accepted = workQueue.offer(buffer, 100, TimeUnit.MILLISECONDS);
                            if (!accepted) pauses.incrementAndGet();
                        } while(--retries > 0 && !accepted);
                        if (!accepted) {
                            logger.warn("failed to queue full buffer for flusher :(");
                            buffer.init();
                            availableBuffers.add(buffer);
                        }
                    } else {
                        // return to the queue
                        availableBuffers.add(buffer);
                    }
                } catch (Exception e) {
                    fatality.set(true);
                    logger.error("aborting stream due to error: " + e.getMessage());
                    job.cancel(true);
                }
            });

            // This should block until all data from the Job is prepared for the stream
            job.get();

            // Drain the flush worker
            logger.info("waiting up to {} seconds for flush task to finish...", Config.arrowFlushTimeout);
            isFeeding.set(false);
            flushJob.get(Config.arrowFlushTimeout, TimeUnit.SECONDS);

            // Clean up any outstanding buffers
            assert(availableBuffers.size() == job.maxPartitionCnt);
            availableBuffers.stream()
                    .filter(buffer -> buffer.getVectorDimension() > 0)
                    .forEach(buffer -> {
                        buffer.prepareForFlush();
                        try {
                            flush(listener, loader, schema, buffer.getVectors(), buffer.getVectorDimension());
                        } catch (Exception e) {
                            logger.warn("failed to flush remaining data in a buffer");
                        }
                    });
            logger.info("flushing complete");

            // Just in case there's anything lingering
            AutoCloseables.close(availableBuffers);
            AutoCloseables.close(workQueue);

            if (drops.get() > 0 || pauses.get() > 0) {
                logger.warn(String.format("dropped %,d rows, paused %,d times", drops.get(), pauses.get()));
            }
            logger.debug("finishing getStream for ticket {}", ticket);

        } catch (Exception e) {
            // Ideally we don't get here...errors should be handled above if at all possible.
            fatality.set(true);
            logger.error(String.format("fatal error in stream: %s", e.getMessage()), e);
            listener.error(CallStatus.INTERNAL.withCause(e).withDescription(e.getMessage()).toRuntimeException());
        } finally {
            flightMap.remove(ticket);
            if (!fatality.get()) listener.completed();

            /*
             * We *must* close the allocators after the VectorSchemaRoot!
             */
            AutoCloseables.closeNoChecked(converterAllocator);
            AutoCloseables.closeNoChecked(transmitAllocator);
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
     */
    private static void flush(ServerStreamListener listener, VectorLoader loader, Schema schema,
                              List<ValueVector> vectors, int dimension) {
        final List<ArrowFieldNode> nodes = new ArrayList<>();
        final List<ArrowBuf> buffers = new ArrayList<>();

        try {
            for (ValueVector vector : vectors) {
                nodes.add(new ArrowFieldNode(dimension, vector.getNullCount()));

                if (vector instanceof BaseListVector) {
                    // Variable-width ListVectors have some special crap we need to deal with
                    if (vector instanceof ListVector) {
                        buffers.add(vector.getValidityBuffer());
                        buffers.add(vector.getOffsetBuffer());
                    } else {
                        buffers.add(vector.getValidityBuffer());
                    }

                    for (FieldVector child : ((BaseListVector) vector).getChildrenFromFields()) {
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
                    Collections.addAll(buffers, vector.getBuffers(false));
                }
            }
        } catch (Exception e) {
            logger.error("error preparing batch", e);
            vectors.forEach(ValueVector::close);
            throw e;
        }

        // The actual data transmission...
        try (ArrowRecordBatch batch = new ArrowRecordBatch(dimension, nodes, buffers)) {
            loader.load(batch);
            listener.putNext(); // TODO: check isReady(), yield if not
        } catch (Exception e) {
            listener.error(CallStatus.UNKNOWN.withDescription("Unknown error during batching").toRuntimeException());
            throw e;
        } finally {
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
        assert job != null;
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
        logger.info("closing producer...");
        for (Job job : jobMap.values()) {
            job.cancel(true);
            job.get(5, TimeUnit.SECONDS);
        }
        AutoCloseables.close(closeable);
        AutoCloseables.close(allocator);
    }
}
