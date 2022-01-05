package org.neo4j.arrow.job;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.AutoCloseableLock;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.neo4j.arrow.batch.ArrowBatch;
import org.neo4j.arrow.batch.ArrowBatches;
import org.neo4j.arrow.Config;
import org.neo4j.arrow.action.GdsMessage;
import org.neo4j.arrow.action.GdsWriteNodeMessage;
import org.neo4j.arrow.action.GdsWriteRelsMessage;
import org.neo4j.arrow.action.Message;
import org.neo4j.arrow.batch.BatchedVector;
import org.neo4j.arrow.gds.ArrowAdjacencyList;
import org.neo4j.arrow.gds.ArrowGraphStore;
import org.neo4j.arrow.gds.ArrowNodeProperties;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.*;
import org.neo4j.gds.api.schema.NodeSchema;
import org.neo4j.gds.api.schema.PropertySchema;
import org.neo4j.gds.config.GraphCreateConfig;
import org.neo4j.gds.core.huge.HugeGraph;
import org.neo4j.gds.core.loading.CSRGraphStoreUtil;
import org.neo4j.gds.core.loading.CatalogRequest;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.loading.GraphStoreWithConfig;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.utils.collection.primitive.PrimitiveLongIterable;
import org.neo4j.gds.core.utils.collection.primitive.PrimitiveLongIterator;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.LifeSupport;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.LongPredicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;


public class GdsWriteJob extends WriteJob {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(GdsWriteJob.class);

    private final Consumer<ArrowBatch> batchConsumer;
    private final CompletableFuture<Boolean> future;
    private final DatabaseManagementService dbms;

    /**
     * Create a new GdsWriteJob for processing the given {@link GdsMessage}, creating or writing to
     * an in-memory GDS graph.
     * <p>
     * The supplied username is assumed to allow GDS to enforce authorization and is assumed to be
     * previously authenticated.
     *
     * @param msg      the {@link GdsMessage} to process in the job
     * @param username an already authenticated username
     * @param dbms reference to a {@link DatabaseManagementService}
     */
    public GdsWriteJob(Message msg, // XXX need to abstract here?
                       String username, BufferAllocator allocator,
                       DatabaseManagementService dbms) throws RuntimeException {
        super(allocator);
        this.dbms = dbms;

        logger.info("GdsWriteJob called with msg: {}", msg);

        final BlockingQueue<ArrowBatch> incoming = new LinkedBlockingQueue<>();
        batchConsumer = incoming::add;

        setStatus(Status.PENDING);

        future = getSchema()
                .thenApplyAsync(schema -> {
                    setStatus(Status.PRODUCING);
                    return new ArrowBatches(schema, allocator, "gds-write-" + getJobId());
                })
                .thenApplyAsync(batches -> {
                    // XXX for now we consume the entire stream :-(
                    final Future<Void> streamComplete = getStreamCompletion();
                    while (!streamComplete.isDone() && !streamComplete.isCancelled()) {
                        try {
                            final ArrowBatch batch = incoming.poll(5, TimeUnit.MILLISECONDS);
                            if (batch != null) {
                                batches.appendBatch(batch);
                            }
                        } catch (InterruptedException e) {
                            throw new RuntimeException("Interrupted while building ArrowBatches", e);
                        } catch (RuntimeException e) {
                            throw new RuntimeException("Failed to build ArrowBatches", e);
                        }
                    }
                    return batches;
                })
                .thenApplyAsync(batches -> {
                    if (msg instanceof GdsWriteNodeMessage) {
                        return handleNodeJob((GdsWriteNodeMessage) msg, username, batches);
                    } else if (msg instanceof GdsWriteRelsMessage) {
                        return handleRelationshipsJob((GdsWriteRelsMessage) msg, username, batches);
                    } else {
                        throw CallStatus.UNIMPLEMENTED.withDescription("unhandled request type").toRuntimeException();
                    }
                }).handleAsync((aBoolean, throwable) -> {
                    final String result = (aBoolean == null ? "failed" : "ok!");
                    logger.info("GdsWriteJob completed! result: {}", result);
                    AutoCloseables.closeNoChecked(this);

                    if (throwable != null) {
                        logger.error(throwable.getMessage(), throwable);
                        return false;
                    } else {
                        onCompletion(() -> result);
                        return aBoolean;
                    }
                });
    }

    protected boolean handleNodeJob(GdsWriteNodeMessage msg, String username, ArrowBatches arrowBatches) {
        final GraphDatabaseAPI api = (GraphDatabaseAPI) dbms.database(msg.getDbName());
        final NamedDatabaseId dbId = api.databaseId();

        logger.info("configuring job for {}", msg);

        // XXX push schema validation to Producer side prior to full stream being formed
        final Schema schema = arrowBatches.getSchema();
        final long rowCount = arrowBatches.getRowCount();

        // XXX this assumes we only load up to ((1 << 31) - 1) (~2.1B) node ids
        // these will throw IllegalArg exceptions
        schema.findField(msg.getIdField());
        //final BigIntVector nodeIdVector = (BigIntVector) arrowBatch.getVector(msg.getIdField());
        final BatchedVector nodeIdVector = arrowBatches.getVector(msg.getIdField());

        schema.findField(msg.getLabelsField());
        //final ListVector labelsVector = (ListVector) arrowBatch.getVector(msg.getLabelsField());
        final BatchedVector labelsVector = arrowBatches.getVector(msg.getLabelsField());

        // This is ugly
        final Map<NodeLabel, Map<String, ArrowNodeProperties>> labelToPropMap = new ConcurrentHashMap<>();
        final Map<NodeLabel, Map<String, PropertySchema>> labelToPropSchemaMap = new ConcurrentHashMap<>();
        final Map<String, NodeProperties> globalPropMap = new ConcurrentHashMap<>();
        final Map<Long, Integer> idMap = new ConcurrentHashMap<>();
        final AtomicLong globalMaxId = new AtomicLong(0);

        // Temporary optimization
        final List<BatchedVector> propertyVectors = arrowBatches.getFieldVectors().stream()
                .filter(vec -> !Objects.equals(vec.getName(), msg.getIdField())
                        && !Objects.equals(vec.getName(), msg.getLabelsField()))
                .collect(Collectors.toList());

        // Brute force. Terrible.
        logger.info(String.format("preprocessing %,d nodes to build label->propMap mapping", arrowBatches.getRowCount()));
        final AtomicInteger cnt = new AtomicInteger(0);
        IntStream.range(0, arrowBatches.getRowCount()).parallel().forEach(idx -> {
            int progress = cnt.incrementAndGet();
            if (progress % 1_000_000 == 0) {
                logger.info(String.format("...%,d", progress));
            }

            final long originalNodeId = nodeIdVector.getNodeId(idx);
            if (idMap.put(originalNodeId, idx) != null) {
                logger.error("key collision! (nodeId: {}, idx: {})", originalNodeId, idx);
            }

            final List<String> labels = labelsVector.getLabels(idx);
            labels.forEach(label -> {
                final NodeLabel nodeLabel = NodeLabel.of(label);
                final Map<String, ArrowNodeProperties> propMap =
                        labelToPropMap.getOrDefault(nodeLabel, new ConcurrentHashMap<>());

                // TODO: clean up
                propertyVectors.forEach(vec -> {
                    final ArrowNodeProperties props = new ArrowNodeProperties(vec, nodeLabel, (int) rowCount); // XXX cast
                    propMap.putIfAbsent(vec.getName(), props);
                    globalPropMap.putIfAbsent(vec.getName(), props);
                });
                labelToPropMap.put(nodeLabel, propMap); // ??? is this needed?
            });
            globalMaxId.updateAndGet(i -> Math.max(originalNodeId, i));
        });

        // groan
        labelToPropMap.forEach((label, propMap) ->
                propMap.forEach((str, props) ->
                        labelToPropSchemaMap.getOrDefault(label, new ConcurrentHashMap<>())
                                .putIfAbsent(str, PropertySchema.of(str, propMap.get(str).valueType()))));

        final NodeSchema nodeSchema = NodeSchema.of(labelToPropSchemaMap);

        // We need our own style of NodeMapping do deal with the fact we manage the node ids
        final NodeMapping nodeMapping = new NodeMapping() {
            @Override
            public NodeMapping withFilteredLabels(Collection<NodeLabel> nodeLabels, int concurrency) {
                // TODO: add in filtered label support
                throw new UnsupportedOperationException("sorry...I haven't implemented filtered label support yet! -dv");
            }

            @Override
            public Set<NodeLabel> nodeLabels(long nodeId) {
                return labelsVector.getLabels(nodeId).stream()
                        .map(Object::toString)
                        .map(NodeLabel::of)
                        .collect(Collectors.toUnmodifiableSet());
            }

            @Override
            public void forEachNodeLabel(long nodeId, NodeLabelConsumer consumer) {
                nodeLabels(nodeId).forEach(consumer::accept);
            }

            @Override
            public Set<NodeLabel> availableNodeLabels() {
                return labelToPropMap.keySet();
            }

            @Override
            public boolean hasLabel(long nodeId, NodeLabel label) {
                return nodeLabels(nodeId).contains(label);
            }

            @Override
            public NodeMapping rootNodeMapping() {
                return this;
            }

            @Override
            public Collection<PrimitiveLongIterable> batchIterables(long batchSize) {
                logger.info("generating iterable batches of size {}", batchSize);
                final List<PrimitiveLongIterable> iterables = new ArrayList<>();
                for (long l = 0; l < rowCount; l += batchSize) {
                    long start = l;
                    long finish = Math.min(l + batchSize, rowCount);
                    iterables.add(() -> new PrimitiveLongIterator() {
                        private final Iterator<Long> iterator = LongStream.range(start, finish).iterator();
                        @Override
                        public boolean hasNext() {
                            return iterator.hasNext();
                        }

                        @Override
                        public long next() {
                            return iterator.next();
                        }
                    });
                }
                return iterables;
            }

            @Override
            public long toMappedNodeId(long nodeId) {
                if (nodeId < 0) {
                    throw new IllegalArgumentException("nodeId < 0?!?");
                }
                final Integer mappedId = idMap.get(nodeId);
                if (mappedId == null) {
                    logger.warn("attempted to translate original id {}, but have no mapping!", nodeId);
                    logger.warn("current idMap: {}", idMap);
                    throw new RuntimeException("mapping is corrupt!");
                    // return NOT_FOUND;
                }
                return mappedId.longValue();
            }

            @Override
            public long safeToMappedNodeId(long nodeId) {
               try {
                   return toMappedNodeId(nodeId);
               } catch (Exception e) {
                   return NodeMapping.NOT_FOUND;
               }
            }

            @Override
            public long toOriginalNodeId(long nodeId) {
                if (nodeId < 0) {
                    throw new IllegalArgumentException("nodeId < 0?!?");
                }
                return nodeIdVector.getNodeId(nodeId);
            }

            @Override
            public long toRootNodeId(long nodeId) {
                if (nodeId < 0) {
                    throw new IllegalArgumentException("nodeId < 0?!?");
                }
                return nodeId;
            }

            @Override
            public boolean contains(long nodeId) {
                if (nodeId < 0) {
                    throw new IllegalArgumentException("nodeId < 0?!?");
                }
                return (nodeId < rowCount);
            }

            @Override
            public long nodeCount() {
                return rowCount;
            }

            @Override
            public long rootNodeCount() {
                return rowCount;
            }

            @Override
            public long highestNeoId() {
                // UNSUPPORTED
                logger.info("someone wants the highest neo id?");
                return globalMaxId.get();
            }

            @Override
            public void forEachNode(LongPredicate consumer) {
                logger.info("walking nodes (0 -> {}) via LongPredicate...", rowCount);
                LongStream.range(0, rowCount).forEach(consumer::test);
            }

            @Override
            public PrimitiveLongIterator nodeIterator() {
                logger.info("someone wants a nodeIterator!");
                Thread.dumpStack();
                return new PrimitiveLongIterator() {
                    final private Iterator<Long> iterator = LongStream.range(0, rowCount).iterator();
                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    @Override
                    public long next() {
                        return iterator.next();
                    }
                };
            }

            @Override
            public IdMapping cloneIdMapping() {
                // TODO: implement cloning for id maps
                return NodeMapping.super.cloneIdMapping();
            }
        };

        final HugeGraph hugeGraph = GraphFactory.create(nodeMapping, nodeSchema, globalPropMap,
                RelationshipType.of("__empty__"), Relationships.of(0, Orientation.NATURAL, true,
                        new AdjacencyList() {
                            // See GraphStoreFilterTest.java for inspiration of how to stub out
                            @Override
                            public int degree(long node) {
                                return 0;
                            }

                            @Override
                            public AdjacencyCursor adjacencyCursor(long node, double fallbackValue) {
                                return AdjacencyCursor.EmptyAdjacencyCursor.INSTANCE;
                            }

                            @Override
                            public AdjacencyCursor rawAdjacencyCursor() {
                                return AdjacencyCursor.EmptyAdjacencyCursor.INSTANCE;
                            }

                            @Override
                            public void close() {
                                // XXX hack
                                logger.info("fauxAdjacencyList closing");
                            }
                        }), AllocationTracker.empty());

        final GraphStore store = CSRGraphStoreUtil.createFromGraph(
                dbId, hugeGraph, "__empty__", Optional.empty(),
                Config.arrowMaxPartitions, AllocationTracker.create());

        // Try wiring in our arbitrary node properties.
        labelToPropMap.forEach((label, propMap) ->
                propMap.forEach((name, props) -> {
                    logger.info("mapping label {} to property {}", label, name);
                    store.addNodeProperty(label, name, props);
                }));

        final GraphCreateConfig config = new GraphCreateConfig() {
            @Override
            public String graphName() {
                return msg.getGraphName();
            }

            @Override
            public GraphStoreFactory.Supplier graphStoreFactory() {
                throw new RuntimeException("oops: graphStoreFactory() called");
            }

            @Override
            public <R> R accept(Cases<R> visitor) {
                // TODO: what the heck is this Cases<R> stuff?!
                logger.info("accept called with visitor: {}", visitor);
                return null;
            }

            @Override
            public int readConcurrency() {
                return GraphCreateConfig.super.readConcurrency();
            }

            @Override
            public long nodeCount() {
                return rowCount;
            }

            @Override
            public long relationshipCount() {
                return 0;
            }

            @Override
            public boolean validateRelationships() {
                return false;
            }

            @Override
            public String username() {
                return username;
            }
        };

        GraphStoreCatalog.set(config, ArrowGraphStore.wrap(store, () -> {
            nodeIdVector.close();
            labelsVector.close();
            arrowBatches.close();
            logger.info("closed nodeIdVector, labelsVector, and arrowBatch");
        }));

        // nuke our garbage rel?
        store.deleteRelationships(RelationshipType.of("__empty__"));

        logger.info("node job complete. nodes = {}, mapping size = {}", store.nodeCount(), idMap.keySet().size());
        logger.info(String.format("processed ArrowBatch of size %,d MiB", (arrowBatches.estimateSize() >> 20)));
        // logger.info("batch output: {}", arrowBatch.getAllocatorOverview());
        return true;
    }

    protected boolean handleRelationshipsJob(GdsWriteRelsMessage msg, String username, ArrowBatches arrowBatches) {
        // Assumes a prior node write job created the graph.
        logger.info("configuring job for {}", msg);

        final GraphDatabaseAPI api = (GraphDatabaseAPI) dbms.database(msg.getDbName());
        final NamedDatabaseId dbId = api.databaseId();

        final GraphStoreWithConfig storeWithConfig = GraphStoreCatalog.get(CatalogRequest.of(username, dbId), msg.getGraphName());
        assert(storeWithConfig != null);

        final BatchedVector sourceIdVector = arrowBatches.getVector(msg.getSourceField());
        assert(sourceIdVector != null);
        final BatchedVector targetIdVector = arrowBatches.getVector(msg.getTargetField());
        assert(targetIdVector != null);
        final BatchedVector typesVector = arrowBatches.getVector(msg.getTypeField());
        assert(typesVector != null);

        // Ugliness abounds...
        // Edge maps, keyed by Type. Each Type has it's own mapping of GDS Node Id to a Queue of target GDS node Ids
        // { type: { nodeVectorId: [ offsets into node vector ] } }
        final Map<String, Map<Integer, List<Integer>>> sourceTypeIdMap = new ConcurrentHashMap<>();
        final Map<String, Integer> types = new ConcurrentHashMap<>();
        final Map<String, Map<Integer, Integer>> inDegreeMap = new ConcurrentHashMap<>();

        // Use our nodeMapping to deal with the fact we get "original" ids in the target vector and don't know the
        // "internal" GDS ids from our node vectors
        final NodeMapping nodeMapping = storeWithConfig.graphStore().nodes();

        logger.info(String.format("analyzing %,d relationships to build type->rel mappings", arrowBatches.getRowCount()));
        final AtomicInteger cnt = new AtomicInteger(0);
        IntStream.range(0, arrowBatches.getRowCount())
                .parallel() // XXX need to check if we solved the concurrency bug
                .forEach(idx -> {
                    int progress = cnt.incrementAndGet();
                    if (progress % 1_000_000 == 0) {
                        logger.info(String.format("...%,d", progress));
                    }
                    final long originalSourceId = sourceIdVector.getNodeId(idx);
                    final int sourceId = (int) nodeMapping.toMappedNodeId(originalSourceId); // XXX cast
                    final long originalTargetId = targetIdVector.getNodeId(idx);
                    final int targetId = (int) nodeMapping.toMappedNodeId(originalTargetId); // XXX cast
                    final String type = typesVector.getType(idx);

                    logger.trace("recording idx {}: ({} @ {})-[{}]->({} @ {})", idx, originalSourceId, sourceId, type, originalTargetId, targetId);

                    types.compute(type, (key, n) -> (n == null) ? 1 : n + 1);

                    sourceTypeIdMap.compute(type, (k, v) -> {
                        final Map<Integer, List<Integer>> sourceIdMap = (v == null) ? new ConcurrentHashMap<>() : v;
                        sourceIdMap.compute(sourceId, (k2, v2) -> {
                            final List<Integer> targetList = (v2 == null) ? new Vector<>() : v2;
                                targetList.add(targetId);
                            return targetList;
                        });
                        return sourceIdMap;
                    });

                    inDegreeMap.compute(type, (k, v) -> {
                        final Map<Integer, Integer> outMap = (v == null) ? new ConcurrentHashMap<>() : v;
                        outMap.compute(sourceId, (k2, v2) -> (v2 == null) ? 1 : v2 + 1);
                        return outMap;
                    });
                });

        logger.info("reltypes counts: {}", types);

        // TODO: relationship properties!
        // We don't really use Arrow buffers for the adjacency data (yet?)
        sourceIdVector.close();
        targetIdVector.close();
        typesVector.close();

        // TODO: XXX when we implement rel props, do NOT close this here!
        arrowBatches.close();

        // Sort our lists
        sourceTypeIdMap.forEach((type, map) -> {
            logger.info("sorting adjacency lists for type {}...", type);
            map.keySet().parallelStream()
                    .forEach(key -> {
                        final List<Integer> list = map.get(key);
                        map.put(key, list.stream().sorted().collect(Collectors.toList()));
                    });
            logger.info("done sorting type {}", type);
        });

        types.forEach((type, num) -> {
            // TODO: wire in maps
            final Relationships rels = Relationships.of(num, Orientation.NATURAL, true,
                    new ArrowAdjacencyList(sourceTypeIdMap.get(type), inDegreeMap.get(type), (unused) -> arrowBatches.close()));
            logger.info("adding relationship type {} to graph {} (type edge size: {})", type,
                    storeWithConfig.config().graphName(), sourceTypeIdMap.get(type).size());
            storeWithConfig.graphStore()
                    .addRelationshipType(RelationshipType.of(type), Optional.empty(), Optional.empty(), rels);
        });

        logger.info("finished relationship write, graph has the following relationships: {}",
                storeWithConfig.graphStore().relationshipTypes());
        return true;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        final boolean cancelled = future.cancel(mayInterruptIfRunning);
        if (cancelled) {
            AutoCloseables.closeNoChecked(this);
        }
        return cancelled;
    }

    @Override
    public Consumer<ArrowBatch> getConsumer() {
        return batchConsumer;
    }
}
