package org.neo4j.arrow.job;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.Schema;
import org.neo4j.arrow.Config;
import org.neo4j.arrow.action.GdsMessage;
import org.neo4j.arrow.action.GdsWriteNodeMessage;
import org.neo4j.arrow.action.GdsWriteRelsMessage;
import org.neo4j.arrow.action.Message;
import org.neo4j.arrow.gds.ArrowAdjacencyList;
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

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongPredicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;


public class GdsWriteJob extends WriteJob {
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
                       String username, DatabaseManagementService dbms) throws RuntimeException {
        super();
        this.dbms = dbms;

        final CompletableFuture<Boolean> job;
        logger.info("GdsWriteJob called with msg: {}", msg);

        if (msg instanceof GdsWriteNodeMessage) {
            job = handleNodeJob((GdsWriteNodeMessage) msg, username);
        } else if (msg instanceof GdsWriteRelsMessage) {
            job = handleRelationshipsJob((GdsWriteRelsMessage) msg, username);
        } else {
            throw CallStatus.UNIMPLEMENTED.withDescription("unhandled request type").toRuntimeException();
        }

        future = job.exceptionally(throwable -> {
            logger.error(throwable.getMessage(), throwable);
            return false;
        }).handleAsync((aBoolean, throwable) -> {
            logger.info("GdsWriteJob completed! result: {}", (aBoolean == null ? "failed" : "ok!"));
            if (throwable != null)
                logger.error(throwable.getMessage(), throwable);
            return false;
        });
    }

    protected CompletableFuture<Boolean> handleNodeJob(GdsWriteNodeMessage msg, String username) {
        final GraphDatabaseAPI api = (GraphDatabaseAPI) dbms.database(msg.getDbName());
        final NamedDatabaseId dbId = api.databaseId();

        logger.info("configuring job for {}", msg);

        return CompletableFuture.supplyAsync(() -> {
            // XXX we assume we're creating a graph (for now), not updating
            final VectorSchemaRoot root;
            try {
                root = getStreamCompletion().get(10, TimeUnit.MINUTES);    // XXX
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                e.printStackTrace();
                return false;
            }

            // XXX push schema validation to Producer side prior to full stream being formed
            final Schema schema = root.getSchema();
            final long rowCount = root.getRowCount();

            // XXX this assumes we only load up to ((1 << 31) - 1) (~2.1B) node ids
            // these will throw IllegalArg exceptions
            schema.findField(msg.getIdField());
            final BigIntVector nodeIdVector = (BigIntVector) root.getVector(msg.getIdField());

            schema.findField(msg.getLabelsField());
            final ListVector labelsVector = (ListVector) root.getVector(msg.getLabelsField());

            // This is ugly
            final Map<NodeLabel, Map<String, ArrowNodeProperties>> labelToPropMap = new ConcurrentHashMap<>();
            final Map<NodeLabel, Map<String, PropertySchema>> labelToPropSchemaMap = new ConcurrentHashMap<>();
            final Map<String, NodeProperties> globalPropMap = new ConcurrentHashMap<>();
            final Map<Long, Integer> idMap = new ConcurrentHashMap<>();
            final AtomicLong globalMaxId = new AtomicLong(0);

            // Brute force. Terrible.
            logger.info("analyzing vectors to build label->propMap mapping");
            IntStream.range(0, root.getRowCount())
                    //.parallel()
                    .forEach(idx -> {
                        final long originalNodeId = nodeIdVector.get(idx);
                        final List<?> labels = labelsVector.getObject(idx);

                        labels.stream().map(Object::toString).forEach(label -> {
                            final NodeLabel nodeLabel = NodeLabel.of(label);
                            final Map<String, ArrowNodeProperties> propMap =
                                    labelToPropMap.getOrDefault(nodeLabel, new ConcurrentHashMap<>());
                            root.getFieldVectors().stream()
                                    .filter(vec -> !Objects.equals(vec.getName(), msg.getIdField())
                                            && !Objects.equals(vec.getName(), msg.getLabelsField()))
                                    .forEach(vec -> {
                                        final ArrowNodeProperties props = new ArrowNodeProperties(vec, nodeLabel, (int) rowCount); // XXX cast
                                        propMap.putIfAbsent(vec.getName(), props);
                                        globalPropMap.putIfAbsent(vec.getName(), props);
                                    });
                            labelToPropMap.put(nodeLabel, propMap); // ??? is this needed?
                        });

                        idMap.put(originalNodeId, idx);
                        globalMaxId.updateAndGet(i -> Math.max(originalNodeId, i));
                    });
            // logger.info("labelToPropMap: {}", labelToPropMap);

            // groan
            labelToPropMap.forEach((label, propMap) ->
                    propMap.forEach((str, props) ->
                            labelToPropSchemaMap.getOrDefault(label, new ConcurrentHashMap<>())
                                    .putIfAbsent(str, PropertySchema.of(str, propMap.get(str).valueType()))));

            final NodeSchema nodeSchema = NodeSchema.of(labelToPropSchemaMap);
            assert(root.getRowCount() == nodeIdVector.getValueCount());

            // We need our own style of NodeMapping do deal with the fact we manage the node ids
            final NodeMapping nodeMapping = new NodeMapping() {

                @Override
                public Set<NodeLabel> nodeLabels(long nodeId) {
                    if (labelsVector.getObject((int) nodeId) == null)
                        return Set.of();

                    return labelsVector.getObject((int) nodeId).stream()
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
                public Collection<PrimitiveLongIterable> batchIterables(long batchSize) {
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
                    return idMap.get(nodeId);
                }

                @Override
                public long toOriginalNodeId(long nodeId) {
                    return nodeIdVector.get((int) nodeId);
                }

                @Override
                public long toRootNodeId(long nodeId) {
                    return nodeId;
                }

                @Override
                public boolean contains(long nodeId) {
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
                    return globalMaxId.get();
                }

                @Override
                public void forEachNode(LongPredicate consumer) {
                    LongStream.range(0, rowCount).forEach(consumer::test);
                }

                @Override
                public PrimitiveLongIterator nodeIterator() {
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
            };

            final HugeGraph hugeGraph = GraphFactory.create(nodeMapping, nodeSchema, globalPropMap,
                    RelationshipType.ALL_RELATIONSHIPS, Relationships.of(0, Orientation.NATURAL, true,
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
                                    root.close();
                                }
                            }), AllocationTracker.empty());

            final GraphStore store = CSRGraphStoreUtil.createFromGraph(
                    dbId, hugeGraph, "REL", Optional.empty(),
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

            GraphStoreCatalog.set(config, store);

            return true;
        });
    }

    protected CompletableFuture<Boolean> handleRelationshipsJob(GdsWriteRelsMessage msg, String username) {
        // Assumes a prior node write job created the graph.
        logger.info("configuring job for {}", msg);

        final GraphDatabaseAPI api = (GraphDatabaseAPI) dbms.database(msg.getDbName());
        final NamedDatabaseId dbId = api.databaseId();

        return CompletableFuture.supplyAsync(() -> {
            final VectorSchemaRoot root;
            try {
                root = getStreamCompletion().get(10, TimeUnit.MINUTES);    // XXX
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                e.printStackTrace();
                return false;
            }

            final GraphStoreWithConfig storeWithConfig = GraphStoreCatalog.get(CatalogRequest.of(username, dbId), msg.getGraphName());
            assert(storeWithConfig != null);

            final BigIntVector sourceIdVector = (BigIntVector) root.getVector(msg.getSourceField());
            assert(sourceIdVector != null);
            final BigIntVector targetIdVector = (BigIntVector) root.getVector(msg.getTargetField());
            assert(targetIdVector != null);
            final VarCharVector typesVector = (VarCharVector) root.getVector(msg.getTypeField());
            assert(typesVector != null);

            // Ugliness abounds...
            // Edge maps, keyed by Type. Each Type has it's own mapping of GDS Node Id to a Queue of target GDS node Ids
            // { type: { nodeVectorId: [ offsets into node vector ] } }
            final Map<String, Map<Integer, Queue<Integer>>> sourceTypeIdMap = new ConcurrentHashMap<>();
            final Map<String, Integer> types = new ConcurrentHashMap<>();

            // Use our nodeMapping to deal with the fact we get "original" ids in the target vector and don't know the
            // "internal" GDS ids from our node vectors
            final NodeMapping nodeMapping = storeWithConfig.graphStore().nodes();

            logger.info("analyzing vectors to build type->rel mappings");
            IntStream.range(0, root.getRowCount())
                    //.parallel() // XXX need to check if we solved the concurrency bug
                    .forEach(idx -> {
                        final long originalSourceId = sourceIdVector.get(idx);
                        final int sourceId = (int) nodeMapping.toMappedNodeId(originalSourceId); // XXX cast
                        final long originalTargetId = targetIdVector.get(idx);
                        final String type = new String(typesVector.get(idx), StandardCharsets.UTF_8);

                        logger.trace("recording {} -[{}]> {}", originalSourceId, type, originalTargetId);

                        types.compute(type, (key, cnt) -> (cnt == null) ? 1 : cnt + 1);

                        sourceTypeIdMap.compute(type, (k, v) -> {
                            final Map<Integer, Queue<Integer>> sourceIdMap = (v == null) ? new ConcurrentHashMap<>() : v;
                            sourceIdMap.compute(sourceId, (k2, v2) -> {
                                final Queue<Integer> targetList = (v2 == null) ? new ConcurrentLinkedQueue<>() : v2;
                                targetList.add((int) nodeMapping.toMappedNodeId(originalTargetId)); // XXX cast
                                return targetList;
                            });
                            return sourceIdMap;
                        });
                    });

            logger.info("types counts: {}", types);

            // TODO: properties

            types.forEach((type, cnt) -> {
                // TODO: wire in maps
                final Relationships rels = Relationships.of(cnt, Orientation.NATURAL, true,
                        new ArrowAdjacencyList(sourceTypeIdMap.get(type), (unused) -> root.close()));
                logger.info("adding relationship type {} to graph {} (type edge size: {})", type, storeWithConfig.config().graphName(),
                        sourceTypeIdMap.get(type).size());
                storeWithConfig.graphStore().addRelationshipType(RelationshipType.of(type), Optional.empty(), Optional.empty(), rels);
            });

            return true;
        });
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return future.cancel(mayInterruptIfRunning);
    }

    @Override
    public void close() {
        future.cancel(true);
    }

    @Override
    public void onError(Exception e) {
        logger.info("failure", e);
        future.completeExceptionally(e);
    }

}
