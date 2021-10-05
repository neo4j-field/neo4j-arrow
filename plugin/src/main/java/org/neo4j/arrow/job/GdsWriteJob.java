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
import org.neo4j.gds.core.loading.construction.NodesBuilder;
import org.neo4j.gds.core.loading.construction.NodesBuilderBuilder;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


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
            final AtomicLong maxId = new AtomicLong(0);

            // Brute force. Terrible.
            logger.info("analyzing vectors to build label->propMap mapping");
            IntStream.range(0, root.getRowCount())
                    .parallel()
                    .boxed()
                    .forEach(idx -> {
                        final long nodeId = nodeIdVector.get(idx);
                        final List<?> labels = labelsVector.getObject(idx);
                        labels.stream().map(Object::toString).forEach(label -> {
                            final NodeLabel nodeLabel = NodeLabel.of(label);
                            final Map<String, ArrowNodeProperties> propMap =
                                    labelToPropMap.getOrDefault(nodeLabel, new ConcurrentHashMap<>());
                            root.getFieldVectors().stream()
                                    .filter(vec -> !Objects.equals(vec.getName(), msg.getIdField())
                                            && !Objects.equals(vec.getName(), msg.getLabelsField()))
                                    .forEach(vec -> {
                                        final ArrowNodeProperties props = new ArrowNodeProperties(vec, nodeLabel, idMap);
                                        propMap.putIfAbsent(vec.getName(), props);
                                        globalPropMap.putIfAbsent(vec.getName(), props);
                                    });
                            labelToPropMap.put(nodeLabel, propMap); // ??? is this needed?
                        });
                        idMap.put(nodeId, idx);
                        maxId.updateAndGet(i -> Math.max(nodeId, i));
                    });
            logger.info("labelToPropMap: {}", labelToPropMap);

            // groan
            labelToPropMap.forEach((label, propMap) ->
                    propMap.forEach((str, props) ->
                            labelToPropSchemaMap.getOrDefault(label, new ConcurrentHashMap<>())
                                    .putIfAbsent(str, PropertySchema.of(str, propMap.get(str).valueType()))));

            final NodeSchema nodeSchema = NodeSchema.of(labelToPropSchemaMap);

            final NodesBuilder builder = (new NodesBuilderBuilder())
                    .concurrency(Config.arrowMaxPartitions)
                    .hasLabelInformation(true)
                    .hasProperties(true)
                    .allocationTracker(AllocationTracker.empty())
                    .maxOriginalId(maxId.get())
                    .nodeCount(rowCount)
                    .build();

            assert(root.getRowCount() == nodeIdVector.getValueCount());

            // Sadly need to re-run through this :-(, thanks max original id!
            IntStream.range(0, nodeIdVector.getValueCount())
                    .parallel()
                    .forEach(idx -> {
                        // XXX don't recompute?
                        final String[] labels = labelsVector.getObject(idx).stream()
                                .map(Object::toString).collect(Collectors.toList()).toArray(String[]::new);
                        final NodeLabel[] nodeLabels = NodeLabel.listOf(labels).toArray(NodeLabel[]::new);
                        builder.addNode(nodeIdVector.get(idx), nodeLabels);
                    });

            final HugeGraph hugeGraph = GraphFactory.create(builder.build().nodeMapping(), nodeSchema, globalPropMap,
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

            // XXX push schema validation to Producer side prior to full stream being formed
            final Schema schema = root.getSchema();
            final long rowCount = root.getRowCount();

            final GraphStoreWithConfig storeWithConfig = GraphStoreCatalog.get(CatalogRequest.of(username, dbId), msg.getGraphName());
            assert(storeWithConfig != null);

            final BigIntVector sourceIdVector = (BigIntVector) root.getVector(msg.getSourceField());
            final BigIntVector targetIdVector = (BigIntVector) root.getVector(msg.getTargetField());
            final VarCharVector typesVector = (VarCharVector) root.getVector(msg.getTypeField());

            // Ugliness abounds
            final Map<Long, List<Integer>> sourceIdMap = new ConcurrentHashMap<>();
            final Map<Long, List<Integer>> targetIdMap = new ConcurrentHashMap<>();
            final Map<String, Long> types = new ConcurrentHashMap<>();

            logger.info("analyzing vectors to build type->rel mappings");
            IntStream.range(0, root.getRowCount())
                    .parallel()
                    .boxed()
                    .forEach(idx -> {
                        final long sourceId = sourceIdVector.get(idx);
                        final long targetId = targetIdVector.get(idx);
                        final String type = new String(typesVector.get(idx), StandardCharsets.UTF_8);

                        final List<Integer> targetList = sourceIdMap.getOrDefault(sourceId, new ArrayList<>());
                        targetList.add(idx);
                        sourceIdMap.put(sourceId, targetList);

                        final List<Integer> sourceList = targetIdMap.getOrDefault(targetId, new ArrayList<>());
                        sourceList.add(idx);
                        targetIdMap.put(targetId, sourceList);

                        types.put(type, types.getOrDefault(type, 0L) + 1);
                    });

            // TODO: properties

            types.forEach((type, cnt) -> {
                // TODO: wire in maps
                final Relationships rels = Relationships.of(cnt, Orientation.NATURAL, false, new AdjacencyList() {
                    @Override
                    public int degree(long node) {
                        return 0;
                    }

                    @Override
                    public AdjacencyCursor adjacencyCursor(long node, double fallbackValue) {
                        return new AdjacencyCursor() {
                            @Override
                            public void init(long index, int degree) {

                            }

                            @Override
                            public int size() {
                                return 0;
                            }

                            @Override
                            public boolean hasNextVLong() {
                                return false;
                            }

                            @Override
                            public long nextVLong() {
                                return 0;
                            }

                            @Override
                            public long peekVLong() {
                                return 0;
                            }

                            @Override
                            public int remaining() {
                                return 0;
                            }

                            @Override
                            public long skipUntil(long nodeId) {
                                return 0;
                            }

                            @Override
                            public long advance(long nodeId) {
                                return 0;
                            }

                            @Override
                            public AdjacencyCursor shallowCopy(AdjacencyCursor destination) {
                                return AdjacencyCursor.empty();
                            }

                            @Override
                            public void close() {

                            }
                        };
                    }

                    @Override
                    public AdjacencyCursor rawAdjacencyCursor() {
                        return AdjacencyCursor.empty();
                    }

                    @Override
                    public void close() {
                        root.close();
                    }
                });

                logger.info("adding relationship type {} to graph {}", type, storeWithConfig.config().graphName());
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
