package org.neo4j.arrow.job;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.types.pojo.Schema;
import org.neo4j.arrow.action.BulkImportMessage;
import org.neo4j.arrow.batch.ArrowBatch;
import org.neo4j.arrow.batchimport.NodeInputIterator;
import org.neo4j.arrow.batchimport.QueueInputIterator;
import org.neo4j.arrow.batchimport.RelationshipInputIterator;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseExistsException;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.core.Settings;
import org.neo4j.internal.batchimport.AdditionalInitialIds;
import org.neo4j.internal.batchimport.BatchImporterFactory;
import org.neo4j.internal.batchimport.ImportLogic;
import org.neo4j.internal.batchimport.InputIterable;
import org.neo4j.internal.batchimport.input.*;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleException;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.scheduler.JobScheduler;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

public class BulkImportJob extends WriteJob {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BulkImportJob.class);

    private static final String META_KEY = "stream.type";
    private static final String NODE_STREAM = "node";
    private static final String RELS_STREAM = "rels";

    private final CompletableFuture<Schema> nodeSchema = new CompletableFuture<>();
    private final CompletableFuture<Schema> relsSchema = new CompletableFuture<>();

    private final CompletableFuture<Boolean> future;

    private final String username;
    private final String dbName;

    private final Path homePath;
    private final FileSystemAbstraction fs;
    private final BulkInput bulkInput;

    @Override
    public void onStreamComplete() {
        super.onStreamComplete();
        bulkInput.signalCompletion();
    }

    @Override
    public void onSchema(Schema schema) {
        // We expect TWO streams, so we need to differentiate Schema based on metadata
        final Map<String, String> metadata = schema.getCustomMetadata();
        assert (metadata != null); // XXX pretty sure this is guaranteed, but not sure

        switch (metadata.getOrDefault(META_KEY, "")) {
            case NODE_STREAM:
                nodeSchema.complete(schema);
                break;
            case RELS_STREAM:
                relsSchema.complete(schema);
                break;
            default:
                logger.warn("unexpected or missing schema metadata type field ({})", META_KEY);
        }
    }

    public BulkImportJob(BulkImportMessage msg, String username, BufferAllocator allocator,
                         DatabaseManagementService dbms) {
        super(allocator);

        this.username = username;
        this.dbName = msg.getDbName();
        logger.info("initializing {}", this);

        // We need a reference to a GraphDatabaseAPI. Any reference will do.
        final GraphDatabaseAPI api = (GraphDatabaseAPI) dbms.database(GraphDatabaseSettings.SYSTEM_DATABASE_NAME);
        this.homePath = api.databaseLayout().getNeo4jLayout().homeDirectory();
        this.fs = api.getDependencyResolver().resolveDependency(FileSystemAbstraction.class);
        this.bulkInput = new BulkInput(
                msg.getIdField(), msg.getLabelsField(),
                msg.getSourceField(), msg.getTargetField(), msg.getTypeField());

        final Config config = Config.defaults(Settings.neo4jHome(), homePath);
        final DatabaseLayout dbLayout = Neo4jLayout.of(config).databaseLayout(dbName);

        // TODO: figure out the LogService api
        final LogService logService = new SimpleLogService(NullLogProvider.getInstance());
        final LifeSupport lifeSupport = new LifeSupport();
        final JobScheduler jobScheduler = lifeSupport.add(JobSchedulerFactory.createScheduler());

        setStatus(Status.PENDING);

        logger.info("awaiting node and relationship streams");

        future = CompletableFuture.allOf(nodeSchema, relsSchema)
                .thenApplyAsync((unused) -> {
                    logger.info("building importer for database {}", dbName);

                    lifeSupport.start();
                    setStatus(Status.PRODUCING);
                    var importer = Neo4jProxy.instantiateBatchImporter(
                            BatchImporterFactory.withHighestPriority(),
                            dbLayout,
                            fs,
                            PageCacheTracer.NULL,
                            Runtime.getRuntime().availableProcessors(), // TODO: parameterize
                            Optional.empty(),
                            logService,
                            Neo4jProxy.invisibleExecutionMonitor(),
                            AdditionalInitialIds.EMPTY,
                            config,
                            RecordFormatSelector.selectForConfig(config, logService.getInternalLogProvider()),
                            ImportLogic.NO_MONITOR,
                            jobScheduler,
                            Collector.EMPTY
                    );
                    try {
                        logger.info("doing import of {}", dbName);
                        importer.doImport(bulkInput);
                        dbms.createDatabase(dbName);
                        dbms.startDatabase(dbName);
                        logger.info("created {}", dbName);
                        return true;
                    } catch (IOException io) {
                        logger.error("failed import", io);
                    } catch (DatabaseExistsException dbe) {
                        logger.error("failed to create database {}", dbName);
                    } catch (DatabaseNotFoundException dbe) {
                        logger.error("failed to start database {}", dbName);
                    }
                    return false;
                }).handleAsync((aBoolean, throwable) -> {
                    final String result = (aBoolean == null ? "failed" : "ok!");
                    onCompletion(() -> result);

                    try {
                        lifeSupport.stop();
                    } catch (LifecycleException e) {
                        logger.error("failed to stop life support", e);
                    }
                    // Let's try proactive cleaning up here...
                    AutoCloseables.closeNoChecked(bulkInput);
                    AutoCloseables.closeNoChecked(allocator);

                    if (throwable != null) {
                        logger.error("oh crap", throwable);
                        return false;
                    }
                    logger.info("finished BulkImportJob: {}", result);
                    return aBoolean;
                });
    }

    @Override
    public Consumer<ArrowBatch> getConsumer(Schema schema) {
        // We expect TWO streams, so we need to differentiate Schema based on metadata
        final Map<String, String> metadata = schema.getCustomMetadata();
        assert (metadata != null); // XXX pretty sure this is guaranteed, but not sure

        switch (metadata.getOrDefault(META_KEY, "")) {
            case NODE_STREAM:
                return bulkInput.nodeBatchConsumer;
            case RELS_STREAM:
                return bulkInput.relsBatchConsumer;
            default:
                throw new RuntimeException(String.format("invalid stream metadata, no valid %s value found", META_KEY));
        }
    }

    static class BulkInput implements Input, AutoCloseable {

        private final BlockingQueue<ArrowBatch> incomingNodes = new LinkedBlockingQueue<>();
        private final BlockingQueue<ArrowBatch> incomingRels = new LinkedBlockingQueue<>();

        public final Consumer<ArrowBatch> nodeBatchConsumer = incomingNodes::add;
        public final Consumer<ArrowBatch> relsBatchConsumer = incomingRels::add;

        private final QueueInputIterator nodeIterator;
        private final QueueInputIterator relsIterator;

        public BulkInput(String idField, String labelsField, String sourceField, String targetField, String typeField) {
            this.nodeIterator = NodeInputIterator.fromQueue(incomingNodes, idField, labelsField);
            this.relsIterator = RelationshipInputIterator.fromQueue(incomingRels, sourceField, targetField, typeField);
        }

        public void signalCompletion() {
            nodeIterator.closeQueue();
            relsIterator.closeQueue();
        }

        @Override
        public InputIterable nodes(Collector badCollector) {
            logger.info("nodes()");
            return () -> nodeIterator;
        }

        @Override
        public InputIterable relationships(Collector badCollector) {
            logger.info("rels()");
            return () -> relsIterator;
        }

        @Override
        public IdType idType() {
            return IdType.ACTUAL;
        }

        @Override
        public ReadableGroups groups() {
            return Groups.EMPTY;
        }

        @Override
        public Estimates calculateEstimates(PropertySizeCalculator valueSizeCalculator) throws IOException {
            // TODO wire in estimate details to a BulkImportMessage parameter
            return Input.knownEstimates(5, 1, 0, 0, 0, 0, 1);
        }

        @Override
        public void close() throws Exception {
            AutoCloseables.closeNoChecked(nodeIterator);
            AutoCloseables.closeNoChecked(relsIterator);
        }
    }

    public String getUsername() {
        return username;
    }

    @Override
    public String toString() {
        return "BulkImportJob{" +
                "username='" + username + '\'' +
                ", jobId='" + jobId + '\'' +
                ", status='" + getStatus() + '\'' +
                '}';
    }
}
