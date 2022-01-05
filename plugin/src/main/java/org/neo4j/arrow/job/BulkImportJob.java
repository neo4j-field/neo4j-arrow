package org.neo4j.arrow.job;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.neo4j.arrow.action.BulkImportMessage;
import org.neo4j.arrow.batch.ArrowBatch;
import org.neo4j.arrow.batchimport.NodeInputIterator;
import org.neo4j.arrow.batchimport.RelationshipInputIterable;
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
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.scheduler.JobScheduler;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class BulkImportJob extends WriteJob {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BulkImportJob.class);

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
        this.bulkInput = new BulkInput(msg.getIdField(), msg.getLabelsField());

        final Config config = Config.defaults(Settings.neo4jHome(), homePath);
        final DatabaseLayout dbLayout = Neo4jLayout.of(config).databaseLayout(dbName);

        // TODO: figure out the LogService api
        final LogService logService = new SimpleLogService(NullLogProvider.getInstance());
        final LifeSupport lifeSupport = new LifeSupport();
        final JobScheduler jobScheduler = lifeSupport.add(JobSchedulerFactory.createScheduler());

        setStatus(Status.PENDING);

        logger.info("awaiting schema");

        future = getSchema()
                .thenApplyAsync((schema) -> {
                    logger.info("building importer for database {}", dbName);
                    setStatus(Status.PRODUCING);

                    lifeSupport.start();

                    var importer = Neo4jProxy.instantiateBatchImporter(
                            BatchImporterFactory.withHighestPriority(),
                            dbLayout,
                            fs,
                            PageCacheTracer.NULL,
                            1, //Runtime.getRuntime().availableProcessors(),
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
                    lifeSupport.stop();
                    if (throwable != null) {
                        logger.error("oh crap", throwable);
                        return false;
                    }
                    logger.info("finished job: {}", aBoolean);
                    return aBoolean;
                });
    }

    @Override
    public Consumer<ArrowBatch> getConsumer() {
        // TODO: toggle between node and rel consumption
        return bulkInput.nodeBatchConsumer;
    }

    static class BulkInput implements Input {

        private final BlockingQueue<ArrowBatch> incomingNodes = new LinkedBlockingQueue<>();
        private final BlockingQueue<ArrowBatch> incomingRels = new LinkedBlockingQueue<>();

        public final Consumer<ArrowBatch> nodeBatchConsumer = incomingNodes::add;
        public final Consumer<ArrowBatch> relsBatchConsumer = incomingRels::add;

        private final NodeInputIterator nodeIterator;
        // private final RelationshipInputIterator relsIterator;

        public BulkInput(String idField, String labelsField) {
            this.nodeIterator = (NodeInputIterator) NodeInputIterator.fromQueue(incomingNodes, idField, labelsField);
        }

        public void signalCompletion() {
            nodeIterator.closeQueue();
        }

        @Override
        public InputIterable nodes(Collector badCollector) {
            logger.info("nodes()");
            return () -> nodeIterator;
        }

        @Override
        public InputIterable relationships(Collector badCollector) {
            logger.info("rels()");
            return new RelationshipInputIterable();
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
