package org.neo4j.arrow.job;

import org.neo4j.arrow.action.BulkImportMessage;
import org.neo4j.arrow.batchimport.NodeInputIterable;
import org.neo4j.arrow.batchimport.RelationshipInputIterable;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.core.Settings;
import org.neo4j.internal.batchimport.AdditionalInitialIds;
import org.neo4j.internal.batchimport.BatchImporterFactory;
import org.neo4j.internal.batchimport.ImportLogic;
import org.neo4j.internal.batchimport.InputIterable;
import org.neo4j.internal.batchimport.input.*;
import org.neo4j.io.fs.FileSystemAbstraction;
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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class BulkImportJob extends WriteJob {

    private final CompletableFuture<Boolean> future;

    private final String username;

    private final Path homePath;
    private final FileSystemAbstraction fs;

    public BulkImportJob(BulkImportMessage msg, String username, DatabaseManagementService dbms) {
        super();
        this.username = username;

        // We need a reference to a GraphDatabaseAPI. Any reference will do.
        final GraphDatabaseAPI api = (GraphDatabaseAPI) dbms.database(GraphDatabaseSettings.SYSTEM_DATABASE_NAME);
        this.homePath = api.databaseLayout().getNeo4jLayout().homeDirectory();
        this.fs = api.getDependencyResolver().resolveDependency(FileSystemAbstraction.class);

        var config = Config.defaults(Settings.neo4jHome(), homePath);
        var dbLayout = Neo4jLayout.of(config).databaseLayout(msg.getDbName());

        // TODO: figure out the LogService api
        final LogService logService = new SimpleLogService(NullLogProvider.getInstance());
        final LifeSupport lifeSupport = new LifeSupport();
        final JobScheduler jobScheduler = lifeSupport.add(JobSchedulerFactory.createScheduler());


        future = CompletableFuture.supplyAsync(() -> {
            lifeSupport.start();

            var importer = Neo4jProxy.instantiateBatchImporter(
                    BatchImporterFactory.withHighestPriority(),
                    dbLayout,
                    fs,
                    PageCacheTracer.NULL,
                    1,
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
                importer.doImport(new BulkInput());
            } catch (IOException io) {
                logger.error("failed import", io);
                return false;
            }
            return true;
        }).exceptionally(throwable -> {
            return false;
        }).handleAsync((aBoolean, throwable) -> {
            lifeSupport.stop();
            return aBoolean;
        });
    }


    @Override
    public void onError(Exception e) {

    }

    static class BulkInput implements Input {

        @Override
        public InputIterable nodes(Collector badCollector) {
            return new NodeInputIterable();
        }

        @Override
        public InputIterable relationships(Collector badCollector) {
            return new RelationshipInputIterable();
        }

        @Override
        public IdType idType() {
            return IdType.ACTUAL;
        }

        @Override
        public ReadableGroups groups() {
            return new ReadableGroups() {
                @Override
                public Group get(int id) {
                    return new Group() {
                        @Override
                        public int id() {
                            return 0;
                        }

                        @Override
                        public String name() {
                            return "uhhh-a-group?";
                        }
                    };
                }

                @Override
                public int size() {
                    return 1;
                }
            };
        }

        @Override
        public Estimates calculateEstimates(PropertySizeCalculator valueSizeCalculator) throws IOException {
            return null;
        }
    }

    
}
