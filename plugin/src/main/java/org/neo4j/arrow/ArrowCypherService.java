package org.neo4j.arrow;

import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;

import java.util.concurrent.TimeUnit;

public class ArrowCypherService extends LifecycleAdapter {

    private final DatabaseManagementService dbms;
    private final Log log;
    private final JobCreator jobCreator;

    private Neo4jFlightApp app;
    private BufferAllocator allocator;
    private Location location;

    public ArrowCypherService(DatabaseManagementService dbms, LogService logService) {
        this.dbms = dbms;
        this.log = logService.getUserLog(ArrowCypherService.class);

        // TODO: integrate a Settings thing
        this.jobCreator = (message, mode, username, password) ->
                new ServerSideJob(message, mode,
                        dbms.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME), this.log);
    }

    @Override
    public void init() throws Exception {
        super.init();
        log.info(">>>--[Arrow]--> init()");
        allocator = new RootAllocator(Config.maxGlobalMemory);
        location = Location.forGrpcInsecure(Config.host, Config.port);
        app = new Neo4jFlightApp(allocator, location, jobCreator);
    }

    @Override
    public void start() throws Exception {
        super.start();
        log.info(">>>--[Arrow]--> start()");
        app.start();
    }

    @Override
    public void stop() throws Exception {
        super.stop();

        log.info(">>>--[Arrow]--> stop()");
        long timeout = 3;
        TimeUnit unit = TimeUnit.SECONDS;

        try {
            log.info(String.format(">>>--[Arrow]--> awaiting %d %s", timeout, unit));
            app.awaitTermination(timeout, unit);
        } catch (InterruptedException e) {
            log.info(">>>--[Arrow]--> terminated.");
        }
    }

    @Override
    public void shutdown() throws Exception {
        super.shutdown();
        log.info(">>>--[Arrow]--> shutdown()");
        AutoCloseables.close(allocator, app);
    }
}
