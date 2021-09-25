package org.neo4j.arrow;

import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.auth2.BasicCallHeaderAuthenticator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.neo4j.arrow.action.CypherActionHandler;
import org.neo4j.arrow.action.GdsActionHandler;
import org.neo4j.arrow.auth.NativeAuthValidator;
import org.neo4j.arrow.job.GdsReadJob;
import org.neo4j.arrow.job.TransactionApiJob;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * An Apache Arrow service for Neo4j, offering Arrow RPC-based access to Cypher and GDS services.
 * <p>
 * Since this runs as a database plugin, all Cypher access is via the Transaction API. GDS access
 * is available directly to the Graph Catalog.
 */
public class ArrowService extends LifecycleAdapter {

    private final DatabaseManagementService dbms;
    private final Log log;

    private App app;
    private Location location;
    private BufferAllocator allocator;

    public ArrowService(DatabaseManagementService dbms, LogService logService) {
        this.dbms = dbms;
        this.log = logService.getUserLog(ArrowService.class);
    }

    @Override
    public void init() throws Exception {
        super.init();
        log.info(">>>--[Arrow]--> init()");
        allocator = new RootAllocator(Config.maxGlobalMemory);
        location = Location.forGrpcInsecure(Config.host, Config.port);

        // Use GDS's handy hooks to get our Auth Manager. Needs to be deferred as it will fail
        // if we try to get a reference here since it doesn't exist yet.
        final Supplier<AuthManager> authManager = () ->
                GraphDatabaseApiProxy.resolveDependency(dbms.database(
                        GraphDatabaseSettings.SYSTEM_DATABASE_NAME), AuthManager.class);

        app = new App(allocator, location, "neo4j-arrow-plugin",
                new BasicCallHeaderAuthenticator(new NativeAuthValidator(authManager, log)));

        app.registerHandler(new CypherActionHandler(
                (msg, mode, username) -> new TransactionApiJob(msg, username, dbms, log)));
        app.registerHandler(new GdsActionHandler(
                (msg, mode, username) -> new GdsReadJob(msg, username), log));
    }

    @Override
    public void start() throws Exception {
        super.start();
        log.info(">>>--[Arrow]--> start()");
        app.start();
        log.info("started arrow app at location " + location);
    }

    @Override
    public void stop() throws Exception {
        super.stop();

        log.info(">>>--[Arrow]--> stop()");

        // Use an async approach to stopping so we don't completely block Neo4j's shutdown waiting
        // for streams to terminate.
        long timeout = 5;
        TimeUnit unit = TimeUnit.SECONDS;

        log.info(String.format(">>>--[Arrow]--> waiting %d %s for jobs to complete", timeout, unit));
        CompletableFuture.runAsync(() -> {
            try {
                app.awaitTermination(timeout, unit);
                log.info("stopped app " + app);
            } catch (InterruptedException e) {
                log.error("failed to stop app " + app, e);
            }
        });
    }

    @Override
    public void shutdown() throws Exception {
        super.shutdown();
        log.info(">>>--[Arrow]--> shutdown()");

        // XXX you must close the allocator BEFORE the app!
        AutoCloseables.close(allocator, app);
    }
}
