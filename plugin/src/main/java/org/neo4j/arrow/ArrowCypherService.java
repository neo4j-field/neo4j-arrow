package org.neo4j.arrow;

import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.neo4j.arrow.job.CypherJob;
import org.neo4j.arrow.job.GdsJob;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ArrowCypherService extends LifecycleAdapter {

    private final DatabaseManagementService dbms;
    private final Log log;

    private Neo4jFlightApp cypherApp, gdsApp;
    private Location cypherAppLocation, gdsAppLocation;
    private BufferAllocator allocator;

    public ArrowCypherService(DatabaseManagementService dbms, LogService logService) {
        this.dbms = dbms;
        this.log = logService.getUserLog(ArrowCypherService.class);
    }

    @Override
    public void init() throws Exception {
        super.init();
        log.info(">>>--[Arrow]--> init()");
        allocator = new RootAllocator(Config.maxGlobalMemory);

        // To keep it simple for now, spin up 2 apps (only difference being the job type)
        // The cypher app gets the PORT, the gds app gets PORT+1
        cypherAppLocation = Location.forGrpcInsecure(Config.host, Config.port);
        gdsAppLocation = Location.forGrpcInsecure(Config.host, Config.port + 1);

        cypherApp = new Neo4jFlightApp(allocator, cypherAppLocation,
                (message, mode, username, password) -> {
                    log.info("creating CypherJob...");
                    return new CypherJob(message, mode, dbms.database(Config.database), log);
                });

        gdsApp = new Neo4jFlightApp(allocator, gdsAppLocation,
                (message, mode, username, password) -> {
                    log.info("creating GdsJob...");
                    return new GdsJob(message, mode, this.log);
                });

    }

    @Override
    public void start() throws Exception {
        super.start();
        log.info(">>>--[Arrow]--> start()");
        cypherApp.start();
        log.info("started cypher arrow app at location " + cypherAppLocation);
        gdsApp.start();
        log.info("started gds arrow app at location " + gdsAppLocation);
    }

    @Override
    public void stop() throws Exception {
        super.stop();

        log.info(">>>--[Arrow]--> stop()");
        long timeout = 5;
        TimeUnit unit = TimeUnit.SECONDS;

        log.info(String.format(">>>--[Arrow]--> stopping apps %d %s", timeout, unit));
        CompletableFuture.allOf(Stream.of(cypherApp, gdsApp)
                .map(app -> CompletableFuture.runAsync(() -> {
                    try {
                        app.awaitTermination(timeout, unit);
                        log.info("stopped app " + app);
                    } catch (InterruptedException e) {
                        log.error("failed to stop app " + app, e);
                    }
                })).collect(Collectors.toList()).toArray(new CompletableFuture[2]));
    }

    @Override
    public void shutdown() throws Exception {
        super.shutdown();
        log.info(">>>--[Arrow]--> shutdown()");
        AutoCloseables.close(allocator, cypherApp, gdsApp);
    }
}
