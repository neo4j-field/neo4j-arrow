package org.neo4j.arrow;

import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

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
        long timeout = 5;
        TimeUnit unit = TimeUnit.SECONDS;

        log.info(String.format(">>>--[Arrow]--> stopping apps %d %s", timeout, unit));
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
        AutoCloseables.close(allocator, app);
    }
}
