package org.neo4j.arrow;

import org.neo4j.driver.*;

import java.util.concurrent.atomic.AtomicInteger;

public class Neo4jDirectClient {
    private static final org.slf4j.Logger logger;
    static {
        // Set up nicer logging output.
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "[yyyy-MM-dd'T'HH:mm:ss:SSS]");
        System.setProperty("org.slf4j.simpleLogger.logFile", "System.out");
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        logger = org.slf4j.LoggerFactory.getLogger(Neo4jDirectClient.class);
    }

    private static final String cypher =
            System.getenv("GDS") != null
                    ? "CALL gds.graph.streamNodeProperty('mygraph', 'n')"
                    : "UNWIND range(1, 1000000) AS row RETURN row, [_ IN range(1, 128) | rand()] as fauxEmbedding";

    public static void main(String[] args) {
        logger.info("Connecting to {} using Java Driver", Config.neo4jUrl);
        try (Driver driver =
                     GraphDatabase.driver(Config.neo4jUrl, AuthTokens.basic(Config.username, Config.password))) {
            logger.info("Opening session with database {}", Config.database);
            final SessionConfig config = SessionConfig.builder()
                    .withDatabase(Config.database)
                    .withFetchSize(Config.boltFetchSize)
                    .withDefaultAccessMode(AccessMode.READ)
                    .build();
            try (Session session = driver.session(config)) {
                AtomicInteger cnt = new AtomicInteger(0);
                logger.info("Executing cypher: {}", cypher);
                final long start = System.currentTimeMillis();
                session.run(cypher).stream().forEach(record -> {
                    int i = cnt.incrementAndGet();
                    if (i % 25_000 == 0) {
                        logger.info("Current Row @ {}:\t[fields: {}]", i, record.keys());
                    }
                });
                final float delta = (System.currentTimeMillis() - start) / 1000.0f;
                logger.info(String.format("Done! Time Delta: %,f s", delta));
                logger.info(String.format("Count=%,d rows, Rate=%,d rows/s",
                        cnt.get(), cnt.get() / Math.round(delta)));
            }
        }

    }
}
