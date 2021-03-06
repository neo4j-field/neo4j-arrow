package org.neo4j.arrow;

import org.neo4j.driver.*;

import java.util.Locale;
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

    private static final String cypher;
    static {
        switch (System.getenv()
                .getOrDefault("TEST_MODE", "CYPHER")
                .toUpperCase(Locale.ROOT)) {
            case "GDS":
                cypher = "CALL gds.graph.streamNodeProperty('mygraph', 'n')";
                break;
            case "RANDOM":
                cypher = "UNWIND range(1, 10000000) AS row RETURN row, [_ IN range(1, 256) | rand()] as fauxEmbedding";
                break;
            case "SIMPLE":
                cypher = "UNWIND range(1, toInteger(1e7)) AS row RETURN row, range(0, 64) AS data";
                break;
            case "CYPHER":
            default:
                cypher = "MATCH (n) RETURN id(n) as nodeId, n.fastRp AS embedding";
                break;
        }
    }

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
