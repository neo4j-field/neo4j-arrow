package org.neo4j.arrow;

import org.junit.jupiter.api.Test;
import org.neo4j.driver.*;

public class SillyBenchmark {
    private static final org.slf4j.Logger logger;
    static {
        // Set up nicer logging output.
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "[yyyy-MM-dd'T'HH:mm:ss:SSS]");
        System.setProperty("org.slf4j.simpleLogger.logFile", "System.out");
        logger = org.slf4j.LoggerFactory.getLogger(SillyBenchmark.class);
    }
    @Test
    public void testSilly1MInts() {
        try (Driver driver = GraphDatabase.driver("neo4j://localhost:7687",
                AuthTokens.basic("neo4j", "password"))) {
            for (int i = 0; i < 10; i++) {
                try (Session session = driver.session(SessionConfig.builder()
                        .withDefaultAccessMode(AccessMode.READ).build())) {
                    try {
                        long start = System.currentTimeMillis();
                        Result result = session.run("UNWIND range(1, 1000000) AS n RETURN n");
                        while (result.hasNext()) {
                            result.next();
                        }
                        long finish = System.currentTimeMillis();
                        logger.info("finished in {}ms", finish - start);
                    } catch (Exception e) {
                        logger.error("oops", e);
                    }
                }
            }
        }
    }
}
