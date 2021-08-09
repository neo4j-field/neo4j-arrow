package org.neo4j.arrow;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.*;

import java.util.Map;

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
    //@Disabled
    public void testSilly1MillionEmbeddings() {
        try (Driver driver = GraphDatabase.driver("neo4j://localhost:7687",
                AuthTokens.basic("neo4j", "password"))) {
            for (int i = 0; i < 1; i++) {
                try (Session session = driver.session(SessionConfig.builder()
                        .withDefaultAccessMode(AccessMode.READ).build())) {
                    try {
                        long start = System.currentTimeMillis();
                        Result result = session.run(
                               /* "UNWIND range(1, $rows) AS row\n" +
                                "RETURN row, [_ IN range(1, $dimension) | rand()] as fauxEmbedding",*/
                                "call gds.graph.streamNodeProperty('mygraph', 'n') yield nodeId, propertyValue\n" +
                                        "return nodeId, propertyValue as value",
                                Map.of("rows", 1_000_000, "dimension", 128));
                        long cnt = 0;
                        while (result.hasNext()) {
                            result.next();
                            cnt++;
                            if (cnt % 25_000 == 0)
                                logger.info("Current Row @ {} [fields: {}]", cnt, result.keys());
                        }
                        long finish = System.currentTimeMillis();
                        logger.info(String.format("finished in %,d ms, rate of %,d rows/sec",
                                (finish - start), 1000 * cnt / (finish - start)));
                    } catch (Exception e) {
                        logger.error("oops", e);
                    }
                }
            }
        }
    }
}
