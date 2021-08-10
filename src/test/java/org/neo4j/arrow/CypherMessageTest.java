package org.neo4j.arrow;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.neo4j.arrow.job.CypherMessage;
import org.neo4j.arrow.job.JobCreator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class CypherMessageTest {
    private static final org.slf4j.Logger logger;

    static {
        // Set up nicer logging output.
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "[yyyy-MM-dd'T'HH:mm:ss:SSS]");
        System.setProperty("org.slf4j.simpleLogger.logFile", "System.out");
        logger = org.slf4j.LoggerFactory.getLogger(Neo4jFlightServerTest.class);
    }

    @Test
    public void canSerializeCypherMessages() throws IOException {
        final String cypher = "MATCH (n:MyLabel) RETURN [n.prop1] AS prop1, n AS node;";
        final Map<String, Object> map = Map.of("n", 123, "name", "Dave");

        CypherMessage msg = new CypherMessage(cypher, map);
        byte[] bytes = msg.serialize();
        Assertions.assertTrue(bytes.length > 0);

        CypherMessage msg2 = CypherMessage.deserialize(bytes);
        System.out.println(StandardCharsets.UTF_8.decode(ByteBuffer.wrap(bytes)));

        Assertions.assertEquals(cypher, msg2.getCypher());
        Assertions.assertEquals(map, msg2.getParams());
    }
}
