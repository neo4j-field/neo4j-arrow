package org.neo4j.arrow.action;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

public class GdsMessageTest {
    @Test
    public void testGdsMessageSerialization() throws IOException {
        final GdsMessage msg = new GdsMessage("db1", "graph1", GdsMessage.RequestType.NODE, List.of("prop1", "prop2"), List.of("filter1"));
        final byte[] bytes = msg.serialize();

        final GdsMessage msg2 = GdsMessage.deserialize(bytes);
        Assertions.assertEquals("db1", msg2.getDbName());
        Assertions.assertEquals("graph1", msg2.getGraphName());
        Assertions.assertArrayEquals(new String[] { "prop1", "prop2" }, msg2.getProperties().toArray(new String[2]));
        Assertions.assertArrayEquals(new String[] { "filter1" }, msg2.getFilters().toArray(new String[1]));
    }
}
