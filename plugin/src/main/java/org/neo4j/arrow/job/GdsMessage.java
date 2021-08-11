package org.neo4j.arrow.job;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

public class GdsMessage {
    private final String graph;
    private final List<String> filters;
    private final Element target;

    enum Element {
        NODE,
        RELATIONSHIP
    }

    public GdsMessage(String graph, Element target) {
        this(graph, target, List.of());
    }

    public GdsMessage(String graph, Element target, List<String> filters) {
        this.graph = graph;
        this.target = target;
        this.filters = filters;
    }

    public static GdsMessage deserialize(byte[] bytes) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(bytes)
                .asReadOnlyBuffer()
                .order(ByteOrder.BIG_ENDIAN);

        return new GdsMessage("mygraph", Element.NODE);
    }
}
