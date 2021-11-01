package org.neo4j.arrow.action;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class KHopMessage implements Message {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(KHopMessage.class);
    static private final ObjectMapper mapper = new ObjectMapper();

    public static final String JSON_KEY_DATABASE_NAME = "db";
    public static final String JSON_KEY_GRAPH_NAME = "graph";
    public static final String JSON_KEY_K = "k";

    private final String dbName;
    private final String graph;
    private final int k;

    public KHopMessage(String dbName, String graph, int k) {
        this.dbName = dbName;
        this.graph = graph;
        this.k = k;
    }

    public String getDbName() {
        return dbName;
    }

    public String getGraph() {
        return graph;
    }

    public int getK() {
        return k;
    }

    private static class MapTypeReference extends TypeReference<Map<String, Object>> { }

    @Override
    public byte[] serialize() {
        try {
            return mapper.writeValueAsString(
                            Map.of(JSON_KEY_DATABASE_NAME, dbName,
                                    JSON_KEY_GRAPH_NAME, graph,
                                    JSON_KEY_K, k))
                    .getBytes(StandardCharsets.UTF_8);
        } catch (JsonProcessingException e) {
            logger.error("serialization error", e);
        }
        return new byte[0];
    }

    public static KHopMessage deserialize(byte[] bytes) throws IOException {
        final JsonParser parser = mapper.createParser(bytes);
        final Map<String, Object> params = parser.readValueAs(new KHopMessage.MapTypeReference());

        final String dbName = params.getOrDefault(JSON_KEY_DATABASE_NAME, "neo4j").toString();
        final String graph = params.getOrDefault(JSON_KEY_GRAPH_NAME, "random").toString();
        final Integer k = (Integer) params.getOrDefault(JSON_KEY_K, 2);

        return new KHopMessage(dbName, graph, k);
    }

    @Override
    public String toString() {
        return "KHopMessage{" +
                "dbName='" + dbName + '\'' +
                "graph='" + graph + '\'' +
                ", k=" + k +
                '}';
    }
}
