package org.neo4j.arrow.action;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class GdsMessage {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(GdsMessage.class);

    static private final ObjectMapper mapper = new ObjectMapper();

    public static final String JSON_KEY_DATABASE_NAME = "db";
    public static final String JSON_KEY_GRAPH_NAME = "graph";
    public static final String JSON_KEY_FILTER_LIST = "filters";
    public static final String JSON_KEY_PROPERTY_LIST = "properties";

    private String dbName;
    private String graphName;
    private List<String> filters;
    private List<String> properties;

    public GdsMessage(String dbName, String graphName, List<String> properties, List<String> filters) {
        this.dbName = dbName;
        this.graphName = graphName;
        this.properties = properties;
        this.filters = filters;

        // TODO: validation / constraints of values?
    }

    public byte[] serialize() {
        try {
            return mapper.writeValueAsString(
                    Map.of(JSON_KEY_DATABASE_NAME, dbName,
                            JSON_KEY_GRAPH_NAME, graphName,
                            JSON_KEY_FILTER_LIST, filters,
                            JSON_KEY_PROPERTY_LIST, properties))
                    .getBytes(StandardCharsets.UTF_8);
        } catch (JsonProcessingException e) {
            logger.error("serialization error", e);
            e.printStackTrace();
        }
        return new byte[0];
    }

    public static GdsMessage deserialize(byte[] bytes) throws IOException {
        final Map<String, Object> params = mapper.createParser(bytes).readValueAs(Map.class);

        logger.info("XXX got gds message keys: {}", params.keySet());

        final String dbName = params.getOrDefault(JSON_KEY_DATABASE_NAME, "neo4j").toString();
        // TODO: assert our minimum schema
        final String graphName = params.get(JSON_KEY_GRAPH_NAME).toString();

        List<String> filters = List.of();
        Object obj = params.getOrDefault(JSON_KEY_FILTER_LIST, filters);
        if (obj instanceof List) {
            filters = ((List<?>)obj).stream().map(Object::toString).collect(Collectors.toList());
        }

        List<String> properties = List.of();
        obj = params.getOrDefault(JSON_KEY_PROPERTY_LIST, properties);
        if (obj instanceof List) {
            properties = ((List<?>)obj).stream().map(Object::toString).collect(Collectors.toList());
        }

        return new GdsMessage(dbName, graphName, properties, filters);
    }

    public String getDbName() {
        return dbName;
    }

    public String getGraphName() {
        return graphName;
    }

    public List<String> getFilters() {
        return filters;
    }

    public List<String> getProperties() {
        return properties;
    }

    @Override
    public String toString() {
        return "GdsMessage{" +
                "dbName='" + dbName + '\'' +
                ", graphName='" + graphName + '\'' +
                ", filters=" + filters +
                ", properties=" + properties +
                '}';
    }
}
