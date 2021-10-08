package org.neo4j.arrow.action;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class GdsWriteRelsMessage implements Message {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(GdsWriteRelsMessage.class);

    static private final ObjectMapper mapper = new ObjectMapper();

    public static final String JSON_KEY_DATABASE_NAME = "db";
    public static final String JSON_KEY_GRAPH_NAME = "graph";

    public static final String JSON_KEY_SOURCE_FIELD = "sourceField";
    public static final String DEFAULT_SOURCE_FIELD = "source";

    public static final String JSON_KEY_TARGET_FIELD = "targetField";
    public static final String DEFAULT_TARGET_FIELD = "target";

    public static final String JSON_KEY_TYPE_FIELD = "typeField";
    public static final String DEFAULT_TYPE_FIELD = "type";

    /** Name of the Neo4j Database where our Graph lives. Optional. Defaults to "neo4j" */
    private final String dbName;
    /** Name of the Graph (projection) from the Graph Catalog. Required. */
    private final String graphName;

    private final String sourceField;
    private final String targetField;
    private final String typeField;

    protected GdsWriteRelsMessage(String dbName, String graphName, String sourceField, String targetField, String typeField) {
        this.dbName = dbName;
        this.graphName = graphName;

        this.sourceField = (sourceField != null) ? sourceField : DEFAULT_SOURCE_FIELD;
        this.targetField = (targetField != null) ? targetField : DEFAULT_TARGET_FIELD;
        this.typeField = (typeField != null) ? typeField : DEFAULT_TYPE_FIELD;
    }

    private static class MapTypeReference extends TypeReference<Map<String, Object>> { }

    public static GdsWriteRelsMessage deserialize(byte[] bytes) throws IOException {
        final JsonParser parser = mapper.createParser(bytes);
        final Map<String, Object> params = parser.readValueAs(new GdsWriteRelsMessage.MapTypeReference());

        final String dbName = params.getOrDefault(JSON_KEY_DATABASE_NAME, "neo4j").toString();
        // TODO: assert our minimum schema?
        final String graphName = params.get(JSON_KEY_GRAPH_NAME).toString();

        final String sourceIdField = params.get(JSON_KEY_SOURCE_FIELD) != null ?
                params.get(JSON_KEY_SOURCE_FIELD).toString() : null;

        final String targetIdField = params.get(JSON_KEY_TARGET_FIELD) != null ?
                params.get(JSON_KEY_TARGET_FIELD).toString() : null;

        final String typeField = params.get(JSON_KEY_TYPE_FIELD) != null ?
                params.get(JSON_KEY_TYPE_FIELD).toString() : null;

        return new GdsWriteRelsMessage(dbName, graphName, sourceIdField, targetIdField, typeField);
    }

    @Override
    public byte[] serialize() {
        try {
            return mapper.writeValueAsString(
                    Map.of(JSON_KEY_DATABASE_NAME, dbName,
                        JSON_KEY_GRAPH_NAME, graphName,
                            JSON_KEY_SOURCE_FIELD, sourceField,
                            JSON_KEY_TARGET_FIELD, targetField,
                        JSON_KEY_TYPE_FIELD, typeField))
                    .getBytes(StandardCharsets.UTF_8);
        } catch (JsonProcessingException e) {
            logger.error("serialization error", e);
        }
        return new byte[0];
    }

    public String getDbName() {
        return dbName;
    }

    public String getGraphName() {
        return graphName;
    }

    public String getSourceField() {
        return sourceField;
    }

    public String getTargetField() {
        return targetField;
    }

    public String getTypeField() {
        return typeField;
    }
}
