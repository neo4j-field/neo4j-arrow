package org.neo4j.arrow.action;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class GdsWriteNodeMessage implements Message {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(GdsWriteNodeMessage.class);

    static private final ObjectMapper mapper = new ObjectMapper();

    public static final String JSON_KEY_DATABASE_NAME = "db";
    public static final String JSON_KEY_GRAPH_NAME = "graph";
    // TODO: "type" is pretty vague...needs a better name
    public static final String JSON_KEY_NODE_ID_FIELD = "idField";
    public static final String DEFAULT_NODE_ID_FIELD = "id";

    public static final String JSON_KEY_LABELS_FIELD = "labelsField";
    public static final String DEFAULT_LABELS_FIELD = "labels";

    /** Name of the Neo4j Database where our Graph lives. Optional. Defaults to "neo4j" */
    private final String dbName;
    /** Name of the Graph (projection) from the Graph Catalog. Required. */
    private final String graphName;

    private final String idField;
    private final String labelsField;

    public GdsWriteNodeMessage(String dbName, String graphName, @Nullable String idField, @Nullable String labelsField) {
        this.dbName = dbName;
        this.graphName = graphName;

        this.idField = (idField != null) ? idField : DEFAULT_NODE_ID_FIELD;
        this.labelsField = (labelsField != null) ? labelsField : DEFAULT_LABELS_FIELD;
    }

    /**
     * Serialize the GdsMessage to a JSON blob of bytes.
     * @return byte[] containing UTF-8 JSON blob or byte[0] on error
     */
    public byte[] serialize() {
        try {
            return mapper.writeValueAsString(
                    Map.of(JSON_KEY_DATABASE_NAME, dbName,
                            JSON_KEY_GRAPH_NAME, graphName,
                            JSON_KEY_NODE_ID_FIELD, idField,
                            JSON_KEY_LABELS_FIELD, labelsField))
                    .getBytes(StandardCharsets.UTF_8);
        } catch (JsonProcessingException e) {
            logger.error("serialization error", e);
        }
        return new byte[0];
    }

    private static class MapTypeReference extends TypeReference<Map<String, Object>> { }

    /**
     * Deserialize the given bytes, containing JSON, into a GdsMessage instance.
     * @param bytes UTF-8 bytes containing JSON payload
     * @return new GdsMessage
     * @throws IOException if error encountered during serialization
     */
    public static GdsWriteNodeMessage deserialize(byte[] bytes) throws IOException {

        final JsonParser parser = mapper.createParser(bytes);
        final Map<String, Object> params = parser.readValueAs(new GdsWriteNodeMessage.MapTypeReference());

        final String dbName = params.getOrDefault(JSON_KEY_DATABASE_NAME, "neo4j").toString();
        // TODO: assert our minimum schema?
        final String graphName = params.get(JSON_KEY_GRAPH_NAME).toString();

        final String idField = params.get(JSON_KEY_NODE_ID_FIELD) != null ?
                params.get(JSON_KEY_NODE_ID_FIELD).toString() : null;

        final String labelsField = params.get(JSON_KEY_LABELS_FIELD) != null ?
            params.get(JSON_KEY_LABELS_FIELD).toString() : null;

        return new GdsWriteNodeMessage(dbName, graphName, idField, labelsField);
    }

    public String getDbName() {
        return dbName;
    }

    public String getGraphName() {
        return graphName;
    }

    public String getIdField() {
        return idField;
    }

    public String getLabelsField() {
        return labelsField;
    }

    @Override
    public String toString() {
        return "GdsWriteNodeMessage{" +
                "db='" + dbName + "'" +
                ", graph='" + graphName + "'" +
                ", idField='" + idField + "'" +
                ", labelsField='" + labelsField + "'" +
                "}";
    }
}
